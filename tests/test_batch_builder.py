from __future__ import annotations

from prostometrics._batch_builder import BatchBuilder
from prostometrics._payload import COUNTER, SUCCESS, TOP, TOTAL, UNIQUE, VALUE, VALUE_SPARSE, Event


def _event(kind: str, metric: str, value: float = 1.0, labels=None, ts: int = 10, unique_id=None, item=None) -> Event:
    return Event(kind, metric, value, list(labels or []), ts, unique_id, item)


def test_counters_sharing_a_series_are_summed():
    builder = BatchBuilder()
    builder.add(_event(COUNTER, "requests", 2, ["m=GET"], ts=10))
    builder.add(_event(COUNTER, "requests", 3, ["m=GET"], ts=14))
    payload = builder.build()
    assert len(payload.counters) == 1
    assert payload.counters[0].value == 5
    assert payload.counters[0].timestamp == 14


def test_counters_on_different_series_stay_separate():
    builder = BatchBuilder()
    builder.add(_event(COUNTER, "requests", 1, ["m=GET"]))
    builder.add(_event(COUNTER, "requests", 1, ["m=POST"]))
    assert len(builder.build().counters) == 2


def test_value_samples_are_never_merged():
    builder = BatchBuilder()
    builder.add(_event(VALUE, "latency", 1.0))
    builder.add(_event(VALUE, "latency", 2.0))
    builder.add(_event(VALUE_SPARSE, "capacity", 3.0))
    payload = builder.build()
    assert [v.value for v in payload.values] == [1.0, 2.0, 3.0]
    assert [v.sparse for v in payload.values] == [False, False, True]


def test_success_samples_are_marked_as_outcomes():
    builder = BatchBuilder()
    builder.add(_event(SUCCESS, "payment", 100.0))
    builder.add(_event(VALUE, "latency", 2.0))
    payload = builder.build()
    assert [v.success for v in payload.values] == [True, False]
    assert [v.sparse for v in payload.values] == [False, False]


def test_repeated_unique_ids_are_deduplicated_within_a_batch():
    builder = BatchBuilder()
    builder.add(_event(UNIQUE, "dau", unique_id="7"))
    builder.add(_event(UNIQUE, "dau", unique_id="7"))
    builder.add(_event(UNIQUE, "dau", unique_id="8"))
    builder.add(_event(UNIQUE, "dau", labels=["p=pro"], unique_id="7"))
    payload = builder.build()
    assert len(payload.uniques) == 3


def test_repeated_top_items_are_deduplicated_on_id_and_item_together():
    builder = BatchBuilder()
    builder.add(_event(TOP, "reads", unique_id="7", item="a|b"))
    builder.add(_event(TOP, "reads", unique_id="7", item="a|b"))
    builder.add(_event(TOP, "reads", unique_id="7", item="c"))
    builder.add(_event(TOP, "reads", unique_id="8", item="a|b"))
    builder.add(_event(TOP, "views", unique_id="7", item="a|b"))
    payload = builder.build()
    assert [(top.metric, top.unique_id, top.item) for top in payload.tops] == [
        ("reads", "7", "a|b"),
        ("reads", "7", "c"),
        ("reads", "8", "a|b"),
        ("views", "7", "a|b"),
    ]


def test_a_top_event_and_a_unique_event_never_deduplicate_each_other():
    builder = BatchBuilder()
    builder.add(_event(UNIQUE, "reads", unique_id="7"))
    builder.add(_event(TOP, "reads", unique_id="7", item="x"))
    payload = builder.build()
    assert len(payload.uniques) == 1
    assert len(payload.tops) == 1


def test_totals_never_reach_the_wire_directly():
    builder = BatchBuilder()
    builder.add(_event(TOTAL, "bytes", 100))
    assert builder.build() is None


def test_empty_builder_builds_nothing():
    assert BatchBuilder().build() is None
