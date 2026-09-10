from __future__ import annotations

from prostometrics._dictionary import (
    DictionaryState,
    analyze_payload_v5,
    encode_payload_v5,
    format_sample,
    round_away_from_zero,
    should_reset_dictionary,
)
from prostometrics._payload import CounterEvent, Payload, TopEvent, UniqueEvent, ValueEvent


def _payload(**kwargs) -> Payload:
    payload = Payload()
    payload.batch_id = kwargs.pop("batch_id", "b1")
    for key, value in kwargs.items():
        setattr(payload, key, value)
    return payload


def test_first_batch_defines_series_and_starts_at_revision_one():
    state = DictionaryState()
    payload = _payload(counters=[CounterEvent("requests", 3, ["method=GET"], 1_700_000_000)])
    body = encode_payload_v5(payload, state).decode()
    lines = body.strip().split("\n")
    assert lines[0] == f"H|5|s|{state.session_id}|1"
    assert lines[1] == "S|0|requests|method=GET"
    assert lines[2] == "c|0|3|1700000000"


def test_repeat_series_reuses_its_id_without_bumping_the_revision():
    state = DictionaryState()
    payload = _payload(counters=[CounterEvent("requests", 1, [], 10)])
    encode_payload_v5(payload, state)
    second = encode_payload_v5(_payload(counters=[CounterEvent("requests", 2, [], 11)]), state).decode()
    assert second.startswith(f"H|5|s|{state.session_id}|1\n")
    assert "S|" not in second
    assert second.strip().endswith("c|0|2|11")


def test_new_series_bumps_the_revision_and_sends_only_the_new_definition():
    state = DictionaryState()
    encode_payload_v5(_payload(counters=[CounterEvent("a", 1, [], 10)]), state)
    body = encode_payload_v5(_payload(counters=[CounterEvent("b", 1, [], 11)]), state).decode()
    lines = body.strip().split("\n")
    assert lines[0].endswith("|2")
    assert lines[1] == "S|1|b"


def test_force_definitions_restates_the_whole_dictionary():
    state = DictionaryState()
    encode_payload_v5(_payload(counters=[CounterEvent("a", 1, [], 10)]), state)
    body = encode_payload_v5(_payload(counters=[CounterEvent("b", 1, [], 11)]), state, force_definitions=True).decode()
    assert "S|0|a" in body
    assert "S|1|b" in body


def test_event_type_markers_cover_the_protocol():
    state = DictionaryState()
    payload = _payload(
        counters=[CounterEvent("c", 1, [], 10)],
        values=[ValueEvent("v", 1.5, False, [], 11), ValueEvent("s", 2.5, True, [], 12)],
        uniques=[UniqueEvent("u", "99", [], 13)],
        tops=[TopEvent("t", "77", "item", [], 14)],
    )
    body = encode_payload_v5(payload, state).decode().split("\n")
    events = [line for line in body if line[:2] in ("c|", "v|", "s|", "u|", "t|")]
    assert events == ["c|0|1|10", "v|1|1.5|11", "s|2|2.5|12", "u|3|99|13", "t|4|77|14|item"]


def test_top_items_go_on_the_wire_verbatim_after_the_timestamp():
    """The item is the last field, so a pipe or a space inside it needs no escaping."""
    state = DictionaryState()
    payload = _payload(tops=[TopEvent("article_reads", "4242", " \u0451\u0436/17 |x", [], 1_700_000_000)])
    lines = encode_payload_v5(payload, state).decode().strip().split("\n")
    assert lines[1] == "S|0|article_reads"
    assert lines[2] == "t|0|4242|1700000000| \u0451\u0436/17 |x"


def test_empty_payload_encodes_to_nothing():
    assert encode_payload_v5(Payload(), DictionaryState()) == b""
    assert encode_payload_v5(None, DictionaryState()) == b""


def test_should_reset_dictionary_at_capacity():
    assert should_reset_dictionary(None) is True
    state = DictionaryState()
    assert should_reset_dictionary(state) is False


def test_counter_rounding_goes_away_from_zero_not_to_even():
    assert [round_away_from_zero(v) for v in (0.5, 1.5, 2.5, 3.5)] == [1, 2, 3, 4]
    assert round_away_from_zero(float("nan")) == 0


def test_integral_samples_render_without_a_trailing_zero():
    assert format_sample(2048.0) == "2048"
    assert format_sample(12.5) == "12.5"
    assert format_sample(float("inf")) == "0"


def test_analyze_reports_the_shape_of_an_encoded_body():
    state = DictionaryState()
    body = encode_payload_v5(_payload(counters=[CounterEvent("a", 1, ["x=1"], 10)]), state)
    diagnostics = analyze_payload_v5(body, state)
    assert diagnostics.dictionary_session == state.session_id
    assert diagnostics.dictionary_revision == 1
    assert diagnostics.series_definitions == 1
    assert diagnostics.event_lines == 1


def test_samples_render_the_same_whether_passed_as_int_or_float():
    """int.is_integer() only exists from Python 3.12, so ints must be coerced."""
    assert format_sample(2048) == format_sample(2048.0) == "2048"
    assert format_sample(0) == "0"
    assert round_away_from_zero(3) == 3
