from __future__ import annotations

import logging
import threading
import time

import pytest

import prostometrics
from prostometrics import Client
from prostometrics._constants import (
    DEFAULT_MAX_TOTAL_SERIES,
    DEFAULT_QUEUE_SIZE,
    MAX_METRIC_BYTES,
    MAX_SAMPLE_VALUE,
    MAX_TOP_ITEM_BYTES,
)


@pytest.fixture()
def client(ingester):
    instance = Client("billing-api", api_key="12_secret", endpoint=ingester.url)
    try:
        yield instance
    finally:
        if not instance.closed:
            instance.close(timeout=3.0)


def _flush(client, ingester, expected: int = 1) -> None:
    client._wake.set()
    assert ingester.wait_for_requests(expected), f"expected {expected} request(s), saw {ingester.request_count()}"


def test_counter_reaches_the_wire(client, ingester):
    client.count("requests", 1, method="GET")
    _flush(client, ingester)
    request = ingester.requests[0]
    assert request.series_lines() == ["S|0|requests|method=GET"]
    assert request.event_lines()[0].startswith("c|0|1|")


def test_counters_on_one_series_are_summed_into_a_single_event(client, ingester):
    for _ in range(5):
        client.count("requests", 2, method="GET")
    _flush(client, ingester)
    events = ingester.all_event_lines()
    assert len(events) == 1
    assert events[0].split("|")[2] == "10"


def test_labels_are_sorted_so_call_order_cannot_split_a_series(client, ingester):
    client.count("requests", 1, status="200", method="GET")
    client.count("requests", 1, method="GET", status="200")
    _flush(client, ingester)
    assert ingester.requests[0].series_lines() == ["S|0|requests|method=GET|status=200"]
    assert len(ingester.all_event_lines()) == 1


def test_positional_and_keyword_labels_mix(client, ingester):
    client.count("requests", 1, "method=GET", status="200")
    _flush(client, ingester)
    assert ingester.requests[0].series_lines() == ["S|0|requests|method=GET|status=200"]


def test_every_metric_kind_uses_its_protocol_marker(client, ingester):
    client.count("c_metric", 1)
    client.value("v_metric", 1.5)
    client.value_sparse("s_metric", 2.5)
    client.success("o_metric", True)
    client.count_unique(99, "u_metric")
    client.count_top(99, "t_metric", "item")
    _flush(client, ingester)
    markers = sorted(line.split("|")[0] for line in ingester.all_event_lines())
    assert markers == ["c", "o", "s", "t", "u", "v"]


def test_success_sends_a_hundred_or_zero(client, ingester):
    client.success("payment", True, provider="stripe")
    client.success("payment", False, provider="stripe")
    _flush(client, ingester)
    assert ingester.requests[0].series_lines() == ["S|0|payment|provider=stripe"]
    values = [line.split("|")[2] for line in ingester.all_event_lines()]
    assert values == ["100", "0"]


def test_repeated_unique_ids_are_deduplicated(client, ingester):
    for _ in range(4):
        client.count_unique(7, "users.dau")
    client.count_unique(8, "users.dau")
    _flush(client, ingester)
    assert len(ingester.all_event_lines()) == 2


def test_top_reaches_the_wire_with_its_item_verbatim(client, ingester):
    client.count_top(4242, "article_reads", "a|b| c ")
    _flush(client, ingester)
    request = ingester.requests[0]
    assert request.series_lines() == ["S|0|article_reads"], "a top series carries only the metric name"
    marker, series, unique_id, _timestamp, item = request.event_lines()[0].split("|", 4)
    assert (marker, series, unique_id, item) == ("t", "0", "4242", "a|b| c ")


def test_repeated_top_items_are_deduplicated(client, ingester):
    for _ in range(4):
        client.count_top(7, "article_reads", "a")
    client.count_top(7, "article_reads", "b")
    client.count_top(8, "article_reads", "a")
    _flush(client, ingester)
    assert len(ingester.all_event_lines()) == 3


def test_total_reports_deltas_and_ignores_its_first_reading(client, ingester):
    client.total("bytes_sent_kb", 100, iface="eth0")
    _flush(client, ingester, expected=0)
    client._wake.set()
    client.total("bytes_sent_kb", 250, iface="eth0")
    client.close(timeout=3.0)
    events = ingester.all_event_lines()
    assert len(events) == 1
    assert events[0].split("|")[2] == "150"


def test_total_treats_a_lower_reading_as_a_source_restart(client, ingester):
    client.total("bytes", 100)
    client._wake.set()
    client.total("bytes", 10)
    client._wake.set()
    client.total("bytes", 30)
    client.close(timeout=3.0)
    values = [line.split("|")[2] for line in ingester.all_event_lines()]
    assert values == ["20"], "the restart rebases instead of emitting a negative delta"


def test_close_delivers_what_is_queued(client, ingester):
    client.count("requests", 3)
    client.close(timeout=3.0)
    assert ingester.request_count() == 1
    assert ingester.all_event_lines()[0].split("|")[2] == "3"


def test_metrics_after_close_are_ignored(client, ingester):
    client.close(timeout=3.0)
    before = ingester.request_count()
    client.count("requests", 1)
    assert ingester.request_count() == before


def test_close_is_idempotent(client, ingester):
    client.close(timeout=3.0)
    client.close(timeout=3.0)
    assert client.closed is True


def test_context_manager_closes_on_exit(ingester):
    with Client("batch-job", api_key="12_k", endpoint=ingester.url) as client:
        client.count("rows.imported", 7)
    assert ingester.request_count() == 1


@pytest.mark.parametrize(
    ("metric", "reason"),
    [("", "empty"), ("   ", "whitespace"), ("a" * (MAX_METRIC_BYTES + 1), "too long"), ("pipe|name", "delimiter")],
)
def test_invalid_metric_names_are_dropped_locally(client, ingester, metric, reason):
    client.count(metric, 1)
    client.close(timeout=3.0)
    assert ingester.request_count() == 0, reason
    assert client.stats().invalid_dropped >= 1


@pytest.mark.parametrize("labels", [{"workload": "other"}, {"a": "x" * 600}])
def test_invalid_labels_drop_the_event(client, ingester, labels):
    client.count("requests", 1, **labels)
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped >= 1


def test_too_many_labels_drop_the_event(client, ingester):
    client.count("requests", 1, **{f"l{index}": index for index in range(9)})
    client.close(timeout=3.0)
    assert ingester.request_count() == 0


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), MAX_SAMPLE_VALUE + 1])
def test_out_of_range_samples_are_dropped(client, ingester, value):
    client.value("latency_ms", value)
    client.close(timeout=3.0)
    assert ingester.request_count() == 0


def test_invalid_unique_id_is_dropped_without_raising(client, ingester):
    client.count_unique("not-a-number", "users.dau")
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped >= 1


@pytest.mark.parametrize(
    "item",
    ["", "x" * (MAX_TOP_ITEM_BYTES + 1), "bad\x01item", "line\nbreak", "\udcff", 42],
    ids=["empty", "too long", "control character", "newline", "lone surrogate", "not text"],
)
def test_invalid_top_item_is_dropped_without_raising(client, ingester, item):
    client.count_top(7, "article_reads", item)
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped == 1


def test_invalid_unique_id_on_a_top_is_dropped_without_raising(client, ingester):
    client.count_top("not-a-number", "article_reads", "a")
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped == 1


def test_a_refused_top_item_is_named_in_the_drop_warning(client, caplog):
    with caplog.at_level(logging.WARNING, logger="prostometrics"):
        client.count_top(7, "article_reads", "line\nbreak")
    assert "invalid_top_item" in caplog.text
    assert "article_reads" in caplog.text


def test_metric_calls_never_raise(client):
    for call in (
        lambda: client.count("m", object()),  # type: ignore[arg-type]
        lambda: client.value("m", "not-a-number"),  # type: ignore[arg-type]
        lambda: client.count(None),  # type: ignore[arg-type]
        lambda: client.count_unique(object(), "m"),
        lambda: client.count_top(object(), "m", "x"),
        lambda: client.count_top(1, "m", object()),  # type: ignore[arg-type]
        lambda: client.count_top(1, None, "x"),  # type: ignore[arg-type]
    ):
        call()


def test_queue_full_drops_the_newest_and_counts_it(manual_client):
    """The worker is parked, so nothing drains the queue while it is filled."""
    client = manual_client()
    for index in range(DEFAULT_QUEUE_SIZE):
        client.count("requests", 1, index=str(index))

    assert client.stats().queue_depth == DEFAULT_QUEUE_SIZE
    client.count("one_too_many", 1)

    stats = client.stats()
    assert stats.queue_dropped == 1
    assert stats.queue_depth == DEFAULT_QUEUE_SIZE, "the newest event is refused, not the oldest evicted"


def test_stats_reports_queue_depth(client):
    client.count("a", 1)
    client.count("b", 1)
    assert client.stats().queue_depth == 2


def test_verbose_logging_is_debug_level(ingester, caplog):
    with (
        caplog.at_level(logging.DEBUG, logger="prostometrics"),
        Client("api", api_key="12_k", endpoint=ingester.url, verbose=True) as client,
    ):
        client.count("requests", 1)
    assert "client version" in caplog.text


def test_silent_suppresses_all_logging(ingester, caplog):
    with (
        caplog.at_level(logging.DEBUG, logger="prostometrics"),
        Client("api", api_key="12_k", endpoint=ingester.url, silent=True) as client,
    ):
        client.count("", 1)
    assert caplog.text == ""


def test_worker_thread_is_named_and_daemonic(client):
    worker = client._worker
    assert worker is not None
    assert worker.daemon is True
    assert "prostometrics" in worker.name


def test_concurrent_producers_lose_nothing(client, ingester):
    def produce():
        for _ in range(200):
            client.count("requests", 1, thread="shared")

    threads = [threading.Thread(target=produce) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    client.close(timeout=5.0)

    total = sum(int(line.split("|")[2]) for line in ingester.all_event_lines())
    assert total == 800


def test_module_level_functions_use_the_client_from_init(ingester):
    client = prostometrics.init("api", api_key="12_k", endpoint=ingester.url)
    try:
        prostometrics.count("requests", 2, method="GET")
        prostometrics.value("latency_ms", 3.5)
        prostometrics.value_sparse("capacity", 4.0)
        prostometrics.success("payment", True)
        prostometrics.count_unique(11, "dau")
        prostometrics.count_top(11, "reads", "a")
        prostometrics.total("bytes", 5)
        assert prostometrics.default() is client
    finally:
        client.close(timeout=3.0)
    assert len(ingester.all_event_lines()) == 6


def test_module_level_functions_are_safe_before_init():
    import prostometrics as module

    previous = module._default_client
    module._default_client = None
    try:
        module.count("requests", 1)
        module.value("latency", 1.0)
        module.count_unique(1, "dau")
        module.count_top(1, "reads", "a")
    finally:
        module._default_client = previous


def test_control_characters_in_a_metric_name_are_rejected(client, ingester):
    """The protocol forbids them, and a name the server refuses fails the batch."""
    for metric in ("request\tcount", "request\x00count", "request\x1bcount", "pipe|name"):
        client.count(metric, 1)
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped == 4


def test_control_characters_in_a_label_are_rejected(client, ingester):
    client.count("requests", 1, route="/a\tb")
    client.close(timeout=3.0)
    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped >= 1


def test_a_cumulative_total_beyond_the_counter_ceiling_still_reports(client, ingester):
    """A long-running total outgrows uint32; only the delta has to fit."""
    client.total("bytes_sent_kb", 10_000_000_000)
    client._wake.set()
    client.total("bytes_sent_kb", 10_000_001_500)
    client.close(timeout=3.0)

    events = ingester.all_event_lines()
    assert len(events) == 1
    assert events[0].split("|")[2] == "1500"


def test_a_total_delta_beyond_the_counter_ceiling_is_dropped_and_counted(client, ingester):
    client.total("bytes", 0)
    client._wake.set()
    client.total("bytes", 5_000_000_000)
    client.close(timeout=3.0)

    assert ingester.request_count() == 0
    assert client.stats().invalid_dropped == 1


def test_exceeding_the_total_series_limit_is_reported(client, ingester, caplog):
    with caplog.at_level(logging.WARNING, logger="prostometrics"):
        for index in range(DEFAULT_MAX_TOTAL_SERIES + 5):
            client.total("bytes", 1, series=str(index))
        client.close(timeout=5.0)

    assert client.stats().invalid_dropped == 5
    assert "total_series_limit" in caplog.text


def test_close_that_times_out_leaves_the_transport_to_the_worker(ingester, caplog):
    """Closing a connection a live worker is using would corrupt it mid-request."""

    class SlowTransport:
        def __init__(self):
            self.closed = False
            self.release = threading.Event()

        def send(self, payload, workload, timeout):
            self.release.wait(5.0)

        def reset_after_fork(self):
            pass

        def close(self):
            self.closed = True

    transport = SlowTransport()
    client = Client("api", transport=transport)
    try:
        client.count("requests", 1)
        client._wake.set()
        time.sleep(0.2)  # let the worker enter send()

        with caplog.at_level(logging.WARNING, logger="prostometrics"):
            client.close(timeout=0.2)

        assert client.closed is True
        assert transport.closed is False, "the worker is still inside send()"
        assert "close timed out" in caplog.text
    finally:
        transport.release.set()
        client._closing = True
        client._closed = True
