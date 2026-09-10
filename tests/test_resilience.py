"""Outage buffering, backoff and the startup authentication grace."""

from __future__ import annotations

import json

from conftest import FakeClock
from prostometrics import _clock
from prostometrics._client import _client_backoff_delay, _retry_delay
from prostometrics._constants import (
    DEFAULT_AUTH_GRACE_WINDOW_MS,
    DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS,
    DEFAULT_RETRY_MAX_DELAY_MS,
)

_UNAUTHORIZED = json.dumps({"code": "unauthorized", "message": "unauthorized"}).encode()
_QUEUE_FULL = b'{"error":"ingest queue full"}'


def test_transient_failure_buffers_the_batch_instead_of_losing_it(manual_client, ingester):
    client = manual_client()
    ingester.enqueue(503, _QUEUE_FULL)
    client.count("requests", 1)
    client._tick()

    assert ingester.request_count() == 1
    stats = client.stats()
    assert stats.buffered_batches == 1
    assert stats.buffered_events == 1
    assert stats.ingest_disabled is False


def test_a_buffered_batch_replays_with_its_original_timestamp_and_batch_id(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue(503, _QUEUE_FULL)
    client.count("requests", 1)
    client._tick()
    first = ingester.requests[0]

    clock.advance(30)
    client._tick()

    assert ingester.request_count() == 2
    replay = ingester.requests[1]
    assert replay.headers["x-pm-batch-id"] == first.headers["x-pm-batch-id"]
    assert replay.event_lines()[0].split("|")[3] == first.event_lines()[0].split("|")[3]
    assert client.stats().buffered_batches == 0


def test_replay_is_paced_to_one_batch_per_cycle(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(3, 503, _QUEUE_FULL)
    for index in range(3):
        client.count(f"metric_{index}", 1)
        client._tick()
        clock.advance(30)

    assert client.stats().buffered_batches == 3
    before = ingester.request_count()
    client._tick()
    assert ingester.request_count() == before + 1, "one replay per cycle"


def test_oldest_buffered_batch_replays_first(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(2, 503, _QUEUE_FULL)
    client.count("first", 1)
    client._tick()
    first_batch_id = ingester.requests[0].headers["x-pm-batch-id"]
    clock.advance(5)
    client.count("second", 1)
    client._tick()
    clock.advance(30)

    client._tick()
    # The metric name is absent from the replay because the dictionary already
    # carries its definition, so the batch id is what identifies the batch.
    assert ingester.requests[-1].headers["x-pm-batch-id"] == first_batch_id


def test_retry_after_delays_the_next_attempt(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue(503, _QUEUE_FULL, {"Retry-After": "120"})
    client.count("requests", 1)
    client._tick()

    clock.advance(60)
    before = ingester.request_count()
    client._tick()
    assert ingester.request_count() == before, "Retry-After must outrank the client's own backoff"

    clock.advance(70)
    client._tick()
    assert ingester.request_count() == before + 1


def test_rejected_key_is_retried_during_the_startup_grace(manual_client, ingester, caplog):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(4, 401, _UNAUTHORIZED)
    with caplog.at_level("WARNING", logger="prostometrics"):
        client.count("requests", 1)
        client._tick()
        assert client.stats().ingest_disabled is False

        for _ in range(3):
            clock.advance(3)
            client._tick()

    assert ingester.request_count() > 1, "the refused batch is retried while the key propagates"
    assert client.stats().ingest_disabled is False
    assert "API key not accepted yet" in caplog.text
    assert caplog.text.count("API key not accepted yet") == 1, "the deferred state is reported once"


def test_rejected_key_after_the_grace_disables_ingest(manual_client, ingester, caplog):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    clock.advance(DEFAULT_AUTH_GRACE_WINDOW_MS / 1000 + 1)
    ingester.enqueue(401, _UNAUTHORIZED)
    with caplog.at_level("ERROR", logger="prostometrics"):
        client.count("requests", 1)
        client._tick()

    assert client.stats().ingest_disabled is True
    assert "ingest disabled" in caplog.text

    before = ingester.request_count()
    client.count("requests", 1)
    client._tick()
    assert ingester.request_count() == before, "a disabled client stops sending entirely"


def test_a_grace_window_costs_a_bounded_number_of_requests(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(60, 401, _UNAUTHORIZED)
    client.count("requests", 1)
    for _ in range(60):
        client._tick()
        clock.advance(1)

    assert client.stats().ingest_disabled is True
    assert ingester.request_count() <= 20, "about one attempt every two seconds for thirty seconds"


def test_a_non_auth_terminal_response_stops_immediately_inside_the_grace(manual_client, ingester):
    client = manual_client()
    ingester.enqueue(400, json.dumps({"code": "unsupported_protocol_version"}).encode())
    client.count("requests", 1)
    client._tick()
    assert client.stats().ingest_disabled is True, "waiting cannot resolve a protocol mismatch"


def test_recovery_clears_the_buffer_and_resets_backoff(manual_client, ingester):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(2, 503, _QUEUE_FULL)
    client.count("a", 1)
    client._tick()
    clock.advance(30)
    client.count("b", 1)
    client._tick()

    clock.advance(30)
    client._tick()
    clock.advance(30)
    client._tick()

    assert client.stats().buffered_batches == 0
    assert client._next_send_attempt_ms == 0
    assert client._transient_backoff_attempt == 0


def test_close_drains_buffered_batches(manual_client, ingester):
    # Real clock: the drain deliberately waits for each batch to become due, so
    # a frozen clock would make it wait forever.
    client = manual_client()

    ingester.enqueue(503, _QUEUE_FULL)
    client.count("requests", 1)
    client._tick()
    assert client.stats().buffered_batches == 1

    client._close_deadline_ms = _clock.monotonic_ms() + 60_000
    client._drain_on_close()
    assert client.stats().buffered_batches == 0
    assert ingester.request_count() == 2


def test_retry_delay_grows_and_is_capped():
    delays = [_retry_delay(attempt) for attempt in range(1, 8)]
    assert delays[0] < delays[2] < DEFAULT_RETRY_MAX_DELAY_MS + 1000
    assert all(delay <= DEFAULT_RETRY_MAX_DELAY_MS + 1000 for delay in delays)


def test_client_backoff_is_capped_and_jittered_widely_on_the_first_failure():
    assert all(_client_backoff_delay(attempt) <= DEFAULT_CLIENT_BACKOFF_MAX_DELAY_MS for attempt in range(1, 12))
    first = {_client_backoff_delay(1) for _ in range(50)}
    assert len(first) > 5, "the first recovery attempt spreads clients across a wide window"


def test_a_retried_batch_resends_the_series_it_introduced(manual_client, ingester):
    """A failed request never reached the server, so its definitions must repeat.

    Without this, the retry references a series the server was never told about
    and the events are rejected while the batch still reports success.
    """
    clock = FakeClock()
    clock.install()
    client = manual_client()

    client.count("first", 1)
    client._tick()
    assert ingester.requests[0].series_lines() == ["S|0|first"]

    ingester.enqueue(503, _QUEUE_FULL)
    client.count("second", 1)
    client._tick()
    failed = ingester.requests[1]
    assert failed.series_lines() == ["S|1|second"]

    clock.advance(30)
    client._tick()

    replay = ingester.requests[2]
    assert replay.headers["x-pm-batch-id"] == failed.headers["x-pm-batch-id"]
    assert replay.series_lines() == ["S|1|second"], "the retry must restate the unacknowledged series"
    assert replay.header_line() == failed.header_line(), "and reuse the same revision"


def test_a_permanently_failed_batch_is_counted_as_lost(manual_client, ingester):
    """A non-retryable response destroys the batch, so stats must say so."""
    client = manual_client()
    ingester.enqueue(400, b'{"error":"malformed batch"}')

    client.count("requests", 1)
    client.count("requests", 1, route="/a")
    client._tick()

    stats = client.stats()
    assert stats.send_dropped == 2, "both events are gone and neither is buffered"
    assert stats.buffered_batches == 0, "a 400 is not retryable"
    assert stats.ingest_disabled is False, "a malformed batch does not disable ingest"


def test_an_unrecognised_conflict_code_is_not_retried(manual_client, ingester):
    client = manual_client()
    ingester.enqueue(409, json.dumps({"code": "some_other_conflict"}).encode())
    client.count("requests", 1)
    client._tick()

    assert client.stats().send_dropped == 1
    assert client.stats().buffered_batches == 0
