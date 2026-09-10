from __future__ import annotations

import json

import pytest

from prostometrics._errors import HTTPTransportError, StopIngestError
from prostometrics._payload import CounterEvent, Payload
from prostometrics._transport import HTTPTransport, _parse_retry_after


def _payload(metric: str = "requests", batch_id: str = "batch-1") -> Payload:
    payload = Payload()
    payload.batch_id = batch_id
    payload.counters.append(CounterEvent(metric, 1, ["method=GET"], 1_700_000_000))
    return payload


@pytest.fixture()
def transport(ingester, make_transport) -> HTTPTransport:
    return make_transport(ingester.url, api_key="12_secret")


def test_sends_the_documented_headers(ingester, transport):
    transport.send(_payload(), "billing-api", 2.0)
    request = ingester.requests[0]
    assert request.headers["authorization"] == "12_secret"
    assert request.headers["x-pm-workload"] == "billing-api"
    assert request.headers["x-pm-batch-id"] == "batch-1"
    assert request.headers["content-type"] == "text/plain; charset=utf-8"


def test_reuses_one_connection_across_batches(ingester, transport):
    transport.send(_payload(batch_id="a"), "api", 2.0)
    transport.send(_payload(batch_id="b"), "api", 2.0)
    assert ingester.request_count() == 2
    assert ingester.requests[1].series_lines() == [], "a reused dictionary resends no definitions"


def test_empty_payload_is_not_sent(ingester, transport):
    transport.send(Payload(), "api", 2.0)
    assert ingester.request_count() == 0


def test_unknown_dictionary_triggers_one_full_resync(ingester, transport):
    transport.send(_payload(batch_id="first"), "api", 2.0)
    ingester.enqueue(409, json.dumps({"code": "unknown_series_dictionary"}).encode())
    transport.send(_payload(metric="other", batch_id="second"), "api", 2.0)

    assert ingester.request_count() == 3
    resync = ingester.requests[2]
    assert resync.header_line().endswith("|1"), "a restarted session begins at revision 1"
    assert len(resync.series_lines()) == 1
    assert resync.headers["x-pm-batch-id"] == "second", "the retry must reuse the original batch id"


def test_oversized_body_resets_the_dictionary_and_propagates(ingester, transport):
    transport.send(_payload(batch_id="a"), "api", 2.0)
    ingester.enqueue(413, b'{"error":"too large"}')
    with pytest.raises(HTTPTransportError) as caught:
        transport.send(_payload(batch_id="b"), "api", 2.0)
    assert caught.value.status_code == 413

    transport.send(_payload(batch_id="c"), "api", 2.0)
    assert ingester.requests[-1].series_lines(), "a fresh session must restate its series"


def test_rejected_key_raises_a_stop_error(ingester, transport):
    ingester.enqueue(401, json.dumps({"code": "unauthorized", "message": "unauthorized"}).encode())
    with pytest.raises(StopIngestError) as caught:
        transport.send(_payload(), "api", 2.0)
    assert caught.value.code == 401


def test_unsupported_protocol_version_is_terminal(ingester, transport):
    ingester.enqueue(400, json.dumps({"code": "unsupported_protocol_version"}).encode())
    with pytest.raises(StopIngestError):
        transport.send(_payload(), "api", 2.0)


def test_transient_failure_carries_status_and_retry_after(ingester, transport):
    ingester.enqueue(503, b'{"error":"ingest queue full"}', {"Retry-After": "7"})
    with pytest.raises(HTTPTransportError) as caught:
        transport.send(_payload(), "api", 2.0)
    assert caught.value.status_code == 503
    assert caught.value.retry_after_ms == 7000
    assert caught.value.request_bytes > 0


def test_balance_exhausted_is_reported_with_its_code(ingester, transport):
    ingester.enqueue(429, json.dumps({"code": "balance_exhausted"}).encode(), {"Retry-After": "60"})
    with pytest.raises(HTTPTransportError) as caught:
        transport.send(_payload(), "api", 2.0)
    assert caught.value.response_code == "balance_exhausted"
    assert caught.value.retry_after_ms == 60_000


def test_partial_acceptance_is_logged(ingester, transport, caplog):
    ingester.enqueue(202, b"", {"X-PM-Accepted": "1", "X-PM-Dropped": "2", "X-PM-Rejected": "3"})
    with caplog.at_level("WARNING", logger="prostometrics"):
        transport.send(_payload(), "api", 2.0)
    assert "accepted partial batch" in caplog.text
    assert "dropped=2" in caplog.text


def test_unreachable_endpoint_produces_a_retryable_error_without_a_status(make_transport):
    transport = make_transport("http://127.0.0.1:1/api/i/batch", api_key="12_k")
    with pytest.raises(HTTPTransportError) as caught:
        transport.send(_payload(), "api", 0.5)
    assert caught.value.status_code == 0


def test_repeated_resyncs_warn_about_cache_churn(ingester, transport, caplog):
    conflict = json.dumps({"code": "unknown_series_dictionary"}).encode()
    with caplog.at_level("WARNING", logger="prostometrics"):
        for index in range(3):
            ingester.enqueue(409, conflict)
            transport.send(_payload(batch_id=f"b{index}"), "api", 2.0)
    assert "repeated ingester dictionary resyncs" in caplog.text


@pytest.mark.parametrize(("raw", "expected"), [(None, 0), ("", 0), ("5", 5000), ("0", 0), ("not-a-date", 0)])
def test_retry_after_parsing(raw, expected):
    assert _parse_retry_after(raw) == expected
