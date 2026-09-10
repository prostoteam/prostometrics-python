"""Telling a client key from a server key, and reporting a refusal that never resolved."""

from __future__ import annotations

import json

from conftest import FakeClock
from prostometrics._api_key import looks_like_client_key, refusal_hint

_UNAUTHORIZED = json.dumps({"code": "unauthorized", "message": "unauthorized"}).encode()


def test_a_client_key_is_recognised_by_its_marker():
    assert looks_like_client_key("1_J790Vup6NxClhwkr5IirkbsSYLy") is False
    assert looks_like_client_key("1_pk_CILo8eLaRH7lP-iPiN3wxMtltpWAu0kt") is True
    assert looks_like_client_key("  42_pk_abc  ") is True
    assert looks_like_client_key("") is False
    assert refusal_hint("1_J790Vup6") == ""
    assert "client key" in refusal_hint("1_pk_abc")


def test_a_client_key_skips_the_grace_and_is_named_as_the_mistake(manual_client, ingester, caplog):
    """Waiting cannot make a client key work, so the accurate error comes at once."""
    client = manual_client(api_key="12_pk_browserkey")

    ingester.enqueue(401, _UNAUTHORIZED)
    with caplog.at_level("ERROR", logger="prostometrics"):
        client.count("requests", 1)
        client._tick()

    assert client.stats().ingest_disabled is True
    assert "ingest disabled" in caplog.text
    assert "this is a client key" in caplog.text
    assert "API key not accepted yet" not in caplog.text


def test_close_reports_a_refusal_that_was_still_unresolved(manual_client, ingester, caplog):
    """A short-lived process must not exit on the reassuring "retrying" line alone."""
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue_many(20, 401, _UNAUTHORIZED)
    client.count("requests", 1)
    client._tick()
    assert client._auth_refusal_pending is True

    with caplog.at_level("WARNING", logger="prostometrics"):
        client.close(timeout=0.0)

    assert "shutting down while the API key was still refused" in caplog.text


def test_close_stays_quiet_once_a_refused_key_is_accepted(manual_client, ingester, caplog):
    clock = FakeClock()
    clock.install()
    client = manual_client()

    ingester.enqueue(401, _UNAUTHORIZED)
    client.count("requests", 1)
    client._tick()
    assert client._auth_refusal_pending is True

    clock.advance(3)
    client._tick()
    assert client._auth_refusal_pending is False

    with caplog.at_level("WARNING", logger="prostometrics"):
        client.close(timeout=0.0)

    assert "shutting down while the API key was still refused" not in caplog.text
