"""What the client puts on the wire, and how big a batch is allowed to get."""

from __future__ import annotations

import prostometrics
from prostometrics._constants import (
    COMPRESS_MIN_BYTES,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SERIES,
    MAX_TOP_ITEM_BYTES,
)
from prostometrics._payload import Event, definition_wire_size, event_wire_size
from prostometrics._transport import _compress_body


def test_a_large_body_goes_up_compressed_and_says_so(ingester) -> None:
    """The body has to arrive as the batch it stands for, and the header has to
    say what was done to it: a server reading it as plain text would see
    rubbish."""
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(400):
        client.count_top(i, "nginx.pages", f"/guides/how-to-instrument-a-service-{i % 64}")
    client.close(timeout=10)

    assert ingester.requests, "nothing was sent"
    compressed = [request for request in ingester.requests if request.was_compressed]
    assert compressed, "a batch of 400 top events went up uncompressed"
    for request in compressed:
        assert request.body.startswith(b"H|5|s|"), "decompressed body is not a v5 batch"
        assert request.sent_bytes < len(request.body), "compression saved nothing"

    events = sum(request.body.count(b"\nt|") for request in ingester.requests)
    assert events == 400, f"{events} events arrived, want all 400"


def test_a_small_body_is_left_alone(ingester) -> None:
    """Below the threshold the saving is smaller than the request's own headers,
    and it would be paid for with the caller's processor."""
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    client.count("signups", 1)
    client.close(timeout=10)

    assert len(ingester.requests) == 1
    request = ingester.requests[0]
    assert not request.was_compressed, f"a {request.sent_bytes} byte body was compressed"


def test_compress_body_leaves_short_input_as_it_is() -> None:
    short = b"a" * (COMPRESS_MIN_BYTES - 1)
    assert _compress_body(short) is short

    long = b"H|5|s|session|1\nc|1|1|1730000000\n" * 200
    compressed = _compress_body(long)
    assert compressed is not long
    assert len(compressed) < len(long)


def test_no_body_passes_the_ceiling_however_long_the_items(ingester) -> None:
    """Counting only events would let a few thousand long top-list items build a
    body the endpoint refuses whole, so the batch closes on size as well."""
    item = "p" * MAX_TOP_ITEM_BYTES
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(3000):
        client.count_top(i, "nginx.pages", item)
    client.close(timeout=30)

    assert len(ingester.requests) >= 2, (
        f"3000 items of {MAX_TOP_ITEM_BYTES} bytes went in {len(ingester.requests)} body"
    )
    for request in ingester.requests:
        assert len(request.body) <= 265 * 1024, f"a body of {len(request.body)} bytes passed the endpoint's ceiling"

    events = sum(request.body.count(b"\nt|") for request in ingester.requests)
    assert events == 3000, f"{events} events arrived, want all 3000"


def test_wire_size_grows_with_what_the_line_carries() -> None:
    plain = Event(type="counter", metric="signups", value=1, labels=[], timestamp=1730000000)
    short_item = Event(
        type="top", metric="nginx.pages", value=0, labels=[], timestamp=1730000000, unique_id="7", item="/a"
    )
    long_item = Event(
        type="top",
        metric="nginx.pages",
        value=0,
        labels=[],
        timestamp=1730000000,
        unique_id="7",
        item="p" * MAX_TOP_ITEM_BYTES,
    )

    assert event_wire_size(long_item) > event_wire_size(short_item) > 0
    assert event_wire_size(plain) > 0
    # The estimate is what stops a batch outgrowing the endpoint's ceiling, so
    # it must not undercount the widest line the protocol allows.
    assert event_wire_size(long_item) >= MAX_TOP_ITEM_BYTES
    assert DEFAULT_MAX_BATCH_BYTES // event_wire_size(long_item) < 1000


def test_an_item_is_measured_in_bytes_not_characters(ingester) -> None:
    """An article name in a non-Latin script is two bytes a character, so
    counting characters would let a batch close at half the size the endpoint
    measures."""
    # 128 Cyrillic characters is 256 bytes, the largest item allowed.
    item = "страница" * 16
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(3000):
        client.count_top(i, "nginx.pages", item)
    client.close(timeout=30)

    for request in ingester.requests:
        assert len(request.body) <= 265 * 1024, f"a body of {len(request.body)} bytes passed the endpoint's ceiling"
    events = sum(request.body.count(b"\nt|") for request in ingester.requests)
    assert events == 3000, f"{events} events arrived, want all 3000"


def test_a_batch_closes_before_it_defines_more_series_than_allowed(ingester) -> None:
    """The endpoint refuses a batch defining more series than it allows, and a
    first flush defines every series it touches."""
    series = 1500
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(series):
        client.count(f"metric.{i}", 1)
    client.close(timeout=30)

    total = 0
    for request in ingester.requests:
        definitions = request.body.count(b"\nS|")
        assert definitions <= DEFAULT_MAX_BATCH_SERIES, (
            f"a request defines {definitions} series, past the {DEFAULT_MAX_BATCH_SERIES} the endpoint allows"
        )
        total += definitions
    assert total == series, f"{total} series definitions arrived, want all {series}"


def test_stays_inside_the_endpoint_ceiling_when_definitions_are_heavy(ingester) -> None:
    """A definition carries the metric name and every label value into the same
    body as the events that need it, and on a first flush there is one per
    series. This client has no splitter, so counting only event lines is the
    difference between a batch that arrives and one refused whole."""
    heavy = "v" * 200
    labels = {key: heavy for key in ("a", "b", "c", "d", "e", "f", "g", "h")}

    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(1000):
        client.count(f"metric.{i}", 1, **labels)
    client.close(timeout=60)

    ceiling = 265 * 1024
    for request in ingester.requests:
        assert len(request.body) <= ceiling, (
            f"a body of {len(request.body)} bytes passed the {ceiling} the endpoint accepts"
        )
    events = sum(request.body.count(b"\nc|") for request in ingester.requests)
    assert events == 1000, f"{events} events arrived, want all 1000"


def test_events_that_do_not_fit_are_kept_for_the_next_flush(ingester) -> None:
    """A flush takes more from the queue than one batch may carry. What does not
    fit has to go back at the head, in order -- dropping it would lose events the
    caller successfully recorded."""
    item = "p" * MAX_TOP_ITEM_BYTES
    client = prostometrics.init("api", api_key="1_secret", endpoint=ingester.url)
    for i in range(3000):
        client.count_top(i, "nginx.pages", item)
    client.close(timeout=60)

    events = sum(request.body.count(b"\nt|") for request in ingester.requests)
    assert events == 3000, f"{events} of 3000 events arrived; the rest were dropped, not deferred"


def test_definition_wire_size_counts_the_labels(ingester=None) -> None:
    bare = definition_wire_size("metric", [])
    labelled = definition_wire_size("metric", ["route=/orders/:id", "status=200"])
    assert labelled > bare
    # Non-Latin label values are two or three bytes a character.
    assert definition_wire_size("metric", ["город=Москва"]) > definition_wire_size("metric", ["city=Moscow"])
