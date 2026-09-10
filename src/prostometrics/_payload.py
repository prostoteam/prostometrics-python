"""In-memory representation of queued events and of one outgoing batch."""

from __future__ import annotations

from collections.abc import Sequence
from typing import List, Optional

COUNTER = "counter"
VALUE = "value"
VALUE_SPARSE = "value_sparse"
SUCCESS = "success"
TOTAL = "total"

# What success() sends: the average of these is the success rate, already as a
# percentage.
SUCCESS_SAMPLE = 100.0
FAILURE_SAMPLE = 0.0
UNIQUE = "unique"
TOP = "top"


def event_wire_size(event: Event) -> int:
    """Roughly what this event costs as a line in the batch body.

    The type letter, the series id, the payload, the timestamp and the
    separators between them. It is an estimate, used only to close a batch
    before it grows past what the ingest endpoint accepts, so it rounds up
    rather than down.
    """
    # t|<series>|<payload>|<seconds>\n, with room for a six-digit series id and
    # an eleven-digit timestamp.
    framing = 1 + 1 + 6 + 1 + 1 + 11 + 1
    if event.type == UNIQUE:
        # Unique ids are decimal digits, so characters and bytes agree.
        return framing + len(event.unique_id or "")
    if event.type == TOP:
        # The item is whatever a visitor touched, and an article name in a
        # non-Latin script is two or three bytes a character. Counting
        # characters would let a batch close at a third of the size the
        # endpoint measures. isascii is a cached flag, so the common case costs
        # nothing and only a non-Latin item is encoded to be measured.
        item = event.item or ""
        item_bytes = len(item) if item.isascii() else len(item.encode("utf-8"))
        return framing + len(event.unique_id or "") + 1 + item_bytes
    # The widest a number is written as. This client encodes with repr(), which
    # picks the shorter of the fixed and exponent forms, so even the extremes
    # are short -- unlike the Go client, whose encoder never uses an exponent
    # and writes 1e-300 as three hundred characters.
    return framing + 26


def definition_wire_size(metric: str, labels: Sequence[str]) -> int:
    """What a series costs the first time a batch mentions it.

    The ``S|`` line naming the metric and every label value. A batch defines
    every series the server's dictionary has not seen, which on a first flush is
    all of them, and those lines can outweigh the events that need them -- a
    metric name plus eight labels is kilobytes where an event line is tens of
    bytes. Counting only events is how a batch reaches several times what the
    endpoint accepts while believing it is in range, and this client has no
    splitter to catch it.
    """
    # S|<series>|<metric>\n, with room for a six-digit series id.
    size = 1 + 1 + 6 + 1 + _utf8_size(metric) + 1
    for label in labels:
        if label:
            size += 1 + _utf8_size(label)
    return size


def _utf8_size(value: str) -> int:
    # isascii is a cached flag, so the common case costs nothing.
    return len(value) if value.isascii() else len(value.encode("utf-8"))


class Event:
    """One recorded metric event, as queued by the caller's thread."""

    __slots__ = ("item", "labels", "metric", "timestamp", "type", "unique_id", "value")

    def __init__(
        self,
        type: str,
        metric: str,
        value: float,
        labels: List[str],
        timestamp: int,
        unique_id: Optional[str] = None,
        item: Optional[str] = None,
    ) -> None:
        self.type = type
        self.metric = metric
        self.value = value
        self.labels = labels
        self.timestamp = timestamp
        self.unique_id = unique_id
        self.item = item


class CounterEvent:
    __slots__ = ("labels", "metric", "timestamp", "value")

    def __init__(self, metric: str, value: float, labels: List[str], timestamp: int) -> None:
        self.metric = metric
        self.value = value
        self.labels = labels
        self.timestamp = timestamp


class ValueEvent:
    __slots__ = ("labels", "metric", "sparse", "success", "timestamp", "value")

    def __init__(
        self,
        metric: str,
        value: float,
        sparse: bool,
        labels: List[str],
        timestamp: int,
        success: bool = False,
    ) -> None:
        self.metric = metric
        self.value = value
        self.sparse = sparse
        # A success outcome: the sample is 100 or 0 and the server presents the
        # metric as a success rate.
        self.success = success
        self.labels = labels
        self.timestamp = timestamp


class UniqueEvent:
    __slots__ = ("labels", "metric", "timestamp", "unique_id")

    def __init__(self, metric: str, unique_id: str, labels: List[str], timestamp: int) -> None:
        self.metric = metric
        self.unique_id = unique_id
        self.labels = labels
        self.timestamp = timestamp


class TopEvent:
    __slots__ = ("item", "labels", "metric", "timestamp", "unique_id")

    def __init__(self, metric: str, unique_id: str, item: str, labels: List[str], timestamp: int) -> None:
        self.metric = metric
        self.unique_id = unique_id
        self.item = item
        self.labels = labels
        self.timestamp = timestamp


class Payload:
    """One batch as it will be encoded and sent."""

    __slots__ = ("batch_id", "counters", "tops", "uniques", "values")

    def __init__(self) -> None:
        self.batch_id = ""
        self.counters: List[CounterEvent] = []
        self.values: List[ValueEvent] = []
        self.uniques: List[UniqueEvent] = []
        self.tops: List[TopEvent] = []

    def is_empty(self) -> bool:
        return not self.counters and not self.values and not self.uniques and not self.tops

    def event_count(self) -> int:
        return len(self.counters) + len(self.values) + len(self.uniques) + len(self.tops)

    def estimated_bytes(self) -> int:
        """Approximate the encoded size, for bounding the outage buffer.

        The estimate is deliberately generous: it bounds memory, so overshooting
        is safe and undershooting is not.
        """
        total = 128 + len(self.batch_id)
        for counter in self.counters:
            total += _series_bytes(counter.metric, counter.labels)
        for value in self.values:
            total += _series_bytes(value.metric, value.labels)
        for unique in self.uniques:
            total += _series_bytes(unique.metric, unique.labels) + len(unique.unique_id)
        for top in self.tops:
            total += _series_bytes(top.metric, top.labels) + len(top.unique_id) + len(top.item.encode("utf-8"))
        return total


def _series_bytes(metric: str, labels: Sequence[str]) -> int:
    total = 64 + len(metric.encode("utf-8"))
    for item in labels:
        total += 16 + len(item.encode("utf-8"))
    return total
