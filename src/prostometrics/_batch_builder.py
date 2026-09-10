"""Assembles queued events into one batch.

Two reductions happen here, both of which preserve the server-side meaning of
the stream while cutting network volume: counters sharing a series are summed,
and unique and top-list events repeating an identifier — or an identifier and
item — within the batch are dropped. Value samples are forwarded raw, because
merging them would change what the metric means.
"""

from __future__ import annotations

from typing import Dict, Optional, Set

from ._constants import DEFAULT_MAX_SERIES_PER_BATCH
from ._payload import (
    COUNTER,
    SUCCESS,
    TOP,
    TOTAL,
    UNIQUE,
    VALUE_SPARSE,
    CounterEvent,
    Event,
    Payload,
    TopEvent,
    UniqueEvent,
    ValueEvent,
)
from ._series import series_key


class BatchBuilder:
    __slots__ = ("_counter_index", "_payload", "_top_seen", "_unique_seen")

    def __init__(self) -> None:
        self._payload = Payload()
        self._counter_index: Dict[str, int] = {}
        self._unique_seen: Set[str] = set()
        self._top_seen: Set[str] = set()

    def add(self, event: Event) -> None:
        kind = event.type
        if kind == COUNTER:
            self._add_counter(event)
        elif kind == UNIQUE:
            self._add_unique(event)
        elif kind == TOP:
            self._add_top(event)
        elif kind == TOTAL:
            # Totals are converted to counter deltas before reaching the builder.
            return
        else:
            self._payload.values.append(
                ValueEvent(
                    metric=event.metric,
                    value=event.value,
                    sparse=kind == VALUE_SPARSE,
                    labels=list(event.labels),
                    timestamp=event.timestamp,
                    success=kind == SUCCESS,
                )
            )

    def build(self) -> Optional[Payload]:
        return None if self._payload.is_empty() else self._payload

    def _add_counter(self, event: Event) -> None:
        key = series_key(event.metric, event.labels)
        index = self._counter_index.get(key)
        if index is not None:
            existing = self._payload.counters[index]
            existing.value += event.value
            if event.timestamp > existing.timestamp:
                existing.timestamp = event.timestamp
            return
        position = len(self._payload.counters)
        self._payload.counters.append(
            CounterEvent(
                metric=event.metric,
                value=event.value,
                labels=list(event.labels),
                timestamp=event.timestamp,
            )
        )
        # Past the cap the aggregation map stops growing; later duplicates are
        # sent as separate events rather than letting the map grow unbounded.
        if len(self._counter_index) < DEFAULT_MAX_SERIES_PER_BATCH:
            self._counter_index[key] = position

    def _add_unique(self, event: Event) -> None:
        if not event.unique_id:
            return
        key = f"{series_key(event.metric, event.labels)}\x01{event.unique_id}"
        if key in self._unique_seen:
            return
        self._unique_seen.add(key)
        self._payload.uniques.append(
            UniqueEvent(
                metric=event.metric,
                unique_id=event.unique_id,
                labels=list(event.labels),
                timestamp=event.timestamp,
            )
        )

    def _add_top(self, event: Event) -> None:
        if not event.unique_id or not event.item:
            return
        # One person touching one item twice in a batch is one event. An item
        # cannot contain a control character, so the separator is unambiguous.
        key = f"{series_key(event.metric, event.labels)}\x01{event.unique_id}\x01{event.item}"
        if key in self._top_seen:
            return
        self._top_seen.add(key)
        self._payload.tops.append(
            TopEvent(
                metric=event.metric,
                unique_id=event.unique_id,
                item=event.item,
                labels=list(event.labels),
                timestamp=event.timestamp,
            )
        )
