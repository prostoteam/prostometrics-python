"""Per-second cap on raw value samples.

Value samples are forwarded unaggregated, so an unbounded producer would grow
the queue faster than it can drain. The cap bounds that at the source.
"""

from __future__ import annotations

from ._constants import MAX_VALUE_EVENTS_PER_SECOND


class ValueRateLimiter:
    __slots__ = ("_accepted", "_window_second")

    def __init__(self) -> None:
        self._window_second = -1
        self._accepted = 0

    def allow(self, now_ms: int) -> bool:
        second = now_ms // 1000
        if second < self._window_second:
            return False
        if second > self._window_second:
            self._window_second = second
            self._accepted = 0
        if self._accepted >= MAX_VALUE_EVENTS_PER_SECOND:
            return False
        self._accepted += 1
        return True

    def reset(self) -> None:
        self._window_second = -1
        self._accepted = 0
