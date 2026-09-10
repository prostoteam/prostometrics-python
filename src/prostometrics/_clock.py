"""Time sources for the client.

Scheduling uses a monotonic clock so that a host clock step cannot stall a
backoff or fire a replay early. Event timestamps use the wall clock, because the
collector buckets datapoints by real event time.

Both are indirected through module-level callables so tests can freeze them.
"""

from __future__ import annotations

import time
from typing import Callable

_monotonic: Callable[[], float] = time.monotonic
_wall: Callable[[], float] = time.time


def monotonic_ms() -> int:
    """Return a monotonic millisecond counter for scheduling decisions."""
    return int(_monotonic() * 1000.0)


def wall_seconds() -> int:
    """Return the current UNIX time in whole seconds, for event timestamps."""
    return int(_wall())


def set_sources(monotonic: Callable[[], float], wall: Callable[[], float]) -> None:
    """Replace both clocks. Test-only."""
    global _monotonic, _wall
    _monotonic = monotonic
    _wall = wall


def reset_sources() -> None:
    """Restore the real clocks. Test-only."""
    global _monotonic, _wall
    _monotonic = time.monotonic
    _wall = time.time
