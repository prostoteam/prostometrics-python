from __future__ import annotations

from prostometrics._constants import MAX_VALUE_EVENTS_PER_SECOND
from prostometrics._value_rate_limiter import ValueRateLimiter


def test_caps_samples_within_one_wall_clock_second():
    limiter = ValueRateLimiter()
    assert all(limiter.allow(1_000) for _ in range(MAX_VALUE_EVENTS_PER_SECOND))
    assert limiter.allow(1_000) is False
    assert limiter.allow(1_999) is False


def test_allowance_resets_in_the_next_second():
    limiter = ValueRateLimiter()
    for _ in range(MAX_VALUE_EVENTS_PER_SECOND):
        limiter.allow(1_000)
    assert limiter.allow(2_000) is True
