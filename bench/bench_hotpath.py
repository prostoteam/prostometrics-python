"""Measures the cost of a metric call on the caller's thread.

The claim this library makes is that a call inside a hot loop costs almost
nothing, so the claim is measured rather than asserted.

The budget is expressed as a multiple of a reference workload measured on the
same machine, not in microseconds. An absolute budget would only describe the
machine that set it: a shared CI runner is roughly twice as slow as a developer
laptop, so a fixed microsecond gate either passes everything locally or fails
everything in CI.

The reference allocates, because that is what actually differs between machines
here. A no-op method call was tried first and measured within 0.5% on both a
laptop and a CI runner while the real work differed twofold — pure interpreter
dispatch says nothing about how fast a machine builds the dicts, sets and
strings that recording a metric spends its time on.

The worker thread is parked and the queue is cleared before each repeat, so what
is timed is the enqueue path itself. Without that, a long run overflows the
queue and ends up timing the much cheaper drop path instead. Run it with:

    python bench/bench_hotpath.py
"""

from __future__ import annotations

import sys
import timeit
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prostometrics import Client
from prostometrics._payload import Payload

ITERATIONS = 20_000
REPEATS = 20

# Measured at roughly 9x on an Apple-silicon laptop. The ceiling leaves room for
# hardware that scales the two measurements differently, while still catching a
# change that makes recording materially more expensive.
BUDGET_RATIO = 14.0

_SETUP = "client._queue.clear(); client._value_rate_limiter.reset()"


class DiscardingTransport:
    """Accepts everything instantly, so only client-side cost is measured."""

    def send(self, payload: Payload, workload: str, timeout: float) -> None:
        return

    def reset_after_fork(self) -> None:
        return

    def close(self) -> None:
        return


def reference_workload() -> object:
    """The calibration workload.

    Deliberately shaped like the work recording a metric does — allocate a few
    small containers, format a couple of strings, sort — without being any of
    the code under test.
    """
    tokens = []
    seen = set()
    for name, value in (("method", "GET"), ("status", "200")):
        seen.add(name)
        tokens.append(f"{name}={value}")
    tokens.sort()
    return {"metric": "m", "labels": tokens, "seen": seen}


CASES = [
    ("count, no labels", 'client.count("m", 1)'),
    ("count, one keyword label", 'client.count("m", 1, method="GET")'),
    ("count, two keyword labels", 'client.count("m", 1, method="GET", status="200")'),
    ("count, one string label", 'client.count("m", 1, "method=GET")'),
    ("value, one keyword label", 'client.value("m", 1.5, route="/login")'),
    ("count_unique", 'client.count_unique(7, "m")'),
    ("count_top", 'client.count_top(7, "m", "articles/2026/09/how-to-measure-things")'),
]


def _park_worker(client: Client) -> None:
    client._closing = True
    client._wake.set()
    worker = client._worker
    if worker is not None:
        worker.join(timeout=5)
    client._closing = False


def _best_us(statement: str, scope: dict, setup: str = "") -> float:
    timer = timeit.Timer(statement, setup=setup, globals=scope)
    samples = timer.repeat(repeat=REPEATS, number=ITERATIONS)
    return min(samples) / ITERATIONS * 1_000_000


def main() -> int:
    client = Client("bench", transport=DiscardingTransport())
    _park_worker(client)
    scope = {"client": client, "reference_workload": reference_workload}
    try:
        reference_us = _best_us("reference_workload()", scope)
        print(f"calibration: the reference workload costs {reference_us:.4f} us here\n")

        results = {}
        for label, statement in CASES:
            per_call = _best_us(statement, scope, _SETUP)
            ratio = per_call / reference_us
            results[label] = ratio
            print(f"{label:<28} {per_call:6.3f} us  = {ratio:6.1f}x reference")

        stats = client.stats()
        if stats.queue_dropped or stats.rate_limited_dropped or stats.invalid_dropped:
            print(f"\nINVALID RUN: measured a rejection path ({stats})")
            return 2
    finally:
        client._closed = True

    worst_label, worst = max(results.items(), key=lambda item: item[1])
    print(f"\nbudget {BUDGET_RATIO:.0f}x; worst is {worst_label!r} at {worst:.1f}x")
    if worst > BUDGET_RATIO:
        print("OVER BUDGET")
        return 1
    print("within budget")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
