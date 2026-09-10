"""Series identity within a dictionary session."""

from __future__ import annotations

from collections.abc import Sequence


class SeriesDefinition:
    __slots__ = ("labels", "metric")

    def __init__(self, metric: str, labels: Sequence[str]) -> None:
        self.metric = metric
        self.labels = list(labels)


def series_key(metric: str, labels: Sequence[str]) -> str:
    """Build the cache key identifying one metric + label combination."""
    if not labels:
        return metric + "\0"
    return metric + "\0" + "\0".join(labels) + "\0"
