"""Bounded FIFO queue that refuses new items when full.

``collections.deque(maxlen=...)`` is deliberately not used: it discards the
oldest item, which would throw away already-recorded history to make room for a
burst. Matching the Go and Node clients, the newest item is refused instead.
"""

from __future__ import annotations

from collections import deque
from typing import Deque, Generic, Optional, TypeVar

T = TypeVar("T")


class RingBuffer(Generic[T]):
    __slots__ = ("_items", "capacity")

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._items: Deque[T] = deque()

    def __len__(self) -> int:
        return len(self._items)

    def push(self, item: T) -> bool:
        """Append an item, returning ``False`` if the buffer is already full."""
        if len(self._items) >= self.capacity:
            return False
        self._items.append(item)
        return True

    def shift(self) -> Optional[T]:
        """Remove and return the oldest item, or ``None`` when empty."""
        if not self._items:
            return None
        return self._items.popleft()

    def clear(self) -> None:
        self._items.clear()
