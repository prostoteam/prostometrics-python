"""Canonical encoding of unique-metric identifiers.

Python integers are arbitrary precision, so the only question is whether the
value fits the protocol's unsigned 64-bit range.
"""

from __future__ import annotations

from typing import Optional

from ._constants import MAX_UNIQUE_ID


def canonical_unique_id(value: object) -> Optional[str]:
    """Return the decimal form of a unique ID, or ``None`` if it is unusable."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return _in_range(value)
    if isinstance(value, str):
        return _from_text(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return _from_text(bytes(value).decode("utf-8"))
        except UnicodeDecodeError:
            return None
    return None


def _from_text(raw: str) -> Optional[str]:
    text = raw.strip()
    if not text or not text.isdigit() or not text.isascii():
        return None
    return _in_range(int(text))


def _in_range(value: int) -> Optional[str]:
    if value < 0 or value > MAX_UNIQUE_ID:
        return None
    return str(value)
