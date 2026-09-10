"""Label construction, validation and normalization.

Labels travel as ``name=value`` tokens. The client validates them before
queueing rather than relying on the server to ignore malformed tokens, and it
sorts them so that the same set of labels always maps to one stored series
regardless of the order the caller passed them in.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Dict, List, Optional, Set

from ._constants import MAX_LABEL_BYTES, MAX_LABELS_PER_SERIES, RESERVED_LABEL_NAME

_FORBIDDEN = ("=", "|", "\n", "\r")


def label(name: str, value: str) -> str:
    """Build a ``name=value`` token, replacing protocol delimiters with ``_``.

    Use this when either half is assembled at runtime and may contain characters
    the wire format reserves.
    """
    return f"{_sanitize(name)}={_sanitize(value)}"


def _sanitize(raw: object) -> str:
    # Scanning for each reserved character and replacing only on a hit is
    # measurably faster than str.translate for the short strings labels use.
    text = str(raw)
    for char in _FORBIDDEN:
        if char in text:
            text = text.replace(char, "_")
    return text


# The server rejects a name containing any byte below 0x20 or 0x7f. Matching
# that exactly matters in both directions: a looser check lets through a name
# the server refuses, and a stricter one — str.isprintable(), for instance —
# would drop legitimate values over characters such as a non-breaking space.
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")


def is_wire_safe(text: str) -> bool:
    """Report whether a string is safe to place in the line protocol.

    The protocol reserves ``|`` as a field delimiter and forbids control
    characters outright, so both are checked here rather than left for the
    server to reject — a name the server refuses fails the request, and that
    failure is not retryable.
    """
    return "|" not in text and _CONTROL_CHARACTERS.search(text) is None


def _too_long(token: str) -> bool:
    """Report whether a token exceeds the label byte limit.

    Character count bounds byte count from below, so the encode is only needed
    once a non-ASCII token is short enough to still be in doubt.
    """
    if len(token) > MAX_LABEL_BYTES:
        return True
    return not token.isascii() and len(token.encode("utf-8")) > MAX_LABEL_BYTES


def build_labels(positional: Sequence[str], keywords: Dict[str, object]) -> Optional[List[str]]:
    """Validate and sort the labels for one event, or return ``None`` to drop it.

    Positional tokens are wire tokens the caller wrote, so they are validated
    and rejected on any problem. Keyword labels were not written by hand, so
    reserved characters in them are replaced rather than treated as a mistake.

    Returning ``None`` drops the whole event. A label the server would refuse or
    silently ignore makes the resulting series wrong, so it is better to report
    the drop locally than to store something the caller did not mean.

    Positional and keyword labels are handled in one pass because this runs on
    the caller's thread for every recorded metric.
    """
    tokens: List[str] = []
    seen: Set[str] = set()

    for raw in positional:
        token = str(raw).strip()
        if not token or _too_long(token) or not is_wire_safe(token):
            return None
        name, separator, _ = token.partition("=")
        name = name.strip()
        if not separator or not name or name == RESERVED_LABEL_NAME or name in seen:
            return None
        seen.add(name)
        tokens.append(token)

    for raw_name, raw_value in keywords.items():
        name = _sanitize(raw_name).strip()
        if not name or name == RESERVED_LABEL_NAME or name in seen:
            return None
        token = f"{name}={_sanitize(raw_value)}"
        if _too_long(token) or not is_wire_safe(token):
            return None
        seen.add(name)
        tokens.append(token)

    if len(tokens) > MAX_LABELS_PER_SERIES:
        return None
    tokens.sort()
    return tokens


def normalize_labels(labels: Iterable[str]) -> Optional[List[str]]:
    """Validate and sort wire tokens only. See :func:`build_labels`."""
    return build_labels(list(labels), {})
