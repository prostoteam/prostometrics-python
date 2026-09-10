"""Workload name validation.

The workload scopes every metric in a batch and is sent as the ``X-PM-Workload``
header, so an invalid one would fail the whole request at the server.
"""

from __future__ import annotations

from ._constants import WORKLOAD_MAX_LEN
from ._errors import InvalidWorkloadError

_ALLOWED_EXTRA = frozenset(".-_/")


def validate_workload(workload: str) -> str:
    """Return the trimmed workload, raising ``InvalidWorkloadError`` if unusable."""
    trimmed = workload.strip()
    if not trimmed:
        raise InvalidWorkloadError("must not be empty")
    if len(trimmed.encode("utf-8")) > WORKLOAD_MAX_LEN:
        raise InvalidWorkloadError(f"must be at most {WORKLOAD_MAX_LEN} bytes")
    for char in trimmed:
        if not (char.isascii() and (char.isalnum() or char in _ALLOWED_EXTRA)):
            raise InvalidWorkloadError("may contain only ASCII letters, digits and . - _ /")
    return trimmed
