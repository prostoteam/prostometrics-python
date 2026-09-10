"""Validation of top-list items.

An item goes on the wire verbatim as the last field of its line, so the client
refuses exactly what the server refuses and alters nothing else: no trimming,
no replacing of reserved characters. A ``|`` is allowed, because the item is
last on the line and needs no escaping.
"""

from __future__ import annotations

import re

from ._constants import MAX_TOP_ITEM_BYTES

# The server's metric-name policy, applied to items: any character below 0x20
# or 0x7f is refused and nothing else is. See ``_labels`` for why the set is
# exactly this one.
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")


def valid_top_item(item: object) -> bool:
    """Report whether ``item`` may be sent as it is.

    Accepted: text that is non-empty, at most ``MAX_TOP_ITEM_BYTES`` of
    well-formed UTF-8, and free of control characters.
    """
    if not isinstance(item, str) or not item or len(item) > MAX_TOP_ITEM_BYTES:
        return False
    if _CONTROL_CHARACTERS.search(item) is not None:
        return False
    if item.isascii():
        return True
    # Character count bounds byte count from below, so only a non-ASCII item
    # short enough to still be in doubt is encoded. A lone surrogate — what
    # ``surrogateescape`` leaves in a path or an argument — has no UTF-8 form
    # and cannot be sent at all.
    try:
        return len(item.encode("utf-8")) <= MAX_TOP_ITEM_BYTES
    except UnicodeEncodeError:
        return False
