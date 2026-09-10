from __future__ import annotations

import pytest

from prostometrics._constants import MAX_TOP_ITEM_BYTES
from prostometrics._top_item import valid_top_item


@pytest.mark.parametrize(
    "item",
    [
        "article-17",
        "a|b|c",
        " spaced ",
        "ёж/17",
        "caf\u00e9\u00a0menu",
        "x" * MAX_TOP_ITEM_BYTES,
        "я" * (MAX_TOP_ITEM_BYTES // 2),
    ],
    ids=[
        "plain",
        "pipes",
        "surrounding spaces",
        "non-latin",
        "non-breaking space",
        "longest ascii",
        "longest two-byte",
    ],
)
def test_accepts_what_the_server_accepts(item):
    assert valid_top_item(item) is True


@pytest.mark.parametrize(
    "item",
    [
        "",
        "x" * (MAX_TOP_ITEM_BYTES + 1),
        "я" * (MAX_TOP_ITEM_BYTES // 2 + 1),
        "bad\x01item",
        "line\nbreak",
        "tab\tbed",
        "del\x7fete",
        "\udcff",
        42,
        None,
        b"bytes",
    ],
    ids=[
        "empty",
        "one byte over",
        "one two-byte character over",
        "control character",
        "newline",
        "tab",
        "delete",
        "lone surrogate",
        "int",
        "none",
        "bytes",
    ],
)
def test_rejects_what_the_server_rejects(item):
    assert valid_top_item(item) is False
