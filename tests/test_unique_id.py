from __future__ import annotations

import pytest

from prostometrics._unique_id import canonical_unique_id


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0"),
        (42, "42"),
        (2**64 - 1, str(2**64 - 1)),
        ("123", "123"),
        (" 123 ", "123"),
        (b"123", "123"),
        (bytearray(b"7"), "7"),
    ],
)
def test_accepts_ids_within_the_unsigned_64_bit_range(value, expected):
    assert canonical_unique_id(value) == expected


@pytest.mark.parametrize("value", [-1, 2**64, "", "abc", "-5", "1.5", None, 1.5, True, b"\xff\xfe", object()])
def test_rejects_ids_outside_the_protocol(value):
    assert canonical_unique_id(value) is None
