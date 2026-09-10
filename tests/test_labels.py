from __future__ import annotations

import pytest

from prostometrics._labels import build_labels, label, normalize_labels


def test_label_replaces_reserved_characters():
    assert label("route", "/a=b|c") == "route=/a_b_c"
    assert label("we|ird", "v\nalue") == "we_ird=v_alue"


def test_sorts_and_keeps_valid_wire_tokens():
    assert normalize_labels(["status=200", "method=GET"]) == ["method=GET", "status=200"]


def test_trims_surrounding_whitespace():
    assert normalize_labels(["  method=GET  "]) == ["method=GET"]


@pytest.mark.parametrize(
    "labels",
    [
        ["novalue"],
        ["=missingname"],
        ["  =x"],
        ["workload=other"],
        ["method=GET", "method=POST"],
        ["bad|token=1"],
        ["bad\ntoken=1"],
        [""],
        ["x=" + "y" * 600],
        [f"l{index}=v" for index in range(9)],
    ],
)
def test_rejects_unusable_wire_tokens(labels):
    assert normalize_labels(labels) is None


def test_keyword_and_positional_labels_are_merged_and_sorted():
    assert build_labels(("status=200",), {"method": "GET"}) == ["method=GET", "status=200"]


def test_keyword_values_are_sanitized_but_positional_tokens_are_not():
    assert build_labels((), {"route": "/a=b"}) == ["route=/a_b"]
    assert build_labels(("route=/a=b",), {}) == ["route=/a=b"]


def test_non_string_keyword_values_are_accepted():
    assert build_labels((), {"status": 200, "ok": True}) == ["ok=True", "status=200"]


def test_reserved_name_is_rejected_from_either_source():
    assert build_labels((), {"workload": "other"}) is None
    assert build_labels(("workload=other",), {}) is None


def test_a_name_repeated_across_sources_is_rejected():
    assert build_labels(("method=GET",), {"method": "POST"}) is None


def test_the_label_count_limit_spans_both_sources():
    positional = tuple(f"p{index}=v" for index in range(5))
    assert build_labels(positional, {f"k{index}": index for index in range(3)}) is not None
    assert build_labels(positional, {f"k{index}": index for index in range(4)}) is None


def test_an_oversized_keyword_label_is_rejected():
    assert build_labels((), {"key": "v" * 600}) is None


def test_non_ascii_labels_are_measured_in_bytes():
    assert build_labels((), {"город": "Москва"}) == ["город=Москва"]
    assert build_labels((), {"k": "я" * 300}) is None


def test_no_labels_yields_an_empty_list():
    assert build_labels((), {}) == []
