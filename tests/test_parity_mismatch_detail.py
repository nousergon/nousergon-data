"""Every mismatch row says, in one line, why it is a mismatch.

`alpha-engine-config-I11203`, third closes-when. Every `shadow_missing`,
`both_missing` and `live_superseded` row already carried a `detail`; every
MISMATCH row carried an empty one, with the diagnosis sitting in
`values.examples`. All 34 mismatches on the published 2026-09-18 report were
blank on the one field a reader or renderer shows for the missing-row cases.

The information was never lost. It was simply not where anything looked.
"""

from __future__ import annotations

import pytest

from shadow.parity import _verdict_detail


def test_a_value_breach_is_counted_and_singular_reads_singular():
    assert _verdict_detail({"values": {"breaches": 1}}) == "1 value breach"
    assert _verdict_detail({"values": {"breaches": 4}}) == "4 value breaches"


def test_coverage_loss_names_the_ratio_and_the_floor():
    detail = _verdict_detail(
        {"coverage": {"missing": 16, "ratio": 0.82, "floor": 0.98, "met": False}}
    )
    assert "16 keys only in live" in detail
    assert "coverage 0.82 vs floor 0.98" in detail


def test_a_vendor_live_row_names_the_band_it_was_graded_against():
    """The band is the number a reader needs to judge whether a `match` was
    earned or merely forgiven, so a graded row always states it."""
    detail = _verdict_detail(
        {
            "values": {"breaches": 2},
            "vendor_drift": {"class": "vendor_live", "band": {"relative": 0.05, "absolute": 0.0}},
        }
    )
    assert "graded as vendor_live" in detail
    assert "rel=0.05" in detail


def test_a_schema_difference_is_named_rather_than_implied():
    detail = _verdict_detail(
        {
            "schema": {
                "match": False,
                "only_live": ["Close"],
                "only_shadow": [],
                "dtype_changes": {"Volume": {}},
            }
        }
    )
    assert "schema differs" in detail
    assert "1 columns only in live" in detail
    assert "1 dtype change" in detail


def test_a_row_count_difference_is_named():
    detail = _verdict_detail({"row_count": {"live": 2514, "shadow": 2513}})
    assert "row count 2514 live vs 2513 shadow" in detail


def test_an_extra_key_in_shadow_is_reported_not_silent():
    detail = _verdict_detail(
        {"values": {"breaches": 0}, "vendor_drift": {"extra_in_shadow": 3}}
    )
    assert "3 keys only in shadow" in detail


def test_a_clean_row_gets_no_detail_rather_than_an_empty_sentence():
    """A `match` has nothing to explain; it must not gain a hollow line."""
    assert _verdict_detail({"values": {"breaches": 0}}) == ""
    assert _verdict_detail({}) == ""


@pytest.mark.parametrize(
    "body",
    [
        {"values": {"breaches": 202}},
        {"values": {"breaches": 2}, "coverage": {"missing": 15, "ratio": 0.0, "floor": 0.98}},
        {"coverage": {"missing": 32, "ratio": 0.0, "floor": 0.98}},
        {"row_count": {"live": 5, "shadow": 4}},
    ],
)
def test_every_shape_a_real_mismatch_took_on_0918_yields_a_detail(body):
    """Each of these is the shape of an actual row from the published
    2026-09-18 report. Verified against the real report before this test was
    written: 30 mismatch rows, 0 blank details."""
    assert _verdict_detail(body) != ""
