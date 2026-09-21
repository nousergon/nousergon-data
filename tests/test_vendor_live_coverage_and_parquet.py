"""The vendor_live class graded against what the 2026-09-18 parity run measured.

`alpha-engine-config-I11203`. The class landed on 2026-09-20 and the very next
run said it had barely worked: of 34 mismatches it cleared 4, not the 33 that
was predicted. Three defects, all in the implementation rather than the ruling:

1.  COVERAGE DENOMINATOR. `denominator = row_count.live` is the document's
    TOP-LEVEL key count -- 3 for `{schema_version, as_of, earnings}` -- while
    the membership diffs it subtracted live at `$.earnings.<ticker>`, ~900 of
    them. `covered = max(3 - 16, 0) = 0` made the ratio 0.0, so
    `market_data/earnings/latest.json` and `market_data/sectors/latest.json`
    mismatched with ZERO value breaches.
2.  PARQUET. The class was implemented in the JSON path only, so every
    `reference/price_cache/*.parquet` row kept failing on ~1e-6 re-derivation
    drift with no contract able to reach it.
3.  DUPLICATE ROWS. Two keys appeared twice, so `summary.total` read 959 for
    957 distinct keys.

The fixtures here are deliberately NESTED, with an entity count DIFFERENT from
the top-level key count. The tests that missed defect 1 used flat 100-key
dicts, where the top-level count happens to equal the entity count -- they
agreed with the wrong premise instead of testing it.
"""

from __future__ import annotations

import io
import json

import pandas as pd
import pytest

from shadow.parity import (
    ContractSchema,
    JsonDiff,
    KeyResult,
    _dedupe_rows,
    _json_diffs,
    _key_pattern_regex,
    compare_bytes,
    resolve_contract,
)

TICKERS = [f"T{i:03d}" for i in range(900)]


def _vendor_live_contract(*, floor: float = 0.98, relative: float = 0.05) -> ContractSchema:
    return ContractSchema(
        path=None,
        provenance_fields=frozenset(),
        pattern=_key_pattern_regex("anything/{x}.json"),
        comparison_class="vendor_live",
        value_band_relative=relative,
        value_band_absolute=0.0,
        coverage_floor=floor,
    )


def _nested(entities: dict[str, float]) -> bytes:
    """A document shaped like the real ones: 3 top-level fields, N entities.

    The gap between `len(doc)` (3) and `len(doc["earnings"])` (N) is the whole
    point of this fixture.
    """
    return json.dumps(
        {"schema_version": 2, "as_of": "2026-09-18", "earnings": entities}
    ).encode()


# ---------------------------------------------------------------------------
# Defect 1 — the coverage denominator
# ---------------------------------------------------------------------------


def test_coverage_denominator_is_the_container_not_the_document():
    live = _nested({t: 1.0 for t in TICKERS})
    shadow = _nested({t: 1.0 for t in TICKERS[16:]})  # 16 tickers missing

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(floor=0.98),
    )

    coverage = body["coverage"]
    assert coverage["denominator"] == 900, "the entity container, not the 3 top-level fields"
    assert coverage["denominator"] != body["row_count"]["live"], (
        "the regression this test exists for: row_count.live is 3 here"
    )
    assert coverage["missing"] == 16
    assert coverage["ratio"] == pytest.approx(884 / 900)
    assert coverage["met"] is True
    assert body["verdict"] == "match"


def test_coverage_below_the_floor_still_mismatches():
    live = _nested({t: 1.0 for t in TICKERS})
    shadow = _nested({t: 1.0 for t in TICKERS[100:]})  # 100 missing, 0.888 < 0.98

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(floor=0.98),
    )

    assert body["coverage"]["ratio"] == pytest.approx(800 / 900)
    assert body["coverage"]["met"] is False
    assert body["verdict"] == "mismatch"
    # The floor is what failed it, NOT a value breach.
    assert body["values"]["breaches"] == 0


def test_zero_missing_keys_reports_the_document_as_its_basis():
    live = _nested({t: 1.0 for t in TICKERS})

    body = compare_bytes(
        "anything/x.json", live, live, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(),
    )

    assert body["coverage"]["ratio"] == 1.0
    assert body["coverage"]["missing"] == 0
    assert "no missing keys" in body["coverage"]["denominator_basis"]
    assert body["verdict"] == "match"


def test_an_extra_key_in_shadow_is_not_a_coverage_loss():
    live = _nested({t: 1.0 for t in TICKERS})
    extra = {t: 1.0 for t in TICKERS}
    extra["ZZZZ"] = 1.0
    shadow = _nested(extra)

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(floor=1.0),
    )

    assert body["coverage"]["missing"] == 0
    assert body["coverage"]["ratio"] == 1.0
    assert body["vendor_drift"]["extra_in_shadow"] == 1
    assert body["verdict"] == "match"


def test_the_denominator_sums_the_distinct_containers_involved():
    live = json.dumps(
        {"schema_version": 2, "sectors": {"A": 1, "B": 2}, "countries": {t: 1 for t in TICKERS}}
    ).encode()
    shadow = json.dumps(
        {"schema_version": 2, "sectors": {"A": 1}, "countries": {t: 1 for t in TICKERS[2:]}}
    ).encode()

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(floor=0.5),
    )

    assert body["coverage"]["denominator"] == 902  # 2 sectors + 900 countries
    assert body["coverage"]["missing"] == 3
    basis = body["coverage"]["denominator_basis"]
    assert "$.sectors(2)" in basis and "$.countries(900)" in basis


def test_a_value_outside_the_band_still_mismatches_whatever_coverage_says():
    live = _nested({t: 100.0 for t in TICKERS})
    shadow = _nested({t: (100.0 if t != "T005" else 130.0) for t in TICKERS})

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(relative=0.05),
    )

    assert body["coverage"]["met"] is True
    assert body["values"]["breaches"] == 1
    assert body["verdict"] == "mismatch"


def test_a_cardinality_diff_is_never_forgiven():
    live = json.dumps({"series": {"DGS10": [1, 2, 3]}}).encode()
    shadow = json.dumps({"series": {"DGS10": [1, 2]}}).encode()

    body = compare_bytes(
        "anything/x.json", live, shadow, rel=1e-6, absolute=1e-9,
        contract=_vendor_live_contract(relative=0.5),
    )

    assert body["values"]["breaches"] == 1
    assert body["verdict"] == "mismatch"


# ---------------------------------------------------------------------------
# The structured walker
# ---------------------------------------------------------------------------


def test_json_diffs_carry_the_container_they_came_from():
    diffs = _json_diffs(
        {"earnings": {"A": 1, "B": 2, "C": 3}}, {"earnings": {"A": 1}}, 1e-6, 1e-9
    )

    membership = [d for d in diffs if d.kind == "membership"]
    assert len(membership) == 2
    for diff in membership:
        assert diff.parent_path == "$.earnings"
        assert diff.parent_size_live == 3
        assert diff.side == "live"


def test_a_key_only_in_shadow_is_sided_shadow():
    (diff,) = _json_diffs({"a": {}}, {"a": {}, "b": 1}, 1e-6, 1e-9)
    assert isinstance(diff, JsonDiff)
    assert diff.kind == "membership" and diff.side == "shadow"
    assert diff.rendered == "$.b: only in shadow"


# ---------------------------------------------------------------------------
# Defect 2 — the parquet comparator
# ---------------------------------------------------------------------------


def _frame(values: dict[str, list[float]], dates: list[str]) -> bytes:
    frame = pd.DataFrame(values, index=pd.to_datetime(dates))
    frame.index.name = "Date"
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


DATES = ["2016-11-01", "2016-12-07", "2017-01-03"]


def _price_contract(*, floor: float = 1.0, relative: float = 1e-4) -> ContractSchema:
    return ContractSchema(
        path=None,
        provenance_fields=frozenset(),
        pattern=_key_pattern_regex("reference/price_cache/{sym}.parquet"),
        comparison_class="vendor_live",
        value_band_relative=relative,
        value_band_absolute=0.0,
        coverage_floor=floor,
    )


def test_parquet_re_derivation_drift_is_absorbed_by_the_declared_band():
    # The MEASURED drift from the 2026-09-18 report: O.parquet, 2016-11-01
    # Open, 1.09e-6 relative -- just outside the 1e-6 global default.
    live = _frame({"Open": [35.804776688248914, 33.28735369691274, 40.0]}, DATES)
    shadow = _frame({"Open": [35.80473766258676, 33.28739887063123, 40.0]}, DATES)

    body = compare_bytes(
        "reference/price_cache/O.parquet", live, shadow,
        rel=1e-6, absolute=1e-9, contract=_price_contract(),
    )

    assert body["comparator"] == "parquet"
    assert body["values"]["breaches"] == 0
    assert body["coverage"]["ratio"] == 1.0
    assert body["verdict"] == "match"


def test_the_same_parquet_without_a_contract_still_mismatches():
    """The band is not a global loosening: `contract=None` keeps 1e-6."""
    live = _frame({"Open": [35.804776688248914, 33.28735369691274, 40.0]}, DATES)
    shadow = _frame({"Open": [35.80473766258676, 33.28739887063123, 40.0]}, DATES)

    body = compare_bytes(
        "reference/price_cache/O.parquet", live, shadow,
        rel=1e-6, absolute=1e-9, contract=None,
    )

    assert body["values"]["breaches"] > 0
    assert body["verdict"] == "mismatch"
    assert "coverage" not in body


def test_a_parquet_value_outside_the_band_is_still_a_breach():
    live = _frame({"Open": [35.80, 33.28, 40.0]}, DATES)
    shadow = _frame({"Open": [35.80, 33.28, 41.0]}, DATES)

    body = compare_bytes(
        "reference/price_cache/O.parquet", live, shadow,
        rel=1e-6, absolute=1e-9, contract=_price_contract(),
    )

    assert body["values"]["breaches"] == 1
    assert body["verdict"] == "mismatch"


def test_a_parquet_column_change_is_graded_exactly():
    """Shape is never forgiven, whatever the band says."""
    live = _frame({"Open": [1.0, 2.0, 3.0], "Close": [1.0, 2.0, 3.0]}, DATES)
    shadow = _frame({"Open": [1.0, 2.0, 3.0]}, DATES)

    body = compare_bytes(
        "reference/price_cache/O.parquet", live, shadow,
        rel=1e-6, absolute=1e-9, contract=_price_contract(relative=0.9),
    )

    assert body["schema"]["match"] is False
    assert body["verdict"] == "mismatch"


def test_a_missing_parquet_row_fails_the_coverage_floor():
    live = _frame({"Open": [1.0, 2.0, 3.0]}, DATES)
    shadow = _frame({"Open": [1.0, 2.0]}, DATES[:2])

    body = compare_bytes(
        "reference/price_cache/O.parquet", live, shadow,
        rel=1e-6, absolute=1e-9, contract=_price_contract(floor=1.0),
    )

    assert body["coverage"]["missing"] == 1
    assert body["coverage"]["met"] is False
    assert body["verdict"] == "mismatch"


# ---------------------------------------------------------------------------
# Defect 3 — one row per key
# ---------------------------------------------------------------------------


def test_duplicate_rows_are_merged_and_attributed_to_both_units():
    rows = [
        KeyResult("market_data/close_history/consolidated.json", ["D14"], "mismatch", "json", {}),
        KeyResult("market_data/other.json", ["D22"], "match", "json", {}),
        KeyResult("market_data/close_history/consolidated.json", ["D02"], "mismatch", "json", {}),
    ]

    deduped = _dedupe_rows(rows)

    assert len(deduped) == 2
    assert [row.key for row in deduped] == [
        "market_data/close_history/consolidated.json",
        "market_data/other.json",
    ]
    assert deduped[0].unit_ids == ["D02", "D14"]


def test_a_verdict_conflict_between_duplicate_rows_is_recorded_not_hidden():
    rows = [
        KeyResult("a.json", ["D1"], "match", "json", {}),
        KeyResult("a.json", ["D2"], "mismatch", "json", {}),
    ]

    (row,) = _dedupe_rows(rows)

    assert row.verdict == "match"
    assert "duplicate_verdict_conflict" in row.body


# ---------------------------------------------------------------------------
# The real contracts these fixes are for
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key",
    ["reference/price_cache/ADC.parquet", "reference/price_cache/SPY.parquet"],
)
def test_the_price_cache_contract_resolves_and_declares_vendor_live(key):
    contract = resolve_contract(key)
    assert contract is not None, "every price_cache parquet was `cls= -` before this"
    assert contract.is_vendor_live
    assert contract.value_band_relative == pytest.approx(1e-4)
    assert contract.coverage_floor == 1.0


def test_the_macro_contract_declares_its_class():
    contract = resolve_contract("market_data/macro/latest.json")
    assert contract is not None and contract.is_vendor_live


def test_rating_performances_wall_clock_stamp_is_provenance_not_data():
    contract = resolve_contract("market_data/technicals/rating_performance.json")
    assert contract is not None
    assert "as_of_utc" in contract.provenance_fields

    live = json.dumps({"as_of_utc": "2026-09-18T20:15:30Z", "horizons": [1]}).encode()
    shadow = json.dumps({"as_of_utc": "2026-09-20T18:42:23Z", "horizons": [1]}).encode()
    body = compare_bytes(
        "market_data/technicals/rating_performance.json", live, shadow,
        rel=1e-6, absolute=1e-9, contract=contract,
    )

    assert body["values"]["breaches"] == 0
    assert body["provenance_diffs"]["count"] == 1
    assert body["verdict"] == "match"
