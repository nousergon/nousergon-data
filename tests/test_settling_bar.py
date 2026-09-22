"""The trading day's own bar is a vendor-state measurement, not a producer defect.

`alpha-engine-config-I11351`. On the first scheduled same-day `shadow-sameday`
run (trading day 2026-09-21) 920 of 933 mismatches were
`reference/price_cache/*.parquet` files whose `row_count`, `schema` and
`symbol_set` all agreed and whose ONLY breaches were one or two cells on
`row == 2026-09-21`: `Volume` in 920/920 files, `Close` in 442, `High`/`Low` in
5. v1 fetched at 16:06 ET, six minutes after the bell; the shadow at 18:41 ET.
The 2,513 history rows agreed inside the 1e-4 `vendor_live` band in every file.

Grading that as `mismatch` says the standalone stack does not reproduce v1,
when the only fact measured is that two fetches two hours apart of a bar the
vendor is still revising differ. So the trading-day cells get their OWN named
surface — recorded, never counted as breaches, and re-graded strictly on the
next day's report.

The four properties this file pins, which are the four ways the fix could go
wrong:

1. a pair differing only on the trading-day row grades `match`, with the
   difference RECORDED rather than dropped;
2. the same pair differing on an EARLIER row still grades `mismatch` — this is
   not a wider band, and `x-vendor-live.band` is untouched;
3. a pair MISSING the trading-day row on one side still grades `mismatch` —
   membership, schema, row count and coverage are not relaxed for any row;
4. `prior_day_settled` reads the previous report and grades both outcomes,
   naming — never hiding — the keys whose shape makes the re-grade impossible.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pytest

from shadow import parity

TRADING_DAY = dt.date(2026, 9, 21)
PRIOR_DAY = dt.date(2026, 9, 18)
PRICE_KEY = "reference/price_cache/A.parquet"


def _frame(rows: list[tuple[str, float, float]]):
    import pandas as pd

    index = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame(
        {"Close": [r[1] for r in rows], "Volume": [r[2] for r in rows]}, index=index
    )


def _parquet(frame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


def _compare(live_rows, shadow_rows, *, key: str = PRICE_KEY, prior_day=None) -> dict:
    return parity.compare_bytes(
        key,
        _parquet(_frame(live_rows)),
        _parquet(_frame(shadow_rows)),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(key),
        trading_day=TRADING_DAY,
        prior_day=prior_day,
    )


_SETTLED_HISTORY = [
    ("2026-09-17", 160.00, 1_000_000.0),
    ("2026-09-18", 161.00, 1_100_000.0),
]


# ---------------------------------------------------------------------------
# The contract declares it; nothing here hand-lists a key
# ---------------------------------------------------------------------------


def test_the_settling_basis_comes_from_the_contract_not_from_this_module():
    assert parity.resolve_contract(PRICE_KEY).settling_basis == "row_date"
    assert (
        parity.resolve_contract("staging/daily_closes/2026-09-21.parquet").settling_basis
        == "key_date"
    )
    assert (
        parity.resolve_contract("market_data/technicals/latest.json").settling_basis
        == "same_day_snapshot"
    )


def test_a_derived_aggregate_is_not_stretched_into_a_settling_bar():
    """`rating_performance.json` is a same-day DERIVATION and deliberately
    carries no `x-settling-bar`: settling-bar forgiveness is declared per key,
    never inferred from "it reads the last bar too"."""
    contract = parity.resolve_contract("market_data/technicals/rating_performance.json")
    assert contract is not None
    assert contract.is_vendor_live
    assert contract.settling_basis == ""


def test_a_key_date_basis_does_not_apply_to_a_key_naming_another_day():
    """`features/metron_supplemental/` carries no date and yesterday's
    `staging/daily_closes` is not today's bar. The forgiveness has to be
    declared AND applicable."""
    contract = parity.resolve_contract("staging/daily_closes/2026-09-18.parquet")
    assert contract.settling_basis == "key_date"
    assert parity.settling_basis("staging/daily_closes/2026-09-18.parquet", contract, TRADING_DAY) == ""
    assert parity.settling_basis("staging/daily_closes/2026-09-21.parquet", contract, TRADING_DAY) == "key_date"


def test_a_comparison_with_no_trading_day_forgives_nothing():
    body = parity.compare_bytes(
        PRICE_KEY,
        _parquet(_frame(_SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)])),
        _parquet(_frame(_SETTLED_HISTORY + [("2026-09-21", 161.94, 2_645_517.0)])),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(PRICE_KEY),
    )
    assert "settling_bar" not in body
    assert body["verdict"] == "mismatch"


def test_a_contract_declaring_a_settling_bar_without_a_basis_is_refused(tmp_path, monkeypatch):
    """A declaration that grades nothing is the state this issue exists to end."""
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "broken.schema.json").write_text(
        json.dumps(
            {
                "x-key-pattern": "some/key/{date}.json",
                "x-settling-bar": {"rationale": "because"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(parity, "_CONTRACTS_DIR", contracts)
    parity._load_contract_schemas.cache_clear()
    try:
        with pytest.raises(ValueError, match="x-settling-bar.basis"):
            parity._load_contract_schemas()
    finally:
        parity._load_contract_schemas.cache_clear()


def test_a_settling_bar_declaration_needs_a_written_rationale(tmp_path, monkeypatch):
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "broken.schema.json").write_text(
        json.dumps({"x-key-pattern": "some/key/{date}.json", "x-settling-bar": {"basis": "key_date"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(parity, "_CONTRACTS_DIR", contracts)
    parity._load_contract_schemas.cache_clear()
    try:
        with pytest.raises(ValueError, match="rationale"):
            parity._load_contract_schemas()
    finally:
        parity._load_contract_schemas.cache_clear()


# ---------------------------------------------------------------------------
# (a) differing only on the trading-day row -> match, with the drift recorded
# ---------------------------------------------------------------------------


def test_a_pair_differing_only_on_the_trading_day_row_is_a_match_with_a_settling_block():
    body = _compare(
        _SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)],
        _SETTLED_HISTORY + [("2026-09-21", 161.94, 2_645_517.0)],
    )
    assert body["verdict"] == "match"
    assert body["values"]["breaches"] == 0
    settling = body["settling_bar"]
    assert settling["basis"] == "row_date"
    assert settling["date"] == "2026-09-21"
    assert settling["cells"] == 2  # Close and Volume, the measured 2026-09-21 shape
    assert settling["max_rel_close"] == pytest.approx(abs(162.03 - 161.94) / 162.03)
    assert settling["max_rel_volume"] == pytest.approx(
        abs(1_870_531.0 - 2_645_517.0) / 1_870_531.0
    )
    assert settling["examples"][0]["row"].startswith("2026-09-21")


def test_the_drift_is_recorded_not_dropped():
    """`match` must not mean `nothing happened`: a reader has to be able to see
    the bar moved ~5 bps on Close and ~40% on Volume."""
    body = _compare(
        _SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)],
        _SETTLED_HISTORY + [("2026-09-21", 161.94, 2_645_517.0)],
    )
    assert "162.03" in json.dumps(body["settling_bar"]["examples"])
    assert "settling-bar cell" in parity._verdict_detail(body)


def test_a_settling_only_key_never_touches_the_vendor_live_band():
    """The band is what absorbs re-derivation noise on SETTLED rows; widening
    it would hide a real 5 bps producer error. It must be unchanged."""
    contract = parity.resolve_contract(PRICE_KEY)
    assert contract.value_band_relative == 1e-4
    body = _compare(
        _SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)],
        _SETTLED_HISTORY + [("2026-09-21", 161.94, 2_645_517.0)],
    )
    assert body["vendor_drift"]["band"] == {"relative": 1e-4, "absolute": 0.0}


# ---------------------------------------------------------------------------
# (b) an EARLIER row still mismatches
# ---------------------------------------------------------------------------


def test_the_same_pair_differing_on_an_earlier_row_still_mismatches():
    body = _compare(
        [("2026-09-17", 160.00, 1_000_000.0), ("2026-09-18", 161.00, 1_100_000.0),
         ("2026-09-21", 162.03, 1_870_531.0)],
        [("2026-09-17", 160.00, 1_000_000.0), ("2026-09-18", 158.00, 1_100_000.0),
         ("2026-09-21", 161.94, 2_645_517.0)],
    )
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["values"]["examples"][0]["row"].startswith("2026-09-18")
    # AND the trading-day cells are still recorded: a key with both a settling
    # block and a strict breach is a mismatch that says why on both counts.
    assert body["settling_bar"]["cells"] == 2


def test_a_drift_just_outside_the_band_on_a_settled_row_is_still_a_breach():
    """1.09e-6 was the measured drift on settled bars and 1e-4 is the declared
    band; 1e-3 is a real producer difference and must survive the settling-bar
    change untouched."""
    body = _compare(
        [("2026-09-18", 100.0, 1.0), ("2026-09-21", 162.03, 1_870_531.0)],
        [("2026-09-18", 100.1, 1.0), ("2026-09-21", 161.94, 2_645_517.0)],
    )
    assert body["verdict"] == "mismatch"


# ---------------------------------------------------------------------------
# (c) membership is NOT relaxed
# ---------------------------------------------------------------------------


def test_a_pair_missing_the_trading_day_row_on_one_side_still_mismatches():
    body = _compare(
        _SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)],
        _SETTLED_HISTORY,
    )
    assert body["verdict"] == "mismatch"
    assert body["coverage"]["met"] is False
    assert body["coverage"]["missing"] == 1
    assert body["row_count"] == {"live": 3, "shadow": 2}


def test_a_schema_difference_on_the_trading_day_row_is_still_a_mismatch():
    import pandas as pd

    live = _frame(_SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)])
    shadow = live.copy()
    shadow["Volume"] = shadow["Volume"].astype("int64")
    body = parity.compare_bytes(
        PRICE_KEY,
        _parquet(live),
        _parquet(shadow),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(PRICE_KEY),
        trading_day=TRADING_DAY,
    )
    assert isinstance(live.index, pd.DatetimeIndex)
    assert body["schema"]["match"] is False
    assert body["verdict"] == "mismatch"


# ---------------------------------------------------------------------------
# The JSON shape: values absorbed, SHAPE graded exactly
# ---------------------------------------------------------------------------


def _json_compare(live: dict, shadow: dict, key: str) -> dict:
    return parity.compare_bytes(
        key,
        json.dumps(live).encode("utf-8"),
        json.dumps(shadow).encode("utf-8"),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(key),
        trading_day=TRADING_DAY,
    )


def test_a_same_day_json_documents_value_diffs_are_settling_but_membership_is_not():
    key = "market_data/eod_closes/2026-09-21.json"
    # Outside the contract's own 0.005 vendor_live band, so the band is not
    # what absorbs it — the settling-bar block is.
    live = {"closes": {"A": {"bar_date": "2026-09-21", "close": 162.03},
                       "B": {"bar_date": "2026-09-21", "close": 10.0}}}
    shadow = {"closes": {"A": {"bar_date": "2026-09-21", "close": 140.00},
                         "B": {"bar_date": "2026-09-21", "close": 10.0}}}
    body = _json_compare(live, shadow, key)
    assert body["verdict"] == "match"
    assert body["settling_bar"]["basis"] == "key_date"
    assert body["settling_bar"]["cells"] == 1
    # `max_rel*` stay null rather than reporting a 0.0 nobody measured: the
    # JSON walker renders a value diff and does not carry the two sides.
    assert body["settling_bar"]["max_rel"] is None

    dropped = {"closes": {"A": {"bar_date": "2026-09-21", "close": 140.00}}}
    missing = _json_compare(live, dropped, key)
    assert missing["verdict"] == "mismatch"
    assert missing["coverage"]["met"] is False


def test_an_undated_same_day_snapshot_absorbs_its_value_diffs():
    key = "market_data/technicals/latest.json"
    body = _json_compare(
        # Outside the contract's own 0.15 band, so the settling-bar block is
        # what absorbs it.
        {"ratings": {"A": 0.81}}, {"ratings": {"A": 0.20}}, key
    )
    assert body["settling_bar"]["basis"] == "same_day_snapshot"
    assert body["settling_bar"]["cells"] == 1
    assert body["verdict"] == "match"


# ---------------------------------------------------------------------------
# (d) prior_day_settled, on both outcomes
# ---------------------------------------------------------------------------


class _Store:
    def __init__(self, documents: dict[str, dict]) -> None:
        self.documents = documents

    def get_bytes(self, key: str) -> bytes:
        if key not in self.documents:
            raise FileNotFoundError(key)
        return json.dumps(self.documents[key]).encode("utf-8")


def _prior_report(*keys: str) -> dict:
    return {
        "schema_version": parity.PARITY_SCHEMA_VERSION,
        "trading_day": PRIOR_DAY.isoformat(),
        "keys": [
            {"key": k, "settling_bar": {"basis": "row_date", "date": PRIOR_DAY.isoformat(), "cells": 2}}
            for k in keys
        ],
    }


def _row(key: str, body: dict) -> parity.KeyResult:
    return parity.KeyResult(key, ["D03"], "match", "parquet", body)


def test_prior_day_settled_grades_a_settled_bar():
    body = _compare(
        _SETTLED_HISTORY + [("2026-09-21", 162.03, 1_870_531.0)],
        _SETTLED_HISTORY + [("2026-09-21", 161.94, 2_645_517.0)],
        prior_day=PRIOR_DAY,
    )
    assert body["prior_day"] == {
        "date": "2026-09-18",
        "rows_compared": 1,
        "breaches": 0,
        "settled": True,
    }
    block = parity.grade_prior_day_settled(
        _Store({parity.parity_key(PRIOR_DAY): _prior_report(PRICE_KEY)}),
        trading_day=TRADING_DAY,
        prior_day=PRIOR_DAY,
        rows=[_row(PRICE_KEY, body)],
    )
    assert block["available"] is True
    assert (block["keys_with_settling_bar"], block["settled"], block["unsettled"]) == (1, 1, 0)


def test_prior_day_settled_grades_a_bar_that_did_not_settle():
    body = _compare(
        [("2026-09-17", 160.0, 1.0), ("2026-09-18", 161.0, 1.0),
         ("2026-09-21", 162.03, 1_870_531.0)],
        [("2026-09-17", 160.0, 1.0), ("2026-09-18", 158.0, 1.0),
         ("2026-09-21", 161.94, 2_645_517.0)],
        prior_day=PRIOR_DAY,
    )
    assert body["prior_day"]["settled"] is False
    assert body["prior_day"]["breaches"] == 1
    block = parity.grade_prior_day_settled(
        _Store({parity.parity_key(PRIOR_DAY): _prior_report(PRICE_KEY)}),
        trading_day=TRADING_DAY,
        prior_day=PRIOR_DAY,
        rows=[_row(PRICE_KEY, body)],
    )
    assert (block["settled"], block["unsettled"]) == (0, 1)
    assert block["unsettled_examples"][0]["key"] == PRICE_KEY


def test_a_key_that_cannot_be_regraded_is_named_never_counted_as_settled():
    """A `row_date` key genuinely missing its previous-day row on either side
    (`no_prior_row`) and a key no longer present in today's report at all
    (`not_in_this_report`) are named, never silently counted as settled. This
    is a `row_date` fixture -- `key_date` (re-graded via `prior_day_settled`,
    alpha-engine-config-I11360) and `same_day_snapshot`
    (`overwritten_in_place`, unregradeable) are pinned in
    `tests/test_shadow_parity.py`."""
    daily = "staging/daily_closes/2026-09-21.parquet"
    block = parity.grade_prior_day_settled(
        _Store({parity.parity_key(PRIOR_DAY): _prior_report(daily, "gone/from/this/report.json")}),
        trading_day=TRADING_DAY,
        prior_day=PRIOR_DAY,
        rows=[_row(daily, {"settling_bar": {"cells": 4}})],
    )
    assert block["settled"] == 0
    assert block["unmeasurable"] == 2
    assert set(block["unmeasurable_reasons"]) == {"no_prior_row", "not_in_this_report"}
    assert daily in block["unmeasurable_reasons"]["no_prior_row"]["examples"]


def test_no_previous_report_says_so_rather_than_reading_as_nothing_was_unsettled():
    block = parity.grade_prior_day_settled(_Store({}), trading_day=TRADING_DAY, prior_day=PRIOR_DAY, rows=[])
    assert block["available"] is False
    assert block["report"] == "parity/2026-09-18.json"
    assert "FileNotFoundError" in block["reason"]


def test_no_store_at_all_is_recorded_as_not_looked():
    block = parity.grade_prior_day_settled(None, trading_day=TRADING_DAY, prior_day=PRIOR_DAY, rows=[])
    assert block["available"] is False
    assert "not read" in block["reason"]
