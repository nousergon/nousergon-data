"""A settling bar forgives a MOVING NUMBER, never an identity — and a JSON series can declare one.

`alpha-engine-config-I11551`, two halves.

1. `market_data/eod_closes/{date}.json` declares a `key_date` settling bar. On
   2026-09-21/22/23 it graded `match` while the shadow's EU rows carried
   `bar_date` D-1 against v1's D, because the `key_date` basis absorbed EVERY
   value diff of the document, the identity fields included. That is how
   alpha-engine-config-I11548 stayed hidden on the dated key. Under `key_date`
   and `row_date` only a numeric move settles; `bar_date`, a currency, a label
   are graded strictly on the trading day too.

2. `market_data/close_history/consolidated.json` (D21) is a series document:
   `$.series.<sym>` is a list of `[date, close]` rows. Its day-D rows moved on
   57-60% of series each day while every earlier row matched exactly — the
   provisional-bar signature price_cache already handles with a `row_date`
   basis. A JSON document has no row index, so the contract now DECLARES
   where its row dates are (`x-settling-bar.row_dates`), and the comparator
   grades exactly the rows both sides date the trading day as settling, and
   re-grades them strictly on the next report (`prior_day`).
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pytest

from shadow import parity

TRADING_DAY = dt.date(2026, 9, 23)
PRIOR_DAY = dt.date(2026, 9, 22)
EOD_KEY = "market_data/eod_closes/2026-09-23.json"
CONSOLIDATED_KEY = "market_data/close_history/consolidated.json"


def _json_compare(live: dict, shadow: dict, key: str, *, prior_day=None) -> dict:
    return parity.compare_bytes(
        key,
        json.dumps(live).encode("utf-8"),
        json.dumps(shadow).encode("utf-8"),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(key),
        trading_day=TRADING_DAY,
        prior_day=prior_day,
    )


def _closes(**rows: tuple[str, float]) -> dict:
    return {
        "schema_version": 1,
        "as_of": TRADING_DAY.isoformat(),
        "source": "test",
        "closes": {
            ticker.replace("_", "."): {"close": close, "currency": "EUR", "bar_date": bar_date}
            for ticker, (bar_date, close) in rows.items()
        },
    }


# ---------------------------------------------------------------------------
# 1. key_date: identity fields stay strict
# ---------------------------------------------------------------------------


def test_bar_date_drift_inside_a_key_date_settling_key_grades_mismatch():
    """The I11548 shape, exactly: the shadow's EU row is yesterday's bar."""
    live = _closes(NOVN_SW=("2026-09-23", 117.60), AAPL=("2026-09-23", 230.00))
    shadow = _closes(NOVN_SW=("2026-09-22", 116.46), AAPL=("2026-09-23", 230.00))
    body = _json_compare(live, shadow, EOD_KEY)
    assert body["settling_bar"]["basis"] == "key_date"
    assert body["verdict"] == "mismatch"
    assert any("bar_date" in example for example in body["values"]["examples"])
    # The close moved 0.97%, outside the 0.005 band — and IS a settling cell:
    # the price is what the vendor revises. Only the identity field is strict.
    assert body["settling_bar"]["cells"] == 1
    assert "NOVN.SW.close" in body["settling_bar"]["examples"][0]["path"]


def test_a_close_only_move_inside_a_key_date_settling_key_still_grades_match():
    """The forgiveness the key_date basis exists for is untouched."""
    live = _closes(NOVN_SW=("2026-09-23", 117.60))
    shadow = _closes(NOVN_SW=("2026-09-23", 116.46))
    body = _json_compare(live, shadow, EOD_KEY)
    assert body["verdict"] == "match"
    assert body["settling_bar"]["cells"] == 1


def test_a_currency_diff_inside_a_key_date_settling_key_grades_mismatch():
    live = _closes(NOVN_SW=("2026-09-23", 117.60))
    shadow = _closes(NOVN_SW=("2026-09-23", 117.60))
    shadow["closes"]["NOVN.SW"]["currency"] = "CHF"
    body = _json_compare(live, shadow, EOD_KEY)
    assert body["verdict"] == "mismatch"
    assert body["settling_bar"]["cells"] == 0


def test_a_same_day_snapshot_keeps_absorbing_every_value_diff():
    """Out of I11551's scope on purpose: a same-day snapshot's WHOLE surface is
    a derivation of the unsettled bar, labels included (I11351's ruling)."""
    body = _json_compare(
        {"ratings": {"A": "BUY"}}, {"ratings": {"A": "HOLD"}}, "market_data/technicals/latest.json"
    )
    assert body["settling_bar"]["basis"] == "same_day_snapshot"
    assert body["verdict"] == "match"


def _parquet(frame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


def test_a_non_numeric_cell_of_a_key_date_frame_is_strict():
    """The frame comparator follows the same rule: `staging/daily_closes`'s
    `date` column naming D-1 on one side is an identity breach, while its
    `Close` move is settling."""
    import pandas as pd

    key = "staging/daily_closes/2026-09-23.parquet"

    def frame(date: str, close: float):
        return pd.DataFrame(
            {"ticker": ["NOVN.SW"], "date": [date], "Close": [close], "Volume": [10.0]}
        )

    body = parity.compare_bytes(
        key,
        _parquet(frame("2026-09-23", 117.60)),
        _parquet(frame("2026-09-22", 116.46)),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(key),
        trading_day=TRADING_DAY,
    )
    assert body["settling_bar"]["basis"] == "key_date"
    assert body["verdict"] == "mismatch"
    assert [e["column"] for e in body["values"]["examples"]] == ["date"]
    assert body["settling_bar"]["cells"] == 1


# ---------------------------------------------------------------------------
# 2. row_date on a JSON series document
# ---------------------------------------------------------------------------


def _history(**series: list[list]) -> dict:
    return {
        "schema_version": 1,
        "series": {sym.replace("_", "."): rows for sym, rows in series.items()},
        "currency": {sym.replace("_", "."): "USD" for sym in series},
    }


_EARLIER = [["2026-09-21", 170.00], ["2026-09-22", 175.00]]


def test_the_contract_declares_row_dates_on_consolidated_only():
    contract = parity.resolve_contract(CONSOLIDATED_KEY)
    assert contract.settling_basis == "row_date"
    assert contract.settling_row_dates.declared == "$.series.*[*][0]"
    # The per-symbol key shares the contract but is NOT named in row_dates, so
    # it carries no settling bar at all — the red default.
    per_symbol = parity.resolve_contract("market_data/close_history/SIZE.json")
    assert per_symbol.settling_basis == ""
    assert per_symbol.settling_row_dates is None


def test_a_series_differing_only_on_its_trading_day_row_is_a_match_with_a_settling_block():
    """The measured 2026-09-23 SIZE row: 176.660004 v1 vs 175.580002 shadow
    (0.61%, outside the 0.005 band)."""
    live = _history(SIZE=_EARLIER + [["2026-09-23", 176.660004]], AAPL=_EARLIER + [["2026-09-23", 1.0]])
    shadow = _history(SIZE=_EARLIER + [["2026-09-23", 175.580002]], AAPL=_EARLIER + [["2026-09-23", 1.0]])
    body = _json_compare(live, shadow, CONSOLIDATED_KEY)
    assert body["verdict"] == "match"
    assert body["values"]["breaches"] == 0
    assert body["settling_bar"]["basis"] == "row_date"
    assert body["settling_bar"]["cells"] == 1
    assert body["settling_bar"]["examples"][0]["path"] == "$.series.SIZE[2][1]"
    assert "settling_bar_declaration_problem" not in body


def test_an_earlier_row_differing_is_still_a_mismatch():
    live = _history(SIZE=[["2026-09-21", 170.0], ["2026-09-22", 175.0], ["2026-09-23", 176.66]])
    shadow = _history(SIZE=[["2026-09-21", 170.0], ["2026-09-22", 171.0], ["2026-09-23", 176.66]])
    body = _json_compare(live, shadow, CONSOLIDATED_KEY)
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1
    assert body["settling_bar"]["cells"] == 0


def test_a_series_missing_its_trading_day_row_on_one_side_is_still_a_mismatch():
    """The I11548 shape on this key: the shadow's EU series ends a day early.
    Membership is not relaxed — the length diff is graded exactly."""
    live = _history(NOVN_SW=_EARLIER + [["2026-09-23", 117.6]])
    shadow = _history(NOVN_SW=list(_EARLIER))
    body = _json_compare(live, shadow, CONSOLIDATED_KEY)
    assert body["verdict"] == "mismatch"
    assert "length 3 live vs 2 shadow" in body["values"]["examples"][0]
    assert body["settling_bar"]["cells"] == 0


def test_a_row_the_two_sides_date_differently_is_never_settling():
    """Same length, last row dated D on one side and D-1 on the other: the date
    cell is an identity breach and the close beside it is NOT forgiven, since
    the row is not the trading day's on both sides."""
    live = _history(SIZE=[["2026-09-21", 170.0], ["2026-09-23", 176.66]])
    shadow = _history(SIZE=[["2026-09-21", 170.0], ["2026-09-22", 175.0]])
    body = _json_compare(live, shadow, CONSOLIDATED_KEY)
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 2
    assert body["settling_bar"]["cells"] == 0


def test_the_next_report_regrades_the_settled_row_strictly():
    """On D+1 the row that was settling on D is ordinary history: graded on
    the strict band, and rolled up by `grade_prior_day_settled`."""
    live = _history(SIZE=_EARLIER + [["2026-09-23", 176.66]])
    settled = _json_compare(live, live, CONSOLIDATED_KEY, prior_day=PRIOR_DAY)
    assert settled["prior_day"] == {
        "date": "2026-09-22",
        "rows_compared": 1,
        "breaches": 0,
        "settled": True,
    }

    shadow = _history(SIZE=[["2026-09-21", 170.0], ["2026-09-22", 171.0], ["2026-09-23", 176.66]])
    unsettled = _json_compare(live, shadow, CONSOLIDATED_KEY, prior_day=PRIOR_DAY)
    assert unsettled["prior_day"]["settled"] is False
    assert unsettled["prior_day"]["breaches"] == 1

    class _Store:
        def get_bytes(self, key: str) -> bytes:
            assert key == parity.parity_key(PRIOR_DAY)
            return json.dumps(
                {
                    "keys": [
                        {
                            "key": CONSOLIDATED_KEY,
                            "settling_bar": {"basis": "row_date", "date": "2026-09-22", "cells": 1},
                        }
                    ]
                }
            ).encode("utf-8")

    for body, outcome in ((settled, (1, 0)), (unsettled, (0, 1))):
        block = parity.grade_prior_day_settled(
            _Store(),
            trading_day=TRADING_DAY,
            prior_day=PRIOR_DAY,
            rows=[parity.KeyResult(CONSOLIDATED_KEY, ["D21"], body["verdict"], "json", body)],
        )
        assert (block["settled"], block["unsettled"]) == outcome


# ---------------------------------------------------------------------------
# The declaration is validated at load, never at grading time
# ---------------------------------------------------------------------------


def _load(tmp_path, monkeypatch, settling: dict, patterns) -> tuple:
    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "c.schema.json").write_text(
        json.dumps({"x-key-pattern": patterns, "x-settling-bar": settling}), encoding="utf-8"
    )
    monkeypatch.setattr(parity, "_CONTRACTS_DIR", contracts)
    parity._load_contract_schemas.cache_clear()
    try:
        return parity._load_contract_schemas()
    finally:
        parity._load_contract_schemas.cache_clear()


@pytest.mark.parametrize(
    ("settling", "message"),
    [
        (
            {"basis": "key_date", "rationale": "r", "row_dates": {"a/{date}.json": "$.s.*[*][0]"}},
            "only meaningful with basis 'row_date'",
        ),
        (
            {"basis": "row_date", "rationale": "r", "row_dates": {"not/a/pattern.json": "$.s.*[*][0]"}},
            "not one of this contract's x-key-pattern",
        ),
        (
            {"basis": "row_date", "rationale": "r", "row_dates": {"a/{date}.json": "$.s.*[0]"}},
            "is not of the form",
        ),
        (
            {"basis": "row_date", "rationale": "r", "row_dates": {"b/{sym}.parquet": "$.s.*[*][0]"}},
            "not a JSON key",
        ),
    ],
)
def test_a_malformed_row_dates_declaration_is_refused(tmp_path, monkeypatch, settling, message):
    with pytest.raises(ValueError, match=message):
        _load(tmp_path, monkeypatch, settling, ["a/{date}.json", "b/{sym}.parquet"])


def test_a_json_key_a_row_date_contract_does_not_name_is_graded_strictly(tmp_path, monkeypatch):
    schemas = _load(
        tmp_path,
        monkeypatch,
        {"basis": "row_date", "rationale": "r", "row_dates": {"a/c.json": "$.s.*[*][0]"}},
        ["a/c.json", "a/{sym}.json", "b/{sym}.parquet"],
    )
    by_template = {s.pattern.pattern: s for s in schemas}
    assert [s.settling_basis for s in by_template.values()] == ["row_date", "", "row_date"]
    assert [
        s.settling_row_dates.container if s.settling_row_dates else None for s in by_template.values()
    ] == [("s", "*"), None, None]
