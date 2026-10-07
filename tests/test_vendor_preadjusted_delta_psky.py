"""Delta rows a vendor pre-adjusted for a registered split go back on the store's basis.

Live case, 2026-10-05: polygon registered PSKY as a 2-for-1 split executing
10-05 and halved every earlier adjusted bar. The settled re-fetch rewrote the
staged 09-30..10-02 rows at half price while the store held the market's
prices, so the feature load showed a -49% jump on 10-02 that the audit
reported as an un-flattened KNOWN split. That RAISES at the Saturday backfill.
The prices below are the stored and staged PSKY rows, as read from S3.
"""

from __future__ import annotations

import pandas as pd
import pytest

import corporate_actions as ca
from features import compute as _c


def _psky_split(ex_date: str = "2026-10-05") -> ca.CorporateAction:
    return ca.CorporateAction(
        ticker="PSKY", type="split", ex_date=ex_date, split_from=1, split_to=2,
        source="polygon",
    )


def _stored() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"])
    return pd.DataFrame(
        {
            "Open": [10.20, 10.12, 10.11, 9.35],
            "High": [10.31, 10.42, 10.26, 9.67],
            "Low": [9.89, 10.06, 9.325, 8.97],
            "Close": [9.99, 10.33, 9.34, 9.50],
            "Volume": [19_006_372, 31_994_462, 44_268_018, 37_388_663],
        },
        index=idx,
    )


def _staged_halved() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2026-09-30", "2026-10-01", "2026-10-02", "2026-10-05"])
    return pd.DataFrame(
        {
            "Open": [5.06, 5.055, 4.675, 9.26],
            "High": [5.21, 5.13, 4.835, 9.87],
            "Low": [5.03, 4.6625, 4.485, 9.16],
            "Close": [5.165, 4.67, 4.75, 9.775],
            "Volume": [63_988_925, 88_536_036, 74_777_326, 31_657_941],
            "VWAP": [5.1248, 4.8075, 4.6724, float("nan")],
        },
        index=idx,
    )


def test_halved_rows_return_to_the_stored_basis():
    out, undone = _c._undo_vendor_preadjustment("PSKY", _stored(), _staged_halved(), [_psky_split()])
    assert [d for d, _ in undone] == ["2026-09-30", "2026-10-01", "2026-10-02"]
    assert out.loc["2026-10-02", "Close"] == pytest.approx(9.50)
    assert out.loc["2026-10-01", "Volume"] == round(88_536_036 * 0.5)
    assert out.loc["2026-09-30", "VWAP"] == pytest.approx(10.2496)
    # The ex-date row is the market's own print and is never touched.
    assert out.loc["2026-10-05", "Close"] == pytest.approx(9.775)


def test_after_the_undo_the_audit_sees_no_unflattened_split():
    out, _ = _c._undo_vendor_preadjustment("PSKY", _stored(), _staged_halved(), [_psky_split()])
    combined = pd.concat([_stored(), out])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()

    class _Registry:
        def list_actions(self, types=None):
            return [_psky_split()]

    audit = _c.audit_action_jumps({"PSKY": combined}, _Registry())
    assert audit.missed == {}
    assert audit.suspected == {}


def test_without_the_undo_the_same_rows_reproduce_the_alert():
    combined = pd.concat([_stored().loc[:"2026-10-01"], _staged_halved().loc["2026-10-02":]])

    class _Registry:
        def list_actions(self, types=None):
            return [_psky_split()]

    audit = _c.audit_action_jumps({"PSKY": combined}, _Registry())
    assert list(audit.missed) == ["PSKY"]
    assert audit.missed["PSKY"][0][0] == "2026-10-02"


def test_a_real_move_on_a_stored_date_is_left_alone():
    staged = _staged_halved()
    staged.loc["2026-10-02", "Close"] = 9.10  # a settled revision, not a rescale
    out, undone = _c._undo_vendor_preadjustment("PSKY", _stored(), staged, [_psky_split()])
    assert "2026-10-02" not in [d for d, _ in undone]
    assert out.loc["2026-10-02", "Close"] == pytest.approx(9.10)


def test_rows_with_no_stored_twin_follow_the_vendor_scale_from_the_last_stored_close():
    # The store ends 09-29, before every halved row: the 09-30 step from $9.99
    # to $5.165 is the factor, and the rows after it stay on that basis.
    out, undone = _c._undo_vendor_preadjustment(
        "PSKY", _stored().loc[:"2026-09-29"], _staged_halved(), [_psky_split()],
    )
    assert [d for d, _ in undone] == ["2026-09-30", "2026-10-01", "2026-10-02"]
    assert out.loc["2026-09-30", "Close"] == pytest.approx(10.33)
    assert out.loc["2026-10-05", "Close"] == pytest.approx(9.775)


def test_unadjusted_rows_with_no_stored_twin_are_untouched():
    staged = _staged_halved()
    staged.loc[: "2026-10-02", ["Open", "High", "Low", "Close", "VWAP"]] *= 2
    out, undone = _c._undo_vendor_preadjustment(
        "PSKY", _stored().loc[:"2026-09-29"], staged, [_psky_split()],
    )
    assert undone == []


def test_near_one_factors_are_never_undone():
    spinoff = ca.CorporateAction(
        ticker="PSKY", type="split", ex_date="2026-10-05", split_from=1000, split_to=1061,
        source="polygon",
    )
    staged = _staged_halved()
    staged["Close"] = _stored()["Close"].reindex(staged.index).fillna(9.775) * (1000 / 1061)
    out, undone = _c._undo_vendor_preadjustment("PSKY", _stored(), staged, [spinoff])
    assert undone == []


def test_apply_daily_delta_restores_the_halved_rows_end_to_end(monkeypatch):
    from tests.test_apply_daily_delta_min_last_date import (
        _daily_closes_frame,
        _stub_s3_with_daily_closes,
    )

    staged = _staged_halved()
    files = {
        d.strftime("%Y-%m-%d"): _daily_closes_frame(
            {"PSKY": {k: staged.loc[d, k] for k in ("Open", "High", "Low", "Close", "Volume", "VWAP")}}
        )
        for d in staged.index
    }
    s3 = _stub_s3_with_daily_closes(files)

    class _Registry:
        def list_actions(self, types=None):
            return [_psky_split()]

        def record_detected(self, action, run_id):
            return False

        def is_applied(self, store, action_id):
            return False

        def mark_applied(self, action, store, run_id=None):
            return True

    monkeypatch.setattr(ca, "detect_splits", lambda start, end: [_psky_split()])
    stored = _stored().loc[:"2026-09-29"]
    out, split_tickers = _c._apply_daily_delta(
        s3, "test-bucket", "2026-10-05", {"PSKY": stored}, registry=_Registry(),
    )
    psky = out["PSKY"]
    assert psky.loc["2026-10-02", "Close"] == pytest.approx(9.50)
    # The market printed no split, so nothing is restated either.
    assert split_tickers == set()
    # And nothing doubled: the history before the halved rows is untouched.
    assert psky.loc["2026-09-29", "Close"] == pytest.approx(9.99)
    assert _c.audit_action_jumps({"PSKY": psky}, _Registry()).missed == {}
