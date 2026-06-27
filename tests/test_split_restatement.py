"""data#1298 — ArcticDB universe must restate splits (full-history back-adjust).

The ArcticDB universe (predictor TRAINING input) is append-only + windowed, so a
split that restates the FULL adjusted history left only a recent window patched —
a split-boundary discontinuity that corrupts cross-boundary training features.
These tests pin the root-cause fix: a detected split triggers a full-history
restatement by the polygon-AUTHORITATIVE factor, so the series materialized for
the ArcticDB ``lib.write`` is continuous and on one adjusted scale.

Covers:
  * cumulative_factor / restate_series_for_splits factor math (forward + reverse)
  * _apply_daily_delta restates the pre-split window on detection (DD-style)
  * the audit guard catches an injected (un-restated) discontinuity
  * round-trip through a real ArcticDB (LMDB) library: the read window is
    continuous post-restate, and a return feature across the boundary is correct
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import features.compute as compute
from split_factor import (
    cumulative_factor,
    restate_series_for_splits,
)


# ── factor math ──────────────────────────────────────────────────────────────


def test_cumulative_factor_forward_split():
    # 3-for-1 forward split on 6/24: split_from=1, split_to=3 → pre-split prices
    # divide by 3 to reach the current (post-split) scale.
    events = [{"execution_date": "2026-06-24", "split_from": 1, "split_to": 3}]
    assert cumulative_factor(events, "2026-06-12") == pytest.approx(1 / 3)
    assert cumulative_factor(events, "2026-06-23") == pytest.approx(1 / 3)
    # On/after the execution date the price is already on the current scale.
    assert cumulative_factor(events, "2026-06-24") == pytest.approx(1.0)
    assert cumulative_factor(events, "2026-06-25") == pytest.approx(1.0)


def test_cumulative_factor_reverse_split():
    # DD's real event: 1-for-3 REVERSE (split_from=3, split_to=1) → pre-split
    # prices MULTIPLY by 3 to reach the higher post-reverse-split scale.
    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]
    assert cumulative_factor(events, "2026-06-12") == pytest.approx(3.0)
    assert cumulative_factor(events, "2026-06-24") == pytest.approx(1.0)


def test_cumulative_factor_compounds():
    events = [
        {"execution_date": "2026-01-10", "split_from": 1, "split_to": 2},
        {"execution_date": "2026-06-24", "split_from": 1, "split_to": 3},
    ]
    # Before both → 1/2 * 1/3
    assert cumulative_factor(events, "2026-01-05") == pytest.approx(1 / 6)
    # Between them → only the later split applies
    assert cumulative_factor(events, "2026-03-01") == pytest.approx(1 / 3)
    assert cumulative_factor(events, "2026-07-01") == pytest.approx(1.0)


def test_restate_series_reverse_split_is_continuous():
    # Synthetic DD: smooth ~$48 trend pre-split, ~3x ($144) trend post reverse
    # split, but with the un-restated history left on the old ($48) scale → a
    # ~3x jump at the boundary. Restating must remove the boundary jump.
    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    pre = pd.Series(np.linspace(47.5, 48.5, len(pre_dates)), index=pre_dates)
    post = pd.Series(np.linspace(142.5, 145.5, len(post_dates)), index=post_dates)
    close = pd.concat([pre, post])
    df = pd.DataFrame(
        {
            "Open": close, "High": close * 1.01, "Low": close * 0.99,
            "Close": close, "Volume": 1_000_000.0,
        }
    )

    # Before restatement: a >45% boundary jump exists.
    raw_ret = df["Close"].pct_change().abs().max()
    assert raw_ret > 0.45

    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]
    out = restate_series_for_splits(df, events)

    # After restatement: no daily move exceeds 45% — fully continuous.
    assert out["Close"].pct_change().abs().max() < 0.45
    # Pre-split rows are now on the post-split (~$144) scale.
    assert out["Close"].loc[pre_dates[-1]] == pytest.approx(48.0 * 3, rel=0.05)
    # Volume scaled inversely.
    assert out["Volume"].loc[pre_dates[0]] == pytest.approx(1_000_000 / 3, rel=0.01)
    # Post-split rows untouched.
    assert out["Close"].loc[post_dates[0]] == pytest.approx(df["Close"].loc[post_dates[0]])


def test_restate_noop_when_split_predates_series():
    dates = pd.bdate_range("2026-06-01", "2026-06-10")
    df = pd.DataFrame({"Close": np.linspace(100, 110, len(dates))}, index=dates)
    events = [{"execution_date": "2020-01-01", "split_from": 1, "split_to": 2}]
    out = restate_series_for_splits(df, events)
    # Every row is after the split → unchanged (same object, no copy).
    assert out is df


# ── _apply_daily_delta restates on detection ─────────────────────────────────


class _FakePolygon:
    """Stand-in for PolygonClient.get_splits — no network."""

    def __init__(self, mapping):
        self._mapping = mapping

    def get_splits(self, ticker):
        return list(self._mapping.get(ticker, []))


def _close_only(df):
    return df["Close"]


def test_apply_daily_delta_restates_split_ticker(monkeypatch):
    """DD-style: a split jump in the merged series triggers a full-history
    restatement via the polygon factor, and the ticker is reported as a
    split_ticker. The materialized series (which feeds the ArcticDB write) is
    continuous."""
    # Base (price_cache) history on the OLD (~$48) scale, ending 6/12.
    base_dates = pd.bdate_range("2026-06-01", "2026-06-12")
    base = pd.DataFrame(
        {
            "Open": np.linspace(47.0, 48.0, len(base_dates)),
            "High": np.linspace(47.5, 48.5, len(base_dates)),
            "Low": np.linspace(46.5, 47.5, len(base_dates)),
            "Close": np.linspace(47.0, 48.0, len(base_dates)),
            "Volume": np.full(len(base_dates), 1_000_000.0),
        },
        index=base_dates,
    )
    price_data = {"DD": base}

    # Delta rows: pre-execution dates (6/15..6/23) are still on the OLD (~$48)
    # scale (the reverse split is effective 6/24); only 6/24 onward is on the
    # NEW (~$144) scale. This is the real shape that produces the data#1298
    # boundary jump — restatement must lift the old-scale rows by ×3.
    exec_date = pd.Timestamp("2026-06-24")
    delta_dates = pd.bdate_range("2026-06-15", "2026-06-24")
    delta_rows = [
        {
            "date": d,
            "Open": 48.0, "High": 48.5, "Low": 47.5,
            "Close": 48.0, "Volume": 1_000_000, "source": "polygon",
        }
        if d < exec_date
        else {
            "date": d,
            "Open": 144.0, "High": 145.0, "Low": 143.0,
            "Close": 144.0, "Volume": 333_000, "source": "polygon",
        }
        for d in delta_dates
    ]

    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes",
        lambda *a, **k: {"DD": delta_rows},
    )
    # Patch the polygon lookup used by _restate_split_window.
    fake = _FakePolygon({"DD": [{"execution_date": "2026-06-24",
                                 "split_from": 3, "split_to": 1}]})
    import split_factor
    monkeypatch.setattr(
        split_factor, "split_events",
        lambda ticker, client=None: fake.get_splits(ticker),
    )

    out, split_tickers = compute._apply_daily_delta(
        s3=None, bucket="b", date_str="2026-06-24", price_data=price_data,
    )

    assert "DD" in split_tickers, "split should have been detected + restated"
    series = out["DD"]["Close"]
    # No residual >45% boundary jump — the full series is split-consistent.
    assert series.pct_change().abs().max() < 0.45
    # The old-scale pre-split rows were lifted onto the ~$144 scale.
    assert series.loc[base_dates[0]] > 120


def test_apply_daily_delta_no_factor_leaves_series_but_audit_flags(monkeypatch):
    """If polygon has no factor (e.g. unreachable), the series is left as-is
    (not silently mangled) AND the audit guard surfaces the discontinuity."""
    base_dates = pd.bdate_range("2026-06-01", "2026-06-12")
    base = pd.DataFrame(
        {"Open": 48.0, "High": 48.5, "Low": 47.5, "Close": 48.0, "Volume": 1e6},
        index=base_dates,
    )
    price_data = {"DD": base}
    delta_dates = pd.bdate_range("2026-06-15", "2026-06-24")
    delta_rows = [
        {"date": d, "Open": 144.0, "High": 145.0, "Low": 143.0,
         "Close": 144.0, "Volume": 333_000, "source": "polygon"}
        for d in delta_dates
    ]
    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes",
        lambda *a, **k: {"DD": delta_rows},
    )
    import split_factor
    monkeypatch.setattr(split_factor, "split_events", lambda ticker, client=None: [])

    out, split_tickers = compute._apply_daily_delta(
        s3=None, bucket="b", date_str="2026-06-24", price_data=price_data,
    )
    assert "DD" not in split_tickers  # nothing restated
    offenders = compute.audit_split_jumps(out)
    assert "DD" in offenders  # the audit guard catches it


def test_audit_split_jumps_clean_universe():
    dates = pd.bdate_range("2026-01-01", "2026-03-01")
    df = pd.DataFrame({"Close": np.linspace(100, 120, len(dates))}, index=dates)
    assert compute.audit_split_jumps({"AAPL": df}) == {}


# ── real ArcticDB (LMDB) round-trip ──────────────────────────────────────────


def test_arcticdb_window_read_continuous_after_restate(tmp_path):
    """End-to-end: the restated, split-consistent series written to a REAL
    ArcticDB library reads back continuous, and a return feature computed ACROSS
    the split boundary is correct (no artificial jump)."""
    adb = pytest.importorskip("arcticdb")

    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    pre = pd.Series(np.linspace(47.5, 48.5, len(pre_dates)), index=pre_dates)
    post = pd.Series(np.linspace(142.5, 145.5, len(post_dates)), index=post_dates)
    close = pd.concat([pre, post])
    df = pd.DataFrame({"Close": close, "Volume": np.full(len(close), 1e6)})

    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]
    restated = restate_series_for_splits(df, events)

    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    lib.write("DD", restated)

    # Windowed read spanning the split boundary (mirrors the predictor's
    # windowed materialization).
    got = lib.read(
        "DD",
        date_range=(pd.Timestamp("2026-06-18"), pd.Timestamp("2026-06-26")),
    ).data

    # No artificial boundary jump in the read window.
    assert got["Close"].pct_change().abs().max() < 0.45
    # A 1-day return feature computed across the split boundary is small/real,
    # not the ~3x split artifact.
    boundary_ret = (
        got["Close"].loc[post_dates[0]] / got["Close"].loc[pre_dates[-1]] - 1
    )
    assert abs(boundary_ret) < 0.1
