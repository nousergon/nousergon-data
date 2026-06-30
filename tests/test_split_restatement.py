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

import io

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
import features.compute as compute
from corporate_actions import CorporateActionRegistry
from corporate_actions import (
    cumulative_factor,
    restate_series_for_splits,
)


class _FakeS3:
    """Minimal in-memory S3 double so the registry round-trips write-if-absent /
    read-back without a live bucket (mirrors tests/test_corporate_actions*.py)."""

    def __init__(self):
        self.store: dict[str, bytes] = {}

    def _err(self, code, op):
        return ClientError({"Error": {"Code": code, "Message": "x"}}, op)

    def head_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._err("404", "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._err("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"ETag": '"x"'}

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


def _registry():
    return CorporateActionRegistry(_FakeS3(), "alpha-engine-research")


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


# ── registry-driven detection + restatement in _apply_daily_delta ────────────


def _delta_split_frame(pre_close, post_close, *, exec_date="2026-06-24"):
    """A (base price_data, delta_rows) pair shaped like the real data#1298
    boundary: base ends 6/12 on the OLD (pre-split) scale; the delta carries
    pre-exec dates still on the OLD scale and exec-onward on the NEW scale."""
    base_dates = pd.bdate_range("2026-06-01", "2026-06-12")
    base = pd.DataFrame(
        {
            "Open": np.full(len(base_dates), pre_close),
            "High": np.full(len(base_dates), pre_close * 1.01),
            "Low": np.full(len(base_dates), pre_close * 0.99),
            "Close": np.full(len(base_dates), float(pre_close)),
            "Volume": np.full(len(base_dates), 1_000_000.0),
        },
        index=base_dates,
    )
    ex = pd.Timestamp(exec_date)
    delta_dates = pd.bdate_range("2026-06-15", exec_date)
    delta_rows = [
        {
            "date": d,
            "Open": pre_close, "High": pre_close * 1.01, "Low": pre_close * 0.99,
            "Close": float(pre_close), "Volume": 1_000_000, "source": "polygon",
        }
        if d < ex
        else {
            "date": d,
            "Open": post_close, "High": post_close * 1.01, "Low": post_close * 0.99,
            "Close": float(post_close), "Volume": 1_000_000, "source": "polygon",
        }
        for d in delta_dates
    ]
    return base, delta_rows


def test_apply_daily_delta_restates_reverse_split(monkeypatch):
    """DD-style 1-for-3 reverse split: registry-driven detection restates the
    full pre-split window, the ticker is reported, and the series is continuous.
    The applied action is recorded as applied to the store (idempotency marker).
    """
    base, delta_rows = _delta_split_frame(48.0, 144.0)
    price_data = {"DD": base}
    base_dates = base.index

    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes", lambda *a, **k: {"DD": delta_rows},
    )
    action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
    monkeypatch.setattr(ca, "detect_splits", lambda *a, **k: [action])

    reg = _registry()
    out, split_tickers = compute._apply_daily_delta(
        s3=None, bucket="b", date_str="2026-06-24", price_data=price_data, registry=reg,
    )

    assert "DD" in split_tickers
    series = out["DD"]["Close"]
    assert series.pct_change().abs().max() < 0.45        # continuous
    assert series.loc[base_dates[0]] > 120                # lifted onto ~$144 scale
    assert reg.is_applied("arcticdb_universe", action.action_id) is True


def test_apply_daily_delta_restates_sub45_split_old_heuristic_missed(monkeypatch):
    """HEADLINE regression fix: a 3-for-2 forward split (-33% boundary move) is
    BELOW the old 45% heuristic's trigger, so the old code SILENTLY left it
    un-restated. Registry-driven detection now restates it regardless of
    magnitude — the series is flattened."""
    # 3-for-2 forward: split_from=2, split_to=3 (factor 2/3). OLD ~$90 →
    # NEW ~$60 at the boundary: a -33% move the old >45% trigger would miss.
    base, delta_rows = _delta_split_frame(90.0, 60.0)
    price_data = {"TST": base}

    # The boundary move the old heuristic would have seen is sub-45% — proving
    # the old code's latent miss (it only restated on |move| > 0.45).
    pre_restate = pd.concat(
        [base["Close"], pd.Series([60.0], index=[pd.Timestamp("2026-06-24")])]
    )
    assert 0.18 < pre_restate.pct_change().abs().max() < 0.45

    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes", lambda *a, **k: {"TST": delta_rows},
    )
    action = ca.CorporateAction.from_split("TST", "2026-06-24", 2, 3)
    monkeypatch.setattr(ca, "detect_splits", lambda *a, **k: [action])

    reg = _registry()
    out, split_tickers = compute._apply_daily_delta(
        s3=None, bucket="b", date_str="2026-06-24", price_data=price_data, registry=reg,
    )

    assert "TST" in split_tickers, "sub-45% registered split must be restated"
    series = out["TST"]["Close"]
    # Fully flattened: no residual move above the diagnostic screen threshold.
    assert series.pct_change().abs().max() < 0.18
    # Pre-split rows lifted onto the post-split (~$60) scale (×2/3).
    assert series.iloc[0] == pytest.approx(60.0, rel=0.02)


def test_apply_daily_delta_double_apply_guard(monkeypatch):
    """PR3 §4: the feature-snapshot path loads the ALREADY-restated ArcticDB and
    re-applies the delta. An action already marked applied to the store is a
    registry noop — re-applying must NOT double-adjust the continuous series."""
    # Continuous (already-restated) series: everything on the post-split scale.
    base_dates = pd.bdate_range("2026-06-01", "2026-06-12")
    base = pd.DataFrame(
        {"Open": 144.0, "High": 145.0, "Low": 143.0, "Close": 144.0, "Volume": 1e6},
        index=base_dates,
    )
    price_data = {"DD": base}
    delta_dates = pd.bdate_range("2026-06-15", "2026-06-24")
    delta_rows = [
        {"date": d, "Open": 144.0, "High": 145.0, "Low": 143.0,
         "Close": 144.0, "Volume": 1_000_000, "source": "polygon"}
        for d in delta_dates
    ]
    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes", lambda *a, **k: {"DD": delta_rows},
    )
    action = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
    monkeypatch.setattr(ca, "detect_splits", lambda *a, **k: [action])

    reg = _registry()
    # The split was already applied to this store by a prior (backfill) pass.
    reg.mark_applied(action, "arcticdb_universe", run_id="prior")

    out, split_tickers = compute._apply_daily_delta(
        s3=None, bucket="b", date_str="2026-06-24", price_data=price_data, registry=reg,
    )

    assert "DD" not in split_tickers          # noop — not re-restated
    series = out["DD"]["Close"]
    # If it had double-applied (×3 on the pre-6/24 rows) the series would jump
    # ~3x at the boundary; it stays flat/continuous.
    assert series.pct_change().abs().max() < 0.05
    assert series.max() == pytest.approx(144.0, rel=0.05)


# ── registry-aware audit: missed (BLOCKING) vs suspected (WARN) ───────────────


def _unrestated_reverse_jump(ticker_close_pre=48.0, ticker_close_post=144.0):
    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    close = pd.concat([
        pd.Series(np.full(len(pre_dates), ticker_close_pre), index=pre_dates),
        pd.Series(np.full(len(post_dates), ticker_close_post), index=post_dates),
    ])
    return pd.DataFrame({"Close": close, "Volume": np.full(len(close), 1e6)})


def test_audit_action_jumps_clean_universe():
    dates = pd.bdate_range("2026-01-01", "2026-03-01")
    df = pd.DataFrame({"Close": np.linspace(100, 120, len(dates))}, index=dates)
    audit = compute.audit_action_jumps({"AAPL": df}, _registry())
    assert audit.missed == {} and audit.suspected == {}


def test_audit_action_jumps_missed_when_registered_split_unflattened():
    """A residual jump that a REGISTERED split explains (ex_date at the jump,
    move matches the factor) is MISSED — the BLOCKING class."""
    reg = _registry()
    reg.record_detected(
        ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1), run_id="r",
    )
    price_data = {"DD": _unrestated_reverse_jump(48.0, 144.0)}  # ×3 jump at 6/24
    audit = compute.audit_action_jumps(price_data, reg)
    assert "DD" in audit.missed
    assert "DD" not in audit.suspected


def test_audit_action_jumps_suspected_when_no_registered_action():
    """A large move (±33%) with NO registered action is SUSPECTED (WARN only) —
    a legitimate earnings move must never be a blocking miss."""
    # ~ -33% single-day move, no registered split.
    dates = pd.bdate_range("2026-06-01", "2026-06-10")
    close = pd.Series(np.full(len(dates), 100.0), index=dates)
    close.iloc[5] = 67.0  # -33% earnings gap
    price_data = {"ER": pd.DataFrame({"Close": close})}
    audit = compute.audit_action_jumps(price_data, _registry())
    assert "ER" in audit.suspected
    assert "ER" not in audit.missed


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


# ── backfill train chokepoint: BLOCKING audit ────────────────────────────────


def _backfill_common_mocks(_bf, monkeypatch, price_data, registry):
    """Patch backfill's loaders up to (and past) the audit so a single
    in-universe symbol flows through without S3 / ArcticDB / polygon."""
    from unittest.mock import MagicMock

    monkeypatch.setattr(_bf, "_load_full_cache", lambda *a, **k: dict(price_data))
    monkeypatch.setattr(_bf, "_build_registry", lambda *a, **k: registry)
    monkeypatch.setattr(_bf, "_apply_daily_delta", lambda *a, **k: (dict(price_data), set()))
    monkeypatch.setattr(_bf.boto3, "client", lambda *a, **k: MagicMock())


def test_backfill_raises_on_missed_registered_split(monkeypatch):
    """At the train chokepoint, a residual jump that a REGISTERED split explains
    (un-flattened KNOWN action) raises CorporateActionAuditError BEFORE any
    ArcticDB write — the data#1298 corruption cannot land silently."""
    from builders import backfill as _bf
    from corporate_actions import CorporateActionAuditError

    reg = _registry()
    reg.record_detected(
        ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1), run_id="r",
    )
    price_data = {"DD": _unrestated_reverse_jump(48.0, 144.0)}
    _backfill_common_mocks(_bf, monkeypatch, price_data, reg)

    with pytest.raises(CorporateActionAuditError):
        _bf.backfill(ticker_filter=None)


def test_backfill_proceeds_on_suspected_only(monkeypatch):
    """A large move with NO registered action is SUSPECTED → WARN, NOT blocking:
    the backfill proceeds past the audit (a real ±33% move must not halt it)."""
    from unittest.mock import MagicMock

    from builders import backfill as _bf

    # A clean in-universe ticker plus a suspected (unexplained) -33% move; the
    # registry is EMPTY so nothing explains the jump.
    dates = pd.date_range("2024-01-01", periods=400, freq="B")
    close = pd.Series(100.0 + np.arange(400) * 0.01, index=dates)
    close.iloc[200] = close.iloc[199] * 0.67  # -33%, no registered action
    aapl = pd.DataFrame(
        {"Open": close, "High": close * 1.01, "Low": close * 0.99,
         "Close": close, "Volume": 1_000_000.0},
        index=dates,
    )
    price_data = {"AAPL": aapl}
    reg = _registry()  # empty → the jump is "suspected", never "missed"
    _backfill_common_mocks(_bf, monkeypatch, price_data, reg)

    universe_lib = MagicMock()
    macro_lib = MagicMock()
    monkeypatch.setattr(_bf, "_assert_no_arctic_regression", lambda *a, **k: None)
    monkeypatch.setattr(_bf, "_load_current_constituents", lambda *a, **k: {"AAPL"})
    monkeypatch.setattr(_bf, "_extract_macro_series", lambda *a, **k: {})
    monkeypatch.setattr(_bf, "_load_sector_map", lambda *a, **k: {"AAPL": "XLK"})
    monkeypatch.setattr(_bf, "_load_cached_fundamentals", lambda *a, **k: {})
    monkeypatch.setattr(_bf, "_load_cached_alternative", lambda *a, **k: {})
    monkeypatch.setattr(_bf, "_build_macro_features_df", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(_bf, "compute_features", lambda df, **_: df)
    monkeypatch.setattr(_bf, "get_universe_lib", lambda *a, **k: universe_lib)
    monkeypatch.setattr(_bf, "get_macro_lib", lambda *a, **k: macro_lib)
    monkeypatch.setattr(
        _bf, "_scan_universe_and_emit_freshness_receipt",
        lambda *a, **k: {"n_symbols_checked": 1, "stalest_symbol": "AAPL",
                         "stalest_age_trading_days": 1, "all_fresh": True},
    )

    # Must NOT raise CorporateActionAuditError — proceeds to a normal summary.
    result = _bf.backfill(ticker_filter=None)
    assert result.get("status") != "error"


# ── PR6: split_factor.py shim consolidated into corporate_actions + deleted ───


def test_split_factor_shim_deleted():
    """The top-level split_factor.py shim is GONE (its math moved into
    corporate_actions._split_math, re-exported from the package). Importing it
    must fail so no consumer silently re-grows a dependency on the loose name."""
    import importlib

    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("split_factor")


def test_split_math_reexported_from_package():
    """The split-factor math is importable from the package surface (the
    repointed import path for every former split_factor consumer)."""
    from corporate_actions import (  # noqa: F401
        cumulative_factor,
        restate_series_for_splits,
        split_events,
    )

    # Parity sanity: the moved cumulative_factor still computes the reverse-split
    # factor (pre-ex prices ×3 for a 1-for-3 reverse split).
    events = [{"execution_date": "2026-06-24", "split_from": 3, "split_to": 1}]
    assert cumulative_factor(events, "2026-06-12") == pytest.approx(3.0)
    assert cumulative_factor(events, "2026-06-24") == pytest.approx(1.0)
