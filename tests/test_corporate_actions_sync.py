"""corporate_actions.sync — unified, pre-read restatement across ALL stores
(PR4 of the unified corporate-actions program, config#1433).

``sync`` is the ONE orchestration entry point invoked BEFORE consumers read, so
the split-boundary discontinuity is flattened up front instead of re-forming
mid-week between Saturday backfills. These tests pin:

  * end-to-end: a registered split → the ArcticDB universe symbol is restated
    AND the daily_closes archive window parquets are restated in place, with
    per-store / per-date applied markers written.
  * archive idempotency (the key new risk: a per-date parquet cannot be
    re-derived from a raw source here, so the registry marker is the only guard)
    — re-running sync is a NO-OP (no double-restate), verified via is_applied +
    value parity, AND a parquet already on the post-split scale is detected by
    the boundary scale-check and NOT multiplied.
  * daily_append basis-consistency: appending after a mid-week split lands on a
    restated history (no boundary discontinuity); and the double-apply guard
    (sync already restated ArcticDB ⇒ daily_append does not re-apply).
  * backfill interplay: a Saturday backfill after a mid-week sync does NOT
    double-restate (the shared arcticdb_universe marker is is_applied=True).
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry
from split_factor import restate_series_for_splits


# ── in-memory S3 double (registry markers + daily_closes parquet round-trip) ──


class _FakeS3:
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


_BUCKET = "alpha-engine-research"
_PREFIX = "staging/daily_closes/"
# DD's real 2026-06-24 event: 1-for-3 REVERSE split (split_from=3, split_to=1)
# → pre-split prices MULTIPLY by 3 to reach the post-reverse-split scale.
_EX = "2026-06-24"
_OLD = 48.0
_NEW = 144.0


def _put_daily_closes(s3, date: str, rows: dict[str, dict]):
    """Write a daily_closes snapshot parquet (index=ticker, OHLCV columns)."""
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "ticker"
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    s3.put_object(Bucket=_BUCKET, Key=f"{_PREFIX}{date}.parquet", Body=buf.getvalue())


def _read_daily_closes(s3, date: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=_BUCKET, Key=f"{_PREFIX}{date}.parquet")
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")


def _row(close: float) -> dict:
    return {
        "Open": close, "High": close * 1.01, "Low": close * 0.99,
        "Close": close, "Volume": 1_000_000.0, "VWAP": close, "source": "polygon",
    }


def _seed_window_parquets(s3):
    """6/22, 6/23 pre-split (OLD $48 scale); 6/24 post-split (NEW $144 scale).

    Mirrors the real data#1298 boundary: the morning re-fetch has put 6/24 on the
    new scale but the trailing pre-ex parquets are still un-restated.
    """
    _put_daily_closes(s3, "2026-06-22", {"DD": _row(_OLD), "AAPL": _row(200.0)})
    _put_daily_closes(s3, "2026-06-23", {"DD": _row(_OLD), "AAPL": _row(201.0)})
    _put_daily_closes(s3, "2026-06-24", {"DD": _row(_NEW), "AAPL": _row(202.0)})


def _seed_arctic_unrestated(tmp_path, ticker="DD"):
    """A REAL LMDB ArcticDB universe seeded with an UN-restated DD history: pre-
    6/24 rows on the OLD ($48) scale, post on the NEW ($144) → a ~3x boundary
    jump that sync's arctic restatement must flatten."""
    adb = pytest.importorskip("arcticdb")
    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    close = pd.concat([
        pd.Series(np.full(len(pre_dates), _OLD), index=pre_dates),
        pd.Series(np.full(len(post_dates), _NEW), index=post_dates),
    ])
    df = pd.DataFrame({
        "Open": close, "High": close * 1.01, "Low": close * 0.99,
        "Close": close, "Volume": np.full(len(close), 1e6),
    })
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    lib.write(ticker, df)
    return lib


def _dd_split():
    return ca.CorporateAction.from_split("DD", _EX, 3, 1)  # 1-for-3 reverse


# ── end-to-end: arctic universe + daily_closes archive both restated ──────────


def test_sync_restates_both_stores_and_writes_markers(tmp_path, monkeypatch):
    s3 = _FakeS3()
    _seed_window_parquets(s3)
    lib = _seed_arctic_unrestated(tmp_path)
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)

    reg = CorporateActionRegistry(s3, _BUCKET)
    action = _dd_split()

    result = ca.sync(
        s3, _BUCKET, "2026-06-22", "2026-06-24",
        stores=[ca.STORE_DAILY_CLOSES_ARCHIVE, ca.STORE_ARCTICDB_UNIVERSE],
        run_id="2026-06-24", tickers=["DD", "AAPL"], registry=reg, actions=[action],
    )

    # ── ArcticDB universe: full history flattened, marker written ────────────
    assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, action.action_id) is True
    dd = lib.read("DD").data
    assert dd["Close"].pct_change().abs().max() < 0.05      # continuous
    assert dd["Close"].iloc[0] == pytest.approx(_NEW, rel=0.02)  # pre lifted ×3

    # ── daily_closes archive: pre-ex parquets restated in place ──────────────
    for d in ("2026-06-22", "2026-06-23"):
        df_d = _read_daily_closes(s3, d)
        assert df_d.at["DD", "Close"] == pytest.approx(_NEW, rel=1e-6)   # 48 → 144
        assert df_d.at["DD", "VWAP"] == pytest.approx(_NEW, rel=1e-6)
        assert df_d.at["DD", "Volume"] == pytest.approx(1_000_000 / 3, rel=1e-3)
        # untouched ticker unchanged
        assert df_d.at["AAPL", "Close"] == pytest.approx(200.0 if d == "2026-06-22" else 201.0)
        store_d = f"{ca.STORE_DAILY_CLOSES_ARCHIVE}/{d}"
        assert reg.is_applied(store_d, action.action_id) is True

    # post-ex parquet (6/24) is NOT a pre-split date → untouched
    df_post = _read_daily_closes(s3, "2026-06-24")
    assert df_post.at["DD", "Close"] == pytest.approx(_NEW, rel=1e-6)

    # ── SyncResult shape ─────────────────────────────────────────────────────
    assert action in result.detected
    assert action in result.notices                       # it restated rows
    assert set(result.applied) == {
        ca.STORE_DAILY_CLOSES_ARCHIVE, ca.STORE_ARCTICDB_UNIVERSE,
    }


def test_sync_rerun_is_a_noop_no_double_restate(tmp_path, monkeypatch):
    """Re-running sync must NOT double-restate: markers gate both stores. Assert
    via is_applied + value parity against the first run's restated values."""
    s3 = _FakeS3()
    _seed_window_parquets(s3)
    lib = _seed_arctic_unrestated(tmp_path)
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)

    reg = CorporateActionRegistry(s3, _BUCKET)
    action = _dd_split()
    common = dict(
        stores=[ca.STORE_DAILY_CLOSES_ARCHIVE, ca.STORE_ARCTICDB_UNIVERSE],
        run_id="2026-06-24", tickers=["DD"], registry=reg, actions=[action],
    )

    ca.sync(s3, _BUCKET, "2026-06-22", "2026-06-24", **common)
    arctic_after_first = lib.read("DD").data["Close"].copy()
    archive_after_first = {
        d: _read_daily_closes(s3, d).at["DD", "Close"] for d in ("2026-06-22", "2026-06-23")
    }

    # A fresh registry pointing at the SAME S3 — markers persist in S3, so the
    # second sync sees is_applied=True everywhere and restates nothing.
    reg2 = CorporateActionRegistry(s3, _BUCKET)
    result2 = ca.sync(s3, _BUCKET, "2026-06-22", "2026-06-24",
                      stores=common["stores"], run_id="2026-06-25",
                      tickers=["DD"], registry=reg2, actions=[action])

    # No second restatement happened anywhere.
    assert result2.notices == []
    flat = [r for rs in result2.applied.values() for r in rs]
    assert all(r["status"] == "noop" for r in flat)

    # Value parity — NOT double-applied (would have ×3'd again).
    pd.testing.assert_series_equal(lib.read("DD").data["Close"], arctic_after_first)
    for d, v in archive_after_first.items():
        assert _read_daily_closes(s3, d).at["DD", "Close"] == pytest.approx(v, rel=1e-9)
        assert _read_daily_closes(s3, d).at["DD", "Close"] == pytest.approx(_NEW, rel=1e-6)


def test_sync_archive_already_restated_parquet_not_double_applied(tmp_path, monkeypatch):
    """The marker is the ONLY idempotency guard for the archive, but sync also
    boundary-verifies scale so an independent mechanism (the morning polygon
    re-fetch) that already restated a parquet — leaving NO sync marker — is NOT
    multiplied a second time. Pre-ex parquets are ALREADY on the $144 scale."""
    s3 = _FakeS3()
    # All window dates ALREADY on the post-split ($144) scale (re-fetch healed
    # them), but NO sync marker exists yet.
    _put_daily_closes(s3, "2026-06-22", {"DD": _row(_NEW)})
    _put_daily_closes(s3, "2026-06-23", {"DD": _row(_NEW)})
    _put_daily_closes(s3, "2026-06-24", {"DD": _row(_NEW)})
    lib = _seed_arctic_unrestated(tmp_path)
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)

    reg = CorporateActionRegistry(s3, _BUCKET)
    action = _dd_split()
    ca.sync(s3, _BUCKET, "2026-06-22", "2026-06-24",
            stores=[ca.STORE_DAILY_CLOSES_ARCHIVE], run_id="2026-06-24",
            tickers=["DD"], registry=reg, actions=[action])

    # Parquets stay on the $144 scale — NOT multiplied to $432.
    for d in ("2026-06-22", "2026-06-23"):
        assert _read_daily_closes(s3, d).at["DD", "Close"] == pytest.approx(_NEW, rel=1e-6)
        # The date is recorded applied (so we never re-examine it).
        assert reg.is_applied(f"{ca.STORE_DAILY_CLOSES_ARCHIVE}/{d}", action.action_id)


# ── daily_append basis-consistency + double-apply guard ───────────────────────


def _unrestated_reverse_jump():
    pre_dates = pd.bdate_range("2026-06-01", "2026-06-23")
    post_dates = pd.bdate_range("2026-06-24", "2026-07-01")
    close = pd.concat([
        pd.Series(np.full(len(pre_dates), _OLD), index=pre_dates),
        pd.Series(np.full(len(post_dates), _NEW), index=post_dates),
    ])
    return pd.DataFrame({
        "Open": close, "High": close * 1.01, "Low": close * 0.99,
        "Close": close, "Volume": np.full(len(close), 1e6),
    })


def test_daily_append_guard_restates_unrestated_history_before_append(monkeypatch):
    """daily_append's basis-consistency guard: when a registered split is NOT yet
    applied to the ArcticDB universe (sync missed/skipped), the guard restates
    the FULL history (write-then-mark) so today's row lands on a continuous
    scale — never spliced onto an un-restated basis."""
    from builders.daily_append import _ensure_history_restated

    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)
    action = _dd_split()
    reg.record_detected(action, run_id="r")
    hist = _unrestated_reverse_jump()
    assert hist["Close"].pct_change().abs().max() > 0.45      # boundary jump present

    universe_lib = MagicMock()
    out = _ensure_history_restated("DD", hist, [action], reg, universe_lib, "2026-06-24")

    # History restated → continuous, written back, marker set (write-then-mark).
    assert out["Close"].pct_change().abs().max() < 0.05
    assert out["Close"].iloc[0] == pytest.approx(_NEW, rel=0.02)
    universe_lib.write.assert_called_once()
    assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, action.action_id) is True

    # Splice today's (post-split-scale) row onto the RESTATED history → no jump.
    today = pd.Timestamp("2026-07-02")
    spliced = pd.concat([out["Close"], pd.Series([_NEW], index=[today])])
    assert spliced.pct_change().abs().max() < 0.05


def test_daily_append_guard_noop_when_already_restated(monkeypatch):
    """Double-apply guard: when sync already restated the ArcticDB universe
    (is_applied=True), daily_append's guard does NOT re-apply — the history is
    returned untouched and no second full-series write happens."""
    from builders.daily_append import _ensure_history_restated

    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)
    action = _dd_split()
    reg.record_detected(action, run_id="r")
    # sync (or the Saturday backfill) already applied it to this store.
    reg.mark_applied(action, ca.STORE_ARCTICDB_UNIVERSE, run_id="sync")

    # An ALREADY-continuous history (post-restate). The guard must leave it alone.
    dates = pd.bdate_range("2026-06-01", "2026-07-01")
    cont = pd.DataFrame({
        "Open": _NEW, "High": _NEW * 1.01, "Low": _NEW * 0.99,
        "Close": _NEW, "Volume": 1e6,
    }, index=dates)
    universe_lib = MagicMock()
    out = _ensure_history_restated("DD", cont, [action], reg, universe_lib, "2026-06-24")

    assert out is cont                              # untouched (same object)
    universe_lib.write.assert_not_called()          # no re-restate


# ── backfill interplay: shared marker ⇒ Saturday backfill does not re-restate ─


def test_backfill_apply_is_noop_after_sync_marked_arctic(tmp_path, monkeypatch):
    """The Saturday backfill restates via ``corporate_actions.apply(store=
    arcticdb_universe, registry=...)``. After a mid-week ``sync`` marked the same
    (action, arcticdb_universe), the backfill path is a registry NOOP — no
    double-restate (the shared exactly-once marker, PR3 §4 / PR4)."""
    s3 = _FakeS3()
    _seed_window_parquets(s3)
    lib = _seed_arctic_unrestated(tmp_path)
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)

    reg = CorporateActionRegistry(s3, _BUCKET)
    action = _dd_split()
    ca.sync(s3, _BUCKET, "2026-06-22", "2026-06-24",
            stores=[ca.STORE_ARCTICDB_UNIVERSE], run_id="2026-06-24",
            tickers=["DD"], registry=reg, actions=[action])
    restated_close = lib.read("DD").data["Close"].copy()

    # Saturday backfill loads a (freshly materialized) DD frame and runs apply
    # with the SAME registry — it must skip (is_applied=True), not re-multiply.
    bf_frame = lib.read("DD").data
    out, results = ca.apply(
        bf_frame, [action], store=ca.STORE_ARCTICDB_UNIVERSE, registry=reg, run_id="sat",
    )
    assert [r["status"] for r in results] == ["noop"]
    pd.testing.assert_series_equal(out["Close"], restated_close)
