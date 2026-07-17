"""Tests for builders/splice_rebase.py (config#2219 Option B).

Pins: dry-run previews the re-base at the STORED discontinuity (not the true
ex-date) and is side-effect-free; a correct splice-date re-base clears the
canary on --apply, writes ArcticDB, and registers the action at the TRUE
ex-date (which differs from the math boundary — exactly MLI's shape); a wrong
splice-date guess leaves the canary red, dry-run reports it, and --apply
RAISES without writing or registering; missing symbol degrades gracefully.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
import store.arctic_store
from builders.splice_rebase import splice_rebase

_BUCKET = "alpha-engine-research"
_OLD, _NEW = 100.0, 50.0  # 2-for-1 forward: pre (un-split) 2x the post scale


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


def _seed_lib(tmp_path, ticker="MLI", splice_date="2026-06-12"):
    """LMDB universe seeded as a SPLICE: pre-``splice_date`` on the OLD
    (un-split) scale, post on the NEW (split-adjusted) scale — the boundary is
    the splice point, NOT any true corporate-action ex-date."""
    adb = pytest.importorskip("arcticdb")
    all_dates = pd.bdate_range("2026-05-01", "2026-07-15")
    sd = pd.Timestamp(splice_date)
    close = pd.Series(
        [(_OLD if d < sd else _NEW) for d in all_dates], index=all_dates, dtype=float
    )
    df = pd.DataFrame({
        "Open": close, "High": close * 1.01, "Low": close * 0.99,
        "Close": close, "Volume": np.full(len(close), 1e6),
    })
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    lib.write(ticker, df)
    return lib


def _patch_lib(monkeypatch, lib):
    monkeypatch.setattr(store.arctic_store, "get_universe_lib", lambda *a, **k: lib)


class TestDryRun:
    def test_correct_splice_date_previews_cleared(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        out = splice_rebase(
            "MLI", "2026-06-12", "2026-07-01", 1, 2, s3=s3, dry_run=True,
        )
        assert out["status"] == "dry_run_ok"
        assert out["canary_before"] > 0.18
        assert out["canary_after"] < 0.18
        assert out["canary_cleared"] is True
        assert out["n_rows_changed"] > 0

    def test_dry_run_has_no_side_effects(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        splice_rebase("MLI", "2026-06-12", "2026-07-01", 1, 2, s3=s3, dry_run=True)
        assert s3.store == {}
        assert lib.read("MLI").data["Close"].iloc[0] == _OLD

    def test_wrong_splice_date_not_cleared(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        out = splice_rebase(
            "MLI", "2026-06-01", "2026-07-01", 1, 2, s3=s3, dry_run=True,
        )
        assert out["status"] == "dry_run_canary_not_cleared"
        assert out["canary_cleared"] is False


class TestApply:
    def test_clean_case_writes_and_registers_at_true_ex_date(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        out = splice_rebase(
            "MLI", "2026-06-12", "2026-07-01", 1, 2, s3=s3, dry_run=False,
        )
        assert out["status"] == "applied"
        assert out["canary_cleared"] is True
        after = lib.read("MLI").data
        assert after["Close"].pct_change().abs().max() < 0.18
        reg = ca.CorporateActionRegistry(s3, _BUCKET)
        actions = reg.list_actions(ticker="MLI")
        assert len(actions) == 1
        assert actions[0].ex_date == "2026-07-01"  # TRUE ex-date, not the splice date
        assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, out["action_id"])

    def test_wrong_splice_date_raises_and_leaves_series_untouched(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        with pytest.raises(RuntimeError, match="canary NOT cleared"):
            splice_rebase("MLI", "2026-06-01", "2026-07-01", 1, 2, s3=s3, dry_run=False)
        after = lib.read("MLI").data
        assert set(after["Close"].unique()) == {_OLD, _NEW}
        reg = ca.CorporateActionRegistry(s3, _BUCKET)
        assert reg.list_actions(ticker="MLI") == []

    def test_already_applied_is_noop(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, splice_date="2026-06-12")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        splice_rebase("MLI", "2026-06-12", "2026-07-01", 1, 2, s3=s3, dry_run=False)
        out = splice_rebase("MLI", "2026-06-12", "2026-07-01", 1, 2, s3=s3, dry_run=False)
        assert out["status"] == "noop_already_applied"


class TestRobustness:
    def test_missing_symbol_is_graceful(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path)
        _patch_lib(monkeypatch, lib)
        out = splice_rebase(
            "NOPE", "2026-06-12", "2026-07-01", 1, 2, s3=_FakeS3(), dry_run=True,
        )
        assert out["status"] == "no_such_symbol"
