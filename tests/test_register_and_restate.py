"""Tests for builders/register_and_restate.py (config#2219 Option A).

Pins: dry-run is side-effect-free and previews the canary; a clean single-basis
series is flattened (canary cleared) on --apply, registry + applied marker
written; a SPLICE / misdated split (no ex-date boundary) is REFUSED — dry-run
reports canary-not-cleared and --apply RAISES rather than writing a half-fixed
series (the fail-loud guarantee); missing symbol degrades gracefully.
"""
from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
import store.arctic_store
from builders.register_and_restate import register_and_restate

_BUCKET = "alpha-engine-research"


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


_OLD, _NEW = 48.0, 144.0  # DD 1-for-3 reverse: pre ×3 == post


def _seed_lib(tmp_path, ticker="DD", jump_date="2026-06-24"):
    """LMDB universe seeded UN-restated: pre-``jump_date`` on OLD ($48) scale,
    post on NEW ($144) → a ~3× boundary jump at ``jump_date``."""
    adb = pytest.importorskip("arcticdb")
    all_dates = pd.bdate_range("2026-06-01", "2026-07-10")
    jd = pd.Timestamp(jump_date)
    close = pd.Series(
        [(_OLD if d < jd else _NEW) for d in all_dates], index=all_dates, dtype=float
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
    def test_clean_single_basis_previews_cleared(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, jump_date="2026-06-24")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        # register the split at its TRUE ex-date (== the boundary) → apply confirms
        out = register_and_restate("DD", 3, 1, "2026-06-24", s3=s3, dry_run=True)
        assert out["status"] == "dry_run_ok"
        assert out["canary_before"] > 0.33     # un-restated jump present
        assert out["canary_after"] < 0.33      # would be flattened
        assert out["canary_cleared"] is True
        assert out["n_rows_would_adjust"] > 0

    def test_dry_run_has_no_side_effects(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path)
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        register_and_restate("DD", 3, 1, "2026-06-24", s3=s3, dry_run=True)
        assert s3.store == {}                                  # registry untouched
        assert lib.read("DD").data["Close"].iloc[0] == _OLD    # lib unchanged

    def test_splice_or_misdated_split_not_cleared(self, tmp_path, monkeypatch):
        # jump is at 06-24 but we register ex-date 07-01 (misdated, like MLI's
        # splice): no price boundary at the ex-date → apply refuses → red canary.
        lib = _seed_lib(tmp_path, jump_date="2026-06-24")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        out = register_and_restate("DD", 1, 2, "2026-07-01", s3=s3, dry_run=True)
        assert out["status"] == "dry_run_canary_not_cleared"
        assert out["canary_cleared"] is False
        assert out["canary_after"] > 0.33


class TestApply:
    def test_clean_case_writes_and_clears(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, jump_date="2026-06-24")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        out = register_and_restate("DD", 3, 1, "2026-06-24", s3=s3, dry_run=False)
        assert out["status"] == "applied"
        assert out["canary_cleared"] is True
        # ArcticDB actually rewritten to a continuous series
        after = lib.read("DD").data
        assert after["Close"].pct_change().abs().max() < 0.33
        # registry recorded the action + an applied marker
        reg = ca.CorporateActionRegistry(s3, _BUCKET)
        assert len(reg.list_actions(ticker="DD")) == 1
        assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, out["action_id"])

    def test_splice_raises_and_leaves_series_untouched(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path, jump_date="2026-06-24")
        _patch_lib(monkeypatch, lib)
        s3 = _FakeS3()
        with pytest.raises(RuntimeError, match="canary NOT cleared"):
            register_and_restate("DD", 1, 2, "2026-07-01", s3=s3, dry_run=False)
        # refused apply must NOT have corrupted the stored series
        after = lib.read("DD").data
        assert set(after["Close"].unique()) == {_OLD, _NEW}
        # and the refused action is NOT marked applied (exactly-once contract)
        reg = ca.CorporateActionRegistry(s3, _BUCKET)
        acts = reg.list_actions(ticker="DD")
        assert acts and not reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, acts[0].action_id)


class TestRobustness:
    def test_missing_symbol_is_graceful(self, tmp_path, monkeypatch):
        lib = _seed_lib(tmp_path)
        _patch_lib(monkeypatch, lib)
        out = register_and_restate("NOPE", 1, 2, "2026-07-01", s3=_FakeS3(),
                                   dry_run=True)
        assert out["status"] == "no_such_symbol"
