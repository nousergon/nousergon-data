"""Tests for ticker-rename detection + ArcticDB symbol migration (PR6,
config#1433).

Covers:
  * ``CorporateAction.from_rename`` — type/fields + deterministic action_id.
  * ``renames_from_events`` — emit a rename only when the candidate is the OLD
    side of a ticker_change (NEW-side survivor / no-op skipped).
  * ``detect_renames`` over CANDIDATE tickers — rename actions for ticker_changes
    only; a delist-only candidate yields no rename; a polygon FAILURE records the
    candidate in ``failed_candidates`` (history-safety, never silently "no
    rename"); a client-construction failure fails ALL candidates.
  * ``migrate_symbol`` — old->new ArcticDB round-trip (new holds old's full
    history, old deleted), exactly-once idempotency via the registry marker, and
    the new-already-exists no-splice path.
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

_BUCKET = "alpha-engine-research"


# ── in-memory S3 double (registry markers) — mirrors test_corporate_actions* ──


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


def _fake_events_client(events_by_ticker: dict[str, list[dict]], *, raise_for=None):
    """A fake polygon client whose get_ticker_events serves canned rename pairs
    per ticker; ``raise_for`` (a set) raises to simulate a detection failure."""
    client = MagicMock()
    raise_for = raise_for or set()

    def fake_events(ticker):
        if ticker in raise_for:
            raise RuntimeError(f"polygon down for {ticker}")
        return events_by_ticker.get(ticker, [])

    client.get_ticker_events.side_effect = fake_events
    return client


# ── CorporateAction.from_rename ───────────────────────────────────────────────


def test_from_rename_fields_and_deterministic_id():
    a = ca.CorporateAction.from_rename("FB", "META", "2022-06-09")
    assert a.type == "rename"
    assert a.old_ticker == "FB"
    assert a.new_ticker == "META"
    assert a.ticker == "FB"  # keys off the OLD (missing) symbol
    assert a.ex_date == "2022-06-09"
    # Deterministic + content-addressed: same rename -> same id.
    b = ca.CorporateAction.from_rename("FB", "META", "2022-06-09")
    assert a.action_id == b.action_id
    # A different target or date changes the id.
    assert a.action_id != ca.CorporateAction.from_rename("FB", "MVRS", "2022-06-09").action_id
    assert a.action_id != ca.CorporateAction.from_rename("FB", "META", "2022-06-10").action_id
    # Round-trips through to_dict/from_dict.
    assert ca.CorporateAction.from_dict(a.to_dict()).action_id == a.action_id


# ── renames_from_events ───────────────────────────────────────────────────────


def test_renames_from_events_emits_only_when_candidate_is_old():
    events = [{"date": "2022-06-09", "old_ticker": "FB", "new_ticker": "META"}]
    out = ca.renames_from_events("FB", events)
    assert len(out) == 1
    assert (out[0].old_ticker, out[0].new_ticker) == ("FB", "META")
    # Candidate is the NEW side (survivor) — not a rename OFF this candidate.
    assert ca.renames_from_events("META", events) == []


def test_renames_from_events_skips_noop_and_malformed():
    events = [
        {"date": "2022-06-09", "old_ticker": "FB", "new_ticker": "FB"},   # no-op
        {"date": None, "old_ticker": "FB", "new_ticker": "META"},          # bad date
        {"date": "2022-06-09", "old_ticker": "FB", "new_ticker": None},    # bad new
    ]
    assert ca.renames_from_events("FB", events) == []


# ── detect_renames over candidates ────────────────────────────────────────────


def test_detect_renames_emits_for_ticker_change_only():
    client = _fake_events_client({
        "FB": [{"date": "2022-06-09", "old_ticker": "FB", "new_ticker": "META"}],
        "DELISTME": [],  # genuine delist / merger-of-acquired → no ticker_change
    })
    result = ca.detect_renames(["FB", "DELISTME"], client=client)
    assert isinstance(result, ca.RenameDetection)
    assert result.failed_candidates == set()
    assert len(result.renames) == 1
    r = result.renames[0]
    assert (r.old_ticker, r.new_ticker, r.ex_date) == ("FB", "META", "2022-06-09")
    # The delist-only candidate produced no rename → it is NOT in failed either
    # (confirmed safe to prune).
    assert "DELISTME" not in result.failed_candidates


def test_detect_renames_failure_records_candidate_not_silent():
    """History-safety: a polygon query that RAISES must land in
    failed_candidates (so the prune wiring skips it), NOT be silently dropped as
    'no rename'."""
    client = _fake_events_client(
        {"FB": [{"date": "2022-06-09", "old_ticker": "FB", "new_ticker": "META"}]},
        raise_for={"BROKEN"},
    )
    result = ca.detect_renames(["FB", "BROKEN"], client=client)
    assert [r.old_ticker for r in result.renames] == ["FB"]
    assert result.failed_candidates == {"BROKEN"}


def test_detect_renames_client_construction_failure_fails_all(monkeypatch):
    """No usable polygon client → EVERY candidate is a detection failure (none
    confirmed safe to prune this pass)."""
    import polygon_client as _pc

    def boom(*a, **k):
        raise RuntimeError("no api key")

    monkeypatch.setattr(_pc, "polygon_client", boom)
    result = ca.detect_renames(["FB", "AAPL"])  # client=None → tries to construct
    assert result.renames == []
    assert result.failed_candidates == {"FB", "AAPL"}


def test_detect_renames_empty_input():
    assert ca.detect_renames([]).renames == []
    assert ca.detect_renames(None).failed_candidates == set()


# ── migrate_symbol: real ArcticDB (LMDB) round-trip ───────────────────────────


def _seed_symbol(tmp_path, ticker="FB", n=30):
    adb = pytest.importorskip("arcticdb")
    dates = pd.bdate_range("2022-04-01", periods=n)
    close = pd.Series(np.linspace(200.0, 210.0, n), index=dates)
    df = pd.DataFrame({
        "Open": close, "High": close * 1.01, "Low": close * 0.99,
        "Close": close, "Volume": np.full(n, 1e6),
    })
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    lib.write(ticker, df)
    return lib, df


def test_migrate_symbol_round_trip_old_to_new(tmp_path):
    lib, src = _seed_symbol(tmp_path, "FB")
    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)

    did = ca.migrate_symbol(
        lib, "FB", "META", registry=reg, run_id="r1", ex_date="2022-06-09",
    )
    assert did is True
    # New key holds the OLD symbol's full history; old key gone.
    assert lib.has_symbol("META")
    assert not lib.has_symbol("FB")
    got = lib.read("META").data
    pd.testing.assert_frame_equal(got, src, check_freq=False)


def test_migrate_symbol_idempotent_second_call_noop(tmp_path):
    lib, _ = _seed_symbol(tmp_path, "FB")
    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)

    assert ca.migrate_symbol(lib, "FB", "META", registry=reg, run_id="r1", ex_date="2022-06-09") is True
    # Second call: registry marker already set → noop (returns False), and it
    # must NOT touch the already-live META series.
    meta_before = lib.read("META").data
    did = ca.migrate_symbol(lib, "FB", "META", registry=reg, run_id="r2", ex_date="2022-06-09")
    assert did is False
    pd.testing.assert_frame_equal(lib.read("META").data, meta_before)


def test_migrate_symbol_new_already_exists_no_splice(tmp_path):
    """If the new ticker already has its OWN live history, do NOT merge/overwrite
    (no splice) — drop the orphaned old key, mark applied."""
    lib, _ = _seed_symbol(tmp_path, "FB", n=30)
    # Seed META with a DIFFERENT live history.
    adb = pytest.importorskip("arcticdb")
    meta_dates = pd.bdate_range("2022-06-09", periods=10)
    meta_close = pd.Series(np.linspace(300.0, 310.0, 10), index=meta_dates)
    meta_df = pd.DataFrame({
        "Open": meta_close, "High": meta_close, "Low": meta_close,
        "Close": meta_close, "Volume": np.full(10, 2e6),
    })
    lib.write("META", meta_df)
    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)

    did = ca.migrate_symbol(lib, "FB", "META", registry=reg, run_id="r1", ex_date="2022-06-09")
    assert did is True
    # META's live history is preserved (NOT overwritten by FB's), FB dropped.
    assert not lib.has_symbol("FB")
    pd.testing.assert_frame_equal(lib.read("META").data, meta_df, check_freq=False)
    # Marked applied → a re-run is a noop.
    action = ca.CorporateAction.from_rename("FB", "META", "2022-06-09")
    assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, action.action_id)


def test_migrate_symbol_old_absent_is_noop(tmp_path):
    lib, _ = _seed_symbol(tmp_path, "FB")
    reg = CorporateActionRegistry(_FakeS3(), _BUCKET)
    # GHOST has no ArcticDB history — nothing to migrate.
    assert ca.migrate_symbol(lib, "GHOST", "NEWX", registry=reg, run_id="r1", ex_date="2022-06-09") is False
    assert not lib.has_symbol("NEWX")
