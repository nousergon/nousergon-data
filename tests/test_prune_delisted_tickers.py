"""Tests for builders/prune_delisted_tickers.py.

Two-condition guard:
  (A) ticker absent from latest constituents.json::tickers
  (B) last_date older than --absent-days threshold

Either condition alone must NOT prune — that's the no-flapping invariant.
A Wikipedia parsing hiccup (constituents missing a real ticker) or a
multi-week daily_closes outage (last_date stale on a still-listed ticker)
would otherwise blow up valid universe entries.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from builders import prune_delisted_tickers as _mod


def _build_pointer_payload(weekly_date: str = "2026-04-25") -> dict:
    return {
        "date": weekly_date,
        "s3_prefix": f"market_data/weekly/{weekly_date}/",
    }


def _build_constituents_payload(tickers: list[str]) -> dict:
    return {
        "date": "2026-04-25",
        "tickers": list(tickers),
        "sector_map": {t: "Industrials" for t in tickers},
    }


def _stub_s3(*, constituents_tickers: list[str], weekly_date: str = "2026-04-25"):
    """Return a MagicMock s3 client whose get_object serves pointer +
    constituents.json + records put_object calls."""
    s3 = MagicMock()
    pointer_body = json.dumps(_build_pointer_payload(weekly_date)).encode()
    constituents_body = json.dumps(_build_constituents_payload(constituents_tickers)).encode()

    def fake_get_object(**kwargs):
        key = kwargs["Key"]
        if key == "market_data/latest_weekly.json":
            return {"Body": MagicMock(read=lambda: pointer_body)}
        if key.endswith("/constituents.json"):
            return {"Body": MagicMock(read=lambda: constituents_body)}
        raise KeyError(f"unexpected key {key}")

    s3.get_object.side_effect = fake_get_object
    return s3


def _stub_universe_lib(*, symbols: list[str], last_dates: dict[str, str]):
    """Return a MagicMock universe_lib whose tail() returns a frame with
    a single row at the given last_date for each symbol."""
    lib = MagicMock()
    lib.list_symbols.return_value = symbols

    def fake_tail(symbol, n):
        if symbol not in last_dates:
            return MagicMock(data=pd.DataFrame())
        idx = pd.DatetimeIndex([last_dates[symbol]])
        return MagicMock(data=pd.DataFrame({"Close": [100.0]}, index=idx))

    lib.tail.side_effect = fake_tail
    return lib


def _patch_targets(monkeypatch, *, s3_mock, universe_lib_mock):
    """Common patch surface — stub boto3.client + get_universe_lib."""
    monkeypatch.setattr(
        _mod, "boto3", MagicMock(client=lambda *a, **k: s3_mock),
    )
    monkeypatch.setattr(_mod, "get_universe_lib", lambda *a, **k: universe_lib_mock)
    # PR6: prune now runs rename detection before deletion. Default the polygon
    # seam to a fake that reports NO ticker_change for any candidate, so the
    # two-condition prune invariants below behave exactly as pre-PR6 (a genuine
    # delist with no rename). Tests that exercise renames patch this themselves.
    no_rename_poly = MagicMock()
    no_rename_poly.get_ticker_events.return_value = []
    monkeypatch.setattr(_mod, "polygon_client", lambda *a, **k: no_rename_poly)


# ── A. Two-condition invariant ─────────────────────────────────────────────────


def test_absent_from_constituents_AND_stale_prunes(monkeypatch):
    """Both conditions met → ticker pruned."""
    s3 = _stub_s3(constituents_tickers=["AAPL", "MSFT"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "MSFT", "HOLX"],
        last_dates={
            "AAPL": "2026-04-25", "MSFT": "2026-04-25",
            "HOLX": "2026-04-06",  # 22 days stale @ today=2026-04-28
        },
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 1
    assert summary["pruned"][0]["ticker"] == "HOLX"
    lib.delete.assert_called_once_with("HOLX")


def test_absent_but_recent_does_not_prune(monkeypatch):
    """Absent from constituents but last_date is recent → flapping
    guard fires, no prune. This is the 'Wikipedia hiccup' scenario."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "MSFT"],
        # MSFT is missing from constituents (simulated parsing miss)
        # but its data is fresh — must not be deleted.
        last_dates={"AAPL": "2026-04-25", "MSFT": "2026-04-25"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["skipped_recent_count"] == 1
    assert summary["skipped_recent"][0]["ticker"] == "MSFT"
    lib.delete.assert_not_called()


def test_present_in_constituents_but_stale_does_not_prune(monkeypatch):
    """Present in constituents → never a candidate, even if last_date
    is stale. This is the 'daily_closes outage' scenario."""
    s3 = _stub_s3(constituents_tickers=["AAPL", "MSFT"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "MSFT"],
        # MSFT is in constituents but data is 22d old (e.g. polygon
        # 403 streak). Must not delete a still-listed ticker.
        last_dates={"AAPL": "2026-04-25", "MSFT": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["candidates_count"] == 0
    lib.delete.assert_not_called()


# ── B. Macro / sector ETF protection ───────────────────────────────────────────


def test_macro_keys_never_pruned(monkeypatch):
    """SPY, VIX, etc. live in macro_lib, but if any leaked into
    universe_lib by accident, they must never be touched here. Their
    absence from the equity constituents list is by design."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "SPY", "VIX", "GLD"],
        last_dates={
            "AAPL": "2026-04-25", "SPY": "2026-04-06",
            "VIX": "2026-04-06", "GLD": "2026-04-06",
        },
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    lib.delete.assert_not_called()


def test_sector_etfs_never_pruned(monkeypatch):
    """XLK / XLE / XLF / etc. — same protection as macro keys."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "XLK", "XLE", "XLF"],
        last_dates={
            "AAPL": "2026-04-25",
            "XLK": "2026-04-06", "XLE": "2026-04-06", "XLF": "2026-04-06",
        },
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    lib.delete.assert_not_called()


# ── C. Dry-run vs apply ────────────────────────────────────────────────────────


def test_dry_run_does_not_call_delete(monkeypatch):
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX"],
        last_dates={"AAPL": "2026-04-25", "HOLX": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=False,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 1  # plan still records it
    assert summary["applied"] is False
    lib.delete.assert_not_called()


def test_apply_calls_delete_per_ticker(monkeypatch):
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX", "RACE"],
        last_dates={
            "AAPL": "2026-04-25",
            "HOLX": "2026-04-06", "RACE": "2026-04-01",
        },
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 2
    assert lib.delete.call_count == 2
    deleted = sorted(call.args[0] for call in lib.delete.call_args_list)
    assert deleted == ["HOLX", "RACE"]


# ── D. Override path ───────────────────────────────────────────────────────────


def test_tickers_override_skips_constituents_diff(monkeypatch):
    """--tickers HOLX bypasses the constituents diff but still gates on
    last_date staleness — operator can't blow up a fresh symbol via typo."""
    # Note: constituents could even contain HOLX — override skips that check.
    s3 = _stub_s3(constituents_tickers=["AAPL", "HOLX"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX"],
        last_dates={"AAPL": "2026-04-25", "HOLX": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        tickers_override=["HOLX"],
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 1
    assert summary["pruned"][0]["ticker"] == "HOLX"


def test_tickers_override_still_gates_on_staleness(monkeypatch):
    """Even with explicit override, a fresh-data ticker is NOT deleted —
    refuses to blow up a still-active symbol via operator typo."""
    s3 = _stub_s3(constituents_tickers=["AAPL", "MSFT"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "MSFT"],
        last_dates={"AAPL": "2026-04-25", "MSFT": "2026-04-25"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        tickers_override=["MSFT"],  # operator typo — MSFT is fresh + listed
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["skipped_recent_count"] == 1
    lib.delete.assert_not_called()


def test_tickers_override_ignores_unknown_symbols(monkeypatch):
    """A symbol not in ArcticDB gets logged but doesn't raise — the
    override is a hint, not a contract."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL"], last_dates={"AAPL": "2026-04-25"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        tickers_override=["NONEXISTENT"],
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["candidates_count"] == 0


# ── E. Refuse-to-delete-what-we-cant-verify ────────────────────────────────────


def test_unreadable_tail_skips_not_prunes(monkeypatch):
    """If tail(1) raises, refuse to delete — we can't verify staleness.
    Operator must investigate manually."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = MagicMock()
    lib.list_symbols.return_value = ["AAPL", "BROKEN"]
    # AAPL reads fine; BROKEN raises.
    def fake_tail(symbol, n):
        if symbol == "AAPL":
            idx = pd.DatetimeIndex(["2026-04-25"])
            return MagicMock(data=pd.DataFrame({"Close": [100.0]}, index=idx))
        raise RuntimeError("ArcticDB read failed")
    lib.tail.side_effect = fake_tail
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["skipped_unreadable_count"] == 1
    assert "BROKEN" in summary["skipped_unreadable"]
    lib.delete.assert_not_called()


def test_empty_series_skips(monkeypatch):
    """Empty DataFrame from tail(1) → can't verify last_date → skip."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = MagicMock()
    lib.list_symbols.return_value = ["AAPL", "EMPTYSYMB"]
    def fake_tail(symbol, n):
        if symbol == "AAPL":
            idx = pd.DatetimeIndex(["2026-04-25"])
            return MagicMock(data=pd.DataFrame({"Close": [100.0]}, index=idx))
        return MagicMock(data=pd.DataFrame())
    lib.tail.side_effect = fake_tail
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert summary["skipped_unreadable_count"] == 1


# ── F. Hard-fail on bad input ──────────────────────────────────────────────────


def test_empty_constituents_raises(monkeypatch):
    """An empty constituents.json::tickers list would mean every ArcticDB
    symbol becomes a candidate — refuse loudly. This is a defensive check
    against a constituents-fetch regression."""
    s3 = MagicMock()
    pointer_body = json.dumps(_build_pointer_payload()).encode()
    bad_constituents = json.dumps({"tickers": [], "date": "2026-04-25"}).encode()

    def fake_get_object(**kwargs):
        if kwargs["Key"] == "market_data/latest_weekly.json":
            return {"Body": MagicMock(read=lambda: pointer_body)}
        return {"Body": MagicMock(read=lambda: bad_constituents)}
    s3.get_object.side_effect = fake_get_object

    lib = _stub_universe_lib(symbols=["AAPL"], last_dates={"AAPL": "2026-04-25"})
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    # Post-L1397 (lift-to-shared-helper): error message broadened from
    # prune-specific "empty universe" to the shared helper's "empty
    # constituents set" (serves both backfill and prune call sites).
    with pytest.raises(RuntimeError, match="empty constituents set"):
        _mod.prune_delisted_tickers(
            absent_days=14, apply=True,
            today=pd.Timestamp("2026-04-28"),
        )


def test_arctic_delete_failure_propagates(monkeypatch):
    """If lib.delete() raises mid-loop, propagate so the operator sees a
    half-pruned state and investigates rather than silently 'completing'."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = MagicMock()
    lib.list_symbols.return_value = ["AAPL", "HOLX", "RACE"]

    def fake_tail(symbol, n):
        if symbol == "AAPL":
            idx = pd.DatetimeIndex(["2026-04-25"])
        else:
            idx = pd.DatetimeIndex(["2026-04-06"])
        return MagicMock(data=pd.DataFrame({"Close": [100.0]}, index=idx))

    lib.tail.side_effect = fake_tail
    # First delete succeeds, second raises
    lib.delete.side_effect = [None, RuntimeError("S3 5xx")]
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    with pytest.raises(RuntimeError, match="Failed to delete"):
        _mod.prune_delisted_tickers(
            absent_days=14, apply=True,
            today=pd.Timestamp("2026-04-28"),
        )


# ── G. Audit trail ─────────────────────────────────────────────────────────────


def test_audit_written_on_dry_run(monkeypatch):
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX"],
        last_dates={"AAPL": "2026-04-25", "HOLX": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    _mod.prune_delisted_tickers(
        absent_days=14, apply=False,
        today=pd.Timestamp("2026-04-28"),
    )

    s3.put_object.assert_called_once()
    call = s3.put_object.call_args
    assert call.kwargs["Key"].startswith("builders/prune_audit/2026-04-28-")
    assert call.kwargs["Key"].endswith("-dryrun.json")
    body = json.loads(call.kwargs["Body"])
    assert body["pruned_count"] == 1
    assert body["pruned"][0]["ticker"] == "HOLX"


def test_audit_written_on_apply(monkeypatch):
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX"],
        last_dates={"AAPL": "2026-04-25", "HOLX": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    s3.put_object.assert_called_once()
    call = s3.put_object.call_args
    assert call.kwargs["Key"].endswith("-apply.json")


def test_audit_failure_does_not_block_prune(monkeypatch):
    """put_object failure is observability — must not cause the prune
    operation to roll back. The operator already saw the WARN log of the
    delete; the audit miss is a downstream concern."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    s3.put_object.side_effect = RuntimeError("S3 down")
    lib = _stub_universe_lib(
        symbols=["AAPL", "HOLX"],
        last_dates={"AAPL": "2026-04-25", "HOLX": "2026-04-06"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True,
        today=pd.Timestamp("2026-04-28"),
    )

    # Prune still happened; audit failure was swallowed with WARN.
    assert summary["pruned_count"] == 1
    lib.delete.assert_called_once_with("HOLX")


# ── E. constituents_override (in-process freshness reference) ──────────────────


def test_constituents_override_uses_in_process_set(monkeypatch):
    """When the caller passes constituents_override, the latest_weekly.json
    pointer read is bypassed entirely — the in-process set is authoritative.
    Lets a caller that just refreshed constituents in-process (e.g.
    pre-MorningEnrich preflight) prune against the freshest membership
    without updating the public pointer (which has cross-module fan-out)."""
    s3 = MagicMock()
    # If the override is honored, get_object must NOT be called for the pointer
    # — pointer fetch would hit the side_effect and raise.
    s3.get_object.side_effect = AssertionError(
        "pointer S3 read must not happen when constituents_override is set"
    )
    lib = _stub_universe_lib(
        symbols=["AAPL", "STRAGGLER"],
        last_dates={"AAPL": "2026-04-25", "STRAGGLER": "2026-04-20"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=5, apply=True,
        today=pd.Timestamp("2026-04-28"),
        constituents_override={"AAPL"},  # STRAGGLER absent from this set
    )

    assert summary["pruned_count"] == 1
    assert summary["pruned"][0]["ticker"] == "STRAGGLER"
    assert summary["constituents_date"] == "(in-process override)"


def test_constituents_override_accepts_list_or_set(monkeypatch):
    """Accept either set or list for ergonomic caller flexibility — the
    pre-MorningEnrich code path has the tickers as a list and shouldn't
    have to convert to a set just to satisfy the parameter type."""
    s3 = MagicMock()
    s3.put_object = MagicMock()
    lib = _stub_universe_lib(
        symbols=["AAPL"],
        last_dates={"AAPL": "2026-04-25"},
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    # Both invocations should succeed without TypeError.
    _mod.prune_delisted_tickers(
        absent_days=5, apply=False,
        today=pd.Timestamp("2026-04-28"),
        constituents_override=["AAPL"],  # list
    )
    _mod.prune_delisted_tickers(
        absent_days=5, apply=False,
        today=pd.Timestamp("2026-04-28"),
        constituents_override={"AAPL"},  # set
    )


def test_constituents_override_still_gates_on_last_date(monkeypatch):
    """Override only swaps the freshness reference — the last_date staleness
    gate still applies. A ticker absent from the override but with a fresh
    last_date stays put. Locks the no-flapping invariant for the override
    code path too."""
    s3 = MagicMock()
    lib = _stub_universe_lib(
        symbols=["AAPL", "FRESH_BUT_ABSENT"],
        last_dates={
            "AAPL": "2026-04-25",
            "FRESH_BUT_ABSENT": "2026-04-27",  # 1 day stale, well under threshold
        },
    )
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    summary = _mod.prune_delisted_tickers(
        absent_days=5, apply=True,
        today=pd.Timestamp("2026-04-28"),
        constituents_override={"AAPL"},
    )

    assert summary["pruned_count"] == 0
    assert summary["skipped_recent_count"] == 1


def test_tickers_override_and_constituents_override_mutually_exclusive(monkeypatch):
    """Two semantically distinct overrides must not be mixed — the former
    targets a delete list, the latter swaps the freshness reference. Mixing
    them would silently use only one and the other's intent would be lost."""
    s3 = MagicMock()
    lib = _stub_universe_lib(symbols=["AAPL"], last_dates={"AAPL": "2026-04-25"})
    _patch_targets(monkeypatch, s3_mock=s3, universe_lib_mock=lib)

    with pytest.raises(ValueError, match="mutually exclusive"):
        _mod.prune_delisted_tickers(
            absent_days=5, apply=False,
            today=pd.Timestamp("2026-04-28"),
            tickers_override=["AAPL"],
            constituents_override={"AAPL"},
        )


# ── D. PR6: rename-triggered migration BEFORE the prune deletion ──────────────

import io  # noqa: E402

import numpy as np  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

import corporate_actions as ca  # noqa: E402


class _FakeS3Registry:
    """In-memory S3 double with proper 404 semantics so a real
    CorporateActionRegistry round-trips its write-once markers (a MagicMock s3
    would make head_object truthy and falsely report everything 'applied')."""

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


def _real_universe_lib(tmp_path, symbols: dict[str, str]):
    """A REAL LMDB ArcticDB universe seeded with one row per symbol at the given
    last_date (so has_symbol / read / write / delete all behave for real)."""
    adb = pytest.importorskip("arcticdb")
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    for ticker, last_date in symbols.items():
        idx = pd.DatetimeIndex([pd.Timestamp(last_date)])
        df = pd.DataFrame(
            {"Open": [1.0], "High": [1.0], "Low": [1.0],
             "Close": [100.0], "Volume": [1e6]},
            index=idx,
        )
        lib.write(ticker, df)
    return lib


def _patch_rename(monkeypatch, *, s3_mock, universe_lib, events_by_ticker, raise_for=None):
    """Patch prune for the rename phase: boto3 + get_universe_lib + a real
    registry (proper marker semantics) + a fake polygon ticker-events client."""
    monkeypatch.setattr(_mod, "boto3", MagicMock(client=lambda *a, **k: s3_mock))
    monkeypatch.setattr(_mod, "get_universe_lib", lambda *a, **k: universe_lib)
    reg = ca.CorporateActionRegistry(_FakeS3Registry(), "alpha-engine-research")
    monkeypatch.setattr(_mod, "_build_registry", lambda *a, **k: reg)

    raise_for = raise_for or set()
    poly = MagicMock()

    def fake_events(ticker):
        if ticker in raise_for:
            raise RuntimeError(f"polygon down for {ticker}")
        # Translate (date,new) into the get_ticker_events pair shape.
        return events_by_ticker.get(ticker, [])

    poly.get_ticker_events.side_effect = fake_events
    monkeypatch.setattr(_mod, "polygon_client", lambda *a, **k: poly)
    return reg


def test_renamed_candidate_is_migrated_not_pruned(monkeypatch, tmp_path):
    """FB renamed -> META: FB is MIGRATED (history under META, FB gone) and NOT
    in the prune list; a true delist (DEAD) IS pruned. Migration runs BEFORE the
    deletion (FB's history survives under META rather than being deleted)."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _real_universe_lib(tmp_path, {
        "AAPL": "2026-04-25",   # in constituents → never a candidate
        "FB": "2026-04-01",     # absent + stale → candidate, but RENAMED
        "DEAD": "2026-04-01",   # absent + stale → candidate, true delist
    })
    _patch_rename(
        monkeypatch, s3_mock=s3, universe_lib=lib,
        events_by_ticker={
            "FB": [{"date": "2026-04-10", "old_ticker": "FB", "new_ticker": "META"}],
            "DEAD": [],
        },
    )

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True, today=pd.Timestamp("2026-04-28"),
    )

    # FB migrated, not pruned.
    assert summary["migrated_count"] == 1
    assert summary["migrated"][0]["old_ticker"] == "FB"
    assert summary["migrated"][0]["new_ticker"] == "META"
    assert summary["migrated"][0]["migrated"] is True
    pruned_tickers = {p["ticker"] for p in summary["pruned"]}
    assert "FB" not in pruned_tickers
    # FB's history carried to META (proves migration ran before any delete).
    assert lib.has_symbol("META")
    assert not lib.has_symbol("FB")
    # True delist pruned.
    assert "DEAD" in pruned_tickers
    assert not lib.has_symbol("DEAD")
    assert lib.has_symbol("AAPL")


def test_polygon_detection_failure_does_not_prune_candidate(monkeypatch, tmp_path):
    """History-safety: a candidate whose ticker-events query RAISES must NOT be
    pruned this pass (it might be an undetected rename), while a confirmed delist
    still prunes."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _real_universe_lib(tmp_path, {
        "AAPL": "2026-04-25",
        "FB": "2026-04-01",     # detection RAISES → must be skipped, not pruned
        "DEAD": "2026-04-01",   # confirmed no rename → pruned
    })
    _patch_rename(
        monkeypatch, s3_mock=s3, universe_lib=lib,
        events_by_ticker={"DEAD": []},
        raise_for={"FB"},
    )

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True, today=pd.Timestamp("2026-04-28"),
    )

    pruned_tickers = {p["ticker"] for p in summary["pruned"]}
    assert "FB" not in pruned_tickers
    assert "FB" in summary["skipped_rename_detect_failed"]
    assert lib.has_symbol("FB")  # history preserved — NOT deleted on a failure
    # The confirmed delist still prunes.
    assert "DEAD" in pruned_tickers
    assert not lib.has_symbol("DEAD")


def test_no_polygon_client_refuses_to_prune_blind(monkeypatch, tmp_path):
    """If the polygon client can't be constructed, rename detection is
    impossible → refuse to prune ANY candidate this pass (history-safety)."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _real_universe_lib(tmp_path, {
        "AAPL": "2026-04-25", "DEAD": "2026-04-01",
    })
    monkeypatch.setattr(_mod, "boto3", MagicMock(client=lambda *a, **k: s3))
    monkeypatch.setattr(_mod, "get_universe_lib", lambda *a, **k: lib)
    reg = ca.CorporateActionRegistry(_FakeS3Registry(), "alpha-engine-research")
    monkeypatch.setattr(_mod, "_build_registry", lambda *a, **k: reg)
    monkeypatch.setattr(_mod, "polygon_client", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no key")))

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=True, today=pd.Timestamp("2026-04-28"),
    )

    assert summary["pruned_count"] == 0
    assert "DEAD" in summary["skipped_rename_detect_failed"]
    assert lib.has_symbol("DEAD")


def test_dry_run_does_not_migrate_or_prune(monkeypatch, tmp_path):
    """apply=False: detection reports the rename but NO ArcticDB mutation."""
    s3 = _stub_s3(constituents_tickers=["AAPL"])
    lib = _real_universe_lib(tmp_path, {
        "AAPL": "2026-04-25", "FB": "2026-04-01",
    })
    _patch_rename(
        monkeypatch, s3_mock=s3, universe_lib=lib,
        events_by_ticker={
            "FB": [{"date": "2026-04-10", "old_ticker": "FB", "new_ticker": "META"}],
        },
    )

    summary = _mod.prune_delisted_tickers(
        absent_days=14, apply=False, today=pd.Timestamp("2026-04-28"),
    )

    # Reported as a would-migrate, but not actually migrated, and FB untouched.
    assert summary["migrated_count"] == 0
    assert {m["old_ticker"] for m in summary["migrated"]} == {"FB"}
    assert lib.has_symbol("FB")
    assert not lib.has_symbol("META")
    assert summary["pruned_count"] == 0
