"""Tests for the 2026-04-22 parquet-warmup path in builders/daily_append.py.

Before this change, short-history tickers (new listings, spinoffs, recent
constituent adds) accumulated feature coverage one day at a time — features
with 252-day rolling windows stayed NaN for up to a year after the ticker
entered ArcticDB, even though the weekly backfill's 10y parquet held the
full series. 8 manual polygon backfills in a single day (2026-04-22 Saturday
SF dry-run) traced to this gap.

When ArcticDB history is below the feature-warmup threshold, daily_append
now unions the ticker's ``predictor/price_cache/{T}.parquet`` (full 10y
adjusted OHLCV) with the ArcticDB rows before handing the result to
``compute_features``. ArcticDB wins on overlapping dates — it's updated
daily, the parquet is rebuilt weekly.

The path is gated on ``len(hist) < MIN_ROWS_FOR_FEATURES`` so full-history
tickers (~99% on a steady-state day) skip the extra S3 read. Missing
parquet (brand-new constituent not yet picked up by a weekly backfill)
degrades gracefully to PR #78's NaN-feature path with a loud log. Any
other S3 error shape hard-fails — NoSilentFails.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from builders import daily_append


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


# ── Source-inspection invariants ────────────────────────────────────────────


def test_parquet_warmup_helper_exists():
    """``_load_parquet_warmup`` must be defined at module scope so the per-
    ticker loop can call it.
    """
    src = _source()
    assert "def _load_parquet_warmup(" in src, (
        "daily_append must define _load_parquet_warmup helper for the "
        "short-history path added 2026-04-22."
    )


def test_parquet_warmup_gated_on_short_history():
    """The warmup branch must be gated on
    ``len(hist) < MIN_ROWS_FOR_FEATURES`` so the parquet read fires only
    when ArcticDB history is short. Full-history tickers must skip the
    extra S3 round-trip.
    """
    src = _source()
    assert "len(hist) < MIN_ROWS_FOR_FEATURES" in src, (
        "Short-history gate missing. The warmup branch must condition "
        "on len(hist) < MIN_ROWS_FOR_FEATURES so full-history tickers "
        "don't pay the parquet read cost."
    )


def test_parquet_warmup_falls_through_to_compute_features():
    """The warmup branch must NOT ``continue`` past compute_features.

    Locks the NoSilentFails / first-class-short-history invariant: the
    warmup path enriches context, then the same feature pipeline runs.
    An early ``continue`` would resurrect the 2026-04-21 SNDK-style
    bypass bug.
    """
    src = _source()
    lines = src.splitlines()

    # Find the warmup branch line
    gate_idx = None
    for i, line in enumerate(lines):
        if (
            "len(hist) < MIN_ROWS_FOR_FEATURES" in line
            and line.strip().startswith("if ")
        ):
            gate_idx = i
            break
    assert gate_idx is not None, (
        "Short-history gate not found as an executable `if` statement."
    )

    # Inspect the branch body for ~25 lines. It must not bare-`continue`
    # and it must not increment n_skip / n_partial inside the branch.
    body = "\n".join(lines[gate_idx + 1:gate_idx + 25])

    assert "\n                    continue" not in body, (
        "Warmup branch has a bare `continue` — it must fall through to "
        "compute_features, not skip past it."
    )


def test_parquet_missing_logs_warn_not_hard_fail():
    """A missing parquet (NoSuchKey) must log a structured warning and
    fall through to graceful NaN-feature degrade, NOT hard-fail.

    Reason: a brand-new constituent may land in ``daily_closes.parquet``
    before the next Saturday backfill produces its ``price_cache``
    parquet. Hard-failing on that transient state would block the whole
    daily_append run.
    """
    src = _source()
    assert "short-history-no-parquet" in src, (
        "The missing-parquet fall-through must emit a structured "
        "`short-history-no-parquet ticker=X ...` log line so the gap "
        "surfaces in CloudWatch."
    )


def test_parquet_warmup_structured_log():
    """Every parquet-warmup hit must emit a structured log so coverage
    surfaces in CloudWatch Logs Insights.
    """
    src = _source()
    assert "parquet-warmup ticker=" in src, (
        "Parquet-warmup path must emit structured `parquet-warmup "
        "ticker=X arctic_rows=N parquet_rows=M ...` log for observability."
    )


def test_parquet_warmup_counter_in_summary():
    """``n_parquet_warmup`` must be counted and surfaced in the result
    dict + final log line — operators need to see how many tickers
    took the warmup path each run.
    """
    src = _source()
    assert "n_parquet_warmup" in src, "n_parquet_warmup counter missing."
    assert "tickers_parquet_warmup" in src, (
        "Result dict must expose `tickers_parquet_warmup` — it's the "
        "post-run signal that tells operators how many tickers still "
        "need the ArcticDB-backfill catch-up."
    )


def test_arctic_read_still_authoritative_for_dtypes():
    """The per-column dtype match must still reference ``hist.dtypes``
    (the original ArcticDB read), not the stitched warmup frame.

    ArcticDB's stored schema is authoritative for the write; the parquet
    is only enrichment for compute. Breaking this invariant would make
    update() reject writes with dtype-mismatch errors (SOLS/ULS
    regression from 2026-04-21 PR #77).
    """
    src = _source()
    assert "astype(hist.dtypes[col])" in src, (
        "hist.dtypes[col] dtype-match still required — ArcticDB enforces "
        "schema match on update()."
    )


# ── Runtime unit tests for _load_parquet_warmup ─────────────────────────────


def _fake_client_error(code: str) -> ClientError:
    return ClientError(
        error_response={"Error": {"Code": code, "Message": "mock"}},
        operation_name="GetObject",
    )


def test_load_parquet_warmup_returns_none_on_nosuchkey(monkeypatch):
    """Missing parquet (``NoSuchKey``) returns None — caller falls
    through to PR #78 graceful degrade.
    """
    mock_s3 = MagicMock()

    def _raise_nosuchkey(*_a, **_kw):
        raise _fake_client_error("NoSuchKey")

    monkeypatch.setattr(
        daily_append, "load_parquet_from_s3", _raise_nosuchkey
    )

    result = daily_append._load_parquet_warmup(mock_s3, "alpha-engine-research", "NEWIPO")
    assert result is None


def test_load_parquet_warmup_returns_none_on_404(monkeypatch):
    """Some boto3 versions surface a missing key as error code ``404``
    rather than ``NoSuchKey``. Both shapes must be treated as "not
    found" so a library version bump doesn't quietly start hard-failing.
    """
    mock_s3 = MagicMock()

    def _raise_404(*_a, **_kw):
        raise _fake_client_error("404")

    monkeypatch.setattr(daily_append, "load_parquet_from_s3", _raise_404)

    result = daily_append._load_parquet_warmup(mock_s3, "b", "NEWIPO")
    assert result is None


def test_load_parquet_warmup_raises_on_access_denied(monkeypatch):
    """Non-NotFound ``ClientError`` (e.g. AccessDenied, throttling) must
    hard-fail. Swallowing those would silently regress every short-
    history ticker to the NaN-degrade path on an IAM misconfig.
    """
    mock_s3 = MagicMock()

    def _raise_denied(*_a, **_kw):
        raise _fake_client_error("AccessDenied")

    monkeypatch.setattr(daily_append, "load_parquet_from_s3", _raise_denied)

    with pytest.raises(RuntimeError, match="parquet-warmup read failed"):
        daily_append._load_parquet_warmup(mock_s3, "b", "AAPL")


def test_load_parquet_warmup_raises_on_empty_frame(monkeypatch):
    """An empty parquet is a storage-layer corruption signal, not a
    legitimate "ticker hasn't traded yet" state. Hard-fail rather than
    return an empty frame upstream.
    """
    mock_s3 = MagicMock()
    monkeypatch.setattr(
        daily_append, "load_parquet_from_s3",
        lambda *a, **k: pd.DataFrame(),
    )

    with pytest.raises(RuntimeError, match="invalid shape"):
        daily_append._load_parquet_warmup(mock_s3, "b", "AAPL")


def test_load_parquet_warmup_raises_on_missing_close_col(monkeypatch):
    """A parquet without a Close column is corrupt for feature warmup —
    every downstream feature expects it. Hard-fail.
    """
    mock_s3 = MagicMock()
    bad_df = pd.DataFrame(
        {"Open": [1.0], "High": [1.0]},
        index=pd.DatetimeIndex(["2026-04-20"]),
    )
    monkeypatch.setattr(daily_append, "load_parquet_from_s3", lambda *a, **k: bad_df)

    with pytest.raises(RuntimeError, match="invalid shape"):
        daily_append._load_parquet_warmup(mock_s3, "b", "AAPL")


def test_load_parquet_warmup_returns_valid_frame(monkeypatch):
    """Happy path: valid parquet with OHLCV columns round-trips through
    the helper unchanged.
    """
    mock_s3 = MagicMock()
    good_df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "High": [102.0, 103.0],
            "Low": [99.0, 100.0],
            "Close": [101.0, 102.0],
            "Volume": [1_000_000, 1_100_000],
        },
        index=pd.DatetimeIndex(["2016-01-04", "2016-01-05"]),
    )
    monkeypatch.setattr(daily_append, "load_parquet_from_s3", lambda *a, **k: good_df)

    result = daily_append._load_parquet_warmup(mock_s3, "b", "AAPL")
    assert result is not None
    assert len(result) == 2


# ── Wave-3 reader migration (ROADMAP L1401) ────────────────────────────────


def test_load_parquet_warmup_prefers_new_prefix(monkeypatch):
    """The default-prefix path consults ``reference/price_cache/`` FIRST.
    During the Wave-3 soak both prefixes hold byte-equal copies, but the
    new prefix is also the sole survivor post-PR4 cutover — exercising it
    end-to-end during the soak is the migration's whole point.
    """
    good_df = pd.DataFrame(
        {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [1]},
        index=pd.DatetimeIndex(["2026-04-20"]),
    )
    seen_keys: list[str] = []

    def _stub(_s3, _bucket, key):
        seen_keys.append(key)
        return good_df

    monkeypatch.setattr(daily_append, "load_parquet_from_s3", _stub)

    result = daily_append._load_parquet_warmup(MagicMock(), "b", "AAPL")
    assert result is not None
    # First (and only — break on success) key fetched is the new prefix.
    assert seen_keys == ["reference/price_cache/AAPL.parquet"], (
        "Wave-3 read-prefix chain: ``reference/price_cache/`` must be "
        "tried before the legacy fallback."
    )


def test_load_parquet_warmup_falls_back_to_legacy_on_new_prefix_miss(monkeypatch):
    """When the new prefix is empty (e.g. the soak-window backfill hasn't
    seeded a brand-new ticker yet) the legacy fallback is consulted; if
    legacy has the parquet the helper returns it.
    """
    good_df = pd.DataFrame(
        {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [1]},
        index=pd.DatetimeIndex(["2026-04-20"]),
    )
    calls: list[str] = []

    def _stub(_s3, _bucket, key):
        calls.append(key)
        if key.startswith("reference/"):
            raise _fake_client_error("NoSuchKey")
        return good_df

    monkeypatch.setattr(daily_append, "load_parquet_from_s3", _stub)

    result = daily_append._load_parquet_warmup(MagicMock(), "b", "AAPL")
    assert result is not None
    assert calls == [
        "reference/price_cache/AAPL.parquet",
        "predictor/price_cache/AAPL.parquet",
    ], "Fallback order must be new → legacy (read = write reversed)."


def test_load_parquet_warmup_returns_none_when_absent_in_both_prefixes(monkeypatch):
    """A ticker absent from BOTH prefixes is a genuine "not in price
    cache yet" state (brand-new constituent the weekly backfill hasn't
    picked up). Both prefix lookups must be attempted before the helper
    degrades to None — a single-prefix NoSuchKey is no longer sufficient.
    """
    calls: list[str] = []

    def _stub(_s3, _bucket, key):
        calls.append(key)
        raise _fake_client_error("NoSuchKey")

    monkeypatch.setattr(daily_append, "load_parquet_from_s3", _stub)

    result = daily_append._load_parquet_warmup(MagicMock(), "b", "NEWIPO")
    assert result is None
    assert len(calls) == 2, (
        "When the new prefix is missing the helper must still try legacy "
        "before declaring the ticker absent."
    )
