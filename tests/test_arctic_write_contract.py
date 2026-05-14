"""Regression: store.arctic_store.to_arctic_safe must strip Categorical dtypes.

2026-05-12 EOD incident: ``builders/daily_append.py``'s ``update_batch`` call
raised ``arcticdb.exceptions.ArcticDbNotYetImplemented`` on BRK-B because
PR #211 ("perf(provenance): categorical dtype for source column") had
converted the ``source`` column to ``pd.CategoricalDtype`` for memory savings
in ``_apply_daily_delta``, and ArcticDB's ``_handle_categorical_columns``
rejects categoricals on every append/update path.

The institutional fix keeps PR #211's in-memory memory win (~108MB across
the universe pass) and converts to object dtype only at the storage
boundary, via ``store.arctic_store.to_arctic_safe`` — a single named
helper called immediately before every ``update_batch`` / ``write_batch`` /
``write`` invocation.

This test pins the contract: the helper must convert Categorical → object
without mutating the caller's frame, must preserve values + index, and
must short-circuit on empty / no-categorical frames (no needless copies).
A regression that re-introduces categoricals at the write boundary —
either by removing the wrap or by changing the helper's behavior — should
fail loudly here.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from store.arctic_store import to_arctic_safe


_REPO_ROOT = Path(__file__).parent.parent


def _make_payload_with_categorical_source(n: int = 5) -> pd.DataFrame:
    """Mimic the shape produced by ``features.compute._apply_daily_delta``:
    OHLCV + ``source`` as ``CategoricalDtype`` per PR #211."""
    idx = pd.date_range("2026-05-08", periods=n, freq="B", name="date")
    return pd.DataFrame(
        {
            "Open": range(100, 100 + n),
            "High": range(101, 101 + n),
            "Low": range(99, 99 + n),
            "Close": range(100, 100 + n),
            "Volume": [1_000_000] * n,
            "source": pd.Categorical(
                ["yfinance"] * n,
                categories=("polygon", "yfinance", "fred", "unknown"),
            ),
        },
        index=idx,
    )


def test_to_arctic_safe_strips_categorical_source_column():
    """The 2026-05-12 EOD failure mode: Categorical ``source`` column must
    be converted to object dtype before ArcticDB sees the payload.

    Without this conversion ArcticDB raises
    ``ArcticDbNotYetImplemented: DataFrame/Series contains categorical
    data, cannot append or update``.
    """
    df = _make_payload_with_categorical_source()
    assert isinstance(df["source"].dtype, pd.CategoricalDtype), (
        "test fixture precondition: payload must start with Categorical "
        "source column (this mirrors features.compute.make_source_series)"
    )

    out = to_arctic_safe(df)

    assert not isinstance(out["source"].dtype, pd.CategoricalDtype), (
        "to_arctic_safe must strip CategoricalDtype — ArcticDB's "
        "update_batch / write_batch reject categoricals."
    )
    assert out["source"].dtype == object, (
        "Cast target must be object dtype (matches PR #196's pre-#211 "
        "storage representation that round-trips cleanly through ArcticDB)."
    )


def test_to_arctic_safe_preserves_values_and_index():
    """Conversion is dtype-only; ticker values, index, and ordering must
    survive untouched. Downstream readers (predictor inference, backtester)
    rely on ``source`` round-tripping as the same string label.
    """
    df = _make_payload_with_categorical_source(n=5)
    df.loc[df.index[2], "source"] = "polygon"  # mix two categories
    df.loc[df.index[4], "source"] = "fred"

    out = to_arctic_safe(df)

    assert list(out["source"]) == ["yfinance", "yfinance", "polygon", "yfinance", "fred"]
    assert list(out.index) == list(df.index)
    assert list(out.columns) == list(df.columns)


def test_to_arctic_safe_does_not_mutate_input():
    """The helper must return a new DataFrame when conversion is needed.
    Mutating the caller's frame would defeat PR #211's memory win
    (the Categorical representation must survive intact in the in-memory
    compute path; only the write-bound copy gets cast).
    """
    df = _make_payload_with_categorical_source()
    before_dtype = df["source"].dtype

    _ = to_arctic_safe(df)

    assert df["source"].dtype == before_dtype, (
        "to_arctic_safe must not mutate the caller's frame — PR #211's "
        "in-memory Categorical representation is the source of the "
        "~108MB memory saving and must survive intact through the "
        "compute path."
    )
    assert isinstance(df["source"].dtype, pd.CategoricalDtype)


def test_to_arctic_safe_returns_input_unchanged_when_no_categoricals():
    """Fast path: frames with no Categorical columns return unchanged
    (no copy). This keeps the wrap cheap to apply uniformly at every
    write site, including the macro_lib writes that have never used
    Categorical.
    """
    idx = pd.date_range("2026-05-08", periods=3, freq="B", name="date")
    df = pd.DataFrame({"Close": [400.0, 401.5, 402.0]}, index=idx)

    out = to_arctic_safe(df)

    assert out is df, (
        "Fast path must return the same object (no copy) when there's "
        "nothing to convert — keeps the wrap free for macro/sector writes."
    )


def test_to_arctic_safe_returns_input_unchanged_when_empty():
    """Empty frame fast path. Empty payloads can appear in dry-run paths
    and on tickers with no rows after filtering."""
    df = pd.DataFrame()

    out = to_arctic_safe(df)

    assert out is df
    assert out.empty


def test_to_arctic_safe_handles_multiple_categorical_columns():
    """Defensive against future writers that add additional categoricals
    (e.g. a per-row ``quality`` flag, a ``vendor`` enum). The helper must
    strip every CategoricalDtype column it finds, not just ``source``.
    """
    idx = pd.date_range("2026-05-08", periods=3, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Close": [100.0, 101.0, 102.0],
            "source": pd.Categorical(["yfinance"] * 3, categories=("yfinance", "polygon")),
            "quality": pd.Categorical(["clean"] * 3, categories=("clean", "warned", "blocked")),
        },
        index=idx,
    )

    out = to_arctic_safe(df)

    assert not isinstance(out["source"].dtype, pd.CategoricalDtype)
    assert not isinstance(out["quality"].dtype, pd.CategoricalDtype)
    assert out["source"].dtype == object
    assert out["quality"].dtype == object


# ── Call-site regressions ────────────────────────────────────────────────────
# Source-level checks that pin the wrap at every ArcticDB write site. A future
# PR that removes the wrap (or adds a new write site without it) trips here.

_DAILY_APPEND_SRC = (_REPO_ROOT / "builders" / "daily_append.py").read_text()
_BACKFILL_SRC = (_REPO_ROOT / "builders" / "backfill.py").read_text()


def test_daily_append_wraps_update_batch_payload():
    """update_batch payload must go through to_arctic_safe — this is the
    exact site that failed 2026-05-12 on BRK-B.
    """
    assert "UpdatePayload(symbol=ticker, data=to_arctic_safe(today_row))" in _DAILY_APPEND_SRC, (
        "daily_append.py's UpdatePayload data must be wrapped in "
        "to_arctic_safe — bare today_row carries Categorical 'source' "
        "(PR #211) which ArcticDB update_batch rejects."
    )


def test_daily_append_wraps_write_batch_payload():
    """The backfill-branch WritePayload also concatenates today_row's
    Categorical source into a combined frame — same wrap required.
    """
    assert "WritePayload(symbol=ticker, data=to_arctic_safe(combined))" in _DAILY_APPEND_SRC, (
        "daily_append.py's WritePayload (backfill branch) must wrap "
        "combined in to_arctic_safe."
    )


def test_backfill_wraps_universe_write():
    """Saturday SF backfill writes per-ticker symbol_df with the same
    Categorical 'source' (PR #211). Must wrap before lib.write.
    """
    assert "universe_lib.write(ticker, to_arctic_safe(symbol_df))" in _BACKFILL_SRC, (
        "backfill.py's universe_lib.write must wrap symbol_df in "
        "to_arctic_safe — PR #211's Categorical source column flows "
        "through here on Saturday SF."
    )


def test_backfill_wraps_macro_writes():
    """Macro writes never use Categorical today, but uniform wrapping
    keeps the contract single-source — and defends against a future
    writer that adds one. Cheap because the helper short-circuits on
    no-categorical frames.
    """
    assert "macro_lib.write(\"features\", to_arctic_safe(macro_df))" in _BACKFILL_SRC
    assert "macro_lib.write(key, to_arctic_safe(macro_series_df))" in _BACKFILL_SRC
    assert "macro_lib.write(key, to_arctic_safe(sector_df))" in _BACKFILL_SRC


def test_daily_append_today_row_column_order_matches_storage():
    """Pin the today_row column order to [OHLCV, source, FEATURES].

    2026-05-14 EOD failure: 99.9% (903 / 904) of update_batch calls failed
    with ``StreamDescriptorMismatch`` because today_row was being built as
    [OHLCV, FEATURES, source] (source appended at the end via the bare
    ``today_row[PROVENANCE_COL] = …`` assignment) while every persisted
    universe symbol carries source at idx 7 (between VWAP and the first
    feature). ArcticDB's update_batch enforces strict column-order match
    against the existing version's descriptor; the backfill-branch
    write_batch path masks this because full rewrite ignores prior
    descriptor.

    The fix re-projects today_row via an explicit column order before the
    write queue. If a future PR removes that re-projection, this test
    fails — preventing a silent re-introduction of the same EOD outage.
    """
    assert (
        "ordered_cols = (\n"
        "                    [c for c in OHLCV_COLS if c in today_row.columns]\n"
        "                    + ([PROVENANCE_COL] if PROVENANCE_COL in today_row.columns else [])\n"
        "                    + [f for f in FEATURES if f in today_row.columns]\n"
        "                )\n"
        "                today_row = today_row[ordered_cols]"
    ) in _DAILY_APPEND_SRC, (
        "daily_append.py must re-project today_row to "
        "[OHLCV, source, FEATURES] before queuing the UpdatePayload — "
        "matches the persisted ArcticDB descriptor (source at idx 7). "
        "The bare `today_row[PROVENANCE_COL] = …` assignment alone "
        "appends source at the end, which trips StreamDescriptorMismatch "
        "on update_batch (2026-05-14 EOD)."
    )
