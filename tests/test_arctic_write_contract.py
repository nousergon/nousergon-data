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

from store.arctic_store import (
    OHLCV_COLS,
    PROVENANCE_COL,
    to_arctic_canonical,
    to_arctic_safe,
)


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
    """update_batch payload must go through ``to_arctic_canonical`` —
    the chokepoint enforces BOTH the column order
    (2026-05-14 incident class) AND the Categorical-strip
    (2026-05-12 BRK-B incident class).
    """
    assert "UpdatePayload(symbol=ticker, data=to_arctic_canonical(today_row))" in _DAILY_APPEND_SRC, (
        "daily_append.py's UpdatePayload data must be wrapped in "
        "to_arctic_canonical — projects to OHLCV+source+FEATURES and "
        "strips Categorical 'source' (PR #211) which ArcticDB "
        "update_batch rejects on both descriptor + dtype mismatches."
    )


def test_daily_append_wraps_write_batch_payload():
    """The backfill-branch WritePayload concatenates ``today_row``'s
    Categorical source into a combined frame whose
    ``pd.concat`` outer-join may have appended novel columns at the
    end (2026-05-21 EOD: PR #279 widened FEATURES, write-path
    rewrote 891/904 symbols with pillars-at-end, the same-day
    EOD UPDATE then failed 905/905). The chokepoint re-projects
    to canonical here.
    """
    assert "WritePayload(symbol=ticker, data=to_arctic_canonical(combined))" in _DAILY_APPEND_SRC, (
        "daily_append.py's WritePayload (backfill branch) must wrap "
        "combined in to_arctic_canonical — re-projects to OHLCV+source"
        "+FEATURES so the persisted descriptor stays canonical."
    )


def test_backfill_wraps_universe_write():
    """Saturday SF backfill writes per-ticker symbol_df via the
    chokepoint. Single source of truth for column order + dtype.
    """
    assert "universe_lib.write(ticker, to_arctic_canonical(symbol_df))" in _BACKFILL_SRC, (
        "backfill.py's universe_lib.write must wrap symbol_df in "
        "to_arctic_canonical — column-order enforcement + Categorical "
        "strip both happen at this single boundary."
    )


def test_backfill_wraps_macro_writes():
    """Macro writes use a different schema from universe (no FEATURES
    column block) so they continue to use ``to_arctic_safe`` directly
    for the Categorical strip. Uniform wrapping keeps the contract
    single-source — and defends against a future writer that adds one.
    """
    assert "macro_lib.write(\"features\", to_arctic_safe(macro_df))" in _BACKFILL_SRC
    assert "macro_lib.write(key, to_arctic_safe(macro_series_df))" in _BACKFILL_SRC
    assert "macro_lib.write(key, to_arctic_safe(sector_df))" in _BACKFILL_SRC


# ── to_arctic_canonical chokepoint contract ──────────────────────────────────
# Round-trip + drop-non-canonical + features-default behaviour. Pins the
# institutional invariant lifted at the 2026-05-22 chokepoint PR: every
# universe write boundary projects to ``OHLCV + source + FEATURES`` before
# the bytes hit ArcticDB, so no caller can violate column order by accident.


def test_to_arctic_canonical_reorders_scrambled_input():
    """If a caller builds a frame with FEATURES inserted mid-column-list
    in the WRONG order (the exact 2026-05-21 EOD failure shape — pillars
    appended at the end via ``pd.concat`` outer-join), the chokepoint
    must re-project to canonical order before the write.
    """
    idx = pd.date_range("2026-05-08", periods=3, freq="B", name="date")
    # Build in a scrambled order to mimic pd.concat([hist, today_row])
    # when today_row carries 2 novel features absent from hist.
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0, 102.0],
            "Close": [100.5, 101.5, 102.5],
            "novel_feat_a": [0.1, 0.2, 0.3],  # appended at end
            "Volume": [1_000_000] * 3,
            "novel_feat_b": [0.4, 0.5, 0.6],  # appended at end
            "source": ["yfinance"] * 3,
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 100.0, 101.0],
            "VWAP": [100.4, 101.4, 102.4],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=["novel_feat_a", "novel_feat_b"])

    expected = ["Open", "High", "Low", "Close", "Volume", "VWAP", "source",
                "novel_feat_a", "novel_feat_b"]
    assert list(out.columns) == expected, (
        "to_arctic_canonical must re-project to OHLCV+source+FEATURES "
        f"order; got {list(out.columns)!r}"
    )


def test_to_arctic_canonical_drops_unknown_columns():
    """Columns outside OHLCV + PROVENANCE + features are silently
    dropped (matches the pre-chokepoint per-site recipe). Defends
    against an accidental leakage of intermediate columns into
    persisted storage.
    """
    idx = pd.date_range("2026-05-08", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "Close": [100.5, 101.5],
            "Volume": [1_000_000, 1_000_000],
            "_internal_debug_col": ["x", "y"],
            "source": ["polygon", "polygon"],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=[])

    assert "_internal_debug_col" not in out.columns
    assert list(out.columns) == ["Open", "Close", "Volume", "source"]


def test_to_arctic_canonical_preserves_values_and_index():
    """Reorder is a pure projection — row values, the index, and the
    Categorical-strip semantics survive untouched.
    """
    idx = pd.date_range("2026-05-08", periods=3, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Close": [100.0, 101.0, 102.0],
            "source": pd.Categorical(
                ["yfinance", "polygon", "yfinance"],
                categories=("polygon", "yfinance", "fred", "unknown"),
            ),
            "Open": [99.5, 100.5, 101.5],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=[])

    assert list(out.columns) == ["Open", "Close", "source"]
    assert list(out.index) == list(idx)
    assert list(out["source"]) == ["yfinance", "polygon", "yfinance"]
    assert out["source"].dtype == object, (
        "to_arctic_canonical must run the Categorical-strip via "
        "to_arctic_safe — single chokepoint for both invariants."
    )


def test_to_arctic_canonical_empty_passthrough():
    """Empty frames return unchanged (no copy, no reorder)."""
    df = pd.DataFrame()
    out = to_arctic_canonical(df, features=[])
    assert out is df
    assert out.empty


def test_to_arctic_canonical_features_default_resolves_to_feature_engineer():
    """When ``features`` is omitted, the helper resolves the default
    from ``features.feature_engineer.FEATURES`` — the same constant
    every steady-state caller imports. Pins the lazy-import default
    contract so the chokepoint is correct without per-call kwargs.
    """
    from features.feature_engineer import FEATURES as _CANONICAL_FEATURES

    idx = pd.date_range("2026-05-08", periods=2, freq="B", name="date")
    # Build with one real FEATURE + one bogus column. The default lookup
    # should preserve the real FEATURE and drop the bogus column.
    real_feat = _CANONICAL_FEATURES[0]
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            real_feat: [0.5, 0.6],
            "_bogus_not_a_feature": [1, 2],
        },
        index=idx,
    )

    out = to_arctic_canonical(df)  # no features kwarg → default lookup

    assert real_feat in out.columns
    assert "_bogus_not_a_feature" not in out.columns
    assert list(out.columns)[0] == "Open"  # OHLCV head ordering preserved


def test_to_arctic_canonical_no_reorder_when_already_canonical():
    """Fast-path: a frame already in canonical order and free of
    categoricals passes through unchanged. Keeps the helper cheap
    enough to apply uniformly at every universe write site.
    """
    idx = pd.date_range("2026-05-08", periods=2, freq="B", name="date")
    df = pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "High": [101.0, 102.0],
            "Low": [99.0, 100.0],
            "Close": [100.5, 101.5],
            "Volume": [1_000_000, 1_000_000],
            "VWAP": [100.4, 101.4],
            "source": ["polygon", "polygon"],
            "feat_x": [0.1, 0.2],
        },
        index=idx,
    )

    out = to_arctic_canonical(df, features=["feat_x"])

    assert out is df, (
        "Already-canonical frames must pass through without a copy "
        "(no reorder, no Categorical strip needed)."
    )
