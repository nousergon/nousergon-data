"""
store/arctic_store.py — ArcticDB connection manager.

Thin wrapper over ArcticDB that provides library access for all modules.
Uses S3 backend — no additional infrastructure beyond the existing bucket.

Usage:
    from store.arctic_store import get_universe_lib, get_macro_lib

    universe = get_universe_lib()
    df = universe.read("AAPL").data

Libraries:
    universe          — per-ticker time series (OHLCV + 53 computed features)
    macro             — market-wide time series (VIX, yields, commodities, macro features)
    delisted_history  — survivorship-free retention store: full OHLCV history of
                        tickers pruned from ``universe`` on delisting, so a
                        point-in-time (as-of-membership) backtest universe can be
                        reconstructed. NEVER read by live-trading code paths — they
                        want only currently-tradable names and are unaffected by this
                        library's existence. See ``get_delisted_history_lib`` for the
                        record schema/contract (config#1943, Leg 3).
"""

from __future__ import annotations

import logging
import os
from typing import Sequence

import arcticdb as adb
import pandas as pd
from nousergon_lib.arcticdb import arctic_uri, open_macro_lib, open_universe_lib

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
ARCTIC_PREFIX = "arcticdb"

# Survivorship-free retention store (config#1943, Leg 3). Delisted tickers are
# MOVED here (full OHLCV + delisting metadata) rather than hard-deleted from
# ``universe``, so a point-in-time backtest universe can be reconstructed
# without survivorship bias (~1-4%/yr overstatement otherwise). Deliberately
# a SEPARATE library — not an in-``universe`` flag — so live-trading code
# paths (which want only tradable names) require no change and never see the
# retained-for-backtest-only rows. See ``get_delisted_history_lib`` for the
# per-symbol record schema/contract.
DELISTED_HISTORY_LIB = "delisted_history"

# Canonical universe-library schema. Persisted layout is
# ``OHLCV_COLS + [PROVENANCE_COL] + FEATURES`` — any write that lays
# columns down in a different order trips ArcticDB's
# ``StreamDescriptorMismatch`` on the next update_batch (observed
# 2026-05-14 EOD, 2026-05-21 EOD). These constants are the single
# source of truth; ``builders/daily_append.py`` re-exports them for
# backwards-compat with operator scripts that already import from
# there.
OHLCV_COLS: list[str] = ["Open", "High", "Low", "Close", "Volume", "VWAP"]
PROVENANCE_COL: str = "source"

# CRSP total-return basis column (corporate-actions PR7, config#1434).
# ``Close`` stays the split-adjusted price LEVEL (polygon-authoritative);
# ``total_return_close`` is that same series further dividend-back-adjusted
# (the SEPARATE total-return axis built by
# ``corporate_actions.total_return_series``). It is persisted immediately
# AFTER ``Close`` in the canonical layout so the price-level and
# return-basis columns sit adjacent. ADDITIVE: it is absent on every live
# ``universe`` symbol today (the ``to_arctic_canonical`` guard makes it a
# no-op there) and is laid down only on the OFFLINE scratch CRSP-basis
# build (``builders/migrate_universe_crsp_basis.py``). The live basis flip
# (features + label recomputed on this column) is gated to PR7-7c after the
# shadow-retrain/backtest gate — NOT this PR.
TOTAL_RETURN_COL: str = "total_return_close"

# Live ArcticDB library names a scratch migration must NEVER target. The
# offline CRSP-basis build writes a distinct scratch library
# (e.g. ``universe_crsp``); ``get_scratch_universe_lib`` refuses these
# names structurally so a misconfigured ``--scratch-lib`` can't clobber
# the live universe / macro libraries the live pipeline reads.
_LIVE_LIB_NAMES: frozenset[str] = frozenset({"universe", "macro", "delisted_history"})

_arctic_instance: adb.Arctic | None = None


def _get_arctic(bucket: str | None = None) -> adb.Arctic:
    """Get or create the ArcticDB connection singleton."""
    global _arctic_instance
    if _arctic_instance is not None:
        return _arctic_instance

    bucket = bucket or os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)
    region = os.environ.get("AWS_REGION", "us-east-1")
    # Single source of truth for the S3 URI — nousergon_lib.arcticdb.arctic_uri
    # (path_prefix=arcticdb, aws_auth=true). Hand-rolling this f-string here was
    # the SNDK-incident bug class (path_prefix collapsing under shell quoting,
    # 2026-04-21); route it through the lib so it stays consistent everywhere.
    uri = arctic_uri(bucket, region=region)

    log.info("Connecting to ArcticDB: s3://%s/%s (region=%s)", bucket, ARCTIC_PREFIX, region)
    _arctic_instance = adb.Arctic(uri)
    return _arctic_instance


def get_universe_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the universe library (per-ticker OHLCV + features).

    Delegates to ``nousergon_lib.arcticdb.open_universe_lib`` — the shared
    library-open chokepoint (uniform URI + uniform RuntimeError-with-bucket
    error shape, config#804). This is a PRODUCER site, so ``create_if_missing``
    stays ``True`` to preserve cold-start bootstrap on a fresh bucket.
    """
    bucket = bucket or os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)
    return open_universe_lib(bucket, create_if_missing=True)


def get_macro_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the macro library (market-wide time series).

    Delegates to ``nousergon_lib.arcticdb.open_macro_lib`` (shared open
    chokepoint, config#804); ``create_if_missing=True`` preserves the
    producer cold-start bootstrap.
    """
    bucket = bucket or os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)
    return open_macro_lib(bucket, create_if_missing=True)


def get_scratch_universe_lib(name: str, bucket: str | None = None) -> adb.library.Library:
    """Get a SCRATCH universe-shaped library for an offline migration build.

    Used by ``builders/migrate_universe_crsp_basis.py`` (corporate-actions
    PR7-7a, config#1434) to write the reconstructed CRSP-basis universe into
    an isolated library (default ``universe_crsp``) WITHOUT ever touching the
    live ``universe`` library the daily/weekly pipelines read.

    Structurally refuses the live library names (``universe`` / ``macro``):
    a scratch build must use a distinct name, so a misconfigured
    ``--scratch-lib`` can never clobber live data. This is the single
    chokepoint enforcing the "offline, live-untouched" contract — every
    scratch write goes through here.
    """
    if name in _LIVE_LIB_NAMES:
        raise ValueError(
            f"refusing to open {name!r} as a scratch library — it is a LIVE "
            f"ArcticDB library the live pipeline reads. A scratch migration "
            f"must use a distinct name (e.g. 'universe_crsp'). Live names: "
            f"{sorted(_LIVE_LIB_NAMES)}"
        )
    arctic = _get_arctic(bucket)
    return arctic.get_library(name, create_if_missing=True)


def get_delisted_history_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the ``delisted_history`` library — the survivorship-free retention
    store for tickers pruned from ``universe`` on delisting (config#1943, Leg 3).

    Design decision (separate library, NOT an in-``universe`` flag):
        Delisted names are physically segregated into their own ArcticDB
        library so that every live-trading consumer of the ``universe`` lib
        (executor, daily_append, features/compute, the morning pipeline)
        keeps seeing ONLY currently-tradable names with zero code change —
        no consumer has to learn a "delisted" flag or filter it out. The
        retained-for-backtest-only data lives out-of-band and is read solely
        by the (follow-on) backtester universe-construction path.

    Record schema / S3 contract (one ArcticDB symbol per delisted ticker,
    keyed by the ticker string, e.g. ``"HOLX"``):

        data (pd.DataFrame)
            The ticker's FULL stored OHLCV history AS READ from the universe
            library at prune time — re-keyed verbatim, NOT re-projected (same
            identity-preserving discipline as ``corporate_actions.migrate_
            symbol``). Whatever columns the universe symbol carried (canonical
            ``OHLCV_COLS [+ total_return_close] + source + FEATURES``) are
            preserved, so a reconstructed backtest sees the same frame the live
            universe held on the ticker's last active day.

        metadata (dict) — the as-of-membership provenance:
            ``schema_version``      int, currently 1 (forward-compat guard).
            ``symbol``              str, the ticker (redundant with the key,
                                    carried in-band for standalone records).
            ``delisted_detected_on`` str ``YYYY-MM-DD`` — the prune run's
                                    ``today`` (the date this pruner CONFIRMED the
                                    delisting; NOT necessarily the true ex-date).
            ``last_active_date``    str ``YYYY-MM-DD`` — last index date present
                                    in ``data`` (the ArcticDB ``last_date`` the
                                    staleness gate keyed on). Together with the
                                    first index date this is the last-known
                                    membership window.
            ``first_active_date``   str ``YYYY-MM-DD`` — first index date in
                                    ``data`` (start of the retained window).
            ``rows``                int, len(data) — cheap integrity check.
            ``constituents_date``   str — the weekly constituents partition the
                                    absence was judged against (provenance for
                                    "which membership snapshot dropped it").
            ``retained_at``         str ISO-8601 UTC timestamp of the write.
            ``source``              str, ``"prune_delisted_tickers"`` — the
                                    producer that retained the record.

    Idempotency: retention writes overwrite the same symbol key with
    ``prune_previous_versions=True``, so re-running the pruner over an
    already-retained (but not-yet-deleted-from-universe) ticker refreshes the
    record in place rather than duplicating or corrupting it.

    ``create_if_missing=True``: this is the sole PRODUCER site and must
    bootstrap the library on a fresh bucket (cold start), mirroring
    ``get_universe_lib`` / ``get_macro_lib``. Routed through the shared
    ``_get_arctic`` connection singleton + canonical ``arctic_uri`` so the
    S3 endpoint/path_prefix conventions match every other library exactly.
    """
    arctic = _get_arctic(bucket)
    return arctic.get_library(DELISTED_HISTORY_LIB, create_if_missing=True)


def reset_connection():
    """Reset the singleton (useful for testing or credential rotation)."""
    global _arctic_instance
    _arctic_instance = None


def to_arctic_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a DataFrame to dtypes ArcticDB accepts on write/update.

    ArcticDB's ``_handle_categorical_columns`` raises
    ``ArcticDbNotYetImplemented`` on any ``CategoricalDtype`` column during
    ``update_batch`` / ``write_batch`` (verified 2026-05-12 EOD incident on
    BRK-B). Callers may keep Categorical in-memory for memory savings
    (PR #211: ~108MB reduction across the universe pass in
    ``_apply_daily_delta``) — this helper is the *single* boundary between
    that in-memory representation and the ArcticDB storage contract.

    Call exactly once, immediately before every ``update_batch`` /
    ``write_batch`` / ``write`` invocation. Empty frames and frames with no
    categorical columns return unchanged (no copy); frames with categoricals
    are copied + cast to object dtype (matches PR #196's pre-#211 storage
    representation, which round-trips cleanly through ArcticDB).
    """
    if df.empty:
        return df
    cat_cols = [c for c in df.columns if isinstance(df[c].dtype, pd.CategoricalDtype)]
    if not cat_cols:
        return df
    df = df.copy()
    for col in cat_cols:
        df[col] = df[col].astype(object)
    return df


def to_arctic_canonical(
    df: pd.DataFrame,
    *,
    features: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Project a universe-shaped DataFrame to canonical
    ``OHLCV_COLS + [PROVENANCE_COL] + FEATURES`` order, then strip
    Categorical dtypes.

    This is the chokepoint enforcing the universe library's column-order
    contract — every WritePayload / UpdatePayload / ``lib.write`` call
    site that writes universe symbols must go through this helper, so
    no new caller can lay down columns in a non-canonical order and
    silently corrupt the persisted descriptor.

    Why a centralized chokepoint:
        Per-site discipline failed twice in one week
        (2026-05-14 UPDATE-path, 2026-05-21 WRITE-path) when FEATURES
        widened mid-list and one of three call sites missed the
        accompanying reorder. Both incidents required emergency operator
        recovery — instance start + SSM migration + EOD redrive.
        Centralizing here removes the per-call-site discipline so
        future FEATURES widenings become safe additive changes with
        no companion column-ordering audit.

    Behaviour:
        - Intersect-then-reorder. Columns outside
          ``OHLCV_COLS + [TOTAL_RETURN_COL] + [PROVENANCE_COL] + features``
          are DROPPED (matches the prior per-site recipe).
        - ``TOTAL_RETURN_COL`` (``total_return_close``), when present, is
          placed immediately AFTER ``Close`` (CRSP basis, PR7 config#1434).
          Absent on live universe symbols → the layout is byte-identical to
          the pre-PR7 ``OHLCV + source + features`` order there (additive).
        - Empty frames pass through unchanged (no copy).
        - Frames already in canonical order and free of categoricals
          pass through unchanged (no copy).
        - ``features`` defaults to ``features.feature_engineer.FEATURES``
          (lazy-imported to keep ``store`` free of a top-level
          ``features`` dependency). Pass an explicit ``features``
          when the caller has its own contract (e.g. tests).

    Call exactly once, immediately before every universe
    ``update_batch`` / ``write_batch`` / ``write`` invocation. Macro
    writes have a different schema and continue to use
    ``to_arctic_safe`` directly.
    """
    if df.empty:
        return df

    if features is None:
        # Lazy import — keeps the dependency direction
        # ``builders → features`` + ``builders → store`` without
        # forcing ``store → features`` at module load.
        from features.feature_engineer import FEATURES as _FEATURES
        features = _FEATURES

    # Build the OHLCV head, splicing total_return_close in right after Close
    # (CRSP basis, PR7 config#1434). The guard keeps this a no-op for live
    # universe symbols (which carry no total_return_close column).
    head: list[str] = []
    for c in OHLCV_COLS:
        if c in df.columns:
            head.append(c)
        if c == "Close" and TOTAL_RETURN_COL in df.columns:
            head.append(TOTAL_RETURN_COL)
    # Degenerate guard: total_return_close present but no Close (shouldn't
    # happen in practice) — still keep it rather than silently dropping it.
    if TOTAL_RETURN_COL in df.columns and TOTAL_RETURN_COL not in head:
        head.append(TOTAL_RETURN_COL)

    canonical: list[str] = (
        head
        + ([PROVENANCE_COL] if PROVENANCE_COL in df.columns else [])
        + [f for f in features if f in df.columns]
    )

    if list(df.columns) != canonical:
        df = df[canonical]

    return to_arctic_safe(df)
