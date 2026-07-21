"""
builders/daily_append.py — Append today's OHLCV + features to ArcticDB universe.

Reads today's daily_closes from S3 (already written by daily_closes.py),
loads recent history from ArcticDB for feature warmup, computes today's
features, and appends a single row per ticker to the universe library.

Usage:
    python -m builders.daily_append                          # today
    python -m builders.daily_append --date 2026-04-07        # specific date
    python -m builders.daily_append --dry-run                # compute but skip write
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import io
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd

from botocore.exceptions import ClientError

from features.feature_engineer import (
    FEATURES,
    MIN_ROWS_FOR_FEATURES,
    compute_features,
)
from features.factor_momentum import update_factor_momentum_latest
from features.compute import (
    DEFAULT_BUCKET,
    _SKIP_TICKERS,
    _UNIVERSE_EXTRA,
    _is_sector_etf,
    _load_sector_map,
    _load_sub_sector_etf_map,
    _load_cached_fundamentals,
    _load_cached_alternative,
)
from arcticdb.version_store.library import ReadRequest, UpdatePayload, WritePayload
from arcticdb_ext.version_store import DataError
# Schema-drift exception family — RAISED (not returned-as-DataError) when a
# row's column set / dtypes / order no longer match the persisted ArcticDB
# stream descriptor. ``StreamDescriptorMismatch`` is the canonical case
# (2026-05-21 EOD incident); ``SchemaException`` / ``NormalizationException``
# are its siblings under ``ArcticException`` for the same "row shape ≠ stored
# descriptor" class. These propagate out of update()/write()/update_batch()/
# write_batch() rather than landing in a per-symbol DataError, so before
# config#1150 they aborted the run UNCOUNTED. We count-then-re-raise (fail-loud).
from arcticdb.exceptions import (
    NormalizationException,
    SchemaException,
    StreamDescriptorMismatch,
)

_SCHEMA_DRIFT_EXC = (StreamDescriptorMismatch, SchemaException, NormalizationException)

from store.arctic_store import (
    OHLCV_COLS as _CANONICAL_OHLCV_COLS,
    PROVENANCE_COL as _CANONICAL_PROVENANCE_COL,
    get_universe_lib,
    get_macro_lib,
    to_arctic_canonical,
    to_arctic_safe,
)
from store.parquet_loader import load_parquet_from_s3
from builders._price_cache_writeboth import (
    PRICE_CACHE_LEGACY_PREFIX,
    price_cache_read_prefixes,
)
from validators.price_validator import (
    ALL_ANOMALY_TYPES,
    ANOMALY_INTRABAR_INCONSISTENT,
    DEFAULT_BLOCK_ANOMALY_TYPES,
    validate_today_row,
)
# L2 per-series data-contract gates (alpha-engine-config#2456): calendar-
# aware continuity, vol-scaled outlier, and calendar-monotonic checks that
# validate_today_row/validate_parquet above do not cover (see
# series_contract's module docstring for the full delta). Supplements,
# does not replace, the existing block/warn plumbing above — schema/sanity
# are also run for parity with a repo that has no price_validator, but
# ``price_validator``'s OHLC/intrabar/volume checks remain this repo's
# authoritative source for the checks it already owns.
from nousergon_lib.series_contract import (
    GATE_NAMES as _L2_GATE_NAMES,
    DEFAULT_BLOCK_GATES as _L2_DEFAULT_BLOCK_GATES,
    quarantine_decision as _l2_quarantine_decision,
    validate_series as _l2_validate_series,
)

log = logging.getLogger(__name__)

# OHLCV_COLS + PROVENANCE_COL are the canonical universe-library schema —
# re-exported from store.arctic_store so the chokepoint
# (``to_arctic_canonical``) and these per-site usages share a single
# source of truth. Pre-2026-05-22 both were defined here AND in
# ``store/arctic_store.py``; consolidating them removes the per-site
# discipline that the 2026-05-14 + 2026-05-21 column-order incidents
# both relied on. Existing operator scripts that
# ``from builders.daily_append import OHLCV_COLS, PROVENANCE_COL``
# continue to work via these re-exports.
OHLCV_COLS = _CANONICAL_OHLCV_COLS
PROVENANCE_COL = _CANONICAL_PROVENANCE_COL
# Legacy single-prefix constant retained for backward-compat with any caller
# that imports it (audited 2026-05-19: only the local ``_load_parquet_warmup``,
# which now goes through ``price_cache_read_prefixes``). Wave-3 PR4 cutover
# deletes this constant in the same edit that flips the read helper.
PRICE_CACHE_PREFIX = PRICE_CACHE_LEGACY_PREFIX

# Process the universe in chunks of this size through Phase 1+2 (read,
# compute, write). The full-universe pass holds ~900 ticker histories in
# memory simultaneously (~180MB peak resident) — on the 2GB t3.small
# trading instance that co-existed with the 1GB daily_append working set,
# IB Gateway, and a (now-fixed) crash-looping daemon, peak memory blew
# past available + swap and OOM-killed the process partway through the
# universe loop (2026-05-11 incident). Chunking caps per-iteration
# resident memory at ~30MB / 150 tickers and gc.collect() between
# chunks forces release of the cycled DataFrames whose BlockManager
# reference cycles would otherwise defer freeing.
#
# 150 is a balance: smaller → more gc overhead + more read_batch RTTs;
# larger → tighter on memory headroom. 900/150 = 6 chunks per run.
UNIVERSE_CHUNK_SIZE = 150


def _align_schema_for_update(
    new_row: pd.DataFrame, existing_series: pd.DataFrame
) -> pd.DataFrame:
    """Bridge schema differences between existing ArcticDB series and a new row.

    ArcticDB's ``update()`` requires column-set match between existing series
    and the row being inserted. When schemas drift across migrations (e.g.
    the 2026-05-09 provenance ``source`` column added to write paths but
    not yet present in pre-migration rows), un-aligned schemas cause
    update() to fail or silently coerce.

    This helper:
      * Drops columns from ``new_row`` that aren't in ``existing_series``
        (compatibility mode — daily_append-side writers can carry richer
        schemas than the still-old existing series; the extra cols get
        added by the next full backfill write).
      * Adds NaN columns to ``new_row`` for any columns ``existing_series``
        has but ``new_row`` doesn't (preserves the existing schema's
        contract; the next backfill rewrites with proper values).
      * Reorders to match ``existing_series.columns`` so update() doesn't
        complain about positional mismatch.

    Idempotent — calling it on already-aligned schemas is a no-op.
    Used inside ``_write_row_backfill_safe`` so the migration boundary is
    handled in one place.
    """
    if existing_series is None or existing_series.empty:
        return new_row
    existing_cols = list(existing_series.columns)
    if list(new_row.columns) == existing_cols:
        # Schemas already match — no-op short-circuit so the original
        # DataFrame reference passes through (preserves mock-call
        # identity for tests + avoids needless reordering work).
        return new_row
    new_cols = set(new_row.columns)
    extra = new_cols - set(existing_cols)
    if extra:
        new_row = new_row.drop(columns=list(extra))
    missing = [c for c in existing_cols if c not in new_row.columns]
    if missing:
        for col in missing:
            new_row[col] = np.nan
    return new_row[existing_cols]


def _write_row_backfill_safe(
    lib,
    symbol: str,
    new_row: pd.DataFrame,
    existing_series: pd.DataFrame | None = None,
) -> str:
    """Write a single-date row to ArcticDB, handling both append and backfill cases.

    Returns the mode used: ``"append"`` (target_date > all existing dates,
    used update() — fast) or ``"backfill"`` (target_date is in the middle
    of an existing series, used read+splice+write() — necessary because
    update() requires monotonic insertion at the head).

    The backfill path is ~10-100x slower per ticker (full series read +
    full rewrite vs. single-row update) but fires only for rare backfill
    operations like the 2026-04-24 historical VWAP repair after the
    polygon outage.

    Schema-bridges via ``_align_schema_for_update`` so writers with newer
    columns (e.g. provenance ``source`` added 2026-05-09) don't trip
    ArcticDB's strict-column-match contract on existing pre-migration
    series. Once a full-series backfill writes the richer schema, all
    subsequent updates carry the new columns.
    """
    target_ts = new_row.index[0]

    # If caller already has the existing series (the per-ticker loop in
    # daily_append already reads `hist` for feature warmup), reuse it
    # instead of double-reading.
    if existing_series is None:
        try:
            existing_series = lib.read(symbol).data
        except Exception:
            # Symbol doesn't exist yet — first write is always an append.
            lib.write(symbol, new_row, prune_previous_versions=True)
            return "append"

    if existing_series.empty or target_ts > existing_series.index.max():
        # Append at head — fast path. update() is idempotent for same-date
        # rows (replaces in place rather than appending duplicates).
        new_row = _align_schema_for_update(new_row, existing_series)
        lib.update(symbol, new_row)
        return "append"

    # Backfill — splice new_row into existing series, write back full
    # series. Required because ArcticDB's update() refuses non-monotonic
    # insertion ("index must be monotonic increasing or decreasing").
    # Same-date rows are deduped with keep="last" so the new row wins
    # over any existing row at target_ts (matches update() semantics).
    new_row = _align_schema_for_update(new_row, existing_series)
    combined = pd.concat([existing_series, new_row])
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    lib.write(symbol, combined, prune_previous_versions=True)
    return "backfill"


def _emit_missing_from_closes_metric(count: int) -> None:
    """Emit ``AlphaEngine/Data/missing_from_closes_count`` gauge.

    Best-effort: CloudWatch errors WARN but don't fail the pipeline — the
    hard-fail above the threshold is the load-bearing path, the metric +
    alarm catches slow drift below the threshold (1-2 silently-missing
    tickers per day adds up to a regression like the 2026-04-25 incident
    if uncaught). Pattern mirrors ``_emit_admission_refused_metric`` in
    alpha-engine/executor/signal_reader.py.
    """
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "missing_from_closes_count",
                "Value": float(count),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        log.warning(
            "CloudWatch missing_from_closes_count metric failed: %s. "
            "Not blocking daily_append — the threshold check above already "
            "surfaced the count.",
            exc,
        )


class UniverseFreshnessViolation(RuntimeError):
    """Raised by :func:`_scan_universe_and_emit_freshness_receipt` when one or
    more UNRELATED universe symbols are stale — i.e. the just-completed
    target-date write itself succeeded, but the whole-universe scan that
    runs after it found a separate symbol exceeding the staleness threshold.

    Subclasses ``RuntimeError`` so every existing caller that hard-fails
    on (or asserts) a bare ``RuntimeError`` — the weekday/EOD SF paths,
    the existing test suite — keeps its exact current behavior unchanged.
    ``.stale_symbols`` lets a caller that WANTS to distinguish "my target
    date's write succeeded, an unrelated ticker is stale" from a genuine
    write failure do so (config#2685) without weakening the hard-fail
    safety net itself (config-I2703 / the 2026-04-21 ASGN/MOH incident).
    """

    def __init__(self, message: str, stale_symbols: list[dict]):
        super().__init__(message)
        self.stale_symbols = stale_symbols


UNIVERSE_FRESHNESS_RECEIPT_KEY = "health/universe_freshness.json"
# Trading-day-aware staleness threshold. 3 trading days ≈ the prior
# 5-calendar-day threshold (which was Fri→Wed under weekend buffer);
# trading-day arithmetic handles weekends + holidays natively via
# nousergon_lib.dates.trading_days_stale.
UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS = 3
_UNIVERSE_SCAN_WORKERS = 20

# ArcticDB freshness-monitor sentinel (config#1787, Brian's 2026-07-08
# Option-B ruling). A small, UNCONDITIONAL S3 marker written on every
# successful daily_append ArcticDB write — deliberately separate from
# ``health/universe_freshness.json`` above, which is a richer per-symbol
# staleness receipt that only gets written when the whole scan passes
# (and hard-raises otherwise). This sentinel exists purely so
# ``nousergon_lib.artifact_freshness``'s ordinary S3 ArtifactSpec probe
# (HEAD/LIST + recency, zero new backend code) has something to check for
# the ArcticDB feature-store producer, per Brian's explicit "ordinary S3
# ArtifactSpec, zero changes to nousergon_lib.artifact_freshness" ruling —
# rejecting Option (A) (a first-class arcticdb backend) as premature
# generalization for one consumer.
FEATURE_STORE_FRESHNESS_SENTINEL_KEY = "feature_store/_freshness.json"


def _write_feature_store_freshness_sentinel(
    s3, bucket: str, *, library: str = "universe", symbol_or_library: str | None = None,
) -> dict:
    """Write the ArcticDB feature-store freshness sentinel (config#1787).

    Best-effort: any S3 write failure is logged at WARN and swallowed — the
    sentinel is an observability nicety for the freshness monitor, not a
    load-bearing part of the daily_append pipeline, so a sentinel-write
    failure must never fail (or even affect) the pipeline run that already
    completed its real ArcticDB writes.
    """
    sentinel = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol_or_library": symbol_or_library or library,
        "library": library,
        "writer": "alpha-engine-data:builders/daily_append.py",
    }
    try:
        s3.put_object(
            Bucket=bucket,
            Key=FEATURE_STORE_FRESHNESS_SENTINEL_KEY,
            Body=json.dumps(sentinel, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        log.info(
            "Wrote ArcticDB feature-store freshness sentinel to s3://%s/%s",
            bucket, FEATURE_STORE_FRESHNESS_SENTINEL_KEY,
        )
    except Exception as exc:
        log.warning(
            "ArcticDB feature-store freshness sentinel write FAILED "
            "(non-fatal, OBSERVE): %s", exc,
        )
    return sentinel


# Verify-by-artifact precondition sentinel for the EOD self-heal loop
# (alpha-engine-config-I2702 deliverable #1/#2). Deliberately a SEPARATE S3
# key from FEATURE_STORE_FRESHNESS_SENTINEL_KEY above, not a second `library`
# value written to the SAME key: the universe sentinel (written once per
# ticker-loop pass, deep in the per-symbol loop) and this macro sentinel
# (written once per run, right after the macro/SPY readback-verification
# block below) fire on different code paths and different cadences — sharing
# one S3 key would let whichever write lands LAST silently overwrite/mask the
# other's information, defeating the whole point of a per-artifact freshness
# signal. This key is read by infrastructure/lambdas/eod-precondition-probe,
# which is the ONLY thing that ever reads it — it is not a general freshness
# API, and unlike the universe sentinel (`timestamp`-only recency check) it
# carries `run_date` explicitly: the EOD probe needs to confirm THIS
# SPECIFIC trading day's SPY close was verified present, not merely "some
# macro write happened recently" (a stale sentinel from an old run, or a
# backfill of an unrelated older date, would otherwise false-positive).
MACRO_FRESHNESS_SENTINEL_KEY = "feature_store/_macro_freshness.json"


def _write_macro_freshness_sentinel(
    s3, bucket: str, *, run_date: str, verified_keys: list[str],
) -> dict:
    """Write the ArcticDB macro/SPY freshness sentinel (config-I2702).

    Called ONLY after the macro readback-verification block below confirms
    (via `verification_failures`) that every key in ``verified_keys`` is
    genuinely queryable in ArcticDB for ``run_date`` — this sentinel is the
    ARTIFACT the EOD precondition probe checks, so it must never be written
    optimistically ahead of that verification.

    Best-effort, matching `_write_feature_store_freshness_sentinel`'s
    posture: a sentinel-write failure is an observability gap for the probe
    (which then correctly reports precondition_met=False and the self-heal
    loop retries — never a silent false-green), NOT a reason to fail a
    daily_append run whose real ArcticDB writes already succeeded and were
    already verified above. Swallow rationale (feedback_no_silent_fails):
    (a) failure mode swallowed = S3 PutObject error on the freshness
    sentinel only, never the ArcticDB write/verification itself; (c) logged
    at WARN here, and the resulting probe-reported precondition_met=False
    is itself the recording surface (drives the self-heal loop / pages on
    non-convergence) rather than a silent no-op.
    """
    sentinel = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_date": run_date,
        "verified_keys": sorted(verified_keys),
        "writer": "alpha-engine-data:builders/daily_append.py",
    }
    try:
        s3.put_object(
            Bucket=bucket,
            Key=MACRO_FRESHNESS_SENTINEL_KEY,
            Body=json.dumps(sentinel, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        log.info(
            "Wrote ArcticDB macro-freshness sentinel to s3://%s/%s (run_date=%s, keys=%s)",
            bucket, MACRO_FRESHNESS_SENTINEL_KEY, run_date, sentinel["verified_keys"],
        )
    except Exception as exc:
        log.warning(
            "ArcticDB macro-freshness sentinel write FAILED "
            "(non-fatal, OBSERVE — the EOD precondition probe will correctly "
            "report precondition_met=False and the self-heal loop will retry): %s",
            exc,
        )
    return sentinel


def _load_block_anomaly_types() -> frozenset[str]:
    """Read ``DAILY_APPEND_BLOCK_ANOMALY_TYPES`` env var or fall back to defaults.

    Format: JSON list of anomaly type strings, e.g. ``'["bad_ohlc",
    "negative_or_zero_close", "extreme_daily_move"]'``. Unknown types
    raise — silent typo would let bad rows through. Empty/unset uses the
    conservative default set (only definitely-bad rows block).
    """
    raw = os.environ.get("DAILY_APPEND_BLOCK_ANOMALY_TYPES", "").strip()
    if not raw:
        return DEFAULT_BLOCK_ANOMALY_TYPES
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"DAILY_APPEND_BLOCK_ANOMALY_TYPES is not valid JSON: {exc}. "
            f"Expected a JSON list of anomaly type strings."
        ) from exc
    if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
        raise RuntimeError(
            f"DAILY_APPEND_BLOCK_ANOMALY_TYPES must be a JSON list of strings, "
            f"got {parsed!r}"
        )
    unknown = set(parsed) - ALL_ANOMALY_TYPES
    if unknown:
        raise RuntimeError(
            f"DAILY_APPEND_BLOCK_ANOMALY_TYPES contains unknown anomaly types: "
            f"{sorted(unknown)}. Known types: {sorted(ALL_ANOMALY_TYPES)}"
        )
    return frozenset(parsed)


def _load_l2_block_gates() -> frozenset[str]:
    """Read ``DAILY_APPEND_L2_BLOCK_GATES`` env var or fall back to
    ``nousergon_lib.series_contract.DEFAULT_BLOCK_GATES``.

    Mirrors :func:`_load_block_anomaly_types`'s override contract for the
    new L2 series-contract gates (schema / sanity / staleness / continuity
    / outlier / calendar_monotonic). Default block set is schema / sanity /
    calendar_monotonic (unambiguous corruption); staleness / continuity /
    outlier default to alarm-and-allow since they can legitimately arise
    from an operational gap or a real market event, not just corruption.
    Format: JSON list of gate name strings. Unknown names raise.
    """
    raw = os.environ.get("DAILY_APPEND_L2_BLOCK_GATES", "").strip()
    if not raw:
        return _L2_DEFAULT_BLOCK_GATES
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"DAILY_APPEND_L2_BLOCK_GATES is not valid JSON: {exc}. "
            f"Expected a JSON list of gate name strings."
        ) from exc
    if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
        raise RuntimeError(
            f"DAILY_APPEND_L2_BLOCK_GATES must be a JSON list of strings, "
            f"got {parsed!r}"
        )
    unknown = set(parsed) - set(_L2_GATE_NAMES)
    if unknown:
        raise RuntimeError(
            f"DAILY_APPEND_L2_BLOCK_GATES contains unknown gate names: "
            f"{sorted(unknown)}. Known gates: {sorted(_L2_GATE_NAMES)}"
        )
    return frozenset(parsed)


def _emit_quality_gate_metrics(
    counts_by_type: dict[str, int], n_blocked: int, n_warned: int
) -> None:
    """Emit ``AlphaEngine/Data/daily_append_quality_*`` gauges.

    Best-effort: CloudWatch errors WARN but don't fail the pipeline — the
    aggregated run-level quality-gate record (one per run, after the chunk
    loop) is the load-bearing Flow Doctor surface; the metric catches slow
    drift so a chronic 1-2-ticker-per-day anomaly stream surfaces before
    it cumulates into a regression. Mirrors ``_emit_missing_from_closes_metric``.
    """
    if not counts_by_type and n_blocked == 0 and n_warned == 0:
        return
    try:
        cw = boto3.client("cloudwatch")
        metric_data: list[dict] = [
            {
                "MetricName": "daily_append_quality_blocked_count",
                "Value": float(n_blocked),
                "Unit": "Count",
            },
            {
                "MetricName": "daily_append_quality_warned_count",
                "Value": float(n_warned),
                "Unit": "Count",
            },
        ]
        for atype, count in counts_by_type.items():
            metric_data.append({
                "MetricName": "daily_append_quality_anomaly_count",
                "Dimensions": [{"Name": "anomaly_type", "Value": atype}],
                "Value": float(count),
                "Unit": "Count",
            })
        cw.put_metric_data(Namespace="AlphaEngine/Data", MetricData=metric_data)
    except Exception as exc:
        log.warning(
            "CloudWatch daily_append_quality_* metric failed: %s. "
            "Not blocking daily_append — the aggregated run-level "
            "quality-gate record is the load-bearing Flow Doctor surface.",
            exc,
        )


def _emit_schema_drift_metric(count: int) -> None:
    """Emit ``AlphaEngine/Data/daily_append_schema_drift_count`` gauge.

    ``count`` is the number of ArcticDB schema-drift write failures
    (``StreamDescriptorMismatch`` / ``SchemaException`` /
    ``NormalizationException``) seen on the universe + macro write paths this
    run. Emitted on EVERY run (zero on a clean run) so the evaluator's
    trailing-4w Sum has a continuous baseline — a missing datapoint then means
    "producer didn't run", not "zero incidents". Best-effort: a CloudWatch
    error WARNs but does NOT mask the schema-drift exception itself, which
    still propagates and fails the run loud. Mirrors ``_emit_quality_gate_metrics``.
    """
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "daily_append_schema_drift_count",
                "Value": float(count),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        log.warning(
            "CloudWatch daily_append_schema_drift_count metric failed: %s. "
            "Not blocking daily_append — the count is observability hung off "
            "the schema-drift failure path; the incident itself still raises.",
            exc,
        )


def _write_schema_drift_manifest(s3, bucket: str, date_str: str, count: int) -> None:
    """Persist the per-run schema-drift count to ``market_data/weekly/{date}/manifest.json``.

    The durable, pull-based artifact companion to the CloudWatch metric —
    written on EVERY run (including a clean run, count==0) so the artifact
    reflects the last run's true state. Best-effort: an S3 error WARNs but
    does NOT mask the schema-drift exception. ``schema_drift_incidents`` is
    merged into any existing manifest so a co-located key from another writer
    is preserved.
    """
    key = f"market_data/weekly/{date_str}/manifest.json"
    manifest: dict = {}
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        loaded = json.loads(existing)
        if isinstance(loaded, dict):
            manifest = loaded
    except Exception:
        # No prior manifest (common) — start fresh. A malformed/absent object
        # is not worth failing the run over; we overwrite with our keys.
        manifest = {}
    manifest["schema_drift_incidents"] = int(count)
    manifest["schema_drift_writer"] = "alpha-engine-data:builders/daily_append.py"
    manifest["schema_drift_written_at"] = datetime.now(timezone.utc).isoformat()
    manifest.setdefault("date", date_str)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(manifest, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:
        log.warning(
            "manifest.json schema-drift write failed (s3://%s/%s): %s. "
            "Not blocking daily_append — CloudWatch carries the same count and "
            "the incident itself still raises.",
            bucket, key, exc,
        )


@contextlib.contextmanager
def _count_schema_drift(counter: list[int], *, on_drift=None):
    """Count an ArcticDB schema-drift write failure, then RE-RAISE it (fail-loud).

    Wrap an ArcticDB write call (update / write / update_batch / write_batch).
    On a schema-drift exception (``_SCHEMA_DRIFT_EXC``) we increment
    ``counter[0]``, invoke ``on_drift(counter[0])`` (the durable CloudWatch +
    manifest emit, so the count survives the abort) if supplied, and re-raise
    the original exception unchanged — the count is observability hung off the
    existing failure path, NEVER a swallow. The run still fails loud. Non-schema
    exceptions pass straight through, uncounted.
    """
    try:
        yield
    except _SCHEMA_DRIFT_EXC as exc:
        counter[0] += 1
        log.error(
            "ArcticDB schema-drift write failure #%d this run: %s: %s — "
            "counting then re-raising (fail-loud; the row shape no longer "
            "matches the persisted stream descriptor).",
            counter[0], type(exc).__name__, exc,
        )
        if on_drift is not None:
            # Emit-on-abort so the incident lands in CloudWatch + manifest even
            # though the run is about to fail. Best-effort: the emit itself must
            # never mask the original schema-drift exception.
            try:
                on_drift(counter[0])
            except Exception as emit_exc:  # pragma: no cover - defensive
                log.warning(
                    "schema-drift emit-on-abort failed: %s (original incident "
                    "still re-raised below)", emit_exc,
                )
        raise


def _scan_universe_and_emit_freshness_receipt(
    s3,
    bucket: str,
    universe_lib,
    max_stale_trading_days: int = UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS,
    expected_tickers: list[str] | None = None,
) -> dict:
    """Producer-side post-write validation: every universe symbol's
    last-row date must be within ``max_stale_trading_days`` NYSE sessions
    of today. Trading-day-aware via ``nousergon_lib.dates`` so weekend
    runs don't false-fail on calendar-day weekend gaps.

    On all-fresh: writes ``s3://{bucket}/health/universe_freshness.json``
    so downstream consumers (predictor inference, executor, backtester)
    read a single O(1) artifact instead of re-running this 200s scan
    themselves on every Lambda invocation. The 2026-05-01 weekday SF
    timeout cascade traced back to PR #68 adding this same scan to the
    predictor inference preflight, multiplying the cost.

    On any stale: hard-raises ``RuntimeError`` so the SF MorningEnrich
    step fails. The 2026-04-21 ASGN/MOH incident class — partial-write
    where macro/SPY stays fresh while individual tickers stall — is
    exactly what this catches at the producer.

    When ``expected_tickers`` is provided, the scan is scoped to
    ``arctic_universe ∩ expected_tickers`` — same scoping the pre-write
    missing-from-closes check uses (see ``daily_append`` docstring for
    the full rationale). S&P churn-out stragglers (in arctic awaiting
    prune, no longer in current constituents) are excluded so they don't
    trip the post-write scan after the pre-write check correctly let
    them through. 2026-05-02 incident: the pre-write check correctly
    excluded 8 churn-outs, daily writes completed (n_ok=898), then this
    scan tripped on the same 8 stragglers (one 25d stale) and halted
    the SF.

    ``_UNIVERSE_EXTRA`` members (currently: SPY) are HARD-PINNED benchmark
    symbols, never churn-eligible — they are excluded from ``_SKIP_TICKERS``
    membership here via the same ``(... not in _SKIP_TICKERS or ... in
    _UNIVERSE_EXTRA)`` carve-out the write-path predicate
    (``_daily_append_admits`` in tests/test_spy_universe_member.py; see
    ``stock_tickers`` below) already uses. 2026-07-15 P0 (config-I2703):
    this scan (and the missing-from-closes check below) had DRIFTED from
    that write-path predicate — they filtered on bare ``_SKIP_TICKERS``
    with no ``_UNIVERSE_EXTRA`` carve-out, so SPY (in ``_SKIP_TICKERS`` by
    design, see features/compute.py) was silently excluded from BOTH
    freshness accounting paths every run, logged as an "S&P churn-out
    straggler, awaiting prune" — even though SPY is a permanent member,
    never a prune candidate, and is the executor's benchmark hard-fail
    dependency (eod_reconcile._spy_close). A genuine SPY write failure
    would have gone undetected by this gate. Fixed by aligning all three
    ``expected_tickers``-scoping call sites (write / missing-from-closes /
    freshness-scan) on the identical predicate.

    Returns scan metadata (also embedded in the receipt) for logging
    by the caller. Skipped automatically on dry_run.
    """
    all_syms = list(universe_lib.list_symbols())
    if not all_syms:
        raise RuntimeError(
            f"Universe-freshness scan: library is empty on bucket {bucket!r}; "
            "upstream pipeline has not written anything."
        )

    if expected_tickers is not None:
        expected_set = {
            t.lstrip("^") for t in expected_tickers
            if (t.lstrip("^") not in _SKIP_TICKERS or t.lstrip("^") in _UNIVERSE_EXTRA)
            and not _is_sector_etf(t.lstrip("^"))
        }
        syms = [s for s in all_syms if s in expected_set]
        excluded = [s for s in all_syms if s not in expected_set]
        if excluded:
            log.info(
                "Universe-freshness scan: excluding %d ArcticDB symbols absent "
                "from expected_tickers (S&P churn-out stragglers, awaiting "
                "prune): %s",
                len(excluded), sorted(excluded)[:20],
            )
        if not syms:
            raise RuntimeError(
                f"Universe-freshness scan: zero symbols after expected_tickers "
                f"intersection (arctic={len(all_syms)}, expected_set={len(expected_set)}). "
                "Either expected_tickers is empty or the constituents collector "
                "broke the schema."
            )
    else:
        syms = all_syms

    from nousergon_lib.dates import trading_days_stale
    today = datetime.now(timezone.utc).date()
    today_iso = today.isoformat()

    def _last_date_for(sym: str) -> tuple[str, "pd.Timestamp | None", "str | None"]:
        try:
            df = universe_lib.tail(sym, n=1).data
        except Exception as exc:  # pragma: no cover — surfaces below
            return sym, None, f"{type(exc).__name__}: {exc}"
        if df.empty:
            return sym, None, "empty frame"
        last_ts = pd.Timestamp(df.index[-1])
        if last_ts.tzinfo is not None:
            last_ts = last_ts.tz_convert("UTC").tz_localize(None)
        return sym, last_ts.normalize(), None

    t0 = time.time()
    rows: list[tuple[str, "pd.Timestamp | None", "str | None"]] = []
    with ThreadPoolExecutor(max_workers=_UNIVERSE_SCAN_WORKERS) as ex:
        rows = list(ex.map(_last_date_for, syms))
    scan_seconds = time.time() - t0

    read_errors = [(s, err) for s, _, err in rows if err is not None]
    if read_errors:
        head = ", ".join(f"{s}({e[:40]})" for s, e in read_errors[:5])
        raise RuntimeError(
            f"Universe-freshness scan: {len(read_errors)} symbol(s) failed to read "
            f"(threshold 0): {head}"
            + ("…" if len(read_errors) > 5 else "")
        )

    ages = []  # (sym, last_date_iso, trading_days_stale)
    for sym, last_date, _ in rows:
        age_trading_days = trading_days_stale(last_date.date(), today_iso)
        ages.append((sym, last_date.date().isoformat(), age_trading_days))

    stale = [(s, d, a) for s, d, a in ages if a > max_stale_trading_days]
    stalest = max(ages, key=lambda r: r[2])

    if stale:
        stale.sort(key=lambda r: -r[2])
        head = ", ".join(f"{s}({a} trading-d, last={d})" for s, d, a in stale[:10])
        raise UniverseFreshnessViolation(
            f"Universe-freshness scan: {len(stale)} symbol(s) older than "
            f"{max_stale_trading_days} trading-day(s) threshold "
            f"(stalest first): {head}"
            + ("…" if len(stale) > 10 else ""),
            stale_symbols=[
                {"symbol": s, "last_date": d, "age_trading_days": a} for s, d, a in stale
            ],
        )

    receipt = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "library": "universe",
        "bucket": bucket,
        "n_symbols_checked": len(syms),
        "max_stale_trading_days_threshold": max_stale_trading_days,
        "all_fresh": True,
        "stalest_symbol": stalest[0],
        "stalest_last_date": stalest[1],
        "stalest_age_trading_days": stalest[2],
        "scan_seconds": round(scan_seconds, 1),
        "writer": "alpha-engine-data:builders/daily_append.py",
    }

    s3.put_object(
        Bucket=bucket,
        Key=UNIVERSE_FRESHNESS_RECEIPT_KEY,
        Body=json.dumps(receipt, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    log.info(
        "Universe-freshness receipt written: n=%d all_fresh stalest=%s(%d trading-d) scan=%.1fs",
        len(syms), stalest[0], stalest[2], scan_seconds,
    )
    return receipt


def _load_parquet_warmup(s3, bucket: str, ticker: str) -> pd.DataFrame | None:
    """Load a ticker's 10y price-cache parquet for feature warmup.

    Returns None when the parquet doesn't exist (new constituent that hasn't
    been picked up by the weekly backfill yet). Hard-fails on any other
    error shape — NoSilentFails.

    Wave-3 reader migration (ROADMAP L1401): iterates the
    ``price_cache_read_prefixes`` fallback chain — new prefix
    (``reference/price_cache/``) consulted first, legacy
    (``predictor/price_cache/``) is the soak-window safety net.
    "Not found" means absent in BOTH prefixes; non-404 errors hard-fail
    on the first prefix that raises (preserving NoSilentFails).
    """
    df: pd.DataFrame | None = None
    last_key: str | None = None
    not_found = 0
    for prefix in price_cache_read_prefixes(PRICE_CACHE_PREFIX):
        last_key = f"{prefix}{ticker}.parquet"
        try:
            df = load_parquet_from_s3(s3, bucket, last_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404"):
                not_found += 1
                continue
            raise RuntimeError(
                f"parquet-warmup read failed for {ticker} (bucket={bucket}, "
                f"key={last_key}): {exc}"
            ) from exc
        break

    if df is None:
        # Absent in every active prefix → genuine "not in price cache".
        return None

    if df.empty or "Close" not in df.columns:
        raise RuntimeError(
            f"parquet-warmup for {ticker}: parquet exists but invalid shape "
            f"(empty={df.empty}, cols={list(df.columns)[:6]}, key={last_key})"
        )
    return df


def _load_daily_closes(s3, bucket: str, date_str: str) -> dict[str, dict]:
    """Load today's daily_closes parquet from S3. Raises if the file is missing or unreadable.

    VWAP semantics (per the 2026-04-17 Phase 7 VWAP centralization decision,
    refined by the 2026-04-23 split-by-source PR):

      * Polygon grouped-daily (collected via ``daily_closes --source polygon_only``
        in the morning enrichment pass) → true volume-weighted VWAP from
        polygon's ``vw`` field.

      * yfinance EOD pass (``daily_closes --source yfinance_only``, ~1:05 PM PT)
        → VWAP=None. yfinance does not expose true VWAP and the (H+L+C)/3
        typical-price proxy was explicitly REJECTED in 2026-04-17 because it
        misrepresented arithmetic typical price as volume-weighted VWAP.
        Morning polygon enrichment overwrites the row to fill VWAP.

      * FRED fallback for indices (VIX/VIX3M/TNX/IRX) → VWAP=None. Single
        daily Close value with no trade distribution to weight.

    Missing VWAP becomes ``NaN`` in the output (not an error); downstream
    consumers (executor's ``load_daily_vwap``) handle NaN by walking back up
    to 5 trading days for a populated value.
    """
    key = f"staging/daily_closes/{date_str}.parquet"
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")

    records = {}
    for ticker, row in df.iterrows():
        vwap_raw = row.get("VWAP")
        # Provenance: per-row data source set by daily_closes.collect
        # ("polygon" / "yfinance" / "fred"). Surface it on the records dict
        # so the daily_append per-ticker loop can carry it through to the
        # ArcticDB universe write — closes the audit trail of "where did
        # this row's value come from" at row granularity.
        source_raw = row.get("source")
        records[str(ticker)] = {
            "Open": float(row.get("Open", np.nan)),
            "High": float(row.get("High", np.nan)),
            "Low": float(row.get("Low", np.nan)),
            "Close": float(row.get("Close", np.nan)),
            "Volume": int(row.get("Volume", 0)),
            "VWAP": float(vwap_raw) if pd.notna(vwap_raw) else np.nan,
            "source": str(source_raw) if pd.notna(source_raw) else "unknown",
        }
    if not records:
        raise RuntimeError(
            f"daily_closes/{date_str}.parquet loaded zero tickers — upstream daily_closes collection is broken"
        )
    log.info("Loaded daily closes for %s: %d tickers", date_str, len(records))
    return records


_LOCK_ENV_VAR = "ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED"


def _writer_lock_enabled(dry_run: bool) -> bool:
    """True if the producer-side writer lock should fire for this call.

    Default-OFF rollout: ``ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED`` must
    be explicitly set to a truthy value. ``dry_run`` always bypasses the
    lock because dry-run paths do not write to ArcticDB — locking them
    would block legitimate concurrent dry-runs (e.g. operator inspection
    while the Saturday SF is running).
    """
    if dry_run:
        return False
    return os.environ.get(_LOCK_ENV_VAR, "").lower() in ("1", "true", "yes")


def _build_writer_id() -> str:
    """Compose a process-identifying writer_id for the lock body.

    Pattern: ``daily_append-{user}-pid{pid}``. The user and pid let the
    operator recognize the holder when inspecting the lock object
    (``aws s3 cp s3://alpha-engine-research/locks/universe-writer.lock -``)
    or reading a ``LockHeldByAnotherWriterError`` from a failed
    acquisition.
    """
    user = os.environ.get("USER", "unknown")
    return f"daily_append-{user}-pid{os.getpid()}"


def _ensure_history_restated(
    ticker: str,
    hist: pd.DataFrame,
    actions: list,
    registry,
    universe_lib,
    date_str: str,
) -> pd.DataFrame:
    """Basis-consistency guard (PR4, config#1433): restate ``ticker``'s FULL
    ArcticDB history for any registered split NOT yet applied to the universe,
    BEFORE today's (post-split-scale) row is spliced onto it.

    daily_append appends today's polygon row onto an ArcticDB history that may
    still be pre-split until Saturday's backfill. The morning ``daily_closes``
    sync (earlier in the weekday SF) normally restates the universe first, so in
    steady state ``is_applied`` is already True here and this is a fast no-op.
    This is the ROBUST backstop for "sync missed/skipped / standalone run": we
    restate-then-append rather than ever appending onto an un-restated history.

    WRITE-THEN-MARK: the ``arcticdb_universe`` applied marker is the shared
    exactly-once contract with both ``sync`` and the Saturday backfill — it is
    written ONLY after the full-history rewrite lands, so a failed rewrite never
    leaves the marker (and a later read) believing the history is restated when
    it is not. Returns the (possibly restated) history to splice today's row on.
    """
    import corporate_actions as _ca

    pending = [
        a for a in actions
        if not registry.is_applied(_ca.STORE_ARCTICDB_UNIVERSE, a.action_id)
    ]
    if not pending:
        return hist

    run_id = f"daily_append:{date_str}"
    # registry=None ⇒ apply does the math + row counts WITHOUT marking, so the
    # mark below is strictly write-then-mark.
    restated, applied = _ca.apply(
        hist, pending, store=_ca.STORE_ARCTICDB_UNIVERSE, registry=None, run_id=run_id,
    )
    n_changed = sum(1 for r in applied if r["n_rows_adjusted"] > 0)
    if n_changed:
        log.warning(
            "daily_append basis-consistency: %s ArcticDB history was NOT yet "
            "restated for %d registered split(s) (morning sync missed/skipped) "
            "— restating full history BEFORE append so today's row lands on a "
            "continuous adjusted scale, not a split-boundary splice (data#1298)",
            ticker, n_changed,
        )
        universe_lib.write(
            ticker, to_arctic_canonical(restated), prune_previous_versions=True,
        )
    # Mark ONLY actions apply() actually folded in (status == "applied") —
    # marking an unconfirmed/ambiguous refusal poisons the exactly-once
    # contract (the 2026-07-01 incident: HON/DD marked applied on refused
    # applies, permanently freezing their un-restated histories behind the
    # marker). A refused action stays pending and re-checks every run.
    applied_ids = {r["action_id"] for r in applied if r["status"] == "applied"}
    for a in pending:
        if a.action_id in applied_ids:
            registry.mark_applied(a, _ca.STORE_ARCTICDB_UNIVERSE, run_id=run_id)
        else:
            log.warning(
                "daily_append basis-consistency: %s action %s NOT marked "
                "applied (apply() refused it) — will re-check next run",
                ticker, a.action_id,
            )
    return restated if n_changed else hist


# An incoming-row-vs-prior-stored-close ratio at or beyond this multiple (or
# its reciprocal) engages the splice basis-guard's checks. Genuine market
# moves overlap the split-ratio zone (2026 scan: CAR −48%, KD −55% are real),
# so crossing this threshold is a TRIGGER for the deterministic tests
# (registered-action match, same-date stored-row disagreement), never a
# refusal by magnitude alone.
_SPLICE_GUARD_SOFT_RATIO = 1.5
# Tolerance for matching the observed splice ratio to a registered action's
# factor (mirrors corporate_actions._PRICE_EVIDENCE_REL_TOL).
_SPLICE_GUARD_REL_TOL = 0.15
# Operator escape hatch for a GENUINE ≥90% market move the guard would
# otherwise refuse: comma-separated TICKER:YYYY-MM-DD entries.
_SPLICE_GUARD_ALLOW_ENV = "DAILY_APPEND_SPLICE_GUARD_ALLOW"
# Price fields scaled by a split factor (volume scales inversely).
_SPLICE_PRICE_FIELDS = ("Open", "High", "Low", "Close", "VWAP")


def _splice_basis_guard(
    ticker: str,
    bar,
    hist: pd.DataFrame,
    today_ts,
    actions: list,
    registry,
):
    """Refuse — or deterministically restate — an incoming daily row whose
    close sits on a different adjusted basis than the stored history.

    The discriminator between a basis mismatch and a genuine market move
    (calibrated on the 2026 universe scan: STRL +52%, CAR −48%, KD −55% are
    all REAL single-day moves that overlap the 2:1-split ratio zone, so
    magnitude alone cannot refuse):

      * A registered split with ``ex_date > row_date`` already applied to the
        store explains a ``1/factor`` gap exactly → the row arrived on the
        pre-action basis (polygon restates its aggregates only once the ex
        date lands — the CRWD 2026-06-30 case). Deterministic: restate it.
      * A split-like disagreement with an ALREADY-STORED row for the SAME
        date whose value coheres with its own neighbors is NEVER a market
        move (same date, same market — the HON 2026-06-26 case: incoming
        464.42 vs stored 232.21 on a series whose 06-25 was 231.24) →
        refuse the overwrite, keep the stored row.
      * A pure append (no stored same-date row) with an unexplained
        split-like ratio can be a genuine crash/pop → WRITE it, but at
        ERROR severity so the flow-doctor pages the same day (if it was a
        missed corporate action, the registry detection + restatement path
        heals it as soon as a record lands; if it never lands, the page is
        the operator's cue).

    Returns ``(bar, verdict)`` with verdict ``"ok"`` / ``"restated"`` /
    ``"refused"``. Fail-open on missing inputs (no prior close / NaN bar
    close): the guard protects against DISCONTINUITY; absence of evidence is
    not one. ``DAILY_APPEND_SPLICE_GUARD_ALLOW=TICKER:YYYY-MM-DD`` force-writes
    through a refusal.
    """
    import corporate_actions as _ca

    try:
        bar_close = float(bar["Close"])
    except Exception:  # noqa: BLE001 - NaN/absent close is handled upstream
        return bar, "ok"
    if hist is None or hist.empty or "Close" not in hist.columns:
        return bar, "ok"
    prior = hist.loc[hist.index < today_ts, "Close"].dropna()
    if prior.empty or not np.isfinite(bar_close) or bar_close <= 0:
        return bar, "ok"
    prior_close = float(prior.iloc[-1])
    if prior_close <= 0:
        return bar, "ok"

    ratio = bar_close / prior_close
    if 1.0 / _SPLICE_GUARD_SOFT_RATIO < ratio < _SPLICE_GUARD_SOFT_RATIO:
        return bar, "ok"

    # A registered split whose ex_date is AFTER this row's date and which is
    # already folded into the stored history explains the gap exactly: the
    # incoming row is pre-action basis, the store is post-action basis, and
    # the observed ratio is 1/factor. Restate the row by the action's factor.
    for a in actions:
        if getattr(a, "type", None) != "split":
            continue
        try:
            ex_ts = pd.Timestamp(a.ex_date).normalize()
            factor = _ca.expected_factor(a)
        except Exception:  # noqa: BLE001 - malformed action, can't explain
            continue
        if factor <= 0 or ex_ts <= today_ts:
            continue
        if registry is not None and not registry.is_applied(
            _ca.STORE_ARCTICDB_UNIVERSE, a.action_id
        ):
            continue
        expected_gap = 1.0 / factor
        if abs(ratio - expected_gap) <= _SPLICE_GUARD_REL_TOL * expected_gap:
            restated_bar = bar.copy()
            for fld in _SPLICE_PRICE_FIELDS:
                try:
                    val = float(restated_bar[fld])
                except Exception:  # noqa: BLE001 - absent/NaN field, skip
                    continue
                if np.isfinite(val):
                    restated_bar[fld] = val * factor
            try:
                vol = float(restated_bar["Volume"])
                if np.isfinite(vol):
                    restated_bar["Volume"] = round(vol / factor)
            except Exception:  # noqa: BLE001 - absent/NaN volume, skip
                pass
            log.warning(
                "daily_append splice basis-guard: %s @ %s row arrived on the "
                "PRE-action basis (ratio %.4f vs stored history; registered "
                "%s, ex %s, already applied to the store) — restated the "
                "incoming row by factor %.6g before splice",
                ticker, today_ts.date(), ratio, a.human(), a.ex_date, factor,
            )
            return restated_bar, "restated"

    allow = {
        entry.strip()
        for entry in os.environ.get(_SPLICE_GUARD_ALLOW_ENV, "").split(",")
        if entry.strip()
    }
    allowlisted = f"{ticker}:{today_ts.date()}" in allow

    # Same-date disagreement test: an on-basis stored row for THIS date that
    # coheres with its own prior neighbor cannot be re-printed a split-like
    # factor away by the same market — the incoming row is off-basis.
    stored_same = None
    if today_ts in hist.index:
        try:
            v = float(hist.loc[today_ts, "Close"])
            if np.isfinite(v) and v > 0:
                stored_same = v
        except Exception:  # noqa: BLE001 - malformed stored cell, no verdict
            stored_same = None
    if stored_same is not None:
        same_ratio = bar_close / stored_same
        stored_coheres = (
            1.0 / _SPLICE_GUARD_SOFT_RATIO
            < stored_same / prior_close
            < _SPLICE_GUARD_SOFT_RATIO
        )
        if (
            not (1.0 / _SPLICE_GUARD_SOFT_RATIO < same_ratio < _SPLICE_GUARD_SOFT_RATIO)
            and stored_coheres
        ):
            if allowlisted:
                log.warning(
                    "daily_append splice basis-guard: %s @ %s same-date "
                    "disagreement (incoming %.4f vs stored %.4f) is "
                    "operator-allowlisted via %s — overwriting",
                    ticker, today_ts.date(), bar_close, stored_same,
                    _SPLICE_GUARD_ALLOW_ENV,
                )
                return bar, "ok"
            log.error(
                "daily_append splice basis-guard: REFUSING %s @ %s — incoming "
                "close %.4f disagrees with the ALREADY-STORED close %.4f for "
                "the SAME date by a split-like factor (%.4f) while the stored "
                "row coheres with its neighbors. Same-date same-market "
                "disagreement is a basis mismatch, never a move (2026-06-26 "
                "HON incident class) — keeping the stored row. Force with "
                "%s=%s:%s",
                ticker, today_ts.date(), bar_close, stored_same, same_ratio,
                _SPLICE_GUARD_ALLOW_ENV, ticker, today_ts.date(),
            )
            return bar, "refused"

    # Pure append with an unexplained split-like ratio: can be a genuine
    # crash/pop (CAR −48%, KD −55% verified real) — write it, page loudly.
    log.error(
        "daily_append splice basis-guard: %s @ %s incoming close %.4f vs "
        "prior stored close %.4f (ratio %.4f) is split-like and NO registered "
        "corporate action explains it. Writing (a genuine move is possible), "
        "but if a corporate-action record lands later the restatement path "
        "must flatten this boundary — verify the split calendar",
        ticker, today_ts.date(), bar_close, prior_close, ratio,
    )
    return bar, "ok"


def daily_append(
    date_str: str | None = None,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    skip_if_exists: bool = False,
    expected_tickers: list[str] | None = None,
) -> dict:
    """
    Append today's features to ArcticDB universe.

    For each ticker:
    1. Read recent history from ArcticDB (tail ~300 rows for feature warmup)
    2. Append today's OHLCV row
    3. Compute features for the combined series
    4. Extract the last row (today) and append to ArcticDB

    **Producer-side write-coordination lock.** When
    ``ALPHA_ENGINE_UNIVERSE_WRITER_LOCK_ENABLED=true`` is in the
    environment, this function acquires the
    :func:`nousergon_lib.locks.universe_writer_lock` at the top of
    the call and releases it on exit. The lock closes the
    manual-invocation half of the single-writer-per-resource invariant
    (forensic / backfill / dev shells running ``python -m
    builders.daily_append`` that bypass the SF entirely). The SF-entry
    half lives in the L274 MutualExclusionGuard (DynamoDB-side).
    Default-OFF for safe rollout — flip the env var to ``true`` after
    one clean weekday + Saturday cycle. ``dry_run=True`` always bypasses
    the lock (read-only path, no race surface).

    On lock contention, :exc:`nousergon_lib.locks.LockHeldByAnotherWriterError`
    propagates to the caller — fail-loud per
    ``~/Development/CLAUDE.md``'s no-silent-fails rule.

    Parameters
    ----------
    skip_if_exists
        When True, tickers whose ``date_str`` row is already in ArcticDB
        skip the read/compute/write cycle entirely (counted as ``n_skip``).
        Use for re-runs of EOD post-market (yfinance source) where today's
        row is final and re-writing it is a wasteful full-series rewrite
        via the backfill path. Always leave False for MorningEnrich
        (polygon source) — that path must overwrite to apply polygon's
        true volume-weighted VWAP over yfinance's NaN.

        Background: a re-run with ``skip_if_exists=False`` enters
        ``_write_row_backfill_safe``'s backfill branch on every ticker
        (target_ts == existing.index.max()), which calls
        ``lib.write(combined, prune_previous_versions=True)`` per ticker.
        904 × ~1.5s = ~22 min — over the SSM 1200s timeout. The 2026-05-01
        EOD SF rerun timed out exactly here after our manual recovery
        run had already written today's rows.
    expected_tickers
        When provided, the missing-from-closes hard-fail at step 2b is
        scoped to ``arctic_universe ∩ expected_tickers`` instead of the
        full ArcticDB universe. Lets the caller (MorningEnrich /
        weekday DailyData) say "these are the tickers I asked polygon
        for" so S&P churn-out stragglers (still in ArcticDB awaiting a
        prune cycle, no longer in current constituents.json) don't trip
        the threshold. 2026-05-02 incident: 8 tickers got dropped from
        the index this past week (ASGN, GTM, HOLX, KMPR, LW, MOH,
        MTCH, PAYC); ArcticDB universe still had them; MorningEnrich
        no longer requested them; missing-from-closes saw 12 vs the
        threshold of 5 and halted the SF. With expected_tickers passed,
        only the 4 chronic polygon-coverage gaps (BF-B, BRK-B, MOG-A,
        PSTG) trip the check, all under threshold.

        When None (default — preserves the prior behavior for any caller
        not yet updated), the check uses the full ArcticDB universe as
        the expected set.

    Returns summary dict.
    """
    if _writer_lock_enabled(dry_run):
        from nousergon_lib.locks import universe_writer_lock

        with universe_writer_lock(writer_id=_build_writer_id()):
            return _daily_append_impl(
                date_str=date_str,
                bucket=bucket,
                dry_run=dry_run,
                skip_if_exists=skip_if_exists,
                expected_tickers=expected_tickers,
            )
    return _daily_append_impl(
        date_str=date_str,
        bucket=bucket,
        dry_run=dry_run,
        skip_if_exists=skip_if_exists,
        expected_tickers=expected_tickers,
    )


def _daily_append_impl(
    date_str: str | None = None,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    skip_if_exists: bool = False,
    expected_tickers: list[str] | None = None,
) -> dict:
    """Inner implementation of :func:`daily_append`.

    Single-writer-lock semantics are provided by the outer
    :func:`daily_append` wrapper; this function performs the actual
    OHLCV + features write to ArcticDB. Callers should invoke
    :func:`daily_append`, not this. Exists separately so the lock
    wrap is a tiny diff and the existing 800-line body stays
    structurally unchanged.
    """
    s3 = boto3.client("s3")
    if date_str is None:
        from dates import default_run_date  # config#1014: trading-day axis

        date_str = default_run_date()

    # NYSE-calendar gate (config#1572): appending a row for a non-trading day
    # plants a phantom session in the ArcticDB training store (2026-06-19
    # Juneteenth entered universe-wide via a fabricated daily-closes parquet).
    # A non-trading date_str is always a caller error — fail loud, same
    # calendar source of truth as the Step Function's CheckTradingDay gate.
    from datetime import date as _date

    from nousergon_lib.trading_calendar import is_trading_day as _is_td

    if not _is_td(_date.fromisoformat(date_str)):
        raise ValueError(
            f"daily_append: date_str={date_str} is not an NYSE trading day — "
            f"refusing to append a phantom session to the universe "
            f"(config#1572)."
        )

    today_ts = pd.Timestamp(date_str)
    t0 = time.time()

    # ── 1. Load today's OHLCV ────────────────────────────────────────────────
    # _load_daily_closes raises on missing/empty file; no need for status-return guard.
    closes = _load_daily_closes(s3, bucket, date_str)

    # ── 2. Load supporting data ──────────────────────────────────────────────
    sector_map = _load_sector_map(s3, bucket)
    # sub_sector_etf_map (config#934): ticker → sub-sector benchmark ETF
    # (SMH/IGV/…), defaulting to the sector ETF for sub-industries with no
    # liquid proxy. Best-effort: an empty map (file not yet written by the
    # weekly collector) degrades sub_sector_vs_benchmark_* to neutral 0.0.
    sub_sector_etf_map = _load_sub_sector_etf_map(s3, bucket)
    # The distinct NON-sector sub-sector ETF symbols this run must keep fresh
    # in ArcticDB (the XL* sector ETFs are already handled by the sector-ETF
    # write/read loops). Derived from the map's values so the fetched symbol
    # universe tracks whatever GICS_SUBINDUSTRY_TO_ETF currently maps — no
    # separate hard-coded list to drift out of sync.
    sub_sector_etf_symbols = sorted(
        {sym for sym in sub_sector_etf_map.values() if sym and not _is_sector_etf(sym)}
    )
    fundamentals = _load_cached_fundamentals(s3, bucket, date_str)
    alt_data = _load_cached_alternative(s3, bucket)

    if not dry_run:
        universe_lib = get_universe_lib(bucket)
        macro_lib = get_macro_lib(bucket)
    else:
        universe_lib = None
        macro_lib = None

    # ── 2a. Update macro / sector-ETF series in ArcticDB ─────────────────────
    # This block was previously the final write step (old "step 5") and ran
    # AFTER the universe-coverage guard at step 2b. That ordering coupled
    # macro/SPY freshness to stock-coverage correctness: a 7-stock universe
    # gap on 2026-04-27 raised at the guard before SPY ever got written, which
    # then hard-failed the EOD reconcile (alpha against stale SPY is by-design
    # rejected) and blacked out the EOD email + alpha tracking for the day.
    #
    # Macro keys are a fixed list of ~18 well-known tickers (SPY, VIX, sector
    # ETFs); their freshness has nothing to do with whether 5 or 50 stocks
    # went missing in the universe collection. Doing the macro write FIRST
    # decouples the two concerns: macro lands in ArcticDB regardless of
    # downstream stock-side issues, and the universe guard still raises
    # non-zero so operators get paged on the universe gap. Net effect on
    # 2026-04-27-style failures: EOD email goes out, daily-data still exits 1.
    #
    # update() semantics: same-date rows overwrite instead of appending. See
    # _write_row_backfill_safe — it routes append vs backfill correctly.
    #
    # Per feedback_hard_fail_until_stable: count which keys got updated vs
    # silently skipped due to missing closes data, verify the writes actually
    # landed, and raise with a named reject list if anything went missing.
    # Previous behavior: if closes.get(key) returned None (upstream collection
    # gap), the update was silently skipped. Combined with stock tickers all
    # hitting the "today already exists" skip path after a backfill, a run
    # could return status="ok" with ZERO data actually written. 2026-04-15
    # 08:39 PT manual rerun reproduced this — Step Function marked SUCCEEDED,
    # macro/SPY stayed at 4/10 for 5 days until an inference-side preflight
    # caught it.
    # ── Schema-drift incident counter (config#1150 Batch B) ──────────────────
    # Counts ArcticDB schema-drift write failures (StreamDescriptorMismatch /
    # SchemaException / NormalizationException — see _SCHEMA_DRIFT_EXC) across
    # BOTH the macro/sector and the chunked universe write paths this run.
    # Single-element list so the inner write blocks share one mutable count.
    # Before config#1150 these exceptions propagated UNCOUNTED — a descriptor
    # regression was invisible to the report card until an operator noticed the
    # failed run by hand. ``_emit_schema_drift`` writes the count to CloudWatch +
    # manifest.json; on a clean run it fires once at the end (count 0), and on a
    # schema-drift abort the _count_schema_drift wrapper fires it before the
    # exception re-raises so the incident still lands.
    n_schema_drift = [0]

    def _emit_schema_drift(count: int) -> None:
        _emit_schema_drift_metric(count)
        _write_schema_drift_manifest(s3, bucket, date_str, count)

    macro_missing_from_closes: list[str] = []
    macro_updated: list[str] = []
    sector_updated: list[str] = []

    # Track per-symbol write mode (append vs backfill) so the verification
    # check below can apply the right correctness assertion. Append: last
    # readback row should equal target_ts. Backfill: target_ts should be
    # in the readback index (could be anywhere in the middle).
    macro_write_modes: dict[str, str] = {}

    macro_keys = ["SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"]

    if not dry_run:
        for key in macro_keys:
            bar = closes.get(key)
            if bar is None or np.isnan(bar.get("Close", np.nan)):
                macro_missing_from_closes.append(key)
                continue
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):
                    mode = _write_row_backfill_safe(macro_lib, key, new_row)
                macro_updated.append(key)
                macro_write_modes[key] = mode
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to update macro {key} bar for {date_str}: {exc}"
                ) from exc

        # Sector ETFs — iterate the expected list explicitly rather than
        # filtering closes.keys(), so a missing XL* key surfaces as a loud
        # reject instead of a silent skip.
        sector_etfs = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
                       "XLP", "XLRE", "XLU", "XLV", "XLY"]
        for sym in sector_etfs:
            bar = closes.get(sym)
            if bar is None or np.isnan(bar.get("Close", np.nan)):
                macro_missing_from_closes.append(sym)
                continue
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):
                    mode = _write_row_backfill_safe(macro_lib, sym, new_row)
                sector_updated.append(sym)
                macro_write_modes[sym] = mode
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to update sector ETF {sym} bar for {date_str}: {exc}"
                ) from exc

        # Sub-sector benchmark ETFs (config#934) — SMH/IGV/XBI/… . Unlike the
        # XL* sector ETFs above (load-bearing for sector_vs_spy_* on every
        # stock, hence hard-fail), these are ADDITIVE/best-effort: a missing
        # bar degrades one feature family (sub_sector_vs_benchmark_*) to its
        # neutral default for the affected stocks rather than failing the run,
        # matching feature_engineer's None-input neutral-0.0 fallback and the
        # HYOAS-style optional-input philosophy. Not added to
        # macro_missing_from_closes (which drives the mandatory hard-fail).
        for sym in sub_sector_etf_symbols:
            bar = closes.get(sym)
            if bar is None or np.isnan(bar.get("Close", np.nan)):
                log.warning(
                    "Sub-sector ETF %s missing from daily closes for %s — "
                    "sub_sector_vs_benchmark_* will neutral-default for its "
                    "stocks (non-fatal, additive feature).",
                    sym, date_str,
                )
                continue
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):
                    mode = _write_row_backfill_safe(macro_lib, sym, new_row)
                sector_updated.append(sym)
                macro_write_modes[sym] = mode
            except Exception as exc:
                log.warning(
                    "Sub-sector ETF %s bar write failed for %s (non-fatal — "
                    "sub_sector_vs_benchmark_* will neutral-default): %s",
                    sym, date_str, exc,
                )

        # HYOAS (config#939, credit spreads) — best-effort, NOT added to
        # `macro_keys` above. Unlike SPY/VIX/TNX/etc. (battle-tested,
        # load-bearing for every downstream feature), HYOAS is a newer,
        # optional macro input: FRED-license-gated to 2023+ and not yet
        # guaranteed present in every daily_closes parquet. Gating the
        # WHOLE daily pipeline on its freshness (via macro_missing_from_closes
        # -> the hard-fail below) would be a disproportionate blast radius
        # for one optional feature — feature_engineer.compute_features
        # already neutral-defaults hy_oas_credit_spread_pct to 0.0 when
        # hyoas_series is None, so a missing HYOAS bar degrades one column
        # to its neutral default rather than failing the run.
        bar = closes.get("HYOAS")
        if bar is not None and not np.isnan(bar.get("Close", np.nan)):
            try:
                new_row = pd.DataFrame(
                    [{"Close": bar["Close"]}],
                    index=pd.DatetimeIndex([today_ts]),
                )
                new_row.index.name = "date"
                with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):
                    mode = _write_row_backfill_safe(macro_lib, "HYOAS", new_row)
                macro_updated.append("HYOAS")
                macro_write_modes["HYOAS"] = mode
            except Exception as exc:
                log.warning(
                    "HYOAS macro bar write failed for %s (non-fatal — "
                    "hy_oas_credit_spread_pct will neutral-default): %s",
                    date_str, exc,
                )
        else:
            log.info(
                "HYOAS bar missing/NaN from today's daily_closes for %s — "
                "hy_oas_credit_spread_pct will neutral-default this run "
                "(non-fatal, unlike the mandatory macro_keys set below).",
                date_str,
            )

        # Hard-fail on any missing key — macro inputs are not optional.
        # downstream feature compute + predictor preflight both depend on
        # these being fresh.
        if macro_missing_from_closes:
            raise RuntimeError(
                f"Macro/sector-ETF keys missing from today's daily_closes "
                f"parquet: {macro_missing_from_closes}. Upstream daily_closes "
                f"collection (polygon → FRED → yfinance fallback chain) did "
                f"not produce bars for these tickers on {date_str}. Macro "
                f"data is critical for downstream inference (SPY for "
                f"return_vs_spy_5d, VIX for vix_level, sector ETFs for "
                f"sector-relative features). Fix the upstream collection "
                f"before claiming pipeline success."
            )

        # Verify writes landed. The update() / write() calls above are
        # fire-and-forget (no return value surfaces a success flag), so
        # we read back each key and assert target_ts is present. The
        # check is mode-aware:
        #   - append mode: last readback row should equal target_ts
        #     (catches the 2026-04-15 silent-stale failure where SSM
        #     reported SUCCEEDED but macro/SPY stayed 5 days behind)
        #   - backfill mode: target_ts should be IN the readback index,
        #     anywhere (last date is naturally future relative to
        #     target_ts when we backfill an old date)
        target_ts_norm = today_ts.normalize()
        verification_failures: list[tuple[str, str]] = []
        for key in macro_updated + sector_updated:
            try:
                readback = macro_lib.read(key).data
            except Exception as exc:
                verification_failures.append((key, f"readback error: {exc}"))
                continue
            if readback.empty:
                verification_failures.append((key, "readback empty"))
                continue
            mode = macro_write_modes.get(key, "append")
            if mode == "backfill":
                # Target date should be present somewhere in the series.
                index_norm = pd.DatetimeIndex(readback.index).normalize()
                if target_ts_norm not in index_norm:
                    verification_failures.append(
                        (key, f"backfill target {target_ts_norm.date()} not in readback index "
                              f"(last={pd.Timestamp(readback.index[-1]).date()})")
                    )
            else:
                last_ts = pd.Timestamp(readback.index[-1]).normalize()
                if last_ts != target_ts_norm:
                    verification_failures.append(
                        (key, f"last date {last_ts.date()} != expected {target_ts_norm.date()}")
                    )
        if verification_failures:
            raise RuntimeError(
                f"Macro update verification failed for {date_str}: "
                f"{verification_failures}. update()/write() calls completed without "
                f"exception but readback shows the row is missing. Investigate "
                f"ArcticDB commit / consistency semantics."
            )

        # config-I2702 deliverable #1/#2: only reachable when every key in
        # macro_updated + sector_updated has just been readback-verified
        # present for date_str (the raise above is unconditional on any
        # failure) — this is the artifact-asserting checkpoint the EOD
        # precondition probe reads. Fires even when macro_updated is a
        # subset of macro_keys was never possible (macro_missing_from_closes
        # hard-raised earlier in this function for any missing macro key) —
        # SPY is always in verified_keys whenever this line executes.
        _write_macro_freshness_sentinel(
            s3, bucket, run_date=date_str, verified_keys=macro_updated + sector_updated,
        )

    # ── 2b. Detect tickers that exist in ArcticDB universe but are missing
    #        from today's daily_closes parquet ─────────────────────────────────
    # Without this guard, the line ~274 ``stock_tickers = [t for t in closes ...]``
    # filter silently drops every ArcticDB symbol absent from today's closes —
    # no counter increments, no WARN log. That class was the recurring "8
    # tickers regressed to 4/01" failure mode (ROADMAP 2026-04-25 P1) — daily
    # closes upstream stops returning a ticker, daily_append silently no-ops
    # writes for it across many weekdays, and the regression only surfaces
    # when an unrelated freshness preflight catches it days later.
    #
    # Runs AFTER the macro write at step 2a so a stock-universe gap can't
    # block macro/SPY freshness — see the rationale on the 2a header for the
    # 2026-04-27 EOD blackout that motivated decoupling these two concerns.
    # This guard still raises non-zero on threshold violations; pipelines
    # exit 1 and operators get paged. Threshold default (5) matches the
    # absolute count of the 2026-04-25 regression; env-overridable so prod
    # can tune without a redeploy.
    n_missing_from_closes = 0
    if not dry_run:
        try:
            arctic_stock_symbols = set(universe_lib.list_symbols())
        except Exception as exc:
            raise RuntimeError(
                f"Could not list ArcticDB universe symbols (needed for "
                f"missing-from-closes check): {exc}"
            ) from exc
        # closes contains everything: stocks + macro keys + sector ETFs.
        # Reduce to the stock set so the diff is apples-to-apples with
        # universe_lib's contents (which holds only stocks). Same predicate
        # as the ``stock_tickers`` write-path filter below — keep them in
        # lockstep (the ``_UNIVERSE_EXTRA`` carve-out is REQUIRED here: SPY
        # is in ``_SKIP_TICKERS`` but IS written to `universe`, so without
        # the carve-out SPY would always compute as "missing from closes"
        # even on days it's genuinely present — config-I2703 fixed the
        # sibling gap where this same omission made the freshness scan
        # blind to SPY entirely).
        closes_stock_keys = {
            t for t in closes
            if (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)
            and not _is_sector_etf(t)
        }
        # Scope "expected" to the intersection of ArcticDB universe and the
        # caller's request list. A ticker dropped from S&P this week (still
        # in ArcticDB awaiting prune, no longer in constituents.json) was
        # never asked for from polygon — its absence from closes is by
        # design, not a data gap. Without this, every S&P churn week trips
        # the threshold (2026-05-02: 8 churn-out tickers + 4 chronic =
        # 12 > 5 → SF halt). With it, only tickers we both want to track
        # AND have history for can flag the alarm. ``_UNIVERSE_EXTRA``
        # members are excepted from the ``_SKIP_TICKERS`` exclusion (same
        # carve-out as ``closes_stock_keys`` above and the write-path
        # ``stock_tickers`` filter) — SPY is a hard-pinned benchmark, never
        # a churn-eligible S&P straggler (config-I2703).
        if expected_tickers is not None:
            expected_stocks = {
                t.lstrip("^") for t in expected_tickers
                if (t.lstrip("^") not in _SKIP_TICKERS or t.lstrip("^") in _UNIVERSE_EXTRA)
                and not _is_sector_etf(t.lstrip("^"))
            }
            relevant_arctic = arctic_stock_symbols & expected_stocks
            stragglers = arctic_stock_symbols - expected_stocks
            if stragglers:
                log.info(
                    "daily_append: %d ArcticDB stock symbols absent from "
                    "expected_tickers (S&P churn-out stragglers, awaiting "
                    "prune) — excluded from missing-from-closes check: %s",
                    len(stragglers), sorted(stragglers)[:20],
                )
        else:
            relevant_arctic = arctic_stock_symbols
        missing_from_closes = sorted(relevant_arctic - closes_stock_keys)
        n_missing_from_closes = len(missing_from_closes)
        # Always emit a metric — silent regression in the slow-drift band
        # (1-2 tickers) won't trip the hard-fail but is still observable.
        _emit_missing_from_closes_metric(n_missing_from_closes)
        threshold = int(
            os.environ.get("DAILY_APPEND_MISSING_THRESHOLD", "5")
        )
        if n_missing_from_closes > threshold:
            raise RuntimeError(
                f"daily_append: {n_missing_from_closes} tickers in ArcticDB "
                f"universe missing from today's daily_closes parquet "
                f"(threshold={threshold}). Missing: {missing_from_closes}. "
                f"Either upstream daily_closes collection stopped emitting "
                f"these tickers (most common — polygon/yfinance hiccup or "
                f"ticker-listing change), or these tickers are legitimately "
                f"delisted and need to be pruned from ArcticDB universe "
                f"(see ROADMAP P2 'ArcticDB universe pruning'). "
                f"Override threshold via DAILY_APPEND_MISSING_THRESHOLD env "
                f"var if you've already triaged the list."
            )
        elif n_missing_from_closes > 0:
            log.warning(
                "daily_append: %d tickers in ArcticDB universe missing from "
                "closes (below %d hard-fail threshold) — %s",
                n_missing_from_closes, threshold, missing_from_closes,
            )

    # ── 3. Load macro series from ArcticDB into in-memory dict ───────────────
    # Read-only on ArcticDB. Builds a multi-day series per macro key for the
    # per-stock feature loop (return_vs_spy_5d, vix_level, etc., which need
    # context not just today's value). After step 2a's write, today's row is
    # already in ArcticDB — the pd.concat below is now redundant in the happy
    # path, but kept defensively so this block remains correct if step 2a is
    # ever skipped or factored out.
    macro: dict[str, pd.Series] = {}

    if not dry_run:
        for key in macro_keys:
            try:
                mdf = macro_lib.read(key).data
            except Exception as exc:
                raise RuntimeError(
                    f"Macro series {key} unreadable from ArcticDB — features depend on all macro inputs: {exc}"
                ) from exc
            if "Close" not in mdf.columns:
                raise RuntimeError(
                    f"Macro series {key} has no Close column — ArcticDB schema drift"
                )
            series = mdf["Close"].dropna()
            ticker_close = closes.get(key)
            if ticker_close and not np.isnan(ticker_close["Close"]):
                series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                # Re-sort after the today_ts concat: when today_ts is NOT the
                # latest date in the stored macro series (holiday backfill —
                # e.g. Juneteenth 2026-06-19 closed, so a Monday append carries
                # today_ts=Thu 6/18 while ArcticDB macro already holds a later
                # session), dedup-keep-last moves the today_ts row to the tail
                # of an otherwise-ascending index → non-monotonic. compute_features
                # does `macro.reindex(df.index, method="ffill")`, which raises
                # "index must be monotonic increasing or decreasing" for EVERY
                # ticker (shared series). Mirror the per-ticker combined/warmup
                # frames, which already dedup AND sort. (2026-06-22 weekday-SF fail.)
                series = series[~series.index.duplicated(keep="last")].sort_index()
            macro[key] = series

        # Sector ETFs — every XL* in the macro library must read cleanly.
        # Missing any one corrupts sector-relative features for stocks in
        # that sector.
        for sym in macro_lib.list_symbols():
            if sym.startswith("XL"):
                try:
                    mdf = macro_lib.read(sym).data
                except Exception as exc:
                    raise RuntimeError(
                        f"Sector ETF {sym} unreadable from ArcticDB: {exc}"
                    ) from exc
                if "Close" not in mdf.columns:
                    raise RuntimeError(
                        f"Sector ETF {sym} has no Close column — ArcticDB schema drift"
                    )
                series = mdf["Close"].dropna()
                ticker_close = closes.get(sym)
                if ticker_close and not np.isnan(ticker_close["Close"]):
                    series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                    # Re-sort after the today_ts concat — see the macro_keys
                    # loop above; same non-monotonic-on-holiday-backfill hazard.
                    series = series[~series.index.duplicated(keep="last")].sort_index()
                macro[sym] = series

        # Sub-sector benchmark ETFs (config#934) — best-effort read, mirroring
        # the best-effort write above. NOT a hard-fail loop like the XL*
        # sector ETFs: a symbol absent from ArcticDB (never populated yet, or
        # unreadable) simply leaves macro[sym] unset, and the downstream
        # `sub_sector_etf_series = macro.get(sym)` resolves to None →
        # sub_sector_vs_benchmark_* neutral-defaults for the affected stocks.
        for sym in sub_sector_etf_symbols:
            if sym in macro:
                continue  # already loaded (e.g. also a macro_key) — don't reread
            try:
                mdf = macro_lib.read(sym).data
            except Exception as exc:
                log.warning(
                    "Sub-sector ETF %s unreadable from ArcticDB (non-fatal — "
                    "sub_sector_vs_benchmark_* will neutral-default): %s",
                    sym, exc,
                )
                continue
            if "Close" not in mdf.columns:
                log.warning(
                    "Sub-sector ETF %s has no Close column — ArcticDB schema "
                    "drift (non-fatal, treated as missing).", sym,
                )
                continue
            series = mdf["Close"].dropna()
            ticker_close = closes.get(sym)
            if ticker_close and not np.isnan(ticker_close["Close"]):
                series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                # Re-sort after the today_ts concat — see the macro_keys loop
                # above; same non-monotonic-on-holiday-backfill hazard.
                series = series[~series.index.duplicated(keep="last")].sort_index()
            macro[sym] = series

        # HYOAS (config#939, credit spreads) — best-effort read, mirroring
        # the best-effort write above. Not part of the mandatory
        # `macro_keys` hard-fail loop: if the symbol doesn't exist yet
        # (fresh deploy before any backfill/daily-write has populated it)
        # or the read otherwise fails, `macro["HYOAS"]` simply stays
        # unset and `hyoas_series = macro.get("HYOAS")` downstream is
        # None — feature_engineer.compute_features already neutral-
        # defaults hy_oas_credit_spread_pct to 0.0 in that case.
        try:
            mdf = macro_lib.read("HYOAS").data
        except Exception as exc:
            log.info(
                "HYOAS macro series unreadable from ArcticDB (non-fatal — "
                "hy_oas_credit_spread_pct will neutral-default): %s", exc,
            )
        else:
            if "Close" in mdf.columns:
                series = mdf["Close"].dropna()
                ticker_close = closes.get("HYOAS")
                if ticker_close and not np.isnan(ticker_close["Close"]):
                    series = pd.concat([series, pd.Series([ticker_close["Close"]], index=[today_ts])])
                    series = series[~series.index.duplicated(keep="last")].sort_index()
                macro["HYOAS"] = series
            else:
                log.warning(
                    "HYOAS macro series has no Close column — ArcticDB "
                    "schema drift (non-fatal, treated as missing)."
                )

    t_load = time.time() - t0
    log.info("Data loaded in %.1fs: %d closes, %d macro series", t_load, len(closes), len(macro))

    # ── 4. Compute features and append ───────────────────────────────────────
    spy_series = macro.get("SPY")
    vix_series = macro.get("VIX")
    tnx_series = macro.get("TNX")
    irx_series = macro.get("IRX")
    gld_series = macro.get("GLD")
    uso_series = macro.get("USO")
    vix3m_series = macro.get("VIX3M")
    hyoas_series = macro.get("HYOAS")

    # Filter to stock tickers only
    # _UNIVERSE_EXTRA (SPY) is maintained as a full universe member here too;
    # it bootstraps into `universe` on the weekly backfill and is skipped
    # gracefully (hist None) until then. Still written Close-only to `macro`
    # separately. SPY stays in _SKIP_TICKERS so the coverage-diff / freshness
    # accounting below keeps treating it as non-stock.
    stock_tickers = [
        t for t in closes
        if (t not in _SKIP_TICKERS or t in _UNIVERSE_EXTRA)
        and not _is_sector_etf(t)
    ]

    n_ok = 0              # fully-featured rows (all FEATURES finite)
    n_skip = 0            # legitimate skips (dry_run, NaN close from upstream)
    n_err = 0             # ArcticDB read failures
    n_partial = 0         # rows written with ≥1 NaN feature (short-history, etc.)
    n_parquet_warmup = 0  # rows whose feature compute used parquet-enriched context
    n_quality_blocked = 0  # rows refused by validate_today_row (block severity)
    n_quality_warned = 0   # rows written but flagged by validate_today_row (warn)
    quality_counts_by_type: dict[str, int] = {}
    quality_blocked_details: list[tuple[str, str]] = []  # (ticker, anomaly_type)

    # L2 series-contract counters (alpha-engine-config#2456) — separate from
    # the validate_today_row counters above since the two gates run
    # independently and can each block/warn on the same row for different
    # reasons; keeping them distinct preserves per-surface attribution in
    # the aggregated end-of-run record.
    n_l2_quarantined = 0  # rows refused by the L2 series-contract gate
    n_l2_alarmed = 0      # rows written but flagged (alarm, no quarantine)
    l2_counts_by_gate: dict[str, int] = {}
    l2_quarantined_details: list[tuple[str, str]] = []  # (ticker, gate_name)

    # Read DAILY_APPEND_BLOCK_ANOMALY_TYPES once per run (raises on malformed
    # JSON / unknown types — fail fast before the chunked pass begins).
    block_anomaly_types = _load_block_anomaly_types()
    l2_block_gates = _load_l2_block_gates()

    # ── Corporate-action basis-consistency guard setup (PR4, config#1433) ────
    # Build the registry + the registered splits grouped by ticker ONCE.
    # Registry-driven (NO polygon call). In steady state every relevant split is
    # already applied (the morning sync did it), so the per-ticker guard below is
    # a cheap ``is_applied`` no-op; it only does work as a backstop when a
    # symbol's history is somehow still un-restated when we are about to append.
    ca_registry = None
    splits_by_ticker: dict[str, list] = {}
    if not dry_run:
        try:
            import corporate_actions as _ca

            ca_registry = _ca.CorporateActionRegistry(s3, bucket)
            for _action in ca_registry.list_actions(types=["split"]):
                splits_by_ticker.setdefault(_action.ticker, []).append(_action)
        except Exception as exc:
            log.warning(
                "daily_append: corporate-action registry unavailable (%s) — "
                "basis-consistency guard inactive this run", exc,
            )
            ca_registry = None
            splits_by_ticker = {}

    # ── 4. Chunked universe pass ─────────────────────────────────────────────
    # The full-universe Phase 1+2 in one shot holds ~900 ticker histories in
    # memory simultaneously (~180MB peak resident) — on the 2GB t3.small
    # trading instance this co-exists with the daily_append base working set,
    # IB Gateway, daemon, and SSM agent. 2026-05-11 incident: OOM partway
    # through the loop (PROCESS_GONE near ticker SOLS, ~876/900). Chunking
    # caps per-iteration resident memory and gc.collect() between chunks
    # forces release of the cycled DataFrames.
    #
    # Each chunk runs its own Phase 1 (read_batch), Phase 1.5 (per-ticker
    # compute), and Phase 2 (update_batch + write_batch). ArcticDB's native
    # batch parallelism is preserved within each chunk; only the outer
    # universe iteration is chunked. n_ok / n_partial / n_skip / n_err
    # counters accumulate across chunks unchanged.
    n_chunks = (len(stock_tickers) + UNIVERSE_CHUNK_SIZE - 1) // UNIVERSE_CHUNK_SIZE
    for chunk_idx in range(n_chunks):
        chunk_start = chunk_idx * UNIVERSE_CHUNK_SIZE
        chunk_tickers = stock_tickers[chunk_start : chunk_start + UNIVERSE_CHUNK_SIZE]

        # ── 4a. Batch-read this chunk's full universe history ────────────────
        # Replaces the prior per-ticker `universe_lib.read(ticker)` loop. ArcticDB's
        # read_batch parallelizes the underlying S3 round-trips internally, cutting
        # ~900 sequential reads at ~0.3-0.5s each (5-7 minutes wall time) down to a
        # single batched call. The full series (no date_range slice) is required
        # because `_write_row_backfill_safe` rewrites the full symbol on the
        # backfill path, and most MorningEnrich runs hit backfill (target_ts is
        # the prior trading day, already written by post-close DailyData).
        # Missing symbols come back as DataError objects (not exceptions) — they're
        # filtered into n_err with the same semantics as the old per-ticker
        # `try/except Exception` branch.
        hists_by_ticker: dict[str, pd.DataFrame] = {}
        if not dry_run and chunk_tickers:
            t_read0 = time.time()
            read_results = universe_lib.read_batch(
                [ReadRequest(symbol=t) for t in chunk_tickers]
            )
            for ticker, result in zip(chunk_tickers, read_results):
                if isinstance(result, DataError):
                    log.warning(
                        "Ticker %s not in ArcticDB: %s",
                        ticker, result.exception_string,
                    )
                    n_err += 1
                    continue
                hists_by_ticker[ticker] = result.data
            log.info(
                "Chunk %d/%d universe read: %d/%d tickers in %.1fs",
                chunk_idx + 1, n_chunks,
                len(hists_by_ticker), len(chunk_tickers), time.time() - t_read0,
            )

        # ── 4b. Phase 1 — sequential compute pass ────────────────────────────
        # Per-ticker feature compute stays sequential so we don't have to reason
        # about pandas/numpy thread safety on the shared macro series. The
        # bottleneck this PR targets is the I/O-bound write phase below; CPU
        # parallelism is a separate (higher-risk) lever to pull later if needed.
        write_tasks: list[tuple[str, pd.DataFrame, pd.DataFrame, list[str]]] = []
        for ticker in chunk_tickers:
            try:
                # Read recent history from ArcticDB (need ~265 rows for feature warmup)
                if dry_run:
                    n_skip += 1
                    continue

                hist = hists_by_ticker.get(ticker)
                if hist is None:
                    # Ticker was missing from ArcticDB — already counted into
                    # n_err during the batch read above, skip silently.
                    continue

                # Re-running daily_append for the same date MUST overwrite the
                # existing row by default — universe_lib.update() is idempotent
                # for same-date rows, but the 2026-04-17 polygon-label incident
                # showed the path matters: when MorningEnrich's polygon refresh
                # arrives, it must overwrite yfinance's NaN-VWAP row with
                # polygon's true volume-weighted VWAP.
                #
                # ``skip_if_exists`` is the source-aware opt-out: EOD post-market
                # passes True (yfinance, immutable once written), MorningEnrich
                # leaves False (polygon, must overwrite). Without this, an EOD
                # re-run on a day whose row already exists hits the backfill
                # branch in ``_write_row_backfill_safe`` (target_ts ==
                # existing.index.max()) and rewrites the full series per ticker
                # — 904 × ~1.5s blew the 1200s SSM timeout on the 2026-05-01
                # EOD recovery rerun.
                if skip_if_exists and today_ts in hist.index:
                    n_skip += 1
                    continue

                # Build today's OHLCV row
                bar = closes[ticker]
                if np.isnan(bar["Close"]):
                    n_skip += 1
                    continue

                # ── basis-consistency guard (PR4, config#1433) ──────────────
                # Before splicing today's (post-split-scale) row onto this
                # symbol's history, ensure the history is restated for any
                # registered split (restate-then-append, never append onto an
                # un-restated basis). No-op in steady state (morning sync did it).
                if ca_registry is not None and ticker in splits_by_ticker:
                    try:
                        hist = _ensure_history_restated(
                            ticker, hist, splits_by_ticker[ticker],
                            ca_registry, universe_lib, date_str,
                        )
                        hists_by_ticker[ticker] = hist
                    except Exception as exc:
                        # Fail-loud (recorded), but a single symbol's guard
                        # failure must not abort the whole universe pass; the
                        # Saturday backfill's BLOCKING audit remains the gate.
                        log.warning(
                            "daily_append basis-consistency guard failed for %s "
                            "(%s) — appending on the existing basis; Saturday "
                            "backfill audit remains the correctness gate",
                            ticker, exc,
                        )

                # ── splice basis-guard (2026-07-02 incident) ────────────────
                # The incoming row and the stored history can sit on DIFFERENT
                # adjusted bases around a corporate action: polygon serves the
                # pre-action basis until the ex date lands (CRWD 6/30 spliced
                # raw at 763.14 onto a ×0.25-restated series), and an old-basis
                # parquet row can land on a freshly-rebuilt clean series (HON
                # 6/26 = 464.42 onto the new-basis history — the discontinuity
                # that later fed FALSE price evidence to an inverted feed
                # record). Never splice a split-like discontinuity raw: restate
                # the incoming row when a registered action deterministically
                # explains the basis gap, refuse it (loud) otherwise.
                bar, splice_verdict = _splice_basis_guard(
                    ticker, bar, hist, today_ts,
                    splits_by_ticker.get(ticker, []), ca_registry,
                )
                if splice_verdict == "refused":
                    n_quality_blocked += 1
                    quality_blocked_details.append((ticker, "splice_basis_guard"))
                    continue

                new_row_data = {col: bar.get(col, np.nan) for col in OHLCV_COLS}
                # Carry per-row provenance through to the ArcticDB write. Source
                # set by daily_closes.collect (polygon / yfinance / fred); falls
                # through to "unknown" when the staging parquet predates the
                # provenance migration.
                new_row_data[PROVENANCE_COL] = bar.get(PROVENANCE_COL, "unknown")
                new_row = pd.DataFrame(
                    [new_row_data],
                    index=pd.DatetimeIndex([today_ts]),
                )

                # Warmup context — ArcticDB by default; parquet-enriched when the
                # ArcticDB history is too short for full feature warmup.
                #
                # Before this change, short-history tickers (new listings, spinoffs,
                # recent constituent adds) accumulated feature coverage one day at
                # a time — features with 252-day rolling windows stayed NaN for
                # up to a year after the ticker entered ArcticDB, even though the
                # weekly backfill's 10y parquet held the full series. That state
                # routinely caused manual polygon backfills (8 tickers in one day,
                # 2026-04-22) just to unblock downstream consumers.
                #
                # When len(hist) is below the feature-warmup threshold we union
                # the ticker's `predictor/price_cache/{T}.parquet` (full 10y
                # adjusted OHLCV, rebuilt every Saturday by backfill.py) with
                # ArcticDB by date. ArcticDB wins on overlapping dates because
                # daily_append writes there every weekday — it's fresher than a
                # parquet that can be up to 6 days old. Full-history tickers
                # (~99% of the universe on a steady-state day) skip the parquet
                # read entirely.
                #
                # `hist` (the original ArcticDB read) remains authoritative for
                # the write schema (dtype matching via hist.dtypes[col] at the
                # update() call below). Only the feature-compute context is
                # enriched.
                warmup_source = hist
                if len(hist) < MIN_ROWS_FOR_FEATURES:
                    parquet_df = _load_parquet_warmup(s3, bucket, ticker)
                    if parquet_df is None:
                        log.warning(
                            "short-history-no-parquet ticker=%s arctic_rows=%d "
                            "— falling through to NaN-feature degrade",
                            ticker, len(hist),
                        )
                    else:
                        parquet_ohlcv = parquet_df[
                            [c for c in OHLCV_COLS if c in parquet_df.columns]
                        ]
                        arctic_ohlcv = hist[
                            [c for c in OHLCV_COLS if c in hist.columns]
                        ]
                        warmup_source = pd.concat([parquet_ohlcv, arctic_ohlcv])
                        warmup_source = warmup_source[
                            ~warmup_source.index.duplicated(keep="last")
                        ].sort_index()
                        n_parquet_warmup += 1
                        log.info(
                            "parquet-warmup ticker=%s arctic_rows=%d "
                            "parquet_rows=%d stitched_rows=%d",
                            ticker, len(hist), len(parquet_df), len(warmup_source),
                        )

                # Combine warmup OHLCV + today's bar for feature computation.
                # Strip the ``source`` provenance metadata from the row going
                # into ``compute_features`` so that step gets a clean OHLCV
                # frame; the source value is re-attached to today_row below.
                hist_ohlcv = warmup_source[
                    [c for c in OHLCV_COLS if c in warmup_source.columns]
                ]
                new_row_ohlcv = new_row[
                    [c for c in OHLCV_COLS if c in new_row.columns]
                ]
                combined = pd.concat([hist_ohlcv, new_row_ohlcv])
                combined = combined[~combined.index.duplicated(keep="last")].sort_index()

                # Compute features on the combined series. `compute_features`
                # returns rows with NaN for features whose rolling-window
                # warmup exceeds the available history (short-history tickers
                # get ATR-14 computed on ≥14 rows, while 252-day features
                # stay NaN). Rows are never dropped — see 2026-04-21 docstring
                # in features/feature_engineer.py.
                sector_etf_sym = sector_map.get(ticker)
                sector_etf_series = macro.get(sector_etf_sym) if sector_etf_sym else None
                # Sub-sector benchmark ETF (config#934) — resolved the same way
                # as the sector ETF above. sub_sector_etf_map falls back to the
                # sector ETF for unmapped sub-industries, so this is often the
                # SAME series as sector_etf_series (→ sub_sector_vs_benchmark_*
                # == sector_vs_spy_*). None if the ETF's series never loaded
                # (best-effort) → the feature neutral-defaults.
                sub_sector_etf_sym = sub_sector_etf_map.get(ticker)
                sub_sector_etf_series = (
                    macro.get(sub_sector_etf_sym) if sub_sector_etf_sym else None
                )
                ticker_alt = alt_data.get(ticker, {})

                featured = compute_features(
                    combined,
                    spy_series=spy_series,
                    vix_series=vix_series,
                    sector_etf_series=sector_etf_series,
                    sub_sector_etf_series=sub_sector_etf_series,
                    tnx_series=tnx_series,
                    irx_series=irx_series,
                    gld_series=gld_series,
                    uso_series=uso_series,
                    vix3m_series=vix3m_series,
                    hyoas_series=hyoas_series,
                    earnings_data=ticker_alt.get("earnings"),
                    revision_data=ticker_alt.get("revisions"),
                    options_data=ticker_alt.get("options"),
                    fundamental_data=fundamentals.get(ticker),
                )

                if today_ts not in featured.index:
                    # Only possible if combined had a genuine upstream data
                    # issue (today's row disappeared during feature compute).
                    log.warning(
                        "Ticker %s: today_ts missing from featured frame — "
                        "unexpected after compute_features stopped dropping rows",
                        ticker,
                    )
                    n_err += 1
                    continue

                # Extract today's row with OHLCV + every feature that has
                # a column in the featured frame. Features that failed to
                # compute arrive as NaN and are written as NaN — first-class
                # support for partial coverage.
                keep_cols = (
                    [c for c in OHLCV_COLS if c in featured.columns]
                    + [f for f in FEATURES if f in featured.columns]
                )
                today_row = featured.loc[[today_ts], keep_cols].copy()
                # Pull provenance from the new_row directly rather than relying
                # on compute_features to preserve non-OHLCV string columns. Even
                # if compute_features incidentally retains source through its
                # pipeline today, that's an implementation detail of an
                # OHLCV-shaped function — sourcing from new_row keeps the
                # provenance contract decoupled from feature_engineer's column
                # passthrough behaviour.
                today_row[PROVENANCE_COL] = new_row.iloc[0][PROVENANCE_COL]

                # ── L4484: align today_row to the stored schema ──────────────
                # The universe lib is STATIC-schema, so update_batch's descriptor
                # must match the stored symbol's column SET. A FEATURES column
                # the stored series carries but ``compute_features`` does NOT emit
                # — ``factor_momentum_ratio``, a cross-sectional SECOND-PASS
                # column written by features.factor_momentum over the full panel
                # (it cannot be produced per-ticker here) — would otherwise be
                # absent from today_row, tripping StreamDescriptorMismatch on the
                # first daily_append after a backfill that added it. Add any such
                # column as NaN so the descriptor matches; its real go-forward
                # value is filled by the factor-momentum second pass after the
                # write loop. Generalizes _align_schema_for_update (single-row
                # path) to the batch path. Per [[feedback_no_silent_fails]].
                for _stored_col in hist.columns:
                    if _stored_col in FEATURES and _stored_col not in today_row.columns:
                        today_row[_stored_col] = np.nan

                # Column order is enforced by ``to_arctic_canonical`` at
                # the queue site below — no per-site reorder required.

                # Per-ticker coverage observability: count NaN features now
                # so the eventual log + counter reflects exactly what's
                # being written. Silent partial coverage is forbidden
                # (feedback_no_silent_fails). Increment is deferred until
                # after universe_lib.update() so an exception rolls back
                # cleanly into n_err.
                nan_features = [
                    f for f in FEATURES
                    if f in today_row.columns and today_row[f].isna().iloc[0]
                ]

                # Match stored schema dtype per-column. ArcticDB rejects
                # updates whose column dtypes don't match the existing
                # version; stored dtype varies across tickers (some Volume
                # int64, some float64 depending on backfill vintage).
                # hist.dtypes[col] is authoritative by construction.
                # Feature columns that aren't yet in storage default to
                # float32 — matches the predictor training schema.
                #
                # ``source`` is metadata (string), not numeric — skip the
                # dtype-cast fallback path; if it's already in hist the cast
                # picks up the existing string dtype, otherwise it stays
                # object/string (default pandas inference).
                for col in today_row.columns:
                    if col in hist.columns:
                        today_row[col] = today_row[col].astype(hist.dtypes[col])
                    elif col == PROVENANCE_COL:
                        continue  # string metadata; no float32 fallback
                    elif col in FEATURES:
                        today_row[col] = today_row[col].astype("float32")

                today_row.index.name = "date"

                # ── Write-time quality gate ──────────────────────────────
                # Runs after today_row is fully shaped (OHLCV + features +
                # provenance + dtypes aligned) but before the row is queued
                # for batch write. Two outcomes: block (skip queue + log
                # + count) or warn (queue write + log warning + count).
                # DEFAULT_BLOCK_ANOMALY_TYPES blocks only definitely-bad
                # rows (High<Low, Close<=0); operators can upgrade types
                # via the DAILY_APPEND_BLOCK_ANOMALY_TYPES env var.
                #
                # Per-ticker block lines log at WARNING; the load-bearing
                # Flow Doctor surface is the single aggregated run-level
                # record emitted after the chunk loop (one systemic event
                # → one alert, not one per ticker — see the 2026-06-11
                # EOD storm note there).
                qg = validate_today_row(today_row, hist, ticker)
                blocking_anomalies = [
                    a for a in qg["anomalies"] if a["type"] in block_anomaly_types
                ]
                if blocking_anomalies:
                    for a in blocking_anomalies:
                        log.warning(
                            "Quality gate BLOCK %s.%s: %s",
                            ticker, a["type"], a["detail"],
                        )
                        quality_counts_by_type[a["type"]] = (
                            quality_counts_by_type.get(a["type"], 0) + 1
                        )
                        quality_blocked_details.append((ticker, a["type"]))
                    n_quality_blocked += 1
                    continue  # do not queue this row for write
                if qg["anomalies"]:
                    # All anomalies present but none blocking — warn-only.
                    for a in qg["anomalies"]:
                        log.warning(
                            "Quality gate WARN %s.%s: %s",
                            ticker, a["type"], a["detail"],
                        )
                        quality_counts_by_type[a["type"]] = (
                            quality_counts_by_type.get(a["type"], 0) + 1
                        )
                    n_quality_warned += 1

                # ── L2 series-contract gate (alpha-engine-config#2456) ────
                # Supplements validate_today_row above with the three checks
                # it doesn't cover: calendar-aware continuity (vs.
                # validate_parquet's naive calendar-DAY gap heuristic),
                # vol-scaled outlier (vs. validate_today_row's fixed
                # MAX_DAILY_RETURN=0.50), and calendar-monotonic (no
                # existing equivalent). schema/sanity also run here for
                # parity with the shared-lib module's full six-gate
                # contract, even though price_validator's OHLC/negative-
                # close checks above already cover this repo's write path
                # for those two. Runs against hist+today_row combined so
                # continuity/outlier/monotonic see real trailing history,
                # not just the single new row.
                #
                # today_row's date REPLACES any existing hist row at the
                # same date rather than being concatenated alongside it —
                # the MorningEnrich overwrite contract (skip_if_exists=
                # False, the default) intentionally re-writes today's row
                # when it's already in hist (polygon settling yfinance's
                # NaN-VWAP placeholder); that legitimate overwrite must not
                # look like a duplicate-date corruption to
                # calendar_monotonic. drop(..., errors="ignore") is a
                # no-op on the common append-at-head case (today_ts not
                # yet in hist.index).
                if not hist.empty:
                    l2_series = pd.concat(
                        [hist.drop(index=today_row.index, errors="ignore"), today_row]
                    ).sort_index()
                else:
                    l2_series = today_row
                l2_report = _l2_validate_series(
                    l2_series, ticker, as_of=today_ts.date(),
                )
                l2_decision = _l2_quarantine_decision(
                    l2_report, block_gates=l2_block_gates,
                )
                if l2_decision.alarm:
                    for r in l2_report.failing:
                        log.warning(
                            "L2 series-contract %s %s.%s: %s",
                            "QUARANTINE" if r.gate in l2_decision.blocking_gates
                            else "WARN",
                            ticker, r.gate, r.reason,
                        )
                        l2_counts_by_gate[r.gate] = l2_counts_by_gate.get(r.gate, 0) + 1
                    if l2_decision.quarantine:
                        for gate_name in l2_decision.blocking_gates:
                            l2_quarantined_details.append((ticker, gate_name))
                        n_l2_quarantined += 1
                        continue  # do not queue this row for write
                    n_l2_alarmed += 1

                # Defer the actual ArcticDB write — collected here so Phase 2
                # can run them in parallel via a thread pool. The previous
                # sequential per-ticker `_write_row_backfill_safe` call took
                # ~300-400ms × 900 = ~5 minutes wall time, the residual half
                # of the budget after the read_batch optimization in PR #99.
                write_tasks.append((ticker, today_row, hist, nan_features))

            except Exception as exc:
                log.warning("Failed to compute %s: %s", ticker, exc)
                n_err += 1

        # ── 4c. Phase 2 — bulk writes via ArcticDB batch API ─────────────────────
        # 2026-05-05: replaced ThreadPoolExecutor + per-symbol lib.update() loop
        # with `update_batch` + `write_batch`. PR #152's per-task timing
        # instrumentation measured the prior threadpool achieving no parallelism
        # in practice — wall ≈ 900 × 1.7s/ticker (2026-05-05 MorningEnrich
        # incident, 1535s for 900 tickers, workers=16, hit the 30-min SSM cap).
        # Phase 1's `read_batch` runs at 84ms/ticker against the same library,
        # so the ArcticDB native parallelism is the right primitive — and is
        # documented as such ("perform an update operation on a list of symbols
        # in parallel"). Same shift PR #99 made for reads. The #152
        # instrumentation becomes obsolete with this refactor (no per-task
        # threadpool to time) and is removed in this PR.
        #
        # Path split: append-at-head (target_ts > existing.index.max(), the
        # common morning-enrich case) → UpdatePayload + update_batch.
        # Backfill (target_ts in middle of series, rare — historical VWAP
        # repair etc.) → splice + WritePayload + write_batch. Mirrors
        # `_write_row_backfill_safe`'s branching for the per-symbol path
        # (which still serves macro_lib's small N=7-11 sequential writes).
        update_payloads: list[UpdatePayload] = []
        write_payloads: list[WritePayload] = []
        payload_meta: dict[str, tuple[list[str] | None, int]] = {}  # ticker → (nan_features, hist_rows)

        for ticker, today_row, hist, nan_features in write_tasks:
            target_ts = today_row.index[0]
            payload_meta[ticker] = (nan_features, len(hist))
            if hist.empty or target_ts > hist.index.max():
                # Append at head — fast path. update_batch with upsert=True
                # also handles the rare "symbol doesn't exist yet" case
                # (replaces _write_row_backfill_safe's lib.write fallback).
                # ``to_arctic_canonical`` enforces the
                # ``OHLCV + source + FEATURES`` column order AND strips
                # Categorical ``source`` dtype (PR #211) — single
                # chokepoint for both descriptor-match invariants.
                update_payloads.append(
                    UpdatePayload(symbol=ticker, data=to_arctic_canonical(today_row))
                )
            else:
                # Backfill — splice into existing series, full rewrite. Same
                # logic as _write_row_backfill_safe's backfill branch.
                #
                # ``pd.concat``'s default outer-join preserves ``hist``'s
                # column order and appends any new columns from
                # ``today_row`` at the end. ``to_arctic_canonical`` below
                # re-projects to ``OHLCV + source + FEATURES`` before the
                # write so the persisted descriptor stays canonical and a
                # subsequent same-or-later-date UpdatePayload doesn't trip
                # ArcticDB's StreamDescriptorMismatch (2026-05-21 EOD).
                combined = pd.concat([hist, today_row])
                combined = combined[~combined.index.duplicated(keep="last")].sort_index()
                write_payloads.append(
                    WritePayload(symbol=ticker, data=to_arctic_canonical(combined))
                )

        if update_payloads or write_payloads:
            t_write0 = time.time()
            # A per-symbol schema mismatch comes back as a DataError in the
            # results list (handled in _aggregate → n_err). A BATCH-LEVEL
            # descriptor mismatch — the 2026-05-21 EOD incident, where the
            # whole update_batch aborts before producing per-symbol results —
            # RAISES StreamDescriptorMismatch out of the call. Count that here,
            # emit the incident (the wrapper fires _emit_schema_drift before the
            # exception escapes), then re-raise (fail-loud): a batch-level
            # descriptor mismatch is a real data-integrity failure, not a
            # per-ticker hiccup.
            with _count_schema_drift(n_schema_drift, on_drift=_emit_schema_drift):
                update_results = (
                    universe_lib.update_batch(update_payloads, upsert=True)
                    if update_payloads else []
                )
                write_results = (
                    universe_lib.write_batch(write_payloads, prune_previous_versions=True)
                    if write_payloads else []
                )
            write_wall = time.time() - t_write0
            log.info(
                "Batch writes: %d updates + %d backfills in %.1fs",
                len(update_payloads), len(write_payloads), write_wall,
            )

            # Iterate results — ArcticDB returns DataError per failed symbol
            # rather than raising, so explicit per-symbol error detection is
            # required. Pair each result with the originating payload's symbol
            # via positional alignment (ArcticDB guarantees i-th result
            # corresponds to i-th payload).
            def _aggregate(payloads, results, label: str):
                nonlocal n_ok, n_err, n_partial
                for payload, result in zip(payloads, results):
                    ticker = payload.symbol
                    if isinstance(result, DataError):
                        log.warning(
                            "Failed to %s %s: %s (code=%s, category=%s)",
                            label, ticker, result.exception_string,
                            result.error_code, result.error_category,
                        )
                        n_err += 1
                        continue
                    nan_features, hist_rows = payload_meta[ticker]
                    if nan_features:
                        log.warning(
                            "partial-features ticker=%s rows=%d nan=%d/%d features=%s",
                            ticker, hist_rows, len(nan_features), len(FEATURES),
                            nan_features,
                        )
                        n_partial += 1
                    else:
                        n_ok += 1

            _aggregate(update_payloads, update_results, "update")
            _aggregate(write_payloads, write_results, "backfill")

        # Free chunk-resident memory before the next iteration. Without
        # this, ~150 ticker hist DataFrames + write_payloads.combined
        # frames stay reachable in the chunk loop's frame scope and
        # accumulate across iterations. gc.collect() is load-bearing
        # because pandas' BlockManager holds reference cycles that
        # del-alone can't break.
        del hists_by_ticker, write_tasks, update_payloads, write_payloads
        gc.collect()

    # ── Aggregated quality-gate alert (one per run, not one per ticker) ──
    # 2026-06-11: the EOD run blocked 10 tickers' unsettled bars and fanned
    # out 10 Flow Doctor alert emails + auto-filed issues (hitting
    # max_alerts_per_day) for ONE systemic provider-settlement event. The
    # per-ticker BLOCK lines above stay at WARNING for log forensics; this
    # single record is what crosses the logging boundary into Flow Doctor
    # (only ERROR-level records do, per
    # feedback_collector_return_dict_invisible_to_flow_doctor).
    #
    # EOD severity carve-out: on the post-market path (skip_if_exists=True,
    # minutes after the close) an intrabar_inconsistent block is the
    # EXPECTED provider artifact — the closing-auction print lands in Close
    # before the High/Low aggregates settle (NVR/ADC/HPE/… daily, all
    # sub-1% gaps). The heal is structural: next morning's MorningEnrich
    # overwrites with settled polygon data, and the universe-freshness scan
    # hard-raises at >3 trading days as backstop. So an all-intrabar EOD
    # block set logs the summary at WARNING (recorded surface: per-ticker
    # WARN lines + CW quality metrics + result counters — no alert). Any
    # other anomaly type, or ANY block on the settled MorningEnrich path,
    # is a real data-quality event → single ERROR → one alert.
    if n_quality_blocked:
        blocked_types = {atype for _, atype in quality_blocked_details}
        detail_list = ", ".join(
            f"{tkr}.{atype}" for tkr, atype in quality_blocked_details[:20]
        )
        if len(quality_blocked_details) > 20:
            detail_list += f", … +{len(quality_blocked_details) - 20} more"
        if skip_if_exists and blocked_types == {ANOMALY_INTRABAR_INCONSISTENT}:
            log.warning(
                "Quality gate blocked %d row(s) this run — expected EOD "
                "unsettled-bar artifact (close-auction print ahead of "
                "High/Low settlement); rows heal via next MorningEnrich "
                "overwrite: %s",
                n_quality_blocked, detail_list,
            )
        else:
            log.error(
                "Quality gate blocked %d row(s) this run (types=%s): %s",
                n_quality_blocked, sorted(blocked_types), detail_list,
            )

    # ── Aggregated L2 series-contract alert (one per run, not one per
    # ticker) — mirrors the validate_today_row aggregation immediately
    # above, same rationale (2026-06-11 EOD alert-storm note there): a
    # systemic event (e.g. a shared-upstream gap that hits every S&P
    # constituent) must fan out as ONE alert, not one per ticker.
    # Quarantine (block-gate failures) is the load-bearing signal → ERROR.
    # Non-quarantining alarms (warn-gate failures — staleness/continuity/
    # outlier by default) still page per the issue's "quarantine + alarm,
    # do not sit silent" requirement, but at WARNING severity — these can
    # legitimately arise from an operational gap or a real market event,
    # not just corruption, so they don't carry the same urgency as a
    # quarantined row.
    if n_l2_quarantined:
        quarantined_gates = {g for _, g in l2_quarantined_details}
        l2_detail_list = ", ".join(
            f"{tkr}.{g}" for tkr, g in l2_quarantined_details[:20]
        )
        if len(l2_quarantined_details) > 20:
            l2_detail_list += f", … +{len(l2_quarantined_details) - 20} more"
        log.error(
            "L2 series-contract quarantined %d row(s) this run (gates=%s): %s",
            n_l2_quarantined, sorted(quarantined_gates), l2_detail_list,
        )
    elif n_l2_alarmed:
        log.warning(
            "L2 series-contract flagged %d row(s) this run (non-quarantining; "
            "gate_counts=%s)",
            n_l2_alarmed, dict(l2_counts_by_gate),
        )

    t_total = time.time() - t0

    result = {
        "status": "ok",
        # config#2685: always True on a normal return — the only raises
        # between here and `return result` (error-rate gate, freshness
        # scan) either abort the function (target date write genuinely
        # failed) or, for an UNRELATED stale symbol elsewhere in the
        # universe, raise UniverseFreshnessViolation instead of returning
        # — see that class + _self_heal_missing_universe_days for the
        # caller-side distinction this field exists to support.
        "target_date_write_ok": True,
        "date": date_str,
        "tickers_appended": n_ok,
        "tickers_partial": n_partial,
        "tickers_skipped": n_skip,
        "tickers_errored": n_err,
        "tickers_parquet_warmup": n_parquet_warmup,
        "tickers_missing_from_closes": n_missing_from_closes,
        "tickers_quality_blocked": n_quality_blocked,
        "tickers_quality_warned": n_quality_warned,
        "quality_anomaly_counts": dict(quality_counts_by_type),
        "quality_block_anomaly_types": sorted(block_anomaly_types),
        "tickers_l2_quarantined": n_l2_quarantined,
        "tickers_l2_alarmed": n_l2_alarmed,
        "l2_gate_counts": dict(l2_counts_by_gate),
        "l2_block_gates": sorted(l2_block_gates),
        "schema_drift_incidents": n_schema_drift[0],
        "load_seconds": round(t_load, 1),
        "total_seconds": round(t_total, 1),
        "dry_run": dry_run,
    }

    log.info(
        "ArcticDB daily_append: stocks n_ok=%d n_partial=%d n_skip=%d n_err=%d "
        "n_parquet_warmup=%d n_missing_from_closes=%d (of %d) "
        "quality_blocked=%d quality_warned=%d anomaly_counts=%s "
        "l2_quarantined=%d l2_alarmed=%d l2_gate_counts=%s | "
        "macro_updated=%d sector_updated=%d | %.1fs total",
        n_ok, n_partial, n_skip, n_err, n_parquet_warmup,
        n_missing_from_closes, len(stock_tickers),
        n_quality_blocked, n_quality_warned, dict(quality_counts_by_type),
        n_l2_quarantined, n_l2_alarmed, dict(l2_counts_by_gate),
        len(macro_updated) if not dry_run else 0,
        len(sector_updated) if not dry_run else 0,
        t_total,
    )

    if not dry_run:
        _emit_quality_gate_metrics(
            quality_counts_by_type, n_quality_blocked, n_quality_warned,
        )
        # Clean-run schema-drift emit (count 0, or any non-fatal residual). On a
        # schema-drift ABORT this line is never reached — the _count_schema_drift
        # wrapper already emitted the incident before re-raising — so the count
        # always lands exactly once, on both the clean and the fail-loud path.
        _emit_schema_drift(n_schema_drift[0])

    # Hard-fail on high error rate. ``n_ok == 0`` alone is NOT a failure
    # signal — it correctly occurs when every ticker hit the
    # "today already in ArcticDB" skip path (a second same-day invocation,
    # or a Step Function retry that runs after the first one succeeded).
    # The real silent-fail we're guarding against (ArcticDB-wide auth /
    # connectivity failure making every read throw) now registers as
    # ``n_err`` rather than ``n_skip`` after PR #24, so the 5% error-rate
    # threshold catches it without false positives on no-op reruns.
    # dry_run is exempt because it short-circuits the per-ticker loop.
    if not dry_run:
        err_rate = n_err / max(len(stock_tickers), 1)
        if err_rate > 0.05:
            raise RuntimeError(
                f"ArcticDB daily_append error rate {err_rate:.1%} exceeds 5% threshold "
                f"(n_ok={n_ok} n_err={n_err} of {len(stock_tickers)}) — treating as pipeline failure"
            )

        # ArcticDB feature-store freshness sentinel (config#1787). Written
        # here — right after the error-rate gate confirms this was a
        # successful ArcticDB write batch, and BEFORE the staleness-scan
        # receipt below (which hard-raises on stale symbols) — so the
        # sentinel reflects "a write happened" independent of whether the
        # separate per-symbol staleness scan later passes or raises.
        # Best-effort/non-fatal by construction (see function docstring).
        _write_feature_store_freshness_sentinel(s3, bucket, library="universe")

        # ── L4484: factor-momentum daily go-forward second pass ──────────────
        # factor_momentum_ratio is a cross-sectional-time-series feature that
        # can't be produced per-ticker in the loop above (it ranks the WHOLE
        # cross-section + builds factor-return portfolios). Now that today's
        # OHLCV+loadings rows are written, recompute the latest date's value
        # over a slim trailing panel and update it in place. Best-effort +
        # OBSERVE: the function never raises; gate off via the env var if it
        # ever misbehaves. Skipped on dry_run (no writes happened).
        if os.environ.get("FACTOR_MOMENTUM_DAILY_ENABLED", "true").lower() != "false":
            try:
                fm_result = update_factor_momentum_latest(
                    universe_lib, stock_tickers, today_ts,
                    canonical_fn=to_arctic_canonical,
                )
                log.info("Factor-momentum daily update: %s", json.dumps(fm_result, default=str))
            except Exception as exc:  # belt-and-suspenders — never fail the daily pipeline
                log.warning("Factor-momentum daily update FAILED (OBSERVE, non-fatal): %s", exc)

        # ── C.1: factor-loading z-score daily go-forward second pass ─────────
        # The 9 *_zscore Barra loadings (C.3 / predictor risk_model_persist)
        # are cross-sectional — same structural gap as factor_momentum_ratio.
        # S3 feature store already runs apply_factor_zscores in compute.py;
        # this pass keeps ArcticDB (predictor training + C.2b F+D persistence)
        # in sync. Best-effort + gated; never fails the daily pipeline.
        if os.environ.get("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "true").lower() != "false":
            try:
                from features.cross_sectional import update_factor_loading_zscores_latest
                flz_result = update_factor_loading_zscores_latest(
                    universe_lib, stock_tickers, today_ts,
                    canonical_fn=to_arctic_canonical,
                )
                log.info(
                    "Factor-loading z-score daily update: %s",
                    json.dumps(flz_result, default=str),
                )
            except Exception as exc:
                log.warning(
                    "Factor-loading z-score daily update FAILED (non-fatal): %s", exc,
                )

        # Producer-side post-write validation. Catches the partial-write
        # class (2026-04-21 ASGN/MOH) that the per-ticker error-rate gate
        # above misses — symbols not in today's batch but stale from
        # earlier silent skips. Emits health/universe_freshness.json so
        # consumers don't repeat this 200s scan on every Lambda
        # invocation (the cause of the 2026-05-01 SF timeout cascade).
        receipt = _scan_universe_and_emit_freshness_receipt(
            s3, bucket, universe_lib,
            expected_tickers=expected_tickers,
        )
        result["universe_freshness_receipt"] = {
            "n_symbols_checked": receipt["n_symbols_checked"],
            "stalest_symbol": receipt["stalest_symbol"],
            "stalest_age_trading_days": receipt["stalest_age_trading_days"],
            "scan_seconds": receipt["scan_seconds"],
        }

    return result


def main():
    parser = argparse.ArgumentParser(description="Append daily features to ArcticDB universe")
    parser.add_argument("--date", default=None, help="Target date (YYYY-MM-DD, default: today UTC)")
    parser.add_argument("--dry-run", action="store_true", help="Compute but skip ArcticDB writes")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help=f"S3 bucket (default: {DEFAULT_BUCKET})")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help=(
            "Skip tickers whose target-date row is already in ArcticDB. "
            "Use for EOD post-market re-runs (yfinance, immutable). Leave "
            "off for MorningEnrich runs (polygon must overwrite)."
        ),
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    result = daily_append(
        # config#1014: pass args.date through; daily_append() defaults a None
        # date_str to the trading-day axis via dates.default_run_date().
        date_str=args.date,
        bucket=args.bucket,
        dry_run=args.dry_run,
        skip_if_exists=args.skip_if_exists,
    )

    if result["status"] != "ok":
        log.error("Daily append failed: %s", result.get("error"))
        sys.exit(1)

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
