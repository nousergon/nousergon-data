"""
collectors/signal_returns.py — Seed and backfill signal performance tables.

Seeds score_performance (BUY signal entry prices) and predictor_outcomes
(predictor prediction records) from S3 artifacts. Backfills forward returns
by JOINing against universe_returns (already populated by the universe_returns
collector) — no yfinance or external API calls needed.

Must run AFTER universe_returns in the Phase 1 pipeline.

Target tables in research.db:
  - score_performance: entry prices + 5d/10d/30d forward returns for BUY signals
  - predictor_outcomes: prediction records + log-domain canonical alpha
    at the configured horizon (`forward_days`, default 21d post Track A
    cutover). New horizon-agnostic columns: actual_log_alpha, horizon_days,
    correct. Legacy columns (actual_5d_return, correct_5d) dual-written
    during the transition window for backtester COALESCE fallback.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Default prediction horizon. Matches the predictor's canonical training
# target (alpha-engine-predictor #114 Track A cutover, 2026-05-09). Should be
# config-driven once weekly_collector exposes a `signal_returns.forward_days`
# YAML setting; until then this constant is the source of truth and the
# `forward_days` parameter on `collect` lets call sites override.
_DEFAULT_FORWARD_DAYS = 21


def collect(
    bucket: str,
    db_path: str,
    signals_prefix: str = "signals",
    dry_run: bool = False,
    forward_days: int = _DEFAULT_FORWARD_DAYS,
) -> dict:
    """Seed and backfill signal performance tables in research.db.

    Steps:
      1. Seed score_performance from S3 signals (entry prices from universe_returns)
      2. Backfill score_performance returns from universe_returns JOIN
      3. Seed predictor_outcomes from S3 predictions
      4. Backfill predictor_outcomes returns from universe_returns JOIN

    Args:
        forward_days: prediction horizon in trading days. Drives which
            universe_returns columns are read (return_{N}d / log_return_{N}d)
            and which value is recorded in `predictor_outcomes.horizon_days`.
            Defaults to the predictor's current canonical 21d. Schema columns
            for the configured horizon must exist in `universe_returns` —
            21d arithmetic + log columns added by alpha-engine-data PR
            #197; other horizons require new schema columns first.

    Returns dict with status, counts for each step.
    """
    s3 = boto3.client("s3")
    results = {}

    # Step 1: Seed score_performance
    results["seed_score_performance"] = _seed_score_performance(
        s3, bucket, db_path, signals_prefix, dry_run,
    )

    # Step 1b: Backfill calibrator-v1 context for any legacy rows whose
    # canonical columns are NULL (rows seeded before this collector
    # learned to write them). UPDATE-WHERE-NULL so re-runs are no-ops.
    results["backfill_score_context"] = _backfill_score_context(
        s3, bucket, db_path, signals_prefix, dry_run,
    )

    # Step 2: Backfill score_performance returns via universe_returns JOIN
    results["backfill_score_returns"] = _backfill_score_returns(db_path, dry_run)

    # Step 2b: Drift gate — emit canonical-context coverage as a CW gauge
    # so an alarm fires if the producer ever regresses (e.g. signals.json
    # shape drift, seed-path bug, schema migration skew). Closes the loop
    # on the 2026-05-09 producer-side bug class.
    if not dry_run:
        results["context_coverage_drift"] = _emit_context_coverage_metric(db_path)

    # Step 3: Seed predictor_outcomes
    results["seed_predictor_outcomes"] = _seed_predictor_outcomes(
        s3, bucket, db_path, dry_run,
    )

    # Step 4: Backfill predictor_outcomes via universe_returns JOIN
    results["backfill_predictor_returns"] = _backfill_predictor_returns(
        db_path, dry_run, forward_days=forward_days,
    )

    # Upload updated research.db back to S3
    total_written = sum(r.get("rows_written", 0) for r in results.values())
    if not dry_run and total_written > 0:
        try:
            s3.upload_file(db_path, bucket, "research.db")
            logger.info("Uploaded research.db to s3://%s/research.db", bucket)
        except Exception as e:
            logger.warning("Failed to upload research.db: %s", e)

    has_errors = any(r.get("status") == "error" for r in results.values())
    return {
        "status": "partial" if has_errors else ("ok_dry_run" if dry_run else "ok"),
        "total_written": total_written,
        **results,
    }


# ── Step 1: Seed score_performance ────────────────────────────────────────────


def _extract_signal_context(payload: dict, ticker: str) -> dict:
    """Pull calibrator-v1 context fields for a ticker from a signals.json
    payload. Mirrors the extraction logic in alpha-engine-research's
    scripts/backfill_calibrator_v1_context.py so the producer-side seed
    and the legacy backfill agree on every field's source.

    Returns a dict with all 5 keys; values may be None when the source
    payload omits them (older schemas, partial outputs).
    """
    sigs = payload.get("signals") or {}
    sig = sigs.get(ticker) or {}
    sector = sig.get("sector")
    sector_modifiers = payload.get("sector_modifiers") or {}
    return {
        "quant_score": sig.get("quant_score"),
        "qual_score": sig.get("qual_score"),
        "conviction": sig.get("conviction"),
        "sector_modifier": sector_modifiers.get(sector) if sector else None,
        "market_regime": payload.get("market_regime"),
    }


def _seed_score_performance(
    s3, bucket: str, db_path: str, signals_prefix: str, dry_run: bool,
) -> dict:
    """Insert BUY-rated signals into score_performance with entry prices
    from universe_returns. Includes the 5 calibrator-v1 context columns
    (quant_score, qual_score, conviction, sector_modifier, market_regime)
    on initial INSERT — values come from the same signals.json payload
    that drives the BUY filter, so no second round-trip is needed.

    Pre-2026-05-10 seed inserts wrote only (symbol, score_date, score,
    price_on_date) and left the canonical columns NULL — that bug is the
    root cause behind the 2026-05-09 evaluator weight_optimizer ERROR.
    Existing NULL rows get repaired by ``_backfill_score_context``.
    """
    try:
        conn = sqlite3.connect(db_path)
        _ensure_score_performance_schema(conn)

        existing = {
            (r[0], r[1]) for r in
            conn.execute("SELECT symbol, score_date FROM score_performance").fetchall()
        }

        # List signal dates from S3
        signal_dates = _list_signal_dates(s3, bucket, signals_prefix)

        # rows_to_insert carries (ticker, sig_date, score, context_dict) so
        # the canonical context is captured at the same point the BUY
        # filter runs — single source of truth per signals.json payload.
        rows_to_insert: list[tuple[str, str, float, dict]] = []
        for sig_date in signal_dates:
            try:
                obj = s3.get_object(Bucket=bucket, Key=f"{signals_prefix}/{sig_date}/signals.json")
                signals = json.loads(obj["Body"].read())
            except (ClientError, json.JSONDecodeError):
                continue

            for stock in signals.get("universe", []):
                ticker = stock.get("ticker")
                score = stock.get("score", 0)
                rating = stock.get("rating", "")
                if not ticker or rating != "BUY" or (ticker, sig_date) in existing:
                    continue
                rows_to_insert.append(
                    (ticker, sig_date, score, _extract_signal_context(signals, ticker))
                )

            # v1 format fallback
            sigs = signals.get("signals", {})
            if isinstance(sigs, dict):
                for ticker, s in sigs.items():
                    score = s.get("score", 0)
                    rating = s.get("rating", "")
                    if rating != "BUY" or (ticker, sig_date) in existing:
                        continue
                    rows_to_insert.append(
                        (ticker, sig_date, score, _extract_signal_context(signals, ticker))
                    )

        if not rows_to_insert:
            conn.close()
            return {"status": "ok", "rows_written": 0, "note": "all rows already seeded"}

        # Get entry prices from universe_returns (already in the DB)
        inserted = 0
        for ticker, sig_date, score, ctx in rows_to_insert:
            if score is None:
                continue
            # Look up entry price from universe_returns
            row = conn.execute(
                "SELECT close_price FROM universe_returns WHERE ticker = ? AND eval_date = ?",
                (ticker, sig_date),
            ).fetchone()
            price = row[0] if row else None
            if price is None:
                continue

            if not dry_run:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO score_performance (
                        symbol, score_date, score, price_on_date,
                        quant_score, qual_score, conviction,
                        sector_modifier, market_regime
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker, sig_date,
                        round(float(score), 2), round(price, 2),
                        ctx["quant_score"], ctx["qual_score"],
                        ctx["conviction"], ctx["sector_modifier"],
                        ctx["market_regime"],
                    ),
                )
            inserted += 1

        if not dry_run:
            conn.commit()
        conn.close()

        if inserted:
            logger.info("Seeded %d score_performance rows from %d signal dates", inserted, len(signal_dates))
        return {"status": "ok", "rows_written": inserted}

    except Exception as e:
        logger.error("seed_score_performance failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


def _backfill_score_context(
    s3, bucket: str, db_path: str, signals_prefix: str, dry_run: bool,
) -> dict:
    """Repair existing score_performance rows whose calibrator-v1 context
    columns are NULL (rows seeded before _seed_score_performance learned
    to write them).

    Groups NULL-bearing rows by score_date so each signals.json fetch
    serves all symbols for that date. UPDATE-WHERE-NULL means re-runs are
    no-ops once the population converges.
    """
    try:
        conn = sqlite3.connect(db_path)
        _ensure_score_performance_schema(conn)

        rows = conn.execute(
            """
            SELECT symbol, score_date
            FROM score_performance
            WHERE quant_score IS NULL
               OR qual_score IS NULL
               OR conviction IS NULL
               OR sector_modifier IS NULL
               OR market_regime IS NULL
            ORDER BY score_date, symbol
            """
        ).fetchall()

        if not rows:
            conn.close()
            return {"status": "ok", "rows_written": 0, "note": "no NULL context rows"}

        by_date: dict[str, list[str]] = {}
        for symbol, score_date in rows:
            by_date.setdefault(score_date, []).append(symbol)

        updated = 0
        for sig_date, symbols in by_date.items():
            try:
                obj = s3.get_object(
                    Bucket=bucket,
                    Key=f"{signals_prefix}/{sig_date}/signals.json",
                )
                payload = json.loads(obj["Body"].read())
            except (ClientError, json.JSONDecodeError):
                continue

            for symbol in symbols:
                ctx = _extract_signal_context(payload, symbol)
                # Skip if signals.json has no useful context for this symbol
                # (rare — typically only legacy archive shapes).
                if all(v is None for v in ctx.values()):
                    continue

                cur = conn.execute(
                    """
                    SELECT quant_score, qual_score, conviction,
                           sector_modifier, market_regime
                    FROM score_performance
                    WHERE symbol = ? AND score_date = ?
                    """,
                    (symbol, sig_date),
                ).fetchone()
                if cur is None:
                    continue

                cur_q, cur_qu, cur_c, cur_s, cur_r = cur
                updates: list[tuple[str, object]] = []
                if cur_q is None and ctx["quant_score"] is not None:
                    updates.append(("quant_score", ctx["quant_score"]))
                if cur_qu is None and ctx["qual_score"] is not None:
                    updates.append(("qual_score", ctx["qual_score"]))
                if cur_c is None and ctx["conviction"] is not None:
                    updates.append(("conviction", ctx["conviction"]))
                if cur_s is None and ctx["sector_modifier"] is not None:
                    updates.append(("sector_modifier", ctx["sector_modifier"]))
                if cur_r is None and ctx["market_regime"] is not None:
                    updates.append(("market_regime", ctx["market_regime"]))

                if not updates:
                    continue
                if dry_run:
                    updated += 1
                    continue

                set_clause = ", ".join(f"{col} = ?" for col, _ in updates)
                values = [v for _, v in updates] + [symbol, sig_date]
                conn.execute(
                    f"UPDATE score_performance SET {set_clause} "
                    f"WHERE symbol = ? AND score_date = ?",
                    values,
                )
                updated += 1

        if not dry_run:
            conn.commit()
        conn.close()

        if updated:
            logger.info(
                "Backfilled calibrator-v1 context on %d score_performance rows",
                updated,
            )
        return {"status": "ok", "rows_written": updated}

    except Exception as e:
        logger.error("backfill_score_context failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Step 2: Backfill score_performance returns ────────────────────────────────


def _backfill_score_returns(db_path: str, dry_run: bool) -> dict:
    """Backfill 5d/10d/30d returns in score_performance by JOINing universe_returns."""
    try:
        conn = sqlite3.connect(db_path)
        _ensure_score_performance_schema(conn)

        updated = 0
        for horizon in ("5d", "10d", "30d"):
            bdays = {"5d": 5, "10d": 10, "30d": 30}[horizon]

            # Find score_performance rows missing this horizon's return
            pending = pd.read_sql_query(
                f"SELECT symbol, score_date, price_on_date FROM score_performance WHERE return_{horizon} IS NULL",
                conn,
            )
            if pending.empty:
                continue

            for _, row in pending.iterrows():
                ticker = row["symbol"]
                score_date = row["score_date"]
                entry_price = row["price_on_date"]
                if entry_price is None:
                    continue

                # Look up forward return from universe_returns
                ur = conn.execute(
                    f"SELECT return_{horizon}, spy_return_{horizon}, beat_spy_{horizon} FROM universe_returns WHERE ticker = ? AND eval_date = ?",
                    (ticker, score_date),
                ).fetchone()

                if ur is None or ur[0] is None:
                    continue

                stock_return = ur[0]  # already as decimal (e.g., 0.05)
                spy_return = ur[1]
                beat_spy = ur[2]
                exit_price = round(entry_price * (1 + stock_return), 2)

                if not dry_run:
                    conn.execute(
                        f"UPDATE score_performance SET price_{horizon}=?, return_{horizon}=?, spy_{horizon}_return=?, beat_spy_{horizon}=? WHERE symbol=? AND score_date=? AND return_{horizon} IS NULL",
                        (
                            exit_price,
                            round(stock_return * 100, 2),  # stored as percentage
                            round(spy_return * 100, 2) if spy_return is not None else None,
                            beat_spy,
                            ticker, score_date,
                        ),
                    )
                updated += 1

        # Repair: fix beat_spy columns where return exists but beat_spy is NULL
        for horizon in ("5d", "10d", "30d"):
            repaired = conn.execute(
                f"UPDATE score_performance SET beat_spy_{horizon} = CASE WHEN return_{horizon} > spy_{horizon}_return THEN 1 ELSE 0 END WHERE return_{horizon} IS NOT NULL AND spy_{horizon}_return IS NOT NULL AND beat_spy_{horizon} IS NULL",
            ).rowcount
            if repaired:
                logger.info("Repaired %d beat_spy_%s values", repaired, horizon)

        if not dry_run:
            conn.commit()
        conn.close()

        if updated:
            logger.info("Backfilled %d score_performance returns via universe_returns JOIN", updated)
        return {"status": "ok", "rows_written": updated}

    except Exception as e:
        logger.error("backfill_score_returns failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Step 3: Seed predictor_outcomes ───────────────────────────────────────────


def _seed_predictor_outcomes(s3, bucket: str, db_path: str, dry_run: bool) -> dict:
    """Seed predictor_outcomes from S3 predictions/*.json files."""
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="predictor/predictions/", Delimiter="/")
        keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".json") and "latest" not in obj["Key"]]

        if not keys:
            return {"status": "ok", "rows_written": 0, "note": "no prediction files in S3"}

        conn = sqlite3.connect(db_path)
        existing = {
            (r[0], r[1]) for r in
            conn.execute("SELECT symbol, prediction_date FROM predictor_outcomes").fetchall()
        }

        inserted = 0
        for key in keys:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(obj["Body"].read())
                pred_date = data.get("date") or key.split("/")[-1].replace(".json", "")
                for p in data.get("predictions", []):
                    ticker = p.get("ticker")
                    if not ticker or (ticker, pred_date) in existing:
                        continue
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO predictor_outcomes (symbol, prediction_date, predicted_direction, prediction_confidence, p_up, p_flat, p_down, score_modifier_applied) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (ticker, pred_date, p.get("predicted_direction"), p.get("prediction_confidence"), p.get("p_up"), p.get("p_flat"), p.get("p_down"), 0.0),
                        )
                    existing.add((ticker, pred_date))
                    inserted += 1
            except (ClientError, json.JSONDecodeError, KeyError) as e:
                logger.info("Skipping prediction file %s: %s", key, e)

        if not dry_run:
            conn.commit()
        conn.close()

        if inserted:
            logger.info("Seeded %d predictor_outcomes rows from %d S3 files", inserted, len(keys))
        return {"status": "ok", "rows_written": inserted}

    except Exception as e:
        logger.error("seed_predictor_outcomes failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Step 4: Backfill predictor_outcomes ───────────────────────────────────────


def _backfill_predictor_returns(
    db_path: str,
    dry_run: bool,
    forward_days: int = _DEFAULT_FORWARD_DAYS,
) -> dict:
    """Backfill predictor_outcomes log-domain canonical alpha at the configured horizon.

    Reads `return_{N}d`, `spy_return_{N}d`, `log_return_{N}d`,
    `log_spy_return_{N}d` from universe_returns where N=forward_days. Writes
    horizon-agnostic `actual_log_alpha` (log(stock) - log(spy) at horizon),
    `horizon_days`, `correct`.

    Legacy columns (`actual_5d_return`, `correct_5d`) are NOT dual-written.
    Old rows that resolved under the pre-PR-C legacy 5d-only path retain
    their values — the backtester COALESCE pattern reads
    `COALESCE(actual_log_alpha, actual_5d_return)` so historical reads still
    work. New rows post-PR-C populate the canonical column only; PR F
    retires the legacy column from new writes (already a no-op here) and
    drops the COALESCE fallback in the backtester analytics.

    The pending-row filter switches to `actual_log_alpha IS NULL` so rows
    already populated under the legacy 5d-only path get re-resolved at the
    new horizon when their forward window has closed in universe_returns.

    Args:
        forward_days: prediction horizon in trading days. Driver for both
            the universe_returns column to JOIN on AND the value persisted
            in `predictor_outcomes.horizon_days` per row.
    """
    h = forward_days
    log_col = f"log_return_{h}d"
    log_spy_col = f"log_spy_return_{h}d"

    try:
        conn = sqlite3.connect(db_path)
        _ensure_predictor_outcomes_schema(conn)

        # Verify the universe_returns schema has the log columns we need at
        # this horizon. Pre-PR-A databases only have arithmetic 5d/10d/30d;
        # 21d log columns added by alpha-engine-data #197. Fail loudly
        # rather than silently producing wrong-domain alpha values.
        ur_cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(universe_returns)"
        ).fetchall()}
        missing = [c for c in (log_col, log_spy_col) if c not in ur_cols]
        if missing:
            conn.close()
            return {
                "status": "error",
                "error": (
                    f"universe_returns missing required log-domain columns "
                    f"for forward_days={h}: {missing}. Run alpha-engine-data "
                    f"PR A migration (#197) or use a horizon whose log "
                    f"columns exist."
                ),
                "rows_written": 0,
            }

        # Pending rows: never-resolved (new column NULL). Includes rows that
        # had the legacy 5d-only path populate actual_5d_return — they get
        # re-resolved at the new horizon, the legacy column gets refreshed
        # with whatever forward_days indicates so backtester COALESCE
        # consumers see consistent values.
        pending = pd.read_sql_query(
            "SELECT id, symbol, prediction_date, predicted_direction "
            "FROM predictor_outcomes WHERE actual_log_alpha IS NULL",
            conn,
        )
        if pending.empty:
            conn.close()
            return {"status": "ok", "rows_written": 0}

        resolved = 0
        for _, row in pending.iterrows():
            ticker = row["symbol"]
            pred_date = row["prediction_date"]

            ur = conn.execute(
                f"SELECT {log_col}, {log_spy_col} "
                f"FROM universe_returns WHERE ticker = ? AND eval_date = ?",
                (ticker, pred_date),
            ).fetchone()

            if ur is None or ur[0] is None:
                # Either no universe_returns row for this (ticker, date), or
                # the forward window has not yet closed (log_return_Nd
                # NULL — gated by the universe_returns collector). Leave
                # the predictor_outcomes row unresolved; it will be picked
                # up on the next collector run.
                continue

            log_stock = ur[0]
            log_spy = ur[1] if ur[1] is not None else 0.0
            log_alpha = log_stock - log_spy

            direction = row["predicted_direction"]
            if direction == "UP":
                correct = 1 if log_alpha > 0 else 0
            elif direction == "DOWN":
                correct = 1 if log_alpha < 0 else 0
            elif direction == "FLAT":
                # FLAT correctness band: |log_alpha| < 0.01 log-units
                # (≈1% arithmetic at small magnitudes via log(1+r) ≈ r).
                correct = 1 if abs(log_alpha) < 0.01 else 0
            else:
                continue

            if not dry_run:
                conn.execute(
                    "UPDATE predictor_outcomes SET "
                    "actual_log_alpha=?, horizon_days=?, correct=? "
                    "WHERE symbol=? AND prediction_date=?",
                    (round(log_alpha, 6), h, correct, ticker, pred_date),
                )
            resolved += 1

        if not dry_run:
            conn.commit()
        conn.close()

        if resolved:
            logger.info(
                "Backfilled %d predictor_outcomes at horizon=%dd "
                "(log-domain canonical) via universe_returns JOIN",
                resolved, h,
            )
        return {"status": "ok", "rows_written": resolved}

    except Exception as e:
        logger.error("backfill_predictor_returns failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _ensure_score_performance_schema(conn) -> None:
    """Add forward-return + calibrator-v1 context columns to
    score_performance if they don't exist yet.

    Belt-and-suspenders with alpha-engine-research's archive/schema.py
    migrations (v11 forward-returns, v12 calibrator-v1 context). The
    Saturday SF runs DataPhase1 first; if it ever fires against a fresh
    research.db before the research Lambda has cold-started and applied
    its migrations, this ensures the seed/backfill INSERT/UPDATE
    statements still target valid columns. Idempotent — each ALTER is
    skipped when the column already exists.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(score_performance)").fetchall()}
    for col, col_type in [
        ("price_5d", "REAL"), ("return_5d", "REAL"), ("spy_5d_return", "REAL"),
        ("beat_spy_5d", "INTEGER"), ("eval_date_5d", "TEXT"),
        ("price_10d", "REAL"), ("return_10d", "REAL"), ("spy_10d_return", "REAL"),
        ("beat_spy_10d", "INTEGER"), ("eval_date_10d", "TEXT"),
        ("price_30d", "REAL"), ("return_30d", "REAL"), ("spy_30d_return", "REAL"),
        ("beat_spy_30d", "INTEGER"), ("eval_date_30d", "TEXT"),
        # Calibrator-v1 context (alpha-engine-research migration #12)
        ("quant_score", "REAL"), ("qual_score", "REAL"),
        ("conviction", "TEXT"), ("sector_modifier", "REAL"),
        ("market_regime", "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE score_performance ADD COLUMN {col} {col_type}")
    conn.commit()


def _ensure_predictor_outcomes_schema(conn) -> None:
    """Add horizon-agnostic columns to predictor_outcomes if not present.

    Defensive idempotent migration that mirrors alpha-engine-research's
    schema v13 (research/archive/schema.py). The data Lambda writes to
    research.db too — if it runs BEFORE the research Lambda's cold-start
    schema migration, the new columns wouldn't exist and the backfill
    UPDATE would fail. Belt-and-suspenders: each ALTER is wrapped to
    skip if the column already exists. Predictor 21d migration plan at
    alpha-engine-docs/private/predictor-21d-migration-260509.md (PR C).
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(predictor_outcomes)").fetchall()}
    for col, col_type in [
        ("actual_log_alpha", "REAL"),
        ("horizon_days", "INTEGER"),
        ("correct", "INTEGER"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE predictor_outcomes ADD COLUMN {col} {col_type}")
    conn.commit()


# Canonical context columns the producer-side seed must populate.
# Source of truth for the drift-gate query; mirrors the columns added by
# alpha-engine-research migration #12 (calibrator-v1 context).
_CANONICAL_CONTEXT_COLUMNS = (
    "quant_score",
    "qual_score",
    "conviction",
    "sector_modifier",
    "market_regime",
)

# Effective date for the drift gate — first Saturday SF run AFTER this PR
# merges. Rows with `score_date >= _DRIFT_EFFECTIVE_DATE` are counted
# against the producer's coverage contract. Pre-cutover rows are excluded
# so the gate isn't polluted by legacy NULLs the backfill step is still
# catching up on.
_DRIFT_EFFECTIVE_DATE = "2026-05-17"


def _emit_context_coverage_metric(db_path: str) -> dict:
    """Drift-detection CloudWatch gauge for the canonical-context contract.

    Producer-side: after seed + backfill complete, query score_performance
    for rows with score_date >= _DRIFT_EFFECTIVE_DATE and compute the
    percentage that have ALL 5 canonical context columns populated
    (non-NULL). Emit as ``AlphaEngine/Data/score_performance_canonical_coverage_pct``
    so an alarm can fire if the percentage drops below the contract
    threshold for any cycle.

    Always emits (including 100.0) so alarm baselines are continuous;
    CloudWatch missing-data is harder to alarm against than a steady
    series. Best-effort: read / metric-emit errors log a warning but
    never raise — this is observability, not a load-bearing path.

    Mirrors the chronic-gap drift detection pattern at
    weekly_collector.py:_check_chronic_gap_polygon_recovery.
    """
    summary: dict = {"status": "ok"}
    try:
        conn = sqlite3.connect(db_path)
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM score_performance WHERE score_date >= ?",
                (_DRIFT_EFFECTIVE_DATE,),
            ).fetchone()[0]
            if total == 0:
                # Pre-cutover, or no new data this cycle. Coverage is
                # undefined; report 100.0 so the alarm doesn't fire on a
                # legitimately-empty window.
                summary.update(
                    rows_post_cutoff=0,
                    rows_fully_populated=0,
                    coverage_pct=100.0,
                    note="no rows past effective_date — coverage undefined",
                )
            else:
                null_clause = " OR ".join(
                    f"{col} IS NULL" for col in _CANONICAL_CONTEXT_COLUMNS
                )
                nulls = conn.execute(
                    f"SELECT COUNT(*) FROM score_performance "
                    f"WHERE score_date >= ? AND ({null_clause})",
                    (_DRIFT_EFFECTIVE_DATE,),
                ).fetchone()[0]
                populated = total - nulls
                coverage_pct = (populated / total) * 100.0
                summary.update(
                    rows_post_cutoff=total,
                    rows_fully_populated=populated,
                    coverage_pct=round(coverage_pct, 2),
                )
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "context_coverage_metric: DB read failed — drift alarm "
            "cadence may degrade until next cycle. %s", exc,
        )
        summary["status"] = "skipped"
        summary["error"] = str(exc)
        return summary

    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "score_performance_canonical_coverage_pct",
                "Value": float(summary["coverage_pct"]),
                "Unit": "Percent",
            }],
        )
    except Exception as exc:
        logger.warning(
            "score_performance_canonical_coverage_pct metric emit failed: "
            "%s — drift alarm cadence may degrade until next cycle.", exc,
        )
        summary["status"] = "skipped"
        summary["error"] = str(exc)

    if summary["status"] == "ok" and summary.get("coverage_pct", 100.0) < 100.0:
        logger.warning(
            "Canonical context coverage drift: %.2f%% (%d/%d post-%s rows "
            "fully populated). Expected 100%%. Producer-side regression — "
            "investigate _seed_score_performance / signals.json shape.",
            summary["coverage_pct"], summary["rows_fully_populated"],
            summary["rows_post_cutoff"], _DRIFT_EFFECTIVE_DATE,
        )

    return summary


def _list_signal_dates(s3, bucket: str, prefix: str) -> list[str]:
    """List all signal dates from S3."""
    dates = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if len(part) == 10 and part[4] == "-" and part[7] == "-":
                dates.append(part)
    dates.sort()
    return dates
