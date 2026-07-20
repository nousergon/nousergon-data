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
  - score_performance_outcomes: the long-format outcome store (EPIC
    config#1483). Written at ``_DECAY_CURVE_POLICY``'s horizons — the fleet
    DEFAULT_POLICY's primary (21d) + diagnostic (5d) plus config#1981's
    intermediate decay-curve diagnostics (1d/3d/10d/15d) — so the backtester's
    alpha-decay-curve consumer has enough points between the two canonical
    horizons to plot a real fade-over-time curve, not just two endpoints.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from nousergon_lib.quant.horizons import DEFAULT_POLICY, HorizonPolicy

logger = logging.getLogger(__name__)

# Default prediction horizon. Matches the predictor's canonical training
# target (alpha-engine-predictor #114 Track A cutover, 2026-05-09). Should be
# config-driven once weekly_collector exposes a `signal_returns.forward_days`
# YAML setting; until then this constant is the source of truth and the
# `forward_days` parameter on `collect` lets call sites override.
_DEFAULT_FORWARD_DAYS = 21

# config#1981 — alpha-decay-curve intermediate-horizon ladder (operator
# ruling "Option A", 2026-07-16): score_performance_outcomes previously only
# carried the fleet DEFAULT_POLICY's two horizons (5d diagnostic, 21d
# primary), which is not enough points to plot a genuine decay curve. This
# is a LOCAL policy override, constructed the same way the producer's own
# test suite already demonstrates (test_signal_returns_outcome_records.py
# ``HorizonPolicy(primary_horizon=21, diagnostic_horizons=(5, 10))``) —
# deliberately NOT a change to nousergon_lib's fleet-wide DEFAULT_POLICY
# (that constant is consumed by the predictor/evaluator/executor too; a
# schema/label decision like widening it is out of scope here). Diagnostic
# horizons are graceful-empty by design (HorizonPolicy contract), so any
# entry whose universe_returns columns aren't populated yet (or an older
# row predating this PR) is skipped, not an error — see
# ``_backfill_outcome_records``'s per-horizon column-existence check.
#
# Ladder choice: 1d/3d/5d/10d/15d/21d. 5d and 21d are the pre-existing
# canonical points; 10d already had raw universe_returns columns (added
# for the retired legacy 10d horizon, config#1456) but was never wired into
# the long-format outcome store. 1d/3d/15d are genuinely new
# universe_returns columns (this PR) chosen to give roughly even coverage
# of the first three trading weeks post-entry, where alpha decay is
# expected to be steepest.
_DECAY_CURVE_DIAGNOSTIC_HORIZONS: tuple[int, ...] = (1, 3, 5, 10, 15)
_DECAY_CURVE_POLICY = HorizonPolicy(
    primary_horizon=DEFAULT_POLICY.primary_horizon,
    diagnostic_horizons=_DECAY_CURVE_DIAGNOSTIC_HORIZONS,
)


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
      2. Primary outcome write: long-format score_performance_outcomes (fail-loud);
         wide-column bookkeeping echo writes only price_{h}d (outcome columns retired)
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

    # Step 2 — PRIMARY outcome write (EPIC config#1483 Phase 4, config#1550):
    # the long-format score_performance_outcomes table is now the canonical
    # outcome store — one row per (signal, score_date, HorizonPolicy horizon),
    # sourced from universe_returns decimals + validated against the
    # nousergon_lib outcome_record contract. Every consumer reads THIS
    # (analysis.outcome_store & peers; burn-down allowlists {} fleet-wide), so a
    # write failure must FAIL the whole collector step — the Phase-2 dual-write
    # soak that let it degrade to status:partial is retired.
    #
    # Policy: _DECAY_CURVE_POLICY (config#1981), a LOCAL extension of
    # DEFAULT_POLICY's diagnostic horizons (adds 1d/3d/10d/15d alongside the
    # existing 5d) so the long-format store carries enough intermediate
    # points for a real alpha-decay curve. The primary horizon (21d) is
    # unchanged — this only adds diagnostic ROWS, no schema change, per the
    # HorizonPolicy contract (nousergon_lib.quant.horizons).
    results["backfill_outcome_records"] = _backfill_outcome_records(
        db_path, dry_run, policy=_DECAY_CURVE_POLICY,
    )

    # Step 2c — wide-column bookkeeping ECHO. The horizon-suffixed OUTCOME
    # columns (return_/spy_*_return/beat_spy_/log_alpha_21d) are RETIRED: no
    # longer written now that the long store is primary and consumers are cut
    # over (config#1550, EPIC config#1483 Phase 4). Only the non-outcome
    # price_{h}d bookkeeping survives (not part of the outcome contract; kept
    # until a follow-on decides otherwise).
    results["backfill_score_returns"] = _backfill_score_returns(db_path, dry_run)

    # Step 2b: Drift gate — emit canonical-context coverage as a CW gauge
    # so an alarm fires if the producer ever regresses (e.g. signals.json
    # shape drift, seed-path bug, schema migration skew). Closes the loop
    # on the 2026-05-09 producer-side bug class.
    if not dry_run:
        results["context_coverage_drift"] = _emit_context_coverage_metric(db_path)

    # Step 3: Seed predictor_outcomes (live / champion)
    results["seed_predictor_outcomes"] = _seed_predictor_outcomes(
        s3, bucket, db_path, dry_run,
    )

    # Step 3b: Seed challenger (shadow) predictor_outcomes — champion/challenger
    # Phase 2 (L4469). No-op until the Phase-1 shadow runner writes shadow files.
    results["seed_shadow_predictor_outcomes"] = _seed_shadow_predictor_outcomes(
        s3, bucket, db_path, dry_run,
    )

    # Step 4: Backfill predictor_outcomes via universe_returns JOIN
    results["backfill_predictor_returns"] = _backfill_predictor_returns(
        db_path, dry_run, forward_days=forward_days,
    )

    # Step 4b: Horizon-grading freshness gate (config#2972) — emit the
    # trading-day lag between "newest date whose forward_days window has
    # closed" and "newest date actually graded" for both universe_returns and
    # predictor_outcomes. A healthy pipeline keeps this at 0; sustained lag
    # growth (not a one-off 1-2td lag right after a window closes) is the
    # alarmable signal that the JOIN/backfill has genuinely stalled, as
    # distinct from the expected wait for a forward-looking window to close.
    if not dry_run:
        results["horizon_grading_lag"] = _emit_horizon_grading_lag_metric(
            db_path, forward_days=forward_days,
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


def _load_stance_lookup_for_date(s3, bucket: str, sig_date: str) -> dict[str, str]:
    """Read predictions.json for ``sig_date`` and return
    {ticker: stance}. Empty dict on any failure (S3 404, JSON parse
    error, predictions.json without stance field).

    Stance field was added to predictions.json on 2026-05-11
    (alpha-engine-predictor#137 heuristic classifier). Predictions
    older than that lack the field — lookup returns an empty dict
    and the score_performance row's stance stays NULL. Backtester's
    by_stance attribution treats NULL as "no stance recorded."

    Per-date result is cached by ``_seed_score_performance`` / the
    backfill caller so the same date isn't fetched twice per run.
    """
    key = f"predictor/predictions/{sig_date}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read())
    except (ClientError, json.JSONDecodeError):
        return {}
    out: dict[str, str] = {}
    for pred in payload.get("predictions") or []:
        if not isinstance(pred, dict):
            continue
        ticker = pred.get("ticker")
        stance = pred.get("stance")
        if isinstance(ticker, str) and isinstance(stance, str):
            out[ticker] = stance
    return out


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
        # Per-date stance lookups cached so we hit S3 once per sig_date
        # instead of once per (sig_date, ticker). Stance was added to
        # predictions.json on 2026-05-11; older dates' lookups return
        # empty + the row's stance column stays NULL.
        stance_by_date: dict[str, dict[str, str]] = {}
        for sig_date in signal_dates:
            try:
                obj = s3.get_object(Bucket=bucket, Key=f"{signals_prefix}/{sig_date}/signals.json")
                signals = json.loads(obj["Body"].read())
            except (ClientError, json.JSONDecodeError):
                continue
            stance_by_date.setdefault(
                sig_date, _load_stance_lookup_for_date(s3, bucket, sig_date),
            )

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

            # Stance lookup — NULL if predictor didn't score this ticker
            # on this date or if the predictions.json for sig_date
            # predates the stance field (2026-05-11+).
            stance = stance_by_date.get(sig_date, {}).get(ticker)

            if not dry_run:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO score_performance (
                        symbol, score_date, score, price_on_date,
                        quant_score, qual_score, conviction,
                        sector_modifier, market_regime, stance
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ticker, sig_date,
                        round(float(score), 2), round(price, 2),
                        ctx["quant_score"], ctx["qual_score"],
                        ctx["conviction"], ctx["sector_modifier"],
                        ctx["market_regime"], stance,
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
    """Backfill the wide ``price_{h}d`` exit-price bookkeeping columns in
    score_performance by JOINing universe_returns.

    EPIC config#1483 Phase 4 (config#1550): the horizon-suffixed OUTCOME columns
    (``return_{h}d`` / ``spy_{h}d_return`` / ``beat_spy_{h}d`` / ``log_alpha_21d``)
    are RETIRED — the canonical outcome store is the long-format
    ``score_performance_outcomes`` table (Step 2, ``_backfill_outcome_records``),
    which every consumer now reads. This function keeps only the non-outcome
    ``price_{h}d`` write (an exit-price convenience, not part of the outcome
    contract), and the beat_spy-repair + log_alpha-backfill blocks retire WITH
    the wide writes (the long-store insert already derives beat_spy and carries
    log_alpha). Physical columns are NOT dropped (SQLite; dead columns are
    harmless — legacy rows keep their historical values).
    """
    try:
        conn = sqlite3.connect(db_path)
        _ensure_score_performance_schema(conn)

        updated = 0
        for horizon in ("5d", "10d", "21d", "30d"):
            # Pending = rows whose price_{h}d exit bookkeeping isn't computed yet.
            # (Re-keyed off price_{h}d rather than the now-retired return_{h}d.)
            pending = pd.read_sql_query(
                f"SELECT symbol, score_date, price_on_date FROM score_performance WHERE price_{horizon} IS NULL",
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

                # Look up forward return from universe_returns purely to derive
                # the exit price — the return itself is NOT persisted anymore.
                ur = conn.execute(
                    f"SELECT return_{horizon} FROM universe_returns WHERE ticker = ? AND eval_date = ?",
                    (ticker, score_date),
                ).fetchone()

                if ur is None or ur[0] is None:
                    continue

                stock_return = ur[0]  # already as decimal (e.g., 0.05)
                exit_price = round(entry_price * (1 + stock_return), 2)

                if not dry_run:
                    conn.execute(
                        f"UPDATE score_performance SET price_{horizon}=? WHERE symbol=? AND score_date=? AND price_{horizon} IS NULL",
                        (exit_price, ticker, score_date),
                    )
                updated += 1

        if not dry_run:
            conn.commit()
        conn.close()

        if updated:
            logger.info("Backfilled %d score_performance price_{h}d columns via universe_returns JOIN", updated)
        return {"status": "ok", "rows_written": updated}

    except Exception as e:
        logger.error("backfill_score_returns failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Step 2c: Long-format outcome dual-write (EPIC config#1483 Phase 2) ─────────


def _ensure_score_performance_outcomes_schema(conn) -> None:
    """Create the long-format ``score_performance_outcomes`` table if absent.

    The config#1483 root-cause fix for the wide horizon-suffixed columns: one
    row per (signal, score_date, horizon_days) so a horizon change is a DATA
    change, not a fleet-wide column rename. Field names + semantics mirror
    ``nousergon_lib.contracts`` ``outcome_record`` (v1) and
    ``nousergon_lib.quant.horizons.OutcomeColumns``. Self-creating (IF NOT
    EXISTS) so this producer is Phase-2-self-sufficient; the authoritative
    research-side migration + consumer reads land in Phase 3. Returns/spy
    stored as DECIMALS (matching the universe_returns source + log_alpha),
    NOT the legacy percent quirk of the wide columns.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS score_performance_outcomes (
            id             INTEGER PRIMARY KEY,
            signal_id      TEXT NOT NULL,
            symbol         TEXT NOT NULL,
            score_date     TEXT NOT NULL,
            horizon_days   INTEGER NOT NULL,
            beat_spy       INTEGER,
            stock_return   REAL,
            spy_return     REAL,
            log_alpha      REAL,
            is_primary     INTEGER NOT NULL,
            resolved_at    TEXT NOT NULL,
            schema_version INTEGER NOT NULL DEFAULT 1,
            UNIQUE(signal_id, horizon_days)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_spo_horizon ON score_performance_outcomes(horizon_days)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_spo_score_date ON score_performance_outcomes(score_date)"
    )
    conn.commit()


def _backfill_outcome_records(
    db_path: str, dry_run: bool, resolved_at: str | None = None, policy=None,
) -> dict:
    """Dual-write the long-format ``score_performance_outcomes`` rows alongside
    the wide ``score_performance`` columns (EPIC config#1483 Phase 2).

    For each canonical ``HorizonPolicy`` horizon (primary 21d + diagnostics),
    reads the DECIMAL forward returns straight from ``universe_returns`` (the
    same JOIN Step 2 uses, before its percent conversion), derives the
    long-format record, VALIDATES it against the ``outcome_record`` contract
    (fail-loud on a shape/label violation), and idempotently inserts it. The
    canonical primary horizon carries the log-domain ``log_alpha``; diagnostic
    horizons carry ``log_alpha = NULL`` (no canonical alpha at a non-primary
    horizon — the contract's if/then permits null only when ``is_primary`` is
    False). Legacy 10d/30d horizons are deliberately NOT carried into the
    canonical store — only the policy horizons.

    ``resolved_at``/``policy`` are injectable for deterministic tests.
    """
    from nousergon_lib.contracts import validate
    from nousergon_lib.quant.horizons import DEFAULT_POLICY

    policy = policy or DEFAULT_POLICY
    resolved_at = resolved_at or datetime.now(timezone.utc).isoformat()

    try:
        conn = sqlite3.connect(db_path)
        _ensure_score_performance_outcomes_schema(conn)

        ur_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(universe_returns)").fetchall()
        }
        has_log = {"log_return_21d", "log_spy_return_21d"} <= ur_cols
        written = 0

        for h in policy.all_horizons:
            hcol = f"{h}d"
            need = {f"return_{hcol}", f"spy_return_{hcol}", f"beat_spy_{hcol}"}
            if not need <= ur_cols:
                # A policy horizon whose universe_returns columns don't exist
                # yet (e.g. a newly-added diagnostic horizon before its data
                # collector ships). Skip + WARN — graceful-empty, detectable.
                logger.warning(
                    "score_performance_outcomes: universe_returns lacks %s — "
                    "skipping horizon %dd",
                    sorted(need - ur_cols), h,
                )
                continue

            is_primary = policy.is_primary(h)
            log_select = (
                ", ur.log_return_21d, ur.log_spy_return_21d"
                if (is_primary and has_log)
                else ""
            )
            rows = conn.execute(
                f"""
                SELECT sp.symbol, sp.score_date,
                       ur.return_{hcol}, ur.spy_return_{hcol}, ur.beat_spy_{hcol}{log_select}
                FROM score_performance sp
                JOIN universe_returns ur
                  ON ur.ticker = sp.symbol AND ur.eval_date = sp.score_date
                WHERE ur.return_{hcol} IS NOT NULL
                """
            ).fetchall()

            for row in rows:
                symbol, score_date, stock_return, spy_return, beat_spy = row[:5]
                # A resolved record requires both legs; a missing spy leg means
                # the outcome isn't fully resolved yet — skip (re-picked up on a
                # later run once universe_returns has it).
                if stock_return is None or spy_return is None:
                    continue

                log_alpha = None
                if is_primary and has_log:
                    lr, lsr = row[5], row[6]
                    if lr is not None:
                        log_alpha = round(lr - (lsr if lsr is not None else 0.0), 6)
                if is_primary and log_alpha is None:
                    # The primary row MUST carry the canonical label; writing a
                    # null-alpha primary would violate the contract. Skip + WARN
                    # rather than fabricate — the row lands once the log columns
                    # resolve. (Post-#197 universe_returns always has them.)
                    logger.warning(
                        "score_performance_outcomes: canonical log_alpha "
                        "unavailable for primary %dd row %s@%s — skipping",
                        h, symbol, score_date,
                    )
                    continue

                # beat_spy: prefer the stored flag; derive from the decimal
                # legs when null (mirrors Step 2's beat_spy repair), never
                # fabricate.
                beat = bool(beat_spy) if beat_spy is not None else (stock_return > spy_return)

                record = {
                    "schema_version": 1,
                    "signal_id": f"{symbol}:{score_date}",
                    "score_date": score_date,
                    "horizon_days": int(h),
                    "beat_spy": beat,
                    "stock_return": float(stock_return),
                    "spy_return": float(spy_return),
                    "log_alpha": float(log_alpha) if log_alpha is not None else None,
                    "resolved_at": resolved_at,
                    "is_primary": is_primary,
                }
                # Fail-loud on a contract violation — a malformed record is a
                # producer bug (caught by the step-level except → status:error,
                # which surfaces as collect() status:partial, NOT a silent skip).
                validate("outcome_record", record)

                if not dry_run:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO score_performance_outcomes
                            (signal_id, symbol, score_date, horizon_days, beat_spy,
                             stock_return, spy_return, log_alpha, is_primary,
                             resolved_at, schema_version)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["signal_id"], symbol, score_date, int(h),
                            1 if beat else 0,
                            record["stock_return"], record["spy_return"],
                            record["log_alpha"],
                            1 if is_primary else 0,
                            resolved_at, 1,
                        ),
                    )
                written += 1

        if not dry_run:
            conn.commit()
        conn.close()

        if written:
            logger.info(
                "Dual-wrote %d long-format score_performance_outcomes rows "
                "(config#1483 Phase 2)", written,
            )
        return {"status": "ok", "rows_written": written}

    except Exception as e:
        # FAIL-LOUD (EPIC config#1483 Phase 4, config#1550). The Phase-2 soak
        # carve-out that swallowed this into status:partial is retired: the
        # long-format store is now the PRIMARY outcome write that every consumer
        # reads, so a schema / contract / JOIN failure here starves the live eval
        # system and MUST fail the collector step rather than degrade silently.
        logger.error("backfill_outcome_records failed: %s", e)
        raise


# ── Step 3: Seed predictor_outcomes ───────────────────────────────────────────


def _seed_predictor_outcomes(s3, bucket: str, db_path: str, dry_run: bool) -> dict:
    """Seed predictor_outcomes from S3 predictions/*.json files."""
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="predictor/predictions/", Delimiter="/")
        keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".json") and "latest" not in obj["Key"]]

        if not keys:
            return {"status": "ok", "rows_written": 0, "note": "no prediction files in S3"}

        conn = sqlite3.connect(db_path)
        # Ensure horizon-agnostic + barrier_win_prob + model_version columns
        # exist BEFORE the seed INSERT (Step 3) — the backfill (Step 4) also
        # calls this but runs later, so the INSERT below would reference a
        # missing column on first run without this. Idempotent: Step 4's call
        # becomes a no-op.
        _ensure_predictor_outcomes_schema(conn)
        # Live (champion) dedup is on (symbol, prediction_date): there is exactly
        # ONE live prediction per symbol/day, so version is not part of the live
        # key (legacy rows with NULL model_version still dedup correctly, and a
        # re-seed never duplicates). Shadow rows are deduped separately on
        # (symbol, date, version_id) in _seed_shadow_predictor_outcomes.
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
                model_version = data.get("model_version")
                for p in data.get("predictions", []):
                    ticker = p.get("ticker")
                    if not ticker or (ticker, pred_date) in existing:
                        continue
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO predictor_outcomes (symbol, prediction_date, predicted_direction, prediction_confidence, p_up, p_flat, p_down, score_modifier_applied, barrier_win_prob, model_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (ticker, pred_date, p.get("predicted_direction"), p.get("prediction_confidence"), p.get("p_up"), p.get("p_flat"), p.get("p_down"), 0.0, p.get("barrier_win_prob"), model_version),
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


# ── Step 3b: Seed challenger (shadow) outcomes ────────────────────────────────


def _ensure_shadow_outcomes_schema(conn) -> None:
    """Create the challenger-outcomes table if absent (champion/challenger
    Phase 2, L4469).

    Deliberately a SEPARATE table from ``predictor_outcomes`` rather than
    multi-version rows in it: the live table is ``UNIQUE(symbol,
    prediction_date)`` and every existing consumer (drift gate, last-week
    scorecard, backtester) assumes exactly one row per (symbol, date). Injecting
    challenger rows there would violate the constraint AND silently inflate
    those consumers' counts. This table holds the observe-only shadow
    predictions keyed by ``UNIQUE(symbol, prediction_date, model_version)``;
    the backfill resolves its realized canonical alpha exactly like the live
    table so per-version rank IC can be scored against the champion.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS predictor_outcomes_shadow (
            id                      INTEGER PRIMARY KEY,
            symbol                  TEXT NOT NULL,
            prediction_date         TEXT NOT NULL,
            model_version           TEXT NOT NULL,
            predicted_direction     TEXT,
            prediction_confidence   REAL,
            p_up                    REAL,
            p_flat                  REAL,
            p_down                  REAL,
            barrier_win_prob        REAL,
            actual_log_alpha        REAL,
            horizon_days            INTEGER,
            correct                 INTEGER,
            UNIQUE(symbol, prediction_date, model_version)
        )
        """
    )
    conn.commit()


def _seed_shadow_predictor_outcomes(s3, bucket: str, db_path: str, dry_run: bool) -> dict:
    """Seed predictor_outcomes_shadow from predictions_shadow/{version_id}/*.json.

    Champion/challenger Phase 2 (L4469): the Phase-1 shadow runner writes each
    registered challenger's predictions to predictor/predictions_shadow/
    {version_id}/{date}.json (trade-on-none). Seed them into the dedicated
    shadow table, tagged with model_version=version_id, so the Step-4 backfill
    resolves each challenger's realized canonical alpha. Dedup on
    (symbol, date, version_id). Best-effort: a bad shadow file is logged +
    skipped. No-op until the shadow runner produces files.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix="predictor/predictions_shadow/"):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".json") and "latest" not in k:
                    keys.append(k)

        if not keys:
            return {"status": "ok", "rows_written": 0, "note": "no shadow prediction files"}

        conn = sqlite3.connect(db_path)
        _ensure_shadow_outcomes_schema(conn)
        existing = {
            (r[0], r[1], r[2]) for r in
            conn.execute(
                "SELECT symbol, prediction_date, model_version FROM predictor_outcomes_shadow"
            ).fetchall()
        }

        inserted = 0
        for key in keys:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = json.loads(obj["Body"].read())
                pred_date = data.get("date") or key.split("/")[-1].replace(".json", "")
                # version_id from the payload; fall back to the path segment
                # predictions_shadow/{version_id}/{date}.json.
                version_id = data.get("version_id") or key.split("/")[-2]
                for p in data.get("predictions", []):
                    ticker = p.get("ticker")
                    if not ticker or (ticker, pred_date, version_id) in existing:
                        continue
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO predictor_outcomes_shadow (symbol, prediction_date, model_version, predicted_direction, prediction_confidence, p_up, p_flat, p_down, barrier_win_prob) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (ticker, pred_date, version_id, p.get("predicted_direction"), p.get("prediction_confidence"), p.get("p_up"), p.get("p_flat"), p.get("p_down"), p.get("barrier_win_prob")),
                        )
                    existing.add((ticker, pred_date, version_id))
                    inserted += 1
            except (ClientError, json.JSONDecodeError, KeyError, IndexError) as e:
                logger.info("Skipping shadow prediction file %s: %s", key, e)

        if not dry_run:
            conn.commit()
        conn.close()

        if inserted:
            logger.info(
                "Seeded %d challenger (shadow) outcome rows from %d S3 files",
                inserted, len(keys),
            )
        return {"status": "ok", "rows_written": inserted}

    except Exception as e:
        logger.error("seed_shadow_predictor_outcomes failed: %s", e)
        return {"status": "error", "error": str(e), "rows_written": 0}


# ── Step 4: Backfill predictor_outcomes ───────────────────────────────────────


def _resolve_pending_for_table(
    conn, table: str, h: int, log_col: str, log_spy_col: str, dry_run: bool,
) -> tuple[int, dict]:
    """Resolve never-resolved (actual_log_alpha IS NULL) rows of ``table`` at
    horizon ``h`` via the universe_returns JOIN. Updates each row BY ID with its
    own ``correct`` (per its predicted_direction); the realized alpha is shared
    across versions for a (ticker, date) but written per-row. Returns
    ``(resolved, non_binary_skipped)``. ``table`` is a fixed internal literal,
    never user input. Shared by the live (predictor_outcomes) and challenger
    (predictor_outcomes_shadow) backfills — L4469 Phase 2.
    """
    pending = pd.read_sql_query(
        f"SELECT id, symbol, prediction_date, predicted_direction "
        f"FROM {table} WHERE actual_log_alpha IS NULL",
        conn,
    )
    resolved = 0
    non_binary_skipped: dict[str, int] = {}
    if pending.empty:
        return resolved, non_binary_skipped

    for _, row in pending.iterrows():
        ticker = row["symbol"]
        pred_date = row["prediction_date"]
        ur = conn.execute(
            f"SELECT {log_col}, {log_spy_col} "
            f"FROM universe_returns WHERE ticker = ? AND eval_date = ?",
            (ticker, pred_date),
        ).fetchone()
        if ur is None or ur[0] is None:
            continue  # no row, or forward window not yet closed — retry next run

        log_stock = ur[0]
        log_spy = ur[1] if ur[1] is not None else 0.0
        log_alpha = log_stock - log_spy

        direction = row["predicted_direction"]
        if direction == "UP":
            correct = 1 if log_alpha > 0 else 0
        elif direction == "DOWN":
            correct = 1 if log_alpha < 0 else 0
        else:
            # Non-binary (legacy FLAT / unexpected) — WARN-counted, left NULL so
            # a later SELECT still surfaces it (fail-loud carve-out, see the
            # caller's warning). If this grows, the binary contract regressed.
            key = str(direction) if direction is not None else "NULL"
            non_binary_skipped[key] = non_binary_skipped.get(key, 0) + 1
            continue

        if not dry_run:
            conn.execute(
                f"UPDATE {table} SET "
                f"actual_log_alpha=?, horizon_days=?, correct=? WHERE id=?",
                (round(log_alpha, 6), h, correct, int(row["id"])),
            )
        resolved += 1
    return resolved, non_binary_skipped


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

        # Resolve BOTH the live champion table and the challenger shadow table
        # (champion/challenger Phase 2, L4469) at this horizon — same realized
        # universe_returns JOIN, each row updated by id with its own `correct`.
        # No early-return on an empty live table: the shadow table may still
        # have pending rows (and vice versa).
        _ensure_shadow_outcomes_schema(conn)
        resolved = 0
        non_binary_skipped: dict[str, int] = {}
        for _table in ("predictor_outcomes", "predictor_outcomes_shadow"):
            _r, _nb = _resolve_pending_for_table(
                conn, _table, h, log_col, log_spy_col, dry_run,
            )
            resolved += _r
            for _k, _v in _nb.items():
                non_binary_skipped[_k] = non_binary_skipped.get(_k, 0) + _v

        if not dry_run:
            conn.commit()
        conn.close()

        if resolved:
            logger.info(
                "Backfilled %d predictor_outcomes(+shadow) rows at horizon=%dd "
                "(log-domain canonical) via universe_returns JOIN",
                resolved, h,
            )
        if non_binary_skipped:
            logger.warning(
                "[signal_returns] skipped %d predictor_outcomes rows with "
                "non-binary predicted_direction (legacy FLAT or unexpected "
                "label, by value: %s). Expected post-#143: binary UP/DOWN "
                "only. If this count grows over time, audit the predictor's "
                "calibrator output contract.",
                sum(non_binary_skipped.values()), non_binary_skipped,
            )
        return {
            "status": "ok",
            "rows_written": resolved,
            "non_binary_skipped": non_binary_skipped,
        }

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
        # Canonical 21d horizon (alpha-engine-research migration #18,
        # 2026-05-29). Arithmetic parity columns + the canonical
        # log-domain market-relative alpha (log_alpha_21d) the predictor
        # trains on (actual_log_alpha). Sourced from universe_returns'
        # return_21d / log_return_21d columns (alpha-engine-data #197).
        # Powers the judge outcome-IC validation (ROADMAP L480 re-scope).
        ("price_21d", "REAL"), ("return_21d", "REAL"), ("spy_21d_return", "REAL"),
        ("beat_spy_21d", "INTEGER"), ("eval_date_21d", "TEXT"),
        ("log_alpha_21d", "REAL"),
        ("price_30d", "REAL"), ("return_30d", "REAL"), ("spy_30d_return", "REAL"),
        ("beat_spy_30d", "INTEGER"), ("eval_date_30d", "TEXT"),
        # Calibrator-v1 context (alpha-engine-research migration #12)
        ("quant_score", "REAL"), ("qual_score", "REAL"),
        ("conviction", "TEXT"), ("sector_modifier", "REAL"),
        ("market_regime", "TEXT"),
        # Stance taxonomy arc (alpha-engine-research migration #16,
        # 2026-05-11). Denormalizes predictor's stance label onto the
        # fact row at write time — Kimball dimensional pattern.
        # Source: predictions.json per-date archive; producer-side
        # extractor _extract_stance_for_date below.
        ("stance", "TEXT"),
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
        # Observe-only López-de-Prado meta-label: P(upper/profit barrier touched
        # before lower/stop barrier), emitted by alpha-engine-predictor #211 into
        # predictions.json. Nullable — null when the meta-label classifier isn't
        # loaded/fitted for a cycle. Recording it here unblocks the backtester's
        # barrier_sizing_optimizer IC gate (was returning barrier_win_prob_column_absent).
        ("barrier_win_prob", "REAL"),
        # Champion/challenger Phase 2 (L4469): which model version produced this
        # prediction row. Live (champion) rows carry predictions.json's
        # model_version; shadow (challenger) rows carry the registry version_id
        # (predictions_shadow/{version_id}/). Legacy rows stay NULL — the live
        # seed dedups on (symbol, prediction_date) so NULL never causes a
        # re-insert, and per-version rank IC simply excludes the unlabelled
        # legacy tail. Enables scoring each version on realized alpha.
        ("model_version", "TEXT"),
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


# ── Horizon-grading freshness gate (config#2972) ─────────────────────────────
#
# 2026-06/07: a prior investigation pass queried research.db directly and
# found predictor_outcomes.horizon_days/correct/actual_log_alpha NULL for
# every prediction_date >= 2026-06-17 (and the same cutoff on
# universe_returns.log_return_21d), and mistook this for a silently-broken
# write path. Root-cause re-investigation (config#2972) found NO break: a
# 21-trading-day-forward metric is *expected* to lag "today" by up to 21
# trading days before it can be populated at all — add_trading_days(2026-06-17,
# 21) == 2026-07-20, which had simply not arrived yet as of the date those
# rows were queried. The apparent "cutoff" was the natural lag boundary, not
# a stall.
#
# The real gap this exposed: NOTHING was distinguishing "expected lag" from
# "the grading pipeline actually stopped advancing" — a prior groom pass spent
# real investigation cycles on a false alarm because there was no cheap,
# producer-side signal for "is the horizon-grading lag inside its expected
# band, or has it stopped shrinking?". This closes that gap the same way
# _emit_context_coverage_metric closes the canonical-context gap: a
# producer-side CloudWatch gauge, best-effort or emit non-fatal, alarmable via
# infrastructure/setup_horizon_grading_alarms.sh.
#
# Metric definition: for each of universe_returns.log_return_{h}d and
# predictor_outcomes.horizon_days, compute
#   lag_trading_days = count_trading_days(MAX(date with the column populated),
#                                          last date whose h-trading-day
#                                          forward window has already closed)
# A healthy pipeline keeps this at 0 (every date whose window has closed is
# graded by the next run). A stalled pipeline (the JOIN/backfill genuinely
# breaking, e.g. a real regression in universe_returns's 21d computation)
# makes this grow without bound instead of resetting to 0 each run — that
# growth, not the raw NULL count, is the alarmable signal.
def _newest_window_closed(
    max_candidate: str | None, today: date, h: int,
) -> str | None:
    """Newest date <= `max_candidate` whose h-trading-day forward window has
    already closed as of `today`. None if no row is closed yet (e.g. the
    table is empty, or every row is still within its forward window).

    `max_candidate` (the newest row present in the table at all) may itself
    still be inside its own forward window — walk backward a bounded number
    of trading days (h + margin) to find the newest one that has closed.
    Bounded at h + 10 steps: a well-formed table has a closed row within
    that many trading days of the newest row, since _get_existing_dates
    keeps re-enqueuing any closed-but-ungraded date every run.
    """
    if not max_candidate:
        return None
    from nousergon_lib.trading_calendar import add_trading_days, previous_trading_day

    d = date.fromisoformat(max_candidate)
    for _ in range(h + 10):
        if add_trading_days(d, h) < today:
            return d.isoformat()
        d = previous_trading_day(d)
    return None


def _emit_horizon_grading_lag_metric(db_path: str, forward_days: int) -> dict:
    """CloudWatch gauge: trading-day lag between "latest closed h-day window"
    and "latest date actually graded" for universe_returns + predictor_outcomes.

    Always emits (including 0) so the alarm baseline is continuous. Best
    effort: read/emit errors log a warning but never raise — this is
    observability, not a load-bearing path (mirrors
    _emit_context_coverage_metric).
    """
    from nousergon_lib.trading_calendar import count_trading_days

    h = forward_days
    today = date.today()

    summary: dict = {"status": "ok", "forward_days": h}
    try:
        conn = sqlite3.connect(db_path)
        try:
            max_eval_date = conn.execute(
                "SELECT MAX(eval_date) FROM universe_returns"
            ).fetchone()[0]
            max_graded_returns = conn.execute(
                f"SELECT MAX(eval_date) FROM universe_returns "
                f"WHERE log_return_{h}d IS NOT NULL"
            ).fetchone()[0]
            max_prediction_date = conn.execute(
                "SELECT MAX(prediction_date) FROM predictor_outcomes"
            ).fetchone()[0]
            max_graded_outcome = conn.execute(
                "SELECT MAX(prediction_date) FROM predictor_outcomes "
                "WHERE horizon_days IS NOT NULL"
            ).fetchone()[0]
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "horizon_grading_lag_metric: DB read failed — drift alarm "
            "cadence may degrade until next cycle. %s", exc,
        )
        summary["status"] = "skipped"
        summary["error"] = str(exc)
        return summary

    newest_closed_returns = _newest_window_closed(max_eval_date, today, h)
    newest_closed_outcomes = _newest_window_closed(max_prediction_date, today, h)

    # Lag = trading days between the newest already-graded date and the
    # newest date whose window has closed (0 if nothing has closed yet, or
    # the newest closed date is already graded — count_trading_days is a
    # half-open (start, end] count so it's 0 when start == end and never
    # negative for a healthy monotonic backfill).
    lag_returns = (
        count_trading_days(date.fromisoformat(max_graded_returns), date.fromisoformat(newest_closed_returns))
        if newest_closed_returns and max_graded_returns
        else 0
    )
    lag_outcomes = (
        count_trading_days(date.fromisoformat(max_graded_outcome), date.fromisoformat(newest_closed_outcomes))
        if newest_closed_outcomes and max_graded_outcome
        else 0
    )

    summary.update(
        max_eval_date=max_eval_date,
        max_graded_returns_eval_date=max_graded_returns,
        newest_window_closed_eval_date=newest_closed_returns,
        universe_returns_lag_trading_days=lag_returns,
        max_prediction_date=max_prediction_date,
        max_graded_outcome_prediction_date=max_graded_outcome,
        newest_window_closed_prediction_date=newest_closed_outcomes,
        predictor_outcomes_lag_trading_days=lag_outcomes,
    )

    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[
                {
                    "MetricName": "universe_returns_horizon_grading_lag_trading_days",
                    "Value": float(lag_returns),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "HorizonDays", "Value": str(h)}],
                },
                {
                    "MetricName": "predictor_outcomes_grading_lag_trading_days",
                    "Value": float(lag_outcomes),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "HorizonDays", "Value": str(h)}],
                },
            ],
        )
    except Exception as exc:
        logger.warning(
            "horizon_grading_lag metric emit failed: %s — drift alarm "
            "cadence may degrade until next cycle.", exc,
        )
        summary["status"] = "skipped"
        summary["error"] = str(exc)

    if summary["status"] == "ok" and (lag_returns > 0 or lag_outcomes > 0):
        logger.warning(
            "Horizon grading lag: universe_returns=%dtd predictor_outcomes=%dtd "
            "(forward_days=%d). A healthy pipeline re-grades to lag=0 every run — "
            "sustained lag > 0 across consecutive runs indicates the grading "
            "JOIN/backfill has actually stalled (not just normal forward-window "
            "wait), unlike a single-run lag of 1-2td which is expected right "
            "after a new window closes.",
            lag_returns, lag_outcomes, h,
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
