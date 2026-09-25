"""collectors/technical_rating_ledger.py — immutable daily technical-rating ledger +
realized-performance scorer (metron-ops#297, part 2).

Writer identity: reuses ``collectors.metron_market_data``'s S3 helpers
(``_read_json`` / ``_write_json``) — the SAME chokepoint ``collect_technicals`` already
writes through, so no new IAM grant is needed (every identity able to write
``market_data/technicals/latest.json`` holds ``alpha-engine-research/*`` or
``market_data/*``, enumerated 2026-09-14). Every write in this module stays under
``market_data/technicals/`` — never wired into ``collect_intraday`` (the dashboard box's
intraday service may write ONLY ``market_data/intraday/*``).

Two artifacts:

    market_data/technicals/rating_history/{YYYY-MM-DD}.json   (one per trading day,
        IMMUTABLE once written with ``basis: "live"`` — a "backfill"-basis date MAY be
        rewritten if a rating-rule version bump makes it stale, since it is a
        reconstruction, not an as-it-happened observation)
        {schema_version, as_of, rating_version, basis: "live"|"backfill",
         ratings: {yf_symbol: {score, label, close}}}

    market_data/technicals/rating_history/_manifest.json   (internal index — which
        dates exist, their basis + rating_version — so ``collect_rating_ledger`` never
        needs an S3 ``list_objects_v2`` scan to know what it has already written)
        {schema_version, dates: [{date, basis, rating_version}, ...]}   (ascending)

    market_data/technicals/rating_performance.json   (recomputed every EOD run from the
        ledger + close_history — shared contract with the metron consumer, metron-ops#298)
        {schema_version, as_of_utc, rating_version, horizons: [1,5,20], windows: [20,60,250],
         segments: {"live"|"backfill"|"all": {"<window>": {"<horizon>": {
             buckets: {"<label>": {n, mean_fwd, hit_rate, mean_excess}},
             spread_strong_buy_minus_strong_sell, ic_mean, ic_n_dates, noise_floor_ic}}}},
         ic_series: [{date, horizon, ic}]}   (last 250 dates, segment "all" / window 250)

Self-seeding backfill: when fewer than ``LEDGER_BACKFILL_MIN_DATES`` (252) ledger dates
exist, the missing trailing sessions (from the close_history reference calendar) are
rated and written with ``basis: "backfill"`` — one-time and idempotent. Measured
2026-09-14: ~139,627 ``compute_technical_rating`` calls took ~110s single-threaded on
this laptop; a full backfill (~960 symbols x 252 dates =~ 241,920 calls) is the same
order of magnitude, a few minutes.

No lookahead: every rating (live or backfill) is computed from closes truncated to
``<= that date`` — see ``_rate_universe_at_dates``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
from datetime import datetime, timezone
from typing import Any

from collectors import metron_market_data as mmd
from features.feature_engineer import RATING_VERSION, compute_technical_rating

logger = logging.getLogger(__name__)

RATING_LEDGER_PREFIX = "market_data/technicals/rating_history/"
RATING_LEDGER_MANIFEST_KEY = f"{RATING_LEDGER_PREFIX}_manifest.json"
RATING_PERFORMANCE_KEY = "market_data/technicals/rating_performance.json"

LEDGER_SCHEMA_VERSION = 1
LEDGER_MANIFEST_SCHEMA_VERSION = 1
PERFORMANCE_SCHEMA_VERSION = 1

LEDGER_BACKFILL_MIN_DATES = 252

HORIZONS: tuple[int, ...] = (1, 5, 20)
WINDOWS: tuple[int, ...] = (20, 60, 250)
LABELS: tuple[str, ...] = ("Strong Sell", "Sell", "Neutral", "Buy", "Strong Buy")

# The window/horizon big enough to bound how many trailing ledger dates the scorer ever
# needs to read: the largest window's dates, plus the largest horizon so the LAST of
# those windowed dates can still have a realized forward return, plus a small buffer for
# non-trading-day slack.
_SCORER_TRAIL_DATES = max(WINDOWS) + max(HORIZONS) + 10


# ── Ledger: rate-and-truncate (no lookahead) ─────────────────────────────────────────


def _reference_calendar(series: dict[str, list]) -> list[str]:
    """Ascending unique trading-day list to backfill against — SPY's own close-history
    dates when present (always published, RISK_FACTOR_ETFS), else the longest series in
    the consolidated close_history. All SP1500 constituents trade the same US calendar,
    so SPY is a representative session list without re-deriving one."""
    if series.get("SPY"):
        rows = series["SPY"]
    elif series:
        rows = max(series.values(), key=len)
    else:
        return []
    dates = sorted({r[0] for r in rows if isinstance(r, (list, tuple)) and len(r) == 2 and r[0]})
    return dates


def _rate_universe_at_dates(
    series: dict[str, list], target_dates: list[str],
) -> dict[str, dict[str, dict]]:
    """``{date: {yf_symbol: {score, label, close}}}`` for every symbol x date pair with
    a computable rating, using ONLY closes ``<= date`` (no lookahead) — one ascending
    pass per symbol regardless of ``len(target_dates)`` (two-pointer truncation), which
    is what keeps a 960-symbol x 252-date backfill inside the measured runtime budget.

    ``target_dates`` MUST be ascending (the caller's responsibility — this is the hot
    path, no defensive sort)."""
    out: dict[str, dict[str, dict]] = {d: {} for d in target_dates}
    if not target_dates:
        return out
    for sym, rows in series.items():
        valid = [
            (r[0], float(r[1])) for r in rows
            if isinstance(r, (list, tuple)) and len(r) == 2 and r[1] is not None
        ]
        if not valid:
            continue
        idx = 0
        n = len(valid)
        closes_vals: list[float] = []
        last_close: float | None = None
        for d in target_dates:
            while idx < n and valid[idx][0] <= d:
                closes_vals.append(valid[idx][1])
                last_close = valid[idx][1]
                idx += 1
            if not closes_vals:
                continue
            # No per-symbol swallow: inputs are already filtered to non-null closes, so a
            # raise here is a defect in compute_technical_rating — it must fail the phase,
            # not silently thin the ledger (producer repo, fail-loud default).
            rating = compute_technical_rating(_as_series(closes_vals))
            if rating is None:
                continue
            out[d][sym] = {
                "score": rating["score"], "label": rating["label"], "close": last_close,
            }
    return out


def _as_series(values: list[float]):
    import pandas as pd

    return pd.Series(values, dtype="float64")


#: The unit whose v1 run manifest says when the ledger manifest was last read before
#: v1 rewrote it for a trading day — D26 itself (``collect_rating_ledger``). A replay
#: pins its read of ``RATING_LEDGER_MANIFEST_KEY`` to the version current at that
#: run's START, which is the ledger as it stood BEFORE v1's own write for the day
#: (alpha-engine-config-I11231).
LEDGER_PIN_UNIT_ID = "D26"


def _get_ledger_manifest(
    s3_client: Any, bucket: str, *, version_id: str | None = None,
) -> tuple[dict | None, dict]:
    """``(manifest, input_ref)`` for one read of the ledger manifest.

    ``input_ref`` is the lib's closed ``InputRef`` for the run manifest
    (alpha-engine-config-I11231): the version this run actually built on. v1
    recording it is what gives a later shadow of the day a DECLARED pin
    (``shadow.pinned_inputs._declared``) — one exact ``GetObject`` by VersionId,
    with no ``s3:ListBucketVersions`` listing to infer it. On the 2026-09-24
    same-day shadow that listing was AccessDenied and v1's D26 manifest carried
    ``inputs: []``, so neither pin could resolve and D26 read v1's own write.

    Raises on a failed read; a missing object is ``(None, <ref with no version>)``
    through the caller's fallback, exactly as ``mmd._read_json`` treats it.
    """
    params: dict[str, Any] = {"Bucket": bucket, "Key": RATING_LEDGER_MANIFEST_KEY}
    if version_id is not None:
        params["VersionId"] = version_id
    obj = s3_client.get_object(**params)
    manifest = json.loads(obj["Body"].read())
    read_version = obj.get("VersionId") or version_id
    etag = obj.get("ETag")
    return manifest, {
        "key": f"s3://{bucket}/{RATING_LEDGER_MANIFEST_KEY}",
        "etag": str(etag).strip('"') if etag else None,
        "version": str(read_version) if read_version else None,
        "schema_version": None,
    }


def _read_current_ledger_manifest(s3_client: Any, bucket: str) -> tuple[dict | None, dict]:
    """The CURRENT object, fail-soft to ``None`` on any miss like ``mmd._read_json``."""
    try:
        return _get_ledger_manifest(s3_client, bucket)
    except Exception:  # noqa: BLE001 - see the three-part rationale below
        # DELIBERATE degrade — the contract `mmd._read_json` gave this read before it
        # needed the VersionId. (a) Failure mode swallowed: the manifest is absent (the
        # ledger's first run) or unreadable/unparseable. (b) The primary deliverable
        # survives: the ledger self-seeds from an empty manifest, as it always has. (c)
        # Recording surface: the returned InputRef carries no `version`, so the run
        # manifest shows the ledger was read at no recorded version.
        return None, {
            "key": f"s3://{bucket}/{RATING_LEDGER_MANIFEST_KEY}",
            "etag": None, "version": None, "schema_version": None,
        }


def _read_ledger_manifest(s3_client: Any, bucket: str) -> tuple[dict | None, dict]:
    """The ledger manifest this run should build on — the CURRENT object in
    production; under a shadow replay, the version v1 itself built on — and the
    ``InputRef`` naming the version actually read.

    alpha-engine-config-I11231. The manifest is a read-modify-write key: D26 reads it,
    adds the trading day, and writes it back. A shadow run of day D executes after v1's
    D26 has already recorded D as ``basis: "live"``, so reading the CURRENT object made
    the shadow's D26 correctly decline to rewrite an immutable date every single time —
    ``not_applicable`` on 09-21, 09-22 and 09-23 — and ``rating_history/_manifest.json``
    could never be compared at all. Reading the pre-v1 version (pinned by
    ``shadow.pinned_inputs.pin_for`` to the version current at v1's D26 start: on
    2026-09-23 that is the 2026-09-22T20:18:09Z version, v1 having started 20:15:06Z and
    written 20:15:15Z) gives the shadow the SAME base v1 had, so it writes a manifest and
    a live ledger date that parity can grade against v1's.

    Outside a replay ``pin_for`` answers ``unpinned`` without touching S3, and this reads
    the current object — which is what production means. When a replay cannot be pinned
    (no v1 D26 manifest for the day, or the version aged out) it also reads the current
    object, and the immutable-date no-op below still records ``not_applicable``, never
    ``failed``.
    """
    pin = None
    try:
        from shadow.pinned_inputs import pin_for

        pin = pin_for(s3_client, bucket, RATING_LEDGER_MANIFEST_KEY, unit_id=LEDGER_PIN_UNIT_ID)
    except Exception as exc:  # noqa: BLE001 - see the three-part rationale below
        # DELIBERATE degrade, at WARNING. (a) Failure mode swallowed: the pin could not be
        # resolved at all (`shadow.pinned_inputs` would not import, or its manifest/version
        # lookup raised) — NOT the ordinary production case, which `pin_for` answers as an
        # `unpinned` Pin without raising. (b) The primary deliverable survives: the manifest
        # is still read, from the CURRENT object, exactly as production reads it. (c)
        # Recording surface: this WARNING line in the collector's log; inside a replay it
        # means the shadow reads a ledger v1 may already have written, which the D26
        # manifest then shows as `not_applicable` (no_new_data_declared) rather than a diff.
        logger.warning(
            "[technical_rating_ledger] input pinning unavailable for %s (%s) — reading the "
            "CURRENT object (alpha-engine-config-I11231)",
            RATING_LEDGER_MANIFEST_KEY, exc,
        )
    if pin is None or not pin.is_pinned:
        if pin is not None:
            # The pin's own reason, always: in production it says no replay is active; in a
            # shadow run it is the only line that says WHY the current object was read.
            logger.info(
                "[technical_rating_ledger] %s unpinned (%s) — reading the CURRENT object",
                RATING_LEDGER_MANIFEST_KEY, pin.detail,
            )
        return _read_current_ledger_manifest(s3_client, bucket)
    logger.info(
        "[technical_rating_ledger] %s pinned to version %s (%s: %s)",
        RATING_LEDGER_MANIFEST_KEY, pin.version_id, pin.basis, pin.detail,
    )
    try:
        return _get_ledger_manifest(s3_client, bucket, version_id=pin.version_id)
    except Exception as exc:  # noqa: BLE001 - see the three-part rationale below
        # DELIBERATE degrade, at WARNING. (a) Failure mode swallowed: the pinned VERSION
        # could not be read or parsed. (b) The primary deliverable survives: the current
        # object is read instead, the production behaviour. (c) Recording surface: this
        # WARNING line; the D26 manifest then shows whatever the current-object read
        # produces (typically `not_applicable` on a same-day replay), never a silent diff.
        logger.warning(
            "[technical_rating_ledger] pinned version %s of %s unreadable (%s) — reading the "
            "CURRENT object (alpha-engine-config-I11231)",
            pin.version_id, RATING_LEDGER_MANIFEST_KEY, exc,
        )
        return _read_current_ledger_manifest(s3_client, bucket)


def collect_rating_ledger(
    *, bucket: str = mmd.DEFAULT_BUCKET, run_date: str | None = None, dry_run: bool = False,
    s3_client: Any = None,
) -> dict:
    """Write today's immutable ledger date (``basis: "live"``) plus any missing/stale
    self-seeded backfill dates, from the consolidated close_history
    (``metron_market_data.CONSOLIDATED_CLOSE_HISTORY_KEY`` — already published by the
    ``metron_market_data_history`` phase this run; no new fetch). Universe = EVERY
    symbol in the consolidated close_history (SP1500 ∪ held ∪ watchlist), not just held
    symbols, so cross-sectional statistics have power.

    A ``basis: "live"`` date, once written, is NEVER overwritten. A ``basis: "backfill"``
    date IS eligible for a one-time rewrite if its stored ``rating_version`` no longer
    matches the current ``RATING_VERSION`` (a reconstruction, not an as-it-happened
    observation) — self-seeding is defined by date-COUNT, so this is the mechanism that
    keeps a version bump from leaving stale-rule backfill rows behind."""
    if run_date is None:
        from dates import default_run_date

        run_date = default_run_date()
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    consolidated = mmd._read_json(s3_client, bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY)
    series = (consolidated or {}).get("series") or {}
    if not series:
        return {"status": "skipped", "reason": "no close_history"}

    ref_calendar = _reference_calendar(series)
    if not ref_calendar:
        return {"status": "skipped", "reason": "empty reference calendar"}

    manifest, ledger_input_ref = _read_ledger_manifest(s3_client, bucket)
    manifest = manifest or {"schema_version": LEDGER_MANIFEST_SCHEMA_VERSION, "dates": []}
    existing_by_date: dict[str, dict] = {e["date"]: e for e in manifest.get("dates", [])}

    # The trailing `LEDGER_BACKFILL_MIN_DATES` sessions STRICTLY BEFORE run_date — the
    # live write below owns run_date exclusively, so backfill never double-writes it.
    candidate_backfill_dates = [d for d in ref_calendar if d < run_date][-LEDGER_BACKFILL_MIN_DATES:]
    missing_backfill_dates = [d for d in candidate_backfill_dates if d not in existing_by_date]
    stale_backfill_dates = [
        d for d in candidate_backfill_dates
        if d in existing_by_date
        and existing_by_date[d].get("basis") == "backfill"
        and existing_by_date[d].get("rating_version") != RATING_VERSION
    ]
    to_backfill = sorted(set(missing_backfill_dates) | set(stale_backfill_dates))

    try:
        backfill_written = 0
        if to_backfill:
            ratings_by_date = _rate_universe_at_dates(series, to_backfill)
            for d in to_backfill:
                entry = {
                    "schema_version": LEDGER_SCHEMA_VERSION, "as_of": d,
                    "rating_version": RATING_VERSION, "basis": "backfill",
                    "ratings": dict(sorted(ratings_by_date.get(d, {}).items())),
                }
                if not dry_run:
                    mmd._write_json(s3_client, bucket, f"{RATING_LEDGER_PREFIX}{d}.json", entry)
                existing_by_date[d] = {
                    "date": d, "basis": "backfill", "rating_version": RATING_VERSION,
                }
                backfill_written += 1

        # Live write — immutable once basis == "live". Owns run_date exclusively;
        # backfill's candidate window is strictly before run_date, so this is the only
        # path that ever writes today's date.
        live_written = False
        live_skipped_reason = None
        already_live = existing_by_date.get(run_date, {}).get("basis") == "live"
        if ref_calendar[-1] != run_date:
            # close_history has no bar for run_date (holiday, or the close not yet
            # published): a rating computed now would be the PRIOR session's rating stamped
            # with run_date, and "live" dates are immutable — so never write it.
            live_skipped_reason = f"no close for {run_date} (latest {ref_calendar[-1]})"
            logger.warning("[technical_rating_ledger] live write skipped: %s", live_skipped_reason)
        elif already_live:
            # alpha-engine-config-I11231: a re-run for a date v1 already published
            # live — the immutability contract, correctly declining to rewrite it.
            # NOT a failure: `skip_reason` below is only surfaced as `auto_skipped`
            # when nothing else was written this cycle either (see the return
            # statement) so `_phase_collect` routes this to `not_applicable` with
            # `run_units.NOT_RUN_NO_NEW_DATA_DECLARED` ("a target date already
            # published" — the lib's own example) instead of raising
            # `EmptyProduction`. A cycle that ALSO backfilled trailing dates this
            # run is not an auto-skip — it published something — so that case is
            # left to record `ok` exactly as before.
            live_skipped_reason = "target date already live-published, immutable"
        else:
            ratings_by_date = _rate_universe_at_dates(series, [run_date])
            entry = {
                "schema_version": LEDGER_SCHEMA_VERSION, "as_of": run_date,
                "rating_version": RATING_VERSION, "basis": "live",
                "ratings": dict(sorted(ratings_by_date.get(run_date, {}).items())),
            }
            if not dry_run:
                mmd._write_json(s3_client, bucket, f"{RATING_LEDGER_PREFIX}{run_date}.json", entry)
            existing_by_date[run_date] = {
                "date": run_date, "basis": "live", "rating_version": RATING_VERSION,
            }
            live_written = True

        if not dry_run and (backfill_written or live_written):
            manifest_out = {
                "schema_version": LEDGER_MANIFEST_SCHEMA_VERSION,
                "dates": [existing_by_date[d] for d in sorted(existing_by_date)],
            }
            mmd._write_json(s3_client, bucket, RATING_LEDGER_MANIFEST_KEY, manifest_out)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[technical_rating_ledger] ledger write failed: %s", e)
        return {"status": "error", "error": str(e)}

    result = {
        "status": "ok", "backfill_written": backfill_written, "live_written": live_written,
        "live_skipped_reason": live_skipped_reason, "total_dates": len(existing_by_date),
        # alpha-engine-config-I11231: the ledger version this run built on, folded onto
        # the run manifest's `inputs` by `weekly_collector._record_phase_lineage` —
        # v1's record is a later shadow's DECLARED pin, and a shadow's own record
        # shows whether it built on v1's base or on v1's write.
        "input_refs": [ledger_input_ref],
    }
    # alpha-engine-config-I11231: nothing published this cycle, and the reason is
    # the immutability contract correctly declining a rewrite -- not an empty
    # production. Signalling it this way (rather than leaving the caller to infer
    # it from backfill_written==0 and live_written==False) is what lets
    # `_record_phase_lineage` route it to `not_applicable` instead of
    # `EmptyProduction`. If backfill_written is also 0 for a DIFFERENT reason
    # (e.g. every trailing date already backfilled) that is fine: the predicate
    # below only cares whether run_date's own already-live state is why nothing
    # moved, which is the one case this issue is about.
    if not backfill_written and not live_written and already_live:
        result["auto_skipped"] = True
        result["skip_reason"] = "target date already live-published, immutable"
    return result


# ── Scorer: realized near-term performance ───────────────────────────────────────────


def _hit(label: str, fwd_return: float) -> bool | None:
    """Directional hit vs. the label's implied call. Buy/Strong Buy expect a positive
    forward return, Sell/Strong Sell a negative one; Neutral has no directional call
    under the published rules, so it contributes to ``n`` but not ``hit_rate``."""
    if label in ("Strong Buy", "Buy"):
        return fwd_return > 0
    if label in ("Strong Sell", "Sell"):
        return fwd_return < 0
    return None


def _round(x: float | None, n: int = 6) -> float | None:
    if x is None:
        return None
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    if x != x or x in (float("inf"), float("-inf")):
        return None
    return round(x, n)


def _spearman_ic(scores: list[float], returns: list[float]) -> float | None:
    """Per-date Spearman IC = Pearson correlation of RANKS — computed by hand (average
    ranks via ``pandas.Series.rank``, then ``numpy.corrcoef``) rather than
    ``Series.corr(method="spearman")``, which imports ``scipy`` (not a fleet
    dependency here). ``None`` when fewer than two pairs or either side is constant
    (an undefined correlation, not a zero one)."""
    import numpy as np
    import pandas as pd

    if len(scores) < 2 or len(scores) != len(returns):
        return None
    s = pd.Series(scores, dtype="float64")
    r = pd.Series(returns, dtype="float64")
    if s.nunique() < 2 or r.nunique() < 2:
        return None
    rank_s = s.rank(method="average").to_numpy()
    rank_r = r.rank(method="average").to_numpy()
    ic = float(np.corrcoef(rank_s, rank_r)[0, 1])
    return ic if ic == ic else None  # NaN check without importing math


def _seeded_permutation(values: list[float], seed_key: str) -> list[float]:
    seed = int(hashlib.sha256(seed_key.encode("utf-8")).hexdigest()[:16], 16)
    rng = random.Random(seed)
    shuffled = list(values)
    rng.shuffle(shuffled)
    return shuffled


def _extract_realized_rows(
    ledger_entries: dict[str, dict], series: dict[str, list], horizons: tuple[int, ...],
) -> dict[int, list[dict]]:
    """Per horizon, every ``{date, symbol, score, label, fwd_return, basis}`` row with a
    REALIZED forward return. Both the base and forward close are read from the CURRENT
    close_history (not the ledger's frozen ``close``), so a return is computed on one
    internally-consistent (possibly dividend-revised) basis rather than mixing a frozen
    rated value with a since-revised one — the ledger's own ``close`` stays the as-rated
    record (Gotcha per metron-ops#297).

    **A horizon is counted in REFERENCE-CALENDAR sessions, never in a symbol's own bars**
    (alpha-engine-config-I11549). The target of rating date ``d`` at horizon ``h`` is the
    session ``h`` places after ``d`` on ``_reference_calendar(series)`` — the same US
    calendar the ledger's dates are drawn from — and a symbol contributes a row only if it
    has a close on BOTH ``d`` and that target date. Counting ``idx + h`` over each symbol's
    own bars made the answer depend on which exchange the symbol lists on and on which of
    its bars happened to be published yet:

    * a non-US listing that trades on a US holiday (D05.SI, NOVN.SW, RMS.PA, SU.PA all
      carry a 2026-09-07 Labor Day bar) reached its "20th session" one US session early,
      so on 2026-09-23 the rating date 2026-08-26 — 19 US sessions back, horizon 20 NOT
      yet realized for the rest of the universe — got an IC over those four names alone
      (0.4). v1 published it; the shadow run, whose close_history lacked three of those
      four listings' day-D bars, did not, which is the extra ``ic_series`` entry on every
      same-day parity report;
    * a symbol with a missing bar had its next available bar used as the forward close,
      so a 2-session return was reported under horizon 1.

    Neither is a realized ``h``-session return, so both are now left out: a rating date
    whose horizon has not elapsed on the reference calendar yields no row for any symbol,
    and a symbol with no close on the exact target date yields no row for that date.
    """
    calendar = _reference_calendar(series)
    cal_pos = {d: i for i, d in enumerate(calendar)}
    # Per-symbol {date: close} over non-null closes, built once.
    sym_closes: dict[str, dict[str, float]] = {
        sym: {
            r[0]: float(r[1]) for r in rows
            if isinstance(r, (list, tuple)) and len(r) == 2 and r[1] is not None
        }
        for sym, rows in series.items()
    }

    rows_by_horizon: dict[int, list[dict]] = {h: [] for h in horizons}
    for date, entry in ledger_entries.items():
        pos = cal_pos.get(date)
        if pos is None:
            continue
        targets = {h: calendar[pos + h] for h in horizons if pos + h < len(calendar)}
        if not targets:
            continue
        basis = entry.get("basis")
        ratings = entry.get("ratings") or {}
        for sym, r in ratings.items():
            closes = sym_closes.get(sym)
            if not closes:
                continue
            base_close = closes.get(date)
            if base_close is None or base_close <= 0:
                continue
            for h, target in targets.items():
                fwd_close = closes.get(target)
                if fwd_close is None:
                    continue
                fwd_return = fwd_close / base_close - 1.0
                rows_by_horizon[h].append({
                    "date": date, "symbol": sym, "score": float(r["score"]),
                    "label": r["label"], "fwd_return": fwd_return, "basis": basis,
                })
    return rows_by_horizon


def _segment_matches(basis: str | None, segment: str) -> bool:
    return segment == "all" or basis == segment


def compute_rating_performance_from_ledger(
    ledger_entries: dict[str, dict], series: dict[str, list], *,
    rating_version: int = RATING_VERSION, horizons: tuple[int, ...] = HORIZONS,
    windows: tuple[int, ...] = WINDOWS, as_of_utc: str | None = None,
) -> dict:
    """Pure function (no S3): ``ledger_entries`` is ``{date: ledger_json}`` (as written
    by ``collect_rating_ledger``), ``series`` is the consolidated close_history's
    ``series`` map. Directly unit-testable — the oracle/random-score/no-lookahead tests
    call this without touching S3."""
    rows_by_horizon = _extract_realized_rows(ledger_entries, series, horizons)

    segments: dict[str, dict] = {}
    # (segment, horizon) -> [(date, ic)] at the LARGEST window, for ic_series.
    ic_pairs_at_max_window: dict[tuple[str, int], list[tuple[str, float]]] = {}
    max_window = max(windows)

    for segment in ("live", "backfill", "all"):
        seg_windows: dict[str, dict] = {}
        for window in windows:
            seg_horizons: dict[str, dict] = {}
            for horizon in horizons:
                rows = [r for r in rows_by_horizon[horizon] if _segment_matches(r["basis"], segment)]
                dates_with_data = sorted({r["date"] for r in rows})[-window:]
                dateset = set(dates_with_data)
                selected = [r for r in rows if r["date"] in dateset]

                # Same-date cross-sectional mean, for mean_excess.
                by_date: dict[str, list[dict]] = {}
                for r in selected:
                    by_date.setdefault(r["date"], []).append(r)
                date_mean = {
                    d: sum(x["fwd_return"] for x in rs) / len(rs) for d, rs in by_date.items()
                }

                buckets: dict[str, dict] = {}
                for label in LABELS:
                    rows_l = [r for r in selected if r["label"] == label]
                    n = len(rows_l)
                    if n == 0:
                        buckets[label] = {"n": 0, "mean_fwd": None, "hit_rate": None, "mean_excess": None}
                        continue
                    mean_fwd = sum(r["fwd_return"] for r in rows_l) / n
                    hits = [h for h in (_hit(label, r["fwd_return"]) for r in rows_l) if h is not None]
                    hit_rate = (sum(1 for h in hits if h) / len(hits)) if hits else None
                    mean_excess = sum(r["fwd_return"] - date_mean[r["date"]] for r in rows_l) / n
                    buckets[label] = {
                        "n": n, "mean_fwd": _round(mean_fwd), "hit_rate": _round(hit_rate),
                        "mean_excess": _round(mean_excess),
                    }

                sb, ss = buckets["Strong Buy"]["mean_fwd"], buckets["Strong Sell"]["mean_fwd"]
                spread = _round(sb - ss) if sb is not None and ss is not None else None

                ic_list: list[float] = []
                noise_list: list[float] = []
                date_ic_pairs: list[tuple[str, float]] = []
                for d, rs in sorted(by_date.items()):
                    scores_d = [r["score"] for r in rs]
                    returns_d = [r["fwd_return"] for r in rs]
                    ic = _spearman_ic(scores_d, returns_d)
                    if ic is not None:
                        ic_list.append(ic)
                        date_ic_pairs.append((d, ic))
                    permuted = _seeded_permutation(returns_d, f"{segment}:{window}:{horizon}:{d}")
                    noise_ic = _spearman_ic(scores_d, permuted)
                    if noise_ic is not None:
                        noise_list.append(noise_ic)

                ic_mean = _round(sum(ic_list) / len(ic_list)) if ic_list else None
                noise_floor_ic = _round(sum(noise_list) / len(noise_list)) if noise_list else None

                seg_horizons[str(horizon)] = {
                    "buckets": buckets,
                    "spread_strong_buy_minus_strong_sell": spread,
                    "ic_mean": ic_mean,
                    "ic_n_dates": len(ic_list),
                    "noise_floor_ic": noise_floor_ic,
                }
                if window == max_window:
                    ic_pairs_at_max_window[(segment, horizon)] = date_ic_pairs
            seg_windows[str(window)] = seg_horizons
        segments[segment] = seg_windows

    ic_series: list[dict] = []
    for horizon in horizons:
        pairs = sorted(ic_pairs_at_max_window.get(("all", horizon), []))[-250:]
        for d, ic in pairs:
            ic_series.append({"date": d, "horizon": horizon, "ic": _round(ic)})
    ic_series.sort(key=lambda x: (x["date"], x["horizon"]))

    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "as_of_utc": as_of_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rating_version": rating_version,
        "horizons": list(horizons),
        "windows": list(windows),
        "segments": segments,
        "ic_series": ic_series,
    }


def collect_rating_performance(
    *, bucket: str = mmd.DEFAULT_BUCKET, dry_run: bool = False, s3_client: Any = None,
) -> dict:
    """Read the ledger manifest + a bounded trailing slice of ledger dates + the
    consolidated close_history, recompute ``rating_performance.json`` and write it.
    Runs after ``collect_rating_ledger`` (needs today's ledger date already written)."""
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    manifest = mmd._read_json(s3_client, bucket, RATING_LEDGER_MANIFEST_KEY)
    dates = sorted(e["date"] for e in (manifest or {}).get("dates", []))
    if not dates:
        return {"status": "skipped", "reason": "empty ledger"}

    consolidated = mmd._read_json(s3_client, bucket, mmd.CONSOLIDATED_CLOSE_HISTORY_KEY)
    series = (consolidated or {}).get("series") or {}
    if not series:
        return {"status": "skipped", "reason": "no close_history"}

    tail_dates = dates[-_SCORER_TRAIL_DATES:]
    ledger_entries: dict[str, dict] = {}
    for d in tail_dates:
        obj = mmd._read_json(s3_client, bucket, f"{RATING_LEDGER_PREFIX}{d}.json")
        if obj:
            ledger_entries[d] = obj

    perf = compute_rating_performance_from_ledger(ledger_entries, series)

    if dry_run:
        return {"status": "ok_dry_run", "ledger_dates_used": len(ledger_entries)}
    try:
        mmd._write_json(s3_client, bucket, RATING_PERFORMANCE_KEY, perf)
    except Exception as e:  # fail loud to the phase registry
        logger.error("[technical_rating_ledger] performance write failed: %s", e)
        return {"status": "error", "error": str(e)}
    return {"status": "ok", "ledger_dates_used": len(ledger_entries)}
