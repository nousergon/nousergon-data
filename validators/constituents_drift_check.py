"""
validators/constituents_drift_check.py — Friday-Preflight detection of
index-membership-vs-ArcticDB constituents drift (close 5/23-SF P0 (g)).

Background:

  The 2026-05-23 SF FAILED at Research because BNY/P/SN were listed S&P
  members (Wikipedia was the membership source at the time) but missing
  from the ArcticDB universe — the constituents
  collector advanced the `latest_weekly.json` pointer AFTER the backfill
  ran, so the backfill saw last-week's constituents and skipped the new
  cohort. Friday-Preflight SF couldn't detect this directly because the
  Saturday constituents collector hadn't run yet.

  The fix: read membership DIRECTLY from any Friday-Preflight Lambda and
  diff it against the ArcticDB universe, with zero dependency on the
  Saturday collector cadence.

  MEMBERSHIP SOURCE — no longer Wikipedia. Since alpha-engine-config-I2812
  the ground truth is the SSGA SPDR daily holdings files (SPY / MDY), read
  through ``collectors.constituents._fetch_constituents``.
  Wikipedia is retained for GICS sector/sub-industry attestation only,
  and for nothing else: its community-edited pages lag real index changes
  by weeks in the removal direction, which was the root cause of
  I2703/I2812. The docstring and the
  alert text below both said "Wikipedia" long after the source moved —
  corrected 2026-08-21 under alpha-engine-config-I8094, because an alert
  that misnames its own upstream sends every triage session to the wrong
  system first.

  WHAT A HIT ACTUALLY THREATENS. Not ``ResearchPreflight``: that stage
  checks only ``macro.SPY`` freshness and cannot fail on ticker
  completeness. The reachable gate is ``fetch_price_data``'s 5% per-ticker
  error-rate ceiling in crucible-research, so the alert states the observed
  fraction against that ceiling rather than asserting a failure. See the
  comment above the message construction.

Usage:

  python -m validators.constituents_drift_check          # checks + alerts on diff
  python -m validators.constituents_drift_check --no-alert  # diagnostic, no SNS/Telegram
  python -m validators.constituents_drift_check --run-date 2026-08-22  # gate only if
                                                                       # that run
                                                                       # collected
  python -m validators.constituents_drift_check --max-stragglers 20  # allow up to N
                                                                      # index members
                                                                      # missing from
                                                                      # arctic before
                                                                      # firing alert

Exit code 0 on clean diff (or under-threshold drift), 1 on alert-worthy
drift. SF Catch on the WeeklySubstrateHealthCheck state turns exit-1 into
an alert.

Composes with [[feedback_no_silent_fails]]: the SSGA holdings files are
the upstream authority; if they list a ticker the ArcticDB universe lacks,
that's the exact failure surface that hit production on 5/23.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import boto3

from collectors.constituents import _fetch_constituents
from features.compute import _SKIP_TICKERS, _is_sector_etf

logger = logging.getLogger(__name__)

# Mirrored from crucible-research
# `data/fetchers/price_fetcher.py::_MAX_ERR_RATE`. This validator does not
# import crucible-research (separate repo, no dependency edge), so the value is
# duplicated deliberately and named here so the drift is greppable from both
# sides. It is used ONLY to phrase this alert's consequence clause — nothing
# here gates on it.
_RESEARCH_MAX_ERR_RATE = 0.05


#: The dated artifact Phase 1 writes only when it actually COLLECTS.
#: ``spot_data_phase1.sh --preflight-only`` (the Friday shell run) enters
#: ``DataPhase1`` and validates without collecting, so entered-stage
#: membership cannot separate the two — the produced artifact can.
_COLLECTION_ARTIFACT = "market_data/weekly/{run_date}/constituents.json"


def collection_ran(s3, bucket: str, run_date: str) -> bool | None:
    """Did THIS run_date's Phase 1 actually collect, or only preflight?

    Returns True/False, or ``None`` when the question could not be answered
    (no S3 client, denied, transport error). ``None`` is not "no": the caller
    keeps gating on an unanswered question, because excusing a drift on a
    failed probe is how a real gap becomes invisible.
    """
    key = _COLLECTION_ARTIFACT.format(run_date=run_date)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001
        response = getattr(exc, "response", None)
        code = (response or {}).get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        logger.warning(
            "collection_ran probe on s3://%s/%s raised (%s: %s) — the question is "
            "UNANSWERED, so the drift check keeps gating rather than excusing itself",
            bucket, key, type(exc).__name__, exc,
        )
        return None


def _open_universe_lib(bucket: str):
    """Open the ArcticDB universe library for read-only symbol listing."""
    from store.arctic_store import get_universe_lib
    return get_universe_lib(bucket)


def check_drift(
    *,
    bucket: str = "alpha-engine-research",
    max_stragglers: int = 0,
    alert: bool = True,
    alert_severity: str = "error",
    run_date: str | None = None,
    s3_client=None,
) -> dict:
    """Run the index-membership → ArcticDB constituents drift check.

    Args:
        bucket: S3 bucket holding the ArcticDB universe library.
        max_stragglers: number of index members allowed to be missing
            from ArcticDB before firing the alert. Default 0 (strict — any
            missing ticker fires). Set higher to tolerate known
            churn-in delay (e.g. the 1-Saturday backfill lag).
        alert: if True, fire an `nousergon_lib.alerts.publish` on drift.
            If False, return the diff without alerting (diagnostic mode).
        alert_severity: severity tag for the published alert.
        run_date: the pipeline run date this check is asserting over. When
            given, drift is GATING only if that run_date's Phase 1 actually
            collected (see :func:`collection_ran`). On a preflight-only run
            the live index has moved but nothing was collected to move
            ArcticDB with it, so the drift is real, expected, and not this
            run's failure — it is reported, not raised. Omit to gate
            unconditionally.
        s3_client: injected boto3 S3 client (tests).

    Returns:
        dict with keys: status (`ok` | `drift_detected` | `error`),
        membership_count, arctic_count, missing_from_arctic (list),
        only_in_arctic (list), within_threshold (bool).
    """
    try:
        (
            tickers, _sector_map, _sector_etf_map, _sub_industry_map,
            sp500_count, sp400_count, _weights,
        ) = _fetch_constituents()
    except Exception as exc:
        logger.exception("Index-membership constituents fetch failed")
        return {
            "status": "error",
            "error": str(exc),
            "stage": "membership_fetch",
        }

    membership_set = set(tickers)
    logger.info(
        "Index membership (SSGA holdings): %d tickers (S&P 500=%d, S&P 400=%d)",
        len(membership_set), sp500_count, sp400_count,
    )

    try:
        lib = _open_universe_lib(bucket)
        arctic_set = set(lib.list_symbols())
    except Exception as exc:
        logger.exception("ArcticDB universe list failed")
        return {
            "status": "error",
            "error": str(exc),
            "stage": "arctic_list",
        }

    # Strip macro/sector members and known-non-stock tickers from the
    # comparison surface — the universe-write set is
    # `membership ∩ ¬_SKIP_TICKERS ∩ ¬sector_etfs` per builders/backfill.py.
    comparable_wiki = {
        t for t in membership_set
        if t not in _SKIP_TICKERS and not _is_sector_etf(t)
    }
    comparable_arctic = {
        t for t in arctic_set
        if t not in _SKIP_TICKERS and not _is_sector_etf(t)
    }

    missing_from_arctic = sorted(comparable_wiki - comparable_arctic)
    only_in_arctic = sorted(comparable_arctic - comparable_wiki)

    logger.info(
        "Drift summary: missing_from_arctic=%d (cap=%d), only_in_arctic=%d "
        "(prune candidates)",
        len(missing_from_arctic), max_stragglers, len(only_in_arctic),
    )

    within_threshold = len(missing_from_arctic) <= max_stragglers

    # ── is this run allowed to be judged on it? (alpha-engine-config-I8094) ──
    #
    # The population changes when S&P reconstitutes, mid-week. The pipeline
    # absorbs that on its own: `collectors/prices.py::_find_stale_fast` counts
    # a ticker with no parquet as stale and fetches its 10y history, then
    # `builders/backfill.py` (Phase 1 step 8, passed the run_date so it reads
    # THIS week's constituents rather than the not-yet-advanced pointer)
    # writes its ArcticDB row. Measured 2026-08-16, the run after a
    # reconstitution: missing_from_arctic=0, only_in_arctic=0.
    #
    # What cannot absorb it is a run that does not collect. The Friday shell
    # run's DataPhase1 invokes `spot_data_phase1.sh --preflight-only`, so it
    # ENTERS the stage and produces nothing — entered-stage membership cannot
    # tell the two apart, which is why this keys on the produced artifact.
    # Measured on execution friday-shell-2026-08-21-eod-2026-08-21-1787342451:
    # SUI and VMRK joined the index after the 2026-08-18 collection, so the
    # check failed the gate on a gap the run had no mechanism to close, took
    # the whole weekly pipeline to DEGRADED, and asked a human to hand-run a
    # backfill the next scheduled run would have done by itself.
    collected: bool | None = None
    if run_date:
        collected = collection_ran(s3_client or boto3.client("s3"), bucket, run_date)
    gating = within_threshold or collected is not False

    if not within_threshold and not gating:
        status = "drift_deferred"
    elif not within_threshold:
        status = "drift_detected"
    else:
        status = "ok"

    result = {
        "status": status,
        "membership_count": len(membership_set),
        "arctic_count": len(arctic_set),
        "missing_from_arctic": missing_from_arctic,
        "only_in_arctic": only_in_arctic,
        "max_stragglers": max_stragglers,
        "within_threshold": within_threshold,
        "run_date": run_date,
        "collection_ran": collected,
        "gating": gating,
    }

    if not within_threshold and not gating:
        logger.info(
            "Constituents drift DEFERRED: %d ticker(s) %s joined the index since the "
            "last collection, and run_date=%s did not collect (no %s) — the next "
            "collecting run fetches their history and writes their ArcticDB row "
            "without any manual backfill. Reported, not raised.",
            len(missing_from_arctic), missing_from_arctic[:20], run_date,
            _COLLECTION_ARTIFACT.format(run_date=run_date),
        )

    if not within_threshold and gating and alert:
        try:
            from nousergon_lib import alerts  # noqa: PLC0415
        except ImportError as exc:
            logger.warning(
                "alerts publish skipped — nousergon_lib.alerts unavailable: %s",
                exc,
            )
            return result
        # Truncate the missing list at 20 for the alert message so we don't
        # blow the SNS subject length on a worst-case 50-ticker drift.
        preview = missing_from_arctic[:20]
        suffix = (
            f" ... +{len(missing_from_arctic) - 20} more"
            if len(missing_from_arctic) > 20 else ""
        )
        # The consequence clause names the gate that can ACTUALLY fail on this
        # input, and the margin it has (alpha-engine-config-I8094).
        #
        # It used to say "Saturday SF will likely fail at Research preflight".
        # Both halves were wrong, and the pairing turned routine index
        # reconstitution into an overnight-urgency page:
        #
        #   * `crucible-research/preflight.py::_check_arcticdb_universe` does
        #     not look at ticker completeness at all — it asserts `macro.SPY`
        #     is fresh within 5 trading days. No number of missing
        #     constituents can fail it.
        #   * The gate that CAN fail is
        #     `crucible-research/data/fetchers/price_fetcher.py::fetch_price_data`,
        #     which raises `PriceFetchError` above `_MAX_ERR_RATE = 0.05`. On
        #     2026-08-21 the drift was 2 of 903 comparable tickers — 0.2%
        #     against a 5% ceiling, two orders of magnitude of headroom — and
        #     the alert still told the reader the pipeline was about to fail.
        #
        # So the fraction is computed and stated. A reader deciding whether to
        # act tonight needs the margin, not an adjective.
        err_fraction = len(missing_from_arctic) / max(len(comparable_wiki), 1)
        consequence = (
            f"Blast radius: {len(missing_from_arctic)} of "
            f"{len(comparable_wiki)} comparable tickers = {err_fraction:.2%} "
            f"against the {_RESEARCH_MAX_ERR_RATE:.0%} per-ticker error-rate "
            f"ceiling in crucible-research "
            f"data/fetchers/price_fetcher.py::fetch_price_data "
            f"(_MAX_ERR_RATE). "
        )
        if err_fraction > _RESEARCH_MAX_ERR_RATE:
            consequence += (
                "OVER the ceiling — Research WILL raise PriceFetchError "
                "unless the Saturday backfill covers these first."
            )
        else:
            consequence += (
                "UNDER the ceiling — Research will not hard-fail on this "
                "alone. weekly_collector.py Phase 1 runs constituents -> "
                "historical_constituents -> prices.collect -> "
                "builders.backfill BEFORE ResearchPredictorParallel, and "
                "prices.collect refreshes any ticker with no parquet, so the "
                "scheduled Saturday run is expected to absorb this. A ticker "
                "still missing AFTER that run is the finding worth paging on."
            )
        message = (
            f"Friday-Preflight constituents drift detected: "
            f"{len(missing_from_arctic)} SSGA-listed S&P ticker(s) "
            f"missing from ArcticDB universe "
            f"(threshold={max_stragglers}). "
            f"Missing: {', '.join(preview)}{suffix}. "
            f"{consequence} "
            f"Membership source is the SSGA SPY/MDY daily holdings files "
            f"(alpha-engine-config-I2812); Wikipedia supplies GICS sector "
            f"attestation only. Common benign cause: an official index "
            f"add/rename in the last few sessions. "
            f"Tracker: alpha-engine-config-I8094."
        )
        try:
            publish_result = alerts.publish(
                message,
                severity=alert_severity,
                source="alpha-engine-data/validators/constituents_drift_check.py",
                dedup_key=f"constituents_drift_{len(missing_from_arctic)}",
                dedup_window_min=720,  # 12h — one alert per dry-pass window
            )
            logger.info(
                "Drift alert publish: sns_ok=%s telegram_ok=%s any_ok=%s",
                publish_result.sns.ok,
                publish_result.telegram.ok,
                publish_result.any_ok,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Drift alert publish failed: %s", exc)

    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Friday-Preflight constituents drift check",
    )
    parser.add_argument("--bucket", default="alpha-engine-research")
    parser.add_argument("--max-stragglers", type=int, default=0,
                        help="index members allowed missing from ArcticDB")
    parser.add_argument("--no-alert", action="store_true",
                        help="diagnostic mode — no SNS/Telegram alert on drift")
    parser.add_argument(
        "--alert-severity", default="error",
        choices=["info", "warn", "warning", "error", "critical"],
    )
    parser.add_argument(
        "--run-date", default=None,
        help=(
            "pipeline run date this check asserts over. When given, drift is "
            "gating only if that run_date's Phase 1 actually collected — a "
            "preflight-only run cannot have moved ArcticDB to match an index "
            "that reconstituted after the last collection "
            "(alpha-engine-config-I8094)."
        ),
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    result = check_drift(
        bucket=args.bucket,
        max_stragglers=args.max_stragglers,
        alert=not args.no_alert,
        alert_severity=args.alert_severity,
        run_date=args.run_date,
    )

    if result["status"] == "error":
        logger.error("Drift check failed at stage=%s: %s",
                     result.get("stage"), result.get("error"))
        return 2

    if result["status"] == "drift_deferred":
        logger.warning(
            "DRIFT DEFERRED: %d ticker(s) missing from ArcticDB (%s), but "
            "run_date=%s did not collect — the next collecting run absorbs the "
            "index change with no manual backfill. Not failing this run.",
            len(result["missing_from_arctic"]),
            result["missing_from_arctic"][:20],
            result["run_date"],
        )
        return 0

    if result["status"] == "drift_detected":
        logger.error(
            "DRIFT DETECTED: %d index member(s) missing from ArcticDB "
            "(threshold=%d). Missing: %s",
            len(result["missing_from_arctic"]),
            result["max_stragglers"],
            result["missing_from_arctic"][:20],
        )
        return 1

    logger.info("Drift check OK: arctic covers every listed index member")
    return 0


if __name__ == "__main__":
    sys.exit(main())
