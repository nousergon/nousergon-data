"""alpha-engine-eod-precondition-probe — verify-by-artifact EOD reconcile gate
(alpha-engine-config-I2702 deliverable #1).

Replaces the EOD Step Function's old ``CheckSkipEODReconcile`` flag test
(``$.data_spot_error IsPresent``) with a probe of the REAL precondition:
does ``run_date``'s SPY close actually exist in ArcticDB? The flag test was
proven wrong live on 2026-07-15 (config-I2699 comment): the SF's data-spot
POLL timed out and stamped ``$.data_spot_error`` while the collector was
STILL RUNNING — it went on to finish rc=0 and write the row, but
``CheckSkipEODReconcile`` had already committed to skipping reconcile based
on the stale launch-phase flag. A flag records "did the launch/poll path see
an error", not "does the artifact exist" — those diverged live.

WHY THIS LAMBDA DOES NOT QUERY ArcticDB DIRECTLY: Brian's 2026-07-08
config#1787 "Option-B" ruling (recorded in
alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml) explicitly rejected
a first-class ArcticDB backend for Lambda-side freshness probes as premature
generalization for one consumer — it would fork a 3-repo-pinned lockstep
(ArtifactSpec schema in nousergon-lib + the registry validator + this repo's
contract tests) and require bundling the ``arcticdb`` package (+ likely VPC
networking) into a Lambda deploy image. The ruling's alternative — and the
one this Lambda follows — is: the ArcticDB PRODUCER (``builders/daily_append.py``)
already does producer-side READBACK verification (queries ArcticDB itself,
inside the EC2/spot box that has the ``arcticdb`` package + IAM role) and,
on success, writes a small unconditional S3 sentinel
(``feature_store/_macro_freshness.json``) recording which run_date + which
keys (incl. "SPY") were VERIFIED present. This Lambda reads that sentinel —
an ordinary S3 GetObject, zero new backend code, zero VPC — which is
*stronger* than a live "does a row happen to exist" query: the sentinel is
only written after the producer has already confirmed the row is queryable,
so the probe is checking a durable, already-verified fact rather than racing
a read against a write.

Grep-anchor for the producer side: ``FEATURE_STORE_FRESHNESS_SENTINEL_KEY`` /
``MACRO_FRESHNESS_SENTINEL_KEY`` in ``builders/daily_append.py``.

ADDITIVE universe-close check (config#3237): the macro sentinel above only
proves SPY's macro-benchmark close (from the ``macro`` ArcticDB library) is
verified for run_date — it says nothing about the OTHER held positions'
closes, which ``executor/eod_reconcile.py`` reads from the ``universe``
library (SPY is ALSO a `universe` member as a held position, per
alpha-engine-data#245, but the other held tickers — ADBE/AMD/COIN/TWLO as of
2026-07-21 — never touch ``macro`` at all). 2026-07-21 (config#3236): the
`universe` append failed 100% while the macro sentinel stayed present, so
this probe reported ``precondition_met: true`` and reconcile hard-crashed
instead of routing to the self-heal loop. Fix: this probe now ALSO reads
``feature_store/_universe_close_freshness.json`` (written by
``_write_universe_close_freshness_sentinel`` in ``builders/daily_append.py``,
config#3237) and requires it too — ``precondition_met`` is the AND of both
checks, since eod_reconcile.py's per-run needs are the union of both
libraries' run_date rows.

The EOD ASL calls this Lambda twice per healing cycle: once before
``CheckSkipEODReconcile`` (the initial gap detection) and once per heal-loop
iteration after a re-dispatched workload completes (``HealReProbe``) — see
``infrastructure/step_function_eod.json``. Every call is a fresh S3 read: no
caching, no trusting a launch-phase flag, per the issue's "verify by artifact,
never by stale flag" mandate.

Also computes the heal-loop deadline (deliverable #5 "poll budgets sized to
reality" / deliverable #3(d) "page ... by a deadline"): 09:00 UTC on the day
AFTER ``run_date``, comfortably past both the EOD pipeline's normal window and
a chronic-gap-heal-extended collector run, and well before the next trading
day's pre-open pipeline needs a verdict. Returned on every call so the ASL
never needs its own date-math (Step Functions' JSONPath dialect has no
date-arithmetic intrinsic) — the Lambda is the one place with real Python
``datetime``.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("EOD_PROBE_BUCKET", "alpha-engine-research")
MACRO_FRESHNESS_SENTINEL_KEY = os.environ.get(
    "EOD_PROBE_SENTINEL_KEY", "feature_store/_macro_freshness.json"
)
# The precondition eod_reconcile.py's _spy_close hard-depends on — SPY must be
# among the readback-verified keys for run_date. Mirrors daily_append.py's
# macro_keys list (SPY is always index 0 there); we only require SPY here
# because that is the ONE key _spy_close reads — a probe stricter than the
# actual consumer would produce false negatives on genuinely-fine days.
REQUIRED_KEY = os.environ.get("EOD_PROBE_REQUIRED_KEY", "SPY")
# config#3237: the `universe`-library counterpart to the macro sentinel
# above — required ADDITIVELY (see module docstring "ADDITIVE universe-close
# check").
UNIVERSE_FRESHNESS_SENTINEL_KEY = os.environ.get(
    "EOD_PROBE_UNIVERSE_SENTINEL_KEY", "feature_store/_universe_close_freshness.json"
)
# Sanity floor, not a per-ticker membership check — this Lambda has no
# visibility into which specific tickers are currently held (that's
# executor-side SQLite state), so it cannot require particular symbols the
# way REQUIRED_KEY does for the single-symbol macro sentinel. Requiring a
# non-zero verified count still catches the 2026-07-21 class (100% universe
# append failure => the sentinel is never written at all => precondition_met
# is False on absence alone, same as the macro check) plus any producer
# regression that writes the sentinel with a suspicious zero count.
UNIVERSE_MIN_VERIFIED_COUNT = int(os.environ.get("EOD_PROBE_UNIVERSE_MIN_VERIFIED_COUNT", "1"))
# Heal-loop deadline: 09:00 UTC the day after run_date (deliverable #3(d)).
HEAL_DEADLINE_HOUR_UTC = int(os.environ.get("EOD_PROBE_HEAL_DEADLINE_HOUR_UTC", "9"))


def _heal_deadline_iso(run_date: str) -> str:
    """09:00 UTC on the calendar day after ``run_date`` (YYYY-MM-DD)."""
    d = datetime.strptime(run_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    deadline = (d + timedelta(days=1)).replace(
        hour=HEAL_DEADLINE_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    return deadline.strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_sentinel(s3client, bucket: str, key: str) -> dict | None:
    """Read + parse the macro-freshness sentinel. Returns None iff the object
    genuinely does not exist (NoSuchKey/404) — a legitimate "never written"
    precondition-not-met state, not an error. ANY OTHER S3 failure (IAM
    drift, throttling, malformed JSON) RAISES — fail-loud per
    feedback_no_silent_fails: a probe that silently treats "I couldn't check"
    the same as "verified absent" would non-deterministically skip a
    perfectly good reconcile."""
    try:
        obj = s3client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return None
        raise
    body = obj["Body"].read()
    return json.loads(body)


def _evaluate(sentinel: dict | None, run_date: str) -> tuple[bool, str]:
    """Returns (precondition_met, reason)."""
    if sentinel is None:
        return False, f"no macro-freshness sentinel found at s3://{BUCKET}/{MACRO_FRESHNESS_SENTINEL_KEY}"
    sentinel_run_date = sentinel.get("run_date")
    if sentinel_run_date != run_date:
        return False, (
            f"sentinel run_date={sentinel_run_date!r} does not match requested "
            f"run_date={run_date!r} (stale or wrong-day sentinel)"
        )
    verified_keys = set(sentinel.get("verified_keys") or [])
    if REQUIRED_KEY not in verified_keys:
        return False, (
            f"sentinel for run_date={run_date} does not list {REQUIRED_KEY!r} "
            f"among verified_keys={sorted(verified_keys)}"
        )
    return True, f"{REQUIRED_KEY} verified present for run_date={run_date}"


def _evaluate_universe(sentinel: dict | None, run_date: str) -> tuple[bool, str]:
    """Returns (precondition_met, reason) for the universe-close sentinel
    (config#3237) — see module docstring "ADDITIVE universe-close check"."""
    if sentinel is None:
        return False, f"no universe-close-freshness sentinel found at s3://{BUCKET}/{UNIVERSE_FRESHNESS_SENTINEL_KEY}"
    sentinel_run_date = sentinel.get("run_date")
    if sentinel_run_date != run_date:
        return False, (
            f"universe sentinel run_date={sentinel_run_date!r} does not match requested "
            f"run_date={run_date!r} (stale or wrong-day sentinel)"
        )
    verified_ticker_count = sentinel.get("verified_ticker_count") or 0
    if verified_ticker_count < UNIVERSE_MIN_VERIFIED_COUNT:
        return False, (
            f"universe sentinel for run_date={run_date} verified_ticker_count="
            f"{verified_ticker_count} below floor {UNIVERSE_MIN_VERIFIED_COUNT}"
        )
    return True, f"universe verified_ticker_count={verified_ticker_count} present for run_date={run_date}"


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function Task handler. ``event`` = ``{"run_date": "YYYY-MM-DD"}``.

    Returns::

        {
          "precondition_met": bool,     # AND of the macro AND universe checks
          "reason": str,                # combined, names which check(s) failed
          "run_date": str,
          "deadline_iso": str,          # 09:00 UTC day-after-run_date
          "past_deadline": bool,        # now() > deadline_iso
          "sentinel": dict | None,          # raw macro sentinel for forensics
          "universe_sentinel": dict | None, # raw universe sentinel for forensics
        }

    Raises on any non-"absent" S3 failure — the ASL Task's own Catch decides
    the fail-safe posture (this repo's EOD ASL treats a probe-infra failure
    as "proceed to EODReconcile, let the proven `_spy_close` hard-fail be the
    backstop" rather than silently skipping a possibly-fine reconcile — see
    the ASL's ``ProbeEODReconcilePrecondition`` Catch comment).
    """
    run_date = str((event or {}).get("run_date") or "").strip()
    if not run_date:
        raise ValueError("event.run_date is required (YYYY-MM-DD)")

    s3client = boto3.client("s3", region_name=REGION)
    sentinel = _read_sentinel(s3client, BUCKET, MACRO_FRESHNESS_SENTINEL_KEY)
    macro_met, macro_reason = _evaluate(sentinel, run_date)

    universe_sentinel = _read_sentinel(s3client, BUCKET, UNIVERSE_FRESHNESS_SENTINEL_KEY)
    universe_met, universe_reason = _evaluate_universe(universe_sentinel, run_date)

    precondition_met = macro_met and universe_met
    if precondition_met:
        reason = f"macro: {macro_reason}; universe: {universe_reason}"
    else:
        # Name only the failing check(s) — a reader shouldn't have to parse
        # a passing check's reason to find the actual blocker.
        failures = []
        if not macro_met:
            failures.append(f"macro: {macro_reason}")
        if not universe_met:
            failures.append(f"universe: {universe_reason}")
        reason = "; ".join(failures)

    deadline_iso = _heal_deadline_iso(run_date)
    now = datetime.now(timezone.utc)
    deadline = datetime.strptime(deadline_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    past_deadline = now > deadline

    logger.info(
        "precondition probe run_date=%s precondition_met=%s reason=%s past_deadline=%s",
        run_date, precondition_met, reason, past_deadline,
    )
    return {
        "precondition_met": precondition_met,
        "reason": reason,
        "run_date": run_date,
        "deadline_iso": deadline_iso,
        "past_deadline": past_deadline,
        "sentinel": sentinel,
        "universe_sentinel": universe_sentinel,
    }
