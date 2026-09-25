"""
Lambda handler for the WeeklyPreflight pre-spend gate —
invoked by `ne-weekly-freshness-pipeline` before AcquireMutex.

Runs the sf_preflight checks against live AWS state and returns a pass/fail
verdict. Failures halt the pipeline; a pass proceeds to the mutex + spot
launch.

The Lambda runs read-only API calls (IAM SimulatePrincipalPolicy, Lambda
GetFunctionConfiguration, CloudWatch GetMetricStatistics, Step Functions
DescribeStateMachine) and zero-cost static analysis.

Check selection is by CAPABILITY, not by hope. This Lambda declares
``sf_preflight.LAMBDA_CAPABILITIES`` (the AWS control plane and nothing
else); checks needing ArcticDB, the repo's collector modules, a Polygon key,
or sibling checkouts on local disk are reported status="skip" and excluded
from the failure count. Before 2026-08-10 this docstring claimed those checks
"gracefully skip" while the handler in fact ran the FULL profile — they
returned status="fail" (ModuleNotFoundError / "not checked out as sibling"),
which would have halted the Saturday pipeline by construction.

Usage:
    Invoked by Step Functions. Returns:
    {"status": "OK", "has_violation": false}                          → proceed, fully observed
    {"status": "DEGRADED", "has_violation": false, "degraded": true}  → proceed, gap disclosed
    {"status": "FAIL", "has_violation": true, ...}                    → halt via ExtractWeeklyPreflightError

alpha-engine-config-I11112: a Lambda-eligible run of 5/15 checks (10 skipped
for missing CAP_ARCTIC/CAP_REPO_MODULES/CAP_POLYGON/CAP_CHECKOUT) previously
returned status="OK" — a skip is not a pass (principles.md §2.7), and this
Lambda cannot see the two-thirds of the check suite its environment lacks.
DEGRADED is the honest middle state: ``has_violation`` stays the SOLE switch
WeeklyPreflightGate reads to halt the run (a REQUIRED check that could not
run is this probe's OWN degradation, not a confirmed violation of the system
it is probing — sf-pipeline-policy.md §5's pre-spend-gate-probe carve-out:
"the probe's own failure must not halt the run, but routes through a
degraded flag plus alert, never silently"). A required check that actually
RAN and found a violation still hard-fails via has_violation, unchanged.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
from dataclasses import asdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Lambda-specific bucket override (not the same as the data-plane bucket,
# since this Lambda reads the SF definition, not the research data).
_SF_DEFINITION_BUCKET = os.environ.get("SF_DEFINITION_BUCKET", "alpha-engine-research")
_WEEKLY_SF_NAME = os.environ.get("WEEKLY_SF_NAME", "ne-weekly-freshness-pipeline")


def _resolve_run_dates(event: dict) -> dict:
    """Calendar date in, trading day out. Pure NYSE arithmetic, no AWS calls.

    ``alpha-engine-config-I8809``. One weekly cycle was written across TWO S3
    date partitions because ``InitializeInput`` stamped the calendar date and
    each consumer then decided for itself whether to normalize. Measured on
    the 2026-08-22 cycle: 28 ``_stage_coverage`` verdicts under ``2026-08-21``
    and 11 under ``2026-08-22``.

    **``resolve_trading_day``, not ``now_dual()``** — and the difference is not
    cosmetic. ``resolve_trading_day(d)`` is *the most recent NYSE trading day
    on or before ``d``*; ``now_dual().trading_day`` is *the last session fully
    CLOSED at this instant*. For a Friday-afternoon manual run those disagree
    (Friday vs Thursday). Every downstream normalizer in the fleet —
    ``crucible-backtester/infrastructure/_spot_common.sh``,
    ``crucible-research``'s signals-envelope and ChallengerShadow handlers, the
    evaluator, the Director — calls ``resolve_trading_day``, and it is
    idempotent by contract. Matching it is what makes this a normalization
    rather than a third partition.

    **Raises rather than degrades on a missing input.** A silent fall-back to
    "today" would key an entire cycle off the Lambda's own clock instead of the
    execution's, which is the same class of defect as the split itself. The
    state machine's Catch turns the raise into a NAMED fail-open
    (``NormalizeRunDatesDegraded``) that keeps the calendar value AND says so
    in ``run_date_family``.
    """
    calendar_date = str((event or {}).get("calendar_date") or "").strip()
    if not calendar_date:
        raise ValueError(
            "resolve_run_dates: calendar_date is REQUIRED — it is the execution's own "
            "date($$.Execution.StartTime), and defaulting it to this Lambda's clock "
            "would key the cycle off the wrong machine (alpha-engine-config-I8809)"
        )

    from krepis.dates import resolve_trading_day

    trading_day = resolve_trading_day(calendar_date)
    return {
        "calendar_date": calendar_date,
        "trading_day": trading_day,
        # Named on BOTH polarities (sf-pipeline-policy.md §2.3a rule 3): the
        # state machine seeds run_date_family='calendar_date' and this is what
        # upgrades it, so a run that never reached here is distinguishable from
        # one that did.
        "run_date_family": "trading_day",
        "normalized": trading_day != calendar_date,
    }


# ── Preflight-assertion metrics (alpha-engine-config-I11112 deliverable 4) ───
#
# THE DEFECT THIS CLOSES. On 2026-09-19 this gate ran 5 of 15 assertions and
# returned `status: OK, warn_count: 0`. `skip_count: 10` was present in the
# Payload and NOTHING KEYED ON IT. The counts existed for exactly one reader:
# a human opening that one execution's `weekly_preflight_result.Payload` in
# the Step Functions console, after the fact, knowing to look.
#
# A count that lives only inside one execution's payload is not observable
# (principles.md §2.7 / observability-policy.md §8.3). As a metric SERIES,
# "the preflight quietly stopped running checks" is a step change on a graph
# and an alarmable condition; as a payload field it is an archaeology task.
#
# NO IAM CHANGE, therefore no operator step: the role's existing
# `PutAlphaEngineMetrics` statement already allows `cloudwatch:PutMetricData`
# under the `AlphaEngine/*` namespace condition, so this deploys by the merge
# button alone (`lambda-deploy-on-merge` -> `deploy.sh`, code-only path).
#
# BEST-EFFORT BY CONSTRUCTION, and this is the one legitimate swallow here:
#   (a) failure mode swallowed — a `PutMetricData` throttle/permission/network
#       error inside the pre-spend gate;
#   (b) the primary deliverable (the gate's verdict) is untouched: an observer
#       that can change the outcome of the thing it observes is a new failure
#       mode bolted onto the one it reports — the same rule
#       `_assert_stage_coverage` below already follows;
#   (c) recording surface — a loud `ERROR` line in
#       `/aws/lambda/alpha-engine-weekly-preflight`, plus the ABSENCE of the
#       series itself, which is precisely what the `cloudwatch-metrics`
#       console adapter renders as UNREPORTED rather than green.
_METRIC_NAMESPACE = os.environ.get("PREFLIGHT_METRIC_NAMESPACE", "AlphaEngine/WeeklyPreflight")
# The dimension VALUE is the Lambda's own function name, deliberately: the
# console's `cloudwatch-metrics` adapter uses the dimension value verbatim as
# the component id (§3.6, never slug-minted), and `alpha-engine-weekly-
# preflight` is already a row in nous-ergon-ops' observability registry. So
# this series MERGES onto the component that already exists (§2.5) instead of
# rendering a second, unregistered row for the same thing.
_METRIC_PROFILE = os.environ.get("PREFLIGHT_METRIC_PROFILE", "alpha-engine-weekly-preflight")


def _emit_preflight_metrics(
    *,
    status: str,
    ran_count: int,
    skip_count: int,
    warn_count: int,
    fail_count: int,
    required_skip_count: int,
    declared_count: int,
) -> dict:
    """Publish this invocation's assertion counts as a CloudWatch series.

    ``ChecksRan`` is deliberately the metric a console adapter reads as
    "invocations": zero is NOT green. `AssertionsDeclared` alongside it is
    what makes `ran == declared` checkable by a reader who does not know how
    many assertions there are supposed to be this week — the 2026-09-19 run
    reported neither, so "5 of 15" was not a number anyone could see.
    """
    try:
        import boto3

        boto3.client("cloudwatch").put_metric_data(
            Namespace=_METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    "Dimensions": [{"Name": "Profile", "Value": _METRIC_PROFILE}],
                    "Value": float(value),
                    "Unit": "Count",
                }
                for name, value in (
                    ("ChecksRan", ran_count),
                    ("ChecksSkipped", skip_count),
                    ("ChecksWarned", warn_count),
                    ("ChecksFailed", fail_count),
                    ("RequiredChecksSkipped", required_skip_count),
                    ("AssertionsDeclared", declared_count),
                )
            ],
        )
        return {"emitted": True, "namespace": _METRIC_NAMESPACE, "status": status}
    except Exception as exc:  # noqa: BLE001 - see (a)/(b)/(c) above
        print(
            f"ERROR: preflight metric emission failed (status={status}, "
            f"ran={ran_count}, skipped={skip_count}, required_skipped="
            f"{required_skip_count}): {exc}"
        )
        return {"emitted": False, "error": str(exc), "status": status}


def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler for the weekly preflight.

    Event payload (from Step Functions):
        bucket: str (optional, S3 bucket override)
        sf_name: str (optional, state machine name override)

    Returns:
        dict with status, has_violation, and detailed results.
    """
    # alpha-engine-config-I8809 — the weekly graph's ONE date normalization.
    #
    # `NormalizeRunDates` invokes this function with an explicit action, before
    # any spend, to turn the execution's CALENDAR date into the cycle's TRADING
    # day. It returns before the preflight's first AWS call: this path performs
    # pure NYSE-calendar arithmetic and touches nothing.
    #
    # WHY HERE rather than in a new function: `InitializeInput` is a Pass and
    # States intrinsics carry no NYSE calendar, so the normalization needs a
    # Lambda. This one already exists, already pins nousergon-lib, is already
    # invoked by this same state machine, and already auto-deploys on merge
    # (`deploy-weekly-preflight.yml`). A NEW function would need an IAM role
    # bootstrap — an operator step a PR cannot perform, and therefore a PR that
    # is not deployable by the merge button alone.
    #
    # SAFE against the real gate: `WeeklyPreflight` passes no explicit Payload,
    # so `event` there IS the whole state input, which carries no `action` key.
    # `event.get("action")` is None on that path and the preflight below runs
    # byte-identically to before.
    if (event or {}).get("action") == "resolve_run_dates":
        return _resolve_run_dates(event)

    # Captured at handler ENTRY: the stage-coverage window must predate any
    # write this invocation makes, or it would be trivially satisfied by it
    # (alpha-engine-config-I7214).
    started = datetime.now(timezone.utc)
    bucket = event.get("bucket", _SF_DEFINITION_BUCKET)
    sf_name = event.get("sf_name", _WEEKLY_SF_NAME)

    try:
        # Import sf_preflight — all heavy imports are deferred inside
        # individual check functions, so this never fails at module level.
        import sf_preflight as sfp  # type: ignore[import-untyped]
    except ImportError as exc:
        # I11112: emit the zero-series too. A gate that could not even import
        # its checks is the LOUDEST case of "ran nothing", and it is the one
        # case where no payload field can say so — the console must see
        # ChecksRan=0 rather than an absence it cannot distinguish from a
        # weekend with no run.
        _emit_preflight_metrics(
            status="ERROR", ran_count=0, skip_count=0, warn_count=0,
            fail_count=0, required_skip_count=0, declared_count=0,
        )
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"sf_preflight import failed: {exc}",
            "traceback": traceback.format_exc(),
        }

    # alpha-engine-config-I7443: the SF Task passes no explicit Payload, so
    # `event` IS the whole state input — run_date and every skip_* flag are
    # already here. Forwarding them lets check_skip_flag_artifact_coherence
    # assert that each skip CLAIM is backed by a real artifact for this
    # run_date, before AcquireMutex and before any spot dispatch. Absent
    # (a bare {} test invoke) => that check reports "nothing claimed", never
    # a failure: this gate must not start halting the pipeline over a payload
    # shape it previously ignored.
    run_date = event.get("run_date")
    # alpha-engine-config-I8809: $.run_date is the cycle's TRADING day past
    # NormalizeRunDates; check_skip_flag_artifact_coherence compares an S3
    # LastModified and needs the wall-clock day instead.
    calendar_date = event.get("calendar_date")
    skip_flags = {k: v for k, v in event.items() if k.startswith("skip_")}

    try:
        n_fail, results = sfp.run_preflight(
            bucket=bucket,
            capabilities=sfp.LAMBDA_CAPABILITIES,
            run_date=run_date,
            skip_flags=skip_flags,
        )
    except Exception as exc:
        _emit_preflight_metrics(
            status="ERROR", ran_count=0, skip_count=0, warn_count=0,
            fail_count=0, required_skip_count=0,
            declared_count=len(getattr(sfp, "CHECKS", ()) or ()),
        )
        return {
            "status": "ERROR",
            "has_violation": True,
            "error": f"run_preflight raised: {exc}",
            "traceback": traceback.format_exc(),
        }

    # I11112: sf_preflight.summarize_results is the SOLE source of the
    # run/skip/warn/fail/required-skip counts — this handler used to compute
    # them inline and never checked whether a skip was REQUIRED, which is
    # exactly how "5 of 15 ran" rendered as status="OK".
    summary = sfp.summarize_results(results)
    result_dicts = summary["result_dicts"]
    fail_results = summary["fail_results"]
    warn_results = summary["warn_results"]
    skip_results = summary["skip_results"]
    ran_count = summary["ran_count"]
    required_skip_count = summary["required_skip_count"]
    required_skip_names = summary["required_skip_names"]
    # alpha-engine-config-I11566: a check BLOCKED by an upstream that did not
    # produce its input did not run. Beside a fail it is already explained by
    # that fail (and never counted as a second one); on its own it is an
    # unobserved required check, so it degrades exactly like a required skip.
    # `.get` so a sf_preflight predating the field still packages cleanly.
    blocked_count = summary.get("blocked_count", 0)
    blocked_names = summary.get("blocked_names", [])
    declared_count = len(getattr(sfp, "CHECKS", ()) or ()) or len(result_dicts)

    def _metrics(status: str) -> dict:
        """I11112 deliverable 4 — one call per terminal branch, so the series
        is complete by construction rather than by remembering."""
        return _emit_preflight_metrics(
            status=status,
            ran_count=ran_count,
            skip_count=len(skip_results),
            warn_count=len(warn_results),
            fail_count=n_fail,
            required_skip_count=required_skip_count,
            declared_count=declared_count,
        )

    if n_fail > 0:
        metrics = _metrics("FAIL")
        return {
            "status": "FAIL",
            "metrics": metrics,
            "has_violation": True,
            "fail_count": n_fail,
            "warn_count": len(warn_results),
            "skip_count": len(skip_results),
            "ran_count": ran_count,
            "required_skip_count": required_skip_count,
            "required_skip_names": required_skip_names,
            "blocked_count": blocked_count,
            "blocked_names": blocked_names,
            "failures": [r["name"] for r in fail_results],
            "results": result_dicts,
        }

    # A gate that ran ZERO checks is not a pass — it is an unobserved
    # system reporting green (principles.md §2.7). Fail closed: this can
    # only happen if CHECKS or CHECK_CAPABILITIES drifted such that no
    # check is eligible under LAMBDA_CAPABILITIES.
    if ran_count == 0:
        metrics = _metrics("ERROR")
        return {
            "status": "ERROR",
            "metrics": metrics,
            "has_violation": True,
            "error": (
                "preflight ran 0 checks — every check was skipped for missing "
                "capabilities; the gate observed nothing"
            ),
            "skip_count": len(skip_results),
            "required_skip_count": required_skip_count,
            "required_skip_names": required_skip_names,
            "results": result_dicts,
        }

    # I11112: a REQUIRED check that could not run is a gap in what THIS
    # PROBE observed, not a confirmed violation of the system it probes —
    # sf-pipeline-policy.md §5's pre-spend-gate-probe carve-out applies
    # (fail-open, but visibly: a degraded flag plus alert, never silent).
    # has_violation stays False so WeeklyPreflightGate's existing Choice
    # keeps proceeding to CheckMutexRole; the NEW WeeklyPreflightGate arm
    # reads "degraded" to route through the alert + $.gate_degraded flag
    # before continuing, mirroring LibPinGateDegraded/PipelineContractCheck's
    # existing fail-open-with-alert convention rather than inventing a new
    # shape. A required check that RAN and found a real violation still
    # hard-fails above via has_violation — this branch is reached only when
    # every check that ran passed.
    if required_skip_count > 0 or blocked_count > 0:
        metrics = _metrics("DEGRADED")
        return {
            "status": "DEGRADED",
            "metrics": metrics,
            "has_violation": False,
            "degraded": True,
            "degraded_reason": "required_checks_unreachable",
            "blocked_count": blocked_count,
            "blocked_names": blocked_names,
            "warn_count": len(warn_results),
            "skip_count": len(skip_results),
            "ran_count": ran_count,
            "required_skip_count": required_skip_count,
            "required_skip_names": required_skip_names,
            "results": result_dicts,
            "stage_coverage": _assert_stage_coverage("WeeklyPreflight", started, run_date),
        }

    metrics = _metrics("OK")
    return {
        "status": "OK",
        "metrics": metrics,
        "has_violation": False,
        "degraded": False,
        "warn_count": len(warn_results),
        "skip_count": len(skip_results),
        "ran_count": ran_count,
        "required_skip_count": 0,
        "required_skip_names": [],
        "results": result_dicts,
        "stage_coverage": _assert_stage_coverage("WeeklyPreflight", started, run_date),
    }


def _assert_stage_coverage(stage: str, started: datetime, run_date: str | None) -> dict:
    """Record this stage's own output verdict (alpha-engine-config-I7214).

    `WeeklyPreflight` is an INFRASTRUCTURE/GATE stage: it positively declares
    in `ARTIFACT_REGISTRY.yaml`'s `pipeline_stages:` that it writes no durable
    artifact, so the verdict is `COVERED_NO_OUTPUT`. It asserts nothing and
    still RECORDS that it declared nothing — "declares nothing" and "was never
    considered" must not be the same absence, which is the whole point of that
    registry section.

    Never alters the handler's outcome: an observer that can change the stage
    it observes is a new failure mode bolted onto the one it reports. The
    ImportError branch is loud rather than silent because the nousergon-lib
    pin may predate the module, and an inert assertion must be distinguishable
    from a covered stage.

    ``run_date`` comes from the state input (``$.run_date`` — the SF Task
    passes ``Payload.$="$"`` so ``event`` already carries it, alpha-engine-
    config-I8155). Never fabricated: a missing/blank run_date is reported
    UNMEASURED rather than defaulting to any derived date, because a stage
    that invents its own timestamp is exactly the defect this mechanism
    exists to catch (alpha-engine-config-I8155 — EXECUTION_RUN_DATE is
    deliberately the ONLY carrier, never $RUN_DATE, which other launchers in
    the fleet reassign to the trading day).
    """
    if not run_date:
        print(f"ERROR: stage-coverage assertion has no run_date for {stage} — event carried none")
        return {"stage": stage, "status": "UNMEASURED", "reason": "no run_date on state input"}
    try:
        from krepis.stage_coverage import assert_stage_coverage
    except ImportError as exc:
        print(f"ERROR: stage-coverage assertion unavailable for {stage}: {exc}")
        return {"stage": stage, "status": "UNMEASURED", "reason": str(exc)}
    return assert_stage_coverage(stage, window_start=started, run_date=run_date)
