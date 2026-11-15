#!/usr/bin/env python3
"""Weekly-SF operator-actions-to-recover metric (alpha-engine-config#6686).

weekly-sf-policy.md §2.5 states the target: **mean operator actions to
recover from any single-stage failure = 1**. Clause
``WSF-2.5-recovery-is-one-command`` was ``kind: none`` — nothing measured
it, so the policy's own revisit trigger ("any weekly run requiring >1
operator rerun") fired only if a human noticed and remembered. This script
closes that gap: it derives, for a given ``run_date`` of
``ne-weekly-freshness-pipeline``, how many operator-recovery executions
(``rerun-*`` / ``watch-rerun-*``, per ``scripts/weekly_sf_rerun.py``'s own
naming convention) it took to reach a real terminal state, publishes the
result alongside the run's other artifacts, and alerts when the count
exceeds the policy's target.

REUSE, NOT DUPLICATION: the execution-history parsing primitives
(``execution_input``, ``list_all_executions``) are imported directly from
``scripts/weekly_sf_rerun.py`` — this module owns none of that parsing
logic itself.

CLASSIFICATION
---------------
Every execution of the state machine within the scan window is one of:

- **counted** — a real automated or operator-recovery attempt for the
  target ``run_date`` (the scheduled ``pipeline_role="weekly"`` cadence
  execution, or a ``rerun-*``/``watch-rerun-*`` recovery execution).
- **excluded_exercise** — ``pipeline_role="weekly-exercise"``/``"exercise"``
  runs belong to the separate weekly-exercise rehearsal chain, never the
  production recovery count for a real ``run_date``.
- **excluded_gate_skip** — a ``pipeline_role="weekly"`` execution that
  SUCCEEDED because ``CheckWeeklyRunDayGate`` self-selected a non-run day
  (config#1824): the THU-SAT cron fires 3x and 2 of those 3 firings
  Succeed-skip at ``WeeklyRunDaySkip`` before any real work state is
  entered. That is the run-day gate doing its job, not a pipeline attempt,
  and must never inflate the recovery count.

  **NOT a duration heuristic** (corrected 2026-08-29 — alpha-engine-
  config-I8057, same class fixed in alpha-engine-config-PR8055's
  ``_is_weekly_gate_out``). The prior implementation used
  ``duration < GATE_SKIP_MAX_SECONDS`` (60s) as a pre-filter before
  confirming via a ``GetExecutionHistory`` walk — safe against a real run
  finishing fast (history confirmation caught it), but blind to a
  gate-out that happened to run slower than the threshold, and it paid an
  extra API call either way. The gate publishes its own verdict directly
  in the execution OUTPUT — ``weekly_run_day_gate.Payload.is_weekly_run_day
  is False`` — readable from the same ``describe_execution`` call already
  made to resolve ``role``/``run_date``, so this needs no extra AWS call
  and no clock. Verified live 2026-08-21/08-22 against both real
  gate-outs (3.5s, 5.7s) and the fleet's shortest genuine run (12.6s,
  ``director-verify-0731-003355Z``): the shortest real run carries no
  ``weekly_run_day_gate`` key at all, so it is never at risk of being
  misread — the test is POSITIVE (excludes only on ``is False``, never on
  absence).

Correlation to a target ``run_date``:

1. ``rerun-{date}-{n}`` / ``watch-rerun-{date}-{n}`` named executions carry
   the date in the name (the ``weekly_sf_rerun.py`` convention) — cheapest
   path, no ``describe_execution`` needed.
2. Everything else (the scheduled cadence execution's SF-generated name) is
   correlated via its own input: an explicit ``run_date`` if present,
   otherwise the UTC date of ``startDate`` — which is exactly what
   ``InitializeInput`` itself stamps when the caller didn't pass one
   (``States.ArrayGetItem(States.StringSplit($$.Execution.StartTime,'T'),0)``
   in ``infrastructure/step_function.json``), so this never has to fetch
   full execution history just to learn the date.

NO-DATA IS NOT GREEN (principles §2.7): zero *counted* executions for the
target ``run_date`` is a distinct, LOUD, nonzero-exit condition — it means
the pipeline never ran or the correlation failed, not that recovery was
clean. A healthy zero (one clean automated run, no reruns) always has
``executions_total == 1``.

ALERTING: publishes through ``nousergon_lib.alerts.publish`` (the fleet's
existing SNS+Telegram fan-out chokepoint — mirrors
``validators/constituents_drift_check.py``) when ``recovery_actions > 1``.
Never crashes the metric write: the S3 artifact PUT happens first and
unconditionally; alert publish failures are caught and logged, never
raised (mirrors the existing ``alerts.publish``-adjacent call sites in this
repo).

Read-only against Step Functions / S3 except the one metric PUT (and the
best-effort alert publish); nothing else is mutated.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

# Repo-root bootstrap so `from scripts.weekly_sf_rerun import ...` resolves
# whether this is invoked as `python scripts/weekly_sf_recovery_metric.py`
# or `python -m scripts.weekly_sf_recovery_metric` (mirrors tests/conftest.py's
# sys.path insert; scripts/ carries no __init__.py, so it is only importable
# as a namespace package with the repo root on sys.path).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from scripts.weekly_sf_rerun import (  # noqa: E402
    list_all_executions,
)

# A Step Functions / S3 API response, an execution's input/output JSON, or the
# emitted metric record: open-ended mappings whose value types vary per key.
JsonDict = dict[str, Any]  # pyright: ignore[reportExplicitAny]  # boto3 response / SF execution JSON / metric record, open-ended shape

logger = logging.getLogger(__name__)

STATE_MACHINE_NAME = "ne-weekly-freshness-pipeline"
DEFAULT_STATE_MACHINE_ARN = (
    f"arn:aws:states:us-east-1:711398986525:stateMachine:{STATE_MACHINE_NAME}"
)
DEFAULT_BUCKET = "alpha-engine-research"
COMPLETION_PREFIX = f"_sf_completion/{STATE_MACHINE_NAME}/"
COMPLETION_MARKER_RE = re.compile(
    re.escape(COMPLETION_PREFIX) + r"(\d{4}-\d{2}-\d{2})\.json$"
)
RECOVERY_KEY_TEMPLATE = COMPLETION_PREFIX + "{run_date}-recovery.json"

# Names emitted by scripts/weekly_sf_rerun.py's next_rerun_name (and the
# legacy pre-"watch-" convention it superseded) — both prefixes carry the
# run_date directly in the name, so correlation never needs a history fetch.
NAME_RUN_DATE_RE = re.compile(r"^(?:watch-)?rerun-(\d{4}-\d{2}-\d{2})-\d+$")

# Roles that pull an execution out of the production recovery count
# entirely — the weekly-exercise rehearsal chain is a distinct pipeline,
# never a real run_date attempt.
EXERCISE_ROLES = frozenset({"exercise", "weekly-exercise"})

def _is_weekly_gate_out(output_json: JsonDict) -> bool:
    """True iff a SUCCEEDED weekly-role execution's OWN OUTPUT proves it did
    no work (alpha-engine-config-I8057, mirrors
    ``gate_sf_run_sweep.py::_is_weekly_gate_out`` from
    alpha-engine-config-PR8055 — the reference implementation for this
    class). Positive test only: excludes on
    ``weekly_run_day_gate.Payload.is_weekly_run_day is False``, never on the
    key's absence — a real run does not carry it at all, so an absent-key-
    as-gate-out reading would strand every real weekly execution.
    """
    gate: object = output_json.get("weekly_run_day_gate")
    if not isinstance(gate, dict):
        return False
    inner: object = cast(JsonDict, gate).get("Payload")
    if not isinstance(inner, dict):
        return False
    return cast(JsonDict, inner).get("is_weekly_run_day") is False


@dataclass(frozen=True)
class ExecutionRecord:
    arn: str
    name: str
    status: str
    start: datetime
    stop: datetime | None
    role: str | None
    run_date: str
    classification: str  # "counted" | "excluded_exercise" | "excluded_gate_skip"


def classify_execution(
    sf: Any,  # pyright: ignore[reportExplicitAny]  # untyped boto3 stepfunctions client (no stubs installed)
    ex: JsonDict,
) -> ExecutionRecord:
    """Classify one `list_executions` summary. Fetches `describe_execution`
    only for executions whose name doesn't already carry the run_date — that
    same call's `output` field (already in hand, no second AWS call) also
    carries the gate-out verdict for weekly-role SUCCEEDED candidates."""
    name: str = ex["name"]
    status: str = ex["status"]
    start: datetime = ex["startDate"]
    stop: datetime | None = ex.get("stopDate")

    m = NAME_RUN_DATE_RE.match(name)
    if m:
        # weekly_sf_rerun.py always emits EMITTED_ROLE="watch-rerun"; the
        # legacy "rerun-" prefix (pre config#2277) carried the same
        # operator-initiated semantics. Either way this is always a counted
        # recovery attempt — never exercise, never a gate-skip (the gate
        # only applies to pipeline_role="weekly").
        return ExecutionRecord(
            arn=ex["executionArn"], name=name, status=status, start=start,
            stop=stop, role="watch-rerun", run_date=m.group(1),
            classification="counted",
        )

    desc: JsonDict = sf.describe_execution(executionArn=ex["executionArn"])
    try:
        input_json: JsonDict = json.loads(desc.get("input") or "{}")
    except json.JSONDecodeError:
        input_json = {}
    role: str | None = input_json.get("pipeline_role")

    run_date: str
    if isinstance(input_json.get("run_date"), str) and input_json["run_date"]:
        run_date = input_json["run_date"]
    else:
        # Mirrors InitializeInput's own fallback
        # (States.ArrayGetItem(States.StringSplit($$.Execution.StartTime,'T'),0))
        # — the UTC date of execution start when no explicit run_date was
        # passed. True for every scheduled cadence execution.
        run_date = start.astimezone(timezone.utc).date().isoformat()

    classification = "counted"
    if role in EXERCISE_ROLES:
        classification = "excluded_exercise"
    elif role == "weekly" and status == "SUCCEEDED":
        try:
            output_json: JsonDict = json.loads(desc.get("output") or "{}")
        except json.JSONDecodeError:
            output_json = {}
        if _is_weekly_gate_out(output_json):
            classification = "excluded_gate_skip"

    return ExecutionRecord(
        arn=ex["executionArn"], name=name, status=status, start=start,
        stop=stop, role=role, run_date=run_date, classification=classification,
    )


def gather_records(
    sf: Any,  # pyright: ignore[reportExplicitAny]  # untyped boto3 stepfunctions client (no stubs installed)
    sm_arn: str,
    run_date: str,
    *,
    scan_cap: int = 300,
) -> list[ExecutionRecord]:
    """Classify every execution in the scan window and return those
    correlated to `run_date` (any classification — callers filter for
    "counted" themselves so excluded classes stay visible for transparency)."""
    records: list[ExecutionRecord] = []
    ex: JsonDict
    for ex in list_all_executions(sf, sm_arn, cap=scan_cap):
        rec = classify_execution(sf, ex)
        if rec.run_date == run_date:
            records.append(rec)
    return records


def resolve_latest_run_date(
    s3: Any,  # pyright: ignore[reportExplicitAny]  # untyped boto3 s3 client (no stubs installed)
    bucket: str,
) -> str:
    """Default --run-date: the most recent WriteCompletionMarker date under
    the weekly SF's own `_sf_completion/` prefix (excludes this script's own
    `-recovery.json` siblings via the strict `{date}.json` match)."""
    dates: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, str] = {"Bucket": bucket, "Prefix": COMPLETION_PREFIX}
        if token:
            kwargs["ContinuationToken"] = token
        resp: JsonDict = s3.list_objects_v2(**kwargs)
        contents: list[JsonDict] = resp.get("Contents", []) or []
        for obj in contents:
            m = COMPLETION_MARKER_RE.match(obj["Key"])
            if m:
                dates.append(m.group(1))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    if not dates:
        raise SystemExit(
            f"FATAL: no completion markers found under "
            f"s3://{bucket}/{COMPLETION_PREFIX} — cannot default --run-date "
            "(no data is not the same as a clean run). Pass --run-date "
            "explicitly."
        )
    return max(dates)


def build_metric(run_date: str, records: list[ExecutionRecord]) -> JsonDict:
    counted = sorted(
        (r for r in records if r.classification == "counted"),
        key=lambda r: r.start,
    )
    executions_total = len(counted)
    recovery_actions = max(0, executions_total - 1)
    first_terminal_status = counted[0].status if counted else None
    succeeded_after_n_reruns: int | None = None
    for i, r in enumerate(counted):
        if r.status == "SUCCEEDED":
            succeeded_after_n_reruns = i
            break

    return {
        "run_date": run_date,
        "executions_total": executions_total,
        "recovery_actions": recovery_actions,
        "first_terminal_status": first_terminal_status,
        "succeeded_after_n_reruns": succeeded_after_n_reruns,
        "excluded_exercise": sum(1 for r in records if r.classification == "excluded_exercise"),
        "excluded_gate_skip": sum(1 for r in records if r.classification == "excluded_gate_skip"),
        "execution_arns": [r.arn for r in counted],
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "sf": STATE_MACHINE_NAME,
    }


def _publish_recovery_alert(metric: JsonDict) -> None:
    """Best-effort alert fan-out (nousergon_lib.alerts.publish — mirrors
    validators/constituents_drift_check.py). NEVER raises: a failure here
    must not take down a metric write that already succeeded."""
    try:
        from nousergon_lib import alerts  # noqa: PLC0415
    except ImportError as exc:
        logger.warning(
            "alerts publish skipped — nousergon_lib.alerts unavailable: %s", exc,
        )
        return
    message = (
        f"weekly-sf-policy §2.5 target breached: {STATE_MACHINE_NAME} run_date "
        f"{metric['run_date']} needed {metric['recovery_actions']} operator "
        f"recovery action(s) (target: mean=1) to reach terminal status "
        f"{metric['first_terminal_status']!r}"
        + (
            f", SUCCEEDED after {metric['succeeded_after_n_reruns']} rerun(s)"
            if metric["succeeded_after_n_reruns"] is not None
            else " (not yet SUCCEEDED)"
        )
        + f". {metric['executions_total']} execution(s) total. "
        f"See s3://{DEFAULT_BUCKET}/{RECOVERY_KEY_TEMPLATE.format(run_date=metric['run_date'])} "
        "and weekly-sf-policy.md §2.5. (alpha-engine-config#6686)"
    )
    try:
        result = alerts.publish(
            message,
            severity="error",
            source="nousergon-data/scripts/weekly_sf_recovery_metric.py",
            # Dedup keyed on the exact count: a stable count within the
            # window sends once; an ESCALATING count (more reruns since the
            # last check) always sends fresh — the operator needs to know
            # the recovery got worse, not just that it happened once.
            dedup_key=f"weekly_sf_recovery_{metric['run_date']}_{metric['recovery_actions']}",
            dedup_window_min=720,
        )
        logger.info(
            "recovery alert publish: sns_ok=%s telegram_ok=%s any_ok=%s dedup_skipped=%s",
            result.sns.ok, result.telegram.ok, result.any_ok, result.dedup_skipped,
        )
    except Exception as exc:  # noqa: BLE001 — alert delivery must never crash the metric write
        logger.warning("recovery alert publish failed: %s", exc)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--run-date", default=None, help="target run_date YYYY-MM-DD (default: latest _sf_completion marker)")
    ap.add_argument("--state-machine-arn", default=DEFAULT_STATE_MACHINE_ARN)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--scan-cap", type=int, default=300, help="max executions to classify while scanning for the target run_date")
    ap.add_argument("--no-write", action="store_true", help="print the metric only; do not PUT it to S3")
    ap.add_argument("--no-alert", action="store_true", help="diagnostic mode: never publish an alert even if recovery_actions>1")
    args = ap.parse_args(argv)

    import boto3  # deferred so the pure functions above stay import-light for tests

    sf = boto3.client("stepfunctions", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    run_date = args.run_date or resolve_latest_run_date(s3, args.bucket)

    records = gather_records(sf, args.state_machine_arn, run_date, scan_cap=args.scan_cap)
    counted = [r for r in records if r.classification == "counted"]

    if not counted:
        # NO-DATA IS NOT GREEN: distinct from a healthy zero (executions_total
        # == 1, recovery_actions == 0). This is either "the pipeline never
        # ran for run_date" or "correlation failed" — both need an operator,
        # neither is silently swallowed as a clean run.
        print(
            f"NO DATA: 0 counted executions found for run_date={run_date} on "
            f"{STATE_MACHINE_NAME} (scanned {len(records)} correlated-but-"
            f"excluded execution(s): exercise or gate-skip). This is NOT the "
            "same as zero reruns — investigate before trusting any recovery "
            "metric for this date.",
            file=sys.stderr,
        )
        return 1

    metric = build_metric(run_date, records)
    print(json.dumps(metric, indent=2, sort_keys=True))

    if not args.no_write:
        key = RECOVERY_KEY_TEMPLATE.format(run_date=run_date)
        s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=json.dumps(metric, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{key}")

    if metric["recovery_actions"] > 1 and not args.no_alert:
        _publish_recovery_alert(metric)

    return 0


if __name__ == "__main__":
    sys.exit(main())
