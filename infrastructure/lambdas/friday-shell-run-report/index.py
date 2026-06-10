"""alpha-engine-friday-shell-run-report — consolidate the Friday shell-run result.

Subscribes to EventBridge ``Step Functions Execution Status Change`` events for
the ``alpha-engine-saturday-pipeline`` state machine (terminal transitions:
SUCCEEDED / FAILED / TIMED_OUT / ABORTED). For executions that ran in **shell-run
mode** (``shell_run: true`` / ``pipeline_role: "shell-run"`` in the execution
input — the Friday-PM dry preflight of the Saturday SF), the handler reads the
execution history, reduces it to a per-state pass/fail summary, and writes a
single consolidated report to
``s3://{bucket}/friday-shell-run/{trading_day}/report.json`` plus a structured
SNS summary — the "12 h before the real Saturday 02:00 PT firing" diagnostic
artifact (ROADMAP L658 design point 5, the scoped follow-on the SF spine
deferred).

This is the consolidated-report half of L658; the shell-run orchestration
(``CheckShellRun`` spine + the ``eod-success-friday-shell-trigger`` Lambda)
already shipped ~2026-05-29. Real Saturday runs (no ``shell_run``) are the
intended no-op path and return ``{"reported": False}``.

Fail-loud semantics (the report IS this Lambda's deliverable, per
``feedback_no_silent_fails``):
  * ``states:GetExecutionHistory`` / ``s3:PutObject`` / ``sns:Publish`` failure
    → raises (EventBridge→Lambda retry + CW Lambda-error alarm surface it).
  * A non-shell-run execution, a non-Saturday state machine, or a non-terminal
    status is an intended skip and returns a structured ``reported: False`` —
    NOT a swallow.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone

import boto3

from alpha_engine_lib.trading_calendar import last_closed_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
SATURDAY_SF_NAME = "alpha-engine-saturday-pipeline"
S3_BUCKET = os.environ.get("S3_BUCKET", "alpha-engine-research")
SNS_TOPIC_ARN = os.environ.get(
    "SNS_TOPIC_ARN", f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts"
)
REPORT_PREFIX = "friday-shell-run"
_TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}
# Shell-run executions are named `friday-shell-{YYYY-MM-DD}-{...}` by the
# eod-success-friday-shell-trigger Lambda — primary trading_day source.
_NAME_DATE_RE = re.compile(r"friday-shell-(\d{4}-\d{2}-\d{2})-")


def _is_shell_run(execution_input: str, name: str) -> bool:
    """True iff this execution ran the Friday-PM preflight. Load-bearing signal
    is the input flag; the name prefix is the corroborating fallback."""
    try:
        payload = json.loads(execution_input or "{}")
    except (json.JSONDecodeError, ValueError):
        payload = {}
    if payload.get("shell_run") is True or payload.get("pipeline_role") == "shell-run":
        return True
    return bool(_NAME_DATE_RE.match(name or ""))


def _trading_day(name: str, stop_date_ms) -> str:
    """trading_day the shell-run validated: parse from the execution name first
    (authoritative — the trigger stamps it), else derive from the completion
    timestamp via the canonical NYSE calendar helper."""
    m = _NAME_DATE_RE.match(name or "")
    if m:
        return m.group(1)
    if stop_date_ms is not None:
        dt = datetime.fromtimestamp(int(stop_date_ms) / 1000, tz=timezone.utc)
        return last_closed_trading_day(dt).isoformat()
    raise RuntimeError(
        "shell-run report: cannot resolve trading_day — execution name lacks the "
        "friday-shell-{date} prefix AND detail.stopDate is absent"
    )


def _summarize_history(execution_arn: str) -> tuple[list[dict], dict | None]:
    """Reduce the execution history to per-state {name, status, duration_s} and
    the ExecutionFailed cause (if any). A state Entered-but-never-Exited is the
    failure point (the execution died inside it)."""
    sfn = boto3.client("stepfunctions", region_name=REGION)
    entered: dict[str, datetime] = {}
    exited: dict[str, datetime] = {}
    order: list[str] = []
    failure: dict | None = None

    next_token = None
    while True:
        kwargs = {"executionArn": execution_arn, "maxResults": 1000,
                  "includeExecutionData": False}
        if next_token:
            kwargs["nextToken"] = next_token
        resp = sfn.get_execution_history(**kwargs)
        for ev in resp.get("events", []):
            etype = ev.get("type", "")
            ts = ev.get("timestamp")
            if etype.endswith("StateEntered"):
                name = ev.get("stateEnteredEventDetails", {}).get("name")
                if name:
                    if name not in entered:
                        order.append(name)
                    entered[name] = ts
            elif etype.endswith("StateExited"):
                name = ev.get("stateExitedEventDetails", {}).get("name")
                if name:
                    exited[name] = ts
            elif etype == "ExecutionFailed":
                d = ev.get("executionFailedEventDetails", {})
                failure = {"error": d.get("error"), "cause": d.get("cause")}
        next_token = resp.get("nextToken")
        if not next_token:
            break

    per_state = []
    for name in order:
        passed = name in exited
        dur = None
        if passed and entered.get(name) and exited.get(name):
            dur = round((exited[name] - entered[name]).total_seconds(), 1)
        per_state.append({
            "name": name,
            "status": "PASS" if passed else "FAIL",
            "duration_s": dur,
        })
    return per_state, failure


def _publish_summary(report: dict) -> None:
    s = report["summary"]
    failed = [p["name"] for p in report["per_state"] if p["status"] == "FAIL"]
    lines = [
        f"Friday shell-run preflight — {report['execution_status']}",
        f"trading_day {report['trading_day']} | readiness {s['readiness']}",
        f"states: {s['passed']} pass / {s['failed']} fail of {s['n_states']}",
    ]
    if failed:
        lines.append("FAILED states: " + ", ".join(failed))
    if report.get("failure"):
        lines.append(f"cause: {report['failure'].get('error')}")
    lines.append(f"report: s3://{S3_BUCKET}/{report['report_key']}")
    boto3.client("sns", region_name=REGION).publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"Saturday Preflight Report — {s['readiness']} ({report['trading_day']})"[:100],
        Message="\n".join(lines),
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    detail = event.get("detail") or {}
    sm_name = detail.get("stateMachineArn", "").rsplit(":", 1)[-1]
    status = detail.get("status", "")
    if sm_name != SATURDAY_SF_NAME or status not in _TERMINAL:
        logger.info("ignored event: sm=%s status=%s", sm_name, status)
        return {"reported": False, "reason": "wrong_event"}

    name = detail.get("name", "")
    if not _is_shell_run(detail.get("input", ""), name):
        logger.info("saturday execution %s is not a shell-run — no report", name)
        return {"reported": False, "reason": "not_shell_run"}

    execution_arn = detail["executionArn"]
    trading_day = _trading_day(name, detail.get("stopDate"))
    per_state, failure = _summarize_history(execution_arn)
    n_pass = sum(1 for p in per_state if p["status"] == "PASS")
    n_fail = sum(1 for p in per_state if p["status"] == "FAIL")
    readiness = "GO_SATURDAY" if (status == "SUCCEEDED" and n_fail == 0) else "HOLD_INVESTIGATE"

    report = {
        "trading_day": trading_day,
        "execution_arn": execution_arn,
        "execution_name": name,
        "execution_status": status,
        "summary": {
            "n_states": len(per_state),
            "passed": n_pass,
            "failed": n_fail,
            "readiness": readiness,
        },
        "per_state": per_state,
        "failure": failure,
    }
    report_key = f"{REPORT_PREFIX}/{trading_day}/report.json"
    report["report_key"] = report_key

    boto3.client("s3", region_name=REGION).put_object(
        Bucket=S3_BUCKET, Key=report_key,
        Body=json.dumps(report, indent=2).encode(), ContentType="application/json",
    )
    _publish_summary(report)
    logger.info(
        "shell-run report written: %s readiness=%s (%d/%d states pass)",
        report_key, readiness, n_pass, len(per_state),
    )
    return {"reported": True, "report_key": report_key, "readiness": readiness}
