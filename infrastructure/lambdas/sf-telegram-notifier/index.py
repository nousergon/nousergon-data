"""alpha-engine-sf-telegram-notifier — fan SF status changes into Telegram.

Subscribes to EventBridge `Step Functions Execution Status Change` events for
the three Alpha Engine Step Functions (saturday / weekday / eod) and forwards
a human-readable summary to the fleet alerts forum via flow-doctor
(``notify_event`` / forum topic ``#pipeline``).

Migration arc: config#1742 (fleet Telegram consolidation T2). Falls back to
``nousergon_lib.telegram.send_message`` when flow-doctor is unavailable
(local tests / init failure).

The existing SNS → email path is unaffected.
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from execution_digest import (
    build_execution_digest,
    parse_run_date_from_execution_name,
    parse_run_date_from_input,
)
from eod_artifact_verification import (
    EOD_PIPELINE_NAME,
    format_eod_artifact_lines,
    verify_eod_artifacts,
)
from flow_doctor_telegram import build_flow_doctor_config, notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import (
    FleetTelegramTopic,
    PIPELINE_OBSERVER_TELEGRAM_TOPICS,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "sf-telegram-notifier"
_DB_BASENAME = "flow_doctor_sf_telegram_notifier"

_SF_LABELS: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness SF",
    "ne-preopen-trading-pipeline": "Pre-open Trading SF",
    "ne-postclose-trading-pipeline": "Post-close Trading SF",
    # 2026-07-28: wired for FAILURE transitions only, via its own
    # `alpha-engine-groom-sf-failure` rule (see deploy.sh §2c). Successes are
    # reported by the SF's own NotifyCycleComplete roll-up, which names each
    # lane's terminal state — strictly more informative than this generic
    # notifier could be. Without this entry the label falls back to the raw
    # state-machine name, which is legible but not consistent with the others.
    "alpha-engine-groom-dispatch": "Backlog Groom Dispatch SF",
}

_PREFLIGHT_LABEL_OVERRIDE: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness Preflight SF",
}

_STATUS_EMOJI: dict[str, str] = {
    "RUNNING": "\U0001f680",
    "SUCCEEDED": "✅",
    "FAILED": "\U0001f534",
    "TIMED_OUT": "⏰",
    "ABORTED": "⛔",
}

# The eod SF's deliberate `Error: "DegradedRun"` Fail terminal (config-I2702
# deliverable #4 / Brian's 2026-07-28 Option-A ruling, alpha-engine-
# config#2699) is a raw Step Functions FAILED status — it must not render
# identically to a genuine crash-FAILED (nousergon-data-i5289: the issue's
# "a degraded EOD must not read as a clean one" requirement, inverted — it
# must also not read as a generic failure). Distinct emoji, distinct label.
_DEGRADED_EMOJI = "\U0001f7e0"  # 🟠

# The weekly SF's deliberate `Error: "VacuousRun"` Fail terminal
# (alpha-engine-config-I9693). Same doctrine as DegradedRun one level up: an
# execution that entered none of the declared spine stages now FAILS rather
# than reporting SUCCEEDED, and it must not then read as a crash either. Five
# consecutive Saturdays were "recovered" by an all-skipped rerun that reported
# green; rendering the honest terminal as a generic red would trade one
# misreading for another.
_VACUOUS_EMOJI = "\U0001f6d1"  # 🛑

# Every severity this notifier can emit is exempt from the fleet-shared daily
# alert budget. Justified by the producer being BOUNDED, not by the traffic
# being important: this Lambda mirrors the lifecycle of a fixed set of state
# machines, so its event count per day is bounded by SF transitions and it
# cannot storm. Repeats of one signature are still suppressed by signature
# dedup + `dedup_cooldown_minutes: 1`. See the ruling note in `handler`.
_RATE_LIMIT_EXEMPT_SEVERITIES: tuple[str, ...] = ("critical", "error", "warning", "info")

_CAUSE_MAX_CHARS = 280
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})


def build_flow_doctor_config_for_tests() -> dict:
    """Expose config builder for fleet wiring contract tests."""
    return build_flow_doctor_config(
        _FLOW_NAME,
        PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        rate_limit_exempt_severities=_RATE_LIMIT_EXEMPT_SEVERITIES,
    )


def _severity_for_status(status: str) -> str:
    if status in ("FAILED", "TIMED_OUT", "ABORTED"):
        return "warning"
    return "info"


def _dedup_key(detail: dict) -> str:
    return ":".join(
        [
            _FLOW_NAME,
            detail.get("executionArn") or detail.get("name") or "unknown",
            detail.get("status") or "UNKNOWN",
        ]
    )


def _label_for_arn(sm_arn: str, *, is_preflight: bool = False) -> str:
    name = sm_arn.rsplit(":", 1)[-1] if sm_arn else ""
    if is_preflight and name in _PREFLIGHT_LABEL_OVERRIDE:
        return _PREFLIGHT_LABEL_OVERRIDE[name]
    return _SF_LABELS.get(name, name or "Unknown SF")


def _format_duration(started_ms: int | None, stopped_ms: int | None) -> str:
    if started_ms is None or stopped_ms is None:
        return ""
    secs = max(0, (int(stopped_ms) - int(started_ms)) // 1000)
    h, rem = divmod(secs, 3600)
    m, _ = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    return f"{m}m"


def _describe_execution(execution_arn: str) -> dict | None:
    if not execution_arn:
        return None
    try:
        sf = boto3.client("stepfunctions", region_name=REGION)
        return sf.describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return None


def _execution_input(describe_resp: dict | None) -> dict:
    """Parsed execution input, or ``{}`` when absent/unparseable."""
    if not describe_resp:
        return {}
    try:
        payload = json.loads(describe_resp.get("input") or "{}")
    except (ValueError, TypeError) as exc:
        logger.warning("could not parse execution input as JSON: %s", exc)
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_preflight_execution(describe_resp: dict | None) -> bool:
    return bool(_execution_input(describe_resp).get("shell_run"))


# The canonical role each state machine's own EventBridge-scheduled cadence
# trigger stamps into its execution input. Mirrors
# alpha-engine-config/scripts/gate_sf_run_sweep.py::_CANONICAL_PIPELINE_ROLE —
# the same distinction, applied to notification rather than to gate clearing.
_CANONICAL_PIPELINE_ROLE = {
    "ne-weekly-freshness-pipeline": "weekly",
    "ne-preopen-trading-pipeline": "daily",
    "ne-postclose-trading-pipeline": "eod",
}


def _skipped_stage_count(payload: dict) -> int:
    return sum(1 for k, v in payload.items() if k.startswith("skip_") and v)


def _is_partial_execution(sm_arn: str, describe_resp: dict | None) -> tuple[bool, int]:
    """Is this a narrowed run rather than the pipeline's real cadence run?

    Returns ``(is_partial, skipped_stage_count)``.

    An execution carrying ``skip_*`` flags ran a deliberately narrowed subset of
    the pipeline. Its failure is not the cadence cycle failing, and paging it at
    the same shape is overseer-policy invariant 17 — severity is a property of
    the invariant breached, not of the check that emitted it.

    Live example this exists for: ``director-verify-20260804T003005Z`` on
    ne-weekly-freshness-pipeline carried **24** ``skip_*: true`` flags and no
    ``pipeline_role``. It ran one stage, failed in 0m, and paged as
    "🔴 Weekly Freshness SF — FAILED / Duration: 0m / States: (no workload
    states in history)" — a message carrying, in its own body, the evidence that
    it was not a weekly run.

    A canonical ``pipeline_role`` (weekly/daily/eod) always wins: a real cadence
    run that legitimately skips a completed stage on a rerun is still the
    cadence run.
    """
    payload = _execution_input(describe_resp)
    name = sm_arn.rsplit(":", 1)[-1] if sm_arn else ""
    skipped = _skipped_stage_count(payload)
    role = payload.get("pipeline_role")
    if role and role == _CANONICAL_PIPELINE_ROLE.get(name):
        return False, skipped
    return skipped > 0, skipped


def _is_degraded_run(describe_resp: dict | None) -> bool:
    """True when a FAILED execution is the deliberate ``DegradedRun`` Fail
    terminal, not a genuine crash. Reads the SAME ``Error`` field the SF
    definition itself stamps (``step_function_eod.json``'s ``DegradedRun``
    Fail state, ``"Error": "DegradedRun"``) rather than re-deriving the
    distinction from cause text — one source of truth, mirrored, not
    reinvented."""
    if not describe_resp:
        return False
    return (describe_resp.get("error") or "") == "DegradedRun"


def _is_vacuous_run(describe_resp: dict | None) -> bool:
    """True when a FAILED execution is the deliberate ``VacuousRun`` Fail
    terminal — the execution entered none of the pipeline's declared spine
    stages (``alpha-engine-config-I9693``). Reads the same ``Error`` field the
    definition stamps, for the same reason :func:`_is_degraded_run` does."""
    if not describe_resp:
        return False
    return (describe_resp.get("error") or "") == "VacuousRun"


def _normalize_cause_snippet(snippet: str, *, max_chars: int = _CAUSE_MAX_CHARS) -> str:
    """Collapse embedded newlines and cap length for a single Telegram line.

    A poll-result `detail` string (ssm-liveness-poller's stderr capture) is
    frequently multi-line ("...not granted.\\nfatal: unable to access...").
    Left as-is, ``text.splitlines()`` — including every test and downstream
    reader that greps for the line starting "Cause:" — sees only the FIRST
    physical line, silently dropping the rest (alpha-engine-config-I9742: the
    2026-09-01 execution's real 403 sat on line 2 of a 3-line detail string).
    """
    snippet = " ".join(snippet.split())
    if len(snippet) > max_chars:
        snippet = snippet[: max_chars - 1] + "…"
    return snippet


# ssm-liveness-poller's `detail` field is the ONE field its own module docs
# name as "every consumer already carries" — it is deliberately more
# substantial than the boilerplate describe_execution error/cause this cap
# otherwise bounds (bounded upstream to ~1500 stderr + ~2000 stdout chars,
# per ssm-liveness-poller/index.py's _STDERR_TAIL_CHARS / _STDOUT_TAIL_CHARS).
# Capping it at the same 280 chars as a one-line boilerplate string would
# have truncated the 2026-09-01 execution's 403 before it appeared — the
# defect deliverable 5 exists to fix, reintroduced by an over-tight cap
# instead of a wrong key.
_DETAILED_CAUSE_MAX_CHARS = 1000


def _failure_cause_from(describe_resp: dict | None) -> str:
    if not describe_resp:
        return ""
    error = (describe_resp.get("error") or "").strip()
    cause = (describe_resp.get("cause") or "").strip()
    if error and cause:
        snippet = f"{error}: {cause}"
    else:
        snippet = error or cause
    return _normalize_cause_snippet(snippet) if snippet else snippet


def _build_message(
    detail: dict,
    describe_resp: dict | None = None,
    *,
    sf_client=None,
    s3_client=None,
) -> tuple[str, bool, bool]:
    """Return ``(text, silent, hollow_suspect, is_partial)``.

    ``text`` may render the raw AWS ``FAILED`` status as ``DEGRADED`` (the
    eod SF's deliberate ``DegradedRun`` Fail terminal) — the returned tuple's
    ``status``-shaped values stay keyed on the raw AWS status throughout;
    only the rendered label/emoji distinguish it.
    """
    status = detail.get("status", "UNKNOWN")
    execution_arn = detail.get("executionArn", "")
    if describe_resp is None:
        describe_resp = _describe_execution(execution_arn)
    is_preflight = _is_preflight_execution(describe_resp)
    sm_arn = detail.get("stateMachineArn", "")
    is_partial, skipped_stages = _is_partial_execution(sm_arn, describe_resp)
    label = _label_for_arn(sm_arn, is_preflight=is_preflight)
    if is_partial:
        # Say what it actually was. A reader must not have to open the console
        # to learn that "Weekly Freshness SF — FAILED" meant one stage of it.
        label = f"{label} (partial run — {skipped_stages} stage(s) skipped)"
    sm_name = sm_arn.rsplit(":", 1)[-1] if sm_arn else ""
    is_degraded = status == "FAILED" and _is_degraded_run(describe_resp)
    is_vacuous = status == "FAILED" and _is_vacuous_run(describe_resp)
    if is_vacuous:
        display_status = "NO WORK DONE"
        emoji = _VACUOUS_EMOJI
    elif is_degraded:
        display_status = "DEGRADED"
        emoji = _DEGRADED_EMOJI
    else:
        display_status = status
        emoji = _STATUS_EMOJI.get(status, "\U0001f4e8")
    exec_name = detail.get("name", "") or "(unknown execution)"
    hollow_suspect = False
    detailed_failure_cause: str | None = None

    lines = [f"{emoji} *{label} — {display_status}*"]

    if status == "RUNNING":
        lines.append(f"Execution: {exec_name}")
        return "\n".join(lines), True, False, is_partial

    duration = _format_duration(detail.get("startDate"), detail.get("stopDate"))
    if duration:
        lines.append(f"Duration: {duration}")

    if status in _TERMINAL_STATUSES and execution_arn:
        if sf_client is None:
            sf_client = boto3.client("stepfunctions", region_name=REGION)
        if s3_client is None and not is_preflight:
            s3_client = boto3.client("s3", region_name=REGION)
        run_date = parse_run_date_from_input((describe_resp or {}).get("input"))
        if not run_date:
            # config#5289: manual re-triggers / input-parse failures carry no
            # `run_date` key — fall back to the ISO date embedded in every
            # scheduled trigger's execution name rather than losing it.
            run_date = parse_run_date_from_execution_name(exec_name)
        if run_date:
            lines.append(f"Run date: {run_date}")
        digest_lines, hollow_suspect, detailed_failure_cause = build_execution_digest(
            execution_arn=execution_arn,
            is_preflight=is_preflight,
            execution_start_ms=detail.get("startDate"),
            run_date=run_date,
            sf_client=sf_client,
            s3_client=None if is_preflight else s3_client,
        )
        if hollow_suspect and status == "SUCCEEDED":
            lines.append("⚠️ *HOLLOW-SUSPECT* — workload state(s) completed implausibly fast")
        lines.append("*States:*")
        lines.extend(digest_lines)

        # nousergon-data-i5289 (alpha-engine-config#5289 scope item 4): a
        # postclose SUCCEEDED/DEGRADED terminal additionally gets its day's
        # artifacts verified — a "SUCCESS" that did not write eod_pnl.csv is
        # the failure mode this issue exists to catch. s3_client is already a
        # real client here (eod is never preflight, so the block above always
        # provisions one).
        if sm_name == EOD_PIPELINE_NAME and (status == "SUCCEEDED" or is_degraded):
            artifact_status = verify_eod_artifacts(s3_client, run_date)
            lines.extend(format_eod_artifact_lines(artifact_status))

    if status == "FAILED":
        # sf-pipeline-policy §2.3 corollary: the terminal failure message must
        # carry the actual error, not the shared Fail state's boilerplate
        # ("One or more weekday pipeline steps failed."). Prefer the
        # diagnostic pulled from the poll-result contract embedded in the
        # execution history; fall back to describe_execution's error/cause
        # only when no such detail is present (alpha-engine-config-I9742
        # deliverable 5).
        cause = (
            _normalize_cause_snippet(detailed_failure_cause, max_chars=_DETAILED_CAUSE_MAX_CHARS)
            if detailed_failure_cause
            else _failure_cause_from(describe_resp)
        )
        if cause:
            lines.append(f"Cause: {cause}")

    lines.append(f"Execution: {exec_name}")
    silent = False
    if status == "SUCCEEDED" and hollow_suspect:
        silent = False
    # A narrowed run is delivered without a push. It is still sent, still
    # readable in the topic, and still recorded — it just does not buzz, because
    # the cadence cycle it is named after did not fail.
    if is_partial:
        silent = True
    return "\n".join(lines), silent, hollow_suspect, is_partial


def handler(event: dict, context) -> dict:  # noqa: ARG001
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    logger.info("SF status change: sf=%s status=%s", sm_name, status)

    text, silent, hollow_suspect, is_partial = _build_message(detail)
    ok = notify_via_flow_doctor(
        text,
        silent=silent,
        # A partial run is recorded at info regardless of its terminal status:
        # severity belongs to the invariant breached, and a narrowed
        # verification run failing does not breach the cadence cycle's.
        severity=(
            "info" if is_partial
            else "warning" if hollow_suspect and status == "SUCCEEDED"
            else _severity_for_status(status)
        ),
        dedup_key=_dedup_key(detail),
        flow_name=_FLOW_NAME,
        topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        # Brian's ruling, 2026-08-17: "eod outcome should land in the same
        # place as 'Post-close Trading SF — RUNNING'". Those two messages take
        # DIFFERENT delivery paths, which is why they had different
        # reliability. RUNNING goes out via the `silent_topic` branch's
        # `send_raw`, which never touches flow-doctor's alert pipeline and so
        # always arrives. The terminal goes through `notify_event`, where the
        # fleet-shared daily budget can drop it — measured on the postclose SF:
        # 2026-08-03, -04 and -07 all reached SUCCEEDED and all three reports
        # were recorded `reason=rate_limited`, delivered nowhere, while that
        # morning's RUNNING ping landed normally. Three of the last eight
        # trading days had a green EOD that was indistinguishable, on the
        # thread the operator actually reads, from a pipeline that never ran.
        #
        # Two changes make the ruling structurally true rather than probable:
        # the exemption below takes this bounded lifecycle mirror out of the
        # shared budget so the ordinary path fires, and `guaranteed_topic` is
        # the belt — any FUTURE suppression that is not dedup still lands the
        # message in PIPELINE, the same thread RUNNING goes to.
        rate_limit_exempt_severities=_RATE_LIMIT_EXEMPT_SEVERITIES,
        guaranteed_topic=FleetTelegramTopic.PIPELINE,
        context={
            "state_machine": sm_name,
            "execution": detail.get("name"),
            "status": status,
            "partial_run": is_partial,
        },
        silent_topic=FleetTelegramTopic.PIPELINE,
        # Matches playbooks.yaml's registered `sf_completion_telegram` class
        # source exactly (config-I3513).
        source="flow-doctor:sf-telegram-notifier",
    )

    return {
        "status": status,
        "state_machine": sm_name,
        "execution": detail.get("name", ""),
        "telegram_sent": ok,
        "silent": silent,
        "hollow_suspect": hollow_suspect,
    }
