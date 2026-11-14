"""alpha-engine-sf-telegram-notifier — fan SF status changes into Telegram.

Subscribes to EventBridge `Step Functions Execution Status Change` events for
the three Alpha Engine Step Functions (saturday / weekday / eod) and forwards
a human-readable summary to the fleet alerts forum via flow-doctor
(``notify_event`` / forum topic ``#pipeline``).

Migration arc: config#1742 (fleet Telegram consolidation T2). Falls back to
``nousergon_lib.telegram.send_message`` when flow-doctor is unavailable
(local tests / init failure).

The existing SNS → email path is unaffected.

ONE MESSAGE A DAY (observability-policy §7.2b, Brian 2026-11-06): the chat
gets one pipelines message per day, success or failure, sent on the day's
closing terminal (`_DAY_CLOSERS`). RUNNING, preflight and every other
terminal send nothing to the chat; they stay on the alert bus and the
console. A failure inside trading hours is caught as a critical by the
Crucible v2 watchdogs, not here.

The day's message counts only cadence runs (never a preflight shell or a
``skip_*`` partial run), reports a SUCCEEDED run whose own report flagged a
concern as not clean, and is claimed once per closer per day in the shared
flow-doctor table so a re-run or a late duplicate cannot send a second one
(alpha-engine-config-I13334 review findings, 2026-11-10).
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, cast

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
from failure_class import classify_failure
from flow_doctor_telegram import build_flow_doctor_config, notify_via_flow_doctor
from weekly_run_report import (
    WEEKLY_PIPELINE_NAME,
    build_weekly_report,
    format_report_lines,
    overall_auto_fix_line,
    report_needs_triage,
    RESOLUTIONS_S3_KEY,
    S3_BUCKET,
    load_bundled_resolutions,
    md_escape,
    parse_resolutions,
)
from nousergon_lib.flow_doctor_fleet import (
    FleetTelegramTopic,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
_FLOW_NAME = "sf-telegram-notifier"
# The one daily pipelines message goes to ONE topic, whatever the outcome.
# It used to fan out to PIPELINE_OBSERVER_TELEGRAM_TOPICS, whose per-topic
# severity filters split the outcomes: an all-green day (info) reached only
# the Pipeline topic, a day with a failure (warning) reached Pipeline AND
# Ops health. Brian 2026-11-14: whatever gets one outcome gets the other.
# Pipeline accepts both info and warning, so both outcomes land there alone.
_DAY_MESSAGE_TOPICS: tuple[FleetTelegramTopic, ...] = (FleetTelegramTopic.PIPELINE,)
_DB_BASENAME = "flow_doctor_sf_telegram_notifier"

_SF_LABELS: dict[str, str] = {
    "ne-weekly-freshness-pipeline": "Weekly Freshness SF",
    "ne-preopen-trading-pipeline": "Pre-open Trading SF",
    "ne-postclose-trading-pipeline": "Post-close Trading SF",
    # 2026-09-30 (alpha-engine-config-I11269 follow-up): the collector-dependent
    # half of the post-close pipeline, split into its own machine. Its terminal
    # is the one that carries the EOD artifact check below.
    "ne-postclose-reconcile-pipeline": "Post-close Reconcile SF",
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
        _DAY_MESSAGE_TOPICS,
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


def _parse_output(describe_resp: dict | None) -> dict:
    """Parsed execution output (the final state record), or ``{}``."""
    if not describe_resp:
        return {}
    try:
        payload = json.loads(describe_resp.get("output") or "{}")
    except (ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _start_datetime(started_ms: int | None) -> datetime:
    if started_ms is None:
        return datetime.now(tz=timezone.utc)
    return datetime.fromtimestamp(int(started_ms) / 1000, tz=timezone.utc)


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
    # Its starters (alpha-engine-eod-backstop's collection-terminal trigger and
    # reconcile backstop) stamp pipeline_role="eod", the same canonical role.
    "ne-postclose-reconcile-pipeline": "eod",
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
    sf_client: Any = None,  # noqa: ANN401  # pyright: ignore[reportExplicitAny] — untyped boto3 client
    s3_client: Any = None,  # noqa: ANN401  # pyright: ignore[reportExplicitAny] — untyped boto3 client
) -> tuple[str, bool, bool, bool, bool, str | None]:
    """Return ``(text, silent, hollow_suspect, is_partial, needs_triage,
    artifact_concern)``.

    ``artifact_concern`` names what the reconcile terminal's EOD artifact check
    found wrong (``"eod artifacts missing"`` / ``"eod artifacts unverified"``),
    or None when the check passed or did not apply. The day message reads it so
    a run that SUCCEEDED without writing its artifacts never counts as clean.

    ``needs_triage`` is True when a weekly terminal carries issue bullets or a
    non-green Director verdict (see weekly_run_report.py). The handler then
    sends at warning severity so the report reaches the alert intake the
    nightly sweep reads, which is what its ``Auto-fix:`` lines promise.

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
    needs_triage = False
    artifact_concern: str | None = None

    lines = [f"{emoji} *{label} — {display_status}*"]

    if status == "RUNNING":
        lines.append(f"Execution: {exec_name}")
        return "\n".join(lines), True, False, is_partial, False, None

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
        # post-close RECONCILE SUCCEEDED/DEGRADED terminal (EOD_PIPELINE_NAME is
        # ne-postclose-reconcile-pipeline since the 2026-09-30 split — the
        # machine that writes the eod_pnl row) additionally gets its day's
        # artifacts verified — a "SUCCESS" that did not write eod_pnl.csv is
        # the failure mode this issue exists to catch. s3_client is already a
        # real client here (eod is never preflight, so the block above always
        # provisions one).
        if sm_name == EOD_PIPELINE_NAME and (status == "SUCCEEDED" or is_degraded):
            artifact_status = verify_eod_artifacts(s3_client, run_date)
            lines.extend(format_eod_artifact_lines(artifact_status))
            if artifact_status is None:
                artifact_concern = "eod artifacts unverified"
            elif not artifact_status.all_present:
                artifact_concern = "eod artifacts missing"

        # Brian, 2026-10-24: a weekly "SUCCEEDED" must also list every step
        # that was less than perfect, each with its auto-fix, and the
        # Director's overall status and top 3 concerns. Real cadence runs
        # only: a preflight shell or a narrowed partial run is not the week.
        if (
            sm_name == WEEKLY_PIPELINE_NAME
            and not is_preflight
            and not is_partial
            and (status == "SUCCEEDED" or is_degraded)
        ):
            exec_output = _parse_output(describe_resp)
            report = build_weekly_report(
                s3_client=s3_client,
                # The definition normalizes run_date to the cycle's trading
                # day; the scheduled input usually carries none.
                run_date=str(exec_output.get("run_date") or run_date or "") or None,
                execution_arn=execution_arn,
                execution_start=_start_datetime(detail.get("startDate")),
                exec_input=_execution_input(describe_resp),
                exec_output=exec_output,
            )
            needs_triage = report_needs_triage(report)
            if report.issues:
                lines[0] = f"{lines[0]} — {len(report.issues)} issue{'s' if len(report.issues) != 1 else ''}"
            lines.extend(format_report_lines(report))
            lines.append(overall_auto_fix_line(report))

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

    # Brian, 2026-11-02: every failure alert says whether anything retries it.
    # The weekly terminal already carries its own report-level Auto-fix line.
    if status in ("FAILED", "TIMED_OUT") and not any(
        line.startswith("Auto-fix:") for line in lines
    ):
        full_cause = detailed_failure_cause or _failure_cause_from(describe_resp)
        lines.append(classify_failure(full_cause).auto_fix_line)
    if status in ("FAILED", "TIMED_OUT", "ABORTED") and not any(
        line.startswith("Resolution:") for line in lines
    ):
        resolution = terminal_resolution_line(sm_name, status)
        if resolution:
            lines.append(resolution)

    lines.append(f"Execution: {exec_name}")
    silent = False
    if status == "SUCCEEDED" and hollow_suspect:
        silent = False
    # A narrowed run is delivered without a push. It is still sent, still
    # readable in the topic, and still recorded — it just does not buzz, because
    # the cadence cycle it is named after did not fail.
    if is_partial:
        silent = True
    return "\n".join(lines), silent, hollow_suspect, is_partial, needs_triage, artifact_concern


_REPO_SHORT = {"alpha-engine-config": "aec", "nousergon-data": "data", "nous-ergon-ops": "ops"}


def terminal_resolution_line(sm_name: str, status: str) -> str | None:
    """``Resolution: <issue> <plan>`` for a terminal alert, or None.

    alpha-engine-config-I13257: flow-doctor >= 0.19.7 moves a sender's own
    ``Resolution:`` line into the footer in place of "none tracked yet". The
    key is ``terminal:<state machine>:<status>``, matched (fnmatch, first wins)
    against the same registry the weekly report reads: the S3 overlay the
    nightly sweep keeps current, then the bundled seed. No match, no line: the
    footer's default stands, and a number is never invented.
    """
    key = f"terminal:{sm_name}:{status}"
    resolutions: dict = {}
    try:
        body = boto3.client("s3", region_name=REGION).get_object(
            Bucket=S3_BUCKET, Key=RESOLUTIONS_S3_KEY
        )["Body"].read()
        resolutions = dict(parse_resolutions(json.loads(body)))
    except Exception as exc:  # noqa: BLE001 — overlay is optional; seed below
        logger.info("resolution overlay unread (%s); using the bundled seed", type(exc).__name__)
    for pattern, res in load_bundled_resolutions().items():
        resolutions.setdefault(pattern, res)
    for pattern, res in resolutions.items():
        if fnmatch.fnmatchcase(key, pattern):
            repo, _, num = res.ref.partition("#")
            label = f"{_REPO_SHORT.get(repo, repo)}#{num}"
            plan = f" ({md_escape(res.plan)})" if res.plan else ""
            return f"Resolution: {label}{plan}"
    return None


#: The terminal that closes a pipeline day: the weekday post-close reconcile,
#: and the Saturday weekly run. Only these send to the chat (§7.2b class 4).
_DAY_CLOSERS: frozenset[str] = frozenset({
    "ne-postclose-reconcile-pipeline",
    "ne-weekly-freshness-pipeline",
})
#: The pipelines a day's message counts. The groom dispatch SF is backlog
#: plumbing, not a pipeline Brian reads about.
_DAY_PIPELINES: tuple[str, ...] = tuple(n for n in _SF_LABELS if n != "alpha-engine-groom-dispatch")
_DAY_WINDOW_S = 20 * 3600
# Bound on list_executions pages walked per pipeline for the day message (100
# runs a page). Exhausting it inside the window reads UNREADABLE, never absent.
_DAY_LIST_MAX_PAGES = 5
_PIPELINES_URL = "https://console.nousergon.ai/component"


#: Shared flow-doctor DynamoDB table (``build_flow_doctor_config``'s store),
#: which this role can already PutItem/GetItem. The two record kinds below use
#: their own ``pk`` prefix and carry none of flow-doctor's index attributes
#: (``flow_name`` / ``action_type`` / ``error_signature`` / ``entity``), so no
#: flow-doctor query or scan ever sees them.
_LEDGER_TABLE = "flow-doctor-store"
_LEDGER_TTL_S = 14 * 86400


def _ddb() -> Any:  # noqa: ANN401  # pyright: ignore[reportExplicitAny] — untyped boto3 client
    return boto3.client("dynamodb", region_name=REGION)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]


def _error_code(exc: Exception) -> str:
    response: object = getattr(exc, "response", None)
    if isinstance(response, dict):
        error: object = cast("dict[str, object]", response).get("Error")
        if isinstance(error, dict):
            return str(cast("dict[str, object]", error).get("Code") or "")
    return ""


def _ledger_item(pk: str, now_s: float, **fields: str) -> dict[str, dict[str, str]]:
    item = {"pk": {"S": pk}, "sk": {"S": "META"},
            "expires_at": {"N": str(int(now_s) + _LEDGER_TTL_S)}}
    item.update({k: {"S": v} for k, v in fields.items()})
    return item


def _verdict_pk(execution_arn: str) -> str:
    return f"{_FLOW_NAME}#run-verdict#{execution_arn}"


def _put_verdict(execution_arn: str, concern: str) -> None:
    """Remember what a run's own report found wrong, for the day message.

    Only a run with a concern is written; an absent row means the report was
    clean. Never raises: the per-run record must not fail on the ledger."""
    if not execution_arn:
        return
    try:
        _ddb().put_item(
            TableName=_LEDGER_TABLE,
            Item=_ledger_item(_verdict_pk(execution_arn),
                              datetime.now(timezone.utc).timestamp(), concern=concern),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("run verdict not recorded for %s: %s", execution_arn, type(exc).__name__)


def _read_verdict(execution_arn: str) -> str | None:
    """The concern a run's report recorded, or None (clean, or unreadable)."""
    if not execution_arn:
        return None
    try:
        resp: object = _ddb().get_item(
            TableName=_LEDGER_TABLE,
            Key={"pk": {"S": _verdict_pk(execution_arn)}, "sk": {"S": "META"}},
            ConsistentRead=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("run verdict unreadable for %s: %s", execution_arn, type(exc).__name__)
        return None
    item: object = cast("dict[str, object]", resp).get("Item") if isinstance(resp, dict) else None
    if not isinstance(item, dict):
        return None
    concern: object = cast("dict[str, object]", item).get("concern")
    if isinstance(concern, dict):
        value: object = cast("dict[str, object]", concern).get("S")
        return value if isinstance(value, str) and value else None
    return None


def _run_concern(
    status: str, *, hollow_suspect: bool, needs_triage: bool, artifact_concern: str | None,
) -> str | None:
    """What a run's own report flagged that its raw ``status`` does not say."""
    if status != "SUCCEEDED":
        return None
    concerns = [c for c in (
        artifact_concern,
        "hollow-suspect" if hollow_suspect else None,
        "needs triage" if needs_triage else None,
    ) if c]
    return ", ".join(concerns) or None


def _day_pk(key: str) -> str:
    return f"{_FLOW_NAME}#day-message#{key}"


def _claim_day(key: str, *, now_s: float) -> bool:
    """Claim the day's one message. False when it was already sent.

    A conditional put on a row that outlives flow-doctor's 1-minute signature
    cooldown, so a closer re-run or a late duplicate terminal hours later is
    refused. A row left by a send that failed (``state = failed``) may be
    reclaimed. Any other store error fails OPEN: a day with no message reads
    exactly like a day that never ran, which is worse than a second one."""
    try:
        _ddb().put_item(
            TableName=_LEDGER_TABLE,
            Item=_ledger_item(_day_pk(key), now_s, state="claimed"),
            ConditionExpression="attribute_not_exists(pk) OR #st = :failed",
            ExpressionAttributeNames={"#st": "state"},
            ExpressionAttributeValues={":failed": {"S": "failed"}},
        )
    except Exception as exc:  # noqa: BLE001
        if _error_code(exc) == "ConditionalCheckFailedException":
            logger.info("day message %s already sent; not sending again", key)
            return False
        logger.warning("day-message claim unreadable (%s); sending anyway", type(exc).__name__)
    return True


def _release_day(key: str, *, now_s: float) -> None:
    """Mark the claim failed so a later closer terminal may still send."""
    try:
        _ddb().put_item(TableName=_LEDGER_TABLE, Item=_ledger_item(_day_pk(key), now_s, state="failed"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("day-message claim not released for %s: %s", key, type(exc).__name__)


def _is_counted_run(machine_arn: str, execution_arn: str) -> bool:
    """A run the day message counts: not a preflight shell, not a narrowed
    partial run. The same tests ``_closes_day`` applies to the closer."""
    describe_resp = cast("dict[str, object] | None", _describe_execution(execution_arn))
    return not _is_preflight_execution(describe_resp) and not _is_partial_execution(
        machine_arn, describe_resp)[0]


def _day_outcomes(
    sm_arn: str,
    *,
    sf_client: Any,  # noqa: ANN401  # pyright: ignore[reportExplicitAny] — untyped boto3 client
    now_s: float,
    concerns: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """``[(label, status)]`` for each pipeline's latest COUNTED run in the last 20 h.

    Pipelines with no run in the window are left out, not counted as failed:
    a weekday has no weekly run and a Saturday has no trading runs. A list
    call that fails is reported as ``UNREADABLE`` so the day never reads as
    cleaner than it was.

    Preflight shells and partial verification runs (``skip_*`` flags with no
    canonical ``pipeline_role``, e.g. ``director-verify-*``) are skipped, so a
    later narrowed run never stands in for the cadence run in either direction.
    A SUCCEEDED run whose own report flagged a concern (missing EOD artifacts,
    hollow-suspect, weekly needs-triage) is reported with that concern, never
    as a plain success. ``concerns`` maps execution ARN to a concern already
    known in this invocation (the closer's own), ahead of the ledger."""
    prefix = sm_arn.rsplit(":", 1)[0]
    known = concerns or {}
    out: list[tuple[str, str]] = []
    for name in _DAY_PIPELINES:
        machine_arn = f"{prefix}:{name}"
        try:
            status = _latest_counted_status(
                machine_arn, sf_client=sf_client, now_s=now_s, known=known)
        except Exception as exc:  # noqa: BLE001 — one unreadable machine must not drop the day
            logger.warning("day outcome for %s unreadable: %s", name, exc)
            out.append((_SF_LABELS[name], "UNREADABLE"))
            continue
        if status is not None:
            out.append((_SF_LABELS[name], status))
    return out


def _latest_counted_status(
    machine_arn: str,
    *,
    sf_client: Any,  # noqa: ANN401  # pyright: ignore[reportExplicitAny] — untyped boto3 client
    now_s: float,
    known: dict[str, str],
) -> str | None:
    """Status of the machine's newest counted run inside the window, or None.

    Pages through ``list_executions`` (newest first) until a counted run is
    found, a page reaches past the window, or the listing ends. Reading one
    page only let ten newer partial or preflight runs hide the cadence run, so
    the pipeline silently dropped out of the day message. ``_DAY_LIST_MAX_PAGES``
    bounds the walk; running out of pages inside the window raises, so the
    pipeline reads ``UNREADABLE`` rather than absent."""
    kwargs: dict[str, object] = {"stateMachineArn": machine_arn, "maxResults": 100}
    for _ in range(_DAY_LIST_MAX_PAGES):
        resp = sf_client.list_executions(**kwargs)
        page = list(resp.get("executions", []))
        in_window = [e for e in page if now_s - _epoch_s(e.get("startDate")) <= _DAY_WINDOW_S]
        runs = sorted(
            (e for e in in_window
             if not str(e.get("name", "")).startswith(("offcycle-shell", "preflight"))),
            key=lambda e: _epoch_s(e.get("startDate")),
            reverse=True,
        )
        for run in runs:
            execution_arn = str(run.get("executionArn") or "")
            if not _is_counted_run(machine_arn, execution_arn):
                continue
            status = str(run.get("status", "UNKNOWN"))
            if status == "SUCCEEDED":
                concern = known.get(execution_arn) or _read_verdict(execution_arn)
                if concern:
                    status = f"SUCCEEDED, {concern}"
            return status
        token = resp.get("nextToken")
        if not token or len(in_window) < len(page):
            return None  # listing ended, or this page already reached past the window
        kwargs["nextToken"] = token
    raise RuntimeError(f"no counted run within {_DAY_LIST_MAX_PAGES} pages")


def _epoch_s(value: object) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        return value / 1000 if value > 1e11 else float(value)
    return 0.0


def day_message(run_date: str | None, outcomes: list[tuple[str, str]]) -> str:
    """The one pipelines message for the day. Pure."""
    day = run_date or "today"
    ok = [label for label, status in outcomes if status == "SUCCEEDED"]
    bad = [f"{label} ({status.lower()})" for label, status in outcomes if status != "SUCCEEDED"]
    if not bad:
        head = f"✅ Pipelines {day}: all {len(ok)} succeeded."
    else:
        head = (f"\U0001f534 Pipelines {day}: {len(ok)} of {len(outcomes)} succeeded. "
                f"Failed: {', '.join(bad)}. Tonight's sweep fixes them; nothing for you.")
    return f"{head}\n{_PIPELINES_URL}"


def _record(text: str, *, silent: bool, severity: str, dedup_key: str) -> bool:
    """Put a per-run report on the alert bus as tracked-only. Never raises."""
    from krepis.fleet_events import emit_alert_event

    return emit_alert_event(
        origin="sf-telegram-notifier", body=text, severity_raw=severity,
        source="flow-doctor:sf-telegram-notifier", dedup_key=dedup_key,
        disable_notification=silent, delivery_tier="tracked-only",
        alert_class="sf_completion_telegram",
    )


def handler(event: dict[str, object], context: object) -> dict[str, object]:  # noqa: ARG001
    """Record every run; send the chat ONE message a day (§7.2b)."""
    result = _record_run(event)
    raw_detail = event.get("detail")
    detail = cast("dict[str, object]", raw_detail) if isinstance(raw_detail, dict) else {}
    if _closes_day(detail):
        concern = result.get("concern")
        result["day_message_sent"] = _send_day_message(
            detail, closer_concern=concern if isinstance(concern, str) else None)
    return result


def _closes_day(detail: dict) -> bool:
    status = detail.get("status", "UNKNOWN")
    sm_arn = detail.get("stateMachineArn") or ""
    if status not in _TERMINAL_STATUSES or sm_arn.rsplit(":", 1)[-1] not in _DAY_CLOSERS:
        return False
    describe_resp = _describe_execution(detail.get("executionArn", ""))
    return not _is_preflight_execution(describe_resp) and not _is_partial_execution(sm_arn, describe_resp)[0]


def _send_day_message(detail: dict[str, object], *, closer_concern: str | None = None) -> bool:
    sm_arn = str(detail.get("stateMachineArn") or "")
    sm_name = sm_arn.rsplit(":", 1)[-1]
    execution_arn = str(detail.get("executionArn") or "")
    exec_name = str(detail.get("name") or "")
    describe_resp = cast("dict[str, object] | None", _describe_execution(execution_arn))
    raw_input = (describe_resp or {}).get("input")
    run_date = parse_run_date_from_input(raw_input if isinstance(raw_input, str) else None) or \
        parse_run_date_from_execution_name(exec_name)
    now = datetime.now(timezone.utc)
    # One message per closer per day. The run date names the day; a run with
    # none falls back to the UTC calendar day, never to the execution name,
    # which differs on every re-run and so would dedup nothing.
    day_key = f"{sm_name}:{run_date or now.date().isoformat()}"
    if not _claim_day(day_key, now_s=now.timestamp()):
        return False
    # From here on the claim is held. Every way out that is not a delivered
    # message must release it, a raise included: a claim left behind refuses
    # every later closer terminal and Lambda retry, and the day's message is
    # lost for good (cross-family review of nousergon-data#2459).
    sent = False
    try:
        sf_client: Any = boto3.client("stepfunctions", region_name=REGION)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportExplicitAny]
        outcomes = _day_outcomes(
            sm_arn,
            sf_client=sf_client,
            now_s=now.timestamp(),
            concerns={execution_arn: closer_concern} if execution_arn and closer_concern else None,
        )
        failed = any(st != "SUCCEEDED" for _, st in outcomes)
        sent = notify_via_flow_doctor(
            day_message(run_date, outcomes),
            silent=False,
            severity="warning" if failed else "info",
            dedup_key=f"{_FLOW_NAME}:day:{day_key}",
            flow_name=_FLOW_NAME,
            topics=_DAY_MESSAGE_TOPICS,
            db_basename=_DB_BASENAME,
            # Out of the shared daily budget, and guaranteed to land: a once-a-day
            # result that is suppressed reads exactly like a day that never ran
            # (Brian's 2026-08-17 ruling, carried over).
            rate_limit_exempt_severities=_RATE_LIMIT_EXEMPT_SEVERITIES,
            guaranteed_topic=FleetTelegramTopic.PIPELINE,
            context={"state_machine": sm_name, "execution": exec_name, "day": run_date},
            # Matches playbooks.yaml's registered `sf_completion_telegram` class.
            source="flow-doctor:sf-telegram-notifier",
        )
    finally:
        if not sent:
            _release_day(day_key, now_s=now.timestamp())
    return sent


def _record_run(event: dict) -> dict[str, object]:
    """The per-run report, built exactly as before 2026-11-06 and RECORDED
    (alert bus, tracked-only: /page/incidents and the nightly sweep), never
    sent to the chat (observability-policy §7.2b)."""
    detail = event.get("detail") or {}
    status = detail.get("status", "UNKNOWN")
    sm_name = (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1]
    logger.info("SF status change: sf=%s status=%s", sm_name, status)

    text, silent, hollow_suspect, is_partial, needs_triage, artifact_concern = _build_message(detail)
    ok = _record(
        text,
        silent=silent,
        severity=(
            "info" if is_partial
            else "warning" if (hollow_suspect or needs_triage) and status == "SUCCEEDED"
            else _severity_for_status(status)
        ),
        dedup_key=_dedup_key(detail),
    )
    # What this report flagged beyond its raw status, kept for the day message
    # so a SUCCEEDED run with a problem never counts as a clean success there.
    typed_detail = cast("dict[str, object]", detail)
    concern = _run_concern(
        str(typed_detail.get("status") or "UNKNOWN"), hollow_suspect=hollow_suspect,
        needs_triage=needs_triage, artifact_concern=artifact_concern,
    )
    if concern and not is_partial:
        _put_verdict(str(typed_detail.get("executionArn") or ""), concern)

    return {
        "status": status,
        "state_machine": sm_name,
        "execution": detail.get("name", ""),
        "telegram_sent": False,
        "recorded": ok,
        "silent": silent,
        "hollow_suspect": hollow_suspect,
        "concern": concern,
    }
