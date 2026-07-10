"""alpha-engine-sf-watch-dispatcher — Fleet-SF Watch.

On a terminal failure of ANY of the three fleet Step Functions — Saturday
(`ne-weekly-freshness-pipeline`), Weekday (`ne-preopen-trading-pipeline`),
or EOD (`ne-postclose-trading-pipeline`) — this dispatcher writes a watch-log
artifact and (when `AGENT_DISPATCH_ENABLED=true`) fires a `repository_dispatch`
that triggers the autonomous resilience agent (diagnose→fix→merge→rerun) in
`alpha-engine-config`. Generalized from the Saturday-only dispatcher
(spec: nousergon/alpha-engine-config#1227, fleet fan-out: #1375).

**Per-pipeline registry.** ``PIPELINES`` maps each SF name → its watch-log
prefix + repository_dispatch event type, so the GHA workflow + dashboard filter
by cadence and each pipeline carries its OWN kill-switch (in the agent charter).
weekday + EOD ship PROPOSE-ONLY and soak before autonomous-merge is flipped on,
independently of Saturday. Fan-out is additive: register a pipeline here, add its
ARN to the single EventBridge rule (deploy.sh), widen the IAM ARNs.

**Why this is NOT a second notifier.** The fleet already has
`alpha-engine-sf-telegram-notifier` (subscribes to all three SFs / all statuses,
pings loud on FAILED with the cause). This Lambda's distinct responsibilities are:
the **per-pipeline, terminal-failure-only** trigger (the seam the agent dispatch
hangs off), the **watch-log artifact** the dashboard page reads, and — ONLY when
it actually takes recovery action (agent dispatch or fast-path rerun) — a
**distinct, SILENT** Telegram receipt (sf-telegram-notifier already buzzed loud
on the failure; observe-only paths are watch-log-only, no Fleet-SF Watch ping).

**Fail-loud (CLAUDE.md no-silent-fails).** The watch-log artifact write is the
primary deliverable → it RAISES on failure so a broken producer surfaces via the
Lambda error metric + CW alarm. Enrichment (DescribeExecution /
GetExecutionHistory), the Telegram record, and the agent dispatch are secondary
observability hung off the primary path: their failure is logged at WARNING and
recorded in the artifact — the artifact still records that a failure was detected.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import date, datetime, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import (
    FleetTelegramTopic,
    PIPELINE_OBSERVER_TELEGRAM_TOPICS,
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
_FLOW_NAME = "saturday-sf-watch-dispatcher"
_DB_BASENAME = "flow_doctor_saturday_sf_watch_dispatcher"
# M2 gate — default OFF. When true, a failure also fires a repository_dispatch
# that triggers the autonomous resilience agent (which diagnoses → fixes →
# merges → reruns from the failed step). The watch-log is written FIRST (so the
# agent reads fresh context), THEN the dispatch fires. The per-pipeline
# autonomous-merge kill-switch is enforced agent-side (charter STEP 0), so this
# single env gate can stay on fleet-wide while weekday/EOD soak in PROPOSE-ONLY.
AGENT_DISPATCH_ENABLED = (
    os.environ.get("AGENT_DISPATCH_ENABLED", "false").lower() == "true"
)
# config#1900 — deterministic zero-token fast path. When true, a failure whose
# execution history exactly matches a known-transient signature (see
# _match_transient_signature) is recovered by a plain fresh rerun started BY
# THIS LAMBDA — no agent dispatch, no tokens. Strictly narrower than the agent:
# first-failure-of-the-day only, never when an order-emitting state ran, never
# on preflight/operator-abort, and any fall-through (signature miss, prior
# attempt, StartExecution error) lands on the normal agent dispatch path.
# OPERATOR-OWNED runtime flag like AGENT_DISPATCH_ENABLED (deploy.sh preserves
# the live value across redeploys — the config#1818 lesson).
FAST_PATH_ENABLED = (
    os.environ.get("FAST_PATH_ENABLED", "false").lower() == "true"
)
# repository_dispatch target — the private alpha-engine-config repo hosts the
# agent GHA workflow (on: repository_dispatch, types: [*-sf-failure]).
DISPATCH_REPO = os.environ.get("DISPATCH_REPO", "nousergon/alpha-engine-config")
# Dedicated fine-grained PAT (SecureString) scoped to the SF-path repos, shared
# across pipelines. Read at dispatch time only — never logged.
GITHUB_PAT_SSM_PARAM = os.environ.get(
    "GITHUB_PAT_SSM_PARAM", "/alpha-engine/saturday_sf_watch/github_pat"
)
_DISPATCH_TIMEOUT_SEC = 15

# --- Per-pipeline registry -------------------------------------------------
# Fan-out is ADDITIVE: register a pipeline here, add its ARN to the single
# EventBridge rule (deploy.sh), widen the IAM ARNs. Everything below — the
# watch-log contract, dispatch, the agent charter — is pipeline-agnostic.
# Each pipeline routes to its OWN watch-log prefix + repository_dispatch event
# type (cadence-filterable) and its OWN kill-switch (charter-enforced).
# `cadence_slug` is the FROZEN cadence label (saturday/weekday/eod) — the SFs
# were renamed to function-descriptive ne- names (config#1381) but the derived
# cadence-keyed resources (watch-log prefix, dispatch type, the charter's
# /alpha-engine/<slug>_sf_watch/ kill-switch param) were intentionally NOT
# renamed. The slug is the single source of truth for that mapping and is passed
# to the agent so the charter never has to parse it back out of the SF name.
# `label` is the human cadence label for the Telegram receipt.
PIPELINES: dict[str, dict[str, object]] = {
    "ne-weekly-freshness-pipeline": {
        "cadence_slug": "saturday",
        "label": "Weekly Freshness",
        "watch_prefix": "consolidated/saturday_sf_watch",
        "dispatch_event_type": "saturday-sf-failure",
        "has_listener": True,
    },
    "ne-preopen-trading-pipeline": {
        "cadence_slug": "weekday",
        "label": "Pre-open Trading",
        "watch_prefix": "consolidated/weekday_sf_watch",
        "dispatch_event_type": "weekday-sf-failure",
        "has_listener": True,
        # config#1900 — fast-path signature scope for THIS pipeline only.
        # `poll_states` are the SSM-poll Task states whose Lambda output carries
        # the raw host-death evidence (status/status_details/ping_status);
        # `data_task_states` are the ephemeral-data-spot SSM states where an
        # Ssm.InvalidInstanceIdException means the target box died (transient);
        # `veto_states` — if ANY of these ever entered, the fast path is
        # forbidden regardless of signature (order-emitting surface: a rerun
        # decision there belongs to the agent's Lane-D discipline, never a
        # deterministic rule).
        "fast_path": {
            "poll_states": frozenset(
                {"WaitForMorningEnrich", "WaitForMorningArcticAppend", "WaitForChronicGap"}
            ),
            "data_task_states": frozenset(
                {"MorningEnrich", "MorningArcticAppend", "ChronicGapSelfHeal"}
            ),
            "veto_states": frozenset({"RunMorningPlanner", "RunDaemon"}),
        },
    },
    "ne-postclose-trading-pipeline": {
        "cadence_slug": "eod",
        "label": "Post-close Trading",
        "watch_prefix": "consolidated/eod_sf_watch",
        "dispatch_event_type": "eod-sf-failure",
        "has_listener": True,
    },
    # TRANSITIONAL (remove at the SF-rename cutover — config#1408 / re-exam
    # 2026-07-03). The EOD SF still runs under its OLD name `alpha-engine-eod-
    # pipeline` (saturday/weekday already renamed; EOD lags). Until cutover, the
    # LIVE failures arrive under the old ARN, so the registry must recognize it —
    # otherwise the handler ignores the event (the 2026-06-29 dead-watch gap).
    # Routes to the SAME eod cadence resources as ne-postclose-trading-pipeline.
    "alpha-engine-eod-pipeline": {
        "cadence_slug": "eod",
        "label": "Post-close Trading",
        "watch_prefix": "consolidated/eod_sf_watch",
        "dispatch_event_type": "eod-sf-failure",
        "has_listener": True,
    },
}
SCHEMA_VERSION = 1
_CAUSE_MAX_CHARS = 600
# config#1827 — human-abort carve-out. An `ABORTED` execution whose top-level
# `error` is one of these markers is a DELIBERATE operator/human stop, not a
# failure needing autonomous recovery: suppress the agent dispatch (still record
# + Telegram loudly). Keep this set SMALL and EXPLICIT — do NOT suppress on bare
# `ABORTED`, because a programmatic/self-abort can still be a real defect that
# must dispatch. `OperatorAbort` is the marker the fleet's manual-stop path sets.
OPERATOR_ABORT_ERRORS = frozenset({"OperatorAbort"})
# Bound the history scan: fetch the newest N events (reverseOrder), reconstruct
# chronological order locally to find the entered-but-not-exited state. The
# failed state's enclosing StateEntered is always in the tail of the history.
_HISTORY_MAX_EVENTS = 1000


def _sf_client():
    return boto3.client("stepfunctions", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _describe_execution(execution_arn: str) -> dict | None:
    """Best-effort DescribeExecution → top-level error/cause + input. None on error."""
    if not execution_arn:
        return None
    try:
        return _sf_client().describe_execution(executionArn=execution_arn)
    except Exception as exc:  # noqa: BLE001 — enrichment, recorded in artifact
        logger.warning("describe_execution failed for %s: %s", execution_arn, exc)
        return None


def _failure_cause(describe_resp: dict | None) -> str:
    if not describe_resp:
        return ""
    error = (describe_resp.get("error") or "").strip()
    cause = (describe_resp.get("cause") or "").strip()
    snippet = f"{error}: {cause}" if (error and cause) else (error or cause)
    if len(snippet) > _CAUSE_MAX_CHARS:
        snippet = snippet[: _CAUSE_MAX_CHARS - 1] + "…"
    return snippet


def _is_operator_abort(status: str, describe_resp: dict | None) -> bool:
    """True iff this is a deliberate human stop — ``status == "ABORTED"`` AND the
    execution's top-level ``error`` is an explicit operator-abort marker
    (config#1827). Deliberately narrow: an ``ABORTED`` with any other error (or
    no error) is NOT treated as operator-initiated, so a programmatic/self-abort
    still dispatches a recovery agent (guards against over-suppression, the
    fail-loud violation of the inverse kind)."""
    if status != "ABORTED":
        return False
    if not describe_resp:
        return False
    error = (describe_resp.get("error") or "").strip()
    return error in OPERATOR_ABORT_ERRORS


def _is_preflight(describe_resp: dict | None) -> bool:
    """True iff execution input has ``shell_run=true`` (the Friday-PM dry-pass)."""
    if not describe_resp:
        return False
    try:
        payload = json.loads(describe_resp.get("input") or "{}")
    except (ValueError, TypeError):
        return False
    return bool(payload.get("shell_run"))


def _failed_state_from_history(execution_arn: str) -> str | None:
    """Return the name of the state that was active (entered, not yet exited)
    when the execution failed — i.e. the culprit state.

    Fetches the newest ``_HISTORY_MAX_EVENTS`` events (reverseOrder), reverses
    them to chronological order, and tracks the entered-but-not-exited state via
    a forward scan. A state that fails enters but never cleanly exits, so it is
    the one left dangling at the terminal failure event. Best-effort: returns
    ``None`` on any API error (recorded in the artifact).
    """
    if not execution_arn:
        return None
    try:
        resp = _sf_client().get_execution_history(
            executionArn=execution_arn,
            maxResults=_HISTORY_MAX_EVENTS,
            reverseOrder=True,
            includeExecutionData=False,
        )
    except Exception as exc:  # noqa: BLE001 — enrichment, recorded in artifact
        logger.warning("get_execution_history failed for %s: %s", execution_arn, exc)
        return None

    events = list(reversed(resp.get("events", [])))  # → chronological
    current: str | None = None
    for ev in events:
        etype = ev.get("type", "")
        if etype.endswith("StateEntered"):
            det = ev.get("stateEnteredEventDetails") or {}
            current = det.get("name") or current
        elif etype.endswith("StateExited"):
            det = ev.get("stateExitedEventDetails") or {}
            if det.get("name") == current:
                current = None
    return current


# --- config#1900: deterministic zero-token fast path -------------------------
# Signature ids (stable, recorded in the watch-log + Telegram receipt):
#   data_spot_host_death      — SSM poll evidence says the command never ran on a
#                               live box (Undeliverable / DeliveryTimedOut, or
#                               rc=-1 with the agent unregistered): a spot
#                               reclaim / host death mid-data-state. Matched on
#                               the RAW poll fields, not the poller's `verdict`
#                               label, so the match is stable across poller
#                               classification changes (nousergon-data#675).
#   data_spot_invalid_instance — SendCommand itself rejected with
#                               Ssm.InvalidInstanceIdException on a data state:
#                               the target spot died before delivery.
# Both mean: no code defect, the ephemeral data spot vanished; recovery is a
# PLAIN fresh rerun (the SF relaunches its own spot; id artifact is
# execution-scoped since nousergon-data#676).
_HOST_DEATH_STATUS_DETAILS = frozenset({"Undeliverable", "DeliveryTimedOut"})
_HOST_DEATH_PING_STATUSES = frozenset({"NotRegistered", "ConnectionLost", "Inactive"})


def _fetch_history_with_data(execution_arn: str) -> list[dict] | None:
    """Newest ``_HISTORY_MAX_EVENTS`` events WITH payloads (the poll-state
    outputs carry the host-death evidence), reversed to chronological order.
    Best-effort: ``None`` on any API error → the caller falls through to the
    normal agent dispatch (never guess a signature without evidence)."""
    if not execution_arn:
        return None
    try:
        resp = _sf_client().get_execution_history(
            executionArn=execution_arn,
            maxResults=_HISTORY_MAX_EVENTS,
            reverseOrder=True,
            includeExecutionData=True,
        )
    except Exception as exc:  # noqa: BLE001 — fast path is optional, dispatch remains
        logger.warning("fast-path history fetch failed for %s: %s", execution_arn, exc)
        return None
    return list(reversed(resp.get("events", [])))


def _scan_history_for_fast_path(events: list[dict], fp_cfg: dict) -> dict:
    """One chronological walk collecting everything the signature match needs:
    whether any order-emitting veto state ever entered, the LAST poll-state
    Lambda output payload, and any TaskFailed errors on the data-spot states."""
    veto_entered = False
    last_poll_payload: dict | None = None
    data_task_errors: list[str] = []
    current: str | None = None
    for ev in events:
        etype = ev.get("type", "")
        if etype.endswith("StateEntered"):
            name = (ev.get("stateEnteredEventDetails") or {}).get("name")
            current = name or current
            if name in fp_cfg["veto_states"]:
                veto_entered = True
        elif etype.endswith("StateExited"):
            det = ev.get("stateExitedEventDetails") or {}
            if det.get("name") == current:
                current = None
        elif etype == "TaskSucceeded" and current in fp_cfg["poll_states"]:
            try:
                out = json.loads(
                    (ev.get("taskSucceededEventDetails") or {}).get("output") or "{}"
                )
            except (ValueError, TypeError):
                continue
            payload = out.get("Payload")
            if isinstance(payload, dict):
                last_poll_payload = payload
        elif etype == "TaskFailed" and current in fp_cfg["data_task_states"]:
            err = (ev.get("taskFailedEventDetails") or {}).get("error") or ""
            data_task_errors.append(err)
    return {
        "veto_entered": veto_entered,
        "last_poll_payload": last_poll_payload,
        "data_task_errors": data_task_errors,
    }


def _match_transient_signature(scan: dict) -> str | None:
    """EXACT-match against the known-transient signature table. No fuzzy
    matching: anything that doesn't match falls through to the agent."""
    payload = scan.get("last_poll_payload")
    if isinstance(payload, dict) and payload.get("status") == "Failed":
        if payload.get("status_details") in _HOST_DEATH_STATUS_DETAILS:
            return "data_spot_host_death"
        if (
            payload.get("response_code") == -1
            and payload.get("ping_status") in _HOST_DEATH_PING_STATUSES
        ):
            return "data_spot_host_death"
    if any(err == "Ssm.InvalidInstanceIdException" for err in scan.get("data_task_errors", [])):
        return "data_spot_invalid_instance"
    return None


def _prior_attempt_state(existing_events: list[dict]) -> tuple[int, int]:
    """(prior_attempts, prior_events) for today from the already-loaded
    watch-log. `agent_attempt`-marked events are agent OR fast-path attempts —
    both consume the SAME budget the charter's STEP 2 counts, so the two
    recovery layers can never exceed the shared 2-attempt ceiling."""
    attempts = sum(
        1
        for ev in existing_events
        if ev.get("agent_attempt") is not None or ev.get("action") == "fast_path_rerun"
    )
    return attempts, len(existing_events)


def _maybe_fast_path(
    record: dict,
    existing_events: list[dict],
    cfg: dict,
    sm_arn: str,
    describe_resp: dict | None,
    run_date: str,
) -> dict:
    """Deterministic recovery, strictly narrower than the agent (config#1900).

    Fires ONLY when ALL hold: flag on; this pipeline declares a `fast_path`
    scope; a genuine FAILED (not preflight, not operator-abort); the FIRST
    recovery attempt of the day (no prior agent/fast-path attempt, fewer than 2
    prior events — a repeat failure earns the agent's judgment); no
    order-emitting state ever entered; the history evidence EXACTLY matches a
    known-transient signature; and no concurrent execution is RUNNING (mutex).
    On success it mutates ``record`` in place (action/lane/attempt/rerun arn)
    BEFORE the watch-log write so the artifact carries the full audit trail.
    Every non-fire returns a reason; StartExecution errors are recorded on the
    record (`fast_path_error`) and fall through to the agent — never silent.
    """
    if not FAST_PATH_ENABLED:
        return {"fast_path": False, "reason": "disabled"}
    fp_cfg = cfg.get("fast_path")
    if not fp_cfg:
        return {"fast_path": False, "reason": "no_fast_path_config"}
    if record.get("status") != "FAILED":
        return {"fast_path": False, "reason": "not_failed_status"}
    if record.get("is_preflight"):
        return {"fast_path": False, "reason": "preflight"}
    if record.get("dispatch_suppressed"):
        return {"fast_path": False, "reason": record["dispatch_suppressed"]}
    if not describe_resp or not describe_resp.get("input"):
        return {"fast_path": False, "reason": "no_original_input"}
    prior_attempts, prior_events = _prior_attempt_state(existing_events)
    if prior_attempts > 0:
        return {"fast_path": False, "reason": "prior_attempt_exists"}
    if prior_events >= 2:
        return {"fast_path": False, "reason": "repeat_failure_day"}
    events = _fetch_history_with_data(record.get("execution_arn", ""))
    if events is None:
        return {"fast_path": False, "reason": "history_unavailable"}
    scan = _scan_history_for_fast_path(events, fp_cfg)
    if scan["veto_entered"]:
        return {"fast_path": False, "reason": "order_emitting_state_ran"}
    signature = _match_transient_signature(scan)
    if signature is None:
        return {"fast_path": False, "reason": "no_signature_match"}
    sf = _sf_client()
    try:
        running = sf.list_executions(
            stateMachineArn=sm_arn, statusFilter="RUNNING", maxResults=1
        ).get("executions", [])
    except Exception as exc:  # noqa: BLE001 — can't prove mutex free → agent decides
        logger.warning("fast-path list_executions failed: %s", exc)
        return {"fast_path": False, "reason": "mutex_check_unavailable"}
    if running:
        return {"fast_path": False, "reason": "execution_already_running"}
    detected_hms = record["detected_at"][11:19].replace(":", "")
    rerun_name = f"fast-path-rerun-{run_date}-{detected_hms}"
    try:
        resp = sf.start_execution(
            stateMachineArn=sm_arn,
            name=rerun_name,
            input=describe_resp["input"],
        )
    except Exception as exc:  # noqa: BLE001 — recorded on the artifact + agent takes over
        logger.warning("fast-path StartExecution failed (falling back to agent): %s", exc)
        record["fast_path_error"] = f"{type(exc).__name__}: {exc}"
        return {"fast_path": False, "reason": "start_execution_error"}
    record["action"] = "fast_path_rerun"
    record["lane"] = "A"
    record["agent_attempt"] = prior_attempts + 1
    record["fast_path_signature"] = signature
    record["rerun_execution_arn"] = resp.get("executionArn", "")
    logger.info(
        "fast-path rerun started: signature=%s rerun=%s", signature, rerun_name
    )
    return {"fast_path": True, "signature": signature, "rerun_execution_arn": record["rerun_execution_arn"]}


def _run_date(describe_resp: dict | None, detail: dict) -> str:
    """Resolve the Saturday firing date (YYYY-MM-DD) for the artifact key.

    Prefers the execution input's ``run_date`` (the canonical key the pipeline
    stamps its artifacts with), then the execution ``startDate`` epoch-ms, then
    ``now`` UTC. Keeps the watch-log aligned with the artifacts it will later
    report integrity on.
    """
    if describe_resp:
        try:
            payload = json.loads(describe_resp.get("input") or "{}")
            rd = payload.get("run_date")
            if isinstance(rd, str) and rd:
                return rd
        except (ValueError, TypeError):
            pass
    start_ms = detail.get("startDate")
    if isinstance(start_ms, (int, float)) and start_ms > 0:
        return datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _artifact_key(watch_prefix: str, run_date: str) -> str:
    return f"{watch_prefix}/{run_date}.json"


def _load_existing(s3, key: str) -> dict:
    """Read the existing watch-log for this date (so repeated failures in one
    run accumulate), or a fresh skeleton. A missing object (404/403) is the
    common first-failure-of-the-day case, NOT an error."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        if isinstance(data, dict) and isinstance(data.get("events"), list):
            return data
    except Exception as exc:  # noqa: BLE001 — absence is expected; bad blob is recoverable
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read existing watch-log %s: %s", key, exc)
    return {"schema_version": SCHEMA_VERSION, "events": []}


def _build_event_record(detail: dict, describe_resp: dict | None, run_date: str, cfg: dict) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    cause = _failure_cause(describe_resp)
    failed_state = _failed_state_from_history(detail.get("executionArn", ""))
    # config#1535: "will an agent actually be dispatched" depends on BOTH the
    # global kill-switch AND this specific pipeline having a wired listener —
    # not the global flag alone (that was the bug: claiming "dispatch" for a
    # pipeline no workflow listens for).
    # config#1827: a deliberate operator abort is recorded loudly but never
    # auto-dispatches a recovery agent (would waste a cycle and, once weekday/EOD
    # leave propose-only, risk an automated countermand of a human decision).
    operator_abort = _is_operator_abort(detail.get("status", ""), describe_resp)
    # is_preflight: the Friday-PM dry pass of ne-weekly-freshness-pipeline
    # (shell_run=true) is a deliberate rehearsal of Saturday's real run, not a
    # production failure — a preflight FAILED/TIMED_OUT/ABORTED must never be
    # indistinguishable from a genuine Saturday failure and summon the full
    # diagnose-fix-merge-rerun agent against production. Prior to this fix
    # is_preflight only gated the deterministic fast-path rerun (_maybe_fast_path
    # reason="preflight"); the agent-dispatch path had no such gate at all, so a
    # failed Friday shell-run WOULD have fired a genuine saturday-sf-failure
    # dispatch (found 2026-07-10, before ever firing live).
    is_preflight = _is_preflight(describe_resp)
    if operator_abort:
        dispatch_suppressed = "operator_abort"
    elif is_preflight:
        dispatch_suppressed = "preflight"
    else:
        dispatch_suppressed = None
    will_dispatch = (
        AGENT_DISPATCH_ENABLED
        and bool(cfg.get("has_listener", True))
        and not operator_abort
        and not is_preflight
    )
    return {
        "detected_at": now_iso,
        "status": detail.get("status", "UNKNOWN"),
        "state_machine": (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1],
        "execution_name": detail.get("name", ""),
        "execution_arn": detail.get("executionArn", ""),
        "failed_state": failed_state,
        "cause": cause or None,
        "is_preflight": is_preflight,
        # `lane` is filled by the dispatched agent (null until it classifies).
        # `action` reflects intent at write time (the log is written just BEFORE
        # the dispatch fires): "dispatch" only when an agent will genuinely be
        # triggered; "observe" when the kill-switch is off OR this pipeline has
        # no wired listener yet.
        "lane": None,
        "action": "dispatch" if will_dispatch else "observe",
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
        "has_listener": bool(cfg.get("has_listener", True)),
        # config#1827/preflight: null unless the dispatch was withheld for a
        # recorded reason; "operator_abort"/"preflight" make the withholding
        # auditable in the watch-log and on the dashboard.
        "dispatch_suppressed": dispatch_suppressed,
    }


def _write_watch_log(
    s3, watch_prefix: str, run_date: str, record: dict, doc: dict | None = None
) -> str:
    """Append the event to the date's watch-log and write it back. PRIMARY
    deliverable — RAISES on failure (fail-loud: a broken producer must surface
    via the Lambda error metric + CW alarm, never silently). ``doc`` lets the
    handler pass the already-loaded document (the fast path reads prior events
    from it first) so load-append-write stays a single read."""
    key = _artifact_key(watch_prefix, run_date)
    if doc is None:
        doc = _load_existing(s3, key)
    doc["schema_version"] = SCHEMA_VERSION
    doc["run_date"] = run_date
    doc["updated_at"] = record["detected_at"]
    doc["events"].append(record)
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=key,
        Body=json.dumps(doc, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _pipeline_label(pipeline_name: str) -> str:
    """SF name → human cadence label for the Telegram receipt (from PIPELINES)."""
    cfg = PIPELINES.get(pipeline_name)
    if cfg:
        return cfg["label"]
    # Fallback for an unregistered name: strip the ne-/-pipeline affixes.
    return pipeline_name.removeprefix("ne-").removesuffix("-pipeline") or pipeline_name


def _watch_is_acting(record: dict, dispatch: dict) -> bool:
    """True only when this invocation actually started recovery work.

    Observe-only paths (kill-switch off, no listener, operator abort, fast-path
    miss with dispatch disabled, etc.) still land in the watch-log but must NOT
    ping Telegram — sf-telegram-notifier already alerted on the failure, and a
    Fleet-SF Watch receipt with no action is noise (especially for pipelines
    removed from the agent surface, e.g. groom post config#1795)."""
    if record.get("action") == "fast_path_rerun":
        return True
    return dispatch.get("dispatched") is True


def _notify(record: dict, key: str, pipeline_name: str, dispatch: dict) -> bool:
    """Distinct, SILENT Telegram receipt — ONLY when ``_watch_is_acting``.

    The sf-telegram-notifier already pinged loud on this FAILED event; this
    receipt names what the watch is doing (fast-path rerun or agent dispatch).
    Best-effort — never raises. Returns False (no send) on observe-only paths."""
    if not _watch_is_acting(record, dispatch):
        return False
    cfg = PIPELINES.get(pipeline_name) or {}
    has_listener = bool(cfg.get("has_listener", True))
    cadence = _pipeline_label(pipeline_name)
    label = f"{cadence} Preflight SF" if record["is_preflight"] else f"{cadence} SF"
    # config#1827: an operator-abort suppresses the dispatch even when the flag +
    # listener are on — the receipt must read OBSERVE, not AUTO-FIX.
    suppressed = record.get("dispatch_suppressed")
    fast_path = record.get("action") == "fast_path_rerun"
    will_dispatch = (
        AGENT_DISPATCH_ENABLED and has_listener and not suppressed and not fast_path
    )
    mode = "AUTO-RERUN" if fast_path else ("AUTO-FIX" if will_dispatch else "OBSERVE")
    lines = [
        f"\U0001f6f0️ *Fleet-SF Watch — {mode}*",
        f"{label}: {record['status']}",
    ]
    if record.get("failed_state"):
        lines.append(f"Failed state: `{record['failed_state']}`")
    if record.get("cause"):
        lines.append(f"Cause: `{record['cause']}`")
    lines.append(f"Watch log: `s3://{WATCH_BUCKET}/{key}`")
    if fast_path:
        lines.append(f"Rerun: `{record.get('rerun_execution_arn', '')}`")
        footer = (
            f"_fast path: known-transient signature `{record.get('fast_path_signature')}` — "
            "plain rerun started, no agent (zero-token recovery, config#1900)_"
        )
    elif will_dispatch:
        footer = "_autonomous fix ACTIVE — resilience agent dispatched (diagnose→fix→merge→rerun)_"
    elif suppressed == "operator_abort":
        footer = "_operator abort — recorded loudly, no autonomous recovery (deliberate human stop)_"
    elif AGENT_DISPATCH_ENABLED and not has_listener:
        footer = "_observe-only for this pipeline — no autonomous remediation wired yet (needs Brian)_"
    else:
        footer = "_autonomous fix DISABLED (observe-only)_"
    lines.append(footer)
    text = "\n".join(lines)
    dedup_key = f"{_FLOW_NAME}:{pipeline_name}:{record.get('execution_arn', key)}"
    try:
        return notify_via_flow_doctor(
            text,
            silent=True,
            severity="info",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
            db_basename=_DB_BASENAME,
            context={
                "pipeline": pipeline_name,
                "status": record.get("status"),
                "failed_state": record.get("failed_state"),
            },
            silent_topic=FleetTelegramTopic.OPS_HEALTH,
        )
    except Exception as exc:  # noqa: BLE001 — secondary observability
        logger.warning("watch Telegram record failed (non-fatal): %s", exc)
        return False


def _get_github_pat() -> str:
    """Read the dedicated fine-grained PAT (SecureString) from SSM. Never logged."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GITHUB_PAT_SSM_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _maybe_dispatch_agent(
    record: dict, run_date: str, key: str, cfg: dict, pipeline_name: str, sm_arn: str
) -> dict:
    """When AGENT_DISPATCH_ENABLED, fire a repository_dispatch that triggers the
    autonomous resilience-agent GHA workflow in DISPATCH_REPO. The event type +
    pipeline context come from the per-pipeline ``cfg`` so the single workflow
    can route + the charter knows which SF to diagnose/rerun.

    Best-effort with a recording surface (CLAUDE.md no-silent-fails secondary
    carve-out): a GitHub/SSM outage logs WARN and is returned in the result, but
    does NOT raise — the primary observe deliverable (watch-log) already landed.
    The watch-log is written BEFORE this call so the agent reads fresh context.
    """
    if record.get("dispatch_suppressed"):
        # config#1827: a deliberate operator abort (or any other recorded
        # suppression reason) never auto-summons a recovery agent. The watch-log
        # + Telegram receipt already fired, so nothing is silenced — only the
        # autonomous ACTION on a human decision is withheld.
        return {"dispatched": False, "reason": record["dispatch_suppressed"]}
    if not AGENT_DISPATCH_ENABLED:
        return {"dispatched": False, "reason": "disabled"}
    if not cfg.get("has_listener", True):
        # config#1535: don't fire a repository_dispatch that no workflow is
        # listening for — a wasted HTTP call, and inconsistent with the
        # notification copy correctly saying "no autonomous fix" for this
        # pipeline (see _notify).
        return {"dispatched": False, "reason": "no_listener"}
    event_type = cfg["dispatch_event_type"]
    try:
        pat = _get_github_pat()
        payload = {
            "event_type": event_type,
            "client_payload": {
                "pipeline_name": pipeline_name,
                "cadence_slug": cfg["cadence_slug"],
                "state_machine_arn": sm_arn,
                "execution_arn": record.get("execution_arn", ""),
                "failed_state": record.get("failed_state"),
                "cause": record.get("cause"),
                "run_date": run_date,
                "status": record.get("status"),
                "watch_log_key": key,
                "is_preflight": record.get("is_preflight", False),
            },
        }
        req = urllib.request.Request(
            f"https://api.github.com/repos/{DISPATCH_REPO}/dispatches",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "sf-watch-dispatcher",
            },
        )
        with urllib.request.urlopen(req, timeout=_DISPATCH_TIMEOUT_SEC) as resp:
            status_code = resp.status
        logger.info(
            "agent repository_dispatch sent to %s (type=%s, http=%s)",
            DISPATCH_REPO, event_type, status_code,
        )
        return {"dispatched": True, "status_code": status_code, "event_type": event_type}
    except Exception as exc:  # noqa: BLE001 — secondary path, recorded not raised
        logger.warning("agent repository_dispatch failed (non-fatal): %s", exc)
        return {"dispatched": False, "error": f"{type(exc).__name__}: {exc}"}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — fires only on a registered fleet SF terminal failure
    (FAILED / TIMED_OUT / ABORTED), per the dedicated rule
    ``alpha-engine-sf-watch-failed`` (scoped to the three SFs in ``PIPELINES``).
    """
    detail = event.get("detail") or {}
    sm_arn = detail.get("stateMachineArn") or ""
    sm_name = sm_arn.rsplit(":", 1)[-1]
    status = detail.get("status", "UNKNOWN")

    # Defensive: the rule scopes to the registered SFs, but never act on anything else.
    cfg = PIPELINES.get(sm_name)
    if cfg is None:
        logger.warning("ignoring unregistered SF event: %s", sm_name)
        return {"ignored": True, "state_machine": sm_name, "status": status}
    logger.info("Fleet-SF Watch: sf=%s status=%s", sm_name, status)

    describe_resp = _describe_execution(detail.get("executionArn", ""))
    run_date = _run_date(describe_resp, detail)
    record = _build_event_record(detail, describe_resp, run_date, cfg)

    s3 = _s3_client()
    key = _artifact_key(cfg["watch_prefix"], run_date)
    doc = _load_existing(s3, key)
    # config#1900: the deterministic fast path runs BEFORE the write so the
    # watch-log event carries the full outcome (action/signature/rerun arn) in
    # one record. It mutates `record` in place on success; every non-fire is a
    # recorded reason and the normal agent dispatch below takes over.
    fast_path = _maybe_fast_path(record, doc.get("events", []), cfg, sm_arn, describe_resp, run_date)

    _write_watch_log(s3, cfg["watch_prefix"], run_date, record, doc=doc)  # PRIMARY — fail-loud
    # M2: fire the agent AFTER the watch-log lands (agent reads fresh context).
    # A successful fast-path rerun REPLACES the agent dispatch for this event.
    if fast_path.get("fast_path"):
        dispatch = {"dispatched": False, "reason": "fast_path_rerun"}
    else:
        dispatch = _maybe_dispatch_agent(record, run_date, key, cfg, sm_name, sm_arn)  # secondary
    # Telegram only when recovery work actually started (not observe-only).
    telegram_sent = _notify(record, key, sm_name, dispatch)               # secondary — best-effort

    logger.info(
        "Fleet-SF Watch recorded: sf=%s run_date=%s failed_state=%s key=%s telegram=%s fast_path=%s dispatched=%s",
        sm_name, run_date, record.get("failed_state"), key, telegram_sent,
        fast_path.get("fast_path"), dispatch.get("dispatched"),
    )
    return {
        "status": status,
        "state_machine": sm_name,
        "run_date": run_date,
        "failed_state": record.get("failed_state"),
        "watch_log_key": key,
        "telegram_sent": telegram_sent,
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
        "fast_path_enabled": FAST_PATH_ENABLED,
        "fast_path": fast_path,
        "agent_dispatch": dispatch,
        # "observe" until the agent enriches the event with its lane/action;
        # when dispatch fires the agent owns the downstream action record.
        "action": record["action"] if fast_path.get("fast_path")
        else ("dispatched" if dispatch.get("dispatched") else "observe"),
    }
