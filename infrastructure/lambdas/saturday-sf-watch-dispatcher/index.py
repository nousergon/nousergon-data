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
hangs off), the **watch-log artifact** the dashboard page reads, and a
**distinct, SILENT** Telegram record (the notifier already buzzed loud).

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

from alpha_engine_lib.telegram import send_message

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
# M2 gate — default OFF. When true, a failure also fires a repository_dispatch
# that triggers the autonomous resilience agent (which diagnoses → fixes →
# merges → reruns from the failed step). The watch-log is written FIRST (so the
# agent reads fresh context), THEN the dispatch fires. The per-pipeline
# autonomous-merge kill-switch is enforced agent-side (charter STEP 0), so this
# single env gate can stay on fleet-wide while weekday/EOD soak in PROPOSE-ONLY.
AGENT_DISPATCH_ENABLED = (
    os.environ.get("AGENT_DISPATCH_ENABLED", "false").lower() == "true"
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
    # Backlog groom dispatch (config#1472) — wraps the EC2-spot-via-SSM groom
    # dispatch in a Step Function purely for uniform observability (watch-log
    # artifact + silent Telegram record), replacing the bespoke external
    # liveness-probe Lambda (data#556). `has_listener=False` (config#1535):
    # `.github/workflows/sf-watch.yml`'s `types:` allowlist does NOT include
    # `dispatch_event_type` below, so a groom failure gets full watch-log +
    # Telegram coverage, but NO repository_dispatch fires and the notification
    # copy says so honestly — it must NOT claim "autonomous fix ACTIVE" when no
    # workflow is listening (config#1535, the exact bug this field fixes).
    # Groom failures are almost always operational (spot capacity, SSM
    # misfire) rather than a code defect, so wiring this into the SAME
    # code-fix-via-PR resilience-agent charter the trading pipelines use needs
    # its own deliberate autonomy-posture decision first (mirrors the still-open
    # weekday/EOD soak-exit decision in config#1408) — tracked separately, not
    # bundled into this SF-wrap. Flip to True ONLY once `groom-sf-failure` is
    # actually added to sf-watch.yml's `types:` list.
    "alpha-engine-groom-dispatch": {
        "cadence_slug": "groom",
        "label": "Backlog Groom",
        "watch_prefix": "consolidated/groom_sf_watch",
        "dispatch_event_type": "groom-sf-failure",
        "has_listener": False,
    },
}
SCHEMA_VERSION = 1
_CAUSE_MAX_CHARS = 600
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
    will_dispatch = AGENT_DISPATCH_ENABLED and bool(cfg.get("has_listener", True))
    return {
        "detected_at": now_iso,
        "status": detail.get("status", "UNKNOWN"),
        "state_machine": (detail.get("stateMachineArn") or "").rsplit(":", 1)[-1],
        "execution_name": detail.get("name", ""),
        "execution_arn": detail.get("executionArn", ""),
        "failed_state": failed_state,
        "cause": cause or None,
        "is_preflight": _is_preflight(describe_resp),
        # `lane` is filled by the dispatched agent (null until it classifies).
        # `action` reflects intent at write time (the log is written just BEFORE
        # the dispatch fires): "dispatch" only when an agent will genuinely be
        # triggered; "observe" when the kill-switch is off OR this pipeline has
        # no wired listener yet.
        "lane": None,
        "action": "dispatch" if will_dispatch else "observe",
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
        "has_listener": bool(cfg.get("has_listener", True)),
    }


def _write_watch_log(s3, watch_prefix: str, run_date: str, record: dict) -> str:
    """Append the event to the date's watch-log and write it back. PRIMARY
    deliverable — RAISES on failure (fail-loud: a broken producer must surface
    via the Lambda error metric + CW alarm, never silently)."""
    key = _artifact_key(watch_prefix, run_date)
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


def _notify(record: dict, key: str, pipeline_name: str) -> bool:
    """Distinct, SILENT Telegram record. The sf-telegram-notifier already pinged
    loud on this FAILED event; this is the additive watch receipt (which state,
    where the artifact is, current dispatch mode). Best-effort — never raises.
    The header + footer reflect the LIVE ``AGENT_DISPATCH_ENABLED`` state AND
    whether this specific pipeline has a wired listener (config#1535 — the
    receipt must never claim "autonomous fix ACTIVE" when no workflow is
    actually listening for this pipeline's dispatch_event_type, same spirit as
    the 2026-06-26 stale-text bug this pattern originally fixed)."""
    cfg = PIPELINES.get(pipeline_name) or {}
    has_listener = bool(cfg.get("has_listener", True))
    cadence = _pipeline_label(pipeline_name)
    label = f"{cadence} Preflight SF" if record["is_preflight"] else f"{cadence} SF"
    will_dispatch = AGENT_DISPATCH_ENABLED and has_listener
    mode = "AUTO-FIX" if will_dispatch else "OBSERVE"
    lines = [
        f"\U0001f6f0️ *Fleet-SF Watch — {mode}*",
        f"{label}: {record['status']}",
    ]
    if record.get("failed_state"):
        lines.append(f"Failed state: {record['failed_state']}")
    if record.get("cause"):
        lines.append(f"Cause: {record['cause']}")
    lines.append(f"Watch log: s3://{WATCH_BUCKET}/{key}")
    if will_dispatch:
        footer = "_autonomous fix ACTIVE — resilience agent dispatched (diagnose→fix→merge→rerun)_"
    elif AGENT_DISPATCH_ENABLED and not has_listener:
        footer = "_observe-only for this pipeline — no autonomous remediation wired yet (needs Brian)_"
    else:
        footer = "_autonomous fix DISABLED (observe-only)_"
    lines.append(footer)
    try:
        return bool(send_message("\n".join(lines), disable_notification=True))
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
    key = _write_watch_log(s3, cfg["watch_prefix"], run_date, record)  # PRIMARY — fail-loud
    telegram_sent = _notify(record, key, sm_name)                      # secondary — best-effort
    # M2: fire the agent AFTER the watch-log lands (agent reads fresh context).
    dispatch = _maybe_dispatch_agent(record, run_date, key, cfg, sm_name, sm_arn)  # secondary

    logger.info(
        "Fleet-SF Watch recorded: sf=%s run_date=%s failed_state=%s key=%s telegram=%s dispatched=%s",
        sm_name, run_date, record.get("failed_state"), key, telegram_sent, dispatch.get("dispatched"),
    )
    return {
        "status": status,
        "state_machine": sm_name,
        "run_date": run_date,
        "failed_state": record.get("failed_state"),
        "watch_log_key": key,
        "telegram_sent": telegram_sent,
        "agent_dispatch_enabled": AGENT_DISPATCH_ENABLED,
        "agent_dispatch": dispatch,
        # "observe" until the agent enriches the event with its lane/action;
        # when dispatch fires the agent owns the downstream action record.
        "action": "dispatched" if dispatch.get("dispatched") else "observe",
    }
