"""alpha-engine-overseer-liveness-probe — registry-driven wiring + run-window
liveness check for the whole fleet watch plane (alpha-engine-config-I2831).

Consolidates the two per-probe enumerations — sf-watch-liveness-probe's config
-drift WIRING checks and groom-liveness-probe's RUN-WINDOW accounting — into ONE
probe that iterates ``infrastructure/overseer/playbooks.yaml``. Each playbook
declares an OPTIONAL ``liveness.checks`` list; a top-level
``watch_plane_liveness.checks`` list covers the cross-cutting intake/dispatcher
plane. Adding a playbook (or a check to one) automatically extends coverage —
the surface is no longer enumerated in per-probe Python constants.

**Read-only.** The sf-watch reclaim-checker (config#2270) and disabled-window
sweep (config#2257) are ACTION paths with their own EC2-event trigger topology
and 45 pinned tests; they STAY in the (now slimmed) sf-watch-liveness-probe. A
follow-up tracks their eventual migration. This probe never mutates fleet state
— it checks wiring + run windows, dedups by problem-set CONTENT, and alerts.

Check types (discriminated union on ``type`` — contract in playbooks.schema.json):
  * ``eventbridge_rule``      — rule exists / ENABLED / target (lambda or queue)
                                / registered stateMachineArn list (sf-watch).
  * ``state_machines_exist``  — each named Step Function actually exists
                                (the 2026-06-29 dead-ARN class).
  * ``lambda_active``         — function Active + LastUpdateStatus Successful;
                                optional kill-switch REPORT (never alerted) +
                                optional launch-config (AMI/SG/subnet) existence.
  * ``run_window``            — per mature expected trigger (fixed-cron UNION the
                                dispatcher decision log), an S3 run artifact's
                                run_start landed in [T, T+ceiling+margin].
  * ``sqs_queue_exists``      — queue (and optional DLQ) exists.
  * ``scheduler_schedule_exists`` — EventBridge Scheduler schedule (a distinct
                                resource type from ``eventbridge_rule``) exists
                                and is ENABLED (alpha-engine-config-I2906).
  * ``sf_watch_invocation_success`` — per mature real terminal-failure
                                execution of a watched pipeline (read from the
                                SFs' own execution history), the day's
                                watch-log doc has a matching event — catches a
                                dispatcher that is wired correctly but crashes
                                on invocation (alpha-engine-config-I2901).

Conventions preserved from both source probes:
  * silent-unless-broken: a clean pass logs + returns, no Telegram noise.
  * content-hash dedup: ONE sha256 fingerprint over the aggregated problem set
    (subsumes groom's per-trigger dedup) — a standing problem doesn't re-ping,
    and the alert state clears automatically the moment everything is clean.
  * fail-loud (CLAUDE.md no-silent-fails): every AWS describe/list is a PRIMARY
    input — an UNEXPECTED API error RAISES so a broken probe surfaces via the
    Lambda Errors metric, alarmed by the watch-plane backstop alarms in
    infrastructure/setup_watch_plane_alarms.sh. Only the specific "does not
    exist" codes each check explicitly looks for are FINDINGS, not raises.
  * kill-switch REPORTED, never alerted: a deliberate operator disable is state.
  * Telegram send + dedup-state write are best-effort (logged, never raise).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import yaml

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
_FLOW_NAME = "overseer-liveness-probe"
_DB_BASENAME = "flow_doctor_overseer_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
STATE_KEY = os.environ.get(
    "OVERSEER_LIVENESS_STATE_KEY", "consolidated/overseer_liveness/alerted.json"
)

# The playbook registry — bundled into the zip at deploy from the repo SSoT
# (infrastructure/overseer/playbooks.yaml), same pattern as overseer-dispatcher.
REGISTRY_PATH = Path(os.environ.get(
    "OVERSEER_REGISTRY_PATH", str(Path(__file__).parent / "playbooks.yaml")
))


class _RegistryError(RuntimeError):
    """The bundled registry is missing/malformed, or declares an unknown
    liveness check type — a packaging/config bug. Raised so it surfaces via the
    Lambda Errors metric (fail-loud), never a silent no-op."""


_REGISTRY_CACHE: dict | None = None


def _registry() -> dict:
    """Load (once per container) the bundled playbook registry — fail-loud on a
    missing/malformed file (mirrors overseer-dispatcher._registry)."""
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is None:
        try:
            doc = yaml.safe_load(REGISTRY_PATH.read_text())
        except Exception as exc:  # noqa: BLE001 — converted to _RegistryError (fail-loud)
            raise _RegistryError(f"cannot read registry {REGISTRY_PATH}: {exc}") from exc
        if not isinstance(doc, dict) or "playbooks" not in doc:
            raise _RegistryError(f"malformed registry {REGISTRY_PATH}: no 'playbooks' key")
        _REGISTRY_CACHE = doc
    return _REGISTRY_CACHE


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _events_client():
    return boto3.client("events", region_name=REGION)


def _sfn_client():
    return boto3.client("stepfunctions", region_name=REGION)


def _lambda_client():
    return boto3.client("lambda", region_name=REGION)


def _s3_client():
    return boto3.client("s3", region_name=REGION)


def _ec2_client():
    return boto3.client("ec2", region_name=REGION)


def _sqs_client():
    return boto3.client("sqs", region_name=REGION)


def _scheduler_client():
    return boto3.client("scheduler", region_name=REGION)


def _on_bus(bus: str | None) -> str:
    return f" on bus '{bus}'" if bus else ""


# ── Check: eventbridge_rule ──────────────────────────────────────────────────


def _check_eventbridge_rule(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """Rule existence/state/target (+ optional stateMachineArn registration).
    Generalizes sf-watch-liveness-probe._check_rule: the target may be a Lambda
    (``expect_target_function``) or an SQS queue (``expect_target_queue``, the
    intake rules), and the rule may live on a custom bus (``event_bus_name``).
    Fail-loud on any error code OTHER than the "does not exist" one checked for."""
    rule_name = spec["rule_name"]
    bus = spec.get("event_bus_name")
    problems: list[str] = []
    events = _events_client()
    describe_kwargs = {"Name": rule_name}
    if bus:
        describe_kwargs["EventBusName"] = bus
    try:
        rule = events.describe_rule(**describe_kwargs)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return [f"EventBridge rule '{rule_name}'{_on_bus(bus)} does NOT EXIST"], {}
        raise

    if spec.get("expect_enabled", True) and rule.get("State") != "ENABLED":
        problems.append(
            f"EventBridge rule '{rule_name}'{_on_bus(bus)} is {rule.get('State')}, not ENABLED"
        )

    expected_arn = None
    if spec.get("expect_target_function"):
        expected_arn = (
            f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{spec['expect_target_function']}"
        )
    elif spec.get("expect_target_queue"):
        expected_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{spec['expect_target_queue']}"
    if expected_arn is not None:
        list_kwargs = {"Rule": rule_name}
        if bus:
            list_kwargs["EventBusName"] = bus
        targets = events.list_targets_by_rule(**list_kwargs).get("Targets", [])
        target_arns = {t.get("Arn", "") for t in targets}
        if expected_arn not in target_arns:
            problems.append(
                f"rule '{rule_name}'{_on_bus(bus)} does not target {expected_arn} "
                f"(targets: {sorted(target_arns) or 'NONE'})"
            )

    expected_sms = spec.get("expect_state_machines")
    if expected_sms:
        pattern = json.loads(rule.get("EventPattern", "{}"))
        registered = set(pattern.get("detail", {}).get("stateMachineArn", []))
        registered_names = {arn.rsplit(":", 1)[-1] for arn in registered}
        expected_names = set(expected_sms)
        missing = expected_names - registered_names
        extra = registered_names - expected_names
        if missing:
            problems.append(f"rule '{rule_name}' is MISSING expected pipeline(s): {sorted(missing)}")
        if extra:
            problems.append(
                f"rule '{rule_name}' has UNEXPECTED extra pipeline(s) not in the registry: "
                f"{sorted(extra)}"
            )
    return problems, {}


# ── Check: state_machines_exist ──────────────────────────────────────────────


def _check_state_machines_exist(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """Each named pipeline's Step Function must actually exist — the exact
    2026-06-29 dead-ARN bug class, caught directly (ported from
    sf-watch-liveness-probe._check_state_machines_exist)."""
    problems: list[str] = []
    sfn = _sfn_client()
    for name in spec["state_machines"]:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
        try:
            sfn.describe_state_machine(stateMachineArn=arn)
        except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
            if _error_code(exc) == "StateMachineDoesNotExist":
                problems.append(f"registered pipeline '{name}' has NO live Step Function (dead ARN)")
            else:
                raise
    return problems, {}


# ── Check: lambda_active (+ optional kill-switch report + launch config) ──────


def _check_launch_config(fn_name: str, lc: dict, env: dict[str, str]) -> list[str]:
    """The deregistered-AMI silent-break guard (ported from
    sf-watch-liveness-probe._check_launch_config): assert the AMI/SG/subnets the
    DEPLOYED Lambda would launch with still exist, reading their ids from its
    LIVE env (no duplicated constants). Uses Filters (not Ids) so a missing
    resource is an EMPTY set, not an error code — unexpected API errors RAISE."""
    problems: list[str] = []
    ami_key, sg_key, subnets_key = lc["ami_env"], lc["security_group_env"], lc["subnets_env"]

    missing_keys = sorted(k for k in (ami_key, sg_key, subnets_key) if not (env.get(k) or "").strip())
    if missing_keys:
        # Fail-loud on env absence: an unreadable launch config is itself the
        # finding (the dispatcher's deploy.sh pins these keys). STOP rather than
        # probe EC2 with unknown ids — the problem line is the recording surface.
        problems.append(
            f"'{fn_name}' live env is MISSING launch-config key(s) {missing_keys} — "
            "AMI/SG/subnet existence is UNVERIFIABLE (its deploy.sh pins these; redeploy it)"
        )
        return problems

    ami = env[ami_key].strip()
    sg = env[sg_key].strip()
    subnets = sorted({s.strip() for s in env[subnets_key].split(",") if s.strip()})

    ec2 = _ec2_client()

    # IncludeDeprecated: an old-but-registered AMI must NOT false-alarm — only a
    # deregistered/deleted one (which every future launch would fail on) is a finding.
    images = ec2.describe_images(
        Filters=[{"Name": "image-id", "Values": [ami]}], IncludeDeprecated=True
    ).get("Images", [])
    if not images:
        problems.append(
            f"'{fn_name}' launch AMI '{ami}' NOT FOUND (deregistered/deleted) — "
            "every future spot launch would fail"
        )
    elif images[0].get("State") != "available":
        problems.append(f"'{fn_name}' launch AMI '{ami}' state={images[0].get('State')}, not available")

    groups = ec2.describe_security_groups(
        Filters=[{"Name": "group-id", "Values": [sg]}]
    ).get("SecurityGroups", [])
    if not groups:
        problems.append(f"'{fn_name}' launch security group '{sg}' NOT FOUND")

    found_subnets = {
        s.get("SubnetId")
        for s in ec2.describe_subnets(
            Filters=[{"Name": "subnet-id", "Values": subnets}]
        ).get("Subnets", [])
    }
    missing_subnets = sorted(set(subnets) - found_subnets)
    if missing_subnets:
        problems.append(f"'{fn_name}' launch subnet(s) NOT FOUND: {missing_subnets}")

    return problems


def _check_lambda_active(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """Function Active + LastUpdateStatus Successful. Optionally REPORTS a
    kill-switch env value (never alerted — a deliberate operator disable is
    state) and verifies launch-config resources. Ported from
    sf-watch-liveness-probe._check_lambda_healthy + _check_spot_dispatch_leg."""
    fn_name = spec["function"]
    switch_key = spec.get("report_kill_switch")
    problems: list[str] = []
    kill_switches: dict[str, str] = {}
    lam = _lambda_client()
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            if switch_key:
                kill_switches[switch_key] = "UNREADABLE(function missing)"
            return [f"Lambda '{fn_name}' does NOT EXIST"], kill_switches
        raise
    if cfg.get("State") != "Active":
        problems.append(f"Lambda '{fn_name}' state={cfg.get('State')}, not Active")
    if cfg.get("LastUpdateStatus") != "Successful":
        problems.append(f"Lambda '{fn_name}' LastUpdateStatus={cfg.get('LastUpdateStatus')}")
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    if switch_key:
        # REPORTED, never alerted: absence of the key means the in-code default ("true").
        kill_switches[switch_key] = env.get(switch_key, "unset(default:true)")
    lc = spec.get("launch_config")
    if lc:
        problems.extend(_check_launch_config(fn_name, lc, env))
    return problems, kill_switches


# ── Check: sqs_queue_exists ──────────────────────────────────────────────────


def _check_sqs_queue_exists(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """The intake queue (+ optional DLQ) must exist. get_queue_url raises
    ``QueueDoesNotExist`` for a truly-absent queue (a FINDING); any other error
    RAISES (fail-loud — an unreadable queue state must not read as 'present')."""
    problems: list[str] = []
    sqs = _sqs_client()
    for queue_name, kind in _queues_to_check(spec):
        try:
            sqs.get_queue_url(QueueName=queue_name)
        except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
            if _error_code(exc) in {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}:
                problems.append(f"{kind} '{queue_name}' does NOT EXIST")
            else:
                raise
    return problems, {}


def _queues_to_check(spec: dict) -> list[tuple[str, str]]:
    out = [(spec["queue_name"], "intake queue")]
    if spec.get("expect_dlq"):
        out.append((spec["expect_dlq"], "intake DLQ"))
    return out


# ── Check: scheduler_schedule_exists ─────────────────────────────────────────


def _check_scheduler_schedule_exists(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """EventBridge Scheduler schedule exists + (by default) ENABLED. A
    DIFFERENT AWS resource from the classic `events` rules the
    ``eventbridge_rule`` check covers — a deleted/disabled Scheduler schedule
    is otherwise invisible (alpha-engine-config-I2906). Deliberately NAME +
    STATE only, never target ARN: a concurrent migration
    (alpha-engine-config-I2832) re-points some of these schedules' targets
    between executor Lambdas and the overseer-dispatcher router, and this
    check must stay valid across that repoint. GetSchedule raises
    ``ResourceNotFoundException`` for a truly-absent schedule (a FINDING); any
    other error RAISES (fail-loud)."""
    name = spec["schedule_name"]
    problems: list[str] = []
    scheduler = _scheduler_client()
    try:
        sched = scheduler.get_schedule(Name=name)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return [f"EventBridge Scheduler schedule '{name}' does NOT EXIST"], {}
        raise
    if spec.get("expect_enabled", True) and sched.get("State") != "ENABLED":
        problems.append(
            f"EventBridge Scheduler schedule '{name}' is {sched.get('State')}, not ENABLED"
        )
    return problems, {}


# ── Check: run_window (ported from groom-liveness-probe) ──────────────────────
# Config comes from the registry spec (was module constants + GROOM_SCHEDULE
# env). The alerted-set dedup is DROPPED here — the unified probe's single
# content-fingerprint (below) subsumes groom's per-trigger dedup.


def _rw_schedule(spec: dict) -> list[dict]:
    return spec["schedule"]


def _rw_lookback_dates(spec: dict, now: datetime) -> list[str]:
    horizon = now - timedelta(hours=spec["lookback_hours"])
    dates: list[str] = []
    d = horizon.date()
    last = now.date()
    while d <= last:
        dates.append(d.isoformat())
        d += timedelta(days=1)
    return dates


def _rw_expected_triggers(spec: dict, now: datetime) -> list[dict]:
    """Enumerate every FIXED-CRON trigger (registry ``schedule``) in the lookback
    window that is now MATURE (had ceiling+margin minutes to finish). Each →
    {at, label}. Kept as a belt-and-braces cross-check alongside the decision-log
    source so a decision-log read failure degrades to (never below) this coverage."""
    lookback_hours = spec["lookback_hours"]
    mature_min = spec["ceiling_min"] + spec["margin_min"]
    horizon = now - timedelta(hours=lookback_hours)
    mature_before = now - timedelta(minutes=mature_min)
    out: list[dict] = []
    day = (horizon - timedelta(days=1)).date()
    last = now.date()
    while day <= last:
        for entry in _rw_schedule(spec):
            if day.weekday() not in set(entry["dows"]):
                continue
            t = datetime(
                day.year, day.month, day.day,
                int(entry["hour"]), int(entry["minute"]),
                tzinfo=timezone.utc,
            )
            if horizon <= t <= mature_before:
                out.append({"at": t, "label": entry.get("label", f"{entry['hour']:02d}:{entry['minute']:02d}")})
        day += timedelta(days=1)
    out.sort(key=lambda d: d["at"])
    return out


def _rw_decision_launched(record: dict) -> bool:
    """True iff this dispatch-decision record shows AT LEAST ONE launch=true
    decision (handles the top-level ``launched``/``launch`` bool and the
    ``decisions: [...]`` list schema). A skip-only record is NOT expected to
    have a run artifact — ignored, not flagged."""
    if record.get("launched") is True or record.get("launch") is True:
        return True
    decisions = record.get("decisions")
    if isinstance(decisions, list):
        for d in decisions:
            if isinstance(d, dict) and (d.get("launch") is True or d.get("launched") is True):
                return True
    return False


def _rw_expected_triggers_from_decisions(spec: dict, s3, now: datetime) -> list[dict]:
    """Enumerate mature expected triggers from the dispatcher's OWN
    dispatch-decision log (``{decision_record_prefix}{date}/*.json``) — how
    sweep-mode (event-driven, no fixed cron) dispatches become visible.
    Best-effort READ: an individual unreadable record (or an entirely
    unavailable log) is skipped/degraded (logged), NOT raised — the fixed-cron
    cross-check is the redundant fallback (unlike the PRIMARY run-artifact read)."""
    prefix_root = spec.get("decision_record_prefix")
    if not prefix_root:
        return []
    lookback_hours = spec["lookback_hours"]
    mature_min = spec["ceiling_min"] + spec["margin_min"]
    horizon = now - timedelta(hours=lookback_hours)
    mature_before = now - timedelta(minutes=mature_min)
    out: list[dict] = []
    for date in _rw_lookback_dates(spec, now):
        prefix = f"{prefix_root}{date}/"
        token = None
        while True:
            kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            try:
                resp = s3.list_objects_v2(**kwargs)
            except Exception as exc:  # noqa: BLE001 — redundant source; fixed-cron cross-check remains
                logger.warning(
                    "run_window[%s]: decision-record list failed for prefix %s (%s) — "
                    "sweep-mode coverage degraded to fixed-cron this run",
                    spec.get("label"), prefix, exc,
                )
                break
            for obj in resp.get("Contents", []) or []:
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                    record = json.loads(body)
                except Exception as exc:  # noqa: BLE001 — one bad record must not hide the rest
                    logger.warning("run_window[%s]: decision record %s unreadable (%s) — skipped",
                                   spec.get("label"), key, exc)
                    continue
                if not _rw_decision_launched(record):
                    continue
                decided_at = record.get("decided_at")
                if not decided_at:
                    continue
                try:
                    t = datetime.fromisoformat(str(decided_at).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if not (horizon <= t <= mature_before):
                    continue
                label = f"decision-log:{record.get('trigger', record.get('run_mode', 'unknown'))}"
                out.append({"at": t, "label": label})
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    out.sort(key=lambda d: d["at"])
    return out


def _rw_all_expected_triggers(spec: dict, s3, now: datetime) -> list[dict]:
    """Union of the fixed-cron schedule and the real dispatch-decision log,
    de-duplicated by ``at`` timestamp so a full-mode trigger appearing in BOTH
    sources is never double-counted."""
    merged: dict[datetime, dict] = {}
    for trig in _rw_expected_triggers(spec, now):
        merged[trig["at"]] = trig
    for trig in _rw_expected_triggers_from_decisions(spec, s3, now):
        merged.setdefault(trig["at"], trig)
    return sorted(merged.values(), key=lambda d: d["at"])


def _rw_fetch_run_artifact_timestamps(spec: dict, s3, now: datetime) -> list[datetime]:
    """``run_start`` timestamps of recent S3 run artifacts
    (``{artifact_prefix}{date}/{run_id}.json``). PRIMARY input — RAISES on error
    (fail-loud); a malformed individual artifact also raises (skipping it
    silently would let a genuinely-missed trigger hide behind a corrupt one)."""
    artifact_prefix = spec["artifact_prefix"]
    stamps: list[datetime] = []
    for date in _rw_lookback_dates(spec, now):
        prefix = f"{artifact_prefix}{date}/"
        token = None
        while True:
            kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                art = json.loads(body)
                run_start = art.get("run_start")
                if not run_start:
                    continue
                stamps.append(datetime.fromisoformat(run_start.replace("Z", "+00:00")))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    return stamps


def _rw_missed(spec: dict, triggers: list[dict], stamps: list[datetime]) -> list[dict]:
    """A trigger is a MISS iff no run artifact's run_start fell inside its run
    window [T, T + ceiling + margin]."""
    window = timedelta(minutes=spec["ceiling_min"] + spec["margin_min"])
    return [trig for trig in triggers if not any(trig["at"] <= s <= trig["at"] + window for s in stamps)]


def _check_run_window(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """Per-trigger run-window accounting — a mature scheduled run with no
    covering S3 artifact = a silent death (ported from groom-liveness-probe,
    minus its per-trigger dedup which the unified fingerprint subsumes)."""
    s3 = _s3_client()
    label = spec["label"]
    triggers = _rw_all_expected_triggers(spec, s3, now)
    if not triggers:
        logger.info("run_window[%s]: no mature triggers in window", label)
        return [], {}
    stamps = _rw_fetch_run_artifact_timestamps(spec, s3, now)  # PRIMARY — fail-loud
    misses = _rw_missed(spec, triggers, stamps)
    problems = [
        f"scheduled {label} run '{m['label']}' @ {m['at'].strftime('%Y-%m-%d %H:%M')}Z filed NO "
        f"terminal report (no S3 run artifact under '{spec['artifact_prefix']}' in-window) — box "
        "likely died silently (spot reclaim / OOM / pre-trap crash) or was never dispatched"
        for m in misses
    ]
    return problems, {}


# ── Check: sf_watch_invocation_success ───────────────────────────────────────
# The exact "wiring vs function" gap (alpha-engine-config-I2901): every check
# above only asserts the sf-watch dispatcher is deployed + correctly WIRED — a
# dispatcher that is Active, correctly targeted, AND crashes on every real
# invocation (2026-07-17 ListBucket/403 while loading/writing the watch-log)
# is invisible to them. This check is the invocation-SUCCESS signal the issue
# asks for: for each mature terminal-failure execution of a watched pipeline —
# read from the state machines' OWN execution history, an INDEPENDENT signal
# from the watch-log the dispatcher writes, so a broken writer can't hide its
# own breakage — the day's watch-log doc must carry a matching event record.

_SF_FAILURE_STATUSES = ("FAILED", "TIMED_OUT", "ABORTED")


def _list_recent_sf_failures(sfn, state_machine_arn: str, horizon: datetime) -> list[dict]:
    """Every FAILED/TIMED_OUT/ABORTED execution of this state machine with
    stopDate >= horizon. ListExecutions returns executions newest-first within
    each status filter, so paging stops as soon as one is older than horizon.
    PRIMARY input — RAISES on any API error (fail-loud)."""
    out: list[dict] = []
    for status in _SF_FAILURE_STATUSES:
        token = None
        while True:
            kwargs: dict = {
                "stateMachineArn": state_machine_arn,
                "statusFilter": status,
                "maxResults": 100,
            }
            if token:
                kwargs["nextToken"] = token
            resp = sfn.list_executions(**kwargs)
            older_than_horizon = False
            for execu in resp.get("executions", []):
                stop = execu.get("stopDate")
                if stop is not None and stop < horizon:
                    older_than_horizon = True
                    break
                out.append(execu)
            token = resp.get("nextToken")
            if older_than_horizon or not token:
                break
    return out


def _sf_watch_run_date_for_execution(sfn, execu: dict, now: datetime) -> str:
    """Mirror saturday-sf-watch-dispatcher._run_date verbatim: prefer the
    execution input's ``run_date``, else the execution ``startDate``, else
    ``now`` — so this check reads the EXACT S3 key the dispatcher itself would
    have written to. DescribeExecution is a PRIMARY input here (needed for
    correctness, not a convenience) — RAISES on an unexpected error (fail-loud,
    an ExecutionDoesNotExist is treated as "no input to read", not swallowed
    silently past that); only malformed input JSON degrades to the startDate
    fallback, mirroring the producer's own tolerant behavior for that one
    narrow case."""
    resp = None
    try:
        resp = sfn.describe_execution(executionArn=execu["executionArn"])
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) != "ExecutionDoesNotExist":
            raise
    if resp is not None:
        try:
            payload = json.loads(resp.get("input") or "{}")
            rd = payload.get("run_date")
            if isinstance(rd, str) and rd:
                return rd
        except (ValueError, TypeError):
            pass
    start = execu.get("startDate")
    if isinstance(start, datetime):
        return start.date().isoformat()
    return now.date().isoformat()


def _watch_log_events(s3, key: str) -> list[dict] | None:
    """Read + parse the day's watch-log doc, or None if it genuinely does not
    exist yet (the common no-failure-today case). Any OTHER read error — 403
    above all, the exact 2026-07-17 incident class — RAISES (fail-loud): a
    check that treated an AccessDenied as "no events yet" would hide the very
    crash it exists to catch."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        doc = json.loads(obj["Body"].read())
    except (ValueError, TypeError):
        return []
    events = doc.get("events") if isinstance(doc, dict) else None
    return events if isinstance(events, list) else []


def _check_sf_watch_invocation_success(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """Per registered pipeline, every MATURE (older than
    ``response_window_min``) terminal-failure execution within
    ``lookback_hours`` must have produced a matching watch-log event. A miss
    means the dispatcher was invoked (EventBridge fired — wiring is fine) but
    never completed its PRIMARY fail-loud watch-log write, i.e. it crashed on
    invocation."""
    problems: list[str] = []
    sfn = _sfn_client()
    s3 = _s3_client()
    horizon = now - timedelta(hours=spec["lookback_hours"])
    mature_before = now - timedelta(minutes=spec["response_window_min"])
    for entry in spec["pipelines"]:
        sm_name = entry["state_machine"]
        watch_prefix = entry["watch_prefix"]
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{sm_name}"
        failures = _list_recent_sf_failures(sfn, arn, horizon)
        for execu in failures:
            stop = execu.get("stopDate")
            if stop is None or stop > mature_before:
                continue  # not mature yet — give the dispatcher time to write
            run_date = _sf_watch_run_date_for_execution(sfn, execu, now)
            key = f"{watch_prefix}/{run_date}.json"
            events = _watch_log_events(s3, key)
            recorded = events is not None and any(
                e.get("execution_arn") == execu.get("executionArn") for e in events
            )
            if not recorded:
                problems.append(
                    f"sf-watch: {sm_name} execution '{execu.get('name')}' terminal-failed "
                    f"({execu.get('status')}) @ {stop.strftime('%Y-%m-%d %H:%M')}Z with NO "
                    f"matching watch-log event under '{key}' {spec['response_window_min']}+ min "
                    "later — the dispatcher was invoked but crashed before its fail-loud "
                    "watch-log write (wiring OK, function broken)"
                )
    return problems, {}


# ── Check dispatch table + aggregation ───────────────────────────────────────

CHECKERS = {
    "eventbridge_rule": _check_eventbridge_rule,
    "state_machines_exist": _check_state_machines_exist,
    "lambda_active": _check_lambda_active,
    "sqs_queue_exists": _check_sqs_queue_exists,
    "run_window": _check_run_window,
    "scheduler_schedule_exists": _check_scheduler_schedule_exists,
    "sf_watch_invocation_success": _check_sf_watch_invocation_success,
}


def _iter_check_specs(registry: dict) -> list[tuple[str, dict]]:
    """Every liveness check in the registry, as (source_label, spec) — each
    playbook's ``liveness.checks`` (sorted for determinism) then the top-level
    ``watch_plane_liveness.checks``."""
    specs: list[tuple[str, dict]] = []
    for pb_name, pb in sorted((registry.get("playbooks") or {}).items()):
        for spec in ((pb.get("liveness") or {}).get("checks") or []):
            specs.append((f"playbook:{pb_name}", spec))
    for spec in ((registry.get("watch_plane_liveness") or {}).get("checks") or []):
        specs.append(("watch_plane", spec))
    return specs


def _run_checks(now: datetime) -> tuple[list[str], dict[str, str]]:
    """Run every registry-declared liveness check, aggregating problems +
    reported kill-switches. An unknown check type RAISES (_RegistryError,
    fail-loud) — a registry that outran the probe's checker table is a
    packaging bug, not a silent skip."""
    registry = _registry()
    problems: list[str] = []
    kill_switches: dict[str, str] = {}
    for label, spec in _iter_check_specs(registry):
        ctype = spec.get("type")
        checker = CHECKERS.get(ctype)
        if checker is None:
            raise _RegistryError(f"{label}: unknown liveness check type {ctype!r}")
        p, ks = checker(spec, now)
        problems.extend(p)
        kill_switches.update(ks)
    return problems, kill_switches


# ── Dedup + alert (content-fingerprint, ported from sf-watch-liveness-probe) ──


def _problem_fingerprint(problems: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(problems)).encode()).hexdigest()[:16]


def _load_alerted_fingerprint(s3) -> str | None:
    """None means 'no state yet' OR 'currently healthy' — both mean nothing to
    suppress against."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read()).get("fingerprint")
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        if _error_code(exc) not in {"NoSuchKey", "404", "403", ""}:
            logger.warning("could not read overseer liveness state %s: %s", STATE_KEY, exc)
        return None


def _save_alerted_fingerprint(s3, fingerprint: str | None) -> None:
    """Best-effort: a write failure only risks a duplicate/missed-clear ping
    next run (logged), never a missed finding — so it does NOT raise."""
    try:
        s3.put_object(
            Bucket=WATCH_BUCKET,
            Key=STATE_KEY,
            Body=json.dumps(
                {"fingerprint": fingerprint, "updated_at": datetime.now(timezone.utc).isoformat()},
                indent=2,
            ).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — dedup state; failure only risks a dup ping
        logger.warning("could not persist overseer liveness state %s: %s", STATE_KEY, exc)


def _alert(problems: list[str], kill_switches: dict[str, str] | None = None) -> bool:
    lines = [
        "\U0001f6f0️ *Overseer Liveness Probe — WATCH-PLANE PROBLEM*",
        f"{len(problems)} wiring/liveness issue(s) found across the fleet watch plane "
        "(the WATCHERS' own wiring, NOT a pipeline failure):",
    ]
    lines.extend(f"• {p}" for p in problems)
    lines.append(
        "_A watcher may not catch (or repair) a real failure right now. Check the "
        "named rules / Step Functions / dispatcher Lambdas / intake queue._"
    )
    text = "\n".join(lines)
    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=f"{_FLOW_NAME}:wiring:{_problem_fingerprint(problems)}",
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context={"problems": len(problems), "kill_switches": kill_switches or {}},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("overseer liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Iterates the playbook registry,
    runs every declared liveness check read-only, dedups by problem-set content,
    and LOUD-alerts only on a NEW/changed problem set. Raises on an unexpected
    AWS API failure (or an unknown registry check type) so the probe can never
    silently no-op."""
    now = datetime.now(timezone.utc)
    problems, kill_switches = _run_checks(now)
    fingerprint = _problem_fingerprint(problems) if problems else None

    # Always surfaced (record + log), never alerted: a deliberate operator
    # disable is state, not an incident.
    logger.info("overseer liveness: dispatch kill-switches: %s", kill_switches)

    s3 = _s3_client()
    already = _load_alerted_fingerprint(s3)

    alerted = False
    if problems and fingerprint != already:
        logger.warning("overseer liveness: %d NEW problem(s): %s", len(problems), problems)
        alerted = _alert(problems, kill_switches)
        if alerted:
            _save_alerted_fingerprint(s3, fingerprint)
    elif problems:
        logger.info(
            "overseer liveness: %d problem(s), unchanged since last alert — suppressed",
            len(problems),
        )
    else:
        logger.info("overseer liveness: all checks clean")
        if already is not None:
            _save_alerted_fingerprint(s3, None)  # clear dedup state now that it's healthy again

    return {
        "problems": problems,
        "alerted": alerted,
        "clean": not problems,
        "kill_switches": kill_switches,
    }
