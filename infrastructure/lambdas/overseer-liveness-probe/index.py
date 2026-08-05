"""alpha-engine-overseer-liveness-probe — registry-driven wiring + run-window
liveness check for the whole fleet watch plane (alpha-engine-config-I2831).

Consolidates the two per-probe enumerations — sf-watch-reclaim-sweep-handler's config
-drift WIRING checks and groom-liveness-probe's RUN-WINDOW accounting — into ONE
probe that iterates ``infrastructure/overseer/playbooks.yaml``. Each playbook
declares an OPTIONAL ``liveness.checks`` list; a top-level
``watch_plane_liveness.checks`` list covers the cross-cutting intake/dispatcher
plane. Adding a playbook (or a check to one) automatically extends coverage —
the surface is no longer enumerated in per-probe Python constants.

**Read-only.** The sf-watch reclaim-checker (config#2270) and disabled-window
sweep (config#2257) are ACTION paths with their own EC2-event trigger topology
and 45 pinned tests; they STAY in the (now slimmed) sf-watch-reclaim-sweep-handler. A
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
  * ``agent_dispatch_completeness`` — for every dispatch the dispatch ledger
                                records as launched=True, the corresponding SSM
                                command must have reached a terminal Success
                                status; catches a dispatch that reported success
                                but whose agent never completed (the 2026-08-01
                                outage class, alpha-engine-config#6164).

Conventions preserved from both source probes:
  * silent-unless-broken: a clean pass logs + returns, no Telegram noise.
  * per-problem dedup: each problem gets its own signature; only NEW problems
    trigger an alert, continuing ones are summarized as a count, and resolved
    ones age out so a genuine recurrence later pages again (alpha-engine-config-I5207).
    The flow-doctor dedup key still covers the full new+continuing set so
    an entire run with zero new problems is fully suppressed.
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
# Full per-finding prose lands here; the page carries headlines and this key.
# Same prefix as STATE_KEY so the existing LivenessDedupState IAM statement
# already grants it — this deploys on merge with no operator step (the rule
# alpha-engine-config-I4472 exists to enforce).
REPORT_PREFIX = os.environ.get(
    "OVERSEER_LIVENESS_REPORT_PREFIX", "consolidated/overseer_liveness/reports/"
)

# Message shape. A page is a summons, not a report: it must answer "what is
# broken, how bad, where do I look" in one screen. Findings beyond the cap and
# every finding's full prose live in the S3 detail report.
_MAX_HEADLINES = 8       # headline lines rendered before "…and N more"
_HEADLINE_ITEMS = 3      # per-finding items (timestamps, runs) named inline


def _finding(component: str, headline: str, detail: str | None = None) -> dict:
    """One liveness finding, in the two registers a page needs.

    ``headline`` is scannable and one line; ``detail`` is the full prose that
    used to BE the message. Checks that are already one short sentence pass
    only a headline and the two are identical."""
    return {
        "component": component,
        "headline": headline,
        "detail": detail if detail is not None else headline,
    }

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
    Generalizes sf-watch-reclaim-sweep-handler._check_rule: the target may be a Lambda
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
    sf-watch-reclaim-sweep-handler._check_state_machines_exist)."""
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
    sf-watch-reclaim-sweep-handler._check_launch_config): assert the AMI/SG/subnets the
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
    sf-watch-reclaim-sweep-handler._check_lambda_healthy + _check_spot_dispatch_leg."""
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


def _rw_fetch_run_artifacts(spec: dict, s3, now: datetime) -> list[tuple[datetime, str, dict]]:
    """Recent S3 run artifacts (``{artifact_prefix}{date}/{run_id}.json``) as
    (run_start, key, artifact). PRIMARY input — RAISES on error (fail-loud); a
    malformed individual artifact also raises (skipping it silently would let a
    genuinely-missed trigger hide behind a corrupt one).

    The start-timestamp field is REGISTRY-DECLARED (``run_start_field``,
    default ``run_start``) because the fleet's run artifacts do not share one
    schema: the groom artifact carries ``run_start``, the alert-drain ledger
    carries ``started_at``. Until G16 (alpha-engine-config-I5284) collapses
    them into one execution artifact, the reader must be told which field to
    read rather than assuming groom's.

    An artifact WITHOUT the declared field raises — this is the exact defect
    this parameter fixes. From 2026-07-25 the drain leg read `art.get("run_start")`
    on ledgers that never carry it, `continue`d past every one, and reported
    100% of mature alert-drain triggers as silent deaths while the drain was
    running normally: four consecutive false pages on 2026-07-29. A silent skip
    here cannot be told apart from a genuinely missing run, which is the one
    distinction the whole check exists to make."""
    artifact_prefix = spec["artifact_prefix"]
    start_field = spec.get("run_start_field", "run_start")
    found: list[tuple[datetime, str, dict]] = []
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
                run_start = art.get(start_field)
                if not run_start:
                    raise _RegistryError(
                        f"run_window[{spec.get('label')}]: artifact s3://{WATCH_BUCKET}/{key} "
                        f"has no {start_field!r} field (registry run_start_field). Every run "
                        "under this prefix would read as a missed run — fix the registry "
                        "field name or the producer, do not skip the artifact."
                    )
                found.append(
                    (datetime.fromisoformat(run_start.replace("Z", "+00:00")), key, art)
                )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
    return found


def _rw_missed(spec: dict, triggers: list[dict], stamps: list[datetime]) -> list[dict]:
    """A trigger is a MISS iff no run artifact's start timestamp fell inside its
    run window [T, T + ceiling + margin]."""
    window = timedelta(minutes=spec["ceiling_min"] + spec["margin_min"])
    return [trig for trig in triggers if not any(trig["at"] <= s <= trig["at"] + window for s in stamps)]


def _rw_resolve_field(field: str, art: dict):
    """Resolve a dot-separated field path through a nested artifact dict.

    ``"ingested.queue"`` resolves to ``art["ingested"]["queue"]``. A field
    without dots behaves identically to ``art.get(field)``. If the resolved
    value is a list (e.g. ``incidents``), its length is returned so the
    numeric operators work without the caller knowing the schema — a
    ``productive_when`` clause can say ``{field: "incidents", eq: 0}`` and
    it evaluates against the number of incidents, not the list itself.
    """
    value = art
    for part in field.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    if isinstance(value, list):
        return len(value)
    return value


def _rw_clause_holds(clause: dict, art: dict) -> bool:
    """Evaluate ONE declarative ``productive_when`` clause against an artifact.

    Supported: ``{field: <name>, gt|ge|lt|le|eq: <number-or-value>}``. An
    unknown operator RAISES — a registry that outran the evaluator is a
    packaging bug, and silently treating the clause as unsatisfied would make
    every run look dead (or, worse, alive) for the wrong reason.

    Fields support dot-notation for nested dict access (e.g. ``ingested.queue``)
    and list values are auto-resolved to their length.
    """
    value = _rw_resolve_field(clause["field"], art)
    for op, expected in clause.items():
        if op == "field":
            continue
        if op == "eq":
            if value != expected:
                return False
        elif op in ("gt", "ge", "lt", "le"):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                return False
            if not {
                "gt": value > expected,
                "ge": value >= expected,
                "lt": value < expected,
                "le": value <= expected,
            }[op]:
                return False
        else:
            raise _RegistryError(f"run_window: unknown productive_when operator {op!r}")
    return True


def _rw_dead_runs(spec: dict, artifacts: list[tuple[datetime, str, dict]]) -> list[tuple[str, str]]:
    """Run artifacts that prove a box BOOTED but not that the run did anything.

    A pure existence check answers "was an artifact written?", which is not the
    question the probe exists to answer. On 2026-07-28 all three groom lanes
    crash-cascaded two minutes after boot — every chunk agent refused to start
    because a truncated `runuser` left the run as root — and each lane still
    wrote a well-formed artifact (`engaged: 0`, `floor_fail: true`,
    `artifact_ok: true`). 378 issues went un-dispositioned and this check saw
    full coverage, because it only ever read `run_start`.

    ``productive_when`` is an OR of declarative clauses; an artifact matching
    NONE of them is a dead run. Specs without the key keep the old
    existence-only semantics, so this is additive per registry entry.

    Returns (headline_fragment, detail) per dead run — the caller collapses
    them into ONE finding so a bad day reads as one line, not N paragraphs.
    """
    clauses = spec.get("productive_when")
    if not clauses:
        return []
    label = spec["label"]
    out: list[tuple[str, str]] = []
    for run_start, key, art in sorted(artifacts, key=lambda t: t[0]):
        if any(_rw_clause_holds(c, art) for c in clauses):
            continue
        detail = ", ".join(
            f"{f}={art[f]!r}"
            for f in ("engaged", "total_issues", "undispositioned", "floor_fail",
                      "spot_interrupted", "elapsed_min")
            if f in art
        )
        stop = str(art.get("stop_reason") or "").strip()
        stamp = run_start.strftime("%m-%d %H:%M")
        engaged, total = art.get("engaged"), art.get("total_issues")
        short = (
            f"{stamp}Z engaged {engaged} of {total}"
            if engaged is not None and total is not None
            else f"{stamp}Z did no work"
        )
        out.append((
            short,
            f"scheduled {label} run @ {run_start.strftime('%Y-%m-%d %H:%M')}Z RAN BUT "
            f"DID NO WORK — the artifact exists, so run-window coverage looks green, "
            f"but the run engaged nothing it was dispatched to engage ({detail}). "
            f"artifact=s3://{WATCH_BUCKET}/{key}"
            + (f" stop_reason={stop[:200]!r}" if stop else ""),
        ))
    return out


def _check_run_window(spec: dict, now: datetime) -> tuple[list[dict], dict]:
    """Per-trigger run-window accounting — a mature scheduled run with no
    covering S3 artifact = a silent death (ported from groom-liveness-probe,
    minus its per-trigger dedup which the unified fingerprint subsumes).

    Emits at most TWO findings — one per failure mode — each collapsing every
    affected run into a single headline with the per-run prose in ``detail``.
    Per-run lines were the dominant term in an unreadable page: five missed
    drain triggers rendered as five near-identical paragraphs saying one thing."""
    s3 = _s3_client()
    label = spec["label"]
    triggers = _rw_all_expected_triggers(spec, s3, now)
    if not triggers:
        logger.info("run_window[%s]: no mature triggers in window", label)
        return [], {}
    artifacts = _rw_fetch_run_artifacts(spec, s3, now)  # PRIMARY — fail-loud
    misses = _rw_missed(spec, triggers, [a[0] for a in artifacts])
    findings: list[dict] = []
    if misses:
        when = ", ".join(m["at"].strftime("%m-%d %H:%M") + "Z" for m in misses[:_HEADLINE_ITEMS])
        if len(misses) > _HEADLINE_ITEMS:
            when += f", +{len(misses) - _HEADLINE_ITEMS} more"
        findings.append(_finding(
            label,
            f"{len(misses)} of {len(triggers)} scheduled runs filed no report ({when})",
            "\n".join(
                f"scheduled {label} run '{m['label']}' @ {m['at'].strftime('%Y-%m-%d %H:%M')}Z filed NO "
                f"terminal report (no S3 run artifact under '{spec['artifact_prefix']}' in-window) — box "
                "likely died silently (spot reclaim / OOM / pre-trap crash) or was never dispatched"
                for m in misses
            ),
        ))
    # Second failure mode, same check: the artifact EXISTS but the run did
    # nothing. Assessed per-artifact rather than per-trigger, so one dead lane
    # is visible even when its sibling lanes covered the same trigger.
    dead = _rw_dead_runs(spec, artifacts)
    if dead:
        shorts = "; ".join(s for s, _ in dead[:_HEADLINE_ITEMS])
        if len(dead) > _HEADLINE_ITEMS:
            shorts += f"; +{len(dead) - _HEADLINE_ITEMS} more"
        findings.append(_finding(
            label,
            f"{len(dead)} run(s) wrote an artifact but did no work ({shorts})",
            "\n".join(d for _, d in dead),
        ))
    return findings, {}


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


def _check_sf_watch_invocation_success(spec: dict, now: datetime) -> tuple[list[dict], dict]:
    """Per registered pipeline, every MATURE (older than
    ``response_window_min``) terminal-failure execution within
    ``lookback_hours`` must have produced a matching watch-log event. A miss
    means the dispatcher was invoked (EventBridge fired — wiring is fine) but
    never completed its PRIMARY fail-loud watch-log write, i.e. it crashed on
    invocation.

    One finding regardless of how many executions missed: N unrecorded
    dispatches are one broken dispatcher, and rendering them as N paragraphs
    makes the page longer without making it more actionable."""
    problems: list[str] = []
    missed_names: list[str] = []
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
                missed_names.append(sm_name)
                problems.append(
                    f"sf-watch: {sm_name} execution '{execu.get('name')}' terminal-failed "
                    f"({execu.get('status')}) @ {stop.strftime('%Y-%m-%d %H:%M')}Z with NO "
                    f"matching watch-log event under '{key}' {spec['response_window_min']}+ min "
                    "later — the dispatcher was invoked but crashed before its fail-loud "
                    "watch-log write (wiring OK, function broken)"
                )
    if not problems:
        return [], {}
    pipelines = sorted(set(missed_names))
    named = ", ".join(pipelines[:_HEADLINE_ITEMS])
    if len(pipelines) > _HEADLINE_ITEMS:
        named += f", +{len(pipelines) - _HEADLINE_ITEMS} more"
    return [_finding(
        "sf-watch",
        f"{len(problems)} pipeline failure(s) got no watch-log event — dispatcher "
        f"invoked but crashing ({named})",
        "\n".join(problems),
    )], {}


# ── Check: dispatch_invocation_success ────────────────────────────────────────
# Generalized from the sf-watch-only check above — covers all four dispatch
# families (alpha-engine-config-I5262). The sf-watch check had an independent
# signal (the pipeline SF's own execution history). The other three families
# (groom, ci-watch, alert-drain) use the dispatcher's own decision log as the
# "expected dispatch" source — a dispatcher that writes a launch record but
# whose box dies before the completion marker lands IS a real gap (exactly
# what happened 2026-07-28 when all 3 groom lanes were reclaimed).
#
# Each family declares:
#   dispatch_prefix  — S3 prefix for records of "a box was launched"
#   completion_prefix — S3 prefix for "the box finished + wrote its outcome"
#   completion_key_field — field within the dispatch record that names the
#     completion marker key (e.g. "run_token", "completion_key")
#   response_window_min — how long after dispatch to wait before a missing
#     completion marker is a finding
#   lookback_hours — how far back to look for dispatched-but-unfinished boxes


def _family_dispatch_records(s3, spec: dict, now: datetime) -> list[dict]:
    """Enumerate dispatch records within the lookback window. Each record must
    carry at minimum the ``completion_key_field`` value and a timestamp field
    (``dispatched_at``, ``decided_at``, or ``at``). Returns list of
    {key, timestamp, completion_key} dicts.

    If ``completion_key_field`` is absent, the completion key is derived from
    the dispatch record's S3 key filename (stripped of path prefix) — used
    when the dispatch record and completion marker share the same base key
    (e.g. ci-watch: ``dispatched/repo-sha.json`` → ``completed/repo-sha.json``).
    """
    prefix = spec["dispatch_prefix"]
    lookback_hours = spec["lookback_hours"]
    mature_before = now - timedelta(minutes=spec["response_window_min"])
    horizon = now - timedelta(hours=lookback_hours)
    key_field = spec.get("completion_key_field")
    records: list[dict] = []
    token = None
    while True:
        kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        try:
            resp = s3.list_objects_v2(**kwargs)
        except Exception:  # noqa: BLE001 — best-effort enumeration
            logger.warning("dispatch_invocation_success: list failed for %s", prefix, exc_info=True)
            break
        for obj in resp.get("Contents", []) or []:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            last_modified = obj.get("LastModified")
            if last_modified and last_modified < horizon:
                continue
            try:
                body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                record = json.loads(body)
            except Exception:
                logger.warning("dispatch_invocation_success: unreadable %s", key)
                continue
            # Extract the completion key
            completion_key = None
            if key_field:
                # Field-based: the record itself names its completion key
                completion_key = record.get(key_field)
            if not completion_key:
                # Filename-based: dispatch and completion share the same filename
                filename = key.rsplit("/", 1)[-1]
                completion_key = filename  # e.g. "nousergon-crucible-research-abc1234.json"
            # Skip records missing a timestamp (we need to know when the dispatch happened)
            timestamp = record.get("dispatched_at") or record.get("decided_at") or record.get("at")
            if timestamp:
                try:
                    ts = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = last_modified or now
            else:
                ts = last_modified or now
            if ts < horizon:
                continue
            if ts > mature_before:
                continue  # not mature yet
            records.append({
                "dispatch_key": key,
                "dispatch_time": ts,
                "completion_key": completion_key,
            })
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return records


def _check_dispatch_invocation_success(spec: dict, now: datetime) -> tuple[list[str], dict]:
    """For every mature dispatch record in the lookback window, assert a
    completion marker exists at ``{completion_prefix}/{completion_key}.json``.
    A missing marker means the box was launched but never finished — it died
    before writing an outcome (spot reclaim / OOM / pre-trap crash / SSM
    command failure)."""
    problems: list[str] = []
    s3 = _s3_client()
    family = spec.get("family", spec.get("label", "unknown"))
    completion_prefix = spec["completion_prefix"]
    records = _family_dispatch_records(s3, spec, now)
    if not records:
        logger.info("dispatch_invocation_success[%s]: no mature dispatch records in window", family)
        return [], {}

    for rec in records:
        completion_key = f"{completion_prefix}{rec['completion_key']}.json"
        try:
            s3.head_object(Bucket=WATCH_BUCKET, Key=completion_key)
        except Exception as exc:
            if _error_code(exc) in {"NoSuchKey", "404", ""}:
                problems.append(
                    f"{family}: dispatch recorded at {rec['dispatch_time'].strftime('%Y-%m-%d %H:%M')}Z "
                    f"(s3://{WATCH_BUCKET}/{rec['dispatch_key']}) — NO completion marker at "
                    f"s3://{WATCH_BUCKET}/{completion_key} after "
                    f"{spec['response_window_min']}+ min. Box likely died silently "
                    "(spot reclaim / OOM / SSM failure / pre-trap crash) before writing its outcome."
                )
            else:
                raise
    return problems, {}


# ── Check: agent_dispatch_completeness ────────────────────────────────────────
# alpha-engine-config#6164: for every dispatch the dispatch ledger records as
# launched=True, the corresponding SSM command must have reached a terminal
# Success status. A dispatch that reported success and whose command terminated
# non-success (Failed/TimedOut/Cancelled) is a finding, immediately.
#
# This is the 2026-08-01 outage class (nous-ergon-ops-I368): every sensor
# upstream of the agent reported success — the SF dispatcher fired, routed via
# overseer-dispatcher (http=202, launched=True), spot box launched — but the
# bootstrap aborted 75s in with SECRET_ENV_FILE: unbound variable. SSM recorded
# Failed at PT1M75.6S. No issue, no PR, no alert. Found by a human nine hours
# later.
#
# Per overseer-policy.md §5 Layer B: reads SSM (the upstream system's own
# record), never the response plane's log.

# Terminal SSM command statuses that are NOT Success — these mean the command
# reached a terminal state without the agent completing successfully.
_ADC_FAILED_STATUSES = {"Failed", "TimedOut", "Cancelled", "Cancelling"}

# Non-terminal statuses — the command is still running or pending.
_ADC_RUNNING_STATUSES = {"Pending", "InProgress", "Delayed"}


def _check_agent_dispatch_completeness(spec: dict, now: datetime) -> tuple[list[dict], dict]:
    """For every dispatch the dispatch ledger records as launched=True within
    lookback_hours, the corresponding SSM command must have reached a terminal
    Success status. A command that terminated non-success (or is still running
    past the response window with the instance unresponsive) is a finding.

    Tolerates: (a) the window between ledger write and SSM command reaching a
    terminal state — commands still InProgress/Pending/Delayed within
    response_window_min are skipped; (b) commands whose SSM invocation no
    longer exists (beyond ~30-day retention) — the ledger entry is the only
    surviving record, so skip rather than false-flag.
    """
    problems: list[str] = []
    s3 = _s3_client()
    horizon = now - timedelta(hours=spec["lookback_hours"])
    mature_before = now - timedelta(minutes=spec["response_window_min"])
    ssm_retention = timedelta(days=spec.get("ssm_retention_days", 30))

    # 1. Enumerate recent dispatch-ledger entries.
    prefix_root = spec["dispatch_ledger_prefix"]
    lookup_dates: list[str] = []
    d = (horizon - timedelta(days=1)).date()
    last = now.date()
    while d <= last:
        lookup_dates.append(d.isoformat())
        d += timedelta(days=1)

    dispatched: list[dict] = []
    for date in lookup_dates:
        prefix = f"{prefix_root}{date}/"
        token = None
        while True:
            kwargs = {"Bucket": WATCH_BUCKET, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                try:
                    body = s3.get_object(Bucket=WATCH_BUCKET, Key=key)["Body"].read()
                    record = json.loads(body)
                except Exception as exc:  # noqa: BLE001 — one bad record must not hide the rest
                    logger.warning(
                        "agent_dispatch_completeness: ledger entry %s unreadable (%s) — skipped",
                        key, exc,
                    )
                    continue
                outcome = record.get("outcome") if isinstance(record, dict) else None
                if not isinstance(outcome, dict):
                    continue
                if outcome.get("launched") is not True:
                    continue
                launched_at = record.get("started_at")
                if not launched_at:
                    continue
                try:
                    t = datetime.fromisoformat(str(launched_at).replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if not (horizon <= t <= mature_before):
                    continue
                command_id = outcome.get("command_id")
                if not command_id:
                    # launched=True but no command_id — the executor returned a
                    # malformed verdict (the dispatcher writes what it gets).
                    # Record as a finding (the dispatch is unverifiable).
                    problems.append(
                        f"dispatch ledger entry s3://{WATCH_BUCKET}/{key}: "
                        f"launched=True but NO command_id in outcome — "
                        "unverifiable dispatch (executor verdict missing command_id)"
                    )
                    continue
                dispatched.append({
                    "ledger_key": key,
                    "command_id": command_id,
                    "playbook": record.get("playbook", "unknown"),
                    "launched_at": t,
                })
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

    if not dispatched:
        logger.info(
            "agent_dispatch_completeness: no mature launched dispatches in window"
        )
        return [], {}

    # 2. For each dispatched entry, check the SSM command invocation status.
    # Guard: ssm:ListCommandInvocations + ssm:DescribeInstanceInformation must
    # be in the probe's IAM role before this check can produce results (the
    # CHECKERS dispatch table isolates a per-check runtime error — an
    # AccessDenied will become its own problem line, not crash the probe).
    ssm = boto3.client("ssm", region_name=REGION)
    missed: list[dict] = []
    for entry in dispatched:
        command_id = entry["command_id"]
        try:
            invocations = ssm.list_command_invocations(
                CommandId=command_id, Details=False
            )
        except Exception as exc:  # noqa: BLE001 — isolate per I4473
            err_code = _error_code(exc)
            if err_code == "AccessDeniedException":
                # The IAM grant hasn't landed yet — report once, not per-entry.
                problems.append(
                    "agent_dispatch_completeness: ssm:ListCommandInvocations "
                    f"AccessDenied — IAM grant missing on the probe role; "
                    "all dispatched entries are UNVERIFIED this run"
                )
                break
            if err_code == "InvocationDoesNotExist":
                # Command is beyond SSM retention — skip (the ledger entry is
                # the only surviving record; we cannot verify it).
                continue
            logger.warning(
                "agent_dispatch_completeness: SSM ListCommandInvocations "
                "failed for command_id=%s: %s", command_id, exc,
            )
            continue

        cmd_invocations = invocations.get("CommandInvocations") or []
        if not cmd_invocations:
            # No invocation record at all — the command may not have been
            # delivered. If mature_before has passed, this is a finding.
            launched_age = now - entry["launched_at"]
            if launched_age > timedelta(minutes=spec["response_window_min"]):
                missed.append(entry)
            continue

        # SSM can return multiple invocations (one per instance); we care
        # about the aggregate: did ANY instance reach Success?
        terminal_success = False
        terminal_failed = False
        still_running = False
        failed_detail = ""
        for inv in cmd_invocations:
            status = inv.get("Status", "Unknown")
            if status == "Success":
                terminal_success = True
                break
            if status in _ADC_FAILED_STATUSES:
                terminal_failed = True
                status_details = inv.get("StatusDetails", "")
                failed_detail = f"Status={status}"
                if status_details:
                    failed_detail += f" StatusDetails={status_details}"
            elif status in _ADC_RUNNING_STATUSES:
                still_running = True

        if terminal_success:
            continue  # The agent completed — dispatch is whole.

        if terminal_failed:
            missed.append(entry)
            problems.append(
                f"{entry['playbook']} dispatch @ {entry['launched_at'].strftime('%Y-%m-%d %H:%M')}Z "
                f"(command_id={command_id}): launched=True but SSM command "
                f"terminated non-success ({failed_detail}) — the agent never "
                f"completed. Ledger: s3://{WATCH_BUCKET}/{entry['ledger_key']}"
            )
            continue

        if still_running:
            # Command still running past the response window — check instance
            # liveness the same way ssm-liveness-poller does.
            launched_age = now - entry["launched_at"]
            if launched_age > timedelta(minutes=spec["response_window_min"]):
                # Attempt to read PingStatus via DescribeInstanceInformation.
                # The instance_id is not in the dispatch ledger entry by
                # default (the ledger stores the dispatcher-level outcome,
                # which carries command_id but not instance_id). Fall back to
                # listing by command_id and reading instance IDs from the
                # invocation record.
                instance_ids = [
                    inv.get("InstanceId") for inv in cmd_invocations
                    if inv.get("InstanceId")
                ]
                instance_unresponsive = False
                for iid in instance_ids:
                    try:
                        info = ssm.describe_instance_information(
                            Filters=[
                                {"Key": "InstanceIds", "Values": [iid]}
                            ]
                        )
                        instances = info.get("InstanceInformationList") or []
                        if not instances:
                            instance_unresponsive = True
                            break
                        ping = instances[0].get("PingStatus", "Unknown")
                        if ping != "Online":
                            instance_unresponsive = True
                            break
                    except Exception:
                        continue
                if instance_unresponsive:
                    missed.append(entry)
                    problems.append(
                        f"{entry['playbook']} dispatch @ "
                        f"{entry['launched_at'].strftime('%Y-%m-%d %H:%M')}Z "
                        f"(command_id={command_id}): SSM command still "
                        f"InProgress past {spec['response_window_min']}min "
                        f"window AND instance unresponsive — the box is "
                        f"wedged or gone. Ledger: "
                        f"s3://{WATCH_BUCKET}/{entry['ledger_key']}"
                    )

    if not problems:
        return [], {}

    playbooks = sorted(set(m["playbook"] for m in missed))
    named = ", ".join(playbooks[:_HEADLINE_ITEMS])
    if len(playbooks) > _HEADLINE_ITEMS:
        named += f", +{len(playbooks) - _HEADLINE_ITEMS} more"
    return [_finding(
        "dispatch-completeness",
        f"{len(problems)} dispatch(es) launched but agent never completed "
        f"({named})",
        "\n".join(problems),
    )], {}


# ── Check dispatch table + aggregation ───────────────────────────────────────

CHECKERS = {
    "eventbridge_rule": _check_eventbridge_rule,
    "state_machines_exist": _check_state_machines_exist,
    "lambda_active": _check_lambda_active,
    "sqs_queue_exists": _check_sqs_queue_exists,
    "run_window": _check_run_window,
    "scheduler_schedule_exists": _check_scheduler_schedule_exists,
    "sf_watch_invocation_success": _check_sf_watch_invocation_success,
    "dispatch_invocation_success": _check_dispatch_invocation_success,
    "agent_dispatch_completeness": _check_agent_dispatch_completeness,
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


def _component_name(label: str) -> str:
    """Registry source label → the name a human uses for it. ``playbook:groom``
    is a lookup path; ``groom`` is what the page should say."""
    return label.split(":", 1)[1] if label.startswith("playbook:") else label.replace("_", "-")


def _run_checks(now: datetime) -> tuple[list[dict], dict[str, str], int, int]:
    """Run every registry-declared liveness check, aggregating findings +
    reported kill-switches.

    Checkers may return either a plain string (already one short sentence — it
    becomes both headline and detail) or a ``_finding`` dict. Normalisation
    happens HERE, once, so a check that has nothing to summarise stays a
    one-liner and only the checks that fan out carry the extra structure.

    An unknown check type RAISES (_RegistryError, fail-loud) — a registry that
    outran the probe's checker table is a packaging bug that makes the WHOLE
    registry untrustworthy, not a silent skip.

    A checker that raises at RUNTIME (an AWS API error — most often an IAM
    grant that drifted from the repo policy) is ISOLATED: it becomes its own
    problem line and every other check still runs (alpha-engine-config-I4473).
    Before this, one `AccessDenied` on one check aborted the handler and
    erased the coverage of all ~25 — a 2026-07-23 scheduler IAM drift left the
    probe reporting nothing at all for four days while the alert-drain plane
    died underneath it, unseen. Fail-loud is right; losing every *other*
    check's coverage to one check's failure is not, because a probe's whole
    job is to report N independent findings.

    Returns (findings, kill_switches, checks_run, checks_failed)."""
    registry = _registry()
    findings: list[dict] = []
    kill_switches: dict[str, str] = {}
    checks_run = 0
    checks_failed = 0
    for label, spec in _iter_check_specs(registry):
        ctype = spec.get("type")
        checker = CHECKERS.get(ctype)
        if checker is None:
            raise _RegistryError(f"{label}: unknown liveness check type {ctype!r}")
        checks_run += 1
        component = _component_name(label)
        try:
            p, ks = checker(spec, now)
        except Exception as exc:  # noqa: BLE001 — isolated per I4473; recorded as a finding below, never swallowed
            checks_failed += 1
            logger.error(
                "liveness check FAILED to run: %s type=%s: %s: %s",
                label, ctype, type(exc).__name__, exc,
            )
            findings.append(_finding(
                component,
                f"check '{ctype}' FAILED TO RUN ({type(exc).__name__}) — coverage ABSENT",
                f"{label}: liveness check '{ctype}' FAILED TO RUN "
                f"({type(exc).__name__}: {exc}) — this check's coverage is ABSENT; "
                "the component it watches is unverified, not healthy",
            ))
            continue
        for item in p:
            findings.append(item if isinstance(item, dict) else _finding(component, item))
        kill_switches.update(ks)

    if checks_run and checks_failed == checks_run:
        # Distinct, loudest case: the probe is structurally unable to observe
        # anything (blanket IAM/credential/network failure). Reporting this
        # identically to N individual check failures would understate it —
        # the plane is not degraded, it is BLIND.
        findings.insert(0, _finding(
            "PROBE",
            f"BLIND — all {checks_run} checks failed to run; the plane is unobserved",
            f"PROBE BLIND: all {checks_run} liveness checks failed to run — "
            "the watch plane is unobserved, not healthy. Check the probe role's "
            "IAM against infrastructure/lambdas/overseer-liveness-probe/iam-policy.json.",
        ))
    return findings, kill_switches, checks_run, checks_failed


# ── Dedup + alert (per-problem signature, alpha-engine-config-I5207) ─────────


def _stable_problem_key(problem: str) -> str:
    """SHA256 hex digest (first 16 hexits) of the problem text — used as a
    per-problem dedup identity.

    Each problem's FULL text is the key (including timestamps), because a
    run-window finding for a different trigger window IS a different problem.
    This is the correct semantic for age-out: if a problem text changes (e.g.
    the trigger timestamp advances for a still-missing run), it becomes a NEW
    per-problem entry — the old one resolves and the new one alerts once.
    """
    return hashlib.sha256(problem.encode()).hexdigest()[:16]


def _load_alerted_state(s3) -> dict[str, str] | None:
    """Previously-alerted per-problem state: ``{fingerprint: problem_text}``.

    Returns ``None`` when there is no prior state at all (first-ever run, or
    the state object has never been written) — distinct from ``{}``, which
    means \"same as healthy\" (the state was cleared after a clean pass).

    A missing/403 key (the common first-run or healthy-cleared case) skips
    the warning log — these are expected, not anomalous."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=STATE_KEY)
        raw = json.loads(obj["Body"].read())
        if isinstance(raw, dict):
            # Backward compat (I5207): old format stored a single
            # ``fingerprint`` key — treat it as "no per-problem state"
            # so the first run with new code treats everything as new
            # (safe one-time re-alert, after which the new format is
            # persisted).
            if "fingerprint" in raw and "per_problem" not in raw:
                return None
            return raw.get("per_problem") or {}
        return {}
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        if _error_code(exc) not in {"NoSuchKey", "404", "403", ""}:
            logger.warning("could not read overseer liveness state %s: %s", STATE_KEY, exc)
        return None


def _save_alerted_state(s3, state: dict[str, str] | None) -> None:
    """Persist (or clear) the per-problem dedup state.

    ``state is None`` (or ``{}``) clears the state — written as ``{}`` so
    the next load sees an empty set rather than ``None``, enabling the
    \"problems resolved\" branch without retriggering on a subsequent clean
    pass.

    Best-effort: a write failure only risks a duplicate/missed-clear ping
    next run (logged), never a missed finding — so it does NOT raise."""
    payload = {"per_problem": state or {}, "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        s3.put_object(
            Bucket=WATCH_BUCKET,
            Key=STATE_KEY,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — dedup state; failure only risks a dup ping
        logger.warning("could not persist overseer liveness state %s: %s", STATE_KEY, exc)


def _write_detail_report(s3, findings: list[dict], now: datetime,
                         checks_run: int, checks_failed: int) -> str | None:
    """Persist the full per-finding prose and return its s3:// URI, or None if
    the write failed.

    Best-effort BY DESIGN, and the one swallow in this module that is not a
    defect: the report is a delivery convenience for a page that must go out
    regardless. Raising here would trade a readable page for no page at all.
    Failure is recorded two ways — a logged warning, and the caller falling
    back to inlining every detail in the message, so the information is never
    lost, only rendered less pleasantly."""
    key = f"{REPORT_PREFIX}{now.strftime('%Y-%m-%dT%H%M%SZ')}.json"
    try:
        s3.put_object(
            Bucket=WATCH_BUCKET,
            Key=key,
            Body=json.dumps(
                {
                    "generated_at": now.isoformat(),
                    "checks_run": checks_run,
                    "checks_failed": checks_failed,
                    "findings": findings,
                },
                indent=2,
            ).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — delivery aid; caller inlines detail instead
        logger.warning("could not write liveness detail report %s: %s — "
                       "falling back to inline detail in the page", key, exc)
        return None
    return f"s3://{WATCH_BUCKET}/{key}"


def _alert_diff(
    current_problems: list[str],
    previous_state: dict[str, str] | None,
) -> tuple[list[str], list[str], list[str]]:
    """Diff the CURRENT problem set against the PREVIOUSLY-ALERTED per-problem
    state.

    Returns ``(new_problems, continuing_problems, resolved_keys)``:

    * ``new_problems`` — problems that exist now but were NOT in the previous
      state (ordered by the stable key for determinism). Never empty.
    * ``continuing_problems`` — problems whose fingerprint matches one in the
      previous state. Empty when ``previous_state is None``.
    * ``resolved_keys`` — keys present in the previous state but NOT in the
      current set. These should be aged out of the persisted state.

    ``previous_state is None`` (no prior state at all) means every current
    problem is treated as \"new\" — the probe has never alerted before, so
    there is nothing to compare against."""
    current_map: dict[str, str] = {}
    for p in current_problems:
        current_map[_stable_problem_key(p)] = p

    if previous_state is None:
        return list(current_map.values()), [], []

    prev_keys = set(previous_state.keys())
    cur_keys = set(current_map.keys())

    new_keys = cur_keys - prev_keys
    continuing_keys = cur_keys & prev_keys
    resolved = list(prev_keys - cur_keys)

    new_problems = [current_map[k] for k in sorted(new_keys)]
    continuing_problems = [current_map[k] for k in sorted(continuing_keys)]
    return new_problems, continuing_problems, resolved


def _alert(
    new_findings: list[dict],
    continuing_count: int,
    resolved_count: int,
    total_findings: int,
    kill_switches: dict[str, str] | None = None,
    checks_run: int = 0,
    checks_failed: int = 0,
    now: datetime | None = None,
) -> bool:
    """Page Brian with a summons he can act on from one screen.

    Carries: the new/continuing/resolved accounting (alpha-engine-config-I5207),
    ONE headline per new finding, the coverage caveat, and a pointer. Full prose
    goes to an S3 detail report.

    The prior format inlined every finding's full paragraph, so a bad morning
    produced a wall of near-identical text in which the one line that mattered —
    a groom run that engaged 0 of 21 issues — sat below five paragraphs of the
    same sentence about missed drain runs. It also pointed the reader at
    CloudWatch logs for anything past the cap; the report is a better answer to
    the same problem."""
    now = now or datetime.now(timezone.utc)
    report_uri = _write_detail_report(
        _s3_client(), new_findings, now, checks_run, checks_failed
    )

    parts = [f"{total_findings} finding(s)"]
    if new_findings:
        parts.append(f"{len(new_findings)} new")
    if continuing_count:
        parts.append(f"{continuing_count} continuing")
    if resolved_count:
        parts.append(f"{resolved_count} resolved")

    lines = [
        "\U0001f6f0️ *Overseer Liveness — watch plane*",
        f"{' · '.join(parts)} · {checks_run - checks_failed}/{checks_run} checks ran "
        "(the WATCHERS' own wiring, not a pipeline failure)",
        "",
    ]
    lines.extend(
        f"• *{f['component']}*: {f['headline']}" for f in new_findings[:_MAX_HEADLINES]
    )
    if len(new_findings) > _MAX_HEADLINES:
        lines.append(f"…and {len(new_findings) - _MAX_HEADLINES} more new")
    if checks_failed:
        # I4473: coverage caveat, stated BEFORE the closing pointer — a reader
        # must not read this report as a complete picture when part of the
        # plane went unobserved.
        lines.append(
            f"⚠️ *Coverage incomplete:* {checks_failed} of {checks_run} checks could not "
            "run — the components they watch are UNVERIFIED, not confirmed healthy."
        )
    lines.append("")
    if report_uri:
        lines.append(f"Detail: `{report_uri}`")
    else:
        # Degraded, and SAID so: the report write failed, so the full prose is
        # inlined here rather than dropped.
        lines.append("_Detail report unavailable (S3 write failed) — full findings inline:_")
        lines.extend(f"— {f['detail']}" for f in new_findings)
    text = "\n".join(lines)

    # Flow-doctor dedup key: hash the new-problem keys + continuing count so
    # a run that only adds new problems gets through, but a run whose
    # continuing set is unchanged does not re-page if flow-doctor called us
    # with the same set twice (belt-and-braces — the handler never calls
    # _alert when new_problems is empty).
    dedup_fingerprint = hashlib.sha256(
        "\n".join(sorted(_stable_problem_key(f["detail"]) for f in new_findings)
                   + [f"c:{continuing_count}", f"r:{resolved_count}"]).encode()
    ).hexdigest()[:16]

    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=f"{_FLOW_NAME}:wiring:{dedup_fingerprint}",
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context={
                "problems": total_findings,
                "new": len(new_findings),
                "continuing": continuing_count,
                "resolved": resolved_count,
                "detail_report": report_uri,
                "kill_switches": kill_switches or {},
                "checks_run": checks_run,
                "checks_failed": checks_failed,
            },
            # No playbooks.yaml alert_classes row exists yet for this Lambda's
            # own identity (config-I3513 audit finding) — note this is
            # DISTINCT from the registered `overseer_dispatch_escalation`
            # class (source "overseer-dispatcher"), which belongs to the
            # separate overseer-dispatcher Lambda. Using _FLOW_NAME is still
            # strictly correct (this Lambda's own naming convention);
            # follow-up filed to add a row.
            source=_FLOW_NAME,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("overseer liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Iterates the playbook registry,
    runs every declared liveness check read-only, dedups PER PROBLEM
    (alpha-engine-config-I5207), and LOUD-alerts only when NEW problems appear.

    Per-problem dedup replaces the old aggregate-fingerprint approach: each
    problem gets its own signature tracked in S3 state. On each run the
    current problem set is diffed against the saved set:
      * NEW problems page individually (capped at 5).
      * CONTINUING problems (same signature as before) are summarized as a
        count — the list was already delivered, the reader has seen it.
      * RESOLVED problems (key absent from current set) age out of the state
        so a genuine recurrence later pages again.

    A runtime AWS failure in ONE check is isolated into its own problem line
    so the rest of the plane is still observed (alpha-engine-config-I4473);
    the per-problem dedup means a single new check failure does not re-page
    every other standing check's problem."""
    now = datetime.now(timezone.utc)
    findings, kill_switches, checks_run, checks_failed = _run_checks(now)

    # Always surfaced (record + log), never alerted: a deliberate operator
    # disable is state, not an incident.
    logger.info("overseer liveness: dispatch kill-switches: %s", kill_switches)

    s3 = _s3_client()
    previous_state = _load_alerted_state(s3)

    new_count = continuing_count = resolved_count = 0
    alerted = False

    if findings:
        # Dedup identity stays the finding's full DETAIL text (I5207 semantics
        # unchanged): the headline is presentation, and a re-worded page must
        # never be able to re-page on its own.
        by_detail = {f["detail"]: f for f in findings}
        new_details, continuing_details, resolved_keys = _alert_diff(
            list(by_detail), previous_state
        )
        new_findings = [by_detail[d] for d in new_details]
        new_count = len(new_findings)
        continuing_count = len(continuing_details)
        resolved_count = len(resolved_keys)

        if new_findings:
            logger.warning(
                "overseer liveness: %d new, %d continuing, %d resolved: %s",
                new_count, continuing_count, resolved_count, new_details,
            )
            alerted = _alert(
                new_findings, continuing_count, resolved_count,
                len(findings), kill_switches, checks_run, checks_failed, now,
            )
            if alerted:
                # Persist the FULL current set so the next run can diff against it.
                _save_alerted_state(s3, {_stable_problem_key(d): d for d in by_detail})
        else:
            logger.info(
                "overseer liveness: %d problem(s), unchanged since last alert — suppressed",
                len(findings),
            )
    else:
        logger.info("overseer liveness: all checks clean")
        if previous_state is not None:
            _save_alerted_state(s3, None)  # clear dedup state now that it's healthy again

    return {
        # `problems` stays the full prose list — the payload is a record, not a
        # page, and nothing that reads it benefits from the shortening.
        "problems": [f["detail"] for f in findings],
        "findings": findings,
        "alerted": alerted,
        "clean": not findings,
        "kill_switches": kill_switches,
        "new": new_count,
        "continuing": continuing_count,
        "resolved": resolved_count,
        # I4473: coverage accounting. `clean: true` with checks_failed > 0 is
        # impossible by construction (a failed check IS a finding), but these
        # make "how much did we actually observe" answerable from the return
        # payload alone, without reading logs.
        "checks_run": checks_run,
        "checks_failed": checks_failed,
    }
