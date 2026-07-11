"""alpha-engine-sf-watch-liveness-probe — external wiring-integrity check for
Fleet-SF Watch itself.

Fleet-SF Watch (saturday-sf-watch-dispatcher) is event-driven: it only fires
when a registered pipeline's Step Function reaches a terminal FAILED/TIMED_OUT
/ABORTED status via its EventBridge rule. That means there is no natural
"session" to report a begin/end for — and, critically, NOTHING notices if the
watcher's own wiring silently breaks. That is exactly what happened on
2026-06-29: the EventBridge rule pointed at a deleted SF ARN for an unknown
period before a real failure exposed it, and the Lambda's own Errors metric
stayed at zero the whole time — it simply never got invoked. A "0 errors"
health signal looked fine while the watcher was completely dead.

This probe is the external watchdog FOR the watchdog — mirrors the groom
liveness probe's philosophy (an external observer of a producer that cannot be
trusted to report its own death), applied one layer up. It runs on a schedule
and asserts, read-only:

  1. The EventBridge rule exists, is ENABLED, and targets the expected Lambda.
  2. The rule's registered stateMachineArn list matches EXPECTED_PIPELINE_NAMES
     below (keep in lockstep with saturday-sf-watch-dispatcher/index.py's
     PIPELINES dict AND that dispatcher's own deploy.sh EVENT_PATTERN — a
     regression test cross-checks this file against deploy.sh, mirroring
     test_registry_and_eventbridge_rule_are_in_lockstep in that Lambda's own
     tests).
  3. Every expected SF ARN's state machine actually EXISTS — catches the exact
     2026-06-29 dead-ARN class directly, instead of waiting for a real failure
     to expose it.
  4. The target Lambda is Active with a successful last code update.
  5. The EC2-spot dispatch leg — the LIVE repair path since the 2026-07-10
     spot migration (config#2001/#2106): alpha-engine-sf-watch-spot-dispatcher
     and alpha-engine-ci-watch-dispatcher exist and are Active. Their
     kill-switch env values (SF_WATCH_DISPATCH_ENABLED /
     CI_WATCH_DISPATCH_ENABLED) are READ AND REPORTED in the probe record so a
     disabled watch is visible — but never alerted on: a deliberate operator
     disable is state, not an incident (config#2265; sweep obligation lives
     with config#2257).
  6. The spot launch config still exists, read from the DEPLOYED spot
     dispatcher's live env (SF_WATCH_AMI_ID / SF_WATCH_SECURITY_GROUP /
     SF_WATCH_SUBNETS — pinned by that Lambda's deploy.sh, so the env is the
     observable source of truth; this probe deliberately duplicates NO
     constants): DescribeImages / DescribeSecurityGroups / DescribeSubnets.
     A deregistered AMI or deleted SG/subnet would break every future spot
     launch with ZERO signal until the next real failure needed a repair —
     the exact "healthy idle vs silently broken idle" class that bit twice on
     2026-07-10. A missing expected env key is itself a LOUD finding, never a
     skip.

Silent-unless-broken (mirrors the groom probe and Fleet-SF Watch's own
failure-driven design): a clean check logs and returns, no Telegram noise. Any
problem fires a LOUD alert, deduplicated by the CONTENT of the problem set
(a hash), not a timestamp — so a standing issue doesn't re-ping every run, and
the alert state clears automatically the moment the check is clean again.

**Mid-run spot-reclaim checker (config#2270).** This Lambda is ALSO the
EventBridge target for `EC2 Spot Instance Interruption Warning` and
`EC2 Instance State-change Notification` (state=terminated) events — the
handler branches on event shape (the scheduled probe payload is `{}`; the EC2
events carry `source: aws.ec2`). A spot reclaim MID-REPAIR used to kill the
watch box with no completion marker, no failure marker, and no relaunch —
detection was the >=6.5h spot-orphan-reaper ping. The checker lives HERE
(rather than a new Lambda) per the issue's candidate-home evaluation: this is
already the external observer of the watch plane. Flow: the EC2 events carry
only an instance-id (no tags — the rules cannot be tag-scoped), so the
checker DescribeTags the instance (tags stay queryable post-termination for a
while) → non-watch boxes (`Name` != alpha-engine-sf-watch-spot) exit quietly
→ for a watch box it heads the S3 completion marker
`sf_watch/_control/completed/{cadence}-{pipeline}-{run_date}.json` (the same
key the spot-orphan-reaper derives) → marker present = clean run, exit;
marker ABSENT = the box died mid-run: re-invoke
alpha-engine-sf-watch-spot-dispatcher ONCE with the original dispatch fields
(reconstructed from the discriminator tags + the newest watch-log event) and
`force_on_demand: true` — the groom-SF bounded-relaunch pattern
(config#1645 / nousergon-data#658). Exactly-one bound: the relaunch decision
is recorded as an `action: reclaim_relaunch` watch-log event (which the
saturday dispatcher's config#2269 mechanical attempt ceiling counts) BEFORE
the invoke; a second death for the same (cadence, pipeline, run_date)
escalates LOUD instead of relaunching again (second death = human).

**Fail-loud (CLAUDE.md no-silent-fails).** Every AWS describe/list call here is
the PRIMARY input: an UNEXPECTED API error (anything other than the specific
"this resource doesn't exist" codes we're explicitly checking for) RAISES, so a
broken probe surfaces via the Lambda Errors metric — alarmed by the watch-plane
CloudWatch alarms provisioned in infrastructure/setup_watch_plane_alarms.sh
(the dead-probe backstop) — rather than silently skipping the one check that
verifies nothing else is silently broken.
The Telegram alert itself is a secondary delivery surface: its own failure is
logged + returned, not raised.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
_FLOW_NAME = "sf-watch-liveness-probe"
_DB_BASENAME = "flow_doctor_sf_watch_liveness_probe"
_OPS_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

RULE_NAME = os.environ.get("SF_WATCH_RULE_NAME", "alpha-engine-saturday-sf-watch-failed")
EXPECTED_TARGET_FUNCTION = os.environ.get(
    "SF_WATCH_FUNCTION_NAME", "alpha-engine-saturday-sf-watch-dispatcher"
)
# MUST stay in lockstep with saturday-sf-watch-dispatcher/index.py's PIPELINES
# dict AND that dispatcher's own deploy.sh EVENT_PATTERN (test_handler.py cross-
# checks this list against deploy.sh's literal ARNs, mirroring the sibling
# lockstep guard already in saturday-sf-watch-dispatcher/test_handler.py).
EXPECTED_PIPELINE_NAMES = [
    "ne-weekly-freshness-pipeline",
    "ne-preopen-trading-pipeline",
    "ne-postclose-trading-pipeline",
    # transitional alpha-engine-eod-pipeline alias retired 2026-07-11
    # (config#2272; dormant old SF deleted live).
]

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
STATE_KEY = os.environ.get("SF_WATCH_LIVENESS_STATE_KEY", "consolidated/sf_watch_liveness/alerted.json")

# ── EC2-spot dispatch leg (the LIVE repair path since 2026-07-10) ────────────
SPOT_DISPATCHER_FUNCTION = os.environ.get(
    "SF_WATCH_SPOT_DISPATCHER_FUNCTION", "alpha-engine-sf-watch-spot-dispatcher"
)
CI_WATCH_DISPATCHER_FUNCTION = os.environ.get(
    "CI_WATCH_DISPATCHER_FUNCTION", "alpha-engine-ci-watch-dispatcher"
)
# (function name, kill-switch env key). The kill-switch value is REPORTED in
# the probe record, never alerted on — a deliberate operator disable is state,
# not an incident.
SPOT_LEG_DISPATCHERS: list[tuple[str, str]] = [
    (SPOT_DISPATCHER_FUNCTION, "SF_WATCH_DISPATCH_ENABLED"),
    (CI_WATCH_DISPATCHER_FUNCTION, "CI_WATCH_DISPATCH_ENABLED"),
]
# Launch-config keys read from the DEPLOYED spot dispatcher's live env (its
# deploy.sh pins them — a lockstep test in sf-watch-spot-dispatcher/
# test_handler.py holds deploy.sh's pins equal to that index.py's defaults).
# Reading the live env instead of re-declaring the values here means this
# probe can never drift from what the dispatcher will actually launch with.
LAUNCH_CONFIG_ENV_KEYS = ("SF_WATCH_AMI_ID", "SF_WATCH_SECURITY_GROUP", "SF_WATCH_SUBNETS")

# ── Mid-run spot-reclaim checker (config#2270) ───────────────────────────────
# Tag names/keys mirror sf-watch-spot-dispatcher/index.py (SF_WATCH_TAG_NAME +
# the three SF_WATCH_*_TAG_KEY discriminators) and spot-orphan-reaper's
# WATCH_KINDS sf-watch entry — the same triple every watch-plane consumer keys
# on. The completion-marker key shape below is the reaper's `_completion_key`
# shape for the sf-watch kind.
SF_WATCH_SPOT_TAG_NAME = "alpha-engine-sf-watch-spot"
SF_WATCH_CADENCE_TAG_KEY = "sf-watch-cadence"
SF_WATCH_PIPELINE_TAG_KEY = "sf-watch-pipeline"
SF_WATCH_RUN_DATE_TAG_KEY = "sf-watch-run-date"
COMPLETION_MARKER_PREFIX = "sf_watch/_control/completed/"
RECLAIM_INTERRUPTION_DETAIL_TYPE = "EC2 Spot Instance Interruption Warning"
RECLAIM_STATE_CHANGE_DETAIL_TYPE = "EC2 Instance State-change Notification"
RECLAIM_DETAIL_TYPES = frozenset(
    {RECLAIM_INTERRUPTION_DETAIL_TYPE, RECLAIM_STATE_CHANGE_DETAIL_TYPE}
)


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


def _check_rule() -> list[str]:
    """Rule existence/state/target. Fail-loud on any error code OTHER than the
    specific "does not exist" one we're explicitly checking for."""
    problems: list[str] = []
    events = _events_client()
    try:
        rule = events.describe_rule(Name=RULE_NAME)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return [f"EventBridge rule '{RULE_NAME}' does NOT EXIST"]
        raise

    if rule.get("State") != "ENABLED":
        problems.append(f"EventBridge rule '{RULE_NAME}' is {rule.get('State')}, not ENABLED")

    targets = events.list_targets_by_rule(Rule=RULE_NAME).get("Targets", [])
    target_arns = {t.get("Arn", "") for t in targets}
    expected_fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{EXPECTED_TARGET_FUNCTION}"
    if expected_fn_arn not in target_arns:
        problems.append(
            f"rule '{RULE_NAME}' does not target {EXPECTED_TARGET_FUNCTION} "
            f"(targets: {sorted(target_arns) or 'NONE'})"
        )

    pattern = json.loads(rule.get("EventPattern", "{}"))
    registered = set(pattern.get("detail", {}).get("stateMachineArn", []))
    registered_names = {arn.rsplit(":", 1)[-1] for arn in registered}
    expected_names = set(EXPECTED_PIPELINE_NAMES)
    missing = expected_names - registered_names
    extra = registered_names - expected_names
    if missing:
        problems.append(f"rule is MISSING expected pipeline(s): {sorted(missing)}")
    if extra:
        problems.append(f"rule has UNEXPECTED extra pipeline(s) not in the registry: {sorted(extra)}")
    return problems


def _check_state_machines_exist() -> list[str]:
    """Each expected pipeline's SF must actually exist — the exact 2026-06-29
    dead-ARN bug class, caught directly instead of waiting for a real failure."""
    problems: list[str] = []
    sfn = _sfn_client()
    for name in EXPECTED_PIPELINE_NAMES:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
        try:
            sfn.describe_state_machine(stateMachineArn=arn)
        except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
            if _error_code(exc) == "StateMachineDoesNotExist":
                problems.append(f"registered pipeline '{name}' has NO live Step Function (dead ARN)")
            else:
                raise
    return problems


def _check_lambda_healthy() -> list[str]:
    problems: list[str] = []
    lam = _lambda_client()
    try:
        cfg = lam.get_function_configuration(FunctionName=EXPECTED_TARGET_FUNCTION)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) == "ResourceNotFoundException":
            return [f"target Lambda '{EXPECTED_TARGET_FUNCTION}' does NOT EXIST"]
        raise
    if cfg.get("State") != "Active":
        problems.append(f"target Lambda '{EXPECTED_TARGET_FUNCTION}' state={cfg.get('State')}, not Active")
    if cfg.get("LastUpdateStatus") != "Successful":
        problems.append(
            f"target Lambda '{EXPECTED_TARGET_FUNCTION}' LastUpdateStatus={cfg.get('LastUpdateStatus')}"
        )
    return problems


def _check_launch_config(env: dict[str, str]) -> list[str]:
    """The deregistered-AMI silent-break guard: assert the AMI/SG/subnets the
    DEPLOYED spot dispatcher would launch with still exist. Uses Filters (not
    ImageIds/GroupIds/SubnetIds) so a missing resource comes back as an EMPTY
    result set instead of a per-service error code to pattern-match; unexpected
    API errors therefore always RAISE (fail-loud)."""
    problems: list[str] = []

    missing_keys = sorted(k for k in LAUNCH_CONFIG_ENV_KEYS if not (env.get(k) or "").strip())
    if missing_keys:
        # Fail-loud on env absence: an unreadable launch config is itself the
        # finding (deploy.sh pins these keys; their absence means the deployed
        # env drifted from deploy.sh). Deliberately STOP here rather than probe
        # EC2 with unknown ids — the problem line above is the recording surface.
        problems.append(
            f"spot dispatcher '{SPOT_DISPATCHER_FUNCTION}' live env is MISSING launch-config "
            f"key(s) {missing_keys} — AMI/SG/subnet existence is UNVERIFIABLE (its deploy.sh "
            "pins these; redeploy it)"
        )
        return problems

    ami = env["SF_WATCH_AMI_ID"].strip()
    sg = env["SF_WATCH_SECURITY_GROUP"].strip()
    subnets = sorted({s.strip() for s in env["SF_WATCH_SUBNETS"].split(",") if s.strip()})

    ec2 = _ec2_client()

    # IncludeDeprecated: an old-but-still-registered AMI must NOT false-alarm —
    # only a deregistered/deleted one (which every future spot launch would
    # fail on) is a finding.
    images = ec2.describe_images(
        Filters=[{"Name": "image-id", "Values": [ami]}], IncludeDeprecated=True
    ).get("Images", [])
    if not images:
        problems.append(
            f"spot AMI '{ami}' NOT FOUND (deregistered/deleted) — every future "
            "sf-watch spot launch would fail"
        )
    elif images[0].get("State") != "available":
        problems.append(f"spot AMI '{ami}' state={images[0].get('State')}, not available")

    groups = ec2.describe_security_groups(
        Filters=[{"Name": "group-id", "Values": [sg]}]
    ).get("SecurityGroups", [])
    if not groups:
        problems.append(f"spot security group '{sg}' NOT FOUND")

    found_subnets = {
        s.get("SubnetId")
        for s in ec2.describe_subnets(
            Filters=[{"Name": "subnet-id", "Values": subnets}]
        ).get("Subnets", [])
    }
    missing_subnets = sorted(set(subnets) - found_subnets)
    if missing_subnets:
        problems.append(f"spot subnet(s) NOT FOUND: {missing_subnets}")

    return problems


def _check_spot_dispatch_leg() -> tuple[list[str], dict[str, str]]:
    """The live EC2-spot repair path (config#2001/#2106): both dispatcher
    Lambdas exist + Active, kill-switch env values read + REPORTED (never
    alerted — see module docstring), and the spot dispatcher's launch config
    verified against live EC2 state."""
    problems: list[str] = []
    kill_switches: dict[str, str] = {}
    lam = _lambda_client()

    for fn_name, switch_key in SPOT_LEG_DISPATCHERS:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
        except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
            if _error_code(exc) == "ResourceNotFoundException":
                problems.append(
                    f"spot-leg dispatcher Lambda '{fn_name}' does NOT EXIST — "
                    "the live repair path cannot launch"
                )
                kill_switches[switch_key] = "UNREADABLE(function missing)"
                # Launch-config check deliberately skipped for a missing spot
                # dispatcher: there is no env to read, and the does-NOT-EXIST
                # problem line above is the loud recording surface for it.
                continue
            raise
        if cfg.get("State") != "Active":
            problems.append(
                f"spot-leg dispatcher Lambda '{fn_name}' state={cfg.get('State')}, not Active"
            )
        if cfg.get("LastUpdateStatus") != "Successful":
            problems.append(
                f"spot-leg dispatcher Lambda '{fn_name}' LastUpdateStatus={cfg.get('LastUpdateStatus')}"
            )
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        # REPORTED, never alerted: absence of the key means the dispatcher's
        # own in-code default applies ("true").
        kill_switches[switch_key] = env.get(switch_key, "unset(default:true)")
        if fn_name == SPOT_DISPATCHER_FUNCTION:
            problems.extend(_check_launch_config(env))

    return problems, kill_switches


def _problem_fingerprint(problems: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(problems)).encode()).hexdigest()[:16]


def _load_alerted_fingerprint(s3) -> str | None:
    """None means either 'no state yet' or 'currently healthy' — both treated
    the same way (nothing to suppress against)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read()).get("fingerprint")
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        if _error_code(exc) not in {"NoSuchKey", "404", "403", ""}:
            logger.warning("could not read sf-watch liveness state %s: %s", STATE_KEY, exc)
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
        logger.warning("could not persist sf-watch liveness state %s: %s", STATE_KEY, exc)


def _alert(problems: list[str], kill_switches: dict[str, str] | None = None) -> bool:
    lines = [
        "\U0001f6f0️ *Fleet-SF Watch Liveness Probe — WIRING PROBLEM*",
        f"{len(problems)} issue(s) found with the Fleet-SF Watch trigger itself "
        "(NOT a pipeline failure — the WATCHER's own wiring):",
    ]
    for p in problems:
        lines.append(f"• {p}")
    lines.append(
        "_Fleet-SF Watch may not catch (or repair) a real pipeline failure right "
        "now. Check the EventBridge rule, the saturday-sf-watch-dispatcher "
        "Lambda, and the sf-watch/ci-watch spot dispatchers._"
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
        logger.warning("sf-watch liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


# ── Mid-run spot-reclaim checker (config#2270) ───────────────────────────────


def _is_reclaim_event(event: dict) -> bool:
    """True iff this invocation is an EC2 reclaim/termination EventBridge
    event rather than the scheduled probe (whose payload is ``{}``)."""
    return (
        isinstance(event, dict)
        and event.get("source") == "aws.ec2"
        and event.get("detail-type") in RECLAIM_DETAIL_TYPES
    )


def _instance_tags(instance_id: str) -> dict[str, str]:
    """Tags for ``instance_id`` via DescribeTags — still queryable for a
    while after termination, unlike the instance record itself. Raises on any
    API error (fail-loud: the tags are the PRIMARY input of the reclaim
    check; an unreadable tag set must surface via the Lambda Errors metric,
    never silently classify a dead watch box as 'not ours')."""
    resp = _ec2_client().describe_tags(
        Filters=[{"Name": "resource-id", "Values": [instance_id]}]
    )
    return {t.get("Key", ""): t.get("Value", "") for t in resp.get("Tags", [])}


def _completion_marker_exists(marker_key: str) -> bool:
    """HeadObject on the run's completion marker. Only a true absence
    (404/NoSuchKey/NotFound) means "no marker"; any OTHER error RAISES —
    misreading an S3 hiccup as 'absent' would fire a duplicate relaunch, and
    misreading it as 'present' would silently drop coverage (the config#2267
    site-4 lesson, applied to this read)."""
    try:
        _s3_client().head_object(Bucket=WATCH_BUCKET, Key=marker_key)
        return True
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _load_watch_log(s3, watch_log_key: str) -> dict | None:
    """The day's watch-log doc, or None when it truly doesn't exist (404).
    Any other read error RAISES (fail-loud — the exactly-one relaunch bound
    depends on this read). A present-but-unparseable doc also returns None:
    without a readable event history the checker can neither reconstruct the
    dispatch fields nor verify the exactly-one bound, and the None path below
    escalates LOUDLY instead of relaunching — that escalation is the
    recording surface for this swallow (failure mode swallowed: a corrupted
    watch-log blob; the reclaim finding itself still pages)."""
    try:
        obj = s3.get_object(Bucket=WATCH_BUCKET, Key=watch_log_key)
    except Exception as exc:  # noqa: BLE001 — inspect code below; re-raise if unexpected
        if _error_code(exc) in {"404", "NoSuchKey"}:
            return None
        raise
    try:
        doc = json.loads(obj["Body"].read())
    except (ValueError, TypeError) as exc:
        logger.warning("watch-log %s unparseable during reclaim check: %s", watch_log_key, exc)
        return None
    if isinstance(doc, dict) and isinstance(doc.get("events"), list):
        return doc
    logger.warning("watch-log %s has an unexpected shape during reclaim check", watch_log_key)
    return None


def _record_reclaim_relaunch(s3, watch_log_key: str, doc: dict, record: dict) -> None:
    """Append the relaunch decision to the day's watch-log and write it back.
    PRIMARY deliverable of the relaunch path — RAISES on failure, and runs
    BEFORE the dispatcher invoke: if this write fails, NO relaunch fires (the
    Lambda error pages via the watch-plane alarms) — the safe failure
    direction, since a relaunch without its record would break the
    exactly-one bound and permit unbounded relaunches."""
    doc["updated_at"] = record["detected_at"]
    doc["events"].append(record)
    s3.put_object(
        Bucket=WATCH_BUCKET,
        Key=watch_log_key,
        Body=json.dumps(doc, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def _reclaim_escalate(text: str, dedup_key: str, context_info: dict) -> bool:
    """LOUD reclaim-path escalation (second death / unreconstructable dispatch
    fields = human needed). Best-effort delivery surface — a Telegram outage
    logs WARNING and is returned, never masks the finding (which is already
    in the CloudWatch log + returned verdict)."""
    try:
        return notify_via_flow_doctor(
            text,
            silent=False,
            severity="error",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still logged + returned
        logger.warning("reclaim escalation Telegram send failed (non-fatal): %s", exc)
        return False


def _reclaim_note(text: str, dedup_key: str, context_info: dict) -> bool:
    """Silent Telegram note for a successful bounded relaunch (mirrors the
    dispatcher's silent 'watch is acting' receipt). Best-effort."""
    try:
        return notify_via_flow_doctor(
            text,
            silent=True,
            severity="info",
            dedup_key=dedup_key,
            flow_name=_FLOW_NAME,
            topics=_OPS_TOPICS,
            db_basename=_DB_BASENAME,
            context=context_info,
            silent_topic=FleetTelegramTopic.OPS_HEALTH,
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; relaunch already fired + recorded
        logger.warning("reclaim relaunch Telegram note failed (non-fatal): %s", exc)
        return False


def _handle_reclaim_event(event: dict) -> dict:
    """Mid-run spot-reclaim checker (config#2270) — see module docstring."""
    detail = event.get("detail") or {}
    detail_type = str(event.get("detail-type") or "")
    instance_id = str(detail.get("instance-id") or "")
    if not instance_id:
        # Rule contract violation — the EventBridge patterns always carry an
        # instance-id. Fail loud rather than silently ignoring a malformed
        # event that might be a real watch-box death.
        raise ValueError(f"EC2 reclaim event without instance-id (detail-type={detail_type!r})")
    base = {"reclaim_event": True, "detail_type": detail_type, "instance_id": instance_id}

    # Defense in depth: the terminated rule's pattern filters state=terminated;
    # a stopping/running state-change reaching us anyway is not a death.
    if detail_type == RECLAIM_STATE_CHANGE_DETAIL_TYPE and str(detail.get("state") or "") != "terminated":
        logger.info("reclaim check: ignoring non-terminated state-change for %s", instance_id)
        return {**base, "handled": False, "reason": "not_terminated"}

    tags = _instance_tags(instance_id)
    if tags.get("Name") != SF_WATCH_SPOT_TAG_NAME:
        # Every instance in the account hits these rules (they cannot be
        # tag-scoped) — non-watch boxes exit quietly, log only.
        logger.info("reclaim check: %s is not an sf-watch box (Name=%r) — ignoring",
                    instance_id, tags.get("Name"))
        return {**base, "watch_box": False}

    cadence = tags.get(SF_WATCH_CADENCE_TAG_KEY, "")
    pipeline = tags.get(SF_WATCH_PIPELINE_TAG_KEY, "")
    run_date = tags.get(SF_WATCH_RUN_DATE_TAG_KEY, "")
    if not (cadence and pipeline and run_date):
        # A watch box died inside the narrow launch→tag window (or a tag write
        # regressed): no discriminators means no marker key and no dispatch
        # fields — a human must look. LOUD, never a quiet drop.
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — UNTAGGED watch box died*\n"
            f"Watch box `{instance_id}` terminated without its cadence/pipeline/"
            "run-date discriminator tags — cannot verify completion or relaunch. "
            "Check the sf-watch-spot-dispatcher tag-write path (config#2267 site 2).",
            dedup_key=f"{_FLOW_NAME}:reclaim_untagged:{instance_id}",
            context_info={"instance_id": instance_id, "tags": tags},
        )
        return {**base, "watch_box": True, "handled": False,
                "reason": "missing_discriminator_tags", "escalated": alerted}

    key_ctx = {"instance_id": instance_id, "cadence": cadence,
               "pipeline": pipeline, "run_date": run_date}
    marker_key = f"{COMPLETION_MARKER_PREFIX}{cadence}-{pipeline}-{run_date}.json"
    if _completion_marker_exists(marker_key):
        logger.info("reclaim check: %s finished cleanly (marker %s present)",
                    instance_id, marker_key)
        return {**base, "watch_box": True, "completed": True}

    # No completion marker: the box died MID-RUN.
    watch_log_key = f"consolidated/{cadence}_sf_watch/{run_date}.json"
    s3 = _s3_client()
    doc = _load_watch_log(s3, watch_log_key)
    events = (doc or {}).get("events", [])
    relaunches = [ev for ev in events if ev.get("action") == "reclaim_relaunch"]

    if any(ev.get("dead_instance_id") == instance_id for ev in relaunches):
        # The interruption WARNING and the terminated state-change both fire
        # for one reclaim — the second notification of the SAME death is a
        # duplicate, not a second death.
        logger.info("reclaim check: death of %s already handled — duplicate notification",
                    instance_id)
        return {**base, "watch_box": True, "completed": False,
                "duplicate_notification": True}

    if relaunches:
        # Exactly-one bound: a DIFFERENT box already died and was relaunched
        # for this (cadence, pipeline, run_date) — second death = human.
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — SECOND watch-box death*\n"
            f"{cadence}/{pipeline}@{run_date}: relaunched box `{instance_id}` "
            "ALSO died without a completion marker (prior relaunch: "
            f"`{relaunches[-1].get('dead_instance_id', '?')}` → on-demand). "
            "The bounded relaunch budget is spent — human needed (config#2270).",
            dedup_key=f"{_FLOW_NAME}:reclaim_second_death:{pipeline}:{run_date}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "second_death", "escalated": alerted}

    # First mid-run death for this key: reconstruct the dispatch fields. The
    # tags carry (cadence, pipeline, run_date); the execution context comes
    # from the NEWEST watch-log event carrying an execution_arn — the failure
    # this box was dispatched for (the dispatcher writes the log BEFORE
    # dispatching, so a dispatched box always has one... unless the log is
    # missing/corrupted, in which case escalate LOUD instead of guessing).
    source_ev = next((ev for ev in reversed(events) if ev.get("execution_arn")), None)
    if source_ev is None:
        alerted = _reclaim_escalate(
            "\U0001f6a8 *SF-Watch reclaim checker — watch box died mid-run, "
            "dispatch fields UNRECONSTRUCTABLE*\n"
            f"{cadence}/{pipeline}@{run_date}: box `{instance_id}` died without "
            f"a completion marker, and the watch-log `{watch_log_key}` has no "
            "usable event to rebuild the dispatch from — relaunch manually "
            "(config#2270).",
            dedup_key=f"{_FLOW_NAME}:reclaim_no_source:{pipeline}:{run_date}",
            context_info=key_ctx,
        )
        return {**base, "watch_box": True, "completed": False, "relaunched": False,
                "reason": "no_source_event", "escalated": alerted}

    payload = {
        "pipeline_name": pipeline,
        "cadence_slug": cadence,
        "run_date": run_date,
        "execution_arn": source_ev.get("execution_arn", ""),
        "state_machine_arn": f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{pipeline}",
        "failed_state": source_ev.get("failed_state") or "",
        "cause": source_ev.get("cause") or "",
        "watch_log_key": watch_log_key,
        "is_preflight": "true" if source_ev.get("is_preflight") else "false",
        # The whole point: a spot reclaim already proved spot unreliable for
        # this run — relaunch straight to on-demand (lib >= 0.106.0
        # launch_with_fallback(force_on_demand=True), config#1645 pattern).
        "force_on_demand": "true",
    }

    now_iso = datetime.now(timezone.utc).isoformat()
    record = {
        "detected_at": now_iso,
        # Counted by the saturday dispatcher's config#2269 mechanical attempt
        # ceiling (_BUDGET_CONSUMING_ACTIONS) — the relaunch consumes the same
        # shared budget as agent dispatches and fast-path reruns.
        "action": "reclaim_relaunch",
        "source": _FLOW_NAME,
        "dead_instance_id": instance_id,
        "reclaim_detail_type": detail_type,
        "cadence_slug": cadence,
        "pipeline": pipeline,
        "run_date": run_date,
        "execution_arn": source_ev.get("execution_arn", ""),
        "force_on_demand": True,
    }
    # Record FIRST (exactly-one bound), then invoke — both fail-loud.
    _record_reclaim_relaunch(s3, watch_log_key, doc, record)
    _lambda_client().invoke(
        FunctionName=SPOT_DISPATCHER_FUNCTION,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    logger.warning(
        "reclaim check: watch box %s (%s/%s@%s) died mid-repair — relaunch "
        "dispatched with force_on_demand", instance_id, cadence, pipeline, run_date,
    )
    noted = _reclaim_note(
        "\U0001f6f0️ *SF-Watch reclaim checker — bounded relaunch*\n"
        f"Watch box `{instance_id}` ({cadence}/{pipeline}@{run_date}) was "
        "reclaimed mid-repair — relaunched ON-DEMAND (attempt 1/1; a second "
        "death escalates loud, config#2270).",
        dedup_key=f"{_FLOW_NAME}:reclaim_relaunch:{pipeline}:{run_date}:{instance_id}",
        context_info=key_ctx,
    )
    return {**base, "watch_box": True, "completed": False, "relaunched": True,
            "telegram_sent": noted, "watch_log_key": watch_log_key}


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint — plus the mid-run spot-reclaim
    checker branch (config#2270) when invoked by the EC2 reclaim/termination
    EventBridge rules. Read-only on the probe path; raises on an unexpected
    AWS API failure so the check can never silently no-op."""
    if _is_reclaim_event(event or {}):
        return _handle_reclaim_event(event)
    spot_problems, kill_switches = _check_spot_dispatch_leg()
    problems = (
        _check_rule() + _check_state_machines_exist() + _check_lambda_healthy() + spot_problems
    )
    fingerprint = _problem_fingerprint(problems) if problems else None

    # Always surfaced (record + log), never alerted: a deliberate operator
    # disable is state, not an incident.
    logger.info("sf-watch liveness: dispatch kill-switches: %s", kill_switches)

    s3 = _s3_client()
    already = _load_alerted_fingerprint(s3)

    alerted = False
    if problems and fingerprint != already:
        logger.warning("sf-watch liveness: %d NEW problem(s): %s", len(problems), problems)
        alerted = _alert(problems, kill_switches)
        if alerted:
            _save_alerted_fingerprint(s3, fingerprint)
    elif problems:
        logger.info("sf-watch liveness: %d problem(s), unchanged since last alert — suppressed", len(problems))
    else:
        logger.info("sf-watch liveness: all checks clean")
        if already is not None:
            _save_alerted_fingerprint(s3, None)  # clear dedup state now that it's healthy again

    return {
        "problems": problems,
        "alerted": alerted,
        "clean": not problems,
        "kill_switches": kill_switches,
    }
