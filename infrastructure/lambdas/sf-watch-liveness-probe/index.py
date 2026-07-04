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

Silent-unless-broken (mirrors the groom probe and Fleet-SF Watch's own
failure-driven design): a clean check logs and returns, no Telegram noise. Any
problem fires a LOUD alert, deduplicated by the CONTENT of the problem set
(a hash), not a timestamp — so a standing issue doesn't re-ping every run, and
the alert state clears automatically the moment the check is clean again.

**Fail-loud (CLAUDE.md no-silent-fails).** Every AWS describe/list call here is
the PRIMARY input: an UNEXPECTED API error (anything other than the specific
"this resource doesn't exist" codes we're explicitly checking for) RAISES, so a
broken probe surfaces via the Lambda error metric + CW alarm rather than
silently skipping the one check that verifies nothing else is silently broken.
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
    "alpha-engine-eod-pipeline",  # transitional alias, config#1408 / re-exam 2026-07-03
    "alpha-engine-groom-dispatch",  # config#1472
]

WATCH_BUCKET = os.environ.get("WATCH_BUCKET", "alpha-engine-research")
STATE_KEY = os.environ.get("SF_WATCH_LIVENESS_STATE_KEY", "consolidated/sf_watch_liveness/alerted.json")


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


def _alert(problems: list[str]) -> bool:
    lines = [
        "\U0001f6f0️ *Fleet-SF Watch Liveness Probe — WIRING PROBLEM*",
        f"{len(problems)} issue(s) found with the Fleet-SF Watch trigger itself "
        "(NOT a pipeline failure — the WATCHER's own wiring):",
    ]
    for p in problems:
        lines.append(f"• {p}")
    lines.append(
        "_Fleet-SF Watch may not catch a real pipeline failure right now. "
        "Check the EventBridge rule + saturday-sf-watch-dispatcher Lambda._"
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
            context={"problems": len(problems)},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface; finding still returned
        logger.warning("sf-watch liveness alert Telegram send failed (non-fatal): %s", exc)
        return False


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Scheduled (EventBridge) entrypoint. Read-only; raises on an unexpected
    AWS API failure so the check can never silently no-op."""
    problems = _check_rule() + _check_state_machines_exist() + _check_lambda_healthy()
    fingerprint = _problem_fingerprint(problems) if problems else None

    s3 = _s3_client()
    already = _load_alerted_fingerprint(s3)

    alerted = False
    if problems and fingerprint != already:
        logger.warning("sf-watch liveness: %d NEW problem(s): %s", len(problems), problems)
        alerted = _alert(problems)
        if alerted:
            _save_alerted_fingerprint(s3, fingerprint)
    elif problems:
        logger.info("sf-watch liveness: %d problem(s), unchanged since last alert — suppressed", len(problems))
    else:
        logger.info("sf-watch liveness: all checks clean")
        if already is not None:
            _save_alerted_fingerprint(s3, None)  # clear dedup state now that it's healthy again

    return {"problems": problems, "alerted": alerted, "clean": not problems}
