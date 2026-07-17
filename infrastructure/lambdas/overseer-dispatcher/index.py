"""alpha-engine-overseer-dispatcher — registry-driven router in front of the
fleet's failure-response executor Lambdas (alpha-engine-config-I2823, epic
alpha-engine-config-I2821).

WHY A ROUTER, NOT A MERGE: the three response paths (sf-watch, ci-watch,
groom) are domain policy layers over the already-consolidated
``nousergon_lib.spot_dispatch`` primitives — their per-path semantics
(defer-not-drop vs concurrent-skip, drill isolation, attempt ceilings, pace
gates) are essential, test-pinned complexity, not accidental duplication.
What the fleet lacked was ONE dispatch entry + ONE registry of the response
surface + ONE place that turns a bad executor verdict into a P1 + loud page
(previously duplicated in sf-watch.yml GHA yaml, and absent entirely for
non-GHA callers). This Lambda is that entry; the executors are unchanged.

It also removes GitHub from the SF-failure loop: saturday-sf-watch-dispatcher
M2 (flag ``M2_DISPATCH_TARGET=overseer``) invokes this router Lambda-to-Lambda
instead of round-tripping EventBridge -> repository_dispatch -> GHA ->
lambda invoke — a chain that made SF recovery depend on GitHub availability
(cf. the 2026-07-16 GitHub 503 incident).

REGISTRY: ``playbooks.yaml`` (bundled at deploy from
``infrastructure/overseer/playbooks.yaml``; contract in
``playbooks.schema.json`` + tests/test_overseer_playbook_registry.py). Only
``routed: true`` + ``enabled: true`` playbooks are dispatchable.

EVENT SHAPE (direct invoke, sync or async):
    {"playbook": "sf-watch" | "ci-watch",
     "payload":  {<executor event, passed through with bools stringified>}}

CONTRACT: mirrors the executors' synchronous posture — every anticipated
failure returns a clean JSON verdict, never raises. Callers:
  - sf-watch.yml's ci-watch-dispatch GHA job (sync RequestResponse; branches
    on ``.verdict.launched`` / ``.verdict.reason`` and files a P1 ONLY if the
    router invoke itself fails — verdict-based P1s are owned HERE now).
  - saturday-sf-watch-dispatcher M2 in overseer mode (async Event; relies on
    this router's escalation path entirely).
  - operator/drill manual invokes.

ESCALATION (the consolidation payoff): a non-benign executor verdict — or a
router-level wiring error (unknown playbook, invalid event, executor invoke
failure) — files a P1 issue in alpha-engine-config (PAT from SSM) AND pages
loudly via ``krepis.alerts.publish`` (severity=critical). Both legs are
best-effort with the OTHER leg + the returned verdict + CloudWatch logs as
recording surfaces; on krepis>=0.15.0 the page also lands on the
``nousergon-alerts`` intake bus (phase-1 chokepoint), so the phase-3 drain
sees every escalation as a structured event.

LEDGER: every routed dispatch writes
``s3://alpha-engine-research/overseer/dispatch_ledger/{date}/...json``
(best-effort, secondary — the executor verdict + logs are primary).

Managed OUTSIDE CloudFormation like its sibling dispatchers:
operator-deployed via ``deploy.sh --bootstrap``.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml
from botocore.config import Config

# Executor invokes are SYNCHRONOUS and NON-IDEMPOTENT (they launch spot
# boxes): boto3's default 60s read-timeout + automatic retries caused a
# double-dispatch on the first live alert-drain drill (first invoke launched
# the box, the silent retry hit concurrent_skip). Read-timeout must exceed
# the slowest executor's runtime (300s Lambda timeout on alert-drain), and
# retries MUST be zero — the caller's verdict handling is the retry policy.
_EXECUTOR_INVOKE_CONFIG = Config(
    connect_timeout=10, read_timeout=290, retries={"max_attempts": 0}
)

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: OVERSEER_DISPATCH_ENABLED=false refuses ALL routing without
# touching callers — mirrors every fleet dispatcher's safety valve. Per-
# playbook `enabled: false` in the registry does the same for one playbook.
DISPATCH_ENABLED = os.environ.get("OVERSEER_DISPATCH_ENABLED", "true").lower() == "true"

REGISTRY_PATH = Path(os.environ.get(
    "OVERSEER_REGISTRY_PATH", str(Path(__file__).parent / "playbooks.yaml")
))

# Escalation targets. The PAT param is the same fleet-standard one every
# watch-plane component reads (fine-grained, org-wide issues:write).
ISSUES_REPO = os.environ.get("OVERSEER_ISSUES_REPO", "nousergon/alpha-engine-config")
GH_PAT_SSM = os.environ.get(
    "OVERSEER_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
_ISSUE_TIMEOUT_SEC = int(os.environ.get("OVERSEER_ISSUE_TIMEOUT_SEC", "10"))

LEDGER_BUCKET = os.environ.get("OVERSEER_LEDGER_BUCKET", "alpha-engine-research")
LEDGER_PREFIX = os.environ.get("OVERSEER_LEDGER_PREFIX", "overseer/dispatch_ledger")

_INVOKE_TIMEOUT_NOTE = (
    "executor invoke is synchronous; this Lambda's own timeout must exceed "
    "the slowest executor's (see deploy.sh --timeout 300)"
)


class _RegistryError(RuntimeError):
    """The bundled registry is missing/malformed — a packaging bug."""


_REGISTRY_CACHE: dict | None = None


def _registry() -> dict:
    """Load (once per container) the bundled playbook registry."""
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is None:
        try:
            doc = yaml.safe_load(REGISTRY_PATH.read_text())
        except Exception as exc:  # noqa: BLE001 — converted to _RegistryError, escalated by the handler
            raise _RegistryError(f"cannot read registry {REGISTRY_PATH}: {exc}") from exc
        if not isinstance(doc, dict) or "playbooks" not in doc:
            raise _RegistryError(f"malformed registry {REGISTRY_PATH}: no 'playbooks' key")
        _REGISTRY_CACHE = doc
    return _REGISTRY_CACHE


def _stringify_bools(payload: dict) -> dict:
    """Executors regex-validate string fields ("true"/"false"); JSON callers
    may send real booleans. Normalize top-level bool values only."""
    return {
        k: ("true" if v is True else "false" if v is False else v)
        for k, v in payload.items()
    }


def _invoke_executor(function_name: str, payload: dict) -> dict:
    """Synchronously invoke the executor and parse its JSON verdict."""
    resp = boto3.client(
        "lambda", region_name=REGION, config=_EXECUTOR_INVOKE_CONFIG
    ).invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    body = resp["Payload"].read()
    if resp.get("FunctionError"):
        raise RuntimeError(f"executor {function_name} errored: {body[:500]!r}")
    verdict = json.loads(body)
    if not isinstance(verdict, dict):
        raise RuntimeError(f"executor {function_name} returned non-dict: {body[:200]!r}")
    return verdict


def _file_p1(playbook: str, reason: str, detail: str, payload: dict) -> dict:
    """File the escalation P1 in ISSUES_REPO. Best-effort — the loud page +
    returned verdict + logs are the other recording surfaces."""
    try:
        pat = boto3.client("ssm", region_name=REGION).get_parameter(
            Name=GH_PAT_SSM, WithDecryption=True
        )["Parameter"]["Value"]
        body = "\n".join([
            f"Overseer dispatch for playbook `{playbook}` did not launch and the",
            f"decline reason is not in the playbook's benign list.",
            "",
            f"- **Reason:** `{reason}`",
            f"- **Detail:** {detail or '(none)'}",
            f"- **Payload:** `{json.dumps(payload, default=str)[:1500]}`",
            f"- **Dispatched via:** alpha-engine-overseer-dispatcher "
            f"(alpha-engine-config-I2823)",
            "",
            "Closes-when: the underlying failure this dispatch was covering is",
            "confirmed handled (rerun green / fix merged / consciously declined),",
            "and any router/executor defect this exposed is fixed or filed.",
        ])
        req = urllib.request.Request(
            f"https://api.github.com/repos/{ISSUES_REPO}/issues",
            data=json.dumps({
                "title": f"[P1] overseer: {playbook} dispatch failed ({reason})",
                "body": body,
                "labels": ["P1", "bug", "area:monitoring"],
            }).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "overseer-dispatcher",
            },
        )
        with urllib.request.urlopen(req, timeout=_ISSUE_TIMEOUT_SEC) as resp:
            issue = json.loads(resp.read())
        logger.info("overseer escalation P1 filed: %s", issue.get("html_url"))
        return {"filed": True, "url": issue.get("html_url")}
    except Exception as exc:  # noqa: BLE001 — best-effort leg; recording surfaces: the loud page below, this WARNING, the returned verdict
        logger.warning("overseer escalation P1 filing FAILED: %s: %s",
                       type(exc).__name__, exc)
        return {"filed": False, "error": f"{type(exc).__name__}: {exc}"}


def _page_loud(playbook: str, reason: str, detail: str, p1: dict) -> bool:
    """Loud operator page for a non-benign dispatch outcome. Best-effort —
    recording surfaces: the P1 leg, this WARNING, the returned verdict. On
    krepis>=0.15.0 this also emits a structured event onto the intake bus."""
    try:
        from krepis import alerts

        issue_line = p1.get("url") or f"P1 filing failed: {p1.get('error')}"
        result = alerts.publish(
            f"Overseer dispatch FAILED for playbook '{playbook}': {reason}. "
            f"{detail or ''} Issue: {issue_line}",
            severity="critical",
            source="overseer-dispatcher",
            dedup_key=f"overseer-escalation-{playbook}-{reason}",
        )
        return result.any_ok
    except Exception as exc:  # noqa: BLE001 — best-effort leg; recording surfaces: the P1 leg, this WARNING, the returned verdict
        logger.warning("overseer loud page FAILED: %s: %s", type(exc).__name__, exc)
        return False


def _escalate(playbook: str, reason: str, detail: str, payload: dict) -> dict:
    p1 = _file_p1(playbook, reason, detail, payload)
    paged = _page_loud(playbook, reason, detail, p1)
    if not p1.get("filed") and not paged:
        # Both escalation legs down: the ONLY remaining surfaces are this
        # ERROR log (-> watch-plane CW alarm on Lambda errors won't fire for
        # a logged error, but the alarm on THIS message pattern is the
        # backstop's job) and the returned verdict. Log at ERROR with a
        # stable marker so it is grep-able and alarm-able.
        logger.error(
            "OVERSEER_ESCALATION_DELIVERY_FAILED playbook=%s reason=%s — both "
            "P1 filing and the loud page failed; verdict is the only surface",
            playbook, reason,
        )
    return {"p1": p1, "paged": paged}


def _write_ledger(record: dict) -> str | None:
    """Best-effort dispatch-ledger write. Secondary surface — a ledger outage
    must never affect the dispatch; recording surface: this WARNING + logs."""
    now = datetime.now(timezone.utc)
    key = (
        f"{LEDGER_PREFIX}/{now:%Y-%m-%d}/"
        f"{now:%H%M%S}-{record.get('playbook', 'unknown')}-{uuid.uuid4().hex[:8]}.json"
    )
    try:
        boto3.client("s3", region_name=REGION).put_object(
            Bucket=LEDGER_BUCKET,
            Key=key,
            Body=json.dumps(record, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        return key
    except Exception as exc:  # noqa: BLE001 — secondary surface, see docstring
        logger.warning("overseer ledger write failed (non-fatal): %s", exc)
        return None


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Route a dispatch request to its playbook's executor. Clean-JSON
    contract: anticipated failures return ``routed: false`` verdicts (and
    escalate where the failure means lost coverage), never raise."""
    event = event or {}
    playbook_name = str(event.get("playbook") or "").strip()
    payload = event.get("payload")

    if not DISPATCH_ENABLED:
        logger.warning("OVERSEER_DISPATCH_ENABLED=false — dispatch refused")
        return {"routed": False, "reason": "disabled", "playbook": playbook_name}

    if not playbook_name or not isinstance(payload, dict):
        detail = f"playbook={playbook_name!r} payload_type={type(payload).__name__}"
        logger.error("invalid overseer event: %s", detail)
        # A malformed dispatch request means a caller wiring bug AND (for
        # async callers) a real failure losing its coverage — escalate.
        escalation = _escalate(playbook_name or "unknown", "invalid_event", detail, event)
        return {"routed": False, "reason": "invalid_event", "error": detail,
                "escalation": escalation}

    try:
        registry = _registry()
    except _RegistryError as exc:
        logger.error("overseer registry unusable: %s", exc)
        escalation = _escalate(playbook_name, "registry_error", str(exc), payload)
        return {"routed": False, "reason": "registry_error", "error": str(exc),
                "escalation": escalation}

    spec = (registry.get("playbooks") or {}).get(playbook_name)
    if spec is None:
        detail = f"playbook {playbook_name!r} not in registry"
        logger.error("overseer: %s", detail)
        escalation = _escalate(playbook_name, "unknown_playbook", detail, payload)
        return {"routed": False, "reason": "unknown_playbook", "error": detail,
                "escalation": escalation}
    if not spec.get("routed", False):
        detail = f"playbook {playbook_name!r} is inventory-only (routed: false)"
        logger.error("overseer: %s", detail)
        escalation = _escalate(playbook_name, "not_routable", detail, payload)
        return {"routed": False, "reason": "not_routable", "error": detail,
                "escalation": escalation}
    if not spec.get("enabled", False):
        # A deliberately-disabled playbook is an operator decision, not a
        # wiring bug — clean decline, no escalation (mirrors the executors'
        # own kill-switch verdicts).
        logger.warning("overseer: playbook %s disabled in registry", playbook_name)
        return {"routed": False, "reason": "playbook_disabled", "playbook": playbook_name}

    function_name = spec["executor_function"]
    executor_payload = _stringify_bools(payload)
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        verdict = _invoke_executor(function_name, executor_payload)
    except Exception as exc:  # noqa: BLE001 — clean-JSON contract; escalated (lost coverage)
        detail = f"{type(exc).__name__}: {exc}"
        logger.error("overseer executor invoke FAILED for %s: %s", function_name, detail)
        escalation = _escalate(playbook_name, "executor_invoke_failed", detail, payload)
        result = {"routed": False, "reason": "executor_invoke_failed",
                  "playbook": playbook_name, "executor_function": function_name,
                  "error": detail, "escalation": escalation}
        result["ledger_key"] = _write_ledger(
            {"playbook": playbook_name, "started_at": started_at,
             "payload": executor_payload, "outcome": result}
        )
        return result

    launched = verdict.get("launched") is True
    reason = str(verdict.get("reason") or "")
    benign = reason in set(spec.get("benign_reasons") or [])
    escalation = None
    if not launched and not benign:
        escalation = _escalate(
            playbook_name, reason or "unknown_decline",
            f"executor verdict: {json.dumps(verdict, default=str)[:500]}", payload,
        )

    result = {
        "routed": True,
        "playbook": playbook_name,
        "executor_function": function_name,
        "verdict": verdict,
        "benign": (not launched) and benign,
        "escalation": escalation,
    }
    result["ledger_key"] = _write_ledger(
        {"playbook": playbook_name, "started_at": started_at,
         "payload": executor_payload, "outcome": {k: result[k] for k in
                                                  ("routed", "verdict", "benign", "escalation")}}
    )
    logger.info(
        "overseer routed: playbook=%s executor=%s launched=%s reason=%s benign=%s escalated=%s",
        playbook_name, function_name, launched, reason, benign, escalation is not None,
    )
    return result
