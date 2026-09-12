"""alpha-engine-freshness-monitor — absence-driven S3 artifact monitor.

Phase 3 of the artifact-freshness-monitor arc (plan doc at
``~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md``).
Closes the silent absence-of-artifact bug class — the 2026-05-17→27
``pit_parity.json`` incident (load-bearing artifact silently absent
for 11 days), the 2026-05-18 factor-profiles orphan, and the
2026-05-23 missing-``signals.json`` incident are the sibling triggers.
SF Catch / flow-doctor / substrate-health-check are all *event-driven*
(failure → alert); this Lambda is the *absence-driven* complement
(silence → alert).

**Architecture.** EventBridge fires this Lambda every 15min. Per
invocation:

  1. Load the registry from S3
     (``s3://{REGISTRY_BUCKET}/{REGISTRY_KEY}``, YAML).
  2. Walk every spec. For each, call
     :func:`nousergon_lib.artifact_freshness.check_freshness`
     against the current ``now`` (UTC).
  3. Aggregate results into a single ``check_results.json`` artifact
     under ``_freshness_monitor/`` (the dashboard surface reads this).
  4. Emit a self-heartbeat at ``_freshness_monitor/heartbeat.json``
     — the monitor monitors itself; substrate-health-check daily
     watches the heartbeat.
  5. For misses past SLA (``state ∈ {missing, stale, probe_failed}``),
     the sweep composes ONE grouped digest page (config-I7713,
     :func:`_publish_digest`) covering every alerting row, routed via SNS
     (:func:`krepis.alerts.publish`, ``telegram=False``) and Telegram via
     flow-doctor forum topics (config#1742 T2 / config#1747). The
     digest's ``dedup_key`` (:func:`_digest_dedup_key`) is a fingerprint of
     the SET of currently-open per-artifact EPISODES (an episode is the
     unbroken span one artifact stays in one alerting state; see
     :func:`_episode_signature`), never of ``now``'s calendar date — a standing, unchanged episode set
     dedups across UTC-midnight rollovers instead of re-paging every day
     the condition simply continues. **``severity=warning`` registry rows
     are console-only** (written to
     ``check_results.json``; no SNS/Telegram — see ARTIFACT_REGISTRY
     "dashboard-only" convention) — with two config-I3086 exceptions:
     a row listing the live champion arm in ``critical_while_champion_arm``
     is coerced to critical at probe time, and a warning row
     confirmed-missing for ``WARNING_ESCALATION_RUNS`` consecutive sweeps
     escalates to the critical page path.
  6. **Owning-item join (observability-policy §7.4a / I7326)**: before
     emitting, resolve the ``artifact_id`` against the OPEN issues of
     ``ISSUES_REPO`` — the union of the self-filed ``issue_filed_url``
     marker and a tracker search that also sees items a HUMAN filed. The
     page names the item that owns the cause, its age and its priority,
     and lists overlapping items as members. Where an item is open,
     warning→critical escalation is driven by that item's AGE against its
     priority SLA rather than the miss count; where none is open, crossing
     the miss ladder CREATES the item as the escalation's first action.
     Per alert class, ``_freshness_monitor/execution_loop.json`` and the
     ``AlertPagesWithOpenOwningItemFraction`` /
     ``OwningItemAgeDaysAtPageMedian`` CW metrics record whether a class is
     paging about a component or about a backlog that is not being drained
     — emitted EVERY run, zeros included. None of this suppresses anything:
     no cooldown or grace window is widened by it, and a row whose page is
     not delivered still carries its true state and its full owning-item
     block into ``check_results.json``.
  7. **OBSERVE-mode gate**: when env
     ``FRESHNESS_MONITOR_ENABLED`` is anything other than
     ``"true"`` (case-insensitive), alerts are suppressed but the
     check results and heartbeat are still emitted. Phase 6 cutover
     flips the env var via ``aws lambda update-function-configuration``
     without redeploying — mirrors the mnemon 0.7.0rc4 pattern.

**Never raises.** Lambda failures cannot be silent (this monitor IS
the silent-failure trap). The handler catches per-spec exceptions
and records them in ``check_results.json`` so a bad registry entry
doesn't take down the whole probe pass. The handler's own outer
exception path is a CW Logs-level surface.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import replace as dc_replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import yaml

from krepis.alerts import publish
from nousergon_lib.artifact_freshness import (
    ArtifactSpec,
    CheckResult,
    check_freshness,
    cycle_completion,
    has_wildcard_segment,
    key_pattern,
    key_suffix,
    listable_prefix,
    resolve_current_cycle,
)
from nousergon_lib.trading_calendar import last_closed_trading_day, previous_trading_day
from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_FLOW_NAME = "freshness-monitor"
_DB_BASENAME = "flow_doctor_freshness_monitor"
_FRESHNESS_TELEGRAM_TOPICS = (
    FleetTelegramTopic.CRITICAL,
    FleetTelegramTopic.OPS_HEALTH,
)

# ── Configuration (env-driven so Phase 6 cutover is a single CLI flip) ──────

REGISTRY_BUCKET = os.environ.get("REGISTRY_BUCKET", "alpha-engine-research")
REGISTRY_KEY = os.environ.get(
    "REGISTRY_KEY", "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
)
HEARTBEAT_KEY = "_freshness_monitor/heartbeat.json"
CHECK_RESULTS_KEY = "_freshness_monitor/check_results.json"
HISTORY_KEY = "_freshness_monitor/history.json"
CYCLE_VERDICT_KEY = "_freshness_monitor/cycle_verdict.json"
# I7326 / observability-policy §7.4a clause (c) — the durable surface for
# "is this class of page evidence about the component, or about a backlog
# that is not being drained?". Written EVERY run, including the healthy run
# where every number is zero: a metric that only appears when something is
# wrong is indistinguishable from a dead emitter.
EXECUTION_LOOP_KEY = "_freshness_monitor/execution_loop.json"

# config#1297 — the general sweep moved from a 15-min cron to daily (Brian's
# 2026-06-27 directive: the 15-min sweep was unnecessary noise once the
# saturday_sf/run_calendar staleness models were fixed). These two artifacts
# stay on a 30-min weekday-market-hours mini-rule (separate EB cron, event={
# "mode": "intraday"}) so genuinely intraday monitoring isn't blinded by the
# daily cadence: `open_orders_latest` (market-hours order-book freshness) and
# `freshness_monitor_heartbeat` (the monitor's OWN dead-man's-switch artifact
# — its whole purpose is fast detection of a monitor outage, which a daily
# cadence would defeat).
INTRADAY_ARTIFACT_IDS = frozenset({"open_orders_latest", "freshness_monitor_heartbeat"})

# config#1240 — auto-remediation dispatch. The confirmed-miss path reads the
# per-artifact `recovery:` spec from the registry and DISPATCHES the named
# backfill primitive (SF start_execution / Lambda invoke) instead of (or, per
# the spec's mode, in addition to) only paging. The monitor is a pure
# reconciler — it never hardcodes per-artifact logic; it reads the declarative
# spec and drives to desired state.
#
# DEDUP. A still-missing artifact would otherwise re-dispatch on every 15-min
# poll until the heal lands and the next probe sees it fresh. We write an
# in-progress marker to S3 keyed by the per-cycle label
# (:func:`resolve_current_cycle`, via :func:`_recovery_marker_key`) — a
# DELIBERATELY different, cycle-scoped key from the alert page's episode key
# (:func:`_episode_signature`): this marker exists to rate-limit repeated
# backfill DISPATCH attempts within one cycle, not to dedup the operator
# PAGE across an unbroken episode. The marker's presence within a cooldown
# window suppresses re-dispatch for that (artifact, cycle-window). The
# marker lives
# under the already-granted `_freshness_monitor/` prefix (iam-policy.json
# S3WriteMonitorArtifacts) so no new write grant is required.
RECOVERY_MARKER_PREFIX = "_freshness_monitor/_recovery/"

# Cooldown (minutes) after a dispatch during which the same (artifact,
# cycle-window) is NOT re-dispatched. Sized longer than the 15-min poll so at
# least one full backfill attempt completes before a retry is considered, but
# short enough that a genuinely-failed heal is retried within the cycle. Env-
# tunable to mirror the OBSERVE-mode cutover-by-CLI-flip pattern.
RECOVERY_COOLDOWN_MINUTES = int(
    os.environ.get("RECOVERY_COOLDOWN_MINUTES", "120")
)

# Master gate for the dispatch side effect, independent of ALERTS_ENABLED so
# the cutover can stage: alerts can go live while dispatch stays in OBSERVE
# (log the would-dispatch, write no marker, call no AWS) until validated. Phase
# flips this via `aws lambda update-function-configuration` with no redeploy.
RECOVERY_DISPATCH_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_RECOVERY_ENABLED", "false").lower()
    == "true"
)

# config-I3282 — freshness-critical → overseer drain dispatch (phase 1;
# Brian directive 2026-07-22). Every CRITICAL page from a row whose declared
# response lane is NOT `remediation: operator` (and that has no `recovery:`
# heal of its own in flight) triggers ONE event-time alert-drain run through
# the overseer router, instead of the critical sitting in the intake queue
# until the next scheduled drain (10:00/22:00 UTC — today's four criticals
# waited ~10h). The drain agent consumes the whole queue, so a sweep that
# pages several criticals dispatches ONCE, and the router's alert-drain
# playbook + the executor's EC2 tag lock (concurrent_skip benign) guard
# overlap. Async Event invoke — the router owns escalation on executor
# failure (mirrors saturday-sf-watch-dispatcher's M2 posture).
#
# Rows with a `recovery:` block are EXCLUDED: their declared lane is the
# auto-backfill heal, and a drain on top of an in-flight heal is redundant.
# Rows with no declaration at all (only reachable via warning-escalation or
# probe_failed coercion — the PR-time completeness check requires a lane on
# every statically-critical row) DEFAULT to dispatch: a critical page nobody
# declared a response for is exactly the case that must not sit unactioned.
DRAIN_DISPATCH_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_DRAIN_DISPATCH_ENABLED", "false").lower()
    == "true"
)
DRAIN_DISPATCH_MARKER_KEY = (
    "_freshness_monitor/_dispatch/last_drain_dispatch.json"
)
# One drain covers every critical in the queue at launch; the cooldown only
# bounds how often a PERSISTENTLY-critical sweep can relaunch one. Sized to
# the drain's own runtime ceiling (3h watchdog) would starve genuinely new
# incidents; 120min matches the recovery cooldown's retry philosophy.
DRAIN_DISPATCH_COOLDOWN_MINUTES = int(
    os.environ.get("DRAIN_DISPATCH_COOLDOWN_MINUTES", "120")
)
OVERSEER_DISPATCHER_FUNCTION = os.environ.get(
    "OVERSEER_DISPATCHER_FUNCTION", "alpha-engine-overseer-dispatcher"
)
DRAIN_PLAYBOOK = os.environ.get("FRESHNESS_DRAIN_PLAYBOOK", "alert-drain")

# CloudWatch namespace for the per-cycle completion rollup metric. Shares
# the substrate-health namespace so cycle-completion + substrate-row health
# graph together. Dimensioned by Cadence only (low-cardinality, alarm-able).
CW_NAMESPACE = "AlphaEngine/Substrate"

# Historical-mode lookback depth per cadence. Tunable via event payload
# (event["lookback"] = {"saturday_sf": 12, ...}). Defaults sized for ~3
# months of history at ~negligible cost: 51 artifacts × 50 cycles ≈
# 2,500 S3 HEAD requests per daily historical run ≈ $0.001/day.
_DEFAULT_LOOKBACK = {
    "saturday_sf": 12,
    "weekday_sf": 30,
    "eod_sf": 30,
    "continuous": 0,  # current-state probe covers continuous artifacts
}

# OBSERVE-mode gate. Plan §3 invariant 10 + §4 Phase 6 default. Anything
# other than literal "true" (case-insensitive) suppresses alerts. Check
# results + heartbeat are emitted regardless.
ALERTS_ENABLED = (
    os.environ.get("FRESHNESS_MONITOR_ENABLED", "false").lower() == "true"
)

# config-I3086 — dynamic severity + warning escalation. Two post-detection
# gaps surfaced by the 2026-07-20 stale-champion-feed incident (config-I3053):
# a row's declared severity is static while "hard-blocks downstream" is a
# dynamic property of the promoted champion arm, and a severity=warning miss
# is console-only forever no matter how long it persists.
#
# 1. Rows may declare `critical_while_champion_arm: [<arm>, ...]` — effective
#    severity is coerced to critical at probe time while the live champion
#    pointer (config/producer_champion.json, schema_version-1 field
#    `champion` — the same key crucible-executor's champion.py
#    load_champion_pointer reads) names a listed arm. A
#    pointer read failure coerces listed rows to critical too: fail toward
#    paging, never toward silence.
# 2. A severity=warning row confirmed-missing for WARNING_ESCALATION_RUNS
#    consecutive evaluated sweeps escalates to the critical page path. The
#    counter is carried in check_results.json (`consecutive_miss_runs`), so
#    no new state surface is introduced.
CHAMPION_POINTER_KEY = os.environ.get(
    "CHAMPION_POINTER_KEY", "config/producer_champion.json"
)
WARNING_ESCALATION_RUNS = int(os.environ.get("WARNING_ESCALATION_RUNS", "3"))

# config#2055 Gap 2 — key-deliverable extended-staleness escalation into the
# Decision Queue (Brian's 2026-07-21 Option-A ruling: the Lambda files the
# issue directly, mirroring overseer-dispatcher's `_file_p1`). A row opts in
# via the registry's `escalate_to_issue: true` flag (parsed as a parallel
# map, same pattern as `critical_while_champion_arm` — not a schema field on
# the frozen lib `ArtifactSpec`). For an `event_driven` row (whose own
# freshness check ALWAYS short-circuits to fresh — see `check_freshness`'s
# event-driven short-circuit — so its own `consecutive_miss_runs` is always
# 0), the threshold is evaluated against its `liveness_via` ANCHOR's
# miss-streak instead; see `_escalate_stale_key_deliverables`.
#
# I7326 / observability-policy §7.4a clause (b). This used to be a SEPARATE,
# much longer rung (`ISSUE_ESCALATION_RUNS = 14`) ABOVE the critical-page
# threshold — "nobody acted on the page either, so now file". That ordering
# IS the defect the clause names: severity is raised first and the owning
# item created second, so the operator is paged repeatedly about a condition
# that is not yet tracked anywhere. On 2026-08-14 `director_retro` sat at
# miss 13 — one short of 14 — so no auto-filed item existed when the CRITICAL
# fired. Creating the owning item is now the FIRST action of the escalation:
# the threshold collapses onto the point the row reaches the critical page
# path (`_escalation_threshold`), and above that point the ladder is the
# owning item's age, not the miss count (`_maybe_alert`).


def _escalation_threshold(spec: ArtifactSpec) -> int:
    """Consecutive confirmed-miss sweeps at which the owning item is
    created — i.e. the sweep on which this row first reaches the critical
    page path. A `warning` row reaches it via the WARNING_ESCALATION_RUNS
    promotion; a `critical` row pages on its first confirmed miss."""
    if spec.severity == "warning" and WARNING_ESCALATION_RUNS > 0:
        return WARNING_ESCALATION_RUNS
    return 1


ISSUES_REPO = os.environ.get("ISSUES_REPO", "nousergon/alpha-engine-config")
# Same SSM param overseer-dispatcher already reads (IAM-reuse convention) —
# no new secret, just a new grant on the existing parameter.
GH_PAT_SSM = os.environ.get(
    "GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
_ISSUE_TIMEOUT_SEC = int(os.environ.get("ISSUE_ESCALATION_TIMEOUT_SEC", "10"))

# ArtifactSpec field set — used to strip extra YAML keys (e.g., the
# top-level `defaults` shape carries `s3_bucket` which we want, but a
# future schema extension would otherwise pollute the constructor).
_SPEC_FIELDS = frozenset(
    {
        "artifact_id",
        "s3_bucket",
        "s3_key_template",
        "cadence",
        "sla_minutes_after_cron",
        "severity",
        "owner_repo",
        "created_at",
        "grace_period_cycles",
        "recovery_key_template",
        "calendar_aware",
        "interval_minutes",
        # Continuous run-calendar (nousergon-lib >= v0.73.0) — the single
        # source of truth for a continuous artifact's calendar-awareness
        # (trading_days / all_days / market_hours). Drives both the idle
        # short-circuit and a trading-day-aware freshness floor.
        "run_calendar",
        # Continuous active-window bound: active_hours_utc is the
        # market_hours session window (nousergon-lib >= v0.63.0). The
        # deprecated active_trading_days_only boolean (subsumed by
        # run_calendar) was removed in nousergon-lib v0.102.0 / config#1334;
        # unknown keys in the registry are stripped by the loader below, so
        # this is forward-safe regardless of the pinned lib version.
        "active_hours_utc",
        "produces",
        "depends_on",
        "liveness_via",
    }
)


# ── Registry loader ─────────────────────────────────────────────────────────


def _coerce_date(value: Any) -> date:
    """YAML ``safe_load`` already returns ``datetime.date`` for ISO date
    scalars; this is a defensive coercion for string inputs (e.g.,
    when the registry is hand-built in a test fixture)."""
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"created_at must be date or ISO-string, got {type(value).__name__}")


def load_registry(s3_client: Any, bucket: str, key: str) -> list[ArtifactSpec]:
    """Fetch the registry from S3 and parse into :class:`ArtifactSpec`
    instances. The YAML ``defaults`` block is merged into every entry
    (per-entry keys override defaults).

    Raises on YAML parse error or schema violation — the Lambda's outer
    handler logs + re-raises so the failure surfaces in CW Logs +
    Lambda error metrics (which Brian's existing CW alarms cover).
    The registry's PR-time validator (alpha-engine-config
    ``scripts/validate_artifact_registry.py``) is the prevent-it-at-PR
    chokepoint; this is the runtime defense.
    """
    return load_registry_with_recovery(s3_client, bucket, key)[0]


def load_registry_with_recovery(
    s3_client: Any, bucket: str, key: str
) -> tuple[list[ArtifactSpec], dict[str, dict], dict[str, list[str]],
           dict[str, bool], dict[str, str], dict[str, str],
           dict[str, Any]]:
    """Like :func:`load_registry`, but also returns the per-artifact
    ``recovery:`` spec map (config#1240), the
    ``critical_while_champion_arm`` map (config-I3086), the
    ``escalate_to_issue`` map (config#2055 Gap 2), the
    ``remediation:`` declared-response-lane map (config-I3282), the
    ``producer_trigger`` map (config-I6570) — all keyed by ``artifact_id`` —
    and the ``declared_off`` input (config-I8719): the well-formed
    ``declared_off:`` blocks plus the publisher's ``declared_off_resolution``
    block, which :func:`resolve_declared_off` turns into a suppression map
    once ``now`` is known.

    ``ArtifactSpec`` is a frozen lib dataclass without a ``recovery``
    field (the monitor's dispatch concern is not the substrate's
    freshness concern), so the recovery block is parsed into a parallel
    map rather than threaded onto the spec; the champion-arm and
    escalate-to-issue blocks follow the same pattern. Artifacts without a
    block are simply absent from the respective map — the dispatch path
    treats a missing key as "no auto-remediation, page only"; the
    severity path treats it as "static severity only"; the escalation
    path treats it as "console/page-only, never files an issue".
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    data = yaml.safe_load(body)
    if not isinstance(data, dict) or not data.get("artifacts"):
        raise ValueError(f"registry at s3://{bucket}/{key} missing 'artifacts'")

    defaults = data.get("defaults", {}) or {}
    specs: list[ArtifactSpec] = []
    recovery_by_id: dict[str, dict] = {}
    critical_arms_by_id: dict[str, list[str]] = {}
    escalate_to_issue_by_id: dict[str, bool] = {}
    remediation_by_id: dict[str, str] = {}
    producer_trigger_by_id: dict[str, str] = {}
    for entry in data["artifacts"]:
        merged = {**defaults, **entry}
        merged["created_at"] = _coerce_date(merged["created_at"])
        # Strip any extension fields (forward-compat with future schema).
        spec_kwargs = {k: v for k, v in merged.items() if k in _SPEC_FIELDS}
        spec = ArtifactSpec(**spec_kwargs)
        specs.append(spec)
        recovery = merged.get("recovery")
        if isinstance(recovery, dict):
            recovery_by_id[spec.artifact_id] = recovery
        arms = merged.get("critical_while_champion_arm")
        if isinstance(arms, list) and arms:
            critical_arms_by_id[spec.artifact_id] = [str(a) for a in arms]
        if merged.get("escalate_to_issue") is True:
            escalate_to_issue_by_id[spec.artifact_id] = True
        remediation = merged.get("remediation")
        if isinstance(remediation, str) and remediation:
            remediation_by_id[spec.artifact_id] = remediation
        # config-I6570 — only a well-formed trigger is carried. A malformed
        # value is dropped here rather than raising: the field's whole job is
        # to REMOVE a page, so a typo must degrade to today's behaviour, not
        # to a registry that fails to load. The PR-time validator in
        # alpha-engine-config is the chokepoint that catches the typo.
        producer_trigger = merged.get("producer_trigger")
        if producer_trigger is not None:
            # config-I7509: a row may name SEVERAL producers. The groom store
            # is written by whichever of four schedules fires; suppressing on
            # one of them being off would silence a row three live producers
            # are still expected to write. A list suppresses only when EVERY
            # named trigger is off — see apply_producer_suppression.
            declared = (
                list(producer_trigger)
                if isinstance(producer_trigger, list)
                else [producer_trigger]
            )
            parsed = [t for t in declared if parse_producer_trigger(t) is not None]
            if parsed and len(parsed) == len(declared):
                producer_trigger_by_id[spec.artifact_id] = tuple(parsed)
            else:
                logger.warning(
                    "registry row %s carries a malformed producer_trigger %r — "
                    "expected '<events|scheduler|gha>:<name>'; row keeps "
                    "alerting. A partially-valid list is dropped whole: "
                    "suppressing on the subset that parsed would silence the "
                    "row on fewer producers than it declares",
                    spec.artifact_id, producer_trigger,
                )
    # config-I8719 — the declared-off input. Both halves come from THIS file:
    # the per-row declarations and the publisher's resolution of their clearing
    # milestones, so a declaration can never be read against a resolution from
    # a different publish.
    declared_off_input = {
        "rows": parse_declared_off(data),
        "resolution": data.get("declared_off_resolution"),
    }
    return (specs, recovery_by_id, critical_arms_by_id,
            escalate_to_issue_by_id, remediation_by_id, producer_trigger_by_id,
            declared_off_input)


# ── Dynamic severity (config-I3086) ─────────────────────────────────────────


def _load_champion_arm(s3_client: Any) -> tuple[str | None, bool]:
    """Read the live champion pointer. Returns ``(arm, read_failed)``.

    ``read_failed=True`` on any read/parse problem — the caller coerces
    listed rows to critical in that case (fail toward paging, never
    toward silence).
    """
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHAMPION_POINTER_KEY)
        pointer = json.loads(obj["Body"].read())
        # schema_version-1 pointer key is `champion` (verified against the
        # live object AND crucible-executor champion.py's own read —
        # pointer["champion"]). The original I3086 patch read a
        # `champion_arm` key that never existed in the pointer schema.
        arm = pointer.get("champion")
        if isinstance(arm, str) and arm:
            return arm, False
        logger.warning(
            "champion pointer at %s has no usable `champion` field: %r",
            CHAMPION_POINTER_KEY, pointer,
        )
        return None, True
    except Exception as exc:  # noqa: BLE001 — read failure must not sink the pass
        logger.warning("champion pointer read failed (config-I3086): %s", exc)
        return None, True


def apply_dynamic_severity(
    s3_client: Any,
    specs: list[ArtifactSpec],
    critical_arms_by_id: dict[str, list[str]],
) -> tuple[list[ArtifactSpec], set[str]]:
    """Coerce effective severity to ``critical`` for rows whose
    ``critical_while_champion_arm`` names the live champion arm
    (config-I3086). Returns ``(specs, coerced_ids)``.

    Root incident: ``research_free_backfill`` was correctly
    ``severity=warning`` at registration (observational backfill); the
    2026-07-13 champion promotion silently made it a hard-block live
    trade feed and nothing re-derived severity — its confirmed miss
    stayed console-only until the weekday order pipeline hard-failed
    (config-I3053).
    """
    if not critical_arms_by_id:
        return specs, set()
    arm, read_failed = _load_champion_arm(s3_client)
    out: list[ArtifactSpec] = []
    coerced: set[str] = set()
    for spec in specs:
        arms = critical_arms_by_id.get(spec.artifact_id)
        if arms and spec.severity != "critical" and (read_failed or arm in arms):
            logger.info(
                "dynamic severity (config-I3086): %s %s→critical "
                "(champion_arm=%s%s)",
                spec.artifact_id, spec.severity, arm,
                "; pointer unreadable — fail-loud coercion" if read_failed else "",
            )
            out.append(dc_replace(spec, severity="critical"))
            coerced.add(spec.artifact_id)
        else:
            out.append(spec)
    return out, coerced


# ── Producer-trigger suppression (alpha-engine-config-I6570) ────────────────
#
# A registry row's miss means "this artifact is late". It does NOT distinguish
# "the producer ran and failed" from "the producer was deliberately switched
# off" — and after the 2026-08-07 automation-pause ruling (config-I6617)
# disabled 40 triggers, most of the registry is in the second state. Paging on
# a deliberately-off producer trains the operator to ignore the monitor, which
# is the same outcome as not having one.
#
# The authority is LIVE AWS, never the pause manifest. `automation_pause.json`
# is the record of the ruling; a rule can also be off for reasons that ruling
# never covered, and the manifest can drift from reality in both directions.
# Asking EventBridge whether the producing rule is ENABLED answers the actual
# question and needs no second source to stay in sync.
#
# THREE properties this must not lose:
#   1. It fails toward PAGING. Any error resolving a trigger's state — denied,
#      throttled, renamed, absent — leaves the row alerting exactly as today.
#      A suppression path that fails open is a monitor that can be silenced by
#      an IAM regression.
#   2. It is never silent. A suppressed row still lands in check_results.json
#      with its true state and `alert_suppressed: true` + the trigger name, so
#      the console renders "stale — producer disabled", never green.
#      principles.md §2.7: no data is never rendered as green, and neither is
#      deliberately-off.
#   3. It EXPIRES. A pause that becomes permanent must not become a permanent
#      blindfold. First-observed-disabled is persisted, and past
#      PRODUCER_SUPPRESSION_MAX_DAYS the suppression lapses and the row pages
#      again — the page then reads as "this has been off for N days", which is
#      the decision the operator actually owes.
PRODUCER_SUPPRESSION_MAX_DAYS = int(
    os.environ.get("PRODUCER_SUPPRESSION_MAX_DAYS", "14")
)
PRODUCER_DISABLED_SINCE_KEY = "_freshness_monitor/producer_disabled_since.json"
# alpha-engine-config-I6817 D4. The suppression path above answers "is THIS
# artifact's producer off?" — it can only see triggers some registry row names.
# The class defect is wider: a schedule switched off under a pause has no
# release path, and NOTHING in the fleet enumerates what is currently off. The
# 2026-08-07 pause disabled 23 rules; `alpha-research-thinktank-daily` stayed
# off for three days after the condition that justified it was resolved,
# because its tracking issue was CLOSED and no surface listed the rule.
#
# So this key is the inventory, written every daily sweep: every disabled
# trigger in the account, whether or not any artifact row references it, with
# how long it has been off. It is deliberately DATA and not an alert — deciding
# whether a given pause has a release item needs the backlog, which this Lambda
# has no credential for and should not. The join is a separate sweep's job.
DISABLED_PRODUCER_INVENTORY_KEY = "_freshness_monitor/disabled_producers.json"

# alpha-engine-config-I7509. AWS is not the only place a producer is switched
# off. Brian's 2026-08-12 groomer/Overseer deactivation (config-I6984) disabled
# TEN GitHub Actions workflows alongside three EventBridge rules; the AWS half
# suppressed correctly and the GitHub half paged for five days, because
# `gha` was not an expressible producer surface. `groom_flow_metrics` and
# `pr_resting_state_trend` escalated warning→critical off miss_count and reached
# Telegram as CRITICAL — the exact "trains the operator to ignore the monitor"
# outcome the block above exists to prevent, arriving through the one surface it
# could not see.
#
# This Lambda holds no GitHub credential and must not acquire one — its blast
# radius is already the fleet's alerting (same argument as
# `disabled_producer_latch_sweep.py`, which is why the backlog join lives in a
# sweep and not here). So the live GitHub state arrives as DATA: the latch sweep
# already runs daily with a `ne-groomer` App token AND `saturday-sf-watch-role`,
# and writes every enrolled repo's workflow states to this key.
#
# The three properties above are preserved through the indirection:
#   1. FAILS TOWARD PAGING — an absent, unreadable, malformed or STALE
#      inventory resolves no trigger, so every gha-backed row keeps alerting.
#      The staleness ceiling matters most: the inventory's own producer is a
#      GitHub Actions workflow, so a freeze that disables the writer must not
#      silently freeze the last-known-good states into a permanent blindfold.
#   2. NEVER SILENT — identical annotation path; the row still lands in
#      check_results.json with its true state and `alert_suppressed: true`.
#   3. EXPIRES — same PRODUCER_SUPPRESSION_MAX_DAYS clock, and see
#      PAUSE_OWNERSHIP_KEY below for the one narrow way a pause outlives it.
#
# ONLY `disabled_manually` suppresses. GitHub also sets `disabled_inactivity`
# on scheduled workflows in repos it considers dormant — that is GitHub
# switching a producer off, not us, and it is precisely the failure
# `alpha-engine-config-I7370` exists for ("a workflow disabled by GitHub reads
# as HEALTHY"). Suppressing on it would convert that open finding into a
# permanent blind spot.
GHA_WORKFLOW_STATE_KEY = "_freshness_monitor/gha_workflow_states.json"
GHA_INVENTORY_MAX_AGE_HOURS = int(
    os.environ.get("GHA_INVENTORY_MAX_AGE_HOURS", "36")
)
# alpha-engine-config-I7509. The 14-day expiry is right for a pause NOBODY
# owns — that is the latch case, and going loud is the correct end state. It is
# wrong for a pause that IS owned: I6984 is an open P1 carrying the exact
# restore commands, a `gate:decision` label, and a backstop re-exam two months
# out. Paging every 30 minutes for eight weeks about a decision already written
# down and queued is noise with a tracking number.
#
# So ownership EXTENDS suppression, and only ownership does. The file is
# written by the same latch sweep, which has the backlog credential this Lambda
# deliberately lacks; it maps trigger → the OPEN item owning the pause. No
# owner, unreadable file, stale file, or a closed item ⇒ the 14-day clock
# applies unchanged. A pause whose owning issue is closed is a latch again, and
# goes loud again, which is the I6828 incident's whole lesson.
PAUSE_OWNERSHIP_KEY = "_freshness_monitor/pause_ownership.json"
PAUSE_OWNERSHIP_MAX_AGE_HOURS = int(
    os.environ.get("PAUSE_OWNERSHIP_MAX_AGE_HOURS", "72")
)

_PRODUCER_SURFACES = ("events", "scheduler", "gha")


def parse_producer_trigger(value: Any) -> tuple[str, str] | None:
    """Parse a registry ``producer_trigger`` scalar into ``(surface, name)``.

    Grammar is ``"<surface>:<name>"`` where surface is ``events`` (an
    EventBridge rule), ``scheduler`` (an EventBridge Scheduler schedule) —
    different APIs, not aliases, exactly as ``automation_pause.py`` splits
    them — or ``gha`` (a GitHub Actions workflow, config-I7509), whose name is
    ``<owner>/<repo>/<workflow-file>``. Anything else returns ``None`` and the
    row keeps today's behaviour; a malformed field must never suppress.
    """
    if not isinstance(value, str) or ":" not in value:
        return None
    surface, _, name = value.partition(":")
    surface, name = surface.strip(), name.strip()
    if surface not in _PRODUCER_SURFACES or not name:
        return None
    if surface == "gha":
        # owner/repo/workflow-file — three non-empty segments. A two-segment
        # value is ambiguous between "repo default workflow" and a typo, and
        # guessing would suppress against a workflow nobody named.
        parts = name.split("/")
        if len(parts) != 3 or not all(p.strip() for p in parts):
            return None
    return surface, name


def _load_json_with_age_ceiling(
    s3_client: Any, key: str, now: datetime, max_age_hours: int, label: str,
) -> dict[str, Any] | None:
    """Read a JSON object that is only trusted while recent (config-I7509).

    Returns ``None`` — meaning "resolve nothing from this file" — when the
    object is absent, unreadable, malformed, missing ``generated_at``, or older
    than ``max_age_hours``. Every one of those is the fail-toward-paging
    direction: this file can only ever REMOVE a page, so refusing to read it
    leaves the pass exactly as loud as it would be without the feature.
    """
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=key)
        payload = json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absent/denied ⇒ page as before
        logger.info(
            "%s unreadable at %s (%s) — every row it would have covered keeps "
            "alerting", label, key, type(exc).__name__,
        )
        return None
    if not isinstance(payload, dict):
        logger.warning("%s at %s is not an object — ignored", label, key)
        return None
    generated_at = payload.get("generated_at")
    try:
        stamped = datetime.fromisoformat(str(generated_at))
    except (TypeError, ValueError):
        logger.warning(
            "%s at %s has no parseable generated_at (%r) — ignored; a file that "
            "cannot prove its age cannot be trusted to silence a page",
            label, key, generated_at,
        )
        return None
    if stamped.tzinfo is None:
        stamped = stamped.replace(tzinfo=timezone.utc)
    age_hours = (now - stamped).total_seconds() / 3600.0
    if age_hours > max_age_hours:
        logger.warning(
            "%s at %s is %.1fh old (ceiling %dh) — ignored. Its own producer "
            "may be off; a frozen snapshot must not become a permanent "
            "blindfold, so the rows it covers page again",
            label, key, age_hours, max_age_hours,
        )
        return None
    return payload


def _load_gha_workflow_states(s3_client: Any, now: datetime) -> dict[str, dict]:
    """``{"owner/repo/workflow.yml": {"state", "disabled_since"}}`` from the
    latch sweep's inventory, or ``{}`` when it is absent, malformed or stale.

    ``disabled_since`` is GitHub's own ``updated_at`` for the workflow, i.e.
    the moment it was actually switched off — better than this Lambda's
    first-observed stamp, which can only ever say "since the monitor noticed".
    Two surfaces disagreeing about how old a pause is, is the thing the
    disabled-since bookkeeping exists to prevent."""
    payload = _load_json_with_age_ceiling(
        s3_client, GHA_WORKFLOW_STATE_KEY, now,
        GHA_INVENTORY_MAX_AGE_HOURS, "gha workflow-state inventory",
    )
    if payload is None:
        return {}
    if payload.get("complete") is False:
        # A partial enumeration cannot distinguish "absent because enabled"
        # from "absent because the listing broke half way".
        logger.warning(
            "gha workflow-state inventory reports complete=false — ignored; "
            "a partial listing cannot prove a workflow is enabled",
        )
        return {}
    workflows = payload.get("workflows")
    if not isinstance(workflows, dict):
        return {}
    out: dict[str, dict] = {}
    for name, row in workflows.items():
        # Accept the bare-string shape too, so a writer rolled back to the
        # simpler payload degrades to "no disabled_since", never to a crash.
        if isinstance(row, str):
            out[str(name)] = {"state": row, "disabled_since": None}
        elif isinstance(row, dict) and isinstance(row.get("state"), str):
            since = row.get("disabled_since")
            out[str(name)] = {
                "state": row["state"],
                "disabled_since": str(since)[:10] if since else None,
            }
    return out


def _load_pause_ownership(s3_client: Any, now: datetime) -> dict[str, dict]:
    """``{trigger: {"item", "url", "state"}}`` for pauses an OPEN backlog item
    owns, or ``{}`` when the file is absent, malformed or stale."""
    payload = _load_json_with_age_ceiling(
        s3_client, PAUSE_OWNERSHIP_KEY, now,
        PAUSE_OWNERSHIP_MAX_AGE_HOURS, "pause-ownership map",
    )
    if payload is None:
        return {}
    owners = payload.get("owners")
    if not isinstance(owners, dict):
        return {}
    out: dict[str, dict] = {}
    for trigger, owner in owners.items():
        # Only an OPEN item extends a suppression. A closed one means the
        # pause outlived its ruling and is a latch again (I6828).
        if isinstance(owner, dict) and owner.get("state") == "open" and owner.get("item"):
            out[str(trigger)] = owner
    return out


def resolve_disabled_producers(
    triggers: set[str],
    events_client: Any = None,
    scheduler_client: Any = None,
    gha_states: dict[str, str] | None = None,
) -> dict[str, str]:
    """Return ``{trigger: reason}`` for triggers LIVE-confirmed disabled.

    A trigger absent from the result is treated as enabled, which is the
    fail-toward-paging direction: an unresolvable trigger must not suppress
    its artifact's page. Each lookup is trapped individually so one denied or
    renamed trigger cannot blind the rest of the pass.

    ``gha_states`` carries the GitHub half (config-I7509), already read from
    the inventory by the caller; an empty or omitted map resolves no ``gha:``
    trigger, so those rows keep alerting.
    """
    disabled: dict[str, str] = {}
    if not triggers:
        return disabled
    for trigger in sorted(triggers):
        parsed = parse_producer_trigger(trigger)
        if parsed is None:
            continue
        surface, name = parsed
        try:
            if surface == "gha":
                state = ((gha_states or {}).get(name) or {}).get("state")
                if state == "disabled_manually":
                    disabled[trigger] = (
                        f"GitHub Actions workflow {name} is disabled_manually"
                    )
                elif state == "disabled_inactivity":
                    # Deliberately NOT suppressed — see the GHA_WORKFLOW_STATE_KEY
                    # comment. This is GitHub switching a producer off, which is
                    # a fault to page on, not a ruling to respect (I7370).
                    logger.warning(
                        "%s is disabled_inactivity — NOT suppressing; GitHub "
                        "turned this off, not us (config-I7370)", name,
                    )
            elif surface == "events":
                client = events_client or boto3.client("events")
                state = client.describe_rule(Name=name).get("State", "")
                if state == "DISABLED":
                    disabled[trigger] = f"EventBridge rule {name} is DISABLED"
            else:
                client = scheduler_client or boto3.client("scheduler")
                state = client.get_schedule(Name=name).get("State", "")
                if state == "DISABLED":
                    disabled[trigger] = f"Scheduler schedule {name} is DISABLED"
        except Exception as exc:  # noqa: BLE001 — fail toward paging, never toward silence
            logger.warning(
                "producer-trigger state unresolved for %s (%s: %s) — treating "
                "as ENABLED so its artifacts keep alerting",
                trigger, type(exc).__name__, exc,
            )
    return disabled


def _load_producer_disabled_since(s3_client: Any) -> dict[str, str]:
    """First-observed-disabled dates, keyed by trigger. Unreadable ⇒ empty,
    which means today becomes the first observation — the conservative
    direction, since a shorter observed pause expires sooner."""
    try:
        obj = s3_client.get_object(
            Bucket=REGISTRY_BUCKET, Key=PRODUCER_DISABLED_SINCE_KEY
        )
        data = json.loads(obj["Body"].read())
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items() if isinstance(v, str)}
    except Exception as exc:  # noqa: BLE001 — absent on first run; never fatal
        logger.info(
            "producer_disabled_since unreadable (%s) — treating every currently "
            "disabled trigger as first observed today", type(exc).__name__,
        )
    return {}


def _save_producer_disabled_since(s3_client: Any, mapping: dict[str, str]) -> None:
    try:
        s3_client.put_object(
            Bucket=REGISTRY_BUCKET,
            Key=PRODUCER_DISABLED_SINCE_KEY,
            Body=json.dumps(mapping, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — bookkeeping; the pass's deliverables stand
        logger.warning(
            "could not persist producer_disabled_since (%s: %s) — suppression "
            "ages from today on the next pass instead of from first observation",
            type(exc).__name__, exc,
        )


# ── Declared-off rows (alpha-engine-config-I8719) ───────────────────────────
#
# `producer_trigger` above answers "is this artifact's producing SCHEDULE
# switched off?" by asking live AWS or the GitHub workflow inventory. There is
# a class it structurally cannot see: a producer switched off INSIDE a pipeline
# that is itself still enabled.
#
# `backtest_pit_parity` is that class. The weekly SF's `SaturdayTrigger` rule
# is ENABLED and fires every week; what is off is the `PitParityCompare` stage,
# bypassed by `"skip_parity": true` on the trigger Input (Brian ruling
# 2026-08-13, re-affirmed 2026-08-26, gated on the `crucible_phase_3`
# milestone). No AWS object's state answers "does PitParityCompare run?", so
# there is nothing to probe — and on 2026-08-26T12:00:17Z the row was CRITICAL
# with `escalation_basis=owning_item_age`, i.e. the escalation fired precisely
# BECAUSE the deliberate disable was old. The ladder was anti-correlated with
# the thing it should measure.
#
# So this state is DECLARED, in the registry row, never inferred
# (`observability-policy.md` §8.3: DISABLED, DEPRECATED and RETIRED are the
# three declared states, and a disposition that exists only as an inference has
# to be re-derived by hand every time anyone asks).
#
# THE SAME THREE PROPERTIES as producer-trigger suppression, and they are why
# this is a sibling of that mechanism rather than a new one:
#
#   1. It FAILS TOWARD PAGING. A malformed block, an absent resolution, an
#      unknown milestone, a milestone already `reached`, or a resolution older
#      than DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS all leave the row alerting
#      exactly as today. Nothing about this path can be made to silence a row
#      by breaking it.
#   2. It is NEVER SILENT. A declared-off row still lands in
#      check_results.json with its true state, `declared_off: true`,
#      `console_state: "DISABLED"`, the ruling's date, its age in days, the
#      owning item and the clearing milestone — so the console renders
#      "declared off, N days" with the ruling attached, never green and never
#      an omission. `principles.md` §2.7: no data is never green, and neither
#      is deliberately-off.
#   3. It EXPIRES BY ITS OWN PREDICATE, and by nothing else. There is
#      deliberately NO day-count ceiling here, unlike
#      PRODUCER_SUPPRESSION_MAX_DAYS. A pause nobody owns is a latch and going
#      loud is the right end state; a declared-off row is the opposite case —
#      it names its owning item and its clearing milestone up front, and a
#      calendar expiry would page for a producer that is still deliberately
#      off. It clears when MILESTONE_REGISTRY.yaml flips the named milestone to
#      `reached`, which is the same predicate that clears the `gate:milestone`
#      label on the owning item.
#
# WHERE THE MILESTONE COMES FROM. This Lambda holds no GitHub credential and
# must not acquire one (same argument as GHA_WORKFLOW_STATE_KEY above). The
# milestone is resolved at PUBLISH time by
# `alpha-engine-config/scripts/resolve_declared_off.py` and appended to the
# registry we already read as a top-level `declared_off_resolution` block. So
# the declaration and its resolved predicate arrive together, in one file, over
# the transport that already exists.
#
# THE STALENESS CEILING is on the RESOLUTION, not on the declaration.
# `sync-artifact-registry.yml` republishes daily; if it stops — broken,
# disabled, or switched off by GitHub for repo dormancy
# (alpha-engine-config-I7370) — `resolved_at` ages past this ceiling inside
# 36h and every declared-off row pages again. A resolution nothing refreshes is
# a permanent blindfold, and that is the one failure this mechanism must not
# introduce.
DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS = int(
    os.environ.get("DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS", "36")
)
#: Every key a `declared_off:` block must carry to be honoured. A block short
#: of any of them is DROPPED (the row keeps paging) and logged — matching
#: `producer_trigger`'s degrade-to-alerting posture, because a suppression
#: field must never be able to take down a registry load.
DECLARED_OFF_REQUIRED = ("since", "reason", "owning_item", "clears_when")


def parse_declared_off(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Extract well-formed ``declared_off`` blocks from a parsed registry.

    Returns ``{artifact_id: block}``. A malformed block is dropped with a
    WARNING rather than raising: the field's whole job is to REMOVE a page, so
    a typo must degrade to today's behaviour, not to a registry that fails to
    load and takes every row's monitoring with it. The PR-time chokepoint is
    ``alpha-engine-config/scripts/validate_artifact_registry.py``, which fails
    loudly on exactly the shapes dropped here.
    """
    out: dict[str, dict[str, Any]] = {}
    for entry in data.get("artifacts") or []:
        if not isinstance(entry, dict):
            continue
        block = entry.get("declared_off")
        if block is None:
            continue
        aid = str(entry.get("artifact_id"))
        if not isinstance(block, dict):
            logger.warning(
                "registry row %s carries a declared_off that is not a mapping "
                "(%s) — row keeps alerting", aid, type(block).__name__,
            )
            continue
        missing = [k for k in DECLARED_OFF_REQUIRED if not block.get(k)]
        milestone = (block.get("clears_when") or {}).get("milestone") \
            if isinstance(block.get("clears_when"), dict) else None
        if missing or not milestone:
            logger.warning(
                "registry row %s carries a malformed declared_off "
                "(missing=%s, milestone=%r) — row keeps alerting. A "
                "declared-off state without a clearing milestone could never "
                "lapse, which is worse than the page it would have removed",
                aid, missing, milestone,
            )
            continue
        out[aid] = dict(block)
    return out


def resolve_declared_off(
    declared_off_by_id: dict[str, dict[str, Any]],
    resolution: Any,
    now: datetime,
) -> dict[str, dict[str, Any]]:
    """Decide which declared-off rows are actually suppressed this sweep.

    Returns ``{artifact_id: {...}}`` for EVERY row carrying a well-formed
    block — including the ones that are NOT suppressed. That is deliberate:
    the console has to render a declared-off row whose suppression has lapsed
    differently from one that never declared anything, and dropping the
    un-suppressed rows here would make those two indistinguishable downstream.

    ``suppressed`` is True only when ALL of the following hold. Every other
    combination leaves the row on the normal page path:

      * the published registry carries a ``declared_off_resolution`` block;
      * that block names this artifact;
      * its ``milestone_status`` is ``pending`` (``reached`` means the ruling
        has cleared and normal freshness resumes; ``unknown`` means the
        publisher could not evaluate the predicate at all);
      * its ``resolved_at`` parses and is within
        :data:`DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS`.
    """
    if not declared_off_by_id:
        return {}
    res = resolution if isinstance(resolution, dict) else {}
    rows = res.get("rows") if isinstance(res.get("rows"), dict) else {}
    resolved_at_raw = res.get("resolved_at")
    resolution_age_hours: float | None = None
    if isinstance(resolved_at_raw, str):
        try:
            parsed = datetime.fromisoformat(resolved_at_raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            resolution_age_hours = (now - parsed).total_seconds() / 3600.0
        except ValueError:
            resolution_age_hours = None
    resolution_fresh = (
        resolution_age_hours is not None
        and resolution_age_hours <= DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS
    )
    if declared_off_by_id and not resolution_fresh:
        logger.warning(
            "declared-off resolution is ABSENT or STALE (resolved_at=%r, "
            "age_hours=%s, ceiling=%d) — %d declared-off row(s) keep alerting. "
            "The publisher is alpha-engine-config's sync-artifact-registry.yml, "
            "which runs daily; a resolution nothing refreshes is a permanent "
            "blindfold, so this fails toward paging on purpose",
            resolved_at_raw, resolution_age_hours,
            DECLARED_OFF_RESOLUTION_MAX_AGE_HOURS, len(declared_off_by_id),
        )

    out: dict[str, dict[str, Any]] = {}
    for aid, block in declared_off_by_id.items():
        milestone = str((block.get("clears_when") or {}).get("milestone") or "")
        row = rows.get(aid) if isinstance(rows.get(aid), dict) else {}
        status = str(row.get("milestone_status") or "unresolved")
        since = str(block.get("since") or "")
        try:
            days = (now.date() - date.fromisoformat(since)).days
        except ValueError:
            days = 0
        suppressed = bool(resolution_fresh and status == "pending")
        if not suppressed:
            logger.warning(
                "declared-off NOT suppressed for %s: milestone=%s status=%s "
                "resolution_fresh=%s — the row pages exactly as it would "
                "without the block",
                aid, milestone, status, resolution_fresh,
            )
        else:
            logger.info(
                "declared-off (config-I8719): %s — %s (since %s, %d days, "
                "owning_item=%s, clears when milestone %s is reached); "
                "console-only, no SNS/Telegram, and NOT eligible for "
                "owning_item_age escalation",
                aid, block.get("reason"), since, days,
                block.get("owning_item"), milestone,
            )
        out[aid] = {
            "suppressed": suppressed,
            "since": since,
            "days_declared_off": days,
            "reason": block.get("reason"),
            "owning_item": block.get("owning_item"),
            "clears_when_milestone": milestone,
            "milestone_status": status,
            "resolution_age_hours": (
                round(resolution_age_hours, 2)
                if resolution_age_hours is not None else None
            ),
            "resolution_fresh": resolution_fresh,
        }
    return out


def _declared_triggers(value: Any) -> tuple[str, ...]:
    """Normalise a row's ``producer_trigger`` map value to a tuple. The loader
    already stores tuples; a bare string is accepted so every existing caller
    and test keeps working unchanged."""
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def apply_producer_suppression(
    s3_client: Any,
    producer_trigger_by_id: dict[str, str | tuple[str, ...]],
    now: datetime,
    events_client: Any = None,
    scheduler_client: Any = None,
) -> dict[str, dict[str, Any]]:
    """Resolve which artifacts have a deliberately-disabled producer.

    Returns ``{artifact_id: {"trigger", "reason", "disabled_since",
    "days_disabled", "suppressed"}}`` for every artifact whose producer is
    live-confirmed DISABLED. ``suppressed`` is False once the pause has run
    past :data:`PRODUCER_SUPPRESSION_MAX_DAYS` — the row is still annotated
    (so the console can explain the page) but it pages again — UNLESS an open
    backlog item owns the pause (config-I7509), in which case ``pause_owner``
    carries that item and the suppression holds.

    A row may declare SEVERAL triggers (config-I7509). It is suppressed only
    when EVERY one of them is off, and its clock runs from the MOST RECENT
    switch-off — a row with one live producer left is still a row whose
    artifact somebody expects to be written.
    """
    if not producer_trigger_by_id:
        return {}
    all_triggers = {
        t for v in producer_trigger_by_id.values() for t in _declared_triggers(v)
    }
    gha_states = _load_gha_workflow_states(s3_client, now)
    disabled = resolve_disabled_producers(
        all_triggers, events_client, scheduler_client, gha_states=gha_states,
    )
    ownership = _load_pause_ownership(s3_client, now) if disabled else {}
    since = _load_producer_disabled_since(s3_client)
    today = now.date().isoformat()
    # Drop triggers that came back — a re-enabled producer restarts the clock.
    next_since = {k: v for k, v in since.items() if k in disabled}
    for trigger in disabled:
        # A gha trigger carries GitHub's own switch-off date; prefer it over
        # both the stored first-observation and today.
        parsed = parse_producer_trigger(trigger)
        authoritative = (
            (gha_states.get(parsed[1]) or {}).get("disabled_since")
            if parsed and parsed[0] == "gha" else None
        )
        if authoritative:
            next_since[trigger] = authoritative
        else:
            next_since.setdefault(trigger, today)
    if next_since != since:
        _save_producer_disabled_since(s3_client, next_since)

    out: dict[str, dict[str, Any]] = {}
    for artifact_id, declared in producer_trigger_by_id.items():
        triggers = _declared_triggers(declared)
        # EVERY declared producer must be off. One live producer left means
        # the artifact is still expected, and the miss is a real one.
        if not all(t in disabled for t in triggers):
            continue
        reason = "; ".join(disabled[t] for t in triggers)
        trigger = ", ".join(triggers)
        first_seens = [next_since.get(t, today) for t in triggers]
        # The pause is only as old as its most recent switch-off — that is the
        # date from which "has anybody looked at this?" is a fair question.
        first_seen = max(first_seens)
        try:
            days = (now.date() - date.fromisoformat(first_seen)).days
        except ValueError:
            days = 0
        # Any owned trigger owns the whole row's pause: the row is quiet
        # because of a decision, and the decision has a tracking number.
        owner = next((ownership[t] for t in triggers if t in ownership), None)
        within_clock = days < PRODUCER_SUPPRESSION_MAX_DAYS
        suppressed = within_clock or owner is not None
        if not within_clock and owner is not None:
            logger.info(
                "producer-suppression HELD past the %d-day clock for %s: %s "
                "has been disabled %d days and %s owns the pause — the "
                "decision is already written down and queued, so this stays "
                "console-only (config-I7509)",
                PRODUCER_SUPPRESSION_MAX_DAYS, artifact_id, trigger, days,
                owner.get("item"),
            )
        elif not suppressed:
            logger.warning(
                "producer-suppression LAPSED for %s: %s has been disabled %d "
                "days (limit %d) with no open item owning the pause — paging "
                "again, a pause this long that nobody owns is a latch",
                artifact_id, trigger, days, PRODUCER_SUPPRESSION_MAX_DAYS,
            )
        out[artifact_id] = {
            "trigger": trigger,
            "reason": reason,
            "disabled_since": first_seen,
            "days_disabled": days,
            "suppressed": suppressed,
            "pause_owner": owner.get("item") if owner else None,
            "pause_owner_url": owner.get("url") if owner else None,
        }
    return out


def enumerate_disabled_schedules(
    events_client: Any = None,
    scheduler_client: Any = None,
) -> list[dict[str, str]]:
    """Every DISABLED EventBridge rule and EventBridge Scheduler schedule.

    Unlike :func:`resolve_disabled_producers`, this takes no trigger set — it
    walks the account. That is the point: a rule nothing in ARTIFACT_REGISTRY
    references is exactly the one no surface can currently see, and the two
    probes paused on 2026-08-07 (`alpha-engine-router-exposure-probe-15min`,
    `alpha-engine-ssm-reachability-probe-5min`) are both in that class.

    A surface that cannot be enumerated is reported as an ERROR row rather
    than omitted. Silence here would reproduce the defect this exists to
    close — an empty inventory must mean "nothing is disabled", never
    "the walk failed".
    """
    out: list[dict[str, str]] = []
    try:
        client = events_client or boto3.client("events")
        paginator = client.get_paginator("list_rules")
        for page in paginator.paginate():
            for rule in page.get("Rules", []):
                if rule.get("State") == "DISABLED":
                    out.append({
                        "trigger": f"events:{rule['Name']}",
                        "name": rule["Name"],
                        "surface": "events",
                        "schedule": rule.get("ScheduleExpression", ""),
                    })
    except Exception as exc:  # noqa: BLE001 — recorded, never silently empty
        logger.error(
            "DISABLED_INVENTORY_ENUMERATION_FAILED for events (%s: %s) — the "
            "inventory is INCOMPLETE and says so", type(exc).__name__, exc,
        )
        out.append({"trigger": "", "name": "", "surface": "events",
                    "schedule": "", "error": f"{type(exc).__name__}: {exc}"})
    try:
        client = scheduler_client or boto3.client("scheduler")
        paginator = client.get_paginator("list_schedules")
        for page in paginator.paginate():
            for sch in page.get("Schedules", []):
                if sch.get("State") == "DISABLED":
                    out.append({
                        "trigger": f"scheduler:{sch['Name']}",
                        "name": sch["Name"],
                        "surface": "scheduler",
                        "schedule": sch.get("ScheduleExpression", ""),
                    })
    except Exception as exc:  # noqa: BLE001 — recorded, never silently empty
        logger.error(
            "DISABLED_INVENTORY_ENUMERATION_FAILED for scheduler (%s: %s) — "
            "the inventory is INCOMPLETE and says so", type(exc).__name__, exc,
        )
        out.append({"trigger": "", "name": "", "surface": "scheduler",
                    "schedule": "", "error": f"{type(exc).__name__}: {exc}"})
    return out


def write_disabled_producer_inventory(
    s3_client: Any,
    now: datetime,
    referenced_triggers: set[str],
    events_client: Any = None,
    scheduler_client: Any = None,
) -> dict[str, Any]:
    """Write the account-wide disabled-schedule inventory (config-I6817 D4).

    Reuses ``producer_disabled_since`` for the age of triggers the suppression
    path already tracks, and stamps today for the rest. ``referenced`` says
    whether ANY artifact row names the trigger — an unreferenced disabled rule
    is invisible to every other surface in the fleet, which is why the flag is
    on the row rather than left to the consumer to derive.

    Returns the payload. Never raises: this is an observability side effect and
    must not take down a sweep whose alerting already ran.
    """
    today = now.date().isoformat()
    rows = enumerate_disabled_schedules(events_client, scheduler_client)
    since = _load_producer_disabled_since(s3_client)
    incomplete = any(r.get("error") for r in rows)
    for row in rows:
        if row.get("error"):
            continue
        first_seen = since.get(row["trigger"], today)
        row["disabled_since"] = first_seen
        try:
            row["days_disabled"] = (
                now.date() - date.fromisoformat(first_seen)
            ).days
        except ValueError:
            row["days_disabled"] = 0
        row["referenced_by_registry"] = row["trigger"] in referenced_triggers
    payload = {
        "generated_at": now.isoformat(),
        "complete": not incomplete,
        "disabled_count": len([r for r in rows if not r.get("error")]),
        "unreferenced_count": len(
            [r for r in rows
             if not r.get("error") and not r.get("referenced_by_registry")]
        ),
        "rows": rows,
    }
    try:
        s3_client.put_object(
            Bucket=REGISTRY_BUCKET,
            Key=DISABLED_PRODUCER_INVENTORY_KEY,
            Body=json.dumps(payload, indent=2).encode(),
            ContentType="application/json",
        )
        logger.info(
            "disabled-producer inventory: %d disabled (%d unreferenced by any "
            "artifact row), complete=%s",
            payload["disabled_count"], payload["unreferenced_count"],
            payload["complete"],
        )
    except Exception as exc:  # noqa: BLE001 — see docstring; the ERROR is the surface
        logger.error(
            "DISABLED_INVENTORY_WRITE_FAILED (non-fatal, the sweep's alerting "
            "already ran): %s: %s", type(exc).__name__, exc,
        )
    return payload


def _load_prev_miss_counts(s3_client: Any) -> dict[str, int]:
    """Previous sweep's per-artifact ``consecutive_miss_runs`` counters,
    read back from ``check_results.json`` (config-I3086 warning
    escalation). Missing/malformed prior results reset all counters —
    surfaced as a ::warning, never fatal."""
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHECK_RESULTS_KEY)
        data = json.loads(obj["Body"].read())
        return {
            row["artifact_id"]: int(row.get("consecutive_miss_runs", 0))
            for row in data.get("results", [])
            if isinstance(row, dict) and row.get("artifact_id")
        }
    except Exception as exc:  # noqa: BLE001 — counter loss degrades to reset, not failure
        logger.warning(
            "previous check_results read failed — escalation counters reset "
            "(config-I3086): %s", exc,
        )
        return {}


def _load_prev_issue_filed(s3_client: Any) -> dict[str, str]:
    """Previous sweep's per-artifact filed-issue URLs (config#2055 Gap 2),
    read back from ``check_results.json``. A present entry means an
    escalation P1 was already filed for this artifact's CURRENT incident —
    dedup source of truth, so a still-stale row doesn't re-file every day.
    Missing/malformed prior results degrade to "nothing filed yet" (an
    extra issue on next threshold-cross is a much smaller cost than a
    counter read failure silently suppressing a real escalation forever)."""
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHECK_RESULTS_KEY)
        data = json.loads(obj["Body"].read())
        return {
            row["artifact_id"]: row["issue_filed_url"]
            for row in data.get("results", [])
            if isinstance(row, dict) and row.get("artifact_id") and row.get("issue_filed_url")
        }
    except Exception as exc:  # noqa: BLE001 — read failure degrades to "nothing filed yet"
        logger.warning(
            "previous check_results read failed — issue-filed markers reset "
            "(config#2055): %s", exc,
        )
        return {}


def _load_prev_episode_state(s3_client: Any) -> dict[str, dict[str, str]]:
    """Previous sweep's per-artifact open-episode sentinel, read back from
    ``check_results.json`` — the SAME round-trip ``consecutive_miss_runs``
    / ``issue_filed_url`` already use (config#2055: "carried in
    check_results.json ... so no new state surface is introduced"). Only
    ``missing``/``probe_failed`` rows need this: :func:`_episode_signature`
    derives a ``stale`` row's episode key straight from the freshest
    instance's own ``last_modified``, with no round-trip required.

    Returns ``{artifact_id: {"state": ..., "opened_at": ...}}``.
    Missing/malformed prior results degrade to "no open episode carried
    forward" — the next alerting sweep mints a fresh episode (one extra
    page is a far smaller cost than a read failure silently gluing two
    unrelated incidents into one suppressed key forever)."""
    try:
        obj = s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=CHECK_RESULTS_KEY)
        data = json.loads(obj["Body"].read())
        out: dict[str, dict[str, str]] = {}
        for row in data.get("results", []):
            if not isinstance(row, dict):
                continue
            aid = row.get("artifact_id")
            state = row.get("episode_state")
            opened_at = row.get("episode_opened_at")
            if aid and state and opened_at:
                out[aid] = {"state": state, "opened_at": opened_at}
        return out
    except Exception as exc:  # noqa: BLE001 — read failure degrades to "no episode carried forward"
        logger.warning(
            "previous check_results read failed — episode markers reset: %s",
            exc,
        )
        return {}


def _is_confirmed_miss(result: CheckResult) -> bool:
    """The same confirmed-miss shape the alert path fires on: an
    alerting state past its SLA grace (probe_failed has no grace)."""
    if result.state not in _ALERTING_STATES:
        return False
    return result.state == "probe_failed" or result.sla_violated_by_minutes > 0


# ── Per-spec probe (catches per-spec errors so one bad row doesn't sink the pass) ─


# ── "never once written" is not an SLA miss (config-I7603 d2 / config-I7622) ─
#
# A `missing` row means the recency scan found no instance. Two very different
# facts wear that state:
#
#   * an artifact that HAS been produced before and is now absent — a producer
#     failure, which is what the SLA ladder and the page path are for;
#   * an artifact NO CODE HAS EVER WRITTEN — a registry row registered ahead of
#     (or instead of) its producer. Its absence is correct. Paging on it, daily,
#     forever, is the false alarm Brian named on 2026-08-19: "if these are false
#     alarms i don't want to receive them."
#
# Measured 2026-08-19T15:41:32Z: the single remaining page in the whole sweep was
# `rag_corpus_scope_state`, whose key `rag_corpus/scope_state/latest.json` has no
# writer anywhere in the fleet — `grep -rn scope_state` across nousergon-data,
# crucible-research, nousergon-lib and alpha-engine-config finds none, and the
# `rag_corpus/` prefix does not exist in the bucket at all. It had escalated to
# the critical path on 6 consecutive misses. `backtest_contribution_lift` and
# `rag_ingestion_progress` were the two prior instances of the same class, the
# latter reaching 14 escalations.
#
# They have different causes, different owners and different fixes, and only one
# of them is worth waking someone for. So: distinguish them, page on one, and
# REPORT the other — never silence it. A never-written row still appears in
# check_results.json with its true state, and still gets a standing line in the
# digest under its own heading, so the registry's own debt stays visible without
# being an alarm.

_NEVER_WRITTEN_PROBE_MAX = int(os.environ.get("NEVER_WRITTEN_PROBE_MAX", "25"))


_NEVER_WRITTEN_SCAN_PAGES = int(os.environ.get("NEVER_WRITTEN_SCAN_PAGES", "5"))


def _prefix_has_ever_been_written(
    s3_client: Any, spec: ArtifactSpec, result: CheckResult
) -> bool | None:
    """Has ANY instance of THIS artifact ever existed?

    Returns True/False, or None when the question could not be answered — which
    is NOT the same as False and must never be rendered as "never written". An
    unanswerable probe leaves the row on the normal page path, because the
    direction this must never fail in is silencing a real absence.

    The recency scan that produced ``missing`` is bounded by a cadence-derived
    window; this asks the unbounded question, and only for rows already in
    ``missing`` (three of 146 in the measured sweep).

    **Prefix alone is not the question (config-I7622 follow-up).** A
    date-templated key like ``research/{date}/self_test.json`` has the fixed head
    ``research/`` — a shared top-level prefix with thousands of unrelated objects
    under it. Measured 2026-08-19: ``research_self_test`` resolved
    ``never_written=False`` on a populated ``research/`` prefix while
    ``aws s3 ls s3://alpha-engine-research/research/ --recursive | grep
    self_test.json`` returned NOTHING — the artifact has never once been written
    and the probe could not see it. ``backtest_contribution_lift`` sits under
    ``backtest/`` with the same shape. So the trailing fixed segment of the
    template is matched too, and the answer is only ``False`` when a key actually
    bearing that suffix is found.

    The scan is bounded at :data:`_NEVER_WRITTEN_SCAN_PAGES` pages of 1000. Hitting
    the cap returns None — not False. A prefix too large to search is a question
    left unanswered, and answering it "never written" on the strength of having
    given up is the failure this whole function is shaped to avoid.
    """
    template = spec.s3_key_template
    # `listable_prefix` stops at the first `{` OR the producer-chosen `*`
    # segment (alpha-engine-config-I10200). Deriving the head with a bare
    # `split("{")` here got `oos_rows/*/` for the dated row and the WHOLE
    # template — literal `*` and all — for `oos_rows/*/latest.parquet`, so the
    # LIST ran against a prefix no object can start with, returned KeyCount 0,
    # and this function answered "never written" about an artifact that had
    # existed since 2026-09-05. Borrowed from the lib rather than re-derived:
    # three resolvers must agree about this grammar (`policy-shared-code`).
    head = listable_prefix(template)
    if head.endswith("/"):
        prefix = head
    elif "/" in head:
        prefix = head.rsplit("/", 1)[0] + "/"
    else:
        prefix = head
    if not prefix:
        prefix = result.observed_key or template

    # The authoritative test for a wildcard template is the compiled shape —
    # its coarse suffix (`.parquet`) would sweep in every sibling diagnostic
    # under `predictor/diagnostics/`, which is the same over-match that made
    # `research_self_test` read never_written=False off a populated
    # `research/` prefix (config-I7622 follow-up).
    pattern = key_pattern(template) if has_wildcard_segment(template) else None

    # Everything after the LAST placeholder — e.g. "/self_test.json" for
    # `research/{date}/self_test.json`. A template ending at a directory
    # boundary (`groom/{date}/`) has no distinguishing trailing segment, so
    # prefix membership IS the question there and the cheap MaxKeys=1 path is
    # the right one.
    suffix = key_suffix(template) if "{" in template else ""
    if not suffix.strip("/"):
        suffix = ""
    if pattern is not None:
        # A wildcard row always needs the matching scan, never the MaxKeys=1
        # prefix-membership shortcut: prefix membership is exactly the
        # question it cannot answer.
        suffix = suffix or "/"

    try:
        if not suffix:
            resp = s3_client.list_objects_v2(
                Bucket=spec.s3_bucket, Prefix=prefix, MaxKeys=1
            )
            return int(resp["KeyCount"]) > 0

        token = None
        for _ in range(_NEVER_WRITTEN_SCAN_PAGES):
            kwargs = {"Bucket": spec.s3_bucket, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3_client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                key = str(obj.get("Key", ""))
                if pattern is not None:
                    if pattern.match(key) is not None:
                        return True
                elif key.endswith(suffix):
                    return True
            if not resp.get("IsTruncated"):
                return False
            token = resp.get("NextContinuationToken")
            if not token:
                return False
        logger.info(
            "never-written probe for %s gave up after %d page(s) under %r — "
            "recorded as UNKNOWN, so the row keeps the normal page path",
            spec.artifact_id, _NEVER_WRITTEN_SCAN_PAGES, prefix,
        )
        return None
    except Exception as exc:  # noqa: BLE001 — unanswerable, so the row keeps the page path; this WARNING and never_written=None on the row are the recording surfaces
        logger.warning(
            "never-written probe for %s could not run (prefix=%r) — the row "
            "keeps the normal page path: %s",
            spec.artifact_id, prefix, exc,
        )
        return None


# ── Per-run LIST cache (alpha-engine-config-I9206) ──────────────────────────
#
# MEASURED 2026-09-06 from the laptop against the live registry: for every
# TEMPLATED spec, `check_freshness` LISTs the whole `_listable_prefix`. Summed
# per spec that is 106.7s of LISTing; done ONCE per distinct (bucket, prefix)
# it is 9.7s. The `_prefix_has_ever_been_written` probes are NOT the cost (17
# probes, 3.0s total). That difference is the whole regression: the daily
# 12:00 UTC sweep ran in 71s on 2026-08-22, ~100-112s on 08-27..08-31, and has
# hit its 120s timeout every day since 2026-09-01 (3 Errors/day = the initial
# invocation plus two async retries), latching
# `alpha-engine-lambda-errors-freshness-monitor` and
# `alpha-engine-watch-plane-freshness-monitor-errors` daily.
#
# The fix is a per-run dict THE CALLER OWNS, threaded into `check_freshness`.
# It is not module-level state: a Lambda container is reused across days, and a
# LIST result cached beyond one invocation would answer a freshness question
# with yesterday's listing — the exact inversion this monitor exists to catch.
_CHECK_FRESHNESS_ACCEPTS_LIST_CACHE = (
    "list_cache" in inspect.signature(check_freshness).parameters
)

_LIST_CACHE_UNAVAILABLE_WARNING = (
    "list cache unavailable: installed nousergon-lib predates "
    "check_freshness(list_cache=); bump the pin — alpha-engine-config-I9206"
)


def _installed_nousergon_lib_version() -> str:
    """Best-effort version string for the degradation warning only.

    Never raises and never gates behaviour: the capability probe above is the
    decision, this is the label the operator reads next to it.
    """
    try:
        from importlib.metadata import version

        return version("nousergon-lib")
    except Exception:  # noqa: BLE001 — a log LABEL, not a decision
        return "unknown"


def _new_list_cache() -> dict:
    """One LIST cache for one pass, plus the honest report when it is inert.

    FAIL-LOUD carve-out (the repo's default is RAISE, and this is a writer of
    nothing — a reader):
      (a) failure mode swallowed: the pinned nousergon-lib predates the
          `list_cache=` keyword, so every templated spec LISTs its prefix again
          and the pass runs ~10x its LIST budget — i.e. straight back into the
          120s timeout this change exists to remove;
      (b) why the primary deliverable survives: `check_freshness` is called
          exactly as before and every freshness VERDICT is identical; only the
          duration degrades;
      (c) recording surface: this WARNING, emitted once per pass into the
          Lambda's CloudWatch log group, naming the installed version and the
          tracked follow-up. A degradation nobody can read is not a
          degradation, it is a silence.
    """
    if not _CHECK_FRESHNESS_ACCEPTS_LIST_CACHE:
        logger.warning(
            "%s (installed nousergon-lib==%s)",
            _LIST_CACHE_UNAVAILABLE_WARNING,
            _installed_nousergon_lib_version(),
        )
    return {}


def _check_one(
    s3_client: Any,
    spec: ArtifactSpec,
    now: datetime,
    list_cache: dict | None = None,
) -> tuple[CheckResult, Exception | None]:
    """Wrap :func:`check_freshness` with a per-spec exception trap.

    Returns ``(result, None)`` on success or
    ``(synthesized_probe_failed_result, exc)`` on a per-spec error
    (e.g., a malformed key template that fails ``str.format``,
    a transient network blip the substrate didn't classify).

    ``list_cache`` (alpha-engine-config-I9206) is the caller-owned per-run LIST
    cache. It is threaded only when the installed substrate accepts it; see
    :func:`_new_list_cache` for what happens when it does not.
    """
    try:
        if _CHECK_FRESHNESS_ACCEPTS_LIST_CACHE:
            return check_freshness(s3_client, spec, now, list_cache=list_cache), None
        return check_freshness(s3_client, spec, now), None
    except Exception as exc:  # noqa: BLE001 — per-spec resilience
        result = CheckResult(
            state="probe_failed",
            reason=f"per-spec exception: {type(exc).__name__}: {exc}",
            canonical_key=spec.s3_key_template,
        )
        return result, exc


# ── Aggregation + S3 emission ───────────────────────────────────────────────


def suppression_coverage(
    pairs: list[tuple[Any, Any]],
    producer_trigger_by_id: dict[str, str | list],
    suppression_by_id: dict[str, dict[str, Any]],
    inventory: dict[str, Any] | None,
) -> dict[str, Any]:
    """Which not-fresh rows nothing can explain (alpha-engine-config-I7606).

    Producer suppression only ever fires for a row that DECLARED its
    ``producer_trigger``. Coverage of that field is therefore the thing
    standing between "a producer was deliberately switched off" and "Brian
    gets a CRITICAL page for his own ruling" — and until now the only way to
    learn a row was missing the declaration was to receive that page.

    Measured 2026-08-18: 13 of 145 rows declared it. Two that did not —
    ``health_alpha_engine_predictor_health_check`` and
    ``predictor_drift_detection`` — had been escalating warning->critical on
    miss-count for producers DISABLED under the 2026-08-07 pause ruling
    (config-I6617), while six rows that did declare it sat correctly quiet in
    the same sweep. The mechanism was working; the field was absent. Nothing
    reported the absence.

    This does not guess. A registry row does not say what produces it, so
    "undeclared" here means exactly that and never "its producer is off" —
    the inventory of what IS off is the other half, already written next door,
    and the two are reported side by side so a human can join them. The number
    that matters is ``undeclared_not_fresh``: rows that are stale or missing
    and carry no declaration that could ever explain it.
    """
    not_fresh = [
        spec for spec, result in pairs
        if getattr(result, "state", None) in ("stale", "missing")
    ]
    undeclared = sorted(
        spec.artifact_id for spec in not_fresh
        if spec.artifact_id not in producer_trigger_by_id
    )
    disabled_rows = [
        r for r in ((inventory or {}).get("rows") or []) if not r.get("error")
    ]
    return {
        "registry_rows": len(pairs),
        "rows_declaring_producer_trigger": len(producer_trigger_by_id),
        "not_fresh": len(not_fresh),
        "suppressed": len(
            [v for v in suppression_by_id.values() if v.get("suppressed")]
        ),
        "undeclared_not_fresh": len(undeclared),
        "undeclared_not_fresh_ids": undeclared,
        # The join a human has to make by hand today: these are switched off
        # and no registry row names them, so no artifact's page can currently
        # be explained by them even if one of them is the cause.
        "disabled_producers_unreferenced": len(
            [r for r in disabled_rows if not r.get("referenced_by_registry")]
        ),
        "inventory_complete": bool((inventory or {}).get("complete")),
    }


def _emit_suppression_coverage_metrics(
    cw_client: Any, coverage: dict[str, Any]
) -> None:
    """Emit coverage to CW on EVERY run, zeros included (config-I7606).

    Zeros included for the reason the execution-loop emitter states: the
    absence of these datapoints means the emitter is dead, never that coverage
    is complete. ``UndeclaredNotFreshRows`` is the alarmable one — it is the
    count of pages that no suppression could ever prevent.
    """
    cw_client.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=[
            {"MetricName": "UndeclaredNotFreshRows",
             "Value": float(coverage["undeclared_not_fresh"]), "Unit": "Count"},
            {"MetricName": "RowsDeclaringProducerTrigger",
             "Value": float(coverage["rows_declaring_producer_trigger"]),
             "Unit": "Count"},
            {"MetricName": "DisabledProducersUnreferenced",
             "Value": float(coverage["disabled_producers_unreferenced"]),
             "Unit": "Count"},
        ],
    )


def _serialize_check_results(
    pairs: list[tuple[ArtifactSpec, CheckResult]], now: datetime,
    miss_counts: dict[str, int] | None = None,
    coerced_ids: set[str] | None = None,
    issue_filed_by_id: dict[str, str | None] | None = None,
    suppression_by_id: dict[str, dict[str, Any]] | None = None,
    declared_off_by_id: dict[str, dict[str, Any]] | None = None,
    owning_by_id: dict[str, dict[str, Any]] | None = None,
    execution_loop: dict[str, Any] | None = None,
    coverage: dict[str, Any] | None = None,
    never_written_by_id: dict[str, bool | None] | None = None,
    episode_by_id: dict[str, dict[str, str] | None] | None = None,
    driver_by_id: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Build the ``check_results.json`` payload — one row per spec for
    the dashboard surface (Phase 5). ``miss_counts``/``coerced_ids``
    (config-I3086) persist the warning-escalation counters and mark rows
    whose severity was dynamically coerced, so the dashboard can explain
    a row paging as critical while the registry declares warning.
    ``issue_filed_by_id`` (config#2055 Gap 2) persists the extended-
    staleness escalation's dedup marker — the URL of the P1 filed for
    this artifact's current incident, or ``None``/absent if none is
    in flight.

    ``suppression_by_id`` (config-I6570) annotates rows whose producing
    trigger is live-confirmed DISABLED. The row keeps its TRUE state — a
    stale artifact behind a paused producer is still stale — and gains the
    trigger, the date it was first observed off, and whether the page was
    suppressed. The console must render these as "producer disabled", never
    as green and never as a silent omission."""
    miss_counts = miss_counts or {}
    coerced_ids = coerced_ids or set()
    issue_filed_by_id = issue_filed_by_id or {}
    suppression_by_id = suppression_by_id or {}
    declared_off_by_id = declared_off_by_id or {}
    owning_by_id = owning_by_id or {}
    episode_by_id = episode_by_id or {}
    driver_by_id = driver_by_id or {}
    rows = []
    for spec, result in pairs:
        suppression = suppression_by_id.get(spec.artifact_id)
        declared_off = declared_off_by_id.get(spec.artifact_id)
        # config-I9336 — computed for every row, not only ones that page,
        # so the console can watch a driver/episode build BEFORE it pages.
        # `episode_state`/`episode_opened_at` are the two fields
        # `_load_prev_episode_state` reads back next sweep — the round-trip
        # is what keeps a `missing`/`probe_failed` sentinel stable across
        # the whole streak with no new persisted-state surface.
        episode = episode_by_id.get(spec.artifact_id)
        driver = driver_by_id.get(spec.artifact_id) or _evaluate_driver(spec, result)
        # §7.4a: the RECORD carries the full owning-item block for every
        # row that had one resolved — including rows whose page was
        # console-only or suppressed. Suppression is a delivery decision,
        # never a recording one.
        owning = owning_by_id.get(spec.artifact_id) or {}
        owning_item = owning.get("owning_item") or {}
        rows.append(
            {
                "owning_item_number": owning_item.get("number"),
                "owning_item_url": owning_item.get("url"),
                "owning_item_title": owning_item.get("title"),
                "owning_item_priority": owning_item.get("priority"),
                "owning_item_age_days": owning_item.get("age_days"),
                "owning_item_sla_days": owning_item.get("sla_days"),
                "owning_item_members": [
                    m["number"] for m in owning.get("members") or []
                ],
                "owning_item_lookup_attempted": bool(owning),
                "owning_item_lookup_degraded": bool(owning.get("degraded")),
                "owning_item_lookup_degraded_reason": owning.get("degraded_reason"),
                "producer_trigger": (
                    suppression["trigger"] if suppression else None
                ),
                "producer_disabled": suppression is not None,
                "producer_disabled_since": (
                    suppression["disabled_since"] if suppression else None
                ),
                "alert_suppressed": bool(
                    (suppression and suppression["suppressed"])
                    or (declared_off and declared_off["suppressed"])
                ),
                # config-I8719 — the DECLARED-off half. Deliberately its own
                # set of fields rather than reusing `producer_disabled`: that
                # one means "a live probe found the producing trigger
                # DISABLED", and a declared-off row has no such probe. Two
                # senses of one word made a check unclearable once already
                # (config-I8105); these stay distinct.
                #
                # `console_state` resolves the row to a member of
                # observability-policy.md §8.3's closed vocabulary, so the
                # console renders a declared decision rather than inferring one
                # from an absent artifact. Only a SUPPRESSED row is DISABLED —
                # a declared-off row whose resolution went stale or whose
                # milestone was reached is back under normal freshness, and
                # saying DISABLED there would be a claim the monitor is no
                # longer acting on.
                "declared_off": declared_off is not None,
                "declared_off_suppressed": bool(
                    declared_off and declared_off["suppressed"]
                ),
                "declared_off_since": (
                    declared_off["since"] if declared_off else None
                ),
                "days_declared_off": (
                    declared_off["days_declared_off"] if declared_off else None
                ),
                "declared_off_reason": (
                    declared_off["reason"] if declared_off else None
                ),
                "declared_off_owning_item": (
                    declared_off["owning_item"] if declared_off else None
                ),
                "declared_off_clears_when_milestone": (
                    declared_off["clears_when_milestone"]
                    if declared_off else None
                ),
                "declared_off_milestone_status": (
                    declared_off["milestone_status"] if declared_off else None
                ),
                "declared_off_resolution_age_hours": (
                    declared_off["resolution_age_hours"]
                    if declared_off else None
                ),
                "console_state": (
                    "DISABLED"
                    if (declared_off and declared_off["suppressed"])
                    else None
                ),
                # config-I7622 — True: no instance has EVER existed under this
                # key, so the row is a producer-birth gap and does not page.
                # False: it has been written before and this is a real miss.
                # None/absent: not probed, or the probe could not answer — which
                # is NOT evidence of either, and leaves the page path intact.
                "never_written": (never_written_by_id or {}).get(
                    spec.artifact_id
                ),
                # config-I7509: the console must be able to say WHOSE decision
                # is holding this row quiet past the 14-day clock. A suppressed
                # row with no named owner is the latch case and reads
                # differently to an operator.
                "pause_owner": (
                    suppression.get("pause_owner") if suppression else None
                ),
                "pause_owner_url": (
                    suppression.get("pause_owner_url") if suppression else None
                ),
                "artifact_id": spec.artifact_id,
                "owner_repo": spec.owner_repo,
                "severity": spec.severity,
                "severity_dynamic": spec.artifact_id in coerced_ids,
                "consecutive_miss_runs": miss_counts.get(spec.artifact_id, 0),
                "cadence": spec.cadence,
                "canonical_key": result.canonical_key,
                "state": result.state,
                "reason": result.reason,
                "last_modified": (
                    result.last_modified.isoformat()
                    if result.last_modified is not None
                    else None
                ),
                "sla_violated_by_minutes": result.sla_violated_by_minutes,
                "recovery_substituted": result.recovery_substituted,
                "issue_filed_url": issue_filed_by_id.get(spec.artifact_id),
                # config-I9336 — driver attribution, EVERY row, every sweep.
                "driver": driver["driver"],
                "driver_consequence": driver["consequence"],
                "driver_clears_when": driver["clears_when"],
                # config-I9336 — open-episode identity, None when this row
                # is not currently alerting. `episode_state` +
                # `episode_opened_at` are the round-trip pair
                # `_load_prev_episode_state` reads back next sweep.
                "episode_key": (episode or {}).get("key"),
                "episode_state": (episode or {}).get("state"),
                "episode_opened_at": (episode or {}).get("opened_at"),
            }
        )
    return {
        "run_at": now.isoformat(),
        "alerts_enabled": ALERTS_ENABLED,
        "n_entries": len(rows),
        # §7.4a clause (c) — rendered alongside the rows so the console
        # never has to fetch a second artifact to answer "is this class
        # paging about a component or about an undrained backlog?".
        "execution_loop": execution_loop,
        # config-I7606 — the coverage of the suppression mechanism itself, on
        # the same surface as the rows it governs, for the same reason: a
        # consumer can now tell "quiet because its producer is deliberately
        # off" from "loud because nothing declared what produces it" without
        # joining two artifacts by hand.
        "suppression_coverage": coverage,
        "results": rows,
    }


def _serialize_heartbeat(
    pairs: list[tuple[ArtifactSpec, CheckResult]],
    now: datetime,
    started_at_epoch: float,
) -> dict[str, Any]:
    """Build the ``heartbeat.json`` payload. Plan §3 invariant 9: the
    monitor monitors itself; substrate-health-check daily SSM watches
    this artifact's freshness. Self-registered in
    ``ARTIFACT_REGISTRY.yaml`` as ``freshness_monitor_heartbeat``."""
    counts: dict[str, int] = {
        "fresh": 0,
        "stale": 0,
        "missing": 0,
        "probe_failed": 0,
        "grace_period": 0,
    }
    for _spec, result in pairs:
        counts[result.state] = counts.get(result.state, 0) + 1

    return {
        "last_run": now.isoformat(),
        "alerts_enabled": ALERTS_ENABLED,
        "duration_seconds": round(time.time() - started_at_epoch, 3),
        "n_entries_checked": len(pairs),
        "counts": counts,
    }


def _put_json(s3_client: Any, bucket: str, key: str, payload: dict) -> None:
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


# ── Per-cycle completion rollup (L249 consumer) ─────────────────────────────


def _serialize_cycle_verdicts(
    pairs: list[tuple[ArtifactSpec, CheckResult]], now: datetime
) -> dict[str, Any]:
    """Roll the per-artifact probe results up into one completion verdict
    per execution cycle, via :func:`cycle_completion`.

    The registry walk covers EVERY cadence in a single 15-min pass, so the
    grouping by ``(cadence, cycle_label)`` is mandatory — a single rollup
    over the mixed-cadence ``pairs`` would conflate the Saturday, weekday,
    EOD and continuous cycles into one meaningless verdict. ``weekday_sf``
    and ``eod_sf`` share a date-shaped label, so the cadence is part of the
    group key to keep them distinct.

    ``cycle_completion`` itself filters to ``severity="critical"`` rows; a
    group whose cadence has no critical artifacts rolls up vacuously
    complete (``n_required=0``).
    """
    groups: dict[tuple[str, str], list[tuple[ArtifactSpec, CheckResult]]] = (
        defaultdict(list)
    )
    for spec, result in pairs:
        _, label = resolve_current_cycle(spec, now)
        groups[(spec.cadence, label)].append((spec, result))

    verdicts = []
    for (cadence, label), grp in sorted(groups.items()):
        v = cycle_completion(grp, cycle_label=label)
        verdicts.append(
            {
                "cadence": cadence,
                "cycle_label": label,
                "state": v.state,
                "complete": v.complete,
                "n_required": v.n_required,
                "n_satisfied": v.n_satisfied,
                "missing": v.missing,
                "stale": v.stale,
                "probe_failed": v.probe_failed,
                "grace_period": v.grace_period,
                "reason": v.reason,
            }
        )
    return {"run_at": now.isoformat(), "verdicts": verdicts}


def _emit_cycle_metrics(cw_client: Any, verdict_payload: dict[str, Any]) -> None:
    """Emit one ``ArtifactFreshnessCycleComplete`` datapoint per cadence
    (1.0 complete / 0.0 not), in :data:`CW_NAMESPACE`.

    Dimensioned by ``Cadence`` ONLY — a stable, low-cardinality set
    (``{saturday_sf, weekday_sf, eod_sf, continuous}``) that a CW alarm can
    bind to. The per-cycle ``cycle_label`` is recorded in the S3 artifact,
    NOT a metric dimension: a label is high-cardinality (a new value every
    week/day) and would make the metric both unalarmable and costly.
    """
    metric_data = [
        {
            "MetricName": "ArtifactFreshnessCycleComplete",
            "Dimensions": [{"Name": "Cadence", "Value": v["cadence"]}],
            "Value": 1.0 if v["complete"] else 0.0,
            "Unit": "Count",
        }
        for v in verdict_payload["verdicts"]
    ]
    if metric_data:
        # Cadence set is ≤4 → one call, well under CW's 1000-metric cap.
        cw_client.put_metric_data(Namespace=CW_NAMESPACE, MetricData=metric_data)


# ── The execution-loop number (observability-policy §7.4a clause c) ─────────


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2:
        return round(ordered[mid], 2)
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 2)


def _alert_class(spec: ArtifactSpec) -> str:
    """The alert class this page belongs to. Cadence-scoped: a stable,
    low-cardinality set a CW alarm can bind to, and the axis on which
    "detection works, execution does not" actually differs."""
    return f"{_FLOW_NAME}.{spec.cadence}"


def _summarize_execution_loop(
    pairs: list[tuple[ArtifactSpec, CheckResult]],
    page_records: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    """Per alert class: the fraction of pages fired against an ALREADY-OPEN
    owning item, and the median age of that item at page time.

    A high fraction is a backlog-drain defect, not a monitoring defect —
    monitoring worked. §7.4a forbids the reachable remedy (raising the
    class's cooldown to quiet it), because that deletes the only evidence
    that the execution loop is not closing.

    Every class present in this pass's registry walk gets a row, whether or
    not it paged, plus an aggregate ``freshness-monitor.all``. A class that
    fired nothing renders ``pages: 0`` — the denominator is published with
    the ratio so 0.0 can never be read as "healthy" when it means "nothing
    measured". ``lookup_degraded`` counts pages whose owning-item search
    could not run: those pages are NOT in the denominator of a claim they
    could not support.
    """
    classes: dict[str, dict[str, Any]] = {}
    for spec, _result in pairs:
        classes.setdefault(_alert_class(spec), {"pages": 0, "owned": 0,
                                                "degraded": 0, "ages": []})
    classes.setdefault(f"{_FLOW_NAME}.all", {"pages": 0, "owned": 0,
                                             "degraded": 0, "ages": []})
    for rec in page_records:
        for key in (rec["alert_class"], f"{_FLOW_NAME}.all"):
            bucket = classes.setdefault(
                key, {"pages": 0, "owned": 0, "degraded": 0, "ages": []}
            )
            bucket["pages"] += 1
            if rec.get("lookup_degraded"):
                bucket["degraded"] += 1
            if rec.get("owning_item_number") is not None:
                bucket["owned"] += 1
                bucket["ages"].append(float(rec.get("owning_item_age_days") or 0.0))
    return {
        "run_at": now.isoformat(),
        "policy_clause": "OB-7.4a-recurring-pages-are-measured-against-the-execution-loop",
        "classes": {
            name: {
                "pages": b["pages"],
                "pages_with_open_owning_item": b["owned"],
                "fraction_with_open_owning_item": (
                    round(b["owned"] / b["pages"], 4) if b["pages"] else 0.0
                ),
                "median_owning_item_age_days_at_page": _median(b["ages"]),
                "pages_with_degraded_lookup": b["degraded"],
            }
            for name, b in sorted(classes.items())
        },
    }


def _emit_execution_loop_metrics(
    cw_client: Any, payload: dict[str, Any]
) -> None:
    """Emit the §7.4a numbers to CW, dimensioned by ``AlertClass``. Emitted
    on EVERY run, zero included — the absence of these datapoints means the
    emitter is dead, never that the loop is healthy."""
    metric_data = []
    for name, stats in payload["classes"].items():
        dims = [{"Name": "AlertClass", "Value": name}]
        metric_data.extend([
            {"MetricName": "AlertPages", "Dimensions": dims,
             "Value": float(stats["pages"]), "Unit": "Count"},
            {"MetricName": "AlertPagesWithOpenOwningItem", "Dimensions": dims,
             "Value": float(stats["pages_with_open_owning_item"]), "Unit": "Count"},
            {"MetricName": "AlertPagesWithOpenOwningItemFraction",
             "Dimensions": dims,
             "Value": float(stats["fraction_with_open_owning_item"]), "Unit": "None"},
            {"MetricName": "OwningItemAgeDaysAtPageMedian", "Dimensions": dims,
             "Value": float(stats["median_owning_item_age_days_at_page"]),
             "Unit": "None"},
            {"MetricName": "OwningItemLookupDegraded", "Dimensions": dims,
             "Value": float(stats["pages_with_degraded_lookup"]), "Unit": "Count"},
        ])
    # CW caps a single PutMetricData at 1000 datapoints; classes are
    # cadence-scoped (≤6) × 5 metrics, so one call always suffices.
    for i in range(0, len(metric_data), 900):
        cw_client.put_metric_data(
            Namespace=CW_NAMESPACE, MetricData=metric_data[i:i + 900]
        )


def _emit_cycle_verdict_error(stage: str) -> None:
    """Emit one ``ArtifactFreshnessCycleVerdictError`` datapoint (Value=1.0) so a
    swallowed cycle-verdict rollup failure has an alarmable recording surface
    (config#1236) — not only the absence/staleness of ``cycle_verdict.json``.

    Dimensioned by ``Stage`` (``serialize_or_s3_write`` / ``cw_metric_emit``) to
    locate the failing step. Best-effort: the emit itself is trapped so this
    error-signal path can never sink the monitor (and a missing PutMetricData
    grant — the very thing it might be reporting — won't raise here).
    """
    try:
        boto3.client("cloudwatch").put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "ArtifactFreshnessCycleVerdictError",
                    "Dimensions": [{"Name": "Stage", "Value": stage}],
                    "Value": 1.0,
                    "Unit": "Count",
                }
            ],
        )
    except Exception as exc:  # noqa: BLE001 — error-signal emit must never raise
        logger.warning(
            "failed to emit ArtifactFreshnessCycleVerdictError[%s] (non-fatal): %s",
            stage, exc,
        )


# ── Auto-remediation dispatch (config#1240) ─────────────────────────────────
#
# Promote the monitor from alert-only to alert+heal. On a confirmed miss past
# grace, an artifact carrying a `recovery:` spec gets its backfill primitive
# DISPATCHED. The monitor reads the spec; it is NEVER hardcoded per artifact.


# (resolve_current_cycle is imported at module top alongside the other
# substrate entry points.)


def _resolve_recovery_params(
    params: dict[str, Any] | None, spec: ArtifactSpec, now: datetime
) -> dict[str, Any]:
    """Resolve ``{date}``/``{trading_day}``/``{cycle_label}`` placeholders in
    a recovery spec's ``params`` against the CURRENT MISS's cycle.

    The miss is for *this* cycle, so the backfill must target this cycle's
    trading day — NOT "today" (a Saturday-cron miss probed Monday must still
    backfill the Saturday cycle). We reuse the substrate's cycle resolution so
    the date the backfill targets is exactly the date the probe checked.
    Non-string param values pass through untouched.
    """
    if not params:
        return {}
    cycle_tick, cycle_label = resolve_current_cycle(spec, now)
    iso = cycle_tick.date().isoformat()
    # Match nousergon_lib.artifact_freshness._format_key (I8240): {date} is
    # the cycle/calendar axis; {trading_day} is last_closed_trading_day.
    trading_day = last_closed_trading_day(cycle_tick).isoformat()
    resolved: dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, str):
            resolved[k] = v.format(
                date=iso, trading_day=trading_day, cycle_label=cycle_label,
            )
        else:
            resolved[k] = v
    return resolved


def _recovery_marker_key(spec: ArtifactSpec, now: datetime) -> str:
    """In-progress dedup marker key for the current (artifact, cycle-window).

    Keyed by the SAME per-cycle label the alert dedup uses, so a backfill is
    dispatched at most once per cycle per artifact regardless of how many
    15-min polls observe the still-missing artifact before the heal lands.
    """
    _, label = resolve_current_cycle(spec, now)
    return f"{RECOVERY_MARKER_PREFIX}{spec.artifact_id}/{label}.json"


def _recovery_already_dispatched(
    s3_client: Any, spec: ArtifactSpec, now: datetime
) -> bool:
    """True if a dispatch marker for this (artifact, cycle) exists AND is
    within the cooldown window — i.e. a recovery is already in-flight and the
    artifact simply hasn't reappeared yet, so we must NOT re-dispatch.

    A marker older than the cooldown is treated as stale (the prior heal
    evidently failed) and dispatch is allowed again. A HEAD failure other than
    404 is treated as "assume dispatched" (fail-closed) so a transient S3 blip
    can't trigger a dispatch storm.
    """
    key = _recovery_marker_key(spec, now)
    try:
        resp = s3_client.head_object(Bucket=REGISTRY_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001 — classify by error code
        code = str(
            getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        )
        status = getattr(exc, "response", {}).get("ResponseMetadata", {}).get(
            "HTTPStatusCode", 0
        )
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False  # no marker → first dispatch for this cycle
        # Any other error (403/500/network) → fail-closed: assume in-flight.
        logger.warning(
            "recovery marker HEAD for %s failed (%s) — assuming dispatched "
            "to avoid a re-dispatch storm",
            spec.artifact_id, exc,
        )
        return True
    lm = resp.get("LastModified")
    if lm is None:
        return True
    age_min = (now - lm).total_seconds() / 60.0
    return age_min < RECOVERY_COOLDOWN_MINUTES


def _write_recovery_marker(
    s3_client: Any, spec: ArtifactSpec, now: datetime, payload: dict[str, Any]
) -> None:
    """Persist the in-progress marker so subsequent polls dedup against it."""
    key = _recovery_marker_key(spec, now)
    _put_json(s3_client, REGISTRY_BUCKET, key, payload)


def _dispatch_recovery(
    aws_clients: dict[str, Any],
    spec: ArtifactSpec,
    recovery: dict[str, Any],
    now: datetime,
) -> None:
    """Dispatch the recovery primitive named by the spec.

    ``type: step_function`` → ``stepfunctions.start_execution`` with the
    resolved params JSON as input. ``type: lambda`` → ``lambda.invoke``
    (Event/async) with the resolved params as the payload. Lazily-created
    clients are cached in ``aws_clients`` so a pass dispatching several
    recoveries shares one client per service.
    """
    rtype = recovery.get("type")
    target = recovery.get("target")
    resolved_params = _resolve_recovery_params(recovery.get("params"), spec, now)

    if rtype == "step_function":
        sf = aws_clients.get("stepfunctions")
        if sf is None:
            sf = boto3.client("stepfunctions")
            aws_clients["stepfunctions"] = sf
        sf.start_execution(
            stateMachineArn=target,
            input=json.dumps(resolved_params, default=str),
        )
    elif rtype == "lambda":
        lam = aws_clients.get("lambda")
        if lam is None:
            lam = boto3.client("lambda")
            aws_clients["lambda"] = lam
        lam.invoke(
            FunctionName=target,
            InvocationType="Event",  # async fire-and-forget; the next probe verifies
            Payload=json.dumps(resolved_params, default=str).encode("utf-8"),
        )
    else:
        raise ValueError(f"unknown recovery.type={rtype!r} for {spec.artifact_id}")


def _maybe_dispatch_recovery(
    s3_client: Any,
    aws_clients: dict[str, Any],
    spec: ArtifactSpec,
    recovery: dict[str, Any] | None,
    result: CheckResult,
    now: datetime,
) -> bool:
    """Auto-remediation entry point — mirror of :func:`_maybe_alert`.

    Returns ``True`` iff a dispatch was actually performed this pass. Fires
    only when ALL of:
      - a ``recovery:`` spec exists for the artifact;
      - the same confirmed-miss gate the alert path uses holds
        (``state ∈ {missing, stale}`` past SLA — ``probe_failed`` is NOT
        auto-healed: a broken probe means the monitor is blind, not that the
        artifact is absent, so blind-dispatching a backfill is unsafe);
      - no in-flight dispatch marker within the cooldown (dedup);
      - :data:`RECOVERY_DISPATCH_ENABLED` (OBSERVE-mode gate — logs the
        would-dispatch and writes NO marker / calls NO AWS when off).

    On dispatch, an in-progress marker is written so the next 15-min poll
    against the still-missing artifact dedups instead of re-dispatching.
    """
    if recovery is None:
        return False
    if result.state not in ("missing", "stale"):
        return False
    if result.sla_violated_by_minutes == 0:
        return False  # still within SLA grace — same gate as _maybe_alert

    if not RECOVERY_DISPATCH_ENABLED:
        logger.info(
            "OBSERVE-mode (recovery): would dispatch %s recovery for %s "
            "(state=%s) target=%s",
            recovery.get("type"), spec.artifact_id, result.state,
            recovery.get("target"),
        )
        return False

    # Dedup: a recovery already in-flight for this (artifact, cycle) → skip.
    if _recovery_already_dispatched(s3_client, spec, now):
        logger.info(
            "recovery for %s already dispatched this cycle (deduped)",
            spec.artifact_id,
        )
        return False

    # Write the marker BEFORE dispatching so a dispatch that succeeds but
    # whose marker-write would have failed can't loop; and so a crash between
    # dispatch and marker-write errs toward not-re-dispatching. The marker is
    # the dedup source of truth.
    marker = {
        "artifact_id": spec.artifact_id,
        "dispatched_at": now.isoformat(),
        "state": result.state,
        "recovery_type": recovery.get("type"),
        "target": recovery.get("target"),
    }
    _write_recovery_marker(s3_client, spec, now, marker)

    _dispatch_recovery(aws_clients, spec, recovery, now)
    logger.info(
        "DISPATCHED %s recovery for %s (state=%s) target=%s",
        recovery.get("type"), spec.artifact_id, result.state,
        recovery.get("target"),
    )
    return True


# ── Freshness-critical → overseer drain dispatch (config-I3282 phase 1) ─────


def _drain_dispatch_in_cooldown(s3_client: Any, now: datetime) -> bool:
    """True if a drain-dispatch marker exists within the cooldown window.

    Global (not per-artifact): one drain consumes the WHOLE intake queue, so
    every critical paged before (or shortly after) the launch is covered by
    the same run. Fail-closed on non-404 HEAD errors, mirroring
    :func:`_recovery_already_dispatched` — a transient S3 blip must not
    trigger a dispatch storm.
    """
    try:
        resp = s3_client.head_object(
            Bucket=REGISTRY_BUCKET, Key=DRAIN_DISPATCH_MARKER_KEY
        )
    except Exception as exc:  # noqa: BLE001 — classify by error code
        code = str(
            getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        )
        status = getattr(exc, "response", {}).get("ResponseMetadata", {}).get(
            "HTTPStatusCode", 0
        )
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            return False  # no marker → first dispatch
        logger.warning(
            "drain-dispatch marker HEAD failed (%s) — assuming in-flight to "
            "avoid a dispatch storm", exc,
        )
        return True
    lm = resp.get("LastModified")
    if lm is None:
        return True
    age_min = (now - lm).total_seconds() / 60.0
    return age_min < DRAIN_DISPATCH_COOLDOWN_MINUTES


def _maybe_dispatch_drain(
    s3_client: Any,
    aws_clients: dict[str, Any],
    candidate_ids: list[str],
    now: datetime,
) -> bool:
    """Event-time overseer drain dispatch for this sweep's critical pages
    (config-I3282 phase 1). Called ONCE per pass with every artifact whose
    critical page fired and whose declared lane admits dispatch.

    Returns ``True`` iff a dispatch was performed. Fires only when ALL of:
      - ``candidate_ids`` is non-empty;
      - :data:`DRAIN_DISPATCH_ENABLED` (OBSERVE-mode gate — logs the
        would-dispatch and writes NO marker / calls NO AWS when off);
      - no dispatch marker within the cooldown window (global dedup — one
        drain covers the whole queue).

    The router invoke is async (``Event``): the overseer-dispatcher owns
    verdict handling and escalation (P1 + loud page) end-to-end, exactly as
    it does for saturday-sf-watch-dispatcher's M2 dispatches. The marker is
    written BEFORE the invoke (same crash-ordering argument as the recovery
    marker: err toward not-re-dispatching).
    """
    if not candidate_ids:
        return False

    if not DRAIN_DISPATCH_ENABLED:
        logger.info(
            "OBSERVE-mode (drain-dispatch): would dispatch playbook=%s via %s "
            "for %d critical page(s): %s",
            DRAIN_PLAYBOOK, OVERSEER_DISPATCHER_FUNCTION,
            len(candidate_ids), sorted(candidate_ids),
        )
        return False

    if _drain_dispatch_in_cooldown(s3_client, now):
        logger.info(
            "drain dispatch in cooldown — %d critical page(s) (%s) covered by "
            "the in-flight/recent drain",
            len(candidate_ids), sorted(candidate_ids),
        )
        return False

    _put_json(
        s3_client, REGISTRY_BUCKET, DRAIN_DISPATCH_MARKER_KEY,
        {
            "dispatched_at": now.isoformat(),
            "artifact_ids": sorted(candidate_ids),
            "playbook": DRAIN_PLAYBOOK,
        },
    )

    lam = aws_clients.get("lambda")
    if lam is None:
        lam = boto3.client("lambda")
        aws_clients["lambda"] = lam
    lam.invoke(
        FunctionName=OVERSEER_DISPATCHER_FUNCTION,
        InvocationType="Event",  # async; the router owns escalation
        Payload=json.dumps({
            "playbook": DRAIN_PLAYBOOK,
            "payload": {"trigger": "freshness-critical", "is_drill": "false"},
        }).encode("utf-8"),
    )
    logger.info(
        "DISPATCHED overseer playbook=%s via %s for %d critical page(s): %s",
        DRAIN_PLAYBOOK, OVERSEER_DISPATCHER_FUNCTION,
        len(candidate_ids), sorted(candidate_ids),
    )
    return True


# ── Alerting (gated on ALERTS_ENABLED) ──────────────────────────────────────


_ALERTING_STATES = frozenset({"missing", "stale", "probe_failed"})


# ── Episode identity (dedup fix — run-keyed vs. episode-keyed) ──────────────
#
# The digest's dedup key used to be ``freshness_digest_{today}_{fingerprint
# of alerting artifact_ids}`` — hashing the SET fixed the 17-pages-for-one-
# cause defect (config-I7713), but baking ``today`` into the key meant an
# UNCHANGED standing condition still minted a brand-new key every UTC
# midnight and re-paged, forever, for as long as it lasted (measured live:
# ``director_retro_trend`` paged CRITICAL again on 2026-08-14 for the exact
# incident already paged on 2026-08-12, 30209 → 33089 minutes past SLA,
# nothing about the condition having changed). Same defect class as the
# router-canary fix in claude-code-config-PR221 (~430 identical ERROR pages
# over 18 days) — run-keyed rather than episode-keyed.
#
# An EPISODE is the unbroken span during which one artifact stays in one
# alerting state. Its key must be stable for that whole span and change
# only when the episode genuinely ends:
#
#   - ``stale``: the freshest instance's own ``last_modified`` IS the
#     episode's identity — it does not move while nobody writes a newer
#     instance, and a newer instance landing (recovery, or a later relapse
#     with a NEW last_modified) is exactly what should mint a new key. No
#     persisted state needed; derived straight from this sweep's probe.
#   - ``missing`` / ``probe_failed``: the probe found no instance at all, so
#     there is nothing to anchor a signature on. These carry a sentinel
#     timestamp — minted once, the sweep the state is FIRST observed — and
#     round-tripped forward via ``check_results.json`` for as long as the
#     SAME state persists (:func:`_load_prev_episode_state`), the identical
#     "no new state surface" convention ``consecutive_miss_runs`` /
#     ``issue_filed_url`` already use. The moment the state changes —
#     including an interlude where it went ``stale``/``fresh`` in between —
#     the sentinel resets, so ``missing`` → ``stale`` → ``missing`` opens a
#     genuinely NEW episode rather than reusing the first one's (now
#     permanently-suppressing, since the digest publishes with
#     ``dedup_window_min=None``) marker.


def _episode_signature(
    spec: ArtifactSpec,
    result: CheckResult,
    prev_episode: dict[str, str] | None,
    now: datetime,
) -> dict[str, str] | None:
    """This row's open-episode identity, or ``None`` when it is not
    alerting (no episode is open). See the module note above.

    Returns ``{"key": ..., "state": ..., "opened_at": ...}``. ``state`` +
    ``opened_at`` are exactly the two fields :func:`_load_prev_episode_state`
    reads back next sweep to keep a ``missing``/``probe_failed`` sentinel
    stable across the whole streak.
    """
    if result.state not in _ALERTING_STATES:
        return None
    if result.state == "stale" and result.last_modified is not None:
        opened_at = result.last_modified.isoformat()
    else:
        prev = prev_episode or {}
        if prev.get("state") == result.state and prev.get("opened_at"):
            opened_at = prev["opened_at"]
        else:
            opened_at = now.isoformat()
    return {
        "key": f"freshness_{spec.artifact_id}_{result.state}_{opened_at}",
        "state": result.state,
        "opened_at": opened_at,
    }


# ── Owning-item resolution (observability-policy §7.4a) ─────────────────────
#
# alpha-engine-config-I7326 / nous-ergon-ops-PR661. On 2026-08-14 08:00 UTC
# this monitor paged CRITICAL for `director_retro` after 13 consecutive
# misses. At that moment alpha-engine-config-I6562 — which had root-caused
# the exact condition nine days earlier — was open, ungated and unexecuted,
# and three further open P1s (#6155, #6345, #6747) described the same
# condition. The page named none of them, and its `after_consecutive_miss_
# runs=13` would have read identically had no fix ever been filed.
#
# Three obligations, one per registered §7.4a clause:
#
#   (a) OB-7.4a-a-page-names-its-owning-tracked-item — before emitting, the
#       artifact_id is resolved against the OPEN issues of ISSUES_REPO and
#       the owning item's number, title, age and priority go in the body.
#       The monitor already tracked `issue_filed_url` (config#2055), but
#       that join is self-referential: it only ever knew about issues THIS
#       Lambda filed, which is precisely why #6562 — filed by a human — was
#       invisible. The resolution below is the union of the two.
#   (b) OB-7.4a-escalation-tracks-the-owning-items-age-not-the-miss-count —
#       see `_maybe_alert`: where an owning item is open, the warning→
#       critical promotion is a function of that item's age against its
#       priority SLA. Where none is open, the miss-count ladder stands AND
#       crossing it CREATES the owning item (see `_escalation_threshold`) —
#       creation IS the escalation, not a second rung above it.
#   (c) OB-7.4a-recurring-pages-are-measured-against-the-execution-loop —
#       see `_summarize_execution_loop`.
#
# NOT SUPPRESSION. Nothing here removes a record. A row whose page is not
# delivered still carries its true state, its full owning-item block and its
# console row into check_results.json, exactly as config-I6570's producer
# suppression does. No cooldown or grace window is widened by this path;
# `test_cooldown_constants_are_not_a_noise_remedy` pins that.

# Read-only GitHub search over ISSUES_REPO. Reuses the SSM PAT and the
# egress path `_file_escalation_issue` already uses — no new grant, no new
# secret, so a code-only deploy is sufficient.
OWNING_ITEM_LOOKUP_ENABLED = (
    os.environ.get("FRESHNESS_OWNING_ITEM_LOOKUP", "true").lower() == "true"
)
_OWNING_ITEM_TIMEOUT_SEC = int(
    os.environ.get("OWNING_ITEM_LOOKUP_TIMEOUT_SEC", "5")
)
# MEASURED 2026-08-14: the deployed function's timeout is 120s. A per-query
# timeout alone does not bound this phase — the query CAP times the per-query
# timeout is the real worst case, and 24 x 5s would eat the whole function.
# A Lambda timeout is a HARD FAIL that halts the sweep, so the join is bounded
# by wall clock as well as by count, and blowing the budget degrades the
# REMAINING rows (recorded, still paging) instead of killing the pass.
OWNING_ITEM_LOOKUP_MAX_SECONDS = float(
    os.environ.get("OWNING_ITEM_LOOKUP_MAX_SECONDS", "25")
)
# GitHub's search API allows 30 authenticated requests/minute. A sweep with
# many simultaneous misses must not spend the whole budget and then fail the
# rest opaquely: past this cap the remaining rows resolve DEGRADED with an
# explicit reason, which is a recorded fact rather than a silent absence.
OWNING_ITEM_LOOKUP_MAX_QUERIES = int(
    os.environ.get("OWNING_ITEM_LOOKUP_MAX_QUERIES", "24")
)
# Age at which an open item of each priority stops being "in progress" and
# starts being the actionable fact. A P1 open nine days escalates because it
# is a P1 open nine days.
PRIORITY_SLA_DAYS = {"P0": 1, "P1": 3, "P2": 14, "P3": 30}
DEFAULT_PRIORITY_SLA_DAYS = int(
    os.environ.get("OWNING_ITEM_DEFAULT_SLA_DAYS", "7")
)
_PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
# Cap on how many sibling items a single page names, so one page can never
# become a wall. The full candidate set is still written to
# check_results.json — this trims the DELIVERED view, never the record.
_OWNING_ITEM_MEMBERS_IN_BODY = 5


def _new_lookup_state() -> dict[str, Any]:
    """Per-pass owning-item lookup state: one SSM PAT read, one query
    budget, one per-artifact result cache."""
    return {
        "pat": None, "pat_error": None, "queries": 0, "cache": {},
        # config-I7622 — started on the FIRST lookup, not here. This state is
        # built before the probe loop, which took 68.5s over 146 rows in the
        # 2026-08-19T15:41:32Z sweep; a wall clock started at construction was
        # therefore already 40s past a 25s budget before the first GitHub query
        # ran, and every row resolved `lookup_time_budget_exhausted` regardless
        # of how few misses there were. Measured that sweep: 2 pages, 2 degraded
        # lookups, on a pass with 11 confirmed misses — the budget was measuring
        # the probe loop it was never meant to bound.
        "started_at": None,
    }


def _unresolved(reason: str) -> dict[str, Any]:
    """A resolution that could not be made. `degraded=True` is what the
    page prints as `owning_item=unknown owning_item_lookup=degraded` and
    what the execution-loop metric counts — an unreachable tracker must
    still page, and must never look like 'no owning item exists'."""
    return {
        "resolved": False,
        "degraded": True,
        "degraded_reason": reason,
        "owning_item": None,
        "members": [],
        "n_candidates": 0,
    }


def _read_github_pat() -> str:
    """SSM-sourced GitHub PAT — the same parameter and client construction
    `_file_escalation_issue` has always used (IAM-reuse convention)."""
    return boto3.client(
        "ssm", region_name=os.environ.get("AWS_REGION", "us-east-1")
    ).get_parameter(Name=GH_PAT_SSM, WithDecryption=True)["Parameter"]["Value"]


def _cached_github_pat(state: dict[str, Any]) -> str | None:
    """One SSM read per pass. A failure is sticky for the pass and carries
    its reason into every subsequent `_unresolved`."""
    if state.get("pat"):
        return state["pat"]
    if state.get("pat_error"):
        return None
    try:
        state["pat"] = _read_github_pat()
        return state["pat"]
    except Exception as exc:  # noqa: BLE001 — degrades to "owning item unknown"; recording surfaces: this WARNING, the row's owning_item_lookup_degraded_reason, the OwningItemLookupDegraded CW metric
        state["pat_error"] = f"pat_read_failed: {type(exc).__name__}: {exc}"
        logger.warning(
            "owning-item lookup (§7.4a) cannot authenticate — pages will "
            "carry owning_item=unknown for this pass: %s", state["pat_error"],
        )
        return None


def _github_search_open_issues(term: str, pat: str) -> list[dict]:
    """One quoted-phrase search over ISSUES_REPO's OPEN issues. Raises on
    failure — the caller owns the degrade."""
    query = f'repo:{ISSUES_REPO} is:issue is:open "{term}"'
    url = (
        "https://api.github.com/search/issues?per_page=20&sort=created"
        "&order=asc&q=" + urllib.parse.quote(query, safe="")
    )
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {pat}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "freshness-monitor",
        },
    )
    with urllib.request.urlopen(req, timeout=_OWNING_ITEM_TIMEOUT_SEC) as resp:
        payload = json.loads(resp.read())
    items = payload.get("items") or []
    return [i for i in items if isinstance(i, dict) and i.get("number")]


def _artifact_id_variants(artifact_id: str) -> list[str]:
    """The spellings a human or a machine might use for one artifact_id.

    The verbatim id is what the registry row and a machine-filed item use.
    The hyphen/space variants exist because a HUMAN writes the condition in
    prose — #6562's title says `director-retro-judge`, which an exact
    `director_retro` match misses entirely, and missing it is the measured
    defect that motivated the variants.
    """
    out = [artifact_id]
    for variant in (artifact_id.replace("_", "-"), artifact_id.replace("_", " ")):
        if variant not in out:
            out.append(variant)
    return out


def _dedated_key(canonical_key: str) -> str:
    """``canonical_key`` with date-shaped path segments removed.

    ``predictor/2026-08-25/self_test.json`` → ``predictor/self_test.json``.
    Used as a RELEVANCE signal only (an item that names the S3 path instead
    of the artifact_id), never as a search term — it costs a query and
    almost never appears verbatim in a title.
    """
    segments = [
        s for s in (canonical_key or "").split("/")
        if s and not s[:1].isdigit()
    ]
    return "/".join(segments)


def _search_terms(artifact_id: str, canonical_key: str) -> list[str]:
    """Search terms derived from the alert's own §7.2 signature.

    The artifact_id and its prose variants — and NOTHING ELSE.

    config-I8680: this used to also emit the canonical key's bare first path
    segment (`trades`, `predictor`, `backtest`). GitHub's issue search is
    full-text, so a single common word returned twenty-to-thirty open issues
    that had nothing to do with the artifact, and `_rank_key` — which scores
    no relevance at all — then handed ownership to whichever of them was the
    oldest P0. Measured 2026-08-26: `open_orders_latest` was reported as
    owned by alpha-engine-config#4500, "Groom/alert-drain boxes run egress
    proxy v2.0.0 without auto-redaction", whose body matches neither
    `open_orders` nor `trades/`.

    That is not merely a cosmetic misattribution. `_alert_decision` replaces
    the miss-count ladder with the owning item's AGE whenever an item
    resolves, so a wrongly-attributed 30-day-old P0 makes
    `age_days >= sla_days` trivially true and the row pages CRITICAL on its
    FIRST confirmed miss — with `WARNING_ESCALATION_RUNS` bypassed. #4500
    also carries `gate:dependency`, so its age only grows: the misattribution
    was a PERMANENT critical-on-first-miss pin.

    The path prefix is not lost, only demoted: it survives as a relevance
    signal via :func:`_dedated_key`, where it can confirm an already-matched
    candidate but can never be the sole basis for one.
    """
    return _artifact_id_variants(artifact_id)


def _is_relevant(
    issue: dict, artifact_id: str, canonical_key: str,
) -> bool:
    """Whether this issue may own ``artifact_id`` AT ALL (config-I8680).

    The bar is deliberately concrete: the artifact_id — in one of its prose
    variants — or the de-dated S3 key appears in the issue's title or body.
    An item that merely came back from a full-text search is NOT a candidate.

    This is a FILTER, not a rank term, because a relevance score that only
    reorders still lets an irrelevant item own the cause whenever it is the
    only result. When nothing passes, `_resolve_owning_item` returns
    `owning_item=None` and the miss-count ladder stays in force — which is
    the correct clock for a condition that is genuinely undiagnosed.
    """
    haystack = f"{issue.get('title') or ''}\n{issue.get('body') or ''}".lower()
    for variant in _artifact_id_variants(artifact_id):
        if variant.lower() in haystack:
            return True
    dedated = _dedated_key(canonical_key)
    return bool(dedated) and dedated.lower() in haystack


def _priority_of(issue: dict) -> str | None:
    for label in issue.get("labels") or []:
        name = label.get("name") if isinstance(label, dict) else str(label)
        if name in PRIORITY_SLA_DAYS:
            return name
    return None


def _age_days(created_at: str | None, now: datetime) -> float:
    if not created_at:
        return 0.0
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return round(max((now - created).total_seconds(), 0.0) / 86400.0, 2)


def _summarize_issue(
    issue: dict, now: datetime, known_artifact_ids: set[str],
) -> dict[str, Any]:
    priority = _priority_of(issue)
    haystack = f"{issue.get('title') or ''}\n{issue.get('body') or ''}"
    return {
        "number": int(issue["number"]),
        "url": issue.get("html_url"),
        "title": (issue.get("title") or "")[:160],
        "priority": priority,
        "sla_days": PRIORITY_SLA_DAYS.get(priority, DEFAULT_PRIORITY_SLA_DAYS),
        "age_days": _age_days(issue.get("created_at"), now),
        "created_at": issue.get("created_at"),
        # Specificity: how many OTHER registry artifacts this item also
        # claims. An item naming four stale artifacts is over-broad relative
        # to one naming exactly this artifact's cause.
        "n_artifacts_named": sum(
            1 for aid in known_artifact_ids if aid and aid in haystack
        ),
    }


# Sentinel for "names no registry artifact at all" in `_rank_key`'s
# specificity term. Any value above the largest plausible `n_artifacts_named`
# works; it is named rather than inline so the intent (sort LAST) cannot be
# misread as a count.
_UNSPECIFIC_RANK = 10 ** 6


def _rank_key(item: dict[str, Any]) -> tuple:
    """Deterministic cause-ownership order (§7.4a's many-items-one-cause
    rule): highest priority first, then the NARROWEST claim, then the
    oldest — the item that has gone unexecuted longest owns the cause, and
    a broader item that merely contains this artifact is a member, never
    the owner."""
    return (
        _PRIORITY_RANK.get(item["priority"], 9),
        # config-I8680: this was `... if n else 99`, which sorted an item
        # naming ZERO registry artifacts — i.e. maximally irrelevant — into
        # the same bucket as every other zero-namer, collapsing the whole
        # sort to "oldest of the highest priority wins". Zero now sorts
        # LAST, where it belongs. With `_is_relevant` filtering upstream a
        # zero-namer can still appear (it may match on the de-dated S3 key
        # rather than the artifact_id), so the branch is live, not dead.
        item["n_artifacts_named"] or _UNSPECIFIC_RANK,
        item["created_at"] or "",
        item["number"],
    )


def _resolve_owning_item(
    artifact_id: str,
    canonical_key: str,
    known_artifact_ids: set[str],
    now: datetime,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the OPEN tracked item that owns this artifact's condition.

    Union of two sources, which is the point: the self-filed
    `issue_filed_url` this monitor already carried, plus a search over
    ISSUES_REPO that also sees items a HUMAN filed. Returns a resolution
    dict — never None, never raises. A search that cannot run degrades to
    `owning_item=None, degraded=True` WITH a reason: the page still fires,
    and the degradation is itself a recorded, alarmable fact.
    """
    if not OWNING_ITEM_LOOKUP_ENABLED:
        return _unresolved("lookup_disabled")
    cached = state["cache"].get(artifact_id)
    if cached is not None:
        return cached

    pat = _cached_github_pat(state)
    if pat is None:
        resolution = _unresolved(state.get("pat_error") or "pat_unavailable")
        state["cache"][artifact_id] = resolution
        return resolution

    by_number: dict[int, dict] = {}
    errors: list[str] = []
    ran_any = False
    n_filtered = 0
    for term in _search_terms(artifact_id, canonical_key):
        if state["queries"] >= OWNING_ITEM_LOOKUP_MAX_QUERIES:
            errors.append("lookup_budget_exhausted")
            break
        if state.get("started_at") is None:
            state["started_at"] = time.monotonic()
        elif (
            time.monotonic() - state["started_at"]
            >= OWNING_ITEM_LOOKUP_MAX_SECONDS
        ):
            errors.append("lookup_time_budget_exhausted")
            break
        state["queries"] += 1
        try:
            for issue in _github_search_open_issues(term, pat):
                # config-I8680 relevance gate. Full-text search is a
                # RECALL mechanism; it decides what to look at, never what
                # owns the cause. An item that does not name this artifact
                # (or its de-dated S3 key) is discarded here rather than
                # ranked, because a rank can still crown an irrelevant item
                # when it is the only result.
                if not _is_relevant(issue, artifact_id, canonical_key):
                    n_filtered += 1
                    continue
                by_number.setdefault(int(issue["number"]), issue)
            ran_any = True
        except Exception as exc:  # noqa: BLE001 — a single failed query degrades that term only; recording surfaces: `errors` → degraded_reason on the page and in check_results.json, plus the OwningItemLookupDegraded CW metric
            errors.append(f"{term}: {type(exc).__name__}: {exc}")

    if not ran_any:
        resolution = _unresolved("; ".join(errors) or "no_query_ran")
        state["cache"][artifact_id] = resolution
        return resolution

    ranked = sorted(
        (_summarize_issue(i, now, known_artifact_ids) for i in by_number.values()),
        key=_rank_key,
    )
    if not ranked:
        logger.info(
            "owning-item lookup: %s matched %d open issue(s) by full-text "
            "search, none of which name the artifact or its key — no owning "
            "item; the miss-count ladder stays in force (config-I8680)",
            artifact_id, n_filtered,
        )
    resolution = {
        "resolved": True,
        # A PARTIAL search still resolved something, but the operator is
        # told the candidate set may be incomplete.
        "degraded": bool(errors),
        "degraded_reason": "; ".join(errors) or None,
        "owning_item": ranked[0] if ranked else None,
        "members": ranked[1:],
        "n_candidates": len(ranked),
        # How many full-text hits the relevance gate discarded. Recorded so
        # a gate that is silently rejecting everything is visible as a
        # number rather than as an absence (principles.md §2.7).
        "n_filtered_irrelevant": n_filtered,
    }
    state["cache"][artifact_id] = resolution
    return resolution


def _merge_self_filed(
    resolution: dict[str, Any], self_filed_url: str | None,
) -> dict[str, Any]:
    """Union the self-filed marker into the resolution. The search is the
    richer source (it carries priority and age), so a self-filed URL only
    matters when the search found nothing or could not run — otherwise the
    monitor would report a stale marker as the owner while a human's item
    outranks it."""
    if not self_filed_url or resolution.get("owning_item"):
        return resolution
    try:
        number = int(str(self_filed_url).rstrip("/").rsplit("/", 1)[-1])
    except (ValueError, IndexError):
        return resolution
    merged = dict(resolution)
    merged["owning_item"] = {
        "number": number,
        "url": self_filed_url,
        "title": "(self-filed extended-staleness escalation)",
        "priority": "P1",
        "sla_days": PRIORITY_SLA_DAYS["P1"],
        # Unknown from the marker alone; 0.0 keeps the age-driven ladder
        # from escalating on a fact we do not have. The miss-count ladder
        # is not disabled by a self-filed item (see `_maybe_alert`).
        "age_days": 0.0,
        "created_at": None,
        "n_artifacts_named": 1,
        "source": "self_filed_marker",
    }
    merged["n_candidates"] = merged.get("n_candidates", 0) + 1
    return merged


def _owning_item_body_fragment(owning: dict[str, Any] | None) -> str:
    """The §7.4a clause-(a) half of the notification body. Exactly one of
    three shapes, and never silence — an absent lookup and an absent item
    are different facts and must not read the same."""
    if owning is None:
        return "owning_item=unknown owning_item_lookup=not_attempted"
    item = owning.get("owning_item")
    if item is None:
        if owning.get("degraded"):
            return (
                "owning_item=unknown owning_item_lookup=degraded "
                f"owning_item_lookup_reason={owning.get('degraded_reason')!r}"
            )
        return "owning_item=none owning_item_lookup=ok"
    frag = (
        f"owning_item=#{item['number']} "
        f"owning_item_priority={item.get('priority') or 'unlabelled'} "
        f"owning_item_age_days={item['age_days']} "
        f"owning_item_sla_days={item['sla_days']} "
        f"owning_item_url={item.get('url')} "
        f"owning_item_title={item.get('title')!r}"
    )
    members = owning.get("members") or []
    if members:
        shown = members[:_OWNING_ITEM_MEMBERS_IN_BODY]
        frag += (
            " owning_item_members="
            + ",".join(f"#{m['number']}" for m in shown)
            + f" owning_item_members_total={len(members)}"
        )
    if owning.get("degraded"):
        frag += (
            " owning_item_lookup=degraded "
            f"owning_item_lookup_reason={owning.get('degraded_reason')!r}"
        )
    return frag


# ── Driver attribution — a closed set, computed on EVERY evaluation ─────────
#
# An episode-keyed page must earn the "fixed, not muted" reading by saying
# more per message, not less: what is wrong, WHY (from a closed set the
# probe already resolves to — never a free-text guess), what happens if
# nobody acts, and the concrete S3 key that clears it. Computed for every
# spec/result pair every sweep — including rows that never page — so the
# field can be watched building toward an incident rather than only
# appearing the moment one fires.
_DRIVER_UNATTRIBUTED = "unattributed"


def _evaluate_driver(spec: ArtifactSpec, result: CheckResult) -> dict[str, str]:
    """Attribute this row's freshness state from the CLOSED set
    :data:`CheckResult.state` already resolves to — ``missing`` (no
    instance found at all), ``stale`` (an instance exists but predates the
    freshness floor), ``probe_failed`` (an S3 client error other than 404 —
    403/network/endpoint fault; the monitor itself may be blind). Every
    other resolved state (``fresh`` / ``grace_period``) is not late.

    The ``else`` branch is terminal, not a plausible default: it is reached
    only if ``CheckResult.state`` gains a value this function was not
    updated for, and says so explicitly rather than silently guessing one
    of the known drivers.
    """
    if result.state == "missing":
        return {
            "driver": "missing_no_instance",
            "consequence": (
                f"downstream consumers of {spec.artifact_id!r} keep reading "
                f"a key nothing has written this incident; nothing "
                f"self-heals — the producer in {spec.owner_repo!r} must "
                f"write it"
            ),
            "clears_when": (
                f"an object lands at s3://{spec.s3_bucket}/{result.canonical_key}"
            ),
        }
    if result.state == "stale":
        return {
            "driver": "stale_instance_past_sla",
            "consequence": (
                f"consumers of {spec.artifact_id!r} keep serving the last "
                f"instance found; it falls further behind the freshness "
                f"floor every sweep the producer in {spec.owner_repo!r} "
                f"does not write a newer one"
            ),
            "clears_when": (
                f"a newer object lands under the {spec.artifact_id!r} key "
                f"family (canonical probe target: "
                f"s3://{spec.s3_bucket}/{result.canonical_key})"
            ),
        }
    if result.state == "probe_failed":
        return {
            "driver": "probe_error",
            "consequence": (
                f"the monitor itself cannot see {spec.artifact_id!r} right "
                f"now — freshness is UNKNOWN, not confirmed fresh; treat as "
                f"an incident in {spec.owner_repo!r} (or in the monitor's "
                f"own S3 access) until the probe succeeds again"
            ),
            "clears_when": (
                f"a HEAD/LIST of s3://{spec.s3_bucket}/{result.canonical_key} "
                f"succeeds again"
            ),
        }
    if result.state in ("fresh", "grace_period"):
        return {
            "driver": "not_applicable",
            "consequence": "none — row is not late",
            "clears_when": "n/a",
        }
    return {
        "driver": _DRIVER_UNATTRIBUTED,
        "consequence": (
            f"state={result.state!r} falls outside this function's closed "
            f"set — the driver classifier is stale against "
            f"CheckResult.state and needs updating; treat the gap itself "
            f"as an incident"
        ),
        "clears_when": "unknown until the classifier is updated",
    }


def _alert_decision(spec: ArtifactSpec, result: CheckResult, now: datetime,
                    consecutive_miss_runs: int = 0,
                    producer_suppression: dict[str, Any] | None = None,
                    owning: dict[str, Any] | None = None,
                    never_written: bool | None = None,
                    declared_off: dict[str, Any] | None = None,
                    episode: dict[str, str] | None = None,
                    driver: dict[str, str] | None = None) -> dict[str, Any] | None:
    """Decide whether this probe result belongs on the operator page, and
    return the decision. Returns ``None`` when it does not.

    **This function performs no I/O.** It carries every gate the per-artifact
    publish path used to apply inline — states, SLA grace, OBSERVE mode,
    producer suppression, probe-failure coercion, the config-I3086 warning
    ladder and the §7.4a owning-item escalation basis — and hands the surviving
    rows to :func:`_publish_digest`, which emits ONE page per sweep covering all
    of them (config-I7713).

    Splitting the decision from the delivery is what makes the rollup possible
    at all: while each row published itself, "one page per sweep" could only
    have been a cooldown, which suppresses facts rather than combining them.

    Only fires when:
      - ``result.state ∈ {missing, stale, probe_failed}``
      - ``result.sla_violated_by_minutes > 0`` for missing/stale (give
        the SLA grace window), OR ``probe_failed`` (no grace for
        broken probes — operator needs to know immediately)
      - :data:`ALERTS_ENABLED` is True
      - resolved severity is ``critical`` (``severity=warning`` rows are
        console-only via ``check_results.json`` — no SNS/Telegram)

    This row's per-artifact EPISODE identity (:func:`_episode_signature`)
    is threaded into the returned decision and folds into
    :func:`_digest_dedup_key` ⇒ the whole sweep's digest pages at most once
    per unbroken episode set, regardless of how many 15min probes or UTC
    day-rollovers occur while the same episodes stay open.
    """
    if result.state not in _ALERTING_STATES:
        return None

    # config-I8719 — the producer is off BY A RECORDED RULING that no live
    # probe can observe (a bypassed stage inside a still-enabled pipeline).
    #
    # Placed FIRST, above every other gate including the config-I3086 warning
    # ladder and the §7.4a `owning_item_age` basis, and that position is the
    # whole fix: `owning_item_age` promotes a warning row once its owning item
    # is old enough, and a declared-off row's owning item is old PRECISELY
    # BECAUSE the producer is deliberately off. The escalation got louder the
    # longer the declared state held — anti-correlated with the thing it exists
    # to measure. Returning here means an `owning_item_age` promotion is
    # structurally unreachable for a declared-off row, rather than suppressed
    # after the fact somewhere further down.
    #
    # This removes a PAGE, never a fact: the row still reaches
    # check_results.json with its true state, `declared_off: true`,
    # `console_state: "DISABLED"` and its age in days (see
    # `_serialize_check_results`).
    if declared_off and declared_off.get("suppressed"):
        logger.info(
            "declared-off (config-I8719): %s state=%s — %s (declared off %s, "
            "%d days; owning_item=%s; clears when milestone %s is reached); "
            "console-only, no SNS/Telegram, no escalation",
            spec.artifact_id, result.state, declared_off.get("reason"),
            declared_off.get("since"), declared_off.get("days_declared_off"),
            declared_off.get("owning_item"),
            declared_off.get("clears_when_milestone"),
        )
        return None

    # config-I6570 — the producing trigger is live-confirmed DISABLED, so this
    # miss is a deliberate switch-off rather than a producer failure. Placed
    # BEFORE the SLA and severity gates on purpose: severity describes what
    # the artifact's absence costs downstream, and that cost is unchanged by
    # WHY it is absent — only the page is. The row still reaches
    # check_results.json with its true state and `alert_suppressed: true`
    # (see `_serialize_check_results`), so this removes a page, never a fact.
    # Suppression lapses past PRODUCER_SUPPRESSION_MAX_DAYS.
    if producer_suppression and producer_suppression.get("suppressed"):
        logger.info(
            "producer-suppressed (config-I6570): %s state=%s — %s "
            "(disabled since %s, %d days); console-only, no SNS/Telegram",
            spec.artifact_id, result.state, producer_suppression["reason"],
            producer_suppression["disabled_since"],
            producer_suppression["days_disabled"],
        )
        return None

    # Substrate already filters fresh/grace; the SLA-grace filter
    # mirrors the substrate's clip-at-zero arithmetic for
    # missing/stale. probe_failed has no SLA — fire immediately.
    if result.state in ("missing", "stale") and result.sla_violated_by_minutes == 0:
        return None

    # config-I7603 d2 / config-I7622 — an artifact NO CODE HAS EVER WRITTEN is a
    # registry row registered ahead of its producer, not a producer failure. Its
    # absence is correct, so it does not page and does not climb the miss ladder.
    # It is NOT silenced: `never_written: true` reaches check_results.json and
    # the row gets its own standing block in the digest (see `_compose_digest`),
    # so the registry's debt stays visible without being an alarm. `None` means
    # the probe could not answer and the row keeps the page path.
    if never_written is True:
        logger.info(
            "never-written (config-I7622): %s state=%s — no instance has ever "
            "existed under this key, so this is a registry/producer-birth gap, "
            "not an SLA miss; reported in the digest, not paged",
            spec.artifact_id, result.state,
        )
        return None

    if not ALERTS_ENABLED:
        logger.info(
            "OBSERVE-mode: would alert on %s state=%s reason=%r",
            spec.artifact_id, result.state, result.reason,
        )
        return None

    # Probe failures route to critical (the monitor itself is broken);
    # missing/stale respect the spec's severity. Plan §3 invariant 6.
    severity = "critical" if result.state == "probe_failed" else spec.severity

    # config-I3086 warning escalation: a warning row confirmed-missing for
    # WARNING_ESCALATION_RUNS consecutive evaluated sweeps stops being a
    # console-only fact and pages via the critical path. One cycle of
    # console-only is the designed noise floor; a PERSISTENT warning is an
    # incident nobody is looking at (the I3053 champion-feed staleness sat
    # on dashboard page 26 for days).
    #
    # §7.4a (I7326) supersedes the BASIS of that promotion whenever an
    # owning item is open: a miss count measures how long the artifact has
    # been absent, which stops being the actionable question the moment the
    # cause is known and filed. What is actionable then is that a diagnosed
    # defect has gone unexecuted for N days against its priority's SLA. The
    # miss-count ladder remains in force for an UNDIAGNOSED condition (no
    # owning item), where it is still the only clock available.
    owning_item = (owning or {}).get("owning_item")
    escalation_basis = "miss_count"
    if severity == "warning" and owning_item and owning_item.get("created_at"):
        escalation_basis = "owning_item_age"
        escalated = owning_item["age_days"] >= owning_item["sla_days"]
    else:
        escalated = (
            severity == "warning"
            and WARNING_ESCALATION_RUNS > 0
            and consecutive_miss_runs >= WARNING_ESCALATION_RUNS
        )
    if escalated:
        severity = "critical"
        logger.info(
            "warning-escalation: %s paging via critical path (basis=%s, "
            "consecutive_miss_runs=%d, owning_item=%s)",
            spec.artifact_id, escalation_basis, consecutive_miss_runs,
            (owning_item or {}).get("number"),
        )

    # Registry convention: severity=warning means dashboard/console-only —
    # the operator surface is check_results.json + this page, not ops-health
    # Telegram. Critical (and probe_failed, coerced above) pages via SNS +
    # flow-doctor. Aligns with ARTIFACT_REGISTRY comments ("dashboard-only")
    # and the fleet notification consolidation arc (config#1740 / #1724).
    if severity == "warning":
        logger.info(
            "console-only (severity=warning): %s state=%s — surfaced in "
            "check_results.json, no SNS/Telegram",
            spec.artifact_id, result.state,
        )
        return None

    driver = driver or _evaluate_driver(spec, result)
    episode_key = (episode or {}).get("key")
    episode_opened_at = (episode or {}).get("opened_at")
    episode_age_minutes = None
    if episode_opened_at:
        try:
            opened_dt = datetime.fromisoformat(episode_opened_at)
            episode_age_minutes = int(
                max(0, (now - opened_dt).total_seconds() // 60)
            )
        except ValueError:
            episode_age_minutes = None

    # The per-artifact detail line. Extends the pre-digest per-artifact page
    # body (field-for-field) with the driver attribution (config-I9336):
    # what is wrong, WHY from a closed set, what happens if nobody acts, and
    # the concrete S3 key that clears it — so the answer is on the page, not
    # just the assignment.
    line = (
        f"artifact_id={spec.artifact_id} "
        f"owner_repo={spec.owner_repo} "
        f"state={result.state} "
        f"key={result.canonical_key} "
        f"sla_violated_by_minutes={result.sla_violated_by_minutes} "
        f"reason={result.reason} "
        f"driver={driver['driver']} "
        f"episode_open_since={episode_opened_at} "
        f"episode_age_minutes={episode_age_minutes} "
        f"if_unaddressed={driver['consequence']} "
        f"clears_when={driver['clears_when']}"
    )
    if escalated:
        line += (
            f" escalated_from=warning escalation_basis={escalation_basis}"
            f" after_consecutive_miss_runs={consecutive_miss_runs}"
        )
    line += " " + _owning_item_body_fragment(owning)

    return {
        "artifact_id": spec.artifact_id,
        "owner_repo": spec.owner_repo,
        "cadence": spec.cadence,
        "alert_class": _alert_class(spec),
        "state": result.state,
        "canonical_key": result.canonical_key,
        "sla_violated_by_minutes": result.sla_violated_by_minutes,
        "severity": severity,
        "escalated": escalated,
        "escalation_basis": escalation_basis,
        "consecutive_miss_runs": consecutive_miss_runs,
        "group_key": _cause_group_key(spec),
        "owning": owning,
        "owning_item": owning_item,
        "driver": driver["driver"],
        "driver_consequence": driver["consequence"],
        "driver_clears_when": driver["clears_when"],
        "episode_key": episode_key,
        "episode_opened_at": episode_opened_at,
        "episode_age_minutes": episode_age_minutes,
        "line": line,
    }


def _maybe_alert(spec: ArtifactSpec, result: CheckResult, now: datetime,
                 consecutive_miss_runs: int = 0,
                 producer_suppression: dict[str, Any] | None = None,
                 owning: dict[str, Any] | None = None,
                 never_written: bool | None = None,
                 declared_off: dict[str, Any] | None = None) -> bool:
    """Whether this row belongs on the sweep's page.

    Retained as the boolean face of :func:`_alert_decision` — it is the gate
    predicate the drain lane, the execution-loop records and a long tail of
    tests are written against, and that predicate did not change. What changed
    is that it no longer SENDS: since config-I7713 a sweep emits one grouped
    page via :func:`_publish_digest` rather than one message per artifact.
    """
    return _alert_decision(
        spec, result, now,
        consecutive_miss_runs=consecutive_miss_runs,
        producer_suppression=producer_suppression,
        owning=owning,
        never_written=never_written,
        declared_off=declared_off,
    ) is not None


# ── Digest rollup (config-I7713) ────────────────────────────────────────────
#
# Brian, 2026-08-19: "I should only get one if it points to a singular issue,
# instead i'm getting ~20. The single error should encompass all errors
# currently triggering."
#
# The monitor used to publish once per artifact per cadence window. That dedup
# is correct at the artifact level and useless at the operator level: on
# 2026-08-19T12:03:13Z a single sweep emitted 17 pages, and all 17 were one
# cause (78 registry rows carrying a weekday cadence over a once-weekly
# producer — alpha-engine-config-I7709). Seventeen true statements about one
# fact is not seventeen findings.
#
# So: ONE page per sweep, grouped by CAUSE. The grouping key is derived from
# what the registry already declares about a row's producer, in this order:
#
#   1. `produced_by` — the pipeline(s) that write it. Rows that go stale
#      together because one pipeline did not run share this key exactly.
#   2. `producer_trigger` — the schedule/rule/workflow, for rows with no
#      pipeline (the `continuous` tier).
#   3. `owner_repo` — the last resort, so a row with neither declaration still
#      lands in a named group rather than in a nameless remainder.
#
# Dedup is on the SET of open EPISODES (config-I9336), not the artifact and
# not the calendar day: the page re-fires when that set CHANGES — something
# joined, something's episode closed because a newer instance landed, or a
# row's driver changed (e.g. stale → probe_failed) — and stays quiet while
# the same episodes persist, including across UTC-midnight rollovers. The
# original config-I7713 shape baked ``now``'s date directly into the key,
# which re-paged an unchanged standing incident every day it continued
# (measured: ``director_retro_trend`` paged again 2026-08-14 for the same
# incident already paged 2026-08-12 — the exact defect this fixes). A
# cooldown would have suppressed the 17th page, or the 2nd day's page;
# episode-keying suppresses nothing that is still true and says everything
# that is, once per fact.


def _cause_group_key(spec: ArtifactSpec) -> str:
    """The cause this row's absence belongs to — see the module note above.

    Derived, never hand-kept: every input is already declared on the row, and a
    second copy of "what produces this" is the drift shape the registry's own
    cadence coupling exists to prevent.
    """
    pipelines = sorted(
        {
            entry.get("pipeline")
            for entry in (getattr(spec, "produced_by", None) or [])
            if isinstance(entry, dict) and entry.get("pipeline")
        }
    )
    if pipelines:
        return "pipeline:" + "+".join(pipelines)
    declared = getattr(spec, "producer_trigger", None)
    triggers = _declared_triggers(declared) if declared else ()
    if triggers:
        return "trigger:" + "+".join(sorted(triggers))
    return f"owner_repo:{spec.owner_repo}"


def _digest_dedup_key(
    decisions: list[dict[str, Any]],
    unproduced: list[str] | None = None,
) -> str:
    """One key per distinct SET of open per-artifact EPISODES
    (config-I9336) — deliberately NOT per calendar day.

    Hashing each decision's own :func:`_episode_signature` key (falling back
    to the bare ``artifact_id`` for a decision with no episode threaded, so
    the function degrades safely rather than raising) is what makes an
    unbroken standing condition dedup FOREVER (the digest publishes with
    ``dedup_window_min=None``) while it stays unbroken: the episode key for
    every member is stable across sweeps and across UTC-midnight, so the
    fingerprint is stable too. An artifact joining, an episode closing
    (newer instance landed) or a row's driver changing state all change at
    least one member's episode key, which changes the fingerprint and
    re-pages — because the situation genuinely changed, which is the only
    thing that is supposed to re-page.
    """
    ids = ",".join(
        sorted(
            f"{d['artifact_id']}={d.get('episode_key') or d['artifact_id']}"
            for d in decisions
        )
    )
    # The unproduced set is part of the condition: a registry row that becomes
    # never-written, or stops being, is a change worth re-stating once.
    ids += "|never_written:" + ",".join(sorted(unproduced or []))
    fingerprint = hashlib.sha256(ids.encode("utf-8")).hexdigest()[:16]
    return f"freshness_digest_{fingerprint}"


def _compose_digest(
    decisions: list[dict[str, Any]],
    now: datetime,
    never_written: dict[str, bool | None] | None = None,
) -> str:
    """The single page body: a one-line verdict, then one block per cause.

    Ordering is deterministic (critical groups first, then by group size, then
    by key) so the same condition renders the same way every time and a
    diff between two pages means something.
    """
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for decision in decisions:
        groups[decision["group_key"]].append(decision)

    unproduced = sorted(
        aid for aid, flag in (never_written or {}).items() if flag is True
    )
    n_critical = sum(1 for d in decisions if d["severity"] == "critical")
    header = (
        f"{len(decisions)} artifact(s) past SLA across {len(groups)} cause(s) "
        f"({n_critical} critical) — freshness sweep {now.isoformat()}"
    )
    # config-I9336 — every row in `decisions` is an OPEN episode as of THIS
    # sweep (recomputed fresh each pass, never carried incrementally), so a
    # standing condition opened on an earlier sweep is enumerated here every
    # time regardless of whether this sweep's dedup key actually sends a new
    # message — the ages make a multi-day episode visible even though the
    # digest itself is quiet while it persists.
    episode_ages = [
        d["episode_age_minutes"] for d in decisions
        if d.get("episode_age_minutes") is not None
    ]
    if episode_ages:
        header += f" · oldest open episode {max(episode_ages)}min"
    if unproduced:
        header += f" · {len(unproduced)} registry row(s) never written"

    def _group_rank(item: tuple[str, list[dict[str, Any]]]) -> tuple:
        key, members = item
        has_critical = any(m["severity"] == "critical" for m in members)
        return (0 if has_critical else 1, -len(members), key)

    blocks = [header]
    for key, members in sorted(groups.items(), key=_group_rank):
        members.sort(key=lambda m: (m["severity"] != "critical", m["artifact_id"]))
        worst = max(m["sla_violated_by_minutes"] for m in members)
        # The owning item is a property of the CAUSE, so it is named once for
        # the group rather than repeated on every member line.
        owning_fragment = _owning_item_body_fragment(
            next((m["owning"] for m in members if (m["owning"] or {}).get("owning_item")),
                 members[0]["owning"])
        )
        blocks.append(
            f"\n[{key}] {len(members)} artifact(s), worst "
            f"sla_violated_by_minutes={worst}\n  {owning_fragment}"
        )
        for member in members:
            blocks.append(f"  - {member['line']}")

    # Reported, never paged: these rows assert an SLA over a key nothing has
    # ever written, so their absence is correct and the fix is a registry or
    # producer-birth change, not an incident. Kept in the body so the debt is
    # visible on the same surface as the real misses (config-I7622).
    if unproduced:
        blocks.append(
            f"\n[never-written] {len(unproduced)} registry row(s) assert an SLA "
            f"over a key that has NEVER had an instance — a producer-birth gap, "
            f"not a producer failure. Not paged; fix is to land the producer or "
            f"retire the row:"
        )
        for aid in unproduced:
            blocks.append(f"  - artifact_id={aid} never_written=true")
    return "\n".join(blocks)


def _publish_digest(
    decisions: list[dict[str, Any]],
    now: datetime,
    never_written: dict[str, bool | None] | None = None,
) -> int:
    """Emit the sweep's single page. Returns the number of artifacts it covers
    (0 when there is nothing to say, which is silence by absence of condition —
    never by suppression).

    Severity is the MAX over the covered rows: a digest containing one critical
    row is a critical page, so grouping can never demote an alert. That is the
    invariant that makes the rollup safe — it changes how many messages are
    sent, never which conditions are reportable.
    """
    unproduced = sorted(
        aid for aid, flag in (never_written or {}).items() if flag is True
    )
    if not decisions and not unproduced:
        return 0

    severity = "critical" if any(
        d["severity"] == "critical" for d in decisions
    ) else "warning"
    body = _compose_digest(decisions, now, never_written)
    dedup_key = _digest_dedup_key(decisions, unproduced)

    publish_result = publish(
        body,
        severity=severity,
        source="freshness-monitor",
        dedup_key=dedup_key,
        dedup_window_min=None,
        telegram=False,
    )
    if publish_result.dedup_skipped:
        logger.info(
            "digest suppressed: the same %d-artifact condition set already "
            "paged today (%s)",
            len(decisions), publish_result.dedup_reason,
        )
        return len(decisions)

    notify_via_flow_doctor(
        body,
        silent=False,
        severity=severity,
        dedup_key=dedup_key,
        source="freshness-monitor",
        flow_name=_FLOW_NAME,
        topics=_FRESHNESS_TELEGRAM_TOPICS,
        db_basename=_DB_BASENAME,
        context={
            "n_artifacts": len(decisions),
            "n_groups": len({d["group_key"] for d in decisions}),
            "artifact_ids": sorted(d["artifact_id"] for d in decisions),
            "group_keys": sorted({d["group_key"] for d in decisions}),
        },
    )
    return len(decisions)


# ── Key-deliverable extended-staleness escalation (config#2055 Gap 2) ───────
#
# Even a confirmed critical page (above) is console/Telegram-only — nothing
# lands where Brian triages open work. A `severity=warning` row is worse:
# it's dashboard-only forever, no matter how long it persists (the exact
# "sat on dashboard page 26 for days" shape config-I3086 already fixed once
# for the *critical-page* threshold). This closes the same gap one rung
# higher: an artifact flagged `escalate_to_issue: true` that's been
# confirmed-missing for `ISSUE_ESCALATION_RUNS` consecutive daily sweeps
# gets a `[P1] gate:operator` issue filed directly on the Decision Queue
# (Brian's 2026-07-21 Option-A ruling on config#2055) — mirrors
# `overseer-dispatcher/index.py::_file_p1` byte-for-byte (same SSM-sourced
# PAT, same urllib POST, same repo target) rather than inventing a second
# GitHub-issue-filing implementation.


def _file_escalation_issue(
    artifact_id: str, owner_repo: str, miss_runs: int, anchor_id: str,
    threshold: int, owning_degraded_reason: str | None = None,
) -> dict:
    """File the extended-staleness P1 on ISSUES_REPO. Best-effort — the
    WARNING log + the returned dict (persisted into check_results.json's
    ``issue_filed_url`` for dedup) are the other recording surfaces.

    ``owning_degraded_reason`` is set when the §7.4a owning-item search
    could not run, so this filing may duplicate an item that already
    exists. It is filed anyway — the codebase-wide convention is to fail
    toward action, never toward silence — and the reason is written into
    the body so the duplicate is reconcilable rather than mysterious.
    """
    try:
        pat = _read_github_pat()
        body = "\n".join([
            f"`{artifact_id}` (owner: `{owner_repo}`) has been confirmed-missing/"
            f"stale for {miss_runs} consecutive daily freshness sweeps via its "
            f"liveness proxy `{anchor_id}` — well past the point a `severity="
            "warning` console-only page is enough; nobody has acted on it.",
            "",
            "**Summary:** Freshness monitor's extended-staleness escalation "
            f"(config#2055 Gap 2) fired for `{artifact_id}` — flagged "
            "key-deliverable, confirmed stale via its liveness proxy for "
            f"{miss_runs}+ consecutive daily sweeps with no operator action.",
            "**Ask:** Investigate why `{}` (or its liveness proxy `{}`) has "
            "stopped updating, and either fix the producer or acknowledge the "
            "staleness is expected right now.".format(artifact_id, anchor_id),
            "**Options:** A) Investigate the producer pipeline for "
            f"`{artifact_id}` / `{anchor_id}` now (recommended) B) Acknowledge "
            "as expected (e.g. a genuinely quiet promotion period) and push "
            "out the re-exam date",
            "**SOTA:** Every key-deliverable artifact's staleness is caught "
            "and triaged within its cadence window — no silent multi-week gaps "
            "(the config#2054 incident this escalation path exists to prevent).",
            "**Delta:** IS SOTA — no delta; this issue IS the triage step.",
            "**Consequence of no action:** This artifact (or the promotion "
            "pipeline behind it) may stay silently stalled indefinitely — the "
            "exact config#2054 failure shape, just past the point a console "
            "page alone was working.",
            "",
            f"- **Anchor (liveness proxy):** `{anchor_id}`",
            f"- **Consecutive confirmed-miss daily sweeps:** {miss_runs} "
            f"(threshold: {threshold})",
            "- **Filed via:** alpha-engine-freshness-monitor (config#2055 Gap 2, "
            "observability-policy §7.4a / I7326 — creating this item IS the "
            "escalation's first action)",
            *(
                [
                    "- **Owning-item search DEGRADED at filing time** "
                    f"(`{owning_degraded_reason}`) — this item may duplicate "
                    "an existing open item for the same condition. Reconcile "
                    "at the cause per §7.4a; do not silence either."
                ]
                if owning_degraded_reason else []
            ),
            "",
            "Closes-when: the underlying staleness is resolved (producer fixed "
            "and a fresh write confirmed) or explicitly acknowledged as "
            "expected for this period.",
        ])
        req = urllib.request.Request(
            f"https://api.github.com/repos/{ISSUES_REPO}/issues",
            data=json.dumps({
                "title": f"[P1] Freshness monitor: {artifact_id} stale for "
                         f"{miss_runs}+ consecutive sweeps — extended staleness",
                "body": body,
                "labels": ["P1", "gate:operator", "area:infrastructure"],
            }).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "freshness-monitor",
            },
        )
        with urllib.request.urlopen(req, timeout=_ISSUE_TIMEOUT_SEC) as resp:
            issue = json.loads(resp.read())
        logger.info(
            "config#2055 extended-staleness P1 filed for %s: %s",
            artifact_id, issue.get("html_url"),
        )
        return {"filed": True, "url": issue.get("html_url")}
    except Exception as exc:  # noqa: BLE001 — best-effort leg; recording surfaces: this WARNING, the returned dict
        logger.warning(
            "config#2055 extended-staleness P1 filing FAILED for %s: %s: %s",
            artifact_id, type(exc).__name__, exc,
        )
        return {"filed": False, "error": f"{type(exc).__name__}: {exc}"}


def _escalate_stale_key_deliverables(
    pairs: list[tuple[ArtifactSpec, CheckResult]],
    miss_counts: dict[str, int],
    escalate_to_issue_by_id: dict[str, bool],
    prev_issue_filed: dict[str, str],
    now: datetime,
    owning_by_id: dict[str, dict[str, Any]] | None = None,
) -> dict[str, str | None]:
    """For every ``escalate_to_issue``-flagged spec, file a Decision-Queue
    P1 once its confirmed-miss streak reaches :func:`_escalation_threshold`
    — the sweep on which the row first reaches the critical page path.
    Creating the owning item is the escalation's FIRST action (§7.4a).

    ``owning_by_id`` carries this pass's §7.4a resolutions. An artifact
    with an already-open owning item — including one a HUMAN filed, which
    the self-referential ``prev_issue_filed`` marker could never see — does
    NOT get a second item; the page names the existing one instead. Where
    the search DEGRADED, the item is filed anyway with the degradation
    recorded in its body: failing toward a possible duplicate is the
    cheaper error than failing toward an untracked condition.

    An ``event_driven`` row's OWN ``check_freshness`` result always
    short-circuits to ``fresh`` (see the event-driven short-circuit in
    ``nousergon_lib.artifact_freshness``), so its own ``miss_counts`` entry
    is always 0 — the miss-streak that actually matters is its
    ``liveness_via`` ANCHOR's, which this same sweep already computed (both
    rows are walked in the same registry pass, so the anchor's entry is
    always present in ``miss_counts`` by the time this runs). Non-
    ``event_driven`` flagged rows (should any exist in future) use their
    own miss streak directly.

    Returns the artifact_id -> issue URL map to persist into
    check_results.json (config#2055's dedup source of truth): sticky while
    the miss-streak persists, reset to ``None`` the moment it recovers so a
    FUTURE incident can file a fresh issue.
    """
    if not escalate_to_issue_by_id:
        return {}
    owning_by_id = owning_by_id or {}
    results_by_id = {spec.artifact_id: (spec, result) for spec, result in pairs}
    issue_filed_by_id: dict[str, str | None] = {}
    for artifact_id in escalate_to_issue_by_id:
        pair = results_by_id.get(artifact_id)
        if pair is None:
            continue
        spec, _result = pair
        anchor_id = (
            spec.liveness_via if spec.cadence == "event_driven" else spec.artifact_id
        )
        anchor_miss = miss_counts.get(anchor_id, 0)
        anchor_spec = (results_by_id.get(anchor_id) or (spec, None))[0]
        threshold = _escalation_threshold(anchor_spec)

        if anchor_miss == 0:
            # Recovered (or never missing) — clear any sticky marker so a
            # future incident can file a fresh issue.
            issue_filed_by_id[artifact_id] = None
            continue

        already_filed = prev_issue_filed.get(artifact_id)
        if already_filed:
            # Still stale, already escalated for THIS incident — carry
            # forward, don't re-file.
            issue_filed_by_id[artifact_id] = already_filed
            continue

        # §7.4a: an item that already OWNS this condition — whoever filed
        # it — is the escalation. Filing a second one is the overlapping-
        # items defect the clause exists to stop.
        owning = owning_by_id.get(artifact_id) or owning_by_id.get(anchor_id) or {}
        owning_item = owning.get("owning_item")
        if owning_item:
            logger.info(
                "§7.4a: not filing for %s — open owning item #%s (%s, %.1fd) "
                "already owns this condition; the page names it instead",
                artifact_id, owning_item["number"],
                owning_item.get("priority"), owning_item.get("age_days", 0.0),
            )
            issue_filed_by_id[artifact_id] = None
            continue

        if not ALERTS_ENABLED:
            logger.info(
                "OBSERVE-mode: would escalate %s to Decision Queue P1 "
                "(anchor=%s miss_runs=%d)", artifact_id, anchor_id, anchor_miss,
            )
            issue_filed_by_id[artifact_id] = None
            continue

        if anchor_miss < threshold:
            issue_filed_by_id[artifact_id] = None
            continue

        filed = _file_escalation_issue(
            artifact_id, spec.owner_repo, anchor_miss, anchor_id, threshold,
            owning.get("degraded_reason") if owning.get("degraded") else None,
        )
        issue_filed_by_id[artifact_id] = filed.get("url") if filed.get("filed") else None

    return issue_filed_by_id


# ── Handler ─────────────────────────────────────────────────────────────────


# ── Historical-mode probe ───────────────────────────────────────────────────
#
# Closes the gap surfaced 2026-05-28: the current-state probe answers
# "is the artifact present *now*?" but operators also need "did it
# land last weekend? the weekend before? are there gaps in the
# producer's history?" Filed per the same feedback memory
# [[feedback_observe_mode_unconditional_gates_govern_cutover]] —
# absence-of-artifact is the failure mode, and a single-cycle absence
# could be a false-positive (instance failure) where a multi-cycle gap
# is a real producer regression.
#
# Fires on a separate EventBridge cron (daily ~04:00 UTC, off-peak)
# via event={"mode": "historical"}. Writes
# s3://alpha-engine-research/_freshness_monitor/history.json which
# page 26 reads to surface per-artifact gap counts + per-row history
# expanders.
#
# Date resolution is intentionally simple (calendar-naive):
#   - saturday_sf: last N calendar Saturdays
#   - weekday_sf / eod_sf: last N Mon-Fri days
# NYSE holidays show up as false-positive "absent" days. Operators
# interpret them in context (or filter via the page 26 surface). When
# the holiday-aware backfill becomes worth the dependency lift, we
# can route via nousergon_lib.dates.


def _iter_sf_firing_dates(cadence: str, now: datetime, count: int) -> list[date]:
    """Return the N most recent SF firing dates (calendar) for the given
    cadence, newest-first. The SF cron's actual firing dates — Saturdays
    for saturday_sf, Mon-Fri for weekday_sf / eod_sf. Calendar-naive
    (NYSE holidays NOT skipped at this layer — observable false-positives
    for holiday-skipped firings surface as ❌ absent cells, which the
    operator interprets in context).
    """
    if count <= 0:
        return []
    today = now.date()
    dates: list[date] = []
    if cadence == "saturday_sf":
        d = today - timedelta(days=1)
        while len(dates) < count:
            if d.weekday() == 5:  # Saturday
                dates.append(d)
            d -= timedelta(days=1)
    elif cadence in {"weekday_sf", "eod_sf"}:
        d = today - timedelta(days=1)
        while len(dates) < count:
            if d.weekday() < 5:  # Mon-Fri
                dates.append(d)
            d -= timedelta(days=1)
    return dates


def _resolve_axis_dates(
    firing_dates: list[date], template: str, cadence: str,
) -> list[date]:
    """Translate SF firing dates to the date axis the s3_key_template
    actually uses. Two axes are supported:

      - ``{date}`` — calendar date (the SF firing date itself). Used by
        artifacts whose key reflects the SF firing identity, e.g.
        ``_weekly/{date}/manifest.json`` (the data manifest IS the
        Saturday firing receipt).
      - ``{trading_day}`` — NYSE trading day. Used by artifacts whose
        key reflects the trading-day the data refers to, NOT the SF
        firing date. Cadence-specific resolution:
          * saturday_sf: previous_trading_day(saturday) → typically Fri
            (the trading day whose close drove this Saturday's research).
          * weekday_sf: previous_trading_day(weekday) → the prior trading
            day's close (the AM SF fires before market open).
          * eod_sf: weekday itself → today's close (the EOD SF fires
            after market close, so today IS the trading_day).

    Per the system-wide ``now_dual()`` convention
    (``trading_day = last_closed_trading_day(now)``); see
    alpha-engine-docs/private/DATE_CONVENTIONS.md.

    Calendar-naive at the SF-firing layer above, but trading_day
    resolution uses ``nousergon_lib.trading_calendar.previous_trading_day``
    which IS NYSE-holiday-aware. So holiday-skipped firings still
    surface as cleanly-absent cells, but their resolved trading_day
    skips the holiday correctly.
    """
    if "{trading_day}" in template:
        if cadence == "eod_sf":
            return list(firing_dates)
        return [previous_trading_day(d) for d in firing_dates]
    return list(firing_dates)


def _iter_historical_cycle_dates(
    cadence: str, now: datetime, count: int, template: str = "",
) -> list[date]:
    """Return the N most recent cycle dates resolved to the axis the
    template uses. See ``_iter_sf_firing_dates`` +
    ``_resolve_axis_dates`` for the two-stage derivation.

    Backward compat: callers that omit ``template`` get calendar-axis
    resolution (the pre-2026-05-28 behavior). The historical-mode
    handler always passes the template.
    """
    firing_dates = _iter_sf_firing_dates(cadence, now, count)
    return _resolve_axis_dates(firing_dates, template, cadence)


def _format_historical_key(template: str, target_date: date) -> str:
    """Substitute date placeholders. Supports the same placeholders the
    substrate's _format_key handles: ``{date}``, ``{trading_day}``.
    ``{cycle_label}`` (fortnightly/quarterly buckets) is not historical-
    probable from a single date, so artifacts using it are skipped.
    """
    iso = target_date.isoformat()
    return template.format(date=iso, trading_day=iso)


def _probe_historical(
    s3_client: Any,
    spec: ArtifactSpec,
    cycle_dates: list[date],
) -> tuple[list[dict], bool]:
    """Probe the last N cycles' keys for one artifact. Returns
    ``(cycles, is_latest_pointer)``. Each ``cycles`` entry is a dict
    with ``date``, ``present``, ``size``, ``last_modified``.

    For artifacts whose ``s3_key_template`` is a latest-pointer (no
    ``{date}``/``{trading_day}`` placeholder), returns a single-entry
    list with the pointer's current state — historical sequence isn't
    observable from the pointer alone, so the page must render this
    distinction.
    """
    template = spec.s3_key_template
    has_date_placeholder = "{date}" in template or "{trading_day}" in template
    has_unsupported_placeholder = "{cycle_label}" in template

    if has_unsupported_placeholder:
        return [], False

    bucket = spec.s3_bucket or REGISTRY_BUCKET

    if not has_date_placeholder:
        # Latest-pointer: HEAD once, report current state only.
        try:
            resp = s3_client.head_object(Bucket=bucket, Key=template)
            return [{
                "date": "(latest)",
                "present": True,
                "size": resp["ContentLength"],
                "last_modified": resp["LastModified"].isoformat(),
            }], True
        except Exception as exc:  # noqa: BLE001 — record per-spec failures inline
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "unknown"))
            entry: dict = {"date": "(latest)", "present": False}
            if code not in {"404", "403", "NoSuchKey"}:
                entry["error_code"] = code
            return [entry], True

    # Date-templated: probe each historical date.
    cycles = []
    for d in cycle_dates:
        try:
            key = _format_historical_key(template, d)
        except (KeyError, IndexError) as exc:
            cycles.append({
                "date": d.isoformat(),
                "present": False,
                "error_code": f"template_render_failed:{type(exc).__name__}",
            })
            continue
        try:
            resp = s3_client.head_object(Bucket=bucket, Key=key)
            cycles.append({
                "date": d.isoformat(),
                "present": True,
                "size": resp["ContentLength"],
                "last_modified": resp["LastModified"].isoformat(),
            })
        except Exception as exc:  # noqa: BLE001
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", "unknown"))
            # 404 (object missing) AND 403 (object missing, no ListBucket) both
            # mean "not present" in S3 semantics — when the Lambda lacks
            # s3:ListBucket on the bucket, S3 returns 403 instead of 404 for
            # missing keys. Surface both as cleanly-absent (no error_code) so
            # the page 26 display doesn't show spurious "403 errors" on
            # legitimately-absent historical cycles. Other codes (500, etc.)
            # keep error_code for operator visibility.
            if code in {"404", "403", "NoSuchKey"}:
                cycles.append({"date": d.isoformat(), "present": False})
            else:
                cycles.append({
                    "date": d.isoformat(),
                    "present": False,
                    "error_code": code,
                })
    return cycles, False


def _handle_historical(
    s3_client: Any,
    now: datetime,
    started_at: float,
    lookback_overrides: dict | None,
) -> dict:
    """Walk the registry, probe each artifact's last N cycles, write
    ``history.json``. Same outer error handling as the current-state
    handler — load_registry raises on YAML parse / schema, per-spec
    failures are caught inline."""
    logger.info(
        "freshness-monitor invoked in HISTORICAL mode at %s",
        now.isoformat(),
    )
    lookback = dict(_DEFAULT_LOOKBACK)
    if lookback_overrides:
        lookback.update(lookback_overrides)

    specs = load_registry(s3_client, REGISTRY_BUCKET, REGISTRY_KEY)
    logger.info("loaded %d specs from registry", len(specs))

    artifacts_history: dict[str, dict] = {}
    skipped_unsupported = 0
    total_cycles_probed = 0
    for spec in specs:
        count = lookback.get(spec.cadence, 0)
        cycle_dates = _iter_historical_cycle_dates(
            spec.cadence, now, count, template=spec.s3_key_template,
        )
        cycles, is_latest_pointer = _probe_historical(s3_client, spec, cycle_dates)
        if not cycles and "{cycle_label}" in spec.s3_key_template:
            skipped_unsupported += 1
            continue
        total_cycles_probed += len(cycles)
        # Gap count: present=False entries in date-templated history.
        # Latest-pointers don't have a meaningful gap count (single point).
        if is_latest_pointer:
            gap_count = None
            continuous = (
                len(cycles) == 1 and cycles[0].get("present") is True
            )
        else:
            gap_count = sum(1 for c in cycles if not c.get("present"))
            continuous = (gap_count == 0 and len(cycles) > 0)
        artifacts_history[spec.artifact_id] = {
            "cadence": spec.cadence,
            "severity": spec.severity,
            "owner_repo": spec.owner_repo,
            "s3_key_template": spec.s3_key_template,
            "is_latest_pointer": is_latest_pointer,
            "lookback_cycles": count if not is_latest_pointer else 1,
            "gap_count": gap_count,
            "continuous": continuous,
            "history": cycles,
        }

    payload = {
        "generated_at": now.isoformat(),
        "lookback": lookback,
        "duration_seconds": round(time.time() - started_at, 2),
        "n_artifacts": len(artifacts_history),
        "n_cycles_probed": total_cycles_probed,
        "skipped_unsupported": skipped_unsupported,
        "artifacts": artifacts_history,
    }
    _put_json(s3_client, REGISTRY_BUCKET, HISTORY_KEY, payload)

    logger.info(
        "freshness-monitor HISTORICAL complete: %d artifacts, %d cycles probed, %d skipped, duration=%.2fs",
        len(artifacts_history),
        total_cycles_probed,
        skipped_unsupported,
        payload["duration_seconds"],
    )

    return {
        "mode": "historical",
        "n_artifacts": len(artifacts_history),
        "n_cycles_probed": total_cycles_probed,
        "skipped_unsupported": skipped_unsupported,
        "duration_seconds": payload["duration_seconds"],
    }


# ── Intraday-mode probe (config#1297) ───────────────────────────────────────


def _handle_intraday(s3_client: Any, now: datetime, started_at: float) -> dict:
    """30-min weekday-market-hours mini-rule, scoped to
    :data:`INTRADAY_ARTIFACT_IDS` only.

    Alerts/dispatches exactly like the daily full sweep (same
    :func:`_run_probe_pass`) but writes NO check_results/heartbeat/
    cycle_verdict — those full-registry dashboard surfaces are owned solely
    by the daily sweep (which itself checks these same two artifacts, just
    once a day), so a partial pass can never overwrite them with a
    2-artifact-only view.
    """
    logger.info(
        "freshness-monitor invoked in INTRADAY mode at %s (alerts_enabled=%s)",
        now.isoformat(), ALERTS_ENABLED,
    )

    (specs, recovery_by_id, critical_arms_by_id, _escalate_to_issue_by_id,
     remediation_by_id, producer_trigger_by_id, declared_off_input) = (
        load_registry_with_recovery(s3_client, REGISTRY_BUCKET, REGISTRY_KEY)
    )
    specs, _coerced = apply_dynamic_severity(s3_client, specs, critical_arms_by_id)
    intraday_specs = [s for s in specs if s.artifact_id in INTRADAY_ARTIFACT_IDS]
    missing_ids = INTRADAY_ARTIFACT_IDS - {s.artifact_id for s in intraday_specs}
    if missing_ids:
        logger.warning(
            "intraday mode: registry is missing expected artifact_id(s) %s",
            sorted(missing_ids),
        )

    # config-I6570 — restricted to the two intraday rows, so the mini-rule
    # makes at most two extra describe calls per 30min.
    intraday_triggers = {
        aid: t for aid, t in producer_trigger_by_id.items()
        if aid in INTRADAY_ARTIFACT_IDS
    }
    suppression_by_id = apply_producer_suppression(
        s3_client, intraday_triggers, now
    )
    # config-I8719 — restricted to the intraday rows for the same reason the
    # trigger suppression is: this path runs every 30min and must not do work
    # for rows it will not probe. No AWS call either way — the declaration and
    # its resolution both came from the registry object already read.
    declared_off_by_id = {
        aid: v for aid, v in resolve_declared_off(
            declared_off_input.get("rows") or {},
            declared_off_input.get("resolution"),
            now,
        ).items()
        if aid in INTRADAY_ARTIFACT_IDS
    }

    (pairs, alerted, dispatched, per_spec_exceptions, _miss_counts,
     _telemetry) = _run_probe_pass(
        s3_client, intraday_specs, recovery_by_id, now,
        remediation_by_id=remediation_by_id,
        suppression_by_id=suppression_by_id,
        declared_off_by_id=declared_off_by_id,
        # config-I9206 — one cache per INVOCATION, created here and discarded
        # with the pass. The intraday rule probes two rows, so it gains little;
        # it is threaded so there is exactly one way check_freshness is called.
        list_cache=_new_list_cache(),
    )

    duration_seconds = round(time.time() - started_at, 2)
    logger.info(
        "freshness-monitor INTRADAY complete: %s checked, %s alerted, %s dispatched, "
        "%s per-spec exceptions, duration=%.2fs",
        len(pairs), alerted, dispatched, per_spec_exceptions, duration_seconds,
    )

    return {
        "mode": "intraday",
        "n_entries_checked": len(pairs),
        "alerts_enabled": ALERTS_ENABLED,
        "alerted": alerted,
        "dispatched": dispatched,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": duration_seconds,
    }


# ── Probe pass (shared by the daily full sweep + the intraday mini-rule) ────


def _run_probe_pass(
    s3_client: Any,
    specs: list[ArtifactSpec],
    recovery_by_id: dict[str, dict],
    now: datetime,
    prev_miss_counts: dict[str, int] | None = None,
    remediation_by_id: dict[str, str] | None = None,
    suppression_by_id: dict[str, dict[str, Any]] | None = None,
    declared_off_by_id: dict[str, dict[str, Any]] | None = None,
    prev_issue_filed: dict[str, str] | None = None,
    prev_episode_state: dict[str, dict[str, str]] | None = None,
    list_cache: dict | None = None,
) -> tuple[
    list[tuple[ArtifactSpec, CheckResult]], int, int, int, dict[str, int],
    dict[str, Any],
]:
    """Walk ``specs``, probe each, dispatch confirmed-miss recoveries, and
    alert. Returns ``(pairs, alerted, dispatched, per_spec_exceptions,
    miss_counts, telemetry)`` where ``telemetry`` carries the §7.4a
    per-artifact owning-item resolutions, one record per page fired, and
    (config-I9336) every row's episode identity + driver attribution —
    computed for EVERY row, not only ones that page.

    ``prev_episode_state`` (config-I9336) carries the previous sweep's
    per-artifact open-episode sentinel (:func:`_load_prev_episode_state`);
    the intraday mini-rule passes ``None`` and gets a fresh sentinel every
    tick for any still-``missing``/``probe_failed`` intraday row, mirroring
    the existing ``prev_miss_counts=None`` intraday tradeoff below — the
    daily sweep (which also covers the two intraday artifacts) re-syncs the
    sentinel at least once every 24h.

    config-I3282: after the walk, one aggregated event-time drain dispatch
    fires for the pass's critical pages (see :func:`_maybe_dispatch_drain`
    for the eligibility + dedup semantics) — independently trapped like the
    per-artifact recovery dispatches, so it can never sink the pass.

    Shared verbatim by the daily full-registry sweep and the intraday
    mini-rule (config#1297) — the only difference between the two callers
    is which `specs` they pass in and what they do with the returned
    `pairs` (the full sweep serializes them to the shared dashboard
    surfaces; the intraday mini-rule only alerts, per `handler`'s docstring).

    ``prev_miss_counts`` (config-I3086) carries the previous sweep's
    per-artifact consecutive confirmed-miss counters; the returned
    ``miss_counts`` is this sweep's updated map (persisted into
    check_results.json by the daily caller — the intraday mini-rule
    passes None and gets all-zero counters, so it never escalates).
    """
    pairs: list[tuple[ArtifactSpec, CheckResult]] = []
    alerted = 0
    digest_decisions: list[dict[str, Any]] = []
    never_written_by_id: dict[str, bool | None] = {}
    dispatched = 0
    per_spec_exceptions = 0
    prev_miss_counts = prev_miss_counts or {}
    remediation_by_id = remediation_by_id or {}
    miss_counts: dict[str, int] = {}
    drain_candidates: list[str] = []
    # §7.4a (I7326) — one SSM PAT read and one query budget for the whole
    # pass; resolutions are per-artifact and cached inside the state.
    prev_issue_filed = prev_issue_filed or {}
    prev_episode_state = prev_episode_state or {}
    # config-I9206 — one LIST cache for this pass. Defaulted here rather than
    # required, so a caller that forgets it still gets ONE cache for the pass
    # instead of none; it is never module-level (see _new_list_cache).
    if list_cache is None:
        list_cache = _new_list_cache()
    lookup_state = _new_lookup_state()
    known_artifact_ids = {s.artifact_id for s in specs}
    owning_by_id: dict[str, dict[str, Any]] = {}
    page_records: list[dict[str, Any]] = []
    # config-I9336 — computed for EVERY row every sweep, not only ones that
    # page, so both fields can be watched building toward a page rather than
    # only appearing the moment one fires.
    episode_by_id: dict[str, dict[str, str] | None] = {}
    driver_by_id: dict[str, dict[str, str]] = {}
    # Per-pass cache of lazily-created SF/Lambda clients (shared across the
    # walk so a pass dispatching several recoveries reuses one client each).
    aws_clients: dict[str, Any] = {}
    for spec in specs:
        result, exc = _check_one(s3_client, spec, now, list_cache=list_cache)
        if exc is not None:
            per_spec_exceptions += 1
            logger.warning(
                "per-spec exception for %s: %s", spec.artifact_id, exc,
            )
        pairs.append((spec, result))

        # config-I9336 — driver attribution (closed set) on EVERY row, and
        # this row's open-episode identity (None when not alerting). Both
        # must run before `_alert_decision` below, which threads them into
        # the page line and the digest dedup key.
        driver_by_id[spec.artifact_id] = _evaluate_driver(spec, result)
        episode = _episode_signature(
            spec, result, prev_episode_state.get(spec.artifact_id), now,
        )
        episode_by_id[spec.artifact_id] = episode

        # config#1240 — auto-remediation. Attempt a dispatch on a confirmed
        # miss (independently trapped so a dispatch failure can NEVER sink the
        # monitor's primary alert/heartbeat deliverables). `mode: dispatch`
        # suppresses the page once a heal is dispatched this cycle; the default
        # `dispatch_and_page` pages AND heals (belt-and-braces).
        recovery = recovery_by_id.get(spec.artifact_id)
        did_dispatch = False
        try:
            did_dispatch = _maybe_dispatch_recovery(
                s3_client, aws_clients, spec, recovery, result, now,
            )
        except Exception as disp_exc:  # noqa: BLE001 — dispatch must not sink the pass
            logger.warning(
                "recovery dispatch for %s failed (non-fatal): %s",
                spec.artifact_id, disp_exc, exc_info=True,
            )
        if did_dispatch:
            dispatched += 1

        # config-I3086: consecutive confirmed-miss counter (0 on any
        # non-miss, including grace/fresh — a recovered artifact resets).
        miss_runs = (
            prev_miss_counts.get(spec.artifact_id, 0) + 1
            if _is_confirmed_miss(result) else 0
        )
        miss_counts[spec.artifact_id] = miss_runs

        suppress_page = (
            did_dispatch
            and isinstance(recovery, dict)
            and recovery.get("mode", "dispatch_and_page") == "dispatch"
        )
        # §7.4a clause (a): resolve the OPEN tracked item that owns this
        # condition BEFORE emitting, so the page can name it. Only for
        # confirmed misses — a fresh row has no condition to own. Trapped:
        # the tracker is not allowed to decide whether the operator is
        # paged, so an unreachable GitHub API degrades to
        # `owning_item=unknown` and the page still fires. Recording
        # surfaces: the WARNING below, `owning_item_lookup_degraded_reason`
        # on the check_results row, and the OwningItemLookupDegraded metric.
        # config-I7622 — only `missing` rows can be "never written", and only
        # a handful are missing in any sweep (3 of 146, measured 2026-08-19), so
        # this costs one bounded LIST each rather than a scan. Runs BEFORE the
        # owning-item join (config-I7730) because that join's budget gate reads
        # this answer — a never-written row cannot page, so it must not spend a
        # GitHub query.
        #
        # alpha-engine-config-I8810 — `spec.cadence == "event_driven"` is ALSO
        # eligible, independent of `result.state`. An event_driven row's OWN
        # `check_freshness` result is unconditionally `fresh`
        # (`nousergon_lib.artifact_freshness` short-circuits it before any age
        # floor is consulted — event_driven has no cadence to be stale against),
        # so `result.state == "missing"` can never be true for one and this
        # probe never ran. That is exactly how `thinktank_inst_ownership`
        # (`data/inst_ownership/latest.json`, shipped 2026-07-13, zero invoker)
        # stayed invisible: its row declares `liveness_via:
        # thinktank_coverage_ledger`, so its OWN key was never once checked for
        # existence by anything — the anchor's freshness was graded, this key's
        # was not. The probe itself is unchanged (`_prefix_has_ever_been_written`
        # still answers True/False/None on the artifact's own key), and the
        # downstream handling is unchanged too: `never_written=True` does not
        # page (`_alert_decision`'s first gate already excludes `fresh` from
        # `_ALERTING_STATES`, so an event_driven row was never going to page
        # regardless) but IS threaded into `check_results.json` per-row
        # (`_serialize_check_results`) and into the standing digest section
        # (`_compose_digest`'s `unproduced` list reads `never_written_by_id`
        # directly, not through `_alert_decision`) — so the gap renders instead
        # of reading as a healthy `fresh` row forever. `_NEVER_WRITTEN_PROBE_MAX`
        # still bounds the per-sweep cost.
        never_written: bool | None = None
        if (
            (result.state == "missing" or spec.cadence == "event_driven")
            and len(never_written_by_id) < _NEVER_WRITTEN_PROBE_MAX
        ):
            never_written = _prefix_has_ever_been_written(s3_client, spec, result)
            if never_written is not None:
                never_written = not never_written
            never_written_by_id[spec.artifact_id] = never_written

        owning: dict[str, Any] | None = None
        # config-I7730 — spend the budget where it can change an outcome.
        # `_resolve_owning_item` ran for EVERY confirmed miss at 2+ GitHub
        # searches each, against a 24-query cap. MEASURED 2026-08-19T17:02Z: 12
        # confirmed misses, of which 9 could not page under ANY owning-item
        # answer — 7 producer-suppressed, 2 never-written — so the cap was spent
        # on them and the ONE row that did page carried
        # `owning_item_lookup=degraded owning_item_lookup_reason=
        # 'lookup_budget_exhausted'` onto Brian's Telegram page. The join that
        # exists to explain a page was starved by rows that cannot produce one.
        #
        # These two gates are exactly `_alert_decision`'s own early returns,
        # BOTH of which sit above every use of `owning`, so skipping the lookup
        # for them cannot change any verdict — it only stops paying for an
        # answer nothing reads. The row still reaches check_results.json with
        # its true state; what it loses is an owning-item block on a page it was
        # never going to appear on.
        _suppressed = (suppression_by_id or {}).get(spec.artifact_id)
        # config-I8719 — a declared-off row is the third shape that cannot
        # page, and it is exactly `_alert_decision`'s first early return, so
        # skipping the lookup for it cannot change any verdict. Including it
        # here is not an optimisation: on 2026-08-19 the 24-query cap was spent
        # on rows that could not page and the ONE row that DID page carried
        # `lookup_budget_exhausted` onto Brian's Telegram.
        _declared_off = (declared_off_by_id or {}).get(spec.artifact_id)
        _cannot_page = (
            (_suppressed and _suppressed.get("suppressed"))
            or (_declared_off and _declared_off.get("suppressed"))
            or never_written_by_id.get(spec.artifact_id) is True
            or suppress_page
        )
        if _is_confirmed_miss(result) and not _cannot_page:
            try:
                owning = _resolve_owning_item(
                    spec.artifact_id, result.canonical_key,
                    known_artifact_ids, now, lookup_state,
                )
            except Exception as owning_exc:  # noqa: BLE001 — the join must never sink a page
                logger.warning(
                    "owning-item resolution for %s raised (non-fatal): %s",
                    spec.artifact_id, owning_exc, exc_info=True,
                )
                owning = _unresolved(
                    f"resolver_raised: {type(owning_exc).__name__}: {owning_exc}"
                )
            owning = _merge_self_filed(
                owning, prev_issue_filed.get(spec.artifact_id)
            )
            owning_by_id[spec.artifact_id] = owning

        # config-I7713 — decide here, deliver ONCE after the loop. `paged`
        # keeps its exact meaning (this row is on the operator page), so the
        # execution-loop records, the drain-candidate gate and the `alerted`
        # count below are unchanged; only the number of MESSAGES changed.
        decision = None
        if not suppress_page:
            decision = _alert_decision(
                spec, result, now, consecutive_miss_runs=miss_runs,
                producer_suppression=(suppression_by_id or {}).get(
                    spec.artifact_id),
                owning=owning,
                never_written=never_written,
                declared_off=(declared_off_by_id or {}).get(spec.artifact_id),
                episode=episode,
                driver=driver_by_id[spec.artifact_id],
            )
        paged = decision is not None
        if paged:
            digest_decisions.append(decision)
            alerted += 1

        if paged:
            owning_item = (owning or {}).get("owning_item")
            page_records.append({
                "artifact_id": spec.artifact_id,
                "alert_class": _alert_class(spec),
                "owning_item_number": (
                    owning_item["number"] if owning_item else None
                ),
                "owning_item_age_days": (
                    owning_item["age_days"] if owning_item else None
                ),
                "lookup_degraded": bool((owning or {}).get("degraded")),
            })

        # config-I3282 — collect this pass's dispatch-eligible critical
        # pages. `_maybe_alert` returns True ONLY on an actual critical
        # publish (warnings and OBSERVE mode return False), so `paged` is
        # already the effective-severity gate. Excluded: rows with a
        # `recovery:` heal of their own (their declared lane), and rows
        # declared `remediation: operator` (page-only by declaration).
        if (
            paged
            and spec.artifact_id not in recovery_by_id
            and remediation_by_id.get(spec.artifact_id) != "operator"
        ):
            drain_candidates.append(spec.artifact_id)

    # config-I7713 — the sweep's SINGLE page, covering every row that decided
    # to alert above. Trapped like every other side effect: a delivery failure
    # must not sink check_results.json or the heartbeat, which are the durable
    # record this page is only a notification of.
    try:
        _publish_digest(digest_decisions, now, never_written_by_id)
    except Exception as digest_exc:  # noqa: BLE001 — the page is a notification; the durable record is check_results.json, and this ERROR is the recording surface for its loss
        logger.error(
            "FRESHNESS_DIGEST_PUBLISH_FAILED for %d artifact(s) %s "
            "(non-fatal): %s",
            len(digest_decisions),
            sorted(d["artifact_id"] for d in digest_decisions),
            digest_exc, exc_info=True,
        )

    # One aggregated event-time drain per pass (config-I3282), trapped so a
    # dispatch failure can never sink the pass's primary deliverables. The
    # critical page(s) above already fired, so the operator surface exists
    # regardless of this leg's outcome.
    try:
        _maybe_dispatch_drain(s3_client, aws_clients, drain_candidates, now)
    except Exception as drain_exc:  # noqa: BLE001 — side effect; pages already fired, this ERROR + the un-drained queue are the recording surfaces
        logger.error(
            "FRESHNESS_DRAIN_DISPATCH_FAILED for %s (non-fatal): %s",
            sorted(drain_candidates), drain_exc, exc_info=True,
        )

    return (
        pairs, alerted, dispatched, per_spec_exceptions, miss_counts,
        {
            "owning_by_id": owning_by_id,
            "page_records": page_records,
            "never_written_by_id": never_written_by_id,
            "episode_by_id": episode_by_id,
            "driver_by_id": driver_by_id,
        },
    )


# ── Main handler ────────────────────────────────────────────────────────────


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge cron handler — daily walk of the full registry, emit
    heartbeat + check_results, alert on misses past SLA.

    ``event["mode"] == "historical"`` dispatches to the daily
    historical-probe path instead (separate EB cron at ~04:00 UTC).

    ``event["mode"] == "intraday"`` (config#1297) dispatches to a lighter
    pass scoped to :data:`INTRADAY_ARTIFACT_IDS` only, on a separate 30-min
    weekday-market-hours EB cron. It alerts/dispatches exactly like the full
    sweep but does NOT write check_results/heartbeat/cycle_verdict — those
    are the full-registry dashboard surfaces and only the daily sweep (which
    covers every artifact, including these two) owns them, so a partial
    intraday pass can never clobber them with a 2-artifact-only view.
    """
    started_at = time.time()
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3")

    if event and event.get("mode") == "historical":
        return _handle_historical(
            s3, now, started_at, event.get("lookback"),
        )

    if event and event.get("mode") == "intraday":
        return _handle_intraday(s3, now, started_at)

    logger.info(
        "freshness-monitor invoked at %s (alerts_enabled=%s)",
        now.isoformat(), ALERTS_ENABLED,
    )

    # Load registry. If THIS fails, we want the Lambda to error out
    # so the CW alarm fires — a broken registry must not be silent.
    (specs, recovery_by_id, critical_arms_by_id, escalate_to_issue_by_id,
     remediation_by_id, producer_trigger_by_id, declared_off_input) = (
        load_registry_with_recovery(s3, REGISTRY_BUCKET, REGISTRY_KEY)
    )
    # config-I9206 — wall-clock phase marks. The 2026-09-01 timeout was
    # diagnosed by re-deriving the cost from the laptop because the log line
    # published ONE number for the whole invocation: a duration that grew from
    # 71s to 120s over ten days, with nothing saying which phase grew. These
    # marks make the next regression readable from the log alone.
    t_registry_loaded = time.time()
    logger.info(
        "loaded %d specs from registry (%d with recovery specs, %d with "
        "champion-arm dynamic severity, %d flagged for issue escalation, "
        "%d with declared remediation lanes, %d with a declared producer "
        "trigger)",
        len(specs), len(recovery_by_id), len(critical_arms_by_id),
        len(escalate_to_issue_by_id), len(remediation_by_id),
        len(producer_trigger_by_id),
    )

    # config-I3086: dynamic severity + warning-escalation counters.
    specs, coerced_ids = apply_dynamic_severity(s3, specs, critical_arms_by_id)
    prev_miss_counts = _load_prev_miss_counts(s3)
    # config-I6570: which producers are deliberately switched off right now.
    suppression_by_id = apply_producer_suppression(s3, producer_trigger_by_id, now)
    # config-I8719 — the declared-off half. No AWS call: both the declarations
    # and the publisher's resolution of their clearing milestones came from the
    # registry object already read above.
    declared_off_by_id = resolve_declared_off(
        declared_off_input.get("rows") or {},
        declared_off_input.get("resolution"),
        now,
    )
    # Emit zero rather than nothing. A sweep with no declared-off rows says so,
    # and a sweep whose declared-off rows are NOT suppressed says that too —
    # otherwise "the mechanism found nothing" and "the mechanism did not run"
    # are the same silence (observability-policy.md §3.1).
    logger.info(
        "declared-off rows: %d declared, %d suppressed this sweep (%s)",
        len(declared_off_by_id),
        len([v for v in declared_off_by_id.values() if v["suppressed"]]),
        ", ".join(
            f"{a}={'suppressed' if v['suppressed'] else 'PAGING'}"
            f"/milestone={v['clears_when_milestone']}:{v['milestone_status']}"
            for a, v in sorted(declared_off_by_id.items())
        ) or "none",
    )
    if suppression_by_id:
        logger.info(
            "producer-trigger suppression active for %d artifact(s): %s",
            len(suppression_by_id),
            sorted(
                f"{a}({v['trigger']},{v['days_disabled']}d,"
                f"{'suppressed' if v['suppressed'] else 'LAPSED'})"
                for a, v in suppression_by_id.items()
            ),
        )

    # config-I6817 D4 — the account-wide inventory, written every daily sweep.
    # Distinct from suppression above: that answers "is this row's producer
    # off?", this answers "what is off at all?", including schedules no
    # artifact row names. Written before the probe pass so a pass that raises
    # still leaves the inventory current.
    disabled_inventory = write_disabled_producer_inventory(
        s3, now,
        {t for v in producer_trigger_by_id.values() for t in _declared_triggers(v)},
    )
    t_inventory_done = time.time()

    # Read BEFORE the pass: the §7.4a owning-item resolution unions the
    # self-filed marker with the tracker search, and the pass is where the
    # pages are composed.
    prev_issue_filed = _load_prev_issue_filed(s3)
    # config-I9336 — the previous sweep's open-episode sentinel, the same
    # round-trip-through-check_results.json convention as the two reads
    # above (no new persisted-state surface).
    prev_episode_state = _load_prev_episode_state(s3)

    # config-I9206 — created once per handler invocation, never at module
    # scope: a warm container outlives a day, and a LIST answer cached across
    # invocations would answer today's freshness question with yesterday's
    # listing.
    list_cache = _new_list_cache()

    (pairs, alerted, dispatched, per_spec_exceptions, miss_counts,
     pass_telemetry) = _run_probe_pass(
        s3, specs, recovery_by_id, now, prev_miss_counts,
        remediation_by_id=remediation_by_id,
        suppression_by_id=suppression_by_id,
        declared_off_by_id=declared_off_by_id,
        prev_issue_filed=prev_issue_filed,
        prev_episode_state=prev_episode_state,
        # config-I9206 — ONE cache for this invocation, owned by the handler
        # and discarded with it.
        list_cache=list_cache,
    )
    t_spec_loop_done = time.time()
    owning_by_id = pass_telemetry["owning_by_id"]
    never_written_by_id = pass_telemetry.get("never_written_by_id") or {}
    episode_by_id = pass_telemetry.get("episode_by_id") or {}
    driver_by_id = pass_telemetry.get("driver_by_id") or {}

    # config#2055 Gap 2 + §7.4a: extended-staleness -> Decision Queue P1,
    # skipped where an item already owns the condition. Runs after the full
    # pass so flagged `event_driven` rows can look up their `liveness_via`
    # ANCHOR's miss-streak from this same sweep (their own
    # `consecutive_miss_runs` is always 0 — event_driven never self-pages).
    issue_filed_by_id = _escalate_stale_key_deliverables(
        pairs, miss_counts, escalate_to_issue_by_id, prev_issue_filed, now,
        owning_by_id=owning_by_id,
    )

    # §7.4a clause (c) — the number, computed every run including the run
    # where it is all zeros.
    execution_loop = _summarize_execution_loop(
        pairs, pass_telemetry["page_records"], now,
    )
    _all = execution_loop["classes"][f"{_FLOW_NAME}.all"]
    logger.info(
        "§7.4a execution-loop: pages=%d with_open_owning_item=%d "
        "fraction=%.4f median_owning_item_age_days=%.2f degraded_lookups=%d",
        _all["pages"], _all["pages_with_open_owning_item"],
        _all["fraction_with_open_owning_item"],
        _all["median_owning_item_age_days_at_page"],
        _all["pages_with_degraded_lookup"],
    )

    # Emit dashboard surface + self-heartbeat.
    # config-I7606: coverage of the suppression mechanism itself, computed
    # before serialization so the numbers and the rows they describe come out
    # of the same pass and cannot disagree.
    coverage = suppression_coverage(
        pairs, producer_trigger_by_id, suppression_by_id, disabled_inventory,
    )
    if coverage["undeclared_not_fresh"]:
        logger.warning(
            "SUPPRESSION COVERAGE: %d not-fresh row(s) declare no "
            "producer_trigger, so no producer pause could ever explain them: "
            "%s. %d of %d rows declare the field; %d disabled producer(s) are "
            "named by no registry row (config-I7606).",
            coverage["undeclared_not_fresh"],
            coverage["undeclared_not_fresh_ids"],
            coverage["rows_declaring_producer_trigger"],
            coverage["registry_rows"],
            coverage["disabled_producers_unreferenced"],
        )
    check_results = _serialize_check_results(
        pairs, now, miss_counts=miss_counts, coerced_ids=coerced_ids,
        issue_filed_by_id=issue_filed_by_id,
        suppression_by_id=suppression_by_id,
        declared_off_by_id=declared_off_by_id,
        owning_by_id=owning_by_id,
        execution_loop=execution_loop,
        coverage=coverage,
        never_written_by_id=never_written_by_id,
        episode_by_id=episode_by_id,
        driver_by_id=driver_by_id,
    )
    heartbeat = _serialize_heartbeat(pairs, now, started_at)

    _put_json(s3, REGISTRY_BUCKET, CHECK_RESULTS_KEY, check_results)
    _put_json(s3, REGISTRY_BUCKET, HEARTBEAT_KEY, heartbeat)

    # §7.4a clause (c) durable surface + CW metrics. Split into independent
    # traps for the same reason the cycle-verdict pair is (config#1236): a
    # CW-grant regression must not suppress the S3 artifact, and a swallowed
    # failure here is alarmable via ArtifactFreshnessCycleVerdictError rather
    # than only visible as a stale execution_loop.json.
    try:
        _put_json(s3, REGISTRY_BUCKET, EXECUTION_LOOP_KEY, execution_loop)
    except Exception as exc:  # noqa: BLE001 — secondary observability; recording surfaces: this ERROR, the CW Stage=execution_loop_s3_write datapoint, and the staleness of execution_loop.json itself
        logger.error(
            "execution-loop S3 write failed (non-fatal): %s", exc, exc_info=True
        )
        _emit_cycle_verdict_error("execution_loop_s3_write")
    try:
        _emit_execution_loop_metrics(boto3.client("cloudwatch"), execution_loop)
    except Exception as exc:  # noqa: BLE001 — secondary observability; recording surfaces: this ERROR, the CW Stage=execution_loop_cw_emit datapoint, and execution_loop.json (already written above)
        logger.error(
            "execution-loop CW metric emit failed (non-fatal): %s", exc,
            exc_info=True,
        )
        _emit_cycle_verdict_error("execution_loop_cw_emit")

    # config-I7606 — coverage on CloudWatch, in its own trap for the same
    # reason as the pair above: a PutMetricData regression must not suppress
    # the S3 artifact that already carries the same numbers.
    try:
        _emit_suppression_coverage_metrics(boto3.client("cloudwatch"), coverage)
    except Exception as exc:  # noqa: BLE001 — secondary observability; recording surfaces: this ERROR, the CW Stage=suppression_coverage_cw_emit datapoint, and check_results.json's suppression_coverage block (already written above)
        logger.error(
            "suppression-coverage CW metric emit failed (non-fatal): %s", exc,
            exc_info=True,
        )
        _emit_cycle_verdict_error("suppression_coverage_cw_emit")

    # ── Per-cycle completion rollup (L249 consumer) ─────────────────────
    # Secondary observability hung off the primary probe pass. The artifact
    # The S3 verdict write comes first (S3 PutObject is already granted); the
    # CW emit is independent (it needs the cloudwatch:PutMetricData grant in
    # iam-policy.json, scoped to the AlphaEngine/Substrate namespace). Both are
    # wrapped so a failure here can NEVER take down the monitor's primary
    # deliverables (check_results + heartbeat + alerts), already persisted above.
    #
    # The two side effects are split into INDEPENDENT traps so a CW-emit failure
    # (e.g. a PutMetricData grant regression) cannot suppress the cycle_verdict.json
    # write — config#1236 found the deployed Lambda not emitting cycle_verdict.json
    # and the single combined trap masked which step failed. Each trap now:
    #   (a) records the swallowed failure with exc_info (full CW Logs traceback), and
    #   (b) emits an ArtifactFreshnessCycleVerdictError CW datapoint dimensioned by
    #       the failing Stage, so a silent rollup failure is alarmable rather than
    #       only visible by the absence/staleness of cycle_verdict.json.
    # Per CLAUDE.md no-silent-fails secondary-observability carve-out.
    cycle_verdicts: dict[str, str] = {}
    verdict_payload: dict[str, Any] | None = None
    try:
        verdict_payload = _serialize_cycle_verdicts(pairs, now)
        _put_json(s3, REGISTRY_BUCKET, CYCLE_VERDICT_KEY, verdict_payload)
        cycle_verdicts = {
            v["cadence"]: v["state"] for v in verdict_payload["verdicts"]
        }
        logger.info("cycle verdicts: %s", cycle_verdicts)
    except Exception as exc:  # noqa: BLE001 — secondary observability, must not sink the monitor
        logger.warning(
            "cycle-verdict serialize/S3-write failed (non-fatal): %s", exc, exc_info=True
        )
        _emit_cycle_verdict_error("serialize_or_s3_write")

    # CW metric emit is best-effort and independent of the S3 write above: even
    # if it fails (grant regression), cycle_verdict.json is already persisted.
    if verdict_payload is not None:
        try:
            _emit_cycle_metrics(boto3.client("cloudwatch"), verdict_payload)
        except Exception as exc:  # noqa: BLE001 — observability emit, must not sink the monitor
            logger.warning(
                "cycle-completion CW metric emit failed (non-fatal): %s", exc, exc_info=True
            )
            _emit_cycle_verdict_error("cw_metric_emit")

    issues_filed_this_run = sum(
        1 for aid, url in issue_filed_by_id.items()
        if url and prev_issue_filed.get(aid) is None  # newly filed, not carried forward
    )
    # config-I9206 — the phases, published next to the total. Each is the wall
    # clock between two marks, so they sum to the total by construction:
    #   registry_load     — load_registry_with_recovery
    #   producer_inventory— dynamic severity, prior-state reads, producer
    #                       suppression, declared-off resolution, and the
    #                       disabled-producer inventory write
    #   spec_loop         — _run_probe_pass over every spec (the LISTs)
    #   post_loop         — escalation, execution-loop summary, serialization,
    #                       dashboard/heartbeat/cycle-verdict writes, dispatch
    t_end = time.time()
    phase_seconds = {
        "registry_load": round(t_registry_loaded - started_at, 2),
        "producer_inventory": round(t_inventory_done - t_registry_loaded, 2),
        "spec_loop": round(t_spec_loop_done - t_inventory_done, 2),
        "post_loop": round(t_end - t_spec_loop_done, 2),
        "total": round(t_end - started_at, 2),
    }
    logger.info(
        "freshness-monitor complete: %s checked, %s alerted, %s dispatched, "
        "%s issues filed, %s per-spec exceptions, duration=%.2fs | phases: "
        "registry_load=%.2fs producer_inventory=%.2fs spec_loop=%.2fs "
        "post_loop=%.2fs total=%.2fs (list_cache=%s, %d distinct prefixes)",
        heartbeat["n_entries_checked"], alerted, dispatched,
        issues_filed_this_run, per_spec_exceptions, heartbeat["duration_seconds"],
        phase_seconds["registry_load"], phase_seconds["producer_inventory"],
        phase_seconds["spec_loop"], phase_seconds["post_loop"],
        phase_seconds["total"],
        "on" if _CHECK_FRESHNESS_ACCEPTS_LIST_CACHE else "UNAVAILABLE",
        len(list_cache),
    )

    return {
        "phase_seconds": phase_seconds,
        "list_cache_supported": _CHECK_FRESHNESS_ACCEPTS_LIST_CACHE,
        "list_cache_entries": len(list_cache),
        "n_entries_checked": heartbeat["n_entries_checked"],
        "counts": heartbeat["counts"],
        "alerts_enabled": ALERTS_ENABLED,
        "recovery_dispatch_enabled": RECOVERY_DISPATCH_ENABLED,
        "drain_dispatch_enabled": DRAIN_DISPATCH_ENABLED,
        "alerted": alerted,
        "dispatched": dispatched,
        "issues_filed": issues_filed_this_run,
        "per_spec_exceptions": per_spec_exceptions,
        "duration_seconds": heartbeat["duration_seconds"],
        "cycle_verdicts": cycle_verdicts,
        "execution_loop": execution_loop["classes"],
    }
