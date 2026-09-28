#!/usr/bin/env python3
"""Mechanical weekday-SF recovery helper (alpha-engine-config#6694).

Second adoption of the config#2277 idiom ``scripts/weekly_sf_rerun.py``
established, covering the two weekday pipelines instead of the Saturday one:

- ``ne-preopen-trading-pipeline`` ("daily") — skip gates: ``skip_morning_
  enrich``, ``skip_predictor_inference``,
  ``skip_morning_planner``, ``skip_run_daemon`` (infrastructure/
  step_function_daily.json). These gates test ONLY the flag itself — no
  pipeline_role conjunction, matching the weekly SF's shape.
- ``ne-postclose-trading-pipeline`` ("eod") — skip gates: ``skip_refresh_
  executor_deploy``, ``skip_post_market_data``, ``skip_capture_snapshot``,
  ``skip_eod_reconcile`` (infrastructure/step_function_eod.json). These
  gates structurally conjunct ``pipeline_role == "operator-replay"``
  (config#1614) — the proven shape a manual operator replay has always used
  (I2700, first exercised 2026-07-13/07-15, and the shape the EOD SF's own
  closed self-heal loop now auto-dispatches via HealDispatchReplay). A skip
  flag emitted under any OTHER role is silently INERT on this pipeline.

Given ``--execution-arn`` (a FAILED/TIMED_OUT/ABORTED execution of either
pipeline — auto-detected from the state-machine segment of the ARN), this
derives the exact ``StartExecution`` input for a correctly-scoped recovery
rerun: the ORIGINAL ``run_date`` + ``sns_topic_arn`` / instance-id fields
(read from the failed execution's own ``ExecutionStarted`` input, NOT
today's date — a fresh manual rerun without the original run_date writes to
a different artifact prefix, orphaning the partial run), the derived
``skip_*`` flag set for every stage the failed execution completed CLEANLY,
and the pipeline-appropriate ``pipeline_role``:

- EOD reruns always emit ``pipeline_role: "operator-replay"`` — the skip
  gates require it (see above); there is no other role under which the
  derived flags would do anything.
- daily reruns PRESERVE the original execution's ``pipeline_role`` — the
  gates are role-unconditional, and daily's mutex key is minute-bucketed
  (``{SM}#{role}#{YYYY-MM-DDTHH:MM}``, config#1416-shape AcquireMutex — NOT
  the weekly SF's date-bucketed run-slot mutex, config#2280), so a rerun
  started at a different wall-clock minute never collides with the failed
  execution's own stale mutex item. No mutex-steal decision matrix is
  needed here (unlike weekly_sf_rerun.py) for exactly this reason.

A stage that DEGRADED rather than completing — EOD's ``EODReconcile`` skipped
via ``SkipEODReconcileDataGap`` (the precondition-probe data-gap route, NOT
an operator skip flag) because the day's macro-freshness sentinel was not
yet verified-present — is recorded as degraded and NEVER skipped, mirroring
weekly_sf_rerun.py's I6055 degraded-overrides-witness rule: the whole point
of a mechanical rerun is to retry exactly the thing that didn't actually run.

COHERENCE VALIDATION
---------------------
Both weekday pipelines are STRICTLY LINEAR per-stage chains — each
``CheckSkip*`` gate's skip route lands exactly on the next stage's gate (no
multi-hop overshoot like the weekly SF's ``skip_backtester``), and no later
stage's ``Parameters`` reads an earlier skippable stage's ``ResultPath`` via
JSONPath (verified against the live definitions at the time this was
written — grep for ``.$`` reads of ``$.scanner_result`` / ``$.predictor_
result`` / ``$.planner_result`` / ``$.snapshot_result`` / ``$.eod_result``
etc. outside each stage's own internal poll loop turns up none). So the one
way a derived skip-set can leave a downstream JSONPath unresolvable in
practice is a STALE flag from an even-earlier rerun's preserved input that
this execution's own history is inconsistent with (e.g. an operator hand-
edited ``skip_eod_reconcile: true`` into a fresh EOD invocation whose
history shows EODReconcile actually failed) — ``_simulate_reachable_works``
below still catches this the same way weekly_sf_rerun.py's does: simulate
the final flags (derived ∪ preserved original, gated by pipeline_role for
EOD) against the stage chain and refuse if any FAILED stage's work state
would be unreachable.

``--dry-run`` (default) prints the derived plan + input; ``--start`` runs the
one pre-start guard (no OTHER execution of the same pipeline is RUNNING with
the same effective run_date — the actual double-writer hazard, independent
of the mutex mechanics above) and starts the execution as
``operator-rerun-{run_date}-{HHMMSS}``.

Read-only by default; nothing is mutated without ``--start``.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# sys.path insertion (not a package import) so this resolves identically
# whether run directly (`python scripts/weekday_sf_rerun.py`) or loaded by
# spec_from_file_location the way tests/test_weekday_sf_rerun.py does.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sf_rerun_common import (  # noqa: E402 — see sys.path insertion above
    derive_run_date,
    effective_run_date_of,
    entered_states,
    execution_input,
    fetch_history,
    list_all_executions,
    verify_skip_flags_live,
)

ACCOUNT_REGION = "arn:aws:states:us-east-1:711398986525:stateMachine:{}"
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})
RERUNNABLE_SOURCE_STATUSES = frozenset({"FAILED", "TIMED_OUT", "ABORTED"})
OPERATOR_REPLAY_ROLE = "operator-replay"


# ---------------------------------------------------------------------------
# Declarative stage tables — pinned against infrastructure/step_function_
# daily.json / step_function_eod.json by tests/test_weekday_sf_rerun.py.
# witness = the state the SF enters iff the stage completed successfully OR
# was skipped; either way the rerun must not re-run it (originally-skipped
# stages carry their flag from the preserved original input anyway).
# degraded_witness = a state entered iff the stage was bypassed WITHOUT an
# operator skip flag (a data-gap / precondition-failure route) — entering
# one OVERRIDES witness: the stage must RE-RUN, never be skipped (mirrors
# weekly_sf_rerun.py's I6055 rule).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Stage:
    name: str
    flag: str
    gate: str                                  # the CheckSkip* Choice state
    work: str                                  # the stage's first work state
    witness: frozenset                         # entered => completed-or-skipped
    degraded_witness: frozenset = frozenset()  # entered => ran-but-bypassed, NOT skippable


@dataclass(frozen=True)
class Pipeline:
    key: str                # "daily" | "eod"
    label: str               # human label for messages, e.g. "preopen (daily)"
    sm_name: str             # deployed state-machine name
    stages: tuple            # tuple[Stage, ...] in chain order
    role_conjunct: str | None  # None: gates are role-unconditional. A string:
                                # every CheckSkip* gate structurally requires
                                # pipeline_role == this value (config#1614).
    emitted_role: str | None   # None: PRESERVE the original execution's
                                # pipeline_role. A string: always emit this
                                # role regardless of the original.

    @property
    def sm_arn(self) -> str:
        return ACCOUNT_REGION.format(self.sm_name)


DAILY_STAGES: tuple[Stage, ...] = (
    # alpha-engine-config-I7811 (Brian ruling 2026-08-20): the `scanner` stage
    # was REMOVED from this pipeline — the scanner forms its two cuts WEEKLY, on
    # the Saturday pipeline, and those feed research and the predictor for the
    # week. morning_enrich's witness therefore moved from CheckSkipScanner to
    # CheckSkipPredictorInference. A stage table that still named `scanner`
    # would emit `skip_scanner` into a definition with no gate to read it, and
    # the helper's own coherence check would reject its own output.
    # alpha-engine-config-I11269 (decoupled data cutover): the morning spot legs
    # left this definition; the standalone ne-data-collection-morning runs them
    # and CheckSkipMorningEnrich's Default now enters the bounded readiness
    # wait on its manifests. Name and flag kept so recovery inputs stay valid.
    # The not-ready route (budget exhausted, or the producer settled failed) is
    # a DEGRADED bypass: a rerun must wait again, never skip it.
    Stage("morning_enrich", "skip_morning_enrich",
          "CheckSkipMorningEnrich", "WaitForCollectionManifests",
          frozenset({"CheckSkipPredictorInference"}),
          degraded_witness=frozenset({"ExtractCollectionNotReadyError"})),
    Stage("predictor_inference", "skip_predictor_inference",
          "CheckSkipPredictorInference", "PredictorInference",
          frozenset({"CheckSkipMorningPlanner"})),
    Stage("morning_planner", "skip_morning_planner",
          "CheckSkipMorningPlanner", "RunMorningPlanner",
          frozenset({"CheckSkipRunDaemon"})),
    Stage("run_daemon", "skip_run_daemon",
          "CheckSkipRunDaemon", "RunDaemon",
          # config#6692 Option-A parity cutover: CheckSkipRunDaemon's skip
          # edge now routes through CheckDegradedOutcome (the shared
          # terminal-decision node, also reached from RunDaemon's own
          # normal Next and its Catch) rather than straight to
          # WriteCompletionMarker, so an earlier data-spot degraded flag is
          # still honored even when this stage itself is skipped.
          frozenset({"CheckDegradedOutcome"})),
)

EOD_STAGES: tuple[Stage, ...] = (
    Stage("refresh_executor_deploy", "skip_refresh_executor_deploy",
          "CheckSkipRefreshExecutorDeploy", "RefreshExecutorDeploy",
          frozenset({"CheckSkipPostMarketData"})),
    # alpha-engine-config-I11269: same repoint as morning_enrich above — the
    # post-market spot legs left this definition for ne-data-collection-eod.
    Stage("post_market_data", "skip_post_market_data",
          "CheckSkipPostMarketData", "WaitForCollectionManifests",
          frozenset({"CheckSkipCaptureSnapshot"}),
          degraded_witness=frozenset({"ExtractCollectionNotReadyError"})),
    Stage("capture_snapshot", "skip_capture_snapshot",
          "CheckSkipCaptureSnapshot", "CaptureSnapshot",
          frozenset({"ProbeEODReconcilePrecondition"})),
    Stage(
        "eod_reconcile", "skip_eod_reconcile",
        "CheckSkipEODReconcile", "EODReconcile",
        frozenset({"StopTradingInstance"}),
        # config-I2702: precondition-probe data-gap route — EODReconcile was
        # bypassed because run_date's SPY close wasn't yet verified-present
        # in ArcticDB, NOT because an operator asked to skip it. The closed
        # self-heal loop this enters may itself dispatch a SEPARATE
        # reconcile-only replay execution (HealDispatchReplay, I2700 shape)
        # that could already be handling it — but THIS execution's own
        # history cannot prove that replay succeeded, so per the same
        # never-skip-a-degraded-stage rule I6055 established, the rerun
        # re-runs EODReconcile rather than gambling on an unverified sibling.
        degraded_witness=frozenset({"SkipEODReconcileDataGap"}),
    ),
)

DAILY = Pipeline(
    key="daily", label="preopen (daily)", sm_name="ne-preopen-trading-pipeline",
    stages=DAILY_STAGES, role_conjunct=None, emitted_role=None,
)
EOD = Pipeline(
    key="eod", label="postclose (EOD)", sm_name="ne-postclose-trading-pipeline",
    stages=EOD_STAGES, role_conjunct=OPERATOR_REPLAY_ROLE, emitted_role=OPERATOR_REPLAY_ROLE,
)
PIPELINES: tuple[Pipeline, ...] = (DAILY, EOD)
STAGES_BY_NAME = {p.key: {s.name: s for s in p.stages} for p in PIPELINES}


def pipeline_for_execution_arn(execution_arn: str) -> Pipeline:
    """Auto-detect daily vs EOD from the state-machine segment of an
    EXECUTION arn (arn:aws:states:<region>:<account>:execution:<sm-name>:
    <exec-name>)."""
    parts = execution_arn.split(":")
    if len(parts) < 8 or parts[5] != "execution":
        raise SystemExit(
            f"FATAL: {execution_arn!r} does not look like a Step Functions "
            "EXECUTION arn (expected arn:aws:states:<region>:<account>:"
            "execution:<state-machine-name>:<execution-name>)."
        )
    sm_name = parts[6]
    for p in PIPELINES:
        if p.sm_name == sm_name:
            return p
    raise SystemExit(
        f"FATAL: execution arn's state machine {sm_name!r} is not one of "
        f"the weekday pipelines this helper covers "
        f"({[p.sm_name for p in PIPELINES]!r}). For the Saturday weekly "
        "pipeline use scripts/weekly_sf_rerun.py instead."
    )


# ---------------------------------------------------------------------------
# Plan derivation (pure — unit-tested over fixtures)
# ---------------------------------------------------------------------------

@dataclass
class RerunPlan:
    pipeline_key: str
    run_date: str
    run_date_provenance: str
    original_input: dict
    emitted_role: str | None
    completed: list = field(default_factory=list)   # stage names
    degraded: list = field(default_factory=list)    # stage names (re-run!)
    failed: list = field(default_factory=list)       # stage names
    skip_flags: dict = field(default_factory=dict)   # flag -> True
    warnings: list = field(default_factory=list)
    notes: list = field(default_factory=list)
    # config-I7807: populated only for a preopen recovery started while the
    # NYSE session is open. None on every other path, so a run that does not
    # need the override never silently carries one.
    market_hours_override: dict | None = None

    def rerun_input(self) -> dict:
        out = dict(self.original_input)
        out["run_date"] = self.run_date
        if self.emitted_role is not None:
            out["pipeline_role"] = self.emitted_role
        out.update(self.skip_flags)
        # alpha-engine-config-I7807. A recovery started while the NYSE session
        # is open halts at MarketHoursBoundary without this field, so a helper
        # whose whole purpose is "recovery is ONE mechanical command"
        # (sf-pipeline-policy §2.5) was emitting an input guaranteed to fail on
        # the case it exists for. Measured 2026-08-20 against the morning's
        # failed preopen: the printed input carried no override, and the same
        # shape shows twice on 2026-08-19 — operator-rerun-...-151046 FAILED in
        # 3s, then ...-151134-override SUCCEEDED, the same command run twice
        # with a field added by hand.
        #
        # §3 makes the override "the normal instrument of this rule, not an
        # exception to it": the boundary exists to make an in-session start
        # DELIBERATE and AUDITABLE, not rare.
        if self.market_hours_override is not None:
            out["market_hours_override"] = self.market_hours_override
        return out


def _simulate_reachable_works(pipeline: Pipeline, flags: dict, original_input: dict, role: str | None) -> set:
    """Walk the (strictly linear) skip-gate chain with the proposed flags
    merged over the preserved original input, gated by pipeline_role exactly
    the way each pipeline's live CheckSkip* Choice states do, and return the
    set of stage names whose WORK state would run. Mirrors weekly_sf_rerun.
    py's `_simulate_reachable_works` — simpler here because neither weekday
    pipeline's skip routes overshoot past a later stage's gate (see the
    module docstring's COHERENCE VALIDATION section)."""
    role_live = pipeline.role_conjunct is None or role == pipeline.role_conjunct
    ran = set()
    for stage in pipeline.stages:
        v = flags.get(stage.flag, original_input.get(stage.flag))
        skipped = role_live and bool(v is True)
        if not skipped:
            ran.add(stage.name)
    return ran


def derive_plan(pipeline: Pipeline, events: list, start_time: datetime | None = None) -> RerunPlan:
    entered = entered_states(events)
    original_input = execution_input(events)
    run_date, provenance = derive_run_date(events, start_time)
    role = pipeline.emitted_role if pipeline.emitted_role is not None else original_input.get("pipeline_role")

    plan = RerunPlan(
        pipeline_key=pipeline.key, run_date=run_date, run_date_provenance=provenance,
        original_input=original_input, emitted_role=role,
    )

    for stage in pipeline.stages:
        if entered & stage.degraded_witness:
            plan.degraded.append(stage.name)
            plan.notes.append(
                f"{stage.name}: DEGRADED (entered "
                f"{sorted(entered & stage.degraded_witness)}) — NOT skipped; "
                "the rerun re-runs it rather than trust an unverified route around it"
            )
        elif entered & stage.witness:
            plan.completed.append(stage.name)
            plan.skip_flags[stage.flag] = True
        elif stage.work in entered:
            plan.failed.append(stage.name)

    if not plan.failed and not plan.degraded:
        plan.warnings.append(
            "no failed or degraded WORK stage identified — the failure was "
            "pre-workload (mutex conflict / SSM-readiness timeout / "
            "deploy-drift or trading-day gate / code-freshness refusal). Fix "
            "the root cause first; this rerun input re-runs everything not "
            "witnessed complete."
        )

    if pipeline.role_conjunct is not None and role != pipeline.role_conjunct:
        # Should never happen — EOD's emitted_role is hardcoded to equal its
        # role_conjunct. If it fires, PIPELINES has drifted from itself.
        raise SystemExit(
            f"FATAL: {pipeline.sm_name}'s skip gates structurally require "
            f"pipeline_role={pipeline.role_conjunct!r} (config#1614 pattern) "
            f"but this rerun would emit role {role!r} — the derived skip "
            f"flags {sorted(plan.skip_flags)} would be silently IGNORED and "
            "every completed stage would re-run/re-burn side effects. "
            "PIPELINES' role_conjunct/emitted_role have drifted from each "
            "other in scripts/weekday_sf_rerun.py."
        )

    reachable = _simulate_reachable_works(pipeline, plan.skip_flags, original_input, role)
    unreachable_failed = [f for f in plan.failed if f not in reachable]
    if unreachable_failed:
        raise SystemExit(
            f"FATAL: derived skip set would make failed stage(s) "
            f"{unreachable_failed} unreachable on {pipeline.sm_name} — "
            "refusing to emit an input that silently skips a failed stage. "
            f"Flags: {sorted(plan.skip_flags)}; original input skip_* keys: "
            f"{ {k: v for k, v in original_input.items() if k.startswith('skip_')} }. "
            "This means the original execution input carried a skip flag "
            "inconsistent with this execution's own history (a hand-edited "
            "or stale input) — inspect and correct the original input, or "
            "the skip-gate topology changed and PIPELINES in "
            "scripts/weekday_sf_rerun.py needs updating."
        )
    for f in plan.failed:
        st = STAGES_BY_NAME[pipeline.key][f]
        if plan.skip_flags.get(st.flag):
            raise SystemExit(
                f"FATAL: internal contradiction — failed stage {f!r} ended "
                "up with its own skip flag set. Refusing (forbidden swallow)."
            )
    return plan


def operator_rerun_name(run_date: str, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"operator-rerun-{run_date}-{now.strftime('%H%M%S')}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_plan(pipeline: Pipeline, plan: RerunPlan, source_arn: str, source_status: str, name: str) -> None:
    print(f"pipeline         : {pipeline.label} ({pipeline.sm_name})")
    print(f"source execution : {source_arn} ({source_status})")
    print(f"run_date         : {plan.run_date}  [{plan.run_date_provenance}]")
    print(f"rerun name       : {name}")
    print(f"pipeline_role    : {plan.emitted_role!r}")
    print(f"completed stages : {', '.join(plan.completed) or '(none)'}")
    print(f"degraded stages  : {', '.join(plan.degraded) or '(none)'}")
    print(f"failed stages    : {', '.join(plan.failed) or '(none identified)'}")
    print(f"derived skips    : {', '.join(sorted(plan.skip_flags)) or '(none)'}")
    for n in plan.notes:
        print(f"NOTE : {n}")
    for w in plan.warnings:
        print(f"WARN : {w}", file=sys.stderr)
    rerun_input = json.dumps(plan.rerun_input(), indent=2, sort_keys=True)
    print("\nStartExecution input:")
    print(rerun_input)
    print("\nequivalent CLI:")
    print(
        f"aws stepfunctions start-execution --state-machine-arn {pipeline.sm_arn} "
        f"--name {name} --input '{json.dumps(plan.rerun_input(), sort_keys=True)}'"
    )


# ── config-I7807: the same-day recovery override ─────────────────────────────

def market_is_open(sf_region: str, when: str | None = None) -> tuple[bool, dict]:
    """Ask the SAME Lambda action `MarketHoursGate` asks.

    Deliberately not a local calendar. The gate this override exists to cross
    is decided by `alpha-engine-predictor-inference:live` / `check_market_hours`,
    and a second implementation here could disagree with it — which would either
    attach an override to a run that does not need one, or withhold it from a run
    that does, on a holiday or an early close. One answer, one owner.
    """
    import boto3
    lam = boto3.client("lambda", region_name=sf_region)
    payload = {"action": "check_market_hours", "execution_input": {}}
    if when:
        payload["now"] = when
    resp = lam.invoke(
        FunctionName="alpha-engine-predictor-inference:live",
        Payload=json.dumps(payload).encode(),
    )
    body = json.loads(resp["Payload"].read())
    return bool(body.get("is_market_hours")), body


def pipeline_is_preopen(pipeline) -> bool:
    """Only the preopen pipeline can cross the market-hours boundary — the
    postclose one runs after the close by construction."""
    return getattr(pipeline, "sm_arn", "").endswith("ne-preopen-trading-pipeline")


def session_close_utc(gate: dict) -> str:
    """Today's NYSE close, in UTC, from the gate's own answer.

    `now_et` and `session_window_et` (e.g. "09:30-16:00") both come back from
    the gate, so the close is read from the same source that decides the
    boundary rather than hardcoded — an early-close day has a different window
    and the override must not outlive it.
    """
    from datetime import datetime

    base = datetime.fromisoformat(gate.get("now_et") or "")
    window = gate.get("session_window_et") or "09:30-16:00"
    hh, mm = (int(x) for x in window.split("-")[-1].strip().split(":"))
    close_et = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    offset = close_et.utcoffset()
    if offset is None:
        return close_et.isoformat() + "Z"
    return (close_et - offset).replace(tzinfo=None).isoformat() + "Z"


def build_market_hours_override(
    *, source_arn: str, source_status: str, authorized_by: str,
    reason: str | None, expires_at: str,
) -> dict:
    """The `{reason, authorized_by, expires_at}` shape `RecordMarketHoursOverride`
    consumes. `reason` defaults to the sentence the two successful recoveries
    used, naming the source execution and the standing rule — §3 wants the
    crossing auditable, and an operator retyping it each time is how it stops
    being."""
    default = (
        f"Same-day recovery of {source_status} preopen execution "
        f"{source_arn.rsplit(':', 1)[-1]}. Standing rule sf-pipeline-policy §3 "
        f"(SFP-3-preopen-same-day-relaunch): a failed preopen is always "
        f"relaunched while the NYSE session is open."
    )
    return {
        "reason": reason or default,
        "authorized_by": authorized_by,
        "expires_at": expires_at,
    }


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--execution-arn", required=True,
                     help="failed weekday (preopen or EOD) execution to recover")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="derive + print only (default)")
    mode.add_argument("--start", action="store_true", help="StartExecution with the derived input")
    ap.add_argument("--region", default="us-east-1")
    # config-I7807 — the same-day recovery override (sf-pipeline-policy §3).
    ap.add_argument(
        "--authorized-by",
        help="who authorized crossing the market-hours boundary. REQUIRED when "
             "recovering a preopen execution while the NYSE session is open — "
             "§3 makes the crossing auditable, not rare.",
    )
    ap.add_argument(
        "--market-hours-override-reason",
        help="override the generated reason line (defaults to one naming the "
             "source execution and the SFP-3 standing rule)",
    )
    args = ap.parse_args(argv)

    pipeline = pipeline_for_execution_arn(args.execution_arn)

    import boto3  # deferred so the pure functions above stay import-light for tests

    sf = boto3.client("stepfunctions", region_name=args.region)

    desc = sf.describe_execution(executionArn=args.execution_arn)
    source_status = desc["status"]
    if source_status not in RERUNNABLE_SOURCE_STATUSES:
        raise SystemExit(
            f"FATAL: source execution {args.execution_arn} is {source_status} "
            f"— only {sorted(RERUNNABLE_SOURCE_STATUSES)} executions can be "
            "recovered (a RUNNING one may still finish; a SUCCEEDED one "
            "needs no recovery)."
        )

    events = fetch_history(sf, args.execution_arn)
    plan = derive_plan(pipeline, events, start_time=desc.get("startDate"))
    name = operator_rerun_name(plan.run_date)

    # config-I7807: attach the market-hours override when, and only when, the
    # recovery would cross the boundary. Probed from the gate's own Lambda so
    # this cannot disagree with the state that enforces it.
    if pipeline_is_preopen(pipeline):
        is_open, gate = market_is_open(args.region)
        if is_open:
            if not args.authorized_by:
                raise SystemExit(
                    "FATAL: the NYSE session is open "
                    f"({gate.get('now_et')}, {gate.get('reason')!r}), so this "
                    "recovery crosses the market-hours boundary and needs "
                    "--authorized-by. sf-pipeline-policy §3 makes that crossing "
                    "DELIBERATE and AUDITABLE — it does not make it rare, and "
                    "the standing rule is that a failed preopen is relaunched "
                    "for as long as the market is open."
                )
            plan.market_hours_override = build_market_hours_override(
                source_arn=args.execution_arn,
                source_status=source_status,
                authorized_by=args.authorized_by,
                reason=args.market_hours_override_reason,
                expires_at=session_close_utc(gate),
            )
            plan.notes.append(
                "market_hours_override attached — NYSE is OPEN "
                f"({gate.get('now_et')}); "
                f"expires_at={plan.market_hours_override['expires_at']}"
            )
        else:
            plan.notes.append(
                "no market_hours_override needed — NYSE is closed "
                f"({gate.get('now_et')}, {gate.get('reason')!r})"
            )

    # Role-gating check against the LIVE definition — protects against the
    # SF drifting away from the role assumption PIPELINES encodes (adding
    # role-conjunction to daily's gates, or removing it from EOD's).
    sm_def = json.loads(sf.describe_state_machine(stateMachineArn=pipeline.sm_arn)["definition"])
    verify_skip_flags_live(
        sm_def, plan.emitted_role or "",
        sf_label=f"the {pipeline.label} SF",
        script_path="scripts/weekday_sf_rerun.py",
    )

    _print_plan(pipeline, plan, args.execution_arn, source_status, name)

    if not args.start:
        print("\n(dry-run — nothing mutated; re-run with --start to execute)")
        return 0

    # --- pre-start guard: no other execution of THIS pipeline is RUNNING
    # with the same effective run_date (the actual double-writer hazard;
    # unlike weekly_sf_rerun.py, no mutex-steal decision is needed — see the
    # module docstring's MUTEX note).
    running = list_all_executions(sf, pipeline.sm_arn, status_filter="RUNNING")
    clashing = [
        ex["executionArn"] for ex in running
        if effective_run_date_of(sf, ex) == plan.run_date
    ]
    if clashing:
        raise SystemExit(
            f"FATAL: execution(s) {clashing} are RUNNING with the same "
            f"run_date {plan.run_date} on {pipeline.sm_name} — starting a "
            "rerun beside a live execution races artifact writes. Wait for "
            "terminal state or abort it deliberately first."
        )

    resp = sf.start_execution(
        stateMachineArn=pipeline.sm_arn,
        name=name,
        input=json.dumps(plan.rerun_input(), sort_keys=True),
    )
    print(f"\nSTARTED {resp['executionArn']}")
    print(
        "Do not block on it — the sf-telegram-notifier + Fleet-SF Watch "
        "track the outcome."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
