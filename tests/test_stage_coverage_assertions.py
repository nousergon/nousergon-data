"""Every weekly-SF stage this repo owns asserts its OWN declared output.

`alpha-engine-config-I7214`, `sf-pipeline-policy.md` §2.1. The assertion lives
in the stage's own launcher / handler, at the boundary where the fact becomes
knowable — NOT in a single end-of-run sweep, which learns of a miss ~3h late
and cannot attribute it to a stage still in flight.

The totality test below derives its denominator from the LIVE SF definition in
this repo rather than from a hand-written list: a test that enumerates what
exists is blind to where one is missing, and a hand-maintained stage list
drifts invisibly because the missing rows produce no signal.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
INFRA = REPO / "infrastructure"
SF_DEFINITION = INFRA / "step_function.json"

#: The CLI front door every bash launcher calls. One implementation, in krepis;
#: a per-repo copy would be the fork policy-shared-code forbids. It must be a
#: `krepis.*` module and not a `nousergon_lib.*` one: under runpy the latter is
#: a guard-less re-export shim that exits 0 SILENTLY without executing
#: (config#1646/#1649), which
#: tests/test_spot_data_weekly_ssm_transport.py::test_uses_lib_ssm_dispatcher_chokepoint
#: enforces — and which caught exactly this call site.
CLI_MODULE = "krepis.stage_coverage"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _states(definition: dict) -> dict[str, dict]:
    """Flatten every state, including Parallel branches and Map iterators."""
    out: dict[str, dict] = {}

    def walk(states: dict) -> None:
        for name, body in states.items():
            out[name] = body
            for branch in body.get("Branches", []) or []:
                walk(branch["States"])
            if "Iterator" in body:
                walk(body["Iterator"]["States"])
            # Newer ASL Map states use ItemProcessor rather than Iterator
            # (e.g. TrainSpecDispatch, nested inside ModelZoo's Map) — both
            # must be walked or a state nested only under ItemProcessor is
            # silently invisible to every totality test in this file.
            if "ItemProcessor" in body:
                walk(body["ItemProcessor"]["States"])

    walk(definition["States"])
    return out


def _ssm_command_text(body: dict) -> str:
    params = (body.get("Parameters") or {}).get("Parameters") or {}
    return json.dumps(params)


def _launcher_stages_in_this_repo() -> dict[str, str]:
    """Return ``{sf_stage: launcher_script_name}`` for every weekly-SF Task
    state whose SSM command runs a script out of THIS repo's infrastructure/."""
    definition = json.loads(SF_DEFINITION.read_text())
    found: dict[str, str] = {}
    for name, body in _states(definition).items():
        if body.get("Type") != "Task" or "sendCommand" not in body.get("Resource", ""):
            continue
        text = _ssm_command_text(body)
        match = re.search(r"bash infrastructure/(spot_[a-z0-9_]+\.sh)", text)
        if match and (INFRA / match.group(1)).exists():
            found[name] = match.group(1)
    return found


def _script(name: str) -> str:
    return (INFRA / name).read_text()


# ── The denominator is derived, not enumerated ───────────────────────────────


def test_the_launcher_stage_set_is_discovered_from_the_live_definition() -> None:
    """Guards the guard: if this returns nothing, every totality test below
    passes vacuously — a detector that cannot fail."""
    stages = _launcher_stages_in_this_repo()
    assert stages, "no launcher-backed weekly stages discovered — the parser broke"
    # The two this repo is known to own. A NEW one appearing here is expected
    # to fail this assertion, which is the point: a stage added without an
    # assertion must break a build, not go quiet. MorningEnrich and DataPhase1
    # were the other two until the decoupled data cutover
    # (alpha-engine-config-I11269) moved that work to ne-data-collection-weekly,
    # whose completion claim is its units' run manifests.
    assert set(stages) == {"DataPhase2", "RAGIngestion"}


# ── Every launcher asserts, and asserts its OWN stage ────────────────────────


@pytest.mark.parametrize(
    ("stage", "script"),
    sorted(_launcher_stages_in_this_repo().items()),
)
def test_each_launcher_asserts_its_own_stage(stage: str, script: str) -> None:
    body = _script(script)
    assert CLI_MODULE in body, f"{script} never invokes {CLI_MODULE}"
    assert f"--stage {stage}" in body, (
        f"{script} backs SF state {stage} but does not assert under that name — "
        "a miss would be attributed to a stage that was working"
    )


@pytest.mark.parametrize(
    ("stage", "script"),
    sorted(_launcher_stages_in_this_repo().items()),
)
def test_each_launcher_passes_the_run_window(stage: str, script: str) -> None:
    """Without a window, a leftover from a previous cycle satisfies the probe
    while the consumer reads last week's belief.

    Two forms are legal, and BOTH are a window — never an omission:

    - ``--window-start "$_STAGE_WINDOW_START"`` — this execution's start, the
      original and still-correct semantics for a stage that always re-writes.
    - ``--window-start "$_<STAGE>_WINDOW"``, assigned from
      ``resolve_stage_window_start`` — the CYCLE's first-attempt window, for a
      stage whose workload auto-skips work an earlier attempt of the same
      cycle already did (`alpha-engine-config-I10194` §3). The scope limit on
      that second form, and the derivation behind it, are enforced by
      `tests/test_stage_window_tracks_the_cycle.py`.
    """
    body = _script(script)
    raw = '--window-start "$_STAGE_WINDOW_START"' in body
    resolved = "resolve_stage_window_start" in body and re.search(
        r'--window-start "\$_[A-Z0-9_]+_WINDOW"', body
    )
    assert raw or resolved, (
        f"{script} passes no --window-start at all — an existence-only probe "
        "cannot tell this run's output from last cycle's leftovers"
    )


@pytest.mark.parametrize(
    ("stage", "script"),
    sorted(_launcher_stages_in_this_repo().items()),
)
def test_each_launcher_passes_an_explicit_run_date(stage: str, script: str) -> None:
    """alpha-engine-config-I8155: on the 2026-08-22 weekly run every one of
    these launchers wrote its verdict under an EMPTY run_date, because the
    CLI's argparse default (`os.environ.get("RUN_DATE", "")`) resolves empty
    when the launcher never receives RUN_DATE — which none of these do. The
    fix is explicit: `--run-date "$EXECUTION_RUN_DATE"`, never a bare
    `krepis.stage_coverage assert` relying on the CLI default, and never
    `$RUN_DATE` — that name is reassigned to the trading day by
    crucible-backtester's infrastructure/_spot_common.sh, so a carrier other
    code rewrites is exactly the defect this fixes."""
    body = _script(script)
    for line in body.splitlines():
        if CLI_MODULE in line and "assert --stage" in line and f"--stage {stage}" in line:
            assert '--run-date "$EXECUTION_RUN_DATE"' in line, (
                f"{script}: the {stage} assertion does not pass an explicit "
                "--run-date $EXECUTION_RUN_DATE"
            )
            assert "$RUN_DATE" not in line.replace("$EXECUTION_RUN_DATE", ""), (
                f"{script}: the {stage} assertion's --run-date must never read "
                "the $RUN_DATE carrier — see alpha-engine-config-I8155"
            )
            break
    else:
        pytest.fail(f"{script}: no assert --stage {stage} line found")


def test_the_window_is_captured_before_the_workload_not_after() -> None:
    """A window taken after the write is trivially satisfied by it."""
    common = _script("_spot_common.sh")
    assert "_STAGE_WINDOW_START=" in common
    # In the one launcher that does not source _spot_common.sh, the capture
    # must precede the phase2 workload block.
    weekly = _script("spot_data_weekly.sh")
    assert weekly.index("_STAGE_WINDOW_START=") < weekly.index('RUN_MODE" = "phase2-only')


# ── The assertion may never fail the stage it observes ───────────────────────


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_the_assertion_cannot_fail_the_stage(script: str) -> None:
    """Observe mode. Every launcher runs under `set -euo pipefail`; an
    unguarded non-zero here would abort the stage — and a degraded summary
    routes the whole ~4h weekly run to a Fail state (config-I6891)."""
    for line in _script(script).splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "||" in line, f"{script}: assertion is not failure-guarded"


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_the_guard_is_loud_not_a_bare_true(script: str) -> None:
    """`|| true` would make an unreachable assertion indistinguishable from a
    covered stage — the exact silence this mechanism exists to remove."""
    for line in _script(script).splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "|| true" not in line
            assert "WARNING" in line and ">&2" in line


# ── Promotion to enforcing is a deliberate, reviewed diff ────────────────────


@pytest.mark.parametrize(
    "script", sorted(set(_launcher_stages_in_this_repo().values()))
)
def test_no_call_site_ships_enforcing(script: str) -> None:
    body = _script(script)
    for line in body.splitlines():
        if CLI_MODULE in line and "assert --stage" in line:
            assert "--enforce" not in line
    assert "STAGE_COVERAGE_ENFORCE" not in body


# ── The Friday shell run must not assert ─────────────────────────────────────


def test_preflight_only_paths_exit_before_the_assertion() -> None:
    """`--preflight-only` is a dry pass that writes nothing by design.
    Asserting on it would report a miss for every stage, every Friday — and a
    detector that cries wolf weekly is a detector that gets muted."""
    for script in ("spot_morning_enrich.sh", "spot_data_phase1.sh", "spot_rag_ingestion.sh"):
        body = _script(script)
        assertion_at = body.index(f"{CLI_MODULE} assert")
        preflight_exit = body.index('if [ "$PREFLIGHT_ONLY" = "1" ]')
        assert preflight_exit < assertion_at, (
            f"{script}: the preflight-only early exit must precede the assertion"
        )

    weekly = _script("spot_data_weekly.sh")
    phase2_assertion = weekly.index("assert --stage DataPhase2")
    preflight_exit = weekly.index(
        "Preflight-only run — heartbeat deliberately NOT emitted"
    )
    assert preflight_exit < phase2_assertion


# ── The multi-mode launcher derives its stage name ───────────────────────────


def test_the_multimode_launcher_asserts_only_the_phase2_stage() -> None:
    """`spot_data_weekly.sh` serves six RUN_MODEs and only `--phase2-only` is
    an SF-wired weekly stage. A file-level assertion would file every other
    mode's run under DataPhase2."""
    body = _script("spot_data_weekly.sh")
    assert body.count(f"{CLI_MODULE} assert") == 1
    block_start = body.index('if [ "$RUN_MODE" = "phase2-only" ]')
    assert body.index("assert --stage DataPhase2") > block_start


# ── The Lambda-backed stages this repo owns ──────────────────────────────────


def test_weekly_preflight_records_its_no_output_declaration() -> None:
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    assert "from krepis.stage_coverage import assert_stage_coverage" in body
    assert '_assert_stage_coverage("WeeklyPreflight", started, run_date)' in body
    assert '"stage_coverage"' in body


def test_the_spot_dispatcher_prefers_the_explicit_sf_stage_identity() -> None:
    """One Lambda, two SF states, and (alpha-engine-config-I10172) BOTH
    callers pass `force_on_demand: true` — the boolean no longer
    distinguishes them, so the old `"RelaunchWeeklyFreshnessSpot" if
    force_on_demand else "DispatchWeeklyFreshnessSpot"` derivation always
    took the first branch. DispatchWeeklyFreshnessSpot's own invocations
    wrote their verdict under RelaunchWeeklyFreshnessSpot's name, and
    DispatchWeeklyFreshnessSpot had zero verdicts in any partition, ever.
    Each SF Task now stamps its own name via a Payload literal (`sf_stage`)
    and the handler must read it FIRST."""
    body = (
        INFRA / "lambdas" / "weekly-freshness-spot-dispatcher" / "index.py"
    ).read_text()
    assert 'str(event.get("sf_stage", "")).strip()' in body
    # The force_on_demand derivation survives only as the fallback for an
    # operator off-cycle invocation that predates the sf_stage field.
    assert '"RelaunchWeeklyFreshnessSpot" if force_on_demand else "DispatchWeeklyFreshnessSpot"' in body


def test_both_spot_dispatcher_sf_states_stamp_their_own_identity() -> None:
    """Guards the SF side of I10172: a Payload literal, not derived, so a
    future third caller cannot silently fall back to the ambiguous
    force_on_demand heuristic without a reviewer noticing a missing field."""
    definition = json.loads(SF_DEFINITION.read_text())
    states = _states(definition)
    for name in ("DispatchWeeklyFreshnessSpot", "RelaunchWeeklyFreshnessSpot"):
        payload = states[name]["Parameters"]["Payload"]
        assert payload.get("sf_stage") == name, (
            f"{name}: Payload does not stamp its own sf_stage identity"
        )


# ── alpha-engine-config-I8155: the two Lambdas pass a real run_date ──────────


def test_weekly_preflight_threads_the_event_run_date() -> None:
    """`WeeklyPreflight`'s Task already passes `Payload.$="$"` (config-I7443),
    so `event["run_date"]` is the state input's `$.run_date` — no SF-side
    Payload change needed, only wiring it into the assertion call."""
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    assert 'run_date = event.get("run_date")' in body
    assert "def _assert_stage_coverage(stage: str, started: datetime, run_date: str | None) -> dict:" in body
    assert "assert_stage_coverage(stage, window_start=started, run_date=run_date)" in body


def test_weekly_preflight_never_fabricates_a_run_date() -> None:
    """A missing run_date on the event must report UNMEASURED, never invent a
    date — that is exactly the defect alpha-engine-config-I8155 fixes."""
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    fn = body[body.index("def _assert_stage_coverage(stage: str, started: datetime, run_date"):]
    assert "if not run_date:" in fn
    guard = fn[: fn.index("try:\n        from krepis.stage_coverage")]
    assert '"status": "UNMEASURED"' in guard


def test_the_spot_dispatcher_sf_payloads_carry_run_date() -> None:
    """DispatchWeeklyFreshnessSpot and RelaunchWeeklyFreshnessSpot previously
    passed a narrow Payload with no run_date at all — this Lambda's
    stage-coverage verdict has been writing under an empty run_date since
    I7214 shipped. Both states now thread `$.run_date` explicitly."""
    definition = json.loads(SF_DEFINITION.read_text())
    states = _states(definition)
    for name in ("DispatchWeeklyFreshnessSpot", "RelaunchWeeklyFreshnessSpot"):
        payload = states[name]["Parameters"]["Payload"]
        assert payload.get("run_date.$") == "$.run_date", (
            f"{name}: Payload does not thread $.run_date"
        )


def test_the_spot_dispatcher_threads_the_event_run_date() -> None:
    body = (
        INFRA / "lambdas" / "weekly-freshness-spot-dispatcher" / "index.py"
    ).read_text()
    assert 'str(event.get("run_date", "")).strip() or None' in body
    assert "def _assert_stage_coverage(stage: str, started: datetime.datetime, run_date: str | None) -> dict:" in body
    assert "assert_stage_coverage(stage, window_start=started, run_date=run_date)" in body


def test_the_spot_dispatcher_never_fabricates_a_run_date() -> None:
    body = (
        INFRA / "lambdas" / "weekly-freshness-spot-dispatcher" / "index.py"
    ).read_text()
    fn = body[body.index("def _assert_stage_coverage(stage: str, started: datetime.datetime, run_date"):]
    assert "if not run_date:" in fn
    guard = fn[: fn.index("try:\n        from krepis.stage_coverage")]
    assert '"status": "UNMEASURED"' in guard


@pytest.mark.parametrize(
    "lambda_dir", ["weekly-preflight", "weekly-freshness-spot-dispatcher"]
)
def test_the_lambda_assertion_is_import_guarded_and_loud(lambda_dir: str) -> None:
    """The nousergon-lib pin may predate the module; an inert assertion must
    stay distinguishable from a covered stage, and must not change the
    handler's outcome."""
    body = (INFRA / "lambdas" / lambda_dir / "index.py").read_text()
    assert "except ImportError as exc:" in body
    assert "UNMEASURED" in body
    assert "except ImportError:\n        pass" not in body


@pytest.mark.parametrize(
    "lambda_dir", ["weekly-preflight", "weekly-freshness-spot-dispatcher"]
)
def test_the_lambda_already_carries_the_nousergon_lib_dependency(
    lambda_dir: str,
) -> None:
    """No new bundle: the assertion must not be the reason a Lambda grows a
    dependency 38 hours before a scheduled run."""
    reqs = (INFRA / "lambdas" / lambda_dir / "requirements.txt").read_text()
    assert "nousergon-lib" in reqs


# ── The end-of-run design is GONE ────────────────────────────────────────────


def test_no_stage_coverage_sf_state_remains() -> None:
    """The ruled rescope removes the convergence-point state entirely — no new
    SF state, no new Lambda action, no topology change."""
    definition = json.loads(SF_DEFINITION.read_text())
    names = set(_states(definition))
    assert not [n for n in names if n.startswith("StageCoverage")]
    assert "StageCoverageAssert" not in SF_DEFINITION.read_text()


def test_the_weekly_preflight_lambda_has_no_coverage_action_dispatch() -> None:
    """The second `action` on alpha-engine-weekly-preflight is gone with it.

    NARROWED 2026-08-27 (alpha-engine-config-I8809). This previously asserted
    that the handler dispatches on NO action at all. That is broader than the
    rule it exists to hold: what the rescope removed is the STAGE-COVERAGE
    action and the end-of-run sweep behind it, not the idea of a fast path.
    The Lambda now carries `action == "resolve_run_dates"` — pure NYSE-calendar
    arithmetic, no AWS call, returning before the preflight body — because the
    weekly graph needs its ONE date normalization somewhere and
    `InitializeInput` is a Pass with no calendar. A new function for it would
    need an IAM role bootstrap, i.e. an operator step, i.e. a PR that is not
    deployable by the merge button alone.

    The assertion below still forbids exactly what was ruled out: any
    stage-coverage action, under either of its names.
    """
    body = (INFRA / "lambdas" / "weekly-preflight" / "index.py").read_text()
    assert "sf_stage_coverage" not in body
    assert '"assert_stage_coverage"' not in body
    assert 'action") == "assert_stage_coverage"' not in body


def test_the_end_of_run_module_is_gone() -> None:
    assert not (REPO / "sf_stage_coverage.py").exists()


# ── alpha-engine-config-I8155: EXECUTION_RUN_DATE reaches every SSM state ────
#
# On the 2026-08-22 weekly run, krepis.stage_coverage verdicts split across
# TWO run_date prefixes for one execution: the 8 shell/subprocess launchers
# (this section's targets) wrote run_date="" because they never received
# RUN_DATE at all; the crucible-backtester family wrote run_date to the
# TRADING DAY because infrastructure/_spot_common.sh in that repo reassigns
# RUN_DATE. EXECUTION_RUN_DATE is the fix: a NEW carrier, exported from
# $.run_date (InitializeInput's single stamp, never rewritten anywhere in
# this definition) into every coverage-asserting SSM state, and never
# normalized by anything downstream.

#: Every SSM sendCommand state whose command runs a launcher that performs a
#: stage-coverage assertion, PLUS `ResolveZooSpecs`, which invokes the CLI
#: inline in its own command list (alpha-engine-config-I10197). Deliberately
#: excludes the two bare `aws s3api head-object` resource-kill-check states
#: (PitParityLookaheadKillCheck-shaped): those run no launcher, declare no
#: stage row, and gaining the export would buy nothing.
_COVERAGE_ASSERTING_SSM_STATES = frozenset({
    # MorningEnrich / DataPhase1 left with alpha-engine-config-I11269.
    "RAGIngestion", "DataPhase2",
    "PredictorTraining", "TrainSpecDispatch", "ModelZooSelect",
    "SaturdayHealthCheck", "WeeklySubstrateHealthCheck",
    "Backtester", "PredictorBacktest", "PortfolioOptimizerBacktest",
    "PitParityLookahead", "PitParityWalkforward", "ParityReplay",
    "PitParityCompare", "EvaluatorDiagnostics", "EvaluatorOptimize",
    "ResolveZooSpecs",
})

_EXECUTION_RUN_DATE_EXPORT = "export EXECUTION_RUN_DATE="


def _sf_states() -> dict[str, dict]:
    return _states(json.loads(SF_DEFINITION.read_text()))


@pytest.mark.parametrize("name", sorted(_COVERAGE_ASSERTING_SSM_STATES))
def test_every_coverage_asserting_ssm_state_exports_execution_run_date(name: str) -> None:
    states = _sf_states()
    assert name in states, f"{name}: state not found in the live SF definition"
    text = _ssm_command_text(states[name])
    assert _EXECUTION_RUN_DATE_EXPORT in text, (
        f"{name}: does not export EXECUTION_RUN_DATE — its stage-coverage "
        "assertion (if any) has no reliable run_date carrier"
    )
    assert "$.run_date" in text, f"{name}: EXECUTION_RUN_DATE is not sourced from $.run_date"


def test_execution_run_date_covers_every_task_state_with_a_krepis_assertion() -> None:
    """The set above is asserted against the live definition, not just
    hand-listed: any Task state whose command names krepis.stage_coverage
    but lacks the export is a miss this test must catch even if the curated
    set above goes stale."""
    states = _sf_states()
    missing = []
    for name, body in states.items():
        if body.get("Type") != "Task" or "sendCommand" not in body.get("Resource", ""):
            continue
        text = _ssm_command_text(body)
        # A state whose *own* SSM command invokes krepis.stage_coverage
        # directly (none do today — the CLI runs inside the launcher script
        # over on the box) OR whose named launcher script is one of this
        # repo's coverage-asserting scripts.
        match = re.search(r"bash infrastructure/(spot_[a-z0-9_]+\.sh)", text)
        if not match:
            continue
        script_path = INFRA / match.group(1)
        if not script_path.exists() or CLI_MODULE not in script_path.read_text():
            continue
        missing.append(name)
    # Every discovered state must actually carry the export.
    for name in missing:
        text = _ssm_command_text(states[name])
        assert _EXECUTION_RUN_DATE_EXPORT in text, (
            f"{name}: backs a coverage-asserting launcher but its SSM state "
            "does not export EXECUTION_RUN_DATE"
        )


def test_run_date_export_does_not_replace_the_backtester_family_run_date() -> None:
    """RUN_DATE is load-bearing for `parity/$RUN_DATE/...` S3 keys in the
    crucible-backtester family — EXECUTION_RUN_DATE is ADDED alongside it,
    never a replacement."""
    states = _sf_states()
    backtester_family = {
        "Backtester", "PredictorBacktest", "PortfolioOptimizerBacktest",
        "PitParityLookahead", "PitParityWalkforward", "ParityReplay",
        "PitParityCompare", "EvaluatorDiagnostics", "EvaluatorOptimize",
    }
    for name in backtester_family:
        text = _ssm_command_text(states[name])
        assert "export RUN_DATE=" in text, f"{name}: lost its load-bearing RUN_DATE export"
        assert _EXECUTION_RUN_DATE_EXPORT in text


def test_the_resource_kill_check_states_do_not_export_execution_run_date() -> None:
    """These two states run a bare `aws s3api head-object` — no launcher, no
    assertion — so they must not gain the export."""
    states = _sf_states()
    for name, body in states.items():
        text = _ssm_command_text(body)
        if "aws s3api head-object" in text and "krepis.ssm_log_capture" not in text:
            assert _EXECUTION_RUN_DATE_EXPORT not in text, (
                f"{name}: a bare resource-kill-check state gained an unneeded export"
            )


# ── alpha-engine-config-I10197: ResolveZooSpecs declares COVERED_NO_OUTPUT ──
#
# This file used to carry `test_resolve_zoo_specs_is_not_a_coverage_asserting_
# state`, pinning the OPPOSITE design decision ("it only lists rotation spec
# ids — it must stay out of the export set"). `alpha-engine-config-I10172`
# superseded that decision fleet-wide: a control stage that legitimately
# writes no durable artifact must still POSITIVELY declare
# `COVERED_NO_OUTPUT`, because silence is indistinguishable from a stage
# nobody ever considered — the I8228 "never wired" class. Measured 2026-09-08:
# `ResolveZooSpecs` had never once written a `_stage_coverage` verdict.
#
# The inverted test below replaces it. The deleted test is named here rather
# than silently dropped: a design decision reversed without a record is
# relitigated.


def test_resolve_zoo_specs_asserts_its_own_coverage() -> None:
    """`ResolveZooSpecs` runs the CLI inline in its own SSM command list —
    it has no launcher script of its own to put the assertion in."""
    states = _sf_states()
    text = _ssm_command_text(states["ResolveZooSpecs"])
    assert CLI_MODULE in text, (
        "ResolveZooSpecs does not invoke krepis.stage_coverage — it is back "
        "to the never-wired class alpha-engine-config-I10197 closed"
    )
    assert "--stage ResolveZooSpecs" in text, (
        "ResolveZooSpecs asserts under some OTHER stage name — the "
        "misattribution class alpha-engine-config-I10172 measured on "
        "DispatchWeeklyFreshnessSpot"
    )
    assert _EXECUTION_RUN_DATE_EXPORT in text
    assert "$.run_date" in text


def test_resolve_zoo_specs_assertion_cannot_pollute_the_spec_array_on_stdout() -> None:
    """`ParseZooSpecs` lifts this state's StandardOutputContent VERBATIM as
    the Map's ItemsPath — the state's own Comment says the git-pull preamble
    is redirected to stderr for exactly this reason. An assertion writing a
    single line to stdout would make the whole rotation unparseable, which is
    a far worse outcome than the missing verdict it fixes."""
    states = _sf_states()
    text = _ssm_command_text(states["ResolveZooSpecs"])
    match = re.search(r"[^']*krepis\.stage_coverage[^']*", text)
    assert match, "no stage_coverage command found"
    command = match.group(0)
    assert "1>&2" in command, (
        f"the assertion does not redirect its stdout to stderr: {command}"
    )


def test_resolve_zoo_specs_assertion_is_observe_mode_and_cannot_fail_the_state() -> None:
    """The state's own Comment: 'a resolve/parse failure routes to
    PublishModelZooFailureImmediate' — under `set -eo pipefail` an unguarded
    assertion would route the whole model-zoo rotation to its failure branch
    on a coverage CLI hiccup. `|| echo ... >&2` rather than `|| true` keeps an
    unreachable assertion distinguishable from a covered stage."""
    states = _sf_states()
    text = _ssm_command_text(states["ResolveZooSpecs"])
    match = re.search(r"[^']*krepis\.stage_coverage[^']*", text)
    assert match
    command = match.group(0)
    assert "|| echo" in command, f"assertion is not guarded: {command}"
    assert "|| true" not in command, (
        "`|| true` erases the difference between an unreachable assertion and "
        "a covered stage"
    )


def test_resolve_zoo_specs_assertion_runs_after_the_spec_resolution() -> None:
    """The window is only meaningful once the workload has run, and the
    array-emitting command must not be displaced from the tail of the list
    where its exit status governs the state."""
    states = _sf_states()
    text = _ssm_command_text(states["ResolveZooSpecs"])
    assert text.index("list-rotation-specs") < text.index("krepis.stage_coverage")


# ── config-I8155: the Lambda-backed coverage stages get $.run_date too ───────
#
# The SSM half above carries EXECUTION_RUN_DATE to the launcher scripts. The
# Lambda half needs the same identity in its EVENT, and five states did not
# have it: WeeklyRunDayGate, LibPinDriftCheck, PipelineContractCheck (all
# `alpha-engine-predictor:live`) and RegimeSubstrate /
# RegimeRetrospectiveEval carried Payloads holding ONLY `action`.
#
# Those five wrote a CORRECT run_date on 2026-08-22 — and only by
# coincidence. Their handlers substituted `datetime.now(timezone.utc).date()`
# or a calendar date derived from it, which equals `$.run_date` exactly when
# the stage runs on the same UTC day the execution started. An execution
# beginning 23:50 UTC, a stage crossing midnight, or any redrive of an older
# run_date splits them — and the split is invisible, because the verdict
# still lands under a plausible-looking prefix.
#
# "Right for the wrong reason" is not a passing state for an identity field.

_LAMBDA_COVERAGE_STATES: dict[str, str] = {
    "WeeklyRunDayGate": "alpha-engine-predictor-inference",
    "LibPinDriftCheck": "alpha-engine-predictor-inference",
    "PipelineContractCheck": "alpha-engine-predictor-inference",
    "RegimeSubstrate": "alpha-engine-predictor-regime-substrate",
    "RegimeRetrospectiveEval": "alpha-engine-predictor-regime-retrospective-eval",
}


@pytest.mark.parametrize("name", sorted(_LAMBDA_COVERAGE_STATES))
def test_every_lambda_coverage_state_threads_the_execution_run_date(name: str) -> None:
    states = _sf_states()
    assert name in states, f"{name}: state not found in the live SF definition"
    payload = states[name]["Parameters"]["Payload"]
    assert payload.get("run_date.$") == "$.run_date", (
        f"{name}: Payload does not thread $.run_date, so its handler has no "
        "way to learn the execution's identity and must either fabricate one "
        "(forbidden) or record UNMEASURED. Payload is "
        f"{sorted(payload)} (alpha-engine-config-I8155)."
    )


def test_no_lambda_coverage_state_substitutes_a_derived_date_for_run_date() -> None:
    """`$.run_date` and nothing else. A Payload wiring `run_date` to a
    trading-day or cycle field would satisfy the presence test above while
    reintroducing exactly the split it exists to prevent."""
    states = _sf_states()
    for name in sorted(_LAMBDA_COVERAGE_STATES):
        wired = states[name]["Parameters"]["Payload"].get("run_date.$")
        assert wired == "$.run_date", (
            f"{name}: run_date is wired to {wired!r}. It must be the "
            "execution's own $.run_date — cycle_date is derived separately by "
            "krepis via last_closed_trading_day() and is a different question."
        )


def test_the_lambda_coverage_state_set_is_not_silently_smaller_than_reality() -> None:
    """Guard the guard. The curated set above is a claim about the pipeline;
    if a Lambda-backed Task state's function is one of the named coverage
    functions and it is absent from the set, the set is stale and this test —
    not a quiet pass — is what says so."""
    states = _sf_states()
    functions = set(_LAMBDA_COVERAGE_STATES.values())
    discovered = set()
    for name, body in states.items():
        if body.get("Type") != "Task" or "lambda:invoke" not in body.get("Resource", ""):
            continue
        fn = str(body.get("Parameters", {}).get("FunctionName", ""))
        if any(fn.startswith(f"{f}:") or fn == f for f in functions):
            discovered.add(name)
    unlisted = sorted(discovered - set(_LAMBDA_COVERAGE_STATES))
    assert not unlisted, (
        "Lambda state(s) on a coverage-asserting function are missing from "
        f"_LAMBDA_COVERAGE_STATES: {unlisted}. Add them (and thread "
        "$.run_date into their Payloads) or the curated set is a smaller "
        "world than its name implies."
    )
    assert discovered, "the lambda-state scan found nothing — it cannot pass vacuously"


# ---------------------------------------------------------------------------
# Every stage-coverage verdict producer is handed the execution that produced it
# (alpha-engine-config-I9247)
#
# `_stage_coverage/` verdict records carried `execution_arn: None` on EVERY
# object, so a deploy-time probe's verdict and a real execution's verdict were
# indistinguishable in the artifact. `crucible-predictor-PR578` (merged
# 2026-08-29) now persists `execution_arn` + `invocation_kind` onto those
# records — but it populates `execution_arn` FROM THE EVENT, so a Task that does
# not thread it writes the same None it always did and the fix is inert.
#
# Why it is load-bearing rather than cosmetic: `alpha-engine-config-I8155`'s
# gate predicate is a raw object COUNT over
# `s3://alpha-engine-research/_stage_coverage/{date}/`, which probe writes
# inflate. The hardened predicate counts only records with a NON-NULL
# `execution_arn` — and that predicate counts zero, and strands the gate, until
# this threading lands (`alpha-engine-config-I9248`).
# ---------------------------------------------------------------------------

_EXECUTION_ARN_REQUIRED = (
    "WeeklyRunDayGate",
    "LibPinDriftCheck",
    "PipelineContractCheck",
    "RegimeSubstrate",
    "RegimeRetrospectiveEval",
)


def _all_states(node, out=None):
    """Every state in a definition, INCLUDING the ones nested inside a Parallel
    branch or a Map's ItemProcessor.

    A top-level `["States"]` walk is not sufficient here and quietly returns the
    wrong answer: `RegimeSubstrate` and `RegimeRetrospectiveEval` live inside
    `ResearchPredictorParallel`'s Branch 0 (config#885 relocated the
    Scanner→RAG→Regime chain into it), so a flat scan reports them ABSENT
    rather than unwired.
    """
    out = {} if out is None else out
    if isinstance(node, dict):
        states = node.get("States")
        if isinstance(states, dict):
            for name, body in states.items():
                out[name] = body
                _all_states(body, out)
        for key in ("Branches", "ItemProcessor", "Iterator"):
            value = node.get(key)
            if isinstance(value, dict):
                _all_states(value, out)
            elif isinstance(value, list):
                for branch in value:
                    _all_states(branch, out)
    return out


def test_stage_coverage_producers_thread_the_execution_arn():
    import json as _json
    import pathlib as _pathlib

    definition = _json.loads(
        (_pathlib.Path(__file__).resolve().parents[1]
         / "infrastructure" / "step_function.json").read_text()
    )
    states = _all_states(definition)
    for name in _EXECUTION_ARN_REQUIRED:
        assert name in states, f"{name} is not in the weekly definition at all"
        payload = states[name].get("Parameters", {}).get("Payload", {})
        assert payload.get("execution_arn.$") == "$$.Execution.Id", (
            f"{name}'s Payload does not thread execution_arn — its stage-coverage "
            "verdicts will keep writing execution_arn: None and stay "
            "indistinguishable from a deploy-time probe's "
            "(alpha-engine-config-I9247)"
        )


# ── alpha-engine-config-I10194 §1 step (1): the partition-split writers ──
#
# `Counterfactual`, `RationaleClustering`, `EvalRollingMean` and
# `EvalJudgeSubmitFirstSaturday` are `crucible-research`
# Lambda handlers. Each derives its OWN `run_date` for the
# `krepis.stage_coverage.assert_stage_coverage` call from
# `event["end_time_iso"]` — the RAW calendar `$$.Execution.StartTime` — because
# none of their Payloads carried `$.run_date` at all. `end_time_iso` is a
# calendar instant; `$.run_date` is the cycle's TRADING day, stamped once by
# InitializeInput and never rewritten. They differ on any weekend cycle, and
# the split is invisible: the verdict still lands under a plausible-looking
# prefix (the `alpha-engine-config-I8155` class, at the Lambda half).
#
# This half of the fix is INERT ON ITS OWN and that is expected: the
# handlers must also be changed to PREFER `event.get("run_date")` over the
# derived value (`alpha-engine-config-I10194` §1 step (2), `crucible-research`,
# a separate PR). Until that lands the handlers ignore the extra Payload key,
# so this change is safe to merge first and cannot regress anything.
#
# The producer-side assertion belongs HERE regardless: this repo owns the SF
# definition, so this repo is where the carrier's absence is detectable.

_PARTITION_SPLIT_RESEARCH_STATES = (
    "Counterfactual",
    "RationaleClustering",
    # `ReplayConcordance` left this tuple with its stage under
    # alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b).
    "EvalRollingMean",
    "EvalJudgeSubmitFirstSaturday",
)


@pytest.mark.parametrize("name", _PARTITION_SPLIT_RESEARCH_STATES)
def test_research_lambda_states_carry_the_execution_run_date(name: str) -> None:
    states = _sf_states()
    assert name in states, f"{name}: state not found in the live SF definition"
    payload = (states[name].get("Parameters") or {}).get("Payload") or {}
    assert payload.get("run_date.$") == "$.run_date", (
        f"{name}: Payload does not carry run_date.$ = $.run_date — its handler "
        "must derive a run_date from end_time_iso, which is a CALENDAR instant "
        "and not the cycle's trading day (alpha-engine-config-I10194 §1)"
    )


@pytest.mark.parametrize("name", _PARTITION_SPLIT_RESEARCH_STATES)
def test_run_date_is_added_alongside_the_existing_payload_keys(name: str) -> None:
    """`run_date` is ADDED, never a replacement: `end_time_iso` still drives
    each handler's own analysis window, and `EvalJudgeSubmitFirstSaturday`'s
    `date` is the eval cadence's date, a different question again. Removing
    either would be a silent behaviour change dressed as an identity fix."""
    states = _sf_states()
    payload = (states[name].get("Parameters") or {}).get("Payload") or {}
    if name == "EvalJudgeSubmitFirstSaturday":
        assert payload.get("date.$") == "$.eval_cadence.eval_date"
    else:
        assert payload.get("end_time_iso.$") == "$$.Execution.StartTime"
