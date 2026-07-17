"""Pins the MorningEnrich → DataPhase1 split in the Saturday SF.

Origin: the preflight-task-split (2026-05-16, plan
alpha-engine-docs/private/preflight-task-split-260516.md). The standing
rule — every preflight-bearing action is its own SF task; a downstream
failure must never re-run a completed upstream task — was violated by
the old `DataPhase1` state, which ran `spot_data_weekly.sh --data-only`
= morning-enrich (~28 min) THEN phase1 on one spot. Every phase1
recovery re-paid the 28-min morning-enrich because its preflight was
buried 28 minutes deep.

This test catches regressions like:
- Someone reroutes InitializeInput back past CheckSkipMorningEnrich and
  silently drops the MorningEnrich state.
- Someone wires MorningEnrich AFTER DataPhase1 (re-introduces the
  re-run-the-28-min-step-on-phase1-failure bug).
- Someone reverts DataPhase1's SSM command back to `--data-only` (which
  re-bundles morning-enrich into phase1).
- Someone drops the HandleFailure Catch on the new states.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.sf_command_utils import extract_commands


_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    return sf["States"]


class TestQuartetPresence:
    """The MorningEnrich quartet (+ Wait/Extract helpers) must exist,
    mirroring the RAGIngestion / DataPhase1 quartets."""

    @pytest.mark.parametrize(
        "name",
        [
            "CheckSkipMorningEnrich",
            "MorningEnrich",
            "WaitForMorningEnrich",
            "CheckMorningEnrichStatus",
            "MorningEnrichWait",
            "ExtractMorningEnrichError",
            "MorningEnrichRetryGate",
            "MorningEnrichReissue",
        ],
    )
    def test_state_exists(self, states, name):
        assert name in states, f"{name} missing from Saturday SF States"


class TestChainOrdering:
    """InitializeInput → CheckSkipMorningEnrich → MorningEnrich →
    WaitForMorningEnrich → CheckMorningEnrichStatus(success) →
    CheckSkipDataPhase1 → DataPhase1 (existing downstream unchanged)."""

    def test_initialize_input_routes_to_morning_enrich_skipgate(self, states):
        # 2026-05-27: L274 SF MutualExclusionGuard inserted CheckMutexRole
        # between InitializeInput and CheckShellRun. The strict-superset
        # property still holds: CheckMutexRole.Default → CheckShellRun,
        # whose Default is the pre-spine target CheckSkipMorningEnrich. The
        # real Saturday run (no shell_run + with pipeline_role='weekly' that
        # acquires the mutex; or any non-cadence role that bypasses) still
        # reaches the MorningEnrich skip-gate first — MorningEnrich still
        # precedes DataPhase1.
        # 2026-06-08: L4517 inserted the lib-pin drift gate as the first
        # workload gate; its skip/check/gate Defaults converge on CheckMutexRole, so the
        # downstream mutex→CheckShellRun→CheckSkipMorningEnrich chain is unchanged.
        # config#830: a cadence-preset gate (CheckRunMode) now precedes the lib-pin
        # gate; CheckRunMode.Default → CheckSkipLibPinDriftCheck, so the chain holds.
        assert states["InitializeInput"]["Next"] == "CheckWeeklyRunDayGate", (
            "InitializeInput hands off to the config#1824 run-day gate, whose "
            "bypass Default -> CheckRunMode (config#830 cadence preset); "
            "CheckRunMode.Default → CheckSkipLibPinDriftCheck (the L4517 lib-pin "
            "gate); see tests/test_sf_lib_pin_drift_wiring.py for the gate→mutex chain"
        )
        assert states["CheckRunMode"]["Default"] == "CheckSkipLibPinDriftCheck"
        assert states["CheckMutexRole"]["Default"] == "CheckShellRun", (
            "Mutex bypass must route to CheckShellRun so the pre-mutex "
            "downstream chain is byte-identical for operator/missing-role inputs"
        )
        assert states["AcquireMutex"]["Next"] == "CheckShellRun", (
            "Mutex acquire path must also land at CheckShellRun so cadence "
            "runs reach the same downstream chain after grabbing the mutex"
        )
        assert states["CheckShellRun"]["Default"] == "CheckSkipMorningEnrich", (
            "CheckShellRun.Default must be CheckSkipMorningEnrich so the "
            "real Saturday run is byte-identical pre-spine."
        )

    def test_skip_morning_enrich_default_runs_substrate_gate_then_morning_enrich(self, states):
        """config#2249: CheckSkipMorningEnrich.Default now routes through the
        new fast pre-dispatch SubstrateHealthGate before MorningEnrich —
        HEALTHY proceeds to MorningEnrich as before; SUBSTRATE_UNHEALTHY
        short-circuits to a named failure WITHOUT ever entering
        MorningEnrich's own retry ladder."""
        assert states["CheckSkipMorningEnrich"]["Default"] == "SubstrateHealthGate"
        assert states["SubstrateHealthGate"]["Next"] == "CheckSubstrateHealthGate"
        healthy = [
            c["Next"]
            for c in states["CheckSubstrateHealthGate"]["Choices"]
            if c.get("StringEquals") == "HEALTHY"
        ]
        assert healthy == ["MorningEnrich"], (
            "SubstrateHealthGate verdict=HEALTHY must proceed into "
            "MorningEnrich unchanged"
        )
        assert (
            states["CheckSubstrateHealthGate"]["Default"]
            == "ExtractSubstrateHealthGateError"
        ), (
            "any non-HEALTHY verdict must short-circuit to the named-error "
            "path, not fall through into MorningEnrich's retry ladder"
        )
        assert states["ExtractSubstrateHealthGateError"]["Next"] == (
            "NormalizeFailureContext"
        )

    def test_skip_morning_enrich_honors_skip_flag(self, states):
        """{"skip_morning_enrich": true} must route to CheckSkipDataPhase1
        (mirrors the skip_data_phase1 / skip_rag_ingestion shape)."""
        choices = states["CheckSkipMorningEnrich"]["Choices"]
        assert len(choices) == 1
        c = choices[0]
        # And[ IsPresent, BooleanEquals true ] on $.skip_morning_enrich
        variables = {cond["Variable"] for cond in c["And"]}
        assert variables == {"$.skip_morning_enrich"}
        assert c["Next"] == "CheckSkipDataPhase1"

    def test_morning_enrich_routes_to_wait_state(self, states):
        assert states["MorningEnrich"]["Next"] == "WaitForMorningEnrich"

    def test_wait_routes_to_status_check(self, states):
        assert states["WaitForMorningEnrich"]["Next"] == "CheckMorningEnrichStatus"

    def test_status_success_routes_to_data_phase1_skipgate(self, states):
        success = [
            c["Next"]
            for c in states["CheckMorningEnrichStatus"]["Choices"]
            if c.get("StringEquals") == "Success"
        ]
        assert success == ["CheckSkipDataPhase1"], (
            "MorningEnrich success must hand off to CheckSkipDataPhase1 — "
            "DataPhase1 runs AFTER a completed MorningEnrich."
        )

    def test_status_inprogress_and_pending_loop_via_wait(self, states):
        nexts = {
            c["StringEquals"]: c["Next"]
            for c in states["CheckMorningEnrichStatus"]["Choices"]
        }
        assert nexts["InProgress"] == "MorningEnrichWait"
        assert nexts["Pending"] == "MorningEnrichWait"
        assert states["MorningEnrichWait"]["Next"] == "WaitForMorningEnrich"

    def test_status_default_routes_through_bounded_retry_to_error(self, states):
        """A non-Success poll status now routes through the bounded
        auto-retry gate (config#1059) before terminating at the error
        path: one idempotent re-issue, then ExtractMorningEnrichError on
        give-up. Keeps the scheduled run alive through a transient blip
        so it succeeds first-pass (moves unattended_first_pass_rate)."""
        assert (
            states["CheckMorningEnrichStatus"]["Default"]
            == "MorningEnrichRetryGate"
        )
        gate = states["MorningEnrichRetryGate"]
        # Give-up path: counter already consumed -> terminate at error.
        give_up = [
            c["Next"] for c in gate["Choices"]
            if c["Next"] == "ExtractMorningEnrichError"
        ]
        assert give_up == ["ExtractMorningEnrichError"], (
            "bounded-retry gate must still terminate at "
            "ExtractMorningEnrichError once the re-issue budget is spent"
        )
        # First-failure path: re-issue once, looping back to the step.
        assert gate["Default"] == "MorningEnrichReissue"
        reissue = states["MorningEnrichReissue"]
        assert reissue["Next"] == "MorningEnrich", (
            "re-issue must loop back to the MorningEnrich step for an "
            "idempotent re-run"
        )
        assert reissue["ResultPath"] == "$.morning_enrich_attempts"

    def test_morning_enrich_is_reachable_before_data_phase1(self, sf, states):
        """Walk the HAPPY path from StartAt (skip-gates take Default = run
        the action; status/verdict checks take the Success/HEALTHY choice)
        and assert MorningEnrich is visited strictly before DataPhase1."""
        order: list[str] = []
        seen: set[str] = set()
        cur = sf["StartAt"]
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                # Status/verdict checks: follow the Success/HEALTHY edge
                # (the real forward path). Skip-gates have no such edge →
                # fall back to Default (= run the action, the no-skip path).
                success = [
                    c["Next"]
                    for c in st.get("Choices", [])
                    if c.get("StringEquals") in ("Success", "HEALTHY")
                ]
                cur = success[0] if success else st.get("Default")
            else:
                cur = st.get("Next")
            if cur == "DataPhase1":
                order.append(cur)
                break
        assert "MorningEnrich" in order, order
        assert "DataPhase1" in order, order
        assert order.index("MorningEnrich") < order.index("DataPhase1"), (
            "MorningEnrich must precede DataPhase1 — the whole point of "
            "the split is that a phase1 failure never re-runs morning-enrich."
        )


class TestSsmCommandShape:
    """MorningEnrich invokes --morning-enrich-only; DataPhase1 switched
    from --data-only to --phase1-only."""

    def _commands(self, states, name):
        # commands.$ States.Array (keystone routed the final launch through
        # a States.Format($.preflight_args) suffix) — resolve via the
        # shared helper, which renders the Format element as its template.
        return extract_commands(states[name])

    def test_morning_enrich_invokes_morning_enrich_only(self, states):
        joined = " ".join(self._commands(states, "MorningEnrich"))
        assert "spot_data_weekly.sh --morning-enrich-only" in joined
        assert "--data-only" not in joined
        assert "--phase1-only" not in joined

    def test_data_phase1_invokes_phase1_only(self, states):
        joined = " ".join(self._commands(states, "DataPhase1"))
        assert "spot_data_weekly.sh --phase1-only" in joined, (
            "DataPhase1 must run --phase1-only post-split — --data-only "
            "re-bundles the 28-min morning-enrich into the phase1 task."
        )
        assert "--data-only" not in joined

    def test_morning_enrich_command_starts_with_pipefail(self, states):
        # Same invariant test_sf_ssm_pipefail_wiring.py pins globally;
        # asserted here too so a MorningEnrich-specific regression is
        # self-documenting.
        cmds = self._commands(states, "MorningEnrich")
        assert cmds[0].startswith("set ") and "pipefail" in cmds[0]

    def test_morning_enrich_log_capture_via_lib_cli(self, states):
        """The trap-and-log-ship invariant is now satisfied by the
        krepis.ssm_log_capture Python CLI (lib v0.25.0), not
        by an inline `trap 'aws s3 cp ...' EXIT` line. The 2026-05-22
        Friday-PM dry-pass caught the prior inline-trap form failing
        under ASL States.Array escape semantics (`\\'` not unescaped to
        `'` inside arg strings) — so we lifted to a Python CLI invoked
        as a single States.Format-rendered token list with no bash
        quoting surface. See alpha-engine-lib PR #57 + this state's
        sibling traps across the 7 other Saturday-SF spot states.
        """
        cmds = self._commands(states, "MorningEnrich")
        work_idx = next(
            i
            for i, c in enumerate(cmds)
            if "krepis.ssm_log_capture run" in c
        )
        work = cmds[work_idx]
        # Right slug and log path
        assert "--slug morning-enrich" in work
        assert "--log /var/log/morning-enrich.log" in work
        # Inner command is the morning-enrich launcher
        assert "-- bash infrastructure/spot_data_weekly.sh --morning-enrich-only" in work
        # No inline trap survives anywhere in this state
        assert not any(c.startswith("trap ") for c in cmds), (
            "Inline `trap 'aws s3 cp ...' EXIT` line must not coexist "
            "with the lib CLI — the CLI internalizes the trap. Two "
            "competing log-ship paths can race on the same /var/log "
            "file."
        )


class TestCatchSemantics:
    """Both new Task states must Catch States.ALL → NormalizeFailureContext
    (config#1819: the single chokepoint in front of HandleFailure, was
    HandleFailure directly pre-fix) with ResultPath $.error, exactly like the
    DataPhase1 / RAGIngestion quartets (the SF halts on infra failure of
    these states)."""

    @pytest.mark.parametrize("name", ["MorningEnrich", "WaitForMorningEnrich"])
    def test_catch_routes_to_handle_failure(self, states, name):
        catches = states[name]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == "NormalizeFailureContext"
            assert c["ResultPath"] == "$.error"

    def test_extract_error_routes_to_handle_failure(self, states):
        st = states["ExtractMorningEnrichError"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "NormalizeFailureContext"
        assert st["Parameters"]["phase"] == "MorningEnrich"


class TestResultPathIsolation:
    """MorningEnrich must not stomp on DataPhase1's SSM result path."""

    def test_distinct_result_paths(self, states):
        assert (
            states["MorningEnrich"]["ResultPath"]
            != states["DataPhase1"]["ResultPath"]
        )
        assert states["MorningEnrich"]["ResultPath"] == "$.morning_enrich_result"

    def test_wait_reads_morning_enrich_command_id(self, states):
        cmd_id = states["WaitForMorningEnrich"]["Parameters"]["CommandId.$"]
        assert "morning_enrich_result" in cmd_id
