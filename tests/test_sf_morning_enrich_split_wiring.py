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
        assert states["InitializeInput"]["Next"] == "CheckMutexRole", (
            "InitializeInput now hands off to the L274 mutex gate; see "
            "tests/test_sf_mutex_wiring.py for the mutex-chain contract"
        )
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

    def test_skip_morning_enrich_default_runs_morning_enrich(self, states):
        assert states["CheckSkipMorningEnrich"]["Default"] == "MorningEnrich"

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

    def test_status_default_extracts_error(self, states):
        assert (
            states["CheckMorningEnrichStatus"]["Default"]
            == "ExtractMorningEnrichError"
        )

    def test_morning_enrich_is_reachable_before_data_phase1(self, sf, states):
        """Walk the HAPPY path from StartAt (skip-gates take Default = run
        the action; status checks take the Success choice) and assert
        MorningEnrich is visited strictly before DataPhase1."""
        order: list[str] = []
        seen: set[str] = set()
        cur = sf["StartAt"]
        while cur and cur in states and cur not in seen:
            seen.add(cur)
            order.append(cur)
            st = states[cur]
            if st.get("Type") == "Choice":
                # Status checks: follow the Success edge (the real
                # forward path). Skip-gates have no Success edge → fall
                # back to Default (= run the action, the no-skip path).
                success = [
                    c["Next"]
                    for c in st.get("Choices", [])
                    if c.get("StringEquals") == "Success"
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
        alpha_engine_lib.ssm_log_capture Python CLI (lib v0.25.0), not
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
            if "alpha_engine_lib.ssm_log_capture run" in c
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
    """Both new Task states must Catch States.ALL → HandleFailure with
    ResultPath $.error, exactly like the DataPhase1 / RAGIngestion
    quartets (the SF halts on infra failure of these states)."""

    @pytest.mark.parametrize("name", ["MorningEnrich", "WaitForMorningEnrich"])
    def test_catch_routes_to_handle_failure(self, states, name):
        catches = states[name]["Catch"]
        assert len(catches) >= 1
        for c in catches:
            assert c["ErrorEquals"] == ["States.ALL"]
            assert c["Next"] == "HandleFailure"
            assert c["ResultPath"] == "$.error"

    def test_extract_error_routes_to_handle_failure(self, states):
        st = states["ExtractMorningEnrichError"]
        assert st["Type"] == "Pass"
        assert st["ResultPath"] == "$.error"
        assert st["Next"] == "HandleFailure"
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
