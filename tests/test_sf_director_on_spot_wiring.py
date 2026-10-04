"""The weekly ``Director`` runs on the launcher spot over SSM, bounded
(alpha-engine-config-I11936, Brian's 2026-10-03 ruling to move it off Lambda).

The Director's plan call outgrew AWS Lambda's 900s maximum. These tests pin the
replacement's shape so it cannot quietly regress to either failure:

* back onto a Lambda (where the 900s cap kills a long plan call), or
* onto an UNBOUNDED command (the Director's failure is terminal, config#6408,
  so a hang would hold the whole weekly run).

The bound numbers mirror ``crucible-evaluator/director/hosting.py``
(``WEEKLY_SPOT_SSM_EXECUTION_TIMEOUT_S`` = 2,700s wall + 900s provisioning
allowance). Neither repo's CI can read the other, so the number is restated
here deliberately; a change on either side is a lockstep PR pair.
"""
from __future__ import annotations

import json
import pathlib
import re

import pytest

from infrastructure.sf_commands import UnresolvedReference, render_commands

_WEEKLY = pathlib.Path(__file__).parent.parent / "infrastructure" / "step_function.json"

#: crucible-evaluator director/hosting.py WEEKLY_SPOT_SSM_EXECUTION_TIMEOUT_S.
SSM_EXECUTION_TIMEOUT_S = 3600
#: Poll cadence and cap: 72 x 60s = 4,320s >= the executionTimeout, so the poll
#: loop outlives the command and always observes its terminal status.
POLL_CAP = 72
POLL_WAIT_S = 60

EXIT_ROUTES = {
    20: "RecordDirectorDegraded",
    21: "RecordDirectorRetroRefused",
    22: "RecordDirectorDegradedRetroRefused",
}


def _walk(states: dict):
    for name, st in states.items():
        yield name, st
        for b in st.get("Branches", []) or []:
            yield from _walk(b["States"])


@pytest.fixture(scope="module")
def states() -> dict:
    return dict(_walk(json.loads(_WEEKLY.read_text())["States"]))


def test_director_is_an_ssm_command_on_the_weekly_spot_not_a_lambda(states):
    d = states["Director"]
    assert d["Type"] == "Task"
    assert d["Resource"] == "arn:aws:states:::aws-sdk:ssm:sendCommand"
    assert "lambda" not in d["Resource"]
    p = d["Parameters"]
    assert p["DocumentName"] == "AWS-RunShellScript"
    assert p["InstanceIds.$"] == "$.ec2_instance_id"


def test_director_command_is_bounded(states):
    d = states["Director"]
    timeout = int(d["Parameters"]["Parameters"]["executionTimeout"][0])
    assert timeout == SSM_EXECUTION_TIMEOUT_S
    # The Task timeout must sit ABOVE executionTimeout so SSM's kill lands
    # first and the poll reads a named status (tests/test_sf_lambda_timeout_ordering.py).
    assert d["TimeoutSeconds"] > timeout


def test_the_deadline_exported_to_the_box_matches_the_execution_timeout(states):
    cmds = states["Director"]["Parameters"]["Parameters"]["commands.$"]
    m = re.search(r"DIRECTOR_SSM_DEADLINE_EPOCH=\$\(\( \$\(date \+%s\) \+ (\d+) \)\)", cmds)
    assert m, "the command must export the SSM deadline before anything else"
    assert int(m.group(1)) == SSM_EXECUTION_TIMEOUT_S
    assert "--deadline-epoch $DIRECTOR_SSM_DEADLINE_EPOCH" in cmds


def test_the_box_entrypoint_is_the_evaluator_script(states):
    cmds = states["Director"]["Parameters"]["Parameters"]["commands.$"]
    assert "crucible-evaluator/infrastructure/director_on_box.sh" in cmds
    assert "krepis.ssm_log_capture run" in cmds and "--slug director" in cmds


def test_the_command_renders_with_the_gate_state_as_one_json_line(states):
    gate_state = {"schema_version": 1, "gate_degraded": False, "note": "a b"}
    bindings = {"run_date": "2026-10-02", "research_dry": False,
                "director_gate_state": gate_state}
    context = {"Execution": {"Name": "exec-under-test"}}
    rendered = render_commands(states["Director"], bindings, context)
    line = json.dumps(gate_state, separators=(",", ":"))
    i = rendered.index(line)
    assert rendered[i - 1].startswith("cat > /tmp/director-gate-state-exec-under-test.json")
    assert rendered[i + 1] == "DIRECTOR_GATE_STATE_EOF"
    tail = rendered[-1]
    assert "--date 2026-10-02" in tail and "--dry-run false" in tail
    assert "--gate-state-file /tmp/director-gate-state-exec-under-test.json" in tail


def test_rendering_refuses_an_absent_gate_state(states):
    with pytest.raises(UnresolvedReference):
        render_commands(
            states["Director"],
            {"run_date": "2026-10-02", "research_dry": False},
            {"Execution": {"Name": "x"}},
        )


def test_poll_loop_is_capped_and_outlives_the_command(states):
    chk = states["CheckDirectorStatus"]
    caps = [
        c["NumericLessThan"]
        for rule in chk["Choices"] for c in rule.get("And", [])
        if c.get("Variable") == "$.director_polls" and "NumericLessThan" in c
    ]
    assert caps == [POLL_CAP]
    assert states["DirectorPollWait"]["Seconds"] == POLL_WAIT_S
    assert POLL_CAP * POLL_WAIT_S >= SSM_EXECUTION_TIMEOUT_S
    assert chk["Default"] == "DirectorLivenessGate"


def test_exit_codes_route_to_the_completed_records(states):
    chk = states["CheckDirectorStatus"]
    seen = {}
    for rule in chk["Choices"]:
        conds = {c.get("Variable"): c for c in rule.get("And", [])}
        rc = conds.get("$.director_poll.ResponseCode")
        if rc:
            assert conds["$.director_poll.Status"]["StringEquals"] == "Failed"
            seen[rc["NumericEquals"]] = rule["Next"]
    assert seen == EXIT_ROUTES
    assert chk["Choices"][0]["Next"] == "RecordDirectorClean"


@pytest.mark.parametrize("record,status,refused,code", [
    ("RecordDirectorClean", "not_degraded", False, 0),
    ("RecordDirectorDegraded", "degraded", False, 20),
    ("RecordDirectorRetroRefused", "not_degraded", True, 21),
    ("RecordDirectorDegradedRetroRefused", "degraded", True, 22),
])
def test_records_rebuild_the_two_fields_downstream_reads(states, record, status, refused, code):
    st = states[record]
    payload = st["Parameters"]["Payload"]
    assert (payload["status"], payload["retro_refused"], payload["exit_code"]) == (status, refused, code)
    assert st["ResultPath"] == "$.director_result"
    assert st["Next"] == "CheckDirectorRetroRefused"


def test_skip_director_is_in_the_box_dispatch_bypass(states):
    """A Director-only recovery must still dispatch a box: if skip_director
    were absent from the conjunct, a rerun skipping every box stage but the
    Director would bypass the dispatch and reach the Director with no instance."""
    choice = next(c for c in states["CheckSpotDispatchNeeded"]["Choices"] if "And" in c)
    flags = set()
    for c in choice["And"]:
        for leaf in c.get("And", []):
            if "BooleanEquals" in leaf:
                flags.add(leaf["Variable"])
    assert "$.skip_director" in flags


def test_a_lost_box_is_a_distinguishable_terminal_failure(states):
    """A spot reclaim reads as Director/SubstrateLost, not as a Director bug,
    and stays terminal — see tests/test_sf_substrate_relaunch_wiring.py
    TERMINAL_SUBSTRATE_LOST for why it does not take the relaunch."""
    gate = states["DirectorLivenessGate"]
    assert gate["Choices"][0]["Next"] == "ExtractDirectorSubstrateLostError"
    assert gate["Default"] == "ExtractDirectorError"
    lost = states["ExtractDirectorSubstrateLostError"]
    assert lost["Parameters"]["phase"] == "Director/SubstrateLost"
    assert lost["Next"] == "NormalizeFailureContext"


def test_every_director_failure_stays_terminal(states):
    d = states["Director"]
    assert [c["Next"] for c in d["Catch"]] == ["NormalizeFailureContext"]
    assert states["ExtractDirectorError"]["Next"] == "NormalizeFailureContext"
    assert states["ExtractDirectorSubstrateLostError"]["Next"] == "NormalizeFailureContext"
