"""Pins config#1645 groom-dispatch SF relaunch recovery wiring.

Origin: 2026-07-06 — Opus groom died ~7 min in (spot/OOM); the dispatch SF
detected marker-absent and entered PrepRelaunch, then ExecutionFailed at
NotifyRelaunch with States.Runtime because PrepRelaunch dropped $.groomPoll
(and CheckCompletionMarker headObject returned 403 — IAM had GetObject but not
HeadObject). Recovery never launched a second box.

This test catches regressions like:
- NotifyRelaunch blocking LaunchGroomSpot (notify must follow launch)
- SF execution role missing s3:HeadObject for the completion-marker check

config#2129: the per-box relaunch lifecycle (LaunchGroomSpot/PrepRelaunch/
SetForceOnDemand/NotifyRelaunch/...) moved from the SF's TOP-LEVEL states into
MapLaunches's ItemProcessor (one iteration per co-launched tier) — the
`states` fixture below now reads that nested processor, not the top level.

2026-07-27 — INVERTED GUARD REMOVED. Until today this module asserted that
PrepRelaunch and SetForceOnDemand *must preserve* `groomPoll.$`/`groomLaunch.$`.
That was correct while the SSM poll loop produced those fields. I4333 replaced
the poll loop with a task-token callback and removed the producer — but the
assertions stayed, so CI stayed green **because** the dangling reference was
still present, while every live run died on an uncatchable States.Runtime at
PrepRelaunch. A regression guard is scoped to the design it guards; when a
migration retires a producer, every guard asserting its output goes with it
(groom-sweep-policy §9). The general form of this check now lives in
`test_sf_groom_field_reachability.py`, which derives what each state may
reference instead of hardcoding a field list.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_groom.json"
_IAM_PATH = (
    _REPO_ROOT
    / "infrastructure"
    / "lambdas"
    / "scheduled-groom-dispatcher"
    / "sf-execution-iam-policy.json"
)

#: What a relaunch-path Pass state MUST carry forward, post-I4333. These are
#: exactly the fields the states downstream of it read: LaunchGroomSpot's Payload
#: merge (schedInput/launchDecision/fod), RelaunchNotifyGate's Choice
#: (retry_count) and CheckRetryBudget's Choice (max_retries).
_PRESERVE_PATHS = (
    "schedInput.$",
    "launchDecision.$",
    "retry_count.$",
    "max_retries.$",
)

#: Retired with the SSM poll loop by I4333. Nothing produces these any more, so a
#: reference to either is an uncatchable States.Runtime on the path that reads it.
#: `$.callbackOutput.groomLaunch` is a DIFFERENT thing — the box's send-task-success
#: payload — and is legitimate.
_RETIRED_FIELDS = ("$.groomPoll", "$.groomLaunch")


@pytest.fixture(scope="module")
def states() -> dict:
    doc = json.loads(_SF_PATH.read_text())["States"]
    return doc["MapLaunches"]["ItemProcessor"]["States"]


@pytest.fixture(scope="module")
def iam_policy() -> dict:
    return json.loads(_IAM_PATH.read_text())


def test_completion_marker_task_token_timeout_routes_to_retry(states):
    """config-I4333: when the task token times out, CheckCompletionMarkerTaskToken
    invokes the Lambda and routes to CheckRetryBudget for the relaunch decision."""
    st = states["CheckCompletionMarkerTaskToken"]
    assert st["Resource"] == "arn:aws:states:::lambda:invoke"
    assert st["Next"] == "CheckRetryBudget"


def test_sf_role_grants_head_object_on_completion_marker(iam_policy):
    marker_stmts = [
        s
        for s in iam_policy["Statement"]
        if s.get("Sid") == "CheckGroomRunCompletionMarker"
    ]
    assert len(marker_stmts) == 1
    actions = marker_stmts[0]["Action"]
    if isinstance(actions, str):
        actions = [actions]
    assert "s3:HeadObject" in actions
    assert "s3:GetObject" in actions


def test_prep_relaunch_carries_the_fields_its_successors_read(states):
    st = states["PrepRelaunch"]
    params = st["Parameters"]
    for key in _PRESERVE_PATHS:
        assert key in params, f"PrepRelaunch must carry {key} through relaunch"
    assert params["retry_count.$"] == "States.MathAdd($.retry_count, 1)"
    assert "fod.$" in params
    assert st["Next"] == "CheckForceOnDemand"


def test_set_force_on_demand_carries_the_fields_its_successors_read(states):
    st = states["SetForceOnDemand"]
    params = st["Parameters"]
    for key in _PRESERVE_PATHS:
        assert key in params
    assert st["Next"] == "LaunchGroomSpot"
    assert params["fod"] == {"force_on_demand": True, "launch_decided": True}


def test_no_state_references_a_retired_poll_loop_field(states):
    """The I4333 regression, pinned so it cannot return.

    `$.callbackOutput.groomLaunch` is explicitly allowed — that is the task-token
    payload the box sends, not the retired SSM-poll field.
    """
    offenders = []
    for name, state in states.items():
        blob = json.dumps({k: v for k, v in state.items() if k != "Comment"})
        blob = blob.replace("$.callbackOutput.groomLaunch", "")
        for field in _RETIRED_FIELDS:
            if field in blob:
                offenders.append(f"{name} references {field}")
    assert not offenders, "; ".join(offenders)


def test_relaunch_critical_path_launch_before_notify(states):
    """LaunchGroomSpot must precede NotifyRelaunch; notify must not gate relaunch."""
    assert states["CheckForceOnDemand"]["Default"] == "LaunchGroomSpot"
    assert states["SetForceOnDemand"]["Next"] == "LaunchGroomSpot"
    assert states["LaunchGroomSpot"]["Next"] == "RelaunchNotifyGate"
    assert states["RelaunchNotifyGate"]["Default"] == "CheckLaunchedCallback"
    relaunch_choices = states["RelaunchNotifyGate"]["Choices"]
    assert relaunch_choices[0]["Next"] == "NotifyRelaunch"
    assert states["NotifyRelaunch"]["Next"] == "CheckLaunchedCallback"
    assert states["NotifyRelaunch"]["Catch"][0]["Next"] == "CheckLaunchedCallback"


def test_notify_relaunch_message_uses_only_fields_prep_relaunch_emits(states):
    """A States.Format intrinsic error in Parameters is NOT catchable, so this
    message may reference only what PrepRelaunch/SetForceOnDemand actually emit."""
    msg = states["NotifyRelaunch"]["Parameters"]["Message.$"]
    emitted = {
        key[:-2]
        for key in states["PrepRelaunch"]["Parameters"]
        if key.endswith(".$")
    } | {"fod"}
    referenced = set(re.findall(r"(?<!\$)\$\.([A-Za-z_][A-Za-z0-9_]*)", msg))
    assert referenced <= emitted, (
        f"NotifyRelaunch reads {sorted(referenced - emitted)}, which PrepRelaunch "
        f"does not emit (emits: {sorted(emitted)})"
    )
    assert "States.JsonToString($.fod.force_on_demand)" in msg


def test_lane_timeout_is_the_sole_bound_no_unemitted_heartbeat(states):
    """groom-sweep-policy §2.1: a HeartbeatSeconds with no SendTaskHeartbeat
    emitter silently becomes the lane's real timeout. On 2026-07-27 a 3600s
    heartbeat sat beside a 21600s timeout with no emitter anywhere in the fleet,
    and every lane died at 3606s."""
    launch = states["LaunchGroomSpot"]
    assert launch["TimeoutSeconds"] == 10800
    assert "HeartbeatSeconds" not in launch, (
        "no SendTaskHeartbeat emitter exists on the groom box — a heartbeat here "
        "would become the real lane timeout"
    )


def test_every_lane_task_declares_a_timeout(states):
    """groom-sweep-policy §2.1: no unbounded state."""
    missing = [
        name
        for name, st in states.items()
        if st.get("Type") == "Task" and "TimeoutSeconds" not in st
    ]
    assert not missing, f"Task states with no declared timeout: {missing}"


def test_state_machine_declares_a_global_ceiling():
    """A runaway backstop, not a budget — the per-lane timeout is the budget."""
    doc = json.loads(_SF_PATH.read_text())
    ceiling = doc.get("TimeoutSeconds")
    assert ceiling, "the state machine must declare a top-level TimeoutSeconds"
    lane = doc["States"]["MapLaunches"]["ItemProcessor"]["States"]["LaunchGroomSpot"]
    max_attempts = doc["States"]["MapLaunches"]["ItemSelector"]["max_retries"] + 1
    assert ceiling > lane["TimeoutSeconds"] * max_attempts, (
        "the global ceiling must not preempt a legitimate full relaunch sequence"
    )


def test_top_level_tasks_declare_timeouts():
    """Same rule, applied outside the Map."""
    doc = json.loads(_SF_PATH.read_text())
    missing = [
        name
        for name, st in doc["States"].items()
        if st.get("Type") == "Task" and "TimeoutSeconds" not in st
    ]
    assert not missing, f"top-level Task states with no declared timeout: {missing}"


def test_task_token_travels_inside_the_lambda_payload(states):
    """`TaskToken` is not a field of the Lambda Invoke API.

    Step Functions rejects it as a sibling of `Payload` at RUNTIME, not at
    definition-validation time — `validate-state-machine-definition` returned
    OK on the broken form, and the 2026-07-27 20:00 UTC scheduled run failed
    11 seconds in with:

        The field "TaskToken" is not supported by Step Functions

    For `lambda:invoke.waitForTaskToken` the token must be carried inside the
    Payload, where the Lambda reads it off the event.
    """
    params = states["LaunchGroomSpot"]["Parameters"]
    assert "TaskToken" not in params and "TaskToken.$" not in params, (
        "TaskToken must not be a sibling of Payload — Step Functions rejects it"
    )
    assert "$$.Task" in params["Payload.$"], (
        "the task token must be merged into the Lambda Payload"
    )


def test_wait_for_task_token_resource_still_declared(states):
    """The token plumbing above is only meaningful with the callback integration."""
    assert states["LaunchGroomSpot"]["Resource"].endswith(".waitForTaskToken")


# ── I5229 regression: fast detection must ACCELERATE recovery, not replace it ──


def _lane_state():
    import json
    from pathlib import Path
    d = json.loads((Path(__file__).resolve().parents[1]
                    / "infrastructure" / "step_function_groom.json").read_text())
    m = [v for v in d["States"].values() if v.get("Type") == "Map"][0]
    return ((m.get("ItemProcessor") or m.get("Iterator"))["States"]
            ["LaunchGroomSpot"])


def test_lane_death_routes_to_the_relaunch_path_not_the_terminal_one():
    """The reconciler must not convert a recoverable death into a dead lane.

    The lane reconciler sends send-task-failure(error="LaneDeath") within ~5
    minutes of a box dying. Before this Catch entry existed that failure fell
    through to States.ALL -> HandleFailure, which is TERMINAL — so shipping the
    reconciler made recovery strictly WORSE: a spot reclaim used to recover via
    States.Timeout -> CheckCompletionMarkerTaskToken -> relaunch (3 attempts,
    the last forced to on-demand), and instead failed permanently.

    Measured live, 2026-07-30 20:00 UTC: all three lanes reclaimed for spot
    capacity, `LaunchGroomSpot` visited exactly 3 times (once per lane), zero
    visits to CheckCompletionMarkerTaskToken or PrepRelaunch.
    """
    catches = _lane_state()["Catch"]
    routes = {tuple(c["ErrorEquals"]): c["Next"] for c in catches}
    assert ("LaneDeath",) in routes, (
        "no Catch for the reconciler's LaneDeath error — a detected lane death "
        "falls to States.ALL and the lane never relaunches"
    )
    timeout_target = routes[("States.Timeout",)]
    assert routes[("LaneDeath",)] == timeout_target, (
        "LaneDeath must route to the SAME state as States.Timeout "
        f"({timeout_target}) — fast detection should accelerate the existing "
        "recovery, never bypass it"
    )


def test_lane_death_catch_precedes_the_catch_all():
    """Order matters: States.ALL first would make the LaneDeath entry dead."""
    catches = _lane_state()["Catch"]
    order = [tuple(c["ErrorEquals"]) for c in catches]
    assert order.index(("LaneDeath",)) < order.index(("States.ALL",)), (
        "the LaneDeath Catch sits after States.ALL and is therefore unreachable"
    )


# ── §2.1: ONE authoritative runtime bound for a lane ─────────────────────────


def _dispatcher_max_runtime() -> int:
    import re
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"
           / "scheduled-groom-dispatcher" / "index.py").read_text()
    m = re.search(r'GROOM_MAX_RUNTIME_SECONDS",\s*"(\d+)"', src)
    assert m, "could not read MAX_RUNTIME_SECONDS from the dispatcher"
    return int(m.group(1))


def test_runtime_bound_is_single_and_authoritative():
    """The box must never outlive the lane the SF is waiting on.

    groom-sweep-policy §2.1: "where two mechanisms bound the same thing, the
    TIGHTER one is the real budget regardless of intent."

    Measured 2026-07-30 — three copies that did not agree:

        SF LaunchGroomSpot TimeoutSeconds   10800s (180 min)  <- real budget
        dispatcher MAX_RUNTIME_SECONDS      21600s (360 min)
        bootstrap watchdog default          21600s (360 min)

    The failure is not cosmetic. The SF abandons the lane at 180 min and fires
    a relaunch while the ORIGINAL box works on for another three hours; the
    relaunch's concurrent-tier guard then refuses because that box is alive —
    the alpha-engine-config-I4987 no-op relaunch, manufactured on a timer. The
    reconciler was mis-armed identically: deadline_utc was now + 360 min, so a
    lane the SF had already given up on stayed 'not overdue' for three hours.
    """
    lane_timeout = _lane_state()["TimeoutSeconds"]
    box_bound = _dispatcher_max_runtime()
    assert box_bound <= lane_timeout, (
        f"the box may run {box_bound}s but the SF abandons the lane at "
        f"{lane_timeout}s — every timeout leaves an orphan box that blocks its "
        "own relaunch"
    )


def test_the_runtime_bound_reaches_the_box():
    """A constant the box never receives is not a bound.

    groom_spot_bootstrap.sh reads `${MAX_RUNTIME_SECONDS:-<default>}`. If the
    dispatcher does not export it, the box silently uses its own default and
    the value tested above governs nothing on the machine it is about.
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"
           / "scheduled-groom-dispatcher" / "index.py").read_text()
    assert "export MAX_RUNTIME_SECONDS=" in src, (
        "the dispatcher never exports MAX_RUNTIME_SECONDS, so the box falls "
        "back to its own default and the two can diverge freely"
    )
    assert "{runtime_bound_export}" in src, (
        "the export is built but never interpolated into the bootstrap command"
    )
