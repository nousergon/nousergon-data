"""alpha-engine-config-I11106 — ModelZooSelect's declared-unservable path.

The 2026-09-19 weekly run terminated DEGRADED because ModelZooSelect reported
failure, but the stage did its job: it evaluated the M-slot arms and correctly
concluded nothing qualifies for promotion. That is a VERDICT
(sf-pipeline-policy.md §2.3a), not a stage failure, and the SF previously could
not tell the two apart — both rendered as a non-Success poll.

The MODEL_ZOO_SELECT_OUTCOME v1 contract (crucible-predictor's
spot-model-zoo-select workload) now prints exactly one stdout line and exits 0
on both a genuine decided outcome and a correctly-refused (unservable) one.
CheckModelZooStatus splits a Success poll on that marker: the declared-unservable
edge sets the branch-local $.model_zoo_unservable and continues to BranchBComplete
without ever touching MarkModelZooDegraded / $.research_degraded_local /
$.degraded_summary — a genuine failure/timeout is completely unchanged.

alpha-engine-config-I11101 deliverable 4 (2026-09-19) replaced HOW that split is
made. The first implementation read the marker with StringMatches on the SSM
poll's StandardOutputContent, and it failed on its first real run: SSM caps
StandardOutputContent at ~24,000 characters and appends "--output truncated--",
the model-zoo-select log on watch-rerun-2026-09-18-1 was 28,759 bytes, and
MODEL_ZOO_SELECT_OUTCOME is its LAST line. The marker sat 4.7 KB past the SF's
horizon, the unservable arm never matched, and the run stamped
model_zoo_unservable=false into both completion markers while the arena's own
decision block read "unservable".

So the verdict is no longer scraped from a log stream at all. ReadModelZooArenaCycle
reads arena/model/{run_date}.verdict — a plain-text projection of the same
validated decision.status, written by emit_cycle in the same call as the cycle
JSON — and CheckModelZooVerdict compares the WHOLE body against one word.

It is a bare word rather than the cycle JSON because neither alternative is safe
inside Step Functions, both measured on 2026-09-19:

  * States.StringToJson on the cycle body raises States.Runtime on a malformed
    artifact, and that error is NOT CATCHABLE. Verified against live Step
    Functions, including from inside a Parallel whose Catch names States.ALL.
    An earlier revision of this change shipped that parse and would have taken
    the whole weekly run down on a bad artifact.
  * String-matching the rendered cycle JSON re-introduces the class being
    removed: decision.comparisons[].status is a second "status" field in the
    same document, so a substring match is a false-positive surface.

Absence, an unreadable object and an unrecognised word are all UNKNOWN and
degrade honestly (sf-pipeline-policy.md §2.3a); none resolves to the passing
value.

These are structural walks over the definition, not live executions.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"


@pytest.fixture
def doc():
    return json.loads((_INFRA / "step_function.json").read_text())


@pytest.fixture
def branch_b(doc):
    return doc["States"]["ResearchPredictorParallel"]["Branches"][1]["States"]


def _edges(state: dict) -> list[str]:
    out = []
    if "Next" in state:
        out.append(state["Next"])
    if "Default" in state:
        out.append(state["Default"])
    for rule in state.get("Choices", []) or []:
        if "Next" in rule:
            out.append(rule["Next"])
    for catch in state.get("Catch", []) or []:
        if "Next" in catch:
            out.append(catch["Next"])
    return out


def _first_path_to(states: dict, start: str, stop: str, max_steps: int = 200) -> list[str]:
    """DFS from `start` to the first reachable `stop`, returning the path taken.

    Fails loudly if `stop` is unreachable, or if the walk references a state
    name that does not exist in `states` — a silent typo in a `Next`/`Default`
    is exactly the class this guard exists to catch before AWS does.
    """
    seen: set[str] = set()
    stack = [[start]]
    while stack:
        path = stack.pop()
        node = path[-1]
        if node == stop:
            return path
        if node in seen or len(path) > max_steps:
            continue
        seen.add(node)
        state = states.get(node)
        assert state is not None, f"path {path} references undefined state {node!r}"
        for nxt in _edges(state):
            stack.append(path + [nxt])
    raise AssertionError(f"{stop!r} not reachable from {start!r} within {max_steps} steps")


def test_unservable_marker_reaches_branch_b_complete_without_degrading(branch_b):
    """An unservable marker on a Success poll must reach BranchBComplete
    without entering any Mark*Degraded state and without writing
    $.degraded_summary anywhere along the way."""
    path = _first_path_to(branch_b, "ModelZooUnservableDeclared", "BranchBComplete")

    degraded_states = [n for n in path if "Degraded" in n]
    assert not degraded_states, (
        "a declared-unservable run must never enter a Mark*Degraded state: "
        f"path was {path}"
    )
    for name in path:
        state = branch_b[name]
        assert state.get("ResultPath") != "$.degraded_summary", (
            f"{name} writes $.degraded_summary on the declared-unservable path: {path}"
        )


def test_unservable_declared_never_touches_research_degraded_local(branch_b):
    """The arena's own correct-refusal verdict must never fold into
    $.research_degraded_local — that would render a correct refusal as though
    ResearchPredictorParallel itself had broken (sf-pipeline-policy.md §2.3a)."""
    for name in ("ModelZooUnservableDeclared", "PublishModelZooUnservableNotice"):
        state = branch_b[name]
        assert state.get("ResultPath") != "$.research_degraded_local"
        params = state.get("Parameters") or {}
        assert "research_degraded_local" not in json.dumps(params)


def test_success_poll_goes_to_the_arena_read_not_a_stdout_match(branch_b):
    """The one Success arm hands off to ReadModelZooArenaCycle. There is no
    longer a second Success arm racing it, so Choice ordering is not what keeps
    the unservable case reachable — the artifact read is."""
    rules = branch_b["CheckModelZooStatus"]["Choices"]
    success_rules = [
        r
        for r in rules
        if r.get("Variable") == "$.model_zoo_poll.Status"
        and r.get("StringEquals") == "Success"
    ]
    assert len(success_rules) == 1, (
        "exactly one Success arm: a second one re-creates the ordering hazard "
        "the artifact read exists to remove"
    )
    assert success_rules[0]["Next"] == "ReadModelZooArenaCycle"


def test_no_verdict_is_decided_by_matching_standard_output_content(branch_b):
    """THE REGRESSION GUARD, and the reason this file exists in its current
    form. SSM truncates StandardOutputContent at ~24,000 characters, so any
    Choice that decides a VERDICT by matching text in it is correct only while
    the log stays short — measured broken on watch-rerun-2026-09-18-1 at 28,759
    bytes. A StandardOutputContent match may still classify a FAILURE (the
    resource-kill idiom); it may never establish an outcome."""
    offenders = []
    for name, state in branch_b.items():
        if state.get("Type") != "Choice":
            continue
        for rule in state.get("Choices", []) or []:
            for cond in [rule] + (rule.get("And") or []) + (rule.get("Or") or []):
                if (
                    cond.get("Variable") == "$.model_zoo_poll.StandardOutputContent"
                    and "StringMatches" in cond
                ):
                    offenders.append((name, cond["StringMatches"], rule.get("Next")))
    assert not offenders, (
        "model-zoo verdict decided by matching truncatable SSM stdout: "
        f"{offenders} — read arena/model/{{run_date}}.json::decision.status instead "
        "(alpha-engine-config-I11101 deliverable 4)"
    )


def test_arena_read_uses_the_dated_key_never_latest(branch_b):
    """latest.verdict is whatever ran most recently, which on a recovery arc
    need not be the cycle being recovered. The read must key on $.run_date."""
    params = branch_b["ReadModelZooArenaCycle"]["Parameters"]
    assert params["Bucket"] == "alpha-engine-research"
    key = params["Key.$"]
    assert key == "States.Format('arena/model/{}.verdict', $.run_date)"
    assert "latest" not in key


def test_nothing_on_this_path_parses_producer_bytes_inside_asl(branch_b):
    """THE OTHER REGRESSION GUARD. States.StringToJson raises States.Runtime on
    a malformed input, and States.Runtime is NOT CATCHABLE — measured against
    live Step Functions on 2026-09-19, including from inside a Parallel whose
    Catch names States.ALL. So a parse of producer bytes anywhere on this path
    is a way for one bad artifact to fail the entire weekly run, and no Catch
    can be added to make it safe. The parse belongs at the producer, where a
    malformed value is an ordinary Python exception; the SF gets a bare word."""
    state = branch_b["ReadModelZooArenaCycle"]
    assert state["Type"] == "Task"
    assert state["Resource"] == "arn:aws:states:::aws-sdk:s3:getObject"
    assert state["ResultPath"] == "$.model_zoo_arena"
    assert "ResultSelector" not in state, (
        "a ResultSelector here is where the uncatchable intrinsic lived"
    )
    assert state.get("Catch"), "a missing object must still be caught"

    import json as _json
    for name in ("ReadModelZooArenaCycle", "CheckModelZooVerdict",
                 "ExtractModelZooVerdictAbsent", "ExtractModelZooVerdictUnreadable"):
        assert "StringToJson" not in _json.dumps(branch_b[name].get("Parameters", {})), name
        assert "StringToJson" not in _json.dumps(branch_b[name].get("ResultSelector", {})), name


def test_the_verdict_is_compared_whole_never_as_a_substring(branch_b):
    """A substring match over a rendered document is the class this change
    removes — the cycle JSON carries decision.comparisons[].status as a second
    `status` field. The projection is one word and is compared as one word."""
    for rule in branch_b["CheckModelZooVerdict"]["Choices"]:
        conds = rule.get("And") or [rule]
        for cond in conds:
            assert "StringMatches" not in cond, cond
            assert cond["Variable"] == "$.model_zoo_arena.Body"


def test_every_status_the_contract_allows_has_an_arm(branch_b):
    """The producer refuses to write a status outside arena_cycle.schema.json's
    decision.status enum, so every word it CAN write must be routed explicitly.
    A word with no arm falls to Default, which is the UNKNOWN edge — correct,
    but it would degrade every run rather than name the drift."""
    # Read from the INSTALLED nousergon-lib, not a sibling checkout: a path
    # guess is wrong in a worktree and wrong in CI, and it would skip — which
    # for a drift guard is the same as not existing.
    from nousergon_lib.contracts import load_schema

    enum = set(
        load_schema("arena_cycle")["properties"]["decision"]
        ["properties"]["status"]["enum"]
    )
    routed = {
        cond["StringEquals"]
        for r in branch_b["CheckModelZooVerdict"]["Choices"]
        for cond in (r.get("And") or [r])
        if "StringEquals" in cond
    }
    assert routed == enum, (
        f"arms {sorted(routed)} vs contract enum {sorted(enum)} — the two "
        f"vocabularies must not drift (alpha-engine-config-I11101)"
    )


def test_an_unrecognised_word_is_unknown_not_a_clean_completion(branch_b):
    """Default is the UNKNOWN edge, not BranchBComplete. The producer already
    refuses an out-of-vocabulary status, so reaching Default means the two
    vocabularies have drifted — exactly when guessing is worst."""
    assert branch_b["CheckModelZooVerdict"]["Default"] == "ExtractModelZooVerdictAbsent"


def test_an_unreadable_verdict_degrades_honestly_and_never_reads_as_servable(branch_b):
    """sf-pipeline-policy.md §2.3a: absence is UNKNOWN, never a pass. Both ways
    the verdict can go missing — the object unreadable, or present but carrying
    no decision.status — converge on the model-zoo fail-open group."""
    (catch,) = branch_b["ReadModelZooArenaCycle"]["Catch"]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "ExtractModelZooVerdictUnreadable"
    assert _first_path_to(
        branch_b, "ExtractModelZooVerdictUnreadable", "MarkModelZooDegraded"
    )[-1] == "MarkModelZooDegraded"
    # The notifier formats $.model_zoo_error; jumping to it without producing
    # that field died with States.Runtime live on 2026-07-10 (config#2160), and
    # tests/test_sf_field_reachability.py is the standing guard.
    unreadable = branch_b["ExtractModelZooVerdictUnreadable"]
    assert unreadable["ResultPath"] == "$.model_zoo_error"
    assert unreadable["Next"] == "PublishModelZooFailureImmediate"

    rules = branch_b["CheckModelZooVerdict"]["Choices"]
    absent = [r for r in rules if r.get("IsPresent") is False]
    assert len(absent) == 1, "the absent-verdict case must be stated, not left to Default"
    assert absent[0]["Variable"] == "$.model_zoo_arena.Body"
    assert absent[0]["Next"] == "ExtractModelZooVerdictAbsent"
    # Two producers, not one: $.model_zoo_arena_error exists only on the Catch
    # path, and tests/test_sf_field_reachability.py holds a state to fields
    # EVERY reaching path produces.
    for name in ("ExtractModelZooVerdictAbsent", "ExtractModelZooVerdictUnreadable"):
        assert branch_b[name]["ResultPath"] == "$.model_zoo_error"
        assert branch_b[name]["Next"] == "PublishModelZooFailureImmediate"
    assert rules.index(absent[0]) == 0, (
        "Choice is first-match; the absence arm must precede any value test"
    )


def test_only_an_explicit_unservable_status_declares_the_slot_unservable(branch_b):
    """Every other DECIDED status — a real promotion, or a decided no-move — is
    the ordinary clean completion, exactly as the pre-I11106 Success edge was."""
    choice = branch_b["CheckModelZooVerdict"]
    to_declared = [r for r in choice["Choices"] if r.get("Next") == "ModelZooUnservableDeclared"]
    assert len(to_declared) == 1
    (guard, value) = to_declared[0]["And"]
    assert guard == {"Variable": "$.model_zoo_arena.Body", "IsPresent": True}
    assert value == {"Variable": "$.model_zoo_arena.Body", "StringEquals": "unservable"}
    clean = {
        r["And"][1]["StringEquals"]
        for r in choice["Choices"] if r.get("Next") == "BranchBComplete"
    }
    assert clean == {"decided", "held", "unmeasurable", "bootstrap"}


def test_wait_for_model_zoo_captures_standard_output_content(branch_b):
    """Still carried forward — the FAILURE paths (ExtractModelZooSelectError,
    the liveness gate) read it for diagnostics. It is no longer what decides
    the slot's verdict; see
    test_no_verdict_is_decided_by_matching_standard_output_content."""
    selector = branch_b["WaitForModelZoo"]["ResultSelector"]
    assert selector["StandardOutputContent.$"] == "$.StandardOutputContent"


def test_model_zoo_unservable_is_floored_and_threaded_to_completion_markers(doc):
    """model_zoo_unservable must be floored false in InitializeInput (both
    polarities, sf-pipeline-policy.md §2.3a rule 3) and embedded in BOTH
    completion markers so a reconciler can see it on every terminal, not only
    the declared-unservable one."""
    init_params = doc["States"]["InitializeInput"]["Parameters"]["merged.$"]
    assert '"model_zoo_unservable":false' in init_params

    for marker in ("WriteCompletionMarker", "WriteCompletionMarkerDegraded"):
        body = doc["States"][marker]["Parameters"]["Body.$"]
        assert '"model_zoo_unservable":{}' in body
        assert "States.JsonToString($.model_zoo_unservable)" in body


def test_set_model_zoo_unservable_is_the_sole_writer_of_the_top_level_flag(doc):
    """SetModelZooUnservable must be the only state writing $.model_zoo_unservable
    at top scope — every OTHER writer of a field with this name is branch-local
    (ResultPath == "$.model_zoo_unservable" inside ResearchPredictorParallel's
    branch B, which is a disjoint JSONPath scope from the top-level document)."""
    top_states = doc["States"]
    writers = [
        name
        for name, st in top_states.items()
        if isinstance(st, dict) and st.get("ResultPath") == "$.model_zoo_unservable"
    ]
    assert writers == ["SetModelZooUnservable"]
