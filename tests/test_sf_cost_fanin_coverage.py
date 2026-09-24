"""The weekly SF's declaration of which stages produce cost records.

**What this guards, and why a count could not.** ``AggregateCosts`` rolls
``decision_artifacts/_cost_raw/{date}/`` into a daily parquet. On
2026-08-13 that prefix was measured on five separate dates and every
record on every one of them came from a single producer — the Think Tank,
which was removed from this pipeline on 2026-08-10 — while
``ReplayConcordance`` (a per-artifact call, up to 150 artifacts) and the
rest of the weekly chain were attributed to nothing at all
(``alpha-engine-config-I7179``). Five files under the prefix is a healthy
count. The defect is only visible as a **set difference**, so the
declaration this module guards is a set, and the aggregator's check is a
set comparison.

**Why the declaration lives in the Step Function.** The set of stages that
can produce a cost record is a property of the pipeline, and the pipeline
is this file. Putting it in the aggregator's own code would put the
denominator inside the thing being measured, one repo away from every
change that alters it.

**Two directions, both enforced — here structurally, and at runtime by the
aggregator.**

- *Declared but absent* — a stage that ran and emitted nothing. The
  I7179 defect.
- *Present but undeclared* — a producer nobody registered. Since
  ``krepis>=0.57`` emits from the environment rather than from a
  per-call-site argument, a newly added LLM stage emits **by
  construction**, so it arrives in the prefix as an undeclared producer
  and the aggregator refuses it until this map is updated. That is what
  keeps the next stage added from silently reproducing the gap, without
  this test having to guess from the ASL which stages call an LLM — a
  guess that would be wrong today: ``dry_run_llm`` is threaded into
  ``Scanner``, ``RationaleClustering``, ``Counterfactual`` and
  ``AggregateCosts``, none of which make an LLM call, and is absent from
  ``Director``, which does (and from ``ChallengerShadow``, which did until
  its single-agent arm was retired on 2026-09-22).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function.json"


@pytest.fixture(scope="module")
def sf() -> dict:
    return json.loads(_SF_PATH.read_text())


@pytest.fixture(scope="module")
def states(sf) -> dict:
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


@pytest.fixture(scope="module")
def scopes(sf) -> dict:
    """Every scope in the definition, keyed by the top-level state that
    contains it: the top level itself under ``None``, and each Parallel
    branch under the Parallel's own state name.

    Reachability is only meaningful WITHIN one scope — ASL gives a state
    in one Parallel branch no ordering relationship with a state in a
    sibling branch. Between scopes the ordering that IS guaranteed is the
    Parallel's own: a Parallel completes before its ``Next``, so every
    state inside it precedes every state reachable after it.
    """
    out: dict = {None: sf["States"]}
    for name, st in sf["States"].items():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                out.setdefault(name, {}).update(branch["States"])
    return out


def _enclosing(stage: str, scopes: dict) -> str | None:
    """The top-level state that contains ``stage`` — ``None`` when the
    stage IS a top-level state."""
    if stage in scopes[None]:
        return None
    for parallel, states in scopes.items():
        if parallel is not None and stage in states:
            return parallel
    raise AssertionError(f"{stage} is in no scope of the definition")


@pytest.fixture(scope="module")
def coverage(states) -> dict:
    return states["AggregateCosts"]["Parameters"]["Payload"]["coverage"]


def _successors(state: dict) -> set:
    """Every state name this state can hand control to."""
    out = set()
    if state.get("Next"):
        out.add(state["Next"])
    if state.get("Default"):
        out.add(state["Default"])
    for choice in state.get("Choices", []) or []:
        if choice.get("Next"):
            out.add(choice["Next"])
    for catch in state.get("Catch", []) or []:
        if catch.get("Next"):
            out.add(catch["Next"])
    return out


def _reaches(start: str, target: str, branch: dict) -> bool:
    seen, frontier = set(), {start}
    while frontier:
        name = frontier.pop()
        if name == target:
            return True
        if name in seen or name not in branch:
            continue
        seen.add(name)
        frontier |= _successors(branch[name])
    return False


class TestDeclarationShape:
    def test_coverage_block_exists(self, coverage):
        assert set(coverage) >= {
            "execution_arn.$",
            "required_producers",
            "conditional_producers",
            "allowed_producers",
        }

    def test_execution_arn_is_threaded(self, coverage):
        """Without the execution ARN the aggregator cannot tell a stage
        that ran and went silent from one that never ran — and a check
        that cannot tell those apart fires on every degraded run and is
        then turned off."""
        assert coverage["execution_arn.$"] == "$$.Execution.Id"

    def test_required_producers_is_not_empty(self, coverage):
        """An empty required set is a check that can never fail, which is
        indistinguishable on every surface from a check that passes."""
        assert coverage["required_producers"]

    @pytest.mark.parametrize("key", ["required_producers", "conditional_producers"])
    def test_producer_ids_are_non_empty_strings(self, coverage, key):
        for stage, ids in coverage[key].items():
            assert isinstance(ids, list) and ids, stage
            for callsite_id in ids:
                assert isinstance(callsite_id, str) and callsite_id.strip(), stage

    def test_allowed_producers_is_a_list_of_patterns(self, coverage):
        assert isinstance(coverage["allowed_producers"], list)
        for pattern in coverage["allowed_producers"]:
            assert isinstance(pattern, str) and pattern.strip()


class TestDeclaredStagesAreReal:
    """A renamed state silently empties the required set, and the check
    then passes for the same reason it was written to fail."""

    @pytest.mark.parametrize("key", ["required_producers", "conditional_producers"])
    def test_every_declared_stage_exists(self, coverage, states, key):
        for stage in coverage[key]:
            assert stage in states, f"{key} names a state that does not exist: {stage}"

    def test_every_declared_stage_is_a_task(self, coverage, states):
        for stage in coverage["required_producers"]:
            assert states[stage]["Type"] == "Task", stage


class TestDeclaredStagesCanActuallyBeSeen:
    """The ordering half — the failure that was live when this was written.

    ``Director`` makes the pipeline's single most expensive call
    (``ultra`` group, callsite ``director-plan``) and runs at the TOP
    LEVEL. ``AggregateCosts`` ran as the last state of
    ``ResearchPredictorParallel`` branch 0, so the Director's rows landed
    in ``_cost_raw`` AFTER the parquet for that date was written and
    could never be in it — ``cost.parquet`` structurally excluded the
    pipeline's largest cost, and ``AlphaEngine/Cost`` under-reported by
    an unbounded margin in the reassuring direction. Fixed by
    ``alpha-engine-config-I7194``: the aggregator moved to the top-level
    tail, on the single edge every real-completion path takes into
    ``CheckShellRunNotify``, and ``Director`` is required rather than
    merely allowed.

    The reachability rule below is stated over SCOPES rather than over
    one hard-coded branch, so it keeps holding wherever the aggregator
    sits — that generality is the whole point: the branch-local version
    of this test could only ever have said "the aggregator cannot see
    Director", never "it must".
    """

    def test_every_required_stage_precedes_the_aggregator(self, coverage, scopes):
        agg_scope = _enclosing("AggregateCosts", scopes)
        for stage in coverage["required_producers"]:
            stage_scope = _enclosing(stage, scopes)
            if stage_scope == agg_scope:
                # Same scope — the ordinary intra-scope walk.
                assert _reaches(stage, "AggregateCosts", scopes[agg_scope]), (
                    f"{stage} cannot reach AggregateCosts, so its cost rows "
                    f"are not guaranteed to exist when the aggregator reads "
                    f"the prefix"
                )
                continue
            # Different scopes. The only ordering ASL guarantees across a
            # Parallel boundary is the Parallel's own: everything inside it
            # finishes before its Next. So the stage's enclosing Parallel
            # must reach the aggregator's scope at the top level, and the
            # aggregator must not itself be inside a Parallel the stage is
            # not in (a sibling branch has no ordering at all).
            assert agg_scope is None, (
                f"{stage} is in {stage_scope or 'the top level'} and "
                f"AggregateCosts is inside {agg_scope} — a Parallel branch "
                f"has no ordering relationship with anything outside it, so "
                f"the aggregator cannot rely on seeing {stage}'s rows"
            )
            assert stage_scope is not None
            assert _reaches(stage_scope, "AggregateCosts", scopes[None]), (
                f"{stage} runs inside {stage_scope}, which does not reach "
                f"AggregateCosts at the top level — its cost rows are not "
                f"guaranteed to exist when the aggregator reads the prefix"
            )

    def test_the_aggregator_does_not_require_itself(self, coverage):
        assert "AggregateCosts" not in coverage["required_producers"]

    def test_the_aggregator_runs_after_the_director(self, sf, scopes):
        """The I7194 defect, stated as the property rather than as a
        position. Anchored on Director's SKIP gate, not on Director or
        DirectorComplete: DirectorComplete is entered only on Director's
        success edge, so an aggregator anchored there would silently not
        run under ``skip_director`` or after a ReportCard fail-open —
        exactly the reruns — and the parquet would be missing entirely
        rather than merely incomplete."""
        assert _enclosing("AggregateCosts", scopes) is None, (
            "AggregateCosts must be a top-level state: Director is one, and "
            "a Parallel branch cannot be ordered after it"
        )
        top = scopes[None]
        assert _reaches("Director", "AggregateCosts", top)
        assert _reaches("CheckSkipDirector", "AggregateCosts", top)
        assert not _reaches("AggregateCosts", "Director", top), (
            "the aggregator must not be able to precede the Director on any "
            "path — that is the whole defect"
        )


class TestKnownProducersStayDeclared:
    """Pins the producers measured on 2026-08-13, so a future edit
    that drops one has to say so in a diff rather than in a silence."""

    def test_replay_concordance_is_not_declared(self, coverage):
        """alpha-engine-config-I10539 (Brian ruling 2026-09-16, option b):
        the ReplayConcordance STAGE is retired, so requiring its cost record
        is a demand for spend that can no longer happen — that is exactly the
        ``CostCoverageError`` that ended the 2026-09-12 canonical run. It must
        also not reappear as conditional or allowed: nothing in the definition
        invokes the concordance Lambda, so an out-of-band standalone
        invocation landing in this run_date's prefix SHOULD read as
        present-but-undeclared and refuse loudly."""
        assert "ReplayConcordance" not in coverage["required_producers"]
        assert "ReplayConcordance" not in coverage["conditional_producers"]
        assert "replay-concordance" not in coverage["allowed_producers"]

    def test_retired_single_agent_quant_is_not_declared(self, coverage):
        """alpha-engine-config-I11501: the single_agent_quant arm was retired
        on 2026-09-22 (alpha-engine-config-I11393) and producers/single_agent.py,
        ChallengerShadow's only paid-model call site, was deleted with it. The
        stage still runs, so requiring its record failed the next rehearsal
        (rehearsal-2026-09-23-2) with a CostCoverageError naming spend that can
        no longer happen. Same shape as the ReplayConcordance retirement above,
        and for the same reason it is not moved to allowed: a single-agent-quant
        record in this prefix would mean something revived the call site."""
        assert "ChallengerShadow" not in coverage["required_producers"]
        assert "ChallengerShadow" not in coverage["conditional_producers"]
        assert "single-agent-quant" not in coverage["allowed_producers"]

    def test_eval_judge_sync_is_required(self, coverage):
        """alpha-engine-config-I9329 — a LIVE defect this cutover had to fix,
        independently true of the substrate swap.

        `evaljudge-batch` was declared REQUIRED, and the batch rung it names
        was retired by alpha-engine-config-I9263 (Brian, 2026-08-29: "at this
        point we shouldn't be using the anthropic api at all"). A producer
        that can never be emitted, demanded on every run, fails the weekly
        fan-in coverage check EVERY WEEK — turning the judge's success into a
        pipeline degradation, which is the exact cry-wolf shape the
        conditional/required split exists to prevent.

        The two swap. Every judged artifact now goes through the router-
        addressed synchronous rung, so `evaljudge-sync` is the producer whose
        ABSENCE means the stage did not run.
        """
        assert coverage["required_producers"]["EvalJudgeProcess"] == [
            "evaljudge-sync"
        ]

    def test_eval_judge_batch_is_allowed_but_never_required(self, coverage):
        """Kept as CONDITIONAL rather than deleted, deliberately. Deleting it
        would make a `evaljudge-batch` row arriving from a replay of an
        archived plan read as a present-but-undeclared producer and fail the
        check; conditional says "this may appear and that is fine", which is
        the true statement about a retired rung whose historical artifacts
        still exist."""
        assert coverage["conditional_producers"]["EvalJudgeProcess"] == [
            "evaljudge-batch"
        ]

    def test_director_plan_is_required(self, coverage):
        """alpha-engine-config-I7194: the pipeline's most expensive call.
        It was ``allowed`` — never required — only because the aggregator
        ran before it, which is exactly the defect. Demoting it back to
        ``allowed`` would restore a coverage check that passes when the
        largest cost in the pipeline is missing."""
        assert coverage["required_producers"]["Director"] == ["director-plan"]
        assert "director-plan" not in coverage["allowed_producers"]

    def test_director_retro_judge_is_conditional_not_required(self, coverage):
        """The SAME Lambda invocation makes a second call —
        ``crucible-evaluator`` ``director/retro.py::grade_prior_plan``,
        callsite ``director-retro-judge`` — which genuinely does not fire
        on every run: ``_run_retro_best_effort`` returns ``skipped`` on
        the first cycle (no prior plan) and on an exhausted invocation
        budget, and swallows its own failure because the plan is already
        persisted. Requiring it would make the detector cry wolf; leaving
        it undeclared would make the aggregator refuse it as a
        present-but-undeclared producer the first time it does fire."""
        assert coverage["conditional_producers"]["Director"] == [
            "director-retro-judge"
        ]

    def test_eval_judge_shadow_is_allowed_but_never_required(self, coverage):
        """alpha-engine-config-I8335.

        ``evaljudge-shadow`` is the OpenRouter shadow judge
        (``crucible-research`` ``evals/judge.py::evaluate_artifact_openrouter``).
        It reads as an ``EvalJudgeProcess`` sibling and is not one: no state
        of this definition reaches it. Its only live invoker is
        ``lambda/openrouter_shadow_handler.py``, fired by the standalone
        EventBridge rule ``alpha-research-openrouter-shadow-weekly`` (Sunday
        10:00 UTC) whose own setup script says verbatim that it is
        "explicitly NOT a new state on the production Saturday ... chain".

        So it can be neither required nor conditional. ``required`` would
        demand a record from a producer nothing in the execution invokes;
        ``conditional_producers["EvalJudgeProcess"]`` would key its admission
        on a stage that does not produce it — admitting it only when eval-judge
        happens to run, and refusing it on a run where eval-judge was skipped
        and a manual shadow re-run landed in the same partition.

        ``allowed`` is the same treatment ``thinktank-*`` gets for the same
        reason: an out-of-band producer sharing the prefix.
        """
        assert "evaljudge-shadow" in coverage["allowed_producers"]
        flat_required = {
            cid for ids in coverage["required_producers"].values() for cid in ids
        }
        flat_conditional = {
            cid for ids in coverage["conditional_producers"].values() for cid in ids
        }
        assert "evaljudge-shadow" not in flat_required
        assert "evaljudge-shadow" not in flat_conditional

    def test_thinktank_is_allowed_but_never_required(self, coverage):
        """The Think Tank left this pipeline on 2026-08-10 and now runs on
        its own daily cadence, but still writes to the same prefix. It is
        allowed so it does not read as an undeclared producer, and never
        required, because its absence says nothing about this pipeline."""
        assert "thinktank-*" in coverage["allowed_producers"]
        flat_required = {
            cid for ids in coverage["required_producers"].values() for cid in ids
        }
        assert not any(cid.startswith("thinktank-") for cid in flat_required)
