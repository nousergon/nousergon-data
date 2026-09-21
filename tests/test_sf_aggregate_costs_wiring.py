"""Pins the AggregateCosts Lambda wiring in the Saturday Step Functions JSON.

ROADMAP L1146 — SF-wire ``scripts/aggregate_costs.py`` CLI. The
companion alpha-engine-research PR adds the
``alpha-engine-research-aggregate-costs:live`` Lambda; this test only
asserts the SF wiring.

Pin the tail of the weekly definition:

    ... DirectorComplete ─┐
        CheckSkipDirector ┤
        Publish/Complete  ├→ CheckSkipAggregateCosts → AggregateCosts
        of the leaf       ┘                          → CheckShellRunNotify

The aggregator must sit AFTER every LLM-emitting state so all of their
``_cost_raw/{run_date}/*.jsonl`` rows exist by the time it reads the
prefix. **alpha-engine-config-I7194 (2026-08-25)** moved it out of
``ResearchPredictorParallel`` branch 0, where it ran BEFORE ``Director``
— the pipeline's single most expensive call — so ``cost.parquet``
structurally could not contain the pipeline's largest cost and
``AlphaEngine/Cost`` under-reported by an unbounded margin in the
reassuring direction.

It is anchored on the SHARED edge into ``CheckShellRunNotify`` rather
than on ``DirectorComplete``: that witness is entered only via
``Director``'s success edge, so anchoring there would silently skip cost
aggregation under ``skip_director``, under ``skip_post_eval`` and after
a ``ReportCard`` fail-open — exactly the reruns — turning an incomplete
parquet into a missing one.

Catches regressions like: a future SF refactor that drops a state, that
reroutes the tail past the aggregator, or that puts it back upstream of
an LLM call.
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
    """Flattened state view: top-level states UNION every Parallel
    branch's states. Mirrors the helper in test_sf_eval_judge_wiring.py.
    """
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


# ── State presence ────────────────────────────────────────────────────────


class TestStatesPresent:
    def test_aggregate_costs_states_exist(self, states):
        assert "CheckSkipAggregateCosts" in states
        assert "AggregateCosts" in states

    def test_aggregate_costs_is_a_task(self, states):
        assert states["AggregateCosts"]["Type"] == "Task"

    def test_aggregate_costs_check_skip_is_a_choice(self, states):
        assert states["CheckSkipAggregateCosts"]["Type"] == "Choice"


# ── Lambda target + payload ───────────────────────────────────────────────


class TestLambdaTarget:
    def test_lambda_function_arn(self, states):
        params = states["AggregateCosts"]["Parameters"]
        assert (
            params["FunctionName"]
            == "alpha-engine-research-aggregate-costs:live"
        )
        assert states["AggregateCosts"]["Resource"] == (
            "arn:aws:states:::lambda:invoke"
        )

    def test_payload_threads_run_date(self, states):
        # The handler hard-requires event["date"] — must be threaded
        # from $.run_date (seeded by InitializeInput from
        # $$.Execution.StartTime).
        payload = states["AggregateCosts"]["Parameters"]["Payload"]
        assert payload["date.$"] == "$.run_date"

    def test_payload_threads_shell_run_dry_flag(self, states):
        # dry_run_llm threading mirrors the rationale_clustering /
        # eval-judge chain — Friday-Preflight shell runs short-circuit
        # the S3 read + parquet write.
        payload = states["AggregateCosts"]["Parameters"]["Payload"]
        assert payload["dry_run_llm.$"] == "$.research_dry"


# ── Failure isolation ─────────────────────────────────────────────────────


def _through_normalizers(states: dict, name: str) -> str:
    """Resolve a transition target past any pure-Pass normalizer in front of it.

    alpha-engine-config#5950 inserted floors that sit between an edge and its
    real destination, flooring the optional fields the destination dereferences.
    A Pass has no Choices, so it cannot change WHICH destination is reached —
    walking through it keeps these guards asserting the destination rather than
    widening them to accept any Pass, which is how a wrong destination would get
    in behind one.
    """
    seen = set()
    while (
        name in states
        and states[name].get("Type") == "Pass"
        and "Next" in states[name]
        and name not in seen
    ):
        seen.add(name)
        name = states[name]["Next"]
    return name


class TestFailureIsolation:
    def test_catch_routes_through_the_degraded_flag(self, states):
        # Cost telemetry is observability — aggregator failure must NOT
        # halt the pipeline. alpha-engine-config#6722: this Catch matches
        # sf-pipeline-policy.md §5's NAMED cost-aggregation carve-out,
        # which REQUIRES a degraded flag, so it routes through
        # MarkAggregateCostsDegraded rather than straight to the notify.
        catches = states["AggregateCosts"]["Catch"]
        assert len(catches) >= 1
        assert any(
            c["Next"] == "MarkAggregateCostsDegraded"
            and "States.ALL" in c["ErrorEquals"]
            for c in catches
        )

    def test_the_degraded_flag_is_its_own_top_level_family(self, states):
        """alpha-engine-config-I7194. Inside the Parallel this path wrote
        the branch-local ``$.research_degraded_local``, hoisted as
        ``branch_a_degraded`` and folded into
        ``$.research_predictor_degraded``. That fold does not exist at the
        top level, and reusing the name would attribute a cost-aggregation
        failure to a Parallel that no longer contains the aggregator — so
        the flag is its own family, in the ReportCard / ScannerLeaderboard
        shape: boolean first, then the summary the terminal reads."""
        flag = states["MarkAggregateCostsDegraded"]
        assert flag["Type"] == "Pass"
        assert flag["Result"] is True
        assert flag["ResultPath"] == "$.aggregate_costs_degraded"
        assert flag["Next"] == "SetAggregateCostsDegradedSummary"

        summary = states["SetAggregateCostsDegradedSummary"]
        assert summary["Type"] == "Pass"
        assert summary["ResultPath"] == "$.degraded_summary"
        assert summary["Parameters"]["degraded"] is True
        assert summary["Parameters"]["reason"]
        # A Choice path added later must not throw on an absent error key.
        assert "stage_error.$" not in summary["Parameters"]
        # The fail-open changes what the run SAYS, never where it goes —
        # one state longer since alpha-engine-config-I8336, which put the
        # named publish on the path. Both edges out of that publish (success
        # and its best-effort Catch) land on CheckShellRunNotify, so the
        # continuation is unchanged in substance.
        assert summary["Next"] == "PublishAggregateCostsDegraded"

    def test_the_fail_open_pages_at_the_moment_it_fires(self, states):
        """alpha-engine-config-I8336.

        Before this, the path set ``$.aggregate_costs_degraded`` and walked
        straight to ``CheckShellRunNotify``: the only signal was the terminal
        email, and ``$.aggregate_costs_degraded`` folds into
        ``NotifyCompleteMultipleDegraded``, which deliberately names no
        specific family. So a cycle with no cost record reached the operator
        as "two or more families degraded". Mirrors
        ``PublishScannerLeaderboardDegraded`` exactly.
        """
        publish = states["PublishAggregateCostsDegraded"]
        assert publish["Type"] == "Task"
        assert publish["Resource"] == "arn:aws:states:::sns:publish"
        assert publish["ResultPath"] == "$.aggregate_costs_degraded_notify"
        assert publish["Next"] == "CheckShellRunNotify"

        # config#1819: Subject and Message are CONSTANTS. A States.Format
        # against an unbounded error string is how an SNS publish starts
        # failing on the run it is meant to report.
        params = publish["Parameters"]
        assert set(params) == {
            "TopicArn.$",
            "Subject",
            "Message",
            "MessageAttributes",
        }
        assert "States.Format" not in params["Subject"]
        assert "States.Format" not in params["Message"]

        # The Catch is what keeps this best-effort: an SNS outage must not
        # convert a deliberately-continued run into a failed one.
        (catch,) = publish["Catch"]
        assert "States.ALL" in catch["ErrorEquals"]
        assert catch["Next"] == "CheckShellRunNotify"
        assert catch["ResultPath"] == "$.aggregate_costs_degraded_notify_error"

    def test_the_alert_does_not_name_a_cause_it_cannot_distinguish(self, states):
        """This Catch is reached by a fan-in COVERAGE BREACH, by the coverage
        check being unable to measure, by a capture-stream staleness raise and
        by an ordinary Lambda failure. The state cannot tell them apart, so the
        message must not claim to — an alert naming the wrong one of four
        causes sends the operator to the wrong place three times in four."""
        message = states["PublishAggregateCostsDegraded"]["Parameters"]["Message"]
        assert "cost" in message.lower()
        assert "$.aggregate_costs_error" in message, (
            "the message must point at where the actual error lives, since it "
            "deliberately does not name one"
        )

    def test_a_degraded_aggregation_can_never_report_a_clean_success(self, states):
        """Without a rule of its own in CheckGateDegradedNotify a run whose
        only degradation is the cost aggregation falls through to
        NotifyComplete — "All steps completed successfully" — while
        terminating in DegradedRun. Folded into the generic combined
        notifier rather than given a per-combination Task, the
        disposition config#6722 already applied to
        ``$.research_predictor_degraded``; registered LAST, so anything
        more consequential reports itself instead."""
        gate = states["CheckGateDegradedNotify"]
        matching = [
            c
            for c in gate["Choices"]
            if any(
                cond.get("Variable") == "$.aggregate_costs_degraded"
                for cond in (c.get("And") or [c])
            )
        ]
        assert len(matching) == 1
        # config#5950: NormalizeMultipleDegradedContext floors the three
        # presence-tested degradation fields the notifier dereferences — they
        # could not go in InitializeInput without moving this very gate. Resolve
        # through it so this still asserts the notifier, not merely a Pass.
        assert _through_normalizers(
            states, matching[0]["Next"]
        ) == "NotifyCompleteMultipleDegraded"
        # alpha-engine-config-I11299 registered a SEVENTH family
        # ($.director_degraded) after this one, also LAST-on-purpose. What
        # this test pins is that nothing MORE consequential sits below the
        # cost-aggregation rule, not the literal final index.
        assert gate["Choices"].index(matching[0]) >= len(gate["Choices"]) - 2
        assert gate["Default"] == "NotifyComplete"
        # The notifier's constant Message must name the flag it now covers —
        # config#1819 forbids formatting the live set into the text, so the
        # enumeration IS the contract with the operator reading it.
        message = states["NotifyCompleteMultipleDegraded"]["Parameters"]["Message"]
        assert "$.aggregate_costs_degraded" in message

    def test_retry_only_on_lambda_service_errors(self, states):
        # Same shape as rationale-clustering — service-level retries
        # (Lambda.ServiceException, TooManyRequestsException) but NO
        # retry on application-level errors (which would mask
        # aggregator bugs).
        retries = states["AggregateCosts"]["Retry"]
        assert any(
            "Lambda.ServiceException" in r["ErrorEquals"]
            and "Lambda.TooManyRequestsException" in r["ErrorEquals"]
            for r in retries
        )


# ── Wiring: edges into and out of AggregateCosts ──────────────────────────


class TestEdges:
    def test_the_aggregator_is_a_top_level_state(self, sf):
        """alpha-engine-config-I7194. Not a stylistic point: ASL gives a
        state in a Parallel branch no ordering relationship with anything
        outside that Parallel, so an aggregator inside one can never be
        ordered after Director."""
        for name in (
            "CheckSkipAggregateCosts",
            "AggregateCosts",
            "MarkAggregateCostsDegraded",
            "SetAggregateCostsDegradedSummary",
        ):
            assert name in sf["States"], f"{name} is not a top-level state"
        for st in sf["States"].values():
            if st.get("Type") != "Parallel":
                continue
            for branch in st["Branches"]:
                assert "AggregateCosts" not in branch["States"]

    def test_every_edge_into_the_notify_gate_passes_the_aggregator_first(self, sf):
        """The load-bearing property. The aggregator is anchored on the
        tail's single convergence point, so no completion path can reach
        the terminal notify without it — which is what an anchor on
        DirectorComplete (a success-ONLY witness) would have allowed
        under skip_director, under skip_post_eval and after a ReportCard
        fail-open."""
        allowed = {
            "CheckSkipAggregateCosts",  # the skip route
            "AggregateCosts",           # the success route
            "SetAggregateCostsDegradedSummary",  # the fail-open route
            # alpha-engine-config-I8336: the named alert the fail-open route
            # now walks through. Both of its edges (success and its
            # best-effort Catch) land here, and it is reachable ONLY from
            # SetAggregateCostsDegradedSummary — which is already downstream
            # of the aggregator, so it adds no path that bypasses it.
            "PublishAggregateCostsDegraded",
        }
        offenders = []
        for name, st in sf["States"].items():
            if name in allowed:
                continue
            targets = [st.get("Next"), st.get("Default")]
            targets += [c.get("Next") for c in st.get("Choices", []) or []]
            targets += [c.get("Next") for c in st.get("Catch", []) or []]
            if "CheckShellRunNotify" in targets:
                offenders.append(name)
        assert not offenders, (
            f"{offenders} reach CheckShellRunNotify without passing the "
            f"cost-aggregation gate — their cost rows would never be "
            f"aggregated (alpha-engine-config-I7194)"
        )

    def test_the_degraded_publish_is_reachable_only_from_the_fail_open(self, states):
        """It is on the allowlist above, so it must not become a second way
        into the notify gate. Its only predecessor is the fail-open summary,
        which is itself downstream of the aggregator."""
        predecessors = []
        for name, st in states.items():
            targets = [st.get("Next"), st.get("Default")]
            targets += [c.get("Next") for c in st.get("Choices", []) or []]
            targets += [c.get("Next") for c in st.get("Catch", []) or []]
            if "PublishAggregateCostsDegraded" in targets:
                predecessors.append(name)
        assert predecessors == ["SetAggregateCostsDegradedSummary"]

    def test_the_leaf_and_the_director_hand_off_to_the_gate(self, states):
        assert states["ScannerLeaderboardComplete"]["Next"] == "CheckSkipAggregateCosts"
        assert states["CheckSkipScannerLeaderboard"]["Choices"][0]["Next"] == (
            "CheckSkipAggregateCosts"
        )
        assert states["PublishScannerLeaderboardDegraded"]["Next"] == (
            "CheckSkipAggregateCosts"
        )
        # The coarse deprecated whole-tail alias too: skip_post_eval bypasses
        # the advisories, and never bypassed cost telemetry.
        assert states["CheckSkipPostEval"]["Choices"][0]["Next"] == (
            "CheckSkipAggregateCosts"
        )

    def test_branch_a_no_longer_routes_through_the_aggregator(self, states):
        """Counterfactual is Branch A's last work state again. All three
        of its exits land on the branch terminal."""
        assert states["Counterfactual"]["Next"] == "BranchAComplete"
        assert states["MarkCounterfactualDegraded"]["Next"] == "BranchAComplete"
        assert states["CheckSkipCounterfactual"]["Default"] == "Counterfactual"
        assert states["CheckSkipCounterfactual"]["Choices"][0]["Next"] == (
            "BranchAComplete"
        )

    def test_aggregate_costs_success_routes_to_the_notify_gate(self, states):
        assert states["AggregateCosts"]["Next"] == "CheckShellRunNotify"

    def test_check_skip_aggregate_costs_skip_routes_to_the_notify_gate(
        self, states,
    ):
        skip = states["CheckSkipAggregateCosts"]
        skip_choices = skip["Choices"]
        assert len(skip_choices) == 1
        assert skip_choices[0]["Next"] == "CheckShellRunNotify"

    def test_check_skip_aggregate_costs_default_is_aggregate_costs(self, states):
        assert states["CheckSkipAggregateCosts"]["Default"] == "AggregateCosts"


# ── Skip-flag semantics ───────────────────────────────────────────────────


class TestSkipFlagSemantics:
    def test_skip_flag_named_skip_aggregate_costs(self, states):
        skip = states["CheckSkipAggregateCosts"]
        # Each choice's conjunction names the variable being inspected.
        choice = skip["Choices"][0]
        # Mirror the pattern used by the other observability skip
        # gates: IsPresent + BooleanEquals true.
        variables = [c["Variable"] for c in choice["And"]]
        assert all(v == "$.skip_aggregate_costs" for v in variables)
        assert any(c.get("BooleanEquals") is True for c in choice["And"])


# ── Result paths (state-merge contract) ───────────────────────────────────


class TestResultPaths:
    def test_success_result_lands_under_aggregate_costs_result(self, states):
        # Mirrors rationale_clustering_result / counterfactual_result —
        # each observability Lambda result is namespaced under its own
        # ResultPath so the parent state doesn't get clobbered.
        assert (
            states["AggregateCosts"]["ResultPath"]
            == "$.aggregate_costs_result"
        )

    def test_failure_result_lands_under_aggregate_costs_error(self, states):
        catches = states["AggregateCosts"]["Catch"]
        catch_all = next(
            c for c in catches if "States.ALL" in c["ErrorEquals"]
        )
        assert catch_all["ResultPath"] == "$.aggregate_costs_error"


# ── Timeout ───────────────────────────────────────────────────────────────


class TestTimeout:
    def test_timeout_is_bounded(self, states):
        # The handler's expected wallclock is ~minutes for thousands of small
        # S3 reads + one parquet write; observed p95 is 6.2s over n=35.
        #
        # Was pinned to exactly 600 until 2026-08-13. That value could never
        # fire: alpha-engine-research-aggregate-costs carries its OWN 300s
        # function timeout, and an SF lambda:invoke has two ceilings of which
        # only the SMALLER ever fires (alpha-engine-config-I6897). So the 600
        # described nothing and the real ceiling was 300, declared in another
        # repo's deploy script.
        #
        # Asserted as a RANGE rather than an equality: an exact pin makes any
        # future re-baselining a test edit, which is how the stale 600 survived
        # in the first place. The binding property is what matters.
        sf_timeout = states["AggregateCosts"]["TimeoutSeconds"]
        assert sf_timeout < 300, (
            f"AggregateCosts TimeoutSeconds must be STRICTLY BELOW the "
            f"Lambda's own 300s ceiling so the declared budget is the one "
            f"that fires; got {sf_timeout}"
        )
        assert sf_timeout >= 60, (
            f"...and comfortably above the observed p95 of ~6s; got {sf_timeout}"
        )
