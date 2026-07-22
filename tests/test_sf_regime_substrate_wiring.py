"""tests/test_sf_regime_substrate_wiring.py — pin the Saturday SF wiring
for the RegimeSubstrate state.

The regime substrate Lambda lives in alpha-engine-predictor; the SF
state that invokes it lives here. This test verifies the wiring
contract between the two so silent drift (e.g. a refactor renames the
Lambda or removes the state) breaks CI rather than the next Saturday SF.

Five invariants (alpha-engine-config-I2515 Phase B reorder: RegimeSubstrate
now runs BEFORE the RAG chain, moved ahead of RAG/ThinkTank, so
SignalsEnvelope reads a same-day regime label; the multi-agent Research
state + CheckSkipResearch were removed):

1. ``RegimeSubstrate`` state exists and is a Task that invokes
   ``alpha-engine-predictor-regime-substrate:live``.
2. ``CheckSkipRegimeSubstrate`` state exists and routes to either
   ``RegimeSubstrate`` (default) or ``SignalsEnvelope`` (when
   ``skip_regime_substrate: true``).
3. The post-RAG control flow lands on ``ThinkTankCoverage`` (moved to run
   after RAG so its theses read the fresh corpus), not on
   ``CheckSkipRegimeSubstrate`` (which now runs BEFORE RAG). Two routes:
   the ``CheckSkipRAGIngestion`` skip path AND the
   ``CheckRAGIngestionStatus`` success path.
4. ``RegimeSubstrate``'s Catch routes to ``SignalsEnvelope``, not
   ``HandleFailure`` — pins the Stage A observe-only contract that a
   regime failure must not halt the load-bearing envelope producer.
5. ``RegimeSubstrate`` payload is ``{"action": "produce"}`` —
   handler-side ``dry_run`` mode would not write the substrate
   artifact, so the SF must always invoke ``produce``.

The general SF IAM grants test (``test_sf_iam_lambda_grants.py``)
covers that the SF role can invoke the regime Lambda; not duplicated
here.
"""
from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SF_PATH = REPO_ROOT / "infrastructure" / "step_function.json"


def _sf() -> dict:
    return json.loads(SF_PATH.read_text())


def _flat_states(sf: dict) -> dict:
    """Flattened state view: top-level states UNION every Parallel
    branch's states. config#885 relocated the Scanner→RAGIngestion→
    RegimeSubstrate→RegimeRetrospectiveEval chain FROM top level INTO
    ResearchPredictorParallel's Branch A head (so PredictorTraining forks
    parallel to it after DataPhase1), so these states now live inside a
    Parallel branch. Mirrors the helper in test_sf_scanner_wiring.py /
    test_sf_eval_judge_wiring.py."""
    flat: dict = dict(sf["States"])
    for st in sf["States"].values():
        if st.get("Type") == "Parallel":
            for branch in st["Branches"]:
                flat.update(branch["States"])
    return flat


def test_regime_substrate_state_exists() -> None:
    sf = _sf()
    states = _flat_states(sf)
    assert "RegimeSubstrate" in states, (
        "Saturday SF must contain a RegimeSubstrate state — Stage A "
        "ships the substrate Lambda invocation between RAG and Research "
        "(config#885: now inside ResearchPredictorParallel Branch A)."
    )
    state = states["RegimeSubstrate"]
    assert state["Type"] == "Task"
    assert state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert state["Parameters"]["FunctionName"] == "alpha-engine-predictor-regime-substrate:live"


def test_regime_substrate_payload_is_produce_action() -> None:
    """Handler-side ``dry_run`` mode returns the payload without writing
    to S3. Post the Friday shell-run KEYSTONE the action is routed via the
    ``$.regime_action`` control var: InitializeInput seeds it ``"produce"``
    (the pre-keystone hardcoded value — the real Saturday run is
    behaviourally identical, the artifact still lands), and
    ApplyShellRunDefaults flips it to ``"dry_run"`` ONLY under shell_run
    (verified clean no-write dry path). Pin the routing so a misguided
    debugging change can't hardcode dry on the production run."""
    sf = _sf()
    states = _flat_states(sf)
    payload = states["RegimeSubstrate"]["Parameters"]["Payload"]
    assert payload.get("action.$") == "$.regime_action", (
        "RegimeSubstrate payload must route action via $.regime_action "
        f"(keystone dry-routing); got {payload!r}"
    )
    init_expr = sf["States"]["InitializeInput"]["Parameters"]["merged.$"]
    assert '"regime_action":"produce"' in init_expr, (
        "InitializeInput must seed regime_action='produce' so the real "
        "Saturday run (no shell_run) still calls produce"
    )


def test_check_skip_regime_substrate_routes_correctly() -> None:
    """alpha-engine-config-I2515 Phase B: RegimeSubstrate now runs BEFORE
    the RAG chain (moved ahead of RAG/ThinkTank) so SignalsEnvelope reads a
    same-day regime label. config#3134: the skip path now lands on
    CheckSkipSignalsEnvelope (the new gate in front of SignalsEnvelope)
    rather than SignalsEnvelope directly, so the envelope's own skip flag
    is honored on every path into it, not just the RegimeSubstrate-ran path."""
    sf = _sf()
    states = _flat_states(sf)
    assert "CheckSkipRegimeSubstrate" in states
    state = states["CheckSkipRegimeSubstrate"]
    assert state["Type"] == "Choice"
    assert state["Default"] == "RegimeSubstrate"
    skip_choice = state["Choices"][0]
    skip_vars = skip_choice["And"]
    assert any(c.get("Variable") == "$.skip_regime_substrate" for c in skip_vars)
    assert skip_choice["Next"] == "CheckSkipSignalsEnvelope"


def test_post_rag_control_flow_lands_on_thinktank_coverage() -> None:
    """alpha-engine-config-I2515 Phase B: ThinkTankCoverage moved to run
    AFTER the RAG chain (was between Scanner and RAG) so its theses read
    the fresh corpus. config#3134: both post-RAG routes now point at
    CheckSkipThinkTankCoverage (the new gate in front of ThinkTankCoverage)
    rather than CheckSkipRegimeSubstrate (which runs BEFORE RAG in the new
    order). Catches a refactor that re-introduces the stale-corpus bug."""
    sf = _sf()
    states = _flat_states(sf)
    # CheckSkipRAGIngestion's skip-branch
    skip_choice = states["CheckSkipRAGIngestion"]["Choices"][0]
    assert skip_choice["Next"] == "CheckSkipThinkTankCoverage", (
        "CheckSkipRAGIngestion skip path must land on CheckSkipThinkTankCoverage"
    )
    # CheckRAGIngestionStatus's success branch
    success_choice = next(
        c for c in states["CheckRAGIngestionStatus"]["Choices"]
        if c.get("StringEquals") == "Success"
    )
    assert success_choice["Next"] == "CheckSkipThinkTankCoverage", (
        "CheckRAGIngestionStatus success path must land on CheckSkipThinkTankCoverage"
    )


def test_regime_substrate_failure_is_non_blocking() -> None:
    """Stage A observe-only contract: RegimeSubstrate failure must NOT
    halt the pipeline. alpha-engine-config-I2515 Phase B: the
    Catch[States.ALL] routes toward SignalsEnvelope (RegimeSubstrate's new
    direct successor, moved ahead of RAG/ThinkTank) so the load-bearing
    envelope producer still gets a chance to run. config#3134: retargeted
    to CheckSkipSignalsEnvelope, the new gate immediately in front of it."""
    sf = _sf()
    states = _flat_states(sf)
    catches = states["RegimeSubstrate"]["Catch"]
    states_all = next(c for c in catches if c["ErrorEquals"] == ["States.ALL"])
    assert states_all["Next"] == "CheckSkipSignalsEnvelope", (
        "RegimeSubstrate Catch[States.ALL] must route to "
        "CheckSkipSignalsEnvelope (non-blocking), not HandleFailure "
        "(blocking). Stage A is observe-only; a regime substrate failure "
        "must not halt downstream."
    )


def test_regime_substrate_next_is_signals_envelope() -> None:
    """alpha-engine-config-I2515 Phase B: Success path lands toward
    SignalsEnvelope (moved ahead of RAG/ThinkTank so the envelope's
    market_regime field reads a same-day regime label). config#3134:
    retargeted to CheckSkipSignalsEnvelope, the new gate immediately in
    front of it."""
    sf = _sf()
    states = _flat_states(sf)
    assert states["RegimeSubstrate"]["Next"] == "CheckSkipSignalsEnvelope"


def test_regime_substrate_timeout_is_reasonable() -> None:
    """Lambda timeout is 300s (per setup-regime-lambda.sh); SF
    TimeoutSeconds must be slightly higher to avoid premature
    timeout-due-to-SF when the Lambda is still working."""
    sf = _sf()
    states = _flat_states(sf)
    sf_timeout = states["RegimeSubstrate"]["TimeoutSeconds"]
    assert sf_timeout >= 300, (
        f"SF TimeoutSeconds for RegimeSubstrate must be >= Lambda timeout "
        f"(300s per setup-regime-lambda.sh); got {sf_timeout}"
    )


# ─────────────────────────────────────────────────────────────────────
# T1 retrospective eval state — Stage C.2 T1 wiring (regime-v3 §5.3.3)
# ─────────────────────────────────────────────────────────────────────


def test_regime_retrospective_eval_state_exists() -> None:
    sf = _sf()
    states = _flat_states(sf)
    assert "RegimeRetrospectiveEval" in states, (
        "Saturday SF must contain a RegimeRetrospectiveEval state — "
        "Stage C.2 T1 wiring (regime-v3 §5.3.3); config#885 relocated it "
        "into ResearchPredictorParallel Branch A."
    )
    state = states["RegimeRetrospectiveEval"]
    assert state["Type"] == "Task"
    assert state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert state["Parameters"]["FunctionName"] == (
        "alpha-engine-predictor-regime-retrospective-eval:live"
    )


def test_regime_retrospective_eval_payload_is_produce_action() -> None:
    """Handler-side dry_run does NOT write the artifact. Post the Friday
    shell-run KEYSTONE the action is routed via the ``$.regime_action``
    control var (InitializeInput seeds ``"produce"`` → real Saturday run
    behaviourally identical; ApplyShellRunDefaults flips to ``"dry_run"``
    ONLY under shell_run — verified clean no-write dry path)."""
    sf = _sf()
    states = _flat_states(sf)
    payload = states["RegimeRetrospectiveEval"]["Parameters"]["Payload"]
    assert payload.get("action.$") == "$.regime_action", (
        "RegimeRetrospectiveEval payload must route action via "
        f"$.regime_action (keystone dry-routing); got {payload!r}"
    )
    init_expr = sf["States"]["InitializeInput"]["Parameters"]["merged.$"]
    assert '"regime_action":"produce"' in init_expr, (
        "InitializeInput must seed regime_action='produce' so the real "
        "Saturday run still calls produce"
    )


def test_check_skip_regime_retrospective_eval_routes_correctly() -> None:
    """{\"skip_regime_retrospective_eval\": true} bypasses to
    CheckSkipDataPhase2. config#885 relocated the
    Scanner→RAG→RegimeSubstrate→RegimeRetrospectiveEval chain INTO
    ResearchPredictorParallel Branch A (so PredictorTraining forks
    parallel to it after DataPhase1). RegimeRetrospectiveEval +
    CheckSkipRegimeRetrospectiveEval now live INSIDE Branch A, so the
    skip path lands on the sibling Branch-A state CheckSkipDataPhase2,
    NOT the Parallel state itself (which would be an invalid
    branch-internal → parent transition). alpha-engine-config-I2515 Phase
    B removed the multi-agent Research state (and CheckSkipResearch) that
    used to sit here — CheckSkipDataPhase2 is now the direct successor.
    Independent of skip_regime_substrate so each regime step has its own
    skip flag."""
    sf = _sf()
    branch_a = sf["States"]["ResearchPredictorParallel"]["Branches"][0][
        "States"
    ]
    assert "CheckSkipRegimeRetrospectiveEval" in branch_a
    state = branch_a["CheckSkipRegimeRetrospectiveEval"]
    assert state["Type"] == "Choice"
    assert state["Default"] == "RegimeRetrospectiveEval"
    skip_choice = state["Choices"][0]
    skip_vars = skip_choice["And"]
    assert any(
        c.get("Variable") == "$.skip_regime_retrospective_eval"
        for c in skip_vars
    )
    # config#885: the chain is INSIDE Branch A now → skip continues to the
    # sibling Branch-A state CheckSkipDataPhase2, never the parent Parallel.
    assert skip_choice["Next"] == "CheckSkipDataPhase2"
    assert "CheckSkipDataPhase2" in branch_a
    # The chain is no longer at top level — it forks parallel to Branch B.
    par = sf["States"]["ResearchPredictorParallel"]
    assert par["Type"] == "Parallel"
    # config#3134: Branch A's StartAt is now CheckSkipScanner (Scanner's
    # own skip gate), not Scanner directly.
    assert par["Branches"][0]["StartAt"] == "CheckSkipScanner"
    assert "CheckSkipRegimeRetrospectiveEval" not in sf["States"]
    assert "Scanner" not in sf["States"]


def test_regime_retrospective_eval_failure_is_non_blocking() -> None:
    """Observe-only contract: a T1 eval failure must NOT halt the
    pipeline. config#885 relocated the chain INTO Branch A, so the
    Catch[States.ALL] now routes to the sibling Branch-A state
    CheckSkipDataPhase2 (non-blocking, in-branch, alpha-engine-config-I2515
    Phase B's new direct successor) — NOT ResearchPredictorParallel (the
    parent, an invalid branch→parent transition) and NOT HandleFailure
    (blocking, and an invalid branch→top-level transition that would
    re-introduce SF Parallel cross-branch cancellation). T1 is
    observability, not gating."""
    sf = _sf()
    branch_a = sf["States"]["ResearchPredictorParallel"]["Branches"][0][
        "States"
    ]
    catches = branch_a["RegimeRetrospectiveEval"]["Catch"]
    states_all = next(c for c in catches if c["ErrorEquals"] == ["States.ALL"])
    assert states_all["Next"] == "CheckSkipDataPhase2", (
        "RegimeRetrospectiveEval Catch[States.ALL] must route to the "
        "in-branch state CheckSkipDataPhase2 (non-blocking), "
        "not HandleFailure (blocking) nor the parent Parallel. T1 is "
        "observability; a failure must not halt Branch A."
    )
    assert states_all["Next"] in branch_a


def test_regime_retrospective_eval_next_is_data_phase2_skip_gate() -> None:
    """config#885: the chain lives INSIDE Branch A, so
    RegimeRetrospectiveEval's success continuation is its sibling
    Branch-A state CheckSkipDataPhase2, not the parent Parallel.
    alpha-engine-config-I2515 Phase B removed the multi-agent Research
    state (and CheckSkipResearch) that used to sit between this chain and
    DataPhase2."""
    sf = _sf()
    branch_a = sf["States"]["ResearchPredictorParallel"]["Branches"][0][
        "States"
    ]
    assert (
        branch_a["RegimeRetrospectiveEval"]["Next"] == "CheckSkipDataPhase2"
    )
    assert "CheckSkipDataPhase2" in branch_a


def test_regime_retrospective_eval_timeout_accommodates_smoother_fit() -> None:
    """Lambda timeout is 600s (per setup-regime-retrospective-eval-lambda.sh)
    — smoother fit + signals/ archive enumeration is heavier than substrate
    fit. SF TimeoutSeconds must be slightly above the Lambda's own timeout."""
    sf = _sf()
    states = _flat_states(sf)
    sf_timeout = states["RegimeRetrospectiveEval"]["TimeoutSeconds"]
    assert sf_timeout >= 600, (
        f"SF TimeoutSeconds for RegimeRetrospectiveEval must be >= Lambda "
        f"timeout (600s per setup-regime-retrospective-eval-lambda.sh); "
        f"got {sf_timeout}"
    )
