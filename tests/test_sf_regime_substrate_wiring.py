"""tests/test_sf_regime_substrate_wiring.py — pin the Saturday SF wiring
for the RegimeSubstrate state.

The regime substrate Lambda lives in alpha-engine-predictor; the SF
state that invokes it lives here. This test verifies the wiring
contract between the two so silent drift (e.g. a refactor renames the
Lambda or removes the state) breaks CI rather than the next Saturday SF.

Five invariants:

1. ``RegimeSubstrate`` state exists and is a Task that invokes
   ``alpha-engine-predictor-regime-substrate:live``.
2. ``CheckSkipRegimeSubstrate`` state exists and routes to either
   ``RegimeSubstrate`` (default) or ``CheckSkipResearch`` (when
   ``skip_regime_substrate: true``).
3. The post-RAG control flow lands on ``CheckSkipRegimeSubstrate``,
   not on ``CheckSkipResearch`` directly. Two routes: the
   ``CheckSkipRAGIngestion`` skip path AND the
   ``CheckRAGIngestionStatus`` success path.
4. ``RegimeSubstrate``'s Catch routes to ``CheckSkipResearch``, not
   ``HandleFailure`` — pins the Stage A observe-only contract that a
   regime failure must not halt Research.
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


def test_regime_substrate_state_exists() -> None:
    sf = _sf()
    assert "RegimeSubstrate" in sf["States"], (
        "Saturday SF must contain a RegimeSubstrate state — Stage A "
        "ships the substrate Lambda invocation between RAG and Research."
    )
    state = sf["States"]["RegimeSubstrate"]
    assert state["Type"] == "Task"
    assert state["Resource"] == "arn:aws:states:::lambda:invoke"
    assert state["Parameters"]["FunctionName"] == "alpha-engine-predictor-regime-substrate:live"


def test_regime_substrate_payload_is_produce_action() -> None:
    """Handler-side ``dry_run`` mode returns the payload without writing
    to S3. Production SF must always call ``produce`` so the artifact
    actually lands; pin to catch a misguided debugging change."""
    sf = _sf()
    payload = sf["States"]["RegimeSubstrate"]["Parameters"]["Payload"]
    assert payload.get("action") == "produce", (
        f"RegimeSubstrate payload must be action=produce; got {payload!r}"
    )


def test_check_skip_regime_substrate_routes_correctly() -> None:
    sf = _sf()
    assert "CheckSkipRegimeSubstrate" in sf["States"]
    state = sf["States"]["CheckSkipRegimeSubstrate"]
    assert state["Type"] == "Choice"
    assert state["Default"] == "RegimeSubstrate"
    skip_choice = state["Choices"][0]
    skip_vars = skip_choice["And"]
    assert any(c.get("Variable") == "$.skip_regime_substrate" for c in skip_vars)
    assert skip_choice["Next"] == "CheckSkipResearch"


def test_post_rag_control_flow_lands_on_regime_skip_gate() -> None:
    """Both post-RAG routes must point at CheckSkipRegimeSubstrate, not
    CheckSkipResearch directly. Catches a refactor that bypasses the
    regime state."""
    sf = _sf()
    # CheckSkipRAGIngestion's skip-branch
    skip_choice = sf["States"]["CheckSkipRAGIngestion"]["Choices"][0]
    assert skip_choice["Next"] == "CheckSkipRegimeSubstrate", (
        "CheckSkipRAGIngestion skip path must land on CheckSkipRegimeSubstrate"
    )
    # CheckRAGIngestionStatus's success branch
    success_choice = next(
        c for c in sf["States"]["CheckRAGIngestionStatus"]["Choices"]
        if c.get("StringEquals") == "Success"
    )
    assert success_choice["Next"] == "CheckSkipRegimeSubstrate", (
        "CheckRAGIngestionStatus success path must land on CheckSkipRegimeSubstrate"
    )


def test_regime_substrate_failure_is_non_blocking() -> None:
    """Stage A observe-only contract: RegimeSubstrate failure must NOT
    halt the pipeline. The Catch[States.ALL] routes to CheckSkipResearch
    (not HandleFailure) so Research still runs and the next Saturday
    has another chance to fit the HMM."""
    sf = _sf()
    catches = sf["States"]["RegimeSubstrate"]["Catch"]
    states_all = next(c for c in catches if c["ErrorEquals"] == ["States.ALL"])
    assert states_all["Next"] == "CheckSkipResearch", (
        "RegimeSubstrate Catch[States.ALL] must route to CheckSkipResearch "
        "(non-blocking), not HandleFailure (blocking). Stage A is observe-only; "
        "a regime substrate failure during the 4-week observation period must "
        "not halt Research."
    )


def test_regime_substrate_next_is_check_skip_research() -> None:
    """Success path also lands on CheckSkipResearch — substrate is
    observe-only at Stage A, so it's parallel to research, not gating."""
    sf = _sf()
    assert sf["States"]["RegimeSubstrate"]["Next"] == "CheckSkipResearch"


def test_regime_substrate_timeout_is_reasonable() -> None:
    """Lambda timeout is 300s (per setup-regime-lambda.sh); SF
    TimeoutSeconds must be slightly higher to avoid premature
    timeout-due-to-SF when the Lambda is still working."""
    sf = _sf()
    sf_timeout = sf["States"]["RegimeSubstrate"]["TimeoutSeconds"]
    assert sf_timeout >= 300, (
        f"SF TimeoutSeconds for RegimeSubstrate must be >= Lambda timeout "
        f"(300s per setup-regime-lambda.sh); got {sf_timeout}"
    )
