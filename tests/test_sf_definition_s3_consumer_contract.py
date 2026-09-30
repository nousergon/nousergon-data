"""The staged S3 Step Function definitions have a THIRD consumer now, and it
halts the trading day (``alpha-engine-config-I7927``).

WHAT THIS PINS
--------------
``infrastructure/deploy-infrastructure.sh`` stamps each definition with
``[git:<sha>] `` and uploads exactly the bytes it then feeds to
``update-state-machine``. Until 2026-08-21 that S3 copy had two consumers:
CloudFormation's ``DefinitionS3Location`` and
``infrastructure/step-functions/check-definition-drift.py``.

``crucible-predictor#538`` added a third. ``inference/deploy_drift.py::
_fetch_s3_definition`` now reads these objects to obtain the **expected**
definition — and, after ``crucible-predictor#540``, the expected deploy SHA —
so the preopen ``DeployDriftCheck`` can reach a verdict without calling
``api.github.com``. ``DeployDriftGate`` **halts trading** when it cannot reach
one, which makes these key names trading-critical.

THE FAILURE MODE
----------------
Rename or re-prefix one of these keys in the deploy, or drop the Comment stamp,
and the probe does not fail. It falls back to ``raw.githubusercontent.com`` —
precisely the third-party dependency on the critical path of the trading day
that I7927 removed — and **nothing goes red** until the morning GitHub is
unreachable. That is a detection blindness, not a bug that announces itself, so
it gets a test rather than a comment.

``infrastructure/contracts/sf_definition_s3.consumer.json`` declares the
contract; this pins that declaration against what the deploy script actually
does, and against the consumer's own path map, which is reproduced here because
neither repo's CI can read the other.
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONTRACT_PATH = _ROOT / "infrastructure" / "contracts" / "sf_definition_s3.consumer.json"
_DEPLOY_SH = _ROOT / "infrastructure" / "deploy-infrastructure.sh"

#: ``crucible-predictor inference/deploy_drift.py::_SF_DEFINITION_PATHS`` — the
#: consumer's own pipeline→path map, reproduced because this repo's CI cannot
#: read that one. The consumer builds its S3 key from these paths verbatim
#: (``_fetch_s3_definition(sf_definition_path)``), so a divergence here is a
#: divergence in what the preopen probe will actually fetch.
_CONSUMER_PATHS = {
    "ne-preopen-trading-pipeline": "infrastructure/step_function_daily.json",
    "ne-postclose-trading-pipeline": "infrastructure/step_function_eod.json",
}

#: ``crucible-predictor inference/deploy_drift.py::_DEFINITION_BUCKET``.
_CONSUMER_BUCKET = "alpha-engine-research"


def _load_guard():
    """Import the drift backstop by path — its filename has hyphens."""
    spec = importlib.util.spec_from_file_location(
        "check_definition_drift",
        _ROOT / "infrastructure" / "step-functions" / "check-definition-drift.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def contract() -> dict:
    return json.loads(_CONTRACT_PATH.read_text())


@pytest.fixture(scope="module")
def deploy_sh() -> str:
    return _DEPLOY_SH.read_text()


def test_every_declared_definition_is_uploaded_by_the_deploy(contract, deploy_sh):
    for entry in contract["definitions"]:
        key = entry["s3_key"]
        assert f'"s3://$BUCKET/{key}"' in deploy_sh, (
            f"{entry['pipeline']}: the contract declares s3://{contract['bucket']}"
            f"/{key} but deploy-infrastructure.sh does not upload to it"
        )


def test_the_declared_bucket_is_the_one_the_deploy_writes(contract, deploy_sh):
    assert contract["bucket"] == _CONSUMER_BUCKET
    assert f'BUCKET="{contract["bucket"]}"' in deploy_sh


def test_the_s3_key_is_the_repo_path_verbatim(contract):
    """The consumer derives its key from the definition's repo path rather than
    from a mapping, so the identity is load-bearing, not cosmetic."""
    for entry in contract["definitions"]:
        assert entry["s3_key"] == entry["repo_path"], entry["pipeline"]


def test_every_declared_definition_exists_in_the_repo(contract):
    for entry in contract["definitions"]:
        assert (_ROOT / entry["repo_path"]).exists(), entry["repo_path"]


def test_the_drift_probe_consumers_match_the_consumers_own_path_map(contract):
    """If the two ever disagree, the preopen probe is fetching a key this repo
    is not writing — and it degrades to GitHub in silence."""
    declared = {
        e["pipeline"]: e["s3_key"]
        for e in contract["definitions"] if e["drift_probe_consumer"]
    }
    assert declared == _CONSUMER_PATHS, (
        "the contract's drift-probe consumers no longer match crucible-predictor "
        "_SF_DEFINITION_PATHS. Update BOTH repos in one cross-repo pair of PRs, "
        "or the preopen probe silently loses its in-region source."
    )


def test_the_deploy_stamps_the_comment_the_consumer_reads(deploy_sh):
    """``_extract_sf_sha`` matches ``^\\[git:([0-9a-f]{7,40})\\]`` against the
    top-level ``Comment``. That stamp is how the S3 copy says which deploy wrote
    it, which is what makes an in-region ``cf_drift`` possible at all."""
    assert "d['Comment'] = f'[git:{sha}] {orig}'.rstrip()" in deploy_sh
    assert re.search(r"GIT_SHA=\"\$\{GITHUB_SHA:-", deploy_sh)


def test_the_same_sha_tags_the_cloudformation_stack(deploy_sh):
    """cf_drift compares the stack tag against the S3 copy's stamp. That is only
    sound because one deploy run writes both from the same ``$GIT_SHA``."""
    assert deploy_sh.count('--tags "Key=git-sha,Value=$GIT_SHA"') == 2  # create + update


def test_the_stack_carries_the_fleet_cost_attribution_tag(deploy_sh):
    """alpha-engine-config-I10905 ruled ``system`` as the fleet cost-attribution
    key. A stack-level tag propagates to every taggable resource, so create and
    update must both carry it — and the value is the stack's own name."""
    tag = '"Key=system,Value=alpha-engine-orchestration"'
    assert deploy_sh.count(tag) == 2  # create + update
    assert "Key=component" not in deploy_sh


def test_the_upload_precedes_the_state_machine_update(deploy_sh):
    """Order matters for what a *failed* deploy leaves behind. Uploading first
    means a failed ``update-state-machine`` leaves S3 holding the NEW definition
    against an OLD live machine — which the probe reads as drift and halts on,
    the conservative direction. It is also what CloudFormation's
    ``DefinitionS3Location`` needs on a stack-create or replacement."""
    upload_at = deploy_sh.index('aws s3 cp "$DAILY_STAMPED"')
    update_at = deploy_sh.index('update_or_defer_to_cfn "$DAILY_ARN"')
    assert upload_at < update_at


def test_the_contract_states_what_happens_when_the_input_is_missing(contract):
    """A consumer contract that does not say what an absent input does is the
    half that gets improvised later (sf-pipeline-policy 2.3a rule 2)."""
    behavior = contract["consumer"]["missing_input_behavior"]
    assert "OMITTED" in behavior and "fail closed" in behavior.lower()


def test_check_definition_drift_still_guards_the_same_keys(contract):
    """The three-way backstop and this contract must not drift apart: it is what
    catches an out-of-band write to a key the preopen probe now trusts."""
    guard = _load_guard()
    guarded = {
        e["sf_name"]: f"{guard.S3_PREFIX}{e['definition_file']}"
        for e in guard.SF_DEFINITIONS
    }
    assert guard.S3_BUCKET == contract["bucket"]
    for entry in contract["definitions"]:
        assert guarded.get(entry["pipeline"]) == entry["s3_key"], (
            f"{entry['pipeline']}: the contract declares {entry['s3_key']} but "
            "check-definition-drift.py diffs "
            f"{guarded.get(entry['pipeline'])!r}"
        )
