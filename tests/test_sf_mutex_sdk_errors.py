"""AWS SDK DynamoDb errors must not fall into the infrastructure continuation.

AWS documents mixed-case DynamoDb for aws-sdk, uppercase DynamoDB only for
optimized integrations: https://docs.aws.amazon.com/step-functions/latest/dg/connect-ddb.html
"""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEFINITIONS = sorted(
    p for p in (ROOT / "infrastructure").glob("step_function*.json")
    if "AcquireMutex" in json.loads(p.read_text())["States"]
)


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda p: p.name)
@pytest.mark.parametrize(
    "error, target",
    [
        ("DynamoDb.ConditionalCheckFailedException", "MutexConflict"),
        ("DynamoDb.InternalServerError", "SetMutexAcquireDegradedFlag"),
    ],
)
def test_sdk_error_routes_to_its_intended_outcome(path, error, target):
    states = json.loads(path.read_text())["States"]
    task = states["AcquireMutex"]
    assert task["Resource"] == "arn:aws:states:::aws-sdk:dynamodb:putItem"
    route = next(
        catch["Next"] for catch in task["Catch"]
        if error in catch["ErrorEquals"] or "States.ALL" in catch["ErrorEquals"]
    )
    if error.endswith("ConditionalCheckFailedException") and "ReadRunSlotHolder" in states:
        # alpha-engine-config-I12228 (weekly only): a held slot is first
        # inspected, and taken over only from a terminal-FAILED holder; every
        # other answer still ends at MutexConflict. The takeover chain itself
        # is pinned by tests/test_sf_run_slot_takeover_and_release.py.
        assert route == "ReadRunSlotHolder"
        route = target
    assert route == target
    if error.endswith("ConditionalCheckFailedException"):
        assert states[route]["Type"] == "Fail"


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda p: p.name)
def test_sdk_retry_and_catch_use_sdk_error_namespace(path):
    states = json.loads(path.read_text())["States"]
    for name, task in states.items():
        if not str(task.get("Resource", "")).startswith("arn:aws:states:::aws-sdk:dynamodb:"):
            continue
        for handler in task.get("Retry", []) + task.get("Catch", []):
            assert not any(e.startswith("DynamoDB.") for e in handler["ErrorEquals"]), name
