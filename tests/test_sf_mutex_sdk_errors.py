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
    assert route == target
    if error.endswith("ConditionalCheckFailedException"):
        assert states[route]["Type"] == "Fail"


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda p: p.name)
def test_sdk_retry_and_catch_use_sdk_error_namespace(path):
    task = json.loads(path.read_text())["States"]["AcquireMutex"]
    for handler in task.get("Retry", []) + task["Catch"]:
        assert not any(e.startswith("DynamoDB.") for e in handler["ErrorEquals"])
