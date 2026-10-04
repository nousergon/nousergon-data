"""Every boto3 client in the SF preflight modules names its region.

alpha-engine-config-I11567 measured a ``NoRegionError`` on the weekly box: its
SSM shell exports no ``AWS_DEFAULT_REGION``, and the preflight checks now run
there (alpha-engine-config-I11568). I11567 and I11568 fixed the CloudWatch and
Step Functions clients they hit, but the rest of ``sf_preflight.py`` still
relied on the ambient region. That covers ``check_backfill_source_freshness``
and ``check_postflight_contracts`` (alpha-engine-config-I11574's note), the
skip-artifact S3 client, and the Step Functions, Lambda, CloudWatch and IAM
clients. S3 happens to tolerate a missing region; the other services do not.
So this guard is a static scan rather than a per-check test: a new client
added without ``region_name`` fails here before it can meet a box with no
region.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_MODULES = ("sf_preflight.py", "sf_preflight_on_spot.py")


def _client_calls(path: Path) -> list[ast.Call]:
    tree = ast.parse(path.read_text(), filename=str(path))
    return [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "client"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in {"boto3", "_boto3"}
    ]


@pytest.mark.parametrize("module", _MODULES)
def test_every_boto3_client_names_its_region(module: str) -> None:
    path = _ROOT / module
    calls = _client_calls(path)
    assert calls, f"{module}: found no boto3.client calls; the scan is not seeing the module"
    missing = [
        f"{module}:{c.lineno}"
        for c in calls
        if not any(kw.arg == "region_name" for kw in c.keywords)
    ]
    assert not missing, (
        "boto3 client(s) built without region_name. The weekly box exports no "
        f"AWS_DEFAULT_REGION (alpha-engine-config-I11567): {missing}"
    )
