"""A Lambda that records a run manifest must be granted the manifest prefix.

`run_units.recorded_entry` writes one manifest per execution through
`nousergon_lib.run_manifest.S3ManifestSink` to `data_collection/runs/<unit>/…`,
whatever the unit's own `writes[]` say. On 2026-09-15 the dashboard box's D36
daily-news wrote all of its data and then died on AccessDenied at exactly that
write; `alpha-engine-crypto-balances-role` simulated implicitDeny on the same
prefix while its schedule was paused (alpha-engine-config-I10865).

Derived, never listed: every `infrastructure/lambdas/*/index.py` that imports
`run_units` must carry a codified `iam-policy.json` granting `s3:PutObject` on
the prefix, so a new recording Lambda is covered the day it is added.
"""
from __future__ import annotations

import ast
import fnmatch
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
LAMBDAS = REPO / "infrastructure" / "lambdas"
SAMPLE = "arn:aws:s3:::alpha-engine-research/data_collection/runs/D38/2026-09-14/01RUN.json"


def _imports_run_units(index: Path) -> bool:
    tree = ast.parse(index.read_text(encoding="utf-8"), filename=str(index))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(a.name == "run_units" for a in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and node.module == "run_units":
            return True
    return False


RECORDING_LAMBDAS = sorted(
    p.parent.name for p in LAMBDAS.glob("*/index.py") if _imports_run_units(p)
)


def test_the_derivation_finds_the_lambda_it_was_written_for():
    assert "crypto-balances" in RECORDING_LAMBDAS, (
        "crypto-balances/index.py no longer imports run_units as derived — this "
        "test would assert nothing about the Lambda it was created for")


def _put_object_resources(policy: Path) -> list[str]:
    out: list[str] = []
    for stmt in json.loads(policy.read_text(encoding="utf-8")).get("Statement", []):
        actions = stmt.get("Action", [])
        actions = [actions] if isinstance(actions, str) else actions
        if stmt.get("Effect") == "Allow" and ("s3:PutObject" in actions or "s3:*" in actions):
            res = stmt.get("Resource", [])
            out.extend([res] if isinstance(res, str) else res)
    return out


@pytest.mark.parametrize("name", RECORDING_LAMBDAS)
def test_a_recording_lambda_may_write_its_run_manifest(name: str):
    policy = LAMBDAS / name / "iam-policy.json"
    assert policy.exists(), f"{name} records a run manifest but codifies no iam-policy.json"
    grants = _put_object_resources(policy)
    assert any(fnmatch.fnmatchcase(SAMPLE, g) for g in grants), (
        f"{name}/index.py imports run_units, so every execution writes "
        f"s3://alpha-engine-research/data_collection/runs/…, and its iam-policy.json "
        f"grants no s3:PutObject there (grants: {sorted(grants)}). Add a "
        f"statement on arn:aws:s3:::alpha-engine-research/data_collection/runs/*.")
