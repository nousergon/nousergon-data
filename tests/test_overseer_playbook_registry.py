"""Contract test for the Overseer playbook registry (alpha-engine-config-I2823).

Pins:
  1. ``infrastructure/overseer/playbooks.yaml`` validates against its shipped
     JSON Schema (``playbooks.schema.json``) — the registry is a versioned
     contract, not a loose config file.
  2. Cross-reference integrity: every playbook's ``executor_lambda_dir``
     exists in ``infrastructure/lambdas/`` and its ``executor_function``
     matches the fleet naming convention derived from that dir.
  3. Benign-reason lockstep: every ``benign_reasons`` entry appears as a
     literal ``"reason": "<value>"`` in the executor's ``index.py`` — a
     registry reason the executor can never return is dead config; an executor
     decline the registry doesn't know stays escalating (correct default), but
     a TYPO'd benign reason would silently page on a by-design decline.
  4. Kill-switch lockstep: ``kill_switch_env`` appears in the executor source.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
OVERSEER_DIR = REPO_ROOT / "infrastructure" / "overseer"
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"

REGISTRY = yaml.safe_load((OVERSEER_DIR / "playbooks.yaml").read_text())
SCHEMA = json.loads((OVERSEER_DIR / "playbooks.schema.json").read_text())


def test_registry_validates_against_schema():
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(REGISTRY, SCHEMA)


@pytest.mark.parametrize("name", sorted(REGISTRY["playbooks"]))
def test_executor_lambda_dir_exists(name):
    spec = REGISTRY["playbooks"][name]
    lambda_dir = LAMBDAS_DIR / spec["executor_lambda_dir"]
    assert (lambda_dir / "index.py").is_file(), (
        f"playbook {name!r}: executor_lambda_dir {spec['executor_lambda_dir']!r} "
        f"has no index.py under infrastructure/lambdas/"
    )


@pytest.mark.parametrize("name", sorted(REGISTRY["playbooks"]))
def test_executor_function_matches_dir_convention(name):
    spec = REGISTRY["playbooks"][name]
    assert spec["executor_function"] == f"alpha-engine-{spec['executor_lambda_dir']}", (
        f"playbook {name!r}: executor_function {spec['executor_function']!r} does not "
        f"follow the alpha-engine-<lambda-dir> fleet naming convention"
    )


@pytest.mark.parametrize(
    "name",
    sorted(k for k, v in REGISTRY["playbooks"].items() if v.get("routed")),
)
def test_benign_reasons_exist_in_executor_source(name):
    spec = REGISTRY["playbooks"][name]
    src = (LAMBDAS_DIR / spec["executor_lambda_dir"] / "index.py").read_text()
    returnable = set(re.findall(r'"reason":\s*"([a-z_]+)"', src))
    missing = set(spec["benign_reasons"]) - returnable
    assert not missing, (
        f"playbook {name!r}: benign_reasons {sorted(missing)} never appear as "
        f'literal "reason" values in {spec["executor_lambda_dir"]}/index.py — '
        f"typo'd benign reasons silently page on by-design declines"
    )


@pytest.mark.parametrize("name", sorted(REGISTRY["playbooks"]))
def test_kill_switch_env_exists_in_executor_source(name):
    spec = REGISTRY["playbooks"][name]
    src = (LAMBDAS_DIR / spec["executor_lambda_dir"] / "index.py").read_text()
    assert spec["kill_switch_env"] in src, (
        f"playbook {name!r}: kill_switch_env {spec['kill_switch_env']!r} not found "
        f"in {spec['executor_lambda_dir']}/index.py"
    )


def test_router_bundles_this_registry():
    """The router's deploy.sh must copy THIS registry file into its zip —
    pin the copy line so a rename breaks CI, not the deploy."""
    deploy = (LAMBDAS_DIR / "overseer-dispatcher" / "deploy.sh").read_text()
    assert "overseer/playbooks.yaml" in deploy
