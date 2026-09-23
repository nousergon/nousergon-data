"""Lockstep guard for alpha-engine-config-I6688 deliverable 2: the weekly-SF-
critical path list must be identical across three places, or the check that
requires a Rehearsal-path trailer and the canary that auto-arms on those
same paths silently drift apart — exactly the shape weekly-sf-policy.md
§7.4 documents happening to the policy text itself (two sections, same
requirement, different vocabulary, different exemption sets).

The three places:

  1. ``scripts/check_rehearsal_path.py``'s ``WEEKLY_CRITICAL_GLOBS`` — the
     canonical list.
  2. ``.github/workflows/rehearsal-path-guard.yml``'s own ``weekly_critical``
     paths-filter (decides whether the trailer check even runs).
  3. The weekly-critical subset of ``.github/workflows/canary-replay.yml``'s
     ``weekly_critical`` paths-filter (decides whether the canary auto-arms
     — this filter also carries its own pre-existing entries unrelated to
     I6688, e.g. ``rag/pipelines/**``, so this test asserts a SUBSET
     relationship there, not equality).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECK_SCRIPT = REPO_ROOT / "scripts" / "check_rehearsal_path.py"
GUARD_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "rehearsal-path-guard.yml"
CANARY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "canary-replay.yml"


def _load_check_module():
    spec = importlib.util.spec_from_file_location("check_rehearsal_path", CHECK_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_rehearsal_path"] = module
    spec.loader.exec_module(module)
    return module


def _weekly_critical_filter_globs(workflow_path: Path, job_id: str) -> list[str]:
    """Extract the `weekly_critical` filter list embedded as a YAML string
    inside a `dorny/paths-filter` step's `with.filters` block."""
    definition = yaml.safe_load(workflow_path.read_text())
    steps = definition["jobs"][job_id]["steps"]
    filter_step = next(s for s in steps if s.get("id") == "filter")
    filters_block = filter_step["with"]["filters"]
    parsed = yaml.safe_load(filters_block)
    return list(parsed["weekly_critical"])


def test_check_script_globs_match_guard_workflow_filter():
    canonical = list(_load_check_module().WEEKLY_CRITICAL_GLOBS)
    guard_globs = _weekly_critical_filter_globs(GUARD_WORKFLOW, "label")
    assert guard_globs == canonical, (
        "rehearsal-path-guard.yml's paths-filter has drifted from "
        "scripts/check_rehearsal_path.py's WEEKLY_CRITICAL_GLOBS — "
        f"guard={guard_globs!r} canonical={canonical!r}"
    )


def test_canary_replay_filter_is_superset_of_weekly_critical_globs():
    canonical = set(_load_check_module().WEEKLY_CRITICAL_GLOBS)
    canary_globs = set(_weekly_critical_filter_globs(CANARY_WORKFLOW, "label"))
    missing = canonical - canary_globs
    assert not missing, (
        "canary-replay.yml's paths-filter is missing weekly-critical paths "
        f"that rehearsal-path-guard.yml enforces: {missing!r} — the canary "
        "will not auto-arm on a PR that the trailer check gates."
    )


# alpha-engine-config-I11435 deliverable 4: every in-repo Lambda the weekly
# Step Function invokes is decided explicitly. nousergon-data-PR1879 changed
# the weekly box's spot rotation inside a Lambda this check did not cover,
# and the guard reported `skipping`.

WEEKLY_SF_DEFINITION = REPO_ROOT / "infrastructure" / "step_function.json"
LAMBDAS_ROOT = REPO_ROOT / "infrastructure" / "lambdas"


def _invoked_function_names(definition: dict) -> set[str]:
    """Every Lambda ``FunctionName`` a Task state in the definition invokes,
    at any nesting depth (Parallel branches, Map iterators), with any
    ``:alias`` suffix and any ``arn:...:function:`` prefix stripped."""
    names: set[str] = set()

    def visit(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "FunctionName" and isinstance(value, str):
                    name = value.rsplit(":function:", 1)[-1].split(":", 1)[0]
                    names.add(name)
                else:
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(definition)
    return names


def _in_repo_lambda_dirs_by_function_name() -> dict[str, str]:
    """Map each deployed function name to its directory, read from the
    ``FUNCTION_NAME="..."`` line every Lambda's ``deploy.sh`` carries."""
    import re

    pattern = re.compile(r'^FUNCTION_NAME="([^"$]+)"', re.MULTILINE)
    mapping: dict[str, str] = {}
    for deploy in sorted(LAMBDAS_ROOT.glob("*/deploy.sh")):
        match = pattern.search(deploy.read_text())
        if match:
            mapping[match.group(1)] = deploy.parent.name
    return mapping


def _weekly_sf_in_repo_lambda_dirs() -> set[str]:
    import json

    invoked = _invoked_function_names(json.loads(WEEKLY_SF_DEFINITION.read_text()))
    by_name = _in_repo_lambda_dirs_by_function_name()
    # Names with no directory here deploy from other repos (evaluator,
    # predictor, research); this repo's guard cannot gate their PRs.
    return {by_name[name] for name in invoked if name in by_name}


def test_every_weekly_sf_lambda_in_this_repo_is_decided():
    module = _load_check_module()
    undecided = []
    for directory in sorted(_weekly_sf_in_repo_lambda_dirs()):
        probe = f"infrastructure/lambdas/{directory}/index.py"
        covered = bool(module.weekly_critical_paths_touched([probe]))
        excluded = directory in module.WEEKLY_SF_LAMBDAS_NOT_CRITICAL
        if covered == excluded:
            undecided.append((directory, covered, excluded))
    assert not undecided, (
        "each in-repo Lambda the weekly SF invokes must be in exactly one of "
        "WEEKLY_CRITICAL_GLOBS or WEEKLY_SF_LAMBDAS_NOT_CRITICAL in "
        f"scripts/check_rehearsal_path.py — (dir, covered, excluded): {undecided!r}"
    )


def test_the_weekly_sf_lambda_scan_is_not_vacuous():
    # If the definition's shape or deploy.sh's FUNCTION_NAME convention
    # changes, the scan above could silently find nothing and pass.
    dirs = _weekly_sf_in_repo_lambda_dirs()
    assert "weekly-freshness-spot-dispatcher" in dirs
    assert len(dirs) >= 5, dirs


def test_the_not_critical_list_names_only_lambdas_the_weekly_sf_invokes():
    stale = set(_load_check_module().WEEKLY_SF_LAMBDAS_NOT_CRITICAL) - _weekly_sf_in_repo_lambda_dirs()
    assert not stale, f"no longer invoked by the weekly SF, remove: {stale!r}"
