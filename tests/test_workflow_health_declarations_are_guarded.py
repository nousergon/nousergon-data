"""PR CI must run the shared finding-surface validator (alpha-engine-config-I13916).

A workflow declares which of its steps are finding steps by NAME
(``WORKFLOW_HEALTH_FINDING_STEPS``). Renaming a step without updating the
declaration is caught only by ``workflow-health-semantics`` from nousergon-lib,
so the guarantee is exactly as strong as the job that invokes it. This pins
that job: it runs on every PR, from a release that contains the validator,
against this repository's own workflow directory, and nothing can skip it.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_ROOT = Path(__file__).resolve().parent.parent
_CI = _ROOT / ".github" / "workflows" / "workflow-health-declarations.yml"
_JOB = "workflow-health-declarations"
_MIN_LIB = 224  # v0.124.224 first shipped the console entry point
_ENTRY = "workflow-health-semantics"


def _ci() -> dict[str, Any]:
    return yaml.safe_load(_CI.read_text())


def _job() -> dict[str, Any]:
    jobs: dict[str, Any] = _ci()["jobs"]
    assert _JOB in jobs, f"{_CI.name} has no `{_JOB}` job"
    return jobs[_JOB]


def _runs() -> list[str]:
    return [s["run"] for s in _job()["steps"] if "run" in s]


def test_workflow_runs_on_pull_requests() -> None:
    # PyYAML parses the bare key `on` as boolean True.
    triggers = _ci().get("on", _ci().get(True))
    assert "pull_request" in triggers
    assert "merge_group" in triggers
    assert "push" in triggers
    # No paths filter on any trigger: it must report on every PR.
    for name, cfg in triggers.items():
        assert not (cfg or {}).get("paths"), f"{name} has a paths filter"
        assert not (cfg or {}).get("paths-ignore"), f"{name} has a paths-ignore filter"


def test_ci_yml_no_longer_carries_the_job() -> None:
    # It moved out so ci.yml stays held to the root nousergon-lib pin.
    ci = yaml.safe_load((_CI.parent / "ci.yml").read_text())
    assert _JOB not in ci["jobs"]


def test_job_is_unconditional() -> None:
    job = _job()
    assert "if" not in job
    assert not job.get("continue-on-error")
    for step in job["steps"]:
        assert "if" not in step, f"step {step.get('name')} is conditional"
        assert not step.get("continue-on-error"), step.get("name")


def test_validator_invoked_against_local_workflows() -> None:
    invocations = [
        line.strip()
        for run in _runs()
        for line in run.splitlines()
        if line.strip().startswith(_ENTRY)
    ]
    assert invocations == [f"{_ENTRY} --workflow-dir .github/workflows"]


def test_validator_comes_from_a_pinned_release_with_it() -> None:
    installs = [line for run in _runs() for line in run.splitlines() if "pip install" in line and "nousergon-lib" in line]
    assert len(installs) == 1, installs
    m = re.search(r"nousergon/nousergon-lib@v0\.124\.(\d+)\"", installs[0])
    assert m, f"nousergon-lib must be pinned to a v0.124.N tag: {installs[0]}"
    assert int(m.group(1)) >= _MIN_LIB
    assert "[" not in installs[0].split("nousergon-lib", 1)[1].split("@", 1)[0], "no extras needed"


def test_no_secret_reaches_the_job() -> None:
    assert "secrets." not in yaml.safe_dump(_job())


def test_workflow_dir_exists() -> None:
    assert (_ROOT / ".github" / "workflows").is_dir()
