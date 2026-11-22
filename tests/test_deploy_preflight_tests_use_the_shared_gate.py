"""No deploy.sh may hand-roll its preflight handler-test block.

WHY (measured 2026-08-28). `_shared/run_handler_tests.sh` exists precisely so
the "install this lambda's test deps, then run its tests" step cannot drift
per-script (config#2381, after the config#2295 incident). Thirty-one deploy.sh
scripts used it. FOUR still carried a hand-rolled copy, and every one of the
four was a variant of:

    if [[ -f "${SCRIPT_DIR}/test_handler.py" ]]; then
      python3 -m pip install --quiet --target "${TEST_DEPS}" pytest
      PYTHONPATH="${TEST_DEPS}" python3 -m pytest "${SCRIPT_DIR}/test_handler.py" -q
    fi

Three defects live in that shape, all of which the shared helper had already
fixed for everyone else:

  1. The dep list is hand-written, so it silently omits what index.py imports
     at module scope. `deploy-preflight-sweep-dispatcher.yml` failed **6 of 6
     runs since it shipped on 2026-08-13** — `ModuleNotFoundError: No module
     named boto3` — and had therefore NEVER deployed successfully.
     `spot-interruption-recorder` hit the identical wall earlier and patched
     its own copy in place instead of the helper, which is exactly what left
     the other three exposed: fixing one call site of a systemic defect is not
     a fix.
  2. Only `test_handler.py` runs. Sibling `test_*.py` files beside it are
     silently never executed, so a test added next to the handler is dead on
     arrival.
  3. `PYTHONPATH` carries only the scratch dir, not `infrastructure/lambdas/`,
     so a handler importing a shared sibling by bare name (flow_doctor_telegram,
     eod_artifact_verification) passes locally and ModuleNotFounds on the
     runner — alpha-engine-config-I7582, already absorbed by the helper.

WHY A DERIVED SCOPE. A hardcoded list of today's scripts leaves the next one
uncovered on the day it lands, which is how all four of these survived. The
universe is every git-tracked `infrastructure/lambdas/*/deploy.sh`.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# The two carve-outs the helper's own header declares: these run
# `python3 test_handler.py` directly with zero deps and no pytest at all, and
# that is preserved deliberately in ci.yml too. Named here so the exemption is
# visible and finite rather than inferred from a passing test.
_DECLARED_CARVE_OUTS = {
    "changelog-incident-mirror",
    "changelog-cloudwatch-mirror",
}

# A debt register, not a permanent exemption: each entry is tracked, each has a
# named reason, and the set must shrink. An entry whose issue is closed and whose
# gate is still missing is itself a finding.
#
#   thinktank-spot-dispatcher — REMOVED 2026-08-28 (alpha-engine-config-I9114).
#     Its deploy.sh was the one private dialect in this repo — bare `aws` calls,
#     a host-arch `pip install --target`, no handler-test gate, and a hand-rolled
#     `get-role >/dev/null 2>&1 || create-role` IAM block carrying the exact
#     misclassification the shared applier had just been fixed for. It is now
#     migrated onto deploy_run.sh / run_handler_tests.sh / apply_iam_policy.sh,
#     and tests/test_thinktank_deploy_uses_shared_helpers.py EXECUTES the result
#     against a fake `aws` to prove the code-only path reaches no IAM write.
_TRACKED_UNGATED: set[str] = set()

# A hand-rolled gate is any direct `python3 -m pytest` invocation inside a
# deploy.sh. The helper is the only sanctioned way to reach pytest from one.
_BARE_PYTEST = re.compile(r"python3\s+-m\s+pytest")


def _code_only(body: str) -> str:
    """Shell source with `#` comments stripped.

    NOT cosmetic. Eleven deploy.sh scripts carry the line

        # ... so this gate can never re-drift into the naive no-install
        # `python3 -m pytest` form (config#2295).

    directly above their CORRECT `run_handler_tests` call. A scan that reads
    that comment as an execution path reports eleven false offenders and puts
    the pressure on deleting the rationale that explains the fix — the fleet
    has already recorded this exact shape once (a scheduled-identity scan that
    matched a role name inside a YAML comment and went red on main).

    Deliberately naive about `#` inside quotes: a false NEGATIVE here costs a
    missed offender that the sibling assertion below still catches, whereas a
    false POSITIVE costs a red main and a deleted comment.
    """
    return "\n".join(line.split("#", 1)[0] for line in body.splitlines())


def _deploy_scripts() -> list[Path]:
    out = subprocess.run(  # noqa: S603
        ["git", "ls-files", "infrastructure/lambdas/*/deploy.sh"],  # noqa: S607
        cwd=_REPO_ROOT, capture_output=True, text=True, check=True,
    )
    paths = [_REPO_ROOT / line for line in out.stdout.split() if line]
    assert paths, "no deploy.sh discovered — the glob or cwd is wrong"
    return paths


def test_every_deploy_script_reaches_pytest_through_the_shared_gate() -> None:
    offenders: list[str] = []
    for path in _deploy_scripts():
        if path.parent.name in _DECLARED_CARVE_OUTS:
            continue
        body = _code_only(path.read_text())
        if _BARE_PYTEST.search(body):
            offenders.append(f"{path.parent.name}: invokes `python3 -m pytest` directly")
    assert not offenders, (
        "deploy.sh scripts hand-rolling their preflight handler tests instead of "
        "sourcing _shared/run_handler_tests.sh:\n  " + "\n  ".join(offenders)
        + "\n\nUse:\n"
        '  source "${SCRIPT_DIR}/../_shared/run_handler_tests.sh"\n'
        '  run_handler_tests "${SCRIPT_DIR}" [deps...]\n'
        "\nA hand-rolled copy hand-writes its dep list (which is how "
        "preflight-sweep-dispatcher failed 6 of 6 deploys on a missing boto3), "
        "runs only test_handler.py, and omits infrastructure/lambdas/ from "
        "PYTHONPATH."
    )


def test_a_lambda_with_handler_tests_actually_runs_them() -> None:
    """The other half: sourcing the helper is not the same as calling it. A
    deploy.sh with a test_handler.py beside it that never invokes the gate has
    no preflight at all — the config#2295 shape, where the drift was invisible
    pre-merge because ci.yml has its own correct runner."""
    offenders: list[str] = []
    for path in _deploy_scripts():
        name = path.parent.name
        if name in _DECLARED_CARVE_OUTS or name in _TRACKED_UNGATED:
            continue
        if not (path.parent / "test_handler.py").exists():
            continue
        if "run_handler_tests " not in _code_only(path.read_text()):
            offenders.append(name)
    assert not offenders, (
        "these lambdas have a test_handler.py that their deploy.sh never runs, "
        "so the post-merge preflight gate is absent for them:\n  "
        + "\n  ".join(offenders)
    )


def test_the_ungated_debt_register_does_not_grow_silently() -> None:
    """`_TRACKED_UNGATED` is a debt register of named, tracked entries.
    It must shrink, never grow — and an entry that has since been GATED must be
    removed from it, or the exemption outlives the debt and starts hiding a
    regression on the same lambda."""
    assert len(_TRACKED_UNGATED) <= 1, (
        "the ungated-lambda exemption list grew. A new lambda shipping without "
        "its preflight gate is the defect this file exists to stop; add the "
        "gate rather than an exemption."
    )
    still_ungated = {
        name for name in _TRACKED_UNGATED
        if "run_handler_tests " not in _code_only(
            (_REPO_ROOT / "infrastructure" / "lambdas" / name / "deploy.sh").read_text()
        )
    }
    assert still_ungated == _TRACKED_UNGATED, (
        "these are now gated and must be removed from _TRACKED_UNGATED: "
        f"{sorted(_TRACKED_UNGATED - still_ungated)}"
    )
