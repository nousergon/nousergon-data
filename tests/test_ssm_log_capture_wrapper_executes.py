"""The SF's ssm_log_capture wrapper must EXECUTE, not merely import.

Bug class this guards (config#1646, 2026-07-03): the weekly SF wraps every
EC2 workload as ``python -m <module> run --slug X --log Y -- bash <launcher>``.
At lib v0.66.0 ``nousergon_lib.ssm_log_capture`` became a re-export shim
(``import krepis.ssm_log_capture as _mod; sys.modules[__name__] = _mod``)
with no ``if __name__ == "__main__"`` block. Under ``python -m`` (runpy) the
shim imports, rebinds, falls off the end — **exit 0, the inner command never
runs, no log, no error**. All 11 SSM workload states became instant silent
"successes": no predictor training, no backtester, no evaluator, no emails —
while the SF (and the Friday-shell preflight, 107/107 "pass") reported green.

String-pinning wiring tests provably cannot catch this: the module was
importable, its public surface resolved — it just wasn't *executable*. The
only structural guard is to RUN the exact module path the SF invokes with a
sentinel inner command and assert (a) the sentinel executed, (b) the log file
was written, (c) the inner exit code propagates. That is what this test does.

The module paths under test are extracted from ``infrastructure/
step_function.json`` itself (chokepoint: a future caller migration is
automatically covered — whatever path the SF names must execute here).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SF_PATHS = [
    _REPO_ROOT / "infrastructure" / "step_function.json",
    _REPO_ROOT / "infrastructure" / "step_function_daily.json",
    _REPO_ROOT / "infrastructure" / "step_function_eod.json",
    _REPO_ROOT / "infrastructure" / "step_function_groom.json",
]

# `python -m <module> run --slug ...` as embedded in SF command strings.
_WRAPPER_RE = re.compile(r"-m\s+([\w.]+\.ssm_log_capture)\s+run\b")


def _wrapper_modules() -> set[str]:
    """Every ssm_log_capture module path any SF definition invokes via -m."""
    modules: set[str] = set()
    for sf_path in _SF_PATHS:
        if not sf_path.exists():
            continue
        modules.update(_WRAPPER_RE.findall(sf_path.read_text()))
    return modules


def test_sf_definitions_reference_at_least_one_wrapper():
    """Sanity: extraction must find the wrapper — an empty set would make the
    executability tests below vacuously pass (the silent-no-op of tests)."""
    assert _wrapper_modules(), (
        "No `-m <module>.ssm_log_capture run` invocations found in any SF "
        "definition — either the wrapper was removed (update this test's "
        "extraction) or the regex went stale."
    )


@pytest.mark.parametrize("module", sorted(_wrapper_modules()))
def test_wrapper_module_executes_inner_command(module, tmp_path):
    """`python -m <module> run ... -- <cmd>` must actually run <cmd>.

    A guard-less shim exits 0 here with no output and no log file — the
    exact 2026-07-03 failure mode — and this test fails on all three
    assertions.
    """
    log = tmp_path / "wrapper.log"
    sentinel = "SENTINEL_WRAPPER_EXECUTED_1646"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            module,
            "run",
            "--slug",
            "wrapper-execute-test",
            "--log",
            str(log),
            "--",
            sys.executable,
            "-c",
            f"print('{sentinel}')",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert sentinel in proc.stdout, (
        f"`python -m {module} run -- <cmd>` did NOT execute the inner "
        f"command (stdout={proc.stdout!r}, stderr tail="
        f"{proc.stderr[-300:]!r}). If {module} is a re-export shim it needs "
        "an `if __name__ == '__main__':` delegate — this is the config#1646 "
        "silent-no-op bug class."
    )
    assert log.exists() and sentinel in log.read_text(), (
        f"wrapper ran but did not tee the inner output to --log ({log})"
    )
    assert proc.returncode == 0


@pytest.mark.parametrize("module", sorted(_wrapper_modules()))
def test_wrapper_module_propagates_inner_exit_code(module, tmp_path):
    """A failing workload must fail the SSM command — rc must propagate."""
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            module,
            "run",
            "--slug",
            "wrapper-execute-test",
            "--log",
            str(tmp_path / "wrapper.log"),
            "--",
            sys.executable,
            "-c",
            "raise SystemExit(7)",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 7, (
        f"inner exit code not propagated (got {proc.returncode}) — a "
        "workload failure would be reported to the SF as success"
    )
