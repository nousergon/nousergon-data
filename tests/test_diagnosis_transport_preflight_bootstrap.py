"""Every spot substrate that can run flow-doctor diagnosis must prove the
diagnosis wire's client library is importable BEFORE the workload starts
(alpha-engine-config-I10880).

## Why this exists

``flow-doctor.yaml`` sets ``diagnosis.provider: router``, which resolves to
``flow_doctor.diagnosis.provider.RouterProvider``. That class imports
``openai`` lazily, inside ``_call_openai_compat_chat``, at the moment a
diagnosis actually fires — not at process start. Nothing in this repo's
dependency chain (``nousergon-lib[flow-doctor]`` -> ``krepis[flow-doctor]``
-> a bare ``flow-doctor>=0.16.0``) ever installed the ``openai`` package
krepis's own ``openai`` extra declares, so every data-spot box's FIRST real
diagnosis attempt failed closed with ``ModuleNotFoundError: No module named
'openai'`` — measured 2026-09-15, box i-0521ac62ccfa5196a, shadow-weekday
run, SSM command 426ffbd1 (nousergon-data-I1743/I1752).

``requirements.txt`` now declares the dependency directly (the pinned
``krepis`` line carries the ``[openai]`` extra), which fixes it going
forward. This test file guards the boot-time GATE that catches a future
regression the same way the sibling gitleaks/DLP gate
(``test_gitleaks_dlp_preflight_bootstrap.py``, alpha-engine-config-I10370)
guards its own dependency: fail closed at boot, not silently at the first
LLM call in production.

## What is asserted, and why grep-shaped rather than executed

These scripts run remotely over SSM on an EC2 spot (or, for the dispatcher
Lambda, are rendered as a shell string and shipped the same way) — there is
nothing to import or execute locally. The contract is therefore structural,
mirroring ``test_gitleaks_dlp_preflight_bootstrap.py``'s own rationale.

Three covered substrates:

- ``_spot_common.sh``'s ``install_gitleaks_dlp()`` — used by
  ``spot_data_phase1.sh``, ``spot_morning_enrich.sh`` and
  ``spot_rag_ingestion.sh``.
- ``spot_data_weekly.sh``'s inline copy of the same block (it does not
  source ``_spot_common.sh``).
- ``infrastructure/lambdas/data-spot-dispatcher/index.py``'s
  ``_bootstrap_command()``, which renders its OWN inline copy of the
  gitleaks/DLP gate (a third instance of the same pattern, predating this
  test) and is the ACTUAL boot path that produced the 2026-09-15 failure
  for the ``shadow-weekday`` workload.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_INFRA = _REPO_ROOT / "infrastructure"

_SPOT_COMMON = _INFRA / "_spot_common.sh"
_STANDALONE_CALLER = "spot_data_weekly.sh"
_DISPATCHER = _INFRA / "lambdas" / "data-spot-dispatcher" / "index.py"

#: A gate that runs `python -c "import openai"` (or `"$PYTHON_BIN" -c
#: "import openai"`) and aborts (non-zero exit / `|| fail(...)`) if it fails.
_BASH_GATE_RE = re.compile(
    r'if\s+!\s+"\$PYTHON_BIN"\s+-c\s+"import\s+openai"[^\n]*;\s*then'
    r"(?:(?!\bfi\b).)*?exit\s+1",
    re.S,
)
_PY_STRING_GATE_RE = re.compile(
    r'python\s+-c\s+"import\s+openai"\s+\|\|\s+fail\s+"'
)


def _text(path: Path) -> str:
    assert path.is_file(), f"expected bootstrap file at {path}"
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def spot_common_text() -> str:
    return _text(_SPOT_COMMON)


def test_spot_common_gates_the_diagnosis_transport(spot_common_text: str):
    m = re.search(r"\ninstall_gitleaks_dlp\(\)\s*\{(.*?)\n\}", spot_common_text, re.S)
    assert m, "_spot_common.sh must define install_gitleaks_dlp()"
    body = m.group(1)
    assert _BASH_GATE_RE.search(body), (
        "install_gitleaks_dlp() must run `\"$PYTHON_BIN\" -c \"import openai\"` "
        "as a boot gate whose failure aborts (exit 1) — the diagnosis wire's "
        "client library"
    )
    # The diagnosis gate must come after the DLP gate (same ordering the DLP
    # gate already imposes relative to install_deps): both are boot-time
    # proofs that a dependency an LLM call needs is actually present.
    dlp_idx = body.index("krepis.session_dlp preflight")
    diag_idx = body.index('import openai')
    assert diag_idx > dlp_idx


@pytest.fixture(scope="module")
def weekly_text() -> str:
    return _text(_INFRA / _STANDALONE_CALLER)


def test_weekly_gates_the_diagnosis_transport(weekly_text: str):
    gitleaks_idx = weekly_text.index('run_ssm "gitleaks-dlp" 300')
    block = weekly_text[gitleaks_idx:]
    assert _BASH_GATE_RE.search(block), (
        f"{_STANDALONE_CALLER} must run `\"$PYTHON_BIN\" -c \"import openai\"` "
        "as a boot gate whose failure aborts (exit 1)"
    )
    dlp_idx = block.index("krepis.session_dlp preflight")
    diag_idx = block.index('import openai')
    assert diag_idx > dlp_idx


@pytest.fixture(scope="module")
def dispatcher_text() -> str:
    return _text(_DISPATCHER)


def test_dispatcher_bootstrap_gates_the_diagnosis_transport(dispatcher_text: str):
    """The dispatcher Lambda renders its own inline copy of the gate — this
    is the actual boot path measured failing for `shadow-weekday` on
    2026-09-15 (i-0521ac62ccfa5196a, SSM command 426ffbd1)."""
    m = re.search(
        r"def _bootstrap_command\(.*?\n(    tail = f\"\"\".*?\"\"\")\n",
        dispatcher_text,
        re.S,
    )
    assert m, "_bootstrap_command() must define its `tail` f-string block"
    tail = m.group(1)
    assert _PY_STRING_GATE_RE.search(tail), (
        "_bootstrap_command()'s rendered script must run "
        '`python -c "import openai" || fail "..."` as a boot gate — the '
        "diagnosis wire's client library"
    )
    dlp_idx = tail.index("krepis.session_dlp preflight")
    diag_idx = tail.index('import openai')
    assert diag_idx > dlp_idx, (
        "the diagnosis-transport gate must come after the DLP preflight, "
        "and before the workload command runs"
    )
    collector_idx = tail.index("{collector_cmd}")
    assert diag_idx < collector_idx, (
        "the diagnosis-transport gate must run BEFORE the workload command"
    )
