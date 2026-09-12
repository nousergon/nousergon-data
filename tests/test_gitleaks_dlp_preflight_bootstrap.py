"""Every spot substrate that installs krepis and can make an LLM call must
install the gitleaks BINARY and run the DLP preflight gate before the
workload starts (alpha-engine-config-I10370).

## Why this exists

``krepis.session_dlp`` (the LLMClient DLP hook) shells out to the gitleaks
binary on every LLM call and fails CLOSED when it is absent. Once
``krepis-PR211`` ships the ruleset as package data, the binary becomes the
only missing half — and nothing in ``infrastructure/spot_data_*.sh`` or
``_spot_common.sh`` installed it, so every flow-doctor-diagnosing spot this
repo boots failed closed on its first LLM call.

## What is asserted, and why grep-shaped rather than executed

These scripts run remotely over SSM on an EC2 spot — there is nothing to
import or execute locally. The contract is therefore structural: each
covered bootstrap PATH must contain (a) a pinned gitleaks version + sha256
verified against the GitHub release tarball, and (b) a
``python -m krepis.session_dlp preflight`` call whose failure aborts the
script (``exit 1`` reachable from the same conditional), before the
workload/deps step that would otherwise be the first thing to discover a
missing scanner.

Four covered substrates, three shapes:

- ``spot_data_phase1.sh``, ``spot_morning_enrich.sh``, ``spot_rag_ingestion.sh``
  all source ``_spot_common.sh`` and call its ``install_gitleaks_dlp()``
  after ``install_deps``.
- ``spot_data_weekly.sh`` does NOT source ``_spot_common.sh`` (see its own
  header) and carries an inline copy of the same block in its own
  ``run_ssm`` dispatch, directly after its ``deps`` step.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_INFRA = _REPO_ROOT / "infrastructure"

_SPOT_COMMON = _INFRA / "_spot_common.sh"
_CALLERS_VIA_SPOT_COMMON = (
    "spot_data_phase1.sh",
    "spot_morning_enrich.sh",
    "spot_rag_ingestion.sh",
)
_STANDALONE_CALLER = "spot_data_weekly.sh"

#: A pinned gitleaks version whose downloaded tarball is verified against a
#: fixed sha256 before being trusted — not merely "some version is present".
_PINNED_INSTALL_RE = re.compile(
    r'GITLEAKS_VERSION=8\.30\.1\b.*?'
    r'GITLEAKS_SHA256=[0-9a-f]{64}\b.*?'
    r"echo\s+\"\$\{GITLEAKS_SHA256\}\s+/tmp/gitleaks\.tar\.gz\"\s*\|\s*sha256sum\s+-c\s+-",
    re.S,
)

#: The boot-gate: a preflight call whose failure aborts the script.
_PREFLIGHT_GATE_RE = re.compile(
    r"if\s+!\s+\"\$PYTHON_BIN\"\s+-m\s+krepis\.session_dlp\s+preflight\s*;\s*then"
    r"(?:(?!\bfi\b).)*?exit\s+1",
    re.S,
)


def _text(path: Path) -> str:
    assert path.is_file(), f"expected bootstrap file at {path}"
    return path.read_text(encoding="utf-8")


# ── The shared function in _spot_common.sh ───────────────────────────────────


@pytest.fixture(scope="module")
def spot_common_text() -> str:
    return _text(_SPOT_COMMON)


def test_spot_common_defines_install_gitleaks_dlp(spot_common_text: str):
    m = re.search(
        r"\ninstall_gitleaks_dlp\(\)\s*\{(.*?)\n\}", spot_common_text, re.S
    )
    assert m, "_spot_common.sh must define install_gitleaks_dlp()"
    body = m.group(1)
    assert _PINNED_INSTALL_RE.search(body), (
        "install_gitleaks_dlp() must install a PINNED gitleaks version "
        "verified against a sha256 checksum, not an unpinned/unverified curl"
    )
    assert _PREFLIGHT_GATE_RE.search(body), (
        "install_gitleaks_dlp() must run `python -m krepis.session_dlp "
        "preflight` as a boot gate whose failure aborts (exit 1)"
    )
    assert "run_ssm" in body, (
        "install_gitleaks_dlp() must dispatch its script remotely via "
        "run_ssm() — this runs on the spot, not on the caller's box"
    )


def test_spot_common_gitleaks_install_is_fail_closed_after_install(
    spot_common_text: str,
):
    m = re.search(r"\ninstall_gitleaks_dlp\(\)\s*\{(.*?)\n\}", spot_common_text, re.S)
    assert m
    body = m.group(1)
    assert re.search(
        r"command -v gitleaks >/dev/null 2>&1 \|\| \{[^}]*exit 1", body
    ), "the post-install gitleaks check must abort (exit 1) if the binary is still absent"


def test_spot_common_python_resolution_has_no_silent_fallback(spot_common_text: str):
    """The fleet-wide forbidden shape: probing python3.12 and silently
    falling through to a bare python3 (nousergon-data#1294/#1296 class).
    install_gitleaks_dlp() must assert python3.12 strictly.
    """
    m = re.search(r"\ninstall_gitleaks_dlp\(\)\s*\{(.*?)\n\}", spot_common_text, re.S)
    assert m
    body = m.group(1)
    silent_fallback = re.compile(
        r"command\s+-v\s+python3\.\d+[^\n]*\|\|\s*\w*(?:PY|PYTHON)\w*\s*=\s*python3\b",
        re.I,
    )
    assert not silent_fallback.search(body), (
        "install_gitleaks_dlp() must not silently fall back from python3.12 "
        "to python3 — assert strictly and exit non-zero instead"
    )


# ── The three callers that source _spot_common.sh ────────────────────────────


@pytest.mark.parametrize("filename", _CALLERS_VIA_SPOT_COMMON)
def test_caller_invokes_install_gitleaks_dlp_after_install_deps(filename: str):
    text = _text(_INFRA / filename)
    m = re.search(r"(?m)^install_deps\s*$\n^install_gitleaks_dlp\s*$", text)
    assert m, (
        f"{filename} must call install_gitleaks_dlp() immediately after "
        "install_deps — the DLP preflight gate must run before any workload "
        "step that would make an LLM call"
    )


# ── The standalone launcher (does not source _spot_common.sh) ───────────────


@pytest.fixture(scope="module")
def weekly_text() -> str:
    return _text(_INFRA / _STANDALONE_CALLER)


def test_weekly_does_not_source_spot_common(weekly_text: str):
    """Documents the reason this file needs its own inline copy rather than
    calling install_gitleaks_dlp() — if this ever becomes false, the inline
    copy below should be replaced by the shared function instead."""
    executable_lines = "\n".join(
        line for line in weekly_text.splitlines() if not line.lstrip().startswith("#")
    )
    assert not re.search(r"\bsource\b[^\n]*_spot_common\.sh", executable_lines), (
        f"{_STANDALONE_CALLER} now sources _spot_common.sh — replace its "
        "inline gitleaks-dlp block with a call to install_gitleaks_dlp()"
    )


def test_weekly_installs_pinned_gitleaks_after_deps(weekly_text: str):
    deps_idx = weekly_text.index('run_ssm "deps" 900 <<DEPS')
    gitleaks_idx = weekly_text.index('run_ssm "gitleaks-dlp" 300')
    assert gitleaks_idx > deps_idx, (
        "the gitleaks-dlp run_ssm step must come after the deps step in "
        f"{_STANDALONE_CALLER}"
    )
    block = weekly_text[gitleaks_idx:]
    assert _PINNED_INSTALL_RE.search(block), (
        f"{_STANDALONE_CALLER} must install a PINNED gitleaks version "
        "verified against a sha256 checksum"
    )
    assert _PREFLIGHT_GATE_RE.search(block), (
        f"{_STANDALONE_CALLER} must run `python -m krepis.session_dlp "
        "preflight` as a boot gate whose failure aborts (exit 1)"
    )


def test_weekly_gitleaks_pin_matches_spot_common(
    weekly_text: str, spot_common_text: str
):
    """The pin must move in lockstep between the shared function and this
    standalone copy — a bump to one without the other reintroduces drift."""
    weekly_pin = re.search(
        r"GITLEAKS_VERSION=(\S+)\nGITLEAKS_SHA256=(\S+)", weekly_text
    )
    common_pin = re.search(
        r"GITLEAKS_VERSION=(\S+)\nGITLEAKS_SHA256=(\S+)", spot_common_text
    )
    assert weekly_pin and common_pin
    assert weekly_pin.groups() == common_pin.groups(), (
        "gitleaks version/sha256 pin differs between spot_data_weekly.sh and "
        "_spot_common.sh's install_gitleaks_dlp() — bump both together"
    )
