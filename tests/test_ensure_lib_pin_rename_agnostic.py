"""Behavioral regression test for ``scripts/ensure_lib_pin.sh``.

Closes the rename-gap bug class (alpha-engine-config#1369): the shared lib was
renamed ``alpha-engine-lib`` -> ``nousergon-lib`` at v0.60.0 (the AGPL rebrand).
``ensure_lib_pin.sh`` originally greped ``^alpha-engine-lib`` and probed
``import nousergon_lib``, so after the rename the grep silently stopped
matching the renamed pin and the heal became a **no-op** — the trading box froze
at the last pre-rename lib (the 2026-06-29 weekday MorningEnrich crash). The
existing ``test_sf_lib_pin_self_heal_wiring.py`` only checks that the SF calls
the heal between pull and run; it does NOT exercise the script's own pin
detection, so a future rename could re-break the matching with every wiring test
still green.

This test runs the script for real against fixture ``requirements.txt`` files,
with stubbed ``python`` / ``pip`` on PATH (no real lib install required), and
asserts the rename-agnostic contract directly:

  * a renamed ``nousergon-lib@vX.Y.Z`` pin is DETECTED (not silently skipped) —
    the exact regression that broke prod;
  * the legacy ``alpha-engine-lib`` pin is still matched (back-compat across a
    not-yet-renamed repo);
  * in-sync installed==pinned -> exit 0, "in sync", pip NOT invoked (idempotent);
  * drift installed!=pinned -> pip invoked with the parsed pin spec, then exit 0;
  * a requirements file with no shared-lib pin -> skip (exit 0), unchanged.

Run: ``pytest tests/test_ensure_lib_pin_rename_agnostic.py``
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO_ROOT / "scripts" / "ensure_lib_pin.sh"

# A representative git-URL pin line, as it appears in the fleet's requirements.
_NOUSERGON_PIN = (
    "nousergon-lib[arcticdb,flow_doctor] @ "
    "git+https://github.com/nousergon/nousergon-lib.git@v0.70.0"
)
_LEGACY_PIN = (
    "alpha-engine-lib[arcticdb] @ "
    "git+https://github.com/nousergon/alpha-engine-lib.git@v0.59.4"
)


def _write_stub(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env bash\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _run(
    tmp_path: Path,
    *,
    requirements: str,
    installed_version: str,
) -> tuple[int, str, Path]:
    """Run ensure_lib_pin.sh against a fixture requirements file.

    ``python`` is stubbed to report ``installed_version`` (or ``none`` to
    simulate the lib being absent — the script's ``|| echo none`` path).
    ``pip`` is stubbed to record the spec it was asked to install and to
    "succeed" by bumping the version the next ``python`` call reports.

    Returns (returncode, combined_output, pip_log_path).
    """
    binstub = tmp_path / "bin"
    binstub.mkdir()

    req_file = tmp_path / "requirements.txt"
    req_file.write_text(requirements)

    # The stubbed python reads its reported version from a file so that the
    # pip stub can "heal" it (write a new version) and the post-install probe
    # observes the change — mirroring a real reinstall.
    version_file = tmp_path / "installed_version"
    version_file.write_text(installed_version)

    pip_log = tmp_path / "pip_invocations.log"

    _write_stub(
        binstub / "python",
        # Honour the script's exact probe contract: print the version, or fail
        # (exit non-zero) when the lib is "absent" so the script's `|| echo none`
        # branch fires.
        f'v="$(cat "{version_file}")"\n'
        'if [ "$v" = "none" ]; then exit 1; fi\n'
        'printf "%s\\n" "$v"\n',
    )
    # The script invokes `python -c ...`; the stub above ignores args and just
    # emits the recorded version, which is what every probe in the script wants.

    # The pip stub records its invocation and simulates a faithful
    # "--force-reinstall": it extracts @vX.Y.Z from its args and writes that as
    # the new installed version, so the script's post-install probe observes the
    # heal.
    _write_stub(
        binstub / "pip",
        f'printf "%s\\n" "$*" >> "{pip_log}"\n'
        'ver="$(printf "%s" "$*" | grep -oE "@v[0-9]+\\.[0-9]+\\.[0-9]+" '
        '| head -1 | sed "s/^@v//")"\n'
        f'if [ -n "$ver" ]; then printf "%s\\n" "$ver" > "{version_file}"; fi\n',
    )

    env = dict(os.environ)
    env["PATH"] = f"{binstub}{os.pathsep}{env.get('PATH', '')}"

    proc = subprocess.run(
        ["bash", str(_SCRIPT), str(tmp_path), str(req_file)],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return proc.returncode, proc.stdout + proc.stderr, pip_log


def test_script_exists_and_executable():
    assert _SCRIPT.exists(), "scripts/ensure_lib_pin.sh missing"
    assert os.stat(_SCRIPT).st_mode & stat.S_IXUSR, "must be executable"


def test_renamed_pin_is_detected_not_skipped(tmp_path):
    """The core regression: a renamed nousergon-lib pin must NOT be skipped.

    Before the fix the grep ``^alpha-engine-lib`` did not match this line, so
    the script printed 'no ... pin -- skipping' and exited 0 as a silent no-op
    while the box stayed frozen on a stale lib.
    """
    rc, out, _ = _run(
        tmp_path, requirements=_NOUSERGON_PIN + "\n", installed_version="0.70.0"
    )
    assert rc == 0, out
    assert "skipping" not in out, (
        "renamed nousergon-lib pin was silently skipped — the #1369 no-op "
        f"regression has returned:\n{out}"
    )
    assert "in sync" in out, out


def test_drift_on_renamed_pin_triggers_reinstall(tmp_path):
    """Installed != pinned on a renamed pin must heal via pip (not no-op)."""
    rc, out, pip_log = _run(
        tmp_path, requirements=_NOUSERGON_PIN + "\n", installed_version="0.59.4"
    )
    assert rc == 0, out
    assert pip_log.exists(), "pip was never invoked — heal was a no-op"
    spec = pip_log.read_text()
    assert "nousergon-lib" in spec, spec
    assert "@v0.70.0" in spec, spec
    assert "healed" in out, out


def test_lib_absent_then_reinstalled(tmp_path):
    """`none` installed (lib missing) is drift and must be healed, not skipped."""
    rc, out, pip_log = _run(
        tmp_path, requirements=_NOUSERGON_PIN + "\n", installed_version="none"
    )
    assert rc == 0, out
    assert pip_log.exists() and "nousergon-lib" in pip_log.read_text(), out
    assert "healed" in out, out


def test_legacy_nousergon_lib_pin_still_matched(tmp_path):
    """Back-compat: a not-yet-renamed repo pinning alpha-engine-lib still heals."""
    rc, out, pip_log = _run(
        tmp_path, requirements=_LEGACY_PIN + "\n", installed_version="0.50.0"
    )
    assert rc == 0, out
    assert "skipping" not in out, out
    assert pip_log.exists() and "alpha-engine-lib" in pip_log.read_text(), out


def test_no_shared_lib_pin_skips_cleanly(tmp_path):
    """A requirements file with no shared-lib pin skips (exit 0), unchanged."""
    rc, out, pip_log = _run(
        tmp_path,
        requirements="pandas==2.2.0\nnumpy==1.26.0\n",
        installed_version="0.70.0",
    )
    assert rc == 0, out
    assert "skipping" in out, out
    assert not pip_log.exists(), "pip must not run when there is no pin"


@pytest.mark.parametrize("pin_line", [_NOUSERGON_PIN, _LEGACY_PIN])
def test_in_sync_is_idempotent_no_pip(tmp_path, pin_line):
    """installed == pinned -> 'in sync', exit 0, pip NEVER invoked."""
    pinned = "0.70.0" if pin_line is _NOUSERGON_PIN else "0.59.4"
    rc, out, pip_log = _run(
        tmp_path, requirements=pin_line + "\n", installed_version=pinned
    )
    assert rc == 0, out
    assert "in sync" in out, out
    assert not pip_log.exists(), "in-sync run must not call pip (idempotent)"
