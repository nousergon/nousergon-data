"""Tests for infrastructure/systemd/check-systemd-unit-drift.py (config#2352).

Covers the installed-vs-repo systemd unit drift probe: clean match,
divergence detection, not-installed (box hosts neither pair) as non-error,
and missing repo source as a config error. No real AWS/systemd access —
purely local file comparison, so this is a plain tmp-dir fixture test
(mirrors the module-load pattern used by test_sf_definition_check_drift.py,
minus the subprocess mocking since this script never shells out to AWS).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "systemd" / "check-systemd-unit-drift.py"


@pytest.fixture()
def cd(tmp_path, monkeypatch):
    """Load the module fresh per-test, pointed at an isolated repo+installed dir pair."""
    spec = importlib.util.spec_from_file_location("check_systemd_unit_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    script_dir = tmp_path / "infrastructure" / "systemd"
    script_dir.mkdir(parents=True)
    installed_dir = tmp_path / "etc-systemd-system"
    installed_dir.mkdir()

    monkeypatch.setattr(module, "SCRIPT_DIR", script_dir)
    monkeypatch.setattr(module, "INSTALLED_DIR", installed_dir)

    return module, script_dir, installed_dir


def _write(path: Path, content: str) -> None:
    path.write_text(content)


def test_clean_when_installed_matches_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.timer", "UNIT A\n")
    _write(installed_dir / "daily-news.timer", "UNIT A\n")

    status, detail = module.check_unit("daily-news.timer")

    assert status == "clean"
    assert "OK" in detail


def test_drift_when_installed_diverges_from_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.service", "UNIT NEW\n")
    _write(installed_dir / "metron-intraday.service", "UNIT OLD (stale)\n")

    status, detail = module.check_unit("metron-intraday.service")

    assert status == "drift"
    assert "metron-intraday.service" in detail


def test_not_installed_when_box_never_had_the_unit(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.timer", "UNIT A\n")
    # No file under installed_dir — this box never installed it (e.g. the
    # dashboard box probing for metron-intraday, which only the trading box
    # hosts).

    status, detail = module.check_unit("metron-intraday.timer")

    assert status == "not-installed"


def test_source_error_when_repo_copy_missing(cd):
    module, script_dir, installed_dir = cd
    _write(installed_dir / "ghost.service", "UNIT GHOST\n")
    # No repo copy at all — a genuinely malformed/renamed source.

    status, detail = module.check_unit("ghost.service")

    assert status == "source-error"


def test_all_not_installed_when_box_hosts_neither_pair(cd):
    module, script_dir, installed_dir = cd
    for name in module.ALL_UNITS:
        _write(script_dir / name, f"UNIT {name}\n")
    # installed_dir stays empty — this box hosts none of the tracked units.

    statuses = [module.check_unit(name)[0] for name in module.ALL_UNITS]

    assert all(s == "not-installed" for s in statuses)


def test_main_reports_drift_exit_code_via_cli(cd, monkeypatch, capsys):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.service", "UNIT NEW\n")
    _write(installed_dir / "daily-news.service", "UNIT OLD\n")
    for name in module.ALL_UNITS:
        if name != "daily-news.service":
            _write(script_dir / name, f"UNIT {name}\n")

    monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
    exit_code = module.main()

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "drift" in out.lower()
