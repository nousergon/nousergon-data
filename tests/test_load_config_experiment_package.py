"""Experiment-package precedence for weekly_collector.load_config (config#1042)."""

import os
from pathlib import Path

import yaml

import weekly_collector


def _write(p: Path, data: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(data))


def _isolate_repo_root(tmp_path, monkeypatch):
    """Neutralize nousergon_lib.config's SECOND config root
    (``<repo_root>/../alpha-engine-config`` — resolve_experiment_config's
    ``repo_root=Path(__file__).parent`` inside ``weekly_collector.load_config``)
    so these tests are hermetic regardless of the machine's directory layout.

    Root cause (found running this suite for nousergon-data-PR<fallback-groom>):
    on Brian's laptop ``~/Development`` holds every fleet repo as siblings by
    convention (this repo's own CLAUDE.md: "LOCAL DIRS are still
    ~/Development/alpha-engine-*"), so ``<nousergon-data>/../alpha-engine-config``
    is a REAL, populated sibling checkout — NOT an absent path a test can rely
    on missing. Only ``Path.home()`` was ever monkeypatched here; the real
    sibling repo's ``experiments/reference/data/config.yaml`` silently won
    resolution ahead of ``test_falls_back_to_legacy_when_no_package``'s own
    fixture (which deliberately omits an experiment-package file to exercise
    the legacy fallback), so the test read live production config instead of
    its fixture and failed with ``KeyError: 'source'`` — reproducible on ANY
    machine with a sibling alpha-engine-config clone, CI-green only because
    GitHub Actions checks out just this one repo. Fixing at the true root
    (isolate repo_root itself, not the specific file that happened to exist)
    rather than patching around this one collision, so no other candidate in
    the 5-deep search order (see nousergon_lib.config._candidate_paths) can
    ever leak real sibling-repo content into any of these three tests again.
    """
    fake_module_dir = tmp_path / "isolated_repo" / "nousergon-data"
    fake_module_dir.mkdir(parents=True)
    monkeypatch.setattr(weekly_collector, "__file__", str(fake_module_dir / "weekly_collector.py"))


def test_experiment_package_wins_over_legacy(tmp_path, monkeypatch):
    """experiments/$EXP/data/config.yaml resolves ahead of legacy data/config.yaml."""
    _isolate_repo_root(tmp_path, monkeypatch)
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "data" / "config.yaml", {"source": "legacy"})
    _write(cfg_repo / "experiments" / "reference" / "data" / "config.yaml", {"source": "package"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ALPHA_ENGINE_EXPERIMENT_ID", raising=False)

    assert weekly_collector.load_config()["source"] == "package"


def test_experiment_id_selects_slot(tmp_path, monkeypatch):
    """A non-default ALPHA_ENGINE_EXPERIMENT_ID selects its own package slot."""
    _isolate_repo_root(tmp_path, monkeypatch)
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "experiments" / "reference" / "data" / "config.yaml", {"source": "reference"})
    _write(cfg_repo / "experiments" / "myexp" / "data" / "config.yaml", {"source": "myexp"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.setenv("ALPHA_ENGINE_EXPERIMENT_ID", "myexp")

    assert weekly_collector.load_config()["source"] == "myexp"


def test_falls_back_to_legacy_when_no_package(tmp_path, monkeypatch):
    """With no experiment-package file, the legacy data/config.yaml still resolves."""
    _isolate_repo_root(tmp_path, monkeypatch)
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "data" / "config.yaml", {"source": "legacy"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ALPHA_ENGINE_EXPERIMENT_ID", raising=False)

    assert weekly_collector.load_config()["source"] == "legacy"
