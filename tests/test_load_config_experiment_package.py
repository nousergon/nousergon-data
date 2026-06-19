"""Experiment-package precedence for weekly_collector.load_config (config#1042)."""

import os
from pathlib import Path

import yaml

import weekly_collector


def _write(p: Path, data: dict) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(data))


def test_experiment_package_wins_over_legacy(tmp_path, monkeypatch):
    """experiments/$EXP/data/config.yaml resolves ahead of legacy data/config.yaml."""
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "data" / "config.yaml", {"source": "legacy"})
    _write(cfg_repo / "experiments" / "reference" / "data" / "config.yaml", {"source": "package"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ALPHA_ENGINE_EXPERIMENT_ID", raising=False)

    assert weekly_collector.load_config()["source"] == "package"


def test_experiment_id_selects_slot(tmp_path, monkeypatch):
    """A non-default ALPHA_ENGINE_EXPERIMENT_ID selects its own package slot."""
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "experiments" / "reference" / "data" / "config.yaml", {"source": "reference"})
    _write(cfg_repo / "experiments" / "myexp" / "data" / "config.yaml", {"source": "myexp"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.setenv("ALPHA_ENGINE_EXPERIMENT_ID", "myexp")

    assert weekly_collector.load_config()["source"] == "myexp"


def test_falls_back_to_legacy_when_no_package(tmp_path, monkeypatch):
    """With no experiment-package file, the legacy data/config.yaml still resolves."""
    home = tmp_path / "home"
    cfg_repo = home / "alpha-engine-config"
    _write(cfg_repo / "data" / "config.yaml", {"source": "legacy"})
    monkeypatch.setattr(Path, "home", staticmethod(lambda: home))
    monkeypatch.delenv("ALPHA_ENGINE_EXPERIMENT_ID", raising=False)

    assert weekly_collector.load_config()["source"] == "legacy"
