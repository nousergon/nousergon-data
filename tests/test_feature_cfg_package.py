"""config#1039 — FEATURE_CFG windows load from the experiment package.

Pins: (a) baseline values stay the validated set (spot-pins); (b) per-key
merge semantics; (c) unknown override keys fail loud.
"""
import pytest

from features import feature_engineer as fe


def test_baseline_spot_pins():
    b = fe._BASELINE_FEATURE_CFG
    assert b["rsi_period"] == 14
    assert b["ma_short"] == 50 and b["ma_long"] == 200
    assert b["resid_mom_window"] == 252 and b["resid_mom_skip"] == 21
    assert len(b) == 31


def test_active_cfg_is_baseline_plus_overrides():
    overrides = fe._load_feature_cfg_overrides()
    assert fe.FEATURE_CFG == {**fe._BASELINE_FEATURE_CFG, **overrides}


def test_unknown_override_key_fails_loud(tmp_path, monkeypatch):
    pkg = tmp_path / "alpha-engine-config" / "experiments" / "reference" / "data"
    pkg.mkdir(parents=True)
    (pkg / "config.yaml").write_text("feature_cfg:\n  rsi_perod: 9\n")
    monkeypatch.setenv("ALPHA_ENGINE_EXPERIMENT_ID", "reference")
    # point the home root at tmp_path
    monkeypatch.setattr("pathlib.Path.home", staticmethod(lambda: tmp_path))
    with pytest.raises(KeyError, match="rsi_perod"):
        fe._load_feature_cfg_overrides()


def test_valid_override_merges(tmp_path, monkeypatch):
    pkg = tmp_path / "alpha-engine-config" / "experiments" / "reference" / "data"
    pkg.mkdir(parents=True)
    (pkg / "config.yaml").write_text("feature_cfg:\n  rsi_period: 9\n")
    monkeypatch.setenv("ALPHA_ENGINE_EXPERIMENT_ID", "reference")
    monkeypatch.setattr("pathlib.Path.home", staticmethod(lambda: tmp_path))
    overrides = fe._load_feature_cfg_overrides()
    assert overrides == {"rsi_period": 9}
