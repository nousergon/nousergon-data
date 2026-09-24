"""alpha-engine-config-I11447: shadow ArcticDB libraries are pruned past the
retention window, never a live library, and never without --apply."""

from __future__ import annotations

import datetime as dt

import pytest

from shadow.retention import prune, select_expired
from shadow.root import LIVE_ARCTIC_LIBRARIES, ShadowGuardViolation

TODAY = dt.date(2026, 9, 24)

NAMES = [
    "universe", "macro", "delisted_history", "universe_schema_meta",
    "shadow_20260914_universe", "shadow_20260914_seed_manifest",
    "shadow_20260916_macro",
    "shadow_20260917_universe",  # exactly keep_days before TODAY: kept
    "shadow_20260922_universe",
    "shadow_2026091_universe",   # malformed stamp
    "shadow_20261399_universe",  # unparseable date
    "scratch_universe",
]


class _FakeArctic:
    def __init__(self, names):
        self.names = list(names)
        self.deleted = []

    def list_libraries(self):
        return list(self.names)

    def delete_library(self, name):
        self.deleted.append(name)
        self.names.remove(name)


def test_selects_only_shadow_stamps_older_than_the_window():
    assert select_expired(NAMES, today=TODAY) == [
        "shadow_20260914_seed_manifest", "shadow_20260914_universe", "shadow_20260916_macro",
    ]


def test_never_selects_a_live_library():
    assert not set(select_expired(NAMES, today=TODAY)) & LIVE_ARCTIC_LIBRARIES


def test_a_live_name_that_matches_the_pattern_raises(monkeypatch):
    import shadow.retention as r

    monkeypatch.setattr(r, "LIVE_ARCTIC_LIBRARIES", frozenset({"shadow_20260901_universe"}))
    with pytest.raises(ShadowGuardViolation):
        r.select_expired(["shadow_20260901_universe"], today=TODAY)


def test_keep_days_must_be_positive():
    with pytest.raises(ValueError):
        select_expired(NAMES, today=TODAY, keep_days=0)


def test_prune_is_a_dry_run_by_default():
    arctic = _FakeArctic(NAMES)
    report = prune(arctic, today=TODAY)
    assert arctic.deleted == []
    assert report["apply"] is False
    assert report["expired_stamps"] == ["20260914", "20260916"]
    assert report["deleted"] == []


def test_prune_apply_deletes_exactly_the_expired_set():
    arctic = _FakeArctic(NAMES)
    report = prune(arctic, today=TODAY, apply=True)
    assert arctic.deleted == report["expired"] == report["deleted"]
    assert set(LIVE_ARCTIC_LIBRARIES) <= set(arctic.names)
    assert "shadow_20260917_universe" in arctic.names


def test_cli_defaults_to_dry_run():
    from shadow.__main__ import _parser

    args = _parser().parse_args(["prune"])
    assert args.apply is False and args.keep_days == 7
