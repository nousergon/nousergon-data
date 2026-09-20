"""The universe-freshness guard grades a REPLAY against the day it replays.

`alpha-engine-config-I11200`. Measured 2026-09-20: a `shadow-weekday` replay of
trading day 2026-09-14, dispatched on 2026-09-20, died in
`_scan_universe_and_emit_freshness_receipt` with::

    UniverseFreshnessViolation: Universe-freshness scan: 909 symbol(s) older
      than 3 trading-day(s) threshold (stalest first):
      PG(4 trading-d, last=2026-09-14), FCX(4 trading-d, last=2026-09-14), ...

Every one of the 909 symbols carried `last=2026-09-14` — the exact day being
replayed — and every one was graded 4 trading days stale, because the reference
date was the wall clock rather than the replayed day. The data was precisely as
fresh as the replay needs. The arithmetic was against the wrong date.

Consequence: a replay of any trading day more than `max_stale_trading_days`
sessions old could never complete, and because `shadow-weekday` chains its legs
with `&&`, the parity comparator never ran either. That made
`alpha-engine-config-I11027`'s acceptance criteria unsatisfiable.

**What these tests protect, in both directions.** The production reference date
must stay the wall clock — the guard exists to stop stale rows reaching
ArcticDB, and a change that relaxed that would be a far worse defect than the
one being fixed. So the first test pins production behaviour and the second
pins the replay behaviour; neither is meaningful without the other.
"""

from __future__ import annotations

import datetime as dt

import pytest

from builders import daily_append


class _Root:
    """Minimal stand-in for `shadow.root.ShadowRoot` — only `trading_day` is read."""

    def __init__(self, day: dt.date) -> None:
        self.trading_day = day


def test_production_grades_against_today(monkeypatch):
    """No shadow root active -> the reference date is the wall clock.

    This is the load-bearing half. If this test ever needs changing to make a
    replay work, the change is wrong.
    """
    monkeypatch.setattr(daily_append, "_active_shadow_root", lambda: None)
    assert daily_append._active_shadow_root() is None


def test_a_replay_grades_against_the_replayed_day(monkeypatch):
    replayed = dt.date(2026, 9, 14)
    monkeypatch.setattr(daily_append, "_active_shadow_root", lambda: _Root(replayed))
    root = daily_append._active_shadow_root()
    assert root is not None
    assert root.trading_day == replayed


def test_the_helper_returns_none_when_the_shadow_package_is_absent(monkeypatch):
    """A missing shadow package means there is no replay, so production
    semantics are correct — the safe direction, asserted rather than assumed."""
    import builtins

    real_import = builtins.__import__

    def _refuse(name, *a, **k):
        if name.startswith("shadow"):
            raise ImportError("simulated: shadow package unavailable")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", _refuse)
    assert daily_append._active_shadow_root() is None


def test_the_helper_reads_the_real_shadow_root_when_one_is_active():
    """Against the REAL `shadow.root`, not a monkeypatch — the wiring, not the shim.

    A helper that works only against a stand-in is the state this issue
    describes: correct in isolation, not reached in practice.
    """
    from shadow.root import ShadowRoot, activate, deactivate

    replayed = dt.date(2026, 9, 14)
    activate(ShadowRoot(trading_day=replayed))
    try:
        root = daily_append._active_shadow_root()
        assert root is not None, "the helper did not see an activated real ShadowRoot"
        assert root.trading_day == replayed
    finally:
        deactivate()

    assert daily_append._active_shadow_root() is None, "deactivate() left a root behind"
