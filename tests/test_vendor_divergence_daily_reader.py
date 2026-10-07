"""alpha-engine-config-I10783's closes-when, read from the metric documents.

The issue closes when "the divergence metric is emitted on every trading day for
at least 10 consecutive trading days". `data.phase2.vendor_divergence_emitted`
counts manifest VERDICTS; nothing read the per-day MetricRecord at
`data_collection/metrics/vendor_divergence/<day>.json` the issue actually
delivers. `data_gate.exit_criteria.read_vendor_divergence_daily` does, and
`data.standing.vendor_divergence_daily` publishes it as a STANDING row.

The live-data test replays the ten real records ending 2026-10-02 (version IDs
in the fixture's `_provenance`): the streak is 0, and every unmeasurable day is
named.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from nousergon_lib.trading_calendar import subtract_trading_days  # pyright: ignore[reportAttributeAccessIssue]

from collectors import cross_source_observer as cso
from data_gate import clauses as clause_module
from data_gate import exit_criteria as xc
from data_gate.descriptors import load_units
from data_gate.read import evaluate, load_phases

from tests.data_gate_support import DeniedStore, EmptyStore, TRADING_DAY

FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "vendor_divergence"
    / "metric_records_2026-09-21_2026-10-02.json"
)
DAYS = xc.VENDOR_DIVERGENCE_DAILY_DAYS


def _record(day, status: str = "ok", *, n: int = 900, breaching: list | None = None) -> dict:
    return {
        "metric": "vendor_divergence",
        "trading_day": day.isoformat(),
        "status": status,
        "n": 0 if status == "unmeasurable" else n,
        "reason": "no pair" if status == "unmeasurable" else None,
        "breaching_symbols": breaching or [],
    }


def _store(entries_newest_first: list, *, end=TRADING_DAY) -> EmptyStore:
    """``entries`` newest first: a status string, a full dict, or None (absent)."""
    objects: dict[str, bytes] = {}
    day = end
    for entry in entries_newest_first:
        if entry is not None:
            doc = entry if isinstance(entry, dict) else _record(day, entry)
            objects[xc.VENDOR_DIVERGENCE_METRIC_KEY.format(day=day.isoformat())] = json.dumps(
                doc
            ).encode()
        day = subtract_trading_days(day, 1)
    return EmptyStore(objects)


def _read(store, *, end=TRADING_DAY):
    return xc.read_vendor_divergence_daily(store, trading_day=end, days=DAYS)


def test_the_key_is_the_producers_key():
    """A rename on either side is a red test, never a reader that reads nothing."""
    assert "data_collection/" + xc.VENDOR_DIVERGENCE_METRIC_KEY.format(day="2026-10-02") == (
        cso.vendor_divergence_key("2026-10-02")
    )
    assert xc._VENDOR_MEASURED == cso._MEASURED_STATUSES


def test_ten_measured_days_meet_the_closes_when():
    reading = _read(_store(["ok"] * DAYS))
    assert reading.met, reading.detail
    assert f"{DAYS} consecutive trading day(s)" in reading.detail
    assert reading.window.live and reading.window.complete and not reading.window.failures


def test_nine_measured_days_do_not():
    reading = _read(_store(["ok"] * (DAYS - 1)))
    assert not reading.met
    assert reading.detail.startswith(f"{DAYS - 1} consecutive")
    assert "ABSENT" in reading.detail


def test_a_named_breach_counts_as_emitted():
    """The closes-when asks for EMISSION; a breach stated is a finding, not a gap."""
    named = _record(TRADING_DAY, "breach", breaching=[{"ticker": "XYZ", "diff_bps": 80.0}])
    reading = _read(_store([named] + ["ok"] * (DAYS - 1)))
    assert reading.met, reading.detail


def test_a_breach_naming_no_symbol_does_not_count():
    unnamed = _record(TRADING_DAY, "breach")
    reading = _read(_store([unnamed] + ["ok"] * (DAYS - 1)))
    assert not reading.met
    assert "naming no symbol" in reading.detail


def test_an_unmeasurable_day_breaks_the_streak_and_is_named():
    entries = ["ok"] * DAYS
    entries[3] = "unmeasurable"
    reading = _read(_store(entries))
    assert not reading.met
    assert reading.detail.startswith("3 consecutive")
    bad_day = subtract_trading_days(TRADING_DAY, 3).isoformat()
    assert f"{bad_day}: unmeasurable (no pair)" in reading.detail
    assert reading.window.failures == (f"{bad_day}: unmeasurable (no pair)",)


def test_a_record_for_another_day_does_not_count():
    """A record under day D's key that measures day E is not D's measurement."""
    wrong = _record(subtract_trading_days(TRADING_DAY, 5))
    reading = _read(_store([wrong] + ["ok"] * (DAYS - 1)))
    assert not reading.met
    assert "names trading_day" in reading.detail


def test_the_newest_day_may_be_pending_never_an_older_one():
    """Day T's record is written by T+1's morning run, so T itself absent is
    PENDING and the window shifts back one day; an absent older day breaks it."""
    reading = _read(_store([None] + ["ok"] * DAYS))
    assert reading.met, reading.detail
    assert f"{TRADING_DAY.isoformat()} PENDING" in reading.detail

    reading = _read(_store(["ok", None] + ["ok"] * (DAYS - 2)))
    assert not reading.met
    assert "ABSENT" in reading.detail


def test_nothing_measured_is_not_live_and_never_met():
    reading = _read(EmptyStore())
    assert not reading.met and not reading.unmeasurable
    assert not reading.window.live
    assert "every record there is absent or unmeasurable" in reading.window.not_live


def test_a_denied_store_is_unmeasurable():
    reading = _read(DeniedStore())
    assert reading.unmeasurable and not reading.met


def test_the_real_records_ending_2026_10_02():
    """The ten real records as of 2026-10-05: streak 0, eight blind days named."""
    fixture = json.loads(FIXTURE.read_text())
    store = EmptyStore(
        {
            xc.VENDOR_DIVERGENCE_METRIC_KEY.format(day=day): json.dumps(doc).encode()
            for day, doc in fixture["records"].items()
        }
    )
    import datetime as dt

    reading = _read(store, end=dt.date(2026, 10, 2))
    assert not reading.met
    assert reading.detail.startswith("0 consecutive trading day(s)")
    assert "window 2026-09-21..2026-10-02" in reading.detail
    # 09-21 and 09-22 are measured (ok, n=2); the eight days after are blind.
    assert reading.window.live
    assert reading.window.observed == DAYS
    assert len(reading.window.failures) == 8
    assert reading.window.failures[0].startswith("2026-09-23: unmeasurable")
    assert reading.window.failures[-1].startswith("2026-10-02: unmeasurable")


@pytest.fixture(scope="module")
def board():
    return clause_module.generate(EmptyStore(), load_units(), load_phases(), trading_day=TRADING_DAY)


def test_the_row_is_on_the_board_standing_and_gates_nothing(board):
    clause = next(c for c in board if c.name == "data.standing.vendor_divergence_daily")
    assert clause_module.is_standing(clause)
    assert clause.ruling == clause_module.VENDOR_DIVERGENCE_DAILY_STANDING
    assert not clause.met and not clause.unmeasurable
    for gate in ("data-phase1", "data-phase2", "data-phase3"):
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        assert "data.standing.vendor_divergence_daily" not in {c.name for c in result.clauses}
