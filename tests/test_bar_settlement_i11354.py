"""`dates.bar_settlement` — the settlement clock, and the D03/D19 wiring.

alpha-engine-config-I11354. `assert_no_bar_after` bounds a published series on
the DATE axis; nothing bounded it on the CLOCK, so a run for D that fetched D's
own bar six minutes after the 16:00 ET close published an unsettled number and
passed every guard. Measured 2026-09-21 across 920/920 price-cache files:
`Volume` short by a median 20.5 % (max 62.3 %, never high) and `Close` off by a
median 2.7 bps on 442 of them, against a refetch at 18:41 ET.

These tests pin the grader's three branches (before D, on D, after D), its
boundary at :data:`dates.SETTLED_AFTER_ET`, its DST behaviour, and the fact that
both collectors put the reading on their result dict in the shape
`weekly_collector._record_collector_guards` folds onto a run manifest.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import dates

ET = ZoneInfo("America/New_York")


def _et(y, m, d, hh, mm, ss=0) -> datetime:
    """An ET wall-clock moment, as the UTC instant a collector would record."""
    return datetime(y, m, d, hh, mm, ss, tzinfo=ET).astimezone(timezone.utc)


# ── the constant itself ──────────────────────────────────────────────────────


def test_settled_after_et_is_the_declared_threshold():
    """The threshold is a declared constant, not a literal buried in a branch.

    Pinned so a silent edit is a test failure with the issue number attached:
    the value rests on a ONE-day bracket (16:06 ET unsettled, 18:41 ET settled
    on 2026-09-21) and the 3-day sample that would measure it properly is
    tracked separately. Moving it needs that evidence, not a judgement call.
    """
    assert dates.SETTLED_AFTER_ET == "18:15"
    assert dates.BAR_SETTLED == "settled"
    assert dates.BAR_PROVISIONAL == "provisional"


# ── the grader: both branches and the boundary ───────────────────────────────


def test_fetch_at_1606_et_on_the_trading_day_is_provisional():
    """The measured v1 postclose case — six minutes after the close."""
    assert dates.bar_settlement(_et(2026, 9, 21, 16, 6), "2026-09-21") == "provisional"


def test_fetch_at_1645_et_the_standalone_postclose_time_is_provisional():
    """16:45 ET was `data-collection-eod`'s time — 39 minutes later than v1 and
    still ~1.5 h short of settlement, so the standalone collector inherited the
    defect. That schedule has since moved to 18:15 ET (the constant below), but
    the reading at 16:45 stays pinned here: it is the evidence the move rests on,
    and a grader that stopped calling 16:45 provisional would un-justify it.""" 
    assert dates.bar_settlement(_et(2026, 9, 21, 16, 45), "2026-09-21") == "provisional"


def test_fetch_at_1841_et_the_measured_settled_time_is_settled():
    """The shadow fetch. Verified settled on `Close` against a third fetch at
    22:36 ET: 30/30 sampled tickers identical to the last printed digit."""
    assert dates.bar_settlement(_et(2026, 9, 21, 18, 41), "2026-09-21") == "settled"


@pytest.mark.parametrize(
    ("hh", "mm", "ss", "expected"),
    [
        (18, 14, 59, "provisional"),   # one second before
        (18, 15, 0, "settled"),        # exactly at — inclusive
        (18, 15, 1, "settled"),        # one second after
    ],
)
def test_the_boundary_is_inclusive_to_the_second(hh, mm, ss, expected):
    """18:15:00 ET exactly is SETTLED. An exclusive boundary would make the
    verdict depend on sub-second scheduler jitter on the one run that matters."""
    assert dates.bar_settlement(_et(2026, 9, 21, hh, mm, ss), "2026-09-21") == expected


def test_a_fetch_before_the_session_closed_is_provisional():
    """Pre-close, mid-session, and pre-open all grade provisional — the bar does
    not exist yet, which is strictly worse than unsettled, never better."""
    for hh, mm in ((7, 30), (9, 29), (12, 0), (15, 59)):
        assert dates.bar_settlement(_et(2026, 9, 21, hh, mm), "2026-09-21") == "provisional"


def test_a_fetch_on_a_later_calendar_day_is_settled_at_any_hour():
    """The backfill / rerun case: a run for D executed on D+1 at 00:05 ET reads
    a fully settled D, and the 18:15 threshold must not refuse it."""
    assert dates.bar_settlement(_et(2026, 9, 22, 0, 5), "2026-09-21") == "settled"
    assert dates.bar_settlement(_et(2026, 9, 22, 7, 30), "2026-09-21") == "settled"
    assert dates.bar_settlement(_et(2027, 1, 4, 9, 0), "2026-09-21") == "settled"


def test_a_fetch_on_an_earlier_calendar_day_is_provisional():
    """A run for D that somehow fetched on D-1 cannot have D's bar at all."""
    assert dates.bar_settlement(_et(2026, 9, 18, 19, 0), "2026-09-21") == "provisional"


# ── timezone handling ────────────────────────────────────────────────────────


def test_the_threshold_is_an_exchange_clock_not_utc():
    """22:15 UTC is 18:15 EDT in September (settled) and 17:15 EST in January
    (provisional). A UTC constant would silently drift by an hour twice a year
    and would read `settled` for a full hour of unsettled fetches each winter."""
    sept = datetime(2026, 9, 21, 22, 15, tzinfo=timezone.utc)
    assert dates.bar_settlement(sept, "2026-09-21") == "settled"

    jan = datetime(2027, 1, 4, 22, 15, tzinfo=timezone.utc)
    assert dates.bar_settlement(jan, "2027-01-04") == "provisional"
    assert dates.bar_settlement(
        datetime(2027, 1, 4, 23, 15, tzinfo=timezone.utc), "2027-01-04"
    ) == "settled"


def test_a_naive_datetime_is_read_as_utc():
    """Consistent with the rest of `dates.py`. 22:41Z naive == 18:41 EDT."""
    assert dates.bar_settlement(datetime(2026, 9, 21, 22, 41), "2026-09-21") == "settled"
    assert dates.bar_settlement(datetime(2026, 9, 21, 20, 6), "2026-09-21") == "provisional"


@pytest.mark.parametrize(
    "raw",
    ["2026-09-21T22:41:00Z", "2026-09-21T22:41:00+00:00", "2026-09-21T18:41:00-04:00"],
)
def test_iso_strings_are_accepted_including_a_trailing_z(raw):
    """Manifest timestamps arrive as ISO strings; `Z` is the shape S3 and the
    run-manifest writer both emit, and `fromisoformat` rejects it on <3.11."""
    assert dates.bar_settlement(raw, "2026-09-21") == "settled"


def test_the_trading_day_accepts_every_shape_as_trading_day_does():
    moment = _et(2026, 9, 21, 18, 41)
    assert dates.bar_settlement(moment, "2026-09-21") == "settled"
    assert dates.bar_settlement(moment, date(2026, 9, 21)) == "settled"
    assert dates.bar_settlement(moment, datetime(2026, 9, 21, 16, 0)) == "settled"


def test_an_unparseable_fetch_time_raises_rather_than_defaulting():
    """This is a producer repo: a run that cannot say WHEN it fetched must not
    be graded at all. Defaulting either way fabricates a reading."""
    with pytest.raises(ValueError):
        dates.bar_settlement(None, "2026-09-21")
    with pytest.raises(ValueError):
        dates.bar_settlement(1758499200, "2026-09-21")
    with pytest.raises(ValueError):
        dates.bar_settlement("not-a-timestamp", "2026-09-21")


# ── the guard entry ──────────────────────────────────────────────────────────


def test_the_guard_ships_in_observe_mode_with_a_promotion_criterion():
    """`sf-pipeline-policy.md` §7a. Enforcing when this shipped would have
    refused every write the then-16:45 ET schedule made — i.e. halted EOD
    collection instead of measuring it. The schedule has since moved to the
    settlement hour, which is the FIRST of the three promotion conditions; the
    other two (10 clean cycles, and I11356's 3-day sample) are still open, so
    the guard stays in observe."""
    guard = dates.BAR_SETTLEMENT_GUARD
    assert guard.name == "bar_settlement"
    assert guard.mode.value == "observe"
    assert not guard.enforcing
    assert guard.tracked_issue == "alpha-engine-config-I11354"
    assert "18:15" in guard.promotion_criterion


def test_the_guard_entry_is_shaped_for_record_collector_guards():
    """`weekly_collector._record_collector_guards` reads exactly these keys and
    passes them positionally/by name to `UnitRun.record_guard`. A missing key
    raises there, on the run, which is why the shape is pinned here."""
    entry = dates.bar_settlement_guard_entry(
        _et(2026, 9, 21, 16, 6), "2026-09-21", key="staging/daily_closes/2026-09-21.parquet",
    )
    assert set(entry) == {"guard", "mode", "verdict", "detail", "key", "value", "baseline"}
    assert entry["guard"] == "bar_settlement"
    assert entry["mode"] == "observe"
    assert entry["verdict"] == "provisional"
    assert entry["key"] == "staging/daily_closes/2026-09-21.parquet"
    assert entry["value"] == pytest.approx(16.1, abs=0.01)
    assert entry["baseline"] == pytest.approx(18.25)
    assert "16:06" in entry["detail"]


def test_the_guard_entry_value_and_baseline_are_comparable_hours():
    """The console renders the margin from `value` vs `baseline` without
    re-parsing prose — so both must be the SAME unit (ET hours as a float)."""
    settled = dates.bar_settlement_guard_entry(_et(2026, 9, 21, 18, 41), "2026-09-21")
    assert settled["verdict"] == "settled"
    assert settled["value"] > settled["baseline"]

    provisional = dates.bar_settlement_guard_entry(_et(2026, 9, 21, 16, 45), "2026-09-21")
    assert provisional["verdict"] == "provisional"
    assert provisional["value"] < provisional["baseline"]


def test_the_provisional_detail_names_the_measured_volume_shortfall():
    """A verdict with no number attached is an opinion. The detail carries the
    2026-09-21 measurement so a manifest reader needs no second artifact."""
    detail = dates.bar_settlement_guard_entry(_et(2026, 9, 21, 16, 6), "2026-09-21")["detail"]
    assert "20.5" in detail
    assert "Volume" in detail


def test_every_detail_says_volume_is_never_final_that_evening():
    """Measured: between the 18:41 ET and a 22:36 ET fetch, Volume still moved
    on 29 of 30 sampled tickers (median 0.9 %, max 13.2 %) while Close did not
    move at all. A `settled` verdict must not be read as "Volume is final"."""
    for moment in (_et(2026, 9, 21, 16, 6), _et(2026, 9, 21, 18, 41)):
        detail = dates.bar_settlement_guard_entry(moment, "2026-09-21")["detail"]
        assert "Consolidated Volume is NOT final" in detail


# ── collector wiring: D03 and D19 ────────────────────────────────────────────


def test_d03_prices_collect_records_the_verdict(monkeypatch):
    """`collectors.prices.collect` returns the reading under `guards`, which is
    the generic hook `_record_collector_guards` already folds onto D03's
    manifest — so no change to `run_units.py` or the manifest writer."""
    from collectors import prices

    monkeypatch.setattr(prices.boto3, "client", lambda *a, **k: object())
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["AAA"])
    monkeypatch.setattr(prices, "_refresh_stale", lambda *a, **k: (1, [], [("AAA", 10)]))

    result = prices.collect("bkt", ["AAA"], reference_date="2026-09-21")

    assert len(result["guards"]) == 1
    entry = result["guards"][0]
    assert entry["guard"] == "bar_settlement"
    assert entry["mode"] == "observe"
    assert entry["verdict"] in ("settled", "provisional")
    assert entry["key"].endswith("*.parquet")


def test_d03_grades_provisional_when_the_fetch_opens_before_settlement(monkeypatch):
    """Pinned against a frozen clock rather than wall time — a test whose
    verdict depends on when CI happens to run measures nothing."""
    from collectors import prices

    monkeypatch.setattr(prices.boto3, "client", lambda *a, **k: object())
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["AAA"])
    monkeypatch.setattr(prices, "_refresh_stale", lambda *a, **k: (1, [], [("AAA", 10)]))

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return _et(2026, 9, 21, 16, 6).astimezone(tz or timezone.utc)

    monkeypatch.setattr(prices, "datetime", _FrozenDatetime)

    result = prices.collect("bkt", ["AAA"], reference_date="2026-09-21")
    assert result["guards"][0]["verdict"] == "provisional"

    class _SettledDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return _et(2026, 9, 21, 18, 41).astimezone(tz or timezone.utc)

    monkeypatch.setattr(prices, "datetime", _SettledDatetime)
    result = prices.collect("bkt", ["AAA"], reference_date="2026-09-21")
    assert result["guards"][0]["verdict"] == "settled"


def test_d19_post_close_skip_grades_the_existing_object_not_this_run():
    """The skip branch is where an unsettled bar becomes PERMANENT.

    `_is_post_close_write` treats anything at or after 16:00 ET as
    authoritative, so a parquet written at 16:06 ET makes every later pass for
    D skip — including the 07:30 ET D+1 morning enrich. There is no self-heal.
    Grading that branch on THIS run's clock would return `settled` (it is the
    next morning) for an artifact that is not: the verdict must describe the
    object that stays published.
    """
    written_at = _et(2026, 9, 21, 16, 6)
    entry = dates.bar_settlement_guard_entry(
        written_at, "2026-09-21", key="staging/daily_closes/2026-09-21.parquet",
    )
    assert entry["verdict"] == "provisional"

    # ... whereas this run's own clock, the next morning, would have said:
    assert dates.bar_settlement(_et(2026, 9, 22, 7, 30), "2026-09-21") == "settled"
