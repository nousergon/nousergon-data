"""`data_gate read` grades a real trading day, on every calendar day.

`alpha-engine-config-I11191`. `_read_command` defaulted the trading day to the
bare UTC calendar date, so a weekend or holiday reading graded every unit whose
cadence is neither `scheduled` nor `on_demand` against a
`{run_manifest_prefix}/{trading_day}/` partition that cannot exist.

Measured 2026-09-20, a Sunday: the published board carried
`trading_day: 2026-09-20`, D36 daily-news read UNMET -- *"no run manifest for
this trading day, under runs/D36/2026-09-20/"* -- and its actual manifest from
that same morning was::

    trading_day   = 2026-09-18
    calendar_date = 2026-09-20
    started       = 2026-09-20T11:00:39Z
    status        = ok
    rows_out      = 273

The producer's mapping was right; the reader had none.

The two subcommands in `__main__.py` answer "which trading day is this about"
differently ON PURPOSE, and the difference is the thing worth pinning:

* `report` -- the daily accountability report, about a COMPLETED prior day, so
  strictly-before (`report.previous_trading_day`).
* `read` -- the board, published at 23:30 UTC after the close, so the day
  ITSELF when that day is a trading day (`cadence.latest_trading_day_on_or_before`).

Reusing `previous_trading_day` in `read` would have regressed every weekday
board by one day -- grading Friday's manifests against Thursday's partitions --
which is a quieter defect than the one being fixed. These tests exist so that
swap cannot be made silently.
"""

from __future__ import annotations

import datetime as dt

import pytest

from data_gate.cadence import latest_trading_day_on_or_before
from data_gate.report import previous_trading_day


@pytest.mark.parametrize(
    "calendar_date,expected,why",
    [
        ("2026-09-18", "2026-09-18", "Friday is its own trading day"),
        ("2026-09-19", "2026-09-18", "Saturday snaps back to Friday"),
        ("2026-09-20", "2026-09-18", "Sunday snaps back to Friday -- the measured defect"),
        ("2026-09-21", "2026-09-21", "Monday is its own trading day"),
        ("2026-07-03", "2026-07-02", "observed Independence Day holiday, not a weekend"),
        ("2026-07-04", "2026-07-02", "Saturday after an observed holiday skips both"),
        ("2026-12-25", "2026-12-24", "Christmas, a weekday holiday"),
    ],
)
def test_the_read_snap_resolves_every_calendar_day_to_a_trading_day(calendar_date, expected, why):
    got = latest_trading_day_on_or_before(dt.date.fromisoformat(calendar_date))
    assert got.isoformat() == expected, why


def test_a_weekday_reading_is_about_that_weekday_not_the_one_before():
    """The regression a naive `previous_trading_day` swap would introduce.

    Pinned separately from the table above because it is the failure mode of
    the OBVIOUS fix, not of the original defect.
    """
    friday = dt.date(2026, 9, 18)
    assert latest_trading_day_on_or_before(friday) == friday
    assert previous_trading_day(friday) == dt.date(2026, 9, 17), (
        "report.previous_trading_day is strictly-before by design; if that changed, the two "
        "subcommands' semantics need re-deciding together rather than one of them being edited"
    )
    assert latest_trading_day_on_or_before(friday) != previous_trading_day(friday), (
        "read and report must NOT resolve identically on a trading day -- read is about the "
        "day itself, report is about the completed prior day"
    )


def test_the_read_command_uses_the_snap_and_not_the_bare_calendar_date():
    """Grades the wiring, not just the helper.

    A correct helper that `_read_command` does not call is the exact state this
    issue describes, so the assertion is on the source of the default rather
    than on the function in isolation.
    """
    import inspect

    from data_gate import __main__ as main_module

    # The default now goes through `resolve_trading_day`, which
    # `alpha-engine-config-I11355` added so the two post-parity crons could ask
    # for `yesterday` without a shell expression owning the calendar rule. The
    # invariant is unchanged and is asserted BOTH ways: `_read_command` must
    # route through the helper, and the helper must use the snap.
    command_src = inspect.getsource(main_module._read_command)
    assert "resolve_trading_day" in command_src, (
        "_read_command no longer routes its trading-day default through the resolver"
    )
    assert "else dt.datetime.now(dt.timezone.utc).date()" not in command_src, (
        "_read_command defaults to the bare UTC calendar date again (I11191)"
    )
    resolver_src = inspect.getsource(main_module.resolve_trading_day)
    assert "latest_trading_day_on_or_before" in resolver_src, (
        "resolve_trading_day no longer routes through the snap (I11191)"
    )
    # And the behaviour, not only the spelling: a Sunday still resolves to the
    # Friday, which is the measured defect this file exists for.
    assert main_module.resolve_trading_day(
        None, today=dt.date(2026, 9, 20)
    ) == dt.date(2026, 9, 18)
