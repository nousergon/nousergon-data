"""`shadow-sameday` runs only on the day it is collecting, after the close.

`alpha-engine-config-I11203`. The workload resolves its own trading day on the
box because an EventBridge Scheduler rule carries a static input and cannot
compute today's date. Its guard is:

    TD=$(python -c 'from dates import default_run_date; print(default_run_date())')
    TODAY=$(TZ=America/New_York date +%F)
    [ "$TD" != "$TODAY" ] && exit 0

`default_run_date()` returns the last session whose 4:00 PM ET close HAS
occurred, so that comparison refuses two distinct things — a non-session, and a
fire before today's close. The second was not designed for and is the more
valuable half: a same-day run started before the close would grade a complete
v2 run against a v1 day still in progress, and every key v1 had yet to write
would read `live_missing`.

These tests pin the DATE LOGIC the shell guard encodes, against the real
`dates.default_run_date`. The shell string itself is asserted by
`infrastructure/lambdas/data-spot-dispatcher/test_handler.py`.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dates import default_run_date  # noqa: E402

ET = dt.timezone(dt.timedelta(hours=-4))  # EDT; every case below is in DST


def _guard_runs(moment_utc: str) -> bool:
    """The shell guard's decision, in Python: does the workload proceed?"""
    now = dt.datetime.fromisoformat(moment_utc)
    trading_day = default_run_date(now=now)
    today_et = now.astimezone(ET).strftime("%Y-%m-%d")
    return trading_day == today_et


@pytest.mark.parametrize(
    "label,moment,expected",
    [
        # 18:30 ET Monday — the intended firing time, after the 16:00 close.
        ("monday after the close", "2026-09-21T22:30:00+00:00", True),
        # 12:00 ET Monday — a session, but v1's day is not finished.
        ("monday before the close", "2026-09-21T16:00:00+00:00", False),
        # Saturday — default_run_date gives Friday.
        ("saturday", "2026-09-19T22:30:00+00:00", False),
        # Thanksgiving 2026-11-26 — a weekday the Mon-Fri rule fires on.
        ("nyse holiday", "2026-11-26T23:30:00+00:00", False),
    ],
)
def test_shadow_sameday_guard_refuses_non_sessions_and_pre_close(label, moment, expected):
    assert _guard_runs(moment) is expected, label


def test_the_guard_is_not_merely_a_weekday_check():
    """A weekday check alone would run on Thanksgiving and before the close."""
    thanksgiving = dt.datetime.fromisoformat("2026-11-26T23:30:00+00:00")
    assert thanksgiving.astimezone(ET).weekday() < 5, "it IS a weekday"
    assert _guard_runs("2026-11-26T23:30:00+00:00") is False

    pre_close = dt.datetime.fromisoformat("2026-09-21T16:00:00+00:00")
    assert pre_close.astimezone(ET).weekday() < 5, "it IS a weekday"
    assert _guard_runs("2026-09-21T16:00:00+00:00") is False


# ── alpha-engine-config-I11352: the D+1 morning guard ────────────────────────
#
# `shadow-morning` runs v1's two MORNING legs on v1's own cadence — 07:45 ET,
# fifteen minutes after MorningSchedule's 07:30 — for the PREVIOUS NYSE
# session. Its guard is two questions, and conflating them is the bug:
#
#     IS_SESSION=$(python -c "... is_trading_day(now in ET) ...")
#     TD=$(python -c 'from dates import default_run_date; print(default_run_date())')
#     TODAY=$(TZ=America/New_York date +%F)
#     [ "$IS_SESSION" != "1" ] && exit 0      # v1 does not run either
#     [ "$TD" = "$TODAY" ]     && exit 0      # the close already happened
#
# The second alone is NOT enough, and that is the whole reason the calendar is
# consulted directly: on Thanksgiving `default_run_date()` returns Wednesday, a
# perfectly valid previous session, so `$TD != $TODAY` passes and the run would
# proceed against a v1 morning run that never fired.

from nousergon_lib.trading_calendar import is_trading_day  # noqa: E402


def _morning_guard(moment_utc: str) -> tuple[bool, str | None]:
    """The shell guard's decision, in Python: (runs?, the trading day)."""
    now = dt.datetime.fromisoformat(moment_utc)
    today_et = now.astimezone(ET).date()
    trading_day = default_run_date(now=now)
    if not is_trading_day(today_et):
        return False, None
    if trading_day == today_et.isoformat():
        return False, None
    return True, trading_day


@pytest.mark.parametrize(
    "label,moment,runs,trading_day",
    [
        # 07:45 ET Monday 2026-09-21 — the intended firing time. Targets the
        # PREVIOUS session, Friday the 18th, which is what v1's 07:30 run wrote.
        ("monday pre-open", "2026-09-21T11:45:00+00:00", True, "2026-09-18"),
        # 07:45 ET Tuesday — the ordinary weekday case, targeting yesterday.
        ("tuesday pre-open", "2026-09-22T11:45:00+00:00", True, "2026-09-21"),
        # A Monday 07:45 run targets FRIDAY, not "yesterday" (issue deliverable 3).
        ("monday targets friday", "2026-09-28T11:45:00+00:00", True, "2026-09-25"),
        # Thanksgiving 2026-11-26 — a WEEKDAY the Mon-Fri rule fires on, and
        # the case `$TD != $TODAY` alone gets wrong.
        ("nyse holiday", "2026-11-26T12:45:00+00:00", False, None),
        # 18:30 ET — a late fire, after the close. `default_run_date()` is now
        # today, so this would duplicate `shadow-sameday`'s target.
        ("after the close", "2026-09-21T22:30:00+00:00", False, None),
    ],
)
def test_shadow_morning_guard_refuses_holidays_and_post_close_fires(
    label, moment, runs, trading_day
):
    assert _morning_guard(moment) == (runs, trading_day), label


def test_the_morning_guard_is_not_default_run_date_alone():
    """On Thanksgiving `default_run_date()` gives Wednesday — a real session —
    so the sameday guard's `!=` comparison PASSES and only the direct calendar
    question refuses. This is the case the second half exists for."""
    thanksgiving = "2026-11-26T12:45:00+00:00"
    now = dt.datetime.fromisoformat(thanksgiving)
    assert default_run_date(now=now) != now.astimezone(ET).strftime("%Y-%m-%d")
    assert _morning_guard(thanksgiving)[0] is False


def test_the_morning_guard_is_not_weekday_arithmetic():
    """Holidays have bitten this repo before (`_previous_business_days` in
    collectors/daily_closes.py). The day AFTER Thanksgiving is a session and
    its previous session is Wednesday, skipping Thursday — which naive
    `today - 1 business day` also happens to get right, and which naive
    `today - 1 day` does not."""
    runs, trading_day = _morning_guard("2026-11-27T12:45:00+00:00")
    assert (runs, trading_day) == (True, "2026-11-25")
