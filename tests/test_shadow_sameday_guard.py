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
