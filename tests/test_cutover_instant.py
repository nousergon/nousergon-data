"""`data_gate/cutover.py::CUTOVER_UTC` is a PLANNED instant — pin what makes it correct.

The value is the START of the declared merge window (a weekday, 18:30-23:00
America/New_York; alpha-engine-config-I11269, Brian's 2026-09-21 hold). It is
equivalent to the real deploy instant only because no v1 state machine that
ran a data stage can START between 18:30 ET and 23:00 ET the same day. These
tests pin both halves: the value's shape, and that property derived from the
committed v1 trigger schedules — so neither an edit to the value nor a move of
a v1 trigger can silently break the equivalence.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from zoneinfo import ZoneInfo

import pytest

from data_gate import cutover as cut
from infrastructure import deploy_blast_radius as br

ET = ZoneInfo("America/New_York")
REPO = pathlib.Path(__file__).resolve().parent.parent
ORCHESTRATION = REPO / "infrastructure" / "cloudformation" / "alpha-engine-orchestration.yaml"
WINDOW_START = dt.time(18, 30)
WINDOW_END = dt.time(23, 0)


def is_window_start(value: str) -> bool:
    local = cut.cutover_instant(value).astimezone(ET)
    return local.weekday() < 5 and local.time() == WINDOW_START


def test_cutover_is_marked_planned():
    assert cut.CUTOVER_STATUS == "planned"


def test_cutover_is_a_weekday_merge_window_start():
    assert is_window_start(cut.CUTOVER_UTC), (
        f"CUTOVER_UTC={cut.CUTOVER_UTC} is not a weekday 18:30 America/New_York instant — "
        "see data_gate/cutover.py before editing it"
    )


@pytest.mark.parametrize(
    "bad",
    [
        "2026-09-28T18:00:00Z",  # 14:00 ET: inside the trading day
        "2026-09-26T22:30:00Z",  # a Saturday 18:30 ET
        "2026-09-28T22:45:00Z",  # inside the window but not its start
    ],
)
def test_the_shape_check_is_not_vacuous(bad):
    assert not is_window_start(bad)


def test_the_committed_v1_fixed_clock_triggers_are_the_ones_reasoned_about():
    """The equivalence argument names these two schedules; if either moves,
    this fails and the argument must be re-made."""
    text = ORCHESTRATION.read_text(encoding="utf-8")
    assert "ScheduleExpression: 'cron(0 9 ? * THU-SAT *)'" in text
    assert "ScheduleExpression: 'cron(15 5 ? * MON-FRI *)'" in text
    assert "ScheduleExpressionTimezone: 'America/Los_Angeles'" in text
    assert (br.PREOPEN_HOUR_PT, br.PREOPEN_MINUTE_PT, br.WEEKLY_HOUR_UTC) == (5, 15, 9)


def test_no_fixed_clock_v1_start_falls_inside_the_window():
    start = cut.cutover_instant()
    end = dt.datetime.combine(start.astimezone(ET).date(), WINDOW_END, tzinfo=ET)
    assert br._next_preopen_utc(start) > end
    assert br._next_weekly_utc(start) > end


def test_the_postclose_start_precedes_the_window():
    """The postclose SF starts on the trading daemon's shutdown at the 16:00 ET
    close, which the window start clears by 2.5 h. An execution that started
    before the window is correctly uncounted: it began on the old definition."""
    local = cut.cutover_instant().astimezone(ET)
    assert local.time() > dt.time(16, 0)


def test_the_cutover_trading_day_is_the_window_date():
    assert cut.cutover_trading_day() == cut.cutover_instant().astimezone(ET).date()
    assert cut.cutover_trading_day("2026-09-29T02:59:00Z") == dt.date(2026, 9, 28)


def test_an_offsetless_instant_is_refused():
    with pytest.raises(ValueError, match="no offset"):
        cut.cutover_instant("2026-09-28T22:30:00")
