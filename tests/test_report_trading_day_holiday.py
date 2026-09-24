"""The daily report resolves its trading day on the real NYSE calendar.

`alpha-engine-config-I11193`. `data_gate/report.py` carried a local
`previous_trading_day` that stepped back over Saturdays and Sundays only, so
the morning after a market holiday the report was produced ABOUT the holiday:
run on Monday 2026-07-06 it reported on 2026-07-03 (observed Independence
Day), a day on which every unit correctly did not run, and rendered that as
absence. Meanwhile the board (`cadence.latest_trading_day_on_or_before`) was
already on the shared calendar, so on a holiday boundary the two described
different days while both labelled the field `trading_day`.

The report now uses `nousergon_lib.trading_calendar.previous_trading_day`.
These tests pin the holiday boundaries, the board/report agreement, and that
an out-of-coverage date fails loudly rather than falling back to a weekday
guess.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import pytest

from data_gate import report as report_module
from data_gate.cadence import latest_trading_day_on_or_before

REPO = pathlib.Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("calendar_date", "expected", "why"),
    [
        ("2026-07-06", "2026-07-02", "Monday after the observed Independence Day (Fri 07-03)"),
        ("2026-11-27", "2026-11-25", "Friday after Thanksgiving, a mid-week holiday"),
        ("2026-06-22", "2026-06-18", "Monday after Juneteenth (Fri 06-19)"),
        ("2026-01-02", "2025-12-31", "Friday after New Year's Day (Thu 01-01)"),
        ("2025-12-26", "2025-12-24", "Friday after Christmas (Thu 12-25)"),
        ("2026-09-21", "2026-09-18", "an ordinary Monday still resolves to Friday"),
    ],
)
def test_the_report_skips_market_holidays(calendar_date, expected, why):
    got = report_module.previous_trading_day(dt.date.fromisoformat(calendar_date))
    assert got.isoformat() == expected, why


def test_the_report_and_the_board_agree_on_every_calendar_day():
    """The report is about the completed session before its calendar date; the
    board's `--trading-day yesterday` read is about the same session. They must
    name the same day for every calendar date, holidays included — asserted
    over two whole years rather than by inspection of a few cases."""
    from data_gate.__main__ import resolve_trading_day

    day = dt.date(2025, 1, 2)
    end = dt.date(2026, 12, 31)
    while day <= end:
        report_day = report_module.previous_trading_day(day)
        assert report_day == latest_trading_day_on_or_before(day - dt.timedelta(days=1)), day
        assert report_day == resolve_trading_day("yesterday", today=day), day
        day += dt.timedelta(days=1)


def test_the_report_command_defaults_past_a_holiday(monkeypatch):
    """End to end through the CLI: no `--trading-day`, calendar date the Monday
    after a holiday, and the report is run about the last day that traded."""
    from data_gate import __main__ as cli

    seen: dict = {}

    def _run_report(store, *, trading_day, calendar_date, **_):
        seen["trading_day"] = trading_day
        seen["calendar_date"] = calendar_date
        return {
            "status": "ok",
            "trading_day": trading_day.isoformat(),
            "calendar_date": calendar_date.isoformat(),
            "trigger": "manual",
        }

    monkeypatch.setattr(report_module, "run_report", _run_report)
    monkeypatch.setattr(cli, "open_store", lambda *a, **k: object())
    rc = cli.main(["report", "--store", "/tmp/store", "--calendar-date", "2026-07-06"])
    assert rc == 0
    assert seen == {
        "trading_day": dt.date(2026, 7, 2),
        "calendar_date": dt.date(2026, 7, 6),
    }


def test_an_out_of_coverage_date_fails_loudly_and_never_guesses(monkeypatch, capsys):
    """The shared calendar raises past its declared coverage. The report must
    exit 2 naming the calendar's reason — never fall back to a weekday rule,
    which would reintroduce this defect with a longer fuse."""
    from data_gate import __main__ as cli

    def _must_not_run(*a, **k):
        raise AssertionError("run_report was reached with an unresolvable trading day")

    monkeypatch.setattr(report_module, "run_report", _must_not_run)
    monkeypatch.setattr(cli, "open_store", lambda *a, **k: object())
    rc = cli.main(["report", "--store", "/tmp/store", "--calendar-date", "2099-01-05"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "trading day could not be resolved" in err
    assert "TradingCalendar" in err


def test_no_weekday_rule_remains_in_the_gate_package():
    """Closes-when of I11193: the weekday shim is gone, not merely unused."""
    offenders = [
        f"{path.relative_to(REPO)}:{n}"
        for path in sorted((REPO / "data_gate").rglob("*.py"))
        for n, line in enumerate(path.read_text().splitlines(), 1)
        if "weekday() >= 5" in line
    ]
    assert offenders == []
