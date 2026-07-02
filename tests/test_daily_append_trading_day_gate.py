"""daily_append NYSE-calendar gate + purge_phantom_day sanity gate (config#1572)."""

from __future__ import annotations

import pytest


class TestDailyAppendRefusesNonTradingDay:
    def test_holiday_date_raises(self):
        from builders.daily_append import daily_append

        with pytest.raises(ValueError, match="not an NYSE trading day"):
            daily_append(date_str="2026-06-19")

    def test_weekend_date_raises(self):
        from builders.daily_append import daily_append

        with pytest.raises(ValueError, match="not an NYSE trading day"):
            daily_append(date_str="2026-07-05")


class TestPurgeRefusesRealSession:
    def test_trading_day_refused(self, monkeypatch):
        import sys

        from builders import purge_phantom_day

        monkeypatch.setattr(
            sys, "argv", ["purge", "--date", "2026-07-01", "--apply"],
        )
        with pytest.raises(SystemExit, match="IS an NYSE trading day"):
            purge_phantom_day.main()
