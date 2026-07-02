"""Window-level roll-up of unexplained overwrite discrepancies (2026-07-02).

One corporate action restating adjusted history touches EVERY window date with
one uniform ratio — the HON separation paged six per-date ERROR emails for a
single 2:1 event. The window roll-up pages ONCE per uniform-ratio group and
preserves per-date ERROR semantics for non-uniform rows (a genuine per-date
data-quality anomaly must stay loud on its own date).
"""

from __future__ import annotations

import logging

import pandas as pd

from collectors import daily_closes


def _row(ticker: str, date: str, prior: float, new: float) -> dict:
    return {
        "ticker": ticker, "date": date, "prior": prior, "new": new,
        "ratio": new / prior,
    }


class TestWindowUnexplainedRollup:
    def test_uniform_ratio_group_pages_once(self, caplog):
        rows = [
            _row("HON", "2026-06-23", 444.74, 222.37),
            _row("HON", "2026-06-24", 454.84, 227.42),
            _row("HON", "2026-06-25", 462.48, 231.24),
            _row("HON", "2026-06-26", 464.42, 232.21),
            _row("HON", "2026-06-29", 455.60, 227.80),
            _row("HON", "2026-06-30", 447.80, 223.90),
        ]
        with caplog.at_level(logging.ERROR):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-07-01")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1  # ONE page for six restated dates
        assert "UNIFORM" in errors[0].message
        assert "6 window date(s)" in errors[0].message
        assert "2:1" in errors[0].message  # split-ratio hint carried through

    def test_non_uniform_rows_keep_per_date_errors(self, caplog):
        rows = [
            _row("MMM", "2026-06-24", 100.0, 210.0),
            _row("MMM", "2026-06-26", 100.0, 55.0),
        ]
        with caplog.at_level(logging.ERROR):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-07-01")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 2
        assert all("polygon_only OVERWRITE MMM @" in r.message for r in errors)

    def test_singleton_keeps_per_date_error(self, caplog):
        rows = [_row("XYZ", "2026-06-24", 100.0, 55.0)]
        with caplog.at_level(logging.ERROR):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-07-01")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "OVERWRITE XYZ @ 2026-06-24" in errors[0].message

    def test_empty_rows_no_logging(self, caplog):
        with caplog.at_level(logging.DEBUG):
            daily_closes._emit_window_unexplained_discrepancies([], "2026-07-01")
        assert caplog.records == []

    def test_deferred_per_date_rows_log_warn_not_error(self, caplog):
        # The per-date call under a window defers: the row surface stays (WARN)
        # while the ERROR moves to the aggregate.
        new_df = pd.DataFrame({"Close": [232.21]}, index=["HON"])
        with caplog.at_level(logging.DEBUG):
            explained, unexplained = daily_closes._log_close_discrepancies(
                new_df, {"HON": 464.42}, "2026-06-26", defer_unexplained=True,
            )
        assert explained == []
        assert len(unexplained) == 1
        assert unexplained[0]["ticker"] == "HON"
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert errors == []
        assert any("deferred to the window-level" in r.message for r in warns)
