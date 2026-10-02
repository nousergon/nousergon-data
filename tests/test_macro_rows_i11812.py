"""alpha-engine-config-I11812: D05's run manifest grades macro.json by `rows`.

`run_units.PHASE_UNITS[("phase1", "macro")]` declares `rows_key="rows"`, but
`macro.collect()` reported only `fields`, so every manifest recorded
`market_data/weekly/{date}/macro.json` at rows_out 0 and the standalone weekly
machine's completion check failed it with `rows_below_floor` on every run.
"""

from __future__ import annotations

import pandas as pd

import run_units
from collectors import macro


def _stub(monkeypatch, fred: dict, market: dict) -> None:
    monkeypatch.setattr(macro, "_fetch_fred", lambda: dict(fred))
    monkeypatch.setattr(macro, "_fetch_market_prices", lambda trading_day=None: dict(market))
    monkeypatch.setattr(macro, "_load_breadth_prices", lambda bucket: None)
    cols = ["date", "series_id", "label", "value", "units", "frequency"]
    monkeypatch.setattr(macro, "build_macro_history", lambda *a, **k: pd.DataFrame(columns=cols))

    class _FakeS3:
        def put_object(self, **kwargs):
            return {}

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())


def test_the_declared_rows_key_is_what_collect_reports(monkeypatch):
    _stub(monkeypatch, {"fed_funds_rate": 3.5, "vix": 18.0}, {"sp500_close": 650.0})
    result = macro.collect(bucket="b", run_date="2026-10-02")
    rows_key = run_units.PHASE_UNITS[("phase1", "macro")].rows_key
    assert rows_key in result
    assert result[rows_key] == 3


def test_null_series_and_the_fetched_at_stamp_are_not_rows(monkeypatch):
    _stub(
        monkeypatch,
        {"fed_funds_rate": None, "vix": float("nan"), "unemployment": 4.1},
        {"sp500_close": None},
    )
    result = macro.collect(bucket="b", run_date="2026-10-02")
    assert result["rows"] == 1


def test_an_all_null_publish_reads_zero_not_its_key_count(monkeypatch):
    _stub(monkeypatch, {"fed_funds_rate": None, "vix": None}, {"sp500_close": None})
    result = macro.collect(bucket="b", run_date="2026-10-02")
    assert result["rows"] == 0
    assert result["fields"] == 4


def test_dry_run_reports_the_same_count(monkeypatch):
    _stub(monkeypatch, {"fed_funds_rate": 3.5}, {"sp500_close": 650.0})
    result = macro.collect(bucket="b", run_date="2026-10-02", dry_run=True)
    assert result["status"] == "ok_dry_run"
    assert result["rows"] == 2
