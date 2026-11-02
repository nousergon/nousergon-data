"""alpha-engine-config-I13044: an unexplained split-like pure append opens a
machine-written ticker hold, and a restated series clears it.

Prevention for nous-ergon-ops-I2044 (CTVA 2026-10-01: 77.65 -> 12.57, ratio
0.162, no registered action). The guard keeps writing the bar at ERROR (a
genuine crash looks the same); the hold is what keeps the name out of picks.
"""

from __future__ import annotations

import io
import json
import logging

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from builders import ticker_holds as th
from builders.daily_append import _SPLICE_GUARD_SOFT_RATIO, _splice_basis_guard


class _FakeS3:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body
        return {}

    def get_paginator(self, _op):
        store = self.store

        class _P:
            def paginate(self, *, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in sorted(store) if k.startswith(Prefix)]}

        return _P()

    def body(self, ticker):
        return json.loads(self.store[th.hold_key(ticker)])


def _hist(values_by_date: dict[str, float]) -> pd.DataFrame:
    idx = pd.to_datetime(list(values_by_date))
    return pd.DataFrame({"Close": list(values_by_date.values())}, index=idx).sort_index()


def _bar(close: float) -> pd.Series:
    return pd.Series({"Open": close, "High": close, "Low": close, "Close": close,
                      "Volume": 1_000_000.0, "VWAP": close})


def _open_ctva(s3):
    return th.open_hold(
        s3, "b", ticker="CTVA", break_date="2026-10-01", prior_date="2026-09-30",
        prior_close=77.65, break_close=12.57, ratio=12.57 / 77.65,
    )


def test_unexplained_016_pure_append_pages_and_is_marked_for_a_hold(caplog):
    hist = _hist({"2026-09-29": 77.10, "2026-09-30": 77.65})
    with caplog.at_level(logging.ERROR):
        out, verdict = _splice_basis_guard(
            "CTVA", _bar(12.57), hist, pd.Timestamp("2026-10-01"), [], None
        )
    assert verdict == "unexplained"
    assert float(out["Close"]) == pytest.approx(12.57)  # still written
    assert any("NO registered corporate action" in r.message for r in caplog.records)


def test_open_hold_writes_an_active_dated_record():
    s3 = _FakeS3()
    body = _open_ctva(s3)
    stored = s3.body("CTVA")
    assert body == stored
    assert stored["status"] == "active"
    assert stored["break_date"] == "2026-10-01"
    assert stored["ratio"] == pytest.approx(0.161880, abs=1e-5)
    assert stored["issue"] == "alpha-engine-config-I13044"
    assert th.load_active_holds(s3, "b") == {"CTVA": stored}


def test_open_hold_keeps_the_earliest_active_break():
    s3 = _FakeS3()
    _open_ctva(s3)
    again = th.open_hold(
        s3, "b", ticker="CTVA", break_date="2026-10-02", prior_date="2026-10-01",
        prior_close=12.57, break_close=4.0, ratio=4.0 / 12.57,
    )
    assert again is None
    assert s3.body("CTVA")["break_date"] == "2026-10-01"


def test_a_cleared_hold_is_replaced_when_the_name_breaks_again():
    s3 = _FakeS3()
    th.clear_hold(s3, "b", _open_ctva(s3), reason="x", cleared_by="operator")
    assert th.load_active_holds(s3, "b") == {}
    assert th.open_hold(
        s3, "b", ticker="CTVA", break_date="2026-11-02", prior_date="2026-10-30",
        prior_close=12.0, break_close=40.0, ratio=40.0 / 12.0,
    ) is not None
    assert s3.body("CTVA")["status"] == "active"


def test_restated_series_clears_and_unrestated_does_not():
    hold = {"ticker": "CTVA", "break_date": "2026-10-01", "ratio": 0.1619}
    still_broken = _hist({"2026-09-30": 77.65, "2026-10-01": 12.57, "2026-10-02": 12.9})
    # The vendor re-adjusted the pre-spin history (implied factor ~0.150):
    # 10-01 now reads +7.89% over the restated 09-30.
    restated = _hist({"2026-09-30": 11.65, "2026-10-01": 12.57, "2026-10-02": 12.9})
    assert th.break_restated(hold, still_broken, _SPLICE_GUARD_SOFT_RATIO)[0] is False
    ok, ratio = th.break_restated(hold, restated, _SPLICE_GUARD_SOFT_RATIO)
    assert ok is True and ratio == pytest.approx(12.57 / 11.65)


@pytest.mark.parametrize(
    "hist",
    [
        pd.DataFrame(),
        _hist({"2026-09-30": 11.65, "2026-10-02": 12.9}),  # break row absent
        _hist({"2026-10-01": 12.57}),  # nothing before the break
    ],
)
def test_absence_of_evidence_never_clears(hist):
    hold = {"ticker": "CTVA", "break_date": "2026-10-01"}
    assert th.break_restated(hold, hist, _SPLICE_GUARD_SOFT_RATIO) == (False, None)


def test_operator_clear_is_a_dry_run_without_apply(monkeypatch, capsys):
    s3 = _FakeS3()
    _open_ctva(s3)
    monkeypatch.setattr("boto3.client", lambda *a, **k: s3)
    assert th.main(["--bucket", "b", "clear", "CTVA", "--reason", "genuine move"]) == 0
    assert s3.body("CTVA")["status"] == "active"
    assert th.main(["--bucket", "b", "clear", "CTVA", "--reason", "genuine move", "--apply"]) == 0
    cleared = s3.body("CTVA")
    assert cleared["status"] == "cleared"
    assert cleared["cleared_by"] == "operator"
    assert cleared["clear_reason"] == "genuine move"


# ── daily_append end to end ──────────────────────────────────────────────────


def _run_daily_append(monkeypatch, *, closes_override, active_holds):
    from builders import daily_append as _da
    from tests.conftest import recent_trading_day_str
    from tests.test_daily_append_skip_if_exists import _patch_targets

    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")
    monkeypatch.setenv("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "false")
    today_str = recent_trading_day_str()
    _patch_targets(
        monkeypatch, universe_symbols=["AAPL", "CTVA"], today_in_hist=False,
        today_str=today_str,
    )
    closes = _da._load_daily_closes()
    for ticker, close in closes_override.items():
        closes[ticker] = dict(closes[ticker], Open=close, High=close, Low=close,
                              Close=close, VWAP=close)
    monkeypatch.setattr(_da, "_load_daily_closes", lambda *a, **k: closes)

    written: list[dict] = []
    monkeypatch.setattr(th, "load_active_holds", lambda *a, **k: dict(active_holds))
    monkeypatch.setattr(th, "read_hold", lambda *a, **k: None)
    monkeypatch.setattr(th, "_put", lambda s3, bucket, body: written.append(body))
    return _da.daily_append(date_str=today_str), written, today_str


def test_daily_append_opens_a_hold_and_still_writes_the_bar(monkeypatch):
    result, written, today_str = _run_daily_append(
        monkeypatch, closes_override={"CTVA": 16.2}, active_holds={},
    )
    assert result["status"] == "ok"
    assert result["ticker_holds_opened"] == ["CTVA"]
    assert result["ticker_holds_cleared"] == []
    assert [b["ticker"] for b in written] == ["CTVA"]
    assert written[0]["status"] == "active"
    assert written[0]["break_date"] == today_str
    assert written[0]["ratio"] == pytest.approx(0.162)
    assert result["tickers_quality_blocked"] == 0


def test_daily_append_clears_a_hold_whose_series_was_restated(monkeypatch):
    # The harness's stored history is flat at 100.0, so a hold on a stored
    # date reads ratio 1.0 there: the break is gone.
    hold = {"ticker": "AAPL", "status": "active", "break_date": "2024-06-03",
            "ratio": 0.16}
    result, written, _ = _run_daily_append(
        monkeypatch, closes_override={}, active_holds={"AAPL": hold},
    )
    assert result["ticker_holds_cleared"] == ["AAPL"]
    assert result["ticker_holds_opened"] == []
    assert written[0]["status"] == "cleared"
    assert written[0]["cleared_by"] == "auto:builders.daily_append"
