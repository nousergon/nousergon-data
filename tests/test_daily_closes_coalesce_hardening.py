"""Coalesce-arc hardening tests (L4480 / L4482 / L4486).

Follow-ups to the 2026-06-01 FRED-429 / polygon-timeout incident:

  * L4480 — `_fred_get_with_retry`: bounded exponential backoff + jitter on the
    transient class (429 / 5xx / timeout), immediate raise on a deterministic
    4xx, surface failure after the attempt budget.
  * L4482 — a TRANSIENT polygon network failure in ``polygon_only`` mode must
    not abort the date; FRED + the macro yfinance backstop still run.
  * L4486 — a FRED-index restatement toward the authoritative value logs at
    WARN (`fred_restatement`), not the ERROR band reserved for equity drift.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests
from botocore.exceptions import ClientError

from collectors import daily_closes


def _resp(status_code: int, *, observations=None, headers=None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = {"observations": observations or []}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _no_existing_parquet_s3():
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject",
    )
    return s3


# ── L4480: FRED backoff + jitter ─────────────────────────────────────────────

class TestFredRetry:
    def test_retries_429_then_succeeds(self):
        ok = _resp(200, observations=[{"date": "2026-06-01", "value": "16.0"}])
        seq = [_resp(429), ok]
        with patch.object(daily_closes.requests, "get", side_effect=seq) as g, \
                patch.object(daily_closes.time, "sleep") as sleep:
            out = daily_closes._fred_get_with_retry({"series_id": "VIXCLS"})
        assert out is ok
        assert g.call_count == 2          # one 429, one success
        assert sleep.call_count == 1      # backed off once

    def test_persistent_429_raises_after_budget(self):
        with patch.object(daily_closes.requests, "get", return_value=_resp(429)), \
                patch.object(daily_closes.time, "sleep"):
            with pytest.raises(requests.HTTPError):
                daily_closes._fred_get_with_retry({"series_id": "VIXCLS"})

    def test_deterministic_4xx_not_retried(self):
        # A 404 is not in the transient class — raise immediately, no backoff.
        with patch.object(daily_closes.requests, "get", return_value=_resp(404)) as g, \
                patch.object(daily_closes.time, "sleep") as sleep:
            with pytest.raises(requests.HTTPError):
                daily_closes._fred_get_with_retry({"series_id": "BOGUS"})
        assert g.call_count == 1
        assert sleep.call_count == 0

    def test_timeout_retried_then_reraised(self):
        with patch.object(daily_closes.requests, "get",
                          side_effect=requests.Timeout("read timeout")), \
                patch.object(daily_closes.time, "sleep") as sleep:
            with pytest.raises(requests.Timeout):
                daily_closes._fred_get_with_retry({"series_id": "VIXCLS"})
        # Budget-1 backoffs before the final reraise.
        assert sleep.call_count == daily_closes._FRED_MAX_ATTEMPTS - 1


# ── L4482: transient polygon failure is non-fatal to the macro backstop ──────

class TestPolygonTransientNonFatal:
    def test_polygon_timeout_still_fills_macro_via_fred(self):
        s3 = _no_existing_parquet_s3()

        def _fred_side_effect(tickers, date_str, records, window_cache=None):
            for t in tickers:
                records.append({
                    "ticker": t.lstrip("^"), "date": date_str,
                    "Open": 16.0, "High": 16.0, "Low": 16.0, "Close": 16.0,
                    "Adj_Close": 16.0, "Volume": 0, "VWAP": None, "source": "fred",
                })
            return len(tickers)

        with patch("collectors.daily_closes.boto3.client", return_value=s3), \
                patch("collectors.daily_closes._fetch_polygon_closes",
                      side_effect=requests.Timeout("read timeout")), \
                patch("collectors.daily_closes._fetch_fred_closes",
                      side_effect=_fred_side_effect):
            # Macro-only ticker list → no equity-coverage gate to satisfy.
            result = daily_closes.collect(
                bucket="b", tickers=["^VIX"], run_date="2026-06-01",
                source="polygon_only", dry_run=True,
            )
        # Did NOT raise on the polygon timeout; the macro key filled from FRED.
        assert result.get("status") != "error", result

    def test_polygon_forbidden_still_propagates(self):
        # The structural 403 is NOT transient — it must still raise loudly.
        s3 = _no_existing_parquet_s3()
        from polygon_client import PolygonForbiddenError
        with patch("collectors.daily_closes.boto3.client", return_value=s3), \
                patch("collectors.daily_closes._fetch_polygon_closes",
                      side_effect=PolygonForbiddenError("403")):
            with pytest.raises(PolygonForbiddenError):
                daily_closes.collect(
                    bucket="b", tickers=["AAPL"], run_date="2026-06-01",
                    source="polygon_only", dry_run=True,
                )


# ── L4486: FRED-index restatement → WARN, equity drift → ERROR ───────────────

class TestRestatementSeverity:
    def _df(self, ticker: str, close: float) -> pd.DataFrame:
        return pd.DataFrame(
            [{"Close": close}], index=pd.Index([ticker], name="ticker"),
        )

    def test_fred_index_large_jump_is_warn_not_error(self, caplog):
        # VIX 16.01 → 17.26 (~7.8%, >5%) — an expected reconciliation self-heal.
        with caplog.at_level(logging.WARNING):
            daily_closes._log_close_discrepancies(
                self._df("VIX", 17.26), {"VIX": 16.01}, "2026-06-02",
            )
        assert any(r.levelno == logging.WARNING and "fred_restatement" in r.message
                   for r in caplog.records)
        assert not any(r.levelno == logging.ERROR for r in caplog.records)

    def test_equity_large_jump_stays_error(self, caplog):
        # AAPL 100 → 120 (20%, >5%) — genuine cross-source drift, still ERROR.
        with caplog.at_level(logging.WARNING):
            daily_closes._log_close_discrepancies(
                self._df("AAPL", 120.0), {"AAPL": 100.0}, "2026-06-02",
            )
        assert any(r.levelno == logging.ERROR for r in caplog.records)
        assert not any("fred_restatement" in r.message for r in caplog.records)
