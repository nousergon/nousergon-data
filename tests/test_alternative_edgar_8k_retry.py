"""EDGAR 8-K search retry + failure accounting — alpha-engine-config-I11473.

efts.sec.gov full-text search returns intermittent HTTP 500s. Before this fix
``_fetch_news`` made one bare ``requests.get``: a single 500 dropped that
ticker's 8-K block (MSFT, AVGO, TSLA, JPM and SNDK in one rehearsal run), the
per-ticker JSON still landed, and the stage stayed green. The GET now goes
through ``nousergon_lib.http_retry.request_with_retry`` (429 + 5xx retried),
and tickers whose 8-K search still failed are counted in the phase result.
"""

from __future__ import annotations

import json
import types
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import pytest
import requests

from collectors import alternative


def _fake_feedparser():
    return types.SimpleNamespace(parse=lambda url, **kw: types.SimpleNamespace(entries=[]))


def _resp(status: int, hits: list | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.headers = {}
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status} Server Error")
    else:
        resp.raise_for_status.return_value = None
    resp.json.return_value = {"hits": {"hits": hits or []}}
    return resp


_HIT = {"_source": {"display_names": ["SNDK Corp"], "file_date": "2026-09-22", "form_type": "8-K"}}


@pytest.fixture
def no_backoff():
    # request_with_retry binds time.sleep as a default argument, so patch the
    # delay it computes instead: retries run instantly.
    with patch("krepis.http_retry.backoff_delay", return_value=0.0) as m:
        yield m


def _run_fetch_news(side_effect):
    calls = []

    def _request(self, method, url, **kwargs):
        calls.append({"method": method, "url": url, "headers": dict(self.headers)})
        nxt = side_effect.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    with patch.dict("sys.modules", {"feedparser": _fake_feedparser()}), \
         patch.object(requests.Session, "request", autospec=True, side_effect=_request):
        out = alternative._fetch_news("SNDK", "2026-09-23")
    return out, calls


def test_single_500_is_retried_and_8k_lands(no_backoff):
    out, calls = _run_fetch_news([_resp(500), _resp(200, [_HIT])])

    assert len(calls) == 2
    assert out["sec_filings_8k"] == [
        {"title": "SNDK Corp", "date": "2026-09-22", "form_type": "8-K"}
    ]
    assert alternative._EDGAR_8K_FAILED_KEY not in out
    # SEC fair-access requires the User-Agent on every attempt.
    assert all(c["headers"]["User-Agent"] == "alpha-engine-data/1.0" for c in calls)
    assert urlparse(calls[0]["url"]).hostname == "efts.sec.gov"


def test_429_is_retried(no_backoff):
    out, calls = _run_fetch_news([_resp(429), _resp(200, [_HIT])])
    assert len(calls) == 2
    assert len(out["sec_filings_8k"]) == 1


def test_sustained_500_is_bounded_and_marked_failed(no_backoff):
    n = alternative._EDGAR_8K_MAX_ATTEMPTS
    out, calls = _run_fetch_news([_resp(500) for _ in range(n)])

    assert len(calls) == n
    assert out["sec_filings_8k"] == []
    assert out[alternative._EDGAR_8K_FAILED_KEY] is True


def test_4xx_is_not_retried(no_backoff):
    out, calls = _run_fetch_news([_resp(404)])
    assert len(calls) == 1
    assert out[alternative._EDGAR_8K_FAILED_KEY] is True


def test_timeout_stays_single_shot(no_backoff):
    # retry_network=False: a hang is not retried, so an efts blackhole does
    # not triple this sub-fetch's worst-case runtime across ~900 tickers.
    out, calls = _run_fetch_news([requests.ReadTimeout("read timed out")])
    assert len(calls) == 1
    assert out[alternative._EDGAR_8K_FAILED_KEY] is True


def _payload(ticker: str, *, edgar_failed: bool) -> dict:
    news = {"articles": [{"headline": "X", "source": "Yahoo"}], "sec_filings_8k": []}
    if edgar_failed:
        news[alternative._EDGAR_8K_FAILED_KEY] = True
    return {
        "ticker": ticker,
        "fetched_at": "2026-09-23T20:00:00+00:00",
        "analyst_consensus": {"rating": "Buy", "target_price": 200.0,
                              "num_analysts": 25, "earnings_surprises": []},
        "eps_revision": {"current_estimate": 6.5, "revision_4w": 1.2, "streak": 2},
        "options_flow": {"put_call_ratio": 0.7, "iv_rank": 35, "expected_move_pct": 4.5},
        "insider_activity": {"cluster_buying": True, "net_shares_30d": 5,
                             "transactions": [{"insider": "CEO"}]},
        "institutional": {"accumulation": False, "funds_increasing": 0,
                          "funds_decreasing": 0},
        "news": news,
    }


def test_marker_is_stripped_before_the_s3_write():
    s3 = MagicMock()
    with patch.object(alternative, "_fetch_all_alternative",
                      return_value=_payload("SNDK", edgar_failed=True)):
        res = alternative._process_one_ticker(
            "SNDK", "2026-09-23", "b", frozenset(), s3, "market_data/",
        )

    assert res["status"] == "ok"
    assert res["edgar_8k_failed"] is True
    body = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert alternative._EDGAR_8K_FAILED_KEY not in body["news"]


def test_collect_reports_persistent_8k_failures(monkeypatch):
    tickers = [f"TKR{i}" for i in range(5)]
    failed = {"TKR1", "TKR3"}
    payloads = {t: _payload(t, edgar_failed=t in failed) for t in tickers}

    s3 = MagicMock()
    monkeypatch.setattr(alternative, "boto3", MagicMock(client=lambda *a, **k: s3))
    monkeypatch.setattr(alternative, "_fetch_all_alternative",
                        lambda ticker, run_date, bucket: payloads[ticker])

    result = alternative.collect(
        bucket="b", s3_prefix="market_data/", run_date="2026-09-23", tickers=tickers,
    )

    assert result["tickers_edgar_8k_failed"] == 2
    assert result["edgar_8k_failed_tickers"] == ["TKR1", "TKR3"]
    manifest = next(
        json.loads(c.kwargs["Body"]) for c in s3.put_object.call_args_list
        if c.kwargs["Key"].endswith("/alternative/manifest.json")
    )
    assert manifest["tickers_edgar_8k_failed"] == 2
    assert manifest["edgar_8k_failed_tickers"] == ["TKR1", "TKR3"]


def test_collect_reports_zero_when_8k_healthy(monkeypatch):
    tickers = ["TKR0", "TKR1"]
    s3 = MagicMock()
    monkeypatch.setattr(alternative, "boto3", MagicMock(client=lambda *a, **k: s3))
    monkeypatch.setattr(alternative, "_fetch_all_alternative",
                        lambda ticker, run_date, bucket: _payload(ticker, edgar_failed=False))

    result = alternative.collect(
        bucket="b", s3_prefix="market_data/", run_date="2026-09-23", tickers=tickers,
    )
    assert result["tickers_edgar_8k_failed"] == 0
    assert result["edgar_8k_failed_tickers"] == []
