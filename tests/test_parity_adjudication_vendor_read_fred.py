"""The FRED half of the parity vendor read (alpha-engine-config-I12023).

Run 37491382676 lost the whole read to one bare ``HTTP 400`` from FRED: the
error carried no reason and the Polygon half, already fetched, was discarded.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "parity_adjudication_vendor_read",
    Path(__file__).resolve().parents[1] / "scripts" / "parity_adjudication_vendor_read.py",
)
vendor_read = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(vendor_read)


class _Response:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


@pytest.fixture
def fred(monkeypatch):
    import requests

    calls = []

    def fake_get(url, params, timeout):
        calls.append((url, dict(params)))
        if params.get("series_id") == "BAD":
            return _Response(400, {"error_code": 400, "error_message": "Bad Request. Series does not exist."})
        if url.endswith("/series"):
            return _Response(200, {"seriess": [{"last_updated": "2026-09-29 15:16:02-05"}]})
        return _Response(200, {"observations": [
            {"date": "2026-09-25", "value": "4.18", "realtime_start": "2026-09-26", "realtime_end": "9999-12-31"},
        ]})

    monkeypatch.setenv("FRED_API_KEY", "k" * 32)
    monkeypatch.setattr(requests, "get", fake_get)
    return calls


def test_the_real_time_window_starts_at_the_observation(fred):
    out = vendor_read.read_fred(["DGS10:2026-09-25"])
    params = fred[0][1]
    assert params["realtime_start"] == "2026-09-25"
    assert params["observation_start"] == params["observation_end"] == "2026-09-25"
    assert out["DGS10:2026-09-25"]["vintages"][0]["value"] == "4.18"


def test_a_failing_series_records_fred_reason_and_the_rest_still_reads(fred):
    out = vendor_read.read_fred(["BAD:2026-09-25", "DGS10:2026-09-25"])
    assert "Series does not exist" in out["BAD:2026-09-25"]["error"]
    assert "k" * 32 not in out["BAD:2026-09-25"]["error"]
    assert out["DGS10:2026-09-25"]["vintages"]


def test_main_prints_the_payload_and_fails_when_a_series_failed(fred, capsys):
    assert vendor_read.main(["--fred", "BAD:2026-09-25,DGS10:2026-09-25"]) == 1
    printed = capsys.readouterr().out
    assert "BEGIN-PARITY-VENDOR-READ" in printed and "END-PARITY-VENDOR-READ" in printed


def test_the_ticker_filter_keeps_only_named_tickers_and_names_the_absent(monkeypatch, capsys):
    import sys
    import types

    bar = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10, "vwap": 1.2}

    class FakeClient:
        def get_grouped_daily(self, day):
            return {"AAA": bar, "BBB": bar, "CCC": bar}

        def get_single_day_bar(self, ticker, day):
            return None

    monkeypatch.setitem(sys.modules, "polygon_client", types.SimpleNamespace(PolygonClient=FakeClient))
    out = vendor_read.read_polygon(["2026-09-28"], {"AAA", "CCC", "ZZZ"})
    assert sorted(out["dates"]["2026-09-28"]) == ["AAA", "CCC"]
    assert "['ZZZ']" in capsys.readouterr().out
    assert sorted(vendor_read.read_polygon(["2026-09-28"])["dates"]["2026-09-28"]) == ["AAA", "BBB", "CCC"]


def test_a_named_ticker_the_grouped_file_omits_is_read_per_ticker_and_kept_apart(monkeypatch, capsys):
    """OTC symbols (GTBIF, MARUY, TELWY) are not in the grouped file (alpha-engine-config-I12023)."""
    import sys
    import types

    bar = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10, "vwap": 1.2}
    otc = {"open": 8.1, "high": 8.4, "low": 7.9, "close": 8.25, "volume": 31337.0, "vwap": None}
    asked = []

    class FakeClient:
        def get_grouped_daily(self, day):
            return {"AAA": bar}

        def get_single_day_bar(self, ticker, day):
            asked.append((ticker, day))
            return otc if ticker == "GTBIF" else None

    monkeypatch.setitem(sys.modules, "polygon_client", types.SimpleNamespace(PolygonClient=FakeClient))
    out = vendor_read.read_polygon(["2026-09-28"], {"AAA", "GTBIF", "ZZZ"})
    assert sorted(out["dates"]["2026-09-28"]) == ["AAA"]
    assert out["per_ticker"]["2026-09-28"] == {"GTBIF": [8.1, 8.4, 7.9, 8.25, 31337.0, None], "ZZZ": None}
    assert sorted(asked) == [("GTBIF", "2026-09-28"), ("ZZZ", "2026-09-28")]
    assert "1 of 2 read; no bar from either call: ['ZZZ']" in capsys.readouterr().out


def test_an_unfiltered_read_never_calls_the_per_ticker_endpoint(monkeypatch):
    import sys
    import types

    class FakeClient:
        def get_grouped_daily(self, day):
            return {"AAA": {"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}}

        def get_single_day_bar(self, ticker, day):
            raise AssertionError("no named ticker, so nothing is absent")

    monkeypatch.setitem(sys.modules, "polygon_client", types.SimpleNamespace(PolygonClient=FakeClient))
    assert vendor_read.read_polygon(["2026-09-28"])["per_ticker"] == {}
