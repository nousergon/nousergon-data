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
