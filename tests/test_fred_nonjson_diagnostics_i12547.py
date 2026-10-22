"""A non-JSON FRED response names what came back (alpha-engine-config-I12547).

On 2026-10-21 22:17-23:46Z every FRED call returned non-JSON and D23
(``metron_macro_data``) logged only ``Expecting value: line 1 column 1 (char 0)``
seven times — nothing to tell an outage page from a throttle from a truncated
payload. The warning now carries status, content-type and length, plus a short
key-redacted prefix for an HTML/plain-text error page only. This repo is public:
the API key, the request URL and any body that could be vendor data never reach
the log.
"""

from __future__ import annotations

import datetime as dt
import logging
import urllib.request

import pytest

from collectors import metron_market_data as mmd

# Deliberately low-entropy and not key-shaped: a literal 32-hex value here
# trips gitleaks' generic-api-key rule on this public repo (I12547 fixture).
KEY = "not-a-real-fred-key-" + "x" * 12


class _Headers(dict):
    def get(self, name, default=None):  # case-insensitive like http.client's
        for k, v in self.items():
            if k.lower() == name.lower():
                return v
        return default


class _Response:
    def __init__(self, body: bytes, content_type: str | None, status: int = 200):
        self._body = body
        self.status = status
        self.headers = _Headers({"Content-Type": content_type} if content_type else {})

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


def _serve(monkeypatch, response: _Response) -> None:
    monkeypatch.setattr(urllib.request, "urlopen", lambda url, timeout=None: response)


def _warnings(caplog) -> str:
    return "\n".join(r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING)


def test_html_error_page_logs_status_type_length_and_a_capped_redacted_prefix(monkeypatch, caplog):
    page = (
        b"<html><head><title>503 Service Temporarily Unavailable</title></head>\n<body>"
        b"request /fred/series/observations?series_id=DGS10&api_key=" + KEY.encode()
        + b"&file_type=json failed " + b"x" * 1000 + b"</body></html>"
    )
    _serve(monkeypatch, _Response(page, "text/html; charset=utf-8"))
    with caplog.at_level(logging.WARNING):
        assert mmd._fred_series_history(["DGS10"], dt.date(2026, 10, 21), KEY) == {}

    msg = _warnings(caplog)
    assert "FRED fetch failed for DGS10" in msg
    assert "status=200" in msg
    assert "content-type='text/html; charset=utf-8'" in msg
    assert f"length={len(page)}" in msg
    assert "503 Service Temporarily Unavailable" in msg
    assert KEY not in msg
    assert "api_key=<redacted>" in msg
    prefix = msg.split("body_prefix=", 1)[1]
    assert len(prefix) <= mmd.FRED_ERROR_BODY_PREFIX_CHARS + 2  # the repr's quotes
    assert "x" * 300 not in msg


@pytest.mark.parametrize("content_type", ["application/json", "text/xml", None])
def test_a_body_that_could_be_data_logs_no_content(monkeypatch, caplog, content_type):
    truncated = b'{"observations": [{"date": "2026-10-20", "value": "4.0123"}, {"date'
    _serve(monkeypatch, _Response(truncated, content_type))
    with caplog.at_level(logging.WARNING):
        assert mmd._fred_series_history(["DGS10"], dt.date(2026, 10, 21), KEY) == {}

    msg = _warnings(caplog)
    assert f"length={len(truncated)}" in msg
    assert "status=200" in msg
    assert "body_prefix" not in msg
    assert "4.0123" not in msg and "2026-10-20" not in msg
    assert KEY not in msg


def test_the_request_url_never_reaches_the_log(monkeypatch, caplog):
    _serve(monkeypatch, _Response(b"Too Many Requests", "text/plain", status=200))
    with caplog.at_level(logging.WARNING):
        mmd._fred_series_history(["DGS10"], dt.date(2026, 10, 21), KEY)

    msg = _warnings(caplog)
    assert "Too Many Requests" in msg
    assert "stlouisfed" not in msg
    assert "observation_start" not in msg
    assert KEY not in msg


def test_publication_evidence_shares_the_diagnostics(monkeypatch, caplog):
    _serve(monkeypatch, _Response(b"<html>maintenance</html>", "text/html", status=200))
    series = {"DGS10": [("2026-10-20", 4.0)]}
    with caplog.at_level(logging.WARNING):
        assert mmd._fred_publication_evidence(series, KEY) == {}

    msg = _warnings(caplog)
    assert "first-release dates unavailable for DGS10" in msg
    assert "content-type='text/html'" in msg and "maintenance" in msg
    assert KEY not in msg


def test_valid_json_is_unchanged(monkeypatch):
    body = b'{"observations": [{"date": "2026-10-20", "value": "4.01"}, {"date": "2026-10-21", "value": "."}]}'
    _serve(monkeypatch, _Response(body, "application/json"))
    assert mmd._fred_series_history(["DGS10"], dt.date(2026, 10, 21), KEY) == {
        "DGS10": [("2026-10-20", 4.01)]
    }


def test_a_response_without_status_or_headers_still_describes_itself():
    """The existing test doubles carry only ``read()``; the helper must not crash on them."""

    class _Bare:
        def read(self):
            return b"<html>"

    with pytest.raises(mmd.FredNonJsonResponse, match=r"status=None content-type='unknown' length=6$"):
        mmd._fred_json(_Bare(), KEY)
