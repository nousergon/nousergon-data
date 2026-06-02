"""Regression tests pinning api_key scrubbing on FRED-fetch error paths.

A FRED 500 during MorningEnrich or the repair script propagates a
``requests.exceptions.HTTPError`` whose ``str()`` embeds the full
request URL, including ``api_key=<live-credential>`` in the
querystring. Logging that to CloudWatch is a credential leak.

Surfaced 2026-05-12 during the post-merge repair run for PR #219 — a
transient FRED 500 on VXVCLS dumped the key to the operator terminal /
conversation transcript.

These tests lock the contract that:

  - ``_scrub_api_key`` masks the ``api_key=...`` fragment in any string
  - ``_fetch_fred_closes`` (production daily-pipeline fetcher) routes
    exceptions through the scrubber before logging
  - ``_fetch_fred_range`` (repair-script fetcher) does the same on its
    retry and final-failure paths
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import patch

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors import daily_closes
from collectors import daily_closes_fred_repair as repair_mod


# ── _scrub_api_key ─────────────────────────────────────────────────────────


def test_scrub_masks_api_key_in_url():
    msg = (
        "500 Server Error: Internal Server Error for url: "
        "https://api.stlouisfed.org/fred/series/observations"
        "?series_id=VIXCLS&api_key=4509846484a78c3ee667a118d5179de7"
        "&file_type=json"
    )
    scrubbed = daily_closes._scrub_api_key(msg)
    assert "4509846484a78c3ee667a118d5179de7" not in scrubbed
    assert "api_key=***" in scrubbed


def test_scrub_handles_exception_object_directly():
    """The helper must accept an exception object (not just str) — that's
    how it'll be invoked at the ``logger.warning("... %s", e)`` site."""
    try:
        resp = requests.Response()
        resp.status_code = 500
        resp.url = (
            "https://api.stlouisfed.org/fred/series/observations"
            "?series_id=VIXCLS&api_key=SECRETVALUEXYZ&file_type=json"
        )
        resp.reason = "Internal Server Error"
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        scrubbed = daily_closes._scrub_api_key(e)

    assert "SECRETVALUEXYZ" not in scrubbed
    assert "api_key=***" in scrubbed


def test_scrub_passthrough_when_no_api_key():
    msg = "FRED returned empty observations for VIXCLS"
    assert daily_closes._scrub_api_key(msg) == msg


def test_scrub_terminates_at_ampersand_not_eating_other_querystring():
    """Make sure the regex stops at ``&`` so it doesn't eat unrelated
    querystring fragments after the key (e.g. ``&file_type=json``)."""
    msg = "url: https://x/?api_key=SECRET&file_type=json"
    scrubbed = daily_closes._scrub_api_key(msg)
    assert scrubbed == "url: https://x/?api_key=***&file_type=json"


# ── _fetch_fred_closes error-log path ──────────────────────────────────────


def test_fetch_fred_closes_scrubs_api_key_on_http_error(
    monkeypatch, caplog,
):
    """The production daily-pipeline fetcher's except-block must not log
    the raw HTTPError string (which contains api_key=...)."""
    monkeypatch.setenv("FRED_API_KEY", "live-test-key-1234567890")

    class _ExplodingResponse:
        status_code = 200  # L4480: _fred_get_with_retry reads status_code first
        headers: dict = {}

        def raise_for_status(self):
            err_msg = (
                "500 Server Error: Internal Server Error for url: "
                "https://api.stlouisfed.org/fred/series/observations"
                "?series_id=VIXCLS&api_key=live-test-key-1234567890"
                "&file_type=json&observation_end=2026-05-12"
                "&sort_order=desc&limit=5"
            )
            raise requests.exceptions.HTTPError(err_msg)

    records: list[dict] = []
    with caplog.at_level(logging.WARNING, logger="collectors.daily_closes"):
        with patch(
            "collectors.daily_closes.requests.get",
            return_value=_ExplodingResponse(),
        ):
            daily_closes._fetch_fred_closes(
                tickers=["VIX"],
                date_str="2026-05-12",
                records=records,
            )

    assert records == []
    combined = "\n".join(rec.message for rec in caplog.records)
    assert "live-test-key-1234567890" not in combined
    assert "api_key=***" in combined


# ── _fetch_fred_range retry + final-error path ─────────────────────────────


def test_fetch_fred_range_scrubs_api_key_on_retry_log(monkeypatch, caplog):
    """First-attempt 500 must log via scrubbed retry warning, never the
    raw URL with api_key=..."""
    class _ExplodingResponse:
        def __init__(self):
            self.calls = 0

        def raise_for_status(self):
            self.calls += 1
            if self.calls < 2:
                raise requests.exceptions.HTTPError(
                    "500 Server Error for url: "
                    "https://api.stlouisfed.org/fred/?series_id=DGS10"
                    "&api_key=secret-leak-test-abc&file_type=json"
                )

        def json(self):
            return {"observations": [{"date": "2026-04-22", "value": "4.34"}]}

    resp = _ExplodingResponse()

    with caplog.at_level(logging.WARNING, logger="collectors.daily_closes_fred_repair"):
        with patch(
            "collectors.daily_closes_fred_repair.requests.get",
            return_value=resp,
        ), patch("collectors.daily_closes_fred_repair.time.sleep"):
            out = repair_mod._fetch_fred_range(
                series_id="DGS10",
                start="2026-04-15",
                end="2026-04-22",
                api_key="secret-leak-test-abc",
            )

    assert out == {"2026-04-22": 4.34}
    combined = "\n".join(rec.message for rec in caplog.records)
    assert "secret-leak-test-abc" not in combined
    assert "api_key=***" in combined


def test_fetch_fred_range_scrubs_api_key_on_final_failure(monkeypatch, caplog):
    """After 3 failed attempts, the RuntimeError + the final ``logger.error``
    line must both be scrubbed — that's the surface that bubbles into
    operator terminals."""
    class _ExplodingResponse:
        def raise_for_status(self):
            raise requests.exceptions.HTTPError(
                "500 Server Error for url: "
                "https://api.stlouisfed.org/fred/?series_id=DGS10"
                "&api_key=secret-final-failure-xyz&file_type=json"
            )

    with caplog.at_level(logging.ERROR, logger="collectors.daily_closes_fred_repair"):
        with patch(
            "collectors.daily_closes_fred_repair.requests.get",
            return_value=_ExplodingResponse(),
        ), patch("collectors.daily_closes_fred_repair.time.sleep"):
            with pytest.raises(RuntimeError) as excinfo:
                repair_mod._fetch_fred_range(
                    series_id="DGS10",
                    start="2026-04-15",
                    end="2026-04-22",
                    api_key="secret-final-failure-xyz",
                )

    assert "secret-final-failure-xyz" not in str(excinfo.value)
    assert "api_key=***" in str(excinfo.value)
    combined = "\n".join(rec.message for rec in caplog.records)
    assert "secret-final-failure-xyz" not in combined
