"""Verify _finalize surfaces per-collector errors to logging.ERROR.

Surfaced 2026-05-09 from a Saturday SF DataPhase1 PARTIAL run where the
arcticdb backfill regression preflight failure was stored in the result
dict but never logged at ERROR level — only main()'s generic "non-ok
status" summary fired, which produces a single dedup signature across
every partial run and contains no actual error text for Flow Doctor's
LLM diagnose pipeline to work with.

Fix: ``_finalize`` now calls
``alpha_engine_lib.collector_results.report_collector_errors(results["collectors"])``
which emits one ``logger.error()`` per error-status entry with the
collector name + original message. This wiring test pins that call site
so a future refactor can't silently drop it.
"""

from __future__ import annotations

import logging

import pytest

from weekly_collector import _finalize


def test_finalize_logs_each_collector_error(caplog: pytest.LogCaptureFixture):
    """Per-collector error messages are visible at ERROR level.

    Two distinct collector failures must produce two distinct ERROR
    records — Flow Doctor's dedup keys off the rendered message, so
    one alert per failure is the load-bearing property.
    """
    results = {
        "phase": 1,
        "collectors": {
            "constituents": {"status": "ok"},
            "arcticdb": {
                "status": "error",
                "error": "Backfill regression preflight failed: 38 symbols would regress",
            },
            "fundamentals": {"status": "error", "error": "Polygon 429 rate limit"},
        },
    }
    with caplog.at_level(logging.ERROR):
        _finalize(
            results,
            bucket="test-bucket",
            market_prefix="market_data/",
            run_date="2026-05-09",
            dry_run=True,  # skip _write_manifest / _write_validation_json + postflight
            only=None,
        )

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    messages = [r.getMessage() for r in error_records]

    assert any(
        "collector arcticdb failed: Backfill regression preflight failed" in m
        for m in messages
    ), f"arcticdb error not logged at ERROR. messages={messages}"
    assert any(
        "collector fundamentals failed: Polygon 429" in m for m in messages
    ), f"fundamentals error not logged at ERROR. messages={messages}"
    assert results["status"] == "partial"


def test_finalize_silent_on_all_ok(caplog: pytest.LogCaptureFixture):
    """All-ok run must not emit any ERROR-level records.

    Spurious ERROR logs would dedup-spam Flow Doctor's daily caps.
    """
    results = {
        "phase": 1,
        "collectors": {
            "constituents": {"status": "ok"},
            "prices": {"status": "ok"},
            "macro": {"status": "ok_dry_run"},
        },
    }
    with caplog.at_level(logging.ERROR):
        _finalize(
            results,
            bucket="test-bucket",
            market_prefix="market_data/",
            run_date="2026-05-09",
            dry_run=True,
            only=None,
        )

    assert results["status"] == "ok"
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records == [], f"unexpected ERROR records on all-ok run: {error_records}"
