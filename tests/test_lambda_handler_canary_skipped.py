"""Regression tests for lambda/handler.py canary contract — `skipped` ≠ ERROR.

Locks the 2026-05-24 incident invariant: when the data-collector Lambda is
invoked with `dry_run=true` (the canary path in `infrastructure/deploy.sh`)
and the underlying `alternative.collect()` returns
`{"status": "skipped", "reason": "no tickers"}` because signals.json hasn't
been populated yet for `run_date`, the handler MUST map this to canary-status
`SKIPPED` — NOT collapse into `ERROR`.

Why: deploy.sh's canary contract is to verify Lambda boot + S3 read after a
new image lands. "No tickers" on a `dry_run=true` invocation says nothing
about Lambda health; it's an upstream-data state. The previous behavior
(`else: ERROR`) caused a perfectly-working Lambda v101 to be auto-rolled-back
on a Sunday deploy because Sunday's `run_date` defaulted to "today" with no
signals.json yet emitted.

Production semantics are preserved: when `dry_run=False` and status is
`skipped`, the handler still returns ERROR so the Saturday SF's DataPhase2
state correctly surfaces an upstream Research-output failure.

`lambda` is a Python keyword so the handler can only be imported via
`importlib.import_module`. The lambda/ directory is a PEP-420 namespace
package (no __init__.py); namespace-package submodule resolution works.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch


def _load_handler():
    return importlib.import_module("lambda.handler").handler


def _stub_get_secret(name, required=False, default=""):
    """Return non-empty for the two required keys so handler boot proceeds."""
    if name in ("FMP_API_KEY", "FINNHUB_API_KEY"):
        return "stub"
    return default


def _invoke_with_collect_result(collect_result, dry_run: bool):
    """Invoke the handler with the collector mocked to return `collect_result`."""
    fake_alternative = MagicMock()
    fake_alternative.collect.return_value = collect_result
    fake_collectors = MagicMock(alternative=fake_alternative)

    with patch.dict(
        sys.modules,
        {"collectors": fake_collectors, "collectors.alternative": fake_alternative},
    ), patch("nousergon_lib.secrets.get_secret", side_effect=_stub_get_secret):
        handler = _load_handler()
        return handler({"phase": 2, "dry_run": dry_run}, None)


def test_canary_skipped_with_dry_run_returns_SKIPPED_not_ERROR():
    """Canary path: dry_run=True + collect=skipped → handler returns SKIPPED.

    deploy.sh's canary parser (infrastructure/deploy.sh:122) already accepts
    `s in ('OK', 'SKIPPED')` as canary-OK, so this maps the upstream no-op
    cleanly without requiring a deploy.sh change.
    """
    result = _invoke_with_collect_result(
        {"status": "skipped", "reason": "no tickers"}, dry_run=True
    )
    assert result["status"] == "SKIPPED", (
        f"Canary dry_run=True with no tickers must return SKIPPED, got: {result}"
    )
    assert result.get("skip_reason") == "no tickers"
    assert result["dry_run"] is True


def test_production_skipped_without_dry_run_still_returns_ERROR():
    """Production path: dry_run=False + collect=skipped → handler returns ERROR.

    Preserves the Saturday-SF DataPhase2 contract: if Research correctly ran
    but emitted zero promoted tickers, that IS a real failure that must
    surface as ERROR (not get silently dropped per [[feedback_no_silent_fails]]).
    """
    result = _invoke_with_collect_result(
        {"status": "skipped", "reason": "no tickers"}, dry_run=False
    )
    assert result["status"] == "ERROR", (
        f"Production dry_run=False with no tickers must return ERROR, got: {result}"
    )
    assert "no tickers" in result.get("error", "")


def test_dry_run_with_tickers_still_returns_OK():
    """Sanity: dry_run=True + collect=ok_dry_run (tickers present) → OK.

    Confirms the new SKIPPED branch doesn't shadow the existing OK path
    when collect() returns the normal dry-run success status.
    """
    result = _invoke_with_collect_result(
        {"status": "ok_dry_run", "tickers": 25, "ticker_list": ["AAPL", "MSFT"]},
        dry_run=True,
    )
    assert result["status"] == "OK", (
        f"Canary dry_run=True with tickers must return OK, got: {result}"
    )
    assert result["dry_run"] is True
