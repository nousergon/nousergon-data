"""Regression tests for lambda/handler.py canary contract — `skipped` ≠ ERROR.

Locks the 2026-05-24 incident invariant: when the data-collector Lambda is
invoked with `dry_run=true` (the canary path in `infrastructure/deploy.sh`)
and the underlying `alternative.collect()` returns
`{"status": "skipped", "reason": "no tickers"}` because the upstream
artifact hasn't been populated yet for `run_date`, the handler MUST map
this to canary-status `SKIPPED` — NOT collapse into `ERROR`.

Why: deploy.sh's canary contract is to verify Lambda boot + S3 read after a
new image lands. "No tickers" on a `dry_run=true` invocation says nothing
about Lambda health; it's an upstream-data state. The previous behavior
(`else: ERROR`) caused a perfectly-working Lambda v101 to be auto-rolled-back
on a Sunday deploy because Sunday's `run_date` defaulted to "today" with no
signals.json yet emitted.

Production semantics are preserved: when `dry_run=False` and status is
`skipped`, the handler still returns ERROR so the Saturday SF's DataPhase2
state correctly surfaces an upstream Research-output failure.

Scope contract (alpha-engine-config#6508): since PR #1186 repointed the SF's
DataPhase2 state to EC2-spot `weekly_collector.py --phase 2`, this handler is
a manual/ad-hoc entry point only, and it MUST resolve the ticker list from
constituents.json exactly like the production path — never the deprecated
signals.json::universe resolver. These tests pin that the handler always
passes an explicit `tickers=` list to `alternative.collect()` (the fallback
is reachable only when `tickers` is None), and that a missing constituents
artifact fails loud in production while staying canary-safe for dry_run.

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


def _invoke_with_collect_result(
    collect_result,
    dry_run: bool,
    constituents_result: dict | None = {"tickers": ["AAPL", "MSFT"]},
):
    """Invoke the handler with the collector mocked to return `collect_result`.

    `constituents_result` is what `constituents.load_from_s3` returns; pass
    None to exercise the no-constituents path (tickers resolves to []).
    """
    fake_alternative = MagicMock()
    fake_alternative.collect.return_value = collect_result
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = constituents_result
    fake_collectors = MagicMock(
        alternative=fake_alternative, constituents=fake_constituents
    )

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


# ---------------------------------------------------------------------------
# Scope contract (alpha-engine-config#6508) — constituents, never the
# deprecated signals.json::universe fallback
# ---------------------------------------------------------------------------


def test_collect_always_called_with_explicit_constituent_tickers():
    """The handler passes the resolved constituents list to collect().

    `alternative.collect` falls into the deprecated `_load_promoted_tickers`
    resolver ONLY when `tickers` is None (alpha-engine-config#6508); this
    asserts the handler can never do that — a manual `aws lambda invoke`
    behaves exactly like the production `weekly_collector._run_phase2` path.
    """
    fake_alternative = MagicMock()
    fake_alternative.collect.return_value = {
        "status": "ok",
        "tickers_processed": 2,
        "tickers_failed": 0,
    }
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {
        "tickers": ["AAPL", "MSFT", "NVDA"],
    }
    fake_collectors = MagicMock(
        alternative=fake_alternative, constituents=fake_constituents
    )

    with patch.dict(
        sys.modules,
        {"collectors": fake_collectors, "collectors.alternative": fake_alternative},
    ), patch("nousergon_lib.secrets.get_secret", side_effect=_stub_get_secret):
        handler = _load_handler()
        result = handler({"phase": 2, "dry_run": False}, None)

    assert result["status"] == "OK"
    fake_alternative.collect.assert_called_once()
    _, kwargs = fake_alternative.collect.call_args
    assert kwargs.get("tickers") == ["AAPL", "MSFT", "NVDA"], (
        f"collect() must be called with the constituents tickers, got: {kwargs}"
    )


def test_dry_run_no_constituents_returns_SKIPPED_without_collect():
    """dry_run=True with constituents.json missing → SKIPPED, collect not called.

    Canary-safe: the code booted and the S3 read worked; the upstream
    artifact is what's missing. Same contract as the post-collect skipped
    branch, but earlier — and without ever touching the deprecated
    signals.json resolver.
    """
    fake_alternative = MagicMock()
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = None
    fake_collectors = MagicMock(
        alternative=fake_alternative, constituents=fake_constituents
    )

    with patch.dict(
        sys.modules,
        {"collectors": fake_collectors, "collectors.alternative": fake_alternative},
    ), patch("nousergon_lib.secrets.get_secret", side_effect=_stub_get_secret):
        handler = _load_handler()
        result = handler({"phase": 2, "dry_run": True}, None)

    assert result["status"] == "SKIPPED"
    assert result.get("skip_reason") == "no constituents"
    fake_alternative.collect.assert_not_called()


def test_production_no_constituents_returns_ERROR_without_collect():
    """dry_run=False with constituents.json missing → ERROR, collect not called.

    Mirrors `weekly_collector._run_phase2`'s fail-loud stance: collecting
    against an implicit ticker list is exactly the silent scope move the
    deprecated fallback enabled (alpha-engine-config-I5814) — refuse it.
    """
    fake_alternative = MagicMock()
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {}
    fake_collectors = MagicMock(
        alternative=fake_alternative, constituents=fake_constituents
    )

    with patch.dict(
        sys.modules,
        {"collectors": fake_collectors, "collectors.alternative": fake_alternative},
    ), patch("nousergon_lib.secrets.get_secret", side_effect=_stub_get_secret):
        handler = _load_handler()
        result = handler({"phase": 2, "dry_run": False}, None)

    assert result["status"] == "ERROR"
    assert "constituents.json" in result.get("error", "")
    fake_alternative.collect.assert_not_called()
