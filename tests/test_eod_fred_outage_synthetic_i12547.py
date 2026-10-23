"""alpha-engine-config-I12547: a synthetic FRED outage driven through the EOD
harness, end to end, from the transport layer to the process exit code.

2026-10-21: FRED answered every observation request with a non-JSON body
(``Expecting value: line 1 column 1``). D23 (``metron_macro_data``) reported
``skipped``, ``_run_daily`` aggregated that to ``failed``, ``main()`` exited 1,
and the EOD Step Function never reached ``PostMarketArcticAppend`` -- so D32
did not append 10-21 and SPY went stale (the follow-on preflight stall was
nousergon-data#2251).

nousergon-data#2243 pinned the AGGREGATOR (``_aggregate_daily_status``) with
hand-written collector maps. These tests do not hand-write the outcome. They
inject the outage where it happened -- ``urllib.request.urlopen`` returning an
HTML body -- and let the real D23 code (``collect_macro`` ->
``_fred_series_history`` -> ``_fred_json``) decide what status it reports, then
let the real ``run_weekly`` -> ``_run_daily`` -> ``main()`` decide the exit
code. A regression anywhere on that path (D23 starting to raise instead of
returning ``skipped``, a rename that drops the phase out of
``_EOD_NON_HALTING_PHASES``, the exit mapping treating ``degraded`` as a
failure again) turns these red, where the aggregator unit tests would stay
green.

Every OTHER phase is stubbed ``ok`` so the outage is the only variable, and the
control test proves the harness still exits 1 when a core phase fails during
the same outage.
"""

from __future__ import annotations

import io
import urllib.request
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import weekly_collector as wc
from collectors import metron_market_data as mmd

RUN_DATE = "2026-10-21"
# Deliberately not key-shaped (nousergon-data#2252): gitleaks must not see a
# FRED-key-looking literal in a fixture.
SYNTHETIC_KEY = "synthetic-outage-not-a-key"
OUTAGE_BODY = b"<html><body>503 Service Temporarily Unavailable</body></html>"


class _OutageResponse(io.BytesIO):
    """What urlopen handed back on 10-21: a 200-ish response whose body is HTML."""

    status = 503
    reason = "Service Temporarily Unavailable"
    headers = {"Content-Type": "text/html"}

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@pytest.fixture
def fred_outage(monkeypatch):
    """Every FRED request answers with a non-JSON body; anything else is refused,
    so a test that reaches another host fails loudly instead of going online."""
    calls: list[str] = []

    def fake_urlopen(url, *args, **kwargs):
        target = url if isinstance(url, str) else url.full_url
        calls.append(target)
        if "api.stlouisfed.org" not in target:
            raise AssertionError(f"unexpected network call in the outage harness: {target}")
        return _OutageResponse(OUTAGE_BODY)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return calls


# ── D23 on its own: the outage is reported as `skipped`, nothing is written ──


def test_d23_reports_skipped_and_writes_nothing_under_the_outage(fred_outage):
    s3 = MagicMock()
    result = mmd.collect_macro(bucket="test-bucket", run_date=RUN_DATE, s3_client=s3,
                               api_key=SYNTHETIC_KEY)

    assert result["status"] == "skipped", result
    assert fred_outage, "the harness never reached FRED -- the outage was not exercised"
    assert all("api.stlouisfed.org" in u for u in fred_outage)
    s3.put_object.assert_not_called()


def test_the_outage_body_is_diagnosed_as_non_json(fred_outage):
    with pytest.raises(mmd.FredNonJsonResponse):
        mmd._fred_json(_OutageResponse(OUTAGE_BODY), SYNTHETIC_KEY)


# ── The EOD harness: run_weekly -> _run_daily -> main() exit code ──────────


def _ok(**extra):
    return lambda *a, **k: {"status": "ok", **extra}


def _eod_args(**overrides) -> SimpleNamespace:
    """PostMarketData's argv: ``--daily --skip-arctic-append --date <run_date>``.
    dry_run keeps the phase registry, health markers and every S3 write out."""
    base = dict(
        date=RUN_DATE, dry_run=True, daily=True, skip_arctic_append=True, only=None,
        morning_enrich=False, morning_arctic_append=False, daily_arctic_append=False,
        chronic_gap_heal=False, daily_heal=False, preflight_only=False,
        guard_skip_record=None, log_level="INFO", config=None, phase=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _Sink:
    def __init__(self):
        self.writes = []

    def write(self, key, record):
        self.writes.append((key, record))

    def __getattr__(self, name):  # any other sink method is a no-op recorder
        return lambda *a, **k: self.writes.append((name, a, k))


@pytest.fixture
def eod_harness(fred_outage, monkeypatch):
    """Stub every EOD phase EXCEPT D23 to ``ok``. D23 runs for real against the
    outage, with the FRED key resolved the way production resolves it."""
    monkeypatch.setattr(wc, "_load_daily_universe_tickers", lambda *a, **k: ["AAPL", "SPY"])
    monkeypatch.setattr(wc.daily_closes, "collect",
                        _ok(tickers_captured=2, polygon=0, fred=0, yfinance=2,
                            source="yfinance_only"))
    monkeypatch.setattr(wc.prices, "collect", _ok())
    for name in ("collect", "collect_history", "collect_reference",
                 "collect_fundamentals", "collect_technicals",
                 "collect_security_performance", "collect_analyst",
                 "collect_sentiment"):
        if hasattr(mmd, name):
            monkeypatch.setattr(mmd, name, _ok())
    monkeypatch.setattr(wc.technical_rating_ledger, "collect_rating_ledger", _ok())
    monkeypatch.setattr(wc.technical_rating_ledger, "collect_rating_performance", _ok())
    monkeypatch.setattr("features.compute.compute_and_write", _ok())
    # D23 resolves its key through nousergon_lib.secrets when none is injected,
    # exactly as on the box; the outage is in FRED, not in the key.
    monkeypatch.setattr("nousergon_lib.secrets.get_secret",
                        lambda name, *a, **k: SYNTHETIC_KEY if name == "FRED_API_KEY" else "")
    # D23 builds its own boto3 client before it fetches; nothing may reach AWS.
    monkeypatch.setattr("boto3.client", lambda *a, **k: MagicMock())
    return fred_outage


def _run_main(args) -> int:
    """Drive ``main()`` with the EOD argv and return its exit code (0 when it
    returns normally)."""
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    with patch.object(wc, "_parse_args", return_value=args), \
         patch.object(wc, "load_config", return_value=config), \
         patch("preflight.DataPreflight") as preflight, \
         patch.object(wc, "get_flow_doctor", return_value=None), \
         patch("run_units.manifest_sink", return_value=_Sink()):
        try:
            wc.main()
        except SystemExit as e:
            return int(e.code or 0)
    # The weekday producers take the `daily_write` preflight (nousergon-data#2251),
    # which logs a stale SPY instead of refusing -- the outage's second hop.
    assert preflight.call_args.args[1] == "daily_write"
    return 0


def test_eod_run_under_the_outage_is_degraded_with_d23_named(eod_harness):
    config = {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}}
    with patch("run_units.manifest_sink", return_value=_Sink()):
        results = wc.run_weekly(config, _eod_args())

    assert eod_harness, "D23 never called FRED -- the harness stubbed the phase under test"
    assert results["collectors"]["metron_macro_data"]["status"] == "skipped"
    assert results["status"] == "degraded"
    assert results["degraded_collectors"] == ["metron_macro_data"]
    others = {k: v["status"] for k, v in results["collectors"].items()
              if k != "metron_macro_data"}
    assert others and set(others.values()) <= {"ok", "ok_dry_run"}, others


def test_eod_process_exits_0_under_the_outage_so_the_arctic_append_runs(eod_harness):
    # Exit 0 is what lets the EOD SF reach PostMarketArcticAppend (D32).
    assert _run_main(_eod_args()) == 0
    assert eod_harness


def test_a_core_phase_failing_during_the_outage_still_exits_1(eod_harness, monkeypatch):
    # Control: the outage harness must not have made everything non-halting.
    monkeypatch.setattr(wc.daily_closes, "collect",
                        lambda *a, **k: {"status": "error", "error": "synthetic"})
    assert _run_main(_eod_args()) == 1
