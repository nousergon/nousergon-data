"""Test fixtures + sys.path setup.

Pins ``ALPHA_ENGINE_SECRETS_SOURCE=env`` for every test so
``alpha_engine_lib.secrets.get_secret()`` reads from monkeypatched
env vars only — never the real SSM Parameter Store. Without this,
tests that simulate "missing API key" via ``monkeypatch.delenv``
would be silently no-op'd by a live SSM read. See
``alpha-engine-docs/private/env-to-ssm-260512.md`` § Risks.
"""

import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Hard-disable flow-doctor for the whole pytest session. The lib's
# PYTEST_CURRENT_TEST guard does not cover handler attachment at module
# IMPORT time (several tests import weekly_collector, whose module-top
# setup_logging runs during collection, before PYTEST_CURRENT_TEST is
# set) — on 2026-06-11 a local test run leaked REAL alert emails + GitHub
# issues + S3 changelog entries for synthetic fixture tickers (T0/T1/BAD).
# Must be set before any test module import, hence module level here, not
# a fixture.
os.environ.setdefault("FLOW_DOCTOR_DISABLED", "1")


@pytest.fixture(autouse=True)
def _isolate_secrets_from_ssm(monkeypatch):
    """Force every test to read secrets from env only, not SSM.

    Also clears the per-process secret cache before each test so a prior
    test's reads don't leak into the next test's state.
    """
    monkeypatch.setenv("ALPHA_ENGINE_SECRETS_SOURCE", "env")
    try:
        from alpha_engine_lib.secrets import clear_cache
    except ImportError:
        # Lib not installed (rare — tests that don't import secrets path).
        yield
        return
    clear_cache()
    yield
    clear_cache()


def recent_trading_day_str() -> str:
    """Most recent NYSE trading day as of now, ISO ``YYYY-MM-DD``.

    Shared chokepoint for date-driven ``daily_append`` tests. ``daily_append``
    enforces an NYSE-trading-day gate (config#1572: "refusing to append a
    phantom session") — a raw ``datetime.now()`` fed straight into
    ``date_str`` detonates every weekend and market holiday (surfaced
    2026-07-03, the observed Independence Day holiday: 7 tests red on
    ``main`` with "is not an NYSE trading day"). Anchoring to the most
    recent *trading* day keeps the date a valid session (passes the gate)
    AND within the freshness threshold (staleness is 0 trading days — the
    last row IS this date), so it neither rots (the 2026-05-04
    hardcoded-date failure) nor trips the phantom-session guard.

    Originally a test-local helper in
    ``tests/test_daily_append_skip_if_exists.py`` (PR #599); promoted here
    so any new date-driven ``daily_append`` test has a single discoverable
    chokepoint instead of hand-rolling ``datetime.now()`` again (config#1630).

    Do NOT use this for the intentionally hardcoded scenario tests
    (``test_daily_append_writer_lock.py``, ``test_daily_append_macro_monotonic.py``,
    ``test_daily_append_missing_from_closes.py``,
    ``test_daily_append_trading_day_gate.py``) — those pin specific
    historical/holiday dates on purpose to exercise fixed scenarios, and
    migrating them to a live anchor would destroy what they assert.
    """
    from nousergon_lib.trading_calendar import is_trading_day, previous_trading_day

    d = datetime.now(timezone.utc).date()
    if not is_trading_day(d):
        d = previous_trading_day(d)
    return d.isoformat()


@pytest.fixture
def recent_trading_day() -> str:
    """Pytest fixture wrapping :func:`recent_trading_day_str`.

    Returns the most recent NYSE trading day as an ISO ``YYYY-MM-DD``
    string — a valid ``daily_append(date_str=...)`` anchor on any day the
    suite runs (weekday, weekend, or market holiday). Prefer this fixture
    for new tests; call the module-level function directly only when a
    fixture isn't ergonomic (e.g. inside a helper that isn't itself a
    test function).
    """
    return recent_trading_day_str()
