"""Trading-day axis for default collector/builder artifact keys (config#1014).

The weekly/daily collector chain historically defaulted ``run_date`` /
``--date`` to the *calendar* UTC date::

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

On a Saturday (the weekly SF firing day) the calendar date is NOT a trading
session, so artifacts mis-keyed to Saturday instead of Friday's close
(``market_data/weekly/{Sat}/``, ``staging/daily_closes/{Sat}.parquet``,
``features/{Sat}/schema_version.json``, the ArcticDB macro-index bar, ...).

This pins the root-cause fix: a single repo chokepoint, ``dates.default_run_date()``,
which routes the default through the fleet-canonical
``nousergon_lib.dates.now_dual().trading_day`` (mirroring the predictor
fix crucible-predictor#289 / config#1015). The chokepoint is reached by every
``... or default_run_date()`` / ``if run_date is None`` default site.

Backward-compat contract (also pinned here): when an explicit date is passed
(the SF production path threads ``$.run_date`` / ``RUN_DATE``), the helper is
NOT consulted — historical artifacts are never re-keyed.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import dates


# Saturday 2026-06-27; Friday 2026-06-26 is the last closed NYSE session.
_SATURDAY = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc)
_FRIDAY_ISO = "2026-06-26"
# Sunday 2026-06-28 also attributes back to Friday 2026-06-26.
_SUNDAY = datetime(2026, 6, 28, 23, 0, tzinfo=timezone.utc)


def test_saturday_default_run_date_keys_to_friday():
    """The core defect: a Saturday default must resolve to Friday's session,
    not Saturday's calendar date."""
    assert dates.default_run_date(now=_SATURDAY) == _FRIDAY_ISO


def test_sunday_default_run_date_keys_to_friday():
    assert dates.default_run_date(now=_SUNDAY) == _FRIDAY_ISO


def test_weekday_after_close_keys_to_that_session():
    """A weekday after the 4pm ET close attributes to that day's session."""
    # Friday 2026-06-26 21:00 UTC = 17:00 ET, after the close.
    fri_after_close = datetime(2026, 6, 26, 21, 0, tzinfo=timezone.utc)
    assert dates.default_run_date(now=fri_after_close) == _FRIDAY_ISO


def test_returns_iso_string():
    out = dates.default_run_date(now=_SATURDAY)
    assert isinstance(out, str)
    # ISO YYYY-MM-DD round-trips.
    datetime.strptime(out, "%Y-%m-%d")


def test_fallback_to_calendar_on_lib_failure(monkeypatch):
    """Date defaulting must never block a run: if the lib lookup raises, fall
    back to the calendar UTC date (the prior behaviour) rather than crashing."""
    import nousergon_lib.dates as lib_dates

    def _boom(*a, **k):
        raise RuntimeError("simulated trading-calendar outage")

    monkeypatch.setattr(lib_dates, "now_dual", _boom)
    # Saturday calendar date is returned as the documented fallback.
    assert dates.default_run_date(now=_SATURDAY) == "2026-06-27"


def test_no_now_argument_does_not_raise():
    """Smoke: zero-arg call (the real production signature) returns a valid
    ISO date for 'now'."""
    out = dates.default_run_date()
    datetime.strptime(out, "%Y-%m-%d")


# ── Default-site wiring: each entrypoint routes its None-default through the
#    chokepoint (so a Saturday run keys to Friday). We assert the wiring at the
#    source level rather than booting the heavy ArcticDB/S3 collectors. ──────


@pytest.mark.parametrize(
    "module_path, marker",
    [
        ("weekly_collector.py", "args.date or default_run_date()"),
        ("collectors/daily_closes.py", "default_run_date()"),
        ("collectors/metron_market_data.py", "default_run_date()"),
        ("collectors/macro.py", "default_run_date()"),
        ("collectors/short_interest.py", "default_run_date()"),
        ("collectors/alternative.py", "default_run_date()"),
        ("builders/daily_append.py", "default_run_date()"),
        ("features/compute.py", "default_run_date()"),
    ],
)
def test_entrypoint_routes_default_through_chokepoint(module_path, marker):
    import pathlib

    repo = pathlib.Path(__file__).resolve().parent.parent
    src = (repo / module_path).read_text()
    assert marker in src, (
        f"{module_path} no longer routes its default date through "
        f"dates.default_run_date() — config#1014 regression."
    )


def test_no_calendar_now_default_remains_in_migrated_sites():
    """Regression pin: none of the migrated entrypoints may reintroduce a
    calendar ``... or datetime.now(timezone.utc).strftime(\"%Y-%m-%d\")``
    default for run_date/date_str."""
    import pathlib

    repo = pathlib.Path(__file__).resolve().parent.parent
    bad = 'or datetime.now(timezone.utc).strftime("%Y-%m-%d")'
    offenders = []
    for module_path in [
        "weekly_collector.py",
        "collectors/daily_closes.py",
        "collectors/metron_market_data.py",
        "collectors/macro.py",
        "collectors/short_interest.py",
        "collectors/alternative.py",
        "builders/daily_append.py",
        "features/compute.py",
    ]:
        src = (repo / module_path).read_text()
        for i, line in enumerate(src.splitlines(), 1):
            stripped = line.strip()
            if bad in stripped and ("run_date" in stripped or "date_str" in stripped):
                offenders.append(f"{module_path}:{i}: {stripped}")
    assert not offenders, (
        "Calendar-now default reintroduced at a migrated artifact-keying "
        "site (config#1014). Route through dates.default_run_date():\n"
        + "\n".join(offenders)
    )
