"""dates.py — repo-local chokepoint for default artifact-keying dates.

config#1014 (trading-day axis migration: alpha-engine-data weekly/daily
collector keys). Every collector / builder entrypoint historically defaulted
its ``run_date`` / ``--date`` to the *calendar* date::

    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

On a Saturday (or any non-trading day / pre-open weekday morning) the calendar
date is NOT the session the data is about, so the artifact gets keyed on the
wrong axis (e.g. ``market_data/weekly/2026-06-27/`` on a Saturday instead of
Friday's ``2026-06-26/`` close). Producers and consumers currently *agree* on
calendar keying, so this module is the single place the default is migrated to
the trading-day axis — mirroring the predictor fix (crucible-predictor#289,
config#1015) which routed ``train_handler``'s default through
``nousergon_lib.dates.now_dual().trading_day``.

Root-cause, not band-aid: rather than 12 scattered ``now_dual()`` patches with
12 copies of the try/except fallback, every default-date site calls
``default_run_date()`` here. The lib chokepoint (``now_dual``) lives in
``nousergon_lib.dates`` and is reachable from this repo at the pinned
``nousergon-lib@v0.59.4`` (requirements.txt) — so this is a clean import, no
lib release/pin bump required.

Backward-compat: when an explicit ``--date`` / ``run_date`` is passed (the SF
production path threads one via ``$.run_date`` / ``RUN_DATE``), this helper is
NOT consulted — behaviour is unchanged. The trading-day default only takes
effect on the manual / ad-hoc / daily-cron path that previously fell through to
calendar ``now()``. No historical artifact is re-keyed or orphaned.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def default_run_date(now: datetime | None = None) -> str:
    """Resolve the default artifact-keying date on the trading-day axis.

    Returns the last *closed* NYSE session as an ISO ``YYYY-MM-DD`` string via
    the fleet-canonical ``nousergon_lib.dates.now_dual().trading_day``
    chokepoint. Falls back to the calendar UTC date only if the lib lookup
    raises — date defaulting must never block a collection run.

    Args:
        now: optional timezone-aware moment (mainly for tests). Defaults to
            current UTC time inside ``now_dual``.

    Returns:
        ISO ``YYYY-MM-DD`` string. On a non-trading day this is the most recent
        session whose 4:00 PM ET close has occurred (e.g. Saturday -> Friday).
    """
    try:
        from nousergon_lib.dates import now_dual

        dd = now_dual(now=now) if now is not None else now_dual()
        log.info(
            "default_run_date: resolved trading_day=%s (calendar=%s)",
            dd.trading_day,
            dd.calendar_date,
        )
        return dd.trading_day
    except Exception:  # noqa: BLE001 — date defaulting must not block a run
        ref = now or datetime.now(timezone.utc)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        fallback = ref.astimezone(timezone.utc).strftime("%Y-%m-%d")
        log.warning(
            "default_run_date: could not resolve trading_day via "
            "nousergon_lib.dates.now_dual; fell back to calendar date %s",
            fallback,
            exc_info=True,
        )
        return fallback
