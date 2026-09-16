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


# ---------------------------------------------------------------------------
# alpha-engine-config-I10893 — history fetches are bounded by the trading day.
#
# A ``--date D`` run executed on D+1 (retry, late rerun, backfill, shadow run)
# used to fetch vendor history with ``period="10y"`` and no ``start``/``end``,
# so the series ended at vendor "now" — including a PARTIAL, pre-close D+1
# session — and that bar was published as a settled close
# (``reference/price_cache/*.parquet``, ``market_data/fx_history/*.json``,
# measured 2026-09-15). The window start drifted with wall-clock time too.
#
# Point-in-time rule: every history fetch serving a run for D asks for
# ``[D − window, D]`` (``end`` exclusive at D + 1 calendar day), and every
# writer publishing such a series calls :func:`assert_no_bar_after` first,
# which RAISES — never trims — on a bar dated after D.
# ---------------------------------------------------------------------------


class FutureBarError(ValueError):
    """A series about to be published carries a bar dated after its run's
    trading day (alpha-engine-config-I10893). Subclasses ``ValueError`` and is
    deliberately re-raised through per-ticker ``except Exception`` blocks by
    every writer: it is a contract violation of the run, not a one-ticker
    vendor hiccup."""


def as_trading_day(trading_day: "date | datetime | str") -> "date":
    """Normalize an ISO string / ``date`` / ``datetime`` to a ``date``."""
    from datetime import date as _date

    if isinstance(trading_day, datetime):
        return trading_day.date()
    if isinstance(trading_day, _date):
        return trading_day
    return _date.fromisoformat(str(trading_day)[:10])


def history_window(
    trading_day: "date | datetime | str", period: str = "10y",
) -> "tuple[date, date]":
    """``(start, end_exclusive)`` for a history fetch serving trading day D.

    ``start`` is D minus ``period`` on the calendar (``"10y"`` → same month/day
    ten years earlier; ``"6mo"`` → months; ``"35d"`` → days), so the first bar
    is a pure function of D and never of wall-clock time. ``end_exclusive`` is
    D + 1 calendar day — the vendor convention (yfinance ``end``) is exclusive,
    and a calendar day (not the next session) is the tightest bound that still
    includes D: nothing dated after D is requestable.

    Raises ``ValueError`` on a period shape it cannot parse — a mistyped window
    must not silently become a different window on a producer.
    """
    import pandas as pd

    d = pd.Timestamp(as_trading_day(trading_day))
    p = str(period).strip()
    try:
        if p.endswith("mo"):
            start = d - pd.DateOffset(months=int(p[:-2]))
        elif p.endswith("y"):
            start = d - pd.DateOffset(years=int(p[:-1]))
        elif p.endswith("d"):
            start = d - pd.Timedelta(days=int(p[:-1]))
        else:
            raise ValueError(p)
    except ValueError as exc:
        raise ValueError(
            f"history_window: unparseable period {period!r} "
            "(expected '<n>y', '<n>mo' or '<n>d')"
        ) from exc
    return start.date(), (d + pd.Timedelta(days=1)).date()


def clip_to_trading_day(frame, trading_day: "date | datetime | str", *, label: str):
    """Drop rows dated after D from a FETCHED frame at the fetch boundary.

    The fetch already asks for ``end = D + 1`` (exclusive); a vendor that
    answers beyond the requested end is out of contract, so rows past D are
    removed here and the count is logged at WARNING (the recording surface).
    This is the request bound, applied to the response — it is NOT the write
    guard: writers still call :func:`assert_no_bar_after`, which raises.
    """
    import pandas as pd

    if frame is None or len(frame) == 0:
        return frame
    cutoff = pd.Timestamp(as_trading_day(trading_day))
    idx = pd.to_datetime(frame.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    keep = idx.normalize() <= cutoff
    dropped = int((~keep).sum())
    if dropped:
        log.warning(
            "%s: vendor returned %d row(s) dated after trading_day %s despite "
            "end-bound — dropped at the fetch boundary (alpha-engine-config-I10893)",
            label, dropped, cutoff.date().isoformat(),
        )
    return frame[keep]


def assert_no_bar_after(
    bar_dates, trading_day: "date | datetime | str", *, artifact: str,
) -> None:
    """Write-site guard: raise :class:`FutureBarError` when any bar in
    ``bar_dates`` (a DatetimeIndex, or an iterable of ISO strings / dates /
    timestamps) is dated after ``trading_day``. Raises, never trims — a
    publisher that reaches this with a future bar bypassed the fetch bound."""
    import pandas as pd

    d = as_trading_day(trading_day)
    values = list(bar_dates) if not isinstance(bar_dates, pd.Index) else bar_dates
    if len(values) == 0:
        return
    idx = pd.to_datetime(pd.Index(values))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    last = idx.max()
    if pd.isna(last) or last.date() <= d:
        return
    n_after = int((idx.normalize() > pd.Timestamp(d)).sum())
    raise FutureBarError(
        f"{artifact}: refusing to publish a series whose last bar "
        f"{last.date().isoformat()} is after the run's trading_day "
        f"{d.isoformat()} ({n_after} bar(s) after D). A run for D must never "
        "publish a later (possibly pre-close) session — see "
        "alpha-engine-config-I10893."
    )
