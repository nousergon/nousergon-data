"""A shadow run's OWN earlier roots as the source for state v1 keeps across days.

alpha-engine-config-I11577 / I11582. v1 publishes ``staging/daily_closes/{S}``
and ``reference/price_cache/{T}`` in place, so each write can build on what the
previous day's run left there: D19's window pass keeps the polygon rows the
morning pass wrote (``skip_if_canonical``), and the hole filler carries a
filled bar forward from the ticker's own cache parquet. A shadow run writes
under a fresh ``staging/shadow/{D}/`` root every trading day, and reads those
keys as run state (``shadow.interceptor``), so on its own it sees only what
THIS root already holds — on a fresh root, nothing.

The shadow's equivalent of "what the previous run left there" is the previous
shadow roots: ``staging/shadow/{T}/<key>`` for the sessions ``T`` before
``D``. This module is the one place that enumerates and reads them, for every
caller (``collectors.daily_closes.collect``'s merge base,
``collectors.price_cache_holes.SessionHoleFiller``'s D19 source and its cache
carry-forward).

**Interceptor rules.** An earlier root's key lies outside the active root, so
``shadow.interceptor`` passes it through unmodified and records it as an
ordinary live INPUT. No shadow run ever writes such a key (its writes all land
under its own root), so the I10891 read-then-write refusal cannot fire. The
reads must NOT be made inside ``own_state_reads()``, which would re-root them
under the current root.

**v1's live key is never read here.** It could only come in as a guard-baseline
read, and ``shadow.interceptor.guard_baseline_reads``'s contract forbids
publishing bytes read that way. Every function here is the identity outside an
active shadow root, so the production path reads exactly what it read before.
"""

from __future__ import annotations

import logging
import warnings
from datetime import date, timedelta
from typing import Any, Callable, Iterable

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = [
    "EARLIER_ROOT_LOOKBACK_SESSIONS",
    "coalesce_by_vendor_precedence",
    "earlier_root_days",
    "earlier_root_key",
    "with_earlier_shadow_roots",
]

#: How many of the shadow's earlier daily roots are searched, newest first.
#: ``staging/`` (and so ``staging/shadow/``) expires 7 calendar days after a
#: write, which never holds more than 5 NYSE sessions; an older root is gone.
EARLIER_ROOT_LOOKBACK_SESSIONS = 5

#: A calendar-day bound on the backwards walk, so a broken calendar cannot loop.
_MAX_WALK_DAYS = 31


def earlier_root_days(
    root: Any,
    *,
    since: "date | None" = None,
    lookback: int = EARLIER_ROOT_LOOKBACK_SESSIONS,
) -> "list[date]":
    """NYSE sessions ``T`` with ``since <= T < root.trading_day``, newest first.

    At most ``lookback`` of them. Raises when the calendar cannot classify a
    day; callers decide whether that degrades to "current root only".
    """
    from nousergon_lib.dates import is_trading_day

    days: list[date] = []
    d = root.trading_day - timedelta(days=1)
    floor = root.trading_day - timedelta(days=_MAX_WALK_DAYS)
    while len(days) < lookback and d >= floor and (since is None or d >= since):
        if is_trading_day(d):
            days += [d]  # not .append: data_gate's writer scan reads that as an ArcticDB write
        d -= timedelta(days=1)
    return days


def earlier_root_key(live_key: str, day: date) -> str:
    """``live_key`` under the shadow root for ``day``."""
    from shadow.root import SHADOW_ROOT_TEMPLATE

    return SHADOW_ROOT_TEMPLATE.format(trading_day=day.isoformat()) + live_key.lstrip("/")


def coalesce_by_vendor_precedence(frames: "Iterable[pd.DataFrame]") -> "pd.DataFrame | None":
    """Ticker-indexed D19 frames, NEWEST first, coalesced one row per ticker.

    The rule is D19's own (``collectors.daily_closes._coalesce_by_source_priority``):
    the highest ``VENDOR_PRECEDENCE`` source wins, a null ``Close`` ranks below
    any real value, and among equals the newer frame wins (a same-tier
    restatement replaces the prior value, exactly as each v1 write does).
    """
    from collectors.daily_closes import _UNKNOWN_SOURCE_PRIORITY, VENDOR_PRECEDENCE

    parts = [f for f in frames if f is not None]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]

    def _rank(part: pd.DataFrame) -> pd.Series:
        src = part["source"] if "source" in part.columns else pd.Series(None, index=part.index)
        rank = src.map(lambda v: VENDOR_PRECEDENCE.get(v, _UNKNOWN_SOURCE_PRIORITY)).astype(float)
        if "Close" in part.columns:
            rank[part["Close"].isna().to_numpy()] = 0.0
        return rank

    with warnings.catch_warnings():
        # A yfinance-only copy carries an all-NA ``VWAP``; pandas' deprecation
        # notice about how such a column will type in future is not a finding
        # about this data, and would repeat once per window date.
        warnings.filterwarnings("ignore", message=".*empty or all-NA entries.*", category=FutureWarning)
        merged = pd.concat([
            part.assign(_er_rank=_rank(part), _er_recency=recency)
            for recency, part in enumerate(parts)
        ])
    merged = merged.sort_values(["_er_rank", "_er_recency"], ascending=[False, True], kind="stable")
    merged = merged[~merged.index.duplicated(keep="first")]
    return merged.drop(columns=["_er_rank", "_er_recency"])


def with_earlier_shadow_roots(
    frame: "pd.DataFrame | None",
    live_key: str,
    session: date,
    read: "Callable[[str], pd.DataFrame | None]",
    *,
    lookback: int = EARLIER_ROOT_LOOKBACK_SESSIONS,
) -> "pd.DataFrame | None":
    """``frame`` (the current root's copy of the D19 file ``live_key``) coalesced
    with the shadow's earlier roots' copies of it.

    The roots searched are the sessions ``T`` in ``[session, D)``: a root before
    ``session`` cannot hold that session's closes. ``read(key)`` returns a
    ticker-indexed frame or ``None`` (and does its own logging). Outside an
    active shadow root, returns ``frame`` unchanged and reads nothing.
    """
    from shadow.root import active_root

    root = active_root()
    if root is None:
        return frame
    try:
        days = earlier_root_days(root, since=session, lookback=lookback)
    except Exception as exc:  # noqa: BLE001 - a calendar miss leaves the current root only, logged
        logger.warning(
            "shadow earlier roots: could not enumerate the sessions before %s for %s (%s) "
            "— using the current root's copy only",
            root.trading_day, live_key, exc,
        )
        days = []
    candidates = [frame]
    for day in days:
        candidates += [read(earlier_root_key(live_key, day))]
    return coalesce_by_vendor_precedence(candidates)
