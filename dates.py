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

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from nousergon_lib.guard_mode import GuardMode, GuardStaging

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


def default_session_date(now: datetime | None = None) -> str:
    """Resolve the default artifact-keying date on the EVENT-TIME axis — the
    session a moment falls WITHIN, not the last one that has fully closed.

    ``default_run_date()`` above resolves to ``nousergon_lib.dates.now_dual()
    .trading_day``, the *knowledge/as-of* axis: the newest session whose data a
    computation may safely use. That is right for a writer settling an EOD
    close. It is wrong for a writer recording an IN-SESSION observation — an
    intraday timer tick — because during a live session the two axes differ by
    exactly one session: at 11:00 UTC on a Monday the as-of axis still reads
    Friday (Monday's own close hasn't happened yet), so an intraday manifest
    keyed by ``default_run_date()`` files itself under the PREVIOUS session's
    folder for the entire session, every trading day (measured live,
    `alpha-engine-config-I10810`: D37's ``data_run_manifest.v1`` objects
    written 2026-09-21 all carried ``trading_day: "2026-09-18"``, three
    sessions stale, because the 04:55Z run started long before that day's own
    16:00 ET close).

    Returns the NYSE session ``now`` belongs to via the fleet-canonical
    ``nousergon_lib.dates.session_date()`` chokepoint — the same event-time
    axis `nousergon_lib`'s own docstring names as the fix for exactly this
    off-by-one class (config#1610). Falls back to the calendar UTC date only
    if the lib lookup raises — date defaulting must never block a collection
    run.

    Args:
        now: optional timezone-aware moment (mainly for tests). Defaults to
            the current moment inside ``session_date``.

    Returns:
        ISO ``YYYY-MM-DD`` string.
    """
    try:
        from nousergon_lib.dates import session_date as _session_date

        sd = _session_date(now) if now is not None else _session_date()
        log.info("default_session_date: resolved session_date=%s", sd)
        return sd.isoformat()
    except Exception:  # noqa: BLE001 — date defaulting must not block a run
        ref = now or datetime.now(timezone.utc)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        fallback = ref.astimezone(timezone.utc).strftime("%Y-%m-%d")
        log.warning(
            "default_session_date: could not resolve session_date via "
            "nousergon_lib.dates.session_date; fell back to calendar date %s",
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
#
# alpha-engine-config-I10794 (ruling (a), 2026-10-25) extends "never trims" to
# the FETCH boundary: a vendor answer that runs past ``end`` used to be clipped
# there with a WARNING, so the run went on ``ok`` and the write-site raise was
# unreachable for an over-answering vendor. The fetch bound now refuses too,
# with a typed :class:`FutureBarFinding` (dataset, ticker, as-of, bar date).
# Measured before the change (2026-10-26, ``/alpha-engine/data-spot``, 30
# days): the old clip WARNING fired 0 times, so refusing changes no observed run.
# ---------------------------------------------------------------------------


class FutureBarError(ValueError):
    """A series about to be published carries a bar dated after its run's
    trading day (alpha-engine-config-I10893). Subclasses ``ValueError`` and is
    deliberately re-raised through per-ticker ``except Exception`` blocks by
    every writer: it is a contract violation of the run, not a one-ticker
    vendor hiccup."""


#: The finding's name on every surface it reaches (the exception, the ERROR
#: log line, a manifest reason). Closed: one value, one meaning.
FUTURE_BAR_FINDING = "pit_future_bar"


@dataclass(frozen=True)
class FutureBarFinding:
    """WHAT a point-in-time read refused, as data (alpha-engine-config-I10794).

    Brian's ruling on I10794 (option (a), 2026-10-25): a point-in-time read
    that finds a bar dated after its as-of date FAILS with a finding; the bar
    is never silently dropped. This is that finding. Every field is a plain
    string or int, so :meth:`to_record` is JSON as it stands and a reader
    never parses the exception message to learn which bar was refused.

    ``dataset``
        The read the bar arrived on: the label's stem
        (``price_cache_refresh`` for ``price_cache_refresh[AAPL]``), or the
        artifact key for a write-site refusal.
    ``ticker``
        The symbol whose series carried it, when the read is per-symbol;
        ``None`` for a frame that is not (``macro_market_prices``).
    ``as_of``
        The trading day the read serves (ISO date).
    ``bar_date``
        The FIRST bar dated after ``as_of`` (ISO date): the offending bar.
    ``last_bar_date``
        The series' last bar (ISO date); equal to ``bar_date`` for one stray.
    ``bars_after``
        How many bars fall after ``as_of``.
    """

    dataset: str
    ticker: str | None
    as_of: str
    bar_date: str
    last_bar_date: str
    bars_after: int

    def to_record(self) -> dict:
        return {
            "finding": FUTURE_BAR_FINDING,
            "guard": "pit",
            "dataset": self.dataset,
            "ticker": self.ticker,
            "as_of": self.as_of,
            "bar_date": self.bar_date,
            "last_bar_date": self.last_bar_date,
            "bars_after": self.bars_after,
            "tracked_issue": "alpha-engine-config-I10794",
        }

    def describe(self) -> str:
        who = f"{self.dataset}[{self.ticker}]" if self.ticker else self.dataset
        return (
            f"{FUTURE_BAR_FINDING}: {who} answered a bar dated {self.bar_date} "
            f"({self.bars_after} bar(s) after as_of {self.as_of}, last {self.last_bar_date})"
        )


class FutureBarRefused(FutureBarError):
    """A :class:`FutureBarError` that carries its :class:`FutureBarFinding`.

    Raised by both point-in-time bounds: the fetch-boundary refusal
    (:func:`refuse_bars_after`, and :func:`clip_to_trading_day`, which now
    delegates to it) and the write-site guard (:func:`assert_no_bar_after`).
    A subclass, so every existing ``except FutureBarError: raise`` handler
    already propagates it run-level; ``.finding`` is what a recorder reads.
    """

    def __init__(self, message: str, finding: FutureBarFinding) -> None:
        super().__init__(message)
        self.finding = finding


#: ``price_cache_refresh[AAPL]`` -> (``price_cache_refresh``, ``AAPL``). The
#: label convention every fetch-boundary call site already uses.
_LABEL_RE = re.compile(r"^(?P<dataset>[^\[\]]+)\[(?P<ticker>[^\[\]]+)\]$")


def _split_label(label: str) -> "tuple[str, str | None]":
    m = _LABEL_RE.match(str(label))
    if m is None:
        return str(label), None
    return m.group("dataset"), m.group("ticker")


def _bars_after(index, trading_day: "date | datetime | str"):
    """``(d, normalized_index, mask_after_d)`` for any date-like index."""
    import pandas as pd

    d = as_trading_day(trading_day)
    idx = pd.to_datetime(pd.Index(index))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    after = idx.normalize() > pd.Timestamp(d)
    return d, idx, after


def _finding(idx, after, d, *, dataset: str, ticker: str | None) -> FutureBarFinding:
    offending = idx[after]
    return FutureBarFinding(
        dataset=dataset,
        ticker=ticker,
        as_of=d.isoformat(),
        bar_date=offending.min().date().isoformat(),
        last_bar_date=idx.max().date().isoformat(),
        bars_after=int(after.sum()),
    )


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


def refuse_bars_after(
    frame, trading_day: "date | datetime | str", *, dataset: str, ticker: str | None = None,
):
    """Fetch-boundary point-in-time bound: REFUSE a fetched frame that carries
    any bar dated after D (alpha-engine-config-I10794, ruling (a)).

    The fetch already asks for ``end = D + 1`` (exclusive); a vendor that
    answers beyond the requested end is out of contract. Until I10794 those
    rows were dropped here with a WARNING and the run went on ``ok``, so an
    induced vendor future bar never reached the write-site raise and the
    ``pit`` guard could not be commissioned on the units this bound serves
    (D03 among them). Now the read fails: :class:`FutureBarRefused`, carrying
    a :class:`FutureBarFinding` that names the dataset, the ticker, the as-of
    date and the offending bar date, and one ERROR line with the finding as
    JSON (the recording surface). Nothing is trimmed, ever.

    Returns ``frame`` unchanged when every bar is on or before D (including an
    empty or ``None`` frame), so a call site keeps its ``frame = ...`` shape.
    """
    if frame is None or len(frame) == 0:
        return frame
    d, idx, after = _bars_after(frame.index, trading_day)
    if not after.any():
        return frame
    finding = _finding(idx, after, d, dataset=dataset, ticker=ticker)
    log.error("PIT_FUTURE_BAR %s", json.dumps(finding.to_record(), sort_keys=True))
    raise FutureBarRefused(
        f"{finding.describe()}; refusing the read, never trimming it: a vendor "
        "that answers past the requested end is out of contract (alpha-engine-config-I10794)",
        finding,
    )


def clip_to_trading_day(frame, trading_day: "date | datetime | str", *, label: str):
    """The fetch-boundary bound every fetch site calls, under its I10893 name.

    **It no longer clips.** Brian's ruling on alpha-engine-config-I10794
    (option (a), 2026-10-25): a point-in-time read that finds a bar dated after
    its as-of date fails with a finding and the bar is never silently dropped.
    This delegates to :func:`refuse_bars_after`, splitting the call site's
    ``label`` (``dataset[TICKER]``) into the finding's ``dataset`` and
    ``ticker``. The name is kept so the call sites (some of them on paths held
    from edits tonight) pick the ruling up without a rename; a frame with no
    bar after D is returned unchanged, exactly as before.
    """
    dataset, ticker = _split_label(label)
    return refuse_bars_after(frame, trading_day, dataset=dataset, ticker=ticker)


def assert_no_bar_after(
    bar_dates, trading_day: "date | datetime | str", *, artifact: str,
) -> None:
    """Write-site guard: raise :class:`FutureBarError` when any bar in
    ``bar_dates`` (a DatetimeIndex, or an iterable of ISO strings / dates /
    timestamps) is dated after ``trading_day``. Raises, never trims — a
    publisher that reaches this with a future bar bypassed the fetch bound.

    The raise is a :class:`FutureBarRefused` carrying a
    :class:`FutureBarFinding` whose ``dataset`` is ``artifact``
    (alpha-engine-config-I10794)."""
    import pandas as pd

    values = list(bar_dates) if not isinstance(bar_dates, pd.Index) else bar_dates
    if len(values) == 0:
        return
    d, idx, after = _bars_after(values, trading_day)
    last = idx.max()
    if pd.isna(last) or last.date() <= d:
        return
    finding = _finding(idx, after, d, dataset=artifact, ticker=None)
    log.error("PIT_FUTURE_BAR %s", json.dumps(finding.to_record(), sort_keys=True))
    raise FutureBarRefused(
        f"{artifact}: refusing to publish a series whose last bar "
        f"{last.date().isoformat()} is after the run's trading_day "
        f"{d.isoformat()} ({finding.bars_after} bar(s) after D). A run for D must never "
        "publish a later (possibly pre-close) session — see "
        "alpha-engine-config-I10893.",
        finding,
    )


# ---------------------------------------------------------------------------
# alpha-engine-config-I11354 — the bar for D is not final when D's session
# ends.
#
# `assert_no_bar_after` above bounds a series on the DATE axis: nothing later
# than D may be published for a run serving D. It says nothing about the CLOCK
# — a run for D that fetches D's own bar six minutes after the 16:00 ET close
# passes it and still publishes an unsettled number.
#
# Measured 2026-09-21 (`s3://alpha-engine-research/data_collection/parity/
# 2026-09-21.json`, one trading day, 920 of 920 price-cache files):
#
#   * v1's postclose data stage fetched D's bar at 16:06 ET. Refetched at
#     18:41 ET, `Volume` differed on 920/920 files (median 20.5 % short, p90
#     36.0 %, max 62.3 %, and NEVER high) and `Close` on 442/920 (median
#     2.7 bps, p90 6.4 bps, max 16.4 bps). Every breach was on D's own row;
#     all 2,512 earlier rows agreed inside 1e-4.
#   * The 18:41 ET bar was then checked against a third fetch at 22:36 ET on a
#     30-ticker sample: `Close` identical on 30/30 to the last printed digit.
#     `Volume` still moved (median 0.9 %, max 13.2 %).
#
# So: the official closing print is FINAL well before 18:15 ET, and
# consolidated volume is not final that evening at all. This module grades the
# price axis, which is what every feature, technical and trading decision
# depends on. A same-evening volume is provisional by construction. The next
# morning's Polygon T+1 overwrite (D17) settles it in `staging/daily_closes`,
# D18 carries it into ArcticDB, and D50 (`features/settled_regrade.py`)
# rebuilds the published `features/{D}` snapshot from it in the same morning
# schedule (Crucible v2 ruling on alpha-engine-config-I12023). Every artifact
# written the evening of D carries provisional volume until that regrade, and
# one written by a unit nothing regrades carries it for good.
# ---------------------------------------------------------------------------

#: The ET wall-clock time from which day D's official closing print is treated
#: as final. Declared, not inferred.
#:
#: **Evidence is ONE trading day** (2026-09-21): 16:06 ET unsettled, 18:41 ET
#: settled on `Close`. 18:15 is the conservative point inside that bracket —
#: the consolidated tape is final by ~17:30 ET and the vendor publishes the
#: official close by then. The 3-day sample at 16:45 / 17:30 / 18:15 / 18:45
#: that would turn a one-day bracket into a measured threshold is tracked as
#: `alpha-engine-config-I11356`.
#:
#: **I11356 measured it** (2026-09-30 → 10-02, 50 tickers, 16:47–22:35 ET):
#: `Close` was final at the first 16:47 ET fetch on all three days; `Volume`
#: froze at a provisional figure until ~21:00–22:30 ET. The value stays 18:15
#: (I11356 closing comment, 2026-10-03): earlier gains nothing for `Close`, and
#: provisional Volume is overwritten by the D+1 morning polygon pass.
SETTLED_AFTER_ET = "18:15"

#: The exchange clock every settlement judgement is made on. Never UTC: the
#: 18:15 boundary is an exchange-local fact and moves with US DST.
_SETTLEMENT_TZ = "America/New_York"

#: Verdict vocabulary for :func:`bar_settlement`. Closed on purpose — a third
#: value would have to mean something to every manifest reader.
BAR_SETTLED = "settled"
BAR_PROVISIONAL = "provisional"


def bar_settlement(
    fetched_at_utc: "datetime | str", trading_day: "date | datetime | str",
) -> str:
    """Was day ``trading_day``'s bar already settled when it was fetched?

    Returns :data:`BAR_SETTLED` (``"settled"``) when ``fetched_at_utc`` falls
    at or after :data:`SETTLED_AFTER_ET` on ``trading_day`` in
    ``America/New_York`` — including any moment on a LATER calendar day, which
    is the backfill / rerun case. Returns :data:`BAR_PROVISIONAL`
    (``"provisional"``) otherwise, which covers both the postclose window
    (after the 16:00 ET close, before the print settles) and a fetch made
    before the session has even closed.

    This is a GRADER, not a guard: it never raises and never trims. The verdict
    rides on the run manifest (`alpha-engine-config-I11354`, observe mode) so a
    consumer can tell an unsettled artifact from a settled one, and so a
    promotion to enforce has a count of clean cycles behind it rather than an
    argument.

    Args:
        fetched_at_utc: when the vendor fetch for this run began. A naive
            ``datetime`` is read as UTC (consistent with the rest of this
            module); an ISO string is parsed, with a trailing ``Z`` accepted.
        trading_day: the session the artifact is keyed on.

    Raises:
        ValueError: on an unparseable ``fetched_at_utc``. A run that cannot say
            WHEN it fetched must not be graded ``settled`` by default — this is
            a producer repo and a silent ``provisional`` would be a fabricated
            reading, not a degraded one.
    """
    from datetime import time as _time
    from zoneinfo import ZoneInfo

    if isinstance(fetched_at_utc, str):
        raw = fetched_at_utc.strip()
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        moment = datetime.fromisoformat(raw)
    elif isinstance(fetched_at_utc, datetime):
        moment = fetched_at_utc
    else:
        raise ValueError(
            f"bar_settlement: fetched_at_utc must be a datetime or an ISO "
            f"string, got {type(fetched_at_utc)!r}"
        )
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)

    d = as_trading_day(trading_day)
    local = moment.astimezone(ZoneInfo(_SETTLEMENT_TZ))
    hh, mm = (int(part) for part in SETTLED_AFTER_ET.split(":"))
    if local.date() > d:
        return BAR_SETTLED
    if local.date() < d:
        return BAR_PROVISIONAL
    return BAR_SETTLED if local.timetz().replace(tzinfo=None) >= _time(hh, mm) else BAR_PROVISIONAL


#: The guard name a collector records a VENDOR's own publish time under
#: (`alpha-engine-config-I11203`). Its `key` is ``"<live key>#<json path>"``
#: (``market_data/macro/latest.json#$.series.DGS10``) and its `value` is the
#: vendor's last-updated moment for that series, read AFTER the run's own
#: observations (so an upper bound on the version it read), as POSIX seconds,
#: UTC. With :data:`VENDOR_FIRST_RELEASED_GUARD` it is the evidence
#: `shadow.parity`'s `vendor_published_after_v1_fetch` reads: an observation
#: first released on a later day than v1's version was last updated could not
#: have been in what v1 read — a v1-side timing fact, proven rather than
#: asserted.
VENDOR_PUBLISHED_AT_GUARD = "vendor_published_at"

#: Its companion: the date one OBSERVATION was first released by the vendor.
#: `key` is ``"<live key>#<json path>@<observation date>"`` and `value` that
#: release date at 00:00 UTC, as POSIX seconds (`alpha-engine-config-I11203`).
VENDOR_FIRST_RELEASED_GUARD = "vendor_first_released"

#: Staging for the ``bar_settlement`` reading (`sf-pipeline-policy.md` §7a).
#: It shipped OBSERVE because enforcing it against the 16:45 ET
#: `data-collection-eod` schedule would have refused every D03/D19 write that
#: schedule made. That schedule now fires at :data:`SETTLED_AFTER_ET`
#: (`nousergon-data-PR1868`, Brian's ruling on alpha-engine-config-I11354), the
#: 3-day sample (alpha-engine-config-I11356) kept 18:15 ET, and the codified
#: per-guard criterion (`data_gate/guard_promotion.py`) reads READY on the
#: scheduled D03/D19 runs, so it is promoted to ENFORCE here. The raise site is
#: :func:`assert_settled_bar`, called before either collector's vendor fetch.
BAR_SETTLEMENT_GUARD = GuardStaging(
    name="bar_settlement",
    mode=GuardMode.ENFORCE,
    promotion_criterion=(
        "The standalone postclose schedule fetches at or after "
        f"{SETTLED_AFTER_ET} ET (Brian's ruling on alpha-engine-config-I11354), "
        "AND the codified per-guard risk-based criterion holds "
        "(data_gate/guard_promotion.py, bar_settlement: moderate risk — the latest "
        "scheduled D03+D19 runs record verdict='settled'; Brian's 2026-10-04 option (c) "
        "on alpha-engine-config-I11973 replaced the ten-run count), "
        "AND the 3-day settlement-time sample (alpha-engine-config-I11356) "
        f"confirms {SETTLED_AFTER_ET} ET rather than the one-day 2026-09-21 "
        "bracket this threshold currently rests on."
    ),
    tracked_issue="alpha-engine-config-I11354",
)


class UnsettledBarError(RuntimeError):
    """A D03/D19 fetch for day D's own bar opened before :data:`SETTLED_AFTER_ET`."""


def assert_settled_bar(
    fetched_at_utc: "datetime | str",
    trading_day: "date | datetime | str",
    *,
    unit: str,
) -> None:
    """The ENFORCE raise site for :data:`BAR_SETTLEMENT_GUARD`.

    Called at the OPENING edge of a D03/D19 vendor fetch, before anything is
    fetched or written, so a refused run publishes nothing rather than an
    unsettled bar. A settled fetch returns ``None``. A provisional one goes
    through :meth:`GuardStaging.verdict`: logged at ERROR in either mode, and
    raised as :class:`UnsettledBarError` only while the staging is ENFORCE — so
    demoting the guard back to OBSERVE is the one-line revert.
    """
    if bar_settlement(fetched_at_utc, trading_day) == BAR_SETTLED:
        return
    BAR_SETTLEMENT_GUARD.verdict(
        logging.getLogger(__name__),
        (
            f"{unit}: fetch for trading_day {as_trading_day(trading_day).isoformat()} "
            f"opened at {fetched_at_utc} — before the {SETTLED_AFTER_ET} ET settlement "
            "threshold, so day D's bar is provisional (alpha-engine-config-I11354)"
        ),
        raise_with=UnsettledBarError,
    )


def bar_settlement_guard_entry(
    fetched_at_utc: "datetime | str",
    trading_day: "date | datetime | str",
    *,
    key: str | None = None,
) -> dict:
    """One ``result["guards"]`` entry carrying this run's settlement verdict.

    Shaped for ``weekly_collector._record_collector_guards``, the generic hook
    that folds a collector's self-graded readings onto its run manifest — so
    recording this needs no change to ``run_units.py`` or to the manifest
    writer, only a ``guards`` key on the collector's result dict.

    ``value`` is the fetch moment's ET clock as a float hour (``18.25`` for
    18:15 ET) and ``baseline`` is :data:`SETTLED_AFTER_ET` in the same unit, so
    the console can render the margin without re-parsing the detail string.
    """
    from zoneinfo import ZoneInfo

    verdict = bar_settlement(fetched_at_utc, trading_day)
    moment = fetched_at_utc
    if isinstance(moment, str):
        raw = moment.strip()
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        moment = datetime.fromisoformat(raw)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    local = moment.astimezone(ZoneInfo(_SETTLEMENT_TZ))
    hh, mm = (int(part) for part in SETTLED_AFTER_ET.split(":"))
    return {
        "guard": BAR_SETTLEMENT_GUARD.name,
        "mode": BAR_SETTLEMENT_GUARD.mode.value,
        "verdict": verdict,
        "detail": (
            f"fetch for trading_day {as_trading_day(trading_day).isoformat()} began "
            f"{local.isoformat()} ({local.tzname()}); settlement threshold "
            f"{SETTLED_AFTER_ET} ET. Verdict {verdict}. "
            + (
                "The official closing print is final at this hour."
                if verdict == BAR_SETTLED
                else "Close may still move (measured 2026-09-21: median 2.7 bps, "
                     "max 16.4 bps between a 16:06 ET and an 18:41 ET fetch) and "
                     "Volume is short (median 20.5 %, max 62.3 %, never high)."
            )
            + " Consolidated Volume is NOT final at any evening hour — see "
              "alpha-engine-config-I11354."
        ),
        "key": key,
        "value": round(local.hour + local.minute / 60 + local.second / 3600, 4),
        "baseline": round(hh + mm / 60, 4),
    }
