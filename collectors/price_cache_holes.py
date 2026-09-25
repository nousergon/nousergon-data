"""Interior-session hole fill for the price-cache refresh (alpha-engine-config-I11553).

``collectors/prices.py::_refresh_stale`` replaces each ticker's
``reference/price_cache/{T}.parquet`` wholesale with a fresh 10-year vendor
answer. Its two write guards look only at the ENDS of that answer: the
short-fetch guard at the row count (and only under 400 rows), the behind-fetch
guard (alpha-engine-config-I11467) at the last bar. Neither sees a session
missing from the MIDDLE of an otherwise current series.

Measured 2026-09-24: the 2026-09-23 00:10Z / 01:44Z weekly rehearsal
overwrote the cache with series ending 2026-09-21 (the I11467 case), and every
refresh after it (2026-09-23T20:07Z, 2026-09-24T20:07Z and 20:12Z) answered
``[..., 09-21, 09-23, 09-24]`` for 837 of the ~930 cached tickers — a
2512-row series one session short, which no guard reads as short. D21
(``market_data/close_history/``) rebuilds from this cache every run, so its
live artifact carried 2026-09-22 for 95 of 955 symbols. Polygon's
``staging/daily_closes/2026-09-22.parquet`` (D19) held all 932 that day, and
matched the vendor's own 09-21 / 09-23 closes to a median 2e-8.

:class:`SessionHoleFiller` closes that hole before the upload:

* **Detection** is calendar-only and costs no S3 read: an NYSE session
  strictly between a series' first and last bar that the series does not
  carry is a hole. A current, complete answer therefore pays nothing.
* **Sources**, in order: the D19 ``staging/daily_closes/{session}.parquet``
  row (the fleet's canonical EOD record, shared across every holed ticker,
  so one GET per holed session per run), then the ticker's own current cache
  parquet (so a bar filled once is CARRIED FORWARD by every later refresh —
  ``staging/`` expires after 7 days, and a vendor that never restores the
  session would otherwise re-open the hole the day the staging file went).
* **Basis.** The cache is dividend-adjusted (``auto_adjust=True``); neither
  source need be on the fetch's current basis. Each filled bar is rescaled by
  ``fetched.Close / source.Close`` measured on the hole's two neighbouring
  sessions, and the fill is REFUSED when those two factors disagree by more
  than :data:`HOLE_FILL_FACTOR_AGREEMENT` — the signature of a corporate
  action adjacent to the hole, where a single factor would be wrong on one
  side. The error in any filled bar is bounded by that tolerance.
* **Recording.** One aggregated WARNING per run names what was filled and
  from where; one aggregated record names what could not be filled, at ERROR
  when any unfilled hole is recent enough that a source should still exist
  (:data:`HOLE_ALERT_TRADING_DAYS`), else at WARNING (a long-standing gap such
  as a listing transfer, which no source here can fill).

An unfilled hole never blocks the upload: refusing would freeze the ticker's
cache at an older last bar, and D21 then routes a stale ticker to a yfinance
gap-fill that carries the same hole.
"""

from __future__ import annotations

import io
import logging
import math
from collections import Counter
from datetime import date, timedelta
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

#: D19's per-session EOD snapshot (``collectors/daily_closes.py``), indexed by ticker.
DAILY_CLOSES_PREFIX = "staging/daily_closes/"

#: Max relative disagreement between the basis factor measured on the session
#: before the hole and the one after it. Wider than the vendor-vs-polygon noise
#: (median 2e-8, p99 6e-8 on 2026-09-21/23), narrower than any dividend worth
#: correcting: a filled bar is never further than this from the right basis.
HOLE_FILL_FACTOR_AGREEMENT = 1e-3

#: A basis factor this far from 1 is a split-scale change, which (unlike a
#: dividend) also rescales volume.
_SPLIT_SCALE_THRESHOLD = 0.2

#: An unfilled hole this many NYSE sessions old or newer pages at ERROR: the
#: D19 staging file for it should still exist (``staging/`` expires after 7
#: calendar days), so a miss is a defect, not a known historical gap.
HOLE_ALERT_TRADING_DAYS = 5

#: Under a shadow root, how many of the shadow's own earlier daily roots (the
#: NYSE sessions before its trading day, newest first) are searched for a
#: session's canonical ``staging/daily_closes`` file (alpha-engine-config-I11577).
#: Matches :data:`HOLE_ALERT_TRADING_DAYS`: an older hole is past the window a
#: staging source is expected to exist for, and falls to the cached parquet.
SHADOW_ROOT_LOOKBACK_SESSIONS = HOLE_ALERT_TRADING_DAYS

_PRICE_COLS = ("Open", "High", "Low", "Close")
_SAMPLE = 20


def _session_dates(start: date, end: date) -> list[date]:
    """NYSE sessions in ``[start, end]``, clamped to the calendar's coverage.

    A date before the krepis holiday table's first year cannot be classified
    (every weekday would read as a session), so the scan starts there instead.
    """
    from nousergon_lib.dates import is_trading_day

    try:
        from krepis.trading_calendar import NYSE_CALENDAR_COVERS_FROM
    except ImportError:  # pinned krepis predates the coverage constant
        NYSE_CALENDAR_COVERS_FROM = date(2016, 1, 1)
    first = max(start, NYSE_CALENDAR_COVERS_FROM)
    days = (first + timedelta(days=i) for i in range((end - first).days + 1))
    return [d for d in days if is_trading_day(d)]


def _is_missing(exc: Exception) -> bool:
    """True when ``exc`` means "no such S3 object" and nothing else."""
    response = getattr(exc, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return code in {"NoSuchKey", "404"} or type(exc).__name__.lstrip("_") == "NoSuchKey"


def _index_dates(index) -> list[date]:
    idx = pd.to_datetime(pd.Index(index))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    return [ts.date() for ts in idx.normalize()]


class SessionHoleFiller:
    """Fill interior NYSE-session holes in a fetched price-cache frame.

    One instance per ``_refresh_stale`` call: it memoises the D19 staging
    frames across tickers and accumulates what it filled for :meth:`report`.
    """

    def __init__(
        self,
        s3: Any,
        bucket: str,
        *,
        window_start: date,
        expected_last: date,
        cache_keys: "Any" = None,
        skip: "frozenset[str] | set[str]" = frozenset(),
    ):
        self._s3 = s3
        self._bucket = bucket
        self._cache_keys = cache_keys  # ticker -> [candidate price-cache keys]
        self._skip = frozenset(skip)
        self._expected_last = expected_last
        try:
            self._sessions = _session_dates(window_start, expected_last)
        except Exception as exc:  # noqa: BLE001 - a calendar miss must not block the refresh
            logger.warning(
                "price_cache hole scan disabled this run: the NYSE calendar could "
                "not resolve %s..%s (%s) — interior-session holes will NOT be "
                "detected (alpha-engine-config-I11553)",
                window_start, expected_last, exc,
            )
            self._sessions = []
        recent = self._sessions[-HOLE_ALERT_TRADING_DAYS:]
        self._alert_from = recent[0] if recent else expected_last
        self._daily_closes: dict[date, "pd.DataFrame | None"] = {}
        self.filled: dict[str, dict[date, str]] = {}
        self.unfilled: dict[str, dict[date, str]] = {}

    # ── detection ────────────────────────────────────────────────────────
    def holes(self, index) -> list[date]:
        """NYSE sessions strictly inside ``index``'s span that it does not carry."""
        dates = _index_dates(index)
        if len(dates) < 2 or not self._sessions:
            return []
        present = set(dates)
        first, last = min(dates), max(dates)
        return [s for s in self._sessions if first < s < last and s not in present]

    # ── sources ──────────────────────────────────────────────────────────
    def _read_daily_closes(self, key: str, session: date) -> "pd.DataFrame | None":
        try:
            obj = self._s3.get_object(Bucket=self._bucket, Key=key)
            frame = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            if "ticker" in frame.columns:
                frame = frame.set_index("ticker")
            return frame[~frame.index.duplicated(keep="last")]
        except Exception as exc:  # noqa: BLE001 - an absent source is an unfilled hole, recorded
            # Expired staging (7-day lifecycle) is the ordinary case for an old
            # hole and is summarised by report(); anything else is a read fault.
            (logger.info if _is_missing(exc) else logger.warning)(
                "price_cache hole fill: s3://%s/%s unavailable (%s: %s) — holes on "
                "%s need another source",
                self._bucket, key, type(exc).__name__, exc, session,
            )
            return None

    def _staging_frame(self, session: date) -> "pd.DataFrame | None":
        if session in self._daily_closes:
            return self._daily_closes[session]
        key = f"{DAILY_CLOSES_PREFIX}{session.isoformat()}.parquet"
        frame = self._read_daily_closes(key, session)
        from shadow.root import active_root

        root = active_root()
        if root is not None:
            frame = self._with_earlier_shadow_roots(frame, key, session, root)
        self._daily_closes[session] = frame
        return frame

    def _with_earlier_shadow_roots(
        self, frame: "pd.DataFrame | None", key: str, session: date, root: Any,
    ) -> "pd.DataFrame | None":
        """``frame`` coalesced with the shadow's OWN earlier roots' copies of ``key``.

        alpha-engine-config-I11577. Under a shadow root, ``staging/daily_closes/``
        is run state (``shadow.interceptor.OWN_STATE_KEY_PATTERNS``), so the
        plain read above resolves to the CURRENT root's copy — on a fresh
        same-day root, the D19 window pass's own yfinance-only rewrite, with
        none of the polygon rows v1's live file keeps (``skip_if_canonical``).
        On 2026-09-24 that file carried 835 tickers' 2026-09-21 closes stamped
        2026-09-22, and this filler published them into the shadow price cache.

        The session's canonical record in the shadow's world is the one its
        own earlier runs wrote: ``staging/shadow/{T}/staging/daily_closes/{S}``
        for the sessions ``T`` between ``S`` and the root's trading day (the
        2026-09-23 root's 2026-09-22 file is the shadow morning pass's polygon
        overwrite). Those keys are outside this root, so the interceptor reads
        them LIVE as ordinary inputs, and this run never writes one — the
        I10891 read-then-write rule cannot trip. v1's live file is NOT read:
        a guard-baseline read may decide whether to publish, never supply what
        is published (``shadow.interceptor.guard_baseline_reads``).

        Rows are coalesced per ticker the way D19 coalesces a merge base
        (``collectors.daily_closes.VENDOR_PRECEDENCE``, a null Close ranking
        below everything): the highest-precedence source wins, and among
        equals the newest root (the current one first).
        """
        from collectors.daily_closes import VENDOR_PRECEDENCE
        from shadow.root import SHADOW_ROOT_TEMPLATE

        try:
            roots = _session_dates(session, root.trading_day - timedelta(days=1))
        except Exception as exc:  # noqa: BLE001 - a calendar miss leaves the current root only
            logger.warning(
                "price_cache hole fill: could not enumerate earlier shadow roots for %s "
                "(%s) — using the current root's copy only", session, exc,
            )
            roots = []
        candidates = [frame] if frame is not None else []
        for t in reversed(roots[-SHADOW_ROOT_LOOKBACK_SESSIONS:]):
            earlier = SHADOW_ROOT_TEMPLATE.format(trading_day=t.isoformat()) + key
            got = self._read_daily_closes(earlier, session)
            if got is not None:
                candidates += [got]
        if len(candidates) <= 1:
            return candidates[0] if candidates else None

        def _rank(part: pd.DataFrame) -> pd.Series:
            src = part["source"] if "source" in part.columns else pd.Series(None, index=part.index)
            rank = src.map(lambda v: VENDOR_PRECEDENCE.get(v, 1)).astype(float)
            if "Close" in part.columns:
                rank[part["Close"].isna()] = 0.0
            return rank

        merged = pd.concat([
            part.assign(_hole_rank=_rank(part), _hole_recency=recency)
            for recency, part in enumerate(candidates)
        ])
        merged = merged.sort_values(["_hole_rank", "_hole_recency"], ascending=[False, True], kind="stable")
        merged = merged[~merged.index.duplicated(keep="first")]
        return merged.drop(columns=["_hole_rank", "_hole_recency"])

    def _staging_bars(self, ticker: str, sessions: "list[date]") -> "pd.DataFrame | None":
        rows = {}
        for s in sessions:
            frame = self._staging_frame(s)
            if frame is None or ticker not in frame.index:
                continue
            rows[pd.Timestamp(s)] = frame.loc[ticker]
        if not rows:
            return None
        return pd.DataFrame.from_dict(rows, orient="index")

    def _cached_bars(self, ticker: str) -> "pd.DataFrame | None":
        """The ticker's current cache parquet, read as run state.

        It is a merge base (its bars may be published again), so under a
        shadow root it resolves to the shadow's own copy
        (``shadow.interceptor.own_state_reads``) — never a live input the
        same run then overwrites. Outside a shadow root this is a plain read.
        """
        from shadow.interceptor import own_state_reads

        for key in (self._cache_keys(ticker) if self._cache_keys else []):
            try:
                with own_state_reads():
                    obj = self._s3.get_object(Bucket=self._bucket, Key=key)
                df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            except Exception as exc:  # noqa: BLE001 - missing/unreadable -> next source, recorded below
                logger.info(
                    "price_cache hole fill: cached parquet %s not usable for %s (%s)",
                    key, ticker, type(exc).__name__,
                )
                continue
            df.index = pd.DatetimeIndex([pd.Timestamp(d) for d in _index_dates(df.index)])
            return df[~df.index.duplicated(keep="last")]
        return None

    # ── fill ─────────────────────────────────────────────────────────────
    def _rescaled_bar(
        self, fetched: pd.DataFrame, source: pd.DataFrame, hole: date,
    ) -> "tuple[dict | None, str]":
        """``source``'s bar for ``hole`` on ``fetched``'s basis, or a refusal reason."""
        h = pd.Timestamp(hole)
        if "Close" not in source.columns or "Close" not in fetched.columns:
            return None, "no Close column"
        if h not in source.index:
            return None, "source has no bar for the session"
        before = fetched.index[fetched.index < h]
        after = fetched.index[fetched.index > h]
        factors: list[float] = []
        for ts in (before.max(), after.min()):
            day = pd.Timestamp(ts.date())
            if day not in source.index:
                return None, f"source has no bar for neighbour {ts.date()}"
            f_close = float(fetched.at[ts, "Close"])
            s_close = float(source.at[day, "Close"])
            if not (math.isfinite(f_close) and math.isfinite(s_close)) or f_close <= 0 or s_close <= 0:
                return None, f"unusable close on neighbour {ts.date()}"
            factors += [f_close / s_close]
        if abs(factors[0] / factors[1] - 1.0) > HOLE_FILL_FACTOR_AGREEMENT:
            return None, (
                f"basis factors disagree around the hole ({factors[0]:.6f} vs "
                f"{factors[1]:.6f}) — a corporate action adjacent to it"
            )
        k = (factors[0] + factors[1]) / 2.0
        bar = source.loc[h]
        close = float(bar.get("Close", float("nan")))
        if not math.isfinite(close) or close <= 0:
            return None, "source close for the session is unusable"
        row: dict = {}
        for col in fetched.columns:
            if col in _PRICE_COLS:
                val = bar.get(col, float("nan"))
                row[col] = float(val) * k if pd.notna(val) else float("nan")
            elif col == "Volume":
                vol = bar.get("Volume", float("nan"))
                if pd.isna(vol):
                    row[col] = float("nan")
                else:
                    vol = float(vol) / k if abs(k - 1.0) > _SPLIT_SCALE_THRESHOLD else float(vol)
                    row[col] = int(round(vol)) if pd.api.types.is_integer_dtype(fetched[col]) else vol
            else:
                row[col] = float("nan")
        return row, "ok"

    def fill(self, ticker: str, fetched: pd.DataFrame) -> pd.DataFrame:
        """``fetched`` with every fillable interior-session hole filled."""
        if ticker in self._skip:
            return fetched
        holes = self.holes(fetched.index)
        if not holes:
            return fetched

        rows: dict[pd.Timestamp, dict] = {}
        pending = list(holes)
        reasons: dict[date, str] = {}
        sessions_needed = set(holes)
        for h in holes:
            ts = pd.Timestamp(h)
            before = fetched.index[fetched.index < ts]
            after = fetched.index[fetched.index > ts]
            sessions_needed |= {before.max().date(), after.min().date()}
        loaders = (
            ("daily_closes", lambda: self._staging_bars(ticker, sorted(sessions_needed))),
            ("cache", lambda: self._cached_bars(ticker)),
        )
        for name, load in loaders:
            if not pending:
                break
            source = load()
            if source is None:
                for h in pending:
                    reasons.setdefault(h, f"{name}: unavailable")
                continue
            still: list[date] = []
            for h in pending:
                row, why = self._rescaled_bar(fetched, source, h)
                if row is None:
                    reasons[h] = f"{name}: {why}"
                    still += [h]
                else:
                    rows[pd.Timestamp(h)] = row
                    self.filled.setdefault(ticker, {})[h] = name
            pending = still

        if pending:
            self.unfilled[ticker] = {h: reasons.get(h, "no source") for h in pending}
        if not rows:
            return fetched
        add = pd.DataFrame.from_dict(rows, orient="index").reindex(columns=fetched.columns)
        add.index.name = fetched.index.name
        return pd.concat([fetched, add]).sort_index()

    # ── recording ────────────────────────────────────────────────────────
    def report(self, log: logging.Logger = logger) -> dict:
        """Emit the run's one aggregated record per outcome; return the summary."""
        summary = {
            "tickers_filled": len(self.filled),
            "tickers_unfilled": len(self.unfilled),
        }
        if self.filled:
            by_session = Counter(h.isoformat() for m in self.filled.values() for h in m)
            by_source = Counter(src for m in self.filled.values() for src in m.values())
            log.warning(
                "price_cache hole fill: the vendor answer was missing %d interior NYSE "
                "session bar(s) across %d ticker(s); filled before upload (by session: "
                "%s; by source: %s; sample: %s) — see alpha-engine-config-I11553",
                sum(by_session.values()), len(self.filled), dict(sorted(by_session.items())),
                dict(by_source), sorted(self.filled)[:_SAMPLE],
            )
            summary["filled_by_session"] = dict(by_session)
        if self.unfilled:
            by_session = Counter(h.isoformat() for m in self.unfilled.values() for h in m)
            by_reason = Counter(r for m in self.unfilled.values() for r in m.values())
            recent = any(h >= self._alert_from for m in self.unfilled.values() for h in m)
            (log.error if recent else log.warning)(
                "price_cache hole fill: %d interior NYSE session bar(s) across %d "
                "ticker(s) could NOT be filled and are published missing (by session: "
                "%s; by reason: %s; sample: %s)%s — see alpha-engine-config-I11553",
                sum(by_session.values()), len(self.unfilled), dict(sorted(by_session.items())),
                dict(by_reason), sorted(self.unfilled)[:_SAMPLE],
                "" if recent else " [all older than the D19 staging retention: long-standing gaps]",
            )
            summary["unfilled_by_session"] = dict(by_session)
        return summary
