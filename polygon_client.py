"""
Polygon.io market data client with rate limiting and dividend adjustment.

Free tier: 5 API calls/min, ~2 years historical depth, EOD data only.
Index tickers (VIX/TNX/IRX) are not available on free tier.

Used by collectors/universe_returns.py for grouped-daily price fetches.

Usage:
    from polygon_client import PolygonClient, polygon_client

    # Singleton (reads POLYGON_API_KEY from env):
    client = polygon_client()
    bars = client.get_daily_bars("AAPL", "2025-01-01", "2026-03-28")

    # All US stocks for a single date:
    prices = client.get_grouped_daily("2026-03-28")
    # -> {"AAPL": {"open": 253.9, "high": 255.5, ...}, ...}
"""

from __future__ import annotations

import logging
import random
import re
import time
from collections import deque
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from nousergon_lib.secrets import get_secret

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.polygon.io"
_MAX_BARS_PER_REQUEST = 50_000  # polygon limit param max

# L4495 (security): polygon authenticates via the ``apiKey`` querystring
# (set on the session in ``__init__``), so ``requests.HTTPError`` /
# ``RequestException`` ``str()`` representations — which embed the full
# effective request URL — leak the live key into logs and operator
# transcripts (confirmed 2026-06-03 from a grouped-daily 500 WARNING).
# Mask ``apiKey=...`` AND ``api_key=...`` before any polygon error string
# is raised or logged. Sibling of ``daily_closes._scrub_api_key``.
_POLYGON_API_KEY_RE = re.compile(r"(?:apiKey|api_key)=[^&\s]+")


def _scrub_api_key(msg: object) -> str:
    """Mask the polygon ``apiKey=...`` (or ``api_key=...``) querystring."""
    return _POLYGON_API_KEY_RE.sub(lambda m: m.group(0).split("=", 1)[0] + "=***", str(msg))


# L4496: polygon 5xx + transient network errors on the grouped-daily TARGET
# fetch were NOT in the retry class — a single transient server error (500/
# 502/503) raised ``HTTPError`` via ``raise_for_status`` and hard-failed the
# whole daily_closes run (4 reruns on 2026-06-03 AM). Bounded exponential
# backoff + full jitter; honors a server ``Retry-After`` when present; fails
# loud after the cap so a sustained outage still surfaces.
_POLYGON_MAX_ATTEMPTS = 4
_POLYGON_BACKOFF_BASE = 1.0   # seconds; wait ≈ base * 2**attempt + U(0, base)
_POLYGON_BACKOFF_CAP = 30.0   # seconds; never wait longer than this between tries


class PolygonRateLimitError(Exception):
    """Raised when rate limit is exhausted and caller should backoff."""


class PolygonForbiddenError(Exception):
    """Raised when polygon returns 403 (free-tier "before end of day", missing/invalid key, etc.).

    Previously `_get` swallowed 403's and returned an empty `{"results": []}` dict,
    which caused `collectors/daily_closes.py` to silently fall through to yfinance —
    masking the real failure (free tier can't access today's data) and silently
    writing VWAP=None for every stock. Per `feedback_no_silent_fails`, callers
    must see the failure and decide whether to abort or escalate.
    """


def _split_ratio_num(value):
    """Parse one polygon split-ratio field, preserving fractional factors.

    Polygon publishes FRACTIONAL ``split_from``/``split_to`` for spinoff-style
    adjustments (``1000:1061``, ``1:1.0517…``; live 2026-06: CCBC ``1:1.2``,
    NRWRF ``20.625:21.625``). The pre-2026-07-02 ``int()`` cast silently
    truncated those (``1.2 → 1``, corrupting the registry's expected factor)
    and raised on numeric strings, degrading the WHOLE window's split
    detection to empty. Integral values normalize to ``int`` so the
    content-addressed action id derivation is unchanged for the (dominant)
    integer-ratio case. Returns ``None`` for a malformed/non-positive value.
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not (f > 0) or f != f or f in (float("inf"), float("-inf")):
        return None
    return int(f) if float(f).is_integer() else f


def _parse_split_row(r: dict, *, require_ticker: bool = False):
    """Parse one ``/v3/reference/splits`` result row; ``None`` + WARN if
    malformed (per-row skip — one bad row must not degrade the whole window's
    split detection to empty, which is what a raised cast did before
    2026-07-02)."""
    exec_date = r.get("execution_date")
    sf = _split_ratio_num(r.get("split_from"))
    st = _split_ratio_num(r.get("split_to"))
    ticker = r.get("ticker")
    if not exec_date or sf is None or st is None or (require_ticker and not ticker):
        logger.warning(
            "polygon splits: skipping malformed record (execution_date=%r, "
            "split_from=%r, split_to=%r, ticker=%r)",
            exec_date, r.get("split_from"), r.get("split_to"), ticker,
        )
        return None
    row = {
        "execution_date": str(exec_date),
        "split_from": sf,
        "split_to": st,
    }
    if ticker:
        row["ticker"] = str(ticker)
    return row


class PolygonClient:
    """Rate-limited polygon.io REST client with dividend adjustment."""

    def __init__(self, api_key: str | None = None, calls_per_min: int = 5):
        self._api_key = api_key or get_secret("POLYGON_API_KEY", required=False, default="")
        if not self._api_key:
            raise ValueError("POLYGON_API_KEY not set")
        self._calls_per_min = calls_per_min
        self._call_times: deque[float] = deque()
        self._session = requests.Session()
        self._session.params = {"apiKey": self._api_key}  # type: ignore[assignment]
        # Per-process cache for grouped-daily responses. Historical grouped-daily
        # data is immutable, and callers (universe_returns) fetch the same
        # calendar dates repeatedly across overlapping eval_date windows
        # (t0, +5d, +10d, +30d). Dedup'ing here cuts the free-tier 5 calls/min
        # rate-limit tax by ~3.5× on backfill runs.
        self._grouped_daily_cache: dict[str, dict[str, dict]] = {}

    # -- Rate limiter --------------------------------------------------------

    def _wait_for_slot(self) -> None:
        """Block until a rate limit slot is available."""
        now = time.monotonic()
        window = 60.0  # 1 minute window
        # Purge old timestamps
        while self._call_times and now - self._call_times[0] > window:
            self._call_times.popleft()
        if len(self._call_times) >= self._calls_per_min:
            wait = window - (now - self._call_times[0]) + 0.5
            logger.debug("Rate limit: waiting %.1fs", wait)
            time.sleep(wait)
            # Purge again after sleep
            now = time.monotonic()
            while self._call_times and now - self._call_times[0] > window:
                self._call_times.popleft()
        self._call_times.append(time.monotonic())

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Make a rate-limited GET request.

        Retries the recoverable class — 429 rate-limit, 5xx server error
        (L4496), and transient network errors (Timeout/ConnectionError) —
        with bounded exponential backoff + full jitter, then fails loud.
        403 (free-tier "before end of day" / bad key) raises immediately as
        ``PolygonForbiddenError`` — never silently swallowed (the prior
        behavior masked the 2026-04-17→23 VWAP outage). All error strings are
        scrubbed of the ``apiKey`` querystring before raising (L4495).
        """
        self._wait_for_slot()
        url = f"{_BASE_URL}{path}"
        for attempt in range(_POLYGON_MAX_ATTEMPTS):
            last = attempt == _POLYGON_MAX_ATTEMPTS - 1
            try:
                resp = self._session.get(url, params=params or {}, timeout=30)
            except (requests.Timeout, requests.ConnectionError) as exc:
                # L4496: a transient network blip on the target fetch must not
                # hard-fail the whole run on the first try — retry, then raise.
                if last:
                    raise requests.ConnectionError(
                        _scrub_api_key(f"polygon transient error on {path} "
                                       f"after {_POLYGON_MAX_ATTEMPTS} attempts: {exc}")
                    ) from None
                self._backoff(attempt, f"polygon transient {type(exc).__name__} on {path}")
                continue

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 15))
                logger.warning("Rate limited (429), waiting %ds", retry_after)
                time.sleep(retry_after)
                self._call_times.clear()  # Reset window after forced wait
                continue
            if resp.status_code == 403:
                # Free tier returns 403 for same-day grouped-daily ("before end
                # of day"). Raise so callers can decide whether to abort the
                # whole pipeline or fall back to a different source — never
                # silently return an empty result set (the prior behavior
                # masked the 2026-04-17 → 2026-04-23 VWAP outage by letting
                # daily_closes.collect fall through to yfinance, which writes
                # VWAP=None).
                try:
                    msg = resp.json().get("message", "Not authorized")
                except (ValueError, KeyError):
                    msg = resp.text[:200] or "Not authorized"
                raise PolygonForbiddenError(
                    _scrub_api_key(f"Polygon 403 on {path}: {msg}")
                )
            if resp.status_code >= 500 and not last:
                # L4496: transient polygon server error — back off + retry
                # before the target-date hard-fail; a sustained 5xx still
                # raises (scrubbed) once the attempts are exhausted.
                retry_after = resp.headers.get("Retry-After")
                self._backoff(
                    attempt, f"polygon {resp.status_code} on {path}",
                    retry_after=retry_after,
                )
                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                # L4495: the HTTPError str embeds the effective URL incl.
                # ``apiKey`` — re-raise with a scrubbed message.
                raise requests.HTTPError(_scrub_api_key(exc), response=resp) from None
            return resp.json()
        raise PolygonRateLimitError(
            f"Rate limited after {_POLYGON_MAX_ATTEMPTS} retries"
        )

    def _backoff(self, attempt: int, reason: str, retry_after: str | None = None) -> None:
        """Sleep ``base * 2**attempt + U(0, base)`` (capped), honoring a
        server ``Retry-After`` header when present. Shared by the 5xx and
        transient-network retry paths in :meth:`_get`."""
        wait = None
        if retry_after is not None:
            try:
                wait = float(retry_after)
            except (TypeError, ValueError):
                wait = None
        if wait is None:
            wait = _POLYGON_BACKOFF_BASE * (2 ** attempt)
        wait = min(wait + random.uniform(0, _POLYGON_BACKOFF_BASE), _POLYGON_BACKOFF_CAP)
        logger.warning(
            "%s — backing off %.1fs (attempt %d/%d)",
            reason, wait, attempt + 1, _POLYGON_MAX_ATTEMPTS,
        )
        time.sleep(wait)

    # -- Core endpoints ------------------------------------------------------

    def get_grouped_daily(self, date_str: str) -> dict[str, dict]:
        """Fetch OHLCV + VWAP for ALL US stocks on a single date.

        Returns {ticker: {"open": float, "high": float, "low": float,
                          "close": float, "volume": float,
                          "vwap": float | None}}

        Responses are cached per-instance (see __init__). Empty results
        (non-trading days) are cached too — same URL returns the same answer.
        """
        if date_str in self._grouped_daily_cache:
            return self._grouped_daily_cache[date_str]
        data = self._get(
            f"/v2/aggs/grouped/locale/us/market/stocks/{date_str}",
            params={"adjusted": "true"},
        )
        results = data.get("results", [])
        parsed = {
            r["T"]: {
                "open": r["o"],
                "high": r["h"],
                "low": r["l"],
                "close": r["c"],
                "volume": r["v"],
                "vwap": r.get("vw"),
            }
            for r in results
            if "T" in r
        }
        self._grouped_daily_cache[date_str] = parsed
        return parsed

    def get_single_day_bar(self, ticker: str, date_str: str) -> dict | None:
        """Fetch a single-day OHLCV+VWAP bar for one ticker.

        Same source and shape as ``get_grouped_daily``'s per-ticker dict,
        but hits the per-ticker ``/aggs/ticker`` endpoint instead of the
        bulk grouped one. Used as a fallback for tickers that polygon's
        grouped-daily endpoint sometimes drops — observed 2026-05-02
        when two grouped calls 4h apart returned non-overlapping
        913-ticker subsets of the same 921 requested. The per-ticker
        endpoint is a different code path on polygon's side and recovers
        most of the transient misses without leaving polygon source.

        Returns ``{"open", "high", "low", "close", "volume", "vwap"}``
        on success, or ``None`` on no-data / 403.
        """
        try:
            data = self._get(
                f"/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}",
                params={"adjusted": "true"},
            )
        except PolygonForbiddenError:
            return None
        results = data.get("results") or []
        if not results:
            return None
        r = results[0]
        return {
            "open": r["o"],
            "high": r["h"],
            "low": r["l"],
            "close": r["c"],
            "volume": r["v"],
            "vwap": r.get("vw"),
        }

    def get_splits(self, ticker: str) -> list[dict]:
        """Fetch the corporate-split history for one ticker (authoritative).

        Polygon is the authoritative corporate-action source (see data#1298):
        yfinance ``auto_adjust`` LAGS a fresh split (it back-adjusts only the
        most recent rows for a day or two), so it cannot be trusted to restate
        a freshly-split ArcticDB universe series. The polygon ``/v3/reference/
        splits`` endpoint carries the exact effective date + ratio the day the
        split lands.

        Returns a list of ``{"execution_date": "YYYY-MM-DD",
        "split_from": int|float, "split_to": int|float}`` sorted ascending by
        date. A forward N-for-1 split is ``split_from=1, split_to=N`` (one old
        share becomes N new shares → adjusted price divides by N); a reverse
        1-for-N split is ``split_from=N, split_to=1``. The per-event
        multiplicative factor applied to prices BEFORE the execution date is
        ``split_from / split_to``. Ratio fields are FRACTIONAL when polygon
        publishes them so (spinoff-style records like ``1000:1061`` or
        ``1:1.0517…``; live examples 2026-06: CCBC ``1:1.2``, NRWRF
        ``20.625:21.625``) — ``int()`` truncation silently corrupted those
        factors before 2026-07-02. Integral values normalize to ``int`` so
        content-addressed action ids stay stable.
        """
        results: list[dict] = []
        try:
            data = self._get(
                "/v3/reference/splits",
                params={"ticker": ticker, "limit": 1000, "order": "asc"},
            )
        except PolygonForbiddenError:
            return []
        for r in data.get("results", []) or []:
            row = _parse_split_row(r)
            if row is not None:
                results.append(
                    {k: row[k] for k in ("execution_date", "split_from", "split_to")}
                )
        results.sort(key=lambda r: r["execution_date"])
        return results

    def get_recent_splits(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch ALL corporate splits executing in ``[start_date, end_date]``.

        Unlike :meth:`get_splits` (one ticker, full history), this queries the
        ``/v3/reference/splits`` endpoint by ``execution_date`` range WITHOUT a
        ticker filter — so the whole market's splits in a window come back in
        ONE call (the endpoint paginates; we take the first page, ample for the
        handful of splits that execute in a ~2-week window). This is what lets
        the daily-closes polygon window skip already-canonical dates while still
        re-fetching dates whose adjusted close a recent split has restated
        (config#717), at a bounded one-call-per-window cost.

        Returns ``[{"ticker": str, "execution_date": "YYYY-MM-DD",
        "split_from": int, "split_to": int}]`` sorted ascending by date. A
        forward N-for-1 split is ``split_from=1, split_to=N``; a reverse 1-for-N
        split is ``split_from=N, split_to=1``. Malformed rows are skipped; a 403
        (free-tier / bad key) returns ``[]`` so the caller degrades to the
        legacy always-fetch behavior rather than hard-failing the window.
        """
        results: list[dict] = []
        try:
            data = self._get(
                "/v3/reference/splits",
                params={
                    "execution_date.gte": start_date,
                    "execution_date.lte": end_date,
                    "limit": 1000,
                    "order": "asc",
                    "sort": "execution_date",
                },
            )
        except PolygonForbiddenError:
            return []
        for r in data.get("results", []) or []:
            row = _parse_split_row(r, require_ticker=True)
            if row is not None:
                results.append(row)
        results.sort(key=lambda r: r["execution_date"])
        return results

    def get_dividends(self, ticker: str) -> list[dict]:
        """Fetch the cash-dividend history for one ticker (authoritative).

        Sibling of :meth:`get_splits` for the corporate-actions program
        (config#1433). Dividends are tracked as a SEPARATE total-return series
        (CRSP/Barra basis) and are NEVER folded into the stored split-adjusted
        price level — this endpoint is the authoritative source of the cash
        amount + ex-dividend date the total-return-factor math consumes.

        Queries ``/v3/reference/dividends?ticker=`` (full history, paginated;
        we take the first page — ``limit=1000`` is ample for decades of
        quarterly dividends). Returns a list of
        ``{"ex_dividend_date": "YYYY-MM-DD", "cash_amount": float,
        "dividend_type": str, ...}`` sorted ascending by ex_dividend_date.
        Malformed rows (missing ex date / non-positive cash amount) are
        skipped; a 403 (free-tier / bad key) returns ``[]`` so a dividend
        detection miss degrades gracefully rather than hard-failing.
        """
        results: list[dict] = []
        try:
            data = self._get(
                "/v3/reference/dividends",
                params={"ticker": ticker, "limit": 1000, "order": "asc"},
            )
        except PolygonForbiddenError:
            return []
        for r in data.get("results", []) or []:
            ex_date = r.get("ex_dividend_date")
            cash = r.get("cash_amount")
            if not ex_date or cash is None:
                continue
            try:
                cash_f = float(cash)
            except (TypeError, ValueError):
                continue
            if cash_f <= 0:
                continue
            results.append(
                {
                    "ex_dividend_date": str(ex_date),
                    "cash_amount": cash_f,
                    "dividend_type": str(r.get("dividend_type") or ""),
                }
            )
        results.sort(key=lambda r: r["ex_dividend_date"])
        return results

    def get_recent_dividends(self, start_date: str, end_date: str) -> list[dict]:
        """Fetch ALL cash dividends going ex in ``[start_date, end_date]``.

        Sibling of :meth:`get_recent_splits` — queries the
        ``/v3/reference/dividends`` endpoint by ``ex_dividend_date`` range
        WITHOUT a ticker filter, so the whole market's dividends in a window
        come back in ONE call (the endpoint paginates; we take the first page,
        ample for a ~2-week window even though dividends are far more frequent
        than splits — hundreds of names go ex per quarter). This is the
        one-call-per-window scan the corporate-actions ``sync`` uses to RECORD
        dividend events into the registry (CRSP-separate: recorded only, never
        applied to a price store).

        Returns ``[{"ticker": str, "ex_dividend_date": "YYYY-MM-DD",
        "cash_amount": float, "dividend_type": str}]`` sorted ascending by ex
        date. Malformed rows (missing ticker / ex date / non-positive cash
        amount) are skipped; a 403 (free-tier / bad key) returns ``[]`` so the
        caller degrades gracefully rather than hard-failing the window.
        """
        results: list[dict] = []
        try:
            data = self._get(
                "/v3/reference/dividends",
                params={
                    "ex_dividend_date.gte": start_date,
                    "ex_dividend_date.lte": end_date,
                    "limit": 1000,
                    "order": "asc",
                    "sort": "ex_dividend_date",
                },
            )
        except PolygonForbiddenError:
            return []
        for r in data.get("results", []) or []:
            ex_date = r.get("ex_dividend_date")
            cash = r.get("cash_amount")
            ticker = r.get("ticker")
            if not ticker or not ex_date or cash is None:
                continue
            try:
                cash_f = float(cash)
            except (TypeError, ValueError):
                continue
            if cash_f <= 0:
                continue
            results.append(
                {
                    "ticker": str(ticker),
                    "ex_dividend_date": str(ex_date),
                    "cash_amount": cash_f,
                    "dividend_type": str(r.get("dividend_type") or ""),
                }
            )
        results.sort(key=lambda r: r["ex_dividend_date"])
        return results

    def get_ticker_events(self, ticker: str) -> list[dict]:
        """Fetch ticker-RENAME events for one ticker (corporate-actions PR6,
        config#1433).

        Queries the polygon ticker-events API
        (``/vX/reference/tickers/{ticker}/events``). polygon resolves the
        ticker to its underlying entity (figi/cik) and returns that entity's
        FULL ticker history as a list of ``ticker_change`` events, each carrying
        the ticker the entity changed TO and the date of the change (e.g. an
        entity that IPO'd as ``FB`` on 2012-05-18 then became ``META`` on
        2022-06-09 returns both as ``ticker_change`` events). Consecutive
        changes — sorted ascending by date — define old->new RENAME PAIRS; the
        EARLIEST event is the initial listing (no prior ticker) and yields no
        pair.

        There is NO whole-market "all renames in a range" endpoint — this API
        is per-ticker — so detection is prune-triggered (a symbol going missing
        from constituents is what flags it), see
        ``corporate_actions.detect_renames``.

        Returns ``[{"date": "YYYY-MM-DD", "old_ticker": str,
        "new_ticker": str}]`` sorted ascending by date (one per adjacent
        transition; no-op pairs where old == new are dropped). A 403
        (free-tier / bad key) returns ``[]`` (mirrors :meth:`get_splits`); the
        apiKey is scrubbed from any raised error by the shared ``_get`` path.

        A 404 with ``{"status": "NOT_FOUND"}`` ALSO returns ``[]`` (config#2812
        finding): verified live that polygon returns this — not a 5xx/timeout —
        for a ticker whose entity record has no events at all, which is the
        expected response for a fully-retired/delisted symbol (confirmed via
        curl: AAPL/META 200, BLD/JHG 404 NOT_FOUND after their 2026-07-01
        delisting). Before this fix, the 404 propagated as an unhandled
        ``requests.HTTPError`` and ``detect_renames`` treated it as a detection
        FAILURE (history-safety skip) — meaning ``prune_delisted_tickers`` could
        never auto-prune the exact class of ticker (genuinely gone from
        polygon's reference set) this check exists to clear. A 404 whose body
        does NOT carry ``status=NOT_FOUND`` (an unexpected shape) still
        propagates, preserving the history-safety default for anything not
        confirmed to mean "no events".

        Any OTHER failure (network / 5xx after retries / unexpected 404 body)
        PROPAGATES — ``detect_renames`` catches it per-candidate and refuses to
        prune that candidate this pass (history-safety: a detection outage must
        never delete a symbol that might be a rename).
        """
        try:
            data = self._get(f"/vX/reference/tickers/{ticker}/events")
        except PolygonForbiddenError:
            return []
        except requests.HTTPError as exc:
            resp = exc.response
            if resp is not None and resp.status_code == 404:
                try:
                    body = resp.json()
                except ValueError:
                    body = {}
                if body.get("status") == "NOT_FOUND":
                    return []
            raise
        results_obj = data.get("results") or {}
        events = results_obj.get("events") or []
        # Collect ascending (date, new_ticker) transitions from ticker_change
        # events; ignore any other event type the endpoint may carry.
        changes: list[dict] = []
        for ev in events:
            if ev.get("type") != "ticker_change":
                continue
            date = ev.get("date")
            tc = ev.get("ticker_change") or {}
            new_t = tc.get("ticker")
            if not date or not new_t:
                continue
            changes.append({"date": str(date), "ticker": str(new_t)})
        changes.sort(key=lambda c: c["date"])
        # Adjacent transitions form the old->new rename pairs. The earliest
        # change is the initial listing (no prior ticker) → no pair.
        pairs: list[dict] = []
        for prev, cur in zip(changes, changes[1:]):
            if prev["ticker"] == cur["ticker"]:
                continue  # defensive: drop a no-op repeat
            pairs.append(
                {
                    "date": cur["date"],
                    "old_ticker": prev["ticker"],
                    "new_ticker": cur["ticker"],
                }
            )
        return pairs

    def get_daily_bars(
        self,
        ticker: str,
        start: str,
        end: str,
        adjusted: bool = True,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV bars for a single ticker.

        Returns DataFrame with DatetimeIndex and columns:
        [Open, High, Low, Close, Volume]
        """
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": _MAX_BARS_PER_REQUEST,
        }
        data = self._get(
            f"/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}",
            params=params,
        )
        results = data.get("results", [])
        if not results:
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

        df = pd.DataFrame(results)
        df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
        df = df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"})
        df = df.set_index("date")[["Open", "High", "Low", "Close", "Volume"]]
        df = df.sort_index()
        return df


# -- Singleton ---------------------------------------------------------------

_singleton: PolygonClient | None = None


def polygon_client(api_key: str | None = None) -> PolygonClient:
    """Get or create a singleton PolygonClient."""
    global _singleton
    if _singleton is None:
        _singleton = PolygonClient(api_key=api_key)
    return _singleton
