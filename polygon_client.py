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
import time
from collections import deque
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from alpha_engine_lib.secrets import get_secret

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.polygon.io"
_MAX_BARS_PER_REQUEST = 50_000  # polygon limit param max


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
        """Make a rate-limited GET request. Handles 429 with retry."""
        self._wait_for_slot()
        url = f"{_BASE_URL}{path}"
        for attempt in range(3):
            resp = self._session.get(url, params=params or {}, timeout=30)
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
                    f"Polygon 403 on {path}: {msg}"
                )
            resp.raise_for_status()
            return resp.json()
        raise PolygonRateLimitError("Rate limited after 3 retries")

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
