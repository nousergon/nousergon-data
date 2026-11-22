"""FRED (Federal Reserve Economic Data) price-source adapter.

Serves the index/macro symbols not on polygon's free tier (VIX, VIX3M, TNX, IRX,
TWO, HYOAS, BAA10Y) as daily closes (OHLC all = close; no volume/VWAP).

Phase 1a: faithfully wraps ``collectors.daily_closes._fetch_fred_closes``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, cast

from collectors import daily_closes as _dc

from .contract import PriceBar, SourceCapabilities
from .registry import register

_LegacyRecord = dict[str, object]
_WindowCache = dict[str, list[tuple[str, float]]]


class _FredFetcher(Protocol):
    def __call__(
        self,
        tickers: list[str],
        run_date: str,
        records: list[_LegacyRecord],
        window_cache: _WindowCache | None = None,
    ) -> int: ...


# The legacy conversion uses an unparameterized dict annotation; pin its actual
# adapter-facing type at that untyped boundary.
_price_bar_from_record = cast(Callable[[_LegacyRecord], PriceBar], PriceBar.from_record)


class FredAdapter:
    """FRED daily index/macro closes (single-value; no OHLC spread, no volume)."""

    name = "fred"
    capabilities = SourceCapabilities(
        vwap=False,
        adjusted_close=False,
        intraday=False,
        regions=("US",),
        asset_classes=("index", "macro"),
    )

    def map_symbol(self, ticker: str) -> str:
        # FRED series mapping is internal (``_FRED_INDEX_MAP``); the store-key is
        # carried through unchanged.
        return ticker

    def fetch_into(
        self,
        records: list[_LegacyRecord],
        tickers: list[str],
        run_date: str,
        *,
        strict: bool = False,
        window_cache: _WindowCache | None = None,
    ) -> int:
        # FRED supports windowed reconciliation — forward the prefetched cache.
        # Resolve on each call so tests and callers can replace the legacy fetcher.
        fetch = cast(_FredFetcher, _dc._fetch_fred_closes)  # pyright: ignore[reportPrivateUsage]
        return fetch(
            tickers, run_date, records, window_cache=window_cache,
        )

    def fetch_ohlcv(
        self, tickers: list[str], run_date: str, *, strict: bool = False
    ) -> list[PriceBar]:
        records: list[_LegacyRecord] = []
        self.fetch_into(records, tickers, run_date, strict=strict)
        return [_price_bar_from_record(r) for r in records]


register(FredAdapter())
