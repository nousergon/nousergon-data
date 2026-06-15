"""FRED (Federal Reserve Economic Data) price-source adapter.

Serves the index/macro symbols not on polygon's free tier (VIX, VIX3M, TNX, IRX,
TWO, HYOAS, BAA10Y) as daily closes (OHLC all = close; no volume/VWAP).

Phase 1a: faithfully wraps ``collectors.daily_closes._fetch_fred_closes``.
"""

from __future__ import annotations

from collectors import daily_closes as _dc

from .contract import PriceBar, SourceCapabilities
from .registry import register


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
        records: list[dict],
        tickers: list[str],
        run_date: str,
        *,
        strict: bool = False,
        window_cache: dict | None = None,
    ) -> int:
        # FRED supports windowed reconciliation — forward the prefetched cache.
        return _dc._fetch_fred_closes(
            tickers, run_date, records, window_cache=window_cache,
        )

    def fetch_ohlcv(
        self, tickers: list[str], run_date: str, *, strict: bool = False
    ) -> list[PriceBar]:
        records: list[dict] = []
        self.fetch_into(records, tickers, run_date, strict=strict)
        return [PriceBar.from_record(r) for r in records]


register(FredAdapter())
