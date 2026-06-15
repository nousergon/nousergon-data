"""Polygon grouped-daily price-source adapter.

Phase 1a: faithfully wraps ``collectors.daily_closes._fetch_polygon_closes`` so
output is byte-identical to the live pipeline. Phase 1b moves the implementation
here and inverts the dependency (``collect()`` calls the adapter, not vice-versa).
"""

from __future__ import annotations

from collectors import daily_closes as _dc

from .contract import PriceBar, SourceCapabilities
from .registry import register


class PolygonAdapter:
    """Polygon.io grouped-daily OHLCV + true VWAP (US equities/ETFs)."""

    name = "polygon"
    capabilities = SourceCapabilities(
        vwap=True,            # polygon grouped-daily carries volume-weighted VWAP
        adjusted_close=False,  # Adj_Close == Close (no separate adjustment series)
        intraday=True,         # polygon can supply intraday/real-time (EOD used here)
        regions=("US",),
        asset_classes=("equity", "etf"),
    )

    def map_symbol(self, ticker: str) -> str:
        # Dash store-key → polygon's dot form for class shares (BRK-B → BRK.B).
        return _dc._polygon_symbol(ticker.lstrip("^"))

    def fetch_into(
        self,
        records: list[dict],
        tickers: list[str],
        run_date: str,
        *,
        strict: bool = False,
        window_cache: dict | None = None,
    ) -> int:
        # ``polygon_only`` raises on 403 / empty grouped-daily; ``auto`` degrades.
        # Mutates ``records`` in place (partial appends survive a mid-fetch raise).
        return _dc._fetch_polygon_closes(
            tickers, run_date, records,
            source="polygon_only" if strict else "auto",
        )

    def fetch_ohlcv(
        self, tickers: list[str], run_date: str, *, strict: bool = False
    ) -> list[PriceBar]:
        records: list[dict] = []
        self.fetch_into(records, tickers, run_date, strict=strict)
        return [PriceBar.from_record(r) for r in records]


register(PolygonAdapter())
