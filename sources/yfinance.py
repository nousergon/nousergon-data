"""yfinance price-source adapter.

Same-day OHLCV fallback (and the EOD pass source). Provides a true adjusted
close but NO true VWAP (None is written — never a (H+L+C)/3 proxy, per the
2026-04-17 VWAP-centralization decision).

Phase 1a: faithfully wraps ``collectors.daily_closes._fetch_yfinance_closes``.
"""

from __future__ import annotations

from collectors import daily_closes as _dc

from .contract import PriceBar, SourceCapabilities
from .registry import register


class YfinanceAdapter:
    """yfinance daily OHLCV (broad but uneven international coverage)."""

    name = "yfinance"
    capabilities = SourceCapabilities(
        vwap=False,            # no true volume-weighted VWAP
        adjusted_close=True,   # provides a split/dividend-adjusted close
        intraday=False,        # EOD daily bars here
        regions=("global",),   # broad-but-uneven global coverage
        asset_classes=("equity", "etf", "index"),
    )

    def map_symbol(self, ticker: str) -> str:
        # The legacy fetch strips the caret internally; store-key passes through.
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
        # yfinance has no strict mode (it logs + returns a partial count); the
        # ``strict`` flag is accepted for port-uniformity and ignored.
        return _dc._fetch_yfinance_closes(tickers, run_date, records)

    def fetch_ohlcv(
        self, tickers: list[str], run_date: str, *, strict: bool = False
    ) -> list[PriceBar]:
        records: list[dict] = []
        self.fetch_into(records, tickers, run_date, strict=strict)
        return [PriceBar.from_record(r) for r in records]


register(YfinanceAdapter())
