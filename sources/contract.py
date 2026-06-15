"""
sources/contract.py — the canonical, provider-agnostic market-data contract.

``alpha-engine-data`` is the SOLE market-data producer for the whole Nous Ergon
system; every other module (Metron, predictor, research, backtester) is a pure
S3 consumer. This module defines the API-neutral price record (:class:`PriceBar`)
that every upstream vendor is normalized INTO, and the
:class:`PriceSourceAdapter` port that each vendor (yfinance, Polygon, FRED, and —
post-beta — Databento / Twelve Data) plugs into. Swapping or adding a vendor is
then "implement one adapter + register it", with zero change to the persisted
artifacts or any downstream consumer.

**Phase 1a (this landing):** the contract + port + a registry of the three
existing sources, additively, WITHOUT yet rewiring
``collectors.daily_closes.collect``. The adapters faithfully wrap today's
``_fetch_*_closes`` implementations so output is byte-identical to the live
pipeline. Phase 1b moves the implementations in and makes ``collect()`` dispatch
through the registry / config-driven routing.

See ``sources/SCHEMA.md`` and alpha-engine-config#1082.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Optional, Protocol, runtime_checkable


# The exact persisted record keys — the existing ``staging/daily_closes/{date}.parquet``
# columns (capitalized OHLCV) + provenance. ``PriceBar.to_record()`` reproduces
# THIS dict precisely, so an adapter-driven ``collect()`` (Phase 1b) is
# byte-identical to today. ``currency`` is intentionally NOT a persisted key yet
# (see SCHEMA.md §4).
RECORD_KEYS: tuple[str, ...] = (
    "ticker", "date", "Open", "High", "Low", "Close",
    "Adj_Close", "Volume", "VWAP", "source",
)


@dataclass(frozen=True)
class PriceBar:
    """One ticker's OHLCV bar for one trading day — normalized, provider-neutral.

    The capitalized-OHLCV persisted columns map to these lowercase fields;
    ``to_record()`` / ``from_record()`` bridge to the legacy dict. ``currency``
    (ISO-4217) is carried in-memory now for international readiness (decision
    2026-06-15) and materialized to the persisted artifact in a later phase.
    See ``sources/SCHEMA.md``.
    """

    ticker: str                    # canonical store-key (dash form: BRK-B), caret-stripped
    date: str                      # YYYY-MM-DD trading day of the bar
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int                    # raw shares (0 when the source carries no volume, e.g. FRED)
    source: str                    # provenance — the producing adapter's name
    currency: str = "USD"          # ISO-4217 native currency of the listing
    vwap: Optional[float] = None   # true volume-weighted price; None when the source can't provide it

    def to_record(self) -> dict:
        """Reproduce the EXACT legacy pipeline record dict.

        ``currency`` is omitted — it is not (yet) a persisted column; the
        persisted schema is unchanged in Phase 1a. See SCHEMA.md §4.
        """
        return {
            "ticker": self.ticker,
            "date": self.date,
            "Open": self.open,
            "High": self.high,
            "Low": self.low,
            "Close": self.close,
            "Adj_Close": self.adj_close,
            "Volume": self.volume,
            "VWAP": self.vwap,
            "source": self.source,
        }

    @classmethod
    def from_record(cls, r: dict, *, currency: str = "USD") -> "PriceBar":
        """Build a PriceBar from a legacy pipeline record dict."""
        return cls(
            ticker=r["ticker"],
            date=r["date"],
            open=r["Open"],
            high=r["High"],
            low=r["Low"],
            close=r["Close"],
            adj_close=r["Adj_Close"],
            volume=r["Volume"],
            vwap=r["VWAP"],
            source=r["source"],
            currency=currency,
        )


# Canonical PriceBar field names — the authoritative set the schema-contract test
# cross-checks against ``sources/SCHEMA.md``.
PRICEBAR_FIELDS: tuple[str, ...] = tuple(f.name for f in fields(PriceBar))


@dataclass(frozen=True)
class SourceCapabilities:
    """What a provider adapter can supply — lets the orchestrator route by need.

    e.g. only adapters with ``vwap=True`` are asked for true VWAP; international
    holdings route to an adapter whose ``regions`` include the listing's market.
    """

    vwap: bool                       # provides true volume-weighted VWAP
    adjusted_close: bool             # provides a split/dividend-adjusted close distinct from close
    intraday: bool                   # can supply intraday / real-time (vs EOD only)
    regions: tuple[str, ...]         # coverage, e.g. ("US",) or ("US", "EU", "HK")
    asset_classes: tuple[str, ...]   # e.g. ("equity", "etf", "index")


@runtime_checkable
class PriceSourceAdapter(Protocol):
    """The provider-agnostic ingestion port. One implementation per vendor.

    Implementations: ``PolygonAdapter``, ``FredAdapter``, ``YfinanceAdapter``
    today; ``DatabentoAdapter`` / ``TwelveDataAdapter`` are a drop-in away.
    """

    name: str
    capabilities: SourceCapabilities

    def map_symbol(self, ticker: str) -> str:
        """Translate a canonical store-key to this vendor's symbol convention.

        (e.g. ``BRK-B`` → ``BRK.B`` for Polygon.) The persisted record always
        keeps the canonical store-key; this mapping is only for talking to the
        vendor.
        """
        ...

    def fetch_ohlcv(
        self, tickers: list[str], run_date: str, *, strict: bool = False
    ) -> list[PriceBar]:
        """Fetch one trading day's bars for ``tickers``, normalized to PriceBar.

        ``strict=True`` asks the adapter to RAISE on source failure / empty
        result (mirrors today's ``*_only`` modes) rather than degrade silently.
        """
        ...
