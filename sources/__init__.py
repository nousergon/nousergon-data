"""Pluggable market-data **source adapters** — the provider-agnostic ingestion seam.

``alpha-engine-data`` is the sole market-data producer for the whole system;
this package isolates the upstream VENDOR behind a stable contract
(:class:`~sources.contract.PriceBar` + :class:`~sources.contract.PriceSourceAdapter`)
so swapping or adding a provider (yfinance / Polygon / FRED today; Databento /
Twelve Data next) is "implement one adapter + register it", with zero change to
the persisted artifacts or any downstream consumer.

See ``sources/SCHEMA.md`` and alpha-engine-config#1082.
"""

from __future__ import annotations

from .contract import (
    PRICEBAR_FIELDS,
    RECORD_KEYS,
    PriceBar,
    PriceSourceAdapter,
    SourceCapabilities,
)
from .registry import available, get_adapter, register

# Importing the adapter modules self-registers their instances.
from . import fred as _fred  # noqa: E402,F401
from . import polygon as _polygon  # noqa: E402,F401
from . import yfinance as _yfinance  # noqa: E402,F401

__all__ = [
    "PriceBar",
    "PriceSourceAdapter",
    "SourceCapabilities",
    "RECORD_KEYS",
    "PRICEBAR_FIELDS",
    "register",
    "get_adapter",
    "available",
]
