"""Finnhub analyst adapter — FREE.

Pulls analyst-recommendation buckets (strongBuy/buy/hold/sell/strongSell
counts) from Finnhub's ``/stock/recommendation`` endpoint. Free tier
supports US tickers.

Note: Finnhub's `/stock/price-target` endpoint requires a paid tier
(returns 403 on free). Mean target therefore stays None on this
adapter — pair with the yfinance adapter for the target via the
snapshotter's multi-source merge.

Normalizes the per-bucket counts to a single consensus_rating using
the same plurality rule used in ``collectors/alternative.py::_fetch_analyst``:

  bullish = strongBuy + buy
  bearish = sell + strongSell
  bullish > bearish AND bullish >= hold       → "buy"
  bearish > bullish                            → "sell"
  otherwise                                    → "hold"

We use the canonical 5-class ladder ("strongBuy" / "buy" / "hold" /
"sell" / "strongSell"). Today this adapter emits "buy" / "hold" /
"sell" — the strong tiers require ratio-thresholding which is a
follow-up.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from nousergon_lib.sources import AnalystSnapshot

logger = logging.getLogger(__name__)


class FinnhubAnalystAdapter:
    """Finnhub analyst data adapter. Implements ``AnalystSource``."""

    name = "finnhub"

    def __init__(self, finnhub_get_fn: Any = None) -> None:
        self._finnhub_get = finnhub_get_fn

    def _get_fetcher(self):
        if self._finnhub_get is None:
            from collectors.finnhub_client import finnhub_get
            self._finnhub_get = finnhub_get
        return self._finnhub_get

    def fetch(self, ticker: str) -> AnalystSnapshot | None:
        try:
            data = self._get_fetcher()("stock/recommendation", {"symbol": ticker})
        except Exception as e:
            logger.warning(
                "[finnhub_analyst] fetch failed for %s: %s", ticker, e,
            )
            return None
        if not isinstance(data, list) or not data:
            return None

        latest = data[0]
        totals = {
            k: int(latest.get(k, 0) or 0)
            for k in ("strongBuy", "buy", "hold", "sell", "strongSell")
        }
        total = sum(totals.values())
        if total == 0:
            return None

        bullish = totals["strongBuy"] + totals["buy"]
        bearish = totals["sell"] + totals["strongSell"]
        if bullish > bearish and bullish >= totals["hold"]:
            rating = "buy"
        elif bearish > bullish:
            rating = "sell"
        else:
            rating = "hold"

        return AnalystSnapshot(
            ticker=ticker,
            source=self.name,
            fetched_at=datetime.now(timezone.utc),
            consensus_rating=rating,
            mean_target=None,  # paid-tier only
            median_target=None,
            num_analysts=total,
            rating_changes_30d=(),  # require trend tracking against prior periods
        )
