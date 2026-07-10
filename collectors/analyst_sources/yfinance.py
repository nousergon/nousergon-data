"""yfinance analyst adapter — FREE.

Pulls consensus price target + analyst-opinion count from
``yfinance.Ticker(ticker).info`` (the ``targetMeanPrice`` /
``targetMedianPrice`` / ``numberOfAnalystOpinions`` /
``recommendationKey`` fields).

yfinance scrapes Yahoo Finance's HTML/API, so coverage tracks what
Yahoo surfaces — usually good for US large/mid-cap and patchy below.

Normalizes Yahoo's recommendationKey strings to the canonical 5-class
ladder (``strongBuy``/``buy``/``hold``/``sell``/``strongSell``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from nousergon_lib.sources import AnalystSnapshot

logger = logging.getLogger(__name__)


# yfinance returns recommendationKey strings in lower_snake_case;
# normalize to camelCase to match the canonical ladder.
_RATING_NORMALIZE = {
    "strong_buy": "strongBuy",
    "buy": "buy",
    "hold": "hold",
    "underperform": "sell",
    "sell": "sell",
    "strong_sell": "strongSell",
}


class YfinanceAnalystAdapter:
    """yfinance analyst data adapter. Implements ``AnalystSource``."""

    name = "yfinance"

    def __init__(self, yf_module: Any = None) -> None:
        self._yf = yf_module  # default lazy-import

    def _get_yf(self) -> Any:
        if self._yf is None:
            import yfinance
            self._yf = yfinance
        return self._yf

    def fetch(self, ticker: str) -> AnalystSnapshot | None:
        try:
            info = self._get_yf().Ticker(ticker).info
        except Exception as e:
            logger.warning(
                "[yfinance_analyst] fetch failed for %s: %s", ticker, e,
            )
            return None
        if not isinstance(info, dict) or not info:
            return None

        rec_key = (info.get("recommendationKey") or "").lower()
        consensus_rating = _RATING_NORMALIZE.get(rec_key)

        mean_target = _as_float(info.get("targetMeanPrice"))
        median_target = _as_float(info.get("targetMedianPrice"))
        num_analysts = _as_int(info.get("numberOfAnalystOpinions"))

        return AnalystSnapshot(
            ticker=ticker,
            source=self.name,
            fetched_at=datetime.now(timezone.utc),
            consensus_rating=consensus_rating,
            mean_target=mean_target,
            median_target=median_target,
            num_analysts=num_analysts,
            rating_changes_30d=(),  # yfinance.info doesn't expose revisions
        )


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        if f != f:  # NaN
            return None
        return round(f, 4)
    except (TypeError, ValueError):
        return None


def _as_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
