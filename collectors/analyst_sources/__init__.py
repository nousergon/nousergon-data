"""Analyst-data adapter implementations.

Concrete adapters implementing the ``nousergon_lib.sources.AnalystSource``
Protocol. Free-tier adapters wired today (yfinance, Finnhub); paid stubs
(IBES, Visible Alpha) fail loud on construction so a future operator
wiring them sees the gap immediately.

Architectural pattern: lib defines the contract; this module is the
producer-side concrete implementation; consumers read producer outputs
from S3 (via the daily snapshotter + derived revisions module).
"""

from collectors.analyst_sources.finnhub import FinnhubAnalystAdapter
from collectors.analyst_sources.ibes import IbesAnalystAdapter
from collectors.analyst_sources.visible_alpha import VisibleAlphaAnalystAdapter
from collectors.analyst_sources.yfinance import YfinanceAnalystAdapter

__all__ = [
    "YfinanceAnalystAdapter",
    "FinnhubAnalystAdapter",
    "IbesAnalystAdapter",
    "VisibleAlphaAnalystAdapter",
]
