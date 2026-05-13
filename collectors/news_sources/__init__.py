"""News-source adapter implementations.

Concrete adapters implementing the ``alpha_engine_lib.sources.NewsSource``
Protocol. Free-tier adapters wired today (Polygon / GDELT / Yahoo RSS);
paid stubs (Benzinga / RavenPack / Bloomberg) fail loud on construction
so a future operator wiring them sees the gap immediately.

Architectural pattern: lib defines the contract; this module is the
producer-side concrete implementation; consumers (alpha-engine-research,
backtester) never import these adapters — they read the producer's
outputs from S3 / RAG.

See ``alpha-engine-docs/private/data-revamp-260513.md`` for the full
4-wave arc plan.
"""

from collectors.news_sources.benzinga import BenzingaNewsAdapter
from collectors.news_sources.bloomberg import BloombergNewsAdapter
from collectors.news_sources.gdelt import GdeltNewsAdapter
from collectors.news_sources.polygon import PolygonNewsAdapter
from collectors.news_sources.ravenpack import RavenpackNewsAdapter
from collectors.news_sources.yahoo_rss import YahooRssNewsAdapter

__all__ = [
    "PolygonNewsAdapter",
    "GdeltNewsAdapter",
    "YahooRssNewsAdapter",
    "BenzingaNewsAdapter",
    "RavenpackNewsAdapter",
    "BloombergNewsAdapter",
]
