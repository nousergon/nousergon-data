"""RavenPack news adapter — PAID, not yet wired.

Stub for Phase 4. RavenPack provides pre-extracted event records +
sentiment + relevance scores on a per-(article, ticker) basis. When
wired, this adapter's structured output becomes the highest-trust
news source (trust weight ~1.0).
"""

from __future__ import annotations

from nousergon_lib.sources import NewsArticle


class RavenpackNewsAdapter:
    name = "ravenpack"

    def __init__(self) -> None:
        raise NotImplementedError(
            "RavenpackNewsAdapter is a Phase 4 paid-tier stub. "
            "See ~/Development/alpha-engine-docs/private/data-revamp-260513.md."
        )

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        raise NotImplementedError
