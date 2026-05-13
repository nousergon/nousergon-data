"""Benzinga news adapter — PAID, not yet wired.

Stub for Phase 4 subscription upgrade. Drop-in implementation when a
Benzinga API key lands: implement ``fetch()`` against the Benzinga
News endpoint (https://docs.benzinga.io/) returning ``NewsArticle``
records.

Until then, instantiating this adapter raises a configuration error so
a future operator wiring it sees the gap immediately rather than
silent fallthrough.
"""

from __future__ import annotations

from alpha_engine_lib.sources import NewsArticle


class BenzingaNewsAdapter:
    """Stub. Phase 4 — wire when BENZINGA_API_KEY is provisioned."""

    name = "benzinga"

    def __init__(self) -> None:
        raise NotImplementedError(
            "BenzingaNewsAdapter is a Phase 4 paid-tier stub. "
            "Wire against the Benzinga News endpoint "
            "(https://docs.benzinga.io/) when BENZINGA_API_KEY lands. "
            "See ~/Development/alpha-engine-docs/private/data-revamp-260513.md."
        )

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        raise NotImplementedError
