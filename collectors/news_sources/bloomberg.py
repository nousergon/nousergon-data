"""Bloomberg news adapter — PAID, not yet wired.

Stub for Phase 4. Bloomberg's institutional news feed requires a
Terminal subscription + the BPIPE/B-PIPE message bus or the Enterprise
API. Drop-in implementation when BBG_API_KEY (or LP equivalent) lands.

Note: Bloomberg licensing typically requires per-seat agreements;
expect significant subscription cost in Phase 4 budget planning.
"""

from __future__ import annotations

from nousergon_lib.sources import NewsArticle


class BloombergNewsAdapter:
    name = "bloomberg"

    def __init__(self) -> None:
        raise NotImplementedError(
            "BloombergNewsAdapter is a Phase 4 paid-tier stub. "
            "Requires institutional Terminal + BPIPE access. "
            "See ~/Development/alpha-engine-docs/private/data-revamp-260513.md."
        )

    def fetch(
        self,
        tickers: list[str],
        *,
        hours: int = 48,
    ) -> list[NewsArticle]:
        raise NotImplementedError
