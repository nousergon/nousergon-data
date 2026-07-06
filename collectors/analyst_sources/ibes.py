"""IBES analyst adapter — PAID, not yet wired.

Stub for Phase 4 subscription upgrade. IBES (Institutional Brokers'
Estimate System, Refinitiv) is the institutional gold standard for
consensus + revisions + recommendations data. Drop-in implementation
when IBES_API_KEY (or Refinitiv Eikon Data API access) lands.

When wired, this adapter's structured output (with full revision
history) becomes the highest-trust analyst source.
"""

from __future__ import annotations

from nousergon_lib.sources import AnalystSnapshot


class IbesAnalystAdapter:
    name = "ibes"

    def __init__(self) -> None:
        raise NotImplementedError(
            "IbesAnalystAdapter is a Phase 4 paid-tier stub. "
            "Requires Refinitiv Eikon Data API access. "
            "See ~/Development/alpha-engine-docs/private/data-revamp-260513.md."
        )

    def fetch(self, ticker: str) -> AnalystSnapshot | None:
        raise NotImplementedError
