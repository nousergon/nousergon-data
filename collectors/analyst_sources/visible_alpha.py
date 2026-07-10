"""Visible Alpha analyst adapter — PAID, not yet wired.

Stub for Phase 4. Visible Alpha aggregates sell-side analyst models
with per-line-item estimates (revenue, EPS, FCF by segment), real-
time revisions, and a normalized "Standard Estimates" view. When
wired, this adapter exposes both the headline consensus AND the
per-segment estimate distribution that other vendors don't expose.
"""

from __future__ import annotations

from nousergon_lib.sources import AnalystSnapshot


class VisibleAlphaAnalystAdapter:
    name = "visible_alpha"

    def __init__(self) -> None:
        raise NotImplementedError(
            "VisibleAlphaAnalystAdapter is a Phase 4 paid-tier stub. "
            "See ~/Development/alpha-engine-docs/private/data-revamp-260513.md."
        )

    def fetch(self, ticker: str) -> AnalystSnapshot | None:
        raise NotImplementedError
