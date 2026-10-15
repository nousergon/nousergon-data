"""Declared corporate actions — the reviewed, in-repo source for actions the
vendor feed does not carry (alpha-engine-config-I11806).

WHY THIS EXISTS:
    Every other corporate action reaches the price stores from polygon's split
    feed, and the restatement design leans on the vendor for two things: the
    record itself, and a vendor-adjusted history that already reflects it after
    any refetch. A large spin-off has neither. Corteva (CTVA) distributed one
    share of Vylor (VYLR) per CTVA share on 2026-10-01 and moved ~84% of its
    value into VYLR; polygon published no split-style record for it, and both
    the yfinance 10-year price cache and polygon's adjusted aggregates still show
    a raw 77.65 -> 12.57 close (a false -84% day) at the ex-date.

    A declared action fixes the record half. This module does NOT write the
    registry and does NOT use applied markers. A marker means "this store is on
    the adjusted basis now", which is false the moment a vendor refetch rewrites
    the history unadjusted, and the 10-year cache is rewritten that way every
    time it is refreshed. So a declared action is gated on EVIDENCE instead:
    :func:`corporate_actions.apply_declared` restates a series only when its own
    raw close still prints the declared factor at the ex-date boundary. That
    makes it idempotent (a flattened series is a no-op), safe against a vendor
    that later adjusts on its own (also a no-op, never a double adjustment), and
    self-healing after every refetch.

THE FACTOR (CRSP relative-value convention):
    ``price_factor = P_parent_ex / (P_parent_ex + r * P_spin_ex)`` where
    ``P_parent_ex`` is the parent's first ex-date close, ``P_spin_ex`` the
    spun-off company's first regular-way close, and ``r`` the distribution ratio
    (spun-off shares per parent share). Every parent price strictly before the
    ex-date is multiplied by it. Volume is NOT scaled: a spin-off does not change
    the parent's share count, unlike a split.

    The other common formula, ``(P_prev - r * P_spin_ex) / P_prev``, charges the
    whole ex-date move to the small remaining parent; for CTVA it gives 0.1209
    and turns 2026-10-01 into a fabricated +33.9% day. The relative-value factor
    gives +4.1%, which is what a holder of one CTVA share actually earned
    (12.57 + 68.26 = 80.83 against 77.65).

ADDING OR RETIRING AN ENTRY:
    An entry is a reviewed value, so it changes by PR only. Remove an entry once
    the vendor publishes its own record for the same ticker and ex-date
    (:func:`merge_declared` already lets the vendor record win in the meantime),
    or once the ex-date is older than every stored history window.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "DeclaredSpinoff",
    "DECLARED_SPINOFFS",
    "crsp_spinoff_factor",
    "declared_actions",
    "declared_tickers",
    "merge_declared",
]


def crsp_spinoff_factor(
    parent_ex_close: float, spin_ex_close: float, distribution_ratio: float,
) -> float:
    """The CRSP relative-value price factor for pre-ex parent prices.

    Raises ``ValueError`` on a non-positive input: a malformed declaration must
    fail at import, never become a silent 1.0 or a negative price.
    """
    p, s, r = float(parent_ex_close), float(spin_ex_close), float(distribution_ratio)
    if not (p > 0 and s > 0 and r > 0):
        raise ValueError(
            f"spin-off factor inputs must be positive, got parent={p!r}, "
            f"spin={s!r}, ratio={r!r}"
        )
    return p / (p + r * s)


@dataclass(frozen=True)
class DeclaredSpinoff:
    """One declared spin-off, with the inputs its factor is derived from.

    The factor is DERIVED, never typed in, so the number that reaches the price
    stores is always the one the cited inputs produce.
    """

    ticker: str            # the parent, whose pre-ex history is restated
    spun_ticker: str       # the distributed company
    ex_date: str           # YYYY-MM-DD; rows strictly before it are restated
    distribution_ratio: float  # spun-off shares per parent share
    parent_ex_close: float     # parent's first ex-date close
    spin_ex_close: float       # spun-off company's first regular-way close
    sources: tuple[str, ...]   # where the ratio, dates and closes come from
    why: str

    @property
    def price_factor(self) -> float:
        return crsp_spinoff_factor(
            self.parent_ex_close, self.spin_ex_close, self.distribution_ratio,
        )


DECLARED_SPINOFFS: tuple[DeclaredSpinoff, ...] = (
    DeclaredSpinoff(
        ticker="CTVA",
        spun_ticker="VYLR",
        ex_date="2026-10-01",
        distribution_ratio=1.0,
        parent_ex_close=12.57,
        spin_ex_close=68.26,
        sources=(
            # Ratio, record date (2026-09-24) and distribution date (before the
            # open on 2026-10-01, VYLR regular-way from that open): Corteva's
            # Form 8-K, Exhibit 99.1.
            "https://www.sec.gov/Archives/edgar/data/0001755672/"
            "000119312526391369/d71834dex991.htm",
            # Closes: this pipeline's own polygon archive,
            # s3://alpha-engine-research/staging/daily_closes/2026-10-01.parquet
            # (CTVA 12.57, VYLR 68.26; CTVA 2026-09-30 77.65).
            "s3://alpha-engine-research/staging/daily_closes/2026-10-01.parquet",
        ),
        why=(
            "Corteva distributed 1 VYLR per CTVA share on 2026-10-01; polygon "
            "published no split-style record (alpha-engine-config-I11806)."
        ),
    ),
)


def declared_actions() -> list:
    """Every declared action as a :class:`corporate_actions.CorporateAction`."""
    from corporate_actions import CorporateAction  # local: avoid import cycle

    return [
        CorporateAction.from_spinoff(
            d.ticker,
            d.ex_date,
            d.price_factor,
            spun_ticker=d.spun_ticker,
            raw={
                "distribution_ratio": d.distribution_ratio,
                "parent_ex_close": d.parent_ex_close,
                "spin_ex_close": d.spin_ex_close,
                "sources": list(d.sources),
                "why": d.why,
            },
        )
        for d in DECLARED_SPINOFFS
    ]


def declared_tickers() -> frozenset[str]:
    return frozenset(d.ticker for d in DECLARED_SPINOFFS)


def merge_declared(vendor_actions: list, declared: list | None = None) -> list:
    """The declared actions a caller should apply alongside ``vendor_actions``.

    A declared action yields to any vendor record for the same ticker and
    ex-date: the vendor's own adjusted history is then the authority, and
    applying both would stack two adjustments for one event.
    """
    declared = declared_actions() if declared is None else declared
    vendor_keys = {(a.ticker, a.ex_date) for a in vendor_actions or []}
    return [a for a in declared if (a.ticker, a.ex_date) not in vendor_keys]
