"""What a symbol in Metron's published universe may be (alpha-engine-config-I10937).

``metron/holdings_universe.json`` and ``metron/watchlist_universe.json`` are the
held-ticker contract this collector reads: the D36 news fan-out takes its held
symbols from them, and the EOD spine grades its cardinality guard against them
(floor 1.0). On 2026-09-15 a Metron demo fixture published 25 ``DEMO-``
synthetics into that contract. No vendor will ever price them, so the guard could
never read 1.0 and three real gaps sat unnoticed among 25 synthetic misses.

The producer was fixed at the source (metron#486: the demo tenant is excluded at
the query root, with ``_assert_no_demo_symbols`` as its backstop). This module is
the consumer half: a symbol in a declared synthetic namespace is a PRODUCER
CONTRACT VIOLATION. The loaders keep it out of the universe and say so: an ERROR
naming every offending symbol, and a counted rejection on the loader's own record,
so the read is never silently filtered. A silent filter is how the original
contamination stayed invisible for two days.

Declaring these symbols in ``contracts/exclusions/unpriced_symbols.yaml`` is not
an alternative. Its classes describe real instruments a vendor cannot price; a
synthetic demo ticker is not an instrument.
"""

from __future__ import annotations

from typing import Iterable

#: Declared synthetic namespaces. A symbol starting with one of these is never a
#: real tradable instrument, whatever tenant published it.
FORBIDDEN_SYMBOL_PREFIXES: tuple[str, ...] = ("DEMO-",)


def is_forbidden_symbol(symbol: object) -> bool:
    """True when ``symbol`` sits in a declared synthetic namespace."""
    return str(symbol).strip().upper().startswith(FORBIDDEN_SYMBOL_PREFIXES)


def forbidden_symbols(symbols: Iterable[object]) -> list[str]:
    """The distinct forbidden symbols in ``symbols``, normalised and sorted."""
    return sorted({str(s).strip().upper() for s in symbols if is_forbidden_symbol(s)})


def describe_rejection(source: str, rejected: list[str], limit: int = 10) -> str:
    """One line naming the rejection, for the log and the loader's record."""
    shown = ", ".join(rejected[:limit])
    more = f" (+{len(rejected) - limit} more)" if len(rejected) > limit else ""
    return (
        f"{source}: rejected {len(rejected)} symbol(s) in a forbidden synthetic "
        f"namespace {list(FORBIDDEN_SYMBOL_PREFIXES)}: {shown}{more}"
    )
