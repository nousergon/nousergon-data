"""sources/registry.py — registry of available price-source adapters.

Config-driven routing (Phase 1b) resolves source names → adapters through this
registry. Adding a vendor = implement :class:`~sources.contract.PriceSourceAdapter`
and ``register()`` it (the adapter modules self-register on import).
"""

from __future__ import annotations

from .contract import PriceSourceAdapter

_ADAPTERS: dict[str, PriceSourceAdapter] = {}


def register(adapter: PriceSourceAdapter) -> None:
    """Register an adapter under its ``name`` (idempotent — last wins)."""
    _ADAPTERS[adapter.name] = adapter


def get_adapter(name: str) -> PriceSourceAdapter:
    """Return the registered adapter for ``name`` or raise ValueError."""
    try:
        return _ADAPTERS[name]
    except KeyError:
        raise ValueError(
            f"unknown price source {name!r}; registered: {sorted(_ADAPTERS)}"
        ) from None


def available() -> list[str]:
    """Sorted list of registered adapter names."""
    return sorted(_ADAPTERS)
