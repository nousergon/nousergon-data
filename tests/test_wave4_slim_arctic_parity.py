"""
Wave-4 terminal-state guard — the predictor/price_cache_slim tier is gone.

History: PR0b seeded this file as the migration scaffolding (a slim<->ArcticDB
parity harness + a consumer-set anti-drift lock) while consumers were moved
off slim with a fallback (PR1a macro-breadth, PR1b feature-compute, PR2
backtester exit_timing) and the cutover was observed via WAVE4_PARITY_METRIC.
After the 5/23 parity observation confirmed equivalence, PR4 deleted the slim
writer, the load_slim_cache API, the consumer fallbacks, and the
``predictor/price_cache_slim/`` S3 prefix.

The parity-harness tests are retired with the tier they guarded (lib's own
``test_reconcile`` / ``test_arcticdb`` still cover the substrate). What
remains is a **permanent regression guard**: the slim tier must never come
back. ArcticDB universe/macro libs are the single source of truth.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

# Functional slim surface — a callable definition/call or an import of the
# deleted module. The bare prefix string is tolerated in removal-marker
# comments/docstrings (e.g. "predictor/price_cache_slim/ deleted (Wave-4)");
# what must never return is executable slim machinery.
_SLIM_CODE_RE = re.compile(
    r"\b(def\s+(load|build)_slim_cache"
    r"|(load|build)_slim_cache\s*\("
    r"|import\s+slim_cache"
    r"|from\s+collectors\s+import[^\n]*\bslim_cache\b"
    r"|from\s+collectors\.slim_cache\b"
    r"|from\s+store\.parquet_loader\s+import[^\n]*\bload_slim_cache\b)"
)


_EXCLUDED_PREFIXES = ("tests/", ".venv/", "build/", "dist/", ".git/")


def _production_py_files():
    for p in _REPO.rglob("*.py"):
        rel = p.relative_to(_REPO).as_posix()
        if rel.startswith(_EXCLUDED_PREFIXES) or "/.claude/" in f"/{rel}":
            continue
        yield rel, p


def test_slim_cache_module_deleted():
    assert not (_REPO / "collectors" / "slim_cache.py").exists(), (
        "collectors/slim_cache.py must stay deleted (Wave-4). The slim "
        "writer is gone; ArcticDB universe/macro libs are canonical."
    )


def test_no_functional_slim_surface_in_production():
    """No slim writer/loader definition, call, or import anywhere in
    production code. Regression guard — the tier must not return."""
    offenders = []
    for rel, path in _production_py_files():
        for i, line in enumerate(path.read_text().splitlines(), 1):
            if _SLIM_CODE_RE.search(line):
                offenders.append(f"{rel}:{i}: {line.strip()[:100]}")
    assert not offenders, (
        "Functional slim-cache surface re-introduced (Wave-4 deleted it — "
        "use nousergon_lib.arcticdb load_universe_ohlcv / "
        "load_macro_series instead):\n" + "\n".join(offenders)
    )
