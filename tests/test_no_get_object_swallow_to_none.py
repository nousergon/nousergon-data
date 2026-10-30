"""Class guard: no new `s3.get_object` load whose failure is swallowed into `None`
(alpha-engine-config-I11069).

I10923 fixed the severe instance: `features/compute.py` swallowed a failed
reference-map load into a neutral `{}`, and every surface read `ok`. Its class
sweep found further sites with the same shape in a milder form: a `try` around
`s3.get_object` whose broad handler returns `None` and never re-raises. `None`
usually fails one frame later, but the cause (throttle, AccessDenied, a corrupt
body) is erased and the failure is reported in the wrong place.

This test pins that shape in `features/`, `builders/` and `collectors/`:

* A handler that re-raises anything (the compliant `daily_closes_fred_repair`
  / `macro` shape: re-raise a non-404 `ClientError`, `None` only on a genuine
  404) is not flagged.
* A handler that catches only a not-found type (`NoSuchKey`, `NotFound`,
  `FileNotFoundError`, `KeyError`) is not flagged: "absent" is an answer, not a
  swallowed failure.
* Every other handler that returns `None` is flagged.

The sites present when this guard landed are grandfathered by
`(path, function)` so a line shift does not churn the list. The set may only
SHRINK. A site leaves it in the PR that makes it raise, or that narrows its
handler and records its rationale per I11069 deliverable 2. The stale-entry
test fails if one is left behind, and a new site fails review.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_TREES = ("features", "builders", "collectors")
_NOT_FOUND_TYPES = ("NoSuchKey", "NotFound", "FileNotFoundError", "KeyError")

# Measured 2026-10-30 against main 4994b1eb. Shrink-only; see the module docstring.
GRANDFATHERED: frozenset[tuple[str, str]] = frozenset(
    {
        ("features/compute.py", "_expected_alternative_count"),
        ("features/reader.py", "read_feature_snapshot"),
        ("features/reader.py", "read_registry"),
        ("collectors/alternative.py", "load_from_s3"),
        ("collectors/alternative.py", "_get_latest_date"),
        ("collectors/constituents.py", "load_from_s3"),
        ("collectors/crypto_balances.py", "_read_json"),
        ("collectors/daily_closes.py", "_read_earlier_root_file"),
        ("collectors/historical_constituents.py", "closes"),
        ("collectors/metron_market_data.py", "_read_json"),
        ("collectors/nasdaq100.py", "load_from_s3"),
        ("collectors/price_cache_holes.py", "_read_daily_closes"),
        ("collectors/price_cache_holes.py", "_read_cached"),
        ("collectors/universe_returns.py", "_load_sector_map"),
    }
)


def _calls_get_object(nodes: list[ast.stmt]) -> bool:
    return any(
        isinstance(c, ast.Call)
        and isinstance(c.func, ast.Attribute)
        and c.func.attr == "get_object"
        for n in nodes
        for c in ast.walk(n)
    )


def _only_not_found(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return all(any(k in ast.unparse(t) for k in _NOT_FOUND_TYPES) for t in types)


def _swallows_to_none(handler: ast.ExceptHandler) -> bool:
    if _only_not_found(handler):
        return False
    nodes = list(ast.walk(handler))
    if any(isinstance(n, ast.Raise) for n in nodes):
        return False
    return any(
        isinstance(n, ast.Return)
        and (
            n.value is None
            or (isinstance(n.value, ast.Constant) and n.value.value is None)
        )
        for n in nodes
    )


def find_sites(source: str, rel: str) -> set[tuple[str, str]]:
    """`(rel, innermost function)` for every get_object swallow-to-None in `source`."""
    found: set[tuple[str, str]] = set()
    funcs = (ast.FunctionDef, ast.AsyncFunctionDef)
    for fn in ast.walk(ast.parse(source)):
        if not isinstance(fn, funcs):
            continue
        nested = {
            id(n)
            for inner in ast.walk(fn)
            if inner is not fn and isinstance(inner, funcs)
            for n in ast.walk(inner)
        }
        for node in ast.walk(fn):
            if id(node) in nested or not isinstance(node, ast.Try):
                continue
            if _calls_get_object(node.body) and any(
                _swallows_to_none(h) for h in node.handlers
            ):
                found.add((rel, fn.name))
    return found


def _repo_sites() -> set[tuple[str, str]]:
    sites: set[tuple[str, str]] = set()
    for tree in _TREES:
        for path in sorted((_REPO_ROOT / tree).rglob("*.py")):
            rel = path.relative_to(_REPO_ROOT).as_posix()
            sites |= find_sites(path.read_text(encoding="utf-8"), rel)
    return sites


def test_no_new_get_object_swallow_to_none():
    new = sorted(_repo_sites() - GRANDFATHERED)
    assert not new, (
        "s3.get_object load failure swallowed into None (alpha-engine-config-I11069). "
        "Raise, re-raise everything but a genuine not-found, or catch only the "
        f"not-found type: {new}"
    )


def test_grandfathered_entries_are_still_needed():
    stale = sorted(GRANDFATHERED - _repo_sites())
    assert not stale, f"fixed or moved; remove from GRANDFATHERED: {stale}"


def test_detector_flags_the_swallow_and_spares_the_compliant_shapes():
    """The detector is not vacuous: one fixture per branch."""
    src = """
def swallow(s3):
    try:
        return s3.get_object(Bucket="b", Key="k")
    except Exception:
        return None

def bare_return(s3):
    try:
        body = s3.get_object(Bucket="b", Key="k")
    except Exception:
        return
    return body

def reraise_non_404(s3):
    try:
        return s3.get_object(Bucket="b", Key="k")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise

def not_found_only(s3):
    try:
        return s3.get_object(Bucket="b", Key="k")
    except s3.exceptions.NoSuchKey:
        return None

def outer(s3):
    def inner():
        try:
            return s3.get_object(Bucket="b", Key="k")
        except Exception:
            return None
    return inner()
"""
    assert find_sites(src, "x.py") == {
        ("x.py", "swallow"),
        ("x.py", "bare_return"),
        ("x.py", "inner"),
    }
