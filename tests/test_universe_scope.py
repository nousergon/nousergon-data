"""S-18 ``subset-universe`` chokepoint (alpha-engine-config-I12506).

Three properties:

1. prod is a provable no-op: ``universe_scope.scope`` returns the very object it
   was given, without resolving the registry or touching S3, and every roster
   site in ``weekly_collector.py`` has the shape ``x = universe_scope.scope(<the
   expression x was assigned before>, path=...)``. Together: in prod every one
   of those rosters is the list it was before this change.
2. outside prod with ``scope: subset`` every roster is filtered to the declared
   subset, in roster order; anything unresolvable refuses (never the full roster).
3. no bypass: every function in ``weekly_collector.py`` that forms a roster from
   a constituents source calls the chokepoint, or is exempt here with a reason.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest
from moto import mock_aws

import universe_scope
import weekly_collector

REPO = Path(__file__).resolve().parent.parent
SOURCE = (REPO / "weekly_collector.py").read_text(encoding="utf-8")

#: function -> (path label, roster variable, the expression the variable held before the chokepoint).
#: ``None`` expression: ``x = scope(x)`` (the variable itself).
SITES: dict[str, tuple[str, str, str | None]] = {
    "_run_phase1": ("phase1", "tickers", None),
    "_run_phase2": ("phase2", "universe_tickers", None),
    "_run_morning_enrich": ("morning_enrich", "tickers", None),
    "_self_heal_missing_universe_days": ("universe_gap_heal", "tickers", "sorted(tickers_set)"),
    "_run_chronic_gap_heal": ("chronic_gap_heal", "chronic_tickers", "_load_chronic_polygon_gaps(config)"),
    "_run_morning_arctic_append": ("morning_arctic_append", "tickers", None),
    "_load_daily_universe_tickers": ("daily_universe", "tickers", None),
}

#: Calls that FORM a roster (constituents collect / pointer / run-date reads /
#: Wikipedia fallback / the chronic-gap allowlist).
ROSTER_SOURCES = {
    ("constituents", "collect"),
    ("constituents", "load_from_s3"),
    ("constituents", "_fetch_constituents"),
    (None, "load_constituents_for_run_date"),
    (None, "load_newest_dated_constituents"),
    (None, "_load_chronic_polygon_gaps"),
}

#: Functions that read a roster source without collecting against it.
EXEMPT: dict[str, str] = {
    "_detect_chronic_gap_constituents_drift": (
        "reads constituents as the REFERENCE set of a membership test over chronic_tickers, which "
        "_run_chronic_gap_heal has already scoped; its output is a subset of that scoped roster"
    ),
}


def _functions() -> dict[str, ast.FunctionDef]:
    return {n.name: n for n in ast.walk(ast.parse(SOURCE)) if isinstance(n, ast.FunctionDef)}


def _call_key(call: ast.Call) -> tuple[str | None, str] | None:
    fn = call.func
    if isinstance(fn, ast.Attribute):
        base = fn.value.id if isinstance(fn.value, ast.Name) else None
        return (base, fn.attr)
    if isinstance(fn, ast.Name):
        return (None, fn.id)
    return None


def _scope_assignments(fn: ast.FunctionDef) -> list[ast.Assign]:
    return [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
        and _call_key(n.value) == ("universe_scope", "scope")
    ]


_DECLARED_SUBSET = universe_scope.declared_subset  # the cached function, whatever a test patches


@pytest.fixture(autouse=True)
def _fresh_cache():
    _DECLARED_SUBSET.cache_clear()
    yield
    _DECLARED_SUBSET.cache_clear()


# ── 1. prod is a no-op ────────────────────────────────────────────────────


@pytest.mark.parametrize("ne_env", [None, "", "  ", "prod"])
@pytest.mark.parametrize("roster", [["AAPL", "MSFT", "BRK-B"], [], ("A", "B"), {"X", "Y"}])
def test_prod_returns_the_same_object_without_any_lookup(monkeypatch, ne_env, roster):
    if ne_env is None:
        monkeypatch.delenv("NE_ENV", raising=False)
    else:
        monkeypatch.setenv("NE_ENV", ne_env)

    def _boom(*_a, **_k):
        raise AssertionError("prod must not resolve an environment or read a subset")

    monkeypatch.setattr(universe_scope, "declared_subset", _boom)
    monkeypatch.setattr(universe_scope, "_read_s3_json", _boom)
    for label, _var, _expr in SITES.values():
        assert universe_scope.scope(roster, path=label) is roster


@pytest.mark.parametrize("func", sorted(SITES))
def test_each_site_only_wraps_the_roster_it_had(func):
    """In prod ``scope`` is the identity, so ``x = scope(<old expr>)`` leaves x exactly as before."""
    label, var, expr = SITES[func]
    fn = _functions()[func]
    assigns = _scope_assignments(fn)
    assert len(assigns) == 1, f"{func}: expected exactly one chokepoint call, found {len(assigns)}"
    node = assigns[0]
    assert [ast.unparse(t) for t in node.targets] == [var]
    call = node.value
    assert len(call.args) == 1 and [k.arg for k in call.keywords] == ["path"]
    assert ast.literal_eval(call.keywords[0].value) == label
    assert ast.unparse(call.args[0]) == (expr or var)


def test_path_labels_are_unique():
    labels = [label for label, _v, _e in SITES.values()]
    assert len(labels) == len(set(labels))


def _moto_bucket(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="alpha-engine-research")
    client.put_object(
        Bucket="alpha-engine-research", Key="market_data/weekly/2026-10-06/constituents.json",
        Body=json.dumps({"tickers": ["AAPL", "MSFT", "NVDA", "SKYD"]}).encode(),
    )
    return client


def test_daily_universe_in_prod_is_unchanged(monkeypatch):
    monkeypatch.delenv("NE_ENV", raising=False)
    with mock_aws():
        _moto_bucket(monkeypatch)
        tickers = weekly_collector._load_daily_universe_tickers({"bucket": "alpha-engine-research"}, "2026-10-07")
    assert tickers == weekly_collector._augment_with_macro_daily_tickers(["AAPL", "MSFT", "NVDA", "SKYD"])


# ── 2. subset outside prod ────────────────────────────────────────────────


def _fake_env(monkeypatch, scope_name: str, doc=None):
    import nousergon_lib.env as ne_env

    fake = SimpleNamespace(universe_scope=scope_name, universe_uri="s3://staging-crucible/universe/staging_subset.json")
    monkeypatch.setattr(ne_env, "resolve", lambda name=None, **_k: fake)
    reads: list[str] = []

    def _read(uri):
        reads.append(uri)
        if isinstance(doc, Exception):
            raise doc
        return doc

    monkeypatch.setattr(universe_scope, "_read_s3_json", _read)
    monkeypatch.setenv("NE_ENV", "staging")
    return reads


def test_subset_filters_in_roster_order_and_reads_once(monkeypatch):
    reads = _fake_env(monkeypatch, "subset", {"tickers": ["nvda", "AAPL", "ZZZZ"]})
    assert universe_scope.scope(["MSFT", "AAPL", "NVDA"], path="phase1") == ["AAPL", "NVDA"]
    assert universe_scope.scope(("NVDA", "IBM"), path="phase2") == ("NVDA",)
    assert reads == ["s3://staging-crucible/universe/staging_subset.json"]


def test_subset_accepts_a_bare_list(monkeypatch):
    _fake_env(monkeypatch, "subset", ["AAPL"])
    assert universe_scope.scope(["AAPL", "MSFT"], path="daily_universe") == ["AAPL"]


def test_full_scope_outside_prod_is_a_no_op(monkeypatch):
    reads = _fake_env(monkeypatch, "full")
    roster = ["AAPL", "MSFT"]
    assert universe_scope.scope(roster, path="phase1") is roster
    assert reads == []


@pytest.mark.parametrize(
    "scope_name, doc",
    [
        ("subset", RuntimeError("AccessDenied")),
        ("subset", {"tickers": []}),
        ("subset", {"names": ["AAPL"]}),
        ("partial", None),
    ],
)
def test_outside_prod_refuses_rather_than_falling_back_to_the_full_roster(monkeypatch, scope_name, doc):
    _fake_env(monkeypatch, scope_name, doc)
    with pytest.raises(universe_scope.UniverseScopeError, match=r"\[phase1\]"):
        universe_scope.scope(["AAPL"], path="phase1")


def test_unknown_environment_refuses(monkeypatch):
    monkeypatch.setenv("NE_ENV", "no-such-env-i12506")
    with pytest.raises(universe_scope.UniverseScopeError, match="not resolvable"):
        universe_scope.scope(["AAPL"], path="phase2")


def test_daily_universe_in_staging_is_the_subset_plus_macro_keys(monkeypatch):
    with mock_aws():
        _moto_bucket(monkeypatch)
        _fake_env(monkeypatch, "subset", {"tickers": ["NVDA", "AAPL"]})
        tickers = weekly_collector._load_daily_universe_tickers({"bucket": "alpha-engine-research"}, "2026-10-07")
    assert tickers == weekly_collector._augment_with_macro_daily_tickers(["AAPL", "NVDA"])
    assert "MSFT" not in tickers and "SPY" in tickers


# ── 3. no bypass ──────────────────────────────────────────────────────────


def test_every_roster_forming_function_goes_through_the_chokepoint():
    forming = {}
    for name, fn in _functions().items():
        keys = {_call_key(c) for c in ast.walk(fn) if isinstance(c, ast.Call)}
        if keys & ROSTER_SOURCES:
            forming[name] = bool(_scope_assignments(fn))
    unscoped = sorted(n for n, scoped in forming.items() if not scoped and n not in EXEMPT)
    assert not unscoped, (
        f"{unscoped} form a ticker roster without universe_scope.scope (S-18 subset-universe, "
        "alpha-engine-config-I12506): route the roster through it, or exempt the function in "
        "tests/test_universe_scope.py with the reason"
    )
    assert {n for n, scoped in forming.items() if scoped} == set(SITES)
    assert set(EXEMPT) <= set(forming), "stale EXEMPT entry"
