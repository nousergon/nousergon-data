"""config#2226 — sf-watch-spot-dispatcher's `_WATCH_PREFIXES` must stay in
LOCKSTEP with saturday-sf-watch-dispatcher's `PIPELINES` watch prefixes.

The canonical watch_log_key is minted ONLY by saturday-sf-watch-dispatcher's
`_artifact_key(watch_prefix, run_date)`. The spot dispatcher mirrors the
{pipeline_name: watch_prefix} column so an operator re-fire with an EMPTY
watch_log_key can synthesize the same `{prefix}/{run_date}.json` key. A
silent drift between the two would route the repair agent's watch log to a
wrong/orphaned S3 prefix with zero pipeline signal — so drift fails CI here.

Both dicts are extracted STATICALLY (ast over the source text — the same
source-derived pattern as tests/test_sf_watch_deploy_flag_preserve.py):
importing either index.py would drag in boto3 clients and each Lambda's own
pinned nousergon-lib, which this repo-level pytest process does not carry.
"""

from __future__ import annotations

import ast
from pathlib import Path

_LAMBDAS = Path(__file__).parent.parent / "infrastructure" / "lambdas"
_SPOT_DISPATCHER = _LAMBDAS / "sf-watch-spot-dispatcher" / "index.py"
_SATURDAY_DISPATCHER = _LAMBDAS / "saturday-sf-watch-dispatcher" / "index.py"
_LIVENESS_PROBE = _LAMBDAS / "sf-watch-reclaim-sweep-handler" / "index.py"


def _module_assign_value(path: Path, name: str) -> ast.expr:
    """The AST value node of module-level `name = ...` / `name: T = ...`."""
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            if name in targets:
                return node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == name:
                assert node.value is not None, f"{name} annotated but unassigned in {path}"
                return node.value
    raise AssertionError(f"module-level assignment {name!r} not found in {path}")


def _spot_dispatcher_prefixes() -> dict[str, str]:
    value = _module_assign_value(_SPOT_DISPATCHER, "_WATCH_PREFIXES")
    prefixes = ast.literal_eval(value)
    assert isinstance(prefixes, dict) and prefixes, "_WATCH_PREFIXES must be a non-empty dict"
    return prefixes


def _saturday_pipeline_prefixes() -> dict[str, str]:
    """{pipeline_name: watch_prefix} out of PIPELINES — extracted key-by-key
    (the inner dicts carry non-literal values like frozenset(), so a whole-
    dict literal_eval is not possible)."""
    value = _module_assign_value(_SATURDAY_DISPATCHER, "PIPELINES")
    assert isinstance(value, ast.Dict), "PIPELINES must be a dict literal"
    prefixes: dict[str, str] = {}
    for key_node, cfg_node in zip(value.keys, value.values):
        assert isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)
        assert isinstance(cfg_node, ast.Dict), f"PIPELINES[{key_node.value!r}] must be a dict literal"
        for inner_key, inner_value in zip(cfg_node.keys, cfg_node.values):
            if isinstance(inner_key, ast.Constant) and inner_key.value == "watch_prefix":
                assert isinstance(inner_value, ast.Constant) and isinstance(inner_value.value, str)
                prefixes[key_node.value] = inner_value.value
                break
        else:
            raise AssertionError(f"PIPELINES[{key_node.value!r}] has no watch_prefix")
    assert prefixes, "PIPELINES yielded no watch prefixes"
    return prefixes


def test_spot_dispatcher_watch_prefixes_match_saturday_dispatcher_exactly():
    """EXACT equality, both directions: a pipeline added/removed/renamed (or a
    prefix changed) in saturday-sf-watch-dispatcher's PIPELINES must land in
    the spot dispatcher's _WATCH_PREFIXES in the same PR — exactly how the
    TRANSITIONAL alpha-engine-eod-pipeline alias was removed from both
    together at the config#2272 retirement (2026-07-11)."""
    assert _spot_dispatcher_prefixes() == _saturday_pipeline_prefixes()


def test_liveness_probe_sweep_prefixes_match_saturday_dispatcher_exactly():
    """config#2257: the liveness probe's dropped-failure sweep reads the
    canonical watch-log key to decide whether a terminal execution is already
    covered — its `_WATCH_PREFIXES` mirror is the THIRD copy of the
    {pipeline_name: watch_prefix} column and drifts fail CI here exactly like
    the spot dispatcher's copy above (a drifted prefix would make the sweep
    read a wrong/empty log and re-dispatch an already-covered failure)."""
    value = _module_assign_value(_LIVENESS_PROBE, "_WATCH_PREFIXES")
    prefixes = ast.literal_eval(value)
    assert isinstance(prefixes, dict) and prefixes, "_WATCH_PREFIXES must be a non-empty dict"
    assert prefixes == _saturday_pipeline_prefixes()


def test_synthesized_key_shape_matches_artifact_key():
    """The spot dispatcher synthesizes f"{prefix}/{run_date}.json" — the same
    shape as saturday-sf-watch-dispatcher's `_artifact_key`. Guard the shape
    at the source level so a format change there is caught here."""
    saturday_src = _SATURDAY_DISPATCHER.read_text()
    assert 'return f"{watch_prefix}/{run_date}.json"' in saturday_src
    spot_src = _SPOT_DISPATCHER.read_text()
    assert '''f"{prefix}/{fields['run_date']}.json"''' in spot_src
    probe_src = _LIVENESS_PROBE.read_text()
    assert 'f"{prefix}/{run_date}.json"' in probe_src
