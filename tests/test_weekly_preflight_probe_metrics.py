"""A non-pipeline invocation never writes the weekly preflight's health series.

alpha-engine-config-I12838 (console row `alpha-engine-weekly-preflight`,
measured 2026-11-02): the v1 weekly SF had not run since 2026-10-24, yet the
`AlphaEngine/WeeklyPreflight` series carried about two invocations a day. Each
was a pre-spend gate probe (`check_prespend_gate_arm.py` stamps
`invocation_kind: probe`) arriving with no `run_date`, and the console graded
the component FAILED from the series those probes wrote.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
import types
from pathlib import Path

import pytest

INDEX_PY = (
    Path(__file__).resolve().parent.parent
    / "infrastructure" / "lambdas" / "weekly-preflight" / "index.py"
)


@pytest.fixture
def index(monkeypatch):
    calls: list[dict] = []

    class _CW:
        def put_metric_data(self, **kwargs):
            calls.append(kwargs)

    fake_boto3 = types.SimpleNamespace(client=lambda name, **_: _CW())
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    spec = importlib.util.spec_from_file_location("weekly_preflight_index_probe", INDEX_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._calls = calls
    return mod


_COUNTS = dict(
    ran_count=0, skip_count=0, warn_count=0, fail_count=3,
    required_skip_count=0, declared_count=15,
)


@pytest.mark.parametrize("kind", ["probe", "deploy_smoke"])
def test_non_pipeline_invocation_writes_nothing(index, kind):
    out = index._emit_preflight_metrics(status="FAIL", invocation_kind=kind, **_COUNTS)
    assert index._calls == []
    assert out["emitted"] is False
    assert out["skipped"] == f"invocation_kind={kind}"


@pytest.mark.parametrize("kind", [None, "sf", "execution"])
def test_pipeline_invocation_still_writes_the_series(index, kind):
    out = index._emit_preflight_metrics(status="FAIL", invocation_kind=kind, **_COUNTS)
    assert out["emitted"] is True
    assert len(index._calls) == 1
    names = {m["MetricName"] for m in index._calls[0]["MetricData"]}
    assert {"ChecksRan", "ChecksFailed", "AssertionsDeclared"} <= names


def test_every_emission_site_forwards_the_invocation_kind():
    """A call site that forgets the kwarg would re-open the leak silently."""
    tree = ast.parse(INDEX_PY.read_text())
    sites = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "_emit_preflight_metrics"
    ]
    assert len(sites) >= 3, "expected the import-error, run-error and terminal emission sites"
    missing = [n.lineno for n in sites if "invocation_kind" not in {k.arg for k in n.keywords}]
    assert not missing, f"_emit_preflight_metrics call(s) without invocation_kind at lines {missing}"


def test_deploy_smoke_invoke_names_itself():
    deploy = (INDEX_PY.parent / "deploy.sh").read_text()
    assert '"invocation_kind": "deploy_smoke"' in deploy
