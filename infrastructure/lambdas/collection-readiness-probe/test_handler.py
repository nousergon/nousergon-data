"""Unit tests for alpha-engine-collection-readiness-probe (alpha-engine-config-I11264).

The probe is a thin door onto `data_gate/run_manifest_predicate.py`; these tests
pin the consumer-side semantics (ready / settled / lookback) against the REAL
predicate and the committed unit descriptors, never a stub of either.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import sys

import pytest

_HERE = pathlib.Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parents[2]
sys.path.insert(0, str(_REPO_ROOT))  # the packaged data_gate/ + registry.d/, as in the zip

from data_gate import run_manifest_predicate  # noqa: E402

_EOD_START = "2026-09-24T20:02:00Z"  # a postclose execution start (~16:02 ET)


def _load_index():
    spec = importlib.util.spec_from_file_location(
        "collection_readiness_probe_index", _HERE / "index.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeS3:
    """Only the two calls the predicate makes, so a third fails loud here."""

    def __init__(self, objects):
        self.objects = dict(objects)

    def list_objects_v2(self, **kw):
        prefix, after = kw["Prefix"], kw.get("StartAfter", "")
        keys = sorted(k for k in self.objects if k.startswith(prefix) and k > after)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg names
        payload = self.objects[Key]

        class _Body:
            def read(self):
                return json.dumps(payload).encode()

        return {"Body": _Body()}


def _d20(**over):
    doc = {
        "schema_version": "data_run_manifest.v1",
        "run_id": "01K5AAAAAAAAAAAAAAAAAAAAAA",
        "unit_id": "D20",
        "trading_day": "2026-09-24",
        "status": "ok",
        "reason": "",
        "started": "2026-09-24T22:16:00Z",
        "finished": "2026-09-24T22:40:00Z",
        "outputs": [
            {"key": "market_data/eod_closes/2026-09-24.json", "rows_out": 88},
            {"key": "market_data/eod_closes/latest.json", "rows_out": 88},
            {"key": "market_data/fx/2026-09-24.json", "rows_out": 12},
            {"key": "market_data/fx/latest.json", "rows_out": 12},
        ],
        "guards": [],
    }
    doc.update(over)
    return doc


_D20_KEY = "data_collection/runs/D20/2026-09-24/01K5AA.json"


@pytest.fixture
def probe(monkeypatch):
    index = _load_index()

    def _with(objects):
        fake = _FakeS3(objects)
        monkeypatch.setattr(run_manifest_predicate, "_s3_client", lambda: fake)
        return index

    return _with


def _event(**over):
    event = {"collection": "eod", "units": ["D20"], "not_before": _EOD_START,
             "lookback_seconds": 0}
    event.update(over)
    return event


def test_ready_when_this_cycles_manifest_is_published(probe):
    out = probe({_D20_KEY: _d20()}).handler(_event(), None)["readiness"]
    assert out["ready"] is True and out["settled"] is True
    assert out["missing"] == [] and out["failed"] == [] and out["failure_mode"] == ""
    assert out["baseline"] == _EOD_START


def test_not_ready_and_not_settled_while_the_producer_has_not_finished(probe):
    """The normal case at 16:05 ET: the collection fires at 18:15. Not an error."""
    stale = _d20(trading_day="2026-09-23", finished="2026-09-23T22:40:00Z")
    out = probe({"data_collection/runs/D20/2026-09-23/01K4ZZ.json": stale}).handler(
        _event(), None
    )["readiness"]
    assert out["ready"] is False and out["settled"] is False
    assert out["missing"] == ["D20"]
    assert out["failure_mode"] == "manifest_missing"


def test_settled_but_not_ready_when_the_run_failed(probe):
    """Waiting longer cannot change a failed manifest: the SF degrades at once."""
    failed = _d20(status="failed", reason="RuntimeError: vendor 429", outputs=[])
    out = probe({_D20_KEY: failed}).handler(_event(), None)["readiness"]
    assert out["ready"] is False and out["settled"] is True
    assert out["failed"] == ["D20"] and out["missing"] == []
    assert out["failure_mode"] == "run_not_ok"


def test_the_lookback_admits_a_producer_that_finished_before_the_consumer_started(probe):
    """Preopen shape: the morning collection fires at 07:30 ET, the preopen SF
    at 08:15 ET, so a manifest that finished at 08:05 ET is this cycle's."""
    doc = _d20(finished="2026-09-24T12:05:00Z")
    index = probe({_D20_KEY: doc})
    start = "2026-09-24T12:15:00Z"
    assert index.handler(_event(not_before=start, lookback_seconds=2700), None)[
        "readiness"]["ready"] is True
    # ...and the same manifest is NOT this cycle's without the lookback.
    assert index.handler(_event(not_before=start, lookback_seconds=0), None)[
        "readiness"]["missing"] == ["D20"]


def test_it_asks_the_shared_predicate_not_a_copy(probe, monkeypatch):
    seen = []
    real = run_manifest_predicate._check_unit

    def spy(s3, unit_id, raw, started_at):
        seen.append(unit_id)
        return real(s3, unit_id, raw, started_at)

    index = probe({_D20_KEY: _d20()})
    monkeypatch.setattr(run_manifest_predicate, "_check_unit", spy)
    index.handler(_event(), None)
    assert seen == ["D20"]


@pytest.mark.parametrize("key", ["workload", "action"])
def test_a_dispatcher_shaped_event_is_refused(probe, key):
    index = probe({})
    with pytest.raises(ValueError, match="launches nothing"):
        index.handler(_event(**{key: "post-market-data"}), None)


def test_an_undeclared_unit_raises_rather_than_reading_ready(probe):
    with pytest.raises(ValueError, match="no descriptor"):
        probe({}).handler(_event(units=["D99"]), None)


def test_an_empty_unit_list_raises(probe):
    with pytest.raises(ValueError, match="no units"):
        probe({}).handler(_event(units=[]), None)


@pytest.mark.parametrize("bad", [-1, "soon"])
def test_a_bad_lookback_raises(probe, bad):
    with pytest.raises(ValueError, match="lookback_seconds"):
        probe({}).handler(_event(lookback_seconds=bad), None)


def test_the_role_can_read_manifests_and_nothing_else():
    """The point of a separate function: it cannot launch, write or send."""
    policy = json.loads((_HERE / "iam-policy.json").read_text(encoding="utf-8"))
    actions = set()
    for stmt in policy["Statement"]:
        acts = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
        actions.update(acts)
    assert actions == {
        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
        "s3:ListBucket", "s3:GetObject",
    }


def test_deploy_packages_the_shared_predicate_and_the_descriptors():
    deploy = (_HERE / "deploy.sh").read_text(encoding="utf-8")
    for path in ("data_gate/__init__.py", "data_gate/descriptors.py",
                 "data_gate/run_manifest_predicate.py", "registry.d/units"):
        assert path in deploy, path


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([os.path.abspath(__file__), "-q"]))
