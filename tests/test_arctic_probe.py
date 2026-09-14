"""Producer contract tests for collectors/arctic_probe.py (data-collector plan
P-05, alpha-engine-config-I10748).

Mirrors the existing producer-contract pattern (tests/test_technical_rating_contracts.py):
every write this producer makes must validate cleanly against its own versioned
JSON Schema, and the fail-loud "never a partial record" contract is exercised
directly against a fake ArcticDB library rather than trusted by inspection.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import arctic_probe
from contracts import validate_arctic_probe


class _FakeDesc:
    """Stand-in for arcticdb's SymbolDescription — a fake ArcticDB
    description, exposing only the fields _describe_library reads."""

    def __init__(self, row_count: int, last_date=None):
        self.row_count = row_count
        self.date_range = (None, last_date) if last_date is not None else None


class _FakeTimestamp:
    """Stand-in for a pandas Timestamp — only `.date().isoformat()` is used."""

    def __init__(self, iso: str):
        self._iso = iso

    def date(self):
        return self

    def isoformat(self):
        return self._iso


class _FakeLibrary:
    """A fake Arctic library: implements exactly the two methods
    `collectors.arctic_probe._describe_library` calls."""

    def __init__(self, descriptions: dict[str, _FakeDesc] | None = None, *, list_fails=False, batch_fails=False):
        self._descriptions = descriptions or {}
        self._list_fails = list_fails
        self._batch_fails = batch_fails

    def list_symbols(self):
        if self._list_fails:
            raise RuntimeError("simulated list_symbols failure")
        return list(self._descriptions)

    def get_description_batch(self, symbols):
        if self._batch_fails:
            raise RuntimeError("simulated get_description_batch failure")
        return [self._descriptions[s] for s in symbols]


def _healthy_libs():
    return {
        "universe": _FakeLibrary({
            "AAPL": _FakeDesc(2500, _FakeTimestamp("2026-09-12")),
            "MSFT": _FakeDesc(2500, _FakeTimestamp("2026-09-12")),
        }),
        "macro": _FakeLibrary({"VIX": _FakeDesc(3000, _FakeTimestamp("2026-09-12"))}),
        "delisted_history": _FakeLibrary({}),
    }


class TestBuildProbeRecord:
    def test_healthy_probe_validates(self):
        record = arctic_probe.build_probe_record("b", trading_day="2026-09-12", libs=_healthy_libs())
        assert record["schema_version"] == 1
        assert record["trading_day"] == "2026-09-12"
        assert record["libraries"]["universe"]["row_count"] == 5000
        assert record["libraries"]["universe"]["symbol_count"] == 2
        assert record["libraries"]["universe"]["last_index_date"] == "2026-09-12"
        assert record["libraries"]["universe"]["version_id"] is None
        assert record["libraries"]["universe"]["read_ok"] is True
        # An empty library is a legitimate (if degenerate) result, not a failure.
        assert record["libraries"]["delisted_history"] == {
            "row_count": 0, "symbol_count": 0, "last_index_date": None,
            "version_id": None, "read_ok": True,
        }
        assert validate_arctic_probe(record) == []

    def test_minimal_hand_built_fixture_validates(self):
        """Independent of the producer: a schema-conformant fixture must pass on
        its own construction, not merely because it matches the producer's shape."""
        record = {
            "schema_version": 1,
            "as_of_utc": "2026-09-12T23:30:00Z",
            "trading_day": "2026-09-12",
            "libraries": {
                "universe": {
                    "row_count": 5000, "symbol_count": 2, "last_index_date": "2026-09-12",
                    "version_id": None, "read_ok": True,
                },
                "macro": {
                    "row_count": 3000, "symbol_count": 1, "last_index_date": "2026-09-12",
                    "version_id": None, "read_ok": True,
                },
                "delisted_history": {
                    "row_count": 0, "symbol_count": 0, "last_index_date": None,
                    "version_id": None, "read_ok": True,
                },
            },
        }
        assert validate_arctic_probe(record) == []

    def test_missing_library_fails_schema(self):
        record = {
            "schema_version": 1, "as_of_utc": "2026-09-12T23:30:00Z", "trading_day": "2026-09-12",
            "libraries": {
                "universe": {
                    "row_count": 1, "symbol_count": 1, "last_index_date": "2026-09-12",
                    "version_id": None, "read_ok": True,
                },
            },
        }
        assert validate_arctic_probe(record) != []

    def test_a_library_that_cannot_be_listed_raises_never_writes_a_partial_record(self):
        libs = _healthy_libs()
        libs["macro"] = _FakeLibrary(list_fails=True)
        with pytest.raises(arctic_probe.ProbeError, match="macro"):
            arctic_probe.build_probe_record("b", trading_day="2026-09-12", libs=libs)

    def test_a_library_whose_batch_describe_fails_raises(self):
        libs = _healthy_libs()
        libs["universe"] = _FakeLibrary({"AAPL": _FakeDesc(1)}, batch_fails=True)
        with pytest.raises(arctic_probe.ProbeError, match="universe"):
            arctic_probe.build_probe_record("b", trading_day="2026-09-12", libs=libs)

    def test_library_open_failure_raises_before_any_describe(self, monkeypatch):
        def _boom():
            raise RuntimeError("simulated open failure")

        import store.arctic_store as arctic_store

        monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket: _boom())
        with pytest.raises(arctic_probe.ProbeError, match="universe"):
            arctic_probe.build_probe_record("b", trading_day="2026-09-12")


class TestRunProbe:
    def test_run_probe_writes_the_keyed_record_and_returns_it(self):
        puts: dict[str, bytes] = {}

        class _S3:
            def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
                assert Bucket == "b"
                assert ContentType == "application/json"
                puts[Key] = Body

        record = arctic_probe.run_probe(bucket="b", trading_day="2026-09-12", libs=_healthy_libs(), s3_client=_S3())
        assert set(puts) == {"data_collection/probes/arctic/2026-09-12.json"}
        written = json.loads(puts["data_collection/probes/arctic/2026-09-12.json"])
        assert written == record
        assert validate_arctic_probe(written) == []

    def test_run_probe_writes_nothing_on_a_broken_library(self):
        class _S3:
            def put_object(self, **kwargs):
                raise AssertionError("must not write when a library probe failed")

        libs = _healthy_libs()
        libs["delisted_history"] = _FakeLibrary(list_fails=True)
        with pytest.raises(arctic_probe.ProbeError):
            arctic_probe.run_probe(bucket="b", trading_day="2026-09-12", libs=libs, s3_client=_S3())


def test_dispatcher_declares_the_arctic_probe_workload():
    """The dispatcher's _WORKLOADS entry must invoke this module's CLI verbatim
    so the SF-driven in-region run and `python -m collectors.arctic_probe` stay
    the same code path. AST-parsed (not imported) to stay hermetic — mirrors
    infrastructure/data_collection_stack.py::dispatcher_workloads."""
    import ast
    import re

    path = (
        Path(__file__).resolve().parent.parent
        / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"))
    workloads: dict[str, str] | None = None
    workload_re: str | None = None
    for node in ast.walk(tree):
        target = getattr(node, "target", None) or (getattr(node, "targets", None) or [None])[0]
        if isinstance(target, ast.Name) and target.id == "_WORKLOADS":
            workloads = {k.value: v.value for k, v in zip(node.value.keys, node.value.values)}
        if isinstance(target, ast.Name) and target.id == "_WORKLOAD_RE":
            # `re.compile(r"...")` — grab the pattern literal.
            workload_re = node.value.args[0].value
    assert workloads is not None and workload_re is not None
    assert workloads["arctic-probe"] == "python -m collectors.arctic_probe"
    assert re.match(workload_re, "arctic-probe")
