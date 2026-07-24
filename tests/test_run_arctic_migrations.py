"""Unit tests for scripts/run_arctic_migrations.py (alpha-engine-config-I3242,
the in-region ArcticDB migration runner — pairs with the already-merged
schema-migration framework, nousergon-data-PR988).

No real AWS: every boto3-touching function (states:ListExecutions, S3 PUT,
Telegram/flow-doctor) is monkeypatched at the module level. Covers:

  * the mutex probe (running_pipeline_executions / MutexProbeError) —
    fail-CLOSED on a States API error, unlike the fleet's spot dispatchers'
    coverage-beats-dedupe posture (a migration racing a live append is the
    dangerous direction here, not an unwatched incident);
  * resolve_current_version's None -> BASELINE_SCHEMA_VERSION mapping;
  * apply_pending's strict per-migration run()-then-verify() ordering, and
    that it aborts (MigrationRunError, naming the failing migration number)
    the instant either call raises, WITHOUT touching the next migration;
  * the run() orchestration's four terminal states (refused_mutex_active,
    refused_mutex_probe_failed, noop_up_to_date, success, failure) and their
    exit codes;
  * that a mutex-active or mutex-probe-failure refusal never even OPENS the
    universe/schema-meta ArcticDB libraries (no writes attempted);
  * dry-run never applies or stamps anything.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "run_arctic_migrations.py"


@pytest.fixture()
def mod(monkeypatch):
    # Fresh module object per test (not module-scoped) so per-test monkeypatch
    # of module globals (notify/write_completion_marker/file_failure_issue)
    # never leaks across tests.
    spec = importlib.util.spec_from_file_location("run_arctic_migrations", SCRIPT)
    m = importlib.util.module_from_spec(spec)
    sys.modules["run_arctic_migrations"] = m
    spec.loader.exec_module(m)
    return m


def _migration(number, name="test_migration", run=None, verify=None):
    calls = []

    def _run(lib, meta_lib):
        calls.append(("run", number))
        if run is not None:
            run(lib, meta_lib)

    def _verify(lib):
        calls.append(("verify", number))
        if verify is not None:
            verify(lib)

    return SimpleNamespace(number=number, name=name, run=_run, verify=_verify), calls


# ── completion_marker_key ────────────────────────────────────────────────


def test_completion_marker_key_format(mod):
    assert mod.completion_marker_key(7) == (
        "overseer/_control/completed/arctic-migration-0007.json"
    )
    assert mod.completion_marker_key(0) == (
        "overseer/_control/completed/arctic-migration-0000.json"
    )


# ── resolve_current_version ──────────────────────────────────────────────


def test_resolve_current_version_none_is_baseline(mod, monkeypatch):
    import store.schema_version as sv

    monkeypatch.setattr(sv, "read_schema_version", lambda meta_lib: None)
    assert mod.resolve_current_version(object()) == sv.BASELINE_SCHEMA_VERSION


def test_resolve_current_version_passthrough(mod, monkeypatch):
    import store.schema_version as sv

    monkeypatch.setattr(sv, "read_schema_version", lambda meta_lib: 3)
    assert mod.resolve_current_version(object()) == 3


# ── mutex probe ───────────────────────────────────────────────────────────


class _FakeSfn:
    def __init__(self, running=None, error_for=None):
        self.running = set(running or ())
        self.error_for = error_for
        self.calls = []

    def list_executions(self, *, stateMachineArn, statusFilter, maxResults):  # noqa: N803
        self.calls.append(stateMachineArn)
        name = stateMachineArn.rsplit(":", 1)[-1]
        if self.error_for and name == self.error_for:
            raise RuntimeError("States API hiccup")
        if name in self.running:
            return {"executions": [{"executionArn": f"arn:...:{name}:run-1"}]}
        return {"executions": []}


def test_running_pipeline_executions_returns_running_names(mod, monkeypatch):
    sfn = _FakeSfn(running=["ne-preopen-trading-pipeline"])
    monkeypatch.setattr(mod, "_stepfunctions_client", lambda region: sfn)
    out = mod.running_pipeline_executions(region="us-east-1")
    assert out == ["ne-preopen-trading-pipeline"]
    assert len(sfn.calls) == 3  # all three guarded pipelines probed


def test_running_pipeline_executions_none_running(mod, monkeypatch):
    sfn = _FakeSfn(running=[])
    monkeypatch.setattr(mod, "_stepfunctions_client", lambda region: sfn)
    assert mod.running_pipeline_executions(region="us-east-1") == []


def test_running_pipeline_executions_raises_mutex_probe_error_on_api_failure(mod, monkeypatch):
    sfn = _FakeSfn(error_for="ne-weekly-freshness-pipeline")
    monkeypatch.setattr(mod, "_stepfunctions_client", lambda region: sfn)
    with pytest.raises(mod.MutexProbeError):
        mod.running_pipeline_executions(region="us-east-1")


# ── apply_pending ─────────────────────────────────────────────────────────


def test_apply_pending_applies_in_order_and_returns_numbers(mod):
    m1, calls1 = _migration(1)
    m2, calls2 = _migration(2)
    applied = mod.apply_pending([m1, m2], universe_lib=object(), meta_lib=object())
    assert applied == [1, 2]
    assert calls1 == [("run", 1), ("verify", 1)]
    assert calls2 == [("run", 2), ("verify", 2)]


def test_apply_pending_aborts_on_first_failure_wraps_migration_run_error(mod):
    def _boom_run(lib, meta_lib):
        raise RuntimeError("rewrite failed")

    m1, calls1 = _migration(1, run=_boom_run)
    m2, calls2 = _migration(2)
    with pytest.raises(mod.MigrationRunError) as excinfo:
        mod.apply_pending([m1, m2], universe_lib=object(), meta_lib=object())
    assert excinfo.value.migration_number == 1
    assert "rewrite failed" in str(excinfo.value.cause)
    # migration 2 was NEVER touched — the strict per-migration ordering.
    assert calls2 == []


def test_apply_pending_aborts_on_verify_failure_not_just_run(mod):
    def _boom_verify(lib):
        raise RuntimeError("verify failed")

    m1, calls1 = _migration(1, verify=_boom_verify)
    m2, calls2 = _migration(2)
    with pytest.raises(mod.MigrationRunError) as excinfo:
        mod.apply_pending([m1, m2], universe_lib=object(), meta_lib=object())
    assert excinfo.value.migration_number == 1
    assert calls1 == [("run", 1), ("verify", 1)]
    assert calls2 == []


# ── run() orchestration ───────────────────────────────────────────────────


def _args(mod, **overrides):
    base = dict(
        merged_sha="a" * 40, head_migration_number=1,
        bucket="alpha-engine-research", region="us-east-1", dry_run=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _stub_side_effects(mod, monkeypatch):
    """Silence completion-marker/notify/issue-filing side effects for tests
    that only care about the decision logic, and record calls."""
    recorded = {"markers": [], "notifies": [], "issues": []}
    monkeypatch.setattr(
        mod, "write_completion_marker",
        lambda **kw: recorded["markers"].append(kw),
    )
    monkeypatch.setattr(
        mod, "notify", lambda **kw: recorded["notifies"].append(kw),
    )
    monkeypatch.setattr(
        mod, "file_failure_issue", lambda **kw: recorded["issues"].append(kw),
    )
    return recorded


def _forbid_arctic_open(mod, monkeypatch):
    """Any attempt to open the universe/schema-meta libraries during a mutex
    refusal is a bug — patch both to explode if called."""
    import store.arctic_store as arctic_store

    def _boom(*a, **kw):
        raise AssertionError("must not open ArcticDB libraries on a mutex refusal")

    monkeypatch.setattr(arctic_store, "get_universe_lib", _boom)
    monkeypatch.setattr(arctic_store, "get_schema_meta_lib", _boom)


def test_run_refuses_cleanly_when_mutex_active_no_writes_attempted(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    _forbid_arctic_open(mod, monkeypatch)
    monkeypatch.setattr(mod, "running_pipeline_executions", lambda region: ["ne-preopen-trading-pipeline"])

    rc = mod.run(_args(mod))
    assert rc == 0
    (marker,) = recorded["markers"]
    assert marker["payload"]["state"] == "refused_mutex_active"
    assert marker["payload"]["rc"] == 0
    (note,) = recorded["notifies"]
    assert note["severity"] == "warning"
    assert recorded["issues"] == []


def test_run_refuses_loud_when_mutex_probe_fails(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    _forbid_arctic_open(mod, monkeypatch)

    def _boom(region):
        raise mod.MutexProbeError("states API down")

    monkeypatch.setattr(mod, "running_pipeline_executions", _boom)

    rc = mod.run(_args(mod))
    assert rc == 1
    (marker,) = recorded["markers"]
    assert marker["payload"]["state"] == "refused_mutex_probe_failed"
    assert marker["payload"]["rc"] == 1
    (note,) = recorded["notifies"]
    assert note["severity"] == "critical"
    (issue,) = recorded["issues"]
    assert "mutex probe failed" in issue["error"]


def test_run_noop_when_nothing_pending(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    monkeypatch.setattr(mod, "running_pipeline_executions", lambda region: [])
    monkeypatch.setattr(mod, "resolve_current_version", lambda meta_lib: 3)

    import migrations as migrations_mod
    import store.arctic_store as arctic_store

    monkeypatch.setattr(arctic_store, "get_schema_meta_lib", lambda bucket: object())
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket: object())
    monkeypatch.setattr(migrations_mod, "pending_migrations", lambda current: [])

    rc = mod.run(_args(mod))
    assert rc == 0
    (marker,) = recorded["markers"]
    assert marker["payload"]["state"] == "noop_up_to_date"
    assert marker["payload"]["current_version"] == 3


def test_run_success_applies_all_pending(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    monkeypatch.setattr(mod, "running_pipeline_executions", lambda region: [])
    monkeypatch.setattr(mod, "resolve_current_version", lambda meta_lib: 0)

    import migrations as migrations_mod
    import store.arctic_store as arctic_store

    monkeypatch.setattr(arctic_store, "get_schema_meta_lib", lambda bucket: object())
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket: object())
    m1, _ = _migration(1)
    monkeypatch.setattr(migrations_mod, "pending_migrations", lambda current: [m1])

    rc = mod.run(_args(mod, head_migration_number=1))
    assert rc == 0
    (marker,) = recorded["markers"]
    assert marker["payload"]["state"] == "success"
    assert marker["payload"]["applied_migrations"] == [1]
    (note,) = recorded["notifies"]
    assert note["severity"] == "info"
    assert recorded["issues"] == []


def test_run_failure_when_a_migration_raises(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    monkeypatch.setattr(mod, "running_pipeline_executions", lambda region: [])
    monkeypatch.setattr(mod, "resolve_current_version", lambda meta_lib: 0)

    import migrations as migrations_mod
    import store.arctic_store as arctic_store

    monkeypatch.setattr(arctic_store, "get_schema_meta_lib", lambda bucket: object())
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket: object())

    def _boom_run(lib, meta_lib):
        raise RuntimeError("write_batch exploded")

    m1, _ = _migration(1, run=_boom_run)
    monkeypatch.setattr(migrations_mod, "pending_migrations", lambda current: [m1])

    rc = mod.run(_args(mod, head_migration_number=1))
    assert rc == 1
    (marker,) = recorded["markers"]
    assert marker["payload"]["state"] == "failure"
    assert marker["payload"]["failed_migration_number"] == 1
    assert marker["payload"]["rc"] == 1
    (note,) = recorded["notifies"]
    assert note["severity"] == "critical"
    (issue,) = recorded["issues"]
    assert issue["head_migration_number"] == 1


def test_dry_run_never_applies_or_stamps(mod, monkeypatch):
    recorded = _stub_side_effects(mod, monkeypatch)
    monkeypatch.setattr(mod, "running_pipeline_executions", lambda region: [])
    monkeypatch.setattr(mod, "resolve_current_version", lambda meta_lib: 0)

    import migrations as migrations_mod
    import store.arctic_store as arctic_store

    monkeypatch.setattr(arctic_store, "get_schema_meta_lib", lambda bucket: object())
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda bucket: object())
    m1, calls1 = _migration(1)
    monkeypatch.setattr(migrations_mod, "pending_migrations", lambda current: [m1])

    rc = mod.run(_args(mod, dry_run=True))
    assert rc == 0
    assert calls1 == []  # run()/verify() never called
    assert recorded["markers"] == []  # no completion marker for a dry-run
    assert recorded["notifies"] == []


# ── notify() defensiveness ────────────────────────────────────────────────


def test_notify_never_raises_even_when_the_sink_import_is_broken(mod, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _boom_import(name, *a, **kw):
        if name == "flow_doctor_telegram":
            raise ImportError("simulated: flow_doctor_telegram unavailable")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", _boom_import)
    # Must not raise.
    mod.notify(
        outcome="success", severity="info", text="hi", dedup_key="k",
        context={"merged_sha": "a" * 40},
    )


# ── write_completion_marker() best-effort swallow ────────────────────────


def test_write_completion_marker_swallows_s3_failure(mod, monkeypatch):
    class _BoomS3:
        def put_object(self, **kw):
            raise RuntimeError("S3 down")

    import boto3

    monkeypatch.setattr(boto3, "client", lambda name, **kw: _BoomS3())
    # Must not raise — documented non-fatal swallow.
    mod.write_completion_marker(
        bucket="alpha-engine-research", region="us-east-1",
        head_migration_number=1, payload={"state": "success"},
    )
