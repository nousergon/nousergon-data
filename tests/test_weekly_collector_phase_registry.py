"""Tests for the nousergon_lib.phase_registry adoption in weekly_collector
(L4528 — data is the 2nd consumer of the lib phase framework, after the
backtester).

Covers the `_phase_collect` helper's contract against a real PhaseRegistry
driven by an in-memory fake S3:
  * happy path records an `ok` marker + the declared artifact key;
  * a 2nd run auto-skips (run_fn NOT called) — recorded as `ok`, not `skipped`,
    so the module's non-ok-fails-the-run aggregator treats a resume as success;
  * L4524: an `ok` marker whose artifact has gone missing RE-RUNS the phase;
  * a collector `status=error` writes an `error` marker (→ next run re-runs) and
    is caught so the best-effort loop continues;
  * `--force` / `--force-phases` override auto-skip;
  * dry-run builds NO registry (reg is None) → run_fn runs directly, no markers;
  * `supports_auto_skip=False` never skips even with a valid marker.
"""

from __future__ import annotations

import io
import json
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

import weekly_collector
from nousergon_lib.phase_registry import PhaseRegistry


# ── in-memory fake S3 ────────────────────────────────────────────────────────


class _FakeS3:
    """Minimal S3 stand-in for PhaseRegistry: markers live in `objects`,
    artifact existence in `artifacts`."""

    def __init__(self, artifacts: set[str] | None = None):
        self.objects: dict[str, bytes] = {}        # key -> body (markers)
        self.artifacts: set[str] = set(artifacts or [])
        self.put_calls: list[str] = []

    @staticmethod
    def _missing(key: str):
        return ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self._missing(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.put_calls.append(Key)
        return {}

    def head_object(self, Bucket, Key):
        if Key in self.artifacts:
            return {"ContentLength": 1}
        raise ClientError({"Error": {"Code": "404", "Message": "no"}}, "HeadObject")


def _reg(fake: _FakeS3, *, force=False, force_phases=None, skip_phases=None) -> PhaseRegistry:
    return PhaseRegistry(
        date="2026-06-13",
        bucket="alpha-engine-research",
        marker_prefix="data",
        s3_client=fake,
        force=force,
        force_phases=force_phases or [],
        skip_phases=skip_phases or [],
    )


# ── _phase_collect contract ──────────────────────────────────────────────────


def test_happy_path_records_marker_and_artifact():
    fake = _FakeS3()
    reg = _reg(fake)
    calls = []

    def run_fn():
        calls.append(1)
        return {"status": "ok", "n": 42}

    out = weekly_collector._phase_collect(
        reg, "macro", run_fn, artifact_key="market_data/weekly/2026-06-13/macro.json",
    )
    assert out["status"] == "ok" and out["n"] == 42
    assert calls == [1]
    marker = json.loads(fake.objects["data/2026-06-13/.phases/macro.json"])
    assert marker["status"] == "ok"
    assert marker["artifact_keys"] == ["market_data/weekly/2026-06-13/macro.json"]


def test_second_run_auto_skips_when_artifact_present():
    # First run writes the marker; mark the artifact present, then a fresh
    # registry (same date) must auto-skip without calling run_fn.
    art = "market_data/weekly/2026-06-13/macro.json"
    fake = _FakeS3()
    weekly_collector._phase_collect(_reg(fake), "macro", lambda: {"status": "ok"}, artifact_key=art)
    fake.artifacts.add(art)

    calls = []
    out = weekly_collector._phase_collect(
        _reg(fake), "macro", lambda: calls.append(1) or {"status": "ok"}, artifact_key=art,
    )
    assert calls == [], "run_fn must NOT run on an auto-skip"
    assert out["status"] == "ok" and out["auto_skipped"] is True


def test_marker_ok_but_artifact_missing_reruns():
    # L4524: a status=ok marker whose declared artifact is absent on S3 is a lie
    # → the phase re-runs rather than trusting the marker.
    art = "market_data/weekly/2026-06-13/macro.json"
    fake = _FakeS3()
    weekly_collector._phase_collect(_reg(fake), "macro", lambda: {"status": "ok"}, artifact_key=art)
    # NOTE: do NOT add `art` to fake.artifacts → head_object 404s.
    calls = []
    weekly_collector._phase_collect(
        _reg(fake), "macro", lambda: calls.append(1) or {"status": "ok"}, artifact_key=art,
    )
    assert calls == [1], "missing artifact must invalidate the marker and re-run"


def test_collector_error_writes_error_marker_and_is_caught():
    fake = _FakeS3()
    out = weekly_collector._phase_collect(
        _reg(fake), "prices", lambda: {"status": "error", "error": "boom"},
        supports_auto_skip=False,
    )
    assert out["status"] == "error"
    marker = json.loads(fake.objects["data/2026-06-13/.phases/prices.json"])
    assert marker["status"] == "error"


def test_error_marker_does_not_auto_skip_next_run():
    art = "archive/fundamentals/2026-06-13.json"
    fake = _FakeS3()
    weekly_collector._phase_collect(
        _reg(fake), "fundamentals", lambda: {"status": "error", "error": "x"}, artifact_key=art,
    )
    calls = []
    weekly_collector._phase_collect(
        _reg(fake), "fundamentals", lambda: calls.append(1) or {"status": "ok"}, artifact_key=art,
    )
    assert calls == [1], "an error marker must re-run on the next attempt"


def test_raising_collector_is_caught_best_effort():
    fake = _FakeS3()

    def boom():
        raise RuntimeError("kaboom")

    out = weekly_collector._phase_collect(_reg(fake), "macro", boom, supports_auto_skip=False)
    assert out["status"] == "error" and "kaboom" in out["error"]


def test_force_phases_overrides_auto_skip():
    art = "market_data/weekly/2026-06-13/macro.json"
    fake = _FakeS3(artifacts={art})
    weekly_collector._phase_collect(_reg(fake), "macro", lambda: {"status": "ok"}, artifact_key=art)
    calls = []
    weekly_collector._phase_collect(
        _reg(fake, force_phases=["macro"]), "macro",
        lambda: calls.append(1) or {"status": "ok"}, artifact_key=art,
    )
    assert calls == [1], "--force-phases must re-run despite a valid marker"


def test_supports_auto_skip_false_never_skips():
    art = "x/y.parquet"
    fake = _FakeS3(artifacts={art})
    # Pre-seed an ok marker WITH the artifact recorded.
    weekly_collector._phase_collect(
        _reg(fake), "prices", lambda: {"status": "ok"}, artifact_key=art, supports_auto_skip=False,
    )
    calls = []
    weekly_collector._phase_collect(
        _reg(fake), "prices", lambda: calls.append(1) or {"status": "ok"},
        artifact_key=art, supports_auto_skip=False,
    )
    assert calls == [1], "supports_auto_skip=False phases always run"


# ── _build_registry / dry-run ────────────────────────────────────────────────


def _args(**kw):
    base = dict(dry_run=False, only=None, skip_phases="", force_phases="", force=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_dry_run_builds_no_registry():
    reg = weekly_collector._build_registry({"bucket": "b"}, _args(dry_run=True), "2026-06-13")
    assert reg is None


def test_dry_run_phase_collect_runs_directly_without_markers():
    calls = []
    out = weekly_collector._phase_collect(
        None, "macro", lambda: calls.append(1) or {"status": "ok"},
        artifact_key="market_data/weekly/2026-06-13/macro.json",
    )
    assert calls == [1] and out["status"] == "ok"


def test_only_forces_all_phases():
    # --only <collector> means the operator explicitly wants that work → force=True.
    reg = weekly_collector._build_registry({"bucket": "b"}, _args(only="prices"), "2026-06-13")
    assert reg._force_all is True


def test_build_registry_reads_hard_caps_from_config():
    cfg = {"bucket": "b", "full_run_hard_caps_seconds": {"prices": 1200.0}}
    reg = weekly_collector._build_registry(cfg, _args(), "2026-06-13")
    assert reg._hard_caps == {"prices": 1200.0}


# ── verify_artifact_exists (config-I2702 deliverable #2) ─────────────────────
#
# rc=0 / status="ok" must mean the contracted artifact actually landed on S3,
# never just "the collector call didn't raise". These tests drive
# _phase_collect's NEW verify_artifact_exists gate directly against a
# _FakeS3 substituted for boto3.client("s3") inside _s3_object_exists —
# independent of the PhaseRegistry's OWN marker-vs-artifact auto-skip check
# (test_marker_ok_but_artifact_missing_reruns, above), which is a different
# mechanism (a marker recorded on a PRIOR run) than this same-run gate.


def test_verify_artifact_exists_passes_when_artifact_present(monkeypatch):
    art = "staging/daily_closes/2026-06-13.parquet"
    fake = _FakeS3(artifacts={art})
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)

    out = weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: {"status": "ok"},
        artifact_key=art, verify_artifact_exists=True, bucket="alpha-engine-research",
    )
    assert out["status"] == "ok"


def test_verify_artifact_exists_downgrades_ok_to_error_when_artifact_absent(monkeypatch):
    # The exact 2026-07-15-class bug: the collector's own internal logic
    # reports status="ok" but the parquet never actually landed on S3.
    art = "staging/daily_closes/2026-06-13.parquet"
    fake = _FakeS3()  # artifacts deliberately empty
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)

    out = weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: {"status": "ok"},
        artifact_key=art, verify_artifact_exists=True, bucket="alpha-engine-research",
    )
    assert out["status"] == "error"
    assert "does not exist" in out["error"]
    assert "config-I2702" in out["error"]


def test_verify_artifact_exists_error_marker_reruns_next_attempt(monkeypatch):
    art = "staging/daily_closes/2026-06-13.parquet"
    fake = _FakeS3()
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)

    weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: {"status": "ok"},
        artifact_key=art, verify_artifact_exists=True, bucket="alpha-engine-research",
    )
    calls = []
    weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: calls.append(1) or {"status": "ok"},
        artifact_key=art, verify_artifact_exists=True, bucket="alpha-engine-research",
    )
    assert calls == [1], "a verify-by-artifact failure must not auto-skip the retry"


def test_verify_artifact_exists_skips_check_on_dry_run_status(monkeypatch):
    # ok_dry_run collectors write nothing real to S3 — the verify gate must
    # not spuriously fail a dry-run.
    art = "staging/daily_closes/2026-06-13.parquet"
    fake = _FakeS3()  # no artifacts — a real check here would fail
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)

    out = weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: {"status": "ok_dry_run"},
        artifact_key=art, verify_artifact_exists=True, bucket="alpha-engine-research",
    )
    assert out["status"] == "ok_dry_run"


def test_verify_artifact_exists_false_leaves_existing_behavior_unchanged(monkeypatch):
    # Default (verify_artifact_exists=False, the ~19 untouched call sites)
    # must never call _s3_object_exists at all.
    art = "staging/daily_closes/2026-06-13.parquet"
    fake = _FakeS3()  # no artifacts — would fail the check if it ran

    def _boom_client(*a, **k):
        raise AssertionError("boto3.client must not be called when verify_artifact_exists=False")

    monkeypatch.setattr(weekly_collector.boto3, "client", _boom_client)

    out = weekly_collector._phase_collect(
        _reg(fake), "daily_closes", lambda: {"status": "ok"}, artifact_key=art,
    )
    assert out["status"] == "ok"


def test_s3_object_exists_head_true(monkeypatch):
    fake = _FakeS3(artifacts={"a/b.parquet"})
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)
    assert weekly_collector._s3_object_exists("bucket", "a/b.parquet") is True


def test_s3_object_exists_head_false_on_404(monkeypatch):
    fake = _FakeS3()
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)
    assert weekly_collector._s3_object_exists("bucket", "a/b.parquet") is False


def test_s3_object_exists_raises_on_other_client_error(monkeypatch):
    class _Boom(_FakeS3):
        def head_object(self, Bucket, Key):
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "HeadObject")

    fake = _Boom()
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: fake)
    with pytest.raises(ClientError):
        weekly_collector._s3_object_exists("bucket", "a/b.parquet")
