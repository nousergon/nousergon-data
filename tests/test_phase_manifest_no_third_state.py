"""No exit-0 `skipped`/`degraded` outcome leaves the run unrecorded.

`alpha-engine-config-I10784` (P-17). The plan's §1 layer verdict names two
specific defects as the reason the vendor-ingest layer is kept rather than
rewritten: `skipped`/`degraded` exiting 0 (I7572), and D15's swallowed
scope-baseline write. These tests are the closes-when for both.

Three properties are asserted, and the third is the one that makes the first two
safe to ship:

1. A phase that did not run leaves a manifest — `failed` when the unit SHOULD
   have run and an upstream input was missing, `not_applicable` with a
   closed-list reason when the declaration said there was nothing to do.
2. A `degraded` phase's manifest says `failed`. `data_run_manifest.v1` has no
   third ok-but-degraded state; a run that produced a defective artifact is
   `failed`.
3. **The process posture does not move.** The EOD Step Function reads this
   process's exit code to decide whether to run the ArcticDB append, and a
   features-only column defect must not withhold the day's SPY close
   (2026-08-17). So the manifest's honesty is asserted alongside the collector
   result the caller still receives.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import run_units
import weekly_collector


class _PhaseCtx:
    def __init__(self) -> None:
        self.skipped = False
        self.skip_reason = None
        self.artifacts: list[str] = []

    def record_artifact(self, key: str) -> None:
        self.artifacts.append(key)


class FakeS3:
    def __init__(self, objects: dict[str, int] | None = None, fail_puts_matching: str | None = None):
        self.objects = objects or {}
        self.puts: list[tuple[str, dict]] = []
        self.fail_puts_matching = fail_puts_matching

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        if self.fail_puts_matching and self.fail_puts_matching in Key:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
        raw = Body.decode("utf-8") if isinstance(Body, (bytes, bytearray)) else Body
        self.puts.append((Key, json.loads(raw)))
        return {"ETag": '"abc"'}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key]}


class FakeRegistry:
    def __init__(self, s3: FakeS3, mode: str = "daily"):
        self.date = "2026-09-14"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = mode

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


FEATURES_KEY = "features/2026-09-14/schema_version.json"


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "b" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifests(s3: FakeS3) -> list[dict]:
    return [body for key, body in s3.puts if key.startswith("data_collection/runs/")]


# ── 1. A phase that did not run leaves a record ──────────────────────────────


def test_a_missing_upstream_input_writes_a_failed_manifest_and_keeps_the_nonzero_exit():
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="phase1")

    payload = weekly_collector._phase_not_run(
        reg, "fundamentals", reason="no tickers", applicable=True
    )

    # The caller's dict is unchanged: `skipped` is what the aggregator already
    # routes to results["status"] = "failed" and main() to SystemExit(1). This
    # is a new RECORD of an existing non-zero exit, not a new exit path.
    assert payload == {"status": "skipped", "reason": "no tickers"}
    manifests = _manifests(s3)
    assert len(manifests) == 1
    assert manifests[0]["unit_id"] == "D10"
    assert manifests[0]["status"] == "failed"
    assert "no tickers" in manifests[0]["reason"]
    assert manifests[0]["outputs"] == []


def test_a_collector_disabled_by_declaration_writes_not_applicable_with_a_closed_list_reason():
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="phase1")

    payload = weekly_collector._phase_not_run(
        reg, "short_interest", reason="disabled_in_config", applicable=False
    )

    assert payload == {"status": "ok", "skipped": "disabled_in_config"}
    manifests = _manifests(s3)
    assert len(manifests) == 1
    assert manifests[0]["unit_id"] == "D06"
    assert manifests[0]["status"] == "not_applicable"
    # The reason is drawn from `nousergon_lib`'s CLOSED list, never free text —
    # a free-text reason is how a unit quietly stops being graded.
    assert manifests[0]["reason"] == run_units.NOT_RUN_NOT_APPLICABLE
    assert manifests[0]["reason"] in __import__(
        "nousergon_lib.run_manifest", fromlist=["x"]
    ).NOT_APPLICABLE_REASONS


def test_a_not_applicable_run_is_counted_not_silent():
    """`not_applicable` is not a softer `ok`: the record exists so a unit that
    answers it every cycle is visible as a unit that has stopped working."""
    s3 = FakeS3()
    weekly_collector._phase_not_run(
        FakeRegistry(s3, mode="phase1"), "short_interest", reason="disabled_in_config",
        applicable=False,
    )
    key = [k for k, _ in s3.puts][0]
    assert key.startswith("data_collection/runs/D06/2026-09-14/")


def test_a_dry_run_records_nothing():
    """`reg is None` is the dry run. A manifest claiming a unit ran would be the
    one lie this whole record exists to prevent."""
    payload = weekly_collector._phase_not_run(
        None, "fundamentals", reason="no tickers", applicable=True
    )
    assert payload == {"status": "skipped", "reason": "no tickers"}


# ── 2. `degraded` is `failed` on the manifest ────────────────────────────────


def test_a_degraded_phase_writes_a_failed_manifest():
    s3 = FakeS3({FEATURES_KEY: 2048})
    reg = FakeRegistry(s3, mode="daily")

    result = weekly_collector._phase_collect(
        reg,
        "features",
        lambda: {"status": "degraded", "error": "zero-variance columns: rsi_14_raw"},
        artifact_key=FEATURES_KEY,
    )

    # The PROCESS still sees `degraded` — this is what keeps main() at exit 0 so
    # the EOD SF runs the ArcticDB append.
    assert result == {"status": "degraded", "error": "zero-variance columns: rsi_14_raw"}

    manifests = _manifests(s3)
    assert len(manifests) == 1
    assert manifests[0]["status"] == "failed"
    assert "DEGRADED" in manifests[0]["reason"]
    assert "rsi_14_raw" in manifests[0]["reason"]


def test_the_degraded_failure_manifest_still_carries_the_run_telemetry():
    """`observability-policy` §3.1: the failure path writes the same telemetry
    as the success path, except the completion claim. The defect is only
    diagnosable if the output, the guard and the metric survived onto it."""
    s3 = FakeS3({FEATURES_KEY: 2048})
    weekly_collector._phase_collect(
        FakeRegistry(s3, mode="daily"),
        "features",
        lambda: {"status": "degraded", "error": "zero-variance columns"},
        artifact_key=FEATURES_KEY,
    )
    m = _manifests(s3)[0]
    assert [o["key"] for o in m["outputs"]] == [FEATURES_KEY]
    assert m["guards"], "the empty-fresh guard reading must survive onto the failure manifest"
    assert m["metrics"], "the verdict metric must survive onto the failure manifest"


def test_an_ok_phase_is_still_ok():
    """The regression guard on the two tests above: `degraded` moved, `ok` did not."""
    s3 = FakeS3({FEATURES_KEY: 2048})
    result = weekly_collector._phase_collect(
        FakeRegistry(s3, mode="daily"),
        "features",
        lambda: {"status": "ok"},
        artifact_key=FEATURES_KEY,
    )
    assert result == {"status": "ok"}
    assert _manifests(s3)[0]["status"] == "ok"


# ── 3. D15's scope-baseline write no longer swallows ─────────────────────────


def test_the_d15_scope_baseline_write_raises_rather_than_warning(monkeypatch):
    """The swallow named in plan §1 (`weekly_collector.py:838` at the audit's
    baseline) is gone: a failed scope-baseline write is the failure, not a
    warning followed by a collection run against a baseline nobody can advance.
    """
    s3 = FakeS3(fail_puts_matching="alternative/scope.json")
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: s3)

    with pytest.raises(ClientError):
        weekly_collector._write_alternative_scope_baseline(
            "alpha-engine-research",
            "market_data/weekly/2026-09-14/alternative/scope.json",
            "2026-09-14",
            ["AAPL", "MSFT"],
        )


def test_an_induced_d15_scope_baseline_failure_surfaces_as_a_failed_run(monkeypatch):
    """End to end: the raise reaches D15's own run manifest as `status: failed`
    rather than crashing outside every record the unit writes."""
    s3 = FakeS3(fail_puts_matching="alternative/scope.json")
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: s3)
    reg = FakeRegistry(s3, mode="phase2")

    def _body():
        weekly_collector._write_alternative_scope_baseline(
            "alpha-engine-research",
            "market_data/weekly/2026-09-14/alternative/scope.json",
            "2026-09-14",
            ["AAPL"],
        )
        raise AssertionError("collection must not be reached once the baseline write failed")

    result = weekly_collector._phase_collect(
        reg, "alternative", _body,
        artifact_key="market_data/weekly/2026-09-14/alternative/manifest.json",
    )

    assert result["status"] == "error"
    manifests = _manifests(s3)
    assert len(manifests) == 1
    assert manifests[0]["unit_id"] == "D15"
    assert manifests[0]["status"] == "failed"
    assert manifests[0]["outputs"] == []


def test_the_scope_baseline_write_logs_and_returns_on_success(monkeypatch):
    s3 = FakeS3()
    monkeypatch.setattr(weekly_collector.boto3, "client", lambda *a, **k: s3)
    weekly_collector._write_alternative_scope_baseline(
        "alpha-engine-research", "market_data/weekly/2026-09-14/alternative/scope.json",
        "2026-09-14", ["AAPL", "MSFT", "NVDA"],
    )
    key, body = s3.puts[0]
    assert key.endswith("alternative/scope.json")
    assert body["tickers_requested"] == 3
    assert body["resolved_from"] == "constituents.json"
