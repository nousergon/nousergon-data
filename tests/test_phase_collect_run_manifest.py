"""`_phase_collect` writes one run manifest per phase, on BOTH paths.

`alpha-engine-config-I10773` (P-06). The closes-when asks for a manifest on a
normal run AND on an induced failure, for a unit on each machine. These tests
induce both against the real `_phase_collect`, with a fake registry and a fake
S3, so the assertion is about the wiring rather than about the wrapper (which
`nousergon-lib/tests/test_run_manifest.py` covers).
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import weekly_collector


class _PhaseCtx:
    def __init__(self, skipped: bool = False):
        self.skipped = skipped
        self.skip_reason = "prior ok marker" if skipped else None
        self.artifacts: list[str] = []

    def record_artifact(self, key: str) -> None:
        self.artifacts.append(key)


class FakeS3:
    """Records PUTs; HEADs resolve against a declared object table."""

    def __init__(self, objects: dict[str, int] | None = None):
        self.objects = objects or {}
        self.puts: list[tuple[str, dict]] = []

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        self.puts.append((Key, json.loads(Body.decode("utf-8"))))
        return {"ETag": '"abc"'}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key]}


class FakeRegistry:
    def __init__(self, s3: FakeS3, skipped: bool = False):
        self.date = "2026-09-14"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = "daily"
        self._skipped = skipped

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx(skipped=self._skipped)


KEY = "staging/daily_closes/2026-09-14.parquet"


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    """A real sha and a declared log location, so the wrapper's pre-body
    refusals are not what these tests are measuring."""
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifests(s3: FakeS3) -> list[dict]:
    return [body for key, body in s3.puts if key.startswith("data_collection/runs/")]


def test_a_normal_phase_writes_one_ok_manifest_with_its_output():
    s3 = FakeS3({KEY: 4096})
    reg = FakeRegistry(s3)

    result = weekly_collector._phase_collect(
        reg,
        "daily_closes",
        lambda: {"status": "ok", "tickers_captured": 896},
        artifact_key=KEY,
    )

    assert result["status"] == "ok"
    manifests = _manifests(s3)
    assert len(manifests) == 1
    m = manifests[0]
    assert m["unit_id"] == "D19"
    assert m["trading_day"] == "2026-09-14"
    assert m["status"] == "ok"
    assert m["reason"] == ""
    # `bytes` / `version_id` / `version_capture` arrived with nousergon-lib
    # v0.124.131 (lib PR418, alpha-engine-config-I10892): every output record
    # now carries the provenance a later reader needs to fetch THAT object
    # rather than whatever is at the key now. `head_object` is the capture
    # mode when the sink measured it itself, which is this case — FakeS3
    # reports 4096 bytes for KEY. Asserting the WHOLE record on purpose: a
    # subset assertion would not have caught the field set changing under us,
    # and this mismatch is what the pin bump to v0.124.133 surfaced.
    assert m["outputs"] == [
        {
            "key": KEY,
            "etag": None,
            "schema_version": None,
            "rows_out": 896,
            "bytes": 4096,
            "version_id": None,
            "version_capture": "head_object",
        }
    ]
    assert m["rows_out"] == 896
    assert [g["verdict"] for g in m["guards"]] == ["ok"]


def test_the_manifest_key_is_addressed_by_unit_and_trading_day():
    s3 = FakeS3({KEY: 10})
    weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", lambda: {"status": "ok", "tickers_captured": 1},
        artifact_key=KEY,
    )
    key = [k for k, _ in s3.puts][0]
    assert key.startswith("data_collection/runs/D19/2026-09-14/")
    assert key.endswith(".json")


def test_a_failing_collector_writes_a_failed_manifest_and_still_returns_its_error_dict():
    """The best-effort-continue posture is unchanged; the RECORD is not."""
    s3 = FakeS3()
    result = weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes",
        lambda: {"status": "error", "error": "polygon returned 503"},
        artifact_key=KEY,
    )

    assert result == {"status": "error", "error": "polygon returned 503"}
    m = _manifests(s3)[0]
    assert m["status"] == "failed"
    assert "polygon returned 503" in m["reason"]
    # No completion claim: a dying run never advances the artifact a detector
    # reads as proof it finished (`observability-policy` §3.1).
    assert m["outputs"] == []
    assert m["rows_out"] == 0


def test_a_raising_collector_writes_a_failed_manifest():
    s3 = FakeS3()

    def boom():
        raise RuntimeError("vendor timeout")

    result = weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", boom, artifact_key=KEY
    )
    assert result["status"] == "error"
    m = _manifests(s3)[0]
    assert m["status"] == "failed"
    assert "vendor timeout" in m["reason"]


def test_an_empty_publish_is_caught_but_does_not_change_the_exit_code():
    """OBSERVE mode: the verdict is real, the consequence is not (§7a)."""
    s3 = FakeS3({KEY: 0})
    result = weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", lambda: {"status": "ok", "tickers_captured": 5},
        artifact_key=KEY,
    )
    assert result["status"] == "ok", "observe mode must not move the exit code"
    guard = _manifests(s3)[0]["guards"][0]
    assert guard["verdict"] == "empty_fresh"
    assert guard["mode"] == "observe"


def test_an_absent_artifact_under_an_ok_status_is_caught_by_the_guard():
    s3 = FakeS3({})  # nothing published
    weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", lambda: {"status": "ok", "tickers_captured": 5},
        artifact_key=KEY,
    )
    assert _manifests(s3)[0]["guards"][0]["verdict"] == "empty_fresh"


def test_a_unit_reporting_no_row_count_reads_unmeasurable_never_green():
    s3 = FakeS3({"features/x.parquet": 100})
    weekly_collector._phase_collect(
        FakeRegistry(s3), "features", lambda: {"status": "ok"},
        artifact_key="features/x.parquet",
    )
    m = _manifests(s3)[0]
    assert m["unit_id"] == "D31"
    assert m["guards"][0]["verdict"] == "unmeasurable"


def test_an_auto_skip_publishes_nothing_and_says_so_rather_than_reading_as_empty():
    """`alpha-engine-config-I11011`: `not_applicable`, never `ok`.

    A same-date auto-skip published nothing — `rows_out: 0`, `outputs: []` —
    and used to record `status: ok`, which every downstream surface counted as
    a run that produced its deliverable. It is the lib's own closed-list
    `no_new_data_declared` ("a target date already published"), and it is
    COUNTED: a unit answering not-applicable every cycle is a unit that has
    stopped working, and the board can see that only because the non-run left a
    record saying what it was.

    The collector's own result dict is unchanged, so no exit code moves.
    """
    s3 = FakeS3({KEY: 4096})
    reg = FakeRegistry(s3, skipped=True)
    result = weekly_collector._phase_collect(
        reg, "daily_closes", lambda: pytest.fail("must not recompute"), artifact_key=KEY
    )
    assert result["auto_skipped"] is True
    assert result["status"] == "ok"
    m = _manifests(s3)[0]
    assert m["status"] == "not_applicable"
    assert m["reason"].startswith("no_new_data_declared")
    assert m["outputs"] == []
    assert m["rows_out"] == 0
    assert m["guards"][0]["verdict"] == "not_applicable"


def test_a_collector_self_reported_auto_skip_routes_the_same_as_a_registry_one():
    """`alpha-engine-config-I11231`: the immutable-date no-op in
    `technical_rating_ledger.collect_rating_ledger` discovers its own auto-skip
    AT EXECUTION TIME (a live read against S3), not from the phase registry's
    prior-marker cache-hit check — so it signals it by setting `auto_skipped`/
    `skip_reason` on its OWN returned dict, with `supports_auto_skip=False`
    (no framework-level cache-hit check at all, matching the real D26 call
    site). `_record_phase_lineage` must treat this identically to the
    registry-driven auto-skip above: `not_applicable` /
    `no_new_data_declared`, never `EmptyProduction`.
    """
    s3 = FakeS3()
    reg = FakeRegistry(s3)  # skipped=False: the registry itself never skips this phase
    result = weekly_collector._phase_collect(
        reg, "metron_rating_ledger",
        lambda: {
            "status": "ok",
            "auto_skipped": True,
            "skip_reason": "target date already live-published, immutable",
            "backfill_written": 0,
            "live_written": False,
        },
        supports_auto_skip=False,
    )
    assert result["status"] == "ok"
    assert result["auto_skipped"] is True
    m = _manifests(s3)[0]
    assert m["status"] == "not_applicable"
    # The manifest's `reason` is the lib's closed-list member itself; the
    # human-readable detail (the collector's own `skip_reason`) rides on the
    # guard/log text instead, verified via `caplog`.
    assert m["reason"] == "no_new_data_declared"
    assert m["outputs"] == []
    assert m["rows_out"] == 0


def test_a_self_reported_auto_skip_still_records_what_it_read():
    """`alpha-engine-config-I11231`. A no-op read something -- D26's no-op is the
    ledger manifest at a version that already carried the day -- and on a shadow
    run that version is the evidence of whether it built on v1's base or on v1's
    own write. An auto-skip publishes nothing, so it records no output; it still
    records its inputs."""
    s3 = FakeS3()
    reg = FakeRegistry(s3)
    ref = {
        "key": "s3://alpha-engine-research/market_data/technicals/rating_history/_manifest.json",
        "etag": None, "version": "9Mf1cVm0zUZ4mmkQU_y91k0gTk2dm65L", "schema_version": None,
    }
    weekly_collector._phase_collect(
        reg, "metron_rating_ledger",
        lambda: {
            "status": "ok",
            "auto_skipped": True,
            "skip_reason": "target date already live-published, immutable",
            "backfill_written": 0,
            "live_written": False,
            "input_refs": [ref],
        },
        supports_auto_skip=False,
    )
    m = _manifests(s3)[0]
    assert m["status"] == "not_applicable"
    assert m["outputs"] == []
    assert m["inputs"] == [ref]


def test_the_collector_self_reported_skip_reason_reaches_the_log(caplog):
    """The detail behind `no_new_data_declared` — WHY this cycle had nothing new
    — must still be discoverable, even though it does not live in `reason`."""
    s3 = FakeS3()
    reg = FakeRegistry(s3)
    with caplog.at_level("INFO"):
        weekly_collector._phase_collect(
            reg, "metron_rating_ledger",
            lambda: {
                "status": "ok",
                "auto_skipped": True,
                "skip_reason": "target date already live-published, immutable",
                "backfill_written": 0,
                "live_written": False,
            },
            supports_auto_skip=False,
        )
    assert any(
        "target date already live-published, immutable" in r.message
        for r in caplog.records
    )


def test_dry_run_writes_no_manifest():
    """`reg is None` is the dry-run path and its posture is unchanged."""
    result = weekly_collector._phase_collect(None, "daily_closes", lambda: {"status": "ok"})
    assert result == {"status": "ok"}


def test_a_metric_record_rides_on_every_manifest():
    s3 = FakeS3({KEY: 4096})
    weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", lambda: {"status": "ok", "tickers_captured": 896},
        artifact_key=KEY,
    )
    metrics = _manifests(s3)[0]["metrics"]
    assert [m["name"] for m in metrics] == ["data.D19.guard.empty_fresh"]
    assert metrics[0]["status"] == "GREEN"


def test_every_manifest_conforms_to_the_contract():
    from nousergon_lib import contracts

    s3 = FakeS3({KEY: 4096})
    weekly_collector._phase_collect(
        FakeRegistry(s3), "daily_closes", lambda: {"status": "ok", "tickers_captured": 896},
        artifact_key=KEY,
    )
    assert contracts.conformance_errors("data_run_manifest", _manifests(s3)[0]) == []


def test_an_extra_output_is_graded_by_the_empty_fresh_guard_too():
    """`alpha-engine-config-I10785`: every published key, not just `artifact_key`.

    D08 (`universe_returns`) writes `research.db` and a dated backup through
    `extra_outputs` — before this, neither key was ever HEAD-checked by this
    guard, only recorded as an output.
    """
    s3 = FakeS3({"research.db": 2048, "backups/research_2026-09-14.db": 2048})
    reg = FakeRegistry(s3)
    reg.data_mode = "phase1"
    result = weekly_collector._phase_collect(
        reg,
        "universe_returns",
        lambda: {"status": "ok", "rows_inserted": 40, "db_upload": {"pointer_key": "research.db", "backup_key": "backups/research_2026-09-14.db"}},
        supports_auto_skip=False,
        extra_outputs=(
            ("research.db", lambda r: bool((r.get("db_upload") or {}).get("pointer_key")), lambda r: r.get("rows_inserted") or 0),
            (f"backups/research_2026-09-14.db", lambda r: bool((r.get("db_upload") or {}).get("backup_key")), lambda r: r.get("rows_inserted") or 0),
        ),
    )
    assert result["status"] == "ok"
    m = _manifests(s3)[0]
    assert m["unit_id"] == "D08"
    # Both extra keys, plus the (absent) primary — three guard readings, one
    # each, all clean since both extra keys published real bytes.
    guarded_keys = {g["key"] for g in m["guards"] if g["key"]}
    assert guarded_keys == {"research.db", "backups/research_2026-09-14.db"}
    assert all(g["verdict"] == "ok" for g in m["guards"] if g["key"] in guarded_keys)
    # Still exactly ONE board metric per run — the worst of the graded keys.
    assert len(m["metrics"]) == 1
    assert m["metrics"][0]["name"] == "data.D08.guard.empty_fresh"
    assert m["metrics"][0]["status"] == "GREEN"


def test_a_broken_extra_output_is_the_one_the_board_metric_reports():
    """The worst key wins the single per-run board row, not the first one."""
    s3 = FakeS3({"research.db": 2048})  # backup key never landed on S3
    reg = FakeRegistry(s3)
    reg.data_mode = "phase1"
    result = weekly_collector._phase_collect(
        reg,
        "universe_returns",
        lambda: {"status": "ok", "rows_inserted": 40, "db_upload": {"pointer_key": "research.db", "backup_key": "backups/research_2026-09-14.db"}},
        supports_auto_skip=False,
        extra_outputs=(
            ("research.db", lambda r: bool((r.get("db_upload") or {}).get("pointer_key")), lambda r: r.get("rows_inserted") or 0),
            (f"backups/research_2026-09-14.db", lambda r: bool((r.get("db_upload") or {}).get("backup_key")), lambda r: r.get("rows_inserted") or 0),
        ),
    )
    assert result["status"] == "ok", "observe mode must not move the exit code"
    m = _manifests(s3)[0]
    by_key = {g["key"]: g["verdict"] for g in m["guards"] if g["key"]}
    assert by_key["research.db"] == "ok"
    assert by_key["backups/research_2026-09-14.db"] == "empty_fresh"
    assert m["metrics"][0]["status"] == "RED", "the broken key must win the single board row"


def test_the_run_mode_is_resolved_from_the_same_args_run_weekly_dispatches_on():
    import argparse

    def ns(**kw):
        base = dict(
            morning_enrich=False, morning_arctic_append=False, daily_arctic_append=False,
            chronic_gap_heal=False, daily_heal=False, daily=False, phase=None,
        )
        base.update(kw)
        return argparse.Namespace(**base)

    assert weekly_collector._resolve_run_mode(ns()) == "phase1"
    assert weekly_collector._resolve_run_mode(ns(phase=2)) == "phase2"
    assert weekly_collector._resolve_run_mode(ns(daily=True)) == "daily"
    assert weekly_collector._resolve_run_mode(ns(morning_enrich=True)) == "morning_enrich"
    assert weekly_collector._resolve_run_mode(ns(daily=True, daily_heal=True)) == "daily_heal"
