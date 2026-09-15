"""The EOD cardinality guard is wired at the D20 call site it was built for.

`alpha-engine-config-I10827`, closing the wiring half of
`alpha-engine-config-I10780` (P-13). The guard, its exclusion contracts and the
`data.D20.completeness` clause shipped in `nousergon-data-PR1724`; nothing
called it. A guard nobody calls is indistinguishable from a guard that was never
built, and it is worse than absent: the clause exists, so the board has a row
that can never turn green for a reason nobody can see from the row.

What is asserted here:

1. The verdict reaches BOTH surfaces from one reading — this run's manifest
   (`guards[]`, for diagnosing one execution) and
   `data_collection/metrics/eod_completeness/{trading_day}.json` (a fixed
   address per trading day, which is what the ladder clause reads).
2. It is written on **every** scheduled execution, the failed one included.
   A guard that records only on the happy path is a guard that stopped running,
   and a trading day with no document must read red rather than absent.
3. A failed write grades `unmeasurable`, never zero coverage — different claims.
4. The mirrored floor constant equals the descriptor's declared floor.
5. OBSERVE mode: no verdict moves an exit code.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
import yaml
from botocore.exceptions import ClientError

import weekly_collector
from collectors import metron_market_data
from data_gate.descriptors import REPO_ROOT
from validators import expectations

TRADING_DAY = "2026-09-14"


class FakeS3:
    def __init__(self, fail_puts_matching: str | None = None):
        self.puts: list[tuple[str, dict]] = []
        self.fail_puts_matching = fail_puts_matching

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        if self.fail_puts_matching and self.fail_puts_matching in Key:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
        raw = Body.decode("utf-8") if isinstance(Body, (bytes, bytearray)) else Body
        self.puts.append((Key, json.loads(raw)))
        return {"ETag": '"abc"'}

    def head_object(self, Bucket, Key):  # noqa: N803
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")


HOLDINGS = [
    {"yf_symbol": "AAPL", "currency": "USD"},
    {"yf_symbol": "MSFT", "currency": "USD"},
    {"yf_symbol": "NVDA", "currency": "USD"},
]


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(
        metron_market_data, "load_metron_universe", lambda bucket, s3: (HOLDINGS, ["USD"])
    )


def _collect(s3, *, priced):
    return metron_market_data.collect(
        bucket="alpha-engine-research",
        run_date=TRADING_DAY,
        s3_client=s3,
        close_source=lambda symbols: {s: (100.0, TRADING_DAY) for s in priced},
        fx_source=lambda ccys: {c: 1.0 for c in ccys},
    )


def _metric_docs(s3: FakeS3) -> list[dict]:
    return [b for k, b in s3.puts if k.startswith("data_collection/metrics/eod_completeness/")]


# ── 1. One reading, two surfaces ────────────────────────────────────────────


def test_full_coverage_publishes_an_ok_completeness_metric(universe):
    s3 = FakeS3()
    result = _collect(s3, priced=["AAPL", "MSFT", "NVDA"])

    assert result["status"] == "ok"
    keys = [k for k, _ in s3.puts]
    assert f"data_collection/metrics/eod_completeness/{TRADING_DAY}.json" in keys
    doc = _metric_docs(s3)[0]
    assert doc["name"] == "data.D20.completeness"
    assert doc["status"] == "GREEN"
    assert doc["value"] == 1.0

    # And the SAME reading rides on the result, for the run manifest.
    assert [g["verdict"] for g in result["guards"]] == ["ok"]
    assert result["guards"][0]["guard"] == expectations.CARDINALITY_GUARD.name
    assert result["guards"][0]["mode"] == "observe"


def test_an_undeclared_miss_is_below_floor_and_names_the_symbol(universe):
    s3 = FakeS3()
    result = _collect(s3, priced=["AAPL", "MSFT"])

    # OBSERVE mode: the run is still `ok`; the verdict is the finding.
    assert result["status"] == "ok"
    assert [g["verdict"] for g in result["guards"]] == ["below_floor"]
    assert "NVDA" in result["guards"][0]["detail"]
    assert _metric_docs(s3)[0]["status"] == "RED"


# ── 2 + 3. Every execution, including the failed one ────────────────────────


def test_a_failed_artifact_write_still_publishes_an_unmeasurable_metric(universe):
    """`observability-policy` §3.1 — the failure path writes the same telemetry
    as the success path, except the completion claim."""
    s3 = FakeS3(fail_puts_matching="market_data/")
    result = _collect(s3, priced=["AAPL", "MSFT", "NVDA"])

    assert result["status"] == "error"
    docs = _metric_docs(s3)
    assert len(docs) == 1, "a failed EOD run must still leave a completeness document"
    assert docs[0]["status"] == "N/A-MISSING-INPUT"
    assert [g["verdict"] for g in result["guards"]] == ["unmeasurable"]
    # UNMEASURABLE, not zero coverage: "we published nothing" and "we published
    # none of the universe" are different claims with different owners.
    assert "not zero coverage" in result["guards"][0]["detail"]


def test_a_metric_put_failure_never_fails_the_eod_run(universe, caplog):
    """A measurement must not gate the thing it measures. The closes are already
    durable; the verdict still reaches the run manifest through `guards`."""
    s3 = FakeS3(fail_puts_matching="eod_completeness")
    result = _collect(s3, priced=["AAPL", "MSFT", "NVDA"])

    assert result["status"] == "ok"
    assert [g["verdict"] for g in result["guards"]] == ["ok"]
    assert any("completeness metric PUT failed" in r.message for r in caplog.records)


# ── 4. The mirrored floor cannot drift from the descriptor ──────────────────


def test_the_mirrored_floor_equals_the_descriptor():
    raw = yaml.safe_load(
        (REPO_ROOT / "registry.d" / "units" / "D20-metron-eod-closes-fx.yaml").read_text()
    )
    assert metron_market_data.D20_COMPLETENESS_FLOOR == float(raw["completeness"]["floor"]), (
        "collectors/metron_market_data.py mirrors D20's completeness floor so the EOD box "
        "does not load the gate's YAML at runtime; the mirror may not drift from the "
        "descriptor that declares it"
    )


# ── 5. The verdict reaches the run manifest through _phase_collect ──────────


class _PhaseCtx:
    skipped = False
    skip_reason = None

    def record_artifact(self, key: str) -> None:
        pass


class FakeRegistry:
    def __init__(self, s3: FakeS3):
        self.date = TRADING_DAY
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = "daily"

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "d" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifests(s3: FakeS3) -> list[dict]:
    return [b for k, b in s3.puts if k.startswith("data_collection/runs/")]


def test_the_cardinality_verdict_lands_on_the_run_manifest(universe):
    s3 = FakeS3()
    result = weekly_collector._phase_collect(
        FakeRegistry(s3),
        "metron_market_data",
        lambda: _collect(s3, priced=["AAPL", "MSFT"]),
        artifact_key=f"market_data/metron/closes/{TRADING_DAY}.json",
    )
    assert result["status"] == "ok"
    m = _manifests(s3)[0]
    assert m["unit_id"] == "D20"
    verdicts = {g["guard"]: g["verdict"] for g in m["guards"]}
    assert verdicts[expectations.CARDINALITY_GUARD.name] == "below_floor"
    assert expectations.EMPTY_FRESH_GUARD.name in verdicts
    assert any(x["name"] == "data.D20.completeness" for x in m["metrics"])


def test_a_failed_collector_still_carries_its_verdict_onto_the_failure_manifest(universe):
    """The guard reading is recorded even though the phase raised — the failure
    manifest carries the telemetry that explains it, not only its cause."""
    s3 = FakeS3(fail_puts_matching="market_data/")
    result = weekly_collector._phase_collect(
        FakeRegistry(s3),
        "metron_market_data",
        lambda: _collect(s3, priced=["AAPL", "MSFT", "NVDA"]),
        artifact_key=f"market_data/metron/closes/{TRADING_DAY}.json",
    )
    assert result["status"] == "error"
    m = _manifests(s3)[0]
    assert m["status"] == "failed"
    verdicts = {g["guard"]: g["verdict"] for g in m["guards"]}
    assert verdicts[expectations.CARDINALITY_GUARD.name] == "unmeasurable"
