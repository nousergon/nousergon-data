"""A `status="partial"` collector must not produce an `ok` D03 manifest —
alpha-engine-config-I11230.

Measured 2026-09-18/21: `collectors/prices.py::collect` returns
``"status": "ok" if not failed_tickers else "partial"`` (I9256's short-fetch
guard correctly refusing a shrinking refresh), but `weekly_collector.py`'s
manifest wrapper (`_phase_body` / `_record_phase_lineage`) branched on
`"error"` and `"degraded"` only. `"partial"` fell through every guard and the
`run_manifest` wrapper completed normally, writing a clean `ok` D03 manifest
that named neither the failed tickers nor a reason — the 2026-09-18 shadow
replay's manifest (`01M301J15SCD38DRKESDXW3JSC.json`, live-verified against
`s3://alpha-engine-research/staging/shadow/2026-09-18/...`) declared 926
outputs and `status: ok` while missing FDXF/HONA/Q/SOLS with no guard or
rejection entry naming them.

Two layers, both covered here:

1. `weekly_collector.py::_phase_body` now enforces a CLOSED status vocabulary
   (`_KNOWN_COLLECTOR_STATUSES`) — an unrecognized status raises loud rather
   than falling through to a completion claim (deliverable 4: the class, not
   the instance — this is the same choke point every OTHER collector's status
   passes through too, so a second collector introducing a new unhandled
   status is caught here without a per-collector fix).
2. `_record_phase_lineage` gives `"partial"` the SAME treatment as
   `"degraded"` (`_DegradedRun`) — the manifest reads `failed` naming the
   loss, while the collector's own returned dict (and therefore the PROCESS
   exit code / aggregate status this run already produced) is unchanged.
"""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest
from botocore.exceptions import ClientError

import weekly_collector
from collectors import prices


# ---------------------------------------------------------------------------
# Fakes (mirrors tests/test_phase_collect_run_manifest.py)
# ---------------------------------------------------------------------------


class _PhaseCtx:
    def __init__(self, skipped: bool = False):
        self.skipped = skipped
        self.skip_reason = None
        self.artifacts: list[str] = []

    def record_artifact(self, key: str) -> None:
        self.artifacts.append(key)


class FakeS3:
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
    def __init__(self, s3: FakeS3, mode: str = "daily"):
        self.date = "2026-09-18"
        self.bucket = "alpha-engine-research"
        self.s3_client = s3
        self.data_mode = mode

    @contextmanager
    def phase(self, name, supports_auto_skip=True, **kw):
        yield _PhaseCtx()


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _manifests(s3: FakeS3) -> list[dict]:
    return [body for key, body in s3.puts if key.startswith("data_collection/runs/")]


# ---------------------------------------------------------------------------
# Layer 1 — collectors/prices.py names the loss
# ---------------------------------------------------------------------------


def test_prices_partial_result_names_the_failed_tickers(monkeypatch):
    def _fake_refresh_stale(s3, bucket, s3_prefix, stale, fetch_period, batch_size, *, trading_day):
        return 2, ["FDXF", "HONA"], [("AAPL", 2514), ("MSFT", 2514)]

    monkeypatch.setattr(prices, "_refresh_stale", _fake_refresh_stale)
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["AAPL", "MSFT", "FDXF", "HONA"])

    result = prices.collect(
        bucket="alpha-engine-research", tickers=["AAPL", "MSFT", "FDXF", "HONA"],
        s3_prefix="predictor/price_cache/", dry_run=False, reference_date="2026-09-18",
    )

    assert result["status"] == "partial"
    assert result["failed"] == 2
    # I11230 deliverable 2: `_DegradedRun` reads error/detail/reason for the
    # manifest's failure text — without this the manifest says "no detail
    # reported", the exact gap the issue names.
    assert "reason" in result
    assert "FDXF" in result["reason"]
    assert "HONA" in result["reason"]
    assert "2 of" in result["reason"] and "tickers failed to refresh" in result["reason"]


def test_a_clean_refresh_never_gets_a_reason_key(monkeypatch):
    monkeypatch.setattr(
        prices, "_refresh_stale",
        lambda *a, **k: (1, [], [("AAPL", 2514)]),
    )
    monkeypatch.setattr(prices, "_find_stale_fast", lambda *a, **k: ["AAPL"])
    result = prices.collect(
        bucket="b", tickers=["AAPL"], s3_prefix="predictor/price_cache/",
        reference_date="2026-09-18",
    )
    assert result["status"] == "ok"
    assert "reason" not in result


# ---------------------------------------------------------------------------
# Layer 2 — weekly_collector.py's manifest wrapper
# ---------------------------------------------------------------------------


def test_a_partial_prices_result_writes_a_failed_manifest_naming_the_loss():
    """The exact measured shape: prices.collect returns status=partial with 4
    failed tickers out of 930. Before this fix, the manifest wrapper wrote
    `status: ok`. After: `status: failed`, with the failed tickers reachable
    from `rows_rejected` (D03's declared `rejected_keys`) and/or `reason`.
    """
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="daily")
    collector_result = {
        "status": "partial",
        "refreshed": 926,
        "stale": 930,
        "failed": 4,
        "failed_tickers": ["FDXF", "HONA", "Q", "SOLS"],
        "total": 964,
        "reason": "4 of 964 tickers failed to refresh: FDXF, HONA, Q, SOLS",
        "written": {},
    }

    result = weekly_collector._phase_collect(
        reg, "prices", lambda: collector_result, supports_auto_skip=False,
    )

    # The PROCESS posture is unchanged: the collector's own "partial" result
    # is returned verbatim, exactly as "degraded" already does (I10784).
    assert result == collector_result

    m = _manifests(s3)[0]
    assert m["status"] == "failed"
    assert "FDXF" in m["reason"] or any(
        "FDXF" in str(r.get("reason", "")) for r in m.get("rows_rejected", [])
    )
    assert m["rows_rejected"] == [{"count": 4, "reason": "short_fetch_guard_refused"}]
    # No completion claim rides alongside the failure — same contract as any
    # other _CollectorError/_DegradedRun failure manifest.
    assert m["outputs"] == []


def test_a_partial_signal_returns_result_is_also_caught_generically():
    """The class-level fix: `signal_returns.py::collect` and
    `alternative.py::collect` ALSO return `status="partial"` today (swept
    2026-09-21) and are covered by the SAME branch in `_record_phase_lineage`
    without any per-collector change — proving the fix lives at the dispatch
    choke point, not bolted onto `prices` alone.
    """
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="phase1")
    result = weekly_collector._phase_collect(
        reg, "signal_returns", lambda: {"status": "partial", "total_written": 5},
        supports_auto_skip=False,
    )
    assert result == {"status": "partial", "total_written": 5}
    assert _manifests(s3)[0]["status"] == "failed"


def test_an_unrecognized_status_fails_loud_rather_than_completing_ok():
    """Deliverable 4: the vocabulary is closed. A collector returning a status
    nobody has classified here (a typo, a new value) must not silently read
    as a completion — the exact failure mode "partial" was."""
    s3 = FakeS3()
    reg = FakeRegistry(s3, mode="daily")
    result = weekly_collector._phase_collect(
        reg, "prices", lambda: {"status": "mostly_ok", "refreshed": 1}, supports_auto_skip=False,
    )
    assert result["status"] == "error"
    m = _manifests(s3)[0]
    assert m["status"] == "failed"
    assert "mostly_ok" in m["reason"]


def test_known_statuses_cover_every_literal_status_a_dispatched_collector_returns():
    """Sweep (deliverable 4): every top-level `status` literal any collector
    reachable from `_phase_collect` can return, pinned against
    `_KNOWN_COLLECTOR_STATUSES`. Measured 2026-09-21 by reading the actual
    `return {...}` statements of every `collect*`/`backfill*` function wired
    into `weekly_collector.py`'s `_phase_collect` call sites:

    - ok / ok_dry_run / error: every collector in `collectors/` and
      `builders/`.
    - degraded: `features/compute.py::compute_and_write` (I7572).
    - partial: `collectors/prices.py::collect` (I11230),
      `collectors/alternative.py::collect`,
      `collectors/signal_returns.py::collect`,
      `collectors/fred_history.py::backfill_to_s3`.
    - skipped: every `collectors/metron_market_data.py::collect*` function
      (empty universe / outside market window), routed to
      `record_empty_production` when nothing is published — unaffected by
      this issue, already correctly counted.

    A new collector returning a status outside this set fails loud at
    `_phase_body` (the test above) rather than needing a NEW test here to
    notice — this test exists to keep the enumerated comment honest, not as
    the enforcement mechanism itself.
    """
    assert weekly_collector._KNOWN_COLLECTOR_STATUSES == frozenset(
        {"ok", "ok_dry_run", "error", "degraded", "partial", "skipped"}
    )
