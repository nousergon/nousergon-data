"""Unit tests for the expense-collector handler.

Pure-logic coverage (month window, forward-only projection, diff rows, fixed
rows, Neon metric walker) plus a full handler run against fake boto3 clients
and a canned HTTP router — asserting the rollup artifact shape, per-provider
rows, error fencing (one dead provider must not blank the others), the
first-writer-wins baseline/snapshot writes, and the config#2843 over-budget
rising-edge Telegram alert (first breach fires once, sustained breach stays
quiet, drop-then-rebreach re-arms).

Run standalone: ``python3 -m pytest test_handler.py -q`` (deploy.sh preflights
this before every package+ship). Hermetic: `nousergon_lib` +
`flow_doctor_telegram` are git-only / bundled deps this suite does not require
installed — they are stubbed in sys.modules BEFORE `import index` (mirrors the
sibling flow-doctor consumers' tests, e.g. overseer-liveness-probe). The
notify path is a no-op stub by default; alert-specific tests monkeypatch
``index.notify_via_flow_doctor`` directly to assert call/no-call.
"""

from __future__ import annotations

import json
import re
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Stub nousergon_lib + flow_doctor_telegram before importing index ──────────
_ng = types.ModuleType("nousergon_lib")
_ng_fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")


class _FleetTelegramTopic:
    CRITICAL = "CRITICAL"
    OPS_HEALTH = "OPS_HEALTH"


_ng_fleet.FleetTelegramTopic = _FleetTelegramTopic
_ng.flow_doctor_fleet = _ng_fleet
sys.modules.setdefault("nousergon_lib", _ng)
sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", _ng_fleet)

_fdt = types.ModuleType("flow_doctor_telegram")
_fdt.notify_via_flow_doctor = lambda *a, **k: True  # type: ignore[attr-defined]
sys.modules["flow_doctor_telegram"] = _fdt

from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)

assert_hermetic_imports_satisfied(__file__)

import index  # noqa: E402

NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)  # July = 31 days
ELAPSED = ((NOW - datetime(2026, 7, 1, tzinfo=timezone.utc)).total_seconds()
           / (31 * 86400.0))  # ≈ 0.5323


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeS3Error(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self, store: dict[str, bytes]):
        self.store = store

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise FakeS3Error("NoSuchKey")
        return {"Body": _Body(self.store[Key])}

    def put_object(self, **kw):
        if kw.get("IfNoneMatch") == "*" and kw["Key"] in self.store:
            raise FakeS3Error("PreconditionFailed")
        self.store[kw["Key"]] = kw["Body"]
        return {}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k, "LastModified": CUR_REFRESHED.get(k, NOW - timedelta(hours=2))}
                             for k in keys], "IsTruncated": False}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        store = self.store

        class _P:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for k in store if k.startswith(Prefix))
                yield {"Contents": [{"Key": k} for k in keys]} if keys else {}
        return _P()


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class FakeSSM:
    def __init__(self, params: dict[str, str]):
        self.params = params

    def get_parameters(self, Names, WithDecryption):
        return {
            "Parameters": [{"Name": n, "Value": self.params[n]}
                           for n in Names if n in self.params],
            "InvalidParameters": [n for n in Names if n not in self.params],
        }


# ── The AWS billing export, faked (alpha-engine-config-I12168) ─────────────
#
# The collector reads AWS spend from the CUR export's parquet files. Decoding
# parquet is `cur_parquet.py`'s job and `test_cur_parquet.py` pins it against
# files pyarrow wrote; here each "parquet file" is the JSON of its rows and
# `cur_parquet.read_rows` is swapped for a JSON reader (autouse fixture below),
# so these tests exercise everything ABOVE the decoder.

#: Per-key LastModified override for the fake listing (default: NOW - 2h).
CUR_REFRESHED: dict[str, datetime] = {}


def _cur_key(period: str) -> str:
    return f"{index.CUR_DATA_PREFIX}/BILLING_PERIOD={period}/nous-ergon-fleet-cur-00001.snappy.parquet"


def cur_line(day: str, cost: float, *, product: str = "AmazonEC2",
             usage: str = "USE1-BoxUsage:t3.small", line_type: str = "Usage",
             system: str | None = None) -> dict:
    return {
        index.CUR_USAGE_START: day,
        index.CUR_COST: cost,
        index.CUR_PRODUCT: product,
        index.CUR_USAGE_TYPE: usage,
        index.CUR_LINE_TYPE: line_type,
        index.CUR_SYSTEM_TAG: system,
    }


def cur_files(by_period: dict[str, list[dict]], *, drop_columns=()) -> dict[str, bytes]:
    """Store entries for one fake export file per period. ``drop_columns``
    models a period written before the export carried those columns."""
    out = {}
    for period, lines in by_period.items():
        rows = [{k: v for k, v in r.items() if k not in drop_columns} for r in lines]
        out[_cur_key(period)] = json.dumps(rows).encode()
    return out


def _json_read_rows(data: bytes, columns: list[str]) -> list[dict]:
    rows = json.loads(data)
    have = set(rows[0]) if rows else set(columns)
    missing = [c for c in columns if c not in have]
    if missing:
        raise KeyError(", ".join(missing))
    out = []
    for r in rows:
        row = {c: r[c] for c in columns}
        if row.get(index.CUR_USAGE_START):
            row[index.CUR_USAGE_START] = datetime.fromisoformat(
                row[index.CUR_USAGE_START]).replace(tzinfo=timezone.utc)
        out.append(row)
    return out


@pytest.fixture(autouse=True)
def _fake_parquet(monkeypatch):
    monkeypatch.setattr(index.cur_parquet, "read_rows", _json_read_rows)
    CUR_REFRESHED.clear()


def july_export() -> dict[str, list[dict]]:
    """NOW is 2026-07-17 12:00Z. July: EC2 instance hours 8.10 (crucible-v2)
    + S3 4.24 (untagged) = 12.34, real usage posted through 07-16, and one
    Savings Plan fee booked IN ADVANCE for 07-25 that must not count. June:
    the same 12.34, its EC2 day carrying crucible-v2."""
    july = [cur_line("2026-07-01", 8.10, system="crucible-v2"),
            cur_line("2026-07-02", 4.24, product="AmazonS3", usage="Requests-Tier1")]
    july += [cur_line(f"2026-07-{d:02d}", 0.0) for d in range(3, 17)]
    july.append(cur_line("2026-07-25", 0.36, product="ComputeSavingsPlans",
                         usage="ComputeSP:1yrNoUpfront", line_type="SavingsPlanRecurringFee"))
    june = [cur_line("2026-06-01", 8.10, system="crucible-v2"),
            cur_line("2026-06-02", 4.24, product="AmazonS3", usage="Requests-Tier1")]
    return {"2026-06": june, "2026-07": july}


class FakeBoto3:
    """Asking for a Cost Explorer client FAILS the test: the collector must never construct a
    Cost Explorer client (alpha-engine-config-I12168)."""

    def __init__(self, s3, ssm):
        self._by_name = {"s3": s3, "ssm": ssm}

    def client(self, name, region_name=None):
        if name == "ce":
            raise AssertionError("the expense collector constructed a Cost Explorer client")
        return self._by_name[name]


def http_router(routes: dict[str, dict]):
    """Match by substring; raise for routes mapped to an exception."""
    def _fake(url, headers=None):
        for frag, resp in routes.items():
            if frag in url:
                if isinstance(resp, Exception):
                    raise resp
                return resp
        raise RuntimeError(f"unrouted URL in test: {url}")
    return _fake


# ---------------------------------------------------------------------------
# Pure-logic tests
# ---------------------------------------------------------------------------

class TestMonthWindow:
    def test_period_and_elapsed(self):
        mw = index._month_window(NOW)
        assert mw["period"] == "2026-07"
        assert mw["elapsed_frac"] == pytest.approx(ELAPSED, abs=1e-6)

    def test_month_start_instant(self):
        mw = index._month_window(datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc))
        assert mw["period"] == "2026-08"
        assert mw["elapsed_frac"] == 0.0


class TestProjection:
    def test_straight_line(self):
        assert index._project(50.0, 0.5) == pytest.approx(100.0)

    def test_too_early_returns_none(self):
        assert index._project(1.0, 0.01) is None

    def test_partial_baseline_extrapolates_forward_only(self):
        # Observed 25% of the month, currently 50% elapsed: the missing early
        # half-month must NOT be back-filled — projected = 10 + rate*(remaining).
        assert index._project(10.0, 0.5, observed_frac=0.25) == pytest.approx(30.0)

    def test_pace(self):
        assert index._pace(120.0, 100.0) == "over"
        assert index._pace(80.0, 100.0) == "under"
        assert index._pace(None, 100.0) is None
        assert index._pace(80.0, None) is None


class TestDiffRow:
    MW = index._month_window(NOW)
    BUDGETS = {"providers": {"openrouter": {"monthly_budget_usd": 4.0}}}

    def test_no_baseline_establishes(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        row = index._diff_row(index._row("openrouter", "OpenRouter"), self.MW,
                              self.BUDGETS, "openrouter", 42.5, {}, "openrouter_total_usage")
        assert row["mtd_cost_usd"] == 0.0
        assert "baseline established" in row["note"]

    def test_diff_and_pace(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        baseline = {"counters": {"openrouter_total_usage": 40.0},
                    "as_of": {"openrouter_total_usage": "2026-07-01T00:10:00+00:00"}}
        row = index._diff_row(index._row("openrouter", "OpenRouter"), self.MW,
                              self.BUDGETS, "openrouter", 42.5, baseline,
                              "openrouter_total_usage")
        assert row["mtd_cost_usd"] == pytest.approx(2.5)
        # 2.5 over ~53% of month → ~4.7 projected > 4.0 budget
        assert row["pace"] == "over"

    def test_negative_diff_clamped(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        baseline = {"counters": {"deepseek_neg_balance": -10.0},
                    "as_of": {"deepseek_neg_balance": "2026-07-01T00:10:00+00:00"}}
        row = index._diff_row(index._row("deepseek", "DeepSeek"), self.MW, {},
                              "deepseek", -12.0, baseline, "deepseek_neg_balance")
        assert row["mtd_cost_usd"] == 0.0  # top-up mid-month, never negative


def neon_v2_doc(*projects: tuple[str, dict]) -> dict:
    """Shape of a ``consumption_history/v2/projects`` response."""
    return {"projects": [
        {"project_id": pid, "periods": [{"period_id": "per-1", "period_plan": "launch",
          "period_start": "2026-07-01T00:00:00Z",
          "consumption": [{"timeframe_start": "2026-07-01T00:00:00Z",
                           "timeframe_end": "2026-08-01T00:00:00Z",
                           "metrics": [{"metric_name": k, "value": v}
                                       for k, v in metrics.items()]}]}]}
        for pid, metrics in projects]}


def neon_routes(*, v2: dict | Exception, projects: dict | None = None) -> dict:
    """Route table with the v2 consumption endpoint FIRST (its URL does not
    contain ``/api/v2/projects``, but ordering keeps the intent obvious)."""
    default = {"project": {
        "name": "nousergon-rag", "org_id": "org-1",
        "data_transfer_bytes": 8_000_000,
        "consumption_period_start": "2026-07-01T00:00:00Z",
        "consumption_period_end": "2026-08-01T00:00:00Z"}}
    return {
        "/consumption_history/v2/projects": v2,
        "/api/v2/projects/p1": projects or default,
        "/api/v2/projects": {"projects": [{"id": "p1", "org_id": "org-1"}]},
    }


class TestNeonPricing:
    """``price_neon_project`` is the whole bill in one pure function — the
    unit conversions were magnitude-verified against the live account
    2026-07-31 (see the constants' comment in index.py)."""

    def test_live_verified_conversions(self):
        priced = index.price_neon_project({
            "compute_unit_seconds": 43_277,          # → 12.021 CU-h
            "root_branch_bytes_month": 1_059_273_298,  # → 1.0593 GB-month
            "instant_restore_bytes_month": 34_025_675,  # → 0.0340 GB-month
            "public_network_transfer_bytes": 421_855_072,  # 0.42 GB, inside 500
        })
        assert priced["compute_cu_hours"] == pytest.approx(12.021, abs=1e-3)
        assert priced["compute_cost_usd"] == pytest.approx(12.021 * 0.106, abs=1e-3)
        assert priced["storage_gb_month"] == pytest.approx(1.0933, abs=1e-3)
        assert priced["storage_cost_usd"] == pytest.approx(1.0933 * 0.35, abs=1e-3)
        assert priced["data_transfer_cost_usd"] == 0.0  # inside the allowance
        assert priced["total_cost_usd"] == pytest.approx(1.6572, abs=2e-3)

    def test_transfer_allowance_is_per_project(self):
        """500 GB of egress is included PER PROJECT — two projects at 400 GB
        each are both inside it; summing first would fabricate a 300 GB
        overage."""
        each = index.price_neon_project(
            {"public_network_transfer_bytes": 400_000_000_000})
        assert each["data_transfer_cost_usd"] == 0.0
        over = index.price_neon_project(
            {"public_network_transfer_bytes": 600_000_000_000})
        assert over["data_transfer_cost_usd"] == pytest.approx(10.0)

    def test_extra_branches_priced(self):
        priced = index.price_neon_project({"extra_branches_month": 2.0})
        assert priced["extra_branches_cost_usd"] == pytest.approx(3.0)
        assert priced["total_cost_usd"] == pytest.approx(3.0)


class TestNeonPeriodPacing:
    def test_period_aware_projection(self, monkeypatch):
        """Neon's consumption period can start mid-calendar-month (plan
        change) — the GB-vs-quota pacing must use ITS bounds, not the calendar
        month's."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=neon_v2_doc(("p1", {"public_network_transfer_bytes": 2_500_000_000})),
            projects={"project": {
                "name": "nousergon-rag", "org_id": "org-1",
                "data_transfer_bytes": 2_500_000_000,
                # Half the period elapsed at NOW (7/17 12:00): 2.5 GB → 5 GB
                "consumption_period_start": "2026-07-14T12:00:00Z",
                "consumption_period_end": "2026-07-20T12:00:00Z"}})))
        row = index.collect_neon(index._month_window(NOW), {},
                                 {index.SSM_NEON: "k", index.SSM_NEON_QUOTA_GB: "5"})
        assert row["quota"]["used"] == pytest.approx(2.5)
        assert row["quota"]["projected"] == pytest.approx(5.0)
        assert row["pace"] is None or row["pace"] == "under"  # 5.0 !> 5 GB

    def test_compute_and_storage_are_priced_not_unknown(self, monkeypatch):
        """The whole point of config#2913's follow-up: compute and storage are
        REAL line items read from the invoice-aligned v2 metrics, not `None`
        with an excuse. 7200 CU-s = 2 CU-h = $0.212; 2 GB-month = $0.70."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=neon_v2_doc(("p1", {"compute_unit_seconds": 7200,
                                   "root_branch_bytes_month": 2_000_000_000,
                                   "public_network_transfer_bytes": 8_000_000})))))
        row = index.collect_neon(index._month_window(NOW), {}, {index.SSM_NEON: "k"})
        assert row["detail"]["compute_cost_usd"] == pytest.approx(0.212)
        assert row["detail"]["storage_cost_usd"] == pytest.approx(0.70)
        assert row["detail"]["data_transfer_cost_usd"] == 0.0
        assert row["mtd_cost_usd"] == pytest.approx(0.912)
        assert row["source"] == "consumption_history_v2"
        assert "cost_components_unavailable" not in row["detail"]
        assert row["detail"]["by_project"][0]["project"] == "nousergon-rag"

    def test_fixed_override_is_ignored_but_reported(self, monkeypatch):
        """A stale ``fixed_monthly_usd`` (the $19 that config#2913 found on
        this row) must NOT outrank a measured bill — it is reported in detail
        so the stale config is visible, never used as the number."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=neon_v2_doc(("p1", {"compute_unit_seconds": 7200})))))
        budgets = {"providers": {"neon": {
            "fixed_monthly_usd": 19.0, "note": "Launch plan — TEMPORARY"}}}
        row = index.collect_neon(index._month_window(NOW), budgets,
                                 {index.SSM_NEON: "k"})
        assert row["mtd_cost_usd"] == pytest.approx(0.212)
        assert row["detail"]["fixed_monthly_usd_ignored"] == 19.0
        assert row["note"] == "Launch plan — TEMPORARY"  # operator note still lands
        # …but a stale operator note must never be the ONLY pricing story on
        # the page: the adapter's own explanation is always in detail.
        assert "invoice-aligned" in row["detail"]["pricing_note"]

    def test_transfer_overage_projection(self, monkeypatch):
        """600 GB used, full-month period, NOW at exactly 50% elapsed ⇒ 100 GB
        overage MTD ($10.00), straight-line-projected 1200 GB month-end ⇒ 700
        GB overage ($70.00)."""
        now = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "_now_utc", lambda: now)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=neon_v2_doc(("p1", {"public_network_transfer_bytes": 600_000_000_000})))))
        row = index.collect_neon(index._month_window(now), {}, {index.SSM_NEON: "k"})
        assert row["mtd_cost_usd"] == pytest.approx(10.0)
        assert row["projected_month_end_usd"] == pytest.approx(70.0)

    def test_v2_unavailable_falls_back_and_says_so(self, monkeypatch):
        """On a plan where the v2 endpoint is gated (Free), the row must fall
        back to the transfer counter, NAME the failure, and not pass off a
        partial figure as the full bill."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=RuntimeError("HTTP 404 from consumption_history/v2: not found"),
            projects={"project": {
                "name": "nousergon-rag", "org_id": "org-1",
                "data_transfer_bytes": 600_000_000_000,
                "consumption_period_start": "2026-07-01T00:00:00Z",
                "consumption_period_end": "2026-08-01T00:00:00Z"}})))
        row = index.collect_neon(index._month_window(NOW), {}, {index.SSM_NEON: "k"})
        assert row["source"] == "projects_api_fallback"
        assert row["detail"]["pricing_source"] == "projects_api_fallback"
        assert "404" in row["detail"]["consumption_v2_error"]
        assert "v2 unreachable" in row["note"]
        assert row["mtd_cost_usd"] == pytest.approx(10.0)  # transfer overage only
        assert row["quota"]["used"] == pytest.approx(600.0)

    def test_query_window_is_month_aligned(self, monkeypatch):
        """At monthly granularity Neon truncates BOTH bounds to month
        boundaries — a `to` of "now" collapses onto `from` and the request
        400s ("'from' must be before 'to'", hit live 2026-07-31). The window
        must span the whole calendar month."""
        seen = {}

        def _fake_http(url, headers=None):
            if "/consumption_history/v2/projects" in url:
                seen["url"] = url
                return neon_v2_doc(("p1", {"compute_unit_seconds": 3600}))
            if "/api/v2/projects/p1" in url:
                return {"project": {"name": "nousergon-rag", "org_id": "org-1",
                                    "data_transfer_bytes": 0,
                                    "consumption_period_start": "2026-07-01T00:00:00Z",
                                    "consumption_period_end": "2026-08-01T00:00:00Z"}}
            return {"projects": [{"id": "p1", "org_id": "org-1"}]}

        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", _fake_http)
        index.collect_neon(index._month_window(NOW), {}, {index.SSM_NEON: "k"})
        assert "from=2026-07-01T00%3A00%3A00Z" in seen["url"]
        assert "to=2026-08-01T00%3A00%3A00Z" in seen["url"]

    def test_cost_and_quota_pace_both_bind(self, monkeypatch):
        """Over the $ budget with transfer nowhere near the quota still paces
        over — and vice versa (the free-plan quota is the other constraint)."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router(neon_routes(
            v2=neon_v2_doc(("p1", {"compute_unit_seconds": 200_000})))))  # 55.6 CU-h
        row = index.collect_neon(index._month_window(NOW),
                                 {"providers": {"neon": {"monthly_budget_usd": 2.0}}},
                                 {index.SSM_NEON: "k", index.SSM_NEON_QUOTA_GB: "5000"})
        assert row["mtd_cost_usd"] > 2.0
        assert row["pace"] == "over"


class TestFixedRows:
    def test_config_only_subscription_row(self):
        budgets = {"providers": {
            "claude_max": {"label": "Claude Max", "fixed_monthly_usd": 200.0},
            "aws": {"monthly_budget_usd": 100.0},  # live adapter key — skipped
        }}
        rows = index.fixed_rows(budgets, {"aws"})
        assert len(rows) == 1
        assert rows[0]["key"] == "claude_max"
        assert rows[0]["status"] == "fixed"
        assert rows[0]["mtd_cost_usd"] == 200.0


# ---------------------------------------------------------------------------
# Full handler run
# ---------------------------------------------------------------------------

@pytest.fixture()
def env(monkeypatch):
    budgets = {
        "schema_version": 1,
        "providers": {
            "aws": {"monthly_budget_usd": 50.0},
            "claude_max": {"label": "Claude Max 20x subscription",
                           "fixed_monthly_usd": 200.0},
            "github_org": {"included_minutes": 2000},
        },
    }
    cost_jsonl = (json.dumps({"cost_usd": 1.25}) + "\n"
                  + json.dumps({"cost_usd": 0.75}) + "\n"
                  + json.dumps({"cost_usd": None}) + "\n").encode()
    store: dict[str, bytes] = {
        "config/expense_budgets.json": json.dumps(budgets).encode(),
        "decision_artifacts/_cost_raw/2026-07-05/run1/agent1.jsonl": cost_jsonl,
        "expenses/baselines/2026-07.json": json.dumps({
            "schema_version": 1, "period": "2026-07",
            "counters": {"openrouter_total_usage": 40.0, "deepseek_neg_balance": -20.0},
            "as_of": {"openrouter_total_usage": "2026-07-01T00:15:00+00:00",
                      "deepseek_neg_balance": "2026-07-01T00:15:00+00:00"},
        }).encode(),
        **cur_files(july_export()),
    }
    s3 = FakeS3(store)
    ssm = FakeSSM({
        index.SSM_OPENROUTER: "sk-or-xxx",
        index.SSM_DEEPSEEK: "sk-ds-xxx",
        index.SSM_NEON: "neon-xxx",
        index.SSM_NEON_QUOTA_GB: "5",
        index.SSM_GITHUB_TOKEN: "ghp-xxx",
        index.SSM_GITHUB_USER_PAT: "ghp-user-xxx",
        # no ANTHROPIC_ADMIN_KEY → client-telemetry fallback path
    })
    monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm))
    monkeypatch.setattr(index, "_now_utc", lambda: NOW)
    monkeypatch.setattr(index, "_http_json", http_router({
        "openrouter.ai/api/v1/credits": {
            "data": {"total_credits": 50.0, "total_usage": 42.5}},
        "api.deepseek.com/user/balance": {
            "balance_infos": [{"currency": "USD", "total_balance": "15.00"}]},
        "/consumption_history/v2/projects": neon_v2_doc(
            ("p1", {"public_network_transfer_bytes": 3_000_000_000,
                    "compute_unit_seconds": 7200,
                    "root_branch_bytes_month": 500_000_000})),
        "/api/v2/projects/p1": {"project": {
            "name": "nousergon", "org_id": "org-1",
            "data_transfer_bytes": 3_000_000_000,
            "compute_time_seconds": 7200,
            "consumption_period_start": "2026-07-01T00:00:00Z",
            "consumption_period_end": "2026-08-01T00:00:00Z"}},
        "/api/v2/projects": {"projects": [{"id": "p1", "org_id": "org-1"}]},
        "organizations/nousergon/settings/billing/usage": {"usageItems": [
            {"product": "Actions", "unitType": "Minutes", "quantity": 1400,
             "netAmount": 0.0, "repositoryName": "alpha-engine-config"},
            {"product": "Actions", "unitType": "Minutes", "quantity": 400,
             "netAmount": 0.0, "repositoryName": "crucible-dashboard"},  # public → free
            {"product": "Packages", "unitType": "GigabyteHours", "quantity": 10,
             "netAmount": 1.5, "repositoryName": "alpha-engine-config"},
        ]},
        "orgs/nousergon/repos?type=private": [
            {"name": "alpha-engine-config", "private": True}],
        # user PAT present but the endpoint is down → fenced hard error row
        "users/cipher813/settings/billing/usage": RuntimeError(
            "HTTP 500 from github: upstream error"),
    }))
    return s3, store


def _rows_by_key(doc):
    return {r["key"]: r for r in doc["providers"]}


class TestHandler:
    def test_full_run(self, env):
        s3, store = env
        result = index.handler({}, None)
        assert result["period"] == "2026-07"

        doc = json.loads(store["expenses/monthly/2026-07.json"])
        assert json.loads(store["expenses/latest.json"]) == doc
        rows = _rows_by_key(doc)

        # AWS: grouped-service sum; month-end projected from THIS month's
        # usage — MTD 12.34 over the 16 posted days of 31 (Brian 2026-10-02),
        # never AWS's forecast over prior months.
        assert rows["aws"]["mtd_cost_usd"] == pytest.approx(12.34)
        assert rows["aws"]["projected_month_end_usd"] == pytest.approx(12.34 / 16 * 31, abs=0.01)
        assert rows["aws"]["detail"]["projection_source"] == "mtd_run_rate"
        assert rows["aws"]["pace"] == "under"  # 23.91 < 50 budget
        assert rows["aws"]["source"] == "billing_export"
        # Keys are Cost Explorer's SERVICE names, which every budget line uses.
        assert rows["aws"]["detail"]["top_services_usd"][index.EC2_COMPUTE] == pytest.approx(8.10)

        # EVERY service is reported, not just the top 8. Seven budget lines had
        # to be REASONED rather than measured on 2026-09-20 because they fell
        # below the 8th-largest service and were invisible here. Cent-level
        # cushions cannot be sized against a number that is not published.
        detail = rows["aws"]["detail"]
        assert detail["service_count"] == len(detail["all_services_usd"])
        assert set(detail["top_services_usd"]) <= set(detail["all_services_usd"]), \
            "top_services_usd must be a SUBSET of all_services_usd"
        # 4dp, not 2dp: a $0.004/month service rounds to $0.00 at two decimals,
        # which is indistinguishable from a service that cost nothing at all.
        assert detail["all_services_usd"][index.EC2_COMPUTE] == pytest.approx(8.10)
        assert detail["all_services_usd"]["Amazon Simple Storage Service"] == pytest.approx(4.24)
        assert detail["posted_through"] == "2026-07-16"
        # and the published total still reconciles to the per-service sum
        assert sum(detail["all_services_usd"].values()) == pytest.approx(
            rows["aws"]["mtd_cost_usd"], abs=0.01)

        # Anthropic: client-telemetry fallback sums cost_usd, tolerating nulls
        ant = rows["anthropic_api"]
        assert ant["source"] == "client_telemetry"
        assert ant["mtd_cost_usd"] == pytest.approx(2.0)
        assert "ANTHROPIC_ADMIN_KEY" in ant["note"]

        # OpenRouter: lifetime-usage diff against the month baseline
        assert rows["openrouter"]["mtd_cost_usd"] == pytest.approx(2.5)
        assert rows["openrouter"]["detail"]["credits_remaining_usd"] == pytest.approx(7.5)

        # DeepSeek: balance fell 20 → 15 ⇒ 5.0 spent
        assert rows["deepseek"]["mtd_cost_usd"] == pytest.approx(5.0)

        # Neon: 3 GB used at ~53% elapsed projects ~5.6 GB > 5 GB quota
        neon = rows["neon"]
        assert neon["quota"]["used"] == pytest.approx(3.0)
        assert neon["quota"]["limit"] == 5.0
        assert neon["pace"] == "over"

        # GitHub org: quota counts PRIVATE-repo minutes only (1400, not the
        # 1800 incl. public-free), paced 1400 @ 53% → ~2630 > 2000
        gh = rows["github_org"]
        assert gh["quota"]["used"] == 1400
        assert gh["detail"]["total_actions_minutes_incl_public_free"] == 1800
        assert gh["pace"] == "over"
        assert gh["mtd_cost_usd"] == pytest.approx(1.5)

        # Public/private breakdown (2026-07-17: a wrong public/private repo
        # classification burned real AWS spend building unnecessary
        # self-hosted-runner infra for 6 actually-public repos — this
        # breakdown is the console-side guardrail against repeating that).
        assert gh["detail"]["gha_private_minutes"] == 1400
        assert gh["detail"]["gha_public_minutes"] == 400
        by_repo = gh["detail"]["gha_by_repo"]
        assert by_repo == [
            {"repo": "alpha-engine-config", "visibility": "private", "minutes": 1400.0},
            {"repo": "crucible-dashboard", "visibility": "public", "minutes": 400.0},
        ]

        # GitHub user: fenced error — recorded on the row, run continues
        assert rows["github_user"]["status"] == "error"
        assert "500" in rows["github_user"]["error"]

        # Fixed row from budgets config
        assert rows["claude_max"]["status"] == "fixed"
        assert rows["claude_max"]["mtd_cost_usd"] == 200.0

        # Totals: ok+fixed rows only; error row flags incomplete
        assert doc["totals"]["incomplete"] is True
        # neon 0.39 = 2 CU-h ($0.212) + 0.5 GB-month ($0.175); its 3 GB of
        # egress is inside the per-project allowance. It used to contribute
        # $0.00 because compute and storage were reported as unpriceable.
        expected_mtd = 12.34 + 2.0 + 2.5 + 5.0 + 0.39 + 1.5 + 200.0
        assert doc["totals"]["mtd_usd"] == pytest.approx(expected_mtd)

        # First-of-day snapshot written with the raw counters
        snap = json.loads(store["expenses/snapshots/2026-07-17.json"])
        assert snap["counters"]["openrouter_total_usage"] == pytest.approx(42.5)

    def test_budgets_missing_degrades_with_warning(self, env, monkeypatch):
        s3, store = env
        del store["config/expense_budgets.json"]
        index.handler({}, None)
        doc = json.loads(store["expenses/monthly/2026-07.json"])
        assert any("budgets SSoT" in w for w in doc["warnings"])
        assert _rows_by_key(doc)["aws"]["budget_usd"] is None

    def test_baseline_established_on_first_run_of_month(self, env):
        s3, store = env
        del store["expenses/baselines/2026-07.json"]
        index.handler({}, None)
        base = json.loads(store["expenses/baselines/2026-07.json"])
        assert base["counters"]["openrouter_total_usage"] == pytest.approx(42.5)
        doc = json.loads(store["expenses/monthly/2026-07.json"])
        row = _rows_by_key(doc)["openrouter"]
        # Baseline was just established mid-month → MTD accrues from now,
        # flagged via the measured-since note; no projection this early.
        assert row["mtd_cost_usd"] == 0.0
        assert "measured since 2026-07-17" in row["note"]
        assert row["projected_month_end_usd"] is None

    def test_user_billing_404_without_user_pat_is_not_configured(self, env, monkeypatch):
        """No fleet token can read cipher813's personal billing (verified live
        2026-07-17): a 404 WITHOUT the dedicated user PAT param is a known
        credential gap, not an outage — must not pollute the error banner."""
        s3, store = env
        mw = index._month_window(NOW)
        monkeypatch.setattr(index, "_http_json", http_router({
            "users/cipher813/settings/billing/usage": RuntimeError(
                "HTTP 404 from github: Not Found"),
        }))
        secrets = {index.SSM_GITHUB_TOKEN: "ghp-xxx"}  # no SSM_GITHUB_USER_PAT
        row = index.collect_github(mw, {}, secrets, account="cipher813", kind="user")
        assert row["status"] == "not_configured"
        assert "Plan:read" in row["error"]

    def test_all_providers_failing_raises(self, env, monkeypatch):
        s3, store = env
        monkeypatch.setattr(index, "_http_json",
                            http_router({}))  # every HTTP call unrouted → raises
        for k in [k for k in store if k.startswith(index.CUR_DATA_PREFIX)]:
            del store[k]  # no export on day 17 → the AWS row errors
        ssm = FakeSSM({index.SSM_GITHUB_TOKEN: "ghp-xxx", index.SSM_NEON: "n"})
        # No budgets fixed rows either → zero ok rows ⇒ systemic failure raises.
        del store["config/expense_budgets.json"]
        monkeypatch.setattr(
            index, "collect_anthropic",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("s3 down")))
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm))
        with pytest.raises(RuntimeError, match="all provider adapters failed"):
            index.handler({}, None)


# ---------------------------------------------------------------------------
# Over-budget Telegram alert (config#2843) — rising-edge per provider/month
# ---------------------------------------------------------------------------

def _row_with_pace(key: str, pace: str | None, **kw) -> dict:
    row = index._row(key, key.upper())
    row.update(pace=pace, mtd_cost_usd=10.0, projected_month_end_usd=20.0,
               budget_usd=15.0)
    row.update(kw)
    return row


class TestOverBudgetAlert:
    PERIOD = "2026-07"

    def test_first_breach_alerts_once(self, monkeypatch):
        """A provider's FIRST flip to pace="over" this month fires exactly
        one Telegram ping."""
        s3 = FakeS3({})
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.run_over_budget_alerts(
            s3, self.PERIOD, [_row_with_pace("aws", "over")])
        assert result["alerted"] == ["aws"]
        notify.assert_called_once()
        state = json.loads(s3.store["expenses/alert_state/2026-07.json"])
        assert state["providers"] == {"aws": True}

    def test_sustained_breach_stays_quiet(self, monkeypatch):
        """Once a provider is already recorded as breached this month, a
        SECOND run that still reports pace="over" must NOT re-alert."""
        store = {"expenses/alert_state/2026-07.json": json.dumps(
            {"period": "2026-07", "providers": {"aws": True}}).encode()}
        s3 = FakeS3(store)
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.run_over_budget_alerts(
            s3, self.PERIOD, [_row_with_pace("aws", "over")])
        assert result["alerted"] == []
        notify.assert_not_called()
        state = json.loads(s3.store["expenses/alert_state/2026-07.json"])
        assert state["providers"] == {"aws": True}  # still recorded breached

    def test_drop_then_rebreach_rearms(self, monkeypatch):
        """A provider that drops back under budget re-arms — the NEXT flip
        to over must alert again (not treated as still-breached)."""
        store = {"expenses/alert_state/2026-07.json": json.dumps(
            {"period": "2026-07", "providers": {"aws": True}}).encode()}
        s3 = FakeS3(store)
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)

        # Run 1: drops back under — no alert, state clears the flag.
        result = index.run_over_budget_alerts(
            s3, self.PERIOD, [_row_with_pace("aws", "under")])
        assert result["alerted"] == []
        notify.assert_not_called()
        state = json.loads(s3.store["expenses/alert_state/2026-07.json"])
        assert state["providers"] == {"aws": False}

        # Run 2: re-breaches in the SAME month — must alert again (re-armed).
        result = index.run_over_budget_alerts(
            s3, self.PERIOD, [_row_with_pace("aws", "over")])
        assert result["alerted"] == ["aws"]
        notify.assert_called_once()

    def test_new_calendar_month_state_key_isolated(self, monkeypatch):
        """State is keyed per calendar-month period — a provider breached in
        June must alert fresh in July even with no June cleanup."""
        store = {"expenses/alert_state/2026-06.json": json.dumps(
            {"period": "2026-06", "providers": {"aws": True}}).encode()}
        s3 = FakeS3(store)
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.run_over_budget_alerts(
            s3, "2026-07", [_row_with_pace("aws", "over")])
        assert result["alerted"] == ["aws"]
        notify.assert_called_once()

    def test_multiple_providers_independent(self, monkeypatch):
        """Each provider's rising-edge state is independent — one already-
        breached provider must not suppress a different provider's fresh
        breach, nor vice versa."""
        store = {"expenses/alert_state/2026-07.json": json.dumps(
            {"period": "2026-07", "providers": {"aws": True}}).encode()}
        s3 = FakeS3(store)
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.run_over_budget_alerts(s3, self.PERIOD, [
            _row_with_pace("aws", "over"),      # already breached — quiet
            _row_with_pace("neon", "over"),     # fresh breach — alerts
            _row_with_pace("openrouter", "under"),  # never breached — quiet
        ])
        assert result["alerted"] == ["neon"]
        assert notify.call_count == 1

    def test_non_over_pace_never_alerts(self, monkeypatch):
        """under / fixed / None paces must never trigger a ping."""
        s3 = FakeS3({})
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.run_over_budget_alerts(s3, self.PERIOD, [
            _row_with_pace("aws", "under"),
            _row_with_pace("claude_max", "fixed"),
            _row_with_pace("deepseek", None),
        ])
        assert result["alerted"] == []
        notify.assert_not_called()

    def test_alert_pass_failure_never_raises(self, monkeypatch):
        """A bug in the alert pass (e.g. state read/write blowing up in a way
        _load/_save don't already fence) must not propagate — this is a
        notification-only enhancement layered after a successful rollup."""
        s3 = FakeS3({})

        def _boom(*a, **k):
            raise RuntimeError("unexpected alert-pass bug")

        monkeypatch.setattr(index, "_load_alert_state", _boom)
        result = index.run_over_budget_alerts(s3, self.PERIOD, [_row_with_pace("aws", "over")])
        assert result["alerted"] == []
        assert "error" in result

    def test_handler_integration_fires_on_over_pace(self, env, monkeypatch):
        """End-to-end: the handler's Neon AND github_org rows both go "over"
        in the default env fixture (Neon: 3 GB projected ~5.6 GB > 5 GB quota;
        github_org: 1400 private minutes @ 53% elapsed → ~2630 > 2000 included
        minutes — see test_full_run) — the alert pass must fire for both and
        record state in the rollup bucket."""
        s3, store = env
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.handler({}, None)
        assert set(result["alerts"]["alerted"]) == {"neon", "github_org"}
        assert notify.call_count == 2
        state = json.loads(store["expenses/alert_state/2026-07.json"])
        assert state["providers"]["neon"] is True
        assert state["providers"]["github_org"] is True

    def test_handler_integration_quiet_when_no_over_pace(self, env, monkeypatch):
        """Loosening the Neon quota AND the github_org included-minutes budget
        removes the fixture's only two "over" rows — the alert pass must then
        stay quiet end-to-end, while sustained per-provider state (from a
        prior run) suppresses nothing new because nothing breaches."""
        s3, store = env
        ssm = index.boto3._by_name["ssm"]
        ssm.params[index.SSM_NEON_QUOTA_GB] = "500"  # 3 GB used, nowhere near breach
        budgets = json.loads(store["config/expense_budgets.json"])
        budgets["providers"]["github_org"]["included_minutes"] = 20000  # 1400 well under
        store["config/expense_budgets.json"] = json.dumps(budgets).encode()
        notify = MagicMock(return_value=True)
        monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
        result = index.handler({}, None)
        assert result["alerts"]["alerted"] == []
        notify.assert_not_called()


# ---------------------------------------------------------------------------
# Month-close reconciliation (alpha-engine-config#2849)
# ---------------------------------------------------------------------------

class TestPriorMonthWindow:
    def test_prior_month_from_mid_month(self):
        pmw = index._prior_month_window(NOW)  # NOW = 2026-07-17
        assert pmw["period"] == "2026-06"
        assert pmw["start"] == datetime(2026, 6, 1, tzinfo=timezone.utc)
        assert pmw["end"] == datetime(2026, 7, 1, tzinfo=timezone.utc)
        assert pmw["elapsed_frac"] == 1.0

    def test_prior_month_from_january(self):
        pmw = index._prior_month_window(datetime(2026, 1, 15, tzinfo=timezone.utc))
        assert pmw["period"] == "2025-12"
        assert pmw["start"] == datetime(2025, 12, 1, tzinfo=timezone.utc)
        assert pmw["end"] == datetime(2026, 1, 1, tzinfo=timezone.utc)


class TestReconciliationRow:
    PRIOR_DOC = {"providers": [
        {"key": "aws", "mtd_cost_usd": 40.0, "projected_month_end_usd": 45.0},
    ]}

    def test_delta_against_last_recorded_mtd(self):
        row = index._reconciliation_row("aws", self.PRIOR_DOC, 50.0)
        assert row["projected_last_seen"] == 45.0
        assert row["accrued_mtd_final"] == 40.0
        assert row["actual_final"] == 50.0
        assert row["delta_usd"] == pytest.approx(10.0)
        assert row["delta_pct"] == pytest.approx(0.25)
        assert row["status"] == "ok"

    def test_missing_prior_doc_yields_nulls(self):
        row = index._reconciliation_row("aws", None, 50.0)
        assert row["projected_last_seen"] is None
        assert row["accrued_mtd_final"] is None
        assert row["delta_usd"] is None
        assert row["delta_pct"] is None
        assert row["actual_final"] == 50.0

    def test_zero_accrued_nonzero_actual_is_full_drift(self):
        row = index._reconciliation_row(
            "aws", {"providers": [{"key": "aws", "mtd_cost_usd": 0.0,
                                   "projected_month_end_usd": None}]}, 12.0)
        assert row["delta_pct"] == 1.0

    def test_not_available_status_carries_note(self):
        row = index._reconciliation_row("neon", self.PRIOR_DOC, None,
                                        status="not_available", note="no historical endpoint")
        assert row["status"] == "not_available"
        assert row["note"] == "no historical endpoint"
        assert row["actual_final"] is None


class TestReconcileAws:
    def test_reconciles_full_prior_month(self):
        pmw = index._prior_month_window(NOW)
        prior_doc = {"providers": [{"key": "aws", "mtd_cost_usd": 10.0,
                                    "projected_month_end_usd": 20.0}]}
        row = index.reconcile_aws(pmw, {}, prior_doc, FakeS3(cur_files(july_export())))
        # June's export: EC2 8.10 + S3 4.24
        assert row["actual_final"] == pytest.approx(12.34)
        assert row["accrued_mtd_final"] == 10.0
        assert row["delta_usd"] == pytest.approx(2.34)
        assert "billing export" in row["note"]

    def test_a_period_written_before_the_service_columns_still_reconciles(self):
        """2026-09 was exported before the product columns existed; the
        month's TOTAL needs only cost and date."""
        pmw = index._prior_month_window(NOW)
        s3 = FakeS3(cur_files(july_export(), drop_columns=(index.CUR_PRODUCT,
                                                           index.CUR_LINE_TYPE)))
        assert index.reconcile_aws(pmw, {}, None, s3)["actual_final"] == pytest.approx(12.34)

    def test_no_export_for_the_month_is_not_available_not_zero(self):
        row = index.reconcile_aws(index._prior_month_window(NOW), {}, None, FakeS3({}))
        assert row["status"] == "not_available"
        assert row["actual_final"] is None


class TestReconcileAnthropic:
    def test_reuses_admin_api_with_bounded_window(self, monkeypatch):
        pmw = index._prior_month_window(NOW)
        seen_urls = []

        def _fake_http(url, headers=None):
            seen_urls.append(url)
            # cost_report amounts are CENTS (config-I2840): 350 cents = $3.50
            return {"data": [{"results": [{"amount": "350"}]}], "has_more": False}

        monkeypatch.setattr(index, "_http_json", _fake_http)
        prior_doc = {"providers": [{"key": "anthropic_api", "mtd_cost_usd": 3.0,
                                    "projected_month_end_usd": 3.0}]}
        row = index.reconcile_anthropic(
            pmw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None, prior_doc)
        assert row["actual_final"] == pytest.approx(3.50)
        assert "starting_at=2026-06-01" in seen_urls[0]
        # I10013: renamed from `ending_before` — the current Admin API
        # (platform.claude.com/docs/en/api/beta/organization/cost_report/
        # retrieve) documents `ending_at`, not `ending_before`.
        assert "ending_at=2026-07-01" in seen_urls[0]

    def test_admin_api_amounts_are_cents_not_dollars(self, monkeypatch):
        """Regression for the 100x overstatement found live 2026-07-20
        (config-I2840): cost_report `amount` is in currency minor units.
        981.60 (cents) must land as $9.82, not $981.60."""
        mw = index._month_window(NOW)

        def _fake_http(url, headers=None):
            return {"data": [{"results": [{"amount": "981.60"},
                                          {"amount": "18.40"}]}],
                    "has_more": False}

        monkeypatch.setattr(index, "_http_json", _fake_http)
        row = index.collect_anthropic(
            mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert row["mtd_cost_usd"] == pytest.approx(10.0)
        assert row["source"] == "admin_api"

    def test_fallback_bounds_to_full_prior_month_days(self, monkeypatch):
        """No admin key ⇒ client-telemetry fallback must sum through the
        PRIOR month's last day (30 for June), not ``now.day`` (17, in July)."""
        pmw = index._prior_month_window(NOW)
        cost_jsonl = json.dumps({"cost_usd": 5.0}).encode()
        store = {"decision_artifacts/_cost_raw/2026-06-30/run/a.jsonl": cost_jsonl}
        s3 = FakeS3(store)
        prior_doc = {"providers": [{"key": "anthropic_api", "mtd_cost_usd": 4.0,
                                    "projected_month_end_usd": None}]}
        row = index.reconcile_anthropic(pmw, {}, {}, s3, prior_doc)
        assert row["actual_final"] == pytest.approx(5.0)


class TestCollectAnthropicDateRange:
    """alpha-engine-config-I10013: the Admin API 400s with "Invalid date
    range: ending date must be after starting date" whenever `ending_at` is
    omitted and no full day has elapsed since `starting_at` — measured live
    at both scheduled `_collect` runs (00:15 and 12:15 UTC) on 2026-08-01 and
    2026-09-01, and on no other date. `collect_anthropic` must always send an
    explicit `ending_at` strictly after `starting_at`, at every month
    boundary — including the 1st of the month and Dec -> Jan, where a naive
    "start of this month" default would collide with `starting_at` itself."""

    def _fake_http(self, seen_urls):
        def _fn(url, headers=None):
            seen_urls.append(url)
            return {"data": [], "has_more": False}
        return _fn

    def test_ending_at_always_present_and_after_starting_at(self, monkeypatch):
        """Live-collect path (no `end` passed): mid-month, `now` far from a
        boundary — the un-pathological case must still carry an explicit,
        later `ending_at` rather than omitting the param."""
        now = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
        mw = index._month_window(now)
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert "starting_at=2026-07-01T00:00:00Z" in seen_urls[0]
        assert "ending_at=2026-07-18T00:00:00Z" in seen_urls[0]

    def test_first_of_month_early_morning_does_not_collapse_the_window(self, monkeypatch):
        """The exact live failure shape: `now` on the 1st, minutes past
        midnight UTC (2026-09-01T00:15:54Z, the 00:15 scheduled run).
        `starting_at` and a naive same-day `ending_at` would be equal (or
        `ending_at` earlier); the fix must push `ending_at` to the START OF
        THE NEXT DAY so it is always strictly after `starting_at`."""
        now = datetime(2026, 9, 1, 0, 15, 54, tzinfo=timezone.utc)
        mw = index._month_window(now)
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        # I11900: that range still 400'd live on 2026-10-01 (the API clamps
        # ending_at to the last completed day), so on the 1st the read starts
        # a day early; prior-month buckets are dropped (tests below).
        assert "starting_at=2026-08-31T00:00:00Z" in seen_urls[0]
        assert "ending_at=2026-09-02T00:00:00Z" in seen_urls[0]

    def test_first_of_month_midday_still_after_starting_at(self, monkeypatch):
        """The second live failure timestamp: 12:15:53 UTC on the 1st — still
        the same calendar day as `starting_at`, so still must not omit or
        collapse `ending_at`."""
        now = datetime(2026, 8, 1, 12, 15, 53, tzinfo=timezone.utc)
        mw = index._month_window(now)
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert "starting_at=2026-07-31T00:00:00Z" in seen_urls[0]
        assert "ending_at=2026-08-02T00:00:00Z" in seen_urls[0]

    def test_december_to_january_boundary(self, monkeypatch):
        """Year rollover: `now` on Dec 31 means "tomorrow" is Jan 1 of the
        NEXT year — `timedelta(days=1)` must cross that boundary correctly
        rather than wrapping back within December."""
        now = datetime(2026, 12, 31, 23, 50, tzinfo=timezone.utc)
        mw = index._month_window(now)
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert "starting_at=2026-12-01T00:00:00Z" in seen_urls[0]
        assert "ending_at=2027-01-01T00:00:00Z" in seen_urls[0]

    def test_second_of_month_is_not_widened(self, monkeypatch):
        """Once the 1st has completed, the month's own start is a valid
        range again; only the in-progress first day is widened (I11900)."""
        now = datetime(2026, 10, 2, 0, 15, 51, tzinfo=timezone.utc)
        mw = index._month_window(now)
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert "starting_at=2026-10-01T00:00:00Z" in seen_urls[0]
        assert "ending_at=2026-10-03T00:00:00Z" in seen_urls[0]

    def test_first_of_month_drops_the_prior_month_bucket(self, monkeypatch):
        """alpha-engine-config-I11900, the live 2026-10-01T00:15:51Z run: the
        widened read returns 09-30's completed bucket, which must not be
        counted toward October's MTD; an October bucket, if present, is."""
        now = datetime(2026, 10, 1, 0, 15, 51, tzinfo=timezone.utc)
        mw = index._month_window(now)

        def _fake_http(url, headers=None):
            assert "starting_at=2026-09-30T00:00:00Z" in url
            return {"data": [
                {"starting_at": "2026-09-30T00:00:00Z", "ending_at": "2026-10-01T00:00:00Z",
                 "results": [{"amount": "5000"}]},
                {"starting_at": "2026-10-01T00:00:00Z", "ending_at": "2026-10-02T00:00:00Z",
                 "results": [{"amount": "250"}]},
            ], "has_more": False}

        monkeypatch.setattr(index, "_http_json", _fake_http)
        row = index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)
        assert row["mtd_cost_usd"] == pytest.approx(2.50)
        assert row["source"] == "admin_api"

    def test_first_of_month_bucket_without_start_fails_loud(self, monkeypatch):
        """A widened-read bucket that cannot be placed in a month raises (so
        `provider_failed` fires) instead of guessing which month it is."""
        now = datetime(2026, 10, 1, 12, 15, 51, tzinfo=timezone.utc)
        mw = index._month_window(now)
        monkeypatch.setattr(index, "_http_json", lambda url, headers=None: {
            "data": [{"results": [{"amount": "100"}]}], "has_more": False})
        with pytest.raises(RuntimeError, match="starting_at"):
            index.collect_anthropic(mw, {}, {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None)

    def test_reconciliation_still_uses_the_closed_month_boundary(self, monkeypatch):
        """The reconciliation path (`end` explicitly given) must keep using
        that CLOSED-month boundary verbatim, not the "tomorrow" default that
        only applies to the live open-ended read."""
        pmw = index._prior_month_window(datetime(2027, 1, 15, tzinfo=timezone.utc))
        seen_urls = []
        monkeypatch.setattr(index, "_http_json", self._fake_http(seen_urls))
        index.collect_anthropic({**pmw, "elapsed_frac": 1.0}, {},
                                {index.SSM_ANTHROPIC_ADMIN: "admin-key"}, None,
                                end=pmw["end"], last_day=31)
        assert "starting_at=2026-12-01T00:00:00Z" in seen_urls[0]
        assert "ending_at=2027-01-01T00:00:00Z" in seen_urls[0]


class TestLogProviderFailed:
    """alpha-engine-config-I10013 deliverable 1/4: the fail-loud contract is
    one STABLE, greppable line the CloudWatch metric filter can match — not
    the traceback's first line, which varies by exception type."""

    def test_http_error_carries_the_status_code(self, caplog):
        exc = RuntimeError("HTTP 403 from https://example-provider.test/x: {\"message\":\"Forbidden\"}")
        with caplog.at_level("ERROR"):
            index._log_provider_failed("github_org", exc)
        assert "provider_failed provider=github_org status=403" in caplog.text

    def test_non_http_error_reports_n_a_status(self, caplog):
        exc = ValueError("Expecting value: line 1 column 1 (char 0)")
        with caplog.at_level("ERROR"):
            index._log_provider_failed("neon", exc)
        assert "provider_failed provider=neon status=n/a" in caplog.text

    def test_collect_fence_emits_the_stable_line_alongside_the_traceback(self, monkeypatch, caplog):
        """The line must survive through the real `_collect` fence, not just
        the helper in isolation — and the traceback logging must remain."""
        mw = index._month_window(NOW)

        def _boom(*a, **k):
            raise RuntimeError("HTTP 400 from https://example-provider.test/x: bad range")

        monkeypatch.setattr(index, "collect_anthropic", _boom)
        rows: list[dict] = []

        def fenced(key, label, fn):
            try:
                rows.append(fn())
            except Exception as exc:  # noqa: BLE001 — mirrors the real fence
                index._log_provider_failed(key, exc)
                index.logger.exception("provider %s failed", key)
                rows.append(index._row(key, label, status="error", error=str(exc)[:300]))

        with caplog.at_level("ERROR"):
            fenced("anthropic_api", "Anthropic API", lambda: index.collect_anthropic(mw, {}, {}, None))
        assert "provider_failed provider=anthropic_api status=400" in caplog.text
        assert "provider anthropic_api failed" in caplog.text
        assert rows[0]["status"] == "error"


class TestReconcileCounterDiff:
    def test_diffs_two_month_start_baselines(self):
        store = {
            "expenses/baselines/2026-06.json": json.dumps(
                {"counters": {"openrouter_total_usage": 30.0}}).encode(),
            "expenses/baselines/2026-07.json": json.dumps(
                {"counters": {"openrouter_total_usage": 42.5}}).encode(),
        }
        s3 = FakeS3(store)
        prior_doc = {"providers": [{"key": "openrouter", "mtd_cost_usd": 11.0,
                                    "projected_month_end_usd": 12.0}]}
        row = index.reconcile_counter_diff(
            s3, "2026-06", "2026-07", "openrouter", "openrouter_total_usage", prior_doc)
        assert row["actual_final"] == pytest.approx(12.5)
        assert row["status"] == "ok"

    def test_deepseek_diffs_already_oriented_counter(self):
        """deepseek_neg_balance is stored as -balance (rises as spend
        accrues, per ensure_baseline/collect_deepseek) — a plain forward diff
        of the two baselines already yields positive spend, no extra sign
        flip needed."""
        store = {
            "expenses/baselines/2026-06.json": json.dumps(
                {"counters": {"deepseek_neg_balance": -20.0}}).encode(),
            "expenses/baselines/2026-07.json": json.dumps(
                {"counters": {"deepseek_neg_balance": -15.0}}).encode(),
        }
        s3 = FakeS3(store)
        row = index.reconcile_counter_diff(
            s3, "2026-06", "2026-07", "deepseek", "deepseek_neg_balance", None)
        assert row["actual_final"] == pytest.approx(5.0)

    def test_missing_baseline_is_not_available(self):
        s3 = FakeS3({})
        row = index.reconcile_counter_diff(
            s3, "2026-06", "2026-07", "openrouter", "openrouter_total_usage", None)
        assert row["status"] == "not_available"
        assert row["actual_final"] is None


class TestReconcileNeon:
    def test_closed_month_requeried_from_v2(self, monkeypatch):
        """The prior month's FINAL charge is re-read from the same
        invoice-aligned metrics (v2 accepts an arbitrary historical window) —
        this row used to be permanently not_available."""
        seen = {}

        def _fake_http(url, headers=None):
            if "/consumption_history/v2/projects" in url:
                seen["url"] = url
                return neon_v2_doc(("p1", {"compute_unit_seconds": 36_000,
                                           "root_branch_bytes_month": 1_000_000_000}))
            return {"projects": [{"id": "p1", "org_id": "org-1"}]}

        monkeypatch.setattr(index, "_http_json", _fake_http)
        pmw = index._prior_month_window(NOW)
        row = index.reconcile_neon(pmw, {index.SSM_NEON: "k"},
                                   {"providers": [{"key": "neon", "mtd_cost_usd": 1.0}]})
        # 10 CU-h ($1.06) + 1 GB-month ($0.35) = $1.41 final
        assert row["actual_final"] == pytest.approx(1.41)
        assert row["status"] == "ok"
        assert "from=2026-06-01T00%3A00%3A00Z" in seen["url"]
        assert "to=2026-07-01T00%3A00%3A00Z" in seen["url"]

    def test_not_configured_without_key(self):
        row = index.reconcile_neon(index._prior_month_window(NOW), {}, None)
        assert row["status"] == "not_configured"
        assert row["actual_final"] is None


class TestReconcileGithub:
    def test_targets_prior_month_year_month(self, monkeypatch):
        pmw = index._prior_month_window(NOW)
        seen = {}

        def _fake_http(url, headers=None):
            seen["url"] = url
            return {"usageItems": [
                {"product": "Actions", "unitType": "Minutes", "quantity": 900,
                 "netAmount": 2.0, "repositoryName": "alpha-engine-config"},
            ]}

        monkeypatch.setattr(index, "_http_json", _fake_http)
        monkeypatch.setattr(index, "_private_repo_names",
                            lambda account, kind, headers: {"alpha-engine-config"})
        secrets = {index.SSM_GITHUB_TOKEN: "ghp-xxx"}
        prior_doc = {"providers": [{"key": "github_org", "mtd_cost_usd": 1.5,
                                    "projected_month_end_usd": 3.0}]}
        row = index.reconcile_github(pmw, {}, secrets, account=index.GITHUB_ORG,
                                     kind="org", prior_doc=prior_doc)
        assert "year=2026&month=6" in seen["url"]
        assert row["actual_final"] == pytest.approx(2.0)

    def test_not_configured_passthrough(self, monkeypatch):
        pmw = index._prior_month_window(NOW)
        row = index.reconcile_github(pmw, {}, {}, account=index.GITHUB_USER,
                                     kind="user", prior_doc=None)
        assert row["status"] == "not_configured"


class TestRunReconciliation:
    def test_writes_reconciliation_artifact_and_flags_drift(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        prior_doc = {
            "schema_version": 1, "period": "2026-06",
            "providers": [
                {"key": "aws", "mtd_cost_usd": 5.0, "projected_month_end_usd": 6.0},
            ],
        }
        store = {
            "expenses/monthly/2026-06.json": json.dumps(prior_doc).encode(),
            "expenses/baselines/2026-06.json": json.dumps(
                {"counters": {"openrouter_total_usage": 30.0,
                              "deepseek_neg_balance": -20.0}}).encode(),
            "expenses/baselines/2026-07.json": json.dumps(
                {"counters": {"openrouter_total_usage": 42.5,
                              "deepseek_neg_balance": -15.0}}).encode(),
        }
        store.update(cur_files(july_export()))
        s3 = FakeS3(store)
        ssm = FakeSSM({})
        # June's export totals 12.34 → aws delta vs prior_doc's 5.0 accrued is
        # large enough to flag past the threshold.
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm))
        monkeypatch.setattr(index, "_http_json", http_router({}))  # anthropic/github → error rows
        result = index.run_reconciliation(s3, NOW, {}, {})
        assert result["period"] == "2026-06"
        doc = json.loads(store["expenses/reconciliation/2026-06.json"])
        assert doc["period"] == "2026-06"
        assert doc["providers"]["aws"]["actual_final"] == pytest.approx(12.34)
        assert "aws" in doc["flagged"]  # (12.34-5.0)/5.0 = 146% >> 8% threshold
        # no Neon key in this fixture's secrets → honest not_configured (the
        # row is a real re-query when the key is present, see TestReconcileNeon)
        assert doc["providers"]["neon"]["status"] == "not_configured"
        # openrouter/deepseek reconciled purely from the two baselines above,
        # with zero HTTP calls (the unrouted http_router({}) would raise if hit).
        assert doc["providers"]["openrouter"]["actual_final"] == pytest.approx(12.5)
        assert doc["providers"]["deepseek"]["actual_final"] == pytest.approx(5.0)

    def test_one_provider_failure_does_not_blank_others(self, monkeypatch):
        """Mirrors the live collect fence: a reconcile_* exception for one
        provider must not prevent the others from being written."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        s3 = FakeS3({})
        ssm = FakeSSM({})

        def _boom(*a, **k):
            raise RuntimeError("export down")

        monkeypatch.setattr(index, "reconcile_aws", _boom)
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm))
        monkeypatch.setattr(index, "_http_json", http_router({}))
        index.run_reconciliation(s3, NOW, {}, {})
        doc = json.loads(s3.store["expenses/reconciliation/2026-06.json"])
        assert doc["providers"]["aws"]["status"] == "error"
        assert "export down" in doc["providers"]["aws"]["note"]
        assert doc["providers"]["neon"]["status"] == "not_configured"


class TestHandlerReconcileMode:
    def test_handler_mode_reconcile_dispatches(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        s3 = FakeS3({})
        ssm = FakeSSM({})
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm))
        monkeypatch.setattr(index, "_http_json", http_router({}))
        result = index.handler({"mode": "reconcile"}, None)
        assert result["period"] == "2026-06"
        assert "expenses/reconciliation/2026-06.json" in s3.store

    def test_handler_default_mode_is_collect(self, env):
        """Missing/empty event must behave exactly as before this feature —
        the twice-daily Scheduler rule's Input ("{}"​) is unchanged."""
        s3, store = env
        result = index.handler({}, None)
        assert result["period"] == "2026-07"  # collect-mode shape, not reconcile's
        assert "providers" in result and isinstance(result["providers"], int)

    def test_handler_unknown_mode_raises(self, env):
        s3, store = env
        with pytest.raises(ValueError, match="unknown expense-collector event mode"):
            index.handler({"mode": "bogus"}, None)


# ---------------------------------------------------------------------------
# AWS from the billing export, and NEVER from Cost Explorer (I12168)
# ---------------------------------------------------------------------------

class TestNoCostExplorer:
    """AWS Support processes the I10389 $441.67 credit only once the account
    makes no Cost Explorer calls (Brian, 2026-10-08). This Lambda was the last
    machine caller. These fail on any path back."""

    def test_the_module_never_constructs_a_cost_explorer_client(self):
        src = Path(index.__file__).read_text()
        assert not re.search(r"""client\(\s*["']ce["']""", src)
        assert "get_cost_and_usage" not in src
        assert "get_cost_forecast" not in src

    def test_the_role_grants_no_cost_explorer_action(self):
        policy = json.loads((Path(__file__).parent / "iam-policy.json").read_text())
        granted = [a for st in policy["Statement"] if st.get("Effect") == "Allow"
                   for a in ([st["Action"]] if isinstance(st["Action"], str) else st["Action"])]
        assert not [a for a in granted if a.lower().startswith("ce:") or a == "*"], granted

    def test_the_role_can_read_the_export_it_now_depends_on(self):
        policy = json.loads((Path(__file__).parent / "iam-policy.json").read_text())
        resources = json.dumps([st for st in policy["Statement"]
                                if "s3:GetObject" in st.get("Action", [])])
        assert f"arn:aws:s3:::{index.CUR_BUCKET}/{index.CUR_DATA_PREFIX.split('/')[0]}/*" in resources


class TestAwsFromTheBillingExport:
    def test_the_projection_is_mtd_over_posted_days(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        row = index.collect_aws(index._month_window(NOW), {"aws": 50},
                                FakeS3(cur_files(july_export())))
        assert row["mtd_cost_usd"] == pytest.approx(12.34)
        assert row["projected_month_end_usd"] == pytest.approx(12.34 / 16 * 31, abs=0.01)
        assert row["detail"]["projection_source"] == "mtd_run_rate"

    def test_a_fee_booked_for_a_later_day_is_not_month_to_date(self, monkeypatch):
        """The export books the Savings Plan fee for every remaining day in
        advance; counting it would turn a pre-payment into usage."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        row = index.collect_aws(index._month_window(NOW), {},
                                FakeS3(cur_files(july_export())))
        assert "Savings Plans for AWS Compute usage" not in row["detail"]["all_services_usd"]

    def test_mtd_stops_at_the_newest_posted_day_not_at_today(self, monkeypatch):
        """A lagging export must not stretch the projection over days it has
        not posted: 12.34 over 5 posted days, not 16."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        export = july_export()
        export["2026-07"] = [r for r in export["2026-07"]
                             if r[index.CUR_USAGE_START] <= "2026-07-05"
                             or r[index.CUR_LINE_TYPE] != "Usage"]
        row = index.collect_aws(index._month_window(NOW), {}, FakeS3(cur_files(export)))
        assert row["detail"]["posted_through"] == "2026-07-05"
        assert row["projected_month_end_usd"] == pytest.approx(12.34 / 5 * 31, abs=0.01)

    def test_nothing_posted_yet_is_no_projection_not_zero(self, monkeypatch):
        day2 = datetime(2026, 10, 2, 12, 15, tzinfo=timezone.utc)
        CUR_REFRESHED[_cur_key("2026-10")] = day2 - timedelta(hours=3)
        monkeypatch.setattr(index, "_now_utc", lambda: day2)
        export = {"2026-10": [cur_line("2026-10-05", 0.36, product="ComputeSavingsPlans",
                                       line_type="SavingsPlanRecurringFee")]}
        row = index.collect_aws(index._month_window(day2), {}, FakeS3(cur_files(export)))
        assert row["mtd_cost_usd"] == 0
        assert row["projected_month_end_usd"] is None
        assert row["pace"] is None
        assert row["detail"]["projection_source"] == "pending_no_usage_posted"

    def test_the_first_of_the_month_before_the_export_writes_the_period(self, monkeypatch):
        first = datetime(2026, 7, 1, 0, 16, tzinfo=timezone.utc)
        monkeypatch.setattr(index, "_now_utc", lambda: first)
        export = july_export()
        del export["2026-07"]
        row = index.collect_aws(index._month_window(first), {}, FakeS3(cur_files(export)))
        assert row["mtd_cost_usd"] == 0.0
        assert "month just started" in row["note"]
        # the closed-month series the gates read on the 1st-3rd is still there
        series = row["detail"]["daily_by_system"]
        assert series["days"][-1]["date"] == "2026-06-30"

    def test_no_period_after_the_first_is_an_error_not_zero(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        with pytest.raises(RuntimeError, match="no files for 2026-07"):
            index.collect_aws(index._month_window(NOW), {}, FakeS3({}))

    def test_a_stale_export_fails_the_row_rather_than_republishing_it(self, monkeypatch):
        """Consumers grade freshness on the rollup's `as_of`, which is fresh
        every run; a stopped export would hide behind it."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        CUR_REFRESHED[_cur_key("2026-07")] = NOW - timedelta(hours=40)
        with pytest.raises(RuntimeError, match="last refreshed 40.0h ago"):
            index.collect_aws(index._month_window(NOW), {}, FakeS3(cur_files(july_export())))

    def test_service_names_are_cost_explorers(self):
        sf = index.service_for
        assert sf("AmazonEC2", "USE1-BoxUsage:t3.small", "Usage") == index.EC2_COMPUTE
        assert sf("AmazonEC2", "USE1-EBS:VolumeUsage.gp3", "Usage") == "EC2 - Other"
        assert sf("AmazonStates", "StateTransition", "Usage") == "AWS Step Functions"
        assert sf("AWSDataTransfer", "DataTransfer-Out-Bytes", "Tax") == "Tax"
        assert sf("ComputeSavingsPlans", "x", "SavingsPlanRecurringFee") == \
            "Savings Plans for AWS Compute usage"
        # unknown codes publish raw — the monitor grades them `unbudgeted`
        assert sf("AmazonNewThing", "x", "Usage") == "AmazonNewThing"

    def test_savings_plan_covered_usage_and_its_negation_net_out(self):
        rows = _json_read_rows(json.dumps([
            cur_line("2026-07-01", 0.5, line_type="SavingsPlanCoveredUsage"),
            cur_line("2026-07-01", -0.5, line_type="SavingsPlanNegation"),
            cur_line("2026-07-01", 1.0)]).encode(), list(cur_line("x", 0)))
        assert index._unblended_by_service(rows, "2026-07-01", "2026-07-02") == {
            index.EC2_COMPUTE: 1.0}


def _deploy_sh() -> str:
    return (Path(__file__).parent / "deploy.sh").read_text()


class TestTheScheduleRetryBoundIsDeclared:
    """EventBridge Scheduler defaults to 185 retries; each is a fresh run that
    re-reads every provider (I11206). Kept after I12168 removed Cost Explorer."""

    def test_the_retry_bound_is_declared_not_left_to_the_aws_default(self):
        src = _deploy_sh()
        assert "SCHED_MAX_RETRIES=2" in src
        assert "SCHED_MAX_EVENT_AGE_SECONDS=3600" in src

    def test_every_schedule_carries_the_bound_on_create_AND_update(self):
        src = _deploy_sh()
        assert src.count('"RetryPolicy":{"MaximumRetryAttempts":%s') == 1
        assert src.count('--target "${target}"') == 2, (
            "create-schedule and update-schedule must both pass the same target"
        )

    def test_the_parquet_reader_ships_in_the_package(self):
        assert 'cp "${SCRIPT_DIR}/cur_parquet.py" "${PKG}/cur_parquet.py"' in _deploy_sh()


# --------------------------------------------------------------------------
# SPEND-MONITOR repository_dispatch (alpha-engine-config-I11374)
# --------------------------------------------------------------------------

def test_dispatch_is_non_fatal_and_records_the_failure():
    """The rollup has already landed when this runs. A GitHub outage must not
    fail a run whose real work succeeded — but it must not be silent either."""
    import index

    def _boom(*a, **kw):
        raise RuntimeError("github unreachable")

    orig = index.boto3.client
    index.boto3.client = _boom
    try:
        out = index._dispatch_spend_monitor()
    finally:
        index.boto3.client = orig

    assert out["dispatched"] is False
    assert "RuntimeError" in out["error"]
    assert "github unreachable" in out["error"]


def test_dispatch_can_be_switched_off_without_a_deploy(monkeypatch):
    import index

    monkeypatch.setattr(index, "SPEND_MONITOR_DISPATCH_ENABLED", False)
    out = index._dispatch_spend_monitor()
    assert out == {"dispatched": False, "reason": "disabled"}


def test_dispatch_targets_the_spend_monitor_workflow_in_the_config_repo():
    """The event type must match the `repository_dispatch: types:` the
    receiving workflow declares, or the dispatch is accepted by GitHub and
    starts nothing — the failure mode with no error to see."""
    import index

    assert index.SPEND_MONITOR_DISPATCH_REPO == "nousergon/alpha-engine-config"
    assert index.SPEND_MONITOR_DISPATCH_EVENT_TYPE == "aws-spend-monitor"


def test_the_pat_grant_exists_in_the_iam_policy():
    """`_dispatch_spend_monitor` reads a SecureString the role must be allowed
    to read; without the Sid the dispatch AccessDenies on every run."""
    import json
    import pathlib

    policy = json.loads(
        (pathlib.Path(__file__).resolve().parent / "iam-policy.json").read_text())
    sids = {s.get("Sid"): s for s in policy["Statement"]}
    assert "SpendMonitorDispatchPAT" in sids, sorted(sids)
    grant = sids["SpendMonitorDispatchPAT"]
    assert grant["Action"] == ["ssm:GetParameter"]
    # Scoped to ONE parameter — never a prefix wildcard.
    assert grant["Resource"].endswith("/alpha-engine/saturday_sf_watch/github_pat")
    assert "*" not in grant["Resource"]


def test_collect_reports_the_dispatch_outcome_in_its_result():
    """A swallowed failure needs a recording surface. The handler's own return
    value is it, so the outcome is visible in the invocation result."""
    import inspect

    import index

    src = inspect.getsource(index._collect)
    assert "_dispatch_spend_monitor()" in src
    assert "spend_monitor_dispatch" in src


# --------------------------------------------------------------------------
# Per-system daily series (alpha-engine-config-I11707)
# --------------------------------------------------------------------------

class TestDailyBySystem:
    def test_window_covers_the_prior_month_and_thirty_one_days(self):
        # Mid-month: the prior month's 1st is the earlier bound.
        assert index._daily_by_system_window(
            datetime(2026, 9, 29, 12, tzinfo=timezone.utc)) == ("2026-08-01", "2026-09-29")
        # Early March: 31 days back reaches past February's 1st.
        assert index._daily_by_system_window(
            datetime(2026, 3, 1, 0, 16, tzinfo=timezone.utc)) == ("2026-01-29", "2026-03-01")

    @staticmethod
    def _three_a_day(start: str, end: str) -> dict[str, list[dict]]:
        """Every day $2.00 crucible-v2 + $1.00 untagged, as real usage."""
        from datetime import date
        out: dict[str, list[dict]] = {}
        d, stop = date.fromisoformat(start), date.fromisoformat(end)
        while d < stop:
            out.setdefault(d.strftime("%Y-%m"), []).extend([
                cur_line(d.isoformat(), 2.0, system="crucible-v2"),
                cur_line(d.isoformat(), 1.0, product="AmazonS3", usage="TimedStorage")])
            d += timedelta(days=1)
        return out

    def test_groups_split_by_system_and_sum_to_the_account_total(self):
        now = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)
        out = index.collect_aws_daily_by_system(
            FakeS3(cur_files(self._three_a_day("2026-08-01", "2026-09-29"))), now)
        assert out["tag_key"] == "system"
        assert out["source"] == "billing_export"
        assert out["complete"] is True
        assert len(out["days"]) == 59  # 2026-08-01 .. 2026-09-28
        day = out["days"][0]
        assert day["date"] == "2026-08-01"
        assert day["by_system_usd"] == {"(untagged)": 1.0, "crucible-v2": 2.0}
        assert out["days"][0]["estimated"] is False  # August, closed and final
        assert out["days"][-1]["estimated"] is True  # the open month

    def test_the_prior_month_stays_estimated_through_the_fifth(self):
        now = datetime(2026, 10, 3, 12, tzinfo=timezone.utc)
        out = index.collect_aws_daily_by_system(
            FakeS3(cur_files(self._three_a_day("2026-09-01", "2026-10-03"))), now)
        assert {d["estimated"] for d in out["days"]} == {True}

    def test_days_the_export_has_not_posted_are_absent_not_cheap(self):
        """A lagging export: nothing after 09-20 has posted. Those days must be
        missing from the series (crucible: UNMEASURABLE), never $0."""
        now = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)
        out = index.collect_aws_daily_by_system(
            FakeS3(cur_files(self._three_a_day("2026-08-01", "2026-09-21"))), now)
        assert out["posted_through"] == "2026-09-20"
        assert out["days"][-1]["date"] == "2026-09-20"

    def test_a_period_with_no_export_is_incomplete_and_its_days_absent(self):
        now = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)
        out = index.collect_aws_daily_by_system(
            FakeS3(cur_files(self._three_a_day("2026-09-01", "2026-09-29"))), now)
        assert out["complete"] is False
        assert out["days"][0]["date"] == "2026-09-01"

    def test_a_period_written_before_the_service_columns_still_has_the_series(self):
        now = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
        export = self._three_a_day("2026-09-01", "2026-10-08")
        files = cur_files({"2026-09": export["2026-09"]},
                          drop_columns=(index.CUR_PRODUCT, index.CUR_USAGE_TYPE,
                                        index.CUR_LINE_TYPE))
        files.update(cur_files({"2026-10": export["2026-10"]}))
        out = index.collect_aws_daily_by_system(FakeS3(files), now)
        assert out["complete"] is True
        assert len(out["days"]) == 37

    def test_a_failed_series_degrades_only_its_own_field(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)

        def _denied(*a, **k):
            raise RuntimeError("AccessDenied")

        monkeypatch.setattr(index, "collect_aws_daily_by_system", _denied)
        row = index.collect_aws(index._month_window(NOW), {}, FakeS3(cur_files(july_export())))
        assert row["mtd_cost_usd"] == pytest.approx(12.34)
        assert "daily_by_system" not in row["detail"]
        assert "AccessDenied" in row["detail"]["daily_by_system_error"]
