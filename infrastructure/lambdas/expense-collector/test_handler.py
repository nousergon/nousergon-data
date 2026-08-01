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
import sys
import types
from datetime import datetime, timezone
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


class FakeCE:
    def __init__(self, fail_forecast: bool = False):
        self.fail_forecast = fail_forecast

    def get_cost_and_usage(self, **kw):
        return {"ResultsByTime": [{"Groups": [
            {"Keys": ["AmazonEC2"], "Metrics": {"UnblendedCost": {"Amount": "8.10"}}},
            {"Keys": ["AmazonS3"], "Metrics": {"UnblendedCost": {"Amount": "4.24"}}},
        ]}]}

    def get_cost_forecast(self, **kw):
        if self.fail_forecast:
            raise RuntimeError("forecast unavailable")
        # MONTHLY-granularity forecast returns the FULL month-end total (already
        # includes MTD), NOT the remainder — this is the AWS-console figure.
        return {"Total": {"Amount": "25.00"}}


class FakeBoto3:
    def __init__(self, s3, ssm, ce):
        self._by_name = {"s3": s3, "ssm": ssm, "ce": ce}

    def client(self, name, region_name=None):
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
    monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, FakeCE()))
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

        # AWS: grouped-service sum + CE MONTHLY forecast used DIRECTLY as the
        # month-end total (NOT mtd+forecast — that double-counted; see
        # fix/expense-aws-forecast-double-count). Forecast 25.00 > MTD 12.34.
        assert rows["aws"]["mtd_cost_usd"] == pytest.approx(12.34)
        assert rows["aws"]["projected_month_end_usd"] == pytest.approx(25.00)
        assert rows["aws"]["detail"]["projection_source"] == "ce_forecast_monthly"
        assert rows["aws"]["pace"] == "under"  # 25.00 < 50 budget
        assert rows["aws"]["detail"]["top_services_usd"]["AmazonEC2"] == pytest.approx(8.10)

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
        fail_ce = FakeCE()
        fail_ce.get_cost_and_usage = lambda **kw: (_ for _ in ()).throw(RuntimeError("ce down"))
        ssm = FakeSSM({index.SSM_GITHUB_TOKEN: "ghp-xxx", index.SSM_NEON: "n"})
        # No budgets fixed rows either → zero ok rows ⇒ systemic failure raises.
        del store["config/expense_budgets.json"]
        monkeypatch.setattr(
            index, "collect_anthropic",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("s3 down")))
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, fail_ce))
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
    def test_reconciles_full_prior_month(self, monkeypatch):
        monkeypatch.setattr(index, "boto3", FakeBoto3(FakeS3({}), FakeSSM({}), FakeCE()))
        pmw = index._prior_month_window(NOW)
        prior_doc = {"providers": [{"key": "aws", "mtd_cost_usd": 10.0,
                                    "projected_month_end_usd": 20.0}]}
        row = index.reconcile_aws(pmw, {}, prior_doc)
        # FakeCE.get_cost_and_usage always returns EC2 8.10 + S3 4.24 = 12.34
        assert row["actual_final"] == pytest.approx(12.34)
        assert row["accrued_mtd_final"] == 10.0
        assert row["delta_usd"] == pytest.approx(2.34)


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
        assert "ending_before=2026-07-01" in seen_urls[0]

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
        store = {f"decision_artifacts/_cost_raw/2026-06-30/run/a.jsonl": cost_jsonl}
        s3 = FakeS3(store)
        prior_doc = {"providers": [{"key": "anthropic_api", "mtd_cost_usd": 4.0,
                                    "projected_month_end_usd": None}]}
        row = index.reconcile_anthropic(pmw, {}, {}, s3, prior_doc)
        assert row["actual_final"] == pytest.approx(5.0)


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
        s3 = FakeS3(store)
        ssm = FakeSSM({})
        # FakeCE always returns 12.34 for get_cost_and_usage → aws delta vs
        # prior_doc's 5.0 accrued is large enough to flag past the threshold.
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, FakeCE()))
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
            raise RuntimeError("CE down")

        monkeypatch.setattr(index, "reconcile_aws", _boom)
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, FakeCE()))
        monkeypatch.setattr(index, "_http_json", http_router({}))
        result = index.run_reconciliation(s3, NOW, {}, {})
        doc = json.loads(s3.store["expenses/reconciliation/2026-06.json"])
        assert doc["providers"]["aws"]["status"] == "error"
        assert "CE down" in doc["providers"]["aws"]["note"]
        assert doc["providers"]["neon"]["status"] == "not_configured"


class TestHandlerReconcileMode:
    def test_handler_mode_reconcile_dispatches(self, monkeypatch):
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        s3 = FakeS3({})
        ssm = FakeSSM({})
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, FakeCE()))
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
