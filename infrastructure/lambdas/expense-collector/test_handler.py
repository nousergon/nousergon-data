"""Unit tests for the expense-collector handler.

Pure-logic coverage (month window, forward-only projection, diff rows, fixed
rows, Neon metric walker) plus a full handler run against fake boto3 clients
and a canned HTTP router — asserting the rollup artifact shape, per-provider
rows, error fencing (one dead provider must not blank the others), and the
first-writer-wins baseline/snapshot writes.

Run standalone: ``python3 -m pytest test_handler.py -q`` (deploy.sh preflights
this before every package+ship; only dep beyond stdlib is boto3, which the
Lambda runtime provides in prod).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
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
        return {"Total": {"Amount": "10.00"}}


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

    def test_no_baseline_establishes(self):
        row = index._diff_row(index._row("openrouter", "OpenRouter"), self.MW,
                              self.BUDGETS, "openrouter", 42.5, {}, "openrouter_total_usage")
        assert row["mtd_cost_usd"] == 0.0
        assert "baseline established" in row["note"]

    def test_diff_and_pace(self):
        baseline = {"counters": {"openrouter_total_usage": 40.0},
                    "as_of": {"openrouter_total_usage": "2026-07-01T00:10:00+00:00"}}
        row = index._diff_row(index._row("openrouter", "OpenRouter"), self.MW,
                              self.BUDGETS, "openrouter", 42.5, baseline,
                              "openrouter_total_usage")
        assert row["mtd_cost_usd"] == pytest.approx(2.5)
        # 2.5 over ~53% of month → ~4.7 projected > 4.0 budget
        assert row["pace"] == "over"

    def test_negative_diff_clamped(self):
        baseline = {"counters": {"deepseek_neg_balance": -10.0},
                    "as_of": {"deepseek_neg_balance": "2026-07-01T00:10:00+00:00"}}
        row = index._diff_row(index._row("deepseek", "DeepSeek"), self.MW, {},
                              "deepseek", -12.0, baseline, "deepseek_neg_balance")
        assert row["mtd_cost_usd"] == 0.0  # top-up mid-month, never negative


class TestNeonWalker:
    def test_sums_nested_metrics(self):
        doc = {"periods": [{"consumption": [
            {"data_transfer_bytes": 1_000_000_000, "compute_time_seconds": 3600},
            {"data_transfer_bytes": 2_000_000_000, "written_data_bytes": 5},
        ]}]}
        sums: dict[str, float] = {}
        index._sum_metrics(doc, sums)
        assert sums["data_transfer_bytes"] == 3_000_000_000
        assert sums["compute_time_seconds"] == 3600


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
        index.SSM_GITHUB_PAT: "ghp-xxx",
        # no ANTHROPIC_ADMIN_KEY → client-telemetry fallback path
    })
    monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, FakeCE()))
    monkeypatch.setattr(index, "_now_utc", lambda: NOW)
    monkeypatch.setattr(index, "_http_json", http_router({
        "openrouter.ai/api/v1/credits": {
            "data": {"total_credits": 50.0, "total_usage": 42.5}},
        "api.deepseek.com/user/balance": {
            "balance_infos": [{"currency": "USD", "total_balance": "15.00"}]},
        "console.neon.tech": {"periods": [{"consumption": [
            {"data_transfer_bytes": 3_000_000_000, "compute_time_seconds": 7200}]}]},
        "organizations/nousergon/settings/billing/usage": {"usageItems": [
            {"product": "Actions", "unitType": "Minutes", "quantity": 1400,
             "netAmount": 0.0},
            {"product": "Packages", "unitType": "GigabyteHours", "quantity": 10,
             "netAmount": 1.5},
        ]},
        "users/cipher813/settings/billing/usage": RuntimeError(
            "HTTP 403 from github: PAT lacks Plan scope"),
        # legacy included-minutes probes: gone on the enhanced billing platform
        "orgs/nousergon/settings/billing/actions": RuntimeError("HTTP 410"),
        "users/cipher813/settings/billing/actions": RuntimeError("HTTP 410"),
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

        # AWS: grouped-service sum + CE forecast projection
        assert rows["aws"]["mtd_cost_usd"] == pytest.approx(12.34)
        assert rows["aws"]["projected_month_end_usd"] == pytest.approx(22.34)
        assert rows["aws"]["pace"] == "under"  # 22.34 < 50 budget
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

        # GitHub org: minutes quota pacing (1400 @ 53% → ~2630 > 2000)
        gh = rows["github_org"]
        assert gh["quota"]["used"] == 1400
        assert gh["pace"] == "over"
        assert gh["mtd_cost_usd"] == pytest.approx(1.5)

        # GitHub user: fenced error — recorded on the row, run continues
        assert rows["github_user"]["status"] == "error"
        assert "403" in rows["github_user"]["error"]

        # Fixed row from budgets config
        assert rows["claude_max"]["status"] == "fixed"
        assert rows["claude_max"]["mtd_cost_usd"] == 200.0

        # Totals: ok+fixed rows only; error row flags incomplete
        assert doc["totals"]["incomplete"] is True
        expected_mtd = 12.34 + 2.0 + 2.5 + 5.0 + 0.0 + 1.5 + 200.0
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

    def test_all_providers_failing_raises(self, env, monkeypatch):
        s3, store = env
        monkeypatch.setattr(index, "_http_json",
                            http_router({}))  # every HTTP call unrouted → raises
        fail_ce = FakeCE()
        fail_ce.get_cost_and_usage = lambda **kw: (_ for _ in ()).throw(RuntimeError("ce down"))
        ssm = FakeSSM({index.SSM_GITHUB_PAT: "ghp-xxx", index.SSM_NEON: "n"})
        # No budgets fixed rows either → zero ok rows ⇒ systemic failure raises.
        del store["config/expense_budgets.json"]
        monkeypatch.setattr(
            index, "collect_anthropic",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("s3 down")))
        monkeypatch.setattr(index, "boto3", FakeBoto3(s3, ssm, fail_ce))
        with pytest.raises(RuntimeError, match="all provider adapters failed"):
            index.handler({}, None)
