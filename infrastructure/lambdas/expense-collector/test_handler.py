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


class TestNeonPeriodPacing:
    def test_period_aware_projection(self, monkeypatch):
        """Neon's consumption period can start mid-calendar-month (plan
        change) — pacing must use ITS bounds, not the calendar month's."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router({
            "/api/v2/projects/p1": {"project": {
                "name": "nousergon", "data_transfer_bytes": 2_500_000_000,
                "compute_time_seconds": 7200,
                # Half the period elapsed at NOW (7/17 12:00): 2.5 GB → 5 GB
                "consumption_period_start": "2026-07-14T12:00:00Z",
                "consumption_period_end": "2026-07-20T12:00:00Z"}},
            "/api/v2/projects": {"projects": [{"id": "p1"}]},
        }))
        mw = index._month_window(NOW)
        row = index.collect_neon(mw, {}, {index.SSM_NEON: "k",
                                          index.SSM_NEON_QUOTA_GB: "5"})
        assert row["quota"]["used"] == pytest.approx(2.5)
        assert row["quota"]["projected"] == pytest.approx(5.0)
        assert row["pace"] is None or row["pace"] == "under"  # 5.0 !> 5 GB

    def test_operator_note_and_fixed_cost_surface(self, monkeypatch):
        """A budgets note + fixed_monthly_usd (e.g. temporary paid plan) must
        reach the row — the fixed-cost branch used to blank the note."""
        monkeypatch.setattr(index, "_now_utc", lambda: NOW)
        monkeypatch.setattr(index, "_http_json", http_router({
            "/api/v2/projects/p1": {"project": {"name": "nousergon",
                "data_transfer_bytes": 8_000_000,
                "consumption_period_start": "2026-07-01T00:00:00Z",
                "consumption_period_end": "2026-08-01T00:00:00Z"}},
            "/api/v2/projects": {"projects": [{"id": "p1"}]},
        }))
        budgets = {"providers": {"neon": {
            "fixed_monthly_usd": 19.0, "note": "Launch plan — TEMPORARY"}}}
        row = index.collect_neon(index._month_window(NOW), budgets,
                                 {index.SSM_NEON: "k"})
        assert row["mtd_cost_usd"] == 19.0
        assert row["projected_month_end_usd"] == 19.0
        assert row["note"] == "Launch plan — TEMPORARY"


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
        "/api/v2/projects/p1": {"project": {
            "name": "nousergon", "data_transfer_bytes": 3_000_000_000,
            "compute_time_seconds": 7200,
            "consumption_period_start": "2026-07-01T00:00:00Z",
            "consumption_period_end": "2026-08-01T00:00:00Z"}},
        "/api/v2/projects": {"projects": [{"id": "p1"}]},
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
