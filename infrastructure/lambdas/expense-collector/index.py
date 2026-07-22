"""alpha-engine-expense-collector — one normalized monthly spend/quota rollup
for EVERY external service Nous Ergon pays for (or draws quota from), consumed
by the console's Expenses page (alpha-engine-dashboard ``views/50_Expenses.py``).

Providers collected per run (adapter registry, one row each):

  - **aws**            Cost Explorer month-to-date unblended cost + CE forecast
                       (+ top-services breakdown). CE bills $0.01/request — the
                       2-runs/day cadence costs ~$1.2/mo, which shows up in its
                       own row.
  - **anthropic_api**  Admin API ``/v1/organizations/cost_report`` when
                       ``/alpha-engine/expenses/ANTHROPIC_ADMIN_KEY`` exists;
                       until then falls back to summing the research fleet's
                       client-side per-call telemetry
                       (``decision_artifacts/_cost_raw/{date}/**.jsonl``,
                       producer: crucible-research ``graph/llm_cost_tracker.py``)
                       — fleet-only, excludes morning-signal et al (noted on the
                       row).
  - **openrouter**     ``/api/v1/credits`` lifetime-usage counter, diffed
                       against a first-run-of-month baseline
                       (``expenses/baselines/{YYYY-MM}.json``).
  - **deepseek**       ``/user/balance`` prepaid balance, diffed the same way
                       (top-ups make the diff conservative; noted on the row).
  - **neon**           ``/api/v2/consumption_history/account`` — data-transfer
                       GB vs the ``/alpha-engine/NEON_DATA_TRANSFER_QUOTA_GB``
                       quota (free plan ⇒ $0 unless budgets say otherwise).
  - **github (org+user)** enhanced billing usage endpoint per account
                       (nousergon org + cipher813 user): billed $ across all
                       products + Actions minutes vs included quota (legacy
                       ``settings/billing/actions`` probed for the included-
                       minutes figure where still served).
  - **fixed rows**     any provider in the budgets SSoT with
                       ``fixed_monthly_usd`` and no live adapter (Claude Max
                       subscription today; future flat subscriptions are
                       config-only additions).

Budgets/quotas SSoT: ``s3://alpha-engine-research/config/expense_budgets.json``
(operator-edited; seeded from ``expense_budgets.seed.json`` next to this file).

Outputs (bucket ``alpha-engine-research``):
  - ``expenses/monthly/{YYYY-MM}.json``  — the period rollup (overwritten,
    latest ``as_of`` wins; prior months accumulate for history)
  - ``expenses/latest.json``             — same doc, stable key for the console
  - ``expenses/baselines/{YYYY-MM}.json``— month-start counter baseline
    (first-writer-wins, backfilled per-counter as new providers appear)
  - ``expenses/snapshots/{date}.json``   — first reading of each day's raw
    counters (future trend charts; conditional PUT, first-writer-wins)
  - ``expenses/reconciliation/{YYYY-MM}.json`` — month-close true-up
    (alpha-engine-config#2849): once the just-closed month's providers have
    finalized, each provider's FINAL actual is re-queried and compared to what
    was last projected/accrued for that month. Producer: this Lambda's
    ``reconcile`` mode (see below). Consumer: crucible-dashboard's Expenses
    page "Reconciliation (prior months)" section.

Two invocation modes, dispatched off ``event["mode"]`` (default
``"collect"``) — the established pattern for this Lambda's own scheduling
(one function, two EventBridge Scheduler rules with different ``Input``; see
``deploy.sh``'s ``SCHED_NAMES``/``SCHED_CRONS``/``SCHED_INPUTS``):
  - ``"collect"`` (default) — the twice-daily live MTD rollup described above.
  - ``"reconcile"`` — month-close true-up, scheduled ~03:00 UTC on the 2nd of
    each month (after providers finalize the prior month). Re-queries each
    provider's adapter with a CLOSED prior-month window (never re-derives a
    provider's own arithmetic — same principle as the AWS forecast fix:
    anchor to what the provider reports) and diffs against the prior month's
    already-recorded ``expenses/monthly/{YYYY-MM}.json`` rollup.

Failure posture (CLAUDE.md no-silent-fails): each provider adapter is
independently fenced — a single provider API outage must not blank the other
rows, so an adapter exception is RECORDED on that row (``status="error"`` +
message, rendered red by the console page and logged with traceback here)
rather than propagated. The handler still RAISES if the budgets SSoT and every
adapter fail together, or if the artifact write fails — a rollup that can't
record anything is a dead check and must trip the Lambda error metric.

Over-budget Telegram alert (alpha-engine-config#2843): after the rollup write,
any provider row whose ``pace`` flips to ``"over"`` gets a RISING-EDGE Telegram
ping via flow-doctor's OPS_HEALTH topic — once per provider per calendar
month, re-arming if the row drops back under budget/quota and later re-crosses.
State lives at ``expenses/alert_state/{YYYY-MM}.json`` (provider key → last-
observed breached flag), mirroring the (now-retired) usage-pace-alert Lambda's
rising-edge mechanism (git history @ nousergon-data#831~1:
infrastructure/lambdas/usage-pace-alert/index.py). This is a NOTIFICATION
ONLY — usage-pacing ENFORCEMENT (predictive gates) was dismantled fleet-wide
2026-07-14 (Brian ruling) and must never be re-added here. It is also fully
distinct from the separate Claude-Max WET pace alert that Lambda used to send
(different Lambda, different data source) — no duplication.
"""

from __future__ import annotations

import calendar
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

from flow_doctor_telegram import notify_via_flow_doctor
from nousergon_lib.flow_doctor_fleet import FleetTelegramTopic

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("EXPENSE_BUCKET", "alpha-engine-research")

BUDGETS_KEY = os.environ.get("EXPENSE_BUDGETS_KEY", "config/expense_budgets.json")
MONTHLY_PREFIX = "expenses/monthly/"
LATEST_KEY = "expenses/latest.json"
BASELINE_PREFIX = "expenses/baselines/"
SNAPSHOT_PREFIX = "expenses/snapshots/"
COST_RAW_PREFIX = "decision_artifacts/_cost_raw/"
ALERT_STATE_PREFIX = "expenses/alert_state/"
RECONCILIATION_PREFIX = "expenses/reconciliation/"

# Month-close reconciliation (alpha-engine-config#2849) — |delta_pct| beyond
# this is "visible drift", not rounding/timing noise. Matches the console's
# highlight threshold in shared/expense_view.py (kept in lockstep — a
# reconciliation row this Lambda flags but the console renders unhighlighted,
# or vice versa, would be worse than either being wrong alone).
RECONCILIATION_DELTA_PCT_THRESHOLD = 0.08

# Over-budget Telegram alert (config#2843) — notification only, OPS_HEALTH
# topic (this is an advisory pace signal, not a critical incident; unlike the
# retired usage-pace-alert's OVER tier it does not also fan out to CRITICAL).
_FLOW_NAME = "expense-collector"
_DB_BASENAME = "flow_doctor_expense_collector"
_ALERT_TOPICS = (FleetTelegramTopic.OPS_HEALTH,)

# SSM parameter names (batch-read; absent ones degrade that row to
# status="not_configured", never the whole run).
SSM_OPENROUTER = "/alpha-engine/OPENROUTER_API_KEY"
SSM_DEEPSEEK = "/symposion/DEEPSEEK_API_KEY"
SSM_NEON = "/alpha-engine/NEON_API_KEY"
SSM_NEON_QUOTA_GB = "/alpha-engine/NEON_DATA_TRANSFER_QUOTA_GB"
# Verified 2026-07-17: of the fleet's six GitHub tokens, ONLY this one can read
# the enhanced org billing-usage endpoint (the groom/config-runner/etc PATs all
# 403). No existing token can see the cipher813 PERSONAL account's billing —
# that needs a user-scoped fine-grained PAT with "Plan: read", wired below.
SSM_GITHUB_TOKEN = "/alpha-engine/GITHUB_TOKEN"
SSM_GITHUB_USER_PAT = "/alpha-engine/expenses/GITHUB_USER_BILLING_PAT"
SSM_ANTHROPIC_ADMIN = "/alpha-engine/expenses/ANTHROPIC_ADMIN_KEY"
SSM_PARAMS = [SSM_OPENROUTER, SSM_DEEPSEEK, SSM_NEON, SSM_NEON_QUOTA_GB,
              SSM_GITHUB_TOKEN, SSM_GITHUB_USER_PAT, SSM_ANTHROPIC_ADMIN]

GITHUB_ORG = "nousergon"
GITHUB_USER = "cipher813"

HTTP_TIMEOUT = 25
# Don't call a projection before ~1 day of month has elapsed — a 2h-old month
# extrapolates garbage.
MIN_PROJECTION_FRAC = 0.03


# ---------------------------------------------------------------------------
# Small primitives
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _http_json(url: str, headers: dict | None = None) -> dict:
    """GET ``url`` and parse JSON. Raises RuntimeError with a body snippet on
    any non-2xx / parse failure — adapter fences turn that into a row error."""
    req = urllib.request.Request(url, headers={"User-Agent": "alpha-engine-expense-collector",
                                               **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        snippet = exc.read()[:300].decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {snippet}") from exc


def _month_window(now: datetime) -> dict:
    """Calendar-month (UTC) window: period id, start, length, elapsed fraction.
    All the providers here bill on calendar months (UTC or close enough that a
    sub-day skew doesn't move an over/under-pace call)."""
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days = calendar.monthrange(now.year, now.month)[1]
    total_s = days * 86400.0
    return {
        "period": now.strftime("%Y-%m"),
        "start": start,
        "total_seconds": total_s,
        "elapsed_frac": min(max((now - start).total_seconds() / total_s, 0.0), 1.0),
    }


def _project(mtd: float, elapsed_frac: float, observed_frac: float | None = None) -> float | None:
    """Straight-line month-end projection. ``observed_frac`` is the fraction of
    the month the measurement actually covers (< elapsed_frac when a baseline
    was established mid-month) — extrapolation runs FORWARD only, so a partial
    baseline never fabricates early-month spend."""
    obs = elapsed_frac if observed_frac is None else observed_frac
    if obs < MIN_PROJECTION_FRAC:
        return None
    return mtd + (mtd / obs) * (1.0 - elapsed_frac)


def _pace(projected: float | None, limit: float | None) -> str | None:
    if projected is None or limit is None:
        return None
    return "over" if projected > limit else "under"


def _row(key: str, label: str, **kw) -> dict:
    base = {
        "key": key, "label": label, "status": "ok",
        "mtd_cost_usd": None, "projected_month_end_usd": None,
        "budget_usd": None, "pace": None, "quota": None,
        "source": None, "detail": {}, "note": None, "error": None,
    }
    base.update(kw)
    return base


def _finish_usd_row(row: dict, mw: dict, budget_usd: float | None,
                    observed_frac: float | None = None) -> dict:
    """Stamp projection + pace onto a $-denominated row (in place)."""
    row["budget_usd"] = budget_usd
    if row["mtd_cost_usd"] is not None and row["projected_month_end_usd"] is None:
        row["projected_month_end_usd"] = _project(row["mtd_cost_usd"], mw["elapsed_frac"],
                                                  observed_frac)
    row["pace"] = _pace(row["projected_month_end_usd"], budget_usd)
    return row


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_json(s3, key: str) -> dict | None:
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence is an expected state here;
        # callers decide whether a missing doc is fatal (budgets → warning list).
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404"}:
            logger.warning("S3 read %s failed: %s", key, exc)
        return None


def _put_json(s3, key: str, doc: dict, if_none_match: bool = False) -> bool:
    kwargs = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json",
              "Body": json.dumps(doc, indent=2, default=str).encode()}
    if if_none_match:
        kwargs["IfNoneMatch"] = "*"
    try:
        s3.put_object(**kwargs)
        return True
    except Exception as exc:  # noqa: BLE001 — 412 = another writer won the
        # first-writer-wins race for a baseline/snapshot; that is the designed
        # outcome, not a failure. Anything else re-raises.
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if if_none_match and code in {"PreconditionFailed", "412"}:
            return False
        raise


# ---------------------------------------------------------------------------
# Over-budget Telegram alert (config#2843) — rising-edge state per provider,
# per calendar month. Mirrors the retired usage-pace-alert Lambda's
# _load_state/_save_state/notify_via_flow_doctor mechanism (git history @
# nousergon-data#831~1) rather than reimplementing it.
# ---------------------------------------------------------------------------

def _alert_state_key(period: str) -> str:
    return f"{ALERT_STATE_PREFIX}{period}.json"


def _load_alert_state(s3, period: str) -> dict:
    """Prior per-provider breached flags for THIS calendar-month period;
    ``{}`` (nothing armed) if absent, unreadable, or malformed — so a fresh
    month naturally starts with every provider re-armed. Best-effort: a read
    failure is treated as "no prior state" (worst case: one duplicate alert),
    never raises (this is dedup bookkeeping, not the primary breach signal —
    the pace itself already comes from the rollup written above)."""
    key = _alert_state_key(period)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
    except Exception as exc:  # noqa: BLE001 — absence expected; bad blob recoverable
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"NoSuchKey", "404", "403"}:
            logger.warning("could not read expense alert state %s: %s", key, exc)
        return {}
    if not isinstance(data, dict) or not isinstance(data.get("providers"), dict):
        return {}
    return data


def _save_alert_state(s3, period: str, breached: dict[str, bool]) -> None:
    """Persist this run's observed per-provider breached flags. Best-effort: a
    write failure only risks a duplicate alert next run (logged), never a
    missed breach (the breach itself already surfaced via Telegram)."""
    key = _alert_state_key(period)
    try:
        s3.put_object(
            Bucket=BUCKET, Key=key, ContentType="application/json",
            Body=json.dumps({"period": period, "providers": breached}, indent=2).encode(),
        )
    except Exception as exc:  # noqa: BLE001 — dedup state; failure only risks a dup ping
        logger.warning("could not persist expense alert state %s: %s", key, exc)


def _alert_over_budget(row: dict, period: str) -> bool:
    """Telegram ping for a provider row that just RISING-EDGE flipped to
    ``pace == "over"``. Delivery-surface failure is logged, never raised —
    the breach itself is already recorded in the rollup artifact."""
    text = (
        "\U0001f6a8 *Expense pacing — over budget/quota*\n"
        f"*{row['label']}* ({period}) projected to end the month over its "
        "budget/quota.\n"
        f"MTD ${row['mtd_cost_usd']:,.2f}"
        + (f" → projected ${row['projected_month_end_usd']:,.2f}"
           if row.get("projected_month_end_usd") is not None else "")
        + (f" (budget ${row['budget_usd']:,.2f})" if row.get("budget_usd") is not None else "")
        + "\n_See the Expenses page in the console for detail._"
    )
    dedup_key = f"{_FLOW_NAME}:{row['key']}:{period}"
    try:
        return notify_via_flow_doctor(
            text, silent=False, severity="warning", dedup_key=dedup_key,
            flow_name=_FLOW_NAME, topics=_ALERT_TOPICS, db_basename=_DB_BASENAME,
            context={"provider": row["key"], "period": period, "pace": row["pace"]},
        )
    except Exception as exc:  # noqa: BLE001 — delivery surface only
        logger.warning("over-budget alert Telegram send failed (non-fatal): %s", exc)
        return False


def run_over_budget_alerts(s3, period: str, rows: list[dict]) -> dict:
    """Rising-edge Telegram alert pass over this run's rows. Fires once per
    provider per calendar month on the FIRST flip to ``pace == "over"``; stays
    quiet on sustained breach; re-arms (allows a new ping) the moment a row
    drops back to any non-"over" pace, so a later re-breach in the same month
    alerts again. Never raises — this is a notification-only enhancement
    layered after the rollup write already succeeded; a bug here must not cost
    the rollup itself (config#2843 scope: notification only, no enforcement)."""
    try:
        prev = _load_alert_state(s3, period).get("providers", {})
        new_state: dict[str, bool] = {}
        alerted: list[str] = []
        for row in rows:
            key = row["key"]
            breached = row.get("pace") == "over"
            new_state[key] = breached
            if breached and not prev.get(key, False):
                if _alert_over_budget(row, period):
                    alerted.append(key)
        _save_alert_state(s3, period, new_state)
        return {"alerted": alerted}
    except Exception as exc:  # noqa: BLE001 — notification-only; never masks the rollup
        logger.warning("over-budget alert pass failed (non-fatal, rollup unaffected): %s", exc)
        return {"alerted": [], "error": str(exc)[:300]}


def _load_ssm(names: list[str]) -> dict[str, str]:
    """Batch-read SSM params; missing names are simply absent from the result
    (each consumer then reports its own row as not_configured)."""
    ssm = boto3.client("ssm", region_name=REGION)
    out: dict[str, str] = {}
    for i in range(0, len(names), 10):
        resp = ssm.get_parameters(Names=names[i:i + 10], WithDecryption=True)
        for p in resp.get("Parameters", []):
            out[p["Name"]] = p["Value"]
        if resp.get("InvalidParameters"):
            logger.info("SSM params not present (rows degrade to not_configured): %s",
                        resp["InvalidParameters"])
    return out


# ---------------------------------------------------------------------------
# Provider adapters
# ---------------------------------------------------------------------------

def _ce_unblended_by_service(ce, start: str, end: str) -> dict[str, float]:
    """Shared Cost Explorer ``get_cost_and_usage`` call (grouped by SERVICE,
    MONTHLY granularity) — used both for the live current-month MTD read
    (``collect_aws``) and the closed-prior-month reconciliation re-query
    (``reconcile_aws``), so the two never drift on how a service total is
    summed."""
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end}, Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    by_service: dict[str, float] = {}
    for period in resp.get("ResultsByTime", []):
        for g in period.get("Groups", []):
            svc = g["Keys"][0]
            by_service[svc] = by_service.get(svc, 0.0) + float(
                g["Metrics"]["UnblendedCost"]["Amount"])
    return by_service


def collect_aws(mw: dict, budgets: dict) -> dict:
    ce = boto3.client("ce", region_name="us-east-1")
    start = mw["start"].strftime("%Y-%m-%d")
    now = _now_utc()
    end = (now.replace(hour=0, minute=0, second=0, microsecond=0)
           .strftime("%Y-%m-%d"))
    next_month = (mw["start"].replace(day=28) + timedelta(days=4)).replace(day=1)
    row = _row("aws", "AWS", source="cost_explorer")
    if end <= start:  # first UTC day of the month — CE window would be empty
        row.update(mtd_cost_usd=0.0, note="month just started — Cost Explorer window empty")
        return _finish_usd_row(row, mw, _budget_usd(budgets, "aws"))
    by_service = _ce_unblended_by_service(ce, start, end)
    mtd = round(sum(by_service.values()), 2)
    top = dict(sorted(by_service.items(), key=lambda kv: -kv[1])[:8])
    row.update(mtd_cost_usd=mtd,
               detail={"top_services_usd": {k: round(v, 2) for k, v in top.items()}},
               note="Cost Explorer data lags ~24h")
    try:
        # CE's MONTHLY-granularity forecast over a partial-month window returns
        # the FULL month-end total (already includes actuals-to-date) — the
        # exact figure AWS's own console "forecasted month-end" tile shows. Use
        # it DIRECTLY; adding MTD double-counts spend-to-date (that bug made a
        # $103.25 month-end read as $152.14). The DAILY-granularity forecast
        # would instead return the remainder-only, but MONTHLY is what matches
        # the console, so we anchor to the provider-authoritative number.
        fc = ce.get_cost_forecast(
            TimePeriod={"Start": end, "End": next_month.strftime("%Y-%m-%d")},
            Metric="UNBLENDED_COST", Granularity="MONTHLY")
        row["projected_month_end_usd"] = round(float(fc["Total"]["Amount"]), 2)
        row["detail"]["projection_source"] = "ce_forecast_monthly"
    except Exception as exc:  # noqa: BLE001 — forecast is an enhancement; the
        # straight-line fallback below is the recorded degradation surface.
        logger.info("CE forecast unavailable (straight-line fallback): %s", exc)
        row["detail"]["projection_source"] = "straight_line"
    return _finish_usd_row(row, mw, _budget_usd(budgets, "aws"))


def collect_anthropic(mw: dict, budgets: dict, secrets: dict, s3, *,
                      end: datetime | None = None, last_day: int | None = None) -> dict:
    """``end``/``last_day`` default to the live current-month read (open-ended
    Admin API window; fallback loop runs through "today"). Reconciliation
    (``reconcile_anthropic``) passes both bounded to the CLOSED prior month so
    this same adapter — same parsing, same fallback — re-queries a finalized
    window instead of duplicating either code path."""
    row = _row("anthropic_api", "Anthropic API")
    admin_key = secrets.get(SSM_ANTHROPIC_ADMIN)
    if admin_key:
        starting = mw["start"].strftime("%Y-%m-%dT00:00:00Z")
        url = ("https://api.anthropic.com/v1/organizations/cost_report"
               f"?starting_at={starting}&limit=31")
        if end is not None:
            url += f"&ending_before={end.strftime('%Y-%m-%dT00:00:00Z')}"
        headers = {"x-api-key": admin_key, "anthropic-version": "2023-06-01"}
        total, page, pages = 0.0, None, 0
        while pages < 10:
            doc = _http_json(url + (f"&page={page}" if page else ""), headers)
            for bucket in doc.get("data", []):
                for res in bucket.get("results", []):
                    # cost_report `amount` is in CENTS (currency minor units),
                    # not dollars — verified live 2026-07-20 against list
                    # prices: 9.8M Haiku input tokens reported amount=981.60
                    # (= $9.82 at $1/MTok), uniformly 100x across every line
                    # item. Summing as USD overstated the row 100x
                    # (alpha-engine-config-I2840).
                    total += float(res.get("amount", 0) or 0) / 100.0
            pages += 1
            if not doc.get("has_more"):
                break
            page = doc.get("next_page")
        row.update(mtd_cost_usd=round(total, 2), source="admin_api")
        return _finish_usd_row(row, mw, _budget_usd(budgets, "anthropic_api"))
    # Fallback: research-fleet client telemetry (per-call JSONL, cost_usd per row).
    total, n_days = 0.0, 0
    last_day = last_day if last_day is not None else _now_utc().day
    paginator = s3.get_paginator("list_objects_v2")
    for day in range(1, last_day + 1):
        date_str = f"{mw['period']}-{day:02d}"
        for page_ in paginator.paginate(Bucket=BUCKET,
                                        Prefix=f"{COST_RAW_PREFIX}{date_str}/"):
            for obj in page_.get("Contents", []):
                n_days += 1
                body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
                for line in body.splitlines():
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    total += float(rec.get("cost_usd") or 0)
    row.update(
        mtd_cost_usd=round(total, 2), source="client_telemetry",
        note=("research-fleet client telemetry only (excludes morning-signal "
              "and other API consumers) — add SSM "
              f"{SSM_ANTHROPIC_ADMIN} for authoritative org-wide Admin-API costs"),
        detail={"cost_raw_objects": n_days},
    )
    return _finish_usd_row(row, mw, _budget_usd(budgets, "anthropic_api"))


def collect_openrouter(mw: dict, budgets: dict, secrets: dict,
                       counters: dict, baseline: dict) -> dict:
    row = _row("openrouter", "OpenRouter", source="credits_diff")
    if SSM_OPENROUTER not in secrets:
        row.update(status="not_configured", error=f"SSM {SSM_OPENROUTER} missing")
        return row
    usage_now = counters["openrouter_total_usage"]
    row["detail"] = {"lifetime_usage_usd": round(usage_now, 4),
                     "credits_remaining_usd": round(counters["openrouter_credits_remaining"], 4)}
    return _diff_row(row, mw, budgets, "openrouter", usage_now, baseline,
                     "openrouter_total_usage")


def collect_deepseek(mw: dict, budgets: dict, secrets: dict,
                     counters: dict, baseline: dict) -> dict:
    row = _row("deepseek", "DeepSeek", source="balance_diff")
    if SSM_DEEPSEEK not in secrets:
        row.update(status="not_configured", error=f"SSM {SSM_DEEPSEEK} missing")
        return row
    bal = counters["deepseek_balance"]
    row["detail"] = {"balance": round(bal, 4), "currency": counters["deepseek_currency"]}
    # Spend counter = -balance (balance falls as credit is consumed); a top-up
    # mid-month shows as negative spend — clamped to 0 with a note, since the
    # collector cannot see top-up events.
    row = _diff_row(row, mw, budgets, "deepseek", -bal, baseline, "deepseek_neg_balance")
    if row.get("mtd_cost_usd") == 0.0 and row["status"] == "ok":
        row["note"] = (row.get("note") or "") + " (top-ups mid-month can mask spend)"
    return row


def _diff_row(row: dict, mw: dict, budgets: dict, key: str, counter_now: float,
              baseline: dict, counter_key: str) -> dict:
    base = baseline.get("counters", {}).get(counter_key)
    base_ts = baseline.get("as_of", {}).get(counter_key)
    if base is None:
        row.update(mtd_cost_usd=0.0,
                   note="baseline established this run — MTD accrues from today")
        return _finish_usd_row(row, mw, _budget_usd(budgets, key))
    mtd = round(max(counter_now - float(base), 0.0), 4)
    observed = mw["elapsed_frac"]
    if base_ts:
        base_dt = datetime.fromisoformat(base_ts)
        observed = max((_now_utc() - base_dt).total_seconds() / mw["total_seconds"], 0.0)
        if (base_dt - mw["start"]).total_seconds() > 3600:
            row["note"] = f"measured since {base_dt:%Y-%m-%d} baseline (mid-month start)"
    row.update(mtd_cost_usd=mtd)
    return _finish_usd_row(row, mw, _budget_usd(budgets, key), observed_frac=observed)


NEON_TRANSFER_FREE_GB = 500.0
NEON_TRANSFER_OVERAGE_USD_PER_GB = 0.10
# Neon Launch plan pricing (neon.com/pricing, verified live 2026-07-17): no
# flat monthly fee ("pay for what you use, no monthly minimum"). Compute
# $0.106/CU-hour and storage $0.35/GB-month are BOTH left unknown below — the
# per-project detail object (the only endpoint this free/Launch-plan API key
# can reach; ``consumption_history`` is Scale-plan-only, 403/404 confirmed
# live) exposes ``compute_time_seconds`` (wall-clock active-compute seconds,
# NOT CU-scaled — no CU-size/vCPU field is returned anywhere in this response
# to convert it into CU-hours) and ``written_data_bytes`` (a cumulative WRITE
# counter, not a point-in-time storage size — no ``logical_size_bytes`` or
# equivalent current-size field is present either). Fabricating a $ figure
# from either would be a guess dressed up as a bill; both render `None` with
# the gap named in ``detail.cost_components_unavailable`` instead.
NEON_COMPUTE_USD_PER_CU_HOUR = 0.106  # documented for when a CU-size field
NEON_STORAGE_USD_PER_GB_MONTH = 0.35  # ever becomes available; unused today.


def collect_neon(mw: dict, budgets: dict, secrets: dict) -> dict:
    """Free-tier-compatible consumption read: the ``consumption_history``
    endpoints are Scale-plan-only (verified 403/404 live, 2026-07-17), but the
    per-project detail object carries current-consumption-period counters
    (``data_transfer_bytes``, ``compute_time_seconds``, …) plus the period
    bounds on every plan — sum across projects and pace against the period.
    Only the data-transfer line is actually computable in $ from those
    counters (see ``NEON_COMPUTE_USD_PER_CU_HOUR`` docstring above for why
    compute + storage are NOT): a real partial bill, not a fabricated flat
    fee, unless the operator sets an explicit ``fixed_monthly_usd`` override
    (e.g. a manually-confirmed paid-plan invoice) in the budgets SSoT."""
    row = _row("neon", "Neon Postgres", source="projects_api")
    if SSM_NEON not in secrets:
        row.update(status="not_configured", error=f"SSM {SSM_NEON} missing")
        return row
    headers = {"Authorization": f"Bearer {secrets[SSM_NEON]}", "Accept": "application/json"}
    listing = _http_json("https://console.neon.tech/api/v2/projects", headers)
    sums: dict[str, float] = {}
    period_start = period_end = None
    project_names = []
    for proj in listing.get("projects", []):
        detail = _http_json(
            f"https://console.neon.tech/api/v2/projects/{proj['id']}", headers)
        p = detail.get("project", {})
        project_names.append(p.get("name", proj["id"]))
        for k in ("data_transfer_bytes", "compute_time_seconds",
                  "active_time_seconds", "written_data_bytes"):
            if isinstance(p.get(k), (int, float)):
                sums[k] = sums.get(k, 0.0) + float(p[k])
        period_start = period_start or p.get("consumption_period_start")
        period_end = period_end or p.get("consumption_period_end")
    transfer_gb = sums.get("data_transfer_bytes", 0.0) / 1e9
    quota_gb = float(secrets.get(SSM_NEON_QUOTA_GB, 0) or 0) or None
    # Pace against Neon's OWN consumption period when reported (it can start
    # mid-calendar-month, e.g. after a plan change), else the calendar month.
    observed_frac = mw["elapsed_frac"]
    if period_start and period_end:
        try:
            ps = datetime.fromisoformat(period_start.replace("Z", "+00:00"))
            pe = datetime.fromisoformat(period_end.replace("Z", "+00:00"))
            total = (pe - ps).total_seconds()
            if total > 0:
                observed_frac = max((_now_utc() - ps).total_seconds() / total, 0.0)
        except ValueError:
            logger.warning("unparseable Neon consumption period: %s → %s",
                           period_start, period_end)
    projected_gb = None
    if observed_frac >= MIN_PROJECTION_FRAC:
        projected_gb = transfer_gb / observed_frac

    # Data transfer overage is the ONE line item this endpoint can actually
    # price: 500 GB/mo free egress, then $0.10/GB (neon.com/pricing, verified
    # live 2026-07-17). Compute and storage are genuinely uncomputable from
    # the fields this API key can reach — see the module-level constants'
    # docstring — and MUST surface as unknown, not silently dropped or
    # counted as $0.
    transfer_mtd_usd = max(0.0, transfer_gb - NEON_TRANSFER_FREE_GB) * NEON_TRANSFER_OVERAGE_USD_PER_GB
    transfer_projected_usd = None
    if observed_frac >= MIN_PROJECTION_FRAC:
        transfer_projected_usd = (max(0.0, (transfer_gb / observed_frac) - NEON_TRANSFER_FREE_GB)
                                   * NEON_TRANSFER_OVERAGE_USD_PER_GB)
    cost_components_unavailable = [
        {"component": "compute", "unit": "CU-hour", "reason":
         "compute_time_seconds is wall-clock active-compute time, not "
         "CU-scaled — the project detail API returns no CU-size/vCPU field "
         "to convert it into billable CU-hours"},
        {"component": "storage", "unit": "GB-month", "reason":
         "written_data_bytes is a cumulative write counter, not a "
         "point-in-time storage size — the project detail API returns no "
         "logical_size_bytes (or equivalent current-size) field"},
    ]

    fixed_override = _fixed_usd(budgets, "neon")
    if fixed_override is not None:
        # Explicit operator override (e.g. a manually-confirmed paid-plan
        # invoice) — takes precedence over the computed estimate below, same
        # as every other provider's fixed_monthly_usd escape hatch.
        mtd_cost_usd = float(fixed_override)
        projected_month_end_usd = float(fixed_override)
    else:
        mtd_cost_usd = round(transfer_mtd_usd, 4)
        projected_month_end_usd = (round(transfer_projected_usd, 4)
                                    if transfer_projected_usd is not None else None)

    row.update(
        mtd_cost_usd=mtd_cost_usd,
        projected_month_end_usd=projected_month_end_usd,
        quota={"unit": "GB data transfer", "used": round(transfer_gb, 3),
               "limit": quota_gb,
               "projected": round(projected_gb, 3) if projected_gb is not None else None},
        pace=_pace(projected_gb, quota_gb),
        detail={"projects": project_names,
                "compute_hours": round(sums.get("compute_time_seconds", 0.0) / 3600, 2),
                "compute_cost_usd": None,
                "written_gb": round(sums.get("written_data_bytes", 0.0) / 1e9, 3),
                "storage_cost_usd": None,
                "data_transfer_cost_usd": round(transfer_mtd_usd, 4),
                "consumption_period": f"{period_start} → {period_end}",
                "cost_components_unavailable": (None if fixed_override is not None
                                                 else cost_components_unavailable)},
        # Prefer an operator note from the budgets SSoT (e.g. a temporary
        # paid-plan explanation) so what the operator recorded reaches the page;
        # fall back to a note that names the estimate's gaps when no override
        # is configured, so the console never implies this is a full bill.
        note=_cfg_note(budgets, "neon")
             or (None if fixed_override is not None else
                 "estimated from data transfer overage only — compute and "
                 "storage costs are unknown (see detail.cost_components_unavailable); "
                 "actual bill will be higher if either is nonzero"),
    )
    row["budget_usd"] = _budget_usd(budgets, "neon")
    return row


def collect_github(mw: dict, budgets: dict, secrets: dict, *, account: str,
                   kind: str, target_year: int | None = None,
                   target_month: int | None = None) -> dict:
    """One row per billing account (org + personal are separate meters with
    separate included-minutes quotas — they are deliberately NOT merged).
    Enhanced billing platform only: the legacy ``settings/billing/actions``
    endpoints are 410-Gone/404 for both accounts (verified 2026-07-17), so the
    included-minutes quota comes from the budgets SSoT.

    ``target_year``/``target_month`` default to the live current month; the
    billing-usage endpoint already scopes its whole response server-side by
    these two params, so reconciliation (``reconcile_github``) reuses this
    same adapter unchanged for a closed prior month — just pass that month's
    year/month instead of "now"."""
    key = f"github_{kind}"
    row = _row(key, f"GitHub ({account} {kind})", source="billing_usage_api")
    # Personal-account billing needs its own user-scoped PAT ("Plan: read");
    # the org token cannot see it (404) — fall back so the row self-heals the
    # moment the operator adds the param, no code change needed.
    token = (secrets.get(SSM_GITHUB_USER_PAT) or secrets.get(SSM_GITHUB_TOKEN)
             if kind == "user" else secrets.get(SSM_GITHUB_TOKEN))
    if not token:
        row.update(status="not_configured", error=f"SSM {SSM_GITHUB_TOKEN} missing")
        return row
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28"}
    base = ("https://api.github.com/organizations/" if kind == "org"
            else "https://api.github.com/users/")
    now = _now_utc()
    year = target_year if target_year is not None else now.year
    month = target_month if target_month is not None else now.month
    try:
        doc = _http_json(f"{base}{account}/settings/billing/usage"
                         f"?year={year}&month={month}", headers)
    except RuntimeError as exc:
        if kind == "user" and SSM_GITHUB_USER_PAT not in secrets \
                and ("HTTP 404" in str(exc) or "HTTP 403" in str(exc)):
            # Known credential gap, not an outage: no fleet token can read the
            # personal account's billing. Surface as not_configured (kept out
            # of the incomplete-totals error banner) with the fix inline.
            row.update(status="not_configured",
                       error=f"needs a cipher813 fine-grained PAT with Plan:read "
                             f"stored at SSM {SSM_GITHUB_USER_PAT}")
            return row
        raise
    # The included-minutes quota (2000/mo, free org) is drawn ONLY by
    # private-repo minutes; public-repo usage appears in usageItems too but is
    # 100%-discounted free (verified live 2026-07-17: 34.9k total minutes, of
    # which most were public-repo/free) — so quota pacing must filter to
    # private repos or it overstates ~17x.
    private_repos = _private_repo_names(account, kind, headers)
    minutes_private, minutes_total, billed, by_product = 0.0, 0.0, 0.0, {}
    # Per-repo minutes + visibility, surfaced on the console so a repo drawing
    # real Actions minutes but classified "private" (or vice versa) is visible
    # at a glance instead of inferred — a wrong hardcoded public/private repo
    # list (not this live-API-derived set) caused a real incident 2026-07-17:
    # an entire self-hosted-runner migration was built for 6 repos that turned
    # out to be public (free/unlimited GHA already), based on their appearing
    # in a billing pull without checking actual visibility. This breakdown is
    # the console-side guardrail against repeating that mistake.
    by_repo: dict[str, float] = {}
    for item in doc.get("usageItems", []):
        product = str(item.get("product", "")).lower()
        net = float(item.get("netAmount", 0) or 0)
        billed += net
        by_product[product] = round(by_product.get(product, 0.0) + net, 2)
        if product == "actions" and "minute" in str(item.get("unitType", "")).lower():
            qty = float(item.get("quantity", 0) or 0)
            minutes_total += qty
            repo = item.get("repositoryName") or "(unattributed)"
            by_repo[repo] = by_repo.get(repo, 0.0) + qty
            if repo in private_repos:
                minutes_private += qty
    minutes_public = round(minutes_total - minutes_private, 1)
    gha_by_repo = sorted(
        (
            {
                "repo": repo,
                "visibility": "private" if repo in private_repos else "public",
                "minutes": round(minutes, 1),
            }
            for repo, minutes in by_repo.items()
        ),
        key=lambda r: r["minutes"],
        reverse=True,
    )
    included = _included_minutes(budgets, key)
    projected_min = _project(minutes_private, mw["elapsed_frac"])
    row.update(
        mtd_cost_usd=round(billed, 2),
        quota={"unit": "private-repo Actions minutes",
               "used": round(minutes_private, 1), "limit": included,
               "projected": round(projected_min, 0) if projected_min is not None else None},
        detail={"billed_usd_by_product": by_product,
                "total_actions_minutes_incl_public_free": round(minutes_total, 1),
                "gha_private_minutes": round(minutes_private, 1),
                "gha_public_minutes": minutes_public,
                "gha_by_repo": gha_by_repo},
    )
    row = _finish_usd_row(row, mw, _budget_usd(budgets, key))
    # Quota pace (included minutes) is the leading signal; billed-$ pace only
    # overrides when a budget is set and projected $ breaches it.
    if row["pace"] is None:
        row["pace"] = _pace(projected_min, included)
    return row


def _private_repo_names(account: str, kind: str, headers: dict) -> set[str]:
    """Names of the account's PRIVATE repos (the only ones that draw the
    included-minutes quota). Org listing filters server-side via type=private;
    the user path needs the user's own PAT (visibility=private on /user/repos
    only works self-scoped)."""
    url = (f"https://api.github.com/orgs/{account}/repos?type=private&per_page=100"
           if kind == "org"
           else "https://api.github.com/user/repos?visibility=private&affiliation=owner&per_page=100")
    names: set[str] = set()
    for page in range(1, 6):  # 500-repo ceiling, far above fleet size
        batch = _http_json(f"{url}&page={page}", headers)
        names.update(r["name"] for r in batch if isinstance(r, dict) and "name" in r)
        if len(batch) < 100:
            break
    return names


def _cfg_note(budgets: dict, key: str) -> str | None:
    """Operator-authored note for a provider from the budgets SSoT (surfaced on
    the row so a manual explanation — e.g. a temporary plan change — reaches the
    console)."""
    return ((budgets.get("providers") or {}).get(key) or {}).get("note")


def _included_minutes(budgets: dict, key: str) -> float | None:
    v = ((budgets.get("providers") or {}).get(key) or {}).get("included_minutes")
    return float(v) if v is not None else None


def _budget_usd(budgets: dict, key: str) -> float | None:
    v = ((budgets.get("providers") or {}).get(key) or {}).get("monthly_budget_usd")
    return float(v) if v is not None else None


def _fixed_usd(budgets: dict, key: str) -> float | None:
    v = ((budgets.get("providers") or {}).get(key) or {}).get("fixed_monthly_usd")
    return float(v) if v is not None else None


def fixed_rows(budgets: dict, produced_keys: set[str]) -> list[dict]:
    """Budgets-config-only line items (flat subscriptions with no live API) —
    adding a future subscription is a config edit, not a code change."""
    rows = []
    for key, cfg in (budgets.get("providers") or {}).items():
        if key in produced_keys:
            continue
        fixed = cfg.get("fixed_monthly_usd")
        if fixed is None:
            continue
        rows.append(_row(
            key, cfg.get("label", key), status="fixed", source="budgets_config",
            mtd_cost_usd=float(fixed), projected_month_end_usd=float(fixed),
            budget_usd=float(fixed), pace="fixed",
            note=cfg.get("note") or "flat subscription (from expense_budgets.json)"))
    return rows


# ---------------------------------------------------------------------------
# Month-close reconciliation (alpha-engine-config#2849)
#
# Re-queries each provider's OWN adapter (or, for openrouter/deepseek, the
# already-written month-start baseline docs) for the just-CLOSED prior
# month's finalized actual, then diffs it against what the prior month's own
# ``expenses/monthly/{YYYY-MM}.json`` rollup last recorded — never re-derives
# a provider's arithmetic from scratch (same anchor-to-provider principle the
# AWS forecast fix codified). Neon has no closed-period historical read on
# this API tier (its adapter only ever sees the CURRENT consumption period) —
# reconciled as "not_available" rather than guessed.
# ---------------------------------------------------------------------------

def _prior_month_window(now: datetime) -> dict:
    """The most recently CLOSED calendar month (UTC) as of ``now`` — same
    shape as ``_month_window`` (period id + start), plus ``end`` (first
    instant of ``now``'s month = the exclusive upper bound of the prior
    month). ``elapsed_frac`` is always 1.0: a closed month has nothing left
    to project."""
    this_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prior_end = this_month_start
    prior_start = (prior_end - timedelta(days=1)).replace(day=1)
    days = calendar.monthrange(prior_start.year, prior_start.month)[1]
    return {
        "period": prior_start.strftime("%Y-%m"),
        "start": prior_start,
        "end": prior_end,
        "total_seconds": days * 86400.0,
        "elapsed_frac": 1.0,
    }


def _last_recorded_row(prior_doc: dict | None, key: str) -> dict:
    """The prior month's own rollup row for ``key`` (``mtd_cost_usd`` at
    whatever time the last day-of-month collect run happened = "accrued MTD
    final" per the issue's field name; ``projected_month_end_usd`` = "last
    seen" projection) — read directly, never re-derived."""
    if not prior_doc:
        return {}
    for row in prior_doc.get("providers", []):
        if row.get("key") == key:
            return row
    return {}


def _reconciliation_row(key: str, prior_doc: dict | None, actual_final: float | None,
                        *, status: str = "ok", note: str | None = None) -> dict:
    last = _last_recorded_row(prior_doc, key)
    projected_last_seen = last.get("projected_month_end_usd")
    accrued_mtd_final = last.get("mtd_cost_usd")
    delta_usd = delta_pct = None
    if actual_final is not None and accrued_mtd_final is not None:
        delta_usd = round(actual_final - accrued_mtd_final, 2)
        if accrued_mtd_final:
            delta_pct = round(delta_usd / abs(accrued_mtd_final), 4)
        elif actual_final:
            delta_pct = 1.0  # accrued was 0 but the final actual wasn't — full drift
    return {
        "projected_last_seen": projected_last_seen,
        "accrued_mtd_final": accrued_mtd_final,
        "actual_final": round(actual_final, 2) if actual_final is not None else None,
        "delta_usd": delta_usd,
        "delta_pct": delta_pct,
        "status": status,
        "note": note,
    }


def reconcile_aws(prior_mw: dict, budgets: dict, prior_doc: dict | None) -> dict:
    ce = boto3.client("ce", region_name="us-east-1")
    by_service = _ce_unblended_by_service(
        ce, prior_mw["start"].strftime("%Y-%m-%d"), prior_mw["end"].strftime("%Y-%m-%d"))
    return _reconciliation_row("aws", prior_doc, round(sum(by_service.values()), 2))


def reconcile_anthropic(prior_mw: dict, budgets: dict, secrets: dict, s3,
                        prior_doc: dict | None) -> dict:
    days = calendar.monthrange(prior_mw["start"].year, prior_mw["start"].month)[1]
    row = collect_anthropic({**prior_mw, "elapsed_frac": 1.0}, budgets, secrets, s3,
                            end=prior_mw["end"], last_day=days)
    return _reconciliation_row("anthropic_api", prior_doc, row.get("mtd_cost_usd"))


def reconcile_counter_diff(s3, prior_period: str, current_period: str, key: str,
                           counter_key: str, prior_doc: dict | None) -> dict:
    """OpenRouter/DeepSeek: zero new HTTP calls. The current month's own
    month-start baseline (``expenses/baselines/{current_period}.json``,
    written first-writer-wins on the 1st) IS the exact end-of-prior-month
    counter reading; diffed against the prior month's own start baseline it
    gives the prior month's full-month counter delta — the same quantity the
    live adapter computes MTD, just over the now-closed full window. Both
    stored counters (``openrouter_total_usage``, ``deepseek_neg_balance``)
    are already oriented to INCREASE with spend (deepseek's is ``-balance``,
    per ``ensure_baseline``/``collect_deepseek``) — a plain forward diff, no
    extra sign handling needed here."""
    prior_base = _s3_json(s3, f"{BASELINE_PREFIX}{prior_period}.json")
    current_base = _s3_json(s3, f"{BASELINE_PREFIX}{current_period}.json")
    if not prior_base or not current_base:
        return _reconciliation_row(
            key, prior_doc, None, status="not_available",
            note="missing month-start baseline for prior or current period — "
                 "cannot diff a closed-month counter window")
    prior_val = (prior_base.get("counters") or {}).get(counter_key)
    current_val = (current_base.get("counters") or {}).get(counter_key)
    if prior_val is None or current_val is None:
        return _reconciliation_row(
            key, prior_doc, None, status="not_available",
            note=f"counter {counter_key} absent from one of the two baselines")
    actual_final = round(max(current_val - prior_val, 0.0), 4)
    return _reconciliation_row(
        key, prior_doc, actual_final,
        note="counter-diff of two already-written month-start baselines — "
             "exact unless a top-up/consumption landed in the instant between "
             "month rollover and this run")


def reconcile_neon(prior_doc: dict | None) -> dict:
    """Neon's Launch-plan API exposes only the CURRENT consumption period
    (``consumption_history`` is Scale-plan-only, 403/404 verified live) —
    there is no closed-period historical read to re-query, so reconciliation
    is marked not_available rather than approximated from snapshots (Neon's
    own period boundaries aren't calendar-month-aligned)."""
    return _reconciliation_row(
        "neon", prior_doc, None, status="not_available",
        note="Neon's API exposes only the current consumption period — no "
             "historical/closed-period endpoint is reachable on this plan")


def reconcile_github(prior_mw: dict, budgets: dict, secrets: dict, *, account: str,
                     kind: str, prior_doc: dict | None) -> dict:
    row = collect_github(prior_mw, budgets, secrets, account=account, kind=kind,
                         target_year=prior_mw["start"].year,
                         target_month=prior_mw["start"].month)
    key = f"github_{kind}"
    if row["status"] == "not_configured":
        return _reconciliation_row(key, prior_doc, None, status="not_configured",
                                   note=row.get("error"))
    if row["status"] == "error":
        return _reconciliation_row(key, prior_doc, None, status="error", note=row.get("error"))
    return _reconciliation_row(key, prior_doc, row.get("mtd_cost_usd"))


def run_reconciliation(s3, now: datetime, budgets: dict, secrets: dict) -> dict:
    """Month-close true-up entry point (``event["mode"] == "reconcile"``).
    Writes ``expenses/reconciliation/{prior-period}.json`` and returns a
    summary. Each provider is independently fenced (mirrors the ``fenced``
    closure in ``handler`` — one provider's re-query failing must not blank
    the others' reconciliation rows)."""
    prior_mw = _prior_month_window(now)
    prior_doc = _s3_json(s3, f"{MONTHLY_PREFIX}{prior_mw['period']}.json")
    providers: dict[str, dict] = {}

    def fenced(key: str, fn) -> None:
        try:
            providers[key] = fn()
        except Exception as exc:  # noqa: BLE001 — one provider's re-query outage
            # must not blank the others' reconciliation rows.
            logger.exception("reconciliation for %s failed", key)
            providers[key] = _reconciliation_row(key, prior_doc, None, status="error",
                                                 note=str(exc)[:300])

    fenced("aws", lambda: reconcile_aws(prior_mw, budgets, prior_doc))
    fenced("anthropic_api",
          lambda: reconcile_anthropic(prior_mw, budgets, secrets, s3, prior_doc))
    fenced("openrouter", lambda: reconcile_counter_diff(
        s3, prior_mw["period"], now.strftime("%Y-%m"), "openrouter",
        "openrouter_total_usage", prior_doc))
    fenced("deepseek", lambda: reconcile_counter_diff(
        s3, prior_mw["period"], now.strftime("%Y-%m"), "deepseek",
        "deepseek_neg_balance", prior_doc))
    fenced("neon", lambda: reconcile_neon(prior_doc))
    fenced("github_org", lambda: reconcile_github(
        prior_mw, budgets, secrets, account=GITHUB_ORG, kind="org", prior_doc=prior_doc))
    fenced("github_user", lambda: reconcile_github(
        prior_mw, budgets, secrets, account=GITHUB_USER, kind="user", prior_doc=prior_doc))

    flagged = [k for k, r in providers.items()
              if r.get("delta_pct") is not None
              and abs(r["delta_pct"]) > RECONCILIATION_DELTA_PCT_THRESHOLD]
    doc = {
        "schema_version": 1,
        "period": prior_mw["period"],
        "as_of": now.isoformat(),
        "providers": providers,
        "delta_pct_threshold": RECONCILIATION_DELTA_PCT_THRESHOLD,
        "flagged": flagged,
    }
    _put_json(s3, f"{RECONCILIATION_PREFIX}{prior_mw['period']}.json", doc)
    return {"period": prior_mw["period"], "providers": len(providers), "flagged": flagged}


# ---------------------------------------------------------------------------
# Counters (lifetime/balance readings) + baseline
# ---------------------------------------------------------------------------

def read_counters(secrets: dict) -> tuple[dict, dict[str, str]]:
    """Fetch the raw monotonic/balance counters for the diff-based providers.
    Returns (counters, errors) — an unreachable provider lands in ``errors``
    and its row is built as status=error downstream."""
    counters: dict = {}
    errors: dict[str, str] = {}
    if SSM_OPENROUTER in secrets:
        try:
            doc = _http_json("https://openrouter.ai/api/v1/credits",
                             {"Authorization": f"Bearer {secrets[SSM_OPENROUTER]}"})
            d = doc.get("data", {})
            counters["openrouter_total_usage"] = float(d["total_usage"])
            counters["openrouter_credits_remaining"] = (
                float(d.get("total_credits", 0)) - float(d["total_usage"]))
        except Exception as exc:  # noqa: BLE001 — recorded per-row downstream
            logger.exception("openrouter counter fetch failed")
            errors["openrouter"] = str(exc)[:300]
    if SSM_DEEPSEEK in secrets:
        try:
            doc = _http_json("https://api.deepseek.com/user/balance",
                             {"Authorization": f"Bearer {secrets[SSM_DEEPSEEK]}"})
            infos = doc.get("balance_infos") or []
            usd = next((b for b in infos if b.get("currency") == "USD"), infos[0] if infos else None)
            if usd is None:
                raise RuntimeError(f"no balance_infos in response: {doc}")
            counters["deepseek_balance"] = float(usd["total_balance"])
            counters["deepseek_currency"] = usd.get("currency", "USD")
        except Exception as exc:  # noqa: BLE001 — recorded per-row downstream
            logger.exception("deepseek counter fetch failed")
            errors["deepseek"] = str(exc)[:300]
    return counters, errors


def ensure_baseline(s3, period: str, counters: dict) -> dict:
    """First-run-of-month counter baseline (first-writer-wins). If the doc
    exists but lacks a counter we can now read (provider key added mid-month),
    it is backfilled in place — the new counter's MTD then accrues from now."""
    key = f"{BASELINE_PREFIX}{period}.json"
    now_iso = _now_utc().isoformat()
    wanted = {}
    if "openrouter_total_usage" in counters:
        wanted["openrouter_total_usage"] = counters["openrouter_total_usage"]
    if "deepseek_balance" in counters:
        wanted["deepseek_neg_balance"] = -counters["deepseek_balance"]
    existing = _s3_json(s3, key)
    if existing is None:
        doc = {"schema_version": 1, "period": period,
               "counters": wanted, "as_of": {k: now_iso for k in wanted}}
        if _put_json(s3, key, doc, if_none_match=True):
            return doc
        existing = _s3_json(s3, key) or doc  # lost the race — read the winner
    missing = {k: v for k, v in wanted.items() if k not in existing.get("counters", {})}
    if missing:
        existing.setdefault("counters", {}).update(missing)
        existing.setdefault("as_of", {}).update({k: now_iso for k in missing})
        _put_json(s3, key, existing)
    return existing


def write_snapshot(s3, counters: dict) -> None:
    """First reading of each UTC day, kept for future trend charts. Best-effort:
    losing the first-writer race (412) is the designed outcome."""
    date_str = _now_utc().strftime("%Y-%m-%d")
    _put_json(s3, f"{SNAPSHOT_PREFIX}{date_str}.json",
              {"schema_version": 1, "as_of": _now_utc().isoformat(), "counters": counters},
              if_none_match=True)


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Dispatches on ``event.get("mode")`` — one Lambda, two EventBridge
    Scheduler rules with different ``Input`` (this codebase's established
    scheduling pattern; see ``deploy.sh``'s ``SCHED_NAMES``/``SCHED_CRONS``/
    ``SCHED_INPUTS``, not a new mechanism):
      - ``"collect"`` (default, incl. ``{}``/missing — preserves the existing
        twice-daily invocation's ``Input: "{}"`` unchanged) — the live MTD
        rollup.
      - ``"reconcile"`` — month-close true-up (alpha-engine-config#2849),
        scheduled ~03:00 UTC on the 2nd of each month."""
    mode = (event or {}).get("mode", "collect")
    if mode == "reconcile":
        return _reconcile(event, context)
    if mode != "collect":
        raise ValueError(f"unknown expense-collector event mode: {mode!r}")
    return _collect(event, context)


def _reconcile(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now = _now_utc()
    s3 = boto3.client("s3", region_name=REGION)
    budgets = _s3_json(s3, BUDGETS_KEY) or {}
    secrets = _load_ssm(SSM_PARAMS)
    return run_reconciliation(s3, now, budgets, secrets)


def _collect(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    now = _now_utc()
    mw = _month_window(now)
    s3 = boto3.client("s3", region_name=REGION)
    warnings: list[str] = []

    budgets = _s3_json(s3, BUDGETS_KEY)
    if budgets is None:
        # Budgets SSoT missing ⇒ every budget/quota field degrades to null.
        # Recorded here + rendered as a page-level warning by the console.
        warnings.append(f"budgets SSoT s3://{BUCKET}/{BUDGETS_KEY} missing — "
                        "no budget/pace fields; seed it from expense_budgets.seed.json")
        budgets = {}

    secrets = _load_ssm(SSM_PARAMS)
    counters, counter_errors = read_counters(secrets)
    baseline = ensure_baseline(s3, mw["period"], counters)
    write_snapshot(s3, counters)

    rows: list[dict] = []

    def fenced(key: str, label: str, fn) -> None:
        # Adapter fence — one provider outage must not blank the others; the
        # failure's recording surface is this row's error field (+ CW logs).
        try:
            rows.append(fn())
        except Exception as exc:  # noqa: BLE001 — see fence rationale above
            logger.exception("provider %s failed", key)
            rows.append(_row(key, label, status="error", error=str(exc)[:300]))

    fenced("aws", "AWS", lambda: collect_aws(mw, budgets))
    fenced("anthropic_api", "Anthropic API",
           lambda: collect_anthropic(mw, budgets, secrets, s3))
    if "openrouter" in counter_errors:
        rows.append(_row("openrouter", "OpenRouter", status="error",
                         error=counter_errors["openrouter"]))
    else:
        fenced("openrouter", "OpenRouter",
               lambda: collect_openrouter(mw, budgets, secrets, counters, baseline))
    if "deepseek" in counter_errors:
        rows.append(_row("deepseek", "DeepSeek", status="error",
                         error=counter_errors["deepseek"]))
    else:
        fenced("deepseek", "DeepSeek",
               lambda: collect_deepseek(mw, budgets, secrets, counters, baseline))
    fenced("neon", "Neon Postgres", lambda: collect_neon(mw, budgets, secrets))
    fenced("github_org", f"GitHub ({GITHUB_ORG} org)",
           lambda: collect_github(mw, budgets, secrets, account=GITHUB_ORG, kind="org"))
    fenced("github_user", f"GitHub ({GITHUB_USER})",
           lambda: collect_github(mw, budgets, secrets, account=GITHUB_USER, kind="user"))

    rows.extend(fixed_rows(budgets, {r["key"] for r in rows}))

    ok_rows = [r for r in rows if r["status"] in {"ok", "fixed"}]
    err_rows = [r for r in rows if r["status"] == "error"]
    totals = {
        "mtd_usd": round(sum(r["mtd_cost_usd"] or 0 for r in ok_rows), 2),
        "projected_usd": round(sum((r["projected_month_end_usd"]
                                    if r["projected_month_end_usd"] is not None
                                    else (r["mtd_cost_usd"] or 0)) for r in ok_rows), 2),
        "budget_usd": round(sum(r["budget_usd"] for r in ok_rows
                                if r["budget_usd"] is not None), 2),
        "incomplete": bool(err_rows),
    }
    doc = {
        "schema_version": 1,
        "period": mw["period"],
        "as_of": now.isoformat(),
        "month_start": mw["start"].isoformat(),
        "month_elapsed_frac": round(mw["elapsed_frac"], 4),
        "providers": rows,
        "totals": totals,
        "warnings": warnings,
    }
    _put_json(s3, f"{MONTHLY_PREFIX}{mw['period']}.json", doc)
    _put_json(s3, LATEST_KEY, doc)

    # Over-budget Telegram alert (config#2843) — after the rollup write, since
    # it reads the rows' already-computed `pace` field; never allowed to raise
    # (notification-only enhancement layered on top of an already-successful
    # rollup).
    alert_result = run_over_budget_alerts(s3, mw["period"], rows)

    if err_rows and not ok_rows:
        # Every live adapter failed — systemic (network/creds/deploy) breakage,
        # not a provider blip: raise so the Lambda error metric pages.
        raise RuntimeError(f"all provider adapters failed: "
                           f"{[(r['key'], r['error']) for r in err_rows]}")
    return {"period": mw["period"], "providers": len(rows),
            "errors": [(r["key"], r["error"]) for r in err_rows],
            "totals": totals, "alerts": alert_result}
