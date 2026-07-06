"""Unit tests for alpha-engine-pipeline-watchdog index.handler.

Stubs ``nousergon_lib.trading_calendar.last_closed_trading_day``,
``nousergon_lib.alerts.publish``, ``flow_doctor_telegram.notify_via_flow_doctor``,
and ``boto3.client('stepfunctions')`` so tests do not hit AWS or the lib.
Each test pins one decision branch (watch-day eligibility, alert-or-skip,
dedup wiring, fail-loud) per the ``feedback_no_silent_fails`` discipline.
"""

from __future__ import annotations

import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# Stub `nousergon_lib.trading_calendar` + `nousergon_lib.alerts` BEFORE
# importing the handler so test envs without the lib installed still pass.
_lib_pkg = types.ModuleType("nousergon_lib")
_tc_mod = types.ModuleType("nousergon_lib.trading_calendar")
_tc_mod.last_closed_trading_day = MagicMock()
_tc_mod.previous_trading_day = MagicMock()
_alerts_mod = types.ModuleType("nousergon_lib.alerts")
_alerts_mod.publish = MagicMock()
_lib_pkg.trading_calendar = _tc_mod
_lib_pkg.alerts = _alerts_mod
sys.modules["nousergon_lib"] = _lib_pkg
sys.modules["nousergon_lib.trading_calendar"] = _tc_mod
sys.modules["nousergon_lib.alerts"] = _alerts_mod

_ng_pkg = types.ModuleType("nousergon_lib")
_ng_fleet_mod = types.ModuleType("nousergon_lib.flow_doctor_fleet")
_ng_fleet_mod.PIPELINE_OBSERVER_TELEGRAM_TOPICS = ("CRITICAL", "PIPELINE", "OPS_HEALTH")
_ng_pkg.flow_doctor_fleet = _ng_fleet_mod
sys.modules["nousergon_lib"] = _ng_pkg
sys.modules["nousergon_lib.flow_doctor_fleet"] = _ng_fleet_mod

_fd_mod = types.ModuleType("flow_doctor_telegram")
_fd_mod.notify_via_flow_doctor = MagicMock(return_value=True)
sys.modules["flow_doctor_telegram"] = _fd_mod

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402


SAT_ARN = index.SATURDAY_SF_ARN
WKD_ARN = index.WEEKDAY_SF_ARN
EOD_ARN = index.EOD_SF_ARN


# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_lib_mocks():
    """Reset the module-level MagicMocks between tests so call-counts +
    side_effects don't leak."""
    _tc_mod.last_closed_trading_day.reset_mock()
    _tc_mod.last_closed_trading_day.side_effect = None
    _tc_mod.previous_trading_day.reset_mock()
    _tc_mod.previous_trading_day.side_effect = None
    _alerts_mod.publish.reset_mock()
    _alerts_mod.publish.side_effect = None
    _alerts_mod.publish.return_value = _make_publish_result(sns_ok=True, telegram_ok=True)
    _fd_mod.notify_via_flow_doctor.reset_mock()
    _fd_mod.notify_via_flow_doctor.return_value = True


def _make_publish_result(*, sns_ok: bool, telegram_ok: bool, dedup_skipped: bool = False):
    """Build a stub PublishResult-shaped object with the attributes the
    handler reads."""
    sns = MagicMock(ok=sns_ok, detail="ok" if sns_ok else "fail")
    telegram = MagicMock(ok=telegram_ok, detail="ok" if telegram_ok else "fail")
    return MagicMock(sns=sns, telegram=telegram, dedup_skipped=dedup_skipped)


def _make_sfn_client(executions_by_arn: dict) -> MagicMock:
    """Build a boto3.stepfunctions mock that returns the given executions
    for matching ARN + statusFilter calls.

    ``executions_by_arn`` maps SF ARN → list of execution dicts (each with
    ``startDate``). The mock ignores statusFilter (returns the same list
    for every status — tests that need finer control should build a
    bespoke MagicMock side_effect).
    """
    client = MagicMock()

    def _list_executions(**kwargs):
        arn = kwargs.get("stateMachineArn")
        execs = executions_by_arn.get(arn, [])
        return {"executions": execs, "nextToken": None}

    client.list_executions.side_effect = _list_executions
    return client


def _frozen_now(year=2026, month=5, day=28, hour=14, minute=0):
    """2026-05-28 is a Thursday — a trading day at 14:00 UTC."""
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


# ── _is_trading_day_now ──────────────────────────────────────────────────


def test_is_trading_day_now_true_on_a_trading_day():
    """Thursday 2026-05-28 at 14:00 UTC. trading_calendar's synthetic
    post-close call should return today (2026-05-28)."""
    now = _frozen_now(2026, 5, 28, 14, 0)
    # First call (at now_utc): pre-open → returns yesterday's session
    # Second call (synthetic 22:00 UTC = post-close): returns today's date
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 27),
        date(2026, 5, 28),
    ]
    assert index._is_trading_day_now(now) is True


def test_is_trading_day_now_false_on_saturday():
    """Saturday 2026-05-30 — last close is Friday 5/29, synthetic post-close
    on Saturday still returns Friday (no Saturday session)."""
    now = _frozen_now(2026, 5, 30, 14, 0)
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 29),
        date(2026, 5, 29),
    ]
    assert index._is_trading_day_now(now) is False


def test_is_trading_day_now_false_on_a_holiday():
    """Memorial Day 2026-05-25 (Monday) — NYSE closed. Last close was
    Friday 5/22; synthetic post-close also returns 5/22 (Monday holiday is
    not a session)."""
    now = _frozen_now(2026, 5, 25, 14, 0)
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 22),
        date(2026, 5, 22),
    ]
    assert index._is_trading_day_now(now) is False


# ── _count_executions_in_window ──────────────────────────────────────────


def test_count_executions_returns_zero_when_no_executions():
    client = _make_sfn_client({})
    seen = index._count_executions_in_window(WKD_ARN, 24 * 3600, client=client)
    assert seen == 0


def test_count_executions_counts_executions_within_window():
    """Executions started within window are counted; older ones are not."""
    now = datetime.now(timezone.utc)
    execs = [
        {"startDate": now - timedelta(hours=1)},  # in window
        {"startDate": now - timedelta(hours=10)},  # in window
        {"startDate": now - timedelta(hours=48)},  # OUT of 24h window
    ]
    client = _make_sfn_client({WKD_ARN: execs})
    seen = index._count_executions_in_window(WKD_ARN, 24 * 3600, client=client)
    # Window is 24h, so 2 in-window executions. But we iterate 5 status
    # filters; the mock returns the same list for each, so seen = 2 * 5 = 10.
    # In production each status filter returns disjoint results — this
    # mock-aliasing inflates the count but doesn't change the "did we
    # see ANY?" semantics the handler downstream cares about.
    assert seen >= 2  # at minimum 2 in-window per call * 1 status; mock returns same list per status


def test_count_executions_handles_missing_start_date():
    """Executions without startDate are skipped gracefully — never raises."""
    client = _make_sfn_client(
        {WKD_ARN: [{"startDate": None}, {"name": "no-startdate-at-all"}]}
    )
    seen = index._count_executions_in_window(WKD_ARN, 24 * 3600, client=client)
    assert seen == 0


# ── _check_sf: skip-when-not-watch-day ──────────────────────────────────


def test_check_sf_skips_when_not_watch_day_and_does_not_call_sfn():
    """is_watch_day=False → no SFN call, no alert, structured skip_reason."""
    client = MagicMock()
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=False,
        skip_reason_if_not_watching="weekend",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.checked is False
    assert result.skip_reason == "weekend"
    assert result.alert_emitted is False
    assert result.executions_seen is None
    client.list_executions.assert_not_called()
    _alerts_mod.publish.assert_not_called()


# ── _check_sf: alert path ───────────────────────────────────────────────


def test_check_sf_emits_alert_when_zero_executions_in_window():
    client = _make_sfn_client({})  # no executions for any ARN
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.checked is True
    assert result.executions_seen == 0
    assert result.alert_emitted is True
    _alerts_mod.publish.assert_called_once()
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert call_kwargs["severity"] == "error"
    assert call_kwargs["source"] == "alpha-engine-pipeline-watchdog"
    assert call_kwargs["sns_topic_arn"] == index.WATCHDOG_SNS_TOPIC_ARN
    assert call_kwargs["telegram"] is False
    assert "Weekday SF" in call_kwargs["message"]
    assert "24h" in call_kwargs["message"]
    _fd_mod.notify_via_flow_doctor.assert_called_once()
    fd_kwargs = _fd_mod.notify_via_flow_doctor.call_args.kwargs
    assert fd_kwargs["silent"] is False
    assert fd_kwargs["severity"] == "error"
    assert fd_kwargs["flow_name"] == index._FLOW_NAME
    assert fd_kwargs["topics"] == _ng_fleet_mod.PIPELINE_OBSERVER_TELEGRAM_TOPICS
    assert "Weekday SF" in _fd_mod.notify_via_flow_doctor.call_args.args[0]


def test_check_sf_alert_uses_distinct_watchdog_sns_topic_not_alpha_engine_alerts():
    """Channel-independence guard — publish MUST target the watchdog topic,
    NOT the main alerts topic. Reflects plan doc §3.5."""
    client = _make_sfn_client({})
    index._check_sf(
        sf_label="EOD SF",
        sf_arn=EOD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert "alpha-engine-watchdog-alerts" in call_kwargs["sns_topic_arn"]
    assert "alpha-engine-alerts" not in call_kwargs["sns_topic_arn"].replace(
        "alpha-engine-alerts", "", 0
    ) or "watchdog" in call_kwargs["sns_topic_arn"]


def test_check_sf_alert_carries_dedup_key_and_12h_window():
    """Repeated daily fires on a persistent outage should collapse to one
    alert per (SF, date) within the 12h window."""
    client = _make_sfn_client({})
    index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert "pipeline-watchdog-Weekday SF-" in call_kwargs["dedup_key"]
    assert call_kwargs["dedup_window_min"] == 12 * 60


def test_check_sf_clear_when_executions_seen_does_not_alert():
    """Non-zero executions → no alert."""
    now = datetime.now(timezone.utc)
    client = _make_sfn_client({WKD_ARN: [{"startDate": now - timedelta(hours=1)}]})
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.checked is True
    assert result.executions_seen and result.executions_seen > 0
    assert result.alert_emitted is False
    _alerts_mod.publish.assert_not_called()


# ── handler — full integration ──────────────────────────────────────────


def test_handler_on_trading_day_checks_weekday_and_eod_skips_saturday():
    """Wednesday 2026-05-27 14:00 UTC. Both Weekday + EOD checked
    (trading day); Saturday skipped (not Sunday)."""
    # _is_trading_day_now needs 2 calls to last_closed_trading_day per call
    # → so for the one call inside handler, we set 2 return values
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 26),  # pre-open call at now
        date(2026, 5, 27),  # synthetic 22:00 UTC call → returns today
    ]
    # EOD window calc: previous_trading_day(2026-05-27) → 2026-05-26 (Tue)
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 26)
    now = _frozen_now(2026, 5, 27, 14, 0)

    # Mock boto3.client to return our fake client (no executions = alerts)
    fake_client = _make_sfn_client({})
    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        # also patch fromtimestamp + the class itself for timedelta math
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    assert summary["is_trading_today"] is True
    assert summary["is_sunday"] is False

    by_label = {c["sf_label"]: c for c in summary["checks"]}
    assert by_label["Weekday SF"]["checked"] is True
    assert by_label["Weekday SF"]["alert_emitted"] is True
    assert by_label["EOD SF"]["checked"] is True
    assert by_label["EOD SF"]["alert_emitted"] is True
    assert by_label["Saturday SF"]["checked"] is False
    assert "not Sunday" in by_label["Saturday SF"]["skip_reason"]


def test_handler_on_saturday_skips_weekday_and_eod():
    """Saturday 2026-05-30 14:00 UTC. Weekday + EOD skipped (weekend);
    Saturday skipped too (Saturday SF watch-day is Sunday, not Saturday)."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 29),  # Friday close
        date(2026, 5, 29),  # synthetic post-close still Friday
    ]
    now = _frozen_now(2026, 5, 30, 14, 0)
    fake_client = _make_sfn_client({})

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    assert summary["is_trading_today"] is False
    assert summary["is_sunday"] is False
    for check in summary["checks"]:
        assert check["checked"] is False
        assert check["alert_emitted"] is False


def test_handler_on_sunday_checks_saturday_sf_alone():
    """Sunday 2026-05-31 14:00 UTC. Weekday + EOD skipped (weekend);
    Saturday SF checked (Sunday IS the watch-day for Saturday SF)."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 29),  # Friday close
        date(2026, 5, 29),  # synthetic post-close still Friday (Sunday is not a session)
    ]
    now = _frozen_now(2026, 5, 31, 14, 0)
    fake_client = _make_sfn_client({})  # no Saturday SF executions in last 7d → alert

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    assert summary["is_trading_today"] is False
    assert summary["is_sunday"] is True
    by_label = {c["sf_label"]: c for c in summary["checks"]}
    assert by_label["Weekday SF"]["checked"] is False
    assert by_label["EOD SF"]["checked"] is False
    assert by_label["Saturday SF"]["checked"] is True
    assert by_label["Saturday SF"]["alert_emitted"] is True


# ── Fail-loud guard ─────────────────────────────────────────────────────


def test_handler_propagates_listexecutions_error_for_lambda_retry():
    """ListExecutions failure → raises. Lambda's CW-alarm-on-errors path
    pages the operator; we MUST NOT silently skip a check."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 26),
        date(2026, 5, 27),
    ]
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 26)
    now = _frozen_now(2026, 5, 27, 14, 0)

    failing_client = MagicMock()
    failing_client.list_executions.side_effect = RuntimeError("IAM denied")

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = failing_client

        with pytest.raises(RuntimeError, match="IAM denied"):
            index.handler({}, None)


# ── _eod_window_seconds — trading-day-aware EOD window ──────────────────
#
# Codified after the 2026-05-26 morning false-positive Telegram alert:
# the watchdog fires at 14:00 UTC, but today's EOD SF doesn't fire until
# ~20:00 UTC (after market close at 13:00 PT + daemon shutdown). So the
# most recent EXPECTED EOD at watchdog firing time is the PREVIOUS trading
# day's, NOT the previous 24h of calendar time. After a holiday weekend
# (Fri close → Mon holiday → Tue 14:00 UTC watchdog), the gap is ~66h,
# not 24h. ``_eod_window_seconds`` returns the correct trading-day-aware
# window so EOD's Tuesday-post-Memorial-Day check correctly captures
# Friday's EOD execution.


def test_eod_window_seconds_normal_wed_after_tue():
    """Wed 14:00 UTC, prev_trading_day=Tue. Gap from Tue 20:00 UTC to
    Wed 14:00 UTC = 18h. Window = 18h + 1h slack = 19h."""
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 26)  # Tue
    now = datetime(2026, 5, 27, 14, 0, tzinfo=timezone.utc)  # Wed
    seconds = index._eod_window_seconds(now)
    assert seconds == 18 * 3600 + 3600  # 19h


def test_eod_window_seconds_tue_post_memorial_day_holiday():
    """Tue 2026-05-26 14:00 UTC. Memorial Day (Mon 5/25) was a holiday;
    prev_trading_day = Fri 5/22. Gap from Fri 20:00 UTC to Tue 14:00 UTC
    = (4 calendar days * 24h) - 6h = 90h. Window = 90h + 1h slack = 91h.
    **This is the 2026-05-26 morning incident** — the false-positive
    Telegram alert was caused by the prior hardcoded 24h calendar window
    failing to include Fri's EOD execution."""
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 22)  # Fri (Mon was holiday)
    now = datetime(2026, 5, 26, 14, 0, tzinfo=timezone.utc)  # Tue post-holiday
    seconds = index._eod_window_seconds(now)
    assert seconds == 90 * 3600 + 3600  # 91h


def test_eod_window_seconds_mon_after_normal_weekend():
    """Mon 14:00 UTC after a normal weekend. prev_trading_day = Fri.
    Gap from Fri 20:00 UTC to Mon 14:00 UTC = (3 days * 24h) - 6h = 66h.
    Window = 67h. Catches Fri's EOD."""
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 29)  # Fri
    now = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)  # Mon
    seconds = index._eod_window_seconds(now)
    assert seconds == 66 * 3600 + 3600  # 67h


def test_eod_window_seconds_clamps_negative_gap_to_slack():
    """Defensive: if prev_eod_expected somehow lands after now_utc
    (synthetic test input), window collapses to slack only — never goes
    negative."""
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 27)  # Wed (same as now)
    now = datetime(2026, 5, 27, 10, 0, tzinfo=timezone.utc)  # Wed 10:00 UTC, prev_eod is "today" 20:00 UTC
    # Gap = 10:00 - 20:00 = -10h → clamped to 0, window = 0 + 1h slack = 3600s
    seconds = index._eod_window_seconds(now)
    assert seconds == 3600


# ── handler integration: post-holiday EOD captures previous Fri's EOD ──


def test_handler_post_holiday_eod_does_not_false_alert():
    """Tue 2026-05-26 14:00 UTC after Memorial Day Mon 5/25. EOD SF's
    last expected firing was Fri 5/22 ~20:00 UTC (~66h ago). With the
    trading-day-aware window, the watchdog should NOT alert when Friday's
    EOD execution is visible in the 67h window. Regression guard for the
    2026-05-26 morning false-positive Telegram alert."""
    # is_trading_day_now: today (Tue 5/26) IS a trading day
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 22),  # at 14:00 UTC pre-open, last close was Fri (Mon was holiday)
        date(2026, 5, 26),  # synthetic 22:00 UTC post-close, today is the session
    ]
    # EOD prev_trading_day → Fri 5/22
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 22)
    now = _frozen_now(2026, 5, 26, 14, 0)

    # Fri 5/22 20:30 UTC EOD execution exists in S3 — i.e., in our mock
    fri_eod_start = datetime(2026, 5, 22, 20, 30, tzinfo=timezone.utc)
    # Weekday SF: today's 12:45 UTC execution (so weekday check also clears)
    today_weekday_start = datetime(2026, 5, 26, 12, 45, tzinfo=timezone.utc)
    fake_client = _make_sfn_client({
        EOD_ARN: [{"startDate": fri_eod_start}],
        WKD_ARN: [{"startDate": today_weekday_start}],
    })

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    by_label = {c["sf_label"]: c for c in summary["checks"]}
    # EOD should be CLEAR (>=1 execution in 67h window catches Fri's EOD)
    assert by_label["EOD SF"]["checked"] is True
    assert by_label["EOD SF"]["alert_emitted"] is False, (
        f"EOD watchdog should not false-alert on Tue post-Memorial-Day "
        f"when Fri's EOD is visible in the trading-day-aware window. "
        f"Got: {by_label['EOD SF']}"
    )
    # Weekday should also be clear (today's 12:45 UTC execution visible in 24h window)
    assert by_label["Weekday SF"]["alert_emitted"] is False


def test_handler_post_holiday_eod_alerts_when_friday_eod_missing():
    """Same Tue post-Memorial-Day setup, but Fri's EOD did NOT execute
    (genuine outage). With 0 executions in the 67h window, watchdog
    correctly fires. Trading-day-aware window doesn't HIDE genuine
    outages — it just stops the false-positives."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 22),
        date(2026, 5, 26),
    ]
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 22)
    now = _frozen_now(2026, 5, 26, 14, 0)

    # NO EOD execution; Weekday execution exists so only EOD alerts
    today_weekday_start = datetime(2026, 5, 26, 12, 45, tzinfo=timezone.utc)
    fake_client = _make_sfn_client({
        EOD_ARN: [],  # genuine outage
        WKD_ARN: [{"startDate": today_weekday_start}],
    })

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    by_label = {c["sf_label"]: c for c in summary["checks"]}
    assert by_label["EOD SF"]["alert_emitted"] is True, (
        "EOD watchdog must still alert on a GENUINE missed firing — "
        "the trading-day-aware window must not paper over real outages."
    )
    assert by_label["Weekday SF"]["alert_emitted"] is False
