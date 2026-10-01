"""Unit tests for alpha-engine-pipeline-watchdog index.handler.

Stubs ``nousergon_lib.trading_calendar.last_closed_trading_day``,
``nousergon_lib.alerts.publish``, ``flow_doctor_telegram.notify_via_flow_doctor``,
and ``boto3.client('stepfunctions')`` so tests do not hit AWS or the lib.
Each test pins one decision branch (watch-day eligibility, alert-or-skip,
dedup wiring, fail-loud) per the ``feedback_no_silent_fails`` discipline.
"""

from __future__ import annotations

import importlib
import json
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# alpha-engine-config-I8217: import the REAL nousergon_lib.pipeline_status.
# completion_marker before the blanket nousergon_lib stub below replaces the
# top-level package — this is the fleet's canonical, tested double-decode
# fix (I8154/I8186), and the whole point of the regression tests further
# down is to exercise that real implementation, not a hand-rolled fake that
# could silently diverge from it again. Once imported, it stays reachable
# via sys.modules regardless of what the stub does to the parent package.
importlib.import_module("nousergon_lib.pipeline_status.completion_marker")

# Stub `nousergon_lib.trading_calendar` + `nousergon_lib.alerts` BEFORE
# importing the handler so test envs without the lib installed still pass.
_lib_pkg = types.ModuleType("nousergon_lib")
_tc_mod = types.ModuleType("nousergon_lib.trading_calendar")
_tc_mod.last_closed_trading_day = MagicMock()
_tc_mod.previous_trading_day = MagicMock()
# The weekly-silence deadman's slot derivation (config#6738) calls these two.
_tc_mod.is_trading_day = MagicMock()
_tc_mod.next_trading_day = MagicMock()
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

# Importable only AFTER `index`, whose own import block puts the in-repo
# scripts/ dir on sys.path for the non-deployed layout. Tested directly
# because its role vocabulary and the watchdog's role FILTER have to agree,
# and nothing asserted that until they silently disagreed in production
# (alpha-engine-config-I7440).
import weekly_sf_silence_deadman as _deadman  # noqa: E402


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
    # Weekday-only calendar default for the silence deadman's slot derivation.
    # Every date these tests pin is in Aug 2026, which carries no NYSE holiday,
    # so weekday-only is exact for them; a test needing a holiday overrides.
    _tc_mod.is_trading_day.reset_mock()
    _tc_mod.is_trading_day.side_effect = _weekday_only_is_trading_day
    _tc_mod.next_trading_day.reset_mock()
    _tc_mod.next_trading_day.side_effect = _weekday_only_next_trading_day
    _alerts_mod.publish.reset_mock()
    _alerts_mod.publish.side_effect = None
    _alerts_mod.publish.return_value = _make_publish_result(sns_ok=True, telegram_ok=True)
    _fd_mod.notify_via_flow_doctor.reset_mock()
    _fd_mod.notify_via_flow_doctor.return_value = True


def _weekday_only_is_trading_day(d: date) -> bool:
    """Mon-Fri calendar stand-in for nousergon_lib.trading_calendar."""
    return d.weekday() < 5


def _weekday_only_next_trading_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


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
    # The same mock stands in for the SSM client in handler-level tests (they
    # patch index.boto3 wholesale, so every boto3.client(...) returns this),
    # so give it a valid declared cadence — otherwise every handler test would
    # exercise the deadman's degraded path instead of its real one.
    client.get_parameter.return_value = {"Parameter": {"Value": "daily"}}
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


# ── role-filtered counting (alpha-engine-config#5597 / #5590) ────────────────────


def _make_role_sfn_client(executions: list) -> MagicMock:
    """SFN mock whose executions carry a pipeline_role.

    ``executions`` is a list of ``(startDate, role_or_None)``. Only the
    FIRST status filter returns rows so the count isn't inflated 5x by the
    status-filter loop — these tests assert exact numbers.
    """
    client = MagicMock()
    seen_status = {"n": 0}
    inputs = {}
    rows = []
    for i, (start, role) in enumerate(executions):
        arn = f"arn:aws:states:us-east-1:1:execution:sf:e{i}"
        rows.append({"startDate": start, "executionArn": arn})
        inputs[arn] = (
            '{"pipeline_role": "%s"}' % role if role is not None else "{}"
        )

    def _list_executions(**kwargs):
        seen_status["n"] += 1
        if seen_status["n"] > 1:
            return {"executions": [], "nextToken": None}
        return {"executions": rows, "nextToken": None}

    def _describe(**kwargs):
        return {"input": inputs.get(kwargs.get("executionArn"), "{}")}

    client.list_executions.side_effect = _list_executions
    client.describe_execution.side_effect = _describe
    return client


def test_exercise_runs_do_not_satisfy_the_saturday_cadence_check():
    """THE regression (alpha-engine-config#5597 / #5590).

    From 2026-07-29 the weekly SF also runs a post-close-chained daily
    EXERCISE run. Five of those in a 7-day window used to satisfy the
    unfiltered "did the Saturday cron fire" count unconditionally — the
    check could never alert again with the cron completely dead.
    """
    now = datetime.now(timezone.utc)
    client = _make_role_sfn_client(
        [(now - timedelta(days=d), "exercise") for d in range(1, 6)]
    )
    seen = index._count_executions_in_window(
        SAT_ARN, index.WINDOW_SECONDS_WEEKLY, client=client,
        role_filter=index.WEEKLY_CADENCE_ROLES,
    )
    assert seen == 0


def test_cadence_and_recovery_roles_do_satisfy_the_check():
    now = datetime.now(timezone.utc)
    for role in sorted(index.WEEKLY_CADENCE_ROLES):
        client = _make_role_sfn_client([(now - timedelta(days=1), role)])
        seen = index._count_executions_in_window(
            SAT_ARN, index.WINDOW_SECONDS_WEEKLY, client=client,
            role_filter=index.WEEKLY_CADENCE_ROLES,
        )
        assert seen == 1, f"{role} should count as the cadence having fired"


def test_untagged_execution_does_not_count_toward_a_filtered_check():
    # The Saturday cron sets pipeline_role="weekly" explicitly, so an
    # untagged execution is a manual run — and a cadence run that lost its
    # role is itself the outage this watchdog should report.
    now = datetime.now(timezone.utc)
    client = _make_role_sfn_client([(now - timedelta(days=1), None)])
    seen = index._count_executions_in_window(
        SAT_ARN, index.WINDOW_SECONDS_WEEKLY, client=client,
        role_filter=index.WEEKLY_CADENCE_ROLES,
    )
    assert seen == 0


def test_exercise_role_is_excluded_from_the_cadence_role_set():
    assert "exercise" not in index.WEEKLY_CADENCE_ROLES
    assert "smoke" not in index.WEEKLY_CADENCE_ROLES
    assert "shell-run" not in index.WEEKLY_CADENCE_ROLES
    assert index.WEEKLY_CADENCE_ROLES == frozenset(
        {"weekly", "watch-rerun", "recovery"}
    )


def test_unfiltered_count_is_unchanged_for_the_other_two_sfs():
    # Weekday/EOD state machines carry one cadence each — no role filter,
    # and therefore no DescribeExecution cost.
    now = datetime.now(timezone.utc)
    client = _make_role_sfn_client([(now - timedelta(hours=1), "exercise")])
    seen = index._count_executions_in_window(WKD_ARN, 24 * 3600, client=client)
    assert seen == 1
    client.describe_execution.assert_not_called()


def test_role_walk_raises_rather_than_returning_a_truncated_count():
    # A truncated walk that found nothing is indistinguishable from a dead
    # cron — it must never be reported as a clean count.
    now = datetime.now(timezone.utc)
    client = _make_role_sfn_client(
        [(now - timedelta(hours=1), "exercise")]
        * (index._MAX_ROLE_DESCRIBES + 1)
    )
    with pytest.raises(RuntimeError, match="DescribeExecution"):
        index._count_executions_in_window(
            SAT_ARN, index.WINDOW_SECONDS_WEEKLY, client=client,
            role_filter=index.WEEKLY_CADENCE_ROLES,
        )


def test_unparseable_input_does_not_raise_and_does_not_count():
    now = datetime.now(timezone.utc)
    client = MagicMock()
    client.list_executions.side_effect = [
        {"executions": [{"startDate": now, "executionArn": "arn:e"}],
         "nextToken": None},
    ] + [{"executions": [], "nextToken": None}] * 8
    client.describe_execution.return_value = {"input": "{not json"}
    seen = index._count_executions_in_window(
        SAT_ARN, index.WINDOW_SECONDS_WEEKLY, client=client,
        role_filter=index.WEEKLY_CADENCE_ROLES,
    )
    assert seen == 0


def test_alert_body_names_the_role_filter():
    # An operator reading "Saturday SF has not executed" while the console
    # shows a same-SF exercise run needs the message to explain both.
    clause = index._role_clause(index.WEEKLY_CADENCE_ROLES)
    assert "pipeline_role" in clause
    assert "exercise" in clause
    assert index._role_clause(None) == ""


def test_handler_passes_the_cadence_role_filter_for_the_saturday_check():
    """Sunday 2026-05-31 — the Saturday check MUST carry the role filter;
    the two single-cadence SFs must not (no DescribeExecution cost)."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 29),
        date(2026, 5, 29),
        date(2026, 5, 29),  # failed-day checks' target lookup (config#6732)
    ]
    # _eod_window_seconds runs even on a skipped EOD check (the window is an
    # argument), and the autouse fixture resets side_effect but not
    # return_value — so pin it rather than inherit whatever ran before.
    _tc_mod.previous_trading_day.return_value = date(2026, 5, 29)
    now = _frozen_now(2026, 5, 31, 14, 0)
    captured = {}
    real_check_sf = index._check_sf

    def _fake_check_sf(**kwargs):
        captured[kwargs["sf_label"]] = kwargs.get("role_filter")
        return real_check_sf(**{**kwargs, "is_watch_day": False})

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3, \
            patch.object(index, "_check_sf", side_effect=_fake_check_sf):
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = _make_sfn_client({})
        index.handler({}, None)

    assert captured["Saturday SF"] == index.WEEKLY_CADENCE_ROLES
    assert captured["Weekday SF"] is None
    assert captured["EOD SF"] is None


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
    assert result.outcome == "CLEAR"


# ── status-blind watchdog fix (alpha-engine-config-I6991) ────────────────
#
# A window containing only FAILED/TIMED_OUT/ABORTED executions must be an
# ALERT, not a "watchdog clear" — the exact regression measured 2026-08-06:
# "watchdog clear: sf=Weekday SF executions_in_window=1" on a run that
# fired at 05:15:41 PT and failed 3.5s later.


def _make_status_filtered_sfn_client(status_to_execs: dict) -> MagicMock:
    """SFN mock that honors ``statusFilter`` — unlike ``_make_sfn_client``
    (which returns the same rows for every status and can therefore never
    exercise a status-aware code path), each status only sees the rows
    explicitly assigned to it.

    ``describe_execution`` answers from each row's own ``_gate_out`` marker
    (set by ``_row(gate_out=True)``): alpha-engine-config-I8057/I8224/I9233 — the
    walk now reads the run-day gate's OWN verdict out of the execution
    output, so a stand-in that returned no output could never exercise the
    gate-skip path at all. Rows carrying no marker describe as a real run
    (no ``weekly_run_day_gate`` key), which is exactly what live data looks
    like for one.
    """
    client = MagicMock()
    outputs = {
        row["executionArn"]: json.dumps(_GATE_OUT_OUTPUT)
        for rows in status_to_execs.values()
        for row in rows
        if row.get("_gate_out")
    }

    def _list_executions(**kwargs):
        status = kwargs.get("statusFilter")
        return {"executions": status_to_execs.get(status, []), "nextToken": None}

    def _describe(**kwargs):
        arn = kwargs.get("executionArn")
        resp = {"input": "{}"}
        if arn in outputs:
            resp["output"] = outputs[arn]
        return resp

    client.list_executions.side_effect = _list_executions
    client.describe_execution.side_effect = _describe
    client.get_parameter.return_value = {"Parameter": {"Value": "daily"}}
    return client


def _make_status_role_sfn_client(rows: "list[tuple[str, object, object]]") -> MagicMock:
    """SFN + role-aware mock. ``rows`` is a list of ``(status, startDate,
    role_or_None)``. Combines the status-filter honoring of
    ``_make_status_filtered_sfn_client`` with the ``describe_execution``
    role lookup of ``_make_role_sfn_client``."""
    client = MagicMock()
    by_status: "dict[str, list]" = {}
    inputs: "dict[str, str]" = {}
    for i, (status, start, role) in enumerate(rows):
        arn = f"arn:aws:states:us-east-1:1:execution:sf:e{i}"
        by_status.setdefault(status, []).append({"startDate": start, "executionArn": arn})
        inputs[arn] = '{"pipeline_role": "%s"}' % role if role is not None else "{}"

    def _list_executions(**kwargs):
        status = kwargs.get("statusFilter")
        return {"executions": by_status.get(status, []), "nextToken": None}

    def _describe(**kwargs):
        return {"input": inputs.get(kwargs.get("executionArn"), "{}")}

    client.list_executions.side_effect = _list_executions
    client.describe_execution.side_effect = _describe
    return client


def test_status_counts_in_window_buckets_by_status():
    now = datetime.now(timezone.utc)
    client = _make_status_filtered_sfn_client(
        {"FAILED": [{"startDate": now - timedelta(hours=1)}]}
    )
    counts = index._status_counts_in_window(WKD_ARN, 24 * 3600, client=client)
    assert counts == {"FAILED": 1}


def test_check_sf_alerts_when_window_has_only_failed_executions():
    """THE regression. A single FAILED execution must NOT produce
    'watchdog clear' — it must alert, distinctly from the never-fired case."""
    now = datetime.now(timezone.utc)
    client = _make_status_filtered_sfn_client(
        {"FAILED": [{"startDate": now - timedelta(seconds=4)}]}
    )
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.checked is True
    assert result.outcome == "FIRED_AND_FAILED"
    assert result.alert_emitted is True
    _alerts_mod.publish.assert_called_once()
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert "fired and FAILED" in call_kwargs["message"]
    assert "FAILED=1" in call_kwargs["message"]
    assert "Weekday SF" in call_kwargs["message"]
    # Distinct dedup key from the never-fired path, so the two conditions
    # can never collapse into the same throttled alert.
    assert "fired-failed" in call_kwargs["dedup_key"]


def test_check_sf_timed_out_and_aborted_also_alert_as_fired_and_failed():
    now = datetime.now(timezone.utc)
    client = _make_status_filtered_sfn_client(
        {
            "TIMED_OUT": [{"startDate": now - timedelta(hours=1)}],
            "ABORTED": [{"startDate": now - timedelta(hours=2)}],
        }
    )
    result = index._check_sf(
        sf_label="EOD SF",
        sf_arn=EOD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.outcome == "FIRED_AND_FAILED"
    assert result.alert_emitted is True
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert "TIMED_OUT=1" in call_kwargs["message"]
    assert "ABORTED=1" in call_kwargs["message"]


def test_check_sf_running_execution_is_healthy_not_a_failure():
    """A still-in-flight RUNNING execution is not a failure — must not alert."""
    now = datetime.now(timezone.utc)
    client = _make_status_filtered_sfn_client(
        {"RUNNING": [{"startDate": now - timedelta(minutes=5)}]}
    )
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.outcome == "CLEAR"
    assert result.alert_emitted is False
    _alerts_mod.publish.assert_not_called()


def test_check_sf_a_later_succeeded_rerun_clears_an_earlier_failure():
    """A same-window rerun that succeeded is evidence the pipeline is
    healthy even though an earlier attempt in the window failed."""
    now = datetime.now(timezone.utc)
    client = _make_status_filtered_sfn_client(
        {
            "FAILED": [{"startDate": now - timedelta(hours=2)}],
            "SUCCEEDED": [{"startDate": now - timedelta(hours=1)}],
        }
    )
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.outcome == "CLEAR"
    assert result.alert_emitted is False


def test_check_sf_zero_executions_is_still_never_fired_not_fired_and_failed():
    """The never-fired path (config-I6991's other named case) is unchanged
    by the status-aware rewrite."""
    client = _make_status_filtered_sfn_client({})
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.outcome == "NEVER_FIRED"
    assert result.alert_emitted is True
    call_kwargs = _alerts_mod.publish.call_args.kwargs
    assert "has not executed" in call_kwargs["message"]
    assert "fired and FAILED" not in call_kwargs["message"]


def test_check_sf_fired_and_failed_applies_to_saturday_with_role_filter():
    """closes-when requires all THREE covered pipelines, not just Weekday —
    Saturday SF's role-filtered walk must also classify a failed cadence
    run as FIRED_AND_FAILED rather than clear."""
    now = datetime.now(timezone.utc)
    client = _make_status_role_sfn_client(
        [("FAILED", now - timedelta(hours=1), "weekly")]
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=index.WINDOW_SECONDS_WEEKLY,
        client=client,
        role_filter=index.WEEKLY_CADENCE_ROLES,
    )
    assert result.outcome == "FIRED_AND_FAILED"
    assert result.alert_emitted is True


def test_check_sf_fired_and_failed_role_filter_excludes_non_cadence_roles():
    """A FAILED execution outside the cadence role set is not evidence the
    cadence fired at all — it must not manufacture a FIRED_AND_FAILED
    alert for a cron that never actually ran."""
    now = datetime.now(timezone.utc)
    client = _make_status_role_sfn_client(
        [("FAILED", now - timedelta(hours=1), "exercise")]
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=index.WINDOW_SECONDS_WEEKLY,
        client=client,
        role_filter=index.WEEKLY_CADENCE_ROLES,
    )
    assert result.outcome == "NEVER_FIRED"


# ── handler — full integration ──────────────────────────────────────────


def test_handler_on_trading_day_checks_weekday_and_eod_skips_saturday():
    """Wednesday 2026-05-27 14:00 UTC. Both Weekday + EOD checked
    (trading day); Saturday skipped (not Sunday)."""
    # _is_trading_day_now needs 2 calls to last_closed_trading_day per call
    # → so for the one call inside handler, we set 2 return values, plus a
    # 3rd for the preopen-buffer canary's own last_closed_trading_day(now)
    # call (config#2412) — same pre-open semantics as the 1st call.
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 26),  # pre-open call at now
        date(2026, 5, 27),  # synthetic 22:00 UTC call → returns today
        date(2026, 5, 26),  # preopen-buffer canary's target-day lookup
        date(2026, 5, 26),  # failed-day checks' target lookup (config#6732)
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
    # 2026-09-30 split (alpha-engine-config-I11269 follow-up): the reconcile
    # half has its own trigger and so its own liveness check, same watch-day.
    assert by_label["EOD reconcile SF"]["checked"] is True
    assert by_label["EOD reconcile SF"]["alert_emitted"] is True
    assert by_label["Saturday SF"]["checked"] is False
    assert "not Sunday" in by_label["Saturday SF"]["skip_reason"]


def test_handler_on_saturday_skips_weekday_and_eod():
    """Saturday 2026-05-30 14:00 UTC. Weekday + EOD skipped (weekend);
    Saturday skipped too (Saturday SF watch-day is Sunday, not Saturday)."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 29),  # Friday close
        date(2026, 5, 29),  # synthetic post-close still Friday
        date(2026, 5, 29),  # failed-day checks' target lookup (config#6732)
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
        date(2026, 5, 29),  # failed-day checks' target lookup (config#6732)
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
    assert by_label["EOD reconcile SF"]["checked"] is False
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
    # is_trading_day_now: today (Tue 5/26) IS a trading day. 3rd value is
    # the preopen-buffer canary's own last_closed_trading_day(now) call
    # (config#2412) — same pre-open semantics as the 1st call.
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 5, 22),  # at 14:00 UTC pre-open, last close was Fri (Mon was holiday)
        date(2026, 5, 26),  # synthetic 22:00 UTC post-close, today is the session
        date(2026, 5, 22),  # preopen-buffer canary's target-day lookup
        date(2026, 5, 22),  # failed-day checks' target lookup (config#6732)
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
        date(2026, 5, 22),  # preopen-buffer canary's target-day lookup
        date(2026, 5, 22),  # failed-day checks' target lookup (config#6732)
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


# ── Preopen schedule-buffer canary (alpha-engine-config#2412) ──────────────
#
# The trigger (WeekdayPipelineSchedule) has been moved earlier twice after
# finishing after the 06:30 PT open: 06:00→05:45 PT (2026-05-19), then
# 05:45→05:15 PT (2026-07-13). These tests pin the finish-time thresholds
# (06:15 hard / 06:10 warn, both America/Los_Angeles) and the deferral rule
# for a day with no SUCCEEDED execution.


def _pt(y, m, d, hh, mm, ss=0):
    """Build a tz-aware UTC datetime from an America/Los_Angeles wall-clock
    time — mirrors the tz-aware datetimes boto3's ListExecutions actually
    returns."""
    return datetime(y, m, d, hh, mm, ss, tzinfo=index.PT_ZONE).astimezone(timezone.utc)


def _make_buffer_client(rows: list) -> MagicMock:
    """SFN mock for the canary's single ``list_executions(statusFilter=
    'SUCCEEDED')`` call. ``rows`` is a list of ``{"startDate":..., "stopDate":
    ...}`` dicts returned verbatim (single page, no pagination)."""
    client = MagicMock()
    client.list_executions.return_value = {"executions": rows, "nextToken": None}
    return client


def _check_preopen_buffer_at(now_utc, rows, *, is_watch_day=True):
    """Run ``_check_preopen_buffer`` with ``index.datetime.now`` pinned to
    ``now_utc``. Needed because ``_iter_succeeded_weekday_executions``'s
    lookback cutoff uses ``datetime.now(timezone.utc)`` directly — the same
    convention this file's pre-existing ``_count_executions_in_window``
    already uses — so a fixture built around a fixed calendar date (rather
    than an offset from real wall-clock time) needs ``now()`` mocked, same
    as every handler-integration test below already does.
    """
    client = _make_buffer_client(rows)
    with patch("index.datetime") as mock_dt:
        mock_dt.now.return_value = now_utc
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.combine = datetime.combine
        mock_dt.fromtimestamp = datetime.fromtimestamp
        return index._check_preopen_buffer(
            now_utc=now_utc, is_watch_day=is_watch_day, client=client
        )


# ── _classify_buffer_severity — per-day threshold classification ──────────


def test_classify_buffer_severity_quiet_before_0610():
    target = date(2026, 6, 3)
    finish_pt = datetime(2026, 6, 3, 6, 8, tzinfo=index.PT_ZONE)
    assert index._classify_buffer_severity(finish_pt, target) is None


def test_classify_buffer_severity_warn_at_0612():
    target = date(2026, 6, 3)
    finish_pt = datetime(2026, 6, 3, 6, 12, tzinfo=index.PT_ZONE)
    assert index._classify_buffer_severity(finish_pt, target) == "warning"


def test_classify_buffer_severity_error_at_0620():
    target = date(2026, 6, 3)
    finish_pt = datetime(2026, 6, 3, 6, 20, tzinfo=index.PT_ZONE)
    assert index._classify_buffer_severity(finish_pt, target) == "error"


def test_classify_buffer_severity_error_after_0630_open():
    target = date(2026, 6, 3)
    finish_pt = datetime(2026, 6, 3, 6, 34, tzinfo=index.PT_ZONE)
    assert index._classify_buffer_severity(finish_pt, target) == "error"


# ── _check_preopen_buffer — integration ────────────────────────────────────


def test_check_preopen_buffer_skips_on_non_watch_day():
    result = index._check_preopen_buffer(
        now_utc=datetime(2026, 6, 6, 14, 0, tzinfo=timezone.utc),  # Saturday
        is_watch_day=False,
        client=MagicMock(),
    )
    assert result.checked is False
    assert "not a NYSE trading day" in result.skip_reason
    assert result.alert_emitted is False


def test_check_preopen_buffer_quiet_finish_before_0610_no_alert():
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(
        now,
        [{"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 8)}],
    )
    assert result.checked is True
    assert result.target_trading_day == target.isoformat()
    assert result.alert_emitted is False
    assert result.alert_severity is None
    assert result.minutes_before_open == pytest.approx(22.0, abs=0.01)
    _alerts_mod.publish.assert_not_called()


def test_check_preopen_buffer_warn_at_0612_emits_warning_severity():
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(
        now,
        [{"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 12)}],
    )
    assert result.alert_emitted is True
    assert result.alert_severity == "warning"
    _alerts_mod.publish.assert_called_once()
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "warning"
    assert "ne-preopen-trading-pipeline" in _alerts_mod.publish.call_args.kwargs["message"]


def test_check_preopen_buffer_hard_at_0620_emits_error_before_open():
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(
        now,
        [{"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 20)}],
    )
    assert result.alert_emitted is True
    assert result.alert_severity == "error"
    message = _alerts_mod.publish.call_args.kwargs["message"]
    assert "MISSED" not in message
    assert "schedule-buffer breach" in message


def test_check_preopen_buffer_after_open_emits_distinct_missed_open_message():
    """Finish AFTER the actual 06:30 PT open gets a message distinct from a
    mere hard-floor breach before open — the whole point of the canary is
    to distinguish 'buffer is thin' from 'we actually missed the open'."""
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(
        now,
        [{"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 34)}],
    )
    assert result.alert_emitted is True
    assert result.alert_severity == "error"
    assert result.minutes_before_open == pytest.approx(-4.0, abs=0.01)
    message = _alerts_mod.publish.call_args.kwargs["message"]
    assert "MISSED THE 06:30 PT OPEN" in message
    assert "AFTER open" in message


def test_check_preopen_buffer_defers_when_no_succeeded_execution():
    """No SUCCEEDED execution for the target trading day → defer to the
    existing Weekday-SF liveness check / SF failure alert. Must NOT
    double-page."""
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(now, [])  # nothing SUCCEEDED
    assert result.checked is True
    assert result.alert_emitted is False
    assert result.trend_alert_emitted is False
    assert "deferred" in result.skip_reason
    _alerts_mod.publish.assert_not_called()
    _fd_mod.notify_via_flow_doctor.assert_not_called()


def test_check_preopen_buffer_ignores_a_same_day_manual_rerun_started_later():
    """Two SUCCEEDED executions on the target day: the 05:15 PT scheduled
    one (finished quiet, 06:05) and a later manual rerun (09:00 PT, finished
    well after). The EARLIEST-started one is treated as the scheduled run —
    no DescribeExecution/pipeline_role dependency (see module docstring for
    why)."""
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 20, 0, tzinfo=timezone.utc)
    result = _check_preopen_buffer_at(
        now,
        [
            {"startDate": _pt(2026, 6, 3, 9, 0), "stopDate": _pt(2026, 6, 3, 9, 40)},
            {"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 5)},
        ],
    )
    assert result.alert_emitted is False
    assert result.finish_pt == datetime(2026, 6, 3, 6, 5, tzinfo=index.PT_ZONE).isoformat()


# ── Rolling 5-day median trend (issue's "rolling average" implementer-
# discretion gotcha) ────────────────────────────────────────────────────


def test_rolling_trend_median_warns_on_creep_even_when_today_is_individually_quiet():
    """None of the 5 days individually crosses the hard OR warn floor on
    its own reading below, but the median of the last 5 is >= 06:10 PT —
    the trend signal catches persistent creep no single-day threshold
    would."""
    target = date(2026, 6, 5)  # Friday
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 5, 14, 0, tzinfo=timezone.utc)
    rows = [
        {"startDate": _pt(2026, 6, 5, 5, 15), "stopDate": _pt(2026, 6, 5, 6, 9)},
        {"startDate": _pt(2026, 6, 4, 5, 15), "stopDate": _pt(2026, 6, 4, 6, 11)},
        {"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 10)},
        {"startDate": _pt(2026, 6, 2, 5, 15), "stopDate": _pt(2026, 6, 2, 6, 12)},
        {"startDate": _pt(2026, 6, 1, 5, 15), "stopDate": _pt(2026, 6, 1, 6, 8)},
    ]
    result = _check_preopen_buffer_at(now, rows)
    # Today (6/5) itself finished 06:09 — below the 06:10 warn floor, quiet
    # on its own.
    assert result.alert_severity is None
    assert result.alert_emitted is False
    # But the 5-day median (06:10) crosses the warn floor → trend alert.
    assert result.trend_days_used == 5
    assert result.trend_alert_emitted is True
    _alerts_mod.publish.assert_called_once()
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "warning"
    assert "TREND" in _alerts_mod.publish.call_args.kwargs["message"]


def test_rolling_trend_median_skipped_with_fewer_than_3_days_of_data():
    target = date(2026, 6, 3)
    _tc_mod.last_closed_trading_day.return_value = target
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)
    rows = [
        {"startDate": _pt(2026, 6, 3, 5, 15), "stopDate": _pt(2026, 6, 3, 6, 9)},
        {"startDate": _pt(2026, 6, 2, 5, 15), "stopDate": _pt(2026, 6, 2, 6, 12)},
    ]
    result = _check_preopen_buffer_at(now, rows)
    assert result.trend_days_used is None
    assert result.trend_alert_emitted is False


# ── DST boundary — America/Los_Angeles thresholds stay correct year-round ──


def test_dst_boundary_thresholds_resolve_correct_utc_offset_pst_vs_pdt():
    """2026-03-08 is the PT spring-forward date (2am -> 3am). The day
    before is PST (UTC-8); the day of (post-transition, market opens well
    after 3am) and after are PDT (UTC-7). 06:30 PT must resolve to the
    correct UTC instant on both sides — a hardcoded UTC offset would be
    off by an hour on one side."""
    pst_day = date(2026, 3, 6)  # Friday, before the Sunday 3/8 transition — PST
    pdt_day = date(2026, 3, 9)  # Monday, after the transition — PDT

    pst_open_utc = datetime.combine(pst_day, index.MARKET_OPEN_TIME_PT, tzinfo=index.PT_ZONE).astimezone(timezone.utc)
    pdt_open_utc = datetime.combine(pdt_day, index.MARKET_OPEN_TIME_PT, tzinfo=index.PT_ZONE).astimezone(timezone.utc)

    assert pst_open_utc.hour == 14 and pst_open_utc.minute == 30  # UTC-8
    assert pdt_open_utc.hour == 13 and pdt_open_utc.minute == 30  # UTC-7


def test_check_preopen_buffer_classifies_correctly_across_dst_boundary():
    """Same 06:20 PT finish, evaluated on a PST date and a PDT date — both
    must classify as a hard-floor breach (06:20 >= 06:15 regardless of
    which side of the DST transition the date falls on)."""
    for target, now_utc_naive_hour in (
        (date(2026, 1, 15), 14),  # PST — 06:00 PT would be 14:00 UTC
        (date(2026, 7, 15), 14),  # PDT — 07:00 PT would be 14:00 UTC
    ):
        _tc_mod.last_closed_trading_day.return_value = target
        now = datetime(target.year, target.month, target.day, now_utc_naive_hour, 0, tzinfo=timezone.utc)
        result = _check_preopen_buffer_at(
            now,
            [
                {
                    "startDate": datetime(target.year, target.month, target.day, 5, 15, tzinfo=index.PT_ZONE),
                    "stopDate": datetime(target.year, target.month, target.day, 6, 20, tzinfo=index.PT_ZONE),
                },
            ],
        )
        assert result.alert_severity == "error", f"failed for {target}"


# ── handler wiring ──────────────────────────────────────────────────────


def test_handler_includes_preopen_buffer_check_in_summary():
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 6, 2),  # pre-open call at now
        date(2026, 6, 3),  # synthetic 22:00 UTC call → today
        date(2026, 6, 2),  # preopen-buffer canary's target-day lookup
        date(2026, 6, 2),  # failed-day checks' target lookup (config#6732)
    ]
    _tc_mod.previous_trading_day.return_value = date(2026, 6, 2)
    now = datetime(2026, 6, 3, 14, 0, tzinfo=timezone.utc)  # Wednesday

    fake_client = _make_sfn_client({
        WKD_ARN: [{"startDate": now - timedelta(hours=1)}],
        EOD_ARN: [{"startDate": now - timedelta(hours=1)}],
    })

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_dt.combine = datetime.combine
        mock_boto3.client.return_value = fake_client

        summary = index.handler({}, None)

    assert "preopen_buffer_check" in summary
    pbc = summary["preopen_buffer_check"]
    assert pbc["checked"] is True
    # fake_client's list_executions ignores statusFilter and returns the
    # same WKD_ARN rows for the canary's SUCCEEDED-only query too, but
    # those rows have no "stopDate" key → no data → deferred, not alerted.
    assert pbc["alert_emitted"] is False


# ── _check_failed_day — prior-day failed-run check (config#6732) ─────────
#
# sf-pipeline-policy §4.1: independent of the SF's own notifier, sensitive
# to started-but-never-succeeded. Zero-execution days belong to the
# liveness checks; DEGRADED-marker days are Option-A visible degrades and
# must not double-page; everything else without a SUCCEEDED execution
# pages at severity=error (marker UNKNOWN ≠ pass).

import json as _json


class _FakeNoSuchKey(Exception):
    def __init__(self):
        super().__init__("NoSuchKey")
        self.response = {"Error": {"Code": "NoSuchKey"}}


def _make_status_aware_sfn_client(rows_by_status: dict) -> MagicMock:
    """Unlike _make_sfn_client, honors statusFilter — required here because
    _statuses_for_day's SUCCEEDED/FAILED distinction IS the check."""
    client = MagicMock()

    def _list_executions(**kwargs):
        rows = rows_by_status.get(kwargs.get("statusFilter"), [])
        return {"executions": rows, "nextToken": None}

    client.list_executions.side_effect = _list_executions
    return client


def _make_s3_marker_client(*, marker: dict | None = None, error: Exception | None = None) -> MagicMock:
    """alpha-engine-config-I8217: production ``_sf_completion/`` objects are
    written by a Step Functions ``States.Format`` body, so the S3 object is
    a JSON string LITERAL containing the JSON object (double-encoded) — NOT
    a single ``json.dumps(marker)``. This fixture used to write the single-
    encoded form, which is why every test built on it kept passing against
    the (until this issue) permanently-broken ``_completion_marker_status``:
    the fixture never exercised the double-decode the real object needs.
    Encoding it the way the SF actually does turns every test below into a
    real regression test for that bug, not a fixture-shaped pass."""
    s3 = MagicMock()
    if error is not None:
        s3.get_object.side_effect = error
    else:
        body = MagicMock()
        body.read.return_value = _json.dumps(_json.dumps(marker)).encode()
        s3.get_object.return_value = {"Body": body}
    return s3


_TARGET = date(2026, 8, 5)
_TARGET_START = datetime(2026, 8, 5, 12, 20, tzinfo=timezone.utc)  # 05:20 PT


def test_failed_day_pages_when_all_executions_failed_and_no_marker():
    client = _make_status_aware_sfn_client({"FAILED": [{"startDate": _TARGET_START}]})
    s3 = _make_s3_marker_client(error=_FakeNoSuchKey())
    result = index._check_failed_day(
        sf_label="Weekday SF", sf_arn=WKD_ARN,
        pipeline_name="ne-preopen-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is True
    assert result.succeeded_on_day == 0
    assert result.marker_status == "ABSENT"
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "error"
    msg = _alerts_mod.publish.call_args.kwargs["message"]
    assert "none SUCCEEDED" in msg
    assert "no completion marker" in msg


def test_failed_day_quiet_on_degraded_marker_option_a():
    """Option-A (config#6692): a deliberate degraded terminal ends FAILED
    with a DEGRADED marker — status-keyed watchers engaged; no double-page."""
    client = _make_status_aware_sfn_client({"FAILED": [{"startDate": _TARGET_START}]})
    s3 = _make_s3_marker_client(marker={"status": "DEGRADED"})
    result = index._check_failed_day(
        sf_label="Weekday SF", sf_arn=WKD_ARN,
        pipeline_name="ne-preopen-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is False
    assert result.marker_status == "DEGRADED"
    _alerts_mod.publish.assert_not_called()


def test_failed_day_quiet_when_day_succeeded_and_never_reads_marker():
    client = _make_status_aware_sfn_client({
        "SUCCEEDED": [{"startDate": _TARGET_START}],
        "FAILED": [{"startDate": _TARGET_START + timedelta(minutes=5)}],
    })
    s3 = MagicMock()
    result = index._check_failed_day(
        sf_label="Weekday SF", sf_arn=WKD_ARN,
        pipeline_name="ne-preopen-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is False
    assert result.succeeded_on_day == 1
    s3.get_object.assert_not_called()
    _alerts_mod.publish.assert_not_called()


def test_failed_day_skips_zero_execution_day_never_fired_is_livenesss_case():
    client = _make_status_aware_sfn_client({})
    result = index._check_failed_day(
        sf_label="EOD SF", sf_arn=EOD_ARN,
        pipeline_name="ne-postclose-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=MagicMock(),
    )
    assert result.alert_emitted is False
    assert result.executions_on_day == 0
    assert "liveness" in result.skip_reason
    _alerts_mod.publish.assert_not_called()


def test_failed_day_unreadable_marker_pages_unknown_is_not_pass():
    client = _make_status_aware_sfn_client({"TIMED_OUT": [{"startDate": _TARGET_START}]})
    s3 = _make_s3_marker_client(error=RuntimeError("s3 5xx"))
    result = index._check_failed_day(
        sf_label="EOD SF", sf_arn=EOD_ARN,
        pipeline_name="ne-postclose-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is True
    assert result.marker_status == "UNREADABLE"
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "error"


def test_completion_marker_status_decodes_real_double_encoded_object():
    """alpha-engine-config-I8217 regression: this is the ACTUAL object body
    read from s3://alpha-engine-research/_sf_completion/ne-weekly-freshness-pipeline/2026-08-22.json
    (fetched read-only 2026-08-29), a real ``watch-rerun`` recovery marker
    for the weekly cycle whose scheduled cron run FAILED at Director after
    5h15m with APITimeoutError. Before the fix this always returned
    UNREADABLE regardless of content; it must resolve the real ``status``."""
    real_body = (
        '"{\\"sf\\":\\"ne-weekly-freshness-pipeline\\",\\"execution_arn\\":'
        '\\"arn:aws:states:us-east-1:711398986525:execution:'
        'ne-weekly-freshness-pipeline:watch-rerun-2026-08-22-3\\",'
        '\\"status\\":\\"SUCCEEDED\\",\\"started_at\\":'
        '\\"2026-08-22T15:53:30.910Z\\",\\"completed_at\\":'
        '\\"2026-08-22T16:08:02.407Z\\",\\"cycle_key\\":\\"2026-08-22\\",'
        '\\"substrate_relaunches\\":0}"'
    )
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = real_body.encode()
    s3.get_object.return_value = {"Body": body}
    status = index._completion_marker_status(
        s3, "ne-weekly-freshness-pipeline", date(2026, 8, 22),
    )
    assert status == "SUCCEEDED"


def test_failed_day_non_degraded_marker_status_still_pages():
    """A marker whose status is not DEGRADED on a day with zero SUCCEEDED
    executions is contradictory state — page, do not trust it."""
    client = _make_status_aware_sfn_client({"ABORTED": [{"startDate": _TARGET_START}]})
    s3 = _make_s3_marker_client(marker={"status": "SUCCESS"})
    result = index._check_failed_day(
        sf_label="Weekday SF", sf_arn=WKD_ARN,
        pipeline_name="ne-preopen-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is True
    assert result.marker_status == "SUCCESS"


def test_failed_day_running_execution_warns_not_errors():
    client = _make_status_aware_sfn_client({
        "RUNNING": [{"startDate": _TARGET_START}],
        "FAILED": [{"startDate": _TARGET_START}],
    })
    result = index._check_failed_day(
        sf_label="EOD SF", sf_arn=EOD_ARN,
        pipeline_name="ne-postclose-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=MagicMock(),
    )
    assert result.alert_emitted is True
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "warning"


def test_failed_day_ignores_executions_from_other_days():
    """A FAILED execution from the day AFTER the target (e.g. today's
    in-flight morning) must not condemn the target day."""
    next_day_start = datetime(2026, 8, 6, 12, 20, tzinfo=timezone.utc)
    client = _make_status_aware_sfn_client({
        "FAILED": [{"startDate": next_day_start}, {"startDate": _TARGET_START}],
        "SUCCEEDED": [{"startDate": _TARGET_START + timedelta(minutes=40)}],
    })
    s3 = MagicMock()
    result = index._check_failed_day(
        sf_label="Weekday SF", sf_arn=WKD_ARN,
        pipeline_name="ne-preopen-trading-pipeline",
        target_date=_TARGET, client=client, s3_client=s3,
    )
    assert result.alert_emitted is False
    assert result.succeeded_on_day == 1
    assert result.executions_on_day == 2  # target-day rows only


# ── Weekly-SF silence deadman (alpha-engine-config#6738) ─────────────────
#
# Dates are pinned in August 2026 (no NYSE holiday that week) so the
# weekday-only calendar stand-in installed by the autouse fixture is exact:
#   Mon 08-03  Tue 08-04  Wed 08-05  Thu 08-06  Fri 08-07  Sat 08-08  Sun 08-09
# 2026-08-05/06 are the two real days postclose never fired, which is the
# outage this check exists to have caught.


_WEEKLY_EXEC_ARN_PREFIX = (
    "arn:aws:states:us-east-1:711398986525:execution:ne-weekly-freshness-pipeline:"
)

# The run-day gate's own verdict, trimmed from a REAL gate-out's output
# (alpha-engine-config-I8057/I9242 — verified live 2026-08-29 against the
# Fri 2026-08-28 02:00 PT scheduled execution
# `c4cc646e-790b-6490-ab1e-b7a1515724d4_468f5b31-8ffc-1a8d-0f7c-b0e33cd01285`,
# `AWS_PROFILE=ne-admin aws stepfunctions describe-execution --query output`).
# Only `Payload.is_weekly_run_day`/`Payload.marker` are load-bearing to the
# classifier (`weekly_sf_silence_deadman._is_weekly_gate_out`); the other
# real fields (`check_date`, `day_name`, `reason`) are kept so this reads as
# the production shape, not an invented minimal stub. The real response also
# carries `ExecutedVersion`/`SdkHttpMetadata`/`SdkResponseMetadata` — omitted
# here as pure AWS SDK noise the classifier never reads.
_GATE_OUT_OUTPUT = {"weekly_run_day_gate": {"Payload": {
    "check_date": "2026-08-28",
    "day_name": "Friday",
    "is_weekly_run_day": False,
    "marker": "NOT_WEEKLY_RUN_DAY",
    "reason": "2026-08-27 is not the week's last session (later session 2026-08-28)",
}}}


def _make_deadman_client(rows, cadence="daily"):
    """Stepfunctions+SSM stand-in for the silence deadman's live fetch.

    ``rows`` — list of ``(day, role, status, duration_seconds)`` or
    ``(day, role, status, duration_seconds, gate_out)``. Each becomes one
    weekly-SF execution whose ``pipeline_role`` is readable ONLY via
    ``describe_execution`` (exactly like production: neither launch path
    passes an explicit execution Name, so the role never appears on the
    list-executions summary).

    ``gate_out`` (default ``False``) — alpha-engine-config-I8057/I9242: the
    silence deadman no longer infers a run-day-gate skip from duration. A
    row must say so explicitly by carrying the gate's own OUTPUT verdict
    (``_GATE_OUT_OUTPUT``), exactly as `weekly_sf_silence_deadman.py` reads
    it live. A short ``duration`` alone no longer means anything to the
    classifier — see `test_silence_short_real_run_is_never_treated_as_a_gate_noop`.
    """
    execs = []
    inputs = {}
    outputs = {}
    for i, row in enumerate(rows):
        day, role, status, duration = row[:4]
        gate_out = row[4] if len(row) > 4 else False
        arn = f"{_WEEKLY_EXEC_ARN_PREFIX}x{i}"
        start = datetime(day.year, day.month, day.day, 20, 30, tzinfo=timezone.utc)
        execs.append({
            "name": f"x{i}",
            "executionArn": arn,
            "status": status,
            "startDate": start,
            "stopDate": start + timedelta(seconds=duration),
        })
        inputs[arn] = json.dumps({"pipeline_role": role} if role else {})
        if gate_out:
            outputs[arn] = json.dumps(_GATE_OUT_OUTPUT)

    def _describe(executionArn):
        resp = {"input": inputs.get(executionArn, "{}")}
        if executionArn in outputs:
            resp["output"] = outputs[executionArn]
        return resp

    client = MagicMock()
    client.list_executions.side_effect = lambda **kw: {
        "executions": execs if kw.get("stateMachineArn") == SAT_ARN else []
    }
    client.describe_execution.side_effect = _describe
    client.get_parameter.return_value = {"Parameter": {"Value": cadence}}
    return client


def _exercise(day, status="SUCCEEDED", duration=1800):
    return (day, "exercise", status, duration)


def test_silence_never_evaluates_a_slot_that_is_not_yet_due():
    """THE false-positive trap. The watchdog fires 14:00 UTC; today's
    exercise slot is chained off today's ~20:00 UTC postclose, so evaluating
    "today" would page every single trading day. The evaluation date is
    yesterday and today's slot must not appear anywhere in the verdict."""
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)  # Friday
    client = _make_deadman_client([])
    result = index._check_weekly_silence(
        now_utc=now, client=client, ssm_client=client
    )
    assert result.evaluation_date == "2026-08-06"
    assert "2026-08-07:exercise" not in result.critical_slots
    assert "2026-08-07:exercise" not in result.gated_off_slots
    # Mon-Thu of that week, and nothing later.
    assert result.critical_slots == (
        "2026-08-03:exercise",
        "2026-08-04:exercise",
        "2026-08-05:exercise",
        "2026-08-06:exercise",
    )


def test_silence_pages_only_the_days_with_no_execution():
    """The real 2026-08-05/06 shape: Mon+Tue chained fine, Wed+Thu produced
    nothing at all."""
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    client = _make_deadman_client([
        _exercise(date(2026, 8, 3)),
        _exercise(date(2026, 8, 4)),
    ])
    result = index._check_weekly_silence(
        now_utc=now, client=client, ssm_client=client
    )
    assert result.checked is True
    assert result.cadence == "daily"
    assert result.ok == 2
    assert result.critical == 2
    assert result.critical_slots == (
        "2026-08-05:exercise",
        "2026-08-06:exercise",
    )
    assert result.alerts_emitted == 2

    keys = [c.kwargs["dedup_key"] for c in _alerts_mod.publish.call_args_list]
    assert keys == [
        "pipeline-watchdog-weekly-silence-exercise-2026-08-05",
        "pipeline-watchdog-weekly-silence-exercise-2026-08-06",
    ]
    for call in _alerts_mod.publish.call_args_list:
        # Slot-scoped key + a window LONGER than the 5-day look-back: one page
        # per silent slot, never a re-page on each of the next four firings.
        assert call.kwargs["dedup_window_min"] == index.SILENCE_DEDUP_WINDOW_MIN
        assert call.kwargs["dedup_window_min"] > index.SILENCE_WINDOW_DAYS * 24 * 60
        assert call.kwargs["severity"] == "error"
        assert call.kwargs["sns_topic_arn"] == index.WATCHDOG_SNS_TOPIC_ARN


def test_silence_gated_off_by_declaration_is_not_reported_as_silence():
    """§2.6's load-bearing distinction: with the declaration set to
    weekly-only, the very same zero executions are GATED_OFF, not CRITICAL,
    and nothing pages."""
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    client = _make_deadman_client([], cadence="weekly-only")
    result = index._check_weekly_silence(
        now_utc=now, client=client, ssm_client=client
    )
    assert result.cadence == "weekly-only"
    assert result.critical == 0
    assert result.critical_slots == ()
    assert result.gated_off == 4
    assert result.gated_off_slots == (
        "2026-08-03:exercise",
        "2026-08-04:exercise",
        "2026-08-05:exercise",
        "2026-08-06:exercise",
    )
    assert result.alerts_emitted == 0
    _alerts_mod.publish.assert_not_called()


def test_silence_expects_the_weekly_run_the_day_after_the_last_session():
    """Sunday firing. The Saturday cron self-gates true on the day AFTER the
    week's last session, so the weekly slot lands on Sat 08-08 — and every
    exercise slot that week is satisfied."""
    now = datetime(2026, 8, 9, 14, 0, tzinfo=timezone.utc)  # Sunday
    client = _make_deadman_client([
        _exercise(date(2026, 8, 3)),
        _exercise(date(2026, 8, 4)),
        _exercise(date(2026, 8, 5)),
        _exercise(date(2026, 8, 6)),
        _exercise(date(2026, 8, 7)),
    ])
    result = index._check_weekly_silence(
        now_utc=now, client=client, ssm_client=client
    )
    assert result.evaluation_date == "2026-08-08"
    assert result.slots_evaluated == 6  # 5 exercise + 1 weekly
    assert result.critical_slots == ("2026-08-08:weekly",)


def test_silence_treats_a_run_day_gate_noop_as_no_weekly_run():
    """A SUCCEEDED weekly-role execution whose OUTPUT carries the gate's
    `is_weekly_run_day: false` verdict is WeeklyRunDayGateChoice's designed
    skip, not a run. Counting it would hide a genuinely dead cron — the
    gotcha the CLI is built around, asserted here through the Lambda entry
    point too. Duration (2s) is incidental scenery, not the signal — see
    `test_silence_short_real_run_is_never_treated_as_a_gate_noop` for the
    mirror case (alpha-engine-config-I8057/I9242)."""
    now = datetime(2026, 8, 9, 14, 0, tzinfo=timezone.utc)
    weekly_noop = (date(2026, 8, 8), "weekly", "SUCCEEDED", 2, True)
    exercises = [_exercise(date(2026, 8, d)) for d in (3, 4, 5, 6, 7)]

    noop_result = index._check_weekly_silence(
        now_utc=now,
        client=_make_deadman_client(exercises + [weekly_noop]),
        ssm_client=_make_deadman_client(exercises + [weekly_noop]),
    )
    assert noop_result.critical_slots == ("2026-08-08:weekly",)
    assert "no-op" in dict(
        (c.kwargs["dedup_key"], c.kwargs["message"])
        for c in _alerts_mod.publish.call_args_list
    )["pipeline-watchdog-weekly-silence-weekly-2026-08-08"]

    _alerts_mod.publish.reset_mock()
    real_weekly = (date(2026, 8, 8), "weekly", "SUCCEEDED", 3600)
    ok_result = index._check_weekly_silence(
        now_utc=now,
        client=_make_deadman_client(exercises + [real_weekly]),
        ssm_client=_make_deadman_client(exercises + [real_weekly]),
    )
    assert ok_result.critical == 0
    assert ok_result.ok == 6
    _alerts_mod.publish.assert_not_called()


def test_silence_short_real_run_is_never_treated_as_a_gate_noop():
    """alpha-engine-config-I8057's measured fact: the fleet's shortest
    genuine weekly execution ran 12.6s. Absence of the `weekly_run_day_gate`
    key — exactly what a real run's `describe_execution` output looks like
    — must never be read as evidence of a gate-out, however fast the
    execution finished. `gate_out` defaults False in `_make_deadman_client`,
    so this row carries no output at all, mirroring live data."""
    now = datetime(2026, 8, 9, 14, 0, tzinfo=timezone.utc)
    short_real_weekly = (date(2026, 8, 8), "weekly", "SUCCEEDED", 12.6)
    exercises = [_exercise(date(2026, 8, d)) for d in (3, 4, 5, 6, 7)]

    result = index._check_weekly_silence(
        now_utc=now,
        client=_make_deadman_client(exercises + [short_real_weekly]),
        ssm_client=_make_deadman_client(exercises + [short_real_weekly]),
    )
    assert result.critical == 0
    assert result.ok == 6
    assert result.critical_slots == ()
    _alerts_mod.publish.assert_not_called()


def test_silence_reports_unknown_and_pages_when_the_declaration_is_unreadable():
    """An unreadable cadence declaration is never rendered as 'no silent
    slots'. It pages with the exact operator command and reports checked=False
    — and does NOT raise, so the five checks that already published keep their
    summary."""
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    ssm = MagicMock()
    ssm.get_parameter.side_effect = RuntimeError(
        "AccessDeniedException: not authorized to perform ssm:GetParameter"
    )
    sfn = _make_deadman_client([])

    result = index._check_weekly_silence(now_utc=now, client=sfn, ssm_client=ssm)

    assert result.checked is False
    assert result.critical == 0
    assert result.ok == 0
    assert index.CADENCE_SSM_PARAM in result.degraded_reason
    assert result.alerts_emitted == 1
    # Never silently downgraded to the local manifest, and never a fetch.
    sfn.list_executions.assert_not_called()
    _alerts_mod.publish.assert_called_once()
    kwargs = _alerts_mod.publish.call_args.kwargs
    assert kwargs["severity"] == "error"
    assert "UNKNOWN" in kwargs["message"]
    assert "--apply-iam" in kwargs["message"]
    assert index.CADENCE_SSM_PARAM in kwargs["message"]


def test_silence_reads_the_cadence_from_ssm_not_the_repo_manifest():
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
    client = _make_deadman_client([], cadence="off")
    result = index._check_weekly_silence(
        now_utc=now, client=client, ssm_client=client
    )
    client.get_parameter.assert_called_once_with(Name=index.CADENCE_SSM_PARAM)
    assert result.cadence == "off"
    assert result.critical == 0


def test_handler_includes_weekly_silence_check_in_summary():
    """Friday 2026-08-07 14:00 UTC — the check runs on every calendar day, so
    the summary always carries its verdict."""
    _tc_mod.last_closed_trading_day.side_effect = [
        date(2026, 8, 6),   # pre-open call at now
        date(2026, 8, 7),   # synthetic post-close → today is a session
        date(2026, 8, 6),   # preopen-buffer canary target
        date(2026, 8, 6),   # failed-day checks target
    ]
    _tc_mod.previous_trading_day.return_value = date(2026, 8, 6)
    now = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)

    with patch("index.datetime") as mock_dt, patch("index.boto3") as mock_boto3:
        mock_dt.now.return_value = now
        mock_dt.side_effect = lambda *a, **k: datetime(*a, **k)
        mock_dt.fromtimestamp = datetime.fromtimestamp
        mock_boto3.client.return_value = _make_sfn_client({})

        summary = index.handler({}, None)

    silence = summary["weekly_silence_check"]
    assert silence["checked"] is True
    assert silence["cadence"] == "daily"
    assert silence["evaluation_date"] == "2026-08-06"
    assert silence["degraded_reason"] is None
    assert silence["critical"] == 4  # Mon-Thu, no executions in the mock
    assert silence["critical_slots"][-1] == "2026-08-06:exercise"


# ── Weekly failed-CYCLE check (alpha-engine-config-I7440) ────────────────
#
# The live false page these cover, in full: cycle day 2026-08-15's Saturday
# cron run FAILED at PredictorBacktest, six `watch-rerun` executions
# recovered it, and the last one SUCCEEDED at 2026-08-16T03:29Z writing a
# `status: SUCCEEDED` marker with `cycle_key: 2026-08-15`. The watchdog
# paged anyway, reporting "1 execution(s) started, none SUCCEEDED".
#
# FOUR independent defects produced that, and each one alone is sufficient —
# so each gets its own test. Fixing three of four would still page.
#   1. role set     — `watch-rerun`/`recovery` discarded, so the §2.5
#                     recovery path was invisible to the detector.
#   2. normalizer   — `normalize_role` collapsed both to None upstream, which
#                     is WHY `WEEKLY_CADENCE_ROLES` could never match.
#   3. cycle key    — matched on UTC start date, so a Sunday rerun of
#                     Saturday's cycle never counted toward it.
#   4. marker clear — only a DEGRADED marker cleared; a SUCCEEDED one paged.


_CYCLE = date(2026, 8, 15)


def _make_weekly_cycle_client(rows):
    """Weekly-SF stand-in for the failed-cycle check's live fetch.

    ``rows`` — dicts with ``start`` (aware UTC datetime), ``role``,
    ``status``, optional ``run_date``, ``duration`` (seconds; a stand-alone
    short duration no longer means anything to the classifier — see
    ``gate_out`` below) and ``gate_out`` (bool, default ``False``:
    alpha-engine-config-I8057/I9242 — a row is a run-day-gate skip only when
    it says so via the gate's own OUTPUT verdict, ``_GATE_OUT_OUTPUT``,
    never via ``duration`` alone). Role and run_date are readable ONLY via
    ``describe_execution``, exactly as in production.
    """
    execs, inputs, outputs = [], {}, {}
    for i, row in enumerate(rows):
        arn = f"{_WEEKLY_EXEC_ARN_PREFIX}c{i}"
        start = row["start"]
        execs.append({
            "name": f"c{i}",
            "executionArn": arn,
            "status": row["status"],
            "startDate": start,
            "stopDate": start + timedelta(seconds=row.get("duration", 1800)),
        })
        payload = {}
        if row.get("role"):
            payload["pipeline_role"] = row["role"]
        if row.get("run_date"):
            payload["run_date"] = row["run_date"]
        inputs[arn] = json.dumps(payload)
        if row.get("gate_out"):
            outputs[arn] = json.dumps(_GATE_OUT_OUTPUT)

    def _describe(executionArn):
        resp = {"input": inputs.get(executionArn, "{}")}
        if executionArn in outputs:
            resp["output"] = outputs[executionArn]
        return resp

    client = MagicMock()
    client.list_executions.side_effect = lambda **kw: {
        "executions": execs if kw.get("stateMachineArn") == SAT_ARN else []
    }
    client.describe_execution.side_effect = _describe
    return client


def _cron_failure():
    """The 2026-08-15 Saturday cron run: real weekly role, FAILED."""
    return {
        "start": datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        "role": "weekly", "status": "FAILED", "run_date": "2026-08-15",
    }


def _rerun_success():
    """The run that actually closed cycle 2026-08-15 — started the NEXT UTC
    day, under the watch-rerun role, carrying the ORIGINAL run_date."""
    return {
        "start": datetime(2026, 8, 16, 3, 21, tzinfo=timezone.utc),
        "role": "watch-rerun", "status": "SUCCEEDED", "run_date": "2026-08-15",
    }


def test_weekly_cycle_recovered_by_a_watch_rerun_does_not_page():
    """THE regression. The cycle failed on the cron and succeeded on a rerun
    the next UTC day — the exact live shape of 2026-08-15/16. Before the
    fix this paged with 'none SUCCEEDED'."""
    client = _make_weekly_cycle_client([_cron_failure(), _rerun_success()])
    s3 = MagicMock()
    result = index._check_failed_day(
        sf_label="Weekly SF", sf_arn=SAT_ARN,
        pipeline_name="ne-weekly-freshness-pipeline",
        target_date=_CYCLE, cadence="weekly", client=client, s3_client=s3,
    )
    assert result.alert_emitted is False
    assert result.succeeded_on_day == 1
    assert result.executions_on_day == 2
    # A cleared day must never spend an S3 read on the marker.
    s3.get_object.assert_not_called()
    _alerts_mod.publish.assert_not_called()


def test_weekly_cycle_counts_the_recovery_role_too():
    """sf-watch's substrate-reclaim relaunch uses role='recovery'. It is in
    WEEKLY_CADENCE_ROLES and must count exactly like watch-rerun — a cycle
    rescued from a spot reclaim is a cycle that succeeded."""
    recovery = dict(_rerun_success(), role="recovery")
    client = _make_weekly_cycle_client([_cron_failure(), recovery])
    result = index._check_failed_day(
        sf_label="Weekly SF", sf_arn=SAT_ARN,
        pipeline_name="ne-weekly-freshness-pipeline",
        target_date=_CYCLE, cadence="weekly", client=client, s3_client=MagicMock(),
    )
    assert result.alert_emitted is False
    assert result.succeeded_on_day == 1


def test_weekly_cycle_keys_on_run_date_not_on_utc_start_date():
    """Defect 3 in isolation: same role, same status, but the rerun starts on
    2026-08-16 UTC. Keying on the start date drops it; keying on run_date —
    the mutex's and the marker's own cycle key — keeps it."""
    client = _make_weekly_cycle_client([_rerun_success()])
    counts = index._weekly_real_statuses_for_day(client, _CYCLE)
    assert counts == {"SUCCEEDED": 1}
    # ...and it belongs to 08-15's cycle, NOT to 08-16's.
    assert index._weekly_real_statuses_for_day(client, date(2026, 8, 16)) == {}


def test_weekly_cycle_without_run_date_falls_back_to_utc_start_date():
    """Records predating run_date being carried must keep the old behaviour
    rather than silently vanishing from every cycle."""
    legacy = {
        "start": datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        "role": "weekly", "status": "FAILED",
    }
    client = _make_weekly_cycle_client([legacy])
    assert index._weekly_real_statuses_for_day(client, _CYCLE) == {"FAILED": 1}


def test_weekly_cycle_still_excludes_exercise_and_gate_noop_runs():
    """The widened role set must not start counting the two things this
    check has always been required to ignore: the daily chained exercise
    run, and WeeklyRunDayGateChoice's designed skip — signalled by the
    gate's OUTPUT verdict, not by duration alone (alpha-engine-config-
    I8057/I9242)."""
    exercise = {
        "start": datetime(2026, 8, 15, 20, 30, tzinfo=timezone.utc),
        "role": "exercise", "status": "SUCCEEDED", "run_date": "2026-08-15",
    }
    gate_noop = {
        "start": datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        "role": "weekly", "status": "SUCCEEDED", "run_date": "2026-08-15",
        "duration": 2, "gate_out": True,
    }
    client = _make_weekly_cycle_client([exercise, gate_noop])
    assert index._weekly_real_statuses_for_day(client, _CYCLE) == {}


def test_weekly_cycle_short_real_run_is_never_treated_as_a_gate_noop():
    """Mirror case: alpha-engine-config-I8057's measured fact is that the
    fleet's shortest genuine weekly execution ran 12.6s. A short SUCCEEDED
    weekly execution with NO gate-out output — exactly what a real run's
    `describe_execution` output looks like — must be counted regardless of
    duration."""
    short_real = {
        "start": datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        "role": "weekly", "status": "SUCCEEDED", "run_date": "2026-08-15",
        "duration": 12.6,
    }
    client = _make_weekly_cycle_client([short_real])
    assert index._weekly_real_statuses_for_day(client, _CYCLE) == {"SUCCEEDED": 1}


def test_weekly_cycle_with_only_failures_still_pages():
    """The check must still do its job: a cycle whose cron AND whose reruns
    all failed, with no marker, is exactly what it exists to catch."""
    failed_rerun = dict(_rerun_success(), status="FAILED")
    client = _make_weekly_cycle_client([_cron_failure(), failed_rerun])
    s3 = _make_s3_marker_client(error=_FakeNoSuchKey())
    result = index._check_failed_day(
        sf_label="Weekly SF", sf_arn=SAT_ARN,
        pipeline_name="ne-weekly-freshness-pipeline",
        target_date=_CYCLE, cadence="weekly", client=client, s3_client=s3,
    )
    assert result.alert_emitted is True
    assert result.executions_on_day == 2
    assert result.succeeded_on_day == 0
    assert _alerts_mod.publish.call_args.kwargs["severity"] == "error"
    assert "cycle day" in _alerts_mod.publish.call_args.kwargs["message"]


def test_succeeded_marker_clears_instead_of_paging():
    """Defect 4 in isolation. Only DEGRADED used to clear, so a marker
    reading SUCCEEDED — the single most authoritative statement that the
    cycle worked — fell through to the error page."""
    client = _make_weekly_cycle_client([_cron_failure()])
    s3 = _make_s3_marker_client(marker={"status": "SUCCEEDED"})
    result = index._check_failed_day(
        sf_label="Weekly SF", sf_arn=SAT_ARN,
        pipeline_name="ne-weekly-freshness-pipeline",
        target_date=_CYCLE, cadence="weekly", client=client, s3_client=s3,
    )
    assert result.alert_emitted is False
    assert result.marker_status == "SUCCEEDED"
    _alerts_mod.publish.assert_not_called()


def test_normalize_role_preserves_the_recovery_vocabulary():
    """Defect 2 at its own layer — the reason WEEKLY_CADENCE_ROLES could
    never match. A filter naming roles the fetch layer cannot emit is a
    filter that is always empty, and nothing said so."""
    for role in ("weekly", "exercise", "watch-rerun", "recovery", "shell-run"):
        assert _deadman.normalize_role(role) == role
    assert _deadman.normalize_role("something-new") is None
    assert _deadman.normalize_role(None) is None


def test_every_weekly_cadence_role_survives_the_fetch_normalizer():
    """The two layers must agree BY CONSTRUCTION, not by coincidence: every
    role the watchdog filters on has to be one the fetch layer can produce.
    This is the assertion whose absence let the two drift apart."""
    for role in index.WEEKLY_CADENCE_ROLES:
        assert _deadman.normalize_role(role) == role, (
            f"WEEKLY_CADENCE_ROLES names {role!r} but normalize_role discards "
            f"it — the filter can never match and the check is disarmed"
        )


# ── IAM contract: the role can read every marker the code reads ──────────
#
# sf-pipeline-policy §2.2: "Identity & permission — every IAM action each
# stage will call ... asserted by contract test against the codified role."
#
# This is the assertion whose absence produced the 2026-08-16 page. The
# weekly pipeline was added to _FAILED_DAY_PIPELINES (I7036) and its marker
# prefix was never added to CompletionMarkerRead, so every weekly marker
# consult returned AccessDenied → UNREADABLE → "UNKNOWN ≠ pass" → error page.
# Nothing failed at merge, and nothing failed on deploy: the grant is only
# exercised on the branch reached when a cycle has zero counted successes,
# so it sat latent until the first day it mattered and then fired as a
# false alarm about the pipeline rather than about itself.


_IAM_POLICY_PATH = Path(__file__).parent / "iam-policy.json"


def _statement(sid):
    doc = _json.loads(_IAM_POLICY_PATH.read_text())
    for stmt in doc["Statement"]:
        if stmt.get("Sid") == sid:
            return stmt
    raise AssertionError(f"iam-policy.json has no statement with Sid={sid!r}")


def test_completion_marker_read_covers_every_pipeline_the_code_reads():
    """Adding a pipeline to _FAILED_DAY_PIPELINES without extending the role
    must fail HERE, at merge — not silently in production on the first day
    the marker branch is reached."""
    granted = _statement("CompletionMarkerRead")["Resource"]
    for _label, _arn, pipeline_name, _cadence in index._FAILED_DAY_PIPELINES:
        expected = (
            f"arn:aws:s3:::{index.MARKER_BUCKET}/_sf_completion/{pipeline_name}/*"
        )
        assert expected in granted, (
            f"{pipeline_name} is in _FAILED_DAY_PIPELINES, so "
            f"_completion_marker_status will GetObject its marker, but "
            f"iam-policy.json's CompletionMarkerRead does not grant "
            f"{expected}. The check would page 'UNREADABLE' on AccessDenied "
            f"— a false alarm about the pipeline, caused by the watchdog."
        )


def test_list_bucket_prefix_condition_covers_the_same_pipelines():
    """The s3:ListBucket prefix condition gates the same reads; a prefix
    granted in one statement and omitted from the other is the same latent
    denial one layer down."""
    prefixes = _statement("DedupMarkerListBucket")["Condition"]["StringLike"]["s3:prefix"]
    for _label, _arn, pipeline_name, _cadence in index._FAILED_DAY_PIPELINES:
        expected = f"_sf_completion/{pipeline_name}/*"
        assert expected in prefixes, (
            f"DedupMarkerListBucket's prefix condition omits {expected} while "
            f"CompletionMarkerRead grants the object read — the two must list "
            f"the same pipelines."
        )


# ── alpha-engine-config-I8045: a run-day gate no-op is not a clear ────────
#
# Real executions of `ne-weekly-freshness-pipeline`, captured live 2026-08-21
# with `AWS_PROFILE=ne-admin aws stepfunctions list-executions`:
#
#   2026-08-21T02:00:49  SUCCEEDED  5.705s   InitializeInput -> ... -> WeeklyRunDaySkip
#   2026-08-20T02:00:49  SUCCEEDED  3.465s   InitializeInput -> ... -> WeeklyRunDaySkip
#   2026-08-15T02:00:49  FAILED     3h53m    died inside PredictorBacktest
#
# The cron fires THU-SAT and the gate self-selects the one correct day, so
# two of three firings terminate SUCCEEDED in seconds having dispatched
# nothing. That is correct behaviour. Reading it as a watchdog clear is not.

_GATEOUT_0821 = 5.705   # measured
_GATEOUT_0820 = 3.465   # measured
_REAL_FAILED = 14003.967  # measured


def _row(
    *,
    seconds_ago: float,
    duration: float,
    arn: str = "arn:aws:states:us-east-1:1:execution:sf:e",
    gate_out: bool = False,
):
    """One ListExecutions row. ``gate_out`` is what MAKES it a run-day-gate
    skip (alpha-engine-config-I8057/I8224/I9233): ``_make_status_filtered_sfn_client``
    turns the marker into the gate's own ``is_weekly_run_day: false`` output
    on ``describe_execution``, which is the only thing the walk now reads.
    ``duration`` is kept on every row because it is real measured data and
    because the tests below assert it is NO LONGER load-bearing."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=seconds_ago)
    return {
        "startDate": start,
        "stopDate": start + timedelta(seconds=duration),
        "executionArn": arn,
        "_gate_out": gate_out,
    }


def test_a_gate_noop_alone_no_longer_reads_as_a_watchdog_clear():
    """RED before I8045: this returned CLEAR off the 3.5s 08-20 gate-out."""
    client = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=3600, duration=_GATEOUT_0820, gate_out=True)]}
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=7 * 24 * 3600,
        client=client,
        gate_noop_aware=True,
    )
    assert result.outcome == "ONLY_GATE_SKIPS", "a designed no-op is not a clear"
    # ...and it does not page: the silence deadman owns that alert.
    assert result.alert_emitted is False


def test_a_gate_noop_no_longer_masks_a_real_failure_in_the_same_window():
    """The operational harm. The 7-day weekly window held BOTH the 08-15
    failure and the 08-20/08-21 gate-outs; the gate-outs cleared the check
    and the failure never paged."""
    client = _make_status_filtered_sfn_client(
        {
            "SUCCEEDED": [
                _row(seconds_ago=3600, duration=_GATEOUT_0821, arn="…:e21", gate_out=True),
                _row(seconds_ago=90000, duration=_GATEOUT_0820, arn="…:e20", gate_out=True),
            ],
            "FAILED": [_row(seconds_ago=6 * 24 * 3600, duration=_REAL_FAILED, arn="…:e15")],
        }
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=7 * 24 * 3600,
        client=client,
        gate_noop_aware=True,
    )
    assert result.outcome == "FIRED_AND_FAILED", (
        "two gate-outs must not clear a window whose only real run failed"
    )
    assert result.alert_emitted is True
    message = _alerts_mod.publish.call_args.kwargs["message"]
    assert "FAILED=1" in message
    # The skips are named, not hidden — the reader can see why the count of
    # executions exceeds the count of real runs.
    assert "GATE_SKIP=2" in message


def test_the_same_window_read_status_blind_still_clears_proving_the_defect():
    """The RED half, executable: with gate_noop_aware off — which is what
    every call site did before I8045 — the identical window reports CLEAR."""
    client = _make_status_filtered_sfn_client(
        {
            "SUCCEEDED": [_row(seconds_ago=3600, duration=_GATEOUT_0821, gate_out=True)],
            "FAILED": [_row(seconds_ago=6 * 24 * 3600, duration=_REAL_FAILED)],
        }
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=7 * 24 * 3600,
        client=client,
        gate_noop_aware=False,
    )
    assert result.outcome == "CLEAR"
    assert result.alert_emitted is False


def test_a_real_weekly_run_is_not_mistaken_for_a_gate_noop():
    """The inverse failure — reporting a genuine success as a skip would
    page through working behaviour, which is the fix that must not happen."""
    client = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=3600, duration=_REAL_FAILED)]}
    )
    result = index._check_sf(
        sf_label="Saturday SF",
        sf_arn=SAT_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=7 * 24 * 3600,
        client=client,
        gate_noop_aware=True,
    )
    assert result.outcome == "CLEAR"
    assert result.alert_emitted is False


def test_the_gate_noop_test_is_the_deadmans_and_not_a_second_copy():
    """Two checks in one Lambda disagreeing about what a no-op is was the
    2026-08-16 defect. The protection is unchanged; the mechanism it guards
    is not. Before alpha-engine-config-I8057 the shared thing was a 90s
    duration CEILING; now it is the gate's own output verdict, and this
    asserts the same one-implementation property against it: the predicate
    is imported from `weekly_sf_silence_deadman`, never restated here."""
    assert index._is_weekly_gate_out is _deadman._is_weekly_gate_out
    assert index._is_gate_noop is _deadman._is_gate_noop
    assert not hasattr(index, "GATE_NOOP_MAX_SECONDS"), (
        "the duration ceiling is gone from both sides; a re-introduced copy "
        "is how the two checks drift apart again"
    )
    assert not hasattr(_deadman, "GATE_NOOP_MAX_SECONDS")


def test_the_measured_durations_no_longer_decide_anything():
    """alpha-engine-config-I8057's measurement, asserted as a property of
    the classifier rather than as a threshold: the longest genuine gate-out
    (5.705s, 2026-08-21) and the shortest execution that really ran (12.6s,
    2026-08-03) both sat inside the old 90s ceiling, so duration could
    misread in EITHER direction. Here the 5.7s row is a gate-out and the
    12.6s row is a real run purely because of what each one's output says —
    and a 5.7s run with NO gate verdict is counted as real."""
    fast_real = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=3600, duration=_GATEOUT_0821)]}
    )
    assert index._status_counts_in_window(
        SAT_ARN, 7 * 24 * 3600, client=fast_real, gate_noop_aware=True
    ) == {"SUCCEEDED": 1}

    slow_gate_out = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=3600, duration=_REAL_FAILED, gate_out=True)]}
    )
    assert index._status_counts_in_window(
        SAT_ARN, 7 * 24 * 3600, client=slow_gate_out, gate_noop_aware=True
    ) == {index.GATE_SKIP: 1}


def test_a_failed_execution_is_never_bucketed_as_a_gate_skip():
    """The gate verdict is asked only of SUCCEEDED executions.
    `WeeklyRunDaySkip` is a SUCCEEDED terminal, so a FAILED run carrying a
    gate-shaped output — a run that gated out and then died on the way to
    its terminal — is still a failure, and must still page."""
    client = _make_status_filtered_sfn_client(
        {"FAILED": [_row(seconds_ago=3600, duration=_GATEOUT_0821, gate_out=True)]}
    )
    assert index._status_counts_in_window(
        SAT_ARN, 7 * 24 * 3600, client=client, gate_noop_aware=True
    ) == {"FAILED": 1}


def test_an_unparseable_execution_output_raises_rather_than_reads_as_real():
    """Fail loud: an output that is present but not JSON means the SF's own
    result contract is broken. Defaulting it to "not a gate-out" would
    silently reclassify every skip as a real run and clear the check."""
    client = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=3600, duration=_GATEOUT_0821)]}
    )
    client.describe_execution.side_effect = lambda **kw: {
        "input": "{}", "output": "not json at all"
    }
    with pytest.raises(RuntimeError, match="not JSON"):
        index._status_counts_in_window(
            SAT_ARN, 7 * 24 * 3600, client=client, gate_noop_aware=True
        )


def test_the_other_two_pipelines_are_untouched_by_gate_bucketing():
    """Only the weekly SF has a run-day gate; the preopen/postclose checks
    must keep their exact prior behaviour."""
    client = _make_status_filtered_sfn_client(
        {"SUCCEEDED": [_row(seconds_ago=60, duration=2.0)]}
    )
    result = index._check_sf(
        sf_label="Weekday SF",
        sf_arn=WKD_ARN,
        is_watch_day=True,
        skip_reason_if_not_watching="(unused)",
        window_seconds=24 * 3600,
        client=client,
    )
    assert result.outcome == "CLEAR"


# ── 2026-09-30 split: the reconcile half is watched like the EOD SF ──────


def test_the_reconcile_sf_is_in_the_prior_day_failed_run_check():
    """alpha-engine-config-I11269 follow-up: EODReconcile, the eod_pnl row and
    the weekly-exercise chain moved to ne-postclose-reconcile-pipeline, so a
    day on which that machine ran and never succeeded must page exactly as the
    EOD SF's did — keyed on its own marker prefix."""
    rows = {row[0]: row for row in index._FAILED_DAY_PIPELINES}
    label, arn, pipeline, cadence = rows["EOD reconcile SF"]
    assert arn == index.RECONCILE_SF_ARN
    assert arn.endswith(":stateMachine:ne-postclose-reconcile-pipeline")
    assert pipeline == "ne-postclose-reconcile-pipeline"
    assert cadence == "trading_day"
    # The post-close SF keeps its own row — it still runs every trading day.
    assert rows["EOD SF"][2] == "ne-postclose-trading-pipeline"


def test_a_reconcile_started_after_midnight_utc_still_buckets_to_its_trading_day():
    """The reconcile starts on the collection's terminal event (~23:30-01:00
    UTC) or the 02:15 UTC backstop — the NEXT UTC day, but the same PT evening.
    _statuses_for_day buckets by PT date, which is what keeps it exact."""
    start = datetime(2026, 9, 30, 2, 20, tzinfo=timezone.utc)  # 19:20 PDT 09-29
    client = MagicMock()
    client.list_executions.side_effect = lambda **kw: (
        {"executions": [{"name": "eod-reconcile-backstop-2026-09-29-1", "startDate": start}]}
        if kw["statusFilter"] == "SUCCEEDED" else {"executions": []}
    )
    counts = index._statuses_for_day(client, index.RECONCILE_SF_ARN, date(2026, 9, 29))
    assert counts == {"SUCCEEDED": 1}
