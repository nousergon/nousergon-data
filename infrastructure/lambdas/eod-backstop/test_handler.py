"""Unit tests for the alpha-engine-eod-backstop Lambda (config#1229, widened
config-I6690, split 2026-09-30).

The 22:30 UTC firing starts the POST-CLOSE SF IFF it is a trading day and the
day's CaptureSnapshot artifact is missing — regardless of trading-box state
(StartTradingInstance boots it either way). Since the alpha-engine-config-I11269
follow-up split, the same Lambda also starts ne-postclose-reconcile-pipeline:
on ne-data-collection-eod's terminal event, and from a 02:15 UTC reconcile
backstop keyed on the eod_pnl row. No-op otherwise; fail-loud on AWS errors.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import index


def _ec2(state: str | None):
    """An EC2 client mock whose trading instance reports ``state`` (None →
    instance absent)."""
    cli = MagicMock()
    if state is None:
        cli.describe_instances.return_value = {"Reservations": []}
    else:
        cli.describe_instances.return_value = {
            "Reservations": [{"Instances": [{"State": {"Name": state}}]}]
        }
    return cli


def _sf(exec_start: datetime | None, name: str = "eod-x"):
    """A Step Functions client mock. ``exec_start`` (a tz-aware datetime) seeds
    one EOD execution under the RUNNING status; None → no executions. ``name``
    matters since alpha-engine-config-I7582: _backstop_already_fired_today only
    counts executions THIS Lambda started (``eod-backstop-*``)."""
    cli = MagicMock()

    def _list(**kwargs):
        if kwargs.get("statusFilter") == "RUNNING" and exec_start is not None:
            return {"executions": [{"name": name, "startDate": exec_start}]}
        return {"executions": []}

    cli.list_executions.side_effect = _list
    cli.start_execution.return_value = {
        "executionArn": "arn:aws:states:us-east-1:711398986525:execution:ne-postclose-trading-pipeline:eod-backstop-x"
    }
    return cli


# ── Detection helpers ─────────────────────────────────────────────────────────


class TestBoxRunning:
    def test_running_true(self):
        assert index._trading_box_running(_ec2("running")) is True

    def test_stopped_false(self):
        assert index._trading_box_running(_ec2("stopped")) is False

    def test_absent_false(self):
        assert index._trading_box_running(_ec2(None)) is False


class TestBackstopAlreadyFiredToday:
    """One retry per day. `_eod_ran_today` used to be the dispatch predicate and
    is deleted (alpha-engine-config-I7582); this replaces only its
    since-midnight window logic, narrowed to THIS Lambda's own dispatches."""

    NOW = datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc)

    def test_backstop_execution_started_today_is_true(self):
        started = datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc)
        assert index._backstop_already_fired_today(
            self.NOW, _sf(started, name="eod-backstop-2026-06-25-1750000000")
        ) is True

    def test_a_daemon_triggered_eod_is_not_a_backstop_dispatch(self):
        """The whole point of the I7582 change: a daemon-triggered EOD that ran
        and produced nothing must NOT suppress the backstop."""
        started = datetime(2026, 6, 25, 20, 15, tzinfo=timezone.utc)
        assert index._backstop_already_fired_today(
            self.NOW, _sf(started, name="eod-2026-06-25-1750000000")
        ) is False

    def test_yesterdays_backstop_is_not_today(self):
        started = datetime(2026, 6, 24, 22, 30, tzinfo=timezone.utc)
        assert index._backstop_already_fired_today(
            self.NOW, _sf(started, name="eod-backstop-2026-06-24-1749000000")
        ) is False

    def test_no_executions_is_false(self):
        assert index._backstop_already_fired_today(self.NOW, _sf(None)) is False


# ── Handler decision matrix ───────────────────────────────────────────────────


class TestHandler:
    TRADING_NOW = datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc)  # Thursday

    def _run(self, *, trading_day=True, box="running", eod_started=None,
             row_present=False, running=False, backstop_fired=False):
        with patch("index.datetime") as dt, \
             patch("index.is_trading_day", return_value=trading_day), \
             patch("index.last_closed_trading_day", return_value=self.TRADING_NOW.date()), \
             patch("index._trading_box_running", return_value=(box == "running")), \
             patch("index._eod_running", return_value=running), \
             patch("index._snapshot_present", return_value=row_present), \
             patch("index._backstop_already_fired_today", return_value=backstop_fired), \
             patch("index._start_eod", return_value="arn:exec:backstop") as start:
            dt.now.return_value = self.TRADING_NOW
            result = index.handler({}, None)
        return result, start

    def test_starts_eod_when_box_up_and_no_row(self):
        # box running + no execution + trading day -> dispatch (existing
        # behavior preserved), tagged triggered_by="backstop".
        result, start = self._run(box="running", eod_started=None)
        assert result["action"] == "started_eod"
        assert result["execution_arn"] == "arn:exec:backstop"
        assert result["box_was_running"] is True
        start.assert_called_once_with(self.TRADING_NOW.date().isoformat(), "backstop")

    def test_starts_eod_when_box_stopped_and_no_eod_today(self):
        # config-I6690: box stopped + no execution + trading day -> dispatch
        # (the widened case — a box-never-started day must not be a silent
        # no-op), tagged triggered_by="backstop-box-stopped".
        result, start = self._run(box="stopped", eod_started=None)
        assert result["action"] == "started_eod"
        assert result["execution_arn"] == "arn:exec:backstop"
        assert result["box_was_running"] is False
        start.assert_called_once_with(self.TRADING_NOW.date().isoformat(), "backstop-box-stopped")

    def test_noop_when_the_snapshot_is_present(self):
        # The artifact decides, regardless of box state. Since the 2026-09-30
        # split the post-close pipeline's artifact is the snapshot; keying on
        # the eod_pnl row would re-dispatch it every day before the reconcile
        # half (18:15 ET collection onward) had written the row.
        for box in ("running", "stopped"):
            result, start = self._run(box=box, row_present=True)
            assert result["action"] == "noop"
            assert result["reason"] == "snapshot_present"
            start.assert_not_called()

    def test_an_execution_that_ran_and_produced_no_snapshot_is_redispatched(self):
        """The I7582 principle — the ARTIFACT decides, not the fact that an
        execution started — on the post-close half: a post-close run that ended
        without writing the snapshot must be redispatched. (The 2026-08-17
        eod_pnl-row case now belongs to the reconcile backstop — see
        TestReconcileBackstop.test_THE_2026_08_17_CASE_*.)"""
        result, start = self._run(box="stopped", row_present=False, running=False)
        assert result["action"] == "started_eod"
        start.assert_called_once()

    def test_noop_while_an_eod_is_still_running(self):
        """A degraded run still inside its self-heal loop must not be
        re-dispatched underneath itself."""
        result, start = self._run(running=True, row_present=False)
        assert result["action"] == "noop"
        assert result["reason"] == "eod_currently_running"
        start.assert_not_called()

    def test_one_retry_per_day_then_page(self):
        """A second miss is a page, not another trading-box boot. Without this
        the outcome predicate would re-dispatch on every firing for as long as
        the row stayed missing."""
        result, start = self._run(row_present=False, backstop_fired=True)
        assert result["action"] == "noop"
        assert result["reason"] == "backstop_already_fired_and_snapshot_still_missing"
        start.assert_not_called()

    def test_running_is_checked_before_the_artifact(self):
        """Cheapest and most decisive first: a RUNNING execution may still be
        about to write the row, so reading S3 to decide is both wasted and
        misleading."""
        with patch("index.datetime") as dt, \
             patch("index.is_trading_day", return_value=True), \
             patch("index.last_closed_trading_day", return_value=self.TRADING_NOW.date()), \
             patch("index._eod_running", return_value=True), \
             patch("index._snapshot_present") as did_its_job, \
             patch("index._start_eod") as start:
            dt.now.return_value = self.TRADING_NOW
            index.handler({}, None)
        did_its_job.assert_not_called()
        start.assert_not_called()

    def test_noop_when_not_a_trading_day(self):
        result, start = self._run(trading_day=False)
        assert result["action"] == "noop" and result["reason"] == "not_a_trading_day"
        start.assert_not_called()

    def test_snapshot_present_checked_before_box_state(self):
        # The artifact predicate is decisive on its own — box state must not
        # even be consulted once the row is confirmed (avoids an unnecessary
        # EC2 describe_instances call on the common no-op path).
        with patch("index.datetime") as dt, \
             patch("index.is_trading_day", return_value=True), \
             patch("index.last_closed_trading_day", return_value=self.TRADING_NOW.date()), \
             patch("index._trading_box_running") as box_running, \
             patch("index._eod_running", return_value=False), \
             patch("index._snapshot_present", return_value=True), \
             patch("index._backstop_already_fired_today", return_value=False), \
             patch("index._start_eod") as start:
            dt.now.return_value = self.TRADING_NOW
            index.handler({}, None)
        box_running.assert_not_called()
        start.assert_not_called()


class TestStartEodInput:
    def test_start_execution_mirrors_daemon_input(self):
        sf = _sf(None)
        index._start_eod("2026-06-25", "backstop", sf)
        kwargs = sf.start_execution.call_args.kwargs
        assert kwargs["stateMachineArn"].endswith("ne-postclose-trading-pipeline")
        assert kwargs["name"].startswith("eod-backstop-2026-06-25-")
        import json
        payload = json.loads(kwargs["input"])
        assert payload["triggered_by"] == "backstop"
        assert payload["pipeline_role"] == "eod"
        assert payload["run_date"] == "2026-06-25"
        assert payload["trading_instance_id"] == [index.TRADING_INSTANCE_ID]

    def test_start_execution_input_identical_when_box_was_stopped(self):
        # config-I6690: the ONLY difference in the box-stopped case is the
        # triggered_by tag — CaptureSnapshot has comfortable timing margin
        # on a freshly-booted box (see module docstring evidence), so the
        # SF input is otherwise identical to the box-was-running dispatch.
        sf = _sf(None)
        index._start_eod("2026-06-25", "backstop-box-stopped", sf)
        import json
        payload = json.loads(sf.start_execution.call_args.kwargs["input"])
        assert payload["triggered_by"] == "backstop-box-stopped"
        assert payload["pipeline_role"] == "eod"
        assert payload["run_date"] == "2026-06-25"
        assert payload["trading_instance_id"] == [index.TRADING_INSTANCE_ID]
        assert payload["ec2_instance_id"] == [index.DASHBOARD_INSTANCE_ID]
        assert payload["sns_topic_arn"] == index.SNS_TOPIC_ARN


# ── Fail-loud ─────────────────────────────────────────────────────────────────


class TestFailLoud:
    def test_describe_instances_error_raises(self):
        cli = MagicMock()
        cli.describe_instances.side_effect = RuntimeError("ec2 down")
        with pytest.raises(RuntimeError):
            index._trading_box_running(cli)

    def test_list_executions_error_raises(self):
        cli = MagicMock()
        cli.list_executions.side_effect = RuntimeError("states down")
        with pytest.raises(RuntimeError):
            index._backstop_already_fired_today(
                datetime(2026, 6, 25, 22, 30, tzinfo=timezone.utc), cli
            )


# ── Post-close predicate: the snapshot (2026-09-30 split) ─────────────────────


class _ClientError(Exception):
    def __init__(self, code: str, status: int = 400):
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class TestSnapshotPresent:
    def test_present(self):
        s3 = MagicMock()
        assert index._snapshot_present("2026-09-30", s3) is True
        s3.head_object.assert_called_once_with(
            Bucket="alpha-engine-research", Key="trades/snapshots/2026-09-30.json"
        )

    def test_404_is_absent(self):
        s3 = MagicMock()
        s3.head_object.side_effect = _ClientError("404", 404)
        assert index._snapshot_present("2026-09-30", s3) is False

    def test_any_other_error_raises(self):
        """This predicate gates a live-IB capture: "could not check" pages via
        the Lambda-error alarm, never silently dispatches or skips."""
        s3 = MagicMock()
        s3.head_object.side_effect = _ClientError("AccessDenied", 403)
        with pytest.raises(_ClientError):
            index._snapshot_present("2026-09-30", s3)


# ── Reconcile trigger: ne-data-collection-eod's terminal event ────────────────


def _collection_event(*, status="SUCCEEDED", name="a1b2c3d4-sched",
                      start=datetime(2026, 9, 29, 22, 15, tzinfo=timezone.utc),
                      arn=None):
    """A Step Functions status-change event for ne-data-collection-eod.
    2026-09-29 22:15 UTC = 18:15 EDT, the collection's cron."""
    return {
        "source": "aws.states",
        "detail-type": "Step Functions Execution Status Change",
        "time": "2026-09-29T23:40:00Z",
        "detail": {
            "stateMachineArn": index.COLLECTION_SF_ARN,
            "executionArn": arn or f"arn:aws:states:us-east-1:711398986525:execution:ne-data-collection-eod:{name}",
            "name": name,
            "status": status,
            "startDate": int(start.timestamp() * 1000),
        },
    }


class TestCollectionTerminal:
    def _run(self, event, *, trading_day=True, reconcile_running=False, start_side_effect=None):
        with patch("index.is_trading_day", return_value=trading_day), \
             patch("index._reconcile_running", return_value=reconcile_running), \
             patch("index._start_reconcile", return_value="arn:exec:reconcile",
                   side_effect=start_side_effect) as start:
            result = index.handler(event, None)
        return result, start

    @pytest.mark.parametrize("status,tag", [
        ("SUCCEEDED", "collection-succeeded"),
        ("FAILED", "collection-failed"),
        ("TIMED_OUT", "collection-timed-out"),
    ])
    def test_every_terminal_status_starts_the_reconcile(self, status, tag):
        """A FAILED/TIMED_OUT collection still starts it: the reconcile's
        precondition probe and self-heal loop are what act on missing data."""
        result, start = self._run(_collection_event(status=status))
        assert result["action"] == "started_reconcile"
        assert result["trading_day"] == "2026-09-29"
        args = start.call_args
        assert args.args[0] == "2026-09-29" and args.args[1] == tag
        assert args.args[2].startswith("eod-reconcile-2026-09-29-")
        assert args.kwargs["collection_execution_arn"].endswith(":a1b2c3d4-sched")

    def test_aborted_does_not_start_it(self):
        result, start = self._run(_collection_event(status="ABORTED"))
        assert result["reason"] == "status_not_a_reconcile_trigger"
        start.assert_not_called()

    def test_the_reconciles_own_heal_executions_never_start_a_reconcile(self):
        """HealStartCollection names its collection v1-eod-heal-*; its terminal
        must not start a second reconcile underneath the loop that launched it.
        The rule's pattern excludes the prefix too — this is the second line."""
        result, start = self._run(_collection_event(name="v1-eod-heal-2026-09-29-eod-reconcile-x"))
        assert result["reason"] == "heal_execution"
        start.assert_not_called()

    def test_another_state_machine_is_ignored(self):
        event = _collection_event()
        event["detail"]["stateMachineArn"] = "arn:aws:states:us-east-1:711398986525:stateMachine:ne-data-collection-morning"
        result, start = self._run(event)
        assert result["reason"] == "not_the_eod_collection"
        start.assert_not_called()

    def test_not_a_trading_day(self):
        result, start = self._run(_collection_event(), trading_day=False)
        assert result["reason"] == "not_a_trading_day"
        start.assert_not_called()

    def test_a_collection_started_before_the_close_is_not_this_evenings(self):
        # 2026-09-29 19:00 UTC = 15:00 EDT, inside the session.
        event = _collection_event(start=datetime(2026, 9, 29, 19, 0, tzinfo=timezone.utc))
        result, start = self._run(event)
        assert result["reason"] == "collection_started_before_the_close"
        start.assert_not_called()

    def test_session_day_is_the_new_york_date_not_the_utc_date(self):
        """An EST collection started 18:15 ET is 23:15 UTC; one finishing late
        is still the same session. A start at 00:30 UTC (19:30 EST) must map to
        the PREVIOUS UTC day's trading session."""
        event = _collection_event(start=datetime(2026, 12, 2, 0, 30, tzinfo=timezone.utc))
        result, start = self._run(event)
        assert result["trading_day"] == "2026-12-01"
        assert start.call_args.args[0] == "2026-12-01"

    def test_noop_while_a_reconcile_is_running(self):
        result, start = self._run(_collection_event(), reconcile_running=True)
        assert result["reason"] == "reconcile_currently_running"
        start.assert_not_called()

    def test_redelivery_is_a_noop_not_a_second_run(self):
        result, _start = self._run(
            _collection_event(), start_side_effect=_ClientError("ExecutionAlreadyExists")
        )
        assert result["reason"] == "already_started_for_this_collection"

    def test_any_other_start_error_raises(self):
        with pytest.raises(_ClientError):
            self._run(_collection_event(), start_side_effect=_ClientError("AccessDeniedException"))

    def test_an_undateable_event_raises(self):
        event = _collection_event()
        del event["detail"]["startDate"]
        del event["time"]
        with pytest.raises(ValueError):
            self._run(event)

    def test_the_execution_name_is_deterministic_and_leaves_room_for_the_heal_replay(self):
        arn = "arn:aws:states:us-east-1:711398986525:execution:ne-data-collection-eod:" + "x" * 80
        a = index._execution_name_for("2026-09-29", arn)
        assert a == index._execution_name_for("2026-09-29", arn)
        assert a != index._execution_name_for("2026-09-29", arn + "y")
        # HealDispatchReplay: States.Format('eod-heal-replay-{}-{}', run_date, name)
        replay = f"eod-heal-replay-2026-09-29-{a}"
        assert len(replay) <= 80, replay


# ── Reconcile backstop (02:15 UTC TUE-SAT) ────────────────────────────────────


class TestReconcileBackstop:
    # 2026-09-30 02:15 UTC = 2026-09-29 22:15 EDT (a Tuesday session).
    NOW = datetime(2026, 9, 30, 2, 15, tzinfo=timezone.utc)

    def _run(self, *, trading_day=True, reconcile_running=False, collection_running=False,
             row_present=False, fired=False):
        with patch("index.datetime") as dt, \
             patch("index.is_trading_day", return_value=trading_day) as itd, \
             patch("index._reconcile_running", return_value=reconcile_running), \
             patch("index._collection_running", return_value=collection_running), \
             patch("index._eod_did_its_job", return_value=row_present), \
             patch("index._reconcile_backstop_already_fired", return_value=fired), \
             patch("index._start_reconcile", return_value="arn:exec:rb") as start, \
             patch("index._start_eod") as start_eod:
            dt.now.return_value = self.NOW
            result = index.handler({"mode": "reconcile-backstop"}, None)
        start_eod.assert_not_called()
        return result, start, itd

    def test_starts_the_reconcile_when_the_row_is_missing(self):
        result, start, itd = self._run()
        assert result["action"] == "started_reconcile"
        assert result["trading_day"] == "2026-09-29"
        itd.assert_called_once_with(datetime(2026, 9, 29).date())
        assert start.call_args.args[0] == "2026-09-29"
        assert start.call_args.args[1] == "reconcile-backstop"
        assert start.call_args.args[2].startswith("eod-reconcile-backstop-2026-09-29-")

    def test_THE_2026_08_17_CASE_a_run_that_produced_no_row_is_redispatched(self):
        """The defect alpha-engine-config-I7582 exists for, re-homed with the
        eod_pnl row it keys on: an execution RAN and wrote no row (2026-08-17:
        DegradedRun, no EODReconcile). The backstop must start the reconcile."""
        result, start, _ = self._run(row_present=False)
        assert result["action"] == "started_reconcile"
        start.assert_called_once()

    def test_noop_when_the_row_is_present(self):
        result, start, _ = self._run(row_present=True)
        assert result["reason"] == "eod_row_present"
        start.assert_not_called()

    def test_noop_while_a_reconcile_is_running(self):
        result, start, _ = self._run(reconcile_running=True)
        assert result["reason"] == "reconcile_currently_running"
        start.assert_not_called()

    def test_stands_down_while_the_collection_is_still_running(self):
        """Its terminal event starts the reconcile; racing it would reconcile
        against a collection that is still writing."""
        result, start, _ = self._run(collection_running=True)
        assert result["reason"] == "collection_still_running"
        start.assert_not_called()

    def test_one_retry_per_day_then_page(self):
        result, start, _ = self._run(fired=True)
        assert result["reason"] == "backstop_already_fired_and_row_still_missing"
        start.assert_not_called()

    def test_not_a_trading_day(self):
        result, start, _ = self._run(trading_day=False)
        assert result["reason"] == "not_a_trading_day"
        start.assert_not_called()


class TestReconcileBackstopAlreadyFired:
    def test_keys_on_the_trading_day_in_the_name(self):
        sf = MagicMock()
        sf.list_executions.side_effect = lambda **kw: (
            {"executions": [{"name": "eod-reconcile-backstop-2026-09-29-1790000000"}]}
            if kw["statusFilter"] == "SUCCEEDED" else {"executions": []}
        )
        assert index._reconcile_backstop_already_fired("2026-09-29", sf) is True
        assert index._reconcile_backstop_already_fired("2026-09-30", sf) is False
        assert all(
            c.kwargs["stateMachineArn"] == index.RECONCILE_SF_ARN
            for c in sf.list_executions.call_args_list
        )

    def test_an_event_triggered_reconcile_is_not_a_backstop_dispatch(self):
        sf = MagicMock()
        sf.list_executions.return_value = {"executions": [{"name": "eod-reconcile-2026-09-29-abcd1234"}]}
        assert index._reconcile_backstop_already_fired("2026-09-29", sf) is False


class TestStartReconcileInput:
    def test_input_is_the_six_field_entry_contract(self):
        sf = MagicMock()
        sf.start_execution.return_value = {"executionArn": "arn:x"}
        index._start_reconcile(
            "2026-09-29", "collection-succeeded", "eod-reconcile-2026-09-29-abcd1234",
            collection_execution_arn="arn:coll", sf_client=sf,
        )
        kwargs = sf.start_execution.call_args.kwargs
        assert kwargs["stateMachineArn"].endswith(":stateMachine:ne-postclose-reconcile-pipeline")
        assert kwargs["name"] == "eod-reconcile-2026-09-29-abcd1234"
        import json
        payload = json.loads(kwargs["input"])
        assert payload == {
            "trading_instance_id": [index.TRADING_INSTANCE_ID],
            "ec2_instance_id": [index.DASHBOARD_INSTANCE_ID],
            "sns_topic_arn": index.SNS_TOPIC_ARN,
            "run_date": "2026-09-29",
            "triggered_by": "collection-succeeded",
            "pipeline_role": "eod",
            "collection_execution_arn": "arn:coll",
        }

    def test_the_backstop_start_carries_no_collection_arn(self):
        sf = MagicMock()
        sf.start_execution.return_value = {"executionArn": "arn:x"}
        index._start_reconcile("2026-09-29", "reconcile-backstop", "n", sf_client=sf)
        import json
        assert "collection_execution_arn" not in json.loads(sf.start_execution.call_args.kwargs["input"])


class TestRunningProbes:
    def test_reconcile_and_collection_probes_ask_their_own_machines(self):
        sf = MagicMock()
        sf.list_executions.return_value = {"executions": [{"name": "x"}]}
        assert index._reconcile_running(sf) is True
        assert sf.list_executions.call_args.kwargs["stateMachineArn"] == index.RECONCILE_SF_ARN
        sf.list_executions.return_value = {"executions": []}
        assert index._collection_running(sf) is False
        assert sf.list_executions.call_args.kwargs["stateMachineArn"] == index.COLLECTION_SF_ARN
