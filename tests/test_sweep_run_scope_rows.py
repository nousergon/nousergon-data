"""scripts/sweep_run_scope_rows.py — the I11984 stored-row sweep.

Driven with in-memory S3 / Step Functions fakes over the three verbatim
captures nousergon-data-PR2066 added to ``weekly-run-scope/fixtures``:

* ``2026-10-03_first_saturday`` — ``watch-rerun-2026-10-02-1``, the run whose
  stored EvalJudge row is wrong (``EvalJudgeSubmitWeekly`` / ENABLED_FAILED);
* ``2026-08-29_eval_judge_failed`` — a genuine judge failure, the shape of a
  stored row the fixed derivation would DEMOTE;
* ``2026-09-26_weekly`` — a clean weekly run.

The properties pinned here are the ones that make it safe to hand an operator:
nothing is written without ``--apply``; ``--apply`` writes only rows the
existing merge rule accepts, conditional on the ETag read; a demotion is
reported and never written; one author's failure is not the cycle's when
another author completed the stage.
"""
from __future__ import annotations

import copy
import gzip
import io
import json

import pytest

from scripts import sweep_run_scope_rows as sweep

_FIXTURES = sweep.LAMBDA_DIR / "fixtures"
rs = sweep.rs

FIRST_SATURDAY = "arn:aws:states:us-east-1:1:execution:ne-weekly-freshness-pipeline:watch-rerun-2026-10-02-1"
GENUINE_FAILURE = "arn:aws:states:us-east-1:1:execution:ne-weekly-freshness-pipeline:sched-2026-08-29"
WEEKLY = "arn:aws:states:us-east-1:1:execution:ne-weekly-freshness-pipeline:sched-2026-09-26"
NEVER_SCOPED = "arn:aws:states:us-east-1:1:execution:ne-weekly-freshness-pipeline:died-early"

_TAGS = {
    FIRST_SATURDAY: "2026-10-03_first_saturday",
    GENUINE_FAILURE: "2026-08-29_eval_judge_failed",
    WEEKLY: "2026-09-26_weekly",
}


def _load(name: str):
    return json.loads(gzip.decompress((_FIXTURES / name).read_bytes()))


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kwargs):
        return iter(self._pages)


class FakeStates:
    """Serves each fixture execution's definition, input and history, with a
    RunScope-less tail appended so the cut is exercised, not assumed."""

    def __init__(self):
        self.calls = []

    def describe_execution(self, executionArn):
        self.calls.append(("describe_execution", executionArn))
        return {"input": json.dumps({"skip_parity": True}), "stateMachineArn": "arn:sm"}

    def describe_state_machine_for_execution(self, executionArn):
        self.calls.append(("describe_state_machine_for_execution", executionArn))
        if executionArn == NEVER_SCOPED:
            return {"definition": json.dumps({"States": {}})}
        return {"definition": json.dumps(_load(f"definition_{_TAGS[executionArn]}.json.gz"))}

    def get_paginator(self, name):
        assert name == "get_execution_history"
        outer = self

        class _HistoryPaginator:
            def paginate(self, executionArn, **_kwargs):
                outer.calls.append(("get_execution_history", executionArn))
                if executionArn == NEVER_SCOPED:
                    return iter([{"events": [{"id": 1, "type": "ExecutionStarted"}]}])
                history = _load(f"history_{_TAGS[executionArn]}_at_run_scope.json.gz")
                last = history[-1]["id"]
                # What GetExecutionHistory returns for a finished run: events
                # AFTER RunScope's entry, which the Lambda never saw.
                tail = [{"id": last + 1, "previousEventId": last, "type": "TaskStateExited",
                         "stateExitedEventDetails": {"name": "RunScope"}},
                        {"id": last + 2, "previousEventId": last + 1, "type": "ExecutionFailed"}]
                half = len(history) // 2
                return iter([{"events": history[:half]}, {"events": history[half:] + tail}])

        return _HistoryPaginator()


class _PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class FakeS3:
    def __init__(self, objects: dict[str, dict], etag: str = '"etag-1"'):
        self.objects = {k: copy.deepcopy(v) for k, v in objects.items()}
        self.etag = etag
        self.puts = []
        self.fail_put = False

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        keys = sorted(self.objects) + ["backtest/2026-10-02/other.json"]
        return _Paginator([{"Contents": [{"Key": k} for k in keys]}])

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(json.dumps(self.objects[Key]).encode()), "ETag": self.etag}

    def put_object(self, **params):
        if self.fail_put:
            raise _PreconditionFailed()
        self.puts.append(params)


def _stored_wrong_branch() -> dict:
    """``backtest/2026-10-02/run_scope.json`` as the old walk wrote it."""
    scope = rs.build_run_scope(
        _load("definition_2026-10-03_first_saturday.json.gz"),
        _load("history_2026-10-03_first_saturday_at_run_scope.json.gz"),
        run_date="2026-10-02", execution_arn=FIRST_SATURDAY,
    )
    scope["stages"]["EvalJudge"] = {
        "disposition": rs.ENABLED_FAILED,
        "entry_state": "EvalJudgeSubmitWeekly",
        "gate": "CheckSkipEvalJudge",
        "reason": "CheckSkipEvalJudge took its default branch but "
                  "EvalJudgeSubmitWeekly was never entered",
    }
    rs.stamp_provenance(scope, FIRST_SATURDAY, "2026-10-03T15:20:45+00:00")
    scope["scope_merge"] = {"merged": False, "note": "the write before the sweep"}
    return rs._recompute(scope)


KEY = "backtest/2026-10-02/run_scope.json"


def test_cut_stops_at_the_first_run_scope_entry():
    history = [
        {"id": 1, "type": "PassStateEntered", "stateEnteredEventDetails": {"name": "A"}},
        {"id": 2, "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "RunScope"}},
        {"id": 3, "type": "TaskStateExited", "stateExitedEventDetails": {"name": "RunScope"}},
        {"id": 4, "type": "TaskStateEntered", "stateEnteredEventDetails": {"name": "RunScope"}},
    ]
    assert [e["id"] for e in sweep.cut_at_scope(history)] == [1, 2]
    assert sweep.cut_at_scope(history[:1]) is None


def test_authors_include_the_last_writer_and_contributors_after_row_authors():
    stored = {
        "execution_arn": "arn:last",
        "contributing_executions": ["arn:a", "arn:c"],
        "stages": {"X": {"recorded_by_execution_arn": "arn:a"}, "Y": {}},
    }
    assert sweep.row_authors(stored) == ["arn:a", "arn:last", "arn:c"]


def test_report_only_by_default_finds_the_wrong_branch_row_and_writes_nothing(capsys):
    s3 = FakeS3({KEY: _stored_wrong_branch()})
    states = FakeStates()
    assert sweep.main([], s3=s3, states=states) == 0
    out = capsys.readouterr().out
    assert s3.puts == []
    assert "UPGRADE          EvalJudge: ENABLED_FAILED / EvalJudgeSubmitWeekly -> " \
           "ENABLED_COMPLETED / EvalJudgeSubmitFirstSaturday" in out
    assert "REPORT ONLY" in out
    # Read-only on Step Functions too: only Describe*/GetExecutionHistory.
    assert {name for name, _ in states.calls} == {
        "describe_execution", "describe_state_machine_for_execution", "get_execution_history",
    }


def test_the_classification_names_only_the_eval_judge_row():
    result = sweep.sweep_one(FakeS3({KEY: _stored_wrong_branch()}), FakeStates(),
                             sweep.BUCKET, KEY)
    changed = [f for f in result["findings"] if f["kind"] not in (sweep.UNCHANGED, sweep.AFTER_SCOPE)]
    assert [(f["stage"], f["kind"]) for f in changed] == [("EvalJudge", sweep.UPGRADE)]
    assert changed[0]["corrected"]["entry_state_source"] == "execution_history"


def test_apply_writes_the_corrected_row_conditionally_and_keeps_everything_else():
    stored = _stored_wrong_branch()
    s3 = FakeS3({KEY: stored})
    assert sweep.main(["--run-date", "2026-10-02", "--stage", "EvalJudge", "--apply"],
                      s3=s3, states=FakeStates()) == 0
    assert len(s3.puts) == 1
    put = s3.puts[0]
    assert put["Key"] == KEY and put["IfMatch"] == '"etag-1"'
    body = json.loads(put["Body"])
    row = body["stages"]["EvalJudge"]
    assert row["disposition"] == rs.ENABLED_COMPLETED
    assert row["entry_state"] == "EvalJudgeSubmitFirstSaturday"
    assert row["recorded_by_execution_arn"] == FIRST_SATURDAY
    assert row["corrected_by"]["sweep"] == sweep.SWEEP_ID
    assert row["corrected_by"]["replaced"]["disposition"] == rs.ENABLED_FAILED
    # Every other row and the artifact's identity are untouched.
    for name, held in stored["stages"].items():
        if name != "EvalJudge":
            assert body["stages"][name] == held
    assert body["execution_arn"] == stored["execution_arn"]
    assert body["run_date"] == "2026-10-02"
    # The denominator is recomputed, and the ledger records the sweep.
    assert "EvalJudge" in body["graded_stages"]
    assert body["counts"][rs.ENABLED_COMPLETED] == stored["counts"][rs.ENABLED_COMPLETED] + 1
    assert body["counts"][rs.ENABLED_FAILED] == stored["counts"][rs.ENABLED_FAILED] - 1
    assert body["scope_merge"]["sweep"]["corrected"] == ["EvalJudge"]
    assert body["scope_merge"]["previous_scope_merge"] == stored["scope_merge"]
    assert body["scope_merge"]["rejected"] == []


def test_a_demotion_is_reported_and_never_written(capsys):
    """A stored COMPLETED whose branch really failed (the 08-29 shape) is
    weaker under the fix; the merge rule refuses it, so --apply leaves it."""
    stored = rs.build_run_scope(
        _load("definition_2026-08-29_eval_judge_failed.json.gz"),
        _load("history_2026-08-29_eval_judge_failed_at_run_scope.json.gz"),
        run_date="2026-08-28", execution_arn=GENUINE_FAILURE,
    )
    stored["stages"]["EvalJudge"] = dict(
        stored["stages"]["EvalJudge"], disposition=rs.ENABLED_COMPLETED,
        entry_state="EvalJudgeSubmitWeekly",
    )
    rs.stamp_provenance(stored, GENUINE_FAILURE, "2026-08-29T15:00:00+00:00")
    key = "backtest/2026-08-28/run_scope.json"
    s3 = FakeS3({key: rs._recompute(stored)})
    assert sweep.main(["--stage", "EvalJudge", "--apply"], s3=s3, states=FakeStates()) == 0
    assert s3.puts == []
    assert "DOWNGRADE        EvalJudge: ENABLED_COMPLETED" in capsys.readouterr().out


def test_another_author_completing_the_stage_keeps_the_cycle_completed():
    """09-18 shape: the scheduled run's branch failed, a rerun completed it.
    The cycle's row is COMPLETED, so there is nothing to demote."""
    stored = rs.build_run_scope(
        _load("definition_2026-08-29_eval_judge_failed.json.gz"),
        _load("history_2026-08-29_eval_judge_failed_at_run_scope.json.gz"),
        run_date="2026-08-28", execution_arn=GENUINE_FAILURE,
    )
    stored["stages"]["EvalJudge"] = dict(
        stored["stages"]["EvalJudge"], disposition=rs.ENABLED_COMPLETED,
        entry_state="EvalJudgeSubmitWeekly",
    )
    rs.stamp_provenance(stored, GENUINE_FAILURE, "2026-08-29T15:00:00+00:00")
    stored["execution_arn"] = WEEKLY  # the last writer, which completed the judge
    stored["contributing_executions"] = [GENUINE_FAILURE]
    key = "backtest/2026-08-28/run_scope.json"
    result = sweep.sweep_one(FakeS3({key: stored}), FakeStates(), sweep.BUCKET, key,
                             stages=["EvalJudge"])
    assert [f["kind"] for f in result["findings"]] == [sweep.UNCHANGED]


def test_a_write_race_stops_without_overwriting(capsys):
    s3 = FakeS3({KEY: _stored_wrong_branch()})
    s3.fail_put = True
    assert sweep.main(["--apply"], s3=s3, states=FakeStates()) == 2
    assert "CONFLICT" in capsys.readouterr().err


def test_an_author_that_never_reached_run_scope_is_reported_not_guessed():
    stored = _stored_wrong_branch()
    stored["execution_arn"] = NEVER_SCOPED
    result = sweep.sweep_one(FakeS3({KEY: stored}), FakeStates(), sweep.BUCKET, KEY)
    assert "never entered RunScope" in result["authors"][NEVER_SCOPED]
    eval_judge = [f for f in result["findings"] if f["stage"] == "EvalJudge"]
    assert eval_judge[0]["kind"] == sweep.UPGRADE  # still corrected from its real author


def test_a_missing_cycle_is_skipped_loudly(capsys):
    s3 = FakeS3({KEY: _stored_wrong_branch()})
    assert sweep.main(["--run-date", "2026-09-30"], s3=s3, states=FakeStates()) == 1
    assert "SKIPPED" in capsys.readouterr().err


def test_a_malformed_run_date_is_rejected():
    with pytest.raises(SystemExit):
        sweep.main(["--run-date", "10/02"], s3=FakeS3({}), states=FakeStates())


def test_listing_only_picks_run_scope_artifacts():
    s3 = FakeS3({KEY: {}})
    assert sweep.stored_keys(s3, sweep.BUCKET, None) == [KEY]
