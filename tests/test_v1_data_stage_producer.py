"""Unit tests for data_gate/producers/v1_data_stage.py and its reader.

alpha-engine-config-I11035 shipped the producer over ONE state machine;
alpha-engine-config-I11265 widened it to all three v1 machines the decoupled
cutover leaves running. Covers:

* the per-machine survey: cutover boundary, data-stage detection via
  `GetExecutionHistory` (a Choice-routed skip is NOT counted), the per-machine
  `scan_cap`, and the refusal of a future cutover (a vacuous zero);
* the ASL guard (I11265 deliverable 3): every Task state in the three committed
  v1 definitions that reaches `alpha-engine-data-spot-dispatcher` is in
  `V1_DATA_STAGE_STATES`, and a reintroduced one fails;
* `HealStartCollection` is deliberately NOT a data stage (I11266 deliverable 4);
* `main()`'s publish and its public-log redaction;
* `read_v1_data_stage_quiet`: per-machine detail, and the three shapes it
  refuses as UNMEASURABLE rather than grading as quiet.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import pathlib

import pytest

from data_gate import cutover as cut
from data_gate import exit_criteria as xc
from data_gate.producers import v1_data_stage as m

UTC = dt.timezone.utc
REPO = pathlib.Path(__file__).resolve().parent.parent
V1_DEFINITIONS = {
    "ne-weekly-freshness-pipeline": REPO / "infrastructure" / "step_function.json",
    "ne-preopen-trading-pipeline": REPO / "infrastructure" / "step_function_daily.json",
    "ne-postclose-trading-pipeline": REPO / "infrastructure" / "step_function_eod.json",
}
DISPATCHER = "alpha-engine-data-spot-dispatcher"
WEEKLY, PREOPEN, POSTCLOSE = m.V1_DATA_STAGE_STATE_MACHINE_ARNS


def _dt(y, mo, d, h=0, mi=0):
    return dt.datetime(y, mo, d, h, mi, tzinfo=UTC)


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _FakeSFN:
    """`list_executions` newest-first per machine (as the real API does);
    `get_execution_history` keyed by executionArn."""

    def __init__(self, executions_by_machine, histories):
        self._executions = executions_by_machine
        self._histories = histories
        self.listed = []

    def get_paginator(self, name):
        outer = self
        if name == "list_executions":
            class _ListPaginator:
                def paginate(self, stateMachineArn, maxResults=100):  # noqa: N803
                    outer.listed.append(stateMachineArn)
                    return [{"executions": outer._executions.get(stateMachineArn, [])}]

            return _ListPaginator()
        if name == "get_execution_history":
            class _HistPaginator:
                def paginate(self, executionArn, maxResults=1000):  # noqa: N803
                    return [{"events": outer._histories.get(executionArn, [])}]

            return _HistPaginator()
        raise AssertionError(f"unexpected paginator {name!r}")


def _entered(*names):
    return [{"stateEnteredEventDetails": {"name": n}} for n in names]


NOW = _dt(2026, 10, 5)
CUTOVER = _dt(2026, 9, 28, 22, 30)


# ── the survey ───────────────────────────────────────────────────────────────


def test_all_three_v1_machines_are_surveyed():
    assert [a.rsplit(":", 1)[-1] for a in m.V1_DATA_STAGE_STATE_MACHINE_ARNS] == list(
        cut.V1_STATE_MACHINE_NAMES
    )
    sfn = _FakeSFN({}, {})
    result = m.count_executions_since_cutover(sfn, cutover=CUTOVER, now=NOW)
    assert sfn.listed == list(m.V1_DATA_STAGE_STATE_MACHINE_ARNS)
    assert [c.state_machine for c in result.machines] == list(cut.V1_STATE_MACHINE_NAMES)


def test_counts_per_machine_only_executions_entering_a_data_stage():
    executions = {
        WEEKLY: [
            {"executionArn": "w2", "startDate": _dt(2026, 10, 3, 9)},  # DataPhase2 still runs
            {"executionArn": "w1", "startDate": _dt(2026, 9, 26, 9)},  # BEFORE cutover
        ],
        PREOPEN: [
            {"executionArn": "p2", "startDate": _dt(2026, 9, 30, 12, 15)},  # quiet
            {"executionArn": "p1", "startDate": _dt(2026, 9, 29, 12, 15)},  # quiet
        ],
        POSTCLOSE: [
            {"executionArn": "e2", "startDate": _dt(2026, 9, 30, 20)},  # botched: data stage live
            {"executionArn": "e1", "startDate": _dt(2026, 9, 29, 20)},  # healed via the new path
        ],
    }
    histories = {
        "w2": _entered("InitCollectionReadinessPoll", "WaitForCollectionManifests", "DataPhase2"),
        "p1": _entered("WaitForCollectionManifests", "CheckCollectionReadiness"),
        "p2": _entered("WaitForCollectionManifests"),
        "e1": _entered("WaitForCollectionManifests", "HealLoopGate", "HealStartCollection"),
        "e2": _entered("LaunchPostMarketDataSpot"),
    }
    result = m.count_executions_since_cutover(
        _FakeSFN(executions, histories), cutover=CUTOVER, now=NOW
    )
    by_name = {c.state_machine: c for c in result.machines}
    assert by_name["ne-weekly-freshness-pipeline"].executions_since_cutover == 1
    assert by_name["ne-weekly-freshness-pipeline"].executions_scanned == 1  # w1 predates
    assert by_name["ne-preopen-trading-pipeline"].executions_since_cutover == 0
    assert by_name["ne-preopen-trading-pipeline"].executions_scanned == 2
    # e1 entered HealStartCollection — the standalone machine, NOT a v1 data stage.
    assert by_name["ne-postclose-trading-pipeline"].data_stage_executions == ("e2",)
    assert result.executions_since_cutover == 2
    assert result.data_stage_executions == ("w2", "e2")


def test_stops_paging_at_the_cutover_boundary():
    executions = {
        PREOPEN: [
            {"executionArn": f"e{i}", "startDate": _dt(2026, 9, 28, 22, 30) + dt.timedelta(days=i)}
            for i in range(5, -1, -1)
        ]
        + [{"executionArn": "old", "startDate": _dt(2026, 9, 28, 12, 15)}]
    }
    result = m.count_executions_since_cutover(_FakeSFN(executions, {}), cutover=CUTOVER, now=NOW)
    # e0 starts exactly AT the cutover and is included; "old" is not scanned.
    assert result.machines[1].executions_scanned == 6


def test_zero_since_cutover_is_a_real_zero_not_a_gap():
    result = m.count_executions_since_cutover(_FakeSFN({}, {}), cutover=CUTOVER, now=NOW)
    assert result.executions_since_cutover == 0
    assert result.executions_scanned == 0
    assert len(result.machines) == 3


def test_scan_cap_is_per_machine_and_raises_naming_the_machine():
    """The two daily machines run ~250 executions a year each: a cutover set
    months back must RAISE, never silently truncate (I11265 deliverable 5)."""
    many = [
        {"executionArn": f"x{i}", "startDate": _dt(2026, 10, 1) - dt.timedelta(hours=i)}
        for i in range(5)
    ]
    # 3 per machine is under a cap of 3 for each: the cap is NOT the total.
    three = {arn: many[:3] for arn in m.V1_DATA_STAGE_STATE_MACHINE_ARNS}
    result = m.count_executions_since_cutover(
        _FakeSFN(three, {}), cutover=_dt(2020, 1, 1), scan_cap=3, now=NOW
    )
    assert result.executions_scanned == 9
    # ...and one machine over it raises, naming that machine.
    over = dict(three, **{POSTCLOSE: many})
    with pytest.raises(RuntimeError, match="ne-postclose-trading-pipeline.*scan_cap=3"):
        m.count_executions_since_cutover(
            _FakeSFN(over, {}), cutover=_dt(2020, 1, 1), scan_cap=3, now=NOW
        )


def test_a_realistic_months_back_cutover_exceeds_the_default_cap():
    """~250 trading-day executions/year: a year-old cutover is > 500 on neither
    daily machine alone, but 2 years is, and it raises rather than truncating."""
    two_years = [
        {"executionArn": f"d{i}", "startDate": NOW - dt.timedelta(days=i * 365 / 250)}
        for i in range(505)
    ]
    with pytest.raises(RuntimeError, match="scan_cap=500"):
        m.count_executions_since_cutover(
            _FakeSFN({PREOPEN: two_years}, {}), cutover=NOW - dt.timedelta(days=800), now=NOW
        )


def test_a_future_cutover_is_refused_not_published_as_quiet():
    with pytest.raises(RuntimeError, match="in the future"):
        m.count_executions_since_cutover(
            _FakeSFN({}, {}), cutover=_dt(2026, 9, 28, 22, 30), now=_dt(2026, 9, 28, 22, 0)
        )


def test_build_metric_carries_the_parsed_shape_and_the_per_machine_breakdown():
    count = m.ExecutionCount(
        (
            m.MachineCount("ne-weekly-freshness-pipeline", 1, 2, ("a",)),
            m.MachineCount("ne-preopen-trading-pipeline", 0, 5, ()),
            m.MachineCount("ne-postclose-trading-pipeline", 1, 5, ("b",)),
        )
    )
    metric = m.build_metric(cutover_utc=cut.CUTOVER_UTC, count=count, as_of=NOW)
    assert metric["cutover_utc"] == cut.CUTOVER_UTC
    assert metric["executions_since_cutover"] == 2
    assert metric["as_of"] == "2026-10-05T00:00:00Z"
    assert metric["per_state_machine"] == {
        "ne-weekly-freshness-pipeline": {"executions_since_cutover": 1, "executions_scanned": 2},
        "ne-preopen-trading-pipeline": {"executions_since_cutover": 0, "executions_scanned": 5},
        "ne-postclose-trading-pipeline": {"executions_since_cutover": 1, "executions_scanned": 5},
    }
    assert metric["data_stage_execution_arns"] == ["a", "b"]
    assert "data_stage_execution_arns" not in m.public_summary(metric)


# ── the ASL guard (I11265 deliverable 3) ─────────────────────────────────────


def _task_states(states):
    for name, state in states.items():
        yield name, state
        for branch in state.get("Branches", []):
            yield from _task_states(branch["States"])
        for key in ("Iterator", "ItemProcessor"):
            if key in state:
                yield from _task_states(state[key]["States"])


def dispatcher_tasks_outside_the_survey(definition: dict) -> list[str]:
    """Task states that reach the data-spot dispatcher but are not surveyed."""
    return sorted(
        name
        for name, state in _task_states(definition["States"])
        if state.get("Type") == "Task"
        and DISPATCHER in json.dumps({k: state.get(k) for k in ("Resource", "Parameters")})
        and name not in m.V1_DATA_STAGE_STATES
    )


def _definition(machine: str) -> dict:
    return json.loads(V1_DEFINITIONS[machine].read_text(encoding="utf-8"))


@pytest.mark.parametrize("machine", sorted(V1_DEFINITIONS))
def test_every_dispatcher_task_in_the_committed_v1_definitions_is_surveyed(machine):
    assert dispatcher_tasks_outside_the_survey(_definition(machine)) == []


@pytest.mark.parametrize("machine", sorted(V1_DEFINITIONS))
def test_a_reintroduced_data_stage_task_fails_the_guard(machine):
    """Mutation: the guard is not vacuous over a definition that happens to
    carry zero dispatcher states — injecting one, renamed, is caught."""
    definition = copy.deepcopy(_definition(machine))
    definition["States"]["LaunchRenamedDataSpot"] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": {"FunctionName": DISPATCHER, "Payload": {"workload": "post-market-data"}},
        "End": True,
    }
    assert dispatcher_tasks_outside_the_survey(definition) == ["LaunchRenamedDataSpot"]
    # ...and the same Task under a surveyed name passes: the guard keys on the set.
    definition["States"]["LaunchPostMarketDataSpot"] = definition["States"].pop(
        "LaunchRenamedDataSpot"
    )
    assert dispatcher_tasks_outside_the_survey(definition) == []


def test_the_seven_daily_data_stage_names_from_the_issue_are_surveyed():
    assert {
        "LaunchMorningEnrichSpot",
        "LaunchMorningArcticAppendSpot",
        "LaunchPostMarketDataSpot",
        "LaunchPostMarketArcticAppendSpot",
        "LaunchEdgarPitFundamentalsDailySpot",
        "HealLaunchPostMarketDataSpot",
        "HealLaunchArcticAppendSpot",
    } <= m.V1_DATA_STAGE_STATES


def test_heal_start_collection_is_deliberately_not_a_data_stage():
    """I11266 deliverable 4: it starts the standalone machine rather than
    running a collector, and BOTH files say so, so a future sweep does not add
    it and red the clause on every healed day."""
    assert "HealStartCollection" not in m.V1_DATA_STAGE_STATES
    states = _definition("ne-postclose-trading-pipeline")["States"]
    assert "HealStartCollection" in states
    assert "V1_DATA_STAGE_STATES" in states["HealStartCollection"]["Comment"]
    source = (REPO / "data_gate" / "producers" / "v1_data_stage.py").read_text(encoding="utf-8")
    assert "HealStartCollection` is deliberately ABSENT" in source


# ── main ─────────────────────────────────────────────────────────────────────


class _FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {}


def _boto(monkeypatch, sfn, s3):
    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            return {"stepfunctions": sfn, "s3": s3}[name]

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())
    monkeypatch.setattr(m, "_now", lambda: NOW)


def test_main_writes_the_metric_document(monkeypatch, capsys):
    arn = "arn:aws:states:us-east-1:111111111111:execution:ne-postclose-trading-pipeline:e1"
    sfn = _FakeSFN(
        {POSTCLOSE: [{"executionArn": arn, "startDate": _dt(2026, 9, 29, 20)}]},
        {arn: _entered("LaunchPostMarketDataSpot")},
    )
    s3 = _FakeS3()
    _boto(monkeypatch, sfn, s3)

    assert m.main([]) == 0
    # Two PUTs: the metric document, then the run record (alpha-engine-config-I11058).
    assert len(s3.puts) == 2
    body = json.loads(s3.puts[0]["Body"])
    assert body["cutover_utc"] == cut.CUTOVER_UTC
    assert body["executions_since_cutover"] == 1
    assert body["per_state_machine"]["ne-postclose-trading-pipeline"]["executions_since_cutover"] == 1
    assert body["data_stage_execution_arns"] == [arn]
    assert s3.puts[0]["Bucket"] == m.DEFAULT_BUCKET
    assert s3.puts[0]["Key"] == m.DEFAULT_KEY

    run_record = json.loads(s3.puts[1]["Body"])
    assert s3.puts[1]["Key"].startswith("data_collection/runs/v1_data_stage/")
    assert run_record["producer"] == "v1_data_stage"
    assert run_record["status"] == "ok"
    assert run_record["detail"]["per_state_machine"]["ne-postclose-trading-pipeline"] == 1

    # alpha-engine-config-I11274: this repo and its logs are PUBLIC. Stdout
    # names the machines and counts, never an execution ARN.
    out = capsys.readouterr().out
    assert "arn:aws" not in out and "111111111111" not in out
    assert m.DEFAULT_KEY in out
    assert "executions_since_cutover=1" in out


def test_main_no_write_prints_all_three_machines_with_a_count(monkeypatch, capsys):
    """I11265's closes-when: `--no-write` prints a document naming all three
    state machines with a per-machine count."""
    s3 = _FakeS3()
    _boto(monkeypatch, _FakeSFN({}, {}), s3)
    assert m.main(["--no-write"]) == 0
    assert s3.puts == []
    out = capsys.readouterr().out
    document = json.loads(out.strip().splitlines()[-1])
    assert set(document["per_state_machine"]) == set(cut.V1_STATE_MACHINE_NAMES)
    for name in cut.V1_STATE_MACHINE_NAMES:
        assert f"{name}=0" in out


def test_main_before_the_cutover_records_an_error_and_raises(monkeypatch):
    s3 = _FakeS3()
    _boto(monkeypatch, _FakeSFN({}, {}), s3)
    monkeypatch.setattr(m, "_now", lambda: _dt(2026, 9, 25))
    with pytest.raises(RuntimeError, match="in the future"):
        m.main([])
    assert len(s3.puts) == 1  # the error run record, never a metric document
    assert json.loads(s3.puts[0]["Body"])["status"] == "error"


def test_main_writes_an_error_run_record_and_still_raises(monkeypatch):
    class _BrokenSFN:
        def get_paginator(self, name):
            raise RuntimeError("boom: sfn unreachable")

    s3 = _FakeS3()
    _boto(monkeypatch, _BrokenSFN(), s3)
    with pytest.raises(RuntimeError, match="boom"):
        m.main([])
    assert len(s3.puts) == 1
    run_record = json.loads(s3.puts[0]["Body"])
    assert s3.puts[0]["Key"].startswith("data_collection/runs/v1_data_stage/")
    assert run_record["status"] == "error"
    assert "boom" in run_record["error"]


# ── the reader ───────────────────────────────────────────────────────────────


class _Store:
    uri = "file://test"

    def __init__(self, documents):
        self.documents = documents

    def list_keys(self, prefix=""):
        return [k for k in sorted(self.documents) if k.startswith(prefix)]

    def get_bytes(self, key):
        if key not in self.documents:
            raise FileNotFoundError(key)
        return json.dumps(self.documents[key]).encode("utf-8")


def _doc(**over):
    doc = {
        "cutover_utc": cut.CUTOVER_UTC,
        "executions_since_cutover": 0,
        "as_of": "2026-10-05T22:00:00Z",
        "per_state_machine": {
            name: {"executions_since_cutover": 0, "executions_scanned": 4}
            for name in cut.V1_STATE_MACHINE_NAMES
        },
    }
    doc.update(over)
    return doc


def _read(doc):
    return xc.read_v1_data_stage_quiet(_Store({xc.V1_DATA_STAGE_KEY: doc}))


def test_reader_quiet_across_all_three_is_met_and_renders_each():
    reading = _read(_doc())
    assert reading.met is True and reading.unmeasurable is False
    for name in cut.V1_STATE_MACHINE_NAMES:
        assert f"{name}=0" in reading.detail


def test_reader_names_the_pipeline_that_is_not_quiet():
    per = _doc()["per_state_machine"]
    per["ne-postclose-trading-pipeline"]["executions_since_cutover"] = 2
    reading = _read(_doc(executions_since_cutover=2, per_state_machine=per))
    assert reading.met is False and reading.unmeasurable is False
    assert "ne-postclose-trading-pipeline=2" in reading.detail


def test_reader_refuses_the_pre_i11265_single_machine_document():
    old = {"cutover_utc": cut.CUTOVER_UTC, "executions_since_cutover": 0,
           "as_of": "2026-10-05T22:00:00Z"}
    reading = _read(old)
    assert reading.unmeasurable is True and reading.met is False
    assert "ne-preopen-trading-pipeline" in reading.detail


def test_reader_refuses_a_document_from_another_cutover_instant():
    reading = _read(_doc(cutover_utc="2026-09-18T00:00:00Z"))
    assert reading.unmeasurable is True and reading.met is False
    assert cut.CUTOVER_UTC in reading.detail


def test_reader_refuses_a_survey_taken_before_its_own_cutover():
    reading = _read(_doc(as_of="2026-09-28T22:00:00Z"))
    assert reading.unmeasurable is True and reading.met is False


def test_reader_refuses_a_document_that_disagrees_with_itself():
    reading = _read(_doc(executions_since_cutover=1))
    assert reading.unmeasurable is True and reading.met is False


def test_reader_absent_document_is_pending_not_quiet():
    reading = xc.read_v1_data_stage_quiet(_Store({}))
    assert reading.unmeasurable is True and reading.met is False
