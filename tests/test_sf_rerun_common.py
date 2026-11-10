"""Hermetic tests for the shared Step Functions rerun helpers."""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol, cast

import pytest

Record = dict[str, object]


class RerunHelpers(Protocol):
    def entered_states(self, events: list[Record]) -> set[str]: ...
    def execution_input(self, events: list[Record]) -> Record: ...
    def initialize_input_output(self, events: list[Record]) -> Record | None: ...
    def apply_normalized_run_date_output(self, events: list[Record]) -> Record | None: ...
    def derive_run_date(self, events: list[Record], start_time: datetime | None) -> tuple[str, str]: ...
    def derive_calendar_date(self, events: list[Record], start_time: datetime | None) -> tuple[str, str]: ...
    def _walk_states(self, states: Record) -> Iterator[tuple[str, Record]]: ...
    def _rule_role_values(self, rule: Record) -> tuple[bool, set[str]]: ...
    def verify_skip_flags_live(
        self, definition: Record, role: str, *, sf_label: str = ..., script_path: str = ...,
    ) -> None: ...
    def fetch_history(self, sf: object, execution_arn: str) -> list[Record]: ...
    def list_all_executions(
        self, sf: object, sm_arn: str, status_filter: str | None = ..., cap: int = ...,
    ) -> list[Record]: ...
    def effective_run_date_of(self, sf: object, execution: Record) -> str: ...


@pytest.fixture()
def mod() -> RerunHelpers:
    script = Path(__file__).resolve().parents[1] / "scripts" / "sf_rerun_common.py"
    spec = importlib.util.spec_from_file_location("sf_rerun_common", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(RerunHelpers, cast(object, module))


def history(orig: Record, normalized: Record, initialized: Record) -> list[Record]:
    return [
        {"executionStartedEventDetails": {"input": json.dumps(orig)}},
        {"stateExitedEventDetails": {"name": "InitializeInput", "output": json.dumps(initialized)}},
        {"stateExitedEventDetails": {"name": "ApplyNormalizedRunDate", "output": json.dumps(normalized)}},
    ]


START = datetime(2026, 11, 10, 23, 30, tzinfo=timezone(timedelta(hours=-5)))


def test_entered_states(mod: RerunHelpers) -> None:
    assert mod.entered_states([]) == set()
    assert mod.entered_states([
        {"stateEnteredEventDetails": {"name": "First"}},
        {"stateExitedEventDetails": {"name": "Ignored"}},
        {"stateEnteredEventDetails": {"name": "Second"}},
        {"stateEnteredEventDetails": {"name": "First"}},
        {},
    ]) == {"First", "Second"}


@pytest.mark.parametrize("details, expected", [
    ({"input": '{"run_date": "2026-11-01"}'}, {"run_date": "2026-11-01"}),
    ({"input": ""}, {}), ({}, {}),
])
def test_execution_input(mod: RerunHelpers, details: Record, expected: Record) -> None:
    assert mod.execution_input([{}, {"executionStartedEventDetails": details}]) == expected


def test_execution_input_missing_start(mod: RerunHelpers) -> None:
    with pytest.raises(SystemExit, match="^FATAL"):
        mod.execution_input([{"stateEnteredEventDetails": {"name": "Other"}}])


@pytest.mark.parametrize("name", ["InitializeInput", "ApplyNormalizedRunDate"])
@pytest.mark.parametrize("output, expected", [
    ('{"run_date": "2026-11-01"}', {"run_date": "2026-11-01"}),
    ("invalid JSON", None), ("null", None), ("", None), (None, None),
])
def test_state_outputs_first_match(
    mod: RerunHelpers, name: str, output: str | None, expected: Record | None,
) -> None:
    parse = mod.initialize_input_output if name == "InitializeInput" else mod.apply_normalized_run_date_output
    details: Record = {"name": name}
    if output is not None:
        details["output"] = output
    assert parse([]) is None
    assert parse([{"stateExitedEventDetails": {"name": "Other", "output": "{}"}}]) is None
    assert parse([
        {"stateEnteredEventDetails": {"name": name}},
        {"stateExitedEventDetails": {"name": "Other", "output": "{}"}},
        {"stateExitedEventDetails": details},
        {"stateExitedEventDetails": {"name": name, "output": '{"later": true}'}},
    ]) == expected


@pytest.mark.parametrize("tier, provenance", [
    (0, "explicit run_date"), (1, "ApplyNormalizedRunDate"),
    (2, "InitializeInput"), (3, "FALLBACK"),
])
def test_run_date_precedence(mod: RerunHelpers, tier: int, provenance: str) -> None:
    dates = ["2026-11-01", "2026-11-02", "2026-11-03", "2026-11-11"]
    sources: list[Record] = [
        {"run_date": date} if index >= tier else {}
        for index, date in enumerate(dates[:3])
    ]
    date, source = mod.derive_run_date(history(*sources), START)
    assert date == dates[tier]
    assert provenance in source


@pytest.mark.parametrize("invalid", [None, "", 123, False, [], {}])
@pytest.mark.parametrize("tier", [0, 1, 2])
def test_invalid_run_dates_fall_through(mod: RerunHelpers, invalid: object, tier: int) -> None:
    sources: list[Record] = [{}, {}, {}]
    sources[tier] = {"run_date": invalid}
    if tier < 2:
        sources[tier + 1] = {"run_date": "2026-11-04"}
    date, source = mod.derive_run_date(history(*sources), START)
    assert date == ("2026-11-04" if tier < 2 else "2026-11-11")
    assert ["ApplyNormalizedRunDate", "InitializeInput", "FALLBACK"][tier] in source


@pytest.mark.parametrize("tier, provenance", [
    (0, "explicit calendar_date"), (1, "ApplyNormalizedRunDate"),
    (2, "InitializeInput merged output calendar_date"),
    (3, "InitializeInput merged output run_date"), (4, "explicit run_date"), (5, "FALLBACK"),
])
def test_calendar_date_precedence(mod: RerunHelpers, tier: int, provenance: str) -> None:
    orig: Record = {}
    normalized: Record = {}
    initialized: Record = {}
    fields = [(orig, "calendar_date"), (normalized, "calendar_date"),
              (initialized, "calendar_date"), (initialized, "run_date"), (orig, "run_date")]
    dates = [f"2026-11-0{day}" for day in range(1, 6)] + ["2026-11-11"]
    for index, (record, key) in enumerate(fields):
        if index >= tier:
            record[key] = dates[index]
    date, source = mod.derive_calendar_date(history(orig, normalized, initialized), START)
    assert date == dates[tier]
    assert provenance in source


def test_dates_unavailable(mod: RerunHelpers) -> None:
    events = history({}, {}, {})
    with pytest.raises(SystemExit, match="^FATAL: cannot derive run_date"):
        mod.derive_run_date(events, None)
    with pytest.raises(SystemExit, match="^FATAL: cannot derive calendar_date"):
        mod.derive_calendar_date(events, None)


def test_walk_states(mod: RerunHelpers) -> None:
    leaf: Record = {"Type": "Pass"}
    states: Record = {
        "Parallel": {"Type": "Parallel", "Branches": [
            {"States": {"IteratorMap": {"Type": "Map", "Iterator": {"States": {"Old": leaf}}}}},
            {"States": {"Nested": {"Type": "Parallel", "Branches": [
                {"States": {"ProcessorMap": {"Type": "Map", "ItemProcessor": {"States": {"New": leaf}}}}},
            ]}}},
        ]},
        "End": leaf,
    }
    walked = list(mod._walk_states(states))
    assert [name for name, _ in walked] == ["Parallel", "IteratorMap", "Old", "Nested", "ProcessorMap", "New", "End"]
    assert dict(walked)["New"] == leaf
    assert list(mod._walk_states({})) == []


@pytest.mark.parametrize("rule, expected", [
    ({"Variable": "$.pipeline_role", "StringEquals": "replay"}, (True, {"replay"})),
    ({"And": [{"Variable": "$.skip", "BooleanEquals": True}, {"Or": [
        {"Variable": "$.pipeline_role", "StringEquals": "replay"},
        {"Not": {"Variable": "$.pipeline_role", "StringEquals": "daily"}},
    ]}]}, (True, {"replay", "daily"})),
    ({"Variable": "$.pipeline_role", "IsPresent": True}, (True, set())),
    ({"Not": {"Variable": "$.other", "StringEquals": "replay"}}, (False, set())),
    ({}, (False, set())),
])
def test_rule_role_values(mod: RerunHelpers, rule: Record, expected: tuple[bool, set[str]]) -> None:
    assert mod._rule_role_values(rule) == expected


def test_verify_skip_flags_live(mod: RerunHelpers) -> None:
    gated: Record = {"And": [
        {"Variable": "$.skip", "BooleanEquals": True},
        {"Variable": "$.pipeline_role", "StringEquals": "replay"},
    ]}
    mod.verify_skip_flags_live({}, "daily")
    mod.verify_skip_flags_live({"States": {
        "CheckSkipPlain": {"Type": "Choice", "Choices": [{"Variable": "$.skip", "BooleanEquals": True}]},
        "Other": {"Type": "Choice", "Choices": [gated]},
        "CheckSkipPass": {"Type": "Pass", "Choices": [gated]},
    }}, "daily")
    definition: Record = {"States": {"Map": {"Type": "Map", "ItemProcessor": {"States": {
        "CheckSkipStage": {"Type": "Choice", "Choices": [gated]},
    }}}}}
    mod.verify_skip_flags_live(definition, "replay")
    with pytest.raises(SystemExit, match="role gating") as exc:
        mod.verify_skip_flags_live(definition, "daily", sf_label="custom SF", script_path="scripts/custom.py")
    message = str(exc.value)
    assert "custom SF" in message
    assert "scripts/custom.py" in message
    assert "CheckSkipStage" in message


class FakeSF:
    def __init__(self, pages: list[Record]) -> None:
        self.pages = pages
        self.calls: list[Record] = []

    def get_execution_history(self, **kwargs: object) -> Record:
        self.calls.append(kwargs)
        return self.pages[len(self.calls) - 1]

    def list_executions(self, **kwargs: object) -> Record:
        self.calls.append(kwargs)
        return self.pages[len(self.calls) - 1]


def test_fetch_history(mod: RerunHelpers) -> None:
    sf = FakeSF([{"events": [{"id": 1}], "nextToken": "page2"}, {"events": [{"id": 2}]}])
    assert mod.fetch_history(sf, "execution") == [{"id": 1}, {"id": 2}]
    assert sf.calls == [
        {"executionArn": "execution", "maxResults": 1000},
        {"executionArn": "execution", "maxResults": 1000, "nextToken": "page2"},
    ]


@pytest.mark.parametrize("status", [None, "FAILED"])
def test_list_all_executions(mod: RerunHelpers, status: str | None) -> None:
    sf = FakeSF([{"executions": [{"id": 1}], "nextToken": "page2"}, {"executions": [{"id": 2}]}])
    assert mod.list_all_executions(sf, "machine", status) == [{"id": 1}, {"id": 2}]
    first: Record = {"stateMachineArn": "machine", "maxResults": 200}
    if status is not None:
        first["statusFilter"] = status
    assert sf.calls == [first, {**first, "nextToken": "page2"}]


@pytest.mark.parametrize("cap, expected_calls", [(0, 0), (1, 1), (3, 2)])
def test_list_cap_stops_paging(mod: RerunHelpers, cap: int, expected_calls: int) -> None:
    sf = FakeSF([
        {"executions": [{"id": 1}, {"id": 2}], "nextToken": "page2"},
        {"executions": [{"id": 3}, {"id": 4}], "nextToken": "must-not-fetch"},
    ])
    assert mod.list_all_executions(sf, "machine", cap=cap) == [{"id": n} for n in range(1, cap + 1)]
    assert len(sf.calls) == expected_calls


class DescribeSF:
    def __init__(self, raw_input: str, fail: bool) -> None:
        self.raw_input = raw_input
        self.fail = fail
        self.calls: list[str] = []

    def describe_execution(self, *, executionArn: str) -> Record:
        self.calls.append(executionArn)
        if self.fail:
            raise RuntimeError("fake describe failure")
        return {"input": self.raw_input}


@pytest.mark.parametrize("raw_input, fail, expected", [
    ('{"run_date": "2026-11-01"}', False, "2026-11-01"),
    ("{}", False, "2026-11-11"),
    ("invalid JSON", False, "2026-11-11"),
    ("{}", True, "2026-11-11"),
])
def test_effective_run_date_of(
    mod: RerunHelpers, capsys: pytest.CaptureFixture[str], raw_input: str, fail: bool, expected: str,
) -> None:
    sf = DescribeSF(raw_input, fail)
    assert mod.effective_run_date_of(sf, {"executionArn": "execution", "startDate": START}) == expected
    assert sf.calls == ["execution"]
    captured = capsys.readouterr()
    assert captured.out == ""
    if fail:
        assert captured.err.startswith("WARN:")
        assert "execution" in captured.err
        assert "fake describe failure" in captured.err
    elif raw_input == "invalid JSON":
        assert captured.err.startswith("WARN:")
    else:
        assert captured.err == ""
