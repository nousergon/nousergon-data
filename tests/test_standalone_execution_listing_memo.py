"""alpha-engine-config-I12307: `survives_phase4` lists each machine ONCE per run.

CloudTrail 2026-10-14 03:55-04:58Z: 1,164 `states:ListExecutions` from
`github-actions-data-gate-read` across two data-gate.yml runs (~580 a run,
~97 per `python -m data_gate read`), every one a first page: each unit x
schedule pair graded by `standalone._grade_schedule` re-listed its machine
from the newest page. These tests pin the per-run bound and that the shared
listing returns the same executions the per-call walk did.
"""

from __future__ import annotations

import collections
import datetime as dt

import pytest

from data_gate import descriptors, standalone
from data_gate.store import LocalStore

UTC = dt.timezone.utc
TUESDAY = dt.date(2026, 9, 15)
WEDNESDAY_NOON = dt.datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:"


class _Scheduler:
    def get_schedule(self, GroupName, Name):  # noqa: N803 — boto3 kwarg
        machine = Name.replace("data-collection-", "ne-data-collection-")
        schedule = next(s for s in standalone._stack_schedules() if s["name"] == Name)
        return {
            "State": "ENABLED",
            "ScheduleExpression": schedule["expression"],
            "ScheduleExpressionTimezone": schedule["timezone"],
            "Target": {"Arn": f"{ARN}{machine}"},
        }


class _CountingSfn:
    """Newest-first executions, served ``page_size`` at a time with tokens."""

    def __init__(self, executions: dict[str, list[dict]], page_size: int = 100, fail_first: int = 0):
        self.executions = executions
        self.page_size = page_size
        self.fail_first = fail_first
        self.calls: collections.Counter = collections.Counter()

    def list_executions(self, stateMachineArn, maxResults, nextToken=None):  # noqa: N803
        machine = stateMachineArn.rsplit(":", 1)[-1]
        self.calls[machine] += 1
        if self.fail_first:
            self.fail_first -= 1
            raise RuntimeError("ThrottlingException")
        rows = self.executions.get(machine, [])
        start = int(nextToken or 0)
        end = start + min(maxResults, self.page_size)
        page = {"executions": rows[start:end]}
        if end < len(rows):
            page["nextToken"] = str(end)
        return page


def _history(machine: str, newest: dt.datetime, n: int, step: dt.timedelta) -> list[dict]:
    return [
        {
            "executionArn": f"arn:aws:states:us-east-1:000000000000:execution:{machine}:{i}",
            "status": "SUCCEEDED",
            "startDate": newest - i * step,
            "stopDate": newest - i * step + dt.timedelta(minutes=30),
        }
        for i in range(n)
    ]


def _store(tmp_path, sfn) -> LocalStore:
    store = LocalStore(tmp_path)
    store.scheduler_client = _Scheduler()
    store.sfn_client = sfn
    return store


@pytest.fixture(scope="module")
def units() -> list[descriptors.Unit]:
    return list(descriptors.load_units())


def test_every_unit_reading_lists_each_machine_once(tmp_path, units):
    machines = {s["qualified_name"] for s in standalone._stack_schedules()}
    assert machines, "stack defines no schedules"
    executions = {
        name.replace("data-collection-", "ne-data-collection-"): _history(
            name, WEDNESDAY_NOON, 30, dt.timedelta(hours=6)
        )
        for name in (s["name"] for s in standalone._stack_schedules())
    }
    sfn = _CountingSfn(executions)
    store = _store(tmp_path, sfn)

    covered = [u for u in units if standalone.covering_schedules(u.unit_id)]
    assert len(covered) > 3, "too few covered units for the bound to mean anything"
    for unit in covered:
        standalone.read_survives_phase4(store, unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
    # Twice, as `data_gate.read.run` grades the same clause for several gates.
    for unit in covered:
        standalone.read_survives_phase4(store, unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)

    assert sfn.calls, "no listing happened — the test exercised nothing"
    assert max(sfn.calls.values()) == 1, dict(sfn.calls)


def test_readings_match_an_unshared_listing(tmp_path, units):
    """Memoising changes call count, never a verdict: each unit graded against
    its own fresh store (the old one-listing-per-reading shape) reads the same."""
    executions = {
        "ne-data-collection-eod": _history("eod", WEDNESDAY_NOON, 250, dt.timedelta(hours=1)),
        "ne-data-collection-weekly": _history("weekly", WEDNESDAY_NOON, 250, dt.timedelta(hours=2)),
        "ne-data-collection-morning": _history("morning", WEDNESDAY_NOON, 40, dt.timedelta(hours=12)),
    }
    covered = [u for u in units if standalone.covering_schedules(u.unit_id)]
    shared = _store(tmp_path, _CountingSfn(executions, page_size=7))
    for unit in covered:
        alone = standalone.read_survives_phase4(
            _store(tmp_path, _CountingSfn(executions, page_size=7)), unit, trading_day=TUESDAY, now=WEDNESDAY_NOON
        )
        together = standalone.read_survives_phase4(shared, unit, trading_day=TUESDAY, now=WEDNESDAY_NOON)
        assert together == alone, unit.unit_id


def test_the_listing_pages_lazily_and_only_as_deep_as_asked(tmp_path):
    rows = _history("eod", WEDNESDAY_NOON, 25, dt.timedelta(hours=1))
    sfn = _CountingSfn({"ne-data-collection-eod": rows}, page_size=10)
    store = _store(tmp_path, sfn)
    arn = f"{ARN}ne-data-collection-eod"

    recent = standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(hours=5))
    assert [e["executionArn"] for e in recent] == [e["executionArn"] for e in rows[:6]]
    assert sfn.calls["ne-data-collection-eod"] == 1

    deeper = standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(hours=15))
    assert deeper == rows[:16]
    assert sfn.calls["ne-data-collection-eod"] == 2

    assert standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(days=30)) == rows
    assert sfn.calls["ne-data-collection-eod"] == 3
    # Exhausted: nothing more to fetch, ever.
    assert standalone._execution_for(store, arn, WEDNESDAY_NOON - dt.timedelta(days=30)) is None
    assert sfn.calls["ne-data-collection-eod"] == 3


def test_a_failed_listing_is_not_cached(tmp_path):
    rows = _history("eod", WEDNESDAY_NOON, 3, dt.timedelta(hours=1))
    sfn = _CountingSfn({"ne-data-collection-eod": rows}, fail_first=1)
    store = _store(tmp_path, sfn)
    arn = f"{ARN}ne-data-collection-eod"
    with pytest.raises(RuntimeError):
        standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(days=1))
    assert standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(days=1)) == rows
    assert sfn.calls["ne-data-collection-eod"] == 2


def test_a_swapped_client_gets_its_own_listing(tmp_path):
    arn = f"{ARN}ne-data-collection-eod"
    first = _CountingSfn({"ne-data-collection-eod": []})
    store = _store(tmp_path, first)
    assert standalone._executions_since(store, arn, WEDNESDAY_NOON) == []
    rows = _history("eod", WEDNESDAY_NOON, 2, dt.timedelta(hours=1))
    store.sfn_client = _CountingSfn({"ne-data-collection-eod": rows})
    assert standalone._executions_since(store, arn, WEDNESDAY_NOON - dt.timedelta(days=1)) == rows
