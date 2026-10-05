"""The gate's IAM simulate list is generated, and the board makes no simulate call it does not list.

`alpha-engine-config-I11279`: the gate role's ``iam:SimulatePrincipalPolicy``
Resource list was kept by hand in `nous-ergon-ops` and named one role while
`writer_identities.yaml` named five, so five ``identity`` clauses read
UNMEASURABLE on every board. `data_gate/simulate_plan.py` derives the list;
this module holds both ends of that derivation:

* **every simulate call a full board read makes is a plan row, and every plan
  row is called** — run through `clauses.generate`, not `read_identity` alone,
  so a new simulate read added ANYWHERE in the gate without a plan row fails
  here;
* **the committed role list is current**, so the `nous-ergon-ops` cross-repo
  contract that reads it is reading the truth;
* **deliverable 3**: throttling is paced and backed off with jitter, BOUNDED,
  and ends UNMEASURABLE naming the throttle — never an unbounded retry.
"""

from __future__ import annotations

import pytest

from data_gate import clauses as clause_module
from data_gate import simulate_plan, unit_readers
from data_gate.descriptors import load_units
from data_gate.read import load_phases
from tests.data_gate_support import TRADING_DAY, EmptyStore

PROBE_KEY = "data_gate_identity_probe/"


class _AwsError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _RecordingSimulator:
    """Allows every declared key, denies the undeclared probe, records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def simulate_principal_policy(self, **kwargs):
        (action,) = kwargs["ActionNames"]
        (resource,) = kwargs["ResourceArns"]
        self.calls.append((kwargs["PolicySourceArn"], action, resource))
        decision = "implicitDeny" if PROBE_KEY in resource else "allowed"
        return {"EvaluationResults": [{"EvalResourceName": resource, "EvalDecision": decision}], "IsTruncated": False}


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """A fake clock that only `_sleep` advances; returns every sleep taken."""
    now = [1000.0]
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(round(seconds, 9))
        now[0] += seconds

    monkeypatch.setattr(unit_readers, "_sleep", sleep)
    monkeypatch.setattr(unit_readers, "_clock", lambda: now[0])
    monkeypatch.setattr(unit_readers, "_last_call", [])
    return slept


@pytest.fixture(scope="module")
def units():
    return load_units()


@pytest.fixture(scope="module")
def config():
    return unit_readers.load_identities()


def _row_keys(rows, config) -> list[tuple[str, str, str]]:
    return [(f"arn:aws:iam::{config['account_id']}:role/{r.role}", r.action, r.resource) for r in rows]


def _unplanned(calls, planned) -> tuple[set, set]:
    """``(calls with no plan row, plan rows never called)``."""
    return set(calls) - set(planned), set(planned) - set(calls)


def test_every_simulate_call_on_a_full_board_read_is_a_plan_row(units, config):
    store = EmptyStore()
    store.iam_client = _RecordingSimulator()
    clause_module.generate(store, units, load_phases(), trading_day=TRADING_DAY)
    planned = _row_keys(simulate_plan.board_plan(units, config), config)
    unplanned, uncalled = _unplanned(store.iam_client.calls, planned)
    assert not unplanned, (
        f"the board made simulate call(s) no plan row lists: {sorted(unplanned)[:5]}. Add them to "
        "unit_readers.identity_plan so the gate role's simulate grant is generated to cover them."
    )
    assert not uncalled, f"plan row(s) the board never called: {sorted(uncalled)[:5]}"
    assert len(store.iam_client.calls) == len(planned), "a row was simulated more than once"


def test_the_bijection_check_catches_a_call_with_no_row(units, config):
    """The guard above is EXERCISED: drop one row and it must report it."""
    planned = _row_keys(simulate_plan.board_plan(units, config), config)
    unplanned, _ = _unplanned(planned, planned[1:])
    assert unplanned == {planned[0]}


def test_the_committed_role_list_is_current(units, config):
    expected = simulate_plan.render(simulate_plan.roles_document(units, config))
    actual = simulate_plan.GENERATED_ROLES_PATH.read_text(encoding="utf-8")
    assert actual == expected, f"{simulate_plan.GENERATED_ROLES_PATH.name} is stale; run `{simulate_plan.REGENERATE}`"


def test_every_declared_role_with_something_to_simulate_is_listed(units, config):
    """Every per-unit and per-class role a non-retired, non-excluded unit resolves to."""
    listed = {r["role"] for r in simulate_plan.roles_document(units, config)["roles"]}
    for unit in units:
        plan = unit_readers.identity_plan(unit, config)
        if plan.rows:
            assert plan.role in listed, unit.unit_id
    # The five I11279 found missing from the hand-kept grant, as resolved today.
    for role in {config["by_unit"][u] for u in ("D36", "D37", "D38", "D39", "D42")}:
        assert role in listed


def test_a_retired_or_excluded_unit_contributes_no_rows(units, config):
    for unit in units:
        if unit.retired or unit_readers.partial_exclusion_reading(unit, "identity") is not None:
            assert unit_readers.identity_plan(unit, config).rows == (), unit.unit_id


def test_each_unit_plan_is_declared_puts_then_the_probe(units, config):
    unit = next(u for u in units if u.unit_id == "D19")
    plan = unit_readers.identity_plan(unit, config)
    assert [r.expect for r in plan.rows[-2:]] == ["denied", "denied"]
    assert [r.action for r in plan.rows[-2:]] == ["s3:PutObject", "s3:DeleteObject"]
    assert all(r.action == "s3:PutObject" and r.expect == "allowed" for r in plan.rows[:-2])


# ---------------------------------------------------------------------------
# Deliverable 3 — bounded, jittered pacing.
# ---------------------------------------------------------------------------


class _Throttling:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def simulate_principal_policy(self, **kwargs):
        self.calls += 1
        if self.calls <= self.failures:
            raise _AwsError("Throttling")
        (resource,) = kwargs["ResourceArns"]
        return {"EvaluationResults": [{"EvalResourceName": resource, "EvalDecision": "allowed"}], "IsTruncated": False}


def test_a_transient_throttle_is_backed_off_and_answered(monkeypatch, _no_sleep):
    monkeypatch.setattr(unit_readers, "_jitter", lambda: 1.0)
    client = _Throttling(failures=2)
    decision = unit_readers._simulate_one(client, "arn:aws:iam::1:role/r", "s3:PutObject", "arn:aws:s3:::b/k")
    assert decision == "allowed" and client.calls == 3
    # Each backoff exceeds the pacing floor, so no pacing sleep sits between them.
    assert _no_sleep == [unit_readers.SIMULATE_BACKOFF_BASE_S, 2 * unit_readers.SIMULATE_BACKOFF_BASE_S]


def test_backoff_is_jittered_and_capped(monkeypatch, _no_sleep):
    monkeypatch.setattr(unit_readers, "SIMULATE_MIN_INTERVAL_S", 0.0)  # isolate the backoff
    monkeypatch.setattr(unit_readers, "SIMULATE_MAX_ATTEMPTS", 7)  # long enough to reach the cap
    monkeypatch.setattr(unit_readers, "_jitter", lambda: 0.25)
    client = _Throttling(failures=6)
    unit_readers._simulate_one(client, "arn:aws:iam::1:role/r", "s3:PutObject", "arn:aws:s3:::b/k")
    caps = [min(unit_readers.SIMULATE_BACKOFF_CAP_S, unit_readers.SIMULATE_BACKOFF_BASE_S * 2**i) for i in range(6)]
    assert caps[-1] == unit_readers.SIMULATE_BACKOFF_CAP_S
    assert _no_sleep == [0.25 * c for c in caps]


def test_a_persistent_throttle_is_unmeasurable_naming_it_after_bounded_attempts(units):
    store = EmptyStore()
    store.iam_client = _Throttling(failures=10**6)
    reading = unit_readers.read_identity(store, next(u for u in units if u.unit_id == "D27"))
    assert reading.unmeasurable and not reading.met
    assert "Throttling" in reading.detail and "throttled" in reading.detail
    assert store.iam_client.calls == unit_readers.SIMULATE_MAX_ATTEMPTS


def test_a_non_throttle_error_is_not_retried(units):
    class _Denied:
        calls = 0

        def simulate_principal_policy(self, **kwargs):
            _Denied.calls += 1
            raise _AwsError("AccessDenied")

    store = EmptyStore()
    store.iam_client = _Denied()
    reading = unit_readers.read_identity(store, next(u for u in units if u.unit_id == "D19"))
    assert reading.unmeasurable and "AccessDenied" in reading.detail
    assert _Denied.calls == 1


def test_consecutive_calls_are_spaced_by_the_floor(_no_sleep):
    client = _Throttling(failures=0)
    for _ in range(3):
        unit_readers._simulate_one(client, "arn:aws:iam::1:role/r", "s3:PutObject", "arn:aws:s3:::b/k")
    assert _no_sleep == [unit_readers.SIMULATE_MIN_INTERVAL_S] * 2


def test_the_cli_check_passes_on_the_committed_file():
    assert simulate_plan.main(["roles", "--check"]) == 0
