"""The v1 weekly's readiness wait waits out the producer's own retry
(alpha-engine-config-I11812, weekly failure class K02 CollectionReadiness).

`ne-data-collection-weekly` runs a failed workload once more, on demand
(`CheckRetryBudget` -> `RetryOnDemand`), and since nousergon-data#2039 that
retry recomputes a degraded unit. The v1 weekly (`ne-weekly-freshness-pipeline`)
polls the same run manifests through `alpha-engine-collection-readiness-probe`
and, on `settled and not ready`, degrades AT ONCE and fails the whole Saturday
closed.

Before this change `settled` meant only "every unit has a manifest". So a unit
that failed on the producer's attempt 0 settled the wait the moment the last
attempt-0 unit landed, and the v1 weekly failed while the retry that would have
fixed the unit was still running. On 10-03 that did not happen only because the
D14 prune was skipped (no D14 manifest, so never settled) — #2040 made the prune
run on a failed phase 1, which made the attempt-0 settle reachable on 10-10.

These tests replay the real 10-03 manifests (`tests/fixtures/
weekly_completion_2026-10-03/manifests.json`) cut at the end of attempt 0, with
the D14 manifest #2040 now writes there, and pin both constants the probe ships
with to the definitions they mirror.
"""

from __future__ import annotations

import copy
import json
import pathlib

import pytest

from data_gate import run_manifest_predicate as predicate

REPO = pathlib.Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "fixtures" / "weekly_completion_2026-10-03" / "manifests.json"
DAY = "2026-10-02"
#: ne-weekly-freshness-pipeline's 10-03 execution started here (lookback 0).
V1_STARTED = "2026-10-03T09:00:49Z"
#: weekly-phase-one attempt 0 exited rc=1 at 10:42Z; the retry's first
#: manifests landed at 10:45:15Z.
ATTEMPT_0_ENDED = "2026-10-03T10:43:00Z"

V1_DEFINITIONS = {
    "step_function.json": "weekly",
    "step_function_daily.json": "morning",
    "step_function_eod_reconcile.json": "eod",
}


class ReplayS3:
    """Read-only in-memory S3: list + get, as the predicate calls them."""

    def __init__(self, objects: dict[str, dict]):
        self.objects = {k: json.dumps(v).encode() for k, v in objects.items()}

    def list_objects_v2(self, Bucket, Prefix, StartAfter="", ContinuationToken=None, **kw):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def get_object(self, Bucket, Key):  # noqa: N803
        payload = self.objects[Key]

        class _Body:
            def read(self_inner):
                return payload

        return {"Body": _Body()}


@pytest.fixture(autouse=True)
def _fresh_descriptors():
    predicate.reset_unit_cache()
    yield
    predicate.reset_unit_cache()


@pytest.fixture(scope="module")
def recorded() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def v1_wait_units() -> list[str]:
    definition = json.loads((REPO / "infrastructure" / "step_function.json").read_text())
    return list(
        definition["States"]["WaitForCollectionManifests"]["Parameters"]["Payload"]["units"]
    )


def _key(unit: str, run_id: str) -> str:
    return f"data_collection/runs/{unit}/{DAY}/{run_id}.json"


def _manifest(unit: str, status: str, finished: str, *, reason: str = "", outputs=None) -> dict:
    return {
        "unit_id": unit, "trading_day": DAY, "status": status, "reason": reason,
        "started": finished, "finished": finished, "outputs": outputs or [], "guards": [],
    }


def _end_of_attempt_0(recorded: dict) -> dict[str, dict]:
    """S3 as the v1 wait read it when attempt 0 ended, under today's writer.

    The real cut at 10:43Z, plus the one manifest #2040 adds there: the D14
    prune now runs when phase 1 fails, so D14 lands with attempt 0 (its real
    attempt-1 manifest, re-keyed to sort before the retry's runs).
    """
    objects = {
        r["key"]: copy.deepcopy(r["manifest"])
        for r in recorded["manifests"]
        if r["last_modified"] <= ATTEMPT_0_ENDED
    }
    (d14,) = [
        r["manifest"] for r in recorded["manifests"]
        if r["key"].startswith(f"data_collection/runs/D14/{DAY}/")
        and r["last_modified"] < "2026-10-03T12:00:00Z"
    ]
    objects[_key("D14", "01M40NW000D14ATTEMPT0PRUNE0")] = {
        **copy.deepcopy(d14), "started": "2026-10-03T10:42:00Z", "finished": "2026-10-03T10:42:30Z",
    }
    return objects


def _readiness(objects: dict[str, dict], units: list[str], collection: str = "weekly") -> dict:
    return predicate.readiness_check(
        {"collection": collection, "units": units, "not_before": V1_STARTED,
         "lookback_seconds": 0},
        s3_client=ReplayS3(objects),
    )["readiness"]


# ── the defect, on the real manifests ────────────────────────────────────────


def test_attempt_0_with_a_degraded_unit_is_not_settled_while_the_retry_can_fix_it(
    recorded, v1_wait_units
):
    """10-03 attempt 0 ended with D02 degraded (VYLR) and every other waited
    unit published. Before this change the wait read that as settled and the
    v1 weekly failed closed on the next poll; now it keeps polling."""
    readiness = _readiness(_end_of_attempt_0(recorded), v1_wait_units)
    assert readiness["missing"] == []
    assert readiness["failed"] == ["D02"]
    assert readiness["retry_pending"] == ["D02"]
    assert readiness["ready"] is False
    assert readiness["settled"] is False, readiness["summary"]
    assert "retry_pending=['D02']" in readiness["summary"]


def test_the_retry_that_recomputes_the_unit_turns_the_wait_ready(recorded, v1_wait_units):
    """#2039's retry recomputes the degraded D02; the next poll reads READY and
    the Saturday pipeline goes on to research and the predictor."""
    objects = _end_of_attempt_0(recorded)
    real_d02_ok = next(
        r["manifest"] for r in recorded["manifests"]
        if r["key"].startswith(f"data_collection/runs/D02/{DAY}/") and r["manifest"]["status"] == "ok"
    )
    objects[_key("D02", "01M40NWESNCM00HFH23GK8MMK8")] = {
        **copy.deepcopy(real_d02_ok), "started": "2026-10-03T10:45:00Z",
        "finished": "2026-10-03T11:20:00Z",
    }
    readiness = _readiness(objects, v1_wait_units)
    assert readiness["ready"] is True, readiness["summary"]
    assert readiness["settled"] is True
    assert readiness["retry_pending"] == []


def test_a_retry_that_fails_the_unit_again_settles_the_wait_not_ready(recorded, v1_wait_units):
    """The retry is the producer's last word: a second failure degrades at once."""
    objects = _end_of_attempt_0(recorded)
    objects[_key("D02", "01M40NWESNCM00HFH23GK8MMK8")] = _manifest(
        "D02", "failed", "2026-10-03T11:20:00Z", reason="_DegradedRun: still disagrees",
    )
    readiness = _readiness(objects, v1_wait_units)
    assert readiness["ready"] is False
    assert readiness["settled"] is True
    assert readiness["failed"] == ["D02"] and readiness["retry_pending"] == []
    assert readiness["failure_mode"] == "run_not_ok"


def test_the_as_run_10_03_retry_no_op_is_the_producers_last_word(recorded, v1_wait_units):
    """The pre-#2039 retry auto-skipped D02 and filed a same-date no-op pointing
    back at the degraded run. Two fresh visits: settled, not ready — exactly
    the verdict the 10-03 v1 weekly reached, so nothing here hides a real failure."""
    objects = {
        r["key"]: copy.deepcopy(r["manifest"])
        for r in recorded["manifests"]
        if r["last_modified"] <= "2026-10-03T11:30:00Z"
    }
    readiness = _readiness(objects, v1_wait_units)
    assert "D02" in readiness["failed"]
    assert readiness["retry_pending"] == []
    assert readiness["settled"] is True


# ── what stays terminal ──────────────────────────────────────────────────────


def test_a_short_row_count_on_an_ok_run_is_not_retry_pending(recorded, v1_wait_units):
    """The old D05 shape: the run says ok and published too few rows. The
    producer does not retry an ok run, so there is nothing to wait for."""
    objects = _end_of_attempt_0(recorded)
    for doc in objects.values():
        for out in doc["outputs"]:
            if out["key"] == f"market_data/weekly/{DAY}/macro.json":
                out["rows_out"] = 0
    readiness = _readiness(objects, v1_wait_units)
    assert "D05" in readiness["failed"]
    assert "D05" not in readiness["retry_pending"]


def test_a_no_op_with_no_run_behind_it_is_not_retry_pending():
    objects = {
        _key("D02", "01M40NWESNCM00HFH23GK8MMK8"): _manifest(
            "D02", "not_applicable", "2026-10-03T10:45:15Z", reason="no_new_data_declared",
        )
    }
    readiness = _readiness(objects, ["D02"])
    assert readiness["failed"] == ["D02"]
    assert readiness["retry_pending"] == []
    assert readiness["settled"] is True


def test_a_failure_from_before_this_execution_does_not_count_as_a_visit():
    """Only runs of THIS cycle count toward the producer's attempts: an earlier
    execution's failed D02 does not make attempt 0's failure look retried."""
    objects = {
        _key("D02", "01M40A0000000000000000OLD0"): _manifest(
            "D02", "failed", "2026-10-03T08:00:00Z", reason="earlier rehearsal",
        ),
        _key("D02", "01M40GE6V29Y40VMRZNVDG1QNE"): _manifest(
            "D02", "failed", "2026-10-03T09:10:18Z", reason="_DegradedRun: VYLR",
        ),
    }
    readiness = _readiness(objects, ["D02"])
    assert readiness["retry_pending"] == ["D02"]
    assert readiness["settled"] is False


@pytest.mark.parametrize("collection", ["morning", "eod"])
def test_fail_open_consumers_still_degrade_at_once(recorded, v1_wait_units, collection):
    """The trading-clock consumers are out of scope: their verdict is unchanged."""
    readiness = _readiness(_end_of_attempt_0(recorded), v1_wait_units, collection)
    assert readiness["failed"] == ["D02"]
    assert readiness["retry_pending"] == []
    assert readiness["settled"] is True


# ── the constants are derived, not remembered ────────────────────────────────


def test_producer_attempts_match_the_collection_machines_retry_budget():
    asl = json.loads(
        (REPO / "infrastructure" / "step-functions" / "data-collection.asl.json").read_text()
    )
    states = asl["States"]["RunWorkloads"]["ItemProcessor"]["States"]
    (choice,) = states["CheckRetryBudget"]["Choices"]
    assert choice["Variable"] == "$.attempts" and choice["Next"] == "RetryOnDemand"
    # attempts starts at 0 and RetryOnDemand adds 1: a retry runs while
    # attempts < N, so the workload runs N + 1 times in all.
    assert states["RetryOnDemand"]["Parameters"]["attempts.$"] == "States.MathAdd($.attempts, 1)"
    assert predicate.PRODUCER_ATTEMPTS_PER_WORKLOAD == choice["NumericLessThan"] + 1


def _wait_and_fallback(definition: dict) -> tuple[dict, dict]:
    found: dict[str, dict] = {}

    def walk(states: dict) -> None:
        for name, state in states.items():
            found.setdefault(name, state)
            for branch in state.get("Branches") or []:
                walk(branch["States"])

    walk(definition["States"])
    return found["WaitForCollectionManifests"], found["ExtractCollectionNotReadyError"]


def test_the_retry_aware_collections_are_exactly_the_fail_closed_consumers():
    """A consumer that fails CLOSED on not-ready loses its whole run to a
    retryable failure, so it waits out the retry. A fail-open consumer on a
    trading clock keeps degrading at once. Derived from all three v1 definitions."""
    fail_closed = set()
    for filename, collection in V1_DEFINITIONS.items():
        definition = json.loads((REPO / "infrastructure" / filename).read_text())
        wait, fallback = _wait_and_fallback(definition)
        assert wait["Parameters"]["Payload"]["collection"] == collection
        if fallback["Next"] == "NormalizeFailureContext":
            fail_closed.add(collection)
        else:
            assert fallback["Next"] == "ExtractDataSpotError", (filename, fallback["Next"])
    assert fail_closed == set(predicate.AWAITS_PRODUCER_RETRY) == {"weekly"}


def test_the_weekly_budget_still_routes_a_never_retried_failure_to_fail_closed():
    """If the failed workload exits 0 there is no retry and the unit stays
    pending: the wait must still end, through the bounded budget, at the same
    fail-closed state a settled failure reaches."""
    definition = json.loads((REPO / "infrastructure" / "step_function.json").read_text())
    states = definition["States"]
    budget = states["CheckCollectionReadinessBudget"]
    exhausted = [c for c in budget["Choices"] if "shell_run" not in json.dumps(c)]
    assert exhausted and exhausted[0]["Next"] == "ExtractCollectionNotReadyError"
    assert states["ExtractCollectionNotReadyError"]["Next"] == "NormalizeFailureContext"
