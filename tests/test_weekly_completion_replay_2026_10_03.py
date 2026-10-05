"""The weekly completion check, replayed over the REAL Sat 2026-10-03 manifests
(alpha-engine-config-I11812).

I11812 was filed because the standalone weekly (`ne-data-collection-weekly`)
could not pass its own `VerifyRunManifests` on any Saturday: D03, D05, D12 and
D34 failed it by construction. The fixes landed piecemeal —
nousergon-data#2015 (D05 reports `rows`), #2016 (D03/D12/D34 un-named),
#2019/#2020 (D12/D34 retired by declaration), #2039 (a retry's same-date no-op
is graded through the run it points back to, and an empty same-date recompute
files `no_new_data_declared`) — each tested against hand-built manifests.

Nothing yet asserted the property the issue asks for, against what a weekly
actually writes: that a clean weekly PASSES the check, and that a unit which
genuinely did not publish still FAILS it. These tests do that over the 41 real
manifests the 10-03 fire and its recovery wrote (`tests/fixtures/
weekly_completion_2026-10-03/manifests.json`, read-only from S3 and trimmed to
the fields the predicate reads), graded by the shipped predicate against the
`verify_units` the shipped template declares — never a hand-copied unit list.

What "clean" means here, precisely. The 10-03 S3 state is not clean as
written, for two reasons the code has since fixed and these tests keep visible:

* D02 attempt 0 DEGRADED on VYLR (fixed by #2036/#2041); the recovery's D02 is
  the `ok` a fixed run writes.
* The retry's D03/D08 recomputes filed `failed` EmptyProduction over their own
  attempt-0 `ok` — written by the pre-#2039 writer. The clean replay re-files
  exactly those manifests by asking the CURRENT writer's own lookup
  (`weekly_collector._prior_same_date_ok_manifest`) whether an earlier `ok` run
  of that unit and day was on S3 when each was written; nothing else is touched.
"""

from __future__ import annotations

import copy
import json
import pathlib
from types import SimpleNamespace

import pytest

import weekly_collector
from data_gate import run_manifest_predicate as predicate
from data_gate.descriptors import load_units
from infrastructure import data_collection_stack as stack

REPO = pathlib.Path(__file__).resolve().parent.parent
FIXTURE = REPO / "tests" / "fixtures" / "weekly_completion_2026-10-03" / "manifests.json"
BUCKET = "alpha-engine-research"
DAY = "2026-10-02"
#: ne-weekly-freshness-pipeline's 10-03 execution (339dd406…) started here; its
#: `WaitForCollectionManifests` grades with `lookback_seconds: 0`.
V1_STARTED = "2026-10-03T09:00:49Z"


@pytest.fixture(scope="module")
def recorded() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def weekly_units() -> list[str]:
    by_name = {s["name"]: s for s in stack.schedules(stack.load_template())}
    return list(by_name["data-collection-weekly"]["input"]["verify_units"])


@pytest.fixture(scope="module")
def v1_wait_units() -> list[str]:
    definition = json.loads((REPO / "infrastructure" / "step_function.json").read_text())
    return list(
        definition["States"]["WaitForCollectionManifests"]["Parameters"]["Payload"]["units"]
    )


@pytest.fixture(autouse=True)
def _fresh_descriptors():
    predicate.reset_unit_cache()
    yield
    predicate.reset_unit_cache()


class ReplayS3:
    """Read-only in-memory S3 over recorded manifests: list + get, as the predicate calls them."""

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


def _as_of(recorded: dict, cutoff: str) -> dict[str, dict]:
    """The manifests that were on S3 at ``cutoff`` (ISO-8601 Z strings compare in order)."""
    return {
        r["key"]: copy.deepcopy(r["manifest"])
        for r in recorded["manifests"]
        if r["last_modified"] <= cutoff
    }


def _refile_with_current_writer(objects: dict[str, dict]) -> tuple[dict[str, dict], list[str]]:
    """Re-file the pre-#2039 empty same-date recomputes the way today's writer files them.

    Only a `failed` EmptyProduction manifest with no outputs is a candidate, and
    it is re-filed only when the writer's OWN lookup, run over the S3 state at
    the moment that manifest was written, finds an earlier `ok` run of the same
    unit and trading day. Returns the new objects and the keys it re-filed.
    """
    out = copy.deepcopy(objects)
    refiled = []
    for key in sorted(objects):
        doc = objects[key]
        if not (
            doc["status"] == "failed"
            and doc["reason"].startswith("EmptyProduction")
            and not doc["outputs"]
        ):
            continue
        before = ReplayS3({k: v for k, v in out.items() if k < key})
        reg = SimpleNamespace(s3_client=before, bucket=BUCKET, date=doc["trading_day"])
        unit_id = key.split("/")[2]
        if weekly_collector._prior_same_date_ok_manifest(reg, unit_id) is not None:
            out[key] = {**doc, "status": "not_applicable",
                        "reason": predicate.SAME_DATE_NOOP_REASON, "outputs": []}
            refiled.append(key)
    return out, refiled


def _clean(recorded: dict) -> tuple[dict[str, dict], list[str]]:
    return _refile_with_current_writer(_as_of(recorded, recorded["recovery_execution_stopped"]))


def _verify(objects: dict[str, dict], units: list[str], started_at: str) -> dict:
    return predicate.completion_check(
        {"collection": "weekly", "units": units, "started_at": started_at},
        s3_client=ReplayS3(objects),
    )["completion"]


def _failed(completion: dict) -> dict[str, str]:
    return {f["unit"]: f["mode"] for f in completion["findings"]}


# ── the fixture is the run it claims to be ───────────────────────────────────


def test_the_fixture_reproduces_the_recorded_as_run_failure_set(recorded):
    """Sanity on the recording itself: the as-run state carries the eight
    `not_applicable`/`failed` newest manifests the 10-03 Fail state named."""
    as_run = _as_of(recorded, recorded["fire_execution_stopped"])
    newest: dict[str, dict] = {}
    for key in sorted(as_run):
        newest[key.split("/")[2]] = as_run[key]
    not_ok = sorted(u for u, doc in newest.items() if doc["status"] != "ok" and u not in ("D03", "D12"))
    assert not_ok == recorded["fire_verdict_as_run"]["units_failed"]


# ── a clean weekly passes ────────────────────────────────────────────────────


def test_a_clean_weekly_passes_its_own_completion_check(recorded, weekly_units):
    objects, refiled = _clean(recorded)
    # Exactly the four pre-#2039 empty recomputes (D03 and D08, attempt 1 and
    # the recovery) are re-filed; every other manifest is graded as written.
    assert sorted(k.split("/")[2] for k in refiled) == ["D03", "D03", "D08", "D08"]

    result = _verify(objects, weekly_units, recorded["fire"])
    assert result["findings"] == []
    assert result["ok"] is True, result["summary"]
    assert [r["unit"] for r in result["units"]] == weekly_units
    assert all(r["status"] == "ok" for r in result["units"])


def test_the_v1_weekly_wait_reads_the_same_clean_weekly_as_ready(recorded, v1_wait_units):
    objects, _ = _clean(recorded)
    readiness = predicate.readiness_check(
        {"collection": "weekly", "units": v1_wait_units, "not_before": V1_STARTED,
         "lookback_seconds": 0},
        s3_client=ReplayS3(objects),
    )["readiness"]
    assert readiness["ready"] is True, readiness["summary"]
    assert readiness["settled"] is True


def test_the_d05_primary_key_clears_its_floor_with_real_rows(recorded):
    """D05 failed every Saturday at `rows_below_floor` (rows_out=0 on
    macro.json) until #2015. The real 10-03 manifest records the series count."""
    objects, _ = _clean(recorded)
    (macro,) = [
        o for doc in objects.values() for o in doc["outputs"]
        if o["key"] == f"market_data/weekly/{DAY}/macro.json"
    ]
    assert macro["rows_out"] > 0


# ── a genuinely missing unit still fails ─────────────────────────────────────


def _drop(objects: dict[str, dict], unit: str, *, only_ok: bool = False) -> dict[str, dict]:
    return {
        k: v for k, v in objects.items()
        if not (k.split("/")[2] == unit and (not only_ok or v["status"] == "ok"))
    }


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(
            lambda o: _drop(o, "D15"), {"D15": "manifest_missing"},
            id="alternative-phase-two-never-wrote-D15",
        ),
        pytest.param(
            lambda o: _drop(o, "D16"), {"D16": "manifest_missing", "D46": "manifest_missing"},
            id="rag-weekly-ingestion-never-wrote-D16-or-D46",
        ),
        pytest.param(
            lambda o: _drop(o, "D06", only_ok=True), {"D06": "run_not_ok"},
            id="D06-only-no-ops-with-no-published-run-behind-them",
        ),
        pytest.param(
            lambda o: _drop(o, "D02", only_ok=True), {"D02": "run_not_ok"},
            id="D02-degraded-and-never-recovered",
        ),
    ],
)
def test_a_genuinely_missing_unit_still_fails(recorded, weekly_units, mutate, expected):
    objects, _ = _clean(recorded)
    result = _verify(mutate(objects), weekly_units, recorded["fire"])
    assert result["ok"] is False
    assert _failed(result) == expected


def test_a_published_key_below_its_floor_still_fails(recorded, weekly_units):
    """The D05 shape of failure: the manifest is `ok`, the key is there, the rows are not."""
    objects, _ = _clean(recorded)
    for doc in objects.values():
        for out in doc["outputs"]:
            if out["key"] == f"market_data/weekly/{DAY}/macro.json":
                out["rows_out"] = 0
    result = _verify(objects, weekly_units, recorded["fire"])
    assert _failed(result) == {"D05": "rows_below_floor"}


def test_a_run_from_before_this_execution_does_not_count(recorded, weekly_units):
    """Freshness: the same clean manifests graded for an execution that started
    after every one of them finished is a failure on every unit, not a pass."""
    objects, _ = _clean(recorded)
    result = _verify(objects, weekly_units, "2026-10-03T14:00:00Z")
    assert set(_failed(result)) == set(weekly_units)
    assert set(_failed(result).values()) == {"manifest_missing"}


def test_the_as_run_state_still_fails_on_the_two_real_defects(recorded, weekly_units):
    """Graded with today's predicate at the moment the 10-03 fire verified, the
    as-written state fails on D02 (its run really degraded) and D08 (the
    pre-#2039 writer really filed `failed` over it). Today's predicate turned
    eight findings into these two; it did not make either one pass."""
    as_run = _as_of(recorded, recorded["fire_execution_stopped"])
    result = _verify(as_run, weekly_units, recorded["fire"])
    assert _failed(result) == {"D02": "run_not_ok", "D08": "run_not_ok"}


# ── the units this issue un-named stay excluded by declaration ───────────────


def test_d12_and_d34_are_excluded_by_retirement_not_by_loosening(recorded, weekly_units):
    """Naming D12/D34 again would fail the clean weekly — D12 only ever
    auto-skips, D34 no longer runs — and the predicate is not what lets them
    off: their descriptors carry a recorded `retirement:`, and the template does
    not name them. (`test_data_collection_stack.py::
    test_every_standalone_successor_unit_is_covered_or_retired` keeps a retired
    unit out of every schedule.)"""
    units = {u.unit_id: u.raw for u in load_units()}
    for unit_id in ("D12", "D34"):
        assert units[unit_id].get("retirement"), unit_id
        assert unit_id not in weekly_units
    objects, _ = _clean(recorded)
    result = _verify(objects, weekly_units + ["D12", "D34"], recorded["fire"])
    assert _failed(result) == {"D12": "run_not_ok", "D34": "manifest_missing"}


def test_d03_is_graded_by_the_eod_collection_that_owns_it(weekly_units):
    by_name = {s["name"]: s for s in stack.schedules(stack.load_template())}
    assert "D03" not in weekly_units
    assert "D03" in by_name["data-collection-eod"]["input"]["verify_units"]
