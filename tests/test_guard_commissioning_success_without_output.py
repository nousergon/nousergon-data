"""Guard commissioning, class ``success_without_output``: one induced-fault record per unit.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate) and the phase-2 tracker
`alpha-engine-config-I10794`. A guard that has never fired is not in service
(`observability-policy` §9.1). This is the second guard class with a harness,
after ``empty_fresh`` (`tests/test_guard_commissioning_empty_fresh.py`), whose
machinery — the in-memory S3, the harness clock and the walker over the
deployed ``data-collection.asl.json`` — this file reuses unchanged.

**The guard.** ``run_units.record_empty_production``, recorded as
``data_success_without_output`` (`run_units.EMPTY_PRODUCTION_GUARD`). A unit
whose collector reports ``status: ok`` having recorded NO output and no rows
files its manifest ``failed`` (``EmptyProduction``), never ``ok``, unless its
descriptor declares ``empty_is_valid``. It ships ENFORCING on the record: there
is no staged mode flip to take the record against, so this harness runs the
code exactly as shipped, with the ``empty_fresh`` guard left in its shipped
OBSERVE mode.

**Why it is a different record from ``empty_fresh``.** ``empty_fresh`` grades a
PUT that happened and carried nothing. This class grades a run that PUT
nothing at all and still said it succeeded, so there is no key for
``empty_fresh`` to grade. The two consequences differ too, and the record says
which one it observed:

* ``record_empty_production`` deliberately leaves the PROCESS posture alone
  (the raise is caught outside the manifest wrapper), so the workload exits 0
  and the Step Function's on-demand retry never runs;
* the page comes from the completion check (``VerifyRunManifests``) reading the
  unit's newest manifest as ``failed``: ``run_not_ok`` ->
  ``NotifyCompletionFindings`` -> terminal ``RunNotOk``.

**Which units.** A daily-mode ``_phase_collect`` unit on the EOD machine's
``post-market-data`` workload reaches ``record_empty_production`` only when it
can record NO output, which needs a call site with no primary ``artifact_key``
(a primary key is always recorded, with its row count, and an empty one is
``empty_fresh``'s fault). Two such units declare this guard ``present`` and
report ``ok`` when nothing was written:

* D03 (prices): ``written`` is empty — no ticker parquet was uploaded — under
  ``status: ok`` with ``refreshed: 0``;
* D26 (rating ledger): neither a backfill nor a live date was written, for a
  reason other than the immutability contract (which is the declared
  ``already_live`` auto-skip, routed to ``not_applicable`` instead).

Not here, and why:

* D21 (close/fx history): its consolidated close history is recorded on every
  ``status: ok`` run, so an ``ok`` run always records an output; it reaches this
  guard only through ``status: skipped``, which exits 1 and is not a success.
* D20, D22-D25, D27-D30, D31: each call site passes a primary ``artifact_key``,
  so ``record_empty_production`` is unreachable on the daily path; their empty
  write is ``empty_fresh``'s record.
* D19, D01, D02: read by the trading path, held as for ``empty_fresh``.
* Weekly-mode, whole-mode and ``recorded_entry`` units (D04-D14, D33, D38, D39,
  D42): they run on other machines and workloads; each needs its own walk.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

import run_units
from collectors import technical_rating_ledger
from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate.store import LocalStore
from tests import test_guard_commissioning_empty_fresh as ef
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixtures, used by name
    _environment,
    clock,
    s3,
)
from validators import expectations

GUARD_CLASS = "success_without_output"
RECORDED_AS = run_units.EMPTY_PRODUCTION_GUARD
TRADING_DAY = ef.TRADING_DAY


def _d03_shape() -> ef.Shape:
    """D03 (prices): an ``ok`` run that uploaded no ticker parquet at all.

    The call site's ``extra_outputs`` is ``weekly_collector._prices_extra_outputs``
    itself (as in the ``empty_fresh`` harness); the fault is in the result, which
    names no written ticker, so ``prices.written_keys`` returns nothing to record.
    """
    def written(rows: int) -> dict:
        return {t: rows for t in ef._PRICE_TICKERS} if rows else {}

    return ef.Shape(
        result=lambda rows: {"status": "ok", "refreshed": rows, "written": written(rows)},
        written=lambda rows: ef.prices.written_keys({"written": written(rows)}, ef._PRICE_CACHE_PREFIX),
        extra_outputs=ef._d03_shape().extra_outputs,
        auto_skip=False,
    )


def _d26_shape() -> ef.Shape:
    """D26 (rating ledger): an ``ok`` run that wrote neither a backfill nor a live date.

    The ledger manifest is PUT only when the run wrote (``backfill_written or
    live_written``), so this run PUTs nothing. ``auto_skipped`` is NOT set: that
    is the collector's signal for the immutability contract (``already_live``),
    a declared ``not_applicable``, which is not this fault.
    """
    manifest = technical_rating_ledger.RATING_LEDGER_MANIFEST_KEY
    # alpha-engine-config-I11554: a live write also records the dated live entry.
    live_entry = f"{technical_rating_ledger.RATING_LEDGER_PREFIX}{ef.TRADING_DAY}.json"
    return ef.Shape(
        result=lambda rows: {
            "status": "ok", "backfill_written": 0, "live_written": bool(rows),
            "live_rated": rows, "total_dates": rows,
        },
        written=lambda rows: {manifest: rows, live_entry: rows} if rows else {},
        extra_outputs=ef._d26_shape().extra_outputs,
        auto_skip=False,
    )


CASES: tuple[ef.Case, ...] = (
    ef.Case(
        "D03", "prices", None,
        "success without output: the prices phase reports status ok with refreshed 0 and "
        "uploads no per-ticker price-cache parquet",
        shape=_d03_shape(),
    ),
    ef.Case(
        "D26", "metron_rating_ledger", None,
        "success without output: the rating-ledger phase reports status ok having written "
        "neither a backfill nor a live date, so no ledger manifest is PUT",
        shape=_d26_shape(),
    ),
)
CASE_IDS = [c.unit for c in CASES]


def _guard_reading(manifest: dict) -> dict:
    readings = [g for g in manifest["guards"] if g["guard"] == RECORDED_AS]
    assert len(readings) == 1, manifest["guards"]
    return readings[0]


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_ok_with_no_output_fails_pages_once_and_stands_down(case, s3):  # noqa: F811
    """The commissioning record for ``success_without_output``, as shipped, per unit."""
    assert not expectations.EMPTY_FRESH_GUARD.enforcing, (
        "this record is taken with empty_fresh in its shipped OBSERVE mode; if it has been "
        "promoted, re-read which guard fails the run before re-taking this record"
    )

    # Induce: the collector says `ok` and writes nothing.
    faulted = ef.Execution(s3, case, rows_per_attempt=[0])
    terminal = faulted.run()

    # The collector reported success, and the process exited 0: the fault is a
    # success with no output, not a failure the workload noticed.
    assert [a["status"] for a in faulted.attempts] == ["ok"]
    assert not s3.objects.keys() & set(case.written(896)), "the faulted run PUT a contracted key"

    # 1. A `failed` manifest, carrying the guard's enforcing verdict, with no output.
    manifests = s3.manifests(case)
    assert [m["status"] for m in manifests] == ["failed"], manifests
    m = manifests[0]
    assert m["outputs"] == []
    assert "published nothing" in m["reason"] and "empty_is_valid" in m["reason"]
    guard = _guard_reading(m)
    assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "enforce")

    # 2. Exactly one page, from the completion check grading that manifest.
    assert faulted.pages == ["NotifyCompletionFindings"], faulted.path
    assert faulted.completion["ok"] is False
    assert faulted.completion["failure_mode"] == "run_not_ok"

    # 3. Stand-down: terminal Fail with no retry armed (the exit code did not
    # move, so the on-demand retry has nothing to retry)...
    assert terminal == "Fail" and faulted.path[-1] == "RunNotOk"
    assert "RetryOnDemand" not in faulted.path
    assert "NotifyFailure" not in faulted.path

    # ...a same-date rerun that still publishes nothing is NOT carried forward
    # to `ok` (`_prior_same_date_ok_manifest` defers only to an `ok` run)...
    rerun = ef.Execution(s3, case, rows_per_attempt=[0])
    assert rerun.run() == "Fail" and rerun.path[-1] == "RunNotOk"
    assert rerun.pages == ["NotifyCompletionFindings"]
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed"]

    # ...and once the fault is gone, the next run is clean end to end.
    recovered = ef.Execution(s3, case, rows_per_attempt=[896])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed", "ok"]
    ok = s3.manifests(case)[-1]
    assert sorted((o["key"], o["rows_out"]) for o in ok["outputs"]) == sorted(case.written(896).items())
    assert not [g for g in ok["guards"] if g["guard"] == RECORDED_AS]

    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} phase={case.phase} workload={ef.WORKLOAD}"
        f"\n  fault:      {case.fault}"
        f"\n  attempt:    {faulted.attempts[0]['status']} (exit 0)"
        f"\n  manifest:   {m['status']}: {m['reason'][:110]}..."
        f"\n  verdict:    {guard['verdict']} mode={guard['mode']} ({RECORDED_AS})"
        f"\n  pages:      {len(faulted.pages)} {faulted.pages}"
        "\n  path:       " + " > ".join(faulted.path)
        + "\n  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})"
    )


def test_every_case_declares_the_guard_present_and_runs_on_the_eod_machine():
    """A case is commissioned only where the descriptor says the guard is, on the machine it runs on."""
    eod = ef._deployed_eod_input()
    assert ef.WORKLOAD in eod["workloads"]
    for case in CASES:
        assert run_units.PHASE_UNITS[("daily", case.phase)].unit_id == case.unit
        assert case.unit in eod["verify_units"], case.unit
        unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
        block = unit.guards[GUARD_CLASS]
        assert block["state"] == "present", case.unit
        assert block["records_as"] == RECORDED_AS, case.unit
        assert "run_units.py::record_empty_production" in block["code"], case.unit
        # A unit that declares empty-is-valid files `not_applicable`, not `failed`:
        # there would be no fault to induce.
        assert run_units.empty_declaration_for(case.unit) is None, case.unit


def test_no_case_is_on_the_trading_path():
    """D01, D02 and D19 are held for every guard class (the trader's feed and the executor's)."""
    assert not {"D01", "D02", "D19"} & set(CASE_IDS)


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(case, tmp_path):
    """``faults/<unit>/success_without_output/latest.json`` with ``outcome: induced`` reads MET."""
    unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(TRADING_DAY)
    before = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.HARNESSES[GUARD_CLASS]
    document = gc.record_document(
        case.unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(case.unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc.SUCCESS_WITHOUT_OUTPUT_OBSERVED)
    store.put_bytes(gc.record_key(case.unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def test_the_publisher_names_this_files_nodes():
    """The publisher runs these exact nodes; a rename here would publish nothing, loudly."""
    harness = gc.HARNESSES[GUARD_CLASS]
    assert harness.test_file == "tests/test_guard_commissioning_success_without_output.py"
    assert harness.test_name == test_the_fault_record_ok_with_no_output_fails_pages_once_and_stands_down.__name__
    for case in CASES:
        assert gc._fault_for(case.unit, GUARD_CLASS) == case.fault
