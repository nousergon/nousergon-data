"""Guard commissioning, class ``vendor_fallback``: one induced-fault record per unit.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "one induced fault per guard class
(**withheld vendor**, truncated universe, unit-scaled column, empty frame,
stale ``as_of``) -> ``failed``, one page, stand-down"). A guard that has never
fired is not in service (`observability-policy` §9.1). This is the sixth guard
class with a harness, after ``empty_fresh``, ``success_without_output``,
``cardinality``, ``units`` and ``pit``; it reuses the ``empty_fresh``
machinery (the in-memory S3, the harness clock, the walker over the deployed
``data-collection.asl.json``).

**The guard is the refusal to substitute.** D51 (the daily panel) declares
``vendor_fallback: present`` as "one store (ArcticDB universe) and no fallback
path: a failed or empty read raises, it is never substituted". The check is
``builders.daily_panel.compile_panel``: a loader answering zero symbols raises
``PanelCompileError`` before anything is written, and ``run`` turns that into
``status: error``, which the whole-mode wrapper files ``failed`` and ``main``
exits 1 on.

**The fault is a withheld vendor.** The one store D51 reads answers with no
symbols at all (an ArcticDB outage, an expired credential, a library that did
not open): the read the panel is compiled from never arrives, and there is no
second source to fall back to. The fault persists into the on-demand retry.

**What runs for real:** ``builders.daily_panel.run`` and ``compile_panel``,
the contract validation, ``weekly_collector._run_daily_panel`` under the REAL
``_run_whole_mode_unit`` wrapper and run-manifest sink, the completion check
the dispatcher Lambda runs, and the deployed ``data-collection.asl.json``
walked state by state on the D51 schedule's own input
(``ne-data-collection-panel-on-eod``). **Modelled, and stated:** the ArcticDB
read itself (``nousergon_lib.arcticdb.load_universe_ohlcv``), which answers
the 2026-10-02 universe fixture when healthy and nothing when withheld, and
the S3 client, which is the in-memory store.

**The three properties, per the issue's closes-when:**

1. a ``failed`` manifest naming the empty read, on the attempt and on the
   on-demand retry;
2. exactly one page, ``NotifyFailure``: the process exits 1 on both attempts;
3. stand-down: terminal ``CollectionFailed``, and nothing under
   ``data_collection/panel/{day}/`` was written (no parquet, no manifest), so
   no consumer reads a thin or substituted panel. Once the store answers, the
   next run publishes both keys, files ``ok`` and pages nobody.

Not here, and why:

* **D17** (``hard-fails on polygon failure``): it writes
  ``staging/daily_closes``, the executor's feed, which is held for every guard
  class alongside D01, D02 and D19.
* **D20** (``present_bounded``): its yfinance gap-fill IS a fallback, bounded
  and counted; a withheld primary is absorbed, not refused, so the record would
  be ``absorbed``, not ``induced``. It needs its own design.
* **D49**: its rung ladder records the fallback rung as ``weight_method``; the
  same reasoning as D20.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import types
from collections.abc import Callable
from dataclasses import dataclass

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("pyarrow")

import run_units  # noqa: E402
import weekly_collector  # noqa: E402
from builders import daily_panel as pub  # noqa: E402
from contracts import daily_panel as dp  # noqa: E402
from data_gate import descriptors, evidence  # noqa: E402
from data_gate import guard_commissioning as gc  # noqa: E402
from data_gate.store import LocalStore  # noqa: E402
from nousergon_lib import run_manifest  # noqa: E402
from tests import test_guard_commissioning_empty_fresh as ef  # noqa: E402
from tests import test_guard_commissioning_pit as pit  # noqa: E402
from tests.test_daily_panel_publisher import DAY, _frames  # noqa: E402
from tests.test_guard_commissioning_empty_fresh import (  # noqa: E402,F401 -- fixture, used by name
    _environment,
)

GUARD_CLASS = "vendor_fallback"
BUCKET = ef.BUCKET
#: The session the fixture universe covers (`tests/fixtures`, 2026-10-02).
TRADING_DAY = DAY.isoformat()
#: The panel machine starts when the EOD machine succeeds: that evening.
PANEL_START = dt.datetime(2026, 10, 2, 23, 30, tzinfo=dt.timezone.utc)
#: The refusal's own words (`builders.daily_panel.compile_panel`).
REFUSAL = "returned zero symbols"


# ---------------------------------------------------------------------------
# The one store D51 reads
# ---------------------------------------------------------------------------


class UniverseStore:
    """``load_universe_ohlcv`` as D51 calls it: the fixture universe, or nothing."""

    def __init__(self) -> None:
        self.reads: list[bool] = []
        self.healthy = True

    def load(self, bucket, *, symbols=None, lookback_days=None, end=None, region=None):
        self.reads.append(self.healthy)
        return _frames() if self.healthy else {}


@dataclass(frozen=True)
class Case:
    """One unit's induced withheld vendor. ``unit`` and ``fault`` are what the publisher reads."""

    unit: str
    workload: str
    collection: str
    fault: str
    #: The keys a clean run publishes, in write order.
    outputs: tuple[str, ...]
    run: Callable[[], dict] = dataclasses.field(repr=False, compare=False)


class _Args:
    def __init__(self) -> None:
        self.date = TRADING_DAY
        self.dry_run = False


def _run_d51() -> dict:
    return weekly_collector._run_whole_mode_unit(
        "daily_panel", weekly_collector._run_daily_panel, {"bucket": BUCKET}, _Args()
    )


_FAULT = (
    "withheld vendor: the one store D51 compiles from (the ArcticDB universe library, "
    "nousergon_lib.arcticdb.load_universe_ohlcv) answers zero symbols for the session, on "
    "the attempt and on the retry; there is no second source, and none may be substituted"
)

CASES: tuple[Case, ...] = (
    Case(
        "D51", "daily-panel", "panel", _FAULT,
        outputs=(dp.panel_key(DAY), dp.manifest_key(DAY)),
        run=_run_d51,
    ),
)
CASE_IDS = [c.unit for c in CASES]


# ---------------------------------------------------------------------------
# One execution of the deployed machine, on the panel schedule's own input
# ---------------------------------------------------------------------------


class Execution(ef.Execution):
    """The ``empty_fresh`` walker, with D51's whole-mode unit run for real."""

    def __init__(self, s3, case: Case, store: UniverseStore, healthy_per_attempt):
        super().__init__(s3, case, healthy_per_attempt)
        self.store = store

    def tasks(self, name: str, state: dict, data: dict):
        if name == "PollWorkload":
            if data["workload"] != self.case.workload:
                return {"Status": "Success", "StatusDetails": "Success"}
            self.store.healthy = self.rows_per_attempt[len(self.attempts)]
            result = self.case.run()
            self.attempts.append(result)
            status = pit._process_status(result)
            return {"Status": status, "StatusDetails": status}
        return super().tasks(name, state, data)

    def run(self) -> str:
        asl = json.loads(ef.ASL_PATH.read_text())
        data = _panel_input(self.case)
        context = {
            "$$.Execution.Id": "arn:aws:states:::execution:commissioning",
            "$$.Execution.StartTime": self.started_at.isoformat().replace("+00:00", "Z"),
        }
        return ef._walk(asl["States"], asl["StartAt"], data, self.tasks, self.path, context)


def _panel_input(case: Case) -> dict:
    """The panel schedule's execution input that runs this unit, from the deployed template."""
    data = ef._deployed_input(case.collection)
    assert case.unit in data["verify_units"], data
    return data


@pytest.fixture(autouse=True)
def clock(monkeypatch) -> ef.HarnessClock:
    """Every manifest and execution start reads one stepping clock (see ``ef.HarnessClock``)."""
    harness_clock = ef.HarnessClock(PANEL_START)

    class _HarnessDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return harness_clock.now().astimezone(tz) if tz else harness_clock.now()

    monkeypatch.setattr(
        run_manifest, "dt", types.SimpleNamespace(datetime=_HarnessDatetime, timezone=dt.timezone)
    )
    return harness_clock


@pytest.fixture
def s3(monkeypatch) -> ef.InMemoryS3:
    fake = ef.InMemoryS3()
    monkeypatch.setattr(
        run_units, "manifest_sink",
        lambda bucket, s3_client=None: run_units.S3ManifestSink(
            bucket=bucket, prefix=run_units.DEFAULT_MANIFEST_PREFIX, s3_client=fake
        ),
    )

    def _put(key: str, body: bytes, content_type: str) -> None:
        fake.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content_type)

    def _get(key: str) -> bytes:
        return fake.objects[key]

    monkeypatch.setattr(pub, "_s3_io", lambda bucket, dry_run_dir: (_get, _put))
    return fake


@pytest.fixture
def store(monkeypatch) -> UniverseStore:
    universe = UniverseStore()
    monkeypatch.setattr("nousergon_lib.arcticdb.load_universe_ohlcv", universe.load, raising=False)
    return universe


def _manifests(fake: ef.InMemoryS3, case: Case) -> list[dict]:
    prefix = f"data_collection/runs/{case.unit}/{TRADING_DAY}/"
    return [json.loads(fake.objects[k]) for k in sorted(fake.objects) if k.startswith(prefix)]


def _panel_keys(fake: ef.InMemoryS3) -> list[str]:
    return sorted(k for k in fake.objects if k.startswith(f"{dp.KEY_PREFIX}{TRADING_DAY}/"))


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_withheld_vendor_fails_pages_once_and_stands_down(case, s3, store, caplog):
    """The commissioning record for ``vendor_fallback``, on D51's refusal to substitute."""
    caplog.set_level(logging.ERROR)

    # Induce: the store still withholds on the on-demand retry.
    faulted = Execution(s3, case, store, healthy_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A `failed` manifest naming the empty read, on both attempts.
    manifests = _manifests(s3, case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert REFUSAL in m["reason"], m["reason"]
    assert store.reads == [False, False], "the unit read some other source than the one store"
    assert [pit._process_status(a) for a in faulted.attempts] == ["Failed", "Failed"]

    # 2. Exactly one page, after the retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal Fail, and nothing was published for the session:
    # no substituted, thin or partial panel, and no manifest naming one.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert len(faulted.attempts) == 2
    assert _panel_keys(s3) == [], _panel_keys(s3)
    refusals = [r for r in caplog.records if REFUSAL in r.getMessage()]
    assert len(refusals) >= len(faulted.attempts), [r.getMessage() for r in caplog.records]

    # ...and once the store answers, the next run is clean end to end.
    recovered = Execution(s3, case, store, healthy_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True, recovered.completion
    assert [m["status"] for m in _manifests(s3, case)] == ["failed", "failed", "ok"]
    ok = _manifests(s3, case)[-1]
    assert sorted(o["key"] for o in ok["outputs"]) == sorted(case.outputs)
    assert _panel_keys(s3) == sorted(case.outputs)
    published = json.loads(s3.objects[dp.manifest_key(DAY)])
    assert published["row_count"] > 0

    first = manifests[0]
    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} workload={case.workload}"
        f"\n  fault:      {case.fault}"
        "\n  attempts:   " + ", ".join(a["status"] for a in faulted.attempts) + " (exit 1 each)"
        f"\n  manifests:  {', '.join(m['status'] for m in manifests)}: {first['reason'][:110]}..."
        f"\n  pages:      {len(faulted.pages)} {faulted.pages}"
        "\n  path:       " + " > ".join(faulted.path)
        + "\n  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})"
    )


# ---------------------------------------------------------------------------
# What keeps the record honest
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_every_case_declares_this_guard_and_runs_on_its_schedule(case):
    """Commissioned only where the descriptor declares no fallback, on the workload that runs it."""
    unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    block = unit.guards[GUARD_CLASS]
    assert block["state"] == "present", block
    assert "no fallback" in block["note"] and "never substituted" in block["note"], block
    schedule = _panel_input(case)
    assert case.workload in schedule["workloads"]


def test_the_unit_is_the_one_the_runner_maps():
    assert run_units.MODE_UNITS["daily_panel"] == "D51"


def test_the_refusal_is_the_compile_not_a_fallback():
    """``compile_panel`` reads ONE loader and refuses an empty answer; ``run`` never retries
    another source. A second loader, or a swallowed refusal, fails here before any record."""
    import inspect

    compile_src = inspect.getsource(pub.compile_panel)
    assert "from nousergon_lib.arcticdb import load_universe_ohlcv as loader" in compile_src
    assert compile_src.count("loader(") == 1, "compile_panel reads more than one source"
    assert "if not frames:\n        raise PanelCompileError(" in compile_src
    run_src = inspect.getsource(pub.run)
    assert "except PanelCompileError" in run_src and 'result["error"] = str(exc)' in run_src
    with pytest.raises(pub.PanelCompileError, match=REFUSAL):
        pub.compile_panel(BUCKET, trading_day=DAY, loader=lambda *a, **k: {})


def test_no_case_is_on_the_trading_path():
    """D01, D02, D17 and D19 are held for every guard class (the trader's feed and the executor's)."""
    assert not {"D01", "D02", "D17", "D19"} & set(CASE_IDS)


@pytest.mark.parametrize("unit", CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(unit, tmp_path):
    """``faults/<unit>/vendor_fallback/latest.json`` with ``outcome: induced`` reads MET."""
    case = next(c for c in CASES if c.unit == unit)
    descriptor = next(u for u in descriptors.load_units() if u.unit_id == unit)
    fs = LocalStore(tmp_path)
    before = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=DAY)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.HARNESSES[GUARD_CLASS]
    document = gc.record_document(
        unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc.VENDOR_FALLBACK_OBSERVED)
    fs.put_bytes(gc.record_key(unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=DAY)
    assert after.met is True, after.detail


def test_the_publisher_names_this_files_nodes():
    """The publisher runs these exact nodes; a rename here would publish nothing, loudly."""
    harness = gc.HARNESSES[GUARD_CLASS]
    assert harness.test_file == "tests/test_guard_commissioning_vendor_fallback.py"
    assert harness.test_name == (
        test_the_fault_record_withheld_vendor_fails_pages_once_and_stands_down.__name__
    )
    for case in CASES:
        assert gc._fault_for(case.unit, GUARD_CLASS) == case.fault

