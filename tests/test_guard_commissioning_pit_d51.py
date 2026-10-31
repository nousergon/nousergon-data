"""Guard commissioning, class ``pit``, unit D51: the daily panel refuses a stale store.

`alpha-engine-config-I10786` (plan item P-19: "one induced fault per guard class
(... **stale ``as_of``**) -> ``failed``, one page, stand-down") and
`alpha-engine-config-I10782` (P-15: the ``data.<unit>.guard.pit`` clause, whose
commissioning half reads ``faults/D51/pit/latest.json``).

**Why a separate file from the class harness.** ``tests/test_guard_commissioning_pit.py``
walks the append-time freshness scan D13, D18 and D32 share. D51's point-in-time
check is a different one: ``contracts/daily_panel.py::validate_panel_frame``
refuses a panel whose window does not end on its own ``trading_day``, and
``builders.daily_panel.compile_panel`` raises that as ``PanelCompileError``
before anything is written. So a panel keyed 2026-10-02 can never carry
2026-10-01's prices. It runs on the panel schedule, not the EOD one.
``data_gate.guard_commissioning.UNIT_HARNESSES`` routes D51's ``pit`` record
here.

**The fault is a stale ``as_of``.** The ArcticDB universe library answers every
symbol, but its newest row is the PRIOR session: the post-market append (D32)
did not land the trading day's bars. It persists into the on-demand retry.
Everything that runs for real, and everything modelled, is as in the
``vendor_fallback`` harness this file reuses
(``tests/test_guard_commissioning_vendor_fallback.py``).
"""

from __future__ import annotations

import json
import logging

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("pyarrow")

import pandas as pd  # noqa: E402

from contracts import daily_panel as dp  # noqa: E402
from data_gate import descriptors, evidence  # noqa: E402
from data_gate import guard_commissioning as gc  # noqa: E402
from data_gate.store import LocalStore  # noqa: E402
from tests import test_guard_commissioning_vendor_fallback as vf  # noqa: E402
from tests.test_daily_panel_publisher import DAY, _frames  # noqa: E402
from tests.test_guard_commissioning_empty_fresh import (  # noqa: E402,F401 -- fixture, used by name
    _environment,
)
from tests.test_guard_commissioning_vendor_fallback import (  # noqa: E402,F401 -- fixtures
    clock,
    s3,
)

GUARD_CLASS = "pit"
TRADING_DAY = DAY.isoformat()
#: The session the stale store's newest row is on.
PRIOR_SESSION = "2026-10-01"
#: The refusal's own words (`contracts.daily_panel.validate_panel_frame`); the tail of
#: the sentence carries an apostrophe the manifest reason escapes, so it is not matched.
REFUSAL = f"the window ends on {PRIOR_SESSION}"


def _stale_frames() -> dict[str, pd.DataFrame]:
    """The fixture universe with every row on the trading day removed."""
    return {t: f[f.index < pd.Timestamp(DAY)] for t, f in _frames().items()}


class StaleStore(vf.UniverseStore):
    """Every symbol answers, ending on the prior session, until the day's rows land."""

    def load(self, bucket, *, symbols=None, lookback_days=None, end=None, region=None):
        self.reads.append(self.healthy)
        return _frames() if self.healthy else _stale_frames()


_FAULT = (
    "stale as_of: the ArcticDB universe library answers every symbol, but its newest row is "
    f"the prior session, so a panel keyed {TRADING_DAY} would carry an earlier session's "
    "prices; on the attempt and on the retry"
)

CASES: tuple[vf.Case, ...] = (
    vf.Case(
        "D51", "daily-panel", "panel", _FAULT,
        outputs=(dp.panel_key(DAY), dp.manifest_key(DAY)),
        run=vf._run_d51,
    ),
)
CASE_IDS = [c.unit for c in CASES]


@pytest.fixture
def store(monkeypatch) -> StaleStore:
    universe = StaleStore()
    monkeypatch.setattr("nousergon_lib.arcticdb.load_universe_ohlcv", universe.load, raising=False)
    return universe


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_stale_store_fails_pages_once_and_stands_down(case, s3, store, caplog):  # noqa: F811
    """The commissioning record for D51 ``pit``, on the panel's window-end refusal."""
    caplog.set_level(logging.ERROR)

    faulted = vf.Execution(s3, case, store, healthy_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A `failed` manifest naming the window end, on both attempts.
    manifests = vf._manifests(s3, case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert REFUSAL in m["reason"], m["reason"]
    assert store.reads == [False, False]
    assert [vf.pit._process_status(a) for a in faulted.attempts] == ["Failed", "Failed"]

    # 2. Exactly one page, after the retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal Fail, nothing published for the session.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert vf._panel_keys(s3) == [], vf._panel_keys(s3)
    refusals = [r for r in caplog.records if REFUSAL in r.getMessage()]
    assert len(refusals) >= len(faulted.attempts), [r.getMessage() for r in caplog.records]

    # ...and once the session's rows land, the next run is clean end to end.
    recovered = vf.Execution(s3, case, store, healthy_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.pages == []
    assert recovered.completion["ok"] is True, recovered.completion
    assert [m["status"] for m in vf._manifests(s3, case)] == ["failed", "failed", "ok"]
    assert vf._panel_keys(s3) == sorted(case.outputs)
    published = json.loads(s3.objects[dp.manifest_key(DAY)])
    assert published["trading_day"] == TRADING_DAY

    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} workload={case.workload}"
        f"\n  fault:      {case.fault}"
        f"\n  manifests:  {', '.join(m['status'] for m in manifests)}: {manifests[0]['reason'][:110]}..."
        f"\n  pages:      {len(faulted.pages)} {faulted.pages}"
        "\n  path:       " + " > ".join(faulted.path)
        + "\n  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})"
    )


def test_the_stale_store_really_ends_before_the_trading_day():
    newest = max(f.index.max() for f in _stale_frames().values())
    assert newest.date().isoformat() == PRIOR_SESSION
    assert max(f.index.max() for f in _frames().values()).date() == DAY


def test_d51_declares_this_check_and_runs_on_its_schedule():
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D51")
    block = unit.guards[GUARD_CLASS]
    assert block["state"] == "present", block
    assert "builders/daily_panel.py::compile_panel" in block["code"], block
    assert "daily-panel" in vf._panel_input(CASES[0])["workloads"]


def test_the_record_shape_is_what_the_phase_two_gate_reads(tmp_path):
    """``faults/D51/pit/latest.json`` with ``outcome: induced`` reads MET on the commissioning half."""
    case = CASES[0]
    descriptor = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    fs = LocalStore(tmp_path)
    before = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=DAY)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.harness_for(GUARD_CLASS, case.unit)
    document = gc.record_document(
        case.unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(case.unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc.PIT_PANEL_WINDOW_OBSERVED)
    fs.put_bytes(gc.record_key(case.unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=DAY)
    assert after.met is True, after.detail


def test_the_publisher_routes_d51_here_and_the_class_harness_keeps_its_units():
    harness = gc.harness_for(GUARD_CLASS, "D51")
    assert harness.test_file == "tests/test_guard_commissioning_pit_d51.py"
    assert harness.test_name == test_the_fault_record_stale_store_fails_pages_once_and_stands_down.__name__
    assert gc._fault_for("D51", GUARD_CLASS) == CASES[0].fault
    for unit in ("D13", "D18", "D32"):
        assert gc.harness_for(GUARD_CLASS, unit) is gc.HARNESSES[GUARD_CLASS]

