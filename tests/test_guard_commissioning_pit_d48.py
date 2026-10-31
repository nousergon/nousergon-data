"""Guard commissioning, class ``pit``, unit D48: a stale index proxy is refused.

`alpha-engine-config-I10786` (plan item P-19: "one induced fault per guard class
(... **stale ``as_of``**) -> ``failed``, one page, stand-down") and
`alpha-engine-config-I10782` (P-15: the ``data.D48.guard.pit`` clause, whose
commissioning half reads ``faults/D48/pit/latest.json``).

**The check.** D48 (per-constituent index contribution) reads the trading-day
pair off each index proxy's own series rather than a calendar. The NDX proxy,
QQQ, is read from a SECOND source (the ``close_history`` spine D21 publishes),
so it can lag the members. ``collectors/index_contributions.py::
_refuse_proxy_behind_members`` refuses a proxy whose newest close is older than
the members' newest: otherwise the pair is one session behind and a multi-day
move is labelled as one day. The leg is recorded in ``errors``, the run returns
``partial``, the whole-mode wrapper files ``failed`` and ``main`` exits 1.

**The fault is a stale ``as_of``.** D21 did not refresh ``QQQ.json``: QQQ's
newest close is the prior session while every member reaches the trading day.
It persists into the on-demand retry. The SPX leg (proxy SPY, read with the
members) is healthy throughout, which is what the per-index soft-fail is for.

**What runs for real:** ``collectors.index_contributions.collect`` and every
refusal in it, ``weekly_collector._run_index_contributions`` under the REAL
``_run_whole_mode_unit`` wrapper and run-manifest sink, the completion check,
and the deployed ``data-collection.asl.json`` walked on the
``index-contributions`` schedule's own input. **Modelled, and stated:** the
three read seams ``collect`` already injects (weights, member closes, the
close_history spine) and the S3 client. Machinery is shared with
``tests/test_guard_commissioning_vendor_fallback.py``;
``data_gate.guard_commissioning.UNIT_HARNESSES`` routes D48's ``pit`` record
here.
"""

from __future__ import annotations

import json
import logging
import types

import pytest

from collectors import index_contributions as ic
from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate.store import LocalStore
from tests import test_guard_commissioning_vendor_fallback as vf
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixture, used by name
    _environment,
)
from tests.test_guard_commissioning_vendor_fallback import (  # noqa: F401 -- fixtures
    clock,
    s3,
)

GUARD_CLASS = "pit"
TRADING_DAY = vf.TRADING_DAY  # 2026-10-02, a Friday session
PRIOR_SESSION = "2026-10-01"
SESSION_BEFORE = "2026-09-30"
#: The NDX weights D49 published for the week: one session old, inside the 5-session bound.
NDX_WEIGHTS_AS_OF = PRIOR_SESSION
#: The refusal's own words (`_refuse_proxy_behind_members`).
REFUSAL = f"proxy QQQ (close_history) ends at {PRIOR_SESSION}"

SPX_WEIGHTS = {"AAPL": 0.6, "MSFT": 0.4}
NDX_WEIGHTS = {"AAPL": 0.5, "NVDA": 0.5}
_CLOSES = {"AAPL": (230.0, 228.0), "MSFT": (420.0, 418.0), "NVDA": (180.0, 177.0),
           "SPY": (570.0, 568.0), "QQQ": (490.0, 487.0)}


def _series(sym: str, newest: str, older: str) -> list[tuple[str, float]]:
    today, before = _CLOSES[sym]
    return [(newest, today), (older, before)]


def _keys(index: str) -> tuple[str, str]:
    return (f"{ic.INDEX_CONTRIBUTIONS_PREFIX}{index}/{TRADING_DAY}.json",
            f"{ic.INDEX_CONTRIBUTIONS_PREFIX}{index}/latest.json")


class Sources:
    """The three read seams ``collect`` injects. ``healthy`` is whether D21 refreshed QQQ."""

    def __init__(self) -> None:
        self.healthy = True
        self.reads: list[bool] = []

    def weights(self, index: str) -> ic.Weights:
        if index == "SPX":
            return ic.Weights(SPX_WEIGHTS, "float_cap", as_of=PRIOR_SESSION)
        return ic.Weights(NDX_WEIGHTS, "modified_cap_approx", as_of=NDX_WEIGHTS_AS_OF)

    def closes(self, symbols, run_date, n):
        return {s: _series(s, TRADING_DAY, PRIOR_SESSION) for s in symbols if s in _CLOSES}

    def close_history(self, symbols, run_date, n):
        self.reads.append(self.healthy)
        if self.healthy:
            return {s: _series(s, TRADING_DAY, PRIOR_SESSION) for s in symbols}
        return {s: _series(s, PRIOR_SESSION, SESSION_BEFORE) for s in symbols}


def _run_d48() -> dict:
    return vf.weekly_collector._run_whole_mode_unit(
        "index_contributions", vf.weekly_collector._run_index_contributions,
        {"bucket": vf.BUCKET}, vf._Args(),
    )


_FAULT = (
    "stale as_of: the NDX proxy QQQ, read from the close_history spine D21 publishes, was not "
    f"refreshed, so its newest close is {PRIOR_SESSION} while every member's reaches "
    f"{TRADING_DAY}; on the attempt and on the retry"
)

CASES: tuple[vf.Case, ...] = (
    vf.Case(
        "D48", "index-contributions", "index-contributions", _FAULT,
        outputs=(*_keys("SPX"), *_keys("NDX")),
        run=_run_d48,
    ),
)
CASE_IDS = [c.unit for c in CASES]
_PRIOR_NDX = b'{"previous": "session"}'


@pytest.fixture
def store(monkeypatch, s3) -> Sources:  # noqa: F811
    sources = Sources()
    monkeypatch.setattr(ic, "_default_weights_source", lambda bucket: sources.weights)
    monkeypatch.setattr(ic, "_default_closes_source", lambda bucket: sources.closes)
    monkeypatch.setattr(ic, "_default_close_history_source", lambda bucket: sources.close_history)
    monkeypatch.setattr(ic, "boto3", types.SimpleNamespace(client=lambda *a, **k: s3))
    return sources


def _written(fake) -> list[str]:
    return sorted(k for k in fake.objects if k.startswith(ic.INDEX_CONTRIBUTIONS_PREFIX))


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_stale_proxy_fails_pages_once_and_stands_down(case, s3, store, caplog):  # noqa: F811
    """The commissioning record for D48 ``pit``, on the stale-proxy refusal."""
    caplog.set_level(logging.ERROR)
    s3.objects[_keys("NDX")[1]] = _PRIOR_NDX

    faulted = vf.Execution(s3, case, store, healthy_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A `failed` manifest naming the stale proxy, on both attempts.
    manifests = vf._manifests(s3, case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert REFUSAL in m["reason"], m["reason"]
    assert store.reads == [False, False]
    assert [a["status"] for a in faulted.attempts] == ["partial", "partial"]
    assert [vf.pit._process_status(a) for a in faulted.attempts] == ["Failed", "Failed"]

    # 2. Exactly one page, after the retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal Fail; no NDX decomposition was published for the
    # session and yesterday's NDX latest.json stands. The healthy SPX leg did publish.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert _keys("NDX")[0] not in s3.objects
    assert s3.objects[_keys("NDX")[1]] == _PRIOR_NDX
    assert set(_keys("SPX")) <= set(s3.objects)
    refusals = [r for r in caplog.records if REFUSAL in r.getMessage()]
    assert len(refusals) >= len(faulted.attempts), [r.getMessage() for r in caplog.records]

    # ...and once D21 refreshes QQQ, the next run is clean end to end.
    recovered = vf.Execution(s3, case, store, healthy_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.pages == []
    assert recovered.completion["ok"] is True, recovered.completion
    assert [m["status"] for m in vf._manifests(s3, case)] == ["failed", "failed", "ok"]
    assert _written(s3) == sorted(case.outputs)
    ndx = json.loads(s3.objects[_keys("NDX")[1]])
    assert ndx["trading_day"] == TRADING_DAY

    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} workload={case.workload}"
        f"\n  fault:      {case.fault}"
        f"\n  manifests:  {', '.join(m['status'] for m in manifests)}: {manifests[0]['reason'][:110]}..."
        f"\n  pages:      {len(faulted.pages)} {faulted.pages}"
        "\n  path:       " + " > ".join(faulted.path)
        + "\n  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})"
    )


def test_ndx_reads_its_proxy_from_the_second_source():
    """The fault is only reachable because QQQ is read off the close_history spine."""
    ndx = next(s for s in ic.INDEXES if s.index == "NDX")
    assert ndx.proxy_symbol == "QQQ" and ndx.proxy_source == ic.PROXY_SOURCE_CLOSE_HISTORY


def test_d48_declares_this_check_and_runs_on_its_schedule():
    unit = next(u for u in descriptors.load_units() if u.unit_id == "D48")
    block = unit.guards[GUARD_CLASS]
    assert block["state"] == "present", block
    assert "collectors/index_contributions.py::collect" in block["code"], block
    assert "index-contributions" in vf._panel_input(CASES[0])["workloads"]


def test_the_record_shape_is_what_the_phase_two_gate_reads(tmp_path):
    """``faults/D48/pit/latest.json`` with ``outcome: induced`` reads MET on the commissioning half."""
    case = CASES[0]
    descriptor = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    fs = LocalStore(tmp_path)
    day = vf.DAY
    before = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.harness_for(GUARD_CLASS, case.unit)
    document = gc.record_document(
        case.unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(case.unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc.PIT_STALE_PROXY_OBSERVED)
    fs.put_bytes(gc.record_key(case.unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(fs, descriptor, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def test_the_publisher_routes_d48_here():
    harness = gc.harness_for(GUARD_CLASS, "D48")
    assert harness.test_file == "tests/test_guard_commissioning_pit_d48.py"
    assert harness.test_name == test_the_fault_record_stale_proxy_fails_pages_once_and_stands_down.__name__
    assert gc._fault_for("D48", GUARD_CLASS) == CASES[0].fault
