"""Guard commissioning, class ``pit``: one induced-fault record per unit.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "one induced fault per guard class
(withheld vendor, truncated universe, unit-scaled column, empty frame, **stale
``as_of``**) -> ``failed``, one page, stand-down") and the phase-2 tracker
`alpha-engine-config-I10794`. A guard that has never fired is not in service
(`observability-policy` §9.1). This is the fifth guard class with a harness,
after ``empty_fresh``, ``success_without_output``, ``cardinality`` and
``units``; it reuses the ``empty_fresh`` machinery (the in-memory S3, the
harness clock, the walker over the deployed ``data-collection.asl.json``).

**Three units, one guard.** D13 (the Saturday ArcticDB rebuild), D18 (the
morning ArcticDB append) and D32 (the post-market ArcticDB append) all declare
their ``pit`` guard as ``UniverseFreshnessViolation``
(``builders/daily_append.py``), and ``contracts/pit.py`` names the same check
as where point-in-time is enforced at append for the ArcticDB libraries these
units publish. It is one function,
``builders.daily_append._scan_universe_and_emit_freshness_receipt``, called at
the tail of each unit's write: ``_daily_append_impl`` (D18, D32) and
``builders.backfill.backfill`` (D13). It reads every in-scope symbol's last row
and raises when any is more than ``UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS``
NYSE sessions behind the reference date.

**The fault is a stale ``as_of``.** One universe member's vendor stops
delivering bars: the library's last row for ``STALLED`` is
``STALLED_LAST`` (5 sessions behind the trading day), so the write completes
for every other member while that one's ``as_of`` stays behind. That is the
2026-04-21 ASGN/MOH partial-write class the guard exists for. One stalled
ticker is under the pre-write missing-from-closes threshold, so the
post-write scan is what has to catch it.

**What runs for real:** the guard itself, over an ArcticDB-shaped library
whose ``tail`` answers each symbol's last row; for D18 and D32 the REAL mode
bodies (``_run_morning_arctic_append`` / ``_run_daily_arctic_append``) under
the REAL ``_run_whole_mode_unit`` wrapper; for D13 the REAL ``_phase_collect``
with the phase-one call site's own arguments; the data repo's own
``RunStatePhaseRegistry`` writing phase markers; the completion check the
dispatcher Lambda runs; and the deployed ``data-collection.asl.json`` walked
state by state on each unit's own schedule input.

**What is modelled, and stated:** the WRITE half of ``daily_append`` and
``backfill``. Both are 1,400+ line ArcticDB writers; the harness replaces each
with a body that lands today's row for every member the vendor delivered,
then calls the real guard with the exact arguments the real call site passes
(``test_the_guard_call_sites_are_what_the_model_calls`` pins those). The
process exit code is the aggregator's own rule, as in the earlier classes.

**The three properties, per the issue's closes-when:**

1. a ``failed`` manifest naming the stale symbol, on the attempt and on the
   on-demand retry (the vendor gap is still there on the retry);
2. exactly one page, ``NotifyFailure``: the process exits 1 on both attempts;
3. stand-down: terminal ``CollectionFailed``, phase marker ``error`` (never
   ``ok``, so nothing auto-skips the refused write), and neither the
   ``health/universe_freshness.json`` receipt nor (D18/D32) the run-date
   sentinel ``feature_store/_universe_close_freshness.json`` that
   ``eod-precondition-probe`` reads was advanced. Once the vendor delivers the
   missing bars, the next run files ``ok``, advances both and pages nobody.

Not here, and why:

* **D25, D26, D28, D48-D51** (``pit: present``): each declares a different
  unit-local check (a minimum-history refusal, a length-mismatch guard, a
  dated snapshot), on another workload; each needs its own walk.
* **D01, D02, D19**: held for every guard class (the trader's feed and the
  executor's); D01/D02 declare ``pit: absent`` in any case.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import functools
import inspect
import io
import json
import logging
import re
import types
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pytest
from nousergon_lib.dates import trading_days_stale

import run_units
import weekly_collector
from builders import _constituents_loader, backfill as backfill_mod
from builders import daily_append as da
from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate import run_manifest_predicate as completion
from data_gate.store import LocalStore
from features.compute import UNIVERSE_BENCHMARK_PROXIES, admits_universe_write
from shadow.run_state import RunStatePhaseRegistry
from tests import test_guard_commissioning_empty_fresh as ef
from tests import test_pit_future_bar_refusal_i10794 as fb
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixtures, used by name
    _environment,
    clock,
)

GUARD_CLASS = "pit"
TRADING_DAY = ef.TRADING_DAY
BUCKET = ef.BUCKET
#: The wall clock the guard grades against: the trading day's evening.
NOW = dt.datetime(2026, 10, 15, 22, 30, tzinfo=dt.timezone.utc)

STOCKS = ("AAPL", "MSFT", "NVDA", "AMZN", "TOST", "IESC", "EME", "WSM")
#: The member whose vendor stopped delivering bars.
STALLED = "MSFT"
#: Its last row: five NYSE sessions before the trading day.
STALLED_LAST = "2026-10-08"
#: Every other member's last row before this run: the prior session.
PRIOR_SESSION = "2026-10-14"
#: The guard's own words (`_scan_universe_and_emit_freshness_receipt`).
REFUSAL = "Universe-freshness scan: 1 symbol(s) older than"

RECEIPT_KEY = da.UNIVERSE_FRESHNESS_RECEIPT_KEY
SENTINEL_KEY = da.UNIVERSE_CLOSE_FRESHNESS_SENTINEL_KEY
_PRIOR = b'{"previous": "cycle"}'


# ---------------------------------------------------------------------------
# The ArcticDB `universe` library, as the guard reads it
# ---------------------------------------------------------------------------


class UniverseLib:
    """``list_symbols`` and ``tail(sym, n=1).data``: the two calls the guard makes."""

    def __init__(self) -> None:
        self.last: dict[str, str] = {s: PRIOR_SESSION for s in (*STOCKS, *UNIVERSE_BENCHMARK_PROXIES)}
        self.last[STALLED] = STALLED_LAST

    def list_symbols(self) -> list[str]:
        return sorted(self.last)

    def tail(self, symbol: str, n: int = 1):
        frame = pd.DataFrame({"Close": [1.0]}, index=pd.DatetimeIndex([pd.Timestamp(self.last[symbol])]))
        return types.SimpleNamespace(data=frame)

    def land(self, symbols, day: str, *, vendor_healthy: bool) -> list[str]:
        """The write half, modelled: today's row lands for every member the vendor delivered."""
        written = [
            s for s in symbols
            if s in self.last and (vendor_healthy or s != STALLED)
        ]
        for s in written:
            self.last[s] = day
        return written


def _requested(expected_tickers) -> list[str]:
    return [t.lstrip("^") for t in expected_tickers if admits_universe_write(t)]


def _modelled_daily_append(s3, lib: UniverseLib, vendor_healthy: bool):
    """``builders.daily_append.daily_append``: write half modelled, the guard real."""

    def daily_append(date_str=None, bucket=da.DEFAULT_BUCKET, dry_run=False,
                     skip_if_exists=False, expected_tickers=None):
        written = lib.land(_requested(expected_tickers), date_str, vendor_healthy=vendor_healthy)
        result = {
            "status": "ok",
            "date": date_str,
            "tickers_appended": len(written),
            "tickers_published": len(written),
        }
        # `_daily_append_impl`'s own tail, verbatim in its arguments.
        receipt = da._scan_universe_and_emit_freshness_receipt(
            s3, bucket, lib,
            expected_tickers=expected_tickers,
            run_date=date_str,
        )
        result["universe_freshness_receipt"] = {
            "n_symbols_checked": receipt["n_symbols_checked"],
            "stalest_symbol": receipt["stalest_symbol"],
        }
        return result

    return daily_append


def _modelled_backfill(s3, lib: UniverseLib, vendor_healthy: bool):
    """``builders.backfill.backfill``: write half modelled, the guard and its re-raise real."""

    def backfill(bucket, dry_run=False, run_date=None):
        constituents_set = set(STOCKS)
        written = lib.land(
            [*sorted(constituents_set), *sorted(UNIVERSE_BENCHMARK_PROXIES)], run_date,
            vendor_healthy=vendor_healthy,
        )
        # backfill.py step 6b: the scan, and its log-then-raise on failure.
        try:
            da._scan_universe_and_emit_freshness_receipt(
                s3=s3,
                bucket=bucket,
                universe_lib=lib,
                expected_tickers=sorted(constituents_set),
            )
        except Exception as exc:
            logging.getLogger("builders.backfill").error(
                "Saturday DataPhase1 universe-freshness emit FAILED: %s.", exc,
            )
            raise
        return {"status": "ok", "tickers_written": len(written), "tickers_errored": 0,
                "tickers_skipped": 0, "macro_dates": 250}

    return backfill


# ---------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------


class _Args:
    def __init__(self) -> None:
        self.date = TRADING_DAY
        self.dry_run = False


@dataclass(frozen=True)
class Case:
    """One unit's induced stale ``as_of``. ``unit`` and ``fault`` are what the publisher reads."""

    unit: str
    #: The phase marker the unit's write is wrapped in.
    phase: str
    #: The deployed workload that runs it, and the schedule whose input names it.
    workload: str
    collection: str
    fault: str
    #: Published library refs a clean run records.
    outputs: tuple[str, ...]
    #: Whether the call site passes ``run_date`` (and so writes the run-date sentinel).
    writes_sentinel: bool
    run: Callable[..., dict] | None = dataclasses.field(default=None, repr=False, compare=False)
    #: The check the unit's descriptor names in its ``pit`` block.
    guard_token: str = "UniverseFreshnessViolation"
    #: A unit whose ``pit`` guard is NOT the shared freshness scan takes its record
    #: through its own walk, ``record(s3, caplog)``, on the same deployed ASL.
    record: Callable[..., None] | None = dataclasses.field(default=None, repr=False, compare=False)


def _run_mode(mode: str, fn_name: str):
    def run(s3, lib: UniverseLib, vendor_healthy: bool) -> dict:
        return weekly_collector._run_whole_mode_unit(
            mode, getattr(weekly_collector, fn_name), {"bucket": BUCKET}, _Args()
        )

    return run


def _run_d13(s3, lib: UniverseLib, vendor_healthy: bool) -> dict:
    """The phase-one ``arcticdb`` call site, with its own arguments."""
    reg = RunStatePhaseRegistry(date=TRADING_DAY, bucket=BUCKET, marker_prefix="data", s3_client=s3)
    reg.data_mode = "phase1"
    backfill = backfill_mod.backfill
    return weekly_collector._phase_collect(
        reg, "arcticdb",
        lambda: backfill(bucket=BUCKET, dry_run=False, run_date=TRADING_DAY),
        supports_auto_skip=False,
        extra_outputs=weekly_collector._arcticdb_backfill_extra_outputs(),
    )


_FAULT = (
    f"stale as_of: one universe member's vendor stopped delivering bars, so its last ArcticDB "
    f"row stays at {STALLED_LAST} (5 NYSE sessions behind {TRADING_DAY}) while every other "
    f"member's row lands; over the {da.UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS}-session "
    "ceiling of UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS"
)

def _record_d03(s3, caplog) -> None:
    """D03's ``pit`` record: the fetch-boundary future-bar refusal (alpha-engine-config-I12676).

    Not the freshness scan: ``collectors/prices.py::_refresh_stale`` reads each
    vendor frame through ``dates.clip_to_trading_day``, whose ``refuse_bars_after``
    fails the read on a bar dated after the run's as-of date. ``prices.collect``
    and ``_refresh_stale`` run for real; only the vendor is stubbed
    (``tests/test_pit_future_bar_refusal_i10794.py`` holds the vendor and the
    cardinality-harness ``Case`` this walk runs through).
    """
    caplog.set_level(logging.ERROR)
    prior = {key: fb.card._PRIOR for key in fb._published_keys()}
    s3.objects.update(prior)

    faulted = fb.card.Execution(s3, fb.CASE, rows_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A failed manifest naming the finding, on the attempt and on the retry.
    manifests = s3.manifests(fb.CASE)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert fb.CASE.refusal in m["reason"], m["reason"]
        assert f"as_of {fb.TRADING_DAY}" in m["reason"], m["reason"]
    assert [a["status"] for a in faulted.attempts] == ["error", "error"], faulted.attempts

    # 2. Exactly one page, after the on-demand retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal fail, phase marker error, nothing the vendor
    # answered was published (the previous cycle's objects are untouched).
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert s3.marker(fb.CASE)["status"] == "error"
    for key, body in prior.items():
        assert s3.objects[key] == body, f"{key} was overwritten past the refusal"
    assert manifests[0]["outputs"] == []
    findings = [
        json.loads(r.getMessage().removeprefix("PIT_FUTURE_BAR "))
        for r in caplog.records if r.getMessage().startswith("PIT_FUTURE_BAR ")
    ]
    assert len(findings) >= 2, [r.getMessage() for r in caplog.records]
    assert {(f["dataset"], f["ticker"], f["as_of"], f["bar_date"]) for f in findings} == {
        ("price_cache_refresh", fb._TICKER, fb.TRADING_DAY, fb._NEXT_DAY),
    }

    # ...and once the vendor answers in bounds, the next run is clean end to end.
    recovered = fb.card.Execution(s3, fb.CASE, rows_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert [m["status"] for m in s3.manifests(fb.CASE)] == ["failed", "failed", "ok"]
    for key in prior:
        published = pd.read_parquet(io.BytesIO(s3.objects[key]))
        assert published.index.max() == pd.Timestamp(fb.TRADING_DAY), key

    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit=D03 phase={fb.CASE.phase} workload={fb.CASE.workload}"
        f"\n  fault:      {fb.CASE.fault}"
        "\n  attempts:   " + ", ".join(a["status"] for a in faulted.attempts) + " (exit 1 each)"
        f"\n  manifests:  {', '.join(m['status'] for m in manifests)}: {manifests[0]['reason'][:110]}..."
        f"\n  pages:      {len(faulted.pages)} {faulted.pages}"
        "\n  path:       " + " > ".join(faulted.path)
        + "\n  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})"
    )


CASES: tuple[Case, ...] = (
    Case(
        "D13", "arcticdb", "weekly-phase-one", "weekly", _FAULT,
        outputs=("arcticdb/universe", "arcticdb/macro"), writes_sentinel=False, run=_run_d13,
    ),
    Case(
        "D18", "morning_arcticdb", "morning-arctic-append", "morning", _FAULT,
        outputs=("arcticdb/universe",), writes_sentinel=True,
        run=_run_mode("morning_arctic_append", "_run_morning_arctic_append"),
    ),
    Case(
        "D32", "arcticdb", "post-market-arctic-append", "eod", _FAULT,
        outputs=("arcticdb/universe",), writes_sentinel=True,
        run=_run_mode("daily_arctic_append", "_run_daily_arctic_append"),
    ),
    Case(
        "D03", fb.CASE.phase, fb.CASE.workload, fb.CASE.collection, fb.CASE.fault,
        outputs=(), writes_sentinel=False,
        guard_token="dates.py::refuse_bars_after", record=_record_d03,
    ),
)
CASE_IDS = [c.unit for c in CASES]


# ---------------------------------------------------------------------------
# One process attempt, on the deployed ASL
# ---------------------------------------------------------------------------


def _deployed_input(collection: str) -> dict:
    """A schedule's execution input, read from the deployed template."""
    match = re.search(
        rf"Input: '(\{{\"collection\": \"{collection}\".*?\}})'", ef.STACK_PATH.read_text()
    )
    assert match, f"the {collection} schedule's Input could not be found in the collection stack"
    return json.loads(match.group(1))


def _process_status(result: dict) -> str:
    """``main()``'s exit rule: ok/skipped/degraded exit 0; anything else exits 1.

    A whole-mode body returns its own status; one ``_phase_collect`` result
    aggregates through ``_finalize`` (``error`` alone is ``failed``).
    """
    return "Success" if result.get("status") in ("ok", "ok_dry_run", "skipped", "degraded") else "Failed"


class Execution(ef.Execution):
    """The ``empty_fresh`` walker, on the case's own schedule, with its unit run for real."""

    def __init__(self, s3, case: Case, lib: UniverseLib, vendor_healthy):
        super().__init__(s3, case, vendor_healthy)
        self.lib = lib
        self.ledger_calls: list[tuple] = []

    def tasks(self, name: str, state: dict, data: dict):
        if name == "PollWorkload":
            if data["workload"] != self.case.workload:
                return {"Status": "Success", "StatusDetails": "Success"}
            healthy = self.rows_per_attempt[len(self.attempts)]
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(da, "daily_append", _modelled_daily_append(self.s3, self.lib, healthy))
                mp.setattr(backfill_mod, "backfill", _modelled_backfill(self.s3, self.lib, healthy))
                mp.setattr(weekly_collector, "_mark_pending_upgrade",
                           lambda *a, **k: self.ledger_calls.append(("mark", a)))
                mp.setattr(weekly_collector, "_clear_pending_upgrade",
                           lambda *a, **k: self.ledger_calls.append(("clear", a)))
                result = self.case.run(self.s3, self.lib, healthy)
            self.attempts.append(result)
            status = _process_status(result)
            return {"Status": status, "StatusDetails": status}
        return super().tasks(name, state, data)

    def run(self) -> str:
        asl = json.loads(ef.ASL_PATH.read_text())
        data = _deployed_input(self.case.collection)
        context = {
            "$$.Execution.Id": "arn:aws:states:::execution:commissioning",
            "$$.Execution.StartTime": self.started_at.isoformat().replace("+00:00", "Z"),
        }
        return ef._walk(asl["States"], asl["StartAt"], data, self.tasks, self.path, context)


@pytest.fixture
def s3(monkeypatch) -> ef.InMemoryS3:
    store = ef.InMemoryS3()
    monkeypatch.setattr(weekly_collector, "_s3_object_exists", lambda _b, key: key in store.objects)
    # The whole-mode wrapper's sink and the mode bodies' own clients, onto the fake.
    monkeypatch.setattr(
        run_units, "manifest_sink",
        lambda bucket, s3_client=None: run_units.S3ManifestSink(
            bucket=bucket, prefix=run_units.DEFAULT_MANIFEST_PREFIX, s3_client=store
        ),
    )
    monkeypatch.setattr(
        weekly_collector, "RunStatePhaseRegistry",
        functools.partial(RunStatePhaseRegistry, s3_client=store),
    )
    monkeypatch.setattr(weekly_collector, "boto3", types.SimpleNamespace(client=lambda *a, **k: store))
    # The membership each append scopes its scan by (D18 reads the dated file, D32 the pointer).
    monkeypatch.setattr(
        _constituents_loader, "load_constituents_for_run_date",
        lambda _s3, _bucket, run_date=None: (set(STOCKS), TRADING_DAY),
    )
    # D32 reads membership through the pointer; the real function ends with the same
    # macro-ticker union D18 applies, so that half is kept.
    monkeypatch.setattr(
        weekly_collector, "_load_daily_universe_tickers",
        lambda _config, _run_date=None: weekly_collector._augment_with_macro_daily_tickers(list(STOCKS)),
    )

    class _GuardClock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)

    monkeypatch.setattr(da, "datetime", _GuardClock)
    monkeypatch.setattr(da, "_active_shadow_root", lambda: None)
    return store


def _seed_prior_cycle(fake: ef.InMemoryS3) -> None:
    fake.objects[RECEIPT_KEY] = _PRIOR
    fake.objects[SENTINEL_KEY] = _PRIOR


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_stale_as_of_fails_pages_once_and_stands_down(case, s3, caplog):  # noqa: F811
    """The commissioning record for ``pit``, on the shared append-time freshness guard
    (D13, D18, D32) or, for a unit with its own ``pit`` check, through its own walk."""
    if case.record is not None:
        case.record(s3, caplog)
        return
    caplog.set_level(logging.ERROR)
    _seed_prior_cycle(s3)
    lib = UniverseLib()

    # Induce: the vendor still withholds the stalled member's bars on the retry.
    faulted = Execution(s3, case, lib, vendor_healthy=[False, False])
    terminal = faulted.run()

    # 1. A `failed` manifest naming the stale symbol, on both attempts.
    manifests = s3.manifests(case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert REFUSAL in m["reason"] and STALLED in m["reason"], m["reason"]
    assert [_process_status(a) for a in faulted.attempts] == ["Failed", "Failed"]

    # 2. Exactly one page, after the retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal Fail, the retry recomputed, the marker is not `ok`,
    # and nothing downstream was told the library is fresh.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert len(faulted.attempts) == 2
    assert s3.marker(case)["status"] == "error"
    assert s3.objects[RECEIPT_KEY] == _PRIOR, "the freshness receipt was advanced over a stale member"
    assert s3.objects[SENTINEL_KEY] == _PRIOR, "the run-date sentinel was advanced over a stale member"
    assert faulted.ledger_calls == [], faulted.ledger_calls
    # The refusal reaches an ERROR line on every attempt (D13 logs it before re-raising;
    # the mode bodies log the append failure with the guard's exception attached).
    refusals = [
        r for r in caplog.records
        if REFUSAL in r.getMessage() or (r.exc_info and REFUSAL in str(r.exc_info[1]))
    ]
    assert len(refusals) >= len(faulted.attempts), [r.getMessage() for r in caplog.records]

    # ...and once the vendor delivers the missing bars, the next run is clean end to end.
    recovered = Execution(s3, case, lib, vendor_healthy=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True, recovered.completion
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed", "ok"]
    ok = s3.manifests(case)[-1]
    assert sorted(o["key"] for o in ok["outputs"]) == sorted(case.outputs)
    receipt = json.loads(s3.objects[RECEIPT_KEY])
    assert receipt["all_fresh"] is True and receipt["stalest_age_trading_days"] == 0, receipt
    if case.writes_sentinel:
        assert json.loads(s3.objects[SENTINEL_KEY])["run_date"] == TRADING_DAY
    else:
        assert s3.objects[SENTINEL_KEY] == _PRIOR

    first = manifests[0]
    print(
        f"\nFAULT RECORD guard={GUARD_CLASS} unit={case.unit} phase={case.phase} workload={case.workload}"
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


def test_the_fault_crosses_the_ceiling_and_the_healthy_row_does_not():
    """5 sessions is over the 3-session ceiling; the prior session's row is within it."""
    ceiling = da.UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS
    assert trading_days_stale(dt.date.fromisoformat(STALLED_LAST), TRADING_DAY) > ceiling
    assert trading_days_stale(dt.date.fromisoformat(PRIOR_SESSION), TRADING_DAY) <= ceiling
    assert admits_universe_write(STALLED)


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_every_case_declares_this_guard_and_runs_on_its_schedule(case):
    """Commissioned only where the descriptor names this check, on the workload that runs it."""
    unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    block = unit.guards[GUARD_CLASS]
    assert block["state"] == "present", block
    assert case.guard_token in (block["note"] + " ".join(block.get("code") or [])), block
    schedule = _deployed_input(case.collection)
    assert case.workload in schedule["workloads"]
    assert case.unit in schedule["verify_units"]


def test_the_units_are_the_ones_the_runner_maps():
    assert run_units.PHASE_UNITS[("phase1", "arcticdb")].unit_id == "D13"
    assert run_units.MODE_UNITS["morning_arctic_append"] == "D18"
    assert run_units.MODE_UNITS["daily_arctic_append"] == "D32"
    assert run_units.PHASE_UNITS[(fb.CASE.mode, "prices")].unit_id == "D03"


def test_the_guard_call_sites_are_what_the_model_calls():
    """The modelled write halves call the guard with the real call sites' arguments.

    A drift here (a call site that stops calling the scan, gates it behind a
    flag, or changes its scoping) fails before any record is taken.
    """
    impl = inspect.getsource(da._daily_append_impl)
    assert re.search(
        r"receipt = _scan_universe_and_emit_freshness_receipt\(\s*s3, bucket, universe_lib,\s*"
        r"expected_tickers=expected_tickers,\s*run_date=date_str,\s*\)",
        impl,
    ), "daily_append's freshness-scan call site changed"
    bf = inspect.getsource(backfill_mod.backfill)
    assert re.search(
        r"if not dry_run and ticker_filter is None:\s*try:\s*receipt = "
        r"_scan_universe_and_emit_freshness_receipt\(\s*s3=s3,\s*bucket=bucket,\s*"
        r"universe_lib=universe_lib,\s*expected_tickers=sorted\(constituents_set\),\s*\)",
        bf,
    ), "backfill's freshness-scan call site changed"
    # backfill re-raises a failed scan; it never swallows it.
    tail = bf[bf.index("Saturday DataPhase1 universe-freshness emit FAILED"):]
    assert re.match(r"[^\n]*\n(?:[^\n]*\n){0,4}\s*\)\s*\n\s*raise\n", tail), "backfill stopped re-raising"
    # The phase-one call site passes exactly what `_run_d13` passes.
    source = (ef.REPO_ROOT / "weekly_collector.py").read_text()
    assert re.search(
        r'_phase_collect\(\n\s*reg, "arcticdb",\n\s*lambda: backfill\(bucket=bucket, dry_run=dry_run, '
        r"run_date=run_date\),\n\s*supports_auto_skip=False,",
        source,
    ), "the phase-one arcticdb call site changed"


def test_no_case_is_on_the_trading_path():
    """D01, D02 and D19 are held for every guard class (the trader's feed and the executor's)."""
    assert not {"D01", "D02", "D19"} & set(CASE_IDS)


@pytest.mark.parametrize("unit", CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(unit, tmp_path):
    """``faults/<unit>/pit/latest.json`` with ``outcome: induced`` reads MET."""
    case = next(c for c in CASES if c.unit == unit)
    descriptor = next(u for u in descriptors.load_units() if u.unit_id == unit)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(TRADING_DAY)
    before = evidence.read_guard_commissioning(store, descriptor, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    harness = gc.HARNESSES[GUARD_CLASS]
    document = gc.record_document(
        unit, GUARD_CLASS, fault=case.fault, test=harness.nodeid(unit), code_sha="c" * 40,
    )
    assert document["observed"] == list(gc._observed_for(GUARD_CLASS, unit))
    store.put_bytes(gc.record_key(unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, descriptor, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def test_the_publisher_names_this_files_nodes():
    """The publisher runs these exact nodes; a rename here would publish nothing, loudly."""
    harness = gc.HARNESSES[GUARD_CLASS]
    assert harness.test_file == "tests/test_guard_commissioning_pit.py"
    assert harness.test_name == test_the_fault_record_stale_as_of_fails_pages_once_and_stands_down.__name__
    for case in CASES:
        assert gc._fault_for(case.unit, GUARD_CLASS) == case.fault
