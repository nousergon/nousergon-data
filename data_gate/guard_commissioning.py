"""Guard commissioning records: build them, and publish them only from a passing run.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate). `observability-policy` §9.1: a guard that
has never fired is not in service. The phase-2 gate grades that per unit and
guard class by reading ONE document,
``<store>/faults/<unit>/<guard>/latest.json`` with ``outcome: induced``
(:func:`data_gate.evidence.read_guard_commissioning`).

Until this module, nothing wrote that key: nousergon-data#2069 commissioned
D19 ``empty_fresh`` inside pytest and published nothing, so every guard clause
read UNMET on a record that could not exist.

**The record is evidence only because of what produced it.** The gate MET-s a
clause on the document alone, so the document must never be written by hand
or from a run that did not happen. :func:`main` therefore:

1. runs the induced-fault test for each requested unit itself, by its exact
   pytest node id, and reads the per-node outcome back from a JUnit report;
2. writes NOTHING unless every requested node PASSED (all-or-nothing: a
   partial publish would read as "commissioned" for the units that happened
   to pass while hiding the one that did not);
3. stamps each record with the commit the test ran at, and refuses
   ``--write`` from a dirty tree, because a record naming a commit whose code
   is not what ran is not evidence;
4. defaults to a dry run that prints the documents and writes nothing.

Only guard classes with an induced-fault harness are publishable
(:data:`HARNESSES`). Adding a class is adding its harness and one row here.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

__all__ = [
    "CARDINALITY_OBSERVED",
    "EMPTY_FRESH_OBSERVED",
    "HARNESSES",
    "PIT_FUTURE_BAR_OBSERVED",
    "PIT_OBSERVED",
    "PIT_OBSERVED_BY_UNIT",
    "SUCCESS_WITHOUT_OUTPUT_OBSERVED",
    "UNITS_OBSERVED",
    "VENDOR_FALLBACK_OBSERVED",
    "PIT_PANEL_WINDOW_OBSERVED",
    "PIT_STALE_PROXY_OBSERVED",
    "UNIT_HARNESSES",
    "harness_for",
    "Harness",
    "main",
    "record_document",
    "record_key",
]

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_STORE = "s3://alpha-engine-research/data_collection"


#: What the ``empty_fresh`` harness observes. A guard that RAISES (manifest
#: ``failed``, process exit 1) is retried on demand and pages through
#: ``NotifyFailure``.
EMPTY_FRESH_OBSERVED: tuple[str, ...] = (
    "manifest status failed on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL",
    "stand-down: terminal CollectionFailed, phase marker error (not ok)",
    "recovery: the next clean run reaches CollectionSucceeded with 0 pages",
)

#: What the ``success_without_output`` harness observes. That guard does not move
#: the exit code (`run_units.record_empty_production`): the process reports
#: success, so nothing retries, and the page comes from the completion check
#: reading the ``failed`` manifest (``run_not_ok``), not from ``NotifyFailure``.
SUCCESS_WITHOUT_OUTPUT_OBSERVED: tuple[str, ...] = (
    "manifest status failed (EmptyProduction) while the collector reported status ok",
    "exactly one page (NotifyCompletionFindings, failure_mode run_not_ok) on the deployed "
    "data-collection ASL; the process exited 0, so no on-demand retry",
    "stand-down: terminal RunNotOk; a same-date rerun that still publishes nothing files "
    "failed again (never carried forward to ok)",
    "recovery: the next clean run reaches CollectionSucceeded with 0 pages",
)

#: What the ``cardinality`` harness observes. No shared chokepoint enforces this
#: class (`validators/expectations.py::CARDINALITY_GUARD` is staged OBSERVE with
#: no raise site), so the record is taken on each unit's OWN coverage check, the
#: one its descriptor's ``guards.cardinality.code`` names, run for real over a
#: vendor that answers for a truncated universe. A raise (``status: error``) or a
#: ``partial`` result both exit 1, so the retry runs and the page is
#: ``NotifyFailure``.
CARDINALITY_OBSERVED: tuple[str, ...] = (
    "manifest status failed naming the truncated universe, on the attempt and on the "
    "on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL; the process "
    "exited 1 on both attempts",
    "stand-down: terminal CollectionFailed; the retry recomputed, and the truncated answer "
    "overwrote none of the previously published keys it did not cover",
    "recovery: the next clean run reaches CollectionSucceeded with 0 pages",
)


#: What the ``units`` harness observes. No shared chokepoint and no staged mode:
#: the record is taken on the unit's OWN units detection, the check its
#: descriptor's ``guards.units.code`` / ``evaluates`` names, as shipped. The
#: refusal's consequence depends on the box-local roster cache, so both are
#: observed. The weekly phase-one run is not walked, so no page count is claimed.
UNITS_OBSERVED: tuple[str, ...] = (
    "the unit's own units detection refused a weight column in neither declared band "
    "and raised inside the fetch",
    "cold box (no roster cache, the deployed weekly spot): manifest status failed naming "
    "the refusal, phase marker error, none of the unit's published keys written, the "
    "previous snapshot untouched",
    "warm box (roster cache present): the refused weights were never published; the cached "
    "roster was served with weight_method cache_no_weights and an empty weight_map, and the "
    "downstream weight consumer refused that empty map",
    "recovery: the next correctly scaled file publishes ok with every weight normalised to "
    "1.0 within its index",
)

#: What the ``vendor_fallback`` harness observes. The guard is the unit's refusal to
#: substitute a source: D51 compiles from one store and declares no fallback, so a
#: withheld read raises (``builders.daily_panel.compile_panel``) as shipped; no staged
#: mode. The unit runs on its own schedule input (``ne-data-collection-panel``).
VENDOR_FALLBACK_OBSERVED: tuple[str, ...] = (
    "the one store the unit reads answered nothing and the unit refused to compile "
    "(PanelCompileError); no second source was read and nothing was substituted",
    "manifest status failed naming the empty read, on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL, walked on the "
    "unit's own schedule input; the process exited 1 on both attempts",
    "stand-down: terminal CollectionFailed; nothing under the session's panel prefix was "
    "written (no parquet, no manifest)",
    "recovery: once the store answers, the next run publishes both keys and reaches "
    "CollectionSucceeded with 0 pages",
)


#: What the ``pit`` harness observes. The guard is the append-time freshness scan
#: ``builders/daily_append.py::_scan_universe_and_emit_freshness_receipt``
#: (``UniverseFreshnessViolation``), shared by D13, D18 and D32 and run as shipped;
#: no staged mode. Each unit's write half is modelled; the guard and the runner are real.
PIT_OBSERVED: tuple[str, ...] = (
    "the append-time freshness scan refused a universe member whose last row is past the "
    "UNIVERSE_FRESHNESS_MAX_STALE_TRADING_DAYS ceiling (UniverseFreshnessViolation)",
    "manifest status failed naming the stale symbol, on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL, walked on the "
    "unit's own schedule input; the process exited 1 on both attempts",
    "stand-down: terminal CollectionFailed, phase marker error; neither "
    "health/universe_freshness.json nor the run-date sentinel was advanced",
    "recovery: once the missing bars land, the next run reaches CollectionSucceeded with 0 "
    "pages and an all-fresh receipt",
)

#: What D03's ``pit`` walk observes (alpha-engine-config-I10794, ruling (a)): the
#: fetch-boundary refusal ``dates.refuse_bars_after`` (``FutureBarRefused``), which
#: ``collectors/prices.py`` reaches through ``clip_to_trading_day``. A different
#: check from the append-time freshness scan :data:`PIT_OBSERVED` describes, so
#: the record names it per unit (:data:`PIT_OBSERVED_BY_UNIT`), never class-wide.
#: Taken by ``tests/test_pit_future_bar_refusal_i10794.py``; publishable once D03
#: declares ``pit: present`` and joins the ``pit`` harness's ``CASES``.
PIT_FUTURE_BAR_OBSERVED: tuple[str, ...] = (
    "the fetch-boundary point-in-time bound refused a vendor answer carrying a bar dated after "
    "the run's as-of date (FutureBarRefused, finding pit_future_bar naming dataset, ticker, "
    "as_of and bar_date); nothing was trimmed",
    "manifest status failed naming the finding, on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL; the process exited 1 "
    "on both attempts",
    "stand-down: terminal CollectionFailed, phase marker error; no price-cache key was "
    "overwritten past the refusal",
    "recovery: once the vendor answers within the requested end, the next run reaches "
    "CollectionSucceeded with 0 pages",
)

#: What D51's ``pit`` walk observes: the daily panel's own window-end check
#: (``contracts/daily_panel.py::validate_panel_frame``, raised as
#: ``PanelCompileError`` by ``builders.daily_panel.compile_panel``). A different
#: check from the append-time freshness scan, on a different workload, so it has
#: its own harness (:data:`UNIT_HARNESSES`).
PIT_PANEL_WINDOW_OBSERVED: tuple[str, ...] = (
    "the panel compile refused a store whose newest row is the prior session: the window "
    "ends before the panel's trading_day (PanelCompileError); no earlier session was "
    "published under the later session's key",
    "manifest status failed naming the window end, on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL, walked on the "
    "unit's own schedule input; the process exited 1 on both attempts",
    "stand-down: terminal CollectionFailed; nothing under the session's panel prefix was "
    "written (no parquet, no manifest)",
    "recovery: once the session's rows land, the next run publishes a panel ending on its "
    "trading_day and reaches CollectionSucceeded with 0 pages",
)

#: What D48's ``pit`` walk observes: the refusal of an index proxy read from a
#: second source that lags the members
#: (``collectors/index_contributions.py::_refuse_proxy_behind_members``), so the
#: trading-day pair is never one session behind. Its own harness (:data:`UNIT_HARNESSES`).
PIT_STALE_PROXY_OBSERVED: tuple[str, ...] = (
    "the NDX leg refused a proxy whose newest close is older than the members' newest "
    "(IndexContributionsUnavailable); the run returned partial and the healthy SPX leg "
    "still published",
    "manifest status failed naming the stale proxy, on the attempt and on the on-demand retry",
    "exactly one page (NotifyFailure) on the deployed data-collection ASL, walked on the "
    "unit's own schedule input; the process exited 1 on both attempts",
    "stand-down: terminal CollectionFailed; no NDX decomposition was written for the session "
    "and the previous NDX latest.json stands",
    "recovery: once the proxy is refreshed, the next run publishes both legs and reaches "
    "CollectionSucceeded with 0 pages",
)

#: guard class -> unit -> its observed lines, where a unit's check is not the
#: class-wide one its harness's default ``observed`` describes.
PIT_OBSERVED_BY_UNIT: dict[str, tuple[str, ...]] = {"D03": PIT_FUTURE_BAR_OBSERVED}
_OBSERVED_BY_UNIT: dict[str, dict[str, tuple[str, ...]]] = {"pit": PIT_OBSERVED_BY_UNIT}



class Harness:
    """Where a guard class's induced-fault record is taken, and what it induces."""

    def __init__(
        self,
        test_file: str,
        test_name: str,
        mode_under_test: str,
        *,
        cases_module: str,
        observed: tuple[str, ...],
    ) -> None:
        self.test_file = test_file
        self.test_name = test_name
        self.mode_under_test = mode_under_test
        #: The ``tests`` module whose ``CASES`` declare each unit's induced fault.
        self.cases_module = cases_module
        #: What the harness asserts, carried onto the record verbatim.
        self.observed = observed

    def nodeid(self, unit: str) -> str:
        return f"{self.test_file}::{self.test_name}[{unit}]"


#: guard class -> its induced-fault harness. The record is taken against the
#: PROMOTED guard (`enforce`): the phase-2 exit reads "every applicable guard
#: clause enforcing and commissioned", and an observe-mode verdict has no
#: consequence to commission (nousergon-data#2069's design, I10786 2026-10-05).
HARNESSES: dict[str, Harness] = {
    "empty_fresh": Harness(
        "tests/test_guard_commissioning_empty_fresh.py",
        "test_the_fault_record_empty_frame_fails_pages_once_and_stands_down",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_empty_fresh",
        observed=EMPTY_FRESH_OBSERVED,
    ),
    # `data_success_without_output` ships ENFORCING (`run_units.record_empty_production`
    # files `failed`); there is no staged mode flip, so the record is taken against
    # the code exactly as shipped.
    "success_without_output": Harness(
        "tests/test_guard_commissioning_success_without_output.py",
        "test_the_fault_record_ok_with_no_output_fails_pages_once_and_stands_down",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_success_without_output",
        observed=SUCCESS_WITHOUT_OUTPUT_OBSERVED,
    ),
    # No staged mode flip either: each unit's own coverage check raises (or
    # files `partial`) as shipped, and that is the check the record is taken on.
    "cardinality": Harness(
        "tests/test_guard_commissioning_cardinality.py",
        "test_the_fault_record_truncated_universe_fails_pages_once_and_stands_down",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_cardinality",
        observed=CARDINALITY_OBSERVED,
    ),
    # No staged mode flip: the unit's own units detection refuses as shipped. D01's is
    # not the observe-only `data_cardinality`/`data_empty_fresh` staging the trader-feed
    # ruling covers (nousergon-data#2145), so its record names the live behaviour.
    "units": Harness(
        "tests/test_guard_commissioning_units.py",
        "test_the_fault_record_unit_scaled_column_is_refused_and_never_published",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_units",
        observed=UNITS_OBSERVED,
    ),
    # No staged mode flip: the shared append-time freshness scan raises as shipped,
    # at the tail of D13's rebuild and of D18/D32's appends.
    "pit": Harness(
        "tests/test_guard_commissioning_pit.py",
        "test_the_fault_record_stale_as_of_fails_pages_once_and_stands_down",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_pit",
        observed=PIT_OBSERVED,
    ),
    # No staged mode flip: D51's compile refuses an empty read as shipped, and its
    # descriptor declares no fallback path, so the withheld vendor is refused, never
    # absorbed.
    "vendor_fallback": Harness(
        "tests/test_guard_commissioning_vendor_fallback.py",
        "test_the_fault_record_withheld_vendor_fails_pages_once_and_stands_down",
        mode_under_test="enforce",
        cases_module="test_guard_commissioning_vendor_fallback",
        observed=VENDOR_FALLBACK_OBSERVED,
    ),
}

#: guard class -> unit -> the harness that unit's record is taken in, where the
#: unit's check is not the one the class harness walks. D51's ``pit`` check is the
#: panel's window-end refusal and D48's the stale-proxy refusal, each on its own
#: schedule, not the append-time freshness scan :data:`HARNESSES` ``["pit"]``
#: walks, so each gets its own record test rather than a case bent to fit another
#: check's assertions.
UNIT_HARNESSES: dict[str, dict[str, Harness]] = {
    "pit": {
        "D51": Harness(
            "tests/test_guard_commissioning_pit_d51.py",
            "test_the_fault_record_stale_store_fails_pages_once_and_stands_down",
            mode_under_test="enforce",
            cases_module="test_guard_commissioning_pit_d51",
            observed=PIT_PANEL_WINDOW_OBSERVED,
        ),
        "D48": Harness(
            "tests/test_guard_commissioning_pit_d48.py",
            "test_the_fault_record_stale_proxy_fails_pages_once_and_stands_down",
            mode_under_test="enforce",
            cases_module="test_guard_commissioning_pit_d48",
            observed=PIT_STALE_PROXY_OBSERVED,
        ),
    },
}


def harness_for(guard: str, unit: str) -> Harness:
    """The harness ``unit``'s ``guard`` record is taken in: its own, else the class's."""
    per_unit = UNIT_HARNESSES.get(guard, {})
    if unit in per_unit:
        return per_unit[unit]
    return HARNESSES[guard]


def record_key(unit: str, guard: str) -> str:
    """The key `read_guard_commissioning` reads, relative to the store root."""
    return f"faults/{unit}/{guard}/latest.json"


def record_document(
    unit: str,
    guard: str,
    *,
    fault: str,
    test: str,
    code_sha: str,
    mode_under_test: str = "enforce",
    as_of: str | None = None,
    observed: tuple[str, ...] | None = None,
) -> dict:
    """The document the phase-2 gate grades. ``outcome: induced`` is what reads MET."""
    return {
        "schema_version": "guard_commissioning.v1",
        "unit": unit,
        "guard": guard,
        "outcome": "induced",
        "fault": fault,
        "observed": list(observed if observed is not None else _observed_for(guard, unit)),
        "mode_under_test": mode_under_test,
        "test": test,
        "code_sha": code_sha,
        "as_of": as_of or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tracked_issue": "alpha-engine-config-I10786",
    }


def _observed_for(guard: str, unit: str | None = None) -> tuple[str, ...]:
    harness = HARNESSES.get(guard)
    if harness is None:
        raise ValueError(f"{guard!r} has no induced-fault harness; there is nothing it observed")
    per_unit = _OBSERVED_BY_UNIT.get(guard, {})
    if unit is not None and unit in per_unit:
        return per_unit[unit]
    if unit is not None:
        return harness_for(guard, unit).observed
    return harness.observed


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()


def _run_harness(nodeids: list[str]) -> dict[str, str]:
    """Run exactly these pytest nodes; ``{nodeid: passed|failed|skipped|error}``."""
    with tempfile.TemporaryDirectory() as tmp:
        report = pathlib.Path(tmp) / "junit.xml"
        subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-s",
             f"--junitxml={report}", *nodeids],
            cwd=REPO_ROOT,
            check=False,
        )
        if not report.is_file():
            return {n: "error" for n in nodeids}
        tree = ET.parse(report)
    outcomes: dict[str, str] = {}
    for case in tree.iter("testcase"):
        classname = case.get("classname", "")
        name = case.get("name", "")
        nodeid = classname.replace(".", "/") + ".py::" + name
        if case.find("failure") is not None:
            state = "failed"
        elif case.find("error") is not None:
            state = "error"
        elif case.find("skipped") is not None:
            state = "skipped"
        else:
            state = "passed"
        outcomes[nodeid] = state
    return {n: outcomes.get(n, "not_collected") for n in nodeids}


def _fault_for(unit: str, guard: str) -> str:
    """The induced-fault sentence the harness declares for this unit."""
    if guard in HARNESSES:
        import importlib

        sys.path.insert(0, str(REPO_ROOT))
        harness = importlib.import_module(f"tests.{harness_for(guard, unit).cases_module}")

        for case in harness.CASES:
            if case.unit == unit:
                return case.fault
    raise SystemExit(f"{unit}: no induced-fault case for guard {guard!r}; nothing to publish")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m data_gate.guard_commissioning",
        description="Run a guard's induced-fault harness and publish its commissioning records.",
    )
    parser.add_argument("--guard", required=True, choices=sorted(HARNESSES))
    parser.add_argument("--unit", action="append", required=True, help="unit id; repeatable")
    parser.add_argument("--store", default=DEFAULT_STORE)
    parser.add_argument(
        "--write", action="store_true",
        help="publish the records; without it, print them and write nothing",
    )
    args = parser.parse_args(argv)

    units = sorted(set(args.unit))
    faults = {u: _fault_for(u, args.guard) for u in units}

    code_sha = _git("rev-parse", "HEAD")
    if args.write and _git("status", "--porcelain", "--untracked-files=no"):
        print("refusing --write from a dirty tree: the record must name the code that ran",
              file=sys.stderr)
        return 2

    nodeids = [harness_for(args.guard, u).nodeid(u) for u in units]
    outcomes = _run_harness(nodeids)
    for nodeid, state in outcomes.items():
        print(f"{state:>13}  {nodeid}")
    if any(state != "passed" for state in outcomes.values()):
        print("not every induced-fault record passed; NOTHING was published", file=sys.stderr)
        return 1

    from data_gate.store import open_store

    store = open_store(args.store, dry_run=not args.write)
    as_of = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for unit in units:
        document = record_document(
            unit, args.guard, fault=faults[unit], test=harness_for(args.guard, unit).nodeid(unit),
            code_sha=code_sha, mode_under_test=harness_for(args.guard, unit).mode_under_test, as_of=as_of, observed=_observed_for(args.guard, unit),
        )
        key = record_key(unit, args.guard)
        if args.write:
            store.put_bytes(key, json.dumps(document, indent=2).encode("utf-8"))
            print(f"published {store.uri}/{key}")
        else:
            print(f"DRY RUN, would publish {store.uri}/{key}:\n{json.dumps(document, indent=2)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
