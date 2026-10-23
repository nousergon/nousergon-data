"""Guard commissioning, class ``cardinality``: one induced-fault record per unit.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "one induced fault per guard class
(withheld vendor, **truncated universe**, unit-scaled column, empty frame,
stale ``as_of``) -> ``failed``, one page, stand-down") and the phase-2 tracker
`alpha-engine-config-I10794`. A guard that has never fired is not in service
(`observability-policy` §9.1). This is the third guard class with a harness,
after ``empty_fresh`` and ``success_without_output``; it reuses the
``empty_fresh`` machinery unchanged (the in-memory S3, the harness clock and
the walker over the deployed ``data-collection.asl.json``).

**The fault is a truncated universe, and the guard under test is the unit's
own code.** Unlike the two earlier classes, ``cardinality`` has no shared
chokepoint that ENFORCES: the shared ``data_cardinality`` reading
(`validators/expectations.py::CARDINALITY_GUARD`) is staged OBSERVE and has no
ENFORCE raise site at all (`data_gate/guard_promotion.py`). What halts a
truncated universe today is each unit's own coverage check, the one its
descriptor's ``guards.cardinality.code`` / ``evaluates`` names. So each case
runs that collector function FOR REAL, and stubs only the vendor beneath it:

* **D22** (reference data, phase ``metron_reference_data``): the real
  ``collect_reference`` and its real ``_yfinance_classification`` pass, with
  the vendor's ``.info`` call (``yahoo_info``) answering for 10 of the 40 held
  symbols. ``_log_yf_coverage`` raises ``YfCoverageCollapse`` below
  ``METRON_YF_COVERAGE_FLOOR`` (0.70), so neither ``sectors/latest.json`` nor
  ``earnings/latest.json`` is overwritten.
* **D29** (analyst data, phase ``metron_analyst_data``): the real
  ``collect_analyst`` and ``_yfinance_analyst`` over the real
  ``YfinanceAnalystAdapter``, with the same 10-of-40 vendor; the same floor
  raises and ``analyst/latest.json`` is not overwritten.
* **D03** (prices, phase ``prices``): the real ``prices.collect``, with the
  per-ticker refresh (``_refresh_stale``: the vendor fetch plus its upload)
  answering ``vendor_no_data`` for 2 of the requested tickers and uploading the
  rest. ``collect`` turns any unwritten ticker into ``status: partial`` (the
  descriptor's ``evaluates`` line), ``vendor_no_data`` is not a quarantinable
  refusal, and ``_record_phase_lineage`` raises it as ``_DegradedRun``. The
  staleness probe (``_find_stale_fast``, an S3 listing) is stubbed to call
  every ticker stale; it decides what is asked for, not what is checked.
* **D06** (short interest, phase ``short_interest``) and **D07** (universe
  classification, phase ``universe_classification``): the Saturday
  ``weekly-phase-one`` workload (``_run_phase1``, ``reg.data_mode``
  ``phase1``), walked on the deployed ``weekly`` schedule's own input with only
  that workload faulted, as the ``empty_fresh`` weekly cases are. The real
  ``short_interest.collect`` / ``universe_classification.collect`` run over 40
  requested tickers with the vendor's ``.info`` (``yahoo_info``) answering for
  10 (0.25). Each refuses below its ``_MIN_OK_RATIO`` (0.50) with
  ``status: error`` and writes nothing. Both descriptors declare this guard
  ``partial``: the record commissions the collapse half that exists (below
  0.50), not the 0.50-0.90 / 0.50-1.0 band their notes name as remaining work.

**What each record observes**, which is what its ``observed`` lines say:

1. a ``failed`` manifest naming the truncation, on the attempt and on the
   on-demand retry (the fault is in the vendor's answer, so the retry meets it
   again);
2. exactly one page, ``NotifyFailure``, on the deployed definition: the
   process exits 1 (a raise is ``status: error``; D03's ``partial`` aggregates
   to ``partial``, which ``main()`` exits 1 on);
3. stand-down: terminal ``CollectionFailed``, the retry recomputed (never
   auto-skipped), and the previously published artifact untouched — the
   truncated answer never overwrote it (D22, D29) or never reached the keys it
   lacked (D03);
4. recovery: once the vendor answers in full, the next run reaches
   ``CollectionSucceeded`` with 0 pages.

Not here, and why:

* **D20**: its cardinality guard IS the shared ``data_cardinality`` reading,
  which is OBSERVE and has no raise site; there is no consequence to induce
  until a deliberate promotion PR writes one (`guard_promotion.data_cardinality`).
* **D01, D02, D19**: the trader's feed and the executor's, held for every
  guard class.
* **D17, D48, D49, D50, D51**: each runs on another workload or schedule
  (the morning enrich, the standalone index-contributions, ndx-constituents,
  settled-regrade and daily-panel schedules), so each needs its own walk.
* **D16**: its source-yield verdict is observe-only (`rag_source_yield`).
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import re
import types
from collections.abc import Callable
from dataclasses import dataclass
from unittest import mock

import nousergon_lib.secrets as nousergon_secrets
import pytest

import run_units
import weekly_collector
from collectors import metron_market_data, prices, short_interest, universe_classification
from collectors import yahoo_session
from data_gate import descriptors, evidence
from data_gate import guard_commissioning as gc
from data_gate.store import LocalStore
from shadow.run_state import RunStatePhaseRegistry
from tests import test_guard_commissioning_empty_fresh as ef
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixtures, used by name
    _environment,
    clock,
    s3,
)
from validators import expectations

GUARD_CLASS = "cardinality"
TRADING_DAY = ef.TRADING_DAY
BUCKET = ef.BUCKET
_M = metron_market_data

#: The held universe the metron cases read: 40 symbols, comfortably over
#: ``METRON_YF_COVERAGE_MIN_REQUESTED`` (20), so the ratio is evidence.
HELD = tuple(f"S{i:02d}" for i in range(40))
#: How many of them the faulted vendor answers for: 0.25, the 2026-09-22
#: sectors collapse (`METRON_YF_COVERAGE_FLOOR`'s own measured history).
TRUNCATED_TO = 10

#: D03's requested universe (``_ALWAYS_DOWNLOAD`` is appended by ``collect``)
#: and the two tickers the faulted vendor has no data for.
_D03_TICKERS = ("AAA", "BBB", "CCC", "DDD")
_D03_WITHHELD = ("CCC", "DDD")
_D03_ROWS = 2520

#: What the previous (healthy) cycle published, seeded before every run so
#: the stand-down can assert the truncated answer did not overwrite it.
_PRIOR = b'{"previous": "cycle"}'


@dataclass(frozen=True)
class Case:
    """One unit's induced truncation: the phase it runs as, and the call site it runs through.

    ``artifact_key``, ``extra_outputs`` and ``auto_skip`` mirror the unit's
    daily-mode ``_phase_collect`` call site in ``weekly_collector.py``
    (:func:`test_every_call_site_mirror_matches_weekly_collector` pins them).
    ``collect(s3, healthy)`` runs the unit's real collector over a vendor that
    answers in full (``healthy``) or truncated.
    """

    unit: str
    phase: str
    fault: str
    collect: Callable[[ef.InMemoryS3, bool], dict]
    #: The text the failed manifest's reason carries: the guard's own refusal.
    refusal: str
    #: Every key the unit publishes, with the count a HEALTHY run records.
    healthy_writes: Callable[[], dict[str, int]]
    artifact_key: str | None = None
    extra_outputs: Callable[[], tuple] = tuple
    auto_skip: bool = True
    #: The call-site source text that must appear in this phase's block.
    call_site: tuple[str, ...] = ()
    #: The run mode the call site runs under (``reg.data_mode``, the
    #: ``run_units.PHASE_UNITS`` key), the deployed workload that runs it, and the
    #: schedule whose execution input names that workload. EOD defaults, as in
    #: ``empty_fresh``'s ``Case``.
    mode: str = "daily"
    workload: str = ef.WORKLOAD
    collection: str = "eod"
    #: Whether the guard itself logs its refusal at ERROR. D06/D07 return it in
    #: ``status: error`` without logging; the manifest reason still carries it.
    logs_refusal: bool = True


# ---------------------------------------------------------------------------
# The vendor, stubbed beneath the real collector code
# ---------------------------------------------------------------------------


def _vendor_info(covered: frozenset[str]):
    """``yahoo_info``: a full ``.info`` for a covered symbol, ``{}`` otherwise.

    ``{}`` is what yfinance hands back for a symbol it has nothing for; both
    ``_yfinance_classification`` and ``YfinanceAnalystAdapter.fetch`` treat it
    as a per-symbol miss, never as an error.
    """

    def info(symbol: str, *, yf_module=None) -> dict:
        if symbol not in covered:
            return {}
        return {
            "sector": "Technology",
            "country": "United States",
            "recommendationKey": "buy",
            "targetMeanPrice": 120.0,
            "targetMedianPrice": 118.0,
            "numberOfAnalystOpinions": 12,
        }

    return info


def _covered(healthy: bool) -> frozenset[str]:
    return frozenset(HELD if healthy else HELD[:TRUNCATED_TO])


def _seed_universe(fake: ef.InMemoryS3) -> None:
    fake.objects.setdefault(
        _M.HOLDINGS_UNIVERSE_KEY,
        json.dumps({"holdings": [{"yf_symbol": s, "currency": "USD"} for s in HELD]}).encode(),
    )


@contextlib.contextmanager
def _vendor(healthy: bool):
    """Point every vendor seam the metron cases touch at the stub; nothing reaches the network."""
    info = _vendor_info(_covered(healthy))
    with (
        # D22 binds `yahoo_info` at import; the analyst adapter imports it per call.
        mock.patch.object(_M, "yahoo_info", info),
        mock.patch.object(yahoo_session, "yahoo_info", info),
        # `_yfinance_analyst` asks for an optional Finnhub key; no SSM read here.
        mock.patch.object(
            nousergon_secrets, "get_secret",
            lambda name, *, required=True, default=None: default,
        ),
    ):
        yield


def _d22_collect(fake: ef.InMemoryS3, healthy: bool) -> dict:
    """The call site's ``collect_reference(bucket, run_date, dry_run)``, on the harness's S3.

    The sector/country pass is the REAL ``_yfinance_classification``; only the
    SPY-weights and earnings fetches are injected through the function's own
    source parameters, so the one pass under test is the one that can refuse.
    """
    _seed_universe(fake)
    with _vendor(healthy):
        return _M.collect_reference(
            bucket=BUCKET, run_date=TRADING_DAY, dry_run=False, s3_client=fake,
            benchmark_source=lambda: {"Technology": 0.31},
            earnings_source=lambda symbols, _as_of: {s: "2026-10-28" for s in symbols},
        )


def _d29_collect(fake: ef.InMemoryS3, healthy: bool) -> dict:
    """The call site's ``collect_analyst(bucket, run_date, dry_run)``; ``_yfinance_analyst`` is real."""
    _seed_universe(fake)
    with _vendor(healthy):
        return _M.collect_analyst(bucket=BUCKET, run_date=TRADING_DAY, dry_run=False, s3_client=fake)


_D03_PREFIX = ef._PRICE_CACHE_PREFIX


def _d03_requested() -> list[str]:
    return list(dict.fromkeys(list(_D03_TICKERS) + prices._ALWAYS_DOWNLOAD))


def _d03_collect(fake: ef.InMemoryS3, healthy: bool) -> dict:
    """``prices.collect`` for real, over the harness's S3 and a stubbed per-ticker refresh."""
    withheld = () if healthy else _D03_WITHHELD

    def refresh(s3c, bucket, s3_prefix, stale, *_a, failure_reasons=None, **_k):
        """``_refresh_stale``'s contract: upload what the vendor answered, name every miss."""
        written = []
        for ticker in stale:
            if ticker in withheld:
                failure_reasons[ticker] = prices.FAIL_NO_DATA
                continue
            for key in prices.written_keys({"written": {ticker: _D03_ROWS}}, s3_prefix):
                s3c.put_object(Bucket=bucket, Key=key, Body=ef._parquet(3))
            written.append((ticker, _D03_ROWS))
        return len(written), [t for t in stale if t in withheld], written

    with (
        mock.patch.object(prices, "boto3", types.SimpleNamespace(client=lambda *_a, **_k: fake)),
        mock.patch.object(
            prices, "_find_stale_fast", lambda _s3, _b, _p, tickers, *_a, **_k: list(tickers)
        ),
        mock.patch.object(prices, "_refresh_stale", refresh),
    ):
        return prices.collect(
            bucket=BUCKET, tickers=list(_D03_TICKERS), s3_prefix=_D03_PREFIX,
            fetch_period="10y", staleness_threshold_days=1, batch_size=50,
            dry_run=False, reference_date=TRADING_DAY,
        )


def _d03_healthy_writes() -> dict[str, int]:
    return prices.written_keys({"written": {t: _D03_ROWS for t in _d03_requested()}}, _D03_PREFIX)


# ---------------------------------------------------------------------------
# The weekly phase-one cases (D06, D07): the same vendor seam, on Saturday's run
# ---------------------------------------------------------------------------

#: ``_run_phase1``'s workload on the ``weekly`` schedule (``empty_fresh._PHASE_ONE``).
_PHASE_ONE = {"mode": "phase1", "workload": "weekly-phase-one", "collection": "weekly"}
_MARKET_PREFIX = "market_data/"


def _weekly_vendor_info(covered: frozenset[str]):
    """``yahoo_info`` for the weekly ``.info`` collectors: populated for a covered symbol, ``{}`` otherwise."""

    def info(symbol: str, *, yf_module=None) -> dict:
        if symbol not in covered:
            return {}
        return {
            "shortPercentOfFloat": 0.031,
            "shortRatio": 2.4,
            "sharesShort": 1_200_000,
            "sector": "Technology",
            "country": "United States",
            "industry": "Software",
        }

    return info


def _weekly_collect(module):
    """The call site's ``<module>.collect(...)`` for real, over the harness's S3 and a stubbed vendor."""

    def collect(fake: ef.InMemoryS3, healthy: bool) -> dict:
        with (
            mock.patch.object(module, "boto3", types.SimpleNamespace(client=lambda *_a, **_k: fake)),
            mock.patch.object(module, "yahoo_info", _weekly_vendor_info(_covered(healthy))),
        ):
            return module.collect(
                bucket=BUCKET, tickers=list(HELD), s3_prefix=_MARKET_PREFIX,
                run_date=TRADING_DAY, inter_request_delay=0, dry_run=False,
            )

    return collect


_D06_KEY = f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/short_interest.json"
_D07_KEY = f"{_MARKET_PREFIX}universe_classification/{TRADING_DAY}.json"
_D07_LATEST = f"{_MARKET_PREFIX}universe_classification/latest.json"


CASES: tuple[Case, ...] = (
    Case(
        "D22", "metron_reference_data",
        "truncated universe: the vendor's .info pass answers for 10 of the 40 held symbols "
        "(0.25), below the 0.70 collapse floor of the sectors pass",
        _d22_collect,
        refusal="sectors: yfinance covered 10/40 symbols (0.25), below the 0.70 collapse floor",
        healthy_writes=lambda: {
            f"{_M.SECTORS_PREFIX}latest.json": len(HELD),
            f"{_M.EARNINGS_PREFIX}latest.json": len(HELD),
        },
        artifact_key=f"{_M.SECTORS_PREFIX}latest.json",
        extra_outputs=lambda: (
            (
                f"{_M.EARNINGS_PREFIX}latest.json",
                lambda r: r.get("status") == "ok",
                lambda r: r.get("earnings") or 0,
            ),
        ),
        call_site=(
            'artifact_key=f"{metron_market_data.SECTORS_PREFIX}latest.json"',
            'f"{metron_market_data.EARNINGS_PREFIX}latest.json"',
            'lambda r: r.get("earnings") or 0',
        ),
    ),
    Case(
        "D29", "metron_analyst_data",
        "truncated universe: the vendor's analyst snapshot answers for 10 of the 40 held "
        "symbols (0.25), below the 0.70 collapse floor of the analyst pass",
        _d29_collect,
        refusal="analyst: yfinance covered 10/40 symbols (0.25), below the 0.70 collapse floor",
        healthy_writes=lambda: {f"{_M.ANALYST_PREFIX}latest.json": len(HELD)},
        artifact_key=f"{_M.ANALYST_PREFIX}latest.json",
        call_site=('artifact_key=f"{metron_market_data.ANALYST_PREFIX}latest.json"',),
    ),
    Case(
        "D03", "prices",
        "truncated universe: the per-ticker refresh returns vendor_no_data for 2 requested "
        "tickers and uploads the rest, under a run that would otherwise report ok",
        _d03_collect,
        refusal=(
            f"{len(_D03_WITHHELD)} of {len(_d03_requested())} tickers failed to refresh "
            f"(vendor_no_data={len(_D03_WITHHELD)})"
        ),
        healthy_writes=_d03_healthy_writes,
        extra_outputs=lambda: weekly_collector._prices_extra_outputs(_D03_PREFIX),
        auto_skip=False,
    call_site=(
        "supports_auto_skip=False",
        "extra_outputs=_prices_extra_outputs(",
    ),
    ),
    Case(
        "D06", "short_interest",
        "truncated universe: the vendor's .info pass returns populated short-interest fields "
        "for 10 of the 40 requested tickers (0.25), below the 0.50 _MIN_OK_RATIO",
        _weekly_collect(short_interest),
        refusal=(
            f"only {TRUNCATED_TO}/{len(HELD)} tickers ({TRUNCATED_TO / len(HELD):.1%}) "
            "had populated short-interest data"
        ),
        healthy_writes=lambda: {_D06_KEY: len(HELD)},
        artifact_key=_D06_KEY,
        call_site=('artifact_key=f"{market_prefix}weekly/{run_date}/short_interest.json"',),
        logs_refusal=False,
        **_PHASE_ONE,
    ),
    Case(
        "D07", "universe_classification",
        "truncated universe: the vendor's .info pass classifies 10 of the 40 requested "
        "tickers (0.25), below the 0.50 _MIN_OK_RATIO",
        _weekly_collect(universe_classification),
        refusal=(
            f"only {TRUNCATED_TO}/{len(HELD)} tickers ({TRUNCATED_TO / len(HELD):.1%}) "
            "had populated classification data"
        ),
        healthy_writes=lambda: {_D07_KEY: len(HELD), _D07_LATEST: len(HELD)},
        artifact_key=_D07_KEY,
        extra_outputs=lambda: (
            (_D07_LATEST, lambda r: r.get("status") == "ok", lambda r: r.get("ok_count") or 0),
        ),
        call_site=(
            'artifact_key=f"{market_prefix}universe_classification/{run_date}.json"',
            'f"{market_prefix}universe_classification/latest.json"',
            'lambda r: r.get("ok_count") or 0',
        ),
        logs_refusal=False,
        **_PHASE_ONE,
    ),
)
CASE_IDS = [c.unit for c in CASES]


# ---------------------------------------------------------------------------
# One process attempt, through the real `_phase_collect`, on the deployed ASL
# ---------------------------------------------------------------------------


def _run_phase(fake: ef.InMemoryS3, case: Case, healthy: bool) -> dict:
    reg = RunStatePhaseRegistry(date=TRADING_DAY, bucket=BUCKET, marker_prefix="data", s3_client=fake)
    reg.data_mode = case.mode
    return weekly_collector._phase_collect(
        reg, case.phase, lambda: case.collect(fake, healthy),
        artifact_key=case.artifact_key,
        extra_outputs=case.extra_outputs(),
        supports_auto_skip=case.auto_skip,
    )


def _process_status(result: dict) -> str:
    """The daily aggregator's rule for ONE collector (``_finalize`` + ``main()``).

    ``ok`` alone aggregates to ``ok``; ``degraded`` exits 0 by declaration;
    ``error`` and ``partial`` aggregate to ``failed``/``partial`` and exit 1.
    """
    return "Success" if result.get("status") in ("ok", "ok_dry_run", "degraded") else "Failed"


class Execution(ef.Execution):
    """The ``empty_fresh`` walker, with the case's REAL collector behind ``PollWorkload``."""

    def tasks(self, name: str, state: dict, data: dict):
        if name == "PollWorkload" and data["workload"] == self.case.workload:
            healthy = self.rows_per_attempt[len(self.attempts)]
            result = _run_phase(self.s3, self.case, healthy)
            self.attempts.append(result)
            status = _process_status(result)
            return {"Status": status, "StatusDetails": status}
        return super().tasks(name, state, data)


def _seed_prior_cycle(fake: ef.InMemoryS3, case: Case) -> dict[str, bytes]:
    prior = {key: _PRIOR for key in case.healthy_writes()}
    fake.objects.update(prior)
    return prior


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_truncated_universe_fails_pages_once_and_stands_down(case, s3, caplog):  # noqa: F811
    """The commissioning record for ``cardinality``, on the unit's own enforcing check."""
    assert not expectations.EMPTY_FRESH_GUARD.enforcing, (
        "this record is taken with empty_fresh in its shipped OBSERVE mode; if it has been "
        "promoted, re-read which guard fails the run before re-taking this record"
    )
    caplog.set_level(logging.ERROR)
    prior = _seed_prior_cycle(s3, case)

    # Induce: the vendor answers for a truncated universe, on the attempt AND on
    # the on-demand retry (the fault is in the vendor's answer, not the box).
    faulted = Execution(s3, case, rows_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A `failed` manifest naming the truncation, on both attempts.
    manifests = s3.manifests(case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert case.refusal in m["reason"], m["reason"]
    assert all(a["status"] in ("error", "partial") for a in faulted.attempts), faulted.attempts

    # 2. Exactly one page, after the retry: the process exited 1 both times.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal Fail, the retry recomputed, and the truncated
    # answer did not overwrite what the previous cycle published.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert len(faulted.attempts) == 2
    assert not any(a.get("auto_skipped") for a in faulted.attempts)
    if case.unit == "D03":
        withheld = prices.written_keys({"written": dict.fromkeys(_D03_WITHHELD, 1)}, _D03_PREFIX)
        for key in withheld:
            assert s3.objects[key] == _PRIOR, f"{key} was overwritten by a refused ticker"
        refused = {g["key"] for g in manifests[0]["guards"] if g["guard"] == "write_refused"}
        assert set(withheld) <= refused, manifests[0]["guards"]
        assert {o["key"] for o in manifests[0]["outputs"]}.isdisjoint(withheld)
    else:
        for key, body in prior.items():
            assert s3.objects[key] == body, f"{key} was overwritten by the truncated answer"
        assert manifests[0]["outputs"] == []
        assert s3.marker(case)["status"] == "error"
        # The guard's own ERROR line, at least once per attempt.
        if case.logs_refusal:
            refusals = [r for r in caplog.records if case.refusal in r.getMessage()]
            assert len(refusals) >= len(faulted.attempts), [r.getMessage() for r in caplog.records]

    # ...and once the vendor answers in full, the next run is clean end to end.
    recovered = Execution(s3, case, rows_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed", "ok"]
    ok = s3.manifests(case)[-1]
    assert sorted((o["key"], o["rows_out"]) for o in ok["outputs"]) == sorted(case.healthy_writes().items())
    for key in case.healthy_writes():
        assert s3.objects[key] != _PRIOR, f"{key} was not republished by the recovery run"

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


def test_every_case_declares_the_guard_and_runs_on_the_machine_it_names():
    """A case is commissioned only where the descriptor names a halting check, on the
    schedule and workload it actually runs on (EOD ``post-market-data``, or the
    ``weekly`` schedule's ``weekly-phase-one``)."""
    mode_workload = {"daily": ef.WORKLOAD, "phase1": "weekly-phase-one"}
    for case in CASES:
        schedule = ef._deployed_input(case.collection)
        assert case.workload == mode_workload[case.mode], case.unit
        assert case.workload in schedule["workloads"], case.unit
        assert run_units.PHASE_UNITS[(case.mode, case.phase)].unit_id == case.unit
        assert case.unit in schedule["verify_units"], case.unit
        unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
        block = unit.guards[GUARD_CLASS]
        assert block["state"] in ("present", "partial"), case.unit
        # The unit-local check, not the shared observe-mode reading.
        assert "records_as" not in block, case.unit


@pytest.mark.parametrize(
    ("unit", "code", "evaluates"),
    [
        ("D22", "collectors/metron_market_data.py::_yfinance_classification", "floor=METRON_YF_COVERAGE_FLOOR"),
        ("D29", "collectors/metron_market_data.py::_yfinance_analyst", "floor=METRON_YF_COVERAGE_FLOOR"),
        ("D03", "collectors/prices.py::collect", '"status": "ok" if not failed_tickers else "partial"'),
        ("D06", "collectors/short_interest.py::collect", "ok_ratio < _MIN_OK_RATIO"),
        ("D07", "collectors/universe_classification.py::collect", "ok_ratio < _MIN_OK_RATIO"),
    ],
)
def test_the_case_runs_the_code_the_descriptor_names(unit, code, evaluates):
    """The harness induces against exactly the check the descriptor declares, not a neighbour of it."""
    block = next(u for u in descriptors.load_units() if u.unit_id == unit).guards[GUARD_CLASS]
    assert code in block["code"], block
    assert block["evaluates"] == evaluates, block


def test_the_metron_floor_is_the_one_the_fault_crosses():
    """10/40 is under the floor, 40/40 is over it, and 40 requested is enough to count."""
    assert TRUNCATED_TO / len(HELD) < _M.METRON_YF_COVERAGE_FLOOR <= 1.0
    assert len(HELD) >= _M.METRON_YF_COVERAGE_MIN_REQUESTED


def test_the_weekly_floor_is_the_one_the_fault_crosses():
    """10/40 is under each weekly collector's _MIN_OK_RATIO; 40/40 is over it."""
    for module in (short_interest, universe_classification):
        assert TRUNCATED_TO / len(HELD) < module._MIN_OK_RATIO <= 1.0, module.__name__


def test_the_d03_fault_is_not_a_quarantinable_refusal():
    """A quarantined ticker leaves the stage `ok`; the induced cause must be one that fails it."""
    assert prices.FAIL_NO_DATA not in prices._QUARANTINABLE_REASONS


def test_every_call_site_mirror_matches_weekly_collector():
    """The cases call `_phase_collect` with the call site's own arguments; a drift fails here."""
    source = (ef.REPO_ROOT / "weekly_collector.py").read_text()
    # Each mode's own body only: the Saturday phase-one run calls `prices` too.
    runner = {"daily": "_run_daily", "phase1": "_run_phase1"}
    for case in CASES:
        body = re.search(rf"\ndef {runner[case.mode]}\(.*?(?=\ndef )", source, flags=re.DOTALL)
        assert body, f"weekly_collector.{runner[case.mode]} not found"
        match = re.search(
            rf'\n(?P<indent> +)results\["collectors"\]\["{case.phase}"\] = _phase_collect\(\n'
            rf'\s*reg, "{case.phase}",(?P<block>.*?)\n(?P=indent)\)',
            body.group(0), flags=re.DOTALL,
        )
        assert match, f"no {case.mode}-mode _phase_collect call site for {case.phase}"
        block = match.group("block")
        for text in case.call_site:
            assert text in block, f"{case.unit}: {text!r} not at the {case.phase} call site"
        if case.artifact_key is None:
            assert "artifact_key=" not in block, case.unit


def test_no_case_is_on_the_trading_path():
    """D01, D02 and D19 are held for every guard class (the trader's feed and the executor's)."""
    assert not {"D01", "D02", "D19"} & set(CASE_IDS)


@pytest.mark.parametrize("unit", CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(unit, tmp_path):
    """``faults/<unit>/cardinality/latest.json`` with ``outcome: induced`` reads MET."""
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
    assert document["observed"] == list(gc.CARDINALITY_OBSERVED)
    store.put_bytes(gc.record_key(unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, descriptor, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def test_the_publisher_names_this_files_nodes():
    """The publisher runs these exact nodes; a rename here would publish nothing, loudly."""
    harness = gc.HARNESSES[GUARD_CLASS]
    assert harness.test_file == "tests/test_guard_commissioning_cardinality.py"
    assert harness.test_name == test_the_fault_record_truncated_universe_fails_pages_once_and_stands_down.__name__
    for case in CASES:
        assert gc._fault_for(case.unit, GUARD_CLASS) == case.fault
