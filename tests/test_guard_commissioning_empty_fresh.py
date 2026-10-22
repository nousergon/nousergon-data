"""Guard commissioning, class ``empty_fresh``: one induced-fault record.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "Guard commissioning: one induced fault per
guard class (withheld vendor, truncated universe, unit-scaled column, empty
frame, stale ``as_of``) -> ``failed``, one page, stand-down"). A guard that has
never fired is not in service (`observability-policy` §9.1); this file is the
first fault record, for the class the plan pairs with an EMPTY FRAME.

**The fault.** D19 (post-market data, ``weekly_collector.py`` daily mode,
phase ``daily_closes``) PUTs a real zero-row parquet frame to its contracted
key and reports ``status: ok`` with a row count of 0, which is the
empty-but-fresh write the guard exists to catch: fresh by timestamp, a
non-zero-byte object, and nothing in it.

**What runs for real:** ``weekly_collector._phase_collect`` (and through it
the `nousergon_lib` run-manifest wrapper and the guard in
``validators/expectations.py``), the data repo's own
``RunStatePhaseRegistry`` writing real phase markers, the completion check the
dispatcher Lambda runs (``data_gate/run_manifest_predicate.py``), and the
deployed ``infrastructure/step-functions/data-collection.asl.json`` walked
state by state. S3 is an in-memory fake; nothing here reaches AWS.

**What is modelled, and stated:** a workload's process exit code. It is the
daily aggregator's own rule (every collector ``ok``/``ok_dry_run``/``degraded``
exits 0, anything else exits 1 via ``main()``'s ``SystemExit(1)``), applied to
the one collector this record faults.

**The three properties, per the issue's closes-when:**

1. a ``failed`` manifest — under the ENFORCING guard. The guard ships in
   OBSERVE (`sf-pipeline-policy` §7a); the record is taken against the
   promotion itself (the one-line ``mode`` flip), because the phase-2 gate
   reads "every applicable guard clause **enforcing** and **commissioned**",
   and an observe-mode verdict has no consequence to commission. What the
   shipped OBSERVE mode does with the same fault is recorded alongside.
2. exactly one page — counted as ``sns:publish`` states the execution
   actually visits on the deployed definition, retry included.
3. a clean stand-down — the execution reaches a terminal ``Fail`` with no
   state left armed: the phase marker is NOT ``ok``, so the retry and every
   same-date rerun recompute rather than auto-skip the refused artifact; and
   once the fault is gone the next run files ``ok``, passes the completion
   check and pages nobody.

**The weekly cases** (`alpha-engine-config-I10794`): D04-D08, D10 and D11 on the
``weekly-phase-one`` workload (mode ``phase1``) and D15 on
``alternative-phase-two`` (mode ``phase2``) ride the same ``_phase_collect`` path,
so each is walked on the ``weekly`` schedule's own deployed input with only its
own workload faulted (:data:`WEEKLY_CASES`, which also says which weekly units
are not here and why).

**What the first run of this record found** (fixed in the same change): the
enforce branch raised AFTER the phase marker had been written ``ok``, so the
Step Function's on-demand retry auto-skipped the very artifact the guard had
just refused, exited 0, and the only thing still between that and a green run
was the completion check's same-date carry-forward. That is the
`alpha-engine-config-I11812` defect reached through the guard instead of
through ``degraded``; ``weekly_collector._GuardRejectedPublish`` closes it.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import io
import json
import logging
import pathlib
import re
import types
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from nousergon_lib import run_manifest
from nousergon_lib.guard_mode import GuardMode, GuardStaging

import run_units
import weekly_collector
from collectors import fred_history, metron_market_data, prices, technical_rating_ledger
from data_gate import descriptors, evidence
from data_gate import run_manifest_predicate as completion
from data_gate.guard_commissioning import record_document, record_key
from data_gate.store import LocalStore
from shadow.run_state import RunStatePhaseRegistry
from validators import expectations

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ASL_PATH = REPO_ROOT / "infrastructure" / "step-functions" / "data-collection.asl.json"
STACK_PATH = REPO_ROOT / "infrastructure" / "cloudformation" / "nousergon-data-collection.yaml"

GUARD_CLASS = "empty_fresh"
WORKLOAD = "post-market-data"
BUCKET = "alpha-engine-research"

#: When the harness's first execution starts: the EOD schedule's 18:15 ET
#: trigger on a trading day. Every stamp the record is graded on comes from
#: :class:`HarnessClock`, never the wall clock, so the record reads the same on
#: any day and at any hour it is taken.
EOD_START = dt.datetime(2026, 10, 15, 22, 15, tzinfo=dt.timezone.utc)
TRADING_DAY = EOD_START.date().isoformat()


class HarnessClock:
    """The one clock every run manifest and execution start in this file reads.

    The completion check takes the lexically greatest manifest key as a unit's
    newest run, and that key is a ULID: a millisecond timestamp, then 80 random
    bits. Read off the wall clock, the faulted retry and the recovery run started
    1-2 ms apart on a CI-class box, and on a faster one in the SAME millisecond,
    where the random bits decided which was "newest". When they picked the
    retry, the recovery graded the refused run (`run_not_ok`) and failed — a
    random subset of units on each run (alpha-engine-config-I10786, 2026-10-15).
    In production the two are separate box launches minutes apart.

    Each read advances one second, so every run starts strictly after the last
    one finished, and the manifest's whole-second ``finished`` stamp never
    reads earlier than the execution start it is graded against.
    """

    STEP = dt.timedelta(seconds=1)

    def __init__(self, start: dt.datetime) -> None:
        self._now = start

    def now(self) -> dt.datetime:
        self._now += self.STEP
        return self._now


@dataclass(frozen=True)
class Shape:
    """A unit whose published keys are not one primary key plus a literal list.

    D03 (one parquet per ticker), D21 (a consolidated artifact plus one FX file
    per currency), D26 (a ledger manifest written only when the run wrote) and
    D31 (feature groups present only when computed) declare their outputs
    through ``extra_outputs`` entries whose key and count come out of the
    collector's own result. The harness does not re-derive those: the
    ``extra_outputs`` here is the CALL SITE'S (``weekly_collector`` helpers
    where it has one, a mirror of the inline tuple where it does not), and
    ``result`` is the dict the collector would return, so the real
    ``_extra_writes`` reads it back exactly as production does.
    """

    #: ``rows -> the collector's result dict`` (``status``, the unit's ``rows_key``,
    #: and whatever the call site's ``extra_outputs`` read).
    result: Callable[[int], dict]
    #: ``rows -> {key: rows recorded for it}`` for every key that run PUTs. The
    #: assertion on the failed manifest's ``outputs`` is made against this.
    written: Callable[[int], dict[str, int]]
    #: The call site's ``extra_outputs`` tuple.
    extra_outputs: Callable[[], tuple]
    #: The call site's ``supports_auto_skip``.
    auto_skip: bool = True


@dataclass(frozen=True)
class Case:
    """One unit's induced fault: the phase it runs as and the key it publishes.

    ``key`` is the unit's PRIMARY ``artifact_key`` exactly as the daily mode's
    ``_phase_collect`` call site builds it in ``weekly_collector.py`` (from the
    same collector-module constants), so the guard grades the key production
    publishes; ``None`` for a unit whose call site passes none. ``fault`` is the
    induced-fault sentence the record carries.
    """

    unit: str
    phase: str
    key: str | None
    fault: str
    #: D19's call site alone passes ``verify_artifact_exists=True, bucket=...``
    #: (config-I2702); the metron call sites pass neither. Mirrored, not tidied.
    verify_artifact_exists: bool = False
    #: ``(key, result count key)`` for every ``extra_outputs`` entry the call
    #: site declares (alpha-engine-config-I10855) — D20 and D22 publish more
    #: than one key, and the completion check reads ``output_missing`` on a
    #: recovery run that published only the primary one.
    extra: tuple[tuple[str, str], ...] = ()
    #: Set for the units whose outputs are shaped by their own result (see
    #: :class:`Shape`); ``extra`` is then unused.
    shape: Shape | None = None
    #: The run mode the call site runs under (``reg.data_mode``, the
    #: ``run_units.PHASE_UNITS`` key), the deployed workload that runs it, and the
    #: schedule whose execution input names that workload. The EOD defaults are
    #: the original cases'; the weekly ones are :data:`_PHASE_ONE` / :data:`_PHASE_TWO`.
    mode: str = "daily"
    workload: str = WORKLOAD
    collection: str = "eod"

    def written(self, rows: int) -> dict[str, int]:
        """Every key a run at ``rows`` PUTs, with the count its manifest records."""
        if self.shape is not None:
            return self.shape.written(rows)
        return {k: rows for k in self.keys}

    @property
    def keys(self) -> tuple[str, ...]:
        if self.shape is not None:
            return tuple(self.shape.written(0))
        return (*([self.key] if self.key else []), *(k for k, _ in self.extra))

    @property
    def rows_key(self) -> str:
        return run_units.PHASE_UNITS[(self.mode, self.phase)].rows_key


def _json_case(unit: str, phase: str, key: str, extra: tuple[tuple[str, str], ...] = ()) -> Case:
    return Case(
        unit, phase, key,
        f"empty payload: a zero-record JSON document PUT to {key} under status ok, "
        "reporting a row count of 0" + (" (and every co-published key empty too)" if extra else ""),
        extra=extra,
    )


_M = metron_market_data

#: ``price_cfg.get("s3_prefix", ...)`` at the D03 daily call site.
_PRICE_CACHE_PREFIX = "predictor/price_cache/"
_PRICE_TICKERS = ("AAA", "BBB")


def _d03_shape() -> Shape:
    """D03 (prices): no primary key; one parquet per ticker the run actually wrote.

    The call site's ``extra_outputs`` is ``weekly_collector._prices_extra_outputs``
    itself, reading ``result["written"]`` through ``prices.written_keys``.
    """
    return Shape(
        result=lambda rows: {
            "status": "ok", "refreshed": rows, "written": {t: rows for t in _PRICE_TICKERS},
        },
        written=lambda rows: prices.written_keys(
            {"written": {t: rows for t in _PRICE_TICKERS}}, _PRICE_CACHE_PREFIX
        ),
        extra_outputs=lambda: weekly_collector._prices_extra_outputs(_PRICE_CACHE_PREFIX),
        auto_skip=False,
    )


_D21_FX_KEY = f"{_M.FX_HISTORY_PREFIX}EUR.json"


def _d21_shape() -> Shape:
    """D21 (close/fx history): no primary key; the consolidated close history and
    one FX file per currency written. The fault empties the close history; the FX
    file is a real one-rate document, so the guard has to pick the empty key out.

    The call site's ``extra_outputs`` is inline in ``weekly_collector.py``
    (``metron_market_data_history``), so it is mirrored here, not tidied.
    """
    return Shape(
        result=lambda rows: {"status": "ok", "close_series": rows, "fx_currencies": ["EUR"]},
        written=lambda rows: {_M.CONSOLIDATED_CLOSE_HISTORY_KEY: rows, _D21_FX_KEY: 1},
        extra_outputs=lambda: (
            (
                _M.CONSOLIDATED_CLOSE_HISTORY_KEY,
                lambda r: r.get("status") == "ok",
                lambda r: r.get("close_series") or 0,
            ),
            (
                lambda r: [f"{_M.FX_HISTORY_PREFIX}{ccy}.json" for ccy in (r.get("fx_currencies") or [])],
                lambda r: r.get("status") == "ok" and bool(r.get("fx_currencies")),
                lambda r: 1,
            ),
        ),
        auto_skip=False,
    )


def _d26_shape() -> Shape:
    """D26 (rating ledger): no primary key; the ledger manifest, recorded only when
    the run wrote (``backfill_written or live_written``), counted as ``total_dates``.
    Mirrors the inline ``extra_outputs`` of ``metron_rating_ledger``'s call site.
    """
    manifest = technical_rating_ledger.RATING_LEDGER_MANIFEST_KEY
    return Shape(
        result=lambda rows: {"status": "ok", "total_dates": rows, "live_written": True},
        written=lambda rows: {manifest: rows},
        extra_outputs=lambda: (
            (
                manifest,
                lambda r: bool(r.get("backfill_written") or r.get("live_written")),
                lambda r: r.get("total_dates") or 0,
            ),
        ),
        auto_skip=False,
    )


#: What D31 reports for the parts of its write that are not the fault: the catalog
#: entry count ``upload_registry`` returns and the sectors the metron_supplemental
#: sidecar carries. Both are real, non-empty writes.
_D31_REGISTRY_ENTRIES = 120
_D31_SECTORS = 85


def _d31_shape() -> Shape:
    """D31 (daily features): the primary ``schema_version.json``, one parquet per
    feature group computed, the catalog registry and the metron_supplemental sidecar.

    The group and registry entries are ``weekly_collector``'s own helpers; the
    sidecar entry is inline at the call site and mirrored. The fault is a snapshot
    of zero rows in every group, with the registry and sidecar written as normal.
    """
    sidecar = f"features/metron_supplemental/{TRADING_DAY}/sectors.json"
    groups = weekly_collector._FEATURE_GROUPS
    return Shape(
        result=lambda rows: {
            "status": "ok",
            "tickers_computed": rows,
            "groups_written": {g: rows for g in groups},
            "registry_written": _D31_REGISTRY_ENTRIES,
            "metron_supplemental_written": {"sectors": _D31_SECTORS},
        },
        written=lambda rows: {
            f"features/{TRADING_DAY}/schema_version.json": rows,
            **{f"features/{TRADING_DAY}/{g}.parquet": rows for g in groups},
            "features/registry.json": _D31_REGISTRY_ENTRIES,
            sidecar: _D31_SECTORS,
        },
        extra_outputs=lambda: weekly_collector._feature_group_extra_outputs(TRADING_DAY) + (
            weekly_collector._feature_registry_extra_output(),
            (
                sidecar,
                lambda r: bool(
                    {k: v for k, v in (r.get("metron_supplemental_written") or {}).items() if k != "sectors"}
                ) or ((r.get("metron_supplemental_written") or {}).get("sectors") or 0) > 0,
                lambda r: (r.get("metron_supplemental_written") or {}).get("sectors") or 0,
            ),
        ),
    )


#: Every daily-mode ``_phase_collect`` unit on the EOD machine's
#: ``post-market-data`` workload that the trading path does not read. D19 is the
#: first record (nousergon-data#2069); the metron units ride the identical call
#: path (`_phase_collect` -> `_record_phase_lineage` -> `check_empty_fresh`) on the
#: same workload and the same deployed ASL, and each gets its own record because
#: the gate grades commissioning per unit (`faults/<unit>/<guard>/latest.json`).
#: A unit with a primary ``artifact_key`` takes the literal-list shape
#: (:func:`_json_case`); D03, D21, D26 and D31 publish through result-shaped
#: ``extra_outputs`` and carry a :class:`Shape`.
#:
#: Not here, and why: D32 runs on the OTHER workload (``post-market-arctic-append``)
#: and its call site declares no ``artifact_key`` and no ``extra_outputs``, so
#: the guard grades nothing there (`not_applicable`) and there is no fault to
#: induce through this path. D19's `staging/daily_closes` is read by
#: crucible-executor (trading path) and is held with D01/D02.
EOD_CASES: tuple[Case, ...] = (
    Case(
        "D19", "daily_closes", f"staging/daily_closes/{TRADING_DAY}.parquet",
        "empty frame: a zero-row parquet PUT to the contracted key under status ok",
        verify_artifact_exists=True,
    ),
    _json_case(
        "D20", "metron_market_data", f"{_M.CLOSES_PREFIX}{TRADING_DAY}.json",
        extra=(
            (f"{_M.CLOSES_PREFIX}latest.json", "closes"),
            (f"{_M.FX_PREFIX}{TRADING_DAY}.json", "fx"),
            (f"{_M.FX_PREFIX}latest.json", "fx"),
        ),
    ),
    _json_case(
        "D22", "metron_reference_data", f"{_M.SECTORS_PREFIX}latest.json",
        extra=((f"{_M.EARNINGS_PREFIX}latest.json", "earnings"),),
    ),
    _json_case("D23", "metron_macro_data", f"{_M.MACRO_PREFIX}latest.json"),
    _json_case("D24", "metron_fundamentals_data", f"{_M.FUNDAMENTALS_PREFIX}latest.json"),
    _json_case("D25", "metron_technicals_data", f"{_M.TECHNICALS_PREFIX}latest.json"),
    _json_case("D27", "metron_rating_performance", technical_rating_ledger.RATING_PERFORMANCE_KEY),
    _json_case("D28", "metron_security_performance_data", f"{_M.SECURITY_PERFORMANCE_PREFIX}latest.json"),
    _json_case("D29", "metron_analyst_data", f"{_M.ANALYST_PREFIX}latest.json"),
    _json_case("D30", "metron_sentiment_data", f"{_M.SENTIMENT_PREFIX}latest.json"),
    Case(
        "D03", "prices", None,
        "empty frame: every per-ticker price-cache parquet PUT with zero rows under status ok, "
        "reporting refreshed 0",
        shape=_d03_shape(),
    ),
    Case(
        "D21", "metron_market_data_history", None,
        f"empty payload: a zero-series JSON document PUT to {_M.CONSOLIDATED_CLOSE_HISTORY_KEY} "
        "under status ok, reporting a close_series count of 0",
        shape=_d21_shape(),
    ),
    Case(
        "D26", "metron_rating_ledger", None,
        "empty ledger: a zero-date rating-history manifest PUT under status ok, reporting "
        "total_dates 0",
        shape=_d26_shape(),
    ),
    Case(
        "D31", "features", f"features/{TRADING_DAY}/schema_version.json",
        "empty snapshot: the schema marker and every feature-group parquet PUT with zero rows "
        "under status ok, reporting tickers_computed 0",
        shape=_d31_shape(),
    ),
)

# ---------------------------------------------------------------------------
# The weekly cases: the same guard on the Saturday schedule's own workloads
# ---------------------------------------------------------------------------

#: The weekly schedule's ``weekly-phase-one`` workload (``_run_phase1``) and its
#: ``alternative-phase-two`` workload (``_run_phase2``). Both run their units
#: through the same ``_phase_collect`` -> ``_record_phase_lineage`` ->
#: ``check_empty_fresh`` path the EOD cases do, under ``reg.data_mode`` ``phase1``
#: / ``phase2``, on the same deployed ASL, started by the ``weekly`` schedule's
#: input (whose ``verify_units`` names each of them).
_PHASE_ONE = {"mode": "phase1", "workload": "weekly-phase-one", "collection": "weekly"}
_PHASE_TWO = {"mode": "phase2", "workload": "alternative-phase-two", "collection": "weekly"}

#: ``config.get("market_data", {}).get("s3_prefix", "market_data/")`` at the
#: weekly call sites (the deployed config sets no other prefix).
_MARKET_PREFIX = "market_data/"
#: The series the D04 call site passes ``fred_history.backfill_to_s3`` explicitly.
_FRED_SERIES = ("TWO", "HYOAS", "BAA10Y")


def _weekly_json_case(
    unit: str, phase: str, key: str, extra: tuple[tuple[str, str], ...] = (), *, schedule=None,
) -> Case:
    return dataclasses.replace(_json_case(unit, phase, key, extra), **(schedule or _PHASE_ONE))


def _d04_result(rows: int) -> dict:
    """``fred_history.backfill_to_s3``'s own result shape, every series uploaded."""
    return {
        "status": "ok", "refreshed": len(_FRED_SERIES), "total": len(_FRED_SERIES),
        "per_ticker": {t: {"status": "ok", "rows": rows} for t in _FRED_SERIES},
        "dry_run": False,
    }


def _d04_shape() -> Shape:
    """D04 (FRED macro history): no primary key; one price-cache parquet per series
    the run uploaded, each with its own row count. The call site's ``extra_outputs``
    is ``weekly_collector._fred_history_extra_outputs`` itself, reading
    ``per_ticker`` through ``fred_history.written_keys``.
    """
    return Shape(
        result=_d04_result,
        written=lambda rows: fred_history.written_keys(_d04_result(rows), _PRICE_CACHE_PREFIX),
        extra_outputs=lambda: weekly_collector._fred_history_extra_outputs(_PRICE_CACHE_PREFIX),
        auto_skip=False,
    )


#: What D05's two secondary writes report when they are not the fault.
_D05_HISTORY_ROWS = 52
_D05_CALENDAR_ROWS = 12


def _d05_shape() -> Shape:
    """D05 (macro): the primary ``weekly/{date}/macro.json`` plus the two secondary
    parquets ``macro.collect`` writes beside it, each recorded only when its own
    status is ``ok``. The fault empties macro.json; both secondaries are real
    writes, so the guard has to pick the empty key out. The ``extra_outputs`` is
    inline at the call site, so it is mirrored here, not tidied.
    """
    history = f"{_MARKET_PREFIX}macro_history.parquet"
    calendar = f"{_MARKET_PREFIX}macro_release_calendar.parquet"
    return Shape(
        result=lambda rows: {
            "status": "ok", "fields": rows, "rows": rows,
            "macro_history": {"status": "ok", "rows": _D05_HISTORY_ROWS},
            "release_calendar": {"status": "ok", "rows": _D05_CALENDAR_ROWS},
        },
        written=lambda rows: {
            f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/macro.json": rows,
            history: _D05_HISTORY_ROWS,
            calendar: _D05_CALENDAR_ROWS,
        },
        extra_outputs=lambda: (
            (
                history,
                lambda r: (r.get("macro_history") or {}).get("status") == "ok",
                lambda r: (r.get("macro_history") or {}).get("rows") or 0,
            ),
            (
                calendar,
                lambda r: (r.get("release_calendar") or {}).get("status") == "ok",
                lambda r: (r.get("release_calendar") or {}).get("rows") or 0,
            ),
        ),
    )


def _d08_shape() -> Shape:
    """D08 (universe returns): no primary key; the ``research.db`` pointer and its
    dated backup, each recorded when the writer's own ``db_upload`` names it and
    counted as ``rows_inserted``. Mirrors the call site's inline ``extra_outputs``.
    """
    backup = f"backups/research_{TRADING_DAY}.db"
    return Shape(
        result=lambda rows: {
            "status": "ok", "rows_inserted": rows,
            "db_upload": {"pointer_key": "research.db", "backup_key": backup},
        },
        written=lambda rows: {"research.db": rows, backup: rows},
        extra_outputs=lambda: (
            (
                "research.db",
                lambda r: bool((r.get("db_upload") or {}).get("pointer_key")),
                lambda r: r.get("rows_inserted") or 0,
            ),
            (
                backup,
                lambda r: bool((r.get("db_upload") or {}).get("backup_key")),
                lambda r: r.get("rows_inserted") or 0,
            ),
        ),
        auto_skip=False,
    )


#: The resolved universe size D15's scope baseline records when it is not the fault.
_D15_SCOPE_TICKERS = 900


def _d15_shape() -> Shape:
    """D15 (alternative, phase 2): the primary ``alternative/manifest.json`` plus the
    ``scope.json`` baseline the phase body writes before its first vendor call,
    always recorded with the resolved universe size. The fault is a manifest
    covering zero tickers over a real scope baseline.
    """
    base = f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/alternative/"
    return Shape(
        result=lambda rows: {"status": "ok", "tickers_processed": rows},
        written=lambda rows: {f"{base}manifest.json": rows, f"{base}scope.json": _D15_SCOPE_TICKERS},
        extra_outputs=lambda: ((f"{base}scope.json", lambda r: True, lambda r: _D15_SCOPE_TICKERS),),
    )


#: Every weekly ``_phase_collect`` unit whose guard can fire through this path and
#: that the trading path does not read. Each walks the deployed ASL on the
#: ``weekly`` schedule's input, with only its own workload faulted.
#:
#: Not here, and why: D01/D02 (the trader's feed, held by the observe-only ruling,
#: nousergon-data#2145). D13 (``arcticdb``) records each ArcticDB library only
#: when its measured count is above 0 (``_arcticdb_backfill_extra_outputs``), so
#: an empty rebuild records no output, the guard grades ``not_applicable`` and
#: there is no fault to induce through this path. D09 (``signal_returns``) and
#: D12 (weekly ``features``) declare no ``empty_fresh: present`` clause.
WEEKLY_CASES: tuple[Case, ...] = (
    Case(
        "D04", "fred_macro_history", None,
        "empty frame: every FRED-history price-cache parquet PUT with zero rows under status ok, "
        "each series reporting rows 0",
        shape=_d04_shape(), **_PHASE_ONE,
    ),
    Case(
        "D05", "macro", f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/macro.json",
        f"empty payload: a zero-field macro.json PUT to {_MARKET_PREFIX}weekly/{TRADING_DAY}/"
        "macro.json under status ok, reporting rows 0, beside two real secondary parquets",
        shape=_d05_shape(), **_PHASE_ONE,
    ),
    _weekly_json_case("D06", "short_interest", f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/short_interest.json"),
    _weekly_json_case(
        "D07", "universe_classification", f"{_MARKET_PREFIX}universe_classification/{TRADING_DAY}.json",
        extra=((f"{_MARKET_PREFIX}universe_classification/latest.json", "ok_count"),),
    ),
    Case(
        "D08", "universe_returns", None,
        "empty write: research.db and its dated backup uploaded under status ok, reporting "
        "rows_inserted 0",
        shape=_d08_shape(), **_PHASE_ONE,
    ),
    _weekly_json_case("D10", "fundamentals", f"archive/fundamentals/{TRADING_DAY}.json"),
    _weekly_json_case("D11", "metron_valuation_medians", f"{_M.VALUATION_MEDIANS_PREFIX}latest.json"),
    Case(
        "D15", "alternative", f"{_MARKET_PREFIX}weekly/{TRADING_DAY}/alternative/manifest.json",
        "empty payload: an alternative-data manifest covering zero tickers PUT under status ok, "
        "reporting tickers_processed 0, beside a real scope baseline",
        shape=_d15_shape(), **_PHASE_TWO,
    ),
)

CASES: tuple[Case, ...] = EOD_CASES + WEEKLY_CASES
CASE_IDS = [c.unit for c in CASES]
D19 = CASES[0]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class InMemoryS3:
    """The four S3 calls the run path makes, over a dict of bytes."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_):  # noqa: N803 -- boto3 kwarg names
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode("utf-8")
        return {"ETag": '"fake"'}

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix="", StartAfter="", **_):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def manifests(self, case: Case) -> list[dict]:
        prefix = f"data_collection/runs/{case.unit}/{TRADING_DAY}/"
        return [json.loads(self.objects[k]) for k in sorted(self.objects) if k.startswith(prefix)]

    def marker(self, case: Case) -> dict:
        return json.loads(self.objects[f"data/{TRADING_DAY}/.phases/{case.phase}.json"])


def _parquet(rows: int) -> bytes:
    frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(rows)], "close": [1.0] * rows})
    buf = io.BytesIO()
    frame.to_parquet(buf)
    return buf.getvalue()


def _body(key: str, rows: int) -> bytes:
    if key.endswith(".parquet"):
        return _parquet(rows)
    return json.dumps({"records": [{"symbol": f"T{i}"} for i in range(rows)]}).encode("utf-8")


def _collector(s3: InMemoryS3, case: Case, rows: int):
    """The unit's collector body, reduced to its publish: PUT the keys, report its counts."""

    def run() -> dict:
        if case.shape is not None:
            result = case.shape.result(rows)
        else:
            result = {"status": "ok", case.rows_key: rows}
            for _, count_key in case.extra:
                result[count_key] = rows
        for key, recorded in case.written(rows).items():
            s3.put_object(Bucket=BUCKET, Key=key, Body=_body(key, recorded))
        return result

    return run


def _extra_outputs(case: Case) -> tuple:
    """The call site's ``extra_outputs``, in its own shape: written on ``ok``, counted by key."""
    if case.shape is not None:
        return case.shape.extra_outputs()
    return tuple(
        (key, lambda r: r.get("status") == "ok", lambda r, c=count_key: r.get(c) or 0)
        for key, count_key in case.extra
    )


def _run_phase(s3: InMemoryS3, case: Case, rows: int) -> dict:
    """One process attempt of the unit's phase, exactly as the daily mode calls it."""
    reg = RunStatePhaseRegistry(date=TRADING_DAY, bucket=BUCKET, marker_prefix="data", s3_client=s3)
    reg.data_mode = case.mode
    if case.verify_artifact_exists:
        return weekly_collector._phase_collect(
            reg, case.phase, _collector(s3, case, rows), artifact_key=case.key,
            verify_artifact_exists=True, bucket=BUCKET,
        )
    return weekly_collector._phase_collect(
        reg, case.phase, _collector(s3, case, rows), artifact_key=case.key,
        extra_outputs=_extra_outputs(case),
        supports_auto_skip=case.shape.auto_skip if case.shape is not None else True,
    )


def _process_status(result: dict) -> str:
    """The daily aggregator's rule for ONE collector -> the SSM command status."""
    return "Success" if result.get("status") in ("ok", "ok_dry_run", "degraded") else "Failed"


@pytest.fixture(autouse=True)
def _environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:commissioning")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    monkeypatch.delenv("RUN_TOKEN", raising=False)
    completion.reset_unit_cache()


@pytest.fixture(autouse=True)
def clock(monkeypatch) -> HarnessClock:
    """Stamp every run manifest from :class:`HarnessClock` (the wrapper's only clock reads)."""
    harness_clock = HarnessClock(EOD_START)

    class _HarnessDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return harness_clock.now().astimezone(tz) if tz else harness_clock.now()

    monkeypatch.setattr(
        run_manifest, "dt", types.SimpleNamespace(datetime=_HarnessDatetime, timezone=dt.timezone)
    )
    return harness_clock


@pytest.fixture
def s3(monkeypatch) -> InMemoryS3:
    store = InMemoryS3()
    # verify-by-artifact (config-I2702) HEADs through its own boto3 client.
    monkeypatch.setattr(weekly_collector, "_s3_object_exists", lambda _b, key: key in store.objects)
    return store


@pytest.fixture
def enforcing(monkeypatch) -> GuardStaging:
    """The promotion: the one-line ``mode`` flip `EMPTY_FRESH_GUARD` declares."""
    promoted = GuardStaging(
        name=expectations.EMPTY_FRESH_GUARD.name,
        mode=GuardMode.ENFORCE,
        promotion_criterion=expectations.EMPTY_FRESH_GUARD.promotion_criterion,
        tracked_issue=expectations.EMPTY_FRESH_GUARD.tracked_issue,
    )
    monkeypatch.setattr(expectations, "EMPTY_FRESH_GUARD", promoted)
    # `report()` binds its default staging at import, so after an in-test flip
    # its log line would still say `mode=observe`; a source-level promotion
    # rebinds it. Pin the default so the record's log line says what ran.
    report = expectations.report
    monkeypatch.setattr(
        expectations, "report", lambda reading, *, unit_id, staging=promoted, log=None: report(
            reading, unit_id=unit_id, staging=staging, log=log
        )
    )
    return promoted


# ---------------------------------------------------------------------------
# The deployed state machine, walked
# ---------------------------------------------------------------------------


def _deployed_input(collection: str) -> dict:
    """A schedule's execution input, read from the deployed template."""
    match = re.search(
        rf"Input: '(\{{\"collection\": \"{collection}\".*?\}})'", STACK_PATH.read_text()
    )
    assert match, f"the {collection} schedule's Input could not be found in the collection stack template"
    return json.loads(match.group(1))


def _deployed_eod_input() -> dict:
    """The EOD schedule's execution input, read from the deployed template."""
    return _deployed_input("eod")


def _get(data: dict, path: str):
    node = data
    for part in path.removeprefix("$.").split("."):
        m = re.fullmatch(r"(\w+)\[(\d+)\]", part)
        name, index = (m.group(1), int(m.group(2))) if m else (part, None)
        if not isinstance(node, dict) or name not in node:
            raise KeyError(path)
        node = node[name]
        if index is not None:
            if not isinstance(node, list) or index >= len(node):
                raise KeyError(path)
            node = node[index]
    return node


def _rule(rule: dict, data: dict) -> bool:
    if "Or" in rule:
        return any(_rule(r, data) for r in rule["Or"])
    if "And" in rule:
        return all(_rule(r, data) for r in rule["And"])
    if "Not" in rule:
        return not _rule(rule["Not"], data)
    if "IsPresent" in rule:
        try:
            _get(data, rule["Variable"])
            return rule["IsPresent"]
        except KeyError:
            return not rule["IsPresent"]
    value = _get(data, rule["Variable"])
    for op, test in (
        ("BooleanEquals", lambda v, x: v is x),
        ("StringEquals", lambda v, x: v == x),
        ("NumericLessThan", lambda v, x: v < x),
    ):
        if op in rule:
            return test(value, rule[op])
    raise AssertionError(f"Choice operator not modelled by this walker: {rule}")  # fail loud


def _resolve(params: dict, data: dict, context: dict) -> dict:
    out = {}
    for k, v in params.items():
        if not k.endswith(".$"):
            out[k] = v
            continue
        if v.startswith("$$."):
            out[k[:-2]] = context[v]
        elif v.startswith("States.MathAdd("):
            path, inc = re.fullmatch(r"States\.MathAdd\((\$\.\w+), (\d+)\)", v).groups()
            out[k[:-2]] = _get(data, path) + int(inc)
        else:
            out[k[:-2]] = _get(data, v)
    return out


def _walk(states: dict, start: str, data: dict, tasks, path: list[str], context: dict) -> str:
    """Run one (sub-)machine to a terminal state. Returns the terminal's type."""
    name = start
    for _ in range(500):
        state = states[name]
        path.append(name)
        kind = state["Type"]
        if kind in ("Fail", "Succeed"):
            return kind
        if kind == "Choice":
            name = next((c["Next"] for c in state["Choices"] if _rule(c, data)), state.get("Default"))
            assert name, f"Choice {path[-1]} matched nothing and has no Default"
            continue
        if kind == "Pass":
            if "Parameters" in state:
                data = _resolve(state["Parameters"], data, context)
            name = state["Next"]
            continue
        if kind == "Wait":
            name = state["Next"]
            continue
        if kind == "Map":
            failed = False
            for item in _get(data, state["ItemsPath"]):
                item_input = _resolve(state["ItemSelector"], data, {**context, "$$.Map.Item.Value": item})
                proc = state["ItemProcessor"]
                if _walk(proc["States"], proc["StartAt"], item_input, tasks, path, context) == "Fail":
                    failed = True
                    break
            if failed:
                catch = state["Catch"][0]
                data = {**data, "error": {"Error": "States.TaskFailed"}}
                name = catch["Next"]
                continue
            name = state["Next"]
            continue
        if kind == "Task":
            result = tasks(name, state, data)
            if state.get("ResultPath", "$") is not None:
                data = {**data, state["ResultPath"].removeprefix("$."): result}
            name = state["Next"]
            continue
        raise AssertionError(f"state type {kind} not modelled by this walker")
    raise AssertionError("the execution did not terminate within 500 transitions — a stuck state")


class Execution:
    """One execution of the deployed data-collection machine, on the case's own
    schedule input (EOD unless the case says otherwise), with its phase run for real."""

    def __init__(self, s3: InMemoryS3, case: Case, rows_per_attempt):
        self.s3 = s3
        self.case = case
        self.rows_per_attempt = rows_per_attempt
        self.attempts: list[dict] = []
        self.pages: list[str] = []
        self.path: list[str] = []
        # The same clock the manifests are stamped by (the `clock` fixture).
        self.started_at = run_manifest.dt.datetime.now(dt.timezone.utc)

    def tasks(self, name: str, state: dict, data: dict):
        resource = state["Resource"]
        if resource.endswith(":sns:publish"):
            self.pages.append(name)
            return None
        if name == "TradingDayCheck":
            return {"is_trading_day": True}
        if name == "LaunchWorkload":
            return {"data_spot": {"launched": True, "command_id": "cmd", "instance_id": "i-0"}}
        if name == "PollWorkload":
            # A sibling harness's own case type may carry no workload: those walk EOD.
            if data["workload"] != getattr(self.case, "workload", WORKLOAD):
                return {"Status": "Success", "StatusDetails": "Success"}
            rows = self.rows_per_attempt[len(self.attempts)]
            result = _run_phase(self.s3, self.case, rows)
            self.attempts.append(result)
            status = _process_status(result)
            return {"Status": status, "StatusDetails": status}
        if name == "VerifyRunManifests":
            # The dispatcher Lambda's own action, graded over the faulted unit.
            out = completion._completion_check(
                {
                    "collection": data["collection"],
                    "units": [self.case.unit],
                    "started_at": data["started_at"],
                },
                s3_client=self.s3,
            )["completion"]
            self.completion = out
            return out
        raise AssertionError(f"task {name} not modelled by this walker")

    def run(self) -> str:
        asl = json.loads(ASL_PATH.read_text())
        data = _deployed_input(getattr(self.case, "collection", "eod"))
        context = {
            "$$.Execution.Id": "arn:aws:states:::execution:commissioning",
            "$$.Execution.StartTime": self.started_at.isoformat().replace("+00:00", "Z"),
        }
        return _walk(asl["States"], asl["StartAt"], data, self.tasks, self.path, context)


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_empty_frame_fails_pages_once_and_stands_down(case, s3, enforcing, caplog):
    """The commissioning record for ``empty_fresh``, under the promoted guard, per unit."""
    caplog.set_level(logging.ERROR, logger=weekly_collector.logger.name)

    # Induce: the empty publish, on the attempt AND on the on-demand retry
    # (the fault is in the data, so the retry meets it again).
    faulted = Execution(s3, case, rows_per_attempt=[0, 0])
    terminal = faulted.run()

    # 1. A `failed` manifest, carrying the guard's verdict under `enforce`.
    manifests = s3.manifests(case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert "empty_fresh guard" in m["reason"]
        guard = _worst_guard(m)
        assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "enforce")
        assert sorted((o["key"], o["rows_out"]) for o in m["outputs"]) == sorted(case.written(0).items())

    # 2. Exactly one page — not zero, not one per attempt.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    guard_errors = [r for r in caplog.records if "guard=data_empty_fresh" in r.getMessage()]
    # One ERROR verdict line per EMPTY key per attempt: every published PUT is
    # graded, and a key the run wrote with rows (D21's FX file, D31's registry)
    # is graded `ok` and logs nothing.
    assert len(faulted.attempts) == 2
    empty_keys = [k for k, n in case.written(0).items() if n == 0]
    assert empty_keys, "a case with no empty key induces no fault"
    assert len(guard_errors) == len(faulted.attempts) * len(empty_keys)

    # 3. Stand-down: terminal Fail, the retry RECOMPUTED (never auto-skipped
    # the refused artifact), and the marker is not left `ok`.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert faulted.path.count("RetryOnDemand") == 1
    assert not any(a.get("auto_skipped") for a in faulted.attempts)
    marker_after_fault = s3.marker(case)
    assert marker_after_fault["status"] == "error"
    assert "enforcing empty_fresh guard rejected this publish" in marker_after_fault["error"]

    # ...and once the fault is gone, the next run is clean end to end.
    recovered = Execution(s3, case, rows_per_attempt=[896])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True
    # Keys list in run order (HarnessClock), so the completion check's
    # "newest" is the recovery run, not whichever ULID drew higher random bits.
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed", "ok"]
    assert s3.marker(case)["status"] == "ok"

    _print_record(case, faulted, recovered, s3, marker_after_fault)


def test_the_same_fault_under_the_shipped_observe_mode_pages_once_through_the_completion_check(s3):
    """What OBSERVE does with the same fault — recorded, not commissioned.

    The manifest stays ``ok`` (observe moves no consequence) with the RED
    verdict on it, the process exits 0, and the page comes from the
    completion check's per-key floor (``rows_below_floor``), not from the
    guard. So the empty frame is caught today, once — by the completion
    check. That is why this is not the commissioning record: the guard's own
    consequence is only reachable once it enforces.
    """
    observed = Execution(s3, D19, rows_per_attempt=[0])
    assert observed.run() == "Fail"
    m = s3.manifests(D19)[-1]
    assert m["status"] == "ok"
    guard = next(g for g in m["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
    assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "observe")
    assert observed.pages == ["NotifyCompletionFindings"]
    assert observed.completion["failure_mode"] == "rows_below_floor"
    assert observed.path[-1] == "RowsBelowFloor"


def test_observe_mode_never_withholds_the_marker(s3):
    """The marker fix is gated on ENFORCE: in observe, behaviour is unchanged."""
    _run_phase(s3, D19, 0)
    assert s3.marker(D19)["status"] == "ok"


def test_every_case_is_a_phase_unit_on_the_workload_and_schedule_it_runs_on():
    """A case is commissioned only on the machine, schedule and workload it actually runs on.

    The record's page and stand-down are walked on the deployed data-collection
    machine, started by the case's own schedule input, whose ``verify_units``
    must name the unit (else the completion check grades nothing for it) and
    whose ``workloads`` must carry the workload that runs the case's mode: EOD
    ``post-market-data`` for ``daily``, weekly ``weekly-phase-one`` for
    ``phase1`` and ``alternative-phase-two`` for ``phase2``.
    """
    mode_workload = {"daily": WORKLOAD, "phase1": "weekly-phase-one", "phase2": "alternative-phase-two"}
    for case in CASES:
        schedule = _deployed_input(case.collection)
        assert case.workload == mode_workload[case.mode], case.unit
        assert case.workload in schedule["workloads"], case.unit
        assert run_units.PHASE_UNITS[(case.mode, case.phase)].unit_id == case.unit
        assert case.unit in schedule["verify_units"], case.unit
        unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
        assert unit.guards[GUARD_CLASS]["state"] == "present", case.unit


def test_no_case_is_on_the_trading_path():
    """D01, D02 and D19 are held for every guard class (the trader's feed and the executor's)."""
    assert not {"D01", "D02", "D19"} & {c.unit for c in WEEKLY_CASES}
    assert [c.unit for c in CASES].count("D19") == 1 and D19 is EOD_CASES[0]


def test_the_weekly_call_sites_are_what_the_cases_call():
    """Each weekly case's primary key is the literal its call site passes.

    The cases build their keys from constants, so a call site that moved its key
    would leave a record graded on a key production no longer publishes. Pin the
    source text of each call site's ``artifact_key`` (and the extra-output helpers
    D04 and D13 hand ``_phase_collect``) against the case.
    """
    source = (REPO_ROOT / "weekly_collector.py").read_text()
    for literal in (
        'artifact_key=f"{market_prefix}weekly/{run_date}/macro.json"',
        'artifact_key=f"{market_prefix}weekly/{run_date}/short_interest.json"',
        'artifact_key=f"{market_prefix}universe_classification/{run_date}.json"',
        'f"{market_prefix}universe_classification/latest.json"',
        'artifact_key=f"archive/fundamentals/{run_date}.json"',
        'artifact_key=f"{metron_market_data.VALUATION_MEDIANS_PREFIX}latest.json"',
        'artifact_key=f"{market_prefix}weekly/{run_date}/alternative/manifest.json"',
        'scope_key = f"{market_prefix}weekly/{run_date}/alternative/scope.json"',
        'f"{market_prefix}macro_history.parquet"',
        'f"{market_prefix}macro_release_calendar.parquet"',
        'f"backups/research_{run_date}.db"',
        'tickers=["TWO", "HYOAS", "BAA10Y"]',
        "extra_outputs=_fred_history_extra_outputs(",
        "extra_outputs=_arcticdb_backfill_extra_outputs(),",
    ):
        assert literal in source, literal


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(case, tmp_path):
    """``faults/<unit>/<guard>/latest.json`` with ``outcome: induced`` reads MET.

    The gate reader is `data_gate.evidence.read_guard_commissioning`. The
    document is built by the same function the publisher
    (`scripts/publish_guard_commissioning.py`) writes, so what this asserts is
    what lands in the store.
    """
    unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(TRADING_DAY)
    before = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    document = record_document(case.unit, GUARD_CLASS, fault=case.fault, test=nodeid(case), code_sha="c" * 40)
    store.put_bytes(record_key(case.unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def _worst_guard(manifest: dict) -> dict:
    """The manifest's ``data_empty_fresh`` reading that carries the verdict.

    A unit with no primary key (D03, D21, D26) files a ``not_applicable`` reading
    for that absent key BEFORE it files the readings for the keys it did publish,
    so the first entry is not the verdict. The guard's own rule is the worst
    reading wins (``weekly_collector._worst_reading``); the same rule picks it here.
    """
    readings = [g for g in manifest["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name]
    assert readings, manifest["guards"]
    return max(
        readings,
        key=lambda g: weekly_collector._EMPTY_FRESH_SEVERITY.get(g["verdict"], 2),
    )


def nodeid(case: Case) -> str:
    return (
        "tests/test_guard_commissioning_empty_fresh.py::"
        f"test_the_fault_record_empty_frame_fails_pages_once_and_stands_down[{case.unit}]"
    )


def _print_record(case: Case, faulted: Execution, recovered: Execution, s3: InMemoryS3, marker: dict) -> None:
    """The fault record, printed for the PR body (`pytest -s`)."""
    first = s3.manifests(case)[0]
    guard = _worst_guard(first)
    lines = [
        f"FAULT RECORD guard={GUARD_CLASS} unit={case.unit} phase={case.phase} workload={case.workload}",
        f"  fault:      {case.fault}",
        "  attempts:   " + ", ".join(f"{a['status']}" for a in faulted.attempts),
        "  manifests:  " + ", ".join(m["status"] for m in s3.manifests(case)[: len(faulted.attempts)]),
        f"  verdict:    {guard['verdict']} mode={guard['mode']}: {guard['detail']}",
        f"  pages:      {len(faulted.pages)} {faulted.pages}",
        "  path:       " + " > ".join(faulted.path),
        f"  marker:     {marker['status']} ({marker['error'][:90]}...)",
        "  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})",
    ]
    print("\n" + "\n".join(lines))
