"""Which collector phase is which audit unit, and how a run records itself.

`data_collection_plan_260914.md` §2 row 7 / §4.4; `alpha-engine-config-I10773`
(P-06) and `-I10790` (P-24).

The wrapper itself lives in `nousergon_lib.run_manifest` — lifted from
crucible's `runner.run_job` on second adoption. What lives HERE is the part
that is specific to this repo: **which unit a given collector phase is**, and
where its manifests, logs and triggers come from in this process.

**Why a declared table.** `registry.d/units/*.yaml` is the source of units, and
`data_gate/descriptors.py` is the source of truth for that set. What the
descriptors deliberately do NOT carry is a machine-readable link from a
descriptor to the call site — `code_path` is a human-readable pointer
("weekly_collector.py:2811 -> collectors/daily_closes.collect(yfinance_only)"),
not a symbol a program can resolve. So the link is declared once, here, and
`tests/test_run_units.py` grades it against the descriptor set in both
directions: an entry naming a unit that has no descriptor fails, and a
``_phase_collect`` call site with no entry fails. The table cannot drift
silently, which is the only property that matters.

**Why the row-count key is part of the entry.** The empty-but-fresh objective
(plan §2 row 6) is counted from the manifest's ``rows_out``, so every unit has
to say how many rows it published. The collectors do not agree on a key for
that — ``rows``, ``rows_written``, ``tickers_captured``, ``n_changes``,
``technicals`` — and there is no way to guess one safely: a missing key read as
zero would report every unit as an empty-but-fresh write, and a missing key read
as "fine" would report none of them. So each entry NAMES the key its collector
reports, and an entry whose key is ``None`` produces an **unmeasurable** guard
verdict rather than a zero. Unmeasurable is red and counted; it is a work item
with an address, not a pass.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass

from nousergon_lib import run_manifest
from nousergon_lib.run_manifest import DEFAULT_MANIFEST_PREFIX, S3ManifestSink

__all__ = [
    "LOG_LOCATION_ENV",
    "MANIFEST_BUCKET",
    "MODE_UNITS",
    "PHASE_UNITS",
    "TRIGGER_ENV",
    "PhaseUnit",
    "manifest_sink",
    "manual_run",
    "resolve_log_location",
    "resolve_trigger",
    "unit_for",
]

#: The box shell (the SF workload launcher, the spot dispatcher) exports these.
#: Both have honest fallbacks rather than defaults that would state something
#: the process did not measure.
TRIGGER_ENV = "NE_DATA_TRIGGER"
LOG_LOCATION_ENV = "NE_DATA_LOG_LOCATION"

#: The durable log location every scheduled collector workload already writes
#: to (`observability-policy` §6.2; plan §4.4 "log capture"). Used when the box
#: exported no more specific stream.
_DEFAULT_LOG_GROUP = "cloudwatch:/alpha-engine/data-spot"

#: Where run manifests live. The same bucket every collector already writes to,
#: so `data_collection/runs/` needs no new grant on the writer identity.
MANIFEST_BUCKET = "alpha-engine-research"


@dataclass(frozen=True)
class PhaseUnit:
    """One ``_phase_collect`` phase, and the audit unit it IS.

    Args:
        unit_id: The descriptor under ``registry.d/units/`` this phase runs.
        rows_key: The key the collector's own result dict reports its published
            row count under, or ``None`` when the collector does not report one
            — which is recorded as an unmeasurable guard verdict, never as 0.
        mode: Which run mode the phase belongs to. Phase names are not unique
            across modes (``prices`` and ``features`` each appear in two), so
            the mode is half the key.
    """

    unit_id: str
    rows_key: str | None
    mode: str


#: (mode, phase name) -> the unit it is. Modes match ``run_weekly``'s dispatch:
#: ``phase1``/``phase2`` are the Saturday weekly machine, ``daily`` is the
#: weekday EOD machine, and the four self-contained modes each ARE one unit and
#: are wrapped whole (see ``MODE_UNITS``).
PHASE_UNITS: dict[tuple[str, str], PhaseUnit] = {
    # ── weekly, phase 1 ────────────────────────────────────────────────────
    ("phase1", "constituents"): PhaseUnit("D01", "count", "phase1"),
    ("phase1", "historical_constituents"): PhaseUnit("D02", "n_changes", "phase1"),
    ("phase1", "prices"): PhaseUnit("D03", "refreshed", "phase1"),
    ("phase1", "fred_macro_history"): PhaseUnit("D04", "rows", "phase1"),
    ("phase1", "macro"): PhaseUnit("D05", "rows", "phase1"),
    ("phase1", "short_interest"): PhaseUnit("D06", "ok_count", "phase1"),
    ("phase1", "universe_classification"): PhaseUnit("D07", "ok_count", "phase1"),
    ("phase1", "universe_returns"): PhaseUnit("D08", "rows_inserted", "phase1"),
    ("phase1", "signal_returns"): PhaseUnit("D09", "rows_written", "phase1"),
    ("phase1", "fundamentals"): PhaseUnit("D10", "n_ok", "phase1"),
    ("phase1", "metron_valuation_medians"): PhaseUnit("D11", "covered", "phase1"),
    # features/compute.py and builders/backfill.py report no row count on their
    # success path; the guard reads UNMEASURABLE for these rather than 0.
    ("phase1", "features"): PhaseUnit("D12", None, "phase1"),
    ("phase1", "arcticdb"): PhaseUnit("D13", None, "phase1"),
    # ── weekly, phase 2 ────────────────────────────────────────────────────
    ("phase2", "alternative"): PhaseUnit("D15", "tickers_processed", "phase2"),
    # ── weekday EOD ───────────────────────────────────────────────────────
    ("daily", "daily_closes"): PhaseUnit("D19", "tickers_captured", "daily"),
    ("daily", "prices"): PhaseUnit("D03", "refreshed", "daily"),
    ("daily", "metron_market_data"): PhaseUnit("D20", "closes", "daily"),
    ("daily", "metron_market_data_history"): PhaseUnit("D21", "close_series", "daily"),
    ("daily", "metron_reference_data"): PhaseUnit("D22", "sectors", "daily"),
    ("daily", "metron_macro_data"): PhaseUnit("D23", "series", "daily"),
    ("daily", "metron_fundamentals_data"): PhaseUnit("D24", "fundamentals", "daily"),
    ("daily", "metron_technicals_data"): PhaseUnit("D25", "technicals", "daily"),
    ("daily", "metron_rating_ledger"): PhaseUnit("D26", "total_dates", "daily"),
    ("daily", "metron_rating_performance"): PhaseUnit("D27", "ledger_dates_used", "daily"),
    ("daily", "metron_security_performance_data"): PhaseUnit("D28", "performance", "daily"),
    ("daily", "metron_analyst_data"): PhaseUnit("D29", "analyst", "daily"),
    ("daily", "metron_sentiment_data"): PhaseUnit("D30", "sentiment", "daily"),
    ("daily", "features"): PhaseUnit("D31", None, "daily"),
    ("daily", "arcticdb"): PhaseUnit("D32", None, "daily"),
}

#: The run modes that ARE one unit end to end, wrapped whole at the dispatch in
#: ``run_weekly`` rather than per-phase. Each value is the unit id.
MODE_UNITS: dict[str, str] = {
    "morning_enrich": "D17",
    "morning_arctic_append": "D18",
    "daily_heal": "D33",
    "chronic_gap_heal": "D34",
    "daily_arctic_append": "D32",
}


def unit_for(mode: str, phase: str) -> PhaseUnit:
    """The unit a ``_phase_collect`` phase is, or RAISE.

    Fail-loud by design: a phase with no declared unit would write its manifest
    nowhere, and a collector whose runs are invisible is the exact defect the
    run-record objective exists to end. Adding a phase is adding a row here.
    """
    try:
        return PHASE_UNITS[(mode, phase)]
    except KeyError:
        raise KeyError(
            f"collector phase {phase!r} in mode {mode!r} has no declared audit unit in "
            "run_units.PHASE_UNITS. Every execution of every unit writes a run manifest "
            "(data_collection_plan_260914.md §2 row 7) — a phase with no unit id has "
            "nowhere to write one. Add the row, with the descriptor under "
            "registry.d/units/ it names."
        ) from None


def resolve_trigger(default: str) -> str:
    """How this execution was started.

    ``$NE_DATA_TRIGGER`` wins when the launcher exported it. ``default`` is the
    trigger the unit's own descriptor declares for its normal path — every
    ``weekly_collector.py`` mode is ``scheduled``, the manual repair builders
    are ``manual``, the benchmark-proxy backfill is ``on_demand``. An operator
    running one of these by hand exports the variable to say so.
    """
    return os.environ.get(TRIGGER_ENV) or default


def resolve_log_location() -> str:
    """Where this run's FULL logs are (`observability-policy` §6.2).

    The box shell exports the CloudWatch stream or the SSM capture key it is
    writing to. Off a box, the answer is the host and pid, which is a true
    statement about where the logs are rather than a CloudWatch group this run
    never wrote to.
    """
    declared = os.environ.get(LOG_LOCATION_ENV)
    if declared:
        return declared
    if os.environ.get("NE_DATA_INSTANCE_TYPE"):
        return _DEFAULT_LOG_GROUP
    return f"local:{socket.gethostname()}:{os.getpid()}"


def manual_run(
    unit_id: str,
    fn,
    *,
    write: bool,
    bucket: str = MANIFEST_BUCKET,
    trigger: str = "manual",
    trading_day: str | None = None,
):
    """Run a manual/on-demand tool under the same wrapper a scheduled unit uses.

    `data_collection_plan_260914.md` §4.4: "Manual and GHA units call the same
    wrapper, so a hand-run repair still leaves a record"
    (`alpha-engine-config-I10790`). The point is not symmetry for its own sake:
    a repair that writes production data and leaves no record is the one class
    of write nothing on the board can attribute, and the units it touches are
    D13's ArcticDB libraries, which every consumer reads.

    ``write=False`` (a dry run, or a tool's default no-``--apply`` mode) passes
    ``sink=None``: the body runs exactly as it would and nothing is written,
    which is what those flags already promise.

    These tools run **in-region only** (fleet `CLAUDE.md`; the ArcticDB bucket
    carries an explicit Deny that blocks even `ne-admin` from the laptop,
    `alpha-engine-config-I9771`). This wrapper does not enforce that — the
    component role under `alpha-engine-config-I10756` does — but the manifest's
    `compute` row records where the run actually happened, so a laptop
    invocation is visible after the fact rather than merely forbidden.
    """
    from dates import default_run_date  # local: keeps `dates` off the import path of CLIs that do not need it

    return run_manifest.run_unit(
        unit_id,
        fn,
        sink=manifest_sink(bucket) if write else None,
        trigger=resolve_trigger(trigger),
        trading_day=trading_day or default_run_date(),
        log_location=resolve_log_location(),
    )


def manifest_sink(bucket: str, s3_client=None) -> S3ManifestSink:
    """The production sink for this repo's run manifests.

    The collectors' writer identity already holds ``alpha-engine-research/*``,
    so ``data_collection/runs/`` needs no new grant.
    """
    return S3ManifestSink(bucket=bucket, prefix=DEFAULT_MANIFEST_PREFIX, s3_client=s3_client)
