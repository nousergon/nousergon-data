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

import datetime as dt
import logging
import os
import socket
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nousergon_lib import run_manifest
from nousergon_lib.run_manifest import DEFAULT_MANIFEST_PREFIX, S3ManifestSink

__all__ = [
    "LOG_LOCATION_ENV",
    "MANIFEST_BUCKET",
    "MODE_ROWS",
    "MODE_UNITS",
    "NOT_RUN_DISABLED_BY_DECLARATION",
    "NOT_RUN_NO_NEW_DATA_DECLARED",
    "NOT_RUN_OUTSIDE_SESSION_WINDOW",
    "PHASE_UNITS",
    "TRIGGER_ENV",
    "EntryRunFailed",
    "ModeRows",
    "PhaseUnit",
    "manifest_sink",
    "manual_run",
    "resolve_log_location",
    "recorded_entry",
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

logger = logging.getLogger(__name__)


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
    #: ``(result key, rejection reason)`` pairs — the counts this collector
    #: already reports for records it did NOT publish. `alpha-engine-config-
    #: I10810` deliverable 2: a manifest that carries ``rows_out`` without
    #: ``rows_rejected`` says a run published N rows and is silent about the
    #: M it dropped, which is the same shape of blindness ``rows_out: 0``
    #: without a guard verdict has. Declared per unit for the same reason
    #: ``rows_key`` is: the collectors do not agree on a key, and guessing one
    #: either invents rejections or hides them.
    rejected_keys: tuple[tuple[str, str], ...] = ()


#: Every class of ticker ``builders.daily_append.daily_append`` counted and did
#: NOT append, by the key it reports it under. Shared by the D32 phase entry and
#: by the whole-mode append units, which call the same function.
_APPEND_REJECTED_KEYS: tuple[tuple[str, str], ...] = (
    ("tickers_errored", "append_error"),
    ("tickers_skipped", "already_present"),
    ("tickers_missing_from_closes", "missing_from_daily_closes"),
    ("tickers_quality_blocked", "quality_gate_blocked"),
    ("tickers_l2_quarantined", "l2_gate_quarantined"),
)


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
    # features/compute.py reports no row count on its success path; the guard
    # reads UNMEASURABLE for D12/D31 rather than 0. Closing that needs
    # `compute_and_write` to return the snapshot's row count —
    # `alpha-engine-config-I10810` deliverable 2, deferred because `features/`
    # is owned by a concurrent change set.
    ("phase1", "features"): PhaseUnit("D12", None, "phase1"),
    # builders/backfill.py DOES report one (`tickers_written` = n_ok), with the
    # two counts of what it did not write alongside it.
    ("phase1", "arcticdb"): PhaseUnit(
        "D13",
        "tickers_written",
        "phase1",
        rejected_keys=(("tickers_errored", "backfill_error"), ("tickers_skipped", "backfill_skipped")),
    ),
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
    # builders/daily_append.py reports `tickers_appended` (n_ok) and names every
    # class of ticker it did NOT append.
    ("daily", "arcticdb"): PhaseUnit(
        "D32",
        "tickers_appended",
        "daily",
        rejected_keys=_APPEND_REJECTED_KEYS,
    ),
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


@dataclass(frozen=True)
class ModeRows:
    """Where a whole-mode unit's published row count lives in its own result.

    `alpha-engine-config-I10810` deliverable 2. The five whole-mode units do not
    go through ``_phase_collect``, so they have no ``PhaseUnit.rows_key``; what
    they DO have is a nested ``results["collectors"][<step>]`` dict written by
    the step that published. Naming the step and the key here — rather than
    searching the result for something that looks like a count — is the same
    discipline ``rows_key`` enforces one level down: a renamed key is a loud
    ``unmeasurable``, never a silent zero.

    Args:
        collector: The key under ``results["collectors"]`` that published.
        rows_key: The key in THAT dict carrying the published count.
        counts_list: ``rows_key`` names a list whose LENGTH is the count (the
            two heal units report healed items, not a number).
        rejected_keys: ``(key, reason)`` pairs in that same dict.
    """

    collector: str
    rows_key: str
    counts_list: bool = False
    rejected_keys: tuple[tuple[str, str], ...] = ()


#: mode -> where that mode's row count lives. A mode in :data:`MODE_UNITS` with
#: no entry here records an ``unmeasurable`` guard verdict rather than 0 — red,
#: counted, and with an address.
MODE_ROWS: dict[str, ModeRows] = {
    # MorningEnrich and both ArcticDB appends publish through
    # `builders.daily_append.daily_append`, surfaced as the `arcticdb` step.
    "morning_enrich": ModeRows("arcticdb", "tickers_appended", rejected_keys=_APPEND_REJECTED_KEYS),
    "morning_arctic_append": ModeRows("arcticdb", "tickers_appended", rejected_keys=_APPEND_REJECTED_KEYS),
    "daily_arctic_append": ModeRows("arcticdb", "tickers_appended", rejected_keys=_APPEND_REJECTED_KEYS),
    # The two heal units publish DAYS and TICKERS respectively, as lists.
    "daily_heal": ModeRows("universe_gap_heal", "healed_days", counts_list=True),
    "chronic_gap_heal": ModeRows("chronic_gap_self_heal", "healed", counts_list=True),
}

#: The :data:`NOT_APPLICABLE_REASONS` members this repo's whole-mode/phase
#: non-runs map onto (`alpha-engine-config-I10831` deliverable 1, landed as
#: `nousergon-lib` v0.124.130 / nousergon-lib-PR416). Each name is matched to
#: the lib's own one-line definition for that member
#: (`nousergon_lib.run_manifest.NOT_APPLICABLE_REASONS`'s docstring), not
#: guessed:
#:
#: * `disabled_by_declaration` — "the unit itself is switched off by a
#:   standing config declaration ... independent of what any upstream
#:   published this cycle. The non-run is an operator decision, not a data
#:   observation." Matches a collector switched off in `config.yaml`.
#: * `outside_session_window` — "the unit's own schedule fires more often
#:   than its declared window ... and this tick landed outside it. The
#:   non-run is a clock fact." Matches a 5-minute intraday timer's off-session
#:   tick.
#: * `no_new_data_declared` — "an upstream explicitly declared there is
#:   nothing new for THIS run to collect (a vendor feed with no fresh rows, a
#:   target date already published)." Matches MorningEnrich's own freshness
#:   guard (`_should_skip_morning_enrich`'s `stale_overwrite` reason) —
#:   neither an operator decision nor a clock fact, but exactly the lib's own
#:   "target date already published" example.
#:
#: None is a soft `ok`: the manifest is written, the non-run is COUNTED, and a
#: unit answering `not_applicable` every cycle is visible as a unit that has
#: stopped working.
#:
#: Corrected 2026-09-15 (`alpha-engine-config-I10831` review): an earlier
#: revision of this pin mapped the `stale_overwrite` case to
#: `disabled_by_declaration` and defaulted every OTHER skip reason to
#: `outside_session_window` — a default bucket for an unenumerated case, not a
#: match against the lib's own definitions. `weekly_collector.py`'s whole-mode
#: dispatch now raises loud on any skip_reason it has not explicitly
#: classified, rather than defaulting.
NOT_RUN_DISABLED_BY_DECLARATION = "disabled_by_declaration"
NOT_RUN_OUTSIDE_SESSION_WINDOW = "outside_session_window"
NOT_RUN_NO_NEW_DATA_DECLARED = "no_new_data_declared"


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


# ---------------------------------------------------------------------------
# Standalone entry points — a unit whose execution IS its own process.
# ---------------------------------------------------------------------------
#
# `run_units` above answers *which collector phase inside `weekly_collector.py`
# is which audit unit*. This section answers the other half: a unit whose entry
# point is its OWN process — a systemd timer (D36, D37), a Lambda handler
# (D38), a GitHub Actions job (D39, D42) — has no `_phase_collect` to hang a
# manifest off, and each of those five call sites would otherwise grow its own
# copy of the same twenty lines. One copy, here, alongside `manual_run`, which
# is the same concern for hand-run repairs.
#
# **The two rules that shape the API.**
#
# 1. *The record layer must not become a new way for a producer to die.*
#    `run_manifest.run_unit` resolves the running tree's commit sha BEFORE the
#    body runs and REFUSES an invocation that cannot name one. That refusal is
#    right for the manifest and wrong for the collector: a box whose `git` went
#    missing would stop collecting data, not merely stop recording it. So the
#    sha is resolved in `recorded_entry` first, and a failure to measure it
#    degrades to running the body unrecorded with a loud ERROR — never to
#    skipping the unit's work. The degradation is visible: no manifest lands
#    under `data_collection/runs/<unit>/`, which the run-record clause reads as
#    a missing run. Red, never green.
#
# 2. *A body whose work failed still returns what its caller expected.* The
#    established idiom is `weekly_collector.py::_run_whole_mode_unit`: the body
#    raises a sentinel so the manifest says `failed`, and the sentinel is caught
#    OUTSIDE `run_unit` so the caller's return value and the process's
#    exit-code contract are unchanged. `EntryRunFailed` is that sentinel,
#    lifted so every entry point raises the same one.
#
# A write error from the sink itself is deliberately NOT caught (see
# `run_manifest.run_unit`'s own note): a manifest that failed to write is a run
# that did not happen as far as every downstream reader is concerned.

class EntryRunFailed(Exception):
    """Raised INSIDE a unit body whose work reported failure.

    The manifest is written with ``status: failed`` and the reason this
    carries; :func:`recorded_entry` then returns ``value`` to the caller, so the
    entry point's own exit-code contract is exactly what it was before the
    manifest existed. Mirrors ``weekly_collector.py::_CollectorError``.
    """

    def __init__(self, reason: str, value: Any = None) -> None:
        super().__init__(reason)
        self.value = value


def _unrecorded_ctx(unit_id: str, trading_day: str, trigger: str) -> run_manifest.UnitRun:
    """A context the body can record on when NO manifest can be written.

    Everything recorded on it is discarded. It exists so the body is one code
    path whether or not the record layer is available — a second, manifest-less
    body is how the two drift until the recorded one is the untested one.
    """
    started = dt.datetime.now(dt.timezone.utc)
    return run_manifest.UnitRun(
        run_id=run_manifest.new_run_id(started),
        unit_id=unit_id,
        trading_day=trading_day,
        calendar_date=started.date().isoformat(),
        trigger=trigger,
        started=started,
        code_sha="",
        log_location=resolve_log_location(),
    )


def recorded_entry(
    unit_id: str,
    body: Callable[[run_manifest.UnitRun], Any],
    *,
    trigger: str,
    trading_day: str,
    bucket: str = MANIFEST_BUCKET,
    write: bool = True,
    code_sha: str | None = None,
    s3_client: Any = None,
) -> Any:
    """Run ``body`` as unit ``unit_id`` and write one manifest for it.

    Args:
        unit_id: The descriptor under ``registry.d/units/`` this process IS.
        body: ``fn(ctx)`` — records its own inputs, outputs and guards on
            ``ctx`` and raises :class:`EntryRunFailed` if its work failed.
        trigger: The unit's normal trigger, per its descriptor
            (``scheduled`` for a timer, ``gha`` for a workflow). Overridden by
            ``$NE_DATA_TRIGGER`` when the launcher declared one.
        trading_day: The trading-day axis this run is keyed on.
        bucket: Where the manifest lands. The collectors' writer identity
            already holds ``alpha-engine-research/*``.
        write: ``False`` for a dry run — the body runs exactly as it would and
            no manifest is written (``run_unit`` logs one line in its place).
        code_sha: The sha of the tree that is running, when the caller knows it
            better than this process can measure (a Lambda has no git checkout;
            a migration run is FOR a named merge sha). ``None`` measures it.
        s3_client: Injectable for tests.

    Returns whatever ``body`` returned, or — when ``body`` raised
    :class:`EntryRunFailed` — the value that sentinel carried. Every other
    exception propagates AFTER its manifest is durable.
    """
    if code_sha is None:
        try:
            code_sha = run_manifest.resolve_code_sha()
        except run_manifest.CodeShaError as exc:
            # DELIBERATE degrade, per rule 1 in rule 1 of the section header above.
            # (a) Failure mode swallowed: this process cannot measure the commit
            #     sha of the tree it is running, so no well-formed manifest can
            #     be written for this execution.
            # (b) Recording surface: this ERROR line, AND the absence of an
            #     object under data_collection/runs/<unit_id>/<trading_day>/,
            #     which the run-record clause grades as a missing run — red, and
            #     counted. The unit's DATA work still runs: the record layer
            #     never decides whether a producer produces.
            logger.error(
                "unit %s: cannot measure code_sha (%s) — running UNRECORDED. No manifest "
                "will be written for this execution and the run-record clause will read "
                "it as a missing run. Export $%s on a host with no git checkout.",
                unit_id, exc, run_manifest.CODE_SHA_ENV,
            )
            try:
                return body(_unrecorded_ctx(unit_id, trading_day, resolve_trigger(trigger)))
            except EntryRunFailed as failed:
                return failed.value

    try:
        return run_manifest.run_unit(
            unit_id,
            body,
            sink=manifest_sink(bucket, s3_client) if write else None,
            trigger=resolve_trigger(trigger),
            trading_day=trading_day,
            log_location=resolve_log_location(),
            code_sha=code_sha,
        ).value
    except EntryRunFailed as failed:
        # The manifest is already written with `status: failed`. The caller
        # keeps the value it would have received before this wrapper existed.
        return failed.value
