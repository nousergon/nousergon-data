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
import functools
import logging
import os
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from nousergon_lib import run_manifest
from nousergon_lib.run_manifest import DEFAULT_MANIFEST_PREFIX, S3ManifestSink

__all__ = [
    "LOG_LOCATION_ENV",
    "RUN_LOG_S3_ENV",
    "MANIFEST_BUCKET",
    "MODE_ROWS",
    "MODE_UNITS",
    "NOT_RUN_DISABLED_BY_DECLARATION",
    "NOT_RUN_NO_NEW_DATA_DECLARED",
    "NOT_RUN_OUTSIDE_SESSION_WINDOW",
    "PHASE_UNITS",
    "TRIGGER_ENV",
    "EMPTY_PRODUCTION_GUARD",
    "EMPTY_IS_VALID_FIELD",
    "EmptyDeclaration",
    "EmptyProduction",
    "EntryRunFailed",
    "ModeRows",
    "NotInRegionError",
    "PhaseUnit",
    "empty_declaration",
    "empty_declaration_for",
    "is_empty_success",
    "manifest_sink",
    "manual_run",
    "record_empty_production",
    "require_in_region",
    "resolve_log_location",
    "recorded_entry",
    "resolve_trigger",
    "truncate_reason",
    "unit_for",
]

#: The box shell (the SF workload launcher, the spot dispatcher) exports these.
#: Both have honest fallbacks rather than defaults that would state something
#: the process did not measure.
TRIGGER_ENV = "NE_DATA_TRIGGER"
LOG_LOCATION_ENV = "NE_DATA_LOG_LOCATION"

#: The S3 URI of THIS run's whole shipped log, exported by the data-spot
#: dispatcher's bootstrap wrapper (`infrastructure/lambdas/data-spot-dispatcher/
#: index.py::RUN_LOG_ENV`, alpha-engine-config-I11353). It is the durable
#: record; the CloudWatch stream below is a capped live tail — measured
#: 2026-09-21 as 1,025,054 bytes over a 73-minute run, ending three minutes in.
RUN_LOG_S3_ENV = "ALPHA_ENGINE_RUN_LOG_S3"

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
    # alpha-engine-config-I11230 deliverable 2: `collectors/prices.py::collect`
    # already reports its own not-published count under `failed` (tickers the
    # short-fetch guard refused to overwrite, or a batch download that raised)
    # — declared here so the manifest's `rows_rejected` names them, the same
    # way D12/D13 already do for their own collectors, instead of leaving the
    # loss visible only in the `reason` string on a `partial` (now `failed`)
    # manifest.
    ("phase1", "prices"): PhaseUnit(
        "D03", "refreshed", "phase1",
        rejected_keys=(("failed", "short_fetch_guard_refused"),),
    ),
    ("phase1", "fred_macro_history"): PhaseUnit("D04", "rows", "phase1"),
    ("phase1", "macro"): PhaseUnit("D05", "rows", "phase1"),
    ("phase1", "short_interest"): PhaseUnit("D06", "ok_count", "phase1"),
    ("phase1", "universe_classification"): PhaseUnit("D07", "ok_count", "phase1"),
    ("phase1", "universe_returns"): PhaseUnit("D08", "rows_inserted", "phase1"),
    ("phase1", "signal_returns"): PhaseUnit("D09", "rows_written", "phase1"),
    ("phase1", "fundamentals"): PhaseUnit("D10", "n_ok", "phase1"),
    ("phase1", "metron_valuation_medians"): PhaseUnit("D11", "covered", "phase1"),
    # features/compute.py::compute_and_write already returns `tickers_computed`
    # (n_ok — the row count of the snapshot actually written; see the `result`
    # dict at the end of that function) on every non-error path. `alpha-engine-
    # config-I10810` deliverable 2 / I10785: this closes the D12/D31
    # UNMEASURABLE `data_empty_fresh` verdict — rejected tickers are reported
    # separately under `tickers_skipped` (empty featured_df) and
    # `tickers_errored` (compute raised), so they are declared here too rather
    # than left silent the way `rows_out: 0` alone would be.
    ("phase1", "features"): PhaseUnit(
        "D12",
        "tickers_computed",
        "phase1",
        rejected_keys=(("tickers_skipped", "empty_features"), ("tickers_errored", "compute_error")),
    ),
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
    ("daily", "prices"): PhaseUnit(
        "D03", "refreshed", "daily",
        rejected_keys=(("failed", "short_fetch_guard_refused"),),
    ),
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
    # Same producer as D12 above (features/compute.py::compute_and_write) —
    # `tickers_computed` closes the D31 UNMEASURABLE verdict the same way.
    ("daily", "features"): PhaseUnit(
        "D31",
        "tickers_computed",
        "daily",
        rejected_keys=(("tickers_skipped", "empty_features"), ("tickers_errored", "compute_error")),
    ),
    # builders/daily_append.py reports `tickers_published` (n_ok + n_partial —
    # every row actually written to ArcticDB this run, whether fully-featured
    # or carrying >=1 NaN feature) and names every class of ticker it did NOT
    # append. `tickers_appended` (n_ok alone) UNDERCOUNTS real writes — a row
    # with one NaN feature is still published, and on the 2026-09-16 D32 run
    # 909 of 910 published rows were exactly that shape, which read as
    # `rows_out: 0` before this fix (alpha-engine-config-I10810, measured
    # against the live `universe` library, not just the manifest).
    ("daily", "arcticdb"): PhaseUnit(
        "D32",
        "tickers_published",
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
    # `tickers_published` (n_ok + n_partial), not `tickers_appended` (n_ok
    # alone) — see the ("daily", "arcticdb") PhaseUnit above for why.
    "morning_enrich": ModeRows("arcticdb", "tickers_published", rejected_keys=_APPEND_REJECTED_KEYS),
    "morning_arctic_append": ModeRows("arcticdb", "tickers_published", rejected_keys=_APPEND_REJECTED_KEYS),
    "daily_arctic_append": ModeRows("arcticdb", "tickers_published", rejected_keys=_APPEND_REJECTED_KEYS),
    # The two heal units publish DAYS and TICKERS respectively, as lists.
    "daily_heal": ModeRows("universe_gap_heal", "healed_days", counts_list=True),
    "chronic_gap_heal": ModeRows("chronic_gap_self_heal", "healed", counts_list=True),
}

#: The :data:`NOT_APPLICABLE_REASONS` members this repo's whole-mode/phase
#: non-runs map onto (`alpha-engine-config-I10831` deliverable 1, landed as
#: `nousergon-lib` v0.124.133 / nousergon-lib-PR416). Each name is matched to
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


# ---------------------------------------------------------------------------
# A run that published NOTHING — the terminal state, and who may have it.
# ---------------------------------------------------------------------------
#
# `alpha-engine-config-I11011`. The 2026-09-15 shadow run recorded ten units
# (D20, D22-D30) whose manifests read `status: ok`, `rows_out: 0`,
# `outputs: []` — a zero-second run that produced nothing, filed as a success.
# Every surface that consumes the manifest counted it: the board's `run_record`
# column, the completeness clauses. The parity comparator was the only thing
# that noticed, and only because it went looking for outputs that were not
# there.
#
# **The rule.** `rows_out: 0` with an empty `outputs` array is not `ok`. A
# producer unit that completes having written nothing reaches one of two
# terminal states, never the third:
#
# * `not_applicable`, with a closed-list reason — when the unit DECLARES, in
#   its descriptor, that publishing nothing is a legitimate outcome for it (a
#   heal pass with nothing to heal; a weekend-scoped unit on a weekday). The
#   declaration names which `NOT_APPLICABLE_REASONS` member the empty run IS,
#   so the non-run is counted rather than waved through.
# * `failed` — for every unit that has NOT declared it. The fleet default is
#   RAISE (`~/Development/CLAUDE.md`, "fail loud and fast"), and this is the
#   recording-surface half of that rule: the raise happens INSIDE the manifest
#   wrapper, so the record says `failed`, and it is caught OUTSIDE by the
#   caller's existing sentinel handling, so no exit code moves. The manifest
#   becomes honest without the pipeline's posture changing.
#
# Empty-is-valid is never the DEFAULT reading of an empty result, which is the
# whole defect: today "the unit worked and had nothing to say" and "the unit
# silently did nothing" render identically, and so a fix to the second is
# unverifiable — success and silence look the same
# (`engagement-protocol-policy` §5: detection blindness outranks the defects it
# hides).

#: The descriptor block a unit uses to declare that publishing nothing is a
#: legitimate outcome for it. Absent — the default — means it is not.
EMPTY_IS_VALID_FIELD = "empty_is_valid"

#: The guard recorded on EVERY run that completed having published nothing,
#: declared or not. Named for the audit §4.1 class it answers
#: (`guards.success_without_output` in the descriptors): a unit reporting
#: success without producing output. Recorded on the declared path too — a
#: guard that records only when it fires is indistinguishable from a guard that
#: stopped running (`principles.md` §2.7).
EMPTY_PRODUCTION_GUARD = "data_success_without_output"


class EmptyProduction(Exception):
    """A producer unit completed having recorded no output at all.

    Raised INSIDE the unit body, so the manifest is written with
    ``status: failed`` and this as its reason; caught OUTSIDE the wrapper by the
    same sentinel idiom ``_DegradedRun`` / :class:`EntryRunFailed` already use,
    so the caller's return value and the process's exit-code contract are
    unchanged. The record becomes honest; the pipeline's posture does not move.
    """


@dataclass(frozen=True)
class EmptyDeclaration:
    """A unit's declaration that publishing nothing is legitimate FOR IT.

    Args:
        reason: Which :data:`nousergon_lib.run_manifest.NOT_APPLICABLE_REASONS`
            member an empty run of this unit IS. A member, not free text, for
            the same reason the lib closes that list: a free-text reason is how
            a unit quietly stops being graded.
        note: Why zero output is legitimate here, in words. Required — a
            declaration with no evidence behind it is a claim, and this one
            switches off a RAISE.
    """

    reason: str
    note: str


class EmptyDeclarationError(ValueError):
    """A descriptor's ``empty_is_valid`` block is malformed.

    Loud rather than ignored: a declaration that fails to parse would otherwise
    read as "not declared", which is the safe direction for THIS run and the
    wrong direction for the operator, who wrote a declaration and would never
    learn it does nothing.
    """


def empty_declaration(unit_raw: Mapping[str, Any]) -> EmptyDeclaration | None:
    """The unit's empty-is-valid declaration, or ``None`` when it has none.

    ``None`` is the default and the strict reading: a unit that has not
    declared zero output legitimate RAISES on one.
    """
    block = unit_raw.get(EMPTY_IS_VALID_FIELD)
    if block is None or block is False:
        return None
    if not isinstance(block, Mapping):
        raise EmptyDeclarationError(
            f"{EMPTY_IS_VALID_FIELD} must be a mapping with `reason` and `note`, got "
            f"{type(block).__name__}. A bare truthy value would switch off a RAISE without "
            "saying which not-applicable reason the empty run is, or why it is legitimate."
        )
    reason = str(block.get("reason") or "")
    if reason not in run_manifest.NOT_APPLICABLE_REASONS:
        raise EmptyDeclarationError(
            f"{EMPTY_IS_VALID_FIELD}.reason {reason!r} is not one of "
            f"{sorted(run_manifest.NOT_APPLICABLE_REASONS)}. The manifest of a declared-empty "
            "run carries this member verbatim, so a reason outside the closed list would "
            "write a record the schema refuses and nothing downstream counts."
        )
    note = " ".join(str(block.get("note") or "").split())
    if not note:
        raise EmptyDeclarationError(
            f"{EMPTY_IS_VALID_FIELD}.note is empty. The note is the evidence for a declaration "
            "that switches off the fleet's default RAISE; without it the board renders a claim."
        )
    return EmptyDeclaration(reason=reason, note=note)


def empty_declaration_for(unit_id: str) -> EmptyDeclaration | None:
    """:func:`empty_declaration` for the committed descriptor of ``unit_id``.

    The producer call sites know their unit id, not their descriptor. Read from
    ``registry.d/units/`` — the ONLY source of units (plan §4.1) — rather than a
    second list here, which is the drift this module's own header argues
    against. Cached: the descriptors are committed files that do not change
    inside a run.
    """
    return _empty_declarations().get(unit_id)


@functools.lru_cache(maxsize=1)
def _empty_declarations() -> dict[str, EmptyDeclaration]:
    from data_gate.descriptors import load_units  # local: keeps `yaml` off the CLI import path

    out: dict[str, EmptyDeclaration] = {}
    for unit in load_units():
        declared = empty_declaration(unit.raw)
        if declared is not None:
            out[unit.unit_id] = declared
    return out


def is_empty_success(manifest: Mapping[str, Any]) -> bool:
    """Did this manifest claim success while recording no output at all?

    THE predicate, read by the producer (against the run context, before the
    record is written) and by the board's ``run_record`` reader (against the
    record, after). One predicate, both sides, so a producer that stops
    enforcing it cannot also stop it being detected — the reader grades the
    PROPERTY, never the producer's own classification of it.
    """
    if str(manifest.get("status")) != "ok":
        return False
    return not (manifest.get("outputs") or []) and not int(manifest.get("rows_out") or 0)


def record_empty_production(run_ctx, unit_id: str, *, detail: str) -> None:
    """Record the terminal state of a run that published nothing. NEVER returns.

    Raises :class:`nousergon_lib.run_manifest.NotApplicable` when the unit
    declares empty-is-valid, and :class:`EmptyProduction` when it does not.
    Both write a manifest; neither writes ``ok``.
    """
    declared = empty_declaration_for(unit_id)
    detail = " ".join(str(detail).split())
    if declared is None:
        run_ctx.record_guard(
            EMPTY_PRODUCTION_GUARD,
            # `enforce`: the verdict had a consequence on this run — it is what
            # made the manifest `failed` rather than `ok`. The PROCESS posture
            # is unchanged (the raise is caught outside the wrapper), but the
            # record is not observe-only, and `mode` records consequence on the
            # record, not on the exit code.
            mode="enforce",
            # `empty_fresh` is the `data_run_manifest.v1` GuardVerdict for
            # "published an absent or empty artifact" — its own definition, and
            # the closest true member. The enum is closed in `nousergon-lib`.
            verdict="empty_fresh",
            detail=(
                f"{unit_id} completed having published NOTHING — no recorded output and "
                f"rows_out 0 — and its descriptor declares no `{EMPTY_IS_VALID_FIELD}`. "
                f"Recorded `failed`, never `ok`: a run that produced nothing and a run that "
                f"produced its deliverable must not render identically. {detail}"
            )[:2000],
        )
        raise EmptyProduction(
            f"{unit_id} published nothing on this run and does not declare "
            f"{EMPTY_IS_VALID_FIELD}: {detail}"
        )
    run_ctx.record_guard(
        EMPTY_PRODUCTION_GUARD,
        mode="observe",
        verdict="not_applicable",
        detail=(
            f"{unit_id} published nothing on this run, which its descriptor DECLARES "
            f"legitimate ({EMPTY_IS_VALID_FIELD}.reason={declared.reason}): {declared.note}. "
            f"{detail}"
        )[:2000],
    )
    raise run_manifest.NotApplicable(declared.reason, f"{unit_id}: {detail}")


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

    Four answers, in falling order of how specific a claim they make:

    1. ``NE_DATA_LOG_LOCATION`` — an explicit override from the launcher.
    2. ``ALPHA_ENGINE_RUN_LOG_S3`` — the S3 object this box's bootstrap wrapper
       is shipping the whole run log to (alpha-engine-config-I11353). Preferred
       over the CloudWatch group below because it is the RECORD: the stream is
       capped at ~1 MB and the object is not.
    3. The CloudWatch group, when the box declared itself but exported no log
       URI — a box running code from before I11353 landed.
    4. Off a box: host and pid. A true statement about where the logs are,
       rather than a CloudWatch group this run never wrote to. The daily report
       counts a ``local:`` manifest on a non-ok run as a DETECTION GAP row
       (`data_gate.report`), never as silence.

    Deliberately does NOT try to guess between ``<uri>`` and ``<uri>.gz``
    (`alpha-engine-config-I11359`, `krepis.spot_bootstrap.RunLog(gzip=True)`):
    this function runs ON THE BOX, called from inside the very process the
    bootstrap script's ``exec > >(tee …)`` wraps — strictly BEFORE the
    EXIT/TERM-trap that performs the plain-to-gz swap can fire, since that
    trap only runs after this Python process (and the shell around it) has
    already exited. A suffix check here would never observe ``.log.gz`` and
    would be dead code asserting a fact it can never be run early enough to
    know. The literal this returns is the key the launcher decided before the
    run started; resolving which suffix ACTUALLY exists once the run is over
    is a reader's question, answered by ``data_gate/report.py::_resolve_log``.
    """
    declared = os.environ.get(LOG_LOCATION_ENV)
    if declared:
        return declared
    shipped = os.environ.get(RUN_LOG_S3_ENV)
    if shipped:
        return shipped
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
    `alpha-engine-config-I9771`). This wrapper does not itself enforce that —
    D43's own entry points call :func:`require_in_region` before reaching here
    (`alpha-engine-config-I10790`) — but the manifest's `compute` row still
    records where the run actually happened, so a run that somehow reaches
    this wrapper off-box is visible after the fact too, not merely forbidden.
    """
    from dates import default_run_date  # local: keeps `dates` off the import path of CLIs that do not need it

    def _body(ctx):
        value = fn(ctx)
        if write and not ctx.outputs and not int(ctx.rows_out or 0):
            # `alpha-engine-config-I11011`. A repair run that wrote nothing is
            # the one this matters most for: it is invoked BECAUSE something is
            # broken, and "ran, repaired nothing, reported ok" is the shape that
            # closes an incident without fixing it. Deliberately NOT caught here
            # — an operator running a repair by hand is the one caller who
            # should see the raise.
            record_empty_production(
                ctx, unit_id, detail=f"manual/on-demand run of {unit_id} recorded no output"
            )
        return value

    return run_manifest.run_unit(
        unit_id,
        _body,
        sink=manifest_sink(bucket) if write else None,
        trigger=resolve_trigger(trigger),
        trading_day=trading_day or default_run_date(),
        log_location=resolve_log_location(),
    )


class NotInRegionError(RuntimeError):
    """A D43 manual-repair tool was invoked off the in-region box.

    D43 (`registry.d/units/D43-manual-repair-builders.yaml`) writes ArcticDB's
    ``universe`` / ``macro`` libraries directly, and ArcticDB is unreadable
    from the laptop: the ``alpha-engine-data`` bucket carries an explicit Deny
    that blocks even ``ne-admin`` on ``ListObjectsV2`` / ``GetBucketPolicy``
    (measured 2026-09-01, `alpha-engine-config-I9771`). A laptop invocation
    would otherwise fail deep inside the repair with an opaque boto3
    AccessDenied, after having already done some of its work — this refuses
    up front instead.
    """


def require_in_region(tool: str) -> None:
    """Refuse to run ``tool`` unless the box declared ``NE_DATA_INSTANCE_TYPE``.

    `alpha-engine-config-I9771` / `-I10790`: D43's four manual repair CLIs run
    in-region only. ``NE_DATA_INSTANCE_TYPE`` is the same box-declared signal
    :func:`nousergon_lib.run_manifest._resolve_compute` already reads for the
    manifest's ``compute`` row (also read by :func:`resolve_log_location`
    above) — its absence means "not on a declared in-region box," which is
    reused here rather than adding a second identity check.

    Call this BEFORE any ArcticDB read/write or :func:`manual_run` dispatch,
    so an off-box invocation refuses before touching production data — not
    merely after it, via the manifest's ``compute`` row.
    """
    if not os.environ.get("NE_DATA_INSTANCE_TYPE"):
        raise NotInRegionError(
            f"{tool}: refusing to run off the in-region box — "
            "NE_DATA_INSTANCE_TYPE is not set. ArcticDB is unreadable from the "
            "laptop or CI (alpha-engine-config-I9771); run this on the "
            "data-spot/EOD box, in-region."
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

#: `nousergon_lib.run_manifest.run_unit` (>= v0.124.150) bounds the final
#: `reason` field to `REASON_MAX_LEN = 2000` too, and — since I11358 — no
#: longer cuts head-only or silently: it keeps BOTH ends (3:5 head:tail, same
#: split this module uses) behind an explicit ` …[reason_truncated: N
#: bytes]… ` marker. So the library layer losing the tail is no longer the
#: reason this stays under 2,000 (alpha-engine-config-I11383: that reason is
#: now false and the comment it replaced said so). The reason that IS true at
#: this commit: `run_manifest.run_unit` wraps whatever this module returns as
#: `f"{type(exc).__name__}: {exc}"` before its own cut ever runs — the
#: exception class name plus `": "` is instrumentation overhead this layer
#: does not control and did not budget for. 1,800 leaves ~200 chars of
#: headroom for that wrapper (`test_the_budget_stays_under_the_librarys_own_cut`
#: asserts the arithmetic), which keeps this layer's own head:tail split as
#: the ONE truncation that fires in the normal case — a reason built here at
#: 1,800 chars never reaches the library's 2,000-char cut at all. Raising the
#: cap now that the library preserves tails would buy nothing: any content
#: past ~1,970 chars total (post-wrapper) gets re-cut by the library anyway,
#: just wearing a second, nested marker instead of a cleaner single one. 1,800
#: stays the right number for a different reason than the one that used to be
#: written here.
REASON_MAX_LEN = 1800

#: How a truncated reason is SPLIT (alpha-engine-config-I11353). Head-only was
#: the 2026-09-21 defect: `collectors/daily_closes.py::collect` scans a window
#: and the TARGET date — the one that failed — is the LAST entry in the result,
#: so a head cut drops precisely the cause and keeps ten `ok` dates nobody
#: needed. 3:5 head:tail keeps enough of the header to identify the mode and
#: enough of the tail to carry the failing entry. `nousergon-lib` >= v0.124.150
#: adopted the same 3:5 split for its own cut, on second adoption
#: (`policy-shared-code`) — see the note on `REASON_MAX_LEN` above for how the
#: two layers now interact.
#:
#: The 1,800-char ceiling above is NOT raised to the 4,000 the issue proposes,
#: and that is deliberate rather than a shortfall: `run_unit` cuts the final
#: string at 2,000 chars, HEAD-ONLY and with no marker, so a 4,000-char
#: head+tail reason would be re-truncated by the library back to its head and
#: lose the tail this change exists to keep — reintroducing the same defect one
#: layer down, silently. Lifting the library's cut to the same head+tail shape
#: is filed separately; until it lands, 1,800 is the largest budget this layer
#: can actually deliver.
REASON_HEAD_RATIO = 3
REASON_TAIL_RATIO = 5


def elide_bulk(value: Any, max_items: int = 20, _depth: int = 0) -> Any:
    """Recursively replace an over-long list/tuple with a count placeholder.

    `alpha-engine-config-I10941`. A collector result dict is free-form and one
    of its fields can legitimately be a per-ticker array — `tickers`,
    `symbols`, `articles` — with no upper bound the schema enforces. Nothing
    that renders a result dict into a bounded string (a manifest `reason`, a
    log line) may `repr()` it raw, or that array is what eats the budget
    before the actual cause is reached. Depth-limited defensively against a
    pathological/cyclic-looking structure; this is a diagnostic renderer, not
    a general serializer, so it never raises.
    """
    if _depth > 6:
        return "<max depth reached>"
    if isinstance(value, dict):
        return {k: elide_bulk(v, max_items, _depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        if len(value) > max_items:
            kind = "tuple" if isinstance(value, tuple) else "list"
            return f"<{kind}: {len(value)} items elided>"
        return [elide_bulk(v, max_items, _depth + 1) for v in value]
    return value


def describe_mode_failure(mode: str, result: dict, *, max_len: int = REASON_MAX_LEN) -> str:
    """Build a bounded, diagnosable failure reason for a mode/aggregate result
    that reported a non-``ok`` status.

    `alpha-engine-config-I10941`. The 2026-09-16 shadow failure's manifest
    `reason` was `_CollectorError: morning_enrich: morning_enrich returned
    status='failed': {...}` with the WHOLE result dict f-strung in —
    `constituents_preflight`'s 903-ticker `tickers` array ate the entire
    field, and the sub-collector that actually failed never appeared. This
    replaces that raw f-string at every call site of the same shape
    (`weekly_collector.py::_run_whole_mode_unit`,
    `collectors/daily_news.py`, `collectors/metron_market_data.py`).

    The failing sub-collector — the first entry under ``result["collectors"]``
    whose own ``status`` is not ok/skipped/not_applicable — is named FIRST, so
    it lands in the field's first 200 characters before anything
    variable-length. When no such sub-collector is present (a flat result,
    e.g. `daily_news`), the mode's own status stands in for it. The full
    result then follows through :func:`elide_bulk`, never raw, and the whole
    string is hard-capped at ``max_len`` with an explicit ``reason_truncated``
    marker appended when even the elided rendering does not fit — so a future
    truncation is visible as truncation, not a sentence that happens to stop.
    """
    result = result or {}
    status = result.get("status")
    collectors = result.get("collectors") if isinstance(result.get("collectors"), dict) else {}
    failing = [
        (name, sub)
        for name, sub in collectors.items()
        if isinstance(sub, dict)
        and sub.get("status") not in ("ok", "ok_dry_run", "skipped", "not_applicable", None)
    ]
    if failing:
        fname, fsub = failing[0]
        header = (
            f"{mode}: failing_collector={fname!r} "
            f"status={fsub.get('status')!r} error={fsub.get('error') or fsub.get('detail')!r}"
        )
    else:
        header = f"{mode} returned status={status!r} (no sub-collector reported non-ok)"
    reason = f"{header} | full_result(elided)={elide_bulk(result)!r}"
    return truncate_reason(reason, max_len=max_len)


def truncate_reason(reason: str, *, max_len: int = REASON_MAX_LEN) -> str:
    """Bound ``reason`` to ``max_len``, keeping BOTH ends.

    `alpha-engine-config-I11353`. The previous form kept the head alone, and
    the 2026-09-21 `morning_daily_closes` manifest is what that costs: the
    window scan renders 2026-09-08…09-18 as `ok` first and the failing target
    date LAST, so the cut landed before the only entry anyone was reading the
    field for. Whatever a producer puts first, the thing that went wrong is
    usually what it says last.

    The marker names the exact number of characters removed
    (``reason_truncated_bytes``) rather than only that a cut happened, so a
    reader can tell "two lines elided" from "this field is 4% of the story".
    """
    if len(reason) <= max_len:
        return reason
    total = REASON_HEAD_RATIO + REASON_TAIL_RATIO
    # Reserve the marker's own length first, measured rather than guessed: the
    # dropped count appears INSIDE the marker, so a fixed reserve is wrong by
    # however many digits it has.
    probe = (
        f" …[reason_truncated: true, reason_truncated_bytes: {len(reason)}, "
        f"full length {len(reason)} chars]… "
    )
    budget = max(0, max_len - len(probe))
    head_len = budget * REASON_HEAD_RATIO // total
    tail_len = budget - head_len
    head = reason[:head_len]
    tail = reason[len(reason) - tail_len :] if tail_len else ""
    dropped = len(reason) - head_len - tail_len
    marker = (
        f" …[reason_truncated: true, reason_truncated_bytes: {dropped}, "
        f"full length {len(reason)} chars]… "
    )
    return f"{head}{marker}{tail}"[:max_len]


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

    captured: dict[str, Any] = {}

    def _checked(ctx) -> Any:
        captured["value"] = value = body(ctx)
        if write and not ctx.outputs and not int(ctx.rows_out or 0):
            # `alpha-engine-config-I11011`: `rows_out: 0` with an empty
            # `outputs` array is not `ok`. Raised from INSIDE the wrapper so the
            # manifest records the honest terminal state, and caught below so
            # the entry point's own exit-code contract is exactly what it was —
            # the same separation `EntryRunFailed` above makes, for the same
            # reason.
            record_empty_production(
                ctx, unit_id, detail=f"{unit_id} completed without recording any output"
            )
        return value

    try:
        run_result = run_manifest.run_unit(
            unit_id,
            _checked,
            sink=manifest_sink(bucket, s3_client) if write else None,
            trigger=resolve_trigger(trigger),
            trading_day=trading_day,
            log_location=resolve_log_location(),
            code_sha=code_sha,
        )
        # `run_unit` does not re-raise `NotApplicable` — it records the declared
        # non-production and returns `value=None`. The caller's contract is the
        # body's own return value, so the captured one is returned rather than
        # the wrapper's None.
        if run_result.status == "not_applicable":
            return captured.get("value")
        return run_result.value
    except EmptyProduction:
        # The manifest is already durable with `status: failed` and the empty
        # production as its reason.
        return captured.get("value")
    except EntryRunFailed as failed:
        # The manifest is already written with `status: failed`. The caller
        # keeps the value it would have received before this wrapper existed.
        return failed.value
