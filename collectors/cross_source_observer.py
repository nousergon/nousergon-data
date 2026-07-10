"""collectors/cross_source_observer.py — L1 observer-mode annotation (config#1277 Option A).

Operator ruling 2026-07-08 (Option A, console Decision Queue) for the market-value
integrity framework (alpha-engine-config#1277, L1): wire the independent
cross-source agreement gate (``sources/cross_source_gate.py``) into daily-closes
ingestion in **observer mode** —

  * additively record each settled cell's cross-source status + provenance in the
    output parquet, WITHOUT changing which value the existing source-priority
    coalesce chose (``collectors.daily_closes._coalesce_by_source_priority``);
  * single-source-per-mode cells record ``SINGLE_SOURCE_PROVISIONAL`` (the honest
    "not cross-checked" classification — the collector is single-source-per-mode by
    design so the primary/only value each cell carries has no in-hand second
    witness: config#717/#720 bound the morning polygon pass to the free tier and
    ``auto`` uses yfinance only as a gap-fill fallback);
  * a **bounded** high-value set gets a real second-source cross-check
    (``AGREED`` / ``QUARANTINED``), kept tiny so the extra fetch never threatens the
    free-tier bound that made ingestion single-source in the first place;
  * quarantines are surfaced loudly for L4 / paging — the value is NOT withheld in
    observer mode (priority-coalesce still owns the number), only flagged.

The recorded disagreement/quarantine rate then sizes the cost of full L1
enforcement (the deferred Option B/C decision).

STRICTLY ADDITIVE + FAIL-SOFT: this module never mutates a record's ``Close`` or
``source`` and never raises into the ingestion path — an observer failure degrades
to un-annotated rows, never a blocked or corrupted write. It is deliberately NOT
load-bearing: nothing downstream may depend on its columns for a value, only for
provenance/observability.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional

# ``sources.cross_source_gate`` is imported LAZILY (inside the functions that use
# it), NOT at module top level. This module lives under ``collectors/`` which is
# COPY'd into the Phase-2 Lambda image, but ``sources/`` is deliberately NOT in
# that image (its adapter stack — arcticdb/polygon/fred/yfinance — is spot-only)
# and ``sources/__init__`` also imports ``collectors.daily_closes`` (circular).
# A top-level ``from sources...`` here would (a) trip the Dockerfile-copy canary
# (tests/test_dockerfile_copies_match_deployed_imports.py — the PR#254 failure
# class) and (b) reintroduce the circular import ``daily_closes`` already dodges
# with call-time imports. TYPE_CHECKING-only import keeps annotations resolvable
# for tooling without any runtime/load-time cost.
if TYPE_CHECKING:  # pragma: no cover — annotations only
    from sources.cross_source_gate import GateDecision

logger = logging.getLogger(__name__)

# Additive parquet columns this observer writes onto every annotated record.
# Nullable by construction — a legacy reader (or a row the observer could not
# classify) simply sees them absent/NaN and is unaffected.
XSOURCE_STATUS = "xsource_status"                # GateStatus value
XSOURCE_FLAGGED = "xsource_flagged"              # bool: True unless a clean >=2-source agreement
XSOURCE_AGREEMENT_BPS = "xsource_agreement_bps"  # observed max pairwise spread (float) or None
XSOURCE_PROVENANCE = "xsource_provenance"        # compact human-readable audit string (L4)
XSOURCE_COLUMNS = (
    XSOURCE_STATUS,
    XSOURCE_FLAGGED,
    XSOURCE_AGREEMENT_BPS,
    XSOURCE_PROVENANCE,
)

# Default bounded high-value cross-check set: the config#1276 artifact (EOD SPY) —
# the single most-important settled close a human acts on. Kept tiny on purpose so
# the extra second-source fetch stays "cheap" per the Option A ruling and never
# reintroduces the free-tier cost the single-source-per-mode design avoids.
DEFAULT_CROSS_CHECK_TICKERS: tuple[str, ...] = ("SPY",)


def _decision_to_fields(dec: GateDecision) -> dict:
    """Project a :class:`GateDecision` onto the additive observer columns."""
    return {
        XSOURCE_STATUS: dec.status.value,
        XSOURCE_FLAGGED: bool(dec.flagged),
        XSOURCE_AGREEMENT_BPS: dec.agreement_bps,
        XSOURCE_PROVENANCE: dec.provenance,
    }


def _single_source_decision(
    ticker: str, date: str, source: Optional[str], close, tolerance_bps: float
) -> GateDecision:
    """Classify a single-source-per-cell value with the pure L1 gate.

    Exactly one usable source -> ``SINGLE_SOURCE_PROVISIONAL``; a null/absent close
    -> ``NO_DATA``. Uses the same institutional ``evaluate`` logic as the real
    cross-check so the recorded status vocabulary is identical across both paths.
    """
    from sources.cross_source_gate import SourceClose, evaluate  # lazy: see module top

    price = None
    try:
        if close is not None:
            price = float(close)
    except (TypeError, ValueError):
        price = None
    return evaluate(
        ticker,
        date,
        [SourceClose(source=source or "unknown", price=price)],
        tolerance_bps=tolerance_bps,
    )


def annotate_records(
    records: list[dict],
    run_date: str,
    *,
    source_mode: str = "auto",
    cross_check_tickers: tuple[str, ...] = DEFAULT_CROSS_CHECK_TICKERS,
    cross_check_fetch: Optional[Callable[[str, str], "GateDecision"]] = None,
    tolerance_bps: Optional[float] = None,
) -> tuple[list[dict], dict]:
    """Annotate ingestion ``records`` with L1 observer-mode cross-source status.

    Mutates each record dict IN PLACE, adding the :data:`XSOURCE_COLUMNS` keys.
    ``Close``/``source`` are never touched. Never raises — any per-record error
    degrades that row to un-annotated and is counted in the summary.

    Parameters
    ----------
    records:
        The finalized this-run records (post source-priority coalesce), each a
        dict with at least ``ticker`` and (usually) ``Close`` + ``source``.
    run_date:
        The settled date string (``YYYY-MM-DD``) these closes are for.
    source_mode:
        The collector source mode (``auto`` / ``polygon_only`` / ``yfinance_only``)
        — recorded in the summary for context; classification is per-cell.
    cross_check_tickers:
        The bounded set that gets a real second-source fetch (default SPY).
    cross_check_fetch:
        ``(ticker, date) -> GateDecision`` performing the independent two-source
        fetch + gate (e.g. ``sources.cross_source_gate.gate_settled_close``). When
        ``None`` (e.g. dry-run / no network), the bounded set is classified as
        single-source like every other cell — no network is touched.
    tolerance_bps:
        Agreement tolerance for the single-source classifier (the real cross-check
        carries its own tolerance inside ``cross_check_fetch``). ``None`` (default)
        resolves to ``sources.cross_source_gate.DEFAULT_TOLERANCE_BPS`` at call
        time — the lazy resolution keeps ``sources`` out of this module's
        top-level imports (see module docstring).

    Returns
    -------
    ``(records, summary)`` where ``summary`` has ``status_counts`` (per-status
    tally), ``quarantined`` (list of quarantine dicts for L4/paging),
    ``cross_checked`` (count that got a real 2nd-source fetch), ``annotated``,
    ``errors``, and ``source_mode``.
    """
    from sources.cross_source_gate import (  # lazy: see module top
        DEFAULT_TOLERANCE_BPS,
        GateStatus,
    )

    if tolerance_bps is None:
        tolerance_bps = DEFAULT_TOLERANCE_BPS
    check_set = {t.lstrip("^") for t in (cross_check_tickers or ())}
    status_counts: dict[str, int] = {}
    quarantined: list[dict] = []
    cross_checked = 0
    annotated = 0
    errors = 0

    for rec in records:
        ticker = rec.get("ticker")
        if not ticker:
            continue
        try:
            in_bounded_set = ticker.lstrip("^") in check_set
            if in_bounded_set and cross_check_fetch is not None:
                # Real independent second-source fetch + gate (fail-soft inside
                # the callback). Observer-only: we record the decision but do NOT
                # override the priority-coalesced value on the record.
                dec = cross_check_fetch(ticker, run_date)
                cross_checked += 1
                if dec.status is GateStatus.QUARANTINED and dec.discrepancy:
                    quarantined.append(dec.discrepancy)
            else:
                dec = _single_source_decision(
                    ticker, run_date, rec.get("source"), rec.get("Close"), tolerance_bps
                )
            rec.update(_decision_to_fields(dec))
            status_counts[dec.status.value] = status_counts.get(dec.status.value, 0) + 1
            annotated += 1
        except Exception as exc:  # never let observation break ingestion
            errors += 1
            logger.warning(
                "L1 OBSERVER: could not annotate %s @ %s (%s) — leaving row "
                "un-annotated",
                ticker, run_date, exc,
            )

    summary = {
        "source_mode": source_mode,
        "status_counts": status_counts,
        "quarantined": quarantined,
        "cross_checked": cross_checked,
        "annotated": annotated,
        "errors": errors,
    }
    return records, summary
