"""corporate_actions — unified corporate-action model + S3 registry.

WHY THIS EXISTS (unified corporate-actions program, config#1431):
    Corporate actions (splits, dividends, renames) retroactively restate a
    ticker's adjusted price history. The data pipeline already DETECTS splits
    (``polygon_client.get_recent_splits`` + ``split_factor.py``) and RESTATES
    them into the ArcticDB universe (data#1298), but it had no first-class,
    auditable *model* of a corporate action and no durable record of which
    actions were detected/applied. That gap produced two symptoms:

      1. A confirmed split's expected adjusted-close restatement tripped the
         daily-closes ">5% cross-source drift" ERROR band (the HON 1-for-2
         reverse split surfaced as a false flow-doctor ERROR — exactly the
         class of false alarm the ``_split_ratio_hint`` text band-aided but
         could not authoritatively suppress).
      2. No write-once provenance trail for "we detected action X / we applied
         action X to store Y", which a robust restatement path needs to stay
         idempotent across reruns.

    This module is the FOUNDATION layer of the program (PR1+PR2): a frozen
    ``CorporateAction`` dataclass with a deterministic ``action_id``, a
    polygon-backed ``detect_splits`` built ON ``split_factor.py`` (the
    authoritative factor convention), and a ``CorporateActionRegistry`` (S3
    JSON) that makes the split-restatement discrepancy classification
    *registry-authoritative* rather than text-heuristic.

    This PR is behavior-ADDITIVE: it does NOT change the existing
    split-RESTATEMENT path (``split_factor.restate_series_for_splits`` /
    ArcticDB write) — that is a later PR. It only adds the module + registry
    and RECLASSIFIES the discrepancy log (confirmed split -> WARN, not ERROR)
    plus one informational notification.

CONVENTION (inherited from ``split_factor.py``):
    A forward N-for-1 split (``split_from=1, split_to=N``) divides the adjusted
    price by N for every date STRICTLY BEFORE the ex (execution) date. A reverse
    1-for-N split (``split_from=N, split_to=1``) multiplies by N. The per-event
    multiplicative factor applied to dates strictly before ``ex_date`` is
    therefore ``split_from / split_to`` — and we delegate to
    ``split_factor.cumulative_factor`` so this module never re-derives the
    authoritative factor independently.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

import pandas as pd

import split_factor
from corporate_actions.registry import CorporateActionRegistry

log = logging.getLogger(__name__)

__all__ = [
    "CorporateAction",
    "CorporateActionRegistry",
    "CorporateActionAuditError",
    "expected_factor",
    "apply",
    "detect_splits",
    "detect_dividends",
    "detect_renames",
    "splits_from_events",
]

# Action types implemented this PR. Dividends/renames are modeled (fields exist)
# but their detection/factor are deferred to later PRs of the program.
_TYPE_SPLIT = "split"
_TYPE_DIVIDEND = "dividend"
_TYPE_RENAME = "rename"

# The logical store a restatement targets. Both the Saturday full backfill
# (rebuilds from the S3 price cache) and the daily feature-snapshot delta
# (reads the already-restated ArcticDB) write into / read from the SAME
# logical store, so the applied-marker namespace is shared — that is exactly
# what makes ``apply`` exactly-once across the two paths (the daily snapshot
# load of an already-restated series sees the backfill's applied marker and
# skips, the double-apply guard of PR3 §4).
STORE_ARCTICDB_UNIVERSE = "arcticdb_universe"


class CorporateActionAuditError(RuntimeError):
    """A KNOWN, registered corporate action was left un-flattened in a series
    about to be written to a training store.

    This is the BLOCKING half of the registry-aware post-condition audit
    (``audit_action_jumps`` → backfill chokepoint, PR3 §3): a residual
    split-magnitude jump that a registered action *explains* (the action's
    ex_date sits at the jump and the move matches its factor) means the
    restatement of a known action was MISSED — the data#1298 corruption class.
    It is raised so the ArcticDB ``lib.write`` cannot land the discontinuity
    silently. It is DISTINCT from a *suspected* residual — a large move with NO
    registered action explaining it (a legitimate earnings move, or a
    polygon-missed action) — which is a WARN, never a raise, so a real ±33%
    move does not halt the pipeline.
    """


@dataclass(frozen=True)
class CorporateAction:
    """An immutable, content-addressed corporate-action record.

    ``action_id`` is a DETERMINISTIC content hash (so the same real-world
    action always maps to the same id across detections/reruns — the property
    the write-once registry relies on for idempotency). It is computed in
    ``__post_init__`` from ``(type, ticker, ex_date, detail)`` when left blank,
    so any construction path (direct, ``from_split``, ``from_dict``) yields the
    canonical id without the caller having to compute it.
    """

    type: str  # "split" | "dividend" | "rename"
    ticker: str
    ex_date: str  # YYYY-MM-DD; the adjustment applies to rows STRICTLY BEFORE this
    # split fields
    split_from: int | None = None
    split_to: int | None = None
    # dividend fields (modeled now, detected in a later PR)
    cash_amount: float | None = None
    dividend_kind: str | None = None
    # rename fields (modeled now, detected in a later PR)
    old_ticker: str | None = None
    new_ticker: str | None = None
    # provenance
    source: str = "polygon"
    raw: dict = field(default_factory=dict)
    # content-addressed id — auto-derived in __post_init__ when blank
    action_id: str = ""

    def __post_init__(self) -> None:
        if not self.action_id:
            object.__setattr__(self, "action_id", self._compute_action_id())

    # ── id derivation ────────────────────────────────────────────────────
    def _detail(self) -> str:
        """The type-specific discriminator folded into ``action_id``."""
        if self.type == _TYPE_SPLIT:
            return f"{self.split_from}:{self.split_to}"
        if self.type == _TYPE_DIVIDEND:
            return f"{self.cash_amount}:{self.dividend_kind}"
        if self.type == _TYPE_RENAME:
            return f"{self.old_ticker}->{self.new_ticker}"
        return ""

    def _compute_action_id(self) -> str:
        payload = f"{self.type}|{self.ticker}|{self.ex_date}|{self._detail()}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

    # ── factories ────────────────────────────────────────────────────────
    @classmethod
    def from_split(
        cls,
        ticker: str,
        ex_date: str,
        split_from: int,
        split_to: int,
        *,
        source: str = "polygon",
        raw: dict | None = None,
    ) -> "CorporateAction":
        """Build a split action; ``action_id`` is derived deterministically."""
        return cls(
            type=_TYPE_SPLIT,
            ticker=str(ticker),
            ex_date=str(ex_date),
            split_from=int(split_from),
            split_to=int(split_to),
            source=source,
            raw=dict(raw or {}),
        )

    # ── (de)serialization ────────────────────────────────────────────────
    def to_dict(self) -> dict:
        """JSON-serializable view (the registry persists this + audit fields)."""
        return {
            "action_id": self.action_id,
            "type": self.type,
            "ticker": self.ticker,
            "ex_date": self.ex_date,
            "split_from": self.split_from,
            "split_to": self.split_to,
            "cash_amount": self.cash_amount,
            "dividend_kind": self.dividend_kind,
            "old_ticker": self.old_ticker,
            "new_ticker": self.new_ticker,
            "source": self.source,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CorporateAction":
        """Reconstruct from a persisted dict (ignores extra audit keys like
        ``detected_at`` / ``detected_run_id``)."""
        return cls(
            type=d["type"],
            ticker=d["ticker"],
            ex_date=d["ex_date"],
            split_from=d.get("split_from"),
            split_to=d.get("split_to"),
            cash_amount=d.get("cash_amount"),
            dividend_kind=d.get("dividend_kind"),
            old_ticker=d.get("old_ticker"),
            new_ticker=d.get("new_ticker"),
            source=d.get("source", "polygon"),
            raw=d.get("raw") or {},
            # Trust the persisted id if present (it is content-addressed, so it
            # round-trips), else __post_init__ recomputes the identical value.
            action_id=d.get("action_id", ""),
        )

    # ── human description ────────────────────────────────────────────────
    def human(self) -> str:
        """A human-readable one-liner, e.g. '1-for-2 reverse split'."""
        if self.type == _TYPE_SPLIT and self.split_from and self.split_to:
            if self.split_to >= self.split_from:
                # forward N-for-1 (split_from=1, split_to=N)
                n = self.split_to // self.split_from
                return f"{n}-for-1 forward split"
            # reverse 1-for-N (split_from=N, split_to=1)
            n = self.split_from // self.split_to
            return f"1-for-{n} reverse split"
        if self.type == _TYPE_DIVIDEND:
            return f"dividend {self.cash_amount} ({self.dividend_kind})"
        if self.type == _TYPE_RENAME:
            return f"rename {self.old_ticker} -> {self.new_ticker}"
        return self.type


def expected_factor(action: CorporateAction) -> float:
    """The multiplicative factor applied to the adjusted CLOSE of rows STRICTLY
    BEFORE ``action.ex_date`` once the action is reflected in the adjusted
    history.

    For a split this is ``split_from / split_to`` — a reverse 1-for-2 split
    (``split_from=2, split_to=1``) yields ``2.0`` (pre-split prices double on
    the post-split adjusted scale); a forward 10-for-1 split
    (``split_from=1, split_to=10``) yields ``0.1`` (pre-split prices are divided
    by 10). We DELEGATE to ``split_factor.cumulative_factor`` (the authoritative
    convention) rather than re-deriving the ratio independently — the cumulative
    factor of a single event evaluated one day before the ex date is exactly
    ``split_from/split_to``.

    Dividends/renames are not implemented this PR.
    """
    if action.type == _TYPE_SPLIT:
        if not action.split_from or not action.split_to:
            raise ValueError(
                f"split action {action.action_id} missing split_from/split_to"
            )
        ev = {
            "execution_date": action.ex_date,
            "split_from": action.split_from,
            "split_to": action.split_to,
        }
        day_before = (
            pd.Timestamp(action.ex_date) - pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        return split_factor.cumulative_factor([ev], day_before)
    # TODO(corporate-actions program): implement dividend / rename expected
    # factors in a later PR. Fail loud rather than silently returning 1.0.
    raise NotImplementedError(
        f"expected_factor not implemented for action type {action.type!r} "
        "(only splits implemented this PR)"
    )


def apply(
    df: pd.DataFrame,
    actions: list["CorporateAction"],
    *,
    store: str,
    registry: "CorporateActionRegistry | None" = None,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """Restate a SINGLE ticker's price frame so its full history is corporate-
    action-consistent, routing all split restatement through ``split_factor``.

    ``df`` is one ticker's OHLCV(+) frame (DatetimeIndex). ``actions`` are the
    ``CorporateAction``s that pertain to THAT frame's ticker (the caller filters
    by ticker — this PR only restates SPLIT actions; a dividend/rename action
    passed in raises ``NotImplementedError`` because their factor math ships in a
    later PR). The full-history multiplicative restatement itself is delegated to
    :func:`split_factor.restate_series_for_splits` (price ×factor, volume ÷factor
    for every row strictly before each split's ex_date) — this function never
    re-derives the factor convention.

    Idempotency — registry-backed, DECOUPLED from source purity
    -----------------------------------------------------------
    ``restate_series_for_splits`` is NOT idempotent on its own (it always applies
    the FULL cumulative factor), so re-applying it to an already-restated series
    double-adjusts. Two layers guard against that:

      * When a ``registry`` is supplied, an action already marked applied to
        ``store`` (``registry.is_applied``) is SKIPPED (``status="noop"``) — this
        is the exactly-once marker that makes the daily feature-snapshot path
        (which reads the already-restated ArcticDB universe) a no-op rather than
        a double-apply (PR3 §4), and that survives re-runs durably via S3.
        After a real restatement the action is ``registry.mark_applied``-ed.
      * When ``registry is None`` (direct unit tests / dry-run), there is no
        durable marker — idempotency is then purely STRUCTURAL: the caller is
        expected to restate from the raw/un-restated source each time, and
        ``restate_series_for_splits`` yields the same result for the same raw
        input (re-running it on its OWN output would double-adjust, which is why
        the registry markers exist for the production reruns).

    The BLOCKING ``audit_action_jumps`` post-condition (PR3 §3) is the
    correctness backstop on top of both: any residual / double-applied
    discontinuity at a registered action's ex_date is surfaced (and, at the
    training-write chokepoint, RAISED) rather than landing silently.

    Returns ``(restated_df, applied_results)`` where each ``applied_result`` is
    ``{"action_id", "store", "n_rows_adjusted", "factor", "status"}`` and
    ``status`` is ``"applied"`` (restated this call) or ``"noop"`` (already
    applied per the registry — not re-adjusted).
    """
    applied_results: list[dict] = []
    if df is None or getattr(df, "empty", True) or not actions:
        return df, applied_results

    # Only splits are restated this PR. A dividend/rename action reaching here
    # is a caller error (detect_dividends/detect_renames are NotImplemented), so
    # fail loud rather than silently dropping it.
    split_actions: list[CorporateAction] = []
    for a in actions:
        if a.type == _TYPE_SPLIT:
            split_actions.append(a)
        elif a.type in (_TYPE_DIVIDEND, _TYPE_RENAME):
            raise NotImplementedError(
                f"corporate_actions.apply: {a.type!r} actions are deferred to a "
                "later PR of the program (splits only this PR)"
            )
        else:
            raise ValueError(
                f"corporate_actions.apply: unknown action type {a.type!r}"
            )

    if not split_actions:
        return df, applied_results

    idx = df.index if isinstance(df.index, pd.DatetimeIndex) else pd.to_datetime(df.index)

    events_to_apply: list[CorporateAction] = []
    for a in split_actions:
        factor = expected_factor(a)  # == split_from / split_to (authoritative)
        # Exactly-once: skip an action already folded into this store.
        if registry is not None and registry.is_applied(store, a.action_id):
            applied_results.append({
                "action_id": a.action_id,
                "store": store,
                "n_rows_adjusted": 0,
                "factor": factor,
                "status": "noop",
            })
            continue
        events_to_apply.append(a)

    if not events_to_apply:
        return df, applied_results

    events = [
        {
            "execution_date": a.ex_date,
            "split_from": a.split_from,
            "split_to": a.split_to,
        }
        for a in events_to_apply
    ]
    restated = split_factor.restate_series_for_splits(df, events)

    for a in events_to_apply:
        ex = pd.Timestamp(a.ex_date).normalize()
        n_rows_adjusted = int((idx < ex).sum())
        applied_results.append({
            "action_id": a.action_id,
            "store": store,
            "n_rows_adjusted": n_rows_adjusted,
            "factor": expected_factor(a),
            "status": "applied",
        })
        if registry is not None:
            registry.mark_applied(a, store, run_id=run_id)

    return restated, applied_results


def splits_from_events(events: list[dict]) -> list["CorporateAction"]:
    """Map polygon ``get_recent_splits`` event dicts to ``CorporateAction``s.

    Each event is ``{"ticker", "execution_date", "split_from", "split_to"}``.
    Pure transform (no I/O) so callers that already fetched the events (the
    daily-closes window scan) can reuse them WITHOUT a second polygon call.
    Malformed rows are skipped.
    """
    actions: list[CorporateAction] = []
    for ev in events or []:
        ticker = ev.get("ticker")
        ex_date = ev.get("execution_date")
        sf = ev.get("split_from")
        st = ev.get("split_to")
        if not ticker or not ex_date or not sf or not st:
            continue
        actions.append(
            CorporateAction.from_split(
                ticker=str(ticker),
                ex_date=str(ex_date),
                split_from=int(sf),
                split_to=int(st),
                source="polygon",
                raw=dict(ev),
            )
        )
    return actions


def detect_splits(
    start_date: str,
    end_date: str,
    *,
    client=None,
) -> list["CorporateAction"]:
    """Detect all splits executing in ``[start_date, end_date]`` as
    ``CorporateAction``s (type="split", ex_date = polygon execution_date).

    Wraps ``polygon_client.get_recent_splits`` (the whole-market, one-call
    range scan). The client is constructed lazily the same way
    ``collectors/daily_closes._fetch_recent_split_dates`` does, and any
    construction/fetch failure DEGRADES GRACEFULLY to ``[]`` (with the polygon
    apiKey scrubbed from the log) — a corporate-action detection miss must
    never hard-fail the data pipeline; the per-fetch discrepancy logging and
    the next pass remain the backstop.
    """
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # import / construction failure — degrade
            from polygon_client import _scrub_api_key

            log.warning(
                "corporate_actions.detect_splits: could not obtain polygon "
                "client (%s) — returning no detected splits",
                _scrub_api_key(exc),
            )
            return []
    try:
        events = client.get_recent_splits(start_date, end_date)
    except Exception as exc:
        from polygon_client import _scrub_api_key

        log.warning(
            "corporate_actions.detect_splits: polygon split scan failed (%s) "
            "— returning no detected splits",
            _scrub_api_key(exc),
        )
        return []
    return splits_from_events(events)


def detect_dividends(start_date: str, end_date: str, *, client=None):
    """Not implemented this PR (modeled fields exist; detection is a later PR)."""
    raise NotImplementedError(
        "detect_dividends is deferred to a later PR of the corporate-actions program"
    )


def detect_renames(start_date: str, end_date: str, *, client=None):
    """Not implemented this PR (modeled fields exist; detection is a later PR)."""
    raise NotImplementedError(
        "detect_renames is deferred to a later PR of the corporate-actions program"
    )
