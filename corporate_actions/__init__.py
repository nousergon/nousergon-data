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
    "expected_factor",
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
