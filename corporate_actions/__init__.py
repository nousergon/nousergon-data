"""corporate_actions — unified corporate-action model + S3 registry.

WHY THIS EXISTS (unified corporate-actions program, config#1431):
    Corporate actions (splits, dividends, renames) retroactively restate a
    ticker's adjusted price history. The data pipeline already DETECTS splits
    (``polygon_client.get_recent_splits`` + the split-factor math, now in
    ``corporate_actions._split_math``) and RESTATES them into the ArcticDB
    universe (data#1298), but it had no first-class,
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

    The program's foundation is a frozen ``CorporateAction`` dataclass with a
    deterministic ``action_id``, polygon-backed detectors (``detect_splits`` /
    ``detect_dividends`` / ``detect_renames``) built ON the split-factor math
    (``corporate_actions._split_math``, the authoritative factor convention),
    and a ``CorporateActionRegistry`` (S3 JSON) that makes the split-restatement
    discrepancy classification *registry-authoritative* rather than
    text-heuristic, and the per-(action, store) restatement idempotent.

    The split RESTATEMENT path lives in :func:`apply` / :func:`sync` (PR3/PR4);
    dividends are RECORDED ONLY (CRSP-separate total-return series, PR5); and
    ticker RENAMES (PR6) are an identity-preserving 1:1 symbol remap migrated by
    :func:`migrate_symbol` rather than a price restatement — mergers of the
    acquired symbol are NOT spliced, they delist via the existing prune path.

CONVENTION (the split factor, in ``corporate_actions._split_math``):
    A forward N-for-1 split (``split_from=1, split_to=N``) divides the adjusted
    price by N for every date STRICTLY BEFORE the ex (execution) date. A reverse
    1-for-N split (``split_from=N, split_to=1``) multiplies by N. The per-event
    multiplicative factor applied to dates strictly before ``ex_date`` is
    therefore ``split_from / split_to`` — and we delegate to
    :func:`cumulative_factor` so this module never re-derives the authoritative
    factor independently.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from corporate_actions._split_math import (
    cumulative_factor,
    restate_series_for_splits,
    split_events,
)
from corporate_actions.registry import CorporateActionRegistry

log = logging.getLogger(__name__)

__all__ = [
    "CorporateAction",
    "CorporateActionRegistry",
    "CorporateActionAuditError",
    "STORE_ARCTICDB_UNIVERSE",
    "STORE_DAILY_CLOSES_ARCHIVE",
    "SyncResult",
    "expected_factor",
    "dividend_factor",
    "total_return_series",
    "apply",
    "sync",
    "detect_splits",
    "detect_dividends",
    "detect_renames",
    "get_splits",
    "get_dividends",
    "splits_from_events",
    "dividends_from_events",
    "renames_from_events",
    "RenameDetection",
    "migrate_symbol",
    # split-factor math re-exported from the package (moved out of the
    # deprecated top-level ``split_factor.py`` shim, PR6 config#1433).
    "cumulative_factor",
    "restate_series_for_splits",
    "split_events",
]

# Action types. Splits restate the price level (apply/sync); dividends are
# RECORDED ONLY (CRSP-separate, PR5); renames are an identity-preserving 1:1
# symbol remap migrated by migrate_symbol (PR6) — never a price restatement.
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

# The per-date ``staging/daily_closes/{date}.parquet`` archive — the second
# store ``sync`` (PR4, config#1433) restates in place. Unlike the ArcticDB
# universe (rebuilt from the S3 price cache every Saturday), a daily-closes
# parquet is a cross-sectional SNAPSHOT (index=ticker, one trading date per
# file) that CANNOT be re-derived from a raw source on demand inside ``sync``
# (polygon grouped-daily is rate-limited + best-effort, and the morning
# pass's own polygon re-fetch is a separate, opportunistic mechanism). So the
# durable registry ``applied`` marker is the SOLE idempotency guard for this
# store — and it is recorded at PER-(action_id, store, DATE) granularity
# (the marker store passed to the registry is ``f"{STORE_DAILY_CLOSES_ARCHIVE}
# /{date}"``), NOT a single per-(action_id, store) marker. Rationale: the live
# window slides forward and a per-date parquet can ENTER the affected set on a
# later run (a missing/late date materializes); a coarse per-store marker would
# then skip that newly-present parquet and leave a split-boundary discontinuity
# in it. A per-date marker guarantees each parquet is restated exactly once,
# whenever it first appears, regardless of when the others were done.
STORE_DAILY_CLOSES_ARCHIVE = "daily_closes_archive"


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


def _normalize_ratio_field(value):
    """Normalize a split-ratio field: integral → int (id-stable), else float.

    Raises ``ValueError`` on non-positive/non-numeric input — a malformed
    ratio must fail loud at construction, never become a silent 1:1 no-op.
    """
    f = float(value)
    if not (f > 0) or f != f:
        raise ValueError(f"split ratio field must be a positive number, got {value!r}")
    return int(f) if f.is_integer() else f


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
    # split fields — fractional for spinoff-style records (1000:1061, 1:1.2);
    # integral values are normalized to int so content-addressed ids are
    # stable across int/float construction paths.
    split_from: int | float | None = None
    split_to: int | float | None = None
    # dividend fields (detected by detect_dividends, recorded CRSP-separate)
    cash_amount: float | None = None
    dividend_kind: str | None = None
    # rename fields (detected by detect_renames; migrated by migrate_symbol)
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
        split_from: "int | float",
        split_to: "int | float",
        *,
        source: str = "polygon",
        raw: dict | None = None,
    ) -> "CorporateAction":
        """Build a split action; ``action_id`` is derived deterministically.

        Ratio fields accept FRACTIONAL values (polygon's spinoff-style
        ``1000:1061`` / ``1:1.2`` records — an ``int()`` cast here silently
        truncated those factors before 2026-07-02); integral values normalize
        to ``int`` so the content-addressed id is unchanged for the dominant
        integer-ratio case.
        """
        return cls(
            type=_TYPE_SPLIT,
            ticker=str(ticker),
            ex_date=str(ex_date),
            split_from=_normalize_ratio_field(split_from),
            split_to=_normalize_ratio_field(split_to),
            source=source,
            raw=dict(raw or {}),
        )

    @classmethod
    def from_dividend(
        cls,
        ticker: str,
        ex_date: str,
        cash_amount: float,
        dividend_kind: str | None = None,
        *,
        source: str = "polygon",
        raw: dict | None = None,
    ) -> "CorporateAction":
        """Build a cash-dividend action; ``action_id`` is derived
        deterministically from ``(type, ticker, ex_date, cash_amount:kind)`` so
        two dividends on different ex dates OR different amounts never collide
        (the ``_detail`` discriminator folds in ``cash_amount``; ``ex_date`` is
        already in the id payload)."""
        return cls(
            type=_TYPE_DIVIDEND,
            ticker=str(ticker),
            ex_date=str(ex_date),
            cash_amount=float(cash_amount),
            dividend_kind=(str(dividend_kind) if dividend_kind is not None else None),
            source=source,
            raw=dict(raw or {}),
        )

    @classmethod
    def from_rename(
        cls,
        old_ticker: str,
        new_ticker: str,
        ex_date: str,
        *,
        source: str = "polygon",
        raw: dict | None = None,
    ) -> "CorporateAction":
        """Build a ticker-rename action (1:1 identity-preserving symbol remap).

        A rename is the polygon ``ticker_change`` event: on ``ex_date`` the
        entity's symbol changes from ``old_ticker`` to ``new_ticker`` (e.g.
        FB -> META on 2022-06-09). ``ticker`` is set to ``old_ticker`` so the
        action keys off the symbol the pipeline already holds (the one going
        missing from constituents) — that is the side the migration reads from.
        ``action_id`` is derived deterministically from
        ``(type, old_ticker, ex_date, old->new)`` so the same rename folds to
        one id across reruns (the write-once exactly-once migration property).
        """
        return cls(
            type=_TYPE_RENAME,
            ticker=str(old_ticker),
            ex_date=str(ex_date),
            old_ticker=str(old_ticker),
            new_ticker=str(new_ticker),
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
        """A human-readable one-liner, e.g. '1-for-2 reverse split'.

        Uses the EXACT ``split_from``/``split_to`` ratio, not a floor-divided
        approximation: a non-clean-integer ratio (e.g. polygon's ``1000:1061``
        stock-dividend-style "split", ~1.061x) previously floor-divided to
        ``1061 // 1000 == 1`` and printed as the misleading, no-op-looking
        "1-for-1 forward split" — which derailed the config#1455 investigation
        (the diagnostic label made a real ~6.1% action look like garbage data).
        A clean integer ratio (the common case, e.g. 4:1) still prints as
        before; anything else prints the raw ``split_from:split_to`` ratio so
        the reader sees the true numbers rather than a rounded lie.
        """
        if self.type == _TYPE_SPLIT and self.split_from and self.split_to:
            if self.split_to >= self.split_from:
                # forward N-for-1 (split_from=1, split_to=N)
                if self.split_to % self.split_from == 0:
                    n = self.split_to // self.split_from
                    return f"{n}-for-1 forward split"
                return f"{self.split_to}-for-{self.split_from} forward split"
            # reverse 1-for-N (split_from=N, split_to=1)
            if self.split_from % self.split_to == 0:
                n = self.split_from // self.split_to
                return f"1-for-{n} reverse split"
            return f"{self.split_to}-for-{self.split_from} reverse split"
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
    by 10). We DELEGATE to :func:`cumulative_factor` (the authoritative
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
        return cumulative_factor([ev], day_before)
    # TODO(corporate-actions program): implement dividend / rename expected
    # factors in a later PR. Fail loud rather than silently returning 1.0.
    raise NotImplementedError(
        f"expected_factor not implemented for action type {action.type!r} "
        "(only splits implemented this PR)"
    )


# Relative tolerance for the price-evidence boundary-ratio check
# (``has_price_evidence``): splits are integer-ish ratios (factor <= 0.5 or
# >= 2 for anything that would ever be reported as a split), so the observed
# close[d]/close[d-1] boundary ratio at a REAL split sits close to
# 1/expected_factor; a generous 15% band absorbs ordinary multi-day price
# drift around the ex_date without confusing a real split with a coincident
# earnings-move noop. Mirrors ``features.compute._AUDIT_FACTOR_REL_TOL`` (the
# existing post-condition audit's tolerance) — same discipline, applied as a
# PRE-condition here so a phantom action never reaches the price level.
_PRICE_EVIDENCE_REL_TOL = 0.15

# Window (calendar days) around ex_date searched for the boundary-ratio price
# evidence — mirrors ``features.compute._AUDIT_EX_DATE_WINDOW_DAYS`` (a split
# can print 1-2 sessions late/early relative to the polygon execution_date on
# a thin feed).
_PRICE_EVIDENCE_EX_DATE_WINDOW_DAYS = 4

# A factor must sit at least this far (multiplicatively) from 1.0 for the
# direct-vs-inverse orientation test to be discriminable from ordinary daily
# drift: 1.45 keeps every real split ratio (3:2 = 1.5 and up) in scope while
# excluding the near-1 spinoff-adjustment records (1000:1061 ≈ 1.061) whose
# stated factor, inverse, and market noise share one tolerance band.
_ORIENTATION_MIN_SEPARATION = 1.45


def price_evidence_orientation(
    close: pd.Series,
    action: CorporateAction,
    *,
    rel_tol: float = _PRICE_EVIDENCE_REL_TOL,
    window_days: int = _PRICE_EVIDENCE_EX_DATE_WINDOW_DAYS,
) -> str:
    """Classify what the RAW ``close`` series says about ``action``'s factor
    DIRECTION around its ``ex_date``.

    A corporate-action FEED can report a split that never happened (config#1455
    phantom class) — or, observed live 2026-07-01/02, report a REAL split with
    the ``split_from``/``split_to`` ratio INVERTED (polygon published HON's
    forward 1:2 as ``2:1`` — then deleted the record upstream — and DD's
    forward 1:3 as ``3:1``; blindly applying the stated factor multiplied both
    tickers' full pre-ex histories instead of dividing them). The market's own
    boundary move is ground truth for the DIRECTION polygon's adjusted
    aggregates actually applied, so the factor we fold into a price store must
    be corroborated against it, not trusted from the feed's field ordering.

    Looks at every adjacent trading-day pair in ±``window_days`` of ``ex_date``
    on the RAW close (a real split prints its factor as the boundary ratio
    ``close[d]/close[d-1]``) and returns one of:

      * ``"direct"``    — a boundary matches ``expected_factor(action)`` and
        none matches its inverse: the feed's orientation is market-confirmed.
      * ``"inverse"``   — a boundary matches ``1/expected_factor`` and none
        matches the stated factor: the feed record is inverted; the
        market-corrected factor is ``1/expected_factor``.
      * ``"ambiguous"`` — boundaries match BOTH orientations (seen when a
        prior basis-splice corruption fabricates the direct ratio while the
        real move prints the inverse — the HON 2026-07-01 series). Nothing
        can be trusted here; the caller must refuse.
      * ``"none"``      — the window is covered but no boundary matches
        either orientation (the phantom-split case).
      * ``"uncovered"`` — the series doesn't cover the window (< 2 rows), so
        the market can neither confirm nor refute.

    Non-split actions return ``"direct"`` (nothing to check — only splits
    mutate the price level) as does a malformed factor (``expected_factor``
    guards those upstream).
    """
    if action.type != _TYPE_SPLIT:
        return "direct"  # only splits mutate the price level; nothing to check
    expected = expected_factor(action)
    if expected <= 0:
        return "direct"  # malformed ratio — expected_factor already guards this

    if close is None or close.empty:
        return "uncovered"

    idx = close.index if isinstance(close.index, pd.DatetimeIndex) else pd.to_datetime(close.index)
    s = pd.Series(close.to_numpy(dtype="float64"), index=idx).sort_index()
    s = s[s > 0]  # a non-positive close can't form a meaningful ratio
    if s.empty:
        return "uncovered"

    ex = pd.Timestamp(action.ex_date).normalize()
    lo = ex - pd.Timedelta(days=window_days)
    hi = ex + pd.Timedelta(days=window_days)
    window = s[(s.index >= lo) & (s.index <= hi)]
    if len(window) < 2:
        # Window not covered by this series (e.g. ticker's history starts
        # after ex_date, or a sparse feed).
        return "uncovered"

    ratios = (window / window.shift(1)).dropna()
    if ratios.empty:
        return "uncovered"

    direct_hit = bool((ratios.sub(expected).abs() <= rel_tol * expected).any())

    # Orientation is only DISCRIMINABLE when the factor sits well away from
    # 1.0 — for a near-1 factor (polygon's 1000:1061 spinoff-style records)
    # the stated factor, its inverse, and ordinary daily drift all fall inside
    # one tolerance band, so inverse/ambiguous verdicts would be noise. Those
    # factors keep the legacy direct-only semantics.
    if 1.0 / _ORIENTATION_MIN_SEPARATION < expected < _ORIENTATION_MIN_SEPARATION:
        return "direct" if direct_hit else "none"

    inverse = 1.0 / expected
    inverse_hit = bool((ratios.sub(inverse).abs() <= rel_tol * inverse).any())
    if direct_hit and inverse_hit:
        return "ambiguous"
    if direct_hit:
        return "direct"
    if inverse_hit:
        return "inverse"
    return "none"


def has_price_evidence(
    close: pd.Series,
    action: CorporateAction,
    *,
    rel_tol: float = _PRICE_EVIDENCE_REL_TOL,
    window_days: int = _PRICE_EVIDENCE_EX_DATE_WINDOW_DAYS,
) -> bool:
    """Back-compat wrapper over :func:`price_evidence_orientation` — True iff
    the market confirms the action's STATED orientation. Note the semantic
    tightening (2026-07-02): an uncovered window now returns ``False`` (do not
    mutate a price level on zero corroboration) where it previously trusted
    the feed — the inverted-record incident showed feed trust without market
    evidence is how full-history corruption happens. With no pre-ex rows in
    frame there is nothing to restate anyway, so the safe answer costs nothing.
    """
    return price_evidence_orientation(
        close, action, rel_tol=rel_tol, window_days=window_days,
    ) == "direct"


# ── dividends: total-return factor MATH (CRSP/Barra basis) ───────────────────
#
# Dividends are tracked as a SEPARATE total-return series and MUST NOT be folded
# into the stored split-adjusted price LEVEL (Brian-decided, config#1433). The
# primitives below compute that distinct series; they NEVER mutate a price store
# or a feature. PR7 consumes the registry-recorded dividend events to build +
# persist the total-return series under the new schema — these are the math it
# will call. Kept here (not in _split_math) because the split factor convention
# is multiplicative on the price LEVEL whereas a dividend factor is a back-adjust
# applied to a SEPARATE return series.


def dividend_factor(cash_amount: float, close_prev: float) -> float:
    """The CRSP/yfinance total-return back-adjust factor for one cash dividend.

    A dividend of ``cash_amount`` going ex when the prior close is ``close_prev``
    back-adjusts every PRE-ex price by ``1 - cash_amount/close_prev`` to build a
    total-return series (the post-ex price drop by the dividend is "added back"
    into the pre-ex prices so the series is continuous through the ex-date drop).
    This is the standard CRSP/Barra adjustment factor; it is applied to a
    SEPARATE total-return series, never to the stored split-adjusted price level.

    ``close_prev`` must be > 0 (the trading day BEFORE the ex-date). The factor is
    in ``(0, 1]`` for a normal dividend (``0 < cash_amount < close_prev``).
    """
    cash = float(cash_amount)
    cp = float(close_prev)
    if not (cp > 0):
        raise ValueError(
            f"dividend_factor: close_prev must be > 0, got {close_prev!r}"
        )
    return 1.0 - cash / cp


def total_return_series(
    price_df: pd.DataFrame,
    dividend_actions: list["CorporateAction"],
    *,
    close_col: str = "Close",
) -> pd.Series:
    """Build a SEPARATE total-return-adjusted close from a (split-adjusted) price
    series + the dividend events — the CRSP primitive (config#1433).

    ``price_df`` is one ticker's price frame (DatetimeIndex), already on whatever
    split-adjusted scale the caller maintains. ``dividend_actions`` are the
    ``type="dividend"`` :class:`CorporateAction`s for that ticker. Returns a NEW
    ``pd.Series`` (the total-return close); **it does NOT mutate ``price_df``** and
    is wired into NO store/feature — PR7 consumes it.

    Method (mirrors the split-factor compounding correctness, but on a return
    series): process dividends OLDEST→NEWEST; for each ex-date E, read the close
    on the trading day strictly BEFORE E from the RUNNING series, compute
    ``dividend_factor(cash, close_prev)``, and multiply every row STRICTLY BEFORE
    E by that factor. Oldest→newest ordering keeps each factor's ``close_prev`` on
    the un-back-adjusted scale (an earlier dividend only touches rows before its
    own — earlier — ex-date, which are strictly before this dividend's
    ``close_prev`` row), so the factors compound multiplicatively exactly like
    CRSP. Splits and dividends stay INDEPENDENT: ``price_df`` carries the split
    adjustment on the price level; this returns that series further
    dividend-adjusted on the SEPARATE total-return axis.

    Dividends whose ex-date is on/before the first row (no earlier row to adjust)
    contribute nothing and are skipped. The input series order/index is preserved.
    """
    if price_df is None or getattr(price_df, "empty", True):
        return pd.Series(dtype="float64", name="tr_close")
    if close_col not in price_df.columns:
        raise KeyError(
            f"total_return_series: close_col {close_col!r} not in price_df columns "
            f"{list(price_df.columns)}"
        )
    idx = (
        price_df.index
        if isinstance(price_df.index, pd.DatetimeIndex)
        else pd.to_datetime(price_df.index)
    )
    # Operate on a private numpy copy so price_df is never mutated; close_prev is
    # always read from this RUNNING array (the compounding state).
    vals = price_df[close_col].to_numpy(dtype="float64").copy()
    ts = pd.DatetimeIndex(idx).normalize().to_numpy()

    divs = [a for a in (dividend_actions or []) if a.type == _TYPE_DIVIDEND]
    divs.sort(key=lambda a: pd.Timestamp(a.ex_date).normalize())
    for a in divs:
        if a.cash_amount is None:
            continue
        ex = np.datetime64(pd.Timestamp(a.ex_date).normalize())
        before = ts < ex
        if not before.any():
            continue  # ex-date at/before first row — nothing earlier to adjust
        prev_pos = int(np.nonzero(before)[0][-1])  # last row strictly before ex
        factor = dividend_factor(a.cash_amount, vals[prev_pos])
        vals[before] = vals[before] * factor

    return pd.Series(vals, index=price_df.index, name="tr_close")


def apply(
    df: pd.DataFrame,
    actions: list["CorporateAction"],
    *,
    store: str,
    registry: "CorporateActionRegistry | None" = None,
    run_id: str | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    """Restate a SINGLE ticker's price frame so its full history is corporate-
    action-consistent, routing all split restatement through the split-factor
    math (``corporate_actions._split_math``).

    ``df`` is one ticker's OHLCV(+) frame (DatetimeIndex). ``actions`` are the
    ``CorporateAction``s that pertain to THAT frame's ticker (the caller filters
    by ticker — this PR only restates SPLIT actions; a dividend/rename action
    passed in raises ``NotImplementedError`` because their factor math ships in a
    later PR). The full-history multiplicative restatement itself is delegated to
    :func:`restate_series_for_splits` (price ×factor, volume ÷factor
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
    ``status`` is ``"applied"`` (restated this call), ``"noop"`` (already
    applied per the registry — not re-adjusted), or ``"unconfirmed"`` (the feed
    reported the action but the raw close shows NO corroborating price move
    around ``ex_date`` — NOT applied, config#1455 price-evidence gate; never
    marks the registry, so it is re-checked every call rather than remembered
    as permanently skipped).
    """
    applied_results: list[dict] = []
    if df is None or getattr(df, "empty", True) or not actions:
        return df, applied_results

    # Only splits mutate the stored PRICE LEVEL. A dividend reaching here is a
    # caller error: dividends are CRSP-SEPARATE — they are tracked as a distinct
    # total-return series (see ``dividend_factor`` / ``total_return_series``) and
    # MUST NOT be folded into the split-adjusted price level ``apply`` restates.
    # The registry is the dividend persistence layer (``sync`` records them, does
    # NOT apply them); PR7 consumes the recorded events to build + persist the TR
    # series under the new schema. So we fail loud rather than ever multiplying a
    # price by a dividend factor here. Renames are likewise not a price-level
    # restatement (deferred to a later PR).
    split_actions: list[CorporateAction] = []
    for a in actions:
        if a.type == _TYPE_SPLIT:
            split_actions.append(a)
        elif a.type == _TYPE_DIVIDEND:
            raise NotImplementedError(
                "corporate_actions.apply: dividend actions are CRSP-separate and "
                "must NOT mutate the split-adjusted price level — use "
                "total_return_series to build the SEPARATE total-return series "
                "(sync records dividends to the registry; PR7 persists the TR "
                "series). Passing a dividend to apply() is a caller error."
            )
        elif a.type == _TYPE_RENAME:
            raise NotImplementedError(
                "corporate_actions.apply: rename actions are deferred to a later "
                "PR of the program (splits only mutate the price level here)"
            )
        else:
            raise ValueError(
                f"corporate_actions.apply: unknown action type {a.type!r}"
            )

    if not split_actions:
        return df, applied_results

    idx = df.index if isinstance(df.index, pd.DatetimeIndex) else pd.to_datetime(df.index)
    raw_close = df["Close"] if "Close" in df.columns else None

    # (action, effective_factor, orientation_corrected) triples cleared to
    # restate. ``effective_factor`` is the MARKET-CORROBORATED multiplier for
    # pre-ex rows — equal to the feed's ``expected_factor`` when the market
    # confirms the stated orientation, and its inverse when the market shows
    # the feed record is inverted (observed live 2026-07-01/02: polygon
    # published HON's forward 1:2 as ``2:1`` and DD's forward 1:3 as ``3:1``,
    # which — applied as stated — multiplied both full histories instead of
    # dividing them).
    events_to_apply: list[tuple[CorporateAction, float, bool]] = []
    for a in split_actions:
        factor = expected_factor(a)  # == split_from / split_to (as stated by feed)
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
        # PRICE-EVIDENCE + ORIENTATION gate (config#1455 + 2026-07-02
        # inverted-record incident): the market's own boundary move is the
        # ground truth for BOTH whether the action is real (phantom check) and
        # which DIRECTION its factor applies (orientation check). The stated
        # factor reaches the price level only when the raw close corroborates
        # it; an unambiguous inverse match applies the market-corrected
        # 1/factor (loud WARN + flagged in the result); ambiguous/none/
        # uncovered never mutate the store and never mark the registry (an
        # unresolved action must be re-checked every run, not remembered as
        # skipped).
        orientation = price_evidence_orientation(raw_close, a)
        if orientation == "direct":
            events_to_apply.append((a, factor, False))
        elif orientation == "inverse":
            corrected = 1.0 / factor
            applied_note = (
                "corporate_actions.apply: %s %s (%s) — feed record is "
                "INVERTED: raw close boundary matches 1/factor=%.6g, not the "
                "stated factor %.6g. Applying the MARKET-CORRECTED factor "
                "(orientation_corrected=True); action_id=%s"
            )
            log.warning(
                applied_note, a.ticker, a.human(), store, corrected, factor,
                a.action_id,
            )
            events_to_apply.append((a, corrected, True))
        else:
            applied_results.append({
                "action_id": a.action_id,
                "store": store,
                "n_rows_adjusted": 0,
                "factor": factor,
                "status": (
                    "ambiguous_evidence" if orientation == "ambiguous"
                    else "unconfirmed"
                ),
            })
            if orientation == "ambiguous":
                log.error(
                    "corporate_actions.apply: %s %s (%s) — raw close matches "
                    "BOTH the stated factor and its inverse around ex_date "
                    "(a prior basis-splice corruption can fabricate the "
                    "stated-direction ratio). REFUSING to restate — resolve "
                    "the series basis first; action_id=%s",
                    a.ticker, a.human(), store, a.action_id,
                )
            else:
                log.warning(
                    "corporate_actions.apply: %s %s (%s) has NO corroborating "
                    "price move on the raw close within the ex_date window "
                    "(orientation=%s) — NOT applying (feed-reported split "
                    "with no market evidence, config#1455); action_id=%s",
                    a.ticker, a.human(), store, orientation, a.action_id,
                )
            continue

    if not events_to_apply:
        return df, applied_results

    # Feed ``restate_series_for_splits`` the MARKET-CORROBORATED factor: for an
    # orientation-corrected action the from/to pair is swapped so the shared
    # factor convention (multiply pre-ex rows by split_from/split_to) yields
    # the corrected multiplier.
    events = [
        {
            "execution_date": a.ex_date,
            "split_from": a.split_to if corrected_flag else a.split_from,
            "split_to": a.split_from if corrected_flag else a.split_to,
        }
        for a, _eff, corrected_flag in events_to_apply
    ]
    restated = restate_series_for_splits(df, events)

    # POST-RESTATE boundary audit (2026-07-02): on the OUTPUT frame, no
    # boundary pair around any applied ex_date may still print the applied
    # factor or its inverse — a residual means the restatement went the wrong
    # direction or double-applied (the corruption this function exists to
    # prevent). Fail loud BEFORE the caller writes. Near-1 factors are exempt
    # (a correctly-flattened ~1.0 boundary is indistinguishable from a near-1
    # factor within tolerance, so the audit would false-fail).
    if "Close" in restated.columns:
        for a, eff, _corrected_flag in events_to_apply:
            stated = expected_factor(a)
            if 1.0 / _ORIENTATION_MIN_SEPARATION < stated < _ORIENTATION_MIN_SEPARATION:
                continue
            residual = price_evidence_orientation(
                restated["Close"],
                CorporateAction.from_split(
                    a.ticker, a.ex_date, a.split_from, a.split_to,
                    source=a.source, raw=a.raw,
                ),
            )
            if residual in ("direct", "inverse", "ambiguous"):
                raise CorporateActionAuditError(
                    f"corporate_actions.apply: post-restate residual boundary "
                    f"for {a.ticker} {a.human()} (store={store}, "
                    f"orientation={residual}, effective_factor={eff:.6g}, "
                    f"action_id={a.action_id}) — restatement did not flatten "
                    f"the ex_date boundary; refusing so the caller does not "
                    f"persist a corrupted series"
                )

    for a, eff, corrected_flag in events_to_apply:
        ex = pd.Timestamp(a.ex_date).normalize()
        n_rows_adjusted = int((idx < ex).sum())
        applied_results.append({
            "action_id": a.action_id,
            "store": store,
            "n_rows_adjusted": n_rows_adjusted,
            "factor": eff,
            "orientation_corrected": corrected_flag,
            "status": "applied",
        })
        if registry is not None:
            registry.mark_applied(a, store, run_id=run_id)

    return restated, applied_results


# ── sync: unified, pre-read orchestration across ALL stores (PR4, config#1433) ─

# Price / volume columns a daily-closes archive parquet carries that a split
# restates (mirrors restate_series_for_splits' defaults; only the
# columns actually present in a given parquet are touched).
_ARCHIVE_PRICE_COLS = ("Open", "High", "Low", "Close", "VWAP", "Adj_Close")
_ARCHIVE_VOLUME_COLS = ("Volume",)
# Relative tolerance for the daily-closes archive SCALE VERIFICATION (old vs
# already-restated). Splits are integer ratios (factor <= 0.5 or >= 2), so the
# observed pre→post boundary ratio is either ~factor (un-restated) or ~1.0
# (already restated) — both well separated from each other, so a generous tol
# absorbs multi-day price drift without confusing the two regimes.
_ARCHIVE_SCALE_REL_TOL = 0.15


@dataclass(frozen=True)
class SyncResult:
    """Summary returned by :func:`sync`.

    * ``detected`` — the split :class:`CorporateAction`s detected/recorded over
      the window this run (write-if-absent into the registry).
    * ``applied`` — ``{store: [apply_result_dict, ...]}``; each dict carries
      ``action_id`` / ``store`` / ``ticker`` / ``n_rows_adjusted`` / ``factor``
      / ``status`` (``"applied"`` | ``"noop"`` | ``"skipped"``) plus, for the
      daily-closes archive, a ``date``.
    * ``notices`` — the subset of ``detected`` that actually restated at least
      one row in at least one store this run (the operator-notification set:
      "the system saw this action and brought the stores onto its scale").
      SPLITS ONLY — dividends are NEVER notices (CRSP-separate, sub-threshold,
      and frequent; see ``dividends``).
    * ``dividends`` — the dividend :class:`CorporateAction`s detected/recorded
      over the window this run (write-if-absent into the registry). RECORDED
      ONLY: dividends are CRSP-separate, never applied to a price store, and —
      because they are frequent (~quarterly × hundreds of names) and cause
      sub-5% ex-date drops (below the discrepancy ERROR band) — they emit NO
      per-dividend email/notice (that would be noise). The count feeds the
      summary log only; PR7 consumes the recorded events to build the TR series.
    """

    detected: list = field(default_factory=list)
    applied: dict = field(default_factory=dict)
    notices: list = field(default_factory=list)
    dividends: list = field(default_factory=list)


def _scrub(exc: object) -> str:
    """Scrub a polygon apiKey from an exception/text before logging (lazy import
    so ``corporate_actions`` stays free of a hard ``polygon_client`` dep)."""
    try:
        from polygon_client import _scrub_api_key

        return _scrub_api_key(exc)
    except Exception:  # noqa: BLE001 - logging-path fallback, never raise
        return str(exc)


def _sync_arcticdb_universe(
    bucket: str, ticker: str, actions: list, registry, run_id: str | None,
) -> list[dict]:
    """Mid-week restatement of ONE ArcticDB universe symbol (the gap PR4 closes).

    Reads the symbol's FULL series, restates every not-yet-applied split via the
    shared :func:`apply` math, and rewrites it so daily-appended rows land on a
    continuous adjusted scale BEFORE ``daily_append`` reads (today the universe
    history is only restated at Saturday backfill, so the split-boundary
    discontinuity re-forms mid-week).

    WRITE-THEN-MARK: the registry ``applied`` marker is the contract
    ``builders/daily_append.py``'s basis-consistency guard trusts to mean "the
    ArcticDB history is on the restated scale". So the marker is written ONLY
    after the ``lib.write`` actually lands — a mark-before-write would let a
    failed rewrite leave daily_append appending onto an un-restated history
    (the exact corruption this arc prevents). We therefore drive ``apply`` with
    ``registry=None`` (math + is_applied skip handled here) and mark explicitly.
    """
    from store.arctic_store import get_universe_lib, to_arctic_canonical

    lib = get_universe_lib(bucket)
    try:
        df = lib.read(ticker).data
    except Exception as exc:  # noqa: BLE001 - symbol absent ⇒ nothing to restate
        log.info(
            "corporate_actions.sync: %s not in ArcticDB universe — no arctic "
            "restate (%s)", ticker, _scrub(exc),
        )
        return []

    results: list[dict] = []
    pending: list = []
    for a in actions:
        if registry is not None and registry.is_applied(STORE_ARCTICDB_UNIVERSE, a.action_id):
            results.append({
                "action_id": a.action_id, "store": STORE_ARCTICDB_UNIVERSE,
                "ticker": ticker, "n_rows_adjusted": 0,
                "factor": expected_factor(a), "status": "noop",
            })
        else:
            pending.append(a)
    if not pending:
        return results

    # registry=None: compute the restatement + per-action row counts WITHOUT
    # marking, so the mark below is strictly write-then-mark.
    restated, applied_math = apply(
        df, pending, store=STORE_ARCTICDB_UNIVERSE, registry=None, run_id=run_id,
    )
    if any(r["n_rows_adjusted"] > 0 for r in applied_math):
        lib.write(ticker, to_arctic_canonical(restated), prune_previous_versions=True)
    # Mark ONLY actions apply() actually folded in (status == "applied").
    # Marking an unconfirmed/ambiguous refusal would poison the exactly-once
    # contract: the marker permanently blocks the restatement it falsely
    # claims happened (2026-07-01 incident — HON/DD were marked applied on a
    # refused/corrupted apply and their un-restated histories were frozen
    # behind the marker). A refused action stays pending and is re-checked
    # every run — loud, never silently dropped.
    if registry is not None:
        applied_ids = {
            r["action_id"] for r in applied_math if r["status"] == "applied"
        }
        for a in pending:
            if a.action_id in applied_ids:
                registry.mark_applied(a, STORE_ARCTICDB_UNIVERSE, run_id=run_id)
    for r in applied_math:
        r = dict(r)
        r["ticker"] = ticker
        results.append(r)
    return results


def _read_archive_parquet(s3, bucket: str, prefix: str, date: str):
    """Read ``{prefix}{date}.parquet`` (index=ticker) or ``None`` if absent."""
    import io

    key = f"{prefix}{date}.parquet"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except Exception:  # noqa: BLE001 - missing / unreadable ⇒ caller skips date
        return None
    try:
        return pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
    except Exception as exc:  # noqa: BLE001 - corrupt parquet ⇒ skip, never crash
        log.warning(
            "corporate_actions.sync: could not read %s (%s) — skipping date",
            key, _scrub(exc),
        )
        return None


def _write_archive_parquet(s3, bucket: str, prefix: str, date: str, df) -> None:
    import io

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    buf.seek(0)
    s3.put_object(
        Bucket=bucket, Key=f"{prefix}{date}.parquet", Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )


def _classify_archive_scale(c_candidate, c_post, factor: float) -> str:
    """Is the candidate row on the OLD (pre-split) or already-restated scale?

    Uses the post-split reference close ``c_post`` (a window date on/after the
    ex date, definitionally on the current scale). The observed boundary ratio
    ``c_post / c_candidate`` is ~``factor`` when the candidate is un-restated and
    ~``1.0`` once it has been lifted onto the post-split scale. Returns
    ``"old"`` | ``"new"`` | ``"unknown"`` (the last when no reference is
    available or the ratio is ambiguous — in which case ``sync`` conservatively
    declines to multiply, leaving the morning polygon re-fetch / Saturday
    backfill as the heal, so it can NEVER double-apply onto an already-restated
    parquet — the only-the-marker-guards-this-store risk).
    """
    try:
        cc = float(c_candidate)
        cp = float(c_post)
    except (TypeError, ValueError):
        return "unknown"
    if not (cc > 0 and cp > 0 and factor > 0):
        return "unknown"
    ratio = cp / cc
    if abs(ratio - factor) <= _ARCHIVE_SCALE_REL_TOL * factor:
        return "old"
    if abs(ratio - 1.0) <= _ARCHIVE_SCALE_REL_TOL:
        return "new"
    return "unknown"


def _sync_daily_closes_archive(
    s3, bucket: str, ticker: str, actions: list, window_dates: list[str],
    registry, run_id: str | None, *, prefix: str = "staging/daily_closes/",
) -> list[dict]:
    """Restate ONE ticker's rows across the affected daily-closes archive
    parquets in the live window — in place, per-date, idempotently.

    Idempotency is PURELY registry-marker driven (a per-date parquet cannot be
    re-derived from a raw source here), at PER-(action_id, store, DATE)
    granularity. To stay safe against the morning pass's SEPARATE polygon
    re-fetch (which also restates touched dates, via ``adjusted=true``), every
    multiply is gated on a boundary SCALE VERIFICATION
    (:func:`_classify_archive_scale`): a parquet already on the post-split scale
    is NOT multiplied (only marked) — so neither a sync re-run NOR an
    independent polygon re-fetch can double-adjust it. WRITE-THEN-MARK as well.
    """
    results: list[dict] = []
    wdates = sorted(window_dates)
    for a in actions:
        ex = pd.Timestamp(a.ex_date).normalize()
        factor = expected_factor(a)
        affected = [d for d in wdates if pd.Timestamp(d).normalize() < ex]
        post_dates = [d for d in wdates if pd.Timestamp(d).normalize() >= ex]
        # Post-split reference close for scale verification (earliest on/after ex).
        c_post = None
        for pd_date in post_dates:
            post_df = _read_archive_parquet(s3, bucket, prefix, pd_date)
            if post_df is not None and ticker in post_df.index and "Close" in post_df.columns:
                c_post = post_df.at[ticker, "Close"]
                break
        for d in affected:
            store_d = f"{STORE_DAILY_CLOSES_ARCHIVE}/{d}"
            if registry is not None and registry.is_applied(store_d, a.action_id):
                results.append({
                    "action_id": a.action_id, "store": STORE_DAILY_CLOSES_ARCHIVE,
                    "ticker": ticker, "date": d, "n_rows_adjusted": 0,
                    "factor": factor, "status": "noop",
                })
                continue
            df_d = _read_archive_parquet(s3, bucket, prefix, d)
            if df_d is None or ticker not in df_d.index or "Close" not in df_d.columns:
                # Parquet missing / ticker absent — nothing to restate (and do
                # NOT mark, so a later run re-checks once it materializes).
                continue
            scale = _classify_archive_scale(df_d.at[ticker, "Close"], c_post, factor)
            if scale == "old":
                cols = [
                    c for c in (*_ARCHIVE_PRICE_COLS, *_ARCHIVE_VOLUME_COLS)
                    if c in df_d.columns
                ]
                tmp = df_d.loc[[ticker], cols].copy()
                tmp.index = pd.DatetimeIndex([pd.Timestamp(d)])
                ev = [{
                    "execution_date": a.ex_date,
                    "split_from": a.split_from, "split_to": a.split_to,
                }]
                restated_tmp = restate_series_for_splits(tmp, ev)
                for col in cols:
                    df_d.at[ticker, col] = restated_tmp.iloc[0][col]
                _write_archive_parquet(s3, bucket, prefix, d, df_d)
                if registry is not None:
                    registry.mark_applied(a, store_d, run_id=run_id)
                results.append({
                    "action_id": a.action_id, "store": STORE_DAILY_CLOSES_ARCHIVE,
                    "ticker": ticker, "date": d, "n_rows_adjusted": 1,
                    "factor": factor, "status": "applied",
                })
            elif scale == "new":
                # Already on the post-split scale (e.g. the morning polygon
                # re-fetch restated it on a prior run) — record the marker so we
                # don't re-examine, but DO NOT multiply.
                if registry is not None:
                    registry.mark_applied(a, store_d, run_id=run_id)
                results.append({
                    "action_id": a.action_id, "store": STORE_DAILY_CLOSES_ARCHIVE,
                    "ticker": ticker, "date": d, "n_rows_adjusted": 0,
                    "factor": factor, "status": "noop",
                })
            else:
                # Unverifiable scale (no reference / ambiguous ratio) — decline to
                # multiply (cannot prove old-scale ⇒ refuse to risk a double
                # adjust). Leave UN-marked; the morning polygon re-fetch / Saturday
                # backfill remain the heal. Recorded, not silently dropped.
                log.warning(
                    "corporate_actions.sync: cannot verify %s %s scale on %s "
                    "(no post-split reference in window) — skipping archive "
                    "restate, leaving heal to polygon re-fetch / backfill",
                    ticker, a.human(), d,
                )
                results.append({
                    "action_id": a.action_id, "store": STORE_DAILY_CLOSES_ARCHIVE,
                    "ticker": ticker, "date": d, "n_rows_adjusted": 0,
                    "factor": factor, "status": "skipped",
                })
    return results


def sync(
    s3,
    bucket: str,
    start_date,
    end_date,
    *,
    stores: list[str],
    run_id: str,
    tickers: list[str] | None = None,
    registry: "CorporateActionRegistry | None" = None,
    actions: list | None = None,
    dividend_actions: list | None = None,
) -> SyncResult:
    """Unified corporate-action restatement across ALL ``stores`` (PR4,
    config#1433) — ONE pre-read orchestration entry point so the split-boundary
    discontinuity is flattened BEFORE any consumer reads.

    (a) Detects splits over ``[start_date, end_date]`` (or REUSES the
    already-detected ``actions`` — the morning collector passes both its
    ``registry`` and the splits it already scanned, so ``sync`` adds NO extra
    polygon call) and records each write-if-absent. (b) For each detected split
    and each requested store, restates the affected ticker(s) — SKIPPING any
    ``(action, store)`` the registry already marks applied — and marks applied.
    (c) Returns a :class:`SyncResult` summary.

    Per-store topology:

      * ``STORE_ARCTICDB_UNIVERSE`` — read the symbol's full series, restate via
        the shared :func:`apply` math, write back (the mid-week restatement that
        keeps daily-appended rows consistent; idempotent vs the Saturday
        backfill via the shared ``arcticdb_universe`` applied marker — backfill
        sees ``is_applied=True`` and will not re-apply, per PR3 §4).
      * ``STORE_DAILY_CLOSES_ARCHIVE`` — restate the per-date archive parquets in
        the live window in place; idempotency is registry-marker-only, at
        per-(action_id, store, date) granularity, with a boundary scale check so
        it can never double-adjust an already-restated parquet.

    Dividends are ALSO detected + recorded over the window (one extra
    ``get_recent_dividends`` call, or the reused ``dividend_actions``), but
    RECORDED ONLY — they are CRSP-separate (tracked as a distinct total-return
    series, never folded into the price level) and frequent + sub-threshold, so
    they restate NO store and emit NO per-dividend email/notice; only their
    count enters the summary. Renames are not detected yet. Best-effort +
    fail-loud: a per-store / per-ticker failure WARNs (apiKey scrubbed) and
    continues — the PRIMARY morning collection must not die because one symbol's
    restatement failed — but the failure is RECORDED (never silently swallowed),
    and the blocking backfill audit (PR3 §3) remains the train-write correctness
    gate.
    """
    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    if registry is None:
        registry = CorporateActionRegistry(s3, bucket)

    # (a) detect (or reuse) + record write-if-absent.
    if actions is None:
        actions = detect_splits(start_str, end_str)
    ticker_set = set(tickers) if tickers is not None else None
    detected: list = []
    for a in actions:
        if a.type != _TYPE_SPLIT:
            continue
        try:
            registry.record_detected(a, run_id=run_id)
        except Exception as exc:  # noqa: BLE001 - provenance write best-effort
            log.warning(
                "corporate_actions.sync: record_detected failed for %s (%s)",
                a.action_id, _scrub(exc),
            )
        detected.append(a)

    # (a') detect (or reuse) + RECORD dividends — CRSP-separate, RECORDED ONLY.
    # Dividends never restate a price store (they are tracked as a distinct
    # total-return series, built later by PR7 from these recorded events) and
    # never become a notice/email (frequent + sub-5% ex-date drop, below the
    # discrepancy ERROR band). One extra polygon call (gated like the split scan
    # — sync is only invoked on the live, non-dry-run path), or the reused
    # ``dividend_actions`` when the caller already scanned them. A detection miss
    # degrades to [] inside detect_dividends and must never fail the sync.
    if dividend_actions is None:
        try:
            dividend_actions = detect_dividends(start_str, end_str)
        except Exception as exc:  # noqa: BLE001 - detection best-effort
            log.warning(
                "corporate_actions.sync: dividend detection failed (%s) — "
                "recording no dividends this run", _scrub(exc),
            )
            dividend_actions = []
    dividends: list = []
    for a in dividend_actions or []:
        if a.type != _TYPE_DIVIDEND:
            continue
        try:
            registry.record_detected(a, run_id=run_id)
        except Exception as exc:  # noqa: BLE001 - provenance write best-effort
            log.warning(
                "corporate_actions.sync: record_detected failed for dividend "
                "%s (%s)", a.action_id, _scrub(exc),
            )
        dividends.append(a)

    # Restatement is scoped to the requested ticker universe (when given); the
    # detected/recorded set above is NOT scoped (the discrepancy classifier and
    # provenance trail want every detected action).
    by_ticker: dict[str, list] = {}
    for a in detected:
        if ticker_set is not None and a.ticker not in ticker_set:
            continue
        by_ticker.setdefault(a.ticker, []).append(a)

    window_dates = [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start_str, end_str)]

    applied: dict[str, list] = {store: [] for store in stores}
    restated_action_ids: set[str] = set()
    for store in stores:
        for ticker, tactions in by_ticker.items():
            try:
                if store == STORE_ARCTICDB_UNIVERSE:
                    res = _sync_arcticdb_universe(
                        bucket, ticker, tactions, registry, run_id,
                    )
                elif store == STORE_DAILY_CLOSES_ARCHIVE:
                    res = _sync_daily_closes_archive(
                        s3, bucket, ticker, tactions, window_dates, registry, run_id,
                    )
                else:
                    raise ValueError(f"corporate_actions.sync: unknown store {store!r}")
            except Exception as exc:  # noqa: BLE001 - per-store/ticker degrade
                log.warning(
                    "corporate_actions.sync: restate failed for store=%s "
                    "ticker=%s (%s) — continuing; backfill audit remains the "
                    "correctness gate", store, ticker, _scrub(exc),
                )
                continue
            applied[store].extend(res)
            for r in res:
                if r.get("status") == "applied" and r.get("n_rows_adjusted", 0) > 0:
                    restated_action_ids.add(r["action_id"])

    notices = [a for a in detected if a.action_id in restated_action_ids]
    return SyncResult(
        detected=detected, applied=applied, notices=notices, dividends=dividends,
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


def get_splits(ticker: str, *, client=None) -> list["CorporateAction"]:
    """Per-ticker, FULL-history split events for ``ticker`` as
    ``CorporateAction``s (type="split", ex_date = polygon execution_date).

    Sibling of :func:`detect_splits` (which scans the whole market over a
    date RANGE for the daily sync) — this is the one-ticker, full-history
    fetch the offline CRSP-basis migration (PR7, config#1434) needs to
    reconstruct a ticker's entire split-adjusted history. Wraps
    ``polygon_client.get_splits`` (which returns ``{"execution_date",
    "split_from", "split_to"}`` dicts WITHOUT a ticker key) and injects the
    ticker so each event maps to a content-addressed split ``CorporateAction``.
    The client is constructed lazily (mirroring :func:`detect_splits`) and any
    construction/fetch failure DEGRADES GRACEFULLY to ``[]`` (apiKey scrubbed)
    — never hard-fails the caller; the migration's reconciliation backstop is
    what catches a genuinely missing split.
    """
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # import / construction failure — degrade
            from polygon_client import _scrub_api_key

            log.warning(
                "corporate_actions.get_splits: could not obtain polygon client "
                "(%s) — returning no splits for %s",
                _scrub_api_key(exc), ticker,
            )
            return []
    try:
        events = client.get_splits(ticker)
    except Exception as exc:
        from polygon_client import _scrub_api_key

        log.warning(
            "corporate_actions.get_splits: polygon split fetch failed for %s "
            "(%s) — returning no splits",
            ticker, _scrub_api_key(exc),
        )
        return []
    # client.get_splits omits the ticker key; inject it so splits_from_events
    # can build the content-addressed action.
    return splits_from_events([{**ev, "ticker": ticker} for ev in events])


def get_dividends(ticker: str, *, client=None) -> list["CorporateAction"]:
    """Per-ticker, FULL-history cash-dividend events for ``ticker`` as
    ``CorporateAction``s (type="dividend", ex_date = polygon ex_dividend_date,
    cash_amount + dividend_kind populated).

    Sibling of :func:`detect_dividends` (whole-market range scan) — the
    one-ticker, full-history fetch the offline CRSP-basis migration (PR7,
    config#1434) consumes to build the SEPARATE total-return series
    (``total_return_series``). Dividends are CRSP-separate: these actions are
    NEVER passed to a price-store ``apply`` (which raises on a dividend) —
    they feed only the total-return-axis math. Wraps
    ``polygon_client.get_dividends`` (returns ``{"ex_dividend_date",
    "cash_amount", "dividend_type"}`` dicts WITHOUT a ticker key); injects the
    ticker, constructs the client lazily, and DEGRADES GRACEFULLY to ``[]`` on
    any failure (apiKey scrubbed).
    """
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # import / construction failure — degrade
            from polygon_client import _scrub_api_key

            log.warning(
                "corporate_actions.get_dividends: could not obtain polygon "
                "client (%s) — returning no dividends for %s",
                _scrub_api_key(exc), ticker,
            )
            return []
    try:
        events = client.get_dividends(ticker)
    except Exception as exc:
        from polygon_client import _scrub_api_key

        log.warning(
            "corporate_actions.get_dividends: polygon dividend fetch failed "
            "for %s (%s) — returning no dividends",
            ticker, _scrub_api_key(exc),
        )
        return []
    return dividends_from_events([{**ev, "ticker": ticker} for ev in events])


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


def dividends_from_events(events: list[dict]) -> list["CorporateAction"]:
    """Map polygon ``get_recent_dividends`` event dicts to ``CorporateAction``s.

    Each event is ``{"ticker", "ex_dividend_date", "cash_amount",
    "dividend_type"}``. Pure transform (no I/O) so callers that already fetched
    the events can reuse them WITHOUT a second polygon call. Malformed rows
    (missing ticker / ex date / non-positive cash amount) are skipped — the same
    discipline ``polygon_client.get_recent_dividends`` applies, re-asserted here
    so a hand-built event list is also guarded.
    """
    actions: list[CorporateAction] = []
    for ev in events or []:
        ticker = ev.get("ticker")
        ex_date = ev.get("ex_dividend_date")
        cash = ev.get("cash_amount")
        if not ticker or not ex_date or cash is None:
            continue
        try:
            cash_f = float(cash)
        except (TypeError, ValueError):
            continue
        if cash_f <= 0:
            continue
        actions.append(
            CorporateAction.from_dividend(
                ticker=str(ticker),
                ex_date=str(ex_date),
                cash_amount=cash_f,
                dividend_kind=ev.get("dividend_type"),
                source="polygon",
                raw=dict(ev),
            )
        )
    return actions


def detect_dividends(
    start_date: str,
    end_date: str,
    *,
    client=None,
) -> list["CorporateAction"]:
    """Detect all cash dividends going ex in ``[start_date, end_date]`` as
    ``CorporateAction``s (type="dividend", ex_date = polygon ex_dividend_date,
    cash_amount + dividend_kind populated).

    Mirrors :func:`detect_splits`: wraps ``polygon_client.get_recent_dividends``
    (the whole-market, one-call range scan), constructs the client lazily, and
    DEGRADES GRACEFULLY to ``[]`` on any construction/fetch failure (apiKey
    scrubbed) — a dividend detection miss must never hard-fail the data
    pipeline. Dividends are RECORDED ONLY (CRSP-separate): the returned actions
    feed ``sync``'s registry capture, never a price-store ``apply``.
    """
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # import / construction failure — degrade
            from polygon_client import _scrub_api_key

            log.warning(
                "corporate_actions.detect_dividends: could not obtain polygon "
                "client (%s) — returning no detected dividends",
                _scrub_api_key(exc),
            )
            return []
    try:
        events = client.get_recent_dividends(start_date, end_date)
    except Exception as exc:
        from polygon_client import _scrub_api_key

        log.warning(
            "corporate_actions.detect_dividends: polygon dividend scan failed "
            "(%s) — returning no detected dividends",
            _scrub_api_key(exc),
        )
        return []
    return dividends_from_events(events)


def renames_from_events(candidate: str, events: list[dict]) -> list["CorporateAction"]:
    """Map polygon ticker-events rename pairs to rename ``CorporateAction``s for
    ONE candidate.

    ``events`` is :meth:`polygon_client.PolygonClient.get_ticker_events` output —
    ``[{"date", "old_ticker", "new_ticker"}]`` ascending. Emits a rename action
    for every pair whose ``old_ticker`` equals ``candidate`` (the candidate was
    renamed AWAY to ``new_ticker`` — the 1:1 identity-preserving remap this PR
    migrates). Pairs where the candidate is the NEW side (it is the survivor of
    some earlier rename, still live) are NOT emitted — only the symbol going
    missing triggers a migration off itself. Pure transform (no I/O); malformed
    or no-op (old == new) rows are skipped.
    """
    actions: list[CorporateAction] = []
    for ev in events or []:
        old_t = ev.get("old_ticker")
        new_t = ev.get("new_ticker")
        date = ev.get("date")
        if not old_t or not new_t or not date or old_t == new_t:
            continue
        if str(old_t) != str(candidate):
            continue
        actions.append(
            CorporateAction.from_rename(
                old_ticker=str(old_t),
                new_ticker=str(new_t),
                ex_date=str(date),
                source="polygon",
                raw=dict(ev),
            )
        )
    return actions


@dataclass
class RenameDetection:
    """Result of a prune-triggered ticker-rename scan over candidate tickers.

    ``renames`` — the detected old->new rename ``CorporateAction``s (one per
    candidate that polygon reports as the OLD side of a ``ticker_change``).

    ``failed_candidates`` — candidates whose ticker-events query RAISED (polygon
    unreachable / 5xx / unexpected, or client construction failed). The prune
    wiring MUST NOT prune these this pass: a detection OUTAGE must never delete a
    symbol that might be a rename. They are retried next pass.

    A candidate queried successfully with NO ``ticker_change`` is in NEITHER set
    — it is a CONFIRMED non-rename (genuine delist / merger-of-acquired) and is
    safe to prune as a delisting.
    """

    renames: list = field(default_factory=list)
    failed_candidates: set = field(default_factory=set)


def detect_renames(candidate_tickers, *, client=None) -> "RenameDetection":
    """Prune-triggered ticker-rename detection (corporate-actions PR6,
    config#1433).

    polygon has NO "all renames in a date range" endpoint — the ticker-events
    API is PER-TICKER — so detection is not a range scan. The natural signal is
    already in the pipeline: a symbol going MISSING from constituents /
    daily_closes (a prune candidate) is what flags it for examination. For each
    candidate we query :meth:`PolygonClient.get_ticker_events` and emit a rename
    when the candidate is the OLD side of a ``ticker_change`` (it was renamed
    away — an identity-preserving 1:1 remap whose ArcticDB history :func:
    `migrate_symbol` carries to the new key so the predictor stays continuous).

    A candidate with NO ticker_change is a genuine delist or a
    merger-OF-THE-ACQUIRED — NOT spliced (merging two companies' histories is
    poison for the predictor); it falls through to the existing prune path.

    HISTORY-SAFETY (the load-bearing failure semantics): detection is
    best-effort but its FAILURE is NEVER silently treated as "no rename". A
    candidate whose query RAISES is recorded in ``failed_candidates`` so the
    prune wiring SKIPS pruning it this pass — a polygon outage must not cause a
    renamed symbol's history to be wrongly deleted before it can be migrated.
    Only a SUCCESSFUL query that returns no rename confirms a candidate is safe
    to prune. Returns a :class:`RenameDetection`.
    """
    result = RenameDetection()
    candidates = [str(t) for t in (candidate_tickers or [])]
    if not candidates:
        return result
    if client is None:
        try:
            from polygon_client import polygon_client

            client = polygon_client()
        except Exception as exc:  # noqa: BLE001 - construction failure degrades
            # We cannot confirm ANY candidate is not a rename — treat EVERY
            # candidate as a detection failure (none confirmed safe to prune).
            log.warning(
                "corporate_actions.detect_renames: could not obtain polygon "
                "client (%s) — skipping rename detection for %d candidate(s); "
                "none confirmed safe to prune this pass (history-safety)",
                _scrub(exc), len(candidates),
            )
            result.failed_candidates.update(candidates)
            return result
    for cand in candidates:
        try:
            events = client.get_ticker_events(cand)
        except Exception as exc:  # noqa: BLE001 - per-candidate degrade + RECORD
            log.warning(
                "corporate_actions.detect_renames: ticker-events query failed "
                "for %s (%s) — NOT pruning this candidate this pass "
                "(history-safety)", cand, _scrub(exc),
            )
            result.failed_candidates.add(cand)
            continue
        result.renames.extend(renames_from_events(cand, events))
    return result


def migrate_symbol(
    universe_lib,
    old_ticker: str,
    new_ticker: str,
    *,
    registry: "CorporateActionRegistry",
    run_id: str,
    ex_date: str | None = None,
) -> bool:
    """Migrate ONE ArcticDB universe symbol ``old_ticker`` -> ``new_ticker`` for
    a 1:1 ticker rename (corporate-actions PR6, config#1433).

    Identity-preserving remap: read ``old_ticker``'s FULL series, write it
    VERBATIM under the ``new_ticker`` key, delete ``old_ticker`` — so the
    predictor keeps a CONTINUOUS history across the rename (e.g. FB's history
    lives under META). The frame is re-keyed AS READ (no re-projection): it was
    already written through the canonical chokepoint, and a pure remap must not
    transform the data it carries.

    EXACTLY-ONCE via the registry ``arcticdb_universe`` applied marker, keyed on
    the rename ``action_id``. WRITE-THEN-MARK: the marker is written ONLY after
    the new key is written AND the old key deleted, so a mid-migration failure
    never leaves a false "applied" marker (a re-run re-attempts cleanly).
    Idempotent: a second call sees ``is_applied`` True and returns ``False``
    without touching ArcticDB. ``ex_date`` makes the marker id match the detected
    rename's ``action_id`` (the prune wiring passes the detected action's
    ex_date); a standalone call may omit it.

    NEW-ALREADY-EXISTS (safe choice, documented): if ``new_ticker`` already
    holds its OWN history, we do NOT overwrite or merge it — merging two real
    histories is exactly the splice this program forbids (a live ``new_ticker``
    series is already maintained by ``daily_append``; the orphaned ``old_ticker``
    series is the SAME entity pre-rename and is now superseded). We log a WARN,
    delete the orphaned ``old_ticker`` key so it cannot linger as a phantom
    constituent, mark applied, and return ``True``.

    Returns ``True`` if a migration (or its idempotent new-exists cleanup) ran
    this call, ``False`` if it was already applied.
    """
    action = CorporateAction.from_rename(
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        ex_date=str(ex_date) if ex_date is not None else "",
    )
    if registry is not None and registry.is_applied(STORE_ARCTICDB_UNIVERSE, action.action_id):
        log.info(
            "corporate_actions.migrate_symbol: %s -> %s already applied "
            "(action_id=%s) — noop", old_ticker, new_ticker, action.action_id,
        )
        return False

    # Record the rename action for provenance (write-if-absent; harmless if the
    # prune wiring already recorded it).
    if registry is not None:
        try:
            registry.record_detected(action, run_id=run_id)
        except Exception as exc:  # noqa: BLE001 - provenance best-effort
            log.warning(
                "corporate_actions.migrate_symbol: record_detected failed for "
                "%s (%s)", action.action_id, _scrub(exc),
            )

    # Source must exist to migrate. If the old key is gone, there is nothing to
    # carry — treat as a no-op success (a prior run may have migrated + deleted).
    if not universe_lib.has_symbol(old_ticker):
        log.warning(
            "corporate_actions.migrate_symbol: %s not in ArcticDB universe — "
            "nothing to migrate to %s", old_ticker, new_ticker,
        )
        return False

    if universe_lib.has_symbol(new_ticker):
        # new already has its own live history — DO NOT splice. Drop the orphaned
        # old key so it can't masquerade as a phantom constituent.
        log.warning(
            "corporate_actions.migrate_symbol: %s already has ArcticDB history "
            "— NOT overwriting/merging (no splice); deleting orphaned source %s "
            "(superseded by the live %s series)", new_ticker, old_ticker, new_ticker,
        )
        universe_lib.delete(old_ticker)
        if registry is not None:
            registry.mark_applied(action, STORE_ARCTICDB_UNIVERSE, run_id=run_id)
        return True

    df = universe_lib.read(old_ticker).data
    # WRITE-THEN-(DELETE)-THEN-MARK: new key written first, old deleted, marker
    # last — a failure anywhere before the mark leaves no false "applied" record.
    universe_lib.write(new_ticker, df, prune_previous_versions=True)
    universe_lib.delete(old_ticker)
    if registry is not None:
        registry.mark_applied(action, STORE_ARCTICDB_UNIVERSE, run_id=run_id)
    log.warning(
        "corporate_actions.migrate_symbol: MIGRATED %s -> %s (%d rows) "
        "action_id=%s", old_ticker, new_ticker, len(df), action.action_id,
    )
    return True
