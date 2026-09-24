"""The per-key parity diff: shadow output vs the same trading day's v1 output.

Plan `data_collection_plan_260914.md` §6.2 step 4, `alpha-engine-config-I10778`.
The report this produces is the `data-cutover-ready` gate's `parity` clause
evidence (`data_gate/evidence.py::read_parity`, `alpha-engine-config-I10777`).

**What is compared, per plan §6.2 step 4:** row count, symbol set, schema,
value tolerance. Bytes are deliberately NOT the test — a parquet file
round-tripped through a different pyarrow build differs byte-for-byte while
being the same table, and a report that reds on that teaches everyone to ignore
it.

**What is compared is DECLARED, not discovered.** The key list comes from
`registry.d/units/*.yaml` — the same descriptors the board is generated from —
so a unit nobody registered is missing from the parity report for exactly the
reason it is missing from the board, and `data.inventory.writers_declared` is
the clause that catches it. Nothing here hand-lists a key.

**Nothing is silently dropped.** Every declared write entry produces a row.
A write entry this tool cannot turn into an S3 key — an ArcticDB library, a
prose placeholder such as ``research.db::universe_returns``, a foreign store —
produces an ``unmeasurable`` row naming *why*, and an unmeasurable row is never
met. The alternative (skipping what we cannot diff) makes the report's
denominator the set of easy keys.

**Three things cannot be diffed from the laptop** and are recorded as such:

* ArcticDB libraries (``arcticdb/universe`` and friends). ArcticDB is
  unreadable from this laptop at all (`alpha-engine-config-I9771`) and this
  tool never opens it; comparing a shadow library against a live one is an
  in-region job. Verdict ``in_region_only``.
* Anything under a bucket the laptop identity cannot read.
* A shadow run's own ArcticDB output, for the same reason.

Run the tool in-region (the EOD box, off-market hours) with
``--include-arcticdb`` once the in-region ArcticDB comparator lands; until
then those rows are honestly unmeasurable rather than quietly green.
"""

from __future__ import annotations

import datetime as dt
import functools
import hashlib
import io
import json
import math
import pathlib
import re
import string
from dataclasses import dataclass, field
from typing import Any, Iterable

from data_gate import evidence
from data_gate.descriptors import Unit, load_units
from shadow.root import LIVE_ARCTIC_LIBRARIES, ShadowRoot

__all__ = [
    "DEFAULT_ABSOLUTE_TOLERANCE",
    "DEFAULT_RELATIVE_TOLERANCE",
    "PARITY_KEY_TEMPLATE",
    "PARITY_SCHEMA_VERSION",
    "SETTLING_BASES",
    "STAGING_PREFIX_RETENTION_DAYS",
    "ContractSchema",
    "KeyResult",
    "ParityReport",
    "compare_bytes",
    "LEGS_GROUPS",
    "expand_writes",
    "grade_prior_day_settled",
    "key_names_trading_day",
    "merge_legs",
    "parity_key",
    "rebased_key_date_key",
    "resolve_contract",
    "run_parity",
    "settling_basis",
]

#: Where the report is published, relative to the `data_collection` store root
#: — i.e. `s3://alpha-engine-research/data_collection/parity/{trading_day}.json`.
#: The constant is DEFINED by the consumer (`data_gate.evidence`) and imported
#: here, not re-spelled: the gate declares where it reads, and the producer
#: writes there by construction rather than by two files agreeing.
PARITY_KEY_TEMPLATE = evidence.PARITY_KEY_TEMPLATE

#: v2 (`alpha-engine-config-I11351`/`-I11352`) adds the `settling_bar` block and
#: `prior_day_settled` / `summary.settling_bar_keys`, and turns `legs_known`
#: from a bare boolean into one boolean PER DISPATCH GROUP. `data_gate.evidence
#: .read_parity` accepts both versions — the reports the cutover gate reads
#: today were published under v1 and are not invalidated by the bump.
PARITY_SCHEMA_VERSION = "data_parity_report.v2"

#: The dispatch groups a `legs` entry may belong to. `sameday` is the 18:30 ET
#: post-market dispatch on day D; `morning` is the 07:45 ET D+1 dispatch that
#: runs v1's two morning legs for the PREVIOUS session and rewrites the same
#: report (`alpha-engine-config-I11352`).
LEGS_GROUPS: tuple[str, ...] = ("sameday", "morning")
DEFAULT_LEGS_GROUP = "sameday"

#: Value tolerance. Relative by default because prices, ratios and z-scores
#: share a report; the absolute floor exists so a value near zero does not red
#: on float noise.
DEFAULT_RELATIVE_TOLERANCE = 1e-6
DEFAULT_ABSOLUTE_TOLERANCE = 1e-9

#: Column names that identify a row. Checked in order; the first present wins.
_SYMBOL_COLUMNS = ("symbol", "ticker", "Symbol", "Ticker", "sym")

#: Lifecycles whose unit has a live v1 producer today. The issue's closes-when
#: is "every key in the §3 boundary table with a live v1 producer"; a retired,
#: disabled, deprecated or not-yet-built unit has nothing to diff against, and
#: including it would red the report for a reason that is not a parity failure.
#: Every excluded unit is NAMED in the report, so the exclusion is visible.
LIVE_LIFECYCLES: frozenset[str] = frozenset({"in-service"})

#: Units excluded for a reason other than lifecycle, each with its reason. A
#: dict rather than a filter expression so the exclusion list is reviewable.
EXCLUDED_UNITS: dict[str, str] = {
    "D47": (
        "writes the crucible-v2 store, not this component's bucket — it is v2's own "
        "producer (plan §3), and has no v1 counterpart to diff against"
    ),
}

_FORMATTER = string.Formatter()

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONTRACTS_DIR = _REPO_ROOT / "contracts"

#: A `{date}`-style placeholder inside a contract's own `x-key-pattern`. Never
#: crosses a `/` — every key template in this repo names one path segment.
_KEY_PATTERN_PLACEHOLDER_RE = re.compile(r"\{[A-Za-z0-9_]+\}")


def parity_key(trading_day: dt.date) -> str:
    return PARITY_KEY_TEMPLATE.format(trading_day=trading_day.isoformat())


# ---------------------------------------------------------------------------
# Contract resolution (alpha-engine-config-I10894): which fields of a key are
# DATA versus PROVENANCE, declared in the contract, never hand-listed here.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContractSchema:
    """One `contracts/*.schema.json` that declares the key it documents."""

    path: pathlib.Path
    provenance_fields: frozenset[str]
    pattern: "re.Pattern[str]"
    #: ``"deterministic"`` (the default, byte-equal) or ``"vendor_live"``.
    #: Brian ruling 2026-09-20, `alpha-engine-config-I11203`.
    comparison_class: str = "deterministic"
    #: For a `vendor_live` key: the band inside which a value difference is
    #: DRIFT rather than a breach, and the key-set coverage floor.
    value_band_relative: float = 0.0
    value_band_absolute: float = 0.0
    coverage_floor: float = 1.0
    #: ``""`` (this key has no settling bar), or one of :data:`SETTLING_BASES`.
    #: Declared by the contract's own ``x-settling-bar`` block, never
    #: hand-listed here — the same rule every other per-key fact in this module
    #: follows (`alpha-engine-config-I11351`).
    settling_basis: str = ""

    @property
    def is_vendor_live(self) -> bool:
        return self.comparison_class == "vendor_live"


def _key_pattern_regex(template: str) -> "re.Pattern[str]":
    parts: list[str] = []
    pos = 0
    for match in _KEY_PATTERN_PLACEHOLDER_RE.finditer(template):
        parts.append(re.escape(template[pos : match.start()]))
        parts.append(r"[^/]+")
        pos = match.end()
    parts.append(re.escape(template[pos:]))
    return re.compile("^" + "".join(parts) + "$")


def _provenance_fields(schema: dict[str, Any]) -> frozenset[str]:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return frozenset()
    return frozenset(
        name
        for name, prop in properties.items()
        if isinstance(prop, dict) and prop.get("x-provenance") is True
    )


@functools.lru_cache(maxsize=1)
def _load_contract_schemas() -> tuple[ContractSchema, ...]:
    """Every contract that declares an `x-key-pattern` for the key it documents.

    **Declared, not discovered** — the same rule `expand_writes` states for
    write targets. The key-to-schema mapping lives in the contract file
    itself (`x-key-pattern`), so this module never hand-lists a key or a
    schema filename (I10894 deliverable 2). A contract with no
    `x-key-pattern` is simply not resolvable from a key — it is not an error,
    since most contracts here are consumer-pinned by code path, not by S3 key.
    """
    schemas: list[ContractSchema] = []
    if not _CONTRACTS_DIR.is_dir():
        return tuple(schemas)
    for path in sorted(_CONTRACTS_DIR.glob("*.schema.json")):
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        declared = schema.get("x-key-pattern")
        if not declared:
            continue
        # A contract may document SEVERAL keys — `metron_closes` covers both
        # `market_data/eod_closes/{date}.json` and its `latest.json` sidecar,
        # which are the same artifact under two names. A string stays a string
        # (alpha-engine-config-I11203); a list yields one ContractSchema per
        # pattern so `resolve_contract`'s first-match loop is unchanged.
        templates = [declared] if isinstance(declared, str) else list(declared)
        provenance = _provenance_fields(schema)
        comparison_class = str(schema.get("x-comparison-class") or "deterministic")
        if comparison_class not in _COMPARISON_CLASSES:
            raise ValueError(
                f"{path.name}: x-comparison-class must be one of {sorted(_COMPARISON_CLASSES)}, "
                f"got {comparison_class!r}"
            )
        vendor_live = schema.get("x-vendor-live") or {}
        if comparison_class == "vendor_live" and not vendor_live:
            raise ValueError(
                f"{path.name}: x-comparison-class is 'vendor_live' but no x-vendor-live block "
                "declares its value band and coverage floor. A class with no declared bounds "
                "would pass everything (alpha-engine-config-I11203)."
            )
        band = vendor_live.get("value_band") or {}
        settling = schema.get("x-settling-bar") or {}
        settling_basis = str(settling.get("basis") or "")
        if settling and settling_basis not in SETTLING_BASES:
            raise ValueError(
                f"{path.name}: x-settling-bar.basis must be one of {sorted(SETTLING_BASES)}, "
                f"got {settling_basis!r}. A settling-bar declaration with no basis would "
                "silently grade nothing, which is the state I11351 exists to end."
            )
        if settling and not str(settling.get("rationale") or "").strip():
            raise ValueError(
                f"{path.name}: x-settling-bar needs a `rationale` naming WHY this key's "
                "trading-day cells are a vendor-state measurement rather than a producer "
                "defect. A forgiveness with no written reason is a widened band wearing a "
                "different name (alpha-engine-config-I11351)."
            )
        for template in templates:
            if not isinstance(template, str) or not template:
                raise ValueError(
                    f"{path.name}: x-key-pattern entries must be non-empty strings, got {template!r}"
                )
            schemas.append(
                ContractSchema(
                    path,
                    provenance,
                    _key_pattern_regex(template),
                    comparison_class=comparison_class,
                    value_band_relative=float(band.get("relative", 0.0)),
                    value_band_absolute=float(band.get("absolute", 0.0)),
                    coverage_floor=float(vendor_live.get("coverage_floor", 1.0)),
                    settling_basis=settling_basis,
                )
            )
    return tuple(schemas)


#: How a contract may declare WHICH cells of its key are the trading day's own,
#: still-settling bar (`alpha-engine-config-I11351`). Three bases, because the
#: three shapes this repo publishes carry the date in three different places:
#:
#: * ``row_date`` — the artifact is INDEXED BY DATE and holds history. Only the
#:   row whose index is the report's `trading_day` is settling
#:   (`reference/price_cache/{sym}.parquet`).
#: * ``key_date`` — the artifact is indexed by something else (ticker) and the
#:   KEY names one trading day, so the whole artifact IS that day's bar. It is
#:   settling only when the key names the report's own trading day
#:   (`staging/daily_closes/{date}.parquet`, `features/{date}/*.parquet`,
#:   `market_data/eod_closes/{date}.json`).
#: * ``same_day_snapshot`` — an undated, overwritten-in-place `latest`-style
#:   artifact whose whole content is a same-day derivation of the session's bar
#:   (`market_data/technicals/latest.json`).
#:
#: This is NOT a wider tolerance and it never touches `x-vendor-live.band`: the
#: trading-day cells are a different measurement (two fetches of a number the
#: vendor is still revising), so they get their own named surface and are
#: excluded from `values.breaches` — while membership, schema, row count and
#: coverage stay strict for every row INCLUDING the trading day.
SETTLING_BASES: frozenset[str] = frozenset({"row_date", "key_date", "same_day_snapshot"})

#: The lifecycle rule both operands of the `key_date` D-1 re-grade sit under
#: (`alpha-engine-config-I11360`), verified LIVE against the bucket on
#: 2026-09-22 (`aws s3api get-bucket-lifecycle-configuration --bucket
#: alpha-engine-research`), not merely read off the IaC declaration:
#: `expire-staging-after-7-days`, `Filter.Prefix: "staging/"`,
#: `Expiration.Days: 7`, no narrower override for `staging/shadow/`. The live
#: key literally sits under `staging/` (`staging/daily_closes/{date}.parquet`);
#: the shadow key does too, because `shadow.root.SHADOW_ROOT_TEMPLATE` is
#: `staging/shadow/{trading_day}/` — so BOTH expire together, 7 days after
#: they were written on D-1. A scheduled report for D reads that pair at most
#: a few calendar days later (D's own report publishes same-day or the next
#: morning); `tests/test_shadow_parity.py
#: ::test_prior_day_key_date_regrade_reads_inside_the_staging_retention_window`
#: pins the largest observed trading-day gap (4 calendar days, 2020-2029) well
#: inside this window rather than assuming it.
STAGING_PREFIX_RETENTION_DAYS = 7

#: An ISO date appearing as a whole path segment or as a filename stem.
#: `staging/daily_closes/2026-09-21.parquet`, `features/2026-09-21/x.parquet`
#: and `market_data/eod_closes/2026-09-21.json` all match; a key that merely
#: contains the digits inside a longer token does not.
_KEY_DATE_RE = re.compile(r"(?:^|/)(\d{4}-\d{2}-\d{2})(?:/|\.|$)")


def key_names_trading_day(live_key: str, trading_day: dt.date) -> bool:
    """Whether ``live_key`` names ``trading_day`` as a path segment or stem."""
    return any(found == trading_day.isoformat() for found in _KEY_DATE_RE.findall(live_key))


def rebased_key_date_key(key: str, from_date: dt.date, to_date: dt.date) -> str | None:
    """``key`` with its ``from_date`` path segment swapped for ``to_date``.

    `alpha-engine-config-I11360`. Used in both directions: D's key to D-1's
    (to fetch the D-1 pair for the re-grade) and D-1's key back to D's (to
    find, on TODAY's report, the row the re-grade result was attached to,
    when reading YESTERDAY's report in :func:`grade_prior_day_settled`).

    ``None`` when ``from_date`` does not appear as exactly one whole path
    segment — never guessed. A `key_date` key is only ever passed here after
    :func:`key_names_trading_day` already confirmed one match against the
    matching date, so `None` here means the caller's precondition did not
    hold, not that no rewrite was possible.
    """
    iso = from_date.isoformat()
    starts = [m.start(1) for m in _KEY_DATE_RE.finditer(key) if m.group(1) == iso]
    if len(starts) != 1:
        return None
    start = starts[0]
    return key[:start] + to_date.isoformat() + key[start + len(iso) :]


def settling_basis(
    live_key: str, contract: "ContractSchema | None", trading_day: "dt.date | None"
) -> str:
    """The settling basis in force for this key on this report, or ``""``.

    ``""`` — the red default — for a key whose contract declares nothing, for a
    comparison run without a trading day, and for a `key_date` key whose own
    key names some OTHER day (`features/metron_supplemental/`, or yesterday's
    `staging/daily_closes` seen from today's report). Nothing is forgiven by
    default; the forgiveness has to be declared AND applicable.
    """
    if contract is None or trading_day is None or not contract.settling_basis:
        return ""
    if contract.settling_basis == "key_date" and not key_names_trading_day(live_key, trading_day):
        return ""
    return contract.settling_basis


def _relative_difference(live: Any, shadow: Any) -> float | None:
    """``|live - shadow| / |live|`` for two numbers, else ``None``.

    ``None`` — not ``0.0`` and not ``inf`` — when the pair is non-numeric or
    the live side is zero. A substituted number here would be read as a
    measured drift, and `max_rel_*` is published precisely so a reader can
    judge how far the unsettled bar moved.
    """
    try:
        a, b = float(live), float(shadow)
    except (TypeError, ValueError):
        return None
    if math.isnan(a) or math.isnan(b) or a == 0.0:
        return None
    return abs(a - b) / abs(a)


class _SettlingCollector:
    """The trading-day cells of one key, and how far they moved.

    Deliberately NOT a counter on the breach path: a settling cell is recorded
    here and never reaches `values.breaches`, so the two numbers can be added
    up by a reader without double-counting, and a key with only settling cells
    grades `match` while still printing what it absorbed.
    """

    def __init__(self, basis: str, date: "dt.date | None") -> None:
        self.basis = basis
        self.date = date.isoformat() if date is not None else None
        self.cells = 0
        self.examples: list[dict[str, Any]] = []
        #: ``None``, never ``0.0``, until something measurable is recorded — a
        #: zero here would read as "measured, and it did not move" when the
        #: truth is "nothing comparable was measured" (principle 7).
        self.max_rel: float | None = None
        self.max_rel_close: float | None = None
        self.max_rel_volume: float | None = None

    def _note_move(self, column: Any, rel: float | None) -> None:
        if rel is None:
            return
        self.max_rel = rel if self.max_rel is None else max(self.max_rel, rel)
        name = "" if column is None else str(column).lower()
        if "close" in name:
            self.max_rel_close = rel if self.max_rel_close is None else max(self.max_rel_close, rel)
        if "volume" in name:
            self.max_rel_volume = (
                rel if self.max_rel_volume is None else max(self.max_rel_volume, rel)
            )

    def record(self, *, row: Any, column: Any, live: Any, shadow: Any) -> None:
        """One settling CELL of a frame, with both sides in hand."""
        self.cells += 1
        if len(self.examples) < 10:
            self.examples.append(
                {
                    "row": None if row is None else str(row),
                    "column": None if column is None else str(column),
                    "live": _jsonable(live),
                    "shadow": _jsonable(shadow),
                }
            )
        self._note_move(column, _relative_difference(live, shadow))

    def record_json(self, diff: "JsonDiff") -> None:
        """One settling VALUE diff of a JSON document.

        The walker renders a value diff to a string rather than carrying the
        two sides, so `max_rel*` cannot be computed from it and stay ``None``
        — the rendered pair is in `examples`, and a fabricated 0.0 would be
        worse than an honest absence.
        """
        self.cells += 1
        if len(self.examples) < 10:
            self.examples.append({"path": diff.path, "rendered": diff.rendered})

    def as_block(self) -> dict[str, Any]:
        return {
            "basis": self.basis,
            "date": self.date,
            "cells": self.cells,
            "examples": self.examples,
            "max_rel": self.max_rel,
            "max_rel_close": self.max_rel_close,
            "max_rel_volume": self.max_rel_volume,
            "note": (
                "cells on the report's own trading day, whose bar the vendor was still "
                "settling when the two sides fetched it. Recorded, never counted in "
                "values.breaches, and re-graded strictly on the next day's report "
                "(prior_day_settled). Membership, schema, row count and coverage stay "
                "strict for this row (alpha-engine-config-I11351)."
            ),
        }


def resolve_contract(live_key: str) -> ContractSchema | None:
    """The contract documenting `live_key`, or ``None``.

    ``None`` is the honest default for a key with no declared contract: every
    field of it compares as data — "a key with no contract resolves every
    field as data. That is the red default" (I10894 deliverable 2).
    """
    for contract in _load_contract_schemas():
        if contract.pattern.fullmatch(live_key):
            return contract
    return None


# ---------------------------------------------------------------------------
# Turning declared write entries into things that can be compared
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WriteTarget:
    """One declared write entry, classified."""

    unit_id: str
    declared: str
    #: "key" (a concrete S3 key), "prefix" (list-and-diff), "arcticdb", or
    #: "undiffable" (with `reason` set).
    kind: str
    value: str = ""
    reason: str = ""


def _placeholders(template: str) -> list[str]:
    return [f for _, f, _, _ in _FORMATTER.parse(template) if f]


def classify_write(unit_id: str, declared: str, trading_day: dt.date) -> WriteTarget:
    """Classify one ``writes[]`` entry. Never returns ``None`` — see module docstring."""
    text = declared.strip()
    arctic = re.match(r"^arcticdb/([A-Za-z0-9_]+)\b", text)
    if arctic:
        return WriteTarget(unit_id, declared, "arcticdb", value=arctic.group(1))
    # A second ArcticDB spelling: `<library>::{symbol}` (D14's
    # `delisted_history::{ticker}`) — a library/symbol reference, not the
    # `arcticdb/<library>` prefix form above. Gated on `LIVE_ARCTIC_LIBRARIES`
    # (the same registry `shadow.root` uses to redirect writes) so this can
    # never mistake a genuine prose pointer — `research.db::score_performance`
    # (D09), a SQLite table inside a single-file DB, not an ArcticDB library —
    # for a diffable one; `research.db` is not in that set.
    lib_symbol = re.match(r"^([A-Za-z0-9_]+)::\{[A-Za-z0-9_]+\}$", text)
    if lib_symbol and lib_symbol.group(1) in LIVE_ARCTIC_LIBRARIES:
        return WriteTarget(unit_id, declared, "arcticdb", value=lib_symbol.group(1))
    if "::" in text or "(" in text or " " in text:
        return WriteTarget(
            unit_id,
            declared,
            "undiffable",
            reason=(
                "the descriptor declares this write in prose, not as an S3 key template "
                "— there is nothing to address. Give the descriptor a key template, or a "
                "retirement decision (plan §3 'a key with no surviving consumer')."
            ),
        )
    fields = _placeholders(text)
    unresolved = [f for f in fields if f not in {"date", "trading_day"}]
    rendered = text.replace("{date}", trading_day.isoformat()).replace(
        "{trading_day}", trading_day.isoformat()
    )
    if unresolved or "*" in rendered or rendered.endswith("/"):
        prefix = rendered.split("{")[0].split("*")[0]
        if "/" not in prefix.rstrip("/") and not prefix.endswith("/"):
            return WriteTarget(
                unit_id,
                declared,
                "undiffable",
                reason=f"no listable prefix could be derived from {declared!r}",
            )
        return WriteTarget(unit_id, declared, "prefix", value=prefix)
    return WriteTarget(unit_id, declared, "key", value=rendered)


def expand_writes(units: Iterable[Unit], trading_day: dt.date) -> list[WriteTarget]:
    """Every write entry of every live-v1-producer unit, classified and deduped."""
    targets: dict[tuple[str, str], WriteTarget] = {}
    for unit in units:
        if unit.unit_id in EXCLUDED_UNITS:
            continue
        if str(unit.raw.get("lifecycle")) not in LIVE_LIFECYCLES:
            continue
        for declared in unit.raw.get("writes") or []:
            target = classify_write(unit.unit_id, str(declared), trading_day)
            existing = targets.get((target.kind, target.value or target.declared))
            if existing is None:
                targets[(target.kind, target.value or target.declared)] = target
            elif target.unit_id not in existing.unit_id.split(","):
                # Two units writing the SAME key (D17/D19 on staging/daily_closes,
                # plan §2 row 5) is one comparison, attributed to both.
                targets[(target.kind, target.value or target.declared)] = WriteTarget(
                    f"{existing.unit_id},{target.unit_id}",
                    existing.declared,
                    existing.kind,
                    existing.value,
                    existing.reason,
                )
    return sorted(targets.values(), key=lambda t: (t.kind, t.value, t.unit_id))


# ---------------------------------------------------------------------------
# Comparators
# ---------------------------------------------------------------------------


def _symbol_column(frame) -> str | None:
    for candidate in _SYMBOL_COLUMNS:
        if candidate in frame.columns:
            return candidate
    if frame.index.name in _SYMBOL_COLUMNS:
        return None  # handled by the index path below
    return None


def _numeric_close(a: Any, b: Any, rel: float, absolute: float) -> bool:
    try:
        af, bf = float(a), float(b)
    except (TypeError, ValueError):
        return a == b
    if math.isnan(af) and math.isnan(bf):
        return True
    return math.isclose(af, bf, rel_tol=rel, abs_tol=absolute)


def _row_is_day(row_key: Any, iso: str) -> bool:
    """Whether a frame's row key IS the trading day ``iso``.

    The index of a price-cache frame is a pandas Timestamp, whose ``str()`` is
    ``'2026-09-21 00:00:00'``; a date index stringifies as ``'2026-09-21'``.
    Both are matched by comparing the first ten characters, which is also why
    a non-date row key (a ticker) can never accidentally match.
    """
    return str(row_key)[:10] == iso


def _compare_frames(
    live,
    shadow,
    rel: float,
    absolute: float,
    provenance_columns: frozenset[str] = frozenset(),
    settling: "_SettlingCollector | None" = None,
    settling_whole_frame: bool = False,
    prior_day: "dt.date | None" = None,
) -> dict[str, Any]:
    import pandas as pd  # local: pandas import is ~1s and the CLI may never need it

    out: dict[str, Any] = {}
    out["row_count"] = {"live": int(len(live)), "shadow": int(len(shadow))}

    live_cols = {str(c): str(t) for c, t in live.dtypes.items()}
    shadow_cols = {str(c): str(t) for c, t in shadow.dtypes.items()}
    dtype_changes = {
        c: {"live": live_cols[c], "shadow": shadow_cols[c]}
        for c in sorted(set(live_cols) & set(shadow_cols))
        if live_cols[c] != shadow_cols[c]
    }
    out["schema"] = {
        "match": live_cols == shadow_cols,
        "only_live": sorted(set(live_cols) - set(shadow_cols)),
        "only_shadow": sorted(set(shadow_cols) - set(live_cols)),
        "dtype_changes": dtype_changes,
    }

    # Row keys are kept as their NATIVE values, not stringified. Stringifying
    # them and then indexing with the string raises KeyError on a datetime
    # index, and the only place to put that KeyError is a `continue` — a silent
    # swallow that would report `compared_cells: 0` as a clean parity pass.
    column = _symbol_column(live)
    if column is not None:
        live_values = live[column].tolist()
        indexed_live = live.set_index(column)
        if column in shadow.columns:
            shadow_values = shadow[column].tolist()
            indexed_shadow = shadow.set_index(column)
        else:
            shadow_values, indexed_shadow = [], None
    else:
        live_values, shadow_values = live.index.tolist(), shadow.index.tolist()
        indexed_live, indexed_shadow = live, shadow
    live_keys, shadow_keys = set(live_values), set(shadow_values)
    out["symbol_set"] = {
        "basis": column or f"index:{live.index.name}",
        "live": len(live_keys),
        "shadow": len(shadow_keys),
        "only_live": sorted(map(str, live_keys - shadow_keys))[:50],
        "only_shadow": sorted(map(str, shadow_keys - live_keys))[:50],
    }

    compared = 0
    # Counted and collected separately: the EXAMPLES are capped so the report
    # stays a readable size, but the COUNT is not. A capped count would report
    # "50 breaches" over a key where every row drifted, and a parity number that
    # saturates is a parity number nobody can act on.
    breach_count = 0
    breaches: list[dict[str, Any]] = []
    provenance_diff_count = 0
    provenance_diffs: list[dict[str, Any]] = []
    if indexed_shadow is None:
        # The shadow side has no identifier column at all; the schema block
        # above already carries that as a mismatch, and comparing positionally
        # would invent an alignment nobody declared.
        out["values"] = {
            "compared_cells": 0,
            "breaches": 1,
            "examples": [{"reason": f"the shadow side has no {column!r} column to align on"}],
        }
        out["provenance_diffs"] = {"count": 0, "examples": []}
        return out
    settling_iso = settling.date if settling is not None else None
    prior_iso = prior_day.isoformat() if prior_day is not None else None
    prior_rows = 0
    prior_breaches = 0
    shared_columns = [c for c in indexed_live.columns if c in indexed_shadow.columns]
    # Provenance columns (I10894) are declared by the contract, never
    # hand-listed here — a column absent from `provenance_columns` is DATA,
    # the red default for a key with no contract or an undeclared field.
    data_columns = [c for c in shared_columns if c not in provenance_columns]
    prov_columns = [c for c in shared_columns if c in provenance_columns]
    for row_key in sorted(live_keys & shadow_keys, key=str):
        live_row = indexed_live.loc[row_key]
        shadow_row = indexed_shadow.loc[row_key]
        if isinstance(live_row, pd.DataFrame) or isinstance(shadow_row, pd.DataFrame):
            # A duplicated row key. Not comparable cell-by-cell, and a silent
            # `.iloc[0]` would compare arbitrary rows — record it as a breach.
            breach_count += 1
            if len(breaches) < 50:
                breaches.append(
                    {"row": str(row_key), "column": None, "reason": "duplicate row key"}
                )
            continue
        # A row is SETTLING when the whole frame is the trading day's bar
        # (`key_date`/`same_day_snapshot` — the frame is ticker-indexed and the
        # KEY carries the date) or when this row's own index IS that date
        # (`row_date`). Everything else keeps the strict band.
        row_settling = settling is not None and (
            settling_whole_frame or (settling_iso is not None and _row_is_day(row_key, settling_iso))
        )
        row_is_prior = prior_iso is not None and _row_is_day(row_key, prior_iso)
        if row_is_prior:
            prior_rows += 1
        for col in data_columns:
            compared += 1
            if _numeric_close(live_row[col], shadow_row[col], rel, absolute):
                continue
            if row_settling:
                # Recorded, NOT a breach. The membership/schema/row-count
                # checks above already ran over this same row unchanged.
                settling.record(
                    row=row_key, column=col, live=live_row[col], shadow=shadow_row[col]
                )
                continue
            if row_is_prior:
                prior_breaches += 1
            breach_count += 1
            if len(breaches) < 50:
                breaches.append(
                    {
                        "row": str(row_key),
                        "column": str(col),
                        "live": _jsonable(live_row[col]),
                        "shadow": _jsonable(shadow_row[col]),
                    }
                )
        for col in prov_columns:
            if not _numeric_close(live_row[col], shadow_row[col], rel, absolute):
                provenance_diff_count += 1
                if len(provenance_diffs) < 50:
                    provenance_diffs.append(
                        {
                            "row": str(row_key),
                            "column": str(col),
                            "live": _jsonable(live_row[col]),
                            "shadow": _jsonable(shadow_row[col]),
                        }
                    )
    out["values"] = {
        "compared_cells": compared,
        "breaches": breach_count,
        "examples": breaches[:10],
    }
    out["provenance_diffs"] = {"count": provenance_diff_count, "examples": provenance_diffs[:10]}
    if prior_iso is not None:
        # Deliverable 2: the row this key's PREVIOUS report could only record
        # as settling is, on this report, an ordinary historical row graded on
        # the strict band. Counted here rather than re-derived from the capped
        # `examples` list, which saturates at 50 and would silently read as
        # "settled" on a key with more breaches than that.
        out["prior_day"] = {
            "date": prior_iso,
            "rows_compared": prior_rows,
            "breaches": prior_breaches,
            "settled": prior_rows > 0 and prior_breaches == 0,
        }
    return out


def _jsonable(value: Any) -> Any:
    """A JSON-serialisable rendering of one cell.

    ``float(np.float64)`` matters here: numpy scalars pass ``isinstance(...,
    float)`` for ``np.float64`` but are NOT serialisable, and ``np.int64``
    fails the int check outright. A report that raises at ``json.dumps`` after
    the whole comparison has run is the worst possible place to find that out.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, float):
        native = float(value)
        return "NaN" if math.isnan(native) else native
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str):
        return value
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _jsonable(item())
        except (ValueError, TypeError):
            # Deliberate and narrow: (a) the swallowed failure is "this numpy
            # object is not a scalar"; (b) the report survives intact — only
            # this one cell's rendering degrades; (c) it is recorded on the
            # surface that matters, as the repr below, inside the report.
            pass
    return str(value)


@dataclass(frozen=True)
class JsonDiff:
    """One difference between two JSON documents, with the STRUCTURE that
    produced it — not only its rendered string.

    ``_json_breaches`` used to return rendered strings and everything
    downstream re-derived meaning from them with regexes
    (``_MEMBERSHIP_DIFF_RE``, ``_CARDINALITY_DIFF_RE``). That cost a measured
    defect on 2026-09-20: coverage was graded against
    ``row_count.live`` — the TOP-LEVEL key count of the document, 3 for
    ``{schema_version, as_of, earnings}`` — while the membership diffs it
    subtracted lived at ``$.earnings.<ticker>``, ~900 of them. Sixteen missing
    tickers out of three "rows" produced ``covered = max(3 - 16, 0) = 0`` and a
    coverage ratio of 0.0, so `market_data/earnings/latest.json` and
    `market_data/sectors/latest.json` failed with ZERO value breaches, purely on
    a nonsense denominator.

    The walker already knows the container a membership diff came out of, so it
    now says so. ``parent_size_live`` is the size of the live container at
    ``parent_path``: the only correct denominator for "how much of what v1
    published did the shadow reproduce".
    """

    path: str
    kind: str  # "membership" | "cardinality" | "value"
    rendered: str
    side: str = ""  # membership only: "live" (missing from shadow) | "shadow" (extra)
    parent_path: str = ""  # membership only
    parent_size_live: int = 0  # membership only


def _json_diffs(
    live: Any, shadow: Any, rel: float, absolute: float, path: str = "$"
) -> list[JsonDiff]:
    """Every difference between two JSON documents, structurally classified."""
    if isinstance(live, dict) and isinstance(shadow, dict):
        out: list[JsonDiff] = []
        for key in sorted(set(live) | set(shadow)):
            child = f"{path}.{key}"
            if key not in live:
                out.append(
                    JsonDiff(
                        child,
                        "membership",
                        f"{child}: only in shadow",
                        side="shadow",
                        parent_path=path,
                        parent_size_live=len(live),
                    )
                )
            elif key not in shadow:
                out.append(
                    JsonDiff(
                        child,
                        "membership",
                        f"{child}: only in live",
                        side="live",
                        parent_path=path,
                        parent_size_live=len(live),
                    )
                )
            else:
                out.extend(_json_diffs(live[key], shadow[key], rel, absolute, child))
        return out
    if isinstance(live, list) and isinstance(shadow, list):
        if len(live) != len(shadow):
            return [
                JsonDiff(
                    path,
                    "cardinality",
                    f"{path}: length {len(live)} live vs {len(shadow)} shadow",
                )
            ]
        out = []
        for index, (a, b) in enumerate(zip(live, shadow, strict=True)):
            out.extend(_json_diffs(a, b, rel, absolute, f"{path}[{index}]"))
        return out
    if isinstance(live, (int, float)) and isinstance(shadow, (int, float)):
        if _numeric_close(live, shadow, rel, absolute):
            return []
        return [JsonDiff(path, "value", f"{path}: {live!r} live vs {shadow!r} shadow")]
    if live == shadow:
        return []
    return [JsonDiff(path, "value", f"{path}: {live!r} live vs {shadow!r} shadow")]


def _json_breaches(live: Any, shadow: Any, rel: float, absolute: float, path: str = "$") -> list[str]:
    """The rendered form of :func:`_json_diffs`, for callers wanting strings."""
    return [diff.rendered for diff in _json_diffs(live, shadow, rel, absolute, path)]



#: The two comparison classes a contract may declare (Brian ruling 2026-09-20,
#: `alpha-engine-config-I11203`). `deterministic` is the default and the
#: pre-existing behaviour: byte-equal, which 918 of 959 keys already satisfy.
_COMPARISON_CLASSES = frozenset({"deterministic", "vendor_live"})

#: A MEMBERSHIP diff — a key present on one side only.
_MEMBERSHIP_DIFF_RE = re.compile(r": only in (live|shadow)$")

#: A CARDINALITY diff — a list whose length differs. Never forgiven for a
#: vendor_live key: the ruling grades values approximately and SHAPE exactly.
_CARDINALITY_DIFF_RE = re.compile(r": length \d+ live vs \d+ shadow$")


def classify_diff(diff: str) -> str:
    """``"membership"`` | ``"cardinality"`` | ``"value"`` for one RENDERED diff.

    Kept for callers holding only a report's rendered strings (the published
    reports carry no structure). Inside this module the classification now
    comes from :class:`JsonDiff.kind`, which the walker sets from the data
    rather than re-deriving from the string it just printed.
    """
    if _MEMBERSHIP_DIFF_RE.search(diff):
        return "membership"
    if _CARDINALITY_DIFF_RE.search(diff):
        return "cardinality"
    return "value"


#: A top-level field name out of one diff path, e.g. ``"$.fetched_at"`` ->
#: ``"fetched_at"``. Provenance fields declared in a contract are top-level
#: today (`fetched_at`, `revision`, `as_of_utc`); a diff any deeper than that
#: is never matched here and stays a data breach.
_TOP_LEVEL_FIELD_RE = re.compile(r"^\$\.([A-Za-z0-9_]+)\b")


def _split_json_diffs(
    diffs: list[JsonDiff], provenance_fields: frozenset[str]
) -> tuple[list[JsonDiff], list[JsonDiff]]:
    """Route each diff into (data breaches, provenance diffs)."""
    breaches: list[JsonDiff] = []
    provenance: list[JsonDiff] = []
    for diff in diffs:
        match = _TOP_LEVEL_FIELD_RE.match(diff.path)
        if match and match.group(1) in provenance_fields:
            provenance.append(diff)
        else:
            breaches.append(diff)
    return breaches, provenance




def _grade_coverage(
    missing: list[JsonDiff], doc_size: int, floor: float
) -> dict[str, Any]:
    """Coverage of the live key set the SHADOW failed to reproduce.

    The denominator is the size of the live containers the missing keys came
    out of, summed over the distinct containers involved — never the
    document's top-level key count, which is what this graded against until
    2026-09-20 and which produced ratios of 0.0 on documents with zero value
    breaches (`market_data/earnings/latest.json`: 16 missing tickers measured
    against 3 top-level fields).

    With no missing keys there is no container to measure, so the document's
    own size is reported as the basis and the ratio is 1.0 by construction.
    """
    if not missing:
        return {
            "ratio": 1.0,
            "floor": floor,
            "met": 1.0 >= floor,
            "denominator": doc_size,
            "denominator_basis": "document (no missing keys to measure)",
            "missing": 0,
        }
    sizes = {diff.parent_path: diff.parent_size_live for diff in missing}
    denominator = sum(sizes.values())
    basis = "+".join(f"{path}({size})" for path, size in sorted(sizes.items()))
    if denominator <= 0:
        # Every container the missing keys came from is EMPTY on the live
        # side, which cannot happen for a key that is "only in live" and can
        # only mean "only in shadow" extras. Reported as unmeasured rather
        # than divided by a substituted 1, which would invent a ratio.
        return {
            "ratio": 0.0,
            "floor": floor,
            "met": False,
            "denominator": 0,
            "denominator_basis": f"{basis} — no live keys to cover",
            "missing": len(missing),
        }
    covered = max(denominator - len(missing), 0)
    ratio = covered / denominator
    return {
        "ratio": round(ratio, 6),
        "floor": floor,
        "met": ratio >= floor,
        "denominator": denominator,
        "denominator_basis": basis,
        "missing": len(missing),
    }


def compare_bytes(
    key: str,
    live: bytes,
    shadow: bytes,
    *,
    rel: float,
    absolute: float,
    contract: "ContractSchema | None" = None,
    trading_day: "dt.date | None" = None,
    prior_day: "dt.date | None" = None,
) -> dict[str, Any]:
    """Compare one key's two payloads. Returns the row body; never raises for data.

    ``contract``, when resolved (:func:`resolve_contract`), separates
    provenance fields from data fields (I10894): a provenance diff is
    reported under ``provenance_diffs`` and never counts toward
    ``values.breaches`` or the row's verdict. ``None`` is the red default —
    every field compares as data.

    A contract declaring ``x-comparison-class: vendor_live`` is additionally
    graded with ITS OWN band and coverage floor (Brian ruling 2026-09-20,
    `alpha-engine-config-I11203`), for BOTH comparators. The parquet half of
    that was missing from the first implementation, which left every
    `reference/price_cache/*.parquet` row mismatching on ~1e-6 relative
    re-derivation drift that no contract could reach.
    """
    provenance_fields = contract.provenance_fields if contract is not None else frozenset()
    vendor_live = contract is not None and contract.is_vendor_live
    compare_rel = contract.value_band_relative if vendor_live else rel
    compare_abs = contract.value_band_absolute if vendor_live else absolute
    band = {"relative": compare_rel, "absolute": compare_abs}
    absorbed_note = (
        "value differences inside the declared band are drift between two fetches "
        "of a moving number, not a producer defect"
    )
    basis = settling_basis(key, contract, trading_day)
    settling = _SettlingCollector(basis, trading_day) if basis else None
    # The prior day is re-graded only where this report actually holds that
    # day's row: a `row_date` key carries its own history. A `key_date` or
    # `same_day_snapshot` key does not (yesterday's bar lives at yesterday's
    # key, or has been overwritten in place), and claiming otherwise would put
    # a `settled: true` on a row that was never compared.
    prior = prior_day if (contract is not None and contract.settling_basis == "row_date") else None

    if key.endswith(".parquet"):
        import pandas as pd

        try:
            live_frame = pd.read_parquet(io.BytesIO(live))
            shadow_frame = pd.read_parquet(io.BytesIO(shadow))
        except Exception as exc:  # noqa: BLE001 - recorded as a row verdict, never swallowed
            return {
                "comparator": "parquet",
                "verdict": "unmeasurable",
                "unmeasurable_reason": f"parquet would not parse: {type(exc).__name__}: {exc}",
            }
        body = _compare_frames(
            live_frame,
            shadow_frame,
            compare_rel,
            compare_abs,
            provenance_fields,
            settling=settling,
            settling_whole_frame=basis in {"key_date", "same_day_snapshot"},
            prior_day=prior,
        )
        if settling is not None:
            body["settling_bar"] = settling.as_block()
        schema_ok = body["schema"]["match"]
        if not vendor_live:
            matched = (
                body["row_count"]["live"] == body["row_count"]["shadow"]
                and schema_ok
                and not body["symbol_set"]["only_live"]
                and not body["symbol_set"]["only_shadow"]
                and body["values"]["breaches"] == 0
            )
            body.update({"comparator": "parquet", "verdict": "match" if matched else "mismatch"})
            return body

        # The vendor_live grading, in the frame's own terms: SHAPE exactly
        # (the column set and its dtypes), MEMBERSHIP against the declared
        # coverage floor (a symbol the vendor dropped between two fetches is
        # expected; a producer losing half the universe is not), VALUES inside
        # the declared band. Row count is NOT graded separately — for a frame
        # it is the symbol set restated, and grading it twice would fail a key
        # the floor deliberately admits.
        only_live = list(body["symbol_set"]["only_live"])
        denominator = int(body["symbol_set"]["live"])
        covered = max(denominator - len(only_live), 0)
        ratio = (covered / denominator) if denominator > 0 else 0.0
        floor = contract.coverage_floor
        body["vendor_drift"] = {
            "class": "vendor_live",
            "band": band,
            "absorbed_note": absorbed_note,
            "membership_diffs": len(only_live) + len(body["symbol_set"]["only_shadow"]),
            "membership_examples": (
                [f"{s}: only in live" for s in only_live[:10]]
                + [f"{s}: only in shadow" for s in list(body["symbol_set"]["only_shadow"])[:10]]
            )[:10],
        }
        body["coverage"] = {
            "ratio": round(ratio, 6),
            "floor": floor,
            "met": ratio >= floor and denominator > 0,
            "denominator": denominator,
            "denominator_basis": f"{body['symbol_set']['basis']}({denominator})",
            "missing": len(only_live),
        }
        matched = schema_ok and body["coverage"]["met"] and body["values"]["breaches"] == 0
        body.update({"comparator": "parquet", "verdict": "match" if matched else "mismatch"})
        return body

    if key.endswith(".json"):
        try:
            live_doc = json.loads(live.decode("utf-8"))
            shadow_doc = json.loads(shadow.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - recorded as a row verdict, never swallowed
            return {
                "comparator": "json",
                "verdict": "unmeasurable",
                "unmeasurable_reason": f"json would not parse: {type(exc).__name__}: {exc}",
            }
        all_diffs = _json_diffs(live_doc, shadow_doc, compare_rel, compare_abs)
        breaches, provenance_diffs = _split_json_diffs(all_diffs, provenance_fields)

        # A JSON document has no row index, so `row_date` cannot be evaluated
        # over one — the two bases that CAN are the two that say "this whole
        # document is the trading day's bar". A `row_date` declaration landing
        # on a JSON key is recorded rather than silently applied or silently
        # dropped: it means the contract and the artifact disagree.
        if settling is not None and basis == "row_date":
            body_note = (
                "x-settling-bar.basis is 'row_date' but this key is a JSON document with no "
                "row index; no cell was graded as settling. Declare 'key_date' or "
                "'same_day_snapshot' instead (alpha-engine-config-I11351)."
            )
            settling = None
        else:
            body_note = ""
        if settling is not None:
            # VALUE diffs only. A key present on one side alone (membership) or
            # a list whose length differs (cardinality) is SHAPE, graded
            # exactly, whatever the bar was doing — the binding constraint on
            # I11351 and the same rule the vendor_live class already follows.
            for diff in [d for d in breaches if d.kind == "value"]:
                settling.record_json(diff)
            breaches = [d for d in breaches if d.kind != "value"]

        doc_size = len(live_doc) if isinstance(live_doc, (list, dict)) else 1
        body: dict[str, Any] = {
            "comparator": "json",
            "values": {
                "breaches": len(breaches),
                "examples": [d.rendered for d in breaches[:10]],
            },
            "provenance_diffs": {
                "count": len(provenance_diffs),
                "examples": [d.rendered for d in provenance_diffs[:10]],
            },
            "row_count": {
                "live": doc_size,
                "shadow": len(shadow_doc) if isinstance(shadow_doc, (list, dict)) else 1,
            },
        }
        if settling is not None:
            body["settling_bar"] = settling.as_block()
        if body_note:
            body["settling_bar_declaration_problem"] = body_note

        if not vendor_live:
            body["verdict"] = "match" if not breaches else "mismatch"
            return body

        # The ruling grades a vendor_live key on SCHEMA CONFORMANCE, KEY-SET
        # COVERAGE and ROW COUNT, with values inside the band. So:
        #
        #   * value diffs OUTSIDE the band stay breaches -- the band already
        #     absorbed everything inside it;
        #   * cardinality diffs stay breaches, always. Shape is graded exactly;
        #   * membership diffs are counted, not forgiven: a ticker delisted
        #     between two fetches is expected, a producer dropping half the
        #     universe is not. Coverage is graded against the declared floor,
        #     and only keys MISSING FROM THE SHADOW count against it -- a key
        #     the shadow has and live does not is an extra, reported under
        #     `vendor_drift`, never a coverage loss.
        membership = [d for d in breaches if d.kind == "membership"]
        missing = [d for d in membership if d.side == "live"]
        extra = [d for d in membership if d.side == "shadow"]
        non_membership = [d for d in breaches if d.kind != "membership"]

        body["vendor_drift"] = {
            "class": "vendor_live",
            "band": band,
            "absorbed_note": absorbed_note,
            "membership_diffs": len(membership),
            "membership_examples": [d.rendered for d in membership[:10]],
            "extra_in_shadow": len(extra),
        }
        body["coverage"] = _grade_coverage(missing, doc_size, contract.coverage_floor)
        body["values"] = {
            "breaches": len(non_membership),
            "examples": [d.rendered for d in non_membership[:10]],
        }
        body["verdict"] = (
            "match" if (not non_membership and body["coverage"]["met"]) else "mismatch"
        )
        return body

    live_digest = hashlib.sha256(live).hexdigest()
    shadow_digest = hashlib.sha256(shadow).hexdigest()
    return {
        "comparator": "opaque",
        "verdict": "match" if live_digest == shadow_digest else "mismatch",
        "detail": (
            f"no structural comparator for this extension; compared sha256 only "
            f"({len(live)} live bytes, {len(shadow)} shadow bytes)"
        ),
        "sha256": {"live": live_digest, "shadow": shadow_digest},
    }




# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


@dataclass
class KeyResult:
    key: str
    unit_ids: list[str]
    verdict: str
    comparator: str
    body: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        out = {
            "key": self.key,
            "unit_ids": self.unit_ids,
            "verdict": self.verdict,
            "comparator": self.comparator,
        }
        out.update(self.body)
        out["verdict"] = self.verdict
        out["comparator"] = self.comparator
        return out


@dataclass
class ParityReport:
    trading_day: dt.date
    bucket: str
    shadow_prefix: str
    code_sha: str
    rows: list[KeyResult]
    excluded: list[dict[str, str]]
    rel_tolerance: float
    absolute_tolerance: float
    generated_at: str
    #: What the PRODUCER legs did, when the caller knows (`--legs-file`).
    #: Empty when the comparator was run on its own over an existing prefix.
    #:
    #: `alpha-engine-config-I11200`. `shadow-weekday` chained its legs with
    #: `&&`, so one leg's non-zero exit skipped every later leg AND the
    #: comparator. Measured twice on 2026-09-20: the 09-14 dispatch died in
    #: leg 2 on a freshness guard, the 09-18 dispatch died at the end of leg 3
    #: on `features=degraded` -- two unrelated causes, and NEITHER produced a
    #: report, though the 09-18 run had already written 1,977 objects the
    #: comparator could read. Running the legs independently means a report
    #: always exists; carrying their outcomes here is what stops it being read
    #: as if every leg had run.
    legs: list[dict[str, Any]] = field(default_factory=list)
    #: Which dispatch groups told this report what their legs did. One boolean
    #: per group in :data:`LEGS_GROUPS` (`alpha-engine-config-I11352`): the
    #: same-day dispatch and the D+1 morning dispatch write the SAME report, so
    #: one bare boolean could not say that the morning legs had not run yet
    #: without also erasing the same-day ones.
    legs_known: dict[str, bool] = field(default_factory=dict)
    #: Deliverable 2 of `alpha-engine-config-I11351`: whether the bar this
    #: key's PREVIOUS report could only record as settling is, by now, settled
    #: on both sides. ``{}`` when no previous report was available to read.
    prior_day_settled: dict[str, Any] = field(default_factory=dict)

    @property
    def settling_bar_keys(self) -> int:
        return sum(1 for row in self.rows if int((row.body.get("settling_bar") or {}).get("cells") or 0))

    @property
    def summary(self) -> dict[str, int]:
        counts = {
            "total": len(self.rows),
            "match": 0,
            "mismatch": 0,
            "live_missing": 0,
            "shadow_missing": 0,
            "both_missing": 0,
            "unmeasurable": 0,
            "in_region_only": 0,
            "live_superseded": 0,
            # Declared at zero like every other verdict: a count that only
            # appears when non-zero cannot be told apart from health when it is
            # absent (alpha-engine-config-I11231).
            "not_applicable": 0,
        }
        for row in self.rows:
            counts[row.verdict] = counts.get(row.verdict, 0) + 1
        # NOT a verdict — a breakdown OF `match`. Named in
        # `data_gate.evidence.SUMMARY_NON_VERDICT_FIELDS` so the gate reader
        # never mistakes it for an exception count and reads every report as
        # UNMET (`alpha-engine-config-I11351` deliverable 3).
        counts["settling_bar_keys"] = self.settling_bar_keys
        return counts

    @property
    def met(self) -> bool:
        """MET only when every row matched.

        Deliberately strict: an unmeasurable or missing key is not parity
        evidence, and the gate this feeds exists to stop a cutover that has
        not been measured. Plan §4.1 rule 2 — UNMEASURABLE is never met.
        """
        return bool(self.rows) and all(row.verdict == "match" for row in self.rows)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": PARITY_SCHEMA_VERSION,
            "trading_day": self.trading_day.isoformat(),
            "generated_at": self.generated_at,
            "bucket": self.bucket,
            "shadow_prefix": self.shadow_prefix,
            "code_sha": self.code_sha,
            "tolerance": {"relative": self.rel_tolerance, "absolute": self.absolute_tolerance},
            # DECLARED, never inferred. An empty list means "the comparator was
            # not told", which is NOT the same claim as "every leg ran" -- a
            # reader that cannot tell those apart is the state I11200 describes.
            "legs": self.legs,
            "legs_known": self.legs_known or {
                group: any(leg.get("dispatch") == group for leg in self.legs)
                for group in LEGS_GROUPS
            },
            "prior_day_settled": self.prior_day_settled,
            "met": self.met,
            "summary": self.summary,
            "excluded_units": self.excluded,
            "keys": [row.as_dict() for row in self.rows],
        }


def merge_legs(
    previous: list[dict[str, Any]], incoming: list[dict[str, Any]], *, group: str
) -> tuple[list[dict[str, Any]], dict[str, bool]]:
    """This dispatch's legs, merged into whatever the other dispatch recorded.

    `alpha-engine-config-I11352`. The same-day dispatch (18:30 ET on D) and the
    morning dispatch (07:45 ET on D+1) both publish `parity/{D}.json`, and the
    second one rewrites the comparison — the whole point, since the morning
    legs' keys only exist by then. What must NOT be rewritten is the other
    group's leg outcomes: a morning re-run that replaced `legs` wholesale would
    erase the record that the post-market legs ran at all, which is exactly the
    silence `I11200` created this block to end.

    So: entries whose `dispatch` is THIS group are replaced; every other entry
    is kept, in its original order, ahead of the new ones. An entry from a
    pre-I11352 report carries no `dispatch` at all and is attributed to the
    default group, because that is the only dispatch that existed when it was
    written — dropping it would lose a measurement, and keeping it unlabelled
    would make it un-replaceable forever.

    Returns ``(legs, legs_known)``; `legs_known` carries one boolean per group
    in :data:`LEGS_GROUPS`, so a report can say the morning legs have not run
    yet without saying the same-day ones did not.
    """
    if group not in LEGS_GROUPS:
        raise ValueError(f"legs group must be one of {list(LEGS_GROUPS)}, got {group!r}")
    kept: list[dict[str, Any]] = []
    for leg in previous:
        entry = dict(leg)
        entry.setdefault("dispatch", DEFAULT_LEGS_GROUP)
        if entry["dispatch"] != group:
            kept.append(entry)
    fresh = [dict(leg, dispatch=group) for leg in incoming]
    merged = kept + fresh
    known = {g: any(leg.get("dispatch") == g for leg in merged) for g in LEGS_GROUPS}
    return merged, known


class S3Reader:
    """The two reads parity needs, against one bucket."""

    def __init__(self, bucket: str, client=None) -> None:
        self.bucket = bucket
        if client is None:
            import boto3

            client = boto3.client("s3")
        self.client = client

    def get(self, key: str) -> bytes | None:
        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except Exception as exc:  # noqa: BLE001 - NoSuchKey is an answer; anything else re-raises
            code = ""
            response = getattr(exc, "response", None)
            if isinstance(response, dict):
                code = str((response.get("Error") or {}).get("Code") or "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise

    def get_with_meta(self, key: str, version_id: str | None = None) -> dict[str, Any] | None:
        """One object's bytes with the ETag and VersionId it was served as.

        ``version_id`` fetches that exact version (alpha-engine-config-I10892).
        A missing key OR a version no longer retained (the bucket's 30-day
        noncurrent expiry) is ``None``; any other failure re-raises.
        """
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key}
        if version_id:
            kwargs["VersionId"] = version_id
        try:
            resp = self.client.get_object(**kwargs)
        except Exception as exc:  # noqa: BLE001 - a missing key/version is an answer; anything else re-raises
            code = ""
            response = getattr(exc, "response", None)
            if isinstance(response, dict):
                code = str((response.get("Error") or {}).get("Code") or "")
            if code in {"NoSuchKey", "NoSuchVersion", "404", "NotFound"}:
                return None
            raise
        etag = resp.get("ETag")
        version = resp.get("VersionId")
        return {
            "body": resp["Body"].read(),
            "etag": etag.strip('"') if isinstance(etag, str) else None,
            "version_id": version if isinstance(version, str) and version != "null" else None,
        }

    def list(self, prefix: str, limit: int) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": min(1000, limit)}
            if token:
                kwargs["ContinuationToken"] = token
            page = self.client.list_objects_v2(**kwargs)
            keys.extend(item["Key"] for item in page.get("Contents") or [])
            token = page.get("NextContinuationToken")
            if not token or len(keys) >= limit:
                break
        return keys[:limit]


#: `version_capture` values under which a manifest output's etag/version_id is
#: a measurement (nousergon_lib.run_manifest.VERSION_CAPTURES). A record with
#: no `version_capture` at all predates I10892; its etag, when non-null, was
#: passed by the write site and is trusted the same way.
_MEASURED_VERSION_CAPTURES = frozenset({"caller", "head_object"})


def recorded_live_version(manifests: Iterable[dict[str, Any]], live_key: str) -> dict[str, Any] | None:
    """The output record for ``live_key`` in v1's manifests for the trading day,
    when it carries a measured ETag or VersionId; otherwise ``None``.

    Among several manifests recording the key, the latest ``finished`` wins —
    the object a same-day retry left behind is the one that stands for the day.
    """
    best: tuple[str, dict[str, Any]] | None = None
    for manifest in manifests:
        for out in manifest.get("outputs") or []:
            if str(out.get("key") or "") != live_key:
                continue
            capture = out.get("version_capture")
            if capture is not None and capture not in _MEASURED_VERSION_CAPTURES:
                continue
            if not out.get("etag") and not out.get("version_id"):
                continue
            finished = str(manifest.get("finished") or "")
            if best is None or finished >= best[0]:
                best = (finished, out)
    return None if best is None else best[1]


def _read_live_as_recorded(
    reader: S3Reader, live_key: str, expected: dict[str, Any] | None
) -> tuple[bytes | None, dict[str, Any]]:
    """The live bytes to grade, and how they were chosen (``live_version``).

    Returns ``(None, {...basis: superseded_*/recorded_version_not_retained})``
    when the object v1 recorded for the day can no longer be read — the caller
    turns that into ``live_superseded``, never ``mismatch`` and never ``match``.
    """
    if expected is None:
        return reader.get(live_key), {"basis": "unrecorded"}
    manifest_etag = str(expected["etag"]).strip('"') if expected.get("etag") else None
    manifest_version = expected.get("version_id") or None
    current = reader.get_with_meta(live_key)
    info: dict[str, Any] = {
        "manifest_etag": manifest_etag,
        "manifest_version_id": manifest_version,
        "current_etag": current["etag"] if current else None,
    }
    if current is not None and (
        (manifest_etag is not None and current["etag"] == manifest_etag)
        or (manifest_etag is None and manifest_version is not None and current["version_id"] == manifest_version)
    ):
        return current["body"], {"basis": "current_matches_manifest", **info}
    if manifest_version is None:
        if current is None:
            # Nothing to supersede with: the key is gone and no version names
            # the recorded object. That is the ordinary live_missing row.
            return None, {"basis": "live_missing", **info}
        return None, {"basis": "superseded_no_version_id", **info}
    recorded = reader.get_with_meta(live_key, version_id=manifest_version)
    if recorded is None:
        return None, {"basis": "recorded_version_not_retained", **info}
    return recorded["body"], {"basis": "manifest_version_id", **info}


def _compare_one_key(
    reader: S3Reader,
    root: ShadowRoot,
    live_key: str,
    unit_ids: list[str],
    rel,
    absolute,
    expected: dict[str, Any] | None = None,
    trading_day: "dt.date | None" = None,
    prior_day: "dt.date | None" = None,
    shadow_manifests: dict[str, dict[str, Any]] | None = None,
) -> KeyResult:
    """Grade one key. ``expected`` is v1's manifest output record for this
    trading day (:func:`recorded_live_version`); with it, the live side is the
    object v1 published FOR THAT DAY, not whatever a later run left at the key
    (alpha-engine-config-I10892). Without it, the current live object, as before.
    """
    shadow_key = root.key(live_key)
    try:
        live, live_version = _read_live_as_recorded(reader, live_key, expected)
        shadow = reader.get(shadow_key)
    except Exception as exc:  # noqa: BLE001 - a denied/failed read is UNMEASURABLE, and named
        return KeyResult(
            live_key,
            unit_ids,
            "unmeasurable",
            "none",
            {"unmeasurable_reason": f"{type(exc).__name__}: {exc}", "shadow_key": shadow_key},
        )
    if live is None and live_version["basis"] in {"superseded_no_version_id", "recorded_version_not_retained"}:
        why = (
            "the live key's current ETag differs from the one v1's manifest recorded for this "
            "trading day, and the manifest carries no VersionId to fetch the recorded object by"
            if live_version["basis"] == "superseded_no_version_id"
            else "the recorded VersionId is no longer retained (noncurrent-version expiry)"
        )
        return KeyResult(
            live_key, unit_ids, "live_superseded", "none",
            {
                "detail": f"{why}; the current object belongs to a later run and is not parity evidence",
                "shadow_key": shadow_key,
                "live_version": live_version,
            },
        )
    if live is None and shadow is None:
        return KeyResult(
            live_key, unit_ids, "both_missing", "none",
            {"detail": "neither the live key nor its shadow exists", "shadow_key": shadow_key},
        )
    if live is None:
        return KeyResult(
            live_key, unit_ids, "live_missing", "none",
            {"detail": "the shadow run wrote it; v1 did not", "shadow_key": shadow_key},
        )
    if shadow is None:
        return _absent_shadow_result(live_key, unit_ids, shadow_key, shadow_manifests or {})
    body = compare_bytes(
        live_key,
        live,
        shadow,
        rel=rel,
        absolute=absolute,
        contract=resolve_contract(live_key),
        trading_day=trading_day if trading_day is not None else root.trading_day,
        prior_day=prior_day,
    )
    body["shadow_key"] = shadow_key
    body["live_version"] = live_version
    if not body.get("detail"):
        summary = _verdict_detail(body)
        if summary:
            body["detail"] = summary
    return KeyResult(live_key, unit_ids, body.pop("verdict"), body.pop("comparator"), body)


#: The per-key refusal record a collector folds onto its run manifest
#: (``collectors/prices.py::refused_keys_guard_entries``, alpha-engine-config-
#: I11547). Literals here rather than an import: this module must not pull in
#: the collector import graph. ``tests/test_prices_shadow_recent_listings_
#: i11547.py`` pins them against the producer's constants.
WRITE_REFUSED_GUARD = "write_refused"
WRITE_REFUSED_COMPLETE = "complete"


def _refusal_record(manifest: dict[str, Any]) -> "tuple[bool, dict[str, str]] | None":
    """``(complete, {refused key: detail})`` from a manifest's ``write_refused``
    readings, or ``None`` when the unit records no per-key refusals at all.

    ``complete`` is True only when the unkeyed summary reading says every
    refused key is listed — a truncated list cannot rule a key OUT.
    """
    readings = [
        g for g in (manifest.get("guards") or [])
        if isinstance(g, dict) and g.get("guard") == WRITE_REFUSED_GUARD
    ]
    if not readings:
        return None
    summaries = [g for g in readings if not g.get("key")]
    complete = bool(summaries) and all(g.get("verdict") == WRITE_REFUSED_COMPLETE for g in summaries)
    keyed = {str(g["key"]): str(g.get("detail") or "") for g in readings if g.get("key")}
    return complete, keyed


def _absent_shadow_result(
    live_key: str,
    unit_ids: list[str],
    shadow_key: str,
    shadow_manifests: dict[str, dict[str, Any]],
) -> KeyResult:
    """Why the shadow key is absent, read from the OWNING UNIT'S OWN MANIFEST.

    `alpha-engine-config-I11231` closes-when 2. "v1 wrote it; the shadow run
    did not" was the same sentence for three different facts, and the report
    had the answer to all three sitting one read away:

    * the unit DECLINED to run, correctly. D26 (`market_data/technicals/
      rating_history/_manifest.json`) recorded `status: not_applicable,
      reason: no_new_data_declared` on 2026-09-21 — an immutable-date no-op, not
      a producer that lost a key. Graded `not_applicable`, carrying the
      manifest's own `reason` and `run_id`.
    * the unit RAN AND FAILED. The four `reference/price_cache/{FDXF,HONA,Q,
      SOLS}.parquet` rows the same night were `short_fetch_guard_refused`.
      Still `shadow_missing` — the key IS missing — but the detail now names
      the cause instead of leaving a reader to go find the manifest.
    * anything else — no manifest at all, or a manifest saying `ok` — keeps the
      pre-existing wording, which is the honest one for "it should be there".

    `alpha-engine-config-I11547`: a unit's failure is cited as THIS key's cause
    only when it can be. The 2026-09-22 report listed AGNC/CORT/EAT/HUBS as
    `shadow_missing` and blamed each on D03's failure — whose reason named
    FDXF/HONA/Q/SOLS. The shadow D03 never attempted those four (they joined
    the universe in the weekly rehearsal's constituents refresh AFTER the
    shadow ran; the v1 manifest compared against was that rehearsal's re-run).
    When the failed manifest carries a COMPLETE per-key refusal record
    (`write_refused`) and this key is not in it, the failure does not explain
    the absence and the detail says so. A key the record names is cited with
    its own reason. A unit with no per-key record, or a truncated one, keeps
    the unit-level citation, flagged as unattributed.

    `not_applicable` is never `match`, so it is never parity evidence and can
    never make the gate MET (plan §4.1 rule 2). It is a different NEXT ACTION
    from `shadow_missing`, which is the whole reason it is its own verdict:
    one says read the declaration, the other says fix the producer.
    """
    owning = [(unit_id, shadow_manifests[unit_id]) for unit_id in unit_ids if unit_id in shadow_manifests]
    statuses = {str(manifest.get("status") or "") for _, manifest in owning}

    def _cite(manifest: dict[str, Any]) -> str:
        reason = str(manifest.get("reason") or "").strip() or "(no reason recorded)"
        return f"{reason} (run_id {manifest.get('run_id') or 'unknown'})"

    # EVERY owning unit, not any: a key two units write is only a declared no-op
    # when BOTH declined. One unit declaring itself not applicable while the
    # other ran and produced nothing is a missing key, and saying otherwise
    # would turn a real gap into a green declaration.
    if owning and statuses == {"not_applicable"}:
        return KeyResult(
            live_key, unit_ids, "not_applicable", "none",
            {
                "detail": (
                    "the shadow run declared this unit not applicable for the trading day: "
                    + "; ".join(f"{unit_id}: {_cite(manifest)}" for unit_id, manifest in owning)
                    + ". v1 wrote the key; the shadow correctly did not, so there is nothing "
                    "to diff and this is not a producer gap"
                ),
                "shadow_key": shadow_key,
                "shadow_run_status": {unit_id: "not_applicable" for unit_id, _ in owning},
            },
        )

    failed = [(unit_id, manifest) for unit_id, manifest in owning if str(manifest.get("status")) == "failed"]
    detail = "v1 wrote it; the shadow run did not"
    cited: list[str] = []
    not_explained: list[str] = []
    attribution: dict[str, str] = {}
    for unit_id, manifest in failed:
        record = _refusal_record(manifest)
        if record is None:
            attribution[unit_id] = "unrecorded"
            cited.append(f"{unit_id}: {_cite(manifest)}")
            continue
        complete, keyed = record
        if live_key in keyed:
            attribution[unit_id] = "refused_this_key"
            cited.append(f"{unit_id}: refused this key ({keyed[live_key]}) — {_cite(manifest)}")
        elif complete:
            attribution[unit_id] = "not_this_key"
            not_explained.append(f"{unit_id}: {_cite(manifest)}")
        else:
            attribution[unit_id] = "truncated"
            cited.append(
                f"{unit_id}: {_cite(manifest)} [its per-key refusal list is truncated, so "
                "whether it refused this key is not recorded]"
            )
    if cited:
        detail += " — the owning unit's shadow run FAILED: " + "; ".join(cited)
    if not_explained:
        detail += (
            (" — and" if cited else " —")
            + " the owning unit's shadow run also FAILED, but NOT on this key: "
            + "; ".join(not_explained)
            + ". Its manifest lists every key it refused and this is not one of them, so the "
            "unit neither wrote nor refused it — the key was outside the population that run "
            "attempted; compare the two runs' inputs rather than the failure"
        )
    body: dict[str, Any] = {"detail": detail, "shadow_key": shadow_key}
    if owning:
        body["shadow_run_status"] = {
            unit_id: str(manifest.get("status") or "") for unit_id, manifest in owning
        }
    if attribution:
        body["failure_attribution"] = attribution
    return KeyResult(live_key, unit_ids, "shadow_missing", "none", body)


def _verdict_detail(body: dict[str, Any]) -> str:
    """One line saying why this row reads the way it does.

    `alpha-engine-config-I11203`: every `shadow_missing` / `both_missing` row
    already carried a `detail` and every MISMATCH row carried an empty one,
    with the diagnosis sitting in `values.examples`. The information was never
    lost — but a reader, or a renderer, that shows `detail` as the missing-row
    cases do saw nothing on the rows that most needed explaining. All 34
    mismatches on the 2026-09-18 report were blank.

    Built from the counts already computed, so it cannot disagree with them,
    and it names the band when one was applied — the number a reader needs in
    order to judge whether a `match` was earned or merely forgiven.
    """
    parts: list[str] = []
    breaches = int((body.get("values") or {}).get("breaches") or 0)
    if breaches:
        parts.append(f"{breaches} value breach{'es' if breaches != 1 else ''}")

    coverage = body.get("coverage") or {}
    if coverage:
        missing = int(coverage.get("missing") or 0)
        if missing:
            parts.append(
                f"{missing} key{'s' if missing != 1 else ''} only in live "
                f"(coverage {coverage.get('ratio')} vs floor {coverage.get('floor')})"
            )
        elif not coverage.get("met", True):
            parts.append(f"coverage {coverage.get('ratio')} below floor {coverage.get('floor')}")

    drift = body.get("vendor_drift") or {}
    extra = int(drift.get("extra_in_shadow") or 0)
    if extra:
        parts.append(f"{extra} key{'s' if extra != 1 else ''} only in shadow")

    schema = body.get("schema") or {}
    if schema and not schema.get("match", True):
        only_live = len(schema.get("only_live") or [])
        only_shadow = len(schema.get("only_shadow") or [])
        dtypes = len(schema.get("dtype_changes") or {})
        parts.append(
            f"schema differs ({only_live} columns only in live, "
            f"{only_shadow} only in shadow, {dtypes} dtype change"
            f"{'s' if dtypes != 1 else ''})"
        )

    rows = body.get("row_count") or {}
    if rows and rows.get("live") != rows.get("shadow"):
        parts.append(f"row count {rows.get('live')} live vs {rows.get('shadow')} shadow")

    settling = body.get("settling_bar") or {}
    cells = int(settling.get("cells") or 0)
    if cells:
        moves = []
        if settling.get("max_rel_close") is not None:
            moves.append(f"max rel close {settling['max_rel_close']:.3g}")
        if settling.get("max_rel_volume") is not None:
            moves.append(f"max rel volume {settling['max_rel_volume']:.3g}")
        parts.append(
            f"{cells} settling-bar cell{'s' if cells != 1 else ''} on {settling.get('date')} "
            f"({settling.get('basis')}), not counted as breaches"
            + (f" — {', '.join(moves)}" if moves else "")
        )

    prior = body.get("prior_day") or {}
    if prior:
        parts.append(
            f"prior day {prior.get('date')} re-graded strictly: "
            f"{prior.get('breaches')} breach(es) over {prior.get('rows_compared')} row(s)"
        )

    if not parts:
        return ""
    if drift.get("class"):
        band = drift.get("band") or {}
        parts.append(
            f"graded as {drift['class']} (band rel={band.get('relative')}, "
            f"abs={band.get('absolute')})"
        )
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Run-manifest reading (alpha-engine-config-I10890): the run's SCOPE and, for
# a prefix family, its CANDIDATE keys, both read from manifests rather than
# from listing a live prefix or trusting every descriptor whether or not this
# run touched it.
# ---------------------------------------------------------------------------


def _read_manifest(reader: "S3Reader", key: str) -> dict[str, Any] | None:
    raw = reader.get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # A manifest this tool cannot parse is not evidence either way; it is
        # never treated as "this unit ran" or "this unit did not run".
        return None


def _latest_manifests_by_unit(reader: "S3Reader", prefix: str, limit: int = 5000) -> dict[str, dict[str, Any]]:
    """Every `data_run_manifest.v1` JSON under `prefix`, keyed by `unit_id`,
    keeping only the LATEST attempt (max `finished`) per unit.

    The shadow prefix accumulates across attempts — four attempts on 09-15
    left four D17 manifests under the same trading day (I10890 gotcha) — so
    taking every manifest found would double-count a unit's outputs across
    retries. `finished` compares as an ISO-8601 UTC string, which sorts
    lexicographically in time order.
    """
    latest: dict[str, dict[str, Any]] = {}
    for key in sorted(k for k in reader.list(prefix, limit) if k.endswith(".json")):
        manifest = _read_manifest(reader, key)
        if manifest is None:
            continue
        unit_id = str(manifest.get("unit_id") or "")
        if not unit_id:
            continue
        current = latest.get(unit_id)
        if current is None or str(manifest.get("finished") or "") >= str(current.get("finished") or ""):
            latest[unit_id] = manifest
    return latest


def _manifest_output_keys(manifest: dict[str, Any], prefix_value: str) -> set[str]:
    return {
        str(out.get("key") or "")
        for out in manifest.get("outputs") or []
        if str(out.get("key") or "").startswith(prefix_value)
    }


def _dedupe_rows(rows: list[KeyResult]) -> list[KeyResult]:
    """One row per S3 key, attributed to every unit that declared it.

    `expand_writes` dedupes TARGETS on `(kind, value)`, which cannot see that a
    key declared explicitly by one unit is also enumerated inside another
    unit's prefix family. Measured on the 2026-09-18 report
    (`alpha-engine-config-I11203`): `market_data/close_history/consolidated.json`
    and `market_data/technicals/rating_history/_manifest.json` each appeared
    TWICE, so `summary.total` read 959 for 957 distinct keys and every
    percentage computed from it was wrong by that much.

    The duplicate rows are by construction the same comparison of the same two
    objects, so the first is kept and the later ones contribute only their unit
    attribution. A verdict DISAGREEMENT between two rows would mean the same
    bytes graded two ways; it is recorded on the surviving row rather than
    silently resolved.
    """
    merged: dict[str, KeyResult] = {}
    order: list[str] = []
    for row in rows:
        existing = merged.get(row.key)
        if existing is None:
            merged[row.key] = row
            order.append(row.key)
            continue
        for unit_id in row.unit_ids:
            if unit_id not in existing.unit_ids:
                existing.unit_ids.append(unit_id)
        existing.unit_ids.sort()
        if row.verdict != existing.verdict:
            existing.body["duplicate_verdict_conflict"] = (
                f"a second row for this key graded {row.verdict!r} against "
                f"{existing.verdict!r}; the first is reported"
            )
    return [merged[name] for name in order]


def _prior_day_settled_field(
    prior_result: "KeyResult", *, prior_day: dt.date, prior_live_key: str, prior_shadow_key: str
) -> dict[str, Any]:
    """The per-key `prior_day_settled` block for one `key_date` row's D-1 re-grade.

    ``available`` is true only for an ordinary strict `match`/`mismatch`
    outcome — a `key_date` artifact whose whole content IS one trading day's
    bar, so anything else (missing, superseded, itself unmeasurable) is not a
    settled/unsettled fact about the value cells, and ``settled`` stays
    ``None`` rather than a substituted ``False`` that would read as a
    measured breach.
    """
    verdict = prior_result.verdict
    available = verdict in {"match", "mismatch"}
    field: dict[str, Any] = {
        "date": prior_day.isoformat(),
        "key": prior_live_key,
        "shadow_key": prior_shadow_key,
        "available": available,
        "verdict": verdict,
        "settled": (verdict == "match") if available else None,
    }
    if available:
        breaches = int((prior_result.body.get("values") or {}).get("breaches") or 0)
        coverage = prior_result.body.get("coverage") or {}
        if not coverage.get("met", True):
            breaches += int(coverage.get("missing") or 0)
        field["breaches"] = breaches
    else:
        field["unmeasurable_reason"] = (
            prior_result.body.get("unmeasurable_reason")
            or prior_result.body.get("detail")
            or f"the D-1 pair graded {verdict!r}, which is not a strict match/mismatch outcome"
        )
    return field


def _attach_key_date_prior_day_settled(
    rows: list["KeyResult"],
    *,
    reader: S3Reader,
    unit_by_id: dict[str, Unit],
    trading_day: dt.date,
    prior_day: dt.date,
    rel_tolerance: float,
    absolute_tolerance: float,
) -> None:
    """Deliverable 1 of `alpha-engine-config-I11360`.

    For every `key_date` row on report D, additionally read and strictly
    compare the D-1-dated key pair (live `.../{D-1}...` vs the D-1 SHADOW RUN's
    own output at `staging/shadow/{D-1}/...` — never today's shadow root,
    which never touched D-1's data) and record the result under a per-key
    `prior_day_settled` field, mutating each row's body in place.

    **Chosen over a synthetic row keyed by the D-1 key** (the issue's other
    named option): a synthetic row would add a member to `ParityReport.rows`
    that no `writes[]` entry declared for trading day D, inflating
    `summary.total` and coupling "did D's own snapshot match" to "did D-1's
    settle" through the same strict `ParityReport.met` — every existing
    single-trading-day test fixture would need a second day of fixture data
    or `met` would flip UNMET on a `both_missing` D-1 pair that was never in
    scope. A per-key field keeps exactly one row per S3 key `expand_writes`
    actually declared for D, leaves `summary`/`met` computed exactly as
    before, and still gives the cutover gate a place to read the D-1 verdict
    from (`data_gate.evidence.read_parity`, deliverable 3).

    `key_date` only: `same_day_snapshot` is overwritten in place and cannot be
    re-graded without a versioned read (deliverable 2); `row_date` already
    carries its own history in the SAME row's `prior_day` block.
    """
    prior_root = ShadowRoot(prior_day)
    prior_shadow_manifests = _latest_manifests_by_unit(reader, prior_root.key("data_collection/runs/"))
    prior_manifest_cache: dict[str, list[dict[str, Any]]] = {}

    def _expected_prior(live_key: str, owners: list[str]) -> dict[str, Any] | None:
        manifests: list[dict[str, Any]] = []
        for unit_id in owners:
            unit = unit_by_id.get(unit_id)
            if unit is None:
                continue
            if unit_id not in prior_manifest_cache:
                prior_manifest_cache[unit_id] = list(
                    _latest_manifests_by_unit(
                        reader, f"{unit.run_manifest_prefix}/{prior_day.isoformat()}/"
                    ).values()
                )
            manifests.extend(prior_manifest_cache[unit_id])
        return recorded_live_version(manifests, live_key)

    for row in rows:
        if (row.body.get("settling_bar") or {}).get("basis") != "key_date":
            continue
        prior_live_key = rebased_key_date_key(row.key, trading_day, prior_day)
        if prior_live_key is None:
            # Unreachable in practice: `settling_bar.basis == "key_date"` is
            # only ever set (`settling_basis`) after `key_names_trading_day`
            # already matched `trading_day` as exactly one path segment of
            # this same key. Recorded rather than silently skipped, because
            # a mismatch here means that classifier and this rewrite have
            # drifted apart.
            row.body["prior_day_settled"] = {
                "date": prior_day.isoformat(),
                "available": False,
                "unmeasurable_reason": (
                    f"could not locate {trading_day.isoformat()!r} as a single path segment of "
                    f"{row.key!r} to substitute {prior_day.isoformat()!r} in its place"
                ),
            }
            continue
        prior_shadow_key = prior_root.key(prior_live_key)
        prior_result = _compare_one_key(
            reader,
            prior_root,
            prior_live_key,
            row.unit_ids,
            rel_tolerance,
            absolute_tolerance,
            expected=_expected_prior(prior_live_key, row.unit_ids),
            trading_day=trading_day,
            prior_day=None,
            shadow_manifests=prior_shadow_manifests,
        )
        row.body["prior_day_settled"] = _prior_day_settled_field(
            prior_result,
            prior_day=prior_day,
            prior_live_key=prior_live_key,
            prior_shadow_key=prior_shadow_key,
        )


def run_parity(
    *,
    trading_day: dt.date,
    bucket: str,
    reader: S3Reader | None = None,
    units: list[Unit] | None = None,
    code_sha: str = "unknown",
    rel_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute_tolerance: float = DEFAULT_ABSOLUTE_TOLERANCE,
    max_keys_per_prefix: int = 50,
    now: dt.datetime | None = None,
    legs: list[dict[str, Any]] | None = None,
    legs_known: dict[str, bool] | None = None,
    store: Any | None = None,
) -> ParityReport:
    """Diff every declared key of every live-v1-producer unit and build the report.

    ``store`` is the same `GateStore` the report is published to. It is read —
    never written — for exactly one thing: the PREVIOUS trading day's report,
    which names the keys that day could only grade as settling. Absent, the
    report says so under `prior_day_settled` rather than omitting the block,
    because "we did not look" and "nothing was unsettled" are different facts.
    """
    from nousergon_lib.trading_calendar import previous_trading_day

    prior_day = previous_trading_day(trading_day)
    root = ShadowRoot(trading_day)
    reader = reader or S3Reader(bucket)
    units = units if units is not None else load_units()
    unit_by_id = {unit.unit_id: unit for unit in units}
    targets = expand_writes(units, trading_day)

    # alpha-engine-config-I10892: the live object each key is graded against is
    # the one v1's run manifest RECORDED for this trading day. Read once per
    # unit; a unit with no manifest (or a record with no measured version) keeps
    # the pre-I10892 behaviour of grading the current live object.
    v1_manifest_cache: dict[str, list[dict[str, Any]]] = {}

    def _expected(live_key: str, owners: list[str]) -> dict[str, Any] | None:
        manifests: list[dict[str, Any]] = []
        for unit_id in owners:
            unit = unit_by_id.get(unit_id)
            if unit is None:
                continue
            if unit_id not in v1_manifest_cache:
                v1_manifest_cache[unit_id] = list(
                    _latest_manifests_by_unit(
                        reader, f"{unit.run_manifest_prefix}/{trading_day.isoformat()}/"
                    ).values()
                )
            manifests.extend(v1_manifest_cache[unit_id])
        return recorded_live_version(manifests, live_key)

    # alpha-engine-config-I10890: the authoritative scope of THIS run is the
    # set of units that left a run manifest under the shadow prefix — never
    # "every in-service unit", which grades Saturday-only units against a
    # weekday run and always reds. An EMPTY manifest listing is read as "no
    # manifest evidence available" (e.g. a unit list handed in directly,
    # without ever running `shadow run`), not as "nothing ran" — it falls
    # back to grading every declared write, the old, more-conservative
    # behaviour, rather than silently excluding everything.
    shadow_manifest_prefix = root.key("data_collection/runs/")
    shadow_manifests = _latest_manifests_by_unit(reader, shadow_manifest_prefix)
    executed_units = set(shadow_manifests)
    scoped = bool(executed_units)

    rows: list[KeyResult] = []
    out_of_scope_units: set[str] = set()
    for target in targets:
        unit_ids = target.unit_id.split(",")
        if scoped and not (set(unit_ids) & executed_units):
            # Every unit declaring this write sat outside the run; it is
            # named once, per unit, in `excluded` below — never silently
            # dropped, and never a `shadow_missing` row a correct shadow run
            # could not have produced.
            out_of_scope_units.update(unit_ids)
            continue
        if target.kind == "arcticdb":
            rows.append(
                KeyResult(
                    f"arcticdb/{target.value}",
                    unit_ids,
                    "in_region_only",
                    "arcticdb",
                    {
                        "unmeasurable_reason": (
                            "ArcticDB is unreadable from the laptop (alpha-engine-config-I9771) "
                            "and this tool never opens it. The shadow run writes "
                            f"{root.arctic_library(target.value)!r}; comparing it against "
                            f"{target.value!r} is an in-region job."
                        ),
                        "shadow_library": root.arctic_library(target.value),
                    },
                )
            )
            continue
        if target.kind == "undiffable":
            rows.append(
                KeyResult(
                    target.declared, unit_ids, "unmeasurable", "none",
                    {"unmeasurable_reason": target.reason},
                )
            )
            continue
        if target.kind == "prefix":
            if scoped:
                # I10890 deliverable 2: the candidate set is the union of
                # what the RUN MANIFESTS say was written for this trading
                # day — never a live listing, which cannot tell a write made
                # today from a stale object left by a dropped holding, and
                # cannot bound itself to one day's worth of a high-cardinality
                # prefix (`news_aggregates_daily/…`, `close_history/{sym}`).
                v1_keys: set[str] = set()
                any_v1_manifest = False
                for unit_id in unit_ids:
                    unit = unit_by_id.get(unit_id)
                    if unit is None:
                        continue
                    v1_manifests = _latest_manifests_by_unit(
                        reader, f"{unit.run_manifest_prefix}/{trading_day.isoformat()}/"
                    )
                    if v1_manifests:
                        any_v1_manifest = True
                    for manifest in v1_manifests.values():
                        v1_keys |= _manifest_output_keys(manifest, target.value)
                if not any_v1_manifest:
                    rows.append(
                        KeyResult(
                            target.declared, unit_ids, "unmeasurable", "none",
                            {
                                "unmeasurable_reason": (
                                    "no v1 manifest for trading day — v1 wrote no "
                                    f"data_collection/runs/{{unit}}/{trading_day.isoformat()}/ "
                                    "record for any owning unit, so there is nothing to enumerate "
                                    "this family against for this day (I10890 deliverable 2)"
                                )
                            },
                        )
                    )
                    continue
                shadow_keys = set()
                for unit_id in unit_ids:
                    manifest = shadow_manifests.get(unit_id)
                    if manifest is not None:
                        shadow_keys |= _manifest_output_keys(manifest, target.value)
                members = v1_keys | shadow_keys
                if not members:
                    rows.append(
                        KeyResult(
                            target.declared, unit_ids, "both_missing", "none",
                            {
                                "detail": (
                                    f"neither manifest declares an output under {target.value!r} "
                                    "for this trading day"
                                )
                            },
                        )
                    )
                    continue
                for member in sorted(members):
                    rows.append(
                        _compare_one_key(
                            reader, root, member, unit_ids, rel_tolerance, absolute_tolerance,
                            expected=_expected(member, unit_ids),
                            trading_day=trading_day,
                            prior_day=prior_day,
                            shadow_manifests=shadow_manifests,
                        )
                    )
                continue
            # No manifest evidence at all (see `scoped` above) — fall back to
            # the pre-I10890 direct listing, capped and honestly unmeasurable
            # past the cap.
            try:
                live_keys = set(reader.list(target.value, max_keys_per_prefix))
                shadow_keys = {
                    root.live_key(k)
                    for k in reader.list(root.key(target.value), max_keys_per_prefix)
                }
            except Exception as exc:  # noqa: BLE001 - denied/failed list is UNMEASURABLE, named
                rows.append(
                    KeyResult(
                        target.declared, unit_ids, "unmeasurable", "none",
                        {"unmeasurable_reason": f"list failed: {type(exc).__name__}: {exc}"},
                    )
                )
                continue
            if not live_keys and not shadow_keys:
                rows.append(
                    KeyResult(
                        target.declared, unit_ids, "both_missing", "none",
                        {"detail": f"nothing under {target.value!r} on either side"},
                    )
                )
                continue
            for member in sorted(live_keys | shadow_keys)[:max_keys_per_prefix]:
                rows.append(
                    _compare_one_key(
                        reader, root, member, unit_ids, rel_tolerance, absolute_tolerance,
                        expected=_expected(member, unit_ids),
                        trading_day=trading_day,
                        prior_day=prior_day,
                        shadow_manifests=shadow_manifests,
                    )
                )
            if max(len(live_keys), len(shadow_keys)) >= max_keys_per_prefix:
                rows.append(
                    KeyResult(
                        target.declared, unit_ids, "unmeasurable", "none",
                        {
                            "unmeasurable_reason": (
                                f"the listing hit --max-keys-per-prefix={max_keys_per_prefix}; "
                                "the family is only partially compared, so it is reported as "
                                "unmeasurable rather than as a pass over a truncated sample"
                            )
                        },
                    )
                )
            continue
        rows.append(
            _compare_one_key(
                reader, root, target.value, unit_ids, rel_tolerance, absolute_tolerance,
                expected=_expected(target.value, unit_ids),
                trading_day=trading_day,
                prior_day=prior_day,
                shadow_manifests=shadow_manifests,
            )
        )

    excluded = [
        {"unit_id": unit_id, "reason": reason, "class": "hand_excluded"}
        for unit_id, reason in sorted(EXCLUDED_UNITS.items())
    ]
    for unit in units:
        lifecycle = str(unit.raw.get("lifecycle"))
        if unit.unit_id in EXCLUDED_UNITS:
            continue
        if lifecycle not in LIVE_LIFECYCLES:
            excluded.append(
                {
                    "unit_id": unit.unit_id,
                    "reason": f"lifecycle {lifecycle!r} — no live v1 producer",
                    "class": "lifecycle",
                }
            )
        elif unit.unit_id in out_of_scope_units:
            excluded.append(
                {
                    "unit_id": unit.unit_id,
                    "reason": (
                        f"out_of_run_scope: no run manifest for this unit under the shadow run's "
                        f"prefix {shadow_manifest_prefix!r} — it was not executed by this run "
                        f"(executed units: {sorted(executed_units) or ['none']}); a weekday shadow "
                        "run never covers a Saturday-only unit, and this is not a parity failure"
                    ),
                    "class": "out_of_run_scope",
                }
            )

    deduped = _dedupe_rows(rows)
    _attach_key_date_prior_day_settled(
        deduped,
        reader=reader,
        unit_by_id=unit_by_id,
        trading_day=trading_day,
        prior_day=prior_day,
        rel_tolerance=rel_tolerance,
        absolute_tolerance=absolute_tolerance,
    )
    stamp = (now or dt.datetime.now(dt.timezone.utc)).replace(microsecond=0).isoformat()
    return ParityReport(
        trading_day=trading_day,
        bucket=bucket,
        shadow_prefix=root.prefix,
        code_sha=code_sha,
        rows=deduped,
        excluded=sorted(excluded, key=lambda e: e["unit_id"]),
        rel_tolerance=rel_tolerance,
        absolute_tolerance=absolute_tolerance,
        generated_at=stamp.replace("+00:00", "Z"),
        legs=list(legs or []),
        legs_known=dict(legs_known or {}),
        prior_day_settled=grade_prior_day_settled(
            store, trading_day=trading_day, prior_day=prior_day, rows=deduped
        ),
    )


#: Why one key that carried a `settling_bar` block yesterday cannot be re-graded
#: on today's report. Each is a PROPERTY OF THE ARTIFACT, not of the read, and
#: each is published so the `prior_day_settled` denominator is auditable rather
#: than quietly shrinking to the keys that happened to work.
_PRIOR_UNMEASURABLE_REASONS = {
    "not_in_this_report": (
        "this report has no row for the key at all — the unit was out of this run's scope, "
        "or the key is no longer declared"
    ),
    "no_prior_row": (
        "the key has a row_date settling basis, but no row indexed by the previous trading "
        "day is present on EITHER side of this report — the two sides never shared that row "
        "(e.g. a newly listed or delisted ticker), not a shape this basis cannot re-grade"
    ),
    "row_not_compared": (
        "the key has a row_date settling basis but this report compared no row for the "
        "previous trading day (the two sides did not share that row)"
    ),
    "overwritten_in_place": (
        "the key has a same_day_snapshot settling basis — an undated artifact overwritten in "
        "place — so the previous trading day's bar no longer exists anywhere to re-read. "
        "Re-grading it would need a versioned read (the S3 VersionId pattern "
        "`alpha-engine-config-I10892` already uses for `_read_live_as_recorded`); noted as the "
        "future fix and left, never silently forgiven (alpha-engine-config-I11360 deliverable 2)"
    ),
    "prior_pair_unreadable": (
        "the key has a key_date settling basis and this report DID attempt the D-1 pair "
        "re-grade (alpha-engine-config-I11360), but that re-grade did not come back a plain "
        "match/mismatch (missing on one side, superseded, or itself unmeasurable) — see this "
        "report's own row for the D-dated key, its `prior_day_settled.unmeasurable_reason`, "
        "for what actually happened"
    ),
}


def grade_prior_day_settled(
    store: Any, *, trading_day: dt.date, prior_day: dt.date, rows: list[KeyResult]
) -> dict[str, Any]:
    """Whether yesterday's unsettled bars settled, measured on today's report.

    Deliverable 2 of `alpha-engine-config-I11351`, extended by
    `alpha-engine-config-I11360` to `key_date` keys. Yesterday's report names
    the keys whose trading-day cells could not be graded; on THIS report that
    day's bar is re-read and held to the strict band. The pairing is what
    turns "we could not measure today's bar" into a measured claim one day
    later.

    Two different SHAPES carry that re-grade, because a `row_date` key and a
    `key_date` key disagree on WHERE yesterday's row lives:

    * `row_date` (`reference/price_cache/*.parquet`) carries its own history,
      so the re-grade sits in `prior_day` on the SAME key's row — looked up
      here by that exact key string.
    * `key_date` (`staging/daily_closes/{date}.parquet`,
      `features/{date}/*.parquet`, `market_data/eod_closes/{date}.json`)
      files each day under its OWN dated key, so yesterday's key does not
      exist as a row on today's report at all. The re-grade instead sits in
      `prior_day_settled` on the row for TODAY's key — `rebased_key_date_key`
      maps yesterday's key forward to find it
      (`shadow.parity._attach_key_date_prior_day_settled` populated it).

    `same_day_snapshot` (`market_data/technicals/latest.json` and friends) has
    neither shape: the artifact is overwritten in place, so it is always
    `overwritten_in_place`-unmeasurable, by construction.

    Never silently empty: with no store handed in, or no report for the
    previous trading day, the block says which, and a key that cannot be
    re-graded is counted as `unmeasurable` with its reason named — never as
    settled.
    """
    if store is None:
        return {
            "trading_day": prior_day.isoformat(),
            "available": False,
            "reason": "no store was handed to the comparator, so the previous report was not read",
        }
    prior_key = parity_key(prior_day)
    try:
        raw = store.get_bytes(prior_key)
        document = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - recorded as an unavailable block, never swallowed
        # (a) the failure swallowed is "the previous report is absent or
        # unreadable"; (b) the primary deliverable — this day's parity report —
        # is unaffected; (c) it is recorded here, in the published report.
        return {
            "trading_day": prior_day.isoformat(),
            "report": prior_key,
            "available": False,
            "reason": f"{type(exc).__name__}: {exc}",
        }

    settling_yesterday_rows = [
        row
        for row in document.get("keys") or []
        if int((row.get("settling_bar") or {}).get("cells") or 0)
    ]
    settling_yesterday = [str(row.get("key") or "") for row in settling_yesterday_rows]
    basis_by_key = {
        str(row.get("key") or ""): str((row.get("settling_bar") or {}).get("basis") or "")
        for row in settling_yesterday_rows
    }
    by_key = {row.key: row for row in rows}
    settled: list[str] = []
    unsettled: list[dict[str, Any]] = []
    unmeasurable: dict[str, list[str]] = {}
    for key in settling_yesterday:
        basis = basis_by_key.get(key, "")
        if basis == "same_day_snapshot":
            # Never re-graded, whether or not this report happens to carry a
            # row for the key at all — the artifact is overwritten in place,
            # a property of the ARTIFACT, not of whether it was in scope
            # today. Checked before the row lookup so it is never shadowed
            # by `not_in_this_report`.
            unmeasurable.setdefault("overwritten_in_place", []).append(key)
            continue
        lookup_key = key
        if basis == "key_date":
            lookup_key = rebased_key_date_key(key, prior_day, trading_day) or key
        row = by_key.get(lookup_key)
        if row is None:
            unmeasurable.setdefault("not_in_this_report", []).append(key)
            continue
        if basis == "key_date":
            prior = row.body.get("prior_day_settled")
            if not prior:
                # `_attach_key_date_prior_day_settled` runs over every
                # `key_date` row this report has, so an absent field here
                # means the row itself no longer carries a key_date
                # settling_bar today (e.g. the contract changed) rather than
                # a normal unmeasurable outcome.
                unmeasurable.setdefault("not_in_this_report", []).append(key)
                continue
            if not prior.get("available"):
                unmeasurable.setdefault("prior_pair_unreadable", []).append(key)
                continue
        else:
            prior = row.body.get("prior_day")
            if not prior:
                unmeasurable.setdefault("no_prior_row", []).append(key)
                continue
            if not prior.get("rows_compared"):
                unmeasurable.setdefault("row_not_compared", []).append(key)
                continue
        if prior.get("settled"):
            settled.append(key)
        else:
            unsettled.append({"key": key, "breaches": prior.get("breaches")})
    return {
        "trading_day": prior_day.isoformat(),
        "report": prior_key,
        "available": True,
        "keys_with_settling_bar": len(settling_yesterday),
        "settled": len(settled),
        "unsettled": len(unsettled),
        "unmeasurable": sum(len(keys) for keys in unmeasurable.values()),
        "unmeasurable_reasons": {
            reason: {
                "count": len(keys),
                "why": _PRIOR_UNMEASURABLE_REASONS[reason],
                "examples": sorted(keys)[:10],
            }
            for reason, keys in sorted(unmeasurable.items())
        },
        "unsettled_examples": sorted(unsettled, key=lambda e: str(e["key"]))[:10],
    }
