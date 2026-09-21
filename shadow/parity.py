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
    "ContractSchema",
    "KeyResult",
    "ParityReport",
    "compare_bytes",
    "expand_writes",
    "parity_key",
    "resolve_contract",
    "run_parity",
]

#: Where the report is published, relative to the `data_collection` store root
#: — i.e. `s3://alpha-engine-research/data_collection/parity/{trading_day}.json`.
#: The constant is DEFINED by the consumer (`data_gate.evidence`) and imported
#: here, not re-spelled: the gate declares where it reads, and the producer
#: writes there by construction rather than by two files agreeing.
PARITY_KEY_TEMPLATE = evidence.PARITY_KEY_TEMPLATE

PARITY_SCHEMA_VERSION = "data_parity_report.v1"

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
                )
            )
    return tuple(schemas)


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


def _compare_frames(
    live, shadow, rel: float, absolute: float, provenance_columns: frozenset[str] = frozenset()
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
        for col in data_columns:
            compared += 1
            if not _numeric_close(live_row[col], shadow_row[col], rel, absolute):
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
            live_frame, shadow_frame, compare_rel, compare_abs, provenance_fields
        )
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
        }
        for row in self.rows:
            counts[row.verdict] = counts.get(row.verdict, 0) + 1
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
            "legs_known": bool(self.legs),
            "met": self.met,
            "summary": self.summary,
            "excluded_units": self.excluded,
            "keys": [row.as_dict() for row in self.rows],
        }


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
        return KeyResult(
            live_key, unit_ids, "shadow_missing", "none",
            {"detail": "v1 wrote it; the shadow run did not", "shadow_key": shadow_key},
        )
    body = compare_bytes(
        live_key, live, shadow, rel=rel, absolute=absolute, contract=resolve_contract(live_key)
    )
    body["shadow_key"] = shadow_key
    body["live_version"] = live_version
    return KeyResult(live_key, unit_ids, body.pop("verdict"), body.pop("comparator"), body)


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
) -> ParityReport:
    """Diff every declared key of every live-v1-producer unit and build the report."""
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

    stamp = (now or dt.datetime.now(dt.timezone.utc)).replace(microsecond=0).isoformat()
    return ParityReport(
        trading_day=trading_day,
        bucket=bucket,
        shadow_prefix=root.prefix,
        code_sha=code_sha,
        rows=_dedupe_rows(rows),
        excluded=sorted(excluded, key=lambda e: e["unit_id"]),
        rel_tolerance=rel_tolerance,
        absolute_tolerance=absolute_tolerance,
        generated_at=stamp.replace("+00:00", "Z"),
        legs=list(legs or []),
    )
