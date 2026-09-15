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
import hashlib
import io
import json
import math
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
    "KeyResult",
    "ParityReport",
    "compare_bytes",
    "expand_writes",
    "parity_key",
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


def parity_key(trading_day: dt.date) -> str:
    return PARITY_KEY_TEMPLATE.format(trading_day=trading_day.isoformat())


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


def _compare_frames(live, shadow, rel: float, absolute: float) -> dict[str, Any]:
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
    if indexed_shadow is None:
        # The shadow side has no identifier column at all; the schema block
        # above already carries that as a mismatch, and comparing positionally
        # would invent an alignment nobody declared.
        out["values"] = {
            "compared_cells": 0,
            "breaches": 1,
            "examples": [{"reason": f"the shadow side has no {column!r} column to align on"}],
        }
        return out
    shared_columns = [c for c in indexed_live.columns if c in indexed_shadow.columns]
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
        for col in shared_columns:
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
    out["values"] = {
        "compared_cells": compared,
        "breaches": breach_count,
        "examples": breaches[:10],
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


def _json_breaches(live: Any, shadow: Any, rel: float, absolute: float, path: str = "$") -> list[str]:
    if isinstance(live, dict) and isinstance(shadow, dict):
        out: list[str] = []
        for key in sorted(set(live) | set(shadow)):
            if key not in live:
                out.append(f"{path}.{key}: only in shadow")
            elif key not in shadow:
                out.append(f"{path}.{key}: only in live")
            else:
                out.extend(_json_breaches(live[key], shadow[key], rel, absolute, f"{path}.{key}"))
        return out
    if isinstance(live, list) and isinstance(shadow, list):
        if len(live) != len(shadow):
            return [f"{path}: length {len(live)} live vs {len(shadow)} shadow"]
        out = []
        for index, (a, b) in enumerate(zip(live, shadow, strict=True)):
            out.extend(_json_breaches(a, b, rel, absolute, f"{path}[{index}]"))
        return out
    if isinstance(live, (int, float)) and isinstance(shadow, (int, float)):
        return [] if _numeric_close(live, shadow, rel, absolute) else [
            f"{path}: {live!r} live vs {shadow!r} shadow"
        ]
    return [] if live == shadow else [f"{path}: {live!r} live vs {shadow!r} shadow"]


def compare_bytes(
    key: str, live: bytes, shadow: bytes, *, rel: float, absolute: float
) -> dict[str, Any]:
    """Compare one key's two payloads. Returns the row body; never raises for data."""
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
        body = _compare_frames(live_frame, shadow_frame, rel, absolute)
        matched = (
            body["row_count"]["live"] == body["row_count"]["shadow"]
            and body["schema"]["match"]
            and not body["symbol_set"]["only_live"]
            and not body["symbol_set"]["only_shadow"]
            and body["values"]["breaches"] == 0
        )
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
        breaches = _json_breaches(live_doc, shadow_doc, rel, absolute)
        return {
            "comparator": "json",
            "verdict": "match" if not breaches else "mismatch",
            "values": {"breaches": len(breaches), "examples": breaches[:10]},
            "row_count": {
                "live": len(live_doc) if isinstance(live_doc, (list, dict)) else 1,
                "shadow": len(shadow_doc) if isinstance(shadow_doc, (list, dict)) else 1,
            },
        }
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


def _compare_one_key(
    reader: S3Reader, root: ShadowRoot, live_key: str, unit_ids: list[str], rel, absolute
) -> KeyResult:
    shadow_key = root.key(live_key)
    try:
        live = reader.get(live_key)
        shadow = reader.get(shadow_key)
    except Exception as exc:  # noqa: BLE001 - a denied/failed read is UNMEASURABLE, and named
        return KeyResult(
            live_key,
            unit_ids,
            "unmeasurable",
            "none",
            {"unmeasurable_reason": f"{type(exc).__name__}: {exc}", "shadow_key": shadow_key},
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
    body = compare_bytes(live_key, live, shadow, rel=rel, absolute=absolute)
    body["shadow_key"] = shadow_key
    return KeyResult(live_key, unit_ids, body.pop("verdict"), body.pop("comparator"), body)


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
) -> ParityReport:
    """Diff every declared key of every live-v1-producer unit and build the report."""
    root = ShadowRoot(trading_day)
    reader = reader or S3Reader(bucket)
    units = units if units is not None else load_units()
    targets = expand_writes(units, trading_day)

    rows: list[KeyResult] = []
    for target in targets:
        unit_ids = target.unit_id.split(",")
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
                rows.append(_compare_one_key(reader, root, member, unit_ids, rel_tolerance, absolute_tolerance))
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
            _compare_one_key(reader, root, target.value, unit_ids, rel_tolerance, absolute_tolerance)
        )

    excluded = [{"unit_id": unit_id, "reason": reason} for unit_id, reason in sorted(EXCLUDED_UNITS.items())]
    for unit in units:
        lifecycle = str(unit.raw.get("lifecycle"))
        if unit.unit_id not in EXCLUDED_UNITS and lifecycle not in LIVE_LIFECYCLES:
            excluded.append(
                {"unit_id": unit.unit_id, "reason": f"lifecycle {lifecycle!r} — no live v1 producer"}
            )

    stamp = (now or dt.datetime.now(dt.timezone.utc)).replace(microsecond=0).isoformat()
    return ParityReport(
        trading_day=trading_day,
        bucket=bucket,
        shadow_prefix=root.prefix,
        code_sha=code_sha,
        rows=rows,
        excluded=sorted(excluded, key=lambda e: e["unit_id"]),
        rel_tolerance=rel_tolerance,
        absolute_tolerance=absolute_tolerance,
        generated_at=stamp.replace("+00:00", "Z"),
    )
