"""features/gen_parquet_contract.py — generate contracts/features_parquet.schema.json
from features/registry.py::CATALOG (alpha-engine-config-I10793, ruling 2026-10-14
option (a)).

Background: the per-COLUMN contract for ``features/{date}/{group}.parquet`` used
to exist only as ``CATALOG <-> SCHEMA.md §3`` (``tests/test_schema_contract.py``),
which no consumer repository can pin: a Python list and a Markdown table are not
a file a reader in another repository can validate against. The two parquet
readers (crucible-research ``scoring/factor_scoring.py`` and crucible-dashboard
``views/13_Feature_Store.py``) were therefore declared as unpinned consumers on
D31/D50, and ``data.D50.schema_contract`` read UNMET.

This script renders the column contract as a JSON Schema MECHANICALLY from
``CATALOG``, so the pinned file and the catalog cannot drift by construction:
``tests/test_features_parquet_contract.py`` fails CI when the committed file
disagrees with a fresh render.

Shape. One trading day's snapshot, written as ``{group: [row, ...]}``: each
top-level property is one ``features/{date}/{group}.parquet`` file and each array
item is one parquet row (``DataFrame.to_dict("records")``, NaN as ``null``).
Per group:

* ``ticker`` (per-ticker groups) and ``date`` are REQUIRED, exactly as
  ``features/writer.py::snapshot_group_frames`` writes them;
* every ``CATALOG`` column of the group is declared with its JSON type;
  feature columns are NOT required, because the writer skips a column that was
  not computed this run rather than writing it empty;
* ``additionalProperties: false`` — a column the catalog does not declare is a
  contract violation, which is what makes the pin protect a reader.

The ``x-key-pattern`` / ``x-settling-bar`` annotations move here from
``feature_registry.schema.json`` unchanged (same templates, same ``key_date``
basis), so ``shadow.parity`` grades every parquet key exactly as before. The
``x-*`` and ``description`` keywords are annotations: ``data_gate.consumer_pins``
strips them before comparing a pinned copy's shape, so editing them never
stales a consumer's pin.

Usage:
    python3 features/gen_parquet_contract.py           # print the rendered schema
    python3 features/gen_parquet_contract.py --write   # rewrite the committed file
    python3 features/gen_parquet_contract.py --check   # exit 1 if it is stale
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from features.registry import CATALOG, GROUPS  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = REPO_ROOT / "contracts" / "features_parquet.schema.json"

#: CATALOG ``dtype`` -> the JSON type a parquet cell of it reads as. ``null``
#: is always allowed on a feature column: a ticker whose indicator could not
#: be computed this run carries NaN, which a records conversion renders as
#: ``null``. A dtype not in this map refuses to render, so a new dtype is a
#: deliberate edit here rather than a silently wrong type.
_DTYPE_JSON_TYPE: dict[str, str] = {
    "float32": "number",
    "float64": "number",
    "int32": "integer",
    "int64": "integer",
    "bool": "boolean",
    "string": "string",
}

#: The identity columns ``snapshot_group_frames`` writes ahead of the features.
_TICKER = {"type": "string", "minLength": 1, "description": "Ticker symbol (one row per ticker)."}
_DATE = {
    "type": "string",
    "pattern": r"^\d{4}-\d{2}-\d{2}$",
    "description": "The snapshot's trading day, YYYY-MM-DD (the {date} path segment).",
}

#: Moved verbatim from feature_registry.schema.json (alpha-engine-config-I11351):
#: the parity comparator resolves a key to the contract declaring it, so these
#: are what keep every parquet key graded exactly as before the split.
#: `factor_loading.parquet` is deliberately NOT added: it has never resolved to
#: a contract (`*` in a template is literal, not a glob), and giving it a
#: settling bar is a parity-grading change, not part of this split.
KEY_PATTERNS: tuple[str, ...] = (
    "features/{date}/technical.parquet",
    "features/{date}/fundamental.parquet",
    "features/{date}/interaction.parquet",
    "features/{date}/macro.parquet",
    "features/{date}/alternative.parquet",
    "features/{date}/*.parquet",
)

SETTLING_BAR: dict[str, str] = {
    "basis": "key_date",
    "rationale": (
        "Ticker-indexed and dated by the path segment, same shape as staging/daily_closes: "
        "every indicator in features/{date}/*.parquet reads the last bar, so on a same-day run "
        "the whole file is a derivation of the unsettled session (24,961 technical / 3,328 "
        "interaction / 508 alternative / 4 macro breaches measured on 2026-09-21). Graded as "
        "settling only when the key names the report's trading_day (alpha-engine-config-I11351)."
    ),
}


def _column_schema(entry) -> dict[str, Any]:
    try:
        json_type = _DTYPE_JSON_TYPE[entry.dtype]
    except KeyError:
        raise ValueError(
            f"CATALOG entry {entry.name!r} has dtype {entry.dtype!r}, which has no JSON type in "
            "features/gen_parquet_contract.py::_DTYPE_JSON_TYPE. Add the mapping deliberately."
        ) from None
    return {"type": [json_type, "null"], "description": entry.description}


def render_group(group: str) -> dict[str, Any]:
    """The row schema of ``features/{date}/{group}.parquet``."""
    entries = [e for e in CATALOG if e.group == group]
    per_ticker = {e.per_ticker for e in entries}
    if len(per_ticker) != 1:
        raise ValueError(f"group {group!r} mixes per_ticker True and False; the writer cannot")
    properties: dict[str, Any] = {}
    required: list[str] = []
    if per_ticker.pop():
        properties["ticker"] = dict(_TICKER)
        required.append("ticker")
    properties["date"] = dict(_DATE)
    required.append("date")
    for entry in entries:
        if entry.name in properties:
            raise ValueError(f"CATALOG column {entry.name!r} collides with an identity column")
        properties[entry.name] = _column_schema(entry)
    return {
        "type": "object",
        "description": f"One row of features/{{date}}/{group}.parquet.",
        "required": required,
        "properties": properties,
        "additionalProperties": False,
    }


def render() -> dict[str, Any]:
    """The whole contract document, in a stable key order."""
    groups = list(GROUPS)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "nousergon-data/contracts/features_parquet.schema.json",
        "title": "Feature snapshot columns (features/{date}/{group}.parquet, D31/D50)",
        "description": (
            "GENERATED from features/registry.py::CATALOG by features/gen_parquet_contract.py "
            "-- do not hand-edit; regenerate with `python3 features/gen_parquet_contract.py "
            "--write`. One trading day's snapshot as {group: [row, ...]}: each property is one "
            "features/{date}/{group}.parquet file and each item one parquet row "
            "(DataFrame.to_dict('records'), NaN as null). Producer: "
            "features/writer.py::snapshot_group_frames (D31 daily, D50 settled regrade). "
            "Consumers pin a copy: crucible-research scoring/factor_scoring.py and "
            "crucible-dashboard views/13_Feature_Store.py (alpha-engine-config-I10793). "
            "features/registry.json is a SEPARATE artifact with its own contract, "
            "contracts/feature_registry.schema.json."
        ),
        "type": "object",
        "minProperties": 1,
        "additionalProperties": False,
        "properties": {
            group: {"type": "array", "items": {"$ref": f"#/$defs/{group}"}} for group in groups
        },
        "$defs": {group: render_group(group) for group in groups},
        "x-generated-from": "features/registry.py::CATALOG",
        "x-settling-bar": dict(SETTLING_BAR),
        "x-key-pattern": list(KEY_PATTERNS),
    }


def render_text() -> str:
    return json.dumps(render(), indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str]) -> int:
    text = render_text()
    if "--write" in argv:
        CONTRACT_PATH.write_text(text, encoding="utf-8")
        print(f"wrote {CONTRACT_PATH.relative_to(REPO_ROOT)}")
        return 0
    if "--check" in argv:
        committed = CONTRACT_PATH.read_text(encoding="utf-8") if CONTRACT_PATH.exists() else ""
        if committed != text:
            print(
                f"{CONTRACT_PATH.relative_to(REPO_ROOT)} is stale against CATALOG; run "
                "`python3 features/gen_parquet_contract.py --write`",
                file=sys.stderr,
            )
            return 1
        return 0
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
