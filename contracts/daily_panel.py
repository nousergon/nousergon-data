"""The daily panel contract: ONE definition of its keys, columns, sidecar and parity.

`alpha-engine-config-I10791` (plan P-25, amendment 1; foundation rule
`architecture.d/146`). The daily price panel is a DATA-COLLECTOR product: the
collector compiles it once from the ArcticDB ``universe`` library and publishes

* ``data_collection/panel/{trading_day}/panel.parquet`` — the long panel, one row
  per (trading_day, ticker), rows governed by ``daily_panel.schema.json``;
* ``data_collection/panel/{trading_day}/manifest.json`` — written LAST, binding
  the parquet to its contract version and content hash
  (``daily_panel_manifest.schema.json``);

and crucible v2's ``data.daily`` reads it instead of compiling a second copy of
the same universe from raw ArcticDB (audit gap 10's double compile).

Before the reader switches, one REAL trading day is compared side by side —
the published panel against the panel the consumer compiled directly for the
same session — and the result is a receipt,
``data_collection/panel/{trading_day}/parity.json``
(``daily_panel_parity.schema.json``). :func:`compare_panels` below is the only
definition of "equivalent within declared tolerance"; the gate re-reads the
receipt and refuses one computed under any tolerance but
:data:`PARITY_TOLERANCE`.

Everything a producer, the gate and the producer test need to agree on lives
here, so the three cannot drift: a key template restated in the publisher and
again in the gate is two keys the day one of them changes.

Change rule (plan §4.3, `architecture.d/146` rule 3): a change to
:data:`PANEL_COLUMNS` or to either schema is a cross-component PR naming the
consumer in :data:`CONSUMER`, and the consumer's pin moves first.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "CONSUMER",
    "KEY_PREFIX",
    "MANIFEST_SCHEMA_VERSION",
    "PANEL_COLUMNS",
    "PANEL_SCHEMA_VERSION",
    "PARITY_SCHEMA_VERSION",
    "PARITY_TOLERANCE",
    "PRICE_COLUMNS",
    "SCHEMA_FILES",
    "PanelConsumer",
    "build_manifest",
    "compare_panels",
    "manifest_key",
    "panel_key",
    "panel_row_records",
    "parity_key",
    "schema_problems",
    "sha256_hex",
    "store_relative",
    "validate_panel_frame",
]

PANEL_SCHEMA_VERSION = "daily_panel.v1"
MANIFEST_SCHEMA_VERSION = "data_daily_panel_manifest.v1"
PARITY_SCHEMA_VERSION = "data_daily_panel_parity.v1"

#: Bucket-relative. The gate's store is rooted at ``data_collection/``
#: (:func:`store_relative`), which the gate role already reads — no new grant.
KEY_PREFIX = "data_collection/panel/"
_STORE_ROOT = "data_collection/"

#: crucible's ``crucible/data/sources.py::PANEL_COLUMNS``, in its order. The
#: panel a consumer reads after the switch must be the shape it compiled before
#: it, or the switch is a schema change dressed as a source change.
PANEL_COLUMNS: tuple[str, ...] = (
    "trading_day",
    "ticker",
    "open_raw",
    "high_raw",
    "low_raw",
    "close_raw",
    "volume_raw",
)
PRICE_COLUMNS: tuple[str, ...] = ("open_raw", "high_raw", "low_raw", "close_raw")
VALUE_COLUMNS: tuple[str, ...] = (*PRICE_COLUMNS, "volume_raw")

#: The declared parity tolerance. Both sides read the SAME ArcticDB library and
#: parquet round-trips float64 exactly, so the honest expectation is equality;
#: ``price_rel`` admits only float noise and ``volume_abs`` admits none.
#: PROPOSED (alpha-engine-config-I10791): ratifying or loosening it is Brian's
#: threshold call, and a loosening is a PR to this constant, never a receipt
#: carrying its own.
PARITY_TOLERANCE: dict[str, float] = {"price_rel": 1e-9, "volume_abs": 0.0}

_SCHEMA_DIR = Path(__file__).parent
SCHEMA_FILES: dict[str, str] = {
    "row": "contracts/daily_panel.schema.json",
    "manifest": "contracts/daily_panel_manifest.schema.json",
    "parity": "contracts/daily_panel_parity.schema.json",
}


@dataclass(frozen=True)
class PanelConsumer:
    """The ONE pinned consumer the acceptance clause grades (plan amendment 1).

    * ``pin_path`` — the consumer's byte-for-shape copy of the row schema.
    * ``read_site`` — ``path::Symbol`` of the class that reads the published
      panel; it must exist on the consumer's default branch.
    * ``entrypoint`` — ``path::function`` of the job whose direct compile the
      panel replaces. After the switch its body references ``read_site``'s
      symbol and NONE of ``direct_compile_names``: the old path is removed, not
      left beside the new one as a silent second implementation.
    """

    repo: str
    pin_path: str
    read_site: str
    entrypoint: str
    direct_compile_names: tuple[str, ...]

    @property
    def read_site_symbol(self) -> str:
        return self.read_site.split("::", 1)[1]

    @property
    def entrypoint_path(self) -> str:
        return self.entrypoint.split("::", 1)[0]

    @property
    def entrypoint_function(self) -> str:
        return self.entrypoint.split("::", 1)[1]


#: crucible v2 ``data.daily`` (unit D47). ``handle_data_daily`` builds its
#: source today through ``_source`` -> ``ArcticPriceSource``; those are the
#: names that must leave its body. The read-site name is the one the crucible
#: consumer PR introduces — declared here so the clause names exactly what it
#: is waiting for rather than guessing at it.
CONSUMER = PanelConsumer(
    repo="crucible",
    pin_path="tests/contracts/daily_panel.schema.json",
    read_site="crucible/data/sources.py::PublishedPanelSource",
    entrypoint="crucible/track_a.py::handle_data_daily",
    direct_compile_names=("ArcticPriceSource", "_source", "load_universe_ohlcv"),
)


def _day(value: dt.date | str) -> str:
    return value if isinstance(value, str) else value.isoformat()


def panel_key(trading_day: dt.date | str) -> str:
    return f"{KEY_PREFIX}{_day(trading_day)}/panel.parquet"


def manifest_key(trading_day: dt.date | str) -> str:
    return f"{KEY_PREFIX}{_day(trading_day)}/manifest.json"


def parity_key(trading_day: dt.date | str) -> str:
    return f"{KEY_PREFIX}{_day(trading_day)}/parity.json"


def store_relative(key: str) -> str:
    """A bucket key as the gate store (rooted at ``data_collection/``) addresses it."""
    if not key.startswith(_STORE_ROOT):
        raise ValueError(f"{key!r} is not under {_STORE_ROOT!r}")
    return key[len(_STORE_ROOT) :]


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_schema(kind: str) -> dict:
    return json.loads((_SCHEMA_DIR.parent / SCHEMA_FILES[kind]).read_text(encoding="utf-8"))


def schema_problems(document: Any, kind: str) -> list[str]:
    """Every way ``document`` breaks the ``kind`` schema (``row``/``manifest``/``parity``).

    STRICT, unlike ``contracts._validate``: a missing ``jsonschema`` raises
    instead of returning ``[]``, because a gate that cannot validate must not
    read as a gate that validated.
    """
    import jsonschema

    validator = jsonschema.Draft202012Validator(load_schema(kind))
    return [f"{e.json_path}: {e.message}" for e in list(validator.iter_errors(document))[:10]]


def panel_row_records(panel: pd.DataFrame, *, limit: int | None = None) -> list[dict]:
    """Rows as the row schema validates them: ``trading_day`` rendered ISO."""
    frame = panel if limit is None else panel.head(limit)
    records = frame.to_dict("records")
    for record in records:
        day = record.get("trading_day")
        if isinstance(day, (dt.date, dt.datetime)):
            record["trading_day"] = day.isoformat()[:10]
        for column in VALUE_COLUMNS:
            value = record.get(column)
            if hasattr(value, "item"):
                record[column] = value.item()
    return records


def validate_panel_frame(panel: pd.DataFrame, *, trading_day: dt.date) -> list[str]:
    """Every structural way ``panel`` is not a ``daily_panel.v1`` panel ending ``trading_day``.

    Whole-frame facts the per-row schema cannot see: exact column ORDER, key
    uniqueness and sort, a window that ends on ``trading_day`` and carries
    rows for it, and no null or non-positive value anywhere. Returns ``[]``
    when the panel is publishable.
    """
    import pandas as pd

    problems: list[str] = []
    if tuple(panel.columns) != PANEL_COLUMNS:
        return [f"columns {list(panel.columns)} != contract {list(PANEL_COLUMNS)}"]
    if panel.empty:
        return ["the panel has no rows"]
    days = pd.Series(panel["trading_day"])
    if not all(isinstance(d, dt.date) and not isinstance(d, dt.datetime) for d in days):
        problems.append("trading_day must hold datetime.date values (no timestamps)")
        return problems
    if not all(isinstance(t, str) and t for t in panel["ticker"]):
        problems.append("ticker must be a non-empty string on every row")
    if panel.duplicated(subset=["trading_day", "ticker"]).any():
        problems.append("duplicate (trading_day, ticker) rows")
    ordered = panel.sort_values(["trading_day", "ticker"]).reset_index(drop=True)
    if not ordered[["trading_day", "ticker"]].equals(panel[["trading_day", "ticker"]].reset_index(drop=True)):
        problems.append("rows are not sorted by (trading_day, ticker)")
    if days.max() != trading_day:
        problems.append(f"the window ends on {days.max()}, not on the panel's trading_day {trading_day}")
    for column in VALUE_COLUMNS:
        values = pd.to_numeric(panel[column], errors="coerce")
        nulls = int(values.isna().sum())
        if nulls:
            problems.append(f"{column}: {nulls} null/non-numeric value(s) — never zero-filled, never published")
            continue
        bad = int((values < 0).sum()) if column == "volume_raw" else int((values <= 0).sum())
        if bad:
            problems.append(f"{column}: {bad} value(s) out of range")
    return problems


def build_manifest(
    panel: pd.DataFrame,
    payload: bytes,
    *,
    trading_day: dt.date,
    lookback_calendar_days: int,
    module: str,
    code_sha: str,
    generated_at: dt.datetime,
) -> dict:
    """The ``data_daily_panel_manifest.v1`` sidecar for ``panel`` serialized as ``payload``."""
    on_day = panel[panel["trading_day"] == trading_day]
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "panel_schema_version": PANEL_SCHEMA_VERSION,
        "trading_day": trading_day.isoformat(),
        "panel_key": panel_key(trading_day),
        "panel_sha256": sha256_hex(payload),
        "panel_bytes": len(payload),
        "columns": list(panel.columns),
        "row_count": int(len(panel)),
        "symbol_count": int(panel["ticker"].nunique()),
        "symbols_on_trading_day": int(on_day["ticker"].nunique()),
        "session_count": int(panel["trading_day"].nunique()),
        "first_session": min(panel["trading_day"]).isoformat(),
        "last_session": max(panel["trading_day"]).isoformat(),
        "lookback_calendar_days": int(lookback_calendar_days),
        "source": {"store": "arcticdb", "library": "universe"},
        "generated_at": generated_at.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "producer": {"module": module, "code_sha": code_sha},
    }


def _side(panel: pd.DataFrame, *, key: str, sha256: str) -> dict:
    last = max(panel["trading_day"]) if len(panel) else None
    return {
        "key": key,
        "sha256": sha256,
        "trading_day": last.isoformat() if last is not None else "0000-00-00",
        "rows": int(len(panel)),
    }


def compare_panels(
    producer: pd.DataFrame,
    consumer: pd.DataFrame,
    *,
    trading_day: dt.date,
    producer_key: str,
    producer_sha256: str,
    consumer_key: str,
    consumer_sha256: str,
    generated_at: dt.datetime,
    tolerance: dict[str, float] | None = None,
) -> dict:
    """The ``data_daily_panel_parity.v1`` receipt for one same-day comparison.

    The comparison region is the CONSUMER's: its tickers, over its window
    ``[first, last]`` session. Inside it, a consumer row the published panel
    lacks (``missing_in_producer``) and a published row the consumer did not
    read (``missing_in_consumer``) are both mismatches — the published panel
    may carry MORE tickers or a deeper window than one consumer reads, never
    different rows inside what it reads. ``equivalent`` needs at least one row
    compared, nothing missing either way, no value outside ``tolerance``, and
    both panels ending on ``trading_day`` itself.
    """
    import pandas as pd

    tolerance = dict(PARITY_TOLERANCE if tolerance is None else tolerance)
    examples: list[str] = []
    producer_side = _side(producer, key=producer_key, sha256=producer_sha256)
    consumer_side = _side(consumer, key=consumer_key, sha256=consumer_sha256)
    for label, side in (("producer", producer_side), ("consumer", consumer_side)):
        if side["trading_day"] != trading_day.isoformat():
            examples.append(f"{label} panel ends on {side['trading_day']}, not {trading_day}")

    keys = ["trading_day", "ticker"]
    tickers = set(consumer["ticker"]) if len(consumer) else set()
    if len(consumer):
        lo, hi = min(consumer["trading_day"]), max(consumer["trading_day"])
        region = producer[
            producer["ticker"].isin(tickers) & (producer["trading_day"] >= lo) & (producer["trading_day"] <= hi)
        ]
    else:
        region = producer.iloc[0:0]
    merged = pd.merge(
        region[list(PANEL_COLUMNS)],
        consumer[list(PANEL_COLUMNS)],
        on=keys,
        how="outer",
        suffixes=("_producer", "_consumer"),
        indicator=True,
    )
    only_consumer = merged[merged["_merge"] == "right_only"]
    only_producer = merged[merged["_merge"] == "left_only"]
    both = merged[merged["_merge"] == "both"]
    for frame, what in ((only_consumer, "consumer row absent from the published panel"),
                        (only_producer, "published row the consumer did not read")):
        for _, row in frame.head(5).iterrows():
            if len(examples) < 20:
                examples.append(f"{row['trading_day']} {row['ticker']}: {what}")

    import numpy as np

    max_abs = dict.fromkeys(VALUE_COLUMNS, 0.0)
    max_rel = dict.fromkeys(VALUE_COLUMNS, 0.0)
    row_differs = np.zeros(len(both), dtype=bool)
    for column in VALUE_COLUMNS:
        a = both[f"{column}_producer"].to_numpy(dtype="float64")
        b = both[f"{column}_consumer"].to_numpy(dtype="float64")
        a_nan, b_nan = np.isnan(a), np.isnan(b)
        comparable = ~a_nan & ~b_nan
        abs_diff = np.where(comparable, np.abs(a - b), 0.0)
        with np.errstate(divide="ignore", invalid="ignore"):
            rel_diff = np.where(comparable & (b != 0), abs_diff / np.abs(b), np.where(abs_diff == 0, 0.0, np.inf))
        if column == "volume_raw":
            differs = abs_diff > tolerance["volume_abs"]
        else:
            differs = rel_diff > tolerance["price_rel"]
        # NaN on exactly one side is a difference; NaN on both is agreement.
        differs = differs | (a_nan != b_nan)
        if len(both):
            max_abs[column] = float(abs_diff.max())
            finite = rel_diff[np.isfinite(rel_diff)]
            max_rel[column] = float(finite.max()) if len(finite) else 0.0
        for index in np.flatnonzero(differs)[:5]:
            if len(examples) < 20:
                row = both.iloc[int(index)]
                examples.append(
                    f"{row['trading_day']} {row['ticker']} {column}: published {a[index]!r} vs consumer {b[index]!r}"
                )
        row_differs |= differs
    mismatched_rows = int(row_differs.sum())

    rows_compared = int(len(both))
    same_day = producer_side["trading_day"] == consumer_side["trading_day"] == trading_day.isoformat()
    equivalent = (
        rows_compared > 0
        and same_day
        and len(only_consumer) == 0
        and len(only_producer) == 0
        and mismatched_rows == 0
    )
    return {
        "schema_version": PARITY_SCHEMA_VERSION,
        "trading_day": trading_day.isoformat(),
        "verdict": "equivalent" if equivalent else "divergent",
        "tolerance": tolerance,
        "producer": producer_side,
        "consumer": consumer_side,
        "rows_compared": rows_compared,
        "tickers_compared": int(both["ticker"].nunique()) if rows_compared else 0,
        "missing_in_producer": int(len(only_consumer)),
        "missing_in_consumer": int(len(only_producer)),
        "value_mismatches": int(mismatched_rows),
        "max_abs_diff": max_abs,
        "max_rel_diff": max_rel,
        "examples": examples[:20],
        "generated_at": generated_at.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
    }
