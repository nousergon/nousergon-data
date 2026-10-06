"""Semantic admission at the publish boundary (alpha-engine-config-I12082).

``contracts/__init__.py`` validates a document's SHAPE and is advisory: it logs
warnings and never refuses. A document can be schema-valid and still wrong,
and schema-valid is not statistically correct. Examples: a fraction published
as a percent, an ``as_of`` from an older input, a version a consumer cannot
read, or symbols that were never in the run's universe. This module adds the
semantic half and one decision per write:

* **version** -- ``schema_version`` equals the contract's declared version;
* **schema** -- the JSON Schema in ``contracts/<name>.schema.json`` (shared with
  the advisory validator, so the two cannot drift);
* **units** -- declared value ranges for fields whose unit is implicit (a
  fraction must not read as a percent);
* **lineage** -- ``as_of`` is the run date the producer computed from, and not
  older than ``max_lineage_age_days``;
* **manifest** -- the output accounts only for members of the declared input
  population, and is not empty when that population is not.

``admit()`` returns every problem it finds, so the caller sees all of them.
``publish_admitted()`` is the boundary. In ``shadow`` mode, the default, it
logs a refusal and writes anyway, so adopting it changes no output until an
owner switches the mode. In ``enforce`` mode it raises ``AdmissionRefused``
before anything is written. The mode is read from
``NOUSERGON_ADMISSION_MODE`` (``shadow`` | ``enforce``); an unknown value is
``enforce``, so a typo fails closed rather than open.
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from contracts import _validate

logger = logging.getLogger(__name__)

MODE_ENV = "NOUSERGON_ADMISSION_MODE"


class AdmissionRefused(RuntimeError):
    """A document failed semantic admission in enforce mode; nothing was written."""

    def __init__(self, artifact: str, problems: list[str]):
        super().__init__(f"{artifact}: refused: " + "; ".join(problems[:10]))
        self.artifact = artifact
        self.problems = problems


@dataclass(frozen=True)
class Range:
    """Inclusive bounds for one field's implicit unit. ``None`` values pass:
    absence is the schema's business, not the unit's."""

    lo: float | None = None
    hi: float | None = None
    unit: str = ""

    def problem(self, name: str, value: Any) -> str | None:
        if value is None:
            return None
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
        ):
            return f"{name}={value!r} is not a finite number ({self.unit})"
        if (self.lo is not None and value < self.lo) or (
            self.hi is not None and value > self.hi
        ):
            return f"{name}={value} outside [{self.lo}, {self.hi}] ({self.unit})"
        return None


@dataclass(frozen=True)
class AdmissionSpec:
    artifact: str
    schema_name: str
    schema_version: int
    member_field: str
    member_units: dict[str, Range] = field(default_factory=dict)
    member_predicates: tuple[tuple[str, Callable[[dict], bool]], ...] = ()
    max_lineage_age_days: int = 5


def admit(
    document: dict,
    spec: AdmissionSpec,
    *,
    run_date: str,
    population: Iterable[str],
    today: date | None = None,
) -> list[str]:
    """Every reason ``document`` may not be published (empty = admitted)."""
    problems: list[str] = []
    version = document.get("schema_version")
    if version != spec.schema_version:
        problems.append(
            f"version: schema_version {version!r} != contract {spec.schema_version}"
        )
    problems += [f"schema: {w}" for w in _validate(document, spec.schema_name)]

    as_of = document.get("as_of")
    if as_of != run_date:
        problems.append(f"lineage: as_of {as_of!r} is not this run's date {run_date!r}")
    try:
        age = ((today or date.today()) - date.fromisoformat(str(as_of))).days
        if age > spec.max_lineage_age_days:
            problems.append(
                f"lineage: as_of {as_of} is {age} days old (max {spec.max_lineage_age_days})"
            )
    except ValueError:
        problems.append(f"lineage: as_of {as_of!r} is not an ISO date")

    members = document.get(spec.member_field)
    pop = set(population)
    if not isinstance(members, dict):
        problems.append(f"manifest: {spec.member_field} is not an object")
        members = {}
    stray = sorted(set(members) - pop)
    if stray:
        problems.append(
            f"manifest: {len(stray)} member(s) outside the declared population, e.g. {stray[:3]}"
        )
    if pop and not members:
        problems.append(
            f"manifest: empty {spec.member_field} over a population of {len(pop)}"
        )

    unit_problems: list[str] = []
    for key, row in members.items():
        if not isinstance(row, dict):
            continue
        for name, rng in spec.member_units.items():
            p = rng.problem(name, row.get(name))
            if p:
                unit_problems.append(f"{key}: {p}")
        for label, pred in spec.member_predicates:
            try:
                ok = pred(row)
            except (TypeError, ValueError, KeyError):
                ok = False
            if not ok:
                unit_problems.append(f"{key}: {label}")
    if unit_problems:
        problems.append(
            f"units: {len(unit_problems)} violation(s), e.g. "
            + "; ".join(unit_problems[:5])
        )
    return problems


def mode() -> str:
    value = os.environ.get(MODE_ENV, "shadow").strip().lower()
    return value if value in ("shadow", "enforce") else "enforce"


def publish_admitted(
    write: Callable[[], None],
    document: dict,
    spec: AdmissionSpec,
    *,
    run_date: str,
    population: Iterable[str],
    today: date | None = None,
) -> dict:
    """The publish boundary. Runs ``admit`` and then, unless refused in enforce
    mode, calls ``write``. Returns the decision so the caller can surface it."""
    problems = admit(
        document, spec, run_date=run_date, population=population, today=today
    )
    m = mode()
    decision = {
        "artifact": spec.artifact,
        "mode": m,
        "admitted": not problems,
        "problems": problems,
    }
    if problems and m == "enforce":
        logger.error(
            "[admission] %s REFUSED (enforce): %s", spec.artifact, problems[:5]
        )
        raise AdmissionRefused(spec.artifact, problems)
    if problems:
        logger.warning(
            "[admission] %s would be refused (shadow, written anyway): %s",
            spec.artifact,
            problems[:5],
        )
    write()
    return decision


def _within_52w(row: dict) -> bool:
    lo, hi = row.get("low_52w"), row.get("high_52w")
    if lo is None or hi is None:
        return True
    if lo > hi:
        return False
    # ma_50 and ma_200 average closes inside the same 252-session window
    return all(
        row.get(k) is None or lo - 1e-6 <= row[k] <= hi + 1e-6
        for k in ("ma_50", "ma_200")
    )


#: market_data/technicals/latest.json. Units are read from
#: ``collectors/metron_market_data._compute_technicals``: every pct_* and mom_*
#: is a FRACTION (``last / x - 1.0``), never a percent.
TECHNICALS = AdmissionSpec(
    artifact="market_data/technicals/latest.json",
    schema_name="technicals",
    schema_version=4,
    member_field="technicals",
    member_units={
        "rsi_14": Range(0, 100, "RSI points"),
        "pct_in_52w_range": Range(0, 1, "fraction of the 52-week range"),
        "pct_from_52wk_high": Range(-1, 0, "fraction below the 52-week high"),
        "pct_to_ma_50": Range(-1, None, "fraction vs the 50-day MA"),
        "pct_to_ma_200": Range(-1, None, "fraction vs the 200-day MA"),
        "mom_20d": Range(-1, None, "fractional 20-session return"),
        "mom_60d": Range(-1, None, "fractional 60-session return"),
        "high_52w": Range(0, None, "price"),
        "low_52w": Range(0, None, "price"),
    },
    member_predicates=(
        ("ma_50/ma_200 inside [low_52w, high_52w] and low <= high", _within_52w),
    ),
    max_lineage_age_days=5,
)
