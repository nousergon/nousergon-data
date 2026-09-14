"""Evidence readers — what each clause actually reads, and what it does not yet.

**The rule this module exists to enforce: never MET without a read.**

Most of the board's evidence does not exist yet. Phase 0 builds the board; the
run manifests, the generated `observability.d` rows, the reconciled registry and
the commissioning records land in phases 1 to 3. A reader for evidence that does
not exist has exactly two honest options, and "assume it is fine" is neither:

* **UNMET** when we looked and the artifact is not there. An absent artifact is
  a clause's ANSWER, and the missing key is named.
* **UNMEASURABLE** when we could not look at all — a denied read, or a reader
  that has not been built for a phase that has not started. Red, counted toward
  the transparency gap, and the key it WILL read is named so the row is a work
  item rather than a shrug.

A phase-0 stub therefore returns UNMEASURABLE naming its future key. It never
returns MET, and there is a test that says so (`test_no_clause_is_met_without_a
_read`). The two readers that ARE implemented here read real committed
artifacts — the declared schema files in this repository, and the ladder's own
age — and they are allowed to read MET because they actually measured something.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import dataclass, field

from nousergon_lib.gates import LADDER_KEY, GateStore, read_store_document

from data_gate.descriptors import REPO_ROOT, Unit

__all__ = [
    "BASE_REQUIREMENTS",
    "GateStore",
    "Reading",
    "read_base",
    "read_guard_commissioning",
    "read_ladder_freshness",
    "read_objective",
]


@dataclass(frozen=True)
class Reading:
    """One evidence read: met, or unmeasurable, with what it looked at."""

    met: bool
    detail: str
    evidence: tuple[str, ...] = ()
    unmeasurable: bool = False
    source: str | None = None
    as_of: str | None = None


def _pending(key: str, what: str, *, source: str) -> Reading:
    """The honest phase-0 stub: UNMEASURABLE, naming the key it will read."""
    return Reading(
        met=False,
        detail=(
            f"no reader is built for this column yet; it will read {key}. UNMEASURABLE "
            f"rather than UNMET because nothing was denied and nothing was absent — we "
            f"have not looked. {what}"
        ),
        evidence=(key,),
        unmeasurable=True,
        source=source,
    )


BASE_REQUIREMENTS: dict[str, str] = {
    "identity": (
        "{unit} writes under a workload identity scoped to the prefixes it declares — not "
        "through a whole-bucket wildcard, and with no bucket-wide Delete "
        "(alpha-engine-config-I10756)"
    ),
    "consumers": (
        "every consumer {unit} declares resolves to a live reader, or the unit declares "
        "`consumers: []` with a reason, which renders as a finding"
    ),
    "schema_contract": (
        "{unit} publishes a versioned schema with a producer test that validates a real "
        "fixture, and every consumer pins a copy"
    ),
    "artifact_registry": (
        "{unit}'s published keys each have an ARTIFACT_REGISTRY row that agrees with reality "
        "— not parked, not absent, not describing a producer that was never built"
    ),
    "observability_row": (
        "{unit} has an observability.d row of its OWN, generated from its descriptor — a "
        "stage or dispatcher umbrella row is the aggregate-hides-member defect, not a row"
    ),
    "run_record": (
        "every execution of {unit} writes a data_run_manifest.v1 record on BOTH paths, with "
        "no third ok-but-degraded state"
    ),
    "detector": (
        "{unit} has a detector that has been made to FIRE by inducing the real condition, "
        "named the right subject, and stood down when it cleared (observability-policy §9)"
    ),
    "console_entity": (
        "{unit} is reachable on the console as a Component — by name, by structure and by "
        "relation — and never renders green when it has nothing to say"
    ),
    "survives_phase4": (
        "{unit} has a trigger that survives v2 phase 4: a standalone-stack workload with a "
        "successful manifest, or a recorded retirement decision"
    ),
}

#: Where each base column's evidence WILL live. Named on every pending reading,
#: so a red row is a work item with an address rather than an unexplained red.
_BASE_EVIDENCE_KEY: dict[str, str] = {
    "identity": "nous-ergon-ops: codified role policy + IAM last-accessed (phase 2)",
    "consumers": "consumer-repo contract tests declared in the descriptor (phase 1)",
    "schema_contract": "nousergon-data: contracts/<key>.schema.json + producer test (phase 1)",
    "artifact_registry": "alpha-engine-config: private-docs/ARTIFACT_REGISTRY.yaml (phase 1)",
    "observability_row": "nous-ergon-ops: governance/observability.d/<component>.yaml (phase 1)",
    "run_record": "{prefix}/{trading_day}/*.json (phase 1)",
    "detector": "data_collection/commissioning/<detector>/latest.json (phase 3)",
    "console_entity": "console:/component/<component_id> (phase 3)",
    "survives_phase4": "{prefix}/{trading_day}/*.json under the standalone stack (phase 1)",
}


def _declared_schema_files(unit: Unit) -> list[str]:
    contract = unit.raw.get("contract") or {}
    out: list[str] = []
    for field_name in ("schema", "producer_test"):
        value = contract.get(field_name)
        if not value:
            continue
        token = str(value).split(" ")[0].split("::")[0]
        if ":" in token and not token.endswith(".json"):
            # A cross-repo reference such as `metron:tests/...`. This gate has no
            # visibility into another repository's tree, so it is not evidence
            # here; the consumer-pin clause is where that is graded, in phase 1.
            continue
        if token.endswith((".json", ".py")):
            out.append(token)
    return out


def _read_schema_contract(unit: Unit) -> Reading:
    """A REAL read: does the declared schema and producer test exist in this tree?

    The one base column whose evidence is a committed file in this repository,
    so it is measurable on the day the board is built. It is expected to
    reproduce the audit's ten PRESENT cells — and plan §6 phase-0 exit (c) makes
    any disagreement a finding rather than something to reconcile quietly.
    """
    files = _declared_schema_files(unit)
    if not files:
        return Reading(
            met=False,
            detail=(
                "the descriptor declares no schema and no producer test for this unit's "
                "published keys. This is the audit's 35-unit contract gap; phase 1 builds one "
                "per key with a surviving consumer (P-07)."
            ),
            evidence=(unit.path.name,),
            source="registry.d/units",
            as_of=str(unit.raw["audit"]["baseline_date"]),
        )
    missing = [f for f in files if not (REPO_ROOT / f).exists()]
    if missing:
        return Reading(
            met=False,
            detail=(
                f"the descriptor declares {files} but {missing} is/are not in the tree. A "
                "declared contract that does not exist is worse than a missing one: it reads "
                "as coverage."
            ),
            evidence=tuple(files),
            source="nousergon-data tree",
        )
    if not (unit.raw.get("contract") or {}).get("consumer_pins"):
        return Reading(
            met=False,
            detail=(
                f"producer side present ({files}) and NO consumer pin is declared. A schema "
                "the producer validates against and no consumer pins is half a contract: it "
                "cannot fail a consumer that drifts (plan §4.3)."
            ),
            evidence=tuple(files),
            source="nousergon-data tree",
        )
    return Reading(
        met=True,
        detail=f"schema and producer test present, with a declared consumer pin: {files}",
        evidence=tuple(files),
        source="nousergon-data tree",
    )


def read_base(store: GateStore, unit: Unit, column: str, *, trading_day: dt.date) -> Reading:
    """The evidence behind one base clause."""
    if column == "schema_contract":
        return _read_schema_contract(unit)
    key = _BASE_EVIDENCE_KEY[column].format(prefix=unit.run_manifest_prefix, trading_day=trading_day)
    return _pending(
        key,
        f"The audit's 2026-09-14 reading of this cell is the baseline the board reconciles "
        f"against, never the evidence for it ({unit.unit_id} / {column}).",
        source="data_gate.evidence (phase-0 stub)",
    )


def read_guard_commissioning(
    store: GateStore, unit: Unit, guard: str, *, trading_day: dt.date
) -> Reading:
    """Whether this guard class has an induced-fault record proving it fires.

    `observability-policy` §9.1: a guard that has never fired is not in service.
    The record lands in phase 2 (P-19), so this reads UNMEASURABLE until then —
    and it names the exact key, so "not commissioned" is an address rather than
    an opinion.
    """
    key = f"faults/{unit.unit_id}/{guard}/latest.json"
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no induced-fault record at {key}. A guard that has never fired is not in "
                "service (observability-policy §9.1); commissioning is P-19, phase 2."
            ),
            evidence=(key,),
            source="data_collection store",
        )
    document = read.document or {}
    outcome = str(document.get("outcome") or "")
    if outcome != "induced":
        return Reading(
            met=False,
            detail=(
                f"{key} exists with outcome {outcome!r}. Only an `induced` record is evidence "
                "the guard fired; `absorbed` names a run the guard never had to catch."
            ),
            evidence=(key,),
            source="data_collection store",
            as_of=str(document.get("as_of") or ""),
        )
    return Reading(
        met=True,
        detail=f"commissioned: {key} records an induced fault the guard caught and stood down from",
        evidence=(key,),
        source="data_collection store",
        as_of=str(document.get("as_of") or ""),
    )


def read_objective(store: GateStore, key: str) -> Reading:
    """One objective/SLO metric document, or the reason there is none yet."""
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no metric document at {key}. Nothing publishes this number yet — an "
                "objective with no emitter is unobserved, not met (plan §2)."
            ),
            evidence=(key,),
            source="data_collection store",
        )
    document = read.document or {}
    status = str(document.get("status") or "")
    if status not in {"ok", "breach"}:
        return Reading(
            met=False,
            detail=(
                f"{key} carries status {status!r}, which is outside the closed set "
                "{{ok, breach}}. A status nobody defined a rendering for is a finding."
            ),
            evidence=(key,),
            source="data_collection store",
            as_of=str(document.get("as_of") or ""),
        )
    value = document.get("value")
    baseline = document.get("baseline")
    return Reading(
        met=status == "ok",
        detail=f"{key}: status={status}, value={value}, baseline={baseline}",
        evidence=(key,),
        source="data_collection store",
        as_of=str(document.get("as_of") or ""),
    )


def read_ladder_freshness(
    store: GateStore, *, trading_day: dt.date, max_age_hours: int, now: dt.datetime | None = None
) -> Reading:
    """The ladder's own age — the observer observed.

    A REAL read on day one. The clause it backs is what stops the entire board
    from freezing in its last state: without it, a gate job that silently stops
    running leaves 400-odd rows rendering yesterday's answer forever.
    """
    key = LADDER_KEY
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {key}: {read.problem}",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no ladder at {key}. The board has never been published, so every phase row "
                "is UNREPORTED rather than green — which is what this reading is for."
            ),
            evidence=(key,),
            source="data_collection store",
        )
    stamp = str((read.document or {}).get("generated_utc") or "")
    try:
        generated = dt.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return Reading(
            met=False,
            detail=(
                f"{key} carries generated_utc {stamp!r}, which is not a ladder timestamp. An "
                "unparseable stamp is not a fresh one."
            ),
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    moment = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    age_hours = (moment - generated).total_seconds() / 3600.0
    return Reading(
        met=age_hours <= max_age_hours,
        detail=(
            f"{key} generated {stamp} — {age_hours:.1f}h old against a {max_age_hours}h "
            "ceiling" + ("" if age_hours <= max_age_hours else "; page condition 3")
        ),
        evidence=(key,),
        source="data_collection store",
        as_of=stamp,
    )
