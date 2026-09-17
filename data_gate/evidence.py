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
_read`). The readers that ARE implemented here read real artifacts — the
declared schema files in this repository, the ladder's own age, the standalone
stack's roles, and (since `alpha-engine-config-I10810`) the run manifests
themselves — and they are allowed to read MET because they actually measured
something.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import dataclass, field

from nousergon_lib.gates import LADDER_KEY, GateStore, read_store_document
from nousergon_lib.run_manifest import SCHEMA_VERSION as _MANIFEST_SCHEMA
from nousergon_lib.trading_calendar import (  # pyright: ignore[reportAttributeAccessIssue]
    previous_trading_day,
    subtract_trading_days,
)

import run_units
from data_gate.cadence import Cadence, gate_moment, latest_due_fire, unit_cadence
from data_gate.descriptors import REPO_ROOT, Unit

__all__ = [
    "BASE_REQUIREMENTS",
    "EMPTY_FRESH_VERDICT",
    "MANIFEST_STATUSES",
    "PARITY_FRESHNESS_TRADING_DAYS",
    "PARITY_KEY_PREFIX",
    "PARITY_KEY_TEMPLATE",
    "GateStore",
    "Reading",
    "empty_fresh_runs",
    "empty_success_runs",
    "parity_store_key",
    "read_base",
    "read_completeness_metric",
    "read_guard_commissioning",
    "read_ladder_freshness",
    "read_objective",
    "read_parity",
    "read_roles_bootstrapped",
    "read_run_record",
    "read_stack_check_live",
    "manifests_since",
    "STACK_CHECK_LIVE_SCHEMA",
    "STACK_CHECK_LIVE_MAX_AGE",
]

#: Where `infrastructure/data_collection_stack.py check-live` publishes its
#: verdict, RELATIVE TO THE STORE ROOT (`s3://alpha-engine-research/
#: data_collection`). It used to be spelled with the `data_collection/` prefix,
#: which a store already rooted there resolves to
#: `data_collection/data_collection/deploy/...` — a key nothing would ever
#: write. The producer imports nothing from here (it is a standalone script),
#: so `tests/test_data_collection_stack.py` pins the two spellings together.
STACK_CHECK_LIVE_KEY = "deploy/check-live/latest.json"
STACK_CHECK_LIVE_SCHEMA = "data_collection_check_live.v1"
#: check-live runs after every deploy and weekly (Sunday 22:30 UTC). A verdict
#: older than one cycle plus a day means the emitter stopped.
STACK_CHECK_LIVE_MAX_AGE = dt.timedelta(days=8)

#: The `MetricRecord` (`krepis.metrics`) status vocabulary that represents a
#: real reading (the guard looked and has an answer, good or bad) versus the
#: N/A-* states that mean the guard could not evaluate this cycle at all.
_METRIC_REAL_STATUSES: frozenset[str] = frozenset({"GREEN", "WATCH", "RED"})
_METRIC_NA_STATUSES: frozenset[str] = frozenset(
    {"N/A-NOT-IMPL", "N/A-NOT-RUN", "N/A-LOW-N", "N/A-MISSING-INPUT"}
)

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
        "every consumer {unit} declares resolves to a live reader and at least one lives in a "
        "repo that survives v2 phase 4; a unit with none is UNCONNECTED — a finding until a "
        "recorded keep (`consumers_decision`) or retire decision, then graded by no gate"
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
    # Read for real since `alpha-engine-config-I10810` — kept here because the
    # UNMET/UNMEASURABLE details still name the key they looked at.
    "run_record": "{prefix}/{trading_day}/*.json",
    "detector": "data_collection/commissioning/<detector>/latest.json (phase 3)",
    "console_entity": "console:/component/<component_id> (phase 3)",
    "survives_phase4": "{prefix}/{trading_day}/*.json under the standalone stack (phase 1)",
}


def _declared_schema_files(unit: Unit) -> list[str]:
    contract = unit.raw.get("contract") or {}
    out: list[str] = []
    for field_name in ("schema", "producer_test"):
        # A multi-artifact unit (D20-D22, D37) declares a LIST. Stringifying
        # the list matched no suffix and returned zero files, so a fully built
        # contract read UNMET — every value is one declaration, scalar or not.
        for value in _as_list(contract.get(field_name)):
            token = str(value).split(" ")[0].split("::")[0]
            if ":" in token and not token.endswith(".json"):
                # A cross-repo reference such as `metron:tests/...`. This gate has no
                # visibility into another repository's tree, so it is not evidence
                # here; the consumer-pin clause is where that is graded, in phase 1.
                continue
            if token.endswith((".json", ".py")):
                out.append(token)
    return out


def _as_list(value: object) -> list:
    """A descriptor field that may be declared as one value or several."""
    if value is None or value == "":
        return []
    return list(value) if isinstance(value, (list, tuple)) else [value]


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


#: The store is opened at `s3://alpha-engine-research/data_collection`, but a
#: descriptor's `run_manifest_prefix` is the FULL key prefix
#: (`data_collection/runs/D19`). Stripping the store's own prefix is what makes
#: the two addressable from one declaration — and it is done here, once, rather
#: than by rewriting 46 descriptors into a form only this reader can use.
_STORE_PREFIX = "data_collection/"


def _store_relative(prefix: str) -> str:
    return prefix[len(_STORE_PREFIX):] if prefix.startswith(_STORE_PREFIX) else prefix


#: `data_run_manifest.v1`'s closed status set. A manifest carrying anything else
#: is the third ok-but-degraded state the run-record requirement forbids, and it
#: is a finding rather than something to map onto the nearest neighbour.
MANIFEST_STATUSES = frozenset({"ok", "failed", "not_applicable"})

#: The guard verdict that IS an empty-but-fresh write. Objective 6 is counted
#: from this, never from `rows_out == 0`.
EMPTY_FRESH_VERDICT = "empty_fresh"


def empty_fresh_runs(manifests: list[dict]) -> list[str]:
    """The run ids that published a fresh, EMPTY artifact — plan §2 objective 6.

    **Counted from `guards[].verdict`, never from `rows_out == 0`.** The two are
    not the same question and the naive form gets the answer backwards on the
    most common case: a phase that correctly had nothing to do this cycle
    records `rows_out: 0` with a `not_applicable` guard verdict, and an
    auto-skipped phase records the same. Counting zeros would file every one of
    those as an empty-but-fresh write — the objective would breach on units that
    are working exactly as designed, and the real empty writes would be
    indistinguishable inside the noise.

    The guard is the thing that actually looked at the artifact
    (`validators/expectations.py::check_empty_fresh` — it HEADs the object,
    separates "zero bytes" from "zero rows" from "could not count", and returns
    `unmeasurable` rather than a pass when it could not look). Reading its
    verdict is reading a measurement; counting zeros is re-deriving one badly
    from a field that was never the evidence.
    """
    return [
        str(m.get("run_id") or "?")
        for m in manifests
        if any(str(g.get("verdict")) == EMPTY_FRESH_VERDICT for g in (m.get("guards") or []))
    ]


def empty_success_runs(manifests: list[dict]) -> list[str]:
    """The run ids that claimed `ok` while recording no output at all.

    `alpha-engine-config-I11011`. Derived from the RECORD — status, `outputs`
    and `rows_out` — through the one predicate the producer enforces against its
    own run context before writing (:func:`run_units.is_empty_success`). One
    predicate, both sides, deliberately: a reader that instead trusted a marker
    the producer sets would go blind the moment the producer stopped setting
    it, which is the exact failure this issue is about. Grading the property
    keeps the reader honest about producers that predate the rule — the ten
    manifests of the 2026-09-15 shadow run are named by this today, without
    being rewritten.

    Distinct from :func:`empty_fresh_runs`, which counts a unit that published
    a fresh but EMPTY artifact. This counts a unit that published no artifact
    at all and called it a success.
    """
    return [str(m.get("run_id") or "?") for m in manifests if run_units.is_empty_success(m)]


def _read_arctic_probe(store: GateStore, unit: Unit, trading_day: dt.date) -> Reading | None:
    """The ArcticDB probe backing a unit that declares `arcticdb_evidence`.

    `alpha-engine-config-I10772` (P-05). The gate NEVER opens ArcticDB — the
    bucket carries an explicit Deny that blocks even `ne-admin` from outside the
    region (`alpha-engine-config-I9771`), so a reader that tried would be
    UNMEASURABLE on every run for a reason that has nothing to do with the unit.
    Instead the EOD and morning machines write
    `data_collection/probes/arctic/{trading_day}.json` as their FINAL workload,
    and this reads that.

    Returns ``None`` for a unit that declares no `arcticdb_evidence` (the reader
    simply does not apply), and a **red** Reading otherwise whenever the probe
    is absent, unreadable, or withholding the library this unit writes. Never
    MET on a withheld probe: a probe that stopped writing must make these
    clauses UNMEASURABLE rather than leave them stale-green, which is the whole
    reason the probe exists rather than a timestamp.
    """
    declared = unit.raw.get("arcticdb_evidence") or {}
    via = declared.get("via")
    if not via:
        return None
    key = _store_relative(str(via).format(trading_day=trading_day.isoformat()))
    read = read_store_document(store, key)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=(
                f"{unit.unit_id} writes ArcticDB and its run evidence is the probe at {key}, "
                f"which could not be read: {read.problem}"
            ),
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"{unit.unit_id} writes ArcticDB and its run evidence is the probe at {key}, "
                "which is ABSENT for this trading day. The gate never opens ArcticDB "
                "(alpha-engine-config-I9771); a probe that did not write leaves this "
                "UNMEASURABLE rather than stale-green."
            ),
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    document = read.document or {}
    libraries = document.get("libraries") or {}
    if not isinstance(libraries, dict) or not libraries:
        return Reading(
            met=False,
            detail=f"the probe at {key} declares no `libraries` block, so it is evidence of nothing",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    withheld = sorted(
        name
        for name, row in libraries.items()
        if not isinstance(row, dict) or not row.get("read_ok") or row.get("row_count") is None
    )
    if withheld:
        return Reading(
            met=False,
            detail=(
                f"the probe at {key} withheld a reading for library/libraries {withheld} "
                "(read_ok false, or no row_count). A withheld probe is UNMEASURABLE, never MET."
            ),
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    summary = ", ".join(
        f"{name}: {row.get('row_count')} rows @ {row.get('last_index_date')}"
        for name, row in sorted(libraries.items())
    )
    return Reading(
        met=True,
        detail=f"ArcticDB evidence from the in-region probe {key} — {summary}",
        evidence=(key,),
        source="data_collection store",
        as_of=str(document.get("as_of") or trading_day.isoformat()),
    )


def _cadence_note(cadence: Cadence) -> str:
    if cadence.kind != "undeclared":
        return ""
    return (
        f" Graded against the gate's own trading day because the descriptor {cadence.source} "
        "(alpha-engine-config-I10871): a unit that runs less than daily reads red on the days "
        "it is not due until it declares when it runs."
    )


def _parse_utc(stamp: object) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _day_folder(key: str) -> dt.date | None:
    parts = key.split("/")
    try:
        return dt.date.fromisoformat(parts[-2])
    except (ValueError, IndexError):
        return None


@dataclass
class Cycle:
    """The manifests one reader grades, and how they were selected."""

    where: str
    expected: str
    keys: list[str] = field(default_factory=list)
    manifests: list[dict] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)


def _read_keys(store: GateStore, keys: list[str]) -> tuple[list[tuple[str, dict]], list[str]]:
    docs: list[tuple[str, dict]] = []
    problems: list[str] = []
    for key in keys:
        read = read_store_document(store, key)
        if read.problem is not None:
            problems.append(f"{key}: {read.problem}")
        elif read.absent:
            problems.append(f"{key}: vanished between listing and read")
        else:
            docs.append((key, read.document or {}))
    return docs, problems


#: How many calendar days BEFORE a fire a run's manifest folder may be named.
#: A manifest is filed under the trading day it collected, not the day it ran:
#: the 07:30 morning run records T-1, and the Saturday weekly records Friday
#: (or Thursday across a Good Friday). Four days covers the longest such gap.
_FOLDER_LAG_DAYS = 4


def manifests_since(
    store: GateStore, unit: Unit, *, since: dt.datetime, as_of: dt.datetime
) -> tuple[list[tuple[str, dict]], list[str], str]:
    """Every manifest of ``unit`` whose run STARTED in ``[since, as_of]``.

    Lists only the day folders that can hold such a run (see
    `_FOLDER_LAG_DAYS`), never the unit's whole history. Raises on a listing
    failure — the caller classifies that as UNMEASURABLE. A manifest whose
    ``started`` does not parse is kept when its folder is on or after the fire
    date, so an unparseable stamp can never hide a record.
    """
    base = f"{_store_relative(unit.run_manifest_prefix)}/"
    first = since.date() - dt.timedelta(days=_FOLDER_LAG_DAYS)
    last = as_of.date()
    keys: list[str] = []
    day = first
    while day <= last:
        keys.extend(k for k in store.list_keys(f"{base}{day.isoformat()}/") if k.endswith(".json"))
        day += dt.timedelta(days=1)
    docs, problems = _read_keys(store, sorted(keys))
    selected: list[tuple[str, dict]] = []
    for key, doc in docs:
        started = _parse_utc(doc.get("started"))
        folder = _day_folder(key)
        if started is not None:
            if since <= started <= as_of:
                selected.append((key, doc))
        elif folder is not None and folder >= since.date():
            selected.append((key, doc))
    return selected, problems, f"{base}{{{first.isoformat()}..{last.isoformat()}}}/"


def _cycle(
    store: GateStore, unit: Unit, cadence: Cadence, *, trading_day: dt.date, now: dt.datetime | None
) -> Cycle:
    """The manifests `read_run_record` grades, selected by the unit's cadence."""
    base = f"{_store_relative(unit.run_manifest_prefix)}/"
    if cadence.kind == "scheduled":
        as_of = gate_moment(trading_day, now)
        fire = latest_due_fire(cadence, as_of=as_of)
        docs, problems, where = manifests_since(store, unit, since=fire, as_of=as_of)
        return Cycle(
            where=where,
            expected=f"for the run due at {fire.strftime('%Y-%m-%dT%H:%MZ')} ({cadence.source})",
            keys=[k for k, _ in docs],
            manifests=[d for _, d in docs],
            problems=problems,
        )
    if cadence.kind == "on_demand":
        # The most recent invocation, whenever it was. On-demand units are low
        # volume, so listing the unit's whole prefix is bounded.
        days = sorted(
            {d for k in store.list_keys(base) if k.endswith(".json") and (d := _day_folder(k)) and d <= trading_day}
        )
        if not days:
            return Cycle(where=base, expected="for any invocation")
        latest = f"{base}{days[-1].isoformat()}/"
        keys = sorted(k for k in store.list_keys(latest) if k.endswith(".json"))
    else:
        latest = f"{base}{trading_day.isoformat()}/"
        keys = sorted(k for k in store.list_keys(latest) if k.endswith(".json"))
    docs, problems = _read_keys(store, keys)
    return Cycle(
        where=latest,
        expected=(
            "for the most recent invocation" if cadence.kind == "on_demand" else "for this trading day"
        ),
        keys=keys,
        manifests=[d for _, d in docs],
        problems=problems,
    )


def read_run_record(
    store: GateStore, unit: Unit, *, trading_day: dt.date, now: dt.datetime | None = None
) -> Reading:
    """Every execution of this unit on this trading day, as it recorded itself.

    `alpha-engine-config-I10810` deliverable 3 — the reader that replaces the
    phase-0 UNMEASURABLE stub. It lists
    ``data_collection/runs/{unit_id}/{trading_day}/*.json`` and grades what it
    finds against the requirement: a record on BOTH paths, and **no third
    ok-but-degraded state**.

    The distinctions this reader keeps, each of which the obvious shortcut
    loses:

    * **Listing failure is UNMEASURABLE, absence is UNMET.** We could not look
      versus we looked and there was nothing — opposite findings with opposite
      owners.
    * **A `failed` manifest still satisfies this clause.** The requirement is
      that the execution left a record, not that it succeeded; grading a failure
      record as UNMET would reward a unit for writing nothing on its bad days,
      which is precisely the behaviour the objective exists to end.
    * **A status outside the closed set is UNMET**, named. That IS the third
      ok-but-degraded state.
    * **Empty-but-fresh is counted from the guard verdict**, never from
      `rows_out == 0` — see :func:`empty_fresh_runs`.
    * **A run that claimed `ok` while publishing NOTHING is not a recorded
      run** (`alpha-engine-config-I11011`). `rows_out: 0` with an empty
      `outputs` array is a unit that completed having written nothing and filed
      it as a success — indistinguishable, on every surface that reads this
      record, from a unit that published its deliverable. It is UNMET unless
      the descriptor DECLARES zero output legitimate for that unit; it is never
      the default reading of an empty result. Distinct from a `failed` or
      `not_applicable` record, both of which still satisfy the clause: they
      already say what happened.
    * **An ArcticDB unit's evidence is the probe**, and a withheld probe is
      UNMEASURABLE rather than MET — see :func:`_read_arctic_probe`.
    """
    cadence = unit_cadence(unit.raw)
    unit_prefix = f"{_store_relative(unit.run_manifest_prefix)}/"
    try:
        cycle = _cycle(store, unit, cadence, trading_day=trading_day, now=now)
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        # A deliberate catch, not a swallow: the failure mode is "this unit's
        # manifest prefix could not be listed", the gate reading carrying every
        # other clause survives, and the recording surface is this UNMEASURABLE row.
        return Reading(
            met=False,
            detail=f"could not list {unit_prefix}: {type(exc).__name__}: {exc}",
            evidence=(f"{unit_prefix}*.json",),
            unmeasurable=True,
            source="data_collection store",
        )
    prefix = cycle.where

    if cycle.problems:
        # Checked BEFORE absence: an unreadable manifest can carry no `started`
        # to select it by, and "nothing there" must never stand in for "could
        # not read what is there".
        return Reading(
            met=False,
            detail=f"{len(cycle.problems)} manifest(s) under {prefix} unreadable: {cycle.problems[:4]}",
            evidence=tuple(cycle.keys[:8]) or (f"{prefix}*.json",),
            unmeasurable=True,
            source="data_collection store",
        )

    if not cycle.keys:
        if cadence.kind == "on_demand":
            # Plan §2 row 7 asks that every EXECUTION leave a record. A unit
            # that runs only when invoked and has never been invoked has no
            # execution to record: a declared not-applicable (the guard-clause
            # N/A shape), citing the listing that proved it.
            return Reading(
                met=True,
                detail=(
                    f"not applicable: {unit.unit_id} runs on demand ({cadence.source}) and no "
                    f"invocation has been recorded under {unit_prefix} — nothing executed, so "
                    "nothing was left unrecorded"
                ),
                evidence=(f"{unit_prefix}*.json",),
                source="data_collection store",
            )
        return Reading(
            met=False,
            detail=(
                f"no run manifest {cycle.expected}, under {prefix}. {unit.unit_id} either did "
                "not execute or executed without recording itself — and those are "
                "indistinguishable from here, which is exactly the state the run-record "
                "objective exists to end (plan §2 row 7)."
                + _cadence_note(cadence)
            ),
            evidence=(f"{prefix}*.json",),
            source="data_collection store",
        )

    keys = cycle.keys
    manifests = cycle.manifests
    problems = cycle.problems

    if problems:
        return Reading(
            met=False,
            detail=f"{len(problems)} of {len(keys)} manifest(s) under {prefix} unreadable: {problems[:4]}",
            evidence=tuple(keys[:8]),
            unmeasurable=True,
            source="data_collection store",
        )

    wrong_schema = sorted(
        {str(m.get("schema_version")) for m in manifests if m.get("schema_version") != _MANIFEST_SCHEMA}
    )
    if wrong_schema:
        return Reading(
            met=False,
            detail=(
                f"{prefix} holds record(s) whose schema_version is {wrong_schema}, not "
                f"{_MANIFEST_SCHEMA!r}. A reader that accepted them would be grading a "
                "contract nobody declared."
            ),
            evidence=tuple(keys[:8]),
            source="data_collection store",
        )

    statuses = [str(m.get("status")) for m in manifests]
    third_state = sorted({s for s in statuses if s not in MANIFEST_STATUSES})
    if third_state:
        return Reading(
            met=False,
            detail=(
                f"{prefix} holds run(s) with status {third_state}, outside the closed set "
                f"{sorted(MANIFEST_STATUSES)}. This is the third ok-but-degraded state the "
                "requirement forbids: a run that produced a partial or defective artifact "
                "is `failed`, and a cycle it correctly sat out is `not_applicable` with a "
                "closed-list reason."
            ),
            evidence=tuple(keys[:8]),
            source="data_collection store",
        )

    # `alpha-engine-config-I11011`. Checked BEFORE the run is counted: a
    # manifest reading `ok` with `rows_out: 0` and an empty `outputs` array is
    # a run that produced nothing, recorded as a success, and counting it here
    # is what let ten units read as recorded runs on the 2026-09-15 shadow run
    # while the parity comparator was the only thing that noticed they had
    # written no keys. The unit may DECLARE that publishing nothing is
    # legitimate for it (`empty_is_valid` in its descriptor, naming which
    # not-applicable reason the empty run is); that declaration is what makes
    # this readable, and it is never the default reading of an empty result.
    empty_success = empty_success_runs(manifests)
    declaration = run_units.empty_declaration(unit.raw)
    if empty_success and declaration is None:
        return Reading(
            met=False,
            detail=(
                f"{len(empty_success)} of {len(manifests)} run(s) under {prefix} recorded "
                f"`status: ok` with rows_out 0 and an empty `outputs` array — {empty_success[:8]}. "
                f"{unit.unit_id} completed having published NOTHING and filed it as a success, "
                "which is indistinguishable from a real success on every surface that reads "
                "this record. Not counted as a recorded run: a unit for which zero output is "
                "legitimate declares that in its descriptor "
                f"(`{run_units.EMPTY_IS_VALID_FIELD}`, naming the not-applicable reason the "
                "empty run is), and the fleet default is to RAISE."
            ),
            evidence=tuple(keys[:8]),
            source="data_collection store",
        )

    empty_fresh = empty_fresh_runs(manifests)
    counts = {s: statuses.count(s) for s in sorted(set(statuses))}
    summary = (
        f"{len(manifests)} run(s) recorded under {prefix}: {counts}; "
        f"rows_out total {sum(int(m.get('rows_out') or 0) for m in manifests)}; "
        f"empty-but-fresh runs (guards[].verdict == 'empty_fresh') {len(empty_fresh)}"
        + (
            f"; {len(empty_success)} run(s) published nothing, DECLARED legitimate "
            f"({run_units.EMPTY_IS_VALID_FIELD}.reason={declaration.reason}): {declaration.note}"
            if empty_success and declaration is not None
            else ""
        )
    )

    # The probe is filed under the trading day the run COLLECTED, which for a
    # weekly or morning unit is not the gate's own day.
    recorded = sorted(str(m.get("trading_day") or "") for m in manifests if m.get("trading_day"))
    try:
        probe_day = dt.date.fromisoformat(recorded[-1]) if recorded else trading_day
    except ValueError:
        probe_day = trading_day
    probe = _read_arctic_probe(store, unit, probe_day)
    if probe is not None:
        if not probe.met:
            # The manifests exist; the ArcticDB half of the evidence does not.
            # Red, and carrying BOTH halves so the row is a work item with an
            # address rather than a bare unmeasurable.
            return Reading(
                met=False,
                detail=f"{summary}. {probe.detail}",
                evidence=tuple(keys[:8]) + probe.evidence,
                unmeasurable=probe.unmeasurable,
                source=probe.source,
            )
        return Reading(
            met=True,
            detail=f"{summary}. {probe.detail}",
            evidence=tuple(keys[:8]) + probe.evidence,
            source="data_collection store",
            as_of=probe.as_of,
        )

    return Reading(
        met=True,
        detail=summary,
        evidence=tuple(keys[:8]),
        source="data_collection store",
        as_of=str(manifests[-1].get("finished") or ""),
    )


def read_base(store: GateStore, unit: Unit, column: str, *, trading_day: dt.date) -> Reading:
    """The evidence behind one base clause."""
    if column == "schema_contract":
        return _read_schema_contract(unit)
    if column == "run_record":
        return read_run_record(store, unit, trading_day=trading_day)
    if column == "survives_phase4":
        # `alpha-engine-config-I10870`. Imported here: `standalone` imports
        # `Reading` from this module.
        from data_gate import standalone

        return standalone.read_survives_phase4(store, unit, trading_day=trading_day)
    if column in {"observability_row", "artifact_registry", "consumers", "identity"}:
        # `alpha-engine-config-I10823`. Imported here, not at module top:
        # `unit_readers` imports `Reading` from this module.
        from data_gate import unit_readers

        if column == "observability_row":
            return unit_readers.read_observability_row(unit)
        if column == "artifact_registry":
            return unit_readers.read_artifact_registry(store, unit)
        if column == "consumers":
            return unit_readers.read_consumers(store, unit)
        return unit_readers.read_identity(store, unit)
    key =_BASE_EVIDENCE_KEY[column].format(prefix=unit.run_manifest_prefix, trading_day=trading_day)
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


def read_completeness_metric(store: GateStore, unit: Unit, *, trading_day: dt.date) -> Reading:
    """One unit's cardinality/completeness `MetricRecord` for a single trading day.

    `validators/expectations.py::publish_completeness_metric` writes this key
    (`data_collection_plan_260914.md` §2 row 2, plan item P-13;
    `alpha-engine-config-I10780`, extending `alpha-engine-config-I5935`). Ships
    in phase 1 OBSERVE (`sf-pipeline-policy` §7a): this clause reads the real
    MEASURED coverage regardless of the guard's own staging — observe mode
    governs only whether a bad reading halts the collector run, never whether
    the board renders it. Distinct from `data.slo.completeness.<family>`
    (phase 3, a rolling 20-cycle SLO over ALL units in a freshness family):
    this clause is the single-day, single-unit reading that PROVES the guard
    ran and published something today.
    """
    key = f"metrics/eod_completeness/{trading_day.isoformat()}.json"
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
                f"no completeness metric at {key}. The cardinality guard "
                "(validators/expectations.py::check_cardinality /"
                "publish_completeness_metric) has not published a reading for this "
                "trading day — nothing emits it yet, which is the P-13 gap this clause "
                "exists to surface."
            ),
            evidence=(key,),
            source="data_collection store",
        )
    document = read.document or {}
    status = str(document.get("status") or "")
    as_of = str(document.get("last_updated_utc") or "")
    if status in _METRIC_NA_STATUSES:
        return Reading(
            met=False,
            detail=f"{key}: status={status} ({document.get('status_reason')})",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
            as_of=as_of,
        )
    if status not in _METRIC_REAL_STATUSES:
        return Reading(
            met=False,
            detail=(
                f"{key} carries status {status!r}, outside the closed MetricRecord status "
                f"vocabulary {sorted(_METRIC_REAL_STATUSES | _METRIC_NA_STATUSES)}"
            ),
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
            as_of=as_of,
        )
    return Reading(
        met=status == "GREEN",
        detail=(
            f"{key}: status={status}, value={document.get('value')}, "
            f"floor={document.get('target')} — {document.get('status_reason')}"
        ),
        evidence=(key,),
        source="data_collection store",
        as_of=as_of,
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


def read_stack_check_live(store: GateStore, *, now: dt.datetime | None = None) -> Reading:
    """The `nousergon-data-collection` stack's live-vs-checkout comparison.

    `infrastructure/data_collection_stack.py check-live` publishes
    ``{schema_version, as_of, code_sha, stack, measured, in_sync, drift, error}``
    to `STACK_CHECK_LIVE_KEY` on every run — after every deploy and weekly —
    including the runs that fail (`alpha-engine-config-I10870`).

    * Absent: UNMET — the store answers, there is no emitter (a producer
      finding, `read_objective`'s rule).
    * ``measured: false``: UNMEASURABLE — check-live could not look at the stack.
    * Older than `STACK_CHECK_LIVE_MAX_AGE`: UNMET — the emitter stopped, and a
      stale ``in_sync: true`` is not a current one.
    * Otherwise MET exactly when ``in_sync`` is true.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    read = read_store_document(store, STACK_CHECK_LIVE_KEY)
    if read.problem is not None:
        return Reading(
            met=False,
            detail=f"could not read {STACK_CHECK_LIVE_KEY}: {read.problem}",
            evidence=(STACK_CHECK_LIVE_KEY,),
            unmeasurable=True,
            source="data_collection store",
        )
    if read.absent:
        return Reading(
            met=False,
            detail=(
                f"no check-live reading at {STACK_CHECK_LIVE_KEY}. "
                "`infrastructure/data_collection_stack.py check-live` runs on a schedule "
                "and after every deploy but does not yet publish its result here — until "
                "it does, this clause reads the gap rather than the stack"
            ),
            evidence=(STACK_CHECK_LIVE_KEY,),
            source="data_collection store",
        )
    document = read.document or {}
    as_of = str(document.get("as_of") or "")
    common = {"evidence": (STACK_CHECK_LIVE_KEY,), "source": "data_collection store", "as_of": as_of}
    version = document.get("schema_version")
    in_sync = document.get("in_sync")
    if version != STACK_CHECK_LIVE_SCHEMA or not isinstance(in_sync, bool):
        return Reading(
            met=False,
            detail=(
                f"{STACK_CHECK_LIVE_KEY} carries schema_version {version!r} / in_sync {in_sync!r}; "
                f"expected {STACK_CHECK_LIVE_SCHEMA!r} with a boolean in_sync. A document nobody "
                "defined a rendering for is a finding."
            ),
            **common,
        )
    if document.get("measured") is False:
        return Reading(
            met=False,
            detail=f"check-live could not measure the stack at {as_of}: {document.get('error')}",
            unmeasurable=True,
            **common,
        )
    stamp = _parse_utc(as_of) if as_of else None
    if stamp is None or now - stamp > STACK_CHECK_LIVE_MAX_AGE:
        return Reading(
            met=False,
            detail=(
                f"{STACK_CHECK_LIVE_KEY} as_of {as_of!r} is older than "
                f"{STACK_CHECK_LIVE_MAX_AGE.days} days (or unparseable): check-live has stopped "
                "publishing, and a stale verdict is not a current one"
            ),
            **common,
        )
    drift = document.get("drift") or []
    return Reading(
        met=in_sync,
        detail=(
            f"{STACK_CHECK_LIVE_KEY}: stack {document.get('stack')} in_sync={in_sync} at {as_of} "
            f"(code_sha {str(document.get('code_sha') or '?')[:12]})"
            + (f"; drift: {drift[:6]}" if drift else "")
        ),
        **common,
    )


def read_roles_bootstrapped(store: GateStore, role_names: tuple[str, ...]) -> Reading:
    """Whether the standalone stack's roles exist, via `iam:ListRolePolicies`.

    `alpha-engine-config-I10777`: the gate-read identity
    (`github-actions-data-gate-read`) holds `iam:GetRolePolicy`/
    `GetPolicyVersion`/`ListRolePolicies` but NOT `iam:GetRole`.
    `ListRolePolicies` answers the same existence question — it raises
    `NoSuchEntity` for a role that was never created — without the missing
    grant, so this reads (c) of the cutover-ready gate without needing a new
    IAM grant.

    `store` supplies the client: only a live `S3Store` carries an
    `iam_client` (mirroring its own lazy `.client` for S3); `LocalStore` and
    `EmptyStore` do not, so a local/test read is UNMEASURABLE by construction
    rather than reaching for `boto3` — the same "never MET without a read"
    rule this module exists to enforce, applied to IAM instead of S3.
    """
    evidence = tuple(f"iam:{name}" for name in role_names)
    client = getattr(store, "iam_client", None)
    if client is None:
        return Reading(
            met=False,
            detail=(
                "this store backend supplies no IAM client (only a live S3Store does); "
                f"roles: {list(role_names)}"
            ),
            evidence=evidence,
            unmeasurable=True,
            source="iam:ListRolePolicies",
        )
    missing: list[str] = []
    problems: list[str] = []
    for name in role_names:
        try:
            client.list_role_policies(RoleName=name)
        except Exception as exc:  # noqa: BLE001 - classified by AWS error code below
            code = ""
            response = getattr(exc, "response", None)
            if isinstance(response, dict):
                code = str((response.get("Error") or {}).get("Code") or "")
            if code == "NoSuchEntity":
                missing.append(name)
            else:
                problems.append(f"{name}: {type(exc).__name__}: {exc}")
    if problems:
        return Reading(
            met=False,
            detail=f"could not verify role(s): {'; '.join(problems)}",
            evidence=evidence,
            unmeasurable=True,
            source="iam:ListRolePolicies",
        )
    if missing:
        return Reading(
            met=False,
            detail=f"role(s) not bootstrapped — ListRolePolicies raised NoSuchEntity: {missing}",
            evidence=evidence,
            source="iam:ListRolePolicies",
        )
    return Reading(
        met=True,
        detail=f"ListRolePolicies succeeded for every declared role: {list(role_names)}",
        evidence=evidence,
        source="iam:ListRolePolicies",
    )


#: The pre-cutover shadow parity report (plan §6.2 step 4,
#: `alpha-engine-config-I10778`), relative to the `data_collection` store root
#: — i.e. `s3://alpha-engine-research/data_collection/parity/{trading_day}.json`.
#:
#: DEFINED HERE, on the consumer side, and imported by `shadow.parity` rather
#: than re-spelled there: the gate declares where it reads, and the producer
#: writes to that address by construction. Two files agreeing on a key string
#: is a coincidence that survives exactly until one of them is edited.
PARITY_KEY_TEMPLATE = "parity/{trading_day}.json"

#: The prefix every parity report is listed under, so the reader can find the
#: most recent one instead of only the gate's own trading day.
PARITY_KEY_PREFIX = "parity/"

#: How many TRADING days old (never calendar days — a Monday gate reading a
#: Friday shadow run is 1 trading day stale, not 3) a parity report may be and
#: still count as fresh. 5 trading days is one calendar week of slack for a
#: shadow run that is a one-off pre-cutover operation (`alpha-engine-config-
#: I10778`), not a daily producer — declared here, beside the reader that
#: enforces it, per `alpha-engine-config-I10857` deliverable 1.
PARITY_FRESHNESS_TRADING_DAYS = 5


def parity_store_key(trading_day: dt.date) -> str:
    return PARITY_KEY_TEMPLATE.format(trading_day=trading_day.isoformat())


def _parity_report_day(key: str) -> dt.date | None:
    """The trading day a `parity/<day>.json` key names, or `None` if the key
    under the prefix does not match that shape (never crash the listing over
    an unrelated object someone left beside the reports)."""
    if not key.startswith(PARITY_KEY_PREFIX) or not key.endswith(".json"):
        return None
    stem = key[len(PARITY_KEY_PREFIX) : -len(".json")]
    try:
        return dt.date.fromisoformat(stem)
    except ValueError:
        return None


def _trading_days_between(earlier: dt.date, later: dt.date) -> int:
    """How many trading-calendar steps separate two trading days, walking the
    calendar rather than subtracting dates — a Friday-to-Monday gap is 1, not
    3. Capped so a malformed report dated far in the past cannot spin this
    into a long loop; a report that old is stale by any window and the exact
    count stops mattering past the cap."""
    age = 0
    cursor = later
    while cursor > earlier and age <= 10_000:
        cursor = previous_trading_day(cursor)
        age += 1
    return age


def read_parity(store: GateStore, *, trading_day: dt.date) -> Reading:
    """Pre-cutover parity: the most recent report within the freshness window.

    Produced by `python -m shadow parity` (`alpha-engine-config-I10778`): one
    standalone shadow run writes every key under
    `staging/shadow/{trading_day}/`, each key is diffed against the same
    trading day's v1 output on row count, symbol set, schema and value
    tolerance, and the verdicts are published to `parity/{trading_day}.json`.

    **The clause's OWN trading day is not the report's.** A shadow run is a
    one-off pre-cutover operation for a completed day, so it is filed under
    that day's key — never the gate's own, running day. Reading only
    `parity/{gate trading_day}.json` (the pre-`I10857` shape) makes the clause
    structurally unmeetable: on any given day the gate looks for a report that
    cannot exist until the day is over. This reader instead lists every
    published report, keeps the ones dated on or before the gate's trading
    day (a report from the future is never selected, however it got there),
    and grades the most recent of those.

    **Freshness is trading days, via the calendar, never calendar days.**
    `PARITY_FRESHNESS_TRADING_DAYS` bounds how far back the selected report
    may be; older reads UNMET naming its age, so a stale shadow run cannot
    quietly stand in for cutover readiness forever.

    **Absence is UNMET, not UNMEASURABLE.** `read_objective`'s rule applies:
    we CAN look, the store answers, and "no shadow run has been compared
    within the window" is a finding about cutover readiness — which is the
    whole question this clause exists to answer — rather than a failure of
    the read. A failed *listing* (denied, throttled) is UNMEASURABLE, kept
    distinct the same way `read_run_record` keeps it distinct.

    The report's own `met` is not taken on trust: this reader re-derives the
    exception counts from `summary`, so a report claiming `met: true` while
    carrying unmeasurable rows reads UNMET and says which counts contradict it.
    """
    try:
        keys = list(store.list_keys(PARITY_KEY_PREFIX))
    except Exception as exc:  # noqa: BLE001 - classified as UNMEASURABLE, which is red
        # A deliberate catch (fail-loud rule, `AGENTS.md`): the failure mode is
        # "the parity prefix could not be listed"; the reading carrying every
        # other clause survives; the recording surface is this UNMEASURABLE row.
        return Reading(
            met=False,
            detail=f"could not list {PARITY_KEY_PREFIX}: {type(exc).__name__}: {exc}",
            evidence=(f"{PARITY_KEY_PREFIX}*.json",),
            unmeasurable=True,
            source="data_collection store",
        )

    candidates = sorted(
        (day, key)
        for key in keys
        if (day := _parity_report_day(key)) is not None and day <= trading_day
    )
    if not candidates:
        return Reading(
            met=False,
            detail=(
                f"no parity report at or before {trading_day.isoformat()} under "
                f"{PARITY_KEY_PREFIX}. Produce one with `python -m shadow run "
                "--trading-day <day> --module <entrypoint>` followed by `python -m shadow "
                "parity --trading-day <day> --store <uri>` (alpha-engine-config-I10778). "
                "Until then the cutover has no parity evidence, which is the answer, not a "
                "gap in the read."
            ),
            evidence=(f"{PARITY_KEY_PREFIX}*.json",),
            source="data_collection store",
        )

    report_day, key = candidates[-1]
    floor = subtract_trading_days(trading_day, PARITY_FRESHNESS_TRADING_DAYS)
    if report_day < floor:
        age_trading_days = _trading_days_between(report_day, trading_day)
        return Reading(
            met=False,
            detail=(
                f"most recent parity report is {key} (trading_day {report_day.isoformat()}), "
                f"{age_trading_days} trading day(s) before the gate's {trading_day.isoformat()} "
                f"— older than the {PARITY_FRESHNESS_TRADING_DAYS}-trading-day freshness window "
                "(evidence.PARITY_FRESHNESS_TRADING_DAYS). Stale."
            ),
            evidence=(key,),
            source="data_collection store",
        )

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
        # Vanished between listing and read — an access-timing gap, not an
        # answer about cutover readiness (same distinction `read_run_record`
        # draws for a manifest that disappears mid-read).
        return Reading(
            met=False,
            detail=f"{key} was listed but could not be read back (vanished between listing and read).",
            evidence=(key,),
            unmeasurable=True,
            source="data_collection store",
        )
    document = read.document or {}
    as_of = str(document.get("generated_at") or "")
    version = str(document.get("schema_version") or "")
    if version != "data_parity_report.v1":
        return Reading(
            met=False,
            detail=(
                f"{key} carries schema_version {version!r}, not 'data_parity_report.v1'. A "
                "document nobody defined a rendering for is a finding, not a reading."
            ),
            evidence=(key,),
            source="data_collection store",
            as_of=as_of,
        )
    reported_day = str(document.get("trading_day") or "")
    if reported_day != report_day.isoformat():
        return Reading(
            met=False,
            detail=(
                f"{key} reports trading_day {reported_day!r} while its own key names "
                f"{report_day.isoformat()}. A report filed under the wrong day is not "
                "evidence for either."
            ),
            evidence=(key,),
            source="data_collection store",
            as_of=as_of,
        )
    summary = document.get("summary") or {}
    total = int(summary.get("total") or 0)
    matched = int(summary.get("match") or 0)
    exceptions = {
        name: int(count)
        for name, count in summary.items()
        if name not in {"total", "match"} and int(count or 0)
    }
    met = bool(document.get("met")) and total > 0 and matched == total and not exceptions
    detail = f"{matched}/{total} published keys match (report {key}, trading_day {report_day.isoformat()})"
    if exceptions:
        detail += "; " + ", ".join(f"{name}={count}" for name, count in sorted(exceptions.items()))
    if document.get("met") and not met:
        detail += (
            " — the report claims met:true while carrying the exceptions above, so it is "
            "read UNMET"
        )
    return Reading(
        met=met,
        detail=detail,
        evidence=(key,),
        source="data_collection store",
        as_of=as_of,
    )
