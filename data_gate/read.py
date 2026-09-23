"""`data_gate read` — evaluate the board, publish the reading, exit on the gate.

Three rules, carried from `crucible gate` and restated in
`data_collection_plan_260914.md` §4.1 because they are the whole design:

1. **A gate reads; it never runs.** Every clause is evaluated against artifacts
   already written. A merge can never satisfy a gate.
2. **A clause is MET, UNMET or UNMEASURABLE, and UNMEASURABLE is never met.**
3. **The gate's job succeeds when the MEASUREMENT succeeds.** The ladder is
   written and the process exits non-zero unless the gate is met, so CI or a
   person cannot read "not there yet" as "done".

One evaluation per invocation. Every gate's reading, the ladder and the board
are all projections of that ONE clause list, so they cannot disagree with each
other — the defect that made a crucible console re-evaluate every gate under its
own environment and republish a false ladder over the gate publisher's.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
from dataclasses import dataclass

import yaml
from nousergon_lib.gates import (
    GateResult,
    Phase,
    build_ladder,
    gate_key,
    ladder_payload,
)

from data_gate import clauses as clause_module
from data_gate.descriptors import CONNECTION_STATES, REPO_ROOT, load_units

__all__ = [
    "BOARD_KEY",
    "DataPhase",
    "ExitCriterion",
    "GATES",
    "TRIGGERS",
    "evaluate",
    "load_phases",
    "run",
]

#: What caused a gate read, recorded on the dated `gates/<gate>/{date}/
#: gate.json` (`alpha-engine-config-I11355` deliverable 2).
#:
#: * ``schedule`` — the 23:30 UTC daily / 18:00 UTC Saturday backstop crons.
#:   These fire BEFORE the same-day parity publishes (~23:43 UTC), so a reading
#:   carrying this trigger and a stale report is the EXPECTED shape, not a
#:   normal reading: the parity clause reads UNMEASURABLE and says so.
#: * ``post-parity`` — a cron placed after the parity publish (01:30 UTC
#:   Tue–Sat for the same-day report, 12:30 UTC Mon–Fri for the morning
#:   rewrite). This is the reading whose parity clause can be MET or UNMET on
#:   the gate's own trading day.
#: * ``parity-published`` — the event-driven read (alpha-engine-config-I11361):
#:   `shadow parity --dispatch-gate` dispatches this workflow right after a
#:   successful put, from the scheduled same-day and morning runs, with the
#:   trading day it just published. The post-parity crons stay as the backstop
#:   for a dispatch that failed (recorded in the report as `gate_dispatch`).
#: * ``push`` / ``manual`` — the `push` trigger and a human dispatch.
TRIGGERS: frozenset[str] = frozenset(
    {"schedule", "post-parity", "parity-published", "push", "manual"}
)

PHASES_PATH = REPO_ROOT / "data_gate" / "config" / "phases.yaml"

#: The per-clause board document the console's `data-collection-board` fragment
#: reads. `latest.json` only: the console renders current state and never owns
#: history (`console-policy` §1); the dated gate readings are the history.
BOARD_KEY = "gates/board/latest.json"


@dataclass(frozen=True)
class ExitCriterion:
    """One line of a rung's exit, and the clause(s) that MEASURE it.

    `alpha-engine-config-I10954`. Exactly one of ``clause`` and
    ``exit_measured_by`` is set — the first for the clause that exists FOR this
    criterion, the second for the clause(s) that already covered it. Both are
    resolved against the generated clause set by
    `tests/test_phase_exit_criteria_are_graded.py`, so a criterion that names
    nothing, or names a clause the phase's own gate does not grade, is a test
    failure rather than a line of prose the gate reads MET around.
    """

    phase_id: str
    text: str
    #: `fnmatch` patterns, so one criterion can name a clause FAMILY
    #: (``data.D*.run_record``) without restating 46 names that the descriptors
    #: already generate.
    patterns: tuple[str, ...]
    #: Which key declared them — ``clause`` or ``exit_measured_by``.
    declared_by: str


def _exit_criteria(row: dict) -> tuple[ExitCriterion, ...]:
    """A rung's `exit:` list, refusing every shape that could read as graded.

    Raises rather than skipping: a criterion this loader silently dropped is
    exactly the defect I10954 was filed for, one level further in.
    """
    phase_id = str(row["id"])
    declared = row.get("exit")
    if not isinstance(declared, list) or not declared:
        raise ValueError(
            f"{phase_id} declares `exit:` as {type(declared).__name__}, not a non-empty list. "
            "A rung's exit is a list of criteria, each naming the clause that measures it "
            "(alpha-engine-config-I10954) — prose is what let a gate read MET with the "
            "operational counters unmeasured."
        )
    criteria: list[ExitCriterion] = []
    for entry in declared:
        if not isinstance(entry, dict) or not str(entry.get("criterion") or "").strip():
            raise ValueError(f"{phase_id}: an exit entry carries no `criterion:` text: {entry!r}")
        text = " ".join(str(entry["criterion"]).split())
        named = [key for key in ("clause", "exit_measured_by") if entry.get(key)]
        if len(named) != 1:
            raise ValueError(
                f"{phase_id}: exit criterion {text!r} declares {named or 'neither'} — exactly one "
                "of `clause:` (the clause that exists for this criterion) and "
                "`exit_measured_by:` (the clause that already covers it) is required."
            )
        key = named[0]
        value = entry[key]
        patterns = tuple(str(v) for v in (value if isinstance(value, list) else [value]))
        criteria.append(
            ExitCriterion(phase_id=phase_id, text=text, patterns=patterns, declared_by=key)
        )
    return tuple(criteria)


@dataclass(frozen=True)
class DataPhase:
    """One declared rung, before it becomes a ladder `Phase`."""

    id: str
    number: int
    title: str
    gate: str | None
    tracker_issue: int
    tracker_is_placeholder: bool
    exit_criteria: tuple[ExitCriterion, ...]
    notes: str = ""

    def as_ladder_phase(self) -> Phase:
        return Phase.on_alpha_engine_config(
            id=self.id,
            number=self.number,
            title=self.title,
            issue=self.tracker_issue,
            gate=self.gate,
        )


def load_phases(path=None) -> list[DataPhase]:
    document = yaml.safe_load((path or PHASES_PATH).read_text(encoding="utf-8"))
    phases = [
        DataPhase(
            id=row["id"],
            number=int(row["number"]),
            title=row["title"],
            gate=row.get("gate"),
            tracker_issue=int(row["tracker_issue"]),
            tracker_is_placeholder=bool(row.get("tracker_is_placeholder", False)),
            exit_criteria=_exit_criteria(row),
            notes=" ".join(str(row.get("notes") or "").split()),
        )
        for row in document["phases"]
    ]
    if not phases:
        raise ValueError(
            f"{PHASES_PATH} declares no phases. A ladder with no rungs publishes "
            "`current_phase: complete` over a system nobody graded."
        )
    return phases


#: Every registered gate, and the highest clause phase it grades — or `None`
#: for a gate that is not numbered ceiling-style at all. A numbered gate
#: (`"data-phaseN"`) selects every clause tagged `data-phaseM` with `M <= N`;
#: a `None` gate selects only the clauses tagged with ITS OWN name exactly
#: (`alpha-engine-config-I10777`) — `data-cutover-ready` is a sub-gate of
#: phase 1 (`registry.d/phases.yaml`'s `sub_gates`), not a ceiling over it,
#: so it must not silently re-grade phase 1's own clause list under a second
#: name. A gate that is not registered here has no clause list at all, so
#: running it would report a pass over nothing — `evaluate` raises rather
#: than defaulting.
GATES: dict[str, int | None] = {
    "data-phase0": 0,
    "data-cutover-ready": None,
    "data-phase1": 1,
    "data-phase2": 2,
    "data-phase3": 3,
    # `alpha-engine-config-I10793`/`-I10788`, Brian's 2026-09-21 ruling: a
    # second `ceiling=None` gate, alongside `data-cutover-ready`, publishing
    # the three reliability-streak STANDING clauses
    # (`clauses.py::RELIABILITY_GATE`) as their own artifact — Crucible v2
    # phase 4 gates its irreversible v1-pipeline deletion
    # (`alpha-engine-config-I10655`) on this, never on a numbered data-phase
    # gate. Never a ceiling: a numbered rung's own `<= ceiling` selection
    # already cannot match a non-`data-phaseN` phase tag, so this gate is
    # reached only by its own exact name, exactly like `data-cutover-ready`.
    clause_module.RELIABILITY_GATE: None,
}


def _code_sha() -> str:
    try:
        return subprocess.run(  # noqa: S607 - git resolved from PATH in a dev/CI context only
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - provenance is best-effort, the reading is not
        # A deliberate catch: the failure mode is "the commit could not be
        # resolved"; the reading itself survives; and the recording surface is
        # the literal string below, which is published on the artifact so a
        # reader sees that provenance is missing instead of assuming it.
        return "unknown (git rev-parse failed)"


def _phase_number(clause_phase: str | None) -> int | None:
    if not clause_phase or not clause_phase.startswith("data-phase"):
        return None
    try:
        return int(clause_phase[len("data-phase") :])
    except ValueError:
        return None


def evaluate(store, *, gate: str, trading_day: dt.date, all_clauses=None) -> GateResult:
    """``gate``'s clauses, out of one board-wide evaluation.

    ``all_clauses`` lets a caller that has already evaluated the board hand the
    list in, so one invocation never evaluates the same clause twice.
    """
    if gate not in GATES:
        raise KeyError(
            f"unknown gate {gate!r}; the registered gates are {sorted(GATES)}. A gate that "
            "is not registered has no clause list, so running it would report a pass over "
            "nothing."
        )
    if all_clauses is None:
        all_clauses = clause_module.generate(
            store, load_units(), load_phases(), trading_day=trading_day
        )
    ceiling = GATES[gate]
    # RETIRED clauses grade nothing: excluded from every gate's clause list, so
    # they count in no MET/UNMET/UNMEASURABLE denominator and no phase's red
    # count (alpha-engine-config-I10823 deliverable 6). The board still renders
    # them, with the reason.
    # UNCONNECTED clauses (a unit kept with no surviving consumer by a recorded
    # decision, alpha-engine-config-I10873) are excluded the same way: never
    # MET-green, never red toward any phase exit.
    retired = [c for c in all_clauses if clause_module.is_retired(c)]
    unconnected = [c for c in all_clauses if clause_module.is_unconnected(c)]
    gradable = [c for c in all_clauses if not clause_module.is_ungraded(c)]
    if ceiling is None:
        # `alpha-engine-config-I10793`/`-I10788`: a STANDING clause whose OWN
        # declared `phase` names THIS exact gate is graded by it — "graded by
        # no gate" (`is_ungraded`) means no NUMBERED data-phase gate, never
        # "graded by nothing at all". A numbered gate's `<= ceiling` branch
        # below needs no equivalent carve-out: `_phase_number` already returns
        # `None` for a non-`data-phaseN` tag like `RELIABILITY_GATE`, so a
        # standing clause published under its own gate can never match a
        # numbered ceiling regardless of this branch.
        selected = [
            c
            for c in all_clauses
            if c.phase == gate and (not clause_module.is_ungraded(c) or clause_module.is_standing(c))
        ]
    else:
        selected = [
            c
            for c in gradable
            if (_phase_number(c.phase) is not None and _phase_number(c.phase) <= ceiling)
        ]
    result = GateResult(
        gate=gate,
        trading_day=trading_day,
        window=[trading_day],
        clauses=selected,
        store=getattr(store, "uri", None),
        code_sha=_code_sha(),
    )
    if ceiling is None:
        result.coverage = (
            f"grades the {len(selected)} clause(s) tagged phase == {gate!r} out of "
            f"{len(all_clauses)} on the board; the rest are graded by other gates"
        )
    else:
        result.coverage = (
            f"grades the {len(selected)} clause(s) tagged phase <= {ceiling} out of "
            f"{len(all_clauses)} on the board; the rest are graded by later gates"
        )
    if retired:
        result.coverage += f"; {len(retired)} RETIRED clause(s) on the board are graded by no gate"
    if unconnected:
        result.coverage += (
            f"; {len(unconnected)} UNCONNECTED consumers clause(s) (kept, no surviving consumer, "
            "by recorded decision) are graded by no gate"
        )
    return result


#: Board state -> console state (`observability-policy` §8.3). Total over the
#: board's four states; a state without a row here is a KeyError, not a default.
CONSOLE_STATE: dict[str, str] = {
    "MET": "HEALTHY",
    "UNMET": "DEGRADED",
    "UNMEASURABLE": "UNREPORTED",
    "RETIRED": "RETIRED",
    # A unit kept with no surviving consumer by a recorded decision
    # (alpha-engine-config-I10873). §8.3 DISABLED — "deliberately off, with a
    # declared reason, owner and re-exam trigger" — is the closed-vocabulary
    # member for a declared, non-failing, non-healthy disposition; the board's
    # own `state` keeps the precise name.
    "UNCONNECTED": "DISABLED",
}


#: A base or guard clause's audit unit id — `data.D07.schema_contract` or
#: `data.D20.guard.cardinality` — else the row has no unit.
_UNIT_ID_RE = re.compile(r"^data\.(D\d+)\.")


def _row_unit_id(clause_name: str) -> str:
    """The audit unit a board row belongs to, or a category label for a
    non-unit clause — `board`, `gate`, `slo`, `cost`, `pages`, `human_touch`,
    `inventory`, `cutover_ready` (`alpha-engine-config-I10802`).

    Parsed ONCE here, at generation time, rather than re-derived by the
    console — `nousergon-console`'s `records_shape.py` has no regex-
    extraction capability, per that issue's own fix description. Every
    `data.<x>.<...>` clause name carries the category as its second dotted
    segment, unit or not, so one regex plus a fallback split covers every
    clause family this module generates, including ones added after this
    function was written.
    """
    match = _UNIT_ID_RE.match(clause_name)
    if match:
        return match.group(1)
    parts = clause_name.split(".")
    return parts[1] if len(parts) > 1 else clause_name


def _connection_summary(units) -> tuple[dict[str, dict], dict]:
    """Per-unit connection facts and their counts (`alpha-engine-config-I10873`).

    Every unit is exactly one of `CONNECTION_STATES`; the counts state their
    denominator (`console-policy` §5.3) and are published even at zero.
    """
    per_unit: dict[str, dict] = {}
    for unit in units:
        decision = unit.consumers_decision
        per_unit[unit.unit_id] = {
            "connection": unit.connection,
            "consumers": unit.surviving_consumers,
            "retiring_consumers": unit.retiring_consumers,
            "connection_reason": unit.connection_reason,
            "connection_decision": (
                f"{decision['decision']} by {decision['ruled_by']} {decision['ruled_on']}" if decision else None
            ),
        }
    counts = {state: 0 for state in CONNECTION_STATES}
    for facts in per_unit.values():
        counts[facts["connection"]] += 1
    counts["units_total"] = len(per_unit)
    counts["unconnected_decided"] = sum(
        1 for f in per_unit.values() if f["connection"] == "unconnected" and f["connection_decision"]
    )
    counts["unconnected_undecided"] = counts["unconnected"] - counts["unconnected_decided"]
    return per_unit, counts


def _board_document(
    clauses, *, trading_day: dt.date, generated_utc: str, store_uri: str | None, units=None
) -> dict:
    """One row per clause, for the console's board fragment.

    Each row carries `console-policy` §5.1's four fields — state, source, as-of
    and evidence — and the header carries the transparency-gap count, which is
    the number this whole board exists to drive to zero.

    Every row of an audit unit (``unit_id`` ``Dxx``) also carries that unit's
    connection facts — ``connection``, ``consumers``, ``retiring_consumers``,
    ``connection_reason``, ``connection_decision`` — so the console can filter
    the board to what is connected and what is not (`alpha-engine-config-I10873`).
    Non-unit rows carry none of them: connection is not a fact about them.
    """
    per_unit, connection_counts = _connection_summary(units if units is not None else load_units())
    rows = []
    for clause in clauses:
        standing = clause_module.is_standing(clause)
        if clause_module.is_retired(clause):
            state = "RETIRED"
        elif clause_module.is_unconnected(clause):
            state = "UNCONNECTED"
        else:
            # A STANDING clause (`alpha-engine-config-I10793`/`-I10788`, Brian's
            # 2026-09-21 ruling) renders its REAL state here — MET, UNMET or
            # UNMEASURABLE, exactly like an ordinary clause. "No data is never
            # rendered as green" cuts both ways: standing is a fact about
            # which gate counts it, never a euphemism for its own reading.
            state = "UNMEASURABLE" if clause.unmeasurable else ("MET" if clause.met else "UNMET")
        unit_id = _row_unit_id(clause.name)
        row = {
            "clause": clause.name,
            "unit_id": unit_id,
            "state": state,
            # `observability-policy` §8.3's closed vocabulary. RETIRED and
            # DISABLED are declared states (neither green nor red), and
            # `nousergon-console` `console/model/kinds.py` carries both.
            "console_state": CONSOLE_STATE[state],
            "phase": clause.phase,
            "requirement": clause.requirement,
            "detail": clause.detail,
            "evidence": sorted(clause.evidence),
            "source": clause.source,
            "as_of": clause.as_of,
            # `alpha-engine-config-I10793`/`-I10788`: True for a clause read
            # and rendered every day with its real state but graded by NO
            # gate (`clauses.StandingClause`). Distinct from `state`, which
            # stays real — this field is the only place "excluded from
            # gating" is recorded, so a consumer can tell "measured, not
            # gating" apart from "measured, gating, currently green/red".
            "standing": standing,
        }
        if standing:
            row["standing_ruling"] = getattr(clause, "ruling", "")
        if unit_id in per_unit:
            row.update(per_unit[unit_id])
        rows.append(row)
    unmeasurable = sum(1 for r in rows if r["state"] == "UNMEASURABLE" and not r["standing"])
    retired = sum(1 for r in rows if r["state"] == "RETIRED")
    unconnected = sum(1 for r in rows if r["state"] == "UNCONNECTED")
    standing_count = sum(1 for r in rows if r["standing"])
    return {
        "schema_version": "data_board.v1",
        "board": "data-collection",
        "trading_day": trading_day.isoformat(),
        "generated_utc": generated_utc,
        "store": store_uri,
        # The graded denominator: RETIRED, UNCONNECTED and STANDING rows are
        # published but excluded — none of the three is graded by any gate.
        "clauses_total": len(rows) - retired - unconnected - standing_count,
        "clauses_retired": retired,
        "clauses_unconnected": unconnected,
        "clauses_standing": standing_count,
        "connection_counts": connection_counts,
        "clauses_met": sum(1 for r in rows if r["state"] == "MET" and not r["standing"]),
        "clauses_unmet": sum(1 for r in rows if r["state"] == "UNMET" and not r["standing"]),
        # The transparency gap (observability-policy §8.4). Published even at
        # zero: a coverage figure that only appears when non-zero cannot be told
        # apart from health when it is absent. A standing clause's own
        # UNMEASURABLE state (if it ever reads one) is excluded here too — it
        # is graded by no gate, so it cannot widen a gap no gate is reading.
        "transparency_gap": unmeasurable,
        "rows": rows,
    }


def run(
    store,
    *,
    gate: str,
    trading_day: dt.date,
    dry_run: bool = False,
    now: dt.datetime | None = None,
    trigger: str = "manual",
) -> tuple[GateResult, dict, dict]:
    """Evaluate, publish, and return the reading plus the ladder document.

    Publishing is three objects: the dated gate reading (history, never
    overwritten), the ladder (current state, rewritten every read) and the board
    (one row per clause). Under ``dry_run`` nothing is written at all.

    ``trigger`` is recorded on the dated reading (`alpha-engine-config-I11355`).
    """
    if trigger not in TRIGGERS:
        raise ValueError(
            f"unknown trigger {trigger!r}; the declared triggers are {sorted(TRIGGERS)}. An "
            "undeclared value would make the `trigger` column unsummable, which is the "
            "whole reason it is recorded."
        )
    units = load_units()
    phases = load_phases()
    all_clauses = clause_module.generate(store, units, phases, trading_day=trading_day)

    readings = {
        name: evaluate(store, gate=name, trading_day=trading_day, all_clauses=all_clauses)
        for name in GATES
    }
    result = readings[gate]

    ladder = build_ladder(
        store,
        phases=[p.as_ladder_phase() for p in phases],
        trading_day=trading_day,
        evaluate=lambda name: readings[name],
        readings=readings,
        now=now,
        name="data",
    )
    ladder_bytes = ladder_payload(ladder)
    board = _board_document(
        all_clauses,
        trading_day=trading_day,
        generated_utc=ladder.generated_utc,
        store_uri=getattr(store, "uri", None),
        units=units,
    )

    if not dry_run:
        # `trigger` is added here rather than inside `GateResult.to_dict()`:
        # that shape lives in `nousergon_lib.gates` and is shared by every
        # gate in the fleet, and this is a data-collection-specific fact about
        # WHEN the read was taken (alpha-engine-config-I11355). Lifting it into
        # the library is the right move on the second adopter (`policy-shared-
        # code`), not the first.
        reading = result.to_dict()
        reading["trigger"] = trigger
        store.put_bytes(
            gate_key(gate, trading_day.isoformat()),
            json.dumps(reading, indent=2, sort_keys=True).encode("utf-8"),
        )
        store.put_bytes("gates/ladder.json", ladder_bytes)
        store.put_bytes(BOARD_KEY, json.dumps(board, indent=2, sort_keys=True).encode("utf-8"))

    return result, json.loads(ladder_bytes), board


def render(result: GateResult, board: dict, *, dry_run: bool) -> str:
    """What the operator and the CI log see."""
    lines = [result.render(), ""]
    lines.append(
        f"board: {board['clauses_met']} met / {board['clauses_unmet']} unmet / "
        f"{board['transparency_gap']} unmeasurable of {board['clauses_total']} graded clauses "
        f"({board['clauses_retired']} RETIRED, {board['clauses_unconnected']} UNCONNECTED, "
        f"{board.get('clauses_standing', 0)} STANDING, graded by no gate)"
    )
    counts = board["connection_counts"]
    lines.append(
        f"units: {counts['connected']} connected / {counts['unconnected']} unconnected "
        f"({counts['unconnected_undecided']} with no recorded decision) / {counts['retired']} retired "
        f"of {counts['units_total']}"
    )
    lines.append(f"transparency gap (UNREPORTED): {board['transparency_gap']} — objective is 0")
    # `alpha-engine-config-I10793`/`-I10788`: a STANDING clause is graded by no
    # gate, so `result.render()` above (the SPECIFIC gate's own clause list)
    # never shows it — printed here instead, with its real state, so `data_gate
    # read` never hides a real reading behind the exclusion that keeps it from
    # blocking a phase (Brian's 2026-09-21 ruling).
    standing_rows = [r for r in board["rows"] if r.get("standing")]
    if standing_rows:
        lines.append(f"standing (measured daily, gates no phase): {len(standing_rows)}")
        for row in standing_rows:
            marker = "x" if row["state"] == "MET" else ("?" if row["state"] == "UNMEASURABLE" else " ")
            lines.append(f"  [{marker}] {row['clause']}: {row['detail']}")
    if dry_run:
        lines.append("dry run: nothing was written")
    return "\n".join(lines)
