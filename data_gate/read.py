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
from data_gate.descriptors import REPO_ROOT, load_units

__all__ = ["BOARD_KEY", "DataPhase", "GATES", "evaluate", "load_phases", "run"]

PHASES_PATH = REPO_ROOT / "data_gate" / "config" / "phases.yaml"

#: The per-clause board document the console's `data-collection-board` fragment
#: reads. `latest.json` only: the console renders current state and never owns
#: history (`console-policy` §1); the dated gate readings are the history.
BOARD_KEY = "gates/board/latest.json"


@dataclass(frozen=True)
class DataPhase:
    """One declared rung, before it becomes a ladder `Phase`."""

    id: str
    number: int
    title: str
    gate: str | None
    tracker_issue: int
    tracker_is_placeholder: bool
    exit_criteria: str

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
            exit_criteria=str(row.get("exit") or ""),
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
    if ceiling is None:
        selected = [c for c in all_clauses if c.phase == gate]
    else:
        selected = [
            c
            for c in all_clauses
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
    return result


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


def _board_document(clauses, *, trading_day: dt.date, generated_utc: str, store_uri: str | None) -> dict:
    """One row per clause, for the console's board fragment.

    Each row carries `console-policy` §5.1's four fields — state, source, as-of
    and evidence — and the header carries the transparency-gap count, which is
    the number this whole board exists to drive to zero.
    """
    rows = []
    for clause in clauses:
        state = "UNMEASURABLE" if clause.unmeasurable else ("MET" if clause.met else "UNMET")
        rows.append(
            {
                "clause": clause.name,
                "unit_id": _row_unit_id(clause.name),
                "state": state,
                "console_state": {
                    "MET": "HEALTHY",
                    "UNMET": "DEGRADED",
                    "UNMEASURABLE": "UNREPORTED",
                }[state],
                "phase": clause.phase,
                "requirement": clause.requirement,
                "detail": clause.detail,
                "evidence": sorted(clause.evidence),
                "source": clause.source,
                "as_of": clause.as_of,
            }
        )
    unmeasurable = sum(1 for r in rows if r["state"] == "UNMEASURABLE")
    return {
        "schema_version": "data_board.v1",
        "board": "data-collection",
        "trading_day": trading_day.isoformat(),
        "generated_utc": generated_utc,
        "store": store_uri,
        "clauses_total": len(rows),
        "clauses_met": sum(1 for r in rows if r["state"] == "MET"),
        "clauses_unmet": sum(1 for r in rows if r["state"] == "UNMET"),
        # The transparency gap (observability-policy §8.4). Published even at
        # zero: a coverage figure that only appears when non-zero cannot be told
        # apart from health when it is absent.
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
) -> tuple[GateResult, dict, dict]:
    """Evaluate, publish, and return the reading plus the ladder document.

    Publishing is three objects: the dated gate reading (history, never
    overwritten), the ladder (current state, rewritten every read) and the board
    (one row per clause). Under ``dry_run`` nothing is written at all.
    """
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
    )

    if not dry_run:
        store.put_bytes(
            gate_key(gate, trading_day.isoformat()),
            json.dumps(result.to_dict(), indent=2, sort_keys=True).encode("utf-8"),
        )
        store.put_bytes("gates/ladder.json", ladder_bytes)
        store.put_bytes(BOARD_KEY, json.dumps(board, indent=2, sort_keys=True).encode("utf-8"))

    return result, json.loads(ladder_bytes), board


def render(result: GateResult, board: dict, *, dry_run: bool) -> str:
    """What the operator and the CI log see."""
    lines = [result.render(), ""]
    lines.append(
        f"board: {board['clauses_met']} met / {board['clauses_unmet']} unmet / "
        f"{board['transparency_gap']} unmeasurable of {board['clauses_total']} clauses"
    )
    lines.append(f"transparency gap (UNREPORTED): {board['transparency_gap']} — objective is 0")
    if dry_run:
        lines.append("dry run: nothing was written")
    return "\n".join(lines)
