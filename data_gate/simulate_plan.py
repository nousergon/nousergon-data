"""The gate's IAM simulate list, generated from the descriptors (alpha-engine-config-I11279).

The ``identity`` base column grades each unit's writer role with
``iam:SimulatePrincipalPolicy`` (`unit_readers.read_identity`). For that call
to answer at all, the gate's own role (`github-actions-data-gate-read`) needs
``iam:SimulatePrincipalPolicy`` on every role it simulates. That Resource list
lived in `nous-ergon-ops` as a hand-kept list one role long while
`data_gate/config/writer_identities.yaml` named five, so five clauses read
UNMEASURABLE on every board — the third time in a month a Resource list kept in
step with a declared set by hand drifted from it.

This module is the declared set, computed:

* `board_plan` — every ``(unit, role, action, resource, expect)`` row one board
  read sends to IAM, from `unit_readers.identity_plan`, the same function
  `read_identity` issues its calls from. `tests/test_simulate_plan.py` runs the
  reader over every unit with a recording client and fails if a call is not a
  row, or a row is never called.
* `simulated_roles` — the roles those rows name, i.e. the Resource list the
  gate role's ``DataGateSimulateWriterIdentities`` statement must carry. It is
  committed as `data_gate/config/simulate_roles.generated.json` (checked fresh
  by the test above) because `nous-ergon-ops`, which owns the policy and is
  private, reads this PUBLIC file from a clone in its cross-repo contract job;
  the assertion sits on the side that can see both halves
  (`policies/infrastructure-ownership-policy.md` §8).

Usage::

    python -m data_gate.simulate_plan rows [--json]   # every simulate row
    python -m data_gate.simulate_plan roles --check   # generated file is current
    python -m data_gate.simulate_plan roles --write   # regenerate it
    python -m data_gate.simulate_plan run [--role R]  # live, read-only: allowed/implicitDeny per row

`run` makes the same one-ARN-per-call requests as the reader, under whatever
credentials boto3 resolves. It never writes anything.
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import sys
from typing import Any, Iterable

from data_gate.descriptors import REPO_ROOT, Unit, load_units
from data_gate.sources import GATE_ROLE
from data_gate.unit_readers import (
    WRITER_IDENTITIES_PATH,
    SimulateRow,
    ThrottledSimulation,
    UnattributableSimulation,
    _error_code,
    _simulate_one,
    identity_plan,
    load_identities,
)

GENERATED_ROLES_PATH = REPO_ROOT / "data_gate" / "config" / "simulate_roles.generated.json"
SCHEMA_VERSION = "data_gate_simulate_roles.v1"
#: The statement in `nous-ergon-ops/infrastructure/iam/<GATE_ROLE>/data-gate-read.json`
#: whose Resource list the generated file declares.
GRANT_SID = "DataGateSimulateWriterIdentities"
REGENERATE = "python -m data_gate.simulate_plan roles --write"


def board_plan(units: Iterable[Unit] | None = None, config: dict[str, Any] | None = None) -> list[SimulateRow]:
    """Every simulate call one full board read makes, in call order."""
    config = config if config is not None else load_identities()
    rows: list[SimulateRow] = []
    for unit in units if units is not None else load_units():
        rows.extend(identity_plan(unit, config).rows)
    return rows


def simulated_roles(rows: Iterable[SimulateRow]) -> dict[str, list[str]]:
    """``{role: [unit_id, ...]}`` over the plan — the roles the gate role must be able to simulate."""
    by_role: dict[str, set[str]] = collections.defaultdict(set)
    for row in rows:
        by_role[row.role].add(row.unit_id)
    return {role: sorted(units) for role, units in sorted(by_role.items())}


def roles_document(units: Iterable[Unit] | None = None, config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config if config is not None else load_identities()
    rows = board_plan(units, config)
    account = str(config["account_id"])
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_by": REGENERATE,
        "derived_from": [
            WRITER_IDENTITIES_PATH.relative_to(REPO_ROOT).as_posix(),
            "registry.d/units/*.yaml (non-retired, identity not partially excluded, >=1 simulable write)",
        ],
        "gate_role": GATE_ROLE,
        "grant": {
            "repo": "nous-ergon-ops",
            "path": f"infrastructure/iam/{GATE_ROLE}/data-gate-read.json",
            "sid": GRANT_SID,
            "action": "iam:SimulatePrincipalPolicy",
        },
        # Roles only, deliberately: the units behind each role and the call
        # count change with every descriptor edit, and a generated file that
        # every descriptor PR must regenerate would be churn with no consumer.
        # `rows` prints both. This file changes exactly when the grant must.
        "roles": [
            {"role": role, "arn": f"arn:aws:iam::{account}:role/{role}"} for role in simulated_roles(rows)
        ],
    }


def render(document: dict[str, Any]) -> str:
    return json.dumps(document, indent=2, sort_keys=False) + "\n"


def run_live(rows: list[SimulateRow], client) -> list[tuple[SimulateRow, str]]:
    """``[(row, decision)]`` against live IAM. A call that fails is recorded by its error code."""
    account_by_role: dict[str, str] = {}
    out: list[tuple[SimulateRow, str]] = []
    config = load_identities()
    for row in rows:
        arn = account_by_role.setdefault(row.role, f"arn:aws:iam::{config['account_id']}:role/{row.role}")
        try:
            decision = _simulate_one(client, arn, row.action, row.resource)
        except (UnattributableSimulation, ThrottledSimulation) as exc:
            decision = f"ERROR:{type(exc).__name__}"
        except Exception as exc:  # noqa: BLE001 - recorded per row, the run continues
            decision = f"ERROR:{_error_code(exc) or type(exc).__name__}"
        out.append((row, decision))
    return out


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data_gate.simulate_plan", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    rows = sub.add_parser("rows", help="print every simulate row one board read makes")
    rows.add_argument("--json", action="store_true")
    roles = sub.add_parser("roles", help="the generated role list")
    mode = roles.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help=f"regenerate {GENERATED_ROLES_PATH.name}")
    mode.add_argument("--check", action="store_true", help="exit 1 if the committed file is stale")
    live = sub.add_parser("run", help="simulate every row against live IAM (read-only)")
    live.add_argument("--role", action="append", help="only rows for this role (repeatable)")
    live.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "rows":
        plan = board_plan()
        if args.json:
            print(json.dumps([row.__dict__ for row in plan], indent=2))
        else:
            for row in plan:
                print(f"{row.unit_id}\t{row.role}\t{row.action}\t{row.resource}\texpect={row.expect}")
            print(f"# {len(plan)} simulate call(s) per board read, {len(simulated_roles(plan))} role(s)")
        return 0
    if args.command == "roles":
        text = render(roles_document())
        if args.write:
            GENERATED_ROLES_PATH.write_text(text, encoding="utf-8")
            return 0
        if args.check:
            current = GENERATED_ROLES_PATH.read_text(encoding="utf-8") if GENERATED_ROLES_PATH.exists() else ""
            if current != text:
                print(f"{GENERATED_ROLES_PATH.name} is stale; run `{REGENERATE}`", file=sys.stderr)
                return 1
            return 0
        sys.stdout.write(text)
        return 0
    import boto3  # deferred: `rows`/`roles` must run without AWS libraries configured

    plan = [row for row in board_plan() if not args.role or row.role in args.role]
    results = run_live(plan, boto3.client("iam"))
    tally: collections.Counter[tuple[str, str]] = collections.Counter()
    for row, decision in results:
        tally[(row.role, decision)] += 1
    if args.json:
        print(json.dumps([{**row.__dict__, "decision": decision} for row, decision in results], indent=2))
    else:
        for row, decision in results:
            flag = "" if decision == row.expect or (row.expect == "denied" and decision.endswith("Deny")) else "  <-- unexpected"
            print(f"{row.unit_id}\t{row.role}\t{row.action}\t{row.resource}\t{decision}{flag}")
        for (role, decision), count in sorted(tally.items()):
            print(f"# {role}: {decision} x{count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
