"""`python -m data_gate read --gate data-phase<N> --store <uri> [--dry-run]`.

Exit codes are the contract, not a detail:

* **0** — the measurement succeeded AND the gate is met.
* **1** — the measurement succeeded and the gate is NOT met. The ladder is still
  written. This is the code that stops CI or a person reading "not there yet" as
  "done".
* **2** — the measurement itself failed (the clause list would not build, the
  descriptors would not validate). Distinct from 1 on purpose: "the gate says
  no" and "we could not ask" are different facts, and a single non-zero code
  would let a broken grader look exactly like a failing system.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

from data_gate import read as read_module
from data_gate.sources import GITHUB_TOKEN_ENV
from data_gate.store import open_store

EXIT_MET = 0
EXIT_NOT_MET = 1
EXIT_UNMEASURED = 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data_gate", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    reader = sub.add_parser("read", help="read a gate and publish the ladder")
    reader.add_argument("--gate", required=True, choices=sorted(read_module.GATES))
    reader.add_argument(
        "--store",
        required=True,
        help="s3://alpha-engine-research/data_collection, or a local directory",
    )
    reader.add_argument(
        "--trading-day",
        default=None,
        help="ISO date the reading is filed under; defaults to today UTC",
    )
    reader.add_argument(
        "--artifact-registry",
        default=None,
        help=(
            "ARTIFACT_REGISTRY.yaml to grade the artifact_registry column against: a local path or "
            "s3:// URI. Defaults to the published copy for an s3:// store; a local store without "
            "it reads that column UNMEASURABLE."
        ),
    )
    reader.add_argument(
        "--dry-run",
        action="store_true",
        help="evaluate and render, write nothing at all",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    trading_day = (
        dt.date.fromisoformat(args.trading_day)
        if args.trading_day
        else dt.datetime.now(dt.timezone.utc).date()
    )
    store = open_store(
        args.store,
        dry_run=args.dry_run,
        artifact_registry=args.artifact_registry,
        github_token=os.environ.get(GITHUB_TOKEN_ENV) or None,
    )
    try:
        result, _ladder, board = read_module.run(
            store, gate=args.gate, trading_day=trading_day, dry_run=args.dry_run
        )
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        # Deliberate, and narrow in effect: the failure mode is "the measurement
        # could not be taken at all"; nothing is published, so no reader can
        # mistake this for a reading; and the recording surface is stderr plus
        # exit code 2, which is distinct from the gate saying no.
        print(f"data_gate: the measurement failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_UNMEASURED
    print(read_module.render(result, board, dry_run=args.dry_run))
    return EXIT_MET if result.met else EXIT_NOT_MET


if __name__ == "__main__":
    raise SystemExit(main())
