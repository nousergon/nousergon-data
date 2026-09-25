"""`python -m shadow run …`, `python -m shadow parity …`,
`python -m shadow arctic-parity …`, `python -m shadow recompute-lineage …` and
`python -m shadow prune …`.

Five commands. `recompute-lineage` (`alpha-engine-config-I11203`) re-runs D31
over each side's recorded inputs and publishes the record `parity`'s v1_cause
grading of a derived feature key reads; it opens ArcticDB, so it runs on the
box too. `run` and `parity` are the two halves of plan §6.2 step 4;
`arctic-parity` (`alpha-engine-config-I10819`) is the in-region follow-up that
fills the `in_region_only` rows `parity` cannot measure from the laptop.
**Run it on the data-spot box, never the laptop** — it opens ArcticDB
directly, which is unreachable from here (`alpha-engine-config-I9771`).
`prune` (`alpha-engine-config-I11447`) removes shadow ArcticDB libraries past
the retention window; it opens ArcticDB too, so the same box rule applies, and
it is a dry run unless `--apply` is passed.

``run`` is the output-root override. It activates the shadow root BEFORE the
target module is imported — which is the whole reason it exists as a wrapper
rather than a flag inside each collector. A flag would have to be threaded
through every entrypoint, and any entrypoint that forgot it would write live
keys; activating first means the redirect is in place before a single
collector line runs::

    python -m shadow run --trading-day 2026-09-12 --module weekly_collector -- --mode eod

``parity`` is the diff, and its exit codes are the same contract
``data_gate.__main__`` declares, for the same reason: "the keys do not match"
and "we could not compare them" are different facts.

* **0** — the comparison ran AND every key matched.
* **1** — the comparison ran and parity is NOT met. The report is still
  published; this is the code that stops CI or a person reading "not there
  yet" as "done".
* **2** — the comparison itself failed. Nothing is published.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import runpy
import subprocess
import sys

from data_gate.store import open_store
from shadow import parity as parity_module
from shadow.root import ShadowRoot, activate, deactivate

EXIT_MET = 0
EXIT_NOT_MET = 1
EXIT_UNMEASURED = 2

DEFAULT_BUCKET = "alpha-engine-research"


def _code_sha() -> str:
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - provenance is best-effort; "unknown" is recorded as such
        # Swallowed deliberately: (a) the failure mode is "this checkout is not a
        # git working tree"; (b) the report is still produced and still correct —
        # only its provenance field degrades; (c) it is recorded on the surface
        # that matters, as `code_sha: "unknown"` inside the published report.
        return "unknown"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="shadow", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    runner = sub.add_parser("run", help="run a collector entrypoint under the shadow output root")
    runner.add_argument("--trading-day", required=True, help="ISO date; the shadow root's date")
    runner.add_argument("--module", required=True, help="module to run as __main__, e.g. weekly_collector")
    runner.add_argument("argv", nargs=argparse.REMAINDER, help="arguments passed to the module")

    diff = sub.add_parser("parity", help="diff the shadow output against the same day's v1 output")
    diff.add_argument("--trading-day", required=True)
    diff.add_argument("--bucket", default=DEFAULT_BUCKET)
    diff.add_argument(
        "--store",
        required=True,
        help="where the report is published: s3://alpha-engine-research/data_collection, or a directory",
    )
    diff.add_argument("--relative-tolerance", type=float, default=parity_module.DEFAULT_RELATIVE_TOLERANCE)
    diff.add_argument("--absolute-tolerance", type=float, default=parity_module.DEFAULT_ABSOLUTE_TOLERANCE)
    diff.add_argument("--max-keys-per-prefix", type=int, default=50)
    diff.add_argument("--dry-run", action="store_true", help="compare and render, publish nothing")
    diff.add_argument(
        "--dispatch-gate",
        action="store_true",
        help=(
            "After publishing, workflow_dispatch data-gate.yml for this trading day with "
            "trigger=parity-published (alpha-engine-config-I11361). Set only by the "
            "scheduled same-day and morning dispatches, so a manual or replay run never "
            "moves the board. Never fatal: the outcome is recorded in the report as "
            "`gate_dispatch` and the post-publish crons remain the backstop."
        ),
    )
    diff.add_argument(
        "--legs-file",
        default=None,
        help=(
            "JSON file recording what the PRODUCER legs did: a list of "
            '{\"name\": str, \"exit_code\": int} objects. The dispatcher writes it after '
            "running the legs independently, so the report can say which legs ran rather "
            "than leaving a reader to assume all of them did (alpha-engine-config-I11200). "
            "Absent, the report records `legs_known: {sameday: false, morning: false}` -- "
            "which is NOT the same claim as every leg having succeeded."
        ),
    )
    diff.add_argument(
        "--legs-group",
        choices=list(parity_module.LEGS_GROUPS),
        default=parity_module.DEFAULT_LEGS_GROUP,
        help=(
            "Which DISPATCH these legs came from (alpha-engine-config-I11352). The same "
            "trading day's report is written twice: once by `shadow-sameday` at 18:30 ET on "
            "D with the post-market legs, and once by `shadow-morning` at 07:45 ET on D+1 "
            "with v1's two morning legs, whose keys do not exist until then. The second run "
            "MERGES -- it rewrites this group's leg entries and its own comparison, and "
            "keeps the other group's entries -- so `legs_known` reads per group and a "
            "missing morning dispatch renders as `morning: false` rather than as silence. "
            f"Default {parity_module.DEFAULT_LEGS_GROUP!r}."
        ),
    )

    arctic = sub.add_parser(
        "arctic-parity",
        help="in-region only: fill the ArcticDB in_region_only rows of an already-published parity report",
    )
    arctic.add_argument("--trading-day", required=True)
    arctic.add_argument("--bucket", default=DEFAULT_BUCKET)
    arctic.add_argument(
        "--store",
        required=True,
        help="where the report was published: s3://alpha-engine-research/data_collection, or a directory",
    )
    arctic.add_argument("--relative-tolerance", type=float, default=parity_module.DEFAULT_RELATIVE_TOLERANCE)
    arctic.add_argument("--absolute-tolerance", type=float, default=parity_module.DEFAULT_ABSOLUTE_TOLERANCE)
    arctic.add_argument(
        "--await-live-unit",
        action="append",
        default=[],
        metavar="UNIT",
        help=(
            "Before comparing, wait until v1's run manifest for this unit on the trading day "
            "reads status ok (repeatable). ArcticDB has no manifest-pinned version to grade "
            "against, so comparing while v1 is still appending the day grades a half-written "
            "library (alpha-engine-config-I11546). Refuses with exit 2, report untouched, when "
            "--await-timeout-seconds passes first."
        ),
    )
    arctic.add_argument("--await-timeout-seconds", type=float, default=0.0)
    arctic.add_argument("--await-poll-seconds", type=float, default=60.0)
    arctic.add_argument(
        "--dispatch-gate",
        action="store_true",
        help=(
            "After this run, workflow_dispatch data-gate.yml for the trading day with "
            "trigger=parity-published, exactly as `parity --dispatch-gate` does "
            "(alpha-engine-config-I11361). Set only by the scheduled morning dispatch, which "
            "runs this AFTER `parity`, so the gate reads a report that already carries the "
            "ArcticDB rows (alpha-engine-config-I11546). Dispatches on a failed or refused "
            "comparison too, whenever a published report exists: the report parity wrote is "
            "still the newest evidence, and the gate read following it is what the flag is for."
        ),
    )

    lineage = sub.add_parser(
        "recompute-lineage",
        help=(
            "in-region only: re-run D31 over each side's recorded inputs for a trading day and "
            "publish the recompute lineage record parity's v1_cause grading reads"
        ),
    )
    lineage.add_argument("--trading-day", required=True)
    lineage.add_argument("--bucket", default=DEFAULT_BUCKET)
    lineage.add_argument(
        "--store",
        required=True,
        help="the parity store: s3://alpha-engine-research/data_collection, or a directory",
    )
    lineage.add_argument("--dry-run", action="store_true", help="compute and print; publish nothing")

    prune = sub.add_parser(
        "prune",
        help="delete shadow ArcticDB libraries older than the retention window (dry run unless --apply)",
    )
    prune.add_argument("--bucket", default=DEFAULT_BUCKET)
    prune.add_argument("--keep-days", type=int, default=7)
    prune.add_argument("--today", default=None, help="ISO date; defaults to today (UTC)")
    prune.add_argument(
        "--apply",
        action="store_true",
        help="actually delete. Without it the command only reports (alpha-engine-config-I11447)",
    )
    return parser


def _prune(args) -> int:
    from shadow.retention import prune
    from store.arctic_store import _get_arctic

    today = dt.date.fromisoformat(args.today) if args.today else dt.datetime.now(dt.timezone.utc).date()
    report = prune(_get_arctic(args.bucket), today=today, keep_days=args.keep_days, apply=args.apply)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _recompute_lineage(args) -> int:
    """`alpha-engine-config-I11203`: exit 0 when a record was published (complete OR
    refused — a refusal is a finding the record carries), 2 when none could be."""
    import boto3

    from shadow import recompute_lineage

    trading_day = dt.date.fromisoformat(args.trading_day)
    try:
        record = recompute_lineage.evaluate_day(
            trading_day, bucket=args.bucket, client=boto3.client("s3"), code_sha=_code_sha()
        )
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        print(f"shadow recompute-lineage: failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_UNMEASURED
    payload = json.dumps(record, indent=2, sort_keys=True).encode("utf-8")
    key = recompute_lineage.lineage_record_key(trading_day)
    if not args.dry_run:
        open_store(args.store, dry_run=False).put_bytes(key, payload)
    summary = {
        "status": record["status"],
        "published": None if args.dry_run else key,
        "sides": {side: facts.get("refusal") for side, facts in record["sides"].items()},
        "reproduced": {
            k: {side: v[side]["reproduced"] for side in ("v1", "shadow")} for k, v in record["keys"].items()
        },
        "differing_inputs": [item.get("key") for item in record["differing_inputs"]],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _run(args) -> int:
    root = ShadowRoot(dt.date.fromisoformat(args.trading_day))
    activate(root)
    argv = [a for a in args.argv if a != "--"]
    sys.argv = [args.module, *argv]
    print(
        f"shadow: output root active — every S3 write lands under {root.prefix!r} and every "
        f"ArcticDB library under {root.arctic_prefix!r}; running {args.module}",
        file=sys.stderr,
    )
    try:
        runpy.run_module(args.module, run_name="__main__", alter_sys=True)
    except SystemExit as exc:
        return int(exc.code or 0)
    finally:
        deactivate()
    return 0


def _read_legs(path: "str | None") -> list[dict]:
    """The producer legs' outcomes, as the dispatcher recorded them.

    `alpha-engine-config-I11200`. Raises rather than degrading: a legs file
    the caller NAMED and that cannot be read is a broken contract, and
    returning `[]` would render as `legs_known: false` -- indistinguishable
    from "the comparator was run on its own", which is a different fact. The
    whole point of this block is to stop a reader assuming every leg ran.
    """
    if not path:
        return []
    text = open(path, encoding="utf-8").read().strip()
    if not text:
        raise ValueError(
            f"--legs-file {path!r} is empty. A named-but-empty legs file would publish "
            "`legs_known: false`, which claims the comparator was not told what the legs "
            "did -- a different fact from 'no leg ran' (alpha-engine-config-I11200)."
        )
    if text.lstrip().startswith("["):
        legs = json.loads(text)
        if not isinstance(legs, list) or not all(isinstance(leg, dict) for leg in legs):
            raise ValueError(f"--legs-file {path!r}: JSON form must be a list of objects")
    else:
        # `name<TAB>exit_code` per line. The dispatcher writes this form
        # because its workload string is `.format()`ed and a literal `{` would
        # need doubling through two layers of quoting for no gain.
        legs = []
        for lineno, line in enumerate(text.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                raise ValueError(
                    f"--legs-file {path!r} line {lineno}: expected 'name<TAB>exit_code', got {line!r}"
                )
            legs.append({"name": parts[0], "exit_code": int(parts[1])})
    for leg in legs:
        if "name" not in leg or "exit_code" not in leg:
            raise ValueError(
                f"--legs-file {path!r}: every entry needs 'name' and 'exit_code', got {leg!r}"
            )
        leg["ok"] = int(leg["exit_code"]) == 0
    return legs


def _previous_legs(store, key: str) -> list[dict]:
    """The `legs` block of the report already published for this trading day.

    An absent or unparseable prior report yields `[]` — the deliberate,
    narrow swallow here is (a) "no earlier dispatch wrote this day's report",
    which is the ordinary first-write case and not an error; (b) the report
    being produced is unaffected; (c) it is recorded in the published document
    itself, as `legs_known.<group>: false` for every group that did not write.
    """
    try:
        document = json.loads(store.get_bytes(key).decode("utf-8"))
    except Exception:  # noqa: BLE001 - see the docstring's (a)/(b)/(c)
        return []
    legs = document.get("legs")
    return [leg for leg in legs if isinstance(leg, dict)] if isinstance(legs, list) else []


def _parity(args) -> int:
    trading_day = dt.date.fromisoformat(args.trading_day)
    group = getattr(args, "legs_group", parity_module.DEFAULT_LEGS_GROUP)
    legs = _read_legs(getattr(args, "legs_file", None))
    store = open_store(args.store, dry_run=args.dry_run)
    key = parity_module.parity_key(trading_day)
    # `alpha-engine-config-I11352`: MERGE, never overwrite. The comparison
    # itself is rewritten wholesale (that is what the morning dispatch is for
    # — the morning legs' keys did not exist at 18:30 ET), but the OTHER
    # dispatch's leg outcomes survive.
    legs, legs_known = parity_module.merge_legs(
        _previous_legs(store, key), legs, group=group
    )
    try:
        report = parity_module.run_parity(
            trading_day=trading_day,
            bucket=args.bucket,
            code_sha=_code_sha(),
            rel_tolerance=args.relative_tolerance,
            absolute_tolerance=args.absolute_tolerance,
            max_keys_per_prefix=args.max_keys_per_prefix,
            legs=legs,
            legs_known=legs_known,
            store=store,
        )
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        print(f"shadow parity: the comparison failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_UNMEASURED
    document = report.as_dict()
    payload = json.dumps(document, indent=2, sort_keys=True).encode("utf-8")
    if not args.dry_run:
        store.put_bytes(key, payload)
        if getattr(args, "dispatch_gate", False):
            _dispatch_gate(store, key, document, trading_day)
    print(_render(report, key, dry_run=args.dry_run))
    return EXIT_MET if report.met else EXIT_NOT_MET


def _dispatch_gate(
    store, key: str, document: dict, trading_day: dt.date, *, label: str = "shadow parity"
) -> None:
    """`alpha-engine-config-I11361`: trigger the gate read off the publish.

    Runs only AFTER the report is published, so the read it triggers can never
    find an absent report. The outcome is then written into the same report as
    `gate_dispatch`. That second put changes no comparison field, so a gate read
    that somehow landed between the two puts grades the same keys either way.
    """
    from shadow.gate_dispatch import dispatch_gate_read

    outcome = dispatch_gate_read(trading_day)
    if outcome["ok"]:
        print(f"{label}: dispatched data-gate.yml for {trading_day} (trigger parity-published)")
    else:
        print(
            f"{label}: gate dispatch FAILED for {trading_day}: {outcome['error']} "
            "(recorded in the report; the post-publish crons remain the backstop)",
            file=sys.stderr,
        )
    document["gate_dispatch"] = outcome
    store.put_bytes(key, json.dumps(document, indent=2, sort_keys=True).encode("utf-8"))


def _render(report, key: str, *, dry_run: bool) -> str:
    summary = report.summary
    lines = [
        f"parity {report.trading_day} — {'MET' if report.met else 'NOT MET'}",
        f"  shadow root : {report.shadow_prefix}",
        f"  published   : {'(dry run, nothing written)' if dry_run else key}",
        "  " + ", ".join(f"{name}={count}" for name, count in sorted(summary.items()) if count),
    ]
    for row in report.rows:
        if row.verdict != "match":
            detail = row.body.get("unmeasurable_reason") or row.body.get("detail") or ""
            lines.append(f"  [{row.verdict}] {row.key} ({','.join(row.unit_ids)}) {detail}"[:200])
    return "\n".join(lines)


def _arctic_parity(args) -> int:
    from shadow import arctic_parity

    trading_day = dt.date.fromisoformat(args.trading_day)
    store = open_store(args.store, dry_run=False)
    key = parity_module.parity_key(trading_day)
    updated = None
    try:
        awaited = getattr(args, "await_live_unit", None) or []
        if awaited:
            arctic_parity.await_live_units(
                store,
                trading_day,
                awaited,
                timeout_seconds=args.await_timeout_seconds,
                poll_seconds=args.await_poll_seconds,
            )
        updated = arctic_parity.run_arctic_parity(
            trading_day=trading_day,
            bucket=args.bucket,
            store=store,
            rel_tolerance=args.relative_tolerance,
            absolute_tolerance=args.absolute_tolerance,
        )
    except arctic_parity.LiveUnitNotReady as exc:
        print(f"shadow arctic-parity: not compared: {exc}", file=sys.stderr)
        rc = EXIT_UNMEASURED
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        print(f"shadow arctic-parity: the comparison failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        rc = EXIT_UNMEASURED
    else:
        print(json.dumps(updated["summary"], indent=2, sort_keys=True))
        rc = EXIT_MET if updated["met"] else EXIT_NOT_MET
    if getattr(args, "dispatch_gate", False):
        document = updated if updated is not None else _published_report(store, key)
        if document is not None:
            _dispatch_gate(store, key, document, trading_day, label="shadow arctic-parity")
    return rc


def _published_report(store, key: str) -> "dict | None":
    """The report as `parity` published it, for the gate dispatch after a
    comparison that did not rewrite it; `None` when there is none to read."""
    try:
        document = json.loads(store.get_bytes(key).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - reported on stderr; the run already exits 2
        # Deliberate: (a) the failure mode is "no readable report at `key`",
        # i.e. there is nothing for a gate read to follow; (b) the run's exit
        # code is already EXIT_UNMEASURED from the comparison, and the two
        # post-publish crons in data-gate.yml read the store on their own; (c)
        # it is printed to stderr, which the run log ships off the box.
        print(
            f"shadow arctic-parity: no gate dispatch — could not read {key}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return None
    return document if isinstance(document, dict) else None


def main(argv: "list[str] | None" = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "run":
        return _run(args)
    if args.command == "arctic-parity":
        return _arctic_parity(args)
    if args.command == "prune":
        return _prune(args)
    if args.command == "recompute-lineage":
        return _recompute_lineage(args)
    return _parity(args)


if __name__ == "__main__":
    raise SystemExit(main())
