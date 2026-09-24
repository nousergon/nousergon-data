"""`python -m data_gate read --gate data-phase<N> --store <uri> [--dry-run]`
and `python -m data_gate report --store <uri> [--dry-run]`.

Exit codes are the contract, not a detail:

* **0** — the measurement succeeded. This is the default whatever the verdict —
  MET or UNMET — because the verdict is not a process failure: it already has a
  durable surface (the ladder, the board, the dated gate history) and a page
  path (`data.ladder_fresh`). A scheduled reader that fails the job on every
  UNMET reading during a red-by-design phase makes a genuine reader break
  (auth, import, a bad write) indistinguishable from the expected finding —
  see `alpha-engine-config-I10906`.
* **1** — the measurement succeeded and the gate is NOT met, but ONLY when
  `--fail-on-unmet` is passed. That flag is for a human invocation —
  `workflow_dispatch` or a PR-time check — that wants the process to stop CI
  or a review on "not there yet". A cron-triggered reading must never pass it.
* **2** — the measurement itself failed (the clause list would not build, the
  descriptors would not validate). Distinct from 1 on purpose: "the gate says
  no" and "we could not ask" are different facts, and a single non-zero code
  would let a broken grader look exactly like a failing system. Never gated
  behind `--fail-on-unmet` — a reader failure always exits non-zero.

`report` carries the SAME contract with the same two codes, and deliberately no
third: **0** when the daily update was rendered, filed and delivered, whatever
the ladder said, and **2** when it could not be. The gate VERDICT is never
encoded in the exit code - that was `alpha-engine-config-I10906`, and a report
that exited non-zero on an UNMET ladder would make `scheduled-workflow-health`
grade this workflow `failing` for as long as a phase is red by design, hiding a
genuine reporter break inside the expected finding. `--fail-on-unmet` does not
exist on `report` at all: there is no reading for it to fail on, because this
subcommand takes no reading of its own - it quotes one.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

from data_gate import read as read_module
from data_gate.cadence import latest_trading_day_on_or_before
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
        help=(
            "ISO date the reading is filed under, or one of the two literals below; "
            "defaults to `today`. `today` = the latest trading day on or before today "
            "UTC. `yesterday` = the latest trading day on or before YESTERDAY UTC, which "
            "is the session a read taken after midnight UTC is actually about "
            "(alpha-engine-config-I11355): the same-day parity for session D publishes at "
            "~23:43 UTC on D and the morning dispatch rewrites it at 11:45 UTC on D+1, so "
            "every read that follows a publish runs on the NEXT UTC calendar day and must "
            "not file itself under that day. It is a literal rather than arithmetic in the "
            "workflow because a Saturday 01:30 UTC read is about FRIDAY, and "
            "`previous_trading_day(latest_on_or_before(Saturday))` returns Thursday."
        ),
    )
    reader.add_argument(
        "--trigger",
        default="manual",
        choices=sorted(read_module.TRIGGERS),
        help=(
            "What caused this read, recorded on the dated `gates/<gate>/{date}/gate.json` "
            "(alpha-engine-config-I11355 deliverable 2). Without it a reading taken before "
            "the day's parity published is indistinguishable from one taken after, so a "
            "day whose post-publish read never ran reads as an ordinary reading rather "
            "than as a gap."
        ),
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
    reporter = sub.add_parser(
        "report", help="deliver the daily update from what the gate reader published"
    )
    reporter.add_argument(
        "--store",
        required=True,
        help="s3://alpha-engine-research/data_collection, or a local directory",
    )
    reporter.add_argument(
        "--trading-day",
        default=None,
        help=(
            "ISO trading day the update is filed under; defaults to the last weekday "
            "strictly before the calendar date, so Sat/Sun/Mon all resolve to Friday"
        ),
    )
    reporter.add_argument(
        "--calendar-date",
        default=None,
        help=(
            "ISO calendar date, the second key segment and the discriminator; defaults "
            "to today UTC. Without it the three calendar days that share one trading "
            "day would overwrite each other at one key."
        ),
    )
    reporter.add_argument(
        "--console-url",
        default=None,
        help=(
            "base URL of the fleet console; the board link is its Decision list "
            "filtered to this board. Defaults to $DATA_CONSOLE_URL, and the report "
            "says so when neither is set rather than linking nothing silently."
        ),
    )
    reporter.add_argument(
        "--dry-run",
        action="store_true",
        help="render and deliver nothing, write nothing at all",
    )
    reader.add_argument(
        "--fail-on-unmet",
        action="store_true",
        help=(
            "exit 1 when the gate is measured but not MET. Opt-in, for a human "
            "workflow_dispatch or PR-time invocation that wants the exit code to "
            "carry the verdict. A scheduled/cron invocation must NOT pass this — "
            "the verdict already has a durable surface (ladder/board/history); "
            "coupling the job's own success to it makes a real reader break "
            "indistinguishable from the expected red-by-design finding "
            "(alpha-engine-config-I10906)."
        ),
    )
    return parser


def resolve_trading_day(value: "str | None", *, today: dt.date) -> dt.date:
    """The trading day a reading is filed under, from the flag and today's date.

    `alpha-engine-config-I11355`. Two literals plus an explicit ISO date; the
    literals exist so the calendar rule lives in Python with a test on it
    rather than in a shell expression inside a workflow, where a Saturday
    01:30 UTC read (about FRIDAY) is exactly the case a naive
    `previous_trading_day` gets wrong.
    """
    if not value or value == "today":
        return latest_trading_day_on_or_before(today)
    if value == "yesterday":
        return latest_trading_day_on_or_before(today - dt.timedelta(days=1))
    return dt.date.fromisoformat(value)


def _read_command(args) -> int:
    trading_day = resolve_trading_day(
        args.trading_day, today=dt.datetime.now(dt.timezone.utc).date()
    )
    store = open_store(
        args.store,
        dry_run=args.dry_run,
        artifact_registry=args.artifact_registry,
        github_token=os.environ.get(GITHUB_TOKEN_ENV) or None,
    )
    try:
        result, _ladder, board = read_module.run(
            store,
            gate=args.gate,
            trading_day=trading_day,
            dry_run=args.dry_run,
            trigger=getattr(args, "trigger", "manual"),
        )
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        # Deliberate, and narrow in effect: the failure mode is "the measurement
        # could not be taken at all"; nothing is published, so no reader can
        # mistake this for a reading; and the recording surface is stderr plus
        # exit code 2, which is distinct from the gate saying no.
        print(f"data_gate: the measurement failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_UNMEASURED
    print(read_module.render(result, board, dry_run=args.dry_run))
    if result.met:
        return EXIT_MET
    return EXIT_NOT_MET if args.fail_on_unmet else EXIT_MET


def _report_command(args) -> int:
    """Deliver the daily update. Two outcomes, never three.

    0 when the update was rendered, filed and delivered; 2 when it could not
    be. Nothing the LADDER says can change this exit code: the report quotes a
    reading, it does not take one, so there is no verdict here to encode. The
    import is local so `python -m data_gate read` — the scheduled reader, which
    runs under a role holding no Telegram or tracker credential — never pays
    for, or fails on, the reporting module's dependencies.
    """
    from data_gate import report as report_module  # noqa: PLC0415 - see docstring

    calendar_date = (
        dt.date.fromisoformat(args.calendar_date)
        if args.calendar_date
        else dt.datetime.now(dt.timezone.utc).date()
    )
    try:
        trading_day = (
            dt.date.fromisoformat(args.trading_day)
            if args.trading_day
            else report_module.previous_trading_day(calendar_date)
        )
    except report_module.TradingCalendarRangeError as exc:
        # The shared NYSE calendar refuses a date outside its declared coverage
        # rather than guessing (alpha-engine-config-I11193). No weekday
        # fallback here: that guess is the defect this call site used to carry.
        # Nothing was read or filed, so this is the same exit as an undelivered
        # report, with the calendar's own reason on stderr.
        print(
            f"data_gate: the report's trading day could not be resolved: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return EXIT_UNMEASURED
    store = open_store(args.store, dry_run=args.dry_run)
    try:
        manifest = report_module.run_report(
            store,
            trading_day=trading_day,
            calendar_date=calendar_date,
            console_url=args.console_url or os.environ.get("DATA_CONSOLE_URL") or None,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 - classified into exit 2, never swallowed
        # FAILURE MODE SWALLOWED: every way the report can fail to go out - an
        # unreadable ladder, a tracker that refused the comment, a Telegram
        # publish that did not land. THE PRIMARY DELIVERABLE DOES NOT SURVIVE
        # any of them, which is why this is a non-zero exit and not a degraded
        # success. RECORDING SURFACE: `run.json` carries `status: failed` and
        # the reason (written by `run_report` before it re-raises), stderr
        # carries the type and message, and the job's own conclusion is red.
        print(f"data_gate: the report was not delivered: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_UNMEASURED
    print(
        f"data_gate report: {manifest['status']} - trading day {manifest['trading_day']} "
        f"(calendar {manifest['calendar_date']}, trigger {manifest['trigger']})"
    )
    if manifest.get("update_url"):
        print(f"full update: {manifest['update_url']}")
    return EXIT_MET


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "report":
        return _report_command(args)
    return _read_command(args)


if __name__ == "__main__":
    raise SystemExit(main())
