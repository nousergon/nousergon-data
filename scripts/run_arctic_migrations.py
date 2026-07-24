#!/usr/bin/env python3
"""scripts/run_arctic_migrations.py — in-region ArcticDB migration runner
(alpha-engine-config-I3242, runner half of the config-I3236 structural fix).

Invoked by the ``alpha-engine-arctic-migration-dispatcher`` Lambda's SSM
bootstrap command on a dedicated EC2-spot box, once per push to
``nousergon-data`` main that touches ``migrations/**``. The repo is already
cloned + checked out at the merged SHA by the bootstrap prelude before this
script runs — this script does no git operations of its own beyond an
informational HEAD sanity check.

Mechanism (mirrors the discovery contract migrations/README.md documents for
this runner):

    from store.arctic_store import get_universe_lib, get_schema_meta_lib
    from store.schema_version import read_schema_version, BASELINE_SCHEMA_VERSION
    from migrations import pending_migrations

    meta = get_schema_meta_lib(bucket)
    universe_lib = get_universe_lib(bucket)
    current = read_schema_version(meta) or BASELINE_SCHEMA_VERSION
    for m in pending_migrations(current):
        m.run(universe_lib, meta)     # rewrites data AND stamps (framework's
        m.verify(universe_lib)        # own contract — see migrations/_base.py)

Every migration's ``run()`` already stamps the schema-meta library as its own
LAST internal step (config-I3241's contract — see migrations/_template.py);
this runner's job is only to call ``run()`` then ``verify()`` for each
PENDING migration STRICTLY IN ORDER, one at a time, and to abort the whole
run loudly (never proceeding to the next migration) the instant either call
raises. That per-migration ordering — never touching migration N+1 while N is
unverified — is this runner's own "stamp-before-next" discipline, layered on
top of the framework's per-migration "stamp-last-inside-run()" contract.

MUTEX GOTCHA (config-I3242 issue body): a migration full-rewrites every
``universe`` symbol via ``write_batch`` — it must never interleave with a
live ``daily_append``/weekly-collector append. Rather than taking the
run-slot mutex family the append jobs use (a cross-repo DynamoDB lock this
runner has no access surface to touch safely), this runner takes the
simpler, explicitly-sanctioned alternative named in the issue: before writing
anything, confirm NONE of the three fleet trading Step Functions has a
RUNNING execution (``states:ListExecutions``, ``statusFilter=RUNNING``). If
one is mid-flight, this run REFUSES cleanly (exit 0 — a benign, not-a-failure
outcome; re-triggering is a manual operator step today, tracked as a
follow-up — see alpha-engine-config-I3242's PR body). If the mutex PROBE
itself fails (States API error), this is NOT treated as "probably clear" —
unlike the fleet's other spot dispatchers (where a broken probe degrades to
"launch anyway, coverage beats dedupe"), a migration racing a live append is
the asymmetric-risk direction: a broken probe here REFUSES to proceed and
pages loud (exit 1), because we cannot safely rule out a live append.

Exit codes (mirrors alpha-engine-config's alert_drain_run.sh/ci_watch_run.sh
on-box-script contract): 0 = the run reached a clean terminal state (all
pending migrations applied; nothing was pending; or a clean mutex-active
refusal). Non-zero = a real failure (mutex-probe failure, or a migration's
run()/verify() raised) — the stamp is left at whatever the framework's own
idempotent contract leaves it (unbumped for the failing migration and every
migration after it), so the next producer append keeps refusing cleanly
(config-I3236 invariant) until this is re-run successfully.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
# infrastructure/lambdas/flow_doctor_telegram.py is the fleet's ONE Telegram
# sink (config-I2909: raw krepis/nousergon_lib.telegram.send_message imports
# outside it are burndown-guarded, tests/test_telegram_raw_send_message_
# burndown_guard.py). Every Lambda gets it copied into its zip by deploy.sh;
# this on-box script instead imports it directly from its repo-root home
# (the full repo is cloned here, not a zip), mirroring that same chokepoint
# rather than hand-rolling a raw HTTP call.
sys.path.insert(0, str(REPO_ROOT / "infrastructure" / "lambdas"))
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="[arctic-migration-run] %(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ARCTIC_MIGRATION_ACCOUNT_ID", "711398986525")

# The three fleet trading Step Functions a migration full-rewrite must never
# interleave with (see module docstring's MUTEX GOTCHA). Same three names the
# root CLAUDE.md and saturday-sf-watch-dispatcher's PIPELINES both enumerate.
GUARDED_PIPELINES: tuple[str, ...] = (
    "ne-weekly-freshness-pipeline",
    "ne-preopen-trading-pipeline",
    "ne-postclose-trading-pipeline",
)

DIGEST_REPO = "nousergon/alpha-engine-config"


class MutexProbeError(RuntimeError):
    """The pre-migration mutex probe (states:ListExecutions across the three
    guarded pipelines) could not determine whether one is mid-flight. Unlike
    the fleet's spot dispatchers (coverage-beats-dedupe: proceed on a broken
    probe), a migration full-rewrite racing a live append is the DANGEROUS
    direction — this runner refuses to proceed on a probe failure rather than
    guessing the mutex is clear."""


class MigrationRunError(RuntimeError):
    """A pending migration's run()/verify() raised. Carries the migration
    number so the completion marker + P1 + Telegram page can name exactly
    which migration in the chain failed."""

    def __init__(self, migration_number: int, cause: BaseException) -> None:
        self.migration_number = migration_number
        self.cause = cause
        super().__init__(
            f"migration {migration_number:04d} failed: {type(cause).__name__}: {cause}"
        )


# ── Mutex probe ───────────────────────────────────────────────────────────


def _stepfunctions_client(region: str):
    import boto3

    return boto3.client("stepfunctions", region_name=region)


def running_pipeline_executions(
    region: str = DEFAULT_REGION, account_id: str = ACCOUNT_ID
) -> list[str]:
    """Return the subset of GUARDED_PIPELINES with a RUNNING execution right
    now. Raises MutexProbeError (never returns a fail-open guess) if the
    States API itself errors for any pipeline — see module docstring."""
    sfn = _stepfunctions_client(region)
    running: list[str] = []
    for name in GUARDED_PIPELINES:
        arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{name}"
        try:
            resp = sfn.list_executions(
                stateMachineArn=arn, statusFilter="RUNNING", maxResults=1
            )
        except Exception as exc:  # noqa: BLE001 — re-raised as MutexProbeError, never swallowed
            raise MutexProbeError(
                f"states:ListExecutions failed for {name!r} ({arn}): "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        if resp.get("executions"):
            running.append(name)
    return running


# ── Migration application ────────────────────────────────────────────────


def resolve_current_version(meta_lib) -> int:
    """Mirrors migrations/README.md's documented runner snippet: an absent
    stamp reads as BASELINE_SCHEMA_VERSION, never an error."""
    from store.schema_version import BASELINE_SCHEMA_VERSION, read_schema_version

    stamp = read_schema_version(meta_lib)
    return BASELINE_SCHEMA_VERSION if stamp is None else stamp


def apply_pending(pending: list[Any], universe_lib, meta_lib) -> list[int]:
    """Apply every migration in ``pending`` STRICTLY IN ORDER: run() then
    verify(), one migration at a time. Returns the list of migration numbers
    successfully applied. Raises MigrationRunError (never continues to the
    next migration) the instant either call raises for the current one — the
    framework's own idempotent run()/verify() contract makes a re-run of this
    same list safe after a fix."""
    applied: list[int] = []
    for migration in pending:
        log.info(
            "applying migration %04d (%s)...", migration.number, migration.name
        )
        try:
            migration.run(universe_lib, meta_lib)
            migration.verify(universe_lib)
        except Exception as exc:  # noqa: BLE001 — re-raised as MigrationRunError, never swallowed
            raise MigrationRunError(migration.number, exc) from exc
        log.info("migration %04d (%s) applied + verified", migration.number, migration.name)
        applied.append(migration.number)
    return applied


# ── Completion marker + notification ─────────────────────────────────────


def completion_marker_key(head_migration_number: int) -> str:
    return f"overseer/_control/completed/arctic-migration-{head_migration_number:04d}.json"


def write_completion_marker(
    *, bucket: str, region: str, head_migration_number: int, payload: dict
) -> None:
    """Best-effort S3 write of the run's terminal state.

    Deliberate swallow (no-silent-fails rule, documented per the global
    policy): (a) failure mode swallowed = the completion-marker PUT failing;
    (b) the primary deliverable — the migration having been applied/refused/
    failed — already exists independently via the schema-meta stamp itself,
    this process's exit code, and the Telegram/P1 notification below, so a
    marker-write hiccup does not lose the outcome; (c) recording surface =
    this WARNING log line (grep-able in the box's CloudWatch-mirrored SSM
    output)."""
    import boto3

    key = completion_marker_key(head_migration_number)
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    try:
        boto3.client("s3", region_name=region).put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
        log.info("completion marker written: s3://%s/%s", bucket, key)
    except Exception as exc:  # noqa: BLE001 — documented non-fatal swallow, see docstring
        log.warning(
            "completion-marker write FAILED (non-fatal): s3://%s/%s: %s: %s",
            bucket, key, type(exc).__name__, exc,
        )


def notify(
    *, outcome: str, severity: str, text: str, dedup_key: str, context: dict
) -> None:
    """Route the run's outcome through the fleet's ONE Telegram sink
    (flow_doctor_telegram.notify_via_flow_doctor — config-I2909), never a raw
    HTTP/urllib call. Falls back gracefully (inside notify_via_flow_doctor
    itself) to krepis.telegram.send_message — which never raises — when
    flow-doctor's DynamoDB dedup store isn't reachable from this box's IAM
    profile (a known gap the PR body calls out as an operator follow-up, not
    a blocker: the sink degrades, it does not go silent).

    Defensive try/except here too: a notification failure must NEVER abort
    the migration decision logic above — this call happens strictly AFTER
    the migration outcome is already final."""
    try:
        from flow_doctor_telegram import notify_via_flow_doctor
        from nousergon_lib.flow_doctor_fleet import PIPELINE_OBSERVER_TELEGRAM_TOPICS

        notify_via_flow_doctor(
            text,
            silent=(severity == "info"),
            severity=severity,
            dedup_key=dedup_key,
            flow_name="arctic-migration",
            topics=PIPELINE_OBSERVER_TELEGRAM_TOPICS,
            db_basename="arctic_migration",
            context={**context, "outcome": outcome},
        )
    except Exception as exc:  # noqa: BLE001 — notification must never mask the real outcome
        log.warning("Telegram notify FAILED (non-fatal): %s: %s", type(exc).__name__, exc)


def file_failure_issue(*, head_migration_number: int, merged_sha: str, error: str) -> None:
    """Mirrors alert_drain_run.sh's on-box P1-on-crash pattern: best-effort
    gh issue create, non-fatal if gh/GH_TOKEN is unavailable (logged, not
    raised — the Telegram page + nonzero exit code are the primary alarms)."""
    if not os.environ.get("GH_TOKEN"):
        log.warning("GH_TOKEN unset — cannot file the failure issue")
        return
    title = f"ArcticDB migration run FAILED at head {head_migration_number:04d} ({merged_sha[:12]})"
    body = (
        f"The in-region ArcticDB migration runner (alpha-engine-config-I3242, "
        f"scripts/run_arctic_migrations.py) failed applying migration chain up "
        f"to head {head_migration_number:04d}, merged_sha={merged_sha}. "
        f"Error: {error}\n\nThe schema-version stamp was left UNBUMPED for the "
        f"failing migration and everything after it — producer appends "
        f"(daily_append / weekly_collector) will keep refusing cleanly "
        f"(config-I3236 invariant) until this is fixed and re-run. Manual "
        f"triage needed; re-trigger via the arctic-migration-dispatcher Lambda "
        f"or re-run .github/workflows/run-arctic-migrations.yml (workflow_dispatch)."
    )
    try:
        subprocess.run(
            [
                "gh", "issue", "create", "--repo", DIGEST_REPO,
                "--title", title, "--label", "P1", "--body", body,
            ],
            check=True, capture_output=True, text=True, timeout=30,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort; logged, not raised
        log.warning("gh issue create FAILED (non-fatal): %s", exc)


# ── Orchestration ─────────────────────────────────────────────────────────


def _git_head_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, check=True,
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 — sanity-check only, never fatal
        return ""


def run(args: argparse.Namespace) -> int:
    from migrations import pending_migrations
    from store.arctic_store import get_schema_meta_lib, get_universe_lib

    head_sha = _git_head_sha()
    if head_sha and args.merged_sha and head_sha != args.merged_sha:
        log.warning(
            "local git HEAD (%s) != --merged-sha (%s) — the bootstrap prelude "
            "should have checked out the merged SHA; proceeding against "
            "whatever is actually checked out here", head_sha, args.merged_sha,
        )

    context = {
        "owner_repo": "nousergon/nousergon-data",
        "merged_sha": args.merged_sha,
        "head_migration_number": args.head_migration_number,
    }

    # ── 1. Mutex probe ──────────────────────────────────────────────────
    try:
        running = running_pipeline_executions(region=args.region)
    except MutexProbeError as exc:
        log.error("mutex probe FAILED — refusing to proceed: %s", exc)
        write_completion_marker(
            bucket=args.bucket, region=args.region,
            head_migration_number=args.head_migration_number,
            payload={
                "state": "refused_mutex_probe_failed", "rc": 1,
                "merged_sha": args.merged_sha, "error": str(exc),
                "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        notify(
            outcome="refused_mutex_probe_failed", severity="critical",
            text=(
                f"*ArcticDB migration run REFUSED* (head {args.head_migration_number:04d}) — "
                f"the pre-migration mutex probe (states:ListExecutions on the three "
                f"trading pipelines) FAILED: {exc}. Refusing to risk a rewrite racing a "
                f"live append. Manual triage needed."
            ),
            dedup_key=f"arctic-migration-mutex-probe-failed-{args.head_migration_number}",
            context=context,
        )
        file_failure_issue(
            head_migration_number=args.head_migration_number,
            merged_sha=args.merged_sha, error=f"mutex probe failed: {exc}",
        )
        return 1

    if running:
        log.warning(
            "mutex ACTIVE — %s has a RUNNING execution; refusing this migration "
            "run cleanly (manual re-trigger needed once it clears)", running,
        )
        write_completion_marker(
            bucket=args.bucket, region=args.region,
            head_migration_number=args.head_migration_number,
            payload={
                "state": "refused_mutex_active", "rc": 0,
                "merged_sha": args.merged_sha, "running_pipelines": running,
                "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        notify(
            outcome="refused_mutex_active", severity="warning",
            text=(
                f"*ArcticDB migration run deferred* (head {args.head_migration_number:04d}) — "
                f"{running} still running; refusing to rewrite `universe` while a live "
                f"append pipeline is mid-flight. Re-trigger manually once clear "
                f"(re-run the run-arctic-migrations workflow, workflow_dispatch)."
            ),
            dedup_key=f"arctic-migration-mutex-active-{args.head_migration_number}",
            context=context,
        )
        return 0

    if args.dry_run:
        meta_lib = get_schema_meta_lib(args.bucket)
        current = resolve_current_version(meta_lib)
        pending = pending_migrations(current)
        log.info(
            "[dry-run] current=%d pending=%s — no writes performed",
            current, [m.number for m in pending],
        )
        return 0

    # ── 2. Load current stamp + pending chain ──────────────────────────
    meta_lib = get_schema_meta_lib(args.bucket)
    universe_lib = get_universe_lib(args.bucket)
    current = resolve_current_version(meta_lib)
    pending = pending_migrations(current)

    if not pending:
        log.info("current=%d — nothing pending, nothing to do", current)
        write_completion_marker(
            bucket=args.bucket, region=args.region,
            head_migration_number=args.head_migration_number,
            payload={
                "state": "noop_up_to_date", "rc": 0, "merged_sha": args.merged_sha,
                "current_version": current, "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        notify(
            outcome="noop_up_to_date", severity="info",
            text=(
                f"ArcticDB migration run: nothing pending at head "
                f"{args.head_migration_number:04d} (already at v{current})."
            ),
            dedup_key=f"arctic-migration-noop-{args.head_migration_number}",
            context=context,
        )
        return 0

    # ── 3. Apply, strictly in order, abort loud on first failure ───────
    try:
        applied = apply_pending(pending, universe_lib, meta_lib)
    except MigrationRunError as exc:
        log.error("MIGRATION FAILED: %s", exc)
        write_completion_marker(
            bucket=args.bucket, region=args.region,
            head_migration_number=args.head_migration_number,
            payload={
                "state": "failure", "rc": 1, "merged_sha": args.merged_sha,
                "failed_migration_number": exc.migration_number,
                "error": str(exc.cause), "at": datetime.now(timezone.utc).isoformat(),
            },
        )
        notify(
            outcome="failure", severity="critical",
            text=(
                f"*ArcticDB migration run FAILED* at migration "
                f"{exc.migration_number:04d} (head {args.head_migration_number:04d}): "
                f"{exc.cause}. Stamp left UNBUMPED — producer appends will keep "
                f"refusing cleanly. Manual triage needed."
            ),
            dedup_key=f"arctic-migration-failure-{args.head_migration_number}",
            context=context,
        )
        file_failure_issue(
            head_migration_number=args.head_migration_number,
            merged_sha=args.merged_sha, error=str(exc),
        )
        return 1

    log.info("SUCCESS — applied migrations %s", applied)
    write_completion_marker(
        bucket=args.bucket, region=args.region,
        head_migration_number=args.head_migration_number,
        payload={
            "state": "success", "rc": 0, "merged_sha": args.merged_sha,
            "applied_migrations": applied, "at": datetime.now(timezone.utc).isoformat(),
        },
    )
    notify(
        outcome="success", severity="info",
        text=(
            f"ArcticDB migration run succeeded — applied {applied} "
            f"(head {args.head_migration_number:04d}, sha {args.merged_sha[:12]})."
        ),
        dedup_key=f"arctic-migration-success-{args.head_migration_number}",
        context=context,
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--merged-sha", required=True, help="the merge commit this box was cloned at")
    p.add_argument(
        "--head-migration-number", required=True, type=int,
        help="highest migrations/NNNN_*.py number present at --merged-sha",
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--region", default=DEFAULT_REGION)
    p.add_argument(
        "--dry-run", action="store_true",
        help="report current/pending state only; no writes, no stamp",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
