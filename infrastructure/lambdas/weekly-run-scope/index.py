"""alpha-engine-weekly-run-scope — the weekly pipeline's own scope, derived.

Tracked as ``alpha-engine-config-I7620``.

**What it answers.** Which stages did THIS run actually dispatch, and which did
an operator flag switch off? Nothing in the fleet has ever recorded that. The
Director grades the week's numbers without knowing which producers were
disabled, so on 2026-08-14 it reported the deliberate absence of ``pit_parity``
(``skip_parity: true``, set on the Saturday EventBridge target on 2026-08-13 by
a recorded ruling) as ``contamination: UNKNOWN — the producer never ran this
cycle``, and withheld ``issue_filing`` and ``loop_verification`` on the strength
of it. A deliberately-off stage and a dead stage were indistinguishable on the
page.

**Why it derives instead of reading a registry.** ``private-docs/`` already
carries thirteen registries. Every one exists because its fact had no
machine-readable home; a fourteenth listing enabled stages would be a COPY of a
fact that has two authoritative homes already — the state-machine definition
(what stages exist, what flag gates each) and the execution history (which
branch every gate took). A hand-kept copy drifts the first time somebody adds a
stage, which is the failure mode the other thirteen were built to prevent. So:
one flag flip in the CFN preset changes the pipeline, this artifact, and the
Director's purview together, because all three read the same two sources.

**Where it runs.** As the SF state ``RunScope``, immediately before
``ReportCard``, so every work stage upstream of it is already in the history it
reads. ``ReportCard`` carries this scope in-band, which is why it cannot move
later. The four gated stages after it (``ReportCard``, ``Director``,
``ScannerLeaderboard``, ``AggregateCosts``) cannot be in the history yet. They
used to be reported ``NOT_REACHED``, which is false on every run that reaches
them (alpha-engine-config-I11502); they are now listed under ``after_scope``
and left out of the graded denominator, and the statement says so. Each row
records which source decided it (``source``).

**One key per cycle, and more than one execution writes it.**
``backtest/{run_date}/run_scope.json`` is written by the scheduled Saturday run
and again by every recovery rerun launched against the same cycle — and a rerun
is launched to redo ONE stage, so it carries a skip-set for everything else.
Last-writer-wins therefore hands the cycle's scope to its worst-informed
author: both artifacts on S3 on 2026-08-31 were written by skip-flagged reruns
(``watch-rerun-2026-08-22-3``; ``watch-rerun-2026-08-28-13``, written
2026-08-30T18:47Z, a day and a half after the scheduled run wrote that cycle's
attestation) and both claim ``Backtester: DISABLED`` for cycles whose
backtester artifacts exist. ``crucible-evaluator-PR289``
(``alpha-engine-config-I8811``) stopped the consumer acting on that
contradiction; :func:`_persist` here stops it being written. The rule is that a
cycle's scope only ever ACCUMULATES — see ``run_scope.merge_run_scopes``.

**Failure posture — deliberately fail-open, and only here.** A scope block that
could not be built must not kill a run that produced real trading artifacts, so
the handler catches, publishes nothing, and returns a block whose every stage is
``NOT_REACHED``. That degradation is SAFE because ``NOT_REACHED`` is never
graded and never reads as disabled: the consumer's denominator collapses to
zero and the report card says so out loud, rather than rendering a narrow green
page. The one thing this must never do is emit a scope that looks complete.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from krepis.dates import resolve_trading_day

from run_scope import build_run_scope, merge_run_scopes, stamp_provenance

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ.get("RESEARCH_BUCKET", "alpha-engine-research")
KEY_TEMPLATE = "backtest/{run_date}/run_scope.json"


class EmptyRunDateError(ValueError):
    """Raised when the SF handed no ``run_date`` at all.

    ``krepis.dates.resolve_trading_day`` is deliberately defensive — on a parse
    failure it logs a WARNING and returns the input unchanged rather than
    raising, which is right for its other callers but wrong here: an empty
    string would sail through unchanged and produce ``backtest//run_scope.json``,
    a key nothing has ever read from and nothing ever will. This module's own
    ``handler`` already has a fail-open Catch (see the module docstring) that
    routes any raised exception to a degraded, gradeless scope block — so
    raising loudly here is free: it cannot fail the weekly run, and it turns a
    silently-misplaced artifact into an honestly-absent one.
    """


#: Every ``skip_*`` key the SF understands is threaded in by the caller. Read
#: only to EXPLAIN a NOT_REACHED row, never to decide a disposition — the
#: execution record decides, the input merely says what was asked for.
_FLAG_PREFIX = "skip_"


def _history(client, execution_arn: str) -> list[dict]:
    """The execution's own event history, paginated.

    ``includeExecutionData=False``: the derivation reads state names and the
    event chain only, and the payloads are large enough to matter at 1000+
    events.
    """
    events: list[dict] = []
    paginator = client.get_paginator("get_execution_history")
    for page in paginator.paginate(
        executionArn=execution_arn,
        includeExecutionData=False,
        reverseOrder=False,
    ):
        events.extend(page.get("events", []))
    return events


def _definition(client, state_machine_arn: str) -> dict:
    described = client.describe_state_machine(stateMachineArn=state_machine_arn)
    return json.loads(described["definition"])


class ScopeWriteConflictError(RuntimeError):
    """Raised when the artifact changed under us on every attempt.

    Loud on purpose. The SF's Catch on ``RunScope`` routes it to
    ``CheckSkipReportCard`` without failing the weekly run, and the cycle keeps
    whichever scope is already on S3 — the safe direction, since the thing this
    module must never do is destroy an established claim.
    """


#: Read-merge-write attempts before giving up. Concurrent RunScope executions
#: over one ``run_date`` are rare (a rerun launched while the scheduled run is
#: still going); three attempts turn that from a lost write into a retried one.
_WRITE_ATTEMPTS = 3


def _error_code(exc: Exception) -> str:
    """The S3 error code off a botocore ``ClientError``, duck-typed.

    Duck-typed rather than ``except ClientError`` so this module keeps its
    single runtime dependency surface and so the tests can drive every branch
    with a plain exception carrying a ``response``.
    """
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        if isinstance(code, str):
            return code
    return type(exc).__name__


def _read_incumbent(s3, key: str) -> tuple[dict | None, str | None, bool, str]:
    """The cycle's existing scope artifact, its ETag, and whether we KNOW.

    Returns ``(body, etag, read_ok, note)``. ``read_ok`` is the load-bearing
    value: True means the incumbent's presence or absence is ESTABLISHED, False
    means the read itself failed and this execution knows nothing about what is
    already there. The two are handled differently by :func:`_persist` — an
    unknown incumbent is never overwritten.

    A 404 is a successful read: the object is established absent. That branch
    is only reachable because ``iam-policy.json`` also grants a
    ``backtest/*``-scoped ``s3:ListBucket`` — without it S3 answers 403
    AccessDenied for a key that does not exist (config#2878), and an absent
    artifact would be indistinguishable from an unreadable one. Both are safe
    here, but only one of them can merge.
    """
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)
        parsed = json.loads(body["Body"].read())
        etag = body.get("ETag")
        if not isinstance(parsed, dict):
            return None, etag, True, "incumbent body is not a JSON object"
        return parsed, etag, True, "incumbent read"
    except Exception as exc:  # noqa: BLE001
        code = _error_code(exc)
        if code in ("NoSuchKey", "404", "NoSuchBucket"):
            return None, None, True, "no incumbent artifact for this cycle"
        # Every other failure — AccessDenied while the new grant lags the code
        # deploy, a throttle, a truncated body. NOT fatal, and NOT treated as
        # absence: `_persist` degrades to a create-only write, which cannot
        # clobber anything.
        logger.warning(
            "could not read the incumbent run scope at s3://%s/%s (%s) — "
            "falling back to a create-only write, which refuses to overwrite "
            "an existing artifact rather than clobbering one it cannot read.",
            BUCKET, key, code,
        )
        return None, None, False, f"incumbent unreadable: {code}"


def _persist(scope: dict, key: str) -> dict:
    """Accumulate this execution's scope onto the cycle's, and store it.

    alpha-engine-config-I8811. The write is CONDITIONAL in both directions:

    * incumbent read successfully  -> ``IfMatch`` its ETag, so a concurrent
      writer between our read and our write loses the race instead of being
      silently overwritten; a 412 re-reads and re-merges.
    * incumbent NOT readable       -> ``IfNoneMatch: "*"``, so the write lands
      only if the key is empty. A 412 here is the guard working: something is
      already there and this execution cannot merge onto it, so it declines and
      says so at ERROR rather than replacing a claim it never read.

    The second branch is what makes this fix safe from the moment the code
    deploys, before the ``s3:GetObject`` grant in ``iam-policy.json`` is
    applied: without the grant the Lambda cannot merge, but it also cannot
    destroy — the exact failure this exists to prevent.
    """
    s3 = boto3.client("s3")
    last_error = ""
    for attempt in range(1, _WRITE_ATTEMPTS + 1):
        incumbent, etag, read_ok, note = _read_incumbent(s3, key)
        merged, ledger = merge_run_scopes(incumbent, dict(scope))
        ledger["incumbent_read"] = {"ok": read_ok, "note": note}
        ledger["attempt"] = attempt
        merged["scope_merge"] = ledger
        for rejected in ledger["rejected"]:
            # Fail loud: a refused claim is an ERROR line naming both sides, not
            # a quiet no-op. It is also durable — it ships inside the artifact.
            logger.error(
                "run-scope write REFUSED for stage %s: keeping %s established "
                "by %s; this execution claimed %s. %s "
                "(alpha-engine-config-I8811)",
                rejected["stage"], rejected["kept"], rejected["kept_from"],
                rejected["refused"], rejected["why"],
            )
        params = {
            "Bucket": BUCKET,
            "Key": key,
            "Body": json.dumps(merged, indent=2, sort_keys=True).encode("utf-8"),
            "ContentType": "application/json",
        }
        if etag:
            params["IfMatch"] = etag
        else:
            params["IfNoneMatch"] = "*"
        try:
            s3.put_object(**params)
        except Exception as exc:  # noqa: BLE001
            code = _error_code(exc)
            if code not in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
                raise
            if not read_ok:
                # Terminal, and correct. We could not read what is there, and
                # something IS there. Retrying would loop on the same denial.
                logger.error(
                    "run-scope write DECLINED at s3://%s/%s — an artifact "
                    "already exists for this cycle and this execution could "
                    "not read it (%s), so it will not be overwritten. The "
                    "cycle keeps the scope it has. Apply the s3:GetObject "
                    "grant in this lambda's iam-policy.json to restore "
                    "merging (alpha-engine-config-I8811).",
                    BUCKET, key, note,
                )
                scope["write_declined"] = {
                    "key": key, "reason": note,
                    "detail": "an artifact exists and could not be read; "
                              "refusing to overwrite an unread claim.",
                }
                scope["scope_merge"] = ledger
                return scope
            last_error = code
            logger.warning(
                "run-scope write lost a race at s3://%s/%s (%s, attempt %d/%d)"
                " — re-reading and re-merging.",
                BUCKET, key, code, attempt, _WRITE_ATTEMPTS,
            )
            continue
        logger.info(
            "wrote s3://%s/%s (%s)", BUCKET, key,
            "merged onto " + str(ledger["incumbent_execution_arn"])
            if ledger["merged"] else "first writer for this cycle",
        )
        return merged
    raise ScopeWriteConflictError(
        f"s3://{BUCKET}/{key} changed under every one of {_WRITE_ATTEMPTS} "
        f"read-merge-write attempts (last: {last_error}). Nothing was written; "
        "the cycle keeps the scope already on S3."
    )


def _assert_stage_coverage(stage: str, started: datetime, run_date: str | None) -> dict:
    """Record this stage's own output verdict (alpha-engine-config-I10172).

    `RunScope` declares `output: registered` / `artifacts: [weekly_run_scope]`
    in `ARTIFACT_REGISTRY.yaml` — this is a real producer, not a gate, so the
    verdict depends on whether `backtest/{run_date}/run_scope.json` actually
    landed in-window. Measured 2026-09-08: this stage had never once written
    a `_stage_coverage` verdict — the I8228 "never wired" class.

    ``run_date`` here is `event["run_date"]` — by the time `RunScope` runs,
    downstream of `NormalizeRunDates` (alpha-engine-config-I8809), that is
    already the cycle's TRADING day, the same value `_persist` above keys the
    artifact by. Never fabricated: a missing/blank run_date reports
    UNMEASURED rather than defaulting to any derived date
    (alpha-engine-config-I8155). Never alters the handler's outcome — an
    observer that can change the stage it observes is a new failure mode
    bolted onto the one it reports. Mirrors
    `weekly-preflight/index.py::_assert_stage_coverage` (`policy-shared-code`).
    """
    if not run_date:
        logger.error(
            "stage-coverage assertion has no run_date for %s — event carried none",
            stage,
        )
        return {"stage": stage, "status": "UNMEASURED", "reason": "no run_date on state input"}
    try:
        from krepis.stage_coverage import assert_stage_coverage
    except ImportError as exc:
        logger.error("stage-coverage assertion unavailable for %s: %s", stage, exc)
        return {"stage": stage, "status": "UNMEASURED", "reason": str(exc)}
    return assert_stage_coverage(stage, window_start=started, run_date=run_date)


def handler(event, _context):
    # Captured at handler ENTRY, before any write this invocation makes
    # (alpha-engine-config-I7214).
    _stage_coverage_started = datetime.now(timezone.utc)
    calendar_run_date = event.get("run_date") or ""
    execution_arn = event.get("execution_arn") or ""
    state_machine_arn = event.get("state_machine_arn") or ""
    dry_run = bool(event.get("dry_run"))
    flags = {
        key: value for key, value in (event.get("execution_input") or {}).items()
        if key.startswith(_FLAG_PREFIX)
    }
    # Established before the try so the except branch always has a value to
    # key the (possibly degraded) artifact by — even when the failure IS the
    # resolution of `run_date` itself.
    run_date = ""

    try:
        # alpha-engine-config-I8373: `$.run_date` is the SF's CALENDAR date
        # (`InitializeInput` sets it from `$$.Execution.StartTime` — a Saturday
        # for this pipeline). Every artifact under `backtest/{key}/` is keyed by
        # the TRADING day per DATE_CONVENTIONS, and the sole consumer
        # (`crucible-evaluator/grading/handler.py::_resolve_run_date`) normalizes
        # through this same shared primitive before reading. Writing under the
        # raw calendar date put this artifact where nothing has ever looked —
        # measured live, 2026-08-22: `backtest/2026-08-22/run_scope.json` sat
        # alone while the cycle's ~49 other artifacts were under
        # `backtest/2026-08-21/`.
        if not calendar_run_date:
            raise EmptyRunDateError(
                "event['run_date'] was empty — refusing to write "
                "backtest//run_scope.json"
            )
        run_date = resolve_trading_day(calendar_run_date)

        states = boto3.client("stepfunctions")
        definition = _definition(states, state_machine_arn)
        history = _history(states, execution_arn)
        scope = build_run_scope(
            definition,
            history,
            run_date=run_date,
            execution_arn=execution_arn,
            state_machine_arn=state_machine_arn,
            input_flags=flags,
        )
        # Both dates ride along: `run_date` stays the resolved trading day (the
        # consumer's existing contract, unchanged), and the raw calendar date
        # the execution actually started on is named explicitly so a reader can
        # tell which execution produced this artifact without re-deriving it.
        scope["calendar_run_date"] = calendar_run_date
    except Exception as exc:  # noqa: BLE001
        # Fail-open, and loudly. See the module docstring: the degraded block
        # grades NOTHING, so a consumer reading it cannot mistake this for a
        # narrow-but-clean run. Raising here would terminate a weekly execution
        # over an advisory artifact.
        logger.exception("run-scope derivation failed — emitting an empty scope")
        scope = build_run_scope(
            {}, [], run_date=run_date, execution_arn=execution_arn,
            state_machine_arn=state_machine_arn,
        )
        scope["degraded"] = True
        scope["degraded_reason"] = f"{type(exc).__name__}: {exc}"
        scope["calendar_run_date"] = calendar_run_date
        scope["statement"] = (
            "SCOPE UNAVAILABLE — the run's own scope could not be derived, so "
            "NOTHING on this cycle is established as having been dispatched. "
            "No stage is graded. This is not a narrow run; it is an unmeasured "
            f"one. Cause: {type(exc).__name__}."
        )

    scope["written_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    stamp_provenance(scope, execution_arn, scope["written_at"])
    logger.info("run scope: %s", scope["statement"])

    if dry_run:
        # The Friday-PM shell run exercises the whole read+derive path (client
        # construction, the API grants, the derivation) and writes nothing —
        # the same dry contract every advisory producer on this pipeline
        # honours. It DOES exercise the incumbent read, because that grant is
        # new (alpha-engine-config-I8811) and a rehearsal that skipped it would
        # leave a missing `s3:GetObject` to be discovered by the Saturday run
        # it protects.
        scope["dry_run"] = True
        if run_date:
            _, _, read_ok, note = _read_incumbent(
                boto3.client("s3"), KEY_TEMPLATE.format(run_date=run_date)
            )
            scope["incumbent_read"] = {"ok": read_ok, "note": note}
        return scope

    if not run_date:
        # `run_date` never resolved (the EmptyRunDateError path, or a parse
        # failure so total that `resolve_trading_day` itself couldn't return
        # anything usable). Writing here would produce
        # `backtest//run_scope.json` — a key nothing reads and everything after
        # it would silently misinterpret as a real prefix. The Catch on the SF
        # `RunScope` state already routes any raised exception to
        # `CheckSkipReportCard` without failing the run, so an absent artifact
        # here is the honest outcome, not a gap.
        logger.error(
            "run_date never resolved — not writing an artifact for this "
            "execution (calendar_run_date=%r)", calendar_run_date,
        )
        scope["stage_coverage"] = _assert_stage_coverage(
            "RunScope", _stage_coverage_started, None
        )
        return scope

    written = _persist(scope, KEY_TEMPLATE.format(run_date=run_date))
    written["stage_coverage"] = _assert_stage_coverage(
        "RunScope", _stage_coverage_started, run_date
    )
    return written
