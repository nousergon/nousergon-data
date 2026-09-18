"""alpha-engine-weekly-coverage-sweep — the caller the stage-coverage sweep never had.

Tracked as ``alpha-engine-config-I8214`` (carrying ``I8154`` deliverable 4 and
``I8186``'s state-machine half).

**What it answers.** Did this weekly CYCLE actually cover the stages it
declares, and does the completion marker say so? ``nousergon_lib.pipeline_status``
shipped the reader — the cycle's real shape, the union over its contributing
executions, the coverage verdict against the artifact registry — and nothing
called it on a schedule, so it detected nothing. This handler is its caller.

**Why the marker needs it.** On 2026-08-22 the marker at
``_sf_completion/ne-weekly-freshness-pipeline/2026-08-22.json`` was written by
``watch-rerun-2026-08-22-3``, an execution that entered **1 of 16** declared
spine stages. The marker's name is a claim the run could not support: the SF
writes it on reaching its own terminal, which is a narrower fact than "the
cycle completed". So the SF now stamps ``claim: sf_execution_terminal`` and
``cycle_verdict: unknown`` on the object, and this handler AUGMENTS it with the
cycle's real shape — which executions contributed, what each entered, what the
union adds up to. A consumer reading the marker before this runs resolves to
UNKNOWN, never to an implied pass (``sf-pipeline-policy.md`` §2.3a).

**Why a Lambda and not an SSM command on the run's box.** The weekly SF has no
always-on instance: ``$.ec2_instance_id`` is an ephemeral spot, and since
``alpha-engine-config-I8162`` a recovery run whose every box stage is skipped
carries **no instance id at all** — which is exactly the shape of the run that
most needs a coverage verdict. A sweep that dereferences the instance id would
throw ``States.Runtime`` on those runs, one state after the pipeline's own
success terminal. The sweep reads Step Functions and S3 and writes S3 and one
metric; it has no reason to need a box. Mirrors ``weekly-run-scope``, the
sibling that reads the same execution history for the same pipeline.

**Failure posture — fail-open, and never silent.** FOUR outcomes, and the
last two are the ones that must not be collapsed into either of the first:

- ``clean`` — the sweep ran and found no gap.
- ``findings`` — the sweep ran and found a gap. It has already paged from
  inside ``nousergon_lib`` (``krepis.alerts``, deduped per pipeline+run_date).
- ``deferred`` — **the sweep ran and the cycle could not support the claim.**
  ``alpha-engine-config-I10170``. ``absent`` means "expected, entered, and
  recorded nothing", which only a cycle that has stopped changing can support;
  when the cycle is still in flight, or its contributor walk was truncated on
  a non-COMPLETED verdict, the sweep WITHHOLDS the absent count rather than
  asserting it. Withholding is not silence: the sweep still pages, naming
  every would-be-absent stage and the re-sweep command, and
  ``publish_sweep`` emits ``StageCoverageSweepDeferred=1`` on its own metric
  rather than letting the deferral be inferred from another metric's silence.
- ``unavailable`` — **the sweep did not run.** "Found nothing", "could not
  establish" and "did not run" are three different facts and only the last
  means the reader itself is dead (``principles.md`` §2.7). This handler
  returns it as its own outcome so the SF can page for it, because a sweep
  that never ran cannot page for itself.

**What it also answers now — ``alpha-engine-config-I9693``.** The cycle verdict
says whether the WEEK's work happened. It cannot say whether THIS execution did
any of it, and the two diverge on exactly the run that matters: measured live
2026-09-18, ``watch-rerun-2026-09-11-1`` entered ZERO of the sixteen declared
spine stages, reported ``SUCCEEDED``, and is indistinguishable from a four-hour
full run in ``list-executions``. Five consecutive Saturdays were "recovered"
that way. So this handler also returns ``observer_did_work`` — a fact it had
already computed to build the cycle — and the state machine's
``CheckExecutionDidWork`` Choice routes an explicit ``false`` to the
``VacuousRun`` Fail terminal. ``None`` means unestablished and is never
expressible as ``false``: an unknown is not rendered green, and it is not
manufactured into a red either.

It never raises: this state sits DOWNSTREAM of the pipeline's real success
terminal, and an observe-only tail that fails a completed run is a worse defect
than the one it was added to detect (``sf-pipeline-policy.md`` §2.1 blast
radius). The outcome is carried in the return value instead, where the SF's
Choice reads it.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ.get("RESEARCH_BUCKET", "alpha-engine-research")
PIPELINE = os.environ.get("PIPELINE", "ne-weekly-freshness-pipeline")

#: The four outcomes, closed. The SF's Choice matches these literals; adding a
#: fifth without a matching Choice branch lands on the Default, which is
#: ``unavailable`` — the honest fall-through, never ``clean``.
OUTCOME_CLEAN = "clean"
OUTCOME_FINDINGS = "findings"
OUTCOME_UNAVAILABLE = "unavailable"
#: The sweep RAN, and the cycle could not support the claim it was asked to
#: make (``alpha-engine-config-I10170``). Distinct from all three above: it is
#: not clean, it is not a finding about a stage, and the sweep did not fail.
#: The SF Choice has a branch for it; without one it lands on the Default,
#: which is ``unavailable`` — still honest, just less precise.
OUTCOME_DEFERRED = "deferred"

#: The pipeline state this handler IS. It is executing while it grades, so it
#: can never have written a verdict about its own completion — reporting it
#: ``absent`` is a false positive guaranteed on every run, and it was one of
#: the 13 on 2026-09-04 (``alpha-engine-config-I10170``).
SWEEP_STAGE = os.environ.get("SWEEP_STAGE", "WeeklyCoverageSweep")


#: `RunScope`'s own artifact key template (mirrors
#: `weekly-run-scope/index.py::KEY_TEMPLATE`) — the ONE place a cycle records
#: which declared stages it deliberately did not intend to run this week
#: (`disposition: DISABLED`, e.g. `skip_parity: true`), versus which it
#: simply never reached.
RUN_SCOPE_KEY_TEMPLATE = "backtest/{run_date}/run_scope.json"


def _declared_skip_stage_spine(
    *, pipeline: str, run_date: str, bucket: str, s3_client
) -> tuple[tuple[str, ...] | None, tuple[str, ...]]:
    """The declared spine for THIS cycle, minus any stage RunScope recorded
    as ``DISABLED`` — and which stages were excluded, for the caller to log.

    ``alpha-engine-config-I10175``. ``nousergon_lib.pipeline_status.registry
    .PIPELINE_STAGE_ORDER`` declares ``ParityParallel`` / ``PitParityCompare``
    unconditionally for every weekly cycle. A cycle with ``skip_parity: true``
    (a recorded operator flag on the Saturday EventBridge target since
    2026-08-13, not a defect) never intends to enter them — and without this
    exclusion the cycle's completion verdict reads ``INCOMPLETE`` forever,
    every week, for as long as the flag holds. Measured live on the
    2026-09-04 cycle after ``alpha-engine-config-I10170``'s observer fix:
    ``stages 14/16, missing ParityParallel, PitParityCompare``,
    ``walk_exhausted: false`` — not a truncation artifact.

    Returns ``(None, ())`` — the full declared spine, no caller-side
    exclusion — on ANY failure to read or parse ``run_scope.json``: this is
    advisory, best-effort, and NEVER fabricates a claim that a stage was
    disabled. A missing/unreadable ``run_scope.json`` degrades to today's
    behaviour (the cycle reads INCOMPLETE if it genuinely is), never to a
    false COMPLETED.
    """
    try:
        from nousergon_lib.pipeline_status.registry import stage_order_for

        full_spine = stage_order_for(pipeline)
        if not full_spine:
            return None, ()

        key = RUN_SCOPE_KEY_TEMPLATE.format(run_date=run_date)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        import json as _json

        scope = _json.loads(body)
        stages = scope.get("stages") or {}
        excluded = tuple(
            sorted(
                {
                    str(row.get("entry_state") or name)
                    for name, row in stages.items()
                    if isinstance(row, dict) and row.get("disposition") == "DISABLED"
                }
                & set(full_spine)
            )
        )
        if not excluded:
            return None, ()
        adjusted = tuple(s for s in full_spine if s not in excluded)
        return adjusted, excluded
    except Exception as exc:  # noqa: BLE001 — advisory, never blocks the sweep
        logger.warning(
            "coverage sweep: could not derive a declared-skip exclusion for "
            "%s %s from run_scope.json — using the full declared spine "
            "(%s: %s)",
            pipeline, run_date, type(exc).__name__, exc,
        )
        return None, ()


#: What ``observer_did_work`` means when it is absent from this handler's
#: return value: the observer's contribution could not be established. It is
#: NOT ``false``. The SF's ``CheckExecutionDidWork`` Choice fires only on an
#: explicit ``false``, so an unknown never manufactures a ``VacuousRun``
#: terminal out of a sweep that could not read the cycle
#: (``principles.md`` §2.7 cuts both ways — an unmeasured run is not green,
#: and it is not red either; the ``unavailable`` outcome above is the surface
#: that reports it).
OBSERVER_WORK_UNKNOWN = None


def _observer_contribution(sweep, observer_execution_arn: str) -> dict:
    """Did THIS execution enter any declared spine stage? — ``alpha-engine-config-I9693``.

    The cycle verdict answers *did the WEEK's work happen*. It cannot answer
    *did THIS execution do any of it*, and the two diverge on exactly the run
    that matters: measured live 2026-09-18, ``watch-rerun-2026-09-11-1``
    entered **zero** of the sixteen declared spine stages (26 ``skip_*`` flags
    true), reported ``SUCCEEDED``, and is indistinguishable from a full run in
    ``list-executions``. Five consecutive Saturdays were "recovered" this way.

    ``sweep.cycle`` already carries the per-execution breakdown — the observer
    row is flagged ``is_observer`` and its ``stages_entered`` is the declared
    spine stages it entered, computed by
    ``nousergon_lib.pipeline_status`` against ``PIPELINE_STAGE_ORDER``. So this
    derives nothing new and calls nothing new: it SURFACES a fact the sweep
    already established, where the state machine's Choice can read it.

    Returns a dict always carrying ``did_work`` — ``True``/``False`` when the
    observer row was found, ``None`` when it was not. ``None`` is a refusal,
    never a default: see :data:`OBSERVER_WORK_UNKNOWN`.
    """
    cycle = getattr(sweep, "cycle", None)
    if cycle is None:
        return {
            "did_work": OBSERVER_WORK_UNKNOWN,
            "reason": "the cycle could not be read, so this execution's own "
                      "contribution to it is unestablished",
        }
    rows = list(getattr(cycle, "executions", ()) or ())
    row = next((r for r in rows if getattr(r, "is_observer", False)), None)
    if row is None and observer_execution_arn:
        row = next(
            (r for r in rows
             if getattr(r, "execution_arn", "") == observer_execution_arn),
            None,
        )
    if row is None:
        return {
            "did_work": OBSERVER_WORK_UNKNOWN,
            "reason": "this execution is not among the cycle's contributing "
                      f"executions ({len(rows)} read) — its own contribution "
                      "is unestablished",
            "execution_arn": observer_execution_arn,
        }
    entered = tuple(getattr(row, "stages_entered", ()) or ())
    spine = tuple(getattr(cycle, "stage_spine", ()) or ())
    return {
        "did_work": bool(entered),
        "execution_arn": getattr(row, "execution_arn", "") or observer_execution_arn,
        "pipeline_role": getattr(row, "pipeline_role", None),
        "stages_entered": list(entered),
        "stages_entered_count": len(entered),
        "spine_size": len(spine),
        "reason": (
            f"this execution entered {len(entered)} of {len(spine)} declared "
            "spine stages"
            if entered else
            "this execution entered NONE of the "
            f"{len(spine)} declared spine stages — it dispatched no work, so "
            "its terminal must not read as a run that did"
        ),
    }


def _outcome_for(sweep) -> str:
    """The handler's verdict. ``deferred`` OUTRANKS ``findings``.

    ``alpha-engine-config-I10170``. When the cycle cannot support an absence
    claim, the honest headline is that coverage is not established — not that
    N stages are absent, which is the assertion the sweep just declined to
    make. Findings that DID land are still real and still page from inside
    ``nousergon_lib``; they are reported in the explanation either way.
    """
    if sweep.deferred:
        return OUTCOME_DEFERRED
    return OUTCOME_FINDINGS if sweep.should_alert else OUTCOME_CLEAN


def handler(event, _context):
    run_date = (event or {}).get("run_date") or ""
    # alpha-engine-config-I8809: the LEGACY partition. The sweep unions the
    # trading-day family (run_date) with the calendar family until the
    # 2026-09-05 cutover, so one cycle split across both reads as one cycle.
    # Absent => a single-partition sweep, which is the post-cutover shape.
    calendar_date = (event or {}).get("calendar_date") or ""
    state_machine_arn = (event or {}).get("state_machine_arn") or ""
    # alpha-engine-config-I10170: THE OBSERVER. This handler runs as a state
    # INSIDE the execution it is grading, so that execution is RUNNING at the
    # instant it grades — on every run, forever. Measured on the live
    # 2026-09-04 cycle: the sweep at 21:39:18Z declared the cycle in_flight
    # and paged 13 absences in the same artifact; watch-rerun-2026-09-04-4,
    # the execution it called RUNNING, stopped at 21:39:19Z, two hops later.
    # Naming the observer lets nousergon_lib count its entered states (real
    # work) without letting its status make its own cycle non-terminal.
    observer_execution_arn = (event or {}).get("observer_execution_arn") or ""
    dry_run = bool((event or {}).get("dry_run"))

    if not run_date:
        # No cycle key means no cycle to sweep. Unobserved, not clean.
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": "no run_date supplied — there is no cycle key to sweep",
            "run_date": run_date,
        }

    try:
        import boto3
        from krepis.aws_region import resolve_region
        from nousergon_lib.pipeline_status.completion_marker import augment_marker
        from nousergon_lib.pipeline_status.coverage import (
            publish_sweep,
            read_coverage_sweep,
        )

        region = resolve_region()
        s3_client = boto3.client("s3", region_name=region)

        # alpha-engine-config-I10175: a cycle-local exclusion for stages
        # RunScope recorded as deliberately DISABLED this run (skip_parity
        # and its kind) — advisory, never fabricated; see the function
        # docstring for the fail-open contract.
        stage_spine, excluded_stages = _declared_skip_stage_spine(
            pipeline=PIPELINE, run_date=run_date, bucket=BUCKET, s3_client=s3_client,
        )
        if excluded_stages:
            logger.info(
                "coverage sweep %s %s: excluding declared-skip stage(s) from "
                "the cycle spine (RunScope disposition=DISABLED): %s",
                PIPELINE, run_date, ", ".join(excluded_stages),
            )

        sweep = read_coverage_sweep(
            pipeline=PIPELINE,
            run_date=run_date,
            calendar_date=calendar_date or None,
            state_machine_arn=state_machine_arn or None,
            bucket=BUCKET,
            s3_client=s3_client,
            observer_execution_arn=observer_execution_arn or None,
            observer_stage=SWEEP_STAGE,
            stage_spine=stage_spine,
        )
    except Exception as exc:  # noqa: BLE001 — a sweep that cannot run says so
        # Deliberate broad catch with a named recording surface: the return
        # value below IS the recording surface, and the SF pages on it. The
        # failure class swallowed is "the sweep could not be performed"; the
        # primary deliverable (the weekly run, already complete) is untouched.
        logger.exception("coverage sweep could not run for %s", run_date)
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": f"{type(exc).__name__}: {exc}",
            "run_date": run_date,
        }

    explanation = sweep.explain()
    logger.info("coverage sweep %s: %s", run_date, explanation)

    if dry_run:
        # The Friday-PM shell run exercises the whole read path — client
        # construction, every IAM grant the real run needs, the derivation —
        # and writes nothing. The same dry contract every advisory producer on
        # this pipeline honours.
        #
        # The outcome is the REAL one, not a hardcoded clean. Measured
        # 2026-08-22 on the first live dry invocation: the sweep found 28
        # absent verdicts and 1 finding, and this branch returned
        # ``outcome: clean`` anyway — a rehearsal that reports green whatever
        # it saw certifies nothing, and is the same "no data rendered as
        # healthy" defect (principles.md §2.7) the whole sweep exists to
        # detect. What ``dry_run`` withholds is the WRITES and the page, never
        # the verdict.
        observer = _observer_contribution(sweep, observer_execution_arn)
        return {
            "outcome": _outcome_for(sweep),
            "dry_run": True,
            "run_date": run_date,
            "partitions_read": list(sweep.partitions_read),
            "explanation": explanation,
            "observer": observer,
            "observer_did_work": observer["did_work"],
        }

    published = False
    augmented = False
    write_error: str | None = None
    try:
        import boto3

        publish_sweep(
            sweep,
            s3_client=s3_client,
            cloudwatch_client=boto3.client("cloudwatch", region_name=region),
            bucket=BUCKET,
        )
        published = True
        if sweep.cycle is not None:
            # Both partitions the state machine dual-wrote get the cycle
            # verdict, or a consumer on the legacy family reads UNKNOWN beside
            # a known verdict (alpha-engine-config-I8809).
            augment_marker(
                sweep.cycle,
                s3_client=s3_client,
                bucket=BUCKET,
                also_dates=sweep.partitions_read,
            )
            augmented = True
        else:
            logger.warning(
                "the cycle could not be read, so the marker keeps its bare "
                "envelope claim — a marker with no cycle block resolves to "
                "UNKNOWN, never to a pass"
            )
    except Exception as exc:  # noqa: BLE001
        # The sweep RAN; only its write failed. That is still an unobserved
        # surface for every downstream reader of the artifact and the marker,
        # so it reports as unavailable rather than as a clean sweep whose
        # result nobody can read.
        logger.exception("coverage sweep ran but could not publish for %s", run_date)
        write_error = f"{type(exc).__name__}: {exc}"

    observer = _observer_contribution(sweep, observer_execution_arn)

    if write_error is not None:
        return {
            "outcome": OUTCOME_UNAVAILABLE,
            "reason": f"the sweep ran but could not publish its result: {write_error}",
            "run_date": run_date,
            "explanation": explanation,
            "observer": observer,
            "observer_did_work": observer["did_work"],
        }

    if sweep.should_alert:
        try:
            from krepis import alerts

            alerts.publish(
                explanation,
                severity="error",
                source=f"stage-coverage-sweep/{PIPELINE}",
                dedup_key=f"stage-coverage-sweep/{PIPELINE}/{run_date}",
            )
        except Exception:  # noqa: BLE001
            # A failed page must not turn a real finding into a clean result.
            # The outcome below still says findings, and the SF records it.
            logger.exception("coverage sweep finding could not be paged")

    if observer["did_work"] is False:
        # Fail loud on the way out: the terminal the SF is about to take is a
        # Fail, and the log line that explains why belongs next to it.
        logger.error(
            "VACUOUS RUN: %s %s — %s (alpha-engine-config-I9693). The SF's "
            "CheckExecutionDidWork Choice routes this execution to the "
            "VacuousRun Fail terminal so list-executions cannot show it as a "
            "run that did work.",
            PIPELINE, run_date, observer["reason"],
        )

    return {
        "outcome": _outcome_for(sweep),
        "run_date": run_date,
        "observer": observer,
        "observer_did_work": observer["did_work"],
        "coverage_established": sweep.coverage_established,
        "deferral_reason": sweep.deferral_reason,
        "partitions_read": list(sweep.partitions_read),
        "legacy_partition_rows": sweep.legacy_partition_rows,
        "explanation": explanation,
        "published": published,
        "marker_augmented": augmented,
    }
