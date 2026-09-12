"""
validators/stage_output_sweep.py — stage-level OUTPUT assertion for a
scheduled Step Functions run (alpha-engine-config-I7167).

## The class this exists to catch

The weekly pipeline had **no stage-level output assertion**. A stage that
runs, exits 0 and writes nothing was indistinguishable from one that did its
job, because the only thing any surface read was the stage's own exit status.

A full artifact-freshness audit of the scheduled 2026-08-08 run — which
terminated ``SUCCEEDED`` — found FIVE stages that produced no output for that
run, none of them visible anywhere. Five different root causes (a shell trap
that died at line 5, a caller/contract disagreement, a fresh sibling masking a
dead key, a 900s wall-kill, a prefix cutover) produced **one identical
observable: nothing**. Fixing five producers leaves the sixth undetectable, so
the deliverable is the assertion, and the five fixes are its first sweep
(``engagement-protocol-policy.md`` §5 — the fix survives the class).

It is ``sf-pipeline-policy.md`` §2.3a's decorative-verdict test applied to DATA
artifacts, and ``principles.md`` §2.7: a stage emitting nothing is not healthy,
it is **unobserved**, and *no data* is never rendered green.

## Where the declaration lives — deliberately not here

The stage→artifact mapping is ``ARTIFACT_REGISTRY.yaml``'s ``produced_by:``
(``alpha-engine-config``, config-I7180), read here from the S3 mirror the
config repo publishes at ``_freshness_monitor/ARTIFACT_REGISTRY.yaml``. This
module owns **no** artifact declarations of its own. A second registry beside
one that already exists, already carries producer rows, and is already read by
the freshness monitor is ``policy-shared-code``'s duplication failure one layer
up, and I7167 names that trap explicitly.

The division of labour against the freshness monitor is worth stating, because
the two look similar and answer different questions:

  * the **freshness monitor** asks *"is this key younger than its CADENCE
    clock?"* — a standing, run-agnostic SLA sweep;
  * this sweep asks *"did the stage that ran in THIS EXECUTION write the key it
    declares?"* — an execution-scoped, producer-anchored assertion.

A weekly stage that stopped writing can hide from the first behind a daily
producer of the same prefix, or behind a grace period wide enough to cover the
gap. It cannot hide from the second, because the second is anchored to an
execution start time rather than to a calendar.

## Verdicts, and why "could not measure" is its own answer

This fleet has shipped detectors that report a HARNESS FAULT as a FINDING, and
always in the alarming direction. The verdict set keeps the two apart:

  ``wrote``       the declared key exists and was written at/after the
                  execution start — the stage did its job
  ``stale``       the key exists but PREDATES this execution — the silent-stage
                  signature. This is the 2026-08-08 observable for the
                  ``Backtester`` second artifact (87 days old) and
                  ``ParityReplay`` (3+ weeks old)
  ``missing``     no object at the declared key at all
  ``unmeasured``  the check could NOT answer — S3 denied/errored, the key
                  template carries a placeholder this run cannot resolve, or no
                  execution window was supplied so staleness is undecidable.
                  **Never** folded into ``wrote`` and never into ``missing``:
                  a check that cannot run is not evidence of health and is not
                  evidence of a defect either
  ``unresolved``  the registry row declares ``stage: UNRESOLVED`` — the
                  artifact is real but its producing stage was never
                  established (config-I7180's published, ratcheted gap). Not a
                  defect of this run; counted separately so it can never be
                  quietly absorbed into the green number
  ``skipped``     the declaring stage did not enter this execution. Only
                  reachable when an entered-stage set is supplied; without one
                  no stage is ever declared skipped, because "I don't know
                  which stages ran" must not become "the stage was allowed not
                  to run"
  ``dry``         (alpha-engine-config-I7392) the stage entered but this
                  execution ran under the dry-path contract (``shell_run``),
                  which forbids every routed producer from writing. A
                  would-be ``missing``/``stale`` verdict downgrades to
                  ``dry`` here rather than being reported as either — counted
                  in NEITHER ``defects`` NOR ``wrote``, exactly like
                  ``skipped``/``unresolved``. Only reachable when the run's
                  ``run_mode`` resolved to ``"dry"``; a stage that DID write
                  under a dry run (an exempted producer — SignalsEnvelope,
                  ChallengerShadow) still reports ``wrote``, because the key
                  check happens before this downgrade
  ``wrote_by_sibling``
                  (I7392) the key exists, predates ``execution_start``, but
                  its ``LastModified`` falls on this run's ``cycle_date`` —
                  the signature of a same-cycle sibling pipeline (e.g. the
                  postclose SF) writing the artifact minutes before this
                  execution started, not of a silent stage. Not a defect;
                  distinct from ``wrote`` so it stays visible in the counts
  ``gated``       (alpha-engine-config-I9617) the key is absent or stale AND
                  the registry row itself declares that its absence is
                  CORRECT — ``cadence: event_driven`` (the sanctioned
                  contract: absence is expected, liveness is delegated to the
                  ``liveness_via`` anchor) or the legacy
                  ``grace_period_cycles: 999`` alias it replaces. These are
                  gated best-effort studies whose producer auto-skips when its
                  own precondition is unmet, so a MISSING verdict on them is a
                  statement about the gate, not about the run. Counted in
                  NEITHER ``defects`` NOR ``wrote``, and REPORTED by name in
                  the verdict document and the log — a row excused silently is
                  indistinguishable from one nobody checks. A gated row that
                  DID get written still reports ``wrote``: the downgrade only
                  ever applies to a would-be ``missing``/``stale``

## The producer-chosen ``*`` segment

A registry row may declare one path segment whose value the PRODUCER chooses
at write time and no consumer can derive — a single literal ``*`` occupying
one whole segment (alpha-engine-config-I10200). The live instance is
``predictor/diagnostics/oos_rows/*/{date}.parquet``, scoped by the model
FAMILY under alpha-engine-config-I9378 so a parallel zoo spec cannot
overwrite the champion's diagnostic.

Such a resolved key is a PATTERN and cannot be head-ed, so it is resolved by
listing the fixed prefix before the ``*`` and taking the NEWEST instance
matching the pattern — with the ordinary placeholders already substituted, so
the match is anchored to this run's cycle date and only the producer's segment
is free. The verdict logic then runs unchanged: the row is held to exactly the
same standard as a fixed key. Zero matches is ``missing``; a listing that
could not complete is ``unmeasured``; and **never** a fallback to the
unscoped key, which would resurrect the overwrite hazard I9378 exists to
prevent.

``unmeasured`` is deliberately NOT silent. Absence of a measurement is a
first-class finding with its own exit code and its own alert, because the
alternative — a sweep that reports 0 defects because it could not look — is the
exact failure it was written to end.

## Absence is measurable without a window; staleness is not

If no ``--execution-start`` is supplied the sweep still runs, and a
``missing`` key is still a defect: an object that does not exist did not get
written by anybody, whatever the window. What becomes undecidable is
``stale`` — an existing key cannot be attributed to this run or a previous one
— so every present key degrades to ``unmeasured`` rather than to ``wrote``.
That degradation is honest and it is loud, and it is what the sweep does on the
deployment path where the SF has not yet been taught to pass its start time.

## Observe-first, and why enforcement is a separate flip

``alpha-engine-config-I6891`` routes any degraded weekly run through
``CheckDegradedOutcome`` → ``WriteCompletionMarkerDegraded`` → ``DegradedRun``,
a **Fail** state. That is correct and must not be undone — but it means any
non-zero exit from this sweep, invoked as it is from
``WeeklySubstrateHealthCheck``, terminates a four-hour run as FAILED.

Three of the five instances I7167 records are still open today. Shipping this
enforcing would therefore hard-fail the next scheduled run for defects that
were already there — punishing the pipeline for the detector arriving, which is
exactly the shape ``ruling_detect_before_enforcing_when_the_floor_is_unmeasured``
(Brian, 2026-08-11) forbids.

So the default is ``observe``: findings ALERT, are written to a durable verdict
artifact, and the process exits 0. ``--enforce`` makes findings exit non-zero.
The flip is one flag, and its precondition is one measured number — the first
scheduled run's finding count. Observe mode is not silent mode: it is loud on
every surface except the one that kills the run.

The sweep's own verdict artifact
(``_stage_outputs/{pipeline}/{run_date}.json``) carries a row in
``ARTIFACT_REGISTRY.yaml``, so this detector going quiet is itself detected —
``principles.md`` §2.7 applied to the detector rather than only by it.

## Usage

  python -m validators.stage_output_sweep --run-date 2026-08-08 \\
      --execution-start 2026-08-08T09:00:49Z
  python -m validators.stage_output_sweep --run-date 2026-08-08 --enforce
  python -m validators.stage_output_sweep --run-date 2026-08-08 \\
      --registry ./ARTIFACT_REGISTRY.yaml --no-alert --no-publish

Exit codes:
  0  no defect found (or observe mode, whatever was found)
  1  ENFORCE mode and at least one ``missing``/``stale`` verdict
  2  sweep-infra failure: the registry could not be read, or ENFORCE mode with
     at least one ``unmeasured`` verdict. Distinct from 1 on purpose — the
     alert text and the operator's next move differ, and collapsing them is
     how a harness fault gets filed as a data defect
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional, Sequence

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_PIPELINE = "ne-weekly-freshness-pipeline"

# The config repo mirrors ARTIFACT_REGISTRY.yaml here on merge
# (.github/workflows/sync-artifact-registry.yml). Reading the mirror rather
# than a checked-out copy is what lets this run on the dashboard box, which
# does not clone the private config repo.
REGISTRY_KEY = "_freshness_monitor/ARTIFACT_REGISTRY.yaml"

# Where this sweep's own verdict lands. Registered in ARTIFACT_REGISTRY.yaml so
# the detector's SILENCE is detectable — a sweep that stops running must not
# read as a pipeline with nothing to report.
VERDICT_KEY_TEMPLATE = "_stage_outputs/{pipeline}/{run_date}.json"

UNRESOLVED_PRODUCER_STAGE = "UNRESOLVED"

# Verdicts. Ordered worst-first for reporting; the ordering is not a severity
# ladder used for suppression — every non-`wrote` verdict is reported.
WROTE = "wrote"
WROTE_BY_SIBLING = "wrote_by_sibling"
STALE = "stale"
MISSING = "missing"
UNMEASURED = "unmeasured"
UNRESOLVED = "unresolved"
SKIPPED = "skipped"
DRY = "dry"
GATED = "gated"

#: Verdicts that mean "this run has a data defect".
DEFECT_VERDICTS = frozenset({MISSING, STALE})

#: Verdicts that mean "the check could not answer". Never merged into
#: :data:`DEFECT_VERDICTS` — see the module docstring.
UNMEASURED_VERDICTS = frozenset({UNMEASURED})

#: alpha-engine-config-I7392 — the ``run_mode`` discriminator meaning "this
#: execution ran under the dry-path contract (``shell_run``), which forbids
#: every routed producer from writing". Distinct from the ``dry`` VERDICT
#: above: this is the run-level classification; that is the per-finding
#: outcome it produces.
RUN_MODE_DRY = "dry"

#: The one run_mode that is a genuine, artifact-producing run. Any other
#: resolved run_mode (``"exercise"``, ``RUN_MODE_DRY``) — or an unresolved one
#: — suppresses the OBSERVE-mode alert per I7392 item B; verdict publish and
#: the CloudWatch metric still happen regardless.
RUN_MODE_WEEKLY = "weekly"

_PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")

# Truncate finding lists in the alert body so a worst-case
# every-stage-silent run does not blow the SNS subject/body limits.
_ALERT_PREVIEW_LIMIT = 12


#: Execution-history event types that mean "this state was entered". Only the
#: ENTERED events are read: an ``*StateExited`` filter would silently excuse
#: every stage that entered and died, which is the exact population this sweep
#: exists to catch.
_ENTERED_EVENT_SUFFIX = "StateEntered"


class ExecutionContext:
    """What the sweep could establish about the execution it is asserting over.

    All three fields degrade INDEPENDENTLY and honestly. A denied
    ``GetExecutionHistory`` must not take the start time down with it, a
    malformed ``input`` must not take either down with it, and no failure may
    be reported as a fact about the pipeline.
    """

    __slots__ = ("start", "entered_stages", "notes", "run_mode")

    def __init__(
        self,
        start: datetime | None = None,
        entered_stages: frozenset[str] | None = None,
        notes: list[str] | None = None,
        run_mode: str | None = None,
    ) -> None:
        self.start = start
        self.entered_stages = entered_stages
        self.notes = notes or []
        self.run_mode = run_mode


def _parse_run_mode(raw_input: Any, *, notes: list[str]) -> str | None:
    """Classify an execution's ``run_mode`` from its ``describe_execution``
    ``input`` (alpha-engine-config-I7392).

    ``shell_run: true`` is the sole gate ``CheckShellRun``
    (``nousergon-data/infrastructure/step_function.json``) tests before
    routing to ``ApplyShellRunDefaults``, which threads ``--preflight-only``
    into every spot stage and ``dry_run``/``research_dry`` into every
    LLM/Lambda stage — so ``shell_run=true`` is treated as authoritative over
    whatever ``pipeline_role`` says. Absent that, ``pipeline_role`` (e.g.
    ``"weekly"``, ``"exercise"``) is reported as-is.

    Returns ``None`` — never a guess — when ``input`` is missing, not a
    string, or not parseable JSON, or when neither field is present. The
    caller renders ``None`` as "degrade to today's behaviour", which for
    finding classification means no ``missing``/``stale`` verdict is ever
    downgraded to ``dry``, and for alerting means the OBSERVE-mode
    non-weekly suppression in ``_should_alert`` never fires on a guess.
    """
    if not isinstance(raw_input, str):
        notes.append(
            "describe_execution returned no usable 'input' — run_mode is "
            "UNKNOWN, degrading to today's behaviour (no dry-verdict "
            "downgrade, no alert suppression)"
        )
        return None
    try:
        payload = json.loads(raw_input)
    except Exception as exc:  # noqa: BLE001
        notes.append(
            f"execution input is not parseable JSON ({type(exc).__name__}: "
            f"{exc}) — run_mode is UNKNOWN, degrading to today's behaviour"
        )
        return None
    if not isinstance(payload, dict):
        notes.append(
            "execution input parsed but is not a JSON object — run_mode is "
            "UNKNOWN, degrading to today's behaviour"
        )
        return None
    if payload.get("shell_run") is True:
        return RUN_MODE_DRY
    role = payload.get("pipeline_role")
    if isinstance(role, str) and role:
        return role
    notes.append(
        "execution input carries neither shell_run=true nor a usable "
        "pipeline_role — run_mode is UNKNOWN, degrading to today's behaviour"
    )
    return None


def read_execution_context(
    execution_arn: str,
    *,
    sfn_client: Any = None,
) -> ExecutionContext:
    """Read this execution's start time and entered-stage set.

    Why this is worth an API call rather than an SF-threaded parameter: a
    gated-out weekly run terminates ``SUCCEEDED`` in about five seconds
    (``WeeklyRunDayGate``), and roughly two of every three firings are such
    runs. Asserting a full artifact set against one of those would report ~30
    missing artifacts on a run that was correct to do nothing — a detector that
    cries wolf twice a week is a detector that gets muted, and then the real
    silent stage goes with it.

    The entered-stage set is what separates "the stage ran and wrote nothing"
    from "the stage never ran". Only the first is a defect.

    Both halves fail soft into ``None``, which the caller renders as
    ``unmeasured`` / no-stage-excused rather than as health. The
    ``alpha-engine-dashboard-role`` already carries ``states:DescribeExecution``
    and ``states:GetExecutionHistory`` on
    ``execution:ne-weekly-freshness-pipeline:*`` (policy
    ``alpha-engine-dashboard-sfn-read``, measured 2026-08-13), so this needs no
    new grant — but it is written to survive losing one.
    """
    context = ExecutionContext()
    if sfn_client is None:
        try:
            import boto3  # noqa: PLC0415

            from krepis.aws_region import resolve_region  # noqa: PLC0415

            sfn_client = boto3.client("stepfunctions", region_name=resolve_region())
        except Exception as exc:  # noqa: BLE001
            context.notes.append(f"stepfunctions client unavailable: {exc}")
            return context

    try:
        described = sfn_client.describe_execution(executionArn=execution_arn)
        start = described.get("startDate")
        if isinstance(start, datetime):
            context.start = _utc(start)
        else:
            context.notes.append("describe_execution returned no usable startDate")
        context.run_mode = _parse_run_mode(described.get("input"), notes=context.notes)
    except Exception as exc:  # noqa: BLE001
        context.notes.append(
            f"describe_execution failed ({type(exc).__name__}: {exc}) — staleness "
            f"is undecidable for this run, and run_mode is UNKNOWN (degrading to "
            f"today's behaviour — never to silence)"
        )

    try:
        entered: set[str] = set()
        token = None
        while True:
            kwargs = {"executionArn": execution_arn, "maxResults": 1000}
            if token:
                kwargs["nextToken"] = token
            page = sfn_client.get_execution_history(**kwargs)
            for event in page.get("events", []):
                event_type = event.get("type", "")
                if not event_type.endswith(_ENTERED_EVENT_SUFFIX):
                    continue
                details = event.get("stateEnteredEventDetails") or {}
                name = details.get("name")
                if name:
                    entered.add(name)
            token = page.get("nextToken")
            if not token:
                break
        context.entered_stages = frozenset(entered)
    except Exception as exc:  # noqa: BLE001
        context.notes.append(
            f"get_execution_history failed ({type(exc).__name__}: {exc}) — the "
            f"entered-stage set is UNKNOWN, so no stage is excused as skipped"
        )
    return context


class RegistryUnreadable(RuntimeError):
    """The registry could not be read or parsed.

    Raised rather than returned-as-empty: an empty producer set makes every
    assertion vacuously pass, which is a detector that cannot fail wearing the
    clothes of a clean run.
    """


def _utc(value: datetime) -> datetime:
    """Normalise to timezone-aware UTC. Naive input is ASSUMED UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_execution_start(raw: str) -> datetime:
    """Parse an ISO-8601 execution start, accepting the trailing ``Z`` that
    Step Functions' ``$$.Execution.StartTime`` emits.

    ``datetime.fromisoformat`` gained ``Z`` support only in 3.11; the dashboard
    box's venv is pinned below that on at least one fleet host, so the suffix is
    normalised here rather than relied upon.
    """
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    return _utc(datetime.fromisoformat(text))


def resolve_cycle_date(run_date: str) -> str | None:
    """The CYCLE date the registry's key templates are written against.

    This is not the SF's ``$.run_date`` and conflating the two is the single
    most expensive mistake available here. Measured on the 2026-08-08 run:
    ``$.run_date`` was ``2026-08-08`` (the calendar Saturday), while every
    artifact that run produced landed under ``2026-08-07`` — ``backtest/
    2026-08-08/`` does not exist at all, and ``backtest/2026-08-07/report.md``
    carries ``LastModified 2026-08-08T06:13``. Resolving ``{date}`` to the run
    date produced **28 confident false MISSING verdicts** against a run that had
    written almost everything.

    That is this sweep's own failure mode turned on itself — a detector
    reporting a harness fault as a finding about the system, in the alarming
    direction — and it is why this function exists rather than a string
    substitution at the call site.

    ``krepis.dates.expected_last_close`` is the fleet's canonical resolver and
    the same notion of "cycle tick" that
    ``nousergon_lib.artifact_freshness._format_key`` substitutes; it is used
    here rather than re-derived so the two cannot drift.

    Returns ``None`` when the resolver is unavailable, which the caller renders
    as ``unmeasured``. Guessing (weekday arithmetic, "probably yesterday")
    would reintroduce exactly the 28-false-positive failure with no way to see
    it had happened.
    """
    try:
        from krepis.dates import expected_last_close  # noqa: PLC0415

        return expected_last_close(run_date).isoformat()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Cycle-date resolution failed (%s) — every dated key will report "
            "unmeasured rather than be checked against a guessed date", exc,
        )
        return None


def resolve_key(
    template: str, *, run_date: str, cycle_date: str | None
) -> str | None:
    """Resolve a registry ``s3_key_template`` for this run.

    ``{date}`` and ``{trading_day}`` both resolve to ``cycle_date`` — they are
    aliases in the registry convention and in
    ``artifact_freshness._format_key``, the second being a semantic name for
    the same tick. ``{run_date}`` resolves to the SF's own ``$.run_date``.

    A literal ``*`` segment — the producer-chosen segment, the registry's way
    of declaring "one segment whose value the PRODUCER picks and no consumer
    can derive" (alpha-engine-config-I10200) — passes through untouched. The
    result is then a PATTERN, not a key, and :func:`evaluate` resolves it by
    listing rather than head-ing; see :func:`_newest_match`. The grammar is
    validated here (one whole segment, never the last) so a malformed row
    reports ``unmeasured`` with a reason naming the template, rather than
    producing a pattern that silently matches the wrong set.

    Returns ``None`` — meaning ``unmeasured``, never ``missing`` — when the
    template needs a cycle date that could not be resolved, or carries any
    OTHER placeholder (``{cycle_label}``, an hour bucket, anything added
    later). Head-ing a literal key containing braces would 404 every time and
    report a confident, permanent, entirely false ``missing``: the most
    convincing wrong finding this sweep could produce.
    """
    support = _wildcard_support()
    if "*" in template:
        if support is None:
            logger.error(
                "Template %r carries the producer-chosen '*' segment but "
                "nousergon_lib.artifact_freshness is unavailable — reporting "
                "unmeasured rather than head-ing a literal '*', which would "
                "404 and report a confident false missing",
                template,
            )
            return None
        try:
            support["validate_key_template"](template)
        except Exception as exc:  # noqa: BLE001 - any illegal shape is unmeasured
            logger.error(
                "Template %r has an unresolvable wildcard shape (%s) — "
                "unmeasured, not missing", template, exc,
            )
            return None
    resolved = template
    for name in set(_PLACEHOLDER_RE.findall(template)):
        if name in ("date", "trading_day"):
            if cycle_date is None:
                return None
            resolved = resolved.replace("{" + name + "}", cycle_date)
        elif name == "run_date":
            resolved = resolved.replace("{run_date}", run_date)
        else:
            return None
    if "{" in resolved or "}" in resolved:
        return None
    return resolved


def load_registry_rows(
    *,
    bucket: str = DEFAULT_BUCKET,
    registry_path: str | None = None,
    s3_client: Any = None,
) -> list[dict]:
    """Load the artifact registry rows, from a local path or the S3 mirror.

    Raises:
        RegistryUnreadable: on any read/parse failure, or when the parsed
            document carries no ``artifacts`` list. Never returns ``[]`` for a
            failure — see the class docstring.
    """
    try:
        import yaml  # noqa: PLC0415 — optional at import time, required here
    except ImportError as exc:  # pragma: no cover - environment gap
        raise RegistryUnreadable(f"PyYAML unavailable: {exc}") from exc

    try:
        if registry_path:
            with open(registry_path, "rb") as handle:
                raw = handle.read()
        else:
            if s3_client is None:
                import boto3  # noqa: PLC0415

                s3_client = boto3.client("s3")
            raw = s3_client.get_object(Bucket=bucket, Key=REGISTRY_KEY)["Body"].read()
    except Exception as exc:  # noqa: BLE001 - any read failure is unreadable
        raise RegistryUnreadable(
            f"could not read registry "
            f"({registry_path or f's3://{bucket}/{REGISTRY_KEY}'}): {exc}"
        ) from exc

    try:
        document = yaml.safe_load(raw)
    except Exception as exc:  # noqa: BLE001
        raise RegistryUnreadable(f"registry is not parseable YAML: {exc}") from exc

    rows = (document or {}).get("artifacts") if isinstance(document, dict) else None
    if not isinstance(rows, list) or not rows:
        raise RegistryUnreadable(
            "registry carries no non-empty 'artifacts' list — refusing to sweep "
            "against an empty producer set (an assertion over nothing always "
            "passes)"
        )
    return rows


#: The registry cadence symbol meaning "this artifact is written ONLY when a
#: gated producer decides to write, so its ABSENCE is correct". Declared and
#: validator-enforced in ``nousergon_lib.artifact_freshness``: an
#: ``event_driven`` row must name a ``liveness_via`` anchor, so producer
#: liveness is relocated to a signal that CAN go stale rather than lost.
_CADENCE_ABSENCE_IS_CORRECT = "event_driven"

#: The legacy magic number ``event_driven`` replaced. Still carried by eight
#: rows in the live registry (the six gated backtester sub-sweeps,
#: ``backtest_cov_estimator_sweep`` and ``regime_state_dated``). Recognised
#: here so a row that has not yet migrated is not falsely asserted against —
#: the migration is tracked separately; this sweep must not depend on it.
_LEGACY_GATED_FOREVER_GRACE = 999


def absence_declared_correct(row: dict) -> str | None:
    """Why this registry row's ABSENCE is a declared-correct outcome, or None.

    ``ARTIFACT_REGISTRY.yaml`` already states this rule for the OTHER consumer
    of the same declarations: ``pipeline_stages[].artifacts`` (read by
    ``krepis.stage_coverage``) deliberately omits every gated-forever row, and
    ``validate_artifact_registry.py::_validate_pipeline_stages`` Rule 7 fails
    the build if one is added back. This sweep derives its assertable set from
    ``produced_by:`` instead, and so re-admitted exactly the rows that rule
    excludes — alpha-engine-config-I9617.

    Measured on the 2026-08-29 scheduled weekly run
    (``965d925b-f9d6-ce5e-e059-9405433a0724_...``): all six reported STAGE
    OUTPUT MISSING findings were gated-forever rows — the five gated
    Backtester sub-sweeps plus ``regime_state_dated``. Every one of the run's
    findings was false, and the sweep is one ``--enforce`` flip away from
    terminating a four-hour run on them.

    Returns a human-readable reason (rendered into the finding's ``detail``)
    rather than a bool, because a row excused with no reason on the surface is
    indistinguishable from a row nobody checked.
    """
    if row.get("cadence") == _CADENCE_ABSENCE_IS_CORRECT:
        anchor = row.get("liveness_via")
        return (
            "registry declares cadence: event_driven — absence is the correct "
            "outcome for a gated producer, and producer liveness is delegated "
            f"to liveness_via={anchor!r}"
        )
    grace = row.get("grace_period_cycles")
    try:
        gated_forever = grace is not None and int(grace) >= _LEGACY_GATED_FOREVER_GRACE
    except (TypeError, ValueError):
        gated_forever = False
    if gated_forever:
        return (
            f"registry declares grace_period_cycles: {grace} — the legacy "
            "gated-forever marker (superseded by cadence: event_driven). The "
            "producer auto-skips when its own precondition is unmet, so this "
            "key's absence is a statement about the gate, not about the run"
        )
    return None


def declared_outputs(
    rows: Iterable[dict],
    *,
    pipeline: str,
    default_bucket: str = DEFAULT_BUCKET,
) -> list[dict]:
    """Invert the registry into ``[{stage, artifact_id, bucket, template}]``
    for one pipeline.

    A row with two real producers (the Scanner artifacts are written by BOTH
    the weekday preopen SF and the weekly SF) contributes one entry per
    matching producer — which is the whole point of ``produced_by`` being a
    list, and the reason a stopped weekly stage stops hiding behind a live
    daily one.
    """
    out: list[dict] = []
    for row in rows:
        for producer in row.get("produced_by") or []:
            if producer.get("pipeline") != pipeline:
                continue
            out.append(
                {
                    "stage": producer.get("stage") or UNRESOLVED_PRODUCER_STAGE,
                    "artifact_id": row.get("artifact_id"),
                    "bucket": row.get("s3_bucket") or default_bucket,
                    "s3_key_template": row.get("s3_key_template"),
                    "severity": row.get("severity", "warning"),
                    "absence_declared_correct": absence_declared_correct(row),
                }
            )
    return out


def _wildcard_support() -> dict[str, Any] | None:
    """The ``s3_key_template`` grammar, borrowed from ``nousergon_lib``.

    The producer-chosen ``*`` segment is the REGISTRY's grammar, and three
    independent resolvers have to agree about it: this sweep, the freshness
    monitor, and ``alpha-engine-config``'s registry validator. A third
    hand-rolled copy of "where does the prefix stop, what does the wildcard
    match, which shapes are illegal" is ``policy-shared-code``'s duplication
    failure, so the definition lives once in
    :mod:`nousergon_lib.artifact_freshness` and is imported here.

    Imported lazily and returned as ``None`` on failure, matching
    :func:`resolve_cycle_date`: this module is deliberately import-light and
    runs in contexts (a bare spot box, a unit test) where the lib may be
    absent. ``None`` renders every wildcard row ``unmeasured`` — loud, and
    never a head of a literal ``*``.
    """
    try:
        from nousergon_lib.artifact_freshness import (  # noqa: PLC0415
            has_wildcard_segment,
            key_pattern,
            listable_prefix,
            validate_key_template,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "nousergon_lib.artifact_freshness unavailable (%s) — every "
            "registry row carrying the producer-chosen '*' segment will "
            "report unmeasured rather than be checked against a guess", exc,
        )
        return None
    return {
        "has_wildcard_segment": has_wildcard_segment,
        "key_pattern": key_pattern,
        "listable_prefix": listable_prefix,
        "validate_key_template": validate_key_template,
    }


def _newest_match(
    s3_client: Any, bucket: str, pattern: str, *, cap_pages: int = 64
) -> tuple[str, Any, str]:
    """Resolve a ``*``-bearing key PATTERN to its newest existing instance.

    Returns ``(state, detail, key)`` with the same three-way state as
    :func:`_head` — ``("found", last_modified, key)``,
    ``("absent", None, "")``, ``("error", reason, "")`` — so
    :func:`evaluate`'s verdict logic is identical either way and a wildcard
    row is held to exactly the same standard as a fixed one.

    The pattern is the template with the ordinary placeholders already
    substituted, so the match is anchored to THIS run's cycle date and only
    the producer-chosen segment is free. That is deliberately stricter than
    the freshness monitor's recency model over the same rows: the monitor asks
    "is any instance younger than the cadence clock", this sweep asks "did the
    stage that ran in THIS EXECUTION write the key it declares", and answering
    the second with the first would let last week's instance clear this week's
    silent stage.

    **A truncated listing is an error, never ``absent``.** S3 lists
    lexically, so the keys past the cap are not a random sample — and
    "I could not see the whole prefix" is not evidence the artifact is
    missing (the class alpha-engine-config-I7617 was filed about).
    """
    support = _wildcard_support()
    if support is None:
        return "error", "nousergon_lib.artifact_freshness unavailable", ""
    try:
        compiled = support["key_pattern"](pattern)
        prefix = support["listable_prefix"](pattern)
    except Exception as exc:  # noqa: BLE001
        return "error", f"unresolvable pattern {pattern!r}: {exc}", ""

    newest_key = ""
    newest_lm = None
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page_index, page in enumerate(
            paginator.paginate(Bucket=bucket, Prefix=prefix)
        ):
            if page_index >= cap_pages:
                return (
                    "error",
                    f"LIST of {prefix!r} exceeded the {cap_pages}-page scan "
                    f"cap; keys list lexically so the objects past the cap "
                    f"are the newest — this is an unanswered question, not an "
                    f"absent artifact (alpha-engine-config-I7617)",
                    "",
                )
            for obj in page.get("Contents") or []:
                key = obj.get("Key") or ""
                if compiled.match(key) is None:
                    continue
                last_modified = obj.get("LastModified")
                if last_modified is None:
                    continue
                if newest_lm is None or last_modified > newest_lm:
                    newest_lm, newest_key = last_modified, key
    except Exception as exc:  # noqa: BLE001
        return "error", f"{type(exc).__name__}: {exc}", ""

    if newest_lm is None:
        return "absent", None, ""
    return "found", newest_lm, newest_key


def _head(s3_client: Any, bucket: str, key: str) -> tuple[str, Any]:
    """HEAD one key.

    Returns ``("found", last_modified)``, ``("absent", None)``, or
    ``("error", reason)``. The three-way return is the whole point: a 403 or a
    503 is NOT an absent object, and rendering it as one manufactures a defect
    out of a permissions gap.
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:  # noqa: BLE001
        code = ""
        response_meta = getattr(exc, "response", None)
        if isinstance(response_meta, dict):
            code = str(response_meta.get("Error", {}).get("Code", ""))
            status = response_meta.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("404", "NoSuchKey", "NotFound") or status == 404:
                return "absent", None
        return "error", f"{type(exc).__name__}: {code or exc}"
    return "found", response.get("LastModified")


def evaluate(
    declarations: Sequence[dict],
    *,
    run_date: str,
    execution_start: datetime | None,
    head: Callable[[str, str], tuple[str, Any]],
    find: Callable[[str, str], tuple[str, Any, str]] | None = None,
    entered_stages: frozenset[str] | None = None,
    cycle_date: str | None = None,
    run_mode: str | None = None,
    pipeline: str | None = None,
    verdict_bucket: str = DEFAULT_BUCKET,
) -> list[dict]:
    """Assign a verdict to every declared (stage, artifact) pair.

    Args:
        declarations: output of :func:`declared_outputs`.
        run_date: the SF-stamped ``$.run_date``, used to resolve key templates.
        execution_start: this execution's start. ``None`` means staleness is
            undecidable, so every PRESENT key is ``unmeasured`` rather than
            ``wrote`` — see the module docstring.
        head: ``(bucket, key) -> (state, detail)``, injected so the decision
            logic is testable without S3 and so the failing branch can actually
            be exercised.
        find: ``(bucket, pattern) -> (state, detail, key)`` for a resolved key
            carrying the producer-chosen ``*`` segment
            (alpha-engine-config-I10200) — a pattern cannot be head-ed, so it
            is resolved by listing the fixed prefix and taking the newest
            instance matching the pattern. States mirror ``head``, so the
            verdict logic below is identical either way. ``None`` (no lister
            supplied) makes every wildcard row ``unmeasured``: a caller that
            cannot list cannot answer those rows, and saying so is the only
            honest option — head-ing a literal ``*`` would 404 and report a
            confident, permanent, entirely false ``missing``.
        entered_stages: the stages that actually entered this execution, when
            known. ``None`` means unknown, and then NO stage is marked
            ``skipped`` — "I don't know which stages ran" must never become
            "the stage was allowed not to run". When supplied, a declaring
            stage absent from it is ``skipped`` (a degraded run legitimately
            skips stages, and a detector that cries wolf on those is a detector
            that gets turned off).
        run_mode: (I7392) ``RUN_MODE_DRY`` when this execution ran under the
            dry-path contract. Only then does a would-be ``missing``/``stale``
            verdict downgrade to ``dry``. ``None`` (unknown) never downgrades
            — see the module docstring's ``dry`` verdict entry.
        pipeline, verdict_bucket: (I7392) when both are supplied, a
            declaration whose resolved key equals THIS sweep's own verdict
            key (``VERDICT_KEY_TEMPLATE`` for this ``pipeline``/``run_date``
            in ``verdict_bucket``) is excluded entirely rather than evaluated
            — the sweep checks its declared artifacts before writing its own
            verdict, so that key is guaranteed ``missing`` on every run,
            forever, and is not a finding about the pipeline.
    """
    self_key = (
        VERDICT_KEY_TEMPLATE.format(pipeline=pipeline, run_date=run_date)
        if pipeline
        else None
    )
    findings: list[dict] = []
    for declaration in declarations:
        stage = declaration["stage"]
        finding = {
            "stage": stage,
            "artifact_id": declaration["artifact_id"],
            "s3_key_template": declaration["s3_key_template"],
            "bucket": declaration["bucket"],
            "severity": declaration.get("severity", "warning"),
            "key": None,
            # Non-None only for a row whose resolved key carries the
            # producer-chosen '*' segment: the pattern that was matched,
            # alongside `key`, which then names the instance actually found.
            "pattern": None,
            "last_modified": None,
            "detail": None,
        }

        if stage == UNRESOLVED_PRODUCER_STAGE:
            finding["verdict"] = UNRESOLVED
            finding["detail"] = (
                "registry row declares produced_by stage=UNRESOLVED (config-I7180 "
                "ratcheted gap) — this artifact cannot be attributed to a stage, "
                "so this run cannot be held to it"
            )
            findings.append(finding)
            continue

        if entered_stages is not None and stage not in entered_stages:
            finding["verdict"] = SKIPPED
            finding["detail"] = "stage did not enter this execution"
            findings.append(finding)
            continue

        template = declaration["s3_key_template"] or ""
        key = resolve_key(template, run_date=run_date, cycle_date=cycle_date)
        if key is None:
            finding["verdict"] = UNMEASURED
            finding["detail"] = (
                f"key template {template!r} carries a placeholder this sweep "
                f"could not resolve"
                + ("" if cycle_date else " (cycle date unresolved)")
            )
            findings.append(finding)
            continue

        finding["key"] = key

        # I7392 self-exclusion: this sweep's own verdict key is checked
        # BEFORE it is written (evaluate() runs, then _publish_verdict()), so
        # asserting it here is a guaranteed false MISSING on every run,
        # scheduled or rehearsal alike. Excluded rather than evaluated —
        # never appears in findings at all.
        if (
            self_key is not None
            and declaration["bucket"] == verdict_bucket
            and key == self_key
        ):
            continue

        if "*" in key:
            if find is None:
                finding["verdict"] = UNMEASURED
                finding["detail"] = (
                    f"resolved key {key!r} carries the producer-chosen '*' "
                    f"segment and no object lister was supplied, so this row "
                    f"cannot be resolved by a HEAD — unmeasured, not missing "
                    f"(alpha-engine-config-I10200)"
                )
                findings.append(finding)
                continue
            state, detail, observed = find(declaration["bucket"], key)
            # The pattern says where the artifact was expected; the observed
            # key says which instance the verdict was taken over. Reporting
            # only the pattern would leave the verdict unverifiable by hand.
            finding["pattern"] = key
            finding["key"] = observed or None
        else:
            state, detail = head(declaration["bucket"], key)

        if state == "error":
            finding["verdict"] = UNMEASURED
            finding["detail"] = f"S3 head_object could not answer: {detail}"
        elif state == "absent":
            if run_mode == RUN_MODE_DRY:
                finding["verdict"] = DRY
                finding["detail"] = (
                    "no object at the declared key, but this execution ran "
                    "under the dry-path contract (shell_run=true), which "
                    "forbids the declaring stage from writing — not a defect"
                )
            else:
                finding["verdict"] = MISSING
                finding["detail"] = "no object at the declared key"
        else:
            last_modified = _utc(detail) if isinstance(detail, datetime) else None
            finding["last_modified"] = (
                last_modified.isoformat() if last_modified else None
            )
            if last_modified is None:
                finding["verdict"] = UNMEASURED
                finding["detail"] = (
                    "object exists but S3 returned no usable LastModified — "
                    "presence alone cannot distinguish this run's write from a "
                    "previous one"
                )
            elif execution_start is None:
                finding["verdict"] = UNMEASURED
                finding["detail"] = (
                    "object exists but no execution start was supplied, so it "
                    "cannot be attributed to THIS run"
                )
            elif last_modified >= execution_start:
                finding["verdict"] = WROTE
            elif cycle_date and last_modified.date().isoformat() == cycle_date:
                # I7392 same-cycle sibling attribution: the object predates
                # THIS execution but was written on the same cycle_date — the
                # signature of a sibling pipeline (e.g. postclose) writing the
                # artifact for the same cycle shortly before this execution
                # started, not of a silent stage. Checked before the `dry`
                # downgrade below: a sibling-written object explains itself
                # regardless of this run's own mode.
                age = execution_start - last_modified
                finding["verdict"] = WROTE_BY_SIBLING
                finding["detail"] = (
                    f"object predates this execution by {age.days}d "
                    f"{age.seconds // 3600}h{age.seconds % 3600 // 60}m but was "
                    f"written on this run's cycle_date={cycle_date} — attributed "
                    f"to a same-cycle sibling pipeline, not a silent stage"
                )
            elif run_mode == RUN_MODE_DRY:
                age = execution_start - last_modified
                finding["verdict"] = DRY
                finding["detail"] = (
                    f"object predates this execution by {age.days}d "
                    f"{age.seconds // 3600}h and this execution ran under the "
                    f"dry-path contract (shell_run=true), which forbids the "
                    f"declaring stage from writing — not a defect"
                )
            else:
                age = execution_start - last_modified
                finding["verdict"] = STALE
                finding["detail"] = (
                    f"object predates this execution by {age.days}d "
                    f"{age.seconds // 3600}h — the declaring stage did not write "
                    f"it on this run"
                )
        # alpha-engine-config-I9617 — the registry's own gated-forever
        # declaration, applied LAST so it can only ever downgrade a would-be
        # DEFECT. A gated row that DID get written keeps its `wrote` verdict,
        # and an `unmeasured` row stays unmeasured: a check that could not
        # answer is not excused by a gate it never reached.
        gated_reason = declaration.get("absence_declared_correct")
        if gated_reason and finding["verdict"] in DEFECT_VERDICTS:
            was = finding["verdict"]
            finding["verdict"] = GATED
            finding["detail"] = (
                f"{finding['detail']} — NOT a defect ({was} suppressed): "
                f"{gated_reason}"
            )
        findings.append(finding)

    return findings


def summarise(findings: Sequence[dict]) -> dict:
    """Reduce findings to counts plus the two lists that decide the exit code."""
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding["verdict"]] = counts.get(finding["verdict"], 0) + 1
    defects = [f for f in findings if f["verdict"] in DEFECT_VERDICTS]
    unmeasured = [f for f in findings if f["verdict"] in UNMEASURED_VERDICTS]
    # alpha-engine-config-I9617: surfaced as its own list, not folded into the
    # green number. A row excused by the registry is a fact about the REGISTRY
    # that has to stay readable — if a gated study is quietly promoted to a
    # real producer, or a row's grace marker is wrong, this list is where that
    # shows up. Silent exclusion would make the sweep's own blind spot
    # invisible (principles.md §2.7).
    gated = [f for f in findings if f["verdict"] == GATED]
    if defects:
        status = "stage_output_missing"
    elif unmeasured:
        status = "stage_output_unmeasured"
    else:
        status = "ok"
    return {
        "status": status,
        "counts": counts,
        "checked": len(findings),
        "defects": defects,
        "unmeasured": unmeasured,
        "gated": gated,
    }


def sweep(
    *,
    run_date: str,
    pipeline: str = DEFAULT_PIPELINE,
    execution_start: datetime | None = None,
    entered_stages: frozenset[str] | None = None,
    bucket: str = DEFAULT_BUCKET,
    registry_path: str | None = None,
    s3_client: Any = None,
    execution_arn: str | None = None,
    sfn_client: Any = None,
    cycle_date: str | None = None,
    run_mode: str | None = None,
    cloudwatch_client: Any = None,
    alert: bool = True,
    publish: bool = True,
    enforce: bool = False,
    emit_metrics: bool = False,
) -> dict:
    """Assert every artifact this pipeline's stages declare was written by this
    run. Returns the full verdict document; never raises for a finding.
    """
    if cycle_date is None:
        cycle_date = resolve_cycle_date(run_date)
    context_notes: list[str] = []
    if execution_arn:
        context = read_execution_context(execution_arn, sfn_client=sfn_client)
        context_notes = context.notes
        # An explicitly-passed value wins over the API read: the caller may be
        # replaying a historical run whose ARN no longer resolves.
        if execution_start is None:
            execution_start = context.start
        if entered_stages is None:
            entered_stages = context.entered_stages
        if run_mode is None:
            run_mode = context.run_mode
        for note in context_notes:
            logger.warning("Execution context degraded: %s", note)

    try:
        rows = load_registry_rows(
            bucket=bucket, registry_path=registry_path, s3_client=s3_client
        )
    except RegistryUnreadable as exc:
        logger.error("Stage-output sweep could not read the registry: %s", exc)
        return {
            "schema": "stage_output_sweep-1.0.0",
            "status": "registry_unreadable",
            "pipeline": pipeline,
            "run_date": run_date,
            "enforce": enforce,
            "run_mode": run_mode,
            "error": str(exc),
            "checked": 0,
            "counts": {},
            "findings": [],
            "defects": [],
            "unmeasured": [],
            "gated": [],
        }

    if s3_client is None:
        import boto3  # noqa: PLC0415

        s3_client = boto3.client("s3")

    declarations = declared_outputs(rows, pipeline=pipeline, default_bucket=bucket)
    findings = evaluate(
        declarations,
        run_date=run_date,
        execution_start=execution_start,
        head=lambda b, k: _head(s3_client, b, k),
        find=lambda b, k: _newest_match(s3_client, b, k),
        entered_stages=entered_stages,
        cycle_date=cycle_date,
        run_mode=run_mode,
        pipeline=pipeline,
        verdict_bucket=bucket,
    )
    summary = summarise(findings)

    document = {
        "schema": "stage_output_sweep-1.0.0",
        "status": summary["status"],
        "pipeline": pipeline,
        "run_date": run_date,
        "cycle_date": cycle_date,
        "enforce": enforce,
        "run_mode": run_mode,
        "execution_start": (
            execution_start.isoformat() if execution_start else None
        ),
        "entered_stages_known": entered_stages is not None,
        "entered_stage_count": len(entered_stages) if entered_stages is not None else None,
        "execution_arn": execution_arn,
        "context_notes": context_notes,
        "declared_stages": sorted(
            {d["stage"] for d in declarations if d["stage"] != UNRESOLVED_PRODUCER_STAGE}
        ),
        "checked": summary["checked"],
        "counts": summary["counts"],
        "findings": findings,
        "defects": summary["defects"],
        "unmeasured": summary["unmeasured"],
        "gated": summary["gated"],
    }

    logger.info(
        "Stage-output sweep pipeline=%s run_date=%s enforce=%s run_mode=%s: %s",
        pipeline, run_date, enforce, run_mode, summary["counts"],
    )

    if summary["gated"]:
        logger.info(
            "STAGE OUTPUT GATED (not defects, alpha-engine-config-I9617): %d "
            "declared artifact(s) absent whose registry row declares the "
            "absence correct: %s",
            len(summary["gated"]), _describe(summary["gated"]),
        )

    if publish:
        _publish_verdict(s3_client, bucket, document)

    # I7392 item E — best-effort, independently trapped (matches
    # freshness-monitor's split-trap convention): a PutMetricData grant
    # regression must never suppress the verdict artifact already written
    # above, and a metric-emit failure must never be reported as a sweep
    # finding.
    if emit_metrics:
        try:
            _emit_metrics(cloudwatch_client, document)
        except Exception as exc:  # noqa: BLE001 — secondary observability
            logger.warning(
                "Stage-output CW metric emit failed (non-fatal): %s", exc,
                exc_info=True,
            )

    if alert and summary["status"] != "ok" and _should_alert(document):
        _alert(document)
    elif alert and summary["status"] != "ok":
        logger.info(
            "Stage-output alert SUPPRESSED: OBSERVE mode, run_mode=%s is not "
            "%r — findings are still in the published verdict artifact "
            "(alpha-engine-config-I7392)",
            run_mode, RUN_MODE_WEEKLY,
        )

    return document


def _should_alert(document: dict) -> bool:
    """I7392 item B: OBSERVE mode on a non-weekly run_mode publishes the
    verdict artifact and the metric, but nothing to the alert bus.

    ENFORCE mode always alerts on a finding — enforcement failing a run
    silently, with no alert explaining why, would be worse than the noise
    this suppresses. An UNKNOWN run_mode (``None``) never suppresses either:
    failing toward alerting matches every other honest-degradation rule in
    this module (entered_stages, cycle_date) — "I don't know the run_mode"
    must not become "assume it's safe to stay quiet".
    """
    if document.get("enforce"):
        return True
    run_mode = document.get("run_mode")
    if run_mode is None:
        return True
    return run_mode == RUN_MODE_WEEKLY


def _emit_metrics(cloudwatch_client: Any, document: dict) -> None:
    """I7392 item E: ``StageOutputDefects`` in ``AlphaEngine/Substrate``,
    dimensioned by Pipeline + RunMode (both bounded-cardinality — never the
    high-cardinality run id/date, per observability-policy §4).

    ``alpha-engine-dashboard-role`` already carries
    ``cloudwatch:PutMetricData`` scoped to the ``AlphaEngine``/``AlphaEngine/*``
    namespace condition (policy ``alpha-engine-cloudwatch-metrics``, measured
    2026-08-21) — this needs no new IAM grant.
    """
    if cloudwatch_client is None:
        import boto3  # noqa: PLC0415

        cloudwatch_client = boto3.client("cloudwatch")

    run_mode = document.get("run_mode") or "unknown"
    cloudwatch_client.put_metric_data(
        Namespace="AlphaEngine/Substrate",
        MetricData=[
            {
                "MetricName": "StageOutputDefects",
                "Dimensions": [
                    {"Name": "Pipeline", "Value": document["pipeline"]},
                    {"Name": "RunMode", "Value": run_mode},
                ],
                "Value": float(len(document["defects"])),
                "Unit": "Count",
            }
        ],
    )


def _publish_verdict(s3_client: Any, bucket: str, document: dict) -> None:
    """Write the verdict artifact.

    Failure to publish is logged and swallowed rather than raised — (a) the
    failure mode swallowed is an S3 write fault on the REPORTING path, (b) the
    primary deliverable (the alert and the exit code) has already been decided
    from the in-memory document and survives intact, (c) the recording surface
    is this ERROR log line plus the registry row on the verdict key, which goes
    stale and pages when this write stops landing. An exception here would
    convert a reporting fault into a pipeline failure — the reporter destroying
    the thing it reports, a class this fleet has already shipped twice.
    """
    key = VERDICT_KEY_TEMPLATE.format(
        pipeline=document["pipeline"], run_date=document["run_date"]
    )
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(document, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        logger.info("Stage-output verdict written to s3://%s/%s", bucket, key)
    except Exception as exc:  # noqa: BLE001 - see docstring (a)/(b)/(c)
        logger.error(
            "Stage-output verdict publish FAILED for s3://%s/%s: %s — the sweep "
            "verdict below is still authoritative, but the durable record of it "
            "is missing and the registry row on this key will go stale",
            bucket, key, exc,
        )


def _describe(findings: Sequence[dict]) -> str:
    preview = "; ".join(
        f"{f['stage']}→{f['artifact_id']} ({f['verdict']})"
        for f in findings[:_ALERT_PREVIEW_LIMIT]
    )
    if len(findings) > _ALERT_PREVIEW_LIMIT:
        preview += f" ... +{len(findings) - _ALERT_PREVIEW_LIMIT} more"
    return preview


def _alert(document: dict) -> None:
    """Publish the finding. Loud in observe mode — that is the whole design."""
    try:
        from nousergon_lib import alerts  # noqa: PLC0415
    except ImportError as exc:
        logger.warning(
            "Stage-output alert skipped — nousergon_lib.alerts unavailable: %s", exc
        )
        return

    defects = document["defects"]
    unmeasured = document["unmeasured"]
    mode = "ENFORCE" if document["enforce"] else "OBSERVE (exit 0 by design)"
    # I7392 item B: 97 was ARTIFACTS across 18 STAGES, not 97 stages — the
    # prior "{n} stage(s)" wording overstated the finding 5.4x on the
    # 2026-08-21 run. Report both numbers explicitly.
    defect_stage_count = len({f["stage"] for f in defects})

    parts = [
        f"Stage-output sweep [{mode}] {document['pipeline']} "
        f"run_date={document['run_date']}: "
        f"{len(defects)} artifact(s) across {defect_stage_count} stage(s) "
        f"produced NO output for this run, "
        f"{len(unmeasured)} could NOT be measured."
    ]
    if defects:
        parts.append(f"DEFECT (stage ran, key absent or predates the run): {_describe(defects)}.")
    if unmeasured:
        parts.append(
            f"UNMEASURED (the check could not answer — NOT evidence of health "
            f"and NOT evidence of a defect): {_describe(unmeasured)}."
        )
    if not document.get("entered_stages_known"):
        parts.append(
            "Entered-stage set unknown, so no stage was excused as skipped; a "
            "legitimately-skipped stage on a degraded run will appear here."
        )
    parts.append(f"Full verdict: s3://alpha-engine-research/"
                 f"{VERDICT_KEY_TEMPLATE.format(**{k: document[k] for k in ('pipeline', 'run_date')})}"
                 " (alpha-engine-config-I7167, I7392).")

    # I7392 item B: severity from the MAX finding severity, not the defect
    # COUNT — 82 warning-severity findings alone used to force "error" on the
    # 2026-08-21 run. "critical" (registry row severity) maps to alert
    # severity "error"; any other defect (or none) maps to "warn".
    severity = (
        "error"
        if any(f.get("severity") == "critical" for f in defects)
        else "warn"
    )
    try:
        result = alerts.publish(
            " ".join(parts),
            severity=severity,
            source="alpha-engine-data/validators/stage_output_sweep.py",
            dedup_key=f"stage_output_sweep_{document['pipeline']}_{document['run_date']}",
            dedup_window_min=720,
        )
        logger.info("Stage-output alert publish: any_ok=%s", result.any_ok)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Stage-output alert publish failed: %s", exc)


def exit_code(document: dict) -> int:
    """Map a verdict document to a process exit code.

    Observe mode is 0 for findings but NOT for an unreadable registry: that is
    a fault in the sweep itself, and a sweep that cannot run must not report
    the same exit status as a sweep that ran and found nothing.
    """
    if document["status"] == "registry_unreadable":
        return 2
    if not document["enforce"]:
        return 0
    if document["defects"]:
        return 1
    if document["unmeasured"]:
        return 2
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Stage-level output assertion for a scheduled SF run "
                    "(alpha-engine-config-I7167)",
    )
    parser.add_argument("--run-date", required=True,
                        help="SF-stamped $.run_date, e.g. 2026-08-08")
    parser.add_argument("--pipeline", default=DEFAULT_PIPELINE)
    parser.add_argument(
        "--execution-start", default=None,
        help="ISO-8601 execution start ($$.Execution.StartTime). Omitted ⇒ "
             "staleness is undecidable and every present key reports unmeasured",
    )
    parser.add_argument(
        "--entered-stage", action="append", default=None, dest="entered_stages",
        help="repeatable; a stage that entered this execution. Omitted entirely "
             "⇒ the entered set is UNKNOWN and no stage is excused as skipped",
    )
    parser.add_argument(
        "--execution-arn", default=None,
        help="this execution's ARN ($$.Execution.Id). Supplies BOTH the start "
             "time and the entered-stage set, so a gated-out run that did no "
             "work is not asserted against a full artifact set",
    )
    parser.add_argument(
        "--cycle-date", default=None,
        help="the date the registry's {date}/{trading_day} templates are keyed "
             "by. Defaults to krepis.dates.expected_last_close(run_date) — NOT "
             "run_date itself; see resolve_cycle_date()",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--registry", default=None,
                        help="local ARTIFACT_REGISTRY.yaml (default: the S3 mirror)")
    parser.add_argument(
        "--enforce", action="store_true",
        help="findings exit non-zero. Default is OBSERVE — see the module "
             "docstring on the I6891 degraded-run blast radius",
    )
    parser.add_argument("--no-alert", action="store_true")
    parser.add_argument("--no-publish", action="store_true",
                        help="skip writing the verdict artifact")
    parser.add_argument(
        "--no-metrics", action="store_true",
        help="skip emitting the StageOutputDefects CloudWatch metric "
             "(AlphaEngine/Substrate, alpha-engine-config-I7392)",
    )
    parser.add_argument(
        "--run-mode", default=None,
        help="override the run_mode classification (RUN_MODE_DRY='dry', "
             "'weekly', 'exercise', ...). Normally derived from "
             "--execution-arn's describe_execution input "
             "(alpha-engine-config-I7392); an explicit value wins over that "
             "read, same precedence as --execution-start",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    execution_start = None
    if args.execution_start:
        try:
            execution_start = parse_execution_start(args.execution_start)
        except ValueError as exc:
            logger.error(
                "--execution-start %r is not ISO-8601 (%s). Refusing to fall back "
                "to 'no window': a mis-typed timestamp would silently turn every "
                "assertion into unmeasured.",
                args.execution_start, exc,
            )
            return 2

    document = sweep(
        run_date=args.run_date,
        pipeline=args.pipeline,
        execution_start=execution_start,
        entered_stages=(
            frozenset(args.entered_stages) if args.entered_stages else None
        ),
        bucket=args.bucket,
        registry_path=args.registry,
        execution_arn=args.execution_arn,
        cycle_date=args.cycle_date,
        run_mode=args.run_mode,
        alert=not args.no_alert,
        publish=not args.no_publish,
        enforce=args.enforce,
        emit_metrics=not args.no_metrics,
    )

    code = exit_code(document)
    if document["status"] == "registry_unreadable":
        logger.error("Stage-output sweep COULD NOT RUN: %s", document["error"])
    elif document["defects"]:
        logger.error(
            "STAGE OUTPUT MISSING: %d declared artifact(s) absent or stale for "
            "run_date=%s: %s%s",
            len(document["defects"]), args.run_date,
            _describe(document["defects"]),
            "" if args.enforce else " [OBSERVE mode — exiting 0 by design]",
        )
    elif document["unmeasured"]:
        logger.error(
            "STAGE OUTPUT UNMEASURED: %d declared artifact(s) could not be "
            "checked for run_date=%s: %s",
            len(document["unmeasured"]), args.run_date,
            _describe(document["unmeasured"]),
        )
    else:
        logger.info(
            "Stage-output sweep OK: %d declared artifact(s) written by this run"
            "%s",
            document["checked"],
            (
                f" ({len(document['gated'])} gated row(s) excused by the "
                f"registry — see the verdict artifact)"
                if document.get("gated") else ""
            ),
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
