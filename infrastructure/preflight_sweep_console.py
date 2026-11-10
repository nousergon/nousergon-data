"""Console surface for the all-stage preflight sweep — one check-result
envelope per weekly-SF stage, plus a roll-up row for the sweep itself.

WHAT BRIAN ASKED FOR
--------------------
"a daily check that each aspect of the weekly sf is healthy ... and i can
review the check freshness via console."

So the deliverable is not "it runs and pages". It is a console the operator can
open and read, per stage: was this stage's preconditions checked, what was the
verdict, and HOW OLD is that verdict.

WHY ENVELOPES AND NOT A DESCRIPTOR ENUMERATION
-----------------------------------------------
``console-policy`` §2.6 says onboarding is one descriptor pointing at where the
facts already are, and §2.7 says a driver reads a source SHAPE. The fleet
check-result envelope — ``<prefix>/<check_id>/latest.json`` carrying ``status``,
``ran_at`` and ``cadence_minutes`` — is read by the existing ``checks-envelope``
adapter, which projects each key onto a **Component** row and derives staleness
natively from ``ran_at`` against ``cadence_minutes × staleness_factor``.

That gives the three freshness states for free, and — the load-bearing part —
keeps them from collapsing into each other:

* **MEASURED PASS** — ``status: ok`` with ``ran_at``. The console renders the
  verdict AND the age.
* **STALE** — the adapter overrides the status once the row is older than its
  own declared cadence. A four-day-old green renders STALE, not green. This is
  the exact failure that let a frozen entry feed trade silently behind an
  eight-day freshness window; it is not re-implemented here, it is inherited
  from the adapter that already computes it.
* **UNOBSERVED** — a stage that exists in the pipeline with no verdict. Made
  visible by writing an envelope for EVERY derived stage on EVERY run,
  including the stages the sweep could not exercise (``unsweepable``,
  ``unmeasured``). A stage therefore has no envelope only if it was never
  derived at all — which the roll-up row reports as ``stages_unobserved`` and
  which the deadman covers.

Zero console changes and zero enumeration: the denominator is the stage set
derived from the live SF definition each run, so a stage added to the pipeline
appears on the console by itself. Nothing here hand-lists a stage.

WHAT THIS SURFACE DOES AND DOES NOT CLAIM
------------------------------------------
COVERS: each stage's PRECONDITIONS — can it start, are its inputs present, is
its config resolvable, is its tool contract satisfied, does its box boot and
build.

DOES NOT COVER: whether the stage's OUTPUT is correct. That is a different
axis (``sf-pipeline-policy`` §2.3a), owned by the stage-output assertion
(``validators/stage_output_sweep.py``, nousergon-data#1359) and the numeric
self-tests. The ``summary`` field on every row says so in words, because a
reader who sees green here and infers "the weekly run will produce correct
numbers" has been misled by the surface rather than by the sweep.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, cast

# The fleet check-result envelope's own status vocabulary, read by the
# console's checks-envelope adapter. Anything outside it renders UNREPORTED —
# which is correct behaviour, and the reason this mapping is explicit rather
# than a str() of the internal verdict.
ENVELOPE_OK = "ok"
ENVELOPE_ATTENTION = "attention"
ENVELOPE_ERROR = "error"

CHECKS_PREFIX = os.environ.get("PREFLIGHT_SWEEP_CHECKS_PREFIX", "ops/checks")
ROLLUP_CHECK_ID = "ae-preflight-sweep"
STAGE_CHECK_ID_PREFIX = "ae-preflight-stage"

CADENCE_PATH = (
    Path(__file__).resolve().parent / "preflight_sweep_cadence.json"
)

_PRECONDITION_SCOPE = (
    "Scope: PRECONDITIONS only — can this stage start (box boots, repos clone, "
    "venv builds, config resolves, inputs present, tool contract satisfied). "
    "It says NOTHING about whether the stage's output is correct; that axis is "
    "the stage-output assertion (validators/stage_output_sweep.py), not this."
)

# verdict (preflight_sweep.py) -> envelope status. Deliberately not identity:
# the envelope vocabulary is a published contract with the console adapter.
_VERDICT_TO_STATUS = {
    "passed": ENVELOPE_OK,
    # A real finding about the pipeline.
    "failed": ENVELOPE_ERROR,
    # A coverage defect: the stage declares a dry path the sweep cannot
    # exercise. Loud, because an unsweepable stage is an unmonitored stage.
    # NOTE: an `unsweepable` row carrying unsweepable_kind="upstream_pending"
    # is downgraded to ATTENTION below — see _UNSWEEPABLE_KIND_TO_STATUS.
    "unsweepable": ENVELOPE_ERROR,
    # Could not measure. NOT a pass and NOT a defect — "attention" is the
    # envelope's honest middle, and principles.md §2.7 forbids rendering it
    # green.
    "unmeasured": ENVELOPE_ATTENTION,
    "not_attempted": ENVELOPE_ATTENTION,
    # A stage the definition declares with no dry path at all, acknowledged in
    # preflight_sweep_manifest.json with a written reason. It is NOT green: the
    # sweep asserts nothing whatever about it, and a row reading `ok` would
    # claim precondition-health nobody measured. It is not an error either —
    # the gap is declared and reviewed. ATTENTION is the honest rendering, and
    # it is why the row exists at all rather than the stage being absent from
    # the console (alpha-engine-config#7324).
    "no_dry_path": ENVELOPE_ATTENTION,
    # A stage a ruling RETIRED (manifest `retired_stages`): declared, dated and
    # not run. Not green -- nothing measures it -- and not an error: the gap is a
    # reviewed decision, and a past re_exam date raises its own blocking finding.
    # alpha-engine-config-I13374.
    "retired": ENVELOPE_ATTENTION,
}

# An `unsweepable` stage has two very different causes, and the console must
# not render them identically. A coverage defect (missing launcher, missing
# --preflight-only branch, unrenderable command) means a stage that SHOULD be
# monitored is not: error. An unmet same-day upstream means the stage cannot be
# exercised until its producer has run for real — the ordinary state on ~6 days
# of any week: attention, never error, never green (alpha-engine-config#7323).
# A third cause: the stage is preflight-capable but runs on a DEDICATED box the
# sweep does not have and cannot become, declared per stage in the manifest's
# `dedicated_box_stages`. Nothing is broken and nothing is monitored either, so
# it renders exactly like the acknowledged no-dry-path case — ATTENTION, never
# error, never green (alpha-engine-config-I9329).
_UNSWEEPABLE_KIND_TO_STATUS = {
    "coverage_defect": ENVELOPE_ERROR,
    "upstream_pending": ENVELOPE_ATTENTION,
    "dedicated_box": ENVELOPE_ATTENTION,
}


def is_blocking_finding(finding: Any) -> bool:
    """Does this coverage finding mean the sweep's own denominator is WRONG?

    Findings carry their own severity because two different things live in the
    same list: a denominator defect (a stage silently lost its dry path — the
    sweep is measuring the wrong pipeline) and an acknowledged coverage gap (a
    stage that has no dry path, written down with a reason). Both must be NAMED
    on every surface; only the first may fail the run.

    A bare string is treated as blocking. That is the legacy shape written by
    ``preflight_sweep.sh``'s could-not-measure payload and by any report
    predating the structured form, and defaulting an unknown shape to blocking
    keeps the fail-loud direction.
    """
    if isinstance(finding, dict):
        return bool(finding.get("blocking", True))
    return True


def finding_text(finding: Any) -> str:
    """The human-readable body of a coverage finding, either shape."""
    if isinstance(finding, dict):
        return str(finding.get("finding", finding))
    return str(finding)


class CadenceError(RuntimeError):
    """The declared cadence could not be read. Raised, never defaulted."""


def load_cadence(path: Path = CADENCE_PATH) -> dict[str, Any]:
    """The single declared cadence parameter.

    Never defaults. A component that guesses its own cadence produces a
    freshness verdict nobody declared, and a staleness threshold invented at
    runtime is indistinguishable on the surface from one that was ruled.
    """
    try:
        manifest = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CadenceError(f"could not read the cadence declaration at {path}: {exc}") from exc
    for field in ("sweep_cadence", "cadence_minutes", "staleness_factor"):
        if field not in manifest:
            raise CadenceError(f"{path} is missing the required field {field!r}")
    if manifest["sweep_cadence"] not in manifest.get("allowed_values", []):
        raise CadenceError(
            f"{path} declares sweep_cadence={manifest['sweep_cadence']!r}, which is "
            f"not in allowed_values {manifest.get('allowed_values')}"
        )
    return manifest


def stage_check_id(stage_name: str) -> str:
    """The console component id for one stage's precondition row.

    Derived from the stage's own qualified name in the definition, so a stage
    added inside a Parallel branch gets a stable, unique id with no mapping
    table to maintain.
    """
    slug = stage_name.replace(".", "-")
    return f"{STAGE_CHECK_ID_PREFIX}-{slug}"


def stage_envelope(
    result: dict[str, Any],
    run_id: str,
    ran_at: str,
    cadence: dict[str, Any],
) -> dict[str, Any]:
    """One stage's check-result envelope."""
    verdict = result.get("verdict", "")
    status = _VERDICT_TO_STATUS.get(verdict)
    if status is None:
        # An internal verdict with no declared envelope mapping. Rendering it
        # as "ok" would be the worst available default, and dropping the row
        # would make the stage silently disappear from the denominator. Both
        # are forbidden; an unmapped verdict is reported as attention and
        # names itself so the gap is visible rather than absorbed.
        status = ENVELOPE_ATTENTION
        verdict = f"{verdict or 'missing'} (no declared envelope mapping)"

    kind = result.get("unsweepable_kind")
    if verdict == "unsweepable" and kind is not None:
        # An unrecognised kind keeps the ERROR default: a new kind must be
        # added by PR here, not absorbed into the quieter status.
        status = _UNSWEEPABLE_KIND_TO_STATUS.get(kind, ENVELOPE_ERROR)

    return {
        "check_id": stage_check_id(result["stage"]),
        "status": status,
        "ran_at": ran_at,
        "cadence_minutes": cadence["cadence_minutes"],
        "component_id": stage_check_id(result["stage"]),
        "stage": result["stage"],
        "verdict": verdict,
        "unsweepable_kind": kind,
        "upstream": result.get("upstream"),
        "acknowledged_reason": result.get("acknowledged_reason"),
        "repo": result.get("repo"),
        "launcher": result.get("launcher"),
        "returncode": result.get("returncode"),
        "duration_seconds": result.get("duration_seconds"),
        "reason": result.get("reason"),
        "last_stderr_line": result.get("last_stderr_line"),
        "run_id": run_id,
        "summary": (
            f"weekly-SF stage {result['stage']} — preflight verdict: {verdict}. "
            + _PRECONDITION_SCOPE
        ),
        "evidence": result.get("log_key"),
        "schema_version": 1,
    }


def rollup_envelope(
    report: dict[str, Any], cadence: dict[str, Any], ran_at: str
) -> dict[str, Any]:
    """The sweep's own row: coverage, not any one stage's verdict.

    This is where ``stages_unobserved`` lives — stages the definition declares
    and the sweep produced no verdict for. It is published even when it is
    zero, because a coverage number that only appears when it is non-zero is a
    number nobody can tell the absence of from the presence of health.
    """
    declared = int(report.get("stages_declared", 0))
    no_dry_path = int(report.get("stages_no_dry_path", 0))
    retired = int(report.get("stages_retired", 0) or 0)
    # The denominator the sweep is accountable for: every stage the definition
    # declares, MINUS the ones acknowledged in preflight_sweep_manifest.json as
    # having no dry path at all. That subtraction is the only one permitted,
    # it is written down with a reason per stage, and CI fails if the
    # acknowledged set and the definition disagree in either direction — so it
    # cannot be used to quietly shrink what is being counted.
    expected = max(declared - no_dry_path, 0)
    results = report.get("results", []) or []
    # Stages the sweep is accountable for AND produced a verdict for. A
    # `no_dry_path` row is a verdict, but it is not coverage — counting it here
    # would let the acknowledged gaps cancel out the stages they represent and
    # drive `stages_unobserved` to zero while nothing was exercised.
    reported = len([r for r in results if r.get("verdict") != "no_dry_path"])
    unobserved = max(expected - reported, 0)
    findings = report.get("coverage_findings", []) or []
    blocking = [f for f in findings if is_blocking_finding(f)]
    persistent = report.get("persistent_unsweepable_findings", []) or []
    coverage_defects = int(report.get("stages_unsweepable_coverage_defect", 0) or 0)
    upstream_pending = int(report.get("stages_unsweepable_upstream_pending", 0) or 0)
    dedicated_box = int(report.get("stages_unsweepable_dedicated_box", 0) or 0)
    if (
        not report.get("stages_unsweepable_coverage_defect")
        and not upstream_pending
        and not dedicated_box
    ):
        # A report predating the kind split (or the shell's could-not-measure
        # payload) carries only the total. Attribute it to the loud kind rather
        # than assume the quiet one.
        coverage_defects = int(report.get("stages_unsweepable", 0) or 0)

    if not report.get("measured", False):
        status = ENVELOPE_ERROR
    elif (
        report.get("stages_failed")
        or coverage_defects
        or blocking
        or persistent
        or unobserved
    ):
        status = ENVELOPE_ERROR
    elif report.get("stages_unmeasured") or upstream_pending or dedicated_box or no_dry_path or retired:
        # Nothing is broken, but the sweep did not cover everything it declares.
        # Rendering that green is the failure mode this component exists to
        # avoid: "no data" is never green (principles.md §2.7).
        status = ENVELOPE_ATTENTION
    else:
        status = ENVELOPE_OK

    return {
        "check_id": ROLLUP_CHECK_ID,
        "component_id": ROLLUP_CHECK_ID,
        "status": status,
        "ran_at": ran_at,
        "cadence_minutes": cadence["cadence_minutes"],
        "declared_cadence": cadence["sweep_cadence"],
        "run_id": report.get("run_id"),
        "outcome": report.get("outcome"),
        "measured": report.get("measured", False),
        "unmeasured_reason": report.get("unmeasured_reason"),
        "stages_declared": declared,
        "stages_expected": expected,
        "stages_reported": reported,
        "stages_unobserved": unobserved,
        "stages_passed": report.get("stages_passed", 0),
        "stages_failed": report.get("stages_failed", 0),
        "stages_unmeasured": report.get("stages_unmeasured", 0),
        "stages_unsweepable": report.get("stages_unsweepable", 0),
        "stages_unsweepable_coverage_defect": coverage_defects,
        "stages_unsweepable_upstream_pending": upstream_pending,
        "stages_unsweepable_dedicated_box": dedicated_box,
        "stages_no_dry_path": no_dry_path,
        "stages_retired": retired,
        "retired_stages": [
            r.get("stage")
            for r in cast("list[dict[str, object]]", results)
            if r.get("verdict") == "retired"
        ],
        "stages_not_attempted": report.get("stages_not_attempted", 0),
        # Named, not counted: which stages have no dry path, and which have
        # been unsweepable long enough to be a finding in their own right.
        "no_dry_path_stages": [
            r.get("stage") for r in results if r.get("verdict") == "no_dry_path"
        ],
        "upstream_pending_stages": [
            r.get("stage")
            for r in results
            if r.get("verdict") == "unsweepable"
            and r.get("unsweepable_kind") == "upstream_pending"
        ],
        "dedicated_box_stages": [
            r.get("stage")
            for r in results
            if r.get("verdict") == "unsweepable"
            and r.get("unsweepable_kind") == "dedicated_box"
        ],
        "coverage_findings": findings,
        "coverage_findings_blocking": len(blocking),
        "persistent_unsweepable_findings": persistent,
        "unsweepable_streak_state": report.get("unsweepable_streak_state", "unavailable"),
        "declared_stages": report.get("declared_stages", []),
        "definition_sha": report.get("definition_sha"),
        "summary": (
            f"All-stage preflight sweep: {report.get('stages_passed', 0)} passed, "
            f"{report.get('stages_failed', 0)} failed, "
            f"{report.get('stages_unmeasured', 0)} unmeasured, "
            f"{coverage_defects} unsweepable (coverage defect), "
            f"{upstream_pending} awaiting upstream, "
            f"{dedicated_box} on a dedicated box the sweep cannot reach, "
            f"{no_dry_path} with no dry path, "
            f"{retired} retired, "
            f"{unobserved} unobserved of {expected} expected "
            f"({declared} declared, {no_dry_path} acknowledged as having no dry path). "
            + _PRECONDITION_SCOPE
        ),
        "evidence": f"_preflight_sweep/{report.get('run_id')}/report.json",
        "schema_version": 1,
    }


def envelopes(
    report: dict[str, Any], cadence: dict[str, Any], now: dt.datetime | None = None
) -> list[tuple[str, dict[str, Any]]]:
    """Every console object this run publishes, as ``(s3_key, body)`` pairs.

    One row per stage plus the roll-up. Written on EVERY run, including a run
    that measured nothing: an envelope that stops being republished is what
    makes the console render STALE, and skipping the write on a bad run would
    turn a loud failure into a quiet ageing row.
    """
    ran_at = (now or dt.datetime.now(dt.timezone.utc)).isoformat()
    run_id = str(report.get("run_id", "unknown"))
    out: list[tuple[str, dict[str, Any]]] = []
    for result in report.get("results", []):
        body = stage_envelope(result, run_id, ran_at, cadence)
        out.append((f"{CHECKS_PREFIX}/{body['check_id']}/latest.json", body))
    rollup = rollup_envelope(report, cadence, ran_at)
    out.append((f"{CHECKS_PREFIX}/{rollup['check_id']}/latest.json", rollup))
    return out
