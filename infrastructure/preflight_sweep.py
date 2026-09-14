"""Nightly all-stage ``--preflight-only`` sweep for the weekly freshness
pipeline — the driver.

Runs ON the shared launcher box (the same box, built by the same bootstrap,
that ``ne-weekly-freshness-pipeline`` itself runs its stages from). For every
stage the definition declares preflight-capable it runs that stage's OWN
command with ``--preflight-only``, INDEPENDENTLY, continuing past failures,
and reports the whole per-stage matrix in one pass.

WHAT MAKES THIS DIFFERENT FROM THE FRIDAY SHELL-RUN
---------------------------------------------------
``shell_run=true`` already threads ``--preflight-only`` into every stage — but
through the SF, which is a fail-fast chain. The first failing stage aborts the
execution, so a shell-run yields exactly ONE root cause per run. Sixteen
consecutive weekly executions since 2026-08-10 failed with sixteen DIFFERENT
root causes, and 2026-08-10 alone burned nine reruns converging on none of
them. Serial discovery of independent defects is the pathology; a
non-short-circuiting fan-out is the fix, and the SF cannot be that without a
``complexity:ultra`` topology change (``sf-pipeline-policy`` §5).

HONEST DEGRADATION (``sf-pipeline-policy`` §2.3, ``principles.md`` §2.7)
-----------------------------------------------------------------------
Three outcomes are kept distinct at every level, and no data is never rendered
as a pass:

* ``passed``      — the stage's preflight ran and exited 0.
* ``failed``      — the stage's preflight ran and exited non-zero. Real finding.
* ``unmeasured``  — the sweep could not run it, or could not read the result.
                    NOT a pass and NOT a defect. The verdict vocabulary mirrors
                    ``validators/stage_output_sweep.py`` (I7167) deliberately
                    rather than inventing a parallel one.
* ``unsweepable`` — the stage could not be exercised. THREE kinds, kept
                    apart because one is a defect and the others are Tuesday:
                    ``coverage_defect`` (launcher missing, flag unimplemented,
                    command unrenderable) fails the run and pages;
                    ``upstream_pending`` (the stage reads a same-day artifact a
                    preceding stage produces, and that stage has not run for
                    real today) does neither; ``dedicated_box`` (the stage is
                    preflight-capable but runs on a box its own dispatcher
                    creates, which the sweep's launcher box is not and cannot
                    become) does neither either. The second and third are both
                    DECLARED per stage in ``preflight_sweep_manifest.json``:
                    ``upstream_pending`` is still probed on the day, so a
                    reworded launcher error cannot reclassify a real failure,
                    and ``dedicated_box`` is graded both ways by
                    ``manifest_disagreement`` so an acknowledgement cannot
                    outlive its cause. A declaration alone never hides a defect,
                    and an acknowledged stage is NAMED on every surface — it is
                    an acknowledgement, never a silent exclusion.
* ``no_dry_path`` — the stage threads no ``$.preflight_args`` and is never
                    exercised at all. A verdict ROW, not just a counter: a count
                    published without its members is unactionable.
* ``not_attempted`` — the run produced no verdict for a declared stage. The
                    invariant ``len(results) == stages_declared`` holds on every
                    report, and a stage that would have broken it is filled in
                    here AND raised as a blocking coverage finding.

A run that could not measure anything terminates ``failed`` with
``measured: false`` and a reason. It never writes the clean-run pointer.

The success-claim asymmetry (``observability-policy`` §3.1) is explicit here:
every run writes its full report, but only an all-``passed`` run advances
``_preflight_sweep/last_clean.json``. The failure path persists the work and
the cause of death; it never advances the artifact a detector reads as proof.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import subprocess
import sys
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from infrastructure.preflight_sweep_stages import (  # noqa: E402
    NO_DRY_PATH,
    SWEEPABLE,
    UNSWEEPABLE,
    Stage,
    apply_map_bindings,
    derive_required_map_bindings,
    derive_shell_run_bindings,
    derive_stages,
    load_manifest,
    manifest_disagreement,
    map_binding_disagreement,
    upstream_dependencies,
    upstream_dependency_disagreement,
)
from infrastructure.preflight_sweep_console import (  # noqa: E402
    envelopes,
    finding_text as _finding_text,
    is_blocking_finding as _is_blocking,
    load_cadence,
)

# ── Verdict vocabulary (closed; mirrors validators/stage_output_sweep.py) ────
PASSED = "passed"
FAILED = "failed"
UNMEASURED = "unmeasured"
UNSWEEPABLE_VERDICT = "unsweepable"
NO_DRY_PATH_VERDICT = "no_dry_path"
NOT_ATTEMPTED = "not_attempted"

# ── Why a stage is unsweepable (closed; add by PR) ───────────────────────────
# The distinction is load-bearing, not cosmetic. One of these is a defect in
# the sweep's own coverage and MUST page; the other is the ordinary state of a
# stage whose upstream has not run today and MUST NOT. Collapsing them is how
# the first sweep run emailed "2 failed" on a structurally unmeasurable pair.
UNSWEEPABLE_COVERAGE_DEFECT = "coverage_defect"
UNSWEEPABLE_UPSTREAM_PENDING = "upstream_pending"
# The stage threads $.preflight_args and its launcher DOES implement
# --preflight-only, but it runs on a DEDICATED box that its own dispatcher
# bootstraps; the sweep's launcher box does not carry that checkout or its venv
# and never will, so the stage is structurally unreachable from here however
# healthy it is. Declared per stage in `dedicated_box_stages`, graded both ways
# by manifest_disagreement, and named on every surface as a non-blocking
# coverage finding. Not a defect in the sweep's denominator; not coverage
# either. alpha-engine-config-I9329.
UNSWEEPABLE_DEDICATED_BOX = "dedicated_box"

# ── Coverage-finding kinds (closed; add by PR) ───────────────────────────────
# Every finding carries its own `blocking` flag rather than deriving severity
# from which list it landed in: an acknowledged no-dry-path stage is a real
# coverage gap that must be NAMED on every surface, and is not a reason to fail
# the run — an integer that only says how many is a count published without its
# members (alpha-engine-config#7324).
FINDING_MANIFEST_DISAGREEMENT = "manifest_disagreement"
FINDING_MAP_BINDING_DISAGREEMENT = "map_binding_disagreement"
FINDING_UPSTREAM_DECLARATION = "upstream_declaration"
FINDING_NO_DRY_PATH = "no_dry_path"
FINDING_DEDICATED_BOX = "dedicated_box"
FINDING_MISSING_VERDICT = "missing_verdict"

STREAK_STATE_KEY_SUFFIX = "unsweepable_streaks.json"

# Run-level outcome, from observability-policy §3.1's closed vocabulary.
OUTCOME_OK = "ok"
OUTCOME_DEGRADED = "degraded"
OUTCOME_FAILED = "failed"

COMPONENT_ID = "ae-preflight-sweep"
DEFAULT_BUCKET = os.environ.get("PREFLIGHT_SWEEP_BUCKET", "alpha-engine-research")
REPORT_PREFIX = "_preflight_sweep"
CW_NAMESPACE = "AlphaEngine"
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
DEFAULT_SNS_TOPIC = os.environ.get(
    "PREFLIGHT_SWEEP_SNS_TOPIC",
    "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
)
# Bounded fan-out: each stage's --preflight-only launches its OWN nested spot,
# so concurrency is a spot-API and capacity-pool decision, not a CPU one. Six
# keeps RunInstances well inside throttle limits and spreads across the six
# subnets the launchers rotate through.
DEFAULT_CONCURRENCY = int(os.environ.get("PREFLIGHT_SWEEP_CONCURRENCY", "6"))
# Per-stage ceiling. A --preflight-only run is boot + deps + smoke: minutes.
# 30 min is generous headroom over the observed worst case without letting a
# hung stage hold the whole sweep to its own timeout.
DEFAULT_STAGE_TIMEOUT = int(os.environ.get("PREFLIGHT_SWEEP_STAGE_TIMEOUT", "1800"))
# Same rationale as unsweepable_streak_threshold_days: one full weekly cycle
# (the backtest chain writes once, on Saturday) plus one day of slack. Used
# only when a declaration omits its own `max_age_days` — every entry in the
# manifest's `upstream_artifact_dependencies` is expected to declare one.
DEFAULT_UPSTREAM_MAX_AGE_DAYS = 8


@dataclass
class StageResult:
    stage: str
    verdict: str
    repo: str | None = None
    launcher: str | None = None
    returncode: int | None = None
    duration_seconds: float | None = None
    reason: str | None = None
    last_stderr_line: str | None = None
    log_key: str | None = None
    # Set only when verdict == UNSWEEPABLE_VERDICT. Which of the two kinds it
    # is decides whether the run fails and whether the console row is an error.
    unsweepable_kind: str | None = None
    # The declared upstream dependency that produced an `upstream_pending`
    # verdict, echoed onto the row so the operator reads the unmet prefix and
    # its producing stage without opening the manifest.
    upstream: dict[str, Any] | None = None
    # For a `no_dry_path` row: the written acknowledgement from the manifest.
    acknowledged_reason: str | None = None
    # Set only when a same-day `upstream_pending` stage was actually
    # re-measured against an earlier, populated instance of its declared
    # upstream (alpha-engine-config-I10717 (2)). The run_date this row's
    # verdict was ACTUALLY measured against, when it differs from the
    # sweep's own calendar/probe date. A row with this set is PASSED/FAILED
    # for real, never `unsweepable` — remeasurement only ever turns a
    # not-measured stage into a measured one, never the reverse.
    measured_against_run_date: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SweepReport:
    component_id: str
    run_id: str
    started_at: str
    finished_at: str | None = None
    outcome: str = OUTCOME_FAILED
    measured: bool = False
    unmeasured_reason: str | None = None
    definition_sha: str | None = None
    checkout_root: str | None = None
    stages_declared: int = 0
    stages_swept: int = 0
    stages_passed: int = 0
    stages_failed: int = 0
    stages_unmeasured: int = 0
    stages_unsweepable: int = 0
    # The two kinds of unsweepable, published separately and both at zero:
    # a coverage defect fails the run, an unmet same-day upstream does not.
    stages_unsweepable_coverage_defect: int = 0
    stages_unsweepable_upstream_pending: int = 0
    # Acknowledged dedicated-box stages: unreachable from the sweep's own box by
    # construction, declared in `dedicated_box_stages`. Published as its own
    # counter rather than folded into either of the two above, so the number of
    # stages the sweep has DECLARED it cannot reach is readable on its own and
    # cannot grow without the declaration growing with it.
    stages_unsweepable_dedicated_box: int = 0
    stages_no_dry_path: int = 0
    # The subset of `stages_no_dry_path` with NO written acknowledgement in
    # preflight_sweep_manifest.json. The distinction decides whether the run
    # can ever be clean: a RATIFIED no-dry-path stage is a reviewed, permanent
    # boundary of the sweep's denominator, so gating `clean` on it made
    # `outcome: ok` structurally unreachable and `last_clean.json` unwritable
    # for the sweep's whole life (13 of 13 runs, 2026-08-14..2026-08-26). An
    # UNACKNOWLEDGED one is a real gap and still keeps the run out of clean --
    # it also raises its own blocking manifest_disagreement finding, so the
    # protection is not carried by this counter alone.
    stages_no_dry_path_unacknowledged: int = 0
    stages_not_attempted: int = 0
    coverage_findings: list[dict[str, Any]] = field(default_factory=list)
    # A stage unsweepable on EVERY run for the declared streak threshold is its
    # own finding: the sweep covers nothing there. Kept OUT of coverage_findings
    # deliberately so it can never be read as coverage (I7323 deliverable 3).
    persistent_unsweepable_findings: list[dict[str, Any]] = field(default_factory=list)
    unsweepable_streak_state: str = "unavailable"
    unsweepable_streak_threshold_runs: int | None = None
    # Every stage the definition declares, with its classification — so
    # `declared - swept` is auditable from the report alone rather than being a
    # scalar nobody can expand (I7324 deliverable 2).
    declared_stages: list[dict[str, Any]] = field(default_factory=list)
    results: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── AWS side-effects, isolated so the driver is unit-testable ────────────────


class AwsSurface:
    """Every outward effect the sweep has. Injected so the driver can be
    tested without touching S3, CloudWatch or SNS."""

    def __init__(self, region: str = DEFAULT_REGION, bucket: str = DEFAULT_BUCKET):
        self.region = region
        self.bucket = bucket
        self._s3 = None
        self._cw = None
        self._sns = None

    def _client(self, name):
        import boto3

        return boto3.client(name, region_name=self.region)

    def put_json(self, key: str, payload: dict) -> None:
        if self._s3 is None:
            self._s3 = self._client("s3")
        self._s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )

    def get_json(self, key: str) -> dict | None:
        """Read a JSON object, or ``None`` if it does not exist yet.

        Only a genuine absence returns ``None``. Any other error propagates —
        an unreadable streak file must not be silently treated as "no history",
        which would reset every streak to zero on each run and make the
        persistent-unsweepable finding structurally unable to fire.
        """
        if self._s3 is None:
            self._s3 = self._client("s3")
        try:
            body = self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None
        return json.loads(body)

    def list_objects(self, prefix: str, max_keys: int = 1000) -> list[dict[str, Any]]:
        """``[{"Key": ..., "Size": ...}]`` under ``prefix``. Never swallows."""
        if self._s3 is None:
            self._s3 = self._client("s3")
        out: list[dict[str, Any]] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                out.append({"Key": obj["Key"], "Size": obj.get("Size", 0)})
                if len(out) >= max_keys:
                    return out
        return out

    def put_text(self, key: str, body: str) -> None:
        if self._s3 is None:
            self._s3 = self._client("s3")
        self._s3.put_object(
            Bucket=self.bucket, Key=key, Body=body.encode(errors="replace"),
            ContentType="text/plain",
        )

    def put_metrics(self, metrics: list[tuple[str, float]]) -> None:
        if self._cw is None:
            self._cw = self._client("cloudwatch")
        self._cw.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[
                {
                    "MetricName": name,
                    # Bounded dimension set only (observability-policy §4):
                    # the run id is high-cardinality and lives in the record,
                    # never in a metric dimension.
                    "Dimensions": [{"Name": "Component", "Value": COMPONENT_ID}],
                    "Value": float(value),
                    "Unit": "Count",
                }
                for name, value in metrics
            ],
        )

    def publish(self, topic_arn: str, subject: str, message: str) -> None:
        if self._sns is None:
            self._sns = self._client("sns")
        self._sns.publish(TopicArn=topic_arn, Subject=subject[:100], Message=message)


# ── Stage execution ──────────────────────────────────────────────────────────


def _stage_script(stage: Stage) -> str:
    """The shell body for one stage: exactly the commands the SF would send.

    Rendered from the definition, so the sweep runs the pipeline's own command
    rather than a re-implementation of it that can drift.
    """
    return "set -eo pipefail\n" + "\n".join(stage.commands) + "\n"


def run_stage(
    stage: Stage,
    checkout_root: str,
    timeout: int,
    runner=subprocess.run,
) -> StageResult:
    """Execute one stage's dry command. Never raises: a stage that could not be
    run is reported ``unmeasured``, which is neither a pass nor a defect."""
    script = _stage_script(stage)
    started = dt.datetime.now(dt.timezone.utc)
    try:
        proc = runner(
            ["bash", "-c", script],
            cwd=os.path.join(checkout_root, stage.box_dir or ""),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        # A timeout is a HARD FAIL, named as a resource kill in what the
        # operator reads — never retried on an unchanged workload, never
        # folded into a generic non-zero exit (Brian ruling 2026-08-13).
        return StageResult(
            stage=stage.name,
            verdict=FAILED,
            repo=stage.repo,
            launcher=stage.launcher,
            returncode=None,
            duration_seconds=(
                dt.datetime.now(dt.timezone.utc) - started
            ).total_seconds(),
            reason=(
                f"RESOURCE KILL — preflight exceeded the sweep's {timeout}s per-stage "
                "ceiling and was terminated. Not retried."
            ),
        )
    except OSError as exc:
        # The sweep could not start the process at all. That is a fact about
        # the sweep's own harness, not about the stage — reporting it as a
        # stage failure would be a detector reporting its harness fault as a
        # finding, always in the alarming direction.
        return StageResult(
            stage=stage.name,
            verdict=UNMEASURED,
            repo=stage.repo,
            launcher=stage.launcher,
            reason=f"sweep could not launch the stage command: {exc}",
        )

    duration = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
    stderr_lines = [ln for ln in (proc.stderr or "").splitlines() if ln.strip()]
    last_err = stderr_lines[-1][:500] if stderr_lines else None
    verdict = PASSED if proc.returncode == 0 else FAILED
    reason = None
    if proc.returncode != 0:
        if proc.returncode == 137:
            reason = "RESOURCE KILL — rc=137 (OOM). Not retried."
        else:
            reason = f"preflight exited rc={proc.returncode}"
    return StageResult(
        stage=stage.name,
        verdict=verdict,
        repo=stage.repo,
        launcher=stage.launcher,
        returncode=proc.returncode,
        duration_seconds=duration,
        reason=reason,
        last_stderr_line=last_err,
    )


def _stage_log(stage: Stage, result: StageResult, script: str) -> str:
    return (
        f"stage: {stage.name}\nrepo: {stage.repo}\nlauncher: {stage.launcher}\n"
        f"verdict: {result.verdict}\nreturncode: {result.returncode}\n"
        f"reason: {result.reason}\n\n--- command ---\n{script}\n"
    )


# ── Declared same-day upstream dependencies ──────────────────────────────────


def _finding(kind: str, detail: str, *, blocking: bool, stage: str | None = None) -> dict:
    """One coverage finding, carrying its own severity.

    A finding that has to be looked up in a severity table somewhere else is a
    finding whose severity drifts from its text.
    """
    return {"kind": kind, "stage": stage, "blocking": blocking, "finding": detail}


# ── Probe-date normalization (closed vocabulary; add by PR) ──────────────────
# The sweep binds `run_date` to its own UTC calendar date, exactly as the SF's
# InitializeInput does. The LAUNCHERS do not use that value directly: every
# crucible-backtester stage calls `spot_common_normalize_run_date`
# (infrastructure/_spot_common.sh), which snaps RUN_DATE back to the NYSE
# trading day before its stage preflight reads
# `backtest/${RUN_DATE}/`. On any Saturday, Sunday or market holiday the stage
# therefore probes a DIFFERENT prefix than the sweep does, and the manifest's
# stated safety property — "a stage that fails while its upstream prefix IS
# populated stays FAILED" — is void: the sweep would find the (always empty)
# calendar-dated prefix and downgrade a REAL failure to upstream_pending, which
# does not page. Measured on 2026-08-22 and 2026-08-23, where the stage read
# the populated backtest/2026-08-21/ while the sweep's probe named an empty
# backtest/2026-08-2{2,3}/.
#
# So the normalization is DECLARED per dependency and applied to the probe. It
# is never guessed from the launcher, and it never falls back to the calendar
# date when it cannot be computed — falling back is the bug.
NORMALIZE_NONE = "none"
NORMALIZE_NYSE_TRADING_DAY = "nyse_trading_day"
DATE_NORMALIZATIONS = (NORMALIZE_NONE, NORMALIZE_NYSE_TRADING_DAY)


class DateNormalizationUnavailable(Exception):
    """The declared probe-date normalization could not be computed.

    Raised rather than degraded to the calendar date: a probe naming a prefix
    the stage never read is worse than no probe, because it silently reclassifies
    real failures.
    """


def normalize_probe_date(run_date: str, mode: str) -> str:
    """Resolve the date the STAGE will have used, from the sweep's binding.

    ``none`` returns the binding unchanged. ``nyse_trading_day`` routes through
    the same ``nousergon_lib.trading_calendar`` chokepoint
    ``spot_common_normalize_run_date`` uses, so the sweep and the launcher can
    never name different prefixes.
    """
    if mode == NORMALIZE_NONE:
        return run_date
    if mode != NORMALIZE_NYSE_TRADING_DAY:
        raise DateNormalizationUnavailable(
            f"unknown date_normalization {mode!r} — expected one of "
            f"{', '.join(DATE_NORMALIZATIONS)}"
        )
    try:
        from nousergon_lib import trading_calendar as tc  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        raise DateNormalizationUnavailable(
            f"nousergon_lib.trading_calendar is not importable ({type(exc).__name__}: "
            f"{exc}), so the NYSE trading day the launcher normalizes to cannot be "
            "computed"
        ) from exc
    try:
        day = dt.date.fromisoformat(run_date[:10])
        return (day if tc.is_trading_day(day) else tc.previous_trading_day(day)).isoformat()
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        raise DateNormalizationUnavailable(
            f"normalizing {run_date!r} to the NYSE trading day raised "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def _probe_bindings(bindings: dict[str, Any], mode: str) -> dict[str, Any]:
    """``bindings`` with ``run_date`` moved to the date the stage actually used."""
    if mode == NORMALIZE_NONE:
        return bindings
    run_date = bindings.get("run_date")
    if not run_date:
        raise DateNormalizationUnavailable(
            "the sweep has no run_date binding to normalize"
        )
    return {**bindings, "run_date": normalize_probe_date(str(run_date), mode)}


def upstream_content(
    prefix: str, ignore_subprefixes: list[str], lister
) -> tuple[bool, dict[str, Any]]:
    """Is the declared upstream prefix populated with real content?

    CONTENT, never existence. ``backtest/<date>/`` EXISTS on a day nothing
    produced it, because the sweep's own phase markers are written under it —
    a probe that tested existence would report every stage's upstream as
    satisfied and would therefore change nothing while appearing to work. Keys
    under a declared ignore sub-prefix, and zero-byte keys, are not content.
    """
    objects = lister(prefix)
    counted: list[str] = []
    ignored: list[str] = []
    for obj in objects:
        key = obj.get("Key", "")
        rel = key[len(prefix):] if key.startswith(prefix) else key
        if any(rel.startswith(p) for p in ignore_subprefixes):
            ignored.append(key)
            continue
        if int(obj.get("Size", 0) or 0) == 0:
            ignored.append(key)
            continue
        counted.append(key)
    detail = {
        "prefix": prefix,
        "content_keys": len(counted),
        "ignored_keys": len(ignored),
        "ignore_subprefixes": list(ignore_subprefixes),
        "sample": sorted(counted)[:3] or sorted(ignored)[:3],
    }
    return bool(counted), detail


def classify_upstream(
    result: StageResult, declaration: dict[str, Any], bindings: dict[str, Any], lister
) -> StageResult:
    """Reclassify a FAILED stage whose declared same-day upstream is absent.

    Applies only to a stage that RAN and FAILED and carries a declaration in
    ``preflight_sweep_manifest.json``. Three outcomes, all explicit:

    * upstream prefix has content  -> stays ``failed``, and says the upstream
      was present, which is what keeps a real defect from hiding behind the
      declaration;
    * upstream prefix is empty     -> ``unsweepable`` / ``upstream_pending``,
      naming the producing stage and the prefix. Not a failure, does not page;
    * the probe itself could not run, or the declared probe-date normalization
      could not be computed -> ``unmeasured``. The sweep cannot tell a real
      failure from an unmet upstream, and saying either would be a guess.

    The prefix is rendered from the date the STAGE used, not the sweep's own
    calendar date — see ``normalize_probe_date``.
    """
    template = declaration["prefix"]
    mode = declaration.get("date_normalization") or NORMALIZE_NONE
    try:
        bindings = _probe_bindings(bindings, mode)
    except DateNormalizationUnavailable as exc:
        result.verdict = UNMEASURED
        result.reason = (
            f"preflight exited rc={result.returncode}, and the declared upstream prefix "
            f"{template!r} could not be dated: {exc}. The launcher normalizes RUN_DATE to "
            "the NYSE trading day before reading this prefix, so probing the sweep's raw "
            "calendar date would name a prefix the stage never read and could silently "
            "downgrade a real failure — neither verdict is claimed."
        )
        return result
    try:
        prefix = template.format(**bindings)
    except (KeyError, IndexError, ValueError) as exc:
        result.verdict = UNMEASURED
        result.reason = (
            f"preflight exited rc={result.returncode}, and the declared upstream prefix "
            f"template {template!r} could not be rendered from the sweep's bindings "
            f"({type(exc).__name__}: {exc}) — a real failure cannot be told from an "
            "unmet upstream"
        )
        return result

    if lister is None:
        result.verdict = UNMEASURED
        result.reason = (
            f"preflight exited rc={result.returncode}, and the declared upstream "
            f"{prefix!r} (produced by {declaration['produced_by']}) could not be probed: "
            "the sweep has no S3 surface in this run. A real failure cannot be told "
            "from an unmet upstream, so neither is claimed."
        )
        return result

    try:
        present, detail = upstream_content(
            prefix, list(declaration.get("ignore_subprefixes") or []), lister
        )
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        result.verdict = UNMEASURED
        result.reason = (
            f"preflight exited rc={result.returncode}, and probing the declared upstream "
            f"{prefix!r} raised {type(exc).__name__}: {exc} — a real failure cannot be "
            "told from an unmet upstream"
        )
        return result

    result.upstream = {**detail, "produced_by": declaration["produced_by"], "present": present}
    if present:
        result.reason = (
            f"{result.reason or f'preflight exited rc={result.returncode}'} — and its "
            f"declared upstream {prefix} IS populated ({detail['content_keys']} objects "
            f"from {declaration['produced_by']}), so this is a REAL failure, not an "
            "unmet upstream"
        )
        return result

    result.verdict = UNSWEEPABLE_VERDICT
    result.unsweepable_kind = UNSWEEPABLE_UPSTREAM_PENDING
    result.reason = (
        f"NOT MEASURABLE TODAY — this stage reads s3://{DEFAULT_BUCKET}/{prefix}, produced "
        f"by the {declaration['produced_by']} stage, and that prefix holds no content "
        f"({detail['ignored_keys']} key(s) ignored as "
        f"{', '.join(detail['ignore_subprefixes']) or 'n/a'} markers or zero-byte). A "
        f"--preflight-only run of {declaration['produced_by']} does not produce it, so the "
        "preconditions of this stage are UNKNOWN rather than broken. Declared in "
        "preflight_sweep_manifest.json:upstream_artifact_dependencies."
    )
    # Same wiring as the dedicated_box and no_dry_path rows below: the
    # manifest's WRITTEN acknowledgement (which cites the tracked issue and
    # explains the cadence — e.g. "legitimately unsweepable on the ~6
    # non-Saturday days of any week") must reach this row and the
    # notification, not just the auto-generated per-run reason above. Without
    # it an operator reading this result (or the nightly email) sees only
    # "NOT MEASURABLE TODAY" and has no way to tell an expected, declared,
    # cyclical gap from a new one without opening preflight_sweep_manifest.json
    # — alpha-engine-config-I10717.
    result.acknowledged_reason = declaration.get("reason")
    return result


def find_recent_populated_upstream(
    template: str,
    base_bindings: dict[str, Any],
    ignore_subprefixes: list[str],
    lister,
    max_age_days: int,
) -> tuple[str, dict] | None:
    """Walk backward from ``base_bindings['run_date']``, one day at a time, up
    to ``max_age_days``, for the most recent date whose rendered upstream
    prefix holds real content (never a sweep phase-marker or a zero-byte key
    — the same ``upstream_content`` contract ``classify_upstream`` uses).

    Returns ``(date_iso, detail)`` for the first (most recent, i.e. smallest
    offset) hit, or ``None`` if nothing in the window has content — which
    means either the upstream has never run, or its last run is older than
    the declared cadence, and the caller must not claim a measurement that
    was never taken.

    A per-candidate probe failure (a bad template substitution, a transient
    lister error) is treated as "no content at this candidate", not as a
    reason to abort the whole walk — the SAME-DAY probe already has its own
    dedicated UNMEASURED path in ``classify_upstream`` for "the probe itself
    could not run"; this function only widens WHERE a real probe looks, it
    does not relax what counts as a real probe failure.
    """
    start_raw = base_bindings.get("run_date")
    if not start_raw:
        return None
    try:
        start = dt.date.fromisoformat(str(start_raw)[:10])
    except ValueError:
        return None
    for offset in range(0, max_age_days + 1):
        candidate = (start - dt.timedelta(days=offset)).isoformat()
        try:
            prefix = template.format(**{**base_bindings, "run_date": candidate})
        except (KeyError, IndexError, ValueError):
            continue
        try:
            present, detail = upstream_content(prefix, ignore_subprefixes, lister)
        except Exception:  # noqa: BLE001 — one candidate's probe failing is
            # not the same fact as the probe being unavailable; the walk
            # keeps going rather than reporting UNMEASURED on the first
            # unlucky candidate day.
            continue
        if present:
            return candidate, {**detail, "prefix": prefix, "age_days": offset}
    return None


def remeasure_upstream_pending(
    result: StageResult,
    declaration: dict[str, Any],
    bindings: dict[str, Any],
    lister,
    *,
    definition: dict,
    context: dict[str, Any],
    checkout_root: str,
    stage_timeout: int,
    runner,
) -> StageResult:
    """``classify_upstream``, then — if that downgraded the stage to
    ``upstream_pending`` because TODAY's date is empty — actually MEASURE it
    against the most recent POPULATED instance of its declared upstream,
    within the declared ``max_age_days`` cadence window, instead of settling
    for "not measured".

    This is the SOTA half of alpha-engine-config-I10717: ``degraded`` on the
    weekday case was a correct grade for an UNMEASURED stage (principle 7 —
    "no data is never rendered as green"), but "unmeasured" was never
    structurally required — PredictorBacktest and PortfolioOptimizerBacktest
    both implement ``--run-date`` override and a real ``--preflight-only``
    boot+deps+smoke path; the sweep was simply never pointing them at a date
    that has data. Re-deriving the SAME stage's command against an earlier
    run_date and re-running it turns a stage that is unsweepable ~6 nights a
    week into one that is measured for real every night — a real PASS when
    the environment is healthy, a real FAIL (paging, exactly like any other
    stage) when it is not.

    ``degraded``/``upstream_pending`` stays reserved for exactly two cases
    after this: no populated instance exists anywhere in the max-age window
    (the acknowledgement may have gone stale — see the streak finding), or
    the override re-derivation itself cannot produce a sweepable stage (a
    definition-drift case that already has its own coverage-defect path).
    Never for "today's date happens to be empty", which is the ordinary case
    this function exists to stop mis-measuring as unmeasured.
    """
    classify_upstream(result, declaration, bindings, lister)
    if result.unsweepable_kind != UNSWEEPABLE_UPSTREAM_PENDING or lister is None:
        return result

    max_age_days = declaration.get("max_age_days", DEFAULT_UPSTREAM_MAX_AGE_DAYS)
    found = find_recent_populated_upstream(
        declaration["prefix"],
        bindings,
        list(declaration.get("ignore_subprefixes") or []),
        lister,
        max_age_days,
    )
    if found is None:
        result.reason = (
            f"{result.reason} No populated instance of this declared upstream was "
            f"found in the last {max_age_days} day(s) either — this is now a "
            "STALENESS finding, not the ordinary weekday case: either the weekly "
            "pipeline has stopped writing it, or the declared max_age_days is too "
            "short for its real cadence."
        )
        return result

    candidate_date, detail = found
    override_bindings = {**bindings, "run_date": candidate_date}
    override_stages = derive_stages(definition, override_bindings, context, checkout_root)
    override_stage = next((s for s in override_stages if s.name == result.stage), None)
    if override_stage is None or override_stage.classification != SWEEPABLE:
        # The override re-derivation did not even produce a sweepable stage
        # (definition drift, or the override binding broke something a
        # different stage needed). Do not claim a measurement that was never
        # actually taken — the manifest_disagreement path already covers a
        # derivation that cannot be trusted.
        return result

    remeasured = run_stage(override_stage, checkout_root, stage_timeout, runner)
    remeasured.measured_against_run_date = candidate_date
    remeasured.acknowledged_reason = declaration.get("reason")
    remeasured.upstream = {**detail, "produced_by": declaration["produced_by"], "present": True}
    remeasured.reason = (
        f"MEASURED AGAINST {candidate_date} ({detail['age_days']} day(s) old — the most "
        f"recent populated instance of the declared upstream within the "
        f"{max_age_days}-day cadence window), not today's empty "
        f"{result.upstream['prefix'] if result.upstream else declaration['prefix']!r}. "
        f"{remeasured.reason or f'preflight exited rc={remeasured.returncode}'}"
    )
    return remeasured


# ── Persistent-unsweepable streaks ───────────────────────────────────────────


def streak_threshold_runs(manifest: dict, cadence: dict) -> int:
    """Consecutive RUNS that make an unsweepable stage a finding of its own.

    Declared in days in the manifest and converted here against the declared
    cadence, so changing the sweep cadence cannot silently change what "N
    consecutive days" means.
    """
    days = int(manifest.get("unsweepable_streak_threshold_days", 8))
    per_day = 1440.0 / float(cadence["cadence_minutes"])
    return max(1, round(days * per_day))


def update_streaks(
    prior: dict[str, Any] | None,
    results: list[StageResult],
    run_id: str,
    now_iso: str,
    threshold_runs: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Advance the per-stage unsweepable streak and emit the findings it earns.

    A stage that produced any verdict other than ``unsweepable`` this run has
    its streak dropped: it became measurable, which is the condition the
    threshold exists to detect the absence of.

    An acknowledged ``dedicated_box`` stage accrues no streak at all. The streak
    exists to catch a stage that was expected to become measurable and never
    did; a stage DECLARED unreachable from this box will be unsweepable on every
    run forever by construction, so escalating it after N runs would report the
    declaration back as a discovery, on a schedule, permanently. The gap is not
    dropped — it is carried by the non-blocking ``dedicated_box`` coverage
    finding, which is named on every run, and by manifest_disagreement, which
    fails the run if the declaration ever stops matching the definition.
    """
    prior_streaks = (prior or {}).get("streaks", {}) or {}
    streaks: dict[str, Any] = {}
    findings: list[dict[str, Any]] = []
    for result in results:
        if result.verdict != UNSWEEPABLE_VERDICT:
            continue
        if result.unsweepable_kind == UNSWEEPABLE_DEDICATED_BOX:
            continue
        previous = prior_streaks.get(result.stage) or {}
        runs = int(previous.get("consecutive_runs", 0)) + 1
        entry = {
            "consecutive_runs": runs,
            "since": previous.get("since") or now_iso,
            "kind": result.unsweepable_kind,
            "last_run_id": run_id,
            "last_reason": result.reason,
        }
        streaks[result.stage] = entry
        if runs >= threshold_runs:
            findings.append(
                {
                    "stage": result.stage,
                    "kind": result.unsweepable_kind,
                    "consecutive_runs": runs,
                    "threshold_runs": threshold_runs,
                    "since": entry["since"],
                    "finding": (
                        f"{result.stage} has been unsweepable on every one of the last "
                        f"{runs} runs (threshold {threshold_runs}, since {entry['since']}) "
                        "— the sweep has measured NOTHING about this stage's preconditions "
                        "for that whole period. This is not coverage."
                    ),
                }
            )
    return (
        {"schema_version": 1, "updated_at": now_iso, "run_id": run_id, "streaks": streaks},
        findings,
    )


# ── Report rendering ─────────────────────────────────────────────────────────


def render_notification(report: SweepReport) -> tuple[str, str]:
    """ONE notification for the whole run (observability-policy §7.2a: one per
    group failure, never one per member). Carries the full member list by name
    so the operator never has to open a console to learn which stages broke."""
    not_covered = (
        report.stages_unsweepable
        + report.stages_unmeasured
        + report.stages_no_dry_path
        + report.stages_not_attempted
    )
    if not report.measured:
        subject = f"[preflight-sweep] COULD NOT MEASURE — {report.run_id}"
    elif report.outcome == OUTCOME_OK:
        subject = (
            f"[preflight-sweep] all {report.stages_passed} stages clean — {report.run_id}"
        )
    elif report.stages_failed == 0 and report.stages_unsweepable_coverage_defect == 0:
        # Nothing broke, but the run did not cover everything. Naming the
        # denominator's missing part IS the subject — a subject reading
        # "0 failed" over a run that measured 14 of 19 stages is the count
        # published without its members.
        subject = (
            f"[preflight-sweep] no failures — {not_covered} of {report.stages_declared} "
            f"stages not covered ({report.stages_unsweepable_upstream_pending} awaiting "
            f"upstream / {report.stages_unsweepable_dedicated_box} dedicated box / "
            f"{report.stages_no_dry_path} no dry path / "
            f"{report.stages_unmeasured} unmeasured) — {report.run_id}"
        )
    else:
        subject = (
            f"[preflight-sweep] {report.stages_failed} failed / "
            f"{report.stages_unsweepable_coverage_defect} unsweepable / "
            f"{report.stages_unmeasured} unmeasured / "
            f"{report.stages_no_dry_path} no-dry-path / "
            f"{report.stages_unsweepable_upstream_pending} awaiting-upstream "
            f"of {report.stages_declared} — {report.run_id}"
        )

    lines = [
        f"component: {report.component_id}",
        f"run_id:    {report.run_id}",
        f"outcome:   {report.outcome}   measured: {report.measured}",
    ]
    if report.unmeasured_reason:
        lines.append(f"reason:    {report.unmeasured_reason}")
    lines += [
        "",
        f"declared {report.stages_declared} | swept {report.stages_swept} | "
        f"passed {report.stages_passed} | failed {report.stages_failed} | "
        f"unmeasured {report.stages_unmeasured} | "
        f"unsweepable {report.stages_unsweepable} "
        f"(coverage-defect {report.stages_unsweepable_coverage_defect}, "
        f"awaiting-upstream {report.stages_unsweepable_upstream_pending}, "
        f"dedicated-box {report.stages_unsweepable_dedicated_box}) | "
        f"no-dry-path {report.stages_no_dry_path} | "
        f"not-attempted {report.stages_not_attempted}",
        "",
    ]
    blocking = [f for f in report.coverage_findings if _is_blocking(f)]
    acknowledged = [f for f in report.coverage_findings if not _is_blocking(f)]
    if blocking:
        lines.append("COVERAGE FINDINGS (the sweep's own denominator is wrong):")
        lines += [f"  - {_finding_text(f)}" for f in blocking]
        lines.append("")
    if acknowledged:
        lines.append(
            "COVERAGE GAPS (acknowledged in preflight_sweep_manifest.json — the sweep "
            "does not cover these, which is not the same as their being healthy):"
        )
        lines += [f"  - {_finding_text(f)}" for f in acknowledged]
        lines.append("")
    if report.persistent_unsweepable_findings:
        lines.append(
            "PERSISTENTLY UNSWEEPABLE (measured nothing for a full threshold period "
            "— this is NOT coverage):"
        )
        lines += [
            f"  - {f['finding']}" for f in report.persistent_unsweepable_findings
        ]
        lines.append("")
    if report.unsweepable_streak_state != "measured":
        lines.append(
            f"streak state: {report.unsweepable_streak_state} — the persistent-unsweepable "
            "finding could not be evaluated this run."
        )
        lines.append("")
    # EVERY non-passed category is named here, not only the failures. A
    # category with a non-zero count and no members on the human-facing surface
    # is a number nobody can act on (alpha-engine-config#7324).
    for verdict, header in (
        (FAILED, "FAILED"),
        (UNSWEEPABLE_VERDICT, "UNSWEEPABLE"),
        (UNMEASURED, "UNMEASURED (not a pass, not a defect)"),
        (NO_DRY_PATH_VERDICT, "NO DRY PATH (declared, never exercised by the sweep)"),
        (NOT_ATTEMPTED, "NOT ATTEMPTED"),
    ):
        members = [r for r in report.results if r["verdict"] == verdict]
        if members:
            lines.append(f"{header}:")
            for r in members:
                detail = r.get("reason") or ""
                kind = r.get("unsweepable_kind")
                label = f"  - {r['stage']}"
                if kind:
                    label += f" [{kind}]"
                err = r.get("last_stderr_line")
                lines.append(f"{label}: {detail}")
                if r.get("acknowledged_reason"):
                    lines.append(f"      acknowledged: {r['acknowledged_reason']}")
                if err:
                    lines.append(f"      last stderr: {err}")
            lines.append("")
    passed = [
        r["stage"]
        + (f" (measured against {r['measured_against_run_date']})" if r.get("measured_against_run_date") else "")
        for r in report.results
        if r["verdict"] == PASSED
    ]
    if passed:
        lines.append("PASSED: " + ", ".join(passed))
    lines.append("")
    lines.append(
        f"report: s3://{DEFAULT_BUCKET}/{REPORT_PREFIX}/{report.run_id}/report.json"
    )
    return subject, "\n".join(lines)


# ── Driver ───────────────────────────────────────────────────────────────────


def _definition_sha(path: Path) -> str | None:
    try:
        import hashlib

        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        # Not fatal to the report — the definition was already loaded by the
        # time this runs, so its absence here can only mean a concurrent
        # change. Recorded as null in the report rather than swallowed.
        return None


def sweep(
    definition_path: Path,
    manifest_path: Path,
    checkout_root: str,
    run_id: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    stage_timeout: int = DEFAULT_STAGE_TIMEOUT,
    aws: AwsSurface | None = None,
    runner=subprocess.run,
) -> SweepReport:
    started = dt.datetime.now(dt.timezone.utc)
    report = SweepReport(
        component_id=COMPONENT_ID,
        run_id=run_id,
        started_at=started.isoformat(),
        checkout_root=checkout_root,
        definition_sha=_definition_sha(definition_path),
    )

    # ── Derivation. Any failure here means the sweep cannot know what it is
    # supposed to sweep — reported as UNMEASURED, never as a clean run.
    try:
        definition = json.loads(definition_path.read_text())
        manifest = load_manifest(manifest_path)
        base_bindings = derive_shell_run_bindings(definition)
        # run_date is computed by InitializeInput from the execution's own
        # start time (a States.Format, deliberately not read as a static
        # blob), so the RUNNER supplies it — exactly as the SF does. It is set
        # before the map-binding scan so it is not reported as an undeclared
        # Map-scoped variable.
        base_bindings.setdefault("run_date", started.date().isoformat())
        required_map = derive_required_map_bindings(definition, base_bindings)
        report.coverage_findings += [
            _finding(FINDING_MAP_BINDING_DISAGREEMENT, f, blocking=True)
            for f in map_binding_disagreement(required_map, manifest)
        ]
        bindings = apply_map_bindings(base_bindings, manifest)
        context = {"Execution": {"Name": run_id, "Id": f"preflight-sweep:{run_id}"}}
        stages = derive_stages(definition, bindings, context, checkout_root)
        report.coverage_findings += [
            _finding(FINDING_MANIFEST_DISAGREEMENT, f, blocking=True)
            for f in manifest_disagreement(stages, manifest)
        ]
        report.coverage_findings += [
            _finding(FINDING_UPSTREAM_DECLARATION, f, blocking=True)
            for f in upstream_dependency_disagreement(stages, manifest)
        ]
        upstream_decls = upstream_dependencies(manifest)
        cadence = load_cadence()
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        report.finished_at = dt.datetime.now(dt.timezone.utc).isoformat()
        report.outcome = OUTCOME_FAILED
        report.measured = False
        report.unmeasured_reason = (
            f"stage derivation failed ({type(exc).__name__}: {exc}) — the sweep could "
            "not determine what it was supposed to sweep. This is NOT a clean run.\n"
            + traceback.format_exc(limit=6)
        )
        return report

    report.stages_declared = len(stages)
    dedicated_box = {
        entry["stage"]: entry
        for entry in (manifest.get("dedicated_box_stages") or [])
        if isinstance(entry, dict) and entry.get("stage")
    }
    report.stages_no_dry_path = sum(1 for s in stages if s.classification == NO_DRY_PATH)
    # The declared stage list, serialised — so `declared - swept` is auditable
    # from the report alone. A scalar nobody can expand is a count published
    # without its members (alpha-engine-config#7324).
    report.declared_stages = [
        {
            "stage": s.name,
            "classification": s.classification,
            "repo": s.repo,
            "launcher": s.launcher,
            "box_dir": s.box_dir,
            # Carried on the declared row too, so `declared - swept` can be
            # reconciled from the report alone without opening the manifest.
            "dedicated_box_acknowledged": s.name in dedicated_box,
        }
        for s in stages
    ]
    acknowledgements = {
        entry["stage"]: entry
        for entry in (manifest.get("no_dry_path_stages") or [])
        if isinstance(entry, dict) and entry.get("stage")
    }
    report.stages_no_dry_path_unacknowledged = sum(
        1
        for st in stages
        if st.classification == NO_DRY_PATH
        and not (acknowledgements.get(st.name) or {}).get("reason")
    )

    results: list[StageResult] = []
    for stage in stages:
        if stage.classification == UNSWEEPABLE:
            # A DECLARED dedicated-box stage is unreachable from this box by
            # construction, not by defect. It gets its own kind, stays out of
            # the coverage-defect count and out of the streak, and is NAMED
            # here and on every downstream surface — the acknowledgement buys a
            # correct classification, never silence.
            ack = dedicated_box.get(stage.name)
            results.append(
                StageResult(
                    stage=stage.name,
                    verdict=UNSWEEPABLE_VERDICT,
                    unsweepable_kind=(
                        UNSWEEPABLE_DEDICATED_BOX if ack else UNSWEEPABLE_COVERAGE_DEFECT
                    ),
                    repo=stage.repo,
                    launcher=stage.launcher,
                    reason=stage.reason,
                    acknowledged_reason=(ack or {}).get("reason"),
                )
            )
            if ack:
                report.coverage_findings.append(
                    _finding(
                        FINDING_DEDICATED_BOX,
                        (
                            f"{stage.name} runs on a DEDICATED box the sweep does not "
                            f"have and is never exercised by it (launcher="
                            f"{stage.launcher or 'n/a'}, repo={stage.repo or 'n/a'}; "
                            f"derivation said: {stage.reason}). Acknowledged "
                            f"{ack.get('acknowledged')}: {ack['reason']}"
                        ),
                        blocking=False,
                        stage=stage.name,
                    )
                )
        elif stage.classification == NO_DRY_PATH:
            # A verdict ROW, not just an increment. Before this, the three
            # no-dry-path stages existed in the report only as the scalar
            # `stages_no_dry_path: 3` — absent from results[], absent from the
            # notification, and un-nameable after the fact.
            ack = acknowledgements.get(stage.name)
            results.append(
                StageResult(
                    stage=stage.name,
                    verdict=NO_DRY_PATH_VERDICT,
                    repo=stage.repo,
                    launcher=stage.launcher,
                    reason=stage.reason,
                    acknowledged_reason=(ack or {}).get("reason"),
                )
            )
            report.coverage_findings.append(
                _finding(
                    FINDING_NO_DRY_PATH,
                    (
                        f"{stage.name} has NO dry path and is never exercised by the sweep "
                        f"(launcher={stage.launcher or 'n/a'}, repo={stage.repo or 'n/a'}). "
                        + (
                            f"Acknowledged {ack.get('acknowledged')}: {ack['reason']}"
                            if ack
                            else (
                                "NOT acknowledged in preflight_sweep_manifest.json — the "
                                "blocking manifest_disagreement finding above is the one "
                                "that fails this run; this entry only names the gap."
                            )
                        )
                    ),
                    # Never blocking: an acknowledged gap is a reviewed decision,
                    # and an UNacknowledged one already has its own blocking
                    # manifest_disagreement finding. Emitting a second blocking
                    # copy would make one fact fail the run twice.
                    blocking=False,
                    stage=stage.name,
                )
            )

    sweepable = [s for s in stages if s.classification == SWEEPABLE]
    report.measured = True

    if sweepable:
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(run_stage, s, checkout_root, stage_timeout, runner): s
                for s in sweepable
            }
            for future in concurrent.futures.as_completed(futures):
                stage = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    # The worker itself died. The stage was not measured; it
                    # did not pass and it did not fail.
                    result = StageResult(
                        stage=stage.name,
                        verdict=UNMEASURED,
                        repo=stage.repo,
                        launcher=stage.launcher,
                        reason=f"sweep worker raised {type(exc).__name__}: {exc}",
                    )
                results.append(result)
                if aws is not None:
                    try:
                        aws.put_text(
                            f"{REPORT_PREFIX}/{run_id}/stages/{stage.name}.log",
                            _stage_log(stage, result, _stage_script(stage)),
                        )
                        result.log_key = f"{REPORT_PREFIX}/{run_id}/stages/{stage.name}.log"
                    except Exception as exc:  # noqa: BLE001
                        # Telemetry emission must never break the observed work
                        # (observability-policy §4), and must never be silent.
                        # The per-stage verdict is unaffected; only its log
                        # shipping failed, and it says so on the record.
                        print(
                            f"PREFLIGHT_SWEEP_TELEMETRY_DEGRADED stage={stage.name} "
                            f"could not ship stage log: {type(exc).__name__}: {exc}",
                            file=sys.stderr,
                        )

    # ── Declared same-day upstream reclassification ──────────────────────────
    # A stage whose preflight failed SOLELY because an upstream artifact of the
    # same execution does not exist yet was not measured — it did not find a
    # defect. The stages this may apply to are DECLARED in the manifest, so a
    # reworded launcher error can never reclassify a real failure; the prefix
    # is still probed on the day, so the declaration alone can never hide one.
    lister = (lambda prefix: aws.list_objects(prefix)) if aws is not None else None
    for i, result in enumerate(results):
        if result.verdict != FAILED:
            continue
        declaration = upstream_decls.get(result.stage)
        if declaration is None:
            continue
        # remeasure_upstream_pending both classifies (today's-date probe,
        # exactly as before) AND — only when that classification is
        # upstream_pending — attempts a REAL measurement against the most
        # recent populated instance of the declared upstream, so a stage
        # whose same-day precondition is structurally never met on a
        # preflight day is not left "unmeasured" when it does not have to
        # be (alpha-engine-config-I10717 (2)). May return a DIFFERENT
        # StageResult (a real pass/fail from the override run), so the list
        # entry is replaced rather than mutated in place.
        results[i] = remeasure_upstream_pending(
            result,
            declaration,
            bindings,
            lister,
            definition=definition,
            context=context,
            checkout_root=checkout_root,
            stage_timeout=stage_timeout,
            runner=runner,
        )

    order = {s.name: i for i, s in enumerate(stages)}
    # ── The invariant: every declared stage carries a verdict ────────────────
    # `len(results) == stages_declared`. Its absence is the root cause of
    # alpha-engine-config#7324: 19 declared, 16 rows, and the 3 missing stages
    # unrecoverable from the report. A stage with no verdict is filled in as
    # not_attempted AND raised as a blocking coverage finding — the sweep's own
    # denominator is wrong, which outranks anything it found.
    have = {r.stage for r in results}
    for stage in stages:
        if stage.name in have:
            continue
        results.append(
            StageResult(
                stage=stage.name,
                verdict=NOT_ATTEMPTED,
                repo=stage.repo,
                launcher=stage.launcher,
                reason=(
                    "the sweep produced NO verdict for this declared stage — a hole in "
                    "the sweep's own coverage, not a fact about the stage"
                ),
            )
        )
        report.coverage_findings.append(
            _finding(
                FINDING_MISSING_VERDICT,
                (
                    f"declared stage {stage.name!r} (classification "
                    f"{stage.classification!r}) produced no verdict — len(results) did "
                    "not equal stages_declared"
                ),
                blocking=True,
                stage=stage.name,
            )
        )
    results.sort(key=lambda r: order.get(r.stage, 10**6))

    report.stages_swept = sum(1 for r in results if r.verdict in (PASSED, FAILED))
    report.stages_passed = sum(1 for r in results if r.verdict == PASSED)
    report.stages_failed = sum(1 for r in results if r.verdict == FAILED)
    report.stages_unmeasured = sum(1 for r in results if r.verdict == UNMEASURED)
    report.stages_unsweepable = sum(1 for r in results if r.verdict == UNSWEEPABLE_VERDICT)
    report.stages_unsweepable_coverage_defect = sum(
        1
        for r in results
        if r.verdict == UNSWEEPABLE_VERDICT
        and r.unsweepable_kind
        not in (UNSWEEPABLE_UPSTREAM_PENDING, UNSWEEPABLE_DEDICATED_BOX)
    )
    report.stages_unsweepable_dedicated_box = sum(
        1
        for r in results
        if r.verdict == UNSWEEPABLE_VERDICT
        and r.unsweepable_kind == UNSWEEPABLE_DEDICATED_BOX
    )
    report.stages_unsweepable_upstream_pending = sum(
        1
        for r in results
        if r.verdict == UNSWEEPABLE_VERDICT
        and r.unsweepable_kind == UNSWEEPABLE_UPSTREAM_PENDING
    )
    report.stages_not_attempted = sum(1 for r in results if r.verdict == NOT_ATTEMPTED)

    # ── Persistent-unsweepable streaks ───────────────────────────────────────
    report.unsweepable_streak_threshold_runs = streak_threshold_runs(manifest, cadence)
    if aws is None:
        report.unsweepable_streak_state = (
            "unavailable — the sweep had no S3 surface, so no streak history was read "
            "or written"
        )
    else:
        streak_key = f"{REPORT_PREFIX}/{STREAK_STATE_KEY_SUFFIX}"
        try:
            prior = aws.get_json(streak_key)
        except Exception as exc:  # noqa: BLE001 — reported, never swallowed
            # Treating an unreadable history as "no history" would reset every
            # streak to 1 on each run and make the finding structurally unable
            # to fire. It is named instead, and nothing is written over it.
            report.unsweepable_streak_state = (
                f"unavailable — could not read {streak_key} "
                f"({type(exc).__name__}: {exc}); streaks were NOT reset and the "
                "persistent-unsweepable finding could not be evaluated"
            )
        else:
            state, findings = update_streaks(
                prior,
                results,
                run_id,
                dt.datetime.now(dt.timezone.utc).isoformat(),
                report.unsweepable_streak_threshold_runs,
            )
            report.persistent_unsweepable_findings = findings
            try:
                aws.put_json(streak_key, state)
                report.unsweepable_streak_state = "measured"
            except Exception as exc:  # noqa: BLE001
                report.unsweepable_streak_state = (
                    f"degraded — streaks were evaluated but could not be persisted to "
                    f"{streak_key} ({type(exc).__name__}: {exc}); the next run will "
                    "under-count"
                )

    report.results = [r.to_dict() for r in results]
    report.finished_at = dt.datetime.now(dt.timezone.utc).isoformat()

    blocking_findings = [f for f in report.coverage_findings if _is_blocking(f)]
    # A run is CLEAN only if every stage the sweep is accountable for passed.
    # A stage awaiting its upstream is not a failure and not a pass, so it
    # keeps the run out of OK and out of last_clean.json — rendering
    # "measured 14 of 19" as green is exactly the failure this component
    # exists to avoid.
    #
    # A RATIFIED no-dry-path stage is deliberately NOT in this predicate. It is
    # a permanent, reviewed boundary of the denominator, not a same-day
    # measurement gap: five of them are declared, none will ever gain a dry
    # path (three were ruled that way in alpha-engine-config#7329), so gating
    # `clean` on the raw count made `ok` unreachable on every possible day and
    # `_preflight_sweep/last_clean.json` — the pointer this module's own
    # docstring calls the artifact a detector reads as proof, and which
    # registry.d/ae-preflight-sweep.yaml declares this component PRODUCES —
    # was never written once in the sweep's life. A status with one reachable
    # value grades nothing (principles.md §2.7 read the other way round:
    # a permanent amber is as uninformative as a permanent green). The gap is
    # still named on every surface: each such stage carries its own no_dry_path
    # verdict row and its own coverage finding, and an UNACKNOWLEDGED one both
    # raises a blocking manifest_disagreement finding and is counted in
    # stages_no_dry_path_unacknowledged above, so it still cannot be clean.
    # An ACKNOWLEDGED dedicated-box stage is excluded from `clean` for the same
    # reason a ratified no-dry-path stage is: it is a permanent, reviewed
    # boundary of the denominator, not a same-day measurement gap, so gating
    # `clean` on it would make `ok` unreachable on every possible day and stop
    # `last_clean.json` being written at all — a status with one reachable value
    # grades nothing. The gap is still named on every surface (its own verdict
    # row with the acknowledgement text, its own non-blocking coverage finding),
    # and an UNACKNOWLEDGED unsweepable stage is still counted here through
    # stages_unsweepable_coverage_defect, so nothing can be excluded without the
    # declaration that names it.
    clean = (
        report.stages_failed == 0
        and (report.stages_unsweepable - report.stages_unsweepable_dedicated_box) == 0
        and report.stages_unmeasured == 0
        and report.stages_not_attempted == 0
        and report.stages_no_dry_path_unacknowledged == 0
        and not blocking_findings
    )
    if clean and report.stages_passed > 0:
        report.outcome = OUTCOME_OK
    elif (
        report.stages_failed
        or report.stages_unsweepable_coverage_defect
        or report.stages_not_attempted
        or blocking_findings
        or report.persistent_unsweepable_findings
    ):
        report.outcome = OUTCOME_FAILED
    else:
        # Nothing failed, but something could not be measured. Degraded, and
        # explicitly not a clean run — "no data" is never rendered as green.
        report.outcome = OUTCOME_DEGRADED
    if report.stages_passed == 0 and report.stages_swept == 0:
        report.measured = False
        report.unmeasured_reason = (
            "no stage produced a pass or a fail — the sweep exercised nothing"
        )
        report.outcome = OUTCOME_FAILED
    return report


def emit(report: SweepReport, aws: AwsSurface, sns_topic: str) -> None:
    """Publish the run's telemetry: durable report, console rows, metrics, one
    notification."""
    aws.put_json(f"{REPORT_PREFIX}/{report.run_id}/report.json", report.to_dict())
    aws.put_json(f"{REPORT_PREFIX}/latest.json", report.to_dict())

    # Console rows (console-policy §2.6): one check-result envelope per stage
    # plus the sweep's own roll-up. Published on EVERY run — a row that stops
    # being republished is exactly how the console renders STALE, so skipping
    # the write on a bad run would convert a loud failure into a quiet
    # ageing green.
    cadence = load_cadence()
    for key, body in envelopes(report.to_dict(), cadence):
        aws.put_json(key, body)
    if report.outcome == OUTCOME_OK:
        # Success-claim asymmetry (observability-policy §3.1): only a clean run
        # advances the pointer a detector reads as proof. The failure path
        # writes the report above and stops there.
        aws.put_json(f"{REPORT_PREFIX}/last_clean.json", report.to_dict())

    aws.put_metrics(
        [
            # The deadman's subject: emitted on EVERY terminal path, including
            # a run that could not measure. Its ABSENCE means the sweep never
            # reported at all, which is the only thing the alarm should page on.
            ("PreflightSweepRunCompleted", 1),
            ("PreflightSweepStagesDeclared", report.stages_declared),
            ("PreflightSweepStagesSwept", report.stages_swept),
            ("PreflightSweepStagesPassed", report.stages_passed),
            ("PreflightSweepStagesFailed", report.stages_failed),
            ("PreflightSweepStagesUnmeasured", report.stages_unmeasured),
            ("PreflightSweepStagesUnsweepable", report.stages_unsweepable),
            # Published separately and both at zero. Only the coverage-defect
            # series may drive an alarm: a stage awaiting its same-day upstream
            # is the ordinary weekday state and must not page (I7323).
            (
                "PreflightSweepStagesUnsweepableCoverageDefect",
                report.stages_unsweepable_coverage_defect,
            ),
            (
                "PreflightSweepStagesUnsweepableUpstreamPending",
                report.stages_unsweepable_upstream_pending,
            ),
            ("PreflightSweepStagesNoDryPath", report.stages_no_dry_path),
            ("PreflightSweepStagesNotAttempted", report.stages_not_attempted),
            ("PreflightSweepCoverageFindings", len(report.coverage_findings)),
            (
                "PreflightSweepCoverageFindingsBlocking",
                len([f for f in report.coverage_findings if _is_blocking(f)]),
            ),
            (
                "PreflightSweepPersistentUnsweepable",
                len(report.persistent_unsweepable_findings),
            ),
        ]
    )
    subject, message = render_notification(report)
    aws.publish(sns_topic, subject, message)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--definition", default=str(repo_root / "infrastructure" / "step_function.json"))
    parser.add_argument("--manifest", default=str(repo_root / "infrastructure" / "preflight_sweep_manifest.json"))
    parser.add_argument("--checkout-root", default="/home/ec2-user")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--stage-timeout", type=int, default=DEFAULT_STAGE_TIMEOUT)
    parser.add_argument("--sns-topic", default=DEFAULT_SNS_TOPIC)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--derive-only",
        action="store_true",
        help="derive and print the stage matrix without running anything or "
        "emitting telemetry (the zero-spend rehearsal path)",
    )
    args = parser.parse_args(argv)

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime(
        "preflight-sweep-%Y%m%dT%H%M%SZ"
    )

    if args.derive_only:
        definition = json.loads(Path(args.definition).read_text())
        manifest = load_manifest(args.manifest)
        base = derive_shell_run_bindings(definition)
        # run_date is bound BEFORE the map-binding scan, exactly as `sweep()`
        # does it — the RUNNER supplies it, so it is not a Map-scoped variable
        # the manifest must declare. Binding it afterwards made this rehearsal
        # emit a BLOCKING map_binding_disagreement finding for `$.run_date`
        # that the live path never emits: the zero-spend rehearsal disagreed
        # with production about whether the pipeline was renderable at all.
        base.setdefault("run_date", dt.datetime.now(dt.timezone.utc).date().isoformat())
        required_map = derive_required_map_bindings(definition, base)
        findings = map_binding_disagreement(required_map, manifest)
        bindings = apply_map_bindings(base, manifest)
        stages = derive_stages(
            definition,
            bindings,
            {"Execution": {"Name": run_id, "Id": f"preflight-sweep:{run_id}"}},
            args.checkout_root,
        )
        findings += manifest_disagreement(stages, manifest)
        findings += upstream_dependency_disagreement(stages, manifest)
        print(
            json.dumps(
                {
                    "run_id": run_id,
                    "coverage_findings": findings,
                    "stages": [s.to_dict() for s in stages],
                },
                indent=2,
            )
        )
        return 1 if findings else 0

    aws = AwsSurface(region=args.region, bucket=args.bucket)
    report = sweep(
        definition_path=Path(args.definition),
        manifest_path=Path(args.manifest),
        checkout_root=args.checkout_root,
        run_id=run_id,
        concurrency=args.concurrency,
        stage_timeout=args.stage_timeout,
        aws=aws,
    )
    try:
        emit(report, aws, args.sns_topic)
    except Exception as exc:  # noqa: BLE001
        # Telemetry emission must not crash the observed work, and must not be
        # silently swallowed (observability-policy §4). The greppable marker
        # below IS the signal; the non-zero exit below still fires.
        print(
            f"PREFLIGHT_SWEEP_TELEMETRY_FAILED could not emit report: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        traceback.print_exc()

    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    subject, message = render_notification(report)
    print(f"\n{subject}\n{message}", file=sys.stderr)
    return 0 if report.outcome == OUTCOME_OK else 1


if __name__ == "__main__":
    raise SystemExit(main())

