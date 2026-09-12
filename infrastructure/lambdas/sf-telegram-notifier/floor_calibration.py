"""Periodic re-derivation for STATE_DURATION_FLOORS_SEC (alpha-engine-config-I10164 part 2).

WHY THIS EXISTS. Every entry in ``execution_digest.STATE_DURATION_FLOORS_SEC``
is a hand-typed constant, and nothing checked it against reality after it was
written. Two of them rotted the same way: ``PollMorningEnrichSpot``'s 8m floor
sat ABOVE the median of every healthy run (a chronic false positive, caught
only because it happened to fire on 2026-09-08); ``PollMorningArcticAppendSpot``'s
own 8m floor sat BELOW every genuine run and MISSED a confirmed-broken 929.7s
execution (a false negative, never caught at all — it took an unrelated
investigation to find it). Both floors were introduced in the same commit,
reusing an unrelated weekly floor's literal, with no measurement basis
recorded. A hand-kept table is the defect this module targets: it recomputes
what each floor SHOULD be from live Step Functions history, on a schedule, and
reports drift on a durable, alerting surface — never only a log line.

WHAT THIS DOES NOT DO. It does not rewrite ``execution_digest.py`` and it does
not merge anything: floor changes go into that file's git history, and merge
authority is Brian's per ``pull-request-policy.md``. The loop this module
closes is DETECT (pull live durations) -> DIAGNOSE (compare against the
codified floor with a stated formula) -> ACT (fail a gated CI step naming the
exact numbers, the SOTA-available action short of an unattended source edit)
-> VERIFY (the next scheduled run re-checks after a fix lands) -> CLOSE (the
check goes green). ``--check`` is wired into ``sf-arn-drift-check.yml``
(daily) alongside this repo's other codified-vs-live drift detectors, so a
newly-rotted floor cannot go undetected for longer than one day, the same
guarantee every other entry in that workflow already carries.

WHY LOCAL TO THIS LAMBDA, NOT nousergon-lib (policy-shared-code). One
consumer today (this module, called from this one CI step). The policy's own
rule is explicit: lift on the SECOND adoption, not pre-generalize for one that
may never arrive. If another fleet detector needs the same
derive-a-threshold-from-live-history shape, THAT is the trigger to extract a
shared library function — not before.

STATUS-AWARE SAMPLING (the lesson this module exists to encode). A SUCCEEDED
Step Functions execution does not mean every Task inside it did real work —
``PollMorningArcticAppendSpot``'s own output carries a companion
``arctic_append_poll.Status`` field (the raw ``ssm:GetCommandInvocation``
result) that can read ``Failed`` even while the execution completes
SUCCEEDED. Where a state has a KNOWN companion poll-status field
(``KNOWN_POLL_STATUS_KEYS`` below), only ``Status == "Success"`` samples count
as GENUINE for the floor formula; a duration alone would silently launder a
broken run into the distribution. Where no companion key is declared, this
module computes on raw duration and says so — DECLARED rather than inferred,
same discipline ``execution_digest.py`` already applies to
``TERMINAL_ERROR_HANDLING_STATES`` / ``WORK_STATES_ON_TERMINAL_PATHS``.

MEASURE THE SAME SPAN THE NOTIFIER FLOORS (alpha-engine-config-I10574). Every
name in ``STATE_TO_STATE_MACHINE`` MUST be the exact state name that carries
the workload span — never a dispatch Task that fires an SSM/spot command and
returns in under a second while the real work runs in a polled child.
``execution_digest.parse_task_state_durations`` (first-entry -> last-exit,
the same function both this module and the live notifier call) is span-
correct once pointed at the right name; pointed at a dispatch name it
faithfully reports the dispatch's own ~0.2s duration, which is correct and
useless. Measured live against the one genuine EventBridge-triggered
SUCCEEDED ne-weekly-freshness-pipeline execution in the account's full
history carrying these states (name
9b34ac0f-5e2f-f70b-d668-61b4c4751654_6fb6adf9-55fa-4426-6d06-79e4579bcaff,
2026-08-08): ``RAGIngestion``/``PredictorTraining``/``Backtester`` each enter
and exit in 0.19-0.24s, immediately followed by
``WaitForRAGIngestion``/``WaitForPredictorTraining``/``WaitForBacktester``
poll loops spanning 1057s/422s/663s — the same dispatch-then-poll shape
``WaitForMorningEnrich``/``WaitForDataPhase1`` were already renamed to track
under alpha-engine-config-I10545. ``execution_digest.STATE_DURATION_FLOORS_SEC``
carries the corresponding rename; see that file's comment for the measured
values.

CANONICAL EXECUTIONS ONLY, FOR THE WEEKLY MACHINE (alpha-engine-config-
I10574). ``ne-weekly-freshness-pipeline``'s SUCCEEDED-execution history is
NOT one population: alongside the daily EventBridge-triggered execution (name
`<uuid>_<uuid>` — two UUID-shaped segments joined by "_", the only shape that
trigger produces), the same history carries `watch-rerun-*`,
`offcycle-shell-*`, `friday-shell-*`, `recovery-*` and bare-single-UUID ad hoc
reruns/backfills. Measured across all 48 SUCCEEDED executions in the
account's full history (2026-09-12): every ad hoc execution's
WaitForMorningEnrich/WaitForDataPhase1 span was 30-151s (one to a handful of
poll iterations — these reruns skip already-processed phases and their
bootstrap legitimately returns fast), while the ONE canonical execution in
that same history spans 1662s/2417s. Averaging the two populations together
is exactly how ``min_genuine`` landed on 30.0s (the bare poll interval) for
both states in the first `--check` run that had permission to reach live
history (I10574) — a genuine value from a non-representative population, not
a wrong measurement of the right one. ``_CANONICAL_EXECUTION_NAME_RE`` scopes
this filter to the weekly machine only (``STATE_TO_STATE_MACHINE`` values in
``_CANONICAL_NAME_SCOPED_MACHINES``); the weekday pipelines' own ad hoc-
execution population, if any, is undocumented and out of scope here.
"""

from __future__ import annotations

import argparse
import logging
import re
import statistics
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from execution_digest import STATE_DURATION_FLOORS_SEC, parse_task_state_durations

logger = logging.getLogger(__name__)

REGION = "us-east-1"
ACCOUNT_ID = "711398986525"

#: Which Step Functions state machine carries each floored state. Declared,
#: not inferred: DIGEST_STATE_ORDER interleaves weekly and weekday state
#: names in one list with no machine-readable split, so this module names the
#: split explicitly rather than re-deriving it from comment boundaries.
STATE_TO_STATE_MACHINE: Mapping[str, str] = {
    # alpha-engine-config-I10545: was "MorningEnrich" / "DataPhase1" (the
    # always-~0s dispatch states) — renamed to match
    # STATE_DURATION_FLOORS_SEC's move to the poll states that actually span
    # the workload.
    "WaitForMorningEnrich": "ne-weekly-freshness-pipeline",
    "WaitForDataPhase1": "ne-weekly-freshness-pipeline",
    # alpha-engine-config-I10574: was "RAGIngestion" / "PredictorTraining" /
    # "Backtester" — same dispatch-vs-poll defect as the two above, caught a
    # PR later. See the module docstring's "MEASURE THE SAME SPAN" section.
    "WaitForRAGIngestion": "ne-weekly-freshness-pipeline",
    "WaitForPredictorTraining": "ne-weekly-freshness-pipeline",
    "WaitForBacktester": "ne-weekly-freshness-pipeline",
    "ModelZooTrainMap": "ne-weekly-freshness-pipeline",
    "PollMorningEnrichSpot": "ne-preopen-trading-pipeline",
    "PollMorningArcticAppendSpot": "ne-preopen-trading-pipeline",
    "Scanner": "ne-preopen-trading-pipeline",
}

#: Weekly-machine executions whose NAME does not match this pattern are ad
#: hoc reruns/backfills (`watch-rerun-*`, `offcycle-shell-*`,
#: `friday-shell-*`, `recovery-*`, bare single UUIDs), not the daily
#: EventBridge-triggered run — see the module docstring's "CANONICAL
#: EXECUTIONS ONLY" section. The trigger names every execution it starts
#: `<uuid>_<uuid>`: two UUID-shaped segments joined by "_".
_CANONICAL_EXECUTION_NAME_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

#: State machines whose SUCCEEDED-execution history mixes ad hoc reruns with
#: the scheduled trigger (measured true for the weekly machine only —
#: I10574). Scoped deliberately: applying an unmeasured filter to the
#: weekday machines would silently change PollMorningEnrichSpot/
#: PollMorningArcticAppendSpot's already-recalibrated (I10164) sample
#: population for no measured reason.
_CANONICAL_NAME_SCOPED_MACHINES = frozenset({"ne-weekly-freshness-pipeline"})

#: States whose Task output carries a companion SSM-poll-result dict (the raw
#: ssm:GetCommandInvocation shape: Status/ResponseCode/StatusDetails/
#: StandardErrorContent) that is ground truth for whether the underlying spot
#: command actually succeeded, independent of the state's own Task exit and
#: independent of the SF execution's overall terminal status. A state absent
#: here is calibrated from raw duration alone — see the module docstring.
#: A newly poll-backed state gets an entry here in the same PR that adds it.
KNOWN_POLL_STATUS_KEYS: Mapping[str, str] = {
    "PollMorningEnrichSpot": "morning_enrich_poll",
    "PollMorningArcticAppendSpot": "arctic_append_poll",
}

#: Below this many GENUINE samples, a floor is UNMEASURABLE — never defaulted
#: to 0, never dropped from the reported set, always recorded as its own
#: status so a state new to production doesn't masquerade as either "fine" or
#: "gone missing".
MIN_SAMPLES = 10

#: Recommended floor = measured genuine minimum * (1 - MARGIN). Matches the
#: ~15-19% margin both I10164 recalibrations used by hand; codified here so
#: future recalibrations are consistent rather than picked per incident.
MARGIN = 0.15

#: A live floor is reported as drifted only outside this relative band around
#: the recommended value, so ordinary week-to-week sample noise (a handful of
#: new executions shifting the min by a few seconds) doesn't flap the check.
DRIFT_TOLERANCE = 0.20

#: A distribution this tight is treated as DEGENERATE rather than trusted
#: blindly — e.g. every sample coming from one synthetic backfill, or a
#: state that always completes in lockstep with a fixed-length Wait. Flagged
#: for human review; no recommendation is computed from it.
DEGENERATE_SPREAD_RATIO = 1.02


@dataclass(frozen=True)
class FloorRecommendation:
    state_name: str
    status: str  # "ok" | "drift_tighten" | "drift_loosen" | "unmeasurable" | "degenerate"
    current_floor_sec: int
    n_genuine: int
    n_excluded: int
    min_sec: Optional[float] = None
    p10_sec: Optional[float] = None
    median_sec: Optional[float] = None
    p90_sec: Optional[float] = None
    max_sec: Optional[float] = None
    recommended_floor_sec: Optional[int] = None
    basis: str = ""
    #: Which HistoryEvent pair was measured (I10574 deliverable 3) — always
    #: this shape today, named explicitly rather than left implicit so a
    #: --check reader never has to open this module to know what a number
    #: means.
    event_pair: str = ""
    #: Human-readable breakdown of WHY excluded samples were excluded (e.g.
    #: "3 zero-duration, 5 non-canonical-execution") — "" when n_excluded==0.
    exclusion_breakdown: str = ""

    @property
    def is_drift(self) -> bool:
        return self.status in ("drift_tighten", "drift_loosen")


def _percentile(sorted_values: Sequence[float], p: float) -> float:
    n = len(sorted_values)
    idx = min(n - 1, max(0, int(round(p * (n - 1)))))
    return sorted_values[idx]


def collect_state_duration_samples(
    sf_client: Any,
    state_machine_arn: str,
    state_names: Sequence[str],
    *,
    fetch_history: Any,
) -> Dict[str, List[Dict[str, Any]]]:
    """Per-state list of ``{"duration_sec": float, "poll_status": Optional[str],
    "exclusion_reason": Optional[str]}`` samples across every SUCCEEDED
    execution of ``state_machine_arn``.

    ``fetch_history(sf_client, execution_arn) -> List[dict]`` is injected
    (rather than calling ``execution_digest.fetch_execution_history``
    directly) so tests can supply canned event lists without a live SF
    execution to point at — the same shape ``build_execution_digest`` already
    takes ``sf_client``/``s3_client`` as parameters for.

    ``exclusion_reason`` is set here (never inferred later by
    ``compute_recommendation`` alone) for the two reasons this function can
    already see from one execution's events without needing the poll-status
    cross-check: a zero-duration sample (a dispatch Task, not the poll span —
    see module docstring "MEASURE THE SAME SPAN"), and a non-canonical
    execution name on a machine in ``_CANONICAL_NAME_SCOPED_MACHINES`` (an ad
    hoc rerun/backfill — see "CANONICAL EXECUTIONS ONLY"). A poll-status
    exclusion (state has a ``KNOWN_POLL_STATUS_KEYS`` entry) is layered on
    top by ``compute_recommendation``, which is the only place that already
    special-cases it.
    """
    machine_name = state_machine_arn.rsplit(":", 1)[-1]
    canonical_only = machine_name in _CANONICAL_NAME_SCOPED_MACHINES

    samples: Dict[str, List[Dict[str, Any]]] = {name: [] for name in state_names}
    paginator = sf_client.get_paginator("list_executions")
    executions: List[dict] = []
    for page in paginator.paginate(stateMachineArn=state_machine_arn, statusFilter="SUCCEEDED"):
        executions.extend(page.get("executions", []))

    for execution in executions:
        exec_name = execution.get("name") or ""
        non_canonical = canonical_only and not _CANONICAL_EXECUTION_NAME_RE.match(exec_name)
        events = fetch_history(sf_client, execution["executionArn"])
        durations = parse_task_state_durations(events)
        last_outputs = _last_task_outputs(events, state_names)
        for name in state_names:
            if name not in durations:
                continue
            duration = durations[name]
            poll_status = None
            poll_key = KNOWN_POLL_STATUS_KEYS.get(name)
            if poll_key:
                output = last_outputs.get(name)
                if isinstance(output, dict):
                    poll_status = (output.get(poll_key) or {}).get("Status")
            exclusion_reason: Optional[str] = None
            if duration <= 0:
                exclusion_reason = (
                    "zero-duration sample — this Task fires a dispatch "
                    "(SSM/spot command) and returns; the workload runs in a "
                    "companion poll state, not here"
                )
            elif non_canonical:
                exclusion_reason = (
                    f"non-canonical execution name {exec_name!r} — an ad hoc "
                    "rerun/backfill/offcycle-shell, not the scheduled "
                    "EventBridge-triggered run; its bootstrap/skip semantics "
                    "differ from a genuine weekly pass"
                )
            samples[name].append(
                {
                    "duration_sec": duration,
                    "poll_status": poll_status,
                    "exclusion_reason": exclusion_reason,
                }
            )
    return samples


def _last_task_outputs(events: Sequence[dict], state_names: Sequence[str]) -> Dict[str, dict]:
    import json as _json

    wanted = set(state_names)
    last: Dict[str, dict] = {}
    for event in events:
        if event.get("type") != "TaskStateExited":
            continue
        detail = event.get("stateExitedEventDetails") or {}
        name = detail.get("name")
        if name not in wanted:
            continue
        raw = detail.get("output")
        if not raw:
            continue
        try:
            last[name] = _json.loads(raw)
        except (ValueError, TypeError):
            continue
    return last


def compute_recommendation(
    state_name: str,
    samples: Sequence[Mapping[str, Any]],
    current_floor_sec: int,
) -> FloorRecommendation:
    """One state's recommendation, from its collected samples.

    ``samples`` items carry ``duration_sec``, an optional ``exclusion_reason``
    set by ``collect_state_duration_samples`` (zero-duration or non-canonical
    execution — see that function's docstring), and (when the state has a
    ``KNOWN_POLL_STATUS_KEYS`` entry) ``poll_status``. A sample is GENUINE
    only when it carries no ``exclusion_reason`` AND (no companion poll key
    is declared OR ``poll_status == "Success"``). Every excluded sample keeps
    its reason — never silently dropped, never averaged in.
    """
    has_poll_key = state_name in KNOWN_POLL_STATUS_KEYS
    genuine: List[float] = []
    excluded: List[Mapping[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    for s in samples:
        reason = s.get("exclusion_reason")
        bucket = "zero-duration" if reason and "zero-duration" in reason else (
            "non-canonical-execution" if reason and "non-canonical" in reason else None
        )
        if reason is None and s.get("duration_sec", 0) <= 0:
            # Defense in depth: collect_state_duration_samples already tags
            # this, but compute_recommendation never trusts an upstream
            # caller to have done so — a 0s sample is excluded here
            # unconditionally (I10574 deliverable 1).
            reason = (
                "zero-duration sample — this Task fires a dispatch "
                "(SSM/spot command) and returns; the workload runs in a "
                "companion poll state, not here"
            )
            bucket = "zero-duration"
        if reason is None and has_poll_key and s.get("poll_status") != "Success":
            reason = (
                f"poll_status={s.get('poll_status')!r} (not 'Success') — a "
                "broken-but-SUCCEEDED run"
            )
            bucket = "poll-status-failure"
        if reason is None:
            genuine.append(s["duration_sec"])
        else:
            excluded.append(s)
            reason_counts[bucket or "other"] = reason_counts.get(bucket or "other", 0) + 1

    n_genuine = len(genuine)
    n_excluded = len(excluded)
    exclusion_breakdown = (
        ", ".join(f"{count} {bucket}" for bucket, count in sorted(reason_counts.items()))
        if reason_counts
        else ""
    )
    event_pair = (
        f"TaskStateEntered/TaskStateExited on '{state_name}' "
        "(stateEnteredEventDetails.name / stateExitedEventDetails.name), "
        "first entry -> last exit"
    )

    if n_genuine < MIN_SAMPLES:
        return FloorRecommendation(
            state_name=state_name,
            status="unmeasurable",
            current_floor_sec=current_floor_sec,
            n_genuine=n_genuine,
            n_excluded=n_excluded,
            event_pair=event_pair,
            exclusion_breakdown=exclusion_breakdown,
            basis=(
                f"only {n_genuine} genuine sample(s), below MIN_SAMPLES={MIN_SAMPLES} — "
                "no recommendation computed; floor left as-is and recorded unmeasurable, "
                "never defaulted to 0 and never dropped from the report"
            ),
        )

    genuine_sorted = sorted(genuine)
    min_sec = genuine_sorted[0]
    p10_sec = _percentile(genuine_sorted, 0.10)
    median_sec = statistics.median(genuine_sorted)
    p90_sec = _percentile(genuine_sorted, 0.90)
    max_sec = genuine_sorted[-1]

    if min_sec > 0 and (max_sec / min_sec) < DEGENERATE_SPREAD_RATIO:
        return FloorRecommendation(
            state_name=state_name,
            status="degenerate",
            current_floor_sec=current_floor_sec,
            n_genuine=n_genuine,
            n_excluded=n_excluded,
            min_sec=min_sec,
            p10_sec=p10_sec,
            median_sec=median_sec,
            p90_sec=p90_sec,
            max_sec=max_sec,
            event_pair=event_pair,
            exclusion_breakdown=exclusion_breakdown,
            basis=(
                f"spread max/min = {max_sec / min_sec:.4f} < {DEGENERATE_SPREAD_RATIO} — "
                "distribution implausibly tight (a synthetic/backfill-only sample set, or a "
                "state gated by a fixed Wait); flagged for human review, no floor recommended"
            ),
        )

    recommended = max(0, int(round(min_sec * (1 - MARGIN))))
    lower = recommended * (1 - DRIFT_TOLERANCE)
    upper = recommended * (1 + DRIFT_TOLERANCE)
    if lower <= current_floor_sec <= upper:
        status = "ok"
    elif current_floor_sec < lower:
        status = "drift_tighten"  # current floor too loose to catch a real hollow run
    else:
        status = "drift_loosen"  # current floor sits at/above genuine runs — false positives

    return FloorRecommendation(
        state_name=state_name,
        status=status,
        current_floor_sec=current_floor_sec,
        n_genuine=n_genuine,
        n_excluded=n_excluded,
        min_sec=min_sec,
        p10_sec=p10_sec,
        median_sec=median_sec,
        p90_sec=p90_sec,
        max_sec=max_sec,
        recommended_floor_sec=recommended,
        event_pair=event_pair,
        exclusion_breakdown=exclusion_breakdown,
        basis=(
            f"recommended = round(min_genuine * (1-{MARGIN})) = "
            f"round({min_sec:.1f} * {1 - MARGIN}) = {recommended}s; "
            f"current {current_floor_sec}s is "
            f"{'within' if status == 'ok' else 'outside'} the "
            f"+/-{int(DRIFT_TOLERANCE * 100)}% tolerance band "
            f"[{lower:.0f}s, {upper:.0f}s]"
        ),
    )


def compute_all_recommendations(
    samples_by_state: Mapping[str, Sequence[Mapping[str, Any]]],
) -> List[FloorRecommendation]:
    """One recommendation per entry in STATE_DURATION_FLOORS_SEC — the DERIVED
    set, not a hand-kept subset. A state present in the codified floors table
    but missing from ``samples_by_state`` (no live samples reachable, e.g. an
    unrecognized state machine) is still reported, as unmeasurable with
    n=0 — never silently dropped.
    """
    recs = []
    for state_name, floor_sec in STATE_DURATION_FLOORS_SEC.items():
        recs.append(
            compute_recommendation(
                state_name, samples_by_state.get(state_name, []), floor_sec
            )
        )
    return recs


def render_report(recommendations: Sequence[FloorRecommendation]) -> str:
    lines = [
        "state,status,current_floor_sec,n_genuine,n_excluded,recommended_floor_sec,"
        "event_pair,exclusion_breakdown,basis"
    ]
    for rec in recommendations:
        lines.append(
            ",".join(
                [
                    rec.state_name,
                    rec.status,
                    str(rec.current_floor_sec),
                    str(rec.n_genuine),
                    str(rec.n_excluded),
                    "" if rec.recommended_floor_sec is None else str(rec.recommended_floor_sec),
                    f'"{rec.event_pair}"',
                    f'"{rec.exclusion_breakdown}"',
                    f'"{rec.basis}"',
                ]
            )
        )
    return "\n".join(lines)


def _default_fetch_history(sf_client: Any, execution_arn: str) -> List[dict]:
    # alpha-engine-config-I10545: fetch_execution_history now returns
    # (events, truncated) so callers can surface truncation; this adapter
    # keeps collect_state_duration_samples' injected
    # ``fetch_history(sf_client, execution_arn) -> List[dict]`` contract
    # unchanged. Calibration is a batch/offline tool over many executions —
    # a truncated sample here silently understates one execution's duration
    # rather than corrupting a live alert, and floor_calibration.py's own
    # min-based recommendation logic is naturally robust to an UNDERSTATED
    # outlier (it would only pull the recommended floor down, not mask a
    # genuinely broken run); a live truncation is caught where it matters,
    # in execution_digest.build_execution_digest.
    from execution_digest import fetch_execution_history

    events, _truncated = fetch_execution_history(sf_client, execution_arn)
    return events


def run_check(sf_client: Any) -> List[FloorRecommendation]:
    """Pull live samples for every state machine referenced in
    STATE_TO_STATE_MACHINE and return the full recommendation set."""
    states_by_machine: Dict[str, List[str]] = {}
    for state_name, machine in STATE_TO_STATE_MACHINE.items():
        states_by_machine.setdefault(machine, []).append(state_name)

    samples_by_state: Dict[str, List[Dict[str, Any]]] = {}
    for machine, state_names in states_by_machine.items():
        arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{machine}"
        collected = collect_state_duration_samples(
            sf_client, arn, state_names, fetch_history=_default_fetch_history
        )
        samples_by_state.update(collected)

    return compute_all_recommendations(samples_by_state)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any floor has drifted or gone degenerate.",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print the full recommendation table (CSV) and exit 0.",
    )
    args = parser.parse_args(argv)

    import boto3

    sf_client = boto3.client("stepfunctions", region_name=REGION)
    recommendations = run_check(sf_client)
    report = render_report(recommendations)
    print(report)

    if args.report and not args.check:
        return 0

    findings = [r for r in recommendations if r.is_drift or r.status == "degenerate"]
    if findings:
        print("\nFINDINGS:", file=sys.stderr)
        for rec in findings:
            print(f"  {rec.state_name}: {rec.status} — {rec.basis}", file=sys.stderr)
        return 1
    print("\nevery floor is within tolerance or unmeasurable", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
