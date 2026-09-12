"""Step Functions execution history digest for sf-telegram-notifier (config#1672)."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

S3_BUCKET = "alpha-engine-research"

# Minimum plausible wall-clock duration (seconds) for spot workload Task states.
#
# A FLOOR IS AN ANNOTATION, NOT AN ADMISSION TICKET (alpha-engine-config-I6857).
# A state absent from this mapping renders with no floor; it is never dropped.
# The reverse — treating this mapping and DIGEST_STATE_ORDER below as a
# whitelist — is why every weekday alert read "(no workload states in
# history)" on every run, success or failure: both collections list weekly
# pipeline state names only, and not one preopen or postclose state appears
# in either.
STATE_DURATION_FLOORS_SEC: Mapping[str, int] = {
    # Weekly — ne-weekly-freshness-pipeline. Floors sit on the POLL states
    # (WaitForMorningEnrich / WaitForDataPhase1), not the SSM sendCommand
    # DISPATCH states (MorningEnrich / DataPhase1) — same defect class as the
    # weekday PollMorningEnrichSpot/PollMorningArcticAppendSpot floors below:
    # a dispatch Task returns in well under a second having only sent the
    # command, so a floor there fires on every healthy run. RECALIBRATED
    # 2026-09-12 (alpha-engine-config-I10545): the prior 15m floors sat on
    # "MorningEnrich" / "DataPhase1" and breached on every run unconditionally
    # — measured directly on the 09-12 execution
    # (51f6aa74-939a-ba89-22a6-751c65d5f9e3_1067770b-6b63-aa1b-8b21-891b904fcdd0):
    # MorningEnrich 0.22s, DataPhase1 0.24s, both always ~0s. The real
    # workload is the poll loop; measured first-entry->last-exit span of the
    # poll states across the last four canonical weekly runs (2026-08-22,
    # 08-29, 09-05, 09-12):
    #   WaitForMorningEnrich: 31.7 / 34.3 / 35.3 / 35.3 min (min 31.7min = 1902s)
    #   WaitForDataPhase1:    83.6 / 87.2 / 88.2 / 103.4 min (min 83.6min = 5016s)
    # New floors, ~20% below each measured minimum (n=4, smaller sample than
    # the weekday recalibrations above, so the wider margin): WaitForMorningEnrich
    # 1902*0.8=1521.6s -> 1500s (25m, ~21% below); WaitForDataPhase1
    # 5016*0.8=4012.8s -> 4020s (67m, ~19.8% below). The dispatch states'
    # floors are dropped entirely — an always-~0s state has no plausible
    # non-zero floor to set.
    "WaitForMorningEnrich": 25 * 60,
    "WaitForDataPhase1": 67 * 60,
    # RAGIngestion / PredictorTraining / Backtester RENAMED to their
    # WaitForX poll companions 2026-09-12 (alpha-engine-config-I10574) — same
    # defect class as WaitForMorningEnrich/WaitForDataPhase1 above, just not
    # caught in the same pass. Each of these three names a DISPATCH Task
    # (fires an SSM/spot command and returns) that measures 0.2-0.3s on every
    # execution that reaches it; the real multi-minute-to-hour workload runs
    # in a poll loop entered immediately after under "WaitForRAGIngestion" /
    # "WaitForPredictorTraining" / "WaitForBacktester" — confirmed against the
    # one genuine EventBridge-triggered SUCCEEDED execution in the account's
    # full history carrying these states (9b34ac0f.../2026-08-08):
    #   PredictorTraining 0.197s -> WaitForPredictorTraining 422s (7m2s)
    #   RAGIngestion       0.189s -> WaitForRAGIngestion       1057s (17m37s)
    #   Backtester         0.238s -> WaitForBacktester          663s (11m3s)
    # The floor values below are UNCHANGED (not recalibrated) — only the key
    # each floors now names is corrected. The account's SUCCEEDED-execution
    # history for this state machine has exactly one canonical (EventBridge-
    # triggered, name matching `<uuid>_<uuid>`) execution, below
    # floor_calibration.MIN_SAMPLES — floor_calibration.py now filters
    # ad hoc watch-rerun/offcycle-shell/friday-shell/recovery reruns out of
    # the weekly machine's genuine population (they run a different,
    # shorter bootstrap/skip path — see floor_calibration.py module
    # docstring), so these three (and the two above) correctly report
    # "unmeasurable" rather than a fabricated recommendation until more
    # genuine canonical runs accumulate.
    "WaitForRAGIngestion": 10 * 60,
    "WaitForPredictorTraining": 20 * 60,
    "WaitForBacktester": 10 * 60,
    # Was "ModelZooRotation", a state no definition has had for some time —
    # the rotation is ModelZooSelect -> ModelZooTrainMap ("ModelZooResolve"
    # is a phase LABEL in an error extractor, not a state),
    # and the floor sat on the training map's old name, annotating nothing.
    # Same drift class as I6857 itself, caught by
    # test_every_weekday_state_name_in_the_order_list_exists_in_a_definition.
    "ModelZooTrainMap": 8 * 60,
    # Weekday — ne-preopen-trading-pipeline. Floors sit on the POLL states,
    # not the Launch states: a Launch returns in ~20s having only dispatched
    # the spot request, so a floor there would fire on every healthy run,
    # while the poll loop is what actually spans the workload.
    #
    # PollMorningEnrichSpot RECALIBRATED 2026-09-08 (alpha-engine-config-I10164):
    # the prior 8m floor was never measured — `git log -S` on this file shows it
    # was introduced in one commit (7430b545, config-I6857) that reused the
    # unrelated ModelZooTrainMap weekly floor's literal, with no distribution
    # pulled. It fired 🟡 on a SUCCEEDED 2026-09-08 run
    # (98e1983a-1845-4867-9eac-e55f2cab26cb) that completed in 4m. Measured
    # against all 34 SUCCEEDED ne-preopen-trading-pipeline executions in the
    # account's full history (76 executions, no earlier page) that carried this
    # state: min 106.8s / p10 228.2s / median 273.7s / p90 516.9s / max 1169.6s.
    # The 8m (480s) floor sat ABOVE the median — most healthy runs breached it.
    # Cross-checked against workload OUTPUT, not just duration: the SSM command
    # output for the 4m 2026-09-08 run and for two of the slowest runs on record
    # (cf9ff0a5, 15.7m; ae4532ac, 19.5m) all show the same ~903-ticker
    # constituents universe and ~924-929/929 "Polygon grouped-daily" coverage —
    # duration does not track work done here, so a fast run is not a
    # short-scope run. New floor: 90s, ~15% below the measured minimum (106.8s)
    # of a genuine run, so normal variance clears it while a truly degenerate
    # run — the spot dispatcher returning without launching real work, or the
    # SSM command dying before the constituents fetch even starts (all 34
    # measured runs take >=106.8s to reach that point) — still trips it.
    # PollMorningArcticAppendSpot's own 8m floor was NOT touched here: it is a
    # different state with its own distribution, out of scope for this
    # investigation and tracked separately in I10164.
    "PollMorningEnrichSpot": 90,
    #
    # PollMorningArcticAppendSpot RECALIBRATED 2026-09-08 (alpha-engine-config-
    # I10164 part 1). The prior 8m (480s) floor was ALSO unmeasured (same
    # config-I6857 commit, same reused ModelZooTrainMap literal) — but unlike
    # PollMorningEnrichSpot it never produced a false positive, because it sat
    # BELOW every genuine run rather than above the median. That is not
    # evidence it was right: it means the floor was too LOW to catch a real
    # failure, the mirror-image defect.
    #
    # Measured against all 34 SUCCEEDED ne-preopen-trading-pipeline executions
    # in the account's full history that carried this state — but duration
    # alone does not separate genuine from broken here the way it does for
    # PollMorningEnrichSpot: this state's Task output carries a companion
    # `arctic_append_poll.Status` field (the raw ssm:GetCommandInvocation
    # result), which is ground truth for whether the spot command itself
    # actually succeeded. Splitting on THAT (not just duration) found:
    #   Success (n=29): min 1474.9s / p10 1535.4s / median 1919.4s /
    #     p90 2403.1s / max 5595.5s.
    #   Failed  (n=5): 121.3s, 260.3s, 929.7s, 3806.4s, 4808.3s.
    # The three short Failed runs are a genuinely broken spot command (SSM
    # StandardErrorContent: "WARNING: The directory '/home/ec2-user/.cache/
    # pip' ... failed to run commands: exit status 1", and one "Undeliverable")
    # that the SF execution still recorded as SUCCEEDED overall — an
    # independent verification that a FAST run here is not a short-scope run,
    # it is a broken one, exactly the class part 2's mechanism exists to keep
    # catching. The old 480s floor caught two of the three (121.3s, 260.3s)
    # but MISSED the third (929.7s > 480s) — a false negative on a confirmed-
    # broken run, not a hypothetical.
    # New floor: 1200s (20m), ~19% below the measured genuine minimum
    # (1474.9s) — clears every one of the 29 genuine runs with margin, and now
    # catches all three known-broken short runs (121.3s, 260.3s, 929.7s < 1200s
    # all breach). The two long-duration Failed runs (3806.4s, 4808.3s) are
    # NOT caught by any duration floor — a slow failure is a different
    # detection problem (an attestation/output check, not a minimum-duration
    # check) and is filed separately (alpha-engine-config-I10189), not folded
    # into this recalibration.
    "PollMorningArcticAppendSpot": 20 * 60,
    "Scanner": 60,
}

# Display order for digest lines. States NOT listed here still render — they
# sort after these, longest-running first. See _sort_key.
DIGEST_STATE_ORDER: Tuple[str, ...] = (
    # Weekly. "MorningEnrich" / "DataPhase1" (the dispatch states) are
    # deliberately NOT listed here (alpha-engine-config-I10545) — they carry
    # no information, always ~0s. The poll states that actually span the
    # workload lead the order instead.
    "WaitForMorningEnrich",
    "WaitForDataPhase1",
    "WaitForRAGIngestion",
    "ResearchPredictorParallel",
    "WaitForPredictorTraining",
    "DataPhase2",
    "WaitForBacktester",
    # "Parity" and "ModelZooRotation" were stale: the states were split into
    # the names below and this list was never updated, so both entries
    # ordered nothing for however long that has been true.
    "ParityParallel",
    "PitParityCompare",
    "ModelZooSelect",
    "ModelZooTrainMap",
    # config-I3112 deliverable 3: the single Evaluator state became two,
    # and the digest orders by this list — a stale single entry would
    # order NEITHER half, the same way the "Parity"/"ModelZooRotation"
    # entries above silently ordered nothing after their own splits.
    "EvaluatorDiagnostics",
    "EvaluatorOptimize",
    "ReportCard",
    # Weekday preopen, in pipeline order
    "StartExecutorEC2",
    "CodeFreshnessGate",
    "LaunchMorningEnrichSpot",
    "PollMorningEnrichSpot",
    "LaunchMorningArcticAppendSpot",
    "PollMorningArcticAppendSpot",
    "Scanner",
    "PredictorInference",
    "CheckPredictorCoverage",
    "RunMorningPlanner",
    "RunDaemon",
)

# The pipelines' terminal error-handling Task states — the one that SENDS
# the failure alert, not the one that failed. All three definitions
# (step_function.json / step_function_daily.json / step_function_eod.json)
# route every Catch/failure path through a Task named "HandleFailure" (an
# sns:publish immediately before the terminal Fail state) — verified against
# all three committed definitions, not assumed from one pipeline
# (alpha-engine-config-I9742 deliverable 4). With the key fix above,
# last_workload_state_entered's fallback now fires correctly on every
# hard-failure run; without this exclusion the fallback would name
# "HandleFailure" itself on every one of them, converting an alert naming
# nothing into an alert naming the alerter rather than the state that broke.
TERMINAL_ERROR_HANDLING_STATES: frozenset[str] = frozenset({
    "HandleFailure",
    # `HandleFailure` is not the only alerter. Each weekday pipeline refuses an
    # unconsidered in-session start through its own sns:publish Task, and those
    # are the last Task ENTERED on exactly the runs where the digest is read.
    # Excluding only HandleFailure fixes the common path and leaves the
    # market-hours refusals naming the alerter — the same defect, narrower.
    "NotifyMarketHoursBlocked",
    "NotifyMarketHoursOverrideMalformed",
    "NotifyMarketHoursUnverified",
    "TradingDayGateFailed",
    "WeeklyRunDayGateFailed",
})

# The counterpart set, and the reason the one above can be a literal at all.
# Some Task states also sit immediately before a terminal `Fail` and are NOT
# alerters — they do the pipeline's real work on the way out, so naming one is
# informative rather than circular:
#
#   ForceStopInstance              stops the trading box (a spend leak if it
#                                  didn't run) — eod's cost-guard tail.
#   WriteCompletionMarkerDegraded  writes the run's DEGRADED completion marker,
#   WriteCompletionMarkerDegradedCalendar   which is the artifact every
#                                  status-keyed consumer reads.
#
# Declared rather than inferred because `Next` points at a `Fail` state in both
# cases, so no structural rule separates them — only what the state DOES does,
# and that is a judgment this file must state out loud. Together the two sets
# must EXHAUSTIVELY cover every Task preceding a `Fail` across all three
# definitions; test_execution_digest.py recomputes that set from the committed
# definitions and fails on anything in neither, so a newly added terminal state
# is a red test rather than a wrong name in a red alert
# (alpha-engine-config-I9742 deliverable 4).
WORK_STATES_ON_TERMINAL_PATHS: frozenset[str] = frozenset({
    "ForceStopInstance",
    "WriteCompletionMarkerDegraded",
    "WriteCompletionMarkerDegradedCalendar",
})

# Digest rows kept, before the elision line. Telegram messages are read on a
# phone; a preopen execution touches ~25 distinct Task states and dumping all
# of them buries the one that mattered. Truncation is by RELEVANCE (anomalies
# first, then pipeline order, then longest-running) and is always ANNOUNCED —
# a bound that renders as though it were the whole list is how "covered
# everything" gets asserted about a sample.
_MAX_DIGEST_ROWS = 14

_HISTORY_EVENT_TYPES = (
    "TaskStateEntered",
    "TaskStateExited",
    "PassStateEntered",
    "PassStateExited",
)


@dataclass(frozen=True)
class StateDuration:
    name: str
    duration_sec: int
    floor_sec: Optional[int]
    floor_breach: bool
    attestation_failed: bool

    @property
    def anomaly(self) -> bool:
        return self.floor_breach or self.attestation_failed


def format_duration_short(duration_sec: int) -> str:
    """Human-readable duration for Telegram (e.g. ``47m``, ``2h 5m``)."""
    secs = max(0, int(duration_sec))
    h, rem = divmod(secs, 3600)
    m, _ = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m"
    return f"{secs}s"


def _state_name_from_event(event: dict) -> Optional[str]:
    """Extract the state name Step Functions attaches to a state-transition event.

    ``TaskStateEntered``/``TaskStateExited`` are HistoryEvent *types*; the
    detail object Step Functions actually populates for BOTH is
    ``stateEnteredEventDetails`` / ``stateExitedEventDetails`` — the generic
    state-transition detail shared by every state type (Task, Pass, Wait,
    Choice, ...), not a Task-prefixed one (alpha-engine-config-I9742). A
    plausible ``taskStateEnteredEventDetails`` / ``taskStateExitedEventDetails``
    is NOT a field the API emits — `taskScheduledEventDetails`,
    `taskStartedEventDetails`, `taskSucceededEventDetails` and
    `taskFailedEventDetails` exist for the *Task* event family, which is a
    different set of event types entirely. Reading the wrong key here
    silently returned None for every event, unconditionally, for the life of
    this file.
    """
    etype = event.get("type")
    if etype == "TaskStateEntered":
        return (event.get("stateEnteredEventDetails") or {}).get("name")
    if etype == "TaskStateExited":
        return (event.get("stateExitedEventDetails") or {}).get("name")
    return None


def last_workload_state_entered(events: Sequence[dict]) -> Optional[str]:
    """The last tracked workload state ENTERED, completed or not.

    A run that dies before any workload state EXITS produces no durations, so
    the digest rendered "_(no workload states in history)_" — a failure
    notification that names nothing, on a pipeline with 60+ states. Live
    2026-08-10: MorningEnrich hung in its spot bootstrap and was killed, twice;
    both alerts said only that no state was in history, while the state that
    broke was sitting in the events all along as a TaskStateEntered with no
    matching exit.

    Entered-but-never-exited is exactly the signature of the interesting
    failure — a hang, a timeout, a kill — so it is the one the digest must
    name.

    Every Task state is eligible, not only those in DIGEST_STATE_ORDER
    (alpha-engine-config-I6857). Restricting it to known names meant that on
    the weekday pipelines — whose states appear in neither collection — this
    fallback could never fire either, so a run dying before its first exit
    named nothing on the two pipelines that trade real money. Choice and Pass
    states are still excluded, by _state_name_from_event: naming
    "CheckMorningEnrichSpotStatus" would be true and useless.

    TERMINAL_ERROR_HANDLING_STATES is excluded too (alpha-engine-config-I9742
    deliverable 4): once the key bug above is fixed, ``HandleFailure`` is
    ENTERED on every hard-failure run — it is the state that sends the
    alert, always the last Task state entered on a failed execution. Naming
    it here would report the alerter as the failure, which is worse than the
    "no workload states" placeholder it replaces: that placeholder was at
    least honestly empty, this would be confidently wrong.
    """
    last: Optional[str] = None
    for event in events:
        if event.get("type") != "TaskStateEntered":
            continue
        name = _state_name_from_event(event)
        if not name or name in TERMINAL_ERROR_HANDLING_STATES:
            continue
        last = name
    return last


def parse_task_state_durations(events: Sequence[dict]) -> Dict[str, int]:
    """Wall-clock seconds per Task state name: FIRST entry to LAST exit.

    The span, not the longest single entry/exit pair (alpha-engine-config-I6857).

    The weekday pipelines poll: ``PollMorningEnrichSpot`` is entered and
    exited every 15 seconds for as long as the spot workload runs. Under
    max-per-pair that 15-minute stage reported ``0s`` — technically a row,
    and worse than none, because it says the stage was instant. Under the
    span it reports the elapsed time an operator would recognise.

    For a state entered once the two are identical, so nothing about the
    weekly digest changes. For a RETRIED state the span includes the gap
    between attempts, which is the honest number: the pipeline really did
    spend that wall-clock inside the stage, and the floor comparison should
    see it.
    """
    first_entry: Dict[str, datetime] = {}
    last_exit: Dict[str, datetime] = {}

    for event in events:
        etype = event.get("type")
        name = _state_name_from_event(event)
        if not name:
            continue
        ts = event.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        if etype == "TaskStateEntered":
            first_entry.setdefault(name, ts)
        elif etype == "TaskStateExited":
            last_exit[name] = ts

    return {
        name: max(0, int((last_exit[name] - entered).total_seconds()))
        for name, entered in first_entry.items()
        if name in last_exit
    }


def _sort_key(row: "StateDuration") -> Tuple[int, int, int, str]:
    """Relevance order: anomalies, then pipeline order, then longest-running.

    Anomalies lead because the digest is read on a phone under a red alert,
    and because _MAX_DIGEST_ROWS truncates the tail — a bound that can drop a
    floor breach while keeping a healthy state has inverted its own purpose.

    States outside DIGEST_STATE_ORDER sort after those in it, longest-running
    first. This is the branch the old code's comment promised ("unknown states
    sort after, alphabetically") and the whitelist filter one function away
    made unreachable: nothing unknown ever arrived here to be sorted.
    """
    known = DIGEST_STATE_ORDER.index(row.name) if row.name in DIGEST_STATE_ORDER else None
    return (
        0 if row.anomaly else 1,
        len(DIGEST_STATE_ORDER) if known is None else known,
        -row.duration_sec,
        row.name,
    )


def _ms_to_datetime(ms: int | None) -> Optional[datetime]:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _attest_predictor_training(
    s3_client: Any,
    *,
    execution_start: datetime,
) -> bool:
    key = "predictor/metrics/training_summary_latest.json"
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("predictor training attestation head_object failed: %s", exc)
        return False
    modified = head.get("LastModified")
    if not isinstance(modified, datetime):
        return False
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    return modified >= execution_start


def _attest_backtester(
    s3_client: Any,
    *,
    run_date: str,
    execution_start: datetime,
) -> bool:
    prefix = f"backtest/{run_date}/"
    try:
        resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
    except Exception as exc:  # noqa: BLE001
        logger.warning("backtester attestation list_objects failed: %s", exc)
        return False
    contents = resp.get("Contents") or []
    if not contents:
        return False
    latest = max(contents, key=lambda o: o.get("LastModified") or execution_start)
    modified = latest.get("LastModified")
    if not isinstance(modified, datetime):
        return True
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    return modified >= execution_start


def build_state_durations(
    durations_sec: Mapping[str, int],
    *,
    is_preflight: bool,
    execution_start: datetime,
    run_date: Optional[str],
    s3_client: Any | None,
) -> List[StateDuration]:
    rows: List[StateDuration] = []
    for name, secs in durations_sec.items():
        # NO whitelist filter. Every tracked Task state renders; the two
        # module-level collections supply the floor and the sort position
        # when they happen to know the state, and nothing when they do not
        # (alpha-engine-config-I6857).
        floor = None if is_preflight else STATE_DURATION_FLOORS_SEC.get(name)
        floor_breach = bool(floor is not None and secs < floor)
        attestation_failed = False
        if not is_preflight and s3_client is not None:
            # alpha-engine-config-I10574: was "PredictorTraining" /
            # "Backtester" (the dispatch states, renamed off
            # STATE_DURATION_FLOORS_SEC above) — the attestation trigger
            # must follow the same rename or it silently stops firing:
            # dispatch names still appear in durations_sec (their own ~0.2s
            # span), so a stale name here would attest against a row that
            # completed before the real work even started.
            if name == "WaitForPredictorTraining" and name in durations_sec:
                attestation_failed = not _attest_predictor_training(
                    s3_client, execution_start=execution_start
                )
            elif name == "WaitForBacktester" and run_date and name in durations_sec:
                attestation_failed = not _attest_backtester(
                    s3_client,
                    run_date=run_date,
                    execution_start=execution_start,
                )
        rows.append(
            StateDuration(
                name=name,
                duration_sec=secs,
                floor_sec=floor,
                floor_breach=floor_breach,
                attestation_failed=attestation_failed,
            )
        )
    rows.sort(key=_sort_key)
    return rows


def format_digest_lines(
    rows: Sequence[StateDuration], *, last_entered: Optional[str] = None
) -> List[str]:
    if not rows:
        if last_entered:
            return [f"{last_entered} — entered, never completed ⚠️"]
        return ["_(no workload states in history)_"]
    kept, elided = list(rows[:_MAX_DIGEST_ROWS]), len(rows) - _MAX_DIGEST_ROWS
    lines: List[str] = []
    for row in kept:
        dur = format_duration_short(row.duration_sec)
        if row.anomaly:
            detail = "⚠️"
            if row.floor_breach and row.floor_sec is not None:
                detail = f"⚠️(floor {format_duration_short(row.floor_sec)})"
            if row.attestation_failed:
                detail = "⚠️(no artifact)" if detail == "⚠️" else detail + "+no artifact"
        else:
            detail = "✓"
        lines.append(f"{row.name} {dur} {detail}")
    if elided > 0:
        # Announced, never silent. A truncated list rendered as a whole one
        # is an assertion that nothing else ran.
        lines.append(f"_(+{elided} more state{'s' if elided > 1 else ''}, shortest-running, elided)_")
    return lines


# alpha-engine-config-I10545: a long weekly run's raw GetExecutionHistory is
# NOT a handful of pages — the 2026-09-12 ne-weekly-freshness-pipeline
# execution (51f6aa74-939a-ba89-22a6-751c65d5f9e3_1067770b-6b63-aa1b-8b21-
# 891b904fcdd0) took 42 pages / 8916 events to reach its last event, because
# the API pages by response SIZE (~1MB), not by a fixed event count — passing
# maxResults=1000 does not guarantee anything close to 1000 events per page.
# The old max_pages=20 cap silently truncated that execution's history well
# before WaitForDataPhase1's true last exit, so parse_task_state_durations
# (already first-entry->last-exit, not the bug) computed a real but WRONG
# short span from a partial history — the digest rendered "WaitForDataPhase1
# 34m ✓" against a true 103.35m span, a confidently-wrong number rather than
# an honestly-incomplete one. 150 pages is ~3.6x the measured 42-page need,
# generous headroom against a still-longer future run without letting a
# truly runaway/malformed execution loop unbounded.
_MAX_HISTORY_PAGES = 150

# Rendered when fetch_execution_history exhausts _MAX_HISTORY_PAGES with more
# history still unread. A truncated history can only UNDERSTATE durations for
# any state whose last exit fell past the cutoff, so this must force the
# digest into its anomaly path rather than let a partial computation render
# a plain "✓" (alpha-engine-config-I10545) — the same "confidently wrong is
# worse than honestly incomplete" standard last_workload_state_entered's own
# docstring states for the HandleFailure exclusion.
HISTORY_TRUNCATED_LINE = (
    "_(history truncated at {pages} pages — durations below may be understated)_"
)


def fetch_execution_history(
    sf_client: Any,
    execution_arn: str,
    *,
    max_pages: Optional[int] = None,
) -> Tuple[List[dict], bool]:
    """Returns ``(events, truncated)``. ``truncated`` is True only when
    ``max_pages`` was exhausted with a ``nextToken`` still outstanding —
    never inferred from event count, since a genuinely short execution's
    history legitimately fits in one page.

    ``max_pages`` defaults to the module-level ``_MAX_HISTORY_PAGES``,
    resolved at CALL time rather than bound as a literal default — so tests
    (and any future recalibration) can override the module constant without
    also having to pass it through every call site.
    """
    if max_pages is None:
        max_pages = _MAX_HISTORY_PAGES
    events: List[dict] = []
    token: Optional[str] = None
    truncated = False
    for _ in range(max_pages):
        kwargs: dict[str, Any] = {
            "executionArn": execution_arn,
            "includeExecutionData": False,
        }
        if token:
            kwargs["nextToken"] = token
        resp = sf_client.get_execution_history(**kwargs)
        events.extend(resp.get("events") or [])
        token = resp.get("nextToken")
        if not token:
            break
    else:
        truncated = bool(token)
        logger.warning(
            "execution history pagination capped at %s pages for %s (truncated=%s)",
            max_pages,
            execution_arn,
            truncated,
        )
    return events, truncated


# The ssm-liveness-poller poll-result contract (README.md at
# infrastructure/lambdas/ssm-liveness-poller/): every *_poll key populated by
# that Lambda carries a `verdict` the SF Choice states branch on and, on the
# three terminal-failure verdicts below, a non-empty `detail` string holding
# the actual diagnostic (stderr, timeout description, budget exhaustion).
# `SUCCESS` / `IN_PROGRESS` are excluded deliberately — a detail string under
# either of those is not a failure to surface.
_POLL_FAILURE_VERDICTS: frozenset[str] = frozenset(
    {"COMMAND_FAILED", "INSTANCE_UNRESPONSIVE", "POLL_BUDGET_EXHAUSTED"}
)


def _find_poll_failure_detail(node: Any) -> Optional[str]:
    """Depth-first search for a poll-result dict carrying a real failure detail."""
    if isinstance(node, dict):
        verdict = node.get("verdict")
        detail = node.get("detail")
        if (
            verdict in _POLL_FAILURE_VERDICTS
            and isinstance(detail, str)
            and detail.strip()
        ):
            return detail.strip()
        for value in node.values():
            found = _find_poll_failure_detail(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_poll_failure_detail(item)
            if found:
                return found
    return None


def extract_detailed_failure_cause(events: Sequence[dict]) -> Optional[str]:
    """The real error, from the terminal Fail state's own input — not the
    shared Fail state's boilerplate ``Error``/``Cause`` (sf-pipeline-policy
    §2.3 corollary; alpha-engine-config-I9742 deliverable 5).

    ``describe_execution``'s ``error``/``cause`` on all three pipelines is
    the shared ``FailExecution``/``DailyPipelineFailure``/
    ``EODPipelineFailure`` Fail state's own constant ``Error`` field plus a
    boilerplate ``Cause`` string ("One or more weekday pipeline steps
    failed.") — identical for every failure regardless of what broke. The
    actual diagnostic (e.g. a git push 403) is embedded in the accumulated
    state input HandleFailure's SNS publish already carries, under whichever
    ``*_poll`` key ssm-liveness-poller populated on the way there. Returns
    ``None`` when no such detail is present (a failure the poller contract
    does not cover), never a guess.
    """
    for event in reversed(events):
        if event.get("type") != "FailStateEntered":
            continue
        raw_input = (event.get("stateEnteredEventDetails") or {}).get("input")
        if not raw_input:
            continue
        try:
            import json as _json

            payload = _json.loads(raw_input)
        except (ValueError, TypeError):
            continue
        found = _find_poll_failure_detail(payload)
        if found:
            return found
    return None


def build_execution_digest(
    *,
    execution_arn: str,
    is_preflight: bool,
    execution_start_ms: int | None,
    run_date: Optional[str],
    sf_client: Any,
    s3_client: Any | None,
) -> Tuple[List[str], bool, Optional[str]]:
    """Build digest lines + hollow_suspect flag + detailed failure cause for
    terminal SF notifications."""
    if not execution_arn:
        return ["_(digest unavailable: missing executionArn)_"], False, None
    try:
        events, truncated = fetch_execution_history(sf_client, execution_arn)
    except Exception as exc:  # noqa: BLE001
        logger.error("get_execution_history failed for %s: %s", execution_arn, exc)
        return ["_(digest unavailable: history fetch failed)_"], False, None

    detailed_failure_cause = extract_detailed_failure_cause(events)
    durations = parse_task_state_durations(events)
    execution_start = _ms_to_datetime(execution_start_ms) or datetime.now(tz=timezone.utc)
    rows = build_state_durations(
        durations,
        is_preflight=is_preflight,
        execution_start=execution_start,
        run_date=run_date,
        s3_client=s3_client,
    )
    lines = format_digest_lines(rows, last_entered=last_workload_state_entered(events))
    hollow = any(r.anomaly for r in rows) if not is_preflight else False
    if truncated:
        # A truncated history can only understate a duration, never overstate
        # one — so a truncated-but-otherwise-clean row set is still a
        # confidently wrong "healthy" digest unless this forces the anomaly
        # path (alpha-engine-config-I10545).
        lines.append(HISTORY_TRUNCATED_LINE.format(pages=_MAX_HISTORY_PAGES))
        hollow = True
    return (
        lines,
        hollow,
        detailed_failure_cause,
    )


def parse_run_date_from_input(raw_input: str | None) -> Optional[str]:
    if not raw_input:
        return None
    try:
        import json

        payload = json.loads(raw_input)
    except (ValueError, TypeError):
        return None
    run_date = payload.get("run_date")
    return str(run_date) if run_date else None


_EXECUTION_NAME_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_run_date_from_execution_name(execution_name: str | None) -> Optional[str]:
    """Fallback ``run_date`` extraction from the execution name (nousergon-
    data-i5289) — used when the execution input carries no ``run_date`` key
    (e.g. a manually re-triggered execution, or an input parse failure).

    Every scheduled trigger names its execution ``<prefix>-{run_date}-<epoch>``
    (``eod-2026-08-08-1754...`` — ``daemon.py``'s ``_trigger_eod_pipeline``;
    ``eod-backstop-2026-08-08-...`` — eod-backstop/index.py), so the first
    ISO-8601 date substring in the name is the run_date. Absence of a match
    returns ``None`` — never a guess.
    """
    if not execution_name:
        return None
    match = _EXECUTION_NAME_DATE_RE.search(execution_name)
    return match.group(0) if match else None
