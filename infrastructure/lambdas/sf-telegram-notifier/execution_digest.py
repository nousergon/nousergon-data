"""Step Functions execution history digest for sf-telegram-notifier (config#1672)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

S3_BUCKET = "alpha-engine-research"

# Minimum plausible wall-clock duration (seconds) for spot workload Task states.
STATE_DURATION_FLOORS_SEC: Mapping[str, int] = {
    "MorningEnrich": 15 * 60,
    "DataPhase1": 15 * 60,
    "RAGIngestion": 10 * 60,
    "PredictorTraining": 20 * 60,
    "Backtester": 10 * 60,
    "ModelZooRotation": 8 * 60,
}

# Display order for digest lines (unknown states sort after, alphabetically).
DIGEST_STATE_ORDER: Tuple[str, ...] = (
    "MorningEnrich",
    "DataPhase1",
    "RAGIngestion",
    "ResearchPredictorParallel",
    "PredictorTraining",
    "DataPhase2",
    "Backtester",
    "Parity",
    "ModelZooRotation",
    "Evaluator",
    "ReportCard",
)

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
    etype = event.get("type")
    if etype == "TaskStateEntered":
        return (event.get("taskStateEnteredEventDetails") or {}).get("name")
    if etype == "TaskStateExited":
        return (event.get("taskStateExitedEventDetails") or {}).get("name")
    return None


def parse_task_state_durations(events: Sequence[dict]) -> Dict[str, int]:
    """Return max wall-clock seconds per Task state name from history events."""
    entered_at: Dict[str, datetime] = {}
    durations: Dict[str, int] = {}

    for event in events:
        etype = event.get("type")
        name = _state_name_from_event(event)
        if not name:
            continue
        ts = event.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        if etype == "TaskStateEntered":
            entered_at[name] = ts
        elif etype == "TaskStateExited" and name in entered_at:
            delta = int((ts - entered_at[name]).total_seconds())
            durations[name] = max(durations.get(name, 0), delta)
            entered_at.pop(name, None)

    return durations


def _sort_key(name: str) -> Tuple[int, str]:
    try:
        return (DIGEST_STATE_ORDER.index(name), name)
    except ValueError:
        return (len(DIGEST_STATE_ORDER), name)


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
        if name not in STATE_DURATION_FLOORS_SEC and name not in DIGEST_STATE_ORDER:
            continue
        floor = None if is_preflight else STATE_DURATION_FLOORS_SEC.get(name)
        floor_breach = bool(floor is not None and secs < floor)
        attestation_failed = False
        if not is_preflight and s3_client is not None:
            if name == "PredictorTraining" and name in durations_sec:
                attestation_failed = not _attest_predictor_training(
                    s3_client, execution_start=execution_start
                )
            elif name == "Backtester" and run_date and name in durations_sec:
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
    rows.sort(key=lambda r: _sort_key(r.name))
    return rows


def format_digest_lines(rows: Sequence[StateDuration]) -> List[str]:
    if not rows:
        return ["_(no workload states in history)_"]
    lines: List[str] = []
    for row in rows:
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
    return lines


def fetch_execution_history(
    sf_client: Any,
    execution_arn: str,
    *,
    max_pages: int = 20,
) -> List[dict]:
    events: List[dict] = []
    token: Optional[str] = None
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
        logger.warning(
            "execution history pagination capped at %s pages for %s",
            max_pages,
            execution_arn,
        )
    return events


def build_execution_digest(
    *,
    execution_arn: str,
    is_preflight: bool,
    execution_start_ms: int | None,
    run_date: Optional[str],
    sf_client: Any,
    s3_client: Any | None,
) -> Tuple[List[str], bool]:
    """Build digest lines + hollow_suspect flag for terminal SF notifications."""
    if not execution_arn:
        return ["_(digest unavailable: missing executionArn)_"], False
    try:
        events = fetch_execution_history(sf_client, execution_arn)
    except Exception as exc:  # noqa: BLE001
        logger.error("get_execution_history failed for %s: %s", execution_arn, exc)
        return ["_(digest unavailable: history fetch failed)_"], False

    durations = parse_task_state_durations(events)
    execution_start = _ms_to_datetime(execution_start_ms) or datetime.now(tz=timezone.utc)
    rows = build_state_durations(
        durations,
        is_preflight=is_preflight,
        execution_start=execution_start,
        run_date=run_date,
        s3_client=s3_client,
    )
    hollow = any(r.anomaly for r in rows) if not is_preflight else False
    return format_digest_lines(rows), hollow


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
