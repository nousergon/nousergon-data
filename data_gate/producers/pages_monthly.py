"""Producer for ``data.pages.monthly`` (`alpha-engine-config-I10788` A9,
plan P-22).

Writes ``metrics/pages/monthly/latest.json`` under
``s3://alpha-engine-research/data_collection``, the key
`data_gate.clauses._clause_pages_monthly` already reads. The requirement is
`data_collection_plan_260914.md` §2 row 11: **pages <= 2 per month outside
declared vendor outages**, counted as a monthly board Signal.

**Where a page is counted from.** Every page this component sends is an SNS
publish to the ``alpha-engine-alerts`` topic, and
`infrastructure/lambdas/changelog-incident-mirror` writes ONE JSON entry per
message on that topic to
``s3://alpha-engine-research/changelog/entries/{YYYY-MM-DD}/{event_id}.json``
(or ``changelog/quarantine/...`` when the entry fails vocab validation). The
mirror is a subscriber of the same topic as the email delivery, so it is a
ledger of what was delivered. Reading it back costs free S3 GETs, with no
CloudWatch or SNS API call anywhere in this module.

**What counts as this component's page** is the plan's page conditions
(§2 row 11), each matched on what the message itself says:

* condition 1, a collection execution FAILED: the CloudWatch alarm transition
  to ``ALARM`` of any alarm named ``ne-data-collection-*``
  (``execution_failed_alarm``), and the state machine's own SNS publishes,
  whose subjects are READ from `infrastructure/step-functions/
  data-collection.asl.json` rather than hand-listed (``execution_failed_notify``).
  One failed execution sends both, and both are counted: each is a separate
  message to a human.
* conditions 2 and 3, a freshness deadline missed (including the gate ladder's
  own row): a ``freshness-monitor`` page naming at least one artifact whose
  key this component writes. That set is DERIVED from the unit descriptors'
  ``writes`` templates plus the component's own ``data_collection/`` store,
  never hand-listed (``observability-policy`` §2.2).
* condition 4, the morning appends not ready for the trader: the pre-open
  pipeline's CollectionReadiness page (``preopen_not_ready``).

Every other message on the topic is another component's and is not counted.

**Vendor outages.** The requirement excludes pages during declared vendor
outages, but no declaration surface exists yet (plan risk 4 names one in the
run manifest ``reason``; nothing writes it). So every page counts. That is
stricter than the requirement, never laxer, and the document says so.

**Days observed.** A day of the month counts as observed when the mirror
holds at least one entry from the alerts topic for it. The topic carries fleet
traffic every day (3 to 19 messages a day, measured 2026-09-20 to 10-07), so a
day with none means the mirror did not run, not that nothing paged; it is
named in ``unobserved_days`` and is not counted toward ``days_observed``.

**Verdict.** ``breach`` once the month's page count exceeds
:data:`MAX_PAGES_PER_MONTH`; otherwise ``ok`` (no day observed so far fails
the target). The clause reads ``status`` / ``days_observed`` /
``days_in_month`` under Brian's 2026-10-03 observation-window ruling.

**Read failures never produce a verdict.** A failed listing, GET or parse
raises; ``main()`` writes an ``error`` run record and exits non-zero, and the
document keeps its last value until ``stale_after_utc`` makes it
UNMEASURABLE.

**Month end.** The scheduled run reads the month up to its own run time, so
the last hours of a month after the final run would never reach that month's
``latest.json``. On the first :data:`CLOSED_MONTH_REWRITE_DAYS` days of a
month the run therefore also writes the CLOSED previous month, complete, to
``metrics/pages/monthly/{YYYY-MM}.json``.
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import fnmatch
import json
import logging
import pathlib
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Iterable

from data_gate.descriptors import Unit, load_units

__all__ = [
    "ALERTS_TOPIC",
    "CLOSED_MONTH_REWRITE_DAYS",
    "COLLECTION_ASL",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "LEDGER_PREFIXES",
    "MAX_PAGES_PER_MONTH",
    "PAGE_CLASSES",
    "PREOPEN_NOT_READY_SUBJECT",
    "MonthReading",
    "PageEvent",
    "build_document",
    "classify_entry",
    "closed_month_key",
    "collection_notify_subjects",
    "compute_documents",
    "component_key_patterns",
    "main",
    "read_month",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/pages/monthly/latest.json"
_CLOSED_MONTH_KEY_TEMPLATE = "data_collection/metrics/pages/monthly/{month}.json"

#: The changelog-incident-mirror's two write prefixes, one dated folder per
#: UTC day (`infrastructure/lambdas/changelog-incident-mirror/index.py`).
LEDGER_PREFIXES: tuple[str, ...] = ("changelog/entries/", "changelog/quarantine/")

#: The topic every page condition publishes to: the four
#: `ne-data-collection-*-failed` alarms' AlarmActions (nous-ergon-ops
#: `infrastructure/cloudwatch/alarms/`), the collection ASL's
#: `${AlertsTopicArn}`, the pre-open pipeline's notify states and the
#: freshness monitor's critical path. The mirror names each entry after it.
ALERTS_TOPIC = "alpha-engine-alerts"

#: `data_collection_plan_260914.md` §2 row 11, ratified with the plan
#: (2026-09-14): "Pages <= 2/month outside declared vendor outages". Restated
#: here from the plan, not chosen here.
MAX_PAGES_PER_MONTH = 2

COLLECTION_ASL = _REPO_ROOT / "infrastructure" / "step-functions" / "data-collection.asl.json"

#: The pre-open pipeline's CollectionReadiness page
#: (`infrastructure/step_function_daily.json` ::
#: PublishDataSpotFailureImmediate). Pinned against that file by
#: `tests/test_pages_monthly_producer.py`, so renaming it there fails a test
#: instead of silently dropping condition 4 from the count.
PREOPEN_NOT_READY_SUBJECT = "Alpha Engine morning collection NOT READY (fail-open, trading continues)"

_COMPONENT_ALARM_PREFIX = "ne-data-collection-"
_FRESHNESS_SUBJECT_SUFFIX = "freshness-monitor"
_COMPONENT_STORE_PATTERN = "data_collection/*"

#: page class -> the §2 row 11 condition it evidences.
PAGE_CLASSES: dict[str, str] = {
    "execution_failed_alarm": "1: a collection execution FAILED (ne-data-collection-* alarm)",
    "execution_failed_notify": "1: a collection execution FAILED (the state machine's own notify)",
    "freshness_deadline_missed": "2/3: a freshness deadline missed on an artifact this component writes",
    "preopen_not_ready": "4: the morning appends the trader reads were not ready pre-open",
}

#: On days 1..N of a month the run also rewrites the closed previous month.
CLOSED_MONTH_REWRITE_DAYS = 3

#: The document is evidence for two missed daily runs, then UNMEASURABLE —
#: the same horizon `data_gate/producers/slo.py` uses.
STALE_AFTER = dt.timedelta(hours=50)

_ARTIFACT_LINE = re.compile(r"artifact_id=(?P<artifact>\S+).*?\bkey=(?P<key>\S+)")


def _iso(instant: dt.datetime) -> str:
    return instant.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# What counts as this component's page — derived from the repository.
# ---------------------------------------------------------------------------


def collection_notify_subjects(asl_path: pathlib.Path = COLLECTION_ASL) -> frozenset[str]:
    """Every ``sns:publish`` Subject in the data-collection state machine
    definition. Raises when there are none: a definition that pages on
    nothing would silently drop condition 1's notify half."""
    definition = json.loads(pathlib.Path(asl_path).read_text())
    subjects: set[str] = set()

    def walk(states: dict) -> None:
        for state in states.values():
            if str(state.get("Resource", "")).startswith("arn:aws:states:::sns:publish"):
                params = state.get("Parameters") or state.get("Arguments") or {}
                subject = params.get("Subject")
                if isinstance(subject, str) and subject:
                    subjects.add(subject)
            for branch in state.get("Branches") or []:
                walk(branch.get("States") or {})
            for nested in ("Iterator", "ItemProcessor"):
                if isinstance(state.get(nested), dict):
                    walk(state[nested].get("States") or {})

    walk(definition.get("States") or {})
    if not subjects:
        raise ValueError(f"{asl_path} declares no sns:publish Subject; condition 1's notify half has no match")
    return frozenset(subjects)


def component_key_patterns(units: Iterable[Unit]) -> tuple[str, ...]:
    """fnmatch patterns for every S3 key this component writes: each
    non-retired unit's ``writes`` templates with ``{placeholder}`` turned into
    ``*`` and a trailing ``/`` into a prefix, plus the component's own
    ``data_collection/`` store (metrics, run records, the gate ladder).

    Entries that are not S3 keys (ArcticDB libraries, ``research.db::table``,
    prose like ``same keys as D15``, another store's ``crucible-v2 store:``
    keys) are skipped: the freshness monitor never names them."""
    patterns: set[str] = {_COMPONENT_STORE_PATTERN}
    for unit in units:
        if unit.retired:
            continue
        for template in unit.raw.get("writes") or []:
            text = str(template).strip()
            if not text or " " in text or "::" in text or text.startswith("arcticdb/"):
                continue
            pattern = re.sub(r"\{[^}]*\}", "*", text)
            if pattern.endswith("/"):
                pattern += "*"
            patterns.add(pattern)
    return tuple(sorted(patterns))


@dataclass(frozen=True)
class PageEvent:
    ts_utc: str
    page_class: str
    subject: str
    event_id: str
    artifacts: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "ts_utc": self.ts_utc,
            "class": self.page_class,
            "condition": PAGE_CLASSES[self.page_class],
            "subject": self.subject,
            "event_id": self.event_id,
        }
        if self.artifacts:
            row["artifacts"] = list(self.artifacts)
        return row


def _component_artifacts(message: str, key_patterns: tuple[str, ...]) -> tuple[str, ...]:
    found: list[str] = []
    for line in message.splitlines():
        match = _ARTIFACT_LINE.search(line)
        if not match:
            continue
        key = match.group("key")
        if any(fnmatch.fnmatchcase(key, pattern) for pattern in key_patterns):
            found.append(match.group("artifact"))
    return tuple(dict.fromkeys(found))


def classify_entry(
    entry: dict[str, Any],
    *,
    notify_subjects: frozenset[str],
    key_patterns: tuple[str, ...],
) -> PageEvent | None:
    """This component's :class:`PageEvent` for one mirror entry, or ``None``
    for a message that is not one of its pages."""
    sns = entry.get("sns") or {}
    subject = str(sns.get("subject") or "")
    message = str(entry.get("description") or "")
    page_class: str | None = None
    artifacts: tuple[str, ...] = ()

    if subject.startswith("ALARM: "):
        try:
            alarm = json.loads(message)
        except ValueError:
            alarm = {}
        if (
            isinstance(alarm, dict)
            and str(alarm.get("AlarmName") or "").startswith(_COMPONENT_ALARM_PREFIX)
            and alarm.get("NewStateValue") == "ALARM"
        ):
            page_class = "execution_failed_alarm"
    elif subject in notify_subjects:
        page_class = "execution_failed_notify"
    elif subject == PREOPEN_NOT_READY_SUBJECT:
        page_class = "preopen_not_ready"
    elif subject.rstrip().endswith(_FRESHNESS_SUBJECT_SUFFIX):
        artifacts = _component_artifacts(message, key_patterns)
        if artifacts:
            page_class = "freshness_deadline_missed"

    if page_class is None:
        return None
    return PageEvent(
        ts_utc=str(entry.get("ts_utc") or ""),
        page_class=page_class,
        subject=subject,
        event_id=str(entry.get("event_id") or ""),
        artifacts=artifacts,
    )


# ---------------------------------------------------------------------------
# Reading one month of the ledger.
# ---------------------------------------------------------------------------


@dataclass
class MonthReading:
    month: str
    days_in_month: int
    days_read: int
    observed_days: list[str] = field(default_factory=list)
    unobserved_days: list[str] = field(default_factory=list)
    entries_read: int = 0
    pages: list[PageEvent] = field(default_factory=list)
    complete: bool = False


def _month_days(month_start: dt.date) -> int:
    return calendar.monthrange(month_start.year, month_start.month)[1]


def read_month(
    reader: Any,
    *,
    bucket: str,
    month_start: dt.date,
    through: dt.date,
    notify_subjects: frozenset[str],
    key_patterns: tuple[str, ...],
) -> MonthReading:
    """Read the mirror for ``month_start``'s month, day 1 through ``through``
    (inclusive, clamped to the month). ``reader`` is the
    ``list_objects(bucket, prefix)`` / ``get_object(bucket, key)`` seam.
    Any listing, read or parse failure raises."""
    days_in_month = _month_days(month_start)
    last = min(through, month_start.replace(day=days_in_month))
    reading = MonthReading(
        month=f"{month_start:%Y-%m}",
        days_in_month=days_in_month,
        days_read=(last - month_start).days + 1 if last >= month_start else 0,
        complete=last == month_start.replace(day=days_in_month),
    )
    marker = f"_{ALERTS_TOPIC}_"
    day = month_start
    while day <= last:
        stamp = f"{day:%Y-%m-%d}"
        keys: list[str] = []
        for prefix in LEDGER_PREFIXES:
            keys.extend(k for k in reader.list_objects(bucket, f"{prefix}{stamp}/") if marker in k.rsplit("/", 1)[-1])
        if keys:
            reading.observed_days.append(stamp)
        else:
            reading.unobserved_days.append(stamp)
        for key in sorted(keys):
            entry = json.loads(reader.get_object(bucket, key))
            if not isinstance(entry, dict):
                raise ValueError(f"s3://{bucket}/{key} is not a JSON object")
            reading.entries_read += 1
            topic = str((entry.get("sns") or {}).get("topic_arn") or "")
            if not topic.endswith(f":{ALERTS_TOPIC}"):
                continue
            event = classify_entry(entry, notify_subjects=notify_subjects, key_patterns=key_patterns)
            if event is not None:
                reading.pages.append(event)
        day += dt.timedelta(days=1)
    reading.pages.sort(key=lambda p: (p.ts_utc, p.event_id))
    return reading


def build_document(reading: MonthReading, *, now: dt.datetime, bucket: str = DEFAULT_BUCKET) -> dict[str, Any]:
    """The metric document the clause reads (``status``, ``value``,
    ``days_observed``, ``days_in_month``), plus the page list it was counted
    from."""
    count = len(reading.pages)
    status = "breach" if count > MAX_PAGES_PER_MONTH else "ok"
    by_class = {name: 0 for name in PAGE_CLASSES}
    for page in reading.pages:
        by_class[page.page_class] += 1
    unobserved = len(reading.unobserved_days)
    summary = (
        f"{count} page(s) in {reading.month} against a budget of {MAX_PAGES_PER_MONTH}; "
        f"{len(reading.observed_days)} of {reading.days_in_month} day(s) observed"
    )
    if unobserved:
        summary += f"; {unobserved} day(s) with no ledger entry at all (mirror not running), not counted as observed"
    if status == "breach":
        summary += " — over budget: a finding that re-opens the retry class, not a threshold to raise (plan risk 8)"
    return {
        "metric": "data.pages.monthly",
        "status": status,
        "value": count,
        "target": {"max_pages_per_month": MAX_PAGES_PER_MONTH, "source": "data_collection_plan_260914.md §2 row 11"},
        "month": reading.month,
        "month_complete": reading.complete,
        "days_observed": len(reading.observed_days),
        "days_in_month": reading.days_in_month,
        "days_read": reading.days_read,
        "unobserved_days": list(reading.unobserved_days),
        "by_class": by_class,
        "pages": [p.as_dict() for p in reading.pages],
        "vendor_outage_exclusions": {
            "declared": [],
            "excluded_pages": 0,
            "note": (
                "No vendor-outage declaration surface exists yet, so every page counts: stricter than "
                "the requirement, never laxer."
            ),
        },
        "source": {
            "ledger": [f"s3://{bucket}/{prefix}" for prefix in LEDGER_PREFIXES],
            "topic": ALERTS_TOPIC,
            "entries_read": reading.entries_read,
            "writer": "infrastructure/lambdas/changelog-incident-mirror",
        },
        "summary": summary,
        "as_of": _iso(now),
        "generated_utc": _iso(now),
        "stale_after_utc": _iso(now + STALE_AFTER),
    }


def closed_month_key(month: str) -> str:
    return _CLOSED_MONTH_KEY_TEMPLATE.format(month=month)


def _previous_month_start(day: dt.date) -> dt.date:
    first = day.replace(day=1)
    return (first - dt.timedelta(days=1)).replace(day=1)


class _S3Reader:
    """Thin wrapper over a boto3 S3 client: the seam the tests fake."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def list_objects(self, bucket: str, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def get_object(self, bucket: str, key: str) -> bytes:
        return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()


def compute_documents(
    reader: Any,
    *,
    bucket: str,
    key: str,
    now: dt.datetime,
    units: list[Unit] | None = None,
    asl_path: pathlib.Path = COLLECTION_ASL,
) -> dict[str, dict[str, Any]]:
    """Every document this run writes, keyed by S3 key: the current month's
    ``latest.json`` and, on the first :data:`CLOSED_MONTH_REWRITE_DAYS` days
    of a month, the closed previous month."""
    notify_subjects = collection_notify_subjects(asl_path)
    key_patterns = component_key_patterns(units if units is not None else load_units())
    today = now.astimezone(dt.timezone.utc).date()
    current = read_month(
        reader,
        bucket=bucket,
        month_start=today.replace(day=1),
        through=today,
        notify_subjects=notify_subjects,
        key_patterns=key_patterns,
    )
    documents = {key: build_document(current, now=now, bucket=bucket)}
    if today.day <= CLOSED_MONTH_REWRITE_DAYS:
        previous_start = _previous_month_start(today)
        previous = read_month(
            reader,
            bucket=bucket,
            month_start=previous_start,
            through=today,
            notify_subjects=notify_subjects,
            key_patterns=key_patterns,
        )
        documents[closed_month_key(previous.month)] = build_document(previous, now=now, bucket=bucket)
    return documents


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--no-write", action="store_true", help="print the documents; write nothing")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    s3 = boto3.client("s3", region_name=args.region)
    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        documents = compute_documents(_S3Reader(s3), bucket=args.bucket, key=args.key, now=started_at)
    except Exception as exc:  # RAISE after recording - fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="pages_monthly",
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    if args.no_write:
        print(json.dumps(documents, indent=2, sort_keys=True))
        return 0

    for key, document in documents.items():
        s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=json.dumps(document, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        # Counts only: this repository's Actions logs are public.
        print(
            f"WROTE s3://{args.bucket}/{key}: month={document['month']} status={document['status']} "
            f"pages={document['value']} days_observed={document['days_observed']}/{document['days_in_month']}"
        )
    latest = documents[args.key]
    write_run_record(
        s3,
        bucket=args.bucket,
        producer="pages_monthly",
        status="ok",
        started_at=started_at,
        finished_at=dt.datetime.now(dt.timezone.utc),
        detail={
            "metric_key": args.key,
            "documents": sorted(documents),
            "month": latest["month"],
            "pages": latest["value"],
            "status": latest["status"],
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
