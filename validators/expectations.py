"""Write-time expectations — the guards every published key passes.

`data_collection_plan_260914.md` §2 row 6 and §4.5; `alpha-engine-config-I10785`
(plan item P-18).

**The gap this closes.** Before this module, exactly three of forty-six units
guarded against publishing an empty-but-fresh artifact: D19 sets
``verify_artifact_exists=True``, D03 refuses a short fetch, D37 refuses an empty
intraday slice. Every other unit could report ``ok``, advance its freshness
sentinel and publish nothing at all, and every detector downstream would read
green — because they read the key's timestamp, not its contents.

**What this checks.** One guard, `empty_fresh`, at the shared publish
chokepoint:

* the unit's contracted key EXISTS on S3 — an absent key under an ``ok`` status
  is a success claim with no artifact behind it;
* it is not zero bytes;
* the collector's own reported row count is above its declared floor.

**What it does NOT do yet, and why that is stated rather than hidden.** The
guard runs at ``weekly_collector._phase_collect`` — after the collector's own
PUT, not before it. `_phase_collect` is the one place every scheduled collector
passes through, which is what makes a *common* guard possible at all; moving the
predicate strictly ahead of the write means every collector routing its own PUT
through this module, which is the phase-2 change this observe period is the
evidence for. Post-PUT is not a weaker check of the same property — it catches
the same empty artifact, one moment later and before any consumer has been told
the artifact is ready, because nothing downstream reads the key until the phase
marker lands.

**Observe first** (`sf-pipeline-policy` §7a). This guard is NEW on a scheduled
pipeline path and its verdict would halt a stage, so it ships in OBSERVE mode:
the predicate runs, the verdict is logged at ERROR, a MetricRecord rides on the
run manifest and renders on the data board — and the exit code does not move.
The promotion criterion is in this module, below, because a guard parked in
observe mode forever is the same defect one direction over.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from botocore.exceptions import ClientError
from krepis.metrics import MetricRecord
from nousergon_lib.guard_mode import GuardMode, GuardStaging

logger = logging.getLogger(__name__)

__all__ = [
    "EMPTY_FRESH_GUARD",
    "GuardReading",
    "check_empty_fresh",
    "verdict_metric",
]

#: `sf-pipeline-policy` §7a: the staging, its promotion criterion and its
#: tracker, declared in the guard's OWN module.
#:
#: **Promotion criterion: 10 consecutive clean scheduled cycles** — a cycle
#: being clean when every unit's `empty_fresh` verdict on that cycle is `ok`
#: (an `unmeasurable` verdict is NOT clean; it means a unit still reports no row
#: count, and promoting over it would enforce a predicate on units it cannot
#: read). The count is the rolling board Signal from
#: `data.<unit>.guard.empty_fresh`. Promotion is a deliberate PR flipping `mode`
#: to `GuardMode.ENFORCE`, with the ten cycles named in its body.
#:
#: `Re-exam:` is tracked on alpha-engine-config-I10785.
EMPTY_FRESH_GUARD = GuardStaging(
    name="data_empty_fresh",
    mode=GuardMode.OBSERVE,
    promotion_criterion=(
        "enforce after 10 consecutive clean scheduled cycles — every unit's "
        "empty_fresh verdict `ok` on each, `unmeasurable` not counting as clean "
        "(data_collection_plan_260914.md §4.5); Re-exam tracked on "
        "alpha-engine-config-I10785"
    ),
    tracked_issue="alpha-engine-config-I10785",
)

#: The guard's closed verdict vocabulary, matching `data_run_manifest.v1`'s
#: `GuardVerdict.verdict` enum. `unmeasurable` is red and counted — it means the
#: guard could not look, which is never a pass (`observability-policy` §8.3).
VERDICTS = ("ok", "empty_fresh", "below_floor", "unmeasurable", "not_applicable")


@dataclass(frozen=True)
class GuardReading:
    """One guard verdict, with what it measured and what it measured against."""

    verdict: str
    detail: str
    key: str | None = None
    value: float | None = None
    baseline: float | None = None

    @property
    def clean(self) -> bool:
        """`ok` and `not_applicable` are clean; everything else is not.

        `unmeasurable` is deliberately NOT clean: a cycle in which the guard
        could not look is not a cycle in which the guard passed, and counting it
        as one is how a promotion criterion gets met by a guard that never ran.
        """
        return self.verdict in ("ok", "not_applicable")


def _head(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """HEAD the key. ``None`` means a real 404; anything else RAISES.

    Fail-loud per the repo's producer rule: a check that silently reads
    "couldn't look" as "doesn't exist" produces nondeterministic failures, and
    reading it as "exists" defeats the guard entirely. The raise is caught one
    level up and recorded as `unmeasurable`, which is red — never as a pass.
    """
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in ("404", "NoSuchKey"):
            return None
        raise


def check_empty_fresh(
    *,
    unit_id: str,
    artifact_key: str | None,
    bucket: str | None,
    s3_client: Any,
    rows_out: int | None,
    floor: int | None = None,
    staging: GuardStaging = EMPTY_FRESH_GUARD,
) -> GuardReading:
    """Grade one unit's published key: present, non-empty, above its floor.

    Args:
        unit_id: The audit unit being graded, for the log line and the metric.
        artifact_key: The unit's contracted output key, or ``None`` when the
            unit publishes no single stable key (a per-symbol writer, an
            ArcticDB append). That is `not_applicable`, not a pass — the unit's
            descriptor is what says so, and its cardinality guard is the one
            that covers it (phase 2).
        bucket: The bucket ``artifact_key`` lives in.
        rows_out: The collector's own reported row count, or ``None`` when the
            collector reports none. ``None`` is `unmeasurable`, NEVER 0: reading
            an unreported count as zero would mark every such unit an
            empty-but-fresh write, and reading it as fine would mark none of
            them.
        floor: Minimum acceptable ``rows_out``, from the unit's descriptor.
            ``None`` means the unit declares no floor and only the non-empty
            half of the guard applies.

    Returns a :class:`GuardReading`. **It never raises on a verdict** — the
    consequence, if any, is the caller's, which is what keeps observe mode a
    one-line difference from enforcing mode.
    """
    if not artifact_key or not bucket:
        return GuardReading(
            "not_applicable",
            f"{unit_id} declares no single stable published key at this call site; "
            "its cardinality guard covers the per-symbol/ArcticDB writes (phase 2)",
        )

    try:
        head = _head(s3_client, bucket, artifact_key)
    except Exception as exc:  # noqa: BLE001 -- recorded as UNMEASURABLE, which is red
        return GuardReading(
            "unmeasurable",
            f"could not HEAD s3://{bucket}/{artifact_key}: {type(exc).__name__}: {exc}",
            key=artifact_key,
        )

    if head is None:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} reported success but s3://{bucket}/{artifact_key} does not exist — "
            "a success claim with no artifact behind it",
            key=artifact_key,
            value=0.0,
        )

    size = int(head.get("ContentLength") or 0)
    if size == 0:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} published a ZERO-BYTE object at s3://{bucket}/{artifact_key} — "
            "fresh by timestamp, empty by content, which is the exact write every "
            "freshness detector reads as green",
            key=artifact_key,
            value=0.0,
        )

    if rows_out is None:
        return GuardReading(
            "unmeasurable",
            f"{unit_id} publishes s3://{bucket}/{artifact_key} ({size} bytes) but reports "
            "no row count, so the empty-and-floor half of this guard cannot be evaluated. "
            "UNMEASURABLE, not a pass: the fix is the collector reporting its count "
            "(run_units.PHASE_UNITS rows_key)",
            key=artifact_key,
            baseline=None if floor is None else float(floor),
        )

    if rows_out == 0:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} published s3://{bucket}/{artifact_key} with 0 rows",
            key=artifact_key,
            value=0.0,
            baseline=None if floor is None else float(floor),
        )

    if floor is not None and rows_out < floor:
        return GuardReading(
            "below_floor",
            f"{unit_id} published {rows_out} rows to s3://{bucket}/{artifact_key}, below its "
            f"declared floor of {floor}",
            key=artifact_key,
            value=float(rows_out),
            baseline=float(floor),
        )

    return GuardReading(
        "ok",
        f"{unit_id} published {rows_out} rows ({size} bytes) to {artifact_key}",
        key=artifact_key,
        value=float(rows_out),
        baseline=None if floor is None else float(floor),
    )


def verdict_metric(unit_id: str, reading: GuardReading, *, source_path: str) -> MetricRecord:
    """The board row for one verdict — emitted for PASSES as well as failures.

    A guard that records only when it fires is indistinguishable from a guard
    that stopped running (`principles.md` §2.7), so every verdict becomes a
    MetricRecord and rides on the run manifest.
    """
    status = {
        "ok": "GREEN",
        "empty_fresh": "RED",
        "below_floor": "RED",
        "unmeasurable": "N/A-MISSING-INPUT",
        "not_applicable": "N/A-NOT-IMPL",
    }[reading.verdict]
    return MetricRecord(
        name=f"data.{unit_id}.guard.empty_fresh",
        module="nousergon-data",
        metric_type="count",
        # `rows` is the unit of both `value` and `target`: the guard's whole
        # question is how many records landed, against the declared floor.
        unit="rows",
        value=reading.value,
        n_floor=0,
        target=reading.baseline,
        status=status,
        status_reason=reading.detail[:500],
        source_path=source_path,
        last_updated_utc=_now_z(),
    )


def report(
    reading: GuardReading,
    *,
    unit_id: str,
    staging: GuardStaging = EMPTY_FRESH_GUARD,
    log: logging.Logger | None = None,
) -> None:
    """Log the verdict. LOUD while observing (`sf-pipeline-policy` §7a rule 3).

    An observe-mode verdict nobody reads is a suppression, not an observation.
    So a non-clean verdict is a real ERROR on the real log surface in both
    modes, and the only thing that differs at promotion is whether the caller
    then raises.
    """
    logger_ = log or logger
    if reading.clean:
        logger_.info("guard=%s unit=%s verdict=%s %s", staging.name, unit_id, reading.verdict, reading.detail)
        return
    logger_.error(
        "guard=%s unit=%s verdict=%s mode=%s %s [%s]",
        staging.name,
        unit_id,
        reading.verdict,
        staging.mode.value,
        reading.detail,
        staging.describe(),
    )


def _now_z() -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
