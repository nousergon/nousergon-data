"""Shared execution-record writer for the `data_gate/producers/` scheduled
metric producers (`alpha-engine-config-I11058`).

Signal class 1 (execution) of `observability-policy.md` §3.1: did the
scheduled run start, did it finish, with what terminal outcome — on the
failure path as well as the success path. Without this, a period where the
GitHub Actions schedule silently never fired is indistinguishable from a
period it fired and had nothing to report: both would show no NEW metric
document (`v1_data_stage`'s count and `executor_profile`'s count can both
legitimately be flat/zero across periods), and the metric document alone
cannot tell those two apart.

**Not `data_run_manifest.v1`.** That shape is the StepFunctions-verified-unit
contract owned by `collectors/` and the data-collection stack's own run
verification (`alpha-engine-config-I10941`/`I10942`/`I10939`). These two
producers are not `verify_units` members — they run on their own GitHub
Actions schedule (`.github/workflows/phase-exit-metrics.yml`), independent of
`ne-weekly-freshness-pipeline` and the `nousergon-data-collection` stack. This
is a smaller, independent record, keyed by producer name rather than
(unit_id, trading_day, run_id).

**Lands under the already-grandfathered `data_collection/runs/` prefix**
(`alpha-engine-config` `private-docs/ARTIFACT_REGISTRY.yaml`
`grandfathered_paths`) rather than opening a new top-level prefix that would
need its own registry entry — the existing grandfather reason ("variable
cardinality... liveness is per-unit") covers a second, differently-shaped
producer writing into the same prefix just as well as the first.

Two producers adopt this in the same change (`v1_data_stage.py`,
`executor_profile.py`), so per `policy-shared-code`'s second-adoption rule it
is lifted here rather than duplicated inline in both.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

__all__ = ["RUN_RECORD_BUCKET", "run_record_key", "write_run_record"]

RUN_RECORD_BUCKET = "alpha-engine-research"


def run_record_key(producer: str, as_of: dt.date) -> str:
    """``data_collection/runs/<producer>/<as_of>.json`` — one record per
    producer per calendar day, overwritten on same-day re-runs (a manual
    `workflow_dispatch` re-run reports the LAST attempt of the day, which is
    the terminal outcome that matters)."""
    return f"data_collection/runs/{producer}/{as_of:%Y-%m-%d}.json"


def _iso(instant: dt.datetime) -> str:
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=dt.timezone.utc)
    return instant.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_run_record(
    s3: Any,
    *,
    bucket: str,
    producer: str,
    status: str,
    started_at: dt.datetime,
    finished_at: dt.datetime,
    error: str | None = None,
    detail: dict[str, Any] | None = None,
) -> str:
    """Write the run record and return the key it was written to.

    ``status`` is ``"ok"`` or ``"error"`` — never silently coerced. The
    caller writes this from BOTH the success path and the exception path
    (see each producer's `main()`), so a run that raised still leaves a
    record naming what happened, per the fleet's fail-path-writes-the-same-
    telemetry rule (`observability-policy.md` "five rules that survive").
    """
    if status not in ("ok", "error"):
        raise ValueError(f"status must be 'ok' or 'error', got {status!r}")
    finished_utc = finished_at if finished_at.tzinfo else finished_at.replace(tzinfo=dt.timezone.utc)
    record = {
        "producer": producer,
        "status": status,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "duration_seconds": (finished_utc.astimezone(dt.timezone.utc) - (started_at if started_at.tzinfo else started_at.replace(tzinfo=dt.timezone.utc)).astimezone(dt.timezone.utc)).total_seconds(),
        "error": error,
        "detail": detail or {},
    }
    key = run_record_key(producer, finished_utc.astimezone(dt.timezone.utc).date())
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(record, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return key
