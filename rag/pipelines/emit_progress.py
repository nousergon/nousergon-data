"""RAGIngestion inner-step progress telemetry (config-I2966, Brian directive
2026-07-18).

The weekly RAG ingestion pipeline (``rag/pipelines/run_weekly_ingestion.sh``)
runs on the always-on EC2 instance via SSM as a single Step Functions Task —
off-box, the dashboard's Fleet Status page sees only "RAGIngestion RUNNING"
for up to several hours with no indication of which of its 10 inner steps is
executing. This module writes a small JSON progress marker to S3 between
each step so the console can render "step 5/10: news" plus a staleness
telltale (config-I2966 deliverable #2).

Artifact: ``s3://alpha-engine-research/health/rag_ingestion_progress/{run_date}.json``
Shape: ``{"step": int, "of": int, "label": str, "started_at": iso8601,
"updated_at": iso8601}`` — registered in alpha-engine-config's
``ARTIFACT_REGISTRY.yaml`` as ``artifact_id: rag_ingestion_progress``.

Deliberate no-silent-fails DEVIATION: the PUT is fail-soft (WARN + continue)
per explicit issue instruction — this artifact is progress TELEMETRY, not
pipeline output. A failed write must never abort or degrade the actual
ingestion run (SEC filings / news / Form 4 / etc. all still need
``set -euo pipefail`` to hard-fail on THEIR errors); losing one progress
tick just means the console shows a slightly stale "step N/10" until the
next tick lands. This is the one deliberate swallow in the weekly ingestion
path — every other error in ``run_weekly_ingestion.sh`` remains a hard
abort by design (see that script's own docstring).

Usage (called between steps by run_weekly_ingestion.sh)::

    python -m rag.pipelines.emit_progress \\
        --run-date 2026-07-25 --step 5 --of 10 --label news \\
        --started-at 2026-07-25T09:00:12Z
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_BUCKET = "alpha-engine-research"
_KEY_TEMPLATE = "health/rag_ingestion_progress/{run_date}.json"


def emit_progress(
    *,
    run_date: str,
    step: int,
    of: int,
    label: str,
    started_at: str,
    bucket: str = _BUCKET,
) -> bool:
    """Write one progress tick. Returns True on success, False on any
    failure — the caller (main(), and transitively the shell script) treats
    False as WARN-and-continue, never a hard abort. See module docstring for
    the deliberate no-silent-fails deviation this represents."""
    payload = {
        "step": step,
        "of": of,
        "label": label,
        "started_at": started_at,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    key = _KEY_TEMPLATE.format(run_date=run_date)
    try:
        import boto3

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload).encode(),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — deliberate fail-soft swallow;
        # see module docstring's no-silent-fails deviation note. Logged at
        # WARN (not silently dropped) so a chronically-broken progress feed
        # is still discoverable in the SSM run log, it just never blocks
        # the actual ingestion pipeline.
        logger.warning(
            "rag_ingestion_progress PUT failed for s3://%s/%s (step %d/%d: "
            "%s) — telemetry only, ingestion pipeline continues: %s",
            bucket, key, step, of, label, exc,
        )
        return False
    logger.info("rag_ingestion_progress: step %d/%d (%s) -> s3://%s/%s", step, of, label, bucket, key)
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Emit RAGIngestion inner-step progress telemetry")
    parser.add_argument("--run-date", required=True, help="UTC run date, YYYY-MM-DD (matches the ingestion run's own date_str)")
    parser.add_argument("--step", type=int, required=True)
    parser.add_argument("--of", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--started-at", required=True, help="ISO8601 UTC timestamp the pipeline run itself started")
    parser.add_argument("--bucket", default=_BUCKET)
    args = parser.parse_args()

    # Never raise out of main() — a bad CLI invocation from the shell script
    # (malformed arg, etc.) still must not abort the ingestion pipeline that
    # calls this as `|| true`-guarded telemetry. argparse itself can still
    # exit non-zero on missing required args; the shell call site guards
    # with `|| echo WARN ...` for exactly that case (belt-and-suspenders).
    ok = emit_progress(
        run_date=args.run_date,
        step=args.step,
        of=args.of,
        label=args.label,
        started_at=args.started_at,
        bucket=args.bucket,
    )
    if not ok:
        # Exit 0 regardless — see module docstring. The WARN is already
        # logged inside emit_progress; a non-zero exit here would only
        # matter if the shell caller didn't already guard it, and per the
        # issue's fail-soft requirement it must not matter either way.
        pass


if __name__ == "__main__":
    main()
