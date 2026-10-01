"""EOD artifact verification — the fleet definition of "did the EOD do its job".

Originally sf-telegram-notifier-local (alpha-engine-config#5289); lifted to the
shared lambdas dir on second adoption (alpha-engine-config-I7582, shared-code
policy). Two consumers, one definition:

  * ``sf-telegram-notifier`` — so a terminal Telegram message cannot read clean
    when the day's load-bearing artifacts were never written.
  * ``eod-backstop`` — so the same-day trigger of last resort asks whether the
    EOD PRODUCED ITS ROW, not merely whether an execution started.

Those two must never drift: a backstop that stands down on a day the notifier
would call incomplete is the 2026-08-17 gap.

A post-close reconcile run's terminal SUCCEEDED/DEGRADED Telegram message must not
read "clean" when the day's load-bearing artifacts were never written — the
core failure mode alpha-engine-config#5289 exists to catch: "A 'SUCCESS' that
did not write its artifacts is the failure mode this issue exists to catch."
(2026-07-27/28's postclose FAILUREs reached only email; a silently-incomplete
SUCCEEDED would have been worse — no signal at all, on any channel.)

Two independent existence checks, both read-only S3 calls against the same
bucket ``execution_digest.py`` already reads (``alpha-engine-research``):

  1. The SF-envelope completion marker
     (``_sf_completion/ne-postclose-reconcile-pipeline/{run_date}.json``,
     config#2857, written by ``step_function_eod_reconcile.json``'s
     WriteCompletionMarkerNormal/WriteCompletionMarkerDegraded states —
     ``step_function_eod.json`` until the 2026-09-30 split) — proves the
     Step Functions execution itself reached its real terminal, independent
     of downstream deliverables.
  2. A row in ``trades/eod_pnl.csv`` for ``run_date`` (ARTIFACT_REGISTRY
     ``eod_reconcile_pnl``, written by crucible-executor's
     ``eod_reconcile.py`` CSV export) — proves the EOD reconcile actually
     wrote the day's P&L ledger row, the artifact behind NAV / daily-return /
     alpha (nousergon-data#480's ``backfill_eod_pnl --date`` target when it is
     missing).

Both checks fail TOWARD the loud path: any S3 error other than a clean
404/NoSuchKey (throttle, permission, transient) is reported as "absent"
rather than raised, because a check that can't confirm presence must not
render as verified — the entire point of this module is to distrust a
"looks fine" outcome until proven.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

S3_BUCKET = "alpha-engine-research"
#: The machine whose completion marker sits beside the eod_pnl row. Since the
#: 2026-09-30 split (alpha-engine-config-I11269 follow-up) the row is written by
#: ne-postclose-reconcile-pipeline's EODReconcile, so "did the EOD do its job"
#: is that machine's marker + the row. ne-postclose-trading-pipeline still
#: writes its own marker; its artifact is the CaptureSnapshot output, checked by
#: alpha-engine-eod-backstop and alpha-engine-eod-snapshot-existence-check.
EOD_PIPELINE_NAME = "ne-postclose-reconcile-pipeline"
COMPLETION_MARKER_KEY_TEMPLATE = (
    "_sf_completion/" + EOD_PIPELINE_NAME + "/{run_date}.json"
)
EOD_PNL_CSV_KEY = "trades/eod_pnl.csv"

# eod_pnl.csv is a full-history re-export (crucible-executor eod_reconcile.py
# writes the whole `eod_pnl` table every run, not an append) — today's history
# is well under 1MB. A cap guards against parsing an unbounded object inside a
# Lambda invocation; past it, the check reports absent (fail toward loud).
_MAX_CSV_BYTES = 25 * 1024 * 1024  # 25MB


def _is_not_found(exc: Exception) -> bool:
    code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
    status = getattr(exc, "response", {}).get("ResponseMetadata", {}).get(
        "HTTPStatusCode", 0
    )
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


@dataclass(frozen=True)
class EodArtifactStatus:
    run_date: str
    completion_marker_present: bool
    pnl_row_present: bool

    @property
    def all_present(self) -> bool:
        return self.completion_marker_present and self.pnl_row_present


def _check_completion_marker(s3_client: Any, run_date: str) -> bool:
    key = COMPLETION_MARKER_KEY_TEMPLATE.format(run_date=run_date)
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception as exc:  # noqa: BLE001 — classify, then fail toward loud
        if _is_not_found(exc):
            return False
        logger.warning(
            "completion marker HEAD failed for s3://%s/%s (non-404: %s) — "
            "reporting absent rather than raising, so a transient S3 fault "
            "still renders loud, not clean",
            S3_BUCKET, key, exc,
        )
        return False


def _check_pnl_row(s3_client: Any, run_date: str) -> bool:
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=EOD_PNL_CSV_KEY)
    except Exception as exc:  # noqa: BLE001 — classify, then fail toward loud
        if _is_not_found(exc):
            return False
        logger.warning(
            "eod_pnl.csv GetObject failed (non-404: %s) — reporting absent "
            "rather than raising, so a transient S3 fault still renders "
            "loud, not clean",
            exc,
        )
        return False

    body = resp["Body"].read()
    if len(body) > _MAX_CSV_BYTES:
        logger.warning(
            "eod_pnl.csv is %d bytes (> cap %d) — skipping row scan, "
            "reporting absent to fail toward the loud path",
            len(body), _MAX_CSV_BYTES,
        )
        return False

    text = body.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None or "date" not in reader.fieldnames:
        logger.warning(
            "eod_pnl.csv has no 'date' column (fieldnames=%s) — reporting "
            "absent to fail toward the loud path",
            reader.fieldnames,
        )
        return False
    return any((row.get("date") or "").strip() == run_date for row in reader)


def verify_eod_artifacts(
    s3_client: Any, run_date: str | None
) -> EodArtifactStatus | None:
    """Run both existence checks for ``run_date``.

    Returns ``None`` when ``run_date`` could not be resolved at all — there
    is nothing to verify against, and that absence is itself an anomaly the
    caller renders distinctly from a missing artifact.
    """
    if not run_date:
        return None
    marker = _check_completion_marker(s3_client, run_date)
    pnl_row = _check_pnl_row(s3_client, run_date)
    return EodArtifactStatus(
        run_date=run_date,
        completion_marker_present=marker,
        pnl_row_present=pnl_row,
    )


def format_eod_artifact_lines(status: EodArtifactStatus | None) -> list[str]:
    """Render ``status`` as one terse line when healthy, an expanded loud
    block when not.

    Deliberate volume-posture exception (nousergon-data-i5289 PR body): a
    clean SUCCEEDED/DEGRADED EOD run adds exactly one line. A run whose
    artifacts are actually missing may NOT stay one line — a SUCCESS that
    did not write its ledger row reading as terse and clean is precisely the
    failure mode alpha-engine-config#5289 exists to close.
    """
    if status is None:
        return [
            "⚠️ *ARTIFACTS UNVERIFIED* — run_date not resolved from "
            "execution input or name"
        ]
    if status.all_present:
        return [f"Artifacts: ✓ completion marker + eod_pnl row ({status.run_date})"]
    missing = []
    if not status.completion_marker_present:
        missing.append(f"_sf_completion marker for {status.run_date}")
    if not status.pnl_row_present:
        missing.append(f"eod_pnl.csv row for {status.run_date}")
    lines = ["⚠️ *ARTIFACT(S) MISSING* — this run did not write:"]
    lines.extend(f"  • {m}" for m in missing)
    return lines
