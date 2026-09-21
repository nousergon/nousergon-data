"""Producer for `data.phase1.cost_baseline_measured` / `data.cost.monthly`
(`alpha-engine-config-I10788`).

Writes `metrics/cost/monthly/latest.json` under
`s3://alpha-engine-research/data_collection`, the key
`data_gate.exit_criteria.read_cost_baseline_measured` and
`_clause_objective("data.cost.monthly", ...)` already read.

**Sourced from the AWS Cost and Usage Report (CUR) export ONLY — never
Cost Explorer.** `ce:GetCostAndUsage` bills $0.01 per request; a retry loop
against it billed $441.67 in four days (measured 2026-09-09, `data_collection
_plan_260914.md` §2 objective 8). A CUR/Data Export delivers already-computed
Parquet objects to S3 on its own cadence; reading them back is a free S3 GET,
however many times this producer runs, with NO per-request AWS cost API call
anywhere in this module.

**Tag key/value is `component=data-collection`, following the binding plan's
literal text** (`data_collection_plan_260914.md` §2 objective 8;
`data_gate.clauses`'s own `data.cost.monthly` description string). This is a
STATED ASSUMPTION, not a resolution of `alpha-engine-config-I10905` (gate:
decision, OPEN as of 2026-09-21): that issue asks whether `component` or
`system` should be the fleet's ONE cost-attribution key, and its label is
left untouched here. Measured before choosing anyway (`aws ce list-cost-
allocation-tags`, 2026-09-21): `system` was the only ACTIVE cost-allocation
tag key; `component` was INACTIVE. `component` was activated the same day —
see the PR this module shipped in — so tagged spend under this key starts
accruing in CUR from 2026-09-21 forward, never retroactively. If I10905
rules `system` instead, `--tag-key` below is a one-flag change, not a
rewrite.

**CUR location has no fleet default.** Unlike `executor_profile.py`'s
CloudTrail archive (a stack output that already exists), no CUR/Data Export
exists anywhere in this account as of 2026-09-21 (`aws cur describe-report-
definitions` and `aws bcm-data-exports list-exports` both returned empty).
Provisioning one needs an S3 bucket plus a `bcm-data-exports:CreateExport`
call — both blocked from an agent session by the local harness's shared-
resource-mutation guardrail, so `--cur-bucket`/`--cur-export-name` are
REQUIRED with no default and `main()` fails loud, naming the missing export
and the tracked issue, until an operator provisions it. This is the honest
"stop and report" the calling issue's dispatch instructions asked for rather
than a silent fallback to Cost Explorer.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "DEFAULT_TAG_KEY",
    "DEFAULT_TAG_VALUE",
    "CostWindow",
    "build_metric",
    "iter_billing_periods",
    "main",
    "read_cur_window",
]

logger = logging.getLogger(__name__)

_REGION = "us-east-1"

DEFAULT_BUCKET = "alpha-engine-research"
DEFAULT_KEY = "data_collection/metrics/cost/monthly/latest.json"

#: `alpha-engine-config-I10788` assumption — see module docstring.
DEFAULT_TAG_KEY = "component"
DEFAULT_TAG_VALUE = "data-collection"

#: CUR 2.0 / Data Exports column names (lowercase; user-defined tags are
#: prefixed `resource_tags_user_<key>` with `-` folded to `_`).
_COST_COLUMN = "line_item_unblended_cost"
_USAGE_START_COLUMN = "line_item_usage_start_date"


@dataclass(frozen=True)
class CostWindow:
    total_cost: float
    days_requested: int
    days_covered: int
    uncovered_days: tuple[str, ...] = field(default_factory=tuple)
    billing_periods_read: tuple[str, ...] = field(default_factory=tuple)
    objects_read: int = 0
    rows_scanned: int = 0
    rows_matched: int = 0


def iter_billing_periods(start: dt.date, end: dt.date) -> list[str]:
    """Calendar-month `YYYY-MM` strings CUR partitions by, covering
    `[start, end]` inclusive — a window spanning a month boundary reads BOTH
    months' exports, never just the first."""
    periods: list[str] = []
    cursor = start.replace(day=1)
    end_marker = end.replace(day=1)
    while cursor <= end_marker:
        periods.append(f"{cursor:%Y-%m}")
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return periods


def _tag_column(tag_key: str) -> str:
    return "resource_tags_user_" + tag_key.replace("-", "_").lower()


def _read_parquet_bytes(payload: bytes):
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(payload))


def read_cur_window(
    s3: Any,
    *,
    cur_bucket: str,
    cur_prefix: str,
    tag_key: str,
    tag_value: str,
    start: dt.date,
    end: dt.date,
) -> CostWindow:
    """Sum `line_item_unblended_cost` for rows tagged `tag_key=tag_value`
    whose `line_item_usage_start_date` falls in `[start, end)`, reading CUR
    2.0 Parquet objects for every billing period the window touches.

    A billing period with NO listed objects is a GAP in `uncovered_days` —
    every calendar day in that period that also falls in `[start, end)` is
    named uncovered, never silently treated as zero spend (same discipline
    as `executor_profile.count_collection_writes`'s `uncovered_days`: an
    export that has not landed yet is not evidence of zero cost).
    """
    import pyarrow.compute as pc

    tag_column = _tag_column(tag_key)
    periods = iter_billing_periods(start, end)
    total_cost = 0.0
    objects_read = 0
    rows_scanned = 0
    rows_matched = 0
    covered_days: set[str] = set()
    uncovered_days: list[str] = []
    billing_periods_read: list[str] = []

    for period in periods:
        prefix = f"{cur_prefix}/data/BILLING_PERIOD={period}/"
        keys = [k for k in s3.list_objects(cur_bucket, prefix) if k.endswith(".parquet")]
        if not keys:
            # Every day of THIS period that also falls in the requested
            # window is uncovered — a period the export never delivered
            # covers zero days, not "the whole period counts as zero cost".
            year, month = (int(p) for p in period.split("-"))
            day = dt.date(year, month, 1)
            while day.month == month:
                if start <= day <= end:
                    uncovered_days.append(day.isoformat())
                day += dt.timedelta(days=1)
            continue

        billing_periods_read.append(period)
        for key in keys:
            payload = s3.get_object(cur_bucket, key)
            objects_read += 1
            table = _read_parquet_bytes(payload)
            if tag_column not in table.column_names or _USAGE_START_COLUMN not in table.column_names:
                # A CUR schema this reader does not recognise is UNMEASURABLE,
                # never silently treated as zero-cost — raise so the run
                # record captures it rather than publishing a false $0.
                raise ValueError(
                    f"{key} carries columns {table.column_names[:12]}... missing required "
                    f"{tag_column!r} and/or {_USAGE_START_COLUMN!r} — CUR schema mismatch, "
                    "not a zero-cost period"
                )
            rows_scanned += table.num_rows
            usage_days = pc.utf8_slice_codeunits(
                pc.cast(table[_USAGE_START_COLUMN], "string"), 0, 10
            )
            in_window_mask = pc.and_(
                pc.greater_equal(usage_days, start.isoformat()),
                pc.less_equal(usage_days, end.isoformat()),
            )
            tag_mask = pc.equal(pc.cast(table[tag_column], "string"), tag_value)
            mask = pc.and_(in_window_mask, tag_mask)
            filtered = table.filter(mask)
            rows_matched += filtered.num_rows
            if filtered.num_rows:
                total_cost += pc.sum(pc.cast(filtered[_COST_COLUMN], "double")).as_py() or 0.0
                for d in pc.utf8_slice_codeunits(
                    pc.cast(filtered[_USAGE_START_COLUMN], "string"), 0, 10
                ).to_pylist():
                    covered_days.add(d)

        # A billing period whose export landed but named ZERO of our rows in
        # the window still COVERS those days — the report was delivered, our
        # tag simply had no spend that day (e.g. a weekend with no launch).
        # `covered_days` above only records days that matched; the period
        # itself being non-empty is what proves the day was measured.
        year, month = (int(p) for p in period.split("-"))
        day = dt.date(year, month, 1)
        while day.month == month:
            if start <= day <= end:
                covered_days.add(day.isoformat())
            day += dt.timedelta(days=1)

    days_requested = (end - start).days + 1
    return CostWindow(
        total_cost=round(total_cost, 4),
        days_requested=days_requested,
        days_covered=len(covered_days),
        uncovered_days=tuple(sorted(uncovered_days)),
        billing_periods_read=tuple(billing_periods_read),
        objects_read=objects_read,
        rows_scanned=rows_scanned,
        rows_matched=rows_matched,
    )


def build_metric(
    *,
    window: CostWindow,
    tag_key: str,
    tag_value: str,
    cur_bucket: str,
    cur_prefix: str,
    as_of: dt.datetime | None = None,
) -> dict:
    as_of = as_of or dt.datetime.now(dt.timezone.utc)
    return {
        # `baseline` / `days_covered` are the two fields
        # `read_cost_baseline_measured` reads; everything else is provenance.
        "baseline": window.total_cost,
        "days_covered": window.days_covered,
        "as_of": as_of.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "days_requested": window.days_requested,
        "uncovered_days": list(window.uncovered_days),
        "tag_key": tag_key,
        "tag_value": tag_value,
        "cur_source": f"s3://{cur_bucket}/{cur_prefix}",
        "billing_periods_read": list(window.billing_periods_read),
        "objects_read": window.objects_read,
        "rows_scanned": window.rows_scanned,
        "rows_matched": window.rows_matched,
    }


class _S3Reader:
    """Thin wrapper over a boto3 S3 client — the seam `read_cur_window`
    tests against with a fake, and the only place this module calls out."""

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


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help="store bucket to write the metric to")
    ap.add_argument("--key", default=DEFAULT_KEY)
    ap.add_argument(
        "--cur-bucket",
        default=None,
        required=False,
        help="S3 bucket the CUR/Data Export delivers to — REQUIRED, no fleet default exists yet "
        "(alpha-engine-config-I10788 follow-up: no CUR export is provisioned in this account as "
        "of 2026-09-21)",
    )
    ap.add_argument(
        "--cur-export-name",
        default=None,
        required=False,
        help="the export's name / S3 prefix (CUR delivers under <prefix>/<export-name>/data/"
        "BILLING_PERIOD=YYYY-MM/)",
    )
    ap.add_argument("--tag-key", default=DEFAULT_TAG_KEY)
    ap.add_argument("--tag-value", default=DEFAULT_TAG_VALUE)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument("--days", type=int, default=28, help="4-week baseline window (phase-1 exit)")
    ap.add_argument("--no-write", action="store_true")
    args = ap.parse_args(argv)

    import boto3  # noqa: PLC0415 - deferred so import stays light for tests

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    s3 = boto3.client("s3", region_name=args.region)
    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        if not args.cur_bucket or not args.cur_export_name:
            raise RuntimeError(
                "no CUR/Data Export is configured — --cur-bucket and --cur-export-name are "
                "required and neither has a fleet default. No Cost and Usage Report export "
                "exists in this account as of 2026-09-21 (`aws cur describe-report-definitions` "
                "and `aws bcm-data-exports list-exports` both returned empty); provisioning one "
                "needs an S3 bucket plus `bcm-data-exports:CreateExport`, both operator-gated. "
                "This producer refuses to fall back to Cost Explorer ($0.01/request; a retry "
                "loop against it billed $441.67 in four days, measured 2026-09-09) — see "
                "alpha-engine-config-I10788's follow-up issue for the exact provisioning command."
            )

        end = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)  # yesterday: CUR lags
        start = end - dt.timedelta(days=args.days - 1)

        reader = _S3Reader(s3)
        window = read_cur_window(
            reader,
            cur_bucket=args.cur_bucket,
            cur_prefix=args.cur_export_name,
            tag_key=args.tag_key,
            tag_value=args.tag_value,
            start=start,
            end=end,
        )
        metric = build_metric(
            window=window,
            tag_key=args.tag_key,
            tag_value=args.tag_value,
            cur_bucket=args.cur_bucket,
            cur_prefix=args.cur_export_name,
        )
        # NEVER the full document (alpha-engine-config-I11274, CodeQL "Clear-
        # text logging of sensitive information" on this exact line): this
        # repo is PUBLIC and its GitHub Actions logs are public, and `metric`
        # carries the dollar baseline plus the CUR export's bucket/prefix
        # (`cur_source`) and the cost-attribution tag value — all of it
        # written to S3 already, none of it fit for a public log. Print a
        # fixed, non-sensitive summary only.
        print(f"{args.key}: days_covered={window.days_covered}, status=ok")
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer="cost_monthly",
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    if not args.no_write:
        s3.put_object(
            Bucket=args.bucket,
            Key=args.key,
            Body=json.dumps(metric, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"WROTE s3://{args.bucket}/{args.key}")

        write_run_record(
            s3,
            bucket=args.bucket,
            producer="cost_monthly",
            status="ok",
            started_at=started_at,
            finished_at=dt.datetime.now(dt.timezone.utc),
            detail={
                "metric_key": args.key,
                "baseline": window.total_cost,
                "days_covered": window.days_covered,
                "days_requested": window.days_requested,
            },
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
