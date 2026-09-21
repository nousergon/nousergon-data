"""Unit tests for data_gate/producers/cost_monthly.py (alpha-engine-
config-I10788).

Covers: the CUR-partition walk's day-coverage contract (a billing period the
export never delivered is a GAP, never a silent zero), the tag+date filter
(`component=data-collection` per the plan's literal text, an assumption
against the OPEN `alpha-engine-config-I10905`), the schema-mismatch fail-
loud path, and that `main()` refuses to run without an explicit CUR
location rather than falling back to Cost Explorer.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_gate.producers import cost_monthly as m

UTC = dt.timezone.utc


def _parquet_bytes(rows: list[dict]) -> bytes:
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


class _FakeReader:
    """Implements the `list_objects`/`get_object` seam `read_cur_window` uses
    — deliberately NOT a boto3 client double, since that wrapping is
    `_S3Reader`'s own job and is covered by the `main()` tests below."""

    def __init__(self, objects: dict[str, bytes]):
        self._objects = objects

    def list_objects(self, bucket, prefix):
        return [k for k in self._objects if k.startswith(prefix)]

    def get_object(self, bucket, key):
        return self._objects[key]


def _row(cost, usage_date, component="data-collection", other_component=None):
    return {
        "line_item_unblended_cost": cost,
        "line_item_usage_start_date": f"{usage_date}T00:00:00Z",
        "resource_tags_user_component": other_component if other_component is not None else component,
    }


# The `bcm-data-exports create-export` QueryStatement, codified in
# nous-ergon-ops/infrastructure/billing/cur-export/export.json
# (alpha-engine-config-I11272). Mirrored here as a LITERAL rather than read
# cross-repo (this repo's CI has no checkout of nous-ergon-ops) so a change
# to either side's column names fails THIS test rather than silently
# drifting apart. If you change `export.json`'s QueryStatement, update this
# constant in the SAME PR that changes it, and vice versa.
#
# CUR 2.0 / Data Exports has NO `resource_tags_user_component` column —
# `resource_tags` is a MAP, and a key is selected with the dot operator and
# ALIASED: `resource_tags.user_component AS resource_tags_user_component`
# (docs.aws.amazon.com/cur/latest/userguide/dataexports-data-query.html,
# verified 2026-09-21). The alias is what makes this producer need no code
# change once the export exists — confirmed by this test, not assumed.
_CODIFIED_CUR_EXPORT_QUERY_STATEMENT = (
    "SELECT bill_billing_period_start_date, line_item_usage_start_date, "
    "line_item_unblended_cost, resource_tags.user_component AS "
    "resource_tags_user_component, resource_tags.user_system AS "
    "resource_tags_user_system FROM COST_AND_USAGE_REPORT"
)


def test_producers_expected_columns_are_the_names_the_codified_export_aliases_to():
    """Pins `cost_monthly.py`'s column expectations against the exact aliased
    names `nous-ergon-fleet-cur`'s QueryStatement produces. A change to
    either side's naming that is not mirrored to the other fails here,
    before it fails silently at 4am against a real CUR object with a
    KeyError this module raises as a 'schema mismatch'."""
    query = _CODIFIED_CUR_EXPORT_QUERY_STATEMENT
    assert f"AS {m._USAGE_START_COLUMN}" not in query  # not aliased — a base CUR column
    assert m._USAGE_START_COLUMN in query
    assert m._COST_COLUMN in query
    tag_column = m._tag_column("component")
    assert tag_column == "resource_tags_user_component"
    assert f"resource_tags.user_component AS {tag_column}" in query
    # The `system` alias also selected (unused today) so an alpha-engine-
    # config-I10905 ruling toward `system` needs no export edit, only
    # `--tag-key system` — confirm the alias exists under that name too.
    system_column = m._tag_column("system")
    assert f"resource_tags.user_system AS {system_column}" in query


def test_iter_billing_periods_spans_month_boundary():
    periods = m.iter_billing_periods(dt.date(2026, 8, 25), dt.date(2026, 9, 3))
    assert periods == ["2026-08", "2026-09"]


def test_iter_billing_periods_single_month():
    assert m.iter_billing_periods(dt.date(2026, 9, 1), dt.date(2026, 9, 28)) == ["2026-09"]


def test_read_cur_window_sums_only_tagged_rows_in_window():
    key = "cur/nous-ergon-fleet-cur/data/BILLING_PERIOD=2026-09/part-0.parquet"
    rows = [
        _row(1.50, "2026-09-01"),
        _row(2.25, "2026-09-02"),
        _row(9.99, "2026-09-02", other_component="crucible-v2"),  # wrong tag value
        _row(0.75, "2026-08-15"),  # outside the window
    ]
    reader = _FakeReader({key: _parquet_bytes(rows)})
    window = m.read_cur_window(
        reader,
        cur_bucket="bucket",
        cur_prefix="cur/nous-ergon-fleet-cur",
        tag_key="component",
        tag_value="data-collection",
        start=dt.date(2026, 9, 1),
        end=dt.date(2026, 9, 30),
    )
    assert window.total_cost == pytest.approx(3.75)
    assert window.rows_matched == 2
    assert window.rows_scanned == 4
    assert window.days_covered == 30  # the whole delivered September period
    assert window.uncovered_days == ()
    assert window.billing_periods_read == ("2026-09",)


def test_read_cur_window_missing_period_is_a_gap_not_a_zero():
    reader = _FakeReader({})  # no objects for any period
    window = m.read_cur_window(
        reader,
        cur_bucket="bucket",
        cur_prefix="cur/nous-ergon-fleet-cur",
        tag_key="component",
        tag_value="data-collection",
        start=dt.date(2026, 9, 1),
        end=dt.date(2026, 9, 3),
    )
    assert window.total_cost == 0.0
    assert window.days_covered == 0
    assert window.uncovered_days == ("2026-09-01", "2026-09-02", "2026-09-03")
    assert window.billing_periods_read == ()


def test_read_cur_window_a_day_with_zero_matched_spend_still_counts_as_covered():
    """A billing period that DELIVERED but had no matching-tag spend on a
    given day (e.g. a weekend with no launch) is a covered day at $0, not an
    uncovered gap — the report was measured, the answer was zero."""
    key = "cur/x/data/BILLING_PERIOD=2026-09/part-0.parquet"
    rows = [_row(1.0, "2026-09-01")]  # only day 1 has our tag's spend
    reader = _FakeReader({key: _parquet_bytes(rows)})
    window = m.read_cur_window(
        reader,
        cur_bucket="bucket",
        cur_prefix="cur/x",
        tag_key="component",
        tag_value="data-collection",
        start=dt.date(2026, 9, 1),
        end=dt.date(2026, 9, 5),
    )
    assert window.days_covered == 5
    assert window.total_cost == pytest.approx(1.0)


def test_read_cur_window_raises_on_schema_mismatch_rather_than_reporting_zero():
    key = "cur/x/data/BILLING_PERIOD=2026-09/part-0.parquet"
    rows = [{"some_other_column": 1}]
    reader = _FakeReader({key: _parquet_bytes(rows)})
    with pytest.raises(ValueError, match="schema mismatch"):
        m.read_cur_window(
            reader,
            cur_bucket="bucket",
            cur_prefix="cur/x",
            tag_key="component",
            tag_value="data-collection",
            start=dt.date(2026, 9, 1),
            end=dt.date(2026, 9, 2),
        )


def test_build_metric_shape_matches_what_the_clause_reads():
    window = m.CostWindow(
        total_cost=12.34,
        days_requested=28,
        days_covered=28,
        billing_periods_read=("2026-09",),
        objects_read=1,
        rows_scanned=10,
        rows_matched=4,
    )
    metric = m.build_metric(
        window=window,
        tag_key="component",
        tag_value="data-collection",
        cur_bucket="bucket",
        cur_prefix="cur/x",
        as_of=dt.datetime(2026, 9, 21, tzinfo=UTC),
    )
    # `read_cost_baseline_measured` reads exactly these two fields.
    assert metric["baseline"] == 12.34
    assert metric["days_covered"] == 28
    assert metric["tag_key"] == "component"
    assert metric["tag_value"] == "data-collection"


class _PutCapturingS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        return {}


def test_main_refuses_to_run_without_cur_location(monkeypatch):
    """The issue's dispatch instructions: if CUR lacks what is needed, stop
    and report — never fall back to Cost Explorer. No CUR export exists in
    this account as of 2026-09-21, so `main()` must fail loud rather than
    default to a bucket/prefix nothing delivers to."""
    s3 = _PutCapturingS3()

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            assert name == "s3"
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())

    with pytest.raises(RuntimeError, match="no CUR/Data Export is configured"):
        m.main(["--days", "3"])

    # The failure is recorded via a run record, never swallowed.
    assert len(s3.puts) == 1
    run_record = json.loads(s3.puts[0]["Body"])
    assert run_record["status"] == "error"
    assert "CUR" in run_record["error"]


def test_main_writes_the_metric_document_when_cur_is_configured(monkeypatch):
    key = "cur/x/data/BILLING_PERIOD=2026-09/part-0.parquet"
    payload = _parquet_bytes([_row(5.0, "2026-09-01")])

    class _FullS3(_PutCapturingS3):
        def get_paginator(self, name):
            assert name == "list_objects_v2"

            class _Paginator:
                def paginate(self, Bucket, Prefix):
                    if Prefix == "cur/x/data/BILLING_PERIOD=2026-09/":
                        return [{"Contents": [{"Key": key}]}]
                    return [{"Contents": []}]

            return _Paginator()

        def get_object(self, Bucket, Key):
            class _Body:
                def read(self_inner):
                    return payload

            return {"Body": _Body()}

    s3 = _FullS3()

    class _FakeBoto3:
        @staticmethod
        def client(name, region_name=None):
            assert name == "s3"
            return s3

    monkeypatch.setitem(__import__("sys").modules, "boto3", _FakeBoto3())

    rc = m.main(["--days", "1", "--cur-bucket", "curbucket", "--cur-export-name", "cur/x"])
    assert rc == 0
    assert len(s3.puts) == 2
    body = json.loads(s3.puts[0]["Body"])
    assert body["tag_key"] == "component"
    assert body["tag_value"] == "data-collection"
    assert s3.puts[0]["Key"] == m.DEFAULT_KEY

    run_record = json.loads(s3.puts[1]["Body"])
    assert s3.puts[1]["Key"].startswith("data_collection/runs/cost_monthly/")
    assert run_record["status"] == "ok"
