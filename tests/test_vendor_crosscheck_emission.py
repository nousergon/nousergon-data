"""The vendor cross-check verdict is MEASURED, is not overwritten, and reaches
the run manifest (alpha-engine-config-I10783).

Measured 2026-10-03 09:06Z on the live bucket, for trading day 2026-10-02:

* D17's per-date ``collect(source="polygon_only")`` wrote
  ``data_collection/metrics/vendor_divergence/2026-10-02.json`` as
  ``status: ok, n: 926`` — the pairing against D19's source-stamped
  ``staging/daily_closes`` rows worked;
* 0.3 s later ``weekly_collector``'s "no record" fallback overwrote it with
  ``status: unmeasurable, n: 0``, because in window mode the result it reads
  is ``_collect_window``'s aggregate, which never carried ``vendor_divergence``;
* the next morning's backfill window then rewrote older days from the
  polygon-overwritten rows (2026-09-21: n=926 -> n=2);
* and no D17/D19 manifest carried a ``vendor_crosscheck`` guard entry, which is
  the only thing ``data.phase2.vendor_divergence_emitted`` reads.

The fixture is a read-only slice of the real objects (provenance inside it).
"""

from __future__ import annotations

import datetime as dt
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from collectors import daily_closes
from collectors.cross_source_observer import (
    VENDOR_CROSSCHECK_GUARD,
    vendor_crosscheck_guard_entry,
    vendor_crosscheck_not_applicable_entry,
    vendor_divergence_key,
    write_vendor_divergence_metric,
)

FIXTURE = (
    Path(__file__).parent / "fixtures" / "vendor_divergence" / "daily_closes_2026-10-02_slice.json"
)
DAY = "2026-10-02"
BUCKET = "alpha-engine-research"
DC_KEY = f"staging/daily_closes/{DAY}.parquet"
METRIC_KEY = vendor_divergence_key(DAY)


def _fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def _not_found(op: str = "GetObject") -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey", "Message": "nope"}}, op)


def _parquet_bytes(rows: dict[str, dict]) -> bytes:
    df = pd.DataFrame.from_dict(rows, orient="index")
    df.index.name = "ticker"
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    return buf.getvalue()


class _FakeS3:
    """Key-addressed S3 stand-in: what was PUT is what a later GET returns."""

    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)
        self.puts: list[tuple[str, bytes]] = []

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _not_found("HeadObject")
        return {
            "LastModified": dt.datetime(2026, 10, 2, 22, 19, 46, tzinfo=dt.timezone.utc),
            "ContentLength": len(self.objects[Key]),
        }

    def get_object(self, Bucket, Key, **_):
        if Key not in self.objects:
            raise _not_found()
        body = self.objects[Key]
        return {"Body": MagicMock(read=lambda: body)}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body
        self.puts.append((Key, Body))
        return {"ETag": '"x"'}

    def record(self, key: str = METRIC_KEY) -> dict:
        return json.loads(self.objects[key])


def _polygon_side(rows: dict[str, dict]):
    def _fill(tickers, run_date, records, source):
        n = 0
        for ticker, row in rows.items():
            records.append({"ticker": ticker, **{k: v for k, v in row.items() if k != "revision"}})
            n += 1
        return n

    return _fill


def _run_d17_on_real_slice(s3: _FakeS3) -> dict:
    fx = _fixture()
    tickers = sorted(set(fx["d19_prior"]) | set(fx["d17_polygon"]))
    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
            patch.object(daily_closes, "_fetch_polygon_closes", side_effect=_polygon_side(fx["d17_polygon"])), \
            patch.object(daily_closes, "_fetch_yfinance_closes", return_value=0), \
            patch.object(daily_closes, "_fetch_fred_closes", return_value=0), \
            patch.object(daily_closes, "_POLYGON_MIN_COVERAGE", 0.0):
        return daily_closes.collect(
            bucket=BUCKET, tickers=tickers, run_date=DAY, source="polygon_only",
        )


# ── the guard name is the one the clause reads ──────────────────────────────


def test_guard_name_is_the_one_the_exit_clause_counts():
    from data_gate.exit_criteria import VENDOR_GUARD

    assert VENDOR_CROSSCHECK_GUARD == VENDOR_GUARD


# ── real 2026-10-02 data produces a measured verdict ────────────────────────


def test_real_2026_10_02_slice_produces_a_measured_verdict_on_the_result():
    fx = _fixture()
    s3 = _FakeS3({DC_KEY: _parquet_bytes(fx["d19_prior"])})

    result = _run_d17_on_real_slice(s3)

    assert result["status"] == "ok"
    record = s3.record()
    yfinance_prior = {t for t, r in fx["d19_prior"].items() if r["source"] == "yfinance"}
    paired = yfinance_prior & set(fx["d17_polygon"])
    # 12 equities paired; the 4 FRED rows and polygon-only VYLR are not pairs.
    assert record["status"] == "ok"
    assert record["n"] == len(paired) == 12
    assert record["value"] == 0.0
    assert record["breaching_symbols"] == []

    vendor = [g for g in result["guards"] if g["guard"] == VENDOR_CROSSCHECK_GUARD]
    assert len(vendor) == 1
    entry = vendor[0]
    assert entry["verdict"] == "ok"
    assert entry["mode"] == "observe"
    assert entry["key"] == METRIC_KEY
    assert entry["value"] == 0.0
    assert entry["baseline"] == record["bound"]
    assert "0 of 12" in entry["detail"]


def test_the_written_parquet_still_carries_the_polygon_source_stamp():
    fx = _fixture()
    s3 = _FakeS3({DC_KEY: _parquet_bytes(fx["d19_prior"])})
    _run_d17_on_real_slice(s3)
    out = pd.read_parquet(io.BytesIO(s3.objects[DC_KEY]))
    assert out.loc["AAPL", "source"] == "polygon"
    assert int(out.loc["AAPL", "revision"]) == 2
    assert out.loc["TNX", "source"] == "fred"  # retained, not compared


# ── a measured record is never overwritten by a lesser one ──────────────────


def test_unmeasurable_fallback_does_not_clobber_the_measured_record():
    """The 2026-10-02 09:06:05.338Z -> 09:06:05.643Z overwrite, replayed."""
    fx = _fixture()
    s3 = _FakeS3({DC_KEY: _parquet_bytes(fx["d19_prior"])})
    _run_d17_on_real_slice(s3)
    measured = s3.record()

    standing = write_vendor_divergence_metric(BUCKET, {}, {}, DAY, s3_client=s3)

    assert s3.record() == measured
    assert standing == measured
    assert vendor_crosscheck_guard_entry(standing, DAY)["verdict"] == "ok"


def test_a_backfill_pass_over_polygon_rows_does_not_shrink_the_record():
    """The next morning's window sees the polygon-overwritten parquet."""
    fx = _fixture()
    s3 = _FakeS3({DC_KEY: _parquet_bytes(fx["d19_prior"])})
    _run_d17_on_real_slice(s3)
    first = s3.record()

    _run_d17_on_real_slice(s3)  # prior is now D17's own polygon write

    assert s3.record() == first
    assert first["n"] == 12


def test_a_larger_measurement_replaces_a_smaller_one():
    s3 = _FakeS3({})
    write_vendor_divergence_metric(
        BUCKET, {"A": 1.0}, {"A": {"Close": 1.0, "source": "yfinance"}}, DAY, s3_client=s3,
    )
    assert s3.record()["n"] == 1
    write_vendor_divergence_metric(
        BUCKET,
        {"A": 1.0, "B": 2.0},
        {"A": {"Close": 1.0, "source": "yfinance"}, "B": {"Close": 2.0, "source": "yfinance"}},
        DAY,
        s3_client=s3,
    )
    assert s3.record()["n"] == 2


def test_an_unreadable_existing_object_is_replaced():
    s3 = _FakeS3({METRIC_KEY: b"not json"})
    write_vendor_divergence_metric(
        BUCKET, {"A": 1.0}, {"A": {"Close": 1.0, "source": "yfinance"}}, DAY, s3_client=s3,
    )
    assert s3.record()["status"] == "ok"


def test_a_failed_read_of_the_existing_record_raises_rather_than_overwriting_blind():
    s3 = _FakeS3({})
    s3.get_object = MagicMock(
        side_effect=ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject")
    )
    with pytest.raises(ClientError):
        write_vendor_divergence_metric(BUCKET, {}, {}, DAY, s3_client=s3)
    assert s3.puts == []


# ── window mode carries the target's record and verdict ─────────────────────


def test_window_aggregate_carries_the_target_dates_record_and_guard():
    record = {"status": "ok", "n": 926, "value": 0.0, "bound": 0.01}
    guard = vendor_crosscheck_guard_entry(record, DAY)

    def fake_collect(**kwargs):
        d = kwargs["run_date"]
        if d == DAY:
            return {"status": "ok", "vendor_divergence": record, "guards": [guard]}
        return {"status": "ok", "vendor_divergence": {"status": "unmeasurable", "n": 0}}

    with patch.object(daily_closes, "collect", side_effect=fake_collect):
        agg = daily_closes._collect_window(
            bucket=BUCKET, tickers=["AAPL"], run_date=DAY, s3_prefix="staging/daily_closes/",
            dry_run=True, source="polygon_only", window_days=3,
        )
    assert agg["vendor_divergence"] == record
    assert guard in agg["guards"]


# ── D17's fallback fills an absence only, and records the verdict ───────────


def _run_morning(dc_result: dict, writer: MagicMock) -> dict:
    fake_constituents = MagicMock()
    fake_constituents.load_from_s3.return_value = {"tickers": ["AAPL"]}
    fake_constituents.collect.return_value = {"status": "ok", "tickers": ["AAPL"], "date": DAY}
    import weekly_collector

    with patch("weekly_collector.constituents", fake_constituents), \
            patch("builders.prune_delisted_tickers.prune_delisted_tickers",
                  return_value={"status": "ok", "pruned_count": 0, "skipped_recent_count": 0}), \
            patch("weekly_collector.daily_closes.collect", return_value=dc_result), \
            patch("collectors.cross_source_observer.write_vendor_divergence_metric", writer), \
            patch("builders.daily_append.daily_append", return_value={"status": "ok"}):
        return weekly_collector._run_morning_enrich(
            {"bucket": "test-bucket", "market_data": {"s3_prefix": "market_data/"}},
            SimpleNamespace(date=DAY, dry_run=False, morning_enrich=True),
        )


def test_morning_fallback_does_not_fire_when_the_window_carried_a_record():
    record = {"status": "ok", "n": 926, "value": 0.0, "bound": 0.01}
    dc_result = {
        "status": "ok", "source": "polygon_only", "window_days": 2, "target_date": DAY,
        "per_date": {DAY: {"status": "ok"}}, "backfill_failed_dates": [],
        "vendor_divergence": record, "guards": [vendor_crosscheck_guard_entry(record, DAY)],
    }
    writer = MagicMock()
    result = _run_morning(dc_result, writer)
    writer.assert_not_called()
    guards = result["collectors"]["daily_closes"]["guards"]
    assert [g["verdict"] for g in guards if g["guard"] == VENDOR_CROSSCHECK_GUARD] == ["ok"]


def test_morning_fallback_records_the_standing_verdict_on_the_result():
    dc_result = {
        "status": "ok", "source": "polygon_only", "window_days": 2, "target_date": DAY,
        "per_date": {DAY: {"status": "ok", "skipped": True}}, "backfill_failed_dates": [],
    }
    standing = {"status": "ok", "n": 926, "value": 0.0, "bound": 0.01}
    writer = MagicMock(return_value=standing)
    result = _run_morning(dc_result, writer)
    writer.assert_called_once()
    guards = result["collectors"]["daily_closes"]["guards"]
    vendor = [g for g in guards if g["guard"] == VENDOR_CROSSCHECK_GUARD]
    assert [g["verdict"] for g in vendor] == ["ok"]


def test_morning_fallback_write_failure_is_unmeasurable_not_silent():
    dc_result = {
        "status": "ok", "source": "polygon_only", "window_days": 2, "target_date": DAY,
        "per_date": {DAY: {"status": "ok", "skipped": True}}, "backfill_failed_dates": [],
    }
    writer = MagicMock(side_effect=RuntimeError("s3 down"))
    result = _run_morning(dc_result, writer)
    vendor = [
        g for g in result["collectors"]["daily_closes"]["guards"]
        if g["guard"] == VENDOR_CROSSCHECK_GUARD
    ]
    assert [g["verdict"] for g in vendor] == ["unmeasurable"]
    assert "s3 down" in vendor[0]["detail"]


# ── D19's side, and the verdict vocabulary ──────────────────────────────────


def test_eod_yfinance_pass_records_not_applicable_naming_the_morning_comparison():
    fx = _fixture()
    rows = {t: r for t, r in fx["d19_prior"].items() if r["source"] == "yfinance"}

    def _yf(missing, run_date, records):
        for t in missing:
            if t in rows:
                records.append({"ticker": t, **{k: v for k, v in rows[t].items() if k != "revision"}})
        return len(records)

    s3 = _FakeS3({})
    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
            patch.object(daily_closes, "_fetch_yfinance_closes", side_effect=_yf), \
            patch.object(daily_closes, "_fetch_fred_closes", return_value=0), \
            patch.object(daily_closes, "_YFINANCE_MIN_COVERAGE", 0.0):
        result = daily_closes.collect(
            bucket=BUCKET, tickers=sorted(rows), run_date=DAY, source="yfinance_only",
        )
    assert result["status"] == "ok"
    vendor = [g for g in result["guards"] if g["guard"] == VENDOR_CROSSCHECK_GUARD]
    assert vendor == [vendor_crosscheck_not_applicable_entry(DAY)]
    assert METRIC_KEY in vendor[0]["detail"]
    assert METRIC_KEY not in s3.objects  # D19 never writes the metric


def test_breach_verdict_names_the_breaching_symbols():
    record = {
        "status": "breach", "n": 2, "value": 0.5, "bound": 0.01, "breach_threshold_bps": 50.0,
        "champion_vendor": "polygon",
        "breaching_symbols": [{"ticker": "AAPL", "diff_bps": 60.0}],
    }
    entry = vendor_crosscheck_guard_entry(record, DAY)
    assert entry["verdict"] == "breach"
    assert "AAPL" in entry["detail"]


def test_no_record_is_unmeasurable_never_ok():
    assert vendor_crosscheck_guard_entry(None, DAY)["verdict"] == "unmeasurable"
    assert vendor_crosscheck_guard_entry(
        {"status": "unmeasurable", "reason": "no pair"}, DAY
    )["verdict"] == "unmeasurable"
