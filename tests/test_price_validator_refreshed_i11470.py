"""Post-refresh price validation — alpha-engine-config-I11470.

Two defects, one run (2026-09-23): the summary logged "32/100 tickers have
anomalies" with no detail, and MRVL failed with a HeadObject 404 although
``reference/price_cache/MRVL.parquet`` had been written minutes earlier. The
404 was the wrong key, not a race: the validator read ``{s3_prefix}{ticker}``
verbatim with the retired ``predictor/price_cache/`` sentinel, whose tree
froze on 2026-06-19 — before MRVL joined the index.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pandas as pd
from botocore.exceptions import ClientError

from validators.price_validator import (
    ANOMALY_VOLUME_SPIKE,
    HISTORY_ANOMALY_MISSING_OBJECT,
    validate_parquet,
    validate_refreshed,
)


def _frame(n: int = 40, spike_at: int | None = None) -> pd.DataFrame:
    idx = pd.bdate_range("2026-07-01", periods=n)
    vol = [1_000_000.0] * n
    if spike_at is not None:
        vol[spike_at] = 50_000_000.0
    close = [100.0 + i * 0.1 for i in range(n)]
    return pd.DataFrame(
        {"Open": close, "High": [c + 1 for c in close], "Low": [c - 1 for c in close],
         "Close": close, "Volume": vol},
        index=idx,
    )


def _s3(objects: dict[str, pd.DataFrame]) -> MagicMock:
    s3 = MagicMock()

    def download_file(bucket, key, path):
        if key not in objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        objects[key].to_parquet(path)

    s3.download_file.side_effect = download_file
    return s3


def test_the_legacy_sentinel_reads_the_tree_the_refresh_wrote():
    s3 = _s3({"reference/price_cache/MRVL.parquet": _frame()})
    out = validate_refreshed(s3, "bucket", "predictor/price_cache/", ["MRVL"])
    keys = [c.args[1] for c in s3.download_file.call_args_list]
    assert keys == ["reference/price_cache/MRVL.parquet"]
    assert out["anomalies"] == 0
    assert out["validated_prefixes"] == ["reference/price_cache/"]


def test_a_genuinely_missing_object_is_its_own_type_and_names_the_key():
    out = validate_refreshed(_s3({}), "bucket", "predictor/price_cache/", ["MRVL"])
    assert out["anomaly_counts_by_type"] == {HISTORY_ANOMALY_MISSING_OBJECT: 1}
    detail = out["anomaly_details"][0]
    assert "s3://bucket/reference/price_cache/MRVL.parquet" in detail["anomalies"][0]


def test_the_summary_counts_and_names_anomalies_by_type(caplog):
    s3 = _s3({
        "reference/price_cache/AVGO.parquet": _frame(spike_at=30),
        "reference/price_cache/META.parquet": _frame(spike_at=31),
        "reference/price_cache/AAPL.parquet": _frame(),
    })
    with caplog.at_level(logging.WARNING, logger="validators.price_validator"):
        out = validate_refreshed(
            s3, "bucket", "predictor/price_cache/", ["AVGO", "META", "AAPL", "MRVL"]
        )
    assert out["anomalies"] == 3
    assert out["anomaly_counts_by_type"] == {
        ANOMALY_VOLUME_SPIKE: 2, HISTORY_ANOMALY_MISSING_OBJECT: 1,
    }
    assert out["anomaly_tickers_by_type"][ANOMALY_VOLUME_SPIKE] == ["AVGO", "META"]
    summary_line = next(r.getMessage() for r in caplog.records if "3/4" in r.getMessage())
    assert "volume_spike=2" in summary_line and "AVGO" in summary_line
    assert "missing_object=1" in summary_line and "MRVL" in summary_line


def test_validate_parquet_types_run_parallel_to_messages():
    out = validate_parquet(_frame(spike_at=30), "X")
    assert out["anomaly_types"] == [ANOMALY_VOLUME_SPIKE]
    assert len(out["anomalies"]) == len(out["anomaly_types"])
