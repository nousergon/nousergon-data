"""The weekly backfill's S3 client pool covers ``_load_full_cache``'s fan-out.

rehearsal-2026-09-25-1 DataPhase1 logged ``Connection pool is full, discarding
connection: alpha-engine-research.s3.amazonaws.com. Connection pool size: 10``
ten times while ``_load_full_cache`` downloaded 969 parquets on 20 threads
through ``backfill``'s ``boto3.client("s3")``: botocore's default pool is 10,
so every connection past the tenth was opened, used once and thrown away.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import builders.backfill as _bf


def test_backfill_client_pool_covers_the_download_workers() -> None:
    captured: dict = {}

    def _client(service, *args, **kwargs):
        captured.setdefault(service, kwargs)
        return MagicMock()

    # Stop right after the client is built: an empty cache is the backfill's
    # own early return.
    with patch.object(_bf.boto3, "client", side_effect=_client), \
         patch.object(_bf, "_load_full_cache", return_value={}):
        out = _bf.backfill(bucket="test-bucket")

    assert out == {"status": "error", "error": "no_price_data"}
    config = captured["s3"].get("config")
    assert config is not None, "backfill built its S3 client with botocore's default pool (10)"
    assert config.max_pool_connections >= _bf._FULL_CACHE_WORKERS


def test_full_cache_download_uses_the_declared_worker_count() -> None:
    """The pool size and the thread count come from one constant, so raising
    one cannot silently outgrow the other."""
    seen: dict = {}
    real_executor = _bf.ThreadPoolExecutor

    def _executor(*args, **kwargs):
        seen["max_workers"] = kwargs.get("max_workers", args[0] if args else None)
        return real_executor(*args, **kwargs)

    with patch.object(_bf, "list_price_cache_keys", return_value=["reference/price_cache/AAA.parquet"]), \
         patch.object(_bf, "_load_parquet_from_s3", side_effect=RuntimeError("unreadable")), \
         patch.object(_bf, "ThreadPoolExecutor", side_effect=_executor):
        _bf._load_full_cache(MagicMock(), "test-bucket")

    assert seen["max_workers"] == _bf._FULL_CACHE_WORKERS
