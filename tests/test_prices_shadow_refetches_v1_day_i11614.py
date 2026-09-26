"""alpha-engine-config-I11614 — a shadow run's staleness scan must not read
v1's same-day price-cache writes as the shadow's own freshness.

Under a shadow root, S3 reads pass through to live. v1's post-market refresh
rewrites every live parquet before the shadow runs, so the scan found all 930
tickers fresh and the shadow's D03 published nothing (shadow-sameday
2026-09-25, EmptyProduction). A live parquet that already holds the shadow's
trading day is v1's output for that day and must be re-fetched by the shadow.
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone

import pytest

from collectors import prices
from shadow.root import ShadowRoot, activate, deactivate

LIVE = "reference/price_cache/"
LEGACY = "predictor/price_cache/"

# v1 post-market refresh for Fri 2026-09-25 (~20:05Z, after the 16:00 ET close).
V1_SAME_DAY = datetime(2026, 9, 25, 20, 5, tzinfo=timezone.utc)
# The previous session's EOD write.
THU_EOD = datetime(2026, 9, 24, 20, 11, tzinfo=timezone.utc)
FRI = "2026-09-25"


class _FakeS3:
    def __init__(self, objects: dict[str, datetime]):
        self.objects = objects

    def get_paginator(self, _name):
        fake = self

        class _Paginator:
            def paginate(self, *, Bucket, Prefix):
                yield {"Contents": [
                    {"Key": k, "LastModified": lm}
                    for k, lm in fake.objects.items() if k.startswith(Prefix)
                ]}

        return _Paginator()


def _no_splits(start, end):
    return []


@pytest.fixture
def shadow_fri():
    activate(ShadowRoot(trading_day=dt.date(2026, 9, 25)))
    try:
        yield
    finally:
        deactivate()


def test_shadow_refetches_a_parquet_v1_already_wrote_for_the_trading_day(shadow_fri):
    s3 = _FakeS3({f"{LIVE}AAPL.parquet": V1_SAME_DAY, f"{LIVE}MSFT.parquet": V1_SAME_DAY})
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL", "MSFT"], 0, FRI, split_scan=_no_splits,
    )
    assert stale == ["AAPL", "MSFT"]


def test_shadow_still_refetches_an_aged_parquet_and_a_missing_one(shadow_fri):
    s3 = _FakeS3({f"{LIVE}AAPL.parquet": THU_EOD})
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL", "NEWCO"], 0, FRI, split_scan=_no_splits,
    )
    assert stale == ["AAPL", "NEWCO"]


def test_a_normal_run_still_skips_a_fresh_same_day_parquet():
    """Outside a shadow root nothing changes: v1's own fresh write is fresh."""
    s3 = _FakeS3({f"{LIVE}AAPL.parquet": V1_SAME_DAY})
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL"], 0, FRI, split_scan=_no_splits,
    ) == []
