"""`features.compute._load_sector_map` / `_load_sub_sector_etf_map` must
RAISE on a failed load, never silently degrade to an empty map.

`alpha-engine-config-I10923`. Both loaders used to swallow ANY exception
(a missing key, a malformed body, an S3 outage) into `log.warning(...)` +
`return {}`. An empty map does not fail the run — every ticker's sector /
sub-sector feature falls to its neutral default, the column is populated,
nothing is red, and the model trains on it. That is the `avg_volume_20d`
shape this repo's own incident history warns against (a units mismatch
silently failed 901/903 tickers' liquidity gate for months with no visible
symptom), and this repo is a PRODUCER — `AGENTS.md`'s fail-loud rule has no
graceful-degrade carve-out for a writer. `daily_append`'s call site (the
DAILY production path) had no wrapping try/except around either load, so a
raise here reaches the caller cleanly and becomes the failed run the fleet
already knows how to detect — see `test_weekly_collector_morning_enrich.py`
for the established shape (a raised `PolygonForbiddenError` from
`daily_closes.collect` produces `result["status"] == "failed"`) that this
now matches for the sector maps.

Class-sweep disposition (I10923 deliverable 3): grepped every
`s3.get_object`/`.get_object(` call in `features/`, `builders/`,
`collectors/` outside `tests/`. Beyond the two sites fixed here, 12 more
sites share the general "except -> return None/{}" shape
(`features/reader.py:145`, `features/compute.py:1092`,
`collectors/crypto_balances.py:252`, `collectors/alternative.py:975,1217`,
`collectors/signal_returns.py:229`, `collectors/universe_returns.py:386`,
`collectors/daily_closes_fred_repair.py:170`,
`collectors/constituents.py:667,672`, `collectors/macro.py:712`,
`collectors/metron_market_data.py:1010`) but differ materially: each
returns `None` (a value every caller must explicitly branch on) rather
than a falsy-but-still-iterable empty container that silently behaves like
valid data downstream — the exact shape that made the two sector-map sites
dangerous. `daily_closes_fred_repair.py` and `collectors/macro.py` already
re-raise every non-404 `ClientError`, only treating a genuine "not found"
as a `None`. These are lower-risk than the two fixed here and are left for
a follow-up sweep (filed separately) rather than folded into this PR.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from features.compute import ReferenceMapUnavailable, _load_sector_map, _load_sub_sector_etf_map


def _s3_that_raises(exc: Exception) -> MagicMock:
    s3 = MagicMock()
    s3.get_object.side_effect = exc
    return s3


def _s3_with_bad_body() -> MagicMock:
    """A GetObject that "succeeds" but returns a body json.loads chokes on —
    the malformed-payload case, distinct from a transport failure."""
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = b"{not valid json"
    s3.get_object.return_value = {"Body": body}
    return s3


def test_load_sector_map_raises_on_s3_failure():
    s3 = _s3_that_raises(RuntimeError("connection reset"))
    with pytest.raises(ReferenceMapUnavailable, match="sector_map.json"):
        _load_sector_map(s3, "test-bucket")


def test_load_sector_map_raises_on_malformed_json():
    s3 = _s3_with_bad_body()
    with pytest.raises(ReferenceMapUnavailable, match="sector_map.json"):
        _load_sector_map(s3, "test-bucket")


def test_load_sector_map_raises_names_the_bucket_and_key():
    s3 = _s3_that_raises(RuntimeError("NoSuchKey"))
    with pytest.raises(ReferenceMapUnavailable) as exc_info:
        _load_sector_map(s3, "alpha-engine-research")
    assert "s3://alpha-engine-research/data/sector_map.json" in str(exc_info.value)
    assert "alpha-engine-config-I10923" in str(exc_info.value)


def test_load_sector_map_returns_the_map_on_success():
    """Control: a genuinely successful load still returns the parsed map —
    this fix changes the FAILURE path only."""
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = b'{"AAPL": "XLK"}'
    s3.get_object.return_value = {"Body": body}
    assert _load_sector_map(s3, "test-bucket") == {"AAPL": "XLK"}


def test_load_sub_sector_etf_map_raises_on_s3_failure():
    s3 = _s3_that_raises(RuntimeError("connection reset"))
    with pytest.raises(ReferenceMapUnavailable, match="sub_sector_etf_map.json"):
        _load_sub_sector_etf_map(s3, "test-bucket")


def test_load_sub_sector_etf_map_raises_on_malformed_json():
    s3 = _s3_with_bad_body()
    with pytest.raises(ReferenceMapUnavailable, match="sub_sector_etf_map.json"):
        _load_sub_sector_etf_map(s3, "test-bucket")


def test_load_sub_sector_etf_map_returns_the_map_on_success():
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = b'{"AAPL": "SMH"}'
    s3.get_object.return_value = {"Body": body}
    assert _load_sub_sector_etf_map(s3, "test-bucket") == {"AAPL": "SMH"}


def test_daily_append_propagates_sector_map_failure_as_a_failed_run():
    """Integration-level: `builders.daily_append.daily_append` has no
    try/except around either loader (mirrors `_load_daily_closes`'s own
    "raises on missing/empty file; no need for status-return guard" idiom
    immediately above it in that function) — a raised
    `ReferenceMapUnavailable` must reach the caller uncaught, not become a
    silently-empty map."""
    import builders.daily_append as _da

    s3 = _s3_that_raises(RuntimeError("S3 outage"))

    from tests.conftest import recent_trading_day_str

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(_da, "boto3", MagicMock(client=lambda *a, **k: s3))
        mp.setattr(_da, "_load_daily_closes", lambda *a, **k: {
            "AAPL": {"Open": 1, "High": 1, "Low": 1, "Close": 1, "Volume": 1, "VWAP": 1},
        })
        with pytest.raises(ReferenceMapUnavailable):
            _da.daily_append(date_str=recent_trading_day_str())
