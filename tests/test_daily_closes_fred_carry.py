"""A FRED carry never displaces an existing same-day close
(alpha-engine-config-I12866).

Replay of trading day 2026-10-21: FRED was down, so the D19 EOD wrote the
index rows from yfinance with correct same-day closes. The next morning's
window run asked FRED for 10-21, FRED had published only 10-20, and
``_fred_value_on_or_before`` returned the 10-20 observation. ``fred`` outranks
``yfinance`` in ``VENDOR_PRECEDENCE``, so the T-1 value replaced the T close
under the 10-21 date for ~11 hours.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from collectors import daily_closes
from collectors.daily_closes import (
    _coalesce_by_source_priority,
    _fred_carry_tickers,
    _fred_observation_on_or_before,
)
from tests.test_daily_closes_skip_if_canonical import (
    _existing_parquet_with_revisions,
    _written,
)

RUN_DATE = "2026-10-21"
SAME_DAY = {"VIX": 16.68, "VIX3M": 18.72, "TNX": 5.146}
PRIOR_DAY = {"VIX": 17.31, "VIX3M": 18.99, "TNX": 5.12}
TICKERS = ["^VIX", "^VIX3M", "^TNX"]


def _cache(obs_date: str, values: dict[str, float]) -> dict:
    return {t: [("2026-10-17", 1.0), (obs_date, v)] for t, v in values.items()}


def _existing_yfinance() -> dict:
    return {t: {"Close": v, "source": "yfinance", "revision": 1} for t, v in SAME_DAY.items()}


def _collect(s3, source: str, cache: dict):
    with patch("collectors.daily_closes.boto3.client", return_value=s3):
        with patch.object(
            daily_closes, "_fetch_polygon_closes", return_value=0
        ), patch.object(
            daily_closes, "_fetch_yfinance_closes", return_value=0
        ):
            return daily_closes.collect(
                bucket="b", tickers=TICKERS, run_date=RUN_DATE,
                # polygon_only + skip_if_canonical would run the polygon split
                # scan (network) and skip a file with no equity rows; the
                # coalesce under test runs either way.
                source=source, skip_if_canonical=(source != "polygon_only"),
                fred_window_cache=cache,
            )


@pytest.mark.parametrize("source", ["polygon_only", "yfinance_only"])
def test_replay_2026_10_21_same_day_close_is_unchanged(source):
    s3 = _existing_parquet_with_revisions(_existing_yfinance())
    _collect(s3, source, _cache("2026-10-20", PRIOR_DAY))
    out = _written(s3)
    for t, v in SAME_DAY.items():
        assert out.loc[t, "Close"] == v, t
        assert out.loc[t, "source"] == "yfinance", t
        assert out.loc[t, "revision"] == 1, t


@pytest.mark.parametrize("source", ["polygon_only", "yfinance_only"])
def test_a_same_day_fred_observation_still_outranks_yfinance(source):
    s3 = _existing_parquet_with_revisions(_existing_yfinance())
    _collect(s3, source, _cache(RUN_DATE, {"VIX": 16.70, "VIX3M": 18.70, "TNX": 5.13}))
    out = _written(s3)
    assert out.loc["VIX", "Close"] == 16.70
    assert out.loc["VIX", "source"] == "fred"
    assert out.loc["VIX", "revision"] == 2


def test_carry_tickers_are_the_rows_observed_before_the_run_date():
    records = [
        daily_closes._fred_record("VIX", RUN_DATE, 17.31),
        daily_closes._fred_record("TNX", RUN_DATE, 5.13),
        {"ticker": "AAPL", "date": RUN_DATE, "Close": 1.0, "source": "polygon"},
    ]
    cache = {"VIX": [("2026-10-20", 17.31)], "TNX": [("2026-10-20", 5.12), (RUN_DATE, 5.13)]}
    assert _fred_carry_tickers(records, RUN_DATE, cache) == {"VIX"}
    assert _fred_carry_tickers(records, RUN_DATE, None) == set()
    assert _fred_observation_on_or_before(cache["TNX"], "2026-10-20") == "2026-10-20"
    assert _fred_observation_on_or_before(cache["TNX"], "2026-10-19") is None


def _row(ticker, close, source):
    return {"ticker": ticker, "date": RUN_DATE, "Close": close, "source": source, "revision": 1}


def test_a_carry_lands_where_there_is_no_existing_row():
    """The same-day morning file still carries yesterday's FRED value."""
    merged, stats = _coalesce_by_source_priority(
        [_row("VIX", 17.31, "fred")], [], RUN_DATE, stale_new={"VIX"},
    )
    assert merged[0]["Close"] == 17.31
    assert stats["new_only"] == 1


def test_a_carry_still_replaces_a_prior_fred_row():
    merged, stats = _coalesce_by_source_priority(
        [_row("VIX", 17.31, "fred")], [_row("VIX", 17.20, "fred")], RUN_DATE,
        stale_new={"VIX"},
    )
    assert merged[0]["Close"] == 17.31
    assert stats["overwritten"] == 1


def test_a_carry_does_not_replace_a_real_yfinance_close():
    carried: list[str] = []
    merged, stats = _coalesce_by_source_priority(
        [_row("VIX", 17.31, "fred")], [_row("VIX", 16.68, "yfinance")], RUN_DATE,
        carried=carried, stale_new={"VIX"},
    )
    assert merged[0]["Close"] == 16.68
    assert stats["downgrade_blocked"] == 1
    assert carried == ["VIX"]


def test_a_carry_fills_a_null_existing_cell():
    merged, _ = _coalesce_by_source_priority(
        [_row("VIX", 17.31, "fred")], [_row("VIX", None, "yfinance")], RUN_DATE,
        stale_new={"VIX"},
    )
    assert merged[0]["Close"] == 17.31
