"""alpha-engine-config-I11518 — the price-cache staleness scan reads the LIVE
tree, and a skipped (fresh) ticker can never keep split-mismatched history.

Before: ``_find_stale_fast`` listed ``{prefix}`` verbatim, and every production
caller passes the retired ``predictor/price_cache/`` sentinel, whose tree froze
on 2026-06-19 (939 objects). Every ticker read stale on every run and was
re-fetched for 10 years, which also meant every split was folded into the
whole ``auto_adjust=True`` history within a day, whatever else had touched the
parquet (the chronic-gap self-heal APPENDS adjusted rows).

After: the scan resolves the sentinel through ``price_cache_read_prefixes`` —
the same chokepoint the refresh writes through — so a fresh parquet is skipped,
and the split guard decides which fresh tickers still need the full re-fetch.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from collectors import prices
from corporate_actions import CorporateAction

LIVE = "reference/price_cache/"
LEGACY = "predictor/price_cache/"

# 2026-09-23 (Wed) EOD write, as the daily refresh stamps it (~23:xx UTC).
WED_EOD = datetime(2026, 9, 23, 23, 30, tzinfo=timezone.utc)
THU = "2026-09-24"


def _frame(closes: list[float], end: str = "2026-09-23") -> pd.DataFrame:
    idx = pd.bdate_range(end=end, periods=len(closes))
    return pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes, "Close": closes,
         "Volume": [1_000_000] * len(closes)},
        index=idx,
    )


def _parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf)
    return buf.getvalue()


class _FakeS3:
    """A bucket whose listing honours ``Prefix`` and whose objects can be read."""

    def __init__(self, objects: dict[str, tuple[datetime, pd.DataFrame | None]]):
        self.objects = objects
        self.listed_prefixes: list[str] = []
        self.gets: list[str] = []

    def get_paginator(self, _name):
        fake = self

        class _Paginator:
            def paginate(self, *, Bucket, Prefix):
                fake.listed_prefixes.append(Prefix)
                yield {"Contents": [
                    {"Key": k, "LastModified": lm}
                    for k, (lm, _df) in fake.objects.items() if k.startswith(Prefix)
                ]}

        return _Paginator()

    def get_object(self, *, Bucket, Key):
        self.gets.append(Key)
        _lm, df = self.objects[Key]
        if df is None:
            raise RuntimeError("SlowDown: simulated read failure")
        return {"Body": io.BytesIO(_parquet_bytes(df))}


def _no_splits(start, end):
    return []


def _scan(*actions):
    def scan(start, end):
        return list(actions)
    return scan


# ── (1) the scan reads the live tree ────────────────────────────────────────


def test_sentinel_prefix_lists_the_live_tree_never_the_legacy_one():
    s3 = _FakeS3({
        f"{LEGACY}AAPL.parquet": (WED_EOD, None),
        f"{LIVE}MSFT.parquet": (WED_EOD, None),
    })
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL", "MSFT"], 1, THU, split_scan=_no_splits,
    )
    assert s3.listed_prefixes == [LIVE]
    # AAPL exists only in the frozen legacy tree → missing → full fetch.
    assert stale == ["AAPL"]


def test_fresh_live_parquet_is_skipped_although_the_legacy_copy_is_frozen():
    s3 = _FakeS3({
        f"{LEGACY}AAPL.parquet": (datetime(2026, 6, 19, 15, tzinfo=timezone.utc), None),
        f"{LIVE}AAPL.parquet": (WED_EOD, None),
    })
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL"], 1, THU, split_scan=_no_splits,
    ) == []


def test_a_ticker_with_no_live_object_is_missing_never_skipped():
    s3 = _FakeS3({f"{LIVE}archive/NEWCO.parquet": (WED_EOD, None)})
    # A nested key under the live prefix cannot stand in for the ticker.
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["NEWCO"], 3, THU, split_scan=_no_splits,
    ) == ["NEWCO"]


def test_custom_prefix_is_listed_verbatim():
    s3 = _FakeS3({"custom/px/AAPL.parquet": (WED_EOD, None)})
    assert prices._find_stale_fast(
        s3, "b", "custom/px/", ["AAPL"], 1, THU, split_scan=_no_splits,
    ) == []
    assert s3.listed_prefixes == ["custom/px/"]


def test_a_write_after_utc_midnight_holds_only_the_prior_session():
    """Written 00:10 UTC on 09-24 (20:10 ET on 09-23): the parquet can hold at
    most 09-23's bar. On 09-25 at max_stale=1 that is two sessions behind, so
    it must refresh — the calendar date (09-24) would have read it fresh."""
    s3 = _FakeS3({f"{LIVE}AAPL.parquet": (datetime(2026, 9, 24, 0, 10, tzinfo=timezone.utc), None)})
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL"], 1, "2026-09-25", split_scan=_no_splits,
    ) == ["AAPL"]


def test_a_mid_session_write_does_not_hold_that_session():
    assert prices._implied_last_bar(
        datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc)  # 11:00 ET, session open
    ).isoformat() == "2026-09-22"
    assert prices._implied_last_bar(WED_EOD).isoformat() == "2026-09-23"


# ── (3) the split guard ─────────────────────────────────────────────────────


def test_fresh_parquet_written_before_a_split_is_refetched_without_a_read():
    split = CorporateAction.from_split("NVDA", "2026-09-24", 1, 10)
    s3 = _FakeS3({f"{LIVE}NVDA.parquet": (WED_EOD, _frame([100.0] * 60))})
    forced: dict[str, str] = {}
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["NVDA"], 1, THU, split_scan=_scan(split), forced_refresh=forced,
    )
    assert stale == ["NVDA"]
    assert "before the ex_date" in forced["NVDA"]
    assert s3.gets == []


def test_self_heal_append_seam_after_a_split_is_refetched():
    """The chronic-gap self-heal appends post-split adjusted rows onto
    pre-split history and bumps LastModified: fresh by age, wrong by scale."""
    split = CorporateAction.from_split("NVDA", "2026-09-22", 1, 10)
    seamed = _frame([100.0 + 0.1 * i for i in range(58)] + [10.2, 10.3])
    s3 = _FakeS3({f"{LIVE}NVDA.parquet": (WED_EOD, seamed)})
    forced: dict[str, str] = {}
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["NVDA"], 1, THU, split_scan=_scan(split), forced_refresh=forced,
    )
    assert stale == ["NVDA"]
    assert "jumps by the split factor" in forced["NVDA"]


def test_a_flattened_post_split_rewrite_stays_fresh():
    split = CorporateAction.from_split("NVDA", "2026-09-22", 1, 10)
    flat = _frame([10.0 + 0.01 * i for i in range(60)])
    s3 = _FakeS3({f"{LIVE}NVDA.parquet": (WED_EOD, flat)})
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["NVDA"], 1, THU, split_scan=_scan(split),
    ) == []
    assert s3.gets == [f"{LIVE}NVDA.parquet"]


def test_parquet_ending_before_the_ex_date_is_refetched_even_if_written_after():
    """A behind-fetch upload (I11467) can be written after the ex_date and
    still end before it: the whole history is on the pre-split basis."""
    split = CorporateAction.from_split("NVDA", "2026-09-23", 1, 4)
    behind = _frame([100.0] * 60, end="2026-09-22")
    s3 = _FakeS3({f"{LIVE}NVDA.parquet": (WED_EOD, behind)})
    forced: dict[str, str] = {}
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["NVDA"], 1, THU, split_scan=_scan(split), forced_refresh=forced,
    ) == ["NVDA"]
    assert "parquet ends 2026-09-22" in forced["NVDA"]


def test_near_one_spinoff_ratio_is_not_mistaken_for_a_seam():
    split = CorporateAction.from_split("ABC", "2026-09-22", 1000, 1061)
    moving = _frame([50.0 * (0.97 if i % 2 else 1.03) for i in range(60)])
    s3 = _FakeS3({f"{LIVE}ABC.parquet": (WED_EOD, moving)})
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["ABC"], 1, THU, split_scan=_scan(split),
    ) == []


def test_an_unreadable_parquet_with_a_recent_split_is_refetched():
    split = CorporateAction.from_split("NVDA", "2026-09-22", 1, 10)
    s3 = _FakeS3({f"{LIVE}NVDA.parquet": (WED_EOD, None)})
    forced: dict[str, str] = {}
    assert prices._find_stale_fast(
        s3, "b", LEGACY, ["NVDA"], 1, THU, split_scan=_scan(split), forced_refresh=forced,
    ) == ["NVDA"]
    assert "could not read" in forced["NVDA"]


def test_future_splits_and_other_tickers_are_ignored_and_class_shares_map():
    future = CorporateAction.from_split("AAPL", "2026-09-30", 1, 4)
    other = CorporateAction.from_split("ZZZZ", "2026-09-22", 1, 10)
    class_share = CorporateAction.from_split("BRK.B", "2026-09-24", 1, 2)
    s3 = _FakeS3({
        f"{LIVE}AAPL.parquet": (WED_EOD, None),
        f"{LIVE}BRK-B.parquet": (WED_EOD, None),
    })
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["AAPL", "BRK-B"], 1, THU,
        split_scan=_scan(future, other, class_share),
    )
    assert stale == ["BRK-B"]


def test_split_scan_failure_falls_back_to_refreshing_every_fresh_ticker():
    def broken(start, end):
        raise ConnectionError("polygon down ?apiKey=SECRET")

    s3 = _FakeS3({
        f"{LIVE}AAPL.parquet": (WED_EOD, None),
        f"{LIVE}OLD.parquet": (datetime(2026, 9, 1, 23, tzinfo=timezone.utc), None),
    })
    forced: dict[str, str] = {}
    stale = prices._find_stale_fast(
        s3, "b", LEGACY, ["OLD", "AAPL", "NEW"], 1, THU,
        split_scan=broken, forced_refresh=forced,
    )
    assert stale == ["OLD", "AAPL", "NEW"]
    assert set(forced) == {"AAPL"}


def test_split_guard_scan_window_covers_the_lookback():
    seen = {}

    def scan(start, end):
        seen["window"] = (start, end)
        return []

    s3 = _FakeS3({f"{LIVE}AAPL.parquet": (WED_EOD, None)})
    prices._find_stale_fast(s3, "b", LEGACY, ["AAPL"], 1, THU, split_scan=scan)
    assert seen["window"] == ("2026-08-25", THU)


def test_production_split_scan_raises_instead_of_degrading(monkeypatch):
    """``corporate_actions.detect_splits`` swallows failures into ``[]``; the
    guard's scan must not, or "could not look" would read as "no splits"."""
    import polygon_client

    def _boom(*_a, **_k):
        raise RuntimeError("no key")

    monkeypatch.setattr(polygon_client, "polygon_client", _boom)
    with pytest.raises(RuntimeError):
        prices._polygon_split_scan("2026-08-25", THU)


def test_production_split_scan_maps_polygon_events(monkeypatch):
    import polygon_client

    client = MagicMock()
    client.get_recent_splits.return_value = [
        {"ticker": "NVDA", "execution_date": "2026-09-22", "split_from": 1, "split_to": 10},
    ]
    monkeypatch.setattr(polygon_client, "polygon_client", lambda *a, **k: client)
    (action,) = prices._polygon_split_scan("2026-08-25", THU)
    assert (action.ticker, action.ex_date, action.split_to) == ("NVDA", "2026-09-22", 10)
    client.get_recent_splits.assert_called_once_with("2026-08-25", THU)


# ── collect() carries the guard's decisions ─────────────────────────────────


def test_collect_refreshes_split_forced_tickers_and_reports_why(monkeypatch):
    split = CorporateAction.from_split("NVDA", "2026-09-24", 1, 10)
    s3 = _FakeS3({
        f"{LIVE}NVDA.parquet": (WED_EOD, None),
        f"{LIVE}AAPL.parquet": (WED_EOD, None),
    })
    monkeypatch.setattr(prices, "boto3", MagicMock(client=lambda *_a, **_k: s3))
    monkeypatch.setattr(prices, "_ALWAYS_DOWNLOAD", [])
    monkeypatch.setattr(prices, "_polygon_split_scan", _scan(split))
    refreshed: list[list[str]] = []

    def _fake_refresh(s3, bucket, s3_prefix, stale, fetch_period, batch_size, **_kw):
        refreshed.append(list(stale))
        return len(stale), [], [(t, 2500) for t in stale]

    monkeypatch.setattr(prices, "_refresh_stale", _fake_refresh)
    monkeypatch.setattr(
        "validators.price_validator.validate_refreshed", lambda *a, **k: {}, raising=False,
    )
    result = prices.collect(
        bucket="b", tickers=["NVDA", "AAPL"], staleness_threshold_days=1, reference_date=THU,
    )
    assert refreshed == [["NVDA"]]
    assert result["stale"] == 1 and result["total"] == 2
    assert result["split_forced_refresh"] == 1
    assert "before the ex_date" in result["split_forced_sample"]["NVDA"]
