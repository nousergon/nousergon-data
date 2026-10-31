"""alpha-engine-config-I11552 — the date bound reads a bar's exchange-local wall date.

``_bars_after`` used to convert a tz-aware index to UTC before taking the
date. A daily bar stamped at local midnight on an exchange east of UTC then
read as the previous UTC day, so a D+1 bar passed both
``clip_to_trading_day`` (the fetch bound) and ``assert_no_bar_after`` (the
write-site guard). Latent while yfinance daily frames are tz-naive; these
tests pin the local-date basis so a tz-aware frame cannot slip through.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from dates import FutureBarRefused, assert_no_bar_after, clip_to_trading_day

D = date(2026, 9, 24)


def _daily(tz: str, days: list[str]) -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp(x) for x in days]).tz_localize(tz)
    return pd.DataFrame({"Close": range(len(days))}, index=idx)


@pytest.mark.parametrize("tz", ["Asia/Tokyo", "Europe/Zurich"])
def test_a_d_plus_1_bar_east_of_utc_is_refused_at_the_fetch_bound(tz):
    frame = _daily(tz, ["2026-09-23", "2026-09-24", "2026-09-25"])
    # Local midnight on 09-25 is still 09-24 in UTC; the old UTC basis let it pass.
    assert frame.index[-1].tz_convert("UTC").date() == D
    with pytest.raises(FutureBarRefused):
        clip_to_trading_day(frame, D, label=f"price_cache[TEST.{tz[:2]}]")


@pytest.mark.parametrize("tz", ["Asia/Tokyo", "Europe/Zurich"])
def test_a_d_plus_1_bar_east_of_utc_is_refused_at_the_write_site(tz):
    frame = _daily(tz, ["2026-09-24", "2026-09-25"])
    with pytest.raises(FutureBarRefused):
        assert_no_bar_after(frame.index, D, artifact="test_artifact")


@pytest.mark.parametrize("tz", ["Asia/Tokyo", "Europe/Zurich", "America/New_York", "UTC"])
def test_a_series_ending_on_d_passes_in_any_timezone(tz):
    frame = _daily(tz, ["2026-09-22", "2026-09-23", "2026-09-24"])
    out = clip_to_trading_day(frame, D, label="price_cache[TEST]")
    assert len(out) == 3
    assert_no_bar_after(frame.index, D, artifact="test_artifact")


def test_an_evening_bar_west_of_utc_on_d_is_not_read_as_d_plus_1():
    # 20:00 New York on D is 00:00 UTC on D+1. Its trading day is D.
    idx = pd.DatetimeIndex([pd.Timestamp("2026-09-24 20:00")]).tz_localize("America/New_York")
    assert idx[0].tz_convert("UTC").date() > D
    assert_no_bar_after(idx, D, artifact="test_artifact")
