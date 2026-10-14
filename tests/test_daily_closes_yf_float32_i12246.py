"""yfinance closes keep the vendor's decimal print above $1024 (alpha-engine-config-I12246).

yfinance returns prices quantised to float32. ``round(x, 4)`` recovers the
decimal only while the float32 half-ULP is under 5e-5 (prices below 1024).
On 2026-09-28 the shadow D19 wrote GWW 1255.6899 and NVR 6267.8901 for
vendor closes 1255.69 and 6267.89, and GWW's factor-loading z-scores then
missed a recompute from the settled close by up to 9.8e-7.
"""

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from collectors import daily_closes


def _f32(x: float) -> float:
    """What yfinance hands back for a vendor print ``x``."""
    return float(np.float32(x))


@pytest.mark.parametrize("vendor, old_written", [
    (1255.69, 1255.6899),   # GWW 2026-09-28
    (6267.89, 6267.8901),   # NVR 2026-09-28
    (1544.57, 1544.5699),   # MTD 2026-09-28 20:06Z read
    (2913.60, 2913.6001),   # AZO 2026-09-28 20:06Z read
])
def test_float32_artefact_above_1024_is_removed(vendor, old_written):
    # The bug as it shipped: round(x, 4) keeps the float32 artefact.
    assert round(_f32(vendor), 4) == old_written != vendor
    assert daily_closes._yf_price(_f32(vendor)) == vendor


@pytest.mark.parametrize("vendor", [175.21, 13.52, 55.455, 856.2925, 1053.98, 1712.89, 0.0123])
def test_prices_that_already_round_trip_are_unchanged(vendor):
    assert daily_closes._yf_price(_f32(vendor)) == round(_f32(vendor), 4) == vendor


@pytest.mark.parametrize("value", [1255.6899, 1257.5815, 6267.8901, 101.123456])
def test_a_genuine_float64_value_is_only_rounded(value):
    # Not float32-exact, so it never came through float32: do not move it.
    assert float(np.float32(value)) != value
    assert daily_closes._yf_price(value) == round(value, 4)


def test_nan_passes_through():
    assert np.isnan(daily_closes._yf_price(float("nan")))


def test_fetch_writes_vendor_decimal_for_every_price_field():
    gww = _f32(1255.69)
    frame = pd.DataFrame(
        {"Open": [_f32(1240.98)], "High": [_f32(1257.58)], "Low": [_f32(1236.29)],
         "Close": [gww], "Adj Close": [gww], "Volume": [612_345]},
        index=pd.DatetimeIndex(["2026-09-28"]),
    )
    mock_yf = MagicMock()
    mock_yf.download.return_value = frame
    records = []
    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        assert daily_closes._fetch_yfinance_closes(["GWW"], "2026-09-28", records) == 1
    row = records[0]
    assert (row["Open"], row["High"], row["Low"], row["Close"], row["Adj_Close"]) == (
        1240.98, 1257.58, 1236.29, 1255.69, 1255.69)


def test_close_derived_loading_matches_the_settled_close():
    """The downstream symptom: momentum_20d from the written close equals the
    one computed from the vendor's settled close, so its z-score reproduces."""
    prior = 1306.30
    written = daily_closes._yf_price(_f32(1255.69))
    assert written / prior - 1.0 == 1255.69 / prior - 1.0
