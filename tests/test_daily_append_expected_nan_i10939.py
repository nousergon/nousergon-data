"""A declared-expected NaN feature must not mark a row degraded; a real one must.

`alpha-engine-config-I10939`, second cause. After the deferred second-pass
columns were excluded (nousergon-data#1801), the live EOD append still read
`n_ok=0 n_partial=909` on 2026-09-22, 09-23, 09-24 and 09-25. 899 of the
909 partial rows on 09-25 had exactly one NaN feature: `vwap_divergence_pct`.
The EOD `daily_closes` pass writes `VWAP=None` for every yfinance row by
design (source mix on 2026-09-25: yfinance 926/926 NaN VWAP, fred 4/4).
`features.postflight.ALL_NULL_EXPECTED` already declared this, but the
write-time coverage count ignored the declaration.

These tests pin both directions. The declared shape (a yfinance/fred row with
no VWAP) is expected and counted apart. The same NaN on a polygon row (the
morning enrichment, which does carry VWAP) is still degraded.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import builders.daily_append as daily_append
from features.feature_engineer import FEATURES
from features.postflight import ALL_NULL_EXPECTED


def _row(*, source: str, vwap: float, nan: tuple[str, ...] = ()) -> pd.DataFrame:
    """One fully-featured today_row except for the named NaN features."""
    data: dict[str, list] = {c: [100.0] for c in ("Open", "High", "Low", "Close")}
    data["Volume"] = [1_000_000.0]
    data["VWAP"] = [vwap]
    for f in FEATURES:
        data[f] = [np.nan if f in nan or f in daily_append._DEFERRED_SECOND_PASS_FEATURES else 0.1]
    data[daily_append.PROVENANCE_COL] = [source]
    return pd.DataFrame(data, index=[pd.Timestamp("2026-09-25")])


def test_expected_nan_declaration_is_in_lockstep_with_postflight():
    """Every feature declared ALL_NULL_EXPECTED has an input column here, and
    nothing is exempted here that postflight does not also declare."""
    assert set(daily_append._EXPECTED_NAN_INPUT) == set(ALL_NULL_EXPECTED)
    assert "polygon" not in daily_append._VWAP_LESS_SOURCES


@pytest.mark.parametrize("source", ["yfinance", "fred"])
def test_eod_row_without_vwap_is_fully_featured(source):
    """The live 2026-09-25 shape: a VWAP-less source row whose only NaN is
    vwap_divergence_pct counts toward n_ok, and the NaN is reported as expected."""
    row = _row(source=source, vwap=np.nan, nan=("vwap_divergence_pct",))
    degraded, expected = daily_append._write_time_nan_features(row)
    assert degraded == []
    assert expected == ["vwap_divergence_pct"]


def test_polygon_row_missing_vwap_is_still_degraded():
    """A polygon row carries VWAP by design, so a NaN there is a real outage
    and must keep counting as partial."""
    row = _row(source="polygon", vwap=np.nan, nan=("vwap_divergence_pct",))
    degraded, expected = daily_append._write_time_nan_features(row)
    assert degraded == ["vwap_divergence_pct"]
    assert expected == []


def test_vwap_present_but_feature_nan_is_degraded():
    """If the input exists and the feature is still NaN, the declared reason
    does not apply. That is a compute defect, not the structural shape."""
    row = _row(source="yfinance", vwap=101.0, nan=("vwap_divergence_pct",))
    degraded, expected = daily_append._write_time_nan_features(row)
    assert degraded == ["vwap_divergence_pct"]
    assert expected == []


def test_other_nan_features_on_an_eod_row_stay_degraded():
    """The exemption covers the declared feature only. The 10 rows on
    2026-09-25 with a short-history NaN (e.g. sector_mom_pct) stay partial."""
    row = _row(source="yfinance", vwap=np.nan, nan=("vwap_divergence_pct", "sector_mom_pct"))
    degraded, expected = daily_append._write_time_nan_features(row)
    assert degraded == ["sector_mom_pct"]
    assert expected == ["vwap_divergence_pct"]


def test_deferred_second_pass_columns_are_in_neither_list():
    row = _row(source="yfinance", vwap=np.nan, nan=("vwap_divergence_pct",))
    degraded, expected = daily_append._write_time_nan_features(row)
    assert not set(degraded + expected) & daily_append._DEFERRED_SECOND_PASS_FEATURES


def test_row_without_provenance_gets_no_exemption():
    """Unknown provenance cannot claim the structural reason."""
    row = _row(source="yfinance", vwap=np.nan, nan=("vwap_divergence_pct",))
    row = row.drop(columns=[daily_append.PROVENANCE_COL])
    degraded, expected = daily_append._write_time_nan_features(row)
    assert degraded == ["vwap_divergence_pct"]
    assert expected == []


def test_expected_nan_counts_reach_the_result():
    """The expected-NaN count is reported on the result, so exempting it from
    n_partial does not make it invisible."""
    src = Path(daily_append.__file__).read_text()
    assert '"expected_nan_features": dict(expected_nan_counts)' in src
    assert "nan_features, expected_nan = _write_time_nan_features(today_row)" in src
