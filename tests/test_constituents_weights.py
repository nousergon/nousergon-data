"""
Per-constituent index weight capture — alpha-engine-config-I11295.

The ``Weight`` column has always been present in the SSGA holdings files the
collector downloads (the schema comment on ``_SSGA_HOLDINGS_URLS`` names it),
and ``_fetch_ssga_membership`` read only ``Ticker`` and discarded it. Weight is
the missing term in any index-relative contribution: contribution to an index's
daily move is ``weight_at_prior_close x return``, and the returns side already
exists full-population in ``collectors/daily_closes.py``.

These tests hold the contract the consumer depends on
(``alpha-engine-config-I11297``, the per-constituent contribution artifact):
weights are FRACTIONS normalised within each index, the pre-normalisation sum
is recorded so a units flip stays visible, and an absent weight is declared
rather than read as zero.
"""
from __future__ import annotations

from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import constituents


def _ssga_xlsx(rows: dict[str, list]) -> bytes:
    """Build an SSGA-holdings-shaped xlsx: 4-row banner, then the header row."""
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, startrow=4)
    return buf.getvalue()


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


def _patch_ssga(sp500_rows: dict[str, list], sp400_rows: dict[str, list]):
    """Serve the two SSGA holdings URLs and refuse anything else."""

    def fake_get(url, **kwargs):
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 500"]:
            return _Resp(_ssga_xlsx(sp500_rows))
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 400"]:
            return _Resp(_ssga_xlsx(sp400_rows))
        raise AssertionError(f"unexpected URL in weights test: {url}")

    return patch("collectors.constituents.requests.get", side_effect=fake_get)


def test_weights_are_fractions_summing_to_one_within_each_index() -> None:
    """Normalisation is PER INDEX: an S&P 500 name's weight is its share of
    the S&P 500, not of the combined 500+400 roster. Cross-index
    normalisation would make a ticker's weight depend on the other fund's
    holdings, which is not what 'weight in the S&P 500' means."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "MSFT"], "Weight": [70.0, 30.0]},
        {"Ticker": ["TOST", "IESC"], "Weight": [40.0, 60.0]},
    ):
        _tickers, _sp500, _sp400, weights = constituents._fetch_ssga_membership()

    assert weights.weight_map["AAPL"] == pytest.approx(0.70)
    assert weights.weight_map["MSFT"] == pytest.approx(0.30)
    assert weights.weight_map["TOST"] == pytest.approx(0.40)
    assert weights.weight_map["IESC"] == pytest.approx(0.60)

    sp500_sum = sum(
        w for t, w in weights.weight_map.items() if weights.index_of[t] == "S&P 500"
    )
    sp400_sum = sum(
        w for t, w in weights.weight_map.items() if weights.index_of[t] == "S&P 400"
    )
    assert sp500_sum == pytest.approx(1.0, abs=1e-6)
    assert sp400_sum == pytest.approx(1.0, abs=1e-6)
    assert weights.method == "ssga_holdings_file"


def test_index_of_names_the_index_for_every_weighted_ticker() -> None:
    """A weight is uninterpretable without its index, and a consumer must not
    have to re-derive membership from counts — the ordering-contract failure
    mode of alpha-engine-config-I6946."""
    with _patch_ssga(
        {"Ticker": ["AAPL"], "Weight": [100.0]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        _t, _a, _b, weights = constituents._fetch_ssga_membership()

    assert weights.index_of == {"AAPL": "S&P 500", "TOST": "S&P 400"}
    assert set(weights.weight_map) == set(weights.index_of)


def test_raw_pre_normalisation_sum_is_recorded_per_index() -> None:
    """The raw sum is the only place a units flip or a filter change stays
    visible. Normalisation would otherwise absorb it and every weight would
    still sum to 1.0 while being wrong."""
    with _patch_ssga(
        # Equity rows sum to 97.5%; cash carries the rest and is filtered out.
        {"Ticker": ["AAPL", "MSFT", "CASH_USD"], "Weight": [60.0, 37.5, 2.5]},
        {"Ticker": ["TOST"], "Weight": [99.0]},
    ):
        _t, _a, _b, weights = constituents._fetch_ssga_membership()

    assert weights.raw_sum_by_index["S&P 500"] == pytest.approx(97.5)
    assert weights.raw_sum_by_index["S&P 400"] == pytest.approx(99.0)
    # Normalisation still runs over the equity rows only.
    assert weights.weight_map["AAPL"] == pytest.approx(60.0 / 97.5)


def test_missing_weight_column_raises_layout_drift() -> None:
    """A silently absent Weight column must not degrade to an unweighted
    roster — that is the state this issue exists to end."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "MSFT"], "Name": ["APPLE INC", "MICROSOFT CORP"]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        with pytest.raises(RuntimeError, match="missing 'Weight' column"):
            constituents._fetch_ssga_membership()


def test_member_row_with_no_numeric_weight_raises_rather_than_reading_zero() -> None:
    """A member with no weight is UNKNOWN weight. Reading it as zero would
    drop a real constituent's contribution silently and still reconcile."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "MSFT"], "Weight": [100.0, None]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        with pytest.raises(RuntimeError, match="no numeric Weight"):
            constituents._fetch_ssga_membership()


def test_a_sum_in_neither_units_band_raises() -> None:
    """Units are detected by BAND, and a sum in neither band is refused rather
    than guessed at. A single floor ('>= 50 means percent, else fractions')
    reads a broken percent file summing to 30 as fractions summing to 30 —
    nonsense in either unit, accepted as one of them."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "MSFT"], "Weight": [20.0, 10.0]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        with pytest.raises(RuntimeError, match="neither percent"):
            constituents._fetch_ssga_membership()


def test_a_file_already_in_fractions_is_accepted_without_renormalising_wrong() -> None:
    """If SSGA ever publishes fractions, the fraction band accepts it and the
    normalised weights are unchanged — the units flip is handled, not
    absorbed silently."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "MSFT"], "Weight": [0.7, 0.3]},
        {"Ticker": ["TOST"], "Weight": [1.0]},
    ):
        _t, _a, _b, weights = constituents._fetch_ssga_membership()

    assert weights.weight_map["AAPL"] == pytest.approx(0.70)
    assert weights.raw_sum_by_index["S&P 500"] == pytest.approx(1.0)


def test_share_class_weights_survive_the_dot_to_hyphen_bridge() -> None:
    """Membership keys are bridged from BRK.B to BRK-B for the yfinance
    convention the rest of the pipeline uses. A weight keyed on the raw form
    would be unreachable by every consumer."""
    with _patch_ssga(
        {"Ticker": ["BRK.B", "AAPL"], "Weight": [40.0, 60.0]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        tickers, _a, _b, weights = constituents._fetch_ssga_membership()

    assert "BRK-B" in tickers
    assert "BRK.B" not in weights.weight_map
    assert weights.weight_map["BRK-B"] == pytest.approx(0.40)
    assert weights.index_of["BRK-B"] == "S&P 500"


def test_a_ticker_appearing_twice_in_one_file_accumulates() -> None:
    """Two rows for one ticker in a single fund's file is a genuine duplicate
    holding, not a correction — overwriting would discard half its weight."""
    with _patch_ssga(
        {"Ticker": ["AAPL", "AAPL", "MSFT"], "Weight": [40.0, 20.0, 40.0]},
        {"Ticker": ["TOST"], "Weight": [100.0]},
    ):
        _t, _a, _b, weights = constituents._fetch_ssga_membership()

    assert weights.weight_map["AAPL"] == pytest.approx(0.60)


def test_cache_served_run_declares_cache_no_weights() -> None:
    """The cache exists for a source outage. A weight is only meaningful as of
    a date, so yesterday's weight presented as today's is a wrong answer
    wearing a right one's clothes. Absent is declarable; stale is not
    detectable downstream."""
    with patch("collectors.constituents._CACHE_PATH") as cache_path:
        cache_path.exists.return_value = True
        with patch(
            "collectors.constituents.pd.read_csv",
            return_value=pd.DataFrame(
                {
                    "ticker": ["AAPL"],
                    "gics_sector": ["Information Technology"],
                    "sector_etf": ["XLK"],
                    "gics_sub_industry": ["Technology Hardware"],
                    "index_weight": [0.07],
                    "index_name": ["S&P 500"],
                }
            ),
        ):
            *_rest, weights = constituents._load_from_cache()

    assert weights.method == "cache_no_weights"
    assert weights.weight_map == {}


def test_collect_publishes_weights_and_provenance() -> None:
    """The consumer (alpha-engine-config-I11297) reads these keys off
    constituents.json. `weight_method` is provenance it must honour rather
    than infer."""

    def fake_fetch():
        return (
            ["AAPL", "MSFT"],
            {"AAPL": "Information Technology", "MSFT": "Information Technology"},
            {"AAPL": "XLK", "MSFT": "XLK"},
            {},
            2,
            0,
            constituents.SsgaWeights(
                weight_map={"AAPL": 0.6, "MSFT": 0.4},
                index_of={"AAPL": "S&P 500", "MSFT": "S&P 500"},
                raw_sum_by_index={"S&P 500": 97.5},
                method="ssga_holdings_file",
            ),
        )

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch):
        result = constituents.collect(bucket="any", dry_run=True)

    assert result["status"] == "ok_dry_run"

    # dry_run returns the summary, so assert the published payload via a
    # non-dry-run write captured at the S3 boundary.
    captured: dict = {}

    def fake_put_object(**kwargs):
        if kwargs["Key"].endswith("constituents.json"):
            import json

            captured.update(json.loads(kwargs["Body"]))
        return {}

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch), \
         patch("collectors.constituents.boto3.client") as client:
        client.return_value.put_object.side_effect = fake_put_object
        constituents.collect(bucket="any", run_date="2026-09-21")

    assert captured["weight_map"] == {"AAPL": 0.6, "MSFT": 0.4}
    assert captured["index_of"] == {"AAPL": "S&P 500", "MSFT": "S&P 500"}
    assert captured["weight_method"] == "ssga_holdings_file"
    assert captured["weight_sum_raw_sp500"] == pytest.approx(97.5)
    assert captured["weight_sum_raw_sp400"] is None
