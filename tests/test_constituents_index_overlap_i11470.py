"""Rebalance-day constituents snapshots keep their per-index rosters —
alpha-engine-config-I11470.

On a rebalance day both SPY and MDY hold the names moving between the two
indices (2026-09-18: ILMN and P, joining the S&P 500 at the 2026-09-21 open).
The combined ``tickers`` list is deduped across funds, so the two counts no
longer describe it, and ``collect`` used to OMIT ``sp500_tickers`` then —
in exactly the weeks membership moves. historical_constituents skipped that
snapshot. The per-index lists now come from each fund's own holdings.
"""
from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import constituents


def _xlsx(rows: dict[str, list]) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        pd.DataFrame(rows).to_excel(writer, index=False, startrow=4)
    return buf.getvalue()


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


def _serve(spy: dict[str, list], mdy: dict[str, list]):
    def fake_get(url, **kwargs):
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 500"]:
            return _Resp(_xlsx(spy))
        if url == constituents._SSGA_HOLDINGS_URLS["S&P 400"]:
            return _Resp(_xlsx(mdy))
        raise AssertionError(url)

    return patch("collectors.constituents.requests.get", side_effect=fake_get)


def test_the_fetch_carries_each_funds_own_member_list():
    with _serve(
        {"Ticker": ["AAPL", "ILMN", "P"], "Weight": [60.0, 20.0, 20.0]},
        {"Ticker": ["ILMN", "P", "TOST"], "Weight": [30.0, 30.0, 40.0]},
    ):
        tickers, n500, n400, weights = constituents._fetch_ssga_membership()
    assert tickers == ["AAPL", "ILMN", "P", "TOST"]
    assert (n500, n400) == (3, 3)
    assert weights.members_by_index == {
        "S&P 500": ["AAPL", "ILMN", "P"],
        "S&P 400": ["ILMN", "P", "TOST"],
    }


def test_collect_writes_explicit_rosters_and_the_overlap_on_a_rebalance_day():
    weights = constituents.SsgaWeights(
        weight_map={"AAPL": 0.6, "ILMN": 0.3, "TOST": 0.7},
        index_of={"AAPL": "S&P 500", "ILMN": "S&P 400", "TOST": "S&P 400"},
        raw_sum_by_index={"S&P 500": 100.0, "S&P 400": 100.0},
        method="ssga_holdings_file",
        members_by_index={"S&P 500": ["AAPL", "ILMN"], "S&P 400": ["ILMN", "TOST"]},
    )
    sectors = {t: "Information Technology" for t in ("AAPL", "ILMN", "TOST")}
    etfs = {t: "XLK" for t in ("AAPL", "ILMN", "TOST")}

    def fake_fetch():
        return (["AAPL", "ILMN", "TOST"], sectors, etfs, {}, 2, 2, weights)

    captured: dict = {}

    def put_object(**kwargs):
        if kwargs["Key"].endswith("constituents.json"):
            captured.update(json.loads(kwargs["Body"]))
        return {}

    with patch("collectors.constituents._fetch_constituents", side_effect=fake_fetch), \
         patch("collectors.constituents.boto3.client") as client:
        client.return_value.put_object.side_effect = put_object
        constituents.collect(bucket="any", run_date="2026-09-18")

    assert captured["sp500_tickers"] == ["AAPL", "ILMN"]
    assert captured["sp400_tickers"] == ["ILMN", "TOST"]
    assert captured["index_overlap"] == ["ILMN"]

    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(
        (Path(__file__).resolve().parent.parent / "contracts" / "constituents.schema.json").read_text()
    )
    jsonschema.validate(captured, schema)
