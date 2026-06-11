"""Metron market-data producer — EOD closes + FX for Metron's held universe.

`alpha-engine-data` is the system's sole market-data source; Metron consumes these
artifacts. Covers: reading Metron's published universe, building the versioned
closes + FX artifacts, writing dated + ``latest`` keys, omitting unpriceable symbols,
the dry-run no-write path, and the fail-soft empty-universe skip.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from collectors import metron_market_data as mmd


def _universe_s3(universe: dict | None) -> MagicMock:
    """A MagicMock S3 whose get_object returns ``universe`` JSON (or raises if None)."""
    s3 = MagicMock()
    if universe is None:
        s3.get_object.side_effect = Exception("NoSuchKey")
    else:
        body = MagicMock()
        body.read.return_value = json.dumps(universe).encode()
        s3.get_object.return_value = {"Body": body}
    return s3


def _puts(s3: MagicMock) -> dict[str, dict]:
    """Map every put_object call to {key: parsed-json-body}."""
    out = {}
    for call in s3.put_object.call_args_list:
        kw = call.kwargs
        out[kw["Key"]] = json.loads(kw["Body"].decode())
    return out


_UNIVERSE = {
    "schema_version": 1, "as_of": "2026-06-11", "source": "metron",
    "holdings": [
        {"yf_symbol": "AAPL", "currency": "USD"},
        {"yf_symbol": "1299.HK", "currency": "HKD"},
    ],
    "currencies": ["HKD"],
}


def test_builds_and_writes_closes_and_fx_artifacts():
    s3 = _universe_s3(_UNIVERSE)
    closes = lambda syms: {"AAPL": (201.5, "2026-06-11"), "1299.HK": (64.2, "2026-06-11")}
    fx = lambda ccys: {"HKD": 0.1282}

    result = mmd.collect(
        bucket="b", run_date="2026-06-11", s3_client=s3, close_source=closes, fx_source=fx
    )

    assert result["status"] == "ok"
    assert result["universe"] == 2 and result["closes"] == 2 and result["fx"] == 1
    puts = _puts(s3)
    # Dated + latest for both artifacts.
    assert set(puts) == {
        "market_data/eod_closes/2026-06-11.json", "market_data/eod_closes/latest.json",
        "market_data/fx/2026-06-11.json", "market_data/fx/latest.json",
    }
    closes_art = puts["market_data/eod_closes/latest.json"]
    assert closes_art["schema_version"] == mmd.CLOSES_SCHEMA_VERSION
    assert closes_art["source"] == "alpha-engine-data"
    # Currency carried from the universe; foreign listing keyed by yf_symbol.
    assert closes_art["closes"]["1299.HK"] == {"close": 64.2, "currency": "HKD", "bar_date": "2026-06-11"}
    fx_art = puts["market_data/fx/latest.json"]
    assert fx_art["base"] == "USD" and fx_art["rates"] == {"HKD": 0.1282}
    # Dated == latest (same payload written to both).
    assert puts["market_data/eod_closes/2026-06-11.json"] == closes_art


def test_unpriceable_symbol_is_omitted_not_fabricated():
    s3 = _universe_s3(_UNIVERSE)
    closes = lambda syms: {"AAPL": (201.5, "2026-06-11")}  # 1299.HK unpriceable
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3, close_source=closes, fx_source=lambda c: {})
    assert result["closes"] == 1
    closes_art = _puts(s3)["market_data/eod_closes/latest.json"]
    assert "1299.HK" not in closes_art["closes"]
    assert "AAPL" in closes_art["closes"]


def test_empty_universe_skips_without_writing():
    s3 = _universe_s3({"holdings": [], "currencies": []})
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3,
                         close_source=lambda s: {}, fx_source=lambda c: {})
    assert result["status"] == "skipped"
    s3.put_object.assert_not_called()


def test_missing_universe_object_fail_soft_skips():
    s3 = _universe_s3(None)  # get_object raises
    result = mmd.collect(bucket="b", run_date="2026-06-11", s3_client=s3,
                         close_source=lambda s: {}, fx_source=lambda c: {})
    assert result["status"] == "skipped"
    s3.put_object.assert_not_called()


def test_dry_run_writes_nothing():
    s3 = _universe_s3(_UNIVERSE)
    result = mmd.collect(
        bucket="b", run_date="2026-06-11", dry_run=True, s3_client=s3,
        close_source=lambda s: {"AAPL": (201.5, "2026-06-11")}, fx_source=lambda c: {"HKD": 0.1282},
    )
    assert result["status"] == "ok_dry_run"
    s3.put_object.assert_not_called()


def test_load_universe_parses_holdings_and_currencies():
    s3 = _universe_s3(_UNIVERSE)
    holdings, currencies = mmd.load_metron_universe("b", s3)
    assert {h["yf_symbol"] for h in holdings} == {"AAPL", "1299.HK"}
    assert currencies == ["HKD"]
