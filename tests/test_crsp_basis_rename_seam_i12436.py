"""alpha-engine-config-I12436 (2026-10-29 build): a renamed symbol joins its
raw history at the rename, and a seam mismatch fails one name, not the build.

The 2026-10-28 ``universe-crsp-basis-build`` stopped on BNY: polygon's
aggregates are keyed by symbol, and the BNY bars before BNY Mellon's
2026-05-22 rename (BK -> BNY, ``collectors/data/sp500_known_retickers.json``)
belong to the symbol's previous holder, so the un-adjusted yfinance history
(BNY Mellon's own, under its new symbol) could not join them at the seam. That
one name then stopped the build with 793 of 909 names unread.

Every price here is SYNTHETIC (no vendor data, no network).
"""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m
from tests.test_crsp_basis_raw_closes_i12674 import (
    TODAY,
    _FakePolygonBars,
    _FakeYF,
    _ohlcv,
    _sessions,
    _truth_split_only,
)

RENAME = "2026-05-22"
RETICKERS = [{"old": "BK", "new": "BNY", "effective": RENAME}]


class _SymbolKeyedPolygon(_FakePolygonBars):
    """Polygon double that, like polygon, serves whatever traded under the
    symbol in ``[start, end]`` — including a previous holder's bars."""

    def __init__(self, bars: pd.DataFrame, honour_start: bool = True):
        super().__init__(bars)
        self.honour_start = honour_start

    def get_daily_bars(self, ticker, start, end, adjusted=True):
        self.calls.append((ticker, start, end, adjusted))
        bars = self.bars
        if self.honour_start:
            bars = bars[bars.index >= pd.Timestamp(start)]
        return bars.copy()


def _renamed_market(seed: int = 11):
    """The entity's split-only truth (no splits, so it is also raw), its
    yfinance frame under the NEW symbol, and polygon's bars for that symbol:
    another issuer's at a different level before the rename, the entity's from
    it."""
    idx = _sessions("2023-01-02", TODAY)
    truth = _truth_split_only(idx, seed)
    yf_frame = _ohlcv(truth)
    yf_frame["Adj Close"] = truth * 0.97
    yf_frame["Dividends"] = 0.0
    yf_frame["Stock Splits"] = 0.0

    rng = np.random.default_rng(seed + 1)
    other = pd.Series(
        12.0 * np.exp(np.cumsum(rng.normal(0.0, 0.003, len(idx)))), index=idx,
    )
    poly_close = truth.where(idx >= pd.Timestamp(RENAME), other)
    poly = _ohlcv(poly_close)
    poly = poly[poly.index >= pd.Timestamp(TODAY) - pd.Timedelta(days=m.POLYGON_RAW_LOOKBACK_DAYS)]
    return truth, _FakeYF("BNY", yf_frame), poly


def _fetch(monkeypatch, fake_yf, client, **kw):
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    return m.fetch_raw_prices("BNY", client=client, today=TODAY,
                              sleep=lambda s: None, **kw)


def test_symbol_held_from_reads_the_latest_rename_into_the_ticker():
    rows = RETICKERS + [{"old": "XX", "new": "BNY", "effective": "2019-01-02"}]
    assert m.symbol_held_from("BNY", rows) == pd.Timestamp(RENAME)
    assert m.symbol_held_from("BK", rows) is None
    assert m.symbol_held_from("AAPL", rows) is None


def test_the_curated_file_names_bnys_rename():
    assert m.symbol_held_from("BNY", m.load_known_retickers()) == pd.Timestamp(RENAME)


def test_polygon_is_asked_only_from_the_rename(monkeypatch):
    truth, fake_yf, poly = _renamed_market()
    client = _SymbolKeyedPolygon(poly)
    df = _fetch(monkeypatch, fake_yf, client, retickers=RETICKERS)

    (_symbol, start, end, adjusted), = client.calls
    assert (start, end, adjusted) == (RENAME, TODAY, False)
    assert df.attrs[m.POLYGON_RAW_FROM_ATTR] == RENAME
    # Before the rename the close is the entity's own (yfinance), never the
    # previous holder's polygon bars.
    pd.testing.assert_series_equal(
        df["Close"], truth.reindex(df.index), check_names=False, rtol=1e-12,
    )
    assert df.index[0] == pd.Timestamp("2023-01-02")


def test_a_client_that_ignores_start_is_clipped_to_the_rename(monkeypatch):
    truth, fake_yf, poly = _renamed_market(seed=12)
    client = _SymbolKeyedPolygon(poly, honour_start=False)
    df = _fetch(monkeypatch, fake_yf, client, retickers=RETICKERS)
    assert df.attrs[m.POLYGON_RAW_FROM_ATTR] == RENAME
    pd.testing.assert_series_equal(
        df["Close"], truth.reindex(df.index), check_names=False, rtol=1e-12,
    )


def test_without_the_rename_row_the_seam_refuses_the_previous_holder(monkeypatch):
    """The 2026-10-29 failure: joined at polygon's window start, the two
    halves are different instruments and the join refuses, as a JOIN error."""
    _truth, fake_yf, poly = _renamed_market(seed=13)
    with pytest.raises(m.RawPriceJoinError, match="seam"):
        _fetch(monkeypatch, fake_yf, _SymbolKeyedPolygon(poly), retickers=[])


def test_a_rename_outside_polygons_window_changes_nothing(monkeypatch):
    _truth, fake_yf, poly = _renamed_market(seed=14)
    poly = _ohlcv(_truth.loc[poly.index])  # the entity throughout the window
    client = _SymbolKeyedPolygon(poly)
    old = [{"old": "BK", "new": "BNY", "effective": "2019-01-02"}]
    _fetch(monkeypatch, fake_yf, client, retickers=old)
    (_symbol, start, _end, _adj), = client.calls
    assert start == (pd.Timestamp(TODAY) - pd.Timedelta(days=m.POLYGON_RAW_LOOKBACK_DAYS)).strftime("%Y-%m-%d")


def test_a_seam_join_error_fails_its_name_and_the_gate_but_not_the_build(monkeypatch):
    """Every other name is still reconciled, nothing is written, and the
    audit names the failing name with no name skipped."""
    from tests.test_migrate_universe_crsp_basis import _FakePolygon, _patch_orchestration

    idx = _sessions("2026-01-02", "2026-02-27")
    old = {t: pd.Series(50.0, index=idx) for t in ("A", "B", "C")}
    _live, scratch = _patch_orchestration(monkeypatch, old_closes=old)
    s3 = MagicMock()
    monkeypatch.setattr(m.boto3, "client", lambda *a, **k: s3)
    seen: list[str] = []

    def raw_fetch(ticker):
        seen.append(ticker)
        if ticker == "A":
            raise m.RawPriceJoinError(f"{ticker}: un-adjusted yfinance close is off at the seam")
        return _ohlcv(old[ticker])

    with pytest.raises(RuntimeError, match="1 per-ticker error"):
        m.migrate_universe_crsp_basis(
            apply=True, raw_fetch=raw_fetch, client=_FakePolygon({}, {}), workers=1,
            spill_dir=None, macro={}, sector_map={}, fundamentals={}, alt_data={},
        )
    assert seen == ["A", "B", "C"]
    audit = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert audit["status"] == "fail"
    assert audit["errors_count"] == 1 and audit["errors"][0]["ticker"] == "A"
    assert audit["aborted"] == [] and audit["reconciled_count"] == 2
    assert audit["written_count"] == 0
    scratch.write.assert_not_called()
