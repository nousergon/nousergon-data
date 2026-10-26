"""alpha-engine-config-I12674: ``fetch_raw_prices`` must return a genuinely RAW
close, so a split in the window is applied exactly once.

yfinance ``auto_adjust=False`` turns off the dividend adjustment only; its
``Close`` is still split-adjusted. The build applied polygon's splits on top of
it, so a split the price-evidence gate "confirmed" off a coincident move was
applied twice. The fetch now takes polygon ``adjusted=false`` aggregates over
polygon's reach and joins yfinance history, un-adjusted for the splits on/after
polygon's first bar, before it; splits yfinance folded in before that bar are
marked in-basis and never re-applied.

Every price here is SYNTHETIC (no vendor data, no network): yfinance and the
polygon client are both faked. The tickers and ex-dates mirror the two cases
the 2026-10-25/26 build surfaced: EXPE (feed record inverted, corrected by the
market) and HON 2026-06-29 (1-for-2 reverse split).
"""

from __future__ import annotations

import sys
import types

import numpy as np
import pandas as pd
import pytest

import corporate_actions as ca
from builders import migrate_universe_crsp_basis as m
from polygon_client import PolygonRateLimitError

TODAY = "2026-10-23"


# ── synthetic market ─────────────────────────────────────────────────────────


def _sessions(start: str, end: str) -> pd.DatetimeIndex:
    return pd.bdate_range(start, end)


def _truth_split_only(idx: pd.DatetimeIndex, seed: int) -> pd.Series:
    """The correct split-only close LEVEL: a smooth walk with no split print."""
    rng = np.random.default_rng(seed)
    steps = rng.normal(0.0, 0.004, len(idx))
    return pd.Series(100.0 * np.exp(np.cumsum(steps)), index=idx)


def _raw_from_truth(truth: pd.Series, splits: list[tuple[str, float]]) -> pd.Series:
    """Undo split-only adjustment: pre-ex rows x ratio (2.0 = 2-for-1)."""
    raw = truth.copy()
    for ex, ratio in splits:
        raw[raw.index < pd.Timestamp(ex)] *= ratio
    return raw


def _ohlcv(close: pd.Series, volume: float = 1_000_000.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": close * 0.999,
            "High": close * 1.004,
            "Low": close * 0.995,
            "Close": close,
            "Volume": np.full(len(close), volume),
        },
        index=close.index,
    )


class _FakeYF(types.ModuleType):
    """yfinance double: returns a split-adjusted ``auto_adjust=False`` frame
    with its ``Stock Splits`` column, MultiIndexed like ``group_by='ticker'``."""

    def __init__(self, ticker: str, frame: pd.DataFrame):
        super().__init__("yfinance")
        self.ticker = ticker
        self.frame = frame
        self.calls: list[dict] = []

    def download(self, **kw):
        self.calls.append(kw)
        f = self.frame.copy()
        f.columns = pd.MultiIndex.from_product([[self.ticker], f.columns])
        return f


class _FakePolygonBars:
    def __init__(self, bars: pd.DataFrame, fail_times: int = 0):
        self.bars = bars
        self.fail_times = fail_times
        self.calls: list[tuple] = []

    def get_daily_bars(self, ticker, start, end, adjusted=True):
        self.calls.append((ticker, start, end, adjusted))
        if self.fail_times:
            self.fail_times -= 1
            raise PolygonRateLimitError("Rate limited (429)")
        return self.bars.copy()


def _market(ticker, *, yf_splits, seed, start="2023-01-02", seam="2024-11-01",
            yf_listed=None):
    """Build (truth, fake yfinance, fake polygon) for one synthetic name.

    ``yf_splits``: every split, ``[(ex, ratio)]``. yfinance's Close is the
    split-only truth (it folds in every split); polygon serves RAW bars from
    ``seam``. ``yf_listed`` overrides which splits yfinance LISTS (default:
    all of them)."""
    idx = _sessions(start, TODAY)
    truth = _truth_split_only(idx, seed)
    raw = _raw_from_truth(truth, yf_splits)

    yf_frame = _ohlcv(truth)
    yf_frame["Volume"] = (_ohlcv(raw)["Volume"] / (raw / truth)).round()
    yf_frame["Adj Close"] = truth * 0.97  # never read
    yf_frame["Dividends"] = 0.0
    yf_frame["Stock Splits"] = 0.0
    for ex, ratio in (yf_listed if yf_listed is not None else yf_splits):
        yf_frame.loc[pd.Timestamp(ex), "Stock Splits"] = ratio

    poly = _ohlcv(raw)[_ohlcv(raw).index >= pd.Timestamp(seam)]
    return truth, raw, _FakeYF(ticker, yf_frame), _FakePolygonBars(poly)


def _split(ticker, ex, split_from, split_to):
    return ca.CorporateAction.from_split(ticker, ex, split_from, split_to, source="polygon")


def _fetch(monkeypatch, ticker, fake_yf, client, **kw):
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    return m.fetch_raw_prices(ticker, client=client, today=TODAY,
                              sleep=lambda s: None, **kw)


def _status_counts(applied):
    out: dict[str, int] = {}
    for r in applied:
        out[r["status"]] = out.get(r["status"], 0) + 1
    return out


# ── the two cases from the build ─────────────────────────────────────────────


def test_expe_split_in_polygon_window_is_applied_exactly_once(monkeypatch):
    """EXPE: a 2-for-1 inside polygon's window, with polygon's record INVERTED
    (``1-for-2 reverse``), as the 2026-10-25/26 build logged. The market corrects
    the orientation on the raw close and the split lands once: the rebuilt Close
    is the split-only truth everywhere, not half of it before the ex-date."""
    ex = "2026-03-16"
    truth, raw, fake_yf, client = _market("EXPE", yf_splits=[(ex, 2.0)], seed=1)

    raw_df = _fetch(monkeypatch, "EXPE", fake_yf, client)
    pd.testing.assert_series_equal(raw_df["Close"], raw, check_names=False, rtol=1e-12)

    new_df, applied = m.reconstruct_basis(
        "EXPE", raw_df, [_split("EXPE", ex, 2, 1)], [],
    )
    assert _status_counts(applied) == {"applied": 1}
    assert applied[0]["orientation_corrected"] is True
    pd.testing.assert_series_equal(new_df["Close"], truth, check_names=False, rtol=1e-12)


def test_hon_reverse_split_in_polygon_window_is_applied_exactly_once(monkeypatch):
    """HON 2026-06-29: 1-for-2 reverse split (polygon ``2:1``, factor 2). Pre-ex
    rows are doubled once, never quadrupled."""
    ex = "2026-06-29"
    truth, raw, fake_yf, client = _market("HON", yf_splits=[(ex, 0.5)], seed=2)

    raw_df = _fetch(monkeypatch, "HON", fake_yf, client)
    new_df, applied = m.reconstruct_basis(
        "HON", raw_df, [_split("HON", ex, 2, 1)], [],
    )
    assert _status_counts(applied) == {"applied": 1}
    pd.testing.assert_series_equal(new_df["Close"], truth, check_names=False, rtol=1e-12)
    pre = new_df.index < pd.Timestamp(ex)
    np.testing.assert_allclose(new_df.loc[pre, "Close"], raw[pre] * 2.0, rtol=1e-12)


# ── the double-application the old fetch allowed ────────────────────────────


def _coincident_move_market(ticker):
    """A split BEFORE polygon's reach (so the history there is yfinance's)
    whose ex-date also carries a real 50% price move (a spin-off-sized drop).
    yfinance's split-adjusted close therefore prints 0.5 at the ex-date, which
    the price-evidence gate reads as the split itself."""
    ex = "2024-03-15"
    truth, raw, fake_yf, client = _market(ticker, yf_splits=[(ex, 2.0)], seed=3)
    drop = truth.index >= pd.Timestamp(ex)
    for frame in (fake_yf.frame,):
        for c in m._PRICE_COLS:
            frame.loc[drop, c] *= 0.5
    truth = truth.copy()
    truth[drop] *= 0.5
    poly = client.bars.copy()
    for c in m._PRICE_COLS:
        poly[c] *= 0.5  # polygon window is entirely after the ex-date
    client.bars = poly
    return ex, truth, fake_yf, client


def test_old_split_adjusted_close_applies_a_confirmed_split_twice(monkeypatch):
    """Characterises the defect: the yfinance ``auto_adjust=False`` frame the old
    fetch returned, fed straight to ``reconstruct_basis``, lets the gate confirm
    the split off the coincident move and halves every pre-ex row a SECOND time.
    The post-restate audit does not catch it (the boundary comes out flat)."""
    ticker = "HON"
    ex, truth, fake_yf, _client = _coincident_move_market(ticker)
    old_raw = fake_yf.frame[["Open", "High", "Low", "Close", "Volume"]].copy()

    new_df, applied = m.reconstruct_basis(ticker, old_raw, [_split(ticker, ex, 1, 2)], [])
    assert _status_counts(applied) == {"applied": 1}
    pre = new_df.index < pd.Timestamp(ex)
    np.testing.assert_allclose(new_df.loc[pre, "Close"], truth[pre] * 0.5, rtol=1e-12)


def test_split_already_in_the_yfinance_basis_is_never_applied_again(monkeypatch):
    """Same market through the new fetch: the split is recorded in-basis and
    skipped, so the rebuilt Close is the truth — the split applied exactly once
    (by yfinance), and the real 50% move survives."""
    ticker = "HON"
    ex, truth, fake_yf, client = _coincident_move_market(ticker)

    raw_df = _fetch(monkeypatch, ticker, fake_yf, client)
    assert raw_df.attrs[m.SPLITS_IN_BASIS_ATTR] == [(pd.Timestamp(ex), 0.5)]

    new_df, applied = m.reconstruct_basis(ticker, raw_df, [_split(ticker, ex, 1, 2)], [])
    assert _status_counts(applied) == {"in_source_basis": 1}
    pd.testing.assert_series_equal(new_df["Close"], truth, check_names=False, rtol=1e-12)


def test_inverted_feed_record_of_an_in_basis_split_is_still_in_basis():
    raw_df = pd.DataFrame({"Close": [1.0]}, index=pd.DatetimeIndex(["2024-03-18"]))
    raw_df.attrs[m.SPLITS_IN_BASIS_ATTR] = [(pd.Timestamp("2024-03-15"), 0.5)]
    direct = _split("X", "2024-03-15", 1, 2)
    inverted = _split("X", "2024-03-14", 2, 1)
    other = _split("X", "2024-03-15", 1, 3)
    far = _split("X", "2024-04-15", 1, 2)
    got = m.split_actions_in_source_basis(raw_df, [direct, inverted, other, far])
    assert got == {direct.action_id, inverted.action_id}


# ── contract and failure modes ───────────────────────────────────────────────


def test_contract_columns_index_and_dtypes(monkeypatch):
    _truth, _raw, fake_yf, client = _market("EXPE", yf_splits=[("2026-03-16", 2.0)], seed=4)
    df = _fetch(monkeypatch, "EXPE", fake_yf, client)
    assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert isinstance(df.index, pd.DatetimeIndex) and df.index.tz is None
    assert df.index.is_monotonic_increasing and df.index.is_unique
    assert all(df[c].dtype == "float64" for c in ("Open", "High", "Low", "Close"))
    assert df["Volume"].dtype == "int64"
    assert df.index[0] == pd.Timestamp("2023-01-02")  # full history, not polygon's 2y
    assert df.attrs[m.POLYGON_RAW_FROM_ATTR] == "2024-11-01"
    assert fake_yf.calls[0]["auto_adjust"] is False and fake_yf.calls[0]["actions"] is True


def test_polygon_is_asked_for_unadjusted_bars_under_its_own_symbol(monkeypatch):
    _truth, _raw, fake_yf, client = _market("BRK-B", yf_splits=[], seed=5)
    _fetch(monkeypatch, "BRK-B", fake_yf, client)
    (symbol, start, end, adjusted), = client.calls
    assert (symbol, adjusted, end) == ("BRK.B", False, TODAY)
    assert start == (pd.Timestamp(TODAY) - pd.Timedelta(days=m.POLYGON_RAW_LOOKBACK_DAYS)).strftime("%Y-%m-%d")


def test_unlisted_yfinance_split_fails_loud_at_the_seam(monkeypatch):
    """yfinance folded a split in but does not list it: un-adjusting cannot
    reach polygon's raw basis, and the join refuses rather than shift every
    pre-seam row by the split factor."""
    _truth, _raw, fake_yf, client = _market(
        "EXPE", yf_splits=[("2026-03-16", 2.0)], seed=6, yf_listed=[],
    )
    with pytest.raises(m.RawPriceFetchError, match="seam"):
        _fetch(monkeypatch, "EXPE", fake_yf, client)


def test_empty_polygon_bars_fail_loud_never_fall_back(monkeypatch):
    _truth, _raw, fake_yf, client = _market("EXPE", yf_splits=[], seed=7)
    client.bars = client.bars.iloc[0:0]
    with pytest.raises(m.RawPriceFetchError, match="no raw bars"):
        _fetch(monkeypatch, "EXPE", fake_yf, client)


def test_rate_limited_raw_bars_are_retried_then_fail_loud(monkeypatch):
    _truth, _raw, fake_yf, client = _market("EXPE", yf_splits=[], seed=8)
    client.fail_times = 1
    df = _fetch(monkeypatch, "EXPE", fake_yf, client)
    assert len(client.calls) == 2 and not df.empty

    client.fail_times = 99
    with pytest.raises(m.RawPriceFetchError, match="after 4 attempt") as exc:
        _fetch(monkeypatch, "EXPE", fake_yf, client)
    assert "apiKey" not in str(exc.value)


def test_a_raw_fetch_failure_stops_the_build(monkeypatch):
    """Like a corporate-action fetch failure: the audit gate needs zero errors,
    so a name that cannot get a raw close stops the build, and the names after
    it are never fetched."""
    import json
    from unittest.mock import MagicMock

    from tests.test_migrate_universe_crsp_basis import _FakePolygon, _patch_orchestration

    idx = _sessions("2026-01-02", "2026-02-27")
    old = {t: pd.Series(50.0, index=idx) for t in ("A", "B", "C")}
    _live, scratch = _patch_orchestration(monkeypatch, old_closes=old)
    s3 = MagicMock()
    monkeypatch.setattr(m.boto3, "client", lambda *a, **k: s3)
    seen: list[str] = []

    def raw_fetch(ticker):
        seen.append(ticker)
        raise m.RawPriceFetchError(f"{ticker}: polygon returned no raw bars")

    with pytest.raises(RuntimeError, match="skipped after the build was stopped"):
        m.migrate_universe_crsp_basis(
            apply=False, raw_fetch=raw_fetch, client=_FakePolygon({}, {}), workers=1,
        )
    assert seen == ["A"]
    audit = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert audit["errors_count"] == 1 and "raw_prices" in audit["errors"][0]["error"]
    assert audit["aborted"] == ["B", "C"]
    scratch.write.assert_not_called()
