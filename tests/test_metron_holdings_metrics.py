"""Metron Holdings-metrics producer artifacts — fundamentals v2 (P/B + P/S), the derived
technicals artifact, and the SP1500-broad valuation-medians benchmark.

These power Metron's Holdings table valuation / fundamentals / technicals columns and its
"by sector → country" median bands. Covers the contract each consumer pins: schema version,
artifact key, field shape, the no-fabrication coverage-gap behavior, and the no-silent-fail
guard on the medians pass.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from collectors import metron_market_data as mmd

HU_KEY = mmd.HOLDINGS_UNIVERSE_KEY

_UNIVERSE = {
    "schema_version": 1, "as_of": "2026-06-26", "source": "metron",
    "holdings": [
        {"yf_symbol": "AAPL", "currency": "USD"},
        {"yf_symbol": "MSFT", "currency": "USD"},
    ],
    "currencies": [],
}


def _puts(s3: MagicMock) -> dict[str, dict]:
    return {c.kwargs["Key"]: json.loads(c.kwargs["Body"].decode()) for c in s3.put_object.call_args_list}


def _body(obj: dict) -> dict:
    b = MagicMock()
    b.read.return_value = json.dumps(obj).encode()
    return {"Body": b}


# ── Fundamentals v2: P/B + P/S are published ─────────────────────────────────

def test_fundamentals_schema_v5_includes_multiples_balance_sheet_eps_and_valuation_inputs():
    assert mmd.FUNDAMENTALS_SCHEMA_VERSION == 5
    for k in ("priceToBook", "priceToSalesTrailing12Months", "totalDebt", "totalCash",
              "ebitda", "freeCashflow", "trailingEps", "forwardEps",
              "bookValue", "revenuePerShare", "enterpriseValue"):
        assert k in mmd.FUNDAMENTALS_INFO_KEYS

    s3 = MagicMock()
    s3.get_object.side_effect = lambda Bucket, Key: _body(_UNIVERSE) if Key == HU_KEY else (_ for _ in ()).throw(Exception("NoSuchKey"))
    src = lambda syms: {s: {"trailingPE": 30.0, "priceToBook": 6.0, "priceToSalesTrailing12Months": 7.5,
                           "totalDebt": 1.1e11, "totalCash": 6.0e10, "ebitda": 1.3e11} for s in syms}

    result = mmd.collect_fundamentals(bucket="b", run_date="2026-06-26", s3_client=s3, fundamentals_source=src)

    assert result["status"] == "ok"
    art = _puts(s3)[f"{mmd.FUNDAMENTALS_PREFIX}latest.json"]
    assert art["schema_version"] == 5
    aapl = art["fundamentals"]["AAPL"]
    assert aapl["priceToBook"] == 6.0 and aapl["priceToSalesTrailing12Months"] == 7.5
    assert aapl["totalDebt"] == 1.1e11 and aapl["totalCash"] == 6.0e10 and aapl["ebitda"] == 1.3e11


# ── Technicals: derived from close_history, no new fetch ──────────────────────

def _technicals_s3(series_by_sym: dict[str, list[list]]) -> MagicMock:
    """S3 mock: the held universe at HU_KEY, a close_history artifact per symbol key, else NoSuchKey."""
    s3 = MagicMock()

    def _get(Bucket, Key):
        if Key == HU_KEY:
            return _body(_UNIVERSE)
        for sym, series in series_by_sym.items():
            if Key == f"{mmd.CLOSE_HISTORY_PREFIX}{sym}.json":
                return _body({"schema_version": 1, "yf_symbol": sym, "currency": "USD", "closes": series})
        raise Exception("NoSuchKey")

    s3.get_object.side_effect = _get
    return s3


def _ramp(n: int, start: float = 100.0, step: float = 0.5) -> list[list]:
    """An up-trending [[date, close], …] series with day-to-day oscillation (both up and
    down days, so Wilder RSI is well-defined), long enough for the 200d MA."""
    return [
        [f"2025-{1 + i // 28:02d}-{1 + i % 28:02d}", round(start + step * i + 1.5 * ((-1) ** i), 4)]
        for i in range(n)
    ]


def test_technicals_artifact_shape_and_keys():
    s3 = _technicals_s3({"AAPL": _ramp(260), "MSFT": _ramp(260, start=50.0)})

    result = mmd.collect_technicals(bucket="b", run_date="2026-06-26", s3_client=s3)

    assert result["status"] == "ok" and result["technicals"] == 2
    art = _puts(s3)[f"{mmd.TECHNICALS_PREFIX}latest.json"]
    assert art["schema_version"] == mmd.TECHNICALS_SCHEMA_VERSION
    row = art["technicals"]["AAPL"]
    for field in ("rsi_14", "macd_hist", "ma_50", "ma_200", "pct_to_ma_50",
                  "pct_to_ma_200", "high_52w", "low_52w", "pct_in_52w_range",
                  "pct_from_52wk_high", "mom_20d", "mom_60d"):
        assert field in row
    # Up-trending series: RSI elevated, last > both MAs, near the top of its 52w range.
    assert row["rsi_14"] > 55
    assert row["pct_to_ma_50"] > 0 and row["pct_to_ma_200"] > 0
    assert row["pct_in_52w_range"] > 0.8


def test_technicals_omits_symbol_with_too_short_history():
    # AAPL has a usable series; MSFT's is below the minimum-observation gate → omitted (no zeros).
    s3 = _technicals_s3({"AAPL": _ramp(260), "MSFT": _ramp(10)})

    result = mmd.collect_technicals(bucket="b", run_date="2026-06-26", s3_client=s3)

    art = _puts(s3)[f"{mmd.TECHNICALS_PREFIX}latest.json"]
    assert set(art["technicals"]) == {"AAPL"}
    assert result["technicals"] == 1


def test_technicals_short_series_nulls_deep_windows():
    # 120 obs: RSI/50d-MA computable, 200d-MA null (never fabricated on too little data).
    out = mmd._compute_technicals(_ramp(120))
    assert out["ma_50"] is not None and out["rsi_14"] is not None
    assert out["ma_200"] is None and out["pct_to_ma_200"] is None
    assert out["pct_from_52wk_high"] is not None


# ── Security performance: derived from close_history + SPY benchmark ───────────

def test_security_performance_artifact_shape_and_vs_spy_1y():
    spy = _ramp(300, start=400.0, step=0.3)
    aapl = _ramp(300, start=100.0, step=0.6)
    s3 = _technicals_s3({"AAPL": aapl, "SPY": spy})

    result = mmd.collect_security_performance(bucket="b", run_date="2026-06-26", s3_client=s3)

    assert result["status"] == "ok" and result["performance"] == 1
    art = _puts(s3)[f"{mmd.SECURITY_PERFORMANCE_PREFIX}latest.json"]
    assert art["schema_version"] == mmd.SECURITY_PERFORMANCE_SCHEMA_VERSION
    row = art["performance"]["AAPL"]
    for field in (
        "period_returns", "ytd_pct", "ltm_pct", "volatility", "sharpe", "sortino",
        "max_drawdown", "beta_vs_spy", "vs_spy_window", "vs_spy_1y", "n_bars", "history_from",
    ):
        assert field in row
    assert isinstance(row["period_returns"], dict)
    assert row["n_bars"] >= 260


def test_security_performance_omits_symbol_with_no_history():
    s3 = _technicals_s3({"AAPL": _ramp(300), "MSFT": []})

    result = mmd.collect_security_performance(bucket="b", run_date="2026-06-26", s3_client=s3)

    art = _puts(s3)[f"{mmd.SECURITY_PERFORMANCE_PREFIX}latest.json"]
    assert set(art["performance"]) == {"AAPL"}
    assert result["performance"] == 1


def test_price_derived_universe_unions_sp1500_and_metron(monkeypatch):
    """SP1500 ∪ held/watchlist — overlap deduped, metron-only foreign names kept."""
    s3 = MagicMock()
    s3.get_object.side_effect = lambda Bucket, Key: _body(_UNIVERSE) if Key == mmd.HOLDINGS_UNIVERSE_KEY else (
        _body({"holdings": [{"yf_symbol": "RMS.PA", "currency": "EUR"}]})
        if Key == mmd.WATCHLIST_UNIVERSE_KEY else (_ for _ in ()).throw(Exception("NoSuchKey"))
    )
    monkeypatch.setattr(mmd, "_load_sp1500_symbols", lambda bucket: {"AAPL", "MSFT", "NVDA"})
    holdings, currencies = mmd.load_price_derived_universe("b", s3)
    yf = {h["yf_symbol"] for h in holdings}
    assert yf == {"AAPL", "MSFT", "NVDA", "RMS.PA"}
    assert currencies == ["EUR"]
    assert next(h for h in holdings if h["yf_symbol"] == "AAPL")["currency"] == "USD"
    assert next(h for h in holdings if h["yf_symbol"] == "RMS.PA")["currency"] == "EUR"


# ── Valuation medians: SP1500-broad sector & country benchmark ────────────────

def test_valuation_medians_artifact_values():
    universe = ["AAPL", "MSFT", "NVDA", "JPM", "BAC"]
    rows = {
        "AAPL": {"trailingPE": 30.0, "priceToBook": 6.0, "sector": "Technology", "country": "United States"},
        "MSFT": {"trailingPE": 34.0, "priceToBook": 10.0, "sector": "Technology", "country": "United States"},
        "NVDA": {"trailingPE": 50.0, "priceToBook": 20.0, "sector": "Technology", "country": "United States"},
        "JPM": {"trailingPE": 12.0, "priceToBook": 1.8, "sector": "Financial Services", "country": "United States"},
        "BAC": {"trailingPE": -5.0, "priceToBook": 1.2, "sector": "Financial Services", "country": "United States"},
    }
    s3 = MagicMock()
    mmd.collect_valuation_medians(
        bucket="b", run_date="2026-06-26", s3_client=s3,
        universe_source=lambda: universe, valuation_source=lambda syms: rows,
    )
    art = _puts(s3)[f"{mmd.VALUATION_MEDIANS_PREFIX}latest.json"]
    assert art["schema_version"] == mmd.VALUATION_MEDIANS_SCHEMA_VERSION
    tech = art["by_sector"]["Technology"]
    assert tech["n"] == 3 and tech["trailing_pe"] == 34.0 and tech["price_to_book"] == 10.0
    fin = art["by_sector"]["Financial Services"]
    # BAC's negative P/E is dropped as meaningless → median of the single valid P/E (JPM).
    assert fin["trailing_pe"] == 12.0 and fin["n"] == 2
    assert art["by_country"]["United States"]["n"] == 5


def test_valuation_medians_empty_pass_is_error():
    result = mmd.collect_valuation_medians(
        bucket="b", run_date="2026-06-26", s3_client=MagicMock(),
        universe_source=lambda: ["AAPL"], valuation_source=lambda syms: {},
    )
    assert result["status"] == "error"
