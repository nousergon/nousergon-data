"""
Wave-4 PR1b — features.compute._load_price_source.

The riskier consumer migration: ArcticDB (universe lib + macro lib) is the
primary price+macro source, slim cache is the fallback, and a parity
ParityReport is emitted every run while both exist (grep
``WAVE4_PARITY_METRIC compute``). Covers the composed-read, fallback, and
observation paths.
"""

from __future__ import annotations

import pandas as pd
import pytest

from features import compute


def _frame(n=10, start=100.0):
    idx = pd.date_range("2026-04-01", periods=n, freq="D")
    return pd.DataFrame(
        {"Close": [float(start + i) for i in range(n)], "Volume": [1] * n},
        index=idx,
    )


class _FakeMacroLib:
    def __init__(self, symbols):
        self._symbols = symbols

    def list_symbols(self):
        return self._symbols


def _stub_arctic(monkeypatch, *, universe, macro_frames, macro_symbols):
    monkeypatch.setattr(compute, "load_universe_ohlcv", lambda bucket: dict(universe))
    monkeypatch.setattr(
        compute, "open_macro_lib", lambda bucket: _FakeMacroLib(macro_symbols)
    )
    monkeypatch.setattr(
        compute, "load_macro_series", lambda bucket, syms: dict(macro_frames)
    )


def test_composes_universe_and_macro_when_arcticdb_available(monkeypatch):
    """Equities+SPY from universe lib UNIONED with VIX../XL* from macro lib."""
    universe = {"AAPL": _frame(), "SPY": _frame(start=500)}
    macro_frames = {"VIX": _frame(start=18), "XLK": _frame(start=200)}
    _stub_arctic(
        monkeypatch, universe=universe, macro_frames=macro_frames,
        macro_symbols=["VIX", "XLK", "features"],  # 'features' must be ignored
    )
    monkeypatch.setattr(compute, "_load_slim_cache", lambda s3, b: {})

    out = compute._load_price_source(s3=None, bucket="b")
    assert set(out) == {"AAPL", "SPY", "VIX", "XLK"}


def test_falls_back_to_slim_when_arcticdb_fails(monkeypatch, caplog):
    def _boom(bucket):
        raise RuntimeError("ArcticDB down")

    monkeypatch.setattr(compute, "load_universe_ohlcv", _boom)
    slim = {"AAPL": _frame(), "VIX": _frame(start=18)}
    monkeypatch.setattr(compute, "_load_slim_cache", lambda s3, b: slim)

    with caplog.at_level("WARNING"):
        out = compute._load_price_source(s3=None, bucket="b")
    assert set(out) == {"AAPL", "VIX"}
    assert any("falling back to slim cache" in r.message for r in caplog.records)


def test_parity_metric_emitted_when_both_present(monkeypatch, caplog):
    universe = {"AAPL": _frame()}
    macro_frames = {"VIX": _frame(start=18)}
    _stub_arctic(
        monkeypatch, universe=universe, macro_frames=macro_frames,
        macro_symbols=["VIX"],
    )
    # slim carries the same data + an extra symbol the universe lib lacks;
    # require_ticker_match=False -> set asymmetry is reported, not fatal.
    slim = {"AAPL": _frame(), "VIX": _frame(start=18), "OLDSYM": _frame()}
    monkeypatch.setattr(compute, "_load_slim_cache", lambda s3, b: slim)

    with caplog.at_level("INFO"):
        compute._load_price_source(s3=None, bucket="b")

    lines = [
        r.message for r in caplog.records
        if "WAVE4_PARITY_METRIC compute" in r.message
    ]
    assert len(lines) == 1
    import json

    payload = json.loads(lines[0].split("WAVE4_PARITY_METRIC compute ", 1)[1])
    assert payload["max_abs_value_delta"] == 0.0          # overlap identical
    assert payload["passed"] is True                      # value fidelity holds
    assert "OLDSYM" in payload["only_in_a"]               # asymmetry visible


def test_returns_none_when_both_sources_fail(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("down")

    monkeypatch.setattr(compute, "load_universe_ohlcv", _boom)
    monkeypatch.setattr(compute, "_load_slim_cache", _boom)
    assert compute._load_price_source(s3=None, bucket="b") is None


def test_load_prices_and_macro_empty_when_no_source(monkeypatch):
    monkeypatch.setattr(compute, "_load_price_source", lambda s3, b: None)
    prices, macro = compute._load_prices_and_macro(None, "b", "2026-04-10")
    assert prices == {} and macro == {}
