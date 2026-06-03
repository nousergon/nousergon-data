"""
Tests for collectors.macro breadth handling.

Guards against the regression fixed in this PR: previously the collector
would write ``breadth: null`` into macro.json whenever price_data wasn't
supplied, which later crashed alpha-engine-research macro_agent at
``breadth.get("pct_above_50d_ma")`` (NoneType has no .get).

The contract now is:
- If we have price_data (either passed in or loaded from slim cache), write
  a computed breadth dict.
- If we have no price data, OMIT the "breadth" key entirely — never write
  null — so downstream consumers fall through to their own computation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from collectors import macro


def _synthetic_price_frame(n: int = 220, start: float = 100.0) -> pd.DataFrame:
    idx = pd.bdate_range(end=pd.Timestamp("2026-04-10"), periods=n)
    close = np.linspace(start, start * 1.2, n)
    return pd.DataFrame({"Close": close}, index=idx)


def _stub_fetchers(monkeypatch):
    """Stub out the external FRED/yfinance calls so unit tests stay offline."""
    monkeypatch.setattr(
        macro, "_fetch_fred", lambda: {"fed_funds_rate": 3.5, "vix": 18.0}
    )
    monkeypatch.setattr(
        macro,
        "_fetch_market_prices",
        lambda: {"sp500_close": 650.0, "sp500_30d_return": 2.0},
    )
    # Macro history is a secondary write off collect(); stub it empty so these
    # breadth tests stay offline and macro.json remains the single captured PUT.
    _HISTORY_COLS = ["date", "series_id", "label", "value", "units", "frequency"]
    monkeypatch.setattr(macro, "build_macro_history", lambda *a, **k: pd.DataFrame(columns=_HISTORY_COLS))


def test_breadth_computed_when_price_data_supplied(monkeypatch):
    _stub_fetchers(monkeypatch)
    price_data = {
        "AAPL": _synthetic_price_frame(),
        "MSFT": _synthetic_price_frame(start=300.0),
        "GOOG": _synthetic_price_frame(start=140.0),
    }

    # Intercept S3 writes
    written = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            written.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.collect(
        bucket="test-bucket",
        price_data=price_data,
        run_date="2026-04-11",
    )

    assert result["status"] == "ok"
    import json
    body = json.loads(written["Body"])
    assert "breadth" in body
    assert isinstance(body["breadth"], dict)
    assert "pct_above_50d_ma" in body["breadth"]
    assert body["breadth"] is not None


# ── Wave-4 terminal state: breadth reads ArcticDB only (slim deleted) ────────


def _universe(n=220):
    return {
        "AAA": _synthetic_price_frame(n, 100.0),
        "BBB": _synthetic_price_frame(n, 250.0),
    }


def _collect_body(monkeypatch):
    written = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            written.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())
    result = macro.collect(bucket="test-bucket", run_date="2026-04-11")
    assert result["status"] == "ok"
    import json
    return json.loads(written["Body"])


def test_breadth_uses_arcticdb(monkeypatch):
    """ArcticDB universe lib is the sole price source for breadth."""
    _stub_fetchers(monkeypatch)
    monkeypatch.setattr(macro, "load_universe_ohlcv", lambda *a, **k: _universe())

    body = _collect_body(monkeypatch)
    assert isinstance(body["breadth"], dict)
    assert body["breadth"]["n_stocks"] == 2


def test_breadth_key_omitted_when_arcticdb_empty(monkeypatch):
    """The critical regression: breadth must NEVER be serialized as null —
    an empty ArcticDB read omits the key (Research has its own fallback)."""
    _stub_fetchers(monkeypatch)
    monkeypatch.setattr(macro, "load_universe_ohlcv", lambda *a, **k: {})

    body = _collect_body(monkeypatch)
    assert "breadth" not in body  # absent, not null


def test_breadth_key_omitted_when_arcticdb_raises(monkeypatch):
    """ArcticDB unavailable -> breadth key omitted (no slim fallback post
    Wave-4; matches pre-Wave-4 single-source-unavailable behaviour)."""
    _stub_fetchers(monkeypatch)

    def _arctic_boom(*a, **k):
        raise RuntimeError("ArcticDB unreachable")

    monkeypatch.setattr(macro, "load_universe_ohlcv", _arctic_boom)

    body = _collect_body(monkeypatch)
    assert "breadth" not in body
