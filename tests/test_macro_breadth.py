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


def test_breadth_key_omitted_when_no_price_data_and_slim_cache_empty(monkeypatch):
    """The critical regression: breadth must NEVER be serialized as null."""
    _stub_fetchers(monkeypatch)

    # Both sources empty (no ArcticDB symbols, no slim parquets in S3)
    monkeypatch.setattr(macro, "load_universe_ohlcv", lambda *a, **k: {})
    monkeypatch.setattr(macro, "load_slim_cache", lambda s3, bucket: {})

    written = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            written.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.collect(
        bucket="test-bucket",
        run_date="2026-04-11",
    )

    assert result["status"] == "ok"
    import json
    body = json.loads(written["Body"])
    # breadth key must be ABSENT — not present with a null value.
    assert "breadth" not in body


def test_breadth_key_omitted_when_slim_cache_load_raises(monkeypatch):
    _stub_fetchers(monkeypatch)

    def _boom(s3, bucket):
        raise RuntimeError("S3 unreachable")

    def _arctic_boom(*a, **k):
        raise RuntimeError("ArcticDB unreachable")

    monkeypatch.setattr(macro, "load_universe_ohlcv", _arctic_boom)
    monkeypatch.setattr(macro, "load_slim_cache", _boom)

    written = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            written.update(kwargs)

    monkeypatch.setattr(macro.boto3, "client", lambda service: _FakeS3())

    result = macro.collect(bucket="test-bucket", run_date="2026-04-11")
    assert result["status"] == "ok"
    import json
    body = json.loads(written["Body"])
    assert "breadth" not in body


# ── Wave-4 migration: ArcticDB primary / slim fallback / parity emit ─────────


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


def test_breadth_uses_arcticdb_when_available(monkeypatch):
    """ArcticDB is primary — breadth computed from it even if slim is empty."""
    _stub_fetchers(monkeypatch)
    monkeypatch.setattr(macro, "load_universe_ohlcv", lambda *a, **k: _universe())
    monkeypatch.setattr(macro, "load_slim_cache", lambda s3, bucket: {})

    body = _collect_body(monkeypatch)
    assert isinstance(body["breadth"], dict)
    assert body["breadth"]["n_stocks"] == 2


def test_breadth_falls_back_to_slim_when_arcticdb_unavailable(monkeypatch, caplog):
    """ArcticDB read fails -> slim fallback keeps breadth working."""
    _stub_fetchers(monkeypatch)

    def _arctic_boom(*a, **k):
        raise RuntimeError("ArcticDB unreachable")

    monkeypatch.setattr(macro, "load_universe_ohlcv", _arctic_boom)
    monkeypatch.setattr(macro, "load_slim_cache", lambda s3, bucket: _universe())

    with caplog.at_level("WARNING"):
        body = _collect_body(monkeypatch)
    assert isinstance(body["breadth"], dict)
    assert any("falling back to slim cache" in r.message for r in caplog.records)


def test_parity_metric_emitted_when_both_sources_present(monkeypatch, caplog):
    """SOTA observation: dual-read emits a JSON ParityReport every run."""
    _stub_fetchers(monkeypatch)
    monkeypatch.setattr(macro, "load_universe_ohlcv", lambda *a, **k: _universe())
    monkeypatch.setattr(macro, "load_slim_cache", lambda s3, bucket: _universe())

    with caplog.at_level("INFO"):
        body = _collect_body(monkeypatch)

    assert isinstance(body["breadth"], dict)
    metric_lines = [
        r.message for r in caplog.records
        if "WAVE4_PARITY_METRIC breadth" in r.message
    ]
    assert len(metric_lines) == 1
    import json
    payload = json.loads(metric_lines[0].split("WAVE4_PARITY_METRIC breadth ", 1)[1])
    assert payload["passed"] is True  # identical fixtures -> parity holds
    assert payload["max_abs_value_delta"] == 0.0
