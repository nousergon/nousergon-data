"""Tests for the write-time value-range gate in ``collectors/alternative.py``.

Background (ROADMAP L1243, extends alpha-engine-data #215):
``alternative.collect`` writes one feature-source JSON per ticker to S3
that bypasses ``builders/daily_append.py``'s ``validate_today_row`` gate
entirely. The pre-existing per-source ok_ratio gate only checks data
*presence*; a corrupt-but-present numeric sub-field (NaN put/call ratio
from a 0/0 open-interest divide, negative analyst price target from a
malformed yfinance ``.info``, negative fund counts) flowed straight into
the research qual sub-score with no pipeline failure.

This gate runs ``validators.price_validator.validate_feature_record``
over each spec'd sub-section of the assembled per-ticker payload before
the S3 write. A block-severity anomaly (NaN/inf or negative-where-
impossible) refuses the whole ticker write — accounted exactly like a
fetch failure so the existing failed/errors + ok_ratio machinery
surfaces it. A gross outlier warns. Mirrors #215's definitely-bad-blocks
/ rare-but-possible-warns split + the env-tunable block-set loader.

These tests lock the gate's contract so a future "simplify away the
value-range gate" revert reintroduces the silent-corruption surface.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative


def _populated_alt_payload(ticker: str) -> dict:
    """A `_fetch_all_alternative` return where every source has real,
    in-range data."""
    return {
        "ticker": ticker,
        "fetched_at": "2026-05-18T20:00:00+00:00",
        "analyst_consensus": {
            "rating": "Buy",
            "target_price": 200.0,
            "num_analysts": 25,
            "earnings_surprises": [{"date": "2026-Q1", "surprise_pct": 5.2}],
        },
        "eps_revision": {"current_estimate": 6.50, "revision_4w": 1.2, "streak": 2},
        "options_flow": {"put_call_ratio": 0.7, "iv_rank": 35, "expected_move_pct": 4.5},
        "insider_activity": {
            "cluster_buying": True, "net_shares_30d": 50000,
            "transactions": [{"insider": "CEO", "shares": 50000}],
        },
        "institutional": {
            "accumulation": True, "funds_increasing": 7, "funds_decreasing": 2,
        },
        "news": {
            "articles": [{"headline": "X", "source": "Yahoo"}],
            "sec_filings_8k": [{"title": "Item 2.02", "date": "2026-05-15"}],
        },
    }


def _patch_collect(monkeypatch, *, fetch_returns: list[dict]):
    s3 = MagicMock()
    monkeypatch.setattr(alternative, "boto3", MagicMock(client=lambda *a, **k: s3))
    monkeypatch.setattr(
        alternative, "_load_promoted_tickers",
        lambda *a, **k: [d["ticker"] for d in fetch_returns],
    )
    fetch_iter = iter(fetch_returns)
    monkeypatch.setattr(
        alternative, "_fetch_all_alternative",
        lambda ticker, run_date, bucket: next(fetch_iter),
    )
    return s3


def _collect(tickers):
    return alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-05-18",
        tickers=tickers,
    )


# ── Block path ───────────────────────────────────────────────────────────────


def test_nan_put_call_ratio_blocks_ticker(monkeypatch):
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    payloads[0]["options_flow"]["put_call_ratio"] = float("nan")
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 1
    assert result["quality_anomaly_counts"].get("nan_or_inf") == 1
    # Blocked ticker is NOT written to S3 (9 ticker writes + mirror +
    # manifest); accounted as a failure.
    assert result["tickers_failed"] == 1
    assert result["tickers_processed"] == 9


def test_negative_target_price_blocks_ticker(monkeypatch):
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    payloads[3]["analyst_consensus"]["target_price"] = -12.0
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 1
    assert result["quality_anomaly_counts"].get("negative_where_nonneg") == 1


def test_negative_fund_count_blocks_ticker(monkeypatch):
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    payloads[1]["institutional"]["funds_increasing"] = -3
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 1
    assert result["quality_anomaly_counts"].get("negative_where_nonneg") == 1


def test_blocked_ticker_not_written_to_s3(monkeypatch):
    payloads = [_populated_alt_payload("GOOD"), _populated_alt_payload("BAD")]
    payloads[1]["options_flow"]["iv_rank"] = float("inf")
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    _collect(["GOOD", "BAD"])

    written_keys = [
        c.kwargs.get("Key", "")
        for c in s3.put_object.call_args_list
    ]
    assert any("GOOD.json" in k for k in written_keys)
    assert not any("BAD.json" in k for k in written_keys)


# ── Warn path ────────────────────────────────────────────────────────────────


def test_gross_outlier_warns_not_blocks(monkeypatch):
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    # iv_rank hi band is 100; 9999 is a defeated upstream clamp.
    payloads[0]["options_flow"]["iv_rank"] = 9999.0
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 0
    assert result["tickers_quality_warned"] == 1
    assert result["quality_anomaly_counts"].get("gross_outlier") == 1
    assert result["tickers_processed"] == 10  # still written


# ── Clean path + return contract ─────────────────────────────────────────────


def test_clean_run_no_quality_anomalies(monkeypatch):
    payloads = [_populated_alt_payload(f"T{i}") for i in range(5)]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(5)])

    assert result["tickers_quality_blocked"] == 0
    assert result["tickers_quality_warned"] == 0
    assert result["quality_anomaly_counts"] == {}


def test_quality_fields_in_manifest_and_return(monkeypatch):
    payloads = [_populated_alt_payload("AAPL")]
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect(["AAPL"])

    for k in (
        "tickers_quality_blocked", "tickers_quality_warned",
        "quality_anomaly_counts", "quality_block_anomaly_types",
    ):
        assert k in result

    import json
    manifest_call = next(
        c for c in s3.put_object.call_args_list
        if "manifest.json" in c.kwargs.get("Key", "")
    )
    manifest = json.loads(manifest_call.kwargs["Body"])
    assert "tickers_quality_blocked" in manifest
    assert "quality_anomaly_counts" in manifest


# ── Env-tunable block set ────────────────────────────────────────────────────


def test_malformed_block_env_raises(monkeypatch):
    monkeypatch.setenv("ALT_BLOCK_ANOMALY_TYPES", "not-json")
    payloads = [_populated_alt_payload("AAPL")]
    _patch_collect(monkeypatch, fetch_returns=payloads)
    with pytest.raises(RuntimeError, match="not valid JSON"):
        _collect(["AAPL"])


def test_unknown_block_type_in_env_raises(monkeypatch):
    monkeypatch.setenv("ALT_BLOCK_ANOMALY_TYPES", '["made_up"]')
    payloads = [_populated_alt_payload("AAPL")]
    _patch_collect(monkeypatch, fetch_returns=payloads)
    with pytest.raises(RuntimeError, match="unknown anomaly types"):
        _collect(["AAPL"])


def test_env_can_promote_gross_outlier_to_block(monkeypatch):
    monkeypatch.setenv(
        "ALT_BLOCK_ANOMALY_TYPES",
        '["nan_or_inf", "negative_where_nonneg", "gross_outlier"]',
    )
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    payloads[0]["options_flow"]["iv_rank"] = 9999.0
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 1
    assert result["quality_anomaly_counts"].get("gross_outlier") == 1


def test_env_empty_list_is_pure_observation_mode(monkeypatch):
    # "[]" → nothing blocks; even a NaN only warns (observability).
    monkeypatch.setenv("ALT_BLOCK_ANOMALY_TYPES", "[]")
    payloads = [_populated_alt_payload(f"T{i}") for i in range(10)]
    payloads[0]["options_flow"]["put_call_ratio"] = float("nan")
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = _collect([f"T{i}" for i in range(10)])

    assert result["tickers_quality_blocked"] == 0
    assert result["tickers_quality_warned"] == 1
    assert result["quality_anomaly_counts"].get("nan_or_inf") == 1
