"""Tests for the predictor-options write-both mirror in ``collectors/alternative.py``.

Background (yfinance-centralization plan PR 4a):
The canonical alternative-data layout is one JSON per ticker at
``market_data/weekly/{date}/alternative/{TICKER}.json`` (options nested
under ``options_flow``). alpha-engine-predictor's
``data/options_fetcher.py::load_historical_options`` reads a DIFFERENT,
legacy-shaped key — a single flat file at ``archive/options/{date}.json``
mapping ``{ticker: {put_call_ratio, iv_rank, atm_iv}}``.

Per the S3-contract rule (additive only; write-both ≥1 week before any
consumer relies on the new key), the collector now ADDITIVELY also writes
the predictor-expected key/shape alongside the canonical per-ticker files.
These tests lock that contract: both keys land, the mirror carries the
same option payloads as the per-ticker files, units are preserved, and
the canonical files are untouched.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from collectors import alternative


def _alt_payload(ticker: str, *, pc, iv, with_options: bool = True) -> dict:
    """A `_fetch_all_alternative`-shaped return; options_flow populated
    unless ``with_options`` is False (provider dark)."""
    return {
        "ticker": ticker,
        "fetched_at": "2026-05-16T20:00:00+00:00",
        "analyst_consensus": {
            "rating": "Buy", "target_price": 200.0,
            "num_analysts": 25, "earnings_surprises": [{"date": "Q1"}],
        },
        "eps_revision": {"current_estimate": 6.5, "revision_4w": 1.2, "streak": 2},
        "options_flow": (
            {"put_call_ratio": pc, "iv_rank": iv, "expected_move_pct": 4.5}
            if with_options
            else {"put_call_ratio": None, "iv_rank": None, "expected_move_pct": None}
        ),
        "insider_activity": {
            "cluster_buying": True, "net_shares_30d": 5000,
            "transactions": [{"insider": "CEO", "shares": 5000}],
        },
        "institutional": {
            "accumulation": True, "funds_increasing": 7, "funds_decreasing": 2,
        },
        "news": {"articles": [{"headline": "X"}], "sec_filings_8k": [{"title": "8-K"}]},
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


def _puts_by_key(s3) -> dict[str, dict]:
    """Map every put_object Key → parsed JSON body."""
    out: dict[str, dict] = {}
    for c in s3.put_object.call_args_list:
        out[c.kwargs["Key"]] = json.loads(c.kwargs["Body"])
    return out


# ── A. _build_predictor_options_mirror (pure projection) ─────────────────────


def test_mirror_projects_options_flow_to_flat_predictor_shape():
    per_ticker = {
        "AAPL": _alt_payload("AAPL", pc=0.72, iv=35),
        "MSFT": _alt_payload("MSFT", pc=1.10, iv=60),
    }
    mirror = alternative._build_predictor_options_mirror(per_ticker)

    assert set(mirror) == {"AAPL", "MSFT"}
    # Flat shape exactly matches what load_historical_options reads.
    assert set(mirror["AAPL"]) == {"put_call_ratio", "iv_rank", "atm_iv"}
    # put_call_ratio carried RAW (predictor log-transforms on read).
    assert mirror["AAPL"]["put_call_ratio"] == 0.72
    # iv_rank carried on the 0-100 scale (predictor ÷100 on read).
    assert mirror["MSFT"]["iv_rank"] == 60
    # atm_iv not surfaced by _fetch_options → 0.0 (predictor defaults same).
    assert mirror["AAPL"]["atm_iv"] == 0.0


def test_mirror_excludes_tickers_with_no_options_data():
    per_ticker = {
        "AAPL": _alt_payload("AAPL", pc=0.72, iv=35),
        "NVDA": _alt_payload("NVDA", pc=None, iv=None, with_options=False),
    }
    mirror = alternative._build_predictor_options_mirror(per_ticker)
    assert "AAPL" in mirror
    assert "NVDA" not in mirror, (
        "tickers whose options_flow went dark must be omitted — "
        "predictor neutral-fills missing tickers on its side"
    )


def test_mirror_carries_real_atm_iv_if_ever_surfaced():
    """Forward-compatible: if a future PR stores atm_iv in options_flow,
    the mirror must carry it instead of the 0.0 default."""
    per_ticker = {
        "AAPL": {
            **_alt_payload("AAPL", pc=0.8, iv=40),
            "options_flow": {
                "put_call_ratio": 0.8, "iv_rank": 40,
                "expected_move_pct": 3.1, "atm_iv": 0.27,
            },
        }
    }
    mirror = alternative._build_predictor_options_mirror(per_ticker)
    assert mirror["AAPL"]["atm_iv"] == 0.27


# ── B. collect() writes BOTH keys ────────────────────────────────────────────


def test_collect_writes_canonical_and_predictor_mirror_keys(monkeypatch):
    payloads = [
        _alt_payload("AAPL", pc=0.72, iv=35),
        _alt_payload("MSFT", pc=1.10, iv=60),
    ]
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix="market_data/",
        run_date="2026-05-16",
        tickers=["AAPL", "MSFT"],
    )
    assert result["status"] == "ok"

    by_key = _puts_by_key(s3)

    # Canonical per-ticker files (research consumers) — untouched/present.
    assert "market_data/weekly/2026-05-16/alternative/AAPL.json" in by_key
    assert "market_data/weekly/2026-05-16/alternative/MSFT.json" in by_key
    # Canonical manifest still written.
    assert "market_data/weekly/2026-05-16/alternative/manifest.json" in by_key
    # NEW: predictor-expected flat single-file mirror at the legacy key
    # (bucket-root, no market_data/ prefix — matches predictor's hardcoded
    # archive/options/{date}.json read).
    assert "archive/options/2026-05-16.json" in by_key


def test_mirror_payload_matches_canonical_options_flow(monkeypatch):
    """The mirror must carry the SAME option values as the canonical
    per-ticker files — no divergence between the two written keys."""
    payloads = [
        _alt_payload("AAPL", pc=0.72, iv=35),
        _alt_payload("MSFT", pc=1.10, iv=60),
    ]
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix="market_data/",
        run_date="2026-05-16",
        tickers=["AAPL", "MSFT"],
    )
    by_key = _puts_by_key(s3)

    mirror = by_key["archive/options/2026-05-16.json"]
    for tkr in ("AAPL", "MSFT"):
        canonical = by_key[
            f"market_data/weekly/2026-05-16/alternative/{tkr}.json"
        ]["options_flow"]
        assert mirror[tkr]["put_call_ratio"] == canonical["put_call_ratio"]
        assert mirror[tkr]["iv_rank"] == canonical["iv_rank"]


def test_mirror_written_even_on_gate_breach(monkeypatch):
    """Like the manifest, the mirror lands even when the per-source gate
    breaches — the ≥1wk soak that gates predictor PR 4b should proceed on
    every run that produced options data, not only clean ones."""
    # analyst_consensus dark for all 10 → gate breach, but options_flow
    # is still populated.
    payloads = []
    for i in range(10):
        p = _alt_payload(f"TKR{i}", pc=0.9, iv=20)
        p["analyst_consensus"] = {
            "rating": None, "target_price": None,
            "num_analysts": None, "earnings_surprises": [],
        }
        payloads.append(p)
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix="market_data/",
        run_date="2026-05-16",
        tickers=[f"TKR{i}" for i in range(10)],
    )
    assert result["status"] == "error"  # gate breached
    by_key = _puts_by_key(s3)
    assert "archive/options/2026-05-16.json" in by_key
    mirror = by_key["archive/options/2026-05-16.json"]
    assert len(mirror) == 10
    assert mirror["TKR0"]["put_call_ratio"] == 0.9


def test_dry_run_writes_no_mirror(monkeypatch):
    """Dry-run short-circuits before any fetch/write — no mirror key."""
    s3 = MagicMock()
    monkeypatch.setattr(
        alternative, "boto3", MagicMock(client=lambda *a, **k: s3),
    )
    monkeypatch.setattr(
        alternative, "_load_promoted_tickers", lambda *a, **k: ["AAPL"],
    )
    result = alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix="market_data/",
        run_date="2026-05-16",
        tickers=["AAPL"],
        dry_run=True,
    )
    assert result["status"] == "ok_dry_run"
    s3.put_object.assert_not_called()
