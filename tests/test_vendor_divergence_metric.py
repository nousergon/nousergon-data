"""Tests for the daily yfinance-vs-polygon close divergence MetricRecord
(alpha-engine-config-I10783, data-collector plan §2 row 5).

``collectors.cross_source_observer.compute_vendor_divergence`` / the
underlying ``write_vendor_divergence_metric`` writer implement:

  - share of symbols with |close_yf - close_poly| / close_poly > 50 bps
  - bound <= 1% of the universe, each breaching symbol named
  - champion-challenger-policy §7.2: an unmeasurable comparison fails LOUD
    with a reason, never as a silent empty success.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from collectors.cross_source_observer import (
    VENDOR_DIVERGENCE_BOUND,
    VENDOR_DIVERGENCE_METRIC_PREFIX,
    compute_vendor_divergence,
    write_vendor_divergence_metric,
)


def _prior(ticker, close, source="yfinance"):
    return {"ticker": ticker, "Close": close, "source": source}


def test_no_comparable_pairs_is_unmeasurable_not_empty_success():
    """champion-challenger-policy §7.2: n_dates == 0 is a defect, not a
    result — must be reported, never rendered as a clean 'ok'."""
    record = compute_vendor_divergence({}, {}, "2026-09-14")
    assert record["status"] == "unmeasurable"
    assert record["reason"]
    assert record["n"] == 0
    assert record["value"] is None


def test_agreeing_closes_are_ok_and_none_breach():
    prior_rows = {"AAPL": _prior("AAPL", 100.00)}
    new_closes = {"AAPL": 100.01}  # 1 bp apart
    record = compute_vendor_divergence(new_closes, prior_rows, "2026-09-14")
    assert record["status"] == "ok"
    assert record["n"] == 1
    assert record["value"] == 0.0
    assert record["breaching_symbols"] == []


def test_breaching_symbol_is_named_with_both_closes_and_diff_bps():
    prior_rows = {"AAPL": _prior("AAPL", 100.00)}
    new_closes = {"AAPL": 100.60}  # 60 bps apart, > 50 bps threshold
    record = compute_vendor_divergence(new_closes, prior_rows, "2026-09-14")
    assert record["n"] == 1
    assert record["value"] == 1.0  # 1 of 1 breaches
    assert record["status"] == "breach"  # 100% > 1% bound
    assert len(record["breaching_symbols"]) == 1
    row = record["breaching_symbols"][0]
    assert row["ticker"] == "AAPL"
    assert row["close_yfinance"] == 100.00
    assert row["close_polygon"] == 100.60
    assert row["diff_bps"] == 60.0


def test_share_bound_ok_below_one_percent():
    # 200 symbols, 1 breach = 0.5% < 1% bound.
    prior_rows = {f"T{i}": _prior(f"T{i}", 100.0) for i in range(200)}
    new_closes = {f"T{i}": 100.0 for i in range(200)}
    new_closes["T0"] = 100.60  # single breach
    record = compute_vendor_divergence(new_closes, prior_rows, "2026-09-14")
    assert record["n"] == 200
    assert record["value"] == 1 / 200
    assert record["status"] == "ok"
    assert record["value"] <= VENDOR_DIVERGENCE_BOUND
    assert [r["ticker"] for r in record["breaching_symbols"]] == ["T0"]


def test_only_yfinance_prior_vs_polygon_fresh_pairs_are_compared():
    """A prior row from a non-yfinance vendor (e.g. already polygon, or
    fred) is not a yfinance-vs-polygon comparison and must be excluded."""
    prior_rows = {
        "AAPL": _prior("AAPL", 100.0, source="yfinance"),
        "TNX": _prior("TNX", 4.5, source="fred"),
        "MSFT": _prior("MSFT", 300.0, source="polygon"),
    }
    new_closes = {"AAPL": 100.0, "TNX": 4.5, "MSFT": 300.0}
    record = compute_vendor_divergence(new_closes, prior_rows, "2026-09-14")
    assert record["n"] == 1  # only AAPL


def test_champion_vendor_is_named_on_the_record():
    record = compute_vendor_divergence(
        {"AAPL": 100.0}, {"AAPL": _prior("AAPL", 100.0)}, "2026-09-14",
    )
    assert record["champion_vendor"] == "polygon"


def test_write_vendor_divergence_metric_puts_to_the_declared_prefix():
    s3 = MagicMock()
    write_vendor_divergence_metric(
        "alpha-engine-research",
        {"AAPL": 100.5},
        {"AAPL": _prior("AAPL", 100.0)},
        "2026-09-14",
        s3_client=s3,
    )
    assert s3.put_object.call_count == 1
    _, kwargs = s3.put_object.call_args
    assert kwargs["Bucket"] == "alpha-engine-research"
    assert kwargs["Key"] == f"{VENDOR_DIVERGENCE_METRIC_PREFIX}2026-09-14.json"
    assert kwargs["ContentType"] == "application/json"


def test_write_vendor_divergence_metric_does_not_swallow_a_put_failure():
    """Unlike the L1 observer annotation, the write itself must not
    fail-soft — a producer write failure propagates to the caller."""
    s3 = MagicMock()
    s3.put_object.side_effect = RuntimeError("boom")
    try:
        write_vendor_divergence_metric(
            "alpha-engine-research", {"AAPL": 100.5},
            {"AAPL": _prior("AAPL", 100.0)}, "2026-09-14", s3_client=s3,
        )
        assert False, "expected the PUT failure to propagate"
    except RuntimeError as exc:
        assert "boom" in str(exc)
