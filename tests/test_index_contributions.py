"""
Per-constituent index contribution artifact — alpha-engine-config-I11297.

The artifact this publishes is what lets Metron explain a return gap by name
(``metron-ops-I346``). The contract the consumer depends on: contribution is
``weight_at_prior_close x return``, the residual is NAMED rather than hidden
in rounding, a member with no return is counted as missing and never as a
zero, and a decomposition below the coverage floor is refused rather than
published.

Every test injects both sources, so nothing here touches ArcticDB, S3 or the
network.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from collectors import index_contributions as ic

SPX = ic.IndexSpec(index="SPX", label="S&P 500", proxy_symbol="SPY")


def _weights(weight_map: dict[str, float], method: str = "ssga_holdings_file"):
    return ic.Weights(weight_map=weight_map, method=method, as_of="2026-09-19")


def test_contribution_is_prior_weight_times_return() -> None:
    """Hand-computed: AAPL at 60% weight rising 2% contributes 1.2pp."""
    payload = ic.compute_contributions(
        SPX,
        _weights({"AAPL": 0.6, "MSFT": 0.4}),
        {
            "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
            "MSFT": [("2026-09-21", 101.0), ("2026-09-18", 100.0)],
        },
        [("2026-09-21", 101.6), ("2026-09-18", 100.0)],
    )

    by_symbol = {c["symbol"]: c for c in payload["constituents"]}
    assert by_symbol["AAPL"]["return_pct"] == pytest.approx(2.0)
    assert by_symbol["AAPL"]["contribution_pp"] == pytest.approx(1.2)
    assert by_symbol["MSFT"]["contribution_pp"] == pytest.approx(0.4)
    assert payload["explained_pp"] == pytest.approx(1.6)
    assert payload["index_return_pct"] == pytest.approx(1.6)
    assert payload["residual_pp"] == pytest.approx(0.0, abs=1e-9)


def test_weights_are_taken_as_of_the_prior_close() -> None:
    """Contribution to today's move is YESTERDAY's weight times today's
    return. Reading today's weight mixes in the day's own drift and any
    rebalance, which is why `prior_close_date` is published alongside."""
    payload = ic.compute_contributions(
        SPX,
        _weights({"AAPL": 1.0}),
        {"AAPL": [("2026-09-21", 110.0), ("2026-09-18", 100.0)]},
        [("2026-09-21", 110.0), ("2026-09-18", 100.0)],
    )
    assert payload["as_of"] == "2026-09-21"
    assert payload["prior_close_date"] == "2026-09-18"
    assert payload["constituents"][0]["weight_prior_close"] == pytest.approx(1.0)


def test_the_residual_is_named_not_absorbed() -> None:
    """Cash and futures rows carry index weight the equity roster cannot
    account for, so the sum will not tie. A consumer reconciling against this
    artifact needs the gap stated, not discovered."""
    payload = ic.compute_contributions(
        SPX,
        # Equity weights sum to 0.98; the missing 2% is cash the roster drops.
        _weights({"AAPL": 0.58, "MSFT": 0.40}),
        {
            "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
            "MSFT": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        },
        [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
    )
    assert payload["index_return_pct"] == pytest.approx(2.0)
    assert payload["explained_pp"] == pytest.approx(1.96)
    assert payload["residual_pp"] == pytest.approx(0.04)


def test_a_member_with_no_return_is_missing_not_zero() -> None:
    """An absent return is UNKNOWN. Read as zero it would drop a real
    constituent's contribution silently and still appear to reconcile."""
    payload = ic.compute_contributions(
        SPX,
        _weights({"AAPL": 0.98, "HALTED": 0.02}),
        {
            "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
            # HALTED has only the prior close — no return for the session.
            "HALTED": [("2026-09-18", 50.0)],
        },
        [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
    )
    assert payload["members_missing_return"] == ["HALTED"]
    assert payload["coverage"]["members_missing_return"] == 1
    assert payload["coverage"]["weight_with_return"] == pytest.approx(0.98)
    assert [c["symbol"] for c in payload["constituents"]] == ["AAPL"]
    # Its weight lands in the residual, where it is visible.
    assert payload["residual_pp"] == pytest.approx(2.0 - 0.98 * 2.0)


def test_coverage_below_the_floor_refuses_to_publish() -> None:
    """A decomposition missing a tenth of the index explains nothing and must
    not reach a consumer looking authoritative."""
    with pytest.raises(ic.IndexContributionsUnavailable, match="refusing to publish"):
        ic.compute_contributions(
            SPX,
            _weights({"AAPL": 0.5, "GONE": 0.5}),
            {
                "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
                "GONE": [],
            },
            [("2026-09-21", 101.0), ("2026-09-18", 100.0)],
        )


def test_a_proxy_with_one_close_raises_rather_than_guessing_the_prior_day() -> None:
    """Without two observed closes we do not know what the previous TRADING
    day was, and a calendar guess silently labels a multi-day return as one
    day."""
    with pytest.raises(ic.IndexContributionsUnavailable, match="trading-day pair"):
        ic.compute_contributions(
            SPX,
            _weights({"AAPL": 1.0}),
            {"AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)]},
            [("2026-09-21", 102.0)],
        )


def test_index_return_comes_from_the_proxy_and_says_so() -> None:
    """The index VALUES carry a separate licence, so the return is the
    tradeable proxy's move and `proxy_symbol` travels with it — a consumer
    must not be able to misattribute it to the index itself."""
    payload = ic.compute_contributions(
        SPX,
        _weights({"AAPL": 1.0}),
        {"AAPL": [("2026-09-21", 103.0), ("2026-09-18", 100.0)]},
        [("2026-09-21", 102.5), ("2026-09-18", 100.0)],
    )
    assert payload["proxy_symbol"] == "SPY"
    assert payload["index_return_pct"] == pytest.approx(2.5)
    # Deliberately NOT equal to the weighted constituent sum.
    assert payload["explained_pp"] == pytest.approx(3.0)


def test_weight_method_reaches_the_consumer_unchanged() -> None:
    """The Nasdaq-100 weights are approximate until a licensed feed lands. An
    approximation that does not travel with its data gets absorbed as fact."""
    payload = ic.compute_contributions(
        ic.IndexSpec(index="NDX", label="Nasdaq 100", proxy_symbol="QQQ"),
        _weights({"AAPL": 1.0}, method="modified_cap_approx"),
        {"AAPL": [("2026-09-21", 101.0), ("2026-09-18", 100.0)]},
        [("2026-09-21", 101.0), ("2026-09-18", 100.0)],
    )
    assert payload["weight_method"] == "modified_cap_approx"
    assert payload["weights_as_of"] == "2026-09-19"


def test_collect_dry_run_produces_both_indexes() -> None:
    series = {
        "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        "SPY": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        "QQQ": [("2026-09-21", 103.0), ("2026-09-18", 100.0)],
    }

    def closes(symbols, through, limit):
        return {s: v for s, v in series.items() if s in set(symbols)}

    def weights(index):
        return _weights({"AAPL": 1.0}, method="ssga_holdings_file")

    result = ic.collect(
        bucket="any",
        run_date="2026-09-21",
        dry_run=True,
        weights_source=weights,
        closes_source=closes,
    )
    assert result["status"] == "ok_dry_run"
    assert result["indexes"] == ["NDX", "SPX"]
    assert result["errors"] == {}


def test_an_empty_weight_map_is_refused_not_published_as_a_flat_index() -> None:
    """A cache-served constituents run declares cache_no_weights precisely so
    this is refused rather than published as an index that moved 0%."""

    def weights(index):
        return ic.Weights(weight_map={}, method="cache_no_weights")

    def closes(symbols, through, limit):
        return {"SPY": [("2026-09-21", 102.0), ("2026-09-18", 100.0)]}

    with pytest.raises(ic.IndexContributionsUnavailable, match="no index produced"):
        ic.collect(
            bucket="any",
            run_date="2026-09-21",
            dry_run=True,
            weights_source=weights,
            closes_source=closes,
        )


def test_one_index_failing_does_not_withhold_the_other() -> None:
    """Per-index soft-fail is deliberate and narrow, and the error is RECORDED
    rather than swallowed — status reads `partial`, not `ok`."""
    series = {
        "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        "SPY": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        # QQQ has one close, so NDX cannot establish its trading-day pair.
        "QQQ": [("2026-09-21", 103.0)],
    }

    def closes(symbols, through, limit):
        return {s: v for s, v in series.items() if s in set(symbols)}

    def weights(index):
        return _weights({"AAPL": 1.0})

    result = ic.collect(
        bucket="any",
        run_date="2026-09-21",
        dry_run=True,
        weights_source=weights,
        closes_source=closes,
    )
    assert result["indexes"] == ["SPX"]
    assert "NDX" in result["errors"]
    assert "trading-day pair" in result["errors"]["NDX"]


def test_publish_writes_dated_and_latest_for_each_index() -> None:
    """A consumer reads `latest.json` on the happy path and a dated key to
    reconstruct a past session, so both are written from the same payload."""
    series = {
        "AAPL": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        "SPY": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
        "QQQ": [("2026-09-21", 102.0), ("2026-09-18", 100.0)],
    }

    def closes(symbols, through, limit):
        return {s: v for s, v in series.items() if s in set(symbols)}

    def weights(index):
        return _weights({"AAPL": 1.0})

    written: dict[str, str] = {}

    class _FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            written[Key] = Body
            return {}

    with patch.object(ic.boto3, "client", return_value=_FakeS3()):
        result = ic.collect(
            bucket="any",
            run_date="2026-09-21",
            weights_source=weights,
            closes_source=closes,
        )

    assert result["status"] == "ok"
    assert set(written) == {
        "market_data/index_contributions/SPX/2026-09-21.json",
        "market_data/index_contributions/SPX/latest.json",
        "market_data/index_contributions/NDX/2026-09-21.json",
        "market_data/index_contributions/NDX/latest.json",
    }
    dated = json.loads(written["market_data/index_contributions/SPX/2026-09-21.json"])
    latest = json.loads(written["market_data/index_contributions/SPX/latest.json"])
    assert dated == latest
    assert dated["schema_version"] == ic.SCHEMA_VERSION


def test_the_units_identity_holds_exactly() -> None:
    """`weight_prior_close x return_pct = contribution_pp`, asserted as an
    IDENTITY rather than a remembered scale.

    This test exists because a downstream consumer reverse-engineered the wrong
    convention from an inconsistent worked example in the issue body and
    concluded `return_pct` was already a fraction. It is PERCENT. Every field
    here except `weight_prior_close` is on the 100-scale, so a consumer divides
    all of them by 100 and none of them selectively.

    A test that encodes the same misreading as the code passes while both are
    wrong, which is exactly what happened on the consumer side — so this asserts
    the relationship between the fields, not the magnitude of any one of them.
    """
    payload = ic.compute_contributions(
        ic.IndexSpec(index="NDX", label="Nasdaq 100", proxy_symbol="QQQ"),
        _weights({"APP": 0.0121, "REST": 0.9879}, method="modified_cap_approx"),
        {
            # +28.8% on the day.
            "APP": [("2026-09-21", 128.8), ("2026-09-18", 100.0)],
            "REST": [("2026-09-21", 101.0), ("2026-09-18", 100.0)],
        },
        [("2026-09-21", 101.3), ("2026-09-18", 100.0)],
    )
    app = next(c for c in payload["constituents"] if c["symbol"] == "APP")

    assert app["weight_prior_close"] == pytest.approx(0.0121)
    assert app["return_pct"] == pytest.approx(28.8)
    assert app["contribution_pp"] == pytest.approx(0.34848)

    # The identity, for every constituent, not just the interesting one.
    for c in payload["constituents"]:
        assert c["contribution_pp"] == pytest.approx(
            c["weight_prior_close"] * c["return_pct"]
        )

    # And the fraction-domain value a consumer must reach is contribution_pp/100,
    # which equals weight x (return_pct/100) — the same divide-by-100 applied to
    # both fields, never to one of them.
    assert app["contribution_pp"] / 100.0 == pytest.approx(
        app["weight_prior_close"] * (app["return_pct"] / 100.0)
    )
