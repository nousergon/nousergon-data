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
    assert payload["trading_day"] == "2026-09-21"
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



class _NdxS3:
    """S3 for the NDX weights leg: objects by key, listings, paginated LIST."""

    def __init__(self, objects: dict[str, bytes] | None = None, list_error: str | None = None):
        self.objects = dict(objects or {})
        self.list_error = list_error

    @staticmethod
    def _missing(op):
        from botocore.exceptions import ClientError

        return ClientError({"Error": {"Code": "NoSuchKey", "Message": "x"}}, op)

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise self._missing("GetObject")
        body = self.objects[Key]
        return {"Body": type("B", (), {"read": lambda self_: body})()}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadObject")
        return {}

    def get_paginator(self, name):
        outer = self

        class _P:
            def paginate(self, Bucket, Prefix, Delimiter=None):  # noqa: N803
                if outer.list_error:
                    from botocore.exceptions import ClientError

                    raise ClientError({"Error": {"Code": outer.list_error, "Message": "x"}}, "ListObjectsV2")
                keys = sorted(k for k in outer.objects if k.startswith(Prefix))
                if Delimiter:
                    prefixes = sorted({Prefix + k[len(Prefix):].split(Delimiter)[0] + Delimiter
                                       for k in keys if Delimiter in k[len(Prefix):]})
                    yield {"CommonPrefixes": [{"Prefix": p} for p in prefixes]}
                else:
                    yield {"Contents": [{"Key": k} for k in keys]}

        return _P()


def _ndx_source(s3):
    with patch.object(ic.boto3, "client", return_value=s3):
        return ic._default_weights_source("any")


def test_ndx_never_published_is_a_declared_skip_not_an_error() -> None:
    """alpha-engine-config-I11306. D48 is scheduled daily and D49 weekly, so
    the first D48 runs can precede the first NDX.json. With NO evidence D49
    ever published (no ok D49 manifest, no dated NDX snapshot), the NDX leg
    raises IndexLegNotYetProduced: a declared skip, not an outage."""
    s3 = _NdxS3({
        # A FAILED D49 run is not a publish.
        "data_collection/runs/D49/2026-10-16/r1.json": json.dumps({"status": "failed"}).encode(),
        # Other weekly snapshots exist; none of them is NDX.
        "market_data/weekly/2026-10-09/constituents.json": b"{}",
    })
    with patch.object(ic.boto3, "client", return_value=s3):
        source = ic._default_weights_source("any")
        with pytest.raises(ic.IndexLegNotYetProduced, match="never published"):
            source("NDX")


@pytest.mark.parametrize(
    "evidence",
    [
        {"data_collection/runs/D49/2026-10-16/r1.json": json.dumps({"status": "ok"}).encode()},
        {"market_data/weekly/2026-10-16/NDX.json": b"{}"},
    ],
    ids=["ok-d49-manifest", "dated-ndx-snapshot"],
)
def test_ndx_missing_after_a_publish_is_an_outage(evidence) -> None:
    """Once D49 has published (either form of evidence), a missing NDX.json is
    an outage: IndexContributionsUnavailable, NOT the declared skip."""
    with patch.object(ic.boto3, "client", return_value=_NdxS3(evidence)):
        source = ic._default_weights_source("any")
        with pytest.raises(ic.IndexContributionsUnavailable) as raised:
            source("NDX")
    assert not isinstance(raised.value, ic.IndexLegNotYetProduced)
    assert "has published before" in str(raised.value)


def test_ndx_evidence_lookup_failing_is_never_read_as_never_published() -> None:
    """The 'never published' answer is the one that suppresses a page, so an S3
    error while looking for evidence propagates instead of defaulting to it."""
    from botocore.exceptions import ClientError

    with patch.object(ic.boto3, "client", return_value=_NdxS3(list_error="AccessDenied")):
        source = ic._default_weights_source("any")
        with pytest.raises(ClientError):
            source("NDX")


def _ndx_closes(symbols, through, limit):
    series = {
        "AAPL": [("2026-10-13", 102.0), ("2026-10-10", 100.0)],
        "SPY": [("2026-10-13", 102.0), ("2026-10-10", 100.0)],
        "QQQ": [("2026-10-13", 102.0), ("2026-10-10", 100.0)],
    }
    return {s: v for s, v in series.items() if s in set(symbols)}


def test_collect_keeps_a_declared_skip_ok_and_records_it() -> None:
    def weights(index):
        if index == "NDX":
            raise ic.IndexLegNotYetProduced("NDX: D49 has never published weights")
        return _weights({"AAPL": 1.0})

    result = ic.collect(
        bucket="any", run_date="2026-10-13", dry_run=True,
        weights_source=weights, closes_source=_ndx_closes,
    )
    assert result["status"] == "ok_dry_run"
    assert result["indexes"] == ["SPX"]
    assert result["errors"] == {}
    assert "never published" in result["skipped"]["NDX"]


@pytest.mark.parametrize("as_of, outcome", [("2026-10-05", "stale"), ("2026-10-06", "fresh"), (None, "stale")])
def test_stale_ndx_weights_are_an_outage_not_a_skip(as_of, outcome) -> None:
    """NDX weights more than NDX_MAX_WEIGHTS_AGE_SESSIONS (5) NYSE sessions
    behind run_date (Mon 2026-10-05 is 6 behind Tue 2026-10-13, Tue 10-06 is 5)
    mean the current week's snapshot is missing: an error (partial, pages), never a skip.
    Undated weights cannot be checked and are refused the same way."""

    def weights(index):
        return ic.Weights(weight_map={"AAPL": 1.0}, method="m", as_of=as_of)

    result = ic.collect(
        bucket="any", run_date="2026-10-13", dry_run=True,
        weights_source=weights, closes_source=_ndx_closes,
    )
    assert result["skipped"] == {}
    if outcome == "stale":
        assert result["indexes"] == ["SPX"]
        assert "NDX" in result["errors"]
    else:
        assert result["indexes"] == ["NDX", "SPX"]
        assert result["errors"] == {}


def test_ndx_weights_any_other_s3_failure_propagates() -> None:
    """Only the not-yet-published case is converted. AccessDenied or a
    throttle is not 'D49 has not run' and must not read as it."""
    from botocore.exceptions import ClientError

    class _Denied:
        def get_object(self, Bucket, Key):  # noqa: N803
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject")

    with patch.object(ic.boto3, "client", return_value=_Denied()):
        source = ic._default_weights_source("any")
        with pytest.raises(ClientError):
            source("NDX")


# ── NDX proxy source (QQQ is not in the ArcticDB `universe` library) ─────────
# Measured 2026-10-21: the D48 runs for 2026-10-19 and 2026-10-20 filed
# `partial` with "proxy QQQ has 0 close(s) in the window" because QQQ is in
# neither `universe` nor `macro`. QQQ's close is published every EOD run by D21
# as market_data/close_history/QQQ.json, and NDX declares that as its source.


def test_ndx_declares_close_history_as_its_proxy_source() -> None:
    by_index = {spec.index: spec for spec in ic.INDEXES}
    assert by_index["NDX"].proxy_source == ic.PROXY_SOURCE_CLOSE_HISTORY
    assert by_index["SPX"].proxy_source == ic.PROXY_SOURCE_UNIVERSE


def _universe_without_qqq(symbols, through, limit):
    """The production `universe` shape: SPY and members, no QQQ."""
    series = {
        "AAPL": [("2026-10-20", 102.0), ("2026-10-19", 100.0)],
        "SPY": [("2026-10-20", 102.0), ("2026-10-19", 100.0)],
    }
    return {s: v for s, v in series.items() if s in set(symbols)}


def test_ndx_reads_its_proxy_from_the_declared_source_not_universe() -> None:
    asked: list[list[str]] = []

    def proxy_closes(symbols, through, limit):
        asked.append(list(symbols))
        return {"QQQ": [("2026-10-20", 101.0), ("2026-10-19", 100.0)]}

    result = ic.collect(
        bucket="any", run_date="2026-10-20", dry_run=True,
        weights_source=lambda index: ic.Weights({"AAPL": 1.0}, "m", "2026-10-16"),
        closes_source=_universe_without_qqq,
        proxy_closes_source=proxy_closes,
    )
    assert result["indexes"] == ["NDX", "SPX"]
    assert result["errors"] == {}
    assert asked == [["QQQ"]]


def test_a_proxy_source_behind_the_members_is_named_not_a_coverage_failure() -> None:
    def proxy_closes(symbols, through, limit):
        return {"QQQ": [("2026-10-19", 100.0), ("2026-10-16", 99.0)]}

    result = ic.collect(
        bucket="any", run_date="2026-10-20", dry_run=True,
        weights_source=lambda index: ic.Weights({"AAPL": 1.0}, "m", "2026-10-16"),
        closes_source=_universe_without_qqq,
        proxy_closes_source=proxy_closes,
    )
    assert result["indexes"] == ["SPX"]
    assert "proxy QQQ (close_history) ends at 2026-10-19" in result["errors"]["NDX"]


class _CloseHistoryS3:
    def __init__(self, objects: dict[str, dict]):
        self.objects = objects

    def get_object(self, Bucket, Key):  # noqa: N803
        from botocore.exceptions import ClientError

        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "x"}}, "GetObject")

        class _Body:
            def __init__(self, raw):
                self.raw = raw

            def read(self):
                return self.raw

        return {"Body": _Body(json.dumps(self.objects[Key]).encode())}


def test_close_history_source_returns_the_latest_two_on_or_before_through() -> None:
    s3 = _CloseHistoryS3({
        "market_data/close_history/QQQ.json": {
            "adjustment_basis": "dividend_adjusted",
            "closes": [["2026-10-16", 99.0], ["2026-10-19", 100.0],
                       ["2026-10-20", 101.0], ["2026-10-21", 102.0]],
        },
    })
    with patch.object(ic.boto3, "client", return_value=s3):
        out = ic._default_close_history_source("any")(["QQQ", "ONEQ"], "2026-10-20", 2)
    assert out == {"QQQ": [("2026-10-20", 101.0), ("2026-10-19", 100.0)]}


def test_close_history_source_refuses_a_different_adjustment_basis() -> None:
    s3 = _CloseHistoryS3({
        "market_data/close_history/QQQ.json": {
            "adjustment_basis": "split_only",
            "closes": [["2026-10-19", 100.0], ["2026-10-20", 101.0]],
        },
    })
    with patch.object(ic.boto3, "client", return_value=s3):
        with pytest.raises(ic.IndexContributionsUnavailable, match="adjustment_basis"):
            ic._default_close_history_source("any")(["QQQ"], "2026-10-20", 2)
