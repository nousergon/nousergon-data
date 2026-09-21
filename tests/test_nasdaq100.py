"""
Unit tests for collectors/nasdaq100.py (alpha-engine-config-I11296).

All tests operate on committed fixtures / in-memory payloads — no network.
Fixtures under tests/fixtures/nasdaq100_rows_*.json are a trimmed capture of
a live 2026-09-21 response from api.nasdaq.com/api/quote/list-type/nasdaq100
(101 rows, verified against the fleet's own measured-live curl).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from collectors import nasdaq100

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


class TestParseNasdaq100Response:
    def test_full_payload_parses_to_101_members(self):
        payload = _load_fixture("nasdaq100_rows_full.json")
        tickers = nasdaq100.parse_nasdaq100_response(payload)
        assert len(tickers) == 101
        assert "AAPL" in tickers
        assert "GOOG" in tickers and "GOOGL" in tickers  # dual share class
        assert tickers == sorted(tickers)  # sorted, stable order

    def test_truncated_payload_raises(self):
        payload = _load_fixture("nasdaq100_rows_truncated.json")
        with pytest.raises(RuntimeError, match="outside the expected"):
            nasdaq100.parse_nasdaq100_response(payload)

    def test_malformed_payload_raises(self):
        payload = _load_fixture("nasdaq100_rows_malformed.json")
        with pytest.raises(RuntimeError, match="missing data.data.rows"):
            nasdaq100.parse_nasdaq100_response(payload)

    def test_row_missing_symbol_raises(self):
        payload = {"data": {"data": {"rows": [{"companyName": "No Symbol Inc"}]}}}
        with pytest.raises(RuntimeError, match="missing a valid 'symbol'"):
            nasdaq100.parse_nasdaq100_response(payload)

    def test_dot_share_class_converted_to_hyphen(self):
        # Synthetic — none of NDX's real dual-class names use dot notation,
        # but the conversion mirrors constituents.py's SSGA convention and
        # must not silently drop a name shaped that way.
        rows = [{"symbol": f"T{i}"} for i in range(99)]
        rows.append({"symbol": "BRK.B"})
        rows.append({"symbol": "BRK.A"})
        payload = {"data": {"data": {"rows": rows}}}
        tickers = nasdaq100.parse_nasdaq100_response(payload)
        assert "BRK-B" in tickers
        assert "BRK-A" in tickers
        assert "BRK.B" not in tickers


class TestValidateMembershipCount:
    @pytest.mark.parametrize("n", [99, 103, 0])
    def test_count_outside_band_raises(self, n):
        with pytest.raises(RuntimeError, match="outside the expected"):
            nasdaq100._validate_membership_count([f"T{i}" for i in range(n)])

    @pytest.mark.parametrize("n", [100, 101, 102])
    def test_count_within_band_ok(self, n):
        nasdaq100._validate_membership_count([f"T{i}" for i in range(n)])  # no raise


class TestComputeWeights:
    def test_normalises_to_one_and_records_raw_sum(self):
        tickers = ["AAPL", "MSFT", "NVDA"]
        market_caps = {"AAPL": 3_000.0, "MSFT": 2_000.0, "NVDA": 5_000.0}
        weights = nasdaq100.compute_weights(tickers, market_caps, "modified_cap_approx")
        assert weights.raw_sum == 10_000.0
        assert weights.weight_map["AAPL"] == pytest.approx(0.3)
        assert weights.weight_map["MSFT"] == pytest.approx(0.2)
        assert weights.weight_map["NVDA"] == pytest.approx(0.5)
        assert sum(weights.weight_map.values()) == pytest.approx(1.0)
        assert weights.unweighted_tickers == ()
        assert weights.method == "modified_cap_approx"
        assert weights.index_of == {"AAPL": "NDX", "MSFT": "NDX", "NVDA": "NDX"}

    def test_missing_market_cap_declared_not_zero_filled(self):
        # PDD/ARM-shaped case: an NDX member absent from the fleet's
        # ~900-ticker fundamentals-archive universe. Must be named in
        # unweighted_tickers, never given a synthetic zero weight.
        tickers = ["AAPL", "PDD"]
        market_caps = {"AAPL": 1_000.0}
        weights = nasdaq100.compute_weights(tickers, market_caps, "modified_cap_approx")
        assert "PDD" not in weights.weight_map
        assert weights.unweighted_tickers == ("PDD",)
        assert weights.weight_map["AAPL"] == pytest.approx(1.0)
        assert weights.raw_sum == 1_000.0
        # Membership (index_of) still names PDD as a member.
        assert weights.index_of["PDD"] == "NDX"

    def test_zero_coverage_yields_empty_weight_map(self):
        weights = nasdaq100.compute_weights(["AAPL", "MSFT"], {}, "cache_no_weights")
        assert weights.weight_map == {}
        assert weights.raw_sum == 0.0
        assert set(weights.unweighted_tickers) == {"AAPL", "MSFT"}


class TestCollectLadder:
    def test_rung2_success_writes_expected_payload(self, tmp_path, monkeypatch):
        payload = _load_fixture("nasdaq100_rows_full.json")

        class _FakeS3:
            def __init__(self):
                self.puts = {}

            def list_objects_v2(self, Bucket, Prefix):
                return {"Contents": [{"Key": "archive/fundamentals/2026-09-20.json"}]}

            def get_object(self, Bucket, Key):
                assert Key == "archive/fundamentals/2026-09-20.json"
                body = json.dumps(
                    {"AAPL": {"market_cap_raw": 3000.0}, "MSFT": {"market_cap_raw": 2000.0}}
                ).encode()

                class _Body:
                    def read(self_inner):
                        return body

                return {"Body": _Body()}

            def put_object(self, Bucket, Key, Body, ContentType):
                self.puts[Key] = json.loads(Body)

        fake_s3 = _FakeS3()
        monkeypatch.setattr(nasdaq100.boto3, "client", lambda *a, **k: fake_s3)
        monkeypatch.setattr(nasdaq100, "_try_invesco_holdings", lambda: None)
        monkeypatch.setattr(
            nasdaq100, "_fetch_nasdaq100_membership",
            lambda: nasdaq100.parse_nasdaq100_response(payload),
        )
        cache_path = tmp_path / "nasdaq100_cache.csv"
        monkeypatch.setattr(nasdaq100, "_CACHE_PATH", cache_path)

        result = nasdaq100.collect(bucket="fake-bucket", run_date="2026-09-21")

        assert result["status"] == "ok"
        assert result["count"] == 101
        assert result["weight_method"] == "modified_cap_approx"
        assert result["weighted_count"] == 2  # only AAPL/MSFT covered by the fake archive

        latest = fake_s3.puts["market_data/index_constituents/NDX.json"]
        dated = fake_s3.puts["market_data/weekly/2026-09-21/NDX.json"]
        assert latest == dated
        assert latest["constituent_count"] == 101
        assert latest["weight_map"]["AAPL"] + latest["weight_map"]["MSFT"] == pytest.approx(1.0)
        assert latest["invesco_status"] == "unavailable_406"
        assert set(latest["unweighted_tickers"]) >= {"NVDA"}  # not in fake archive
        assert cache_path.exists()  # membership persisted for rung-3 fallback

    def test_rung2_failure_falls_to_cache_no_weights(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "nasdaq100_cache.csv"
        cached_tickers = [f"T{i}" for i in range(101)]  # count-band needs 100-102
        cache_path.write_text("ticker\n" + "\n".join(cached_tickers) + "\n")
        monkeypatch.setattr(nasdaq100, "_CACHE_PATH", cache_path)
        monkeypatch.setattr(nasdaq100, "_try_invesco_holdings", lambda: None)

        def _boom():
            raise RuntimeError("nasdaq endpoint down")

        monkeypatch.setattr(nasdaq100, "_fetch_nasdaq100_membership", _boom)

        class _FakeS3:
            def put_object(self, **kwargs):
                self.last = kwargs

        monkeypatch.setattr(nasdaq100.boto3, "client", lambda *a, **k: _FakeS3())

        result = nasdaq100.collect(bucket="fake-bucket", run_date="2026-09-21")
        assert result["status"] == "ok"
        assert result["weight_method"] == "cache_no_weights"
        assert result["weighted_count"] == 0

    def test_total_failure_raises_unavailable(self, tmp_path, monkeypatch):
        cache_path = tmp_path / "nasdaq100_cache.csv"  # does not exist
        monkeypatch.setattr(nasdaq100, "_CACHE_PATH", cache_path)
        monkeypatch.setattr(nasdaq100, "_try_invesco_holdings", lambda: None)

        def _boom():
            raise RuntimeError("nasdaq endpoint down")

        monkeypatch.setattr(nasdaq100, "_fetch_nasdaq100_membership", _boom)
        monkeypatch.setattr(nasdaq100.boto3, "client", lambda *a, **k: object())

        with pytest.raises(nasdaq100.Nasdaq100Unavailable, match="live fetch failed"):
            nasdaq100.collect(bucket="fake-bucket", run_date="2026-09-21")
