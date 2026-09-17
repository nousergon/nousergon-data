"""Producer contract test for fundamentals_snapshot.schema.json (D10, data-collector
plan P-07, alpha-engine-config-I10873).

Validates the REAL `collectors/fundamentals.py::collect` write body (the whole
`archive/fundamentals/{date}.json` object, keyed by ticker) against the schema,
plus a hand-built minimal fixture independent of the producer code so a producer
bug that matches its own (wrong) output can't also pass the contract by
construction.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

pytest.importorskip("jsonschema")

from collectors import fundamentals
from collectors.fundamentals import NEUTRAL
from contracts import validate_fundamentals_snapshot


class TestRealProducerWriteValidates:
    def test_collect_write_body_validates(self, monkeypatch):
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        good = {"pe_ratio": 12.5, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}
        with patch.object(fundamentals, "_fetch_single_ticker", return_value=good):
            with patch("boto3.client") as mock_boto:
                mock_s3 = mock_boto.return_value
                fundamentals.collect(
                    bucket="test-bucket",
                    tickers=["AAPL", "MSFT"],
                    run_date="2026-09-16",
                    dry_run=False,
                )
        put_calls = [c for c in mock_s3.put_object.call_args_list if "archive/fundamentals/" in c.kwargs["Key"]]
        assert put_calls, "collect() did not PUT to archive/fundamentals/"
        body = json.loads(put_calls[0].kwargs["Body"])
        assert body, "published snapshot must not be empty"
        errors = validate_fundamentals_snapshot(body)
        assert errors == [], errors

    def test_neutral_sentinel_rows_also_validate(self, monkeypatch):
        """A ticker Finnhub returns nothing usable for still writes the NEUTRAL
        shape — same contract, not a hole in it. NEUTRAL rows don't count toward
        the ok_ratio gate, so 9 real + 1 NEUTRAL clears the 90% threshold while
        still exercising the NEUTRAL shape in the published body."""
        monkeypatch.setenv("FINNHUB_API_KEY", "test-key")
        good = {"pe_ratio": 12.5, **{k: NEUTRAL[k] for k in NEUTRAL if k != "pe_ratio"}}
        side_effect = [good] * 9 + [NEUTRAL.copy()]
        with patch.object(fundamentals, "_fetch_single_ticker", side_effect=side_effect):
            with patch("boto3.client") as mock_boto:
                mock_s3 = mock_boto.return_value
                fundamentals.collect(
                    bucket="test-bucket",
                    tickers=[f"T{i}" for i in range(10)],
                    run_date="2026-09-16",
                    dry_run=False,
                )
        put_calls = [c for c in mock_s3.put_object.call_args_list if "archive/fundamentals/" in c.kwargs["Key"]]
        assert put_calls
        body = json.loads(put_calls[0].kwargs["Body"])
        errors = validate_fundamentals_snapshot(body)
        assert errors == [], errors


class TestHandBuiltFixtureValidates:
    def _row(self, **overrides) -> dict:
        row = dict(NEUTRAL)
        row.update(overrides)
        return row

    def test_full_snapshot_validates(self):
        payload = {"AAPL": self._row(pe_ratio=28.4, market_cap_raw=3_000_000_000_000.0)}
        assert validate_fundamentals_snapshot(payload) == []

    def test_neutral_row_validates(self):
        assert validate_fundamentals_snapshot({"XYZ": self._row()}) == []

    def test_empty_snapshot_validates(self):
        assert validate_fundamentals_snapshot({}) == []

    def test_row_missing_required_field_is_rejected(self):
        row = self._row()
        del row["pe_ratio"]
        assert validate_fundamentals_snapshot({"AAPL": row}) != []

    def test_row_with_wrong_type_is_rejected(self):
        payload = {"AAPL": self._row(pe_ratio="not-a-number")}
        assert validate_fundamentals_snapshot(payload) != []

    def test_row_with_extra_field_is_rejected(self):
        payload = {"AAPL": self._row(unexpected_field=1.0)}
        assert validate_fundamentals_snapshot(payload) != []
