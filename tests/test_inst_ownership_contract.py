"""Producer contract test for contracts/inst_ownership.schema.json (D39, data-collector
plan P-07, alpha-engine-config-I10870).

Mirrors the existing contract-test pattern (test_staging_daily_closes_contract.py,
test_crypto_holdings_contract.py): every row this producer writes must validate cleanly
against its own versioned JSON Schema, checked at PR time.

Consumer: crucible v2 `crucible/data/point_in_time.py::SnapshotPointInTimeSource.
_load_institutional`, which reads `data/inst_ownership/{quarter}/latest.parquet` via
`crucible.keys.inst_ownership_key(year, quarter)` and hard-requires `ticker`,
`n_funds_increasing`, `n_funds_decreasing` (`INSTITUTIONAL_COLUMNS`) — a missing column
raises `MissingSourceError`, never a zero-fill (pinned separately in crucible
`tests/contracts/inst_ownership.schema.json` + `tests/test_inst_ownership_contract.py`).

The `data/inst_ownership/{quarter}/{ticker}.parquet` / `latest.json` keys this unit's
descriptor (registry.d/units/D39-inst-ownership.yaml) currently names do not include
the `{prefix}/{quarter}/latest.parquet` key crucible v2 actually reads, and its
`consumers` list does not name crucible — both are `writes`/`consumers` field
corrections, out of scope for this PR (owned by a sibling agent per this session's
scope split); this test pins the row shape the real reader already depends on today.

Covers:
  - A REAL artifact produced by `write_inst_ownership_parquet` (the per-quarter
    `latest.parquet`), read back via `read_inst_ownership_parquet`, one row validated.
  - A hand-built minimal fixture, independent of the producer code.
  - A record missing a crucible-required field (`n_funds_increasing`) is rejected.
"""

from __future__ import annotations

import pytest

pytest.importorskip("jsonschema")

from data.derived.inst_ownership import (
    InstOwnershipRow,
    read_inst_ownership_parquet,
    write_inst_ownership_parquet,
)
from contracts import validate_inst_ownership_row
from tests.test_inst_ownership_reader import _InMemoryS3, _make_row


class TestProducerWriteValidates:
    def test_real_written_row_validates(self):
        s3 = _InMemoryS3()
        write_inst_ownership_parquet([_make_row()], quarter="2026Q2", s3_client=s3)
        df = read_inst_ownership_parquet(s3_client=s3)
        assert len(df) == 1
        row = df.iloc[0].to_dict()
        errors = validate_inst_ownership_row(row)
        assert errors == [], errors

    def test_null_optional_fields_still_validate(self):
        s3 = _InMemoryS3()
        row = _make_row(
            shares_qoq_change=None, value_qoq_change=None,
            top5_concentration_pct=None, put_call_ratio=None,
        )
        write_inst_ownership_parquet([row], quarter="2026Q2", s3_client=s3)
        df = read_inst_ownership_parquet(s3_client=s3)
        errors = validate_inst_ownership_row(df.iloc[0].to_dict())
        assert errors == [], errors


class TestHandBuiltFixtureValidates:
    def _row(self, **overrides) -> dict:
        row = {
            "ticker": "AAPL",
            "quarter": "2026Q2",
            "schema_version": 1,
            "n_funds_holding": 18,
            "total_shares_held": 450_200_000.0,
            "total_value_usd": 90_000_000_000.0,
            "shares_qoq_change": 2_100_000.0,
            "value_qoq_change": 500_000_000.0,
            "top5_concentration_pct": 8.2,
            "n_funds_increasing": 12,
            "n_funds_decreasing": 3,
            "n_funds_new": 1,
            "n_funds_exited": 0,
            "put_call_ratio": None,
        }
        row.update(overrides)
        return row

    def test_full_record_validates(self):
        assert validate_inst_ownership_row(self._row()) == []

    def test_record_missing_crucible_required_field_is_rejected(self):
        row = self._row()
        del row["n_funds_increasing"]
        assert validate_inst_ownership_row(row) != []

    def test_bad_quarter_format_is_rejected(self):
        assert validate_inst_ownership_row(self._row(quarter="2026-Q2")) != []
