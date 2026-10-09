"""The dependency-free parquet reader decodes what pyarrow wrote, exactly.

Fixtures are SYNTHETIC files written by ``fixtures/make_cur_fixtures.py`` with
pyarrow, one per encoding family the reader claims to support; the expected
rows are that generator's own ``rows()``, so no real billing data and no
pyarrow are needed here. The reader was also checked against the live export's
2026-09 and 2026-10 files (7,459 and 2,536 rows): zero mismatches.
"""

from __future__ import annotations

import gzip
import sys
from datetime import timezone
from pathlib import Path

import pytest

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE / "fixtures"))

import cur_parquet as cp  # noqa: E402
from make_cur_fixtures import VARIANTS, rows  # noqa: E402

COLUMNS = list(rows(1)[0])


def _expected():
    out = []
    for r in rows():
        out.append({k: (v.replace(tzinfo=timezone.utc) if hasattr(v, "tzinfo") else v)
                    for k, v in r.items()})
    return out


@pytest.mark.parametrize("variant", sorted(VARIANTS))
def test_every_fixture_decodes_to_the_rows_that_were_written(variant):
    data = (HERE / "fixtures" / f"cur_{variant}.parquet").read_bytes()
    assert cp.column_names(data) == COLUMNS
    assert cp.read_rows(data, COLUMNS) == _expected()


def test_a_column_subset_reads_only_those_columns():
    data = (HERE / "fixtures" / "cur_spark_like.parquet").read_bytes()
    got = cp.read_rows(data, ["line_item_unblended_cost", "resource_tags_user_system"])
    assert got == [{"line_item_unblended_cost": r["line_item_unblended_cost"],
                    "resource_tags_user_system": r["resource_tags_user_system"]}
                   for r in rows()]


def test_a_missing_column_is_a_keyerror_naming_it():
    data = (HERE / "fixtures" / "cur_spark_like.parquet").read_bytes()
    with pytest.raises(KeyError, match="line_item_net_unblended_cost"):
        cp.read_rows(data, ["line_item_net_unblended_cost"])


def test_not_parquet_raises_rather_than_guessing():
    with pytest.raises(cp.ParquetUnsupported):
        cp.read_rows(gzip.compress(b"a,b\n1,2\n"), ["a"])


def test_an_unsupported_codec_raises(monkeypatch):
    data = (HERE / "fixtures" / "cur_spark_like.parquet").read_bytes()
    monkeypatch.setattr(cp, "_CODECS", {0: "UNCOMPRESSED"})
    with pytest.raises(cp.ParquetUnsupported, match="codec"):
        cp.read_rows(data, ["line_item_unblended_cost"])


class TestSnappy:
    def test_literal_and_overlapping_copy(self):
        # "abcabcabcabc": literal "abc" then a 9-byte copy at offset 3, which
        # overlaps its own output.
        stream = bytes([12, (3 - 1) << 2]) + b"abc" + bytes([((9 - 4) << 2) | 1, 3])
        assert cp.snappy_decompress(stream) == b"abcabcabcabc"

    def test_a_length_mismatch_raises(self):
        with pytest.raises(cp.ParquetUnsupported, match="length"):
            cp.snappy_decompress(bytes([5, 0]) + b"a")
