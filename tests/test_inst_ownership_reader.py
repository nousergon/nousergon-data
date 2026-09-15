"""Tests for data/derived/inst_ownership.py's read-side (config#2428).

The write-side (SEC bulk download / CUSIP crosswalk / QoQ delta compute)
predates this PR and isn't covered here — this file exercises the new
``read_inst_ownership_parquet`` consumer-side reader added to support
``rag/pipelines/ingest_13f.py``, following the same
write-then-read-via-latest.json-sidecar contract as
``data/derived/news_aggregates.py``.
"""

from __future__ import annotations

from io import BytesIO

import pytest

from data.derived.inst_ownership import (
    InstOwnershipRow,
    read_inst_ownership_parquet,
    write_inst_ownership_parquet,
)


class _InMemoryS3:
    """Minimal in-memory S3 mock supporting put_object + get_object.

    Mirrors test_news_aggregates.py's _InMemoryS3 (avoids adding moto as
    a test dep).
    """

    class _NoSuchKey(Exception):
        pass

    def __init__(self) -> None:
        self._store: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self._store[(Bucket, Key)] = Body
        return {"ETag": "stub"}

    def get_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self._store:
            raise self._NoSuchKey(f"NoSuchKey: {Bucket}/{Key}")
        return {"Body": BytesIO(self._store[(Bucket, Key)])}


def _make_row(**overrides) -> InstOwnershipRow:
    base = dict(
        ticker="AAPL",
        quarter="2026Q2",
        schema_version=1,
        n_funds_holding=18,
        total_shares_held=450_200_000.0,
        total_value_usd=90_000_000_000.0,
        shares_qoq_change=2_100_000.0,
        value_qoq_change=500_000_000.0,
        top5_concentration_pct=8.2,
        n_funds_increasing=12,
        n_funds_decreasing=3,
        n_funds_new=1,
        n_funds_exited=0,
        put_call_ratio=None,
    )
    base.update(overrides)
    return InstOwnershipRow(**base)


class TestReadInstOwnershipParquet:
    def test_write_then_read_preserves_rows(self):
        s3 = _InMemoryS3()
        rows = [_make_row()]
        write_inst_ownership_parquet(
            rows, quarter="2026Q2", s3_client=s3, run_id="2607141200",
        )
        df = read_inst_ownership_parquet(s3_client=s3)
        assert len(df) == 1
        assert df.iloc[0]["ticker"] == "AAPL"
        assert df.iloc[0]["n_funds_increasing"] == 12
        assert df.iloc[0]["n_funds_decreasing"] == 3

    def test_missing_parquet_returns_empty_schema_df(self):
        s3 = _InMemoryS3()
        df = read_inst_ownership_parquet(s3_client=s3)
        assert len(df) == 0
        for col in InstOwnershipRow.__dataclass_fields__:
            assert col in df.columns

    def test_read_uses_latest_json_sidecar_not_per_quarter_path(self):
        """A stray parquet at the quarter path with no latest.json update
        must not be picked up — canonical-only contract (same as
        news_aggregates' legacy-key regression guard)."""
        s3 = _InMemoryS3()
        rows = [_make_row(ticker="MSFT")]
        write_inst_ownership_parquet(rows, quarter="2026Q2", s3_client=s3)
        # Overwrite latest.json with garbage so the sidecar resolves to
        # nothing — read must degrade to empty, not silently succeed via
        # some other path.
        s3.put_object(
            Bucket="alpha-engine-research",
            Key="data/inst_ownership/latest.json",
            Body=b"{}",
        )
        df = read_inst_ownership_parquet(s3_client=s3)
        assert len(df) == 0


# ── main() CLI guard (§119 rule 1) ────────────────────────────────────

class TestMainCLIGuard:
    """Tests for inst_ownership.main() CLI entry point guards.

    Covers three exit paths: (1) boto3 import failure, (2) missing
    ``--tickers-file`` argument, (3) success path with valid file and
    mocked compute.
    """

    @pytest.fixture(autouse=True)
    def _no_real_manifest_writes(self, monkeypatch):
        """`main()` now writes one D39 run manifest per execution
        (alpha-engine-config-I10810). Swap the SINK so these tests exercise the
        real path without a live S3 PUT — and so the boto3-absent test measures
        the CLI's import guard rather than the sink's own lazy boto3 import."""
        import run_units

        class _NullSink:
            bucket = "test-bucket"

            def write(self, key: str, payload: bytes):  # noqa: ARG002
                return None

        monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
        monkeypatch.setattr(
            run_units, "manifest_sink", lambda bucket, s3_client=None: _NullSink(),
        )

    def test_missing_boto3_exits_with_code_1(self, monkeypatch):
        """Simulate absent boto3 — verifies SystemExit(1) from the import guard."""
        import builtins
        import pytest
        from data.derived.inst_ownership import main

        real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "boto3":
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _mock_import)
        monkeypatch.setattr("sys.argv", ["inst_ownership"])

        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_missing_tickers_file_exits_with_code_1(self, monkeypatch):
        """Omit --tickers-file — verifies SystemExit(1) from the arg guard."""
        import pytest
        from data.derived.inst_ownership import main

        monkeypatch.setattr("sys.argv", ["inst_ownership"])

        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1

    def test_success_path_returns_normally(self, monkeypatch, tmp_path):
        """Valid tickers file with mocked S3 and compute — guard passes."""
        from data.derived.inst_ownership import main

        tickers_file = tmp_path / "tickers.txt"
        tickers_file.write_text("AAPL\nMSFT\nGOOG\n")

        monkeypatch.setattr(
            "sys.argv",
            ["inst_ownership", "--tickers-file", str(tickers_file)],
        )
        monkeypatch.setattr("boto3.client", lambda *a, **k: None)
        monkeypatch.setattr(
            "data.derived.inst_ownership.compute_and_write_inst_ownership",
            lambda *a, **k: [_make_row()],
        )

        main()  # must not raise
