"""D48 and D49 as whole-mode units (alpha-engine-config-I11306).

``weekly_collector.py --index-contributions`` (D48) and
``--nasdaq100-constituents`` (D49) run their REAL producers under the REAL
run-manifest wrapper (`_run_whole_mode_unit`), with only the producers' data
sources and the S3 client stubbed. What a manifest records must be what was
published, and a run that published less than it should must file
``failed``, never ``ok``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

import run_units
import weekly_collector
from collectors import index_contributions as ic
from collectors import nasdaq100

BUCKET = "alpha-engine-research"
DAY = "2026-10-13"


class MemS3:
    """Just enough S3 for the run-manifest sink and both producers' PUTs."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {"ETag": '"e"'}

    def get_object(self, Bucket, Key):  # noqa: N803
        payload = self.objects[Key]
        return {"Body": type("B", (), {"read": lambda self_: payload})()}

    def head_object(self, Bucket, Key):  # noqa: N803
        return {"ContentLength": 1, "ETag": '"e"'}

    def list_objects_v2(self, Bucket, Prefix, **kw):  # noqa: N803
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)]}


class _Args:
    date = DAY
    dry_run = False


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    mem = MemS3()
    monkeypatch.setattr(
        run_units,
        "manifest_sink",
        lambda bucket, s3_client=None: run_units.S3ManifestSink(
            bucket=bucket, prefix=run_units.DEFAULT_MANIFEST_PREFIX, s3_client=mem
        ),
    )
    monkeypatch.setattr(ic.boto3, "client", lambda *a, **k: mem)
    monkeypatch.setattr(nasdaq100.boto3, "client", lambda *a, **k: mem)
    return mem


def _manifest(mem: MemS3, unit: str) -> dict:
    (key,) = [k for k in mem.objects if k.startswith(f"data_collection/runs/{unit}/{DAY}/")]
    return json.loads(mem.objects[key])


# -- D48 ----------------------------------------------------------------------

_SERIES = {
    "AAPL": [(DAY, 102.0), ("2026-10-10", 100.0)],
    "SPY": [(DAY, 102.0), ("2026-10-10", 100.0)],
    "QQQ": [(DAY, 102.0), ("2026-10-10", 100.0)],
}


def _d48_sources(monkeypatch, ndx_weights: bool = True, ndx_never_published: bool = False):
    def closes(bucket):
        return lambda symbols, through, limit: {
            s: v for s, v in _SERIES.items() if s in set(symbols)
        }

    def weights(bucket):
        def source(index):
            if index == "NDX" and ndx_never_published:
                raise ic.IndexLegNotYetProduced("NDX: D49 has never published weights")
            if index == "NDX" and not ndx_weights:
                raise ic.IndexContributionsUnavailable(
                    "NDX: no weights, but D49 has published before; the file is missing"
                )
            return ic.Weights(weight_map={"AAPL": 1.0}, method="test", as_of="2026-10-10")

        return source

    monkeypatch.setattr(ic, "_default_closes_source", closes)
    monkeypatch.setattr(ic, "_default_weights_source", weights)


def test_the_modes_are_declared_with_their_units_and_row_sources():
    assert run_units.MODE_UNITS["index_contributions"] == "D48"
    assert run_units.MODE_UNITS["nasdaq100_constituents"] == "D49"
    d48 = run_units.MODE_ROWS["index_contributions"]
    d49 = run_units.MODE_ROWS["nasdaq100_constituents"]
    assert (d48.collector, d48.rows_key, d48.library_ref) == ("index_contributions", "rows_out", None)
    assert (d49.collector, d49.rows_key, d49.library_ref) == ("nasdaq100_constituents", "count", None)


def test_d48_publishing_both_indexes_files_ok_with_its_four_keys(s3, monkeypatch):
    _d48_sources(monkeypatch)
    result = weekly_collector._run_whole_mode_unit(
        "index_contributions", weekly_collector._run_index_contributions, {"bucket": BUCKET}, _Args()
    )
    assert result["status"] == "ok"
    doc = _manifest(s3, "D48")
    assert doc["status"] == "ok"
    assert {o["key"] for o in doc["outputs"]} == {
        f"market_data/index_contributions/{i}/{f}.json"
        for i in ("SPX", "NDX")
        for f in (DAY, "latest")
    }


def test_d48_ndx_never_published_is_a_declared_skip_that_files_ok(s3, monkeypatch):
    """Before D49's FIRST run: SPX is published and the manifest is `ok`, so the
    execution does not page. The skip is on the manifest, with its reason, as a
    `not_applicable` cardinality reading for the NDX leg."""
    _d48_sources(monkeypatch, ndx_never_published=True)
    result = weekly_collector._run_whole_mode_unit(
        "index_contributions", weekly_collector._run_index_contributions, {"bucket": BUCKET}, _Args()
    )
    assert result["status"] == "ok"
    doc = _manifest(s3, "D48")
    assert doc["status"] == "ok"
    assert {o["key"] for o in doc["outputs"]} == {
        f"market_data/index_contributions/SPX/{DAY}.json",
        "market_data/index_contributions/SPX/latest.json",
    }
    (skip,) = [g for g in doc["guards"] if g["key"] == "market_data/index_contributions/NDX/"]
    assert skip["guard"] == "data_cardinality" and skip["verdict"] == "not_applicable"
    assert "NDX leg skipped (declared)" in skip["detail"] and "never published" in skip["detail"]


def test_d48_ndx_missing_after_a_publish_publishes_spx_and_files_failed(s3, monkeypatch):
    """A real D49 outage (NDX.json existed before, now missing or stale): SPX
    is published, the run is `partial`, and the manifest says failed so the
    execution pages. Never `ok`, and never recorded as a declared skip."""
    _d48_sources(monkeypatch, ndx_weights=False)
    result = weekly_collector._run_whole_mode_unit(
        "index_contributions", weekly_collector._run_index_contributions, {"bucket": BUCKET}, _Args()
    )
    assert result["status"] == "partial"
    assert "NDX" in result["error"]
    assert result["collectors"]["index_contributions"]["guards"] == []
    assert _manifest(s3, "D48")["status"] == "failed"
    assert f"market_data/index_contributions/SPX/{DAY}.json" in s3.objects
    assert not any(k.startswith("market_data/index_contributions/NDX/") for k in s3.objects)


def test_d48_dry_run_publishes_nothing_and_writes_no_manifest(s3, monkeypatch):
    _d48_sources(monkeypatch)

    class DryArgs(_Args):
        dry_run = True

    result = weekly_collector._run_whole_mode_unit(
        "index_contributions", weekly_collector._run_index_contributions, {"bucket": BUCKET}, DryArgs()
    )
    assert result["status"] == "ok"
    assert result["collectors"]["index_contributions"]["status"] == "ok_dry_run"
    assert s3.objects == {}


# -- D49 ----------------------------------------------------------------------

_NDX = [f"T{i:03d}" for i in range(101)]


def _d49_sources(monkeypatch, s3, tmp_path):
    monkeypatch.setattr(nasdaq100, "_try_invesco_holdings", lambda: None)
    monkeypatch.setattr(nasdaq100, "_fetch_nasdaq100_membership", lambda: list(_NDX))
    monkeypatch.setattr(nasdaq100, "_CACHE_PATH", tmp_path / "ndx.csv")
    caps = {t: {"market_cap_raw": 1.0e9 * (i + 1)} for i, t in enumerate(_NDX[:99])}
    s3.objects["archive/fundamentals/2026-10-09.json"] = json.dumps(caps).encode()


def test_d49_files_ok_with_both_keys_counted(s3, monkeypatch, tmp_path):
    _d49_sources(monkeypatch, s3, tmp_path)
    result = weekly_collector._run_whole_mode_unit(
        "nasdaq100_constituents",
        weekly_collector._run_nasdaq100_constituents,
        {"bucket": BUCKET},
        _Args(),
    )
    assert result["status"] == "ok"
    doc = _manifest(s3, "D49")
    assert doc["status"] == "ok"
    assert {o["key"]: o["rows_out"] for o in doc["outputs"]} == {
        "market_data/index_constituents/NDX.json": 101,
        f"market_data/weekly/{DAY}/NDX.json": 101,
    }
    published = json.loads(s3.objects["market_data/index_constituents/NDX.json"])
    assert published["date"] == DAY and published["weighted_count"] == 99


def test_d49_then_d48_reads_the_weights_d49_wrote(s3, monkeypatch, tmp_path):
    """The two units chain through S3 only: D48's NDX leg reads the NDX.json
    D49 published, through the real default weights source."""
    _d49_sources(monkeypatch, s3, tmp_path)
    weekly_collector._run_whole_mode_unit(
        "nasdaq100_constituents",
        weekly_collector._run_nasdaq100_constituents,
        {"bucket": BUCKET},
        _Args(),
    )
    weights = ic._default_weights_source(BUCKET)("NDX")
    assert len(weights.weight_map) == 99
    assert weights.method == "modified_cap_approx" and weights.as_of == DAY


def test_the_flags_resolve_to_their_modes_and_dispatch():
    assert (
        weekly_collector._resolve_run_mode(argparse.Namespace(index_contributions=True))
        == "index_contributions"
    )
    assert (
        weekly_collector._resolve_run_mode(argparse.Namespace(nasdaq100_constituents=True))
        == "nasdaq100_constituents"
    )
    src = " ".join(Path(weekly_collector.__file__).read_text().split())
    assert '"index_contributions", _run_index_contributions' in src
    assert '"nasdaq100_constituents", _run_nasdaq100_constituents' in src
