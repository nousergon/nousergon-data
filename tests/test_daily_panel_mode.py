"""D51 as a whole-mode unit: ``weekly_collector.py --daily-panel`` (alpha-engine-config-I10791).

The REAL ``builders.daily_panel.run`` runs under the REAL run-manifest wrapper
(`_run_whole_mode_unit`) over the 2026-10-02 universe fixture, with only the
ArcticDB read and the S3 client stubbed. What the manifest records must be what
was published — the two keys, with the panel's row count — and a refused
contract must file ``failed``, never ``ok``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("jsonschema")
pytest.importorskip("pyarrow")

import run_units  # noqa: E402
import weekly_collector  # noqa: E402
from builders import daily_panel as pub  # noqa: E402
from contracts import daily_panel as dp  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))
from test_daily_panel_publisher import DAY, _Sink, _frames  # noqa: E402

BUCKET = "alpha-engine-research"


class MemS3:
    """Just enough S3 for the run-manifest sink."""

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


class _Args:
    date = DAY.isoformat()
    dry_run = False


@pytest.fixture
def harness(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    manifests, panel = MemS3(), _Sink()
    monkeypatch.setattr(
        run_units,
        "manifest_sink",
        lambda bucket, s3_client=None: run_units.S3ManifestSink(
            bucket=bucket, prefix=run_units.DEFAULT_MANIFEST_PREFIX, s3_client=manifests
        ),
    )
    monkeypatch.setattr(pub, "_s3_io", lambda bucket, dry_run_dir: (panel.get, panel.put))
    state = {"frames": _frames()}
    monkeypatch.setattr(
        "nousergon_lib.arcticdb.load_universe_ohlcv",
        lambda bucket, **kw: state["frames"],
        raising=False,
    )
    return manifests, panel, state


def _manifest(manifests: MemS3) -> dict:
    (key,) = [k for k in manifests.objects if k.startswith(f"data_collection/runs/D51/{DAY}/")]
    return json.loads(manifests.objects[key])


def test_the_mode_is_declared_as_d51_with_a_row_source():
    assert run_units.MODE_UNITS["daily_panel"] == "D51"
    spec = run_units.MODE_ROWS["daily_panel"]
    assert (spec.collector, spec.rows_key, spec.library_ref) == ("daily_panel", "rows", None)


def test_a_published_panel_files_ok_and_records_the_two_keys(harness):
    manifests, panel, _ = harness
    result = weekly_collector._run_whole_mode_unit(
        "daily_panel", weekly_collector._run_daily_panel, {"bucket": BUCKET}, _Args()
    )
    assert result["status"] == "ok"
    doc = _manifest(manifests)
    assert doc["status"] == "ok"
    outputs = {o["key"]: o["rows_out"] for o in doc["outputs"]}
    published = json.loads(panel.objects[dp.manifest_key(DAY)])
    assert outputs == {dp.panel_key(DAY): published["row_count"], dp.manifest_key(DAY): 1}
    assert panel.order == [dp.panel_key(DAY), dp.manifest_key(DAY)]


def test_a_refused_contract_files_failed_and_writes_no_panel(harness):
    manifests, panel, state = harness
    state["frames"]["MSFT"] = state["frames"]["MSFT"].iloc[0:0]
    result = weekly_collector._run_whole_mode_unit(
        "daily_panel", weekly_collector._run_daily_panel, {"bucket": BUCKET}, _Args()
    )
    assert result["status"] == "error" and "empty frame" in result["error"]
    assert _manifest(manifests)["status"] == "failed"
    assert panel.objects == {}


def test_dry_run_publishes_nothing_and_writes_no_manifest(harness):
    manifests, panel, _ = harness

    class DryArgs(_Args):
        dry_run = True

    result = weekly_collector._run_whole_mode_unit(
        "daily_panel", weekly_collector._run_daily_panel, {"bucket": BUCKET}, DryArgs()
    )
    assert result["status"] == "ok" and result["collectors"]["daily_panel"]["status"] == "ok_dry_run"
    assert panel.objects == {} and manifests.objects == {}


def test_the_flag_resolves_to_the_daily_panel_mode_and_dispatches():
    import argparse

    assert weekly_collector._resolve_run_mode(argparse.Namespace(daily_panel=True)) == "daily_panel"
    src = " ".join(Path(weekly_collector.__file__).read_text().split())
    assert '"daily_panel", _run_daily_panel' in src
