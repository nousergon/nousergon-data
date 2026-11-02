"""D51 catch-up: a session with no panel is published by the next scheduled run (alpha-engine-config-I12652).

``ne-data-collection-panel`` starts only when ``ne-data-collection-eod``
SUCCEEDS. 2026-10-21's EOD failed and was healed through ArcticDB by hand, so
nothing ever published its panel or its D51 run record. The scheduled
``--daily-panel`` run now fills every hole in the window
``data.phase3.daily_panel_adopted`` grades, each under its own run manifest.

The REAL publisher runs under the REAL whole-mode wrapper over the 2026-10-02
universe fixture; only ArcticDB, S3 and the window are stubbed.
"""

from __future__ import annotations

import datetime as dt
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
from data_gate import panel as panel_gate  # noqa: E402

sys.path.insert(0, str(Path(__file__).parent))
from test_daily_panel_mode import BUCKET, MemS3  # noqa: E402
from test_daily_panel_publisher import DAY, _frames, _Sink  # noqa: E402

HOLE = dt.date(2026, 10, 1)
PRESENT = dt.date(2026, 9, 30)


class _Paginator:
    def __init__(self, keys):
        self.keys = keys

    def paginate(self, Bucket, Prefix):  # noqa: N803
        yield {"Contents": [{"Key": k} for k in sorted(self.keys) if k.startswith(Prefix)]}


class _ListS3:
    def __init__(self, keys):
        self.keys = keys

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self.keys)


class _Args:
    date = None
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
    monkeypatch.setattr(
        pub, "published_manifest_keys", lambda bucket, s3=None: {k for k in panel.objects if k.endswith("/manifest.json")}
    )
    monkeypatch.setattr(weekly_collector, "default_run_date", lambda *a, **k: DAY.isoformat())
    monkeypatch.setattr(panel_gate, "gap_window", lambda latest: [PRESENT, HOLE])
    state = {"frames": _frames()}
    monkeypatch.setattr(
        "nousergon_lib.arcticdb.load_universe_ohlcv",
        lambda bucket, **kw: state["frames"],
        raising=False,
    )
    panel.objects[dp.manifest_key(PRESENT)] = b"{}"
    return manifests, panel, state


def _run(args=None) -> dict:
    return weekly_collector.run_weekly({"bucket": BUCKET}, _DailyPanelArgs() if args is None else args)


class _DailyPanelArgs(_Args):
    daily_panel = True


def _run_manifests(manifests: MemS3, day: dt.date) -> list[dict]:
    prefix = f"data_collection/runs/D51/{day}/"
    return [json.loads(v) for k, v in manifests.objects.items() if k.startswith(prefix)]


def test_a_missing_session_is_published_with_its_own_backfill_run_record(harness):
    manifests, panel, _ = harness
    result = _run()
    assert result["status"] == "ok"
    assert dp.manifest_key(DAY) in panel.objects
    published = json.loads(panel.objects[dp.manifest_key(HOLE)])
    assert published["trading_day"] == HOLE.isoformat()
    assert result["catch_up"] == {
        "missing": [HOLE.isoformat()],
        "published": [{"date": HOLE.isoformat(), "status": "ok", "error": None}],
        "deferred": [],
    }
    (record,) = _run_manifests(manifests, HOLE)
    assert record["status"] == "ok" and record["trigger"] == "backfill"
    (own,) = _run_manifests(manifests, DAY)
    assert own["trigger"] == "scheduled"


def test_the_trigger_override_does_not_leak_past_the_catch_up(harness):
    _run()
    import os

    assert os.environ[run_units.TRIGGER_ENV] == "scheduled"


def test_a_declared_gap_is_left_alone(harness, monkeypatch):
    manifests, panel, _ = harness
    monkeypatch.setitem(panel_gate.DECLARED_GAPS, HOLE.isoformat(), "test: left absent on purpose")
    result = _run()
    assert "catch_up" not in result
    assert dp.manifest_key(HOLE) not in panel.objects
    assert _run_manifests(manifests, HOLE) == []


def test_an_operator_replay_of_one_day_does_not_catch_up(harness):
    manifests, panel, _ = harness

    class OneDay(_DailyPanelArgs):
        date = DAY.isoformat()

    result = _run(OneDay())
    assert "catch_up" not in result
    assert dp.manifest_key(HOLE) not in panel.objects


def test_a_dry_run_does_not_catch_up(harness):
    manifests, panel, _ = harness

    class Dry(_DailyPanelArgs):
        dry_run = True

    result = _run(Dry())
    assert "catch_up" not in result
    assert dp.manifest_key(HOLE) not in panel.objects


def test_a_failed_session_run_skips_the_catch_up(harness):
    _, panel, state = harness
    state["frames"]["MSFT"] = state["frames"]["MSFT"].iloc[0:0]
    result = _run()
    assert result["status"] == "error" and "catch_up" not in result
    assert dp.manifest_key(HOLE) not in panel.objects


def test_a_hole_that_cannot_be_filled_fails_the_run_loudly(harness, monkeypatch):
    manifests, panel, _ = harness
    real = pub.compile_panel

    def refuse_the_hole(bucket, *, trading_day, **kw):
        if trading_day == HOLE:
            raise pub.PanelCompileError(f"test: no data for {trading_day}")
        return real(bucket, trading_day=trading_day, **kw)

    monkeypatch.setattr(pub, "compile_panel", refuse_the_hole)
    result = _run()
    assert result["status"] == "error"
    assert "2026-10-01: test: no data" in result["error"]
    assert dp.manifest_key(DAY) in panel.objects
    (record,) = _run_manifests(manifests, HOLE)
    assert record["status"] == "failed"


def test_catch_up_is_bounded_per_run_oldest_first(harness, monkeypatch):
    _, panel, _ = harness
    days = [dt.date(2026, 9, 28), dt.date(2026, 9, 29), HOLE]
    panel.objects.pop(dp.manifest_key(PRESENT))
    monkeypatch.setattr(panel_gate, "gap_window", lambda latest: [*days[:2], PRESENT, HOLE])
    monkeypatch.setattr(weekly_collector, "MAX_PANEL_CATCH_UP_PER_RUN", 2)
    result = _run()
    assert [c["date"] for c in result["catch_up"]["published"]] == ["2026-09-28", "2026-09-29"]
    assert result["catch_up"]["deferred"] == [PRESENT.isoformat(), HOLE.isoformat()]


def test_the_listing_reads_only_manifests_under_the_panel_prefix():
    keys = {
        dp.manifest_key(HOLE),
        dp.panel_key(HOLE),
        f"{dp.KEY_PREFIX}history/m-slot/manifest.json",
        "data_collection/runs/D51/2026-10-01/x.json",
    }
    got = pub.published_manifest_keys(BUCKET, s3=_ListS3(keys))
    assert dp.manifest_key(HOLE) in got
    assert dp.panel_key(HOLE) not in got
    assert "data_collection/runs/D51/2026-10-01/x.json" not in got


def test_the_window_is_the_gate_window():
    """One definition of a hole: the producer fills what the gate grades."""
    src = Path(weekly_collector.__file__).read_text()
    assert "panel_gate.gap_window(latest)" in src
    assert "panel_gate.DECLARED_GAPS" in src
