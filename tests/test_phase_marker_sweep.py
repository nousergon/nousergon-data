"""Tests for the Weekly-SF post-run phase-marker sweep (config#2322)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _FakeS3:
    """Minimal boto3 S3 client fake: list_objects_v2 pagination + get_object."""

    def __init__(self, keys_and_bodies: dict[str, bytes]):
        self._bodies = keys_and_bodies
        contents = [{"Key": k} for k in keys_and_bodies]
        self._pages = [{"Contents": contents}] if contents else [{}]

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)

    def get_object(self, Bucket, Key):
        body = MagicMock()
        body.read.return_value = self._bodies[Key]
        return {"Body": body}


def _marker(phase: str, status: str, error: str | None = None) -> bytes:
    return json.dumps({
        "schema_version": 1,
        "phase": phase,
        "date": "2026-07-18",
        "status": status,
        "started_at": "2026-07-18T09:00:00Z",
        "completed_at": "2026-07-18T09:00:01Z",
        "duration_s": 1.0,
        "artifact_keys": [],
        "error": error,
    }).encode()


def test_sweep_no_markers_returns_ok(monkeypatch):
    from validators import phase_marker_sweep

    monkeypatch.setattr(phase_marker_sweep, "boto3", MagicMock(
        client=lambda *a, **k: _FakeS3({})
    ))
    result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "ok"
    assert result["checked_count"] == 0
    assert result["error_phases"] == []


def test_sweep_all_ok_markers_returns_ok(monkeypatch):
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
        "backtest/2026-07-18/.phases/evaluator.json": _marker("evaluator", "ok"),
    })
    monkeypatch.setattr(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake))
    result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "ok"
    assert result["checked_count"] == 2
    assert result["error_phases"] == []


def test_sweep_detects_error_marker():
    """The canonical 2026-07-11 scenario: one phase status=error."""
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
        "backtest/2026-07-18/.phases/scanner_predictor_research_free_backfill.json": _marker(
            "scanner_predictor_research_free_backfill", "error",
            error="FileNotFoundError: missing local weights sync",
        ),
    })
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "phase_errors_detected"
    assert result["checked_count"] == 2
    assert len(result["error_phases"]) == 1
    assert result["error_phases"][0]["phase"] == "scanner_predictor_research_free_backfill"
    assert "FileNotFoundError" in result["error_phases"][0]["error"]


def test_sweep_unparseable_marker_skipped_not_fatal():
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/corrupt.json": b"not json {{{",
        "backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok"),
    })
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "ok"
    assert result["checked_count"] == 1


def test_sweep_s3_failure_returns_error():
    from validators import phase_marker_sweep

    boom_client = MagicMock(side_effect=Exception("S3 unreachable"))
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=boom_client)):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "s3_list"


def test_sweep_publishes_alert_on_error_phase_with_dedup_key():
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker(
            "evaluator", "error", error="ValueError: bad input",
        ),
    })
    fake_alerts = MagicMock()
    fake_alerts.publish.return_value = MagicMock(
        sns=MagicMock(ok=True), telegram=MagicMock(ok=True), any_ok=True,
    )
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)), \
         patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        result = phase_marker_sweep.sweep(run_date="2026-07-18", alert=True)

    assert result["status"] == "phase_errors_detected"
    fake_alerts.publish.assert_called_once()
    _, kwargs = fake_alerts.publish.call_args
    assert kwargs["dedup_key"] == "phase_marker_sweep_2026-07-18_evaluator"
    assert kwargs["severity"] == "error"


def test_sweep_no_alert_flag_skips_publish():
    from validators import phase_marker_sweep

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker(
            "evaluator", "error", error="ValueError: bad input",
        ),
    })
    fake_alerts = MagicMock()
    with patch.object(phase_marker_sweep, "boto3", MagicMock(client=lambda *a, **k: fake)), \
         patch.dict("sys.modules", {"nousergon_lib": MagicMock(alerts=fake_alerts),
                                     "nousergon_lib.alerts": fake_alerts}):
        phase_marker_sweep.sweep(run_date="2026-07-18", alert=False)
    fake_alerts.publish.assert_not_called()


def test_main_exit_code_ok():
    from validators.phase_marker_sweep import main

    fake = _FakeS3({"backtest/2026-07-18/.phases/simulate.json": _marker("simulate", "ok")})
    with patch("validators.phase_marker_sweep.boto3", MagicMock(client=lambda *a, **k: fake)):
        rc = main(["--run-date", "2026-07-18", "--no-alert"])
    assert rc == 0


def test_main_exit_code_phase_errors_detected():
    from validators.phase_marker_sweep import main

    fake = _FakeS3({
        "backtest/2026-07-18/.phases/evaluator.json": _marker("evaluator", "error", error="boom"),
    })
    with patch("validators.phase_marker_sweep.boto3", MagicMock(client=lambda *a, **k: fake)):
        rc = main(["--run-date", "2026-07-18", "--no-alert"])
    assert rc == 1


def test_main_requires_run_date():
    from validators.phase_marker_sweep import main

    with pytest.raises(SystemExit):
        main(["--no-alert"])
