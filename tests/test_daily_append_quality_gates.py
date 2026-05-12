"""Regression tests for the write-time quality gate in builders/daily_append.py.

Background (ROADMAP L940 "Data quality gates"):
``validators/price_validator.py`` shipped earlier with thresholds for 50%
daily moves / 10x volume spikes / OHLC inversions / zero close / zero
volume, but was wired only into ``collectors/slim_cache.py`` +
``collectors/prices.py`` as non-blocking observation. The canonical write
path (``builders/daily_append.py``) had no validation — definitely-bad
rows (Close<=0, High<Low) could land in ArcticDB and silently pollute
downstream feature compute + predictor training.

This wave wires ``validate_today_row`` into the per-symbol write_tasks
loop with per-anomaly-type severity semantics: ``block`` definitely-bad
rows by default (refuse the queue + log error so Flow Doctor surfaces),
``warn`` legitimately-rare-but-possible signals (queue write + log
warning + emit metric). Operators upgrade types to block via
``DAILY_APPEND_BLOCK_ANOMALY_TYPES``.

Source-text invariants pin the wiring shape; functional cases through
``_load_block_anomaly_types`` cover the env-var loader.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from validators.price_validator import (
    ANOMALY_BAD_OHLC,
    ANOMALY_EXTREME_DAILY_MOVE,
    ANOMALY_NEGATIVE_OR_ZERO_CLOSE,
    ANOMALY_VOLUME_SPIKE,
    ANOMALY_ZERO_VOLUME,
    DEFAULT_BLOCK_ANOMALY_TYPES,
)
from builders.daily_append import (
    _emit_quality_gate_metrics,
    _load_block_anomaly_types,
)


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


# ── 1. Source-text invariants ──────────────────────────────────────────────────


def test_validator_imported_from_price_validator():
    """The validator must come from validators.price_validator — divergent
    re-implementations would let block-set / threshold changes drift."""
    src = _source()
    assert "from validators.price_validator import" in src
    assert "validate_today_row" in src
    assert "DEFAULT_BLOCK_ANOMALY_TYPES" in src
    assert "ALL_ANOMALY_TYPES" in src


def test_validate_today_row_called_before_write_queue():
    """The validation call must precede ``write_tasks.append(...)`` — wiring
    it after the append would let bad rows reach update_batch/write_batch."""
    src = _source()
    validate_idx = src.find("validate_today_row(today_row, hist, ticker)")
    append_idx = src.find("write_tasks.append((ticker, today_row, hist, nan_features))")
    assert validate_idx > 0
    assert append_idx > 0
    assert validate_idx < append_idx, (
        "validate_today_row must run before write_tasks.append — otherwise "
        "blocking-severity rows would be queued for write before the gate "
        "decides to refuse them."
    )


def test_block_path_uses_logger_error_not_warning():
    """Blocking anomalies must emit logger.error so the FlowDoctorHandler
    picks them up — per feedback_collector_return_dict_invisible_to_flow_doctor,
    only logger.error / logger.critical cross the logging boundary."""
    src = _source()
    # The block branch must use log.error
    assert 'log.error(\n                            "Quality gate BLOCK %s.%s: %s"' in src or \
        'log.error(\n                            "Quality gate BLOCK' in src, (
            "Block path must call log.error so Flow Doctor surfaces the "
            "anomaly via SNS / GitHub issue."
        )


def test_block_path_skips_write_via_continue():
    """The block branch must ``continue`` past write_tasks.append — otherwise
    refused rows still land in ArcticDB."""
    src = _source()
    # The block branch must continue out of the loop iteration
    assert "n_quality_blocked += 1\n                    continue" in src


def test_env_var_loader_present():
    """DAILY_APPEND_BLOCK_ANOMALY_TYPES must be operator-tunable without
    a code redeploy — mirrors ALT_MIN_OK_RATIOS in collectors/alternative.py."""
    src = _source()
    assert "_load_block_anomaly_types" in src
    assert "DAILY_APPEND_BLOCK_ANOMALY_TYPES" in src
    assert "DEFAULT_BLOCK_ANOMALY_TYPES" in src


def test_quality_metric_helper_emits_namespace():
    """CW metric helper must emit under AlphaEngine/Data with the gauge
    names alarms can target."""
    src = _source()
    assert "_emit_quality_gate_metrics" in src
    assert "AlphaEngine/Data" in src
    assert "daily_append_quality_blocked_count" in src
    assert "daily_append_quality_warned_count" in src
    assert "daily_append_quality_anomaly_count" in src
    assert "anomaly_type" in src  # CW dimension key for per-type partition


def test_result_dict_exposes_quality_counts():
    """The return dict surfaces per-run quality stats for postflight readers
    and the Saturday SF completion email."""
    src = _source()
    assert '"tickers_quality_blocked": n_quality_blocked' in src
    assert '"tickers_quality_warned": n_quality_warned' in src
    assert '"quality_anomaly_counts": dict(quality_counts_by_type)' in src
    assert '"quality_block_anomaly_types": sorted(block_anomaly_types)' in src


def test_metric_emit_called_when_not_dry_run():
    """The metric emit must be gated on ``not dry_run`` — dry runs shouldn't
    pollute the production CW namespace."""
    src = _source()
    assert "if not dry_run:\n        _emit_quality_gate_metrics(" in src


# ── 2. Functional: _load_block_anomaly_types ──────────────────────────────────


class TestLoadBlockAnomalyTypes:
    def test_unset_returns_defaults(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DAILY_APPEND_BLOCK_ANOMALY_TYPES", None)
            assert _load_block_anomaly_types() == DEFAULT_BLOCK_ANOMALY_TYPES

    def test_empty_string_returns_defaults(self):
        with patch.dict(os.environ, {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": ""}):
            assert _load_block_anomaly_types() == DEFAULT_BLOCK_ANOMALY_TYPES

    def test_explicit_empty_list_yields_pure_observability(self):
        """Setting `[]` flips to pure observability — nothing blocks, all
        anomalies warn-only. Operator escape hatch for known-noisy days."""
        with patch.dict(os.environ, {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": "[]"}):
            assert _load_block_anomaly_types() == frozenset()

    def test_explicit_block_set(self):
        with patch.dict(
            os.environ,
            {
                "DAILY_APPEND_BLOCK_ANOMALY_TYPES": json.dumps([
                    ANOMALY_BAD_OHLC, ANOMALY_EXTREME_DAILY_MOVE,
                ])
            },
        ):
            assert _load_block_anomaly_types() == frozenset({
                ANOMALY_BAD_OHLC, ANOMALY_EXTREME_DAILY_MOVE,
            })

    def test_malformed_json_raises(self):
        with patch.dict(
            os.environ, {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": "not-json"}
        ):
            with pytest.raises(RuntimeError, match="not valid JSON"):
                _load_block_anomaly_types()

    def test_non_list_raises(self):
        with patch.dict(
            os.environ, {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": '{"a": 1}'}
        ):
            with pytest.raises(RuntimeError, match="JSON list of strings"):
                _load_block_anomaly_types()

    def test_non_string_items_raise(self):
        with patch.dict(
            os.environ, {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": "[1, 2]"}
        ):
            with pytest.raises(RuntimeError, match="JSON list of strings"):
                _load_block_anomaly_types()

    def test_unknown_anomaly_type_raises(self):
        with patch.dict(
            os.environ,
            {"DAILY_APPEND_BLOCK_ANOMALY_TYPES": '["typo_anomaly"]'},
        ):
            with pytest.raises(RuntimeError, match="unknown anomaly types"):
                _load_block_anomaly_types()


# ── 3. Functional: _emit_quality_gate_metrics ─────────────────────────────────


class TestEmitQualityGateMetrics:
    def test_no_op_when_nothing_to_report(self):
        """Zero counts → no CW call (avoids per-day useless data points)."""
        with patch("builders.daily_append.boto3") as mock_boto:
            _emit_quality_gate_metrics({}, 0, 0)
            mock_boto.client.assert_not_called()

    def test_emits_blocked_and_warned_gauges(self):
        with patch("builders.daily_append.boto3") as mock_boto:
            cw = mock_boto.client.return_value
            _emit_quality_gate_metrics(
                {ANOMALY_BAD_OHLC: 1, ANOMALY_EXTREME_DAILY_MOVE: 3},
                n_blocked=1,
                n_warned=3,
            )
            cw.put_metric_data.assert_called_once()
            call = cw.put_metric_data.call_args
            assert call.kwargs["Namespace"] == "AlphaEngine/Data"
            metric_names = {m["MetricName"] for m in call.kwargs["MetricData"]}
            assert "daily_append_quality_blocked_count" in metric_names
            assert "daily_append_quality_warned_count" in metric_names
            assert "daily_append_quality_anomaly_count" in metric_names

    def test_per_type_dimension_present(self):
        with patch("builders.daily_append.boto3") as mock_boto:
            cw = mock_boto.client.return_value
            _emit_quality_gate_metrics(
                {ANOMALY_VOLUME_SPIKE: 5}, n_blocked=0, n_warned=5,
            )
            metric_data = cw.put_metric_data.call_args.kwargs["MetricData"]
            anomaly_count = next(
                m for m in metric_data
                if m["MetricName"] == "daily_append_quality_anomaly_count"
            )
            dims = anomaly_count["Dimensions"]
            assert any(
                d["Name"] == "anomaly_type" and d["Value"] == ANOMALY_VOLUME_SPIKE
                for d in dims
            )

    def test_cloudwatch_failure_swallowed(self):
        """Pipeline must not fail on CW IAM / network errors — the load-bearing
        observability surface is the logger.error calls, not the metric."""
        with patch("builders.daily_append.boto3") as mock_boto:
            mock_boto.client.return_value.put_metric_data.side_effect = (
                RuntimeError("CW unavailable")
            )
            # Should not raise
            _emit_quality_gate_metrics(
                {ANOMALY_BAD_OHLC: 1}, n_blocked=1, n_warned=0,
            )
