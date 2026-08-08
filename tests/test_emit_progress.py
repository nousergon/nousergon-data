"""Unit tests for ``rag.pipelines.emit_progress`` (config-I2966).

Covers:
  - Happy-path PUT: correct bucket/key/body shape.
  - Fail-soft swallow: a boto3 exception returns False (never raises) —
    the deliberate no-silent-fails deviation this module documents.
  - CLI ``main()`` never raises even when the underlying PUT fails.

All boto3 calls mocked; no live AWS / network.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from rag.pipelines.emit_progress import emit_progress, main


def test_emit_progress_writes_expected_key_and_body():
    fake_s3 = MagicMock()
    with patch("boto3.client", return_value=fake_s3):
        ok = emit_progress(
            run_date="2026-07-25",
            step=5,
            of=10,
            label="news",
            started_at="2026-07-25T09:00:00Z",
        )
    assert ok is True
    fake_s3.put_object.assert_called_once()
    kwargs = fake_s3.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "alpha-engine-research"
    assert kwargs["Key"] == "health/rag_ingestion_progress/2026-07-25.json"
    assert kwargs["ContentType"] == "application/json"
    body = json.loads(kwargs["Body"])
    assert body["step"] == 5
    assert body["of"] == 10
    assert body["label"] == "news"
    assert body["started_at"] == "2026-07-25T09:00:00Z"
    assert "updated_at" in body


def test_emit_progress_fails_soft_on_boto_error():
    """A PUT failure must return False, never raise — the caller (the shell
    script's ``|| echo WARN`` guard) depends on this being a clean False,
    not an exception that would propagate out of a `python -m` call and
    trip `set -euo pipefail` in the caller."""
    fake_s3 = MagicMock()
    fake_s3.put_object.side_effect = RuntimeError("S3 unavailable")
    with patch("boto3.client", return_value=fake_s3):
        ok = emit_progress(
            run_date="2026-07-25",
            step=3,
            of=10,
            label="earnings_transcripts",
            started_at="2026-07-25T09:00:00Z",
        )
    assert ok is False


def test_main_never_raises_on_put_failure(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "emit_progress.py",
            "--run-date", "2026-07-25",
            "--step", "7",
            "--of", "10",
            "--label", "inst_ownership_13f",
            "--started-at", "2026-07-25T09:00:00Z",
        ],
    )
    fake_s3 = MagicMock()
    fake_s3.put_object.side_effect = RuntimeError("boom")
    with patch("boto3.client", return_value=fake_s3):
        main()  # must not raise
