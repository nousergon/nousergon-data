"""Guard: ``filing_change_detection.py --key-prefix`` writes to a scoped S3
key and never touches the production ``latest.json`` pointer.

Added for the Saturday-replay canary (alpha-engine-config#2246): the
canary must exercise this pipeline against the live pgvector corpus
without any chance of shadowing what the real weekly-SF's Step 8/9
Step writes and downstream consumers read.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest


def _run_main(argv, mock_s3):
    from rag.pipelines import filing_change_detection

    with (
        patch.object(sys, "argv", ["filing_change_detection.py", *argv]),
        patch.object(filing_change_detection, "compute_filing_changes", return_value=[]),
        patch("boto3.client", return_value=mock_s3),
    ):
        filing_change_detection.main()


class TestKeyPrefixDefaultsToCurrentBehavior:
    def test_no_key_prefix_writes_dated_key_and_latest(self):
        mock_s3 = MagicMock()
        _run_main(["--output-s3"], mock_s3)

        put_keys = [c.kwargs["Key"] for c in mock_s3.put_object.call_args_list]
        assert any(k.startswith("rag/filing_changes/") and k != "rag/filing_changes/latest.json" for k in put_keys)
        assert "rag/filing_changes/latest.json" in put_keys
        assert mock_s3.put_object.call_count == 2


class TestKeyPrefixIsolatesCanaryWrites:
    def test_key_prefix_scopes_the_dated_key(self):
        mock_s3 = MagicMock()
        _run_main(["--output-s3", "--key-prefix", "canary/run-123/"], mock_s3)

        put_keys = [c.kwargs["Key"] for c in mock_s3.put_object.call_args_list]
        assert any(k.startswith("rag/filing_changes/canary/run-123/") for k in put_keys)

    def test_key_prefix_never_writes_latest_json(self):
        mock_s3 = MagicMock()
        _run_main(["--output-s3", "--key-prefix", "canary/run-123/"], mock_s3)

        put_keys = [c.kwargs["Key"] for c in mock_s3.put_object.call_args_list]
        assert "rag/filing_changes/latest.json" not in put_keys
        assert mock_s3.put_object.call_count == 1


class TestResultJsonLine:
    def test_result_json_line_is_printed(self, capsys):
        mock_s3 = MagicMock()
        _run_main(["--output-s3", "--key-prefix", "canary/run-123/"], mock_s3)

        out = capsys.readouterr().out
        result_lines = [line for line in out.splitlines() if line.startswith("RESULT_JSON=")]
        assert len(result_lines) == 1

        import json
        payload = json.loads(result_lines[0][len("RESULT_JSON="):])
        assert payload["status"] == "OK"
        assert payload["n_analyzed"] == 0
