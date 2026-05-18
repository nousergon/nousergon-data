"""Pins the spot_drift_detection.sh --preflight-only Friday shell-run dry path.

Origin: ROADMAP "Friday shell-run — per-module dry-path activation" — this
closes the DriftDetection skip-exception (the one per-module SF step that
was still SKIPPED rather than dry-run on the Friday shell_run). Under the
Friday shell-run the DriftDetection spot must boot for real, run its
read-only preflight, then exit 0 with ZERO drift scan, ZERO external API
data fetch, and ZERO S3/CloudWatch/SNS/config writes — catching
bootstrap-class breakage (lib-pin drift, sys.path / sibling-clone
collision, missing dep, SSM/region env gap) ~12h before the real
Saturday run.

Static greps / source-position assertions, matching the convention of
tests/test_preflight_only_dry_path.py: the spot scripts only run
end-to-end on a real spot, so the assertions pin the load-bearing
invariant — --preflight-only is an early `exit 0` AFTER a read-only
preflight and strictly BEFORE the `monitoring.drift_detector` invocation
(the sole S3/SNS/scan code path) and before the CloudWatch heartbeat, so
no scan / fetch / write code path is reachable.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SPOT = _REPO_ROOT / "infrastructure" / "spot_drift_detection.sh"


@pytest.fixture(scope="module")
def spot_text() -> str:
    return _SPOT.read_text()


class TestSpotDriftDetectionPreflightOnly:
    def test_flag_parsed_as_modifier(self, spot_text):
        """--preflight-only sets PREFLIGHT_ONLY=1 (a modifier orthogonal
        to RUN_MODE, mirroring the #259 pattern), and PREFLIGHT_ONLY is
        initialised to 0 before the parse loop for `set -u` safety."""
        assert re.search(
            r'--preflight-only\)\s*PREFLIGHT_ONLY=1;\s*shift\s*;;',
            spot_text,
        ), "--preflight-only must set PREFLIGHT_ONLY=1 in the arg-parse loop"
        assert "PREFLIGHT_ONLY=0" in spot_text

    def test_preflight_only_block_precedes_drift_and_heartbeat(self, spot_text):
        """The PREFLIGHT_ONLY guard must come strictly BEFORE the
        "Full drift detection" section (the DRIFT heredoc) and the
        CloudWatch put-metric-data heartbeat — otherwise the scan +
        writes would already have happened, defeating the dry path.

        Anchored on the unique "# ── Full drift detection ──" section
        header rather than the `<<DRIFT` heredoc string, because that
        heredoc string is also mentioned verbatim in the header comment
        above the guard (which would yield a pre-guard match)."""
        i_guard = spot_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_drift = spot_text.index("# ── Full drift detection ────────────────────────────────────────────────────")
        i_heartbeat = spot_text.index("aws cloudwatch put-metric-data")

        assert i_guard < i_drift < i_heartbeat, (
            "Ordering must be: PREFLIGHT_ONLY guard → DRIFT heredoc → "
            f"CloudWatch heartbeat. Got guard={i_guard}, "
            f"drift={i_drift}, heartbeat={i_heartbeat}."
        )

    def test_preflight_only_block_exits_zero_before_drift(self, spot_text):
        """The PREFLIGHT_ONLY block must `exit 0` before the DRIFT
        heredoc so the scan/write path is unreachable, and a passed
        preflight is a clean (exit 0) outcome so SSM/SF report Success."""
        i_guard = spot_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_drift = spot_text.index("# ── Full drift detection ────────────────────────────────────────────────────")
        block = spot_text[i_guard:i_drift]
        assert "exit 0" in block, (
            "the PREFLIGHT_ONLY block must `exit 0` before the DRIFT heredoc"
        )

    def test_no_scan_or_write_in_preflight_only_block(self, spot_text):
        """Hard invariant: no drift scan, no S3/CW/SNS write, no real
        workload invocation inside the PREFLIGHT_ONLY block."""
        i_guard = spot_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_drift = spot_text.index("# ── Full drift detection ────────────────────────────────────────────────────")
        block = spot_text[i_guard:i_drift]

        assert "drift_detector --alert" not in block, (
            "the real drift scan (drift_detector --alert) must NOT run "
            "under --preflight-only"
        )
        assert "put-metric-data" not in block, (
            "CloudWatch heartbeat must NOT be emitted for a preflight"
        )
        assert "aws s3 cp" not in block, (
            "no S3 log/object upload inside the preflight-only block"
        )
        assert "aws sns" not in block, (
            "no SNS publish inside the preflight-only block"
        )

    def test_preflight_reuses_canonical_lib_base(self, spot_text):
        """The preflight must compose the canonical
        alpha_engine_lib.preflight.BasePreflight (env-vars + S3 HEAD,
        both read-only) — NO duplicated preflight scaffolding — plus an
        import-only smoke of the drift module (no scan invoked)."""
        i_guard = spot_text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then')
        i_drift = spot_text.index("# ── Full drift detection ────────────────────────────────────────────────────")
        block = spot_text[i_guard:i_drift]

        assert "from alpha_engine_lib.preflight import BasePreflight" in block, (
            "must reuse the canonical lib BasePreflight, not bespoke scaffolding"
        )
        assert "pf.check_env_vars(" in block
        assert "pf.check_s3_bucket()" in block
        assert 'importlib.import_module("monitoring.drift_detector")' in block, (
            "must import-smoke the drift module under the real PYTHONPATH"
        )
        # Import-only: the scan entrypoints must NOT be *called* in the block.
        assert "mod.check_drift(" not in block
        assert "mod.main(" not in block
