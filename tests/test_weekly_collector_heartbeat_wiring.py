"""Pins weekly_collector.main()'s flow-doctor heartbeat emit (config#646).

config#646 (Option A: dedicated emit_heartbeat() call sites) wires
flow-doctor's ``FlowDoctor.emit_heartbeat(bucket, *, prefix=None)`` into the
data-collector producing entrypoint. At end-of-run (after ``run_weekly()``
returns, before the exit-code decision) ``main()`` writes the flow's
``status()`` snapshot to
``s3://alpha-engine-research/_flow_doctor/heartbeat/data-collector/{date}.json``
so the dashboard System Health consumer — which reads heartbeats from the
**alpha-engine-research** bucket — can see the data flow's liveness.

Two layers of coverage:

- Behavioral: drive ``main()`` with ``_parse_args`` / ``load_config`` /
  ``DataPreflight`` / ``run_weekly`` stubbed and ``get_flow_doctor`` returning
  a MagicMock, then assert ``emit_heartbeat(bucket="alpha-engine-research")``
  fired exactly once. ``emit_heartbeat`` itself never runs (the accessor is a
  mock) so no S3/network call is made.
- Source guard: the emit must reference ``config["bucket"]`` (the research
  bucket) and be gated on a truthy flow-doctor instance, mirroring the
  soft-fail contract.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_COLLECTOR = _REPO_ROOT / "weekly_collector.py"

_RESEARCH_BUCKET = "alpha-engine-research"


def _main_source() -> str:
    tree = ast.parse(_COLLECTOR.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return ast.get_source_segment(_COLLECTOR.read_text(), node)
    raise AssertionError("weekly_collector.main() not found")


@pytest.fixture
def _stub_args():
    """A minimal args namespace: the phase-1 happy path with no special modes."""
    args = MagicMock()
    args.log_level = "INFO"
    args.config = "config.yaml"
    args.morning_enrich = False
    args.morning_arctic_append = False
    args.daily_arctic_append = False
    args.chronic_gap_heal = False
    args.daily = False
    args.preflight_only = False
    args.phase = 1
    return args


def test_main_emits_heartbeat_to_research_bucket(_stub_args):
    """main() calls fd.emit_heartbeat(bucket=<research bucket>) at end-of-run."""
    import weekly_collector

    fd = MagicMock()
    config = {"bucket": _RESEARCH_BUCKET}

    with patch.object(weekly_collector, "_parse_args", return_value=_stub_args), \
         patch.object(weekly_collector, "load_config", return_value=config), \
         patch("preflight.DataPreflight") as preflight, \
         patch.object(weekly_collector, "run_weekly", return_value={"status": "ok", "collectors": {}}), \
         patch.object(weekly_collector, "get_flow_doctor", return_value=fd):
        preflight.return_value.run.return_value = None
        weekly_collector.main()

    fd.emit_heartbeat.assert_called_once_with(bucket=_RESEARCH_BUCKET)


def test_main_skips_heartbeat_when_flow_doctor_inactive(_stub_args):
    """When get_flow_doctor() returns None, no emit is attempted (no crash)."""
    import weekly_collector

    config = {"bucket": _RESEARCH_BUCKET}

    with patch.object(weekly_collector, "_parse_args", return_value=_stub_args), \
         patch.object(weekly_collector, "load_config", return_value=config), \
         patch("preflight.DataPreflight") as preflight, \
         patch.object(weekly_collector, "run_weekly", return_value={"status": "ok", "collectors": {}}), \
         patch.object(weekly_collector, "get_flow_doctor", return_value=None):
        preflight.return_value.run.return_value = None
        # Must not raise (None guard); nothing to assert beyond a clean return.
        weekly_collector.main()


def test_heartbeat_emit_uses_research_bucket_expr():
    """Source guard: the emit references config['bucket'] (the research bucket)."""
    src = _main_source()
    assert "emit_heartbeat(bucket=config[\"bucket\"])" in src, (
        "main() must emit the heartbeat to config['bucket'] — the research "
        "bucket (RESEARCH_BUCKET default 'alpha-engine-research') the flow "
        "already writes manifests/health-markers to, and where the dashboard "
        "System Health consumer reads heartbeats from. See config#646."
    )


def test_heartbeat_emit_gated_on_flow_doctor_instance():
    """Source guard: emit is gated on a truthy flow-doctor instance AND the
    presence of the emit_heartbeat method (soft-fail + lib-version skew safe).

    ``hasattr(fd, "emit_heartbeat")`` is load-bearing: emit_heartbeat only
    exists in flow-doctor >=0.6.2 and the 5 producing repos deploy
    independently, so an older lib on a version-skewed box would AttributeError
    at end-of-run without it (config#646 lib-pin follow-up).
    """
    src = _main_source()
    assert "fd = get_flow_doctor()" in src
    assert 'if fd and hasattr(fd, "emit_heartbeat"):' in src, (
        "The emit_heartbeat call must be guarded by "
        '`if fd and hasattr(fd, "emit_heartbeat"):` so a disabled flow-doctor '
        "(get_flow_doctor() -> None) OR an older lib without emit_heartbeat "
        "(>=0.6.2 only) is a no-op — never an AttributeError in production."
    )
