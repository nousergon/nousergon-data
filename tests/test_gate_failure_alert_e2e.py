"""End-to-end synthetic gate-failure alert test (config#2459 scope item 4:
"a synthetic L1/L2/L3 gate failure is confirmed to actually alert via
flow-doctor end-to-end").

This wires together the REAL pieces:

  1. ``nousergon_lib.logging.setup_logging(..., flow_doctor_yaml=...)`` —
     the exact call every alpha-engine-data entrypoint makes at module-top
     (see ``tests/test_flow_doctor_wiring.py``, the direct precedent this
     test follows for constructing a real ``FlowDoctorHandler`` against a
     redirectable yaml with stubbed secrets).
  2. ``nousergon_lib.gate_alerts.alert_gate_failure`` — the new shared
     L1/L2/L3 helper (config#2459) — called exactly as L1/L2/L3 gate code
     would call it.
  3. The real ``flow_doctor.FlowDoctorHandler`` capturing the resulting
     ERROR record and enqueuing it for dispatch.

We mock ONLY the actual outbound dispatch boundary —
``flow_doctor.FlowDoctor.report`` (the method that would otherwise send a
real email / open a real GitHub issue) — matching the task's "mock the
flow-doctor dispatch boundary, assert it was called with the right shape"
instruction. Everything upstream of that boundary (setup_logging,
FlowDoctorHandler attachment, alert_gate_failure's log call, the handler's
enqueue + background-thread drain) is exercised for real.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _flow_doctor_available() -> bool:
    try:
        import flow_doctor  # noqa: F401
        return True
    except ImportError:
        return False


flow_doctor_required = pytest.mark.skipif(
    not _flow_doctor_available(),
    reason="flow-doctor not installed (pip install nousergon-lib[flow_doctor])",
)


@pytest.fixture
def reset_root_logger():
    root = logging.getLogger()
    saved = list(root.handlers)
    yield
    root.handlers = saved


@pytest.fixture
def stub_flow_doctor_env(monkeypatch):
    """Same stub-secrets shape as test_flow_doctor_wiring.py's fixture —
    non-empty placeholder values so flow-doctor's notifier preflight
    checks pass at construction time without contacting any real
    service (email/GitHub/Telegram).

    NOTE: this repo's flow-doctor.yaml DOES carry Telegram notifiers
    (config#645/#1741 — CRITICAL + OPS_HEALTH forum-topic routing,
    unconditional on every load, not gated on Telegram actually firing
    in a given test). flow-doctor fails loud on any unresolved ${VAR} at
    config-load time, so all four Telegram env vars must be seeded here
    too, mirroring test_flow_doctor_wiring.py's fixture exactly — a
    same-repo test using synthetic (non-CRITICAL) severities still
    resolves the whole config file, Telegram block included, at
    setup_logging() time."""
    monkeypatch.delenv("FLOW_DOCTOR_DISABLED", raising=False)
    monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "1")
    monkeypatch.setenv("FLOW_DOCTOR_SKIP_PREFLIGHT", "1")
    monkeypatch.setenv("EMAIL_SENDER", "test@example.com")
    monkeypatch.setenv("EMAIL_RECIPIENTS", "test@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "stub-password")
    monkeypatch.setenv("FLOW_DOCTOR_GITHUB_TOKEN", "stub-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:stub-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100stub")
    monkeypatch.setenv("FLOW_DOCTOR_TELEGRAM_THREAD_CRITICAL", "1")
    monkeypatch.setenv("FLOW_DOCTOR_TELEGRAM_THREAD_OPS_HEALTH", "2")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stub-anthropic-key")


@pytest.fixture
def temp_flow_doctor_yaml(tmp_path):
    import yaml as yamllib
    with open(REPO_ROOT / "flow-doctor.yaml") as f:
        cfg = yamllib.safe_load(f)
    cfg["store"] = {"type": "sqlite", "path": str(tmp_path / "flow_doctor_test.db")}
    yaml_path = tmp_path / "flow-doctor.yaml"
    with open(yaml_path, "w") as f:
        yamllib.safe_dump(cfg, f)
    return str(yaml_path)


@flow_doctor_required
def test_synthetic_gate_failure_reaches_flow_doctor_report(
    stub_flow_doctor_env, reset_root_logger, temp_flow_doctor_yaml,
):
    """The full chain: alert_gate_failure() -> logger.error() ->
    FlowDoctorHandler.emit() -> (background thread) -> FlowDoctor.report().

    Mocks only FlowDoctor.report (the dispatch boundary that would
    otherwise email/open-a-GitHub-issue) and asserts it fires with the
    gate-failure's layer/series/detail baked into the reported
    message/context — proving a synthetic L1/L2/L3 gate failure actually
    alerts via flow-doctor end-to-end, exactly as config#2459's closing
    condition requires.
    """
    from nousergon_lib.logging import setup_logging, get_flow_doctor
    from nousergon_lib.gate_alerts import alert_gate_failure
    import flow_doctor

    setup_logging(
        "data-collector-test-gate-failure-e2e",
        flow_doctor_yaml=temp_flow_doctor_yaml,
        exclude_patterns=[],
    )

    fd = get_flow_doctor()
    assert fd is not None, "flow-doctor singleton must be populated by setup_logging"

    mock_report = MagicMock(return_value="fake-report-id")
    fd.report = mock_report

    # The handler attached by setup_logging must be the real
    # FlowDoctorHandler wrapping this exact `fd` instance.
    attached = [
        h for h in logging.getLogger().handlers
        if isinstance(h, flow_doctor.FlowDoctorHandler)
    ]
    assert len(attached) == 1
    handler = attached[0]

    # ── Fire a SYNTHETIC L2 gate failure (this PR's own construction —
    # config#2456's real L2 code need not exist yet; alert_gate_failure's
    # contract is identical regardless of which layer calls it). ──
    alert_gate_failure(
        layer="L2",
        series="AAPL",
        detail="continuity gap: 2026-07-11 missing, expected trading day",
        severity="error",
    )

    # FlowDoctorHandler.emit() enqueues onto a background-thread queue;
    # shutdown() drains it synchronously so the assertion below is
    # deterministic rather than racing the worker thread.
    handler.shutdown(timeout=5)

    mock_report.assert_called_once()
    call_args, call_kwargs = mock_report.call_args
    reported_message = call_args[0] if call_args else call_kwargs.get("message")
    assert "[gate-failure]" in reported_message
    assert "layer=L2" in reported_message
    assert "series=AAPL" in reported_message
    assert "continuity gap" in reported_message
    assert call_kwargs.get("severity") == "error"


@flow_doctor_required
def test_synthetic_l1_l3_gate_failures_also_reach_flow_doctor_report(
    stub_flow_doctor_env, reset_root_logger, temp_flow_doctor_yaml,
):
    """Confirms the SAME shared helper (not a per-layer bespoke path)
    carries L1 and L3 synthetic failures through the identical chain —
    the whole point of a shared alert_gate_failure is that L1/L2/L3 all
    reach flow-doctor via one tested path rather than three."""
    from nousergon_lib.logging import setup_logging, get_flow_doctor
    from nousergon_lib.gate_alerts import alert_gate_failure
    import flow_doctor

    setup_logging(
        "data-collector-test-gate-failure-e2e-multi",
        flow_doctor_yaml=temp_flow_doctor_yaml,
        exclude_patterns=[],
    )
    fd = get_flow_doctor()
    mock_report = MagicMock(return_value="fake-report-id")
    fd.report = mock_report

    handler = next(
        h for h in logging.getLogger().handlers
        if isinstance(h, flow_doctor.FlowDoctorHandler)
    )

    alert_gate_failure(
        layer="L1", series="SPY",
        detail="cross-source disagreement: polygon=101.2 yfinance=98.7",
    )
    alert_gate_failure(
        layer="L3", series="NAV",
        detail="T+1 reconcile mismatch: broker=104213.55 internal=104198.02",
        severity="critical",
    )
    handler.shutdown(timeout=5)

    assert mock_report.call_count == 2
    messages = [
        (c.args[0] if c.args else c.kwargs.get("message"))
        for c in mock_report.call_args_list
    ]
    assert any("layer=L1" in m and "series=SPY" in m for m in messages)
    assert any("layer=L3" in m and "series=NAV" in m for m in messages)


@flow_doctor_required
def test_alert_gate_failure_warning_does_not_reach_flow_doctor_report(
    stub_flow_doctor_env, reset_root_logger, temp_flow_doctor_yaml,
):
    """Sub-ERROR severities must NOT reach flow-doctor's dispatch — this
    pins the intentional level-gating (FlowDoctorHandler is attached at
    level=ERROR) at the full end-to-end boundary, not just the unit-test
    boundary in test_gate_alerts.py."""
    from nousergon_lib.logging import setup_logging, get_flow_doctor
    from nousergon_lib.gate_alerts import alert_gate_failure
    import flow_doctor

    setup_logging(
        "data-collector-test-gate-failure-e2e-warning",
        flow_doctor_yaml=temp_flow_doctor_yaml,
        exclude_patterns=[],
    )
    fd = get_flow_doctor()
    mock_report = MagicMock(return_value="fake-report-id")
    fd.report = mock_report

    handler = next(
        h for h in logging.getLogger().handlers
        if isinstance(h, flow_doctor.FlowDoctorHandler)
    )

    alert_gate_failure(
        layer="L2", series="MSFT", detail="minor stale quote", severity="warning",
    )
    handler.shutdown(timeout=5)

    mock_report.assert_not_called()
