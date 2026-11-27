"""Tests for ``infrastructure/overseer/alert_class_response_gap.py`` — the
overseer-policy.md invariant 4 detector added by alpha-engine-config-I8991
("closed loop or tracked gap ... there is no third state").

Pins:
  1. The REAL registry has no gap (find_new_gaps == []).
  2. The grandfather list is empty and each formerly grandfathered class has
     a disposition ruling.
  3. ``metron_deploy_drift`` specifically carries
     ``migration_issue: alpha-engine-config-I8991``.
  4. The detector actually detects: a synthetic pageable class with neither
     field, not in the grandfather list, IS flagged — the "demonstrated
     failing" requirement for a self-detecting check. Without this test, a
     regression in ``is_pageable``/``find_new_gaps`` (e.g. a typo'd severity
     set) could silently stop catching real gaps and nothing would notice.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"))
import alert_class_response_gap as g  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
PLAYBOOKS_PATH = REPO_ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
REAL_ALERT_CLASSES = yaml.safe_load(PLAYBOOKS_PATH.read_text())["alert_classes"]


# ── real registry ──


def test_real_registry_has_no_gaps() -> None:
    """Every pageable class has an explicit disposition ruling."""
    assert g.find_new_gaps(REAL_ALERT_CLASSES) == []


def test_metron_deploy_drift_carries_migration_issue() -> None:
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    assert "metron_deploy_drift" in by_name
    row = by_name["metron_deploy_drift"]
    assert row.get("migration_issue") == "alpha-engine-config-I8991"
    assert "operator_reason" not in row  # exactly one of the two fields


def test_all_43_swept_classes_have_disposition_rulings() -> None:
    """The complete I8991 backlog is ruled on and no exception remains."""
    swept = {
        "alpha_engine_preopen_deploy_readiness_probe", "backtester_champion_promotion",
        "backtester_cost_anomaly", "backtester_live_key_reconciliation",
        "backtester_stance_drift", "boot_pull", "box_health", "canary_replay_liveness",
        "check_systemd_unit_drift", "data_constituents_drift", "data_phase_marker_sweep",
        "data_sf_definition_drift", "data_stage_output_sweep",
        "deploy_notification_backtester_concordance",
        "deploy_notification_backtester_counterfactual",
        "deploy_notification_backtester_health", "deploy_notification_changelog",
        "deploy_notification_data_plane", "deploy_notification_predictor",
        "deploy_notification_research", "deploy_on_merge", "eod_snapshot_existence_check",
        "executor_turnover_tripwire", "executor_zero_entries_alarm",
        "groom_lifecycle_bus_events", "llm_egress_proxy_sli", "llm_router_reload_health",
        "metron_deploy", "metron_reconciliation", "overseer_dispatch_escalation",
        "pipeline_watchdog_stuck_sf", "predictor_inference", "predictor_model_zoo",
        "predictor_predictions_write", "reboot_if_needed",
        "research_archive_writer_quarantine", "research_cut_promotion_alerts",
        "research_daemon_down", "research_eval_rolling_mean_producer_failure",
        "research_signals_envelope_rejected", "research_weekly_ledger_alerts",
        "router_canary", "weekly_sf_recovery_metric",
    }
    assert len(swept) == 43
    assert g.GRANDFATHERED_CLASSES == frozenset()
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    assert swept <= by_name.keys()
    assert all(g.has_disposition_field(by_name[name]) for name in swept)


# ── synthetic: proves the detector actually detects ──

_UNGRANDFATHERED_GAP_CLASS = {
    "class": "synthetic_new_pageable_class_no_disposition",
    "source": "synthetic-test-source",
    "severities": ["error"],
    "intake": "bus",
    "response": "drain-queue",
}


def test_detects_a_new_pageable_class_with_no_disposition_field() -> None:
    """A class shaped exactly like the real gaps found by the I8991 sweep —
    pageable severity, drain-queue response, no operator_reason/migration_issue
    — is flagged. This demonstrates the check
    failing when a class's field is missing/removed, per I8991's task 4."""
    assert _UNGRANDFATHERED_GAP_CLASS["class"] not in g.GRANDFATHERED_CLASSES
    gaps = g.find_new_gaps([_UNGRANDFATHERED_GAP_CLASS])
    assert gaps == ["synthetic_new_pageable_class_no_disposition"]


def test_removing_the_disposition_field_from_metron_deploy_drift_is_detected() -> None:
    """Directly demonstrates the 'field removed' scenario I8991 task 4 names:
    take the real metron_deploy_drift row, strip migration_issue, and confirm
    the detector flags it unconditionally."""
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    row = dict(by_name["metron_deploy_drift"])
    assert "metron_deploy_drift" not in g.GRANDFATHERED_CLASSES
    row.pop("migration_issue", None)
    row.pop("operator_reason", None)
    assert g.find_new_gaps([row]) == ["metron_deploy_drift"]


def test_adding_a_disposition_field_removes_the_gap() -> None:
    fixed = dict(_UNGRANDFATHERED_GAP_CLASS)
    fixed["operator_reason"] = "Declared human-only for this synthetic test row."
    assert g.find_new_gaps([fixed]) == []


def test_response_playbook_is_not_pageable_in_this_sense() -> None:
    """A class already routed to an automated T2 agent (response:playbook:*)
    is not in the third state — it has a closed loop, just not via the
    operator_reason/migration_issue mechanism."""
    routed = {
        "class": "already_routed_class",
        "source": "x", "severities": ["error"], "intake": "bus",
        "response": "playbook:alert-drain",
    }
    assert g.find_new_gaps([routed]) == []


def test_warning_only_severity_is_not_pageable() -> None:
    """A class whose severities never reach error/critical/dynamic never
    pushes per alerting.py's severity-is-the-push-switch contract, so it is
    not in the third state even with no disposition field."""
    warn_only = {
        "class": "warning_only_class",
        "source": "x", "severities": ["warning"], "intake": "bus",
        "response": "drain-queue",
    }
    assert g.find_new_gaps([warn_only]) == []


def test_main_exits_nonzero_when_registry_has_a_gap(tmp_path: Path) -> None:
    playbooks = tmp_path / "playbooks.yaml"
    playbooks.write_text(yaml.dump({
        "schema_version": 1,
        "playbooks": {},
        "alert_classes": [_UNGRANDFATHERED_GAP_CLASS],
    }))
    rc = g.main(["--playbooks", str(playbooks)])
    assert rc == 1


def test_main_exits_zero_when_registry_is_clean(tmp_path: Path) -> None:
    """A registry with a disposition on every pageable class exits zero."""
    playbooks = tmp_path / "playbooks.yaml"
    clean = dict(_UNGRANDFATHERED_GAP_CLASS)
    clean["migration_issue"] = "alpha-engine-config-I1"
    playbooks.write_text(yaml.dump({
        "schema_version": 1,
        "playbooks": {},
        "alert_classes": [clean],
    }))
    rc = g.main(["--playbooks", str(playbooks)])
    assert rc == 0
