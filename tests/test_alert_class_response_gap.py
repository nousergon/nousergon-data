"""Tests for the overseer-policy invariant-4 disposition detector."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"))
import alert_class_response_gap as g  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
PLAYBOOKS_PATH = REPO_ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
REAL_ALERT_CLASSES = yaml.safe_load(PLAYBOOKS_PATH.read_text())["alert_classes"]

_PAGEABLE_GAP = {
    "class": "synthetic_pageable_class_no_disposition",
    "source": "synthetic-test-source",
    "severities": ["error"],
    "intake": "bus",
    "response": "drain-queue",
}


def test_real_registry_has_no_gaps() -> None:
    """Every pageable class in the real registry has a disposition."""
    assert g.find_new_gaps(REAL_ALERT_CLASSES) == []


def test_grandfather_list_is_empty() -> None:
    assert g.GRANDFATHERED_CLASSES == frozenset()


def test_metron_deploy_drift_carries_migration_issue() -> None:
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    row = by_name["metron_deploy_drift"]
    assert row.get("migration_issue") == "alpha-engine-config-I8991"
    assert "operator_reason" not in row


def test_detects_pageable_class_with_no_disposition_field() -> None:
    assert g.find_new_gaps([_PAGEABLE_GAP]) == [
        "synthetic_pageable_class_no_disposition"
    ]


def test_removing_disposition_from_existing_row_is_detected() -> None:
    by_name = {c["class"]: c for c in REAL_ALERT_CLASSES}
    row = dict(by_name["metron_deploy"])
    row.pop("migration_issue", None)
    row.pop("operator_reason", None)
    assert g.find_new_gaps([row]) == ["metron_deploy"]


def test_adding_disposition_removes_gap() -> None:
    fixed = dict(_PAGEABLE_GAP)
    fixed["migration_issue"] = "alpha-engine-config-I1"
    assert g.find_new_gaps([fixed]) == []


def test_playbook_response_is_not_a_gap() -> None:
    routed = dict(_PAGEABLE_GAP)
    routed["response"] = "playbook:alert-drain"
    assert g.find_new_gaps([routed]) == []


def test_warning_only_class_is_not_a_gap() -> None:
    warning = dict(_PAGEABLE_GAP)
    warning["severities"] = ["warning"]
    assert g.find_new_gaps([warning]) == []


def test_main_exits_nonzero_when_registry_has_gap(tmp_path: Path) -> None:
    playbooks = tmp_path / "playbooks.yaml"
    playbooks.write_text(yaml.dump({"alert_classes": [_PAGEABLE_GAP]}))
    assert g.main(["--playbooks", str(playbooks)]) == 1


def test_main_exits_zero_when_registry_is_clean(tmp_path: Path) -> None:
    playbooks = tmp_path / "playbooks.yaml"
    fixed = dict(_PAGEABLE_GAP)
    fixed["operator_reason"] = "Synthetic test requires a human decision."
    playbooks.write_text(yaml.dump({"alert_classes": [fixed]}))
    assert g.main(["--playbooks", str(playbooks)]) == 0
