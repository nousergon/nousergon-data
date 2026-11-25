"""Tests for the overseer-policy invariant-4 alert-class detector."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"))
import alert_class_response_gap as g  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
PLAYBOOKS_PATH = REPO_ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
REAL_ALERT_CLASSES: list[dict[str, Any]] = yaml.safe_load(PLAYBOOKS_PATH.read_text())["alert_classes"]

_PAGEABLE_GAP: dict[str, Any] = {
    "class": "synthetic_pageable_class_no_disposition",
    "source": "synthetic-test-source",
    "severities": ["error"],
    "intake": "bus",
    "response": "drain-queue",
}


def test_real_registry_has_no_gaps() -> None:
    assert g.find_new_gaps(REAL_ALERT_CLASSES) == []
    assert g.GRANDFATHERED_CLASSES == frozenset()


def test_detects_pageable_class_with_no_disposition_field() -> None:
    assert g.find_new_gaps([_PAGEABLE_GAP]) == [
        "synthetic_pageable_class_no_disposition"
    ]


def test_removing_field_from_existing_row_is_detected() -> None:
    row = next(
        dict(alert_class)
        for alert_class in REAL_ALERT_CLASSES
        if alert_class["class"] == "backtester_stance_drift"
    )
    row.pop("migration_issue")
    assert g.find_new_gaps([row]) == ["backtester_stance_drift"]


def test_operator_reason_satisfies_disposition_requirement() -> None:
    fixed = dict(_PAGEABLE_GAP)
    fixed["operator_reason"] = "A human decision is required."
    assert g.find_new_gaps([fixed]) == []


def test_migration_issue_satisfies_disposition_requirement() -> None:
    fixed = dict(_PAGEABLE_GAP)
    fixed["migration_issue"] = "alpha-engine-config-I1"
    assert g.find_new_gaps([fixed]) == []


def test_nonpageable_classes_do_not_require_disposition() -> None:
    warning_only = {**_PAGEABLE_GAP, "severities": ["warning"]}
    automated = {**_PAGEABLE_GAP, "response": "playbook:alert-drain"}
    assert g.find_new_gaps([warning_only, automated]) == []


def test_main_exits_nonzero_when_registry_has_gap(
    tmp_path: Path, capsys: Any
) -> None:
    playbooks = tmp_path / "playbooks.yaml"
    playbooks.write_text(yaml.safe_dump({"alert_classes": [_PAGEABLE_GAP]}))
    assert g.main(["--playbooks", str(playbooks)]) == 1
    assert "synthetic_pageable_class_no_disposition" in capsys.readouterr().err


def test_main_exits_zero_when_registry_is_clean(
    tmp_path: Path, capsys: Any
) -> None:
    playbooks = tmp_path / "playbooks.yaml"
    fixed = {**_PAGEABLE_GAP, "migration_issue": "alpha-engine-config-I1"}
    playbooks.write_text(yaml.safe_dump({"alert_classes": [fixed]}))
    assert g.main(["--playbooks", str(playbooks)]) == 0
    assert "OK (1 classes checked)" in capsys.readouterr().out
