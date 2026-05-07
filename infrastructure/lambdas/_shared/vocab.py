"""Vendored controlled-vocab + validator for the system-wide changelog.

Shared by both auto-emit Lambdas in this directory:

- ``changelog-incident-mirror`` (SNS → S3)
- ``changelog-cloudwatch-mirror`` (CloudWatch Logs subscription filter → S3)

Source-of-truth: alpha-engine-config/changelog/vocab.yaml (private repo).
This module mirrors that file's enums as Python frozensets so the Lambdas
can validate without an extra runtime S3 read on the hot path. When the
upstream YAML changes (add a new enum value), bump SCHEMA_VERSION here +
re-deploy both Lambdas; CLAUDE.md's S3-contract rule (additive-only)
makes this safe — old values never disappear, so re-deploys land cleanly.

Closes ROADMAP P0 sub-item 3 (line ~2148) — auto-emit validation +
quarantine path. Entries failing validation are written to
``s3://alpha-engine-research/changelog/quarantine/{date}/{event_id}.json``
instead of ``changelog/entries/`` so the corpus + retro-mining filter
only see vocab-conforming entries; quarantine surface is reviewed by
operator (ROADMAP follow-on for dashboard tile).
"""

from __future__ import annotations

from typing import Any

SCHEMA_VERSION = "1.0.0"

# Frozenset → O(1) membership check. Mirrors vocab.yaml as of v1.0.0.
EVENT_TYPES: frozenset[str] = frozenset({
    "incident",
    "change",
    "recovery",
    "investigation",
    "regression_test_added",
    "prompt_version_change",
    "infrastructure_change",
    "eval_score_regression",
})

SEVERITIES: frozenset[str] = frozenset({
    "critical", "high", "medium", "low", "informational",
})

SUBSYSTEMS: frozenset[str] = frozenset({
    "retrieval", "agents", "predictor", "executor", "backtester",
    "dashboard", "research", "infrastructure", "prompts", "eval",
    "data_pipeline", "telemetry",
})

ROOT_CAUSE_CATEGORIES: frozenset[str] = frozenset({
    "data_quality", "model_behavior", "infrastructure_failure", "code_bug",
    "third_party_api", "prompt_regression", "schema_evolution", "configuration",
})

RESOLUTION_TYPES: frozenset[str] = frozenset({
    "code_fix", "prompt_revision", "config_change", "dependency_update",
    "architectural_refactor", "monitoring_added", "manual_intervention",
    "no_action_required",
})

# Required fields per event_type. Auto-emit defaults populate all of these
# for incident entries; investigation/change entries set them differently
# (out of scope for this Lambda — those land via the changelog-log CLI).
_INCIDENT_REQUIRED = ("event_id", "ts_utc", "event_type", "severity", "subsystem", "summary")


def validate_entry(entry: dict) -> list[str]:
    """Return a list of validation errors for ``entry`` (empty = valid).

    Checks:
    1. ``schema_version`` matches SCHEMA_VERSION (additive-only contract —
       a future schema bump means callers haven't been re-deployed).
    2. Required fields present + non-empty (incident entries only — other
       event_types validated by the changelog-log CLI before they land
       in this code path).
    3. Vocab-typed fields fall within their controlled enum (None tolerated
       for nullable fields like resolution_type that operators populate
       on follow-up).

    Errors are human-readable strings — operator reads them in the
    quarantine entry's ``validation_errors`` field.
    """
    errors: list[str] = []

    # 1. Schema version
    sv = entry.get("schema_version")
    if sv != SCHEMA_VERSION:
        errors.append(
            f"schema_version={sv!r} does not match Lambda's {SCHEMA_VERSION!r} "
            "(re-deploy needed if vocab.yaml changed)"
        )

    # 2. Required fields (incident entries — current Lambdas only emit incidents)
    if entry.get("event_type") == "incident":
        for field in _INCIDENT_REQUIRED:
            value = entry.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"required field {field!r} missing or empty")

    # 3. Vocab-typed fields
    _check_in_set(entry, "event_type", EVENT_TYPES, errors, required=True)
    _check_in_set(entry, "severity", SEVERITIES, errors, required=False)
    _check_in_set(entry, "subsystem", SUBSYSTEMS, errors, required=False)
    _check_in_set(entry, "root_cause_category", ROOT_CAUSE_CATEGORIES, errors, required=False)
    _check_in_set(entry, "resolution_type", RESOLUTION_TYPES, errors, required=False)

    return errors


def _check_in_set(
    entry: dict,
    field: str,
    allowed: frozenset[str],
    errors: list[str],
    *,
    required: bool,
) -> None:
    """Append a vocab-violation error for ``field`` if its value is set
    but outside ``allowed``. None tolerated unless ``required``."""
    value = entry.get(field)
    if value is None:
        if required:
            errors.append(f"required vocab field {field!r} missing")
        return
    if not isinstance(value, str):
        errors.append(
            f"vocab field {field!r} must be string, got {type(value).__name__}"
        )
        return
    if value not in allowed:
        errors.append(
            f"vocab field {field!r}={value!r} not in allowed set "
            f"({sorted(allowed)})"
        )


def is_valid(entry: dict) -> bool:
    """True iff ``validate_entry(entry)`` returns no errors."""
    return not validate_entry(entry)
