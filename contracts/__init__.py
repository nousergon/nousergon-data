"""
contracts/ — JSON Schema data contracts for inter-module communication.

The SLOT boundary schemas (signals = Slot R, predictions = Slot M) live in
``nousergon_lib.contracts`` (single source of truth since lib v0.59.x, M0 —
config#989); this package DELEGATES to the lib for those and keeps only the
``executor_params`` schema local (backtester→executor tuned-config boundary,
not a slot contract). Validation here is advisory — log warnings on mismatch,
never hard-fail.

Usage:
    from contracts import validate_signals, validate_predictions

    warnings = validate_signals(data)
    if warnings:
        logger.warning("Signals schema warnings: %s", warnings)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA_DIR = Path(__file__).parent


_LIB_HOSTED = {"signals", "predictions"}


def _load_schema(name: str) -> dict:
    if name in _LIB_HOSTED:
        # Slot contracts: single source of truth in alpha-engine-lib.
        from nousergon_lib.contracts import load_schema

        return load_schema(name)
    path = _SCHEMA_DIR / f"{name}.schema.json"
    with open(path) as f:
        return json.load(f)


def _validate(data: dict, schema_name: str) -> list[str]:
    """
    Validate data against a JSON Schema. Returns list of warning strings.
    Returns empty list if valid or if jsonschema is not installed.
    """
    try:
        import jsonschema
    except ImportError:
        return []

    schema = _load_schema(schema_name)
    validator = jsonschema.Draft202012Validator(schema)
    errors = list(validator.iter_errors(data))
    return [f"{e.json_path}: {e.message}" for e in errors[:10]]


def validate_signals(data: dict) -> list[str]:
    """Validate signals.json against contract schema."""
    return _validate(data, "signals")


def validate_predictions(data: dict) -> list[str]:
    """Validate predictions.json against contract schema."""
    return _validate(data, "predictions")


def validate_executor_params(data: dict) -> list[str]:
    """Validate executor_params.json against contract schema."""
    return _validate(data, "executor_params")


def validate_technicals(data: dict) -> list[str]:
    """Validate market_data/technicals/latest.json against contract schema (metron-ops#293:
    v3 additive `rating` object)."""
    return _validate(data, "technicals")


def validate_technical_ratings(data: dict) -> list[str]:
    """Validate market_data/intraday/technical_ratings.json against contract schema
    (metron-ops#293 — new artifact)."""
    return _validate(data, "technical_ratings")


def validate_rating_ledger_entry(data: dict) -> list[str]:
    """Validate a market_data/technicals/rating_history/{date}.json entry against
    contract schema (metron-ops#297 part 2 — new artifact)."""
    return _validate(data, "rating_ledger_entry")


def validate_rating_performance(data: dict) -> list[str]:
    """Validate market_data/technicals/rating_performance.json against contract schema
    (metron-ops#297 part 2 — new artifact, shared contract with the metron consumer,
    metron-ops#298)."""
    return _validate(data, "rating_performance")


def validate_arctic_probe(data: dict) -> list[str]:
    """Validate data_collection/probes/arctic/{trading_day}.json against contract
    schema (data-collector plan P-05, alpha-engine-config-I10748 — the ArcticDB
    in-region probe `collectors/arctic_probe.py` writes)."""
    return _validate(data, "arctic_probe")
