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


def validate_staging_daily_closes_row(data: dict) -> list[str]:
    """Validate ONE ROW of staging/daily_closes/{date}.parquet (post
    `df.reset_index().to_dict('records')`) against contract schema (data-collector
    plan P-16, alpha-engine-config-I10783 — `source` + `revision` columns)."""
    return _validate(data, "staging_daily_closes")


# ── P-07 (alpha-engine-config-I10774): Metron market-data spine, constituents /
# universe_classification, and the ArcticDB universe library row contract — every
# published key in plan §3's boundary table with a surviving consumer after phase 4.


def validate_metron_closes(data: dict) -> list[str]:
    """Validate market_data/eod_closes/{run_date,latest}.json (collect_metron_data)."""
    return _validate(data, "metron_closes")


def validate_metron_fx(data: dict) -> list[str]:
    """Validate market_data/fx/{run_date,latest}.json (collect_metron_data)."""
    return _validate(data, "metron_fx")


def validate_metron_close_history(data: dict) -> list[str]:
    """Validate market_data/close_history/{yf_symbol}.json (collect_history)."""
    return _validate(data, "metron_close_history")


def validate_metron_fx_history(data: dict) -> list[str]:
    """Validate market_data/fx_history/{CCY}.json (collect_history)."""
    return _validate(data, "metron_fx_history")


def validate_metron_sectors(data: dict) -> list[str]:
    """Validate market_data/sectors/latest.json (collect_reference)."""
    return _validate(data, "metron_sectors")


def validate_metron_earnings(data: dict) -> list[str]:
    """Validate market_data/earnings/latest.json (collect_reference)."""
    return _validate(data, "metron_earnings")


def validate_metron_macro(data: dict) -> list[str]:
    """Validate market_data/macro/latest.json (collect_macro)."""
    return _validate(data, "metron_macro")


def validate_metron_fundamentals(data: dict) -> list[str]:
    """Validate market_data/fundamentals/latest.json (collect_fundamentals)."""
    return _validate(data, "metron_fundamentals")


def validate_metron_security_performance(data: dict) -> list[str]:
    """Validate market_data/security_performance/latest.json (collect_security_performance)."""
    return _validate(data, "metron_security_performance")


def validate_metron_analyst(data: dict) -> list[str]:
    """Validate market_data/analyst/latest.json (collect_analyst)."""
    return _validate(data, "metron_analyst")


def validate_metron_sentiment(data: dict) -> list[str]:
    """Validate market_data/sentiment/latest.json (collect_sentiment)."""
    return _validate(data, "metron_sentiment")


def validate_metron_valuation_medians(data: dict) -> list[str]:
    """Validate market_data/valuation_medians/latest.json (collect_valuation_medians)."""
    return _validate(data, "metron_valuation_medians")


def validate_metron_intraday_latest(data: dict) -> list[str]:
    """Validate market_data/intraday/latest.json (collect_intraday)."""
    return _validate(data, "metron_intraday_latest")


def validate_constituents(data: dict) -> list[str]:
    """Validate market_data/weekly/{date}/constituents.json (collectors/constituents.py)."""
    return _validate(data, "constituents")


def validate_universe_classification(data: dict) -> list[str]:
    """Validate market_data/universe_classification/{run_date,latest}.json
    (collectors/universe_classification.py)."""
    return _validate(data, "universe_classification")


def validate_arctic_universe_row(data: dict) -> list[str]:
    """Validate one ArcticDB `universe` library symbol-frame row against the pinned
    column/dtype/index contract (crucible's consumer copy: contracts/arctic_universe.schema.json)."""
    return _validate(data, "arctic_universe")


def validate_crypto_holdings(data: dict) -> list[str]:
    """Validate crypto/holdings.json (collectors/crypto_balances.py::collect, D38).
    Consumer: Metron api/services/crypto.py (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "crypto_holdings")


def validate_inst_ownership_row(data: dict) -> list[str]:
    """Validate one row of data/inst_ownership/{quarter}/latest.parquet
    (data/derived/inst_ownership.py::InstOwnershipRow, D39). Consumer: crucible v2
    crucible/data/point_in_time.py::SnapshotPointInTimeSource._load_institutional
    (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "inst_ownership")


def validate_alternative_ticker(data: dict) -> list[str]:
    """Validate market_data/weekly/{date}/alternative/{ticker}.json
    (collectors/alternative.py::_process_one_ticker, D15). SELF-CONSUMED: the sole
    reader is this repo's features/compute.py::_load_cached_alternative /
    _alt_entry_from_payload (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "alternative_ticker")


def validate_news_digest_daily(data: dict) -> list[str]:
    """Validate data/news_digest_daily/{run_id,latest}.json
    (data/derived/news_digest.py::build_digest/write_digest, D36). Consumer
    (HARD requirement): morning-signal src/morning_signal/news_context.py::
    load_news_context (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "news_digest_daily")


def validate_news_article_row(data: dict) -> list[str]:
    """Validate one row of data/news_articles_daily/{run_id}_articles.parquet
    (data/derived/news_articles.py::NewsArticleRecord, D36). Consumer:
    crucible-dashboard views/Daily_News.py (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "news_article_row")


def validate_news_aggregate_row(data: dict) -> list[str]:
    """Validate one row of data/news_aggregates_daily/{run_id}.parquet
    (data/derived/news_aggregates.py::NewsTickerDailyAggregate, D36). Producer
    side only — live consumer is crucible-research (v1, alpha-engine-config-I10870,
    P-07)."""
    return _validate(data, "news_aggregate_row")


def validate_feature_registry(data: dict) -> list[str]:
    """Validate features/registry.json (features/registry.py::generate_registry_json,
    D12). Consumer: crucible-dashboard views/13_Feature_Store.py::_load_registry
    (alpha-engine-config-I10870, P-07)."""
    return _validate(data, "feature_registry")


def validate_fundamentals_snapshot(data: dict) -> list[str]:
    """Validate archive/fundamentals/{date}.json (collectors/fundamentals.py::collect,
    D10). Consumer: crucible-dashboard health_checker.py (`archive/fundamentals/`
    freshness check) (alpha-engine-config-I10873, P-07)."""
    return _validate(data, "fundamentals_snapshot")


def validate_rag_manifest(data: dict) -> list[str]:
    """Validate rag/manifest/{date,latest}.json (rag/pipelines/emit_manifest.py::
    build_manifest, D16). Consumer: crucible-dashboard loaders/s3_loader.py::
    load_rag_manifest (alpha-engine-config-I10873, P-07)."""
    return _validate(data, "rag_manifest")
