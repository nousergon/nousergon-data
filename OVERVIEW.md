# alpha-engine-data — Code Index

> Index of entry points, key files, and data contracts. Companion to [README.md](README.md). System overview lives in [`alpha-engine-docs`](https://github.com/nousergon/nousergon-docs).

## Module purpose

Centralized data collection — price universe, macro, alternative data, features, RAG ingestion — for the Alpha Engine system.

## Entry points

| File | What it does |
|---|---|
| [`weekly_collector.py`](weekly_collector.py) | CLI for Phase 1 + Phase 2 — `--phase 1 / --phase 2 / --only <component>` |
| [`collectors/daily_closes.py`](collectors/daily_closes.py) | EOD weekday OHLCV capture for all tickers |
| [`rag/preflight.py`](rag/preflight.py) | RAG ingestion preflight — env vars, DB connectivity, freshness |
| [`rag/pipelines/run_weekly_ingestion.sh`](rag/pipelines/run_weekly_ingestion.sh) | Top-level shell driver for the RAG ingest pipelines |

## Where things live

| Concept | File |
|---|---|
| polygon.io rate-limited client | [`polygon_client.py`](polygon_client.py) |
| S&P constituents + GICS sectors | [`collectors/constituents.py`](collectors/constituents.py) |
| 10y OHLCV → ArcticDB | [`collectors/prices.py`](collectors/prices.py) |
| 2y slim cache (inference) | [`collectors/slim_cache.py`](collectors/slim_cache.py) |
| FRED macro + market breadth | [`collectors/macro.py`](collectors/macro.py) |
| Forward returns (full population) | [`collectors/universe_returns.py`](collectors/universe_returns.py) |
| Per-ticker alternative data (Phase 2) | [`collectors/alternative.py`](collectors/alternative.py) |
| Short interest (FINRA, bi-monthly) | [`collectors/short_interest.py`](collectors/short_interest.py) |
| Fundamentals | [`collectors/fundamentals.py`](collectors/fundamentals.py) |
| Signal-returns collector | [`collectors/signal_returns.py`](collectors/signal_returns.py) |
| Engineered feature store | [`features/feature_engineer.py`](features/feature_engineer.py) |
| Feature registry | [`features/registry.py`](features/registry.py) |
| Feature reader / writer | [`features/reader.py`](features/reader.py), [`features/writer.py`](features/writer.py) |
| Daily-append builder (ArcticDB) | [`builders/daily_append.py`](builders/daily_append.py) |
| Backfill builder | [`builders/backfill.py`](builders/backfill.py) |
| Delisted-ticker pruner | [`builders/prune_delisted_tickers.py`](builders/prune_delisted_tickers.py) |
| Price quality validator | [`validators/price_validator.py`](validators/price_validator.py) |
| Postflight validator | [`validators/postflight.py`](validators/postflight.py) |
| RAG ingestion pipelines | [`rag/pipelines/`](rag/pipelines/) |
| Per-step completion emails | [`emailer.py`](emailer.py) |
| Step Function preflight | [`sf_preflight.py`](sf_preflight.py) |
| SSM secret loader | [`ssm_secrets.py`](ssm_secrets.py) |

## Inputs / outputs

### Reads
| Source | Path |
|---|---|
| Promoted tickers for Phase 2 scope | `s3://alpha-engine-research/signals/{date}/signals.json` |

### Writes
| Destination | Path |
|---|---|
| 10y OHLCV universe | `s3://alpha-engine-research/arcticdb/universe/` |
| 2y inference slim cache | `s3://alpha-engine-research/arcticdb/universe_slim/` |
| Daily OHLCV staging (7-day lifecycle) | `s3://alpha-engine-research/staging/daily_closes/{date}.parquet` |
| Engineered features | `s3://alpha-engine-research/features/{date}/` |
| Weekly market data bundle | `s3://alpha-engine-research/market_data/weekly/{date}/` |
| Phase 1 completion marker | `s3://alpha-engine-research/health/data_phase1.json` |
| Universe returns table | `s3://alpha-engine-research/research.db` (`universe_returns`) |
| RAG corpus | Neon pgvector — `rag.documents`, `rag.chunks` (HNSW) |

## Run modes

| Mode | Where | Command |
|---|---|---|
| Production Phase 1 | EC2 SSM (always-on micro) | weekly Step Function |
| Production Phase 2 | Lambda | weekly Step Function |
| Production EOD | EC2 SSM (`ae-trading`) | EOD Step Function |
| Production RAG ingest | EC2 SSM | weekly Step Function |
| Local dry run | venv | `python weekly_collector.py --phase 1 --dry-run` |
| Single component | venv | `python weekly_collector.py --phase 1 --only macro` |

Deploy: `git push origin main && ae-dashboard "cd ~/alpha-engine-data && git pull"`. Health monitoring runs 6-hourly on the micro instance with SNS alerts on stale data.

## Tests

`pytest tests/` covers collectors, validators, ArcticDB roundtrips, daily-append backfill safety, RAG preflight, and Step Function preflight. No integration tests against live APIs in CI — those run manually pre-deploy.
