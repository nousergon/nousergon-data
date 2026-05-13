"""Derived signals — per-(ticker, date) aggregates computed from the
multi-source raw inputs.

Wave 1 of the institutional data revamp uses this package for:

  news_aggregates    — sentiment / event / entity rollup per ticker per day
                       (PR A.2 — this PR)
  revisions          — self-derived analyst estimate revisions from FMP
                       snapshots (PR C, deferred)
  insider_aggregates — Form 4 rollup per ticker (PR B)
  inst_ownership     — 13F rollup per ticker (PR B)

Each derived module:

1. Reads raw producer output (RAG corpus, FMP snapshots, parquet files).
2. Computes structured per-(ticker, date) rows.
3. Writes one parquet file per date under
   ``s3://alpha-engine-research/data/{slot}/{date}.parquet``.

Consumers (alpha-engine-research, backtester) read these parquets to
populate the snapshot — never re-aggregate from raw producer output.

See ``alpha-engine-docs/private/data-revamp-260513.md`` for full arc.
"""
