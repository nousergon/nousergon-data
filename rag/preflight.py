"""
RAG weekly ingestion preflight.

Called at the top of ``run_weekly_ingestion.sh`` before any of the five
ingestion pipelines run. Mirrors the DataPreflight pattern:
connectivity + env-var checks that fail fast and loud instead of
letting the Saturday pipeline run to "success" with empty RAG tables.

Run via ``python -m rag.preflight`` (or equivalent) — raises on any
missing requirement. Flow-doctor picks up the raised exception and
fires an email + GitHub issue.
"""

from __future__ import annotations

import logging
import os
import sys

from alpha_engine_lib.logging import setup_logging, guard_entrypoint
from alpha_engine_lib.preflight import BasePreflight

# Structured logging + flow-doctor singleton via alpha-engine-lib (shared
# pattern across all 5 entrypoints; see executor/main.py for reference).
# Module-top so any import-time error in BasePreflight or downstream
# pipelines invoked after preflight is captured by flow-doctor's ERROR
# handler. flow-doctor.yaml lives at the repo root (one dir above rag/).
_FLOW_DOCTOR_EXCLUDE_PATTERNS: list[str] = []
_FLOW_DOCTOR_YAML = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "flow-doctor.yaml",
)
setup_logging(
    "rag-preflight",
    flow_doctor_yaml=_FLOW_DOCTOR_YAML,
    exclude_patterns=_FLOW_DOCTOR_EXCLUDE_PATTERNS,
)

log = logging.getLogger(__name__)


class RAGPreflight(BasePreflight):
    """Preflight for the RAG weekly ingestion pipeline.

    Required env vars:
    - ``AWS_REGION`` — S3 client region (matches other modules)
    - ``VOYAGE_API_KEY`` — embedding provider for all 5 pipelines
    - ``FINNHUB_API_KEY`` — earnings transcript ingestion (step 3)
    - ``EDGAR_IDENTITY`` — SEC EDGAR User-Agent for filings (steps 1, 2)
    - ``RAG_DATABASE_URL`` — postgres+pgvector connection string (all pipelines)

    Required S3 access:
    - bucket reachable for `alpha-engine-research`
    """

    def __init__(self, bucket: str):
        super().__init__(bucket)

    def run(self) -> None:
        self.check_env_vars(
            "AWS_REGION",
            "VOYAGE_API_KEY",
            "FINNHUB_API_KEY",
            "EDGAR_IDENTITY",
            "RAG_DATABASE_URL",
        )
        self.check_s3_bucket()


def main() -> int:
    """CLI entrypoint invoked by run_weekly_ingestion.sh."""
    # setup_logging already ran at module-top (see comment near the
    # alpha_engine_lib.logging import).
    bucket = os.environ.get("ALPHA_ENGINE_BUCKET", "alpha-engine-research")
    RAGPreflight(bucket).run()
    log.info("RAG pre-flight OK")
    return 0


if __name__ == "__main__":
    # Capture an uncaught crash via flow-doctor before re-raising
    # (no-ops when flow-doctor is inactive).
    with guard_entrypoint():
        sys.exit(main())
