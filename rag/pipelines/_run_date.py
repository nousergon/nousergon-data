"""Resolve the cycle date a RAG weekly-ingestion producer keys its dated S3 objects by.

alpha-engine-config-I11514. ``emit_manifest`` and ``filing_change_detection``
used to key ``rag/manifest/{date}.json`` / ``rag/filing_changes/{date}.json``
by ``date.today()`` — the box's wall clock. The registry resolves ``{date}`` to
the Step Function's cycle date, so any RAGIngestion run that crossed midnight
UTC filed its output under the NEXT day and the stage-output sweep reported the
dated manifest missing on every weekly run.

The caller (``run_weekly_ingestion.sh --run-date``, fed by the SF's
``$.run_date``) now passes the cycle date explicitly. The wall-clock fallback
remains only for manual/local runs, and it is never silent: it logs a WARNING
naming the date it guessed.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone

logger = logging.getLogger(__name__)

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def resolve_run_date(value: str | None, *, producer: str) -> str:
    """Return the ``YYYY-MM-DD`` cycle date to key dated objects by.

    ``value`` is the caller's ``--run-date``. A malformed value raises
    ``ValueError`` (it would otherwise become an S3 key). ``None``/empty falls
    back to today's UTC date with a WARNING.
    """
    if value:
        if not _ISO_DATE_RE.match(value):
            raise ValueError(f"{producer}: --run-date must be YYYY-MM-DD, got {value!r}")
        # Rejects impossible calendar dates such as 2026-02-30.
        return date.fromisoformat(value).isoformat()

    fallback = datetime.now(timezone.utc).date().isoformat()
    logger.warning(
        "%s: no --run-date given — keying dated S3 objects by the UTC wall-clock date %s. "
        "This may not match the Step Function's cycle date if the run crossed midnight UTC "
        "(alpha-engine-config-I11514); pass --run-date from the caller.",
        producer,
        fallback,
    )
    return fallback
