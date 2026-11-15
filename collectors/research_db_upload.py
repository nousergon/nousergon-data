"""The single writer of `research.db` and its dated backup.

**Why this module exists.** Until 2026-07-12 both keys were written by ONE
method — `crucible-research`'s `ArchiveManager.upload_db`, which uploaded
`research.db` and `backups/research_<date>.db` in the same call. That method's
only caller was deleted with the retired LangGraph pass
(`alpha-engine-config-I7827`), and the pointer acquired a DIFFERENT writer here
in `nousergon-data` while the dated backup acquired none.

The result was invisible for 59 days (`alpha-engine-config-I10202`): the
freshness row named `research_db_backup` watched `research.db`, whose new
writer kept it fresh, so the row read healthy while the thing it is named for
had stopped. The last dated object is `backups/research_20260710.db`.

So the fix is not "add an upload back somewhere" — it is to make the two keys
share a writer again, because a second writer for one of them is exactly how
they diverged. Every caller goes through :func:`upload_research_db`; there is
no supported path that writes the pointer alone.

**Key format.** New backups are ISO-dated — `backups/research_2026-09-12.db` —
so `ARTIFACT_REGISTRY.yaml` can express the key with its `{date}` placeholder
and give the series a real freshness check. Objects written before 2026-07-11
use the compressed `research_20260710.db` form; they are historical and are not
renamed. A registry template that cannot render the key it watches is a row
that reports UNMEASURED forever (`alpha-engine-config-I10200`), which is the
same class of failure this issue is about.

**Failure posture: RAISE.** Both uploads are producer writes, and the fleet
rule forbids graceful-degrade on a producer (`~/Development/CLAUDE.md`, "Fail
loud and fast"). The previous sites logged a WARNING and continued, so a failed
upload left the caller reporting success — which is how a 407 MB database can
stop being backed up without any run going red.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

#: The live pointer. Read by every consumer of the research database.
DB_KEY = "research.db"

#: The dated backup series. `{date}` is an ISO trading day (YYYY-MM-DD).
BACKUP_KEY_TEMPLATE = "backups/research_{date}.db"


def backup_key(run_date: str) -> str:
    """Return the dated backup key for ``run_date`` (ISO ``YYYY-MM-DD``)."""
    if not run_date:
        raise ValueError("run_date is required to name the dated backup")
    return BACKUP_KEY_TEMPLATE.format(date=run_date)


def upload_research_db(s3: Any, db_path: str, bucket: str, run_date: str) -> dict[str, str]:  # pyright: ignore[reportExplicitAny]  # untyped boto3 S3 client (no stubs); a Protocol rejects the caller's client
    """Upload ``db_path`` to BOTH the live pointer and the dated backup.

    Raises on either failure — see the module docstring. Returns the two keys
    written so a caller can record them in its own result envelope.
    """
    key = backup_key(run_date)
    s3.upload_file(db_path, bucket, DB_KEY)
    log.info("Uploaded research.db to s3://%s/%s", bucket, DB_KEY)
    s3.upload_file(db_path, bucket, key)
    log.info("Uploaded dated backup to s3://%s/%s", bucket, key)
    return {"pointer_key": DB_KEY, "backup_key": key}
