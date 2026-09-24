"""Retention for shadow ArcticDB libraries (`alpha-engine-config-I11447`).

Every shadow run seeds a full copy of each live library into
``shadow_{YYYYMMDD}_<name>`` (``shadow/arctic_seed.py``). The S3 half of a
shadow run, ``staging/shadow/``, expires after 7 days by bucket lifecycle. The
ArcticDB half had no retention: 5 dated sets, ~100k objects and 3.3 GB on
2026-09-23, growing ~0.65 GB per run.

A lifecycle rule on ``arcticdb/shadow_`` is the wrong tool. It would delete the
data and leave each library's entry in ``arcticdb/_arctic_cfg/``, so anything
that enumerates libraries would find libraries it cannot read. Removal goes
through ``Arctic.delete_library``, which removes both.

Two guards, both structural:

* Only names matching ``shadow_{8 digits}_<name>`` are ever candidates, and a
  candidate whose name is in ``LIVE_ARCTIC_LIBRARIES`` raises rather than being
  skipped. The pattern cannot match a live name today; the raise is for the day
  someone widens it.
* The default is a dry run. Deleting a library cannot be undone, so
  ``python -m shadow prune`` reports what it WOULD delete and changes nothing
  unless ``--apply`` is passed.

The window is 7 days from the stamp's own trading day, matching the
``staging/`` expiry that ``shadow/parity.py``'s D-1 regrade already relies on.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from shadow.root import LIVE_ARCTIC_LIBRARIES, ShadowGuardViolation

__all__ = ["DEFAULT_KEEP_DAYS", "SHADOW_LIBRARY_RE", "prune", "select_expired"]

DEFAULT_KEEP_DAYS = 7

#: ``shadow_20260914_universe`` → stamp ``20260914``. Mirrors
#: ``root.ARCTIC_SHADOW_PREFIX_TEMPLATE``.
SHADOW_LIBRARY_RE = re.compile(r"^shadow_(\d{8})_(.+)$")


def _stamp_date(name: str) -> "dt.date | None":
    match = SHADOW_LIBRARY_RE.match(name)
    if match is None:
        return None
    try:
        return dt.datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def select_expired(
    library_names: "list[str]", *, today: dt.date, keep_days: int = DEFAULT_KEEP_DAYS,
) -> "list[str]":
    """Shadow libraries whose stamp is more than ``keep_days`` before ``today``.

    A library with no parseable stamp is never selected: this function only
    removes what it can prove is an expired shadow copy.
    """
    if keep_days < 1:
        raise ValueError(f"keep_days must be >= 1, got {keep_days}")
    cutoff = today - dt.timedelta(days=keep_days)
    expired = []
    for name in sorted(library_names):
        stamp = _stamp_date(name)
        if stamp is None or stamp >= cutoff:
            continue
        if name in LIVE_ARCTIC_LIBRARIES:
            raise ShadowGuardViolation(
                f"refusing to prune {name!r}: it is a LIVE library ({sorted(LIVE_ARCTIC_LIBRARIES)})"
            )
        expired.append(name)
    return expired


def prune(
    arctic: Any, *, today: dt.date, keep_days: int = DEFAULT_KEEP_DAYS, apply: bool = False,
) -> dict:
    """Report, and with ``apply=True`` delete, the expired shadow libraries."""
    names = list(arctic.list_libraries())
    expired = select_expired(names, today=today, keep_days=keep_days)
    deleted = []
    if apply:
        for name in expired:
            arctic.delete_library(name)
            deleted.append(name)
    stamps = sorted({SHADOW_LIBRARY_RE.match(n).group(1) for n in expired})
    return {
        "apply": apply,
        "today": today.isoformat(),
        "keep_days": keep_days,
        "shadow_libraries": sum(1 for n in names if _stamp_date(n) is not None),
        "expired": expired,
        "expired_stamps": stamps,
        "deleted": deleted,
    }
