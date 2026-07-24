"""Shared EDGAR CIK lookup + process-level ``company_tickers.json`` cache.

config#2956 deliverable 5: the ~10k-entry CIK map
(``https://www.sec.gov/files/company_tickers.json``) was re-downloaded
2-3x per weekly-ingestion run because each of ``ingest_sec_filings.py``,
``ingest_8k_filings.py``, and ``ingest_form4.py`` kept its OWN
module-level ``_CIK_CACHE`` dict, and each pipeline step is a SEPARATE
``python -m`` invocation (see ``run_weekly_ingestion.sh``) — so the
per-process cache never survives past one step, and the full map was
fetched fresh 2-3 times per run for no reason (the map does not change
intra-run, or even intra-day).

Fix: a single shared loader, used by all three ingestors, backed by a
``/tmp`` file cache with an mtime-based TTL. The FIRST ingest step in a
run does the real download and writes the file cache; every subsequent
step in the same run (or the same day) reads the file cache instead of
hitting EDGAR again. Each caller still keeps its own in-memory
``_CIK_CACHE`` dict (unchanged — existing tests monkeypatch that
per-module attribute directly to force a fresh fetch), this module only
replaces what backs a COLD in-memory cache.

Cache location and TTL are both overridable so tests never touch the
real ``/tmp`` cache file or depend on wall-clock TTL behavior.
"""

from __future__ import annotations

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# One shared file cache location for all three ingestors — a run's first
# pipeline step populates it, every subsequent step (same run or same
# day) reads it instead of re-downloading. Overridable via env var so
# concurrent/test runs can point elsewhere.
DEFAULT_CACHE_PATH = os.environ.get(
    "CIK_CACHE_PATH", "/tmp/alpha_engine_company_tickers_cache.json"
)

# SEC republishes this file infrequently (new listings/tickers) — a
# same-day cache is always safe, and a run never spans more than a few
# hours, so this comfortably covers one run without risking staleness
# across days.
DEFAULT_TTL_SECONDS = 12 * 3600  # 12h


def load_cik_map(
    *,
    http=None,
    cache_path: str | None = None,
    ttl_seconds: float = DEFAULT_TTL_SECONDS,
    headers: dict | None = None,
    monotonic_time=time.time,
) -> dict[str, str]:
    """Return ``{TICKER: cik_str}`` for the full EDGAR universe.

    Reads a fresh-enough ``/tmp`` file cache if present; otherwise
    downloads ``company_tickers.json`` and writes the cache for the next
    process (same run or same day) to reuse. Returns ``{}`` (never
    raises) on any download/parse failure so a transient EDGAR/network
    blip degrades to "no CIK found" per-ticker rather than crashing the
    ingest step.

    ``cache_path`` defaults to the MODULE global ``DEFAULT_CACHE_PATH``
    looked up at call time (not bound at import time) so tests can
    isolate the file cache with
    ``monkeypatch.setattr(_cik_lookup, "DEFAULT_CACHE_PATH", str(tmp_path / "x.json"))``
    without needing every caller to thread a path through.
    """
    if cache_path is None:
        cache_path = DEFAULT_CACHE_PATH
    cached = _read_cache(cache_path, ttl_seconds, monotonic_time)
    if cached is not None:
        return cached

    if http is None:
        import requests as http  # local import: mirrors existing call sites

    try:
        resp = http.get(
            _COMPANY_TICKERS_URL,
            headers=headers or {"User-Agent": "AlphaEngine research@nousergon.ai"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(
                "[cik_lookup] company_tickers.json returned %d", resp.status_code
            )
            return {}
        data = resp.json()
    except Exception as e:
        logger.warning("[cik_lookup] CIK map download failed: %s", e)
        return {}

    cik_map: dict[str, str] = {}
    for entry in data.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik = str(entry.get("cik_str", ""))
        if ticker:
            cik_map[ticker] = cik

    _write_cache(cache_path, cik_map)
    return cik_map


def _read_cache(
    cache_path: str, ttl_seconds: float, monotonic_time
) -> dict[str, str] | None:
    try:
        stat = os.stat(cache_path)
    except OSError:
        return None
    age = monotonic_time() - stat.st_mtime
    if age < 0 or age > ttl_seconds:
        return None
    try:
        with open(cache_path, encoding="utf-8") as f:
            payload = json.load(f)
        cik_map = payload.get("cik_map")
        if not isinstance(cik_map, dict):
            return None
        return cik_map
    except Exception as e:
        logger.warning("[cik_lookup] failed to read CIK cache %s: %s", cache_path, e)
        return None


def _write_cache(cache_path: str, cik_map: dict[str, str]) -> None:
    try:
        tmp_path = f"{cache_path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"cik_map": cik_map}, f)
        os.replace(tmp_path, cache_path)
    except Exception as e:
        # Cache write is a pure optimization — never fail the ingest run
        # over a /tmp permissions or disk-space blip.
        logger.warning("[cik_lookup] failed to write CIK cache %s: %s", cache_path, e)
