"""Institutional ownership (13F) — quarterly QoQ deltas per ticker.

Wave 1 PR B of the institutional data-revamp arc. Builds a per-ticker
institutional-ownership snapshot from the SEC's official quarterly
Form 13F bulk data sets (free, authoritative, no vendor dependency).

SEC data source (corrected alpha-engine-config-I10529 — the old
``.../dera/data/form-13f-data-sets/{YYYYq1}/{YYYYq1}.zip`` scheme 404s for
every quarter; SEC moved this data set under ``structureddata`` and, from
2024 onward, files by three-month FILING WINDOW rather than calendar
quarter)::

    Index:  https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets
    File:   https://www.sec.gov/files/structureddata/data/form-13f-data-sets/{window}_form13f.zip

    where {window} is either a legacy calendar quarter (``2023q4``, through
    2023) or, from 2024 on, a filing window: ``01dec{Y-1}-28feb{Y}``,
    ``01mar{Y}-31may{Y}``, ``01jun{Y}-31aug{Y}``, ``01sep{Y}-30nov{Y}``
    (``29feb`` in a leap year).

The window filename is discovered from the index page rather than
constructed blind (deterministic construction is only the fallback when
the index page is unreachable) because a window's PUBLICATION date lags
its own end by weeks and the newest window is not knowable a priori.

Each window ZIP contains, tab-separated with a header row:

  SUBMISSION.tsv  — one row per filing: ACCESSION_NUMBER, FILING_DATE,
                    SUBMISSIONTYPE (``13F-HR``, ``13F-HR/A``, ...), CIK,
                    PERIODOFREPORT (the actual 13F "quarter" — NOT the
                    window's own date range, which mixes report periods).
  INFOTABLE.tsv   — individual holdings rows keyed by ACCESSION_NUMBER:
                    CUSIP, VALUE (USD, not thousands, since 2023-01-03 —
                    see FORM13F_readme.htm in the zip), SSHPRNAMT,
                    PUTCALL, etc.

A window's holdings therefore span more than one report period (mostly
the quarter-end 45 days before the window, plus late/amended filings for
earlier periods) — the report period actually wanted is
``SUBMISSION.PERIODOFREPORT``, joined to ``INFOTABLE`` via
``ACCESSION_NUMBER``.

Approach (per I2428 / I10529 scope):

1. Discover and download the newest 2 published window ZIPs.
2. Parse SUBMISSION + INFOTABLE from both; dedupe to one accession per
   (CIK, PERIODOFREPORT), an amendment (``.../A``) superseding the
   original for the same filer+period.
3. Select the 2 most recent PERIODOFREPORT dates present → current/prior.
4. Aggregate INFOTABLE per CUSIP for each selected period.
5. Resolve CUSIP → ticker via the OpenFIGI mapping API (cached in S3).
6. Compute QoQ share/value deltas + top-N concentration per ticker.
7. Write parquet to ``data/derived/inst_ownership/{quarter}/{ticker}.parquet``
   (one file per ticker for incremental reads, plus a quarterly aggregate).

S3 layout::

    s3://alpha-engine-research/data/inst_ownership/{YYYYQ1}/{ticker}.parquet
    s3://alpha-engine-research/data/inst_ownership/latest.json  (sidecar)

Design notes:

- 13F is 45-day delayed by regulation. The "current" quarter may be
  2 quarters in the past relative to today — this is inherent to the
  signal, not a bug.
- QoQ changes are computed against the most recent prior quarter
  available, skipping any gap quarter where SEC data isn't published.
- Options (put_call column) are excluded from the core holdings count
  and tracked separately.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
import time
import zipfile
from dataclasses import asdict, dataclass
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

import pandas as pd
import requests

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DEFAULT_S3_BUCKET = "alpha-engine-research"
DEFAULT_S3_PREFIX = "data/inst_ownership"

# The Scanner's universe-membership pointer (crucible-research/scoring/
# universe_membership.py). ``ranks`` carries the FULL scanned universe
# (~900 names), not the narrower feed cut ``rag/pipelines/_rag_scope.py``
# resolves for the RAG corpus — this producer covers the whole 13F-eligible
# universe, matching the module docstring's "our ~900-name universe".
MEMBERSHIP_LATEST_KEY = "universe_membership/latest.json"

SEC_13F_INDEX_URL = (
    "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
)
SEC_13F_BASE_URL = (
    "https://www.sec.gov/files/structureddata/data/form-13f-data-sets"
)

_MONTH_ABBR = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun",
    7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec",
}
_MONTH_NUM = {v: k for k, v in _MONTH_ABBR.items()}

# Regex for the 2024+ three-month filing-window filename, e.g.
# "01mar2026-31may2026_form13f.zip".
_WINDOW_FILENAME_RE = re.compile(
    r"^01([a-z]{3})(\d{4})-(\d{2})([a-z]{3})(\d{4})_form13f\.zip$"
)
# Regex for the legacy pre-2024 calendar-quarter filename, e.g.
# "2023q4_form13f.zip".
_LEGACY_QUARTER_FILENAME_RE = re.compile(r"^(\d{4})q([1-4])_form13f\.zip$")

# Delay between SEC HTTP requests (rate limiting courtesy).
_SEC_REQUEST_DELAY = 0.5

# Headers SEC requires for programmatic access.
_SEC_HEADERS = {
    "User-Agent": (
        "NousErgonResearch/1.0 "
        "(alpha-engine-research@nousergon.com; research use only)"
    ),
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

# Cache TTL for CUSIP→ticker crosswalk (days).
_CUSIP_CACHE_TTL_DAYS = 30

# 17 CFR 240.13f-1(a): an institutional investment manager must file a Form
# 13F within 45 days after the end of each calendar quarter. The crucible
# consumer (`crucible/data/point_in_time.py`) admits a quarter Q for a
# session S only when ``(quarter_end + 45 days) < S`` and never inspects
# FILING_DATE itself — it trusts this producer to have already excluded
# anything filed later. A submission (original or amendment) filed after
# this deadline is therefore a point-in-time / look-ahead leak into
# whatever session first admits the quarter, not merely late data
# (alpha-engine-config-I10733).
THIRTEEN_F_FILING_DEADLINE_DAYS = 45

# FORM13F_readme.htm (inside every SEC 13F bulk ZIP) documents a schema
# change effective this date: INFOTABLE.VALUE is reported in THOUSANDS of
# USD for submissions filed before 2023-01-03, and in whole USD from that
# date on. The scale is a property of the FILING, not the window file (a
# window can carry a late/amended legacy-period submission alongside
# current ones) — resolved per ACCESSION_NUMBER via SUBMISSION.FILING_DATE,
# never assumed from which window a row came from.
SEC_THIRTEEN_F_THOUSANDS_CUTOFF = Date(2023, 1, 3)

# Calendar quarter-end months/days a ``--report-period`` value must land on.
_QUARTER_END_MONTHS = (3, 6, 9, 12)

# OpenFIGI mapping API (alpha-engine-config-I10529). Keyless: 25 req/min,
# 10 jobs/request. With ``OPENFIGI_API_KEY`` (X-OPENFIGI-APIKEY header):
# 250 req/min, 100 jobs/request. https://www.openfigi.com/api
OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_KEYLESS_BATCH_SIZE = 10
OPENFIGI_KEYLESS_RATE_PER_MIN = 25
OPENFIGI_KEYED_BATCH_SIZE = 100
OPENFIGI_KEYED_RATE_PER_MIN = 250
OPENFIGI_API_KEY_ENV_VAR = "OPENFIGI_API_KEY"


# ═══════════════════════════════════════════════════════════════════
# Schema
# ═══════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class InstOwnershipRow:
    """Per-(ticker, quarter) institutional ownership snapshot.

    All numeric fields are optional — quarters when a ticker has zero
    institutional holdings remain possible (rare for our ~900-name
    universe but handled gracefully).
    """

    ticker: str
    quarter: str  # e.g. "2024Q1"
    schema_version: int

    n_funds_holding: int
    """Number of unique fund managers reporting this ticker."""

    total_shares_held: float
    """Aggregate shares held across all funds."""

    total_value_usd: float
    """Aggregate market value (USD)."""

    shares_qoq_change: float | None
    """QoQ share count change. Positive = new accumulation.
    None for the first observed quarter (no baseline)."""

    value_qoq_change: float | None
    """QoQ market value change (USD)."""

    top5_concentration_pct: float | None
    """% of total shares held by the top 5 funds for this ticker.
    None when fewer than 5 funds hold it."""

    n_funds_increasing: int
    """Funds that increased their position QoQ."""

    n_funds_decreasing: int
    """Funds that decreased their position QoQ."""

    n_funds_new: int
    """Funds that opened a new position this quarter."""

    n_funds_exited: int
    """Funds that fully exited this quarter."""

    put_call_ratio: float | None
    """Number of puts divided by calls for this ticker.
    >1 = bearish options positioning; <1 = bullish. None if no options
    reported or only one side is present."""


# ═══════════════════════════════════════════════════════════════════
# Universe resolution (alpha-engine-config-I10529)
# ═══════════════════════════════════════════════════════════════════


class UniverseUnavailable(RuntimeError):
    """The scanned universe could not be resolved from the membership pointer.

    Raised rather than falling back to a stale local list — a producer
    running on the wrong week's universe is a silent scope error, not a
    degraded run (PRODUCER-repo fail-loud default, AGENTS.md).
    """


class InstOwnershipPeriodUnavailable(RuntimeError):
    """A ``--report-period`` backfill could not produce data for the named
    period — the needed SEC window(s) aren't published, or nothing joined
    after the filing-date cutoff / universe filter.

    Historical mode (alpha-engine-config-I10733) fails loud and names the
    period rather than skipping it: a silently-skipped backfill quarter is
    indistinguishable from a quarter that legitimately has no institutional
    holders, and crucible's point-in-time consumer has no way to tell the
    difference between "not backfilled" and "genuinely empty".
    """


def load_universe_from_membership(
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    key: str = MEMBERSHIP_LATEST_KEY,
) -> list[str]:
    """Resolve the full scanned universe from the Scanner's membership
    pointer (``s3://{bucket}/{key}``, written by
    ``crucible-research/scoring/universe_membership.py``).

    Uses the ``ranks`` map (full scanned universe with a rankable
    attractiveness score, ~900 names) rather than the narrower
    ``cuts.<feed_cut>.tickers`` set ``rag/pipelines/_rag_scope.py`` reads for
    the RAG corpus — this producer's docstring targets "our ~900-name
    universe", the whole 13F-eligible board, not the top-N feed cut.

    Scheduled entry point for alpha-engine-config-I10529 (the producer had no
    trigger at all before this): avoids requiring a hand-maintained ticker
    file that goes stale the week the universe changes.
    """
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        payload = json.loads(obj["Body"].read())
    except Exception as e:
        raise UniverseUnavailable(
            f"universe membership artifact s3://{bucket}/{key} is missing or "
            f"unparseable ({type(e).__name__}: {e}). The Scanner writes it "
            f"every weekly-SF run; refusing to guess the universe."
        ) from e

    ranks = payload.get("ranks") or {}
    tickers = sorted({str(t).strip().upper() for t in ranks if str(t).strip()})
    if not tickers:
        raise UniverseUnavailable(
            f"membership artifact s3://{bucket}/{key} carries no non-empty "
            f"'ranks' map — cannot resolve a universe to process."
        )
    logger.info(
        "[inst_ownership] resolved %d tickers from s3://%s/%s (generated_at=%s)",
        len(tickers), bucket, key, payload.get("generated_at"),
    )
    return tickers


# ═══════════════════════════════════════════════════════════════════
# CUSIP → Ticker resolution
# ═══════════════════════════════════════════════════════════════════


def _load_cusip_cache(s3_client: Any | None, bucket: str) -> dict[str, str]:
    """Load the persisted CUSIP→ticker mapping from S3.

    Returns it regardless of ``_CUSIP_CACHE_TTL_DAYS`` age (CUSIP→ticker is
    stable enough that a stale-but-present entry beats no entry — it is
    still the substitutability fallback below when OpenFIGI is
    unreachable); only logs when the cache has aged past the TTL, as a
    signal a rebuild may be worth checking. Returns ``{}`` if there is no
    S3 client, no cache object, or it fails to parse.
    """
    if s3_client is None:
        return {}
    try:
        obj = s3_client.get_object(
            Bucket=bucket, Key="data/crosswalks/cusip_to_ticker.json"
        )
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        cached_date = Date.fromisoformat(payload.get("as_of", "2000-01-01"))
        if (Date.today() - cached_date).days >= _CUSIP_CACHE_TTL_DAYS:
            logger.info("cusip cache is older than %d days", _CUSIP_CACHE_TTL_DAYS)
        return payload.get("mapping", {})
    except Exception as e:
        logger.info("cusip cache unreadable (%s) — starting from empty cache", type(e).__name__)
    return {}


def _save_cusip_cache(
    mapping: dict[str, str], *, s3_client: Any, bucket: str,
) -> None:
    """Persist CUSIP→ticker mapping to S3."""
    payload = {
        "as_of": Date.today().isoformat(),
        "schema_version": 1,
        "mapping": mapping,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key="data/crosswalks/cusip_to_ticker.json",
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("cusip cache written (%d entries)", len(mapping))


class IdentifierMapper(Protocol):
    """Maps security identifiers (CUSIP) to tickers.

    One vendor = one implementation behind this protocol — a different
    mapping vendor is a new class, never a change to every call site
    (principles.md #8, substitutability).
    """

    def map_cusips(self, cusips: list[str]) -> dict[str, str]:
        """Return ``{cusip: ticker}`` for whichever cusips could be resolved.

        Silently omits cusips it could not resolve — callers treat a
        missing key as "unmapped", not an error.
        """
        ...


class CachedMapper:
    """Resolves CUSIPs against a preloaded mapping — no network.

    This is the substitutability fallback for ``OpenFigiMapper``: when the
    live API is down, rate-limited, or keyless-throttled, a run still
    produces from whatever was already resolved on a prior run.
    """

    def __init__(self, cache: dict[str, str]) -> None:
        self._cache = cache

    def map_cusips(self, cusips: list[str]) -> dict[str, str]:
        return {c: self._cache[c] for c in cusips if c in self._cache}


class _TokenBucket:
    """Token-bucket rate limiter: at most ``rate`` operations per
    ``per_seconds``. ``time_fn``/``sleep_fn`` are injectable so tests don't
    sleep on a real clock.
    """

    def __init__(
        self,
        rate: int,
        per_seconds: float,
        *,
        time_fn: Any = time.monotonic,
        sleep_fn: Any = time.sleep,
    ) -> None:
        self._rate = float(rate)
        self._per_seconds = per_seconds
        self._time_fn = time_fn
        self._sleep_fn = sleep_fn
        self._tokens = float(rate)
        self._last = time_fn()

    def acquire(self) -> None:
        now = self._time_fn()
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self._rate, self._tokens + elapsed * (self._rate / self._per_seconds))
        if self._tokens < 1.0:
            wait = (1.0 - self._tokens) * (self._per_seconds / self._rate)
            self._sleep_fn(wait)
            self._tokens = 0.0
            self._last = self._time_fn()
        else:
            self._tokens -= 1.0


class OpenFigiQuotaExhausted(RuntimeError):
    """OpenFIGI answered HTTP 429 for several consecutive batches.

    MEASURED 2026-09-13 (nousergon-data run 34773054198): keyless from a
    GitHub-hosted runner, EVERY batch answered 429 for 35 minutes until the
    job timeout — OpenFIGI's keyless quota (25 req/min) is per source IP and
    the runner egress IPs are shared across GitHub. A token bucket cannot
    help with a quota someone else has spent. The honest outcome is to stop
    after a few consecutive 429s and name the fix (an API key in SSM at
    ``/alpha-engine/OPENFIGI_API_KEY``), not to burn the job's timeout.
    """


#: Consecutive HTTP 429 answers after which the mapper gives up.
OPENFIGI_MAX_CONSECUTIVE_429 = 5


class OpenFigiMapper:
    """CUSIP→ticker resolution via the OpenFIGI mapping API.

    Keyless: 25 req/min, 10 jobs/request. With an API key
    (``X-OPENFIGI-APIKEY`` header, read from ``OPENFIGI_API_KEY`` by the
    caller): 250 req/min, 100 jobs/request. Keyless works, just slower —
    this must never be a hard dependency on the key existing.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        http_post: Any = None,
        batch_size: int | None = None,
        rate_limit_per_min: int | None = None,
        bucket: "_TokenBucket | None" = None,
    ) -> None:
        self.api_key = api_key
        self._post = http_post or self._default_post
        self.batch_size = batch_size or (
            OPENFIGI_KEYED_BATCH_SIZE if api_key else OPENFIGI_KEYLESS_BATCH_SIZE
        )
        rate = rate_limit_per_min or (
            OPENFIGI_KEYED_RATE_PER_MIN if api_key else OPENFIGI_KEYLESS_RATE_PER_MIN
        )
        self._bucket = bucket or _TokenBucket(rate, 60.0)

    @staticmethod
    def _default_post(url: str, *, json: Any, headers: dict[str, str], timeout: int):
        return requests.post(url, json=json, headers=headers, timeout=timeout)

    def map_cusips(self, cusips: list[str]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        unique = list(dict.fromkeys(c for c in cusips if c))
        consecutive_429 = 0
        for i in range(0, len(unique), self.batch_size):
            batch = unique[i:i + self.batch_size]
            self._bucket.acquire()
            jobs = [{"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"} for c in batch]
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["X-OPENFIGI-APIKEY"] = self.api_key
            try:
                resp = self._post(OPENFIGI_MAPPING_URL, json=jobs, headers=headers, timeout=30)
                if getattr(resp, "status_code", None) == 429:
                    consecutive_429 += 1
                    if consecutive_429 >= OPENFIGI_MAX_CONSECUTIVE_429:
                        raise OpenFigiQuotaExhausted(
                            f"OpenFIGI answered HTTP 429 for {consecutive_429} consecutive "
                            f"batches ({'keyed' if self.api_key else 'keyless'} mode). The "
                            f"keyless quota is per source IP and is exhausted on shared "
                            f"egress; set an API key in SSM at /alpha-engine/OPENFIGI_API_KEY "
                            f"(free at https://www.openfigi.com/api) and grant the role "
                            f"ssm:GetParameter on it."
                        )
                    logger.warning(
                        "OpenFIGI 429 for batch of %d cusips (%d consecutive)",
                        len(batch), consecutive_429,
                    )
                    continue
                resp.raise_for_status()
                results = resp.json()
                consecutive_429 = 0
            except OpenFigiQuotaExhausted:
                raise
            except Exception as e:
                logger.warning(
                    "OpenFIGI mapping request failed for batch of %d cusips: %s: %s",
                    len(batch), type(e).__name__, e,
                )
                continue
            for cusip, result in zip(batch, results):
                data = (result or {}).get("data") or []
                if data:
                    ticker = data[0].get("ticker")
                    if ticker:
                        mapping[cusip] = str(ticker).upper()
        return mapping


def build_cusip_to_ticker(
    cusips: set[str] | list[str],
    *,
    s3_client: Any | None = None,
    bucket: str = DEFAULT_S3_BUCKET,
    force_rebuild: bool = False,
    api_key: str | None = None,
    mapper: "IdentifierMapper | None" = None,
) -> dict[str, str]:
    """Resolve ``{cusip: ticker}`` for the given CUSIPs — the CUSIPs that
    actually appear in the SEC INFOTABLE rows being processed, not the
    universe tickers (the mapping direction the filings carry is
    CUSIP→ticker, not the reverse).

    Reads the persisted S3 crosswalk cache first (``CachedMapper``,
    ``force_rebuild`` bypasses it) and only queries OpenFIGI
    (``OpenFigiMapper`` by default, or ``mapper`` for tests/other
    vendors) for whatever the cache doesn't already have. The cache is the
    substitutability fallback: a run with OpenFIGI down still produces
    from cached mappings, and logs how many CUSIPs stayed unmapped.
    """
    cusip_list = sorted({c for c in cusips if c})
    cache = {} if force_rebuild else _load_cusip_cache(s3_client, bucket)
    mapping = dict(CachedMapper(cache).map_cusips(cusip_list))
    missing = [c for c in cusip_list if c not in mapping]

    resolved_count = 0
    if missing:
        live_mapper = mapper or OpenFigiMapper(api_key=api_key)
        resolved = live_mapper.map_cusips(missing)
        mapping.update(resolved)
        resolved_count = len(resolved)

    unmapped = len(cusip_list) - len(mapping)
    logger.info(
        "cusip→ticker: %d total, %d from cache, %d resolved via OpenFIGI, %d unmapped",
        len(cusip_list), len(cusip_list) - len(missing), resolved_count, unmapped,
    )

    if s3_client is not None and mapping:
        merged_cache = {**cache, **mapping}
        _save_cusip_cache(merged_cache, s3_client=s3_client, bucket=bucket)

    return mapping


# ═══════════════════════════════════════════════════════════════════
# SEC bulk data download and parse
# ═══════════════════════════════════════════════════════════════════


def _quarter_str_for_date(d: Any) -> str:
    """``Date(2024, 3, 15)`` → ``"2024Q1"``. Accepts a ``date``, a
    ``datetime``, or a ``pandas.Timestamp`` (all expose ``.year``/``.month``).
    """
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}Q{quarter}"


def _is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _quarter_end_day(month: int) -> int:
    """The last calendar day of a quarter-end month (3/6/9/12)."""
    return 31 if month in (3, 12) else 30


def _is_quarter_end(d: Date) -> bool:
    """True iff ``d`` is exactly a calendar quarter-end date (03-31, 06-30,
    09-30, 12-31) — the only dates ``--report-period`` may name (I10733)."""
    return d.month in _QUARTER_END_MONTHS and d.day == _quarter_end_day(d.month)


def _previous_quarter_end(d: Date) -> Date:
    """The quarter-end date immediately before ``d`` (``d`` itself must
    already be a quarter-end — callers validate with ``_is_quarter_end``
    first)."""
    idx = _QUARTER_END_MONTHS.index(d.month)
    if idx == 0:
        return Date(d.year - 1, 12, 31)
    prev_month = _QUARTER_END_MONTHS[idx - 1]
    return Date(d.year, prev_month, _quarter_end_day(prev_month))


def _window_filename(year: int, idx: int) -> str:
    """Deterministically construct the SEC filing-window filename for
    window ``idx`` (0=Dec-Feb, 1=Mar-May, 2=Jun-Aug, 3=Sep-Nov) ending
    within calendar ``year`` (the Dec-Feb window's START month is
    ``year - 1``).
    """
    if idx == 0:
        start_y, start_m, start_d = year - 1, 12, 1
        end_d = 29 if _is_leap_year(year) else 28
        end_y, end_m = year, 2
    elif idx == 1:
        start_y, start_m, start_d = year, 3, 1
        end_y, end_m, end_d = year, 5, 31
    elif idx == 2:
        start_y, start_m, start_d = year, 6, 1
        end_y, end_m, end_d = year, 8, 31
    elif idx == 3:
        start_y, start_m, start_d = year, 9, 1
        end_y, end_m, end_d = year, 11, 30
    else:
        raise ValueError(f"window idx must be 0-3, got {idx}")
    start = f"{start_d:02d}{_MONTH_ABBR[start_m]}{start_y}"
    end = f"{end_d:02d}{_MONTH_ABBR[end_m]}{end_y}"
    return f"{start}-{end}_form13f.zip"


def _window_index_for_date(d: Date) -> tuple[int, int]:
    """Return ``(year, idx)`` for the filing window containing date ``d``,
    using ``_window_filename``'s ``(year, idx)`` convention (``year`` is
    the window's END year).
    """
    m = d.month
    if m == 12:
        return (d.year + 1, 0)
    if m in (1, 2):
        return (d.year, 0)
    if m in (3, 4, 5):
        return (d.year, 1)
    if m in (6, 7, 8):
        return (d.year, 2)
    return (d.year, 3)  # 9, 10, 11


def _prior_window(year: int, idx: int) -> tuple[int, int]:
    """Return the ``(year, idx)`` of the window immediately before the
    given one."""
    if idx == 0:
        return (year - 1, 3)
    return (year, idx - 1)


def _recent_window_filenames(d: Date, count: int) -> list[str]:
    """Deterministic fallback: the ``count`` most recent window filenames
    walking backward from (and including) the window containing ``d``,
    most-recent-first. Used only when the SEC index page can't be fetched
    or parsed — the index page is the primary source of truth since a
    window's publication date isn't knowable from the calendar alone.
    """
    year, idx = _window_index_for_date(d)
    names = []
    for _ in range(count):
        names.append(_window_filename(year, idx))
        year, idx = _prior_window(year, idx)
    return names


def _window_end_date(filename: str) -> Date | None:
    """Return the sort key (end date) for a SEC 13F zip filename — either
    the 2024+ window naming or the legacy pre-2024 calendar-quarter naming.
    Returns ``None`` for anything else found on the index page.
    """
    m = _WINDOW_FILENAME_RE.match(filename)
    if m:
        end_day = int(m.group(3))
        end_month = _MONTH_NUM.get(m.group(4))
        end_year = int(m.group(5))
        if end_month is None:
            return None
        try:
            return Date(end_year, end_month, end_day)
        except ValueError:
            return None
    m2 = _LEGACY_QUARTER_FILENAME_RE.match(filename)
    if m2:
        year = int(m2.group(1))
        q = int(m2.group(2))
        end_month = q * 3
        end_day = 31 if end_month in (3, 12) else 30
        return Date(year, end_month, end_day)
    return None


def _window_date_range(filename: str) -> tuple[Date, Date] | None:
    """Parse the ``(start, end)`` filing-window date range directly from a
    SEC 13F bulk zip filename — legacy calendar-quarter or 2024+ windowed
    naming — for the historical backfill's window-selection-by-date-overlap
    (I10733). Parses the filename's own tokens rather than reconstructing
    from ``_window_filename``/``_window_index_for_date``, which only know
    the regular Dec-Feb/Mar-May/Jun-Aug/Sep-Nov cadence: the real SEC index
    carries an odd transitional window (``01jan2024-29feb2024_form13f.zip``,
    2 months, measured live 2026-09-14) that a deterministic reconstruction
    would never produce. Returns ``None`` for anything unrecognized.
    """
    m = _WINDOW_FILENAME_RE.match(filename)
    if m:
        start_month = _MONTH_NUM.get(m.group(1))
        start_year = int(m.group(2))
        end_day = int(m.group(3))
        end_month = _MONTH_NUM.get(m.group(4))
        end_year = int(m.group(5))
        if start_month is None or end_month is None:
            return None
        try:
            return (Date(start_year, start_month, 1), Date(end_year, end_month, end_day))
        except ValueError:
            return None
    m2 = _LEGACY_QUARTER_FILENAME_RE.match(filename)
    if m2:
        year = int(m2.group(1))
        q = int(m2.group(2))
        start_month = (q - 1) * 3 + 1
        end_month = q * 3
        return (Date(year, start_month, 1), Date(year, end_month, _quarter_end_day(end_month)))
    return None


def _windows_overlapping_range(candidates: list[str], lo: Date, hi: Date) -> list[str]:
    """Filenames from ``candidates`` whose parsed date range overlaps the
    inclusive ``[lo, hi]`` filing-date range. Inclusive at both ends
    deliberately: a filing dated exactly on a window boundary (e.g. the
    quarter-end date itself) must not fall through the selection net —
    the true cutoff enforcement is ``_apply_filing_cutoff``, not this
    coarse window-selection pass.
    """
    out = []
    for name in candidates:
        rng = _window_date_range(name)
        if rng is None:
            continue
        start, end = rng
        if start <= hi and end >= lo:
            out.append(name)
    return out


def _filing_ranges_for_period(current_period: Date, prior_period: Date) -> list[tuple[Date, Date]]:
    """The two on-time-filing date ranges (prior, prior+45d] and
    (current, current+45d] whose SEC windows must be downloaded to cover
    both report periods' filings (I10733)."""
    deadline = timedelta(days=THIRTEEN_F_FILING_DEADLINE_DAYS)
    return [
        (prior_period, prior_period + deadline),
        (current_period, current_period + deadline),
    ]


def _windows_needed_for_period(
    current_period: Date, prior_period: Date, *, index_html: str | None = None,
) -> list[str]:
    """The sorted set of SEC 13F window filenames that must be downloaded
    to cover both ``current_period`` and ``prior_period``'s on-time filing
    ranges. Fetches the live index page (or uses a caller-supplied
    ``index_html``, e.g. to fetch it once per CLI invocation across many
    periods) rather than deterministic construction — the index page is
    the only source that also carries pre-2024 legacy quarters and the
    2024 transition window.

    Raises ``RuntimeError`` when the index page is unreachable or no
    published window covers a needed range — historical backfill fails
    loud rather than silently skipping a period (I10733 / AGENTS.md
    fail-loud PRODUCER default).
    """
    html = index_html if index_html is not None else _fetch_sec_index_html()
    if not html:
        raise RuntimeError(
            "SEC 13F index page unreachable — cannot resolve which window "
            f"file(s) cover report periods {current_period.isoformat()} / "
            f"{prior_period.isoformat()}"
        )
    all_names = discover_window_filenames_from_html(html)
    needed: set[str] = set()
    for lo, hi in _filing_ranges_for_period(current_period, prior_period):
        matches = _windows_overlapping_range(all_names, lo, hi)
        if not matches:
            raise RuntimeError(
                f"no published SEC 13F window covers on-time filings for "
                f"{lo.isoformat()} through {hi.isoformat()} "
                f"({THIRTEEN_F_FILING_DEADLINE_DAYS}-day deadline) — cannot "
                f"resolve report periods {current_period.isoformat()} / "
                f"{prior_period.isoformat()}"
            )
        needed |= set(matches)
    return sorted(needed)


def _download_windows(
    filenames: list[str], *, cache: dict[str, zipfile.ZipFile] | None = None,
) -> list[tuple[str, zipfile.ZipFile]]:
    """Download each named window, deduped by filename via ``cache`` so the
    same window (needed by two adjacent report periods in one backfill
    invocation) is never fetched twice. Raises ``RuntimeError`` naming the
    filename if a window ``_windows_needed_for_period`` said was required
    turns out unpublished (HTTP 404) — historical mode never silently
    drops a needed window (I10733).
    """
    if cache is None:
        cache = {}
    out: list[tuple[str, zipfile.ZipFile]] = []
    fresh_downloads = 0
    for name in filenames:
        if name not in cache:
            if fresh_downloads:
                time.sleep(_SEC_REQUEST_DELAY)
            zf = _download_sec_bulk_zip(name)
            if zf is None:
                raise RuntimeError(
                    f"SEC 13F window {name} is required to cover a needed "
                    "on-time filing range but is not published (HTTP 404)"
                )
            cache[name] = zf
            fresh_downloads += 1
        out.append((name, cache[name]))
    return out


def discover_window_filenames_from_html(html: str) -> list[str]:
    """Parse the SEC 13F index page for ``*_form13f.zip`` filenames,
    returning them most-recent-first (by parsed end date). Filenames the
    module doesn't recognize (any future naming change) are skipped rather
    than raising — this only degrades to fewer discovered candidates, and
    the deterministic fallback still runs if discovery yields nothing.
    """
    names = set(re.findall(r"form-13f-data-sets/([A-Za-z0-9_.\-]+\.zip)", html))
    dated = [(n, _window_end_date(n)) for n in names]
    dated = [(n, d) for n, d in dated if d is not None]
    dated.sort(key=lambda pair: pair[1], reverse=True)
    return [n for n, _ in dated]


def _fetch_sec_index_html() -> str | None:
    """Fetch the SEC 13F data-sets index page HTML, or ``None`` on any
    failure (network, non-2xx, timeout) — callers fall back to
    deterministic window-name construction."""
    try:
        resp = requests.get(SEC_13F_INDEX_URL, headers=_SEC_HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(
            "SEC 13F index page fetch failed (%s): %s: %s",
            SEC_13F_INDEX_URL, type(e).__name__, e,
        )
        return None


def _candidate_window_filenames(*, today: Date | None = None, count: int = 8) -> list[str]:
    """The ``count`` most-recent-first candidate window filenames to try
    downloading. Discovers from the live SEC index page first; falls back
    to deterministic construction (walking backward from the window
    containing ``today``) only if the index page is unreachable or yields
    no recognizable filenames.
    """
    html = _fetch_sec_index_html()
    if html:
        discovered = discover_window_filenames_from_html(html)
        if discovered:
            return discovered[:count]
        logger.warning(
            "SEC 13F index page fetched but no recognizable window "
            "filenames found — falling back to deterministic construction"
        )
    return _recent_window_filenames(today or Date.today(), count)


def _user_agent() -> dict[str, str]:
    """SEC-mandated User-Agent for bulk data downloads."""
    return dict(_SEC_HEADERS)


def _download_sec_bulk_zip(filename: str) -> zipfile.ZipFile | None:
    """Download one SEC Form 13F filing-window bulk ZIP from SEC.gov by
    its exact filename (e.g. ``"01mar2026-31may2026_form13f.zip"``).

    Streams to a temp file rather than buffering in memory — these ZIPs
    run ~100-400MB and a GitHub-hosted runner shouldn't hold that in RAM.
    Returns a ``ZipFile`` opened on the temp path, or ``None`` if the
    window isn't published yet (HTTP 404) or the request otherwise failed.
    """
    url = f"{SEC_13F_BASE_URL}/{filename}"
    try:
        with requests.get(url, headers=_SEC_HEADERS, timeout=120, stream=True) as resp:
            status = resp.status_code
            if status == 404:
                logger.info("SEC 13F bulk not published: %s (HTTP 404)", url)
                return None
            resp.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(suffix=f"-{filename}", delete=False)
            total = 0
            try:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        tmp.write(chunk)
                        total += len(chunk)
            finally:
                tmp.close()
        logger.info(
            "downloaded SEC 13F bulk %s: HTTP %d, %d bytes -> %s",
            filename, status, total, tmp.name,
        )
        return zipfile.ZipFile(tmp.name)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        logger.warning("SEC 13F bulk not available for %s (HTTP %s): %s", filename, status, e)
        return None
    except Exception as e:
        logger.warning("SEC 13F bulk download failed for %s: %s: %s", filename, type(e).__name__, e)
        return None


def _download_recent_windows(
    *, count: int = 2, max_candidates: int = 8,
) -> list[tuple[str, zipfile.ZipFile]]:
    """Discover and download the ``count`` newest published window ZIPs,
    skipping unpublished (404) candidates. Respects SEC's rate-limiting
    courtesy ask (``_SEC_REQUEST_DELAY`` between requests, well under the
    documented 10 req/s).
    """
    candidates = _candidate_window_filenames(count=max_candidates)
    downloaded: list[tuple[str, zipfile.ZipFile]] = []
    for i, name in enumerate(candidates):
        if len(downloaded) >= count:
            break
        if i > 0:
            time.sleep(_SEC_REQUEST_DELAY)
        zf = _download_sec_bulk_zip(name)
        if zf is not None:
            downloaded.append((name, zf))
    logger.info(
        "downloaded %d/%d requested SEC 13F window files: %s",
        len(downloaded), count, [n for n, _ in downloaded],
    )
    return downloaded


def _find_zip_member(zf: zipfile.ZipFile, member_name: str) -> str | None:
    """Return the actual archive name for a top-level SEC 13F member file
    (e.g. ``"SUBMISSION.tsv"``), tolerating a window ZIP whose entries are
    nested under a subdirectory instead of sitting at the archive root.

    MEASURED 2026-09-14 (alpha-engine-config-I10763): every 13F window ZIP
    inspected is flat except ``01jun2025-31aug2025_form13f.zip`` (published
    2025-09-02), whose 9 members all sit under a
    ``01JUN2025-31AUG2025_form13f/`` subdirectory — SEC's own packaging is
    inconsistent and not predictable from the window filename. An exact
    ``zf.open(member_name)`` lookup KeyErrors on that layout; the prior
    behaviour (catch and treat the window as carrying no such file) then
    silently read as "this window has zero submissions/holdings" instead of
    "this window's SUBMISSION.tsv could not be located" — which is exactly
    what made the 2025Q2 backfill (the only period whose on-time filings
    live solely in that one window) report "no on-time submission" instead
    of the real parse failure.

    Resolved from the archive's actual member list (never assumed from the
    window's filename or a fixed calendar), per the deliverable: select and
    read files from what SEC actually published, not an assumed layout.
    Matches case-insensitively on the basename; prefers a root-level exact
    match, else the first nested match found (an SEC window ZIP carries one
    top-level directory at most). Returns ``None`` if nothing matches — the
    genuinely-missing-file case callers already handle.
    """
    names = zf.namelist()
    if member_name in names:
        return member_name
    lowered = member_name.lower()
    for name in names:
        if name.rsplit("/", 1)[-1].lower() == lowered:
            return name
    return None


def _parse_submission(zf: zipfile.ZipFile) -> pd.DataFrame:
    """Parse SUBMISSION.tsv from a SEC 13F window bulk ZIP.

    Tab-separated with a header row. Columns of interest: ACCESSION_NUMBER,
    FILING_DATE, SUBMISSIONTYPE (e.g. ``13F-HR``, ``13F-HR/A``), CIK,
    PERIODOFREPORT (the actual 13F report-period date — a window mixes
    multiple report periods, this is the one downstream code wants).

    Returns a DataFrame with FILING_DATE/PERIODOFREPORT parsed to
    ``datetime64``, or an empty DataFrame if the file/columns are missing.
    """
    member = _find_zip_member(zf, "SUBMISSION.tsv")
    if member is None:
        logger.warning(
            "SUBMISSION.tsv not found in SEC bulk ZIP (members: %s)",
            zf.namelist()[:10],
        )
        return pd.DataFrame()
    with zf.open(member) as f:
        df = pd.read_csv(f, delimiter="\t", dtype=str, low_memory=False)

    if len(df) == 0:
        return df

    df.columns = [c.strip().upper() for c in df.columns]
    required = ("ACCESSION_NUMBER", "FILING_DATE", "SUBMISSIONTYPE", "CIK", "PERIODOFREPORT")
    missing = [c for c in required if c not in df.columns]
    if missing:
        logger.warning("SUBMISSION.tsv missing required columns: %s", missing)
        return pd.DataFrame()

    df["FILING_DATE"] = pd.to_datetime(df["FILING_DATE"], format="%d-%b-%Y", errors="coerce")
    df["PERIODOFREPORT"] = pd.to_datetime(df["PERIODOFREPORT"], format="%d-%b-%Y", errors="coerce")
    return df[list(required)]


def _apply_filing_cutoff(submissions: pd.DataFrame) -> pd.DataFrame:
    """Drop SUBMISSION rows filed more than
    ``THIRTEEN_F_FILING_DEADLINE_DAYS`` after their own PERIODOFREPORT (17
    CFR 240.13f-1(a); crucible's point-in-time consumer rule — see the
    constant's docstring). Applied BEFORE ``_dedupe_amendments`` so a late
    amendment can never supersede an on-time original with a deadline-
    violating restatement — the original stands and the late amendment is
    simply excluded, exactly as a session that already admitted the
    quarter would never see it.
    """
    if len(submissions) == 0:
        return submissions
    df = submissions.dropna(subset=["PERIODOFREPORT", "FILING_DATE"]).copy()
    if len(df) == 0:
        return df
    deadline = df["PERIODOFREPORT"] + pd.Timedelta(days=THIRTEEN_F_FILING_DEADLINE_DAYS)
    admitted = df[df["FILING_DATE"] <= deadline]
    excluded = len(df) - len(admitted)
    if excluded:
        logger.info(
            "[inst_ownership] filing-date cutoff (17 CFR 240.13f-1(a), "
            "+%dd): excluded %d/%d submissions filed after their report "
            "period's deadline",
            THIRTEEN_F_FILING_DEADLINE_DAYS, excluded, len(df),
        )
    return admitted


def _apply_value_scaling(infotable: pd.DataFrame, winners: pd.DataFrame) -> pd.DataFrame:
    """Scale ``market_value`` x1000 for INFOTABLE rows whose owning
    submission was filed before ``SEC_THIRTEEN_F_THOUSANDS_CUTOFF``
    (2023-01-03) — see that constant's docstring for the SEC readme
    citation. Joined via ``accession_number`` -> ``winners.FILING_DATE``
    since a single window's rows can straddle the boundary (a late or
    amended legacy-period filing arriving in a post-2023 window); a row
    whose accession isn't in ``winners`` (already excluded upstream, e.g.
    by the filing cutoff or amendment dedupe) is left unscaled — it plays
    no further part in the pipeline.
    """
    if len(infotable) == 0 or len(winners) == 0:
        return infotable
    filing_dates = winners.set_index("ACCESSION_NUMBER")["FILING_DATE"]
    accession_filing_date = infotable["accession_number"].map(filing_dates)
    is_legacy = (accession_filing_date < pd.Timestamp(SEC_THIRTEEN_F_THOUSANDS_CUTOFF)).fillna(False)
    if not is_legacy.any():
        return infotable
    out = infotable.copy()
    out.loc[is_legacy, "market_value"] = out.loc[is_legacy, "market_value"] * 1000
    logger.info(
        "[inst_ownership] VALUE scaling (SEC FORM13F_readme.htm, pre-%s "
        "reported in thousands): scaled x1000 for %d/%d INFOTABLE rows",
        SEC_THIRTEEN_F_THOUSANDS_CUTOFF.isoformat(), int(is_legacy.sum()), len(out),
    )
    return out


def _dedupe_amendments(submissions: pd.DataFrame) -> pd.DataFrame:
    """Collapse SUBMISSION rows to one winning ACCESSION_NUMBER per
    (CIK, PERIODOFREPORT): the latest-filed submission wins, with an
    amendment (``SUBMISSIONTYPE`` ending ``/A``) breaking a same-day tie
    over the original — an amendment is the authoritative holdings
    snapshot for that filer+period, per alpha-engine-config-I10529 scope.
    """
    df = submissions.dropna(subset=["PERIODOFREPORT", "CIK", "FILING_DATE"]).copy()
    if len(df) == 0:
        return df
    df["_is_amendment"] = df["SUBMISSIONTYPE"].fillna("").str.endswith("/A")
    df = df.sort_values(["FILING_DATE", "_is_amendment"], ascending=[True, True])
    winners = df.groupby(["CIK", "PERIODOFREPORT"], as_index=False).tail(1)
    return winners.drop(columns=["_is_amendment"])


def _select_report_periods(winners: pd.DataFrame, count: int = 2) -> list[Any]:
    """Return the ``count`` most recent distinct PERIODOFREPORT values
    present in the deduped SUBMISSION winners, most-recent-first."""
    if len(winners) == 0:
        return []
    periods = sorted(winners["PERIODOFREPORT"].dropna().unique(), reverse=True)
    return list(periods[:count])


def _parse_infotable(zf: zipfile.ZipFile) -> pd.DataFrame:
    """Parse INFOTABLE.tsv from a SEC 13F window bulk ZIP.

    Tab-separated with a header row (alpha-engine-config-I10529 — the
    2024+ format; measured against the 2026-06-01-published
    ``01mar2026-31may2026_form13f.zip``). Columns of interest:
    - ACCESSION_NUMBER: str (joins to SUBMISSION.tsv)
    - CUSIP: str (9-char, alphanumeric)
    - PUTCALL: str (empty for equity, "PUT"/"CALL" for options)
    - SSHPRNAMT: float (shares)
    - VALUE: float — USD since 2023-01-03, THOUSANDS of USD before that
      per FORM13F_readme.htm. This parser does NOT scale VALUE — the
      historical backfill (I10733) can download legacy pre-2023 windows,
      and the scale is a property of the FILING (SUBMISSION.FILING_DATE),
      not the window file, so scaling is applied by the orchestrator
      (``_apply_value_scaling``) after joining to SUBMISSION, not here.

    Returns a DataFrame renamed to the lowercase names downstream
    aggregation code expects: accession_number, cusip, put_call, shares,
    market_value.
    """
    member = _find_zip_member(zf, "INFOTABLE.tsv")
    if member is None:
        logger.warning(
            "INFOTABLE.tsv not found in SEC bulk ZIP (members: %s)",
            zf.namelist()[:10],
        )
        return pd.DataFrame()
    with zf.open(member) as f:
        df = pd.read_csv(
            f,
            delimiter="\t",
            dtype=str,
            low_memory=False,
        )

    if len(df) == 0:
        return df

    # Normalize column names (SEC may vary case)
    df.columns = [c.strip().upper() for c in df.columns]

    # Required columns
    for col in ("ACCESSION_NUMBER", "CUSIP"):
        if col not in df.columns:
            logger.warning("INFOTABLE missing required column: %s", col)
            return pd.DataFrame()

    # Parse numeric columns
    for col in ("SSHPRNAMT", "VALUE"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")

    # Filter to equity only (exclude options)
    if "PUTCALL" in df.columns:
        df = df[df["PUTCALL"].isna() | (df["PUTCALL"].str.strip() == "")]

    # Drop rows with invalid CUSIP (9-char alphanumeric)
    df = df[df["CUSIP"].str.match(r"^[0-9A-Z]{9}$", na=False)]

    return df.rename(columns={
        "ACCESSION_NUMBER": "accession_number",
        "CUSIP": "cusip",
        "PUTCALL": "put_call",
        "SSHPRNAMT": "shares",
        "VALUE": "market_value",
    })


def _per_fund_holdings(
    df: pd.DataFrame,
    cusip_to_ticker: dict[str, str],
    accession_to_cik: dict[str, str] | None,
    keep_tickers: set[str] | None,
) -> pd.DataFrame:
    """One row per (ticker, fund) for one period: ``ticker, fund, shares, market_value``.

    ``fund`` is the filer CIK (via ``accession_to_cik``, the winners map from
    SUBMISSION.tsv) so a fund is the same identity across periods; without the
    map the accession number stands in (unique per fund within a period once
    amendments are collapsed, but not comparable across periods).

    Filters to ``keep_tickers`` FIRST — the universe is ~900 names against
    ~13k mapped tickers and ~3M rows, so everything downstream is an order of
    magnitude smaller. All operations are vectorized: the previous shape did
    ``rows[rows.ticker == t]`` per ticker (O(tickers x rows)) and ran 42 min
    to the job timeout on 2026-09-13 (run 34780401480).
    """
    if len(df) == 0:
        return pd.DataFrame(columns=["ticker", "fund", "shares", "market_value"])
    out = pd.DataFrame({
        "ticker": df["cusip"].map(cusip_to_ticker),
        "fund": (df["accession_number"].map(accession_to_cik)
                 if accession_to_cik else df["accession_number"]),
        "shares": pd.to_numeric(df["shares"], errors="coerce").fillna(0.0),
        "market_value": pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0),
    })
    out = out[out["ticker"].notna() & out["fund"].notna()]
    if keep_tickers is not None:
        out = out[out["ticker"].isin(keep_tickers)]
    if len(out) == 0:
        return pd.DataFrame(columns=["ticker", "fund", "shares", "market_value"])
    return out.groupby(["ticker", "fund"], as_index=False, sort=False)[["shares", "market_value"]].sum()


def _aggregate_quarter(funds: pd.DataFrame) -> pd.DataFrame:
    """Per-ticker totals from per-fund holdings: n_funds_holding,
    total_shares_held, total_value_usd (indexed by ticker)."""
    if len(funds) == 0:
        return pd.DataFrame(columns=["n_funds_holding", "total_shares_held", "total_value_usd"])
    agg = funds.groupby("ticker").agg(
        n_funds_holding=("fund", "nunique"),
        total_shares_held=("shares", "sum"),
        total_value_usd=("market_value", "sum"),
    )
    return agg


# ═══════════════════════════════════════════════════════════════════
# QoQ delta computation
# ═══════════════════════════════════════════════════════════════════


def _compute_qoq_deltas(
    current: pd.DataFrame,
    prior: pd.DataFrame,
    cusip_to_ticker: dict[str, str],
    current_quarter: str,
    *,
    accession_to_cik: dict[str, str] | None = None,
    keep_tickers: set[str] | None = None,
) -> list[InstOwnershipRow]:
    """Per-ticker institutional-ownership rows with quarter-over-quarter deltas.

    Vectorized end to end: per-fund holdings for each period, an outer merge
    on (ticker, fund) to classify every fund as new / exited / increasing /
    decreasing / unchanged, then one groupby per ticker.
    """
    cf = _per_fund_holdings(current, cusip_to_ticker, accession_to_cik, keep_tickers)
    if len(cf) == 0:
        return []
    pf = _per_fund_holdings(prior, cusip_to_ticker, accession_to_cik, keep_tickers)

    curr_agg = _aggregate_quarter(cf)
    prior_agg = _aggregate_quarter(pf)

    merged = cf.merge(pf, on=["ticker", "fund"], how="outer", suffixes=("_c", "_p"), indicator=True)
    both = merged["_merge"] == "both"
    merged["new"] = merged["_merge"] == "left_only"
    merged["exited"] = merged["_merge"] == "right_only"
    merged["increasing"] = both & (merged["shares_c"] > merged["shares_p"])
    merged["decreasing"] = both & (merged["shares_c"] < merged["shares_p"])
    fund_counts = merged.groupby("ticker")[["new", "exited", "increasing", "decreasing"]].sum()

    # Top-5 concentration: share of the ticker's institutional shares held by
    # its five largest funds, only where at least five funds hold it.
    top5 = (cf.sort_values("shares", ascending=False)
              .groupby("ticker").head(5)
              .groupby("ticker")["shares"].sum())
    top5_pct = (top5 / curr_agg["total_shares_held"].replace(0, float("nan")) * 100.0)
    top5_pct = top5_pct.where(curr_agg["n_funds_holding"] >= 5)

    rows: list[InstOwnershipRow] = []
    for ticker, c in curr_agg.iterrows():
        counts = fund_counts.loc[ticker] if ticker in fund_counts.index else None
        has_prior = ticker in prior_agg.index
        rows.append(InstOwnershipRow(
            ticker=str(ticker),
            quarter=current_quarter,
            schema_version=SCHEMA_VERSION,
            n_funds_holding=int(c["n_funds_holding"]),
            total_shares_held=float(c["total_shares_held"]),
            total_value_usd=float(c["total_value_usd"]),
            shares_qoq_change=(float(c["total_shares_held"] - prior_agg.loc[ticker, "total_shares_held"])
                               if has_prior else None),
            value_qoq_change=(float(c["total_value_usd"] - prior_agg.loc[ticker, "total_value_usd"])
                              if has_prior else None),
            n_funds_increasing=int(counts["increasing"]) if counts is not None else 0,
            n_funds_decreasing=int(counts["decreasing"]) if counts is not None else 0,
            n_funds_new=int(counts["new"]) if counts is not None else 0,
            n_funds_exited=int(counts["exited"]) if counts is not None else 0,
            put_call_ratio=None,  # options rows are excluded by the parser
            top5_concentration_pct=(float(top5_pct.loc[ticker])
                                    if ticker in top5_pct.index and pd.notna(top5_pct.loc[ticker])
                                    else None),
        ))
    return rows


def rows_to_dataframe(rows: list[InstOwnershipRow]) -> pd.DataFrame:
    """Convert rows to a DataFrame with canonical column order."""
    if not rows:
        cols = list(InstOwnershipRow.__dataclass_fields__.keys())
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame([asdict(r) for r in rows])
    # Ensure canonical column order
    ordered_cols = [c for c in InstOwnershipRow.__dataclass_fields__ if c in df.columns]
    return df[ordered_cols]


def write_inst_ownership_parquet(
    rows: list[InstOwnershipRow],
    *,
    quarter: str,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    run_id: str | None = None,
    update_global_sidecar: bool = True,
) -> str:
    """Write a per-(ticker, quarter) institutional-ownership parquet.

    Output format: one parquet per quarter at
    ``s3://bucket/prefix/{quarter}/result.parquet``
    with a ``latest.json`` sidecar pointing at the most recent run.

    ``update_global_sidecar`` (default ``True``, the weekly-mode
    behaviour): when ``False`` the per-quarter ``{prefix}/{quarter}/
    latest.parquet`` and run artifact are still written, but
    ``{prefix}/latest.json`` is left untouched. That sidecar is the
    freshness pointer the weekly-run readback step and downstream
    freshness monitoring key off; a historical backfill run
    (``compute_and_write_inst_ownership(report_period=...)``, I10733)
    writing an old quarter must never move it backward or otherwise
    disturb what it currently points at.

    Returns the artifact S3 key.
    """
    import json as _json
    from nousergon_lib.eval_artifacts import (
        eval_artifact_key, eval_latest_key, new_eval_run_id,
    )

    df = rows_to_dataframe(rows)
    run_id = run_id or new_eval_run_id()
    artifact_key = f"{prefix}/{quarter}/{run_id}/result.parquet"
    latest_key = f"{prefix}/latest.json"

    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    s3_client.put_object(
        Bucket=bucket, Key=artifact_key, Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    # Also write a per-quarter latest for incremental readers
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{prefix}/{quarter}/latest.parquet",
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    if update_global_sidecar:
        s3_client.put_object(
            Bucket=bucket, Key=latest_key,
            Body=_json.dumps({
                "run_id": run_id,
                "artifact_key": artifact_key,
                "quarter": quarter,
                "schema_version": SCHEMA_VERSION,
                "row_count": int(len(df)),
                "written_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }).encode("utf-8"),
            ContentType="application/json",
        )
    else:
        logger.info(
            "[inst_ownership] historical write for %s — leaving the global "
            "%s freshness sidecar untouched", quarter, latest_key,
        )

    logger.info(
        "[inst_ownership] wrote %d rows for %s to s3://%s/%s",
        len(df), quarter, bucket, artifact_key,
    )
    return artifact_key


def read_inst_ownership_parquet(
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
) -> pd.DataFrame:
    """Consumer-side read. Resolves the canonical artifact via the
    ``latest.json`` sidecar (same shape/convention as
    ``news_aggregates.read_news_aggregates_parquet``).

    Used by ``rag/pipelines/ingest_13f.py`` (config#2428) to source the
    per-ticker QoQ summary it converts into RAG document chunks, and by
    ``crucible-research``'s ``data.substrate.reader.read_inst_ownership``
    (a separate copy of the same ``latest.json``-sidecar read, since that
    repo consumes the parquet without importing this module directly).

    Returns an empty DataFrame with the canonical schema when no
    artifact exists (e.g. the SEC quarter hasn't been published yet, or
    ``compute_and_write_inst_ownership`` hasn't run this quarter).
    """
    latest_key = f"{prefix}/latest.json"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=latest_key)
        sidecar = json.loads(obj["Body"].read())
        artifact_key = sidecar.get("artifact_key")
        if artifact_key:
            body = s3_client.get_object(Bucket=bucket, Key=artifact_key)
            return pd.read_parquet(io.BytesIO(body["Body"].read()), engine="pyarrow")
    except Exception as e:
        logger.info(
            "[inst_ownership] canonical sidecar read failed for %s (%s)",
            latest_key, type(e).__name__,
        )

    cols = list(InstOwnershipRow.__dataclass_fields__.keys())
    return pd.DataFrame(columns=cols)


# ═══════════════════════════════════════════════════════════════════
# End-to-end orchestrator
# ═══════════════════════════════════════════════════════════════════


def _rows_for_selected_periods(
    *,
    winners: pd.DataFrame,
    infotable: pd.DataFrame,
    current_period: Any,
    prior_period: Any | None,
    universe_tickers: list[str],
    s3_client: Any,
    bucket: str,
    force_rebuild_cusip: bool,
    openfigi_api_key: str | None,
) -> tuple[list[InstOwnershipRow], str] | None:
    """Shared tail of the pipeline once (current_period, prior_period) are
    known, for BOTH the weekly "2 most recent" mode and the explicit
    historical ``--report-period`` mode (I10733): VALUE scaling, join
    INFOTABLE to each period's winning accessions, resolve CUSIP→ticker,
    compute QoQ deltas, filter to the scanned universe.

    Returns ``(rows, current_quarter_str)``, or ``None`` if nothing could
    be produced (empty join, unmapped CUSIPs, or nothing in-universe) —
    callers decide whether an empty result is a soft skip (weekly) or a
    loud failure (historical backfill).
    """
    current_q = _quarter_str_for_date(current_period)
    prior_q = _quarter_str_for_date(prior_period) if prior_period is not None else None

    current_accessions = set(
        winners.loc[winners["PERIODOFREPORT"] == current_period, "ACCESSION_NUMBER"]
    )
    prior_accessions = (
        set(winners.loc[winners["PERIODOFREPORT"] == prior_period, "ACCESSION_NUMBER"])
        if prior_period is not None else set()
    )

    infotable = _apply_value_scaling(infotable, winners)

    current_df = infotable[infotable["accession_number"].isin(current_accessions)]
    if len(current_df) == 0:
        logger.warning("no INFOTABLE rows joined to the current period %s", current_q)
        return None
    prior_df = (
        infotable[infotable["accession_number"].isin(prior_accessions)]
        if prior_accessions else pd.DataFrame()
    )

    logger.info(
        "INFOTABLE joined to periods: %s: %d rows, %s: %d rows",
        current_q, len(current_df),
        prior_q, len(prior_df),
    )

    # Resolve CUSIP→ticker for the CUSIPs actually present in the filings
    # (union of both periods) — not the universe tickers, which is the
    # wrong mapping direction (config#2428 / alpha-engine-config-I10529).
    filing_cusips = set(current_df["cusip"].dropna().astype(str))
    if len(prior_df) > 0:
        filing_cusips |= set(prior_df["cusip"].dropna().astype(str))

    cusip_to_ticker = build_cusip_to_ticker(
        filing_cusips,
        s3_client=s3_client,
        bucket=bucket,
        force_rebuild=force_rebuild_cusip,
        api_key=openfigi_api_key,
    )
    if not cusip_to_ticker:
        logger.warning(
            "no cusip→ticker mapping resolved for %d filing cusips — cannot build inst_ownership",
            len(filing_cusips),
        )
        return None

    # Compute QoQ deltas
    universe_set = {t.strip().upper() for t in universe_tickers if t.strip()}
    accession_to_cik = dict(zip(winners["ACCESSION_NUMBER"], winners["CIK"].astype(str)))
    rows = _compute_qoq_deltas(
        current_df, prior_df, cusip_to_ticker, current_q,
        accession_to_cik=accession_to_cik,
        keep_tickers=universe_set or None,
    )
    if not rows:
        logger.info("no tickers resolved from CUSIP mapping in %s", current_q)
        return None

    # Filter to the scanned universe (the mapping can resolve CUSIPs to
    # tickers outside it — OpenFIGI isn't universe-scoped).
    if universe_set:
        before = len(rows)
        rows = [r for r in rows if r.ticker in universe_set]
        logger.info(
            "inst_ownership: %d/%d resolved tickers are in the %d-name scanned universe",
            len(rows), before, len(universe_set),
        )
    if not rows:
        logger.info("no resolved tickers fall within the scanned universe for %s", current_q)
        return None

    logger.info("inst_ownership: %d tickers resolved for %s", len(rows), current_q)
    return rows, current_q


def _compute_ownership_for_report_period(
    report_period: Date,
    universe_tickers: list[str],
    *,
    s3_client: Any,
    bucket: str,
    prefix: str,
    force_rebuild_cusip: bool,
    openfigi_api_key: str | None,
    update_global_sidecar: bool,
    window_cache: dict[str, zipfile.ZipFile] | None = None,
    index_html: str | None = None,
) -> list[InstOwnershipRow]:
    """Historical backfill for one explicit report period (I10733) — the
    ``report_period=`` branch of ``compute_and_write_inst_ownership``.

    Unlike the weekly "2 most recent published PERIODOFREPORT" mode, both
    periods and the windows that cover them are selected EXPLICITLY from
    ``report_period`` itself, and every failure mode (bad date, unpublished
    window, empty join) raises rather than returning ``None`` — a
    backfill run silently skipping a quarter is indistinguishable from a
    quarter that legitimately has no institutional holders.
    """
    if not _is_quarter_end(report_period):
        raise ValueError(
            f"--report-period {report_period.isoformat()} is not a calendar "
            "quarter-end date (expected one of 03-31, 06-30, 09-30, 12-31)"
        )
    prior_period = _previous_quarter_end(report_period)

    filenames = _windows_needed_for_period(report_period, prior_period, index_html=index_html)
    windows = _download_windows(filenames, cache=window_cache)

    submission_frames: list[pd.DataFrame] = []
    infotable_frames: list[pd.DataFrame] = []
    for name, zf in windows:
        sub = _parse_submission(zf)
        info = _parse_infotable(zf)
        logger.info(
            "parsed SEC 13F window %s: %d submissions, %d infotable rows",
            name, len(sub), len(info),
        )
        if len(sub) > 0:
            submission_frames.append(sub)
        if len(info) > 0:
            infotable_frames.append(info)

    if not submission_frames or not infotable_frames:
        raise InstOwnershipPeriodUnavailable(
            f"report period {report_period.isoformat()}: no parseable "
            f"SUBMISSION/INFOTABLE data in required window(s) {filenames}"
        )

    submissions = pd.concat(submission_frames, ignore_index=True)
    infotable = pd.concat(infotable_frames, ignore_index=True)

    submissions = _apply_filing_cutoff(submissions)
    winners = _dedupe_amendments(submissions)

    current_ts = pd.Timestamp(report_period)
    prior_ts = pd.Timestamp(prior_period)
    if current_ts not in set(winners["PERIODOFREPORT"]):
        raise InstOwnershipPeriodUnavailable(
            f"report period {report_period.isoformat()}: no on-time (or "
            f"on-time-amended) submission for this PERIODOFREPORT in "
            f"downloaded window(s) {filenames} after the "
            f"{THIRTEEN_F_FILING_DEADLINE_DAYS}-day filing-date cutoff"
        )

    logger.info(
        "inst_ownership backfill: report_period=%s prior=%s from window "
        "file(s) %s", report_period.isoformat(), prior_period.isoformat(), filenames,
    )

    result = _rows_for_selected_periods(
        winners=winners, infotable=infotable,
        current_period=current_ts, prior_period=prior_ts,
        universe_tickers=universe_tickers, s3_client=s3_client, bucket=bucket,
        force_rebuild_cusip=force_rebuild_cusip, openfigi_api_key=openfigi_api_key,
    )
    if result is None:
        raise InstOwnershipPeriodUnavailable(
            f"report period {report_period.isoformat()}: no joinable "
            "INFOTABLE rows / resolved tickers within the scanned universe"
        )
    rows, quarter = result

    write_inst_ownership_parquet(
        rows, quarter=quarter, s3_client=s3_client,
        bucket=bucket, prefix=prefix,
        update_global_sidecar=update_global_sidecar,
    )
    return rows


def compute_and_write_inst_ownership(
    universe_tickers: list[str],
    *,
    s3_client: Any,
    bucket: str = DEFAULT_S3_BUCKET,
    prefix: str = DEFAULT_S3_PREFIX,
    force_rebuild_cusip: bool = False,
    openfigi_api_key: str | None = None,
    report_period: Date | None = None,
    update_global_sidecar: bool = True,
    _window_cache: dict[str, zipfile.ZipFile] | None = None,
    _index_html: str | None = None,
) -> list[InstOwnershipRow] | None:
    """Download, parse, aggregate, and write 13F institutional ownership.

    Two modes:

    - **Weekly** (``report_period=None``, the default): discover and
      download the 2 newest published SEC 13F filing-window ZIPs, dedupe
      to one accession per (CIK, PERIODOFREPORT) with an amendment
      superseding the original, and select the 2 most recent
      PERIODOFREPORT dates present as current/prior. Returns ``None``
      (nothing written) on any empty-data outcome — this is the
      long-standing scheduled-run contract other callers depend on.
    - **Historical backfill** (``report_period=<a quarter-end date>``,
      I10733): both periods are named explicitly (``report_period`` and
      the quarter before it) and only the SEC window(s) whose filing-date
      coverage actually intersects their 45-day on-time deadlines are
      downloaded — see ``_compute_ownership_for_report_period``. Fails
      loud (raises, naming the period) rather than returning ``None`` on
      any empty-data outcome, and never rewrites the global
      ``{prefix}/latest.json`` freshness sidecar when
      ``update_global_sidecar=False`` (the CLI's default for this mode)
      — that sidecar is the weekly run's own freshness pointer.

    Both modes apply, in order: (1) the ``THIRTEEN_F_FILING_DEADLINE_DAYS``
    filing-date cutoff BEFORE amendment dedupe, so a late amendment can
    never supersede an on-time original; (2) VALUE x1000 scaling for
    submissions filed before ``SEC_THIRTEEN_F_THOUSANDS_CUTOFF``; (3)
    CUSIP→ticker resolution (cache + OpenFIGI) for the CUSIPs seen in
    either period; (4) QoQ deltas per ticker, filtered to the scanned
    universe.

    Returns the list of rows written. For the weekly mode, ``None`` on a
    soft empty outcome (see above) — callers must not treat a
    non-exception return as success without checking for ``None``. For
    the historical mode, always a non-empty list (empty outcomes raise
    ``InstOwnershipPeriodUnavailable``/``ValueError``/``RuntimeError``
    instead).
    """
    if report_period is not None:
        return _compute_ownership_for_report_period(
            report_period, universe_tickers,
            s3_client=s3_client, bucket=bucket, prefix=prefix,
            force_rebuild_cusip=force_rebuild_cusip,
            openfigi_api_key=openfigi_api_key,
            update_global_sidecar=update_global_sidecar,
            window_cache=_window_cache, index_html=_index_html,
        )

    windows = _download_recent_windows(count=2)
    if not windows:
        logger.warning("no SEC 13F window files could be downloaded")
        return None

    submission_frames: list[pd.DataFrame] = []
    infotable_frames: list[pd.DataFrame] = []
    for name, zf in windows:
        sub = _parse_submission(zf)
        info = _parse_infotable(zf)
        logger.info(
            "parsed SEC 13F window %s: %d submissions, %d infotable rows",
            name, len(sub), len(info),
        )
        if len(sub) > 0:
            submission_frames.append(sub)
        if len(info) > 0:
            infotable_frames.append(info)

    if not submission_frames or not infotable_frames:
        logger.warning(
            "no parseable SUBMISSION/INFOTABLE data in the %d downloaded "
            "window file(s)", len(windows),
        )
        return None

    submissions = pd.concat(submission_frames, ignore_index=True)
    infotable = pd.concat(infotable_frames, ignore_index=True)

    submissions = _apply_filing_cutoff(submissions)
    winners = _dedupe_amendments(submissions)
    periods = _select_report_periods(winners, count=2)
    if not periods:
        logger.warning("no report periods resolved from SUBMISSION data")
        return None

    current_period = periods[0]
    prior_period = periods[1] if len(periods) > 1 else None
    current_q = _quarter_str_for_date(current_period)
    prior_q = _quarter_str_for_date(prior_period) if prior_period is not None else None

    logger.info(
        "inst_ownership: selected report periods current=%s (%s), prior=%s (%s) "
        "from window file(s) %s",
        current_period.date() if hasattr(current_period, "date") else current_period,
        current_q,
        prior_period.date() if hasattr(prior_period, "date") else prior_period,
        prior_q, [n for n, _ in windows],
    )

    result = _rows_for_selected_periods(
        winners=winners, infotable=infotable,
        current_period=current_period, prior_period=prior_period,
        universe_tickers=universe_tickers, s3_client=s3_client, bucket=bucket,
        force_rebuild_cusip=force_rebuild_cusip, openfigi_api_key=openfigi_api_key,
    )
    if result is None:
        return None
    rows, quarter = result

    write_inst_ownership_parquet(
        rows, quarter=quarter, s3_client=s3_client,
        bucket=bucket, prefix=prefix,
        update_global_sidecar=update_global_sidecar,
    )

    return rows


# ═══════════════════════════════════════════════════════════════════
# Command-line entry point
# ═══════════════════════════════════════════════════════════════════

# Well-known real CUSIPs used only for --dry-run's keyless OpenFIGI sanity
# check — read-only, no S3, no SEC download.
_DRY_RUN_SAMPLE_CUSIPS = [
    "037833100",  # AAPL
    "594918104",  # MSFT
    "023135106",  # AMZN
    "02079K305",  # GOOGL
    "88160R101",  # TSLA
]


def _run_dry_run() -> int:
    """Map a handful of real CUSIPs via keyless OpenFIGI and print the
    count. Safe to run anywhere: read-only, public API, no S3 writes, no
    SEC download. Exists so a PR touching the mapper carries a real
    measurement rather than an untested claim about the keyless rate.
    """
    mapper = OpenFigiMapper()
    mapping = mapper.map_cusips(_DRY_RUN_SAMPLE_CUSIPS)
    print(
        f"OpenFIGI keyless dry-run: {len(mapping)}/{len(_DRY_RUN_SAMPLE_CUSIPS)} "
        "sample CUSIPs mapped"
    )
    for cusip in _DRY_RUN_SAMPLE_CUSIPS:
        print(f"  {cusip} -> {mapping.get(cusip, '(unmapped)')}")
    return 0 if mapping else 1


def main() -> None:
    """CLI entry point: ``python -m data.derived.inst_ownership ...``."""
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Build 13F institutional-ownership derived table"
    )
    parser.add_argument(
        "--tickers-file", type=str,
        help="Path to a text file with one ticker per line",
    )
    parser.add_argument(
        "--from-membership", action="store_true",
        help=(
            "Resolve the universe from the Scanner's "
            f"s3://<bucket>/{MEMBERSHIP_LATEST_KEY} pointer instead of a "
            "static file (scheduled-run entry point, alpha-engine-config-I10529)"
        ),
    )
    parser.add_argument(
        "--bucket", type=str, default=DEFAULT_S3_BUCKET,
        help=f"S3 bucket (default: {DEFAULT_S3_BUCKET})",
    )
    parser.add_argument(
        "--force-rebuild-cusip", action="store_true",
        help="Force rebuild CUSIP→ticker cache",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Map a handful of well-known CUSIPs via keyless OpenFIGI and "
            "print the count. No SEC download, no S3 reads/writes."
        ),
    )
    parser.add_argument(
        "--report-period", type=str, default=None,
        help=(
            "Comma-separated calendar quarter-end date(s) (YYYY-MM-DD, one "
            "of *-03-31/*-06-30/*-09-30/*-12-31) to backfill historical 13F "
            "report periods instead of the weekly current/prior-quarter "
            "window (alpha-engine-config-I10733). Processed in ascending "
            "order in one invocation; each writes {prefix}/{quarter}/"
            "latest.parquet only — the global latest.json freshness "
            "sidecar (the weekly run's pointer) is never touched."
        ),
    )
    args = parser.parse_args()

    if args.dry_run:
        sys.exit(_run_dry_run())

    if args.tickers_file and args.from_membership:
        print("--tickers-file and --from-membership are mutually exclusive", file=sys.stderr)
        sys.exit(1)

    report_periods: list[Date] = []
    if args.report_period:
        for tok in args.report_period.split(","):
            tok = tok.strip()
            if not tok:
                continue
            try:
                d = Date.fromisoformat(tok)
            except ValueError:
                print(
                    f"--report-period value {tok!r} is not a valid YYYY-MM-DD date",
                    file=sys.stderr,
                )
                sys.exit(1)
            if not _is_quarter_end(d):
                print(
                    f"--report-period value {tok!r} is not a calendar "
                    "quarter-end date (expected *-03-31, *-06-30, *-09-30, "
                    "or *-12-31)",
                    file=sys.stderr,
                )
                sys.exit(1)
            report_periods.append(d)
        report_periods.sort()

    # Bootstrap S3 access
    try:
        import boto3
    except ImportError:
        print("boto3 required for S3 access", file=sys.stderr)
        sys.exit(1)

    s3 = boto3.client("s3")

    # Load tickers
    if args.tickers_file:
        with open(args.tickers_file) as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
    elif args.from_membership:
        try:
            tickers = load_universe_from_membership(s3_client=s3, bucket=args.bucket)
        except UniverseUnavailable as e:
            print(f"universe resolution failed: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(
            "Usage: python -m data.derived.inst_ownership "
            "(--tickers-file <path> | --from-membership | --dry-run)",
            file=sys.stderr,
        )
        sys.exit(1)

    openfigi_api_key = os.environ.get(OPENFIGI_API_KEY_ENV_VAR) or None

    if report_periods:
        # Historical backfill (I10733): one SEC index-page fetch and one
        # in-process window-zip cache shared across every period in this
        # invocation — adjacent quarters need overlapping windows (P's
        # "current" window is P+1's "prior" window).
        index_html = _fetch_sec_index_html()
        if not index_html:
            print(
                "SEC 13F index page unreachable — cannot resolve historical "
                "window filenames",
                file=sys.stderr,
            )
            sys.exit(1)
        window_cache: dict[str, zipfile.ZipFile] = {}
        total_rows = 0
        for period in report_periods:
            print(f"Processing report period {period.isoformat()}...")
            try:
                rows = compute_and_write_inst_ownership(
                    tickers, s3_client=s3, bucket=args.bucket,
                    force_rebuild_cusip=args.force_rebuild_cusip,
                    openfigi_api_key=openfigi_api_key,
                    report_period=period, update_global_sidecar=False,
                    _window_cache=window_cache, _index_html=index_html,
                )
            except Exception as e:
                # Fail-loud producer contract (AGENTS.md): a backfill period
                # that couldn't be produced is a hard error naming the
                # period, never a silently skipped period (I10733).
                print(
                    f"FAILED report period {period.isoformat()}: "
                    f"{type(e).__name__}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(f"  wrote {len(rows)} rows for {rows[0].quarter}")
            total_rows += len(rows)
        print(
            f"Backfill complete: {len(report_periods)} period(s), "
            f"{total_rows} total rows"
        )
        return

    print(f"Processing {len(tickers)} tickers for 13F institutional ownership...")
    rows = compute_and_write_inst_ownership(
        tickers, s3_client=s3, bucket=args.bucket,
        force_rebuild_cusip=args.force_rebuild_cusip,
        openfigi_api_key=openfigi_api_key,
    )
    # Fail-loud producer contract (AGENTS.md): a run that produced nothing
    # exits non-zero. `sys.exit(0)` here previously masked every failure
    # mode below main() as workflow success (alpha-engine-config-I10529).
    if not rows:
        print("No data processed (see logs for details).", file=sys.stderr)
        sys.exit(1)

    print(f"Written: {len(rows)} rows for {rows[0].quarter}")
    print(f"Sample: {rows[0].ticker} — {rows[0].n_funds_holding} funds, "
          f"{rows[0].total_shares_held:,.0f} shares")


if __name__ == "__main__":
    # Alpha-engine-config-I10529: `python -m data.derived.inst_ownership` was
    # a no-op before this — main() existed but nothing ever called it, so the
    # module's own documented CLI entry point could not have been invoked by
    # anything, ever. Found while wiring the first real scheduled caller.
    main()
