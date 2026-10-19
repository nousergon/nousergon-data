"""collectors/edgar_pit_fundamentals.py — point-in-time fundamentals keyed by FILING date.

Origin: alpha-engine-config-I10733 (child of I10721). The v2 attractiveness
pillars read fundamentals through `crucible/data/point_in_time.py`. Until this
dataset existed the only point-in-time source was v1's dated
`features/{date}/fundamental.parquet` snapshots, whose fields were degenerate
before label 2026-08-19 (`fcf_yield` one distinct value, `gross_margin` two), so
the pillars were measured on ~17 sessions. ArcticDB `universe` fundamentals are
not an alternative: `features/feature_engineer.py` broadcasts the collection
day's value over every historical row, which is look-ahead by construction.

Source
======

SEC EDGAR XBRL `companyfacts` (no API key; SEC requires a declared User-Agent
and at most 10 requests per second). Every fact carries the date it was
`filed`, which is the knowledge date this module keys on. Never the period end.
The bulk `companyfacts.zip` (one request, ~1.4 GB, rebuilt nightly by SEC) is
the default. The per-CIK API is the fallback for a narrowed run.

The knowledge rule
==================

`sessions/{L}.parquet` is built from facts with `filed <= L` and the close of
session L, and is admissible for a session S only when L < S. An EDGAR filing
accepted after 17:30 ET carries the NEXT business day as its filing date, so
the rule never admits a filing before it was public.

Restatements: every version of a fact is kept (the audit `facts/` table). As
of L, a period's value is the one from the most recent filing on or before L.
A restatement filed after L therefore never reaches back into L, and a
restatement filed before L replaces the earlier value from its filing date on.

Tag map
=======

Each quantity lists XBRL concepts in priority order. For each reporting period
the first concept that reports that exact period wins, so a company that moved
from `SalesRevenueNet` to `RevenueFromContractWithCustomerExcludingAssessedTax`
in 2018 keeps one continuous series. `TAG_MAP` below is the single declaration;
`features/SCHEMA.md` §2c renders it for readers.

Definitions (v1 parity)
=======================

The eleven fields keep v1's names AND v1's normalisations
(`collectors/fundamentals.py::_fetch_single_ticker`), because the crucible
feature columns (`pe_div30_ratio`, ...) say those normalisations in their
names:

======================  =================================================  ==============
field                   definition                                         v1 clip
======================  =================================================  ==============
roe                     net income TTM / stockholders' equity               [-1, 1]
debt_to_equity          total debt / equity, / 2                            [-3, 3]
gross_margin            gross profit TTM / revenue TTM                      [0, 1]
current_ratio           current assets / current liabilities, / 3          [0, 3]
pe_ratio                market cap / net income TTM, / 30                   [-3, 3]
pb_ratio                market cap / equity, / 5                            [-3, 3]
fcf_yield               (operating cash flow - capex) TTM / market cap     [-0.5, 0.5]
revenue_growth_3y       3-year CAGR of annual revenue                       [-0.5, 1.5]
eps_growth_3y           3-year CAGR of annual diluted EPS (split-adjusted)  [-1, 2]
capex_growth_5y         5-year CAGR of annual capex                         [-1, 2]
payout_ratio            dividends paid TTM / net income TTM                 [0, 2]
======================  =================================================  ==============

A value that cannot be computed is null, never a neutral constant (the
`alpha-engine-config-I8255` class). Declared deltas from v1:

1. roe, debt_to_equity: null when equity <= 0 (v1's vendor emitted a sign-flipped ratio).
2. fcf_yield: signed. v1 wrote 0.0 for negative free cash flow.
3. payout_ratio: null when net income <= 0. Dividends are 0.0 only when the
   company reported operating cash flow in the trailing window and tagged no
   dividend payment in it; a silent tag is not read as zero otherwise.
4. Growth: null unless both endpoints are positive (a CAGR of a sign change is undefined).

TTM: a 10-K value when the newest period is a fiscal year; otherwise the sum of
the four newest discrete quarters, where Q2-Q4 are derived from the year-to-date
cumulatives a 10-Q reports (6M - 3M, 9M - 6M, FY - 9M) when no 3-month fact
exists. A period older than `MAX_PERIOD_AGE_DAYS` before the knowledge date is
not used (a filer that stopped filing is not frozen onto later sessions).

Price basis
===========

Market cap = ArcticDB `universe` Close on session L (split-adjusted to the
current share basis, `features/SCHEMA.md` §2a) x shares outstanding reported at
date d / (product of split factors with execution date after d). The split
factors are the corporate-action registry (the record that restated Close)
merged with polygon's split history back to `SPLIT_LOOKBACK_YEARS` before the
first session, so price and shares sit on one basis. The registry alone holds
only splits the daily sync saw (2026 onward), and every earlier split was missing
until alpha-engine-config-I12418. Diluted EPS for the growth CAGR is put on the
same basis from its filing date. A (ticker, date) in `SPLIT_FACTORS_DECLARED`
replaces whatever the two sources hold there, and a split the merged set holds
together with its own inverse on one date refuses the run: the pair multiplies
to 1.0, which erases the split without anything noticing (DD, HON,
alpha-engine-config-I12434).

Share counts are put on that basis by what they are: a dei cover-page count is
dated and takes the splits after its own date; a us-gaap balance-sheet count
or weighted average is restated by its filing for earlier splits, so it takes
the splits after its filing date. `ShareCountScreen` then refuses a share fact
a whole power of ten from the last accepted count (a units or `decimals`
error) unless another tag in the same filing agrees with it, and any count
under `MIN_SHARE_COUNT` (a pre-listing shell's founder shares). A concept a
filer is known to tag with something other than its share count is refused
outright through `SHARE_FACTS_REFUSED` (alpha-engine-config-I12433). Refusals
are counted in the run summary (alpha-engine-config-I12426).

Gates (the run refuses to write when one fails)
===============================================

* Split source: each `SPLIT_SOURCE_CANARIES` name in the universe has its known
  split in the merged set, and a polygon error stops the run rather than
  degrading to no splits.
* Coverage: the newest session covers at least `COVERAGE_FLOOR_RATIO` of the
  eligible universe, the same floor the crucible consumer applies.
* Cross-check: on the newest v1 fundamentals snapshot at or before the newest
  session, the EDGAR trailing P/E and v1's vendor P/E agree in rank (Spearman >=
  `CROSSCHECK_MIN_SPEARMAN`) and in level (median |log ratio| <=
  `CROSSCHECK_MAX_MEDIAN_ABS_LOG_RATIO`). A units, share-count or split-basis
  error moves both. Disabling it requires a written reason, which is recorded.
  Every earlier v1 snapshot the run built a session for is held to the same
  thresholds; one v1 cannot be compared on (its P/E was a constant before
  2026-05) is counted, not failed.
* Share-count continuity: on EVERY session the run built, no ticker's share
  count moves more than `SHARE_COUNT_JUMP_RATIO` between adjacent sessions
  without a split executed between them, unless the jump is declared with a
  reason in `SHARE_COUNT_JUMPS_DECLARED`. The cross-checks are medians over
  hundreds of names and pass with one name a million times off; this does not.

Dataset (in the data bucket; nothing here names it)
===================================================

* `fundamentals_pit/edgar/v1/sessions/{YYYY-MM-DD}.parquet`: one row per ticker
  with a resolved filing, schema `contracts/edgar_pit_fundamentals_session.schema.json`.
  Write-if-absent unless `--overwrite`.
* `fundamentals_pit/edgar/v1/facts/{run_date}/{run_id}.parquet`: every admitted
  fact version for the universe, the audit trail a restatement is replayed from.
* `fundamentals_pit/edgar/v1/runs/{run_date}/{run_id}.json`: the run summary.

Run in-region, on the `edgar-pit-fundamentals-backfill` /
`edgar-pit-fundamentals-daily` workloads of `alpha-engine-data-spot-dispatcher`
(ArcticDB is unreadable from the laptop, alpha-engine-config-I9771).

Known residuals, measured or declared
=====================================

* GICS sector: no free dated GICS history exists. Sector stays on the
  constituents snapshots (from 2026-05-01), which bounds pillar depth.
* Multi-class share structures whose shares are reported in another class's
  equivalents (BRK-B reports class-A-equivalent shares) produce a wrong market
  cap. The cross-check names every name off by more than 3x in the run summary.
* Shares issued between periodic filings are invisible until the next one: the
  knowledge rule admits 10-K/10-Q facts only. AHR listed on 2024-02-07 with
  about twice the shares of its last 10-Q, and its first post-listing cover
  count (2024-03-22) is about half the later ones, so its market cap reads about
  half until the 2024-08-09 10-Q. A genuine 7x-14x (times a power of ten) change
  reported by one tag only would be refused as a scale error; the one measured
  (Chesapeake's 2021 emergence, EXE) predates the first session.
* Coverage measured 2026-09-14: 903 of 903 universe tickers map to a CIK; 40 of
  40 sampled file us-gaap facts; 38 of 40 have net income filed on or before 2019.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import io
import json
import logging
import math
import os
import sys
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Any, Protocol

import pandas as pd

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/edgar/v1"
SESSIONS_PREFIX = f"{DATASET_PREFIX}/sessions/"


def session_key(label: dt.date) -> str:
    return f"{SESSIONS_PREFIX}{label.isoformat()}.parquet"


def facts_key(run_date: dt.date, run_id: str) -> str:
    return f"{DATASET_PREFIX}/facts/{run_date.isoformat()}/{run_id}.parquet"


def run_summary_key(run_date: dt.date, run_id: str) -> str:
    return f"{DATASET_PREFIX}/runs/{run_date.isoformat()}/{run_id}.json"


# alpha-engine-config-I10750: fixed-key freshness sentinel, written alongside
# every dated run summary — see the comment at its write site in `run()`.
RUN_LATEST_KEY = f"{DATASET_PREFIX}/runs/latest.json"


# ── SEC access ──────────────────────────────────────────────────────────────

SEC_USER_AGENT_ENV = "SEC_EDGAR_USER_AGENT"
# The same declared identity `data/derived/inst_ownership.py` sends SEC.
DEFAULT_SEC_USER_AGENT = (
    "NousErgonResearch/1.0 (alpha-engine-research@nousergon.com; research use only)"
)
#: SEC's published fair-access ceiling.
SEC_MAX_REQUESTS_PER_SECOND = 10.0
#: What this client paces to, under the ceiling.
SEC_TARGET_REQUESTS_PER_SECOND = 8.0
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_ZIP_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
COMPANYFACTS_API_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class SecFetchError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


class _SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class SecClient:
    """A paced, retrying GET client that always declares the User-Agent SEC requires."""

    def __init__(
        self,
        *,
        user_agent: str | None = None,
        session: Any = None,
        clock: Clock | None = None,
        requests_per_second: float = SEC_TARGET_REQUESTS_PER_SECOND,
        max_attempts: int = 5,
    ) -> None:
        agent = user_agent if user_agent is not None else os.environ.get(SEC_USER_AGENT_ENV)
        agent = (agent if agent is not None else DEFAULT_SEC_USER_AGENT).strip()
        if not agent:
            raise ValueError("SEC requires a declared User-Agent; an empty one is refused")
        if not 0 < requests_per_second <= SEC_MAX_REQUESTS_PER_SECOND:
            raise ValueError(
                f"requests_per_second={requests_per_second} is outside (0, "
                f"{SEC_MAX_REQUESTS_PER_SECOND}], SEC's fair-access ceiling"
            )
        if session is None:
            import requests

            session = requests.Session()
        self._session = session
        self._headers = {"User-Agent": agent, "Accept-Encoding": "gzip, deflate"}
        self._interval = 1.0 / requests_per_second
        self._clock = clock or _SystemClock()
        self._last_request_at: float | None = None
        self._max_attempts = max_attempts
        self.request_count = 0

    @property
    def user_agent(self) -> str:
        return self._headers["User-Agent"]

    def _pace(self) -> None:
        now = self._clock.monotonic()
        if self._last_request_at is not None:
            wait = self._last_request_at + self._interval - now
            if wait > 0:
                self._clock.sleep(wait)
                now = self._clock.monotonic()
        self._last_request_at = now

    def get(self, url: str, *, stream: bool = False, timeout: float = 120.0) -> Any:
        for attempt in range(1, self._max_attempts + 1):
            self._pace()
            self.request_count += 1
            response = self._session.get(url, headers=self._headers, stream=stream, timeout=timeout)
            status = int(response.status_code)
            if status == 200:
                return response
            if status in _RETRYABLE_STATUS and attempt < self._max_attempts:
                backoff = min(60.0, 2.0**attempt)
                logger.warning(
                    "SEC GET %s -> HTTP %d (attempt %d/%d); retrying in %.0fs",
                    url, status, attempt, self._max_attempts, backoff,
                )
                self._clock.sleep(backoff)
                continue
            raise SecFetchError(
                f"SEC GET {url} -> HTTP {status} after {attempt} attempt(s)", status=status
            )
        raise AssertionError("unreachable: the loop returns or raises")

    def get_json(self, url: str) -> Any:
        return self.get(url).json()

    def download(self, url: str, destination: str) -> int:
        response = self.get(url, stream=True, timeout=900.0)
        total = 0
        with open(destination, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1 << 20):
                if chunk:
                    handle.write(chunk)
                    total += len(chunk)
        declared = response.headers.get("Content-Length")
        encoded = response.headers.get("Content-Encoding")
        if total == 0:
            raise SecFetchError(f"SEC GET {url} returned an empty body")
        if declared is not None and not encoded and int(declared) != total:
            raise SecFetchError(
                f"SEC GET {url} was truncated: {total} bytes read, {declared} declared"
            )
        return total


def normalize_ticker(ticker: str) -> str:
    """SEC and the fleet both write class suffixes with a hyphen (`BRK-B`)."""
    return ticker.strip().upper().replace(".", "-")


def ticker_cik_map(payload: Mapping[str, Any]) -> dict[str, int]:
    """`company_tickers.json` -> {ticker: cik}. The first row for a ticker wins."""
    out: dict[str, int] = {}
    for row in payload.values():
        out.setdefault(normalize_ticker(str(row["ticker"])), int(row["cik_str"]))
    if not out:
        raise SecFetchError("company_tickers.json carried no tickers")
    return out


class CompanyFactsSource(Protocol):
    name: str

    def get(self, cik: int) -> dict[str, Any] | None: ...


class ZipCompanyFacts:
    """SEC's nightly bulk archive, opened from a local path."""

    name = "companyfacts.zip"

    def __init__(self, path: str) -> None:
        self._zip = zipfile.ZipFile(path)
        self._members = set(self._zip.namelist())
        if not self._members:
            raise SecFetchError(f"{path} holds no companyfacts members")

    def get(self, cik: int) -> dict[str, Any] | None:
        member = f"CIK{cik:010d}.json"
        if member not in self._members:
            return None
        with self._zip.open(member) as handle:
            return json.load(handle)


class ApiCompanyFacts:
    """One paced request per CIK."""

    name = "companyfacts-api"

    def __init__(self, client: SecClient) -> None:
        self._client = client

    def get(self, cik: int) -> dict[str, Any] | None:
        try:
            return self._client.get_json(COMPANYFACTS_API_URL.format(cik=cik))
        except SecFetchError as exc:
            if exc.status == 404:
                return None
            raise


# ── the tag map ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Quantity:
    name: str
    kind: str  # "duration" | "instant"
    unit: str
    concepts: tuple[tuple[str, str], ...]
    per_share: bool = False


_G = "us-gaap"
TAG_MAP: tuple[Quantity, ...] = (
    Quantity(
        "revenue",
        "duration",
        "USD",
        (
            (_G, "Revenues"),
            (_G, "RevenueFromContractWithCustomerExcludingAssessedTax"),
            (_G, "SalesRevenueNet"),
            (_G, "RevenueFromContractWithCustomerIncludingAssessedTax"),
            (_G, "SalesRevenueGoodsNet"),
        ),
    ),
    Quantity(
        "cost_of_revenue",
        "duration",
        "USD",
        ((_G, "CostOfRevenue"), (_G, "CostOfGoodsAndServicesSold"), (_G, "CostOfGoodsSold")),
    ),
    Quantity("gross_profit", "duration", "USD", ((_G, "GrossProfit"),)),
    Quantity(
        "net_income",
        "duration",
        "USD",
        (
            (_G, "NetIncomeLoss"),
            (_G, "NetIncomeLossAvailableToCommonStockholdersBasic"),
            (_G, "ProfitLoss"),
        ),
    ),
    Quantity(
        "eps_diluted",
        "duration",
        "USD/shares",
        (
            (_G, "EarningsPerShareDiluted"),
            (_G, "EarningsPerShareBasicAndDiluted"),
            (_G, "EarningsPerShareBasic"),
        ),
        per_share=True,
    ),
    Quantity(
        "equity",
        "instant",
        "USD",
        (
            (_G, "StockholdersEquity"),
            (_G, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
        ),
    ),
    Quantity("assets_total", "instant", "USD", ((_G, "Assets"),)),
    Quantity("liabilities_total", "instant", "USD", ((_G, "Liabilities"),)),
    Quantity("assets_current", "instant", "USD", ((_G, "AssetsCurrent"),)),
    Quantity("liabilities_current", "instant", "USD", ((_G, "LiabilitiesCurrent"),)),
    Quantity(
        "debt_combined",
        "instant",
        "USD",
        ((_G, "DebtLongtermAndShorttermCombinedAmount"), (_G, "DebtAndCapitalLeaseObligations")),
    ),
    Quantity("long_term_debt_total", "instant", "USD", ((_G, "LongTermDebt"),)),
    Quantity(
        "long_term_debt_noncurrent",
        "instant",
        "USD",
        ((_G, "LongTermDebtNoncurrent"), (_G, "LongTermDebtAndCapitalLeaseObligations")),
    ),
    Quantity(
        "long_term_debt_current",
        "instant",
        "USD",
        ((_G, "LongTermDebtCurrent"), (_G, "LongTermDebtAndCapitalLeaseObligationsCurrent")),
    ),
    Quantity("short_term_borrowings", "instant", "USD", ((_G, "ShortTermBorrowings"),)),
    Quantity("commercial_paper", "instant", "USD", ((_G, "CommercialPaper"),)),
    Quantity("debt_current", "instant", "USD", ((_G, "DebtCurrent"),)),
    Quantity(
        "operating_cash_flow",
        "duration",
        "USD",
        (
            (_G, "NetCashProvidedByUsedInOperatingActivities"),
            (_G, "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
        ),
    ),
    Quantity(
        "capex",
        "duration",
        "USD",
        (
            (_G, "PaymentsToAcquirePropertyPlantAndEquipment"),
            (_G, "PaymentsToAcquireProductiveAssets"),
        ),
    ),
    Quantity(
        "dividends_paid",
        "duration",
        "USD",
        (
            (_G, "PaymentsOfDividendsCommonStock"),
            (_G, "PaymentsOfDividends"),
            (_G, "PaymentsOfOrdinaryDividends"),
        ),
    ),
    Quantity(
        "shares_outstanding",
        "instant",
        "shares",
        (("dei", "EntityCommonStockSharesOutstanding"), (_G, "CommonStockSharesOutstanding")),
    ),
    Quantity(
        "diluted_weighted_shares",
        "duration",
        "shares",
        ((_G, "WeightedAverageNumberOfDilutedSharesOutstanding"),),
    ),
)

#: (taxonomy, concept, unit) -> (quantity, priority)
_CONCEPT_INDEX: dict[tuple[str, str, str], tuple[Quantity, int]] = {}
for _quantity in TAG_MAP:
    for _priority, (_taxonomy, _concept) in enumerate(_quantity.concepts):
        _CONCEPT_INDEX[(_taxonomy, _concept, _quantity.unit)] = (_quantity, _priority)
_QUANTITY_BY_NAME: dict[str, Quantity] = {q.name: q for q in TAG_MAP}

#: Periodic reports only: an 8-K or proxy fact is a partial, unaudited restatement of these.
ADMITTED_FORMS = frozenset(
    {"10-K", "10-K/A", "10-Q", "10-Q/A", "10-KT", "10-KT/A", "10-QT", "10-QT/A"}
)

MAX_PERIOD_AGE_DAYS = 400
MAX_ANNUAL_AGE_DAYS = 550
_QUARTER_DAYS = (80, 100)
_CUMULATIVE_DAYS = ((80, 100), (170, 190), (260, 285), (350, 380))
_ANNUAL_DAYS = (350, 380)
_ANNUAL_MATCH_TOLERANCE_DAYS = 20


@dataclass(frozen=True, slots=True)
class Fact:
    quantity: str
    priority: int
    start: int | None  # proleptic ordinal; None for an instant
    end: int
    value: float
    filed: int
    accession: str
    taxonomy: str
    concept: str
    unit: str
    form: str


@dataclass
class ExtractStats:
    admitted: int = 0
    wrong_form: int = 0
    malformed: int = 0


def _ordinal(text: str) -> int:
    return dt.date.fromisoformat(text).toordinal()


def extract_facts(document: Mapping[str, Any], stats: ExtractStats | None = None) -> list[Fact]:
    """Every tag-map fact from one companyfacts document, in filing order.

    A row missing `end`, `filed`, `val` or `accn`, or carrying an unparseable
    date, is counted in ``stats.malformed`` and not used. That is a recorded
    skip, not a silent one: the count is written to the run summary, and one
    malformed SEC row must not withhold a company's other ten thousand facts.
    """
    stats = stats if stats is not None else ExtractStats()
    out: list[Fact] = []
    for taxonomy, concepts in (document.get("facts") or {}).items():
        for concept, body in concepts.items():
            for unit, rows in (body.get("units") or {}).items():
                hit = _CONCEPT_INDEX.get((taxonomy, concept, unit))
                if hit is None:
                    continue
                quantity, priority = hit
                for row in rows:
                    form = str(row.get("form") or "")
                    if form not in ADMITTED_FORMS:
                        stats.wrong_form += 1
                        continue
                    try:
                        end = _ordinal(row["end"])
                        filed = _ordinal(row["filed"])
                        start = _ordinal(row["start"]) if quantity.kind == "duration" else None
                        value = float(row["val"])
                        accession = str(row["accn"])
                    except (KeyError, TypeError, ValueError):
                        stats.malformed += 1
                        continue
                    if not math.isfinite(value) or (start is not None and start > end):
                        stats.malformed += 1
                        continue
                    stats.admitted += 1
                    out.append(
                        Fact(
                            quantity=quantity.name,
                            priority=priority,
                            start=start,
                            end=end,
                            value=value,
                            filed=filed,
                            accession=accession,
                            taxonomy=taxonomy,
                            concept=concept,
                            unit=unit,
                            form=form,
                        )
                    )
    out.sort(key=lambda f: (f.filed, f.accession))
    return out


# ── splits ──────────────────────────────────────────────────────────────────

#: [(execution-date ordinal, factor)], factor = split_from / split_to: the multiplier a
#: price strictly before the execution date takes to reach the current basis.
SplitEvents = Sequence[tuple[int, float]]


def split_factor_after(events: SplitEvents, ordinal: int) -> float:
    """Product of the factors of every split executed strictly after ``ordinal``."""
    factor = 1.0
    for executed, ratio in events:
        if executed > ordinal:
            factor *= ratio
    return factor


# ── share-count basis and scale screen ──────────────────────────────────────

SHARE_QUANTITIES = frozenset({"shares_outstanding", "diluted_weighted_shares"})


def share_count_current_basis(fact: Fact, splits: SplitEvents) -> float:
    """A reported share count on the current split basis.

    The dei cover-page count is dated: it states the shares outstanding on its
    own date and is never restated, so it takes the splits after that date. A
    us-gaap balance-sheet count or weighted average is restated retroactively
    for any split made before the statements are issued (ASC 260-10-55-12, SAB
    Topic 4C), so it already sits on the basis of its FILING date. Rebasing it
    from its period end adjusted it twice: GOOG and GOOGL, whose only share tag
    is the weighted average, read 20x their share count from the 10-Q of
    2022-07-27, filed after the 2022-07-18 split for a quarter ended 06-30
    (alpha-engine-config-I12426).
    """
    dated = fact.end if fact.taxonomy == "dei" else fact.filed
    return fact.value / split_factor_after(splits, dated)

#: How close to a whole power of ten (in log10) a share count must sit from the
#: reference to read as a decimal shift: ratios in [7.1, 14.1] x 10^k, k != 0.
_SCALE_SHIFT_TOLERANCE_LOG10 = 0.15

#: Two share counts in one filing "agree" when they are within this factor.
_SHARE_AGREEMENT_RATIO = 3.0

#: A share count under this is a pre-listing shell's, not a traded company's: a
#: spin-off or merger vehicle reports its 100 or 1,000 founder shares in the
#: 10-Q it files before the transaction closes. Measured: KNF 1,000 (2023-05-16),
#: SKYD 2,000, SW 100 (2024), which put their first traded sessions at a market
#: cap of a few thousand dollars. The smallest real count in the universe is
#: millions.
MIN_SHARE_COUNT = 100_000

#: Share concepts a filer tags with something that is not its share count, keyed by
#: (CIK, concept). The screen cannot catch these: the value is not a power of ten off,
#: and the filer has no other undimensioned share tag to compare it with. Refusing it
#: leaves market cap empty for the filings it would have set, which is honest; using
#: it is not. Add an entry only with the measured values and a reason a reader can
#: check.
SHARE_FACTS_REFUSED: Mapping[tuple[int, str], str] = {
    (1794669, "WeightedAverageNumberOfDilutedSharesOutstanding"): (
        "Shift4 (FOUR) tags its cover-page count by share class only, so this is its sole "
        "undimensioned share tag from 2022 to 2025. Its 10-Ks of 2025-02-19 and 2026-02-27 "
        "carry 3,864,579 (FY2022), 1,942,054 (FY2023), 1,668,826 (FY2024) and 1,333,686 "
        "(FY2025) against 82.9M shares in its 2021 10-Q and an 81.2M cover count in the "
        "2026-02-27 10-K: about 2% of the share count, not a decimal shift. Used, it put "
        "market cap about 50x low from 2025-02-19 to 2026-02-26 "
        "(alpha-engine-config-I12433)."
    ),
}


def refused_share_concepts(cik: int | None) -> frozenset[str]:
    """The concepts `SHARE_FACTS_REFUSED` refuses for ``cik``."""
    if cik is None:
        return frozenset()
    return frozenset(concept for (key, concept) in SHARE_FACTS_REFUSED if key == int(cik))


def scale_shift(value: float, reference: float) -> int:
    """The power of ten ``value`` sits from ``reference``; 0 when it is not a decimal shift.

    Filers mis-scale share counts by whole powers of ten (a cover-page count
    tagged in thousands, a weighted average tagged with the wrong `decimals`).
    Measured in companyfacts (alpha-engine-config-I12426): HXL 10-Qs of
    2024-07 and 2024-10 at 10^6, HL 10-Qs of 2023-08, 2024-05 and 2024-08 at
    10^3, NTAP's 2024-02-29 10-Q at 10^-1, EchoStar's (ECHO) diluted weighted
    average in seven 10-Qs from 2024-05 to 2025-11 at 10^-3. A genuine change
    in share count is a merger, an offering or a buyback, which does not land
    on a power of ten. Splits are taken out before the comparison.
    """
    if value <= 0 or reference <= 0:
        return 0
    exponent = math.log10(value / reference)
    nearest = round(exponent)
    if nearest != 0 and abs(exponent - nearest) <= _SCALE_SHIFT_TOLERANCE_LOG10:
        return int(nearest)
    return 0


@dataclass(frozen=True)
class ShareRejection:
    filed: int
    accession: str
    concept: str
    end: int
    value: float
    reference: float | None
    shift: int
    reason: str  # "scale" | "placeholder"


class ShareCountScreen:
    """Refuses share-count facts that sit a decimal shift from the last accepted count.

    The reference is the accepted share count (either quantity, on the current
    split basis) with the newest period end from an EARLIER filing date, used
    while it is no older than `MAX_PERIOD_AGE_DAYS`. A fact a power of ten away
    from it is refused unless another concept in the same filing agrees with
    the fact, which is what a real level change the split set does not know
    about looks like (the cover page and the income statement both move). A
    company whose only share tag is mis-scaled for several filings in a row is
    refused every time, because the reference only moves on accepted facts.
    """

    def __init__(self, splits: SplitEvents, refused: frozenset[str] = frozenset()) -> None:
        self._splits = splits
        self._refused = refused
        self._reference: tuple[int, float] | None = None
        self.rejections: list[ShareRejection] = []

    def _basis(self, fact: Fact) -> float:
        return share_count_current_basis(fact, self._splits)

    def screen(self, filed: int, batch: Sequence[Fact]) -> set[int]:
        """Indexes into ``batch`` of the share facts to drop; updates the reference."""
        shares = [(i, f, self._basis(f)) for i, f in enumerate(batch) if f.quantity in SHARE_QUANTITIES]
        reference = self._reference
        if reference is not None and filed - reference[0] > MAX_PERIOD_AGE_DAYS:
            reference = None
        dropped: set[int] = set()
        for i, fact, value in shares:
            declared = fact.concept in self._refused
            if declared or 0 < value < MIN_SHARE_COUNT:
                dropped.add(i)
                self.rejections.append(
                    ShareRejection(
                        filed=filed,
                        accession=fact.accession,
                        concept=fact.concept,
                        end=fact.end,
                        value=fact.value,
                        reference=None,
                        shift=0,
                        reason="declared" if declared else "placeholder",
                    )
                )
        placeholders = set(dropped)
        for i, fact, value in shares:
            if i in placeholders:
                continue
            shift = scale_shift(value, reference[1]) if reference is not None else 0
            if shift == 0:
                continue
            corroborated = any(
                other.concept != fact.concept
                and j not in placeholders
                and other_value > 0
                and 1.0 / _SHARE_AGREEMENT_RATIO <= other_value / value <= _SHARE_AGREEMENT_RATIO
                for j, other, other_value in shares
            )
            if corroborated:
                continue
            dropped.add(i)
            self.rejections.append(
                ShareRejection(
                    filed=filed,
                    accession=fact.accession,
                    concept=fact.concept,
                    end=fact.end,
                    value=fact.value,
                    reference=reference[1],
                    shift=shift,
                    reason="scale",
                )
            )
        for i, fact, value in shares:
            if i in dropped or value <= 0:
                continue
            if self._reference is None or fact.end >= self._reference[0]:
                self._reference = (fact.end, value)
        return dropped


# ── as-of resolution ────────────────────────────────────────────────────────


class AsOfState:
    """Every fact version seen so far; per (period, concept priority) the latest filing wins."""

    def __init__(self) -> None:
        self._periods: dict[str, dict[tuple[int | None, int], dict[int, tuple[int, str, float]]]] = (
            defaultdict(dict)
        )

    def add(self, fact: Fact, value: float) -> None:
        slot = self._periods[fact.quantity].setdefault((fact.start, fact.end), {})
        current = slot.get(fact.priority)
        if current is None or (fact.filed, fact.accession) >= (current[0], current[1]):
            slot[fact.priority] = (fact.filed, fact.accession, value)

    def resolved(self, quantity: str) -> dict[tuple[int | None, int], float]:
        """One value per period: the highest-priority concept reporting that period."""
        return {
            period: by_priority[min(by_priority)][2]
            for period, by_priority in self._periods.get(quantity, {}).items()
        }


def discrete_quarters(periods: Mapping[tuple[int | None, int], float]) -> dict[int, float]:
    """{quarter end: value}, direct 3-month facts first, then YTD differences."""
    direct: dict[int, float] = {}
    by_start: dict[int, list[tuple[int, float]]] = defaultdict(list)
    for (start, end), value in periods.items():
        if start is None:
            continue
        days = end - start + 1
        if _QUARTER_DAYS[0] <= days <= _QUARTER_DAYS[1]:
            direct.setdefault(end, value)
        if any(lo <= days <= hi for lo, hi in _CUMULATIVE_DAYS):
            by_start[start].append((end, value))
    derived: dict[int, float] = {}
    for items in by_start.values():
        items.sort()
        for (end_a, value_a), (end_b, value_b) in zip(items, items[1:], strict=False):
            if _QUARTER_DAYS[0] <= end_b - end_a <= _QUARTER_DAYS[1] and end_b not in direct:
                derived.setdefault(end_b, value_b - value_a)
    return {**derived, **direct}


def _annuals(periods: Mapping[tuple[int | None, int], float]) -> dict[int, float]:
    return {
        end: value
        for (start, end), value in periods.items()
        if start is not None and _ANNUAL_DAYS[0] <= end - start + 1 <= _ANNUAL_DAYS[1]
    }


def trailing_twelve_months(
    periods: Mapping[tuple[int | None, int], float], *, as_of: int
) -> tuple[float, int] | None:
    """(TTM value, newest period end), or None when four quarters or a fresh year are missing."""
    annual = _annuals(periods)
    quarters = discrete_quarters(periods)
    ends = set(annual) | set(quarters)
    if not ends:
        return None
    newest = max(ends)
    if as_of - newest > MAX_PERIOD_AGE_DAYS:
        return None
    if newest in annual:
        return annual[newest], newest
    chain = [newest]
    total = quarters[newest]
    for _ in range(3):
        earlier = [e for e in quarters if _QUARTER_DAYS[0] <= chain[-1] - e <= _QUARTER_DAYS[1]]
        if not earlier:
            return None
        chosen = max(earlier)
        chain.append(chosen)
        total += quarters[chosen]
    return total, newest


def latest_instant(
    periods: Mapping[tuple[int | None, int], float], *, as_of: int
) -> tuple[float, int] | None:
    points = {end: value for (start, end), value in periods.items() if start is None}
    if not points:
        return None
    newest = max(points)
    if as_of - newest > MAX_PERIOD_AGE_DAYS:
        return None
    return points[newest], newest


def annual_cagr(
    periods: Mapping[tuple[int | None, int], float], years: int, *, as_of: int
) -> float | None:
    annual = _annuals(periods)
    if not annual:
        return None
    newest = max(annual)
    if as_of - newest > MAX_ANNUAL_AGE_DAYS:
        return None
    target = newest - round(365.25 * years)
    candidates = [e for e in annual if abs(e - target) <= _ANNUAL_MATCH_TOLERANCE_DAYS]
    if not candidates:
        return None
    base_end = min(candidates, key=lambda e: abs(e - target))
    latest_value, base_value = annual[newest], annual[base_end]
    if latest_value <= 0 or base_value <= 0:
        return None
    return (latest_value / base_value) ** (1.0 / years) - 1.0


def _has_recent_period(
    periods: Mapping[tuple[int | None, int], float], *, as_of: int
) -> bool:
    return any(as_of - end <= MAX_PERIOD_AGE_DAYS for (_, end) in periods)


def _total_debt(state: AsOfState, *, as_of: int, balance_sheet_end: int | None) -> float | None:
    """Total debt on one balance-sheet date, or None when no long-term figure is tagged.

    A balance sheet that tags only short-term borrowings (JPM since 2014: its
    long-term debt has no us-gaap balance tag in companyfacts) would otherwise
    read as a low-debt company. Absence of every long-term tag is unmeasured.
    """
    names = (
        "debt_combined",
        "long_term_debt_total",
        "long_term_debt_noncurrent",
        "long_term_debt_current",
        "short_term_borrowings",
        "commercial_paper",
        "debt_current",
    )
    points = {
        name: {end: v for (start, end), v in state.resolved(name).items() if start is None}
        for name in names
    }
    if balance_sheet_end is None:
        ends = [end for series in points.values() for end in series]
        if not ends:
            return None
        balance_sheet_end = max(ends)
    if as_of - balance_sheet_end > MAX_PERIOD_AGE_DAYS:
        return None
    at = {name: series.get(balance_sheet_end) for name, series in points.items()}
    if at["debt_combined"] is not None:
        return at["debt_combined"]
    if at["long_term_debt_total"] is not None:
        long_term = at["long_term_debt_total"]
        current_portion_counted = True
    elif at["long_term_debt_noncurrent"] is not None:
        long_term = at["long_term_debt_noncurrent"] + (at["long_term_debt_current"] or 0.0)
        current_portion_counted = at["long_term_debt_current"] is not None
    else:
        return None
    short_term = (at["short_term_borrowings"] or 0.0) + (at["commercial_paper"] or 0.0)
    short_term_tagged = at["short_term_borrowings"] is not None or at["commercial_paper"] is not None
    if not current_portion_counted and not short_term_tagged and at["debt_current"] is not None:
        # `DebtCurrent` already aggregates the current portion, borrowings and paper.
        short_term = at["debt_current"]
    return long_term + short_term


def _clip(value: float | None, low: float, high: float) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return max(low, min(high, value))


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


@dataclass(frozen=True)
class FilingEvent:
    """What was known right after one filing date: everything price-independent."""

    filed: int
    accession: str
    raw: dict[str, float | None]
    shares_current_basis: float | None


PRICE_INDEPENDENT_FIELDS = (
    "roe",
    "debt_to_equity",
    "gross_margin",
    "current_ratio",
    "revenue_growth_3y",
    "eps_growth_3y",
    "capex_growth_5y",
    "payout_ratio",
)
PRICE_FIELDS = ("pe_ratio", "pb_ratio", "fcf_yield")
V1_FIELDS = (
    "roe",
    "debt_to_equity",
    "gross_margin",
    "current_ratio",
    "pe_ratio",
    "pb_ratio",
    "fcf_yield",
    "revenue_growth_3y",
    "eps_growth_3y",
    "capex_growth_5y",
    "payout_ratio",
)
RAW_FIELDS = (
    "close_raw",
    "market_cap_raw",
    "shares_outstanding_raw",
    "net_income_ttm_raw",
    "revenue_ttm_raw",
    "gross_profit_ttm_raw",
    "equity_raw",
    "total_debt_raw",
    "assets_current_raw",
    "liabilities_current_raw",
    "assets_total_raw",
    "liabilities_total_raw",
    "fcf_ttm_raw",
    "dividends_ttm_raw",
)
SESSION_COLUMNS: tuple[str, ...] = (
    "ticker",
    "cik",
    "knowledge_date",
    "schema_version",
    "latest_filed",
    "latest_accession",
    *V1_FIELDS,
    *RAW_FIELDS,
)


def resolve_event(state: AsOfState, *, as_of: int, splits: SplitEvents) -> dict[str, Any]:
    """Every price-independent quantity and field as known on ``as_of``."""
    revenue = trailing_twelve_months(state.resolved("revenue"), as_of=as_of)
    cost = trailing_twelve_months(state.resolved("cost_of_revenue"), as_of=as_of)
    gross = trailing_twelve_months(state.resolved("gross_profit"), as_of=as_of)
    income = trailing_twelve_months(state.resolved("net_income"), as_of=as_of)
    cash = trailing_twelve_months(state.resolved("operating_cash_flow"), as_of=as_of)
    capex = trailing_twelve_months(state.resolved("capex"), as_of=as_of)
    dividend_periods = state.resolved("dividends_paid")
    dividends = trailing_twelve_months(dividend_periods, as_of=as_of)
    equity = latest_instant(state.resolved("equity"), as_of=as_of)
    assets = latest_instant(state.resolved("assets_current"), as_of=as_of)
    liabilities = latest_instant(state.resolved("liabilities_current"), as_of=as_of)
    assets_total = latest_instant(state.resolved("assets_total"), as_of=as_of)
    liabilities_total = latest_instant(state.resolved("liabilities_total"), as_of=as_of)

    revenue_value = revenue[0] if revenue else None
    if gross is not None and revenue is not None and gross[1] == revenue[1]:
        gross_value: float | None = gross[0]
    elif cost is not None and revenue is not None and cost[1] == revenue[1]:
        gross_value = revenue[0] - cost[0]
    else:
        gross_value = None
    income_value = income[0] if income else None
    equity_value = equity[0] if equity else None
    fcf_value = (
        cash[0] - capex[0] if cash is not None and capex is not None and cash[1] == capex[1] else None
    )
    if dividends is not None:
        dividends_value: float | None = dividends[0]
    elif cash is not None and not _has_recent_period(dividend_periods, as_of=as_of):
        dividends_value = 0.0
    else:
        dividends_value = None
    current_ratio = (
        _ratio(assets[0], liabilities[0])
        if assets is not None and liabilities is not None and assets[1] == liabilities[1]
        else None
    )
    debt = _total_debt(state, as_of=as_of, balance_sheet_end=equity[1] if equity else None)

    positive_equity = equity_value is not None and equity_value > 0
    raw = {
        "net_income_ttm_raw": income_value,
        "revenue_ttm_raw": revenue_value,
        "gross_profit_ttm_raw": gross_value,
        "equity_raw": equity_value,
        "total_debt_raw": debt,
        "assets_current_raw": assets[0] if assets else None,
        "liabilities_current_raw": liabilities[0] if liabilities else None,
        "assets_total_raw": assets_total[0] if assets_total else None,
        "liabilities_total_raw": liabilities_total[0] if liabilities_total else None,
        "fcf_ttm_raw": fcf_value,
        "dividends_ttm_raw": dividends_value,
    }
    fields = {
        "roe": _clip(_ratio(income_value, equity_value), -1.0, 1.0) if positive_equity else None,
        "debt_to_equity": (
            _clip(_ratio(debt, equity_value) / 2.0, -3.0, 3.0)
            if positive_equity and debt is not None
            else None
        ),
        "gross_margin": (
            _clip(_ratio(gross_value, revenue_value), 0.0, 1.0)
            if revenue_value is not None and revenue_value > 0
            else None
        ),
        "current_ratio": (
            _clip(current_ratio / 3.0, 0.0, 3.0)
            if current_ratio is not None and liabilities is not None and liabilities[0] > 0
            else None
        ),
        "revenue_growth_3y": _clip(
            annual_cagr(state.resolved("revenue"), 3, as_of=as_of), -0.5, 1.5
        ),
        "eps_growth_3y": _clip(annual_cagr(state.resolved("eps_diluted"), 3, as_of=as_of), -1.0, 2.0),
        "capex_growth_5y": _clip(annual_cagr(state.resolved("capex"), 5, as_of=as_of), -1.0, 2.0),
        "payout_ratio": (
            _clip(_ratio(dividends_value, income_value), 0.0, 2.0)
            if income_value is not None and income_value > 0
            else None
        ),
    }
    # Share facts entered the state on the current split basis (`filing_events`).
    shares = latest_instant(state.resolved("shares_outstanding"), as_of=as_of)
    if shares is None:
        weighted = {
            period: v
            for period, v in state.resolved("diluted_weighted_shares").items()
            if period[0] is not None
            and (
                _QUARTER_DAYS[0] <= period[1] - period[0] + 1 <= _QUARTER_DAYS[1]
                or _ANNUAL_DAYS[0] <= period[1] - period[0] + 1 <= _ANNUAL_DAYS[1]
            )
        }
        if weighted:
            newest = max(weighted, key=lambda p: p[1])
            if as_of - newest[1] <= MAX_PERIOD_AGE_DAYS:
                shares = (weighted[newest], newest[1])
    shares_current = shares[0] if shares and shares[0] > 0 else None
    return {"raw": raw, "fields": fields, "shares_current_basis": shares_current}


def filing_events(
    facts: Sequence[Fact],
    *,
    splits: SplitEvents,
    evaluate_from: int,
    share_rejections: list[ShareRejection] | None = None,
    cik: int | None = None,
) -> list[FilingEvent]:
    """One event per distinct filing date, resolved from every fact filed on or before it.

    Events before ``evaluate_from`` are folded into the state but not resolved,
    except the last one before it, which a session at the start of the range reads.
    Share-count facts `ShareCountScreen` refuses never enter the state; they are
    appended to ``share_rejections`` when it is given. ``cik`` selects the
    `SHARE_FACTS_REFUSED` entries that apply.
    """
    state = AsOfState()
    events: list[FilingEvent] = []
    dates = sorted({f.filed for f in facts})
    last_before = max((d for d in dates if d < evaluate_from), default=None)
    index = 0
    screen = ShareCountScreen(splits, refused_share_concepts(cik))
    for filed in dates:
        accession = ""
        batch: list[Fact] = []
        while index < len(facts) and facts[index].filed == filed:
            batch.append(facts[index])
            index += 1
        dropped = screen.screen(filed, batch)
        for position, fact in enumerate(batch):
            accession = max(accession, fact.accession)
            if position in dropped:
                continue
            value = fact.value
            if _QUANTITY_BY_NAME[fact.quantity].per_share:
                # Per-share values move with splits; put each on the current basis from
                # the basis it was filed on.
                value = value * split_factor_after(splits, fact.filed)
            elif fact.quantity in SHARE_QUANTITIES:
                value = share_count_current_basis(fact, splits)
            state.add(fact, value)
        if filed < evaluate_from and filed != last_before:
            continue
        resolved = resolve_event(state, as_of=filed, splits=splits)
        events.append(
            FilingEvent(
                filed=filed,
                accession=accession,
                raw={**resolved["raw"], **resolved["fields"]},
                shares_current_basis=resolved["shares_current_basis"],
            )
        )
    if share_rejections is not None:
        share_rejections.extend(screen.rejections)
    return events


def session_row(
    *, ticker: str, cik: int, label: dt.date, event: FilingEvent, close: float | None
) -> dict[str, Any]:
    raw = event.raw
    market_cap = (
        close * event.shares_current_basis
        if close is not None and close > 0 and event.shares_current_basis is not None
        else None
    )
    row: dict[str, Any] = {
        "ticker": ticker,
        "cik": int(cik),
        "knowledge_date": label.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "latest_filed": dt.date.fromordinal(event.filed).isoformat(),
        "latest_accession": event.accession,
    }
    for name in PRICE_INDEPENDENT_FIELDS:
        row[name] = raw[name]
    income, equity = raw["net_income_ttm_raw"], raw["equity_raw"]
    row["pe_ratio"] = _clip(_ratio(market_cap, income) / 30.0, -3.0, 3.0) if (
        market_cap is not None and income not in (None, 0)
    ) else None
    row["pb_ratio"] = _clip(_ratio(market_cap, equity) / 5.0, -3.0, 3.0) if (
        market_cap is not None and equity not in (None, 0)
    ) else None
    row["fcf_yield"] = (
        _clip(_ratio(raw["fcf_ttm_raw"], market_cap), -0.5, 0.5)
        if market_cap is not None and raw["fcf_ttm_raw"] is not None
        else None
    )
    row["close_raw"] = close
    row["market_cap_raw"] = market_cap
    row["shares_outstanding_raw"] = event.shares_current_basis
    for name in RAW_FIELDS:
        row.setdefault(name, raw.get(name))
    return row


def session_frame(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows), columns=list(SESSION_COLUMNS))
    for column in ("ticker", "knowledge_date", "latest_filed", "latest_accession"):
        frame[column] = frame[column].astype("string")
    for column in ("cik", "schema_version"):
        frame[column] = frame[column].astype("int64")
    for column in (*V1_FIELDS, *RAW_FIELDS):
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
    return frame.sort_values("ticker").reset_index(drop=True)


def materialize_sessions(
    events_by_ticker: Mapping[str, tuple[int, Sequence[FilingEvent]]],
    closes: pd.DataFrame,
    sessions: Sequence[dt.date],
) -> dict[dt.date, pd.DataFrame]:
    """{session label: frame}; a ticker appears once it has a filing on or before the label."""
    out: dict[dt.date, pd.DataFrame] = {}
    close_index = {d: i for i, d in enumerate(pd.DatetimeIndex(closes.index).date)}
    for label in sessions:
        ordinal = label.toordinal()
        position = close_index.get(label)
        rows = []
        for ticker, (cik, events) in events_by_ticker.items():
            filed = [e.filed for e in events]
            at = bisect.bisect_right(filed, ordinal) - 1
            if at < 0:
                continue
            close = None
            if position is not None and ticker in closes.columns:
                value = closes.iloc[position][ticker]
                close = float(value) if pd.notna(value) and value > 0 else None
            rows.append(session_row(ticker=ticker, cik=cik, label=label, event=events[at], close=close))
        out[label] = session_frame(rows)
    return out


# ── gates ───────────────────────────────────────────────────────────────────

#: The crucible consumer's `GROUP_COVERAGE_FLOOR_RATIO`, applied at write time too.
COVERAGE_FLOOR_RATIO = 0.90
CROSSCHECK_MIN_NAMES = 100
CROSSCHECK_MIN_SPEARMAN = 0.60
CROSSCHECK_MAX_MEDIAN_ABS_LOG_RATIO = math.log(1.5)
_CROSSCHECK_OUTLIER_RATIO = 3.0


class GateFailed(RuntimeError):
    """A pre-write gate refused the build; nothing was written."""


class CrosscheckNotComparable(GateFailed):
    """The v1 snapshot cannot be compared at all (too few usable P/E values, no column)."""


def coverage_reading(frame: pd.DataFrame, eligible: Sequence[str]) -> dict[str, Any]:
    wanted = set(eligible)
    if not wanted:
        raise GateFailed("the eligible universe is empty; there is nothing to measure coverage over")
    covered = sorted(wanted & set(frame["ticker"].astype(str)))
    return {
        "covered": len(covered),
        "eligible": len(wanted),
        "ratio": len(covered) / len(wanted),
        "missing": sorted(wanted - set(covered)),
    }


def assert_coverage(frame: pd.DataFrame, eligible: Sequence[str], *, label: dt.date) -> dict:
    reading = coverage_reading(frame, eligible)
    if reading["ratio"] < COVERAGE_FLOOR_RATIO:
        raise GateFailed(
            f"session {label} covers {reading['covered']} of {reading['eligible']} eligible names "
            f"({reading['ratio']:.3f}), below the {COVERAGE_FLOOR_RATIO:.2f} floor; first missing: "
            f"{reading['missing'][:20]}"
        )
    return reading


def crosscheck_against_v1(frame: pd.DataFrame, v1_snapshot: pd.DataFrame) -> dict[str, Any]:
    """Compare EDGAR trailing P/E with v1's vendor P/E on one snapshot; raise on disagreement."""
    ours = frame.set_index("ticker")
    edgar_pe = ours["market_cap_raw"] / ours["net_income_ttm_raw"]
    theirs = v1_snapshot.drop_duplicates("ticker", keep="last").set_index("ticker")
    if "pe_ratio" not in theirs.columns:
        raise CrosscheckNotComparable("the v1 snapshot carries no `pe_ratio`; the cross-check cannot run")
    # v1 stores trailing P/E / 30 clipped to [-3, 3]; only unclipped positive values are levels.
    vendor = theirs["pe_ratio"].astype(float)
    vendor_pe = (vendor * 30.0).where((vendor > 0) & (vendor < 3.0))
    joined = pd.DataFrame({"edgar": edgar_pe, "vendor": vendor_pe}).dropna()
    joined = joined[(joined["edgar"] > 0) & (joined["edgar"] < 90.0)]
    reading: dict[str, Any] = {"names": int(len(joined))}
    if len(joined) < CROSSCHECK_MIN_NAMES:
        raise CrosscheckNotComparable(
            f"only {len(joined)} names carry a positive P/E on both sides, under the "
            f"{CROSSCHECK_MIN_NAMES}-name minimum; the cross-check cannot establish agreement"
        )
    log_ratio = (joined["edgar"] / joined["vendor"]).map(math.log)
    # Spearman as the Pearson correlation of average-tie ranks (pandas' own
    # method="spearman" needs scipy, which this repo does not ship).
    reading["spearman"] = float(joined["edgar"].rank().corr(joined["vendor"].rank()))
    reading["median_abs_log_ratio"] = float(log_ratio.abs().median())
    reading["outliers_over_3x"] = sorted(
        str(t) for t in log_ratio.index[log_ratio.abs() > math.log(_CROSSCHECK_OUTLIER_RATIO)]
    )
    if (
        reading["spearman"] < CROSSCHECK_MIN_SPEARMAN
        or reading["median_abs_log_ratio"] > CROSSCHECK_MAX_MEDIAN_ABS_LOG_RATIO
    ):
        raise GateFailed(
            f"EDGAR P/E disagrees with v1's vendor P/E over {len(joined)} names: Spearman "
            f"{reading['spearman']:.3f} (floor {CROSSCHECK_MIN_SPEARMAN}), median |log ratio| "
            f"{reading['median_abs_log_ratio']:.3f} (ceiling "
            f"{CROSSCHECK_MAX_MEDIAN_ABS_LOG_RATIO:.3f}). A units, share-count or split-basis "
            "error moves both; nothing was written"
        )
    return reading


#: Adjacent sessions whose share count moves by more than this, with no split
#: executed between them, refuse the run. Shares, not market cap: market cap is
#: price times shares, and a price can move 3x in a day for a real reason. A share
#: count cannot, outside a merger, a spin-off or a split.
SHARE_COUNT_JUMP_RATIO = 3.0

#: Jumps measured on the 2026-10-19 sessions that are not data errors this module
#: can correct, keyed by (ticker, the later session's `latest_filed`). Each one is
#: reported in the run summary on every run that crosses it. Add an entry only with
#: a reason a reader can check; never to get a run through.
SHARE_COUNT_JUMPS_DECLARED: Mapping[tuple[str, str], str] = {
    ("ECHO", "2024-02-29"): (
        "EchoStar's DISH Network acquisition closed 2023-12-31; the FY2023 10-K reports "
        "270.8M diluted weighted shares against 83.9M in Q3 2023, prior years restated "
        "on the combined basis (common-control accounting)."
    ),
}


def assert_share_count_continuity(
    sessions: Mapping[dt.date, pd.DataFrame], splits: Mapping[str, SplitEvents]
) -> dict[str, Any]:
    """Refuse a run where any ticker's share count jumps between adjacent sessions.

    Every session the run built is checked, not just the newest: a mis-scaled
    share count in a 2024 filing reaches only the sessions it covers, and the v1
    cross-check has no snapshot before 2026 (alpha-engine-config-I12426). Only rows
    with a market cap are compared, so a name before its first traded close is not
    held to it.
    """
    labels = sorted(sessions)
    declared: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    split_exempt = 0
    pairs = 0
    previous: dict[str, float] = {}
    for prior, label in zip([None, *labels], labels, strict=False):
        frame = sessions[label]
        valid = frame[frame["market_cap_raw"].notna() & (frame["shares_outstanding_raw"] > 0)]
        current = dict(zip(valid["ticker"].astype(str), valid["shares_outstanding_raw"].astype(float), strict=True))
        filed = dict(zip(valid["ticker"].astype(str), valid["latest_filed"].astype(str), strict=True))
        if prior is not None:
            for ticker, shares in current.items():
                before = previous.get(ticker)
                if before is None:
                    continue
                pairs += 1
                ratio = shares / before
                if 1.0 / SHARE_COUNT_JUMP_RATIO <= ratio <= SHARE_COUNT_JUMP_RATIO:
                    continue
                if any(
                    prior.toordinal() < executed <= label.toordinal()
                    for executed, _ in splits.get(normalize_ticker(ticker), ())
                ):
                    split_exempt += 1
                    continue
                jump = {
                    "ticker": ticker,
                    "from_session": prior.isoformat(),
                    "to_session": label.isoformat(),
                    "latest_filed": filed[ticker],
                    "ratio": round(ratio, 6),
                }
                if (ticker, filed[ticker]) in SHARE_COUNT_JUMPS_DECLARED:
                    declared.append(jump)
                else:
                    violations.append(jump)
        previous = current
    reading = {
        "sessions": len(labels),
        "adjacent_pairs": pairs,
        "split_exempt": split_exempt,
        "declared": declared,
        "violations": len(violations),
    }
    if violations:
        raise GateFailed(
            f"{len(violations)} share-count jump(s) over {SHARE_COUNT_JUMP_RATIO:g}x between "
            f"adjacent sessions with no split between them, first: {violations[:20]}. A "
            "mis-scaled or misattributed share count moves market cap, P/E, P/B and FCF yield; "
            "nothing was written (alpha-engine-config-I12426)"
        )
    return reading


def crosscheck_history(
    sessions: Mapping[dt.date, pd.DataFrame], snapshots: Mapping[dt.date, pd.DataFrame]
) -> dict[str, Any]:
    """The v1 cross-check on every snapshot whose label is a built session.

    A snapshot that cannot be compared (v1's P/E was a constant before 2026-05)
    is counted, not failed. One that compares and disagrees refuses the run.
    """
    compared: dict[str, dict[str, Any]] = {}
    not_comparable: list[str] = []
    for label in sorted(snapshots):
        frame = sessions.get(label)
        if frame is None:
            continue
        try:
            compared[label.isoformat()] = crosscheck_against_v1(frame, snapshots[label])
        except CrosscheckNotComparable:
            not_comparable.append(label.isoformat())
        except GateFailed as exc:
            raise GateFailed(f"v1 snapshot {label}: {exc}") from exc
    outliers = sorted({t for reading in compared.values() for t in reading["outliers_over_3x"]})
    return {
        "snapshots_compared": len(compared),
        "snapshots_not_comparable": len(not_comparable),
        "first_compared": min(compared, default=None),
        "worst_spearman": min((r["spearman"] for r in compared.values()), default=None),
        "worst_median_abs_log_ratio": max(
            (r["median_abs_log_ratio"] for r in compared.values()), default=None
        ),
        "outliers_over_3x_any_snapshot": outliers,
    }


# ── storage, prices, splits ─────────────────────────────────────────────────


class ObjectStore(Protocol):
    def list_keys(self, prefix: str) -> list[str]: ...

    def get_bytes(self, key: str) -> bytes: ...

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None: ...


class S3ObjectStore:
    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def get_bytes(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)


class PriceReader(Protocol):
    def symbols(self) -> list[str]: ...

    def closes(self, symbols: Sequence[str], start: dt.date, end: dt.date) -> pd.DataFrame: ...


class ArcticPriceReader:
    """Split-adjusted `Close` from the ArcticDB `universe` library (in-region only)."""

    def __init__(self, bucket: str) -> None:
        from store.arctic_store import get_universe_lib

        self._lib = get_universe_lib(bucket)

    def symbols(self) -> list[str]:
        return sorted(self._lib.list_symbols())

    def closes(self, symbols: Sequence[str], start: dt.date, end: dt.date) -> pd.DataFrame:
        series = {}
        for symbol in symbols:
            data = self._lib.read(
                symbol,
                columns=["Close"],
                date_range=(pd.Timestamp(start), pd.Timestamp(end)),
            ).data
            series[symbol] = data["Close"]
        frame = pd.DataFrame(series)
        frame.index = pd.DatetimeIndex(frame.index).normalize()
        return frame.sort_index()


def registry_split_events(s3_client: Any, bucket: str) -> dict[str, list[tuple[int, float]]]:
    """Every registered split, per ticker, from the registry that restated `Close`."""
    from corporate_actions.registry import CorporateActionRegistry

    out: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for action in CorporateActionRegistry(s3_client, bucket).list_actions(types=["split"]):
        split_from, split_to = float(action.split_from), float(action.split_to)
        if split_from <= 0 or split_to <= 0:
            raise GateFailed(f"registered split {action.action_id} has a non-positive ratio")
        out[normalize_ticker(action.ticker)].append(
            (_ordinal(action.ex_date), split_from / split_to)
        )
    return {ticker: sorted(events) for ticker, events in out.items()}


#: How far before the first session the vendor split history must reach. A fact
#: the run uses can be filed this long before a session: the 3-year diluted-EPS
#: CAGR reaches four fiscal years back, plus filing lag and
#: `MAX_PERIOD_AGE_DAYS`. Seven years covers that with margin.
SPLIT_LOOKBACK_YEARS = 7

#: Splits that are public record, one per name. When one of these names is in the
#: universe and its date is inside the fetched window, the merged split set must
#: hold an event within `_SPLIT_DATE_TOLERANCE_DAYS` of it, or the run refuses.
#: The registry alone held only 2026 splits, and nothing noticed: NVDA's P/E was
#: about 10x off and CMG's about 50x off before their 2024 splits
#: (alpha-engine-config-I12418).
SPLIT_SOURCE_CANARIES: Mapping[str, str] = {
    "NVDA": "2024-06-10",
    "CMG": "2024-06-26",
    "AVGO": "2024-07-15",
    "AAPL": "2020-08-31",
}
_SPLIT_DATE_TOLERANCE_DAYS = 3


#: The smaller side of a share split's ratio is at most this. A forward N-for-1,
#: a reverse 1-for-N, a 3-for-2 and a 5% stock dividend (20:21) all pass.
#: Polygon also publishes spin-offs and other distributions in the splits feed,
#: as ratios like 1000:1061 or 1:1.0517. Those do not change the parent's share
#: count, and the ArcticDB `Close` is not restated for them, so applying one put
#: market cap and per-share fields off by the distribution's size. Measured on
#: the 2026-10-18 rebuild: MMM 1.196, GE 1.25, FTV 1.33, VZ 0.985
#: (alpha-engine-config-I12418).
_SPLIT_RATIO_MAX_SMALL_SIDE = 25


def is_share_split(split_from: float, split_to: float) -> bool:
    """True when the ratio is a share split or stock dividend, not a distribution."""
    if split_from <= 0 or split_to <= 0 or split_from == split_to:
        return False
    # Reduce the ratio first, so 1:1.5 reads as the 2:3 it is.
    ratio = split_to / split_from
    reduced = Fraction(ratio).limit_denominator(_SPLIT_RATIO_MAX_SMALL_SIDE)
    if not math.isclose(float(reduced), ratio, rel_tol=1e-6):
        return False
    return min(reduced.numerator, reduced.denominator) <= _SPLIT_RATIO_MAX_SMALL_SIDE


def vendor_split_events(
    rows: Iterable[Mapping[str, Any]], stats: dict[str, int] | None = None
) -> dict[str, list[tuple[int, float]]]:
    """Polygon `/v3/reference/splits` rows -> {ticker: [(ordinal, factor)]}, share splits only."""
    out: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for row in rows:
        split_from, split_to = float(row["split_from"]), float(row["split_to"])
        if not is_share_split(split_from, split_to):
            if stats is not None:
                stats["vendor_not_share_split"] = stats.get("vendor_not_share_split", 0) + 1
            continue
        out[normalize_ticker(str(row["ticker"]))].append(
            (_ordinal(str(row["execution_date"])[:10]), split_from / split_to)
        )
    return {ticker: sorted(events) for ticker, events in out.items()}


def merge_split_events(
    registry: Mapping[str, SplitEvents], vendor: Mapping[str, SplitEvents]
) -> tuple[dict[str, list[tuple[int, float]]], dict[str, int]]:
    """Union of the registry and the vendor history, one event per split.

    A vendor event within `_SPLIT_DATE_TOLERANCE_DAYS` of an event already kept
    (registry, or an earlier vendor row) is the same split. The registry's copy
    wins, because that is the factor that restated `Close`. When the two disagree on the factor it is counted as a
    conflict and the registry is still kept.
    """
    merged: dict[str, list[tuple[int, float]]] = {t: list(ev) for t, ev in registry.items()}
    stats = {"registry": 0, "vendor_added": 0, "duplicates": 0, "factor_conflicts": 0}
    stats["registry"] = sum(len(ev) for ev in registry.values())
    for ticker, events in vendor.items():
        known = merged.setdefault(ticker, [])
        for ordinal, factor in events:
            # Against everything kept so far, so a vendor row repeated in the
            # feed is not applied twice either.
            match = next(
                (r for r in known if abs(r[0] - ordinal) <= _SPLIT_DATE_TOLERANCE_DAYS), None
            )
            if match is None:
                known.append((ordinal, factor))
                stats["vendor_added"] += 1
            elif math.isclose(match[1], factor, rel_tol=1e-6):
                stats["duplicates"] += 1
            else:
                stats["factor_conflicts"] += 1
    return {t: sorted(ev) for t, ev in merged.items() if ev}, stats


#: Split factors fixed by review, keyed by (ticker, execution date): (factor, reason).
#: Each replaces every registry and vendor event for that ticker within
#: `_SPLIT_DATE_TOLERANCE_DAYS` of the date; a factor of 1.0 removes them. The factor
#: is the one that puts a share count on the basis of the stored `Close`, which is
#: what market cap multiplies. Add an entry only with evidence a reader can check.
SPLIT_FACTORS_DECLARED: Mapping[tuple[str, str], tuple[float, str]] = {
    ("DD", "2026-06-24"): (
        3.0,
        "DuPont's 1-for-3 reverse split, effective 12:01 a.m. 2026-06-24 (8-K "
        "0001666700-26-000034; the 10-Q of 2026-08-04 restates Q2 2025's diluted weighted "
        "average from 419.7M to 139.9M). Polygon's record (3:1) is right; the registry's "
        "operator correction d33c1022d713f6ad flipped it to 1:3, and applied together the "
        "two multiply to 1.0, so DD's share counts before 2026-08-04 read 3x high against "
        "a Close already on the post-split basis (alpha-engine-config-I12434).",
    ),
    ("HON", "2026-06-29"): (
        1.0,
        "Honeywell's Aerospace spin-off (one HONA per two HON) and its 1-for-2 reverse split "
        "both took effect 2026-06-29. The stored Close carries neither: it is continuous "
        "across the date, with the spin-off halving the price and the reverse split "
        "doubling it. Share counts are therefore not rebased, which keeps P/E with v1's "
        "before the date (2026-06-15: 1.22 against 1.03; rebased by 2 it would be 0.61). "
        "Sessions 2026-06-29 to 2026-07-22 still read the pre-split count, about 2x high, "
        "until the 10-Q of 2026-07-23. The registry holds polygon's 2:1 and an inverse "
        "operator correction (alpha-engine-config-I12434).",
    ),
}


def apply_declared_split_factors(
    splits: Mapping[str, SplitEvents],
) -> tuple[dict[str, list[tuple[int, float]]], list[str]]:
    """``splits`` with every `SPLIT_FACTORS_DECLARED` entry in place of what was there."""
    out: dict[str, list[tuple[int, float]]] = {t: list(ev) for t, ev in splits.items()}
    applied: list[str] = []
    for (ticker, date_text), (factor, _reason) in SPLIT_FACTORS_DECLARED.items():
        ordinal = _ordinal(date_text)
        kept = [ev for ev in out.get(ticker, []) if abs(ev[0] - ordinal) > _SPLIT_DATE_TOLERANCE_DAYS]
        if not math.isclose(factor, 1.0):
            kept.append((ordinal, factor))
        out[ticker] = sorted(kept)
        applied.append(f"{ticker} {date_text}")
    return {t: ev for t, ev in out.items() if ev}, applied


def check_cancelling_splits(splits: Mapping[str, SplitEvents], universe: Iterable[str]) -> None:
    """Raise when a universe name holds a split and its inverse on one date.

    The registry keeps a correction beside the record it corrects, so both reach
    this module, and the two factors multiply to 1.0: the split disappears and no
    gate sees it. Only one of the two can be right, and which one is a fact about
    the filing, so it goes in `SPLIT_FACTORS_DECLARED` (alpha-engine-config-I12434).
    """
    for ticker in sorted({normalize_ticker(t) for t in universe}):
        events = list(splits.get(ticker, ()))
        for i, (first, first_factor) in enumerate(events):
            for second, second_factor in events[i + 1 :]:
                if (
                    abs(first - second) <= _SPLIT_DATE_TOLERANCE_DAYS
                    and not math.isclose(first_factor, 1.0)
                    and math.isclose(first_factor * second_factor, 1.0, rel_tol=1e-6)
                ):
                    raise GateFailed(
                        f"split source holds {ticker}'s split on "
                        f"{dt.date.fromordinal(first).isoformat()} together with its inverse "
                        f"({first_factor:g} and {second_factor:g}), which cancel. Declare the "
                        "right factor in SPLIT_FACTORS_DECLARED (alpha-engine-config-I12434)."
                    )


def check_split_canaries(
    splits: Mapping[str, SplitEvents], universe: Iterable[str], *, window_start: dt.date
) -> list[str]:
    """The canaries that should have a split in `splits` and do; raises when one is missing."""
    names = {normalize_ticker(t) for t in universe}
    checked: list[str] = []
    for ticker, date_text in SPLIT_SOURCE_CANARIES.items():
        ordinal = _ordinal(date_text)
        if ticker not in names or ordinal < window_start.toordinal():
            continue
        if not any(
            abs(executed - ordinal) <= _SPLIT_DATE_TOLERANCE_DAYS for executed, _ in splits.get(ticker, ())
        ):
            raise GateFailed(
                f"split source is incomplete: no split for {ticker} near {date_text}. "
                "Per-share fields and market cap would sit on the pre-split basis "
                "(alpha-engine-config-I12418)."
            )
        checked.append(ticker)
    return checked


def resolve_split_events(
    s3_client: Any,
    bucket: str,
    *,
    universe: Sequence[str],
    start: dt.date,
    end: dt.date,
    fetch_vendor: Any = None,
) -> tuple[dict[str, list[tuple[int, float]]], dict[str, Any]]:
    """Registry splits plus the vendor's split history back to `SPLIT_LOOKBACK_YEARS` before `start`.

    `fetch_vendor(start_iso, end_iso)` returns polygon split rows. Any error it
    raises stops the run: an empty or partial history is the defect this exists
    to prevent.
    """
    window_start = dt.date(max(start.year - SPLIT_LOOKBACK_YEARS, 2000), 1, 1)
    if fetch_vendor is None:
        from polygon_client import polygon_client

        fetch_vendor = polygon_client().get_all_splits_between
    rows = fetch_vendor(window_start.isoformat(), end.isoformat())
    shape: dict[str, int] = {"vendor_not_share_split": 0}
    merged, stats = merge_split_events(
        registry_split_events(s3_client, bucket), vendor_split_events(rows, shape)
    )
    merged, declared = apply_declared_split_factors(merged)
    check_cancelling_splits(merged, universe)
    canaries = check_split_canaries(merged, universe, window_start=window_start)
    # Counts only: the rows are licensed vendor data and this repo's logs are public.
    summary = {
        "window_start": window_start.isoformat(),
        "vendor_rows": len(rows),
        **shape,
        **stats,
        "tickers_with_splits": len(merged),
        "declared_factors": declared,
        "canaries_checked": len(canaries),
    }
    logger.info("split events: %s", json.dumps(summary, sort_keys=True))
    return merged, summary


# ── the build ───────────────────────────────────────────────────────────────


@dataclass
class BuildResult:
    sessions: dict[dt.date, pd.DataFrame]
    facts_path: str | None
    summary: dict[str, Any] = field(default_factory=dict)


_FACT_COLUMNS = (
    "ticker",
    "cik",
    "taxonomy",
    "concept",
    "unit",
    "quantity",
    "start",
    "end",
    "value",
    "form",
    "filed",
    "accession",
)


def build(
    *,
    tickers: Sequence[str],
    cik_map: Mapping[str, int],
    facts_source: CompanyFactsSource,
    closes: pd.DataFrame,
    splits: Mapping[str, SplitEvents],
    start: dt.date,
    end: dt.date,
    facts_path: str | None = None,
) -> BuildResult:
    """Resolve every ticker's filing events and materialize the sessions in [start, end]."""
    stats = ExtractStats()
    share_rejections: dict[str, list[ShareRejection]] = {}
    events_by_ticker: dict[str, tuple[int, list[FilingEvent]]] = {}
    unmapped: list[str] = []
    without_facts: list[str] = []
    evaluate_from = start.toordinal() - MAX_PERIOD_AGE_DAYS
    documents: dict[int, dict[str, Any] | None] = {}
    writer = None
    try:
        for raw_ticker in sorted(set(tickers)):
            ticker = normalize_ticker(raw_ticker)
            cik = cik_map.get(ticker)
            if cik is None:
                unmapped.append(raw_ticker)
                continue
            if cik not in documents:
                documents.clear()  # one document in memory at a time; share classes are adjacent
                documents[cik] = facts_source.get(cik)
            document = documents[cik]
            facts = extract_facts(document, stats) if document else []
            if not facts:
                without_facts.append(raw_ticker)
                continue
            rejected: list[ShareRejection] = []
            events = filing_events(
                facts,
                splits=splits.get(ticker, ()),
                evaluate_from=evaluate_from,
                share_rejections=rejected,
                cik=cik,
            )
            events_by_ticker[raw_ticker] = (cik, events)
            if rejected:
                share_rejections[raw_ticker] = rejected
            if facts_path is not None:
                writer = _append_facts(writer, facts_path, raw_ticker, cik, facts)
    finally:
        if writer is not None:
            writer.close()
    sessions = [
        d for d in sorted(set(pd.DatetimeIndex(closes.index).date)) if start <= d <= end
    ]
    frames = materialize_sessions(events_by_ticker, closes, sessions)
    summary = {
        "tickers_requested": len(set(tickers)),
        "tickers_with_events": len(events_by_ticker),
        "unmapped_tickers": sorted(unmapped),
        "tickers_without_facts": sorted(without_facts),
        "facts_admitted": stats.admitted,
        "facts_wrong_form": stats.wrong_form,
        "facts_malformed_skipped": stats.malformed,
        "share_facts_rejected": _share_rejection_summary(share_rejections, since=evaluate_from),
        "sessions_built": len(frames),
        "first_session": sessions[0].isoformat() if sessions else None,
        "last_session": sessions[-1].isoformat() if sessions else None,
    }
    return BuildResult(sessions=frames, facts_path=facts_path, summary=summary)


def _share_rejection_summary(
    rejections: Mapping[str, Sequence[ShareRejection]], *, since: int
) -> dict[str, Any]:
    """Counts for the run summary; the filings named are the ones this run's sessions can read."""
    by_reason: dict[str, int] = defaultdict(int)
    recent: dict[str, list[str]] = {}
    for ticker, items in sorted(rejections.items()):
        for item in items:
            by_reason[item.reason] += 1
        filings = sorted(
            {
                f"{dt.date.fromordinal(r.filed).isoformat()} {r.concept} 10^{r.shift}"
                if r.reason == "scale"
                else f"{dt.date.fromordinal(r.filed).isoformat()} {r.concept} {r.reason}"
                for r in items
                if r.filed >= since and r.end >= since - MAX_PERIOD_AGE_DAYS
            }
        )
        if filings:
            recent[ticker] = filings
    return {"by_reason": dict(by_reason), "tickers": len(rejections), "in_range": recent}


def _append_facts(writer: Any, path: str, ticker: str, cik: int, facts: Sequence[Fact]) -> Any:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "ticker": [ticker] * len(facts),
            "cik": [int(cik)] * len(facts),
            "taxonomy": [f.taxonomy for f in facts],
            "concept": [f.concept for f in facts],
            "unit": [f.unit for f in facts],
            "quantity": [f.quantity for f in facts],
            "start": [dt.date.fromordinal(f.start).isoformat() if f.start else None for f in facts],
            "end": [dt.date.fromordinal(f.end).isoformat() for f in facts],
            "value": [f.value for f in facts],
            "form": [f.form for f in facts],
            "filed": [dt.date.fromordinal(f.filed).isoformat() for f in facts],
            "accession": [f.accession for f in facts],
        }
    ).select(list(_FACT_COLUMNS))
    if writer is None:
        writer = pq.ParquetWriter(path, table.schema)
    writer.write_table(table)
    return writer


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def v1_snapshot_labels(store: ObjectStore) -> list[dt.date]:
    import re

    pattern = re.compile(r"^features/(\d{4}-\d{2}-\d{2})/fundamental\.parquet$")
    return sorted(
        dt.date.fromisoformat(m.group(1))
        for key in store.list_keys("features/")
        if (m := pattern.match(key))
    )


def latest_v1_snapshot(store: ObjectStore, on_or_before: dt.date) -> tuple[str, pd.DataFrame]:
    labels = v1_snapshot_labels(store)
    admissible = [d for d in labels if d <= on_or_before]
    if not admissible:
        raise GateFailed(f"no v1 fundamentals snapshot is labelled on or before {on_or_before}")
    key = f"features/{admissible[-1].isoformat()}/fundamental.parquet"
    return key, pd.read_parquet(io.BytesIO(store.get_bytes(key)))


def run(
    *,
    store: ObjectStore,
    tickers: Sequence[str],
    cik_map: Mapping[str, int],
    facts_source: CompanyFactsSource,
    closes: pd.DataFrame,
    splits: Mapping[str, SplitEvents],
    start: dt.date,
    end: dt.date,
    eligible: Sequence[str],
    overwrite: bool = False,
    dry_run: bool = False,
    crosscheck_disabled_reason: str | None = None,
    run_date: dt.date | None = None,
    run_id: str | None = None,
    work_dir: str | None = None,
) -> dict[str, Any]:
    """Build, gate, then write. A failed gate raises before any object is written."""
    run_date = run_date or dt.datetime.now(dt.UTC).date()
    run_id = run_id or uuid.uuid4().hex[:12]
    started = dt.datetime.now(dt.UTC).isoformat()
    work_dir = work_dir or tempfile.mkdtemp(prefix="edgar-pit-")
    local_facts = os.path.join(work_dir, f"facts-{run_id}.parquet")
    result = build(
        tickers=tickers,
        cik_map=cik_map,
        facts_source=facts_source,
        closes=closes,
        splits=splits,
        start=start,
        end=end,
        facts_path=local_facts,
    )
    if not result.sessions:
        raise GateFailed(f"no price sessions fall in [{start}, {end}]; nothing to materialize")
    newest = max(result.sessions)
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "started_at": started,
        "companyfacts_source": facts_source.name,
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        **result.summary,
    }
    summary["coverage_newest_session"] = {
        "session": newest.isoformat(),
        **{k: v for k, v in assert_coverage(result.sessions[newest], eligible, label=newest).items()},
    }
    ratios = [coverage_reading(frame, eligible)["ratio"] for frame in result.sessions.values()]
    summary["coverage_ratio_by_session"] = {
        "min": min(ratios),
        "median": float(pd.Series(ratios).median()),
        "max": max(ratios),
    }
    summary["share_count_continuity"] = assert_share_count_continuity(result.sessions, splits)
    if crosscheck_disabled_reason:
        summary["crosscheck"] = {"disabled": crosscheck_disabled_reason}
    else:
        key, snapshot = latest_v1_snapshot(store, newest)
        label = dt.date.fromisoformat(key.split("/")[1])
        ours = result.sessions.get(label)
        if ours is None:
            raise GateFailed(
                f"the newest v1 snapshot {key} is labelled {label}, a session this run did not "
                "build; widen the range to include it or disable the cross-check with a reason"
            )
        summary["crosscheck"] = {"snapshot": key, **crosscheck_against_v1(ours, snapshot)}
        # Every earlier snapshot this run built a session for, too (alpha-engine-config-I12426).
        history = {
            d: pd.read_parquet(io.BytesIO(store.get_bytes(f"features/{d.isoformat()}/fundamental.parquet")))
            for d in v1_snapshot_labels(store)
            if d != label and d in result.sessions
        }
        summary["crosscheck_history"] = crosscheck_history(result.sessions, history)

    existing = set(store.list_keys(SESSIONS_PREFIX))
    written, skipped = [], []
    for label, frame in sorted(result.sessions.items()):
        key = session_key(label)
        if key in existing and not overwrite:
            skipped.append(label.isoformat())
            continue
        if not dry_run:
            store.put_bytes(key, _parquet_bytes(frame), "application/octet-stream")
        written.append(label.isoformat())
    summary["sessions_written"] = len(written)
    summary["sessions_skipped_existing"] = len(skipped)
    summary["dry_run"] = dry_run
    if not dry_run and os.path.exists(local_facts):
        with open(local_facts, "rb") as handle:
            store.put_bytes(facts_key(run_date, run_id), handle.read(), "application/octet-stream")
        summary["facts_key"] = facts_key(run_date, run_id)
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    if not dry_run:
        summary_bytes = json.dumps(summary, indent=2, sort_keys=True, default=str).encode()
        store.put_bytes(run_summary_key(run_date, run_id), summary_bytes, "application/json")
        # alpha-engine-config-I10750: `runs/{run_date}/{run_id}.json` carries a
        # per-run, non-derivable `run_id` segment — the freshness monitor's
        # date-templated probe (nousergon_lib.artifact_freshness, `{date}` /
        # `{trading_day}` / `{cycle_label}` only) cannot resolve it, and the
        # registry's `*` producer-chosen-segment support (config-I10200)
        # explicitly forbids the wildcard occupying the LAST path segment
        # (which `{run_id}.json` would be here). Same shape as
        # `price_cache_freshness_sentinel` / `_write_feature_store_freshness_
        # sentinel`: write a FIXED-key pointer every successful run so the
        # registry watches one exact, unambiguous key instead of a
        # variable-cardinality prefix. Best-effort: a sentinel-write failure
        # must never fail a run that already wrote the real artifacts above.
        try:
            store.put_bytes(RUN_LATEST_KEY, summary_bytes, "application/json")
        except Exception:  # noqa: BLE001 — observability nicety, not load-bearing
            logger.warning("edgar_pit_fundamentals: failed to write %s", RUN_LATEST_KEY, exc_info=True)
    return summary


# ── CLI ─────────────────────────────────────────────────────────────────────


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.edgar_pit_fundamentals",
        description="Build the filing-date-indexed EDGAR fundamentals dataset (in-region).",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    for mode in ("backfill", "incremental"):
        p = sub.add_parser(mode)
        p.add_argument("--bucket", default=None, help="data bucket (default: store.arctic_store.DEFAULT_BUCKET)")
        p.add_argument("--tickers", default=None, help="comma-separated subset (default: the universe library)")
        p.add_argument("--companyfacts", choices=("zip", "api"), default="zip")
        p.add_argument("--overwrite", action="store_true")
        p.add_argument("--dry-run", action="store_true")
        p.add_argument(
            "--no-crosscheck",
            metavar="REASON",
            default=None,
            help="disable the v1 P/E cross-check; the reason is recorded in the run summary",
        )
        p.add_argument("--end", type=dt.date.fromisoformat, default=None)
        if mode == "backfill":
            p.add_argument("--start", type=dt.date.fromisoformat, required=True)
        else:
            p.add_argument("--lookback-sessions", type=int, default=15)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parser().parse_args(argv)
    if args.no_crosscheck is not None and not args.no_crosscheck.strip():
        raise SystemExit("--no-crosscheck needs a written reason")

    import boto3

    from features.compute import UNIVERSE_BENCHMARK_PROXIES
    from store.arctic_store import DEFAULT_BUCKET

    bucket = args.bucket or DEFAULT_BUCKET
    s3 = boto3.client("s3")
    store = S3ObjectStore(s3, bucket)
    prices = ArcticPriceReader(bucket)
    proxies = {normalize_ticker(t) for t in UNIVERSE_BENCHMARK_PROXIES}
    universe = (
        sorted({t.strip().upper() for t in args.tickers.split(",") if t.strip()})
        if args.tickers
        else [s for s in prices.symbols() if normalize_ticker(s) not in proxies]
    )
    if not universe:
        raise SystemExit("the universe resolved to no symbols")

    end = args.end or dt.datetime.now(dt.UTC).date()
    if args.mode == "backfill":
        start = args.start
        closes = prices.closes(universe, start, end)
    else:
        window = prices.closes(universe, end - dt.timedelta(days=45), end)
        recent = sorted(set(pd.DatetimeIndex(window.index).date))[-args.lookback_sessions :]
        if not recent:
            raise SystemExit(f"no price sessions in the 45 days before {end}")
        start = recent[0]
        closes = window
    newest_session = max(pd.DatetimeIndex(closes.index).date)
    eligible = [t for t in universe if t in closes.columns and pd.notna(closes[t].iloc[-1])]
    logger.info(
        "universe %d symbols, %d eligible on %s; range %s..%s", len(universe), len(eligible),
        newest_session, start, end,
    )

    client = SecClient()
    cik_map = ticker_cik_map(client.get_json(COMPANY_TICKERS_URL))
    work_dir = tempfile.mkdtemp(prefix="edgar-pit-")
    if args.companyfacts == "zip":
        path = os.path.join(work_dir, "companyfacts.zip")
        size = client.download(COMPANYFACTS_ZIP_URL, path)
        logger.info("downloaded companyfacts.zip: %d bytes", size)
        facts_source: CompanyFactsSource = ZipCompanyFacts(path)
    else:
        facts_source = ApiCompanyFacts(client)

    splits, split_summary = resolve_split_events(
        s3, bucket, universe=universe, start=start, end=end
    )
    summary = run(
        store=store,
        tickers=universe,
        cik_map=cik_map,
        facts_source=facts_source,
        closes=closes,
        splits=splits,
        start=start,
        end=end,
        eligible=eligible,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        crosscheck_disabled_reason=args.no_crosscheck,
        work_dir=work_dir,
    )
    summary["sec_requests"] = client.request_count
    summary["splits"] = split_summary
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
