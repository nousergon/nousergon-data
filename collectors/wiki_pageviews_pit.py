"""Point-in-time daily English Wikipedia pageviews per issuer.

Why this exists: the model zoo wants retail attention as a feature, with
history back to 2017. Wikipedia pageviews are free, daily, and go back to
2015-07-01, the start of the Wikimedia pageviews API.

Issuers: the newest EDGAR PIT facts run (`issuers_from_edgar_facts`), the same
universe as the filing-text and 8-K datasets. Each CIK is mapped to its English
Wikipedia article through Wikidata, which records the SEC Central Index Key as
property P5531. That is one SPARQL request for the whole universe, and it maps
by CIK, so no name matching is involved.

Source per article:
    wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/
        all-access/user/{article}/daily/{YYYYMMDD}/{YYYYMMDD}
`user` excludes traffic Wikimedia classifies as spiders or automated.

Known limits, recorded in every run record:
- The CIK-to-article map is as of the run date, not point in time. A company
  that gained its Wikidata CIK late is still mapped for its whole history.
- Pageviews count only the article's current title. Views made under an older
  title sit on a redirect and are not counted, so history before a rename reads
  low.
- Current names only; delisted issuers are absent from the universe.

Rows, one per (article, day):
    view_date, cik, ticker, article, views, known_at
Knowledge rule: use a row for session S only when `known_at <= S`.
`known_at` is `view_date` plus two days. Wikimedia publishes a UTC day's counts
within about a day of its end, so two days is conservative.

Output, one parquet per calendar month of `view_date`:

    fundamentals_pit/wiki_pageviews/v1/months/{YYYY-MM}.parquet
    fundamentals_pit/wiki_pageviews/v1/mapping/{run_date}.parquet   (cik -> article used)
    fundamentals_pit/wiki_pageviews/v1/runs/{run_date}/{run_id}.json  (+ runs/latest.json)

`backfill` writes every complete month from --start that is not already
written. `refresh` rewrites the previous and current month.

An article that answers 404 has no pageviews and contributes no rows. Any other
failure stops the run: a missing name would read as "no attention".
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import os
import urllib.parse
import uuid
from collections.abc import Sequence
from typing import Any

import pandas as pd

from collectors.edgar_pit_fundamentals import S3ObjectStore, SecClient, SecFetchError
from collectors.filing_text_pit import issuers_from_edgar_facts

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
DATASET_PREFIX = "fundamentals_pit/wiki_pageviews/v1"
MONTHS_PREFIX = f"{DATASET_PREFIX}/months/"
MAPPING_PREFIX = f"{DATASET_PREFIX}/mapping/"
RUNS_PREFIX = f"{DATASET_PREFIX}/runs/"
RUN_LATEST_KEY = f"{RUNS_PREFIX}latest.json"
DEFAULT_BUCKET = "alpha-engine-research"
API_START = dt.date(2015, 7, 1)
KNOWN_AFTER_DAYS = 2
USER_AGENT = (
    "nousergon-data/1.0 (https://github.com/nousergon/nousergon-data; research)"
)
REQUESTS_PER_SECOND = 5.0

SPARQL_URL = "https://query.wikidata.org/sparql"
SPARQL_QUERY = """
SELECT ?cik ?article WHERE {
  ?item wdt:P5531 ?cik .
  ?article schema:about ?item ;
           schema:isPartOf <https://en.wikipedia.org/> .
}
"""
PAGEVIEWS_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/"
    "all-access/user/{article}/daily/{start}/{end}"
)
ARTICLE_URL_PREFIX = "https://en.wikipedia.org/wiki/"

ROW_COLUMNS: tuple[str, ...] = (
    "view_date",
    "cik",
    "ticker",
    "article",
    "views",
    "known_at",
    "schema_version",
)
LIMITS = (
    "CIK-to-article map is as of the run date, not point in time; "
    "views under an article's former titles are not counted; "
    "current names only, no delisted issuers"
)


class SourceFormatError(RuntimeError):
    """Wikidata or the pageviews API answered in a shape this collector does not read."""


# -- mapping --------------------------------------------------------------------


def sparql_url() -> str:
    return f"{SPARQL_URL}?{urllib.parse.urlencode({'query': SPARQL_QUERY, 'format': 'json'})}"


def parse_mapping(payload: Any) -> dict[int, list[str]]:
    """CIK -> sorted English article titles (spaces as underscores)."""
    try:
        bindings = payload["results"]["bindings"]
    except (KeyError, TypeError) as error:
        raise SourceFormatError("SPARQL answer has no results.bindings") from error
    out: dict[int, set[str]] = {}
    for binding in bindings:
        raw_cik = str(binding.get("cik", {}).get("value", "")).strip()
        url = str(binding.get("article", {}).get("value", ""))
        if not raw_cik.isdigit() or not url.startswith(ARTICLE_URL_PREFIX):
            continue
        title = urllib.parse.unquote(url[len(ARTICLE_URL_PREFIX) :]).replace(" ", "_")
        if title:
            out.setdefault(int(raw_cik), set()).add(title)
    if not out:
        raise SourceFormatError("SPARQL answer maps no CIK to an English article")
    return {cik: sorted(titles) for cik, titles in out.items()}


# -- pageviews ------------------------------------------------------------------


def pageviews_url(article: str, start: dt.date, end: dt.date) -> str:
    return PAGEVIEWS_URL.format(
        article=urllib.parse.quote(article, safe=""),
        start=start.strftime("%Y%m%d"),
        end=end.strftime("%Y%m%d"),
    )


def parse_pageviews(payload: Any, article: str) -> list[tuple[dt.date, int]]:
    try:
        items = payload["items"]
    except (KeyError, TypeError) as error:
        raise SourceFormatError(
            f"pageviews answer for {article!r} has no items"
        ) from error
    out: list[tuple[dt.date, int]] = []
    for item in items:
        stamp = str(item.get("timestamp", ""))
        views = item.get("views")
        try:
            day = dt.datetime.strptime(stamp[:8], "%Y%m%d").date()
        except ValueError as error:
            raise SourceFormatError(
                f"pageviews timestamp {stamp!r} unreadable"
            ) from error
        if not isinstance(views, int) or views < 0:
            raise SourceFormatError(f"pageviews count {views!r} on {day} unreadable")
        out.append((day, views))
    return out


def fetch_article(
    client: Any, article: str, start: dt.date, end: dt.date
) -> list[tuple[dt.date, int]]:
    """Daily (date, views) for one article; empty when the API has no data (404)."""
    try:
        response = client.get(pageviews_url(article, start, end), timeout=120.0)
    except SecFetchError as error:
        if error.status == 404:
            return []
        raise
    return parse_pageviews(response.json(), article)


# -- months ---------------------------------------------------------------------


def parse_month(text: str) -> tuple[int, int]:
    try:
        year, month = (int(part) for part in text.split("-"))
        dt.date(year, month, 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"{text!r} is not YYYY-MM") from error
    return year, month


def month_label(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def month_key(year: int, month: int) -> str:
    return f"{MONTHS_PREFIX}{month_label(year, month)}.parquet"


def months_between(
    first: tuple[int, int], last: tuple[int, int]
) -> list[tuple[int, int]]:
    out, (year, month) = [], first
    while (year, month) <= last:
        out.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def month_end(year: int, month: int) -> dt.date:
    nxt = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
    return nxt - dt.timedelta(days=1)


def _to_parquet(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False, compression="zstd")
    return buffer.getvalue()


# -- run ------------------------------------------------------------------------


def run(
    store: Any,
    client: Any,
    issuers: Sequence[tuple[str, int]],
    months: Sequence[tuple[int, int]],
    *,
    run_date: dt.date,
    through: dt.date,
    overwrite: bool,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or uuid.uuid4().hex[:12]
    existing = set() if overwrite else set(store.list_keys(MONTHS_PREFIX))
    todo = [ym for ym in months if month_key(*ym) not in existing]
    summary: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "limits": LIMITS,
        "issuers": len(issuers),
        "months_requested": len(months),
        "months_skipped_existing": len(months) - len(todo),
        "months_written": 0,
        "rows_written": 0,
    }
    if todo:
        first = max(API_START, dt.date(todo[0][0], todo[0][1], 1))
        last = min(through, month_end(*todo[-1]))
        mapping = parse_mapping(client.get(sparql_url(), timeout=300.0).json())
        tickers = {cik: ticker for ticker, cik in issuers}
        mapped = [
            (cik, article)
            for cik in sorted(tickers)
            for article in mapping.get(cik, [])
        ]
        summary.update(
            {
                "issuers_mapped": len({cik for cik, _ in mapped}),
                "articles": len(mapped),
                "articles_without_views": 0,
            }
        )
        rows: list[tuple[dt.date, int, str, str, int]] = []
        for cik, article in mapped:
            views = fetch_article(client, article, first, last)
            if not views:
                summary["articles_without_views"] += 1
            rows += [(day, cik, tickers[cik], article, n) for day, n in views]
        map_frame = pd.DataFrame(mapped, columns=["cik", "article"])
        map_frame["ticker"] = map_frame["cik"].map(tickers).astype("string")
        store.put_bytes(
            f"{MAPPING_PREFIX}{run_date.isoformat()}.parquet",
            _to_parquet(map_frame),
            "application/octet-stream",
        )
        frame = pd.DataFrame(
            rows, columns=["view_date", "cik", "ticker", "article", "views"]
        )
        frame["known_at"] = frame["view_date"].map(
            lambda d: d + dt.timedelta(days=KNOWN_AFTER_DAYS)
        )
        frame["schema_version"] = SCHEMA_VERSION
        for column in ("ticker", "article"):
            frame[column] = frame[column].astype("string")
        frame["_month"] = frame["view_date"].map(lambda d: (d.year, d.month))
        months_written: dict[str, int] = {}
        for ym in todo:
            part = frame[frame["_month"] == ym]
            if part.empty:
                continue
            part = part.drop(columns="_month").sort_values(
                ["view_date", "cik", "article"]
            )[list(ROW_COLUMNS)]
            store.put_bytes(
                month_key(*ym),
                _to_parquet(part.reset_index(drop=True)),
                "application/octet-stream",
            )
            months_written[month_label(*ym)] = len(part)
        if not months_written:
            raise SourceFormatError(
                "no pageviews for any mapped article in the requested months"
            )
        summary["months_written"] = len(months_written)
        summary["rows_written"] = sum(months_written.values())
        summary["months"] = months_written
    summary["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    body = json.dumps(summary, indent=2, sort_keys=True).encode()
    store.put_bytes(
        f"{RUNS_PREFIX}{run_date.isoformat()}/{run_id}.json", body, "application/json"
    )
    store.put_bytes(RUN_LATEST_KEY, body, "application/json")
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m collectors.wiki_pageviews_pit",
        description="Point-in-time daily English Wikipedia pageviews per issuer.",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    backfill = sub.add_parser(
        "backfill", help="complete months from --start; write-if-absent"
    )
    backfill.add_argument("--start", default="2015-07")
    backfill.add_argument(
        "--end", help="last month, YYYY-MM (default: the previous month)"
    )
    backfill.add_argument("--overwrite", action="store_true")
    sub.add_parser("refresh", help="rewrite the previous and current month")
    parser.add_argument(
        "--bucket", default=os.environ.get("NE_RESEARCH_BUCKET", DEFAULT_BUCKET)
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    args = _parser().parse_args(argv)
    today = dt.date.today()
    previous = (
        (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    )
    if args.mode == "backfill":
        end = parse_month(args.end) if args.end else previous
        months, overwrite = months_between(parse_month(args.start), end), args.overwrite
    else:
        months, overwrite = [previous, (today.year, today.month)], True

    import boto3

    store = S3ObjectStore(boto3.client("s3"), args.bucket)
    client = SecClient(user_agent=USER_AGENT, requests_per_second=REQUESTS_PER_SECOND)
    summary = run(
        store,
        client,
        issuers_from_edgar_facts(store),
        months,
        run_date=today,
        through=today - dt.timedelta(days=1),
        overwrite=overwrite,
    )
    logger.info(
        "run %s: %d month(s) written, %d skipped, %d rows, %s of %d issuers mapped",
        summary["run_id"],
        summary["months_written"],
        summary["months_skipped_existing"],
        summary["rows_written"],
        summary.get("issuers_mapped", "-"),
        summary["issuers"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
