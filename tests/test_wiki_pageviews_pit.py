"""collectors/wiki_pageviews_pit.py — Wikipedia pageviews per issuer."""

from __future__ import annotations

import datetime as dt
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from collectors import wiki_pageviews_pit as m
from collectors.edgar_pit_fundamentals import SecFetchError

_REPO = Path(__file__).resolve().parents[1]


def _binding(cik, url):
    return {"cik": {"value": cik}, "article": {"value": url}}


_SPARQL = {
    "results": {
        "bindings": [
            _binding("0000320193", "https://en.wikipedia.org/wiki/Apple_Inc."),
            _binding("789019", "https://en.wikipedia.org/wiki/Microsoft"),
            _binding("1", "https://en.wikipedia.org/wiki/AC%2FDC_Corp"),
            _binding("bad", "https://en.wikipedia.org/wiki/X"),
            _binding("2", "https://de.wikipedia.org/wiki/Y"),
        ]
    }
}


def _views(days):
    return {
        "items": [
            {"timestamp": d.strftime("%Y%m%d") + "00", "views": n} for d, n in days
        ]
    }


class _Response:
    def __init__(self, payload) -> None:
        self.payload = payload

    def json(self):
        return self.payload


class _Client:
    def __init__(self, articles) -> None:
        self.articles, self.urls = articles, []

    def get(self, url, **_):
        self.urls.append(url)
        if url.startswith(m.SPARQL_URL):
            return _Response(_SPARQL)
        for article, payload in self.articles.items():
            if f"/user/{article}/daily/" in url:
                return _Response(payload)
        raise SecFetchError("not found", status=404)


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects = {k: b"" for k in keys}

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body


def test_mapping_is_by_cik_and_keeps_only_english_articles() -> None:
    mapping = m.parse_mapping(_SPARQL)
    assert mapping == {320193: ["Apple_Inc."], 789019: ["Microsoft"], 1: ["AC/DC_Corp"]}
    assert "AC%2FDC_Corp" in m.pageviews_url(
        "AC/DC_Corp", dt.date(2017, 1, 1), dt.date(2017, 1, 2)
    )
    with pytest.raises(m.SourceFormatError):
        m.parse_mapping({"results": {"bindings": []}})


def test_unreadable_counts_stop_the_run() -> None:
    with pytest.raises(m.SourceFormatError, match="count"):
        m.parse_pageviews({"items": [{"timestamp": "2017010100", "views": None}]}, "A")
    with pytest.raises(m.SourceFormatError, match="items"):
        m.parse_pageviews({}, "A")


def test_run_writes_months_with_known_at_and_skips_404s() -> None:
    days = [(dt.date(2017, 1, 31), 10), (dt.date(2017, 2, 1), 20)]
    client = _Client({"Apple_Inc.": _views(days)})
    store = _Store()
    summary = m.run(
        store,
        client,
        [("AAPL", 320193), ("MSFT", 789019), ("ZZZ", 999)],
        [(2017, 1), (2017, 2)],
        run_date=dt.date(2026, 10, 21),
        through=dt.date(2026, 10, 20),
        overwrite=False,
        run_id="r1",
    )
    assert summary["issuers_mapped"] == 2 and summary["articles_without_views"] == 1
    assert summary["months"] == {"2017-01": 1, "2017-02": 1}
    jan = pd.read_parquet(io.BytesIO(store.objects[m.month_key(2017, 1)]))
    assert tuple(jan.columns) == m.ROW_COLUMNS
    row = jan.iloc[0]
    assert row["ticker"] == "AAPL" and row["views"] == 10
    assert row["known_at"] == dt.date(2017, 2, 2)
    assert json.loads(store.objects[m.RUN_LATEST_KEY])["limits"] == m.LIMITS
    assert f"{m.MAPPING_PREFIX}2026-10-21.parquet" in store.objects


def test_existing_months_are_not_refetched() -> None:
    store = _Store(keys=[m.month_key(2017, 1)])
    client = _Client({})
    summary = m.run(
        store,
        client,
        [("AAPL", 320193)],
        [(2017, 1)],
        run_date=dt.date(2026, 10, 21),
        through=dt.date(2026, 10, 20),
        overwrite=False,
    )
    assert summary["months_skipped_existing"] == 1 and client.urls == []


def test_no_views_anywhere_stops_the_run() -> None:
    with pytest.raises(m.SourceFormatError, match="no pageviews"):
        m.run(
            _Store(),
            _Client({}),
            [("AAPL", 320193)],
            [(2017, 1)],
            run_date=dt.date(2026, 10, 21),
            through=dt.date(2026, 10, 20),
            overwrite=False,
        )


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"wiki-pageviews-pit-backfill"' in source
    assert "python -m collectors.wiki_pageviews_pit backfill --start 2015-07" in source
