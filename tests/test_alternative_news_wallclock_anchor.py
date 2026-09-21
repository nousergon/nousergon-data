"""Tests for ``collectors/alternative.py::_fetch_news`` — alpha-engine-config
-I11216 deliverable 5: the Yahoo RSS 72h cutoff and the EDGAR 8-K 3-day
search window are CONTENT (they decide what lands in the artifact), and must
anchor on ``run_date``, not the wall clock. Mirrors ``_fetch_insider``'s
already-correct ``today = datetime.strptime(run_date, "%Y-%m-%d")`` shape in
this same file.
"""

from __future__ import annotations

import time as _time_mod
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from collectors import alternative


def _struct(dt: datetime) -> _time_mod.struct_time:
    return dt.astimezone(timezone.utc).timetuple()


def _feed_entry(title: str, published: datetime):
    return {
        "title": title,
        "link": f"https://x/{title}",
        "published_parsed": _struct(published),
        "source": {"title": "Yahoo Finance"},
    }


def _fake_feedparser_module(entries):
    mod = types.SimpleNamespace()

    def parse(url, **kwargs):
        feed = types.SimpleNamespace()
        feed.entries = entries
        return feed

    mod.parse = parse
    return mod


def _fake_edgar_response():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"hits": {"hits": []}}
    return resp


class _FixedNow(datetime):
    """``datetime`` subclass whose ``.now()`` returns a controllable fixed
    instant — lets a test move "the wall clock" without touching real time."""

    _fixed: datetime = datetime(2000, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls._fixed if tz is None else cls._fixed.astimezone(tz)


def test_fetch_news_uses_run_date_not_wall_clock(monkeypatch):
    """Moving the wall clock while holding ``run_date`` fixed must not
    change ``_fetch_news``'s output — proves the 72h Yahoo RSS cutoff and
    the EDGAR 8-K window are anchored on ``run_date``
    (alpha-engine-config-I11216 deliverable 5).

    The article is published 2h before run_date's midnight — well inside a
    72h window measured from run_date, but the wall clock moves by 74h
    between the two calls (more than the 72h window), which would push the
    article outside a wall-clock-anchored cutoff on the second call and
    keep it on the first under the defect.
    """
    run_date = "2026-09-18"
    anchor = datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc)
    published = anchor - timedelta(hours=2)
    entries = [_feed_entry("headline", published)]

    monkeypatch.setattr(alternative, "datetime", _FixedNow)

    with patch.dict("sys.modules", {"feedparser": _fake_feedparser_module(entries)}), \
         patch.object(alternative.requests, "get", return_value=_fake_edgar_response()):
        _FixedNow._fixed = anchor
        out_a = alternative._fetch_news("AAPL", run_date)

        _FixedNow._fixed = anchor + timedelta(hours=74)
        out_b = alternative._fetch_news("AAPL", run_date)

    assert out_a == out_b
    assert [a["headline"] for a in out_a["articles"]] == ["headline"]


def test_fetch_news_edgar_window_anchored_on_run_date(monkeypatch):
    """The EDGAR 8-K search's start/end dates must derive from run_date, not
    the wall clock."""
    run_date = "2026-09-18"
    captured_urls = []

    def _get(url, **kwargs):
        captured_urls.append(url)
        return _fake_edgar_response()

    monkeypatch.setattr(alternative, "datetime", _FixedNow)
    _FixedNow._fixed = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)  # replay a week later

    with patch.dict("sys.modules", {"feedparser": _fake_feedparser_module([])}), \
         patch.object(alternative.requests, "get", side_effect=_get):
        alternative._fetch_news("AAPL", run_date)

    assert captured_urls, "EDGAR 8-K search was never called"
    url = captured_urls[0]
    assert "enddt=2026-09-18" in url
    assert "startdt=2026-09-15" in url  # run_date - 3 days
