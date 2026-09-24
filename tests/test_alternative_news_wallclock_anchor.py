"""Tests for ``collectors/alternative.py::_fetch_news`` — alpha-engine-config
-I11216 deliverable 5: the Yahoo RSS 72h cutoff and the EDGAR 8-K 3-day
search window are CONTENT (they decide what lands in the artifact), and must
anchor on ``run_date`` for a DECLARED REPLAY (``shadow.root.active_root()``)
and on the real wall clock for a LIVE run — ``run_date`` is populated on
BOTH paths (``weekly_collector.py``'s ``args.date or default_run_date()``),
so its mere presence cannot tell the two apart. ``_fetch_insider`` now takes
the same anchor (alpha-engine-config-I11308,
``tests/test_alternative_insider_wallclock_anchor.py``).
"""

from __future__ import annotations

import datetime as dt
import time as _time_mod
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative
from shadow.root import ShadowRoot, activate, deactivate


@pytest.fixture
def declared_replay():
    """Activates a shadow root for the duration of the test — the in-process
    signal ``python -m shadow run`` sets before a single collector line runs
    (``shadow/root.py``), and the only thing ``_fetch_news`` now checks to
    decide whether ``run_date`` or the wall clock anchors its windows."""
    root = ShadowRoot(dt.date(2026, 9, 18))
    activate(root)
    try:
        yield root
    finally:
        deactivate()


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


def test_fetch_news_uses_run_date_not_wall_clock_under_a_declared_replay(monkeypatch, declared_replay):
    """Moving the wall clock while holding ``run_date`` fixed, UNDER A
    DECLARED REPLAY, must not change ``_fetch_news``'s output — proves the
    72h Yahoo RSS cutoff and the EDGAR 8-K window are anchored on
    ``run_date`` when ``active_root()`` says this is a replay
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


def test_fetch_news_edgar_window_anchored_on_run_date_under_a_declared_replay(monkeypatch, declared_replay):
    """The EDGAR 8-K search's start/end dates must derive from run_date, not
    the wall clock, when a replay is declared."""
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


def test_fetch_news_uses_wall_clock_on_a_live_run_no_declared_replay(monkeypatch):
    """Outside a declared replay (``active_root()`` is ``None`` — the live/
    scheduled path), ``_fetch_news`` must anchor on the REAL wall clock, not
    ``run_date``, even though ``run_date`` is populated (``weekly_
    collector.py`` always passes ``args.date or default_run_date()``).

    An article published 2h before the (later) wall clock, but MORE than 72h
    after ``run_date``'s midnight, must still be included — it would be
    wrongly dropped by a run_date-anchored cutoff, which is exactly the
    silent content-narrowing this fix prevents on Saturday.
    """
    assert alternative.active_root() is None, "no shadow root should be active in this test"

    run_date = "2026-09-18"  # what a Saturday run's default_run_date() resolves to (Friday)
    wall_clock = datetime(2026, 9, 20, 8, 0, tzinfo=timezone.utc)  # actual Saturday morning
    published = wall_clock - timedelta(hours=2)  # well inside 72h of the REAL wall clock,
    # but ~56h after run_date's 72h-from-midnight cutoff would have been
    entries = [_feed_entry("headline", published)]

    monkeypatch.setattr(alternative, "datetime", _FixedNow)
    _FixedNow._fixed = wall_clock

    with patch.dict("sys.modules", {"feedparser": _fake_feedparser_module(entries)}), \
         patch.object(alternative.requests, "get", return_value=_fake_edgar_response()):
        out = alternative._fetch_news("AAPL", run_date)

    assert [a["headline"] for a in out["articles"]] == ["headline"], (
        "a live run must see content published after run_date, up to the real wall clock"
    )
