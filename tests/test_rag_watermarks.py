"""Watermark store + gap resolution (config-I5701).

``rag-corpus-policy.md`` §2.2: the expensive call is gated on what is already
held, and the window is *since the last successful ingest* rather than a fixed
rolling lookback.

The load-bearing assertion in this file is
``test_second_run_with_nothing_new_issues_zero_vendor_requests``. §7 requires
it explicitly, and without it a regression here is invisible until the next
timeout — which is exactly how the 903-ticker / 6h-budget state was reached.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock

from rag.pipelines._watermarks import (
    WatermarkStore,
    resolve_outstanding,
)

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def _s3(marks: dict | None = None, *, readable: bool = True):
    s3 = MagicMock()
    if not readable:
        s3.get_object.side_effect = RuntimeError("AccessDenied")
        return s3
    if marks is None:
        s3.get_object.side_effect = RuntimeError("NoSuchKey: nope")
        return s3
    s3.get_object.return_value = {
        "Body": BytesIO(json.dumps({"marks": marks}).encode())
    }
    return s3


def _store(marks=None, *, readable=True):
    return WatermarkStore("news", s3_client=_s3(marks, readable=readable))


# ── the health check §4 calls honest ─────────────────────────────────────


def test_second_run_with_nothing_new_issues_zero_vendor_requests():
    fresh = (NOW - timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    store = _store({f"{t}|news_article": fresh for t in ("AAPL", "MSFT")})

    outstanding, hours = resolve_outstanding(
        ["AAPL", "MSFT"], store, doc_type="news_article", max_hours=168, now=NOW,
    )
    # Not "a request for zero hours" — NO request at all.
    assert outstanding == []
    assert hours == 0


def test_a_ticker_at_watermark_contributes_nothing_while_a_stale_one_does():
    marks = {
        "AAPL|news_article": (NOW - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        "MSFT|news_article": (NOW - timedelta(hours=30)).isoformat().replace("+00:00", "Z"),
    }
    outstanding, hours = resolve_outstanding(
        ["AAPL", "MSFT"], _store(marks), doc_type="news_article",
        max_hours=168, now=NOW,
    )
    assert outstanding == ["MSFT"]
    # Sized to the oldest outstanding mark (~30h) + overlap, NOT the 168h
    # default — a budget derived from outstanding work, per §2.4.
    assert 30 <= hours < 168


# ── first fill and the §6 carve-out ──────────────────────────────────────


def test_never_ingested_ticker_gets_the_full_first_fill_window():
    # A newly in-scope ticker legitimately fetches its whole lookback — a
    # gap-fill, not a re-fetch (§6). It has no history to bound the window.
    outstanding, hours = resolve_outstanding(
        ["NEW"], _store({}), doc_type="news_article", max_hours=168, now=NOW,
    )
    assert outstanding == ["NEW"]
    assert hours == 168


def test_one_never_ingested_ticker_widens_the_shared_window():
    # The batch takes ONE window, so a single first-fill ticker forces the full
    # lookback for the batch. Stated as a test so the tradeoff is deliberate
    # rather than discovered.
    marks = {"OLD|news_article": (NOW - timedelta(hours=20)).isoformat().replace("+00:00", "Z")}
    outstanding, hours = resolve_outstanding(
        ["OLD", "NEW"], _store(marks), doc_type="news_article",
        max_hours=168, now=NOW,
    )
    assert set(outstanding) == {"OLD", "NEW"}
    assert hours == 168


def test_absent_store_is_first_fill_not_a_degradation():
    store = _store(None)  # NoSuchKey
    outstanding, hours = resolve_outstanding(
        ["AAPL"], store, doc_type="news_article", max_hours=168, now=NOW,
    )
    assert outstanding == ["AAPL"] and hours == 168
    assert store.unreadable is False


# ── failure directions ───────────────────────────────────────────────────


def test_unreadable_store_treats_everything_as_outstanding():
    # Over-fetching once is the safe direction. Skipping a fetch on the
    # strength of state we could not read is not.
    store = _store(None, readable=False)
    store.load()
    assert store.unreadable is True
    outstanding, hours = resolve_outstanding(
        ["AAPL", "MSFT"], store, doc_type="news_article", max_hours=168, now=NOW,
    )
    assert set(outstanding) == {"AAPL", "MSFT"}
    assert hours == 168


def test_unparseable_store_does_not_silently_read_as_empty_marks():
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": BytesIO(b"{not json")}
    store = WatermarkStore("news", s3_client=s3)
    store.load()
    assert store.unreadable is True


def test_flush_failure_does_not_raise_over_stored_documents():
    # Losing the marks costs one redundant fetch next run; raising here would
    # discard documents that are already in the corpus.
    s3 = _s3({})
    s3.put_object.side_effect = RuntimeError("throttled")
    store = WatermarkStore("news", s3_client=s3)
    store.advance("AAPL", "news_article", NOW)
    assert store.flush() is False


# ── the advance contract ─────────────────────────────────────────────────


def test_advance_then_flush_round_trips_through_the_stored_payload():
    s3 = _s3({})
    store = WatermarkStore("news", s3_client=s3)
    store.advance("aapl", "news_article", NOW)
    assert store.flush() is True

    body = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert body["schema_version"] == 1 and body["source"] == "news"
    # Ticker normalised on the way in, so a case difference upstream cannot
    # create two marks for one name.
    assert "AAPL|news_article" in body["marks"]


def test_watermarks_are_keyed_per_doc_type():
    marks = {"AAPL|news_article": NOW.isoformat().replace("+00:00", "Z")}
    store = _store(marks)
    assert store.get("AAPL", "news_article") is not None
    # A different document class for the same ticker is independently
    # outstanding — one source advancing must never mask another's gap.
    assert store.get("AAPL", "8-K") is None


def test_store_key_is_per_source_so_pipelines_cannot_contend():
    assert WatermarkStore("news", s3_client=MagicMock()).key != \
        WatermarkStore("sec_edgar", s3_client=MagicMock()).key
