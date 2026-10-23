"""The corporate-action scan manifest (alpha-engine-config-I11076, option A).

The property under test is the completeness promise: a manifest exists for a
day only if every vendor query for that day succeeded, so the reader can treat
"no object" as "unknown" and "empty lists" as "nothing happened".
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from corporate_actions import scan_manifest as sm
from polygon_client import PolygonClient, PolygonForbiddenError

pytestmark = pytest.mark.live_ca_scan_hook

WEEKDAYS = lambda d: d.weekday() < 5  # noqa: E731 - test calendar


class FakeClient:
    def __init__(self, *, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def get_all_splits_between(self, start, end):
        self.calls.append(("splits", start, end))
        if self.fail_on == "splits":
            raise PolygonForbiddenError("403")
        return [{"ticker": "NVDA", "execution_date": end, "split_from": 1, "split_to": 10}]

    def get_all_dividends_between(self, start, end, *, date_field):
        self.calls.append((date_field, start, end))
        if self.fail_on == date_field:
            raise RuntimeError("polygon 502 after retries")
        return [{"ticker": "KO", "cash_amount": 0.51, "currency": "USD",
                 date_field: end, "record_date": "2026-10-01"}]


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = json.loads(Body)


def test_window_runs_from_the_day_after_the_prior_session():
    # Monday's window covers the weekend, so a Saturday pay date lands on Monday.
    assert sm.window_start("2026-10-26", is_trading_day=WEEKDAYS) == "2026-10-24"
    assert sm.window_start("2026-10-22", is_trading_day=WEEKDAYS) == "2026-10-22"


def test_a_complete_scan_is_written_with_every_query(monkeypatch):
    monkeypatch.setattr(sm, "window_start", lambda d: "2026-10-24")
    client, s3 = FakeClient(), FakeS3()
    out = sm.run("2026-10-26", bucket="b", client=client, s3=s3)
    assert out["status"] == "ok"
    assert out["key"] == "corporate_actions/scans/2026-10-26.json"
    body = s3.objects[("b", out["key"])]
    assert body["schema"] == sm.SCHEMA and body["complete"] is True
    assert (body["window_start"], body["window_end"]) == ("2026-10-24", "2026-10-26")
    assert len(body["splits"]) == len(body["dividends_paid"]) == len(body["dividends_ex"]) == 1
    assert {c[0] for c in client.calls} == {"splits", "pay_date", "ex_dividend_date"}
    assert len(body["content_sha256"]) == 64


@pytest.mark.parametrize("failing", ["splits", "pay_date", "ex_dividend_date"])
def test_any_failed_query_writes_nothing(monkeypatch, failing):
    monkeypatch.setattr(sm, "window_start", lambda d: "2026-10-22")
    s3 = FakeS3()
    with pytest.raises(Exception):
        sm.run("2026-10-22", bucket="b", client=FakeClient(fail_on=failing), s3=s3)
    assert s3.objects == {}


def test_per_session_failures_are_isolated_and_recorded(monkeypatch):
    monkeypatch.setattr(sm, "window_start", lambda d: d)

    class HalfBroken(FakeClient):
        def get_all_splits_between(self, start, end):
            if end == "2026-10-21":
                raise RuntimeError("boom")
            return super().get_all_splits_between(start, end)

    s3 = FakeS3()
    out = sm.run_for_sessions(["2026-10-21", "2026-10-22"], bucket="b",
                              client=HalfBroken(), s3=s3)
    assert out["2026-10-21"]["status"] == "error"
    assert out["2026-10-22"]["status"] == "ok"
    assert list(s3.objects) == [("b", "corporate_actions/scans/2026-10-22.json")]


def test_morning_scan_covers_today_and_the_enriched_session(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        sm, "run_for_sessions",
        lambda days, **k: seen.setdefault("days", sorted(days)) and
        {d: {"status": "ok"} for d in days},
    )
    now = datetime(2026, 10, 22, 12, 30, tzinfo=timezone.utc)  # 08:30 ET Thu
    out = sm.morning_scan("b", "2026-10-21", now=now, is_trading_day=WEEKDAYS)
    assert seen["days"] == ["2026-10-21", "2026-10-22"]
    assert out["status"] == "ok"
    # Saturday: no session today, only the enriched one.
    seen.clear()
    sat = datetime(2026, 10, 24, 12, 30, tzinfo=timezone.utc)
    sm.morning_scan("b", "2026-10-23", now=sat, is_trading_day=WEEKDAYS)
    assert seen["days"] == ["2026-10-23"]


def test_morning_scan_never_raises(monkeypatch):
    def broken(_):
        raise RuntimeError("calendar down")

    out = sm.morning_scan("b", "2026-10-21", is_trading_day=broken)
    assert out["status"] == "error"


def test_declared_spinoffs_in_window_are_carried():
    rows = sm._declared_in_window("2026-10-01", "2026-10-01")
    assert [r["ticker"] for r in rows] == ["CTVA"]
    assert sm._declared_in_window("2026-10-02", "2026-10-05") == []


class _Resp:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.headers = payload, status, {}

    def json(self):
        return self.payload

    def raise_for_status(self):
        pass


def test_strict_dividends_query_follows_every_page_and_keeps_dates(monkeypatch):
    client = PolygonClient.__new__(PolygonClient)
    pages = [
        {"results": [{"ticker": "KO", "cash_amount": 0.51, "currency": "USD",
                      "pay_date": "2026-10-22", "record_date": "2026-10-01",
                      "ex_dividend_date": "2026-10-01"}],
         "next_url": "https://api.polygon.io/v3/reference/dividends?cursor=abc&apiKey=SECRET"},
        {"results": [{"ticker": "PG", "cash_amount": 1.05, "currency": "USD",
                      "pay_date": "2026-10-22"},
                     {"ticker": "BAD", "cash_amount": 0, "pay_date": "2026-10-22"}]},
    ]
    calls = []

    def fake_get(path, params=None):
        calls.append((path, dict(params or {})))
        return pages[len(calls) - 1]

    monkeypatch.setattr(client, "_get", fake_get, raising=False)
    rows = client.get_all_dividends_between("2026-10-22", "2026-10-22")
    assert [r["ticker"] for r in rows] == ["KO", "PG"]
    assert rows[0]["record_date"] == "2026-10-01" and rows[0]["currency"] == "USD"
    assert calls[0][1]["pay_date.gte"] == "2026-10-22"
    assert calls[1][1] == {"cursor": "abc"}  # apiKey never re-sent from next_url


def test_strict_dividends_query_raises_on_forbidden(monkeypatch):
    client = PolygonClient.__new__(PolygonClient)

    def fake_get(path, params=None):
        raise PolygonForbiddenError("403")

    monkeypatch.setattr(client, "_get", fake_get, raising=False)
    with pytest.raises(PolygonForbiddenError):
        client.get_all_dividends_between("2026-10-22", "2026-10-22")
    with pytest.raises(ValueError):
        client.get_all_dividends_between("2026-10-22", "2026-10-22", date_field="nope")
