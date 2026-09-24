"""HTTP 429 backoff — alpha-engine-config-I11470.

On 2026-09-23 POOL's rename check waited a flat 15s before each of four
attempts, all inside one rate-limit window of a key the fleet shares, then
failed. The 429 path now backs off exponentially with jitter, never waits
less than a server-sent Retry-After, and does not sleep after the last
attempt.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import MagicMock, patch

import pytest

import polygon_client as pc
from polygon_client import PolygonClient, PolygonRateLimitError


def _resp(status: int, headers: dict | None = None, body: dict | None = None) -> MagicMock:
    r = MagicMock(status_code=status, headers=headers or {})
    r.json.return_value = body or {}
    r.raise_for_status.return_value = None
    return r


def _client() -> PolygonClient:
    return PolygonClient(api_key="test-key", calls_per_min=1000)


def test_waits_grow_exponentially_and_the_last_attempt_does_not_sleep():
    client = _client()
    sleeps: list[float] = []
    with patch.object(client._session, "get", return_value=_resp(429)), \
         patch.object(pc.time, "sleep", side_effect=sleeps.append), \
         patch.object(pc.random, "uniform", return_value=0.0):
        with pytest.raises(PolygonRateLimitError) as excinfo:
            client._get("/v3/reference/tickers/POOL/events")
    assert sleeps == [15.0, 30.0, 60.0]
    assert len(sleeps) == pc._POLYGON_MAX_ATTEMPTS - 1
    assert "POOL" in str(excinfo.value)


def test_a_server_retry_after_is_a_floor_not_replaced_by_the_schedule():
    assert pc._rate_limit_wait(0, "45") >= 45.0
    # Shorter than the schedule: the schedule wins.
    with patch.object(pc.random, "uniform", return_value=0.0):
        assert pc._rate_limit_wait(1, "2") == 30.0


def test_a_huge_retry_after_is_capped():
    with patch.object(pc.random, "uniform", return_value=0.0):
        assert pc._rate_limit_wait(0, "9999") == pc._POLYGON_429_RETRY_AFTER_MAX


def test_retry_after_accepts_an_http_date():
    when = datetime.now(timezone.utc) + timedelta(seconds=90)
    secs = pc._retry_after_seconds(format_datetime(when, usegmt=True))
    assert 80 <= secs <= 91
    assert pc._retry_after_seconds("not a date") is None


def test_jitter_is_added():
    with patch.object(pc.random, "uniform", return_value=3.0):
        assert pc._rate_limit_wait(0, None) == 18.0


def test_a_429_then_success_returns_the_body():
    client = _client()
    with patch.object(
        client._session, "get",
        side_effect=[_resp(429), _resp(200, body={"results": {"ok": 1}})],
    ), patch.object(pc.time, "sleep"):
        assert client._get("/x") == {"results": {"ok": 1}}
