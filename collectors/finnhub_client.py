"""
collectors/finnhub_client.py — shared rate-limited Finnhub HTTP client.

Extracted from ``collectors/alternative.py`` (2026-04-24) so the
fundamentals collector can share the same throttle + lock state.
Without a shared client, two collectors importing private copies of
the helper would each track their own ``_last_call`` timestamp and
exceed Finnhub's free-tier rate limit (60 req/min) when the
DataPhase1 orchestrator runs them sequentially in the same process.

Free-tier rate limit: 60 calls/min. Conservative ``_MIN_INTERVAL = 1.1s``
gives ~54 calls/min — leaves headroom for clock skew and HTTP latency.

Usage::

    from collectors.finnhub_client import finnhub_get

    payload = finnhub_get("stock/metric", {"symbol": "AAPL", "metric": "all"})
    if payload:
        metrics = payload.get("metric", {})

Returns ``[]`` (or empty dict if upstream expects ``dict``) when the API
key is missing or the call hit a 429 — callers must handle the empty
response. Other errors (5xx, timeouts) propagate via
``requests.HTTPError``; callers decide whether to retry, fall through,
or hard-fail per their no-silent-fails policy.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

from alpha_engine_lib.secrets import get_secret

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"

# Module-level state — shared across all callers in the same process.
_finnhub_lock = threading.Lock()
_finnhub_last_call = 0.0
_FINNHUB_MIN_INTERVAL = 1.1  # seconds; ~54 calls/min, under 60/min free-tier ceiling

_TIMEOUT = 10


def finnhub_get(endpoint: str, params: dict | None = None) -> dict | list:
    """Rate-limited Finnhub GET.

    Returns the parsed JSON response (dict for object-shaped endpoints,
    list for array-shaped endpoints), or ``[]`` if:
      * ``FINNHUB_API_KEY`` is not set
      * Finnhub returned a 429 rate-limit response

    Raises ``requests.HTTPError`` on other non-2xx responses (5xx,
    timeouts). Caller decides retry / fall-through / hard-fail.
    """
    global _finnhub_last_call
    api_key = get_secret("FINNHUB_API_KEY", required=False, default="")
    if not api_key:
        return []

    url = f"{_FINNHUB_BASE}/{endpoint}"
    p = {"token": api_key}
    if params:
        p.update(params)

    with _finnhub_lock:
        now = time.monotonic()
        wait = _FINNHUB_MIN_INTERVAL - (now - _finnhub_last_call)
        if wait > 0:
            time.sleep(wait)
        _finnhub_last_call = time.monotonic()

    resp = requests.get(url, params=p, timeout=_TIMEOUT)
    if resp.status_code == 429:
        logger.warning("Finnhub 429 rate-limited on %s", endpoint)
        return []
    resp.raise_for_status()
    return resp.json()
