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

Resilience (2026-06-11): each call now retries the transient class
(429 + 5xx + network errors) with bounded exponential backoff + full
jitter via ``nousergon_lib.http_retry.request_with_retry`` (the
L4499 chokepoint). Previously this was a single attempt — a one-off 429
or 5xx returned ``[]`` / raised immediately, so a Saturday rate-limit
burst silently nulled whole alt-data sources. That is what zeroed the
``analyst_consensus`` source (Finnhub supplies its rating / num_analysts
/ earnings) and breached DataPhase2's per-source populated-ratio gate
(alpha-engine-data #397 / #399: analyst_consensus dipped to 0/10 then
recovered to 7/10 within ~40 min — the signature of intermittent
throttling, not a dead key).

Auth is sent via the ``X-Finnhub-Token`` header rather than a ``token=``
query param: the http-retry api-key scrubber masks ``api_key=`` /
``apiKey=`` but NOT ``token=``, and ``request_with_retry`` embeds the
effective URL in its backoff logs / ``HttpRetryError``. Keeping the
secret out of the URL means there is nothing to leak.

Usage::

    from collectors.finnhub_client import finnhub_get

    payload = finnhub_get("stock/metric", {"symbol": "AAPL", "metric": "all"})
    if payload:
        metrics = payload.get("metric", {})

Returns ``[]`` (or empty dict if upstream expects ``dict``) when the API
key is missing or the call hit a 429 that survived all retries — callers
must handle the empty response. Other non-2xx responses (4xx, or a 5xx
that survived retries) propagate via ``requests.HTTPError``; an exhausted
network error raises ``nousergon_lib.http_retry.HttpRetryError``.
Callers decide whether to fall through or hard-fail per their
no-silent-fails policy.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

from nousergon_lib.http_retry import request_with_retry
from nousergon_lib.secrets import get_secret

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"

# Module-level state — shared across all callers in the same process.
_finnhub_lock = threading.Lock()
_finnhub_last_call = 0.0
_FINNHUB_MIN_INTERVAL = 1.1  # seconds; ~54 calls/min, under 60/min free-tier ceiling

# Bounded retry on the transient class (429 + 5xx + network). 3 attempts with
# full-jitter exponential backoff — enough to ride out an intermittent throttle
# without unbounded blocking inside the rate-limited Saturday collection.
_FINNHUB_MAX_ATTEMPTS = 3

_TIMEOUT = 10


def finnhub_get(endpoint: str, params: dict | None = None) -> dict | list:
    """Rate-limited Finnhub GET with bounded transient-error retry.

    Returns the parsed JSON response (dict for object-shaped endpoints,
    list for array-shaped endpoints), or ``[]`` if:
      * ``FINNHUB_API_KEY`` is not set
      * Finnhub returned a 429 that survived all ``_FINNHUB_MAX_ATTEMPTS``
        retries (still throttled).

    Retries 429 / 5xx / network errors with bounded backoff first. Raises
    ``requests.HTTPError`` on other non-2xx responses, or ``HttpRetryError``
    on an exhausted network error. Caller decides fall-through / hard-fail.
    """
    global _finnhub_last_call
    api_key = get_secret("FINNHUB_API_KEY", required=False, default="")
    if not api_key:
        return []

    url = f"{_FINNHUB_BASE}/{endpoint}"

    # Auth via header (NOT a token= query param) so the secret never reaches the
    # URL that request_with_retry logs / embeds in HttpRetryError. See module doc.
    session = requests.Session()
    session.headers["X-Finnhub-Token"] = api_key

    # Shared free-tier throttle: serialize callers in-process and hold the
    # min-interval since the last call. Retries inside request_with_retry add
    # their own (>= 1s) backoff, which keeps them comfortably under the ceiling.
    with _finnhub_lock:
        now = time.monotonic()
        wait = _FINNHUB_MIN_INTERVAL - (now - _finnhub_last_call)
        if wait > 0:
            time.sleep(wait)
        _finnhub_last_call = time.monotonic()

    try:
        resp = request_with_retry(
            url,
            params=params or {},
            session=session,
            timeout=_TIMEOUT,
            max_attempts=_FINNHUB_MAX_ATTEMPTS,
            label=f"finnhub:{endpoint}",
            logger=logger,
        )
    finally:
        session.close()

    if resp.status_code == 429:
        # Still throttled after every retry. Existing contract: return empty so
        # the caller treats it as no-data. WARN (not silent) so the throttle is
        # visible — the upstream per-source populated-ratio gate still hard-fails
        # if enough tickers come back empty, so this never hides a systemic gap.
        logger.warning(
            "Finnhub 429 rate-limited on %s after %d attempts",
            endpoint,
            _FINNHUB_MAX_ATTEMPTS,
        )
        return []
    resp.raise_for_status()
    return resp.json()
