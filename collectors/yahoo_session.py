"""Yahoo session refresh and bounded retry for yfinance's authenticated calls.

alpha-engine-config-I11578. On 2026-09-24 at 22:54 UTC the shadow box's Yahoo
session went bad, and every crumb-authenticated yfinance call got HTTP 401.
yfinance hides that error by default: ``Ticker.info`` came back ``{}``, so D22
published 0 of 75 sectors and D24 published 43 of 75 fundamentals. Only
``funds_data`` surfaced the error (``SPY sector weights fetch failed: HTTP Error
401``). yfinance's own recovery is a single retry with its other cookie
strategy. It keeps the crumb it minted, the cookie it saved to disk, and an
in-process response cache, so every later call in the process reuses the same
rejected session.

This module is the one place that handles that for every ``Ticker.info`` /
``funds_data`` / ``get_earnings_dates`` caller in this repo:

* :func:`call_yahoo` runs the call exactly as yfinance normally would. It
  retries only when the vendor rejected the SESSION, never when a symbol simply
  has no data. On a session rejection it drops the crumb, the cookie (in memory
  and on disk) and the response cache, starts a fresh HTTP session, backs off,
  and retries. After :data:`MAX_ATTEMPTS` rejections it raises
  :class:`YahooAuthError`, so the unit fails loudly instead of publishing a
  near-empty artifact.
* An empty answer is ambiguous. A non-listed CUSIP is empty, and so is a 401
  that yfinance hid. So an empty answer is asked ONCE more with yfinance's HTTP
  errors surfaced. That call is not made for a non-empty answer, and a non-auth
  error on it leaves the original empty answer standing.
"""

from __future__ import annotations

import contextlib
import logging
import re
import threading
import time
from collections.abc import Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Total attempts per call, the first included, before :class:`YahooAuthError`.
MAX_ATTEMPTS = 3
#: Backoff before retry ``n`` (0-based) is ``BACKOFF_S * 2**n`` seconds.
BACKOFF_S = 2.0

_AUTH_MESSAGE = re.compile(r"HTTP Error 401\b|\b401 Client Error\b|Invalid Crumb|Invalid Cookie", re.I)

_sleep = time.sleep  # module seam for tests


class YahooAuthError(RuntimeError):
    """Yahoo kept rejecting the session after every refresh this call may make."""


def is_auth_failure(exc: BaseException) -> bool:
    """True when ``exc`` is Yahoo rejecting the session (HTTP 401, a bad crumb
    or cookie), not a symbol-level miss such as a 404."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status == 401:
        return True
    return bool(_AUTH_MESSAGE.search(str(exc)))


def refresh_yahoo_session() -> None:
    """Throw away every piece of yfinance's session state, so the next call mints
    a new cookie and crumb.

    That means a fresh HTTP session with an empty cookie jar, the in-memory
    crumb and cookie, the cookie persisted in yfinance's on-disk cache (which
    would otherwise be reloaded on the next request), and the in-process
    response cache (which would otherwise replay the rejected response). The
    names are yfinance internals; ``tests/test_yahoo_session_i11578.py`` pins
    them for the installed version, so a yfinance upgrade that moves them fails
    a test instead of quietly downgrading this to retry-only.
    """
    try:
        from yfinance import cache as yf_cache
        from yfinance._http import new_session
        from yfinance.data import YfData
    except ImportError as exc:  # noqa: BLE001 -- recorded: the retry still happens, un-refreshed
        logger.warning("[yahoo_session] cannot refresh the yfinance session (%s); retrying as-is", exc)
        return
    try:
        yf_cache.get_cookie_cache().store("curlCffi", None)
    except Exception as exc:  # noqa: BLE001 -- recorded: the in-memory reset below still applies
        logger.warning("[yahoo_session] could not drop yfinance's persisted cookie: %s", exc)
    data = YfData(session=new_session())
    with data._cookie_lock:
        data._cookie = None
        data._crumb = None
    YfData.cache_get.cache_clear()
    logger.warning("[yahoo_session] Yahoo session refreshed: new HTTP session, crumb and cookie dropped")


_surface_lock = threading.Lock()
_surface_depth = 0
_surface_prior: Any = None


@contextlib.contextmanager
def _http_errors_surfaced():
    """Make yfinance raise HTTP errors instead of logging them and returning
    empty. ``hide_exceptions`` is process-wide, so this is reference-counted and
    held only for the duration of one diagnostic call."""
    global _surface_depth, _surface_prior
    try:
        import yfinance
        cfg = yfinance.config.debug
    except Exception:  # noqa: BLE001 -- no yfinance means nothing to surface
        yield
        return
    with _surface_lock:
        if _surface_depth == 0:
            _surface_prior = cfg.hide_exceptions
            cfg.hide_exceptions = False
        _surface_depth += 1
    try:
        yield
    finally:
        with _surface_lock:
            _surface_depth -= 1
            if _surface_depth == 0:
                cfg.hide_exceptions = _surface_prior


def _falsy(result: Any) -> bool:
    return not result


def call_yahoo(
    fn: Callable[[], T],
    *,
    label: str,
    is_empty: Callable[[Any], bool] = _falsy,
    attempts: int = MAX_ATTEMPTS,
) -> T:
    """Run ``fn`` against Yahoo, refreshing the session and retrying when Yahoo
    rejects it. ``fn`` must build its own ``yf.Ticker`` on every call, because a
    Ticker memoises its first answer. Raises :class:`YahooAuthError` once
    ``attempts`` calls in a row are rejected, and re-raises any other error
    ``fn`` raises, unchanged."""
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            result = fn()
            if not is_empty(result):
                return result
            try:
                with _http_errors_surfaced():
                    again = fn()
            except Exception as exc:
                if is_auth_failure(exc):
                    raise
                return result  # a symbol-level miss: yfinance's own empty answer stands
            return result if is_empty(again) else again
        except Exception as exc:
            if not is_auth_failure(exc):
                raise
            last = exc
        if attempt + 1 < attempts:
            delay = BACKOFF_S * 2 ** attempt
            logger.warning(
                "[yahoo_session] %s: Yahoo rejected the session (%s); refreshing and retrying "
                "in %.0fs (attempt %d/%d)", label, last, delay, attempt + 2, attempts,
            )
            refresh_yahoo_session()
            _sleep(delay)
    raise YahooAuthError(
        f"{label}: Yahoo rejected the session on {attempts} consecutive attempts, each after a "
        f"crumb/cookie refresh; last error: {last} (alpha-engine-config-I11578)"
    )


def yahoo_info(symbol: str, *, yf_module: Any = None) -> dict:
    """``yf.Ticker(symbol).info`` through :func:`call_yahoo`. ``{}`` when the
    symbol has no data. Raises :class:`YahooAuthError` when the session cannot
    be restored."""
    if yf_module is None:
        import yfinance as yf_module
    return call_yahoo(lambda: yf_module.Ticker(symbol).info or {}, label=f"info[{symbol}]")
