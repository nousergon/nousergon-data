"""Shared yfinance log-noise suppression + per-run coverage aggregation.

yfinance logs its own ERROR record per failing symbol — ≥5 distinctly-worded
messages for ONE unpriceable ticker (``quoteSummary`` 404, "possibly delisted"
in two forms, "1 Failed download:", per-period repeats). Flow Doctor keys its
dedup on message text, so each line becomes its own report/email:

  * the 2026-06-12 PCKM storm via ``collectors/metron_market_data.py``
    (config#1029), and
  * the 2026-06-19 PCAR recurrence via the un-wrapped 10y price-cache refresh
    in ``collectors/prices.py`` — three Flow Doctor issues for ONE active,
    non-delisted ticker (the LLM diagnosis even hallucinated a delisting).

The chokepoint: demote yfinance's internal logger to CRITICAL for the duration
of each fetch (:func:`quiet_yfinance` / :func:`yf_quiet`), and replace the
suppressed per-symbol spray with ONE aggregated coverage record per artifact
per run (:func:`log_yf_coverage`) — the named recording surface, so a real
provider outage still surfaces once, loudly, instead of once per symbol.

Extracted from ``collectors/metron_market_data.py`` on 2026-06-19 as an
in-repo single source of truth the moment the SAME bug class recurred through
a second collector. Lifting this into ``nousergon_lib`` is the cross-repo
chokepoint follow-up, gated on nousergon-data crossing to lib >=0.60.2 (the
alias-shim ``importlib.resources`` fix required by the ``[contracts]`` extra).
"""

from __future__ import annotations

import contextlib
import functools
import logging
from collections.abc import Callable, Collection, Iterable

__all__ = ["quiet_yfinance", "yf_quiet", "log_yf_coverage"]


@contextlib.contextmanager
def quiet_yfinance():
    """Demote yfinance's internal logger to CRITICAL for the duration of a fetch.

    yfinance emits an ERROR record per failing symbol (and several distinctly
    worded variants per symbol). Without this, each line is its own Flow Doctor
    report. Failure recording is NOT lost — callers aggregate per-run coverage
    via :func:`log_yf_coverage`, the named recording surface.
    """
    yf_logger = logging.getLogger("yfinance")
    prior_level = yf_logger.level
    yf_logger.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        yf_logger.setLevel(prior_level)


def yf_quiet(fn: Callable) -> Callable:
    """Run a yfinance fetcher under :func:`quiet_yfinance` (see rationale there)."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        with quiet_yfinance():
            return fn(*args, **kwargs)

    return wrapper


def log_yf_coverage(
    logger: logging.Logger,
    kind: str,
    requested: Iterable[str],
    covered: Collection[str] | dict,
    *,
    error_on_empty: bool = False,
    note: str = "",
) -> None:
    """One aggregated coverage record per artifact per run (config#1029).

    Replaces yfinance's per-symbol ERROR spray (demoted by
    :func:`quiet_yfinance`): symbols with no data are reported as a SINGLE WARN
    naming them all. A full miss on a non-empty request escalates to ERROR only
    where the caller marks the artifact load-bearing (``error_on_empty``), so a
    Yahoo outage surfaces once, loudly, instead of once per artifact per symbol.

    Args:
        logger: the caller's module logger (carries attribution).
        kind: short artifact label, e.g. ``"closes"`` / ``"price_cache_refresh"``.
        requested: every symbol the fetch asked for.
        covered: symbols that came back with data (set, list, or the result dict
            whose keys are the covered symbols).
        error_on_empty: escalate a total miss to ERROR (load-bearing artifact).
        note: optional caller context appended to the message.
    """
    missing = sorted(set(requested) - set(covered))
    if not missing:
        return
    suffix = f" — {note}" if note else ""
    if error_on_empty and not covered:
        logger.error(
            "%s: yfinance returned NO data for any of %d requested symbols (%s) "
            "— full-miss on a load-bearing artifact (provider outage?)%s",
            kind, len(missing), ", ".join(missing), suffix,
        )
        return
    logger.warning(
        "%s: no yfinance data for %d/%d symbols: %s%s",
        kind, len(missing), len(requested), ", ".join(missing), suffix,
    )
