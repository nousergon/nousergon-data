"""collectors — the fleet's per-source data-fetch entry points.

``CaretTickerError`` lives here (rather than in ``builders/_price_cache_writeboth.py``,
where ``assert_valid_price_cache_ticker`` raises a bare ``ValueError``) because every
write site that must re-raise it — ``collectors/prices.py``, ``collectors/fred_history.py``,
and ``weekly_collector.py``'s chronic-gap self-heal — already imports from ``collectors``
or sits alongside it, and this repo's fail-loud default (AGENTS.md) forbids a producer
write site from folding a population-contract violation into a per-ticker miss
(alpha-engine-config-I10904). A dedicated type — rather than a bare
``except ValueError: raise`` — means only the caret guard's own failure escalates;
an unrelated ``ValueError`` raised elsewhere in the same try block still becomes a
per-ticker failure, exactly as before.
"""

from __future__ import annotations


class CaretTickerError(ValueError):
    """A caret-prefixed ticker (``^VIX3M``) reached a price-cache WRITE site.

    ``builders._price_cache_writeboth.assert_valid_price_cache_ticker`` raises a bare
    ``ValueError`` for this — the write-time invariant of last resort (I9288). Its
    docstring says a writer reaching that guard with a caret ticker "has a bug upstream
    of the population filter" and "fails loud on its own writers" — but every write site
    caught that ``ValueError`` in a broad ``except Exception`` and folded it into an
    ordinary per-ticker miss (I10904), which is exactly the silent corruption the guard's
    own docstring says must not happen. Each write site wraps the guard call and
    re-raises this type so an ``except CaretTickerError: raise`` ahead of its broad
    handler — mirroring the existing ``except FutureBarError: raise`` (I10893) — lets the
    contract violation propagate out of the run instead.
    """
