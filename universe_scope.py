"""The ONE chokepoint between a collection workload's ticker roster and the
environment's declared universe (S-18 ``subset-universe``,
alpha-engine-config-I12506).

``weekly_collector.py`` forms its roster in several places (constituents
collect, the ``latest_weekly.json`` pointer, run-date constituents reads, the
Wikipedia fallback, the chronic-gap allowlist). Every one of them passes the
roster through :func:`scope` before anything is collected against it, and
``tests/test_universe_scope.py`` fails when a new roster-forming function in
that module does not.

Behaviour:

- ``NE_ENV`` unset, empty or ``prod``: :func:`scope` returns the SAME list
  object it was given. No import, no registry read, no I/O. Prod is a provable
  no-op, pinned per path by the tests.
- any other ``NE_ENV``: the environment is resolved from the fleet registry
  (``nousergon_lib.env``). ``universe.scope: full`` is a no-op as in prod.
  ``scope: subset`` reads the declared subset object (``universe_uri``; staging:
  ``crucible``/``universe/staging_subset.json``) once per process and keeps
  only roster names in it, in roster order.

Fails closed outside prod: an unresolvable environment, an unreadable or
empty subset, or an unknown scope raises :class:`UniverseScopeError`. It never
falls back to the full roster, because a staging run against the full roster
is the exact defect this module exists to remove (live vendor calls for ~900
names). Callers apply it to the CONSTITUENT roster; the macro/benchmark keys
(``_MACRO_DAILY_TICKERS``) are added after it and are never filtered.

It never feeds a delete: MorningEnrich's ArcticDB straggler prune still runs
against the unscoped refreshed roster, so a subset can never prune names.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import TypeVar

ENV_VAR = "NE_ENV"
PROD = "prod"

T = TypeVar("T", list, set, frozenset, tuple)


class UniverseScopeError(RuntimeError):
    """Outside prod, the declared universe could not be applied."""


def environment() -> str:
    """The environment this process serves (``$NE_ENV``; unset is prod)."""
    return os.environ.get(ENV_VAR, "").strip() or PROD


def _read_s3_json(uri: str):  # noqa: ANN202 — parsed JSON
    import boto3  # noqa: PLC0415

    if not uri.startswith("s3://"):
        raise UniverseScopeError(f"universe uri {uri!r} is not an s3:// uri")
    bucket, _, key = uri[len("s3://"):].partition("/")
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def _names(doc) -> frozenset[str]:  # noqa: ANN001 — parsed JSON
    """A subset document is a JSON list of tickers or an object with ``tickers``
    (the ``latest_weekly.json`` / constituents shape)."""
    names = doc.get("tickers") if isinstance(doc, dict) else doc
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        raise UniverseScopeError("universe subset must be a list of tickers or an object with a 'tickers' list")
    return frozenset(n.strip().upper() for n in names if n.strip())


@lru_cache(maxsize=None)
def declared_subset(env_name: str) -> frozenset[str] | None:
    """The declared subset of ``env_name``, or None when it collects the full
    roster. Read once per process. Never called for prod."""
    try:
        from nousergon_lib import env  # noqa: PLC0415

        e = env.resolve(env_name)
    except Exception as exc:  # noqa: BLE001 — re-raised as the one refusal type
        raise UniverseScopeError(f"{ENV_VAR}={env_name!r}: environment not resolvable: {exc}") from exc
    if e.universe_scope == "full":
        return None
    if e.universe_scope != "subset":
        raise UniverseScopeError(f"{ENV_VAR}={env_name!r}: unknown universe scope {e.universe_scope!r}")
    uri = e.universe_uri
    try:
        names = _names(_read_s3_json(uri))
    except UniverseScopeError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise UniverseScopeError(f"{ENV_VAR}={env_name!r}: universe subset {uri} unreadable: {exc}") from exc
    if not names:
        raise UniverseScopeError(f"{ENV_VAR}={env_name!r}: universe subset {uri} is empty")
    return names


def scope(tickers: T, *, path: str) -> T:
    """Apply the environment's declared universe to one roster.

    ``path`` names the call site (it appears in the refusal and the log). In
    prod, and in any environment declaring ``scope: full``, returns ``tickers``
    itself. Otherwise returns a new collection of the same type holding only
    the names in the subset, in input order.
    """
    env_name = environment()
    if env_name == PROD:
        return tickers
    try:
        subset = declared_subset(env_name)
    except UniverseScopeError as exc:
        raise UniverseScopeError(f"[{path}] {exc}") from exc
    if subset is None:
        return tickers
    kept = [t for t in tickers if str(t).upper() in subset]
    import logging  # noqa: PLC0415

    logging.getLogger(__name__).info(
        "universe scope (%s, %s): %d of %d roster names in the declared subset",
        env_name, path, len(kept), len(tickers),
    )
    return type(tickers)(kept) if not isinstance(tickers, list) else kept
