"""
collectors/index_contributions.py — per-constituent contribution to an index's
daily move (alpha-engine-config-I11297).

Publishes, per index per trading day:

    market_data/index_contributions/{index}/{date}.json
    market_data/index_contributions/{index}/latest.json

Each constituent's contribution to the index's move is
``weight_at_prior_close x return``. Both terms already existed and were never
joined:

  * **weights** — ``collectors/constituents.py`` (S&P 500, from the ``Weight``
    column in the SSGA holdings file, alpha-engine-config-I11295) and
    ``market_data/index_constituents/NDX.json`` (Nasdaq-100,
    alpha-engine-config-I11296).
  * **returns** — the ArcticDB ``universe`` library, fed daily by
    ``collectors/daily_closes.py`` for the whole tracked roster. The index
    PROXY's closes come from wherever its ``IndexSpec.proxy_source`` declares:
    SPY from ``universe``; QQQ, which ``universe`` does not hold, from D21's
    ``market_data/close_history/QQQ.json`` (see ``NDX_PROXY_SOURCE``).

WHO READS THIS. Metron (``metron-ops-I346``) shows the holdings-vs-benchmark
return gap — "you +1.8%, Nasdaq-100 +2.0%" — and cannot explain it, because
nothing knows which names produced the index's 2.0%. Metron is a pure S3
consumer and makes no market-data calls of its own, so the join belongs here.
Brian, 2026-09-21: "metron should be getting its data entirely from the data
collector. all data should live in the collector itself." Cadence, same
instruction: post-close daily, not intraday.

DELIBERATELY NOT the obvious alternative. ``market_data/close_history/`` is
read per-symbol by Metron and exists to serve tickers a portfolio actually
holds; widening it to the ~600-name union of index members would inflate the
spine for every tenant to serve one aggregate. This publishes the finished
small artifact instead.

THE INDEX LEVEL IS LICENCE-ENCUMBERED. ``collectors/metron_market_data.py``
records that the index VALUES ``^GSPC`` / ``^IXIC`` / ``^NDX`` / ``^RUT``
carry a separate index licence, which is why the fleet carries tradeable ETF
proxies. ``index_return_pct`` here is the PROXY's close-to-close move, and
``proxy_symbol`` travels in the payload so no consumer can misattribute it to
the index itself.

THE TRADING-DAY AXIS COMES FROM THE PROXY'S OWN SERIES, not from a calendar.
A weight file dated to a non-trading day, or a ``prior_close_date`` that is
not the previous *trading* day, silently produces a multi-day return labelled
as one day. The proxy trades on exactly the days the index is computed, so its
two most recent observed dates at or before ``run_date`` ARE the pair — no
holiday calendar to disagree with.

THE SESSION FIELD IS ``trading_day``, NOT ``as_of``. Across the fleet ``as_of``
names a RUN timestamp and is declared ``x-provenance`` so a re-production of the
same artifact is not graded as a data mismatch
(``tests/test_declared_contracts_are_reachable_from_their_keys.py``). The session
this artifact describes is a DATA value: re-producing 2026-09-21 must yield
2026-09-21, and a shadow run that produced the wrong day has to be flagged, not
excluded from comparison. Naming it ``trading_day`` keeps it gradable and leaves
``fetched_at`` as the one provenance field.

FAIL LOUD. A member with no return is counted in
``coverage.members_missing_return`` and falls into the residual; it is never a
silent zero. Below ``COVERAGE_FLOOR`` of index weight the run RAISES rather
than publishing: a decomposition missing a tenth of the index explains
nothing and must not reach a consumer looking authoritative.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping, Sequence

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

INDEX_CONTRIBUTIONS_PREFIX = "market_data/index_contributions/"

# D49's latest NDX weights (collectors/nasdaq100.py), the NDX leg's weight source.
NDX_WEIGHTS_KEY = "market_data/index_constituents/NDX.json"

# Fraction of index weight that must have a usable return before the
# decomposition is publishable. Not 1.0: a handful of members legitimately
# lack a return on any given day (a halt, a fresh addition ArcticDB has not
# ingested yet), and the residual names them. Far below this the join is
# broken, not merely incomplete.
COVERAGE_FLOOR = 0.97


class IndexContributionsUnavailable(RuntimeError):
    """The inputs for a publishable decomposition could not be assembled.

    A distinct type, not a bare ``RuntimeError``, so a caller that genuinely
    wants to soft-fail one index and continue with the other can catch exactly
    this — mirroring ``constituents.ConstituentsUnavailable``. The fleet's
    fail-loud default stays the default and opting out is explicit.
    """


class IndexLegNotYetProduced(IndexContributionsUnavailable):
    """An index's weight producer has NEVER published, so its leg is skipped.

    alpha-engine-config-I11306. D48 runs after every EOD collection and D49
    (the NDX weights) after the weekly one, so from the day both are scheduled
    until D49's first weekly run there is no NDX.json at all. That is a
    declared, expected state, not an outage: :func:`collect` records the leg
    under ``skipped`` with this reason and the run stays ``ok``.

    Raised ONLY on evidence that no publish ever happened (no D49 ok run
    manifest and no dated NDX snapshot). Once one exists, a missing or stale
    NDX.json is :class:`IndexContributionsUnavailable` again, the run is
    ``partial``, and it pages, because that is a real D49 outage.
    """


@dataclass(frozen=True)
class IndexSpec:
    """An index, its tradeable proxy, and the label a consumer displays.

    ``max_weights_age_sessions`` refuses weights more than that many NYSE
    sessions behind ``run_date`` (``None``: not checked here).

    ``proxy_source`` names where the proxy's closes are read, DECLARED per
    index rather than discovered by falling through a chain:
    ``PROXY_SOURCE_UNIVERSE`` (the ArcticDB ``universe`` library, the same read
    as the members) or ``PROXY_SOURCE_CLOSE_HISTORY`` (the
    ``market_data/close_history/{sym}.json`` spine D21 publishes in the same EOD
    run). See ``NDX_PROXY_SOURCE`` for why the two differ.
    """

    index: str
    label: str
    proxy_symbol: str
    max_weights_age_sessions: int | None = None
    proxy_source: str = "universe"


# NDX weights are D49's weekly snapshot, keyed to the week's last session, and
# D48 runs every session after it. At the next week's last session they are 5
# sessions behind (holiday-aware: a Thursday-keyed snapshot before a closed
# Friday is still 5 at the next Friday). 6 or more means the current week's
# snapshot is missing: the weekly run, or D49 after it, did not publish.
# Measured with nousergon_lib.dates.trading_days_stale: Fri 2026-10-09 vs
# Fri 2026-10-16 = 5, vs Mon 2026-10-19 = 6; Thu 2026-04-02 vs Fri 2026-04-10 = 5.
NDX_MAX_WEIGHTS_AGE_SESSIONS = 5

PROXY_SOURCE_UNIVERSE = "universe"
PROXY_SOURCE_CLOSE_HISTORY = "close_history"

# QQQ IS NOT IN THE ArcticDB `universe` LIBRARY, nor in `macro`. `universe`
# admits exactly the stocks plus features.compute.UNIVERSE_BENCHMARK_PROXIES
# (SPY and the attribution proxies), and declaring QQQ there is not a free
# edit: daily_append's freshness scan raises UniverseFreshnessViolation on any
# declared proxy absent from the library, so the EOD append would fail until an
# in-region D35 backfill ran. Measured 2026-10-21: the D48 runs for 2026-10-19
# and 2026-10-20, the first two after D49 first published NDX.json, both filed
# `partial` with "proxy QQQ has 0 close(s) in the window" while the SPX leg
# published. QQQ's daily close IS already published every EOD run, by D21, as
# market_data/close_history/QQQ.json (collectors/metron_market_data.py
# INDEX_PROXY_SYMBOLS), so the NDX leg reads its proxy there.
NDX_PROXY_SOURCE = PROXY_SOURCE_CLOSE_HISTORY

INDEXES: tuple[IndexSpec, ...] = (
    IndexSpec(index="SPX", label="S&P 500", proxy_symbol="SPY"),
    IndexSpec(
        index="NDX",
        label="Nasdaq 100",
        proxy_symbol="QQQ",
        max_weights_age_sessions=NDX_MAX_WEIGHTS_AGE_SESSIONS,
        proxy_source=NDX_PROXY_SOURCE,
    ),
)


@dataclass(frozen=True)
class Weights:
    """Per-constituent index weight as of a stated date.

    ``weight_map`` values are FRACTIONS summing to 1.0 over the index.
    ``method`` is provenance carried from the producer and must reach the
    consumer unchanged — the Nasdaq-100 weights are ``modified_cap_approx``
    until a licensed feed lands, and an approximation that does not travel
    with its data gets absorbed as fact.
    """

    weight_map: Mapping[str, float]
    method: str
    as_of: str | None = None


# A closes source answers: for these tickers, what are the adjusted closes on
# or before `through`, most recent first? Injected so the whole computation is
# testable with no ArcticDB and no network, per this repo's convention (the
# `source=` seams elsewhere in the fleet; tests/conftest.py makes any live AWS
# call raise).
ClosesSource = Callable[[Sequence[str], str, int], Mapping[str, list[tuple[str, float]]]]
WeightsSource = Callable[[str], Weights]


def _proxy_date_pair(proxy_series: list[tuple[str, float]], proxy_symbol: str) -> tuple[str, str]:
    """The (trading_day, prior_close_date) pair, read off the proxy's own series.

    Raises rather than reaching for a calendar: if the proxy has fewer than two
    observed closes we do not know what "the previous trading day" was, and
    guessing produces a multi-day return labelled as one day.
    """
    if len(proxy_series) < 2:
        raise IndexContributionsUnavailable(
            f"proxy {proxy_symbol} has {len(proxy_series)} close(s) in the window — "
            f"two are required to establish the trading-day pair, and a calendar "
            f"guess would silently label a multi-day return as one day"
        )
    return proxy_series[0][0], proxy_series[1][0]


def compute_contributions(
    spec: IndexSpec,
    weights: Weights,
    closes: Mapping[str, list[tuple[str, float]]],
    proxy_series: list[tuple[str, float]],
) -> dict:
    """Join weights with returns into the publishable payload. Pure.

    ``closes`` maps ticker to [(date, adj_close), ...] most recent first, and
    only the two dates the proxy establishes are used — a member whose series
    skips one of them has no return for the day and is counted as missing
    rather than interpolated.
    """
    as_of, prior_date = _proxy_date_pair(proxy_series, spec.proxy_symbol)
    proxy_prior = proxy_series[1][1]
    if proxy_prior == 0:
        raise IndexContributionsUnavailable(
            f"proxy {spec.proxy_symbol}: prior close is 0, so a percent change "
            f"is undefined"
        )
    index_return_pct = (proxy_series[0][1] / proxy_prior - 1.0) * 100.0

    constituents: list[dict] = []
    weight_with_return = 0.0
    missing: list[str] = []

    for symbol, weight in sorted(weights.weight_map.items()):
        series = {d: c for d, c in closes.get(symbol, [])}
        latest = series.get(as_of)
        prior = series.get(prior_date)
        if latest is None or prior is None or prior == 0:
            # NOT a zero contribution. An absent return is unknown, and its
            # weight stays in the residual where it is visible.
            missing.append(symbol)
            continue
        return_pct = (latest / prior - 1.0) * 100.0
        constituents.append(
            {
                "symbol": symbol,
                "weight_prior_close": weight,
                "return_pct": return_pct,
                "contribution_pp": weight * return_pct,
            }
        )
        weight_with_return += weight

    coverage = {
        "weight_with_return": weight_with_return,
        "members": len(weights.weight_map),
        "members_missing_return": len(missing),
    }
    if weight_with_return < COVERAGE_FLOOR:
        raise IndexContributionsUnavailable(
            f"{spec.index} {as_of}: only {weight_with_return:.4f} of index weight has a "
            f"usable return (floor {COVERAGE_FLOOR}), {len(missing)} of "
            f"{len(weights.weight_map)} members missing. A decomposition missing this "
            f"much of the index explains nothing — refusing to publish. "
            f"Sample missing: {missing[:10]}"
        )

    explained_pp = sum(c["contribution_pp"] for c in constituents)
    return {
        "schema_version": SCHEMA_VERSION,
        "index": spec.index,
        "index_label": spec.label,
        "proxy_symbol": spec.proxy_symbol,
        "trading_day": as_of,
        "prior_close_date": prior_date,
        "index_return_pct": index_return_pct,
        "weight_method": weights.method,
        "weights_as_of": weights.as_of,
        # Named, not hidden in rounding. The sum will not tie exactly: cash and
        # futures rows carry index weight the equity roster cannot account for,
        # members missing a return sit here, and the Nasdaq-100 weights are
        # approximate. A consumer reconciling against this artifact needs the
        # gap stated, not discovered.
        "residual_pp": index_return_pct - explained_pp,
        "explained_pp": explained_pp,
        "coverage": coverage,
        "members_missing_return": missing,
        "constituents": constituents,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def collect(
    bucket: str,
    run_date: str | None = None,
    dry_run: bool = False,
    weights_source: WeightsSource | None = None,
    closes_source: ClosesSource | None = None,
    indexes: Sequence[IndexSpec] = INDEXES,
    proxy_closes_source: ClosesSource | None = None,
) -> dict:
    """Publish one contribution artifact per index for the latest session.

    Runs IN-REGION (the trading box off-market-hours, or a data-spot
    instance): it reads the ~900-ticker close archive, and a read that size
    from the laptop is dominated by S3 round-trip latency — a 20-40 minute
    in-region job measured 3+ hours locally on 2026-07-15.

    ``run_date`` bounds the window; the actual ``trading_day`` is whatever the proxy
    last traded on at or before it, so a run on a holiday or a weekend
    publishes the prior session rather than failing or inventing a day.

    ``proxy_closes_source`` answers proxies whose spec declares
    ``PROXY_SOURCE_CLOSE_HISTORY``. Left ``None`` it is the close_history spine,
    unless ``closes_source`` was injected, in which case the injected source
    answers every read (the test seam stays one function).
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if weights_source is None:
        weights_source = _default_weights_source(bucket)
    if proxy_closes_source is None:
        proxy_closes_source = (
            closes_source if closes_source is not None
            else _default_close_history_source(bucket)
        )
    if closes_source is None:
        closes_source = _default_closes_source(bucket)

    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    skipped: dict[str, str] = {}

    for spec in indexes:
        try:
            weights = weights_source(spec.index)
            _refuse_stale_weights(spec, weights, run_date)
            if not weights.weight_map:
                raise IndexContributionsUnavailable(
                    f"{spec.index}: no weights available (method={weights.method!r}). "
                    f"An empty weight map is not a decomposition — a cache-served "
                    f"constituents run declares cache_no_weights precisely so this "
                    f"is refused rather than published as an index that moved 0%."
                )
            if spec.proxy_source == PROXY_SOURCE_CLOSE_HISTORY:
                closes = closes_source(list(weights.weight_map), run_date, 2)
                proxy_series = list(
                    proxy_closes_source([spec.proxy_symbol], run_date, 2).get(
                        spec.proxy_symbol, []
                    )
                )
                _refuse_proxy_behind_members(spec, proxy_series, closes)
            else:
                symbols = [*weights.weight_map, spec.proxy_symbol]
                closes = closes_source(symbols, run_date, 2)
                proxy_series = list(closes.get(spec.proxy_symbol, []))
            results[spec.index] = compute_contributions(
                spec, weights, closes, proxy_series
            )
        except IndexLegNotYetProduced as exc:
            # Declared, expected, and not an error: the weight producer has
            # never published (see IndexLegNotYetProduced). Recorded with its
            # reason, never silently dropped, and the run stays `ok`.
            logger.warning("index_contributions %s skipped: %s", spec.index, exc)
            skipped[spec.index] = str(exc)
        except IndexContributionsUnavailable as exc:
            # Per-index soft-fail is deliberate and narrow: one index's weight
            # producer being down must not withhold the other index's honest
            # decomposition. The error is RECORDED and returned, never
            # swallowed, and a caller that reports `status` sees `partial`.
            logger.error("index_contributions %s unavailable: %s", spec.index, exc)
            errors[spec.index] = str(exc)

    if not results:
        raise IndexContributionsUnavailable(
            f"no index produced a publishable decomposition for {run_date}: "
            f"errors={errors} skipped={skipped}"
        )

    if dry_run:
        for index, payload in results.items():
            logger.info(
                "[dry-run] index_contributions %s %s: index %+.4f%%, explained %+.4f%%, "
                "residual %+.4f%%, %d constituents, coverage %.4f (%s)",
                index, payload["trading_day"], payload["index_return_pct"],
                payload["explained_pp"], payload["residual_pp"],
                len(payload["constituents"]),
                payload["coverage"]["weight_with_return"], payload["weight_method"],
            )
        return {
            "status": "ok_dry_run",
            "indexes": sorted(results),
            "errors": errors,
            "skipped": skipped,
        }

    s3 = boto3.client("s3")
    written: list[str] = []
    for index, payload in results.items():
        body = json.dumps(payload, indent=2)
        for s3_path in (
            f"{INDEX_CONTRIBUTIONS_PREFIX}{index}/{payload['trading_day']}.json",
            f"{INDEX_CONTRIBUTIONS_PREFIX}{index}/latest.json",
        ):
            s3.put_object(
                Bucket=bucket, Key=s3_path, Body=body, ContentType="application/json"
            )
            written.append(s3_path)
        logger.info(
            "Wrote index_contributions for %s %s (%d constituents, residual %+.4f pp)",
            index, payload["trading_day"], len(payload["constituents"]),
            payload["residual_pp"],
        )

    return {
        "status": "ok" if not errors else "partial",
        "indexes": sorted(results),
        "paths_written": written,
        "rows_out": sum(len(p["constituents"]) for p in results.values()),
        "errors": errors,
        "skipped": skipped,
    }


def _refuse_proxy_behind_members(
    spec: IndexSpec,
    proxy_series: list[tuple[str, float]],
    closes: Mapping[str, list[tuple[str, float]]],
) -> None:
    """Raise when a proxy read from a SECOND source ends before the members do.

    The proxy's two latest dates are the trading-day pair. Read from the same
    library as the members, both move together; read from the close_history
    spine, a D21 run that did not refresh QQQ would leave the pair one session
    behind, and every member would then "miss" the older date and trip the
    coverage floor with a message about members, not about the proxy. Named
    here instead. A proxy with fewer than two closes is left to
    :func:`_proxy_date_pair`, which already says so.
    """
    if not proxy_series:
        return
    members_head = max((s[0][0] for s in closes.values() if s), default=None)
    if members_head is not None and proxy_series[0][0] < members_head:
        raise IndexContributionsUnavailable(
            f"{spec.index}: proxy {spec.proxy_symbol} ({spec.proxy_source}) ends at "
            f"{proxy_series[0][0]} but the members' closes reach {members_head}; the "
            f"proxy source was not refreshed for the latest session"
        )


def _refuse_stale_weights(spec: IndexSpec, weights: Weights, run_date: str) -> None:
    """Raise when ``spec`` bounds its weights' age and these are past it, or
    carry no date to check. Stale weights are an outage of their producer, so
    this is :class:`IndexContributionsUnavailable` (``partial``, pages), never
    :class:`IndexLegNotYetProduced`."""
    if spec.max_weights_age_sessions is None:
        return
    if not weights.as_of:
        raise IndexContributionsUnavailable(
            f"{spec.index}: weights carry no date, so their age against "
            f"{run_date} cannot be checked"
        )
    from nousergon_lib.dates import trading_days_stale

    age = trading_days_stale(
        datetime.fromisoformat(str(weights.as_of)[:10]).date(), run_date[:10]
    )
    if age > spec.max_weights_age_sessions:
        raise IndexContributionsUnavailable(
            f"{spec.index}: weights dated {weights.as_of} are {age} sessions behind "
            f"{run_date}, over the {spec.max_weights_age_sessions}-session bound; the "
            f"current week's snapshot was not published"
        )


# Where D49's run manifests live (nousergon_lib.run_manifest's default prefix,
# registry.d/units/D49-*.yaml run_manifest_prefix) and its dated snapshots
# (collectors/nasdaq100.py).
D49_RUN_MANIFEST_PREFIX = "data_collection/runs/D49/"
WEEKLY_PREFIX = "market_data/weekly/"


def ndx_weights_ever_published(bucket: str, s3) -> bool:
    """Whether D49 has EVER published NDX weights: any ``ok`` D49 run manifest,
    or any dated ``market_data/weekly/{date}/NDX.json`` snapshot.

    Evidence, not a date: the answer flips the first time D49 publishes and
    never flips back, so once it is True a missing NDX.json is an outage.
    An S3 error here propagates. It never reads as "never published", because
    that answer is the one that suppresses a page.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=D49_RUN_MANIFEST_PREFIX):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            if json.loads(body).get("status") == "ok":
                return True
    for page in paginator.paginate(Bucket=bucket, Prefix=WEEKLY_PREFIX, Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            key = f"{prefix['Prefix']}NDX.json"
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return True
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404", "NotFound"):
                    raise
    return False


def _default_weights_source(bucket: str) -> WeightsSource:
    """Read weights from whichever producer owns each index."""

    def source(index: str) -> Weights:
        if index == "SPX":
            from collectors import constituents

            payload = constituents.load_from_s3(bucket) or {}
            index_of = payload.get("index_of") or {}
            weight_map = {
                t: w
                for t, w in (payload.get("weight_map") or {}).items()
                if index_of.get(t) == "S&P 500"
            }
            return Weights(
                weight_map=weight_map,
                method=payload.get("weight_method", "unavailable"),
                as_of=payload.get("date"),
            )
        if index == "NDX":
            s3 = boto3.client("s3")
            try:
                obj = s3.get_object(Bucket=bucket, Key=NDX_WEIGHTS_KEY)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
                    raise
                # No NDX.json. Any other S3 failure is NOT this case and
                # propagates (the re-raise above). Which of two states this is
                # is decided by evidence, not a date:
                #   * D49 has NEVER published: a declared skip of the NDX leg.
                #     SPX still publishes and the run stays `ok`.
                #   * D49 published before and the file is gone: an outage.
                #     The run is `partial` and pages.
                if not ndx_weights_ever_published(bucket, s3):
                    raise IndexLegNotYetProduced(
                        f"NDX: D49 (collectors/nasdaq100.py) has never published "
                        f"weights (no s3://{bucket}/{NDX_WEIGHTS_KEY}, no ok "
                        f"{D49_RUN_MANIFEST_PREFIX} manifest, no dated "
                        f"{WEEKLY_PREFIX}*/NDX.json); NDX leg skipped until its "
                        f"first weekly run (alpha-engine-config-I11306)"
                    ) from exc
                raise IndexContributionsUnavailable(
                    f"NDX: no weights at s3://{bucket}/{NDX_WEIGHTS_KEY}, but D49 "
                    f"has published before; the file is missing"
                ) from exc
            payload = json.loads(obj["Body"].read())
            # `date`, not `as_of`: contracts/ndx_constituents.schema.json is
            # closed and names the snapshot date `date`, the same field the
            # SPX leg above reads. Pinned by
            # tests/test_ndx_constituents_consumer_contract.py.
            return Weights(
                weight_map=payload.get("weight_map") or {},
                method=payload.get("weight_method", "unavailable"),
                as_of=payload.get("date"),
            )
        raise IndexContributionsUnavailable(f"no weights producer for index {index!r}")

    return source


def _default_close_history_source(bucket: str) -> ClosesSource:
    """Dividend-adjusted closes from D21's ``market_data/close_history/{sym}.json``.

    Same shape as :func:`_default_closes_source`, most recent first. The basis
    is CHECKED, not assumed: the members' ``universe`` closes are
    dividend-adjusted, and a proxy on a different basis would put an ex-dividend
    day's payout into ``index_return_pct`` and the residual. A missing key is
    an empty series, which :func:`_proxy_date_pair` refuses by name; any other
    S3 failure propagates.
    """

    def source(
        symbols: Sequence[str], through: str, limit: int
    ) -> Mapping[str, list[tuple[str, float]]]:
        from collectors.metron_market_data import CLOSE_HISTORY_PREFIX

        s3 = boto3.client("s3")
        out: dict[str, list[tuple[str, float]]] = {}
        for symbol in dict.fromkeys(symbols):
            key = f"{CLOSE_HISTORY_PREFIX}{symbol}.json"
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in ("NoSuchKey", "404"):
                    raise
                continue
            payload = json.loads(obj["Body"].read())
            basis = payload.get("adjustment_basis")
            if basis != "dividend_adjusted":
                raise IndexContributionsUnavailable(
                    f"s3://{bucket}/{key}: adjustment_basis={basis!r}, but the members' "
                    f"closes are dividend-adjusted; refusing to mix bases"
                )
            rows = [
                (str(d)[:10], float(c))
                for d, c in (payload.get("closes") or [])
                if c is not None and str(d)[:10] <= through
            ]
            rows.sort(key=lambda r: r[0])
            out[symbol] = list(reversed(rows[-limit:]))
        return out

    return source


def _default_closes_source(bucket: str) -> ClosesSource:
    """Adjusted closes from the ArcticDB universe library, most recent first.

    ``read_batch`` rather than a read per ticker: ~600 individual reads is the
    shape that makes this job unrunnable outside the bucket's region.
    """

    def source(
        symbols: Sequence[str], through: str, limit: int
    ) -> Mapping[str, list[tuple[str, float]]]:
        from store.arctic_store import get_universe_lib

        lib = get_universe_lib(bucket)
        out: dict[str, list[tuple[str, float]]] = {}
        unique = list(dict.fromkeys(symbols))
        for item in lib.read_batch(unique):
            symbol = getattr(item, "symbol", None)
            data = getattr(item, "data", None)
            if symbol is None or data is None or data.empty:
                # A symbol ArcticDB does not hold is a missing RETURN, handled
                # by the coverage floor — not an error here, and not a zero.
                continue
            column = "Adj_Close" if "Adj_Close" in data.columns else "Close"
            series = data[column].dropna()
            series = series[series.index <= through]
            tail = series.tail(limit)
            out[symbol] = [
                (str(idx)[:10], float(val)) for idx, val in reversed(list(tail.items()))
            ]
        return out

    return source
