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
    ``collectors/daily_closes.py`` for the whole tracked roster.

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

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

INDEX_CONTRIBUTIONS_PREFIX = "market_data/index_contributions/"

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


@dataclass(frozen=True)
class IndexSpec:
    """An index, its tradeable proxy, and the label a consumer displays."""

    index: str
    label: str
    proxy_symbol: str


INDEXES: tuple[IndexSpec, ...] = (
    IndexSpec(index="SPX", label="S&P 500", proxy_symbol="SPY"),
    IndexSpec(index="NDX", label="Nasdaq 100", proxy_symbol="QQQ"),
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
    """The (as_of, prior_close_date) pair, read off the proxy's own series.

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
        "as_of": as_of,
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
) -> dict:
    """Publish one contribution artifact per index for the latest session.

    Runs IN-REGION (the trading box off-market-hours, or a data-spot
    instance): it reads the ~900-ticker close archive, and a read that size
    from the laptop is dominated by S3 round-trip latency — a 20-40 minute
    in-region job measured 3+ hours locally on 2026-07-15.

    ``run_date`` bounds the window; the actual ``as_of`` is whatever the proxy
    last traded on at or before it, so a run on a holiday or a weekend
    publishes the prior session rather than failing or inventing a day.
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if weights_source is None:
        weights_source = _default_weights_source(bucket)
    if closes_source is None:
        closes_source = _default_closes_source(bucket)

    results: dict[str, dict] = {}
    errors: dict[str, str] = {}

    for spec in indexes:
        try:
            weights = weights_source(spec.index)
            if not weights.weight_map:
                raise IndexContributionsUnavailable(
                    f"{spec.index}: no weights available (method={weights.method!r}). "
                    f"An empty weight map is not a decomposition — a cache-served "
                    f"constituents run declares cache_no_weights precisely so this "
                    f"is refused rather than published as an index that moved 0%."
                )
            symbols = [*weights.weight_map, spec.proxy_symbol]
            closes = closes_source(symbols, run_date, 2)
            proxy_series = list(closes.get(spec.proxy_symbol, []))
            results[spec.index] = compute_contributions(
                spec, weights, closes, proxy_series
            )
        except IndexContributionsUnavailable as exc:
            # Per-index soft-fail is deliberate and narrow: one index's weight
            # producer being down must not withhold the other index's honest
            # decomposition. The error is RECORDED and returned, never
            # swallowed, and a caller that reports `status` sees `partial`.
            logger.error("index_contributions %s unavailable: %s", spec.index, exc)
            errors[spec.index] = str(exc)

    if not results:
        raise IndexContributionsUnavailable(
            f"no index produced a publishable decomposition for {run_date}: {errors}"
        )

    if dry_run:
        for index, payload in results.items():
            logger.info(
                "[dry-run] index_contributions %s %s: index %+.4f%%, explained %+.4f%%, "
                "residual %+.4f%%, %d constituents, coverage %.4f (%s)",
                index, payload["as_of"], payload["index_return_pct"],
                payload["explained_pp"], payload["residual_pp"],
                len(payload["constituents"]),
                payload["coverage"]["weight_with_return"], payload["weight_method"],
            )
        return {
            "status": "ok_dry_run",
            "indexes": sorted(results),
            "errors": errors,
        }

    s3 = boto3.client("s3")
    written: list[str] = []
    for index, payload in results.items():
        body = json.dumps(payload, indent=2)
        for s3_path in (
            f"{INDEX_CONTRIBUTIONS_PREFIX}{index}/{payload['as_of']}.json",
            f"{INDEX_CONTRIBUTIONS_PREFIX}{index}/latest.json",
        ):
            s3.put_object(
                Bucket=bucket, Key=s3_path, Body=body, ContentType="application/json"
            )
            written.append(s3_path)
        logger.info(
            "Wrote index_contributions for %s %s (%d constituents, residual %+.4f pp)",
            index, payload["as_of"], len(payload["constituents"]),
            payload["residual_pp"],
        )

    return {
        "status": "ok" if not errors else "partial",
        "indexes": sorted(results),
        "paths_written": written,
        "rows_out": sum(len(p["constituents"]) for p in results.values()),
        "errors": errors,
    }


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
            obj = s3.get_object(
                Bucket=bucket, Key="market_data/index_constituents/NDX.json"
            )
            payload = json.loads(obj["Body"].read())
            return Weights(
                weight_map=payload.get("weight_map") or {},
                method=payload.get("weight_method", "unavailable"),
                as_of=payload.get("as_of"),
            )
        raise IndexContributionsUnavailable(f"no weights producer for index {index!r}")

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
