#!/usr/bin/env python3
"""scripts/backfill_benchmark_proxies.py — load every DECLARED benchmark proxy
into the ArcticDB ``universe`` library, in-region.

Origin: alpha-engine-config-I10704. Measured 2026-09-14 in-region, the
``universe`` library on ``alpha-engine-research`` held ``SPY`` and none of the
five attribution proxies (``IWM``, ``XLK``, ``XLV``, ``XLF``, ``XLE``) that
``alpha-engine-config/strategy/slots/attribution.yaml`` declares. Since
``crucible-PR271`` every panel compile fetches EVERY declared proxy from that
library and refuses on a missing one, so ``data.weekly`` would have failed at
its first stage on the graded 2026-09-19 arc.

This is the AUTOMATED load for that class — a committed, idempotent,
re-runnable CLI, never an operator procedure (fleet rule: a post-merge step is
code in the repo, not text in a PR body). It is a THIN composition of two
entry points that already exist; it invents no new write path:

  1. ``builders.repair_macro_series.repair_symbol`` — fetches full history
     from the canonical upstream, UNIONS it with the existing price-cache
     parquet and ``macro`` symbol, and refuses to shrink either. This is what
     creates ``IWM``'s parquet (``IWM`` had no price cache at all before
     I10704 added it to ``collectors/prices.py::_ALWAYS_DOWNLOAD``); for the
     XL* proxies, whose parquets the weekly collector already maintains, it is
     a no-shrink top-up.
  2. ``builders.backfill.backfill(ticker_filter=...)`` — the repo's existing
     universe-write entry point, run once per proxy. It reads that parquet and
     writes the symbol to ``universe`` with the full feature schema.

RUN IN-REGION. ``nousergon-data/AGENTS.md``: manual one-off production
data-repo writes run on an EC2 box in the bucket's region, never from the
laptop — ``alpha-engine-data`` (ArcticDB) carries an explicit Deny that blocks
even ``ne-admin`` from the laptop (alpha-engine-config-I9771), and a ~900-row
write is dominated by S3 round-trip latency anyway. The supported invocation
is the ``benchmark-proxy-backfill`` workload on the existing
``alpha-engine-data-spot-dispatcher`` Lambda, which launches a fresh in-region
spot, clones ``main``, runs this module and self-terminates.

``--dry-run`` is read-only and safe anywhere the ArcticDB bucket is readable
(which is NOT the laptop).

History depth: the upstream fetch period defaults to ``10y``
(``repair_macro_series.DEFAULT_FETCH_PERIOD``), comfortably covering both the
486-calendar-day panel window and the S training window that I10704 specifies
from 2022-01-01.

Exit codes: 0 = every declared proxy is present in ``universe`` at the end of
the run (including the case where they all already were). Non-zero = at least
one proxy could not be loaded, and the failure is named on stderr. A partial
success is a FAILURE — a proxy that silently drops out is survivorship bias,
which is the whole reason the panel compile refuses on it.

Usage::

    python -m scripts.backfill_benchmark_proxies
    python -m scripts.backfill_benchmark_proxies --dry-run
    python -m scripts.backfill_benchmark_proxies --symbols IWM,XLE
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import run_units
from dates import default_run_date
from features.compute import UNIVERSE_BENCHMARK_PROXIES
from nousergon_lib import run_manifest
from store.arctic_store import DEFAULT_BUCKET, get_universe_lib

log = logging.getLogger(__name__)

#: This script IS audit unit D35 (`registry.d/units/D35-benchmark-proxy-backfill.yaml`).
UNIT_ID = "D35"


def _resolve_symbols(raw: str | None) -> list[str]:
    """Resolve the requested symbol set against the DECLARATION.

    With no ``--symbols`` this is every declared proxy. With ``--symbols`` it
    is the named subset — and a name that is NOT declared is refused rather
    than loaded, because this script's whole contract is that the declared
    list is the only list. Loading an undeclared symbol here would recreate
    the second-hand-list defect I10704 exists to remove.
    """
    if raw is None:
        return sorted(UNIVERSE_BENCHMARK_PROXIES)
    requested = [s.strip().upper() for s in raw.split(",") if s.strip()]
    undeclared = [s for s in requested if s not in UNIVERSE_BENCHMARK_PROXIES]
    if undeclared:
        raise SystemExit(
            f"Refusing undeclared symbol(s) {undeclared}: this loader only "
            f"writes members of features.compute.UNIVERSE_BENCHMARK_PROXIES "
            f"({sorted(UNIVERSE_BENCHMARK_PROXIES)}). Declare the symbol "
            f"there first — that declaration is what makes every scoping "
            f"predicate, the price collector and the freshness coverage check "
            f"maintain it."
        )
    return sorted(set(requested))


def load_proxy(
    symbol: str,
    bucket: str = DEFAULT_BUCKET,
    period: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Load one declared proxy end to end. Returns a per-symbol result dict."""
    # Imported lazily: both modules pull in the full feature-compute graph and
    # boto3 clients at import time, which the ``--help`` path has no use for.
    from builders import backfill as _backfill
    from builders import repair_macro_series as _repair

    result: dict = {"symbol": symbol, "dry_run": dry_run}

    result["price_cache"] = _repair.repair_symbol(
        symbol,
        bucket=bucket,
        period=period or _repair.DEFAULT_FETCH_PERIOD,
        dry_run=dry_run,
    )

    universe_result = _backfill.backfill(
        bucket=bucket, dry_run=dry_run, ticker_filter=symbol,
    )
    result["universe"] = universe_result
    if universe_result.get("status") == "error":
        raise RuntimeError(
            f"universe write refused for declared proxy {symbol}: "
            f"{universe_result.get('error')}"
        )
    return result


def verify(symbols: list[str], bucket: str = DEFAULT_BUCKET) -> dict:
    """Read back: every named symbol must now be a ``universe`` symbol.

    The readback is the deliverable, not the write's return value — I10704's
    sibling class (alpha-engine-config-I1906) is a step recorded as done that
    never ran. Absence here is a hard failure.
    """
    universe_lib = get_universe_lib(bucket)
    present = set(universe_lib.list_symbols())
    missing = sorted(s for s in symbols if s not in present)
    spans = {}
    for sym in symbols:
        if sym in missing:
            continue
        df = universe_lib.tail(sym, n=1).data
        head = universe_lib.head(sym, n=1).data
        spans[sym] = {
            "first_date": str(head.index.min().date()) if len(head) else None,
            "last_date": str(df.index.max().date()) if len(df) else None,
        }
    return {"present": sorted(set(symbols) - set(missing)), "missing": missing, "spans": spans}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Load the declared benchmark proxies into ArcticDB `universe` (run IN-REGION)",
    )
    parser.add_argument(
        "--symbols", default=None,
        help="comma-separated subset of the DECLARED proxies (default: all of them)",
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument(
        "--period", default=None,
        help="upstream fetch period (default: repair_macro_series.DEFAULT_FETCH_PERIOD, 10y)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # alpha-engine-config-I10790 (P-24): a hand-run repair leaves the same
    # record a scheduled unit does. The wrapper writes
    # `data_collection/runs/D35/{trading_day}/{run_id}.json` on both paths, so
    # an on-demand load is counted rather than invisible — including the
    # laptop-triggered dispatch of the in-region workload.
    #
    # `--dry-run` passes `sink=None`: the body runs exactly as it would, and
    # nothing is written, which is what the flag already promises.
    sink = None if args.dry_run else run_units.manifest_sink(args.bucket)
    try:
        return run_manifest.run_unit(
            UNIT_ID,
            lambda ctx: _execute(args, ctx),
            sink=sink,
            trigger=run_units.resolve_trigger("on_demand"),
            trading_day=default_run_date(),
            log_location=run_units.resolve_log_location(),
        ).value
    except _ProxyLoadFailed as failed:
        print("FAILED: " + " | ".join(failed.failures), file=sys.stderr)
        return 1


class _ProxyLoadFailed(RuntimeError):
    """At least one declared proxy could not be loaded.

    Raised so the run manifest reads `status: failed` with the named cause —
    a partial success IS a failure here, because a proxy that silently drops
    out is survivorship bias, which is what the panel compile refuses on.
    """

    def __init__(self, failures: list[str]):
        self.failures = failures
        super().__init__("; ".join(failures)[:2000])


def _execute(args: argparse.Namespace, ctx: run_manifest.UnitRun) -> int:
    symbols = _resolve_symbols(args.symbols)
    log.info("Declared benchmark proxies to load: %s (bucket=%s, dry_run=%s)",
             symbols, args.bucket, args.dry_run)
    ctx.rows_in = len(symbols)

    results: list[dict] = []
    failures: list[str] = []
    for symbol in symbols:
        try:
            results.append(load_proxy(
                symbol, bucket=args.bucket, period=args.period, dry_run=args.dry_run,
            ))
            log.info("Loaded declared proxy %s", symbol)
        except Exception as exc:
            # Recorded and CONTINUED so one bad upstream response does not hide
            # the state of the other four; the run still exits non-zero below.
            # The exception text is carried into the summary, never swallowed.
            log.error("Declared proxy %s FAILED: %s: %s", symbol, type(exc).__name__, exc)
            failures.append(f"{symbol}: {type(exc).__name__}: {exc}")
            results.append({"symbol": symbol, "status": "error", "error": str(exc)})

    summary: dict = {"symbols": symbols, "results": results, "failures": failures}
    if not args.dry_run:
        summary["verify"] = verify(symbols, bucket=args.bucket)
        if summary["verify"]["missing"]:
            failures.append(
                f"readback: declared proxies still absent from `universe`: "
                f"{summary['verify']['missing']}"
            )

    print(json.dumps(summary, indent=2, default=str))

    loaded = [r for r in results if r.get("status") != "error"]
    ctx.record_output(
        "arcticdb/universe (benchmark proxy series)",
        rows_out=len(loaded),
        schema_version="arcticdb/universe",
    )
    if failures:
        ctx.reject("declared_proxy_not_loaded", len(failures))

    if failures:
        raise _ProxyLoadFailed(failures)
    return 0


if __name__ == "__main__":
    sys.exit(main())
