"""Repair a truncated macro series in the ArcticDB ``macro`` library and its
price-cache parquet, from the canonical upstream source.

Origin: alpha-engine-config-I9256 / I9255. ``macro/VIX3M`` fell from 2509 rows
to 16 across the 2026-08-22 and 2026-08-29 Saturday backfills because
``reference/price_cache/VIX3M.parquet`` had itself been overwritten with a
1-row yfinance response. ``builders/backfill.py`` then rewrote the ArcticDB
symbol wholesale from that parquet plus the ~16-trading-day ``daily_closes``
delta, which is why the surviving window slid forward one week at a time.

This module is the AUTOMATED repair for that class — a committed, idempotent,
re-runnable CLI rather than an operator procedure. It:

  1. fetches full history for each named symbol from the canonical source
     (yfinance; ``^``-prefixed for the index tickers, matching
     ``collectors/prices.py::_CARET_SYMBOLS``),
  2. UNIONS it with whatever the price-cache parquet and the ArcticDB symbol
     already hold — existing rows are never dropped, fetched rows win on
     overlapping dates,
  3. refuses to write anything that would leave either store with fewer rows
     than it started with,
  4. reads both back and reports the row count actually observed.

Run IN-REGION (AGENTS.md: manual one-off production data-repo writes run on an
EC2 box in the bucket's region, never from the laptop). ``--dry-run`` is
read-only and safe anywhere.

Usage::

    python -m builders.repair_macro_series --symbols VIX3M
    python -m builders.repair_macro_series --symbols VIX3M --dry-run
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys

import boto3
import pandas as pd

from builders._price_cache_writeboth import (
    price_cache_read_prefixes,
    price_cache_write_prefixes,
)
from collectors.daily_closes import _FRED_INDEX_MAP
from collectors.prices import _CARET_SYMBOLS
from store.arctic_store import DEFAULT_BUCKET, get_macro_lib

log = logging.getLogger(__name__)

# Fetch window. Matches ``collectors/prices.py``'s production ``fetch_period``
# so a repaired series has exactly the span the weekly refresh would maintain.
DEFAULT_FETCH_PERIOD = "10y"

_OHLCV_COLS = ["Open", "High", "Low", "Close", "Volume"]


def _yf_symbol(symbol: str) -> str:
    return f"^{symbol}" if symbol in _CARET_SYMBOLS else symbol


def _fetch_yfinance(symbol: str, period: str = DEFAULT_FETCH_PERIOD) -> pd.DataFrame:
    """Fetch ``period`` of daily OHLCV for ``symbol`` from yfinance."""
    import yfinance as yf

    from nousergon_lib.yfinance_quiet import yf_quiet

    @yf_quiet
    def _dl() -> pd.DataFrame:
        return yf.download(
            _yf_symbol(symbol),
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            threads=False,
        )

    raw = _dl()
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    if raw is None or raw.empty or "Close" not in raw.columns:
        raise RuntimeError(
            f"Repair fetch for {symbol} ({_yf_symbol(symbol)}) returned no usable rows"
        )
    out = raw[[c for c in _OHLCV_COLS if c in raw.columns]].copy()
    out = out.dropna(subset=["Close"])
    idx = pd.to_datetime(out.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    out.index = idx.normalize()
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.index.name = "date"
    if out.empty:
        raise RuntimeError(f"Repair fetch for {symbol} produced an empty frame after cleaning")
    return out


def _fetch_fred(symbol: str, period: str = DEFAULT_FETCH_PERIOD) -> pd.DataFrame:
    """Fetch full history for an index ticker from FRED.

    The four index tickers (``VIX``/``VIX3M``/``TNX``/``IRX``) already have a
    declared FRED series in ``collectors/daily_closes.py::_FRED_INDEX_MAP`` —
    that is how ``daily_closes`` gets their DAILY bar, since they are not on
    polygon. FRED is therefore the canonical, non-yfinance source for exactly
    these symbols, which matters here: measured 2026-08-29, yfinance answers a
    ``^VIX3M`` 10y request with 2484 rows from a laptop and **1 row** from an
    EC2 host in us-east-1 — and the weekly collector runs on EC2.
    """
    from collectors.fred_history import fetch_fred_history

    series_id = _FRED_INDEX_MAP.get(symbol)
    if series_id is None:
        raise RuntimeError(f"No FRED series declared for {symbol} in _FRED_INDEX_MAP")
    years = int(period.rstrip("y")) if period.endswith("y") and period[:-1].isdigit() else 10
    fred_df = fetch_fred_history(series_id, period_years=years)
    val = fred_df["value"].astype(float)
    out = pd.DataFrame(
        {"Open": val, "High": val, "Low": val, "Close": val, "Volume": 0},
        index=pd.to_datetime(fred_df.index).normalize(),
    )
    out = out[~out.index.duplicated(keep="last")].sort_index()
    out.index.name = "date"
    if out.empty:
        raise RuntimeError(f"FRED fetch for {symbol} ({series_id}) returned no rows")
    return out


def fetch_full_history(
    symbol: str,
    period: str = DEFAULT_FETCH_PERIOD,
    source: str = "auto",
) -> pd.DataFrame:
    """Fetch full history for ``symbol``, taking the LONGEST answer available.

    ``source="auto"`` (default) queries yfinance and, for symbols with a
    declared FRED series, FRED as well, and keeps whichever returned more rows.
    That is deliberate rather than a preference order: the failure this repairs
    is a source silently answering short, so the repair must never depend on one
    source being healthy. Every attempt that fails is reported; only ALL of them
    failing raises.
    """
    attempts: list[tuple[str, pd.DataFrame]] = []
    errors: list[str] = []

    wanted = []
    if source in ("auto", "yfinance"):
        wanted.append(("yfinance", _fetch_yfinance))
    if source in ("auto", "fred") and symbol in _FRED_INDEX_MAP:
        wanted.append(("fred", _fetch_fred))
    if not wanted:
        raise RuntimeError(f"No usable source for {symbol} with source={source!r}")

    for name, fn in wanted:
        try:
            df = fn(symbol, period)
        except Exception as exc:  # noqa: BLE001 - every failure is reported below
            errors.append(f"{name}: {exc}")
            continue
        log.info("%s: %s returned %d rows (%s -> %s)",
                 symbol, name, len(df), df.index.min().date(), df.index.max().date())
        attempts.append((name, df))

    if not attempts:
        raise RuntimeError(f"All sources failed for {symbol}: {'; '.join(errors)}")

    best_name, best = max(attempts, key=lambda kv: len(kv[1]))
    log.info("%s: using %s (%d rows)", symbol, best_name, len(best))
    return best


def union_no_shrink(existing: pd.DataFrame | None, fetched: pd.DataFrame, label: str) -> pd.DataFrame:
    """Union ``existing`` and ``fetched`` (fetched wins on overlap); never shrink.

    Raises if the union somehow holds fewer rows than ``existing`` — that would
    mean a column/index contract violation, and a repair that loses rows is the
    defect it is repairing.
    """
    if existing is None or existing.empty:
        return fetched
    common = [c for c in fetched.columns if c in existing.columns]
    if not common:
        # Existing store keeps a narrower schema (the macro lib is Close-only).
        # Project the fetch onto the existing columns so the union is well-formed.
        common = [c for c in existing.columns if c in fetched.columns]
    if not common:
        raise RuntimeError(
            f"{label}: existing columns {list(existing.columns)} and fetched columns "
            f"{list(fetched.columns)} do not overlap — refusing to write"
        )
    merged = pd.concat([existing[common], fetched[common]])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    merged.index.name = existing.index.name or "date"
    if len(merged) < len(existing):
        raise RuntimeError(
            f"{label}: union produced {len(merged)} rows from an existing {len(existing)} — "
            "refusing to write a shrinking repair"
        )
    return merged


def _read_parquet(s3, bucket: str, s3_prefix: str, symbol: str) -> pd.DataFrame | None:
    for prefix in price_cache_read_prefixes(s3_prefix):
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}{symbol}.parquet")
        except s3.exceptions.NoSuchKey:
            continue
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        df.index = pd.to_datetime(df.index).normalize()
        return df
    return None


def repair_symbol(
    symbol: str,
    bucket: str = DEFAULT_BUCKET,
    s3_prefix: str = "predictor/price_cache/",
    period: str = DEFAULT_FETCH_PERIOD,
    source: str = "auto",
    dry_run: bool = False,
) -> dict:
    """Repair one macro symbol end to end. Returns a per-symbol result dict."""
    s3 = boto3.client("s3")
    macro_lib = get_macro_lib(bucket)

    fetched = fetch_full_history(symbol, period=period, source=source)

    existing_pq = _read_parquet(s3, bucket, s3_prefix, symbol)
    pq_before = 0 if existing_pq is None else len(existing_pq)
    merged_pq = union_no_shrink(existing_pq, fetched, f"price_cache.{symbol}")

    try:
        existing_arctic = macro_lib.read(symbol).data
        existing_arctic.index = pd.to_datetime(existing_arctic.index).normalize()
    except Exception:
        existing_arctic = None
    arctic_before = 0 if existing_arctic is None else len(existing_arctic)

    close_frame = pd.DataFrame({"Close": fetched["Close"]}, index=fetched.index)
    close_frame.index.name = "date"
    merged_arctic = union_no_shrink(existing_arctic, close_frame, f"macro.{symbol}")

    result = {
        "symbol": symbol,
        "fetched_rows": len(fetched),
        "parquet_rows_before": pq_before,
        "parquet_rows_planned": len(merged_pq),
        "arctic_rows_before": arctic_before,
        "arctic_rows_planned": len(merged_arctic),
        "first_date": str(merged_arctic.index.min().date()),
        "last_date": str(merged_arctic.index.max().date()),
        "dry_run": dry_run,
    }

    if dry_run:
        result["status"] = "ok_dry_run"
        return result

    buf = io.BytesIO()
    merged_pq.to_parquet(buf, engine="pyarrow", compression="snappy")
    body = buf.getvalue()
    for prefix in price_cache_write_prefixes(s3_prefix):
        s3.put_object(Bucket=bucket, Key=f"{prefix}{symbol}.parquet", Body=body)

    macro_lib.write(symbol, merged_arctic)

    readback = macro_lib.read(symbol).data
    result["arctic_rows_after"] = len(readback)
    result["arctic_first_after"] = str(pd.Timestamp(readback.index.min()).date())
    result["arctic_last_after"] = str(pd.Timestamp(readback.index.max()).date())
    if len(readback) < len(merged_arctic):
        raise RuntimeError(
            f"macro.{symbol}: readback {len(readback)} rows < written {len(merged_arctic)}"
        )
    pq_after = _read_parquet(s3, bucket, s3_prefix, symbol)
    result["parquet_rows_after"] = 0 if pq_after is None else len(pq_after)
    result["status"] = "ok"
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", required=True,
                        help="comma-separated macro symbols, e.g. VIX3M or VIX3M,HYOAS")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--s3-prefix", default="predictor/price_cache/")
    parser.add_argument("--period", default=DEFAULT_FETCH_PERIOD)
    parser.add_argument("--source", default="auto", choices=["auto", "yfinance", "fred"],
                        help="auto (default) takes the LONGEST of yfinance and FRED")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # alpha-engine-config-I10790 (P-24): audit unit D43 — a manual repair of
    # D13's parent libraries, under the same run-manifest wrapper a scheduled
    # unit uses, so a hand-run repair leaves a record (plan §4.4).
    import run_units

    # In-region only (alpha-engine-config-I9771): refuse before touching
    # ArcticDB or the price-cache parquets at all, not just before the write.
    run_units.require_in_region("repair_macro_series")

    def _body(ctx):
        results = []
        for symbol in [s.strip() for s in args.symbols.split(",") if s.strip()]:
            res = repair_symbol(
                symbol,
                bucket=args.bucket,
                s3_prefix=args.s3_prefix,
                period=args.period,
                source=args.source,
                dry_run=args.dry_run,
            )
            log.info("repair %s: %s", symbol, json.dumps(res, default=str))
            results.append(res)
            ctx.record_output(
                f"{args.s3_prefix}{symbol}.parquet",
                rows_out=int(res.get("rows_after") or res.get("rows") or 0),
                schema_version="price_cache/parquet",
            )

        ctx.rows_in = len(results)
        print(json.dumps({"results": results}, default=str, indent=2))
        return 0

    return run_units.manual_run(
        "D43", _body, write=not args.dry_run, bucket=args.bucket
    ).value


if __name__ == "__main__":
    sys.exit(main())
