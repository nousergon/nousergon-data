"""Metron market-data producer — EOD closes + FX for Metron's held-ticker universe.

`alpha-engine-data` is the single market-data ground truth for the whole Nous Ergon
system. Metron publishes its held-ticker universe to
``s3://<bucket>/metron/holdings_universe.json`` (yf_symbols + the non-USD currencies it
holds); this producer reads it and writes two artifacts the Metron app consumes — so
Metron makes NO direct market-data API calls of its own:

    market_data/eod_closes/{date}.json   + market_data/eod_closes/latest.json
    market_data/fx/{date}.json           + market_data/fx/latest.json

Closes cover the held union — including foreign listings (``1299.HK``, ``RMS.PA``), OTC
(``GTBIF``), and funds (``FNILX``) that the ~903-name SP1500 constituent cache refuses.
FX covers the held non-USD currencies (``{CCY}USD=X``).

Artifact schemas (versioned — Metron's consumer pins on ``schema_version``):

    closes: {schema_version, as_of, source, closes: {yf_symbol: {close, currency, bar_date}}}
    fx:     {schema_version, as_of, base: "USD", rates: {CCY: rate}}

Runs each weekday in ``weekly_collector._run_daily``. Best-effort per the module posture:
the universe read fail-softs to an empty pull (logged), and a fetch/​write error returns
an ``error`` status so the phase registry records it without aborting the daily run.

Entry point: ``python -m collectors.metron_market_data [--date YYYY-MM-DD] [--dry-run]``
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
# Metron publishes its held universe here (see metron api/services/data_spine.py).
HOLDINGS_UNIVERSE_KEY = "metron/holdings_universe.json"
CLOSES_PREFIX = "market_data/eod_closes/"
FX_PREFIX = "market_data/fx/"
# History artifacts (per-symbol / per-currency) — power Metron's Performance NAV
# reconstruction (close series) + as-of-date realized/dividend FX conversion. With these,
# Metron reads ALL its market data from the spine and removes yfinance entirely.
CLOSE_HISTORY_PREFIX = "market_data/close_history/"
FX_HISTORY_PREFIX = "market_data/fx_history/"
CLOSES_SCHEMA_VERSION = 1
FX_SCHEMA_VERSION = 1
CLOSE_HISTORY_SCHEMA_VERSION = 1
FX_HISTORY_SCHEMA_VERSION = 1
BASE_CURRENCY = "USD"
DEFAULT_HISTORY_PERIOD = "10y"  # mirrors the predictor price_cache 10y convention

_YFINANCE_BATCH_SIZE = 100
_YFINANCE_BATCH_DELAY = 2  # seconds between batches (rate-limit courtesy)

# A close source maps yf_symbols → {yf_symbol: (close, bar_date_iso)}. Default is
# yfinance; tests inject their own. Mirrors the price-source seam in the Metron consumer.
CloseSource = Callable[[list[str]], dict[str, tuple[float, str]]]
# An FX source maps currencies → {currency: rate} (base per 1 unit of currency).
FxSource = Callable[[list[str]], dict[str, float]]
# History sources map symbols/currencies → {key: [(date_iso, value), …]} ascending.
CloseHistorySource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]
FxHistorySource = Callable[[list[str]], dict[str, list[tuple[str, float]]]]


# ── Universe read ───────────────────────────────────────────────────────────


def load_metron_universe(bucket: str, s3_client: Any) -> tuple[list[dict], list[str]]:
    """Read Metron's published held universe → ``(holdings, currencies)``.

    ``holdings`` = ``[{"yf_symbol", "currency"}, …]``; ``currencies`` = distinct non-USD
    currencies held. Fail-soft: a missing object / no creds / parse error → ``([], [])``
    (logged) so the daily run proceeds rather than aborting."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=HOLDINGS_UNIVERSE_KEY)
        data = json.loads(obj["Body"].read())
        holdings = [
            {"yf_symbol": str(h["yf_symbol"]).strip(), "currency": str(h.get("currency", "USD")).strip()}
            for h in data.get("holdings", [])
            if str(h.get("yf_symbol", "")).strip()
        ]
        currencies = [str(c).strip().upper() for c in data.get("currencies", []) if str(c).strip()]
        logger.info("[metron_market_data] universe: %d instruments, %d non-USD currencies",
                    len(holdings), len(currencies))
        return holdings, currencies
    except Exception as e:  # missing object, no creds, parse error, etc.
        logger.warning("[metron_market_data] metron universe unavailable (%s) — empty pull", e)
        return [], []


# ── yfinance fetchers (default sources) ─────────────────────────────────────


def _yfinance_closes(yf_symbols: list[str]) -> dict[str, tuple[float, str]]:
    """Latest daily close per yf_symbol via yfinance → ``{yf_symbol: (close, bar_date)}``.
    Foreign listings (``.HK``/``.PA``/…) resolve natively. Unpriceable symbols omitted."""
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover - yfinance/pandas are prod deps
        logger.warning("[metron_market_data] yfinance/pandas unavailable")
        return {}

    out: dict[str, tuple[float, str]] = {}
    batches = [yf_symbols[i:i + _YFINANCE_BATCH_SIZE] for i in range(0, len(yf_symbols), _YFINANCE_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            raw = yf.download(
                tickers=batch[0] if len(batch) == 1 else batch,
                period="5d", interval="1d", auto_adjust=False,
                progress=False, group_by="ticker", threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for sym in batch:
                try:
                    df = (raw[sym] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    last = df.iloc[-1]
                    bar_date = df.index[-1].date().isoformat()
                    out[sym] = (round(float(last["Close"]), 4), bar_date)
                except Exception as e:
                    logger.warning("[metron_market_data] close extract failed for %s: %s", sym, e)
        except Exception as e:
            logger.warning("[metron_market_data] yfinance close batch failed: %s", e)
    logger.info("[metron_market_data] closes: %d/%d symbols priced", len(out), len(yf_symbols))
    return out


def _yfinance_fx(currencies: list[str], base: str = BASE_CURRENCY) -> dict[str, float]:
    """Latest FX rate per currency via yfinance ``{CCY}{BASE}=X`` → ``{CCY: rate}``
    (``base`` per 1 unit of ``CCY``). Unresolvable pairs omitted — no fabrication."""
    if not currencies:
        return {}
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        logger.warning("[metron_market_data] yfinance/pandas unavailable for FX")
        return {}

    pairs = {f"{c}{base}=X": c for c in currencies if c and c != base}
    if not pairs:
        return {}
    out: dict[str, float] = {}
    try:
        raw = yf.download(
            tickers=list(pairs) if len(pairs) > 1 else next(iter(pairs)),
            period="5d", interval="1d", auto_adjust=False,
            progress=False, group_by="ticker", threads=True,
        )
        is_multi = isinstance(raw.columns, pd.MultiIndex)
        for pair, ccy in pairs.items():
            try:
                df = (raw[pair] if is_multi else raw).copy()
                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue
                out[ccy] = round(float(df.iloc[-1]["Close"]), 6)
            except Exception as e:
                logger.warning("[metron_market_data] FX extract failed for %s: %s", pair, e)
    except Exception as e:
        logger.warning("[metron_market_data] yfinance FX batch failed: %s", e)
    logger.info("[metron_market_data] fx: %d/%d currencies resolved", len(out), len(pairs))
    return out


def _yf_history(symbols: list[str], period: str, *, is_fx: bool = False, base: str = BASE_CURRENCY) -> dict[str, list[tuple[str, float]]]:
    """Daily close series per symbol via yfinance over ``period`` →
    ``{key: [(bar_date, close), …]}`` ascending. ``is_fx`` maps a currency to the
    ``{CCY}{BASE}=X`` pair and keys the result by the bare currency. Empty series omitted."""
    try:
        import pandas as pd
        import yfinance as yf
    except ImportError:  # pragma: no cover
        logger.warning("[metron_market_data] yfinance/pandas unavailable for history")
        return {}

    if is_fx:
        targets = {f"{c}{base}=X": c for c in symbols if c and c != base}
    else:
        targets = {s: s for s in symbols if s}
    if not targets:
        return {}

    out: dict[str, list[tuple[str, float]]] = {}
    fetch_keys = list(targets)
    batches = [fetch_keys[i:i + _YFINANCE_BATCH_SIZE] for i in range(0, len(fetch_keys), _YFINANCE_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(_YFINANCE_BATCH_DELAY)
        try:
            raw = yf.download(
                tickers=batch[0] if len(batch) == 1 else batch,
                period=period, interval="1d", auto_adjust=False,
                progress=False, group_by="ticker", threads=True,
            )
            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for key in batch:
                try:
                    df = (raw[key] if is_multi else raw).copy()
                    df.index = pd.to_datetime(df.index)
                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        continue
                    series = [(d.date().isoformat(), round(float(c), 6)) for d, c in df["Close"].items()]
                    out[targets[key]] = series
                except Exception as e:
                    logger.warning("[metron_market_data] history extract failed for %s: %s", key, e)
        except Exception as e:
            logger.warning("[metron_market_data] yfinance history batch failed: %s", e)
    logger.info("[metron_market_data] history: %d/%d series captured", len(out), len(targets))
    return out


def _yfinance_close_history(yf_symbols: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    return _yf_history(yf_symbols, period, is_fx=False)


def _yfinance_fx_history(currencies: list[str], period: str = DEFAULT_HISTORY_PERIOD) -> dict[str, list[tuple[str, float]]]:
    return _yf_history(currencies, period, is_fx=True)


# ── S3 write (the single put-object site for this file) ──────────────────────


def _write_json(s3_client: Any, bucket: str, key: str, obj: dict) -> None:
    """Write ``obj`` as compact JSON to ``s3://bucket/key``. The ONE put_object site in
    this module — every artifact (dated + latest) routes through here, so the
    artifact-registry coverage guard pins a single count."""
    s3_client.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


# ── Orchestration ────────────────────────────────────────────────────────────


def collect(
    *,
    bucket: str = DEFAULT_BUCKET,
    run_date: str | None = None,
    dry_run: bool = False,
    s3_client: Any = None,
    close_source: CloseSource | None = None,
    fx_source: FxSource | None = None,
) -> dict:
    """Read Metron's held universe → fetch EOD closes + FX → write the two artifacts
    (dated + ``latest``). Returns a status dict. ``close_source``/``fx_source`` inject
    fetchers for tests; ``s3_client`` injects a fake S3."""
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    holdings, currencies = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}

    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    yf_symbols = sorted(ccy_by_yf)

    fetch_closes = close_source or _yfinance_closes
    fetch_fx = fx_source or _yfinance_fx
    priced = fetch_closes(yf_symbols)
    rates = fetch_fx(currencies)

    closes = {
        yf: {"close": close, "currency": ccy_by_yf.get(yf, "USD"), "bar_date": bar_date}
        for yf, (close, bar_date) in sorted(priced.items())
    }
    closes_artifact = {
        "schema_version": CLOSES_SCHEMA_VERSION, "as_of": run_date,
        "source": "alpha-engine-data", "closes": closes,
    }
    fx_artifact = {
        "schema_version": FX_SCHEMA_VERSION, "as_of": run_date,
        "base": BASE_CURRENCY, "rates": dict(sorted(rates.items())),
    }

    closes_key = f"{CLOSES_PREFIX}{run_date}.json"
    fx_key = f"{FX_PREFIX}{run_date}.json"
    if dry_run:
        logger.info("[metron_market_data] DRY-RUN: %d closes, %d fx (not written)", len(closes), len(rates))
        return {"status": "ok_dry_run", "universe": len(holdings),
                "closes": len(closes), "fx": len(rates)}

    try:
        _write_json(s3_client, bucket, closes_key, closes_artifact)
        _write_json(s3_client, bucket, f"{CLOSES_PREFIX}latest.json", closes_artifact)
        _write_json(s3_client, bucket, fx_key, fx_artifact)
        _write_json(s3_client, bucket, f"{FX_PREFIX}latest.json", fx_artifact)
    except Exception as e:  # fail loud to the phase registry — never a silent producer
        logger.error("[metron_market_data] artifact write failed: %s", e)
        return {"status": "error", "error": str(e)}

    logger.info("[metron_market_data] wrote %d closes + %d fx → s3://%s/%s{,latest}",
                len(closes), len(rates), bucket, CLOSES_PREFIX)
    return {
        "status": "ok", "universe": len(holdings),
        "closes": len(closes), "fx": len(rates),
        "closes_key": closes_key, "fx_key": fx_key,
    }


def collect_history(
    *,
    bucket: str = DEFAULT_BUCKET,
    dry_run: bool = False,
    s3_client: Any = None,
    period: str = DEFAULT_HISTORY_PERIOD,
    close_history_source: CloseHistorySource | None = None,
    fx_history_source: FxHistorySource | None = None,
) -> dict:
    """Write per-symbol close-history + per-currency FX-history artifacts for Metron's
    held universe — the series powering Metron's Performance NAV reconstruction +
    as-of-date realized/dividend FX conversion (so Metron needs no yfinance of its own).

        market_data/close_history/{yf_symbol}.json  {schema_version, yf_symbol, currency, closes: [[date, close], …]}
        market_data/fx_history/{CCY}.json           {schema_version, currency, base, rates: [[date, rate], …]}

    Idempotent (full-series overwrite each run). Injectable sources/S3 for tests."""
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    holdings, currencies = load_metron_universe(bucket, s3_client)
    if not holdings:
        return {"status": "skipped", "reason": "empty metron universe", "universe": 0}

    ccy_by_yf = {h["yf_symbol"]: h["currency"] for h in holdings}
    yf_symbols = sorted(ccy_by_yf)
    closes = (close_history_source or _yfinance_close_history)(yf_symbols)
    fx = (fx_history_source or _yfinance_fx_history)(currencies)

    if dry_run:
        logger.info("[metron_market_data] DRY-RUN history: %d close series, %d fx series (not written)",
                    len(closes), len(fx))
        return {"status": "ok_dry_run", "close_series": len(closes), "fx_series": len(fx)}

    try:
        for yf_sym, series in sorted(closes.items()):
            _write_json(s3_client, bucket, f"{CLOSE_HISTORY_PREFIX}{yf_sym}.json", {
                "schema_version": CLOSE_HISTORY_SCHEMA_VERSION, "yf_symbol": yf_sym,
                "currency": ccy_by_yf.get(yf_sym, "USD"), "closes": [list(p) for p in series],
            })
        for ccy, series in sorted(fx.items()):
            _write_json(s3_client, bucket, f"{FX_HISTORY_PREFIX}{ccy}.json", {
                "schema_version": FX_HISTORY_SCHEMA_VERSION, "currency": ccy,
                "base": BASE_CURRENCY, "rates": [list(p) for p in series],
            })
    except Exception as e:  # fail loud to the phase registry
        logger.error("[metron_market_data] history write failed: %s", e)
        return {"status": "error", "error": str(e)}

    logger.info("[metron_market_data] wrote %d close-history + %d fx-history series", len(closes), len(fx))
    return {"status": "ok", "close_series": len(closes), "fx_series": len(fx)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m collectors.metron_market_data", description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--date", default=None, help="run date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--history", action="store_true", help="also write close/FX history artifacts")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    result = collect(bucket=args.bucket, run_date=args.date, dry_run=args.dry_run)
    logger.info("[metron_market_data] latest done: %s", result)
    ok = result.get("status") in ("ok", "ok_dry_run", "skipped")
    if args.history:
        hist = collect_history(bucket=args.bucket, dry_run=args.dry_run)
        logger.info("[metron_market_data] history done: %s", hist)
        ok = ok and hist.get("status") in ("ok", "ok_dry_run", "skipped")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
