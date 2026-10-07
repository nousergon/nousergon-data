"""Read the vendor's settled values a parity adjudication needs (`alpha-engine-config-I12023`).

`data_gate.parity_adjudication` clears a frozen parity exception only when each
settling input is re-checked after settlement against an INDEPENDENT source.
For the 2026-09-28 report every exception needs a vendor read, because v1's own
later writes either do not exist (v1 stopped at the cutover) or use a different
definition (v1's later volumes are rounded to hundreds). Egress from the
laptop and from cloud sessions denies the vendors, so this runs in CI, where
the repo's `POLYGON_API_KEY` / `FRED_API_KEY` secrets already are.

Read-only: it writes nothing anywhere. It prints:

* ``polygon_grouped_daily``: every ticker's bar for each requested date, from
  the SAME call and parameters the collector uses
  (`polygon_client.get_grouped_daily`, ``adjusted=true``), at full precision;
  a requested ticker the grouped file omits (OTC symbols such as GTBIF are not
  in it) is read from the per-ticker aggregates endpoint instead
  (`polygon_client.get_single_day_bar`, also ``adjusted=true``) and kept apart
  under ``per_ticker`` so the record can cite which call served each bar;
* ``fred_vintages``: for each requested ``SERIES:OBSERVATION_DATE``, every
  ALFRED vintage of that observation (``realtime_start`` is the release date),
  plus the series' ``last_updated`` timestamp.

The payload is one JSON document, gzip + base64 between BEGIN/END markers, with
its SHA-256 printed beside it, so a reader of the job log can reconstruct it
exactly and pin it in an adjudication record.

Usage: ``python scripts/parity_adjudication_vendor_read.py --dates 2026-09-25,2026-09-28
--fred DGS10:2026-09-25,DGS2:2026-09-25,T10Y2Y:2026-09-28,T10YIE:2026-09-28``
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import gzip
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("ALPHA_ENGINE_SECRETS_SOURCE", "env")

FRED_BASE = "https://api.stlouisfed.org/fred"
CHUNK = 76


def _fred(path: str, **params) -> dict:
    import requests

    query = {**params, "api_key": os.environ["FRED_API_KEY"], "file_type": "json"}
    response = requests.get(f"{FRED_BASE}/{path}", params=query, timeout=30)
    if response.status_code != 200:
        # Never echo the URL: it carries the key. FRED's own error_message names
        # the offending variable, which is what makes a 400 diagnosable.
        try:
            detail = str(response.json().get("error_message", ""))[:300]
        except ValueError:
            detail = ""
        raise RuntimeError(f"FRED {path} returned HTTP {response.status_code}: {detail}")
    return response.json()


def read_polygon(dates: list[str], tickers: set[str] | None = None) -> dict:
    from polygon_client import PolygonClient

    def row(bar: dict) -> list:
        return [bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"], bar.get("vwap")]

    client = PolygonClient()
    out: dict[str, dict] = {}
    per_ticker: dict[str, dict] = {}
    for day in dates:
        bars = client.get_grouped_daily(day)
        out[day] = {
            ticker: row(bar)
            for ticker, bar in sorted(bars.items())
            if tickers is None or ticker in tickers
        }
        missing = sorted(tickers - out[day].keys()) if tickers else []
        print(f"polygon grouped daily {day}: {len(bars)} tickers, {len(out[day])} kept; "
              f"absent from the vendor file: {missing}", flush=True)
        if missing:
            # A NAMED ticker the grouped file omits is read from the per-ticker
            # endpoint; None records that the vendor has no bar for it either.
            found = {ticker: client.get_single_day_bar(ticker, day) for ticker in missing}
            per_ticker[day] = {ticker: (row(bar) if bar else None) for ticker, bar in found.items()}
            unread = sorted(t for t, bar in found.items() if not bar)
            print(f"polygon per-ticker {day}: {len(missing) - len(unread)} of {len(missing)} read; "
                  f"no bar from either call: {unread}", flush=True)
    return {"call": "/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted=true",
            "fields": ["open", "high", "low", "close", "volume", "vwap"], "dates": out,
            "per_ticker_call": "/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}?adjusted=true",
            "per_ticker": per_ticker}


def read_fred(specs: list[str]) -> dict:
    out = {}
    for spec in specs:
        series, observation = spec.split(":")
        try:
            # Vintages of one observation can only be published on or after it,
            # so the real-time window starts there rather than at FRED's epoch.
            vintages = _fred(
                "series/observations", series_id=series, realtime_start=observation,
                realtime_end=dt.date.today().isoformat(), observation_start=observation,
                observation_end=observation,
            ).get("observations", [])
            meta = (_fred("series", series_id=series).get("seriess") or [{}])[0]
        except RuntimeError as exc:
            # One unreadable series must not discard the whole read (the Polygon
            # half is the larger, slower one); it is recorded and fails the run.
            out[spec] = {"error": str(exc)}
            print(f"fred {spec}: ERROR {exc}", flush=True)
            continue
        out[spec] = {
            "vintages": [{k: v.get(k) for k in ("date", "value", "realtime_start", "realtime_end")} for v in vintages],
            "series_last_updated": meta.get("last_updated"),
        }
        print(f"fred {spec}: {len(vintages)} vintage(s)", flush=True)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dates", default="")
    parser.add_argument("--fred", default="")
    parser.add_argument("--tickers", default="",
                        help="Comma-separated tickers to keep from each grouped file; empty keeps all. "
                             "The full file is ~12.5k tickers, too large to recover from a job log.")
    args = parser.parse_args(argv)
    tickers = {t.strip() for t in args.tickers.split(",") if t.strip()} or None
    dates = [d for d in args.dates.split(",") if d]
    specs = [s for s in args.fred.split(",") if s]
    for day in dates:
        dt.date.fromisoformat(day)
    missing = [name for name, needed in (("POLYGON_API_KEY", dates), ("FRED_API_KEY", specs))
               if needed and not os.environ.get(name)]
    if missing:
        print(f"skip: {', '.join(missing)} not set (fork or local run)")
        return 0

    document = {
        "schema_version": "parity_adjudication_vendor_read.v1",
        "retrieved_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "polygon_grouped_daily": read_polygon(dates, tickers) if dates else None,
        "polygon_tickers_filter": sorted(tickers) if tickers else None,
        "fred_vintages": read_fred(specs) if specs else None,
    }
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = base64.b64encode(gzip.compress(raw, mtime=0)).decode("ascii")
    print(f"PAYLOAD sha256(json)={hashlib.sha256(raw).hexdigest()} bytes={len(raw)} b64_chars={len(encoded)}")
    print("BEGIN-PARITY-VENDOR-READ")
    for i in range(0, len(encoded), CHUNK):
        print(encoded[i:i + CHUNK])
    print("END-PARITY-VENDOR-READ")
    failed = [spec for spec, row in (document["fred_vintages"] or {}).items() if "error" in row]
    if failed:
        print(f"FRED read failed for {', '.join(failed)}; payload above carries the rest", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
