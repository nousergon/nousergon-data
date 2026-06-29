"""Crypto wallet-balance producer for Metron's standalone crypto page (metron-ops#111).

Metron publishes the wallet-address fetch universe to ``metron/crypto/wallet_addresses.json``;
this collector reads it, queries each chain for the on-chain balance, prices the coins, and
writes ``crypto/holdings.json`` for Metron to read back. Metron itself makes NO chain calls —
this is the same data-spine split as the EOD/intraday market-data producers.

v1 scope: BTC + ETH via free, no-API-key public endpoints —
  * BTC: Blockstream Esplora address API (confirmed funded − spent, in sats).
  * ETH: a public JSON-RPC ``eth_getBalance`` (wei).
  * prices: CoinGecko simple price (USD).
Adding a chain = a balance fetcher here + a validator in Metron's ``crypto`` service; the S3
artifact contract is unchanged. To scale past a handful of coins, swap the per-chain fetchers
for a multi-chain aggregator behind this same ``collect`` signature.

Fail-soft PER ADDRESS: one chain/RPC error skips that address (WARN + counter — the recording
surface), never zeroes it and never aborts the whole artifact. A run with zero readable
addresses writes nothing (there is nothing to say).
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger("crypto_balances")

DEFAULT_BUCKET = "alpha-engine-research"
WALLET_ADDRESSES_KEY = "metron/crypto/wallet_addresses.json"   # Metron → producer (input)
HOLDINGS_KEY = "crypto/holdings.json"                          # producer → Metron (output)
HOLDINGS_SCHEMA_VERSION = 1

_HTTP_TIMEOUT = 15
_UA = {"User-Agent": "nousergon-crypto-balances/1.0"}
_BLOCKSTREAM = "https://blockstream.info/api/address/{address}"
_ETH_RPC = "https://cloudflare-eth.com"
_COINGECKO = "https://api.coingecko.com/api/v3/simple/price"
# chain → (CoinGecko id, display symbol)
_COIN = {"BTC": ("bitcoin", "BTC"), "ETH": ("ethereum", "ETH")}


# ── HTTP via stdlib (no third-party dep → a dependency-free Lambda zip, no platform wheels) ─


def _get_json(url: str) -> Any:
    """GET ``url`` → parsed JSON. Raises (HTTPError/URLError) on a non-2xx or network error —
    callers fail soft per address."""
    req = urllib.request.Request(url, headers=_UA)
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as r:  # noqa: S310 - fixed https hosts
        return json.loads(r.read())


def _post_json(url: str, payload: dict) -> Any:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST",
        headers={**_UA, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as r:  # noqa: S310 - fixed https host
        return json.loads(r.read())


# ── Chain balance fetchers (injectable for tests) ───────────────────────────────────────


def fetch_btc_balance(address: str) -> float:
    """Confirmed BTC balance for ``address`` (funded − spent txo sums, sats → BTC)."""
    cs = _get_json(_BLOCKSTREAM.format(address=address)).get("chain_stats", {})
    sats = int(cs.get("funded_txo_sum", 0)) - int(cs.get("spent_txo_sum", 0))
    return sats / 1e8


def fetch_eth_balance(address: str) -> float:
    """Native ETH balance for ``address`` via JSON-RPC ``eth_getBalance`` (wei → ETH)."""
    payload = {"jsonrpc": "2.0", "method": "eth_getBalance", "params": [address, "latest"], "id": 1}
    body = _post_json(_ETH_RPC, payload)
    if "error" in body:
        raise RuntimeError(f"eth_getBalance error: {body['error']}")
    return int(body["result"], 16) / 1e18


def fetch_prices(symbols: Iterable[str]) -> dict[str, float]:
    """``{symbol: usd_price}`` for the given chain symbols via CoinGecko simple price."""
    ids = sorted({_COIN[s][0] for s in symbols if s in _COIN})
    if not ids:
        return {}
    url = _COINGECKO + "?" + urllib.parse.urlencode({"ids": ",".join(ids), "vs_currencies": "usd"})
    data = _get_json(url)
    out: dict[str, float] = {}
    for sym, (cg_id, _) in _COIN.items():
        usd = data.get(cg_id, {}).get("usd")
        if usd is not None:
            out[sym] = float(usd)
    return out


_BALANCE_FETCHERS: dict[str, Callable[..., float]] = {
    "BTC": fetch_btc_balance,
    "ETH": fetch_eth_balance,
}


# ── S3 helpers ──────────────────────────────────────────────────────────────────────────


def _read_json(s3_client: Any, bucket: str, key: str) -> dict | None:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _write_json(s3_client: Any, bucket: str, key: str, obj: dict) -> None:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


# ── Orchestration ───────────────────────────────────────────────────────────────────────


def collect(
    *,
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    balance_fetchers: dict[str, Callable[..., float]] | None = None,
    price_fetcher: Callable[..., dict[str, float]] | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
) -> dict:
    """Read the wallet-address universe, fetch balances + prices, write ``crypto/holdings.json``.

    Returns a small status dict. ``s3_client`` + the fetchers are injectable for tests.
    Per-address failures are WARN-logged + counted (``n_failed``), never fatal."""
    now = now or datetime.now(UTC)
    fetchers = balance_fetchers or _BALANCE_FETCHERS
    price_fn = price_fetcher or fetch_prices
    if s3_client is None:
        import boto3
        s3_client = boto3.client("s3")

    universe = _read_json(s3_client, bucket, WALLET_ADDRESSES_KEY)
    addresses = (universe or {}).get("addresses", [])
    if not addresses:
        logger.info("[crypto_balances] no wallet addresses published — nothing to do")
        return {"status": "skipped", "reason": "no addresses"}

    chains_present = {str(a.get("chain", "")).upper() for a in addresses}
    try:
        prices = price_fn(chains_present)
    except Exception as e:  # noqa: BLE001 - prices are best-effort; balances still publish
        logger.warning("[crypto_balances] price fetch failed (values omitted this cycle): %s", e)
        prices = {}

    balances: list[dict] = []
    n_failed = 0
    for a in addresses:
        chain = str(a.get("chain", "")).upper()
        address = str(a.get("address", ""))
        fetcher = fetchers.get(chain)
        if fetcher is None or not address:
            n_failed += 1
            logger.warning("[crypto_balances] unsupported/empty entry skipped: %r", a)
            continue
        try:
            bal = fetcher(address)
        except Exception as e:  # noqa: BLE001 - per-address fail-soft (recorded), never abort
            n_failed += 1
            logger.warning("[crypto_balances] %s balance fetch failed for %s: %s", chain, address, e)
            continue
        symbol = _COIN.get(chain, (None, chain))[1]
        px = prices.get(chain)
        row = {"chain": chain, "address": address, "symbol": symbol, "balance": bal}
        if px is not None:
            row["price_usd"] = px
            row["value_usd"] = bal * px
        balances.append(row)

    if not balances:
        logger.warning("[crypto_balances] every address fetch failed (%d) — not writing", n_failed)
        return {"status": "error", "reason": "all fetches failed", "n_failed": n_failed}

    artifact = {
        "schema_version": HOLDINGS_SCHEMA_VERSION,
        "as_of_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "blockstream+eth_rpc+coingecko",
        "balances": balances,
        "prices": {s: prices[s] for s in sorted(prices)},
    }
    if dry_run:
        logger.info("[crypto_balances] dry-run: %d balances, %d failed (no write)", len(balances), n_failed)
        return {"status": "dry-run", "n_balances": len(balances), "n_failed": n_failed}

    _write_json(s3_client, bucket, HOLDINGS_KEY, artifact)
    logger.info(
        "[crypto_balances] wrote %d balances (%d failed) → s3://%s/%s",
        len(balances), n_failed, bucket, HOLDINGS_KEY,
    )
    return {"status": "ok", "n_balances": len(balances), "n_failed": n_failed}


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="python -m collectors.crypto_balances", description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    result = collect(bucket=args.bucket, dry_run=args.dry_run)
    return 0 if result.get("status") in ("ok", "dry-run", "skipped") else 1


if __name__ == "__main__":
    raise SystemExit(main())
