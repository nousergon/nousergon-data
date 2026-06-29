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
import time
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
# Each chain has MULTIPLE free, no-key public providers tried in order — no single endpoint
# is a SPOF (Cloudflare's cloudflare-eth.com gateway, the original sole ETH RPC, started
# returning -32603 once deprecated; that one dead endpoint silently zeroed every ETH wallet).
# BTC: Esplora-compatible address API (Blockstream → mempool.space fallback, same schema).
_BTC_ESPLORA = ["https://blockstream.info/api", "https://mempool.space/api"]
# ETH: JSON-RPC eth_getBalance on reliable keyless public nodes (PublicNode + dRPC both
# verified live 2026-06-29; llamarpc kept as a last resort). NOT Ankr's public endpoint — it
# now requires an API key, so it's a dead free fallback.
_ETH_RPCS = [
    "https://ethereum-rpc.publicnode.com",
    "https://eth.drpc.org",
    "https://eth.llamarpc.com",
]
_COINGECKO = "https://api.coingecko.com/api/v3/simple/price"
# chain → (CoinGecko id, display symbol)
_COIN = {"BTC": ("bitcoin", "BTC"), "ETH": ("ethereum", "ETH")}

# BTC extended-public-key (HD wallet) support. A single BTC address is NOT the wallet balance
# — funds are spread across many addresses derived from one seed — so a self-custody wallet is
# tracked by its xpub/ypub/zpub, from which we derive + sum every address. Prefix → BIP script
# type: xpub=BIP44 legacy P2PKH, ypub=BIP49 P2SH-P2WPKH, zpub=BIP84 native-segwit P2WPKH.
_XPUB_PREFIXES = ("xpub", "ypub", "zpub")
# Gap limit: stop a branch after this many consecutive unused (tx_count==0) addresses (BIP44).
_GAP_LIMIT = 20
# Small politeness delay between per-address Esplora queries during a gap-limit scan, so a
# many-address wallet doesn't trip the public providers' rate limits.
_SCAN_DELAY_S = 0.05


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


def _is_extended_key(s: str) -> bool:
    """True if ``s`` is a BTC extended public key (HD wallet) rather than a plain address."""
    return s[:4] in _XPUB_PREFIXES


def _esplora_address_stats(address: str) -> tuple[int, int, int]:
    """``(funded_sats, spent_sats, tx_count)`` (confirmed) for a single BTC address, trying each
    Esplora provider in order. Raises only if all fail (caught by the per-address fail-soft)."""
    last_err: Exception | None = None
    for base in _BTC_ESPLORA:
        try:
            cs = _get_json(f"{base}/address/{address}").get("chain_stats", {})
            return (
                int(cs.get("funded_txo_sum", 0)),
                int(cs.get("spent_txo_sum", 0)),
                int(cs.get("tx_count", 0)),
            )
        except Exception as e:  # noqa: BLE001 - try the next provider
            last_err = e
    raise RuntimeError(f"all BTC providers failed; last: {last_err}")


def fetch_btc_balance(address: str) -> float:
    """Confirmed BTC balance, in BTC. Dispatches: an extended public key (xpub/ypub/zpub) is an
    HD wallet → derive + sum all its addresses; otherwise a single address (funded − spent)."""
    if _is_extended_key(address):
        return fetch_btc_xpub_balance(address)
    funded, spent, _ = _esplora_address_stats(address)
    return (funded - spent) / 1e8


def _xpub_address_fn(xpub: str):
    """Return ``child_hdkey -> address`` for the script type implied by the key's prefix
    (xpub→P2PKH, ypub→P2SH-P2WPKH, zpub→P2WPKH). Imports embit lazily so the module loads
    without the dep (only the xpub path needs it)."""
    from embit import script

    prefix = xpub[:4]
    if prefix == "zpub":
        return lambda c: script.p2wpkh(c).address()
    if prefix == "ypub":
        return lambda c: script.p2sh(script.p2wpkh(c)).address()
    return lambda c: script.p2pkh(c).address()  # xpub (legacy)


def fetch_btc_xpub_balance(xpub: str, *, gap_limit: int = _GAP_LIMIT) -> float:
    """Total confirmed BTC across an HD wallet, in BTC. Derives addresses on both the receive
    (0) and change (1) branches and sums their balances, extending each branch until
    ``gap_limit`` consecutive unused (tx_count==0) addresses — the standard HD-wallet scan.
    Derivation is client-side (trustless) via embit, verified against the BIP84 test vector."""
    from embit import bip32

    root = bip32.HDKey.from_string(xpub)
    to_addr = _xpub_address_fn(xpub)
    total_sats = 0
    for branch in (0, 1):  # external (receive) + internal (change)
        gap = 0
        i = 0
        while gap < gap_limit:
            address = to_addr(root.derive([branch, i]))
            funded, spent, tx_count = _esplora_address_stats(address)
            total_sats += funded - spent
            gap = 0 if tx_count else gap + 1
            i += 1
            if _SCAN_DELAY_S:
                time.sleep(_SCAN_DELAY_S)
    return total_sats / 1e8


def fetch_eth_balance(address: str) -> float:
    """Native ETH balance for ``address`` via JSON-RPC ``eth_getBalance`` (wei → ETH). Tries
    each public RPC in order; a node-level JSON-RPC error counts as a failure and falls through
    to the next. Raises only if all RPCs fail."""
    payload = {"jsonrpc": "2.0", "method": "eth_getBalance", "params": [address, "latest"], "id": 1}
    last_err: Exception | None = None
    for rpc in _ETH_RPCS:
        try:
            body = _post_json(rpc, payload)
            if "error" in body:
                raise RuntimeError(f"{rpc} eth_getBalance error: {body['error']}")
            return int(body["result"], 16) / 1e18
        except Exception as e:  # noqa: BLE001 - try the next RPC
            last_err = e
    raise RuntimeError(f"all ETH RPCs failed; last: {last_err}")


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
