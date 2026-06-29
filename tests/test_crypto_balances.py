"""Crypto wallet-balance producer (metron-ops#111).

Reads Metron's published wallet-address universe, fetches per-chain balances + prices, and
writes ``crypto/holdings.json``. Covers the happy path, per-address fail-soft, the
no-addresses / all-failed skips, the dry-run no-write path, and price-fetch degradation.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from collectors import crypto_balances as cb

_BTC = "bc1q9zpgru5j9q3dccf6n5xm9wglv5jh0w8r4d5xkp"
_ETH = "0x52908400098527886e0f7030069857d2e4169ee7"
# Canonical BIP84 test vector (mnemonic "abandon abandon … about"): zpub + its m/…/0/0 address.
_ZPUB = "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"
_ZPUB_FIRST_RECEIVE = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
# A bare xpub (BIP44 account key of the same test mnemonic) — its script type is ambiguous, so
# the scanner derives all three types (the real-world Ledger-exports-segwit-as-xpub case).
_XPUB = "xpub6BosfCnifzxcFwrSzQiqu2DBVTshkCXacvNsWGYJVVhhawA7d4R5WSWGFNbi8Aw6ZRc1brxMyWMzG3DSSSSoekkudhUd9yLb6qx39T9nMdj"
_NOW = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)


def _s3(universe: dict | None) -> MagicMock:
    s3 = MagicMock()

    def _get(Bucket, Key):
        if Key == cb.WALLET_ADDRESSES_KEY and universe is not None:
            body = MagicMock()
            body.read.return_value = json.dumps(universe).encode()
            return {"Body": body}
        raise Exception("NoSuchKey")

    s3.get_object.side_effect = _get
    return s3


def _puts(s3: MagicMock) -> dict[str, dict]:
    return {c.kwargs["Key"]: json.loads(c.kwargs["Body"].decode()) for c in s3.put_object.call_args_list}


def _universe(*pairs: tuple[str, str]) -> dict:
    return {"schema_version": 1, "addresses": [{"chain": c, "address": a} for c, a in pairs]}


def _fetchers(balances: dict[str, float]):
    """Balance fetchers keyed by chain, returning a canned balance (or raising for an
    address mapped to an Exception)."""
    def _make(chain):
        def _fetch(address):
            val = balances[chain]
            if isinstance(val, Exception):
                raise val
            return val
        return _fetch
    return {c: _make(c) for c in balances}


def test_happy_path_writes_balances_and_values():
    s3 = _s3(_universe(("BTC", _BTC), ("ETH", _ETH)))
    r = cb.collect(
        s3_client=s3,
        balance_fetchers=_fetchers({"BTC": 0.5, "ETH": 2.0}),
        price_fetcher=lambda syms: {"BTC": 60000.0, "ETH": 3000.0},
        eth_token_fetcher=lambda a: [],
        now=_NOW,
    )
    assert r["status"] == "ok" and r["n_balances"] == 2 and r["n_failed"] == 0
    art = _puts(s3)[cb.HOLDINGS_KEY]
    assert art["schema_version"] == 1 and art["as_of_utc"] == "2026-06-29T12:00:00Z"
    by_chain = {b["chain"]: b for b in art["balances"]}
    assert by_chain["BTC"]["balance"] == 0.5 and by_chain["BTC"]["value_usd"] == 30000.0
    assert by_chain["ETH"]["value_usd"] == 6000.0
    assert art["prices"] == {"BTC": 60000.0, "ETH": 3000.0}


def test_per_address_failure_is_soft():
    s3 = _s3(_universe(("BTC", _BTC), ("ETH", _ETH)))
    r = cb.collect(
        s3_client=s3,
        balance_fetchers=_fetchers({"BTC": 0.5, "ETH": RuntimeError("rpc down")}),
        price_fetcher=lambda syms: {"BTC": 60000.0},
        eth_token_fetcher=lambda a: [],
        now=_NOW,
    )
    assert r["status"] == "ok" and r["n_balances"] == 1 and r["n_failed"] == 1
    art = _puts(s3)[cb.HOLDINGS_KEY]
    assert [b["chain"] for b in art["balances"]] == ["BTC"]


def test_price_failure_still_writes_balances_without_value():
    s3 = _s3(_universe(("BTC", _BTC)))

    def _boom(syms):
        raise RuntimeError("coingecko 429")

    r = cb.collect(
        s3_client=s3,
        balance_fetchers=_fetchers({"BTC": 1.0}), price_fetcher=_boom, now=_NOW,
    )
    assert r["status"] == "ok"
    row = _puts(s3)[cb.HOLDINGS_KEY]["balances"][0]
    assert row["balance"] == 1.0 and "value_usd" not in row and "price_usd" not in row


def test_no_addresses_skips_without_write():
    s3 = _s3(_universe())
    r = cb.collect(s3_client=s3, now=_NOW)
    assert r["status"] == "skipped" and s3.put_object.call_count == 0


def test_missing_universe_artifact_skips():
    s3 = _s3(None)
    r = cb.collect(s3_client=s3, now=_NOW)
    assert r["status"] == "skipped" and s3.put_object.call_count == 0


def test_all_failed_does_not_write():
    s3 = _s3(_universe(("ETH", _ETH)))
    r = cb.collect(
        s3_client=s3,
        balance_fetchers=_fetchers({"ETH": RuntimeError("down")}),
        price_fetcher=lambda syms: {}, eth_token_fetcher=lambda a: [], now=_NOW,
    )
    assert r["status"] == "error" and s3.put_object.call_count == 0


def test_dry_run_does_not_write():
    s3 = _s3(_universe(("BTC", _BTC)))
    r = cb.collect(
        s3_client=s3,
        balance_fetchers=_fetchers({"BTC": 1.0}),
        price_fetcher=lambda syms: {"BTC": 50000.0},
        now=_NOW, dry_run=True,
    )
    assert r["status"] == "dry-run" and s3.put_object.call_count == 0


class TestProviderFallback:
    """Each chain has multiple public providers tried in order — no single endpoint is a SPOF
    (the original sole ETH RPC, cloudflare-eth.com, went dead and silently zeroed ETH)."""

    def test_eth_falls_through_to_next_rpc(self, monkeypatch):
        calls = []

        def fake_post(url, payload):
            calls.append(url)
            if url == cb._ETH_RPCS[0]:
                raise RuntimeError("rpc 0 unreachable")
            return {"result": hex(2 * 10**18)}  # 2 ETH

        monkeypatch.setattr(cb, "_post_json", fake_post)
        assert cb.fetch_eth_balance(_ETH) == 2.0
        assert calls[:2] == cb._ETH_RPCS[:2]

    def test_eth_jsonrpc_error_falls_through(self, monkeypatch):
        def fake_post(url, payload):
            if url == cb._ETH_RPCS[0]:
                return {"error": {"code": -32603, "message": "Internal error"}}  # the cloudflare failure
            return {"result": hex(10**18)}  # 1 ETH

        monkeypatch.setattr(cb, "_post_json", fake_post)
        assert cb.fetch_eth_balance(_ETH) == 1.0

    def test_eth_all_rpcs_fail_raises(self, monkeypatch):
        def _boom(url, payload):
            raise RuntimeError("down")

        monkeypatch.setattr(cb, "_post_json", _boom)
        with pytest.raises(RuntimeError):
            cb.fetch_eth_balance(_ETH)

    def test_btc_falls_through_to_mempool(self, monkeypatch):
        def fake_get(url):
            if url.startswith(cb._BTC_ESPLORA[0]):
                raise RuntimeError("blockstream down")
            return {"chain_stats": {"funded_txo_sum": 150_000_000, "spent_txo_sum": 50_000_000, "tx_count": 1}}  # 1 BTC

        monkeypatch.setattr(cb, "_get_json", fake_get)
        assert cb.fetch_btc_balance(_BTC) == 1.0


class TestXpub:
    """HD-wallet (xpub/ypub/zpub) support — a single BTC address isn't the wallet balance, so
    self-custody wallets are tracked by an extended key whose addresses we derive + sum."""

    def test_is_extended_key(self):
        assert cb._is_extended_key(_ZPUB)
        assert not cb._is_extended_key(_BTC)

    def test_derivation_matches_bip84_vector(self, monkeypatch):
        # Real embit derivation: the first receive address must be the canonical BIP84 vector.
        seen: list[str] = []

        def fake_stats(addr):
            seen.append(addr)
            return (0, 0, 0)  # all empty → the scan terminates at the gap limit

        monkeypatch.setattr(cb, "_esplora_address_stats", fake_stats)
        monkeypatch.setattr(cb, "_SCAN_DELAY_S", 0)
        assert cb.fetch_btc_xpub_balance(_ZPUB, gap_limit=3) == 0.0
        assert seen[0] == _ZPUB_FIRST_RECEIVE

    def test_gap_limit_sums_then_stops(self, monkeypatch):
        calls = {"n": 0}

        def fake_stats(addr):
            calls["n"] += 1
            if calls["n"] == 1:  # first receive address holds a net 1 BTC
                return (150_000_000, 50_000_000, 1)
            return (0, 0, 0)

        monkeypatch.setattr(cb, "_esplora_address_stats", fake_stats)
        monkeypatch.setattr(cb, "_SCAN_DELAY_S", 0)
        assert cb.fetch_btc_xpub_balance(_ZPUB, gap_limit=3) == 1.0
        # receive: 0 used + 3 empty (gap hits 3) = 4 calls; change: 3 empty = 3 calls → 7 total.
        assert calls["n"] == 7

    def test_fetch_btc_dispatches_xpub(self, monkeypatch):
        monkeypatch.setattr(cb, "fetch_btc_xpub_balance", lambda x, **k: 2.5)
        assert cb.fetch_btc_balance(_ZPUB) == 2.5

    def test_fetch_btc_single_address_path(self, monkeypatch):
        monkeypatch.setattr(cb, "_esplora_address_stats", lambda a: (150_000_000, 50_000_000, 1))
        assert cb.fetch_btc_balance(_BTC) == 1.0

    def test_zpub_one_script_type_xpub_all_three(self):
        assert len(cb._xpub_address_fns(_ZPUB)) == 1   # unambiguous version → P2WPKH only
        assert len(cb._xpub_address_fns(_XPUB)) == 3   # ambiguous → scan P2PKH/P2SH-WPKH/P2WPKH

    def test_xpub_scans_every_script_type(self, monkeypatch):
        # All addresses empty → each script type's two branches hit the gap limit. With 3 types
        # the scan does 3× the work of a single-type (zpub) key — proves all types are scanned.
        monkeypatch.setattr(cb, "_SCAN_DELAY_S", 0)
        n = {"x": 0, "z": 0}

        monkeypatch.setattr(cb, "_esplora_address_stats", lambda a: (n.__setitem__("x", n["x"] + 1), (0, 0, 0))[1])
        assert cb.fetch_btc_xpub_balance(_XPUB, gap_limit=2) == 0.0
        monkeypatch.setattr(cb, "_esplora_address_stats", lambda a: (n.__setitem__("z", n["z"] + 1), (0, 0, 0))[1])
        assert cb.fetch_btc_xpub_balance(_ZPUB, gap_limit=2) == 0.0
        assert n["x"] == 3 * n["z"]  # 3 script types vs 1


class TestEthTokens:
    """ERC-20 token holdings for an ETH address (Blockscout v2). Only tokens with a USD
    exchange_rate + value ≥ $1 are kept — spam/airdrop tokens (no rate) self-filter."""

    def _blockscout(self):
        return [
            {"value": str(752_800_000_000_000_000), "token": {
                "type": "ERC-20", "symbol": "STETH", "decimals": "18",
                "exchange_rate": "1618.23", "address": "0xae7"}},
            {"value": str(10**18), "token": {  # spam: no exchange_rate → dropped
                "type": "ERC-20", "symbol": "SPAM", "decimals": "18",
                "exchange_rate": None, "address": "0xspam"}},
            {"value": str(10**17), "token": {  # dust: 0.1 × $0.001 = $0.0001 < $1 → dropped
                "type": "ERC-20", "symbol": "DUST", "decimals": "18",
                "exchange_rate": "0.001", "address": "0xdust"}},
        ]

    def test_filters_spam_and_dust(self, monkeypatch):
        monkeypatch.setattr(cb, "_get_json", lambda url: self._blockscout())
        toks = cb.fetch_eth_tokens(_ETH)
        assert [t["symbol"] for t in toks] == ["STETH"]
        assert toks[0]["value_usd"] == pytest.approx(0.7528 * 1618.23)
        assert toks[0]["contract"] == "0xae7"

    def test_collect_emits_native_eth_plus_token_rows(self):
        s3 = _s3(_universe(("ETH", _ETH)))
        r = cb.collect(
            s3_client=s3,
            balance_fetchers={"ETH": lambda a: 0.5},
            price_fetcher=lambda syms: {"ETH": 2000.0},
            eth_token_fetcher=lambda a: [{"symbol": "STETH", "balance": 1.0, "price_usd": 1600.0, "value_usd": 1600.0, "contract": "0xae7"}],
            now=_NOW,
        )
        assert r["status"] == "ok" and r["n_balances"] == 2  # native ETH + 1 token
        art = _puts(s3)[cb.HOLDINGS_KEY]
        syms = [b["symbol"] for b in art["balances"]]
        assert syms == ["ETH", "STETH"]
        assert art["balances"][1]["value_usd"] == 1600.0

    def test_token_fetch_failure_keeps_native_eth(self):
        s3 = _s3(_universe(("ETH", _ETH)))

        def _boom(a):
            raise RuntimeError("blockscout down")

        r = cb.collect(
            s3_client=s3,
            balance_fetchers={"ETH": lambda a: 0.5},
            price_fetcher=lambda syms: {"ETH": 2000.0},
            eth_token_fetcher=_boom, now=_NOW,
        )
        assert r["status"] == "ok" and r["n_balances"] == 1  # native ETH survives
        assert _puts(s3)[cb.HOLDINGS_KEY]["balances"][0]["symbol"] == "ETH"
