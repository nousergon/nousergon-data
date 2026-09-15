"""Producer contract test for crypto_holdings.schema.json (D38, data-collector plan
P-07, alpha-engine-config-I10870).

Mirrors the existing contract-test pattern (test_staging_daily_closes_contract.py,
test_p07_contracts.py): every artifact this producer writes must validate cleanly
against its own versioned JSON Schema, checked at PR time. Consumer: Metron
`api/services/crypto.py::_read_holdings_s3` / `for_portfolio` (metron
`tests/contracts/crypto_holdings.schema.json`, pinned separately).

Covers:
  - A REAL artifact produced by `collectors.crypto_balances.collect()` (via the
    injectable fetcher seam already used by test_crypto_balances.py), for both the
    priced and the price-degraded ("prices" partial/empty) cases.
  - A hand-built minimal fixture, independent of the producer code, so a producer
    bug that silently matches its own (wrong) output can't also pass the contract
    by construction.
  - A record missing a required field is rejected.

D38 is PAUSED (alpha-engine-config-I10748, 2026-08-07); the contract is built
regardless of run cadence — Metron already reads `crypto/holdings.json` (plan §3:
a contract belongs to any surviving consumer, not only an actively-scheduled one).
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("jsonschema")

from collectors import crypto_balances as cb
from contracts import validate_crypto_holdings
from tests.test_crypto_balances import _BTC, _ETH, _NOW, _fetchers, _puts, _s3, _universe


def _schema() -> dict:
    from pathlib import Path

    path = Path(__file__).parent.parent / "contracts" / "crypto_holdings.schema.json"
    return json.loads(path.read_text())


class TestSchemaIsValid:
    def test_schema_file_parses_as_json_schema(self):
        import jsonschema

        jsonschema.Draft202012Validator.check_schema(_schema())

    def test_schema_pins_version_1(self):
        assert _schema()["properties"]["schema_version"]["const"] == 1


class TestProducerWriteValidates:
    def test_happy_path_artifact_validates(self):
        s3 = _s3(_universe(("BTC", _BTC), ("ETH", _ETH)))
        r = cb.collect(
            s3_client=s3,
            balance_fetchers=_fetchers({"BTC": 0.5, "ETH": 2.0}),
            price_fetcher=lambda syms: {"BTC": 60000.0, "ETH": 3000.0},
            eth_token_fetcher=lambda a: [],
            now=_NOW,
        )
        assert r["status"] == "ok"
        art = _puts(s3)[cb.HOLDINGS_KEY]
        errors = validate_crypto_holdings(art)
        assert errors == [], errors

    def test_price_degraded_artifact_still_validates(self):
        """price_fetcher failure omits price_usd/value_usd (best-effort) — the
        producer still writes balance-only rows, which must remain contract-valid."""
        s3 = _s3(_universe(("BTC", _BTC),))
        r = cb.collect(
            s3_client=s3,
            balance_fetchers=_fetchers({"BTC": 0.5}),
            price_fetcher=lambda syms: (_ for _ in ()).throw(RuntimeError("prices down")),
            eth_token_fetcher=lambda a: [],
            now=_NOW,
        )
        assert r["status"] == "ok"
        art = _puts(s3)[cb.HOLDINGS_KEY]
        assert "price_usd" not in art["balances"][0]
        errors = validate_crypto_holdings(art)
        assert errors == [], errors


class TestHandBuiltFixtureValidates:
    """Independent of the producer code — a producer bug can't pass by construction."""

    def _artifact(self, **overrides) -> dict:
        art = {
            "schema_version": 1,
            "as_of_utc": "2026-09-15T12:00:00Z",
            "source": "blockstream+eth_rpc+coingecko+blockscout",
            "balances": [
                {"chain": "BTC", "address": _BTC, "symbol": "BTC", "balance": 0.5,
                 "price_usd": 60000.0, "value_usd": 30000.0},
            ],
            "prices": {"BTC": 60000.0},
        }
        art.update(overrides)
        return art

    def test_full_record_validates(self):
        assert validate_crypto_holdings(self._artifact()) == []

    def test_balance_without_price_validates(self):
        art = self._artifact()
        del art["balances"][0]["price_usd"]
        del art["balances"][0]["value_usd"]
        assert validate_crypto_holdings(art) == []

    def test_missing_required_top_level_field_is_rejected(self):
        art = self._artifact()
        del art["as_of_utc"]
        assert validate_crypto_holdings(art) != []

    def test_balance_row_missing_required_field_is_rejected(self):
        art = self._artifact()
        del art["balances"][0]["balance"]
        assert validate_crypto_holdings(art) != []
