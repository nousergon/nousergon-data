# alpha-engine-crypto-balances

24/7 crypto wallet-balance producer for Metron's standalone crypto page (metron-ops#111).

**What it does.** Every 15 minutes (EventBridge Scheduler `rate(15 minutes)`) the Lambda runs
`collectors/crypto_balances.collect()`: reads Metron's published wallet addresses
(`metron/crypto/wallet_addresses.json`), fetches BTC (Blockstream) + ETH (public JSON-RPC)
balances and CoinGecko USD prices, and writes `crypto/holdings.json` for Metron to read back.

**Why a Lambda, not a systemd timer.** Crypto trades 24/7, but the trading box (where the
EOD/intraday market-data producers live) only runs during the weekday pipeline and stops
after EOD. A timer there would sync crypto only during market hours — stale overnight and all
weekend, exactly when crypto moves. A scheduled Lambda runs around the clock with no box
coupling. (This replaced the initial `metron-crypto.{service,timer}` systemd approach.)

**Code reuse.** The handler (`index.py`) is thin; the logic lives in the tested
`collectors/crypto_balances.py`, which `deploy.sh` vendors flat into the package (it has no
intra-repo imports). One source of truth, covered by `tests/test_crypto_balances.py`.

**Fail posture.** `collect()` fails soft per address (WARN + counter). The handler RAISES only
on a systemic failure (every fetch failed → `status="error"`), so EventBridge retries + the
Lambda error metric surface it. `ok` (wrote) and `skipped` (no addresses) are healthy.

**Kill-switch.** Env `CRYPTO_BALANCES_ENABLED=false` pauses the producer without deleting the
schedule.

## Deploy (operator)

Managed outside CloudFormation. Merging the PR has **zero live effect** until bootstrapped.

```bash
# first-time: create the Lambda + IAM roles + the 15-min EventBridge Scheduler rule
bash infrastructure/lambdas/crypto-balances/deploy.sh --bootstrap

# subsequent code updates
bash infrastructure/lambdas/crypto-balances/deploy.sh

# preview without applying
bash infrastructure/lambdas/crypto-balances/deploy.sh --dry-run

# one real invocation (⚠ writes crypto/holdings.json)
bash infrastructure/lambdas/crypto-balances/deploy.sh --smoke
```

The Lambda role (`iam-policy.json`) is least-privilege: read
`metron/crypto/wallet_addresses.json`, write `crypto/holdings.json`, CloudWatch Logs.

## After first live write

Register the `crypto/holdings.json` freshness row in
`alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml` — **with/after** the producer is
live (never ahead of it). Cadence ~15 min. (metron-ops#111)
