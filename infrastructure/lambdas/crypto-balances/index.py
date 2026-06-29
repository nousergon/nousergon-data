"""alpha-engine-crypto-balances — 24/7 crypto wallet-balance producer (metron-ops#111).

Why a Lambda (and NOT a systemd timer on the trading box): crypto trades 24/7, but the
trading box only runs during the weekday pipeline and stops after EOD — a timer there would
sync crypto ONLY during market hours, going stale overnight and all weekend (exactly when
crypto keeps moving). An EventBridge Scheduler ``rate(15 minutes)`` → this Lambda gives
guaranteed around-the-clock execution with a CloudWatch trail and zero box coupling. The
EventBridge-Scheduler wiring mirrors the sibling ``scheduled-groom-dispatcher``.

Reuses the tested ``collectors.crypto_balances.collect()`` — vendored next to this handler
by ``deploy.sh`` (it has no intra-repo imports, only stdlib + boto3) — which reads
``metron/crypto/wallet_addresses.json``, fetches BTC/ETH balances + prices, and writes
``crypto/holdings.json``.

Fail posture: ``collect()`` already fails SOFT per address (WARN + counter — the recording
surface), so a healthy run returns ``ok`` (wrote) or ``skipped`` (no addresses). Only a
SYSTEMIC failure — every fetch failed (``status="error"``) — RAISES here, so EventBridge
retries + the Lambda error metric surface it rather than silently writing nothing.

Managed OUTSIDE CloudFormation — operator-deployed via ``deploy.sh --bootstrap``. Merging the
PR has ZERO live effect until bootstrapped.
"""

from __future__ import annotations

import logging
import os

import crypto_balances  # vendored alongside index.py by deploy.sh

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ.get("MARKET_DATA_BUCKET", "alpha-engine-research")
# Kill-switch (default on). Flip the Lambda env var to "false" to pause the producer
# without deleting the schedule.
ENABLED = os.environ.get("CRYPTO_BALANCES_ENABLED", "true").lower() == "true"


def handler(event, context):  # noqa: ARG001 - Lambda signature; event/context unused
    if not ENABLED:
        logger.info("crypto-balances disabled (CRYPTO_BALANCES_ENABLED != true) — skipping")
        return {"statusCode": 200, "body": {"status": "disabled"}}
    result = crypto_balances.collect(bucket=BUCKET, dry_run=False)
    logger.info("crypto-balances result: %s", result)
    if result.get("status") == "error":
        # Every address fetch failed — systemic. RAISE so EventBridge retries and the
        # Lambda error metric / alarm fire (a soft per-address miss never reaches here).
        raise RuntimeError(f"crypto-balances run failed: {result}")
    return {"statusCode": 200, "body": result}
