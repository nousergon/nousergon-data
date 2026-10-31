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
import pathlib

import crypto_balances  # vendored alongside index.py by deploy.sh
import run_units  # vendored alongside index.py by deploy.sh
from dates import default_run_date  # vendored alongside index.py by deploy.sh

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET = os.environ.get("MARKET_DATA_BUCKET", "alpha-engine-research")
# Kill-switch (default on). Flip the Lambda env var to "false" to pause the producer
# without deleting the schedule.
ENABLED = os.environ.get("CRYPTO_BALANCES_ENABLED", "true").lower() == "true"

#: The sha of the tree this zip was built from, written into the package by
#: ``deploy.sh``. A Lambda has no git checkout, so ``run_manifest`` could not
#: measure this for itself — and a sha carried in the ARTIFACT is the stronger
#: answer anyway: it names the code that is actually deployed rather than
#: whatever a working tree happened to be at some later moment.
_CODE_SHA_FILE = pathlib.Path(__file__).resolve().parent / "code_sha.txt"


def _deployed_code_sha() -> str | None:
    """The deployed sha, or ``None`` — never a guess.

    ``None`` means this package predates ``deploy.sh`` writing the file (or the
    file was lost). ``run_units.recorded_entry`` then runs the collector
    UNRECORDED and logs an ERROR; the crypto balances still publish. That is the
    deliberate ordering: the record layer never decides whether a producer
    produces (`data_collection_plan_260914.md` §4.4).
    """
    try:
        sha = _CODE_SHA_FILE.read_text(encoding="utf-8").strip()
    except OSError as exc:
        # (a) Failure mode swallowed: the packaged code_sha.txt is missing or
        #     unreadable, so no well-formed manifest can be written for this
        #     invocation. (b) Recording surface: this ERROR in the function's
        #     CloudWatch log group, plus the ABSENCE of an object under
        #     data_collection/runs/D38/, which the run-record clause grades as a
        #     missing run — red and counted, never green.
        logger.error(
            "crypto-balances: %s unreadable (%s) — this invocation will run UNRECORDED. "
            "Redeploy with deploy.sh, which writes the file into the package.",
            _CODE_SHA_FILE, exc,
        )
        return None
    return sha or None


def _record_run(ctx, result: dict) -> None:
    """Record what this D38 run read and published, with measured counts."""
    ctx.record_input(f"s3://{BUCKET}/{crypto_balances.WALLET_ADDRESSES_KEY}")
    n_failed = int(result.get("n_failed") or 0)
    if n_failed:
        # `collect()` already fails soft per address with a WARN; the manifest
        # is where that count becomes a number somebody can trend.
        ctx.reject("address_fetch_failed", n_failed)
    if result.get("status") == "skipped":
        # Nothing was published this cycle. Recorded as a guard reading rather
        # than as an output with rows_out=0, which would claim a write.
        ctx.record_guard(
            "data_empty_fresh",
            mode="observe",
            verdict="empty_fresh",
            detail=(
                f"D38 returned status='skipped' (reason={result.get('reason')!r}) and "
                f"published no key — s3://{BUCKET}/{crypto_balances.HOLDINGS_KEY} was not "
                "refreshed this cycle."
            ),
            key=crypto_balances.HOLDINGS_KEY,
            value=0.0,
        )
        return
    raw_rows = result.get("n_balances")
    rows = None if raw_rows is None else int(raw_rows)
    ctx.record_output(crypto_balances.HOLDINGS_KEY, rows_out=rows or 0)
    # alpha-engine-config-I10785 (P-18): the PUBLISH path is graded too, not just
    # the skip path above, so every D38 run files one `data_empty_fresh` verdict
    # and the guard's promotion reading can see it ran. Graded on the count
    # collect() measured from the rows it wrote, the way
    # weekly_collector.py::_check_arctic_library_output grades a measured count:
    # this Lambda vendors run_units.py flat without `validators/`, and HEADing
    # the key would need a read grant it does not hold. collect() refuses an
    # empty `balances` list before writing, so `empty_fresh` here means that
    # refusal regressed. An unreported count is `unmeasurable`, never a pass.
    if rows is None:
        verdict, detail = (
            "unmeasurable",
            "D38 reported status='ok' with no n_balances, so the rows behind "
            f"s3://{BUCKET}/{crypto_balances.HOLDINGS_KEY} cannot be counted.",
        )
    elif rows <= 0:
        verdict, detail = (
            "empty_fresh",
            f"D38 published s3://{BUCKET}/{crypto_balances.HOLDINGS_KEY} with ZERO "
            "balance rows: a fresh, empty write.",
        )
    else:
        verdict, detail = (
            "ok",
            f"D38 published {rows} balance row(s) to "
            f"s3://{BUCKET}/{crypto_balances.HOLDINGS_KEY}.",
        )
    ctx.record_guard(
        "data_empty_fresh",
        mode="observe",
        verdict=verdict,
        detail=detail,
        key=crypto_balances.HOLDINGS_KEY,
        value=None if rows is None else float(rows),
    )
    if verdict != "ok":
        logger.error("guard=data_empty_fresh unit=D38 verdict=%s mode=observe %s", verdict, detail)


def handler(event, context):  # noqa: ARG001 - Lambda signature; event/context unused
    if not ENABLED:
        logger.info("crypto-balances disabled (CRYPTO_BALANCES_ENABLED != true) — skipping")
        # The kill-switch short-circuit writes no manifest ON PURPOSE: the unit
        # did not execute, and a record claiming it did is worse than no record.
        # A disabled producer is visible as an ABSENCE of D38 manifests, which
        # the run-record clause reads as a missing run — which is exactly what a
        # disabled producer is. D38's descriptor declares `lifecycle: disabled`
        # so the board renders it as a decision rather than as a defect.
        return {"statusCode": 200, "body": {"status": "disabled"}}

    captured: dict = {}

    def _body(run_ctx) -> dict:
        result = crypto_balances.collect(bucket=BUCKET, dry_run=False)
        captured["result"] = result
        logger.info("crypto-balances result: %s", result)
        _record_run(run_ctx, result)
        if result.get("status") == "error":
            # Every address fetch failed — systemic. RAISE so EventBridge retries and the
            # Lambda error metric / alarm fire (a soft per-address miss never reaches here).
            # Raised from INSIDE the body so the manifest records `failed` with this
            # reason BEFORE the exception reaches EventBridge, and re-raised after the
            # record is durable — the handler's contract is unchanged.
            raise RuntimeError(f"crypto-balances run failed: {result}")
        return result

    result = run_units.recorded_entry(
        "D38",
        _body,
        trigger="scheduled",
        trading_day=default_run_date(),
        bucket=BUCKET,
        code_sha=_deployed_code_sha(),
    )
    return {"statusCode": 200, "body": result}
