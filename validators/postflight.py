"""
Data-module postflight: consumer-contract checks run at the end of DataPhase1
before the health marker is written.

Producer owns the consumer contract. Every downstream freshness/shape check
(research's ``PriceFetchError`` / ``MacroFetchError``, predictor's
``_verify_arctic_fresh``, backtester's ArcticDB reads) must have a matching
producer-side postflight that fails FIRST. This eliminates blast-radius
fan-out — one ``alpha-engine-saturday-sf-failed`` alarm at DataPhase1 instead
of 3× downstream cold-starts each reporting the same root cause — and avoids
compute waste on downstream Lambda invocations / spot-EC2 bootstraps doomed
to fail at preflight.

Contract encoded here is the union of:

  1. Predictor `inference/stages/load_prices.py::_verify_arctic_fresh`
     (SPY last-row ≥ run_date - 1)
  2. Research `data/fetchers/price_fetcher.py::_load_constituents_from_s3`
     (``constituents.json`` HEAD + parse + ≥ 800 tickers)
  3. Research `data/fetchers/macro_fetcher.py::fetch_macro_data`
     (``macro.json`` HEAD + parse + ``fed_funds_rate`` populated)
  4. Research preflight `_check_arcticdb_universe`
     (universe library reachable, sample tickers fresh)

Failure semantics: each check raises ``PostflightError`` (a ``RuntimeError``
subclass) with a specific named message. The caller in
``weekly_collector._finalize()`` catches, flips ``results["status"]`` to
``"postflight_failed"``, writes the health marker accordingly, and lets
``main()``'s SystemExit(1) propagate through SSM → Step Function
HandleFailure → CloudWatch alarm.
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


class PostflightError(RuntimeError):
    """Raised when a DataPhase1 output fails a consumer contract check."""
    pass


# Universe sample size for freshness checks. 20 tickers balances confidence
# in detecting partial writes (one missing ticker in 900 is ~0.1% — sampling
# 20 gives ~20% chance of catching a single-ticker miss per run, but catches
# any systematic write failure that drops >5% of tickers with near-certainty).
_UNIVERSE_SAMPLE_SIZE = 20

# Max staleness of a sampled universe ticker relative to SPY's last row, in
# *trading days*. >0 tolerates a single missing session (e.g. one ticker
# caught in a partial-write race); >2 starts looking like systematic write
# failure. Trading-day-aware via nousergon_lib.dates so post-Saturday
# redrives don't trip on calendar-day weekend artifacts.
_UNIVERSE_MAX_STALE_VS_SPY_TRADING_DAYS = 2

# Minimum macro.SPY freshness in *trading days*: must carry the most recent
# NYSE close that exists as of run_date. Holiday-aware via nousergon_lib.
# dates.is_fresh_in_trading_days — replaces the calendar-day arithmetic that
# broke every post-Saturday redrive (2026-05-24 incident). Threshold is 0 (the
# producer just wrote; must carry the latest close, no T+1 tolerance).
_MACRO_SPY_MAX_STALE_TRADING_DAYS = 0

# Minimum constituent count for a valid constituents.json payload.
# Matches research's ``fetch_sp500_sp400_with_sectors`` contract
# (S&P 500 + S&P 400 ≈ 900 tickers; we tolerate down to 800 for membership
# churn + deduplication margin).
_MIN_CONSTITUENTS = 800


class DataPostflight:
    """Producer-side consumer-contract checks for DataPhase1 output.

    Parameters
    ----------
    bucket : str
        S3 bucket hosting market_data/ + ArcticDB + health/ prefixes.
    run_date : str
        YYYY-MM-DD stamp identifying the Saturday pipeline run.
    market_prefix : str
        S3 key prefix for market_data (typically ``"market_data/"``).
    phase : int
        Phase number (only Phase 1 is gated today; Phase 2 gets its own
        postflight when the alternative-data contract is encoded).
    """

    def __init__(
        self,
        bucket: str,
        run_date: str,
        market_prefix: str,
        phase: int,
    ) -> None:
        self.bucket = bucket
        self.run_date = run_date
        self.market_prefix = market_prefix
        self.phase = phase
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        # Lazy-initialized handles (set on first use to keep __init__ cheap).
        self._s3 = None
        self._universe_lib = None
        self._macro_lib = None

    # ── Lazy handles ─────────────────────────────────────────────────────────

    def _s3_client(self):
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client("s3", region_name=self.region)
        return self._s3

    def _open_arctic_libs(self) -> "tuple[Any, Any]":
        if self._universe_lib is None or self._macro_lib is None:
            from nousergon_lib.arcticdb import open_universe_lib, open_macro_lib
            try:
                self._universe_lib = open_universe_lib(self.bucket, region=self.region)
                self._macro_lib = open_macro_lib(self.bucket, region=self.region)
            except Exception as exc:
                raise PostflightError(str(exc)) from exc
        return self._universe_lib, self._macro_lib

    # ── Checks ───────────────────────────────────────────────────────────────

    def _check_macro_spy_fresh(self) -> None:
        """Consumer: predictor ``_verify_arctic_fresh``.

        SPY lives in the ArcticDB macro library. Its last row must carry the
        most recent NYSE close that exists as of ``run_date``. Trading-day-
        aware via ``nousergon_lib.dates.is_fresh_in_trading_days``:
        Friday's close passes on Saturday/Sunday/Memorial-Day-Monday runs
        because zero NYSE sessions have closed in between. The earlier
        calendar-day formulation (``(run_date - last_date).days > 1``) broke
        every post-Saturday redrive (2026-05-24 incident).
        """
        from nousergon_lib.dates import (
            is_fresh_in_trading_days,
            trading_days_stale,
            expected_last_close,
        )

        _, macro_lib = self._open_arctic_libs()
        try:
            df = macro_lib.read("SPY", columns=["Close"]).data
        except Exception as exc:
            raise PostflightError(
                f"ArcticDB macro.SPY unreadable: {exc} — universe_returns or "
                f"daily_append did not write SPY this run."
            ) from exc

        if df is None or df.empty:
            raise PostflightError(
                "ArcticDB macro.SPY has zero rows — daily_append has never written."
            )

        last_ts = pd.Timestamp(df.index[-1])
        if last_ts.tzinfo is not None:
            last_ts = last_ts.tz_convert("UTC").tz_localize(None)
        last_date = last_ts.normalize().date()

        if not is_fresh_in_trading_days(
            last_date, self.run_date,
            max_stale=_MACRO_SPY_MAX_STALE_TRADING_DAYS,
        ):
            stale = trading_days_stale(last_date, self.run_date)
            expected = expected_last_close(self.run_date)
            raise PostflightError(
                f"ArcticDB macro.SPY last_date={last_date} is {stale} "
                f"trading-day(s) behind the expected last close {expected} "
                f"for run_date={self.run_date} "
                f"(>{_MACRO_SPY_MAX_STALE_TRADING_DAYS}d threshold). "
                f"Predictor's _verify_arctic_fresh will reject this."
            )
        log.info(
            "postflight: ArcticDB macro.SPY last_date=%s "
            "(0 trading-day(s) stale ≤ %d threshold)",
            last_date, _MACRO_SPY_MAX_STALE_TRADING_DAYS,
        )

    def _check_universe_sample_fresh(self) -> None:
        """Consumer: research preflight + predictor per-ticker ArcticDB reads.

        Samples ``_UNIVERSE_SAMPLE_SIZE`` tickers from the universe library and
        asserts each has a last-row date within ``_UNIVERSE_MAX_STALE_VS_SPY_DAYS``
        of SPY's last row. Catches partial writes where most tickers landed
        but some didn't — the symptom that would otherwise surface as
        per-ticker error rate in research's ``PriceFetchError`` check at
        Lambda runtime.
        """
        from nousergon_lib.dates import trading_days_stale

        universe_lib, macro_lib = self._open_arctic_libs()

        # SPY last date serves as the staleness reference (already validated above).
        spy_last = pd.Timestamp(macro_lib.read("SPY", columns=["Close"]).data.index[-1])
        if spy_last.tzinfo is not None:
            spy_last = spy_last.tz_convert("UTC").tz_localize(None)
        spy_last_date = spy_last.normalize().date()

        symbols = list(universe_lib.list_symbols())
        # Filter sector ETFs + macro symbols out of the stock sample.
        macro_syms = {"SPY", "VIX", "VIX3M", "TNX", "IRX", "GLD", "USO"}
        sector_prefixes = ("XL",)  # XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY
        stock_syms = [
            s for s in symbols
            if s not in macro_syms and not s.startswith(sector_prefixes)
        ]
        if len(stock_syms) < _UNIVERSE_SAMPLE_SIZE:
            raise PostflightError(
                f"ArcticDB universe has only {len(stock_syms)} non-macro symbols "
                f"(expected ≥ {_UNIVERSE_SAMPLE_SIZE}). Backfill has not run or "
                f"universe library is empty."
            )

        # Deterministic sample on run_date to keep the check reproducible
        # for debugging (same run_date → same sample → same failure mode).
        rng = random.Random(self.run_date)
        sample = rng.sample(stock_syms, _UNIVERSE_SAMPLE_SIZE)

        stale_tickers: list[tuple[str, int]] = []
        for sym in sample:
            try:
                df = universe_lib.read(sym, columns=["Close"]).data
            except Exception as exc:
                raise PostflightError(
                    f"ArcticDB universe.{sym} read failed: {exc}"
                ) from exc
            if df is None or df.empty:
                stale_tickers.append((sym, 9999))
                continue
            last_ts = pd.Timestamp(df.index[-1])
            if last_ts.tzinfo is not None:
                last_ts = last_ts.tz_convert("UTC").tz_localize(None)
            # Trading-day staleness vs SPY's last_date — calendar arithmetic
            # would over-fail on partial writes that landed late in the
            # session window (e.g. Friday write where one ticker landed
            # Thursday — calendar 1d, trading 1d). Both fine here, but the
            # primitive aligns with the system-wide convention.
            stale = trading_days_stale(last_ts.normalize().date(), spy_last_date)
            if stale > _UNIVERSE_MAX_STALE_VS_SPY_TRADING_DAYS:
                stale_tickers.append((sym, stale))

        if stale_tickers:
            raise PostflightError(
                f"ArcticDB universe sample has {len(stale_tickers)}/{_UNIVERSE_SAMPLE_SIZE} "
                f"tickers >{_UNIVERSE_MAX_STALE_VS_SPY_TRADING_DAYS} trading-day(s) "
                f"stale vs SPY ({spy_last_date}): {stale_tickers[:5]}"
                + (" ..." if len(stale_tickers) > 5 else "")
                + ". daily_append partial-write suspected — downstream reads "
                "will silently drop stale tickers."
            )
        log.info(
            "postflight: universe sample %d/%d tickers fresh "
            "(within %d trading-day(s) of SPY %s)",
            len(sample) - len(stale_tickers), len(sample),
            _UNIVERSE_MAX_STALE_VS_SPY_TRADING_DAYS, spy_last_date,
        )

    def _check_macro_json_contract(self) -> None:
        """Consumer: research ``macro_fetcher.fetch_macro_data``.

        Asserts ``market_data/weekly/<run_date>/macro.json`` exists, is
        parseable JSON, and has a populated ``fed_funds_rate`` field.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/macro.json"
        data = self._fetch_json(key, name="macro.json")
        if data.get("fed_funds_rate") is None:
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing 'fed_funds_rate' — "
                f"research's MacroFetchError will reject this. Upstream "
                f"collector produced a malformed output."
            )
        log.info("postflight: macro.json OK (fed_funds_rate=%s)", data["fed_funds_rate"])

    def _check_short_interest_json_contract(self) -> None:
        """Consumer: research short-interest reader (Phase 7c follow-up).

        Asserts ``market_data/weekly/<run_date>/short_interest.json`` exists,
        is parseable JSON, and has at least 50% of its requested tickers
        populated. Mirrors the collector's ``_MIN_OK_RATIO`` gate so an upstream
        partial-write that bypassed the collector's own status check still
        fails postflight.

        Soft-launch tolerance: if the file is absent (collector disabled via
        ``config["short_interest"]["enabled"] = false``), log + skip rather
        than hard-fail. Once research wires up the consumer, missing-file
        will become a hard-fail — but until then, the collector is an
        opt-out feature that shouldn't break the pipeline by being absent.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/short_interest.json"
        s3 = self._s3_client()
        try:
            s3.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            log.info(
                "postflight: short_interest.json absent (collector likely disabled) "
                "— skipping check. Will become hard-fail once research consumer ships."
            )
            return

        data = self._fetch_json(key, name="short_interest.json")
        if not isinstance(data.get("data"), dict):
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing 'data' dict — schema violation."
            )
        ticker_count = data.get("ticker_count", 0)
        ok_count = data.get("ok_count", 0)
        if ticker_count == 0:
            raise PostflightError(
                f"s3://{self.bucket}/{key} ticker_count=0 — collector wrote an empty payload."
            )
        ok_ratio = ok_count / ticker_count
        if ok_ratio < 0.50:
            raise PostflightError(
                f"s3://{self.bucket}/{key} only {ok_count}/{ticker_count} tickers "
                f"populated ({ok_ratio:.1%}) — yfinance outage suspected."
            )
        log.info(
            "postflight: short_interest.json OK (%d/%d tickers populated, %.1f%%)",
            ok_count, ticker_count, ok_ratio * 100,
        )

    def _check_constituents_json_contract(self) -> None:
        """Consumer: research ``price_fetcher.fetch_sp500_sp400_with_sectors``.

        Asserts ``market_data/weekly/<run_date>/constituents.json`` exists,
        is parseable JSON, has a ``tickers`` array of ≥ 800 symbols, and
        has a ``sector_map`` dict covering them.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/constituents.json"
        data = self._fetch_json(key, name="constituents.json")
        tickers = data.get("tickers", [])
        if not tickers or len(tickers) < _MIN_CONSTITUENTS:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has {len(tickers)} tickers "
                f"(expected ≥ {_MIN_CONSTITUENTS}). Research's PriceFetchError "
                f"will reject this on ingest."
            )
        if not isinstance(data.get("sector_map"), dict):
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing 'sector_map' dict — "
                f"research scanner requires ticker→sector mapping."
            )
        log.info(
            "postflight: constituents.json OK (%d tickers, %d sector_map entries)",
            len(tickers), len(data["sector_map"]),
        )

    def _check_latest_weekly_pointer(self) -> None:
        """Pointer must roll forward to ``run_date``.

        The #1 class of bug hiding between a successful collector run and a
        consumer that reads yesterday's data: the per-date artifacts write
        successfully but the ``latest_weekly.json`` pointer doesn't update.
        Downstream reads stale data while upstream reports ``status=ok``.
        """
        key = f"{self.market_prefix}latest_weekly.json"
        data = self._fetch_json(key, name="latest_weekly.json")
        ptr_date = data.get("date")
        if ptr_date != self.run_date:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has date={ptr_date!r} but run_date="
                f"{self.run_date!r}. Pointer did not roll forward — consumers "
                f"would read yesterday's data while upstream reports success."
            )
        expected_prefix = f"{self.market_prefix}weekly/{self.run_date}/"
        if data.get("s3_prefix") != expected_prefix:
            raise PostflightError(
                f"s3://{self.bucket}/{key} has s3_prefix={data.get('s3_prefix')!r} "
                f"but expected {expected_prefix!r}."
            )
        log.info("postflight: latest_weekly.json pointer OK (date=%s)", ptr_date)

    def _check_health_marker_matches(self, expected_status: str) -> None:
        """The health marker is written AFTER this postflight — so we can't
        cross-check its content here without a chicken-and-egg.

        Instead, this is a placeholder for a Phase 7 follow-up: assert that
        the last-seen ``health/data_phase1.json`` matches the in-process
        ``results["status"]`` flowing through ``_finalize``. For now we skip
        — the in-process status is what we're writing into the marker next,
        so they are tautologically equal at this point in the flow.
        """
        log.debug(
            "postflight: health marker consistency check skipped (tautological at this phase)"
        )

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _fetch_json(self, key: str, name: str) -> dict:
        s3 = self._s3_client()
        try:
            obj = s3.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            raise PostflightError(
                f"s3://{self.bucket}/{key} unreadable: {exc} — "
                f"{name} did not write or pointer is broken."
            ) from exc
        try:
            return json.loads(obj["Body"].read())
        except Exception as exc:
            raise PostflightError(
                f"s3://{self.bucket}/{key} is not valid JSON: {exc}"
            ) from exc

    def _check_alternative_manifest_contract(self) -> None:
        """Consumer: research scoring's qual sub-score reads
        ``market_data/weekly/<run_date>/alternative/<TICKER>.json`` per
        promoted ticker.

        Phase 2's collector (``collectors/alternative.py::collect``) ships
        a per-source ``ok_ratio`` gate — if any of the 6 sub-fetchers
        (analyst_consensus, eps_revision, options_flow, insider_activity,
        institutional, news) drops below its source-specific floor, the
        collector returns ``status="error"``. This postflight check
        re-verifies the manifest's per-source ratios meet the same
        contract — belt-and-suspenders against a partial write that
        bypassed the collector's own status check.

        Soft-launch tolerance: if the manifest is absent (Phase 2 not yet
        run for this ``run_date`` on a Phase-1-only postflight invocation,
        or the alternative collector was disabled), log + skip rather
        than hard-fail. Mirrors the ``short_interest`` opt-out path.
        """
        key = f"{self.market_prefix}weekly/{self.run_date}/alternative/manifest.json"
        s3 = self._s3_client()
        try:
            s3.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            log.info(
                "postflight: alternative/manifest.json absent (Phase 2 likely "
                "not yet run for %s) — skipping check.", self.run_date,
            )
            return

        data = self._fetch_json(key, name="alternative/manifest.json")
        n_tickers = data.get("tickers_requested", 0)
        if n_tickers == 0:
            raise PostflightError(
                f"s3://{self.bucket}/{key} tickers_requested=0 — "
                f"Phase 2 collector wrote an empty payload."
            )

        floors = data.get("source_min_ok_ratios") or {}
        observed = data.get("source_ok_ratios") or {}
        if not floors or not observed:
            raise PostflightError(
                f"s3://{self.bucket}/{key} missing source_min_ok_ratios "
                f"or source_ok_ratios — schema violation. Phase 2 collector "
                f"must emit both fields per the per-source ok_ratio gate."
            )

        breached: list[str] = []
        for source, floor in floors.items():
            ratio = observed.get(source)
            if ratio is None:
                breached.append(f"{source}: missing from manifest")
            elif ratio < floor:
                breached.append(
                    f"{source}: {ratio:.1%} < {floor:.0%} threshold"
                )

        if breached:
            raise PostflightError(
                f"s3://{self.bucket}/{key} per-source ok_ratio gate breached "
                f"for {len(breached)} of {len(floors)} sources — {breached}. "
                f"Research scoring would silently degrade. Phase 2 collector "
                f"already returned status=error; this check is the "
                f"belt-and-suspenders verification."
            )
        log.info(
            "postflight: alternative/manifest.json OK (%d tickers, %d sources "
            "all above per-source floors)",
            n_tickers, len(floors),
        )

    # ── Entry point ──────────────────────────────────────────────────────────

    def run(self) -> None:
        """Run every check in sequence. Fail on the first contract violation."""
        if self.phase == 1:
            # Ordered so the cheapest + most-likely-to-fail checks run first.
            self._check_latest_weekly_pointer()
            self._check_macro_json_contract()
            self._check_constituents_json_contract()
            self._check_short_interest_json_contract()
            self._check_macro_spy_fresh()
            self._check_universe_sample_fresh()
            log.info("postflight: all DataPhase1 consumer contracts satisfied")
        elif self.phase == 2:
            self._check_alternative_manifest_contract()
            log.info("postflight: all DataPhase2 consumer contracts satisfied")
        else:
            log.info(
                "postflight: phase=%d is not gated today. Skipping.",
                self.phase,
            )
            return
