"""
Data-module preflight: connectivity + freshness checks run at the top of
``weekly_collector.main()`` before any real collection work starts.

Primitives live in ``alpha_engine_lib.preflight.BasePreflight``; this
module composes them with module-specific HTTP probes (polygon, FRED,
FMP /stable) + an ArcticDB-libraries-present gate.

Consolidated 2026-04-30 — the legacy ``validators/preflight.py`` has been
retired. Both files were running back-to-back from ``weekly_collector``
in the phase1 path with overlapping scope; the lib-based path is now
the single source of truth. See alpha-engine-lib README for the
2026-04-14 failure mode that motivated the library.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from alpha_engine_lib.preflight import BasePreflight
from alpha_engine_lib.secrets import get_secret

log = logging.getLogger(__name__)

_HTTP_TIMEOUT_SECS = 10.0

# FMP /stable probe: cheapest auth-gated call that distinguishes
# (a) valid key on /stable from (b) a key that still works on the
# sunsetted v3 endpoints but would silently 402/403 across our real
# collector calls. AAPL is guaranteed to exist and returns a small
# payload. Added 2026-04-20 after the v3→/stable migration; the
# collectors had been silently zeroing fundamentals for two weeks
# before detection.
_FMP_STABLE_PROBE_URL = "https://financialmodelingprep.com/stable/key-metrics-ttm"
_FMP_STABLE_PROBE_SYMBOL = "AAPL"

# Polygon.io reference-data probe — cheapest auth-gated call that
# validates both network reachability AND API-key validity.
_POLYGON_PROBE_URL = "https://api.polygon.io/v3/reference/tickers/AAPL"

# FRED observation probe — DFF (Federal Funds Rate) is a well-known
# series guaranteed to exist; matches collectors/macro.py usage.
_FRED_PROBE_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_PROBE_SERIES = "DFF"


class DataPreflight(BasePreflight):
    """Preflight checks for the alpha-engine-data entrypoint.

    Mode determines which external services must be reachable:

    - ``"daily"`` — weekday DailyData step. ArcticDB must be readable
      and SPY must be ≤4 days stale (covers Fri→Tue long weekends +
      1 day of buffer).
    - ``"phase1"`` — Saturday DataPhase1. External APIs (FRED, polygon)
      needed; no ArcticDB freshness check (phase1 is what *populates*
      ArcticDB).
    - ``"phase2"`` — Saturday DataPhase2. FMP /stable + Finnhub + SEC
      EDGAR needed.
    """

    def __init__(self, bucket: str, mode: str):
        super().__init__(bucket)
        if mode not in ("daily", "phase1", "phase2"):
            raise ValueError(f"DataPreflight: unknown mode {mode!r}")
        self.mode = mode

    def run(self) -> None:
        # Order: cheapest first so a trivially-broken run fails in <1s.
        # 1. env vars (local lookup)
        # 2. S3 bucket (~ms, IAM)
        # 3. mode-specific HTTP probes (~200ms each)
        # 4. ArcticDB checks (~100ms list_libraries; ~seconds for read)
        # AWS_REGION is a plain env var (boto3 region) — not a secret —
        # so it stays an os.environ check. The API keys below moved to
        # SSM via get_secret() in the .env-deprecation arc (#241/#242):
        # every collector + the reachability probes in this file resolve
        # them via get_secret(), so an os.environ assertion here is stale
        # and fails spuriously on the SSM-backed spot (no .env present).
        # Origin: 2026-05-16 Saturday SF DataPhase1 — phase1 preflight
        # aborted "required env vars missing: ['FRED_API_KEY',
        # 'POLYGON_API_KEY']" even though MorningEnrich (same collectors)
        # had just fetched polygon + FRED fine via get_secret().
        self.check_env_vars("AWS_REGION")
        if self.mode == "phase1":
            self._check_secrets("FRED_API_KEY", "POLYGON_API_KEY")
        elif self.mode == "phase2":
            self._check_secrets("FMP_API_KEY", "FINNHUB_API_KEY", "EDGAR_IDENTITY")

        self.check_s3_bucket()

        if self.mode == "phase1":
            # Catch credential drift / upstream outages BEFORE 30 min of
            # collector work. Net ~400ms across both probes.
            self._check_polygon_reachable()
            self._check_fred_reachable()
            # Bucket policies + IAM denies that HEAD doesn't catch:
            # PUT a sentinel + DELETE it. ~50ms. Caught the 2026-04-12
            # IAM-deny class on the spot's executor-role inline policy.
            self._check_s3_writeable_sentinel()
            # Phase 1 BUILDS ArcticDB universe + macro on first run, but
            # subsequent runs need both libraries already present. Enforce
            # that they exist so a typo in path_prefix fails in 100ms not
            # 50min into the run. Catches the 2026-04-14 silent-skip class.
            self._check_arcticdb_libraries_present(("universe", "macro"))
        elif self.mode == "phase2":
            self._check_fmp_stable_reachable()

        if self.mode == "daily":
            # SPY lives in the `macro` library (market-wide series). The
            # `universe` library holds per-stock OHLCV for S&P 500/400
            # constituents. daily_append writes to both libraries, so
            # macro/SPY freshness is a sufficient signal for the write
            # path being healthy end-to-end.
            # 4-day threshold covers Fri→Tue long weekends + 1 day of buffer.
            self.check_arcticdb_fresh("macro", "SPY", max_stale_days=4)
            # Both libraries must be present — same gate as phase1 for
            # operator-clarity on partial-deploy scenarios.
            self._check_arcticdb_libraries_present(("universe", "macro"))

    # ── Secret presence ──────────────────────────────────────────────────

    def _check_secrets(self, *names: str) -> None:
        """SSM-aware equivalent of ``check_env_vars`` for API-key secrets.

        Post the .env-deprecation arc (#241/#242) the API keys live in
        SSM, fetched lazily via ``get_secret()`` deeper in the run. This
        keeps the fail-fast intent of the old ``check_env_vars`` gate —
        abort in <1s, not 30min in — but resolves from SSM (with env
        fallback, which ``get_secret`` handles) instead of asserting
        ``os.environ`` directly. Raises the same RuntimeError shape so
        operator-facing failure text is unchanged.
        """
        missing = [
            n
            for n in names
            if not (get_secret(n, required=False, default="") or "").strip()
        ]
        if missing:
            raise RuntimeError(
                f"Pre-flight: required secrets missing: {missing}"
            )

    # ── Mode-specific primitives ─────────────────────────────────────────

    def _check_fmp_stable_reachable(self) -> None:
        """Validate FMP /stable auth + endpoint availability.

        Guards against the exact failure mode from the 2026-04 incident:
        the v3 endpoints silently 403'd (or paid-tier endpoints 402'd),
        the per-ticker exceptions logged at debug level, the collector
        returned all-NEUTRAL, and two weeks of fundamentals were zeroed
        before anyone noticed. A /stable probe at startup fails the
        Step Function in ~1s instead.
        """
        import requests

        api_key = (get_secret("FMP_API_KEY", required=False, default="") or "").strip()
        try:
            resp = requests.get(
                _FMP_STABLE_PROBE_URL,
                params={"symbol": _FMP_STABLE_PROBE_SYMBOL, "apikey": api_key},
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Pre-flight: FMP /stable unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code in (401, 403):
            raise RuntimeError(
                f"Pre-flight: FMP /stable auth failed (HTTP {resp.status_code}): "
                f"FMP_API_KEY invalid, revoked, or still pointing at the sunsetted v3 plan."
            )
        if resp.status_code == 402:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned HTTP 402 Payment Required on "
                f"key-metrics-ttm — the free tier no longer covers this endpoint. "
                f"Subscribe or move the collector to a different provider."
            )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned HTTP {resp.status_code} — upstream outage."
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned unexpected HTTP {resp.status_code} "
                f"on {_FMP_STABLE_PROBE_URL}: {resp.text[:200]}"
            )
        payload = resp.json()
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(
                f"Pre-flight: FMP /stable returned 200 but body was empty/malformed "
                f"for {_FMP_STABLE_PROBE_SYMBOL}: {str(payload)[:200]}"
            )
        log.info("preflight: FMP /stable reachable + auth valid (HTTP 200)")

    def _check_polygon_reachable(self) -> None:
        """Validate polygon.io network + auth via reference-data call.

        Catches expired API key, polygon outage, blocked egress. Does NOT
        catch rate-limit ceiling (next collector call will retry/fail
        loudly by design).
        """
        import requests

        api_key = (get_secret("POLYGON_API_KEY", required=False, default="") or "").strip()
        try:
            resp = requests.get(
                _POLYGON_PROBE_URL,
                params={"apiKey": api_key},
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Pre-flight: polygon.io unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code in (401, 403):
            raise RuntimeError(
                f"Pre-flight: polygon.io auth failed (HTTP {resp.status_code}): "
                f"POLYGON_API_KEY is invalid or revoked."
            )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Pre-flight: polygon.io returned HTTP {resp.status_code} on a reference-data call "
                f"— upstream outage. Check status.polygon.io."
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Pre-flight: polygon.io returned unexpected HTTP {resp.status_code} "
                f"on {_POLYGON_PROBE_URL}: {resp.text[:200]}"
            )
        log.info("preflight: polygon.io reachable + auth valid (HTTP 200)")

    def _check_fred_reachable(self) -> None:
        """Validate FRED network + auth via single-observation DFF call."""
        import requests

        api_key = (get_secret("FRED_API_KEY", required=False, default="") or "").strip()
        try:
            resp = requests.get(
                _FRED_PROBE_URL,
                params={
                    "series_id": _FRED_PROBE_SERIES,
                    "api_key": api_key,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 1,
                },
                timeout=_HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Pre-flight: FRED unreachable: {exc} — network outage or egress blocked."
            ) from exc

        if resp.status_code == 400:
            # FRED returns 400 with body containing "api_key" on bad key
            body = resp.text[:200].lower()
            if "api_key" in body or "invalid" in body:
                raise RuntimeError(
                    f"Pre-flight: FRED auth failed (HTTP 400): FRED_API_KEY is invalid. "
                    f"Response: {resp.text[:200]}"
                )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"Pre-flight: FRED returned HTTP {resp.status_code} on DFF call "
                f"— upstream outage."
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Pre-flight: FRED returned unexpected HTTP {resp.status_code}: {resp.text[:200]}"
            )
        log.info("preflight: FRED reachable + auth valid (HTTP 200)")

    def _check_s3_writeable_sentinel(self) -> None:
        """Validate S3 bucket grants PUT + DELETE via a sentinel object.

        ``BasePreflight.check_s3_bucket()`` only HEADs the bucket; that
        passes when IAM grants ListBucket but denies PutObject (bucket
        policy or scoped role). Surfacing the deny here saves ~40 min of
        spot time burning on collectors that all silently write 0 rows.
        """
        import boto3

        s3 = boto3.client("s3", region_name=self.region)
        sentinel_key = f"preflight/sentinel-{uuid.uuid4().hex}.txt"
        try:
            s3.put_object(
                Bucket=self.bucket,
                Key=sentinel_key,
                Body=b"preflight-sentinel",
                ContentType="text/plain",
            )
        except Exception as exc:
            raise RuntimeError(
                f"Pre-flight: S3 PUT s3://{self.bucket}/{sentinel_key} failed: {exc} — "
                f"IAM lacks s3:PutObject or bucket policy blocks writes."
            ) from exc

        try:
            s3.delete_object(Bucket=self.bucket, Key=sentinel_key)
        except Exception as exc:
            # Non-fatal: PUT (the load-bearing op for collectors) succeeded;
            # missing DELETE means sentinels accumulate but writes work.
            log.warning(
                "preflight: sentinel DELETE failed (%s) — preflight-sentinel objects "
                "may accumulate in s3://%s/preflight/. Check s3:DeleteObject IAM grant.",
                exc, self.bucket,
            )
        log.info("preflight: S3 bucket s3://%s read + write OK", self.bucket)

    def _check_arcticdb_libraries_present(self, expected: tuple[str, ...]) -> None:
        """Validate ArcticDB connection + that ``expected`` libraries exist.

        ``BasePreflight.check_arcticdb_fresh()`` covers freshness on a
        specific library/symbol pair, but the libraries-existence gate
        is a separate concern — useful at the cold-start / partial-deploy
        boundary where a typo in path_prefix or a half-applied infra
        change leaves the bucket reachable but the libraries absent.
        """
        try:
            import arcticdb as adb
        except ImportError as exc:
            raise RuntimeError(
                f"Pre-flight: arcticdb not importable — install "
                f"alpha-engine-lib[arcticdb] or add arcticdb to the deploy image: {exc}"
            ) from exc

        uri = (
            f"s3s://s3.{self.region}.amazonaws.com:{self.bucket}"
            "?path_prefix=arcticdb&aws_auth=true"
        )
        try:
            arctic = adb.Arctic(uri)
            libs = set(arctic.list_libraries())
        except Exception as exc:
            raise RuntimeError(
                f"Pre-flight: ArcticDB connection failed at {uri}: {exc}. "
                f"Check s3 prefix + credentials + arcticdb version."
            ) from exc

        missing = set(expected) - libs
        if missing:
            raise RuntimeError(
                f"Pre-flight: ArcticDB missing expected libraries: {sorted(missing)} "
                f"(found: {sorted(libs)}). Run backfill or verify path_prefix."
            )
        log.info(
            "preflight: ArcticDB connectable, libraries present: %s",
            sorted(expected),
        )
