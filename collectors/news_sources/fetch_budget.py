"""Shared wall-clock budget derivation for the multi-source news sweep.

Single source of truth (alpha-engine-config#2938) for the time budgets that
size the Polygon-bottlenecked news fetch across its surfaces:

  * per-source ``max_fetch_seconds`` (the adapter deadline guards below),
  * the daily ``daily-news.service`` ``TimeoutStartSec`` backstop, and
  * the weekly ``RAGIngestion`` SSM ``executionTimeout`` in nousergon-data's
    ``infrastructure/step_function.json`` (guarded there against the
    ``WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS`` cap declared here).

Why DERIVED, not hardcoded (config#2938 ruling 2). The 2026-07-18 double
incident — the daily digest *and* the weekly Saturday SF both SIGKILLed with
zero output — was a DRIFT bug: the news universe grew ~9x (27→903 signals
tickers) while a hardcoded 3600s timeout stayed put, so the sweep silently
outgrew its budget. The weekly runtime budget below is a pure function of the
LIVE universe size, so it can never again fall silently behind universe
growth.

The bottleneck is Polygon's free tier: 5 req/min (12s/ticker), an
account-wide quota shared with the price endpoints. ~944 tickers ⇒ ~3.15h of
irreducible Polygon crawl; GDELT (1 req/s) and Yahoo RSS are not the long
pole. The genuine SOTA (decoupling news collection from the Saturday critical
path — a warm, continuously-ingested corpus so Saturday reads instead of
fetches) is larger and tracked separately (config#2938 "out of scope"). This
module makes the on-critical-path sweep COMPLETE within a universe-sized
budget and, beyond it, FAIL SOFT (partial coverage + a loud WARN, never a
SIGKILL-with-zero-output) rather than taking the whole pipeline down.
"""

from __future__ import annotations

import math

# Polygon free tier: 5 req/min ⇒ 12s between requests, plus ~0.5s per-item
# processing. The account-wide 5 req/min quota (shared with the price
# endpoints via polygon_client) is the hard floor — concurrency cannot beat
# it, so this is the per-ticker cost that sizes every budget below.
POLYGON_SECONDS_PER_TICKER = 12.5


# ── Weekly (Saturday RAGIngestion) — config#2938 ruling 1: FULL coverage ────
# The weekly corpus feeds predictor training + research, so it is sized to
# COMPLETE the full-universe Polygon sweep, NOT to bail early. Hard-capped at
# ~6h (ruling 2) so the fetch plus the rest of the RAGIngestion step
# (GDELT/Yahoo/NLP/RAG-ingest, a few min) fit inside the SSM executionTimeout.
#
# LOCKSTEP: this cap is mirrored by three timeouts in nousergon-data —
# the RAGIngestion ``executionTimeout`` in infrastructure/step_function.json,
# the inner ``run_ssm "rag-only"`` timeout, and the rag-only spot-watchdog
# ``MAX_RUNTIME_SECONDS`` — and is CI-guarded there. Changing it here means
# changing all three (their guard test names this constant).
WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS = 21_600  # 6h

# Reserve inside the step for everything that is NOT the Polygon crawl:
# spot bootstrap/deps + the SEC-filings ingestion phase + the GDELT/Yahoo
# sources + NLP + the RAG-ingest step. MEASURED, not guessed: on 2026-07-18
# the filings phase ALONE ran >=3600s on both live attempts (attempt-0 and
# watch-rerun-2026-07-18-2 were still mid-filings when killed at the 1h
# inner cap) — the original 2_400s reserve under-counted it, leaving
# nominal filings + full Polygon sweep (~11_328s) over the old 4h envelope.
_WEEKLY_STEP_NON_POLYGON_RESERVE_SECONDS = 6_000

# A small universe still gets a sane budget (never below this floor).
_WEEKLY_NEWS_FLOOR_SECONDS = 1_800

# The Polygon sweep may consume at most (6h − reserve) so the rest of the
# step always gets to run within the SSM executionTimeout.
_WEEKLY_POLYGON_CAP_SECONDS = (
    WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS - _WEEKLY_STEP_NON_POLYGON_RESERVE_SECONDS
)  # 15_600


def weekly_news_max_fetch_seconds(universe_size: int) -> int:
    """``max_fetch_seconds`` for the WEEKLY (Saturday) Polygon news sweep.

    Sized from the LIVE universe to COMPLETE the full sweep (config#2938
    ruling 1), floored so a small universe still gets a sane budget, and
    capped so the fetch plus the rest of the RAGIngestion step fit inside the
    6h SSM ``executionTimeout``. The adapter's own deadline guard is then a
    SIGKILL backstop (partial coverage + WARN), never the intended operating
    mode — at the ~944-ticker universe the ~3.15h crawl completes well inside
    the derived ~11.8k s budget.
    """
    raw = math.ceil(max(universe_size, 0) * POLYGON_SECONDS_PER_TICKER)
    return int(min(max(raw, _WEEKLY_NEWS_FLOOR_SECONDS), _WEEKLY_POLYGON_CAP_SECONDS))


# ── Daily (daily-news.service) — config#2938 ruling 1: bail-early partial ───
# The daily digest tolerates reduced coverage (``partial: true``); a tight
# per-source budget bails and still writes a real digest. Sources run under
# the CONCURRENT AsyncNewsAggregator, so the outer systemd ``TimeoutStartSec``
# need only cover ONE source's budget plus aggregation/write — NOT a
# sequential sum over the universe (which is why the daily backstop stays a
# universe-independent constant while the weekly budget scales).
DAILY_NEWS_MAX_FETCH_SECONDS = 1_200  # 20 min (matches GDELT's prior default)

# NLP + dedup + digest build + S3 write + margin, on top of the single
# per-source bail budget above.
DAILY_NEWS_AGGREGATION_RESERVE_SECONDS = 1_200


def daily_news_timeout_start_seconds() -> int:
    """systemd ``TimeoutStartSec`` backstop for ``daily-news.service``.

    One source's bail budget + the aggregation/write reserve. Sources fetch
    CONCURRENTLY (AsyncNewsAggregator), so this is a max-plus-reserve, not a
    sequential sum — the outer cap is a backstop, the per-source
    ``max_fetch_seconds`` is the primary defence.
    """
    return DAILY_NEWS_MAX_FETCH_SECONDS + DAILY_NEWS_AGGREGATION_RESERVE_SECONDS
