"""Shared RAG corpus ticker-scope resolver (config#2943 / EPIC config#2967).

BINDING RULING (Brian, 2026-07-18 late, alpha-engine-config#2943): the RAG
corpus must NOT cover the full signals universe (~900 tickers) — that was
accidental coupling via ``--from-signals`` picking up every ticker in
``signals.json`` after the predictor-side quant-envelope universe expansion
(27 → 903 tickers; that expansion is deliberate and STAYS, it just should
never have been the RAG corpus's ticker source too).

The corpus scope RULING is::

    holdings ∪ active candidates ∪ top-60 signals board   ("Think Tank" tier)

evaluated at INGESTION TIME from live artifacts — not the full universe:

  * holdings      — Metron's nightly ``metron/holdings_universe.json``
                     (config#1506; same source ``collectors/daily_news.py``
                     already reads for the daily news union).
  * candidates    — the Scanner Lambda's same-day
                     ``candidates/{trading_day}/candidates.json``
                     (``scanner_candidates_json`` in ARTIFACT_REGISTRY.yaml;
                     runs after DataPhase1, before RAGIngestion, so a
                     same-day read is always available by the time any
                     corpus ingestor — daily or Saturday — runs) plus the
                     research Lambda's ``buy_candidates`` slice of
                     ``signals/{trading_day}/signals.json`` (belt-and-
                     braces: both are "actionable now" sets, a ticker in
                     either is in scope).
  * top-60 board  — the highest-``score`` 60 entries in signals.json's
                     ``universe`` array (NOT a literal top-60 filter over
                     holdings/candidates — those retain coverage even
                     outside the top 60, per the ruling's explicit carve-out
                     that "held names and active candidates must retain
                     corpus coverage even when outside the top-60").

This module replaces the whole-universe ``--from-signals`` flag with a single
``--scope holdings+candidates+board60`` resolver SHARED by every corpus
ingestor (SEC filings, 8-K, earnings transcripts, theses, news, 13F) — one
resolver function in one place, not six duplicated inline S3 fetches.

Fail-soft, matching the sibling loaders' convention (``_signals_universe``,
``collectors/daily_news._load_holdings_universe``): a missing/unreadable
artifact degrades that ONE slice to empty rather than aborting the whole
scope resolution — a corpus ingestor with a partial scope on a bad day is
better than a hard failure that blocks the whole daily delta or Saturday
top-up.

Ticker churn (a ticker entering scope for the first time) is a caller-side
concern, not this module's — see ``rag/pipelines/run_daily_corpus_delta.sh``
(Step 1-2) plus ``rag/pipelines/_corpus_scope_state.py``, which diffs
today's resolved scope against yesterday's cached scope (S3
``rag_corpus/scope_state/latest.json``) to detect newly-entered tickers and
fold their 2yr-filings backfill into that day's delta pass (config#2943
deliverable 2b). ``_corpus_scope_state.py`` also backs the Saturday
top-up's cold-start/missed-week guard (``needs_wide_topup``) — if the
daily delta hasn't run recently, the Saturday step widens its own lookback
windows back to full-coverage instead of silently running a thin top-up.
"""

from __future__ import annotations

import json
import logging
from datetime import date as Date
from datetime import timedelta
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"

HOLDINGS_UNIVERSE_KEY = "metron/holdings_universe.json"

# Top-N of the signals.json ``universe`` array, ranked by ``score`` desc.
# The ruling's "~100-150 effective tickers" comes from this ∪ holdings ∪
# candidates — NOT from raising this constant. Keep at 60 per the ruling's
# literal "top-60 signals board" language.
BOARD_TOP_N = 60

# How many trailing days to search backward for the latest dated prefix of
# an S3-dated artifact (signals/{date}/, candidates/{date}/) when today's
# hasn't landed yet (weekend/holiday gap, or this resolver running before
# the same-day producer). Mirrors the fallback window already used by
# ``collectors/alternative.py``'s signals reader.
_LOOKBACK_DAYS = 8


def _get_s3_client(s3_client: Any = None) -> Any:
    if s3_client is not None:
        return s3_client
    import boto3
    return boto3.client("s3")


def _read_json_with_fallback(
    s3_client: Any,
    bucket: str,
    prefix_template: str,
    filename: str,
    as_of: Date,
    label: str,
) -> tuple[dict | None, Date | None]:
    """Read ``{prefix_template.format(date=d)}{filename}`` for ``as_of``,
    falling back to the most recent prior day (up to ``_LOOKBACK_DAYS``)
    if today's object doesn't exist yet.

    Returns ``(data, resolved_date)`` — ``(None, None)`` if nothing was
    found in the whole lookback window (fail-soft: caller treats this as
    an empty slice, not a hard error).
    """
    for days_back in range(0, _LOOKBACK_DAYS + 1):
        d = as_of - timedelta(days=days_back)
        key = f"{prefix_template.format(date=d.isoformat())}{filename}"
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())
            if days_back:
                logger.info(
                    "[corpus_scope] %s: using fallback date %s (today %s not yet present)",
                    label, d, as_of,
                )
            return data, d
        except Exception:
            continue
    logger.warning(
        "[corpus_scope] %s: no object found in s3://%s/%s for the last %d days",
        label, bucket, prefix_template.format(date=as_of.isoformat()), _LOOKBACK_DAYS,
    )
    return None, None


def _extract_tickers(entries: list, ticker_keys: tuple[str, ...] = ("ticker", "symbol")) -> set[str]:
    out: set[str] = set()
    for entry in entries or []:
        if isinstance(entry, dict):
            for k in ticker_keys:
                t = entry.get(k)
                if t:
                    out.add(str(t).strip().upper())
                    break
        elif isinstance(entry, str) and entry.strip():
            out.add(entry.strip().upper())
    return out


def load_holdings(bucket: str = DEFAULT_BUCKET, s3_client: Any = None) -> set[str]:
    """Metron's nightly held-ticker snapshot (fail-soft → empty set)."""
    s3 = _get_s3_client(s3_client)
    try:
        obj = s3.get_object(Bucket=bucket, Key=HOLDINGS_UNIVERSE_KEY)
        data = json.loads(obj["Body"].read())
        tickers = _extract_tickers(data.get("tickers", []))
        logger.info("[corpus_scope] holdings: %d tickers", len(tickers))
        return tickers
    except Exception as e:
        logger.warning("[corpus_scope] holdings unavailable (%s) — treating as empty", e)
        return set()


def _load_signals_json(
    bucket: str, s3_client: Any, as_of: Date, _signals_data: dict | None = None,
) -> dict | None:
    """Shared same-day signals.json fetch, with an injectable override.

    ``load_active_candidates`` (buy_candidates slice) and ``load_board_top_n``
    (universe/score ranking) both read the SAME S3 object; ``resolve_corpus_scope``
    fetches it ONCE and threads the parsed dict into both via ``_signals_data``
    so a single ``resolve_corpus_scope()`` call only ever does ONE
    signals.json GET, not two. Direct callers of either function (tests,
    ad-hoc scripts) omit ``_signals_data`` and get the original
    fetch-it-yourself behavior.
    """
    if _signals_data is not None:
        return _signals_data
    data, _ = _read_json_with_fallback(
        s3_client, bucket, "signals/{date}/", "signals.json", as_of, "signals_json",
    )
    return data


def load_active_candidates(
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    as_of: Date | None = None,
    _signals_data: dict | None = None,
) -> set[str]:
    """Active candidates = Scanner's same-day board ∪ research's buy_candidates.

    Both are "actionable now" sets from independent producers (Scanner
    Lambda vs. research Lambda) — a ticker flagged by either is in scope.
    Each slice fails soft independently so one producer's outage doesn't
    zero out the other's signal.

    ``_signals_data``: internal — lets ``resolve_corpus_scope`` pass an
    already-fetched signals.json dict instead of re-fetching it (see
    ``_load_signals_json``). Direct callers should leave this unset.
    """
    s3 = _get_s3_client(s3_client)
    as_of = as_of or Date.today()

    candidates: set[str] = set()

    scanner_data, _ = _read_json_with_fallback(
        s3, bucket, "candidates/{date}/", "candidates.json", as_of, "scanner_candidates",
    )
    if scanner_data is not None:
        if isinstance(scanner_data, list):
            entries = scanner_data
        else:
            entries = scanner_data.get("candidates") or []
        candidates |= _extract_tickers(entries)

    signals_data = _load_signals_json(bucket, s3, as_of, _signals_data)
    if signals_data:
        candidates |= _extract_tickers(signals_data.get("buy_candidates", []))

    logger.info("[corpus_scope] active candidates: %d tickers", len(candidates))
    return candidates


def load_board_top_n(
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    as_of: Date | None = None,
    top_n: int = BOARD_TOP_N,
    _signals_data: dict | None = None,
) -> set[str]:
    """Top-``top_n`` tickers in the same-day signals.json ``universe``,
    ranked by ``score`` descending (missing/non-numeric score sorts last,
    never crashes the ranking).

    ``_signals_data``: internal — see ``load_active_candidates``.
    """
    s3 = _get_s3_client(s3_client)
    as_of = as_of or Date.today()

    signals_data = _load_signals_json(bucket, s3, as_of, _signals_data)
    if not signals_data:
        return set()

    universe = signals_data.get("universe", [])
    if not isinstance(universe, list):
        return set()

    def _score(entry: Any) -> float:
        if not isinstance(entry, dict):
            return float("-inf")
        s = entry.get("score")
        try:
            return float(s)
        except (TypeError, ValueError):
            return float("-inf")

    ranked = sorted(universe, key=_score, reverse=True)
    top = _extract_tickers(ranked[:top_n])
    logger.info("[corpus_scope] board top-%d: %d tickers", top_n, len(top))
    return top


def resolve_corpus_scope(
    bucket: str = DEFAULT_BUCKET,
    s3_client: Any = None,
    as_of: Date | None = None,
    board_top_n: int = BOARD_TOP_N,
) -> set[str]:
    """Resolve the RAG corpus ticker scope: holdings ∪ active candidates ∪
    top-N signals board (config#2943 binding ruling, ≈100-150 effective
    tickers — the "Think Tank" tier).

    This is THE single shared scope resolver — every corpus ingestor
    (filings, 8-K, transcripts, theses, news, 13F) must call this instead
    of independently loading the full ``signals.json`` universe via
    ``--from-signals``. One resolver, one place.

    Each of the three slices fails soft independently (see
    ``load_holdings`` / ``load_active_candidates`` / ``load_board_top_n``),
    so a single missing artifact degrades the scope rather than aborting
    ingestion — an ingestor that runs against a partial scope on a bad day
    is preferable to one that hard-fails and skips the whole daily delta
    or Saturday top-up.

    Returns an empty set only if ALL THREE sources are unavailable — the
    caller should treat that as "abort, log loud" (mirrors
    ``_signals_universe.load_signals_tickers``'s empty-list convention)
    since ingesting nothing silently is worse than a visible failure.
    """
    s3 = _get_s3_client(s3_client)
    as_of = as_of or Date.today()

    # Fetch signals.json ONCE — load_active_candidates (buy_candidates) and
    # load_board_top_n (universe/score ranking) both read the same S3
    # object; without this, one resolve_corpus_scope() call would do TWO
    # signals.json GETs instead of one.
    signals_data = _load_signals_json(bucket, s3, as_of)

    holdings = load_holdings(bucket=bucket, s3_client=s3)
    candidates = load_active_candidates(bucket=bucket, s3_client=s3, as_of=as_of, _signals_data=signals_data)
    board = load_board_top_n(bucket=bucket, s3_client=s3, as_of=as_of, top_n=board_top_n, _signals_data=signals_data)

    scope = holdings | candidates | board
    logger.info(
        "[corpus_scope] resolved scope: %d holdings ∪ %d candidates ∪ %d board-top-%d = %d unique",
        len(holdings), len(candidates), len(board), board_top_n, len(scope),
    )
    return scope


# ── CLI ticker-resolution helper (shared by every ingestor's argparse) ──────

SCOPE_FLAG_VALUE = "holdings+candidates+board60"


def add_scope_arg(parser) -> None:
    """Attach the shared ``--scope`` flag to an ingestor's argparse parser.

    Replaces the old whole-universe ``--from-signals`` flag. Kept as a
    single literal value (rather than a free-form string) since there is
    exactly one ruling-defined scope today; a future second scope profile
    would add a new literal here, not a parallel flag.
    """
    parser.add_argument(
        "--scope",
        choices=[SCOPE_FLAG_VALUE],
        help=(
            "Resolve tickers via the shared corpus-scope resolver "
            "(holdings ∪ active candidates ∪ top-60 signals board — "
            "config#2943). Replaces the retired whole-universe "
            "--from-signals flag."
        ),
    )


def resolve_tickers_from_args(args, *, bucket: str | None = None, s3_client: Any = None) -> list[str]:
    """Shared ``--tickers`` / ``--scope`` resolution for ingestor CLIs.

    ``bucket`` defaults to ``args.bucket`` if the caller's parser defines
    that flag, else ``DEFAULT_BUCKET``. Returns a sorted list (stable,
    deterministic ordering across runs — helps diff-based debugging of
    which tickers a run actually covered).
    """
    if getattr(args, "tickers", None):
        return sorted({t.strip().upper() for t in args.tickers.split(",") if t.strip()})

    if getattr(args, "scope", None) == SCOPE_FLAG_VALUE:
        resolved_bucket = bucket if bucket is not None else getattr(args, "bucket", DEFAULT_BUCKET)
        scope = resolve_corpus_scope(bucket=resolved_bucket, s3_client=s3_client)
        return sorted(scope)

    return []
