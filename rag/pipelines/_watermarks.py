"""Per-source ingestion watermarks — fetch the gap, not the window.

``rag-corpus-policy.md`` §2.2: *the expensive call is gated on what is already
held*, and the window is **since the last successful ingest**, not a fixed
rolling lookback.

WHAT THIS FIXES
---------------
``rag/pipelines/run_news_pipeline.py`` fetched a flat ``--hours 168`` for every
ticker in scope, every run, unconditionally. Dedup happened afterwards, in
``ingest_news`` — which saves the embedding call and saves nothing else, because
the vendor request has already been paid for and vendor requests are the binding
constraint (Polygon: 5 req/min, account-wide, ~12.5 s/ticker).

The consequence is ``rag-corpus-policy.md`` §4's honest health check inverted:
*the marginal cost of a no-new-documents cycle should be ≈ zero requests.* It
was the full cycle cost, so the steady-state spend measured nothing about what
had actually changed upstream.

WHAT COSTS, AND THEREFORE WHAT THIS GATES
-----------------------------------------
**Ticker count, not window width.** A ticker costs ~12.5 s whether we ask it for
1 hour or 168; the per-request window is free. So the gate that matters is
*should this ticker be fetched at all*, and the window for those that are is
sized to the OLDEST outstanding watermark among them. Deliberately not per-ticker
windows: the aggregator takes one ``hours`` for the batch, and bucketing tickers
into window groups would trade a free dimension for extra round trips.

CONTRACT
--------
* Key: ``(source, ticker, doc_type)``. One S3 object per source, so two
  pipelines never contend on a write — each pipeline owns exactly one source.
* **Advanced only on a CONFIRMED successful ingest.** Advancing on fetch turns a
  transient store failure into a permanent, silent hole — the same defect class
  this module removes, inverted and made worse.
* A missing watermark means "never ingested" and yields the caller's full
  first-fill window. That is a gap-fill, explicitly permitted by §6, and is
  bounded by how fast the decision set churns.
* Unreadable store ⇒ **treat every ticker as outstanding** and log loudly. The
  safe direction is to over-fetch once, never to skip a fetch on the strength of
  state we could not read.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
WATERMARK_KEY_TPL = "rag/watermarks/v1/{source}.json"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse(ts: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class WatermarkStore:
    """Load-modify-flush store for one source's ingestion watermarks.

    Read once at the start of a run, advanced in memory as documents are
    confirmed stored, flushed once at the end — one GET and one PUT per run
    rather than per document.
    """

    def __init__(
        self,
        source: str,
        *,
        bucket: str = DEFAULT_BUCKET,
        s3_client: Any = None,
    ) -> None:
        self.source = source
        self.bucket = bucket
        self.key = WATERMARK_KEY_TPL.format(source=source)
        self._s3 = s3_client
        self._marks: dict[str, str] = {}
        self._loaded = False
        # Set when the store could not be READ (as opposed to legitimately
        # absent). Callers must treat every ticker as outstanding — see
        # module docstring.
        self.unreadable = False

    # ── internals ────────────────────────────────────────────────────────
    def _client(self):
        if self._s3 is None:
            import boto3
            self._s3 = boto3.client("s3")
        return self._s3

    @staticmethod
    def _k(ticker: str, doc_type: str) -> str:
        return f"{ticker.strip().upper()}|{doc_type}"

    # ── api ──────────────────────────────────────────────────────────────
    def load(self) -> dict[str, str]:
        if self._loaded:
            return self._marks
        self._loaded = True
        try:
            obj = self._client().get_object(Bucket=self.bucket, Key=self.key)
        except Exception as e:
            # A genuinely absent store is the first run for this source and is
            # NOT a degradation — everything is outstanding either way, which
            # is exactly what an empty dict produces.
            if "NoSuchKey" in str(e) or "404" in str(e):
                logger.info(
                    "[watermarks] %s: no store at s3://%s/%s yet — first fill",
                    self.source, self.bucket, self.key,
                )
            else:
                self.unreadable = True
                logger.warning(
                    "[watermarks] %s: store s3://%s/%s UNREADABLE (%s) — every "
                    "ticker treated as outstanding this run. Over-fetching once "
                    "is the safe direction; skipping a fetch on state we could "
                    "not read is not.",
                    self.source, self.bucket, self.key, e,
                )
            self._marks = {}
            return self._marks
        try:
            data = json.loads(obj["Body"].read())
            self._marks = dict(data.get("marks") or {})
        except Exception as e:
            self.unreadable = True
            logger.warning(
                "[watermarks] %s: store unparseable (%s) — every ticker treated "
                "as outstanding this run.", self.source, e,
            )
            self._marks = {}
        return self._marks

    def get(self, ticker: str, doc_type: str) -> datetime | None:
        """Last CONFIRMED ingest for this (ticker, doc_type), or None."""
        if self.unreadable:
            return None
        raw = self.load().get(self._k(ticker, doc_type))
        return _parse(raw) if raw else None

    def advance(self, ticker: str, doc_type: str, when: datetime | None = None) -> None:
        """Record a CONFIRMED successful ingest. In memory until :meth:`flush`.

        Never call this from a fetch path — only after the document is stored.
        """
        self.load()
        ts = (when or _utcnow()).astimezone(timezone.utc)
        self._marks[self._k(ticker, doc_type)] = ts.isoformat().replace("+00:00", "Z")

    def flush(self) -> bool:
        """Persist. Returns False (logged) rather than raising: losing a
        watermark write costs one extra fetch next run, while failing the whole
        ingestion would discard documents already stored."""
        if not self._loaded:
            return True
        payload = {
            "schema_version": 1,
            "source": self.source,
            "updated_at": _utcnow().isoformat().replace("+00:00", "Z"),
            "marks": self._marks,
        }
        try:
            self._client().put_object(
                Bucket=self.bucket, Key=self.key,
                Body=json.dumps(payload).encode(),
                ContentType="application/json",
            )
            logger.info("[watermarks] %s: flushed %d mark(s)",
                        self.source, len(self._marks))
            return True
        except Exception as e:
            logger.warning(
                "[watermarks] %s: flush FAILED (%s) — the documents ingested "
                "this run are stored; only the marks were lost, costing one "
                "redundant fetch next run.", self.source, e,
            )
            return False


def resolve_outstanding(
    tickers: list[str],
    store: WatermarkStore,
    *,
    doc_type: str,
    max_hours: int,
    min_hours: int = 1,
    now: datetime | None = None,
) -> tuple[list[str], int]:
    """Split the scope into what still needs fetching, and how far back to ask.

    Returns ``(outstanding_tickers, hours)``.

    A ticker whose watermark is newer than ``min_hours`` contributes **zero
    vendor requests**. ``hours`` is sized to the OLDEST outstanding watermark
    (clamped to ``max_hours``) so one batch covers every gap — see the module
    docstring for why the window is not per-ticker.

    ``([], 0)`` means the corpus is current: the caller must issue no request
    at all, not a request for zero hours.
    """
    now = now or _utcnow()
    outstanding: list[str] = []
    oldest: datetime | None = None
    never_ingested = False

    for t in tickers:
        mark = store.get(t, doc_type)
        if mark is None:
            outstanding.append(t)
            never_ingested = True
            continue
        if (now - mark).total_seconds() / 3600.0 < min_hours:
            continue
        outstanding.append(t)
        if oldest is None or mark < oldest:
            oldest = mark

    if not outstanding:
        return [], 0

    # Any never-ingested ticker forces the full first-fill window — it has no
    # history to bound. Otherwise size to the oldest mark plus a 10% overlap so
    # an article landing on a run boundary cannot fall between two runs.
    if never_ingested or oldest is None:
        return outstanding, max_hours

    span_h = (now - oldest).total_seconds() / 3600.0
    hours = min(max_hours, max(min_hours, int(span_h * 1.1) + 1))
    return outstanding, hours
