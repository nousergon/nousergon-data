"""Per-source yield of one weekly RAG ingestion run, and the verdict over it.

alpha-engine-config-I11472. The 2026-09-23 rehearsal's RAGIngestion stage
finished ``COVERED 1/1`` and its completion email said ``ok`` for every
collector, while its own log said ``Total: 0 transcripts ingested for 118
tickers`` and ``Signals thesis ingestion: 0 theses``. Nothing distinguished "a
source that has nothing new this week" from "a source that returned nothing at
all" — both print a 0 and exit 0, and the earnings-transcript source had been
the second kind for as long as the corpus has existed (no
``earnings_transcript`` document has ever been stored).

HOW IT WORKS
------------
Each ingestion step is its own ``python -m`` process, so each one writes ONE
small JSON file describing what its source did — tickers asked, documents the
source OFFERED (``discovered``), documents stored, documents already held, and
named failure counts — into a run-scoped directory
(``$RAG_SOURCE_YIELD_DIR``). After the last step, ``--report`` reads every
file and renders one verdict:

* ``degraded`` — the source offered **zero** documents, and nothing
  declared that legitimate (an empty scope included: a run asked about
  nothing is not a clean run). This is the "whole source
  returned 0" condition. ``discovered`` is the measure rather than
  ``ingested``, because a filing source whose documents are all already held
  ingests 0 in an ordinary week.
* ``degraded`` — the source offered new documents but EVERY one of them
  failed (a Form 4 fetch that only ever yields unparseable pages is a source
  returning nothing, however many filings it listed).
* ``degraded`` — a source that was expected to report did not (its step never
  reached the write). Silence is not a clean result.
* ``expected_empty`` — the source offered zero documents AND declared why that
  is correct (``expected_empty`` carries the reason). Not a gap.
* ``ok`` — otherwise. Per-document failures (e.g. filings with no extractable
  sections) are carried in the record and the log, not in the status.

The verdict NEVER changes the exit code — the same honest-degradation shape
as ``assert_corpus_freshness`` (``weekly-sf-policy.md`` §2.3): a degraded
source degrades the run's report, it does not fail the weekly pipeline.
It is surfaced three ways: a ``WARNING`` line naming every degraded source,
the completion email's per-collector status (and a ``DEGRADED`` subject), and
— under the recorded entry point — a ``rag_source_yield`` guard on the D16 run
manifest.

Usage (called by ``run_weekly_ingestion.sh`` after the last step)::

    python -m rag.pipelines.source_yield --report
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

YIELD_DIR_ENV = "RAG_SOURCE_YIELD_DIR"
VERDICT_FILE = "verdict.json"

#: The sources ``run_weekly_ingestion.sh`` runs and expects a yield from. A
#: missing file for any of them is itself a degraded source.
EXPECTED_SOURCES: tuple[str, ...] = (
    "sec_filings",
    "8k_events",
    "earnings_transcripts",
    "thesis_history",
    "form4_insider",
)

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_EXPECTED_EMPTY = "expected_empty"


@dataclass
class SourceYield:
    """What one source did in one run.

    Args:
        source: The collector name, as the completion email and
            :data:`EXPECTED_SOURCES` spell it.
        scope: How many tickers (or inputs) the source was asked about.
        discovered: Documents the source OFFERED inside the window, whether or
            not they were new. Zero across a non-empty scope is the degraded
            condition.
        ingested: Documents newly stored this run.
        already_held: Documents skipped because the corpus already holds them.
        failures: Named per-document or per-request failure counts, e.g.
            ``{"no_sections": 3}`` or ``{"http_403": 118}``. Zero counts are
            dropped.
        expected_empty: Why zero discovered documents is CORRECT for this
            source this run. ``None`` (the default) means zero is a gap.
        detail: Free-text context for the log and the email.
    """

    source: str
    scope: int = 0
    discovered: int = 0
    ingested: int = 0
    already_held: int = 0
    failures: dict[str, int] = field(default_factory=dict)
    expected_empty: str | None = None
    detail: str = ""

    def fail(self, name: str, n: int = 1) -> None:
        self.failures[name] = self.failures.get(name, 0) + n


def yield_dir(directory: str | os.PathLike | None = None) -> Path:
    """The run-scoped directory yields are written to and read from.

    ``$RAG_SOURCE_YIELD_DIR`` when set (``run_weekly_ingestion.sh`` sets it per
    run), else a per-UTC-day directory under the system temp dir so an ad-hoc
    single-step run still has somewhere to write.
    """
    if directory is not None:
        return Path(directory)
    env = os.environ.get(YIELD_DIR_ENV)
    if env:
        return Path(env)
    day = datetime.now(timezone.utc).date().isoformat()
    return Path("/tmp") / "rag_source_yield" / day


def write_yield(y: SourceYield, directory: str | os.PathLike | None = None) -> Path | None:
    """Write one source's yield. Returns the path, or ``None`` if it could not.

    A failed write is logged, not raised: the ingestion itself succeeded, and
    the verdict reads a MISSING yield file as a degraded source — so a lost
    write surfaces as degraded rather than disappearing.
    """
    out_dir = yield_dir(directory)
    y.failures = {k: int(v) for k, v in y.failures.items() if int(v)}
    path = out_dir / f"{y.source}.json"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(y), sort_keys=True))
    except OSError as exc:
        logger.warning(
            "[rag_source_yield] could not write %s (%s) — the verdict will read "
            "source %r as unreported, i.e. DEGRADED", path, exc, y.source,
        )
        return None
    logger.info(
        "[rag_source_yield] %s: scope=%d discovered=%d ingested=%d already_held=%d "
        "failures=%s%s",
        y.source, y.scope, y.discovered, y.ingested, y.already_held, y.failures or "{}",
        f" expected_empty={y.expected_empty!r}" if y.expected_empty else "",
    )
    return path


def _source_status(y: dict[str, Any]) -> tuple[str, str]:
    """(status, reason) for one yield record."""
    scope = int(y.get("scope") or 0)
    discovered = int(y.get("discovered") or 0)
    failures = y.get("failures") or {}
    already_held = int(y.get("already_held") or 0)
    ingested = int(y.get("ingested") or 0)
    failed = sum(int(v) for v in failures.values())
    fresh = discovered - already_held
    if fresh > 0 and ingested == 0 and failed >= fresh:
        return STATUS_DEGRADED, (
            f"every one of the {fresh} new document(s) the source offered failed ("
            + ", ".join(f"{k}={v}" for k, v in sorted(failures.items())) + ")"
        )
    if discovered == 0:
        if y.get("expected_empty"):
            return STATUS_EXPECTED_EMPTY, f"0 documents, declared expected: {y['expected_empty']}"
        reason = (
            f"source returned 0 documents for {scope} input(s)"
            if scope
            else "source was asked about 0 inputs, so it returned nothing"
        )
        if failures:
            reason += " (" + ", ".join(f"{k}={v}" for k, v in sorted(failures.items())) + ")"
        if y.get("detail"):
            reason += f" — {y['detail']}"
        return STATUS_DEGRADED, reason
    return STATUS_OK, ""


def assess(
    directory: str | os.PathLike | None = None,
    expected_sources: tuple[str, ...] = EXPECTED_SOURCES,
) -> dict[str, Any]:
    """Read every yield in ``directory`` and render the run's verdict."""
    in_dir = yield_dir(directory)
    records: dict[str, dict[str, Any]] = {}
    if in_dir.is_dir():
        for path in sorted(in_dir.glob("*.json")):
            if path.name == VERDICT_FILE:
                continue
            try:
                rec = json.loads(path.read_text())
            except (OSError, ValueError) as exc:
                logger.warning("[rag_source_yield] unreadable yield %s (%s)", path, exc)
                continue
            records[str(rec.get("source") or path.stem)] = rec

    sources: dict[str, dict[str, Any]] = {}
    for name in sorted(set(expected_sources) | set(records)):
        rec = records.get(name)
        if rec is None:
            sources[name] = {
                "status": STATUS_DEGRADED,
                "reason": "no yield reported — the step did not record what its source returned",
            }
            continue
        status, reason = _source_status(rec)
        sources[name] = {**rec, "status": status, "reason": reason}

    degraded = [
        {"source": n, "reason": s["reason"]}
        for n, s in sources.items()
        if s["status"] == STATUS_DEGRADED
    ]
    return {
        "schema_version": 1,
        "assessed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": STATUS_DEGRADED if degraded else STATUS_OK,
        "degraded_sources": degraded,
        "sources": sources,
    }


def load_verdict(directory: str | os.PathLike | None = None) -> dict[str, Any] | None:
    """The verdict ``--report`` wrote for this run, or ``None`` if there is none."""
    path = yield_dir(directory) / VERDICT_FILE
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def email_collectors(
    base: dict[str, dict[str, Any]],
    verdict: dict[str, Any] | None,
) -> tuple[str, dict[str, dict[str, Any]]]:
    """Overlay the verdict onto the completion email's collector table.

    Returns ``(overall_status, collectors)``. With no verdict at all the run
    reports ``degraded`` — the report step not running is not evidence that
    every source yielded.
    """
    collectors = {k: dict(v) for k, v in base.items()}
    if verdict is None:
        return STATUS_DEGRADED, {
            **collectors,
            "source_yield": {"status": STATUS_DEGRADED, "error": "no source-yield verdict for this run"},
        }
    for name, info in (verdict.get("sources") or {}).items():
        status = info.get("status", STATUS_OK)
        entry: dict[str, Any] = {"status": status}
        if status == STATUS_DEGRADED and info.get("reason"):
            entry["error"] = info["reason"]
        collectors[name] = entry
    return verdict.get("status", STATUS_DEGRADED), collectors


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report", action="store_true", help="Assess every yield and write the verdict.")
    p.add_argument("--dir", default=None, help=f"Yield directory (default: ${YIELD_DIR_ENV}).")
    args = p.parse_args(argv)
    if not args.report:
        p.error("nothing to do: pass --report")

    verdict = assess(args.dir)
    if verdict["status"] == STATUS_DEGRADED:
        logger.warning(
            "[rag_source_yield] DEGRADED — %s",
            "; ".join(f"{d['source']}: {d['reason']}" for d in verdict["degraded_sources"]),
        )
    else:
        logger.info("[rag_source_yield] OK — every source returned documents or declared why not")
    for name, info in verdict["sources"].items():
        if info["status"] == STATUS_EXPECTED_EMPTY:
            logger.info("[rag_source_yield] %s: %s", name, info["reason"])

    out = yield_dir(args.dir) / VERDICT_FILE
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, sort_keys=True))
    except OSError as exc:
        logger.warning(
            "[rag_source_yield] could not write %s (%s) — the completion email will "
            "report the run degraded for want of a verdict", out, exc,
        )
    print(json.dumps(verdict, indent=2, sort_keys=True))
    # ALWAYS 0: a degraded source is a flag on the run, never a pipeline failure.
    return 0


if __name__ == "__main__":
    sys.exit(main())
