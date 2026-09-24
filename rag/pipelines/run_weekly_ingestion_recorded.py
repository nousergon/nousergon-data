"""D16 entry point — RAG weekly ingestion, wrapped in a `data_run_manifest.v1`.

`alpha-engine-config-I10862`. D16's descriptor
(``registry.d/units/D16-rag-weekly-ingestion.yaml``) declares
``run_manifest_prefix: data_collection/runs/D16``, but its pipeline —
``run_weekly_ingestion.sh``, run bash-driven as the ``rag-weekly-ingestion``
data-spot-dispatcher workload — wrote no manifest at all. That made the
``run_record`` data_gate clause unmeasurable for D16, failed the weekly
machine's ``verify_units`` completion check on every execution that names it
(PR1725), and made a failed or partial ingestion indistinguishable from a unit
that never ran.

**Also covers D46 (`alpha-engine-config-I10753`).** D46 (insider_transactions,
Form 4) is step 6 of this SAME script and gets no dispatcher key of its own —
a second entry point would re-run the identical EDGAR fetch a second time per
week (see `index.py::_WORKLOADS["rag-weekly-ingestion"]`'s comment). Rather
than leave D46 with no run record at all, its descriptor's
``run_manifest_prefix`` points at THIS manifest (`data_collection/runs/D16`,
not its own `D16`-shaped prefix under `D46`), and this module's
``OUTPUT_PREFIXES`` includes D46's declared write prefix
(`data/insider_transactions/`) so D46's keys land in the one manifest this
process already writes. The dispatcher's completion check reads
`run_manifest_prefix` from each unit's own descriptor (`index.py::_check_unit`),
so naming "D46" in a schedule's `verify_units` grades those same keys against
D46's own `writes:` floor — no second manifest, no second script run.

This module is a THIN wrapper (`data_collection_plan_260914.md` §4.4;
``nousergon-data`` AGENTS.md's "wrap, don't reimplement" precedent): it runs
the existing bash script completely unchanged — same nine ingestion steps,
same venv/PYTHON_BIN resolution, same LM-dict bootstrap, same
``--dry-run``/``--preflight-only`` flags — and adds exactly one thing around
it, a ``run_units.recorded_entry("D16", ...)`` call. It never reimplements the
pipeline and never parses the script's stdout/logs for what it published:
after the subprocess exits, it reads back the real S3 objects each ingestion
step already writes (``emit_manifest.py``, ``filing_change_detection.py``,
``assert_corpus_freshness.py``, the per-source watermark stores,
``emit_progress.py``) and records each with the row count *that object itself
carries* — the same "declare the key, never guess it" discipline
``run_units.PhaseUnit.rows_key`` enforces one level down.

**The manifest's ``trading_day`` and the script's dated keys are ONE value.**
The dispatcher's completion check (`index.py::_key_pattern`) substitutes
``{date}``/``{trading_day}`` in ``writes:`` templates from the MANIFEST's own
``trading_day`` field, so a manifest whose ``trading_day`` differs from the
date embedded in the keys the run wrote silently fails every
``rag/manifest/{date}.json`` match. Since alpha-engine-config-I11514 this
wrapper passes its ``trading_day`` to ``run_weekly_ingestion.sh --run-date``,
which keys ``rag/manifest/{date}.json``, ``rag/filing_changes/{date}.json`` and
``health/rag_ingestion_progress/{date}.json`` by it — they can no longer drift
apart, even across midnight UTC. (Before I11514 each step read the wall clock
itself and this wrapper had to guess the same value.)

**``--date`` defaults to the cycle date, never the UTC calendar day**
(alpha-engine-config-I11475, Brian's ruling of 2026-09-24: the whole D16 key
family is keyed by the SF cycle date, the same value v1's RAGIngestion passes
from ``$.run_date`` since I11514). The data-collection weekly schedule's input
is static and cannot carry a date, so the default is `dates.default_run_date()`
— the last closed NYSE session, which is what the v1 SF's ``$.run_date``
resolves to for the Saturday run (Friday), what `run_units.run_phase` keys
every other unit in the same weekly execution by, and what the dispatcher's own
run-log partition (`index.py::_run_log_trading_day`) resolves to. Before this,
the default was today's UTC date, so the v2 path filed Saturday's run under
Saturday while v1 filed the same cycle under Friday.

Replaces ``bash rag/pipelines/run_weekly_ingestion.sh`` as the
``rag-weekly-ingestion`` workload command in
``infrastructure/lambdas/data-spot-dispatcher/index.py::_WORKLOADS``.

Usage::

    python -m rag.pipelines.run_weekly_ingestion_recorded
    python -m rag.pipelines.run_weekly_ingestion_recorded --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import run_units
from rag.pipelines import source_yield
from validators import expectations

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh"
BUCKET = "alpha-engine-research"

#: Every S3 prefix D16's descriptor declares under `writes:` (all six entries
#: — the three `rag/manifest*`/`rag/filing_changes*` literals collapse to
#: their shared parent prefix; `rag/watermarks/` and `rag/corpus_freshness/`
#: are already prefixes; `health/rag_ingestion_progress/{date}.json` gets its
#: own), PLUS D46's declared write prefix (`data/insider_transactions/`,
#: alpha-engine-config-I10753 — step 6 of this same script, no separate
#: manifest). Declared here, not derived from either descriptor at runtime,
#: for the same reason `run_units.PHASE_UNITS` is a literal table: a renamed
#: prefix on either side is a loud test failure, never a silent miss.
OUTPUT_PREFIXES: tuple[str, ...] = (
    "rag/manifest/",
    "rag/watermarks/v1/",
    "rag/filing_changes/",
    "rag/corpus_freshness/",
    "health/rag_ingestion_progress/",
    "data/insider_transactions/",
)


def _ingestion_argv(dry_run: bool, run_date: str) -> list[str]:
    """The exact argv the bash pipeline is run with.

    ``--run-date`` is always passed (alpha-engine-config-I11514) so every dated
    key the script writes carries this manifest's ``trading_day``.
    """
    argv = ["bash", str(SCRIPT_PATH), "--run-date", run_date]
    if dry_run:
        argv.append("--dry-run")
    return argv


def _run_ingestion_script(dry_run: bool, run_date: str, yield_dir: str | None = None) -> int:
    """Run the existing bash pipeline unchanged. Returns its exit code.

    A subprocess call, not a reimplementation: everything about HOW the nine
    steps run stays exactly what `run_weekly_ingestion.sh` already does. The
    one thing passed IN is where the script's per-source yields go
    (``$RAG_SOURCE_YIELD_DIR``), so this process can read the verdict back.
    """
    argv = _ingestion_argv(dry_run, run_date)
    env = None
    if yield_dir is not None:
        env = {**os.environ, source_yield.YIELD_DIR_ENV: yield_dir}
    proc = subprocess.run(argv, cwd=str(REPO_ROOT), env=env)  # noqa: S603 -- fixed argv, no shell
    return proc.returncode


#: The guard name the source-yield verdict is recorded under
#: (alpha-engine-config-I11472).
SOURCE_YIELD_GUARD = "rag_source_yield"


def _record_source_yield(ctx: run_units.run_manifest.UnitRun, yield_dir: str) -> None:
    """Record the script's source-yield verdict as a guard on the manifest.

    Recorded on every run, clean or not — a guard that records only when it
    fires is indistinguishable from one that stopped running. OBSERVE mode:
    a degraded source is reported, never a failed run (``source_yield``
    module docstring).
    """
    verdict = source_yield.load_verdict(yield_dir)
    if verdict is None:
        ctx.record_guard(
            SOURCE_YIELD_GUARD,
            mode="observe",
            verdict="unmeasurable",
            detail=f"no source-yield verdict under {yield_dir}",
        )
        return
    degraded = verdict.get("degraded_sources") or []
    ctx.record_guard(
        SOURCE_YIELD_GUARD,
        mode="observe",
        verdict=verdict.get("status", "unmeasurable"),
        detail=(
            "; ".join(f"{d['source']}: {d['reason']}" for d in degraded)
            or "every source returned documents or declared why not"
        ),
        value=float(len(degraded)),
    )


def _utcnow() -> datetime:
    """Wall clock, factored out so tests can pin the "since" boundary
    `_record_outputs` lists against without racing the real clock."""
    return datetime.now(timezone.utc)


def _s3_client() -> Any:
    import boto3  # local import: keeps this module importable without boto3 in unit tests

    return boto3.client("s3")


def _get_json(s3: Any, key: str) -> Any:
    """Read one object back as JSON, or None — a presence/content probe, never
    load-bearing for the pipeline's own success/failure."""
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as exc:  # noqa: BLE001 -- read-back probe, not the pipeline's own write
        logger.warning("D16: could not read s3://%s/%s back (%s)", BUCKET, key, exc)
        return None
    try:
        return json.loads(body)
    except (TypeError, ValueError):
        return None


def _rows_out_for(doc: Any, key: str) -> int:
    """The MEASURED row count for one published object — never a guess.

    * ``rag/manifest/*.json`` (``emit_manifest.build_manifest``) carries
      ``totals.documents``.
    * ``rag/filing_changes/*.json`` (``filing_change_detection.main``) carries
      ``n_analyzed``.
    * A per-source watermark object (``_watermarks.RagWatermarkStore``) is a
      flat ``{"ticker::doc_type": iso_ts}`` map — its own length IS the count
      of tickers advanced.
    * ``rag/corpus_freshness/latest.json`` (``assert_corpus_freshness``) and
      ``health/rag_ingestion_progress/*.json`` (``emit_progress``) are
      singleton status objects with no row concept: one object landing is the
      fact being recorded, so ``1``.
    """
    if isinstance(doc, dict):
        totals = doc.get("totals")
        if isinstance(totals, dict) and "documents" in totals:
            return int(totals["documents"])
        if "n_analyzed" in doc:
            return int(doc["n_analyzed"])
        if key.startswith("rag/watermarks/v1/"):
            return len(doc)
    return 1


def _iter_new_keys(s3: Any, prefix: str, since: datetime) -> list[str]:
    """Every key under ``prefix`` last modified at or after ``since``."""
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        for obj in page.get("Contents") or []:
            if obj["LastModified"] >= since:
                keys.append(obj["Key"])
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return keys


def _record_outputs(ctx: run_units.run_manifest.UnitRun, s3: Any, since: datetime) -> int:
    """Record every object this run actually published, with a measured count.

    Reads the artifacts each ingestion step already wrote — never the run's
    logs. Returns how many keys were found, so the caller can tell a
    genuinely silent run from a script that exited 0 and wrote nothing.
    """
    published = 0
    for prefix in OUTPUT_PREFIXES:
        for key in _iter_new_keys(s3, prefix, since):
            doc = _get_json(s3, key)
            ctx.record_output(key, rows_out=_rows_out_for(doc, key))
            published += 1
    return published


def _body(ctx: run_units.run_manifest.UnitRun, *, dry_run: bool, run_date: str) -> dict[str, Any]:
    since = _utcnow()
    yield_dir = str(source_yield.yield_dir(None) / f"d16-{since.strftime('%Y%m%dT%H%M%SZ')}")
    exit_code = _run_ingestion_script(dry_run, run_date, yield_dir=yield_dir)
    published = 0 if dry_run else _record_outputs(ctx, _s3_client(), since)
    _record_source_yield(ctx, yield_dir)

    if published:
        ctx.record_guard(
            expectations.EMPTY_FRESH_GUARD.name,
            mode=expectations.EMPTY_FRESH_GUARD.mode.value,
            verdict="ok",
            detail=f"D16 published {published} key(s) under {', '.join(OUTPUT_PREFIXES)}",
            value=float(ctx.rows_out),
        )
    else:
        ctx.record_guard(
            expectations.EMPTY_FRESH_GUARD.name,
            mode=expectations.EMPTY_FRESH_GUARD.mode.value,
            verdict="not_applicable" if dry_run else "empty_fresh",
            detail=(
                "dry run: no writes expected"
                if dry_run
                else f"run_weekly_ingestion.sh exited {exit_code} and published NO key "
                f"under any declared D16 prefix"
            ),
        )

    result = {"status": "ok", "exit_code": exit_code, "published": published}

    if exit_code != 0:
        result["status"] = "failed"
        raise run_units.EntryRunFailed(
            f"run_weekly_ingestion.sh exited {exit_code}", value=result
        )
    if not dry_run and published == 0:
        result["status"] = "failed"
        raise run_units.EntryRunFailed(
            "run_weekly_ingestion.sh exited 0 but published no output under any "
            "declared D16 prefix — a silent no-write success",
            value=result,
        )
    return result


def _default_cycle_date() -> str:
    """The cycle date a scheduled run with no ``--date`` keys D16 by.

    ``dates.default_run_date()`` (alpha-engine-config-I11475): the last closed
    NYSE session, so a Saturday run is filed under Friday's cycle exactly as
    v1's ``$.run_date`` files it. Imported here, not at module top, for the same
    reason `run_units.run_phase` does: it keeps ``dates`` off the import path of
    callers that always pass an explicit date.
    """
    from dates import default_run_date

    return default_run_date()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="The manifest's trading_day, also passed to run_weekly_ingestion.sh as "
        "--run-date so every dated key it writes carries the same value "
        "(alpha-engine-config-I11514). Default: the cycle date, "
        "dates.default_run_date() — the last closed NYSE session, never the UTC "
        "calendar day (alpha-engine-config-I11475).",
    )
    args = parser.parse_args(argv)

    trading_day = args.date or _default_cycle_date()

    def _entry(ctx: run_units.run_manifest.UnitRun) -> dict[str, Any]:
        return _body(ctx, dry_run=args.dry_run, run_date=trading_day)

    result = run_units.recorded_entry(
        "D16",
        _entry,
        trigger="scheduled",
        trading_day=trading_day,
        write=not args.dry_run,
    )
    logger.info("[D16 rag_weekly_ingestion] complete: %s", result)
    return 0 if isinstance(result, dict) and result.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
