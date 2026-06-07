#!/usr/bin/env python3
"""One-time backfill: re-classify historical SNS-mirror changelog entries.

The ``changelog-incident-mirror`` Lambda used to hard-code every SNS message to
``incident`` / ``high`` / ``infrastructure_failure``. The go-forward fix routes
new messages through ``_shared/classify.py``; this script applies the SAME
classifier to the entries already written to S3 so the corpus (and the console
Retros page that mines it) reflects the corrected types instead of carrying
months of mislabeled SUCCESS/OK noise.

Scope: only entries with ``source == "sns-mirror"`` are touched — CI-deploy,
cloudwatch-mirror (errors by construction), manual, and changelog-log entries
are left exactly as-is. An entry is rewritten only if at least one of
``event_type`` / ``severity`` / ``subsystem`` / ``root_cause_category`` changes;
unchanged entries are skipped (idempotent — safe to re-run). Each rewrite stamps
``reclassified_at`` + ``reclassified_from`` for provenance.

Safe by default: DRY-RUN unless ``--apply`` is passed. Self-contained on stdlib
+ the ``aws`` CLI (no boto3), matching the aggregator scripts.

  python3 reclassify_history.py                 # preview (dry-run)
  python3 reclassify_history.py --apply         # rewrite changed entries
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "_shared"))
from classify import classify_sns  # noqa: E402

DEFAULT_BUCKET = "alpha-engine-research"
ENTRIES_PREFIX = "changelog/entries"
_CLASSIFIED_FIELDS = ("event_type", "severity", "subsystem", "root_cause_category")


def _aws_sync_down(bucket: str, prefix: str, dest: Path) -> None:
    proc = subprocess.run(
        ["aws", "s3", "sync", f"s3://{bucket}/{prefix}/", str(dest), "--quiet"],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"ERROR: aws s3 sync failed (exit {proc.returncode})")


def _aws_put(bucket: str, key: str, body: bytes) -> None:
    proc = subprocess.run(
        ["aws", "s3", "cp", "-", f"s3://{bucket}/{key}", "--content-type", "application/json"],
        input=body, capture_output=True, check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode("utf-8", errors="replace"))
        raise SystemExit(f"ERROR: aws s3 cp (write) failed for {key}")


def _subject_of(entry: dict) -> str:
    """The text the original Lambda classified from: the SNS subject, falling
    back to the entry summary (which itself fell back to the message)."""
    return (entry.get("sns") or {}).get("subject") or entry.get("summary") or ""


def reclassify_entry(entry: dict) -> dict | None:
    """Return an updated copy if classification changes, else None."""
    subject = _subject_of(entry)
    message = entry.get("description") or ""
    event_type, severity, subsystem, rcc = classify_sns(subject, message)
    new = {
        "event_type": event_type,
        "severity": severity,
        "subsystem": subsystem,
        "root_cause_category": rcc,
    }
    if all(entry.get(k) == new[k] for k in _CLASSIFIED_FIELDS):
        return None
    updated = dict(entry)
    updated["reclassified_from"] = {k: entry.get(k) for k in _CLASSIFIED_FIELDS}
    updated["reclassified_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated.update(new)
    return updated


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=ENTRIES_PREFIX)
    parser.add_argument("--apply", action="store_true",
                        help="Write changes back to S3 (default: dry-run preview).")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap entries written (0 = no cap). Dry-run still scans all.")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmp:
        corpus = Path(tmp)
        _aws_sync_down(args.bucket, args.prefix, corpus)

        scanned = sns = changed = written = 0
        transitions: Counter = Counter()
        for path in sorted(corpus.glob("**/*.json")):
            scanned += 1
            try:
                entry = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if entry.get("source") != "sns-mirror":
                continue
            sns += 1
            updated = reclassify_entry(entry)
            if updated is None:
                continue
            changed += 1
            old = (entry.get("event_type"), entry.get("severity"))
            new = (updated["event_type"], updated["severity"])
            transitions[f"{old} -> {new}"] += 1
            if args.apply and (args.limit == 0 or written < args.limit):
                # Reconstruct the S3 key from the local path (relative to corpus).
                key = f"{args.prefix}/{path.relative_to(corpus).as_posix()}"
                _aws_put(args.bucket, key, json.dumps(updated).encode("utf-8"))
                written += 1

        print(f"Scanned {scanned} entries; {sns} sns-mirror; {changed} need reclassification.")
        print("Transitions (old (type,sev) -> new):")
        for k, n in transitions.most_common():
            print(f"  {n:4d}  {k}")
        if args.apply:
            print(f"APPLIED: rewrote {written} entries to s3://{args.bucket}/{args.prefix}/")
        else:
            print("DRY-RUN: no writes. Re-run with --apply to rewrite.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
