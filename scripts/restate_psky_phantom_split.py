#!/usr/bin/env python3
"""Restate the five PSKY rows that Polygon's phantom 2:1 split halved.

Tracker: alpha-engine-config-I12359. Code fix: nousergon-data#2192 (refuse a
vendor split restatement with no price evidence). Ruling: Brian approved
2026-10-18 03:16Z ("yes 1-6", red-line item 6 — restate the 5 PSKY rows, with
an audit trail).

**What happened.** Polygon's splits feed carries a 2:1 PSKY split dated
2026-10-05 that the trade tape does not show. From the 2026-10-05 11:5xZ
morning run onward, the ``polygon_only`` window overwrote PSKY in
``staging/daily_closes/{date}.parquet`` with Polygon's split-adjusted bars:
exactly x0.5 price and x2 volume. The repo's own price-evidence gate
(``corporate_actions.price_evidence_orientation``) calls the split
unevidenced. #2192 stops the overwrite. This script corrects the rows that
were already written.

**What it does, per file** (``staging/daily_closes/{date}.parquet`` for the
five dates in ``TARGETS``):

1. Reads every S3 version of the file (the bucket is versioned) and the PSKY
   row in each.
2. Finds the single halving transition: the first version whose PSKY row is
   x0.5 Open/High/Low/Close and x2 Volume against the version before it. The
   version before it is the **restore source**, the last polygon write on the
   true basis. That source is cross-checked against the file's first write
   (yfinance, rev 1).
3. Refuses unless every later version, including the current one, still
   carries exactly the halved row, and the current Close is the value
   measured on 2026-10-18 (``TARGETS``). A row already back on the true basis
   is reported ``ALREADY-CORRECT`` and skipped.
4. Builds the new object: the CURRENT object with only the PSKY row replaced
   by the restore source's row. ``revision`` becomes current+1, the same rule
   the collector's coalesce uses. ``xsource_provenance`` says it was restated
   and from which version. Every other ticker's row is left byte-for-byte as
   it is now, and the parquet schema must match.
5. With ``--apply``: ``put_object`` conditional on the ETag it read
   (``IfMatch``), so a pipeline write that lands in between is never lost.
   The restatement id and source version go on the object as metadata. It
   then reads back the exact new version and fails unless PSKY equals the
   planned row and every other row equals the pre-write object.

At the end it writes ONE provenance record, under a prefix that is not
lifecycle-expired, plus a local copy:
``s3://alpha-engine-research/data_collection/restatements/{run_date}/{restatement_id}.json``.
The record carries: what changed, why, the ruling, the PR links, the operator
ARN, and per row the before values (current version id), the after values,
the restore-source version id, the first halved version id and the new
version id. It is also written when a run stops part-way, with
``status: partial``.

**Gates.** ``--apply`` refuses unless nousergon-data#2192 is MERGED. Merged is
checked with ``gh api`` when ``gh`` is available, otherwise by
``origin/main:corporate_actions/registry.py`` defining
``classify_discrepancy``, the method #2192 adds. Without the fix, the next
morning window could re-halve the rows. A dry run (the default) is
read-only, prints the full before/after plan, and reports the gate.

**Reversible.** The bucket is versioned. The record names each pre-write
version id, so ``aws s3api copy-object --copy-source
'alpha-engine-research/<key>?versionId=<before_version_id>'`` puts any file
back.

**Lifetime.** ``staging/`` expires 7 days after an object's last write
(bucket rule ``expire-staging-after-7-days``, mirrored in
``infrastructure/s3_lifecycle_staging.json``). The pipeline stops rewriting a
date after ~10 trading days. So these files age out at about 00:00 UTC on
10-20 (09-28), 10-21, 10-22, 10-23 and 10-24 (10-02). A file already gone is
reported ``AGED-OUT`` (moot). Writing a restated version restarts that file's
7-day clock.

Usage (operator, admin profile)::

    AWS_PROFILE=ne-admin AWS_REGION=us-east-1 python scripts/restate_psky_phantom_split.py           # dry run
    AWS_PROFILE=ne-admin AWS_REGION=us-east-1 python scripts/restate_psky_phantom_split.py --apply

Exit codes: 0 = plan OK (dry run) / applied and verified / nothing to do;
1 = refused (unexpected state; nothing written); 2 = a post-write read-back
failed; 3 = dry run planned cleanly but ``--apply`` would be refused by the
#2192 gate.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import math
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BUCKET = "alpha-engine-research"
PREFIX = "staging/daily_closes/"
TICKER = "PSKY"
TRACKER = "alpha-engine-config-I12359"
FIX_PR = "https://github.com/nousergon/nousergon-data/pull/2192"
FIX_PR_NUMBER = 2192
FIX_MARKER = "def classify_discrepancy("
RULING = 'Brian approved 2026-10-18 03:16Z ("yes 1-6"), red-line item 6: restate the 5 PSKY rows, with an audit trail'
REASON = (
    "Polygon's splits feed carries a PSKY 2:1 split dated 2026-10-05 that the trade "
    "tape does not show (10-02 close 9.50 -> 10-05 open 9.26 / close 9.78; Polygon's own "
    "10-05 bar 9.775). The polygon_only morning window overwrote PSKY from 2026-10-05 "
    "onward with x0.5 price / x2 volume. corporate_actions.price_evidence_orientation "
    "finds no evidence for the split; nousergon-data#2192 refuses such overwrites."
)
RECORD_PREFIX = "data_collection/restatements/"
LIFECYCLE_DAYS = 7

#: date -> the halved Close measured read-only on 2026-10-18 (decision sheet,
#: item 6). The current object must still carry exactly this value, or the
#: run refuses: the store moved since it was measured, and a human looks again.
TARGETS: dict[str, float] = {
    "2026-09-28": 5.14,
    "2026-09-29": 4.995,
    "2026-09-30": 5.165,
    "2026-10-01": 4.67,
    "2026-10-02": 4.75,
}

PRICE_COLS = ("Open", "High", "Low", "Close")
HALF_TOL = 0.0005        # |ratio - 0.5| for prices; |ratio - 2| / 2 for volume
YF_XCHECK_TOL = 0.02     # restore-source Close vs the file's first write (yfinance)


class Refused(Exception):
    """Unexpected state: nothing is written."""


# ── pure helpers ─────────────────────────────────────────────────────────────

def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def is_halved(after: dict, before: dict) -> bool:
    """True when ``after`` is exactly ``before`` at x0.5 price and x2 volume."""
    for c in PRICE_COLS:
        a, b = _num(after.get(c)), _num(before.get(c))
        if a is None or b is None or b == 0 or abs(a / b - 0.5) > HALF_TOL:
            return False
    va, vb = _num(after.get("Volume")), _num(before.get("Volume"))
    if va is None or vb is None or vb == 0:
        return False
    return abs(va / vb - 2.0) / 2.0 <= HALF_TOL


def same_bar(a: dict, b: dict) -> bool:
    """Same OHLCV (the comparison that decides 'already correct')."""
    for c in (*PRICE_COLS, "Volume"):
        x, y = _num(a.get(c)), _num(b.get(c))
        if x is None or y is None or abs(x - y) > 1e-9 * max(1.0, abs(y)):
            return False
    return True


def find_restore_source(history: list[dict]) -> tuple[int, int, bool]:
    """Given the PSKY row of each version, oldest first, return
    ``(restore_source_index, first_halved_index, already_restated)``.

    Exactly one halving transition is allowed. After it, the versions must be
    a run of the halved bar, optionally followed by versions that carry the
    restore-source bar again (an earlier run of this script). In that case
    ``already_restated`` is True. Any other shape means the store moved in a
    way this script was not written for, and it refuses.
    """
    transitions = [i for i in range(1, len(history)) if is_halved(history[i], history[i - 1])]
    if len(transitions) != 1:
        raise Refused(f"expected exactly one x0.5/x2 transition, found {len(transitions)} at {transitions}")
    first = transitions[0]
    src = history[first - 1]
    j = first
    while j < len(history) and is_halved(history[j], src):
        j += 1
    rest = history[j:]
    if not all(same_bar(h, src) for h in rest):
        raise Refused(f"version #{j} is neither the halved bar nor the restore-source bar; "
                      "later change not understood")
    return first - 1, first, bool(rest)


def restated_row(current_row: dict, source_row: dict, *, source_version_id: str,
                 restatement_id: str, run_date: str) -> dict:
    """The row to write: the source row's bar under the current row's lineage."""
    row = dict(source_row)
    row["revision"] = int(current_row["revision"]) + 1
    prov = source_row.get("xsource_provenance") or ""
    row["xsource_provenance"] = (
        f"{prov} | RESTATED {run_date} {restatement_id}: phantom 2:1 split undone, "
        f"bar restored from versionId={source_version_id} ({TRACKER})"
    ).strip(" |")
    return row


# ── S3 / parquet ─────────────────────────────────────────────────────────────

def _read_frame(s3, key: str, version_id: str | None = None):
    import pandas as pd

    kw = {"Bucket": BUCKET, "Key": key}
    if version_id:
        kw["VersionId"] = version_id
    obj = s3.get_object(**kw)
    body = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(body), engine="pyarrow"), obj, body


def _row(df, ticker: str = TICKER) -> dict:
    hits = df.loc[df.index == ticker]
    if len(hits) != 1:
        raise Refused(f"{ticker} appears {len(hits)} times (expected exactly 1)")
    return hits.iloc[0].to_dict()


def _versions(s3, key: str) -> tuple[list[dict], dict | None]:
    """All object versions of ``key`` oldest first, and the latest delete marker if it is current."""
    versions, markers = [], []
    kw = {"Bucket": BUCKET, "Prefix": key}
    while True:
        page = s3.list_object_versions(**kw)
        versions += [v for v in page.get("Versions", []) if v["Key"] == key]
        markers += [m for m in page.get("DeleteMarkers", []) if m["Key"] == key]
        if not page.get("IsTruncated"):
            break
        kw.update(KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"])
    # S3 lists a key's versions newest first. That order is authoritative;
    # LastModified has 1-second resolution and can tie.
    versions.reverse()
    latest_marker = next((m for m in markers if m.get("IsLatest")), None)
    return versions, latest_marker


def _expiry(last_modified: dt.datetime) -> str:
    """S3 lifecycle: creation + N days, rounded up to the next 00:00 UTC."""
    t = last_modified + dt.timedelta(days=LIFECYCLE_DAYS)
    midnight = dt.datetime(t.year, t.month, t.day, tzinfo=dt.UTC)
    if t > midnight:
        midnight += dt.timedelta(days=1)
    return midnight.strftime("%Y-%m-%dT%H:%MZ")


def _bar(row: dict) -> dict:
    keys = ("date", *PRICE_COLS, "Adj_Close", "Volume", "VWAP", "source", "revision", "xsource_provenance")
    out = {}
    for k in keys:
        v = row.get(k)
        if hasattr(v, "item"):
            v = v.item()
        if isinstance(v, float) and math.isnan(v):
            v = None
        out[k] = v
    return out


@dataclass
class RowPlan:
    date: str
    key: str
    status: str                      # RESTATE | ALREADY-CORRECT | AGED-OUT
    expires_utc: str | None = None
    current_version_id: str | None = None
    current_etag: str | None = None
    restore_source_version_id: str | None = None
    restore_source_last_modified: str | None = None
    first_halved_version_id: str | None = None
    first_halved_last_modified: str | None = None
    before: dict | None = None
    after: dict | None = None
    first_write: dict | None = None
    new_version_id: str | None = None
    verified: bool | None = None
    _current_df: Any = field(default=None, repr=False)
    _new_df: Any = field(default=None, repr=False)

    def public(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


def plan_file(s3, date: str, expected_halved_close: float, *, restatement_id: str, run_date: str) -> RowPlan:
    key = f"{PREFIX}{date}.parquet"
    versions, marker = _versions(s3, key)
    if not versions or marker is not None:
        return RowPlan(date=date, key=key, status="AGED-OUT")
    current_meta = versions[-1]
    if not current_meta.get("IsLatest"):
        raise Refused(f"{key}: newest version {current_meta['VersionId']} is not IsLatest")

    history, frames = [], {}
    for v in versions:
        df, _obj, _body = _read_frame(s3, key, v["VersionId"])
        frames[v["VersionId"]] = df
        history.append(_row(df))

    cur_df, cur_obj, _ = _read_frame(s3, key, current_meta["VersionId"])
    current = _row(cur_df)
    plan = RowPlan(
        date=date, key=key, status="", expires_utc=_expiry(current_meta["LastModified"]),
        current_version_id=current_meta["VersionId"], current_etag=cur_obj["ETag"],
        before=_bar(current), first_write=_bar(history[0]), _current_df=cur_df,
    )

    src_i, first_i, already = find_restore_source(history)
    src_meta, first_meta = versions[src_i], versions[first_i]
    source = history[src_i]
    plan.restore_source_version_id = src_meta["VersionId"]
    plan.restore_source_last_modified = src_meta["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")
    plan.first_halved_version_id = first_meta["VersionId"]
    plan.first_halved_last_modified = first_meta["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")

    if already or same_bar(current, source):
        plan.status = "ALREADY-CORRECT"
        return plan
    if abs((_num(current["Close"]) or 0) - expected_halved_close) > 1e-9:
        raise Refused(f"{key}: current {TICKER} Close {current['Close']} != {expected_halved_close} measured "
                      "2026-10-18 and not the restore-source bar either; the store moved, look again")
    yf_close, src_close = _num(history[0]["Close"]), _num(source["Close"])
    if yf_close is None or src_close is None or abs(src_close / yf_close - 1) > YF_XCHECK_TOL:
        raise Refused(f"{key}: restore source Close {src_close} disagrees with the first write {yf_close} "
                      f"by more than {YF_XCHECK_TOL:.0%}")

    after = restated_row(current, source, source_version_id=src_meta["VersionId"],
                         restatement_id=restatement_id, run_date=run_date)
    new_df = cur_df.copy()
    for col in new_df.columns:
        new_df.at[TICKER, col] = after[col] if col in after else current[col]
    new_df = new_df.astype(cur_df.dtypes.to_dict())
    if not new_df.drop(index=TICKER).equals(cur_df.drop(index=TICKER)):
        raise Refused(f"{key}: planned object changes rows other than {TICKER}")
    if list(new_df.columns) != list(cur_df.columns) or list(new_df.index) != list(cur_df.index):
        raise Refused(f"{key}: planned object changes the column set or row order")
    plan.after = _bar(_row(new_df))
    plan.status = "RESTATE"
    plan._new_df = new_df
    return plan


def _to_parquet_bytes(df) -> bytes:
    # The collector's own encoding (collectors/daily_closes.py write step).
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=True)
    return buf.getvalue()


def _schema_of(body: bytes):
    import pyarrow.parquet as pq

    return pq.read_schema(io.BytesIO(body)).remove_metadata()


def apply_file(s3, plan: RowPlan, restatement_id: str) -> None:
    """ETag-conditional write of the planned object, then read-back of that exact version."""
    _cur_df, _obj, cur_body = _read_frame(s3, plan.key, plan.current_version_id)
    body = _to_parquet_bytes(plan._new_df)
    if not _schema_of(body).equals(_schema_of(cur_body)):
        raise Refused(f"{plan.key}: re-encoded parquet schema differs from the current object")
    resp = s3.put_object(
        Bucket=BUCKET, Key=plan.key, Body=body, ContentType="application/octet-stream",
        IfMatch=plan.current_etag,
        Metadata={
            "restatement-id": restatement_id,
            "restated-ticker": TICKER,
            "restored-from-version-id": plan.restore_source_version_id or "",
            "replaces-version-id": plan.current_version_id or "",
            "tracker": TRACKER,
        },
    )
    plan.new_version_id = resp.get("VersionId")
    back, _o, _b = _read_frame(s3, plan.key, plan.new_version_id)
    ok = (
        _bar(_row(back)) == plan.after
        and back.drop(index=TICKER).equals(plan._current_df.drop(index=TICKER))
    )
    versions, _m = _versions(s3, plan.key)
    ok = ok and bool(versions) and versions[-1]["VersionId"] == plan.new_version_id
    plan.verified = ok


# ── gate, record, CLI ────────────────────────────────────────────────────────

def fix_merged() -> tuple[bool, str]:
    """Is nousergon-data#2192 merged? (True, how) / (False, why)."""
    if shutil.which("gh"):
        try:
            out = subprocess.run(
                ["gh", "api", f"repos/nousergon/nousergon-data/pulls/{FIX_PR_NUMBER}", "--jq", ".merged"],
                capture_output=True, text=True, timeout=30,
            )
            if out.returncode == 0 and out.stdout.strip() in ("true", "false"):
                merged = out.stdout.strip() == "true"
                return merged, f"gh api: #{FIX_PR_NUMBER} merged={out.stdout.strip()}"
        except (OSError, subprocess.SubprocessError):
            pass
    repo = Path(__file__).resolve().parents[1]
    try:
        src = subprocess.run(
            ["git", "show", "origin/main:corporate_actions/registry.py"],
            cwd=repo, capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"cannot read origin/main ({exc!r}) and gh unavailable"
    if src.returncode != 0:
        return False, f"git show origin/main failed: {src.stderr.strip()[:200]}"
    if FIX_MARKER in src.stdout:
        return True, f"origin/main corporate_actions/registry.py defines {FIX_MARKER!r} (#{FIX_PR_NUMBER})"
    return False, f"origin/main lacks {FIX_MARKER!r}: #{FIX_PR_NUMBER} not merged (or git pull not run)"


def _caller_arn(sts) -> str:
    try:
        return sts.get_caller_identity()["Arn"]
    except Exception as exc:  # noqa: BLE001 — recorded, not fatal
        return f"unknown ({type(exc).__name__})"


def build_record(*, restatement_id: str, run_at: str, status: str, operator: str,
                 gate: str, plans: list[RowPlan], script_pr: str | None) -> dict:
    return {
        "schema_version": 1,
        "restatement_id": restatement_id,
        "status": status,
        "run_at": run_at,
        "operator_arn": operator,
        "store": f"s3://{BUCKET}/{PREFIX}",
        "ticker": TICKER,
        "what_changed": f"{TICKER} row (Open/High/Low/Close/Adj_Close/Volume/VWAP/source) restored "
                        "to the last pre-halving polygon write; every other ticker untouched; "
                        "revision = current+1",
        "why": REASON,
        "ruling": RULING,
        "tracker": TRACKER,
        "fix_pr": FIX_PR,
        "fix_gate": gate,
        "script": "nousergon-data scripts/restate_psky_phantom_split.py",
        "script_pr": script_pr,
        "rollback": "aws s3api copy-object --bucket alpha-engine-research --key <key> "
                    "--copy-source 'alpha-engine-research/<key>?versionId=<current_version_id>'",
        "rows": [p.public() for p in plans],
    }


def _print_plan(plans: list[RowPlan]) -> None:
    for p in plans:
        print(f"\n== {p.key}  [{p.status}]  expires≈{p.expires_utc or '-'}")
        if p.status == "AGED-OUT":
            print("   file is gone (lifecycle) — moot")
            continue
        print(f"   current   v={p.current_version_id}  {json.dumps(_brief(p.before))}")
        if p.status == "RESTATE":
            print(f"   restore   v={p.restore_source_version_id} ({p.restore_source_last_modified})")
            print(f"   halved at v={p.first_halved_version_id} ({p.first_halved_last_modified})")
            print(f"   1st write {json.dumps(_brief(p.first_write))}")
            print(f"   AFTER               {json.dumps(_brief(p.after))}")
        if p.new_version_id:
            print(f"   written   v={p.new_version_id}  verified={p.verified}")


def _brief(bar: dict | None) -> dict:
    if not bar:
        return {}
    return {k: bar.get(k) for k in ("Open", "High", "Low", "Close", "Volume", "source", "revision")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--apply", action="store_true", help="write; without it the run is read-only")
    ap.add_argument("--profile", default=None, help="AWS profile (default: AWS_PROFILE / ambient)")
    ap.add_argument("--region", default=None, help="AWS region (default: AWS_REGION / us-east-1)")
    ap.add_argument("--script-pr", default=None, help="link recorded in the provenance record")
    args = ap.parse_args(argv)

    import boto3

    session = boto3.Session(profile_name=args.profile,
                            region_name=args.region or os.environ.get("AWS_REGION") or "us-east-1")
    s3, sts = session.client("s3"), session.client("sts")
    now = dt.datetime.now(dt.UTC)
    run_at, run_date = now.strftime("%Y-%m-%dT%H:%M:%SZ"), now.strftime("%Y-%m-%d")
    restatement_id = f"psky-phantom-split-{now.strftime('%Y%m%dT%H%M%SZ')}"
    operator = _caller_arn(sts)
    merged, gate = fix_merged()

    print(f"restatement {restatement_id}  operator={operator}  mode={'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"gate: #{FIX_PR_NUMBER} {'MERGED' if merged else 'NOT MERGED'} — {gate}")

    plans: list[RowPlan] = []
    try:
        for date, halved in TARGETS.items():
            plans.append(plan_file(s3, date, halved, restatement_id=restatement_id, run_date=run_date))
    except Refused as exc:
        _print_plan(plans)
        print(f"\nREFUSED (nothing written): {exc}", file=sys.stderr)
        return 1
    _print_plan(plans)
    todo = [p for p in plans if p.status == "RESTATE"]
    print(f"\nplan: {len(todo)} to restate, "
          f"{sum(p.status == 'ALREADY-CORRECT' for p in plans)} already correct, "
          f"{sum(p.status == 'AGED-OUT' for p in plans)} aged out")

    if not args.apply:
        if not merged:
            print(f"DRY RUN OK — but --apply would be REFUSED: #{FIX_PR_NUMBER} is not merged.")
            return 3
        print("DRY RUN OK — re-run with --apply to write.")
        return 0
    if not merged:
        print(f"REFUSED (nothing written): #{FIX_PR_NUMBER} is not merged — {gate}", file=sys.stderr)
        return 1
    if not todo:
        print("nothing to do.")
        return 0

    status, rc = "complete", 0
    try:
        for p in todo:
            apply_file(s3, p, restatement_id)
            if not p.verified:
                status, rc = "partial", 2
                print(f"VERIFY FAILED on {p.key} (new v={p.new_version_id}); stopping", file=sys.stderr)
                break
    except Exception as exc:  # noqa: BLE001 — record the partial run, then fail
        status, rc = "partial", 2
        print(f"WRITE FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
    finally:
        record = build_record(restatement_id=restatement_id, run_at=run_at, status=status,
                              operator=operator, gate=gate, plans=plans, script_pr=args.script_pr)
        body = json.dumps(record, indent=2, sort_keys=True, default=str)
        local = Path.home() / ".ne-restatements" / f"{restatement_id}.json"
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text(body + "\n", encoding="utf-8")
        rec_key = f"{RECORD_PREFIX}{run_date}/{restatement_id}.json"
        try:
            s3.put_object(Bucket=BUCKET, Key=rec_key, Body=body.encode("utf-8"),
                          ContentType="application/json")
            print(f"provenance record: s3://{BUCKET}/{rec_key}  (local copy {local})")
        except Exception as exc:  # noqa: BLE001
            rc = rc or 2
            print(f"PROVENANCE RECORD WRITE FAILED ({exc!r}); local copy at {local}", file=sys.stderr)
    _print_plan(todo)
    print(f"\nRESULT: {status.upper()} (exit {rc})")
    return rc


if __name__ == "__main__":
    sys.exit(main())
