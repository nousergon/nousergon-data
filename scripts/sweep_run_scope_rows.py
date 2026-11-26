#!/usr/bin/env python3
"""Re-grade stored weekly run-scope rows with the corrected derivation.

Tracked as ``alpha-engine-config-I11984``. Follow-up to nousergon-data-PR2066.

**What it corrects.** ``backtest/{run_date}/run_scope.json`` rows were written
by ``weekly-run-scope/run_scope.py`` before it learned two things (PR2066):
follow a routing Choice down the branch the run actually took, and grade a
stage by its branch's end-to-end result. Merging that fix changes new writes
only. A row already on S3 keeps whatever the old walk said. The one that
mattered: ``backtest/2026-10-02/run_scope.json :: stages.EvalJudge`` says
``ENABLED_FAILED`` / ``EvalJudgeSubmitWeekly`` / "never entered" for a
first-Saturday run that graded 96/96 on ``EvalJudgeSubmitFirstSaturday``, and
the Director reported an outage that did not happen.

**How it re-grades, through supported code only.** For each stored artifact:

1. The executions that authored its rows (each row's
   ``recorded_by_execution_arn``, else the artifact's ``execution_arn``).
2. Each one re-derived with :func:`run_scope.build_run_scope` from
   ``DescribeStateMachineForExecution`` (the definition that execution ran
   against) and ``GetExecutionHistory`` cut at its first ``RunScope`` entry.
   That cut is exactly the history the Lambda read when it wrote the row.
3. Those scopes folded through :func:`run_scope.merge_run_scopes`, the same
   accumulation rule the handler applies, giving the cycle's corrected rows.
4. Each stored row is classified against its corrected row:

   ``upgrade``          the corrected claim is strictly stronger. The merge
                        rule accepts it, so ``--apply`` writes it.
   ``downgrade``        the corrected claim is weaker (a stored COMPLETED
                        that was really a caught failure). The merge rule
                        refuses it by design. It is REPORTED, never written:
                        demoting an established claim needs an explicit
                        correction mode, and that is a decision for Brian.
   ``restated``         same strength, different entry state. Reported only.
   ``unchanged``        nothing to do.
   ``after_scope``      a stage that runs after ``RunScope``. The current
                        code lists it under ``after_scope`` instead of as a
                        row (alpha-engine-config-I11502). Left as stored.
   ``not_rederivable``  no corrected row (an author whose history no longer
                        exists, or that never entered ``RunScope``).

**Read-only unless told otherwise.** Without ``--apply`` it reads S3 and
Step Functions and prints the findings. Nothing is written. With ``--apply``
it writes only the ``upgrade`` rows. Each write is conditional on the ETag it
read (``IfMatch``), so a scope written in between makes the sweep stop rather
than overwrite it. Every corrected row carries ``corrected_by``, and the
artifact's ``scope_merge`` ledger records the sweep and keeps the previous
ledger.

    python3 scripts/sweep_run_scope_rows.py                       # every cycle, report only
    python3 scripts/sweep_run_scope_rows.py --run-date 2026-10-02 --stage EvalJudge
    python3 scripts/sweep_run_scope_rows.py --run-date 2026-10-02 --stage EvalJudge --apply
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import pathlib
import re
import sys
import types
from datetime import datetime, timezone
from collections.abc import Iterable, Mapping
from typing import Protocol, cast

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
LAMBDA_DIR = _REPO_ROOT / "infrastructure" / "lambdas" / "weekly-run-scope"

# A raw JSON object: an S3 artifact body, a Step Functions history event or a
# re-derived scope row. Its values are whatever JSON held.
_Json = dict[str, object]


class _Paginator(Protocol):
    def paginate(self, **kwargs: object) -> Iterable[Mapping[str, object]]: ...


class _Client(Protocol):
    def get_paginator(self, name: str) -> _Paginator: ...

    def describe_execution(self, **kwargs: object) -> Mapping[str, object]: ...

    def describe_state_machine_for_execution(self, **kwargs: object) -> Mapping[str, object]: ...

    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...

    def put_object(self, **kwargs: object) -> object: ...


class _Readable(Protocol):
    def read(self) -> bytes: ...


def _object(value: object) -> _Json:
    """Narrow a decoded JSON value at an object boundary."""
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise ValueError("expected string JSON object keys")
    return cast(_Json, mapping)


def _objects(value: object) -> list[_Json]:
    if not isinstance(value, list):
        return []
    return [_object(item) for item in cast(list[object], value)]


def _optional_object(value: object) -> _Json | None:
    return _object(cast(dict[object, object], value)) if isinstance(value, dict) else None


def _load_run_scope() -> types.ModuleType:
    """The Lambda's own derivation, loaded by path rather than copied.

    By path, not by putting the Lambda directory on ``sys.path``: that
    directory also holds an ``index.py``, and a bare ``import index`` anywhere
    else in the process would then resolve to it. ``run_scope.py`` imports
    only the standard library, so it loads cleanly on its own.
    """
    name = "weekly_run_scope_run_scope"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, LAMBDA_DIR / "run_scope.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load {LAMBDA_DIR / 'run_scope.py'}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


rs = _load_run_scope()

BUCKET = "alpha-engine-research"
PREFIX = "backtest/"
KEY_TEMPLATE = "backtest/{run_date}/run_scope.json"
_KEY_RE = re.compile(r"^backtest/(\d{4}-\d{2}-\d{2})/run_scope\.json$")
SWEEP_ID = "alpha-engine-config-I11984 stored-row sweep"

UPGRADE = "upgrade"
DOWNGRADE = "downgrade"
RESTATED = "restated"
UNCHANGED = "unchanged"
AFTER_SCOPE = "after_scope"
NOT_REDERIVABLE = "not_rederivable"


class SweepWriteConflict(RuntimeError):
    """The artifact changed between this sweep's read and its write."""


# ---------------------------------------------------------------------------
# Re-derivation
# ---------------------------------------------------------------------------


def cut_at_scope(history: list[_Json], scope_state: str = rs.SCOPE_STATE) -> list[_Json] | None:
    """The history up to and including the first ``scope_state`` entry.

    That is what the Lambda read when it wrote its row. ``None`` when the
    execution never entered ``scope_state``, so it never wrote a row either.
    """
    for index, event in enumerate(history):
        details = _object(event.get("stateEnteredEventDetails") or {})
        if event.get("type") == "TaskStateEntered" and details.get("name") == scope_state:
            return history[: index + 1]
    return None


def row_authors(stored: _Json) -> list[str]:
    """Every execution that wrote this cycle's artifact, row authors first.

    The authors of the stored rows (``recorded_by_execution_arn``, else the
    artifact's ``execution_arn`` for rows written before provenance existed),
    then the other executions the artifact names: the last writer and
    ``contributing_executions``. The last writer matters even when it
    authored no row. On 2026-09-18 a rerun completed PredictorTraining, but
    its equal claim was not recorded over the scheduled run's. Leaving it out
    would grade the cycle by the scheduled run alone.
    """
    fallback = stored.get("execution_arn") or ""
    authors: list[str] = []

    def _add(arn: object) -> None:
        if isinstance(arn, str) and arn and arn not in authors:
            authors.append(arn)

    rows = _object(stored.get("stages") or {})
    for _, row_value in sorted(rows.items()):
        row = _optional_object(row_value) or {}
        _add(row.get("recorded_by_execution_arn") or fallback)
    _add(fallback)
    contributors: object = stored.get("contributing_executions") or []
    for arn in cast(list[object], contributors) if isinstance(contributors, list) else []:
        _add(arn)
    return authors


def _history(
    states: _Client,
    execution_arn: str,
) -> list[_Json]:
    events: list[_Json] = []
    paginator = states.get_paginator("get_execution_history")
    for page in paginator.paginate(
        executionArn=execution_arn, includeExecutionData=False, reverseOrder=False,
    ):
        page_events = page.get("events", [])
        if isinstance(page_events, list):
            events.extend(_object(event) for event in cast(list[object], page_events))
    return events


def rederive(
    states: _Client,
    execution_arn: str,
    run_date: str,
    recorded_at: str,
) -> tuple[_Json | None, str]:
    """``(scope, note)`` for one execution, re-derived with the current code.

    Read-only: three ``states:Describe*``/``Get*`` calls. Returns ``(None,
    why)`` when the execution cannot be replayed, and never raises for that,
    because one expired history must not stop the rest of the sweep.
    """
    try:
        described = states.describe_execution(executionArn=execution_arn)
        definition_text = states.describe_state_machine_for_execution(
            executionArn=execution_arn
        )["definition"]
        if not isinstance(definition_text, str):
            raise ValueError("execution definition is not a string")
        definition = _object(json.loads(definition_text))
        history = _history(states, execution_arn)
    except Exception as exc:  # noqa: BLE001
        return None, f"could not read the execution: {type(exc).__name__}: {exc}"
    cut = cut_at_scope(history)
    if cut is None:
        return None, f"never entered {rs.SCOPE_STATE}, so it never wrote a row"
    try:
        raw_input = described.get("input") or "{}"
        execution_input = _object(json.loads(raw_input) if isinstance(raw_input, str) else {})
    except ValueError:
        execution_input = {}
    flags: _Json = {
        key: value for key, value in execution_input.items() if key.startswith("skip_")
    }
    scope = rs.build_run_scope(
        definition,
        cut,
        run_date=run_date,
        execution_arn=execution_arn,
        state_machine_arn=described.get("stateMachineArn", ""),
        input_flags=flags,
    )
    rs.stamp_provenance(scope, execution_arn, recorded_at)
    return scope, f"re-derived from {len(cut)} events"


def cycle_target(scopes: Iterable[_Json]) -> _Json | None:
    """The cycle's corrected rows: the re-derived scopes, accumulated by the
    handler's own rule, so a stage one author failed and another completed
    stays completed."""
    target = None
    for scope in scopes:
        target, _ = rs.merge_run_scopes(target, copy.deepcopy(scope))
    return target


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _summary(row: object) -> _Json:
    row = _optional_object(row) or {}
    return {
        "disposition": row.get("disposition"),
        "entry_state": row.get("entry_state"),
        "entry_state_source": row.get("entry_state_source"),
        "failed_state": row.get("failed_state"),
        "recorded_by_execution_arn": row.get("recorded_by_execution_arn"),
    }


def classify(stored: _Json, target: _Json | None, stages: Iterable[str] | None = None) -> list[_Json]:
    """One finding per stored row (or per ``stages``), held vs corrected."""
    wanted = set(stages) if stages else None
    target_rows = _object((target or {}).get("stages") or {})
    after_scope = _object((target or {}).get("after_scope") or {})
    findings: list[_Json] = []
    stored_rows = _object(stored.get("stages") or {})
    for name, held_value in sorted(stored_rows.items()):
        held = _optional_object(held_value) or {}
        if wanted is not None and name not in wanted:
            continue
        new = target_rows.get(name)
        new_row = _optional_object(new)
        if new_row is None:
            kind = AFTER_SCOPE if name in after_scope else NOT_REDERIVABLE
        elif rs.authority(new_row) > rs.authority(held):
            kind = UPGRADE
        elif rs.authority(new_row) < rs.authority(held):
            kind = DOWNGRADE
        elif (new_row.get("disposition"), new_row.get("entry_state")) != (
            held.get("disposition"), held.get("entry_state")
        ):
            kind = RESTATED
        else:
            kind = UNCHANGED
        findings.append({
            "stage": name,
            "kind": kind,
            "stored": _summary(held),
            "corrected": _summary(new_row) if new_row is not None else None,
        })
    return findings


def corrected_artifact(stored: _Json, target: _Json, findings: list[_Json], swept_at: str) -> _Json:
    """The stored artifact with its ``upgrade`` rows replaced, via the merge.

    The incoming body is the stored one carrying only the corrected rows, so
    every top-level field (execution, dates, ``after_scope``) is kept and only
    counts, graded set and statement are recomputed by the merge.
    """
    incoming = copy.deepcopy(stored)
    incoming.pop("scope_merge", None)
    incoming["stages"] = {}
    for finding in findings:
        if finding["kind"] != UPGRADE:
            continue
        target_rows = _object(target["stages"])
        stage = finding["stage"]
        if not isinstance(stage, str):
            raise ValueError("finding stage is not a string")
        row = _object(copy.deepcopy(target_rows[stage]))
        row["corrected_by"] = {
            "sweep": SWEEP_ID,
            "swept_at": swept_at,
            "replaced": finding["stored"],
        }
        _object(incoming["stages"])[stage] = row
    merged, ledger = rs.merge_run_scopes(copy.deepcopy(stored), incoming)
    ledger = _object(ledger)
    accepted_rows = _objects(ledger["accepted"])
    accepted = {
        stage
        for entry in accepted_rows
        if isinstance((stage := entry.get("stage")), str)
    }
    incoming_rows = _object(incoming["stages"])
    missing = sorted(set(incoming_rows) - accepted)
    if missing or ledger["rejected"]:
        # Unreachable while UPGRADE means "strictly stronger"; checked anyway
        # so a change to the merge rule cannot turn this into a silent no-op.
        raise RuntimeError(
            f"the merge did not accept the corrected rows {missing}: {ledger['rejected']}"
        )
    ledger["sweep"] = {
        "id": SWEEP_ID,
        "swept_at": swept_at,
        "corrected": sorted(incoming_rows),
    }
    ledger["previous_scope_merge"] = stored.get("scope_merge")
    merged["scope_merge"] = ledger
    return merged


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------


def stored_keys(
    s3: _Client,
    bucket: str,
    run_dates: list[str] | None,
) -> list[str]:
    if run_dates:
        return [KEY_TEMPLATE.format(run_date=d) for d in sorted(set(run_dates))]
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        contents: object = page.get("Contents", []) or []
        for obj_value in cast(list[object], contents) if isinstance(contents, list) else []:
            obj = _optional_object(obj_value) or {}
            key = obj.get("Key")
            if isinstance(key, str) and _KEY_RE.match(key):
                keys.append(key)
    return sorted(keys)


def read_artifact(
    s3: _Client,
    bucket: str,
    key: str,
) -> tuple[_Json, str]:
    response = s3.get_object(Bucket=bucket, Key=key)
    stream = cast(_Readable, response["Body"])
    body = _object(json.loads(stream.read()))
    etag = response.get("ETag", "")
    return body, etag if isinstance(etag, str) else ""


def write_artifact(
    s3: _Client,
    bucket: str,
    key: str,
    body: _Json,
    etag: str,
) -> None:
    """Conditional on the ETag read. A lost race stops the sweep; it never
    retries onto a body it has not classified."""
    if not etag:
        raise SweepWriteConflict(f"no ETag was read for s3://{bucket}/{key}; refusing to write")
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(body, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
            IfMatch=etag,
        )
    except Exception as exc:  # noqa: BLE001
        error_response = cast(object, getattr(exc, "response", None))
        response_object = _optional_object(error_response) or {}
        error = response_object.get("Error")
        error_object = _optional_object(error)
        code = error_object.get("Code") if error_object is not None else None
        if code in ("PreconditionFailed", "412", "ConditionalRequestConflict"):
            raise SweepWriteConflict(
                f"s3://{bucket}/{key} changed after it was read ({code}); nothing "
                "was written. Re-run the sweep to classify the new body."
            ) from exc
        raise


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def sweep_one(
    s3: _Client,
    states: _Client,
    bucket: str,
    key: str,
    *,
    stages: Iterable[str] | None = None,
    apply: bool = False,
    now: datetime | None = None,
) -> _Json:
    swept_at = (now or datetime.now(timezone.utc)).isoformat(timespec="seconds")
    match = _KEY_RE.match(key)
    if match is None:
        raise ValueError(f"not a run-scope key: {key}")
    run_date = match.group(1)
    stored, etag = read_artifact(s3, bucket, key)
    notes: dict[str, str] = {}
    scopes: list[_Json] = []
    for arn in row_authors(stored):
        scope, note = rederive(states, arn, run_date, swept_at)
        notes[arn] = note
        if scope is not None:
            scopes.append(scope)
    target = cycle_target(scopes)
    findings = classify(stored, target, stages)
    result: _Json = {"key": key, "authors": notes, "findings": findings, "written": False}
    # An upgrade finding exists only where ``target`` has the row, so the
    # ``target is not None`` test never changes which cycles are written.
    if apply and target is not None and any(f["kind"] == UPGRADE for f in findings):
        body = corrected_artifact(stored, target, findings, swept_at)
        write_artifact(s3, bucket, key, body, etag)
        result["written"] = True
    return result


def _render(result: _Json, verbose: bool) -> list[str]:
    lines = [f"== s3://{BUCKET}/{result['key']}"]
    authors = _object(result["authors"])
    for arn, note in authors.items():
        lines.append(f"   author {arn.rsplit(':', 1)[-1]}: {note}")
    for finding in _objects(result["findings"]):
        if finding["kind"] in (UNCHANGED, AFTER_SCOPE) and not verbose:
            continue
        held = _object(finding["stored"])
        corrected = finding["corrected"]
        new = _optional_object(corrected) or {}
        kind = finding["kind"]
        if not isinstance(kind, str):
            raise ValueError("finding kind is not a string")
        line = (
            f"   {kind.upper():16s} {finding['stage']}: "
            f"{held['disposition']} / {held['entry_state']} -> "
            f"{new.get('disposition')} / {new.get('entry_state')}"
        )
        if new.get("failed_state"):
            line += f" (failed_state {new['failed_state']})"
        if kind == DOWNGRADE:
            line += " [NOT APPLIED: the merge rule refuses a weaker claim; needs an explicit correction mode]"
        lines.append(line)
    if result["written"]:
        lines.append("   WROTE corrected artifact (IfMatch on the ETag read)")
    return lines


def _run_date(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise argparse.ArgumentTypeError(f"{value!r} is not YYYY-MM-DD")
    return value


def main(
    argv: list[str] | None = None,
    *,
    s3: _Client | None = None,
    states: _Client | None = None,
) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--run-date", action="append", dest="run_dates", type=_run_date,
                        help="cycle to sweep (repeatable); default every stored cycle")
    parser.add_argument("--stage", action="append", dest="stages",
                        help="limit to these stage rows (repeatable)")
    parser.add_argument("--apply", action="store_true",
                        help="write the upgrade rows; without it nothing is written")
    parser.add_argument("--json", action="store_true", help="print the findings as JSON")
    parser.add_argument("--verbose", action="store_true", help="also list unchanged rows")
    args = parser.parse_args(argv)

    if s3 is None or states is None:
        import boto3  # pyright: ignore[reportMissingTypeStubs] — boto3 ships no type information

        s3 = s3 or cast(_Client, boto3.client("s3", region_name=args.region))  # pyright: ignore[reportUnknownMemberType] — boto3 is untyped
        states = states or cast(_Client, boto3.client("stepfunctions", region_name=args.region))  # pyright: ignore[reportUnknownMemberType] — boto3 is untyped

    results: list[_Json] = []
    status = 0
    for key in stored_keys(s3, args.bucket, args.run_dates):
        try:
            results.append(sweep_one(
                s3, states, args.bucket, key, stages=args.stages, apply=args.apply,
            ))
        except SweepWriteConflict as exc:
            print(f"CONFLICT: {exc}", file=sys.stderr)
            status = 2
        except Exception as exc:  # noqa: BLE001
            # One unreadable artifact (a --run-date with no scope, a corrupt
            # body) is reported and the rest of the sweep still runs.
            print(f"SKIPPED s3://{args.bucket}/{key}: {type(exc).__name__}: {exc}",
                  file=sys.stderr)
            status = max(status, 1)
    totals: dict[str, int] = {}
    for result in results:
        for finding in _objects(result["findings"]):
            kind = finding["kind"]
            if not isinstance(kind, str):
                raise ValueError("finding kind is not a string")
            totals[kind] = totals.get(kind, 0) + 1
    if args.json:
        print(json.dumps({"apply": args.apply, "totals": totals, "results": results},
                         indent=2, sort_keys=True))
    else:
        for result in results:
            print("\n".join(_render(result, args.verbose)))
        mode = "APPLY" if args.apply else "REPORT ONLY (nothing written; pass --apply to write upgrades)"
        written = sum(1 for r in results if r["written"])
        print(f"{mode}: {len(results)} artifact(s), {written} written; "
              + ", ".join(f"{k}={v}" for k, v in sorted(totals.items())))
    return status


if __name__ == "__main__":
    sys.exit(main())
