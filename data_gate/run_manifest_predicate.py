"""The run-manifest completion predicate, shared by producer and consumer.

ONE implementation of "did this unit's run publish what its descriptor says it
publishes", asked from two sides:

* the PRODUCER — ``ne-data-collection-*``'s ``VerifyRunManifests`` state, via the
  data-spot dispatcher's ``{"action": "completion-check"}``
  (`alpha-engine-config-I10787`, data collector plan P-20);
* the CONSUMER — the three v1 state machines' ``WaitForCollectionManifests``
  states, via the ``alpha-engine-collection-readiness-probe`` Lambda
  (`alpha-engine-config-I11264`: "Do not reimplement the predicate; share it").

It lived inside the dispatcher's ``index.py`` until the consumer needed it. It
moved here, verbatim, rather than being imported from the dispatcher, because
the dispatcher's module imports its launch stack (``krepis``, ``nousergon_lib``
EC2 spot) at load time and the probe must not ship or be able to reach any of
it: a v1 state machine that can invoke the function that launches collector
boxes is the second-writer path `alpha-engine-config-I11266` closes.

Pure: stdlib, the descriptor loader, and an S3 client the caller may inject.
Each calling Lambda's ``deploy.sh`` packages ``data_gate/__init__.py``,
``data_gate/descriptors.py``, this file and ``registry.d/units/`` at their
repo-relative paths, and each one's deploy workflow path-filters on them.
"""

from __future__ import annotations

import json
import logging
import os
import re

__all__ = [
    "COMPLETION_FAILURE_MODES",
    "DEFAULT_ROWS_OUT_FLOOR",
    "MANIFEST_BUCKET",
    "completion_check",
    "parse_ts",
    "readiness_check",
    "reset_unit_cache",
    "unit_descriptors",
]

logger = logging.getLogger(__name__)

REGION = os.environ.get("AWS_REGION", "us-east-1")


def _s3_client():
    import boto3  # noqa: PLC0415 - deferred: tests inject a fake and never need it

    return boto3.client("s3", region_name=REGION)


#
# WHY THIS LIVES HERE AND NOT IN THE ASL. The completion claim is now the run
# manifest (`data_run_manifest.v1`) of every unit the machine runs, graded on
# three properties: the manifest EXISTS for this execution, its `status` is
# `ok`, and every key the unit's descriptor says it publishes appears in
# `outputs[]` at or above its declared `rows_out` floor.
#
# Pure ASL could do the first two (`s3:listObjectsV2` + `s3:getObject` +
# `States.StringToJson` + a Choice). It cannot do the third: `outputs` is an
# ARRAY OF OBJECTS and ASL has no way to search an array by a field value, so
# per-key floor compliance is not expressible declaratively — the best a Map
# over `outputs` could assert is "some output exists", which is the claim we are
# replacing. A `{"action": "completion-check"}` action on the data-spot dispatcher
# (whose zip carries the descriptors it reads) runs this module, mirrors the
# existing `trading-day-check` precedent (a pure computation that launches
# nothing), and is unit-testable. SOTA is the declarative integration; the delta is that the
# declarative form cannot express the per-key claim at all, so it would have had
# to be weakened to fit the mechanism.
#
# FAIL LOUD, NEVER FAIL OPEN. This function RAISES on anything it cannot
# measure (an undeclared unit, an unparseable manifest, a missing `rows_out`),
# and returns `ok: false` with a machine-readable finding list on anything it
# measured and found wanting. The ASL routes BOTH to a Fail state — a dispatcher
# error through the existing Catch, a finding through four distinct named Fail
# states — so there is no path from this state to CollectionSucceeded except an
# affirmative, measured pass.

#: The bucket every collector already writes its manifests to
#: (`run_units.MANIFEST_BUCKET`). Env-overridable for a rehearsal account.
MANIFEST_BUCKET = os.environ.get("DATA_COLLECTION_MANIFEST_BUCKET", "alpha-engine-research")

#: The floor a published key's ``rows_out`` must meet when its descriptor
#: declares none. ONE, not zero: a key published with zero rows is the
#: empty-but-fresh silent degradation this whole objective exists to end, and a
#: default of zero would make the floor mechanism vacuous everywhere it was not
#: hand-calibrated. A unit for which zero is legitimate declares that, with a
#: reason, under ``completeness.rows_out_floor_na_code``. Calibrated per-unit
#: cardinality floors are P-13 (alpha-engine-config-I5935); this ships the
#: mechanism and the loud default.
DEFAULT_ROWS_OUT_FLOOR = 1

#: Failure modes in PRECEDENCE order. The ASL switches on the first one present
#: across all units so the execution's named error is the most upstream cause —
#: a missing manifest explains a missing output, never the other way round.
COMPLETION_FAILURE_MODES = (
    "manifest_missing",
    "run_not_ok",
    "output_missing",
    "rows_below_floor",
)

#: How far back to list manifests. The prefix is partitioned by trading day, so
#: `StartAfter` at (execution start - this many days) bounds a listing that
#: would otherwise grow without limit, while staying partition-agnostic: the
#: check never has to GUESS which trading day the unit filed its run under.
MANIFEST_LOOKBACK_DAYS = int(os.environ.get("DATA_COLLECTION_MANIFEST_LOOKBACK_DAYS", "3"))

#: The guard whose `not_applicable` verdict is a unit DECLARING that it
#: published nothing this run (same-date auto-skip or a dry run —
#: `weekly_collector.py::_record_phase_lineage`). It exempts key coverage and
#: nothing else. Legitimate because the auto-skip predicate re-verifies the
#: artifact's presence on S3 before returning the cache hit, so the published
#: object IS there; the unit simply did not rewrite it. Without this exemption
#: every idempotent re-drive of the weekly phase 1 would be a failure.
EMPTY_FRESH_GUARD = "empty_fresh"

#: A ``writes:`` entry that is a prose declaration rather than an S3 key
#: template — `arcticdb/universe (library)`, `research.db::score_performance`,
#: `predictor/price_cache/<macro series>`. These are COUNTED and returned under
#: `unverifiable`, never silently dropped: the swallowed failure mode would be
#: "this unit's publish claim grades nothing", and the recording surface is the
#: `unverifiable` list on every completion-check response plus the WARNING log
#: line below. Typing `writes:` entries (`kind: s3-key|arctic-library|table`) is
#: the robust fix and is a tracked follow-up, not something to infer here.
_NON_S3_WRITE = re.compile(r"(::|\s|<|>|\(|\))")
_WRITE_TOKEN = re.compile(r"\{[a-z_]+\}|\*")
_MAX_SUMMARY_CHARS = 4000

_UNITS_CACHE: dict[str, dict] | None = None


def _descriptors():
    """The repo's ONE descriptor loader, imported lazily.

    Lazy because a caller that never grades (a dispatcher launch invocation)
    must not pay to parse 46 YAML files. `data_gate/descriptors.py` and
    `registry.d/units/` are packaged into each calling Lambda's zip at their
    repo-relative paths (see each deploy.sh), so the loader's own
    ``REPO_ROOT``-relative ``UNITS_DIR`` resolves to ``/var/task`` — one
    implementation of "what a unit declares", not a second parser that drifts.
    """
    from data_gate import descriptors

    return descriptors


def _unit_descriptors() -> dict[str, dict]:
    global _UNITS_CACHE
    if _UNITS_CACHE is None:
        _UNITS_CACHE = {u.unit_id: u.raw for u in _descriptors().load_units()}
    return _UNITS_CACHE


def _parse_ts(value, *, where: str):
    """RFC3339 -> aware datetime, or RAISE naming where the bad value came from.

    Both timestamps this reads are contracts: `$$.Execution.StartTime` and the
    manifest's `finished`. A value that will not parse is a contract violation,
    and guessing one would silently move the freshness baseline.
    """
    from datetime import datetime, timezone

    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{where} is empty; the completion check has no freshness baseline")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{where}={value!r} is not an RFC3339 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _key_pattern(template: str, trading_day: str):
    """One ``writes:`` template as a regex over concrete manifest output keys.

    ``{date}``/``{trading_day}`` resolve to the day the MANIFEST ITSELF declares
    it ran for — never a day this function computes, which is how a check like
    this ends up disagreeing with the producer about what "today" was. Every
    other placeholder and every ``*`` is a fan-out (per ticker, per symbol, per
    currency) and matches one path segment. A trailing ``/`` is a declared
    prefix and matches anything under it. Returns None for a prose declaration.
    """
    if _NON_S3_WRITE.search(template) or template.startswith("arcticdb/"):
        return None
    parts, pos = [], 0
    for match in _WRITE_TOKEN.finditer(template):
        parts.append(re.escape(template[pos : match.start()]))
        token = match.group(0)
        parts.append(re.escape(trading_day) if token in ("{date}", "{trading_day}") else r"[^/]+")
        pos = match.end()
    parts.append(re.escape(template[pos:]))
    body = "".join(parts)
    if template.endswith("/"):
        body += r".+"
    return re.compile(rf"^{body}$")


def _rows_out_floor(unit_id: str, completeness: dict) -> tuple[int | None, str]:
    """The per-key ``rows_out`` floor for a unit, and how it was arrived at.

    ``completeness.floor`` is deliberately NOT used: it is a RATIO against a
    denominator (`metron/holdings_universe.json`, "constituents - delisted"),
    which this function cannot resolve and must not approximate. The absolute
    floor is its own declaration.
    """
    declared = completeness.get("rows_out_floor")
    if declared is not None:
        return int(declared), "declared"
    na_code = completeness.get("rows_out_floor_na_code")
    if na_code:
        if na_code not in _descriptors().NA_TAXONOMY:
            raise ValueError(
                f"{unit_id}: completeness.rows_out_floor_na_code={na_code!r} is not in "
                f"observability-policy §3.5's closed taxonomy "
                f"{sorted(_descriptors().NA_TAXONOMY)}"
            )
        return None, f"not_applicable ({na_code})"
    if completeness.get("status") == "not_applicable":
        return None, f"not_applicable ({completeness.get('na_code')})"
    return DEFAULT_ROWS_OUT_FLOOR, "default"


def _finding(mode: str, unit_id: str, key: str | None, detail: str) -> dict:
    if mode not in COMPLETION_FAILURE_MODES:
        raise ValueError(f"unknown completion failure mode {mode!r}")
    return {"mode": mode, "unit": unit_id, "key": key, "detail": detail}


def _newest_manifest(s3, prefix: str, started_at):
    """The unit's newest run manifest, if it finished at or after ``started_at``.

    ``run_id`` is a ULID and the day partition is an ISO date, so the prefix
    lists in execution order and ``max()`` IS the newest run — if THAT one
    predates this execution, every other one does too.
    """
    from datetime import timedelta

    prefix = f"{prefix.rstrip('/')}/"
    start_after = f"{prefix}{(started_at - timedelta(days=MANIFEST_LOOKBACK_DAYS)).date().isoformat()}"
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": MANIFEST_BUCKET, "Prefix": prefix, "StartAfter": start_after}
        if token:
            kwargs = {"Bucket": MANIFEST_BUCKET, "Prefix": prefix, "ContinuationToken": token}
        page = s3.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json"))
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    if not keys:
        return None, None
    key = max(keys)
    doc = json.loads(s3.get_object(Bucket=MANIFEST_BUCKET, Key=key)["Body"].read())
    if _parse_ts(doc.get("finished"), where=f"{key}:finished") < started_at:
        return key, None
    return key, doc


def _check_unit(s3, unit_id: str, raw: dict, started_at) -> tuple[list[dict], dict]:
    """Grade one unit's run against its descriptor. Returns (findings, row)."""
    prefix = str(raw["run_manifest_prefix"])
    row = {"unit": unit_id, "manifest": None, "status": None, "keys_checked": 0,
           "unverifiable": [], "auto_skipped": False, "floor": None}
    key, doc = _newest_manifest(s3, prefix, started_at)
    if doc is None:
        stale = f" (newest is {key}, which finished before it)" if key else ""
        return [
            _finding(
                "manifest_missing", unit_id, None,
                f"no data_run_manifest.v1 under s3://{MANIFEST_BUCKET}/{prefix}/ finished at "
                f"or after this execution started{stale}: the workloads exited 0 but the "
                f"unit left no run record for this run",
            )
        ], row

    row["manifest"] = key
    status = str(doc.get("status") or "")
    row["status"] = status
    if status != "ok":
        return [
            _finding(
                "run_not_ok", unit_id, None,
                f"manifest {key} reports status={status!r} reason={str(doc.get('reason') or '')[:400]!r}. "
                f"Naming a unit in verify_units IS the machine's declaration that this run must "
                f"publish it, so `not_applicable` is not a pass here — a unit that may "
                f"legitimately do nothing on this schedule is simply not named.",
            )
        ], row

    outputs = list(doc.get("outputs") or [])
    trading_day = str(doc.get("trading_day") or "")
    auto_skipped = any(
        g.get("guard") == EMPTY_FRESH_GUARD and g.get("verdict") == "not_applicable"
        for g in (doc.get("guards") or [])
    )
    row["auto_skipped"] = auto_skipped
    floor, floor_source = _rows_out_floor(unit_id, raw.get("completeness") or {})
    row["floor"] = floor if floor is not None else floor_source

    findings: list[dict] = []
    for template in raw.get("writes") or []:
        pattern = _key_pattern(str(template), trading_day)
        if pattern is None:
            row["unverifiable"].append(template)
            continue
        row["keys_checked"] += 1
        matched = [o for o in outputs if pattern.match(str(o.get("key") or ""))]
        if not matched:
            if auto_skipped:
                continue
            findings.append(
                _finding(
                    "output_missing", unit_id, str(template),
                    f"{unit_id} declares it publishes {template!r} but manifest {key} lists no "
                    f"matching key in outputs[] (it lists {[o.get('key') for o in outputs]}). "
                    f"A run that did not record the artifact did not publish it.",
                )
            )
            continue
        if floor is None:
            continue
        for out in matched:
            if "rows_out" not in out:
                raise ValueError(
                    f"{key}: outputs entry {out.get('key')!r} has no rows_out; "
                    "data_run_manifest.v1 requires it and 'we did not count' is not a value"
                )
            rows = int(out["rows_out"])
            if rows < floor:
                findings.append(
                    _finding(
                        "rows_below_floor", unit_id, str(out.get("key")),
                        f"{unit_id} published {out.get('key')!r} with rows_out={rows}, below its "
                        f"floor of {floor} ({floor_source}). Manifest {key}.",
                    )
                )
    if row["unverifiable"]:
        logger.warning(
            "completion-check: %s declares %d writes entries that are not S3 key templates "
            "and are therefore ungraded: %s",
            unit_id, len(row["unverifiable"]), row["unverifiable"],
        )
    return findings, row


def _completion_check(event: dict, s3_client=None) -> dict:
    """Grade every named unit's run manifest for this execution.

    Returns ``{"completion": {...}}`` — `ok`, a `failure_mode` the ASL switches
    on, the full machine-readable `findings` list, one `units` row per unit
    (including the ones that passed, so a unit emitting nothing is visible), and
    a `summary` the Fail state uses as its Cause.
    """
    units = [str(u).strip() for u in (event.get("units") or []) if str(u).strip()]
    if not units:
        raise ValueError(
            "completion-check was invoked with no units. The ASL only reaches this state "
            "when verify_units is non-empty, so an empty list here is a mis-wired input, "
            "not a machine with nothing to verify."
        )
    started_at = _parse_ts(event.get("started_at"), where="started_at")
    collection = str(event.get("collection") or "unknown")
    descriptors = _unit_descriptors()
    s3 = s3_client if s3_client is not None else _s3_client()

    findings: list[dict] = []
    rows: list[dict] = []
    for unit_id in units:
        raw = descriptors.get(unit_id)
        if raw is None:
            raise ValueError(
                f"verify_units names {unit_id!r}, which has no descriptor under "
                f"registry.d/units/. The descriptors are the only source of units "
                f"(data_collection_plan §4.1); a machine verifying a unit nobody declared "
                f"would grade nothing and read as a pass."
            )
        unit_findings, row = _check_unit(s3, unit_id, raw, started_at)
        findings.extend(unit_findings)
        rows.append(row)

    mode = next(
        (m for m in COMPLETION_FAILURE_MODES if any(f["mode"] == m for f in findings)), ""
    )
    summary = (
        f"data collection {collection}: completion check PASSED over {len(units)} unit(s)"
        if not findings
        else f"data collection {collection}: {len(findings)} completion finding(s) over "
        f"{len(units)} unit(s); first mode {mode}. "
        + " | ".join(f"[{f['mode']}] {f['unit']} {f['key'] or ''}: {f['detail']}" for f in findings)
    )[:_MAX_SUMMARY_CHARS]
    logger.info("completion-check %s: ok=%s mode=%s", collection, not findings, mode or "-")
    return {
        "completion": {
            "ok": not findings,
            "failure_mode": mode,
            "findings": findings,
            "units": rows,
            "summary": summary,
        }
    }


# ── Consumer-side readiness (alpha-engine-config-I11264) ─────────────────────
#
# `_completion_check` above is the PRODUCER's claim: the standalone collection
# machine grades its own run. Nothing read that claim from the consumer side,
# so once the decoupled cutover (alpha-engine-config-I11269) removed the v1
# SFs' inline data stages, every surviving v1 consumer would have run AHEAD of
# its producer (plan §6.2b). `_readiness_check` is the same predicate — the
# same `_check_unit`, not a second implementation of it — asked by a CONSUMER:
# "has the standalone run for this cycle published everything I read?". The
# `alpha-engine-collection-readiness-probe` Lambda is its only caller; it is a
# separate function from the data-spot dispatcher so that no v1 state machine
# invokes the function that launches collector boxes (alpha-engine-config-
# I11266 deliverable 6).
#
# Two differences from `completion-check`, both about who is asking:
#
#   * The freshness baseline is the CONSUMER's execution start minus a declared
#     `lookback_seconds`, because the consumer does not start when the producer
#     does. The preopen SF starts at 08:15 ET and the morning collection at
#     07:30 ET, so a manifest that finished at 08:05 is this cycle's and must
#     count; the postclose and weekly SFs start BEFORE their producer, so their
#     lookback is 0. Each v1 definition declares its own lookback and a test
#     derives it from the two schedules.
#   * It never FAILS on a finding — a consumer polling a producer that has not
#     finished yet is the normal case, not an error. It returns `ready` (no
#     finding at all) and `settled` (every unit has a manifest for this cycle,
#     so waiting longer cannot change the answer). A consumer stops polling on
#     either; `settled and not ready` is a producer failure the consumer
#     degrades on at once instead of burning its whole budget. It still RAISES
#     on anything it cannot measure (an undeclared unit, a malformed manifest),
#     exactly as `_completion_check` does — the v1 SF's Catch counts that as
#     one not-ready poll, so a persistent raise exhausts the bounded budget and
#     degrades loudly rather than proceeding on an unmeasured claim.


def _readiness_check(event: dict, s3_client=None) -> dict:
    """Has the standalone collection published every named unit for this cycle?

    Returns ``{"readiness": {...}}`` — ``ready``, ``settled``, the units still
    ``missing`` a manifest, the units whose manifest exists but ``failed`` the
    predicate, the first ``failure_mode`` in `COMPLETION_FAILURE_MODES`
    precedence, a ``summary`` and the ``baseline`` the manifests were graded
    against.
    """
    from datetime import timedelta

    units = [str(u).strip() for u in (event.get("units") or []) if str(u).strip()]
    if not units:
        raise ValueError(
            "readiness-check was invoked with no units. A consumer that waits on "
            "nothing would read ready on an empty claim."
        )
    not_before = _parse_ts(event.get("not_before"), where="not_before")
    try:
        lookback = int(event.get("lookback_seconds", 0))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"lookback_seconds={event.get('lookback_seconds')!r} is not an integer"
        ) from exc
    if lookback < 0:
        raise ValueError(f"lookback_seconds={lookback} is negative")
    baseline = not_before - timedelta(seconds=lookback)
    collection = str(event.get("collection") or "unknown")
    descriptors = _unit_descriptors()
    s3 = s3_client if s3_client is not None else _s3_client()

    findings: list[dict] = []
    for unit_id in units:
        raw = descriptors.get(unit_id)
        if raw is None:
            raise ValueError(
                f"readiness-check names {unit_id!r}, which has no descriptor under "
                f"registry.d/units/; a consumer waiting on a unit nobody declared "
                f"would grade nothing and read as ready."
            )
        unit_findings, _row = _check_unit(s3, unit_id, raw, baseline)
        findings.extend(unit_findings)

    missing = sorted({f["unit"] for f in findings if f["mode"] == "manifest_missing"})
    failed = sorted({f["unit"] for f in findings if f["mode"] != "manifest_missing"})
    mode = next(
        (m for m in COMPLETION_FAILURE_MODES if any(f["mode"] == m for f in findings)), ""
    )
    ready = not findings
    settled = not missing
    summary = (
        f"collection {collection}: READY over {len(units)} unit(s)"
        if ready
        else f"collection {collection}: not ready over {len(units)} unit(s); "
        f"missing={missing} failed={failed}; first mode {mode}. "
        + " | ".join(f"[{f['mode']}] {f['unit']} {f['key'] or ''}: {f['detail']}" for f in findings)
    )[:_MAX_SUMMARY_CHARS]
    logger.info(
        "readiness-check %s: ready=%s settled=%s missing=%s failed=%s",
        collection, ready, settled, missing, failed,
    )
    return {
        "readiness": {
            "ready": ready,
            "settled": settled,
            "missing": missing,
            "failed": failed,
            "failure_mode": mode,
            "baseline": baseline.isoformat().replace("+00:00", "Z"),
            "summary": summary,
        }
    }


# ── public surface ───────────────────────────────────────────────────────────


def completion_check(event: dict, s3_client=None) -> dict:
    """The producer's claim (see `_completion_check`)."""
    return _completion_check(event, s3_client=s3_client)


def readiness_check(event: dict, s3_client=None) -> dict:
    """The consumer's question (see `_readiness_check`)."""
    return _readiness_check(event, s3_client=s3_client)


def parse_ts(value, *, where: str):
    return _parse_ts(value, where=where)


def unit_descriptors() -> dict[str, dict]:
    return _unit_descriptors()


def reset_unit_cache() -> None:
    """Drop the parsed descriptor cache (tests that edit a descriptor in place)."""
    global _UNITS_CACHE
    _UNITS_CACHE = None
