"""Monthly threshold-tightening drill for the ``freshness-monitor`` detector
(`alpha-engine-config-I10795`, Brian's ruling of 2026-10-14: "run the
threshold-tightening drill as one scripted monthly workflow").

Writes one commissioning receipt per unit that declares a
``freshness-monitor`` detector, at
``data_collection/commissioning/<unit_id>/freshness-monitor/latest.json``
(schema ``data_detector_commissioning.v1``, read by
:mod:`data_gate.detector_readers`). ``observability-policy`` §9.1 asks for
three recorded facts: the condition was INDUCED, the detector FIRED on the
right subject, and it STOOD DOWN when the condition cleared.

**How the condition is induced without touching any data.** The monitor's
verdict for a registry row is
:func:`nousergon_lib.artifact_freshness.check_freshness`, which compares the
newest instance's ``LastModified`` against a FRESHNESS FLOOR
(``_freshness_floor``: ~2 trading days for ``weekday_sf``/``eod_sf``, 10 days
for ``saturday_sf``, ``interval + sla`` for ``continuous``). The drill:

1. **Baseline** — runs the real ``check_freshness`` on the LIVE published row
   (``_freshness_monitor/ARTIFACT_REGISTRY.yaml``, the copy the Lambda reads)
   against live S3 metadata. The row must read ``fresh``; a row already
   alerting cannot attribute a fire to the drill, so the receipt says so.
2. **Induce** — tightens that one threshold to zero max-age (the floor becomes
   the probe instant) and drops the row's ``deadline_local`` re-grade for that
   one probe (the re-grade can only re-admit an on-time row, never page it).
   Everything else — key resolution, LIST/HEAD, the holiday and grace gates,
   classification — is the monitor's own code, unchanged.
3. **Fired** — that probe must classify the row ``stale``, an alerting state
   the monitor pages on (``index._ALERTING_STATES``), naming the row's
   ``artifact_id``: the subject the reader checks against the unit's
   ``registry_rows``.
4. **Cleared** — the threshold is restored and the row must read ``fresh``
   again.

Nothing is written to any artifact, no registry is edited, and no page is
sent: the only writes are the receipts and this producer's run record. What
the receipt proves is that the deployed detector's classification path fires
on the right subject when its threshold is crossed and stands down when it is
not — measured on live evidence each month. Page DELIVERY is a separate
property with its own detectors (the digest path, the heartbeat).

**Honest outcomes only.** A unit whose drill does not complete gets a receipt
with ``outcome: "inconclusive"`` and the reason — the reader grades that
UNMET, which is what it is, and a previous month's ``induced`` receipt can
never outlive a failed drill. A unit with nothing drillable (no
``registry_rows`` subject, or only ``event_driven`` rows, whose own freshness
is never graded on age) gets NO receipt and is listed in the run record: a
receipt the reader cannot check would turn an UNMET row UNMEASURABLE.

**Cadence.** ``--only-if-due`` makes the step a no-op (no AWS call at all)
except on the first NYSE trading day of the calendar month, so it rides an
existing daily job in ``phase-exit-metrics.yml`` instead of a new workflow.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import json
import logging
import os
import sys
from typing import Any, Callable, Iterator

import yaml

from data_gate.descriptors import Unit, load_units
from data_gate.detector_readers import RECEIPT_SCHEMA, expected_subjects, receipt_key

__all__ = [
    "DETECTOR_KIND",
    "PRODUCER",
    "SPEC_FIELDS",
    "build_specs",
    "drill_row",
    "drill_unit",
    "is_drill_day",
    "main",
    "run_drill",
    "tightened_threshold",
]

logger = logging.getLogger(__name__)

PRODUCER = "freshness_commissioning_drill"
DETECTOR_KIND = "freshness-monitor"

DEFAULT_STORE = "s3://alpha-engine-research/data_collection"
DEFAULT_REGISTRY = "s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml"
_REGION = "us-east-1"

#: The ``ArtifactSpec`` keys the freshness-monitor Lambda threads from a
#: registry row (``infrastructure/lambdas/freshness-monitor/index.py::
#: _SPEC_FIELDS``). Mirrored, not imported: importing the Lambda module pulls
#: its notifier, and a drill must never be one import away from a real page.
#: ``tests/test_freshness_commissioning_drill.py`` asserts the two sets are
#: equal, so a field the monitor starts honouring cannot be silently dropped
#: here.
SPEC_FIELDS = frozenset(
    {
        "artifact_id",
        "s3_bucket",
        "s3_key_template",
        "cadence",
        "sla_minutes_after_cron",
        "severity",
        "owner_repo",
        "created_at",
        "grace_period_cycles",
        "recovery_key_template",
        "calendar_aware",
        "interval_minutes",
        "run_calendar",
        "active_hours_utc",
        "produces",
        "depends_on",
        "liveness_via",
        "absence_expected",
        "deadline_local",
    }
)

#: The verdict a tightened floor must produce on a row that has an instance.
#: ``missing`` would mean the instance vanished mid-drill and ``probe_failed``
#: that the monitor went blind — neither is the induced condition firing.
_FIRED_STATE = "stale"
_CLEARED_STATE = "fresh"


# ---------------------------------------------------------------------------
# Cadence.
# ---------------------------------------------------------------------------


def is_drill_day(today: dt.date) -> bool:
    """True on the first NYSE trading day of ``today``'s calendar month."""
    from nousergon_lib.trading_calendar import is_trading_day  # noqa: PLC0415

    if not is_trading_day(today):
        return False
    day = today.replace(day=1)
    while day < today:
        if is_trading_day(day):
            return False
        day += dt.timedelta(days=1)
    return True


# ---------------------------------------------------------------------------
# Registry -> specs, exactly as the monitor builds them.
# ---------------------------------------------------------------------------


def _coerce_date(value: Any) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def build_specs(document: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """``({artifact_id: ArtifactSpec}, {artifact_id: why it could not be built})``.

    Same merge as the monitor's ``load_registry_with_recovery``: the
    ``defaults`` block under each row, ``created_at`` coerced, and only
    :data:`SPEC_FIELDS` passed to the constructor. A row that fails to
    construct is recorded, never skipped silently.
    """
    from nousergon_lib.artifact_freshness import ArtifactSpec  # noqa: PLC0415

    if not isinstance(document, dict) or not isinstance(document.get("artifacts"), list):
        raise ValueError("registry document carries no `artifacts:` list")
    defaults = document.get("defaults") or {}
    specs: dict[str, Any] = {}
    broken: dict[str, str] = {}
    for entry in document["artifacts"]:
        if not isinstance(entry, dict):
            continue
        merged = {**defaults, **entry}
        artifact_id = str(merged.get("artifact_id") or "")
        try:
            merged["created_at"] = _coerce_date(merged["created_at"])
            specs[artifact_id] = ArtifactSpec(**{k: v for k, v in merged.items() if k in SPEC_FIELDS})
        except Exception as exc:  # noqa: BLE001 - recorded per row; the monitor would fail the same row
            broken[artifact_id] = f"{type(exc).__name__}: {exc}"
    return specs, broken


# ---------------------------------------------------------------------------
# The induction.
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def tightened_threshold() -> Iterator[None]:
    """Move the monitor's freshness floor to the probe instant (zero max-age).

    Patches ``nousergon_lib.artifact_freshness._freshness_floor`` for the
    duration of one probe and restores it on exit, exception or not. A pinned
    library that no longer has that seam raises here — the drill then fails
    loudly instead of reporting a fire it never induced.
    """
    from nousergon_lib import artifact_freshness  # noqa: PLC0415

    original = getattr(artifact_freshness, "_freshness_floor", None)
    if not callable(original):
        raise RuntimeError(
            "nousergon_lib.artifact_freshness._freshness_floor is gone: the threshold this drill "
            "tightens has moved, so no induction is possible until the drill follows it"
        )

    def _zero_max_age(spec: Any, now_utc: dt.datetime, cycle_tick: dt.datetime) -> dt.datetime:
        del spec, cycle_tick
        return now_utc

    artifact_freshness._freshness_floor = _zero_max_age
    try:
        yield
    finally:
        artifact_freshness._freshness_floor = original


def _iso(instant: dt.datetime) -> str:
    return instant.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclasses.dataclass
class RowDrill:
    """One registry row taken through baseline -> induce -> fire -> clear."""

    artifact_id: str
    commissioned: bool
    problem: str | None = None
    induced: dict[str, str] | None = None
    fired: dict[str, str] | None = None
    cleared: dict[str, str] | None = None
    states: dict[str, str] = dataclasses.field(default_factory=dict)
    #: The row is ``event_driven``: nothing to tighten. A unit whose every
    #: subject is one gets no receipt rather than an inconclusive one.
    event_driven: bool = False

    def summary(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "commissioned": self.commissioned,
            "problem": self.problem,
            "states": dict(self.states),
        }


def drill_row(
    spec: Any,
    *,
    s3: Any,
    clock: Callable[[], dt.datetime],
    run_ref: str,
    check: Callable[..., Any] | None = None,
) -> RowDrill:
    """Baseline, induce, fire, clear — one row, against live S3 metadata."""
    if check is None:
        from nousergon_lib.artifact_freshness import check_freshness as check  # noqa: PLC0415

    drill = RowDrill(artifact_id=spec.artifact_id, commissioned=False, event_driven=spec.cadence == "event_driven")
    if drill.event_driven:
        drill.problem = (
            "event_driven row: the monitor never grades its own freshness on age (liveness is "
            f"delegated to {spec.liveness_via!r}), so no threshold exists to tighten"
        )
        return drill

    baseline = check(s3, spec, clock())
    drill.states["baseline"] = baseline.state
    if baseline.state != _CLEARED_STATE:
        drill.problem = (
            f"baseline reads {baseline.state!r}, not {_CLEARED_STATE!r} ({baseline.reason}); a row "
            "that is not fresh before the induction cannot attribute a fire to it"
        )
        return drill

    induced_at = clock()
    induced_spec = dataclasses.replace(spec, deadline_local=None) if getattr(spec, "deadline_local", None) else spec
    with tightened_threshold():
        tightened = check(s3, induced_spec, clock())
    fired_at = clock()
    drill.states["induced"] = tightened.state
    condition = (
        f"freshness threshold for registry row {spec.artifact_id} ({spec.cadence}) tightened to "
        "zero max-age (floor = probe instant) for one check_freshness probe against live S3 "
        "metadata; no artifact or registry row was modified"
    )
    if tightened.state != _FIRED_STATE:
        drill.problem = (
            f"with its threshold tightened the row read {tightened.state!r}, not {_FIRED_STATE!r} "
            f"({tightened.reason}); the detector did not fire on the induced condition"
        )
        return drill

    restored = check(s3, spec, clock())
    cleared_at = clock()
    drill.states["cleared"] = restored.state
    if restored.state != _CLEARED_STATE:
        drill.problem = (
            f"with its threshold restored the row read {restored.state!r}, not {_CLEARED_STATE!r} "
            f"({restored.reason}); no stand-down was observed"
        )
        return drill
    if not induced_at <= fired_at < cleared_at:
        drill.problem = (
            f"clock did not advance through the drill (induced {_iso(induced_at)}, fired "
            f"{_iso(fired_at)}, cleared {_iso(cleared_at)}); the order cannot be recorded honestly"
        )
        return drill

    drill.commissioned = True
    drill.induced = {"at": _iso(induced_at), "condition": condition, "row": spec.artifact_id}
    drill.fired = {
        "at": _iso(fired_at),
        "subject": spec.artifact_id,
        "evidence": f"check_freshness -> {tightened.state}: {tightened.reason} ({run_ref})",
    }
    drill.cleared = {
        "at": _iso(cleared_at),
        "evidence": f"threshold restored; check_freshness -> {restored.state}: {restored.reason} ({run_ref})",
    }
    return drill


def _freshness_detectors(unit: Unit) -> list[dict[str, Any]]:
    return [d for d in unit.raw.get("detectors") or [] if isinstance(d, dict) and d.get("kind") == DETECTOR_KIND]


def drill_unit(
    unit: Unit,
    detector: dict[str, Any],
    specs: dict[str, Any],
    *,
    s3: Any,
    clock: Callable[[], dt.datetime],
    run_ref: str,
    check: Callable[..., Any] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
    """``(receipt or None, per-row summaries, why no receipt)``.

    Rows are tried in descriptor order until one commissions; a single
    commissioned row is the evidence the reader needs.
    """
    subjects = expected_subjects(unit, detector)
    if not subjects:
        return None, [], "the descriptor names no registry_rows subject this detector could fire on"
    rows: list[RowDrill] = []
    for subject in subjects:
        spec = specs.get(subject)
        if spec is None:
            rows.append(RowDrill(artifact_id=subject, commissioned=False, problem="not in the published registry"))
            continue
        row = drill_row(spec, s3=s3, clock=clock, run_ref=run_ref, check=check)
        rows.append(row)
        if row.commissioned:
            break
    summaries = [r.summary() for r in rows]
    if all(r.event_driven for r in rows):
        return None, summaries, "no drillable row: " + "; ".join(f"{r.artifact_id}: {r.problem}" for r in rows)

    receipt: dict[str, Any] = {
        "schema": RECEIPT_SCHEMA,
        "unit_id": unit.unit_id,
        "detector": {"kind": DETECTOR_KIND, "via": detector.get("via")},
        "method": "threshold-tightening drill (alpha-engine-config-I10795, ruling 2026-10-14)",
        "producer": PRODUCER,
        "run": run_ref,
        "rows": summaries,
    }
    done = next((r for r in rows if r.commissioned), None)
    if done is None:
        receipt["outcome"] = "inconclusive"
        receipt["problem"] = "; ".join(f"{r.artifact_id}: {r.problem}" for r in rows)
    else:
        receipt.update(outcome="induced", induced=done.induced, fired=done.fired, cleared=done.cleared)
    return receipt, summaries, None


def run_drill(
    units: list[Unit],
    specs: dict[str, Any],
    *,
    s3: Any,
    clock: Callable[[], dt.datetime],
    run_ref: str,
    check: Callable[..., Any] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    """``({receipt_key: receipt}, {unit_id: why no receipt})`` for every
    non-retired unit declaring a ``freshness-monitor`` detector."""
    receipts: dict[str, dict[str, Any]] = {}
    skipped: dict[str, str] = {}
    for unit in units:
        if unit.retired:
            continue
        for detector in _freshness_detectors(unit):
            receipt, _rows, why = drill_unit(
                unit, detector, specs, s3=s3, clock=clock, run_ref=run_ref, check=check
            )
            if receipt is None:
                skipped[unit.unit_id] = str(why)
            else:
                receipts[receipt_key(unit, DETECTOR_KIND)] = receipt
    return receipts, skipped


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _run_ref() -> str:
    server = os.environ.get("GITHUB_SERVER_URL")
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return "local run (no GITHUB_RUN_ID)"


def _load_registry(uri: str, s3: Any) -> dict[str, Any]:
    if uri.startswith("s3://"):
        bucket, _, key = uri[len("s3://") :].partition("/")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        with open(uri[len("file://") :] if uri.startswith("file://") else uri, "rb") as handle:
            body = handle.read()
    return yaml.safe_load(body)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--store", default=DEFAULT_STORE, help="s3://bucket/prefix or a local directory")
    ap.add_argument("--registry", default=DEFAULT_REGISTRY, help="published registry (s3:// or a path)")
    ap.add_argument("--region", default=_REGION)
    ap.add_argument(
        "--only-if-due",
        action="store_true",
        help="exit 0 without touching AWS unless today (UTC) is the month's first NYSE trading day",
    )
    ap.add_argument("--no-write", action="store_true", help="print the receipts instead of writing them")
    args = ap.parse_args(argv)

    started_at = dt.datetime.now(dt.timezone.utc)
    if args.only_if_due and not is_drill_day(started_at.date()):
        print(f"{PRODUCER}: not due on {started_at.date()} (runs on the month's first NYSE trading day)")
        return 0

    import boto3  # noqa: PLC0415 - deferred so the module imports without AWS

    from data_gate.producers._run_record import RUN_RECORD_BUCKET, write_run_record  # noqa: PLC0415
    from data_gate.producers.slo import _StoreS3Adapter  # noqa: PLC0415
    from data_gate.store import open_store  # noqa: PLC0415

    s3 = boto3.client("s3", region_name=args.region)
    store = open_store(args.store, dry_run=args.no_write)
    recorder = _StoreS3Adapter(store)
    clock = lambda: dt.datetime.now(dt.timezone.utc)  # noqa: E731 - injected seam
    try:
        specs, broken = build_specs(_load_registry(args.registry, s3))
        receipts, skipped = run_drill(load_units(), specs, s3=s3, clock=clock, run_ref=_run_ref())
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                recorder,
                bucket=RUN_RECORD_BUCKET,
                producer=PRODUCER,
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=f"{type(exc).__name__}: {exc}",
            )
        raise

    inconclusive = sorted(r["unit_id"] for r in receipts.values() if r["outcome"] != "induced")
    commissioned = sorted(r["unit_id"] for r in receipts.values() if r["outcome"] == "induced")
    # Unit ids and verdicts only: this repository's Actions logs are public.
    print(f"{PRODUCER}: commissioned={len(commissioned)} {commissioned}")
    print(f"{PRODUCER}: inconclusive={len(inconclusive)} {inconclusive}")
    print(f"{PRODUCER}: no receipt (nothing drillable)={len(skipped)} {sorted(skipped)}")
    if broken:
        print(f"{PRODUCER}: {len(broken)} registry row(s) failed to build a spec: {sorted(broken)}")

    if args.no_write:
        print(json.dumps({"receipts": receipts, "skipped": skipped}, indent=2, sort_keys=True))
        return 1 if inconclusive else 0

    for key, receipt in sorted(receipts.items()):
        store.put_bytes(key, json.dumps(receipt, sort_keys=True).encode("utf-8"))
    write_run_record(
        recorder,
        bucket=RUN_RECORD_BUCKET,
        producer=PRODUCER,
        status="error" if inconclusive else "ok",
        started_at=started_at,
        finished_at=dt.datetime.now(dt.timezone.utc),
        error=(f"inconclusive drill for {inconclusive}" if inconclusive else None),
        detail={
            "receipts_written": len(receipts),
            "commissioned": commissioned,
            "inconclusive": inconclusive,
            "skipped": skipped,
        },
    )
    return 1 if inconclusive else 0


if __name__ == "__main__":
    sys.exit(main())
