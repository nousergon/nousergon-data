"""Unregistered ``ops/checks/`` envelope sweep (alpha-engine-config-I11840).

WHY THIS EXISTS
---------------
The freshness monitor grades only keys that have an ``ARTIFACT_REGISTRY.yaml``
row. The fleet-checks contract (``nousergon_lib.fleet_check_result``) lets any
producer publish ``ops/checks/<id>/latest.json`` with no row, so a producer can
also STOP with no row to notice. ``ci_runner_mode_guard`` sat stale for 64 days
that way (2026-07-31 to 2026-10-02) and nothing paged. On 2026-10-03, 202 of the
213 live envelopes had no row.

The first fix was a 6-hourly GitHub workflow (alpha-engine-config-PR11925). That PR was closed
under Brian's ruling #25 (2026-10-14), which moved the sweep here instead: no GHA
minutes, and it uses the page path, dedup and escalation the monitor already has.

WHAT IT DOES
------------
On every DAILY sweep (never the intraday or historical modes), it lists
``ops/checks/*/latest.json``. For each key that no registry row names, it grades
the envelope's age against the cadence the envelope declares about itself:

    stale  <=>  age > max(STALE_FACTOR x cadence_minutes, floor(substrate))

* ``STALE_FACTOR`` is 2. That is one whole missed cycle on top of the console's
  ``staleness_factor 1.5``, so the console flags first and the monitor pages
  second.
* The floor keeps a 15-minute producer from paging on a short blip. It is
  ``DEFAULT_FLOOR_MINUTES`` (12h), or longer where the component's ``substrate``
  in the PUBLISHED observability registry is a host that legitimately sleeps
  (``SUBSTRATE_FLOOR_MINUTES``; a closed laptop gets 72h).
* ``cadence_minutes: null`` is how an EVENT-DRIVEN producer declares itself, so
  there is no clock to grade. It is reported as ``unclocked`` and never pages.

The whole set becomes ONE synthetic row, :data:`SWEEP_ARTIFACT_ID`, which
``_run_probe_pass`` treats like any registry row. That gives one line on the
digest page however many envelopes go stale (config#2855's alert storm), one
owning-item lookup (the 24-query budget is not spent per envelope, config-I7730),
and one episode. The row's ``last_modified`` is the newest last-write among the
stale envelopes. The episode key is built from it, so a NEW envelope going stale
opens a new episode and pages again, instead of folding silently into the open one.

The per-envelope grades are written to :data:`REPORT_KEY` on every daily sweep,
whatever the verdict. It sits under ``_freshness_monitor/``, which the Lambda can
already write, so the sweep needs no new IAM.

An unfinishable listing is ``probe_failed``, never ``fresh``. "Could not look"
must not read as "nothing found".
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import yaml

from nousergon_lib.artifact_freshness import ArtifactSpec, CheckResult

logger = logging.getLogger(__name__)

CHECKS_PREFIX = "ops/checks/"
ENVELOPE_NAME = "latest.json"
OBSERVABILITY_PREFIX = "ops/registry/observability.d/"

SWEEP_ARTIFACT_ID = "unregistered_envelope_staleness"
REPORT_KEY = "_freshness_monitor/unregistered_envelopes.json"
#: The daily cron (``cron(0 12 * * ? *)``) this sweep rides.
SWEEP_INTERVAL_MINUTES = 1440

STALE_FACTOR = 2
DEFAULT_FLOOR_MINUTES = 720
#: Substrates whose host legitimately stops publishing, keyed on the
#: observability registry's ``substrate:`` value.
SUBSTRATE_FLOOR_MINUTES = {"laptop-launchd": 4320}

#: Concurrent GETs. boto3's default connection pool is 10.
_FETCH_WORKERS = 10

STATE_FRESH = "fresh"
STATE_STALE = "stale"
STATE_UNCLOCKED = "unclocked"


@dataclass(frozen=True)
class Envelope:
    """What the sweep needs from one ``ops/checks/<id>/latest.json``."""

    check_id: str
    key: str
    last_modified: datetime
    ran_at: datetime | None
    cadence_minutes: float | None
    status: str | None


@dataclass(frozen=True)
class Grade:
    check_id: str
    state: str
    age_minutes: int
    threshold_minutes: int | None
    cadence_minutes: float | None
    substrate: str | None
    last_seen: str
    detail: str


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def envelope_from(key: str, last_modified: datetime, body: bytes | None) -> Envelope:
    """Parse one envelope. Never raises: an unreadable or unparseable body is
    graded on its S3 ``LastModified`` with no cadence, which is ``unclocked``."""
    doc: dict = {}
    if body is not None:
        try:
            loaded = json.loads(body)
            doc = loaded if isinstance(loaded, dict) else {}
        except (ValueError, UnicodeDecodeError):
            doc = {}
    cadence = doc.get("cadence_minutes")
    if isinstance(cadence, bool) or not isinstance(cadence, (int, float)) or cadence <= 0:
        cadence = None
    status = doc.get("status")
    return Envelope(
        check_id=key[len(CHECKS_PREFIX):].split("/", 1)[0],
        key=key,
        last_modified=_utc(last_modified),
        ran_at=_parse_ts(doc.get("ran_at")),
        cadence_minutes=cadence,
        status=status if isinstance(status, str) else None,
    )


def registered_keys(specs: Iterable[ArtifactSpec]) -> set[str]:
    """Every ``ops/checks/`` key a registry row names. Those keys are graded
    by their own row, so this sweep skips them."""
    return {
        s.s3_key_template for s in specs
        if s.s3_key_template.startswith(CHECKS_PREFIX)
    }


def substrate_map(rows: Iterable[Any]) -> dict[str, str]:
    """``component_id`` and every ``alias_ids`` entry -> ``substrate``."""
    out: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("substrate"):
            continue
        for cid in [row.get("component_id"), *(row.get("alias_ids") or [])]:
            if cid:
                out[str(cid)] = str(row["substrate"])
    return out


def threshold_minutes(cadence: float, substrate: str | None) -> int:
    floor = SUBSTRATE_FLOOR_MINUTES.get(substrate or "", DEFAULT_FLOOR_MINUTES)
    return int(max(STALE_FACTOR * cadence, floor))


def grade(
    envelopes: Iterable[Envelope],
    *,
    registered: set[str],
    substrates: dict[str, str],
    now: datetime,
) -> list[Grade]:
    """Grade every UNREGISTERED envelope. Pure; the tests drive this."""
    grades: list[Grade] = []
    for env in sorted(envelopes, key=lambda e: e.check_id):
        if env.key in registered:
            continue
        seen = env.ran_at or env.last_modified
        age = int((now - seen).total_seconds() // 60)
        substrate = substrates.get(env.check_id)
        if env.cadence_minutes is None:
            grades.append(Grade(
                env.check_id, STATE_UNCLOCKED, age, None, None, substrate,
                seen.isoformat(),
                "no cadence_minutes: event-driven by declaration, or not a "
                "fleet_check_result envelope",
            ))
            continue
        limit = threshold_minutes(env.cadence_minutes, substrate)
        if age <= limit:
            grades.append(Grade(
                env.check_id, STATE_FRESH, age, limit, env.cadence_minutes,
                substrate, seen.isoformat(), "within threshold",
            ))
            continue
        grades.append(Grade(
            env.check_id, STATE_STALE, age, limit, env.cadence_minutes, substrate,
            seen.isoformat(),
            f"last written {age // 60}h ago, threshold {limit // 60}h, "
            f"last status {env.status!r}",
        ))
    return grades


def sweep_spec(bucket: str) -> ArtifactSpec:
    """The synthetic registry row the sweep's verdict travels under.

    ``critical``: alpha-engine-config-I11840's closes-when is a page within one day of an
    envelope going stale, and the warning ladder (``WARNING_ESCALATION_RUNS``
    consecutive daily sweeps) could not meet that. ``s3_key_template`` names
    the report this sweep writes, which is the artifact the row describes.
    """
    return ArtifactSpec(
        artifact_id=SWEEP_ARTIFACT_ID,
        s3_bucket=bucket,
        s3_key_template=REPORT_KEY,
        cadence="continuous",
        sla_minutes_after_cron=0,
        severity="critical",
        owner_repo="nousergon-data",
        created_at=date(2026, 10, 15),
        grace_period_cycles=0,
        calendar_aware=False,
        interval_minutes=SWEEP_INTERVAL_MINUTES,
        run_calendar="all_days",
    )


def verdict(grades: list[Grade], now: datetime) -> CheckResult:
    """Fold the grades into the one result the synthetic row carries."""
    stale = [g for g in grades if g.state == STATE_STALE]
    unclocked = sum(1 for g in grades if g.state == STATE_UNCLOCKED)
    counts = (f"{len(grades)} unregistered envelope(s): {len(stale)} stale, "
              f"{unclocked} unclocked")
    if not stale:
        return CheckResult(
            state="fresh", last_modified=now, reason=counts,
            canonical_key=REPORT_KEY, observed_key=REPORT_KEY,
        )
    newest = max(_parse_ts(g.last_seen) for g in stale)
    named = "; ".join(f"{g.check_id} ({g.detail})" for g in stale)
    return CheckResult(
        state="stale",
        last_modified=newest,
        sla_violated_by_minutes=max(
            g.age_minutes - (g.threshold_minutes or 0) for g in stale
        ),
        reason=f"{len(stale)} unregistered ops/checks envelope(s) STALE: {named}. "
               f"{counts}. Per-envelope grades: {REPORT_KEY}",
        canonical_key=REPORT_KEY,
        observed_key=REPORT_KEY,
    )


# ── I/O ─────────────────────────────────────────────────────────────────────


def list_envelopes(s3: Any, bucket: str) -> list[Envelope]:
    """Every ``ops/checks/<id>/latest.json`` with its body. Raises on a failed
    listing, because a partial inventory must not read as a clean one."""
    keys: list[tuple[str, datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=CHECKS_PREFIX,
    ):
        for obj in page.get("Contents") or []:
            parts = obj["Key"][len(CHECKS_PREFIX):].split("/")
            if len(parts) == 2 and parts[1] == ENVELOPE_NAME:
                keys.append((obj["Key"], obj["LastModified"]))

    def fetch(item: tuple[str, datetime]) -> Envelope:
        key, last_modified = item
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception:  # noqa: BLE001 — one unreadable body grades on LastModified (unclocked)
            logger.warning("could not read %s; grading it on LastModified", key)
            body = None
        return envelope_from(key, last_modified, body)

    with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        return list(pool.map(fetch, keys))


def load_observability_rows(s3: Any, bucket: str) -> list[dict]:
    """The observability registry as PUBLISHED, which is the copy the console reads."""
    keys = [
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=OBSERVABILITY_PREFIX,
        )
        for obj in page.get("Contents") or []
        if obj["Key"].endswith((".yaml", ".yml"))
    ]

    def fetch(key: str) -> Any:
        try:
            return yaml.safe_load(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except Exception:  # noqa: BLE001 — a bad row loses its floor and falls to the default
            logger.warning("could not read observability row %s", key)
            return None

    with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        return [r for r in pool.map(fetch, keys) if isinstance(r, dict)]


def run_sweep(
    s3: Any,
    bucket: str,
    specs: list[ArtifactSpec],
    now: datetime,
) -> tuple[ArtifactSpec, CheckResult, dict[str, Any]]:
    """Sweep, and return ``(spec, result, report)``. Never raises.

    A failed listing is ``probe_failed`` (it pages, and has no grace). An
    unreadable observability registry downgrades only the floors: every
    envelope gets the default floor, which leans toward paging, and the
    report says so.
    """
    spec = sweep_spec(bucket)
    notes: list[str] = []
    try:
        envelopes = list_envelopes(s3, bucket)
    except Exception as exc:  # noqa: BLE001 — cannot-look is a probe failure, never all-clear
        reason = (f"could not list s3://{bucket}/{CHECKS_PREFIX} "
                  f"({type(exc).__name__}: {exc})")
        logger.error("unregistered-envelope sweep: %s", reason)
        result = CheckResult(state="probe_failed", reason=reason,
                             canonical_key=REPORT_KEY)
        return spec, result, {
            "schema_version": 1, "run_at": now.isoformat(),
            "state": result.state, "summary": reason, "grades": [], "notes": [],
        }
    try:
        substrates = substrate_map(load_observability_rows(s3, bucket))
    except Exception as exc:  # noqa: BLE001 — fail toward paging: default floor for every row
        substrates = {}
        notes.append(f"observability registry unreadable ({type(exc).__name__}); "
                     "every envelope graded on the default floor")

    grades = grade(envelopes, registered=registered_keys(specs),
                   substrates=substrates, now=now)
    result = verdict(grades, now)
    report = {
        "schema_version": 1,
        "run_at": now.isoformat(),
        "state": result.state,
        "summary": result.reason,
        "stale_factor": STALE_FACTOR,
        "default_floor_minutes": DEFAULT_FLOOR_MINUTES,
        "substrate_floor_minutes": SUBSTRATE_FLOOR_MINUTES,
        "grades": [asdict(g) for g in grades],
        "notes": notes,
    }
    logger.info("unregistered-envelope sweep: %s", result.reason)
    return spec, result, report
