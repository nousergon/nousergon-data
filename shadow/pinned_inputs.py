"""Read a replayed day's MUTABLE INPUTS as that day's run actually saw them.

`alpha-engine-config-I11216`. A shadow replay of trading day D re-reads the
inputs v1 read, but some of those inputs are overwritten in place, so a replay
run on D+N reads D+N's version of them. Measured on the 2026-09-18 replay,
dispatched 2026-09-20:

    market_data/earnings/latest.json      live 74 symbols, shadow 60
    market_data/sectors/latest.json       live 75,         shadow 61
    market_data/analyst/latest.json       live 75,         shadow 60
    market_data/fundamentals/latest.json  live 81,         shadow 66

The SAME 15 symbols were missing from all four -- artifacts with nothing in
common except their input. `metron/holdings_universe.json` is rewritten about
three times a day, and its S3 version history says exactly what happened:

    2026-09-20T22:39:31Z  as_of=2026-09-20  n= 75  of-the-15:  0   <- the replay
    2026-09-18T20:51:48Z  as_of=2026-09-18  n= 75  of-the-15:  0
    2026-09-17T22:38:26Z  as_of=2026-09-17  n=117  of-the-15: 15   <- what v1 read

v1's reference collector ran during the 09-18 morning leg, when the newest
published universe was still 09-17's 117 instruments. Brian's holdings changed
that afternoon. The replay had no way to know which universe the day it was
replaying had actually used, so it graded a 75-instrument world against a
117-instrument one and called the difference a parity failure.

THE PRECEDENT IS ALREADY IN THIS REPO, on the grading side. `shadow/parity.py`
grades every key against the object VERSION v1's run manifest recorded for the
trading day (`alpha-engine-config-I10892`), falling back to the recorded
VersionId when the current ETag no longer matches, and naming `live_superseded`
when neither can be read. This module is the same idea one step earlier: the
PRODUCER side of a replay should read the same input versions too.

TWO SOURCES, IN ORDER, AND THE REPORT SAYS WHICH WAS USED:

1.  DECLARED -- the VersionId v1 recorded under `inputs` in its run manifest
    for that trading day. Exact, and the durable record. `UnitRun.inputs` has
    always existed; until this change no collector populated it, so every
    manifest carries `inputs: []`.

2.  INFERRED -- the version that was current at the instant v1's manifest says
    the run STARTED, selected from `list_object_versions`. Correct for any day
    still inside the bucket's 30-day noncurrent retention, and it is what makes
    already-collected days (2026-09-14, 2026-09-18) replayable at all, since
    neither has declared inputs and neither ever will.

The distinction is never silent: `pin_for` returns the basis alongside the
version, the caller logs it, and an inferred pin is a weaker claim that a
reader can see is weaker. An UNPINNED read is weaker still and also says so.

Absence is not an error. Outside a replay there is nothing to pin and every
function here answers `None` immediately -- production reads the current
object, which is the whole point of production.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: How a pin was arrived at. Carried into the log line and, for a collector
#: that records its inputs, into the manifest -- an inferred pin must never be
#: readable as a declared one.
PIN_BASES = frozenset({"declared", "inferred", "unpinned"})


@dataclass(frozen=True)
class Pin:
    """One input object's resolved version for a replayed trading day."""

    key: str
    version_id: str | None
    basis: str
    detail: str

    @property
    def is_pinned(self) -> bool:
        return self.version_id is not None

    def as_input_record(self) -> dict[str, Any]:
        """The shape `UnitRun.inputs` takes, for a collector recording reads."""
        return {
            "key": self.key,
            "version_id": self.version_id,
            "version_capture": "pinned_replay" if self.is_pinned else "not_captured",
            "pin_basis": self.basis,
            "pin_detail": self.detail,
        }


def active_shadow_root():
    """The active shadow root, or ``None`` in an ordinary production run.

    Imported lazily and defensively, for the reason
    `builders/daily_append.py::_active_shadow_root` states: production
    collectors import the modules that call this and have no reason to carry
    the shadow package, whose `__init__` pulls in the writer-side interceptor.
    ``None`` is the production answer and the safe direction -- read the
    current object.
    """
    try:
        from shadow.root import active_root
    except Exception:  # pragma: no cover - production has no shadow concern
        return None
    return active_root()


def _manifests_for(s3_client, bucket: str, unit_id: str, trading_day: dt.date) -> list[dict]:
    """Every v1 run manifest for one unit on one trading day, newest last."""
    prefix = f"data_collection/runs/{unit_id}/{trading_day.isoformat()}/"
    try:
        page = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    except Exception as exc:  # noqa: BLE001 - an unreadable manifest is "no pin", named
        logger.warning("[pinned_inputs] cannot list %s (%s) — no declared pin", prefix, exc)
        return []
    out: list[dict] = []
    for item in sorted(page.get("Contents") or [], key=lambda c: c["Key"]):
        try:
            out.append(json.loads(s3_client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()))
        except Exception as exc:  # noqa: BLE001 - one bad manifest is not the others' problem
            logger.warning("[pinned_inputs] manifest %s unreadable (%s)", item["Key"], exc)
    return out


def _declared(manifests: list[dict], key: str) -> str | None:
    """The VersionId a manifest DECLARES it read for ``key``, newest wins."""
    found: str | None = None
    for manifest in manifests:
        for record in manifest.get("inputs") or []:
            if record.get("key") == key and record.get("version_id"):
                found = str(record["version_id"])
    return found


def _run_started(manifests: list[dict]) -> dt.datetime | None:
    """The EARLIEST start across a unit's manifests for the day.

    Earliest, not latest: the inference below asks "what did this run see when
    it began", and a unit that ran twice read its inputs at the first start.
    Selecting the later one would pin a version the first run never saw.
    """
    starts: list[dt.datetime] = []
    for manifest in manifests:
        raw = manifest.get("started")
        if not raw:
            continue
        try:
            parsed = dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        starts.append(parsed)
    return min(starts) if starts else None


def _current_at(s3_client, bucket: str, key: str, moment: dt.datetime) -> str | None:
    """The VersionId that was the current object at ``moment``.

    That is the newest version whose LastModified is at or before it. A key
    whose every retained version is NEWER than the moment answers ``None``:
    the version v1 read has aged out of the 30-day noncurrent window, and
    saying so is the honest answer. Substituting the oldest surviving version
    would be a guess wearing a pin's clothes.
    """
    try:
        paginator = s3_client.get_paginator("list_object_versions")
        candidates: list[tuple[dt.datetime, str]] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            for version in page.get("Versions") or []:
                if version.get("Key") != key:
                    continue  # Prefix is a prefix; siblings share it
                candidates.append((version["LastModified"], str(version["VersionId"])))
    except Exception as exc:  # noqa: BLE001 - an unlistable key is "no pin", named
        logger.warning("[pinned_inputs] cannot list versions of %s (%s)", key, exc)
        return None
    eligible = [(when, vid) for when, vid in candidates if when <= moment]
    if not eligible:
        return None
    return max(eligible, key=lambda pair: pair[0])[1]


def pin_for(
    s3_client,
    bucket: str,
    key: str,
    *,
    unit_id: str,
    trading_day: dt.date | None = None,
) -> Pin:
    """Which version of ``key`` a replay of ``trading_day`` should read.

    Outside a replay (and whenever ``trading_day`` cannot be established) the
    answer is an explicit UNPINNED result, not an exception: production reads
    the current object by design.
    """
    if trading_day is None:
        root = active_shadow_root()
        if root is None:
            return Pin(key, None, "unpinned", "no shadow replay active — reading the current object")
        trading_day = root.trading_day

    manifests = _manifests_for(s3_client, bucket, unit_id, trading_day)
    declared = _declared(manifests, key)
    if declared:
        return Pin(key, declared, "declared", f"{unit_id} run manifest for {trading_day} recorded this version")

    started = _run_started(manifests)
    if started is None:
        return Pin(
            key,
            None,
            "unpinned",
            f"no {unit_id} run manifest for {trading_day} declares an input or a start time",
        )
    inferred = _current_at(s3_client, bucket, key, started)
    if inferred is None:
        return Pin(
            key,
            None,
            "unpinned",
            (
                f"no retained version of {key} predates {unit_id}'s {started.isoformat()} start — "
                "the version v1 read has aged out of the bucket's 30-day noncurrent window"
            ),
        )
    return Pin(
        key,
        inferred,
        "inferred",
        f"the version current at {unit_id}'s {started.isoformat()} start; v1 declared no input",
    )
