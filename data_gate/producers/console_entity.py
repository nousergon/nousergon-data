"""Producer for the 42 ``data.<unit>.console_entity`` clauses
(`alpha-engine-config-I10795`, audit gap A3 part 2 — the receipt WRITER).

`data_gate/console_entity_readers.py` grades one console reachability receipt
per unit at ``console_entity/<unit_id>/latest.json`` under
``s3://alpha-engine-research/data_collection``. Until this module, nothing
wrote one, so all 40 graded rows read UNMET "no receipt". This is the writer.

**It runs on the console box, against the console that is actually serving.**
For each unit descriptor it asks the live console the question the clause
grades — ``GET /doctor/<component_id>`` with ``Accept: application/json``,
`nousergon-console`'s ``console.diagnose.doctor`` rendered by ``as_dict`` —
and files the answer as the receipt. The gate runs on GitHub Actions and cannot
reach the console, so the receipt is what crosses that boundary. It holds the
console's own words, never a re-derivation: a writer that re-implemented the
chain would grade its own reimplementation, not the surface.

**The unit set is DERIVED from the descriptors** (`load_units`), never
hand-listed (`observability-policy` §2.2) — a unit added under
``registry.d/units/`` gets a receipt on the next run with no edit here.

**All-or-nothing, and nothing is ever invented.** The writer asks for every
unit first and writes only if every answer came back as a doctor payload from a
built, current index:

* the console is down, answers non-200, or answers something that is not a
  ``doctor`` payload: nothing is written, the run record says ``error``;
* the console's index is still the bootstrap index or is stale: nothing is
  written. A diagnosis of an index that stopped rebuilding is a statement about
  the past, and filing it with today's ``checked_at`` would launder it current.

In both cases the previous receipts age past the reader's 26 h bound and the
clauses go UNMET by themselves — the honest outcome. A partial write would mix
two console states under one run.

The receipt's ``entity`` block comes from the doctor's ``facts``
(``entity_kind``, ``entity_state``, ``reporting_claims`` — added to
`nousergon-console`'s doctor so a consumer reads values, not prose). A console
that predates them yields ``null`` there, and the reader grades that UNMET
("not a count"): an older console is never filed as a passing one.

The only writes are the receipts and this producer's run record
(``data_collection/runs/console_entity/<date>.json``, `_run_record.py`).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from data_gate.descriptors import Unit, load_units

__all__ = [
    "DEFAULT_BUCKET",
    "DEFAULT_CONSOLE_URL",
    "RECEIPT_KEY",
    "RECEIPT_SCHEMA",
    "STORE_PREFIX",
    "ConsoleUnusable",
    "build_receipt",
    "collect_receipts",
    "main",
    "receipt_object_key",
]

logger = logging.getLogger(__name__)

RECEIPT_SCHEMA = "data_console_entity.v1"

#: Relative to the store root — the reader resolves it the same way.
RECEIPT_KEY = "console_entity/{unit_id}/latest.json"

DEFAULT_BUCKET = "alpha-engine-research"
STORE_PREFIX = "data_collection"

#: The console's own bind on its box (`nousergon-console` serves on
#: 127.0.0.1:5180 by default). Overridable, because it is the box's config.
DEFAULT_CONSOLE_URL = "http://127.0.0.1:5180"

#: The console's JSON wire version this writer understands
#: (`console/render/json.py::SCHEMA_VERSION`). A different one is refused,
#: never guessed at.
_WIRE_SCHEMA_VERSION = 1

_TIMEOUT_SECONDS = 30
_PRODUCER = "console_entity"
_REGION = "us-east-1"

#: ``(status, body)`` for one GET. Injected so tests drive the real code path
#: with recorded console payloads instead of a socket.
Fetch = Callable[[str], "tuple[int, bytes]"]


class ConsoleUnusable(RuntimeError):
    """The console could not give an answer worth filing. Nothing is written."""


@dataclass(frozen=True)
class Collected:
    unit: Unit
    key: str
    receipt: dict[str, Any]


def receipt_object_key(unit: Unit) -> str:
    """The full object key under the bucket, e.g.
    ``data_collection/console_entity/D03/latest.json``."""
    return f"{STORE_PREFIX}/{RECEIPT_KEY.format(unit_id=unit.unit_id)}"


def _iso(instant: dt.datetime) -> str:
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=dt.timezone.utc)
    return instant.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _urlopen_fetch(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:  # noqa: S310 - fixed scheme
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""
    except (urllib.error.URLError, OSError) as exc:
        raise ConsoleUnusable(f"console unreachable at {url}: {exc}") from exc


def doctor_url(console_url: str, component_id: str) -> str:
    return f"{console_url.rstrip('/')}/doctor/{urllib.parse.quote(component_id, safe='')}"


def _doctor_payload(fetch: Fetch, console_url: str, component_id: str) -> dict[str, Any]:
    url = doctor_url(console_url, component_id)
    status, body = fetch(url)
    if status != 200:
        raise ConsoleUnusable(f"{url} answered HTTP {status}")
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise ConsoleUnusable(f"{url} did not answer JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ConsoleUnusable(f"{url} answered a {type(payload).__name__}, not a doctor payload")
    if payload.get("schema_version") != _WIRE_SCHEMA_VERSION or payload.get("view") != "doctor":
        raise ConsoleUnusable(
            f"{url} answered schema_version={payload.get('schema_version')!r} "
            f"view={payload.get('view')!r}, expected {_WIRE_SCHEMA_VERSION} / 'doctor'"
        )
    if str(payload.get("identifier") or "") != component_id:
        raise ConsoleUnusable(f"{url} diagnosed {payload.get('identifier')!r}, not {component_id!r}")
    index = payload.get("index")
    if not isinstance(index, dict):
        raise ConsoleUnusable(f"{url} carried no `index` freshness block: its as-of is unknown")
    if index.get("bootstrap"):
        raise ConsoleUnusable(
            "the console is still serving its bootstrap index (first build not landed): "
            "a diagnosis of an empty index says nothing about the surface"
        )
    if index.get("stale") is not False:
        raise ConsoleUnusable(
            f"the console's index is stale (stale={index.get('stale')!r}, built_at={index.get('built_at')!r}, "
            f"basis={index.get('staleness_basis')!r}): filing it with today's checked_at would launder it current"
        )
    return payload


def build_receipt(unit: Unit, payload: dict[str, Any], *, checked_at: dt.datetime, console_url: str) -> dict[str, Any]:
    """One unit's receipt, in the reader's ``data_console_entity.v1`` shape.

    Everything under ``doctor`` and ``entity`` is the console's own answer,
    copied, never computed here.
    """
    facts = payload.get("facts") if isinstance(payload.get("facts"), dict) else {}
    index = payload.get("index") or {}
    return {
        "schema": RECEIPT_SCHEMA,
        "unit_id": unit.unit_id,
        "component_id": unit.component_id,
        "checked_at": _iso(checked_at),
        "doctor": {
            "identifier": payload.get("identifier"),
            "ok": payload.get("ok"),
            "summary": payload.get("summary"),
            "broken": payload.get("broken"),
            "remedy": payload.get("remedy"),
            "steps": list(payload.get("steps") or []),
        },
        "entity": {
            "kind": facts.get("entity_kind"),
            "state": facts.get("entity_state"),
            "reporting_claims": facts.get("reporting_claims"),
        },
        "console": {
            "url": doctor_url(console_url, unit.component_id),
            "index_built_at": index.get("built_at"),
            "index_age_seconds": index.get("age_seconds"),
        },
    }


def collect_receipts(
    units: list[Unit],
    *,
    console_url: str = DEFAULT_CONSOLE_URL,
    fetch: Fetch = _urlopen_fetch,
    now: dt.datetime | None = None,
) -> list[Collected]:
    """Ask the console about every unit; raise :class:`ConsoleUnusable` if any
    answer is unusable. Writes nothing."""
    if not units:
        raise ConsoleUnusable("no units to check: a run over an empty set would file nothing and read as done")
    checked_at = now or dt.datetime.now(dt.timezone.utc)
    collected: list[Collected] = []
    for unit in units:
        payload = _doctor_payload(fetch, console_url, unit.component_id)
        collected.append(
            Collected(
                unit=unit,
                key=receipt_object_key(unit),
                receipt=build_receipt(unit, payload, checked_at=checked_at, console_url=console_url),
            )
        )
    return collected


def _line(item: Collected) -> str:
    receipt = item.receipt
    doctor = receipt["doctor"]
    entity = receipt["entity"]
    verdict = "reachable" if doctor["ok"] is True else f"broken at {doctor['broken']!r}"
    return (
        f"{item.unit.unit_id} {item.unit.component_id}: {verdict}; "
        f"{entity['kind']} {entity['state']} on {entity['reporting_claims']} reporting claim(s)"
    )


def main(argv: list[str] | None = None, *, fetch: Fetch = _urlopen_fetch, s3: Any = None) -> int:
    logging.basicConfig(level="INFO")
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--console-url", default=DEFAULT_CONSOLE_URL)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--region", default=_REGION)
    ap.add_argument(
        "--unit",
        action="append",
        default=None,
        help="restrict to these unit ids (repeatable); default every descriptor",
    )
    ap.add_argument("--no-write", action="store_true", help="dry run: print the receipts, write nothing")
    args = ap.parse_args(argv)

    if s3 is None and not args.no_write:
        import boto3  # noqa: PLC0415 - deferred so import stays light for tests

        s3 = boto3.client("s3", region_name=args.region)

    from data_gate.producers._run_record import write_run_record  # noqa: PLC0415

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        units = load_units()
        if args.unit:
            wanted = set(args.unit)
            unknown = sorted(wanted - {u.unit_id for u in units})
            if unknown:
                raise ConsoleUnusable(f"no unit descriptor for {unknown}")
            units = [u for u in units if u.unit_id in wanted]
        collected = collect_receipts(units, console_url=args.console_url, fetch=fetch, now=started_at)
    except Exception as exc:  # RAISE after recording — fail loud, never a silent swallow
        if not args.no_write:
            write_run_record(
                s3,
                bucket=args.bucket,
                producer=_PRODUCER,
                status="error",
                started_at=started_at,
                finished_at=dt.datetime.now(dt.timezone.utc),
                error=str(exc),
            )
        raise

    for item in collected:
        print(_line(item))
    reachable = sum(1 for item in collected if item.receipt["doctor"]["ok"] is True)

    if args.no_write:
        for item in collected:
            print(f"--- s3://{args.bucket}/{item.key} (dry run, not written)")
            print(json.dumps(item.receipt, indent=2, sort_keys=True))
        print(f"dry run: {len(collected)} receipt(s), {reachable} fully reachable; nothing written")
        return 0

    for item in collected:
        s3.put_object(
            Bucket=args.bucket,
            Key=item.key,
            Body=json.dumps(item.receipt, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
    print(f"WROTE {len(collected)} receipt(s) under s3://{args.bucket}/{STORE_PREFIX}/console_entity/; {reachable} fully reachable")
    write_run_record(
        s3,
        bucket=args.bucket,
        producer=_PRODUCER,
        status="ok",
        started_at=started_at,
        finished_at=dt.datetime.now(dt.timezone.utc),
        detail={"receipts": len(collected), "fully_reachable": reachable},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
