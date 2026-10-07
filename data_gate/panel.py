"""`data.phase3.daily_panel_adopted` — the daily panel acceptance clause (audit gap A10).

`alpha-engine-config-I10791` (plan P-25, amendment 1) and `-I10795`. The daily
panel is an ADOPTED phase-3 deliverable, but until this clause nothing on the
board graded it: `data_gate read --gate data-phase3` could have read MET with
crucible still compiling its own panel from raw ArcticDB. The October 4 audit
(`alpha-engine-config-I11973`) asked for one clause requiring the producer
schema, a pinned actual consumer, one real same-trading-day parity result with
a declared tolerance, and the duplicate direct-Arctic compile removed after the
compatible consumer is active. Those are five legs here, in the order the work
lands, and the clause is their conjunction:

1. ``contract``  — the three schemas and the producer test are in this tree.
2. ``published`` — the panel for the latest due trading day is in the store,
   with a manifest that validates and names the parquet beside it.
3. ``consumer_pin`` — the consumer pins the row schema (same validation shape)
   on its default branch, and the read site that reads the panel exists there.
4. ``parity`` — the latest parity receipt is ``equivalent`` under exactly
   :data:`contracts.daily_panel.PARITY_TOLERANCE`, both sides end on the
   receipt's own trading day, and its producer side is the panel actually
   published for that day (same sha256).
5. ``direct_compile_removed`` — the consumer's ``data.daily`` entrypoint no
   longer references the direct-ArcticDB compile. Not a flag somebody sets:
   read from the consumer's code on its default branch, so restoring the old
   path turns the clause red again.

Any UNMET leg makes the clause UNMET; otherwise any leg we could not look at
makes it UNMEASURABLE. Never MET on an absent or unreadable proof (option (c),
Brian 2026-10-04: a current failure or a stale or missing proof blocks).
"""

from __future__ import annotations

import ast
import datetime as dt
import json
from dataclasses import dataclass

from nousergon_lib.gates import GateStore, read_store_document
from nousergon_lib.trading_calendar import (  # pyright: ignore[reportAttributeAccessIssue]
    is_trading_day,
    previous_trading_day,
)

from contracts import daily_panel as dp
from data_gate.consumer_pins import shape_hash
from data_gate.descriptors import REPO_ROOT
from data_gate.evidence import Reading
from data_gate.sources import GITHUB_TOKEN_ENV, SourceUnavailable

__all__ = [
    "PANEL_CLAUSE",
    "PANEL_REQUIREMENT",
    "PRODUCER_TEST",
    "read_daily_panel_adopted",
]

PANEL_CLAUSE = "data.phase3.daily_panel_adopted"

#: The producer test the ``contract`` leg requires beside the three schemas.
PRODUCER_TEST = "tests/test_daily_panel_contract.py"

PANEL_REQUIREMENT = (
    "the daily panel is a data-collector product that its consumer has ADOPTED (plan P-25, "
    "amendment 1; alpha-engine-config-I10791): a versioned row/manifest/parity contract with a "
    "producer test; the panel published for the latest due trading day; the consumer "
    f"({dp.CONSUMER.repo}) pinning the row schema with its read site on its default branch; one "
    "real same-trading-day parity receipt EQUIVALENT under the declared tolerance against the "
    "panel actually published that day; and the duplicate direct-ArcticDB compile removed from "
    f"{dp.CONSUMER.entrypoint}, not left as a silent second implementation"
)

_SOURCE = (
    "data_collection store (panel/) + nousergon-data tree + GitHub contents API "
    f"({dp.CONSUMER.repo} default branch)"
)

_MET, _UNMET, _UNMEASURABLE = "MET", "UNMET", "UNMEASURABLE"


@dataclass(frozen=True)
class _Leg:
    name: str
    state: str
    detail: str
    evidence: tuple[str, ...] = ()


def _rel(key: str) -> str:
    return dp.store_relative(key)


def _due_days(trading_day: dt.date) -> tuple[dt.date, dt.date]:
    """The latest due session and the one before it.

    The panel publishes after the EOD append; a gate read that runs before that
    finds the previous session's panel, which is still the current one. Two
    sessions back is a publisher that has stopped.
    """
    due = trading_day if is_trading_day(trading_day) else previous_trading_day(trading_day)
    return due, previous_trading_day(due)


def _leg_contract() -> _Leg:
    paths = [*dp.SCHEMA_FILES.values(), PRODUCER_TEST]
    missing = [p for p in paths if not (REPO_ROOT / p).is_file()]
    if missing:
        return _Leg("contract", _UNMET, f"absent from this tree: {missing}", tuple(paths))
    for kind, path in dp.SCHEMA_FILES.items():
        try:
            json.loads((REPO_ROOT / path).read_text(encoding="utf-8"))
        except ValueError as exc:
            return _Leg("contract", _UNMET, f"{path} ({kind}) is not JSON: {exc}", tuple(paths))
    return _Leg("contract", _MET, f"row/manifest/parity schemas + {PRODUCER_TEST} present", tuple(paths))


def _read_manifest(store: GateStore, day: dt.date) -> tuple[str, dict | None, str | None, bool]:
    """``(key, document, problem, unmeasurable)`` for one day's manifest."""
    key = _rel(dp.manifest_key(day))
    read = read_store_document(store, key)
    if read.problem is not None:
        return key, None, f"could not read {key}: {read.problem}", bool(getattr(read, "access_problem", True))
    if read.absent:
        return key, None, None, False
    return key, read.document or {}, None, False


def _grade_manifest(store: GateStore, key: str, document: dict, day: dt.date) -> str | None:
    """A finding for a published day's manifest, or ``None`` when the publish holds."""
    problems = dp.schema_problems(document, "manifest")
    if problems:
        return f"{key} breaks daily_panel_manifest.schema.json: {problems[:3]}"
    if document["trading_day"] != day.isoformat() or document["last_session"] != day.isoformat():
        return (
            f"{key} describes trading_day {document['trading_day']!r} / last_session "
            f"{document['last_session']!r}, not {day}"
        )
    if tuple(document["columns"]) != dp.PANEL_COLUMNS:
        return f"{key} columns {document['columns']} != contract {list(dp.PANEL_COLUMNS)}"
    parquet = _rel(dp.panel_key(day))
    if parquet not in set(store.list_keys(parquet)):
        return f"{key} names {document['panel_key']} but no parquet is there — an incomplete publish"
    return None


def _leg_published(store: GateStore, trading_day: dt.date) -> tuple[_Leg, dict[str, dict]]:
    due, previous = _due_days(trading_day)
    manifests: dict[str, dict] = {}
    unreadable: list[str] = []
    evidence: list[str] = []
    for day in (due, previous):
        key, document, problem, unmeasurable = _read_manifest(store, day)
        evidence.append(key)
        if problem is not None:
            if unmeasurable:
                unreadable.append(problem)
                continue
            return _Leg("published", _UNMET, problem, tuple(evidence)), manifests
        if document is None:
            continue
        finding = _grade_manifest(store, key, document, day)
        if finding is not None:
            return _Leg("published", _UNMET, finding, tuple(evidence)), manifests
        manifests[day.isoformat()] = document
        return (
            _Leg(
                "published",
                _MET,
                f"{key}: {document['row_count']} rows, {document['symbols_on_trading_day']} tickers on "
                f"{day}, {document['session_count']} sessions, sha256 {document['panel_sha256'][:12]}",
                tuple(evidence),
            ),
            manifests,
        )
    if unreadable:
        return _Leg("published", _UNMEASURABLE, "; ".join(unreadable), tuple(evidence)), manifests
    return (
        _Leg(
            "published",
            _UNMET,
            f"no panel published for the latest due session {due} or the one before it ({previous})",
            tuple(evidence),
        ),
        manifests,
    )


def _github_read(store: GateStore, path: str) -> tuple[bytes | None, str | None, bool]:
    """``(content, finding, unmeasurable)`` for one consumer-repo file."""
    github = getattr(store, "github_contents", None)
    label = f"{dp.CONSUMER.repo}:{path}"
    if github is None:
        return None, f"{label}: no GitHub reader configured (set {GITHUB_TOKEN_ENV})", True
    try:
        kind, content = github.read_file(dp.CONSUMER.repo, path)
    except SourceUnavailable as exc:
        return None, f"{label}: {exc}", True
    if kind is None:
        return None, f"{label}: ABSENT from the consumer's default branch", False
    if kind != "file" or content is None:
        return None, f"{label}: resolves to a {kind}, not a file", False
    return content, None, False


def _leg_consumer_pin(store: GateStore) -> _Leg:
    consumer = dp.CONSUMER
    read_site_path = consumer.read_site.split("::", 1)[0]
    evidence = (f"{consumer.repo}:{consumer.pin_path}", f"{consumer.repo}:{consumer.read_site}")
    content, finding, unmeasurable = _github_read(store, consumer.pin_path)
    if finding is not None:
        return _Leg("consumer_pin", _UNMEASURABLE if unmeasurable else _UNMET, finding, evidence)
    try:
        pinned = json.loads(content or b"")
    except ValueError as exc:
        return _Leg("consumer_pin", _UNMET, f"{consumer.pin_path} is not JSON ({exc})", evidence)
    producer_hash, consumer_hash = shape_hash(dp.load_schema("row")), shape_hash(pinned)
    if producer_hash != consumer_hash:
        return _Leg(
            "consumer_pin",
            _UNMET,
            f"STALE — pinned copy shape {consumer_hash} != producer {dp.SCHEMA_FILES['row']} shape {producer_hash}",
            evidence,
        )
    content, finding, unmeasurable = _github_read(store, read_site_path)
    if finding is not None:
        return _Leg("consumer_pin", _UNMEASURABLE if unmeasurable else _UNMET, finding, evidence)
    if f"class {consumer.read_site_symbol}" not in (content or b"").decode("utf-8", "replace"):
        return _Leg(
            "consumer_pin",
            _UNMET,
            f"pin holds (shape {producer_hash}) but {read_site_path} defines no class "
            f"{consumer.read_site_symbol} — a pinned schema nothing reads protects nothing",
            evidence,
        )
    return _Leg("consumer_pin", _MET, f"pin shape {producer_hash} matches; read site {consumer.read_site} present", evidence)


def _latest_parity_key(store: GateStore) -> str | None:
    prefix = _rel(dp.KEY_PREFIX)
    keys = sorted(k for k in store.list_keys(prefix) if k.endswith("/parity.json"))
    return keys[-1] if keys else None


def _leg_parity(store: GateStore) -> _Leg:
    try:
        key = _latest_parity_key(store)
    except Exception as exc:  # noqa: BLE001 - an unlistable prefix is a reading, not a crash
        return _Leg("parity", _UNMEASURABLE, f"could not list {_rel(dp.KEY_PREFIX)}: {type(exc).__name__}: {exc}")
    if key is None:
        return _Leg(
            "parity",
            _UNMET,
            f"no parity receipt under {_rel(dp.KEY_PREFIX)}*/parity.json — no real trading day has been "
            "compared side by side yet",
            (_rel(dp.KEY_PREFIX),),
        )
    read = read_store_document(store, key)
    if read.problem is not None:
        return _Leg("parity", _UNMEASURABLE, f"could not read {key}: {read.problem}", (key,))
    receipt = read.document or {}
    problems = dp.schema_problems(receipt, "parity")
    if problems:
        return _Leg("parity", _UNMET, f"{key} breaks daily_panel_parity.schema.json: {problems[:3]}", (key,))
    day = receipt["trading_day"]
    findings: list[str] = []
    if receipt["tolerance"] != dp.PARITY_TOLERANCE:
        findings.append(f"computed under tolerance {receipt['tolerance']}, not the declared {dp.PARITY_TOLERANCE}")
    for side in ("producer", "consumer"):
        if receipt[side]["trading_day"] != day:
            findings.append(f"{side} panel ends on {receipt[side]['trading_day']}, not the receipt's {day}")
    if not is_trading_day(dt.date.fromisoformat(day)):
        findings.append(f"{day} is not an NYSE session")
    if receipt["verdict"] != "equivalent":
        findings.append(
            f"verdict {receipt['verdict']}: {receipt['value_mismatches']} value mismatch(es), "
            f"{receipt['missing_in_producer']} missing from the published panel, "
            f"{receipt['missing_in_consumer']} unread by the consumer; e.g. {receipt['examples'][:3]}"
        )
    manifest_key, manifest, problem, unmeasurable = _read_manifest(store, dt.date.fromisoformat(day))
    evidence = (key, manifest_key)
    if problem is not None:
        if unmeasurable and not findings:
            return _Leg("parity", _UNMEASURABLE, problem, evidence)
        findings.append(problem)
    elif manifest is None:
        findings.append(f"no panel was published for {day} ({manifest_key} absent) — parity against what?")
    elif manifest.get("panel_sha256") != receipt["producer"]["sha256"]:
        findings.append(
            f"producer side sha256 {receipt['producer']['sha256'][:12]} is not the panel published for "
            f"{day} ({str(manifest.get('panel_sha256'))[:12]})"
        )
    if findings:
        return _Leg("parity", _UNMET, f"{key}: " + "; ".join(findings), evidence)
    return _Leg(
        "parity",
        _MET,
        f"{key}: equivalent on {day} over {receipt['rows_compared']} rows / {receipt['tickers_compared']} "
        f"tickers under {dp.PARITY_TOLERANCE}",
        evidence,
    )


def _function_names(source: str, function: str) -> set[str] | None:
    """Every Name id and Attribute attr referenced in ``function``'s body, or ``None``."""
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function:
            names: set[str] = set()
            for inner in ast.walk(node):
                if isinstance(inner, ast.Name):
                    names.add(inner.id)
                elif isinstance(inner, ast.Attribute):
                    names.add(inner.attr)
            return names
    return None


def _leg_direct_compile_removed(store: GateStore) -> _Leg:
    consumer = dp.CONSUMER
    evidence = (f"{consumer.repo}:{consumer.entrypoint}",)
    content, finding, unmeasurable = _github_read(store, consumer.entrypoint_path)
    if finding is not None:
        return _Leg("direct_compile_removed", _UNMEASURABLE if unmeasurable else _UNMET, finding, evidence)
    source = (content or b"").decode("utf-8", "replace")
    try:
        names = _function_names(source, consumer.entrypoint_function)
    except SyntaxError as exc:
        return _Leg("direct_compile_removed", _UNMET, f"{consumer.entrypoint_path} does not parse: {exc}", evidence)
    if names is None:
        return _Leg(
            "direct_compile_removed",
            _UNMET,
            f"{consumer.entrypoint_path} defines no {consumer.entrypoint_function}",
            evidence,
        )
    still = sorted(set(consumer.direct_compile_names) & names)
    if still:
        return _Leg(
            "direct_compile_removed",
            _UNMET,
            f"{consumer.entrypoint} still references the direct-ArcticDB compile ({still})",
            evidence,
        )
    if consumer.read_site_symbol not in source:
        return _Leg(
            "direct_compile_removed",
            _UNMET,
            f"{consumer.entrypoint} dropped the direct compile but its module never names "
            f"{consumer.read_site_symbol} — it reads the panel from somewhere undeclared",
            evidence,
        )
    return _Leg(
        "direct_compile_removed",
        _MET,
        f"{consumer.entrypoint} references none of {list(consumer.direct_compile_names)} and its module "
        f"reads {consumer.read_site_symbol}",
        evidence,
    )


def read_daily_panel_adopted(store: GateStore, *, trading_day: dt.date) -> Reading:
    """The five legs, rolled up; see the module docstring."""
    published, manifests = _leg_published(store, trading_day)
    legs = [
        _leg_contract(),
        published,
        _leg_consumer_pin(store),
        _leg_parity(store),
        _leg_direct_compile_removed(store),
    ]
    states = {leg.state for leg in legs}
    detail = "; ".join(f"[{leg.name} {leg.state}] {leg.detail}" for leg in legs)
    evidence = tuple(dict.fromkeys(e for leg in legs for e in leg.evidence))
    as_of = next((m.get("generated_at") for m in manifests.values()), None)
    if _UNMET in states:
        return Reading(met=False, detail=detail, evidence=evidence, source=_SOURCE, as_of=as_of)
    if _UNMEASURABLE in states:
        return Reading(met=False, detail=detail, evidence=evidence, unmeasurable=True, source=_SOURCE, as_of=as_of)
    return Reading(met=True, detail=detail, evidence=evidence, source=_SOURCE, as_of=as_of)
