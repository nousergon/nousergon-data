"""The consumer half of `schema_contract`: every declared pin, resolved and compared.

`alpha-engine-config-I11282`. The clause used to read MET when
``contract.consumer_pins`` was any non-empty list, so a free-text string was
indistinguishable from a real pin. Measured 2026-09-21: D10's first pin
byte-copied the BODY schema into crucible-dashboard, whose only reader reads the
object KEY's date and never the body — a pin that protected nothing, which this
clause would have graded MET. It was caught in review, not by the gate.

A pin is now a structured declaration (validated by
`data_gate.descriptors`, closed taxonomy :data:`PIN_KINDS`) and each one is
graded on evidence:

* ``body_schema`` — the pinned file exists on the consumer repository's default
  branch (GitHub contents API, one request per pin, cached per read) and its
  VALIDATION SHAPE equals the producer schema's. A stale copy reads UNMET
  naming both hashes; that drift between two repos' CI runs is exactly what a
  pin exists to catch and, until now, nothing caught.
* ``key_template`` — the pinned fixture exists and its ``object_key_template``
  is one of the templates this unit declares under ``writes``.
* ``in_repo_reader`` — the consumer lives in THIS repository, so there is no
  cross-repo boundary to copy a schema across; the pin is the test in this tree
  that drives the named reader, and it must exist and reference that reader.

A repository the token cannot see, or no token at all, reads UNMEASURABLE —
never MET — naming what could not be read.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from data_gate.descriptors import REPO_ROOT, Unit
from data_gate.evidence import GateStore, Reading
from data_gate.sources import GITHUB_TOKEN_ENV, SourceUnavailable

__all__ = ["read_consumer_pins", "schema_shape", "shape_hash"]

#: JSON Schema keywords that label a schema without constraining what it
#: accepts. Stripped before comparison: a producer that improves a description
#: or adds its own `x-*` gate annotation (`x-key-pattern`, `x-provenance`,
#: `x-vendor-live`, ...) has not changed the contract a consumer validates
#: against, and a comparator that says it has would page on every docs edit
#: until someone stops reading it. Everything else — types, `required`,
#: `enum`, `pattern`, `format`, `additionalProperties`, `$schema`, `$ref`,
#: `default` and any unknown keyword — is compared.
_ANNOTATION_KEYWORDS = frozenset({"title", "description", "$comment", "examples", "$id"})

#: Keywords whose value maps NAMES to subschemas. Their keys are data (a
#: property may well be called `description`), so they are never stripped —
#: only the subschemas under them are normalised.
_NAME_MAPS = frozenset({"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"})


def schema_shape(node: Any, *, names: bool = False) -> Any:
    """``node`` with annotation keywords removed at every schema position."""
    if isinstance(node, dict):
        if names:
            return {key: schema_shape(value) for key, value in node.items()}
        return {
            key: schema_shape(value, names=key in _NAME_MAPS)
            for key, value in node.items()
            if key not in _ANNOTATION_KEYWORDS and not key.startswith("x-")
        }
    if isinstance(node, list):
        return [schema_shape(value) for value in node]
    return node


def shape_hash(document: Any) -> str:
    """sha256 (first 16 hex) of the canonical JSON of :func:`schema_shape`."""
    canonical = json.dumps(schema_shape(document), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _label(pin: dict) -> str:
    return f"{pin['repo']}:{pin['path']} ({pin['pins']})"


def _read_site_symbol(read_site: str) -> str | None:
    """``_alt_entry_from_payload`` from ``features/compute.py::_alt_entry_from_payload``."""
    if "::" not in read_site:
        return None
    symbol = read_site.split("::", 1)[1].strip()
    return symbol.split(".", 1)[0] or None


def _grade_in_repo(pin: dict) -> str | None:
    """A finding for an ``in_repo_reader`` pin, or ``None`` when it holds."""
    test = REPO_ROOT / pin["path"]
    if not test.is_file():
        return f"{_label(pin)}: the pinning test is not in this tree"
    site_path = pin["read_site"].split("::", 1)[0].strip()
    if not (REPO_ROOT / site_path).is_file():
        return f"{_label(pin)}: read site {site_path} is not in this tree"
    symbol = _read_site_symbol(pin["read_site"])
    if symbol is None:
        return f"{_label(pin)}: read site {pin['read_site']!r} names no symbol (`path::symbol`)"
    if symbol not in test.read_text(encoding="utf-8"):
        return f"{_label(pin)}: the pinning test never references the read site {symbol!r}"
    return None


def _grade_remote(pin: dict, content: bytes, unit: Unit) -> str | None:
    """A finding for a ``body_schema``/``key_template`` pin's content, or ``None``."""
    try:
        consumer_doc = json.loads(content)
    except ValueError as exc:
        return f"{_label(pin)}: the pinned file is not JSON ({exc})"
    if pin["pins"] == "key_template":
        template = consumer_doc.get("object_key_template") if isinstance(consumer_doc, dict) else None
        writes = [str(w) for w in unit.raw.get("writes") or []]
        if not template:
            return f"{_label(pin)}: the pinned fixture carries no `object_key_template`"
        if template not in writes:
            return (
                f"{_label(pin)}: STALE — pins key template {template!r}; this unit writes {writes}"
            )
        return None
    producer = REPO_ROOT / pin["producer"]
    try:
        producer_doc = json.loads(producer.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return f"{_label(pin)}: producer contract {pin['producer']} is unreadable ({exc})"
    consumer_hash, producer_hash = shape_hash(consumer_doc), shape_hash(producer_doc)
    if consumer_hash != producer_hash:
        return (
            f"{_label(pin)}: STALE — pinned copy shape {consumer_hash} != producer "
            f"{pin['producer']} shape {producer_hash}"
        )
    return None


def read_consumer_pins(store: GateStore, unit: Unit, producer_files: list[str]) -> Reading:
    """Grade every declared consumer pin; see the module docstring."""
    contract = unit.raw.get("contract") or {}
    pins = [p for p in contract.get("consumer_pins") or [] if isinstance(p, dict)]
    unpinned = [str(c).strip() for c in contract.get("unpinned_consumers") or []]
    evidence = tuple(producer_files) + tuple(_label(p) for p in pins)
    if not pins:
        detail = (
            f"producer side present ({producer_files}) and NO consumer pin is declared. A schema "
            "the producer validates against and no consumer pins is half a contract: it "
            "cannot fail a consumer that drifts (plan §4.3)."
        )
        if unpinned:
            detail += f" Declared consumer(s) with no pinned contract file: {unpinned}."
        return Reading(met=False, detail=detail, evidence=evidence, source="nousergon-data tree")

    github = getattr(store, "github_contents", None)
    held: list[str] = []
    findings: list[str] = []
    unmeasurable: list[str] = []
    for pin in pins:
        if pin["pins"] == "in_repo_reader":
            finding = _grade_in_repo(pin)
        elif github is None:
            unmeasurable.append(f"{_label(pin)} (no GitHub reader configured; set {GITHUB_TOKEN_ENV})")
            continue
        else:
            try:
                kind, content = github.read_file(pin["repo"], pin["path"])
            except SourceUnavailable as exc:
                unmeasurable.append(f"{_label(pin)} ({exc})")
                continue
            if kind is None:
                finding = f"{_label(pin)}: ABSENT from the consumer's default branch"
            elif kind != "file" or content is None:
                finding = f"{_label(pin)}: resolves to a {kind}, not a pinned contract file"
            else:
                finding = _grade_remote(pin, content, unit)
        if finding is None:
            held.append(_label(pin))
        else:
            findings.append(finding)
    if unpinned:
        findings.append(f"declared consumer(s) with no pinned contract file: {unpinned}")

    parts = list(findings)
    if unmeasurable:
        parts.append(f"could not read: {unmeasurable}")
    parts.append(f"pins held {len(held)}/{len(pins)}: {held}")
    detail = "; ".join(parts)
    source = "nousergon-data tree + GitHub contents API (default branch)"
    if findings:
        # A definite finding fails the clause whatever the unreadable pins
        # would have said, so it is UNMET rather than UNMEASURABLE.
        return Reading(met=False, detail=detail, evidence=evidence, source=source)
    if unmeasurable:
        return Reading(met=False, detail=detail, evidence=evidence, unmeasurable=True, source=source)
    return Reading(met=True, detail=f"schema and producer test present; {detail}", evidence=evidence, source=source)
