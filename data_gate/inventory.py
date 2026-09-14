"""The writer inventory: every write site resolves to a descriptor, and back.

This is the one mechanism in the red board that notices a unit **nobody
registered**. Every other clause grades a unit the descriptors already name; a
board built only from what someone remembered to declare renders complete over
whatever was forgotten, which is the failure `observability-policy` §2.2 names
by hand-maintained-list.

So: walk the declared producer roots, find S3 PUT and ArcticDB write call sites
by AST, and assert a bijection against `registry.d/units/`.

**Forward** (write site -> descriptor) is graded strictly: an unclaimed write
site fails the clause and names the file. **Reverse** (descriptor -> write site)
is graded for every unit whose declared `code_path` names an in-repo Python
file, and the units it cannot check that way are NAMED in the detail rather than
dropped — partial coverage reported, never assumed complete.
"""

from __future__ import annotations

import ast
import fnmatch
import pathlib
from dataclasses import dataclass, field

import yaml

from data_gate.descriptors import REPO_ROOT, Unit

__all__ = ["InventoryReading", "WriterInventory", "load_inventory_scope", "scan"]

SCOPE_PATH = REPO_ROOT / "registry.d" / "writer_inventory.yaml"


@dataclass(frozen=True)
class WriterInventory:
    """The declared scope of the scan, read from `registry.d/writer_inventory.yaml`."""

    roots: tuple[str, ...]
    write_calls: frozenset[str]
    shared_writers: dict[str, tuple[str, ...]]
    excluded: tuple[str, ...]
    extra_paths: dict[str, tuple[str, ...]]
    no_python_write_site: dict[str, str]


def load_inventory_scope(path: pathlib.Path | None = None) -> WriterInventory:
    document = yaml.safe_load((path or SCOPE_PATH).read_text(encoding="utf-8"))
    calls: set[str] = set()
    for group in (document.get("write_calls") or {}).values():
        calls.update(group)
    excluded = tuple(entry["glob"] for entry in document.get("excluded_paths") or [])
    return WriterInventory(
        roots=tuple(document["roots"]),
        write_calls=frozenset(calls),
        shared_writers={
            entry["path"]: tuple(entry["parent_units"])
            for entry in document.get("shared_writers") or []
        },
        excluded=excluded,
        extra_paths={
            entry["path"]: tuple(entry["parent_units"])
            for entry in document.get("extra_paths") or []
        },
        no_python_write_site={
            entry["unit"]: entry["reason"] for entry in document.get("no_python_write_site") or []
        },
    )


@dataclass
class InventoryReading:
    """What the scan found, in terms a clause can render without re-deriving."""

    write_sites: dict[str, list[str]] = field(default_factory=dict)
    undeclared: list[str] = field(default_factory=list)
    units_without_write_site: list[str] = field(default_factory=list)
    units_unverifiable: dict[str, str] = field(default_factory=dict)
    units_via_shared_writer: list[str] = field(default_factory=list)
    parse_failures: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.undeclared and not self.units_without_write_site and not self.parse_failures


def _is_excluded(rel: str, scope: WriterInventory) -> bool:
    return any(fnmatch.fnmatch(rel, glob) for glob in scope.excluded)


def _write_calls_in(source: str, scope: WriterInventory) -> list[str]:
    """Attribute names called in ``source`` that the scope counts as a write."""
    tree = ast.parse(source)
    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else None
        if name and name in scope.write_calls:
            found.append(f"{name}:{getattr(node, 'lineno', 0)}")
    return found


def _declared_code_files(unit: Unit) -> list[str]:
    """The in-repo Python files a descriptor's ``code_path`` names.

    ``code_path`` is human prose with `path:line` entries separated by commas —
    it is read by people first. Parsing it here rather than demanding a second
    machine-only field keeps one declaration instead of two that can disagree.
    """
    raw = str(unit.raw.get("code_path") or "")
    files: list[str] = []
    for chunk in raw.replace(";", ",").split(","):
        token = chunk.strip().split(" ")[0]
        token = token.split(":")[-1] if token.startswith(("crucible:", "metron:")) else token
        token = token.split(":")[0]
        if token.endswith(".py"):
            files.append(token)
    return files


def scan(units: list[Unit], scope: WriterInventory | None = None) -> InventoryReading:
    """Walk the declared roots and reconcile write sites against descriptors."""
    scope = scope or load_inventory_scope()
    reading = InventoryReading()

    claimed: dict[str, list[str]] = {}
    for unit in units:
        for file in _declared_code_files(unit):
            claimed.setdefault(file, []).append(unit.unit_id)
    for mapping in (scope.shared_writers, scope.extra_paths):
        for path, parents in mapping.items():
            claimed.setdefault(path, []).extend(parents)

    for root in scope.roots:
        base = REPO_ROOT / root
        if not base.is_dir():
            continue
        for file in sorted(base.rglob("*.py")):
            rel = file.relative_to(REPO_ROOT).as_posix()
            if _is_excluded(rel, scope):
                continue
            try:
                sites = _write_calls_in(file.read_text(encoding="utf-8"), scope)
            except SyntaxError as exc:
                # A file that will not parse is NOT "no write sites". Recorded
                # as a failure so the clause goes red rather than silently
                # shrinking its own denominator.
                reading.parse_failures[rel] = f"{type(exc).__name__}: {exc}"
                continue
            if not sites:
                continue
            reading.write_sites[rel] = sites
            if rel not in claimed:
                reading.undeclared.append(rel)

    for extra in sorted(scope.extra_paths):
        file = REPO_ROOT / extra
        if not file.is_file():
            reading.parse_failures[extra] = "declared in extra_paths and not present in the tree"
            continue
        try:
            sites = _write_calls_in(file.read_text(encoding="utf-8"), scope)
        except SyntaxError as exc:
            reading.parse_failures[extra] = f"{type(exc).__name__}: {exc}"
            continue
        if sites:
            reading.write_sites[extra] = sites

    for unit in units:
        if unit.unit_id in scope.no_python_write_site:
            reading.units_unverifiable[unit.unit_id] = scope.no_python_write_site[unit.unit_id]
            continue
        files = _declared_code_files(unit)
        if not files:
            reading.units_unverifiable[unit.unit_id] = (
                "its code_path names no in-repo Python file, so an AST scan cannot check "
                "the descriptor -> write-site direction for it"
            )
            continue
        via_shared = any(
            unit.unit_id in parents
            for mapping in (scope.shared_writers, scope.extra_paths)
            for parents in mapping.values()
        )
        if via_shared:
            reading.units_via_shared_writer.append(unit.unit_id)
            continue
        if not any(file in reading.write_sites for file in files):
            reading.units_without_write_site.append(unit.unit_id)

    reading.undeclared.sort()
    reading.units_without_write_site.sort()
    return reading
