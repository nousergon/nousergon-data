"""Writer-template reachability: does any writer produce the key a descriptor declares?

`alpha-engine-config-I10895`. D03 declared `predictor/price_cache/*.parquet` —
a tree nothing has written since the Wave 3 PR4 cutover — and D46 declared
`data/insider_transactions/{date}.parquet`, a shape its writer never produced.
Both drifted silently because nothing checked a descriptor's `writes:` entry
against the code that is supposed to produce it; the writer-inventory
bijection (`data_gate/inventory.py`) only checks that a write CALL SITE has a
descriptor, never that the descriptor's declared KEY matches what the call
site actually writes.

This module is the other direction, and is deliberately bounded rather than a
full interpreter:

**Signal A — prefix reachability.** For a unit's declared `code_path` file(s),
collect every string literal that is not a docstring and not assigned to a
name containing ``LEGACY`` (this codebase's own naming convention for a
retired-but-still-present default, e.g. ``PRICE_CACHE_LEGACY_PREFIX`` —
excluding it is what stops a read-compat fallback from keeping a dead write
template looking reachable), plus one hop into a same-repo function the file
imports and calls, collecting only that function's return literals that
resolve to ITS OWN module-level constants (never a return that just echoes an
unresolved parameter — that would make every prefix "reachable"). A `writes:`
entry whose literal prefix (the text before its first `{placeholder}` or `*`)
matches none of these candidates is flagged.

**Signal B — declared-entry undercount.** Counts the unit's own S3 write-verb
call sites (the same four verbs `writer_inventory.yaml` scopes:
`put_object`, `upload_file`, `upload_fileobj`, `copy_object`) in its
`code_path` file. A unit that is the SOLE owner of that file (not named in
`shared_writers`/`extra_paths`, and no other unit shares the same
`code_path`) must declare at least as many `writes:` entries as it has call
sites — this is what catches D46: `write_form4_parquet` makes two distinct
`put_object` calls (the dated artifact, the `latest.json` sidecar) and the
old descriptor declared one.

Both signals are code-derived (no AWS, no S3 listing) and intentionally
conservative: a unit this cannot resolve (no in-repo `code_path`, or a
shared/ambiguous write site) is reported UNVERIFIABLE, never silently passed.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field

from data_gate.descriptors import REPO_ROOT, Unit, load_units
from data_gate.inventory import _declared_code_files, load_inventory_scope

__all__ = [
    "Finding",
    "check_unit",
    "run_all",
]

#: The S3 verbs a key/path argument is worth extracting from. `to_parquet`
#: (pandas) is excluded on purpose — its argument is usually a buffer or a
#: local path, not the published S3 key; the real key for those pipelines
#: shows up at a separate `put_object` call, which this scan does catch.
_S3_KEY_CALLS = frozenset({"put_object", "upload_file", "upload_fileobj", "copy_object"})


@dataclass(frozen=True)
class Finding:
    unit_id: str
    status: str  # "ok" | "flagged" | "unverifiable"
    detail: str


def _is_docstring(node: ast.stmt) -> bool:
    return isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(
        node.value.value, str
    )


def _target_names(node: ast.Assign | ast.AnnAssign) -> list[str]:
    if isinstance(node, ast.AnnAssign):
        return [node.target.id] if isinstance(node.target, ast.Name) else []
    return [t.id for t in node.targets if isinstance(t, ast.Name)]


def _module_level_constants(tree: ast.Module, *, exclude_legacy: bool = True) -> dict[str, str]:
    """Name -> literal value, for plain module-level string assignments.

    Skips the module docstring and anything whose target name contains
    ``LEGACY`` (case-insensitive) — see module docstring for why.
    """
    out: dict[str, str] = {}
    body = tree.body
    start = 1 if body and _is_docstring(body[0]) else 0
    for node in body[start:]:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
            continue
        if not (isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)):
            continue
        for name in _target_names(node):
            if exclude_legacy and "legacy" in name.lower():
                continue
            out[name] = node.value.value
    return out


def _string_literals_in_function(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """Every string-constant fragment in a function body, docstring excluded.

    Includes plain `Constant(str)` nodes and the literal segments of
    f-strings (`JoinedStr`'s `Constant` children) — never the dynamic
    `FormattedValue` parts, which carry no fixed text.
    """
    out: list[str] = []
    body = fn.body
    start = 1 if body and _is_docstring(body[0]) else 0
    for stmt in body[start:]:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                out.append(node.value)
    return out


def _return_literals(fn: ast.FunctionDef | ast.AsyncFunctionDef, module_consts: dict[str, str]) -> list[str]:
    """Literals a function can concretely be shown to return.

    Only `return <constant>`, `return [<constant-or-resolvable-name>, ...]`
    (and the tuple form) count. A `return <parameter-name>` — the
    unresolved-passthrough branch — is deliberately dropped: keeping it would
    make every prefix "reachable" through the parameter that carries
    whatever the caller passed, which is precisely the case this check
    exists to catch (`price_cache_write_prefixes`'s `return [primary]`
    branch, where `primary` is the un-fixed legacy default).
    """
    out: list[str] = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Return) or node.value is None:
            continue
        values: list[ast.expr]
        if isinstance(node.value, (ast.List, ast.Tuple)):
            values = list(node.value.elts)
        else:
            values = [node.value]
        for value in values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                out.append(value.value)
            elif isinstance(value, ast.Name) and value.id in module_consts:
                out.append(module_consts[value.id])
    return out


def _local_module_path(module: str) -> "object | None":
    from pathlib import Path

    candidate = REPO_ROOT / (module.replace(".", "/") + ".py")
    return candidate if candidate.is_file() else None


def _candidate_pool(py_file) -> set[str]:
    """Every reachable literal for one file: its own, plus one hop into
    same-repo functions it imports from."""
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))

    pool: set[str] = set()
    module_consts = _module_level_constants(tree)
    pool.update(module_consts.values())
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            pool.update(_string_literals_in_function(node))

    imported_names: dict[str, str] = {}  # local name -> defining module
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                imported_names[alias.asname or alias.name] = node.module

    hopped: set[str] = set()
    for name, module in imported_names.items():
        hop_path = _local_module_path(module)
        if hop_path is None:
            continue
        try:
            hop_tree = ast.parse(hop_path.read_text(encoding="utf-8"), filename=str(hop_path))
        except SyntaxError:
            continue
        hop_consts = _module_level_constants(hop_tree)
        hopped.update(hop_consts.values())
        for node in ast.walk(hop_tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                hopped.update(_return_literals(node, hop_consts))
    pool.update(hopped)
    return pool


def _prefix_of(template: str) -> str:
    return template.split("{", 1)[0].split("*", 1)[0].rstrip("/")


def _count_s3_write_calls(py_file) -> int:
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(py_file))
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in _S3_KEY_CALLS:
                count += 1
    return count


def check_unit(unit: Unit, *, sole_owner: bool) -> Finding:
    """Grade one unit. `sole_owner` says whether its `code_path` file is not
    shared with another unit (via `shared_writers`/`extra_paths`, or by two
    units literally declaring the same file) — Signal B only applies then."""
    if unit.retired:
        return Finding(unit.unit_id, "ok", "retired — not graded")

    files = _declared_code_files(unit)
    py_files = [REPO_ROOT / f for f in files if (REPO_ROOT / f).is_file()]
    if not py_files:
        return Finding(unit.unit_id, "unverifiable", "code_path names no in-repo Python file")

    s3_templates = [w for w in unit.writes if "/" in w and "::" not in w and "(" not in w]
    if not s3_templates:
        return Finding(unit.unit_id, "unverifiable", "no S3-key-shaped writes[] entry to check")

    pool: set[str] = set()
    for py_file in py_files:
        pool |= _candidate_pool(py_file)

    unmatched = []
    for template in s3_templates:
        prefix = _prefix_of(template)
        if not prefix:
            continue  # placeholder from position 0 — nothing static to check
        if not any(prefix == c.rstrip("/") or prefix.startswith(c.rstrip("/")) or c.startswith(prefix)
                   for c in pool):
            unmatched.append(template)

    if unmatched:
        return Finding(
            unit.unit_id, "flagged",
            f"writes[] {unmatched!r} — no matching literal prefix found in "
            f"{[str(f.relative_to(REPO_ROOT)) for f in py_files]} (+ one local import hop)",
        )

    if sole_owner and len(py_files) == 1:
        call_sites = _count_s3_write_calls(py_files[0])
        if call_sites > len(unit.writes):
            return Finding(
                unit.unit_id, "flagged",
                f"{call_sites} S3 write call site(s) in {py_files[0].relative_to(REPO_ROOT)}, "
                f"only {len(unit.writes)} writes[] entr{'y' if len(unit.writes) == 1 else 'ies'} declared",
            )

    return Finding(unit.unit_id, "ok", "")


def run_all(units: list[Unit] | None = None) -> list[Finding]:
    units = units if units is not None else load_units()
    scope = load_inventory_scope()

    shared_files = set(scope.shared_writers) | set(scope.extra_paths)
    code_path_owners: dict[str, list[str]] = {}
    for unit in units:
        for f in _declared_code_files(unit):
            code_path_owners.setdefault(f, []).append(unit.unit_id)

    findings = []
    for unit in units:
        files = _declared_code_files(unit)
        sole_owner = bool(files) and all(
            f not in shared_files and len(code_path_owners.get(f, [])) == 1 for f in files
        )
        findings.append(check_unit(unit, sole_owner=sole_owner))
    return findings
