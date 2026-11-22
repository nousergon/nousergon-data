"""Class guard: no `except Exception` handler in this repo's source
directories may swallow a failure into `logger.debug(...)` (or `log.debug`)
or a bare `pass`, with the root logger at INFO on every entrypoint — that
means the record is emitted **nowhere** (alpha-engine-config-I10031, the
originating incident in `crucible-executor`).

This repo is the fleet's PRODUCER — it owns the ArcticDB price universe,
macro indicators, the engineered feature store, corporate actions, RAG
ingestion, and per-ticker alternative data (see `AGENTS.md`). Its own
standing rule is stricter than the fleet default: "the fleet's fail-loud
default has no graceful-degrade carve-out on any writer here. A collector
that swallows and returns partial data is a silent corruption of every
consumer, not a degraded run." Every entry in
`.debug-swallow-allowlist.yaml` was triaged against that rule specifically —
see the file header there.

The detector retains the fleet guard's `_is_debug_only_or_pass` shape, but
this repository's call-site also derives a movement-tolerant identity from the
enclosing symbol and a content-sensitive fingerprint from the AST of the whole
`try` statement that holds the swallow: what it guards AND how it swallows.
Hashing the handler alone is not enough — every `except Exception: pass` hashes
identically, so the fingerprint would say nothing about which failure is being
swallowed, and an edit that moves a data write into a cosmetic `try` would keep
its allowance (alpha-engine-config-I10944: "blind to a swallow changing
meaning"). Source positions and comments are excluded from the fingerprint, so
unrelated line movement does not invalidate an allowance while a change to the
guarded statements or the handler does.

A new site with no allowlist entry fails the build (`_UNCOVERED` case). An
allowlist entry whose `expires` has passed fails loudly — re-justify or
remove, never silently re-grandfather (`_EXPIRED` case). An entry that no
longer matches anything ALSO fails, so the allowance cannot quietly widen
after the site it covered is fixed (`_STALE` case) — mirrors
`.provider-linkage-allowlist.yaml` / `nousergon-lib/scripts/
provider_linkage_guard.py` (alpha-engine-config-I9295).
"""

from __future__ import annotations

import ast
import datetime as dt
import hashlib
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ALLOWLIST_PATH = _REPO_ROOT / ".debug-swallow-allowlist.yaml"
_SOURCE_DIRS = (
    "",
    "builders",
    "collectors",
    "contracts",
    "corporate_actions",
    "data",
    "features",
    "infrastructure",
    "lambda",
    "migrations",
    "rag",
    "scripts",
)


def _is_debug_call(statement: ast.stmt) -> bool:
    return (
        isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Call)
        and isinstance(statement.value.func, ast.Attribute)
        and statement.value.func.attr == "debug"
        and isinstance(statement.value.func.value, ast.Name)
        and statement.value.func.value.id == "logger"
    )


def _is_debug_only_or_pass(body: list[ast.stmt]) -> bool:
    records = [statement for statement in body if isinstance(statement, ast.Pass) or _is_debug_call(statement)]
    if len(records) != 1:
        return False
    return all(
        statement in records or isinstance(statement, (ast.Continue, ast.Break, ast.Return))
        for statement in body
    )


def _swallow_fingerprint(statement: ast.Try | ast.TryStar) -> str:
    """Hash the guarded statements and handlers, excluding source positions."""
    content = ast.dump(statement, annotate_fields=True, include_attributes=False)
    return hashlib.sha256(content.encode()).hexdigest()


def _swallows_in_file(path: Path, *, repo_root: Path) -> list[dict[str, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    relative_path = str(path.relative_to(repo_root))
    found: list[dict[str, str]] = []

    def visit(node: ast.AST, symbols: tuple[str, ...]) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols = (*symbols, node.name)
        if isinstance(node, (ast.Try, ast.TryStar)):
            for handler in node.handlers:
                if (
                    isinstance(handler.type, ast.Name)
                    and handler.type.id == "Exception"
                    and _is_debug_only_or_pass(handler.body)
                ):
                    symbol = ".".join(symbols) if symbols else "<module>"
                    found.append(
                        {
                            "path": relative_path,
                            "identity": f"{relative_path}::{symbol}",
                            "fingerprint": _swallow_fingerprint(node),
                        }
                    )
        for child in ast.iter_child_nodes(node):
            visit(child, symbols)

    visit(tree, ())
    return found


def _all_swallow_sites(*, repo_root: Path = _REPO_ROOT) -> list[dict[str, str]]:
    sites: list[dict[str, str]] = []
    for relative_dir in _SOURCE_DIRS:
        source_dir = repo_root / relative_dir if relative_dir else repo_root
        for path in sorted(source_dir.glob("*.py")):
            sites.extend(_swallows_in_file(path, repo_root=repo_root))
    return sites


def _load_allowlist(path: Path = _ALLOWLIST_PATH) -> list[dict[str, Any]]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert document.get("schema_version") == 2, "unrecognized allowlist schema_version"
    return document["entries"]


def _check_allowlist(
    live_sites: list[dict[str, str]], allowlist: list[dict[str, Any]]
) -> list[str]:
    failures: list[str] = []
    required = ("path", "identity", "fingerprint", "reason", "expires", "tracking")
    for entry in allowlist:
        if "line" in entry:
            failures.append(f"line-number key is forbidden: {entry}")
        for field in required:
            if not str(entry.get(field) or "").strip():
                failures.append(f"allowlist entry missing {field!r}: {entry}")

    live_by_identity: dict[str, list[dict[str, str]]] = {}
    for site in live_sites:
        live_by_identity.setdefault(site["identity"], []).append(site)
    allowed_by_identity: dict[str, list[dict[str, Any]]] = {}
    for entry in allowlist:
        identity = entry.get("identity")
        if isinstance(identity, str):
            allowed_by_identity.setdefault(identity, []).append(entry)

    for identity, entries in allowed_by_identity.items():
        if len(entries) > 1:
            failures.append(f"{identity} has duplicate allowlist entries")
    for identity, sites in live_by_identity.items():
        if len(sites) > 1:
            failures.append(f"{identity} identifies multiple live swallows")

    today = dt.date.today()
    for entry in allowlist:
        identity = entry.get("identity")
        if not isinstance(identity, str):
            continue
        try:
            expires = dt.date.fromisoformat(str(entry["expires"]))
        except (KeyError, ValueError):
            failures.append(f"{identity} has invalid expiry {entry.get('expires')!r}")
            continue
        if expires < today:
            failures.append(f"{identity} expired {expires} — re-justify or remove")
        matching_sites = live_by_identity.get(identity, [])
        if not matching_sites:
            failures.append(f"{identity} is stale — no matching debug-only/pass swallow")
        elif all(site["fingerprint"] != entry.get("fingerprint") for site in matching_sites):
            failures.append(f"{identity} changed-body — swallow fingerprint differs")

    for site in live_sites:
        entries = allowed_by_identity.get(site["identity"], [])
        if not any(entry.get("fingerprint") == site["fingerprint"] for entry in entries):
            if entries:
                continue  # The more specific changed-body failure was emitted above.
            failures.append(f"{site['identity']} is uncovered — new debug-only/pass swallow")
    return failures


def test_no_new_debug_only_swallows_outside_allowlist() -> None:
    failures = _check_allowlist(_all_swallow_sites(), _load_allowlist())
    assert not failures, "\n".join(failures)


def test_allowlist_rejects_line_number_key() -> None:
    allowlist = [
        {
            "path": "example.py",
            "identity": "example.py::run",
            "fingerprint": "abc",
            "line": 12,
            "reason": "failure mode and recording surface",
            "expires": "2099-01-01",
            "tracking": "issue-1",
        }
    ]
    assert any("line-number key is forbidden" in failure for failure in _check_allowlist([], allowlist))


def test_movement_is_ignored_but_changed_body_is_rejected(tmp_path: Path) -> None:
    source = "def run():\n    try:\n        work()\n    except Exception:\n        pass\n"
    path = tmp_path / "example.py"
    path.write_text(source, encoding="utf-8")
    site = _swallows_in_file(path, repo_root=tmp_path)[0]
    entry: dict[str, Any] = {
        **site,
        "reason": "failure mode and recording surface",
        "expires": "2099-01-01",
        "tracking": "issue-1",
    }
    path.write_text("\n\n" + source, encoding="utf-8")
    moved = _swallows_in_file(path, repo_root=tmp_path)
    assert not _check_allowlist(moved, [entry])

    path.write_text(source.replace("pass", 'logger.debug("ignored")'), encoding="utf-8")
    changed = _swallows_in_file(path, repo_root=tmp_path)
    assert any("changed-body" in failure for failure in _check_allowlist(changed, [entry]))


def test_changing_what_an_allowed_swallow_guards_is_rejected(tmp_path: Path) -> None:
    """Same handler, new guarded statement: the swallow now hides a different
    failure, so the allowance must not carry over (alpha-engine-config-I10944)."""
    source = "def run():\n    try:\n        parse_duration()\n    except Exception:\n        pass\n"
    path = tmp_path / "example.py"
    path.write_text(source, encoding="utf-8")
    site = _swallows_in_file(path, repo_root=tmp_path)[0]
    entry: dict[str, Any] = {
        **site,
        "reason": "failure mode and recording surface",
        "expires": "2099-01-01",
        "tracking": "issue-1",
    }
    path.write_text(source.replace("parse_duration()", "write_prices()"), encoding="utf-8")
    changed = _swallows_in_file(path, repo_root=tmp_path)
    assert changed[0]["identity"] == site["identity"]
    assert _check_allowlist(changed, [entry]) == ["example.py::run changed-body — swallow fingerprint differs"]

    path.write_text(source.replace("        pass\n", "        # cosmetic only\n        pass\n"), encoding="utf-8")
    assert not _check_allowlist(_swallows_in_file(path, repo_root=tmp_path), [entry])


def test_blank_reason_is_rejected() -> None:
    entry = {
        "path": "example.py",
        "identity": "example.py::run",
        "fingerprint": "abc",
        "reason": "   ",
        "expires": "2099-01-01",
        "tracking": "issue-1",
    }
    assert any("missing 'reason'" in failure for failure in _check_allowlist([], [entry]))


def test_new_swallow_is_uncovered(tmp_path: Path) -> None:
    path = tmp_path / "new.py"
    path.write_text("try:\n    work()\nexcept Exception:\n    pass\n", encoding="utf-8")
    failures = _check_allowlist(_swallows_in_file(path, repo_root=tmp_path), [])
    assert failures == ["new.py::<module> is uncovered — new debug-only/pass swallow"]


def test_duplicate_allowlist_identity_is_rejected() -> None:
    entry = {
        "path": "example.py",
        "identity": "example.py::run",
        "fingerprint": "abc",
        "reason": "failure mode and recording surface",
        "expires": "2099-01-01",
        "tracking": "issue-1",
    }
    failures = _check_allowlist([], [entry, entry.copy()])
    assert "example.py::run has duplicate allowlist entries" in failures


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
