#!/usr/bin/env python3
"""Every declared suppression has an owning OPEN issue and a future expiry.

alpha-engine-config-I9727: 277 suppressed defects fleet-wide, zero with an
expiry, and 13 outliving an issue already CLOSED. The ticket gets closed on
the strength of the mechanism landing, the exemption stays, and from then on
nothing knows the defect is live. This check reads
``private-docs/SUPPRESSION_REGISTRY.yaml`` and fails when:

  * a declared collection or key no longer exists (it drained: delete or
    shrink the row, so the registry cannot keep excusing a fixed defect);
  * an ``exhaustive`` collection holds a key the registry does not declare
    (it grew: a new suppression needs its own issue and expiry);
  * a row's ``expires`` date has arrived;
  * with ``--live``, a row's owning issue is not OPEN.

The collections are read with ``ast`` and never imported, so the offline half
needs only PyYAML and runs as a pytest guard. The ``--live`` half needs a
GitHub token and the REST API.

A collection may also be a top-level mapping (or list) in a ``.json`` /
``.yaml`` / ``.yml`` file, such as a shrink-only findings baseline, named
``<file>::<top-level key>``. A row whose ``keys`` is the string ``all`` covers
the whole collection without enumerating it: for a ledger whose own guard
already holds it shrink-only, the registry adds the owner and the expiry, and
fails once the collection is empty (drained: delete the row).

This file is copied verbatim into each repo that carries a suppression
registry (alpha-engine-config, nousergon-lib, nous-ergon-ops, nousergon-data);
only ``REGISTRY`` differs. Change all copies together.

    check_suppression_registry.py            # offline half, exit 1 on a finding
    GH_TOKEN=... check_suppression_registry.py --live   # also: every owning issue is OPEN
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY = REPO_ROOT / ".suppression-registry.yaml"
OWNING_REPO = "nousergon/alpha-engine-config"
_ISSUE_RE = re.compile(r"^alpha-engine-config-I(\d+)$")


#: ``keys: all`` -- the row covers every entry of the collection.
ALL_KEYS = "all"


@dataclass(frozen=True)
class Row:
    collection: str
    keys: tuple[str, ...]
    exhaustive: bool
    issue: str
    expires: dt.date
    whole: bool = False

    @property
    def path(self) -> str:
        return self.collection.split("::", 1)[0]

    @property
    def name(self) -> str:
        return self.collection.split("::", 1)[1]


def load_rows(registry: Path = REGISTRY) -> tuple[list[Row], list[str]]:
    """``(rows, problems)``. A malformed row is a problem, never skipped."""
    doc = yaml.safe_load(registry.read_text(encoding="utf-8")) or {}
    rows: list[Row] = []
    problems: list[str] = []
    for i, raw in enumerate(doc.get("suppressions") or []):
        where = f"suppressions[{i}]"
        if not isinstance(raw, dict):
            problems.append(f"{where}: not a mapping")
            continue
        collection = str(raw.get("collection") or "")
        if "::" not in collection:
            problems.append(f"{where}: collection must be <path>::<name>, got {collection!r}")
            continue
        keys = raw.get("keys")
        whole = keys == ALL_KEYS
        if whole:
            keys = []
        elif not isinstance(keys, list) or not keys or not all(isinstance(k, str) for k in keys):
            problems.append(
                f"{collection}: keys must be a non-empty list of repr() strings, or {ALL_KEYS!r}")
            continue
        if not isinstance(raw.get("exhaustive"), bool):
            problems.append(f"{collection}: exhaustive must be true or false")
            continue
        issue = str(raw.get("issue") or "")
        if not _ISSUE_RE.match(issue):
            problems.append(f"{collection}: issue must be alpha-engine-config-I<N>, got {issue!r}")
            continue
        expires = raw.get("expires")
        if not isinstance(expires, dt.date):
            problems.append(f"{collection}: expires must be a YYYY-MM-DD date, got {expires!r}")
            continue
        rows.append(Row(collection, tuple(keys), raw["exhaustive"], issue, expires, whole))
    return rows, problems


_DATA_SUFFIXES = {".json", ".yaml", ".yml"}


def _data_keys(path: Path, name: str) -> list[str] | None:
    """repr() of every key of top-level ``name`` in a JSON/YAML file. A list
    yields its scalar elements; anything else (or an unreadable file) is None."""
    try:
        text = path.read_text(encoding="utf-8")
        doc = json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)
    except (OSError, ValueError, yaml.YAMLError):
        return None
    if not isinstance(doc, dict) or name not in doc:
        return None
    value = doc[name]
    if isinstance(value, dict):
        return [repr(k) for k in value]
    if isinstance(value, list) and all(not isinstance(v, (dict, list)) for v in value):
        return [repr(v) for v in value]
    return None


def collection_keys(path: Path, name: str) -> list[str] | None:
    """The repr() of every key of a module-level literal, or None if absent.

    A dict yields its keys; a set, frozenset, list or tuple literal (also
    wrapped in ``frozenset(...)``/``set(...)``) yields its elements. A
    ``.json``/``.yaml`` file yields the keys of its top-level ``name``.
    """
    if path.suffix in _DATA_SUFFIXES:
        return _data_keys(path, name)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return None
    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target, value = node.target.id, node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target, value = node.targets[0].id, node.value
        else:
            continue
        if target != name or value is None:
            continue
        if isinstance(value, ast.Call) and value.args and isinstance(value.func, ast.Name) \
                and value.func.id in {"frozenset", "set", "tuple", "list"}:
            value = value.args[0]
        if isinstance(value, ast.Dict):
            items = value.keys
        elif isinstance(value, (ast.Set, ast.List, ast.Tuple)):
            items = value.elts
        else:
            return None
        try:
            return [repr(ast.literal_eval(k)) for k in items if k is not None]
        except ValueError:
            return None
    return None


def offline_problems(rows: list[Row], today: dt.date, root: Path = REPO_ROOT) -> list[str]:
    problems: list[str] = []
    for row in rows:
        actual = collection_keys(root / row.path, row.name)
        if actual is None:
            problems.append(
                f"{row.collection}: not found as a module-level literal. If it drained "
                "or was deleted, delete this row")
            continue
        if row.whole and not actual:
            problems.append(
                f"{row.collection}: is empty. The suppression drained: delete this row")
        for key in row.keys:
            if key not in actual:
                problems.append(
                    f"{row.collection}: declared key {key} is gone. The suppression "
                    "drained: remove the key from the row (or the row)")
        if row.exhaustive and not row.whole:
            for key in sorted(set(actual) - set(row.keys)):
                problems.append(
                    f"{row.collection}: holds {key}, which the registry does not "
                    "declare. A new suppression needs its own row, issue and expiry")
        if row.expires <= today:
            problems.append(
                f"{row.collection}: expired {row.expires.isoformat()} ({row.issue}). "
                "Drain it, or re-argue it on the issue and move the date")
    return problems


def _issue_state(number: int, token: str) -> str:
    req = urllib.request.Request(  # noqa: S310 -- fixed https host
        f"https://api.github.com/repos/{OWNING_REPO}/issues/{number}",
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
        return str(json.load(resp).get("state"))


def live_problems(rows: list[Row], state_of=None) -> list[str]:
    """Every row whose owning issue is not OPEN. An unreadable state is a
    problem too: a check that cannot look must not report the issue open."""
    if state_of is None:
        # GH_TOKEN only: the caller (a workflow step's `env:`, or `gh auth
        # token` by hand) passes it. nousergon-data's secret-read guard forbids
        # reading the pinned GITHUB_TOKEN name from the environment.
        token = os.environ.get("GH_TOKEN") or ""
        if not token:
            return ["--live: GH_TOKEN is empty, so no issue state was read"]
        state_of = lambda n: _issue_state(n, token)  # noqa: E731
    problems: list[str] = []
    for row in rows:
        number = int(_ISSUE_RE.match(row.issue).group(1))
        try:
            state = state_of(number)
        except Exception as exc:  # noqa: BLE001 -- reported, never swallowed
            problems.append(f"{row.collection}: {row.issue} state UNREAD: {exc}")
            continue
        if state != "open":
            problems.append(
                f"{row.collection}: owning issue {row.issue} is {state} while the "
                "suppression stands. Drain it, or reopen or refile the issue")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--live", action="store_true", help="also require every owning issue OPEN")
    ap.add_argument("--registry", type=Path, default=REGISTRY)
    args = ap.parse_args(argv)
    rows, problems = load_rows(args.registry)
    problems += offline_problems(rows, dt.date.today())
    if args.live:
        problems += live_problems(rows)
    print(f"{len(rows)} declared suppression(s) in {args.registry.name}")
    for p in problems:
        print(f"::error::{p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
