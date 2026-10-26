"""infrastructure/data-trading-requirements.txt is the trading box's dependency
set for this repo's checkout (alpha-engine-config-I12657).

WHY
---
crucible-executor's ``infrastructure/boot-pull.sh`` installs
``infrastructure/data-trading-requirements.txt`` into
``/home/ec2-user/alpha-engine-data/.venv`` whenever the file exists, and falls
back to the full ``requirements.txt`` when it does not. The file never existed,
so every boot of the box that trades installed the whole data stack
(edgartools, voyageai, the RAG chain) and picked up every weekly Dependabot
bump of it. The 2026-10-24 00:31Z boot-pull page (alpha-engine-config-I12615)
raced exactly those upgrades.

What these tests hold:

- every entry is a package ``requirements.txt`` also declares, so the subset
  never introduces a dependency the rest of the repo does not know about;
- no exact ``==`` pins. A copied ``==`` pin lags the next Dependabot bump of
  ``requirements.txt`` and then conflicts with a later nousergon-lib floor at
  boot, on the trading box, where no CI run would see it. (The nousergon-lib
  git ref is held in lockstep by tests/test_lib_pin_lockstep.py instead.);
- the packages left out stay out, and nothing the trading box runs imports
  them. The import walk covers function-local imports too, so it
  over-approximates what the box executes; a hit here means the subset must
  grow (or the import must move), never that this list should shrink quietly.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_TRADING = _REPO / "infrastructure" / "data-trading-requirements.txt"
_ROOT = _REPO / "requirements.txt"

#: What the trading box runs from this checkout (see the file's header).
_ENTRY_POINTS = (
    "infrastructure/systemd/check-systemd-unit-drift.py",
    "weekly_collector.py",
    "builders/daily_append.py",
)

#: Distribution name -> the import name nothing on the trading box may use.
_EXCLUDED = {
    "edgartools": "edgar",
    "voyageai": "voyageai",
    "embit": "embit",
    "beautifulsoup4": "bs4",
}

_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def _normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _entries(path: Path) -> dict[str, str]:
    """Map normalised distribution name -> the requirement line (comment-free)."""
    out: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.split(" #", 1)[0].strip()
        if not line or line.startswith("#"):
            continue
        match = _NAME_RE.match(line)
        assert match, f"{path.name}: unparseable requirement line {raw!r}"
        out[_normalise(match.group(1))] = line
    return out


def test_file_exists_where_boot_pull_looks_for_it():
    assert _TRADING.is_file(), (
        "crucible-executor infrastructure/boot-pull.sh installs "
        "infrastructure/data-trading-requirements.txt on the trading box and "
        "falls back to the full requirements.txt without it"
    )


def test_no_includes_or_option_lines():
    for raw in _TRADING.read_text().splitlines():
        line = raw.strip()
        assert not line.startswith("-"), (
            f"{line!r}: an include or option line (e.g. `-r requirements.txt`) "
            "would bring the full data stack back onto the trading box"
        )


def test_every_entry_is_declared_in_requirements_txt():
    trading = _entries(_TRADING)
    root = _entries(_ROOT)
    assert trading, "data-trading-requirements.txt declares nothing"
    unknown = sorted(set(trading) - set(root))
    assert not unknown, (
        f"{unknown} are in data-trading-requirements.txt but not in "
        "requirements.txt: the trading subset must stay a subset"
    )


def test_no_exact_pins():
    exact = [line for line in _entries(_TRADING).values() if "==" in line]
    assert not exact, (
        f"{exact}: an exact pin here lags requirements.txt after its next bump "
        "and can then conflict with nousergon-lib's own floor at boot on the "
        "trading box; let the dependency ride the lib (krepis does) or use the "
        "same floor requirements.txt carries"
    )


def test_excluded_packages_are_not_declared():
    declared = sorted(set(_entries(_TRADING)) & set(_EXCLUDED))
    assert not declared, (
        f"{declared} are deliberately left off the trading box; see the "
        "header of infrastructure/data-trading-requirements.txt"
    )


def _local_module_file(module: str) -> Path | None:
    base = _REPO / module.replace(".", "/")
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _import_closure(entries: tuple[str, ...]) -> dict[str, set[str]]:
    """Third-party top-level import name -> repo files importing it, over the
    transitive closure of in-repo imports (function-local ones included)."""
    local_tops = {
        p.stem if p.suffix == ".py" else p.name
        for p in _REPO.iterdir()
        if p.suffix == ".py" or (p.is_dir() and not p.name.startswith("."))
    }
    third: dict[str, set[str]] = {}
    seen: set[Path] = set()
    stack = [_REPO / e for e in entries]
    while stack:
        path = stack.pop()
        if path in seen:
            continue
        seen.add(path)
        rel = path.relative_to(_REPO)
        tree = ast.parse(path.read_text(), filename=str(rel))
        package = ".".join(rel.parent.parts)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                if node.level:
                    parts = package.split(".") if package else []
                    parts = parts[: len(parts) - (node.level - 1)]
                    base = ".".join(parts + ([node.module] if node.module else []))
                else:
                    base = node.module or ""
                names = [base] + [f"{base}.{a.name}" for a in node.names]
            else:
                continue
            for name in filter(None, names):
                top = name.split(".")[0]
                if top in local_tops:
                    pieces = name.split(".")
                    for i in range(1, len(pieces) + 1):
                        found = _local_module_file(".".join(pieces[:i]))
                        if found is not None:
                            stack.append(found)
                elif top not in sys.stdlib_module_names:
                    third.setdefault(top, set()).add(str(rel))
    return third


def test_trading_box_entry_points_import_no_excluded_package():
    third = _import_closure(_ENTRY_POINTS)
    hits = {
        dist: sorted(third[mod])
        for dist, mod in _EXCLUDED.items()
        if mod in third
    }
    assert not hits, (
        f"code the trading box runs now imports a package its subset leaves "
        f"out: {hits}. Add it to infrastructure/data-trading-requirements.txt "
        "(and drop it from _EXCLUDED here), or keep the import off the "
        "trading-box path"
    )


def test_closure_walk_sees_the_packages_it_must():
    """Guards the walker itself: if it stopped following imports, the test
    above would pass on an empty closure."""
    third = _import_closure(_ENTRY_POINTS)
    for expected in ("boto3", "pandas", "arcticdb", "nousergon_lib", "krepis"):
        assert expected in third, f"import walk lost {expected}"
