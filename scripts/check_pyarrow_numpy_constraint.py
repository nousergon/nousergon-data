#!/usr/bin/env python3
"""Check every pyarrow/numpy install surface against constraints/pyarrow-numpy.toml.

alpha-engine-config-I13002 (mechanism record nous-ergon-ops#2041). pyarrow >= 26
refuses to import under NumPy 1.x, and the fleet declared the pair separately on
every surface that installs it, so each October fix pinned one surface and the
break came back through the next. The TOML is the one declaration; this script
is what makes a surface that disagrees with it fail.

    python3 scripts/check_pyarrow_numpy_constraint.py            # this repo's surfaces
    python3 scripts/check_pyarrow_numpy_constraint.py --remote   # + other repos', read from GitHub main

Exit 0 when every checked surface agrees, 1 otherwise (each finding printed).
``--remote`` fails closed: a surface it cannot read is a finding, not a skip.

The pytest gate is tests/test_pyarrow_numpy_constraint.py, which runs the local
half on every PR and also fails on any pyarrow/numpy surface in this repo that
the TOML does not register.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tomllib
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet

REPO_ROOT = Path(__file__).resolve().parent.parent
CONSTRAINT_FILE = REPO_ROOT / "constraints" / "pyarrow-numpy.toml"
THIS_REPO = "nousergon-data"
ORG = "nousergon"

KINDS = ("requirements", "lock", "bootstrap", "dependabot")
#: Dependabot ecosystems that bump Python requirements.
_PY_ECOSYSTEMS = ("pip", "uv")

_REQ_LINE = re.compile(
    r"^\s*(pyarrow|numpy)\s*(?:\[[^\]]*\])?\s*([^;#]*?)\s*(?:;.*)?$", re.IGNORECASE
)
_PYPROJECT_ITEM = re.compile(
    r"""["']\s*(pyarrow|numpy)\s*(?:\[[^\]]*\])?\s*([^"';]*?)\s*(?:;[^"']*)?["']""",
    re.IGNORECASE,
)
_LOCK_LINE = re.compile(r"^\s*pyarrow\s*==\s*([0-9][^\s;#\\]*)", re.IGNORECASE)
#: A non-comment line that pip-installs numpy with an upper bound of 2.
_NUMPY_BELOW_2 = re.compile(r"numpy\s*<\s*2(?![0-9.])")
#: A non-comment line that pip-installs numpy or pyarrow by name.
_PIP_INSTALL_PAIR = re.compile(r"\binstall\b.*\b(numpy|pyarrow)\b", re.IGNORECASE)


@dataclass(frozen=True)
class Surface:
    venv: str
    repo: str
    path: str
    kind: str
    pyarrow_ignore: tuple[str, ...] | None = None
    group: str | None = None

    @property
    def label(self) -> str:
        return f"{self.repo}:{self.path} [{self.kind}, venv {self.venv}]"


@dataclass(frozen=True)
class Venv:
    name: str
    pyarrow: SpecifierSet
    numpy_below_2: bool


def load(path: Path = CONSTRAINT_FILE) -> tuple[str, dict[str, Venv], list[Surface]]:
    data = tomllib.loads(path.read_text())
    threshold = str(data["rule"]["pyarrow_needs_numpy2_from"])
    venvs: dict[str, Venv] = {}
    surfaces: list[Surface] = []
    for name, body in data["venvs"].items():
        venvs[name] = Venv(name, SpecifierSet(body["pyarrow"]), bool(body["numpy_below_2"]))
        for s in body.get("surfaces", []):
            ignore = s.get("pyarrow_ignore")
            surfaces.append(
                Surface(
                    venv=name,
                    repo=s.get("repo", THIS_REPO),
                    path=s["path"],
                    kind=s["kind"],
                    pyarrow_ignore=None if ignore is None else tuple(ignore),
                    group=s.get("group"),
                )
            )
    return threshold, venvs, surfaces


def _probes(major: str) -> list[str]:
    n = int(major)
    return [f"{n}.0.0", f"{n}.0.1", f"{n}.1.0", f"{n + 1}.0.0", f"{n + 4}.0.0", "999.0.0"]


def admits_from(spec: SpecifierSet, major: str) -> bool:
    """True when ``spec`` admits some release at or above ``major``."""
    return any(spec.contains(v) for v in _probes(major))


def admits_numpy_1(spec: SpecifierSet) -> bool:
    return any(spec.contains(v) for v in ("1.0.0", "1.24.0", "1.26.4", "1.99.0"))


def _code_lines(text: str) -> list[str]:
    return [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]


def _strip_inline_comment(line: str) -> str:
    """pip treats `` #`` (whitespace then hash) as the start of a comment."""
    return re.split(r"\s#", line, maxsplit=1)[0]


def requirement_specs(path: str, text: str) -> dict[str, list[str]]:
    """``{"pyarrow": [spec, ...], "numpy": [...]}`` declared by a requirements file or pyproject."""
    found: dict[str, list[str]] = {"pyarrow": [], "numpy": []}
    if path.endswith(".toml"):
        for ln in _code_lines(text):
            for m in _PYPROJECT_ITEM.finditer(ln):
                found[m.group(1).lower()].append(m.group(2).strip())
        return found
    for ln in _code_lines(text):
        m = _REQ_LINE.match(_strip_inline_comment(ln))
        if m:
            found[m.group(1).lower()].append(m.group(2).strip())
    return found


def forces_numpy_below_2(text: str) -> bool:
    return any(
        "install" in ln and "pip" in ln.lower() and _NUMPY_BELOW_2.search(ln)
        for ln in _code_lines(text)
    )


def check_surface(surface: Surface, text: str, venv: Venv, threshold: str) -> list[str]:
    out: list[str] = []
    where = surface.label
    if surface.kind == "requirements":
        specs = requirement_specs(surface.path, text)
        if not specs["pyarrow"]:
            out.append(f"{where}: declares no pyarrow; the venv's pyarrow must be pinned here as {venv.pyarrow}")
        for raw in specs["pyarrow"]:
            try:
                got = SpecifierSet(raw)
            except InvalidSpecifier:
                out.append(f"{where}: unparseable pyarrow spec {raw!r}")
                continue
            if got != venv.pyarrow:
                out.append(
                    f"{where}: pyarrow{raw or ' (unbounded)'} disagrees with the declared "
                    f"pyarrow{venv.pyarrow} (constraints/pyarrow-numpy.toml)"
                )
        if admits_from(venv.pyarrow, threshold):
            for raw in specs["numpy"]:
                try:
                    if admits_numpy_1(SpecifierSet(raw)):
                        out.append(
                            f"{where}: numpy{raw or ' (unbounded)'} admits NumPy 1.x, but the venv's "
                            f"pyarrow{venv.pyarrow} admits >={threshold}, which refuses to import under it"
                        )
                except InvalidSpecifier:
                    out.append(f"{where}: unparseable numpy spec {raw!r}")
    elif surface.kind == "lock":
        pins = [m.group(1) for ln in text.splitlines() if (m := _LOCK_LINE.match(ln))]
        if len(pins) != 1:
            out.append(f"{where}: expected exactly one `pyarrow==` pin, found {len(pins)}")
        for pin in pins:
            if not venv.pyarrow.contains(pin):
                out.append(f"{where}: locks pyarrow=={pin}, outside the declared pyarrow{venv.pyarrow}")
    elif surface.kind == "bootstrap":
        forced = forces_numpy_below_2(text)
        if forced and not venv.numpy_below_2:
            out.append(
                f"{where}: pip-installs numpy<2 into venv {venv.name}, whose declared "
                f"pyarrow{venv.pyarrow} admits >={threshold} (refuses NumPy 1.x). Remove the "
                f"override, or cap that venv's pyarrow below {threshold} in the TOML and every surface"
            )
        if venv.numpy_below_2 and not forced:
            out.append(
                f"{where}: venv {venv.name} is declared numpy_below_2 but this bootstrap no longer "
                f"forces it; update constraints/pyarrow-numpy.toml in the same change"
            )
    elif surface.kind == "dependabot":
        out.extend(_check_dependabot(surface, text))
    else:
        out.append(f"{where}: unknown kind {surface.kind!r}; expected one of {KINDS}")
    return out


def _check_dependabot(surface: Surface, text: str) -> list[str]:
    import yaml  # declared in requirements.txt; imported here so the rest stays stdlib + packaging

    where = surface.label
    out: list[str] = []
    doc = yaml.safe_load(text) or {}
    pip_blocks = [u for u in doc.get("updates", []) if u.get("package-ecosystem") in _PY_ECOSYSTEMS]
    want = sorted(surface.pyarrow_ignore or ())
    if not pip_blocks:
        # Nothing bumps Python deps here, so nothing can drift; a declared hold or
        # group with no block to carry it is the disagreement.
        if want or surface.group:
            return [f"{where}: declares a pyarrow ignore/group but has no pip/uv update block"]
        return []
    for block in pip_blocks:
        got: list[str] = []
        for ign in block.get("ignore", []) or []:
            if str(ign.get("dependency-name", "")).lower() == "pyarrow":
                got.extend(ign.get("versions") or ["*"])
        if sorted(got) != want:
            out.append(
                f"{where} (directory {block.get('directory')!r}): pyarrow ignore {sorted(got)} "
                f"disagrees with the declared {want}"
            )
        if surface.group:
            groups = block.get("groups") or {}
            names = list(groups)
            pats = [p.lower() for p in (groups.get(surface.group) or {}).get("patterns", [])]
            if not names or names[0] != surface.group or not {"pyarrow", "numpy"} <= set(pats):
                out.append(
                    f"{where}: pyarrow and numpy must sit in the FIRST group, {surface.group!r} "
                    f"(patterns pyarrow, numpy), so a held pair bump holds no other update"
                )
    return out


def check_venv_rule(venv: Venv, threshold: str) -> list[str]:
    if venv.numpy_below_2 and admits_from(venv.pyarrow, threshold):
        return [
            f"venv {venv.name}: numpy is forced below 2 but pyarrow{venv.pyarrow} admits "
            f">={threshold}, which refuses to import under NumPy 1.x"
        ]
    return []


# ---------------------------------------------------------------------------
# Reading surfaces


def read_local(path: str) -> str | None:
    p = REPO_ROOT / path
    return p.read_text() if p.is_file() else None


def read_remote(repo: str, path: str) -> str | None:
    """Read ``path`` on ``repo``'s default branch. ``gh`` first (reaches private repos), then raw HTTPS."""
    if shutil.which("gh"):
        r = subprocess.run(
            ["gh", "api", f"repos/{ORG}/{repo}/contents/{path}", "-H", "Accept: application/vnd.github.raw"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if r.returncode == 0:
            return r.stdout
    try:
        with urllib.request.urlopen(
            f"https://raw.githubusercontent.com/{ORG}/{repo}/HEAD/{path}", timeout=60
        ) as resp:
            return resp.read().decode()
    except Exception:  # noqa: BLE001 - unreadable is reported as a finding by the caller
        return None


def run(remote: bool, constraint_file: Path = CONSTRAINT_FILE) -> list[str]:
    threshold, venvs, surfaces = load(constraint_file)
    findings: list[str] = []
    for venv in venvs.values():
        findings.extend(check_venv_rule(venv, threshold))
    for s in surfaces:
        if s.venv not in venvs:
            findings.append(f"{s.label}: unknown venv")
            continue
        if s.repo == THIS_REPO:
            text = read_local(s.path)
        elif remote:
            text = read_remote(s.repo, s.path)
        else:
            continue
        if text is None:
            findings.append(f"{s.label}: registered but not readable (moved or deleted? update the TOML)")
            continue
        findings.extend(check_surface(s, text, venvs[s.venv], threshold))
    return findings


# ---------------------------------------------------------------------------
# Discovery: every surface in THIS repo must be registered.

_EXCLUDED_PREFIXES = ("tests/", "docs/", "constraints/", ".git/")
_EXCLUDED_FILES = {".gitleaks-baseline.json", "scripts/check_pyarrow_numpy_constraint.py"}


def _tracked_files() -> list[str]:
    try:
        r = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "ls-files"], capture_output=True, text=True, check=True
        )
        return [f for f in r.stdout.splitlines() if f]
    except (OSError, subprocess.CalledProcessError):
        return [str(p.relative_to(REPO_ROOT)) for p in REPO_ROOT.rglob("*") if p.is_file()]


def unregistered_local_surfaces(constraint_file: Path = CONSTRAINT_FILE) -> list[str]:
    """Files in this repo that declare or pip-install pyarrow/numpy and are not in the TOML."""
    _, _, surfaces = load(constraint_file)
    registered = {s.path for s in surfaces if s.repo == THIS_REPO}
    out: list[str] = []
    for rel in _tracked_files():
        if rel in registered or rel in _EXCLUDED_FILES or rel.startswith(_EXCLUDED_PREFIXES):
            continue
        name = rel.rsplit("/", 1)[-1]
        if not (
            name.endswith((".txt", ".in", ".toml", ".sh", ".py", ".yml", ".yaml", ".cfg"))
            or name.startswith("Dockerfile")
        ):
            continue
        p = REPO_ROOT / rel
        try:
            text = p.read_text()
        except (OSError, UnicodeDecodeError):
            continue
        if rel == ".github/dependabot.yml":
            out.append(rel)
            continue
        is_req = ("requirements" in name and name.endswith((".txt", ".in"))) or name == "pyproject.toml"
        if is_req and any(requirement_specs(rel, text).values()):
            out.append(rel)
            continue
        if any(
            "pip" in ln.lower() and _PIP_INSTALL_PAIR.search(ln) for ln in _code_lines(text)
        ):
            out.append(rel)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--remote", action="store_true", help="also read other repos' surfaces from GitHub")
    args = ap.parse_args(argv)
    findings = run(remote=args.remote)
    findings.extend(f"{THIS_REPO}:{p}: declares or installs pyarrow/numpy but is not registered in "
                    f"constraints/pyarrow-numpy.toml" for p in unregistered_local_surfaces())
    for f in findings:
        print(f"FAIL {f}")
    scope = "all registered surfaces" if args.remote else f"{THIS_REPO} surfaces"
    print(f"{len(findings)} finding(s) across {scope}")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
