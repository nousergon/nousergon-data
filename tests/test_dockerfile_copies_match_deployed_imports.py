"""Pin ``Dockerfile`` to COPY every top-level package imported from the
Lambda-deployed Python modules.

Background
----------
The Phase 2 Lambda image only contains the directories the Dockerfile
explicitly ``COPY``s into ``${LAMBDA_TASK_ROOT}``. When a deployed
module gains a top-level ``from X import ...`` for a local package ``X``
that is NOT in the COPY list, the canary fails at module-load time with
``No module named 'X'`` — but only AFTER the image has been built,
pushed to ECR, the new version published, and the alias swapped. The
canary correctly rolls back so production is unaffected, but the latent
break blocks ANY new code from ever reaching ``live``.

This exact failure mode bit production:

  - 2026-05-16 (PR #254 per-collector value-range validation): added
    top-level ``from validators.price_validator import ...`` to
    ``collectors/alternative.py`` + ``collectors/fundamentals.py`` but
    did not add ``COPY validators/`` to the Dockerfile. CI rolled back
    every push for 10 consecutive deploys (5/18-18:20Z through
    5/20-00:25Z) until the Wave-3 PR3-wave-2 deploy (#273) surfaced
    the gap to the operator.

This test scans every Lambda-deployed module's top-level imports for
``from <local_pkg> import ...`` / ``import <local_pkg>`` where
<local_pkg> resolves to a local directory under the repo root. Every
such <local_pkg> must appear in the Dockerfile's COPY directives.
Future PRs that introduce a new local-package import without the
matching Dockerfile COPY fail this test in CI, not in the post-merge
canary.

Single-file root modules (``dates.py``, ``polygon_client.py``, ...)
--------------------------------------------------------------------
The package-directory check above only resolves ``<local_pkg>`` against
repo-root directories that have an ``__init__.py`` — it structurally
cannot see a module-scope ``from dates import ...`` or
``from polygon_client import ...``, since ``dates`` / ``polygon_client``
are single ``.py`` files, not packages. That blind spot is exactly how
the ``dates.py`` miss (config#464: added 2026-06-25 WITHOUT a matching
``COPY dates.py``, canary-failed every deploy with ``No module named
'dates'``) and the earlier ``polygon_client.py`` miss got past review —
both are called out in the Dockerfile's own comments as the same bug
class as the ``validators/`` incident below, but neither was covered by
this file's original directory-only scan. This test file adds a mirror
check for single-file root modules (``COPY <mod>.py
${LAMBDA_TASK_ROOT}/``) so a future regression in either is caught here
too, not just in package directories.

Pattern source
--------------
Generalizes crucible-predictor PR #352
(``tests/test_dockerfile_handler_import_completeness.py``), which
derives the required COPY set from ``inference/handler.py``'s own
top-level imports via a scoped regex. This file already predates that
PR (added by #274 for the ``validators/`` incident) and goes further —
it walks the full Lambda-deployed closure (``lambda/handler.py`` +
every module transitively COPY'd into the image) via ``ast``, not just
the entrypoint's direct imports, since in this repo the entrypoint's
own imports are only one hop from the packages that actually need
COPY'ing (e.g. ``validators`` and ``dates`` are imported by
``collectors/*``, not by ``lambda/handler.py`` itself).
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DOCKERFILE = _REPO_ROOT / "Dockerfile"

# Modules / dirs that DON'T need to be in the image because the deploy
# isn't Lambda-bound, or they're stdlib / third-party (caught by
# requirements.txt). Keep this list tight — additions here are an
# escape hatch and should be justified inline.
_NON_LAMBDA_PACKAGES = frozenset({
    "tests",  # not deployed
    "builders",  # currently NOT deployed; Lambda Phase-2 path doesn't
    # reach builders code. If a Phase-2 import path ever needs builders
    # (e.g. via collectors → weekly_collector → builders), add the
    # COPY here AND remove from this allowlist.
    "infrastructure",  # deploy scripts, never run in Lambda
    "rag",  # RAG ingestion is an EC2 spot stage, not a Lambda
    "features",  # spot-only feature compute
    "validators",  # canonical state added via this PR; kept here in case
    # the import discipline needs to be relaxed later — see below
    # _DEPLOYED_LOCAL_PACKAGES check.
})

# Single-file root modules that DON'T need a ``COPY <mod>.py`` line
# because no Lambda-deployed file imports them at module scope. Same
# escape-hatch discipline as _NON_LAMBDA_PACKAGES above — additions here
# should be justified inline. (``dates`` and ``polygon_client`` are
# deliberately NOT here: both are deployed and module-scope-imported —
# see _LAMBDA_DEPLOYED_FILES / the dedicated dates.py pin test below.)
_NON_LAMBDA_SINGLE_FILE_MODULES = frozenset({
    "emailer",  # imported lazily inside weekly_collector.py functions
    # (e.g. `from emailer import send_step_email`), never at module
    # scope in the Lambda-deployed closure — not required in the image.
    "preflight",  # weekly-SF preflight gate, EC2 spot path only.
    "sf_preflight",  # same — operator/EC2 tooling, not Lambda-deployed.
    "weekly_collector",  # only imported by tests/ (already excluded via
    # _NON_LAMBDA_PACKAGES); not imported by any Lambda-deployed module.
    # It IS itself COPY'd (it's the weekly-SF entrypoint, a separate
    # deploy path from lambda/handler.py), so this only says "nothing
    # else needs to import it" — unrelated to whether it's in the image.
})

# The actual files copied into the Lambda image. Mirror the Dockerfile
# COPY directives' top-level entries.
_LAMBDA_DEPLOYED_FILES = (
    "lambda/handler.py",
    "weekly_collector.py",
    "polygon_client.py",
)
_LAMBDA_DEPLOYED_DIRS = (
    "collectors",
    "store",
    "validators",
)


def _local_packages() -> set[str]:
    """Set of top-level directory names that contain ``__init__.py`` at
    the repo root — i.e. the local packages a deployed module might
    legitimately import from."""
    return {
        p.name for p in _REPO_ROOT.iterdir()
        if p.is_dir() and (p / "__init__.py").exists() and not p.name.startswith(".")
    }


def _local_single_file_modules() -> set[str]:
    """Set of top-level module names for single-``.py``-file modules at
    the repo root (e.g. ``dates``, ``polygon_client``) — the single-file
    counterpart to ``_local_packages()``. These are invisible to the
    directory-based scan above, which is exactly how the ``dates.py``
    (config#464) and ``polygon_client.py`` COPY misses got past review."""
    return {p.stem for p in _REPO_ROOT.glob("*.py")}


def _toplevel_imports(py_file: Path) -> set[str]:
    """Parse ``py_file`` with ast and return the set of top-level
    package names referenced by ``import X`` / ``from X.Y import ...``
    at MODULE scope (not inside functions/classes).
    """
    tree = ast.parse(py_file.read_text(), filename=str(py_file))
    out: set[str] = set()
    for node in tree.body:  # module-scope only — deferred imports are fine
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                out.add(root)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                root = node.module.split(".")[0]
                out.add(root)
    return out


def _walk_python_files(paths: tuple[str, ...]) -> list[Path]:
    """Expand the deployed-paths tuple into actual ``.py`` Paths."""
    out: list[Path] = []
    for p in paths:
        full = _REPO_ROOT / p
        if full.is_file() and full.suffix == ".py":
            out.append(full)
        elif full.is_dir():
            out.extend(full.rglob("*.py"))
    return out


def _dockerfile_copied_dirs() -> set[str]:
    """Parse the Dockerfile and return the set of directory names
    explicitly COPY'd into ``${LAMBDA_TASK_ROOT}``."""
    text = _DOCKERFILE.read_text()
    out: set[str] = set()
    # Match: ``COPY <name>/ ${LAMBDA_TASK_ROOT}/<name>/`` (the trailing
    # slash on the source convention denotes a directory copy).
    for m in re.finditer(
        r"^COPY\s+([A-Za-z_][A-Za-z0-9_]*)/\s+\${LAMBDA_TASK_ROOT}/",
        text,
        flags=re.MULTILINE,
    ):
        out.add(m.group(1))
    return out


def _dockerfile_copied_single_files() -> set[str]:
    """Parse the Dockerfile and return the set of single-file root
    module names (without ``.py``) explicitly ``COPY``'d into
    ``${LAMBDA_TASK_ROOT}`` — the single-file counterpart to
    ``_dockerfile_copied_dirs()``."""
    text = _DOCKERFILE.read_text()
    out: set[str] = set()
    # Match: ``COPY <name>.py ${LAMBDA_TASK_ROOT}/`` (no trailing slash
    # on the source — that's the directory-copy convention matched
    # above). Excludes ``lambda/handler.py`` (has a path separator) and
    # ``requirements.txt`` (not a .py file).
    for m in re.finditer(
        r"^COPY\s+([A-Za-z_][A-Za-z0-9_]*)\.py\s+\${LAMBDA_TASK_ROOT}/",
        text,
        flags=re.MULTILINE,
    ):
        out.add(m.group(1))
    return out


def test_dockerfile_copies_validators_for_collectors_imports():
    """``collectors/alternative.py`` + ``collectors/fundamentals.py``
    have top-level imports from ``validators.price_validator`` since
    PR #254. The Dockerfile MUST COPY ``validators/`` so the canary
    can resolve those imports at Lambda load.
    """
    deployed = _dockerfile_copied_dirs()
    assert "validators" in deployed, (
        "Dockerfile does not COPY ``validators/``. "
        "``collectors/alternative.py`` + ``collectors/fundamentals.py`` "
        "have top-level ``from validators.price_validator import ...`` "
        "since PR #254. Without this COPY the canary fails with "
        "``No module named 'validators'`` and rolls back to the prior "
        "version — every push since 2026-05-18. Add "
        "``COPY validators/ ${LAMBDA_TASK_ROOT}/validators/`` to the "
        "Dockerfile next to the other application-code COPY lines."
    )


def test_dockerfile_copies_dates_for_weekly_collector_imports():
    """``weekly_collector.py`` has a top-level ``from dates import
    default_run_date`` (config#1014: trading-day-axis default). The
    Dockerfile MUST COPY ``dates.py`` so the canary can resolve it at
    Lambda load.

    Mirrors ``test_dockerfile_copies_validators_for_collectors_imports``
    above for the single-file-module case — pins the config#464
    incident (added 2026-06-25 WITHOUT a matching ``COPY dates.py``;
    every deploy canary-failed with ``No module named 'dates'`` until
    fixed) the same way that test pins the ``validators/`` incident.
    """
    deployed = _dockerfile_copied_single_files()
    assert "dates" in deployed, (
        "Dockerfile does not COPY ``dates.py``. ``weekly_collector.py`` "
        "has a top-level ``from dates import default_run_date`` "
        "(config#1014). Without this COPY the canary fails with "
        "``No module named 'dates'`` and rolls back to the prior "
        "version (config#464). Add ``COPY dates.py "
        "${LAMBDA_TASK_ROOT}/`` to the Dockerfile next to the other "
        "application-code COPY lines."
    )


def test_every_toplevel_local_import_in_lambda_code_is_dockerfile_copied():
    """Scan every deployed Python file's MODULE-SCOPE imports. Any
    ``from <pkg> import ...`` / ``import <pkg>`` where ``<pkg>`` is a
    local directory OR single-file module under the repo root MUST be
    in the Dockerfile's COPY list (or in the explicitly-non-deployed
    allowlist).

    Catches the 2026-05-18 ``validators/`` regression class — top-level
    import added to a deployed module without the matching Dockerfile
    COPY — at PR time, not in the post-merge canary rollback. Also
    catches the single-file-module variant of the same bug class
    (``dates.py`` / ``polygon_client.py``, config#464-adjacent), which
    the directory-only version of this scan structurally could not see.
    """
    local_pkgs = _local_packages()
    local_single_files = _local_single_file_modules()
    deployed_dirs = _dockerfile_copied_dirs()
    deployed_single_files = _dockerfile_copied_single_files()
    deployed_files = _walk_python_files(_LAMBDA_DEPLOYED_FILES) + \
        _walk_python_files(_LAMBDA_DEPLOYED_DIRS)

    missing: dict[str, list[str]] = {}
    missing_kind: dict[str, str] = {}
    for py in deployed_files:
        for imp in _toplevel_imports(py):
            if imp in local_pkgs:
                if imp in _NON_LAMBDA_PACKAGES or imp in deployed_dirs:
                    continue
                missing.setdefault(imp, []).append(str(py.relative_to(_REPO_ROOT)))
                missing_kind[imp] = "dir"
            elif imp in local_single_files:
                if imp in _NON_LAMBDA_SINGLE_FILE_MODULES or imp in deployed_single_files:
                    continue
                missing.setdefault(imp, []).append(str(py.relative_to(_REPO_ROOT)))
                missing_kind[imp] = "file"

    assert not missing, (
        "Deployed Lambda code has top-level imports of local "
        "packages/modules that the Dockerfile does NOT COPY. The "
        "canary will fail at load time with ``No module named "
        "'<pkg>'``.\n\nMissing:\n"
        + "\n".join(
            (
                f"  - {pkg}/  (imported by: {', '.join(sorted(set(files)))})"
                if missing_kind[pkg] == "dir"
                else f"  - {pkg}.py  (imported by: {', '.join(sorted(set(files)))})"
            )
            for pkg, files in sorted(missing.items())
        )
        + "\n\nEither add ``COPY <pkg>/ ${LAMBDA_TASK_ROOT}/<pkg>/`` "
        "(directory) or ``COPY <mod>.py ${LAMBDA_TASK_ROOT}/`` "
        "(single-file module) to the Dockerfile, or — if the import is "
        "intentionally deferred and never reached in the Lambda path — "
        "move it inside the function that needs it so it isn't a "
        "module-scope import."
    )
