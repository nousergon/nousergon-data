"""Source-derived guard for the hermetic Lambda test/deploy gates (config#1746).

## Why this exists

Several Lambda handler unit tests run in a *hermetic* interpreter — the
pre-deploy gate in each ``deploy.sh`` runs ``pytest`` on **bare python + boto3**
and deliberately does NOT install the git-only ``nousergon_lib`` /
``alpha_engine_lib`` packages the handler imports at module scope. Those imports
are satisfied by a hand-written ``sys.modules`` stub block at the top of the
test file, which MUST be kept in lockstep with ``index.py``'s (transitive)
module-level import graph.

That hand-maintained lockstep has drifted **twice**, same class each time:

* 2026-07-02 — ``No module named pytest`` (gate assumed pytest/boto3 ambient).
* 2026-07-04 — ``No module named nousergon_lib`` (the config#1742 flow-doctor
  cutover moved ``index.py`` onto ``nousergon_lib.*`` but the stub was dropped
  instead of migrated).

Each drift surfaced only as a cryptic ``ModuleNotFoundError`` raised by
``import index`` **at deploy time** (post-merge), redding ``main``.

## What this guards

Rather than continue to hand-maintain (and silently drift) the stub, this
helper **derives the invariant from the live source**: it statically walks
``index.py``'s module-level imports — transitively through any sibling handler
modules that live inside the Lambda package (e.g. ``flow_doctor_telegram``) —
and asserts that every non-stdlib, non-installed module the handler imports is
already resolvable (installed) or present in ``sys.modules`` (stubbed) BEFORE
``import index`` runs.

If ``index.py`` (or one of its local siblings) grows a new git-only import that
the stub does not cover, this fails LOUD with a precise, actionable message
naming the exact missing module — turning a silent deploy-time
``ModuleNotFoundError`` into a source-derived pre-merge test failure.

Call it from a hermetic test's stub block, right after the stubs are installed
and before ``import index``::

    from _shared.hermetic_import_guard import assert_hermetic_imports_satisfied
    assert_hermetic_imports_satisfied(__file__)

``__file__`` is the *test* file; the handler (``index.py``) is resolved as its
sibling. Pass ``handler="foo.py"`` to point at a differently-named handler.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

# Modules the hermetic gate installs explicitly (deploy.sh: `pip install boto3`,
# plus pytest as the runner). Treated as always-available so the guard does not
# demand a stub for them.
_GATE_INSTALLED = frozenset({"boto3", "botocore", "pytest"})


def _top_level_module_names(source: str) -> set[str]:
    """Return the fully-dotted module targets of every module-level import.

    Only module-scope imports matter — imports inside functions are deferred and
    do not run at ``import index`` time. ``from a.b import c`` yields ``a.b``
    (Python must import the submodule ``a.b``); ``import a.b`` yields ``a.b``.
    """
    targets: set[str] = set()
    tree = ast.parse(source)
    for node in tree.body:  # module scope only — do not recurse into functions
        if isinstance(node, ast.Import):
            for alias in node.names:
                targets.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            # Skip relative imports (level > 0) and __future__.
            if node.level == 0 and node.module and node.module != "__future__":
                targets.add(node.module)
    return targets


def _is_local_sibling(module: str, search_dirs: list[Path]) -> Path | None:
    """Resolve ``module`` to a ``.py`` file inside the Lambda package, if any."""
    rel = Path(*module.split("."))
    for base in search_dirs:
        candidate = base / rel.with_suffix(".py")
        if candidate.is_file():
            return candidate
        pkg = base / rel / "__init__.py"
        if pkg.is_file():
            return pkg
    return None


def _is_satisfied(module: str) -> bool:
    """True if ``module`` is stubbed (in sys.modules), installed, or stdlib."""
    if module in sys.modules:
        return True
    top = module.split(".")[0]
    if top in sys.modules or top in _GATE_INSTALLED:
        return True
    if top in sys.stdlib_module_names:
        return True
    try:
        # find_spec raises ModuleNotFoundError when a parent package is a bare
        # stub with no __path__; that means "not installed" → not satisfied.
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def assert_hermetic_imports_satisfied(
    test_file: str, *, handler: str = "index.py"
) -> None:
    """Fail loud if the handler's module-level imports are not all satisfiable.

    Walks ``handler``'s module-level imports transitively through sibling
    modules that live inside the Lambda package (so a git-only import pulled in
    *via* ``flow_doctor_telegram`` is caught too), and asserts each non-local,
    non-stdlib, non-installed module is already in ``sys.modules`` (stubbed).

    Args:
        test_file: the calling test's ``__file__``.
        handler: the handler module filename (default ``index.py``), resolved as
            a sibling of ``test_file``.

    Raises:
        AssertionError: naming the exact unsatisfied module(s) and how to fix.
    """
    test_dir = Path(test_file).resolve().parent
    # sys.path in the hermetic tests inserts both the Lambda dir and the shared
    # lambdas/ root; mirror that so sibling handler modules resolve either place.
    search_dirs = [test_dir, test_dir.parent]

    handler_path = test_dir / handler
    if not handler_path.is_file():
        raise AssertionError(
            f"hermetic_import_guard: handler {handler!r} not found next to "
            f"{test_file!r}"
        )

    # BFS over local modules, collecting external imports along the way.
    seen: set[Path] = set()
    queue: list[Path] = [handler_path]
    external: set[str] = set()
    while queue:
        path = queue.pop()
        if path in seen:
            continue
        seen.add(path)
        for module in _top_level_module_names(path.read_text()):
            if module in sys.modules:
                # Already stubbed/imported: the real source will not execute, so
                # its own imports are irrelevant. This covers a test that stubs a
                # whole sibling module (e.g. flow_doctor_telegram) wholesale
                # rather than letting the real file import its git-only deps.
                continue
            sibling = _is_local_sibling(module, search_dirs)
            if sibling is not None:
                queue.append(sibling)  # walk transitively into the package
            else:
                external.add(module)

    unsatisfied = sorted(m for m in external if not _is_satisfied(m))
    if unsatisfied:
        raise AssertionError(
            "hermetic_import_guard (config#1746): "
            f"{handler} imports {unsatisfied} at module scope, but "
            "the deploy test-gate neither installs nor stubs "
            f"{'them' if len(unsatisfied) > 1 else 'it'}. Either add the "
            "module to the sys.modules stub block above (git-only deps like "
            "nousergon_lib.*) or install it in deploy.sh's gate. This derives "
            "the stub requirement from index.py's live import graph so it "
            "cannot silently drift again."
        )
