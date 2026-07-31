"""Chokepoint guard — every ``notify_via_flow_doctor`` call site in production
code must be cited in ``alpha-engine-config/private-docs/severity_taxonomy.md``
(the fleet severity SSoT, config#3209) or reconciled from live code with a
date stamp pending a taxonomy entry.

The reconciliation table in that doc tracks (site, event_class, severity,
silent, citation) for the known call sites as of 2026-07-22. This test is the
machine-enforceable chokepoint (alpha-engine-config#3267) that prevents a new
Lambda/script from adding a ``notify_via_flow_doctor`` call without registering
it here.

This guard lives in nousergon-data because 12 of the 13 known fleet call sites
are in this repo. The 13th (groom-notify.sh in alpha-engine-config) uses
``FlowDoctor.notify_event``, not ``notify_via_flow_doctor`` directly, so this
check's scan pattern does not catch it — its chokepoint lives separately in
alpha-engine-config's own CI (tracked at alpha-engine-config#3267).

Shape: scan production files for ``notify_via_flow_doctor(`` calls, identify the
enclosing function, and assert every (file, function) pair is in this module's
embedded registry. New call sites fail the test until registered here.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent

# Matches a bare `notify_via_flow_doctor(` call — not inside a string, not
# commented out. The leading word-boundary anchor avoids matching
# `def notify_via_flow_doctor` (the definition itself in flow_doctor_telegram.py).
_CALL_SITE_RE = re.compile(r"\bnotify_via_flow_doctor\s*\(")

# Directories/files that are not production paths.
_EXCLUDE_PREFIXES = (
    "tests/",
    "__pycache__/",
    ".venv",
    ".git/",
    "venv/",
)

# The flow-doctor sink itself — this is the DEFINITION of notify_via_flow_doctor,
# not a call site. Permanently excluded from the scan.
_SINK_FILE = "infrastructure/lambdas/flow_doctor_telegram.py"


# ---------------------------------------------------------------------------
# Embedded severity-taxonomy registry
# ---------------------------------------------------------------------------
#
# Every ``notify_via_flow_doctor`` call site MUST appear here, keyed by
# repo-relative file path and enclosing function name.
#
# Entries with ``citation`` starting with ``severity_taxonomy.md`` are
# reconciled against the fleet SSoT (alpha-engine-config/private-docs/
# severity_taxonomy.md, config#3209). Entries with ``reconciled from live code``
# were discovered during the initial chokepoint bootstrap (2026-07-31) and are
# pending formal taxonomy entries — their severity/silent values were read
# directly from the source, not from the SSoT doc.
#
# When adding a NEW call site:
#   1. Add an entry here with the correct file, function, severity, and silent.
#   2. Cite ``severity_taxonomy.md`` if the entry IS in the SSoT, or use
#      ``reconciled from live code YYYY-MM-DD — pending taxonomy entry`` if not yet.
#   3. If the call site's event class / severity / silent params genuinely do
#      not fit the taxonomy model (e.g. a dynamic helper that passes through
#      the caller's choice), note that as the ``event_class``.

_REGISTRY: dict[str, dict[str, list[dict]]] = {
    # --- DOCUMENTED in severity_taxonomy.md ---
    "infrastructure/lambdas/sf-telegram-notifier/index.py": {
        "handler": [
            {
                "event_class": "SF execution RUNNING (mid-execution ping)",
                "severity": "info",
                "silent": True,
                "citation": "severity_taxonomy.md row: index.py:162",
            },
            {
                "event_class": "SF execution SUCCEEDED, no hollow-suspect",
                "severity": "info",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:191-193",
            },
            {
                "event_class": "SF execution SUCCEEDED, hollow-suspect (implausibly fast)",
                "severity": "warning",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:180-183,209",
            },
            {
                "event_class": "SF execution FAILED / TIMED_OUT / ABORTED",
                "severity": "warning",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:68-71,191",
            },
        ],
    },
    "infrastructure/lambdas/scheduled-groom-dispatcher/index.py": {
        "_notify_cycle": [
            {
                "event_class": "Groom CYCLE lifecycle ping (STARTED/COMPLETE) — severity passed by caller, always loud",
                "severity": "dynamic (caller-chosen: info/warning/error)",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry; new caller added post-taxonomy-bootstrap",
            },
        ],
        "_notify_concurrent_cycle_skip": [
            {
                "event_class": "Groom CYCLE skipped — earlier dispatch cycle still RUNNING",
                "severity": "warning",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry; added post-taxonomy-bootstrap (config-I5371)",
            },
        ],
        "_notify_concurrent_skip": [
            {
                "event_class": "Concurrent-same-lane skip",
                "severity": "info",
                "silent": True,
                "citation": "severity_taxonomy.md row: index.py:418-430",
            },
        ],
        "_notify_dispatch_ceiling_exhausted": [
            {
                "event_class": "Daily dispatch ceiling exhausted",
                "severity": "error",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:487-504",
            },
        ],
        "_notify_demand_trigger_failed": [
            {
                "event_class": "Demand-trigger failed",
                "severity": "warning",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:852-870",
            },
        ],
        "_notify_demand_skip": [
            {
                "event_class": "Demand-trigger skip, no-op",
                "severity": "info",
                "silent": True,
                "citation": "severity_taxonomy.md row: index.py:880-889",
            },
        ],
    },
    "infrastructure/lambdas/freshness-monitor/index.py": {
        "_maybe_alert": [
            {
                "event_class": "Artifact SLA miss",
                "severity": "per-artifact spec.severity from ARTIFACT_REGISTRY.yaml (warning or critical; dynamically coerced for champion-arm)",
                "silent": False,
                "citation": "severity_taxonomy.md row: index.py:358-392,967-978",
            },
        ],
    },
    # --- RECONCILED from live code (2026-07-31) — pending taxonomy entries ---
    "infrastructure/lambdas/saturday-sf-watch-dispatcher/index.py": {
        "_notify": [
            {
                "event_class": "SF watch dispatch mid-execution note",
                "severity": "info",
                "silent": True,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
        "_escalate_budget_exhausted": [
            {
                "event_class": "SF watch dispatch budget exhausted",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/sf-watch-reclaim-sweep-handler/index.py": {
        "_reclaim_escalate": [
            {
                "event_class": "SF watch reclaim escalation",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
        "_reclaim_note": [
            {
                "event_class": "SF watch reclaim note (relaunch info)",
                "severity": "info",
                "silent": True,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/overseer-liveness-probe/index.py": {
        "_alert": [
            {
                "event_class": "Overseer wiring-health alert",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/saturday-integrity-sentinel/index.py": {
        "_notify": [
            {
                "event_class": "Saturday integrity check result (go/no-go) — severity/silent dynamic per outcome",
                "severity": "dynamic (caller-chosen: info/warning/error)",
                "silent": "dynamic (caller-chosen, depends on go/no-go result)",
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/pipeline-watchdog/index.py": {
        "_check_sf": [
            {
                "event_class": "Pipeline watchdog — stuck SF alert",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/expense-collector/index.py": {
        "_alert_over_budget": [
            {
                "event_class": "Expense-collector over-budget alert",
                "severity": "warning",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/ci-watch-liveness-probe/index.py": {
        "_escalate": [
            {
                "event_class": "CI-watch reclaim escalation",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
        "_note": [
            {
                "event_class": "CI-watch reclaim relaunch note",
                "severity": "info",
                "silent": True,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "infrastructure/lambdas/alert-drain-liveness-probe/index.py": {
        "_escalate": [
            {
                "event_class": "Alert-drain reclaim escalation",
                "severity": "error",
                "silent": False,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
        "_note": [
            {
                "event_class": "Alert-drain reclaim relaunch note",
                "severity": "info",
                "silent": True,
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
    "scripts/run_arctic_migrations.py": {
        "notify": [
            {
                "event_class": "ArcticDB migration outcome — severity/silent dynamic per outcome",
                "severity": "dynamic (caller-chosen: info/warning/error)",
                "silent": "dynamic (silent when severity==info, else loud)",
                "citation": "reconciled from live code 2026-07-31 — pending taxonomy entry",
            },
        ],
    },
}


# ---------------------------------------------------------------------------
# Scan helpers
# ---------------------------------------------------------------------------


def _is_excluded(rel_posix: str) -> bool:
    if any(rel_posix.startswith(p) for p in _EXCLUDE_PREFIXES):
        return True
    # Lambdas ship test_handler.py in-tree (not under tests/) — exclude any
    # file whose leaf name starts with test_ (mirrors pytest default collection).
    leaf = rel_posix.rsplit("/", 1)[-1]
    return leaf.startswith("test_")


def _enclosing_function_name(lines: list[str], call_line_idx: int) -> str:
    """Scan backward from the call line to find the enclosing ``def``."""
    for i in range(call_line_idx, -1, -1):
        m = re.match(r"^\s*(?:async\s+)?def\s+(\w+)\s*\(", lines[i])
        if m:
            return m.group(1)
    return "<module>"


def _call_sites(root: Path) -> dict[str, set[str]]:
    """Find every ``notify_via_flow_doctor(`` call site in production code.

    Returns ``{repo_relative_path: set_of_enclosing_function_names}``.
    Excludes the flow-doctor sink definition itself.
    """
    out: dict[str, set[str]] = {}
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if _is_excluded(rel):
            continue
        if rel == _SINK_FILE:
            continue
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        fns: set[str] = set()
        for i, line in enumerate(lines):
            if _CALL_SITE_RE.search(line) and not line.lstrip().startswith("#"):
                fns.add(_enclosing_function_name(lines, i))
        if fns:
            out[rel] = fns
    return out


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_all_notify_via_flow_doctor_callsites_are_registered():
    """Every ``notify_via_flow_doctor`` call site in production code must be in
    ``_REGISTRY``, keyed by file path and enclosing function name.

    A NEW file or function that calls ``notify_via_flow_doctor`` without a
    registry entry is the exact regression this chokepoint guards against
    (alpha-engine-config#3267): a producer bypassed the fleet severity taxonomy.
    Add an entry to ``_REGISTRY`` above — citing either a
    ``severity_taxonomy.md`` row or a live-code reconciliation datestamp.
    """
    found = _call_sites(_REPO)
    problems: list[str] = []

    for file_path, call_fns in sorted(found.items()):
        if file_path not in _REGISTRY:
            problems.append(
                f"{file_path}: calls notify_via_flow_doctor but has NO registry "
                f"entry at all — add a {file_path!r} key to _REGISTRY"
            )
            continue
        registered_fns = set(_REGISTRY[file_path].keys())
        missing = call_fns - registered_fns
        for fn in sorted(missing):
            problems.append(
                f"{file_path}:{fn}(): calls notify_via_flow_doctor but is not "
                f"in _REGISTRY[{file_path!r}] — add an entry under function {fn!r}"
            )

    assert not problems, (
        "New notify_via_flow_doctor call site(s) outside the fleet severity "
        "taxonomy:\n  " + "\n  ".join(problems)
        + "\n\nAdd each to _REGISTRY above with its event_class, severity, "
        "silent, and citation (severity_taxonomy.md row or live-code "
        "reconciliation datestamp)."
    )


def test_registry_entries_are_honest():
    """Every registry entry's file must still exist and still actually call
    ``notify_via_flow_doctor`` from the named function — a stale entry (file
    deleted, or function migrated off the call) must be removed, not left to
    silently mask a future regression at that same path."""
    found = _call_sites(_REPO)
    problems: dict[str, str] = {}

    for file_path, fn_map in sorted(_REGISTRY.items()):
        path = _REPO / file_path
        if not path.exists():
            problems[file_path] = "file no longer exists — remove from _REGISTRY"
            continue
        for fn_name in fn_map:
            if file_path not in found or fn_name not in found[file_path]:
                problems[f"{file_path}:{fn_name}()"] = (
                    "function no longer calls notify_via_flow_doctor — migration "
                    "complete, remove from _REGISTRY"
                )

    assert not problems, f"stale _REGISTRY entries: {problems}"


def test_guard_catches_an_injected_violation():
    """Honesty test: prove the scan actually fires on a synthetic new call site,
    not just that the current tree happens to be clean."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # A production directory that would be scanned.
        prod_dir = root / "infrastructure" / "lambdas" / "totally-new-lambda"
        prod_dir.mkdir(parents=True)
        victim = prod_dir / "index.py"
        victim.write_text(
            "from flow_doctor_telegram import notify_via_flow_doctor\n\n"
            "def handler(event, context):\n"
            "    notify_via_flow_doctor('oh no', silent=False, severity='error')\n",
            encoding="utf-8",
        )
        found = _call_sites(root)
        assert "infrastructure/lambdas/totally-new-lambda/index.py" in found
        assert found["infrastructure/lambdas/totally-new-lambda/index.py"] == {"handler"}

        # Also prove that the registry has no entry for this synthetic file,
        # so the real test would fail on it.
        assert "infrastructure/lambdas/totally-new-lambda/index.py" not in _REGISTRY


def test_guard_ignores_test_directory_matches():
    """A test file that references notify_via_flow_doctor (e.g. stubbing,
    mocking, or this guard's own docstrings) must not trip the production scan."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        test_file = root / "tests" / "test_something.py"
        test_file.parent.mkdir(parents=True)
        test_file.write_text(
            "# Verify that notify_via_flow_doctor(text, silent=True, severity='info')\n"
            "from flow_doctor_telegram import notify_via_flow_doctor\n"
            "notify_via_flow_doctor('test', silent=True, severity='info')\n",
            encoding="utf-8",
        )
        found = _call_sites(root)
        assert found == {}


def test_guard_excludes_flow_doctor_sink_itself():
    """The flow_doctor_telegram.py definition must not be flagged — it is the
    sink definition, not a call site."""
    found = _call_sites(_REPO)
    assert _SINK_FILE not in found, (
        f"{_SINK_FILE} is the notify_via_flow_doctor DEFINITION — it must be "
        "excluded from the call-site scan"
    )
