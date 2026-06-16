"""Pins weekly_collector.main()'s --morning-enrich → preflight-mode mapping.

Origin: the preflight-task-split (2026-05-16, plan
alpha-engine-docs/private/preflight-task-split-260516.md). Before the
split, `--morning-enrich` mapped to DataPreflight mode "daily" — which
only probes ArcticDB freshness and does NOT validate polygon/FRED
reachability, even though _run_morning_enrich hits polygon. A drifted
polygon key therefore failed ~28min into the spot run instead of in
<1s at the entry. The fix maps `--morning-enrich` → mode
"morning_enrich" (a dedicated UNION entry preflight).

main() reads argv via _parse_args() with no DI, so this is an AST/source
assertion (the convention used by other static-wiring tests in this
repo) rather than a behavioral test — the behavioral coverage for the
morning_enrich mode itself lives in tests/test_preflight.py
::TestMorningEnrichMode.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_COLLECTOR = _REPO_ROOT / "weekly_collector.py"


def _main_source() -> str:
    """Return the source of weekly_collector.main()."""
    tree = ast.parse(_COLLECTOR.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return ast.get_source_segment(_COLLECTOR.read_text(), node)
    raise AssertionError("weekly_collector.main() not found")


def test_morning_enrich_maps_to_morning_enrich_mode():
    src = _main_source()
    assert 'getattr(args, "morning_enrich", False)' in src, (
        "main() must branch on the --morning-enrich arg before choosing "
        "the preflight mode."
    )
    # The morning-enrich branch must assign mode = "morning_enrich",
    # NOT the old mode = "daily".
    assert 'mode = "morning_enrich"' in src, (
        "main() must map --morning-enrich to DataPreflight mode "
        '"morning_enrich" (the dedicated UNION entry preflight). The old '
        '"daily" mapping skipped polygon/FRED reachability — a drifted '
        "key failed 28min into the spot run. See preflight-task-split "
        "2026-05-16."
    )


def test_morning_enrich_not_mapped_to_daily():
    """Regression guard: the --morning-enrich branch must NOT fall back
    to the "daily" mode (the pre-split behavior)."""
    src = _main_source()
    # Isolate the morning_enrich branch body up to the `elif args.daily`.
    marker = 'getattr(args, "morning_enrich", False)'
    start = src.index(marker)
    rest = src[start:]
    elif_idx = rest.index("elif args.daily")
    branch = rest[:elif_idx]
    assert 'mode = "daily"' not in branch, (
        "The --morning-enrich branch must not map to mode 'daily' — that "
        "is exactly the pre-split bug the task split fixed."
    )


def test_daily_mode_mapping_unchanged():
    """The genuine --daily weekday path still maps to 'daily'. The EOD split
    (2026-06-16) also routes --daily-arctic-append through the same 'daily'
    preflight (it reads daily_closes + the ArcticDB universe — same surface)."""
    src = _main_source()
    assert "elif args.daily" in src
    after = src[src.index("elif args.daily"):]
    branch = after.split("else:")[0]
    assert 'mode = "daily"' in branch
    # The split-out append state shares the daily preflight surface.
    assert 'daily_arctic_append' in branch
