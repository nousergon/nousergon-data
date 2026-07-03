"""AST-walk regression pin: no raw ``now()``/``today()`` fed into
``daily_append(date_str=...)``.

Closes the recurring "test-date brittleness" defect class (config#1630).
Date-driven ``daily_append`` tests each hand-rolled their own "today"
date, and the two production guards — the config#1572 NYSE-trading-day
gate **and** the universe-freshness scan — leave only a narrow valid
window (a *recent trading day*). Ad-hoc dates kept falling outside it:

  - 2026-05-04 — a hardcoded ``2026-04-28`` rotted 6d past the freshness
    threshold.
  - 2026-06-22 — Juneteenth (Fri 2026-06-19 holiday) ordering.
  - 2026-07-03 — raw ``datetime.now()`` landed on the observed
    Independence Day holiday (NYSE closed); 7 tests red on ``main``.

PR #599 fixed the third incident by anchoring
``test_daily_append_skip_if_exists.py`` /
``test_daily_append_universe_chunking.py`` to
``_recent_trading_day_str()`` (most recent NYSE *trading* day). Config#1630
promoted that helper to ``tests/conftest.py`` as the shared
``recent_trading_day_str()`` util + ``recent_trading_day`` fixture — the
one discoverable chokepoint for "give me a valid daily_append date".

This test is the fail-loud guard that keeps the next author from
re-introducing the class: it walks every test module and rejects any
``daily_append(date_str=...)`` (or positional first-arg) call site whose
value is built from a bare ``datetime.now(...)`` / ``dt.now(...)`` /
``date.today()`` / ``datetime.today()`` expression, instead of the
shared fixture/util.

Explicitly whitelisted (do NOT flag): the intentionally hardcoded
scenario tests, which pin specific historical/holiday dates on purpose
and would be broken by migrating to a live anchor:

  - ``test_daily_append_writer_lock.py`` (``2026-05-27``, Memorial-Day race)
  - ``test_daily_append_macro_monotonic.py`` (``2026-06-18``, Juneteenth ordering)
  - ``test_daily_append_missing_from_closes.py`` (``2026-04-28``)
  - ``test_daily_append_trading_day_gate.py`` (holiday dates that MUST raise)

Also whitelisted: ``tests/conftest.py`` itself, which legitimately calls
``datetime.now(timezone.utc)`` inside ``recent_trading_day_str()`` — the
one place the raw "now" call is supposed to live.

Only ``daily_append(date_str=...)`` call sites are in scope. Other uses
of ``datetime.now()`` / ``date.today()`` elsewhere in a test file (e.g.
stubbing an unrelated mock's index) are unaffected — see
``test_daily_append_missing_from_closes.py``'s ``today_row`` stub, which
uses ``datetime.now(timezone.utc).date()`` for a ``.tail()`` mock return
value, not as a ``daily_append`` argument, and is correctly NOT flagged.
"""
from __future__ import annotations

import ast
import pathlib

TESTS_DIR = pathlib.Path(__file__).resolve().parent

# Files that intentionally hardcode specific historical/holiday dates for
# fixed scenarios (config#1630 "explicitly out of scope"). A raw
# datetime.now()/date.today() call feeding daily_append(date_str=...)
# in one of these would still be a real bug, but the file's *purpose* is
# to pin exact dates, so we allowlist by filename rather than trying to
# distinguish "the deliberate hardcoded date" from "an accidental raw
# now()" within them.
FILES_ALLOWED_RAW_NOW = frozenset({
    "test_daily_append_writer_lock.py",
    "test_daily_append_macro_monotonic.py",
    "test_daily_append_missing_from_closes.py",
    "test_daily_append_trading_day_gate.py",
})

# The one legitimate call site for a raw now()/today() call: the shared
# anchor helper itself.
FILES_ALLOWED_RAW_NOW |= {"conftest.py"}

_NOW_TODAY_ATTRS = {
    ("datetime", "now"),
    ("dt", "now"),
    ("date", "today"),
    ("datetime", "today"),
}


def _root_name(node: ast.AST) -> str | None:
    """Innermost ``Name`` id of a (possibly chained) attribute/call
    expression, e.g. ``datetime`` in ``datetime.now(timezone.utc).date()``."""
    while isinstance(node, (ast.Attribute, ast.Call)):
        node = node.func if isinstance(node, ast.Call) else node.value
    return node.id if isinstance(node, ast.Name) else None


def _contains_raw_now_call(expr: ast.AST) -> bool:
    """True if ``expr`` (a date_str argument expression) is built from a
    bare ``datetime.now(...)``/``date.today()``-style call anywhere in
    its subtree — e.g. ``datetime.now(timezone.utc).date().isoformat()``.
    """
    for node in ast.walk(expr):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        root = _root_name(func.value)
        if root is not None and (root, func.attr) in _NOW_TODAY_ATTRS:
            return True
    return False


def _daily_append_date_str_violations(path: pathlib.Path) -> list[tuple[int, str]]:
    """(lineno, source_line) for every ``daily_append(date_str=<raw now>)``
    (or positional first-arg) call site in ``path``."""
    try:
        source = path.read_text()
    except (OSError, UnicodeDecodeError):
        return []
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return []

    source_lines = source.splitlines()
    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else (
            func.attr if isinstance(func, ast.Attribute) else None
        )
        if name != "daily_append":
            continue

        date_expr: ast.AST | None = None
        for kw in node.keywords:
            if kw.arg == "date_str":
                date_expr = kw.value
                break
        if date_expr is None and node.args:
            # date_str is daily_append's first positional parameter.
            date_expr = node.args[0]
        if date_expr is None:
            continue

        if _contains_raw_now_call(date_expr):
            lineno = node.lineno
            src = source_lines[lineno - 1] if lineno - 1 < len(source_lines) else ""
            violations.append((lineno, src.strip()))

    return violations


def _test_files() -> list[pathlib.Path]:
    return sorted(TESTS_DIR.glob("*.py"))


def test_no_raw_now_date_str():
    """``daily_append(date_str=...)`` must never be fed a bare
    ``datetime.now(...)``/``date.today()`` expression directly. Use the
    shared ``tests/conftest.py`` ``recent_trading_day_str()`` util (or
    the ``recent_trading_day`` fixture) instead — it resolves to the
    most recent NYSE *trading* day, which is valid on every weekday,
    weekend, and market holiday. A raw "now" is not guaranteed to be a
    trading day and will intermittently fail the config#1572
    phantom-session gate (config#1630).

    The intentionally hardcoded scenario-test files are exempt — see
    ``FILES_ALLOWED_RAW_NOW`` and this module's docstring.
    """
    all_violations: list[str] = []
    for path in _test_files():
        if path.name in FILES_ALLOWED_RAW_NOW:
            continue
        for lineno, src in _daily_append_date_str_violations(path):
            all_violations.append(f"  {path.name}:{lineno}: {src}")

    assert not all_violations, (
        "Raw datetime.now()/date.today() fed into daily_append(date_str=...) "
        "found in test file(s) not on the intentional-hardcoded-date "
        "allowlist. Use tests/conftest.py's recent_trading_day_str() "
        "(or the recent_trading_day fixture) instead — see config#1630.\n"
        + "\n".join(all_violations)
    )


def test_allowlisted_files_exist_or_are_conftest():
    """Keeps FILES_ALLOWED_RAW_NOW honest: every entry must be either the
    shared conftest (which legitimately defines the raw now() call) or
    an actual test file in tests/. Guards against typos silently
    widening (or narrowing) the allowlist."""
    for name in FILES_ALLOWED_RAW_NOW:
        assert (TESTS_DIR / name).exists(), (
            f"FILES_ALLOWED_RAW_NOW entry {name!r} does not exist in tests/"
        )
