"""Tests for check_suppression_registry.py (alpha-engine-config-I9727)."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import check_suppression_registry as chk  # noqa: E402

TODAY = dt.date(2026, 10, 31)


def _row(**kw) -> chk.Row:
    base = {"collection": "m.py::SUPP", "keys": ("'a'",), "exhaustive": True,
            "issue": "alpha-engine-config-I1", "expires": dt.date(2026, 12, 31)}
    base.update(kw)
    return chk.Row(**base)


def _module(tmp_path: Path, body: str) -> Path:
    (tmp_path / "m.py").write_text(body, encoding="utf-8")
    return tmp_path


def test_the_live_registry_is_well_formed_and_current() -> None:
    """The merge-blocking offline half, over the committed registry."""
    rows, problems = chk.load_rows()
    assert rows, "the registry declares nothing"
    assert problems == []
    assert chk.offline_problems(rows, dt.date.today()) == []


def test_a_declared_key_present_and_unexpired_passes(tmp_path) -> None:
    root = _module(tmp_path, "SUPP = {'a': 'why'}\n")
    assert chk.offline_problems([_row()], TODAY, root) == []


def test_tuple_keys_and_frozenset_literals_are_read(tmp_path) -> None:
    root = _module(tmp_path, "SUPP: frozenset = frozenset({('x', 'y')})\n")
    row = _row(keys=("('x', 'y')",))
    assert chk.offline_problems([row], TODAY, root) == []


def test_an_expired_row_fails(tmp_path) -> None:
    root = _module(tmp_path, "SUPP = {'a': 'why'}\n")
    problems = chk.offline_problems([_row(expires=TODAY)], TODAY, root)
    assert len(problems) == 1 and "expired" in problems[0]


def test_a_drained_key_fails_until_the_row_is_updated(tmp_path) -> None:
    root = _module(tmp_path, "SUPP = {}\n")
    problems = chk.offline_problems([_row()], TODAY, root)
    assert any("is gone" in p for p in problems)


def test_a_missing_collection_fails(tmp_path) -> None:
    root = _module(tmp_path, "OTHER = {}\n")
    problems = chk.offline_problems([_row()], TODAY, root)
    assert any("not found" in p for p in problems)


def test_growth_in_an_exhaustive_collection_fails(tmp_path) -> None:
    root = _module(tmp_path, "SUPP = {'a': 1, 'b': 2}\n")
    problems = chk.offline_problems([_row()], TODAY, root)
    assert len(problems) == 1 and "'b'" in problems[0]


def test_a_non_exhaustive_collection_may_hold_permanent_rows(tmp_path) -> None:
    root = _module(tmp_path, "SUPP = {'a': 1, 'b': 2}\n")
    assert chk.offline_problems([_row(exhaustive=False)], TODAY, root) == []


def test_a_closed_owning_issue_fails_live() -> None:
    problems = chk.live_problems([_row()], state_of=lambda n: "closed")
    assert len(problems) == 1 and "is closed" in problems[0]


def test_an_open_owning_issue_passes_live() -> None:
    assert chk.live_problems([_row()], state_of=lambda n: "open") == []


def test_an_unreadable_issue_state_is_a_problem_not_a_pass() -> None:
    def boom(n):
        raise OSError("network")
    problems = chk.live_problems([_row()], state_of=boom)
    assert len(problems) == 1 and "UNREAD" in problems[0]


def test_live_without_a_token_is_a_problem(monkeypatch) -> None:
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    assert chk.live_problems([_row()]) != []


def test_malformed_rows_are_reported_not_skipped(tmp_path) -> None:
    reg = tmp_path / "r.yaml"
    reg.write_text(
        "suppressions:\n"
        "  - collection: no-separator\n"
        "  - collection: m.py::X\n    keys: ['a']\n    exhaustive: true\n"
        "    issue: I12\n    expires: 2026-12-31\n"
        "  - collection: m.py::Y\n    keys: ['a']\n    exhaustive: true\n"
        "    issue: alpha-engine-config-I12\n    expires: soon\n",
        encoding="utf-8")
    rows, problems = chk.load_rows(reg)
    assert rows == [] and len(problems) == 3


def test_a_json_baseline_is_read_by_its_top_level_key(tmp_path) -> None:
    (tmp_path / "b.json").write_text('{"files": {"x.py": {"bucket": 1}}}', encoding="utf-8")
    row = _row(collection="b.json::files", keys=("'x.py'",))
    assert chk.offline_problems([row], TODAY, tmp_path) == []


def test_a_yaml_collection_reports_growth(tmp_path) -> None:
    (tmp_path / "b.yaml").write_text("entries: [a, b]\n", encoding="utf-8")
    row = _row(collection="b.yaml::entries", keys=("'a'",))
    problems = chk.offline_problems([row], TODAY, tmp_path)
    assert len(problems) == 1 and "'b'" in problems[0]


def test_keys_all_covers_a_ledger_without_enumerating_it(tmp_path) -> None:
    (tmp_path / "b.json").write_text('{"files": {"x.py": 1, "y.py": 2}}', encoding="utf-8")
    row = _row(collection="b.json::files", keys=(), whole=True)
    assert chk.offline_problems([row], TODAY, tmp_path) == []


def test_keys_all_fails_once_the_ledger_is_empty(tmp_path) -> None:
    (tmp_path / "b.json").write_text('{"files": {}}', encoding="utf-8")
    row = _row(collection="b.json::files", keys=(), whole=True)
    problems = chk.offline_problems([row], TODAY, tmp_path)
    assert len(problems) == 1 and "is empty" in problems[0]


def test_keys_all_is_parsed_from_the_registry(tmp_path) -> None:
    reg = tmp_path / "r.yaml"
    reg.write_text(
        "suppressions:\n"
        "  - collection: b.json::files\n    keys: all\n    exhaustive: true\n"
        "    issue: alpha-engine-config-I12\n    expires: 2026-12-31\n",
        encoding="utf-8")
    rows, problems = chk.load_rows(reg)
    assert problems == [] and rows[0].whole and rows[0].keys == ()
