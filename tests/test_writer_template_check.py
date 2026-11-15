"""Direct unit tests for `data_gate.writer_template_check` on synthetic units.

`test_writer_template_reachability.py` grades the real fleet; these tests feed
`check_unit` / `run_all` tiny fake repos under `tmp_path`, so each grading
branch is pinned independently of the committed descriptors.
"""

from __future__ import annotations

import pathlib
import textwrap
from typing import Any

import pytest

from data_gate import writer_template_check as wtc
from data_gate.descriptors import Unit
from data_gate.inventory import WriterInventory
from data_gate.writer_template_check import Finding, check_unit, run_all


def _unit(
    unit_id: str = "DX1",
    *,
    code_path: str = "collectors/mod.py",
    writes: list[str] | None = None,
    lifecycle: str = "in-service",
) -> Unit:
    raw: dict[str, Any] = {
        "component_id": f"example-{unit_id}",
        "title": "example",
        "lifecycle": lifecycle,
        "code_path": code_path,
        "writes": writes if writes is not None else [],
    }
    return Unit(unit_id=unit_id, path=pathlib.Path("example.yaml"), raw=raw)


def _repo(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, files: dict[str, str]) -> None:
    for rel, body in files.items():
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(body), encoding="utf-8")
    monkeypatch.setattr(wtc, "REPO_ROOT", tmp_path)


_TWO_CALLS = """
    PREFIX = "data/example"

    def write(s3) -> None:
        s3.put_object(Bucket="b", Key=PREFIX + "/a.json", Body=b"")
        s3.put_object(Bucket="b", Key="data/example/latest.json", Body=b"")
"""


def test_retired_unit_is_ok_and_not_graded() -> None:
    finding = check_unit(_unit(lifecycle="retired"), sole_owner=True)
    assert finding == Finding("DX1", "ok", "retired — not graded")


def test_missing_code_file_is_unverifiable(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {})
    finding = check_unit(_unit(writes=["data/example/{date}.json"]), sole_owner=True)
    assert finding.status == "unverifiable"
    assert finding.detail == "code_path names no in-repo Python file"


def test_non_s3_shaped_writes_are_unverifiable(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    finding = check_unit(_unit(writes=["arctic::lib/sym", "no_slash_here"]), sole_owner=True)
    assert finding.status == "unverifiable"
    assert finding.detail == "no S3-key-shaped writes[] entry to check"


def test_reachable_prefix_is_ok(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    unit = _unit(writes=["data/example/{date}.json", "data/example/latest.json"])
    assert check_unit(unit, sole_owner=True) == Finding("DX1", "ok", "")


def test_unreachable_prefix_is_flagged(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mutation guarded: dropping the unmatched-prefix branch would grade this ok.
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    unit = _unit(writes=["predictor/retired_tree/{date}.parquet"])
    finding = check_unit(unit, sole_owner=True)
    assert finding.status == "flagged"
    assert "predictor/retired_tree/{date}.parquet" in finding.detail
    assert "collectors/mod.py" in finding.detail


def test_legacy_named_constant_does_not_make_prefix_reachable(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Mutation guarded: removing the LEGACY exclusion would flip this to ok.
    _repo(
        tmp_path,
        monkeypatch,
        {"collectors/mod.py": 'OLD_LEGACY_PREFIX = "predictor/old_tree"\n'},
    )
    finding = check_unit(_unit(writes=["predictor/old_tree/{t}.parquet"]), sole_owner=False)
    assert finding.status == "flagged"


def test_f_string_fragment_makes_prefix_reachable(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    body = """
        def key(date: str) -> str:
            return f"data/example/{date}.json"
    """
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": body})
    finding = check_unit(_unit(writes=["data/example/{date}.json"]), sole_owner=False)
    assert finding.status == "ok"


def test_placeholder_from_position_zero_is_skipped(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": "X = 1\n"})
    finding = check_unit(_unit(writes=["{bucket}/key.json"]), sole_owner=False)
    assert finding.status == "ok"


def test_one_hop_import_return_literal_is_reachable(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = """
        NEW_PREFIX = "reference/example"

        def prefixes(primary: str) -> list[str]:
            return [NEW_PREFIX]
    """
    main = """
        from helperpkg.helper import prefixes

        def write() -> None:
            prefixes("x")
    """
    _repo(tmp_path, monkeypatch, {"helperpkg/helper.py": helper, "collectors/mod.py": main})
    finding = check_unit(_unit(writes=["reference/example/{t}.parquet"]), sole_owner=False)
    assert finding.status == "ok"


def test_unparseable_hop_module_is_skipped(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    main = """
        from helperpkg.broken import thing
        KEY = "data/example"
    """
    _repo(tmp_path, monkeypatch, {"helperpkg/broken.py": "def f(:\n", "collectors/mod.py": main})
    finding = check_unit(_unit(writes=["data/example/{d}.json"]), sole_owner=False)
    assert finding.status == "ok"


def test_undercounted_writes_flagged_for_sole_owner(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Mutation guarded: weakening `>` to `>=`/dropping Signal B would pass this.
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    finding = check_unit(_unit(writes=["data/example/{date}.json"]), sole_owner=True)
    assert finding.status == "flagged"
    assert finding.detail == "2 S3 write call site(s) in collectors/mod.py, only 1 writes[] entry declared"


def test_undercount_ignored_when_file_is_shared(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    finding = check_unit(_unit(writes=["data/example/{date}.json"]), sole_owner=False)
    assert finding.status == "ok"


def test_call_count_equal_to_declared_is_ok(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    unit = _unit(writes=["data/example/{date}.json", "data/example/latest.json"])
    assert check_unit(unit, sole_owner=True).status == "ok"


def test_run_all_marks_shared_file_not_sole_owner(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    scope = WriterInventory(
        roots=("collectors",),
        write_calls=frozenset({"put_object"}),
        shared_writers={},
        excluded=(),
        extra_paths={},
        no_python_write_site={},
    )
    monkeypatch.setattr(wtc, "load_inventory_scope", lambda: scope)
    one = _unit("DA1", writes=["data/example/{date}.json"])
    # Sole owner: undercount is flagged.
    assert [f.status for f in run_all([one])] == ["flagged"]
    # A second unit declaring the same file removes sole ownership for both.
    two = _unit("DA2", writes=["data/example/{date}.json"])
    assert [(f.unit_id, f.status) for f in run_all([one, two])] == [("DA1", "ok"), ("DA2", "ok")]


def test_run_all_shared_writers_entry_removes_sole_ownership(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _repo(tmp_path, monkeypatch, {"collectors/mod.py": _TWO_CALLS})
    scope = WriterInventory(
        roots=("collectors",),
        write_calls=frozenset({"put_object"}),
        shared_writers={"collectors/mod.py": ("DA1",)},
        excluded=(),
        extra_paths={},
        no_python_write_site={},
    )
    monkeypatch.setattr(wtc, "load_inventory_scope", lambda: scope)
    one = _unit("DA1", writes=["data/example/{date}.json"])
    assert [f.status for f in run_all([one])] == ["ok"]


def test_run_all_empty_units_gives_no_findings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wtc, "load_inventory_scope", lambda: WriterInventory((), frozenset(), {}, (), {}, {}))
    assert run_all([]) == []
