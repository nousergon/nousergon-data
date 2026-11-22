"""Unit tests for the writer-inventory parser and AST reconciliation."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from data_gate import inventory
from data_gate.descriptors import Unit
from data_gate.inventory import InventoryReading, WriterInventory, declared_code_files, load_inventory_scope, scan


def _unit(unit_id: str, code_path: str | None) -> Unit:
    raw: dict[str, Any] = {}
    if code_path is not None:
        raw["code_path"] = code_path
    return Unit(unit_id, pathlib.Path("example.yaml"), raw)


def _scope(
    *,
    roots: tuple[str, ...] = ("producers",),
    shared: dict[str, tuple[str, ...]] | None = None,
    excluded: tuple[str, ...] = (),
    extra: dict[str, tuple[str, ...]] | None = None,
    unverifiable: dict[str, str] | None = None,
) -> WriterInventory:
    return WriterInventory(
        roots,
        frozenset({"put_object"}),
        shared or {},
        excluded,
        extra or {},
        unverifiable or {},
    )


def _write(root: pathlib.Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_writer_inventory_and_reading_defaults() -> None:
    scope = _scope(roots=())
    reading = InventoryReading()

    assert scope.write_calls == frozenset({"put_object"})
    assert reading.write_sites == {}
    assert reading.ok is True


def test_inventory_reading_is_not_ok_for_each_failure() -> None:
    # Mutation guard: removing any failure term from InventoryReading.ok makes one case true.
    assert InventoryReading(undeclared=["unclaimed.py"]).ok is False
    assert InventoryReading(units_without_write_site=["DX"]).ok is False
    assert InventoryReading(parse_failures={"broken.py": "SyntaxError"}).ok is False


def test_load_inventory_scope_reads_all_sections(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "inventory.yaml"
    path.write_text(
        """\
roots: [collectors]
write_calls:
  s3: [put_object, upload_file]
  arctic: [write]
shared_writers:
  - path: collectors/shared.py
    parent_units: [D1, D2]
excluded_paths:
  - glob: collectors/generated_*.py
extra_paths:
  - path: weekly.py
    parent_units: [D3]
no_python_write_site:
  - unit: D4
    reason: external writer
""",
        encoding="utf-8",
    )

    assert load_inventory_scope(path) == WriterInventory(
        roots=("collectors",),
        write_calls=frozenset({"put_object", "upload_file", "write"}),
        shared_writers={"collectors/shared.py": ("D1", "D2")},
        excluded=("collectors/generated_*.py",),
        extra_paths={"weekly.py": ("D3",)},
        no_python_write_site={"D4": "external writer"},
    )


def test_load_inventory_scope_rejects_missing_roots(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "inventory.yaml"
    path.write_text("write_calls: {}\n", encoding="utf-8")

    with pytest.raises(KeyError, match="roots"):
        load_inventory_scope(path)


def test_declared_code_files_parses_prose() -> None:
    unit = _unit(
        "DX",
        "collectors/one.py:12 helper; jobs/two.py:7, tasks/three.py:9 note, README.md",
    )

    assert declared_code_files(unit) == [
        "collectors/one.py",
        "jobs/two.py",
        "tasks/three.py",
    ]
    assert declared_code_files(_unit("EMPTY", None)) == []


@pytest.mark.xfail(
    strict=True,
    reason="inventory.py:135 drops prefixed paths that also carry a documented line number",
)
def test_declared_code_files_parses_repository_prefixes() -> None:
    unit = _unit("DX", "crucible:jobs/two.py:7, metron:tasks/three.py:9")

    assert declared_code_files(unit) == ["jobs/two.py", "tasks/three.py"]


def test_scan_reconciles_claimed_shared_excluded_and_extra_writers(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, "producers/claimed.py", "client.put_object()\n")
    _write(tmp_path, "producers/shared.py", "client.put_object()\n")
    _write(tmp_path, "producers/excluded.py", "client.put_object()\n")
    _write(tmp_path, "outside.py", "client.put_object()\n")
    monkeypatch.setattr(inventory, "REPO_ROOT", tmp_path)
    scope = _scope(
        shared={"producers/shared.py": ("SHARED",)},
        excluded=("producers/excluded.py",),
        extra={"outside.py": ("EXTRA",)},
    )

    reading = scan(
        [
            _unit("CLAIMED", "producers/claimed.py:1"),
            _unit("SHARED", "missing/shared.py"),
            _unit("EXTRA", "missing/extra.py"),
        ],
        scope,
    )

    assert reading.write_sites == {
        "producers/claimed.py": ["put_object:1"],
        "producers/shared.py": ["put_object:1"],
        "outside.py": ["put_object:1"],
    }
    assert reading.undeclared == []
    assert reading.units_without_write_site == []
    assert reading.units_via_shared_writer == ["SHARED", "EXTRA"]
    assert reading.ok is True


def test_scan_names_unclaimed_missing_and_unverifiable_units(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, "producers/z_unclaimed.py", "client.put_object()\n")
    _write(tmp_path, "producers/a_unclaimed.py", "client.put_object()\n")
    monkeypatch.setattr(inventory, "REPO_ROOT", tmp_path)

    reading = scan(
        [_unit("MISSING", "producers/no_calls.py"), _unit("NO_PATH", None), _unit("EXTERNAL", None)],
        _scope(unverifiable={"EXTERNAL": "outside this repository"}),
    )

    assert reading.undeclared == ["producers/a_unclaimed.py", "producers/z_unclaimed.py"]
    assert reading.units_without_write_site == ["MISSING"]
    assert reading.units_unverifiable == {
        "NO_PATH": (
            "its code_path names no in-repo Python file, so an AST scan cannot check "
            "the descriptor -> write-site direction for it"
        ),
        "EXTERNAL": "outside this repository",
    }
    assert reading.ok is False


def test_scan_reports_syntax_errors_and_missing_extra_paths(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write(tmp_path, "producers/broken.py", "def broken(:\n")
    _write(tmp_path, "broken_extra.py", "def also_broken(:\n")
    monkeypatch.setattr(inventory, "REPO_ROOT", tmp_path)

    reading = scan(
        [],
        _scope(extra={"broken_extra.py": (), "absent.py": ()}),
    )

    assert reading.parse_failures["producers/broken.py"].startswith("SyntaxError:")
    assert reading.parse_failures["broken_extra.py"].startswith("SyntaxError:")
    assert reading.parse_failures["absent.py"] == "declared in extra_paths and not present in the tree"
    assert reading.ok is False


def test_scan_skips_a_missing_root(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(inventory, "REPO_ROOT", tmp_path)

    reading = scan([], _scope())

    assert reading == InventoryReading()
