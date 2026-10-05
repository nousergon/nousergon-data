"""A unit's guard declaration reads the code (`alpha-engine-config-I10785`, plan item P-18).

Started with `empty_fresh` / `success_without_output` (C22); `cardinality` and `units`
followed on 2026-10-04 (S2). The last two have no shared mechanism: a cell either files
the shared `data_cardinality` name (`records_as`, D20 only) or quotes the unit-local
check it relies on (`evaluates`), which must appear verbatim in the named function.

Both guards already run on shared paths: `weekly_collector.py::_record_phase_lineage`
and `_record_mode_lineage` grade every published key for the empty-but-fresh class,
and `run_units.py::record_empty_production` files every completed run that recorded
no output as `failed`. Until 2026-10-04 the descriptors still declared `absent` for
~30 units those paths cover, so the board said "no code" about code that ran every
cycle. A declaration that can drift from the source is a claim; these tests make it
a reading:

* every `code:` reference resolves to a function that exists and files the
  declared manifest name (`records_as`), or contains the quoted check (`evaluates`);
* the unit actually reaches that function (its phase, mode or entry point);
* and the converse — a live unit that reaches the shared code may not declare the
  class `absent`, so a new unit or a stale descriptor fails here, not on the board.

Commissioning (an induced-fault record, P-19 / `alpha-engine-config-I10786`) is a
separate fact the clause reads from the store; nothing here claims it.
"""

from __future__ import annotations

import ast
import functools
import pathlib
import re

import pytest

import run_units
from data_gate import descriptors
from validators import expectations

REPO = descriptors.REPO_ROOT
SHARED_CLASSES = ("empty_fresh", "success_without_output")
DECLARABLE_CLASSES = descriptors.GUARD_CLASSES

#: How each recorded name appears in source: the module constant, or the literal.
_RECORDED_TOKENS = {
    "data_empty_fresh": ("EMPTY_FRESH_GUARD.name", '"data_empty_fresh"'),
    "data_success_without_output": ("EMPTY_PRODUCTION_GUARD", '"data_success_without_output"'),
    "data_cardinality": ("CARDINALITY_GUARD.name", '"data_cardinality"'),
}

_ENTRY_CALL = re.compile(r"(?:recorded_entry|manual_run)\(\s*\"(D\d+[A-Z]?)\"")
_CARDINALITY_CALL = re.compile(r"check_cardinality\(\s*unit_id=\"(D\d+[A-Z]?)\"")
_GRADE_CALL = re.compile(r"grade_published_outputs\(\s*\w+,\s*\"(D\d+[A-Z]?)\"")


def _units():
    return [u for u in descriptors.load_units() if not u.retired]


def _phase_units() -> set[str]:
    return {u.unit_id for u in run_units.PHASE_UNITS.values()}


def _mode_units() -> set[str]:
    return set(run_units.MODE_UNITS.values())


@functools.lru_cache(maxsize=1)
def _entry_units() -> frozenset[str]:
    """Units whose entry point calls `run_units.recorded_entry` / `manual_run` with a
    literal unit id — the two wrappers that call `record_empty_production`."""
    found: set[str] = set()
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO).as_posix()
        if rel.startswith((".venv/", "tests/")) or "/test_" in rel or rel.startswith("test_"):
            continue
        if path.name == "run_units.py":
            continue
        found.update(_ENTRY_CALL.findall(path.read_text(encoding="utf-8")))
    return frozenset(found)


@functools.lru_cache(maxsize=1)
def _cardinality_graded_units() -> frozenset[str]:
    """Units the shared cardinality check is called for, by literal unit id."""
    found: set[str] = set()
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO).as_posix()
        if rel.startswith((".venv/", "tests/")) or "/test_" in rel or rel.startswith("test_"):
            continue
        found.update(_CARDINALITY_CALL.findall(path.read_text(encoding="utf-8")))
    return frozenset(found)


@functools.lru_cache(maxsize=1)
def _published_graded_units() -> frozenset[str]:
    """Entry-point units whose published keys `run_units.grade_published_outputs`
    grades, by literal unit id (D39, D16, D46)."""
    found: set[str] = set()
    for path in REPO.rglob("*.py"):
        rel = path.relative_to(REPO).as_posix()
        if rel.startswith((".venv/", "tests/")) or "/test_" in rel or rel.startswith("test_"):
            continue
        if path.name == "run_units.py":
            continue
        found.update(_GRADE_CALL.findall(path.read_text(encoding="utf-8")))
    return frozenset(found)


def _squash(text: str) -> str:
    return " ".join(text.split())


@functools.lru_cache(maxsize=None)
def _function_source(ref: str) -> str | None:
    file_part, func = ref.split("::")
    path = REPO / file_part
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func:
            return ast.get_source_segment(text, node)
    return None


def _declared():
    for unit in _units():
        for cls in DECLARABLE_CLASSES:
            block = unit.guards[cls]
            if block.get("code"):
                yield unit, cls, block


def _params():
    return [pytest.param(u, c, b, id=f"{u.unit_id}-{c}") for u, c, b in _declared()]


def test_recorded_names_are_the_producing_modules_own():
    """The class spelling and the manifest name differ; the map is pinned to the code."""
    assert descriptors.GUARD_RECORDED_NAMES["empty_fresh"] == expectations.EMPTY_FRESH_GUARD.name
    assert descriptors.GUARD_RECORDED_NAMES["cardinality"] == expectations.CARDINALITY_GUARD.name
    assert (
        descriptors.GUARD_RECORDED_NAMES["success_without_output"]
        == run_units.EMPTY_PRODUCTION_GUARD
    )


def test_there_are_declarations_to_read():
    assert sum(1 for _ in _declared()) >= 50


def test_the_next_classes_have_declarations_to_read():
    for cls in ("cardinality", "units"):
        assert any(c == cls for _, c, _ in _declared()), f"no `{cls}` cell names its code"


@pytest.mark.parametrize("unit,cls,block", _params())
def test_every_code_reference_resolves_and_files_the_declared_name(unit, cls, block):
    if cls in SHARED_CLASSES:
        assert block.get("records_as") == descriptors.GUARD_RECORDED_NAMES[cls], (
            f"{unit.unit_id}: guards.{cls} names code but not the manifest name it files"
        )
    sources = {}
    for ref in block["code"]:
        sources[ref] = _function_source(ref)
        assert sources[ref] is not None, f"{unit.unit_id}: guards.{cls}.code {ref!r} does not resolve"
    if block.get("records_as"):
        tokens = _RECORDED_TOKENS[block["records_as"]]
        for ref, source in sources.items():
            assert any(t in source for t in tokens), (
                f"{unit.unit_id}: {ref} does not file {block['records_as']!r} — the declaration "
                "names code that does not evaluate this class"
            )
    else:
        quoted = _squash(block["evaluates"])
        assert any(quoted in _squash(source) for source in sources.values()), (
            f"{unit.unit_id}: guards.{cls}.evaluates {block['evaluates']!r} appears in none of "
            f"{list(sources)} — the check this cell relies on is not in the code it names"
        )


@pytest.mark.parametrize("unit,cls,block", _params())
def test_the_unit_reaches_the_code_it_declares(unit, cls, block):
    uid = unit.unit_id
    for ref in block["code"]:
        func = ref.split("::")[1]
        if func == "_record_phase_lineage":
            assert uid in _phase_units(), f"{uid} has no _phase_collect phase in run_units.PHASE_UNITS"
        elif func == "_record_mode_lineage":
            assert uid in _mode_units(), f"{uid} is not a whole-mode unit in run_units.MODE_UNITS"
        elif func == "record_empty_production":
            assert uid in _phase_units() | _mode_units() | _entry_units(), (
                f"{uid} reaches neither _phase_collect, _run_whole_mode_unit, nor a "
                "recorded_entry/manual_run call with its unit id"
            )
        elif func == "grade_published_outputs":
            assert uid in _published_graded_units(), (
                f"{uid}: no grade_published_outputs(<ctx>, {uid!r}, ...) call reaches {ref}"
            )
        elif func == "_grade_cardinality":
            assert uid in _cardinality_graded_units(), (
                f"{uid}: no check_cardinality(unit_id={uid!r}) call reaches {ref}"
            )
        else:
            # A unit-local check: the file is the unit's declared producer
            # (`code_path`), or the file names the unit.
            file_part = ref.split("::")[0]
            code_path = str(unit.raw.get("code_path") or "")
            text = (REPO / file_part).read_text(encoding="utf-8")
            assert (
                file_part[: -len(".py")] in code_path
                or f'"{uid}"' in text
                or f"{uid} " in text
            ), f"{ref} is not in {uid}'s code_path ({code_path!r}) and never names {uid}"


def test_a_unit_on_the_shared_path_does_not_declare_the_class_absent():
    """The converse: a live unit the shared code grades may not say `absent`."""
    stale = []
    for unit in _units():
        uid = unit.unit_id
        if uid in _phase_units() | _mode_units() | _published_graded_units():
            if unit.guards["empty_fresh"]["state"] == "absent":
                stale.append(f"{uid}.empty_fresh")
        if uid in _phase_units() | _mode_units() | _entry_units():
            if unit.guards["success_without_output"]["state"] == "absent":
                stale.append(f"{uid}.success_without_output")
        if uid in _cardinality_graded_units():
            if unit.guards["cardinality"]["state"] == "absent":
                stale.append(f"{uid}.cardinality")
    assert not stale, (
        f"declared `absent` on a path whose shared code evaluates the class: {stale}. "
        "Declare `present` with `code:` and `records_as:` (see D01's descriptor)."
    )


def test_a_malformed_code_reference_is_refused(tmp_path):
    import yaml

    units_dir = tmp_path / "units"
    units_dir.mkdir()
    src = descriptors.UNITS_DIR / "D01-constituents.yaml"
    doc = yaml.safe_load(src.read_text(encoding="utf-8"))
    doc["guards"]["empty_fresh"]["code"] = ["weekly_collector._record_phase_lineage"]
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="is not `<path>.py::<function>`"):
        descriptors.load_units(units_dir)

    doc["guards"]["empty_fresh"]["code"] = ["weekly_collector.py::_record_phase_lineage"]
    doc["guards"]["empty_fresh"]["records_as"] = "empty_fresh"
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="filed on a run manifest as 'data_empty_fresh'"):
        descriptors.load_units(units_dir)

    doc["guards"]["empty_fresh"]["records_as"] = "data_empty_fresh"
    doc["guards"]["empty_fresh"]["state"] = "absent"
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="is not an absent guard"):
        descriptors.load_units(units_dir)

    doc["guards"]["empty_fresh"]["state"] = "present"
    units_block = doc["guards"]["units"]
    units_block.pop("evaluates")
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="neither `records_as`"):
        descriptors.load_units(units_dir)

    units_block["records_as"] = "data_units"
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="filed on a run manifest as None"):
        descriptors.load_units(units_dir)

    units_block.pop("records_as")
    units_block.pop("code")
    units_block["evaluates"] = "lo <= raw_sum <= hi"
    (units_dir / src.name).write_text(yaml.safe_dump(doc), encoding="utf-8")
    with pytest.raises(descriptors.DescriptorError, match="names no `code`"):
        descriptors.load_units(units_dir)
