"""Contract for the known-data-quality-window register.

The register (`data_quality/windows.py`) is the permanent home for a
measured, ruled-on defect in data that is already published — the artifact
class that a GitHub issue serves badly, because an issue is closed and then
stops being read while the affected bytes stay on S3 forever
(Brian, 2026-09-22; the unsettled-bar window was moved here out of
alpha-engine-config-I11367).

These tests exist so the register cannot rot into prose: every field a
consumer relies on is asserted present and well-formed, and the rendered
Markdown view cannot drift from the code the way `features/SCHEMA.md` §3
once drifted from `features/registry.py::CATALOG`.
"""

from __future__ import annotations

from datetime import date

import pytest

from data_quality import gen_windows_md
from data_quality.windows import (
    DISPOSITIONS,
    WINDOWS,
    overlapping,
    window_by_id,
)


def test_window_ids_are_unique():
    ids = [w.window_id for w in WINDOWS]
    assert len(ids) == len(set(ids)), f"duplicate window_id in register: {ids}"


def test_every_window_is_fully_declared():
    """No field a consumer reads may be blank.

    A window with an empty `evidence` or `reexam_trigger` is the failure this
    register exists to prevent: it reads as a governed decision while
    carrying none of what makes it one.
    """
    for window in WINDOWS:
        assert window.window_id, "window_id is required"
        assert window.artifacts, f"{window.window_id}: artifacts is required"
        assert window.defect.strip(), f"{window.window_id}: defect is required"
        assert window.measured, f"{window.window_id}: measured is required"
        assert window.evidence.strip(), f"{window.window_id}: evidence is required"
        assert window.ruled_by.strip(), f"{window.window_id}: ruled_by is required"
        assert window.reexam_trigger.strip(), (
            f"{window.window_id}: reexam_trigger is required — a window with "
            "no condition that re-opens it is a note, not a disposition"
        )


def test_dispositions_are_from_the_declared_set():
    for window in WINDOWS:
        assert window.disposition in DISPOSITIONS, (
            f"{window.window_id}: disposition {window.disposition!r} is not "
            f"one of {DISPOSITIONS}"
        )


def test_date_ranges_are_ordered_and_not_future_sentinels():
    """`last_affected` is None for an open window, never a far-future date.

    A sentinel like 2099-12-31 reads as a real closing date to every consumer
    that does not know the convention, which is all of them.
    """
    for window in WINDOWS:
        if window.last_affected is None:
            continue
        assert window.last_affected >= window.first_affected, (
            f"{window.window_id}: last_affected precedes first_affected"
        )
        assert window.last_affected.year <= date.today().year + 1, (
            f"{window.window_id}: last_affected {window.last_affected} looks "
            "like a far-future sentinel — use None for an open window"
        )


def test_reexam_trigger_is_a_condition_not_a_date():
    """The register's own rule: a trigger is a condition, never a calendar entry.

    A date-based re-exam gets read on schedule to produce the same answer
    until someone stops reading it; a condition fires exactly once, when it
    matters.
    """
    for window in WINDOWS:
        trigger = window.reexam_trigger
        assert not trigger.strip().startswith("20"), (
            f"{window.window_id}: reexam_trigger starts with a year — state "
            "the condition that re-opens the ruling, not a date"
        )


def test_covers_is_inclusive_at_both_ends():
    window = window_by_id("unsettled-session-bar-2026")
    assert window.covers(window.first_affected)
    assert window.covers(window.last_affected)
    assert not window.covers(date(2026, 4, 30))
    assert not window.covers(date(2026, 9, 23))


def test_covers_is_open_ended_when_last_affected_is_none():
    window = window_by_id("unsettled-session-bar-2026")
    open_window = type(window)(
        **{**window.__dict__, "window_id": "t", "last_affected": None}
    )
    assert open_window.covers(date(2099, 1, 1))


def test_window_by_id_raises_on_an_unknown_id():
    """Silently returning 'no known defect' for a typo is the whole hazard."""
    with pytest.raises(KeyError):
        window_by_id("no-such-window")


def test_overlapping_matches_only_declared_artifacts():
    hits = overlapping(
        "features/{date}/technical.parquet", date(2026, 6, 1), date(2026, 6, 2)
    )
    assert [w.window_id for w in hits] == ["unsettled-session-bar-2026"]

    assert overlapping(
        "features/{date}/nonexistent.parquet", date(2026, 6, 1), date(2026, 6, 2)
    ) == ()


def test_overlapping_excludes_ranges_outside_the_window():
    artifact = "staging/daily_closes/{date}.parquet"
    assert overlapping(artifact, date(2026, 1, 1), date(2026, 4, 30)) == ()
    assert overlapping(artifact, date(2026, 9, 23), date(2026, 12, 31)) == ()
    # A range that merely clips the edge still counts.
    assert len(overlapping(artifact, date(2026, 4, 25), date(2026, 5, 1))) == 1


def test_overlapping_refuses_an_inverted_range():
    with pytest.raises(ValueError):
        overlapping(
            "features/{date}/technical.parquet", date(2026, 6, 2), date(2026, 6, 1)
        )


def test_rendered_markdown_matches_the_register():
    """The committed view may not drift from the code it describes.

    Run `python3 data_quality/gen_windows_md.py --write` and commit the
    result.
    """
    committed = gen_windows_md.WINDOWS_MD.read_text(encoding="utf-8")
    assert committed == gen_windows_md.render(), (
        "DATA_QUALITY_WINDOWS.md is stale. Run "
        "`python3 data_quality/gen_windows_md.py --write` and commit."
    )


def test_render_is_deterministic():
    assert gen_windows_md.render() == gen_windows_md.render()


def test_render_names_every_window():
    rendered = gen_windows_md.render()
    for window in WINDOWS:
        assert f"`{window.window_id}`" in rendered
        assert window.evidence in rendered
