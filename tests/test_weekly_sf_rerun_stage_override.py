"""--rerun-stage: force a completed stage back into a recovery.

alpha-engine-config-I11106. The deriver reuses work it witnessed as
completed, which is correct when the only evidence that matters is the
execution history. It is wrong when a stage's OUTPUT stopped being
trustworthy for a reason the history cannot see — the 2026-09-19 recovery
had to re-enter Director so `director-plan` would emit the cost record
AggregateCosts grades (alpha-engine-config-I11100), and the deriver had
correctly witnessed Director complete.

Without this flag the only route is hand-editing the derived JSON, which
bypasses every coherence guard the deriver exists to apply.
"""
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "weekly_sf_rerun",
    Path(__file__).resolve().parents[1] / "scripts" / "weekly_sf_rerun.py",
)
wsr = importlib.util.module_from_spec(_SPEC)
sys.modules["weekly_sf_rerun"] = wsr
_SPEC.loader.exec_module(wsr)


def _history(entered=(), *, run_date="2026-09-18", extra_input=None):
    """Minimal synthetic history: ExecutionStarted plus one entered state each.

    Mirrors tests/test_weekly_sf_rerun.py::_chain_history — kept local rather
    than imported so this file does not couple to that module's fixtures.
    """
    inp = {"pipeline_role": "weekly", "run_date": run_date}
    if extra_input:
        inp.update(extra_input)
    events = [{
        "type": "ExecutionStarted",
        "executionStartedEventDetails": {"input": json.dumps(inp)},
    }]
    events += [
        {"type": "ChoiceStateEntered", "stateEnteredEventDetails": {"name": n}}
        for n in entered
    ]
    return events


# The 2026-09-19 shape: Director ran and was witnessed complete, so the
# deriver would skip it — and its cost record is exactly what the recovery
# needs re-emitted (alpha-engine-config-I11100).
_DIRECTOR_COMPLETED = [
    "InitializeInput", "CheckSkipReportCard", "ReportCard",
    "CheckSkipDirector", "Director", "DirectorComplete",
]


def test_an_unknown_stage_name_is_refused_loudly():
    """A typo must not silently force nothing — it must stop the recovery."""
    with pytest.raises(SystemExit) as exc:
        wsr.derive_plan(_history(), force_rerun=frozenset({"diroctor"}))
    msg = str(exc.value)
    assert "diroctor" in msg
    assert "not a declared stage" in msg
    # The refusal names the vocabulary so the operator can fix it in one go.
    assert "director" in msg


def test_forcing_a_stage_the_input_cannot_enter_fails_loudly():
    """The reachability guard is the point, not an obstacle.

    Three stages are not reachable from a bare input even with NO skip flags
    set — `research_self_test`, `scanner_leaderboard` and
    `backtester_stage_only` — because something other than their own skip flag
    gates them. Dropping a skip flag is therefore NOT sufficient to guarantee
    a stage runs, and the failure mode this flag must never have is a
    recovery that reports success having silently not run what the operator
    asked for. Forcing one of them raises instead.
    """
    with pytest.raises(SystemExit) as exc:
        wsr.derive_plan(_history(), force_rerun=frozenset({"research_self_test"}))
    assert "unreachable" in str(exc.value)
    assert "research_self_test" in str(exc.value)


def test_a_forceable_stage_is_accepted_and_reachable():
    """The common case: a stage gated only by its own skip flag."""
    for name in ("director", "report_card", "aggregate_costs"):
        plan = wsr.derive_plan(_history(), force_rerun=frozenset({name}))
        assert not plan.skip_flags.get(wsr.STAGES_BY_NAME[name].flag), (
            f"{name}: forcing it left its own skip flag set"
        )


def test_forcing_director_also_drops_the_report_card_skip():
    """I8382's freshness coupling must survive a forced Director.

    Director's input contract is a report_card.json from THIS execution. If
    forcing Director left skip_report_card set, Director would grade a card
    from an earlier attempt — the exact 2026-08-22 defect, reintroduced
    through the new flag.
    """
    plan = wsr.derive_plan(_history(_DIRECTOR_COMPLETED), force_rerun=frozenset({"director"}))
    assert not plan.skip_flags.get(wsr.STAGES_BY_NAME["director"].flag)
    assert not plan.skip_flags.get(wsr.STAGES_BY_NAME["report_card"].flag)


def test_a_forced_stage_is_recorded_in_the_notes():
    """Transparency: a recovery that ran more than the deriver chose must say
    so on its own record, or the next reader cannot reconstruct why."""
    plan = wsr.derive_plan(_history(_DIRECTOR_COMPLETED), force_rerun=frozenset({"director"}))
    assert any("FORCED to re-run by --rerun-stage" in n for n in plan.notes)
    assert any("director" in n for n in plan.notes)


def test_no_force_leaves_the_derivation_untouched():
    """The default path is byte-identical to before the flag existed."""
    baseline = wsr.derive_plan(_history(_DIRECTOR_COMPLETED))
    with_empty = wsr.derive_plan(_history(_DIRECTOR_COMPLETED), force_rerun=frozenset())
    assert baseline.skip_flags == with_empty.skip_flags
    assert baseline.completed == with_empty.completed
    assert baseline.notes == with_empty.notes
