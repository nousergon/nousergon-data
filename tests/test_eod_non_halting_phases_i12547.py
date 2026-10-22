"""alpha-engine-config-I12547: a declared non-halting EOD phase that fails makes
the run ``degraded`` (exit 0), never ``failed`` -- and never ``ok``.

2026-10-21: a FRED outage made `metron_macro_data` (D23, Metron's Macro page)
``skipped``, the aggregate ``failed``, the process exit 1, and the EOD Step
Function never ran the ArcticDB append. Every other phase keeps the hard-fail
posture, which is what the last two tests pin.
"""

from __future__ import annotations

import weekly_collector as wc


def _ok(**extra):
    return {"status": "ok", **extra}


def test_non_halting_list_is_exactly_the_declared_set():
    # Add-by-PR-only: widening this changes what may halt the EOD SF.
    assert wc._EOD_NON_HALTING_PHASES == frozenset({"metron_macro_data"})


def test_all_ok_is_ok():
    assert wc._aggregate_daily_status({"daily_closes": _ok(), "features": _ok()}) == ("ok", [])


def test_metron_macro_skipped_on_fred_outage_is_degraded_not_failed():
    collectors = {
        "daily_closes": _ok(),
        "metron_macro_data": {"status": "skipped",
                              "reason": "no macro series (FRED key unset or fetch failed)"},
        "features": _ok(),
    }
    assert wc._aggregate_daily_status(collectors) == ("degraded", ["metron_macro_data"])


def test_metron_macro_error_is_degraded_too():
    status, names = wc._aggregate_daily_status({"metron_macro_data": {"status": "error"}})
    assert (status, names) == ("degraded", ["metron_macro_data"])


def test_feature_degraded_still_degraded():
    status, names = wc._aggregate_daily_status(
        {"features": {"status": "degraded"}, "metron_macro_data": {"status": "skipped"}})
    assert (status, names) == ("degraded", ["features", "metron_macro_data"])


def test_any_other_phase_not_ok_still_fails():
    collectors = {"daily_closes": {"status": "skipped"}, "metron_macro_data": _ok()}
    assert wc._aggregate_daily_status(collectors) == ("failed", [])


def test_other_failure_wins_over_non_halting_failure():
    collectors = {"prices": {"status": "error"}, "metron_macro_data": {"status": "skipped"}}
    assert wc._aggregate_daily_status(collectors) == ("failed", [])


def test_degraded_page_names_the_tracked_issue_and_the_reason():
    text = wc._describe_degraded_defects({
        "degraded_collectors": ["metron_macro_data"],
        "collectors": {"metron_macro_data": {"status": "skipped", "reason": "FRED down"}},
    })
    assert "alpha-engine-config-I12547" in text
    assert "detail=FRED down" in text
