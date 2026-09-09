"""alpha-engine-config-I10359: the daily-collect DEGRADED alert must name the
sub-component, the offending columns, the tracked issue and the clears-when
condition — not a bare "features reported a known defect" every day.

Measured live 2026-09-09: `weekly_collector.py`'s "Collection finished
DEGRADED" log line was the sole content of flow-doctor's auto-filed GitHub
issue (nousergon-data#1667, title "[DIAGNOSIS UNAVAILABLE]"), and it named
nothing actionable — not which column, not alpha-engine-config-I7572 (the
issue that has tracked this exact `factor_momentum_ratio` all-null defect
since 2026-08-17), not what would clear it. The same message had already
paged, identically, on at least 16 consecutive trading-day EOD runs
(2026-08-18 through 2026-09-09, CloudWatch `/alpha-engine/data-spot`) with
zero incremental information each time.

`weekly_collector._describe_degraded_defects` is the fix: it turns a
`degraded` collector's own reported `zero_variance_columns` /
`all_null_columns` plus a small `_DEGRADED_DEFECT_REGISTRY` lookup into the
`Defect detail: ...` clause folded into that same log line, so the string
flow-doctor turns into an issue body carries the detail directly.
"""
from __future__ import annotations

from weekly_collector import _DEGRADED_DEFECT_REGISTRY, _describe_degraded_defects


def _results(all_null=None, zero_variance=None):
    return {
        "degraded_collectors": ["features"],
        "collectors": {
            "features": {
                "status": "degraded",
                "all_null_columns": all_null or [],
                "zero_variance_columns": zero_variance or {},
            },
            "prices": {"status": "ok"},
        },
    }


def test_known_defect_names_the_column_issue_and_clears_when():
    detail = _describe_degraded_defects(
        _results(all_null=["factor_momentum_ratio"])
    )
    assert "factor_momentum_ratio" in detail
    assert _DEGRADED_DEFECT_REGISTRY["features"]["tracked_issue"] in detail
    assert "clears_when" in detail
    # The clears-when text itself, not just the label, must be present —
    # a reader following this alert needs the condition, not a pointer to
    # a key name.
    assert _DEGRADED_DEFECT_REGISTRY["features"]["clears_when"] in detail


def test_zero_variance_columns_are_named_too():
    detail = _describe_degraded_defects(
        _results(zero_variance={"earnings_surprise_pct": 902, "fcf_yield": 902})
    )
    assert "earnings_surprise_pct" in detail
    assert "fcf_yield" in detail


def test_an_unregistered_degraded_collector_reads_as_explicitly_untracked():
    """A future collector reporting `degraded` with no registry entry must
    not silently inherit the tracked shape — it must say UNTRACKED, loud."""
    results = {
        "degraded_collectors": ["some_new_collector"],
        "collectors": {"some_new_collector": {"status": "degraded"}},
    }
    detail = _describe_degraded_defects(results)
    assert "UNTRACKED" in detail
    assert "some_new_collector" in detail


def test_features_registry_entry_points_at_i7572():
    """Pins the current live tracked issue so a rename/retarget is a visible
    diff here, not a silent drift between the code and the tracker."""
    assert _DEGRADED_DEFECT_REGISTRY["features"]["tracked_issue"] == "alpha-engine-config-I7572"
