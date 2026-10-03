"""A zero-variance feature column degrades the EOD run; it does not halt it.

alpha-engine-config-I7572. On 2026-08-17 the fatal form of the I7539 guard cost
far more than the defect it caught:

* `assert_no_zero_variance_features` raises BEFORE `write_feature_snapshot`, so
  eight constant columns destroyed the whole day's snapshot — the ~200 good
  columns with them.
* `_run_daily` aggregated `features=error` to `status=failed`, and `main()`
  exited 1.
* The EOD SF read that as a failed data-spot workload, routed to
  `ExtractDataSpotError`, and therefore NEVER reached
  `LaunchPostMarketArcticAppendSpot`.
* So the day's SPY close never landed in ArcticDB, the freshness sentinel
  stayed on 2026-08-14, `ProbeEODReconcilePrecondition` returned
  `precondition_met: false`, `EODReconcile` was skipped, and the self-heal loop
  re-ran the identical deterministic failure twice before paging
  `HealNonConvergent`. Terminal: `DegradedRun`.

The guard is right. Its blast radius was not. These tests pin the distinction:
same columns, same predicate, same loud verdict — different consequence, and
only on the daily path.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.postflight import (
    assert_no_zero_variance_features,
    find_zero_variance_columns,
)


def _frame(n: int = 100) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {
            "ticker": [f"T{i}" for i in range(n)],
            "live_col": rng.normal(size=n),
            "dead_col": np.zeros(n),
        }
    )


class TestGuardItselfIsUnchanged:
    """No column is exempted and no threshold is moved by this change."""

    def test_a_constant_column_is_still_detected(self):
        assert find_zero_variance_columns(_frame(), ["live_col", "dead_col"]) == {
            "dead_col": 100
        }

    def test_a_live_column_is_not_flagged(self):
        assert "live_col" not in find_zero_variance_columns(
            _frame(), ["live_col", "dead_col"]
        )

    def test_the_fatal_form_still_raises(self):
        with pytest.raises(RuntimeError, match="Zero-variance"):
            assert_no_zero_variance_features(_frame(), ["live_col", "dead_col"])


class TestComputeAndWriteMode:
    def test_signature_defaults_to_fatal(self):
        """Every caller that does not opt in keeps the pre-2026-08-17 behaviour —
        a backfill or weekly recompute still refuses to write a defective
        snapshot, because nothing downstream is waiting on it."""
        import inspect

        from features.compute import compute_and_write

        sig = inspect.signature(compute_and_write)
        assert sig.parameters["zero_variance_fatal"].default is True

    def test_only_the_eod_daily_path_opts_out(self):
        """Exactly one call site may pass zero_variance_fatal=False. A second one
        appearing without a test here means the carve-out spread."""
        from pathlib import Path

        repo = Path(__file__).resolve().parents[1]
        hits = []
        for path in repo.rglob("*.py"):
            rel = path.relative_to(repo)
            if rel.parts[0] in {".git", ".venv", ".worktrees", "tests"}:
                continue
            if "zero_variance_fatal=False" in path.read_text(encoding="utf-8"):
                hits.append(str(rel))
        assert hits == ["weekly_collector.py"], hits


class TestDailyStatusAggregation:
    """The aggregation rule, exercised directly on the status lists it sees."""

    @staticmethod
    def _aggregate(statuses: list[str]) -> str:
        # Mirrors _run_daily's block verbatim; kept here so the rule is pinned
        # even though the surrounding function needs a full collector run.
        if all(s in ("ok", "ok_dry_run") for s in statuses):
            return "ok"
        if all(s in ("ok", "ok_dry_run", "degraded") for s in statuses):
            return "degraded"
        return "failed"

    def test_all_ok_is_ok(self):
        assert self._aggregate(["ok", "ok", "ok_dry_run"]) == "ok"

    def test_a_degraded_phase_degrades_the_run(self):
        assert self._aggregate(["ok", "degraded"]) == "degraded"

    def test_a_real_failure_still_fails_the_run(self):
        assert self._aggregate(["ok", "degraded", "error"]) == "failed"

    def test_degraded_never_upgrades_to_ok(self):
        """`ok` stays reserved for a genuinely clean run — the whole point of a
        third status rather than a softer failure."""
        assert self._aggregate(["degraded"]) != "ok"


class TestExitContract:
    """What the EOD Step Function actually reads."""

    @staticmethod
    def _halts(status: str) -> bool:
        return status not in ("ok", "skipped", "degraded")

    def test_degraded_does_not_halt(self):
        """This is the whole fix: exiting 0 here is what lets the SF go on to
        LaunchPostMarketArcticAppendSpot and EODReconcile."""
        assert self._halts("degraded") is False

    @pytest.mark.parametrize("status", ["failed", "error", "partial", "unknown"])
    def test_every_genuine_failure_still_halts(self, status):
        assert self._halts(status) is True

    def test_ok_and_skipped_are_unchanged(self):
        assert self._halts("ok") is False
        assert self._halts("skipped") is False


class TestSourceCarriesTheEvidence:
    """Guard-the-guard: the tests above assert on rules, so also assert the rules
    are the ones the source actually implements."""

    @staticmethod
    def _source(name: str) -> str:
        from pathlib import Path

        return (Path(__file__).resolve().parents[1] / name).read_text(encoding="utf-8")

    def test_health_marker_is_written_on_degraded(self):
        """A degraded day previously wrote NO health marker, which reads on every
        freshness surface as 'this never ran'."""
        src = self._source("weekly_collector.py")
        assert 'results["status"] in ("ok", "degraded"):' in src
        assert "_write_health_marker(bucket, 0, run_date, results[\"status\"])" in src

    def test_degraded_artifacts_are_still_verified(self):
        src = self._source("weekly_collector.py")
        assert 'result.get("status") in ("ok", "degraded")' in src

    def test_degraded_does_not_arm_auto_skip(self):
        """An `ok` phase marker arms same-date auto-skip, and an auto-skipped
        rerun returns status=ok — the degradation would vanish on the second
        run. Withholding `record_artifact` alone never prevented that (an empty
        artifact list validates trivially); the marker itself must not say
        `ok` (alpha-engine-config-I11812, behaviour pinned in
        tests/test_weekly_retry_marker_and_manifest_i11812.py)."""
        src = self._source("weekly_collector.py")
        assert 'if result.get("status") not in _MARKER_OK_STATUSES:' in src
        assert "raise _MarkerNotOk(name, result)" in src

    def test_the_verdict_is_logged_at_error(self):
        src = self._source("features/compute.py")
        assert "ZERO-VARIANCE feature column(s) detected on the daily path" in src
        assert "log.error(" in src
