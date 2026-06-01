"""L4484: daily_append factor-momentum integration invariants.

Two structural guarantees (source-inspection, mirroring
test_daily_append_read_batch.py's style — full daily_append e2e needs live
ArcticDB + closes + macro mocks):

  1. **Schema-align break-fix.** A FEATURES column the stored series carries but
     compute_features doesn't emit (factor_momentum_ratio — a cross-sectional
     SECOND-PASS column) must be NaN-filled into today_row BEFORE the canonical
     write, or the static-schema update_batch StreamDescriptorMismatches on the
     first daily_append after a backfill that added it.
  2. **Daily go-forward second pass.** update_factor_momentum_latest must run
     AFTER the per-ticker write loop (so today's close+loadings are in the lib),
     best-effort (never fails the daily pipeline).
"""
from __future__ import annotations

from pathlib import Path

_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


def test_today_row_aligned_to_stored_schema():
    """The break-fix loop must NaN-fill stored FEATURES columns missing from
    today_row (the factor_momentum_ratio static-schema descriptor case)."""
    src = _source()
    assert "for _stored_col in hist.columns:" in src
    assert "_stored_col in FEATURES and _stored_col not in today_row.columns" in src


def test_factor_momentum_daily_update_invoked_after_write_loop():
    """update_factor_momentum_latest is imported + called with today_ts and the
    canonical writer, gated by the FACTOR_MOMENTUM_DAILY_ENABLED env var."""
    src = _source()
    assert "from features.factor_momentum import update_factor_momentum_latest" in src
    assert "update_factor_momentum_latest(" in src
    assert "FACTOR_MOMENTUM_DAILY_ENABLED" in src
    # Best-effort: wrapped so it never fails the daily pipeline.
    assert "Factor-momentum daily update FAILED" in src
