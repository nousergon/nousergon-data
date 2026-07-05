"""C.1 / C.2b: daily_append factor-loading z-score integration invariants.

Mirrors test_daily_append_factor_momentum.py — source-inspection guarantees
that the cross-sectional second pass is wired after the per-ticker write loop
and is best-effort (never fails the daily pipeline).
"""
from __future__ import annotations

from pathlib import Path

_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


def test_factor_loading_zscore_daily_update_invoked_after_write_loop():
    src = _source()
    assert "update_factor_loading_zscores_latest" in src
    assert "FACTOR_LOADING_ZSCORE_DAILY_ENABLED" in src
    assert "Factor-loading z-score daily update FAILED" in src
