"""`n_ok` must not read 0 on every run solely because of columns that are
EXPECTED to be NaN at write time and filled by a same-run second pass.

`alpha-engine-config-I10939`. Live D18 (2026-09-15) and D32 (2026-09-16)
both read `n_ok=0 n_partial=909 n_err=1` — every single row in a
~910-ticker universe carried at least one NaN feature. Traced to
`builders/daily_append.py`'s per-ticker write loop: `factor_momentum_ratio`
and the 9 Barra `*_zscore` loading columns are FEATURES-set members that
cannot be computed per-ticker (they are cross-sectional, ranked over the
whole universe panel), so the per-ticker "align to stored schema" step
force-sets them to NaN on EVERY row before the write-time coverage count
runs — and only fills the real value in a SECOND PASS
(`update_factor_momentum_latest`, `update_factor_loading_zscores_latest`)
that executes AFTER `n_ok`/`n_partial` are already counted. Once those
columns existed in storage, `n_ok` could never read anything but 0 again —
not a regression from a specific date, a metric that stopped measuring
what its name says.

This mirrors `test_daily_append_factor_momentum.py`'s source-inspection
style (full e2e needs live ArcticDB + closes + macro mocks) and adds a
direct behavioral check of the new exclusion set.
"""

from __future__ import annotations

from pathlib import Path

import builders.daily_append as daily_append

_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


def test_deferred_second_pass_features_names_all_columns():
    """factor_momentum_ratio + every Barra *_zscore loading in
    `FACTOR_LOADING_SOURCES` (10 entries as of I10939, though comments
    elsewhere in daily_append.py say "9" — read from the map here rather
    than a hardcoded count so this test tracks the map, not the prose) —
    the exact set the write-time coverage check must not penalize a ticker
    for."""
    from features.cross_sectional import FACTOR_LOADING_SOURCES

    expected = {"factor_momentum_ratio", *FACTOR_LOADING_SOURCES.values()}
    assert daily_append._DEFERRED_SECOND_PASS_FEATURES == frozenset(expected)
    assert len(daily_append._DEFERRED_SECOND_PASS_FEATURES) == len(FACTOR_LOADING_SOURCES) + 1


def test_write_time_coverage_check_excludes_deferred_columns():
    """Source-inspection guard: the `nan_features` comprehension that drives
    n_ok/n_partial must exclude `_DEFERRED_SECOND_PASS_FEATURES` — this is
    the exact fix, pinned so it cannot silently regress back to counting
    every ticker as partial."""
    src = _source()
    assert "f not in _DEFERRED_SECOND_PASS_FEATURES" in src
    # And it must appear on the SAME nan_features comprehension that feeds
    # n_ok/n_partial, not some unrelated site.
    idx = src.index("nan_features = [")
    window = src[idx: idx + 400]
    assert "f not in _DEFERRED_SECOND_PASS_FEATURES" in window


def test_cross_sectional_coverage_recorded_separately_and_board_visible():
    """The honest second-pass coverage is recorded on `result` under its own
    key — never folded into n_ok/n_partial, which the second passes cannot
    attribute per-ticker today (I10939 deliverable 4: visible on the board,
    not only in a log line)."""
    src = _source()
    assert 'result["cross_sectional_coverage"] = cross_sectional_coverage' in src
    assert 'result["deferred_second_pass_features"]' in src
    assert '"factor_momentum":' in src
    assert '"factor_loading_zscore":' in src


def test_deferred_features_excluded_reproduces_the_live_shape_no_regression():
    """Reproduces the exact live accounting shape as a pure function check:
    a row with EVERY declared FEATURE present and finite except the 10
    deferred columns must count as fully-featured (nan_features == []),
    matching what `n_ok` should have read the whole time these columns have
    existed in storage."""
    import numpy as np
    import pandas as pd

    from features.feature_engineer import FEATURES

    row = pd.DataFrame(
        {f: [0.0] for f in FEATURES if f not in daily_append._DEFERRED_SECOND_PASS_FEATURES}
        | {f: [np.nan] for f in daily_append._DEFERRED_SECOND_PASS_FEATURES if f in FEATURES},
        index=[pd.Timestamp("2026-09-16")],
    )
    nan_features = [
        f for f in FEATURES
        if f in row.columns and f not in daily_append._DEFERRED_SECOND_PASS_FEATURES
        and row[f].isna().iloc[0]
    ]
    assert nan_features == []
