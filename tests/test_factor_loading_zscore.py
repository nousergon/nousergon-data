"""C.1 — factor-loading cross-sectional z-score tests.

Plan: alpha-engine-docs/private/optimizer-sota-upgrades-260526.md §C.1

The `*_zscore` columns are the loadings of the factor-loading matrix B
the executor consumes for the Σ = B·F·Bᵀ + D risk decomposition (C.3).
These tests cover the cross-sectional ±3σ-winsorized standardization:

  • z-score outputs have mean ≈ 0, std ≈ 1 within the universe at each
    date (the load-bearing institutional property)
  • Winsorization at ±3σ prevents single outliers from breaking the
    cross-sectional distribution
  • Degenerate distribution (σ=0) yields NaN, not div-by-zero
  • Missing source column yields all-NaN destination column (no crash,
    partial-rollout tolerated)
  • Existing source columns flow through `apply_factor_zscores` correctly
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from features.cross_sectional import (
    FACTOR_LOADING_SOURCES,
    _WINSORIZE_SIGMA,
    _winsorize_and_zscore,
    apply_factor_zscores,
    factor_loading_columns,
)


class TestWinsorizeAndZscore:
    def test_zero_mean_unit_std_on_well_behaved_input(self):
        """The load-bearing standardization property — after the function
        runs on a panel with no outliers, the result is approximately
        mean 0 / std 1. Approximate because winsorization preserves the
        distribution shape but the re-standardization is exact."""
        rng = np.random.default_rng(0)
        s = pd.Series(rng.normal(loc=0.5, scale=2.0, size=500))
        z = _winsorize_and_zscore(s)
        assert abs(float(z.mean())) < 0.05
        assert abs(float(z.std(ddof=0)) - 1.0) < 0.05

    def test_winsorization_clips_outliers_at_3_sigma(self):
        """A single 100σ outlier must be clipped — without winsorization
        the re-standardized output would have one entry near 100 and the
        rest crushed to ~0."""
        rng = np.random.default_rng(1)
        s = pd.Series(rng.normal(0, 1.0, size=200))
        s.iloc[0] = 100.0  # extreme outlier
        z = _winsorize_and_zscore(s)
        # Without winsorization, max(z) would be ≈ 14. With winsorization,
        # the outlier should be clipped to within a few σ.
        assert abs(float(z.iloc[0])) < 5.0
        # And the rest of the distribution stays in a normal range
        assert float(z.iloc[1:].abs().max()) < 5.0

    def test_degenerate_constant_input_yields_all_nan(self):
        """Universe of identical values → no meaningful cross-section.
        Per no-silent-fails: returning 0 here would silently treat the
        degenerate case as 'all average', which would propagate into the
        factor-return regression as a zero column."""
        s = pd.Series([0.05] * 100)
        z = _winsorize_and_zscore(s)
        assert z.isna().all()

    def test_too_few_finite_values_yields_all_nan(self):
        """<2 finite values can't compute a meaningful σ → all-NaN."""
        s = pd.Series([np.nan, np.nan, 0.05, np.nan, np.nan])
        z = _winsorize_and_zscore(s)
        assert z.isna().all()

    def test_partial_nan_input_preserves_nans(self):
        """NaN inputs propagate to NaN outputs; finite inputs are
        z-scored on the finite cross-section."""
        rng = np.random.default_rng(3)
        values = rng.normal(0, 1, size=100)
        s = pd.Series(values, dtype=float)
        s.iloc[5:10] = np.nan
        z = _winsorize_and_zscore(s)
        assert z.iloc[5:10].isna().all()
        finite_z = z[np.isfinite(z)]
        assert len(finite_z) == 95
        assert abs(float(finite_z.mean())) < 0.1
        assert abs(float(finite_z.std(ddof=0)) - 1.0) < 0.1

    def test_winsorize_sigma_constant_is_three(self):
        """Document the winsorization radius as a stable constant."""
        assert _WINSORIZE_SIGMA == 3.0


class TestApplyFactorZscores:
    def _baseline_panel(self):
        """Synthetic cross-sectional panel with 50 tickers and all 8
        source columns populated. Each column has a distinct scale to
        verify standardization handles heterogeneous units."""
        rng = np.random.default_rng(7)
        return pd.DataFrame({
            "ticker":              [f"T{i:02d}" for i in range(50)],
            "momentum_20d":        rng.normal(0.01, 0.05, 50),
            "return_60d":          rng.normal(0.05, 0.15, 50),
            "beta_60d":            rng.normal(1.0, 0.3, 50),
            "idio_vol_60d":        rng.uniform(0.10, 0.40, 50),
            "realized_vol_63d":    rng.uniform(0.15, 0.45, 50),
            "dist_from_52w_high":  rng.uniform(-0.30, 0.0, 50),
            "pe_ratio":            rng.lognormal(3.0, 0.3, 50),
            "roe":                 rng.normal(0.12, 0.08, 50),
        })

    def test_emits_all_eight_factor_loading_columns(self):
        df = self._baseline_panel()
        out = apply_factor_zscores(df)
        for zcol in factor_loading_columns():
            assert zcol in out.columns, f"Missing z-score column: {zcol}"

    def test_emitted_columns_are_zero_mean_unit_std_per_factor(self):
        """The load-bearing institutional property: each emitted z-score
        column has mean ≈ 0, std ≈ 1 within the universe at this date.
        This is what makes B a proper Barra-style loading matrix."""
        df = self._baseline_panel()
        out = apply_factor_zscores(df)
        for src, dst in FACTOR_LOADING_SOURCES.items():
            z = out[dst].dropna()
            assert abs(float(z.mean())) < 0.05, (
                f"Column {dst} not centered: mean={float(z.mean()):.4f}"
            )
            assert abs(float(z.std(ddof=0)) - 1.0) < 0.1, (
                f"Column {dst} not unit-std: std={float(z.std(ddof=0)):.4f}"
            )

    def test_source_columns_preserved(self):
        """Z-scoring ADDS columns; the original raw columns must remain
        unchanged so downstream consumers reading bare names still work."""
        df = self._baseline_panel()
        out = apply_factor_zscores(df)
        for src in FACTOR_LOADING_SOURCES.keys():
            pd.testing.assert_series_equal(df[src], out[src], check_names=False)

    def test_missing_source_column_emits_all_nan_zscore(self):
        """Partial-rollout tolerance: if a source column is absent (e.g.,
        a feature-store snapshot pre-dates a given column), the destination
        z-score column is emitted as all-NaN with a WARN log — no crash."""
        df = self._baseline_panel()
        df = df.drop(columns=["pe_ratio"])
        out = apply_factor_zscores(df)
        assert "pe_ratio_zscore" in out.columns
        assert out["pe_ratio_zscore"].isna().all()
        # Other z-scores still computed correctly
        assert not out["momentum_20d_zscore"].isna().all()

    def test_input_panel_returned_as_copy_not_mutated(self):
        """The helper must not mutate the input panel — fail-loud on any
        in-place add that would surprise the caller."""
        df = self._baseline_panel()
        df_before_cols = list(df.columns)
        _ = apply_factor_zscores(df)
        assert list(df.columns) == df_before_cols, (
            "apply_factor_zscores must not mutate the input frame"
        )

    def test_custom_sources_argument_routes_correctly(self):
        """Caller can override the default factor map (used by tests +
        future C.x extensions that re-use the helper for different
        loadings without touching the canonical list)."""
        df = self._baseline_panel()
        custom = {"beta_60d": "my_beta_z"}
        out = apply_factor_zscores(df, sources=custom)
        assert "my_beta_z" in out.columns
        assert "beta_60d_zscore" not in out.columns
        # Default emit not invoked
        assert "momentum_20d_zscore" not in out.columns

    def test_default_source_map_matches_eight_canonical_factors(self):
        """The default factor set is the v1 canonical Barra-style loading
        matrix. Adding a 9th factor here also requires CATALOG + SCHEMA.md
        updates — keep this test as the chokepoint."""
        assert len(FACTOR_LOADING_SOURCES) == 8
        expected_sources = {
            "momentum_20d", "return_60d", "beta_60d", "idio_vol_60d",
            "realized_vol_63d", "dist_from_52w_high", "pe_ratio", "roe",
        }
        assert set(FACTOR_LOADING_SOURCES.keys()) == expected_sources

    def test_z_score_is_orthogonal_to_input_units(self):
        """Z-score output range is in σ-units (typically [-3, 3] after
        winsorization). Independent of the source column's native scale.
        Verifies a 1000× scale change in input does NOT change z-score
        output (up to numerical noise)."""
        df = self._baseline_panel()
        out_small = apply_factor_zscores(df)
        df_scaled = df.copy()
        df_scaled["beta_60d"] = df["beta_60d"] * 1000.0
        out_scaled = apply_factor_zscores(df_scaled)
        # Z-score of the rescaled column should match the original
        pd.testing.assert_series_equal(
            out_small["beta_60d_zscore"],
            out_scaled["beta_60d_zscore"],
            check_names=False,
            atol=1e-10,
        )

    def test_typical_z_score_range_within_winsorization_bounds(self):
        """After ±3σ winsorization the output should mostly sit in
        roughly [-3, 3] (with tolerance for the re-standardization
        slightly expanding it). Sanity check on the magnitude scale."""
        df = self._baseline_panel()
        out = apply_factor_zscores(df)
        for dst in factor_loading_columns():
            z = out[dst].dropna()
            if len(z) == 0:
                continue
            assert float(z.abs().max()) <= 4.5, (
                f"Column {dst} has z-score outside ±4.5σ after winsorization: "
                f"max={float(z.abs().max()):.4f}"
            )


class TestFactorLoadingsRegisteredInSchema:
    """Defensive cross-check that the v1 set is wired into the schema
    contract (the existing test_schema_contract suite enforces parity
    automatically; these are belt-and-suspenders against silent removal)."""

    def test_each_zscore_column_in_features_list(self):
        from features.feature_engineer import FEATURES
        for zcol in factor_loading_columns():
            assert zcol in FEATURES, (
                f"{zcol} missing from features/feature_engineer.py::FEATURES"
            )

    def test_each_zscore_column_in_catalog(self):
        from features.registry import CATALOG
        names = {f.name for f in CATALOG}
        for zcol in factor_loading_columns():
            assert zcol in names, (
                f"{zcol} missing from features/registry.py::CATALOG"
            )

    def test_each_zscore_column_uses_factor_loading_group(self):
        from features.registry import CATALOG
        by_name = {f.name: f for f in CATALOG}
        for zcol in factor_loading_columns():
            entry = by_name[zcol]
            assert entry.group == "factor_loading", (
                f"{zcol} expected group='factor_loading'; got {entry.group!r}"
            )
