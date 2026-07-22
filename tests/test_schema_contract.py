"""Feature-store schema contract test.

Enforces parity across three sources of truth for the feature catalog:

  1. ``features/feature_engineer.FEATURES``           — runtime emit list
  2. ``features/registry.CATALOG``                    — S3 registry.json source
  3. ``features/SCHEMA.md``                           — declarative spec for
                                                       cross-repo consumers

Every column in the feature engineer's output MUST appear in all three.
Adding a column without updating SCHEMA.md is a PR-time failure.

Background: see ``features/SCHEMA.md`` and the
``feature-store-schema-audit-260525.md`` plan doc. The driver was a
silent units mismatch on ``avg_volume_20d`` (predictor ratio vs. scanner
raw shares) that hid for months. The contract layer codifies the
``_raw / _ratio / _zscore / _pct / _log_return`` naming convention and
catches future misnamed columns at PR time.

**Private-pack exemption (alpha-engine-config#1032).** A ``CATALOG``
entry with ``compute=registry.PRIVATE_PACK_COMPUTE`` ("private-pack") is
an alpha-bearing column supplied at runtime by a private feature pack
(``features/private_pack.py``) rather than by this repo's
``feature_engineer.compute_features``. Such entries are exempt from the
``FEATURES`` emit-list sync check ONLY — they must still: be registered
in ``CATALOG``, carry a units-suffixed name (or a written grandfather
exception), and appear in ``SCHEMA.md`` §3 with a real consumer. The one
thing SCHEMA.md is permitted to omit for these rows is the ``Compute``
column body (the literal sentinel replaces the formula/description —
see SCHEMA.md §3b). This is the one contract change #1032 makes; every
other invariant below is unchanged and applies identically to
private-pack rows.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features import gen_schema_md
from features.feature_engineer import FEATURES, compute_features
from features.registry import CATALOG, PRIVATE_PACK_COMPUTE


_FEATURES_DIR = Path(__file__).resolve().parents[1] / "features"
_SCHEMA_MD = _FEATURES_DIR / "SCHEMA.md"


# ── SCHEMA.md parser ─────────────────────────────────────────────────────────

# Match `field_name` at the start of a markdown table row, e.g.
# ``| `avg_volume_20d_raw` | raw shares | ... |``
_FIELD_ROW_RE = re.compile(r"^\|\s*`([a-zA-Z0-9_]+)`\s*\|")

# §3 ("Field catalog") is the authoritative per-column section. The
# §1 ("Naming-convention rule") table also contains backticked tokens
# (the suffix list — `_raw`, `_ratio`, etc.) which are NOT field names.
# Parse only between the §3 header and the next top-level section.
_SECTION_3_HEADER = "## 3. Field catalog"
_SECTION_4_HEADER = "## 4. PR checklist"


def _parse_schema_md_fields() -> set[str]:
    text = _SCHEMA_MD.read_text(encoding="utf-8")
    in_section = False
    fields: set[str] = set()
    for line in text.splitlines():
        if line.startswith(_SECTION_3_HEADER):
            in_section = True
            continue
        if in_section and line.startswith(_SECTION_4_HEADER):
            break
        if not in_section:
            continue
        m = _FIELD_ROW_RE.match(line)
        if m:
            fields.add(m.group(1))
    return fields


# ── Parity tests ─────────────────────────────────────────────────────────────


def test_features_and_catalog_are_in_sync():
    """``FEATURES`` (emit list) and ``CATALOG`` (registry) must agree.

    Private-pack entries (``compute=PRIVATE_PACK_COMPUTE``) are exempt
    from this sync — they are, by design, never in the PUBLIC
    ``feature_engineer.FEATURES`` emit list (alpha-engine-config#1032).
    They are still required to appear in CATALOG (this test's other
    direction) and in SCHEMA.md (enforced separately below).
    """
    catalog_names = {f.name for f in CATALOG}
    private_pack_names = {f.name for f in CATALOG if f.compute == PRIVATE_PACK_COMPUTE}
    public_catalog_names = catalog_names - private_pack_names
    features = set(FEATURES)

    missing_from_catalog = features - catalog_names
    missing_from_features = public_catalog_names - features

    assert not missing_from_catalog, (
        f"Columns in FEATURES but missing from registry.CATALOG "
        f"(add a FeatureEntry to features/registry.py): {sorted(missing_from_catalog)}"
    )
    assert not missing_from_features, (
        f"Columns in registry.CATALOG but missing from FEATURES "
        f"(remove from CATALOG, add to feature_engineer.FEATURES, or if this "
        f"is an alpha-bearing private-pack column mark it "
        f"compute=PRIVATE_PACK_COMPUTE): {sorted(missing_from_features)}"
    )


def test_private_pack_entries_are_absent_from_public_features():
    """Private-pack CATALOG entries must NEVER leak into the public emit list.

    The inverse of the sync exemption above: if a private-pack-marked
    column somehow ALSO appears in feature_engineer.FEATURES, the public
    repo is emitting (i.e. computing) it itself — which defeats the
    entire point of the private-pack mechanism. This is the sniff test
    that would catch someone flipping compute= back to "" without also
    removing the column from the public compute path, or vice versa.
    """
    features = set(FEATURES)
    private_pack_names = {f.name for f in CATALOG if f.compute == PRIVATE_PACK_COMPUTE}
    leaked = private_pack_names & features
    assert not leaked, (
        "Column(s) marked compute=PRIVATE_PACK_COMPUTE also appear in the "
        f"public feature_engineer.FEATURES emit list: {sorted(leaked)}. "
        "A private-pack column must be computed ONLY by the private pack — "
        "remove it from FEATURES or drop the private-pack marking."
    )


def test_schema_md_documents_every_catalog_field():
    """Every registry CATALOG entry must appear in SCHEMA.md §3."""
    schema_fields = _parse_schema_md_fields()
    catalog_names = {f.name for f in CATALOG}

    missing = catalog_names - schema_fields
    assert not missing, (
        "Columns in registry.CATALOG but missing from features/SCHEMA.md §3. "
        "Add a row documenting units + compute + consumers: "
        f"{sorted(missing)}"
    )


def test_schema_md_has_no_orphan_fields():
    """Reverse direction — SCHEMA.md must not mention deleted features."""
    schema_fields = _parse_schema_md_fields()
    catalog_names = {f.name for f in CATALOG}

    orphans = schema_fields - catalog_names
    assert not orphans, (
        "Columns documented in SCHEMA.md but not in registry.CATALOG "
        "(stale doc — remove the SCHEMA.md row or restore the CATALOG entry): "
        f"{sorted(orphans)}"
    )


# ── §3 generated-table drift check (alpha-engine-config#2590) ───────────────
#
# The tests above only check that every field NAME appears somewhere in §3.
# They would not catch a stale Units/Compute/Consumers cell — exactly the
# drift class that caused the avg_volume_20d incident (SCHEMA.md §1). This
# test closes that gap: SCHEMA.md §3's table BLOCKS must be byte-identical
# to a fresh render from registry.CATALOG's units/formula/consumers/
# display_order fields (features/gen_schema_md.py). A PR that edits a
# CATALOG entry's units/formula/consumers without regenerating SCHEMA.md,
# or that hand-edits a §3 table cell without touching CATALOG, fails here.


def test_schema_md_section_3_matches_fresh_catalog_render():
    """SCHEMA.md §3's generated table blocks must equal a fresh CATALOG render.

    Run `python3 features/gen_schema_md.py --write` and commit the result
    to fix. Prose (lead-in paragraphs, the Factor-loadings intro + the
    roe_zscore known-degenerate writeup, and all of §3b) is untouched by
    the generator and is not covered by this check — only the four-column
    `| Field | Units | Compute | Consumers |` table blocks are generated.
    """
    committed = gen_schema_md.SCHEMA_MD.read_text(encoding="utf-8")
    fresh = gen_schema_md.rewrite_schema_md(committed)
    assert committed == fresh, (
        "features/SCHEMA.md §3 has drifted from features/registry.py::CATALOG. "
        "Run `python3 features/gen_schema_md.py --write` and commit the "
        "regenerated file."
    )


def test_gen_schema_md_render_is_deterministic():
    """Rendering twice from the same CATALOG must produce identical output —
    guards against any accidental nondeterminism (e.g. dict/set ordering)
    creeping into the generator."""
    first = gen_schema_md.render_all_tables()
    second = gen_schema_md.render_all_tables()
    assert first == second


def test_gen_schema_md_covers_every_group_section():
    """Every CATALOG group must have a corresponding §3 subsection wired
    into the generator — catches a future new `group` value that the
    generator doesn't know how to render."""
    catalog_groups = {f.group for f in CATALOG}
    generator_groups = {group for _header, group in gen_schema_md.GROUP_SECTIONS}
    assert catalog_groups == generator_groups, (
        "features/gen_schema_md.py::GROUP_SECTIONS is out of sync with the "
        f"groups actually present in CATALOG. catalog={sorted(catalog_groups)} "
        f"generator={sorted(generator_groups)}"
    )


def test_private_pack_row_renders_sentinel_not_formula():
    """A private-pack CATALOG entry's rendered Compute cell must be the
    literal sentinel text, never its (nonexistent) formula — proves the
    generator's disclosure-format branch (SCHEMA.md §3b) independent of
    whether any real private-pack entry exists in CATALOG today."""
    from features.registry import FeatureEntry

    entry = FeatureEntry(
        name="dummy_private_render_check_raw",
        group="technical",
        description="d",
        compute=PRIVATE_PACK_COMPUTE,
        units="raw shares",
        formula="",
        consumers="predictor",
        display_order=999,
    )
    assert gen_schema_md._compute_cell(entry) == "private pack"


# ── Naming-convention sniff tests ────────────────────────────────────────────


_ALLOWED_SUFFIXES = ("_raw", "_ratio", "_pct", "_zscore", "_log_return")

# Bare-named fields are grandfathered (see SCHEMA.md §1). New bare-named
# fields are NOT permitted — the test below enforces it. Adding a new
# bare-named field requires updating this set AND justifying it in the PR
# body.
_GRANDFATHERED_BARE_FIELDS: frozenset[str] = frozenset({
    # Technical
    "rsi_14", "macd_cross", "macd_above_zero", "macd_line_last",
    "price_vs_ma50", "price_vs_ma200", "momentum_20d", "avg_volume_20d",
    "dist_from_52w_high", "momentum_5d", "rel_volume_ratio",
    "return_vs_spy_5d", "dist_from_52w_low", "vol_ratio_10_60",
    "bollinger_pct", "sector_vs_spy_5d", "sector_vs_spy_10d",
    "sector_vs_spy_20d", "price_accel", "ema_cross_8_21", "atr_14_pct",
    "realized_vol_20d", "realized_vol_63d", "volume_trend",
    "obv_slope_10d", "rsi_slope_5d", "volume_price_div",
    "return_60d", "return_120d", "overnight_return_5d",
    "intraday_return_5d", "dist_from_5d_high", "dist_from_20d_high",
    "beta_60d", "idio_vol_60d", "vol_of_vol_30d", "max_drawdown_60d",
    # Macro
    "vix_level", "yield_10y", "yield_curve_slope", "gold_mom_5d",
    "oil_mom_5d", "vix_term_slope", "xsect_dispersion",
    # Interaction
    "mom5d_x_vix", "rsi_x_vix", "sector_x_trend", "atr_x_vix",
    "vol_trend_x_vix",
    # Alternative
    "earnings_surprise_pct", "days_since_earnings", "eps_revision_4w",
    "revision_streak", "put_call_ratio", "iv_rank", "iv_vs_rv",
    # Fundamental
    "pe_ratio", "pb_ratio", "debt_to_equity", "revenue_growth_yoy",
    "fcf_yield", "gross_margin", "roe", "current_ratio",
    "revenue_growth_3y", "eps_growth_3y", "payout_ratio",
    "dividend_yield", "capex_growth_5y",
})


def test_new_fields_must_carry_units_suffix():
    """New fields (not grandfathered) MUST end in an allowed suffix.

    The grandfathered set is frozen as of 2026-05-25 (the avg_volume_20d
    audit). Any future addition that lacks a recognized suffix and is
    not in the grandfathered set fails here — by intent. The PR author
    must either rename the field (preferred) or update the grandfathered
    set with a written rationale.
    """
    catalog_names = {f.name for f in CATALOG}
    new_bare_names: list[str] = []
    for name in catalog_names:
        if name in _GRANDFATHERED_BARE_FIELDS:
            continue
        if not any(name.endswith(suf) for suf in _ALLOWED_SUFFIXES):
            new_bare_names.append(name)
    assert not new_bare_names, (
        "New feature columns lack an explicit units suffix "
        f"(allowed: {_ALLOWED_SUFFIXES}). Either rename to add a suffix "
        "or add to _GRANDFATHERED_BARE_FIELDS with a PR-body rationale: "
        f"{sorted(new_bare_names)}"
    )


def test_grandfathered_set_matches_documented_bare_names():
    """The grandfathered set must be a subset of CATALOG.

    Catches drift if a grandfathered field is renamed or removed without
    updating this list.
    """
    catalog_names = {f.name for f in CATALOG}
    stale = _GRANDFATHERED_BARE_FIELDS - catalog_names
    assert not stale, (
        "_GRANDFATHERED_BARE_FIELDS references columns not in CATALOG. "
        "Either restore the column or remove it from the grandfathered "
        f"set: {sorted(stale)}"
    )


# ── Units sniff test ─────────────────────────────────────────────────────────


def _synthetic_ohlcv(n: int = 400, seed: int = 0) -> pd.DataFrame:
    """Minimal OHLCV frame sufficient for compute_features warmup."""
    rng = np.random.default_rng(seed)
    daily_returns = rng.normal(0.0005, 0.012, n)
    close = 100.0 * np.exp(np.cumsum(daily_returns))
    high = close * (1 + np.abs(rng.normal(0, 0.005, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.005, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    # Realistic raw share volume — well above the 500k MIN_AVG_VOLUME gate.
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=idx,
    )


def test_avg_volume_20d_raw_is_in_raw_share_units():
    """The raw column must produce values >> 1.0 (raw shares scale).

    Sniff-test: if a future refactor accidentally normalizes
    ``avg_volume_20d_raw`` (e.g., copies the divisor from the
    normalized field), values will drop to ~1.0 and this test fails
    loudly. Mirrors the institutional check the Research scanner now
    relies on.
    """
    df = _synthetic_ohlcv()
    out = compute_features(df)
    # Drop rolling-window warmup rows.
    raw = out["avg_volume_20d_raw"].dropna()
    assert len(raw) > 0, "Insufficient warmup — synthetic frame too short."

    median_raw = float(raw.median())
    # Synthetic volume in [1M, 10M] — median rolling-20d-avg must be
    # solidly within that band. The Research scanner's MIN_AVG_VOLUME
    # gate is 500_000; we want to be at least an order of magnitude above
    # the gate threshold so the sniff has real teeth.
    assert median_raw >= 1_000_000, (
        f"avg_volume_20d_raw median is {median_raw:,.0f} but should be "
        ">= 1,000,000 (raw shares scale). Likely re-normalized — check "
        "feature_engineer.py."
    )


def test_avg_volume_20d_is_normalized_ratio():
    """The bare-named column must be ~1.0 (per-ticker normalization)."""
    df = _synthetic_ohlcv()
    out = compute_features(df)
    ratio = out["avg_volume_20d"].dropna()
    assert len(ratio) > 0
    median_ratio = float(ratio.median())
    # Ratio = rolling_20d / global_mean; close to 1.0 by construction.
    assert 0.5 <= median_ratio <= 2.0, (
        f"avg_volume_20d median ratio is {median_ratio:.4f} — expected "
        "~1.0. Predictor consumes this as a normalized relative-liquidity "
        "feature."
    )


def test_avg_volume_20d_raw_is_orders_of_magnitude_above_ratio():
    """Cross-check the two columns are clearly distinct in scale.

    The original bug class was confusing one for the other. This test
    pins the assertion that they MUST live on different scales: the raw
    one should be at least 1e5x larger than the ratio one.
    """
    df = _synthetic_ohlcv()
    out = compute_features(df)
    raw_med = float(out["avg_volume_20d_raw"].dropna().median())
    ratio_med = float(out["avg_volume_20d"].dropna().median())
    assert raw_med / max(ratio_med, 1e-12) > 1e5, (
        "avg_volume_20d_raw and avg_volume_20d are on similar scales — "
        f"raw={raw_med:.2e}, ratio={ratio_med:.4f}. The whole point of "
        "the two-column emit is that they are on different scales."
    )


# ── Description quality ─────────────────────────────────────────────────────


def test_avg_volume_descriptions_disambiguate_units():
    """Registry descriptions for the two columns must explicitly name units.

    Avoids the original "20-day avg volume / global mean volume" wording
    that was ambiguous about units.
    """
    by_name = {f.name: f for f in CATALOG}
    raw_desc = by_name["avg_volume_20d_raw"].description.lower()
    ratio_desc = by_name["avg_volume_20d"].description.lower()

    assert "raw shares" in raw_desc, (
        "avg_volume_20d_raw description must include 'raw shares' to "
        f"disambiguate units: {raw_desc!r}"
    )
    # Ratio description should reference the per-ticker normalization
    # so consumers can't misread it as raw.
    assert "ratio" in ratio_desc or "normalized" in ratio_desc or "/" in ratio_desc, (
        "avg_volume_20d description must explicitly flag it as a "
        f"ratio / normalized field: {ratio_desc!r}"
    )
