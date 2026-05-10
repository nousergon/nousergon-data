"""Tests for Stage 2.5 of the regime-conditioning rebuild — DGS2 (2Y
treasury) and HY OAS credit spread added to daily_closes._FRED_INDEX_MAP
for forward-only daily ingestion. Historical backfill is a follow-up PR.

Plan doc: ~/Development/alpha-engine-docs/private/regime-conditioning-260510.md

These tests lock the contract that:
- TWO maps to FRED series DGS2 (2Y constant maturity treasury)
- HYOAS maps to FRED series BAMLH0A0HYM2 (HY OAS, percent)
- Both are exposed via the same FRED-fallback path the existing index
  tickers use, so daily_closes will write parquet records when polygon
  doesn't carry the symbol.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.daily_closes import _FRED_INDEX_MAP


class TestFredIndexMapAdditions:

    def test_two_maps_to_dgs2(self):
        # 2Y treasury — enables 10Y-2Y curve slope (recession-canonical)
        # alongside the existing 10Y-3M (TNX-IRX cyclical).
        assert _FRED_INDEX_MAP.get("TWO") == "DGS2"

    def test_hyoas_maps_to_bamlh0a0hym2(self):
        # ICE BofA US High Yield Index Option-Adjusted Spread, percent.
        # Major regime indicator that VIX misses — credit widens before
        # vol spikes in many cycles.
        assert _FRED_INDEX_MAP.get("HYOAS") == "BAMLH0A0HYM2"

    def test_baa10y_maps_to_baa10y(self):
        # Stage 2.5c: Moody's BAA Corporate Bond Yield Relative to 10Y
        # Treasury — full 40y FRED history (1986+), the credit-regime
        # signal HYOAS cannot provide across the full predictor training
        # corpus.
        assert _FRED_INDEX_MAP.get("BAA10Y") == "BAA10Y"

    def test_existing_mappings_unchanged(self):
        # Regression: ensure the additions didn't perturb the existing
        # mappings.
        assert _FRED_INDEX_MAP["VIX"] == "VIXCLS"
        assert _FRED_INDEX_MAP["VIX3M"] == "VXVCLS"
        assert _FRED_INDEX_MAP["TNX"] == "DGS10"
        assert _FRED_INDEX_MAP["IRX"] == "DTB3"

    def test_no_yfinance_caret_for_fred_only_symbols(self):
        # TWO, HYOAS, BAA10Y are FRED-only — no yfinance caret prefix.
        # Verifies the integration path routes them through FRED fallback only.
        from collectors.prices import _CARET_SYMBOLS
        assert "TWO" not in _CARET_SYMBOLS
        assert "HYOAS" not in _CARET_SYMBOLS
        assert "BAA10Y" not in _CARET_SYMBOLS

    def test_index_map_total_size(self):
        # Stage 2.5 (TWO + HYOAS) + 2.5c (BAA10Y) add 3 entries on top of
        # the original 4 (VIX/VIX3M/TNX/IRX). Lock so a future drive-by
        # addition doesn't slip through unreviewed.
        assert len(_FRED_INDEX_MAP) == 7
