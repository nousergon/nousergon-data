"""Split-ratio hint on polygon_only OVERWRITE ERRORs (config#1030).

KLAC's 10-for-1 split (effective 2026-06-10) restated three windowed dates by
exactly ÷10; the ERROR messages said only "90.00% diff" and the LLM
auto-diagnosis blamed a producer decimal-shift bug (data#417-419). The hint
puts the strongest evidence — the clean integer ratio — in the message itself.
"""

from __future__ import annotations

import logging

import pandas as pd

from collectors import daily_closes


class TestSplitRatioHint:
    def test_klac_forward_split_ratio_detected(self):
        hint = daily_closes._split_ratio_hint(2139.37, 213.937)
        assert "10:1" in hint
        assert "10-for-1 forward stock split" in hint

    def test_reverse_split_ratio_detected(self):
        hint = daily_closes._split_ratio_hint(2.5, 25.0)
        assert "10:1" in hint
        assert "1-for-10 reverse stock split" in hint

    def test_plain_drift_yields_no_hint(self):
        # 7% cross-source drift — over the ERROR band but nowhere near a clean ratio.
        assert daily_closes._split_ratio_hint(100.0, 93.0) == ""

    def test_ratio_outside_tolerance_yields_no_hint(self):
        # ÷9.8 is 2% off 10:1 — a genuine anomaly must not be masked as a split.
        assert daily_closes._split_ratio_hint(980.0, 100.0) == ""

    def test_degenerate_inputs_yield_no_hint(self):
        assert daily_closes._split_ratio_hint(0.0, 100.0) == ""
        assert daily_closes._split_ratio_hint(100.0, -1.0) == ""

    def test_two_for_one_boundary_detected(self):
        assert "2:1" in daily_closes._split_ratio_hint(100.0, 50.0)

    def test_unity_ratio_never_hints(self):
        # 1:1 (no diff) must not match the N>=2 floor.
        assert daily_closes._split_ratio_hint(100.0, 100.0) == ""


class TestOverwriteErrorCarriesHint:
    def test_error_record_includes_split_hint(self, caplog):
        new_df = pd.DataFrame({"Close": [213.937]}, index=["KLAC"])
        with caplog.at_level(logging.DEBUG):
            daily_closes._log_close_discrepancies(new_df, {"KLAC": 2139.37}, "2026-06-09")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "polygon_only OVERWRITE KLAC" in errors[0].message
        assert "10:1" in errors[0].message

    def test_non_split_error_record_has_no_hint(self, caplog):
        new_df = pd.DataFrame({"Close": [93.0]}, index=["AAPL"])
        with caplog.at_level(logging.DEBUG):
            daily_closes._log_close_discrepancies(new_df, {"AAPL": 100.0}, "2026-06-09")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "ratio" not in errors[0].message
