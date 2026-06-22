"""Regression: macro series fed to compute_features must be monotonic.

2026-06-22 weekday-SF failure. Juneteenth (Fri 2026-06-19) was a NYSE
holiday, so the Monday 2026-06-22 weekday pipeline's MorningArcticAppend
carried ``today_ts = Thu 2026-06-18`` — a date EARLIER than the latest
session already stored in the ArcticDB macro library. The macro-load loop
in ``_daily_append_impl`` appended today's close to each macro series with
``pd.concat(...)`` + ``drop_duplicates(keep="last")`` but did NOT re-sort.
Dedup-keep-last moves the today_ts row to the tail of an otherwise-ascending
index, leaving it non-monotonic. ``compute_features`` then calls
``macro.reindex(df.index, method="ffill")`` (VIX is the first such reindex),
which raises ``"index must be monotonic increasing or decreasing"`` — and
because the macro series are SHARED, it failed for EVERY ticker
(n_err == len(universe) → >5% error-rate → daily_append raised → SF FAILED).

Fix: ``.sort_index()`` after the today_ts concat+dedup at both macro sites,
mirroring the per-ticker ``combined`` / parquet-``warmup_source`` frames which
already dedup AND sort.

This test reproduces the holiday-backfill ordering (stored macro index extends
PAST today_ts) and asserts every macro series handed to compute_features is
monotonic. A revert that drops the sort fails here.
"""

from __future__ import annotations

import pandas as pd
import pytest

import tests.test_daily_append_missing_from_closes as harness


def test_macro_series_monotonic_when_today_predates_stored_max(monkeypatch):
    from builders import daily_append as _da
    from features.feature_engineer import FEATURES

    # today_ts deliberately EARLIER than the latest stored macro session,
    # reproducing the Juneteenth-Monday backfill ordering.
    today_str = "2026-06-18"

    universe_lib, macro_lib = harness._patch_targets(
        monkeypatch,
        universe_symbols=["AAPL", "MSFT"],
        closes_tickers=["AAPL", "MSFT"],
    )

    # Override the stored macro frame so its index extends PAST today_ts:
    # a clean ascending history plus a LATER session (2026-06-19) that the
    # weekly/Saturday backfill had already written. Appending today_ts=6/18
    # and dedup-keep-last (without a re-sort) would land 6/18 after 6/19.
    macro_dates = pd.bdate_range("2025-01-01", "2026-06-19")
    macro_df = pd.DataFrame({"Close": 100.0}, index=macro_dates)
    macro_df.index.name = "date"
    macro_lib.read.return_value = type(
        "_R", (), {"data": macro_df.copy()}
    )()

    # Writing today_ts=6/18 into a series whose max is 6/19 is a BACKFILL
    # write (not append-at-head), so step-2a's mode-aware verification only
    # checks that 6/18 is present in the index — which it is. Match that real
    # behaviour; the harness default stub returns "append" (strict last-row
    # check), which is the wrong mode for this date ordering.
    monkeypatch.setattr(
        _da, "_write_row_backfill_safe",
        lambda lib, sym, df, existing_series=None: "backfill",
    )

    # Capture the macro series actually handed to compute_features and assert
    # each is monotonic. Returns a today_ts-bearing frame with a few FEATURES
    # so the write-extraction path stays happy (no error from the stub itself).
    seen: list[str] = []

    def _checking_compute_features(combined, **kw):
        for name in (
            "spy_series", "vix_series", "tnx_series", "irx_series",
            "gld_series", "uso_series", "vix3m_series", "sector_etf_series",
        ):
            s = kw.get(name)
            if s is not None and len(s):
                seen.append(name)
                assert s.index.is_monotonic_increasing, (
                    f"{name} reached compute_features non-monotonic — "
                    f"macro today_ts concat must re-sort (2026-06-22 SF fail)"
                )
        out = combined.copy()
        for f in list(FEATURES)[:3]:
            out[f] = 0.5
        return out

    monkeypatch.setattr(_da, "compute_features", _checking_compute_features)

    result = _da.daily_append(date_str=today_str)

    # With the fix the run completes cleanly; the monotonic asserts above are
    # the real guard, and a non-monotonic series would have raised inside the
    # per-ticker loop → tickers_errored > 0 → >5% → RuntimeError.
    assert result["status"] == "ok"
    assert result["tickers_errored"] == 0
    assert "vix_series" in seen, "compute_features never received vix_series"
