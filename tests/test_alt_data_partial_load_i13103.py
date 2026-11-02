"""alpha-engine-config-I13103: a PARTIAL alternative load must not zero rows silently.

I12879 made the whole-universe case raise. The partial case — some per-ticker
objects unreadable, or fewer loaded than the manifest's ``tickers_succeeded`` —
still handed every missing ticker the ``compute_features`` defaults (0.0, and
0.5/1.0 for ``iv_rank``/``iv_vs_rv``/``days_since_earnings``): the same values a
genuinely quiet ticker carries, with only a ``log.error`` to tell them apart.

Now a missing ticker's alternative columns are NaN in the snapshot (the
contract's declared absent state), it is named in the run result's
``alt_data_missing``, and a SHORT load makes the run DEGRADED with a reason.
A ticker the producer never covers (``XLRE``, measured 2026-10-30) is NaN-marked
but does not degrade a full load.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from features import compute
from tests.test_alt_data_read_coverage import FakeS3, _universe as _alt_universe

_END = pd.Timestamp("2026-09-24")


def _stock(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.015, n)))
    idx = pd.bdate_range(end=_END, periods=n)
    return pd.DataFrame(
        {
            "Open": close * (1 + rng.normal(0, 0.002, n)),
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
        },
        index=idx,
    )


def _entry(i: int) -> dict:
    return {
        "earnings": {"surprise_pct": 0.01 * (i + 1), "days_since_earnings": 10.0 + i},
        "revisions": {"eps_revision_4w": 0.002 * i, "revision_streak": i % 3},
        "options": {"put_call_ratio": 0.1 * i, "iv_rank": 0.1 + 0.05 * i, "atm_iv": 0.2 + 0.01 * i},
    }


def _patch(monkeypatch, tickers: list[str], alt):
    spy = _stock(300, seed=999)
    price_data = {t: _stock(300, seed=i) for i, t in enumerate(tickers)}
    price_data["SPY"] = spy
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), {"SPY": spy["Close"]}),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_sub_sector_etf_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {})
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: alt)
    # The dead-column postflight is graded elsewhere; this file is about rows.
    monkeypatch.setattr(compute, "find_all_null_columns", lambda df, cols: [])
    monkeypatch.setattr(compute, "find_zero_variance_columns", lambda df, cols: {})


def _alt_load(tickers: list[str], *, short: bool) -> compute.AlternativeLoad:
    alt = compute.AlternativeLoad({t: _entry(i) for i, t in enumerate(tickers)})
    alt.short = short
    return alt


TICKERS = ["AAA", "BBB", "CCC", "DDD"]


def test_alternative_features_are_the_registered_group():
    assert compute.ALTERNATIVE_FEATURES == (
        "earnings_surprise_pct", "days_since_earnings", "eps_revision_4w",
        "revision_streak", "put_call_ratio", "iv_rank", "iv_vs_rv",
    )


def test_a_ticker_missing_from_a_partial_load_is_nan_not_defaults(monkeypatch):
    """RED before the fix: DDD carried 0.0 / 1.0 / 0.5 / 1.0, indistinguishable
    from a ticker whose alternative data really reads that way."""
    _patch(monkeypatch, TICKERS, _alt_load(TICKERS[:3], short=True))

    build = compute.build_feature_frame(_END.date().isoformat(), "test-bucket", s3=None)
    df = build.features_df.set_index("ticker")

    for f in compute.ALTERNATIVE_FEATURES:
        assert math.isnan(df.loc["DDD", f]), f"{f} is {df.loc['DDD', f]!r}, not NaN"
        assert df.loc[["AAA", "BBB", "CCC"], f].notna().all(), f"loaded ticker NaN in {f}"
    assert build.alt_data_missing == ("DDD",)
    assert build.alt_load_short is True
    # Every non-alternative column of the missing ticker is still computed.
    other = [c for c in df.columns if c not in compute.ALTERNATIVE_FEATURES]
    assert df.loc["DDD", other].notna().sum() > 0


def test_a_short_load_degrades_the_run_and_names_the_tickers(monkeypatch):
    from weekly_collector import _DegradedRun

    _patch(monkeypatch, TICKERS, _alt_load(TICKERS[:2], short=True))

    result = compute.compute_and_write(
        date_str=_END.date().isoformat(), bucket="test-bucket",
        dry_run=True, zero_variance_fatal=False,
    )

    assert result["status"] == "degraded"
    assert result["alt_data_missing"] == ["CCC", "DDD"]
    message = str(_DegradedRun("features", result))
    assert "alternative data load SHORT" in message
    assert "DDD" in message
    assert "carry no signal" not in result["reason"]


def test_an_uncovered_ticker_on_a_full_load_is_marked_but_not_degraded(monkeypatch):
    """A ticker the producer never requests (XLRE today) is NaN on every run;
    that alone must not turn every snapshot DEGRADED."""
    _patch(monkeypatch, TICKERS, _alt_load(TICKERS[:3], short=False))

    result = compute.compute_and_write(
        date_str=_END.date().isoformat(), bucket="test-bucket",
        dry_run=True, zero_variance_fatal=False,
    )

    assert result["status"] == "ok"
    assert result["alt_data_missing"] == ["DDD"]
    assert "reason" not in result


def test_a_full_load_marks_nothing(monkeypatch):
    _patch(monkeypatch, TICKERS, _alt_load(TICKERS, short=False))

    build = compute.build_feature_frame(_END.date().isoformat(), "test-bucket", s3=None)

    assert build.alt_data_missing == ()
    assert build.features_df[list(compute.ALTERNATIVE_FEATURES)].notna().all().all()


@pytest.mark.parametrize(
    "unreadable, manifest_short, short",
    [
        (1, False, True),   # one object unreadable
        (0, True, True),    # manifest says more than the partition holds
        (0, False, False),  # full coverage
    ],
)
def test_the_loader_reports_a_short_load(unreadable, manifest_short, short):
    tickers = _alt_universe(20)
    s3 = FakeS3(
        tickers,
        unreadable=set(sorted(tickers)[:unreadable]),
        manifest_count=len(tickers) + 3 if manifest_short else None,
    )
    alt = compute._load_cached_alternative(s3, "alpha-engine-research")
    assert isinstance(alt, compute.AlternativeLoad)
    assert alt.short is short
