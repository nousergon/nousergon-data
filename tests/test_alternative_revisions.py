"""Tests for the EPS-revision sub-collector in ``collectors/alternative.py``.

Contract as of 2026-05-18 (FMP→yfinance migration):

  * Source is yfinance ``Ticker.eps_trend`` (the FMP ``analyst-estimates``
    endpoint began 402-ing on the free tier ~2026-05-17 — paid-tier only).
  * Return dict shape/keys are UNCHANGED:
        {"current_estimate": <float|None>,
         "revision_4w": <float|None>,
         "streak": <int>}
  * ``current_estimate`` ← ``eps_trend`` ``0y`` row, ``current`` column.
  * ``revision_4w``      ← ``(current - 30daysAgo) / abs(30daysAgo) * 100``
                            on the ``0y`` row, rounded to 2dp.
  * ``streak``           ← count of consecutive non-negative steps walking
                            newest→oldest across
                            current → 7daysAgo → 30daysAgo → 60daysAgo →
                            90daysAgo on the ``0y`` row (max 4).
  * yfinance failures must degrade loudly (WARN) and never raise — the
    function is called per-ticker and one provider outage must not poison
    the whole Phase 2 batch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from collectors import alternative


_EPS_TREND_COLS = ["current", "7daysAgo", "30daysAgo", "60daysAgo", "90daysAgo"]


def _eps_trend_df(rows: dict[str, list]) -> pd.DataFrame:
    """Build an eps_trend-shaped DataFrame (index = period rows)."""
    df = pd.DataFrame.from_dict(rows, orient="index", columns=_EPS_TREND_COLS)
    df.index.name = "period"
    return df


def _yf_with_trend(df) -> MagicMock:
    mod = MagicMock()
    mod.Ticker.return_value.eps_trend = df
    return mod


def _no_s3():
    """Patch boto3 so the dead S3 fallback never touches the network."""
    return patch.object(alternative, "boto3", MagicMock())


def test_contract_keys_and_types_unchanged():
    df = _eps_trend_df({
        "0q":  [1.89, 1.89, 1.73, 1.73, 1.73],
        "+1q": [2.01, 2.00, 1.97, 1.97, 1.98],
        "0y":  [8.74, 8.76, 8.50, 8.50, 8.47],
        "+1y": [9.63, 9.60, 9.33, 9.33, 9.30],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("AAPL", "bkt", "2026-05-18")

    assert set(out.keys()) == {"current_estimate", "revision_4w", "streak"}
    assert isinstance(out["current_estimate"], float)
    assert isinstance(out["revision_4w"], float)
    assert isinstance(out["streak"], int)
    # current_estimate = 0y row "current"
    assert out["current_estimate"] == 8.74


def test_revision_4w_positive_delta_math():
    # 0y: current 8.74 vs 30daysAgo 8.50 → +2.8235.. % → 2.82
    df = _eps_trend_df({
        "0y": [8.74, 8.76, 8.50, 8.49, 8.47],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("AAPL", "bkt", "2026-05-18")

    assert out["revision_4w"] == round((8.74 - 8.50) / abs(8.50) * 100, 2)
    assert out["revision_4w"] > 0


def test_revision_4w_negative_delta_when_estimate_cut():
    # estimate cut over the trailing 4w → negative revision_4w
    df = _eps_trend_df({
        "0y": [3.00, 3.10, 3.50, 3.50, 3.60],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("XYZ", "bkt", "2026-05-18")

    assert out["revision_4w"] == round((3.00 - 3.50) / abs(3.50) * 100, 2)
    assert out["revision_4w"] < 0


def test_revision_4w_handles_negative_eps_estimates():
    """Never-profitable names carry negative EPS — abs() denominator keeps
    the sign of the numerator (estimate getting less negative = positive
    revision)."""
    df = _eps_trend_df({
        "0y": [-1.50, -1.51, -1.64, -1.64, -1.61],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("RBLX", "bkt", "2026-05-18")

    # (-1.50) - (-1.64) = +0.14 over abs(1.64) → positive (estimate improved)
    assert out["revision_4w"] == round((-1.50 - -1.64) / abs(-1.64) * 100, 2)
    assert out["revision_4w"] > 0


def test_streak_counts_consecutive_non_negative_steps():
    # newest→oldest: 9>8 ok, 8>7 ok, 7>6 ok, 6>5 ok → streak 4
    df = _eps_trend_df({
        "0y": [9.0, 8.0, 7.0, 6.0, 5.0],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("UP", "bkt", "2026-05-18")
    assert out["streak"] == 4


def test_streak_breaks_on_first_cut():
    # current(8) vs 7d(9): 8-9 = -1 < 0 → streak breaks immediately → 0
    df = _eps_trend_df({
        "0y": [8.0, 9.0, 9.0, 9.0, 9.0],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("CUT", "bkt", "2026-05-18")
    assert out["streak"] == 0


def test_streak_partial_run():
    # current(9)>=7d(8) ok; 7d(8) vs 30d(10) → -2 break → streak 1
    df = _eps_trend_df({
        "0y": [9.0, 8.0, 10.0, 10.0, 10.0],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("PART", "bkt", "2026-05-18")
    assert out["streak"] == 1


def test_missing_30days_ago_leaves_revision_none_but_estimate_populated():
    """NaN in the 30daysAgo slot must null revision_4w without nulling the
    estimate (so _has_revision_data still counts the ticker as populated)."""
    df = _eps_trend_df({
        "0y": [8.74, 8.76, float("nan"), 8.49, 8.47],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("AAPL", "bkt", "2026-05-18")

    assert out["current_estimate"] == 8.74
    assert out["revision_4w"] is None
    assert alternative._has_revision_data(out) is True


def test_empty_eps_trend_returns_none_safe_defaults():
    """yfinance returning an empty DataFrame must not raise and must yield
    the all-None/0 default contract."""
    with _no_s3(), patch.dict(
        "sys.modules", {"yfinance": _yf_with_trend(pd.DataFrame())}
    ):
        out = alternative._fetch_revisions("THIN", "bkt", "2026-05-18")

    assert out == {"current_estimate": None, "revision_4w": None, "streak": 0}
    assert alternative._has_revision_data(out) is False


def test_eps_trend_none_returns_none_safe_defaults():
    with _no_s3(), patch.dict(
        "sys.modules", {"yfinance": _yf_with_trend(None)}
    ):
        out = alternative._fetch_revisions("THIN", "bkt", "2026-05-18")
    assert out == {"current_estimate": None, "revision_4w": None, "streak": 0}


def test_missing_0y_row_returns_defaults():
    """Some names only expose quarterly rows — no 0y → defaults, no raise."""
    df = _eps_trend_df({
        "0q":  [1.0, 1.0, 1.0, 1.0, 1.0],
        "+1q": [1.1, 1.1, 1.1, 1.1, 1.1],
    })
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        out = alternative._fetch_revisions("QONLY", "bkt", "2026-05-18")
    assert out == {"current_estimate": None, "revision_4w": None, "streak": 0}


def test_yfinance_failure_degrades_loudly_without_raising():
    """yfinance raising must not bubble up — degraded defaults returned."""
    mod = MagicMock()
    mod.Ticker.side_effect = RuntimeError("yfinance IP block")
    with _no_s3(), patch.dict("sys.modules", {"yfinance": mod}):
        out = alternative._fetch_revisions("AAPL", "bkt", "2026-05-18")

    assert out["current_estimate"] is None
    assert out["revision_4w"] is None
    assert out["streak"] == 0


def test_signature_preserves_bucket_and_run_date_params():
    """bucket/run_date must remain accepted positional params (callers and
    the legacy S3 fallback are unaffected by the migration)."""
    df = _eps_trend_df({"0y": [5.0, 5.0, 5.0, 5.0, 5.0]})
    with _no_s3(), patch.dict("sys.modules", {"yfinance": _yf_with_trend(df)}):
        # explicit kwargs must still work
        out = alternative._fetch_revisions(
            "AAPL", bucket="some-bucket", run_date="2026-05-18"
        )
    assert out["current_estimate"] == 5.0
