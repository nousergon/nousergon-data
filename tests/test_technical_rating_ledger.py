"""Tests for collectors/technical_rating_ledger.py (metron-ops#297 part 2): the
immutable daily rating ledger, its self-seeding backfill, and the realized-performance
scorer. Covers the no-lookahead guarantee, an oracle test (score == forward return =>
IC = 1.0), a random-score test (|IC| small), and producer-contract validation against
rating_ledger_entry.schema.json / rating_performance.schema.json.

Refs nousergon/metron-ops#297.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collectors import technical_rating_ledger as trl
from contracts import validate_rating_ledger_entry, validate_rating_performance
from features.feature_engineer import RATING_VERSION, _rating_label, compute_technical_rating


# ── Fake S3 (in-memory, generic key store — the ledger touches many keys) ───────────


class _FakeS3:
    def __init__(self, initial: dict[str, dict] | None = None):
        self._store: dict[str, dict] = dict(initial or {})
        self.put_object = MagicMock(side_effect=self._put)

    def _put(self, Bucket, Key, Body, ContentType=None):
        self._store[Key] = json.loads(Body.decode() if isinstance(Body, bytes) else Body)

    def get_object(self, Bucket, Key):
        if Key not in self._store:
            raise Exception("NoSuchKey")
        body = MagicMock()
        body.read.return_value = json.dumps(self._store[Key]).encode()
        return {"Body": body}

    def put_count(self, prefix: str = "") -> int:
        return sum(1 for c in self.put_object.call_args_list if c.kwargs["Key"].startswith(prefix))

    def puts(self) -> dict[str, dict]:
        return dict(self._store)


def _session_dates(n: int, start_date: str = "2025-01-02") -> list[str]:
    from datetime import date, timedelta

    base = date.fromisoformat(start_date)
    # Business-day-spaced (Mon-Fri only) so a >252-session backfill window is plausible
    # over real calendar time, matching the module's own trading-calendar assumption.
    out = []
    d = base
    while len(out) < n:
        if d.weekday() < 5:
            out.append(str(d))
        d += timedelta(days=1)
    return out


def _linear_series(n: int, start: float = 50.0, step: float = 1.0, start_date: str = "2025-01-02") -> list[list]:
    dates = _session_dates(n, start_date)
    return [[dates[i], start + i * step] for i in range(n)]


def _consolidated(series: dict[str, list]) -> dict:
    return {
        "schema_version": 5, "adjustment_basis": "dividend_adjusted",
        "series": series, "currency": {sym: "USD" for sym in series},
    }


# ── _reference_calendar ───────────────────────────────────────────────────────────


class TestReferenceCalendar:
    def test_prefers_spy(self):
        series = {"SPY": _linear_series(5), "AAPL": _linear_series(3)}
        cal = trl._reference_calendar(series)
        assert cal == sorted(r[0] for r in series["SPY"])

    def test_falls_back_to_longest_series_when_no_spy(self):
        series = {"AAPL": _linear_series(3), "MSFT": _linear_series(10)}
        cal = trl._reference_calendar(series)
        assert cal == sorted(r[0] for r in series["MSFT"])

    def test_empty_series_returns_empty_calendar(self):
        assert trl._reference_calendar({}) == []


# ── No-lookahead: the rating for date D uses no close after D ───────────────────────


class TestNoLookahead:
    def test_rating_at_date_d_unaffected_by_closes_published_after_d(self):
        dates = _session_dates(280)
        d_index = 260  # a date well past every vote's min-obs
        d = dates[d_index]

        base_prices = [50.0 + i * 1.0 for i in range(280)]
        series_a = {"X": [[dates[i], base_prices[i]] for i in range(280)]}

        # A SECOND version where every close AFTER d is wildly different (a crash) --
        # if the rating at `d` used any of that, the two outputs would diverge.
        crashed_prices = list(base_prices)
        for i in range(d_index + 1, 280):
            crashed_prices[i] = 1.0
        series_b = {"X": [[dates[i], crashed_prices[i]] for i in range(280)]}

        out_a = trl._rate_universe_at_dates(series_a, [d])
        out_b = trl._rate_universe_at_dates(series_b, [d])
        assert out_a[d]["X"] == out_b[d]["X"]
        # Sanity: not a vacuous comparison -- a rating WAS actually computed.
        assert out_a[d]["X"]["score"] is not None

    def test_matches_direct_truncated_compute_technical_rating(self):
        dates = _session_dates(280)
        d = dates[200]
        rows = _linear_series(280)
        out = trl._rate_universe_at_dates({"X": rows}, [d])
        truncated_closes = [r[1] for r in rows if r[0] <= d]
        direct = compute_technical_rating(pd.Series(truncated_closes, dtype="float64"))
        assert out[d]["X"]["score"] == direct["score"]
        assert out[d]["X"]["label"] == direct["label"]
        assert out[d]["X"]["close"] == truncated_closes[-1]

    def test_two_pointer_pass_handles_multiple_ascending_target_dates(self):
        dates = _session_dates(280)
        rows = _linear_series(280)
        targets = [dates[100], dates[150], dates[279]]
        out = trl._rate_universe_at_dates({"X": rows}, targets)
        for d in targets:
            truncated = [r[1] for r in rows if r[0] <= d]
            direct = compute_technical_rating(pd.Series(truncated, dtype="float64"))
            assert out[d]["X"]["score"] == direct["score"]


# ── collect_rating_ledger ────────────────────────────────────────────────────────


class TestCollectRatingLedger:
    def test_no_close_history_skips(self):
        s3 = _FakeS3()
        result = trl.collect_rating_ledger(bucket="b", run_date="2026-06-26", s3_client=s3)
        assert result["status"] == "skipped"

    def test_self_seeding_backfill_writes_missing_dates_plus_live(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 5)
        dates = _session_dates(300)
        run_date = dates[-1]
        series = {"SPY": _linear_series(300), "AAPL": _linear_series(300, start=100.0)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})

        result = trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        assert result["status"] == "ok"
        # 5 candidate dates STRICTLY BEFORE run_date, all missing -> 5 backfill writes,
        # plus run_date itself always written "live" (never counted in the backfill
        # candidate window) -> 6 total.
        assert result["backfill_written"] == 5
        assert result["live_written"] is True
        assert result["total_dates"] == 6

        manifest = s3.puts()[trl.RATING_LEDGER_MANIFEST_KEY]
        assert len(manifest["dates"]) == 6
        bases = {e["date"]: e["basis"] for e in manifest["dates"]}
        assert bases[run_date] == "live"
        assert sum(1 for b in bases.values() if b == "backfill") == 5

        live_entry = s3.puts()[f"{trl.RATING_LEDGER_PREFIX}{run_date}.json"]
        assert validate_rating_ledger_entry(live_entry) == []
        assert live_entry["basis"] == "live"
        assert live_entry["rating_version"] == RATING_VERSION
        assert "AAPL" in live_entry["ratings"]

    def test_live_date_is_immutable_on_rerun(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 3)
        dates = _session_dates(300)
        run_date = dates[-1]
        series = {"SPY": _linear_series(300)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})
        trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        first_write_count = s3.put_count(f"{trl.RATING_LEDGER_PREFIX}{run_date}.json")
        assert first_write_count == 1

        result = trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        assert result["live_written"] is False
        assert result["backfill_written"] == 0
        # Never a second PUT to the same immutable date key.
        assert s3.put_count(f"{trl.RATING_LEDGER_PREFIX}{run_date}.json") == 1

    def test_live_write_skipped_when_run_date_has_no_close(self, monkeypatch):
        # A run_date past the last published bar would otherwise stamp the PRIOR
        # session's rating as an immutable "live" row for run_date.
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 3)
        dates = _session_dates(300)
        series = {"SPY": _linear_series(300)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})
        run_date = "2099-01-02"
        assert run_date > dates[-1]

        result = trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        assert result["live_written"] is False
        assert dates[-1] in result["live_skipped_reason"]
        assert f"{trl.RATING_LEDGER_PREFIX}{run_date}.json" not in s3.puts()
        assert result["backfill_written"] == 3

    def test_stale_backfill_rating_version_is_rewritten(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 3)
        dates = _session_dates(300)
        run_date = dates[-1]
        stale_date = dates[-2]
        series = {"SPY": _linear_series(300)}
        manifest = {
            "schema_version": 1,
            "dates": [
                {"date": stale_date, "basis": "backfill", "rating_version": 1},  # stale
                {"date": dates[-3], "basis": "backfill", "rating_version": RATING_VERSION},
            ],
        }
        s3 = _FakeS3({
            trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series),
            trl.RATING_LEDGER_MANIFEST_KEY: manifest,
            f"{trl.RATING_LEDGER_PREFIX}{stale_date}.json": {
                "schema_version": 1, "as_of": stale_date, "rating_version": 1,
                "basis": "backfill", "ratings": {},
            },
        })
        result = trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        assert result["status"] == "ok"
        rewritten = s3.puts()[f"{trl.RATING_LEDGER_PREFIX}{stale_date}.json"]
        assert rewritten["rating_version"] == RATING_VERSION
        assert rewritten["basis"] == "backfill"

    def test_dry_run_writes_nothing(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 5)
        dates = _session_dates(300)
        run_date = dates[-1]
        series = {"SPY": _linear_series(300)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})
        trl.collect_rating_ledger(bucket="b", run_date=run_date, dry_run=True, s3_client=s3)
        assert s3.put_object.call_count == 0


# ── compute_rating_performance_from_ledger (pure function) ─────────────────────────


def _score_for(r: float) -> float:
    return round(max(-1.0, min(1.0, r * 4)), 4)  # arbitrary monotone map, unused directly


class TestScorerOracle:
    def test_score_equals_forward_return_gives_ic_one(self):
        # Five symbols, one rating date, horizon=1: score IS the realized forward
        # return by construction -> per-date Spearman IC must be exactly 1.0.
        returns = [-0.20, -0.10, 0.0, 0.10, 0.20]
        syms = [f"S{i}" for i in range(5)]
        d0, d1 = "2026-01-02", "2026-01-05"
        series = {
            sym: [[d0, 100.0], [d1, 100.0 * (1 + r)]]
            for sym, r in zip(syms, returns)
        }
        ratings = {
            sym: {"score": r, "label": _rating_label(r), "close": 100.0}
            for sym, r in zip(syms, returns)
        }
        ledger_entries = {
            d0: {"schema_version": 1, "as_of": d0, "rating_version": RATING_VERSION,
                 "basis": "live", "ratings": ratings},
        }
        perf = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(1,), windows=(20,),
        )
        stats = perf["segments"]["all"]["20"]["1"]
        assert stats["ic_mean"] == pytest.approx(1.0, abs=1e-9)
        assert stats["ic_n_dates"] == 1

    def test_random_scores_give_small_ic(self):
        rng = random.Random(1234)
        dates = _session_dates(40)
        syms = [f"S{i}" for i in range(12)]
        # Independent random walk closes per symbol (so forward returns are unrelated
        # to the ARBITRARY random score assigned per symbol-date below).
        series = {}
        for sym in syms:
            price = 100.0
            rows = []
            for d in dates:
                price *= 1 + rng.uniform(-0.02, 0.02)
                rows.append([d, price])
            series[sym] = rows

        ledger_entries = {}
        for d in dates[:-5]:  # leave room for a realized horizon=5
            ratings = {}
            for sym in syms:
                score = rng.uniform(-1, 1)
                ratings[sym] = {"score": round(score, 4), "label": _rating_label(score), "close": 100.0}
            ledger_entries[d] = {
                "schema_version": 1, "as_of": d, "rating_version": RATING_VERSION,
                "basis": "live", "ratings": ratings,
            }

        perf = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(5,), windows=(60,),
        )
        stats = perf["segments"]["all"]["60"]["5"]
        assert stats["ic_mean"] is not None
        assert abs(stats["ic_mean"]) < 0.4


class TestScorerSegmentsAndWindows:
    def _fixture(self, n_dates: int = 30):
        dates = _session_dates(n_dates + 5)
        syms = [f"S{i}" for i in range(4)]
        series = {sym: [[d, 100.0 + i] for i, d in enumerate(dates)] for sym in syms}
        ledger_entries = {}
        for i, d in enumerate(dates[:n_dates]):
            basis = "backfill" if i % 2 == 0 else "live"
            ratings = {
                sym: {"score": 0.6, "label": "Strong Buy", "close": 100.0 + i}
                for sym in syms
            }
            ledger_entries[d] = {
                "schema_version": 1, "as_of": d, "rating_version": RATING_VERSION,
                "basis": basis, "ratings": ratings,
            }
        return ledger_entries, series

    def test_segment_filters_by_basis(self):
        # 30 dates alternating live/backfill (15 each), 4 symbols/date. Each segment's
        # `window` trims independently over ITS OWN date set, not a shared one, so
        # "all" (30 dates, capped at window=20 -> 20 dates x4=80) is NOT required to
        # equal live+backfill (15 dates each, under the 20 cap -> 15x4=60 apiece).
        ledger_entries, series = self._fixture()
        perf = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(1,), windows=(20,),
        )
        all_n = perf["segments"]["all"]["20"]["1"]["buckets"]["Strong Buy"]["n"]
        live_n = perf["segments"]["live"]["20"]["1"]["buckets"]["Strong Buy"]["n"]
        backfill_n = perf["segments"]["backfill"]["20"]["1"]["buckets"]["Strong Buy"]["n"]
        assert live_n == 60 and backfill_n == 60  # all 15 live / 15 backfill dates x 4 syms
        assert all_n == 80  # last 20 of 30 combined dates x 4 syms
        assert all_n != live_n + backfill_n  # segments window independently, not additive

    def test_window_trims_to_trailing_n_dates(self):
        ledger_entries, series = self._fixture(n_dates=30)
        perf_small = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(1,), windows=(5,),
        )
        perf_large = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(1,), windows=(20,),
        )
        n_small = perf_small["segments"]["all"]["5"]["1"]["buckets"]["Strong Buy"]["n"]
        n_large = perf_large["segments"]["all"]["20"]["1"]["buckets"]["Strong Buy"]["n"]
        assert n_small < n_large

    def test_all_five_label_buckets_present_even_at_zero_n(self):
        ledger_entries, series = self._fixture()
        perf = trl.compute_rating_performance_from_ledger(
            ledger_entries, series, horizons=(1,), windows=(20,),
        )
        buckets = perf["segments"]["all"]["20"]["1"]["buckets"]
        assert set(buckets) == {"Strong Sell", "Sell", "Neutral", "Buy", "Strong Buy"}
        for label in ("Strong Sell", "Sell", "Neutral", "Buy"):
            assert buckets[label] == {"n": 0, "mean_fwd": None, "hit_rate": None, "mean_excess": None}


class TestScorerContract:
    def test_computed_output_validates(self):
        ledger_entries, series = TestScorerSegmentsAndWindows()._fixture()
        perf = trl.compute_rating_performance_from_ledger(ledger_entries, series)
        assert validate_rating_performance(perf) == []

    def test_minimal_hand_built_fixture_validates(self):
        empty_bucket = {"n": 0, "mean_fwd": None, "hit_rate": None, "mean_excess": None}
        buckets = {label: dict(empty_bucket) for label in
                   ("Strong Sell", "Sell", "Neutral", "Buy", "Strong Buy")}
        horizon_stats = {
            "buckets": buckets, "spread_strong_buy_minus_strong_sell": None,
            "ic_mean": None, "ic_n_dates": 0, "noise_floor_ic": None,
        }
        art = {
            "schema_version": 1, "as_of_utc": "2026-06-26T00:00:00Z",
            "rating_version": RATING_VERSION, "horizons": [1, 5, 20], "windows": [20, 60, 250],
            "segments": {
                seg: {str(w): {str(h): horizon_stats for h in (1, 5, 20)} for w in (20, 60, 250)}
                for seg in ("live", "backfill", "all")
            },
            "ic_series": [],
        }
        assert validate_rating_performance(art) == []

    def test_wrong_label_key_fails(self):
        art = {
            "schema_version": 1, "as_of_utc": "2026-06-26T00:00:00Z",
            "rating_version": RATING_VERSION, "horizons": [1], "windows": [20],
            "segments": {"live": {"20": {"1": {
                "buckets": {"Very Bullish": {"n": 0, "mean_fwd": None, "hit_rate": None, "mean_excess": None}},
                "spread_strong_buy_minus_strong_sell": None, "ic_mean": None,
                "ic_n_dates": 0, "noise_floor_ic": None,
            }}}},
            "ic_series": [],
        }
        assert validate_rating_performance(art) != []


# ── collect_rating_performance (S3 wrapper) ──────────────────────────────────────


class TestCollectRatingPerformance:
    def test_empty_ledger_skips(self):
        s3 = _FakeS3()
        result = trl.collect_rating_performance(bucket="b", s3_client=s3)
        assert result["status"] == "skipped"

    def test_writes_performance_artifact_after_ledger_populated(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 5)
        dates = _session_dates(300)
        run_date = dates[-1]
        series = {"SPY": _linear_series(300), "AAPL": _linear_series(300, start=100.0)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})
        trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)

        result = trl.collect_rating_performance(bucket="b", s3_client=s3)
        assert result["status"] == "ok"
        art = s3.puts()[trl.RATING_PERFORMANCE_KEY]
        assert validate_rating_performance(art) == []
        assert art["rating_version"] == RATING_VERSION

    def test_dry_run_does_not_write(self, monkeypatch):
        monkeypatch.setattr(trl, "LEDGER_BACKFILL_MIN_DATES", 5)
        dates = _session_dates(300)
        run_date = dates[-1]
        series = {"SPY": _linear_series(300)}
        s3 = _FakeS3({trl.mmd.CONSOLIDATED_CLOSE_HISTORY_KEY: _consolidated(series)})
        trl.collect_rating_ledger(bucket="b", run_date=run_date, s3_client=s3)
        s3.put_object.reset_mock()

        result = trl.collect_rating_performance(bucket="b", dry_run=True, s3_client=s3)
        assert result["status"] == "ok_dry_run"
        assert s3.put_object.call_count == 0
