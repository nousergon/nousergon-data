"""Splice basis-guard on daily_append's incoming row (2026-07-02 incident).

Two verified live corruptions motivated the guard:

  * HON 2026-06-26 — a raw old-basis parquet row (464.42) overwrote the
    already-stored new-basis row (232.21) on a series whose neighbors were
    ~231: a same-date, same-market split-like disagreement that is never a
    price move. That planted the boundary which later corroborated polygon's
    inverted 2:1 record into a full-history ×2.
  * CRWD 2026-06-30 — the incoming row arrived on the PRE-split basis
    (763.14) while the stored history was already ×0.25-restated for the
    registered 1:4 (ex 2026-07-02): deterministic gap of exactly 1/factor,
    healed by restating the row, never by splicing it raw.

Calibration constraint (2026 universe scan): CAR −48% and KD −55% are
GENUINE single-day moves that overlap the 2:1 ratio zone, so a pure append
with an unexplained split-like ratio must WRITE (with an ERROR page), not
refuse — magnitude alone cannot refuse.
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry
from builders.daily_append import _splice_basis_guard


class _FakeS3:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def head_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"ETag": '"fake"'}

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


def _hist(values_by_date: dict[str, float]) -> pd.DataFrame:
    idx = pd.to_datetime(list(values_by_date))
    vals = list(values_by_date.values())
    return pd.DataFrame({"Close": vals}, index=idx).sort_index()


def _bar(close: float, volume: float = 1_000_000.0) -> pd.Series:
    return pd.Series(
        {"Open": close, "High": close, "Low": close, "Close": close,
         "Volume": volume, "VWAP": close}
    )


class TestSpliceBasisGuard:
    def test_normal_move_passes_untouched(self):
        hist = _hist({"2026-06-26": 100.0, "2026-06-29": 101.0})
        bar = _bar(102.5)
        out, verdict = _splice_basis_guard(
            "X", bar, hist, pd.Timestamp("2026-06-30"), [], None
        )
        assert verdict == "ok"
        assert out is bar

    def test_pre_action_basis_row_restated_by_applied_future_ex_action(self):
        # CRWD case: stored history already ×0.25-restated for the registered
        # 1:4 (ex 2026-07-02); incoming 2026-06-30 row arrives pre-split.
        reg = CorporateActionRegistry(_FakeS3(), "b")
        action = ca.CorporateAction.from_split("CRWD", "2026-07-02", 1, 4)
        reg.record_detected(action, run_id="r")
        reg.mark_applied(action, ca.STORE_ARCTICDB_UNIVERSE, run_id="r")

        hist = _hist({"2026-06-26": 175.27, "2026-06-29": 185.73})
        bar = _bar(763.14, volume=3_687_971)
        out, verdict = _splice_basis_guard(
            "CRWD", bar, hist, pd.Timestamp("2026-06-30"), [action], reg
        )
        assert verdict == "restated"
        assert float(out["Close"]) == pytest.approx(763.14 * 0.25)
        assert float(out["Volume"]) == pytest.approx(3_687_971 / 0.25, rel=1e-6)

    def test_feed_leads_pre_adjusted_row_restated_onto_the_store_basis(self):
        # IESC 2026-08-21 (alpha-engine-config-I8374): polygon retro-adjusted
        # its history for the registered 1:2 (ex 2026-08-24) while ArcticDB was
        # still on the pre-action basis. The incoming row then sits at
        # ``factor`` BELOW the store — the MIRROR of the CRWD case — and
        # ``is_applied`` is False BY DEFINITION, because the split not yet
        # being applied is exactly why the store is stale. Both facts made this
        # orientation structurally unreachable before I8374.
        reg = CorporateActionRegistry(_FakeS3(), "b")
        action = ca.CorporateAction.from_split("IESC", "2026-08-24", 1, 2)
        reg.record_detected(action, run_id="r")  # detected, NOT applied

        assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, action.action_id) is False

        hist = _hist({"2026-08-19": 697.38, "2026-08-20": 683.27})
        bar = _bar(342.52, volume=1_400_000)
        out, verdict = _splice_basis_guard(
            "IESC", bar, hist, pd.Timestamp("2026-08-21"), [action], reg
        )
        assert verdict == "restated"
        # Restated ONTO THE STORE'S basis (×1/factor = ×2), not onto the feed's.
        assert float(out["Close"]) == pytest.approx(342.52 * 2.0)
        assert float(out["Volume"]) == pytest.approx(1_400_000 / 2.0, rel=1e-6)

    def test_restatement_log_names_the_orientation(self, caplog):
        reg = CorporateActionRegistry(_FakeS3(), "b")
        action = ca.CorporateAction.from_split("IESC", "2026-08-24", 1, 2)
        reg.record_detected(action, run_id="r")
        hist = _hist({"2026-08-19": 697.38, "2026-08-20": 683.27})
        with caplog.at_level(logging.WARNING):
            _out, verdict = _splice_basis_guard(
                "IESC", _bar(342.52), hist, pd.Timestamp("2026-08-21"),
                [action], reg,
            )
        assert verdict == "restated"
        assert any("orientation=feed-leads" in r.message for r in caplog.records)

        caplog.clear()
        reg2 = CorporateActionRegistry(_FakeS3(), "b")
        crwd = ca.CorporateAction.from_split("CRWD", "2026-07-02", 1, 4)
        reg2.record_detected(crwd, run_id="r")
        reg2.mark_applied(crwd, ca.STORE_ARCTICDB_UNIVERSE, run_id="r")
        hist2 = _hist({"2026-06-26": 175.27, "2026-06-29": 185.73})
        with caplog.at_level(logging.WARNING):
            _out, verdict = _splice_basis_guard(
                "CRWD", _bar(763.14), hist2, pd.Timestamp("2026-06-30"),
                [crwd], reg2,
            )
        assert verdict == "restated"
        assert any("orientation=store-leads" in r.message for r in caplog.records)

    def test_unapplied_future_ex_action_does_not_restate(self):
        # The store is NOT yet on the post-action scale — a 1/factor gap is
        # then expected pre-basis continuity, not a mismatch to correct.
        reg = CorporateActionRegistry(_FakeS3(), "b")
        action = ca.CorporateAction.from_split("CRWD", "2026-07-02", 1, 4)
        reg.record_detected(action, run_id="r")  # detected, NOT applied

        hist = _hist({"2026-06-26": 701.09, "2026-06-29": 742.91})
        bar = _bar(763.14)
        out, verdict = _splice_basis_guard(
            "CRWD", bar, hist, pd.Timestamp("2026-06-30"), [action], reg
        )
        assert verdict == "ok"
        assert float(out["Close"]) == pytest.approx(763.14)

    def test_same_date_split_like_disagreement_refused(self, caplog):
        # HON case: stored 2026-06-26 row (232.21) coheres with its neighbor
        # (231.24); incoming claims 464.42 for the SAME date — basis mismatch.
        hist = _hist({"2026-06-25": 231.24, "2026-06-26": 232.21})
        bar = _bar(464.42)
        with caplog.at_level(logging.ERROR):
            _out, verdict = _splice_basis_guard(
                "HON", bar, hist, pd.Timestamp("2026-06-26"), [], None
            )
        assert verdict == "refused"
        assert any("REFUSING HON" in r.message for r in caplog.records)

    def test_same_date_refusal_operator_allowlist_overrides(self, monkeypatch):
        hist = _hist({"2026-06-25": 231.24, "2026-06-26": 232.21})
        bar = _bar(464.42)
        monkeypatch.setenv(
            "DAILY_APPEND_SPLICE_GUARD_ALLOW", "HON:2026-06-26"
        )
        _out, verdict = _splice_basis_guard(
            "HON", bar, hist, pd.Timestamp("2026-06-26"), [], None
        )
        assert verdict == "ok"

    def test_pure_append_genuine_crash_writes_with_error_page(self, caplog):
        # KD −55% (2026-02-09, verified genuine): no stored same-date row, no
        # registered action — must WRITE the row, paging at ERROR severity.
        hist = _hist({"2026-02-05": 22.07, "2026-02-06": 23.49})
        bar = _bar(10.59)
        with caplog.at_level(logging.ERROR):
            out, verdict = _splice_basis_guard(
                "KD", bar, hist, pd.Timestamp("2026-02-09"), [], None
            )
        assert verdict == "unexplained"  # written, and held (I13044)
        assert float(out["Close"]) == pytest.approx(10.59)
        assert any("NO registered corporate action" in r.message for r in caplog.records)

    def test_pure_append_at_a_registered_ex_date_does_not_page(self, caplog):
        # IESC 2026-08-24 (alpha-engine-config-I8374): the ex date itself. The
        # compared closes are on DIFFERENT trading days (324.12 vs the 08-21
        # close 685.04 = 0.4731 = the 0.5 factor × a genuine 5.4% interday
        # move), so an EXACT-factor match cannot fire here — only ex-date
        # proximity can. The old code asserted "NO registered corporate action
        # explains it" while holding this very action.
        reg = CorporateActionRegistry(_FakeS3(), "b")
        action = ca.CorporateAction.from_split("IESC", "2026-08-24", 1, 2)
        reg.record_detected(action, run_id="r")

        hist = _hist({"2026-08-20": 683.27, "2026-08-21": 685.04})
        with caplog.at_level(logging.WARNING):
            out, verdict = _splice_basis_guard(
                "IESC", _bar(324.12), hist, pd.Timestamp("2026-08-24"), [], reg
            )
        assert verdict == "ok"
        assert float(out["Close"]) == pytest.approx(324.12)
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert any(
            "REGISTERED corporate action" in r.message
            and action.action_id in r.message
            for r in caplog.records
        )

    def test_pure_append_page_states_the_scope_it_evaluated(self, caplog):
        # A genuinely unexplained split-like append must still page — and the
        # message must name what it actually asked (MRNA 2026-08-19: the
        # registry holds no MRNA action at all, so the claim was true there).
        reg = CorporateActionRegistry(_FakeS3(), "b")
        hist = _hist({"2026-08-18": 61.10, "2026-08-19": 62.96})
        with caplog.at_level(logging.ERROR):
            out, verdict = _splice_basis_guard(
                "MRNA", _bar(174.38), hist, pd.Timestamp("2026-08-20"), [], reg
            )
        assert verdict == "unexplained"  # written, and held (I13044)
        assert float(out["Close"]) == pytest.approx(174.38)
        errs = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errs) == 1
        assert "NO registered corporate action explains it" in errs[0]
        assert "evaluated: persisted registry + session-detected splits" in errs[0]

    def test_pure_append_page_admits_a_narrowed_scope_without_a_registry(self, caplog):
        hist = _hist({"2026-02-05": 22.07, "2026-02-06": 23.49})
        with caplog.at_level(logging.ERROR):
            _out, verdict = _splice_basis_guard(
                "KD", _bar(10.59), hist, pd.Timestamp("2026-02-09"), [], None
            )
        assert verdict == "unexplained"  # written, and held (I13044)
        errs = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
        assert any("session-detected splits only (no registry available)" in m for m in errs)

    def test_missing_prior_close_fails_open(self):
        out, verdict = _splice_basis_guard(
            "NEW", _bar(50.0), pd.DataFrame(), pd.Timestamp("2026-06-30"), [], None
        )
        assert verdict == "ok"
