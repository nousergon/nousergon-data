"""2026-07-02 inverted-feed-record incident class — regression coverage.

The incident (Honeywell separation + DuPont split, 2026-06/07):

  * polygon published BOTH mega-cap June 2026 split records with the
    ``split_from``/``split_to`` ratio INVERTED (HON's forward 1:2 as ``2:1``
    — a record it later deleted upstream — and DD's forward 1:3 as ``3:1``),
    while its adjusted aggregates restated the histories in the true
    (dividing) direction;
  * ``apply()`` trusted the stated orientation, and — corroborated by a
    prior splice-corruption boundary — multiplied HON's full pre-ex history
    ×2 (DD ×3) in the ArcticDB training store;
  * ``_ensure_history_restated`` / ``_sync_arcticdb_universe`` then marked
    even REFUSED actions as applied, permanently freezing the corruption
    behind the exactly-once marker;
  * fractional polygon ratio fields (CCBC ``1:1.2``, NRWRF
    ``20.625:21.625``) were silently int-truncated on ingest;
  * the discrepancy classifier consulted only session-detected actions and
    only the stated orientation, so the (registered!) event still paged six
    per-date ERROR emails when later windows re-touched restated dates.

Each test here pins the corrected behavior for one of those legs.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry


class _FakeS3:
    """Minimal in-memory S3 double for seeding a CorporateActionRegistry."""

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


def _registry() -> CorporateActionRegistry:
    return CorporateActionRegistry(_FakeS3(), "test-bucket")


def _series(values_by_date: dict[str, float]) -> pd.Series:
    idx = pd.to_datetime(list(values_by_date))
    return pd.Series(list(values_by_date.values()), index=idx).sort_index()


def _frame(values_by_date: dict[str, float]) -> pd.DataFrame:
    close = _series(values_by_date)
    return pd.DataFrame(
        {
            "Open": close.values,
            "High": close.values,
            "Low": close.values,
            "Close": close.values,
            "Volume": 1_000_000.0,
            "VWAP": close.values,
        },
        index=close.index,
    )


def _dd_like_frame() -> pd.DataFrame:
    """A clean pre-split series with the genuine forward-split drop at the ex
    date — what the raw market actually printed for DD's 1:3 on 2026-06-24."""
    dates = pd.bdate_range("2026-05-15", "2026-06-30")
    vals = np.where(dates < pd.Timestamp("2026-06-24"), 420.0, 140.0)
    return pd.DataFrame(
        {
            "Open": vals, "High": vals, "Low": vals, "Close": vals,
            "Volume": 1_000_000.0, "VWAP": vals,
        },
        index=dates,
    )


class TestOrientationClassification:
    def test_inverted_record_classified_inverse(self):
        # Feed says 3:1 (reverse, price should TRIPLE); market halved-by-3.
        inverted = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
        df = _dd_like_frame()
        assert ca.price_evidence_orientation(df["Close"], inverted) == "inverse"

    def test_correct_record_classified_direct(self):
        correct = ca.CorporateAction.from_split("DD", "2026-06-24", 1, 3)
        df = _dd_like_frame()
        assert ca.price_evidence_orientation(df["Close"], correct) == "direct"

    def test_splice_corrupted_series_classified_ambiguous(self):
        # The HON 2026-07-01 shape: clean new-basis series EXCEPT one raw
        # old-basis spliced row right before the ex date — the splice prints
        # the stated-direction ratio while the real move prints the inverse.
        vals = {
            "2026-06-24": 227.42,
            "2026-06-25": 231.24,
            "2026-06-26": 464.42,  # old-basis splice row
            "2026-06-29": 227.80,
            "2026-06-30": 223.90,
        }
        inverted = ca.CorporateAction.from_split("HON", "2026-06-29", 2, 1)
        assert (
            ca.price_evidence_orientation(_series(vals), inverted) == "ambiguous"
        )

    def test_near_one_factor_keeps_legacy_direct_semantics(self):
        # 1000:1061 spinoff-style record: direction is not discriminable from
        # daily noise — never returns inverse/ambiguous.
        action = ca.CorporateAction.from_split("HON", "2025-10-30", 1000, 1061)
        vals = {d.strftime("%Y-%m-%d"): 100.0 for d in pd.bdate_range("2025-10-20", "2025-11-05")}
        orientation = ca.price_evidence_orientation(_series(vals), action)
        assert orientation in ("direct", "none")


class TestApplyOrientation:
    _STORE = ca.STORE_ARCTICDB_UNIVERSE

    def test_inverted_record_applies_market_corrected_factor(self):
        inverted = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
        df = _dd_like_frame()
        out, results = ca.apply(df, [inverted], store=self._STORE, registry=None)
        (res,) = results
        assert res["status"] == "applied"
        assert res["orientation_corrected"] is True
        assert res["factor"] == pytest.approx(1 / 3)
        # Pre-ex history DIVIDED (onto the post-split scale), never multiplied.
        assert out.loc["2026-06-23", "Close"] == pytest.approx(140.0)
        assert out.loc["2026-06-24", "Close"] == pytest.approx(140.0)
        # Boundary flattened — no residual split-like jump.
        assert out["Close"].pct_change().abs().max() < 0.33

    def test_ambiguous_evidence_refuses_and_never_marks(self):
        reg = _registry()
        vals = {
            "2026-06-24": 227.42,
            "2026-06-25": 231.24,
            "2026-06-26": 464.42,
            "2026-06-29": 227.80,
            "2026-06-30": 223.90,
        }
        inverted = ca.CorporateAction.from_split("HON", "2026-06-29", 2, 1)
        df = _frame(vals)
        out, results = ca.apply(df, [inverted], store=self._STORE, registry=reg)
        (res,) = results
        assert res["status"] == "ambiguous_evidence"
        assert res["n_rows_adjusted"] == 0
        assert reg.is_applied(self._STORE, inverted.action_id) is False
        # Frame untouched.
        assert out.loc["2026-06-25", "Close"] == pytest.approx(231.24)


class TestFractionalRatios:
    def test_fractional_split_factor_survives_construction(self):
        action = ca.CorporateAction.from_split("CCBC", "2026-06-18", 1, 1.2)
        assert action.split_to == pytest.approx(1.2)
        assert ca.expected_factor(action) == pytest.approx(1 / 1.2)

    def test_integral_float_normalizes_to_int_for_id_stability(self):
        a_int = ca.CorporateAction.from_split("X", "2026-01-02", 1, 2)
        a_float = ca.CorporateAction.from_split("X", "2026-01-02", 1.0, 2.0)
        assert a_int.action_id == a_float.action_id
        assert isinstance(a_float.split_from, int)

    def test_malformed_ratio_fails_loud(self):
        with pytest.raises(ValueError):
            ca.CorporateAction.from_split("X", "2026-01-02", 0, 2)
        with pytest.raises(ValueError):
            ca.CorporateAction.from_split("X", "2026-01-02", "abc", 2)


class TestExplainsDiscrepancyUpgrades:
    def test_inverse_ratio_of_registered_action_explains(self):
        # DD's registry record is the upstream-inverted 3:1; the overwrite the
        # window observes is the TRUE restatement ratio ~1/3. Must classify as
        # that action's restatement (WARN), not page an unexplained ERROR.
        reg = _registry()
        inverted = ca.CorporateAction.from_split("DD", "2026-06-24", 3, 1)
        reg.record_detected(inverted, run_id="r")
        action = reg.explains_discrepancy("DD", "2026-06-20", 420.03, 140.01)
        assert action is not None
        assert action.action_id == inverted.action_id

    def test_persisted_actions_consulted_without_session_detection(self):
        # A FRESH registry instance over the same S3 (new session, no
        # record_detected calls) must still explain via the persisted record —
        # the 2026-07-02 six-email storm was session-scope-only matching.
        s3 = _FakeS3()
        first = CorporateActionRegistry(s3, "test-bucket")
        recorded = ca.CorporateAction.from_split("HON", "2026-06-29", 1, 2)
        first.record_detected(recorded, run_id="r1")

        fresh = CorporateActionRegistry(s3, "test-bucket")
        action = fresh.explains_discrepancy("HON", "2026-06-26", 464.42, 232.21)
        assert action is not None
        assert action.action_id == recorded.action_id


class TestMarkOnlyAppliedDiscipline:
    def test_sync_arcticdb_universe_does_not_mark_refused_action(self, monkeypatch):
        # A phantom action (no market evidence in the symbol's series) must
        # stay UNMARKED after the sync leg — marking it froze HON/DD's
        # un-restated histories behind the exactly-once contract on 2026-07-01.
        reg = _registry()
        phantom = ca.CorporateAction.from_split("HON", "2026-06-29", 1, 2)

        flat = _frame(
            {d.strftime("%Y-%m-%d"): 100.0
             for d in pd.bdate_range("2026-06-20", "2026-07-01")}
        )

        class _FakeLib:
            def read(self, _symbol):
                class _R:
                    data = flat
                return _R()

            def write(self, *a, **k):
                raise AssertionError("refused action must not trigger a write")

        monkeypatch.setattr(
            "store.arctic_store.get_universe_lib", lambda bucket: _FakeLib()
        )
        monkeypatch.setattr(
            "store.arctic_store.to_arctic_canonical", lambda df: df
        )
        results = ca._sync_arcticdb_universe(
            "test-bucket", "HON", [phantom], reg, run_id="r"
        )
        (res,) = results
        assert res["status"] == "unconfirmed"
        assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, phantom.action_id) is False
