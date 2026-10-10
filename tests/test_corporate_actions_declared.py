"""Declared corporate actions: the CTVA -> VYLR spin-off (alpha-engine-config-I11806).

Every number below is CTVA's real stored history: the 2026-09-23..10-02 rows of
``s3://alpha-engine-research/reference/price_cache/CTVA.parquet`` (yfinance,
written 2026-10-02 22:20Z) and VYLR's first close in
``staging/daily_closes/2026-10-01.parquet``. The tests pin:

  * the factor is the CRSP relative-value factor derived from the declared
    inputs (0.1555), not the formula that fabricates a +33.9% day;
  * restating the raw history leaves no false -84% day at the ex-date, scales
    prices only (volume untouched) and leaves ex-date-onward rows alone;
  * the evidence gate: a restated or vendor-adjusted frame is a no-op, a
    mixed-basis frame is refused, a frame that does not span the ex-date is
    left alone, and a vendor record for the same event supersedes the
    declaration;
  * the backfill/feature delta merge, the training-write audit and the morning
    ArcticDB sync all see the declared action.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
import features.compute as compute
from corporate_actions import CorporateActionRegistry
from corporate_actions.declared import (
    DECLARED_SPINOFFS,
    crsp_spinoff_factor,
    declared_actions,
    merge_declared,
)

_EX = "2026-10-01"
# CTVA, reference/price_cache/CTVA.parquet as stored 2026-10-02 22:20Z.
_CTVA_ROWS = [
    # date,        open,      high,      low,       close,     volume
    ("2026-09-23", 79.830002, 82.260002, 79.610001, 80.419998, 5550000.0),
    ("2026-09-24", 81.525002, 81.525002, 79.055000, 79.449997, 3498100.0),
    ("2026-09-25", 78.970001, 78.980003, 77.330002, 78.519997, 4163200.0),
    ("2026-09-28", 78.410004, 78.889999, 77.660004, 77.739998, 3248500.0),
    ("2026-09-29", 77.000000, 78.470001, 76.500000, 77.870003, 3633700.0),
    ("2026-09-30", 77.910004, 78.794998, 77.279999, 77.650002, 4034800.0),
    ("2026-10-01", 14.440000, 14.440000, 11.840000, 12.570000, 88375500.0),
    ("2026-10-02", 12.385000, 13.039500, 11.870000, 11.920000, 78665510.0),
]
_VYLR_FIRST_CLOSE = 68.26


def _ctva_raw(rows=_CTVA_ROWS) -> pd.DataFrame:
    idx = pd.DatetimeIndex([r[0] for r in rows])
    return pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        index=idx,
    )


def _ctva_action():
    (action,) = [a for a in declared_actions() if a.ticker == "CTVA"]
    return action


class _FakeS3:
    def __init__(self):
        self.store: dict[str, bytes] = {}

    def _err(self, code, op):
        return ClientError({"Error": {"Code": code, "Message": "x"}}, op)

    def head_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._err("404", "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise self._err("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"ETag": '"x"'}

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


# ── the factor ──────────────────────────────────────────────────────────────


def test_ctva_factor_is_the_crsp_relative_value_factor():
    action = _ctva_action()
    assert action.type == "spinoff"
    assert action.ex_date == _EX
    assert action.spun_ticker == "VYLR"
    assert action.source == "declared"
    assert ca.expected_factor(action) == pytest.approx(12.57 / (12.57 + 68.26))
    assert round(ca.expected_factor(action), 4) == 0.1555


def test_the_rejected_formula_would_fabricate_a_34pct_day():
    """Why the factor is NOT (P_prev - D) / P_prev: it charges the whole ex-date
    move to the small remaining parent."""
    p_prev = 77.65
    rejected = (p_prev - _VYLR_FIRST_CLOSE) / p_prev
    assert rejected == pytest.approx(0.1209, abs=1e-4)
    assert 12.57 / (p_prev * rejected) - 1 == pytest.approx(0.339, abs=1e-3)
    crsp = crsp_spinoff_factor(12.57, _VYLR_FIRST_CLOSE, 1.0)
    assert 12.57 / (p_prev * crsp) - 1 == pytest.approx(0.041, abs=1e-3)


@pytest.mark.parametrize("bad", [(0, 68.26, 1), (12.57, -1, 1), (12.57, 68.26, 0)])
def test_factor_inputs_must_be_positive(bad):
    with pytest.raises(ValueError):
        crsp_spinoff_factor(*bad)


@pytest.mark.parametrize("bad", [0.0, 1.0, 1.5, -0.2])
def test_spinoff_factor_must_be_strictly_between_zero_and_one(bad):
    with pytest.raises(ValueError):
        ca.CorporateAction.from_spinoff("CTVA", _EX, bad)


def test_spinoff_round_trips_and_has_a_stable_id():
    action = _ctva_action()
    again = ca.CorporateAction.from_dict(action.to_dict())
    assert again == action
    assert again.action_id == ca.CorporateAction.from_spinoff(
        "CTVA", _EX, action.price_factor, spun_ticker="VYLR",
    ).action_id
    assert "VYLR" in action.human()


def test_every_declaration_cites_sources_and_derives_a_factor():
    for d in DECLARED_SPINOFFS:
        assert d.sources, d.ticker
        assert 0.0 < d.price_factor < 1.0, d.ticker


# ── apply_declared on CTVA's real history ───────────────────────────────────


def test_restated_ctva_has_no_false_84pct_drop():
    raw = _ctva_raw()
    assert raw["Close"].pct_change().min() == pytest.approx(-0.838, abs=1e-3)

    out, res = ca.apply_declared(raw, "CTVA")

    assert [r["status"] for r in res] == ["applied"]
    assert res[0]["n_rows_adjusted"] == 6
    rets = out["Close"].pct_change().dropna()
    assert rets.loc[_EX] == pytest.approx(80.83 / 77.65 - 1, abs=1e-3)  # +4.1%
    # Nothing left above the training audit's 18% screen.
    assert rets.abs().max() < compute._ACTION_JUMP_SCREEN_THRESHOLD
    factor = _ctva_action().price_factor
    pre = raw.index < pd.Timestamp(_EX)
    for col in ("Open", "High", "Low", "Close"):
        np.testing.assert_allclose(out.loc[pre, col], raw.loc[pre, col] * factor)
        np.testing.assert_allclose(out.loc[~pre, col], raw.loc[~pre, col])
    # A spin-off does not change the parent's share count: volume untouched.
    np.testing.assert_allclose(out["Volume"], raw["Volume"])
    # The input frame is never mutated.
    assert raw.loc["2026-09-30", "Close"] == pytest.approx(77.650002)


def test_second_pass_is_a_noop():
    once, _ = ca.apply_declared(_ctva_raw(), "CTVA")
    twice, res = ca.apply_declared(once, "CTVA")
    assert [r["status"] for r in res] == ["already_reflected"]
    pd.testing.assert_frame_equal(once, twice)


def test_a_vendor_adjusted_history_is_not_adjusted_again():
    """If yfinance/polygon later adjust the history themselves (with their own,
    slightly different factor), the boundary is already continuous: no-op."""
    raw = _ctva_raw()
    vendor = raw.copy()
    pre = vendor.index < pd.Timestamp(_EX)
    vendor.loc[pre, ["Open", "High", "Low", "Close"]] *= 0.16
    out, res = ca.apply_declared(vendor, "CTVA")
    assert [r["status"] for r in res] == ["already_reflected"]
    pd.testing.assert_frame_equal(out, vendor)


def test_a_mixed_basis_frame_is_refused_not_restated(caplog):
    """Restated 09-23..09-29 followed by a raw 09-30: restating would
    double-adjust the restated rows, so the frame is left alone, loudly."""
    restated, _ = ca.apply_declared(_ctva_raw(), "CTVA")
    mixed = restated.copy()
    mixed.loc["2026-09-30", ["Open", "High", "Low", "Close"]] = [
        77.910004, 78.794998, 77.279999, 77.650002,
    ]
    out, res = ca.apply_declared(mixed, "CTVA")
    assert [r["status"] for r in res] == ["refused"]
    pd.testing.assert_frame_equal(out, mixed)
    assert any(rec.levelname == "ERROR" for rec in caplog.records)


def test_a_frame_that_does_not_span_the_ex_date_is_left_alone():
    raw = _ctva_raw()
    for part in (raw.iloc[:6], raw.iloc[6:]):
        out, res = ca.apply_declared(part, "CTVA")
        assert res[0]["status"] in ("uncovered", "already_reflected")
        pd.testing.assert_frame_equal(out, part)


def test_other_tickers_are_untouched():
    raw = _ctva_raw()
    out, res = ca.apply_declared(raw, "AAPL")
    assert res == []
    assert out is raw


def test_a_vendor_record_for_the_same_event_supersedes_the_declaration():
    vendor = ca.CorporateAction.from_split("CTVA", _EX, 1257, 8083)
    assert merge_declared([vendor]) == []
    other = ca.CorporateAction.from_split("CTVA", "2026-11-02", 1, 2)
    assert merge_declared([other]) == [_ctva_action()]


def test_registry_apply_refuses_a_spinoff():
    with pytest.raises(ValueError, match="apply_declared"):
        ca.apply(_ctva_raw(), [_ctva_action()], store=ca.STORE_ARCTICDB_UNIVERSE)


# ── the training-write audit ────────────────────────────────────────────────


def test_audit_counts_an_unrestated_ctva_as_a_missed_known_action():
    reg = CorporateActionRegistry(_FakeS3(), "alpha-engine-research")
    audit = compute.audit_action_jumps({"CTVA": _ctva_raw()}, reg)
    assert [(d, aid) for d, _r, aid in audit.missed["CTVA"]] == [
        (_EX, _ctva_action().action_id),
    ]
    assert "CTVA" not in audit.suspected


def test_audit_is_clean_on_the_restated_history():
    reg = CorporateActionRegistry(_FakeS3(), "alpha-engine-research")
    restated, _ = ca.apply_declared(_ctva_raw(), "CTVA")
    audit = compute.audit_action_jumps({"CTVA": restated}, reg)
    assert "CTVA" not in audit.missed
    assert "CTVA" not in audit.suspected


# ── the backfill / feature-snapshot delta merge ─────────────────────────────


def _delta_rows(df: pd.DataFrame) -> list[dict]:
    return [
        {
            "date": d, "Open": r.Open, "High": r.High, "Low": r.Low,
            "Close": r.Close, "Volume": r.Volume, "source": "polygon",
        }
        for d, r in df.iterrows()
    ]


def _run_delta(monkeypatch, base: pd.DataFrame, delta: pd.DataFrame, date_str="2026-10-02"):
    monkeypatch.setattr(
        compute, "_load_delta_from_daily_closes",
        lambda *a, **k: {"CTVA": _delta_rows(delta)} if len(delta) else {},
    )
    monkeypatch.setattr(ca, "detect_splits", lambda *a, **k: [])
    reg = CorporateActionRegistry(_FakeS3(), "alpha-engine-research")
    return compute._apply_daily_delta(
        s3=None, bucket="b", date_str=date_str, price_data={"CTVA": base}, registry=reg,
    )


def _assert_restated(series: pd.Series):
    rets = series.pct_change().dropna()
    assert rets.abs().max() < compute._ACTION_JUMP_SCREEN_THRESHOLD
    assert series.loc["2026-09-30"] == pytest.approx(77.650002 * _ctva_action().price_factor)


def test_backfill_raw_cache_base_is_restated(monkeypatch):
    """Saturday backfill: the base is the raw yfinance 10-year cache (spans the
    ex-date), the delta is the archive's last day only."""
    raw = _ctva_raw()
    out, split_tickers = _run_delta(monkeypatch, raw, raw.iloc[-1:])
    assert "CTVA" in split_tickers
    _assert_restated(out["CTVA"]["Close"])


def test_already_restated_base_with_a_raw_archive_delta_across_the_ex_date(monkeypatch):
    """Feature snapshot with a wide delta window: ArcticDB base already restated
    by the morning sync, archive rows (vendor basis) back to 09-29. The archive
    rows are restated on their own before they override base rows."""
    raw = _ctva_raw()
    restated, _ = ca.apply_declared(raw, "CTVA")
    out, split_tickers = _run_delta(monkeypatch, restated, raw.iloc[4:])
    assert "CTVA" in split_tickers
    _assert_restated(out["CTVA"]["Close"])


def test_stale_base_that_ends_before_the_ex_date_is_restated_once(monkeypatch):
    """Base ends 09-28 (does not span the ex-date); the delta carries 09-29
    onward. The merged frame is restated as one, never row-by-row twice."""
    raw = _ctva_raw()
    out, split_tickers = _run_delta(monkeypatch, raw.iloc[:4], raw.iloc[4:])
    assert "CTVA" in split_tickers
    _assert_restated(out["CTVA"]["Close"])
    assert out["CTVA"]["Close"].loc["2026-09-23"] == pytest.approx(
        80.419998 * _ctva_action().price_factor,
    )


def test_no_delta_still_restates_the_base(monkeypatch):
    raw = _ctva_raw()
    out, split_tickers = _run_delta(monkeypatch, raw, raw.iloc[0:0])
    assert "CTVA" in split_tickers
    _assert_restated(out["CTVA"]["Close"])


def test_no_registry_means_no_restatement(monkeypatch):
    raw = _ctva_raw()
    monkeypatch.setattr(compute, "_load_delta_from_daily_closes", lambda *a, **k: {})
    out, split_tickers = compute._apply_daily_delta(
        None, "b", "2026-10-02", {"CTVA": raw},
    )
    assert split_tickers == set()
    pd.testing.assert_frame_equal(out["CTVA"], raw)


# ── the morning sync (ArcticDB universe) ────────────────────────────────────


def _seed_arctic(tmp_path, df):
    adb = pytest.importorskip("arcticdb")
    ac = adb.Arctic(f"lmdb://{tmp_path}")
    lib = ac.get_library("universe", create_if_missing=True)
    lib.write("CTVA", df)
    return lib


def test_sync_restates_arctic_ctva_once_and_leaves_the_archive(tmp_path, monkeypatch):
    s3 = _FakeS3()
    lib = _seed_arctic(tmp_path, _ctva_raw())
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)
    reg = CorporateActionRegistry(s3, "alpha-engine-research")

    def _sync():
        return ca.sync(
            s3, "alpha-engine-research", "2026-10-02", "2026-10-02",
            stores=[ca.STORE_DAILY_CLOSES_ARCHIVE, ca.STORE_ARCTICDB_UNIVERSE],
            run_id="2026-10-02", tickers=["CTVA", "AAPL"], registry=reg,
            actions=[], dividend_actions=[],
        )

    first = _sync()
    statuses = [r["status"] for r in first.applied[ca.STORE_ARCTICDB_UNIVERSE]]
    assert statuses == ["applied"]
    assert first.detected == []          # declared actions are not polygon detections
    assert first.notices == [_ctva_action()]
    _assert_restated(lib.read("CTVA").data["Close"])
    version = lib.read("CTVA").version

    second = _sync()
    statuses = [r["status"] for r in second.applied[ca.STORE_ARCTICDB_UNIVERSE]]
    assert statuses == ["already_reflected"]
    assert lib.read("CTVA").version == version   # no rewrite
    assert second.notices == []

    # No registry record, no marker, no archive write.
    assert not any(k.startswith("corporate_actions/") for k in s3.store)
    assert not any(k.startswith("staging/") for k in s3.store)


def test_sync_respects_the_ticker_scope(tmp_path, monkeypatch):
    s3 = _FakeS3()
    lib = _seed_arctic(tmp_path, _ctva_raw())
    import store.arctic_store as arctic_store
    monkeypatch.setattr(arctic_store, "get_universe_lib", lambda *a, **k: lib)
    result = ca.sync(
        s3, "alpha-engine-research", "2026-10-02", "2026-10-02",
        stores=[ca.STORE_ARCTICDB_UNIVERSE], run_id="r", tickers=["AAPL"],
        registry=CorporateActionRegistry(s3, "alpha-engine-research"),
        actions=[], dividend_actions=[],
    )
    assert result.applied[ca.STORE_ARCTICDB_UNIVERSE] == []
    pd.testing.assert_frame_equal(
        lib.read("CTVA").data, _ctva_raw(), check_freq=False,
    )
