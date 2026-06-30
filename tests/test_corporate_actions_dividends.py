"""PR5 (config#1433) — dividend detection + total-return-factor MATH primitives.

CRSP/Barra basis (Brian-decided): dividends are tracked as a SEPARATE
total-return series and MUST NOT be folded into the stored split-adjusted price
level. PR5 is detection + registry capture + TR-factor math + sync wiring — it
changes NO stored price/feature/label/schema. These tests pin:

  * ``polygon_client.get_dividends`` / ``get_recent_dividends`` — parse polygon
    dividend rows, skip malformed, 403 → [].
  * ``corporate_actions.detect_dividends`` — maps polygon events to dividend
    ``CorporateAction``s with stable, distinct ``action_id``s.
  * ``dividend_factor`` + ``total_return_series`` — the CRSP back-adjust (a $1
    dividend on a $100 close → pre-ex prices ×0.99), compounding multiple
    dividends, split×dividend INDEPENDENCE, and NON-mutation of the input frame.
  * ``sync`` RECORDS dividends (registry write-if-absent) but applies them to NO
    price store and emits NO notice/email (CRSP-separate + sub-threshold).
"""

from __future__ import annotations

import io

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import corporate_actions as ca
from corporate_actions import CorporateActionRegistry
from polygon_client import PolygonClient, PolygonForbiddenError
from split_factor import restate_series_for_splits


# ── polygon_client.get_dividends / get_recent_dividends ──────────────────────


def _make_client() -> PolygonClient:
    return PolygonClient(api_key="test-key", calls_per_min=5)


def test_get_dividends_parses_and_sorts():
    client = _make_client()
    resp = {
        "results": [
            {"ex_dividend_date": "2026-06-10", "cash_amount": 0.24,
             "dividend_type": "CD"},
            {"ex_dividend_date": "2026-03-10", "cash_amount": 0.22,
             "dividend_type": "CD"},
        ],
        "status": "OK",
    }
    with patch.object(client, "_get", return_value=resp) as mock_get:
        out = client.get_dividends("HON")
    assert mock_get.call_count == 1
    # Sorted ascending by ex_dividend_date.
    assert [d["ex_dividend_date"] for d in out] == ["2026-03-10", "2026-06-10"]
    assert out[1] == {"ex_dividend_date": "2026-06-10", "cash_amount": 0.24,
                      "dividend_type": "CD"}
    # ticker filter passed (full history, no range).
    _, kwargs = mock_get.call_args
    assert kwargs["params"]["ticker"] == "HON"


def test_get_dividends_skips_malformed_rows():
    client = _make_client()
    resp = {
        "results": [
            {"ex_dividend_date": "2026-06-10", "cash_amount": 0.24,
             "dividend_type": "CD"},
            {"ex_dividend_date": None, "cash_amount": 0.5},          # bad date
            {"ex_dividend_date": "2026-01-01", "cash_amount": 0.0},  # non-positive
            {"ex_dividend_date": "2026-02-01", "cash_amount": -1.0},  # negative
            {"ex_dividend_date": "2026-03-01"},                      # missing cash
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_dividends("HON")
    assert [d["ex_dividend_date"] for d in out] == ["2026-06-10"]


def test_get_dividends_forbidden_returns_empty():
    client = _make_client()
    with patch.object(client, "_get", side_effect=PolygonForbiddenError("403")):
        assert client.get_dividends("HON") == []


def test_get_recent_dividends_range_scoped_single_call():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "AAPL", "ex_dividend_date": "2026-05-09",
             "cash_amount": 0.25, "dividend_type": "CD"},
            {"ticker": "MSFT", "ex_dividend_date": "2026-05-08",
             "cash_amount": 0.75, "dividend_type": "CD"},
        ],
        "status": "OK",
    }
    with patch.object(client, "_get", return_value=resp) as mock_get:
        out = client.get_recent_dividends("2026-05-01", "2026-05-15")
    assert mock_get.call_count == 1
    _, kwargs = mock_get.call_args
    params = kwargs["params"]
    assert params["ex_dividend_date.gte"] == "2026-05-01"
    assert params["ex_dividend_date.lte"] == "2026-05-15"
    assert "ticker" not in params  # whole-market scan
    # Sorted ascending; carries ticker.
    assert [d["ex_dividend_date"] for d in out] == ["2026-05-08", "2026-05-09"]
    assert out[1]["ticker"] == "AAPL"


def test_get_recent_dividends_skips_malformed_rows():
    client = _make_client()
    resp = {
        "results": [
            {"ticker": "AAPL", "ex_dividend_date": "2026-05-09",
             "cash_amount": 0.25, "dividend_type": "CD"},
            {"ticker": None, "ex_dividend_date": "2026-05-08",
             "cash_amount": 0.75},                                    # missing ticker
            {"ticker": "BAR", "ex_dividend_date": None,
             "cash_amount": 0.5},                                     # missing date
            {"ticker": "BAZ", "ex_dividend_date": "2026-05-07",
             "cash_amount": 0.0},                                     # non-positive
        ]
    }
    with patch.object(client, "_get", return_value=resp):
        out = client.get_recent_dividends("2026-05-01", "2026-05-15")
    assert [d["ticker"] for d in out] == ["AAPL"]


def test_get_recent_dividends_forbidden_returns_empty():
    client = _make_client()
    with patch.object(client, "_get", side_effect=PolygonForbiddenError("403")):
        assert client.get_recent_dividends("2026-05-01", "2026-05-15") == []


# ── corporate_actions.detect_dividends ───────────────────────────────────────


def _fake_div_client(events):
    client = MagicMock()
    client.get_recent_dividends.return_value = events
    return client


def test_detect_dividends_maps_to_actions():
    client = _fake_div_client([
        {"ticker": "AAPL", "ex_dividend_date": "2026-05-09",
         "cash_amount": 0.25, "dividend_type": "CD"},
        {"ticker": "MSFT", "ex_dividend_date": "2026-05-08",
         "cash_amount": 0.75, "dividend_type": "CD"},
    ])
    actions = ca.detect_dividends("2026-05-01", "2026-05-15", client=client)
    assert client.get_recent_dividends.call_count == 1
    assert {a.ticker for a in actions} == {"AAPL", "MSFT"}
    aapl = next(a for a in actions if a.ticker == "AAPL")
    assert aapl.type == "dividend"
    assert aapl.ex_date == "2026-05-09"
    assert aapl.cash_amount == 0.25
    assert aapl.dividend_kind == "CD"
    assert aapl.raw["cash_amount"] == 0.25


def test_detect_dividends_action_ids_stable_and_distinct():
    a = ca.CorporateAction.from_dividend("AAPL", "2026-05-09", 0.25, "CD")
    a_again = ca.CorporateAction.from_dividend("AAPL", "2026-05-09", 0.25, "CD")
    diff_date = ca.CorporateAction.from_dividend("AAPL", "2026-08-09", 0.25, "CD")
    diff_amt = ca.CorporateAction.from_dividend("AAPL", "2026-05-09", 0.26, "CD")
    # Stable (content-addressed) and 16-char.
    assert a.action_id == a_again.action_id
    assert len(a.action_id) == 16
    # Distinct on date AND on amount (so two dividends never collide).
    assert a.action_id != diff_date.action_id
    assert a.action_id != diff_amt.action_id
    # Distinct from a split on the same ticker/date.
    split = ca.CorporateAction.from_split("AAPL", "2026-05-09", 1, 4)
    assert a.action_id != split.action_id


def test_detect_dividends_fetch_failure_degrades_to_empty():
    client = MagicMock()
    client.get_recent_dividends.side_effect = RuntimeError("polygon down")
    assert ca.detect_dividends("2026-05-01", "2026-05-15", client=client) == []


# ── dividend_factor + total_return_series (CRSP primitive) ───────────────────


def test_dividend_factor_basic():
    # $1 dividend on a $100 prior close → pre-ex prices ×0.99.
    assert ca.dividend_factor(1.0, 100.0) == pytest.approx(0.99)
    assert ca.dividend_factor(0.0, 100.0) == pytest.approx(1.0)


def test_dividend_factor_guards_close_prev():
    with pytest.raises(ValueError):
        ca.dividend_factor(1.0, 0.0)
    with pytest.raises(ValueError):
        ca.dividend_factor(1.0, -5.0)


def _flat_close_frame(closes, start="2026-06-01"):
    idx = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"Close": np.asarray(closes, dtype="float64")}, index=idx)


def test_total_return_series_single_dividend_back_adjusts_pre_ex():
    df = _flat_close_frame([100.0, 100.0, 100.0, 100.0, 100.0])
    ex = df.index[3].strftime("%Y-%m-%d")  # close_prev = index[2] = 100 → 0.99
    div = ca.CorporateAction.from_dividend("X", ex, 1.0, "CD")
    tr = ca.total_return_series(df, [div])
    # Rows STRICTLY before ex (0,1,2) ×0.99; rows on/after ex unchanged.
    assert list(tr.to_numpy()) == pytest.approx([99.0, 99.0, 99.0, 100.0, 100.0])
    assert tr.name == "tr_close"
    # Index preserved.
    assert list(tr.index) == list(df.index)


def test_total_return_series_compounds_multiple_dividends_oldest_to_newest():
    df = _flat_close_frame([100.0, 100.0, 100.0, 100.0, 100.0])
    ex1 = df.index[2].strftime("%Y-%m-%d")  # close_prev = index[1]=100 → f1=0.99
    ex2 = df.index[4].strftime("%Y-%m-%d")  # close_prev = index[3]=100 → f2=0.98
    d1 = ca.CorporateAction.from_dividend("X", ex1, 1.0, "CD")
    d2 = ca.CorporateAction.from_dividend("X", ex2, 2.0, "CD")
    # Order of the input list must not matter (sorted oldest→newest internally).
    tr = ca.total_return_series(df, [d2, d1])
    # rows<ex1 (0,1): f1*f2 = 0.9702 → 97.02; row2 (in [ex1,ex2)): f2 → 98;
    # row3 (in [ex1,ex2)): f2 → 98; row4 (>=ex2): unchanged 100.
    assert list(tr.to_numpy()) == pytest.approx([97.02, 97.02, 98.0, 98.0, 100.0])


def test_total_return_series_does_not_mutate_input():
    df = _flat_close_frame([100.0, 100.0, 100.0, 100.0, 100.0])
    df["Volume"] = 1_000_000.0
    before = df.copy(deep=True)
    ex = df.index[3].strftime("%Y-%m-%d")
    ca.total_return_series(df, [ca.CorporateAction.from_dividend("X", ex, 1.0)])
    pd.testing.assert_frame_equal(df, before)


def test_total_return_series_independent_of_split_adjustment():
    """Split adjusts the PRICE LEVEL; dividend adjusts a SEPARATE TR series. The
    TR series is the split-adjusted price further dividend-adjusted — the two
    operations compose independently and the split restatement is untouched."""
    # Un-restated 1-for-3 REVERSE split (ex at index 3): pre rows $40, post $120.
    idx = pd.bdate_range("2026-06-01", periods=5)
    raw = pd.DataFrame(
        {"Close": [40.0, 40.0, 40.0, 120.0, 120.0], "Volume": [3e6] * 5},
        index=idx,
    )
    ex_split = idx[3].strftime("%Y-%m-%d")
    split_events = [{"execution_date": ex_split, "split_from": 3, "split_to": 1}]
    split_adj = restate_series_for_splits(raw, split_events)
    # After split restatement the close is flat $120 (pre rows ×3).
    assert list(split_adj["Close"].to_numpy()) == pytest.approx([120.0] * 5)

    split_adj_snapshot = split_adj.copy(deep=True)
    # Dividend ex at index 2 (before split ex): close_prev = split_adj close[1]
    # = 120 → factor = 1 - 1.2/120 = 0.99.
    ex_div = idx[2].strftime("%Y-%m-%d")
    div = ca.CorporateAction.from_dividend("DD", ex_div, 1.20, "CD")
    tr = ca.total_return_series(split_adj, [div])

    # TR = split-adjusted close, with rows<ex_div (0,1) ×0.99.
    assert list(tr.to_numpy()) == pytest.approx([118.8, 118.8, 120.0, 120.0, 120.0])
    # Independence: the split restatement frame is NOT mutated by the TR math.
    pd.testing.assert_frame_equal(split_adj, split_adj_snapshot)


# ── sync: records dividends, restates NO store, emits NO notice ──────────────


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


_BUCKET = "alpha-engine-research"


def test_sync_records_dividends_but_applies_to_no_store_and_no_notice():
    s3 = _FakeS3()
    reg = CorporateActionRegistry(s3, _BUCKET)
    div = ca.CorporateAction.from_dividend("AAPL", "2026-05-09", 0.25, "CD")

    result = ca.sync(
        s3, _BUCKET, "2026-05-08", "2026-05-12",
        stores=[ca.STORE_DAILY_CLOSES_ARCHIVE],
        run_id="2026-05-12",
        tickers=["AAPL"],
        registry=reg,
        actions=[],                 # no splits (avoids any restatement path)
        dividend_actions=[div],     # injected → no live polygon call
    )

    # Recorded into the registry (write-if-absent).
    assert reg.get_action(div.action_id) is not None
    # Surfaced in SyncResult.dividends; never a split-detected nor a notice.
    assert [a.action_id for a in result.dividends] == [div.action_id]
    assert result.detected == []
    assert result.notices == []   # CRSP-separate + sub-threshold → no email/notice
    # Applied to NO price store: no applied marker for the dividend in any store.
    assert reg.is_applied(ca.STORE_DAILY_CLOSES_ARCHIVE, div.action_id) is False
    assert reg.is_applied(ca.STORE_ARCTICDB_UNIVERSE, div.action_id) is False
    # And nothing was restated (no daily_closes parquet writes occurred).
    assert all(
        not k.endswith(".parquet") for k in s3.store
    ), "sync must not write any price parquet for a dividend"


def test_sync_dividend_record_is_idempotent_across_reruns():
    s3 = _FakeS3()
    reg = CorporateActionRegistry(s3, _BUCKET)
    div = ca.CorporateAction.from_dividend("AAPL", "2026-05-09", 0.25, "CD")
    kwargs = dict(
        stores=[ca.STORE_DAILY_CLOSES_ARCHIVE], run_id="r", tickers=["AAPL"],
        registry=reg, actions=[], dividend_actions=[div],
    )
    r1 = ca.sync(s3, _BUCKET, "2026-05-08", "2026-05-12", **kwargs)
    r2 = ca.sync(s3, _BUCKET, "2026-05-08", "2026-05-12", **kwargs)
    assert [a.action_id for a in r1.dividends] == [div.action_id]
    assert [a.action_id for a in r2.dividends] == [div.action_id]
    # One detected record persisted (content-addressed key); rerun is a no-op.
    div_keys = [k for k in s3.store if div.action_id in k]
    assert len(div_keys) == 1
