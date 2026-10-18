"""A registered split explains a vendor restatement only with price evidence
(alpha-engine-config-I12359).

Polygon's splits feed carried a 2:1 split for one security twice — ``PSKY``
ex 2026-10-05 and ``SKYD`` ex 2026-10-14 — and the trade tape shows neither.
The ``polygon_only`` morning window then overwrote correct closes in
``staging/daily_closes`` with Polygon's halved bars (SKYD 10-13: 9.27 → 4.635,
PSKY 10-02: 9.50 → 4.75), and ``explains_discrepancy`` classified each overwrite
as "explained" on the registry record alone, so nothing paged.

These tests pin the guard: a factor-and-ex-date match must ALSO have the split
on the raw tape (``price_evidence_orientation``: ``direct`` or ``inverse``), or
the overwrite is refused and pages.
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

import corporate_actions as ca
from collectors import daily_closes
from corporate_actions import CorporateActionRegistry


class _FakeS3:
    """In-memory S3 double for the registry's own JSON store."""

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


def _tape(points: dict[str, float]) -> pd.Series:
    return pd.Series(list(points.values()), index=pd.to_datetime(list(points)))


# The real tape, from the issue. SKYD: 10-13 close 9.27 → 10-14 close 7.06
# (ratio 0.76 — a real 2:1 would print ~0.5). PSKY: 10-02 close 9.50 → 10-05
# close 9.78 (Polygon's own 10-05 bar is 9.775).
SKYD_TAPE = _tape({
    "2026-10-12": 9.10, "2026-10-13": 9.27, "2026-10-14": 7.06,
    "2026-10-15": 7.18, "2026-10-16": 7.25,
})
PSKY_TAPE = _tape({
    "2026-10-01": 9.41, "2026-10-02": 9.50, "2026-10-05": 9.78,
    "2026-10-06": 9.70, "2026-10-07": 9.66,
})


def _skyd_phantom():
    # Polygon's "2:1" forward split: split_from=1, split_to=2 → factor 0.5.
    return ca.CorporateAction.from_split("SKYD", "2026-10-14", 1, 2)


def _psky_phantom():
    return ca.CorporateAction.from_split("PSKY", "2026-10-05", 1, 2)


def _registry(*actions) -> CorporateActionRegistry:
    reg = CorporateActionRegistry(_FakeS3(), "alpha-engine-research")
    for a in actions:
        reg.record_detected(a, run_id="r")
    return reg


def _loader(series_by_ticker):
    calls = []

    def _load(action):
        calls.append(action.action_id)
        return series_by_ticker.get(action.ticker)

    _load.calls = calls
    return _load


SETTLED = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)   # after both ex-dates
SKYD_EX_MORNING = datetime(2026, 10, 14, 12, 4, tzinfo=timezone.utc)  # the 12:04Z run


# ── registry: classify_discrepancy ──────────────────────────────────────────


class TestClassifyDiscrepancy:
    def test_skyd_phantom_matches_factor_but_has_no_evidence(self):
        reg = _registry(_skyd_phantom())
        # The legacy, record-only test still matches — that is the defect.
        assert reg.explains_discrepancy("SKYD", "2026-10-13", 9.27, 4.635) is not None
        action, verdict = reg.classify_discrepancy(
            "SKYD", "2026-10-13", 9.27, 4.635,
            close_loader=_loader({"SKYD": SKYD_TAPE}),
        )
        assert action is not None and action.ticker == "SKYD"
        assert verdict == "none"

    def test_psky_phantom_has_no_evidence(self):
        reg = _registry(_psky_phantom())
        action, verdict = reg.classify_discrepancy(
            "PSKY", "2026-10-02", 9.50, 4.75,
            close_loader=_loader({"PSKY": PSKY_TAPE}),
        )
        assert action is not None and action.ticker == "PSKY"
        assert verdict == "none"

    def test_real_split_on_the_tape_is_direct(self):
        # Same record, but a tape that DOES halve at the ex-date boundary.
        real = _tape({
            "2026-10-12": 9.10, "2026-10-13": 9.27, "2026-10-14": 4.66,
            "2026-10-15": 4.70,
        })
        reg = _registry(_skyd_phantom())
        _action, verdict = reg.classify_discrepancy(
            "SKYD", "2026-10-13", 9.27, 4.635, close_loader=_loader({"SKYD": real}),
        )
        assert verdict == "direct"

    def test_inverted_feed_record_with_tape_evidence_is_inverse(self):
        # Feed publishes 2:1 (factor 2.0) for what the tape shows is a 1:2
        # halving — the HON/DD class; the overwrite is still that action's.
        inverted = ca.CorporateAction.from_split("SKYD", "2026-10-14", 2, 1)
        real = _tape({
            "2026-10-12": 9.10, "2026-10-13": 9.27, "2026-10-14": 4.66,
            "2026-10-15": 4.70,
        })
        reg = _registry(inverted)
        _action, verdict = reg.classify_discrepancy(
            "SKYD", "2026-10-13", 9.27, 4.635, close_loader=_loader({"SKYD": real}),
        )
        assert verdict == "inverse"

    def test_no_loader_is_uncovered_never_a_pass(self):
        reg = _registry(_skyd_phantom())
        _action, verdict = reg.classify_discrepancy("SKYD", "2026-10-13", 9.27, 4.635)
        assert verdict == "uncovered"

    def test_loader_failure_is_uncovered_never_a_pass(self):
        def _boom(_action):
            raise RuntimeError("s3 down")

        reg = _registry(_skyd_phantom())
        _action, verdict = reg.classify_discrepancy(
            "SKYD", "2026-10-13", 9.27, 4.635, close_loader=_boom,
        )
        assert verdict == "uncovered"

    def test_unmatched_jump_returns_none_none(self):
        reg = _registry(_skyd_phantom())
        assert reg.classify_discrepancy(
            "SKYD", "2026-10-13", 9.27, 7.0, close_loader=_loader({"SKYD": SKYD_TAPE}),
        ) == (None, None)

    def test_verdict_is_memoized_per_action_for_the_window(self):
        # One registry per window run: the verdict must come from the stores as
        # they stood at the first question, not after this run's own writes.
        load = _loader({"SKYD": SKYD_TAPE})
        reg = _registry(_skyd_phantom())
        for d, prior in (("2026-10-08", 9.40), ("2026-10-09", 9.35), ("2026-10-13", 9.27)):
            reg.classify_discrepancy("SKYD", d, prior, prior / 2, close_loader=load)
        assert len(load.calls) == 1


# ── collector: _log_close_discrepancies ─────────────────────────────────────


class TestLogCloseDiscrepanciesRefusesPhantom:
    def test_skyd_phantom_is_refused_and_pages(self, caplog):
        new_df = pd.DataFrame({"Close": [4.635]}, index=["SKYD"])
        refused: list = []
        with patch.object(daily_closes, "_now_utc", return_value=SETTLED), \
                caplog.at_level(logging.DEBUG):
            explained, unexplained = daily_closes._log_close_discrepancies(
                new_df, {"SKYD": 9.27}, "2026-10-13",
                registry=_registry(_skyd_phantom()),
                close_loader=_loader({"SKYD": SKYD_TAPE}),
                refused=refused,
            )
        assert explained == []
        assert refused == ["SKYD"]
        assert len(unexplained) == 1
        assert unexplained[0]["refused_action_id"] == _skyd_phantom().action_id
        assert unexplained[0]["evidence"] == "none"
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "OVERWRITE REFUSED SKYD" in errors[0].message
        assert not any(
            "corporate_action_restatement" in r.message for r in caplog.records
        )

    def test_psky_phantom_is_refused(self):
        new_df = pd.DataFrame({"Close": [4.75]}, index=["PSKY"])
        refused: list = []
        with patch.object(daily_closes, "_now_utc", return_value=SETTLED):
            explained, _unexplained = daily_closes._log_close_discrepancies(
                new_df, {"PSKY": 9.50}, "2026-10-02",
                registry=_registry(_psky_phantom()),
                close_loader=_loader({"PSKY": PSKY_TAPE}),
                refused=refused,
            )
        assert explained == []
        assert refused == ["PSKY"]

    def test_registry_match_without_loader_is_refused(self, caplog):
        # The guard is on by default: no tape, no restatement.
        new_df = pd.DataFrame({"Close": [4.635]}, index=["SKYD"])
        refused: list = []
        with patch.object(daily_closes, "_now_utc", return_value=SETTLED), \
                caplog.at_level(logging.ERROR):
            explained, _ = daily_closes._log_close_discrepancies(
                new_df, {"SKYD": 9.27}, "2026-10-13",
                registry=_registry(_skyd_phantom()), refused=refused,
            )
        assert explained == []
        assert refused == ["SKYD"]
        assert any("price evidence=uncovered" in r.message for r in caplog.records)

    def test_unsettled_ex_date_refuses_at_warn(self, caplog):
        # The 2026-10-14 12:04Z run: the ex-date session has not traded, so the
        # tape cannot show the split yet. Refuse, but do not page until the
        # next run can read the ex-date bar.
        new_df = pd.DataFrame({"Close": [4.635]}, index=["SKYD"])
        pre_ex = SKYD_TAPE[SKYD_TAPE.index < "2026-10-14"]
        refused: list = []
        with patch.object(daily_closes, "_now_utc", return_value=SKYD_EX_MORNING), \
                caplog.at_level(logging.DEBUG):
            explained, unexplained = daily_closes._log_close_discrepancies(
                new_df, {"SKYD": 9.27}, "2026-10-13",
                registry=_registry(_skyd_phantom()),
                close_loader=_loader({"SKYD": pre_ex}),
                refused=refused,
            )
        assert explained == []
        assert refused == ["SKYD"]
        assert unexplained[0]["pending"] is True
        assert not any(r.levelno == logging.ERROR for r in caplog.records)
        assert any(
            "OVERWRITE REFUSED SKYD" in r.message and "not settled yet" in r.message
            for r in caplog.records if r.levelno == logging.WARNING
        )

    def test_evidenced_split_is_still_explained(self, caplog):
        real = _tape({
            "2026-10-12": 9.10, "2026-10-13": 9.27, "2026-10-14": 4.66,
            "2026-10-15": 4.70,
        })
        new_df = pd.DataFrame({"Close": [4.635]}, index=["SKYD"])
        refused: list = []
        with caplog.at_level(logging.DEBUG):
            explained, unexplained = daily_closes._log_close_discrepancies(
                new_df, {"SKYD": 9.27}, "2026-10-13",
                registry=_registry(_skyd_phantom()),
                close_loader=_loader({"SKYD": real}),
                refused=refused,
            )
        assert [a.ticker for a in explained] == ["SKYD"]
        assert refused == [] and unexplained == []
        assert not any(r.levelno == logging.ERROR for r in caplog.records)


# ── window roll-up ──────────────────────────────────────────────────────────


def _refused_row(date, prior, new, *, pending=False):
    return {
        "ticker": "SKYD", "date": date, "prior": prior, "new": new,
        "ratio": new / prior, "refused_action_id": _skyd_phantom().action_id,
        "evidence": "none", "pending": pending,
    }


class TestWindowRollupOfRefusals:
    def test_refused_window_pages_once_naming_the_phantom(self, caplog):
        rows = [
            _refused_row("2026-10-06", 9.60, 4.80),
            _refused_row("2026-10-07", 9.50, 4.75),
            _refused_row("2026-10-13", 9.27, 4.635),
        ]
        with caplog.at_level(logging.ERROR):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-10-15")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "OVERWRITE REFUSED SKYD" in errors[0].message
        assert "3 window date(s)" in errors[0].message
        assert "phantom" in errors[0].message

    def test_all_pending_refusal_is_warn(self, caplog):
        rows = [
            _refused_row("2026-10-12", 9.10, 4.55, pending=True),
            _refused_row("2026-10-13", 9.27, 4.635, pending=True),
        ]
        with caplog.at_level(logging.DEBUG):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-10-13")
        assert not any(r.levelno == logging.ERROR for r in caplog.records)
        assert any("OVERWRITE REFUSED SKYD" in r.message for r in caplog.records)

    def test_unrefused_rows_for_same_ticker_still_page(self, caplog):
        rows = [
            _refused_row("2026-10-13", 9.27, 4.635),
            {"ticker": "SKYD", "date": "2026-10-09", "prior": 9.0, "new": 12.0,
             "ratio": 12.0 / 9.0},
        ]
        with caplog.at_level(logging.ERROR):
            daily_closes._emit_window_unexplained_discrepancies(rows, "2026-10-15")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 2
        assert any("OVERWRITE SKYD @ 2026-10-09" in r.message for r in errors)


# ── collect(): the refused restatement is not written ───────────────────────


def _archive_s3(rows: dict[str, float]) -> MagicMock:
    """Every archive key returns the same yfinance-written parquet."""
    s3 = MagicMock()
    s3.head_object.return_value = {
        "LastModified": datetime(2026, 10, 13, 23, 0, tzinfo=timezone.utc),
        "ContentLength": 1, "ContentType": "application/octet-stream",
    }
    df = pd.DataFrame(
        [{"Open": c, "High": c, "Low": c, "Close": c, "Adj_Close": c,
          "Volume": 26_560_000, "VWAP": None, "source": "yfinance", "revision": 1}
         for c in rows.values()],
        index=pd.Index(list(rows), name="ticker"),
    )
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=True)
    body = buf.getvalue()
    s3.get_object.side_effect = lambda *a, **k: {"Body": io.BytesIO(body)}
    s3.put_object.return_value = {"ETag": '"abc"'}
    return s3


def _written(s3, run_date) -> pd.DataFrame:
    for call in s3.put_object.call_args_list:
        if call.kwargs["Key"].endswith(f"{run_date}.parquet"):
            return pd.read_parquet(io.BytesIO(call.kwargs["Body"]), engine="pyarrow")
    raise AssertionError("daily_closes parquet was not written")


def test_collect_keeps_prior_row_for_unevidenced_phantom_split():
    s3 = _archive_s3({"SKYD": 9.27, "AAPL": 200.0})

    def _polygon_side(tickers, run_date, records, source):
        for t, close, vol in (("SKYD", 4.635, 53_250_000), ("AAPL", 200.4, 1)):
            records.append({
                "ticker": t, "date": run_date, "Open": close, "High": close,
                "Low": close, "Close": close, "Adj_Close": close, "Volume": vol,
                "VWAP": close, "source": "polygon",
            })
        return 2

    with patch("collectors.daily_closes.boto3.client", return_value=s3), \
            patch.object(daily_closes, "_now_utc", return_value=SETTLED), \
            patch.object(daily_closes, "_fetch_polygon_closes", side_effect=_polygon_side), \
            patch.object(daily_closes, "_fetch_yfinance_closes", return_value=0), \
            patch.object(daily_closes, "_fetch_fred_closes", return_value=0):
        result = daily_closes.collect(
            bucket="b", tickers=["SKYD", "AAPL"], run_date="2026-10-13",
            source="polygon_only", registry=_registry(_skyd_phantom()),
            _emit_ca_email=False,
        )
    assert result.get("status") == "ok"
    assert result["corporate_actions"] == []
    out = _written(s3, "2026-10-13")
    # The phantom halving is NOT written: SKYD keeps its yfinance row.
    assert out.loc["SKYD", "Close"] == 9.27
    assert out.loc["SKYD", "Volume"] == 26_560_000
    assert out.loc["SKYD", "source"] == "yfinance"
    # An ordinary ticker still takes the polygon overwrite.
    assert out.loc["AAPL", "Close"] == 200.4
    assert out.loc["AAPL", "source"] == "polygon"
