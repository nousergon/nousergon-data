"""D21 cannot shrink ``consolidated.json`` silently (alpha-engine-config-I12688).

On 2026-10-16 a D21 run answered 80 of the ~953 series the previous publish carried,
and ``collect_history`` overwrote ``market_data/close_history/consolidated.json`` with
those 80. The interior-bar carry (I11556) only fills sessions inside series the run
answered, so the other ~870 vanished from the artifact with a well-formed header, and
Metron's Demo household valued 13 of its 25 reference symbols one session stale for
two days. Every number below is synthetic.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from collectors import metron_market_data as mmd

RUN_DATE = "2026-10-16"  # a Friday session
DAY = dt.date.fromisoformat(RUN_DATE)
KEY = mmd.CONSOLIDATED_CLOSE_HISTORY_KEY
PREV_DATES = ["2026-10-13", "2026-10-14", "2026-10-15"]
FRESH_DATES = ["2026-10-14", "2026-10-15", "2026-10-16"]
SYMBOLS = [f"S{i:02d}" for i in range(20)]


def _prev_series(i: int) -> list[list]:
    return [[d, 100.0 + i + k] for k, d in enumerate(PREV_DATES)]


def _fresh_series(i: int) -> list[tuple[str, float]]:
    return [(d, 100.0 + i + k + 1) for k, d in enumerate(FRESH_DATES)]


PREVIOUS = {sym: _prev_series(i) for i, sym in enumerate(SYMBOLS)}


# ── the pure rules ──────────────────────────────────────────────────────────


def test_a_series_the_run_omitted_is_carried_unchanged_and_named_with_its_last_date():
    fresh = {"S00": _fresh_series(0)}
    series, carried, dropped = mmd.carry_forward_absent_series(fresh, {"S00": _prev_series(0), "S01": _prev_series(1)}, DAY)
    assert series["S00"] is fresh["S00"]
    assert series["S01"] == [tuple(p) for p in _prev_series(1)]
    assert carried == [{"symbol": "S01", "last_date": "2026-10-15", "sessions_stale": 1}]
    assert dropped == []


def test_an_empty_fresh_series_counts_as_absent():
    series, carried, _ = mmd.carry_forward_absent_series({"S01": []}, {"S01": _prev_series(1)}, DAY)
    assert [d for d, _c in series["S01"]] == PREV_DATES
    assert [c["symbol"] for c in carried] == ["S01"]


def test_a_series_past_the_carry_limit_drops_with_its_reason():
    """A symbol that left the universe stops being carried; it is not carried forever."""
    old = [["2026-10-02", 50.0], ["2026-10-05", 51.0], ["2026-10-08", 52.0]]
    series, carried, dropped = mmd.carry_forward_absent_series({}, {"GONE": old}, DAY)
    assert "GONE" not in series and carried == []
    assert dropped == [{"symbol": "GONE", "last_date": "2026-10-08", "reason": "carry_expired", "sessions_stale": 6}]
    # Five sessions behind is still inside the limit.
    _, carried, _ = mmd.carry_forward_absent_series({}, {"X": [["2026-10-09", 1.0]]}, DAY)
    assert carried == [{"symbol": "X", "last_date": "2026-10-09", "sessions_stale": 5}]


def test_a_previous_series_ending_after_the_run_date_is_never_carried():
    future = [["2026-10-15", 1.0], ["2026-10-19", 2.0]]
    series, carried, dropped = mmd.carry_forward_absent_series({}, {"X": future}, DAY)
    assert series == {} and carried == []
    assert dropped == [{"symbol": "X", "last_date": "2026-10-19", "reason": "bar_after_run_date"}]


def test_a_malformed_previous_series_is_dropped_not_carried():
    _, carried, dropped = mmd.carry_forward_absent_series({}, {"BAD": [["2026-10-15"]], "NIL": []}, DAY)
    assert carried == []
    assert sorted((d["symbol"], d["reason"]) for d in dropped) == [
        ("BAD", "previous_series_malformed"), ("NIL", "previous_series_malformed"),
    ]


def test_the_shrink_verdict():
    fresh_80pct = {s: _fresh_series(i) for i, s in enumerate(SYMBOLS[:2])}
    assert mmd.series_shrink_verdict(fresh_80pct, PREVIOUS)["verdict"] == "shrunk"
    fresh_95pct = {s: _fresh_series(i) for i, s in enumerate(SYMBOLS[:19])}
    verdict = mmd.series_shrink_verdict(fresh_95pct, PREVIOUS)
    assert (verdict["verdict"], verdict["answered"], verdict["missing"]) == ("ok", 19, 1)
    assert mmd.series_shrink_verdict(fresh_80pct, None)["verdict"] == "unmeasurable"
    # A symbol new to this run answers nothing about the previous publish.
    assert mmd.series_shrink_verdict({"NEW": _fresh_series(0)}, PREVIOUS)["answered"] == 0


# ── wired into collect_history ──────────────────────────────────────────────


def _s3_serving(previous_doc):
    s3 = MagicMock()

    def _get(Bucket, Key, **_kw):
        if Key == KEY and previous_doc is not None:
            return {"Body": io.BytesIO(json.dumps(previous_doc).encode())}
        raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")

    s3.get_object.side_effect = _get
    return s3


def _puts(s3) -> dict[str, dict]:
    return {c.kwargs["Key"]: json.loads(c.kwargs["Body"]) for c in s3.put_object.call_args_list}


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(
        mmd, "load_price_derived_universe",
        lambda bucket, s3: ([{"yf_symbol": s, "currency": "USD"} for s in SYMBOLS], []),
    )


def _run(s3, answered: list[str], **kw):
    fresh = {s: _fresh_series(SYMBOLS.index(s)) for s in answered}
    return mmd.collect_history(
        bucket="b", run_date=RUN_DATE, s3_client=s3,
        close_history_source=lambda syms: dict(fresh),
        fx_history_source=lambda ccys: {},
        **kw,
    )


def _previous_doc(series=None, currency=None):
    series = PREVIOUS if series is None else series
    return {
        "schema_version": mmd.CLOSE_HISTORY_SCHEMA_VERSION,
        "adjustment_basis": mmd.CLOSE_HISTORY_ADJUSTMENT_BASIS,
        "series": series, "currency": currency or {s: "USD" for s in series},
    }


def test_a_run_answering_few_series_cannot_shrink_the_artifact_and_fails_loud(universe, caplog):
    """The 2026-10-16 shape: the run answered 2 of 20. The old code published 2."""
    s3 = _s3_serving(_previous_doc())
    with caplog.at_level(logging.WARNING, logger=mmd.logger.name):
        result = _run(s3, SYMBOLS[:2])
    doc = _puts(s3)[KEY]
    assert sorted(doc["series"]) == SYMBOLS
    assert doc["carried_series"] == {s: "2026-10-15" for s in SYMBOLS[2:]}
    for sym in SYMBOLS[2:]:
        assert doc["series"][sym] == PREVIOUS[sym]
    for i, sym in enumerate(SYMBOLS[:2]):
        assert doc["series"][sym] == [list(p) for p in _fresh_series(i)]
    assert result["status"] == "error" and "I12688" in result["error"]
    assert result["series_shrink"]["verdict"] == "shrunk"
    assert (result["carried_series"], result["close_series"]) == (18, 20)
    assert any(r.levelno == logging.ERROR and "SHRINK GUARD" in r.getMessage() for r in caplog.records)


def test_carried_series_are_not_rewritten_as_per_symbol_files(universe):
    s3 = _s3_serving(_previous_doc())
    _run(s3, SYMBOLS[:2])
    per_symbol = {k for k in _puts(s3) if k != KEY and k.startswith(mmd.CLOSE_HISTORY_PREFIX)}
    assert per_symbol == {f"{mmd.CLOSE_HISTORY_PREFIX}{s}.json" for s in SYMBOLS[:2]}


def test_ordinary_churn_carries_marked_and_stays_ok(universe):
    s3 = _s3_serving(_previous_doc())
    result = _run(s3, SYMBOLS[:19])
    doc = _puts(s3)[KEY]
    assert result["status"] == "ok" and result["series_shrink"]["verdict"] == "ok"
    assert doc["carried_series"] == {"S19": "2026-10-15"}
    assert len(doc["series"]) == 20


def test_a_full_run_marks_nothing(universe):
    s3 = _s3_serving(_previous_doc())
    result = _run(s3, SYMBOLS)
    assert result["status"] == "ok" and result["carried_series"] == 0
    assert _puts(s3)[KEY]["carried_series"] == {}


def test_with_no_previous_publish_nothing_is_carried_and_the_run_is_ok(universe):
    s3 = _s3_serving(None)
    result = _run(s3, SYMBOLS[:2])
    doc = _puts(s3)[KEY]
    assert result["status"] == "ok" and result["series_shrink"]["verdict"] == "unmeasurable"
    assert sorted(doc["series"]) == SYMBOLS[:2] and doc["carried_series"] == {}


def test_a_carried_series_keeps_its_published_currency(universe):
    """A non-USD symbol outside this run's universe is not relabelled USD by the carry."""
    series = {**PREVIOUS, "XJPN": [["2026-10-15", 3000.0]]}
    s3 = _s3_serving(_previous_doc(series, {**{s: "USD" for s in SYMBOLS}, "XJPN": "JPY"}))
    _run(s3, SYMBOLS)
    doc = _puts(s3)[KEY]
    assert doc["currency"]["XJPN"] == "JPY" and doc["carried_series"] == {"XJPN": "2026-10-15"}


def test_a_dry_run_reports_the_shrink_as_an_error_and_writes_nothing(universe):
    s3 = _s3_serving(_previous_doc())
    result = _run(s3, SYMBOLS[:2], dry_run=True)
    assert result["status"] == "error" and result["series_shrink"]["verdict"] == "shrunk"
    s3.put_object.assert_not_called()
