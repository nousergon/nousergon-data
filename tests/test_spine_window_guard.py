"""The EOD spine-window guard (`alpha-engine-config-I10780`, plan item P-13).

`check_cardinality` grades ONE session's symbol set. These tests are about the
two things it cannot see and `check_spine_window` exists for: a session the EOD
run never published, and a symbol published with a bar from an earlier session.
The four cases the issue names — complete spine, missing symbol, missing session,
empty-but-fresh — plus the shapes that must never read as a pass.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pytest
from botocore.exceptions import ClientError
from nousergon_lib.dates import trading_days_stale
from nousergon_lib.guard_mode import GuardMode

from collectors import metron_market_data
from validators import expectations

SESSION = "2026-10-02"
WINDOW = ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02"]
UNIVERSE = ["AAPL", "MSFT", "FNILX", "D05", "912810UJ5", "ATAI.CVR"]
EXCLUSIONS = {
    "912810UJ5": {"class": "fixed_income_cusip", "reason": "r", "owner": "brian", "re_exam": "2026-12-14"}
}
SUFFIX_MAP = {"D05": "D05.SI"}


def _lag(bar: str, session: str) -> int:
    return trading_days_stale(dt.date.fromisoformat(bar), session)


def _doc(day: str, *, closes: dict | None = None) -> dict:
    if closes is None:
        closes = {
            "AAPL": {"close": 1.0, "currency": "USD", "bar_date": day},
            "MSFT": {"close": 1.0, "currency": "USD", "bar_date": day},
            # A mutual-fund NAV strikes after the fetch: one session behind, every day.
            "FNILX": {"close": 1.0, "currency": "USD", "bar_date": metron_prev(day)},
            "D05.SI": {"close": 1.0, "currency": "SGD", "bar_date": day},
        }
    return {"schema_version": 1, "as_of": day, "source": "alpha-engine-data", "closes": closes}


def metron_prev(day: str) -> str:
    from nousergon_lib.dates import previous_trading_day

    return previous_trading_day(dt.date.fromisoformat(day)).isoformat()


def _window(**overrides) -> dict:
    docs = {d: _doc(d) for d in WINDOW}
    docs.update(overrides)
    return docs


def _check(documents, *, max_lag: int = 1):
    return expectations.check_spine_window(
        unit_id="D20",
        session=SESSION,
        documents=documents,
        denominator_symbols=UNIVERSE,
        max_bar_lag_sessions=max_lag,
        bar_lag=_lag,
        exclusions=EXCLUSIONS,
        suffix_map=SUFFIX_MAP,
        class_rules=expectations.load_class_rules(),
    )


# ── The four named cases ────────────────────────────────────────────────────


def test_a_complete_spine_is_ok():
    reading = _check(_window())
    assert reading.verdict == "ok", reading.detail
    assert reading.value == 1.0
    assert "5/5 session(s) published" in reading.detail
    # The class rule and the declared exclusion explain the two unpriced symbols.
    assert "ATAI.CVR (contingent_value_right)" in reading.detail
    assert "zero undeclared misses" in reading.detail


def test_a_missing_symbol_is_below_floor_and_named():
    closes = dict(_doc(SESSION)["closes"])
    del closes["MSFT"]
    reading = _check(_window(**{SESSION: _doc(SESSION, closes=closes)}))
    assert reading.verdict == "below_floor"
    assert "UNDECLARED miss(es): MSFT" in reading.detail
    assert reading.value == pytest.approx(3 / 4)


def test_a_missing_session_is_below_floor_and_named():
    reading = _check(_window(**{"2026-09-30": None}))
    assert reading.verdict == "below_floor"
    assert "MISSING session(s): 2026-09-30" in reading.detail
    assert "4/5 session(s)" in reading.detail
    # The symbol axis on the graded session is still clean — the finding is the gap.
    assert reading.value == 1.0


def test_the_graded_session_empty_but_fresh_is_empty_fresh():
    reading = _check(_window(**{SESSION: _doc(SESSION, closes={})}))
    assert reading.verdict == "empty_fresh"
    assert f"EMPTY-but-fresh session(s): {SESSION}" in reading.detail
    assert reading.value == 0.0


# ── Shapes that must never read as a pass ───────────────────────────────────


def test_an_empty_earlier_session_is_below_floor_not_empty_fresh():
    """`empty_fresh` is attributed to the run that carries it
    (`data_gate/evidence.py::empty_fresh_runs`); an earlier session's empty spine
    was not this run's write."""
    reading = _check(_window(**{"2026-09-29": {}}))
    assert reading.verdict == "below_floor"
    assert "EMPTY-but-fresh session(s): 2026-09-29" in reading.detail


def test_a_misdated_session_is_below_floor():
    """Another day's spine under this day's key is not this day's spine."""
    reading = _check(_window(**{"2026-10-01": _doc("2026-09-30") | {"as_of": "2026-09-30"}}))
    assert reading.verdict == "below_floor"
    assert "MISDATED session(s): 2026-10-01 (as_of '2026-09-30')" in reading.detail


def test_a_bar_older_than_the_lag_is_not_covered():
    closes = dict(_doc(SESSION)["closes"])
    closes["AAPL"] = {"close": 1.0, "currency": "USD", "bar_date": "2026-09-30"}
    reading = _check(_window(**{SESSION: _doc(SESSION, closes=closes)}))
    assert reading.verdict == "below_floor"
    assert "AAPL@2026-09-30 (lag 2)" in reading.detail
    assert "UNDECLARED miss(es): AAPL" in reading.detail


def test_lag_zero_counts_the_nav_lag_as_a_miss():
    reading = _check(_window(), max_lag=0)
    assert reading.verdict == "below_floor"
    assert "FNILX@2026-10-01 (lag 1)" in reading.detail


def test_an_unreadable_bar_date_is_not_covered():
    closes = dict(_doc(SESSION)["closes"])
    closes["AAPL"] = {"close": 1.0, "currency": "USD", "bar_date": "not-a-date"}
    reading = _check(_window(**{SESSION: _doc(SESSION, closes=closes)}))
    assert reading.verdict == "below_floor"
    assert "AAPL@not-a-date (lag unreadable)" in reading.detail


def test_an_empty_denominator_is_unmeasurable_never_a_pass():
    reading = expectations.check_spine_window(
        unit_id="D20", session=SESSION, documents=_window(), denominator_symbols=[],
        max_bar_lag_sessions=1, bar_lag=_lag, exclusions={}, suffix_map={}, class_rules=[],
    )
    assert reading.verdict == "unmeasurable"
    assert not reading.clean


def test_documents_must_include_the_graded_session():
    with pytest.raises(ValueError, match="must include the graded session"):
        _check({d: _doc(d) for d in WINDOW[:-1]})


def test_the_staging_is_observe_with_a_tracker():
    staging = expectations.SPINE_WINDOW_GUARD
    assert staging.mode is GuardMode.OBSERVE
    assert staging.tracked_issue == "alpha-engine-config-I10780"
    assert staging.name != expectations.CARDINALITY_GUARD.name


# ── The reader ──────────────────────────────────────────────────────────────


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, error_on: str | None = None):
        self.objects = objects
        self.error_on = error_on
        self.puts: list[tuple[str, dict]] = []

    def get_object(self, Bucket, Key):  # noqa: N803
        if self.error_on and self.error_on in Key:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        raw = Body if isinstance(Body, (bytes, bytearray)) else Body.encode("utf-8")
        self.objects[Key] = bytes(raw)
        self.puts.append((Key, json.loads(raw)))
        return {"ETag": '"abc"'}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}


def _key(day: str) -> str:
    return f"market_data/eod_closes/{day}.json"


def test_the_reader_maps_404_to_none_and_zero_bytes_to_empty():
    s3 = FakeS3({_key("2026-10-01"): json.dumps(_doc("2026-10-01")).encode(), _key("2026-10-02"): b""})
    docs = expectations.read_spine_window(s3, "b", ["2026-09-30", "2026-10-01", "2026-10-02"])
    assert docs["2026-09-30"] is None
    assert docs["2026-10-01"]["as_of"] == "2026-10-01"
    assert docs["2026-10-02"] == {}


def test_the_reader_raises_on_anything_but_a_404():
    s3 = FakeS3({}, error_on="2026-10-01")
    with pytest.raises(ClientError):
        expectations.read_spine_window(s3, "b", ["2026-10-01"])


# ── Wiring at the D20 collector (observe mode) ──────────────────────────────

HOLDINGS = [
    {"yf_symbol": "AAPL", "currency": "USD"},
    {"yf_symbol": "MSFT", "currency": "USD"},
]


@pytest.fixture
def universe(monkeypatch):
    monkeypatch.setattr(metron_market_data, "load_metron_universe", lambda bucket, s3: (HOLDINGS, ["USD"]))


def _collect(s3, *, priced, run_date=SESSION):
    return metron_market_data.collect(
        bucket="alpha-engine-research",
        run_date=run_date,
        s3_client=s3,
        close_source=lambda symbols: {s: (100.0, run_date) for s in priced},
        fx_source=lambda ccys: {c: 1.0 for c in ccys},
    )


def _spine_entry(result: dict) -> dict:
    entries = [g for g in result["guards"] if g["guard"] == expectations.SPINE_WINDOW_GUARD.name]
    assert len(entries) == 1, result["guards"]
    return entries[0]


def _prior_spines(*, skip: str | None = None) -> dict[str, bytes]:
    out = {}
    for day in WINDOW[:-1]:
        if day == skip:
            continue
        closes = {s: {"close": 1.0, "currency": "USD", "bar_date": day} for s in ("AAPL", "MSFT")}
        out[_key(day)] = json.dumps(_doc(day, closes=closes)).encode()
    return out


def test_the_collector_files_an_ok_spine_window_reading(universe):
    s3 = FakeS3(_prior_spines())
    result = _collect(s3, priced=["AAPL", "MSFT"])
    assert result["status"] == "ok"
    entry = _spine_entry(result)
    assert entry["verdict"] == "ok", entry["detail"]
    assert entry["mode"] == "observe"
    assert entry["key"] == _key(SESSION)
    # It read BACK this run's own write, not the in-memory artifact.
    assert "5/5 session(s)" in entry["detail"]


def test_the_collector_names_a_missing_earlier_session_without_failing_the_run(universe):
    s3 = FakeS3(_prior_spines(skip="2026-09-29"))
    result = _collect(s3, priced=["AAPL", "MSFT"])
    assert result["status"] == "ok", "OBSERVE mode: no verdict moves the run's status"
    entry = _spine_entry(result)
    assert entry["verdict"] == "below_floor"
    assert "MISSING session(s): 2026-09-29" in entry["detail"]


def test_a_read_failure_is_unmeasurable_and_never_fails_the_run(universe, caplog):
    s3 = FakeS3(_prior_spines(), error_on="2026-09-30")
    result = _collect(s3, priced=["AAPL", "MSFT"])
    assert result["status"] == "ok"
    entry = _spine_entry(result)
    assert entry["verdict"] == "unmeasurable"
    assert "AccessDenied" in entry["detail"]
    assert any("data_spine_window" in r.message for r in caplog.records)


def test_a_failed_write_still_files_a_spine_window_reading(universe):
    class WriteFails(FakeS3):
        def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
            if Key.startswith("market_data/"):
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
            return super().put_object(Bucket, Key, Body, ContentType, **kw)

    s3 = WriteFails(_prior_spines())
    result = _collect(s3, priced=["AAPL", "MSFT"])
    assert result["status"] == "error"
    entry = _spine_entry(result)
    assert entry["verdict"] == "below_floor"
    assert f"MISSING session(s): {SESSION}" in entry["detail"]


def test_the_proposed_values_are_the_ones_the_collector_grades_with():
    """Threshold values are Brian's; these are the PROPOSED ones, pinned so a change
    to either is a visible diff in review rather than a silent edit."""
    assert metron_market_data.D20_SPINE_WINDOW_SESSIONS == 5
    assert metron_market_data.D20_MAX_BAR_LAG_SESSIONS == 1
