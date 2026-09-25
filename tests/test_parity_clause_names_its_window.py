"""The parity clause's detail names the report day AND the remaining window.

`alpha-engine-config-I11185` deliverable 3 / closes-when 3. The clause used to
report only the content verdict (`918/959 published keys match ... (report
parity/2026-09-18.json, trading_day 2026-09-18)`): a reader could not tell
whether the report was one trading day old or four, and a clause that flipped
to UNMET when the report aged out did so with a detail that looked like the day
before's — the `bugclass_a_control_that_reports_the_wrong_thing` shape.

Pinned here, for every branch of `read_parity` that selects a report:

1. the detail names the report's trading day and how many trading days of the
   `PARITY_FRESHNESS_TRADING_DAYS` window remain, with the last trading day the
   report still counts on;
2. an aged-out report says it is failing on FRESHNESS, not content;
3. a fresh report that does not match says it is failing on CONTENT, inside
   the window — so the two causes of UNMET are never the same string.
"""

from __future__ import annotations

import datetime as dt
import json

from nousergon_lib.trading_calendar import add_trading_days, subtract_trading_days

from data_gate import evidence

WINDOW = evidence.PARITY_FRESHNESS_TRADING_DAYS
GATE_DAY = dt.date(2026, 9, 21)  # Monday


class _Store:
    uri = "file://test"

    def __init__(self, documents: dict[str, dict]) -> None:
        self.documents = documents

    def list_keys(self, prefix: str = ""):
        return [k for k in sorted(self.documents) if k.startswith(prefix)]

    def get_bytes(self, key: str) -> bytes:
        if key not in self.documents:
            raise FileNotFoundError(key)
        return json.dumps(self.documents[key]).encode("utf-8")


def _report(day: dt.date, *, total: int = 10, match: int = 10) -> dict:
    summary: dict[str, int] = {"total": total, "match": match, "settling_bar_keys": 0}
    if match < total:
        summary["mismatch"] = total - match
    return {
        "schema_version": "data_parity_report.v2",
        "trading_day": day.isoformat(),
        "generated_at": f"{day.isoformat()}T23:43:04Z",
        "met": match == total,
        "summary": summary,
        "keys": [],
    }


def _read(day: dt.date, **kw) -> evidence.Reading:
    store = _Store({evidence.parity_store_key(day): _report(day, **kw)})
    return evidence.read_parity(store, trading_day=GATE_DAY)


def test_a_same_day_met_report_names_its_day_and_full_remaining_window():
    reading = _read(GATE_DAY)
    assert reading.met is True
    last = add_trading_days(GATE_DAY, WINDOW)
    assert "trading_day 2026-09-21" in reading.detail
    assert f"{WINDOW} of {WINDOW} trading days remaining" in reading.detail
    assert f"counts through {last.isoformat()}" in reading.detail


def test_a_same_day_content_failure_says_content_not_freshness():
    reading = _read(GATE_DAY, match=9)
    assert reading.met is False and reading.unmeasurable is False
    assert f"{WINDOW} of {WINDOW} trading days remaining" in reading.detail
    assert "failing on CONTENT" in reading.detail
    assert "FRESHNESS" not in reading.detail


def test_a_report_behind_the_gate_day_names_the_window_it_has_left():
    prior = subtract_trading_days(GATE_DAY, 2)
    reading = _read(prior)
    assert reading.unmeasurable is True
    assert f"trading_day {prior.isoformat()}" in reading.detail
    assert f"{WINDOW - 2} of {WINDOW} trading days remaining" in reading.detail
    assert f"counts through {add_trading_days(prior, WINDOW).isoformat()}" in reading.detail


def test_a_report_on_the_last_day_of_its_window_has_zero_remaining_but_is_not_stale():
    edge = subtract_trading_days(GATE_DAY, WINDOW)
    reading = _read(edge)
    assert "Stale" not in reading.detail
    assert f"0 of {WINDOW} trading days remaining" in reading.detail
    assert f"counts through {GATE_DAY.isoformat()}" in reading.detail


def test_an_aged_out_report_says_it_fails_on_freshness_not_content():
    stale = subtract_trading_days(GATE_DAY, WINDOW + 1)
    reading = _read(stale)  # a report whose CONTENT is perfect
    assert reading.met is False and reading.unmeasurable is False
    assert f"trading_day {stale.isoformat()}" in reading.detail
    assert f"0 of {WINDOW} trading days remaining" in reading.detail
    assert f"expired after {add_trading_days(stale, WINDOW).isoformat()}" in reading.detail
    assert "failing on FRESHNESS, not content" in reading.detail
    assert "Stale" in reading.detail
