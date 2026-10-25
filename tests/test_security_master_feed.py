"""The security master's event feed: roster + committed renames -> alias events.

`alpha-engine-config-I12568` (audit DATA-02), second slice. Cases: rename
chains (`BK`→`BNY`, `FB`→`META`), ticker reuse (`ECHO` before EchoStar took it
resolves to nothing, never EchoStar), share classes on a shared CIK, a shared
CIK the suffix cannot split (withheld and reported), no listing evidence
(reported, never invented), and the committed reticker file itself.
Nothing here fetches or writes.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import jsonschema
import pytest

from collectors import security_master as sm
from collectors import security_master_feed as feed

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = json.loads((REPO / "contracts" / "security_master.schema.json").read_text())
RETICKERS_FILE = REPO / "collectors" / "data" / "sp500_known_retickers.json"
SNAP = dt.date(2026, 10, 22)
BUILT = dt.datetime(2026, 10, 23, 3, 0, tzinfo=dt.timezone.utc)

ROSTER_DOC = {
    "0": {"cik_str": 1390777, "ticker": "BNY", "title": "Bank of New York Mellon Corp"},
    "1": {"cik_str": 1415404, "ticker": "ECHO", "title": "EchoStar Corp"},
    "2": {"cik_str": 1067983, "ticker": "BRK-B", "title": "Berkshire Hathaway Inc"},
    "3": {"cik_str": 1067983, "ticker": "BRK-A", "title": "Berkshire Hathaway Inc"},
    "4": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
    "5": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."},
    "6": {"cik_str": 1326801, "ticker": "META", "title": "Meta Platforms, Inc."},
    "7": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
}
RETICKERS_DOC = {
    "retickers": [
        {"old": "BK", "new": "BNY", "effective": "2026-05-22", "evidence": "fixture"},
        {"old": "SATS", "new": "ECHO", "effective": "2026-06-25", "evidence": "fixture"},
        {"old": "FB", "new": "META", "effective": "2022-06-09", "evidence": "fixture"},
        {"old": "OLDX", "new": "GONE", "effective": "2024-01-02", "evidence": "fixture: GONE is not on the roster"},
    ]
}
FIRST_SEEN = {"BK": "2016-01-04", "SATS": "2016-01-04", "FB": "2016-01-04", "AAPL": "2016-01-04"}


def _master(first_seen=FIRST_SEEN, roster=ROSTER_DOC, retickers=RETICKERS_DOC):
    return feed.build_from_sources(
        roster, retickers, snapshot_date=SNAP, master_version="v-test", available_at=BUILT, first_seen=first_seen
    )


def test_every_row_the_feed_builds_is_valid_against_the_contract():
    rows, _ = _master()
    validator = jsonschema.Draft202012Validator(SCHEMA, format_checker=jsonschema.FormatChecker())
    assert [e.message for r in rows for e in validator.iter_errors(r)] == []


def test_a_rename_chain_keeps_one_security_across_the_ticker_change():
    rows, report = _master()
    bk = sm.resolve(rows, "BK", "2026-05-21")
    assert sm.resolve(rows, "BNY", "2026-05-22") == bk
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "BK", "2026-05-22")
    assert sm.resolve(rows, "1390777", "2020-01-02", alias_type="cik") == bk
    assert sm.resolve(rows, "FB", "2020-01-02") == sm.resolve(rows, "META", "2026-10-21")
    assert report.renames == 3


def test_a_reused_ticker_before_its_current_holder_took_it_resolves_to_nothing():
    rows, _ = _master()
    echostar = sm.resolve(rows, "ECHO", "2026-06-25")
    assert sm.resolve(rows, "SATS", "2026-06-24") == echostar
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "ECHO", "2020-06-01")  # Echo Global Logistics' era: never EchoStar


def test_share_classes_on_one_cik_are_two_securities_and_the_cik_is_ambiguous():
    rows, report = _master()
    a, b = sm.resolve(rows, "BRK-A", SNAP), sm.resolve(rows, "BRK-B", SNAP)
    assert a != b
    with pytest.raises(sm.AmbiguousIdentity):
        sm.resolve(rows, "1067983", SNAP, alias_type="cik")
    assert "BRK-A" not in report.cik_withheld


def test_a_shared_cik_the_suffix_cannot_split_is_withheld_and_reported():
    rows, report = _master()
    assert set(report.cik_withheld) == {"GOOG", "GOOGL"}
    assert sm.resolve(rows, "GOOG", SNAP) != sm.resolve(rows, "GOOGL", SNAP)
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "1652044", SNAP, alias_type="cik")


def test_a_security_with_no_listing_evidence_is_floored_at_the_snapshot():
    rows, report = _master()
    assert "GOOG" in report.listed_on_snapshot and "AAPL" not in report.listed_on_snapshot
    sm.resolve(rows, "GOOG", SNAP)
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "GOOG", SNAP - dt.timedelta(days=1))


def test_a_renamed_security_with_no_listing_evidence_is_reported_not_invented():
    first_seen = {k: v for k, v in FIRST_SEEN.items() if k != "BK"}
    rows, report = _master(first_seen=first_seen)
    assert report.no_listing_evidence == ("BNY",)
    assert "2026-05-22 BK->BNY" in report.unplaced_renames
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "BNY", SNAP)
    assert report.securities == len(ROSTER_DOC) - 1


def test_a_rename_onto_a_ticker_not_on_the_roster_is_reported():
    _, report = _master()
    assert report.unplaced_renames == ("2024-01-02 OLDX->GONE",)


def test_listing_evidence_after_the_rename_contradicts_and_builds_nothing():
    with pytest.raises(sm.IdentityConflict, match="not before its rename"):
        _master(first_seen={**FIRST_SEEN, "BK": "2026-06-01"})


def test_a_ticker_under_two_ciks_on_the_roster_is_refused():
    doc = {**ROSTER_DOC, "9": {"cik_str": 1, "ticker": "AAPL", "title": "imposter"}}
    with pytest.raises(sm.IdentityConflict, match="under CIK"):
        feed.roster_from_company_tickers(doc)


def test_a_ticker_renamed_away_and_back_walks_both_steps():
    renames = [feed.Rename("AAA", "BBB", "2020-01-02"), feed.Rename("BBB", "AAA", "2022-01-03")]
    events, report = feed.feed_events({"AAA": 5}, renames, snapshot_date=SNAP, first_seen={"AAA": "2016-01-04"})
    rows = sm.build_master(events, master_version="v", as_of=SNAP, available_at=BUILT)
    sid = sm.resolve(rows, "AAA", "2019-06-03")
    assert sm.resolve(rows, "BBB", "2021-06-01") == sid == sm.resolve(rows, "AAA", "2022-01-03")
    assert report.renames == 2


def test_ids_do_not_move_when_an_unrelated_security_joins_the_roster():
    rows, _ = _master()
    more = {**ROSTER_DOC, "10": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"}}
    rows2, _ = _master(roster=more)
    assert sm.resolve(rows, "BNY", SNAP) == sm.resolve(rows2, "BNY", SNAP)
    assert sm.resolve(rows, "BRK-B", SNAP) == sm.resolve(rows2, "BRK-B", SNAP)


def test_the_committed_reticker_file_feeds_the_master_without_conflict():
    doc = json.loads(RETICKERS_FILE.read_text())
    renames = feed.renames_from_retickers(doc)
    assert renames and len(renames) == len(doc["retickers"])
    terminal = {r.new for r in renames} - {r.old for r in renames}
    roster = {t: 10_000 + i for i, t in enumerate(sorted(terminal))}
    first_seen = {r.old: "2016-01-04" for r in renames}
    events, report = feed.feed_events(roster, renames, snapshot_date=SNAP, first_seen=first_seen)
    sm.build_master(events, master_version="v", as_of=SNAP, available_at=BUILT)
    assert report.unplaced_renames == () and report.no_listing_evidence == ()
