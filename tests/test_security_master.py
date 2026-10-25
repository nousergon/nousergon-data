"""Effective-dated security identity: the table, the producer stub, the resolver.

`alpha-engine-config-I12568` (audit DATA-02), first slice. Cases the issue's
closes-when names: ticker reuse, rename, merger/spinoff (as delist + list),
delisting and share classes; and ambiguity refuses rather than guesses.
No consumer is switched by this slice, so nothing here reads S3.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import jsonschema
import pytest

from collectors import security_master as sm
from contracts import vintage

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = json.loads((REPO / "contracts" / "security_master.schema.json").read_text())
AS_OF = dt.date(2026, 10, 22)
BUILT = dt.datetime(2026, 10, 23, 3, 0, tzinfo=dt.timezone.utc)
L, R, D = "listed", "renamed", "delisted"


def _build(*events: sm.AliasEvent) -> list[dict]:
    return sm.build_master(events, master_version="v-test", as_of=AS_OF, available_at=BUILT)


def _ev(date: str, kind: str, ticker: str, **kw) -> sm.AliasEvent:
    return sm.AliasEvent(date=date, kind=kind, ticker=ticker, source="fixture", **kw)


def test_every_row_is_valid_against_the_contract():
    rows = _build(_ev("2016-01-04", L, "FB", cik=1326801), _ev("2022-06-09", R, "FB", new_ticker="META"))
    validator = jsonschema.Draft202012Validator(SCHEMA, format_checker=jsonschema.FormatChecker())
    for row in rows:
        assert not list(validator.iter_errors(row)), row
    assert {r["alias_type"] for r in rows} == {"ticker", "cik"}


def test_the_contract_declares_a_sound_vintage_contract():
    decl = vintage.load_declaration(SCHEMA)
    assert decl is not None and decl.vintage_field == "master_version" and vintage.replay_supported(decl)


def test_a_rename_keeps_the_security_and_moves_the_ticker():
    rows = _build(_ev("2016-01-04", L, "FB", cik=1326801), _ev("2022-06-09", R, "FB", new_ticker="META"))
    before = sm.resolve(rows, "FB", "2022-06-08")
    assert sm.resolve(rows, "META", "2022-06-09") == before
    assert sm.resolve(rows, "1326801", "2024-01-02", alias_type="cik") == before
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "FB", "2022-06-09")
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "META", "2022-06-08")


def test_a_reused_ticker_is_a_different_security():
    """The current-ticker join DATA-02 names: one symbol, two companies, two eras."""
    rows = _build(
        _ev("2010-01-04", L, "ECHO", cik=1000001),
        _ev("2019-03-20", D, "ECHO"),
        _ev("2024-05-01", L, "ECHO", cik=2000002),
    )
    old, new = sm.resolve(rows, "ECHO", "2015-06-01"), sm.resolve(rows, "ECHO", "2025-06-02")
    assert old != new
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "ECHO", "2020-01-02")  # between the two: nothing, explicitly


def test_a_symbol_freed_and_reissued_on_the_same_day_resolves_to_the_new_security():
    rows = _build(_ev("2010-01-04", L, "XYZ"), _ev("2020-01-02", D, "XYZ"), _ev("2020-01-02", L, "XYZ", cik=7))
    assert sm.resolve(rows, "XYZ", "2019-12-31") != sm.resolve(rows, "XYZ", "2020-01-02")


def test_a_delisting_closes_every_alias_of_the_security():
    rows = _build(_ev("2012-01-03", L, "OLD", cik=42), _ev("2018-07-02", D, "OLD"))
    assert sm.resolve(rows, "OLD", "2018-06-29")
    for alias, kind in (("OLD", "ticker"), ("42", "cik")):
        with pytest.raises(sm.UnresolvedIdentity):
            sm.resolve(rows, alias, "2018-07-02", alias_type=kind)


def test_a_merger_and_a_spinoff_are_a_delisting_and_a_new_listing():
    rows = _build(
        _ev("2010-01-04", L, "PARENT", cik=10),
        _ev("2010-01-04", L, "TARGET", cik=20),
        _ev("2021-03-01", D, "TARGET"),  # merged into PARENT
        _ev("2023-04-03", L, "SPIN", cik=30),  # spun off from PARENT
    )
    ids = {sm.resolve(rows, t, "2024-01-02") for t in ("PARENT", "SPIN")}
    assert len(ids) == 2 and sm.resolve(rows, "TARGET", "2020-12-31") not in ids
    with pytest.raises(sm.UnresolvedIdentity):
        sm.resolve(rows, "TARGET", "2021-03-01")


def test_share_classes_are_distinct_securities_and_their_shared_cik_is_ambiguous():
    rows = _build(
        _ev("1996-05-09", L, "BRK.B", cik=1067983, share_class="B"),
        _ev("1980-03-17", L, "BRK.A", cik=1067983, share_class="A"),
    )
    a, b = sm.resolve(rows, "BRK-A", "2020-01-02"), sm.resolve(rows, "brk.b", "2020-01-02")
    assert a != b
    with pytest.raises(sm.AmbiguousIdentity):
        sm.resolve(rows, "1067983", "2020-01-02", alias_type="cik")


def test_the_id_is_permanent_as_history_grows():
    early = _build(_ev("2016-01-04", L, "FB", cik=1326801))
    later = _build(_ev("2016-01-04", L, "FB", cik=1326801), _ev("2022-06-09", R, "FB", new_ticker="META"))
    assert sm.resolve(early, "FB", "2020-01-02") == sm.resolve(later, "META", "2024-01-02")
    assert sm.resolve(early, "FB", "2020-01-02") == sm.mint_security_id("FB", "2016-01-04", 1326801, None)


def test_overlapping_intervals_for_one_alias_are_refused():
    rows = _build(_ev("2010-01-04", L, "AAA"), _ev("2010-01-04", L, "BBB"))
    clash = [dict(r) for r in rows]
    clash[1] = {**clash[1], "alias_value": "AAA"}
    with pytest.raises(sm.IdentityConflict, match="means both"):
        sm.validate_intervals(clash)
    with pytest.raises(sm.AmbiguousIdentity):
        sm.resolve(clash, "AAA", "2011-01-03")


@pytest.mark.parametrize(
    ("events", "fragment"),
    [
        ((_ev("2020-01-02", R, "NOPE", new_ticker="YES"),), "no security is trading"),
        ((_ev("2020-01-02", D, "NOPE"),), "no security is trading"),
        ((_ev("2020-01-02", L, "AAA"), _ev("2020-06-01", L, "AAA")), "still trading"),
        ((_ev("2020-01-02", L, "AAA"), _ev("2020-01-02", L, "BBB"), _ev("2021-01-04", R, "AAA", new_ticker="BBB")), "held by"),
        ((_ev("2020-01-02", R, "AAA"),), "names no new ticker"),
        ((_ev("2020-13-40", L, "AAA"),), "not an ISO date"),
        ((_ev("2020-01-02", "split", "AAA"),), "unknown event kind"),
    ],
)
def test_contradictory_events_build_nothing(events, fragment):
    with pytest.raises(sm.IdentityConflict, match=fragment):
        _build(*events)


def test_rows_carry_the_point_in_time_stamps():
    (row, *_) = _build(_ev("2016-01-04", L, "FB"))
    assert row["as_of"] == "2026-10-22" and row["available_at"] == "2026-10-23T03:00:00Z"
