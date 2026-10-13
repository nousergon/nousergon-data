"""An owner's ruling accepts a FULLY ATTRIBUTED frozen parity record, and nothing less.

Brian's ruling of 2026-10-12 on `alpha-engine-config-I12023`
(issuecomment-6127891583): accept the frozen 09-28 parity as "attributed, not
met". The frozen report can never be re-measured, so the ruling is the only
way its attributed breaches stop reading red. These pin that a ruling reads
MET as ``ruled`` (never ``cleared``), keeps every breach printed, and cannot
stand in for an attribution or a measurement that was not made.
"""

from __future__ import annotations

from data_gate import parity_adjudication as adj
from tests.test_parity_adjudication_i12023 import RECORD_0, REPORT_KEY, _grades, _read, _store

RATING = "market_data/technicals/rating_performance.json"


def _breach_rating(record):
    record["exceptions"][0]["inputs"][0]["settled"]["value"] = 175.30  # settled != shadow: a real breach


def _ruling(covers=(RATING,), **over):
    ruling = {
        "disposition": adj.RULING_ATTRIBUTED_NOT_MET,
        "by": "cipher813",
        "at": "2026-10-12T15:59:48Z",
        "ref": "https://github.com/nousergon/alpha-engine-config/issues/12023#issuecomment-6127891583",
        "covers": list(covers),
    }
    ruling.update(over)
    return ruling


def _with(ruling, *mutators):
    def mutate(record):
        for m in mutators:
            m(record)
        record["ruling"] = ruling
    return _store(mutate)


def test_without_a_ruling_an_attributed_breach_stays_red():
    reading = _read(_store(_breach_rating))
    assert reading.met is False
    assert "RULED" not in reading.detail


def test_a_ruling_on_a_fully_attributed_record_reads_met_and_keeps_the_breach_printed():
    store = _with(_ruling(), _breach_rating)
    graded = _grades(store)
    assert graded.ruled is True and graded.cleared is False
    reading = _read(store)
    assert reading.met is True, reading.detail
    assert "RULED attributed_not_met by cipher813" in reading.detail
    assert f"{RATING}: breach" in reading.detail  # the accepted breach is still named
    assert "2/4 published keys match" in reading.detail  # and so is the original failed reading
    assert reading.evidence == (REPORT_KEY, RECORD_0)


def test_a_ruling_must_name_exactly_the_breaching_exceptions():
    for covers in ((), (RATING, "arcticdb/universe"), (RATING, RATING)):
        reading = _read(_with(_ruling(covers), _breach_rating))
        assert reading.met is False, covers
        assert "ruling NOT applied" in reading.detail and "exactly the breaching" in reading.detail


def test_a_ruling_never_stands_in_for_a_pending_or_unattributed_path():
    def pending(record):
        del record["exceptions"][0]["inputs"][0]["settled"]

    def unattributed(record):
        record["exceptions"][0]["unattributed"] = ["$.segments.all.20.1.ic_mean"]
        record["exceptions"][0]["attributions"].pop()

    for mutator in (pending, unattributed):
        reading = _read(_with(_ruling(), _breach_rating, mutator))
        assert reading.met is False
        assert "ruling NOT applied" in reading.detail


def test_a_ruling_needs_its_author_time_link_and_disposition():
    for over in ({"by": ""}, {"at": "yesterday"}, {"ref": "slack thread"}, {"disposition": "waived"}):
        reading = _read(_with(_ruling(**over), _breach_rating))
        assert reading.met is False, over
        assert "ruling NOT applied" in reading.detail


def test_a_ruling_on_a_record_that_already_clears_changes_nothing():
    store = _with(_ruling(covers=()))
    graded = _grades(store)
    assert graded.cleared is True and graded.ruled is False
    assert "ADJUDICATED" in _read(store).detail
