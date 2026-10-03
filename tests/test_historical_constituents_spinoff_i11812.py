"""A spin-off addition the reference has not listed yet is a DECLARED exception.

2026-10-03, alpha-engine-config-I11812: the first v2 weekly's D02 run failed
with ``observed added VYLR not in reference``. Corteva's seed business spun
off as Vylor and S&P added it to the S&P 500 on its distribution date,
2026-10-01, next to its parent. So the SPY roster went from 503 to 504 names
with no removal, and the Wikipedia changes table had no row for it yet.

These tests pin three things. A committed declaration explains exactly that
disagreement. It explains it only near its effective date and only until its
``valid_through``. And the artifact names what was explained, so the
exception is never silent.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import collectors.historical_constituents as hc
from collectors.historical_constituents import ADDED, REMOVED, ConstituentChange

VYLR = {
    "ticker": "VYLR", "parent": "CTVA", "effective": "2026-10-01",
    "valid_through": "2026-10-31", "evidence": "S&P DJI 2026-10-01 release",
}
MSG = "observed added VYLR not in reference"


def test_the_committed_file_declares_vylr_with_evidence():
    known = json.loads(hc._DECLARED_SPINOFF_ADDITIONS_PATH.read_text())
    entries = {d["ticker"]: d for d in known["spinoff_additions"]}
    assert entries["VYLR"]["parent"] == "CTVA"
    assert entries["VYLR"]["effective"] == "2026-10-01"
    for d in known["spinoff_additions"]:
        assert d["evidence"] and d["parent"] and d["effective"] <= d["valid_through"]


def test_a_declared_spinoff_addition_is_explained_and_named():
    observed = [ConstituentChange("2026-10-01", "VYLR", ADDED)]
    unexplained, declared = hc.declared_spinoff_exceptions(
        [MSG], observed, as_of="2026-10-02", declarations=[VYLR],
    )
    assert unexplained == []
    assert declared == [
        f"{MSG}: declared spin-off of CTVA effective 2026-10-01 "
        "(valid through 2026-10-31)"
    ]


def test_other_disagreements_are_left_alone():
    observed = [
        ConstituentChange("2026-10-01", "VYLR", ADDED),
        ConstituentChange("2026-10-01", "ZZZ", ADDED),
    ]
    found = [MSG, "observed added ZZZ not in reference",
             "reference removed CTVA not observed"]
    unexplained, declared = hc.declared_spinoff_exceptions(
        found, observed, as_of="2026-10-02", declarations=[VYLR],
    )
    assert unexplained == found[1:]
    assert len(declared) == 1


def test_a_removal_is_never_explained_by_an_addition_declaration():
    found = ["observed removed VYLR not in reference"]
    observed = [ConstituentChange("2026-10-01", "VYLR", REMOVED)]
    unexplained, declared = hc.declared_spinoff_exceptions(
        found, observed, as_of="2026-10-02", declarations=[VYLR],
    )
    assert unexplained == found and declared == []


def test_an_addition_far_from_the_effective_date_is_not_the_declared_event():
    observed = [ConstituentChange("2026-12-01", "VYLR", ADDED)]
    unexplained, declared = hc.declared_spinoff_exceptions(
        [MSG], observed, as_of="2026-10-20", declarations=[VYLR],
    )
    assert unexplained == [MSG] and declared == []


def test_a_declaration_past_valid_through_stops_explaining():
    observed = [ConstituentChange("2026-10-01", "VYLR", ADDED)]
    unexplained, declared = hc.declared_spinoff_exceptions(
        [MSG], observed, as_of="2026-11-01", declarations=[VYLR],
    )
    assert unexplained == [MSG] and declared == []


def _collect(snapshots, reference, declarations):
    rosters = hc.RosterSnapshots(snapshots=snapshots, skipped={})
    s3 = MagicMock()
    real = hc.declared_spinoff_exceptions
    with patch.object(hc, "load_roster_snapshots", return_value=rosters), \
         patch.object(hc, "_fetch_changes_table", return_value=(None, "u")), \
         patch.object(hc, "parse_changes_table", return_value=reference), \
         patch.object(hc, "resolve_renames", return_value=hc.RenameResolution()), \
         patch.object(hc, "declared_spinoff_exceptions",
                      side_effect=lambda f, o, *, as_of: real(
                          f, o, as_of=as_of, declarations=declarations)), \
         patch.object(hc.boto3, "client", return_value=s3):
        out = hc.collect("bucket", ["A", "B", "VYLR"])
    return out, json.loads(s3.put_object.call_args.kwargs["Body"])


SNAPS = {"2026-09-29": ["A", "B"], "2026-10-01": ["A", "B", "VYLR"],
         "2026-10-02": ["A", "B", "VYLR"]}


def test_collect_is_ok_and_publishes_the_declared_exception():
    out, written = _collect(SNAPS, [], [VYLR])
    assert out["status"] == "ok"
    assert out["n_reference_disagreements"] == 0
    assert out["n_declared_reference_exceptions"] == 1
    assert written["attestation"]["status"] == "agreed"
    assert written["attestation"]["declared_exceptions"][0].startswith(MSG)
    assert written["quality"]["declared_reference_exceptions"][0].startswith(MSG)


def test_collect_without_a_declaration_is_still_degraded():
    out, written = _collect(SNAPS, [], [])
    assert out["status"] == "degraded"
    assert out["reference_disagreements"] == [MSG]
    assert written["attestation"]["declared_exceptions"] == []
