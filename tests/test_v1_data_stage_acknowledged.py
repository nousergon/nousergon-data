"""The phase-1 acknowledgement list is NAMED, CLOSED and pinned (alpha-engine-config-I10793 option 1).

Brian's ruling 2026-10-18 14:44Z ("1-9 proceed as recommended") accepted option 1:
the two 2026-10-16 weekday rehearsals that entered v1 ``DataPhase2`` are reported as
acknowledged, and the guard stays at zero for every other v1 data-stage execution.

Changing the list below relaxes (or tightens) a phase-exit guard. Doing it needs its
own cited ruling; this test is what makes that a reviewed edit, not a passing one.
"""

from __future__ import annotations

import json

from data_gate import cutover as cut
from data_gate import exit_criteria as xc

ACCOUNT = "111111111111"
WEEKLY = "ne-weekly-freshness-pipeline"


def _arn(machine: str, name: str) -> str:
    return f"arn:aws:states:us-east-1:{ACCOUNT}:execution:{machine}:{name}"


class _Store:
    uri = "file://test"

    def __init__(self, documents):
        self.documents = documents

    def list_keys(self, prefix=""):
        return [k for k in sorted(self.documents) if k.startswith(prefix)]

    def get_bytes(self, key):
        if key not in self.documents:
            raise FileNotFoundError(key)
        return json.dumps(self.documents[key]).encode("utf-8")


def _doc(per: dict[str, int], arns: list[str] | None):
    doc = {
        "cutover_utc": cut.CUTOVER_UTC,
        "executions_since_cutover": sum(per.values()),
        "as_of": "2026-10-17T23:52:00Z",
        "per_state_machine": {
            name: {
                "executions_since_cutover": per.get(name, 0),
                "executions_scanned": 47,
            }
            for name in cut.V1_STATE_MACHINE_NAMES
        },
    }
    if arns is not None:
        doc["data_stage_execution_arns"] = arns
    return doc


def _read(doc):
    return xc.read_v1_data_stage_quiet(_Store({xc.V1_DATA_STAGE_KEY: doc}))


REHEARSALS = [
    _arn(WEEKLY, "rehearsal-2026-10-16-1"),
    _arn(WEEKLY, "rehearsal-2026-10-16-2"),
]


def test_the_list_is_exactly_the_two_ruled_rehearsals():
    assert len(cut.ACKNOWLEDGED_V1_DATA_STAGE_EXECUTIONS) == 2
    assert [(m, n) for m, n, _ in cut.ACKNOWLEDGED_V1_DATA_STAGE_EXECUTIONS] == [
        (WEEKLY, "rehearsal-2026-10-16-1"),
        (WEEKLY, "rehearsal-2026-10-16-2"),
    ]


def test_every_entry_cites_the_ruling_and_names_a_surveyed_machine():
    for machine, name, citation in cut.ACKNOWLEDGED_V1_DATA_STAGE_EXECUTIONS:
        assert machine in cut.V1_STATE_MACHINE_NAMES
        assert "alpha-engine-config-I10793" in citation and "Brian" in citation
        assert "arn:" not in name and "arn:" not in citation


def test_the_live_10_17_document_reads_met_with_both_acknowledged():
    reading = _read(_doc({WEEKLY: 2}, REHEARSALS))
    assert reading.met is True and reading.unmeasurable is False
    assert "0 v1 data-stage execution(s)" in reading.detail
    assert "2 acknowledged" in reading.detail
    assert (
        "rehearsal-2026-10-16-1" in reading.detail
        and "rehearsal-2026-10-16-2" in reading.detail
    )
    assert ACCOUNT not in reading.detail


def test_any_other_execution_still_fails():
    other = _arn(WEEKLY, "rehearsal-2026-10-19-1")
    reading = _read(_doc({WEEKLY: 3}, [*REHEARSALS, other]))
    assert reading.met is False and reading.unmeasurable is False
    assert "1 v1 data-stage execution(s)" in reading.detail


def test_a_same_named_execution_on_another_machine_is_not_acknowledged():
    preopen = "ne-preopen-trading-pipeline"
    reading = _read(_doc({preopen: 1}, [_arn(preopen, "rehearsal-2026-10-16-1")]))
    assert reading.met is False
    assert f"{preopen}=1" in reading.detail


def test_a_document_without_arns_acknowledges_nothing():
    reading = _read(_doc({WEEKLY: 2}, None))
    assert reading.met is False
    assert "acknowledged" not in reading.detail
