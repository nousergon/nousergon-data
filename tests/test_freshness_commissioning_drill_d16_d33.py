"""D16 and D33 join the monthly freshness-monitor drill (alpha-engine-config-I10795).

Both units already carried their own live ARTIFACT_REGISTRY row in
``registry_rows`` — D16 ``rag_corpus_freshness``, D33 ``daily_heal_summary`` —
but declared no ``freshness-monitor`` detector, so the drill never touched them
and their ``data.D*.detector`` rows stayed UNMET. The drill role could not read
either row; that grant is nous-ergon-ops-PR1732, which Brian approved on
2026-10-18 (red line 1). This file pins the descriptor half: each unit drills
its OWN row, the receipt reads MET through the real reader, and the probe stays
inside exactly the keys/prefix that grant names.

Separate from ``test_freshness_commissioning_drill.py`` only so it does not
collide with open PRs appending to that file; it reuses its doubles.
"""

from __future__ import annotations

import datetime as dt

from data_gate.producers import freshness_commissioning as drill
from tests.test_freshness_commissioning_drill import (  # noqa: F401 - `units` is a fixture
    DRILL_INSTANT,
    _Clock,
    _detector,
    _FakeS3,
    _read,
    _registry,
    units,
)


class _ListingS3(_FakeS3):
    """Adds the ``list_objects_v2`` paginator a date-templated row is probed with."""

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        outer = self

        class _Paginator:
            def paginate(self, *, Bucket: str, Prefix: str):  # noqa: N803 - boto3 shape
                outer.calls.append(("list", Prefix))
                yield {
                    "Contents": [
                        {"Key": k, "LastModified": lm}
                        for k, lm in sorted(outer.objects.items())
                        if k.startswith(Prefix)
                    ]
                }

        return _Paginator()


#: The exact resources the drill role is granted for these two units
#: (nous-ergon-ops `github-actions-data-collection-phase-exit-metrics`):
#: GetObject on the D16 key and on ``data/heal/daily/*``, ListBucket with
#: ``s3:prefix`` ``data/heal/daily/*``.
D16_KEY = "rag/corpus_freshness/latest.json"
D33_PREFIX = "data/heal/daily/"


def test_d16_drills_its_own_corpus_freshness_row_and_reads_met(units):
    unit = units["D16"]
    assert unit.raw["registry_rows"] == ["rag_corpus_freshness"]
    specs, broken = drill.build_specs(
        _registry(
            artifact_id="rag_corpus_freshness",
            s3_key_template=D16_KEY,
            cadence="saturday_sf",
            sla_minutes_after_cron=480,
        )
    )
    assert not broken
    # Written by the Saturday ingestion two days before the drill's Monday.
    s3 = _FakeS3({D16_KEY: dt.datetime(2026, 10, 31, 11, 0, tzinfo=dt.timezone.utc)})
    receipt, rows, why = drill.drill_unit(
        unit, _detector(unit), specs, s3=s3, clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert why is None and receipt is not None
    assert receipt["outcome"] == "induced", receipt
    assert receipt["fired"]["subject"] == "rag_corpus_freshness"
    assert rows[0]["states"] == {"baseline": "fresh", "induced": "stale", "cleared": "fresh"}
    assert s3.calls == [("head", D16_KEY)] * 3, "the probe must stay on the one granted key"
    assert _read(units, "D16", receipt).met


def test_d33_drills_its_own_daily_heal_row_and_reads_met(units):
    unit = units["D33"]
    assert unit.raw["registry_rows"] == ["daily_heal_summary"]
    assert "data/heal/daily/{date}.json" in unit.raw["writes"]
    specs, broken = drill.build_specs(
        _registry(
            artifact_id="daily_heal_summary",
            s3_key_template="data/heal/daily/{trading_day}.json",
            cadence="weekday_sf",
        )
    )
    assert not broken
    # Monday's 05:00 ET heal writes the previous session's summary.
    key = f"{D33_PREFIX}2026-10-30.json"
    s3 = _ListingS3({key: dt.datetime(2026, 11, 2, 9, 2, tzinfo=dt.timezone.utc)})
    receipt, rows, why = drill.drill_unit(
        unit, _detector(unit), specs, s3=s3, clock=_Clock(DRILL_INSTANT), run_ref="t"
    )
    assert why is None and receipt is not None
    assert receipt["outcome"] == "induced", receipt
    assert receipt["fired"]["subject"] == "daily_heal_summary"
    assert rows[0]["states"] == {"baseline": "fresh", "induced": "stale", "cleared": "fresh"}
    assert s3.calls and all(
        op == "list" and prefix.startswith(D33_PREFIX) for op, prefix in s3.calls
    ), "the probe must stay inside the one granted prefix"
    assert _read(units, "D33", receipt).met


def test_both_detectors_are_unverified_until_the_drill_writes_the_receipt(units):
    for unit_id in ("D16", "D33"):
        detector = _detector(units[unit_id])
        assert detector["via"] == "artifact-registry-row"
        assert detector["state"] == "UNVERIFIED"
        assert "alpha-engine-config-I10795" in detector["note"]
