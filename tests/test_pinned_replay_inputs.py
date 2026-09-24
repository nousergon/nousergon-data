"""A replay reads the input VERSIONS the replayed day's run actually read.

`alpha-engine-config-I11216`. `metron/holdings_universe.json` is overwritten in
place about three times a day. A replay of 2026-09-18 dispatched on 09-20 read
the 09-20 universe (75 instruments) while v1 had read the 09-17 one (117), so
15 symbols appeared in v1's output and in none of the shadow's -- across
`earnings`, `sectors`, `analyst` and `fundamentals`, four artifacts whose only
shared ingredient is that input.

The tests here pin the three properties that matter:

* a DECLARED version (recorded by the producer) beats an inferred one;
* an INFERRED version is the one current at the run's START, not the newest;
* everything else is an explicit UNPINNED answer with a reason, never a guess
  and never an exception.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

import collectors.metron_market_data as mmd
from shadow.pinned_inputs import Pin, pin_for

KEY = "metron/holdings_universe.json"
DAY = dt.date(2026, 9, 18)
UTC = dt.timezone.utc


class FakeS3:
    """Enough S3 to answer a pin: manifests by prefix, versions by key."""

    def __init__(self, manifests: dict[str, dict], versions: list[tuple[str, str]]):
        self._manifests = manifests
        self._versions = [
            (dt.datetime.fromisoformat(when).replace(tzinfo=UTC), vid) for when, vid in versions
        ]
        self.get_calls: list[dict] = []

    def list_objects_v2(self, Bucket, Prefix, **kw):  # noqa: N803
        contents = [{"Key": k} for k in sorted(self._manifests) if k.startswith(Prefix)]
        return {"Contents": contents} if contents else {}

    def get_object(self, Bucket, Key, **kw):  # noqa: N803
        self.get_calls.append({"Key": Key, **kw})
        if Key in self._manifests:
            body = json.dumps(self._manifests[Key]).encode()
        else:
            body = b"{}"

        class _Body:
            def read(self_inner):
                return body

        return {"Body": _Body(), "VersionId": kw.get("VersionId"), "ETag": '"abc"'}

    def get_paginator(self, name):  # noqa: ARG002
        versions = [
            {"Key": KEY, "LastModified": when, "VersionId": vid} for when, vid in self._versions
        ]

        class _Paginator:
            def paginate(self_inner, **kw):
                return [{"Versions": versions}]

        return _Paginator()


def _manifest(started: str, inputs: list[dict] | None = None) -> dict:
    return {"unit_id": "D22", "trading_day": DAY.isoformat(), "started": started, "inputs": inputs or []}


MANIFEST_KEY = f"data_collection/runs/D22/{DAY.isoformat()}/01ABC.json"

# The real version history, trimmed: v1 started 2026-09-18T20:12:15Z, BEFORE
# the day's own universe was republished at 20:51:48Z.
VERSIONS = [
    ("2026-09-17T22:38:26", "v-0917-late"),
    ("2026-09-18T20:51:48", "v-0918-first"),
    ("2026-09-20T22:39:31", "v-0920-late"),
]


def test_an_inferred_pin_is_the_version_current_at_the_run_start():
    s3 = FakeS3({MANIFEST_KEY: _manifest("2026-09-18T20:12:15+00:00")}, VERSIONS)

    pin = pin_for(s3, "b", KEY, unit_id="D22", trading_day=DAY)

    assert pin.basis == "inferred"
    assert pin.version_id == "v-0917-late", "the newest version at or before the start"
    assert "20:12:15" in pin.detail


def test_a_declared_pin_wins_over_inference():
    s3 = FakeS3(
        {MANIFEST_KEY: _manifest("2026-09-18T20:12:15+00:00", [{"key": KEY, "version_id": "v-declared"}])},
        VERSIONS,
    )

    pin = pin_for(s3, "b", KEY, unit_id="D22", trading_day=DAY)

    assert pin.basis == "declared"
    assert pin.version_id == "v-declared"


def test_the_earliest_start_is_used_when_a_unit_ran_twice():
    """A unit that ran twice read its inputs at the FIRST start."""
    s3 = FakeS3(
        {
            f"data_collection/runs/D22/{DAY.isoformat()}/01AAA.json": _manifest("2026-09-18T20:12:15+00:00"),
            f"data_collection/runs/D22/{DAY.isoformat()}/01BBB.json": _manifest("2026-09-18T22:00:00+00:00"),
        },
        VERSIONS,
    )

    pin = pin_for(s3, "b", KEY, unit_id="D22", trading_day=DAY)

    assert pin.version_id == "v-0917-late", "the later run must not pin a version the first never saw"


def test_no_manifest_is_unpinned_with_a_reason_not_an_exception():
    s3 = FakeS3({}, VERSIONS)

    pin = pin_for(s3, "b", KEY, unit_id="D22", trading_day=DAY)

    assert pin.basis == "unpinned"
    assert pin.version_id is None
    assert "no D22 run manifest" in pin.detail


def test_a_version_aged_out_of_retention_is_unpinned_not_guessed():
    """Every retained version is NEWER than the start: say so, don't substitute."""
    s3 = FakeS3(
        {MANIFEST_KEY: _manifest("2026-08-01T20:12:15+00:00")},
        VERSIONS,
    )

    pin = pin_for(s3, "b", KEY, unit_id="D22", trading_day=DAY)

    assert pin.basis == "unpinned"
    assert pin.version_id is None
    assert "aged out" in pin.detail


def test_outside_a_replay_there_is_no_pin(monkeypatch):
    monkeypatch.setattr("shadow.pinned_inputs.active_shadow_root", lambda: None)
    s3 = FakeS3({MANIFEST_KEY: _manifest("2026-09-18T20:12:15+00:00")}, VERSIONS)

    pin = pin_for(s3, "b", KEY, unit_id="D22")

    assert pin.basis == "unpinned"
    assert "no shadow replay active" in pin.detail


def test_the_input_record_never_reads_an_inferred_pin_as_declared():
    inferred = Pin(KEY, "v1", "inferred", "because").as_input_record()
    unpinned = Pin(KEY, None, "unpinned", "because").as_input_record()

    assert inferred["pin_basis"] == "inferred"
    assert inferred["version_capture"] == "pinned_replay"
    assert unpinned["version_capture"] == "not_captured"
    assert unpinned["version_id"] is None


# ---------------------------------------------------------------------------
# The reader that consumes the pin
# ---------------------------------------------------------------------------


def test_the_universe_reader_requests_the_pinned_version(monkeypatch):
    monkeypatch.setattr(
        "shadow.pinned_inputs.pin_for",
        lambda *a, **kw: Pin(KEY, "v-0917-late", "inferred", "because"),
    )
    s3 = FakeS3({}, VERSIONS)
    mmd._read_metron_universe_holdings("b", s3, KEY)

    assert s3.get_calls[-1].get("VersionId") == "v-0917-late"


def test_the_universe_reader_omits_versionid_when_unpinned(monkeypatch):
    """Production must issue an ordinary current-object GET."""
    monkeypatch.setattr(
        "shadow.pinned_inputs.pin_for", lambda *a, **kw: Pin(KEY, None, "unpinned", "no replay")
    )
    s3 = FakeS3({}, VERSIONS)
    mmd._read_metron_universe_holdings("b", s3, KEY)

    assert "VersionId" not in s3.get_calls[-1]


def test_a_pinning_failure_never_breaks_the_production_read(monkeypatch):
    """The pin is an enhancement; losing it must not lose the universe."""
    def _boom(*a, **kw):
        raise RuntimeError("no shadow package here")

    monkeypatch.setattr("shadow.pinned_inputs.pin_for", _boom)
    s3 = FakeS3({}, VERSIONS)

    assert mmd._read_metron_universe_holdings("b", s3, KEY) == []
    assert "VersionId" not in s3.get_calls[-1]
