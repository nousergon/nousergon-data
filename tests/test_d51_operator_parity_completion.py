"""A scheduled D51 panel run is complete without the operator-only parity receipt.

The first scheduled panel run (ne-data-collection-panel, 2026-10-14) published
panel.parquet and manifest.json and failed its completion check with
`output_missing` for `data_collection/panel/{trading_day}/parity.json`. That
receipt is written only by the operator CLI (`builders.daily_panel parity`),
never by the scheduled run, so requiring it failed and paged every run.

The exemption is `completeness.operator_writes`. It covers only the templates
it names, and an entry not also under `writes:` is a descriptor error.
"""

from __future__ import annotations

import json

import pytest

from data_gate import run_manifest_predicate as predicate

DAY = "2026-10-14"
PREFIX = "data_collection/runs/D51"
STARTED = "2000-01-01T00:00:00Z"
PANEL = f"data_collection/panel/{DAY}/panel.parquet"
MANIFEST = f"data_collection/panel/{DAY}/manifest.json"


class MemS3:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = objects

    def get_object(self, Bucket, Key):  # noqa: N803
        payload = self.objects[Key]

        class _Body:
            def read(self_inner):
                return payload

        return {"Body": _Body()}

    def list_objects_v2(self, Bucket, Prefix, StartAfter="", ContinuationToken=None, **kw):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


@pytest.fixture(autouse=True)
def _reset():
    predicate.reset_unit_cache()
    yield
    predicate.reset_unit_cache()


def _run(outputs: list[str]) -> dict:
    doc = {
        "status": "ok",
        "trading_day": DAY,
        "finished": "2026-10-15T00:40:00Z",
        "outputs": [{"key": k, "rows_out": 600} for k in outputs],
        "guards": [],
    }
    s3 = MemS3({f"{PREFIX}/{DAY}/01M4YFBMFPPP3H7NZ5C057T0AP.json": json.dumps(doc).encode()})
    return predicate.completion_check(
        {"collection": "daily-panel", "units": ["D51"], "started_at": STARTED}, s3_client=s3
    )["completion"]


def test_a_scheduled_run_without_the_parity_receipt_passes():
    completion = _run([PANEL, MANIFEST])
    assert completion["ok"] is True, completion["summary"]
    (row,) = completion["units"]
    assert row["operator_skipped"] == [
        "data_collection/panel/{trading_day}/parity.json",
        "data_collection/panel/history/",
    ]


def test_a_run_missing_the_panel_is_still_output_missing():
    completion = _run([MANIFEST])
    assert completion["ok"] is False
    assert [(f["mode"], f["key"]) for f in completion["findings"]] == [
        ("output_missing", "data_collection/panel/{trading_day}/panel.parquet")
    ]


def test_an_operator_write_not_under_writes_is_refused(monkeypatch):
    units = dict(predicate._unit_descriptors())
    raw = dict(units["D51"])
    raw["completeness"] = {**raw["completeness"], "operator_writes": ["data_collection/panel/{trading_day}/typo.json"]}
    units["D51"] = raw
    monkeypatch.setattr(predicate, "_unit_descriptors", lambda: units)
    with pytest.raises(ValueError, match="not under writes"):
        _run([PANEL, MANIFEST])
