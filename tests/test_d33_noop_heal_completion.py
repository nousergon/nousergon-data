"""A daily heal that found nothing to heal is a completed run, not a missing output
(alpha-engine-config-I11812, D33 first-run readiness).

`ne-data-collection-daily-heal` names D33 in `verify_units`, and the completion
predicate requires a manifest output for every S3 template D33's descriptor
`writes:`. One of them, `staging/daily_closes/*`, is written once per day the heal
actually healed. On a day with nothing to heal the manifest is honest and `ok`
(heal summary + `empty_fresh` guard, zero rows), and the machine used to fail
and page with `output_missing` for a key that was correctly never written.

The exemption is `completeness.conditional_writes` in the descriptor and it is
deliberately narrow: it holds only while the manifest's own guard reading says
the unit measured ZERO rows. A run that reports rows and records no staging key
is still `output_missing`.
"""

from __future__ import annotations

import json

import pytest

import run_units
import weekly_collector
from data_gate import run_manifest_predicate as predicate

BUCKET = "alpha-engine-research"
DAY = "2026-10-02"
STARTED = "2000-01-01T00:00:00Z"


class MemS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {"ETag": '"e"'}

    def get_object(self, Bucket, Key):  # noqa: N803
        payload = self.objects[Key]

        class _Body:
            def read(self_inner):
                return payload

        return {"Body": _Body()}

    def head_object(self, Bucket, Key):  # noqa: N803
        return {"ContentLength": 1, "ETag": '"e"'}

    def list_objects_v2(self, Bucket, Prefix, StartAfter="", ContinuationToken=None, **kw):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


class _Args:
    date = DAY
    dry_run = False


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "a" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    predicate.reset_unit_cache()


def _heal_run(monkeypatch, healed_days: list[dict]) -> MemS3:
    s3 = MemS3()
    monkeypatch.setattr(
        run_units,
        "manifest_sink",
        lambda bucket, s3_client=None: run_units.S3ManifestSink(
            bucket=bucket, prefix=run_units.DEFAULT_MANIFEST_PREFIX, s3_client=s3
        ),
    )
    result = {
        "status": "ok",
        "date": DAY,
        "days_healed": len(healed_days),
        "collectors": {"universe_gap_heal": {"status": "ok", "healed_days": healed_days}},
    }
    weekly_collector._run_whole_mode_unit(
        "daily_heal", lambda config, args: result, {"bucket": BUCKET}, _Args()
    )
    return s3


def _verify(s3: MemS3) -> dict:
    return predicate.completion_check(
        {"collection": "daily-heal", "units": ["D33"], "started_at": STARTED}, s3_client=s3
    )["completion"]


def test_a_heal_that_found_nothing_passes_the_completion_check(monkeypatch):
    s3 = _heal_run(monkeypatch, [])
    (manifest_key,) = [k for k in s3.objects if k.startswith(f"data_collection/runs/D33/{DAY}/")]
    manifest = json.loads(s3.objects[manifest_key])
    assert manifest["status"] == "ok"
    assert [o["key"] for o in manifest["outputs"]] == [f"data/heal/daily/{DAY}.json", "arcticdb/universe"]

    completion = _verify(s3)
    assert completion["ok"] is True, completion["summary"]
    (row,) = completion["units"]
    assert row["conditional_skipped"] == ["staging/daily_closes/*"]


def test_a_heal_that_healed_a_day_is_graded_on_the_staging_key_too(monkeypatch):
    s3 = _heal_run(monkeypatch, [{"date": "2026-10-01", "kind": "fallback_quality", "tickers": 903}])
    completion = _verify(s3)
    assert completion["ok"] is True, completion["summary"]
    (row,) = completion["units"]
    assert row["conditional_skipped"] == []


def test_rows_reported_without_the_staging_key_is_still_output_missing(monkeypatch):
    s3 = _heal_run(monkeypatch, [{"date": "2026-10-01", "kind": "fallback_quality", "tickers": 903}])
    (manifest_key,) = [k for k in s3.objects if k.startswith(f"data_collection/runs/D33/{DAY}/")]
    manifest = json.loads(s3.objects[manifest_key])
    manifest["outputs"] = [o for o in manifest["outputs"] if not o["key"].startswith("staging/")]
    s3.objects[manifest_key] = json.dumps(manifest).encode()

    completion = _verify(s3)
    assert completion["ok"] is False
    assert [(f["mode"], f["key"]) for f in completion["findings"]] == [
        ("output_missing", "staging/daily_closes/*")
    ]


def test_a_zero_row_heal_that_also_lost_its_summary_key_still_fails(monkeypatch):
    """The exemption covers ONLY the declared conditional template."""
    s3 = _heal_run(monkeypatch, [])
    (manifest_key,) = [k for k in s3.objects if k.startswith(f"data_collection/runs/D33/{DAY}/")]
    manifest = json.loads(s3.objects[manifest_key])
    manifest["outputs"] = [o for o in manifest["outputs"] if not o["key"].startswith("data/heal/")]
    s3.objects[manifest_key] = json.dumps(manifest).encode()

    completion = _verify(s3)
    assert [(f["mode"], f["key"]) for f in completion["findings"]] == [
        ("output_missing", "data/heal/daily/{date}.json")
    ]

def test_the_recorded_guard_name_is_the_librarys():
    from validators import expectations

    assert predicate.EMPTY_FRESH_GUARD_RECORDED_NAME == expectations.EMPTY_FRESH_GUARD.name
