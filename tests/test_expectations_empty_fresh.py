"""The common empty-but-fresh + floor guard (`alpha-engine-config-I10785`, P-18).

Before this guard, three of forty-six units guarded against publishing an
empty-but-fresh artifact. These tests are about the two properties that make the
guard worth having: it catches the empty write, and it NEVER reports a pass for
something it could not read.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError
from nousergon_lib.guard_mode import GuardMode

from validators import expectations


class FakeS3:
    def __init__(self, objects: dict[str, int] | None = None, raises: Exception | None = None):
        self.objects = objects or {}
        self.raises = raises

    def head_object(self, Bucket: str, Key: str):  # noqa: N803 -- boto3's own kwarg names
        if self.raises is not None:
            raise self.raises
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": self.objects[Key]}


def _check(**kw):
    base = dict(
        unit_id="D19",
        artifact_key="staging/daily_closes/2026-09-14.parquet",
        bucket="alpha-engine-research",
        s3_client=FakeS3({"staging/daily_closes/2026-09-14.parquet": 4096}),
        rows_out=896,
    )
    base.update(kw)
    return expectations.check_empty_fresh(**base)


def test_a_real_publish_passes():
    reading = _check()
    assert reading.verdict == "ok"
    assert reading.clean
    assert reading.value == 896.0


def test_an_absent_key_under_a_success_claim_is_empty_fresh():
    reading = _check(s3_client=FakeS3({}))
    assert reading.verdict == "empty_fresh"
    assert "does not exist" in reading.detail
    assert not reading.clean


def test_a_zero_byte_object_is_empty_fresh():
    reading = _check(s3_client=FakeS3({"staging/daily_closes/2026-09-14.parquet": 0}))
    assert reading.verdict == "empty_fresh"
    assert "ZERO-BYTE" in reading.detail


def test_zero_rows_is_empty_fresh_even_when_the_object_is_not_empty():
    reading = _check(rows_out=0)
    assert reading.verdict == "empty_fresh"


def test_below_a_declared_floor_is_its_own_verdict():
    reading = _check(rows_out=400, floor=880)
    assert reading.verdict == "below_floor"
    assert reading.value == 400.0
    assert reading.baseline == 880.0


def test_an_unreported_row_count_is_unmeasurable_never_a_pass_and_never_zero():
    """The whole reason `rows_key` is declared per unit rather than guessed."""
    reading = _check(rows_out=None)
    assert reading.verdict == "unmeasurable"
    assert not reading.clean
    assert "not a pass" in reading.detail


def test_the_verdict_names_which_field_the_row_count_was_read_from():
    """A floor check silently trusting a mis-counted `rows_out` fires falsely
    (`alpha-engine-config-I10785`) — the verdict must say which number it
    read, so a false fire is diagnosable from the manifest alone."""
    reading = _check(rows_out=None, rows_key="rows_inserted")
    assert "result['rows_inserted']" in reading.detail

    reading = _check(rows_out=40, rows_key="rows_inserted")
    assert "result['rows_inserted']" in reading.detail

    # No `rows_key` supplied (a literal count passed directly) names that too,
    # rather than silently omitting the provenance.
    reading = _check(rows_out=None)
    assert "the caller's own count" in reading.detail


def test_a_denied_head_is_unmeasurable_not_a_pass():
    denied = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
    reading = _check(s3_client=FakeS3(raises=denied))
    assert reading.verdict == "unmeasurable"
    assert not reading.clean


def test_a_unit_with_no_single_stable_key_is_not_applicable():
    reading = _check(artifact_key=None)
    assert reading.verdict == "not_applicable"
    assert reading.clean


# ── observe-mode staging (`sf-pipeline-policy` §7a) ───────────────────────


def test_the_guard_ships_observing_with_its_promotion_criterion_and_tracker():
    guard = expectations.EMPTY_FRESH_GUARD
    assert guard.mode is GuardMode.OBSERVE
    assert not guard.enforcing
    assert "10 consecutive clean" in guard.promotion_criterion
    assert guard.tracked_issue == "alpha-engine-config-I10785"


def test_observe_mode_is_loud(caplog):
    """§7a rule 3: a verdict nobody reads is a suppression, not an observation."""
    reading = _check(s3_client=FakeS3({}))
    with caplog.at_level("ERROR"):
        expectations.report(reading, unit_id="D19")
    assert "data_empty_fresh" in caplog.text
    assert "mode=observe" in caplog.text


def test_a_clean_verdict_does_not_shout(caplog):
    with caplog.at_level("ERROR"):
        expectations.report(_check(), unit_id="D19")
    assert caplog.text == ""


def test_unmeasurable_is_not_clean_so_it_cannot_satisfy_the_promotion_criterion():
    """A cycle the guard could not read is not a clean cycle."""
    assert not expectations.GuardReading("unmeasurable", "x").clean
    assert expectations.GuardReading("ok", "x").clean
    assert expectations.GuardReading("not_applicable", "x").clean


# ── the board row ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "verdict, status",
    [
        ("ok", "GREEN"),
        ("empty_fresh", "RED"),
        ("below_floor", "RED"),
        ("unmeasurable", "N/A-MISSING-INPUT"),
        ("not_applicable", "N/A-NOT-IMPL"),
    ],
)
def test_every_verdict_renders_a_metric_record_including_the_passes(verdict, status):
    reading = expectations.GuardReading(verdict, "detail", key="k", value=1.0)
    record = expectations.verdict_metric("D19", reading, source_path="tests")
    assert record.name == "data.D19.guard.empty_fresh"
    assert record.status == status


def test_the_verdict_vocabulary_matches_the_manifest_contract():
    from nousergon_lib import contracts

    schema = contracts.load_schema("data_run_manifest")
    enum = schema["$defs"]["GuardVerdict"]["properties"]["verdict"]["enum"]
    assert set(enum) == set(expectations.VERDICTS)
