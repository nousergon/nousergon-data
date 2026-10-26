"""A failed CRSP-basis build writes nothing; a rebuild refuses an unverified library.

alpha-engine-config-I12684. The 2026-10-26 `universe-crsp-basis-build` failed its
reconcile gate and still left `universe_crsp` holding every name it built, because
the `--apply` write pass ran before the audit and the fail-loud raise. The EDGAR
rebuild's only guard was that the library was CURRENT with `universe`, which a
failed build's output is.

Now:
1. a build that will raise writes nothing to the scratch library, and still writes
   its audit and removes its spill files;
2. a passing `--apply` build writes the library, then a `verified` receipt under
   the audit prefix;
3. `edgar_pit_fundamentals` refuses a non-live `Close` library with no receipt,
   with a receipt a later failed build superseded, or with a library written past
   what the receipt covers, and passes with a valid one.

Every frame and price here is synthetic.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m
from collectors import edgar_pit_fundamentals as e
from tests.test_migrate_universe_crsp_basis import _clean_setup, _patch_orchestration

LIB = "universe_crsp"


class _FakeS3:
    """In-memory S3: the builder's put_object and the collector's list/get."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []

    def put_object(self, *, Bucket, Key, Body, ContentType=None):  # noqa: N803 - boto3 shape
        self.objects[Key] = Body
        self.puts.append(Key)

    def get_object(self, *, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise KeyError(f"NoSuchKey {Key}")
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        objects = self.objects

        class _Pages:
            def paginate(self, *, Bucket, Prefix):  # noqa: N803
                keys = sorted(k for k in objects if k.startswith(Prefix))
                return [{"Contents": [{"Key": k} for k in keys]}]

        return _Pages()

    def json(self, key: str) -> dict:
        return json.loads(self.objects[key])

    def audits(self) -> list[str]:
        return [k for k in self.puts if k.startswith(m.AUDIT_PREFIX) and "/verified/" not in k]


class _Clock(datetime):
    """`datetime.now` that advances a minute per call, so audit keys never collide."""

    _t = datetime(2026, 10, 26, 6, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        cls._t = cls._t + timedelta(minutes=1)
        return cls._t


@pytest.fixture
def s3(monkeypatch) -> _FakeS3:
    fake = _FakeS3()
    monkeypatch.setattr(m, "datetime", _Clock)
    return fake


def _build(monkeypatch, s3: _FakeS3, tmp_path, *, fail: bool, scratch=None):
    raw_frames, old_closes, client = _clean_setup()
    if fail:
        # Old Close diverges beyond tolerance with no acknowledged divergence.
        old_closes["X"] = pd.Series([40.0, 40.0, 50.0, 50.0, 50.0, 50.0],
                                    index=raw_frames["X"].index)
    _, scratch = _patch_orchestration(monkeypatch, old_closes=old_closes, scratch_lib=scratch)
    monkeypatch.setattr(m.boto3, "client", lambda *a, **k: s3)
    spill = tmp_path / "spill"
    kwargs = dict(
        apply=True, raw_fetch=lambda t: raw_frames[t], client=client, rel_tol=0.02,
        workers=1, spill_dir=str(spill), scratch_lib=LIB,
        macro={}, sector_map={}, fundamentals={}, alt_data={},
    )
    return kwargs, scratch, spill, raw_frames["X"].index


# ── the build ────────────────────────────────────────────────────────────────


def test_a_failing_build_writes_nothing_to_scratch_and_still_writes_its_audit(
    monkeypatch, s3, tmp_path,
):
    kwargs, scratch, spill, _ = _build(monkeypatch, s3, tmp_path, fail=True)

    with pytest.raises(RuntimeError, match="FAILED LOUD"):
        m.migrate_universe_crsp_basis(**kwargs)

    scratch.write.assert_not_called()
    assert len(s3.audits()) == 1
    audit = s3.json(s3.audits()[0])
    assert audit["status"] == "fail" and audit["applied"] is True
    assert audit["unexplained_count"] == 1 and audit["written_count"] == 0
    # No receipt of either kind: the library was never touched.
    assert m.verified_receipt_key(LIB) not in s3.objects
    # Spill files are removed on the failing path too.
    assert os.listdir(spill) == []


def test_a_failing_build_leaves_a_previous_receipt_but_the_rebuild_refuses_it(
    monkeypatch, s3, tmp_path,
):
    kwargs, scratch, _, _ = _build(monkeypatch, s3, tmp_path, fail=False)
    m.migrate_universe_crsp_basis(**kwargs)
    good = s3.json(m.verified_receipt_key(LIB))

    kwargs, scratch, _, idx = _build(monkeypatch, s3, tmp_path, fail=True)
    with pytest.raises(RuntimeError, match="FAILED LOUD"):
        m.migrate_universe_crsp_basis(**kwargs)
    scratch.write.assert_not_called()
    assert s3.json(m.verified_receipt_key(LIB)) == good

    store = e.S3ObjectStore(s3, "bucket")
    with pytest.raises(e.GateFailed, match="later --apply build"):
        e.assert_close_library_verified(store, library=LIB, newest_session=idx.max().date())


def test_a_passing_build_writes_scratch_then_a_verified_receipt(monkeypatch, s3, tmp_path):
    kwargs, scratch, spill, idx = _build(monkeypatch, s3, tmp_path, fail=False)

    result = m.migrate_universe_crsp_basis(**kwargs)

    assert result["status"] == "ok" and result["written_count"] == 1
    scratch.write.assert_called_once()
    assert os.listdir(spill) == []
    key = m.verified_receipt_key(LIB)
    assert key.startswith(m.AUDIT_PREFIX)  # the only prefix the build box may PUT under
    # Order: invalidate (writing) → audit → verified.
    audit_key = s3.audits()[0]
    assert s3.puts == [key, audit_key, key]
    receipt = s3.json(key)
    assert receipt["status"] == m.RECEIPT_VERIFIED
    assert receipt["scratch_lib"] == LIB
    assert receipt["audit_key"] == audit_key
    assert receipt["unexplained_count"] == 0
    assert receipt["errors_count"] == 0 and receipt["aborted_count"] == 0
    assert receipt["written_count"] == 1
    assert receipt["full_universe"] is True
    assert receipt["newest_session"] == idx.max().date().isoformat()


def test_a_scratch_write_error_leaves_the_receipt_at_writing(monkeypatch, s3, tmp_path):
    from unittest.mock import MagicMock

    broken = MagicMock()
    broken.write.side_effect = OSError("disk full")
    kwargs, _, spill, idx = _build(monkeypatch, s3, tmp_path, fail=False, scratch=broken)

    with pytest.raises(RuntimeError, match="per-ticker error"):
        m.migrate_universe_crsp_basis(**kwargs)

    assert s3.json(m.verified_receipt_key(LIB))["status"] == m.RECEIPT_WRITING
    assert os.listdir(spill) == []
    store = e.S3ObjectStore(s3, "bucket")
    with pytest.raises(e.GateFailed, match="not 'verified'"):
        e.assert_close_library_verified(store, library=LIB, newest_session=idx.max().date())


def test_a_dry_run_writes_no_receipt(monkeypatch, s3, tmp_path):
    kwargs, scratch, _, _ = _build(monkeypatch, s3, tmp_path, fail=False)
    kwargs["apply"] = False
    m.migrate_universe_crsp_basis(**kwargs)
    scratch.write.assert_not_called()
    assert m.verified_receipt_key(LIB) not in s3.objects


# ── the rebuild's gate ───────────────────────────────────────────────────────


def test_the_two_modules_agree_on_the_receipt_key():
    assert e.CRSP_BUILD_AUDIT_PREFIX == m.AUDIT_PREFIX
    assert e.close_library_receipt_key(LIB) == m.verified_receipt_key(LIB)


def test_the_rebuild_refuses_a_library_with_no_receipt():
    store = e.S3ObjectStore(_FakeS3(), "bucket")
    with pytest.raises(e.GateFailed, match="no readable pass receipt"):
        e.assert_close_library_verified(store, library=LIB, newest_session=dt.date(2026, 6, 8))


def test_the_rebuild_passes_a_verified_library(monkeypatch, s3, tmp_path):
    kwargs, _, _, idx = _build(monkeypatch, s3, tmp_path, fail=False)
    m.migrate_universe_crsp_basis(**kwargs)

    store = e.S3ObjectStore(s3, "bucket")
    reading = e.assert_close_library_verified(store, library=LIB, newest_session=idx.max().date())
    assert reading["receipt_key"] == m.verified_receipt_key(LIB)
    assert reading["audit_key"] == s3.audits()[0]
    assert reading["newest_session"] == idx.max().date().isoformat()

    # A later dry run (applied=False) does not supersede the receipt.
    kwargs["apply"] = False
    m.migrate_universe_crsp_basis(**kwargs)
    assert len(s3.audits()) == 2
    e.assert_close_library_verified(store, library=LIB, newest_session=idx.max().date())


def test_the_rebuild_refuses_a_library_written_past_its_receipt(monkeypatch, s3, tmp_path):
    kwargs, _, _, idx = _build(monkeypatch, s3, tmp_path, fail=False)
    m.migrate_universe_crsp_basis(**kwargs)
    store = e.S3ObjectStore(s3, "bucket")
    later = idx.max().date() + dt.timedelta(days=1)
    with pytest.raises(e.GateFailed, match="written after the verified build"):
        e.assert_close_library_verified(store, library=LIB, newest_session=later)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        ("scratch_lib", "universe_other", "names library"),
        ("unexplained_count", 46, "unexplained_count is 46"),
        ("full_universe", False, "whole-universe"),
        ("audit_key", f"{m.AUDIT_PREFIX}19990101T000000Z.json", "not under"),
    ],
)
def test_the_rebuild_refuses_a_receipt_that_does_not_vouch(
    monkeypatch, s3, tmp_path, field, value, match,
):
    kwargs, _, _, idx = _build(monkeypatch, s3, tmp_path, fail=False)
    m.migrate_universe_crsp_basis(**kwargs)
    key = m.verified_receipt_key(LIB)
    receipt = s3.json(key)
    receipt[field] = value
    s3.objects[key] = json.dumps(receipt).encode()
    store = e.S3ObjectStore(s3, "bucket")
    with pytest.raises(e.GateFailed, match=match):
        e.assert_close_library_verified(store, library=LIB, newest_session=idx.max().date())


def test_backfill_main_refuses_before_any_sec_request(monkeypatch):
    """Wiring: `backfill` on the non-live library runs the receipt gate after the
    currency gate, and refuses before it downloads anything from the SEC."""
    import boto3

    session = pd.Timestamp("2026-06-08")
    frame = pd.DataFrame({"AAA": [10.0], "BBB": [20.0]}, index=pd.DatetimeIndex([session]))

    class _Reader:
        def __init__(self, bucket, library=e.CLOSE_LIBRARY_LIVE):
            self.library = library
            self.missing: list[str] = []

        def symbols(self):
            return ["AAA", "BBB"]

        def closes(self, symbols, start, end):
            return frame

    def _no_sec(*a, **k):
        raise AssertionError("the SEC was contacted before the receipt gate refused")

    monkeypatch.setattr(e, "ArcticPriceReader", _Reader)
    monkeypatch.setattr(e, "SecClient", _no_sec)
    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeS3())

    with pytest.raises(e.GateFailed, match="no readable pass receipt"):
        e.main(["backfill", "--start", "2026-06-01", "--end", "2026-06-08", "--bucket", "b"])
