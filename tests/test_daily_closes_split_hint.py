"""Split-ratio hint on polygon_only OVERWRITE ERRORs (config#1030).

KLAC's 10-for-1 split (effective 2026-06-10) restated three windowed dates by
exactly ÷10; the ERROR messages said only "90.00% diff" and the LLM
auto-diagnosis blamed a producer decimal-shift bug (data#417-419). The hint
puts the strongest evidence — the clean integer ratio — in the message itself.
"""

from __future__ import annotations

import logging

import pandas as pd

from collectors import daily_closes


class TestSplitRatioHint:
    def test_klac_forward_split_ratio_detected(self):
        hint = daily_closes._split_ratio_hint(2139.37, 213.937)
        assert "10:1" in hint
        assert "10-for-1 forward stock split" in hint

    def test_reverse_split_ratio_detected(self):
        hint = daily_closes._split_ratio_hint(2.5, 25.0)
        assert "10:1" in hint
        assert "1-for-10 reverse stock split" in hint

    def test_plain_drift_yields_no_hint(self):
        # 7% cross-source drift — over the ERROR band but nowhere near a clean ratio.
        assert daily_closes._split_ratio_hint(100.0, 93.0) == ""

    def test_ratio_outside_tolerance_yields_no_hint(self):
        # ÷9.8 is 2% off 10:1 — a genuine anomaly must not be masked as a split.
        assert daily_closes._split_ratio_hint(980.0, 100.0) == ""

    def test_degenerate_inputs_yield_no_hint(self):
        assert daily_closes._split_ratio_hint(0.0, 100.0) == ""
        assert daily_closes._split_ratio_hint(100.0, -1.0) == ""

    def test_two_for_one_boundary_detected(self):
        assert "2:1" in daily_closes._split_ratio_hint(100.0, 50.0)

    def test_unity_ratio_never_hints(self):
        # 1:1 (no diff) must not match the N>=2 floor.
        assert daily_closes._split_ratio_hint(100.0, 100.0) == ""


class TestOverwriteErrorCarriesHint:
    def test_error_record_includes_split_hint(self, caplog):
        new_df = pd.DataFrame({"Close": [213.937]}, index=["KLAC"])
        with caplog.at_level(logging.DEBUG):
            daily_closes._log_close_discrepancies(new_df, {"KLAC": 2139.37}, "2026-06-09")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "polygon_only OVERWRITE KLAC" in errors[0].message
        assert "10:1" in errors[0].message

    def test_non_split_error_record_has_no_hint(self, caplog):
        new_df = pd.DataFrame({"Close": [93.0]}, index=["AAPL"])
        with caplog.at_level(logging.DEBUG):
            daily_closes._log_close_discrepancies(new_df, {"AAPL": 100.0}, "2026-06-09")
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "ratio" not in errors[0].message


# ── config#1431: registry-aware corporate-action reclassification ───────────

import io  # noqa: E402

from botocore.exceptions import ClientError  # noqa: E402

import corporate_actions as ca  # noqa: E402
from corporate_actions import CorporateActionRegistry  # noqa: E402


class _FakeS3:
    """Minimal in-memory S3 double for seeding a CorporateActionRegistry."""

    def __init__(self):
        self.store: dict[str, bytes] = {}

    def head_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, *, Bucket, Key):
        if Key not in self.store:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.store[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.store[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {"ETag": '"fake"'}

    def list_objects_v2(self, *, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.store if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}


def _registry_with_hon_reverse_split():
    """Registry seeded with a HON 1-for-2 reverse split, ex-date 2026-06-27."""
    reg = CorporateActionRegistry(_FakeS3(), "alpha-engine-research")
    reg.record_detected(
        ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1), run_id="r"
    )
    return reg


class TestRegistryReclassifiesConfirmedSplit:
    def test_explained_split_logs_warning_not_error(self, caplog):
        # HON 1-for-2 reverse split restated 229.49 -> 458.98 (factor 2.0).
        new_df = pd.DataFrame({"Close": [458.98]}, index=["HON"])
        reg = _registry_with_hon_reverse_split()
        with caplog.at_level(logging.DEBUG):
            explained, _unexplained = daily_closes._log_close_discrepancies(
                new_df, {"HON": 229.49}, "2026-06-25", registry=reg
            )
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        warns = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(errors) == 0  # confirmed split is NOT a flow-doctor ERROR
        assert any("corporate_action_restatement HON" in r.message for r in warns)
        assert len(explained) == 1
        assert explained[0].ticker == "HON"

    def test_registry_none_still_errors(self, caplog):
        new_df = pd.DataFrame({"Close": [458.98]}, index=["HON"])
        with caplog.at_level(logging.DEBUG):
            explained, _unexplained = daily_closes._log_close_discrepancies(
                new_df, {"HON": 229.49}, "2026-06-25", registry=None
            )
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "polygon_only OVERWRITE HON" in errors[0].message
        assert explained == []

    def test_unexplained_jump_with_registry_still_errors(self, caplog):
        # Registry has HON, but the jump is on MMM (no registered action) — must
        # stay loud as an ERROR.
        new_df = pd.DataFrame({"Close": [200.0]}, index=["MMM"])
        reg = _registry_with_hon_reverse_split()
        with caplog.at_level(logging.DEBUG):
            explained, _unexplained = daily_closes._log_close_discrepancies(
                new_df, {"MMM": 100.0}, "2026-06-25", registry=reg
            )
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "polygon_only OVERWRITE MMM" in errors[0].message
        assert explained == []


class TestCorporateActionEmail:
    def test_email_sent_once_for_explained_actions(self, monkeypatch):
        calls = []
        monkeypatch.setattr("emailer.send_email", lambda *a, **k: calls.append((a, k)))
        hon = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        hon_dup = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)  # same id
        nvda = ca.CorporateAction.from_split("NVDA", "2026-06-10", 1, 10)
        daily_closes._send_corporate_action_email([hon, hon_dup, nvda], "2026-06-25")
        assert len(calls) == 1  # ONE email
        subject = calls[0][0][0]
        assert "2 ticker(s)" in subject  # deduped by action_id
        body = calls[0][0][1]
        assert "1-for-2 reverse split" in body
        assert "10-for-1 forward split" in body

    def test_email_not_sent_when_no_actions(self, monkeypatch):
        calls = []
        monkeypatch.setattr("emailer.send_email", lambda *a, **k: calls.append(a))
        daily_closes._send_corporate_action_email([], "2026-06-25")
        assert calls == []

    def test_email_send_failure_is_logged_not_raised(self, monkeypatch, caplog):
        def _boom(*a, **k):
            raise RuntimeError("smtp down")

        monkeypatch.setattr("emailer.send_email", _boom)
        hon = ca.CorporateAction.from_split("HON", "2026-06-27", 2, 1)
        with caplog.at_level(logging.WARNING):
            # Must NOT raise (best-effort secondary notification).
            daily_closes._send_corporate_action_email([hon], "2026-06-25")
        assert any("email send failed" in r.message for r in caplog.records)
