"""Regression coverage for the expense collector's soft cost-source alert."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest


LAMBDA_DIR = Path(__file__).parents[1] / "infrastructure/lambdas/expense-collector"
sys.path.insert(0, str(LAMBDA_DIR))

fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")
fleet.FleetTelegramTopic = types.SimpleNamespace(OPS_HEALTH="OPS_HEALTH")
package = types.ModuleType("nousergon_lib")
package.flow_doctor_fleet = fleet
telegram = types.ModuleType("flow_doctor_telegram")
telegram.notify_via_flow_doctor = MagicMock(return_value=True)
sys.modules.setdefault("nousergon_lib", package)
sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", fleet)
sys.modules.setdefault("flow_doctor_telegram", telegram)

spec = importlib.util.spec_from_file_location("expense_collector_cost_source", LAMBDA_DIR / "index.py")
assert spec and spec.loader
index = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index)


class Body:
    def __init__(self, value: bytes) -> None:
        self.value = value

    def read(self) -> bytes:
        return self.value


class PaginatedS3:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.prefixes: list[str] = []

    def get_paginator(self, name: str) -> PaginatedS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str):  # noqa: ANN201, N803
        assert Bucket == index.BUCKET
        self.prefixes.append(Prefix)
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        midpoint = max(1, len(keys) // 2)
        for page_keys in (keys[:midpoint], keys[midpoint:]):
            yield {"Contents": [{"Key": key} for key in page_keys]}

    def get_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803
        assert Bucket == index.BUCKET
        return {"Body": Body(self.objects[Key])}


def _jsonl(*records: dict) -> bytes:
    return b"\n".join(json.dumps(record).encode() for record in records)


def test_scan_counts_both_sources_by_day_and_callsite_across_pages() -> None:
    prefix = index.COST_RAW_PREFIX
    s3 = PaginatedS3({
        f"{prefix}2026-11-20/a/one.jsonl": _jsonl(
            {"callsite_id": "research", "cost_source": "unpriced"},
            {"callsite_id": "research", "cost_source": "unpriced"},
            {"callsite_id": "ignored", "cost_source": "registry"}),
        f"{prefix}2026-11-20/b/two.jsonl": _jsonl(
            {"callsite_id": "evaluation", "cost_source": "usage_unreported"}),
        f"{prefix}2026-11-18/three.jsonl": _jsonl(
            {"callsite_id": "research", "cost_source": "usage_unreported"}),
        f"{prefix}2026-11-13/outside.jsonl": _jsonl(
            {"callsite_id": "old", "cost_source": "unpriced"}),
        f"{prefix}2026-11-19/not-jsonl.txt": b"not json",
    })

    result = index.scan_cost_source_degradations(
        s3, datetime(2026, 11, 20, 23, 59, tzinfo=timezone.utc))

    assert result == {"counts": [
        {"date": "2026-11-18", "callsite_id": "research",
         "cost_source": "usage_unreported", "count": 1},
        {"date": "2026-11-20", "callsite_id": "evaluation",
         "cost_source": "usage_unreported", "count": 1},
        {"date": "2026-11-20", "callsite_id": "research",
         "cost_source": "unpriced", "count": 2},
    ]}
    assert len(s3.prefixes) == 7
    assert s3.prefixes[-1].endswith("2026-11-14/")


def test_empty_partitions_and_unrelated_rows_do_not_alert(
        monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = PaginatedS3({
        f"{index.COST_RAW_PREFIX}2026-11-20/ok.jsonl": _jsonl(
            {"callsite_id": "research", "cost_source": "registry"}),
    })
    notify = MagicMock()
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify)

    scan = index.scan_cost_source_degradations(
        s3, datetime(2026, 11, 20, tzinfo=timezone.utc))

    assert scan == {"counts": []}
    assert index.alert_cost_source_degradations(scan) == {"alerted": False}
    notify.assert_not_called()


def test_alert_contains_every_group_and_delivery_failure_is_nonfatal(
        monkeypatch: pytest.MonkeyPatch) -> None:
    scan = {"counts": [
        {"date": "2026-11-19", "callsite_id": "alpha", "cost_source": "unpriced", "count": 2},
        {"date": "2026-11-20", "callsite_id": "beta", "cost_source": "usage_unreported", "count": 3},
    ]}
    notify = MagicMock(return_value=True)
    monkeypatch.setattr(index, "notify_via_flow_doctor", notify)
    assert index.alert_cost_source_degradations(scan) == {"alerted": True, "groups": 2}
    message = notify.call_args.args[0]
    assert "2026-11-19 | alpha | unpriced | 2" in message
    assert "2026-11-20 | beta | usage_unreported | 3" in message

    notify.return_value = False
    assert index.alert_cost_source_degradations(scan) == {
        "alerted": False, "error": "notification was not delivered"}

    notify.side_effect = RuntimeError("Telegram unavailable")
    result = index.alert_cost_source_degradations(scan)
    assert result == {"alerted": False, "error": "Telegram unavailable"}


def test_scan_failure_is_a_warning_not_zero(caplog: pytest.LogCaptureFixture) -> None:
    s3 = PaginatedS3({})

    def fail(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise RuntimeError("S3 listing unavailable")

    s3.paginate = fail  # type: ignore[method-assign]
    result = index.scan_cost_source_degradations(
        s3, datetime(2026, 11, 20, tzinfo=timezone.utc))

    assert result["counts"] == []
    assert "scan failed: RuntimeError: S3 listing unavailable" in result["error"]
    assert "scan failed" in caplog.text
