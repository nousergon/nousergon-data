"""Cost-source soft-degradation scan and notification coverage."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest


class FakeS3:
    def __init__(self, pages: dict[str, list[list[str]]], objects: dict[str, bytes]) -> None:
        self.pages = pages
        self.objects = objects

    def get_paginator(self, name: str) -> object:
        assert name == "list_objects_v2"
        owner = self

        class Paginator:
            def paginate(self, *, Bucket: str, Prefix: str) -> object:  # noqa: N803
                del Bucket
                for keys in owner.pages.get(Prefix, [[]]):
                    yield {"Contents": [{"Key": key} for key in keys]}

        return Paginator()

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
        del Bucket
        return {"Body": types.SimpleNamespace(read=lambda: self.objects[Key])}


@pytest.fixture(scope="module")
def expense_index() -> types.ModuleType:
    sys.modules.setdefault("boto3", types.ModuleType("boto3"))
    cur = types.ModuleType("cur_parquet")
    cur.read_rows = lambda *_args, **_kwargs: []  # type: ignore[attr-defined]
    sys.modules.setdefault("cur_parquet", cur)
    telegram = types.ModuleType("flow_doctor_telegram")
    telegram.notify_via_flow_doctor = lambda *_args, **_kwargs: True  # type: ignore[attr-defined]
    sys.modules.setdefault("flow_doctor_telegram", telegram)
    fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")
    fleet.FleetTelegramTopic = types.SimpleNamespace(OPS_HEALTH="OPS_HEALTH")  # type: ignore[attr-defined]
    package = types.ModuleType("nousergon_lib")
    package.flow_doctor_fleet = fleet  # type: ignore[attr-defined]
    sys.modules.setdefault("nousergon_lib", package)
    sys.modules.setdefault("nousergon_lib.flow_doctor_fleet", fleet)
    path = Path("infrastructure/lambdas/expense-collector/index.py")
    spec = importlib.util.spec_from_file_location("expense_collector_cost_source_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _jsonl(*records: dict[str, object]) -> bytes:
    return b"\n".join(json.dumps(record).encode() for record in records)


def test_scan_counts_sources_callsites_dates_and_pagination(expense_index: types.ModuleType) -> None:
    prefix = expense_index.COST_RAW_PREFIX
    pages = {f"{prefix}2026-11-20/": [["a.jsonl"], ["nested/b.jsonl"]],
             f"{prefix}2026-11-19/": [["c.jsonl"]]}
    objects = {
        "a.jsonl": _jsonl({"callsite_id": "alpha", "cost_source": "unpriced"},
                           {"callsite_id": "alpha", "cost_source": "unpriced"},
                           {"callsite_id": "ignored", "cost_source": "registry"}),
        "nested/b.jsonl": _jsonl({"callsite_id": "beta", "cost_source": "usage_unreported"}),
        "c.jsonl": _jsonl({"callsite_id": "alpha", "cost_source": "usage_unreported"}),
    }
    result = expense_index.scan_cost_source_degradations(
        FakeS3(pages, objects), datetime(2026, 11, 20, 12, tzinfo=timezone.utc))
    assert result == [
        {"date": "2026-11-19", "callsite_id": "alpha", "cost_source": "usage_unreported", "count": 1},
        {"date": "2026-11-20", "callsite_id": "alpha", "cost_source": "unpriced", "count": 2},
        {"date": "2026-11-20", "callsite_id": "beta", "cost_source": "usage_unreported", "count": 1},
    ]


def test_empty_partitions_do_not_alert(expense_index: types.ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    notify = MagicMock(return_value=True)
    monkeypatch.setattr(expense_index, "notify_via_flow_doctor", notify)
    assert expense_index.alert_cost_source_degradations([]) is False
    notify.assert_not_called()


def test_alert_contains_every_group_and_delivery_failure_is_nonfatal(
    expense_index: types.ModuleType, monkeypatch: pytest.MonkeyPatch,
) -> None:
    groups = [
        {"date": "2026-11-19", "callsite_id": "alpha", "cost_source": "unpriced", "count": 2},
        {"date": "2026-11-20", "callsite_id": "beta", "cost_source": "usage_unreported", "count": 3},
    ]
    notify = MagicMock(return_value=True)
    monkeypatch.setattr(expense_index, "notify_via_flow_doctor", notify)
    assert expense_index.alert_cost_source_degradations(groups) is True
    message = notify.call_args.args[0]
    assert "2026-11-19 | alpha | unpriced | 2" in message
    assert "2026-11-20 | beta | usage_unreported | 3" in message
    notify.side_effect = RuntimeError("Telegram unavailable")
    assert expense_index.alert_cost_source_degradations(groups) is False


def test_scan_failure_is_not_reported_as_zero(expense_index: types.ModuleType) -> None:
    s3 = FakeS3({f"{expense_index.COST_RAW_PREFIX}2026-11-20/": [["bad.jsonl"]]},
                {"bad.jsonl": b"not-json"})
    with pytest.raises(json.JSONDecodeError):
        expense_index.scan_cost_source_degradations(
            s3, datetime(2026, 11, 20, tzinfo=timezone.utc))
