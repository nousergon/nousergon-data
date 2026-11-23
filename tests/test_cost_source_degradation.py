"""Regression coverage for expense-collector cost-source degradation alerts."""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def collector(monkeypatch: pytest.MonkeyPatch) -> Any:  # noqa: ANN401
    """Load the Lambda with temporary dependency stubs that cannot leak."""
    fleet = types.ModuleType("nousergon_lib.flow_doctor_fleet")
    fleet.FleetTelegramTopic = types.SimpleNamespace(OPS_HEALTH="OPS_HEALTH")  # type: ignore[attr-defined]
    package = types.ModuleType("nousergon_lib")
    package.flow_doctor_fleet = fleet  # type: ignore[attr-defined]
    telegram = types.ModuleType("flow_doctor_telegram")
    telegram.notify_via_flow_doctor = lambda *args, **kwargs: True  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nousergon_lib", package)
    monkeypatch.setitem(sys.modules, "nousergon_lib.flow_doctor_fleet", fleet)
    monkeypatch.setitem(sys.modules, "flow_doctor_telegram", telegram)

    module_path = Path(__file__).parents[1] / "infrastructure/lambdas/expense-collector/index.py"
    monkeypatch.syspath_prepend(str(module_path.parent))
    spec = importlib.util.spec_from_file_location("expense_collector_cost_source_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Body:
    def __init__(self, value: bytes) -> None:
        self.value = value

    def read(self) -> bytes:
        return self.value


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, fail_read: bool = False) -> None:
        self.objects = objects
        self.fail_read = fail_read
        self.prefixes: list[str] = []

    def get_paginator(self, name: str) -> Any:  # noqa: ANN401
        assert name == "list_objects_v2"
        outer = self

        class Paginator:
            def paginate(self, *, Bucket: str, Prefix: str) -> list[dict[str, Any]]:  # noqa: N803
                del Bucket
                outer.prefixes.append(Prefix)
                keys = sorted(key for key in outer.objects if key.startswith(Prefix))
                midpoint = (len(keys) + 1) // 2
                return [
                    {"Contents": [{"Key": key} for key in keys[:midpoint]]},
                    {"Contents": [{"Key": key} for key in keys[midpoint:]]},
                ]

        return Paginator()

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, _Body]:  # noqa: N803
        del Bucket
        if self.fail_read:
            raise RuntimeError("scan unavailable")
        return {"Body": _Body(self.objects[Key])}


def _jsonl(*rows: dict[str, object]) -> bytes:
    return "\n".join(json.dumps(row) for row in rows).encode()


def test_scan_counts_sources_dates_callsites_and_pagination(collector: Any) -> None:  # noqa: ANN401
    prefix = collector.COST_RAW_PREFIX
    s3 = FakeS3({
        f"{prefix}2026-11-20/a.jsonl": _jsonl(
            {"callsite_id": "alpha", "cost_source": "unpriced"},
            {"callsite_id": "alpha", "cost_source": "priced"},
        ),
        f"{prefix}2026-11-20/nested/b.jsonl": _jsonl(
            {"callsite_id": "beta", "cost_source": "usage_unreported"},
        ),
        f"{prefix}2026-11-19/c.jsonl": _jsonl(
            {"callsite_id": "alpha", "cost_source": "unpriced"},
            {"callsite_id": "alpha", "cost_source": "unpriced"},
        ),
        f"{prefix}2026-11-18/ignore.txt": b"not jsonl",
    })

    counts = collector.scan_cost_source_degradations(
        s3, datetime(2026, 11, 20, 23, tzinfo=timezone.utc)
    )

    assert counts == {
        ("2026-11-19", "alpha", "unpriced"): 2,
        ("2026-11-20", "alpha", "unpriced"): 1,
        ("2026-11-20", "beta", "usage_unreported"): 1,
    }
    assert len(s3.prefixes) == 7
    assert s3.prefixes[-1].endswith("2026-11-14/")


def test_alert_contains_groups_and_empty_scan_is_silent(
    collector: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:  # noqa: ANN401
    sent: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        collector, "notify_via_flow_doctor",
        lambda text, **kwargs: sent.append((text, kwargs)) or True,
    )
    counts = {
        ("2026-11-20", "alpha", "unpriced"): 2,
        ("2026-11-19", "beta", "usage_unreported"): 1,
    }
    assert collector.alert_cost_source_degradations(counts) is True
    assert all(value in sent[0][0] for value in (
        "2026-11-20", "alpha", "unpriced", "usage_unreported",
    ))
    assert sent[0][1]["severity"] == "warning"
    assert collector.alert_cost_source_degradations({}) is False
    assert len(sent) == 1


def test_alert_dedup_identity_is_stable_until_degraded_rows_change(
    collector: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:  # noqa: ANN401
    dedup_keys: list[str] = []
    monkeypatch.setattr(
        collector, "notify_via_flow_doctor",
        lambda _text, **kwargs: dedup_keys.append(kwargs["dedup_key"]) or True,
    )
    existing = {("2026-11-20", "alpha", "unpriced"): 1}

    for _ in range(14):
        assert collector.alert_cost_source_degradations(existing) is True

    assert len(set(dedup_keys)) == 1
    assert collector.alert_cost_source_degradations({
        **existing,
        ("2026-11-21", "beta", "usage_unreported"): 1,
    }) is True
    assert dedup_keys[-1] != dedup_keys[0]


def test_scan_and_notification_failures_are_nonfatal_and_loud(
    collector: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:  # noqa: ANN401
    prefix = collector.COST_RAW_PREFIX
    s3 = FakeS3({
        f"{prefix}2026-11-20/a.jsonl": _jsonl(
            {"callsite_id": "alpha", "cost_source": "unpriced"}
        )
    }, fail_read=True)
    with caplog.at_level(logging.WARNING):
        scan_result = collector.run_cost_source_degradation_alert(
            s3, datetime(2026, 11, 20, tzinfo=timezone.utc)
        )
    assert scan_result == {"error": "scan unavailable"}
    assert "scan failed" in caplog.text
    caplog.clear()

    monkeypatch.setattr(
        collector, "notify_via_flow_doctor",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("telegram down")),
    )
    with caplog.at_level(logging.WARNING):
        result = collector.alert_cost_source_degradations({
            ("2026-11-20", "alpha", "unpriced"): 1,
        })
    assert result is False
    assert "telegram down" in caplog.text
