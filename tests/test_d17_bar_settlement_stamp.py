"""D17 stamps `bar_settlement` on the `staging/daily_closes` key it rewrites.

`alpha-engine-config-I11559`. The D-1 re-check of `staging/daily_closes/{D-1}`
grades the object D17's morning run left behind (it rewrites the key after
D19's evening write), so the `v1_bar_provisional` evidence `shadow.parity`
reads for that key comes off D17's manifest. D17 already COMPUTED the stamp:
`daily_closes.collect(source="polygon_only")` returns the same
`dates.bar_settlement_guard_entry` reading D19 records. But D17 is a whole-mode
unit, its collector result sits one level down in `result["collectors"]`, and
the whole-mode wrapper never folded it, so every D17 manifest measured on
2026-09-22/23 carried only `data_empty_fresh`.

Same rule as D19/D20: graded on the moment the vendor fetch began against
`dates.SETTLED_AFTER_ET`. D17 fetches polygon's grouped-daily the morning
AFTER the session, so its verdict is `settled`. That is a fact about v1, and it
makes a D-1 difference on this key stay strict: the stamp is evidence either
way, and here it is evidence against a v1 cause.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pandas as pd
import pytest

import run_units
import weekly_collector
from dates import bar_settlement_guard_entry
from shadow import parity

TRADING_DAY = "2026-09-22"
KEY = f"staging/daily_closes/{TRADING_DAY}.parquet"
#: D17's measured fetch on 2026-09-23 (run 01M373A5Q2TEW48S2F6398PR79 began 12:19:40Z).
D17_FETCH_BEGAN = "2026-09-23T12:19:40Z"


class _S3:
    def __init__(self) -> None:
        self.puts: list[tuple[str, dict]] = []


class _Sink:
    def __init__(self, s3: _S3) -> None:
        self.s3 = s3

    def write(self, key: str, payload: bytes) -> str | None:
        self.s3.puts.append((key, json.loads(payload.decode("utf-8"))))
        return None


class _Args:
    date = TRADING_DAY
    dry_run = False


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


def _run(monkeypatch, mode: str, result: dict) -> dict:
    s3 = _S3()
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: _Sink(s3))
    weekly_collector._run_whole_mode_unit(
        mode, lambda config, args: result, {"bucket": "alpha-engine-research"}, _Args()
    )
    (manifest,) = [body for key, body in s3.puts if key.startswith("data_collection/runs/")]
    return manifest


def _morning_result(status: str = "ok", dc_status: str = "ok") -> dict:
    return {
        "status": status,
        "date": TRADING_DAY,
        "collectors": {
            "daily_closes": {
                "status": dc_status,
                "tickers_captured": 930,
                "guards": [bar_settlement_guard_entry(D17_FETCH_BEGAN, TRADING_DAY, key=KEY)],
            }
        },
    }


def _stamps(manifest: dict) -> list[dict]:
    return [g for g in manifest["guards"] if g["guard"] == "bar_settlement"]


def test_d17_manifest_carries_the_bar_settlement_stamp_for_the_key_it_wrote(monkeypatch):
    manifest = _run(monkeypatch, "morning_enrich", _morning_result())
    assert manifest["unit_id"] == "D17"
    assert manifest["status"] == "ok"
    (stamp,) = _stamps(manifest)
    assert stamp["key"] == KEY
    assert stamp["verdict"] == "settled"
    assert stamp["mode"] == "observe"
    assert [o["key"] for o in manifest["outputs"]] == [KEY]


def test_a_failed_d17_run_still_carries_the_reading_it_graded(monkeypatch):
    """Same rule as `_phase_collect` (`alpha-engine-config-I10827`): a failure
    manifest folds on what the collector graded before the run failed."""
    manifest = _run(monkeypatch, "morning_enrich", _morning_result(status="failed", dc_status="ok"))
    assert manifest["status"] == "failed"
    assert [s["key"] for s in _stamps(manifest)] == [KEY]


def test_no_stamp_is_invented_when_the_collector_graded_none(monkeypatch):
    result = _morning_result()
    result["collectors"]["daily_closes"].pop("guards")
    manifest = _run(monkeypatch, "morning_enrich", result)
    assert _stamps(manifest) == []


def test_a_mode_not_named_does_not_fold_its_nested_collectors(monkeypatch):
    """Per-mode by declaration: D32's arctic append gains no reading here."""
    manifest = _run(
        monkeypatch,
        "daily_arctic_append",
        {
            "status": "ok",
            "collectors": {
                "arcticdb": {"status": "ok", "tickers_published": 3},
                "daily_closes": {"guards": [bar_settlement_guard_entry(D17_FETCH_BEGAN, TRADING_DAY, key=KEY)]},
            },
        },
    )
    assert _stamps(manifest) == []


def _frame(close: float) -> bytes:
    buffer = io.BytesIO()
    pd.DataFrame({"ticker": ["CPRI"], "Close": [close], "source": ["polygon"]}).to_parquet(buffer)
    return buffer.getvalue()


def test_parity_reads_d17s_stamp_and_keeps_a_settled_v1_difference_strict(monkeypatch):
    """End to end: the stamp this wrapper writes is the one `shadow.parity`
    reads for the D-1 re-check. v1's D17 bar was fetched the next morning, so
    it is `settled`: a D-1 price difference is NOT proven v1-caused and the
    row stays a strict `mismatch`."""
    v1 = _run(monkeypatch, "morning_enrich", _morning_result())
    shadow = _run(monkeypatch, "morning_enrich", _morning_result())
    recorded = parity.manifest_recording([v1], KEY)
    assert recorded is v1
    assert parity._bar_settlement_stamp(recorded, KEY)["verdict"] == "settled"

    body = parity.compare_bytes(
        KEY,
        _frame(15.195),
        _frame(15.190),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(KEY),
        trading_day=dt.date(2026, 9, 23),
        v1_cause=parity.V1CauseContext(v1=recorded, shadow=shadow),
    )
    assert body["verdict"] == "mismatch"
    assert "v1_cause" not in body
    assert body["values"]["breaches"] == 1
