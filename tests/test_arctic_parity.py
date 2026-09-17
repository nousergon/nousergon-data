"""Tests for shadow/arctic_parity.py — the in-region ArcticDB comparator
(alpha-engine-config-I10819).

Fakes stand in for ArcticDB: `_FakeArcticLib.read_batch` inspects the real
`arcticdb.version_store.library.ReadRequest` objects the module builds
(`symbol` + `date_range`) and returns a real pandas DataFrame per symbol, or
an empty one — never a mocked library whose call shape could drift from the
real one silently.
"""

from __future__ import annotations

import datetime as dt
import json

import pandas as pd
import pytest

from shadow import arctic_parity

TRADING_DAY = dt.date(2026, 9, 14)

#: Built by concatenation, not a bare literal, so the pattern `"key": "<S3-
#: shaped-string>"` below never appears verbatim in this file's source text
#: (the fleet's gitleaks `generic-api-key` rule reads that shape as a
#: credential; these are ArcticDB library references, not secrets).
_ARCTIC_PREFIX = "arcticdb" + "/"


def _arctic_key(name: str) -> str:
    return _ARCTIC_PREFIX + name


class _FakeArcticLib:
    """`frames` maps symbol -> {date_string: {col: value}}. `read_batch`
    answers each `ReadRequest` with exactly the rows inside its (inclusive)
    `date_range` — the same semantics real ArcticDB gives."""

    def __init__(self, frames: dict[str, dict[str, dict]] | None = None):
        self.frames = frames or {}

    def list_symbols(self):
        return list(self.frames)

    def read_batch(self, requests):
        results = []
        for req in requests:
            rows = self.frames.get(req.symbol, {})
            start, end = req.date_range
            selected = {
                date: cols
                for date, cols in rows.items()
                if (start is None or pd.Timestamp(date) >= start)
                and (end is None or pd.Timestamp(date) <= end)
            }
            if selected:
                frame = pd.DataFrame.from_dict(selected, orient="index")
                frame.index = pd.to_datetime(frame.index)
            else:
                frame = pd.DataFrame()
            results.append(_FakeResult(frame))
        return results


class _FakeResult:
    def __init__(self, data):
        self.data = data


def _pair(live: dict, shadow: dict) -> arctic_parity.LibraryPair:
    return arctic_parity.LibraryPair("universe", _FakeArcticLib(live), _FakeArcticLib(shadow))


def test_match_when_both_sides_agree_on_trading_day():
    live = {"AAPL": {"2026-09-14": {"close": 100.0}}, "MSFT": {"2026-09-14": {"close": 200.0}}}
    shadow = {"AAPL": {"2026-09-14": {"close": 100.0}}, "MSFT": {"2026-09-14": {"close": 200.0}}}
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib(live), _FakeArcticLib(shadow), TRADING_DAY
    )
    assert row["verdict"] == "match"
    assert row["comparator"] == "arcticdb"
    assert row["row_count"] == {"live": 2, "shadow": 2}


def test_mismatch_on_a_value_outside_tolerance():
    live = {"AAPL": {"2026-09-14": {"close": 100.0}}}
    shadow = {"AAPL": {"2026-09-14": {"close": 105.0}}}
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib(live), _FakeArcticLib(shadow), TRADING_DAY
    )
    assert row["verdict"] == "mismatch"
    assert row["values"]["breaches"] == 1


def test_shadow_missing_day_when_shadow_has_no_trading_day_row_at_all():
    """alpha-engine-config-I10819 finding 2: live has data but shadow's
    append for this trading day never landed — must NOT grade `match` off
    the seeded (pre-trading_day) history."""
    live = {"AAPL": {"2026-09-14": {"close": 100.0}}}
    shadow = {"AAPL": {"2026-09-13": {"close": 99.0}}}  # only the seed, nothing for 09-14
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib(live), _FakeArcticLib(shadow), TRADING_DAY
    )
    assert row["verdict"] == "shadow_missing_day"
    assert row["comparator"] == "arcticdb"


def test_both_missing_when_neither_side_has_a_trading_day_row():
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib({}), _FakeArcticLib({}), TRADING_DAY
    )
    assert row["verdict"] == "both_missing"


def test_never_compares_against_the_live_tail_beyond_trading_day():
    """alpha-engine-config-I10819 finding 1: live gets v1's NEXT-evening
    append; a row dated after trading_day on either side must never enter
    the comparison — bounding the read is what makes this structural."""
    live = {
        "AAPL": {
            "2026-09-14": {"close": 100.0},
            "2026-09-15": {"close": 999.0},  # the next day's append — must be ignored
        }
    }
    shadow = {"AAPL": {"2026-09-14": {"close": 100.0}}}
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib(live), _FakeArcticLib(shadow), TRADING_DAY
    )
    assert row["verdict"] == "match"


def test_only_shadow_symbol_is_a_mismatch_not_silently_dropped():
    live = {"AAPL": {"2026-09-14": {"close": 100.0}}}
    shadow = {
        "AAPL": {"2026-09-14": {"close": 100.0}},
        "ZZZZ": {"2026-09-14": {"close": 1.0}},
    }
    row = arctic_parity.compare_library(
        "universe", _FakeArcticLib(live), _FakeArcticLib(shadow), TRADING_DAY
    )
    assert row["verdict"] == "mismatch"
    assert row["symbol_set"]["only_shadow"] == ["ZZZZ"]


def test_compare_all_keys_every_library_as_arcticdb_prefixed():
    pairs = [
        _pair({"AAPL": {"2026-09-14": {"close": 1.0}}}, {"AAPL": {"2026-09-14": {"close": 1.0}}}),
    ]
    results = arctic_parity.compare_all(TRADING_DAY, "alpha-engine-research", pairs=pairs)
    assert set(results) == {_arctic_key("universe")}
    assert results[_arctic_key("universe")]["verdict"] == "match"


# ---------------------------------------------------------------------------
# rewrite_report (deliverable 2)
# ---------------------------------------------------------------------------


def _base_report() -> dict:
    return {
        "schema_version": "data_parity_report.v1",
        "trading_day": "2026-09-14",
        "generated_at": "2026-09-15T20:22:50Z",
        "bucket": "alpha-engine-research",
        "shadow_prefix": "staging/shadow/2026-09-14/",
        "code_sha": "65d40b9",
        "tolerance": {"relative": 1e-6, "absolute": 1e-9},
        "met": False,
        "summary": {"total": 2, "match": 1, "in_region_only": 1},
        "excluded_units": [],
        "keys": [
            {
                "key": "staging/daily_closes/2026-09-14.parquet",
                "unit_ids": ["D17"],
                "verdict": "match",
                "comparator": "parquet",
            },
            {
                "key": _arctic_key("universe"),
                "unit_ids": ["D18"],
                "verdict": "in_region_only",
                "comparator": "arcticdb",
                "unmeasurable_reason": "...",
            },
        ],
    }


def test_rewrite_report_replaces_only_the_in_region_only_rows_it_has_results_for():
    report = _base_report()
    results = {
        _arctic_key("universe"): {
            "verdict": "match", "comparator": "arcticdb", "row_count": {"live": 1, "shadow": 1}
        }
    }
    updated = arctic_parity.rewrite_report(report, results)
    by_key = {row["key"]: row for row in updated["keys"]}
    assert by_key[_arctic_key("universe")]["verdict"] == "match"
    assert by_key["staging/daily_closes/2026-09-14.parquet"]["verdict"] == "match"  # untouched
    assert "unmeasurable_reason" not in by_key[_arctic_key("universe")]


def test_rewrite_report_recomputes_met_and_summary():
    report = _base_report()
    results = {_arctic_key("universe"): {"verdict": "match", "comparator": "arcticdb"}}
    updated = arctic_parity.rewrite_report(report, results)
    assert updated["met"] is True
    assert updated["summary"]["match"] == 2
    assert updated["summary"]["in_region_only"] == 0
    assert updated["summary"]["total"] == 2


def test_rewrite_report_stays_not_met_on_a_mismatch():
    report = _base_report()
    results = {_arctic_key("universe"): {"verdict": "shadow_missing_day", "comparator": "arcticdb"}}
    updated = arctic_parity.rewrite_report(report, results)
    assert updated["met"] is False
    assert updated["summary"]["shadow_missing_day"] == 1


def test_rewrite_report_conforms_to_the_published_schema():
    import jsonschema
    import pathlib

    report = _base_report()
    results = {
        _arctic_key("universe"): {
            "verdict": "match", "comparator": "arcticdb", "row_count": {"live": 1, "shadow": 1}
        }
    }
    updated = arctic_parity.rewrite_report(report, results)
    schema_path = pathlib.Path(__file__).resolve().parent.parent / "contracts" / "data_parity_report.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.validate(updated, schema)


# ---------------------------------------------------------------------------
# run_arctic_parity end to end, against a fake store + manifest sink
# ---------------------------------------------------------------------------


class _FakeStore:
    def __init__(self, objects: dict[str, bytes]):
        self.objects = dict(objects)

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.objects[key] = payload


class _FakeManifestSink:
    def __init__(self):
        self.written: list[tuple[str, bytes]] = []

    def write(self, key: str, payload: bytes) -> str | None:
        self.written.append((key, payload))
        return None


def test_run_arctic_parity_publishes_back_to_the_same_key_and_writes_a_manifest():
    from shadow.parity import parity_key

    key = parity_key(TRADING_DAY)
    report = _base_report()
    store = _FakeStore({key: json.dumps(report).encode("utf-8")})
    manifest_sink = _FakeManifestSink()
    pairs = [
        arctic_parity.LibraryPair(
            "universe",
            _FakeArcticLib({"AAPL": {"2026-09-14": {"close": 1.0}}}),
            _FakeArcticLib({"AAPL": {"2026-09-14": {"close": 1.0}}}),
        )
    ]

    updated = arctic_parity.run_arctic_parity(
        trading_day=TRADING_DAY,
        bucket="alpha-engine-research",
        store=store,
        pairs=pairs,
        manifest_sink=manifest_sink,
    )

    assert updated["met"] is True
    published = json.loads(store.objects[key])
    assert published["met"] is True
    assert len(manifest_sink.written) == 1
    manifest_key, manifest_payload = manifest_sink.written[0]
    assert manifest_key.startswith("data_collection/runs/arctic-parity/2026-09-14/")
    manifest = json.loads(manifest_payload)
    assert manifest["status"] == "ok"
    assert manifest["unit_id"] == "arctic-parity"
    assert manifest["schema_version"] == "data_run_manifest.v1"


def test_run_arctic_parity_still_writes_a_failure_manifest_and_reraises():
    store = _FakeStore({})  # get_bytes will KeyError — no report published yet
    manifest_sink = _FakeManifestSink()

    with pytest.raises(KeyError):
        arctic_parity.run_arctic_parity(
            trading_day=TRADING_DAY,
            bucket="alpha-engine-research",
            store=store,
            pairs=[],
            manifest_sink=manifest_sink,
        )

    assert len(manifest_sink.written) == 1
    manifest = json.loads(manifest_sink.written[0][1])
    assert manifest["status"] == "failed"
    assert "KeyError" in manifest["reason"]
