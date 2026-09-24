"""`v1_cause`: a parity difference PROVEN to be caused on the v1 side.

`alpha-engine-config-I11203`, Brian's ruling 2026-09-24 21:08Z ("proven v1
cause"). A `mismatch` row is re-graded `v1_cause` only when EVERY breach is
explained by machine-checked evidence, recorded inline on the row:

1. `v1_bar_provisional` — v1's own manifest stamps the key's bar
   `bar_settlement: provisional` and the shadow's stamps it `settled`. Numeric
   breaches of a `key_date` key only.
2. `vendor_published_after_v1_fetch` — the shadow's extra trailing
   observations were each first released by the vendor on a later day than
   the version v1 read was last updated.

Everything standalone-side stays strict: schema, membership, coverage, row
count, identity fields. The gate counts `v1_cause` as explained and prints it
apart from `match`.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pandas as pd

from data_gate import evidence
from shadow import parity
from tests.data_gate_support import TRADING_DAY, EmptyStore

REPORT_DAY = dt.date(2026, 9, 23)
DAILY_D1 = "staging/daily_closes/2026-09-22.parquet"
EOD_D1 = "market_data/eod_closes/2026-09-22.json"
MACRO = "market_data/macro/latest.json"


def _stamp(key: str, verdict: str) -> dict:
    return {"guard": "bar_settlement", "mode": "observe", "verdict": verdict, "detail": "d", "key": key}


def _manifest(unit: str, run_id: str, key: str, guards: list[dict], started="2026-09-22T20:03:25Z") -> dict:
    return {
        "unit_id": unit,
        "run_id": run_id,
        "started": started,
        "finished": started,
        "outputs": [{"key": key}],
        "guards": guards,
    }


def _context(key: str, v1_verdict: str | None = "provisional", shadow_verdict: str | None = "settled"):
    return parity.V1CauseContext(
        v1=_manifest("D19", "v1run", key, [_stamp(key, v1_verdict)] if v1_verdict else []),
        shadow=_manifest("D19", "shrun", key, [_stamp(key, shadow_verdict)] if shadow_verdict else []),
    )


def _closes_frame(rows: list[tuple[str, float, str]]) -> bytes:
    frame = pd.DataFrame(
        {
            "ticker": [r[0] for r in rows],
            "Close": [r[1] for r in rows],
            "xsource_provenance": [r[2] for r in rows],
        }
    )
    buffer = io.BytesIO()
    frame.to_parquet(buffer)
    return buffer.getvalue()


def _compare_daily(live_rows, shadow_rows, context) -> dict:
    # Graded on REPORT_DAY's report: the D-1 key does not name the report's
    # trading day, so no settling bar applies and every breach is strict —
    # the exact shape of the prior_day_settled re-check.
    return parity.compare_bytes(
        DAILY_D1,
        _closes_frame(live_rows),
        _closes_frame(shadow_rows),
        rel=parity.DEFAULT_RELATIVE_TOLERANCE,
        absolute=parity.DEFAULT_ABSOLUTE_TOLERANCE,
        contract=parity.resolve_contract(DAILY_D1),
        trading_day=REPORT_DAY,
        v1_cause=context,
    )


_LIVE = [("CPRI", 15.195, "x"), ("SAM", 168.08, "y")]
_SHADOW = [("CPRI", 15.190, "x"), ("SAM", 168.25, "y")]


# ---------------------------------------------------------------------------
# Kind 1: v1's own manifest stamps its bar provisional
# ---------------------------------------------------------------------------


def test_numeric_breaches_on_a_provisional_v1_bar_grade_v1_cause_with_inline_evidence():
    body = _compare_daily(_LIVE, _SHADOW, _context(DAILY_D1))
    assert body["verdict"] == "v1_cause"
    (proof,) = body["v1_cause"]["evidence"]
    assert proof["kind"] == "v1_bar_provisional"
    assert proof["v1_stamp"]["verdict"] == "provisional"
    assert proof["shadow_stamp"]["verdict"] == "settled"
    assert proof["v1_manifest"]["run_id"] == "v1run"


def test_without_the_v1_stamp_the_row_stays_mismatch():
    body = _compare_daily(_LIVE, _SHADOW, _context(DAILY_D1, v1_verdict=None))
    assert body["verdict"] == "mismatch"
    assert "v1_cause" not in body


def test_a_shadow_that_was_also_provisional_proves_nothing():
    body = _compare_daily(_LIVE, _SHADOW, _context(DAILY_D1, shadow_verdict="provisional"))
    assert body["verdict"] == "mismatch"


def test_an_identity_breach_beside_the_numeric_ones_keeps_the_row_strict():
    """The measured 2026-09-22 shape: `xsource_provenance` text differs too."""
    shadow = [("CPRI", 15.190, "CPRI@2026-09-22: yfinance=15.1900"), ("SAM", 168.25, "y")]
    body = _compare_daily(_LIVE, shadow, _context(DAILY_D1))
    assert body["verdict"] == "mismatch"
    assert body["values"]["identity_breaches"] == 1
    assert any("identity" in reason for reason in body["v1_cause_refused"])


def test_a_symbol_on_one_side_only_is_never_explained():
    body = _compare_daily(_LIVE + [("ZZZ", 1.0, "z")], _SHADOW, _context(DAILY_D1))
    assert body["verdict"] == "mismatch"


def test_a_bar_date_diff_in_a_json_key_date_key_is_never_explained():
    live = {"closes": {"NOVN.SW": {"bar_date": "2026-09-22", "close": 117.6, "currency": "CHF"}}}
    shadow = {"closes": {"NOVN.SW": {"bar_date": "2026-09-21", "close": 116.4, "currency": "CHF"}}}
    body = parity.compare_bytes(
        EOD_D1,
        json.dumps(live).encode(),
        json.dumps(shadow).encode(),
        rel=0.0,
        absolute=0.0,
        contract=parity.resolve_contract(EOD_D1),
        trading_day=REPORT_DAY,
        v1_cause=_context(EOD_D1),
    )
    assert body["verdict"] == "mismatch"
    assert any("bar_date" in reason for reason in body["v1_cause_refused"])


def test_the_evidence_is_read_from_the_attempt_that_recorded_the_key():
    """v1's D19 on 2026-09-22 wrote the key (stamped provisional) and then
    re-ran `not_applicable` with no outputs; latest-only would lose the stamp."""
    recorded = _manifest("D19", "first", DAILY_D1, [_stamp(DAILY_D1, "provisional")])
    rerun = {"unit_id": "D19", "run_id": "second", "finished": "2026-09-22T20:09:21Z", "outputs": []}
    assert parity.manifest_recording([recorded, rerun], DAILY_D1)["run_id"] == "first"


# ---------------------------------------------------------------------------
# Kind 2: the vendor published the extra observation after v1's version
# ---------------------------------------------------------------------------


def _ts(text: str) -> float:
    return dt.datetime.fromisoformat(text).timestamp()


def _macro_context(first_released: str | None = "2026-09-23", v1_last_updated: str | None = "2026-09-22 15:17:02-05:00"):
    path = f"{MACRO}#$.series.DGS10"
    v1_guards = (
        [{"guard": "vendor_published_at", "key": path, "value": _ts(v1_last_updated), "verdict": "recorded"}]
        if v1_last_updated
        else []
    )
    shadow_guards = (
        [
            {
                "guard": "vendor_first_released",
                "key": f"{path}@2026-09-22",
                "value": _ts(f"{first_released}T00:00:00+00:00"),
                "verdict": "recorded",
            }
        ]
        if first_released
        else []
    )
    return parity.V1CauseContext(
        v1=_manifest("D23", "v1run", MACRO, v1_guards, started="2026-09-23T20:14:00Z"),
        shadow=_manifest("D23", "shrun", MACRO, shadow_guards, started="2026-09-23T22:52:59Z"),
    )


_MACRO_LIVE = {"series": {"DGS10": [["2026-09-18", 5.01], ["2026-09-21", 4.96]]}}
_MACRO_SHADOW = {"series": {"DGS10": [["2026-09-18", 5.01], ["2026-09-21", 4.96], ["2026-09-22", 4.96]]}}


def _compare_macro(live, shadow, context) -> dict:
    return parity.compare_bytes(
        MACRO,
        json.dumps(live).encode(),
        json.dumps(shadow).encode(),
        rel=0.0,
        absolute=0.0,
        contract=parity.resolve_contract(MACRO),
        trading_day=REPORT_DAY,
        v1_cause=context,
    )


def test_an_observation_first_released_after_v1s_version_grades_v1_cause():
    body = _compare_macro(_MACRO_LIVE, _MACRO_SHADOW, _macro_context())
    assert body["verdict"] == "v1_cause"
    (proof,) = body["v1_cause"]["evidence"]
    assert proof["kind"] == "vendor_published_after_v1_fetch"
    assert proof["extra_observations_first_released"] == {"2026-09-22": "2026-09-23"}
    assert proof["v1_read_version_day"] == "2026-09-22"


def test_an_observation_released_the_same_day_as_v1s_version_is_not_proven():
    body = _compare_macro(_MACRO_LIVE, _MACRO_SHADOW, _macro_context(first_released="2026-09-22"))
    assert body["verdict"] == "mismatch"


def test_no_recorded_publication_facts_prove_nothing():
    assert _compare_macro(_MACRO_LIVE, _MACRO_SHADOW, _macro_context(v1_last_updated=None))["verdict"] == "mismatch"
    assert _compare_macro(_MACRO_LIVE, _MACRO_SHADOW, _macro_context(first_released=None))["verdict"] == "mismatch"


def test_a_shorter_shadow_series_is_a_standalone_loss_never_v1_cause():
    body = _compare_macro(_MACRO_SHADOW, _MACRO_LIVE, _macro_context())
    assert body["verdict"] == "mismatch"


def test_extra_observations_over_a_disagreeing_history_are_not_explained():
    shadow = {"series": {"DGS10": [["2026-09-18", 5.50], ["2026-09-21", 4.96], ["2026-09-22", 4.96]]}}
    assert _compare_macro(_MACRO_LIVE, shadow, _macro_context())["verdict"] == "mismatch"


# ---------------------------------------------------------------------------
# Roll-ups: report, prior_day_settled, gate
# ---------------------------------------------------------------------------


def _report(rows: list[parity.KeyResult], prior: dict | None = None) -> parity.ParityReport:
    return parity.ParityReport(
        trading_day=TRADING_DAY,
        bucket="b",
        shadow_prefix=f"staging/shadow/{TRADING_DAY.isoformat()}/",
        code_sha="x",
        rows=rows,
        excluded=[],
        rel_tolerance=0.0,
        absolute_tolerance=0.0,
        generated_at=f"{TRADING_DAY.isoformat()}T23:00:00Z",
        prior_day_settled=prior or {},
    )


def test_a_report_of_match_and_v1_cause_rows_is_met_and_counts_them_apart():
    report = _report(
        [
            parity.KeyResult("a", ["D1"], "match", "json"),
            parity.KeyResult("b", ["D1"], "v1_cause", "json", {"v1_cause": {"evidence": [{"kind": "x"}]}}),
        ]
    )
    assert report.met is True
    assert report.summary["v1_cause"] == 1
    assert report.summary["match"] == 1
    assert report.as_dict()["schema_version"] == "data_parity_report.v3"


def test_prior_day_settled_counts_a_proven_v1_pair_apart_from_unsettled():
    class _Store:
        def get_bytes(self, key):
            return json.dumps(
                {"keys": [{"key": DAILY_D1, "settling_bar": {"basis": "key_date", "cells": 3}}]}
            ).encode()

    today_key = "staging/daily_closes/2026-09-23.parquet"
    row = parity.KeyResult(
        today_key,
        ["D19"],
        "match",
        "parquet",
        {
            "settling_bar": {"basis": "key_date", "cells": 1},
            "prior_day_settled": {
                "available": True,
                "verdict": "v1_cause",
                "settled": False,
                "breaches": 2,
                "v1_cause": {"evidence": [{"kind": "v1_bar_provisional"}]},
            },
        },
    )
    block = parity.grade_prior_day_settled(
        _Store(), trading_day=REPORT_DAY, prior_day=dt.date(2026, 9, 22), rows=[row]
    )
    assert (block["settled"], block["unsettled"], block["v1_cause"]) == (0, 0, 1)
    assert block["v1_cause_examples"] == [DAILY_D1]


def _gate_report(summary: dict, prior: dict | None = None, met: bool = True) -> bytes:
    return json.dumps(
        {
            "schema_version": "data_parity_report.v3",
            "trading_day": TRADING_DAY.isoformat(),
            "generated_at": f"{TRADING_DAY.isoformat()}T23:00:00Z",
            "met": met,
            "summary": summary,
            "prior_day_settled": prior or {},
        }
    ).encode()


def test_the_gate_reads_v1_cause_as_explained_and_prints_it_separately():
    key = evidence.parity_store_key(TRADING_DAY)
    summary = {"total": 3, "match": 2, "mismatch": 0, "v1_cause": 1, "settling_bar_keys": 0}
    reading = evidence.read_parity(
        EmptyStore({key: _gate_report(summary, {"unsettled": 0, "v1_cause": 2})}),
        trading_day=TRADING_DAY,
    )
    assert reading.met is True
    assert "v1_cause=1" in reading.detail
    assert "prior_day_settled.v1_cause=2" in reading.detail
    assert "2/3 published keys match" in reading.detail


def test_the_gate_still_fails_a_real_mismatch_beside_v1_cause():
    key = evidence.parity_store_key(TRADING_DAY)
    summary = {"total": 3, "match": 1, "mismatch": 1, "v1_cause": 1}
    reading = evidence.read_parity(EmptyStore({key: _gate_report(summary, met=False)}), trading_day=TRADING_DAY)
    assert reading.met is False
    assert "mismatch=1" in reading.detail


# ---------------------------------------------------------------------------
# Producer side: the evidence is recorded where the comparator reads it
# ---------------------------------------------------------------------------


def test_d20_closes_keys_are_stamped_with_the_fetchs_settlement():
    import weekly_collector

    fetched = dt.datetime(2026, 9, 23, 20, 10, tzinfo=dt.timezone.utc)  # 16:10 ET
    result = weekly_collector._with_bar_settlement(
        {"status": "ok", "guards": [{"guard": "data_cardinality"}]},
        fetched_at=fetched,
        run_date="2026-09-23",
        keys=("market_data/eod_closes/2026-09-23.json",),
    )
    stamp = result["guards"][-1]
    assert stamp["guard"] == "bar_settlement"
    assert stamp["verdict"] == "provisional"
    assert stamp["key"] == "market_data/eod_closes/2026-09-23.json"
    unwritten = weekly_collector._with_bar_settlement(
        {"status": "error"}, fetched_at=fetched, run_date="2026-09-23", keys=("k",)
    )
    assert "guards" not in unwritten


def test_d23_records_the_vendor_publication_facts_as_manifest_guards():
    from collectors import metron_market_data

    class _S3:
        def put_object(self, **kwargs):
            pass

    result = metron_market_data.collect_macro(
        run_date="2026-09-23",
        s3_client=_S3(),
        macro_source=lambda ids, as_of: {"DGS10": [("2026-09-21", 4.96), ("2026-09-22", 4.96)]},
        release_source=lambda ids, run_date: ({}, []),
        publication_source=lambda series: {
            "DGS10": {
                "last_updated": (_ts("2026-09-23 15:17:02-05:00"), "2026-09-23 15:17:02-05"),
                "first_released": {"2026-09-22": "2026-09-23"},
            }
        },
    )
    by_guard = {g["guard"]: g for g in result["guards"]}
    assert by_guard["vendor_published_at"]["key"] == f"{MACRO}#$.series.DGS10"
    assert by_guard["vendor_first_released"]["key"] == f"{MACRO}#$.series.DGS10@2026-09-22"
    assert by_guard["vendor_first_released"]["value"] == _ts("2026-09-23T00:00:00+00:00")
