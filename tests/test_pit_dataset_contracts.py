"""PIT datasets that now carry a contract schema with an ``x-pit`` declaration.

`alpha-engine-config-I12567` (audit DATA-01), second slice. The first slice
(`contracts/vintage.py`) listed eight PIT collectors with no contract schema in
``UNCONTRACTED_PIT_DATASETS``. This slice contracts two of them, the 8-K item
codes and FINRA daily short volume, and moves them to
``CONTRACTED_PIT_DATASETS``.

What is graded here:

* each register names a real collector, and a collector is in at most one;
* every contracted dataset's schema carries a SOUND declaration;
* rows built by the collector's OWN row builder validate against the schema,
  and the schema's columns are exactly the collector's ``ROW_COLUMNS``, so the
  declaration cannot name a column the producer does not write;
* replay with :func:`contracts.vintage.admit` under each declaration: a
  same-day row is excluded for its own session, the prior day's row is
  admitted, and a later row (an 8-K/A filed after T) never changes what T
  admits.

No network and no AWS: the collectors' builders run on in-memory fixtures.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import jsonschema
import pandas as pd
import pytest

from collectors import filing_8k_items_pit as k8
from collectors import short_volume_pit as sv
from contracts import vintage
from tests.test_short_volume_pit import _Client, _file

REPO = pathlib.Path(__file__).resolve().parents[1]
CONTRACTS = REPO / "contracts"
NY = vintage.MARKET_TZ


def _schema(name: str) -> dict:
    return json.loads((CONTRACTS / name).read_text())


def _validator(name: str) -> jsonschema.Draft202012Validator:
    return jsonschema.Draft202012Validator(_schema(name), format_checker=jsonschema.FormatChecker())


def _records(frame: pd.DataFrame) -> list[dict]:
    """Parquet rows as a consumer reads them back: dates as ISO strings, NaN as null."""
    out = []
    for rec in frame.astype(object).where(frame.notna(), None).to_dict("records"):
        out.append({k: (v.isoformat() if isinstance(v, dt.date) else v) for k, v in rec.items()})
    return out


def _decision(session: dt.date) -> dt.datetime:
    """09:00 ET on ``session``: a pre-open decision for that session."""
    return dt.datetime(session.year, session.month, session.day, 9, 0, tzinfo=NY)


# ── the registers ──────────────────────────────────────────────────────────


def test_every_contracted_dataset_names_a_real_collector_and_a_sound_schema():
    for collector, schema_name in vintage.CONTRACTED_PIT_DATASETS.items():
        assert (REPO / collector).is_file(), collector
        schema = _schema(schema_name)
        assert vintage.DECLARATION_KEY in schema, schema_name
        assert vintage.declaration_errors(schema) == [], schema_name
        assert vintage.load_declaration(schema) is not None


def test_a_collector_is_contracted_or_uncontracted_never_both():
    both = set(vintage.CONTRACTED_PIT_DATASETS) & set(vintage.UNCONTRACTED_PIT_DATASETS)
    assert not both


@pytest.mark.parametrize(
    ("module", "schema_name"),
    [(k8, "filing_8k_items.schema.json"), (sv, "short_volume.schema.json")],
)
def test_the_contract_columns_are_exactly_the_producers_columns(module, schema_name):
    schema = _schema(schema_name)
    assert set(schema["properties"]) == set(module.ROW_COLUMNS)
    assert set(schema["required"]) == set(module.ROW_COLUMNS)


@pytest.mark.parametrize(
    ("module", "schema_name"),
    [(k8, "filing_8k_items.schema.json"), (sv, "short_volume.schema.json")],
)
def test_the_contract_pins_the_producers_schema_version(module, schema_name):
    assert _schema(schema_name)["properties"]["schema_version"] == {"const": module.SCHEMA_VERSION}


# ── 8-K item codes ─────────────────────────────────────────────────────────

_K8_BLOCK = {
    "accessionNumber": ["0000320193-24-000006", "0000320193-24-000007", "0000320193-24-000009", "0000320193-24-000001"],
    "form": ["8-K", "8-K", "8-K/A", "10-K"],
    "filingDate": ["2024-02-01", "2024-02-02", "2024-02-06", "2024-02-02"],
    "reportDate": ["2024-02-01", "", "2024-02-01", "2023-12-30"],
    "acceptanceDateTime": ["2024-02-01T16:30:52.000Z", "2024-02-02T08:00:00.000Z", "", "2024-02-02T18:04:13.000Z"],
    "items": ["2.02,9.01", "", "2.02", ""],
}


def _k8_rows() -> list[dict]:
    rows = k8.rows_in_block(320193, "AAPL", _K8_BLOCK, start=dt.date(2016, 1, 1))
    frame = pd.DataFrame(rows)
    frame["schema_version"] = k8.SCHEMA_VERSION  # as run() does
    return _records(frame[list(k8.ROW_COLUMNS)])


def test_rows_the_8k_collector_builds_validate_against_the_contract():
    rows = _k8_rows()
    assert len(rows) == 4  # two codes, one empty-code filing, one amendment; the 10-K is dropped
    validator = _validator("filing_8k_items.schema.json")
    errors = [e.message for r in rows for e in validator.iter_errors(r)]
    assert errors == []


def test_an_8k_filed_on_the_session_is_not_admissible_for_it():
    decl = vintage.load_declaration(_schema("filing_8k_items.schema.json"))
    got = vintage.admit(_k8_rows(), decl, decision=_decision(dt.date(2024, 2, 2)))
    assert {r["accession"] for r in got.admitted} == {"0000320193-24-000006"}
    assert got.not_yet_available == 2  # the 02-02 8-K and the 02-06 amendment
    assert got.unknown_availability == 0


def test_an_8k_filed_the_prior_day_is_admitted_the_next_session():
    decl = vintage.load_declaration(_schema("filing_8k_items.schema.json"))
    got = vintage.admit(_k8_rows(), decl, decision=_decision(dt.date(2024, 2, 5)))
    assert {r["accession"] for r in got.admitted} == {"0000320193-24-000006", "0000320193-24-000007"}


def test_an_amendment_filed_after_t_does_not_change_the_replay_at_t():
    decl = vintage.load_declaration(_schema("filing_8k_items.schema.json"))
    at = _decision(dt.date(2024, 2, 5))
    without = [r for r in _k8_rows() if r["form"] != "8-K/A"]
    assert vintage.admit(_k8_rows(), decl, decision=at).admitted == vintage.admit(without, decl, decision=at).admitted
    later = vintage.admit(_k8_rows(), decl, decision=_decision(dt.date(2024, 2, 7)))
    assert "0000320193-24-000009" in {r["accession"] for r in later.admitted}


# ── FINRA short volume ─────────────────────────────────────────────────────

_FEB1, _FEB2 = dt.date(2024, 2, 1), dt.date(2024, 2, 2)


def _sv_rows() -> list[dict]:
    files = {}
    for day, rows in ((_FEB1, [["AAPL", "100", "1", "400", "B,Q,N"], ["MSFT", "50", "0", "200", "Q"]]),
                      (_FEB2, [["AAPL", "120.5", "2", "380.25", "B,Q,N"]])):
        files[sv.BASE_URL.format(code=sv.CONSOLIDATED, ymd=day.strftime("%Y%m%d"))] = _file(day, rows)
    frame, _ = sv.collect_month(_Client(files), 2024, 2, through=_FEB2)
    return _records(frame)


def test_rows_the_short_volume_collector_builds_validate_against_the_contract():
    rows = _sv_rows()
    assert len(rows) == 3
    validator = _validator("short_volume.schema.json")
    errors = [e.message for r in rows for e in validator.iter_errors(r)]
    assert errors == []


def test_a_layout_without_short_exempt_volume_still_validates():
    day = dt.date(2024, 3, 1)  # the pre-2011 layout, on a date the calendar covers
    url = sv.BASE_URL.format(code=sv.CONSOLIDATED, ymd=day.strftime("%Y%m%d"))
    frame, _ = sv.collect_month(_Client({url: _file(day, [["AAPL", "10", "40", "Q"]], exempt=False)}), 2024, 3, through=day)
    rows = _records(frame)
    assert rows[0]["short_exempt_volume"] is None
    assert [e.message for e in _validator("short_volume.schema.json").iter_errors(rows[0])] == []


def test_a_trade_dates_volume_is_admissible_only_from_the_next_session():
    decl = vintage.load_declaration(_schema("short_volume.schema.json"))
    rows = _sv_rows()
    on_feb2 = vintage.admit(rows, decl, decision=_decision(_FEB2))
    assert {r["trade_date"] for r in on_feb2.admitted} == {"2024-02-01"}
    assert on_feb2.not_yet_available == 1
    on_feb5 = vintage.admit(rows, decl, decision=_decision(dt.date(2024, 2, 5)))
    assert len(on_feb5.admitted) == 3
