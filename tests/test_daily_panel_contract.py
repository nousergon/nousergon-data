"""Producer contract test for the daily panel (alpha-engine-config-I10791, plan P-25).

Same pattern as the other P-07 contract tests: the three versioned schemas
(`daily_panel`, `daily_panel_manifest`, `daily_panel_parity`) are checked at PR
time against

* REAL rows: ``tests/fixtures/daily_panel/arctic_universe_2026-10-02.json`` is
  a read-only slice of the ArcticDB ``universe`` library (AAPL, MSFT, SPY,
  sessions to 2026-10-02) — the exact input the panel is compiled from;
* a hand-built panel independent of any producer code, so a producer bug that
  matched its own wrong output cannot also pass the contract;
* drifted panels and receipts the contract must REFUSE.

The parity comparator is graded here too, because it is the one definition of
"equivalent within declared tolerance" the acceptance clause relies on.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from contracts import (
    validate_daily_panel_manifest,
    validate_daily_panel_parity,
    validate_daily_panel_row,
)
from contracts import daily_panel as dp

pytest.importorskip("jsonschema")

_FIXTURE = Path(__file__).parent / "fixtures" / "daily_panel" / "arctic_universe_2026-10-02.json"
DAY = dt.date(2026, 10, 2)
NOW = dt.datetime(2026, 10, 2, 23, 0, tzinfo=dt.timezone.utc)
SHA_A = "a" * 64
SHA_B = "b" * 64


def _real_panel() -> pd.DataFrame:
    """The fixture's ArcticDB rows in the contract's shape, by rename only."""
    fixture = json.loads(_FIXTURE.read_text())
    rows = []
    for ticker, frame in fixture["frames"].items():
        for bar in frame:
            rows.append(
                {
                    "trading_day": dt.date.fromisoformat(bar["date"]),
                    "ticker": ticker,
                    "open_raw": bar["Open"],
                    "high_raw": bar["High"],
                    "low_raw": bar["Low"],
                    "close_raw": bar["Close"],
                    "volume_raw": bar["Volume"],
                }
            )
    panel = pd.DataFrame(rows, columns=list(dp.PANEL_COLUMNS))
    return panel.sort_values(["trading_day", "ticker"]).reset_index(drop=True)


def _hand_panel() -> pd.DataFrame:
    rows = [
        (dt.date(2026, 10, 1), "AAA", 10.0, 11.0, 9.5, 10.5, 1000.0),
        (dt.date(2026, 10, 1), "BBB", 20.0, 21.0, 19.5, 20.5, 2000.0),
        (dt.date(2026, 10, 2), "AAA", 10.5, 11.5, 10.0, 11.0, 1100.0),
        (dt.date(2026, 10, 2), "BBB", 20.5, 21.5, 20.0, 21.0, 0.0),
    ]
    return pd.DataFrame(rows, columns=list(dp.PANEL_COLUMNS))


# -- the row contract -------------------------------------------------------


def test_the_row_schema_declares_exactly_the_panel_columns():
    schema = dp.load_schema("row")
    assert tuple(schema["required"]) == dp.PANEL_COLUMNS
    assert set(schema["properties"]) == set(dp.PANEL_COLUMNS)
    assert schema["additionalProperties"] is False


def test_every_schema_names_its_key_template():
    assert dp.load_schema("row")["x-key-pattern"] == dp.panel_key("{trading_day}")
    assert dp.load_schema("manifest")["x-key-pattern"] == dp.manifest_key("{trading_day}")
    assert dp.load_schema("parity")["x-key-pattern"] == dp.parity_key("{trading_day}")


def test_keys_sit_under_the_gate_store_root():
    assert dp.store_relative(dp.manifest_key(DAY)) == "panel/2026-10-02/manifest.json"
    with pytest.raises(ValueError):
        dp.store_relative("panel/2026-10-02/manifest.json")


@pytest.mark.parametrize("build", [_real_panel, _hand_panel], ids=["real-arctic-rows", "hand-built"])
def test_a_contract_panel_validates_whole_and_row_by_row(build):
    panel = build()
    assert dp.validate_panel_frame(panel, trading_day=DAY) == []
    for record in dp.panel_row_records(panel):
        assert validate_daily_panel_row(record) == [], record
        assert dp.schema_problems(record, "row") == []


def _drift(mutate):
    panel = _hand_panel()
    return mutate(panel)


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda p: p[list(reversed(dp.PANEL_COLUMNS))], "columns"),
        (lambda p: p.assign(extra=1.0), "columns"),
        (lambda p: p.assign(close_raw=[10.5, None, 11.0, 21.0]), "null"),
        (lambda p: p.assign(volume_raw=[1.0, -1.0, 1.0, 1.0]), "out of range"),
        (lambda p: p.assign(open_raw=[0.0, 20.0, 10.5, 20.5]), "out of range"),
        (lambda p: pd.concat([p, p.tail(1)], ignore_index=True), "duplicate"),
        (lambda p: p.iloc[::-1].reset_index(drop=True), "sorted"),
        (lambda p: p[p["trading_day"] == dt.date(2026, 10, 1)], "ends on"),
        (lambda p: p.assign(trading_day=pd.to_datetime(p["trading_day"])), "datetime.date"),
    ],
    ids=["reordered", "extra-column", "null-price", "negative-volume", "zero-price", "duplicate",
         "unsorted", "wrong-end", "timestamps"],
)
def test_the_contract_refuses_a_drifted_panel(mutate, expected):
    problems = dp.validate_panel_frame(_drift(mutate), trading_day=DAY)
    assert problems and any(expected in p for p in problems), problems


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.pop("close_raw"),
        lambda r: r.update(close_raw=None),
        lambda r: r.update(close_raw=0),
        lambda r: r.update(trading_day="10/02/2026"),
        lambda r: r.update(adj_close=1.0),
        lambda r: r.update(ticker=""),
    ],
    ids=["no-close", "null-close", "zero-close", "bad-day", "extra-field", "empty-ticker"],
)
def test_the_row_schema_refuses_a_drifted_row(mutate):
    record = dp.panel_row_records(_hand_panel())[0]
    mutate(record)
    assert validate_daily_panel_row(record), record


# -- the manifest -----------------------------------------------------------


def _manifest(panel=None, payload=b"parquet-bytes"):
    return dp.build_manifest(
        _real_panel() if panel is None else panel,
        payload,
        trading_day=DAY,
        lookback_calendar_days=14,
        module="builders.daily_panel",
        code_sha="0123abc",
        generated_at=NOW,
    )


def test_the_manifest_of_a_real_panel_validates_and_counts_it():
    panel = _real_panel()
    manifest = _manifest(panel)
    assert validate_daily_panel_manifest(manifest) == []
    assert manifest["panel_key"] == "data_collection/panel/2026-10-02/panel.parquet"
    assert manifest["panel_sha256"] == dp.sha256_hex(b"parquet-bytes")
    assert manifest["symbol_count"] == 3 and manifest["symbols_on_trading_day"] == 3
    assert manifest["session_count"] == panel["trading_day"].nunique()
    assert manifest["last_session"] == "2026-10-02"
    assert manifest["generated_at"] == "2026-10-02T23:00:00Z"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m.update(schema_version="data_daily_panel_manifest.v0"),
        lambda m: m.update(panel_schema_version="panel.v1"),
        lambda m: m.update(panel_sha256="nothex"),
        lambda m: m.update(panel_key="panel/2026-10-02/panel.parquet"),
        lambda m: m.update(source={"store": "arcticdb", "library": "macro"}),
        lambda m: m.pop("producer"),
        lambda m: m.update(row_count=0),
    ],
    ids=["old-version", "crucible-version", "bad-sha", "relative-key", "wrong-library", "no-producer",
         "empty"],
)
def test_the_manifest_schema_refuses_a_drifted_manifest(mutate):
    manifest = _manifest()
    mutate(manifest)
    assert validate_daily_panel_manifest(manifest), manifest


# -- parity -----------------------------------------------------------------


def _compare(producer, consumer, **kwargs):
    return dp.compare_panels(
        producer,
        consumer,
        trading_day=kwargs.pop("trading_day", DAY),
        producer_key="data_collection/panel/2026-10-02/panel.parquet",
        producer_sha256=SHA_A,
        consumer_key="crucible-v2 store: data/2026-10-02/panel.parquet",
        consumer_sha256=SHA_B,
        generated_at=NOW,
        **kwargs,
    )


def test_the_same_real_panel_is_equivalent_and_the_receipt_validates():
    receipt = _compare(_real_panel(), _real_panel())
    assert receipt["verdict"] == "equivalent", receipt["examples"]
    assert receipt["rows_compared"] == len(_real_panel())
    assert receipt["tolerance"] == dp.PARITY_TOLERANCE
    assert validate_daily_panel_parity(receipt) == []


def test_a_wider_published_panel_is_still_equivalent():
    """More tickers, and a deeper window, than the consumer reads — not a mismatch."""
    consumer = _real_panel()
    consumer = consumer[(consumer["ticker"] != "SPY") & (consumer["trading_day"] > dt.date(2026, 9, 22))]
    receipt = _compare(_real_panel(), consumer.reset_index(drop=True))
    assert receipt["verdict"] == "equivalent", receipt["examples"]
    assert receipt["tickers_compared"] == 2


def test_float_noise_inside_the_tolerance_is_equivalent():
    consumer = _real_panel()
    consumer["close_raw"] = consumer["close_raw"] * (1 + 1e-12)
    assert _compare(_real_panel(), consumer)["verdict"] == "equivalent"


@pytest.mark.parametrize(
    "mutate, field",
    [
        (lambda c: c.assign(close_raw=c["close_raw"] * 1.001), "value_mismatches"),
        (lambda c: c.assign(volume_raw=c["volume_raw"] + 1), "value_mismatches"),
        (lambda c: pd.concat([c, pd.DataFrame([{**c.iloc[-1].to_dict(), "ticker": "ZZZ"}])], ignore_index=True),
         "missing_in_producer"),
        (lambda c: c.iloc[1:].reset_index(drop=True), "missing_in_consumer"),
    ],
    ids=["price-moved", "volume-moved", "consumer-only-ticker", "producer-row-unread"],
)
def test_a_real_difference_is_divergent_and_named(mutate, field):
    receipt = _compare(_real_panel(), mutate(_real_panel()))
    assert receipt["verdict"] == "divergent"
    assert receipt[field] >= 1
    assert receipt["examples"]
    assert validate_daily_panel_parity(receipt) == []


def test_two_different_days_are_never_equivalent():
    consumer = _real_panel()
    consumer = consumer[consumer["trading_day"] < DAY].reset_index(drop=True)
    producer = _real_panel()
    producer = producer[producer["trading_day"] < DAY].reset_index(drop=True)
    receipt = _compare(producer, consumer)
    assert receipt["verdict"] == "divergent"
    assert any("not 2026-10-02" in e for e in receipt["examples"])


def test_an_empty_comparison_is_never_equivalent():
    empty = _real_panel().iloc[0:0]
    assert _compare(_real_panel(), empty)["verdict"] == "divergent"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.update(verdict="close-enough"),
        lambda r: r["tolerance"].pop("volume_abs"),
        lambda r: r["producer"].update(sha256="x"),
        lambda r: r.update(examples=["e"] * 21),
    ],
    ids=["bad-verdict", "tolerance-half-declared", "bad-sha", "too-many-examples"],
)
def test_the_parity_schema_refuses_a_drifted_receipt(mutate):
    receipt = _compare(_real_panel(), _real_panel())
    mutate(receipt)
    assert validate_daily_panel_parity(receipt), receipt
