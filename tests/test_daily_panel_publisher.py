"""The daily panel publisher's first slice (alpha-engine-config-I10791, plan P-25).

The REAL producer (`builders.daily_panel`) is driven over real ArcticDB
`universe` rows (`tests/fixtures/daily_panel/`, read-only, 2026-10-06) with the
library read stubbed, and what it publishes must be the contract: a parquet
whose rows validate, a manifest written AFTER it that names its sha256, and a
parity receipt that grades the published bytes. Every refusal path writes
nothing.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from builders import daily_panel as pub
from contracts import daily_panel as dp

pytest.importorskip("jsonschema")
pytest.importorskip("pyarrow")

_FIXTURE = Path(__file__).parent / "fixtures" / "daily_panel" / "arctic_universe_2026-10-02.json"
DAY = dt.date(2026, 10, 2)
LOOKBACK = 14
NOW = dt.datetime(2026, 10, 2, 23, 0, tzinfo=dt.timezone.utc)


def _frames(tz: str | None = None) -> dict[str, pd.DataFrame]:
    """The fixture as `load_universe_ohlcv` returns it: ticker -> DatetimeIndex frame."""
    fixture = json.loads(_FIXTURE.read_text())
    frames = {}
    for ticker, rows in fixture["frames"].items():
        frame = pd.DataFrame(rows)
        index = pd.DatetimeIndex(pd.to_datetime(frame.pop("date")), name="date")
        if tz:
            index = index.tz_localize(tz)
        frame.index = index
        frame["source"] = "polygon"  # a non-OHLCV column the library carries, never published
        frames[ticker] = frame
    return frames


def _loader(frames):
    calls = []

    def load(bucket, *, symbols, lookback_days, end, region):
        calls.append({"bucket": bucket, "symbols": symbols, "lookback_days": lookback_days, "end": end})
        return frames

    load.calls = calls
    return load


class _Sink:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.order: list[str] = []

    def put(self, key, body, content_type):
        self.objects[key] = body
        self.order.append(key)

    def get(self, key):
        return self.objects[key]


def _compiled(frames=None):
    return pub.compile_panel("b", trading_day=DAY, lookback_days=LOOKBACK, loader=_loader(_frames() if frames is None else frames))


def test_the_real_rows_compile_to_a_contract_panel():
    loader = _loader(_frames())
    panel = pub.compile_panel("b", trading_day=DAY, lookback_days=LOOKBACK, loader=loader)
    assert loader.calls == [{"bucket": "b", "symbols": None, "lookback_days": LOOKBACK, "end": "2026-10-02"}]
    assert tuple(panel.columns) == dp.PANEL_COLUMNS
    assert dp.validate_panel_frame(panel, trading_day=DAY) == []
    for record in dp.panel_row_records(panel):
        assert dp.schema_problems(record, "row") == []


def test_the_window_is_crucibles_start_exclusive_window():
    """(end - lookback, end]: the fixture's 2026-09-18 bar is 14 days back and is OUT."""
    panel = _compiled()
    assert min(panel["trading_day"]) == dt.date(2026, 9, 21)
    assert max(panel["trading_day"]) == DAY


def test_a_tz_aware_index_is_flattened_not_shifted():
    assert _compiled(_frames("UTC")).equals(_compiled())


def test_a_duplicate_bar_keeps_the_last_write():
    frames = _frames()
    aapl = frames["AAPL"]
    restated = aapl.tail(1).assign(Close=999.0)
    frames["AAPL"] = pd.concat([aapl, restated])
    panel = _compiled(frames)
    last = panel[(panel["ticker"] == "AAPL") & (panel["trading_day"] == DAY)]
    assert len(last) == 1 and float(last["close_raw"].iloc[0]) == 999.0


@pytest.mark.parametrize(
    "mutate, phrase",
    [
        (lambda f: f.update(MSFT=f["MSFT"].iloc[0:0]), "empty frame"),
        (lambda f: f.update(MSFT=f["MSFT"].drop(columns=["Volume"])), "lacks"),
        (lambda f: f.update(MSFT=f["MSFT"].assign(Close=float("nan"))), "null"),
        (lambda f: f.clear(), "zero symbols"),
    ],
    ids=["empty-ticker", "missing-column", "null-close", "no-symbols"],
)
def test_a_bad_compile_refuses_and_publishes_nothing(mutate, phrase):
    frames = _frames()
    mutate(frames)
    with pytest.raises(pub.PanelCompileError, match=phrase):
        _compiled(frames)


def test_a_non_session_is_refused_before_reading():
    loader = _loader(_frames())
    with pytest.raises(pub.PanelCompileError, match="not an NYSE session"):
        pub.compile_panel("b", trading_day=dt.date(2026, 10, 3), loader=loader)
    assert loader.calls == []


def test_publish_writes_the_parquet_then_the_manifest_naming_it():
    panel = _compiled()
    sink = _Sink()
    manifest = pub.publish(panel, trading_day=DAY, lookback_days=LOOKBACK, put=sink.put, code_sha="abc", now=NOW)
    assert sink.order == [dp.panel_key(DAY), dp.manifest_key(DAY)]
    assert dp.schema_problems(manifest, "manifest") == []
    assert json.loads(sink.objects[dp.manifest_key(DAY)]) == manifest
    assert manifest["panel_sha256"] == dp.sha256_hex(sink.objects[dp.panel_key(DAY)])
    assert manifest["row_count"] == len(panel) and manifest["symbols_on_trading_day"] == 3
    round_trip = pub.read_panel(sink.objects[dp.panel_key(DAY)])
    assert round_trip.equals(panel)


def test_the_same_panel_serializes_to_the_same_bytes():
    panel = _compiled()
    assert pub.serialize(panel) == pub.serialize(panel.copy())


def test_parity_against_the_consumers_own_compile_is_equivalent():
    panel = _compiled()
    sink = _Sink()
    pub.publish(panel, trading_day=DAY, lookback_days=LOOKBACK, put=sink.put, code_sha="abc", now=NOW)
    consumer = pub.serialize(panel[panel["ticker"] != "SPY"].reset_index(drop=True))
    receipt = pub.parity(trading_day=DAY, get=sink.get, consumer_payload=consumer, consumer_key="c", put=sink.put,
                         now=NOW)
    assert receipt["verdict"] == "equivalent", receipt["examples"]
    assert receipt["producer"]["sha256"] == json.loads(sink.objects[dp.manifest_key(DAY)])["panel_sha256"]
    assert json.loads(sink.objects[dp.parity_key(DAY)]) == receipt


def test_parity_names_a_restated_bar():
    panel = _compiled()
    sink = _Sink()
    pub.publish(panel, trading_day=DAY, lookback_days=LOOKBACK, put=sink.put, code_sha="abc", now=NOW)
    restated = panel.copy()
    restated.loc[restated.index[-1], "close_raw"] = restated["close_raw"].iloc[-1] * 1.01
    receipt = pub.parity(trading_day=DAY, get=sink.get, consumer_payload=pub.serialize(restated),
                         consumer_key="c", put=sink.put, now=NOW)
    assert receipt["verdict"] == "divergent" and receipt["value_mismatches"] == 1
    assert "close_raw" in receipt["examples"][0]


def test_parity_refuses_a_published_panel_its_manifest_does_not_describe():
    panel = _compiled()
    sink = _Sink()
    pub.publish(panel, trading_day=DAY, lookback_days=LOOKBACK, put=sink.put, code_sha="abc", now=NOW)
    sink.objects[dp.panel_key(DAY)] = pub.serialize(panel.head(3))
    with pytest.raises(pub.PanelCompileError, match="does not match its manifest"):
        pub.parity(trading_day=DAY, get=sink.get, consumer_payload=b"", consumer_key="c", put=sink.put)
    assert dp.parity_key(DAY) not in sink.objects


def test_the_cli_dry_run_writes_locally_and_never_to_s3(tmp_path, monkeypatch):
    compiled = _compiled()
    monkeypatch.setattr(pub, "compile_panel", lambda bucket, **kw: compiled)
    put_calls = []

    class _NoS3:
        def put_object(self, **kwargs):
            put_calls.append(kwargs)

    import boto3

    monkeypatch.setattr(boto3, "client", lambda name: _NoS3())
    assert pub.main(["publish", "--date", "2026-10-02", "--dry-run", str(tmp_path)]) == 0
    assert put_calls == []
    assert (tmp_path / dp.panel_key(DAY)).is_file()
    assert (tmp_path / dp.manifest_key(DAY)).is_file()
