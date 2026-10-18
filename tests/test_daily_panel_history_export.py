"""The one-off frozen history export (`builders.daily_panel history`, alpha-engine-config-I12399).

Reads `universe` and `delisted_history` through one seam, keeps rows in the
window, flags delisted names, carries the current sector, and writes the
parquet before its manifest. A library with no symbols refuses the export.
"""

from __future__ import annotations

import datetime as dt
import io
import json

import pandas as pd
import pytest

from builders import daily_panel as b
from contracts import daily_panel as dp

START, END = dt.date(2016, 1, 1), dt.date(2022, 3, 31)


def _frame(days: list[str], *, with_source: bool = True) -> pd.DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(days))
    frame = pd.DataFrame(
        {"Open": 10.0, "High": 11.0, "Low": 9.0, "Close": 10.5, "Volume": 1000.0, "VWAP": 10.2},
        index=index,
    )
    if with_source:
        frame["source"] = "polygon"
    return frame


LIBS = {
    "universe": {
        "AAA": _frame(["2016-10-17", "2021-12-31", "2022-03-31", "2022-04-01"]),
        "BBB": _frame(["2022-05-02"]),
    },
    "delisted_history": {
        "OLDQ": _frame(["2014-06-02", "2019-08-30"], with_source=False),
        "BAD": _frame(["2018-01-02"]).drop(columns=["Volume"]),
    },
}


def _export(libs=LIBS):
    store: dict[str, bytes] = {}
    manifest = b.export_history(
        name="m-slot-test",
        start=START,
        end=END,
        load=lambda library: libs[library],
        sectors={"AAA": "Technology"},
        put=lambda key, body, ct: store.__setitem__(key, body),
        code_sha="a" * 40,
        now=dt.datetime(2026, 10, 17, tzinfo=dt.timezone.utc),
    )
    return manifest, store


def test_rows_window_flags_and_sector():
    manifest, store = _export()
    assert list(store) == [
        "data_collection/panel/history/m-slot-test/history.parquet",
        "data_collection/panel/history/m-slot-test/manifest.json",
    ]
    history = pd.read_parquet(io.BytesIO(store["data_collection/panel/history/m-slot-test/history.parquet"]))
    assert list(history.columns) == list(b.HISTORY_COLUMNS)
    days = sorted(str(d) for d in pd.to_datetime(history["trading_day"]).dt.date)
    assert days == ["2016-10-17", "2019-08-30", "2021-12-31", "2022-03-31"]
    old = history[history["ticker"] == "OLDQ"].iloc[0]
    assert bool(old["delisted"]) and old["library"] == "delisted_history" and pd.isna(old["source"])
    aaa = history[history["ticker"] == "AAA"].iloc[0]
    assert not bool(aaa["delisted"]) and aaa["sector"] == "Technology" and aaa["vwap_raw"] == 10.2
    assert manifest["parquet_sha256"] == dp.sha256_hex(store[manifest["parquet_key"]])
    assert manifest["first_day"] == "2016-10-17" and manifest["last_day"] == "2022-03-31"


def test_skips_are_named_not_dropped_silently():
    manifest, _ = _export()
    assert manifest["libraries"]["universe"]["skipped"] == {"BBB": "no rows in 2016-01-01..2022-03-31"}
    assert manifest["libraries"]["delisted_history"]["skipped"] == {"BAD": "lacks ['Volume']"}
    assert manifest["libraries"]["delisted_history"]["per_ticker"] == {"OLDQ": ["2019-08-30", "2019-08-30"]}
    json.dumps(manifest)


def test_an_empty_library_refuses_and_writes_nothing():
    store: dict[str, bytes] = {}
    with pytest.raises(b.PanelCompileError, match="zero symbols"):
        b.export_history(
            name="x", start=START, end=END,
            load=lambda library: LIBS["universe"] if library == "universe" else {},
            sectors={}, put=lambda key, body, ct: store.__setitem__(key, body),
        )
    assert store == {}


def test_name_must_be_one_segment():
    with pytest.raises(b.PanelCompileError):
        b.history_key("a/b", "history.parquet")


def test_the_dispatcher_workload_runs_this_export():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "infrastructure/lambdas/data-spot-dispatcher/index.py"
    source = path.read_text()
    assert '"panel-history-export"' in source
    assert "python -m builders.daily_panel history --name m-slot-2016-2022q1" in source
    assert importlib.util.spec_from_file_location("x", path) is not None
