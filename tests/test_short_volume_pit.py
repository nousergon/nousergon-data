"""collectors/short_volume_pit.py — FINRA Reg SHO daily short-sale volume."""

from __future__ import annotations

import datetime as dt
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from collectors import short_volume_pit as m
from collectors.edgar_pit_fundamentals import SecFetchError

_REPO = Path(__file__).resolve().parents[1]
_DAY = dt.date(2024, 2, 5)  # a Monday


def _file(day: dt.date, rows, *, exempt: bool = True) -> str:
    head = "|".join(m.HEADER if exempt else m.HEADER_NO_EXEMPT)
    ymd = day.strftime("%Y%m%d")
    body = ["|".join([ymd, *row]) for row in rows]
    return "\n".join([head, *body, str(len(body))]) + "\n"


class _Response:
    def __init__(self, text: str) -> None:
        self.content = text.encode("latin-1")


class _Client:
    def __init__(self, files: dict[str, str]) -> None:
        self.files, self.urls = files, []

    def get(self, url: str, **_):
        self.urls.append(url)
        if url not in self.files:
            raise SecFetchError(f"GET {url} -> HTTP 403", status=403)
        return _Response(self.files[url])


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects = {k: b"" for k in keys}

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body


def _url(code: str, day: dt.date) -> str:
    return m.BASE_URL.format(code=code, ymd=day.strftime("%Y%m%d"))


class TestParse:
    def test_reads_rows_and_drops_the_trailer(self) -> None:
        text = _file(
            _DAY,
            [("AAPL", "100", "1", "300", "B,Q,N"), ("MSFT", "50.5", "0", "80", "Q")],
        )
        frame, malformed = m.parse_file(text, _DAY)
        assert malformed == 0
        assert list(frame["symbol"]) == ["AAPL", "MSFT"]
        assert frame["short_volume"].tolist() == [100.0, 50.5]
        assert frame["markets"].tolist() == ["B,Q,N", "Q"]

    def test_the_old_layout_without_exempt_volume_is_read(self) -> None:
        frame, _ = m.parse_file(
            _file(_DAY, [("AAPL", "100", "300", "Q")], exempt=False), _DAY
        )
        assert frame["short_exempt_volume"].isna().all()
        assert frame["total_volume"].tolist() == [300.0]

    def test_an_empty_day_is_header_and_trailer_only(self) -> None:
        frame, _ = m.parse_file(_file(_DAY, []), _DAY)
        assert frame.empty

    def test_an_unknown_header_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="header"):
            m.parse_file("Date|Ticker|Short\n20240205|AAPL|1\n", _DAY)

    def test_a_row_for_another_day_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="dated"):
            m.parse_file(
                _file(_DAY, [("AAPL", "1", "0", "2", "Q")]), dt.date(2024, 2, 6)
            )


class TestFetchDay:
    def test_consolidated_file_first(self) -> None:
        client = _Client(
            {_url("CNMS", _DAY): _file(_DAY, [("AAPL", "1", "0", "2", "Q")])}
        )
        frame, source, _ = m.fetch_day(client, _DAY)
        assert source == "CNMS" and len(client.urls) == 1

    def test_facility_files_are_summed_when_the_consolidated_file_is_absent(
        self,
    ) -> None:
        client = _Client(
            {
                _url("FNSQ", _DAY): _file(_DAY, [("AAPL", "10", "0", "30", "Q")]),
                _url("FNYX", _DAY): _file(
                    _DAY, [("AAPL", "5", "1", "20", "N"), ("IBM", "1", "0", "4", "N")]
                ),
            }
        )
        frame, source, _ = m.fetch_day(client, _DAY)
        assert source == "FNSQ+FNYX"
        aapl = frame[frame["symbol"] == "AAPL"].iloc[0]
        assert aapl["short_volume"] == 15 and aapl["total_volume"] == 50
        assert aapl["markets"] == "N,Q"

    def test_no_file_anywhere_is_none(self) -> None:
        assert m.fetch_day(_Client({}), _DAY) is None


class TestRun:
    def _month(self, missing: set[dt.date] = frozenset()):
        files = {}
        for day in m.trading_days(dt.date(2024, 2, 1), dt.date(2024, 2, 29)):
            if day not in missing:
                files[_url("CNMS", day)] = _file(day, [("AAPL", "1", "0", "2", "Q")])
        return files

    def test_writes_one_parquet_per_month(self) -> None:
        store = _Store()
        summary = m.run(
            store,
            _Client(self._month()),
            [(2024, 2)],
            run_date=dt.date(2026, 10, 21),
            through=dt.date(2026, 10, 20),
            overwrite=False,
            run_id="r1",
        )
        frame = pd.read_parquet(io.BytesIO(store.objects[m.month_key(2024, 2)]))
        assert tuple(frame.columns) == m.ROW_COLUMNS
        assert summary["months"]["2024-02"]["trading_days"] == 20  # Presidents' Day off
        assert len(frame) == 20 and frame["trade_date"].is_monotonic_increasing
        assert json.loads(store.objects[m.RUN_LATEST_KEY])["run_id"] == "r1"

    def test_a_few_missing_days_are_recorded_and_too_many_stop_the_run(self) -> None:
        days = m.trading_days(dt.date(2024, 2, 1), dt.date(2024, 2, 29))
        summary = m.run(
            _Store(),
            _Client(self._month({days[3]})),
            [(2024, 2)],
            run_date=_DAY,
            through=dt.date(2026, 1, 1),
            overwrite=True,
        )
        assert summary["months"]["2024-02"]["dates_missing"] == [days[3].isoformat()]
        with pytest.raises(m.SourceFormatError, match="no file"):
            m.run(
                _Store(),
                _Client(self._month(set(days[:4]))),
                [(2024, 2)],
                run_date=_DAY,
                through=dt.date(2026, 1, 1),
                overwrite=True,
            )

    def test_existing_months_are_skipped(self) -> None:
        client = _Client({})
        summary = m.run(
            _Store([m.month_key(2024, 2)]),
            client,
            [(2024, 2)],
            run_date=_DAY,
            through=dt.date(2026, 1, 1),
            overwrite=False,
        )
        assert summary["months_skipped_existing"] == 1 and client.urls == []


def test_month_helpers() -> None:
    assert m.months_between((2016, 11), (2017, 2)) == [
        (2016, 11),
        (2016, 12),
        (2017, 1),
        (2017, 2),
    ]
    assert m.parse_month("2016-01") == (2016, 1)
    with pytest.raises(ValueError):
        m.parse_month("201601")


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"short-volume-pit-backfill"' in source
    assert "python -m collectors.short_volume_pit backfill --start 2016-01" in source
