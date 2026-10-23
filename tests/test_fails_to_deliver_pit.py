"""collectors/fails_to_deliver_pit.py — SEC half-monthly fails-to-deliver."""

from __future__ import annotations

import datetime as dt
import io
import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from collectors import fails_to_deliver_pit as m
from collectors.edgar_pit_fundamentals import SecFetchError

_REPO = Path(__file__).resolve().parents[1]
_A = (2024, 2, "a")

_TEXT = "\n".join(
    [
        "|".join(m.HEADER),
        "20240201|037833100|AAPL|1500|APPLE INC|185.50",
        "20240205|02079K305|googl|20|ALPHABET INC CL A|.",
        "20240214|000000000|BAM|7|BAM| ENTMT INC|1.10",
        "Trailer record count 3",
    ]
)


def _zip(text: str = _TEXT, name: str = "cnsfails202402a.txt") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text.encode("latin-1"))
    return buffer.getvalue()


class _Response:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.headers = {"Last-Modified": "Thu, 29 Feb 2024 20:00:00 GMT"}


class _Client:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads, self.urls = payloads, []

    def get(self, url: str, **_):
        self.urls.append(url)
        if url not in self.payloads:
            raise SecFetchError(f"GET {url} -> HTTP 404", status=404)
        return _Response(self.payloads[url])


class _Store:
    def __init__(self, keys=()) -> None:
        self.objects = {k: b"" for k in keys}

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put_bytes(self, key, body, content_type):
        self.objects[key] = body


def _url(i: int, period) -> str:
    year, month, half = period
    return m.DATASET_URLS[i].format(yyyymm=f"{year:04d}{month:02d}", half=half)


class TestPeriods:
    def test_known_at_is_a_week_past_the_sec_schedule(self) -> None:
        assert m.known_at((2024, 2, "a")) == dt.date(2024, 3, 7)
        assert m.known_at((2024, 12, "b")) == dt.date(2025, 1, 22)

    def test_ranges_and_bounds(self) -> None:
        assert m.periods_between((2023, 12, "b"), (2024, 1, "b")) == [
            (2023, 12, "b"),
            (2024, 1, "a"),
            (2024, 1, "b"),
        ]
        assert m.period_bounds((2024, 2, "b")) == (
            dt.date(2024, 2, 16),
            dt.date(2024, 2, 29),
        )
        assert m.parse_period("201601A") == (2016, 1, "a")
        with pytest.raises(ValueError):
            m.parse_period("2016-01a")

    def test_newest_known_period(self) -> None:
        assert m.last_known_period(dt.date(2026, 10, 21)) == (2026, 9, "a")
        assert m.last_known_period(dt.date(2026, 10, 23)) == (2026, 9, "b")


class TestParse:
    def test_rows_with_a_repaired_description_and_null_price(self) -> None:
        frame, not_data = m.parse_file(_TEXT, _A)
        assert tuple(frame.columns) == m.ROW_COLUMNS
        assert not_data == 1, "only the trailer"
        assert frame["symbol"].tolist() == ["AAPL", "GOOGL", "BAM"]
        assert frame.iloc[2]["description"] == "BAM| ENTMT INC"
        assert pd.isna(frame.iloc[1]["price"]) and frame.iloc[0]["price"] == 185.5
        assert (frame["known_at"] == dt.date(2024, 3, 7)).all()

    def test_an_unknown_header_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="header"):
            m.parse_file("DATE|CUSIP|SYMBOL\n", _A)

    def test_a_row_outside_the_half_month_stops_the_run(self) -> None:
        with pytest.raises(m.SourceFormatError, match="outside"):
            m.parse_file(_TEXT, (2024, 2, "b"))

    def test_a_member_with_no_extension_is_read(self) -> None:
        assert m._member_text(_zip(name="cnsfails202402a")).startswith(
            "SETTLEMENT DATE"
        )


class TestRun:
    def test_falls_through_on_404_and_records_the_source(self) -> None:
        store = _Store()
        summary = m.run(
            store,
            _Client({_url(1, _A): _zip()}),
            [_A],
            run_date=dt.date(2026, 10, 21),
            overwrite=False,
            newest_may_be_unpublished=False,
            run_id="r1",
        )
        record = summary["periods"]["202402a"]
        assert record["source_url"] == _url(1, _A) and record["rows"] == 3
        assert record["last_modified"].startswith("Thu")
        frame = pd.read_parquet(io.BytesIO(store.objects[m.period_key(_A)]))
        assert len(frame) == 3
        assert json.loads(store.objects[m.RUN_LATEST_KEY])["run_id"] == "r1"

    def test_only_the_newest_period_may_be_unpublished(self) -> None:
        b = (2024, 2, "b")
        summary = m.run(
            _Store(),
            _Client({_url(0, _A): _zip()}),
            [_A, b],
            run_date=dt.date(2026, 10, 21),
            overwrite=True,
            newest_may_be_unpublished=True,
        )
        assert summary["periods_unpublished"] == ["202402b"]
        with pytest.raises(SecFetchError):
            m.run(
                _Store(),
                _Client({}),
                [_A, b],
                run_date=dt.date(2026, 10, 21),
                overwrite=True,
                newest_may_be_unpublished=True,
            )


def test_the_dispatcher_exposes_the_backfill_workload() -> None:
    source = (
        _REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    ).read_text()
    assert '"fails-to-deliver-pit-backfill"' in source
    assert (
        "python -m collectors.fails_to_deliver_pit backfill --start 201601a" in source
    )


_LISTING = """
<a href="/files/data/fails-deliver-data/cnsfails202402a.zip">Feb 2024 a</a>
<a href="https://www.sec.gov/files/data/fails-deliver-data/cnsfails202402b_0.zip">b</a>
<a href="/files/data/fails-deliver-data/cnsfails201707a.zip">Jul 2017 a</a>
"""


def test_the_index_page_link_is_used_and_older_periods_are_recorded_as_not_offered() -> (
    None
):
    listing = m.parse_listing(_LISTING)
    assert listing[(2024, 2, "b")].endswith("cnsfails202402b_0.zip")
    assert listing[(2017, 7, "a")].startswith("https://www.sec.gov/files/")
    b = (2024, 2, "b")
    text_b = "\n".join(
        ["|".join(m.HEADER), "20240220|037833100|AAPL|900|APPLE INC|182.00"]
    )
    client = _Client({listing[b]: _zip(text=text_b)})
    summary = m.run(
        _Store(),
        client,
        [(2016, 1, "a"), b],
        run_date=dt.date(2026, 10, 23),
        overwrite=False,
        newest_may_be_unpublished=False,
        listing=listing,
    )
    assert summary["periods_not_offered"] == ["201601a"]
    assert client.urls[0] == listing[b]
