"""alpha-engine-config-I11577 — an earlier yfinance bar stamped as the requested
date, and a shadow hole filler with no correct source in a fresh root.

Measured 2026-09-24 (read-only):

* ``staging/shadow/2026-09-24/staging/daily_closes/2026-09-22.parquet`` (the
  same-day shadow's D19 window pass) held 926 ``yfinance`` rows; only 88 of 930
  closes matched v1's ``staging/daily_closes/2026-09-22.parquet``. yfinance had
  no 2026-09-22 bar for ~835 tickers, and ``_fetch_yfinance_closes`` took the
  latest bar ON OR BEFORE the requested date — 2026-09-21's — and stamped it
  2026-09-22.
* The shadow's ``SessionHoleFiller`` then read that file (``staging/daily_closes/``
  is run state under a shadow root) and published those closes into the shadow
  price cache as 2026-09-22: 835 price_cache mismatches.
* The shadow's own EARLIER root held the right answer:
  ``staging/shadow/2026-09-23/staging/daily_closes/2026-09-22.parquet`` (the
  shadow morning pass) was 926 polygon rows, 930/930 closes equal to v1's.

Part 1 drives ``_fetch_yfinance_closes`` directly. Part 2 drives the REAL
``_refresh_stale`` through a real boto3 client with the REAL interceptor
installed and an in-memory bucket under it (the harness of
``test_prices_shadow_recent_listings_i11547.py``).
"""

from __future__ import annotations

import datetime as dt
import io
import logging
from unittest.mock import MagicMock, patch

import boto3
import botocore.client
import numpy as np
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import collectors.prices as prices
from collectors import daily_closes
from shadow import interceptor
from shadow.root import ShadowRoot, activate, deactivate

# ── part 1: _fetch_yfinance_closes never relabels an equity bar ─────────────

REQUESTED = "2026-09-22"


def _yf_frame(rows: list[tuple[str, float]], tz: str | None = None) -> pd.DataFrame:
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in rows])
    if tz is not None:
        idx = idx.tz_localize(tz)
    close = [c for _, c in rows]
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close,
         "Adj Close": close, "Volume": [1_000] * len(rows)},
        index=idx,
    )


def _fetch(frame: pd.DataFrame, tickers: list[str], date_str: str = REQUESTED) -> list[dict]:
    records: list[dict] = []
    mock_yf = MagicMock()
    mock_yf.download.return_value = frame
    with patch.dict("sys.modules", {"yfinance": mock_yf}):
        daily_closes._fetch_yfinance_closes(tickers, date_str, records)
    return records


def test_the_measured_shape_an_equity_without_the_session_is_not_covered(caplog):
    """yfinance answered [.., 09-18, 09-21] for ``end=09-23`` — the 835-ticker
    shape. The 09-21 close must not be written as 09-22's."""
    frame = _yf_frame([("2026-09-18", 120.13), ("2026-09-21", 117.60)])

    with caplog.at_level(logging.WARNING):
        records = _fetch(frame, ["FDXF"])

    assert records == []
    refusals = [r for r in caplog.records if "alpha-engine-config-I11577" in r.getMessage()]
    assert len(refusals) == 1, "one aggregated record per call, never one per ticker"
    assert "'2026-09-21': 1" in refusals[0].getMessage()


def test_an_equity_bar_on_the_requested_session_is_written():
    frame = _yf_frame([("2026-09-21", 117.60), ("2026-09-22", 119.06)])

    records = _fetch(frame, ["FDXF"])

    assert [(r["ticker"], r["date"], r["Close"]) for r in records] == [("FDXF", REQUESTED, 119.06)]


def test_a_batch_refuses_only_the_tickers_missing_the_session():
    """``group_by="ticker"`` batch over the union of dates: AAPL has 09-22
    (one of the 95 complete answers), A does not (NaN on 09-22)."""
    aapl = _yf_frame([("2026-09-21", 250.0), ("2026-09-22", 251.0)])
    a = _yf_frame([("2026-09-21", 167.0), ("2026-09-22", float("nan"))])
    frame = pd.concat({"AAPL": aapl, "A": a}, axis=1)

    records = _fetch(frame, ["AAPL", "A"])

    assert [(r["ticker"], r["Close"]) for r in records] == [("AAPL", 251.0)]


def test_an_index_ticker_keeps_on_or_before():
    """``^`` indices are FRED's fallback, and FRED resolves on-or-before by
    design (T-1 publication): the latest bar is kept and stamped the date."""
    frame = _yf_frame([("2026-09-18", 4.10), ("2026-09-21", 4.12)])

    records = _fetch(frame, ["^TNX"])

    assert [(r["ticker"], r["date"], r["Close"]) for r in records] == [("TNX", REQUESTED, 4.12)]


def test_a_tz_aware_bar_is_dated_in_its_own_exchange_zone():
    """A European listing's 09-22 bar stamped at local midnight is 09-21 22:00
    UTC. Converted to UTC first, it would read as the PRIOR session and be
    refused; it is 09-22's bar and is written."""
    frame = _yf_frame([("2026-09-21", 60.0), ("2026-09-22", 61.0)], tz="Europe/Zurich")

    records = _fetch(frame, ["NOVN.SW"])

    assert [(r["date"], r["Close"]) for r in records] == [(REQUESTED, 61.0)]


def test_a_bar_after_the_requested_date_is_never_used():
    frame = _yf_frame([("2026-09-21", 117.60), ("2026-09-23", 119.22)])

    assert _fetch(frame, ["FDXF"]) == []
    assert [r["Close"] for r in _fetch(frame, ["^VIX"])] == [117.60]


# ── part 2: the shadow hole filler reads the shadow's own earlier roots ─────

DAY = "2026-09-24"
ROOT = ShadowRoot(dt.date.fromisoformat(DAY))
BUCKET = "alpha-engine-research"
PREFIX = "predictor/price_cache/"
LIVE_CACHE = "reference/price_cache/{t}.parquet"
DC = "staging/daily_closes/{s}.parquet"
HOLE = "2026-09-22"


def _root_key(root_day: str, key: str) -> str:
    return f"staging/shadow/{root_day}/{key}"


def _series(drop: tuple[str, ...] = (HOLE,)) -> pd.DataFrame:
    idx = pd.bdate_range("2024-06-03", DAY)
    idx = idx[idx != pd.Timestamp("2026-09-07")]
    close = np.linspace(100.0, 130.0, len(idx))
    df = pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close,
         "Volume": np.full(len(idx), 1_000.0)},
        index=idx,
    )
    return df.drop(index=[pd.Timestamp(d) for d in drop])


def _parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def _dc_file(session: str, rows: dict[str, tuple[float, str]]) -> bytes:
    """A D19 file: ``{ticker: (close, source)}``."""
    tickers = list(rows)
    close = [rows[t][0] for t in tickers]
    return _parquet(pd.DataFrame(
        {"date": [session] * len(tickers), "Open": close, "High": close, "Low": close,
         "Close": close, "Adj_Close": close, "Volume": [4242] * len(tickers),
         "source": [rows[t][1] for t in tickers]},
        index=pd.Index(tickers, name="ticker"),
    ))


class MemoryS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []
        self.reads: list[str] = []

    def __call__(self, client, operation: str, params: dict):
        key = params.get("Key")
        if operation == "PutObject":
            body = params.get("Body", b"")
            if hasattr(body, "read"):
                body = body.read()
            self.objects[key] = body
            self.puts.append(key)
            return {"ETag": '"e"'}
        if operation in ("GetObject", "HeadObject"):
            self.reads.append(key)
            if key not in self.objects:
                code = "NoSuchKey" if operation == "GetObject" else "404"
                raise ClientError({"Error": {"Code": code, "Message": "missing"}}, operation)
            if operation == "HeadObject":
                return {"ContentLength": len(self.objects[key]), "ETag": '"e"'}
            return {"Body": io.BytesIO(self.objects[key]), "ContentLength": len(self.objects[key])}
        raise AssertionError(f"MemoryS3 does not model {operation}")


@pytest.fixture
def _aws_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setattr(prices, "_sleep_seconds", lambda seconds: None)


@pytest.fixture
def shadow_s3(_aws_env):
    real = botocore.client.BaseClient._make_api_call
    s3 = MemoryS3()
    activate(ROOT)
    interceptor._ORIGINAL = s3
    try:
        yield s3
    finally:
        interceptor._ORIGINAL = real
        deactivate()
    assert botocore.client.BaseClient._make_api_call is real


@pytest.fixture
def live_s3(_aws_env):
    real = botocore.client.BaseClient._make_api_call
    s3 = MemoryS3()
    botocore.client.BaseClient._make_api_call = lambda self, op, params: s3(self, op, params)
    try:
        yield s3
    finally:
        botocore.client.BaseClient._make_api_call = real


def _refresh(monkeypatch, fetched: pd.DataFrame, ticker: str = "A"):
    monkeypatch.setattr(prices.yf, "download", lambda *a, **k: fetched.copy())
    return prices._refresh_stale(
        boto3.client("s3"), BUCKET, PREFIX, [ticker], "10y", 50, trading_day=DAY,
    )


def _neighbours(fetched: pd.DataFrame, source: str) -> dict[str, dict]:
    return {
        s: {"A": (float(fetched.loc[pd.Timestamp(s), "Close"]), source)}
        for s in ("2026-09-21", "2026-09-23")
    }


def _published(s3: MemoryS3, ticker: str = "A") -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(s3.objects[ROOT.key(LIVE_CACHE.format(t=ticker))]))


def test_the_measured_shadow_shape_fills_from_the_shadows_earlier_root(monkeypatch, shadow_s3):
    """Fresh 09-24 root: its own 09-22 file carries 09-21's close under a
    yfinance label (the pre-fix D19 output). The 09-23 root (shadow morning
    pass) has polygon's 09-22. v1's live file has a sentinel that must NOT be
    read. The fill must be polygon's value, from the earlier root."""
    fetched = _series()
    mislabelled = float(fetched.loc[pd.Timestamp("2026-09-21"), "Close"])
    objs = shadow_s3.objects
    for s, rows in _neighbours(fetched, "yfinance").items():
        objs[ROOT.key(DC.format(s=s))] = _dc_file(s, rows)
    objs[ROOT.key(DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (mislabelled, "yfinance")})
    for s, rows in _neighbours(fetched, "polygon").items():
        objs[_root_key("2026-09-23", DC.format(s=s))] = _dc_file(s, rows)
    objs[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (167.26, "polygon")})
    objs[DC.format(s=HOLE)] = _dc_file(HOLE, {"A": (999.0, "polygon")})  # v1 live: never read

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(167.26)
    assert DC.format(s=HOLE) not in shadow_s3.reads, "v1's live file is not a source"
    assert shadow_s3.puts == [ROOT.key(LIVE_CACHE.format(t="A"))], "written under the shadow root only"


def test_a_fresh_root_with_no_file_for_the_session_still_fills(monkeypatch, shadow_s3):
    """After part 1, the same-day D19 pass can no longer cover the session
    from yfinance at all; the current root then has NO file for it, and the
    earlier root is the only source."""
    fetched = _series()
    objs = shadow_s3.objects
    for s, rows in _neighbours(fetched, "polygon").items():
        objs[_root_key("2026-09-23", DC.format(s=s))] = _dc_file(s, rows)
    objs[_root_key("2026-09-22", DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (167.20, "polygon")})
    objs[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (167.26, "polygon")})

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(167.26), (
        "among equal-precedence sources the newest root wins"
    )


def test_a_polygon_row_in_the_current_root_is_preferred(monkeypatch, shadow_s3):
    """The current root's own copy wins when it is at least as canonical."""
    fetched = _series()
    objs = shadow_s3.objects
    for s, rows in _neighbours(fetched, "polygon").items():
        objs[ROOT.key(DC.format(s=s))] = _dc_file(s, rows)
    objs[ROOT.key(DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (167.30, "polygon")})
    objs[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(HOLE, {"A": (167.26, "polygon")})

    _refresh(monkeypatch, fetched)

    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(167.30)


def test_production_reads_only_the_live_file(monkeypatch, live_s3):
    """No shadow root: one GET per session, exactly as before I11577."""
    fetched = _series()
    objs = live_s3.objects
    for s, rows in _neighbours(fetched, "polygon").items():
        objs[DC.format(s=s)] = _dc_file(s, rows)
    objs[DC.format(s=HOLE)] = _dc_file(HOLE, {"A": (167.26, "polygon")})

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert not [k for k in live_s3.reads if k.startswith("staging/shadow/")]
    published = pd.read_parquet(io.BytesIO(live_s3.objects[LIVE_CACHE.format(t="A")]))
    assert float(published.loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(167.26)
