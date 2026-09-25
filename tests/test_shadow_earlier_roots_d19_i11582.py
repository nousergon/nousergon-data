"""alpha-engine-config-I11582 — the shadow's D19 merge base and the hole
filler's cache carry-forward read the shadow's OWN earlier roots.

Measured 2026-09-24 (read-only): the same-day shadow's D19 window pass
(``daily_closes.collect(source="yfinance_only", skip_if_canonical=True)``)
found no ``staging/daily_closes/2026-09-22.parquet`` in its fresh
``staging/shadow/2026-09-24/`` root, so it had no merge base and re-fetched
every ticker for that window date from yfinance. v1 kept the polygon rows its
morning pass had written there. The shadow's own copy of those polygon rows was
in ``staging/shadow/2026-09-23/staging/daily_closes/2026-09-22.parquet``.

Everything here runs the REAL ``collect`` / ``_refresh_stale`` through a real
boto3 client with the REAL interceptor installed and an in-memory bucket under
it (the harness of ``test_yf_closes_label_and_shadow_holefill_i11577.py``). v1's
live keys hold sentinels that must never appear in anything published.
"""

from __future__ import annotations

import io
from unittest.mock import patch

import pandas as pd
import pytest

from collectors import daily_closes, shadow_earlier_roots
from shadow import interceptor
from tests import test_yf_closes_label_and_shadow_holefill_i11577 as harness
from tests.test_yf_closes_label_and_shadow_holefill_i11577 import (
    BUCKET,
    DAY,
    DC,
    HOLE,
    LIVE_CACHE,
    ROOT,
    MemoryS3,
    _dc_file,
    _parquet,
    _published,
    _refresh,
    _root_key,
    _series,
)

# The shared harness's fixtures (real interceptor over an in-memory bucket).
_aws_env = harness._aws_env
shadow_s3 = harness.shadow_s3
live_s3 = harness.live_s3

SENTINEL = 999.0


@pytest.fixture(autouse=True)
def _head_carries_last_modified(monkeypatch):
    """``collect`` reads ``LastModified`` off a HEAD; the shared harness's
    in-memory bucket does not model it."""
    real = MemoryS3.__call__

    def _call(self, client, operation, params):
        out = real(self, client, operation, params)
        if operation == "HeadObject":
            out = {**out, "LastModified": pd.Timestamp("2026-09-24T21:00Z").to_pydatetime()}
        return out

    monkeypatch.setattr(MemoryS3, "__call__", _call)


# ── D19 merge base (collectors/daily_closes.py::collect) ────────────────────


def _collect(tickers, *, source="yfinance_only", run_date=HOLE, **kw):
    """Run the real ``collect`` with the vendor fetches stubbed; return the
    tickers yfinance was asked for."""
    asked: list[str] = []

    def _yf(missing, date_str, records):
        asked.extend(t.lstrip("^") for t in missing)
        for t in missing:
            records.append({
                "ticker": t.lstrip("^"), "date": date_str, "Open": 1.0, "High": 1.0,
                "Low": 1.0, "Close": 1.0, "Adj_Close": 1.0, "Volume": 1, "VWAP": None,
                "source": "yfinance",
            })
        return len(missing)

    with patch.object(daily_closes, "_fetch_yfinance_closes", side_effect=_yf), \
         patch.object(daily_closes, "_fetch_polygon_closes", return_value=0), \
         patch.object(daily_closes, "_fetch_fred_closes", return_value=0):
        result = daily_closes.collect(
            bucket=BUCKET, tickers=tickers, run_date=run_date, source=source,
            skip_if_canonical=True, **kw,
        )
    return result, asked


def _written(s3: MemoryS3, session: str = HOLE) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(s3.objects[ROOT.key(DC.format(s=session))]))


def test_same_day_shadow_d19_keeps_the_earlier_roots_polygon_rows(shadow_s3):
    """The reproduction: fresh 09-24 root, window date 09-22 whose polygon rows
    exist only in the 09-23 root. D19 must keep them, not re-fetch yfinance."""
    objs = shadow_s3.objects
    objs[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(
        HOLE, {"A": (167.26, "polygon"), "B": (42.10, "polygon")},
    )
    objs[DC.format(s=HOLE)] = _dc_file(HOLE, {t: (SENTINEL, "polygon") for t in "ABC"})

    result, asked = _collect(["A", "B", "C"])

    assert result["status"] == "ok", result
    assert asked == ["C"], "canonical rows from the earlier root are not re-fetched"
    out = _written(shadow_s3)
    assert out.loc["A", "Close"] == pytest.approx(167.26)
    assert out.loc["B", "Close"] == pytest.approx(42.10)
    assert list(out.loc[["A", "B"], "source"]) == ["polygon", "polygon"]
    assert out.loc["C", "source"] == "yfinance"
    assert SENTINEL not in set(out["Close"]), "no byte of v1's live file is published"
    assert DC.format(s=HOLE) not in shadow_s3.reads, "v1's live file is never read"
    assert shadow_s3.puts == [ROOT.key(DC.format(s=HOLE))], "written under the shadow root only"


def test_earlier_root_reads_are_inputs_the_run_never_writes(shadow_s3):
    """The real interceptor: earlier-root keys pass through live as INPUTS, the
    current-root key is run state, and nothing trips the I10891 refusal."""
    earlier = _root_key("2026-09-23", DC.format(s=HOLE))
    shadow_s3.objects[earlier] = _dc_file(HOLE, {"A": (167.26, "polygon")})

    result, _ = _collect(["A"])

    assert result["status"] == "ok", result
    ledger = interceptor._LEDGER
    assert (BUCKET, earlier) in ledger._input_reads
    assert (BUCKET, DC.format(s=HOLE)) not in ledger._input_reads
    assert (BUCKET, DC.format(s=HOLE)) in ledger._own_writes
    assert not ledger.baseline_reads(), "no guard-baseline read supplies a merge base"
    # The current root was probed (HEAD) under the shadow key, never the live one.
    assert ROOT.key(DC.format(s=HOLE)) in shadow_s3.reads


def test_the_merge_base_ranks_by_vendor_precedence_then_newest_root(shadow_s3):
    """A: polygon in the older root beats yfinance in the newer one. B: two
    polygon rows, the newer root wins (a same-tier restatement)."""
    objs = shadow_s3.objects
    objs[_root_key("2026-09-23", DC.format(s="2026-09-21"))] = _dc_file(
        "2026-09-21", {"A": (10.0, "yfinance"), "B": (20.0, "polygon")},
    )
    objs[_root_key("2026-09-22", DC.format(s="2026-09-21"))] = _dc_file(
        "2026-09-21", {"A": (11.0, "polygon"), "B": (21.0, "polygon")},
    )

    result, asked = _collect(["A", "B"], run_date="2026-09-21")

    assert result["status"] == "ok", result
    assert asked == []
    out = _written(shadow_s3, "2026-09-21")
    assert (out.loc["A", "Close"], out.loc["A", "source"]) == (11.0, "polygon")
    assert (out.loc["B", "Close"], out.loc["B", "source"]) == (20.0, "polygon")


def test_a_current_root_copy_is_coalesced_with_the_earlier_roots(shadow_s3):
    """An earlier leg of the same run wrote a yfinance row into this root; the
    earlier root's polygon row still wins, and the current root's own rows stay."""
    objs = shadow_s3.objects
    objs[ROOT.key(DC.format(s=HOLE))] = _dc_file(
        HOLE, {"A": (130.0, "yfinance"), "C": (7.0, "yfinance")},
    )
    objs[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(
        HOLE, {"A": (167.26, "polygon"), "B": (42.10, "polygon")},
    )

    result, asked = _collect(["A", "B", "C"])

    assert result["status"] == "ok", result
    assert asked == []
    out = _written(shadow_s3)
    assert (out.loc["A", "Close"], out.loc["A", "source"]) == (pytest.approx(167.26), "polygon")
    assert out.loc["B", "Close"] == pytest.approx(42.10)
    assert out.loc["C", "Close"] == pytest.approx(7.0)


def test_polygon_only_skips_a_date_the_earlier_root_holds_fully_canonical(shadow_s3):
    """The morning polygon window's split-aware skip reads the same merge base,
    so the shadow skips (and writes nothing) exactly where v1 does."""
    shadow_s3.objects[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(
        HOLE, {"A": (167.26, "polygon"), "B": (42.10, "polygon")},
    )

    result, _ = _collect(
        ["A", "B"], source="polygon_only", split_touched_dates=set(), registry=None,
    )

    assert result.get("skipped_reason") == "polygon_canonical", result
    assert shadow_s3.puts == []


def test_a_root_before_the_session_is_never_a_source(shadow_s3):
    """A root older than the session cannot hold its closes; only [S, D) is read."""
    shadow_s3.objects[_root_key("2026-09-21", DC.format(s=HOLE))] = _dc_file(
        HOLE, {"A": (SENTINEL, "polygon")},
    )

    _collect(["A"])

    assert _root_key("2026-09-21", DC.format(s=HOLE)) not in shadow_s3.reads
    assert SENTINEL not in set(_written(shadow_s3)["Close"])


def test_production_reads_only_the_live_merge_base(live_s3):
    """No shadow root: the live file is the merge base and nothing under
    ``staging/shadow/`` is touched."""
    live_s3.objects[DC.format(s=HOLE)] = _dc_file(HOLE, {"A": (167.26, "polygon")})
    live_s3.objects[_root_key("2026-09-23", DC.format(s=HOLE))] = _dc_file(
        HOLE, {"B": (SENTINEL, "polygon")},
    )

    result, asked = _collect(["A", "B"])

    assert result["status"] == "ok", result
    assert asked == ["B"]
    assert not [k for k in live_s3.reads if k.startswith("staging/shadow/")]
    out = pd.read_parquet(io.BytesIO(live_s3.objects[DC.format(s=HOLE)]))
    assert out.loc["A", "Close"] == pytest.approx(167.26)


# ── filler carry-forward (collectors/price_cache_holes.py::_cached_bars) ────


def _cache_with_bar(fetched: pd.DataFrame, close: float) -> bytes:
    """A previously published cache series: ``fetched`` plus the filled hole."""
    series = fetched.copy()
    series.loc[pd.Timestamp(HOLE)] = [close] * 4 + [1_000.0]
    return _parquet(series.sort_index())


def test_a_filled_bar_is_carried_forward_from_an_earlier_root_once_staging_is_gone(
    monkeypatch, shadow_s3,
):
    """No D19 file for the hole survives anywhere (staging expired) and the
    fresh root has no cache yet. v1 carries the bar forward from its own cache;
    the shadow must carry it from the cache it published in an earlier root."""
    fetched = _series()
    objs = shadow_s3.objects
    objs[_root_key("2026-09-23", LIVE_CACHE.format(t="A"))] = _cache_with_bar(fetched, 118.5)
    objs[LIVE_CACHE.format(t="A")] = _cache_with_bar(fetched, SENTINEL)  # v1 live: never read

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(118.5)
    assert LIVE_CACHE.format(t="A") not in shadow_s3.reads, "v1's live cache is never a source"
    assert shadow_s3.puts == [ROOT.key(LIVE_CACHE.format(t="A"))]
    assert (BUCKET, _root_key("2026-09-23", LIVE_CACHE.format(t="A"))) in interceptor._LEDGER._input_reads


def test_the_earlier_root_is_read_when_the_current_roots_cache_lacks_the_bar(
    monkeypatch, shadow_s3,
):
    """This root's cache exists but does not carry the hole; the newest earlier
    root that does is used."""
    fetched = _series()
    objs = shadow_s3.objects
    objs[ROOT.key(LIVE_CACHE.format(t="A"))] = _parquet(fetched)
    objs[_root_key("2026-09-23", LIVE_CACHE.format(t="A"))] = _parquet(fetched)
    objs[_root_key("2026-09-22", LIVE_CACHE.format(t="A"))] = _cache_with_bar(fetched, 118.5)

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(118.5)


def test_the_current_roots_cache_wins_and_stops_the_search(monkeypatch, shadow_s3):
    fetched = _series()
    objs = shadow_s3.objects
    objs[ROOT.key(LIVE_CACHE.format(t="A"))] = _cache_with_bar(fetched, 118.7)
    objs[_root_key("2026-09-23", LIVE_CACHE.format(t="A"))] = _cache_with_bar(fetched, 118.5)

    _refresh(monkeypatch, fetched)

    assert float(_published(shadow_s3).loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(118.7)
    assert _root_key("2026-09-23", LIVE_CACHE.format(t="A")) not in shadow_s3.reads


def test_production_carry_forward_reads_only_the_live_cache(monkeypatch, live_s3):
    fetched = _series()
    live_s3.objects[LIVE_CACHE.format(t="A")] = _cache_with_bar(fetched, 118.5)

    refreshed, failed, _ = _refresh(monkeypatch, fetched)

    assert (refreshed, failed) == (1, [])
    assert not [k for k in live_s3.reads if k.startswith("staging/shadow/")]
    published = pd.read_parquet(io.BytesIO(live_s3.objects[LIVE_CACHE.format(t="A")]))
    assert float(published.loc[pd.Timestamp(HOLE), "Close"]) == pytest.approx(118.5)


# ── the shared helper ────────────────────────────────────────────────────────


def test_both_callers_share_one_earlier_roots_helper():
    """One implementation: the filler's private copy from I11577 is gone."""
    from collectors import price_cache_holes

    assert not hasattr(price_cache_holes.SessionHoleFiller, "_with_earlier_shadow_roots")
    assert shadow_earlier_roots.EARLIER_ROOT_LOOKBACK_SESSIONS == (
        price_cache_holes.SHADOW_ROOT_LOOKBACK_SESSIONS
    )


def test_earlier_root_days_are_sessions_before_the_root_newest_first():
    days = shadow_earlier_roots.earlier_root_days(ROOT)
    assert [d.isoformat() for d in days] == [
        "2026-09-23", "2026-09-22", "2026-09-21", "2026-09-18", "2026-09-17",
    ]
    since = shadow_earlier_roots.earlier_root_days(ROOT, since=pd.Timestamp(HOLE).date())
    assert [d.isoformat() for d in since] == ["2026-09-23", "2026-09-22"]
    assert DAY not in {d.isoformat() for d in days}
