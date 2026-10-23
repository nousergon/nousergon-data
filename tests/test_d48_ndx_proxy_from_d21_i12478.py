"""D48's NDX proxy is QQQ read from D21's close_history spine: regression pins.

alpha-engine-config-I12478. ``ne-data-collection-panel`` failed at
``index-contributions`` (D48) on 2026-10-20 and 2026-10-21 with "proxy QQQ has
0 close(s) in the window": QQQ is not in the ArcticDB ``universe`` library, so
the NDX leg had nothing to read. nousergon-data-PR2228 moved the NDX proxy to
D21's ``market_data/close_history/QQQ.json``.

The tests PR2228 added inject ``proxy_closes_source`` (or monkeypatch both
default sources to the SAME function), so three regressions still pass them:

* ``collect()``'s default wiring sends the NDX proxy back to the universe
  source (the pre-PR2228 read);
* D21 stops publishing QQQ to close_history, or changes the key shape;
* D21 changes ``adjustment_basis`` to something the D48 reader refuses.

Each of these produces exactly the 10-21 failure in production while CI stays
green. The tests below run the real D21 writer into an in-memory bucket and
the real D48 default reader out of it, so the producer->consumer contract is
pinned end to end. Nothing here touches S3, ArcticDB or the network.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from botocore.exceptions import ClientError

from collectors import index_contributions as ic
from collectors import metron_market_data as mmd

REPO = Path(__file__).resolve().parents[1]
RUN_DATE = "2026-10-20"
BUCKET = "b"


class _MemoryS3:
    """A bucket in a dict: what D21 puts, D48 can get."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_kw):  # noqa: N803
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {}

    def get_object(self, Bucket, Key, **_kw):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": Key}}, "GetObject")

        class _Body:
            def __init__(self, raw: bytes) -> None:
                self.raw = raw

            def read(self) -> bytes:
                return self.raw

        return {"Body": _Body(self.objects[Key])}


def _universe_without_qqq(asked: list[list[str]]):
    """The production ``universe`` shape measured 2026-10-21: members and SPY,
    no QQQ. Records every symbol list it is asked for."""
    series = {
        "AAPL": [("2026-10-20", 102.0), ("2026-10-19", 100.0)],
        "SPY": [("2026-10-20", 101.0), ("2026-10-19", 100.0)],
    }

    def source(symbols, through, limit):
        asked.append(list(symbols))
        return {s: v for s, v in series.items() if s in set(symbols)}

    return source


def _weights(index: str) -> ic.Weights:
    return ic.Weights({"AAPL": 1.0}, "m", "2026-10-16")


def _publish_d21(monkeypatch: pytest.MonkeyPatch, s3: _MemoryS3) -> dict:
    """Run the real D21 ``collect_history`` writer into ``s3``. The vendor read
    is injected; everything D21 decides (which symbols, which key, which basis)
    is the production code."""
    monkeypatch.setattr(
        mmd, "load_price_derived_universe",
        lambda bucket, s3_client: ([{"yf_symbol": "AAPL", "currency": "USD"}], []),
    )

    def close_history_source(symbols):
        return {s: [("2026-10-19", 100.0), ("2026-10-20", 101.5)] for s in symbols}

    return mmd.collect_history(
        bucket=BUCKET, run_date=RUN_DATE, s3_client=s3,
        close_history_source=close_history_source,
        fx_history_source=lambda ccys: {},
    )


def test_d21_publishes_qqq_where_and_how_d48_reads_it(monkeypatch) -> None:
    """Producer half: D21's own writer puts QQQ at the key D48's reader builds,
    on the basis D48's reader accepts."""
    s3 = _MemoryS3()
    assert _publish_d21(monkeypatch, s3)["status"] == "ok"

    spec = {s.index: s for s in ic.INDEXES}["NDX"]
    key = f"{mmd.CLOSE_HISTORY_PREFIX}{spec.proxy_symbol}.json"
    assert key == "market_data/close_history/QQQ.json"
    assert key in s3.objects, "D21 no longer publishes D48's NDX proxy to close_history"
    payload = json.loads(s3.objects[key])
    assert payload["adjustment_basis"] == "dividend_adjusted"
    assert payload["closes"][-1] == ["2026-10-20", 101.5]


def test_d48_default_collect_reads_ndx_proxy_from_d21_output(monkeypatch) -> None:
    """End to end on the DEFAULT wiring (no source injected into ``collect``):
    D21 writes, D48 reads QQQ back out of close_history, and the universe
    source is never asked for QQQ. This is the 2026-10-21 failure inverted."""
    s3 = _MemoryS3()
    assert _publish_d21(monkeypatch, s3)["status"] == "ok"

    universe_asked: list[list[str]] = []
    monkeypatch.setattr(ic, "_default_closes_source", lambda bucket: _universe_without_qqq(universe_asked))
    monkeypatch.setattr(ic, "_default_weights_source", lambda bucket: _weights)
    monkeypatch.setattr(ic.boto3, "client", lambda *a, **k: s3)

    result = ic.collect(bucket=BUCKET, run_date=RUN_DATE, dry_run=True)

    assert result["errors"] == {}
    assert result["indexes"] == ["NDX", "SPX"]
    assert not any("QQQ" in symbols for symbols in universe_asked), (
        "the NDX proxy was read from ArcticDB universe, which does not hold QQQ"
    )


def test_d48_default_wiring_sends_only_the_ndx_proxy_to_close_history(monkeypatch) -> None:
    """Routing pin with distinct sentinels for the two default sources. The
    shared fixture in test_index_contributions.py points both defaults at one
    function, so it cannot see a revert of this routing."""
    universe_asked: list[list[str]] = []
    spine_asked: list[list[str]] = []

    def spine(bucket):
        assert bucket == BUCKET

        def source(symbols, through, limit):
            spine_asked.append(list(symbols))
            return {"QQQ": [("2026-10-20", 101.0), ("2026-10-19", 100.0)]}

        return source

    monkeypatch.setattr(ic, "_default_closes_source", lambda bucket: _universe_without_qqq(universe_asked))
    monkeypatch.setattr(ic, "_default_close_history_source", spine)
    monkeypatch.setattr(ic, "_default_weights_source", lambda bucket: _weights)

    result = ic.collect(bucket=BUCKET, run_date=RUN_DATE, dry_run=True)

    assert result["errors"] == {}
    assert spine_asked == [["QQQ"]]
    assert ["AAPL", "SPY"] in universe_asked
    assert not any("QQQ" in symbols for symbols in universe_asked)


def test_d48_refuses_when_d21_has_not_published_qqq(monkeypatch) -> None:
    """A spine with no QQQ object is a named NDX refusal on the default reader,
    and SPX still publishes. Never a silent fallback to another source."""
    s3 = _MemoryS3()
    universe_asked: list[list[str]] = []
    monkeypatch.setattr(ic, "_default_closes_source", lambda bucket: _universe_without_qqq(universe_asked))
    monkeypatch.setattr(ic, "_default_weights_source", lambda bucket: _weights)
    monkeypatch.setattr(ic.boto3, "client", lambda *a, **k: s3)

    result = ic.collect(bucket=BUCKET, run_date=RUN_DATE, dry_run=True)

    assert result["indexes"] == ["SPX"]
    assert "QQQ" in result["errors"]["NDX"]
    assert not any("QQQ" in symbols for symbols in universe_asked)


def test_d21_descriptor_lists_d48_as_a_close_history_consumer() -> None:
    """Lineage pin: a change to D21's close_history set or key shape must see
    D48 in its consumer list."""
    d21 = yaml.safe_load((REPO / "registry.d/units/D21-metron-close-fx-history.yaml").read_text())
    consumers = d21.get("consumers") or []
    assert any("collectors/index_contributions.py" in str(c) for c in consumers)


# ── The 10-21 missing-row case ──────────────────────────────────────────────
#
# 2026-10-21: the FRED outage failed post-market-data (alpha-engine-config-I12547),
# so D32 never appended 10-21 to ArcticDB `universe`, and the 10-22 heal was
# refused at preflight (alpha-engine-config-I12552). D21 had still run inside
# that same post-market-data process and published close_history/QQQ.json
# through 10-21 (23:45Z). So the next panel run saw the NDX proxy ONE SESSION
# AHEAD of every member -- the inverse of `_refuse_proxy_behind_members`.
#
# The properties pinned: NDX never publishes a 10-21 decomposition built from
# members that have no 10-21 close (no return is ever read as a zero
# contribution, and no member return from another pair of dates is
# substituted); SPX stays on its own library's pair; the run is `partial`, so
# the panel pages; and once the heal lands the 10-21 row, the same run
# publishes both legs for 10-21.

MISSING_ROW_DATE = "2026-10-21"


def _publish_d21_through(monkeypatch: pytest.MonkeyPatch, s3: _MemoryS3, run_date: str,
                         closes: list[tuple[str, float]]) -> dict:
    monkeypatch.setattr(
        mmd, "load_price_derived_universe",
        lambda bucket, s3_client: ([{"yf_symbol": "AAPL", "currency": "USD"}], []),
    )
    return mmd.collect_history(
        bucket=BUCKET, run_date=run_date, s3_client=s3,
        close_history_source=lambda symbols: {s: list(closes) for s in symbols},
        fx_history_source=lambda ccys: {},
    )


def _universe(series: dict[str, list[tuple[str, float]]]):
    """An ArcticDB `universe` read: most recent first, at most ``limit`` rows at
    or before ``through``, no QQQ (as measured 2026-10-21)."""

    def source(symbols, through, limit):
        out = {}
        for s in symbols:
            rows = sorted((r for r in series.get(s, []) if r[0] <= through), reverse=True)
            if rows:
                out[s] = rows[:limit]
        return out

    return source


_D21_QQQ_THROUGH_1021 = [("2026-10-19", 100.0), ("2026-10-20", 101.5), ("2026-10-21", 99.0)]
_UNIVERSE_WITHOUT_1021 = {
    "AAPL": [("2026-10-19", 100.0), ("2026-10-20", 102.0)],
    "SPY": [("2026-10-19", 100.0), ("2026-10-20", 101.0)],
}
_UNIVERSE_HEALED = {
    "AAPL": _UNIVERSE_WITHOUT_1021["AAPL"] + [("2026-10-21", 98.0)],
    "SPY": _UNIVERSE_WITHOUT_1021["SPY"] + [("2026-10-21", 99.5)],
}


def _wire(monkeypatch, s3: _MemoryS3, universe: dict) -> None:
    monkeypatch.setattr(ic, "_default_closes_source", lambda bucket: _universe(universe))
    monkeypatch.setattr(ic, "_default_weights_source", lambda bucket: _weights)
    monkeypatch.setattr(ic.boto3, "client", lambda *a, **k: s3)


def test_missing_1021_row_refuses_ndx_rather_than_publishing_it(monkeypatch) -> None:
    s3 = _MemoryS3()
    assert _publish_d21_through(monkeypatch, s3, MISSING_ROW_DATE, _D21_QQQ_THROUGH_1021)["status"] == "ok"
    qqq = json.loads(s3.objects["market_data/close_history/QQQ.json"])
    assert qqq["closes"][-1] == ["2026-10-21", 99.0], "fixture: D21 is a session ahead of universe"
    _wire(monkeypatch, s3, _UNIVERSE_WITHOUT_1021)

    result = ic.collect(bucket=BUCKET, run_date=MISSING_ROW_DATE)

    # NDX's pair comes from QQQ (10-21/10-20); AAPL has no 10-21 close, so the
    # leg is under the coverage floor and refused by name of its trading day.
    assert result["status"] == "partial"
    assert result["indexes"] == ["SPX"]
    assert MISSING_ROW_DATE in result["errors"]["NDX"]
    assert "members missing" in result["errors"]["NDX"]
    assert not [k for k in s3.objects if k.startswith(f"{ic.INDEX_CONTRIBUTIONS_PREFIX}NDX/")], (
        "an NDX artifact was written for a session no member has a close for"
    )
    # SPX stays on universe's own pair: it republishes 10-20, never a 10-21 it
    # cannot compute, and never a mixed 10-21/10-19 return.
    spx = json.loads(s3.objects[f"{ic.INDEX_CONTRIBUTIONS_PREFIX}SPX/latest.json"])
    assert (spx["trading_day"], spx["prior_close_date"]) == ("2026-10-20", "2026-10-19")
    assert spx["coverage"]["members_missing_return"] == 0


def test_missing_1021_row_is_never_filled_from_another_session(monkeypatch) -> None:
    """compute_contributions on the 10-21 state: AAPL's 10-20/10-19 return must
    not stand in for the 10-21/10-20 one the proxy pair asks for."""
    spec = {s.index: s for s in ic.INDEXES}["NDX"]
    closes = _universe(_UNIVERSE_WITHOUT_1021)(["AAPL"], MISSING_ROW_DATE, 2)
    proxy = sorted(_D21_QQQ_THROUGH_1021, reverse=True)[:2]

    with pytest.raises(ic.IndexContributionsUnavailable, match="members missing"):
        ic.compute_contributions(spec, _weights("NDX"), closes, proxy)


def test_heal_landing_the_1021_row_publishes_both_legs_for_1021(monkeypatch) -> None:
    s3 = _MemoryS3()
    assert _publish_d21_through(monkeypatch, s3, MISSING_ROW_DATE, _D21_QQQ_THROUGH_1021)["status"] == "ok"
    _wire(monkeypatch, s3, _UNIVERSE_HEALED)

    result = ic.collect(bucket=BUCKET, run_date=MISSING_ROW_DATE)

    assert result["status"] == "ok"
    assert result["errors"] == {}
    for index in ("NDX", "SPX"):
        payload = json.loads(s3.objects[f"{ic.INDEX_CONTRIBUTIONS_PREFIX}{index}/{MISSING_ROW_DATE}.json"])
        assert (payload["trading_day"], payload["prior_close_date"]) == ("2026-10-21", "2026-10-20")
        assert payload["coverage"]["members_missing_return"] == 0
