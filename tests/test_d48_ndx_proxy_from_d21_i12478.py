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
