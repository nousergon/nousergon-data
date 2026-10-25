"""alpha-engine-config-I12436: the 2026-10-25 universe-crsp-basis-build failure.

Three defects, one test group each:

1. **Silent degrade.** Under polygon 429s, ``corporate_actions.get_splits`` /
   ``get_dividends`` returned ``[]`` and the build kept going, so names were
   reconstructed as if they had never split or paid. The build now fetches
   STRICTLY: it retries a rate-limited fetch patiently, and a name whose history
   still cannot be read fails the build (and stops it) instead of being built.
2. **Racing limiter.** ``PolygonClient._wait_for_slot`` was unlocked shared
   state on a process singleton; eight build threads read "room in the window"
   together and burst past the 5-calls/minute budget.
3. **OOM.** Every featured frame was held until the write pass; ~909 of them
   OOM-killed a 4 GiB box (exit 137). They now spill to disk, one per name.
"""

from __future__ import annotations

import json
import os
import threading
from collections import deque
from unittest.mock import MagicMock

import pandas as pd
import pytest

import corporate_actions as ca
import polygon_client as pc
from builders import migrate_universe_crsp_basis as m
from polygon_client import PolygonRateLimitError

from tests.test_migrate_universe_crsp_basis import (
    _FakePolygon,
    _clean_setup,
    _patch_orchestration,
)


# ── 1a. corporate_actions strict mode ────────────────────────────────────────


class _RaisingClient:
    def __init__(self, exc):
        self.exc = exc

    def get_splits(self, ticker):
        raise self.exc

    def get_dividends(self, ticker):
        raise self.exc


@pytest.mark.parametrize("fn", [ca.get_splits, ca.get_dividends])
def test_default_still_degrades_to_empty(fn):
    """Non-strict callers keep the documented best-effort behaviour."""
    assert fn("X", client=_RaisingClient(PolygonRateLimitError("429"))) == []


@pytest.mark.parametrize("fn", [ca.get_splits, ca.get_dividends])
def test_strict_raises_instead_of_returning_empty(fn):
    with pytest.raises(PolygonRateLimitError):
        fn("X", client=_RaisingClient(PolygonRateLimitError("429")), strict=True)


# ── 1b. the builder's patient, strict fetch ──────────────────────────────────


class _FlakyClient(_FakePolygon):
    """Raises ``fail_times`` rate-limit errors on dividends, then answers."""

    def __init__(self, fail_times, exc=None):
        super().__init__(
            splits={"X": [{"execution_date": "2020-01-02", "split_from": 1, "split_to": 2}]},
            dividends={"X": [{"ex_dividend_date": "2020-03-02", "cash_amount": 0.5,
                              "dividend_type": "CD"}]},
        )
        self.fail_times = fail_times
        self.calls = 0
        self.exc = exc or PolygonRateLimitError("Rate limited (429)")

    def get_dividends(self, ticker):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc
        return super().get_dividends(ticker)


def test_fetch_retries_a_rate_limit_then_succeeds():
    client = _FlakyClient(fail_times=2)
    sleeps: list[float] = []
    splits, divs = m.fetch_corporate_actions("X", client=client, sleep=sleeps.append)
    assert len(splits) == 1 and len(divs) == 1
    assert sleeps == [60.0, 120.0]


def test_fetch_exhausted_raises_never_returns_empty():
    client = _FlakyClient(fail_times=99)
    sleeps: list[float] = []
    with pytest.raises(m.CorporateActionFetchError, match="dividends"):
        m.fetch_corporate_actions("X", client=client, sleep=sleeps.append)
    assert client.calls == m.CA_FETCH_ATTEMPTS
    assert len(sleeps) == m.CA_FETCH_ATTEMPTS - 1


def test_fetch_non_retryable_error_fails_at_once():
    client = _FlakyClient(fail_times=99, exc=ValueError("bad ratio"))
    sleeps: list[float] = []
    with pytest.raises(m.CorporateActionFetchError):
        m.fetch_corporate_actions("X", client=client, sleep=sleeps.append)
    assert client.calls == 1 and sleeps == []


def test_fetch_error_message_never_carries_the_api_key():
    client = _FlakyClient(
        fail_times=99,
        exc=ValueError("https://api.polygon.io/v3/reference/dividends?apiKey=SECRETKEY"),
    )
    with pytest.raises(m.CorporateActionFetchError) as ei:
        m.fetch_corporate_actions("X", client=client, sleep=lambda s: None)
    assert "SECRETKEY" not in str(ei.value)


def test_build_fails_and_stops_when_a_name_cannot_be_fetched(monkeypatch):
    """The whole point: no name is built on an empty action list it could not
    confirm, the run fails, and the names after it are not spent on."""
    raw = _clean_setup()[0]["X"]
    idx = raw.index
    tickers = ["A", "B", "C"]
    old_closes = {t: pd.Series([50.0] * len(idx), index=idx) for t in tickers}
    live_lib, scratch = _patch_orchestration(monkeypatch, old_closes=old_closes)
    monkeypatch.setattr(m.time, "sleep", lambda s: None)

    class _AlwaysLimited(_FakePolygon):
        def get_dividends(self, ticker):
            raise PolygonRateLimitError("Rate limited (429) on /v3/reference/dividends")

    s3 = MagicMock()
    monkeypatch.setattr(m.boto3, "client", lambda *a, **k: s3)
    seen: list[str] = []

    def _raw(t):
        seen.append(t)
        return raw

    with pytest.raises(RuntimeError, match="skipped after the build was stopped"):
        m.migrate_universe_crsp_basis(
            apply=False, raw_fetch=_raw, client=_AlwaysLimited({}, {}),
            workers=1,
        )
    assert seen == ["A"]  # B and C were never fetched
    audit = json.loads(s3.put_object.call_args.kwargs["Body"])
    assert audit["status"] == "fail"
    assert audit["errors_count"] == 1 and "corporate_actions" in audit["errors"][0]["error"]
    assert audit["aborted"] == ["B", "C"]
    assert audit["reconciled_count"] == 0
    scratch.write.assert_not_called()


# ── 2. thread-safe limiter ───────────────────────────────────────────────────


def test_rate_limiter_never_admits_more_than_the_budget_across_threads(monkeypatch):
    clock = {"t": 0.0}
    clock_lock = threading.Lock()

    real_sleep = pc.time.sleep

    def monotonic():
        # Yield the GIL on every clock read so an unlocked limiter's
        # check-then-append window is actually exposed to the other threads.
        real_sleep(0.0005)
        with clock_lock:
            return clock["t"]

    def sleep(s):
        with clock_lock:
            clock["t"] += s

    monkeypatch.setattr(pc.time, "monotonic", monotonic)
    monkeypatch.setattr(pc.time, "sleep", sleep)
    client = pc.PolygonClient(api_key="test-key", calls_per_min=5)
    admitted: list[float] = []

    class _Recording(deque):
        # The limiter's own admission timestamps, exactly as it records them.
        def append(self, x):
            admitted.append(x)
            super().append(x)

    client._call_times = _Recording()
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        for _ in range(5):
            client._wait_for_slot()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(admitted) == 40
    admitted.sort()
    for i, start in enumerate(admitted):
        in_window = [a for a in admitted[i:] if a - start < 60.0]
        assert len(in_window) <= 5, (start, in_window)


# ── 3. spill to disk, one frame in memory at a time ──────────────────────────


def test_apply_spills_frames_and_cleans_up(monkeypatch, tmp_path):
    raw_frames, old_closes, client = _clean_setup()
    scratch = MagicMock()
    _patch_orchestration(monkeypatch, old_closes=old_closes, scratch_lib=scratch)
    spill = tmp_path / "spill"

    result = m.migrate_universe_crsp_basis(
        apply=True, raw_fetch=lambda t: raw_frames[t], client=client,
        rel_tol=0.02, workers=1, spill_dir=str(spill),
        macro={}, sector_map={}, fundamentals={}, alt_data={},
    )
    assert result["status"] == "ok" and result["written_count"] == 1
    scratch.write.assert_called_once()
    written = scratch.write.call_args.args[1]
    assert m.TOTAL_RETURN_COL in written.columns
    # The spill file was consumed and removed.
    assert os.listdir(spill) == []
    # TickerOutcome no longer carries a frame at all.
    assert "new_df" not in m.TickerOutcome.__dataclass_fields__


def test_default_spill_dir_is_not_tmp_and_is_removed(monkeypatch, tmp_path):
    raw_frames, old_closes, client = _clean_setup()
    _patch_orchestration(monkeypatch, old_closes=old_closes)
    monkeypatch.delenv("MIGRATE_UNIVERSE_CRSP_SPILL_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    made: list[str] = []
    real_mkdtemp = m.tempfile.mkdtemp

    def _mkdtemp(**kw):
        path = real_mkdtemp(**kw)
        made.append(path)
        return path

    monkeypatch.setattr(m.tempfile, "mkdtemp", _mkdtemp)
    m.migrate_universe_crsp_basis(
        apply=True, raw_fetch=lambda t: raw_frames[t], client=client,
        rel_tol=0.02, workers=1,
        macro={}, sector_map={}, fundamentals={}, alt_data={},
    )
    assert len(made) == 1
    assert os.path.dirname(made[0]) == str(tmp_path)  # CWD (EBS), not tmpfs /tmp
    assert not os.path.exists(made[0])


def test_default_worker_count_respects_the_polygon_budget():
    assert m.DEFAULT_WORKERS <= 2
