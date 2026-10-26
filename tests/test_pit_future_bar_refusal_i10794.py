"""alpha-engine-config-I10794 — a point-in-time read REFUSES a bar dated after its as-of.

Brian's ruling on I10794 (option (a), 2026-10-25 15:07Z): when a point-in-time
read finds a bar dated after the as-of date it fails with a finding; the bar is
never silently dropped.

Before this change the fetch boundary (``dates.clip_to_trading_day``) dropped
such rows with a WARNING and the run went on ``ok``. That made the D03 ``pit``
guard uncommissionable: an induced vendor future bar was trimmed before it could
reach the write-site raise (``assert_no_bar_after``), so "manifest failed, one
page, stand-down" could not be observed (the 2026-10-25 04:43Z analysis on
I10794, point 1).

What this file pins, with no network (every vendor seam is stubbed):

1. the primitive: :func:`dates.refuse_bars_after` (and ``clip_to_trading_day``,
   which now delegates to it) raises :class:`dates.FutureBarRefused` carrying a
   :class:`dates.FutureBarFinding` naming dataset, ticker, as-of and the
   offending bar date; a clean frame passes through unchanged;
2. D03's REAL ``collectors.prices._refresh_stale`` over a vendor that answers one
   bar past the requested end: the read fails with the finding and nothing is
   uploaded; a clean answer uploads;
3. D03 end to end on the deployed ``data-collection.asl.json`` (the walker the
   guard-commissioning harnesses use), the REAL ``prices.collect`` under the REAL
   ``_phase_collect``: a ``failed`` manifest naming the finding on the attempt and
   the on-demand retry, exactly one page, terminal ``CollectionFailed``, the
   previously published parquet untouched; once the vendor answers in bounds the
   next run succeeds with 0 pages. The record lines this proves are
   ``guard_commissioning.PIT_OBSERVED_BY_UNIT["D03"]``.

Not here: flipping ``registry.d/units/D03-prices.yaml`` ``pit`` to ``present`` and
adding D03 to ``test_guard_commissioning_pit.CASES`` (the publisher's harness).
``registry.d/units/**`` redeploys ``data-spot-dispatcher``, which is held while
the EDGAR rebuild (alpha-engine-config-I12436) runs; that is the follow-up.
"""

from __future__ import annotations

import io
import json
import logging
import types
from datetime import date, timedelta
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import dates
from collectors import metron_market_data as mmd
from collectors import prices
from data_gate import guard_commissioning as gc
from dates import (
    FUTURE_BAR_FINDING,
    FutureBarError,
    FutureBarFinding,
    FutureBarRefused,
    assert_no_bar_after,
    clip_to_trading_day,
    history_window,
    refuse_bars_after,
)
from tests import test_guard_commissioning_cardinality as card
from tests import test_guard_commissioning_empty_fresh as ef
from tests.test_guard_commissioning_empty_fresh import (  # noqa: F401 -- fixtures, used by name
    _environment,
    clock,
    s3,
)

D = "2026-09-14"
D_PLUS_1 = "2026-09-15"


def _ohlcv(start: str, end: str) -> pd.DataFrame:
    idx = pd.bdate_range(start, end)
    n = len(idx)
    return pd.DataFrame(
        {"Open": np.ones(n), "High": np.ones(n), "Low": np.ones(n),
         "Close": np.linspace(10.0, 20.0, n), "Volume": np.ones(n)},
        index=idx,
    )


# ---------------------------------------------------------------------------
# 1. The primitive
# ---------------------------------------------------------------------------


def test_a_future_bar_fails_the_read_with_a_typed_finding():
    frame = _ohlcv("2026-09-08", D_PLUS_1)
    with pytest.raises(FutureBarRefused) as caught:
        refuse_bars_after(frame, D, dataset="price_cache_refresh", ticker="AAPL")
    finding = caught.value.finding
    assert finding == FutureBarFinding(
        dataset="price_cache_refresh", ticker="AAPL", as_of=D,
        bar_date=D_PLUS_1, last_bar_date=D_PLUS_1, bars_after=1,
    )
    assert isinstance(caught.value, FutureBarError)  # every `except FutureBarError: raise` holds
    record = finding.to_record()
    assert record["finding"] == FUTURE_BAR_FINDING == "pit_future_bar"
    assert record["guard"] == "pit"
    assert record["tracked_issue"] == "alpha-engine-config-I10794"
    assert json.loads(json.dumps(record)) == record
    for text in ("price_cache_refresh[AAPL]", D, D_PLUS_1):
        assert text in str(caught.value)


def test_a_clean_frame_passes_through_unchanged():
    frame = _ohlcv("2026-09-08", D)
    assert refuse_bars_after(frame, D, dataset="x") is frame
    assert clip_to_trading_day(frame, D, label="x[Y]") is frame
    assert refuse_bars_after(None, D, dataset="x") is None
    empty = frame.iloc[:0]
    assert refuse_bars_after(empty, D, dataset="x") is empty


def test_the_fetch_boundary_no_longer_trims():
    """The I10893 clip is the refusal now: same name, same call sites, no silent drop."""
    frame = _ohlcv("2026-09-08", "2026-09-17")
    with pytest.raises(FutureBarRefused) as caught:
        clip_to_trading_day(frame, D, label="price_cache_refresh_retry[MSFT]")
    f = caught.value.finding
    assert (f.dataset, f.ticker) == ("price_cache_refresh_retry", "MSFT")
    assert (f.bar_date, f.last_bar_date, f.bars_after) == (D_PLUS_1, "2026-09-17", 3)
    assert len(frame) == 8  # the caller's frame was not mutated either


def test_a_label_without_a_ticker_names_the_dataset_alone():
    with pytest.raises(FutureBarRefused) as caught:
        clip_to_trading_day(_ohlcv("2026-09-10", D_PLUS_1), D, label="macro_market_prices")
    assert (caught.value.finding.dataset, caught.value.finding.ticker) == ("macro_market_prices", None)


def test_a_tz_aware_index_is_graded_on_the_utc_date():
    frame = _ohlcv("2026-09-10", D_PLUS_1)
    frame.index = frame.index.tz_localize("America/New_York")
    with pytest.raises(FutureBarRefused):
        refuse_bars_after(frame, D, dataset="x")
    refuse_bars_after(frame.iloc[:-1], D, dataset="x")


def test_the_refusal_logs_the_finding_as_json_at_error(caplog):
    caplog.set_level(logging.ERROR, logger=dates.log.name)
    with pytest.raises(FutureBarRefused):
        refuse_bars_after(_ohlcv("2026-09-10", D_PLUS_1), D, dataset="fx", ticker="CHFUSD=X")
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("PIT_FUTURE_BAR ")]
    assert len(lines) == 1
    record = json.loads(lines[0].removeprefix("PIT_FUTURE_BAR "))
    assert (record["dataset"], record["ticker"], record["as_of"], record["bar_date"]) == (
        "fx", "CHFUSD=X", D, D_PLUS_1,
    )


def test_the_write_site_guard_carries_the_finding_too():
    with pytest.raises(FutureBarRefused) as caught:
        assert_no_bar_after(["2026-09-11", D_PLUS_1], D, artifact="reference/price_cache/A.parquet")
    f = caught.value.finding
    assert (f.dataset, f.ticker, f.as_of, f.bar_date) == ("reference/price_cache/A.parquet", None, D, D_PLUS_1)
    assert "A.parquet" in str(caught.value) and "alpha-engine-config-I10893" in str(caught.value)


# ---------------------------------------------------------------------------
# 2. D03's real per-ticker refresh, with the vendor stubbed
# ---------------------------------------------------------------------------


class _FakeS3:
    def __init__(self):
        self.uploads: dict[str, bytes] = {}

    def get_object(self, Bucket, Key):  # noqa: N803
        raise ef.ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    def upload_file(self, path, bucket, key):
        with open(path, "rb") as f:
            self.uploads[key] = f.read()


def _vendor(frame: pd.DataFrame, calls: list[dict]):
    def _download(*_a, **kw):
        calls.append(kw)
        return frame.copy()
    return _download


def test_d03_refresh_fails_on_a_vendor_future_bar_and_uploads_nothing(monkeypatch):
    calls: list[dict] = []
    monkeypatch.setattr(prices.yf, "download", _vendor(_ohlcv("2016-09-14", D_PLUS_1), calls))
    fake = _FakeS3()
    with pytest.raises(FutureBarRefused) as caught:
        prices._refresh_stale(fake, "b", "predictor/price_cache/", ["A"], "10y", 50, trading_day=D)
    f = caught.value.finding
    assert (f.dataset, f.ticker, f.as_of, f.bar_date) == ("price_cache_refresh", "A", D, D_PLUS_1)
    assert fake.uploads == {}
    # The request itself was in bounds: the vendor answered past `end`.
    assert (calls[0]["start"], calls[0]["end"]) == ("2016-09-14", D_PLUS_1)


def test_d03_refresh_publishes_a_clean_answer_unchanged(monkeypatch):
    frame = _ohlcv("2016-09-14", D)
    monkeypatch.setattr(prices.yf, "download", _vendor(frame, []))
    fake = _FakeS3()
    refreshed, failed, _written = prices._refresh_stale(
        fake, "b", "predictor/price_cache/", ["A"], "10y", 50, trading_day=D,
    )
    assert (refreshed, failed) == (1, [])
    written = pd.read_parquet(io.BytesIO(fake.uploads["reference/price_cache/A.parquet"]))
    pd.testing.assert_frame_equal(written, frame, check_freq=False)


# ---------------------------------------------------------------------------
# 3. D03 end to end on the deployed ASL: failed, one page, stand-down, recovery
# ---------------------------------------------------------------------------

TRADING_DAY = card.TRADING_DAY
BUCKET = card.BUCKET
#: The one ticker the run is asked to refresh (``_find_stale_fast`` decides what
#: is asked for, not what is checked, as in the cardinality D03 case).
_TICKER = "AAA"
_PREFIX = card._D03_PREFIX
_NEXT_DAY = (date.fromisoformat(TRADING_DAY) + timedelta(days=1)).isoformat()


def _vendor_frame(kw: dict, *, healthy: bool) -> pd.DataFrame:
    """Whatever window was asked for, answered through D, plus D+1 when faulted."""
    end = date.fromisoformat(kw["end"]) - timedelta(days=1)
    frame = _ohlcv(kw["start"], end.isoformat())
    if not healthy:
        frame = pd.concat([frame, _ohlcv(_NEXT_DAY, _NEXT_DAY)])
    return frame


def _d03_collect(fake: ef.InMemoryS3, healthy: bool) -> dict:
    """``prices.collect`` AND ``prices._refresh_stale`` for real; only the vendor is stubbed."""
    fake.upload_file = lambda path, bucket, key: fake.put_object(
        Bucket=bucket, Key=key, Body=open(path, "rb").read(),
    )
    with (
        mock.patch.object(prices, "boto3", types.SimpleNamespace(client=lambda *_a, **_k: fake)),
        mock.patch.object(prices, "_find_stale_fast", lambda *_a, **_k: [_TICKER]),
        mock.patch.object(prices.yf, "download", lambda *_a, **kw: _vendor_frame(kw, healthy=healthy)),
    ):
        return prices.collect(
            bucket=BUCKET, tickers=[_TICKER], s3_prefix=_PREFIX,
            fetch_period="10y", staleness_threshold_days=1, batch_size=50,
            dry_run=False, reference_date=TRADING_DAY,
        )


def _published_keys() -> list[str]:
    return list(prices.written_keys({"written": {_TICKER: 1}}, _PREFIX))


CASE = card.Case(
    "D03", "prices",
    "future bar: the vendor answers one bar dated after the run's as-of date, past the "
    "requested end, on the attempt and on the on-demand retry",
    _d03_collect,
    refusal=f"pit_future_bar: price_cache_refresh[{_TICKER}] answered a bar dated {_NEXT_DAY}",
    healthy_writes=lambda: dict.fromkeys(_published_keys(), 1),
    extra_outputs=lambda: card.weekly_collector._prices_extra_outputs(_PREFIX),
    auto_skip=False,
)


def test_d03_future_bar_fails_pages_once_and_stands_down(s3, caplog):  # noqa: F811
    caplog.set_level(logging.ERROR)
    prior = {key: card._PRIOR for key in _published_keys()}
    s3.objects.update(prior)

    faulted = card.Execution(s3, CASE, rows_per_attempt=[False, False])
    terminal = faulted.run()

    # 1. A failed manifest naming the finding, on the attempt and on the retry.
    manifests = s3.manifests(CASE)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert CASE.refusal in m["reason"], m["reason"]
        assert f"as_of {TRADING_DAY}" in m["reason"], m["reason"]
    assert [a["status"] for a in faulted.attempts] == ["error", "error"], faulted.attempts

    # 2. Exactly one page, after the on-demand retry.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    assert faulted.path.count("RetryOnDemand") == 1

    # 3. Stand-down: terminal fail, phase marker error, nothing the vendor
    # answered was published (the previous cycle's objects are untouched).
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert s3.marker(CASE)["status"] == "error"
    for key, body in prior.items():
        assert s3.objects[key] == body, f"{key} was overwritten past the refusal"
    assert manifests[0]["outputs"] == []
    findings = [
        json.loads(r.getMessage().removeprefix("PIT_FUTURE_BAR "))
        for r in caplog.records if r.getMessage().startswith("PIT_FUTURE_BAR ")
    ]
    assert len(findings) >= 2, [r.getMessage() for r in caplog.records]
    assert {(f["dataset"], f["ticker"], f["as_of"], f["bar_date"]) for f in findings} == {
        ("price_cache_refresh", _TICKER, TRADING_DAY, _NEXT_DAY),
    }

    # ...and once the vendor answers in bounds, the next run is clean end to end.
    recovered = card.Execution(s3, CASE, rows_per_attempt=[True])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert [m["status"] for m in s3.manifests(CASE)] == ["failed", "failed", "ok"]
    for key in prior:
        published = pd.read_parquet(io.BytesIO(s3.objects[key]))
        assert published.index.max() == pd.Timestamp(TRADING_DAY), key


def test_the_d03_record_lines_are_the_ones_this_walk_proves():
    """``PIT_OBSERVED_BY_UNIT['D03']`` is what the walk above asserts, line for line."""
    observed = gc.PIT_OBSERVED_BY_UNIT["D03"]
    assert observed == gc.PIT_FUTURE_BAR_OBSERVED
    assert gc._observed_for("pit", "D03") == observed
    # Every other unit keeps the class-wide freshness-scan record.
    for unit in ("D13", "D18", "D32"):
        assert gc._observed_for("pit", unit) == gc.PIT_OBSERVED
    doc = gc.record_document(
        "D03", "pit", fault=CASE.fault, test="t", code_sha="c" * 40, as_of="2026-10-26T00:00:00Z",
    )
    assert doc["observed"] == list(observed)
    assert "pit_future_bar" in observed[0]


# ---------------------------------------------------------------------------
# The other fetch-boundary readers: a refusal is never folded into a per-symbol miss
# ---------------------------------------------------------------------------


def _yf_frame(end: str) -> pd.DataFrame:
    idx = pd.bdate_range("2026-09-01", end)
    return pd.DataFrame({"Close": np.linspace(1.0, 2.0, len(idx)), "Open": 1.0}, index=idx)


@pytest.mark.parametrize(
    "read",
    [
        lambda: mmd._yfinance_closes(["A"], trading_day=D),
        lambda: mmd._yfinance_fx(["CHF"], trading_day=D),
        lambda: mmd._yfinance_fx_history(["CHF"], "10y", trading_day=D),
        lambda: mmd._yfinance_close_history_dividend_adjusted(["A"], "10y", trading_day=D),
    ],
    ids=["closes", "fx", "fx_history", "close_history_adjusted"],
)
def test_metron_reads_propagate_the_refusal(monkeypatch, read):
    import yfinance

    monkeypatch.setattr(yfinance, "download", _vendor(_yf_frame(D_PLUS_1), []))
    monkeypatch.setattr(mmd.time, "sleep", lambda *_: None)
    with pytest.raises(FutureBarRefused) as caught:
        read()
    assert caught.value.finding.bar_date == D_PLUS_1


def test_history_window_still_requests_through_d_only():
    assert history_window(D, "10y") == (date(2016, 9, 14), date(2026, 9, 15))
