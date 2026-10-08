"""The universe backfill write keeps previous ArcticDB versions
(alpha-engine-config-I12115).

``daily_append``'s Phase 2 sends a row dated before a symbol's latest row
through ``write_batch`` (a full-series rewrite), because ``update`` refuses
non-monotonic inserts. That is the path ``daily-heal`` takes for every past
day it heals. It used to pass ``prune_previous_versions=True``.

On 2026-10-07 daily-heal healed 2026-09-30 through it. For the 149 symbols
of chunk 1/7, all ``tdata`` PUTs landed in the first minute, then the
``ver``/``vref`` commits trickled in at ~11 symbols/min for ~13 min while
ArcticDB walked each ~830-deep version chain, then ~19 min of
``DeleteObjects`` removed ~121k index/data/stats keys. The 3600s heal
timeout fired with 6 of 7 chunks unwritten. The prune also destroys the
versions ``shadow/recompute_lineage.py`` (parity ``v1_cause`` evidence) and
crucible-research's faithful replay read ``as_of``.
"""
from __future__ import annotations

import pandas as pd
import pytest

from tests.conftest import recent_trading_day_str
from tests.test_daily_append_skip_if_exists import _patch_targets


@pytest.fixture(autouse=True)
def _disable_factor_momentum_daily(monkeypatch):
    # Isolate from the daily factor-momentum / loading second passes, as the
    # other _patch_targets users do; they have their own tests.
    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")
    monkeypatch.setenv("FACTOR_LOADING_ZSCORE_DAILY_ENABLED", "false")


def test_universe_backfill_write_batch_does_not_prune(monkeypatch):
    from builders.daily_append import daily_append

    today_str = recent_trading_day_str()
    universe = ["AAPL", "MSFT"]
    universe_lib, _, _ = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=True,
        today_str=today_str,
    )
    # A stored row AFTER the day being written makes it a heal of a past
    # day: target_ts < hist.index.max() routes it to write_batch.
    later = pd.Timestamp(today_str) + pd.offsets.BDay(1)
    hist = universe_lib.read_batch.return_value[0].data.copy()
    hist.loc[later] = hist.iloc[-1]
    for item in universe_lib.read_batch.return_value:
        item.data = hist.copy()

    daily_append(date_str=today_str, skip_if_exists=False)

    assert universe_lib.write_batch.called, (
        "fixture did not reach the backfill branch; the test proves nothing"
    )
    written = {
        p.symbol
        for call in universe_lib.write_batch.call_args_list
        for p in call.args[0]
    }
    assert set(universe) <= written
    for call in universe_lib.write_batch.call_args_list:
        assert call.kwargs.get("prune_previous_versions", False) is False, (
            "the backfill write_batch pruned previous versions: "
            f"{call.kwargs!r}"
        )
