"""Regression tests for the chunked universe pass in builders.daily_append.

PR target: process the universe in fixed-size chunks instead of one
~900-ticker batch in memory. Caps peak resident memory at ~one chunk's
worth so daily_append fits on a 2GB t3.small alongside daemon + IB
Gateway + SSM agent.

Incident anchor (2026-05-11): MorningEnrich python died partway through
the universe write loop (PROCESS_GONE near ticker SOLS, ~876/900) when
the un-chunked Phase 1+2 pass held ~180MB of ticker histories
simultaneously in memory, on top of the daily_append base working set,
on top of a (separately-fixed) crash-looping daemon. Chunking caps each
iteration's resident memory and gc.collect() between chunks forces
release of cycled DataFrames whose BlockManager reference cycles defer
freeing.

These tests pin:

  - With N tickers and UNIVERSE_CHUNK_SIZE=K, read_batch is called
    ceil(N/K) times — one per chunk. Each call is over its chunk's
    ticker slice, not the whole universe.
  - update_batch / write_batch are called once per chunk too.
  - n_ok counter accumulates correctly across chunks (no chunk-local
    counter masking the global state).
  - gc.collect() fires between chunks (load-bearing for actual
    memory release given pandas BlockManager cycles).
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tests.test_daily_append_skip_if_exists import _patch_targets


@pytest.fixture(autouse=True)
def _disable_factor_momentum_daily(monkeypatch):
    # L4484: isolate the chunk-count assertions from the daily factor-momentum
    # second pass (its extra read_batch/update_batch calls would inflate the
    # per-chunk call counts pinned here). The pass has its own tests.
    monkeypatch.setenv("FACTOR_MOMENTUM_DAILY_ENABLED", "false")


def test_universe_pass_chunks_read_batch_calls(monkeypatch):
    """Stock-ticker count + UNIVERSE_CHUNK_SIZE=2 → ceil(N/2) read_batch
    calls (one per chunk, sizes [2, 2, ..., remainder]). Pins the chunk
    iteration count against a future refactor accidentally reverting to
    a single big batch.

    Effective chunk-input size = universe-symbol count + 2:
      * ``XLRE`` (len=4) escapes the ``_is_sector_etf`` len==3 check, and
      * ``SPY`` is now admitted via ``_UNIVERSE_EXTRA`` (full universe
        member since the 2026-05 SPY-as-held-core promotion).
    """
    from builders import daily_append as _da
    from builders.daily_append import daily_append

    monkeypatch.setattr(_da, "UNIVERSE_CHUNK_SIZE", 2)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # 4 universe + XLRE leak + SPY universe-extra = 6 → 3 chunks at K=2
    universe = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    universe_lib, _, _ = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=False,
        today_str=today_str,
    )

    daily_append(date_str=today_str, skip_if_exists=False)

    # ceil(6/2) = 3 chunks → 3 read_batch invocations
    assert universe_lib.read_batch.call_count == 3, (
        f"Expected 3 chunked read_batch calls (6 effective stock tickers / chunk=2), "
        f"got {universe_lib.read_batch.call_count}"
    )

    # Sizes sum to total + at most one is < UNIVERSE_CHUNK_SIZE
    call_sizes = [
        len(call.args[0]) for call in universe_lib.read_batch.call_args_list
    ]
    assert sum(call_sizes) == 6
    assert all(s <= 2 for s in call_sizes)
    # All but the last chunk are at chunk_size
    assert call_sizes[:-1] == [2, 2]


def test_universe_pass_chunks_write_batches_too(monkeypatch):
    """update_batch (the more common path — append-at-head) fires once
    per chunk, not once globally. Same call count as read_batch (one
    write phase per chunk's compute phase)."""
    from builders import daily_append as _da
    from builders.daily_append import daily_append

    monkeypatch.setattr(_da, "UNIVERSE_CHUNK_SIZE", 2)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # 3 universe + XLRE leak + SPY universe-extra = 5 effective → 3 chunks at K=2
    universe = ["AAPL", "MSFT", "GOOGL"]
    universe_lib, _, _ = _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=False,
        today_str=today_str,
    )

    daily_append(date_str=today_str, skip_if_exists=False)

    # ceil(5/2) = 3 chunks → 3 update_batch invocations
    assert universe_lib.update_batch.call_count == 3, (
        f"Expected 3 chunked update_batch calls (5 effective tickers / chunk=2), "
        f"got {universe_lib.update_batch.call_count}"
    )
    # Read and write call counts should match (1 read + 1 write per chunk)
    assert universe_lib.read_batch.call_count == universe_lib.update_batch.call_count


def test_universe_pass_gc_collect_called_between_chunks(monkeypatch):
    """gc.collect() is load-bearing: pandas BlockManager holds reference
    cycles that del-alone can't break. Without explicit collection the
    chunked write_payloads.combined frames accumulate across iterations
    and the memory savings degrade. Pinned via gc.collect call count."""
    from builders import daily_append as _da
    from builders.daily_append import daily_append

    monkeypatch.setattr(_da, "UNIVERSE_CHUNK_SIZE", 2)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    universe = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]  # 5 → 3 chunks
    _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=False,
        today_str=today_str,
    )

    with patch.object(_da.gc, "collect") as mock_collect:
        daily_append(date_str=today_str, skip_if_exists=False)

    # One gc.collect per chunk (3 chunks → ≥3 calls; allow for any
    # incidental gc.collect calls inside libs we don't control).
    assert mock_collect.call_count >= 3, (
        f"Expected ≥3 gc.collect calls (one per chunk), "
        f"got {mock_collect.call_count}"
    )


def test_universe_pass_n_ok_accumulates_across_chunks(monkeypatch):
    """The n_ok / n_partial / n_err counters live in daily_append's outer
    scope, not in a chunk-local. Without that, only the last chunk's
    counts would survive; tickers from earlier chunks would silently
    vanish from the result dict."""
    from builders.daily_append import daily_append
    from builders import daily_append as _da

    monkeypatch.setattr(_da, "UNIVERSE_CHUNK_SIZE", 2)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    universe = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA"]  # 6 → 3 chunks
    _patch_targets(
        monkeypatch,
        universe_symbols=universe,
        today_in_hist=False,
        today_str=today_str,
    )

    result = daily_append(date_str=today_str, skip_if_exists=False)

    # All effective stock tickers (universe + XLRE leak + SPY universe-extra)
    # should land in n_ok or n_partial. Without correct accumulation across
    # chunks, only the last chunk's counts survive.
    expected = len(universe) + 2  # +1 XLRE leak, +1 SPY (_UNIVERSE_EXTRA)
    counted = result["tickers_appended"] + result["tickers_partial"]
    assert counted == expected, (
        f"Counter accumulation across chunks broken: "
        f"n_ok={result['tickers_appended']} + "
        f"n_partial={result['tickers_partial']} = {counted}, "
        f"expected {expected}"
    )
