"""The daily cross-sectional second pass, written to ArcticDB in ONE version.

`builders/daily_append` writes each universe symbol's row for the day, and then
two cross-sectional passes fill in the columns a per-ticker loop cannot compute:
`factor_momentum_ratio` (`features.factor_momentum`) and the nine Barra
`*_zscore` loadings (`features.cross_sectional`). Until alpha-engine-config-I11792
each pass read today's rows back and wrote them with its own `update_batch`, so
every one of the ~907 universe symbols got THREE new ArcticDB versions per burst
a few minutes apart — measured in the 2026-10-08..10 `alpha-engine-research`
access logs as 3 x 4 PUTs (tdata + tindex + ver + vref) per symbol per burst,
~42k PUTs a day.

The two passes read only the raw per-ticker columns the first write stored
(`DEFAULT_FACTOR_LOADINGS` + `Close`, and `FACTOR_LOADING_SOURCES`) and write
disjoint output columns, so neither depends on the other's output. This module
computes both, then reads today's rows ONCE and writes both column sets in ONE
`update_batch`. The row that lands is the row the two sequential writes left
behind; there is one version of it instead of two.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def write_latest_values(
    universe_lib,
    as_of_ts,
    updates: dict[str, dict[str, float]],
    *,
    canonical_fn=None,
    label: str = "second pass",
) -> dict:
    """Set ``updates[ticker][column]`` on each ticker's ``as_of_ts`` row, in one write.

    A full-row update, because the universe lib is static-schema (no
    column-wise update); today's row already carries every second-pass column
    as NaN from daily_append's schema-align, so the descriptor matches.

    Returns ``{"status": "ok", "written": [...], "write_fail": [...]}``, or
    ``{"status": "read_error", "error": ...}`` when today's rows could not be
    read. A ticker with no ``as_of_ts`` row is in neither list, exactly as the
    per-pass writers skipped it. Never raises.
    """
    from arcticdb.version_store.library import ReadRequest, UpdatePayload

    as_of_ts = pd.Timestamp(as_of_ts)
    write_tickers = list(updates)
    try:
        today_results = universe_lib.read_batch(
            [ReadRequest(symbol=t, date_range=(as_of_ts, as_of_ts)) for t in write_tickers]
        )
    except Exception as exc:
        log.warning("%s: today read_batch failed (skipped): %s", label, exc)
        return {"status": "read_error", "error": str(exc)}

    payloads = []
    for t, res in zip(write_tickers, today_results):
        data = getattr(res, "data", None)
        if data is None or data.empty or as_of_ts not in data.index:
            continue
        row = data.copy()
        for col, value in updates[t].items():
            row.loc[as_of_ts, col] = np.float32(value)
        out = canonical_fn(row) if canonical_fn is not None else row
        payloads.append(UpdatePayload(symbol=t, data=out))

    tickers = [p.symbol for p in payloads]
    if not payloads:
        return {"status": "ok", "written": [], "write_fail": []}
    try:
        universe_lib.update_batch(payloads)
    except Exception as exc:
        log.warning("%s: update_batch failed (skipped): %s", label, exc)
        return {"status": "ok", "written": [], "write_fail": tickers}
    return {"status": "ok", "written": tickers, "write_fail": []}


def update_cross_sectional_latest(
    universe_lib,
    tickers,
    as_of_ts,
    *,
    factor_momentum: bool = True,
    factor_loading_zscores: bool = True,
    canonical_fn=None,
) -> tuple[dict | None, dict | None]:
    """Both second passes, one ArcticDB version per symbol.

    Returns ``(fm_result, flz_result)`` — each the result dict the
    corresponding ``update_*_latest`` function has always returned (``status``,
    ``tickers_written``, ``tickers_all_nan``, ``read_fail``, ``write_fail``,
    ``n_computed``), or ``None`` when that pass is disabled. A pass whose
    COMPUTE fails reports ``status: "error"`` and contributes nothing to the
    write, and the other pass is still written, as before. Never raises.
    """
    from features.cross_sectional import (
        compute_factor_loading_zscores_latest,
        finish_factor_loading_zscores_result,
    )
    from features.factor_momentum import (
        compute_factor_momentum_latest,
        finish_factor_momentum_result,
    )

    as_of_ts = pd.Timestamp(as_of_ts)
    fm_values: dict[str, float] | None = None
    z_values: dict[str, dict[str, float]] | None = None
    fm_computed: dict | None = None
    z_computed: dict | None = None

    if factor_momentum:
        try:
            fm_values, fm_computed = compute_factor_momentum_latest(universe_lib, tickers, as_of_ts)
        except Exception as exc:  # belt-and-suspenders — never fail the daily pipeline
            log.warning("Factor-momentum daily update FAILED (OBSERVE, non-fatal): %s", exc)
            fm_computed = {"status": "error", "error": str(exc), "tickers_written": 0}
    if factor_loading_zscores:
        try:
            z_values, z_computed = compute_factor_loading_zscores_latest(universe_lib, tickers, as_of_ts)
        except Exception as exc:
            log.warning("Factor-loading z-score daily update FAILED (non-fatal): %s", exc)
            z_computed = {"status": "error", "error": str(exc), "tickers_written": 0}

    updates: dict[str, dict[str, float]] = {}
    for t, v in (fm_values or {}).items():
        updates.setdefault(t, {})["factor_momentum_ratio"] = v
    for t, cols in (z_values or {}).items():
        updates.setdefault(t, {}).update(cols)

    written: dict = {"status": "ok", "written": [], "write_fail": []}
    if updates:
        written = write_latest_values(
            universe_lib, as_of_ts, updates,
            canonical_fn=canonical_fn, label="cross-sectional second pass",
        )

    def _close(values, computed, finish):
        if computed is None:
            return None
        if values is None:
            return computed  # disabled, failed or could not proceed: already final
        if written["status"] == "read_error":
            return {"status": "read_error", "error": written["error"], "tickers_written": 0,
                    "read_fail": computed["read_fail"]}
        return finish(
            computed, as_of_ts,
            n_written=sum(1 for t in written["written"] if t in values),
            write_fail=sum(1 for t in written["write_fail"] if t in values),
        )

    return (
        _close(fm_values, fm_computed, finish_factor_momentum_result),
        _close(z_values, z_computed, finish_factor_loading_zscores_result),
    )
