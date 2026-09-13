"""Vectorized QoQ deltas keyed on (ticker, filer CIK) — alpha-engine-config-I10529.

The previous shape scanned every INFOTABLE row once per ticker
(O(tickers x rows)) and used CUSIP+shares as a fund proxy; it ran 42 minutes
to the job timeout on 2026-09-13 (nousergon-data run 34780401480) and
counted distinct CUSIPs as "funds".
"""
from __future__ import annotations

import pandas as pd

import data.derived.inst_ownership as inst


def _infotable(rows):
    return pd.DataFrame(rows, columns=["accession_number", "cusip", "shares", "market_value"])


CUSIP_TO_TICKER = {"AAA000001": "AAA", "BBB000002": "BBB", "CCC000003": "CCC"}
ACC_TO_CIK = {"acc-f1-cur": "F1", "acc-f2-cur": "F2", "acc-f3-cur": "F3",
              "acc-f1-pri": "F1", "acc-f2-pri": "F2", "acc-f4-pri": "F4"}


def test_deltas_are_per_fund_by_cik_and_vectorized():
    current = _infotable([
        ("acc-f1-cur", "AAA000001", 100, 1000),   # F1 increases AAA (was 50)
        ("acc-f2-cur", "AAA000001", 20, 200),     # F2 decreases AAA (was 30)
        ("acc-f3-cur", "AAA000001", 5, 50),       # F3 new in AAA
        ("acc-f1-cur", "BBB000002", 10, 100),     # BBB no prior
        ("acc-f1-cur", "ZZZ999999", 1, 1),        # unmapped cusip — dropped
    ])
    prior = _infotable([
        ("acc-f1-pri", "AAA000001", 50, 500),
        ("acc-f2-pri", "AAA000001", 30, 300),
        ("acc-f4-pri", "AAA000001", 7, 70),       # F4 exited AAA
        ("acc-f4-pri", "CCC000003", 9, 90),       # CCC only in prior — no row
    ])
    rows = {r.ticker: r for r in inst._compute_qoq_deltas(
        current, prior, CUSIP_TO_TICKER, "2026Q1", accession_to_cik=ACC_TO_CIK)}
    assert set(rows) == {"AAA", "BBB"}
    a = rows["AAA"]
    assert a.n_funds_holding == 3
    assert a.total_shares_held == 125 and a.total_value_usd == 1250
    assert a.shares_qoq_change == 125 - 87 and a.value_qoq_change == 1250 - 870
    assert (a.n_funds_increasing, a.n_funds_decreasing, a.n_funds_new, a.n_funds_exited) == (1, 1, 1, 1)
    b = rows["BBB"]
    assert b.n_funds_holding == 1 and b.shares_qoq_change is None and b.value_qoq_change is None
    assert (b.n_funds_new, b.n_funds_exited) == (1, 0)


def test_universe_filter_is_applied_before_aggregation():
    current = _infotable([("acc-f1-cur", "AAA000001", 1, 1), ("acc-f1-cur", "BBB000002", 1, 1)])
    rows = inst._compute_qoq_deltas(current, pd.DataFrame(), CUSIP_TO_TICKER, "2026Q1",
                                    accession_to_cik=ACC_TO_CIK, keep_tickers={"BBB"})
    assert [r.ticker for r in rows] == ["BBB"]


def test_without_cik_map_the_accession_is_the_fund_key():
    current = _infotable([("x1", "AAA000001", 1, 1), ("x2", "AAA000001", 2, 2)])
    rows = inst._compute_qoq_deltas(current, pd.DataFrame(), CUSIP_TO_TICKER, "2026Q1")
    assert rows[0].n_funds_holding == 2


def test_no_mapped_rows_returns_empty():
    current = _infotable([("x1", "ZZZ999999", 1, 1)])
    assert inst._compute_qoq_deltas(current, pd.DataFrame(), CUSIP_TO_TICKER, "2026Q1") == []


def test_top5_concentration_only_with_five_or_more_funds():
    cur = _infotable([(f"a{i}", "AAA000001", sh, sh) for i, sh in enumerate([50, 20, 10, 10, 5, 5])])
    cik = {f"a{i}": f"F{i}" for i in range(6)}
    row = inst._compute_qoq_deltas(cur, pd.DataFrame(), CUSIP_TO_TICKER, "2026Q1", accession_to_cik=cik)[0]
    assert row.n_funds_holding == 6
    assert abs(row.top5_concentration_pct - (95 / 100 * 100)) < 1e-9
    few = _infotable([("a0", "AAA000001", 1, 1)])
    assert inst._compute_qoq_deltas(few, pd.DataFrame(), CUSIP_TO_TICKER, "2026Q1")[0].top5_concentration_pct is None
