"""alpha-engine-config-I12436: a reviewed declared row of a share-distribution
class explains a CLOSE-axis residual inside its bound.

yfinance folds a spin-off or a stock dividend into its adjusted ``Close``; the
polygon raw ``Close`` the EDGAR rebuild reads keeps the real ex-date price. The
gate on close therefore stopped the 2026-10-31 build on named spin-off names.
A declared row explains such a residual only while it is inside the row's bound
and before the row expires, and never on a cash class.

Every fixture is synthetic.
"""

from __future__ import annotations

import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m

from tests.test_crsp_basis_gate_on_close_i12436 import _audit, _flat_name, _run
from tests.test_migrate_universe_crsp_basis import _FakePolygon


def _row(ticker="X", cls="spin_off", bound=0.6, expires="2027-01-31", when="2026-03"):
    return m.DeclaredDivergence(
        ticker, cls, "synthetic event", when, bound, "synthetic evidence", expires,
    )


def _rec(ticker="X", dev=0.4, status="out_of_tol", explained=False):
    # The residual steps inside the declared month and is flat before it
    # (alpha-engine-config-I13034); tests/test_crsp_basis_declared_step_i13034.py
    # covers a residual that does not.
    return m.ReconcileRecord(
        ticker=ticker, status=status, n_common_dates=8, max_rel_dev=dev,
        max_dev_date="2026-03-20", explained=explained,
        explanation="CLOSE axis: OUT-OF-TOL",
        step_date="2026-03-10", pre_step_max_rel_dev=0.0,
    )


def _explain(rec, row, today="2026-11-01"):
    return m.explain_close_by_declared([rec], today=today, declared=(row,))


@pytest.mark.parametrize("cls", ["spin_off", "stock_dividend"])
def test_a_residual_inside_the_bound_is_explained_and_named(cls):
    (out,), rows = _explain(_rec(dev=0.4), _row(cls=cls))
    assert out.explained and "explained by declared" in out.explanation
    assert [r["ticker"] for r in rows] == ["X"] and rows[0]["class"] == cls
    assert rows[0]["max_rel_dev_bound"] == 0.6


@pytest.mark.parametrize("rec,row,today", [
    (_rec(dev=0.61), _row(bound=0.6), "2026-11-01"),                      # outside the bound
    (_rec(dev=0.4), _row(expires="2026-11-01"), "2026-11-01"),             # expired on the day
    (_rec(dev=0.4), _row(cls="special_distribution"), "2026-11-01"),       # cash class
    (_rec(dev=0.4), _row(ticker="Y"), "2026-11-01"),                       # no row for the name
    (_rec(dev=float("inf"), status="no_overlap"), _row(), "2026-11-01"),   # never a skipped name
])
def test_everything_else_stays_unexplained(rec, row, today):
    (out,), rows = _explain(rec, row, today)
    assert not out.explained and rows == []


def test_an_operator_acknowledged_record_is_returned_as_it_came():
    rec = _rec(explained=True)
    (out,), rows = _explain(rec, _row())
    assert out is rec and rows == []


def test_the_gate_passes_on_a_declared_row_and_fails_without_it(monkeypatch, tmp_path):
    idx, raw = _flat_name()
    raw.attrs[m.REFERENCE_CLOSE_ATTR] = pd.Series([50.0] * 3 + [100.0] * 5, index=idx)
    old_close = pd.Series([100.0] * 8, index=idx)
    client = _FakePolygon(splits={}, dividends={})

    # The reference steps on idx[3] (2024-01-05); the row declares that day.
    monkeypatch.setattr(m, "DECLARED_RECONCILE_DIVERGENCES", (_row(bound=0.9, when="2024-01-05"),))
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client, apply=True)
    result = m.migrate_universe_crsp_basis(spill_dir=str(tmp_path), **kw)
    assert result["status"] == "ok" and result["unexplained_count"] == 0
    assert [r["ticker"] for r in _audit(s3)["close_declared_explained"]] == ["X"]
    assert scratch.write.call_count == 1

    monkeypatch.setattr(m, "DECLARED_RECONCILE_DIVERGENCES", (_row(bound=0.1, when="2024-01-05"),))
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client, apply=True)
    with pytest.raises(RuntimeError, match="FAILED LOUD.*close axis"):
        m.migrate_universe_crsp_basis(spill_dir=str(tmp_path), **kw)
    assert scratch.write.call_count == 0
    assert _audit(s3)["close_declared_explained"] == []


def test_the_shipped_table_covers_the_classified_close_axis_names():
    table = {d.ticker: d for d in m.DECLARED_RECONCILE_DIVERGENCES}
    assert {t: table[t].divergence_class for t in
            ("BDX", "CBSH", "CMCSA", "CTVA", "FDX", "HON", "WDC")} == {
        "BDX": "spin_off", "CBSH": "stock_dividend", "CMCSA": "spin_off",
        "CTVA": "spin_off", "FDX": "spin_off", "HON": "spin_off", "WDC": "spin_off",
    }
    # Names with no confirmed event stay undeclared and keep failing the gate.
    assert not {"FNF", "MIDD", "RYN", "SPGI"} & set(table)
    assert all(pd.Timestamp(d.expires) > pd.Timestamp("2026-11-01") for d in table.values())
