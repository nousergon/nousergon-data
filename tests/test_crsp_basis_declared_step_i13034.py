"""alpha-engine-config-I13034: a declared spin-off / stock-dividend row explains
a CLOSE-axis residual only when the residual is THAT event's.

A share distribution one side folds in and the other does not is one level
step at its ex-date, with a flat residual before it. Before this, a row
explained any residual inside its bound wherever it started, so an unrelated
missing split or bad close for a name with a wide bound (CTVA, 0.90) passed the
gate and got a verified receipt. Now the residual must step inside the row's
event window and be within the reconcile tolerance before the step; otherwise
the row does not explain it and the gate fails as it would with no row.

Every fixture is synthetic.
"""

from __future__ import annotations

import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m

from tests.test_crsp_basis_gate_on_close_i12436 import _audit, _run
from tests.test_migrate_universe_crsp_basis import _FakePolygon

TODAY = "2026-11-01"
TOL = m.DEFAULT_RECONCILE_REL_TOL


def _row(when="2026-03-16", bound=0.6, cls="spin_off"):
    return m.DeclaredDivergence(
        "X", cls, "synthetic event", when, bound, "synthetic evidence", "2027-01-31",
    )


def _idx(n=40, start="2026-02-02"):
    return pd.bdate_range(start, periods=n)


def _close_record(reference: pd.Series, new: pd.Series | None = None):
    new = pd.Series(100.0, index=reference.index) if new is None else new
    return m.reconcile_close("X", new, reference, rel_tol=TOL)


def _stepped(idx, at: str, factor=2.0, base=100.0) -> pd.Series:
    """A reference that is ``base`` before ``at`` and ``base * factor`` from it:
    the split-only new close stays flat, so the residual steps once, at ``at``."""
    return pd.Series([base if d < pd.Timestamp(at) else base * factor for d in idx], index=idx)


def _explain(rec, row):
    (out,), rows = m.explain_close_by_declared([rec], today=TODAY, declared=(row,), rel_tol=TOL)
    return out, rows


def test_a_step_on_the_declared_ex_date_is_explained():
    idx = _idx()
    rec = _close_record(_stepped(idx, "2026-03-16"))
    assert rec.status == "out_of_tol" and rec.step_date == "2026-03-16"
    assert rec.pre_step_max_rel_dev == pytest.approx(0.0)
    out, rows = _explain(rec, _row(when="2026-03-16"))
    assert out.explained and "steps at 2026-03-16" in out.explanation
    assert rows[0]["step_date"] == "2026-03-16"


def test_a_step_a_few_days_off_a_declared_day_is_still_the_event():
    # e.g. no reference bar on the ex-date itself: the move lands next common date.
    idx = _idx()
    out, _ = _explain(_close_record(_stepped(idx, "2026-03-19")), _row(when="2026-03-16"))
    assert out.explained


def test_a_step_on_a_non_event_date_is_not_explained():
    idx = _idx()
    rec = _close_record(_stepped(idx, "2026-02-20"))  # weeks before the declared day
    out, rows = _explain(rec, _row(when="2026-03-16"))
    assert not out.explained and rows == []
    assert "NOT explained by declared spin_off" in out.explanation
    assert "steps at 2026-02-20, outside the declared event window" in out.explanation


def test_a_step_after_the_window_is_not_explained():
    idx = _idx()
    out, _ = _explain(_close_record(_stepped(idx, "2026-03-25")), _row(when="2026-03-16"))
    assert not out.explained


def test_a_residual_that_is_not_flat_before_the_event_is_not_explained():
    # A 5% residual (an unrelated bad stretch) BEFORE the declared step: the step
    # itself is on the ex-date and inside the bound, but the row cannot explain
    # what came before it.
    idx = _idx()
    ref = _stepped(idx, "2026-03-16")
    ref[(idx >= pd.Timestamp("2026-02-16")) & (idx < pd.Timestamp("2026-02-23"))] = 105.0
    rec = _close_record(ref)
    assert rec.step_date == "2026-03-16"
    assert rec.pre_step_max_rel_dev > TOL
    out, rows = _explain(rec, _row(when="2026-03-16"))
    assert not out.explained and rows == []
    assert "not flat before its step at 2026-03-16" in out.explanation


def test_an_unrelated_move_after_the_declared_step_is_not_explained():
    # codex finding 647df950aaefd1ca: the declared step is the largest move and
    # flat before it, but a later 20% jump (an unrelated missing split or bad
    # close) sits inside the 0.6 bound. It is not the event's residual.
    idx = _idx()
    ref = _stepped(idx, "2026-03-16")
    ref[idx >= pd.Timestamp("2026-03-23")] = 240.0
    rec = _close_record(ref)
    assert rec.step_date == "2026-03-16" and rec.pre_step_max_rel_dev == pytest.approx(0.0)
    assert rec.post_step_max_rel_dev > TOL
    out, rows = _explain(rec, _row(when="2026-03-16"))
    assert not out.explained and rows == []
    assert "moves again after its step at 2026-03-16" in out.explanation


def test_drift_within_tolerance_after_the_step_is_still_flat():
    idx = _idx()
    ref = _stepped(idx, "2026-03-16")
    ref[idx >= pd.Timestamp("2026-03-23")] = 202.0  # 1% < 2% tolerance
    out, _ = _explain(_close_record(ref), _row(when="2026-03-16"))
    assert out.explained


def test_drift_within_tolerance_before_the_step_is_still_flat():
    idx = _idx()
    ref = _stepped(idx, "2026-03-16")
    ref[idx < pd.Timestamp("2026-03-16")] = 101.0  # 1% < 2% tolerance
    ref.iloc[0] = 100.0
    out, _ = _explain(_close_record(ref), _row(when="2026-03-16"))
    assert out.explained


def test_a_month_sourced_row_takes_its_month_plus_the_margin():
    idx = _idx()
    assert _explain(_close_record(_stepped(idx, "2026-03-27")), _row(when="2026-03"))[0].explained
    assert _explain(_close_record(_stepped(idx, "2026-04-02")), _row(when="2026-03"))[0].explained
    assert not _explain(_close_record(_stepped(idx, "2026-02-13")), _row(when="2026-03"))[0].explained


def test_a_year_only_row_explains_nothing_on_the_close_axis():
    idx = _idx()
    out, rows = _explain(_close_record(_stepped(idx, "2026-03-16")), _row(when="2026"))
    assert not out.explained and rows == []
    assert "year-only" in out.explanation
    assert m.declared_event_window(_row(when="2026")) is None


def test_a_record_with_no_located_step_is_not_explained():
    rec = m.ReconcileRecord(
        ticker="X", status="out_of_tol", n_common_dates=8, max_rel_dev=0.4,
        max_dev_date="2026-03-20", explained=False, explanation="CLOSE axis: OUT-OF-TOL",
    )
    out, rows = _explain(rec, _row())
    assert not out.explained and rows == []


@pytest.mark.parametrize("when,lo,hi", [
    ("2026-10-01", "2026-09-26", "2026-10-06"),
    ("2026-02", "2026-01-27", "2026-03-05"),
])
def test_the_event_window(when, lo, hi):
    assert m.declared_event_window(_row(when=when)) == (pd.Timestamp(lo), pd.Timestamp(hi))


def test_the_step_fields_reach_the_audit_record():
    d = _close_record(_stepped(_idx(), "2026-03-16")).to_dict()
    assert d["step_date"] == "2026-03-16" and d["pre_step_max_rel_dev"] == pytest.approx(0.0)


def test_the_gate_fails_on_a_declared_name_whose_residual_steps_off_the_event(
    monkeypatch, tmp_path,
):
    idx = pd.bdate_range("2024-01-02", periods=8)
    raw = pd.DataFrame({"Close": [100.0] * 8, "Volume": [1_000_000] * 8}, index=idx)
    raw.attrs[m.REFERENCE_CLOSE_ATTR] = pd.Series([50.0] * 3 + [100.0] * 5, index=idx)
    old_close = pd.Series([100.0] * 8, index=idx)
    client = _FakePolygon(splits={}, dividends={})

    # Same residual, bound 0.9 covers it; the row's event is a month later.
    monkeypatch.setattr(m, "DECLARED_RECONCILE_DIVERGENCES", (
        m.DeclaredDivergence("X", "spin_off", "synthetic event", "2024-02-15", 0.9,
                             "synthetic evidence", "2027-01-31"),
    ))
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client, apply=True)
    with pytest.raises(RuntimeError, match="FAILED LOUD.*close axis"):
        m.migrate_universe_crsp_basis(spill_dir=str(tmp_path), **kw)
    assert scratch.write.call_count == 0
    assert _audit(s3)["close_declared_explained"] == []
