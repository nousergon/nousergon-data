"""alpha-engine-config-I12436, "Gate on close": the build gates on the axis the
EDGAR rebuild reads.

The EDGAR PIT rebuild reads only the split-only ``Close`` of ``universe_crsp``
(option A, Brian 2026-10-23). The build used to gate only on
``total_return_close`` against the live dividend-adjusted ``Close``, and failed
on 46 dividend residuals the rebuild never reads, while nothing checked the
``Close`` it does read. Now:

* the ``close`` axis (split-only ``Close`` vs yfinance's split-adjusted,
  dividend-unadjusted ``Close``) GATES pass/fail, the scratch write and the
  receipt, and fails loud on a missing, doubled or mis-ratio'd split;
* the ``total_return`` axis is still reconciled, still logged at ERROR, and
  recorded in the audit and the receipt as ``total_return_axis_trusted``.

Every fixture is synthetic.
"""

from __future__ import annotations

import json
import logging
import sys

import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m

from tests.test_crsp_basis_raw_closes_i12674 import _fetch, _market, _split
from tests.test_migrate_universe_crsp_basis import _FakePolygon, _patch_orchestration


class _S3:
    def __init__(self):
        self.puts: dict[str, dict] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.puts[Key] = json.loads(Body)


def _audit(s3: _S3) -> dict:
    keys = sorted(k for k in s3.puts if "/verified/" not in k)
    return s3.puts[keys[-1]]


def _receipt(s3: _S3) -> dict | None:
    return s3.puts.get(m.verified_receipt_key("universe_crsp"))


def _flat_name(n=8, close=100.0):
    idx = pd.bdate_range("2024-01-02", periods=n)
    raw = pd.DataFrame({"Close": [close] * n, "Volume": [1_000_000] * n}, index=idx)
    raw.attrs[m.REFERENCE_CLOSE_ATTR] = pd.Series([close] * n, index=idx)
    return idx, raw


def _run(monkeypatch, *, raw, old_close, client, apply=False, gate_axis=None):
    _live, scratch = _patch_orchestration(
        monkeypatch, old_closes={"X": old_close}, gate_axis=gate_axis or "close",
    )
    s3 = _S3()
    monkeypatch.setattr(m.boto3, "client", lambda *a, **k: s3)
    kw = dict(apply=apply, raw_fetch=lambda t: raw, client=client, rel_tol=0.02,
              workers=1, macro={}, sector_map={}, fundamentals={}, alt_data={})
    return s3, scratch, kw


def test_the_build_defaults_to_the_close_gate():
    assert m.DEFAULT_GATE_AXIS == "close"
    assert set(m.GATE_AXES) == {"close", "total_return"}


def test_fetch_raw_prices_keeps_yfinance_split_adjusted_close_as_the_reference(monkeypatch):
    truth, _raw, fake_yf, client = _market("SYN", yf_splits=[("2026-03-16", 2.0)], seed=3)
    raw_df = _fetch(monkeypatch, "SYN", fake_yf, client)
    ref = raw_df.attrs[m.REFERENCE_CLOSE_ATTR]
    pd.testing.assert_series_equal(ref, truth, check_names=False, rtol=1e-12)


def test_close_axis_passes_a_correct_split_and_fails_a_missing_one(monkeypatch):
    ex = "2026-03-16"
    _truth, _raw, fake_yf, client = _market("SYN", yf_splits=[(ex, 2.0)], seed=4)
    raw_df = _fetch(monkeypatch, "SYN", fake_yf, client)
    ref = raw_df.attrs[m.REFERENCE_CLOSE_ATTR]

    good, _ = m.reconstruct_basis("SYN", raw_df, [_split("SYN", ex, 1, 2)], [])
    rec = m.reconcile_close("SYN", good["Close"], ref, rel_tol=0.02)
    assert rec.status == "within_tol" and rec.explained

    missing, _ = m.reconstruct_basis("SYN", raw_df, [], [])
    rec = m.reconcile_close("SYN", missing["Close"], ref, rel_tol=0.02)
    assert rec.status == "out_of_tol" and not rec.explained
    assert rec.explanation.startswith("CLOSE axis:")


def test_no_reference_close_is_unexplained_never_passed():
    idx = pd.bdate_range("2024-01-02", periods=4)
    rec = m.reconcile_close("X", pd.Series([1.0] * 4, index=idx), None)
    assert rec.status == "no_overlap" and rec.explained is False


def test_a_dividend_residual_no_longer_blocks_but_is_loud_and_recorded(monkeypatch, caplog, tmp_path):
    """The 2026-10-26 case: Close is right, the total-return axis is off. The
    build passes on close, writes, and its receipt says the TR axis is untrusted."""
    idx, raw = _flat_name()
    old_close = pd.Series([80.0] * 3 + [100.0] * 5, index=idx)  # an unexplained TR gap
    client = _FakePolygon(splits={}, dividends={"X": [{"ex_dividend_date": "2024-01-04",
                                                        "cash_amount": 1.0,
                                                        "dividend_type": "CD"}]})
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client,
                           apply=True)
    with caplog.at_level(logging.ERROR, logger=m.log.name):
        result = m.migrate_universe_crsp_basis(spill_dir=str(tmp_path), **kw)

    assert result["status"] == "ok" and result["gate_axis"] == "close"
    assert result["unexplained_count"] == 0
    assert result["tr_unexplained_count"] == 1
    assert result["total_return_axis_trusted"] is False
    assert result["axes"]["total_return"]["unexplained"][0]["ticker"] == "X"
    assert scratch.write.call_count == 1
    receipt = _receipt(s3)
    assert receipt["status"] == m.RECEIPT_VERIFIED
    assert receipt["gate_axis"] == "close"
    assert receipt["total_return_axis_trusted"] is False
    assert receipt["tr_unexplained_count"] == 1 and receipt["unexplained_count"] == 0
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("RECONCILE[total_return] X" in e for e in errors)
    assert any("TOTAL-RETURN AXIS UNTRUSTED" in e for e in errors)


def test_a_close_residual_fails_loud_and_writes_nothing(monkeypatch, tmp_path):
    idx, raw = _flat_name()
    raw.attrs[m.REFERENCE_CLOSE_ATTR] = pd.Series([50.0] * 3 + [100.0] * 5, index=idx)
    old_close = pd.Series([100.0] * 8, index=idx)  # TR axis clean
    client = _FakePolygon(splits={}, dividends={})
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client,
                           apply=True)
    with pytest.raises(RuntimeError, match="FAILED LOUD.*close axis"):
        m.migrate_universe_crsp_basis(spill_dir=str(tmp_path), **kw)
    scratch.write.assert_not_called()
    audit = _audit(s3)
    assert audit["status"] == "fail" and audit["close_unexplained_count"] == 1
    assert audit["total_return_axis_trusted"] is True
    assert _receipt(s3) is None


def test_a_name_fetched_without_its_reference_fails_the_close_gate(monkeypatch):
    idx, raw = _flat_name()
    del raw.attrs[m.REFERENCE_CLOSE_ATTR]
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=pd.Series([100.0] * 8, index=idx),
                           client=_FakePolygon(splits={}, dividends={}))
    with pytest.raises(RuntimeError, match="close axis"):
        m.migrate_universe_crsp_basis(**kw)


def test_the_total_return_gate_is_still_available(monkeypatch):
    idx, raw = _flat_name()
    old_close = pd.Series([80.0] * 3 + [100.0] * 5, index=idx)
    client = _FakePolygon(splits={}, dividends={})
    s3, scratch, kw = _run(monkeypatch, raw=raw, old_close=old_close, client=client,
                           gate_axis="total_return")
    with pytest.raises(RuntimeError, match="total_return axis"):
        m.migrate_universe_crsp_basis(**kw)


def test_an_unknown_gate_axis_is_refused(monkeypatch):
    idx, raw = _flat_name()
    _s3, _scratch, kw = _run(monkeypatch, raw=raw, old_close=pd.Series([100.0] * 8, index=idx),
                             client=_FakePolygon(splits={}, dividends={}))
    with pytest.raises(ValueError, match="gate_axis"):
        m.migrate_universe_crsp_basis(gate_axis="dividends", **kw)


def test_cli_defaults_to_close_and_passes_it_through(monkeypatch):
    seen = {}
    monkeypatch.setattr(m, "migrate_universe_crsp_basis", lambda **kw: seen.update(kw) or {})
    monkeypatch.setattr(sys, "argv", ["prog"])
    m.main()
    assert seen["gate_axis"] == "close"
    monkeypatch.setattr(sys, "argv", ["prog", "--gate-axis", "total_return"])
    m.main()
    assert seen["gate_axis"] == "total_return"
