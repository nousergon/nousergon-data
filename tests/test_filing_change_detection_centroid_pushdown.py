"""Guard: filing change detection aggregates embeddings SERVER-SIDE.

Regression (2026-07-16 Neon quota lockout, config-I2780/I2781): the original
``_load_filing_embeddings`` SELECTed the raw ``rag.chunks.embedding`` column
for every 10-K/10-Q chunk in the corpus on every invocation (~150-250MB of
Neon data-transfer per run), then reduced it client-side to one centroid per
filing. Amplified by the per-PR canary replay re-running it on every push,
that single query shape exhausted the Neon project's 5GB/month data-transfer
quota and hard-locked every RAG consumer out of the DB.

These tests lock the fix:

1. The SQL pushes centroid aggregation down to Postgres (``AVG(c.embedding)``
   + GROUPING SETS) and never selects raw embedding rows in bulk.
2. The client-side reshaping/pairing/flag logic is behaviorally identical to
   the legacy client-side reduction (same output record schema and semantics).
3. ``--sample-tickers`` (the canary/CI knob) parameterizes the query so probe
   runs cannot replay full production load.
"""

from __future__ import annotations

import inspect
from contextlib import contextmanager
from datetime import date

import numpy as np
import pytest

import rag.pipelines.filing_change_detection as fcd


# ── Fake DB plumbing ─────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed_sql: str | None = None
        self.executed_params: list | None = None

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = list(params) if params is not None else None

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


@pytest.fixture
def fake_db(monkeypatch):
    """Patch the lib get_connection chokepoint; returns the cursor for
    asserting on the executed SQL/params after the call."""
    state = {"cursor": _FakeCursor([])}

    @contextmanager
    def _fake_get_connection():
        yield _FakeConn(state["cursor"])

    import nousergon_lib.rag.db as libdb

    monkeypatch.setattr(libdb, "get_connection", _fake_get_connection)
    return state


def _row(ticker, doc_type, filed, section, is_overall, centroid, n_chunks):
    """Mirror _CENTROID_SQL's SELECT column order."""
    return (ticker, doc_type, filed, section, is_overall, centroid, n_chunks)


# ── 1. Query-shape guards ────────────────────────────────────────────────────


def test_sql_aggregates_server_side():
    assert "AVG(c.embedding)" in fcd._CENTROID_SQL
    assert "GROUPING SETS" in fcd._CENTROID_SQL
    assert "DENSE_RANK()" in fcd._CENTROID_SQL


def test_no_bulk_raw_embedding_select_anywhere_in_module():
    # The exact legacy egress-whale projection must never reappear. Strip the
    # module docstring/comments risk by checking the one aggregated read is
    # the ONLY place `c.embedding` is selected.
    src = inspect.getsource(fcd)
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith(("#", '"', "'")):
            continue
        if "c.embedding" in stripped:
            assert (
                "AVG(c.embedding)" in stripped
                or "c.embedding IS NOT NULL" in stripped
            ), f"raw embedding column selected outside the aggregate: {stripped!r}"


def test_default_params_fetch_latest_two(fake_db):
    fcd._load_filing_centroids()
    assert fake_db["cursor"].executed_params == [2]
    # No ticker-sampling clause on production runs.
    assert "LIMIT %s" not in fake_db["cursor"].executed_sql


def test_sample_tickers_parameterizes_query(fake_db):
    fcd._load_filing_centroids(sample_tickers=3)
    assert fake_db["cursor"].executed_params == [3, 2]
    assert "LIMIT %s" in fake_db["cursor"].executed_sql


def test_sample_tickers_rejects_nonpositive(fake_db):
    with pytest.raises(ValueError):
        fcd._load_filing_centroids(sample_tickers=0)


def test_min_filings_raises_fetch_ceiling(fake_db):
    fcd._load_filing_centroids(min_filings=3)
    assert fake_db["cursor"].executed_params == [3]


# ── 2. Reshaping + pairing parity with the legacy client-side reduction ─────


def _two_filing_rows(overall_prev, overall_curr, rf_prev, rf_curr):
    """AAPL 10-K with two filings; MD&A identical both sides, Risk Factors
    parameterized; plus a single-filing MSFT that must be skipped."""
    mdna = [1.0, 0.0, 0.0]
    return [
        # AAPL prev (2024-10-01)
        _row("AAPL", "10-K", date(2024, 10, 1), None, 1, overall_prev, 40),
        _row("AAPL", "10-K", date(2024, 10, 1), "MD&A", 0, mdna, 25),
        _row("AAPL", "10-K", date(2024, 10, 1), "Risk Factors", 0, rf_prev, 15),
        # AAPL curr (2025-10-01)
        _row("AAPL", "10-K", date(2025, 10, 1), None, 1, overall_curr, 42),
        _row("AAPL", "10-K", date(2025, 10, 1), "MD&A", 0, mdna, 26),
        _row("AAPL", "10-K", date(2025, 10, 1), "Risk Factors", 0, rf_curr, 16),
        # AAPL curr also has a section prev lacks — must NOT appear in sims
        _row("AAPL", "10-K", date(2025, 10, 1), "Business", 0, [0.0, 1.0, 0.0], 5),
        # MSFT has only one 10-K — below min_filings, skipped entirely
        _row("MSFT", "10-K", date(2025, 7, 1), None, 1, [1.0, 1.0, 0.0], 30),
        _row("MSFT", "10-K", date(2025, 7, 1), "MD&A", 0, [1.0, 1.0, 0.0], 30),
    ]


def test_identical_filings_flag_lazy(fake_db):
    v = [0.6, 0.8, 0.0]
    fake_db["cursor"] = _FakeCursor(_two_filing_rows(v, v, v, v))
    results = fcd.compute_filing_changes()

    assert len(results) == 1  # MSFT (single filing) skipped
    rec = results[0]
    assert rec["ticker"] == "AAPL"
    assert rec["doc_type"] == "10-K"
    assert rec["prev_date"] == "2024-10-01"
    assert rec["curr_date"] == "2025-10-01"
    assert rec["overall_similarity"] == pytest.approx(1.0)
    assert rec["change_score"] == pytest.approx(0.0)
    assert rec["lazy_flag"] is True
    assert rec["n_prev_chunks"] == 40
    assert rec["n_curr_chunks"] == 42
    # Section present on one side only is excluded, matching legacy behavior.
    assert set(rec["section_similarities"]) == {"MD&A", "Risk Factors"}
    assert rec["section_similarities"]["MD&A"] == pytest.approx(1.0)


def test_risk_factor_change_flagged(fake_db):
    overall = [0.6, 0.8, 0.0]
    rf_prev, rf_curr = [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]  # orthogonal → sim 0
    fake_db["cursor"] = _FakeCursor(
        _two_filing_rows(overall, overall, rf_prev, rf_curr)
    )
    results = fcd.compute_filing_changes()
    rec = results[0]
    assert rec["section_similarities"]["Risk Factors"] == pytest.approx(0.0)
    assert rec["risk_factor_change_flag"] is True


def test_changed_filing_scores_change(fake_db):
    prev, curr = [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]
    fake_db["cursor"] = _FakeCursor(_two_filing_rows(prev, curr, prev, prev))
    results = fcd.compute_filing_changes()
    rec = results[0]
    assert rec["overall_similarity"] == pytest.approx(0.0)
    assert rec["change_score"] == pytest.approx(1.0)
    assert "lazy_flag" not in rec


def test_centroids_coerced_to_float32(fake_db):
    v = [0.5, 0.5, 0.5]
    fake_db["cursor"] = _FakeCursor(_two_filing_rows(v, v, v, v))
    by_ticker = fcd._load_filing_centroids()
    filing = by_ticker["AAPL"][0]
    assert isinstance(filing["centroid"], np.ndarray)
    assert filing["centroid"].dtype == np.float32
    assert filing["sections"]["MD&A"].dtype == np.float32


def test_unlabeled_section_group_rows_are_not_sections(fake_db):
    # A per-section grouping-set row whose label is NULL (chunks with no
    # section_label) must not create a phantom section.
    v = [1.0, 0.0, 0.0]
    rows = [
        _row("AAPL", "10-K", date(2024, 10, 1), None, 1, v, 10),
        _row("AAPL", "10-K", date(2024, 10, 1), None, 0, v, 4),  # NULL-label group
        _row("AAPL", "10-K", date(2025, 10, 1), None, 1, v, 12),
        _row("AAPL", "10-K", date(2025, 10, 1), None, 0, v, 5),
    ]
    fake_db["cursor"] = _FakeCursor(rows)
    results = fcd.compute_filing_changes()
    assert len(results) == 1
    assert results[0]["section_similarities"] == {}
