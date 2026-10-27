"""alpha-engine-config-I12436: the residual classes of the 2026-10-26 CRSP-basis build.

The build failed with 46 names out of reconcile tolerance. This file covers the
three classes fixed or classified in the builder:

* **Class 1, prior tickers (24 names).** polygon's dividend endpoint answers for
  the symbol asked, so a name that changed ticker lost every dividend paid under
  its old one. The build now asks polygon for the name's prior tickers (and reads
  the curated renames file) when the dividend history shows a rename's gap.
* **Class 3, class-share symbols (2 names).** ``BF-B`` was sent to polygon as is;
  polygon keys it ``BF.B``.
* **Classes 2, 4, 5 (9 names).** Spin-offs, a stock dividend and special
  distributions are DECLARED with a bounded deviation, report only.

Every fixture is synthetic: made-up tickers, flat prices, round dividends.
"""

from __future__ import annotations

import pandas as pd
import pytest

from builders import migrate_universe_crsp_basis as m
from polygon_client import PolygonRateLimitError

from tests.test_migrate_universe_crsp_basis import _FakePolygon, _patch_orchestration

RENAME_DATE = "2024-03-01"


def _div(ex: str, cash: float = 1.0) -> dict:
    return {"ex_dividend_date": ex, "cash_amount": cash, "dividend_type": "CD"}


class _CountingPolygon(_FakePolygon):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.calls: list[tuple[str, str]] = []

    def get_splits(self, ticker):
        self.calls.append(("splits", ticker))
        return super().get_splits(ticker)

    def get_dividends(self, ticker):
        self.calls.append(("dividends", ticker))
        return super().get_dividends(ticker)

    def get_ticker_events(self, ticker):
        self.calls.append(("ticker_events", ticker))
        return super().get_ticker_events(ticker)


def _renamed_client(**extra) -> _CountingPolygon:
    """NEWCO traded as OLDCO until RENAME_DATE. polygon files the dividends
    paid before the change under OLDCO, and OLDCO was later re-used by an
    unrelated listing whose dividend must never be folded in."""
    return _CountingPolygon(
        splits={},
        dividends={
            "NEWCO": [_div("2024-06-03")],
            "OLDCO": [_div("2023-06-01"), _div("2023-12-01"), _div("2024-09-03")],
        },
        ticker_events={
            "NEWCO": [{"date": RENAME_DATE, "old_ticker": "OLDCO", "new_ticker": "NEWCO"}],
        },
        **extra,
    )


def _ex_dates(actions) -> list[str]:
    return [pd.Timestamp(a.ex_date).strftime("%Y-%m-%d") for a in actions]


# ── class 1: prior-ticker dividends ──────────────────────────────────────────


def test_renamed_name_gets_the_dividends_paid_under_its_prior_ticker():
    """The regression: before this fix NEWCO's history held one dividend."""
    client = _renamed_client()
    _, divs = m.fetch_corporate_actions("NEWCO", client=client, sleep=lambda s: None)
    assert _ex_dates(divs) == ["2024-06-03"]

    merged, report = m.fetch_prior_ticker_dividends(
        "NEWCO", divs, client=client, window_start=pd.Timestamp("2023-01-03"),
        retickers=[], sleep=lambda s: None,
    )
    # The two OLDCO dividends before the rename are folded in; the one after it
    # belongs to whoever took the symbol next and is left out.
    assert _ex_dates(merged) == ["2023-06-01", "2023-12-01", "2024-06-03"]
    assert {a.ticker for a in merged} == {"NEWCO"}, "keyed to the store ticker"
    assert report == [{
        "symbol": "OLDCO", "start": None, "end": RENAME_DATE,
        "source": "polygon_ticker_events", "dividends_added": 2,
    }]


def test_prior_span_start_excludes_an_earlier_holder_of_the_symbol():
    client = _CountingPolygon(
        splits={},
        dividends={"NEWCO": [], "MIDCO": [_div("2021-03-01"), _div("2023-03-01")]},
        ticker_events={"NEWCO": [
            {"date": "2022-01-03", "old_ticker": "FIRSTCO", "new_ticker": "MIDCO"},
            {"date": "2024-01-02", "old_ticker": "MIDCO", "new_ticker": "NEWCO"},
        ]},
    )
    merged, report = m.fetch_prior_ticker_dividends(
        "NEWCO", [], client=client, window_start=pd.Timestamp("2020-06-30"),
        retickers=[], sleep=lambda s: None,
    )
    # The entity took MIDCO on 2022-01-03; MIDCO's 2021 dividend was someone else's.
    assert _ex_dates(merged) == ["2023-03-01"]
    assert [(r["symbol"], r["start"], r["end"]) for r in report] == [
        ("FIRSTCO", None, "2022-01-03"), ("MIDCO", "2022-01-03", "2024-01-02"),
    ]


def test_curated_rename_is_used_when_polygon_reports_no_event():
    """polygon returns no ticker_change for some renames (BK -> BNY)."""
    client = _CountingPolygon(
        splits={}, dividends={"NEWCO": [], "OLDCO": [_div("2023-06-01")]},
        ticker_events={},
    )
    retickers = [{"old": "OLDCO", "new": "NEWCO", "effective": RENAME_DATE}]
    merged, report = m.fetch_prior_ticker_dividends(
        "NEWCO", [], client=client, window_start=pd.Timestamp("2023-01-03"),
        retickers=retickers, sleep=lambda s: None,
    )
    assert _ex_dates(merged) == ["2023-06-01"]
    assert report[0]["source"] == "known_retickers"


def test_the_repo_renames_file_maps_vmrk_back_to_eqr():
    priors = m._known_prior_tickers("VMRK", m.load_known_retickers())
    assert [(p.symbol, p.end) for p in priors] == [("EQR", "2026-08-18")]


def test_a_dividend_already_filed_under_the_new_symbol_is_not_counted_twice():
    client = _CountingPolygon(
        splits={},
        dividends={"NEWCO": [_div("2023-06-01"), _div("2024-06-03")],
                   "OLDCO": [_div("2023-06-01")]},
        ticker_events={"NEWCO": [{"date": RENAME_DATE, "old_ticker": "OLDCO",
                                  "new_ticker": "NEWCO"}]},
    )
    _, divs = m.fetch_corporate_actions("NEWCO", client=client, sleep=lambda s: None)
    merged, report = m.fetch_prior_ticker_dividends(
        "NEWCO", divs, client=client, window_start=pd.Timestamp("2021-01-04"),
        retickers=[], sleep=lambda s: None,
    )
    assert _ex_dates(merged) == ["2023-06-01", "2024-06-03"]
    assert report[0]["dividends_added"] == 0


def test_no_gap_means_no_polygon_call():
    """The budget guard: a name that paid under one symbol all window long is
    never asked about prior tickers."""
    client = _CountingPolygon(splits={}, dividends={"STEADY": [_div("2020-09-01")]})
    _, divs = m.fetch_corporate_actions("STEADY", client=client, sleep=lambda s: None)
    client.calls.clear()
    merged, report = m.fetch_prior_ticker_dividends(
        "STEADY", divs, client=client, window_start=pd.Timestamp("2020-06-30"),
        retickers=[], sleep=lambda s: None,
    )
    assert client.calls == [] and report == [] and len(merged) == 1


@pytest.mark.parametrize("ex_dates, window, gap", [
    ([], "2020-06-30", True),
    (["2020-09-01"], "2020-06-30", False),
    (["2021-08-30"], "2020-06-30", True),     # 426 days in: a rename's gap
    (["2021-06-30"], "2020-06-30", False),    # 365 days in: an annual payer
    (["2024-06-03"], None, False),
])
def test_dividend_history_gap(ex_dates, window, gap):
    import corporate_actions as ca

    acts = ca.dividends_from_events([{**_div(d), "ticker": "X"} for d in ex_dates])
    w = pd.Timestamp(window) if window else None
    assert m.dividend_history_gap(acts, window_start=w) is gap


def test_a_ticker_events_lookup_that_cannot_be_answered_fails_the_name():
    class _Limited(_FakePolygon):
        def get_ticker_events(self, ticker):
            raise PolygonRateLimitError("Rate limited (429)")

    with pytest.raises(m.CorporateActionFetchError, match="ticker-events"):
        m.fetch_prior_ticker_dividends(
            "NEWCO", [], client=_Limited(splits={}, dividends={}),
            window_start=pd.Timestamp("2023-01-03"), retickers=[],
            sleep=lambda s: None,
        )


def test_a_prior_ticker_dividend_fetch_that_fails_fails_the_name():
    class _Broken(_FakePolygon):
        def get_dividends(self, ticker):
            if ticker == "OLDCO":
                raise ValueError("unexpected shape")
            return super().get_dividends(ticker)

    client = _Broken(splits={}, dividends={}, ticker_events={
        "NEWCO": [{"date": RENAME_DATE, "old_ticker": "OLDCO", "new_ticker": "NEWCO"}],
    })
    with pytest.raises(m.CorporateActionFetchError, match="prior ticker OLDCO"):
        m.fetch_prior_ticker_dividends(
            "NEWCO", [], client=client, window_start=pd.Timestamp("2023-01-03"),
            retickers=[], sleep=lambda s: None,
        )


def test_renames_file_that_cannot_be_read_raises(tmp_path):
    bad = tmp_path / "r.json"
    bad.write_text('{"schema_version": 1}')
    with pytest.raises(KeyError):
        m.load_known_retickers(str(bad))


def test_build_reconciles_a_renamed_name_only_with_its_prior_ticker_dividends(monkeypatch):
    """End to end on a synthetic renamed name: the old (total-return) close
    carries a 5% dividend paid before the rename. Without the prior-ticker
    dividend the build fails loud; with it the name reconciles."""
    idx = pd.bdate_range("2023-11-01", periods=12)
    raw = pd.DataFrame({"Close": [100.0] * 12, "Volume": [1_000_000] * 12}, index=idx)
    ex = idx[5].strftime("%Y-%m-%d")
    rename = idx[8].strftime("%Y-%m-%d")
    old_close = pd.Series([95.0] * 5 + [100.0] * 7, index=idx)
    _patch_orchestration(monkeypatch, old_closes={"NEWCO": old_close})

    def run(client):
        return m.migrate_universe_crsp_basis(
            apply=False, raw_fetch=lambda t: raw, client=client, rel_tol=0.02, workers=1,
        )

    without = _FakePolygon(splits={}, dividends={"OLDCO": [_div(ex, 5.0)]})
    with pytest.raises(RuntimeError, match="FAILED LOUD"):
        run(without)

    with_events = _FakePolygon(
        splits={}, dividends={"OLDCO": [_div(ex, 5.0)]},
        ticker_events={"NEWCO": [{"date": rename, "old_ticker": "OLDCO",
                                  "new_ticker": "NEWCO"}]},
    )
    result = run(with_events)
    assert result["status"] == "ok" and result["within_tol_count"] == 1
    assert result["prior_ticker_lookups_count"] == 1
    assert result["prior_ticker_dividends_count"] == 1
    assert result["prior_ticker_dividends"][0]["prior"][0]["symbol"] == "OLDCO"


# ── class 3: class-share symbols ─────────────────────────────────────────────


def test_class_share_ticker_is_sent_to_polygon_in_its_dot_form():
    client = _CountingPolygon(
        splits={"BF.B": []}, dividends={"BF.B": [_div("2024-06-03")]},
    )
    splits, divs = m.fetch_corporate_actions("BF-B", client=client, sleep=lambda s: None)
    assert ("dividends", "BF.B") in client.calls and ("splits", "BF.B") in client.calls
    assert not any(t == "BF-B" for _, t in client.calls)
    assert [a.ticker for a in divs] == ["BF-B"], "keyed to the store ticker"

    client.calls.clear()
    m.fetch_prior_ticker_dividends(
        "BF-B", [], client=client, window_start=pd.Timestamp("2023-01-03"),
        retickers=[], sleep=lambda s: None,
    )
    assert client.calls == [("ticker_events", "BF.B")]


# ── classes 2, 4, 5: declared, report only ───────────────────────────────────


def _rec(ticker: str, dev: float, status: str = "out_of_tol") -> m.ReconcileRecord:
    return m.ReconcileRecord(
        ticker=ticker, status=status, n_common_dates=10, max_rel_dev=dev,
        max_dev_date="2025-01-02", explained=False, explanation="synthetic",
    )


def test_declared_table_covers_the_nine_classified_names():
    table = m._declared_by_ticker()
    by_class: dict[str, set[str]] = {}
    for d in table.values():
        by_class.setdefault(d.divergence_class, set()).add(d.ticker)
    assert by_class == {
        "spin_off": {"CTVA", "HON", "IBM", "LEN", "BDX"},
        "stock_dividend": {"CBSH"},
        "special_distribution": {"HR", "PSA", "RRX"},
    }


def test_classification_reports_bound_expiry_and_unclassified():
    declared = (
        m.DeclaredDivergence("AAA", "spin_off", "made-up spin", "2024-05", 0.10,
                             "synthetic", "2099-01-01"),
        m.DeclaredDivergence("BBB", "special_distribution", "made-up special",
                             "2024", 0.05, "synthetic", "2020-01-01"),
    )
    out = m.classify_divergences(
        [_rec("AAA", 0.08), _rec("BBB", 0.09), _rec("CCC", 0.30), _rec("DDD", 0.01, "within_tol")],
        today="2026-10-27", declared=declared,
    )
    assert [c["ticker"] for c in out["classified"]] == ["AAA", "BBB"]
    assert out["unclassified"] == ["CCC"]
    assert out["exceeds_bound"] == ["BBB"]
    assert out["expired"] == ["BBB"]
    assert all(c["explained"] is False for c in out["classified"])


@pytest.mark.parametrize("kwargs", [
    {"divergence_class": "rename"},
    {"max_rel_dev_bound": 1.5},
    {"event_when": "24-05"},
    {"evidence": " "},
])
def test_a_malformed_declaration_fails_at_construction(kwargs):
    base = dict(ticker="AAA", divergence_class="spin_off", event="made-up",
                event_when="2024-05", max_rel_dev_bound=0.1, evidence="synthetic",
                expires="2099-01-01")
    with pytest.raises(ValueError):
        m.DeclaredDivergence(**{**base, **kwargs})


def test_a_declared_name_still_fails_the_gate(monkeypatch):
    """Classification never passes the gate: only --known-divergence does."""
    idx = pd.bdate_range("2024-01-02", periods=6)
    raw = pd.DataFrame({"Close": [100.0] * 6, "Volume": [1_000_000] * 6}, index=idx)
    old = pd.Series([80.0, 80.0, 100.0, 100.0, 100.0, 100.0], index=idx)
    _patch_orchestration(monkeypatch, old_closes={"CBSH": old})
    client = _FakePolygon(splits={}, dividends={"CBSH": [_div("2024-01-03")]})
    with pytest.raises(RuntimeError, match="FAILED LOUD"):
        m.migrate_universe_crsp_basis(
            apply=False, raw_fetch=lambda t: raw, client=client, rel_tol=0.02, workers=1,
        )
    # The audit (written before the raise) carries the class beside the failure.
    import json

    audit = json.loads(m.boto3.client.return_value.put_object.call_args.kwargs["Body"])
    assert audit["unexplained_count"] == 1
    assert audit["classified_out_of_tol_count"] == 1
    assert audit["divergence_classes"]["classified"][0]["class"] == "stock_dividend"
