"""perf_tracker: window x comparison returns (alpha-engine-config-I12798, C11).

Every fixture here is SYNTHETIC: round-number NAVs and closes built in this
file. This repository is public; no account or vendor value appears in it.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime, timedelta

import pytest

from data.derived import perf_tracker as pt

NOW = datetime(2026, 10, 29, 6, 0, tzinfo=UTC)


def weekdays(start: date, n: int) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def nav_csv(rows: list[tuple[date, float, float | None, float, str]], *, spy_pct: bool = True) -> str:
    """The eod_pnl export's columns this producer reads. ``spy_return_pct`` is
    derived from the synthetic closes, as the EOD reconcile persists it."""
    buf = io.StringIO()
    buf.write("date,portfolio_nav,spy_close,spy_dividend_per_share,spy_return_pct,nav_basis\n")
    prev = None
    for d, nav, spy, div, basis in sorted(rows):
        pct = "" if not spy_pct or prev is None or spy is None else (spy + div) / prev * 100 - 100
        buf.write(f"{d.isoformat()},{nav},{'' if spy is None else spy},{div},{pct},{basis}\n")
        prev = spy
    return buf.getvalue()


def ledger(days: list[date], *, port_step: float = 0.001, spy_step: float = 0.0005):
    """Synthetic ledger: NAV and SPY each grow by a fixed daily step."""
    rows, nav, spy = [], 1000.0, 100.0
    for i, d in enumerate(days):
        if i:
            nav *= 1 + port_step
            spy *= 1 + spy_step
        rows.append((d, round(nav, 6), round(spy, 6), 0.0, "settled_close"))
    return rows


def history(days: list[date], start: float = 50.0, step: float = 0.0002) -> dict[date, float]:
    out, px = {}, start
    for i, d in enumerate(days):
        if i:
            px *= 1 + step
        out[d] = px
    return out


def build(rows, *, flows=(), summary=None, spy_hist=None, qqq_hist=None, as_of=None, spy_pct=True):
    days = [r[0] for r in rows]
    return pt.build(
        pt.parse_nav_csv(nav_csv(rows, spy_pct=spy_pct)),
        list(flows),
        summary or {"entries_read": 0, "transfer_entries": 0},
        spy_hist if spy_hist is not None else history(days, 100.0, 0.0005),
        qqq_hist if qqq_hist is not None else history(days),
        now=NOW,
        as_of=as_of,
    )


def window(doc, wid):
    return next(w for w in doc["windows"] if w["id"] == wid)


SHORT = weekdays(date(2026, 3, 9), 40)       # ~8 weeks
LONG = weekdays(date(2025, 1, 2), 300)       # > 365 calendar days


# ── shape ────────────────────────────────────────────────────────────────────


def test_every_window_carries_every_comparison_with_a_declared_status():
    doc = build(ledger(SHORT))
    assert doc["schema_version"] == pt.SCHEMA_VERSION
    assert [w["id"] for w in doc["windows"]] == [w for w, _ in pt.WINDOWS]
    for w in doc["windows"]:
        assert set(w["cells"]) == set(pt.COMPARISONS)
        for cell in w["cells"].values():
            assert cell["status"] in pt.STATUSES
    assert sum(doc["status_counts"].values()) == len(pt.WINDOWS) * len(pt.COMPARISONS)
    assert doc["as_of"] == SHORT[-1].isoformat()
    assert doc["inception_date"] == SHORT[0].isoformat()
    assert doc["days_of_history"] == (SHORT[-1] - SHORT[0]).days
    assert doc["trading_days_of_history"] == len(SHORT) - 1


# ── YoY / LTM: the insufficient-history path ────────────────────────────────


def test_yoy_and_ltm_are_insufficient_under_365_days():
    doc = build(ledger(SHORT))
    for wid in ("yoy", "ltm"):
        w = window(doc, wid)
        assert w["base_date"] is None
        assert all(c["status"] == pt.STATUS_INSUFFICIENT for c in w["cells"].values())
        assert "of 365 days" in w["insufficient_reason"]
    for wid in ("day", "wtd", "mtd", "ytd", "inception"):
        assert all(
            c["status"] != pt.STATUS_INSUFFICIENT for c in window(doc, wid)["cells"].values()
        ), wid


def test_yoy_and_ltm_compute_once_a_year_exists():
    doc = build(ledger(LONG))
    assert doc["days_of_history"] >= 365
    ltm, yoy = window(doc, "ltm"), window(doc, "yoy")
    as_of = LONG[-1]
    assert ltm["end_date"] == as_of.isoformat()
    assert date.fromisoformat(ltm["base_date"]) <= as_of.replace(year=as_of.year - 1)
    # Prior-year same-to-date: from inception (2025-01-02, no row before Jan 1 2025).
    assert yoy["partial_from_inception"] is True
    assert yoy["base_date"] == LONG[0].isoformat()
    assert date.fromisoformat(yoy["end_date"]) <= as_of.replace(year=as_of.year - 1)
    expected = (1.001 ** ltm["trading_days"] - 1) * 100
    assert ltm["cells"]["absolute"]["return_pct"] == pytest.approx(expected, abs=1e-4)


# ── TWR: a deposit mid-window is not performance ────────────────────────────


def _with_deposit(days, deposit_index, amount):
    """Same 0.1%/day path, plus a deposit landing after the close of one day."""
    out, nav = [], 1000.0
    for i, (d, _, spy, div, basis) in enumerate(ledger(days)):
        if i:
            nav *= 1.001
        if i == deposit_index:
            nav += amount
        out.append((d, nav, spy, div, basis))
    return out


def test_a_journaled_deposit_mid_window_does_not_count_as_return(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    deposit_day, amount = SHORT[20], 500.0
    entry = {
        "source": pt.TRANSFER_SOURCE,
        "effective_date": deposit_day.isoformat(),
        "postings": [
            {"account": "assets:cash:USD", "amount": f"{amount:.2f}"},
            {"account": "equity:contributions", "amount": f"{-amount:.2f}"},
        ],
    }
    flows, summary = pt.flows_from_journal([entry])
    assert [(f.day, f.amount) for f in flows] == [(deposit_day, amount)]

    with_flow = build(_with_deposit(SHORT, 20, amount), flows=flows, summary=summary)
    clean = build(ledger(SHORT))
    got = window(with_flow, "inception")["cells"]["absolute"]
    want = window(clean, "inception")["cells"]["absolute"]
    assert got["return_pct"] == pytest.approx(want["return_pct"], abs=1e-6)
    assert got["status"] == pt.STATUS_OK
    assert got["reasons"] == []
    # Without the flow the same NAV path would read the deposit as performance.
    naive = build(_with_deposit(SHORT, 20, amount))
    assert window(naive, "inception")["cells"]["absolute"]["return_pct"] > got["return_pct"] + 10


def test_unjournaled_flows_mark_every_return_estimated():
    assert pt.FLOWS_JOURNALED_FROM is None  # the ledger has no transfer builder yet
    doc = build(ledger(SHORT))
    cell = window(doc, "mtd")["cells"]["absolute"]
    assert cell["status"] == pt.STATUS_ESTIMATED
    assert cell["reasons"] == [pt.REASON_FLOWS]
    assert cell["return_pct"] is not None


# ── benchmarks ──────────────────────────────────────────────────────────────


def test_spy_is_total_return_from_the_settled_leg(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    rows = [(d, 1000.0, 100.0, 0.0, "settled_close") for d in SHORT[:5]]
    rows[3] = (rows[3][0], 1000.0, 100.0, 2.0, "settled_close")  # $2 ex-dividend, flat price
    doc = build(rows)
    vs_spy = window(doc, "inception")["cells"]["vs_spy"]
    assert vs_spy["benchmark_pct"] == pytest.approx(2.0, abs=1e-6)
    assert vs_spy["portfolio_pct"] == pytest.approx(0.0, abs=1e-6)
    assert vs_spy["excess_pct"] == pytest.approx(-2.0, abs=1e-6)
    assert vs_spy["status"] == pt.STATUS_OK


def test_a_row_without_the_persisted_spy_return_is_rebuilt_and_marked(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    rows = [(d, 1000.0, 100.0, 0.0, "settled_close") for d in SHORT[:5]]
    rows[3] = (rows[3][0], 1000.0, 100.0, 2.0, "settled_close")
    cell = window(build(rows, spy_pct=False), "inception")["cells"]["vs_spy"]
    assert cell["benchmark_pct"] == pytest.approx(2.0, abs=1e-6)
    assert cell["status"] == pt.STATUS_ESTIMATED
    assert cell["reasons"] == [f"spy_return_pct_missing:{d.isoformat()}" for d in SHORT[1:5]]


def test_a_missing_settled_spy_close_falls_back_and_is_marked(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    rows = ledger(SHORT[:10])
    d, nav, _, div, basis = rows[6]
    rows[6] = (d, nav, None, div, basis)
    doc = build(rows)
    cell = window(doc, "inception")["cells"]["vs_spy"]
    assert cell["status"] == pt.STATUS_ESTIMATED
    assert cell["reasons"] == [f"spy_settled_close_missing:{SHORT[6].isoformat()}"]
    assert cell["benchmark_pct"] is not None


def test_qqq_is_estimated_until_a_settled_series_exists(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    doc = build(ledger(SHORT))
    cell = window(doc, "inception")["cells"]["vs_qqq"]
    assert cell["status"] == pt.STATUS_ESTIMATED
    assert cell["reasons"] == [pt.REASON_QQQ_BASIS]
    assert cell["benchmark_pct"] == pytest.approx((1.0002 ** (len(SHORT) - 1) - 1) * 100, abs=1e-4)


def test_no_benchmark_input_is_estimated_with_a_null_value(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    doc = build(ledger(SHORT[:10]), qqq_hist={})
    cell = window(doc, "inception")["cells"]["vs_qqq"]
    assert cell["status"] == pt.STATUS_ESTIMATED
    assert cell["benchmark_pct"] is None and cell["excess_pct"] is None


# ── alpha ───────────────────────────────────────────────────────────────────


def test_beta_adjusted_alpha_needs_sixty_observations(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", date(2000, 1, 1))
    days = weekdays(date(2026, 1, 5), 90)
    spy_moves = [0.01 if i % 2 else -0.008 for i in range(len(days))]
    rows, nav, spy = [], 1000.0, 100.0
    for i, d in enumerate(days):
        if i:
            nav *= 1 + 2 * spy_moves[i]
            spy *= 1 + spy_moves[i]
        rows.append((d, nav, spy, 0.0, "settled_close"))
    doc = build(rows)
    alpha = window(doc, "inception")["cells"]["alpha"]
    assert alpha["beta"] == pytest.approx(2.0, abs=1e-6)
    assert alpha["beta_status"] == pt.STATUS_OK
    assert alpha["beta_observations"] == len(days) - 1
    assert alpha["excess_vs_spy_pct"] == window(doc, "inception")["cells"]["vs_spy"]["excess_pct"]
    short = window(doc, "mtd")["cells"]["alpha"]
    assert short["beta_observations"] < pt.MIN_BETA_OBSERVATIONS
    assert short["beta"] is None and short["beta_status"] == pt.STATUS_INSUFFICIENT


# ── window bases ────────────────────────────────────────────────────────────


def test_window_bases_on_a_calendar():
    days = weekdays(date(2026, 3, 9), 60)
    as_of = days[-1]  # a weekday
    doc = build(ledger(days))
    assert window(doc, "day")["base_date"] == days[-2].isoformat()
    monday = as_of - timedelta(days=as_of.weekday())
    assert date.fromisoformat(window(doc, "wtd")["base_date"]) < monday
    assert date.fromisoformat(window(doc, "mtd")["base_date"]) < as_of.replace(day=1)
    ytd = window(doc, "ytd")
    assert ytd["partial_from_inception"] is True and ytd["base_date"] == days[0].isoformat()
    assert window(doc, "inception")["trading_days"] == len(days) - 1


def test_a_basis_change_inside_a_window_is_reported():
    rows = ledger(SHORT[:10])
    rows = [(d, n, s, v, "ib_netliq" if i < 5 else "settled_close") for i, (d, n, s, v, _) in enumerate(rows)]
    doc = build(rows)
    assert window(doc, "inception")["nav_basis_changes"] == [
        {"date": SHORT[5].isoformat(), "from": "ib_netliq", "to": "settled_close"}
    ]
    assert window(doc, "day")["nav_basis_changes"] == []


def test_one_row_is_insufficient_everywhere():
    doc = build(ledger(SHORT[:1]))
    assert doc["status_counts"][pt.STATUS_INSUFFICIENT] == len(pt.WINDOWS) * len(pt.COMPARISONS)


def test_as_of_cuts_the_ledger():
    doc = build(ledger(SHORT), as_of=SHORT[9])
    assert doc["as_of"] == SHORT[9].isoformat()
    assert window(doc, "inception")["trading_days"] == 9


# ── determinism, parsing, publish ───────────────────────────────────────────


def test_same_inputs_give_the_same_bytes_whatever_the_row_order():
    rows = ledger(SHORT)
    spy, qqq = history(SHORT, 100.0, 0.0005), history(SHORT)
    a = pt.encode(build(rows, spy_hist=spy, qqq_hist=qqq))
    b = pt.encode(build(list(reversed(rows)), spy_hist=spy, qqq_hist=qqq))
    assert a == b


def test_parse_skips_unusable_rows_and_refuses_an_empty_ledger():
    text = nav_csv(ledger(SHORT[:3])) + "not-a-date,1,1,0,x,0\n2026-12-01,,1,0,x,0\n"
    assert [r.day for r in pt.parse_nav_csv(text)] == SHORT[:3]
    with pytest.raises(pt.PerfInputError):
        pt.parse_nav_csv("date,portfolio_nav\n2026-01-02,0\n")
    with pytest.raises(pt.PerfInputError):
        pt.parse_nav_csv("date,nav\n")


def test_journal_summary_names_the_anchor_and_ignores_corrections():
    flows, summary = pt.flows_from_journal([
        {"source": "daily_auto_correct", "effective_date": "2026-10-20", "postings": []},
        {"source": "opening_balance", "effective_date": "2026-10-23", "postings": []},
    ])
    assert flows == []
    assert summary["entries_read"] == 2 and summary["transfer_entries"] == 0
    assert summary["ledger_anchor_date"] == "2026-10-23"
    assert summary["journaled_from"] is None


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.puts: list[str] = []

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3's names
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.puts.append(Key)
        self.objects[Key] = Body

    def get_paginator(self, _name):
        objects = self.objects

        class _P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {"Contents": [{"Key": k} for k in sorted(objects) if k.startswith(Prefix)]}

        return _P()


def test_load_publish_writes_only_its_own_prefix_dated_first():
    rows = ledger(SHORT)
    closes = {"closes": [[d.isoformat(), 100.0 + i] for i, d in enumerate(SHORT)]}
    s3 = FakeS3({
        pt.NAV_KEY: nav_csv(rows).encode(),
        pt.CLOSE_HISTORY_KEY.format(symbol="SPY"): json.dumps(closes).encode(),
        pt.CLOSE_HISTORY_KEY.format(symbol="QQQ"): json.dumps(closes).encode(),
        f"{pt.JOURNAL_PREFIX}2026-04-01/je_x.json": json.dumps(
            {"source": "daily_auto_correct", "effective_date": "2026-04-01", "postings": []}
        ).encode(),
    })
    doc = pt.load_and_build(s3, "bucket", now=NOW)
    assert doc["inputs"]["flows"]["entries_read"] == 1
    keys = pt.publish(s3, "bucket", doc)
    assert keys == s3.puts == [f"ops/perf/dated/{SHORT[-1].isoformat()}.json", "ops/perf/latest.json"]
    assert all(k.startswith(pt.OUTPUT_PREFIX) for k in s3.puts)
    assert json.loads(s3.objects[pt.LATEST_KEY]) == json.loads(s3.objects[keys[0]])


# ── S1: calendar gaps and the backfill ──────────────────────────────────────


def test_a_missing_trading_day_and_a_closed_day_row_are_marked(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    rows = ledger(SHORT[:10])
    gone = rows.pop(4)[0]                       # a trading day with no NAV row
    saturday = SHORT[6] - timedelta(days=SHORT[6].weekday() + 2)  # inside the span
    rows.append((saturday, rows[-1][1], rows[-1][2], 0.0, "settled_close"))
    doc = build(sorted(rows), spy_hist=history(SHORT[:10], 100.0, 0.0005))
    reasons = window(doc, "inception")["cells"]["absolute"]["reasons"]
    assert f"{pt.REASON_NAV_ROW_MISSING}:{gone.isoformat()}" in reasons
    assert f"{pt.REASON_NAV_ROW_CLOSED_DAY}:{saturday.isoformat()}" in reasons
    assert window(doc, "inception")["cells"]["absolute"]["status"] == pt.STATUS_ESTIMATED
    # A window that does not span either gap stays clean.
    day = window(doc, "day")["cells"]["absolute"]
    assert day["status"] == pt.STATUS_OK and day["reasons"] == []


def test_no_calendar_inference_outside_the_close_history_span(monkeypatch):
    monkeypatch.setattr(pt, "FLOWS_JOURNALED_FROM", SHORT[0])
    rows = ledger(SHORT[:10])
    doc = build(rows, spy_hist=history(SHORT[:5], 100.0, 0.0005))
    reasons = window(doc, "inception")["cells"]["absolute"]["reasons"]
    assert not [r for r in reasons if r.startswith("nav_row_")]


def test_backfill_writes_one_dated_document_per_row_and_never_latest():
    rows = ledger(SHORT)
    closes = {"closes": [[d.isoformat(), 100.0 + i] for i, d in enumerate(SHORT)]}
    s3 = FakeS3({
        pt.NAV_KEY: nav_csv(rows).encode(),
        pt.CLOSE_HISTORY_KEY.format(symbol="SPY"): json.dumps(closes).encode(),
        pt.CLOSE_HISTORY_KEY.format(symbol="QQQ"): json.dumps(closes).encode(),
    })
    docs = pt.load_and_backfill(s3, "bucket", now=NOW)
    assert [d["as_of"] for d in docs] == [d.isoformat() for d in SHORT[1:]]
    assert all(d["backfill"] == {"recomputed_from_inputs_as_of": SHORT[-1].isoformat()} for d in docs)
    # The last backfilled document is the daily one plus the backfill marker.
    daily = pt.load_and_build(s3, "bucket", now=NOW)
    assert {k: v for k, v in docs[-1].items() if k != "backfill"} == daily
    for doc in docs:
        pt.publish(s3, "bucket", doc, include_latest=False)
    assert pt.LATEST_KEY not in s3.puts
    assert s3.puts == [f"ops/perf/dated/{d.isoformat()}.json" for d in SHORT[1:]]
