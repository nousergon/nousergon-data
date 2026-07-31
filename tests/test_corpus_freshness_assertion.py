"""Corpus freshness assertion (config-I5702).

``rag-corpus-policy.md`` §2.3: a decision pipeline may VERIFY corpus freshness;
it may never FILL the corpus. The two assertions that matter most here are the
negative ones — that this module **never fetches** and **never exits non-zero**
— because either would reintroduce the coupling that let a corpus-filling step
take the weekly pipeline down (`alpha-engine-config-I5695`).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

from rag.pipelines import assert_corpus_freshness as acf

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
SCOPE = ["AAA", "BBB", "CCC", "DDD", "EEE"]


def _s3(marks: dict):
    s3 = MagicMock()
    payloads = {
        "universe_membership/latest.json": {
            "run_date": "2026-07-31",
            "cuts": {"scanner_candidates": {"size": len(SCOPE), "tickers": SCOPE}},
        },
        "metron/holdings_universe.json": {"as_of": "2026-07-31", "tickers": []},
        "rag/watermarks/v1/news.json": {"marks": marks},
    }

    def _get(Bucket, Key):
        if Key not in payloads:
            raise RuntimeError(f"NoSuchKey {Key}")
        return {"Body": BytesIO(json.dumps(payloads[Key]).encode())}

    s3.get_object.side_effect = _get
    return s3


def _marks(**ages_h) -> dict:
    return {
        f"{t}|news_article": (NOW - timedelta(hours=h)).isoformat().replace("+00:00", "Z")
        for t, h in ages_h.items()
    }


# ── the two non-negotiables ──────────────────────────────────────────────


def test_assess_never_issues_a_vendor_fetch():
    # The whole point: verification reads state, it does not fill. If this
    # module ever grows a fetch, the coupling that killed the 2026-07-29 run
    # is back.
    with patch("collectors.news_aggregator_async.AsyncNewsAggregator") as agg, \
         patch("collectors.news_sources.polygon.PolygonNewsAdapter") as poly:
        acf.assess(bucket="b", s3_client=_s3(_marks(**{t: 1 for t in SCOPE})), now=NOW)
    agg.assert_not_called()
    poly.assert_not_called()


def test_main_exits_zero_even_when_maximally_degraded(monkeypatch, capsys):
    # Staleness is a degraded FLAG, never a pipeline failure. Failing the
    # weekly SF over stale news degrades the belief set the whole trading week
    # depends on — strictly worse than one stale answer.
    monkeypatch.setattr(
        acf, "assess",
        lambda **kw: {"degraded": True, "degraded_reasons": ["everything is stale"],
                      "scope": 5, "fresh": 0, "stale": 5, "missing": 0,
                      "coverage": 0.0, "oldest_age_hours": 999.0},
    )
    monkeypatch.setattr("sys.argv", ["assert_corpus_freshness", "--no-write"])
    assert acf.main() == 0


def test_main_exits_zero_when_the_assessment_itself_raises(monkeypatch, capsys):
    # An unresolvable scope is a real upstream failure, but every other stage
    # that needs the scope is already loud about it. This step must not be the
    # one that takes the pipeline down over NEWS.
    def _boom(**kw):
        raise RuntimeError("membership artifact missing")

    monkeypatch.setattr(acf, "assess", _boom)
    monkeypatch.setattr("sys.argv", ["assert_corpus_freshness", "--no-write"])
    assert acf.main() == 0
    assert json.loads(capsys.readouterr().out)["degraded"] is True


# ── verdict semantics ────────────────────────────────────────────────────


def test_warm_corpus_is_not_degraded():
    v = acf.assess(
        bucket="b", s3_client=_s3(_marks(**{t: 6 for t in SCOPE})), now=NOW,
    )
    assert v["degraded"] is False
    assert v["fresh"] == 5 and v["coverage"] == 1.0


def test_saturday_tolerates_friday_close_data():
    # The weekly SF fires one day after the week's last trading session, and
    # the daily job is weekdays-only — so a ~2-day-old corpus on a Saturday is
    # the NORMAL state, not a degradation. Written as a test so the tolerance
    # is explicit rather than an accident of the default.
    v = acf.assess(
        bucket="b", s3_client=_s3(_marks(**{t: 48 for t in SCOPE})), now=NOW,
    )
    assert v["degraded"] is False


def test_stale_beyond_the_window_is_degraded_and_says_why():
    v = acf.assess(
        bucket="b", s3_client=_s3(_marks(**{t: 200 for t in SCOPE})), now=NOW,
    )
    assert v["degraded"] is True
    assert v["stale"] == 5
    assert any("older than" in r for r in v["degraded_reasons"])


def test_never_ingested_tickers_are_reported_separately_from_stale():
    # "Missing" and "stale" are different conditions with different causes —
    # a new ticker the daily job has not reached yet vs a daily job that has
    # stopped. Collapsing them would hide which one is happening.
    marks = _marks(AAA=1, BBB=1)
    v = acf.assess(bucket="b", s3_client=_s3(marks), now=NOW)
    assert v["missing"] == 3 and v["stale"] == 0
    assert any("never ingested" in r for r in v["degraded_reasons"])


def test_patchy_coverage_is_flagged_even_when_what_exists_is_fresh():
    # 2 of 5 fresh, 3 never ingested: everything present is current, but the
    # corpus is patchy — a distinct condition worth naming.
    v = acf.assess(bucket="b", s3_client=_s3(_marks(AAA=1, BBB=1)), now=NOW)
    assert v["coverage"] == 0.4
    assert any("below the" in r for r in v["degraded_reasons"])


def test_verdict_carries_the_thresholds_it_was_judged_against():
    # A verdict a reader cannot re-derive is not auditable.
    v = acf.assess(
        bucket="b", s3_client=_s3(_marks(**{t: 1 for t in SCOPE})), now=NOW,
        max_age_hours=12, min_coverage=0.5,
    )
    assert v["max_age_hours"] == 12 and v["min_coverage"] == 0.5
    assert v["scope"] == 5 and "checked_at" in v
