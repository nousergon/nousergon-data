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


def _s3(marks: dict, *, generated_at: str | None = None, scope: list | None = None):
    scope = SCOPE if scope is None else scope
    s3 = MagicMock()
    payloads = {
        "universe_membership/latest.json": {
            "run_date": "2026-07-31",
            "generated_at": generated_at,
            "predictor_universe_cut": "attractiveness_top_20",
            # config-I6630: the attractiveness RANK cut, not the gate cut.
            "cuts": {
                "attractiveness_top_60": {"size": len(scope), "tickers": scope},
                "attractiveness_top_20": {"size": 1, "tickers": scope[:1]},
            },
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


# ── naming and cause (alpha-engine-config-I12468) ────────────────────────
#
# The 2026-10-17 verdict said "8 of 108 stale" and nothing else; working out
# which 8, and why, took a by-hand rebuild of the scope from S3 object
# versions. The answer then was: the scanner published a new cut at 10:55Z,
# the check graded it at 11:00Z, and the eight names that had just entered the
# cut were fetched by the daily ingest at 11:26Z. No feed was dead. These tests
# pin the verdict to saying that on its own.


def _iso_ago(h: float) -> str:
    return (NOW - timedelta(hours=h)).isoformat().replace("+00:00", "Z")


def test_stale_and_missing_tickers_are_named_not_just_counted():
    marks = _marks(AAA=1, BBB=200, CCC=900)
    v = acf.assess(bucket="b", s3_client=_s3(marks), now=NOW)
    assert v["stale"] == 2 and v["missing"] == 2
    # Oldest first, each with the mark it was judged on.
    assert [d["ticker"] for d in v["stale_tickers"]] == ["CCC", "BBB"]
    assert v["stale_tickers"][0]["last_ingested_at"] == _iso_ago(900)
    assert v["stale_tickers"][0]["age_hours"] == 900.0
    assert v["missing_tickers"] == ["DDD", "EEE"]
    assert v["stale_tickers_truncated"] == 0 and v["missing_tickers_truncated"] == 0


def test_scope_published_after_the_last_ingest_is_named_as_the_cause():
    # The 2026-10-17 shape: the ingest last ran 23.5h ago (fresh), the scope
    # was published 5 minutes ago, and two names that just entered it were
    # last fetched weeks back.
    marks = _marks(AAA=23.5, BBB=23.5, CCC=23.5, DDD=1438, EEE=575)
    v = acf.assess(
        bucket="b", s3_client=_s3(marks, generated_at=_iso_ago(5 / 60)), now=NOW,
    )
    assert v["degraded"] is True
    assert v["not_fresh_cause"] == acf.CAUSE_SCOPE_CHANGED_AFTER_LAST_INGEST
    assert v["last_ingest_at"] == _iso_ago(23.5)
    assert v["last_ingest_age_hours"] == 23.5
    assert v["scope_generated_at"] == _iso_ago(5 / 60)
    assert [d["ticker"] for d in v["stale_tickers"]] == ["DDD", "EEE"]
    assert any(acf.CAUSE_SCOPE_CHANGED_AFTER_LAST_INGEST in r
               for r in v["degraded_reasons"])


def test_ingest_that_ran_after_the_scope_and_skipped_names_is_a_fetch_set_divergence():
    # Same marks, but the scope is OLDER than the last ingest: the ingest had
    # its chance and still did not fetch these names. That is a different
    # defect (its fetch set differs from this scope) and must not be reported
    # as the benign "scope just changed".
    marks = _marks(AAA=2, BBB=2, CCC=2, DDD=300)
    v = acf.assess(
        bucket="b", s3_client=_s3(marks, generated_at=_iso_ago(48)), now=NOW,
    )
    assert v["not_fresh_cause"] == acf.CAUSE_OUTSIDE_LAST_FETCH_SET
    assert v["missing_tickers"] == ["EEE"]


def test_an_ingest_that_stopped_is_named_as_such():
    marks = _marks(**{t: 200 for t in SCOPE})
    v = acf.assess(
        bucket="b", s3_client=_s3(marks, generated_at=_iso_ago(1)), now=NOW,
    )
    assert v["not_fresh_cause"] == acf.CAUSE_INGEST_STALE


def test_an_empty_store_is_named_as_no_ingest_recorded():
    v = acf.assess(bucket="b", s3_client=_s3({}), now=NOW)
    assert v["missing"] == 5
    assert v["not_fresh_cause"] == acf.CAUSE_NO_INGEST_RECORDED
    assert v["last_ingest_at"] is None and v["last_ingest_age_hours"] is None


def test_the_cause_never_changes_the_degraded_verdict():
    # Naming a cause is reporting, not grading: a newcomer that entered the
    # scope minutes ago is still a name the run reads no fresh news for, so
    # the flag stays up. And a warm corpus carries no cause at all.
    warm = acf.assess(
        bucket="b",
        s3_client=_s3(_marks(**{t: 6 for t in SCOPE}), generated_at=_iso_ago(0.1)),
        now=NOW,
    )
    assert warm["degraded"] is False and warm["not_fresh_cause"] is None
    assert warm["degraded_reasons"] == []
    assert warm["stale_tickers"] == [] and warm["missing_tickers"] == []

    churned = acf.assess(
        bucket="b",
        s3_client=_s3(_marks(AAA=6, BBB=6, CCC=6, DDD=6, EEE=1438),
                      generated_at=_iso_ago(0.1)),
        now=NOW,
    )
    assert churned["degraded"] is True and churned["stale"] == 1


def test_last_ingest_reads_the_whole_store_not_just_the_scope():
    # A name outside this scope that the ingest fetched an hour ago still
    # proves the ingest is running.
    marks = {**_marks(**{t: 300 for t in SCOPE}), **_marks(ZZZ=1)}
    v = acf.assess(
        bucket="b", s3_client=_s3(marks, generated_at=_iso_ago(48)), now=NOW,
    )
    assert v["last_ingest_age_hours"] == 1.0
    assert v["not_fresh_cause"] == acf.CAUSE_OUTSIDE_LAST_FETCH_SET


def test_named_lists_are_capped_and_the_cap_is_recorded(monkeypatch):
    monkeypatch.setattr(acf, "MAX_NAMED_TICKERS", 2)
    v = acf.assess(
        bucket="b", s3_client=_s3(_marks(**{t: 200 for t in SCOPE})), now=NOW,
    )
    assert len(v["stale_tickers"]) == 2 and v["stale_tickers_truncated"] == 3
    assert v["stale"] == 5
