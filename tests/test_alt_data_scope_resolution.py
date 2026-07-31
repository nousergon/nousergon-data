"""DataPhase2's scope comes from constituents.json, and cannot move silently.

alpha-engine-config-I5814 (Brian ruling 2026-07-31: universe, on spot).

The regression these guard against, measured: on 2026-07-21 this collector's
input went from 27 to 902 tickers overnight, because it resolved its scope from
``signals/{date}/signals.json::universe`` and an unrelated producer swap
(config-I2515 / config#1580) replaced a promoted list with the whole board.
Nothing failed and nothing warned; the only symptom was a 33x wall-clock
increase that broke the 600s Lambda ceiling on the next weekly run.

Two independent fixes, one test module:
  1. the scope is resolved from constituents.json, the same artifact phase 1
     hands to fundamentals.collect, so it no longer tracks the champion;
  2. a run-over-run cardinality assertion, so the NEXT silent scope change
     fails in the same run instead of nine days later.
"""

from __future__ import annotations

import json

import pytest

import collectors.alternative as alternative
from collectors.alternative import ScopeChangedUnexpectedly, _assert_scope_stable


PREFIX = "market_data/"


class _Body:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def read(self) -> bytes:
        return self._raw


class ManifestS3:
    """S3 double serving one prior manifest at a chosen age."""

    def __init__(self, prior: dict[str, int] | None = None) -> None:
        # {"2026-07-24": 903, ...}
        self.prior = prior or {}

    def get_object(self, Bucket: str, Key: str):  # noqa: N803
        for run_date, count in self.prior.items():
            if Key == f"{PREFIX}weekly/{run_date}/alternative/manifest.json":
                return {"Body": _Body(json.dumps({
                    "run_date": run_date,
                    "tickers_requested": count,
                    "tickers_succeeded": count,
                }).encode())}
        raise KeyError(Key)


def test_the_actual_regression_would_have_raised():
    """27 -> 902 is what happened on 2026-07-21. It must not be collectable."""
    s3 = ManifestS3({"2026-07-20": 27})
    with pytest.raises(ScopeChangedUnexpectedly) as exc:
        _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-21", 902)
    message = str(exc.value)
    assert "902" in message and "27" in message
    # The message must name the decision, not just the numbers.
    assert "scope" in message.lower()


def test_a_large_shrink_also_raises():
    """The guard is symmetric — silently collecting 27 when the universe is
    903 is the same defect pointed the other way, and would show up as seven
    feature columns quietly emptying out."""
    s3 = ManifestS3({"2026-07-24": 903})
    with pytest.raises(ScopeChangedUnexpectedly):
        _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 60)


def test_ordinary_reconstitution_churn_passes():
    """S&P 500+400 reconstitution moves a handful of names. That is not a
    scope change and must not page anyone."""
    s3 = ManifestS3({"2026-07-24": 903})
    _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 911)
    _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 890)


def test_boundary_is_the_declared_tolerance():
    s3 = ManifestS3({"2026-07-24": 1000})
    # +30% exactly — inside.
    _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 1300)
    with pytest.raises(ScopeChangedUnexpectedly):
        _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 1301)


def test_no_baseline_fails_open_but_says_so(caplog):
    """First run of a new partition scheme has nothing to compare against.
    Refusing to collect would be worse than collecting — but the run must say
    that the check did not happen, or a silent pass reads as a silent OK."""
    s3 = ManifestS3({})
    with caplog.at_level("WARNING"):
        _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 903)
    assert "no prior manifest" in caplog.text
    assert "cannot be detected" in caplog.text


def test_baseline_older_than_the_lookback_is_not_used():
    """15 days back is outside the window; treated as no baseline."""
    s3 = ManifestS3({"2026-07-10": 27})
    _assert_scope_stable(s3, "alpha-engine-research", PREFIX, "2026-07-25", 903)


def test_collect_runs_the_guard_before_fetching(monkeypatch):
    """The assertion must gate the fetch, not report on it afterwards —
    otherwise a 33x scope change still costs 33x the wall clock before anyone
    hears about it."""
    calls: list[str] = []

    def _fake_assert(*_args, **_kwargs):
        calls.append("guard")
        raise ScopeChangedUnexpectedly("boom")

    def _fail_if_fetched(*_args, **_kwargs):  # pragma: no cover
        calls.append("fetch")
        raise AssertionError("fetched despite a scope-change failure")

    monkeypatch.setattr(alternative, "_assert_scope_stable", _fake_assert)
    monkeypatch.setattr(alternative, "_fetch_all_alternative", _fail_if_fetched)
    monkeypatch.setattr(alternative.boto3, "client", lambda *a, **k: ManifestS3({}))

    with pytest.raises(ScopeChangedUnexpectedly):
        alternative.collect(
            bucket="alpha-engine-research",
            s3_prefix=PREFIX,
            run_date="2026-07-31",
            tickers=["AAPL", "MSFT"],
        )
    assert calls == ["guard"]


def test_phase2_resolves_from_constituents_not_signals(monkeypatch):
    """The scope must come from the same artifact phase 1 gives fundamentals.
    Reading signals.json is what let an unrelated producer swap move it."""
    import weekly_collector as wc

    captured: dict = {}

    monkeypatch.setattr(
        wc.constituents, "load_from_s3",
        lambda bucket, prefix: {"tickers": ["AAA", "BBB", "CCC"]},
    )

    def _fake_collect(**kwargs):
        captured.update(kwargs)
        return {"status": "ok", "tickers_processed": 3, "tickers_failed": 0}

    monkeypatch.setattr(wc.alternative, "collect", _fake_collect)
    monkeypatch.setattr(wc, "_build_registry", lambda *a, **k: None)
    monkeypatch.setattr(wc, "_finalize", lambda *a, **k: None)

    class _Args:
        date = "2026-07-31"
        dry_run = False
        phase = 2

    wc._run_phase2({"bucket": "alpha-engine-research"}, _Args())

    assert captured["tickers"] == ["AAA", "BBB", "CCC"]
    # signals.json must not be consulted at all on this path.
    assert "signals_key" not in captured


def test_phase2_refuses_to_run_without_constituents(monkeypatch):
    """No silent narrowing. A missing constituents artifact used to fall
    through to signals.json — that fallback IS how the scope moved."""
    import weekly_collector as wc

    monkeypatch.setattr(wc.constituents, "load_from_s3", lambda bucket, prefix: None)
    monkeypatch.setattr(wc, "_build_registry", lambda *a, **k: None)
    monkeypatch.setattr(
        wc.alternative, "collect",
        lambda **k: pytest.fail("collected without a constituent universe"),
    )

    class _Args:
        date = "2026-07-31"
        dry_run = False
        phase = 2

    with pytest.raises(wc._CollectorError) as exc:
        wc._run_phase2({"bucket": "alpha-engine-research"}, _Args())
    assert "constituents.json" in str(exc.value)
    assert "signals.json" in str(exc.value)
