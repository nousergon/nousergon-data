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
import pathlib

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


# The real reader, captured before the autouse fixture below replaces it, so
# the tier-0 tests can exercise the genuinely committed declaration.
_REAL_READ_APPROVED_SCOPE = alternative._read_approved_scope


@pytest.fixture(autouse=True)
def _tier0_disabled_by_default(monkeypatch):
    """Disable the approved-scope declaration for the prior-run tests.

    Tier 0 is a committed repo artifact declaring ~903 as approved
    (alpha-engine-config-I5814), so once it exists it legitimately
    short-circuits any run near that scope — which would make every
    prior-run-comparison test below pass for the wrong reason. These tests
    target tiers 1 and 2, so they pin tier 0 out explicitly rather than
    depending on the declaration's current contents; the tier-0 tests opt
    back in.
    """
    monkeypatch.setattr(alternative, "_read_approved_scope", lambda: None)


class TestApprovedScopeDeclaration:
    """Tier 0 — the operator-approved baseline (alpha-engine-config-I5951).

    This is the tier that breaks the deadlock: the manifest baseline is written
    only on collection SUCCESS, so after a deliberate step change the only run
    that could advance it is a run the guard blocks. A declared approved scope
    is the escape, and it is exactly what the guard's own error message asks
    for ("record why and widen the tolerance deliberately").
    """

    def test_the_declaration_is_a_repo_artifact_not_an_s3_object(self):
        """It must ship with the checkout, so the merge button alone deploys it.

        An S3 object would have to be written by hand after merge — the
        post-merge operator step pull-request-policy.md 4.2 forbids. Nothing
        about it fails when it never runs: the guard just keeps taking its
        fail-open path while reading green.
        """
        path = alternative._APPROVED_SCOPE_PATH
        assert path.is_file(), f"{path} must be committed alongside the collector"
        assert path.parent == pathlib.Path(alternative.__file__).parent

    def test_the_declaration_carries_its_ruling(self):
        """A bound with no provenance cannot be reviewed or re-examined."""
        declared = _REAL_READ_APPROVED_SCOPE()
        assert declared is not None
        assert isinstance(declared["approved_n"], int)
        assert declared["approved_n"] > 0
        assert declared["ruling"], "the approved scope must name the ruling"
        assert declared["approved_on"], "the approved scope must carry its date"

    def test_the_approved_scope_passes_with_no_prior_baseline_at_all(
        self, monkeypatch
    ):
        """THE DEADLOCK FIX.

        Every run since 2026-07-21 failed mid-collection, so no manifest was
        written and the newest baseline stayed the pre-ruling 27. Without a
        declared approved scope, the ruled ~903 could never be collected —
        the only run able to write a new baseline was the run being blocked.
        """
        monkeypatch.setattr(
            alternative, "_read_approved_scope", _REAL_READ_APPROVED_SCOPE
        )
        approved_n = _REAL_READ_APPROVED_SCOPE()["approved_n"]
        # No manifests, no scope markers — the state the fleet was actually in.
        _assert_scope_stable(ManifestS3({}), "alpha-engine-research", PREFIX,
                             "2026-07-31", approved_n)

    def test_a_scope_far_from_the_approved_bound_still_raises(self, monkeypatch):
        """The declaration blesses ONE scope, not any scope.

        Falling back to the prior-run comparison on disagreement is what stops
        this from becoming a blanket disable.
        """
        monkeypatch.setattr(
            alternative, "_read_approved_scope", _REAL_READ_APPROVED_SCOPE
        )
        # 200 is far outside the tolerance of BOTH the approved ~903 and the
        # prior run's 903, so neither tier can bless it.
        with pytest.raises(ScopeChangedUnexpectedly):
            _assert_scope_stable(ManifestS3({"2026-07-30": 903}),
                                 "alpha-engine-research", PREFIX,
                                 "2026-07-31", 200)

    def test_the_approved_bound_does_not_swallow_a_disagreeing_prior_silently(
        self, monkeypatch, caplog
    ):
        """A run matching neither tier must say what it was compared against.

        The fall-through from tier 0 is the subtle path: if it were silent, an
        operator would see only the prior-run message and never learn that a
        declared approved scope existed and also disagreed.
        """
        import logging

        monkeypatch.setattr(
            alternative, "_read_approved_scope", _REAL_READ_APPROVED_SCOPE
        )
        with caplog.at_level(logging.WARNING):
            with pytest.raises(ScopeChangedUnexpectedly):
                _assert_scope_stable(ManifestS3({"2026-07-30": 903}),
                                     "alpha-engine-research", PREFIX,
                                     "2026-07-31", 200)
        assert "approved" in caplog.text.lower()


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
    assert "no prior scope/manifest" in caplog.text
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


# ---------------------------------------------------------------------------
# collect() layer: explicit empty list ≠ unspecified (alpha-engine-config#6508)
# ---------------------------------------------------------------------------


def test_collect_empty_list_skips_without_deprecated_resolver(monkeypatch):
    """`tickers=[]` means "no scope" — skip, never the signals fallback.

    alpha-engine-config#6508: `if not tickers:` conflated "caller did not
    specify" with "caller resolved no scope", silently re-entering the
    deprecated signals.json::universe resolver from any caller that resolved
    an empty list (the Lambda handler was exactly that caller). Only `None`
    — the documented direct/ad-hoc path — reaches the deprecated resolver.
    """
    import collectors.alternative as alt

    monkeypatch.setattr(alt.boto3, "client", lambda *a, **k: object())
    resolver_calls: list[bool] = []
    monkeypatch.setattr(
        alt, "_load_promoted_tickers",
        lambda *a, **k: resolver_calls.append(True) or ["T1"],
    )

    result = alt.collect(bucket="b", s3_prefix="p/", run_date="2026-08-06", tickers=[])

    assert result["status"] == "skipped"
    assert resolver_calls == [], "deprecated resolver must not be reached"


def test_collect_none_tickers_keeps_deprecated_resolver_as_ad_hoc_path(monkeypatch):
    """`tickers=None` still resolves via signals.json — the documented
    direct/ad-hoc path, unchanged by the #6508 fix."""
    import collectors.alternative as alt

    monkeypatch.setattr(alt.boto3, "client", lambda *a, **k: object())
    monkeypatch.setattr(alt, "_assert_scope_stable", lambda *a, **k: None)
    resolver_calls: list[bool] = []
    monkeypatch.setattr(
        alt, "_load_promoted_tickers",
        lambda *a, **k: resolver_calls.append(True) or ["T1"],
    )

    result = alt.collect(
        bucket="b", s3_prefix="p/", run_date="2026-08-06",
        tickers=None, dry_run=True,
    )

    assert resolver_calls == [True]
    assert result["status"] == "ok_dry_run"
