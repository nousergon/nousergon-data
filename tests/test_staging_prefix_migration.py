"""Regression tests pinning the staging/daily_closes/ prefix migration.

The path was renamed from ``predictor/daily_closes/{date}.parquet`` to
``staging/daily_closes/{date}.parquet`` to (1) move the intermediate
parquet out of the ``predictor/`` namespace where it lived alongside
authoritative artifacts (it's actually intermediate state between API
fetch and ArcticDB ingest), and (2) make it eligible for an S3 lifecycle
expiration (bucket rule ``expire-staging-after-7-days``, declared and
applied by the nous-ergon-ops repo's bucket-config lane).

These tests lock the prefix in place across the writer + both readers
so a future PR can't silently regress one of the call sites without
the other.

Coordinated cross-repo cutover:

  - alpha-engine-data (this repo): writer + 2 readers — covered here
  - alpha-engine-research: ``feature_store_reader.read_latest_daily_closes``
  - alpha-engine-dashboard: ``health_checker.py`` daily_closes probe +
    ``pages/4_System_Health.py`` S3 object count
  - alpha-engine: stale executor IAM grant (separate cleanup PR)

Per ``feedback_no_silent_fails``: hard-cutover, no fallback. If the new
prefix is missing, every reader fails loud (RuntimeError or NoSuchKey).
"""

from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return (Path(__file__).parent.parent / path).read_text()


# ── Writer-side: collectors/daily_closes.py ─────────────────────────────────


def test_collector_default_prefix_is_staging():
    """``collect()`` must default ``s3_prefix`` to ``staging/daily_closes/``."""
    from collectors.daily_closes import collect
    import inspect
    sig = inspect.signature(collect)
    default = sig.parameters["s3_prefix"].default
    assert default == "staging/daily_closes/", (
        f"collect() s3_prefix default regressed to {default!r}. The "
        f"prefix migration to staging/ is load-bearing for the S3 lifecycle "
        f"policy + the explicit 'intermediate state' contract."
    )


def test_collector_no_legacy_prefix_in_source():
    """Belt-and-suspenders: forbid the legacy prefix string anywhere in the
    collector source, including module docstring + comments. Catches the
    case where someone reverts the default but leaves a stray legacy string."""
    src = _read("collectors/daily_closes.py")
    assert "predictor/daily_closes" not in src, (
        "collectors/daily_closes.py contains 'predictor/daily_closes' — "
        "the prefix was migrated to staging/. Update the offending line."
    )


# ── Reader-side: builders/daily_append.py ───────────────────────────────────


def test_daily_append_reads_staging_prefix():
    """``_load_daily_closes`` must read from ``staging/daily_closes/``."""
    src = _read("builders/daily_append.py")
    assert 'f"staging/daily_closes/{date_str}.parquet"' in src, (
        "builders/daily_append.py:_load_daily_closes is not reading from "
        "staging/daily_closes/{date_str}.parquet. Hard-cutover requires "
        "this exact f-string per the no-fallback contract."
    )
    assert "predictor/daily_closes" not in src, (
        "builders/daily_append.py contains 'predictor/daily_closes' — "
        "fallback to the legacy prefix is forbidden per "
        "feedback_no_silent_fails."
    )


# ── Reader-side: features/compute.py ────────────────────────────────────────


def test_features_compute_reads_staging_prefix():
    """``features/compute.py`` (used by daily_append, backfill,
    prune_delisted_tickers, promote_ohlcv_only_schema, migrate_universe_vwap,
    weekly_collector) must read from ``staging/daily_closes/``."""
    src = _read("features/compute.py")
    assert 'f"staging/daily_closes/{d}.parquet"' in src, (
        "features/compute.py is not reading from staging/daily_closes/. "
        "Hard-cutover requires this exact path; legacy fallback forbidden."
    )
    assert "predictor/daily_closes" not in src, (
        "features/compute.py contains 'predictor/daily_closes' — "
        "fallback forbidden."
    )


# ── Orchestrator: weekly_collector.py ───────────────────────────────────────


def test_weekly_collector_default_prefix_is_staging():
    """Both call sites in weekly_collector must pass the staging/ default
    when config doesn't override."""
    src = _read("weekly_collector.py")
    n_old = src.count('"predictor/daily_closes/"')
    n_new = src.count('"staging/daily_closes/"')
    assert n_old == 0, (
        f"weekly_collector.py contains {n_old} legacy 'predictor/daily_closes/' "
        f"defaults — replace with 'staging/daily_closes/'."
    )
    assert n_new >= 2, (
        f"weekly_collector.py expected to pass 'staging/daily_closes/' as the "
        f"daily_cfg.get fallback in both --daily and --morning-enrich paths "
        f"(2 sites); found {n_new}."
    )


# ── Lifecycle policy artifact ───────────────────────────────────────────────


def test_lifecycle_policy_artifact_exists_and_is_valid_json():
    """The bucket's lifecycle is NOT owned here (alpha-engine-config-I10897).

    This repo used to carry a lifecycle JSON plus a script that applied it
    with a whole-document replace. The bucket's versioning and lifecycle are
    now declared, applied on merge and drift-checked by the nous-ergon-ops
    repo's bucket-config lane, which carries the ``staging/`` 7-day rule this
    test used to pin. Running the old script would have replaced that
    document and dropped every rule it did not list, including the
    noncurrent-version expiry. Neither file may come back.
    """
    infra = Path(__file__).parent.parent / "infrastructure"
    for name in ("s3_lifecycle_staging.json", "apply_s3_lifecycle.sh"):
        path = infra / name
        assert not path.exists(), (
            f"{path} is back. The research bucket's lifecycle is owned by "
            f"the nous-ergon-ops repo's declared bucket-config lane "
            f"(alpha-engine-config-I10897); a second writer here replaces its "
            f"whole document. Change the rule there, not here."
        )
