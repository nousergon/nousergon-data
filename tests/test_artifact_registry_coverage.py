"""
Artifact-registry coverage CI guard.

Phase 4 of the artifact-freshness-monitor arc (plan doc:
``~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md``).
PR 4 of the 4-PR cascade across ae-data / ae-research / ae-predictor /
ae-backtester. Mirrors the
``alpha-engine-data/tests/test_schema_contract.py`` precedent for
forcing operator attention at every new producer addition.

**What this catches.** A new ``s3.put_object(...)`` or
``s3.upload_file(...)`` site in ae-data production code that hasn't
been registered in
``alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml`` (or
explicitly grandfathered via the registry's ``grandfathered_paths``
block). The producer chokepoint at PR time means the silent
absence-of-artifact bug class (e.g., 2026-05-17→27 pit_parity.json)
can't slip through new code without the operator first deciding
whether the artifact is load-bearing enough to warrant an SLA.

**Why per-file count rather than per-key-template extraction.**
Statically extracting key templates from arbitrary f-string
``put_object(Key=...)`` calls is fragile — keys are often
constructed from surrounding context (loop variables, function args,
config values). Per-file count is **stable across refactors** and
sufficient to force operator review at every new addition. The
operator is the source of truth for "which key does this PUT emit";
the test's job is to ensure the operator can't sleepwalk past adding
one without thinking.

**Sister coverage in the freshness-monitor itself.** This test is
the PR-time chokepoint; runtime validation lives in
``infrastructure/lambdas/freshness-monitor/index.py::load_registry``
(which parses the live registry from S3 and fails the Lambda on a
malformed entry) and in
``alpha-engine-config/scripts/validate_artifact_registry.py`` (PR-time
schema validator on the YAML SoT).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]

# Per-file PUT-site counts. Pinning enforces operator attention on
# every new producer addition. When a file gains/loses a PUT site:
#   1. Decide whether the new artifact is load-bearing.
#   2. Register it in alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml
#      (or add the prefix to grandfathered_paths with a one-line reason).
#   3. Bump the count here.
# When a file is added/removed wholesale: add/remove the entry below
# AND mirror the registry change.
#
# Captured 2026-05-27. See ``_enumerate_put_sites`` for the scan
# semantics (skips tests/, infrastructure/lambdas/, .claude/,
# rag/pipelines/ ingest-side scripts are scope-exempt — they write
# to RAG-corpus S3 not the freshness-monitored production bucket).
EXPECTED_PER_FILE_PUT_COUNTS: dict[str, int] = {
    "builders/_price_cache_writeboth.py": 2,
    # universe_freshness.json + weekly/<date>/manifest.json (schema_drift_incidents,
    # config#1150) + feature_store/_freshness.json (ArcticDB freshness-monitor
    # sentinel, config#1787 — Brian's 2026-07-08 Option-B ruling: registered as
    # an ORDINARY S3 ArtifactSpec in ARTIFACT_REGISTRY.yaml, no changes to
    # nousergon_lib.artifact_freshness or its probe machinery).
    "builders/daily_append.py": 4,
    # 3 -> 4 on alpha-engine-config-I2702 (2026-07-15): a second, separate
    # freshness sentinel — feature_store/_macro_freshness.json — written
    # after the macro/SPY readback-verification block succeeds. Deliberately
    # NOT the same key as the existing feature_store/_freshness.json sentinel
    # above: the universe writer and the macro writer fire on different code
    # paths/cadences, and sharing one key would let whichever write lands
    # last silently mask the other (see MACRO_FRESHNESS_SENTINEL_KEY's module
    # docstring). Consumed by the new
    # infrastructure/lambdas/eod-precondition-probe Lambda — the EOD SF's
    # verify-by-artifact precondition probe, replacing the old
    # $.data_spot_error launch-phase flag test at CheckSkipEODReconcile.
    # FOLLOW-UP (tracked, not yet done in this PR — cross-repo, private):
    # register feature_store/_macro_freshness.json in alpha-engine-config/
    # private-docs/ARTIFACT_REGISTRY.yaml alongside the existing
    # feature_store/_freshness.json entry.
    # builders/migrate_universe_crsp_basis_audit/{ts}.json — the per-ticker CRSP
    # reconciliation REPORT (corporate-actions PR7-7a, config#1434). Like the
    # other one-off migration-audit PUTs (feature_order / vwap below), this is an
    # EVENT-DRIVEN operator-run artifact, NOT a periodic freshness-SLA artifact:
    # there is no cadence to monitor and the migration is a manual spot run, so it
    # is grandfathered out of ARTIFACT_REGISTRY.yaml (no freshness row) — pinned
    # here only to force operator review of the new PUT site.
    "builders/migrate_universe_crsp_basis.py": 1,
    "builders/migrate_universe_feature_order.py": 1,
    "builders/migrate_universe_vwap.py": 1,
    "builders/prune_delisted_tickers.py": 1,
    # builders/backfill_delisted_audit/{date}-{HHMMSSZ}.json — per-run audit record for
    # the config#1943 Leg-3 backfill (data#712). Same pattern as prune_delisted_tickers.py's
    # builders/prune_audit/ write directly above: an EVENT-DRIVEN, operator-triggered audit
    # of a one-off/occasional manual run, NOT a periodic freshness-SLA artifact — there is no
    # cadence to monitor (this builder has no scheduled trigger; Brian runs it by hand), so
    # per that established precedent it is grandfathered out of ARTIFACT_REGISTRY.yaml (no
    # freshness row, not even in grandfathered_paths — mirrors prune_delisted_tickers.py's own
    # audit, which also carries no registry row), pinned here only to force operator review of
    # the new PUT site.
    "builders/backfill_delisted_history.py": 1,
    "collectors/alternative.py": 3,
    # data/sub_industry_map.json + reference/price_cache/sub_industry_map.json
    # (config#934 narrow slice, 2026-07-09): additive GICS sub-industry capture
    # alongside the existing sector_map.json dual-write (1 new put_object call
    # site, textually — it writes both paths in a loop). No consumer exists yet
    # (nothing downstream reads it — the cross-repo sub-sector-benchmark/
    # crucible-predictor feature-wiring scope is unstarted, still gated on
    # design Brian hasn't ruled on). No periodic freshness SLA to monitor until
    # a consumer exists, so grandfathered out of ARTIFACT_REGISTRY.yaml (see
    # alpha-engine-config private-docs/ARTIFACT_REGISTRY.yaml grandfathered_paths)
    # rather than registered with a speculative cadence/SLA.
    #
    # 4th PUT site (config#934): data/sub_sector_etf_map.json +
    # reference/price_cache/sub_sector_etf_map.json — ticker → sub-sector
    # benchmark ETF (defaulting to the sector ETF), consumed by
    # feature_engineer's sub_sector_vs_benchmark_* features. Same dual-path
    # loop as the two maps above (1 new put_object call site, textually). The
    # two new S3 paths still need an ARTIFACT_REGISTRY.yaml grandfather —
    # companion config PR, same as config#2020 did for sub_industry_map.
    "collectors/constituents.py": 4,
    # crypto/holdings.json — Metron crypto-page wallet balances (metron-ops#111). The
    # ARTIFACT_REGISTRY freshness row is DEFERRED until the producer is live (IAM + timer
    # installed) per "never register a freshness entry ahead of its producer" — registering
    # now would report a false state=missing. Tracked in metron-ops#111.
    "collectors/crypto_balances.py": 1,
    "collectors/daily_closes.py": 1,
    "collectors/daily_closes_fred_repair.py": 1,
    "collectors/fred_history.py": 1,
    "collectors/fundamentals.py": 1,
    "collectors/historical_constituents.py": 1,  # market_data/historical_constituents.json — PIT S&P 500 membership (#657, G12)
    "collectors/macro.py": 3,  # weekly/<date>/macro.json + macro_history.parquet + macro_release_calendar.parquet
    "collectors/metron_market_data.py": 1,  # one _write_json site → market_data/eod_closes/* + market_data/fx/*
    "collectors/prices.py": 1,
    "collectors/short_interest.py": 1,
    "collectors/universe_classification.py": 1,  # market_data/universe_classification/{date,latest}.json — sector/country/industry for the ~900 universe board
    "collectors/signal_returns.py": 1,
    "collectors/universe_returns.py": 1,
    # corporate_actions/actions/{action_id}.json + applied/{store}/{action_id}.json —
    # EVENT-DRIVEN corporate-action provenance records (config#1431), written only
    # when a split is detected/applied. NOT a periodic freshness-SLA artifact: there
    # is no cadence to monitor and the absence of a split is not an incident, so this
    # is grandfathered out of ARTIFACT_REGISTRY.yaml (no freshness row) per "never
    # register a freshness entry ahead of/without a periodic producer".
    "corporate_actions/registry.py": 1,
    # corporate_actions.sync (PR4, config#1433) rewrites the EXISTING
    # staging/daily_closes/{date}.parquet archive IN PLACE (split restatement) —
    # it produces no NEW artifact (that prefix already has its freshness SLA via
    # the daily_closes collector), so no new ARTIFACT_REGISTRY row is required;
    # this single PUT site is the in-place archive write-back.
    "corporate_actions/__init__.py": 1,
    "data/cache.py": 1,
    "data/derived/analyst_revisions.py": 2,
    "data/derived/news_aggregates.py": 2,
    "data/derived/news_articles.py": 2,  # raw-article parquet + latest.json sidecar
    "data/derived/news_digest.py": 2,  # podcast digest: {run_id}.json + latest.json
    "data/snapshotter/analyst_daily.py": 2,
    "features/compute.py": 1,
    "features/registry.py": 1,
    # features/metron_supplemental/{date}/sectors.json sidecar (metron-ops#177) —
    # the module's parquet writes reuse features/writer.py's existing PUT site
    # (already pinned above); this is the ONE new site, the sectors sidecar.
    # Covered by the already-grandfathered "features/" path_prefix in
    # ARTIFACT_REGISTRY.yaml (per-feature parquet artifacts, ArcticDB migration
    # retired the S3 mirror) — no new registry row needed.
    "features/metron_supplemental.py": 1,
    "features/writer.py": 1,
    "lambda/handler.py": 1,
    "preflight.py": 1,
    "rag/pipelines/emit_manifest.py": 2,
    "rag/pipelines/filing_change_detection.py": 2,
    "rag/pipelines/ingest_form4.py": 2,
    # config#1727: _write_module_health delegates to nousergon_lib.health (1 fewer
    # local put_object); health/{module}.json still written via lib.
    # alpha-engine-config-I2428: SEC quarterly Form 13F bulk data pipeline.
    # Writes inst_ownership/{quarter}/result.parquet (artifact_key),
    # inst_ownership/{quarter}/latest.parquet (per-quarter mirror),
    # data/inst_ownership/latest.json (sidecar), and
    # data/crosswalks/cusip_to_ticker.json (CUSIP→ticker cache).
    # ARTIFACT_REGISTRY.yaml row: thinktank_inst_ownership.
    "data/derived/inst_ownership.py": 4,
    # 5 -> 6 on alpha-engine-config#2672 (2026-07-16, Brian-ratified binding
    # design): _data_quality/pending_upgrades.json — the durable
    # desired-state ledger so a fallback-quality (yfinance-basis) universe
    # day can't age out unhealed past the sliding-window detectors' lookback.
    # A single small JSON object touched at most twice/day (mark on EOD
    # yfinance write, clear on morning polygon-corrected write / gap-heal
    # success) — control-plane reconciliation state, not a periodic
    # freshness-SLA data artifact consumers read on a cadence (its absence
    # is self-evident via the sliding-window detectors that union it, not via
    # a freshness-monitor staleness check). Grandfathered out of
    # ARTIFACT_REGISTRY.yaml for that reason, mirroring the
    # corporate_actions/registry.py and sub_industry_map.json precedents
    # above — pinned here only to force this operator review of the new PUT
    # site.
    # 6 -> 7 on alpha-engine-config-I2717 (2026-07-16): the new standalone
    # --daily-heal entrypoint (_run_daily_heal) writes a heal-summary artifact
    # to data/heal/daily/{run_date}.json — the artifact the freshness-monitor
    # plane watches per the I2722 health-plane ruling (extend the existing
    # Lambda watch plane, no new bundled health SF). FOLLOW-UP tracked as
    # alpha-engine-config-I2749 (cross-repo, private): register
    # data/heal/daily/{run_date}.json in alpha-engine-config/private-docs/
    # ARTIFACT_REGISTRY.yaml.
    "weekly_collector.py": 7,
}


# Path-prefix exemptions. These directories are not part of the
# freshness-monitored production-artifact surface:
#   - tests/ ........................ test code, not production producers
#   - infrastructure/lambdas/ ....... Lambda code; tested per-Lambda
#                                     (sf-telegram-notifier and the
#                                     freshness-monitor itself emit
#                                     observational artifacts already
#                                     covered in their own tests)
#   - .claude/ ...................... worktrees + agent-managed scratch
#   - .venv/, build/ ............... local-dev only
_SCAN_EXEMPT_PREFIXES: tuple[str, ...] = (
    "tests/",
    "infrastructure/lambdas/",
    ".claude/",
    ".venv/",
    "build/",
    "scripts/_",  # operator scratch scripts
)


# ── PUT-site enumeration ────────────────────────────────────────────────────


def _enumerate_put_sites() -> dict[str, int]:
    """Return a ``{relative_path: count}`` mapping of every production
    file containing ``put_object`` or ``upload_file`` literal references.

    Uses ``git grep`` for tracked-file discipline (matches CI-time
    behavior — untracked scratch files don't pollute the count).
    """
    result = subprocess.run(
        [
            "git", "grep", "-l", "-E",
            r"(put_object|upload_file)\(",
            "--", "*.py",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    files = [
        line for line in result.stdout.splitlines()
        if line and not any(line.startswith(p) for p in _SCAN_EXEMPT_PREFIXES)
    ]

    counts: dict[str, int] = {}
    for rel in files:
        path = REPO_ROOT / rel
        text = path.read_text(encoding="utf-8", errors="ignore")
        # Count literal call-site occurrences. We use a regex for
        # ``put_object(`` and ``upload_file(`` rather than counting
        # the bare words so docstring / comment mentions don't inflate.
        matches = re.findall(r"\b(?:put_object|upload_file)\(", text)
        counts[rel] = len(matches)
    return counts


# ── Tests ───────────────────────────────────────────────────────────────────


def test_every_producer_file_is_pinned():
    """Every file containing a PUT site is enumerated in
    :data:`EXPECTED_PER_FILE_PUT_COUNTS`. A new file with a PUT site
    forces the operator to (a) register the artifact in
    ``alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml`` or
    grandfather it, then (b) add the file to the pinned set."""
    actual = _enumerate_put_sites()
    unpinned = sorted(set(actual.keys()) - set(EXPECTED_PER_FILE_PUT_COUNTS.keys()))
    assert not unpinned, (
        "New producer file(s) with S3 PUT sites detected but not pinned:\n"
        + "\n".join(f"  - {f} ({actual[f]} PUT call(s))" for f in unpinned)
        + "\n\nResolution:\n"
        "  1. Register the new artifact(s) in alpha-engine-config/"
        "private-docs/ARTIFACT_REGISTRY.yaml (or add the prefix to "
        "grandfathered_paths with a one-line reason).\n"
        "  2. Add the file(s) to EXPECTED_PER_FILE_PUT_COUNTS in "
        "tests/test_artifact_registry_coverage.py with the per-file count.\n"
        "  3. Re-run this test. Plan doc: "
        "~/Development/alpha-engine-docs/private/artifact-freshness-monitor-260527.md"
    )


def test_every_pinned_file_still_exists():
    """The pinned set must stay aligned with the actual repo state.
    A file removed without updating this set is a stale pin."""
    actual = _enumerate_put_sites()
    stale = sorted(set(EXPECTED_PER_FILE_PUT_COUNTS.keys()) - set(actual.keys()))
    assert not stale, (
        "Pinned file(s) no longer have PUT sites (or no longer exist):\n"
        + "\n".join(f"  - {f}" for f in stale)
        + "\n\nResolution: remove the file from EXPECTED_PER_FILE_PUT_COUNTS. "
        "If the artifact was retired, also retire its row in "
        "alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml."
    )


def test_pinned_counts_match_actual():
    """Per-file PUT-site counts must match the pinned values. A
    delta forces operator review: new PUT site needs registry entry;
    removed PUT site may need registry retirement."""
    actual = _enumerate_put_sites()
    deltas = []
    for path, expected_count in sorted(EXPECTED_PER_FILE_PUT_COUNTS.items()):
        actual_count = actual.get(path, 0)
        if actual_count != expected_count:
            deltas.append(f"  - {path}: expected={expected_count}, actual={actual_count}")
    assert not deltas, (
        "PUT-site count drift detected:\n"
        + "\n".join(deltas)
        + "\n\nResolution: for each delta, either (a) the PUT count changed "
        "legitimately — register the new artifact in alpha-engine-config/"
        "private-docs/ARTIFACT_REGISTRY.yaml (or grandfather), then bump "
        "the pinned count; or (b) the change was inadvertent — revert."
    )
