"""Every unit's declared write targets are pinned, so a `writes:` change is loud.

`alpha-engine-config-I10966`: D46's `writes:` templates were corrected
(`alpha-engine-config-I10895`) from a shape `rag/pipelines/ingest_form4.py`
had never written to the shape it actually writes. The correction was RIGHT —
`nousergon_lib.eval_artifacts` (one run-stamped `..._result.parquet` per
invocation plus a `latest.json` sidecar) is what the code has always done.
`data.D46.artifact_registry` went UNMET the same day, and the wrong side was
NOT this repo: `data_gate/unit_readers.py::read_artifact_registry` and
`data_gate/sources.py` both establish that the artifact registry it grades
against is read from the copy the freshness monitor enforces
(``s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml``),
published by the PRIVATE `alpha-engine-config` repo's own
`sync-artifact-registry.yml` from `private-docs/ARTIFACT_REGISTRY.yaml`.
There is no `ARTIFACT_REGISTRY.yaml` in `nousergon-data` to correct — the
registry that fell out of date lives entirely in the other repo, and this
repo cannot check it out (no cross-repo credential; the runtime reader only
resolves the published S3 copy, under a workflow-scoped role).

What IS a defect in this repo: nothing forced the operator to notice the
correction needed a matching registry-side change. The producer-chokepoint
test that exists for this class (`test_artifact_registry_coverage.py`) grades
PRODUCER CODE — new `s3.put_object`/`upload_file` call sites — and I10895
touched only the descriptor YAML, so it never fired.

This test closes that gap the same shape
`tests/test_every_audit_cell_is_a_clause.py` closes the audit-count one: a
PINNED snapshot of every unit's declared write targets (S3 key templates,
ArcticDB libraries, and explicitly ungradable entries — see
`data_gate.unit_readers.s3_write_targets`). A `writes:` edit that changes what
a unit declares and is not matched by an update to
``EXPECTED_WRITE_TARGETS`` below fails here, at review time, rather than
surfacing as a board finding a day later. The failure message is the
forcing function: go confirm (or file a follow-up naming) the matching
`alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml` row in the same
change.

This does not (and, from this repo, cannot) assert the private registry
itself is correct — only that a LOCAL change to what a unit claims it writes
can never again land unnoticed the way I10895 did.
"""

from __future__ import annotations

from data_gate.descriptors import load_units
from data_gate.unit_readers import s3_write_targets

#: unit_id -> (s3 key templates, ArcticDB libraries, ungradable entries),
#: each tuple sorted. Keep this in sync with `registry.d/units/*.yaml`'s
#: `writes:` blocks — see the module docstring for what a mismatch means.
EXPECTED_WRITE_TARGETS: dict[str, tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]] = {
    "D01": (
        (
            "data/sector_map.json",
            "data/sub_industry_map.json",
            "data/sub_sector_etf_map.json",
            "market_data/latest_weekly.json",
            "market_data/weekly/{date}/constituents.json",
            "reference/price_cache/sector_map.json",
            "reference/price_cache/sub_industry_map.json",
            "reference/price_cache/sub_sector_etf_map.json",
        ),
        (),
        (),
    ),
    "D02": (("market_data/historical_constituents.json",), (), ()),
    "D03": (("reference/price_cache/{ticker}.parquet",), (), ()),
    "D04": (("reference/price_cache/{ticker}.parquet",), (), ()),
    "D05": (
        (
            "market_data/macro_history.parquet",
            "market_data/macro_release_calendar.parquet",
            "market_data/weekly/{date}/macro.json",
        ),
        (),
        (),
    ),
    "D06": (("market_data/weekly/{date}/short_interest.json",), (), ()),
    "D07": (
        (
            "market_data/universe_classification/latest.json",
            "market_data/universe_classification/{date}.json",
        ),
        (),
        (),
    ),
    "D08": (("backups/research_{date}.db", "research.db"), (), ()),
    "D09": ((), (), ("research.db::score_performance",)),
    "D10": (("archive/fundamentals/{date}.json",), (), ()),
    "D11": (("market_data/valuation_medians/latest.json",), (), ()),
    "D12": (
        (
            "features/{date}/alternative.parquet",
            "features/{date}/fundamental.parquet",
            "features/{date}/interaction.parquet",
            "features/{date}/macro.parquet",
            "features/{date}/technical.parquet",
        ),
        (),
        (),
    ),
    "D13": ((), ("macro", "universe"), ()),
    "D14": (
        ("builders/prune_audit/{trading_day}-*.json",),
        (),
        ("delisted_history::{ticker}",),
    ),
    "D15": (
        (
            "market_data/weekly/{date}/alternative/manifest.json",
            "market_data/weekly/{date}/alternative/scope.json",
            "market_data/weekly/{date}/alternative/{ticker}.json",
        ),
        (),
        (),
    ),
    "D15L": (("health/data_phase2.json",), (), ("same keys as D15",)),
    "D16": (
        (
            "health/rag_ingestion_progress/{date}.json",
            "rag/filing_changes/",
            "rag/manifest/latest.json",
            "rag/manifest/{date}.json",
        ),
        (),
        (),
    ),
    "D17": (("staging/daily_closes/{date}.parquet",), (), ()),
    "D18": ((), ("universe",), ()),
    "D19": (("staging/daily_closes/{date}.parquet",), (), ()),
    "D20": (
        (
            "market_data/eod_closes/latest.json",
            "market_data/eod_closes/{date}.json",
            "market_data/fx/latest.json",
            "market_data/fx/{date}.json",
        ),
        (),
        (),
    ),
    "D21": (
        (
            "market_data/close_history/consolidated.json",
            "market_data/close_history/{sym}.json",
            "market_data/fx_history/{ccy}.json",
        ),
        (),
        (),
    ),
    "D22": (("market_data/earnings/latest.json", "market_data/sectors/latest.json"), (), ()),
    "D23": (("market_data/macro/latest.json",), (), ()),
    "D24": (("market_data/fundamentals/latest.json",), (), ()),
    "D25": (("market_data/technicals/latest.json",), (), ()),
    "D26": (
        (
            "market_data/technicals/rating_history/*",
            "market_data/technicals/rating_history/_manifest.json",
        ),
        (),
        (),
    ),
    "D27": (("market_data/technicals/rating_performance.json",), (), ()),
    "D28": (("market_data/security_performance/latest.json",), (), ()),
    "D29": (("market_data/analyst/latest.json",), (), ()),
    "D30": (("market_data/sentiment/latest.json",), (), ()),
    "D31": (("features/metron_supplemental/", "features/{date}/*.parquet"), (), ()),
    "D32": ((), ("universe",), ()),
    "D33": (("data/heal/daily/{date}.json", "staging/daily_closes/*"), ("universe",), ()),
    "D34": (("reference/price_cache/*",), ("universe",), ()),
    "D35": ((), ("universe",), ()),
    "D36": (
        (
            "data/news_aggregates_daily/latest.json",
            "data/news_articles_daily/latest.json",
            "data/news_digest_daily/latest.json",
        ),
        (),
        (),
    ),
    "D37": (
        ("market_data/intraday/latest.json", "market_data/intraday/technical_ratings.json"),
        (),
        (),
    ),
    "D38": (("crypto/holdings.json",), (), ()),
    "D39": (
        ("data/inst_ownership/latest.json", "data/inst_ownership/{quarter}/{ticker}.parquet"),
        (),
        (),
    ),
    "D40": (("data/analyst_snapshots/{ticker}/latest.json",), (), ()),
    "D41": (("data/analyst_revisions/",), (), ()),
    "D42": ((), ("macro", "universe", "universe_schema_meta"), ()),
    "D43": ((), ("macro", "universe"), ()),
    "D46": (
        ("data/insider_transactions/latest.json", "data/insider_transactions/{run_stamp}_result.parquet"),
        (),
        (),
    ),
    "D47": (
        (),
        (),
        (
            "crucible-v2 store: data/{trading_day}/coverage.json",
            "crucible-v2 store: data/{trading_day}/panel.parquet",
            "crucible-v2 store: features/{version}/{trading_day}.parquet",
        ),
    ),
}


def test_every_unit_has_a_pinned_write_target_row():
    units = {u.unit_id for u in load_units()}
    pinned = set(EXPECTED_WRITE_TARGETS)
    assert units == pinned, (
        f"registry.d/units/ and this pin disagree on the unit set: {sorted(units ^ pinned)} — a "
        "unit was added to or removed from registry.d/units/ without updating this file"
    )


def test_every_units_write_targets_match_the_pin():
    units = load_units()
    mismatches = []
    for unit in sorted(units, key=lambda u: u.unit_id):
        keys, libs, ungradable = s3_write_targets(unit)
        got = (tuple(sorted(keys)), tuple(sorted(libs)), tuple(sorted(ungradable)))
        expected = EXPECTED_WRITE_TARGETS.get(unit.unit_id)
        if got != expected:
            mismatches.append((unit.unit_id, expected, got))
    assert not mismatches, (
        "unit(s) whose declared write targets changed without a matching update to "
        "EXPECTED_WRITE_TARGETS in this file. Update the pin here AND confirm the matching "
        "alpha-engine-config/private-docs/ARTIFACT_REGISTRY.yaml row (or grandfathered_paths "
        f"entry) in the same change — that is the step I10966 found missing: {mismatches}"
    )
