"""Tests for sf_preflight.py — Saturday SF dry-rehearsal.

Each check tested independently with mocked S3 / ArcticDB / polygon /
Wikipedia. Asserts both the happy path and the specific failure mode
each check is designed to catch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import sf_preflight as sfp


from collectors import constituents


def _ctx(bucket: str = "test-bucket") -> sfp.PreflightContext:
    return sfp.PreflightContext(
        bucket=bucket,
        today="2026-05-02",
        prior_trading_day="2026-05-01",
    )


# ── check_constituents_fetch ──────────────────────────────────────────────────


def test_constituents_fetch_ok_populates_context():
    ctx = _ctx()
    fake_return = (
        ["AAPL"] * 500 + ["MSFT"] * 400,  # tickers (totals: ~903 like prod)
        {**{f"T{i}": "Industrials" for i in range(900)},  # sector_map covers all
         "AAPL": "Information Technology", "MSFT": "Information Technology"},
        {"AAPL": "XLK", "MSFT": "XLK"},  # sector_etf_map
        {},  # sub_industry_map
        500,  # sp500_count
        400,  # sp400_count
        constituents.SsgaWeights(),  # weights (I11295)
    )
    # Actually use realistic-shape data: deduped tickers + complete sector_map.
    real_tickers = [f"T{i}" for i in range(900)]
    real_sectors = {t: "Industrials" for t in real_tickers}
    fake_return = (
        real_tickers, real_sectors, {}, {}, 500, 400, constituents.SsgaWeights()
    )

    with patch("collectors.constituents._fetch_constituents", return_value=fake_return):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "ok"
    assert "900 tickers" in result.message
    assert ctx.fresh_constituents == set(real_tickers)


def test_constituents_fetch_fails_on_zero_tickers():
    ctx = _ctx()
    with patch("collectors.constituents._fetch_constituents", return_value=([], {}, {}, {}, 0, 0, constituents.SsgaWeights())):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "0 tickers" in result.message
    assert ctx.fresh_constituents is None


def test_constituents_fetch_fails_on_unmapped_tickers():
    """Pre-empts the RuntimeError that constituents.collect would raise."""
    ctx = _ctx()
    tickers = [f"T{i}" for i in range(900)]
    # Sector map is missing 50 tickers — collect() would hard-fail at write time.
    sectors = {t: "Industrials" for t in tickers[:850]}
    with patch("collectors.constituents._fetch_constituents",
               return_value=(
                   tickers, sectors, {}, {}, 500, 400, constituents.SsgaWeights()
               )):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "sector_map missing" in result.message


def test_constituents_fetch_fails_on_sp500_count_drift():
    """If Wikipedia parsing drops the table, sp500_count tanks."""
    ctx = _ctx()
    tickers = [f"T{i}" for i in range(400)]
    with patch(
        "collectors.constituents._fetch_constituents",
        return_value=(
            tickers, {t: "Industrials" for t in tickers}, {}, {}, 0, 400,
            constituents.SsgaWeights(),
        ),
    ):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "S&P 500 count" in result.message


def test_constituents_fetch_fails_on_wikipedia_exception():
    ctx = _ctx()
    with patch("collectors.constituents._fetch_constituents",
               side_effect=ConnectionError("Wikipedia 503")):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "Wikipedia 503" in result.message


# ── check_universe_drift (PR #134 class) ──────────────────────────────────────


def _stub_universe_lib_for_drift(stragglers_with_dates: dict[str, str]):
    """ArcticDB stub returning specified last_dates for stragglers."""
    lib = MagicMock()

    def fake_tail(sym, n=1):
        if sym in stragglers_with_dates:
            df = pd.DataFrame({"Close": [100.0]},
                              index=[pd.Timestamp(stragglers_with_dates[sym])])
        else:
            df = pd.DataFrame({"Close": [100.0]},
                              index=[pd.Timestamp("2026-05-01")])  # fresh
        return MagicMock(data=df)

    lib.tail.side_effect = fake_tail
    return lib


def test_universe_drift_predicts_prune_outcome():
    """The 2026-05-02 case: 8 stragglers in arctic, all stale enough to prune."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    ctx.arctic_universe_symbols = {"AAPL", "MSFT", "ASGN", "GTM", "HOLX",
                                    "KMPR", "LW", "MOH", "MTCH", "PAYC"}

    stale_dates = {
        "ASGN": "2026-04-24", "GTM": "2026-04-24", "HOLX": "2026-04-07",
        "KMPR": "2026-04-24", "LW": "2026-04-24", "MOH": "2026-04-24",
        "MTCH": "2026-04-24", "PAYC": "2026-04-24",
    }
    ctx.universe_lib = _stub_universe_lib_for_drift(stale_dates)

    result = sfp.check_universe_drift(ctx)

    # Escalated to FAIL when any straggler would be pruned. Operator must
    # drop them before launching Backtester / recovery SFs (otherwise we
    # burn a 120-min spot to re-discover them at Backtester preflight).
    assert result.status == "fail"
    assert result.details["candidates_count"] == 8
    assert result.details["would_prune_count"] == 8
    assert result.details["remediation"] is not None


def test_universe_drift_no_stragglers_passes_quietly():
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    ctx.arctic_universe_symbols = {"AAPL", "MSFT"}

    result = sfp.check_universe_drift(ctx)
    assert result.status == "ok"
    assert "No straggler candidates" in result.message


def test_universe_drift_skipped_if_context_unpopulated():
    """If constituents fetch failed upstream, this check fails loudly
    instead of misleadingly passing on partial data."""
    ctx = _ctx()
    # ctx.fresh_constituents and ctx.arctic_universe_symbols left None
    result = sfp.check_universe_drift(ctx)
    assert result.status == "fail"


# ── check_polygon_grouped_coverage (PR #131 class) ────────────────────────────


def test_polygon_grouped_coverage_ok_at_full_coverage(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {"AAPL": {}, "MSFT": {}, "GOOG": {}}
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "ok"
    assert ctx.polygon_returned_tickers == {"AAPL", "MSFT", "GOOG"}


def test_polygon_grouped_coverage_fails_below_95pct(monkeypatch):
    """The exact PR #131 scenario: polygon returns fewer-than-needed tickers."""
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    ctx = _ctx()
    ctx.fresh_constituents = {f"T{i}" for i in range(100)}
    # polygon returns only 50/100 — 50% coverage, below 95% threshold.
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {f"T{i}": {} for i in range(50)}
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "fail"
    assert "coverage" in result.message.lower()


def test_polygon_grouped_coverage_fails_on_403(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    from polygon_client import PolygonForbiddenError
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL"}
    fake_client = MagicMock()
    fake_client.get_grouped_daily.side_effect = PolygonForbiddenError("free tier same-day")
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "fail"
    assert "403" in result.message


def test_polygon_grouped_coverage_skips_when_no_api_key(monkeypatch):
    """Local-laptop preflight without POLYGON_API_KEY must skip gracefully
    (WARN, not FAIL) so the rest of the report stays actionable."""
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL"}
    result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "warn"
    assert "POLYGON_API_KEY" in result.message


# ── check_predicted_missing_from_closes (PR #132 class) ───────────────────────


def test_predicted_missing_under_threshold_passes():
    """Post-prune state: only the chronic 4 polygon-coverage tickers missing
    from constituents — under the threshold of 5."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT", "BF-B", "BRK-B", "MOG-A", "PSTG"}
    ctx.arctic_universe_symbols = ctx.fresh_constituents  # post-prune coherent
    ctx.polygon_returned_tickers = {"AAPL", "MSFT"}  # polygon misses the 4 chronic
    result = sfp.check_predicted_missing_from_closes(ctx)
    assert result.status == "ok"


def test_predicted_missing_above_threshold_fails():
    """Pre-prune state (or stragglers missed): would trip the SF hard-fail."""
    ctx = _ctx()
    ctx.fresh_constituents = {f"T{i}" for i in range(20)}
    ctx.arctic_universe_symbols = ctx.fresh_constituents
    ctx.polygon_returned_tickers = {"T0", "T1"}  # 18 missing, threshold is 5
    result = sfp.check_predicted_missing_from_closes(ctx)
    assert result.status == "fail"
    assert "would halt" in result.message.lower()


def test_predicted_missing_excludes_stragglers_correctly():
    """The PR #134 + PR #132 intersection: stragglers in arctic but not in
    fresh constituents must be excluded from the 'expected' set so they
    don't inflate the missing count post-prune."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    # Arctic still has stragglers (pre-prune state).
    ctx.arctic_universe_symbols = {"AAPL", "MSFT", "STRAGGLER1", "STRAGGLER2"}
    ctx.polygon_returned_tickers = {"AAPL", "MSFT"}
    result = sfp.check_predicted_missing_from_closes(ctx)
    # Post-prune (arctic ∩ constituents) = {AAPL, MSFT}; closes covers both.
    assert result.status == "ok"


# ── check_backfill_source_freshness (PR #130 class) ───────────────────────────


def _bytes_for_parquet(last_date_str: str, has_spy: bool = True) -> bytes:
    import io
    df = pd.DataFrame(
        {"Close": [100.0]},
        index=pd.DatetimeIndex([pd.Timestamp(last_date_str)]),
    )
    if has_spy:
        df.index = pd.Index(["SPY"])  # daily_closes uses ticker as index
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    return buf.getvalue()


def _stub_macro_lib(spy_last_date: str):
    lib = MagicMock()
    lib.tail.return_value = MagicMock(
        data=pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp(spy_last_date)])
    )
    return lib


def test_backfill_source_freshness_passes_when_delta_covers_arctic():
    """Happy path: ArcticDB SPY at 2026-04-30, daily_closes has 2026-05-01,
    backfill source ≥ arctic → no regression predicted."""
    ctx = _ctx()
    ctx.macro_lib = _stub_macro_lib("2026-04-30")

    import io
    cache_df = pd.DataFrame({"Close": [100.0]},
                            index=[pd.Timestamp("2026-04-30")])
    cache_buf = io.BytesIO()
    cache_df.to_parquet(cache_buf, engine="pyarrow")

    delta_df = pd.DataFrame({"Close": [100.0]}, index=pd.Index(["SPY"]))
    delta_buf = io.BytesIO()
    delta_df.to_parquet(delta_buf, engine="pyarrow")

    fake_s3 = MagicMock()
    def fake_get(**kw):
        body = MagicMock()
        if "price_cache" in kw["Key"]:
            body.read.return_value = cache_buf.getvalue()
        else:
            body.read.return_value = delta_buf.getvalue()
        return {"Body": body}
    fake_s3.get_object.side_effect = fake_get

    with patch("boto3.client", return_value=fake_s3):
        result = sfp.check_backfill_source_freshness(ctx)
    assert result.status == "ok"


def test_backfill_source_freshness_fails_when_source_regresses():
    """The PR #130 scenario: ArcticDB has 5/1 (from MorningEnrich earlier),
    but cache is only 4/30 and no daily_closes delta exists → backfill
    would clobber 5/1 → regression."""
    ctx = _ctx()
    ctx.macro_lib = _stub_macro_lib("2026-05-01")  # arctic ahead

    import io
    cache_df = pd.DataFrame({"Close": [100.0]},
                            index=[pd.Timestamp("2026-04-30")])
    cache_buf = io.BytesIO()
    cache_df.to_parquet(cache_buf, engine="pyarrow")

    fake_s3 = MagicMock()
    def fake_get(**kw):
        if "price_cache" in kw["Key"]:
            body = MagicMock()
            body.read.return_value = cache_buf.getvalue()
            return {"Body": body}
        raise Exception("NoSuchKey")
    fake_s3.get_object.side_effect = fake_get

    with patch("boto3.client", return_value=fake_s3):
        result = sfp.check_backfill_source_freshness(ctx)
    assert result.status == "fail"
    assert "regression" in result.message.lower()


# ── check_tool_contracts (I4494 Leg 2) ───────────────────────────────────────


def test_tool_contracts_parse_checkout_repo():
    """Extract the checkout name from a commands.$ path."""
    parts = "/home/ec2-user/alpha-engine-dashboard/.venv/bin/python -m krepis.ssm_log_capture run --correlation-id abc".split()
    checkout = sfp._parse_checkout_repo(parts)
    assert checkout == "alpha-engine-dashboard"

    # Non-standard path — no .venv pattern
    parts2 = "/usr/bin/python3 -m something".split()
    assert sfp._parse_checkout_repo(parts2) is None


def test_tool_contracts_resolve_governing_repo():
    assert sfp._resolve_governing_repo("alpha-engine-dashboard") == "alpha-engine-dashboard"
    assert sfp._resolve_governing_repo("alpha-engine-research") == "alpha-engine-research"
    # Unknown checkout
    assert sfp._resolve_governing_repo("unknown-repo") is None


def test_tool_contracts_version_parsing():
    assert sfp._parse_version("0.18.8") == (0, 18, 8)
    assert sfp._parse_version("0.18.0") == (0, 18, 0)
    assert sfp._parse_version("1.0.0rc1") == (1, 0, 0)


def test_tool_contracts_version_comparison():
    assert sfp._check_version_meets("0.18.8", "0.18.8") is True
    assert sfp._check_version_meets("0.19.0", "0.18.8") is True
    assert sfp._check_version_meets("0.18.0", "0.18.8") is False
    assert sfp._check_version_meets("1.0.0", "0.18.8") is True


def test_tool_contracts_read_pinned_version():
    import tempfile
    req = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    req.write("krepis==0.18.8\nboto3>=1.28\n")
    req.close()
    from pathlib import Path
    pinned = sfp._read_pinned_version(Path(req.name))
    assert pinned == "0.18.8"


def test_tool_contracts_read_pinned_version_none():
    import tempfile
    req = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    req.write("boto3>=1.28\n")
    req.close()
    from pathlib import Path
    pinned = sfp._read_pinned_version(Path(req.name))
    assert pinned is None


def test_tool_contracts_skips_non_venv_commands():
    """A definition whose commands.$ match no `.venv/bin/python` is NOT a
    pass — alpha-engine-config-I11112 reversed this expectation.

    This test previously asserted `status == "ok"` with `checked == 0`, and
    that assertion was the defect written down: measured against the LIVE
    definition, every real `commands.$` is a `States.Array(...)` intrinsic
    that the old parser also failed to match, so the check returned
    `ok / 0 command(s) checked` on every environment forever. A check that
    examined nothing now fails (principles.md §2.7).

    The graceful-skip property this test was protecting is still real and
    still tested — it lives one level down, in `scan_command_checkouts`
    returning `[]` for an unrecognised command
    (`tests/test_sf_tool_contract_commands.py::
    test_scanner_handles_both_definition_shapes`). What changed is what the
    CHECK does with an all-empty result.
    """
    ctx = _ctx()
    from unittest.mock import patch, MagicMock

    fake_sfn = MagicMock()
    fake_sfn.describe_state_machine.return_value = {
        "definition": '{"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "B"}, "B": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"commands.$": "$.shell_cmd"}}}}'
    }
    with patch("boto3.client", return_value=fake_sfn):
        result = sfp.check_tool_contracts(ctx)
    # commands.$ exists but has no .venv pattern — nothing was examined, so
    # the check must say so rather than report a pass over an empty set.
    assert result.status == "fail"
    assert result.details.get("checked", 0) == 0
    assert "ZERO" in result.message


# ── check_definition_input_coherence (I4494 Leg 3) ─────────────────────────


def test_collect_jsonpath_refs():
    sf_def = {
        "StartAt": "A",
        "States": {
            "A": {
                "Type": "Pass",
                "comment.$": "$.comment_text",
                "Next": "B",
            },
            "B": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "my-func",
                    "Payload": {
                        "date.$": "$.run_date",
                        "id.$": "$.execution_id",
                    }
                },
                "Next": "C",
            }
        }
    }
    refs = sfp._collect_jsonpath_refs(sf_def)
    ref_strings = {r[0] for r in refs}
    assert "$.comment_text" in ref_strings
    assert "$.run_date" in ref_strings
    assert "$.execution_id" in ref_strings


def test_build_proposed_input_has_all_skip_flags():
    inp = sfp._build_proposed_input()
    skip_flags = {k for k in inp if k.startswith("skip_")}
    assert "skip_weekly_run_day_gate" in skip_flags
    assert "skip_lib_pin_drift_check" in skip_flags
    assert "skip_morning_enrich" in skip_flags
    assert "skip_data_phase1" in skip_flags
    assert "skip_scanner" in skip_flags
    assert "skip_research" in skip_flags
    assert "skip_eval_judge" in skip_flags
    assert "skip_predictor_training" in skip_flags
    assert "skip_parity" in skip_flags


def test_build_proposed_input_with_override():
    inp = sfp._build_proposed_input({"skip_morning_enrich": True})
    assert inp["skip_morning_enrich"] is True
    assert inp["skip_data_phase1"] is False  # default


def test_definition_input_coherence_checks_refs():
    """Test with a minimal SF definition that the check resolves JSONPaths."""
    import json
    ctx = _ctx()
    from unittest.mock import patch, MagicMock

    minimal_sf = {
        "StartAt": "CheckInput",
        "States": {
            "CheckInput": {
                "Type": "Pass",
                "comment.$": "$.pipeline_role",
                "Next": "Proceed",
            },
            "Proceed": {
                "Type": "Pass",
                "comment.$": "$.run_date",
                "End": True,
            }
        }
    }
    fake_sfn = MagicMock()
    fake_sfn.describe_state_machine.return_value = {
        "definition": json.dumps(minimal_sf)
    }
    with patch("boto3.client", return_value=fake_sfn):
        result = sfp.check_definition_input_coherence(ctx)
    # Both $.pipeline_role and $.run_date exist in the proposed input
    assert result.status == "ok"
    assert result.details["checked"] >= 2


def test_definition_input_coherence_fails_on_unresolvable_path():
    """A JSONPath referencing a non-existent field must be flagged."""
    import json
    ctx = _ctx()
    from unittest.mock import patch, MagicMock

    bad_sf = {
        "StartAt": "Bad",
        "States": {
            "Bad": {
                "Type": "Pass",
                "comment.$": "$.nonexistent_field_xyz",
                "End": True,
            }
        }
    }
    fake_sfn = MagicMock()
    fake_sfn.describe_state_machine.return_value = {
        "definition": json.dumps(bad_sf)
    }
    with patch("boto3.client", return_value=fake_sfn):
        result = sfp.check_definition_input_coherence(ctx)
    assert result.status == "fail"
    assert "unresolvable" in result.message.lower()


# ── check_lambda_memory_headroom (I4494 Leg 4) ──────────────────────────────


def test_walk_invoked_lambdas():
    """_walk_invoked_lambdas collects FunctionName at any nesting."""
    sf_def = {
        "States": {
            "A": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "my-func:live"},
                "Next": "B",
            },
            "B": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "C",
                        "States": {
                            "C": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::lambda:invoke",
                                "Parameters": {"FunctionName": "other-func:prod"},
                                "End": True,
                            }
                        }
                    }
                ],
                "Next": "D",
            }
        }
    }
    found = sfp._walk_invoked_lambdas(sf_def)
    assert "my-func:live" in found
    assert "other-func:prod" in found


def test_walk_invoked_lambdas_skips_non_lambda():
    """ARNs that are not lambda:invoke resources should not be collected
    by _walk_invoked_lambdas — it collects every FunctionName value it sees."""
    sf_def = {
        "States": {
            "A": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "FunctionName": "not-a-lambda",  # FunctionName key but SNS resource
                    "Message": "hello",
                },
                "End": True,
            }
        }
    }
    found = sfp._walk_invoked_lambdas(sf_def)
    assert "not-a-lambda" in found  # _walk_invoked_lambdas collects any FunctionName


# ── Orchestrator ──────────────────────────────────────────────────────────────


def test_run_preflight_isolates_check_failures():
    """A single check raising must NOT abort the suite — we want the full
    picture. Forces one check to raise; asserts the others still ran."""
    def raising_check(ctx):
        raise RuntimeError("boom")

    raising_check.__name__ = "check_test_raise"

    with patch.object(sfp, "CHECKS", [raising_check, sfp.check_arctic_connectivity]), \
         patch("arcticdb.Arctic", side_effect=Exception("arctic stub")):
        n_fail, results = sfp.run_preflight(bucket="test-bucket")

    assert len(results) == 2  # both ran; first wrapped to fail, second ran
    assert results[0].status == "fail"
    assert "boom" in results[0].message


def test_run_preflight_returns_failure_count():
    def fail_check(ctx):
        return sfp.CheckResult(name="x", status="fail", message="nope")
    fail_check.__name__ = "check_x"

    def ok_check(ctx):
        return sfp.CheckResult(name="y", status="ok", message="fine")
    ok_check.__name__ = "check_y"

    with patch.object(sfp, "CHECKS", [fail_check, ok_check, fail_check]):
        n_fail, results = sfp.run_preflight(bucket="test-bucket")
    assert n_fail == 2
    assert len(results) == 3


# ── Research-side static checks (PR #77, #78 prevention) ──────────────────────


import tempfile
from pathlib import Path


def _make_sibling_repos(tmp_path: Path, *, pricing_yaml: str,
                        universe_yaml: str | None = None,
                        research_graph_src: str | None = None,
                        quant_analyst_src: str | None = None,
                        qual_analyst_src: str | None = None) -> Path:
    """Build a tmp sibling-clone directory layout for the static checks
    to walk. Returns the path that should be passed as the 'parent' dir
    (i.e. tmp_path / 'siblings' simulates ~/Development).

    Each yaml/source param is optional — pass None to omit the file
    entirely (e.g. test the missing-file branch)."""
    siblings = tmp_path / "siblings"
    config = siblings / "alpha-engine-config"
    research = siblings / "alpha-engine-research"
    (config / "cost").mkdir(parents=True)
    (config / "research").mkdir(parents=True)
    (research / "agents" / "sector_teams").mkdir(parents=True)
    (research / "graph").mkdir(parents=True)
    # alpha-engine-data placeholder so _sibling_repo's parent-resolution
    # has the right layout (sibling lookup is from this file's parent).
    (siblings / "alpha-engine-data").mkdir()

    (config / "cost" / "model_pricing.yaml").write_text(pricing_yaml)
    if universe_yaml is not None:
        (config / "research" / "universe.yaml").write_text(universe_yaml)
    if research_graph_src is not None:
        (research / "graph" / "research_graph.py").write_text(research_graph_src)
    if quant_analyst_src is not None:
        (research / "agents" / "sector_teams" / "quant_analyst.py").write_text(quant_analyst_src)
    if qual_analyst_src is not None:
        (research / "agents" / "sector_teams" / "qual_analyst.py").write_text(qual_analyst_src)

    return siblings


@pytest.fixture
def patched_sibling(monkeypatch, tmp_path):
    """Returns a callable that builds a tmp sibling layout + monkeypatches
    sf_preflight._sibling_repo to resolve into it. Tests build the layout
    they need then call the check."""
    def _build(**kwargs) -> Path:
        siblings = _make_sibling_repos(tmp_path, **kwargs)
        def _fake_sibling(name: str):
            candidate = siblings / name
            return candidate if candidate.is_dir() else None
        monkeypatch.setattr(sfp, "_sibling_repo", _fake_sibling)
        return siblings
    return _build


# ── check_price_cards_cover_all_models ─────────────────────────────────────────


class _FakeRegistry:
    """Just the surface ``krepis.cost.live_group_primaries`` reads."""

    def __init__(self, groups, models):
        self.groups = groups
        self.models = models

    def live_group_ids(self, group):
        return list(self.groups.get(group, []))


@pytest.fixture
def price_rule_registry(monkeypatch, tmp_path):
    """Lay out an alpha-engine-config sibling carrying a registry file, and
    make krepis' loader return the given fake for it."""
    def _build(registry):
        config = tmp_path / "alpha-engine-config"
        (config / "private-docs").mkdir(parents=True)
        (config / "private-docs" / "LLM_MODEL_REGISTRY.yaml").write_text("{}\n")
        monkeypatch.setattr(
            sfp, "_sibling_repo",
            lambda name: config if name == "alpha-engine-config" else None,
        )
        import krepis.model_registry as _mr
        monkeypatch.setattr(_mr, "load_registry", lambda path=None: registry)
    return _build


def test_price_cards_check_passes_when_every_live_primary_is_priced(price_rule_registry):
    """Reads krepis' own packaged table: ``claude-haiku-4-5`` has a card."""
    price_rule_registry(_FakeRegistry(
        groups={"low": ["haiku"]}, models={"haiku": {"model": "claude-haiku-4-5"}},
    ))
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "ok", result.message


def test_price_cards_check_fails_on_an_unpriced_live_primary(price_rule_registry):
    """The alpha-engine-config-I11100 shape: a live primary with no card."""
    price_rule_registry(_FakeRegistry(
        groups={"ultra": ["new-model"]},
        models={"new-model": {"model": "deliberately-unpriced-model-xyz"}},
    ))
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "fail"
    assert "deliberately-unpriced-model-xyz" in str(result.details)


def test_price_cards_check_skips_chaos_probe_primaries(price_rule_registry):
    """A chaos-probe group never bills, so it is never graded (the shared
    rule's own exclusion, not a copy of it)."""
    price_rule_registry(_FakeRegistry(
        groups={"chaos": ["broken"]},
        models={"broken": {"model": "unpriced-on-purpose", "chaos_probe": True}},
    ))
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "ok"


def test_price_cards_check_fails_when_the_registry_file_is_missing(monkeypatch, tmp_path):
    (tmp_path / "alpha-engine-config").mkdir()
    monkeypatch.setattr(sfp, "_sibling_repo", lambda name: tmp_path / name)
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "fail"
    assert "LLM_MODEL_REGISTRY.yaml" in result.message


def test_price_cards_check_warns_when_sibling_repo_absent(monkeypatch):
    monkeypatch.setattr(sfp, "_sibling_repo", lambda name: None)
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "warn"


# ── check_recursion_budget_for_response_format ────────────────────────────────


def test_recursion_budget_check_passes_when_buffered(patched_sibling):
    """Happy path: ReAct site uses response_format AND has +2 buffer in
    recursion_limit. Mirrors today's PR #78 fix."""
    patched_sibling(
        pricing_yaml="cards: []\n",
        quant_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUANT_MAX_ITERATIONS = 8\n"
            "_QUANT_RECURSION_LIMIT = QUANT_MAX_ITERATIONS * 2 + 2\n"
            "agent = create_react_agent(model, tools, response_format=Output)\n"
            "agent.invoke({}, config={'recursion_limit': _QUANT_RECURSION_LIMIT})\n"
        ),
        qual_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUAL_MAX_ITERATIONS = 8\n"
            "_QUAL_RECURSION_LIMIT = QUAL_MAX_ITERATIONS * 2 + 2\n"
            "agent = create_react_agent(model, tools, response_format=Output)\n"
            "agent.invoke({}, config={'recursion_limit': _QUAL_RECURSION_LIMIT})\n"
        ),
    )
    result = sfp.check_recursion_budget_for_response_format(_ctx())
    assert result.status == "ok"


def test_recursion_budget_check_fails_on_bare_x2(patched_sibling):
    """The 2026-05-02 PR #78 regression: ReAct uses response_format= but
    recursion_limit is bare ``MAX_ITERATIONS * 2`` (no +N buffer). SF
    crashes on the structured-extraction call. SHOULD be caught here."""
    patched_sibling(
        pricing_yaml="cards: []\n",
        quant_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUANT_MAX_ITERATIONS = 8\n"
            "agent = create_react_agent(model, tools, response_format=Output)\n"
            "agent.invoke({}, config={'recursion_limit': QUANT_MAX_ITERATIONS * 2})\n"
        ),
        qual_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUAL_MAX_ITERATIONS = 8\n"
            "_QUAL_RECURSION_LIMIT = QUAL_MAX_ITERATIONS * 2 + 2\n"
            "agent = create_react_agent(model, tools, response_format=Output)\n"
            "agent.invoke({}, config={'recursion_limit': _QUAL_RECURSION_LIMIT})\n"
        ),
    )
    result = sfp.check_recursion_budget_for_response_format(_ctx())
    assert result.status == "fail"
    assert "quant_analyst" in str(result.details)


def test_recursion_budget_check_skips_files_without_response_format(patched_sibling):
    """Files that don't use response_format= aren't subject to the +2 rule."""
    patched_sibling(
        pricing_yaml="cards: []\n",
        quant_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUANT_MAX_ITERATIONS = 8\n"
            "agent = create_react_agent(model, tools)\n"  # no response_format
            "agent.invoke({}, config={'recursion_limit': QUANT_MAX_ITERATIONS * 2})\n"
        ),
        qual_analyst_src=(
            "from langgraph.prebuilt import create_react_agent\n"
            "QUAL_MAX_ITERATIONS = 8\n"
            "agent = create_react_agent(model, tools)\n"  # no response_format
            "agent.invoke({}, config={'recursion_limit': QUAL_MAX_ITERATIONS * 2})\n"
        ),
    )
    result = sfp.check_recursion_budget_for_response_format(_ctx())
    assert result.status == "ok"
    assert all("no response_format" in c for c in result.details["checked"])


def test_recursion_budget_check_warns_when_sibling_absent(monkeypatch):
    monkeypatch.setattr(sfp, "_sibling_repo", lambda name: None)
    result = sfp.check_recursion_budget_for_response_format(_ctx())
    assert result.status == "warn"


# ── check_sf_iam_reachability (alpha-engine-config-I4494) ─────────────────────
#
# Bug class: an identity the weekly SF depends on cannot reach what it targets,
# and nothing notices until a stage fails mid-run. Three live instances on
# 2026-07-27: the evaluator role's unapplied s3 grant, the SF role's missing
# invoke on the new dispatcher, and substrate-health-gate's instance-scoped
# SendCommand against a per-execution box.


@pytest.mark.parametrize(
    "function_name,expected",
    [
        ("alpha-engine-scanner", "alpha-engine-scanner"),
        ("alpha-engine-scanner:live", "alpha-engine-scanner"),
        (
            "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-substrate-health-gate",
            "alpha-engine-substrate-health-gate",
        ),
        (
            "arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-evaluator:live",
            "alpha-engine-evaluator",
        ),
    ],
)
def test_lambda_base_name_handles_bare_names_and_arns(function_name, expected):
    """Regression: the ARN form must not collapse to "arn".

    Caught while validating the check against live AWS — the first run reported
    a false denial for a role that held the grant, because splitting the ARN on
    ":" yielded "arn" and simulated a function that does not exist. A preflight
    that cries wolf gets ignored, so this is a correctness requirement, not a
    cosmetic one.
    """
    assert sfp._lambda_base_name(function_name) == expected


def test_walk_invoked_lambdas_finds_nested_states():
    """FunctionName is collected from Parallel branches and Map iterators too."""
    definition = {
        "States": {
            "Top": {"Parameters": {"FunctionName": "fn-top"}},
            "Par": {
                "Type": "Parallel",
                "Branches": [{"States": {"B": {"Parameters": {"FunctionName": "fn-branch"}}}}],
            },
            "Map": {
                "Type": "Map",
                "ItemProcessor": {
                    "States": {"M": {"Parameters": {"FunctionName": "fn-map"}}}
                },
            },
        }
    }
    assert sfp._walk_invoked_lambdas(definition) == {"fn-top", "fn-branch", "fn-map"}


def test_simulate_treats_an_error_as_not_allowed():
    """"Could not check" must never read as "allowed".

    Conflating the two is the silent-degradation pattern the weekly-SF policy
    forbids: a gate that cannot evaluate has not passed.
    """
    iam = MagicMock()
    iam.simulate_principal_policy.side_effect = RuntimeError("throttled")
    assert sfp._simulate(iam, "arn:role", "lambda:InvokeFunction", "arn:fn") is False


def test_simulate_requires_every_result_allowed():
    iam = MagicMock()
    iam.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalDecision": "allowed"},
            {"EvalDecision": "implicitDeny"},
        ]
    }
    assert sfp._simulate(iam, "arn:role", "ssm:SendCommand", "arn:inst") is False


# ── Environment capability profile (the 2026-08-10 WeeklyPreflight outage) ────
#
# ne-weekly-freshness-pipeline halted on its first real WeeklyPreflightGate
# invocation with ``ModuleNotFoundError: No module named 'nousergon_lib'``,
# raised out of run_preflight's PROLOGUE (_previous_trading_day_str) — before
# the per-check try/except exists, so the whole gate returned status=ERROR.
# Two defects behind it, both pinned here:
#   1. weekly-preflight/requirements.txt did not declare nousergon-lib even
#      though the packaged sf_preflight.py imports it in five places;
#   2. the Lambda ran the FULL check list, which includes checks that require
#      arcticdb, the repo's collector modules and sibling checkouts on local
#      disk — none of which a Lambda has. Those return status="fail", so the
#      gate could not have returned OK even with (1) fixed.

import ast as _ast
import pathlib as _pathlib
import sys as _sys

_REPO_ROOT = _pathlib.Path(sfp.__file__).resolve().parent
_LAMBDA_DIR = _REPO_ROOT / "infrastructure" / "lambdas" / "weekly-preflight"

# Modules the python3.12 Lambda runtime provides without a requirements entry.
_LAMBDA_RUNTIME_MODULES = {"boto3", "botocore", "jmespath", "dateutil", "urllib3", "s3transfer"}
# import name -> distribution name, for the specs weekly-preflight declares.
_DIST_FOR_MODULE = {
    "nousergon_lib": "nousergon-lib",
    "aws_assume_role_lib": "aws-assume-role-lib",
}


def _fn_node(name: str) -> _ast.FunctionDef:
    tree = _ast.parse(_REPO_ROOT.joinpath("sf_preflight.py").read_text())
    for node in tree.body:
        if isinstance(node, _ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in sf_preflight.py")


def _imports_of(node: _ast.AST) -> set[str]:
    found: set[str] = set()
    for sub in _ast.walk(node):
        if isinstance(sub, _ast.Import):
            found.update(a.name.split(".")[0] for a in sub.names)
        elif isinstance(sub, _ast.ImportFrom) and sub.module and sub.level == 0:
            found.add(sub.module.split(".")[0])
    return found


def _declared_distributions() -> set[str]:
    out: set[str] = set()
    for line in _LAMBDA_DIR.joinpath("requirements.txt").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # "nousergon-lib[extra] @ git+https://..." / "pkg==1.2" / "pkg"
        out.add(line.split("@")[0].split("[")[0].split("=")[0].split(">")[0].strip())
    return out


def test_every_check_declares_its_capabilities():
    """A new check may not silently inherit or lose Lambda eligibility."""
    listed = {fn.__name__ for fn in sfp.CHECKS}
    declared = set(sfp.CHECK_CAPABILITIES)
    assert listed == declared, (
        "CHECKS and CHECK_CAPABILITIES disagree; add the missing entry "
        f"(only in CHECKS: {sorted(listed - declared)}; "
        f"only in CHECK_CAPABILITIES: {sorted(declared - listed)})"
    )
    known = sfp.FULL_CAPABILITIES
    for name, caps in sfp.CHECK_CAPABILITIES.items():
        assert caps, f"{name}: declare at least one capability"
        assert caps <= known, f"{name}: unknown capability {sorted(caps - known)}"


def test_every_check_declares_required():
    """alpha-engine-config-I11112: a new check may not silently be exempt
    from the required-skip accounting — mirrors
    test_every_check_declares_its_capabilities for CHECK_REQUIRED."""
    listed = {fn.__name__ for fn in sfp.CHECKS}
    declared = set(sfp.CHECK_REQUIRED)
    assert listed == declared, (
        "CHECKS and CHECK_REQUIRED disagree; add the missing entry "
        f"(only in CHECKS: {sorted(listed - declared)}; "
        f"only in CHECK_REQUIRED: {sorted(declared - listed)})"
    )
    for name, required in sfp.CHECK_REQUIRED.items():
        assert isinstance(required, bool), f"{name}: CHECK_REQUIRED value must be a bool"


def test_summarize_results_flags_only_required_skips():
    """The function I11112 pulls the Lambda handler's counting logic into —
    only a skip of a REQUIRED check inflates required_skip_count."""
    results = [
        sfp.CheckResult(name="sf_iam_reachability", status="ok", message=""),
        sfp.CheckResult(name="arctic_connectivity", status="skip", message="Not run: ... arctic"),
        sfp.CheckResult(name="tool_contracts", status="skip", message="Not run: ... checkout"),
    ]
    summary = sfp.summarize_results(results)
    assert summary["ran_count"] == 1
    assert summary["skip_count"] == 2
    # Both check_arctic_connectivity and check_tool_contracts are declared
    # required=True in CHECK_REQUIRED today.
    assert summary["required_skip_count"] == 2
    assert sorted(summary["required_skip_names"]) == ["arctic_connectivity", "tool_contracts"]


def test_summarize_results_undeclared_check_defaults_required():
    """An undeclared check name (not in CHECK_REQUIRED) defaults to
    required=True — the safe direction, mirroring CHECK_CAPABILITIES'
    undeclared-defaults-to-FULL_CAPABILITIES convention."""
    results = [sfp.CheckResult(name="brand_new_check", status="skip", message="")]
    summary = sfp.summarize_results(results)
    assert summary["required_skip_count"] == 1
    assert summary["required_skip_names"] == ["brand_new_check"]


def test_lambda_profile_runs_at_least_one_check():
    """A gate observing nothing is not a gate (principles.md §2.7)."""
    eligible = [
        fn for fn in sfp.CHECKS
        if sfp.CHECK_CAPABILITIES[fn.__name__] <= sfp.LAMBDA_CAPABILITIES
    ]
    assert eligible, "no check is runnable under LAMBDA_CAPABILITIES"


def test_lambda_profile_imports_are_packaged():
    """Every module the Lambda-eligible code path imports must be in the zip.

    This is the test that would have caught the 2026-08-10 outage: the
    prologue's nousergon_lib import against a requirements.txt that never
    declared it. Scans the prologue plus every Lambda-eligible check.
    """
    packaged = _declared_distributions() | _LAMBDA_RUNTIME_MODULES
    scanned = ["_previous_trading_day_str", "run_preflight"] + [
        fn.__name__ for fn in sfp.CHECKS
        if sfp.CHECK_CAPABILITIES[fn.__name__] <= sfp.LAMBDA_CAPABILITIES
    ]
    for fn_name in scanned:
        for mod in _imports_of(_fn_node(fn_name)):
            if mod in _sys.stdlib_module_names or mod == "sf_preflight":
                continue
            dist = _DIST_FOR_MODULE.get(mod, mod)
            assert dist in packaged or mod in packaged, (
                f"{fn_name} imports {mod!r}, which weekly-preflight's "
                f"requirements.txt does not provide — the Lambda will raise "
                f"ModuleNotFoundError at run time, and the gate will halt the "
                f"Saturday pipeline"
            )


def test_lambda_profile_checks_need_no_local_checkout():
    """_sibling_repo returns None in a Lambda — the check then FAILS, not skips."""
    for fn in sfp.CHECKS:
        if sfp.CHECK_CAPABILITIES[fn.__name__] <= sfp.LAMBDA_CAPABILITIES:
            src = _ast.dump(_fn_node(fn.__name__))
            assert "_sibling_repo" not in src, (
                f"{fn.__name__} reads a sibling checkout but is declared "
                f"Lambda-eligible; add CAP_CHECKOUT to its capabilities"
            )


def test_run_preflight_skips_rather_than_fails_on_missing_capability():
    n_fail, results = sfp.run_preflight(
        bucket="test-bucket", capabilities=frozenset()
    )
    assert n_fail == 0, "an absent capability is not a violation"
    assert results and all(r.status == "skip" for r in results)
    assert all("Not run" in r.message for r in results)


def test_run_preflight_defaults_to_the_full_profile():
    """The CLI and the spot box must keep running every check."""
    def check_sf_iam_reachability(ctx):  # name matters: CHECK_CAPABILITIES key
        return sfp.CheckResult(name="sf_iam_reachability", status="ok", message="ran")

    def check_tool_contracts(ctx):  # requires CAP_CHECKOUT — absent in a Lambda
        return sfp.CheckResult(name="tool_contracts", status="ok", message="ran")

    with patch.object(sfp, "CHECKS", [check_sf_iam_reachability, check_tool_contracts]):
        n_fail, results = sfp.run_preflight(bucket="test-bucket")
        assert n_fail == 0
        assert [r.status for r in results] == ["ok", "ok"]

        _, lambda_results = sfp.run_preflight(
            bucket="test-bucket", capabilities=sfp.LAMBDA_CAPABILITIES
        )
    assert [r.status for r in lambda_results] == ["ok", "skip"]


def test_definition_input_coherence_ignores_mid_flow_refs():
    """A ref to a value a PRIOR STATE produces is not an input violation.

    The 2026-08-10 false-positive class: resolving every ``.$`` ref against
    the execution input reported 177 unresolvable paths on a live, working
    definition ($.libpin_drift_result.Payload, $.Status, $.ec2_instance_id[0]),
    which would have failed the pre-spend gate every Saturday forever.
    """
    import json
    from unittest.mock import patch, MagicMock

    sf_def = {
        "StartAt": "Launch",
        "States": {
            "Launch": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {"FunctionName": "launcher"},
                "ResultPath": "$.launch_result",
                "Next": "Wrap",
            },
            # Writes ec2_instance_id inside an intrinsic format string —
            # invisible to any key walk (WrapEc2InstanceIdInArray's shape).
            "Wrap": {
                "Type": "Pass",
                "Parameters": {
                    "merged.$": "States.JsonMerge($, States.StringToJson("
                                "States.Format('{\"ec2_instance_id\":[\"{}\"]}', "
                                "$.launch_result.instance_id)), false)"
                },
                "OutputPath": "$.merged",
                "Next": "Poll",
            },
            "Poll": {  # output-replacing Task: emits the raw SSM response
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ssm:getCommandInvocation",
                "Parameters": {"InstanceIds.$": "$.ec2_instance_id[0]"},
                "Next": "Done",
            },
            "Done": {
                "Type": "Pass",
                "Parameters": {
                    "status.$": "$.Status",                     # SSM response field
                    "payload.$": "$.launch_result.Payload",     # prior state's result
                    "instance.$": "$.ec2_instance_id[0]",       # intrinsic-written
                    "role.$": "$.pipeline_role",                # execution input
                },
                "End": True,
            },
        },
    }
    fake_sfn = MagicMock()
    fake_sfn.describe_state_machine.return_value = {"definition": json.dumps(sf_def)}
    with patch("boto3.client", return_value=fake_sfn):
        result = sfp.check_definition_input_coherence(_ctx())
    assert result.status == "ok", result.details
    assert result.details["undecidable_refs"] >= 3
    assert result.details["checked"] >= 1  # $.pipeline_role still verified


# ── check_skip_flag_artifact_coherence (alpha-engine-config-I7443) ────────────
#
# The live shape these reproduce: on 2026-08-15's cycle, two recovery
# executions carried `skip_predictor_training: true` with `run_date:
# 2026-08-16` while the weights manifest was last written 2026-08-15. Both
# died on the in-SF `PredictorSkipWeightsStale` guard — correct, but only
# after a spot dispatch and ~18 minutes, twice. This check asserts the same
# predicate at the pre-spend gate (sf-pipeline-policy §2.2).


from datetime import datetime as _dt, timezone as _tz  # noqa: E402


class _NotFound(Exception):
    def __init__(self):
        super().__init__("Not Found")
        self.response = {"Error": {"Code": "404"}}


def _skip_ctx(run_date="2026-08-16", **flags) -> sfp.PreflightContext:
    return sfp.PreflightContext(
        bucket="alpha-engine-research",
        today="2026-08-16",
        prior_trading_day="2026-08-14",
        run_date=run_date,
        skip_flags=dict(flags),
    )


def _s3_with_last_modified(day: str) -> MagicMock:
    s3 = MagicMock()
    s3.head_object.return_value = {
        "LastModified": _dt.fromisoformat(f"{day}T12:00:00+00:00").astimezone(_tz.utc)
    }
    return s3


def test_skip_coherence_fails_when_artifact_predates_run_date():
    """THE regression: manifest dated 2026-08-15, run_date 2026-08-16."""
    ctx = _skip_ctx(run_date="2026-08-16", skip_predictor_training=True)
    with patch("boto3.client", return_value=_s3_with_last_modified("2026-08-15")):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "fail"
    assert "2026-08-15 < calendar_date 2026-08-16" in res.details["violations"][0]
    assert "CheckPredictorSkipWeightsFresh" in res.details["violations"][0]


def test_skip_coherence_passes_when_artifact_is_current():
    ctx = _skip_ctx(run_date="2026-08-15", skip_predictor_training=True)
    with patch("boto3.client", return_value=_s3_with_last_modified("2026-08-15")):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "ok"
    assert res.details["claims_checked"] == 1


def test_skip_coherence_fails_when_the_artifact_does_not_exist():
    ctx = _skip_ctx(skip_predictor_training=True)
    s3 = MagicMock()
    s3.head_object.side_effect = _NotFound()
    with patch("boto3.client", return_value=s3):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "fail"
    assert "does not exist" in res.details["violations"][0]


def test_skip_coherence_unreadable_artifact_is_unknown_not_pass():
    """§2.3a: a verdict that cannot be read is UNKNOWN, never a pass. An S3
    error other than 404 must fail loudly rather than waving the claim
    through — the same rule that made the watchdog's UNREADABLE page."""
    ctx = _skip_ctx(skip_predictor_training=True)
    s3 = MagicMock()
    s3.head_object.side_effect = RuntimeError("s3 5xx")
    with patch("boto3.client", return_value=s3):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "fail"
    assert "unreadable" in res.details["violations"][0]


def test_skip_coherence_rejects_a_non_iso_last_modified_instead_of_wrong_passing():
    """The SF's StringMatches '20*-*-*' shape guard, replicated. A non-ISO
    serialization compared lexicographically could SILENTLY wrong-pass; it
    must become the loud path instead."""
    ctx = _skip_ctx(skip_predictor_training=True)
    s3 = MagicMock()
    s3.head_object.return_value = {"LastModified": "Sat, 15 Aug 2026 12:00:00 GMT"}
    with patch("boto3.client", return_value=s3):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "fail"
    assert "not YYYY-MM-DD" in res.details["violations"][0]


def test_skip_coherence_is_silent_when_the_flag_is_not_claimed():
    """An unclaimed skip must cost zero S3 calls — this runs on every
    weekly execution, including the clean Saturday one."""
    ctx = _skip_ctx(skip_scanner=True)  # registered flags not among them
    s3 = MagicMock()
    with patch("boto3.client", return_value=s3):
        res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "ok"
    assert res.details["claims_checked"] == 0
    s3.head_object.assert_not_called()


def test_skip_coherence_without_execution_input_is_ok_not_fail():
    """The CLI and the spot box call run_preflight without the SF input.
    That is 'nothing observed', not 'violation' — this gate must not start
    halting the pipeline for callers that never passed a payload."""
    ctx = _ctx()  # no run_date, no skip_flags
    res = sfp.check_skip_flag_artifact_coherence(ctx)
    assert res.status == "ok"
    assert res.details["claims_checked"] == 0


def test_skip_predicate_matches_the_sf_definitions_own_guard():
    """Anti-drift pin. This check duplicates a predicate the SF already
    owns; duplication is only safe while the two agree, so assert against
    the definition itself rather than against a copy of it.

    Every registry entry must name a real state in step_function.json, and
    the predictor entry's S3 object path must be the one that state heads.
    """
    import json
    from pathlib import Path

    defn = json.loads(
        (Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json").read_text()
    )

    def walk(states):
        for n, s in states.items():
            yield n, s
            if s.get("Type") == "Parallel":
                for b in s.get("Branches", []):
                    yield from walk(b.get("States", {}))
            if s.get("Type") == "Map":
                it = s.get("Iterator") or s.get("ItemProcessor") or {}
                yield from walk(it.get("States", {}))

    states = dict(walk(defn["States"]))

    assert sfp.SKIP_ARTIFACT_PREDICATES, "registry must not be empty"
    for pred in sfp.SKIP_ARTIFACT_PREDICATES:
        assert pred.sf_guard in states, (
            f"{pred.flag} names sf_guard={pred.sf_guard!r}, which is not a state "
            f"in step_function.json — this check would be the SOLE author of a "
            f"correctness rule instead of an early copy of the SF's own"
        )

    # The predictor entry specifically: the object path must match the
    # HeadObject the SF performs, or the preflight asserts against a
    # different artifact than the guard it claims to mirror.
    validate = states["ValidatePredictorSkipWeightsFresh"]
    sf_target = validate["Parameters"]["Key"]
    pred = next(p for p in sfp.SKIP_ARTIFACT_PREDICATES
                if p.flag == "skip_predictor_training")
    assert pred.key == sf_target, (
        f"preflight checks {pred.key!r} but ValidatePredictorSkipWeightsFresh "
        f"heads {sf_target!r} — the two have drifted"
    )

    # ...and the comparison must still be >= the CALENDAR date, not something
    # else. alpha-engine-config-I8809: this was `$.run_date` until 2026-08-27,
    # when NormalizeRunDates made $.run_date the cycle's TRADING day. The left
    # side is an S3 LastModified — a wall-clock write time — so against the
    # trading day the guard becomes strictly WEAKER on every Saturday run: a
    # manifest written on Friday would satisfy "a training run completed for
    # this cycle". Both sides moved together; that is what this pin exists for.
    choice = states["CheckPredictorSkipWeightsFresh"]["Choices"][0]["And"]
    assert any(
        c.get("StringGreaterThanEqualsPath") == "$.calendar_date" for c in choice
    ), (
        "CheckPredictorSkipWeightsFresh no longer compares >= $.calendar_date; "
        "check_skip_flag_artifact_coherence's last_modified_gte_calendar_date "
        "predicate is now wrong"
    )
    assert pred.kind == "last_modified_gte_calendar_date"


def test_weekly_preflight_receives_the_execution_input():
    """alpha-engine-config-I7443 — structural pin on the definition.

    WeeklyPreflight was the ONLY ``lambda:invoke`` task in this definition
    with no ``Payload`` at all. Under the optimized Lambda integration an
    omitted Payload means the function is invoked with no event, so the
    handler received ``{}`` on every real execution: its documented
    ``bucket`` / ``sf_name`` overrides were dead code in production, and
    ``check_skip_flag_artifact_coherence`` would have had nothing to check.

    A gate that cannot see the input it is gating is not a gate. This pins
    that it can — and that every other lambda:invoke still passes one, so
    the omission cannot silently reappear on a new state.
    """
    import json
    from pathlib import Path

    defn = json.loads(
        (Path(__file__).resolve().parents[1] / "infrastructure" / "step_function.json").read_text()
    )

    def walk(states):
        for n, s in states.items():
            yield n, s
            if s.get("Type") == "Parallel":
                for b in s.get("Branches", []):
                    yield from walk(b.get("States", {}))
            if s.get("Type") == "Map":
                it = s.get("Iterator") or s.get("ItemProcessor") or {}
                yield from walk(it.get("States", {}))

    states = dict(walk(defn["States"]))

    params = states["WeeklyPreflight"]["Parameters"]
    assert params.get("Payload.$") == "$", (
        "WeeklyPreflight must receive the whole execution input; without it "
        "check_skip_flag_artifact_coherence sees no run_date and no skip_* "
        "flags and silently reports 'nothing claimed' on every run"
    )

    missing = [
        name for name, s in states.items()
        if s.get("Resource", "").endswith("lambda:invoke")
        and not any(k.startswith("Payload") for k in s.get("Parameters", {}))
    ]
    assert not missing, (
        f"lambda:invoke states with no Payload receive an empty event: {missing}. "
        f"Every handler's event contract is dead code until one is passed."
    )
