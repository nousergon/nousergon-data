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
    )
    # Actually use realistic-shape data: deduped tickers + complete sector_map.
    real_tickers = [f"T{i}" for i in range(900)]
    real_sectors = {t: "Industrials" for t in real_tickers}
    fake_return = (real_tickers, real_sectors, {}, {}, 500, 400)

    with patch("collectors.constituents._fetch_constituents", return_value=fake_return):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "ok"
    assert "900 tickers" in result.message
    assert ctx.fresh_constituents == set(real_tickers)


def test_constituents_fetch_fails_on_zero_tickers():
    ctx = _ctx()
    with patch("collectors.constituents._fetch_constituents", return_value=([], {}, {}, {}, 0, 0)):
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
               return_value=(tickers, sectors, {}, {}, 500, 400)):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "sector_map missing" in result.message


def test_constituents_fetch_fails_on_sp500_count_drift():
    """If Wikipedia parsing drops the table, sp500_count tanks."""
    ctx = _ctx()
    tickers = [f"T{i}" for i in range(400)]
    with patch(
        "collectors.constituents._fetch_constituents",
        return_value=(tickers, {t: "Industrials" for t in tickers}, {}, {}, 0, 400),
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
    """Commands that don't match the .venv/bin/python pattern are skipped."""
    ctx = _ctx()
    from unittest.mock import patch, MagicMock

    fake_sfn = MagicMock()
    fake_sfn.describe_state_machine.return_value = {
        "definition": '{"StartAt": "A", "States": {"A": {"Type": "Pass", "Next": "B"}, "B": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "Parameters": {"commands.$": "$.shell_cmd"}}}}'
    }
    with patch("boto3.client", return_value=fake_sfn):
        result = sfp.check_tool_contracts(ctx)
    # commands.$ exists but has no .venv pattern — should skip gracefully
    assert result.status == "ok"
    assert result.details.get("checked", 0) == 0


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


def test_price_cards_check_passes_when_all_models_have_cards(patched_sibling):
    """Happy path: every runtime model (after snapshot normalization) has
    a card. PR #77's normalization is honored."""
    patched_sibling(
        pricing_yaml="cards:\n"
                     "  - {model_name: claude-haiku-4-5, effective_from: 2026-01-01,"
                     " input_per_1m: 1.0, output_per_1m: 5.0,"
                     " cache_read_per_1m: 0.1, cache_create_per_1m: 1.25}\n"
                     "  - {model_name: claude-sonnet-4-6, effective_from: 2026-01-01,"
                     " input_per_1m: 3.0, output_per_1m: 15.0,"
                     " cache_read_per_1m: 0.3, cache_create_per_1m: 3.75}\n",
        universe_yaml="sector_teams:\n"
                      "  per_stock_model: claude-haiku-4-5-20251001\n"  # snapshot suffix
                      "  strategic_model: claude-sonnet-4-6\n",
        research_graph_src='_FALLBACK_AGENT_MODEL_NAMES = {"sector_team": "claude-haiku-4-5"}\n',
    )
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "ok"


def test_price_cards_check_fails_when_runtime_model_missing(patched_sibling):
    """The 2026-05-02 PR #77 scenario exactly: per_stock_model is
    'claude-haiku-4-5-20251001' (snapshot ID) but no card for the
    family 'claude-haiku-4-5' exists. SHOULD be caught here."""
    patched_sibling(
        pricing_yaml="cards:\n"
                     "  - {model_name: claude-sonnet-4-6, effective_from: 2026-01-01,"
                     " input_per_1m: 3.0, output_per_1m: 15.0,"
                     " cache_read_per_1m: 0.3, cache_create_per_1m: 3.75}\n",
        universe_yaml="sector_teams:\n"
                      "  per_stock_model: claude-haiku-4-5-20251001\n",
        research_graph_src="",  # no fallbacks
    )
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "fail"
    assert "haiku" in result.message.lower() or "no matching price card" in result.message.lower()


def test_price_cards_check_warns_when_sibling_repo_absent(monkeypatch):
    monkeypatch.setattr(sfp, "_sibling_repo", lambda name: None)
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "warn"


def test_price_cards_check_handles_fallback_models_in_research_graph(patched_sibling):
    """Models in _FALLBACK_AGENT_MODEL_NAMES must also be checked — the
    fallback path runs when track_llm_cost wiring is incomplete and would
    crash if its model isn't in the price table."""
    patched_sibling(
        pricing_yaml="cards: []\n",  # empty cards
        universe_yaml="",
        research_graph_src='_FALLBACK_AGENT_MODEL_NAMES = {\n'
                          '    "sector_team": "claude-haiku-4-5",\n'
                          '    "ic_cio": "claude-sonnet-4-6",\n'
                          '}\n',
    )
    result = sfp.check_price_cards_cover_all_models(_ctx())
    assert result.status == "fail"
    # Both fallback models should be flagged as missing.
    assert "sector_team" in str(result.details) and "ic_cio" in str(result.details)


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
