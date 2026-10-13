"""Phase-2 cardinality guards for D01 (constituents) and D02 (historical_constituents).

`data_collection_plan_260914.md` §2 row 2 and §4.5; `alpha-engine-config-I10785`
(P-18). The template is D20's ``collectors/metron_market_data.py::_grade_cardinality``:
``validators/expectations.py::check_cardinality`` graded inside the collector,
returned as one ``data_cardinality`` guard entry that
``weekly_collector._record_collector_guards`` folds onto the run manifest,
OBSERVE mode on ``expectations.CARDINALITY_GUARD``.

What is pinned here:

* on the last good run's real shape (``tests/fixtures/constituents_guards/``,
  read from the published S3 objects) each guard reads ``ok``;
* a collapse reads ``below_floor`` and names what was lost, and a missing
  baseline reads ``unmeasurable`` (red, never a pass);
* a fault inside the grading is recorded, never raised — D01 also runs
  pre-MorningEnrich every trading day and a D02 failure stops the weekly
  pipeline at CollectionReadiness;
* the published bytes are identical with and without the guard, including on a
  run where the guard fires, which is what OBSERVE mode promises.
"""
from __future__ import annotations

import json
import logging
from contextlib import ExitStack, nullcontext
from pathlib import Path
from unittest.mock import MagicMock, patch

import nousergon_lib
import pytest

import weekly_collector as wc
from collectors import constituents
from collectors import historical_constituents as hc
from validators import expectations

FIXTURES = Path(__file__).parent / "fixtures" / "constituents_guards"
D01 = json.loads((FIXTURES / "d01_rosters.json").read_text())
D02 = json.loads((FIXTURES / "d02_builds.json").read_text())

D01_KEY = "market_data/weekly/2026-10-09/constituents.json"
D02_KEY = "market_data/historical_constituents.json"


def _manifest_shape():
    schema = json.loads(
        (Path(nousergon_lib.__file__).parent / "contracts" / "data_run_manifest.schema.json")
        .read_text()
    )
    return schema["$defs"]["GuardVerdict"]


def _assert_fits_the_manifest(entry: dict) -> None:
    shape = _manifest_shape()
    assert set(shape["required"]) <= set(entry) <= set(shape["properties"])
    assert entry["verdict"] in shape["properties"]["verdict"]["enum"]
    assert entry["mode"] in shape["properties"]["mode"]["enum"]
    assert len(entry["detail"]) <= shape["properties"]["detail"]["maxLength"]
    run_ctx = MagicMock()
    wc._record_collector_guards(run_ctx, {"guards": [entry]})
    run_ctx.record_guard.assert_called_once()
    assert run_ctx.record_guard.call_args.args == (expectations.CARDINALITY_GUARD.name,)


# ── D01: week-over-week membership change ───────────────────────────────────


def test_d01_last_good_run_reads_ok():
    pair = D01["last_good"]
    entry = constituents._grade_cardinality(pair["previous"], pair["current"]["tickers"], key=D01_KEY)
    assert entry["guard"] == "data_cardinality"
    assert entry["mode"] == "observe"
    assert entry["verdict"] == "ok"
    assert entry["value"] == 1.0
    assert entry["baseline"] == constituents.D01_MEMBERSHIP_STABILITY_FLOOR == 0.95
    assert entry["key"] == D01_KEY
    _assert_fits_the_manifest(entry)


def test_d01_the_largest_measured_rebalance_reads_ok_and_names_every_mover():
    pair = D01["rebalance"]
    prev, cur = set(pair["previous"]["tickers"]), set(pair["current"]["tickers"])
    movers = sorted(prev ^ cur)
    assert len(movers) == 6
    entry = constituents._grade_cardinality(pair["previous"], pair["current"]["tickers"], key=D01_KEY)
    assert entry["verdict"] == "ok"
    assert entry["value"] == pytest.approx(len(prev & cur) / len(prev | cur))
    for name in movers:
        assert name in entry["detail"]
    assert "2026-09-18" in entry["detail"]


def test_d01_a_roster_that_lost_the_mdy_file_is_below_floor_and_named(caplog):
    pair = D01["last_good"]
    sp500_only = pair["current"]["tickers"][:503]
    with caplog.at_level(logging.ERROR, logger="collectors.constituents"):
        entry = constituents._grade_cardinality(pair["previous"], sp500_only, key=D01_KEY)
    assert entry["verdict"] == "below_floor"
    assert entry["value"] < 0.95
    lost = sorted(set(pair["previous"]["tickers"]) - set(sp500_only))
    assert len(lost) == 400
    # The first 60 are named and the rest counted, so the count, the floor and
    # the remedy all survive the manifest's 2,000-character detail.
    assert lost[0] in entry["detail"] and "... and 340 more" in entry["detail"]
    assert len(entry["detail"]) < 2000 and entry["detail"].endswith("say otherwise.")
    assert "400 UNDECLARED" in entry["detail"] and "SPY/MDY" in entry["detail"]
    assert any("verdict=below_floor" in r.getMessage() for r in caplog.records)


def test_d01_no_earlier_snapshot_is_unmeasurable_not_a_pass():
    for previous in (None, {"date": "2026-10-08"}, {"date": "2026-10-08", "tickers": []}):
        entry = constituents._grade_cardinality(previous, ["AAPL"], key=D01_KEY)
        assert entry["verdict"] == "unmeasurable"
        assert entry["value"] is None


def test_d01_a_grading_fault_is_recorded_not_raised():
    with patch.object(expectations, "check_cardinality", side_effect=RuntimeError("boom")):
        entry = constituents._grade_cardinality(
            D01["last_good"]["previous"], D01["last_good"]["current"]["tickers"], key=D01_KEY,
        )
    assert entry["verdict"] == "unmeasurable"
    assert "RuntimeError: boom" in entry["detail"]


def _d01_fetch(tickers):
    weights = constituents.SsgaWeights(
        weight_map={t: 1.0 / len(tickers) for t in tickers},
        index_of={t: "S&P 500" for t in tickers},
        raw_sum_by_index={"S&P 500": 100.0},
        method="ssga_holdings_file",
        members_by_index={"S&P 500": list(tickers)},
    )
    sectors = {t: "Information Technology" for t in tickers}
    etfs = {t: "XLK" for t in tickers}
    return lambda: (list(tickers), dict(sectors), dict(etfs), {}, len(tickers), 0, weights)


def _d01_collect(previous, tickers, *, guard=None):
    writes: dict[str, str] = {}

    def fake_put_object(**kwargs):
        writes[kwargs["Key"]] = kwargs["Body"]
        return {}

    with ExitStack() as stack:
        stack.enter_context(
            patch("collectors.constituents._fetch_constituents", side_effect=_d01_fetch(tickers))
        )
        stack.enter_context(
            patch("collectors.constituents._load_previous_snapshot", return_value=previous)
        )
        stack.enter_context(patch.dict("collectors.constituents._SECTOR_OVERRIDES", {}, clear=True))
        client = stack.enter_context(patch("collectors.constituents.boto3.client"))
        if guard is not None:
            stack.enter_context(patch.object(constituents, "_grade_cardinality", guard))
        client.return_value.put_object.side_effect = fake_put_object
        result = constituents.collect(bucket="any", run_date="2026-10-09")
    for key, body in writes.items():
        doc = json.loads(body)
        if isinstance(doc, dict):
            doc.pop("fetched_at", None)
        writes[key] = json.dumps(doc, sort_keys=True)
    return result, writes


@pytest.mark.parametrize("current", [["AAPL", "JPM", "CTVA", "HUBS"], ["AAPL"]], ids=["good-day", "fires"])
def test_d01_publishes_the_same_bytes_with_and_without_the_guard(current):
    previous = {"date": "2026-10-08", "tickers": ["AAPL", "JPM", "CTVA", "HUBS"]}
    guarded, guarded_writes = _d01_collect(previous, current)
    bare, bare_writes = _d01_collect(previous, current, guard=lambda *a, **k: {})
    assert guarded_writes == bare_writes
    assert len(guarded_writes) == 7
    assert guarded["status"] == bare["status"] == "ok"
    [entry] = guarded["guards"]
    assert entry["verdict"] == ("ok" if len(current) == 4 else "below_floor")
    assert {k: v for k, v in guarded.items() if k != "guards"} == {
        k: v for k, v in bare.items() if k != "guards"
    }


# ── D02: the history keeps every name the last build held ───────────────────


def test_d02_last_good_build_reads_ok_and_names_the_reticker_it_folded():
    build = D02["last_good_build"]
    assert build["quality_status"] == "ok"
    entry = hc._grade_cardinality(
        set(D02["prior_build"]["names"]), None, build["membership"], D02["current_roster"],
        key=D02_KEY,
    )
    assert entry["guard"] == "data_cardinality"
    assert entry["mode"] == "observe"
    assert entry["verdict"] == "ok"
    assert entry["baseline"] == hc.D02_HISTORY_RETENTION_FLOOR == 0.99
    assert entry["value"] == pytest.approx(1 - 1 / len(D02["prior_build"]["names"]))
    # PSKY -> SKYD (nousergon-data#2133): a correction, named but within the floor.
    assert "PSKY" in entry["detail"]
    _assert_fits_the_manifest(entry)


def test_d02_lost_frozen_history_is_below_floor_and_named():
    build = D02["last_good_build"]
    newest = max(build["membership"])
    truncated = {newest: build["membership"][newest]}
    entry = hc._grade_cardinality(
        set(D02["prior_build"]["names"]), None, truncated, D02["current_roster"], key=D02_KEY,
    )
    assert entry["verdict"] == "below_floor"
    assert entry["value"] < 0.99
    lost = sorted(set(D02["prior_build"]["names"]) - set(truncated[newest]) - set(D02["current_roster"]))
    assert lost[0] in entry["detail"]
    assert len(entry["detail"]) < 2000 and "lost frozen changes" in entry["detail"]


def test_d02_no_previous_build_is_unmeasurable_not_a_pass():
    entry = hc._grade_cardinality(None, "NoSuchKey: gone", {"2026-10-06": ["A"]}, ["A"], key=D02_KEY)
    assert entry["verdict"] == "unmeasurable"
    assert "NoSuchKey: gone" in entry["detail"]


def test_d02_a_grading_fault_is_recorded_not_raised():
    with patch.object(expectations, "check_cardinality", side_effect=RuntimeError("boom")):
        entry = hc._grade_cardinality({"A"}, None, {"2026-10-06": ["A"]}, ["A"], key=D02_KEY)
    assert entry["verdict"] == "unmeasurable"
    assert "RuntimeError: boom" in entry["detail"]


def test_d02_prior_names_are_every_name_in_the_published_membership():
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = json.dumps(
        {"membership": {"2026-09-21": ["A", "B"], "2026-10-06": ["B", "C"]}}
    ).encode()
    s3.get_object.return_value = {"Body": body}
    assert hc.load_prior_membership_names("bucket", s3=s3) == ({"A", "B", "C"}, None)
    assert s3.get_object.call_args.kwargs["Key"] == D02_KEY

    body.read.return_value = json.dumps({"membership": {}}).encode()
    names, error = hc.load_prior_membership_names("bucket", s3=s3)
    assert names is None and "membership is missing or empty" in error


def _d02_collect(prior_names, *, guard=None):
    rosters = hc.RosterSnapshots(
        snapshots={"2026-09-10": ["A", "B"], "2026-09-17": ["A", "B", "C"]}, skipped={},
    )
    s3 = MagicMock()
    extra = patch.object(hc, "_grade_cardinality", guard) if guard is not None else nullcontext()
    with patch.object(hc, "load_frozen_changes", return_value=[]), \
         patch.object(hc, "load_roster_snapshots", return_value=rosters), \
         patch.object(hc, "load_prior_membership_names", return_value=(prior_names, None)), \
         patch.object(hc, "_fetch_changes_table", return_value=(None, "u")), \
         patch.object(hc, "parse_changes_table",
                      return_value=[hc.ConstituentChange("2026-09-15", "C", hc.ADDED)]), \
         patch.object(hc, "resolve_renames", return_value=hc.RenameResolution()), \
         patch.object(hc.boto3, "client", return_value=s3), extra:
        out = hc.collect("bucket", ["A", "B", "C"])
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    written.pop("built_at")
    return out, json.dumps(written, sort_keys=True)


@pytest.mark.parametrize(
    "prior_names", [{"A", "B", "C"}, {"A", "B", "C", "GONE1", "GONE2"}], ids=["good-day", "fires"],
)
def test_d02_publishes_the_same_bytes_with_and_without_the_guard(prior_names):
    guarded, guarded_body = _d02_collect(prior_names)
    bare, bare_body = _d02_collect(prior_names, guard=lambda *a, **k: {"guard": "x"})
    assert guarded_body == bare_body
    assert guarded["status"] == bare["status"] == "ok"
    [entry] = guarded["guards"]
    assert entry["verdict"] == ("ok" if len(prior_names) == 3 else "below_floor")
    assert {k: v for k, v in guarded.items() if k != "guards"} == {
        k: v for k, v in bare.items() if k != "guards"
    }


# ── The two check_cardinality knobs these units pass ────────────────────────


def test_check_cardinality_defaults_are_the_eod_spines_wording_unchanged():
    kwargs = dict(unit_id="D20", denominator_symbols=["A", "B", "C"], covered_symbols=["A"],
                  exclusions={}, suffix_map={}, class_rules=[])
    default = expectations.check_cardinality(**kwargs)
    assert "B, C. An undeclared missing symbol is a failure" in default.detail
    assert "unpriced_symbols.yaml" in default.detail
    bounded = expectations.check_cardinality(**kwargs, max_named=1, miss_remedy="Fix it.")
    assert "UNDECLARED miss(es): B, ... and 1 more. Fix it." in bounded.detail
    assert "unpriced_symbols.yaml" not in bounded.detail
    assert (bounded.verdict, bounded.value) == (default.verdict, default.value)
