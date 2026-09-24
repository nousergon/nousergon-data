"""The point-in-time map is S&P 500+400, like the roster it is replayed from —
alpha-engine-config-I11525.

``historical_constituents.collect`` walked the combined 903-name S&P 500+400
roster back through S&P 500 changes only. A name that moved from the S&P 400
into the S&P 500 was taken out at its S&P 500 add date and never put back as
an S&P 400 member, and a name that left the S&P 400 was never restored.
Replayed against the 114 roster snapshots on S3 (2026-04-04 .. 2026-09-23),
the map reproduced 3 of them. Brian ruled on 2026-09-24 that the scope is
S&P 500+400, so observed changes are now the diff of the COMBINED rosters, and
every build replays the finished map against every snapshot.

Also here: the SSGA weight/index attribution for a name both funds hold on a
rebalance day (ILMN and P on 2026-09-18) keeps SPY's, not MDY's.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import collectors.historical_constituents as hc
from collectors.historical_constituents import ADDED, REMOVED, ConstituentChange


# ── Reading the combined roster ─────────────────────────────────────────────


def test_the_universe_is_the_union_of_the_explicit_per_index_lists():
    snap = {
        "tickers": ["AAPL", "ILMN", "TOST"],
        "sp500_tickers": ["AAPL", "ILMN"],
        "sp400_tickers": ["ILMN", "TOST"],
        "sp500_count": 2, "sp400_count": 2,
    }
    assert hc.universe_roster_with_provenance(snap) == (["AAPL", "ILMN", "TOST"], "explicit")


def test_a_rebalance_day_legacy_snapshot_is_read_whole_not_sliced():
    """2026-09-18 had no explicit lists and a SPY/MDY overlap of 2. The S&P
    500 slice needs recovery arithmetic; the universe needs none."""
    spy, mdy = ["AAPL", "ILMN", "P"], ["TOST", "ILMN", "P"]
    snap = {"tickers": list(dict.fromkeys(spy + mdy)), "sp500_count": 3, "sp400_count": 3}
    assert hc.universe_roster_with_provenance(snap) == (["AAPL", "ILMN", "P", "TOST"], "tickers")


def test_a_cache_served_roster_is_not_an_observation():
    roster, why = hc.universe_roster_with_provenance(
        {"tickers": ["A"], "sp500_count": 0, "sp400_count": 0}
    )
    assert roster is None and "cache" in why


def test_load_reads_the_universe_even_where_the_sp500_slice_is_refused():
    unreadable_slice = {"tickers": ["A", "B", "C"], "sp500_count": 1, "sp400_count": 1}
    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [{
        "Contents": [{"Key": "market_data/weekly/2026-09-21/constituents.json"}],
    }]
    body = MagicMock()
    body.read.return_value = json.dumps(unreadable_slice).encode()
    s3.get_object.return_value = {"Body": body}
    out = hc.load_roster_snapshots("bucket", s3=s3)
    assert out.universe == {"2026-09-21": ["A", "B", "C"]}
    assert "2026-09-21" in out.skipped and not out.universe_skipped


# ── The movers the issue names ──────────────────────────────────────────────


def _collect(universe, sp500, current, *, reference=None, prior=None):
    """Run collect() with no frozen history, so only the observed window acts."""
    rosters = hc.RosterSnapshots(snapshots=sp500, universe=universe)
    s3 = MagicMock()
    with patch.object(hc, "load_frozen_changes", return_value=[]), \
         patch.object(hc, "load_roster_snapshots", return_value=rosters), \
         patch.object(hc, "load_prior_rename_checks", return_value=(prior, None)), \
         patch.object(hc, "_fetch_changes_table", return_value=(None, "u")), \
         patch.object(hc, "parse_changes_table", return_value=reference or []), \
         patch.object(hc, "resolve_renames", return_value=hc.RenameResolution()), \
         patch.object(hc.boto3, "client", return_value=s3):
        out = hc.collect("bucket", current)
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    return out, written


def _on(written, current, date):
    return hc.membership_as_of(written["membership"], current, date)


def test_a_400_to_500_mover_stays_in_the_universe_before_its_move():
    """ILMN and P: S&P 400 members until the 2026-09-21 open, S&P 500 after.
    The old S&P 500-only walk-back took them out of the universe for every
    date before 2026-09-18."""
    universe = {
        "2026-09-17": ["AAPL", "ILMN", "P", "TOST"],
        "2026-09-21": ["AAPL", "ILMN", "P", "TOST"],
    }
    sp500 = {"2026-09-17": ["AAPL"], "2026-09-21": ["AAPL", "ILMN", "P"]}
    current = ["AAPL", "ILMN", "P", "TOST"]
    reference = [
        ConstituentChange("2026-09-21", "ILMN", ADDED),
        ConstituentChange("2026-09-21", "P", ADDED),
    ]
    out, written = _collect(universe, sp500, current, reference=reference)
    assert {"ILMN", "P"} <= _on(written, current, "2026-09-17")
    assert written["n_changes_observed"] == 0  # not a universe change at all
    assert out["n_replay_mismatches"] == 0
    assert out["n_reference_disagreements"] == 0  # the S&P 500 attestation still sees the adds
    assert out["status"] == "ok"


def test_a_name_that_left_the_400_is_restored_before_its_removal():
    universe = {
        "2026-09-17": ["AAPL", "CPRI", "SAM"],
        "2026-09-21": ["AAPL", "AGNC"],
    }
    sp500 = {"2026-09-17": ["AAPL"], "2026-09-21": ["AAPL"]}
    current = ["AAPL", "AGNC"]
    out, written = _collect(universe, sp500, current)
    before = _on(written, current, "2026-09-17")
    assert {"CPRI", "SAM"} <= before and "AGNC" not in before
    assert _on(written, current, "2026-09-21") == {"AAPL", "AGNC"}
    assert out["n_replay_mismatches"] == 0


def test_the_artifact_declares_its_scope_and_the_pre_cutover_gap():
    universe = {"2026-09-17": ["AAPL"], "2026-09-21": ["AAPL"]}
    _, written = _collect(universe, universe, ["AAPL"])
    assert written["index"] == "S&P 500+400"
    assert written["scope"]["universe"] == "S&P 500+400"
    assert written["scope"]["observed"]["from"] == hc.SNAPSHOT_CUTOVER
    assert written["scope"]["frozen"]["changes"] == "S&P 500 only"
    assert "S&P 400" in written["scope"]["frozen"]["gap"]


# ── The replay check ────────────────────────────────────────────────────────


def test_replay_names_a_date_the_map_does_not_reproduce():
    pit = {"2026-09-21": ["AAPL"]}  # claims only AAPL before 09-21
    universe = {"2026-09-17": ["AAPL", "ILMN"], "2026-09-21": ["AAPL", "ILMN"]}
    out = hc.replay_mismatches(pit, ["AAPL", "ILMN"], universe)
    assert out == ["2026-09-17: missing=['ILMN'] extra=[]"]


def test_replay_expects_the_new_symbol_through_a_retickers_old_dates():
    pit = {"2026-05-22": ["AAPL", "BNY"]}
    universe = {"2026-05-21": ["AAPL", "BK"], "2026-05-22": ["AAPL", "BNY"]}
    assert hc.replay_mismatches(pit, ["AAPL", "BNY"], universe, renames={"BK": "BNY"}) == []


def test_replay_exempts_a_dropped_holdings_file_round_trip():
    """OKE was missing from SPY's 2026-09-10 file only; the map keeps it."""
    universe = {
        "2026-09-09": ["A", "OKE"], "2026-09-10": ["A"], "2026-09-11": ["A", "OKE"],
    }
    dropped = [
        ConstituentChange("2026-09-10", "OKE", REMOVED),
        ConstituentChange("2026-09-11", "OKE", ADDED),
    ]
    assert hc.replay_mismatches({}, ["A", "OKE"], universe, dropped=dropped) == []
    assert hc.replay_mismatches({}, ["A", "OKE"], universe) == [
        "2026-09-10: missing=[] extra=['OKE']"
    ]


def test_a_current_roster_the_snapshots_do_not_lead_to_is_a_replay_mismatch():
    universe = {"2026-09-17": ["AAPL"], "2026-09-21": ["AAPL"]}
    out, _ = _collect(universe, universe, ["AAPL", "ZZZ"])
    assert out["status"] == "degraded"
    assert out["n_replay_mismatches"] == 2
    assert "does not reproduce" in out["detail"]


def test_verdict_is_degraded_on_a_replay_mismatch():
    status, detail = hc._verdict(
        unexplained=[], recent_skips={}, unresolved=[], deferred=[],
        replay=["2026-09-17: missing=['ILMN'] extra=[]"],
    )
    assert status == "degraded" and "ILMN" in detail


# ── Rename verdicts are carried, so S&P 400 churn does not re-query Polygon ──


def _detect(renames=(), failed=()):
    return MagicMock(
        renames=[MagicMock(ticker=o, new_ticker=n) for o, n in renames],
        failed_candidates=set(failed),
    )


def test_a_candidate_polygon_already_answered_is_not_asked_again():
    swaps = {"2026-05-22": ["PSTG"], "2026-05-20": ["FLO"], "2026-09-21": ["CPRI"]}
    prior = {"renamed": {"PSTG": "P"}, "not_renamed": ["FLO"]}
    with patch("builders.prune_delisted_tickers._build_rename_client", return_value=object()), \
         patch("corporate_actions.detect_renames", return_value=_detect()) as detect:
        out = hc.resolve_renames(swaps, reference_removed=set(), prior=prior)
    assert detect.call_args.args[0] == ["CPRI"]
    assert out.renames == {"PSTG": "P"}
    assert out.settled_by_prior_run == ["FLO", "PSTG"]
    assert out.settled() == {"renamed": {"PSTG": "P"}, "not_renamed": ["CPRI", "FLO"]}


def test_a_deferred_candidate_is_not_settled_and_is_asked_again():
    with patch("builders.prune_delisted_tickers._build_rename_client", return_value=object()), \
         patch("corporate_actions.detect_renames", return_value=_detect(failed=["MYST"])):
        out = hc.resolve_renames({"2026-09-01": ["MYST"]}, reference_removed=set())
    assert out.deferred == ["MYST"]
    assert out.settled() == {"renamed": {}, "not_renamed": []}


def test_committed_retickers_are_not_repeated_in_the_settled_verdicts():
    with patch("builders.prune_delisted_tickers._build_rename_client", return_value=object()), \
         patch("corporate_actions.detect_renames", return_value=_detect()):
        out = hc.resolve_renames({"2026-08-18": ["EQR"]}, reference_removed=set())
    assert out.renames == {"EQR": "VMRK"}
    assert out.settled() == {"renamed": {}, "not_renamed": []}


def test_an_unreadable_previous_artifact_means_asking_again_not_failing():
    s3 = MagicMock()
    s3.get_object.side_effect = RuntimeError("NoSuchKey")
    prior, error = hc.load_prior_rename_checks("bucket", s3=s3)
    assert prior is None and "NoSuchKey" in error


def test_the_previous_artifacts_verdicts_are_read_back():
    s3 = MagicMock()
    body = MagicMock()
    body.read.return_value = json.dumps(
        {"rename_checks": {"renamed": {"PSTG": "P"}, "not_renamed": ["FLO"]}}
    ).encode()
    s3.get_object.return_value = {"Body": body}
    prior, error = hc.load_prior_rename_checks("bucket", s3=s3)
    assert error is None and prior == {"renamed": {"PSTG": "P"}, "not_renamed": ["FLO"]}


# ── Dual-held names keep their S&P 500 attribution (I11295 follow-on) ───────


def test_a_name_both_funds_hold_keeps_its_sp500_weight_and_index():
    from tests.test_constituents_index_overlap_i11470 import _serve

    from collectors import constituents

    with _serve(
        {"Ticker": ["AAPL", "ILMN", "P"], "Weight": [60.0, 20.0, 20.0]},
        {"Ticker": ["ILMN", "P", "TOST"], "Weight": [30.0, 30.0, 40.0]},
    ):
        _, _, _, weights = constituents._fetch_ssga_membership()
    assert weights.index_of == {
        "AAPL": "S&P 500", "ILMN": "S&P 500", "P": "S&P 500", "TOST": "S&P 400",
    }
    assert weights.weight_map["ILMN"] == 0.2  # SPY's 20 of 100, not MDY's 30
    assert weights.weight_map["P"] == 0.2
    assert weights.weight_map["TOST"] == 0.4
