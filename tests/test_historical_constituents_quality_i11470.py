"""historical_constituents reports its own accuracy — alpha-engine-config-I11470.

The 2026-09-23 weekly rehearsal published the point-in-time S&P 500 map with
status ok while its log said: 4 reference disagreements (EQR, OKE twice,
VMRK), 3 roster snapshots skipped (including 2026-09-18, the newest one before
the run), and POOL's rename check dropped after HTTP 429s. Each had a cause
that could be named from the data:

* EQR -> VMRK is a reticker (VMRK's price history IS EQR's).
* OKE was missing from SPY's 2026-09-10 holdings file and back on 2026-09-11.
* The skipped snapshots were rebalance days, when both funds hold the names
  moving between indices and the cross-fund dedupe shortens the list.
* The rename query was not needed: the reference already lists POOL as an
  index removal.

These tests pin the fixes, and pin that what is left over is a counted,
named, DEGRADED result rather than a log line.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import collectors.historical_constituents as hc
from collectors.historical_constituents import ADDED, REMOVED, ConstituentChange


# ── Rebalance-day snapshots are recovered, not skipped ───────────────────────


def _rebalance_snapshot() -> dict:
    """The 2026-09-18 shape: SPY first, MDY second, deduped keeping the first
    occurrence, so the two names both funds held vanish from the MDY tail."""
    spy = ["AAPL", "ILMN", "P", "MSFT"]
    mdy = ["TOST", "ILMN", "P", "IESC"]
    return {
        "tickers": list(dict.fromkeys(spy + mdy)),
        "sp500_count": len(spy),
        "sp400_count": len(mdy),
    }


def test_a_rebalance_day_overlap_is_recovered_from_the_spy_prefix():
    roster, how = hc.sp500_roster_with_provenance(_rebalance_snapshot())
    assert roster == ["AAPL", "ILMN", "P", "MSFT"]
    assert how == "prefix_recovered_overlap_2"


def test_a_list_shorter_than_the_sp500_slice_is_refused():
    snap = {"tickers": ["A", "B"], "sp500_count": 3, "sp400_count": 0}
    roster, how = hc.sp500_roster_with_provenance(snap)
    assert roster is None and "distinct" in how


def test_a_cache_served_snapshot_is_refused_with_a_reason():
    roster, how = hc.sp500_roster_with_provenance(
        {"tickers": ["A"], "sp500_count": 0, "sp400_count": 0}
    )
    assert roster is None and "cache" in how


def _fake_s3(bodies: dict[str, dict]) -> MagicMock:
    s3 = MagicMock()
    s3.get_paginator.return_value.paginate.return_value = [{
        "Contents": [
            {"Key": f"market_data/weekly/{d}/constituents.json"} for d in bodies
        ] + [{"Key": "market_data/weekly/latest_weekly.json"}],
    }]

    def get_object(Bucket, Key):
        date = Key.split("/")[2]
        body = MagicMock()
        body.read.return_value = json.dumps(bodies[date]).encode()
        return {"Body": body}

    s3.get_object.side_effect = get_object
    return s3


def test_load_names_every_skip_and_recovery_and_flags_the_recent_ones():
    good = {"sp500_tickers": ["A", "B"], "tickers": ["A", "B"]}
    unreadable = {"tickers": ["A", "B", "C"], "sp500_count": 1, "sp400_count": 1}
    bodies = {
        "2026-04-11": unreadable,          # old: named, not recent
        "2026-09-10": good,
        "2026-09-18": _rebalance_snapshot(),
        "2026-09-21": unreadable,          # newest: recent
    }
    out = hc.load_roster_snapshots("bucket", s3=_fake_s3(bodies))
    assert sorted(out.snapshots) == ["2026-09-10", "2026-09-18"]
    assert set(out.skipped) == {"2026-04-11", "2026-09-21"}
    assert "exceeds" in out.skipped["2026-09-21"]
    assert out.recovered == {"2026-09-18": "prefix_recovered_overlap_2"}
    assert out.recent_skips() == ["2026-09-21"]


# ── A holdings-file round trip is not index churn ───────────────────────────


def test_a_one_day_absence_is_a_flicker_not_two_index_changes():
    changes = [
        ConstituentChange("2026-09-10", "OKE", REMOVED),
        ConstituentChange("2026-09-11", "OKE", ADDED),
        ConstituentChange("2026-09-21", "BE", ADDED),
    ]
    kept, flickers = hc.suppress_flickers(changes)
    assert kept == [ConstituentChange("2026-09-21", "BE", ADDED)]
    assert flickers == ["OKE absent 2026-09-10 -> 2026-09-11 (1d)"]


def test_a_one_file_appearance_is_a_flicker_too():
    changes = [
        ConstituentChange("2026-06-01", "ZZZ", ADDED),
        ConstituentChange("2026-06-02", "ZZZ", REMOVED),
    ]
    kept, flickers = hc.suppress_flickers(changes)
    assert kept == [] and flickers == ["ZZZ present 2026-06-01 -> 2026-06-02 (1d)"]


def test_a_removal_and_readd_further_apart_than_the_window_are_both_kept():
    changes = [
        ConstituentChange("2026-05-01", "X", REMOVED),
        ConstituentChange("2026-08-01", "X", ADDED),
    ]
    kept, flickers = hc.suppress_flickers(changes)
    assert kept == changes and flickers == []


# ── The reference window ────────────────────────────────────────────────────


def test_a_reference_change_after_the_newest_snapshot_is_pending_not_a_disagreement():
    reference = [ConstituentChange("2026-09-28", "NEW", ADDED)]
    assert hc.divergences([], reference, since=hc.SNAPSHOT_CUTOVER, until="2026-09-23") == []
    assert hc.pending_reference_changes(reference, until="2026-09-23") == [
        "2026-09-28 added NEW"
    ]


def test_an_observation_that_leads_the_effective_date_still_matches():
    """Funds buy an addition at the close before it takes effect: 2026-09-18
    held ILMN, effective 2026-09-21. With `until` = 2026-09-18 the reference
    entry is pending, and the observation must still find it."""
    observed = [ConstituentChange("2026-09-18", "ILMN", ADDED)]
    reference = [ConstituentChange("2026-09-21", "ILMN", ADDED)]
    assert hc.divergences(
        observed, reference, since=hc.SNAPSHOT_CUTOVER, until="2026-09-18"
    ) == []


# ── Retickers ───────────────────────────────────────────────────────────────


def test_eqr_to_vmrk_is_a_committed_reticker():
    known = json.loads(hc._KNOWN_RETICKERS_PATH.read_text())
    pairs = {(r["old"], r["new"]) for r in known["retickers"]}
    assert ("EQR", "VMRK") in pairs
    assert all(r.get("evidence") and r.get("effective") for r in known["retickers"])


def test_candidates_the_reference_already_removed_are_not_sent_to_polygon():
    swaps = {"2026-06-22": ["CPB", "POOL"], "2026-08-18": ["EQR"], "2026-09-01": ["MYST"]}
    detection = MagicMock(renames=[], failed_candidates={"MYST"})
    with patch("builders.prune_delisted_tickers._build_rename_client", return_value=object()), \
         patch("corporate_actions.detect_renames", return_value=detection) as detect:
        out = hc.resolve_renames(swaps, reference_removed={"CPB", "POOL"})
    detect.assert_called_once()
    assert detect.call_args.args[0] == ["MYST"]
    assert out.renames == {"EQR": "VMRK"}
    assert out.confirmed_by_reference == ["CPB", "POOL"]
    assert out.deferred == ["MYST"]


def test_every_candidate_is_deferred_by_name_when_detection_is_unavailable():
    with patch("builders.prune_delisted_tickers._build_rename_client", return_value=None):
        out = hc.resolve_renames({"2026-09-01": ["MYST", "POOL"]}, reference_removed=None)
    assert out.deferred == ["MYST", "POOL"]


# ── The verdict ─────────────────────────────────────────────────────────────


def test_verdict_is_ok_with_nothing_unexplained():
    assert hc._verdict(unexplained=[], recent_skips={}, unresolved=[], deferred=[]) == ("ok", None)


def test_verdict_is_degraded_and_names_every_defect():
    status, detail = hc._verdict(
        unexplained=["observed removed EQR not in reference"],
        recent_skips={"2026-09-18": "overlap too large"},
        unresolved=["2026-08-18: out=['EQR'] in=['VMRK']"],
        deferred=["POOL"],
    )
    assert status == "degraded"
    for name in ("EQR", "2026-09-18", "POOL", "VMRK"):
        assert name in detail


def _collect(snapshots, reference, *, skipped=None):
    rosters = hc.RosterSnapshots(snapshots=snapshots, skipped=skipped or {})
    s3 = MagicMock()
    with patch.object(hc, "load_roster_snapshots", return_value=rosters), \
         patch.object(hc, "_fetch_changes_table", return_value=(None, "u")), \
         patch.object(hc, "parse_changes_table", return_value=reference), \
         patch.object(hc, "resolve_renames", return_value=hc.RenameResolution()), \
         patch.object(hc.boto3, "client", return_value=s3):
        out = hc.collect("bucket", ["A", "B", "C"])
    written = json.loads(s3.put_object.call_args.kwargs["Body"])
    return out, written


def test_collect_reports_ok_when_the_reference_agrees():
    snaps = {"2026-09-10": ["A", "B"], "2026-09-17": ["A", "B", "C"]}
    out, written = _collect(snaps, [ConstituentChange("2026-09-15", "C", ADDED)])
    assert out["status"] == "ok"
    assert out["n_reference_disagreements"] == 0
    assert written["quality"]["status"] == "ok"


def test_collect_is_degraded_with_names_on_an_unexplained_disagreement():
    snaps = {"2026-09-10": ["A", "B"], "2026-09-17": ["A", "B", "C"]}
    out, written = _collect(snaps, [])
    assert out["status"] == "degraded"
    assert out["n_reference_disagreements"] == 1
    assert "observed added C not in reference" in out["detail"]
    assert written["quality"]["reference_disagreements"] == [
        "observed added C not in reference"
    ]


def test_collect_is_degraded_on_a_skipped_recent_snapshot():
    snaps = {"2026-09-10": ["A", "B"], "2026-09-17": ["A", "B"]}
    out, _ = _collect(snaps, [], skipped={"2026-09-18": "unreadable"})
    assert out["status"] == "degraded"
    assert out["recent_skipped_snapshots"] == {"2026-09-18": "unreadable"}
    assert "2026-09-18" in out["detail"]


def test_collect_drops_a_flicker_and_names_it():
    snaps = {
        "2026-09-09": ["A", "OKE"],
        "2026-09-10": ["A"],
        "2026-09-11": ["A", "OKE"],
    }
    out, written = _collect(snaps, [])
    assert out["status"] == "ok"
    assert out["snapshot_flickers"] == ["OKE absent 2026-09-10 -> 2026-09-11 (1d)"]
    assert written["n_changes_observed"] == 0


# ── The DEGRADED alert carries the collector's own detail ───────────────────


def test_the_degraded_alert_names_the_disagreements_and_the_tracking_issue():
    from weekly_collector import _DEGRADED_DEFECT_REGISTRY, _describe_degraded_defects

    detail = _describe_degraded_defects({
        "degraded_collectors": ["historical_constituents"],
        "collectors": {"historical_constituents": {
            "status": "degraded",
            "detail": "historical_constituents: 1 unexplained reference "
                      "disagreement(s): ['observed added C not in reference']",
        }},
    })
    assert "observed added C not in reference" in detail
    assert "alpha-engine-config-I11470" in detail
    assert _DEGRADED_DEFECT_REGISTRY["historical_constituents"]["clears_when"] in detail
    assert "UNTRACKED" not in detail
