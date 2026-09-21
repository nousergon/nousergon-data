"""Tests for the Friday-Preflight Wikipedia-vs-ArcticDB constituents
drift check (5/23-SF P0 (g))."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from collectors.constituents import SsgaWeights

import pytest


def test_check_drift_no_drift_returns_ok():
    """When Wikipedia ⊆ ArcticDB universe (modulo skip-list), status=ok."""
    from validators.constituents_drift_check import check_drift

    # Wikipedia returns a small known set; ArcticDB has the same + extras.
    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "NVDA"],  # tickers
        {}, {}, {},                   # sector_map, sector_etf_map, sub_industry_map
        2, 1,                        # sp500_count, sp400_count
        SsgaWeights(),               # weights (I11295)
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT", "NVDA", "GOOG"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_missing_tickers_detected():
    """The canonical 5/23 scenario: Wikipedia lists BNY/P/SN, ArcticDB
    doesn't — drift_detected + missing list populated."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(
        ["AAPL", "MSFT", "BNY", "P", "SN"],
        {}, {}, {},
        3, 2,
        SsgaWeights(),
    ))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "drift_detected"
    assert set(result["missing_from_arctic"]) == {"BNY", "P", "SN"}
    assert result["within_threshold"] is False


def test_check_drift_under_threshold_passes():
    """max_stragglers tolerance: 1 missing with cap=2 → status=ok."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False, max_stragglers=2)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == ["BNY"]
    assert result["within_threshold"] is True


def test_check_drift_skip_list_excluded_from_diff():
    """SPY (in _SKIP_TICKERS) doesn't fire drift even if Wikipedia
    lists it but ArcticDB lacks the SKIP_TICKERS entry — _SKIP_TICKERS
    is stripped from BOTH sides of the comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "SPY", "VIX"], {}, {}, {}, 1, 0, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"
    assert result["missing_from_arctic"] == []


def test_check_drift_sector_etf_excluded():
    """XLK / XLF / XL* prefixes excluded from drift comparison."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "XLK", "XLF"], {}, {}, {}, 1, 0, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["status"] == "ok"


def test_check_drift_membership_fetch_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(side_effect=Exception("Wikipedia 503"))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "membership_fetch"


def test_check_drift_arctic_failure_returns_error():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0, SsgaWeights()))
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               side_effect=Exception("ArcticDB unreachable")):
        result = check_drift(alert=False)
    assert result["status"] == "error"
    assert result["stage"] == "arctic_list"


def test_main_exit_code_ok():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 0


def test_main_exit_code_drift_detected():
    from validators.constituents_drift_check import main

    fake_fetch = MagicMock(return_value=(["AAPL", "BNY"], {}, {}, {}, 1, 1, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        rc = main(["--no-alert"])
    assert rc == 1


# ---------------------------------------------------------------------------
# Alert message content (alpha-engine-config-I8094).
#
# These assert on the emitted STRING, which is unusual for this repo and
# deliberate. The 2026-08-21 SUI/VMRK page was correct in every field except
# its prose: it named Wikipedia as the membership source (moved to SSGA in
# I2812) and told the reader Research preflight was about to fail on a gate
# that cannot fail on this input. Both facts were only ever asserted in an
# f-string, so nothing caught either one drifting. The message is the
# product here — a detector nobody can act on has not detected anything.
# ---------------------------------------------------------------------------


def _capture_alert_message(*, tickers, arctic, sp500=1, sp400=1):
    """Run check_drift with alerting ON and return the published message."""
    from unittest.mock import MagicMock, patch

    from validators import constituents_drift_check as mod

    fake_fetch = MagicMock(return_value=(tickers, {}, {}, {}, sp500, sp400, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = arctic

    published = {}

    class _Leg:
        ok = True

    class _Result:
        sns = _Leg()
        telegram = _Leg()
        any_ok = True

    def _publish(message, **kwargs):
        published["message"] = message
        published["kwargs"] = kwargs
        return _Result()

    fake_alerts = MagicMock()
    fake_alerts.publish = _publish

    with patch.object(mod, "_fetch_constituents", fake_fetch), \
         patch.object(mod, "_open_universe_lib", return_value=fake_lib), \
         patch.dict("sys.modules", {"nousergon_lib.alerts": fake_alerts}), \
         patch("nousergon_lib.alerts", fake_alerts, create=True):
        mod.check_drift(alert=True)

    return published.get("message", "")


def test_alert_names_ssga_not_wikipedia_as_membership_source():
    """Membership ground truth moved to SSGA SPY/MDY holdings in I2812.

    An alert naming Wikipedia sends the reader to audit a system that has
    not been the source of truth since 2026-07.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "SSGA" in msg
    assert "Wikipedia-listed" not in msg


def test_alert_does_not_claim_research_preflight_will_fail():
    """`ResearchPreflight` checks macro.SPY freshness only.

    It cannot fail on ticker completeness, so the alert must not say it
    will. Guards the exact phrasing that produced the 2026-08-21 page.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "will likely fail at Research preflight" not in msg


def test_alert_states_the_margin_against_the_reachable_ceiling():
    """The reachable gate is fetch_price_data's 5% error-rate ceiling.

    A reader deciding whether to act tonight needs the observed fraction
    and the ceiling, not an adjective.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "price_fetcher.py::fetch_price_data" in msg
    assert "5%" in msg


def test_alert_says_under_ceiling_when_drift_is_a_small_fraction():
    """1 of 3 is over 5%; 1 of 40 is under. The clause must follow the
    arithmetic, not the mere existence of drift."""
    small = _capture_alert_message(
        tickers=[f"T{i}" for i in range(40)] + ["SUI"],
        arctic=[f"T{i}" for i in range(40)],
    )
    assert "UNDER the ceiling" in small
    assert "OVER the ceiling" not in small


def test_alert_says_over_ceiling_when_drift_exceeds_it():
    big = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI", "VMRK"], arctic=["AAPL"],
    )
    assert "OVER the ceiling" in big
    assert "UNDER the ceiling" not in big


def test_alert_cites_the_live_tracker_not_archived_roadmap_lines():
    """`ROADMAP P0 (g)` and `L1316` are both closed, archived work.

    Pointing triage at two tombstones costs a session every time.
    """
    msg = _capture_alert_message(
        tickers=["AAPL", "MSFT", "SUI"], arctic=["AAPL", "MSFT"],
    )
    assert "I8094" in msg
    assert "ROADMAP P0 (g)" not in msg
    assert "L1316" not in msg

# ── run-scope: drift is gating only when this run actually collected ────────
# alpha-engine-config-I8094. The population changes mid-week; Phase 1 absorbs
# it (prices fetches the ticker with no parquet, backfill writes its ArcticDB
# row). A preflight-only run enters DataPhase1 and collects nothing, so it
# cannot close a gap the index opened after the last collection — and must not
# fail the pipeline for it.


class _S3:
    """Minimal head_object stub. ``outcome`` is True / False / an exception."""

    def __init__(self, outcome):
        self.outcome = outcome
        self.calls: list[tuple[str, str]] = []

    def head_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg casing
        self.calls.append((Bucket, Key))
        if self.outcome is True:
            return {"ContentLength": 1}
        if self.outcome is False:
            raise _client_error("404")
        raise self.outcome


def _client_error(code: str) -> Exception:
    exc = Exception(f"head_object failed: {code}")
    exc.response = {"Error": {"Code": code}}
    return exc


def _drift(s3, run_date):
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "SUI"], {}, {}, {}, 1, 1, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        return check_drift(alert=False, run_date=run_date, s3_client=s3)


def test_drift_on_a_run_that_did_not_collect_is_deferred_not_detected():
    result = _drift(_S3(False), "2026-08-21")
    assert result["status"] == "drift_deferred"
    assert result["gating"] is False
    assert result["collection_ran"] is False
    # The finding itself is unchanged — deferred means not-this-run's-failure,
    # never not-reported.
    assert result["missing_from_arctic"] == ["SUI"]
    assert result["within_threshold"] is False


def test_drift_on_a_run_that_did_collect_still_gates():
    result = _drift(_S3(True), "2026-08-22")
    assert result["status"] == "drift_detected"
    assert result["gating"] is True
    assert result["collection_ran"] is True


def test_an_unanswerable_probe_keeps_gating():
    """AccessDenied is not permission to excuse the drift."""
    result = _drift(_S3(_client_error("AccessDenied")), "2026-08-22")
    assert result["collection_ran"] is None
    assert result["gating"] is True
    assert result["status"] == "drift_detected"


def test_without_a_run_date_the_check_gates_unconditionally():
    """Callers that supply no run_date get the pre-I8094 behaviour."""
    result = _drift(None, None)
    assert result["collection_ran"] is None
    assert result["gating"] is True
    assert result["status"] == "drift_detected"


def test_no_drift_is_ok_regardless_of_whether_the_run_collected():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL"], {}, {}, {}, 1, 0, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    s3 = _S3(False)
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False, run_date="2026-08-21", s3_client=s3)
    assert result["status"] == "ok"
    assert result["gating"] is True


def test_deferred_drift_publishes_no_alert():
    """A deferred finding must not page: it names work the next scheduled run
    does by itself, and a page nobody can act on is the cry-wolf surface."""
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "SUI"], {}, {}, {}, 1, 1, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL"]
    published: list = []

    class _Alerts:
        @staticmethod
        def publish(*args, **kwargs):
            published.append((args, kwargs))
            raise AssertionError("deferred drift must not publish an alert")

    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib), \
         patch.dict("sys.modules", {"nousergon_lib.alerts": _Alerts}):
        result = check_drift(alert=True, run_date="2026-08-21", s3_client=_S3(False))
    assert result["status"] == "drift_deferred"
    assert published == []


def test_main_exits_zero_on_deferred_drift_and_one_when_gating():
    from validators import constituents_drift_check as mod

    deferred = {
        "status": "drift_deferred", "missing_from_arctic": ["SUI"],
        "run_date": "2026-08-21", "max_stragglers": 0,
    }
    gating = {
        "status": "drift_detected", "missing_from_arctic": ["SUI"],
        "run_date": "2026-08-22", "max_stragglers": 0,
    }
    with patch.object(mod, "check_drift", return_value=deferred):
        assert mod.main(["--run-date", "2026-08-21", "--no-alert"]) == 0
    with patch.object(mod, "check_drift", return_value=gating):
        assert mod.main(["--run-date", "2026-08-22", "--no-alert"]) == 1


def test_collection_ran_probes_the_dated_constituents_artifact():
    from validators.constituents_drift_check import collection_ran

    s3 = _S3(True)
    assert collection_ran(s3, "alpha-engine-research", "2026-08-22") is True
    assert s3.calls == [
        ("alpha-engine-research", "market_data/weekly/2026-08-22/constituents.json"),
    ]


# ── the module may not name Wikipedia as the membership source ──────────────
# alpha-engine-config-I8094. nousergon-data#1495 corrected the alert message
# and the docstring, and the same claim survived in four other places: the
# module summary line, the `Composes with` closing paragraph (which asserted
# the opposite of the paragraph above it), the `--max-stragglers` help text,
# both `main()` log lines, and the `wikipedia_count` result key. A file that
# contradicts itself about its own upstream sends triage to the wrong system
# whichever half is read first.


def test_result_names_the_membership_source_not_wikipedia():
    from validators.constituents_drift_check import check_drift

    fake_fetch = MagicMock(return_value=(["AAPL", "MSFT"], {}, {}, {}, 2, 0, SsgaWeights()))
    fake_lib = MagicMock()
    fake_lib.list_symbols.return_value = ["AAPL", "MSFT"]
    with patch("validators.constituents_drift_check._fetch_constituents", fake_fetch), \
         patch("validators.constituents_drift_check._open_universe_lib",
               return_value=fake_lib):
        result = check_drift(alert=False)
    assert result["membership_count"] == 2
    assert "wikipedia_count" not in result


def test_no_line_claims_wikipedia_is_the_membership_source():
    """Wikipedia may only appear where it is being CORRECTED or where it is
    named as the GICS attestation source — never as the list of members."""
    import inspect

    from validators import constituents_drift_check as mod

    src = inspect.getsource(mod)
    offenders = []
    for lineno, line in enumerate(src.splitlines(), start=1):
        if "ikipedia" not in line:
            continue
        allowed = (
            "GICS" in line
            or "no longer" in line
            or "at the time" in line
            or "supplies" in line
            or "community-edited" in line
            or "retained for" in line
            or "long after the source moved" in line
            or "may only appear" in line
            or "the membership source" in line
        )
        if not allowed:
            offenders.append((lineno, line.strip()))
    assert not offenders, (
        "these lines still present Wikipedia as the membership source "
        f"(it moved to the SSGA holdings files in I2812): {offenders}"
    )
