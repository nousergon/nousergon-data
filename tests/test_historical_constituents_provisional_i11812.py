"""Fix D: a Polygon-confirmed spin-off addition passes D02 PROVISIONALLY.

2026-10-03, alpha-engine-config-I11812. The weekly's D02 run degraded on
``observed added VYLR not in reference``: Corteva's seed spin-off, added to the
S&P 500 on its 2026-10-01 distribution date, which the reference changes table
had not caught up with. The only way through was a hand-written declaration in
``sp500_declared_spinoff_additions.json``. Brian ruled fix D the same day: one
such addition is accepted without a declaration when Polygon confirms it, under
four bounds these tests pin:

* VYLR-shaped data passes, provisionally, and says so;
* two unexplained additions fail as before;
* the acceptance expires 14 days after the observed addition;
* an addition Polygon does not confirm fails as before.

And it is never silent: the artifact, the run manifest (a guard reading) and the
phase marker all carry ``provisional_additions``.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import collectors.historical_constituents as hc
import weekly_collector as wc
from collectors.historical_constituents import ADDED, ConstituentChange
from shadow.run_state import RunStatePhaseRegistry

MSG = "observed added VYLR not in reference"
OBSERVED = [ConstituentChange("2026-10-01", "VYLR", ADDED)]
VYLR_POLYGON = {"ticker": "VYLR", "active": True, "list_date": "2026-10-01",
                "name": "Vylor Inc."}
NOW = "2026-10-03T12:00:00+00:00"


def _confirm(details):
    calls: list[str] = []

    def confirm(ticker):
        calls.append(ticker)
        return details

    confirm.calls = calls
    return confirm


# ── The pure decision ───────────────────────────────────────────────────────


def test_vylr_shaped_addition_passes_provisionally_with_a_deadline():
    confirm = _confirm(VYLR_POLYGON)
    out = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-03", confirm=confirm, now=NOW,
    )
    assert out.unexplained == []
    assert out.refused == []
    assert confirm.calls == ["VYLR"]
    [entry] = out.accepted
    assert entry["ticker"] == "VYLR"
    assert entry["confirmed_at"] == NOW
    assert entry["deadline"] == "2026-10-15"
    assert entry["observed_added_on"] == "2026-10-01"
    assert entry["polygon_list_date"] == "2026-10-01"
    assert entry["reason"].startswith(MSG) and "provisionally" in entry["reason"]


def test_other_disagreements_still_count_next_to_a_provisional_addition():
    found = [MSG, "reference removed ZZZ not observed"]
    out = hc.provisional_spinoff_additions(
        found, OBSERVED, today="2026-10-03", confirm=_confirm(VYLR_POLYGON), now=NOW,
    )
    assert out.unexplained == ["reference removed ZZZ not observed"]
    assert [a["ticker"] for a in out.accepted] == ["VYLR"]


def test_two_additions_fail_and_polygon_is_not_asked():
    found = [MSG, "observed added ZZZ not in reference"]
    observed = OBSERVED + [ConstituentChange("2026-10-01", "ZZZ", ADDED)]
    confirm = _confirm(VYLR_POLYGON)
    out = hc.provisional_spinoff_additions(
        found, observed, today="2026-10-03", confirm=confirm, now=NOW,
    )
    assert out.unexplained == found
    assert out.accepted == []
    assert confirm.calls == []
    assert "at most 1" in out.refused[0] and "['VYLR', 'ZZZ']" in out.refused[0]


def test_the_deadline_day_itself_still_passes():
    out = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-15", confirm=_confirm(VYLR_POLYGON), now=NOW,
    )
    assert out.unexplained == [] and len(out.accepted) == 1


def test_expiry_after_14_days_fails_without_asking_polygon():
    confirm = _confirm(VYLR_POLYGON)
    out = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-16", confirm=confirm, now=NOW,
    )
    assert out.unexplained == [MSG]
    assert out.accepted == []
    assert confirm.calls == []
    assert "EXPIRED 2026-10-15" in out.refused[0]


def test_a_real_classification_replaces_the_provisional_one():
    # The reference listing it (or a declaration) removes the disagreement
    # before this function sees it, so nothing is provisional and nothing
    # expires.
    out = hc.provisional_spinoff_additions(
        [], OBSERVED, today="2026-12-01", confirm=_confirm(VYLR_POLYGON), now=NOW,
    )
    assert out.unexplained == [] and out.accepted == [] and out.refused == []


@pytest.mark.parametrize(
    "details, why",
    [
        (None, "no reference record"),
        ({**VYLR_POLYGON, "active": False}, "inactive"),
        ({**VYLR_POLYGON, "list_date": "1999-03-15"}, "not a new listing"),
        ({**VYLR_POLYGON, "list_date": None}, "no usable list_date"),
    ],
)
def test_a_polygon_unconfirmed_addition_fails(details, why):
    out = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-03", confirm=_confirm(details), now=NOW,
    )
    assert out.unexplained == [MSG]
    assert out.accepted == []
    assert why in out.refused[0]


def test_a_polygon_failure_fails_and_never_leaks_the_key():
    def boom(ticker):
        raise RuntimeError(
            "500 for url https://api.polygon.io/v3/reference/tickers/VYLR?apiKey=SECRET123"
        )

    out = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-03", confirm=boom, now=NOW,
    )
    assert out.unexplained == [MSG] and out.accepted == []
    assert "SECRET123" not in out.refused[0]
    assert "apiKey=***" in out.refused[0]


def test_a_removal_is_never_a_candidate():
    found = ["observed removed VYLR not in reference"]
    confirm = _confirm(VYLR_POLYGON)
    out = hc.provisional_spinoff_additions(
        found, OBSERVED, today="2026-10-03", confirm=confirm, now=NOW,
    )
    assert out.unexplained == found and out.accepted == [] and confirm.calls == []


# ── collect(): the artifact, the result and the manifest guard ──────────────


def _collect(snapshots, details, current):
    rosters = hc.RosterSnapshots(snapshots=snapshots, skipped={})
    s3 = MagicMock()
    real = hc.declared_spinoff_exceptions
    with patch.object(hc, "load_roster_snapshots", return_value=rosters), \
         patch.object(hc, "_fetch_changes_table", return_value=(None, "u")), \
         patch.object(hc, "parse_changes_table", return_value=[]), \
         patch.object(hc, "resolve_renames", return_value=hc.RenameResolution()), \
         patch.object(hc, "declared_spinoff_exceptions",
                      side_effect=lambda f, o, *, as_of: real(
                          f, o, as_of=as_of, declarations=[])), \
         patch.object(hc, "_polygon_ticker_details", side_effect=lambda t: details), \
         patch.object(hc.boto3, "client", return_value=s3):
        out = hc.collect("bucket", current)
    return out, json.loads(s3.put_object.call_args.kwargs["Body"])


def _recent(days_ago: int) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def test_collect_passes_vylr_provisionally_and_publishes_it_everywhere():
    added = _recent(2)
    snaps = {_recent(4): ["A", "B"], added: ["A", "B", "VYLR"]}
    out, written = _collect(snaps, {**VYLR_POLYGON, "list_date": added}, ["A", "B", "VYLR"])

    assert out["status"] == "ok"
    assert out["n_reference_disagreements"] == 0
    assert out["n_provisional_additions"] == 1
    [entry] = out["provisional_additions"]
    assert {"ticker", "reason", "confirmed_at", "deadline"} <= set(entry)
    assert entry["ticker"] == "VYLR"
    assert written["attestation"]["status"] == "agreed"
    assert written["attestation"]["provisional_additions"] == [entry]
    assert written["quality"]["provisional_additions"] == [entry]

    [guard] = out["guards"]
    assert guard["guard"] == hc.PROVISIONAL_ADDITION_GUARD
    assert guard["key"] == "market_data/historical_constituents.json"
    assert "ticker=VYLR" in guard["detail"] and entry["deadline"] in guard["detail"]


def test_collect_with_two_additions_is_degraded():
    added = _recent(2)
    snaps = {_recent(4): ["A", "B"], added: ["A", "B", "VYLR", "ZZZ"]}
    out, written = _collect(snaps, {**VYLR_POLYGON, "list_date": added},
                            ["A", "B", "VYLR", "ZZZ"])
    assert out["status"] == "degraded"
    assert out["n_provisional_additions"] == 0
    assert "guards" not in out
    assert "at most 1" in out["detail"]
    assert written["attestation"]["provisional_additions"] == []


def test_collect_past_the_deadline_is_degraded():
    added = _recent(20)
    snaps = {_recent(25): ["A", "B"], added: ["A", "B", "VYLR"], _recent(1): ["A", "B", "VYLR"]}
    out, _ = _collect(snaps, {**VYLR_POLYGON, "list_date": added}, ["A", "B", "VYLR"])
    assert out["status"] == "degraded"
    assert out["reference_disagreements"] == [MSG]
    assert "EXPIRED" in out["detail"]


def test_collect_with_a_polygon_unconfirmed_addition_is_degraded():
    added = _recent(2)
    snaps = {_recent(4): ["A", "B"], added: ["A", "B", "VYLR"]}
    out, _ = _collect(snaps, None, ["A", "B", "VYLR"])
    assert out["status"] == "degraded"
    assert out["reference_disagreements"] == [MSG]
    assert "no reference record" in out["detail"]


def test_the_guard_reading_fits_the_run_manifest_contract():
    import nousergon_lib

    schema = json.loads(
        (Path(nousergon_lib.__file__).parent / "contracts" / "data_run_manifest.schema.json")
        .read_text()
    )
    accepted = hc.provisional_spinoff_additions(
        [MSG], OBSERVED, today="2026-10-03", confirm=_confirm(VYLR_POLYGON), now=NOW,
    ).accepted
    [guard] = hc.provisional_addition_guards(accepted, key="market_data/historical_constituents.json")
    shape = schema["$defs"]["GuardVerdict"]
    assert set(shape["required"]) <= set(guard) <= set(shape["properties"])
    assert guard["verdict"] in shape["properties"]["verdict"]["enum"]
    assert guard["mode"] in shape["properties"]["mode"]["enum"]
    assert len(guard["detail"]) <= shape["properties"]["detail"]["maxLength"]

    run_ctx = MagicMock()
    wc._record_collector_guards(run_ctx, {"guards": [guard]})
    run_ctx.record_guard.assert_called_once()
    assert run_ctx.record_guard.call_args.args == (hc.PROVISIONAL_ADDITION_GUARD,)


# ── The phase marker ────────────────────────────────────────────────────────


def _registry():
    s3 = MagicMock()
    reg = RunStatePhaseRegistry(date="2026-10-02", bucket="b", marker_prefix="data",
                                s3_client=s3, run_token="")
    return reg, s3


def _markers(s3):
    return [json.loads(c.kwargs["Body"]) for c in s3.put_object.call_args_list]


def test_the_phase_marker_carries_the_provisional_additions():
    entry = {"ticker": "VYLR", "reason": "r", "confirmed_at": NOW, "deadline": "2026-10-15"}
    reg, s3 = _registry()
    result = {"status": "ok", "provisional_additions": [entry]}
    out = wc._phase_body(
        reg, "historical_constituents", lambda: result, artifact_key=None,
        supports_auto_skip=False, verify_artifact_exists=False, bucket="b",
    )
    assert out is result
    [marker] = _markers(s3)
    assert marker["status"] == "ok"
    assert marker["provisional_additions"] == [entry]


def test_an_annotation_is_consumed_and_never_rewrites_the_marker_status():
    reg, s3 = _registry()
    reg.annotate_marker("historical_constituents", provisional_additions=[{"ticker": "X"}],
                        status="forged")
    with reg.phase("historical_constituents"):
        pass
    with reg.phase("historical_constituents"):
        pass
    first, second = _markers(s3)
    assert first["status"] == "ok" and first["provisional_additions"] == [{"ticker": "X"}]
    assert "provisional_additions" not in second


def test_a_phase_with_nothing_provisional_writes_the_marker_unchanged():
    reg, s3 = _registry()
    wc._phase_body(
        reg, "historical_constituents", lambda: {"status": "ok"}, artifact_key=None,
        supports_auto_skip=False, verify_artifact_exists=False, bucket="b",
    )
    [marker] = _markers(s3)
    assert "provisional_additions" not in marker
