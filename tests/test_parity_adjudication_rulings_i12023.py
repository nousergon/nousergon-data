"""The Crucible v2 rulings of 2026-10-06 on `alpha-engine-config-I12023`, as regression tests.

issuecomment-6023457908 (row 6): the collector's SCHEDULED next-day regrade is
the cell re-compared after settlement, against the vendor, exactly; the frozen
shadow value stays recorded. issuecomment-6024224623: (1) the writer's declared
``int()`` cast is the volume quantum, cited by file:line at the regrade's
commit; (3) a FRED release clears on ``released_within: [lower, upper]``, never
an inferred instant; (4) a recompute proof lets a path cite an input that moved
inside the input tolerance; and input groups, each member graded, each path
backed by a recompute.

None of these is a tolerance: every test below that loosens a condition by a
hair expects red.
"""

from __future__ import annotations

import copy
import hashlib
import json

import pytest

from data_gate import parity_adjudication as adj
from data_gate.cutover import CUTOVER_UTC

KEY = "arcticdb/universe-like.json"
SHA = "9400b02a25bce59dbf8654bed02dfac3ae3d9900"
OTHER_SHA = "55d3896e" + "0" * 32

# 16:08 ET on the bar's day: provisional at v1's read (`dates.bar_settlement`).
V1_READ = "2026-09-28T20:08:48Z"
# 08:01 ET the next morning: the D18 scheduled regrade, settled.
REGRADE_WRITTEN = "2026-09-29T12:01:00Z"


def _grade(entry: dict, breaches: int) -> adj.ExceptionGrade:
    report = {"keys": [{"key": KEY, "verdict": "mismatch", "values": {"breaches": breaches}}]}
    report_bytes = json.dumps(report, sort_keys=True).encode()
    record = {
        "schema_version": adj.ADJUDICATION_SCHEMA_VERSION,
        "supersedes": None,
        "report": {"key": "parity/2026-09-28.json", "sha256": hashlib.sha256(report_bytes).hexdigest()},
        "exceptions": [dict(entry, key=KEY, kind="mismatch")],
    }
    graded = adj.grade_record(
        record, record_key="parity_adjudication/2026-09-28/0001.json", report_key="parity/2026-09-28.json",
        report=report, report_bytes=report_bytes, previous_record_key=None, cutover_utc=CUTOVER_UTC,
    )
    assert not graded.problem, graded.problem
    (grade,) = graded.grades
    return grade


def _vendor(value) -> dict:
    return {"source_kind": adj.SETTLED_SOURCE_VENDOR, "vendor": "polygon-grouped-daily",
            "retrieved_at_utc": "2026-10-06T14:00:00Z", "value": value}


def _bar(ident, *, v1, shadow, settled, field="Close", read=V1_READ):
    item = {
        "id": ident, "kind": adj.INPUT_KIND_BAR, "symbol": ident.split(":")[1], "field": field,
        "source_key": f"arcticdb:universe/{ident}", "source_version_id": "412",
        "row_date": "2026-09-28", "v1_read_utc": read, "v1_value": v1, "shadow_value": shadow,
    }
    if settled is not None:
        item["settled"] = _vendor(settled)
    return item


# --------------------------------------------------------------------------
# 1. the regraded settling value
# --------------------------------------------------------------------------

def _volume(*, regraded=2194085, vendor=2194085.662907, shadow=2140233, cast=True, **regrade_over):
    item = _bar("2026-09-28:A:Volume", v1=2101000, shadow=shadow, settled=vendor, field="Volume")
    regrade = {
        "value": regraded,
        "key": "arcticdb:universe/A",
        "version_id": "418",
        "written_utc": REGRADE_WRITTEN,
        "run": {"run_id": "01M3PFACDJ8AF06T5N97R6P86W", "trigger": "scheduled", "code_sha": SHA,
                "manifest_key": "data_collection/runs/D18/2026-09-28/01M3PFACDJ8AF06T5N97R6P86W.json"},
    }
    if cast:
        regrade["cast"] = {
            "rule": adj.CAST_TRUNC,
            "vendor_value": vendor,
            "vendor_trunc": int(vendor),
            "citations": [
                {"file": "collectors/daily_closes.py", "line": 1976, "commit_sha": SHA},
                {"file": "builders/daily_append.py", "line": 1354, "commit_sha": SHA},
            ],
        }
    regrade.update(regrade_over)
    item["regrade"] = regrade
    return item


def _volume_entry(item) -> dict:
    return {"inputs": [item], "attributions": [{"path": "A.Volume", "inputs": [item["id"]]}]}


def test_a_regrade_equal_to_the_vendor_clears_while_the_frozen_shadow_value_stays_recorded():
    item = _volume(regraded=3032931, vendor=3032931, cast=False)  # shadow 2140233 is the frozen, provisional cell
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.CLEARED, grade.detail
    assert item["shadow_value"] == 2140233


def test_a_settling_cell_whose_regraded_value_still_differs_from_the_vendor_stays_red():
    """Ruling 6023457908 condition 5: the comparator's regression test."""
    grade = _grade(_volume_entry(_volume(regraded=2194000, vendor=2194085, cast=False)), 1)
    assert grade.verdict == adj.BREACH
    assert "still differs from the vendor" in grade.detail


def test_the_regrade_not_the_frozen_shadow_value_is_compared_with_the_vendor():
    # The frozen shadow value happens to equal the vendor; the regrade does not. Still red.
    grade = _grade(_volume_entry(_volume(regraded=2194000, vendor=2194085, shadow=2194085, cast=False)), 1)
    assert grade.verdict == adj.BREACH


def test_a_regrade_is_compared_exactly_never_within_the_parity_band():
    # 1 share in ~3M is inside rel 1e-6, and still a breach for a regrade.
    grade = _grade(_volume_entry(_volume(regraded=3032930, vendor=3032931, cast=False)), 1)
    assert grade.verdict == adj.BREACH


@pytest.mark.parametrize("trigger", ["manual", "backfill", None])
def test_a_regrade_by_a_non_scheduled_run_is_invalid(trigger):
    item = _volume()
    item["regrade"]["run"]["trigger"] = trigger
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "scheduled" in grade.detail


def test_a_regrade_without_its_run_id_is_invalid():
    item = _volume()
    del item["regrade"]["run"]["run_id"]
    assert _grade(_volume_entry(item), 1).verdict == adj.INVALID


def test_a_regrade_written_before_settlement_is_invalid():
    # 17:00 ET on the bar's own day: before dates.SETTLED_AFTER_ET.
    grade = _grade(_volume_entry(_volume(written_utc="2026-09-28T21:00:00Z")), 1)
    assert grade.verdict == adj.INVALID and "before" in grade.detail and "settled" in grade.detail


@pytest.mark.parametrize("missing", ["key", "version_id", "written_utc", "value"])
def test_a_regrade_must_name_its_store_version_and_write_time(missing):
    item = _volume()
    del item["regrade"][missing]
    assert _grade(_volume_entry(item), 1).verdict == adj.INVALID


def test_a_regrade_is_graded_against_a_vendor_read_only():
    item = _volume()
    item["settled"] = {"source_kind": adj.SETTLED_SOURCE_V1_LATER, "source_key": "k", "source_version_id": "v",
                       "source_last_modified_utc": "2026-09-28T22:00:00Z", "value": 2194085,
                       "same_definition": True, "definition_note": "n"}
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "vendor" in grade.detail


def test_a_release_cannot_carry_a_regrade():
    item = _volume()
    item["kind"] = adj.INPUT_KIND_RELEASE
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "regrade" in grade.detail


def test_a_regrade_awaiting_its_vendor_read_is_pending():
    item = _volume()
    del item["settled"]
    assert _grade(_volume_entry(item), 1).verdict == adj.PENDING


# --------------------------------------------------------------------------
# 2. the volume quantum: the writer's declared int() cast
# --------------------------------------------------------------------------

def test_a_regrade_equal_to_trunc_vendor_under_a_cited_cast_is_quantized_equal_and_clears():
    grade = _grade(_volume_entry(_volume()), 1)
    assert grade.verdict == adj.CLEARED, grade.detail
    assert grade.quantized == 1 and "quantized_equal" in grade.detail


def test_trunc_mismatch_is_a_breach():
    # 2194086 is Polygon rounded half-up — not the writer's cast.
    grade = _grade(_volume_entry(_volume(regraded=2194086)), 1)
    assert grade.verdict == adj.BREACH and "trunc" in grade.detail


def test_a_regrade_off_by_more_than_the_quantum_is_a_breach():
    grade = _grade(_volume_entry(_volume(regraded=2194084)), 1)
    assert grade.verdict == adj.BREACH


@pytest.mark.parametrize("citations", [None, []])
def test_a_missing_cast_citation_is_invalid(citations):
    item = _volume()
    if citations is None:
        del item["regrade"]["cast"]["citations"]
    else:
        item["regrade"]["cast"]["citations"] = citations
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "citation" in grade.detail


def test_a_cast_cited_at_another_commit_than_the_regrades_is_invalid():
    item = _volume()
    item["regrade"]["cast"]["citations"][1]["commit_sha"] = OTHER_SHA
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "commit" in grade.detail


def test_a_cast_without_the_regrades_commit_is_invalid():
    item = _volume()
    del item["regrade"]["run"]["code_sha"]
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "code_sha" in grade.detail


@pytest.mark.parametrize("bad", [{"file": "collectors/daily_closes.py"}, {"line": 1976}, {"file": "x.py", "line": 0}])
def test_a_cast_citation_needs_file_and_line(bad):
    item = _volume()
    item["regrade"]["cast"]["citations"] = [dict(bad, commit_sha=SHA)]
    assert _grade(_volume_entry(item), 1).verdict == adj.INVALID


def test_the_recorded_trunc_and_vendor_value_must_be_the_real_ones():
    item = _volume()
    item["regrade"]["cast"]["vendor_trunc"] = 2194086
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "vendor_trunc" in grade.detail

    item = _volume()
    item["regrade"]["cast"]["vendor_value"] = 2194085.6
    grade = _grade(_volume_entry(item), 1)
    assert grade.verdict == adj.INVALID and "vendor_value" in grade.detail


def test_an_undeclared_cast_rule_is_invalid():
    item = _volume()
    item["regrade"]["cast"]["rule"] = "round_half_up"
    assert _grade(_volume_entry(item), 1).verdict == adj.INVALID


def test_a_trunc_cast_on_a_non_integer_regrade_is_invalid():
    assert _grade(_volume_entry(_volume(regraded=2194085.0)), 1).verdict == adj.INVALID


# --------------------------------------------------------------------------
# 3. FRED: released_within [lower, upper]
# --------------------------------------------------------------------------

def _release(**settled_over):
    settled = {
        "source_kind": adj.SETTLED_SOURCE_VENDOR, "vendor": "FRED ALFRED",
        "retrieved_at_utc": "2026-10-06T14:00:00Z", "value": 5.17, "realtime_start": "2026-09-28",
        "released_within": {
            "lower": {"key": "market_data/macro/latest.json", "version_id": "v1ver",
                      "written_utc": "2026-09-28T20:13:55Z", "observation_absent": True},
            "upper": {"key": "staging/shadow/2026-09-28/market_data/macro/latest.json", "version_id": "shver",
                      "read_utc": "2026-09-28T22:42:00Z"},
        },
    }
    settled.update(settled_over)
    item = {
        "id": "FRED:DGS10@2026-09-25", "kind": adj.INPUT_KIND_RELEASE, "symbol": "DGS10", "field": "observation",
        "source_key": "market_data/macro/latest.json", "source_version_id": "v1ver", "row_date": "2026-09-25",
        "v1_read_utc": "2026-09-28T20:13:55Z", "v1_value": None, "shadow_value": 5.17, "settled": settled,
    }
    return {"inputs": [item], "attributions": [{"path": "$.series.DGS10", "inputs": [item["id"]]}]}


def _lower(entry):
    return entry["inputs"][0]["settled"]["released_within"]["lower"]


def test_a_release_inside_its_interval_with_the_shadows_value_clears():
    grade = _grade(_release(), 1)
    assert grade.verdict == adj.CLEARED, grade.detail


def test_released_within_lower_bound_missing_is_invalid():
    entry = _release()
    del entry["inputs"][0]["settled"]["released_within"]["lower"]
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "lower" in grade.detail


@pytest.mark.parametrize("field", ["key", "version_id", "written_utc"])
def test_the_lower_bound_needs_key_version_and_write_time(field):
    entry = _release()
    del _lower(entry)[field]
    assert _grade(entry, 1).verdict == adj.INVALID


def test_the_lower_bound_must_show_the_observation_absent():
    entry = _release()
    _lower(entry)["observation_absent"] = False
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "ABSENT" in grade.detail


def test_the_lower_bound_is_v1s_object_never_the_collectors():
    entry = _release()
    _lower(entry)["written_utc"] = "2026-09-28T22:35:00Z"
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "collector" in grade.detail


def test_a_lower_bound_older_than_v1s_read_proves_nothing_about_it():
    entry = _release()
    _lower(entry)["written_utc"] = "2026-09-28T19:00:00Z"
    assert _grade(entry, 1).verdict == adj.INVALID


@pytest.mark.parametrize("field", ["key", "version_id", "read_utc"])
def test_the_upper_bound_needs_key_version_and_read_time(field):
    entry = _release()
    del entry["inputs"][0]["settled"]["released_within"]["upper"][field]
    assert _grade(entry, 1).verdict == adj.INVALID


@pytest.mark.parametrize("realtime_start", ["2026-09-29", "2026-09-27"])
def test_an_alfred_date_outside_the_interval_is_a_breach(realtime_start):
    grade = _grade(_release(realtime_start=realtime_start), 1)
    assert grade.verdict == adj.BREACH and "outside the release interval" in grade.detail


def test_an_alfred_date_is_never_widened_into_an_instant():
    entry = _release()
    del entry["inputs"][0]["settled"]["realtime_start"]
    assert _grade(entry, 1).verdict == adj.INVALID


def test_an_alfred_value_that_differs_from_the_shadows_is_a_breach():
    assert _grade(_release(value=5.18), 1).verdict == adj.BREACH


def test_an_instant_and_an_interval_together_are_invalid():
    grade = _grade(_release(released_at_utc="2026-09-28T21:00:00Z"), 1)
    assert grade.verdict == adj.INVALID and "not both" in grade.detail


# --------------------------------------------------------------------------
# 4. recompute-proof attribution (GNRC)
# --------------------------------------------------------------------------

def _recompute(ids_to_settled: dict, value, shadow_value, **over):
    rc = {
        "value": value,
        "shadow_value": shadow_value,
        "formula": {"ref": "features/technical.py:atr_pct", "commit_sha": SHA},
        "settled_inputs_sha256": adj.settled_inputs_digest(ids_to_settled),
    }
    rc.update(over)
    return rc


def _gnrc(*, recompute=True, value=0.0213, shadow_value=0.0213, v1=208.14, shadow=208.1401, settled=208.14, **over):
    item = _bar("2026-09-25:GNRC:Close", v1=v1, shadow=shadow, settled=settled)
    item["row_date"], item["v1_read_utc"] = "2026-09-25", "2026-09-25T20:07:56Z"
    attribution = {"path": "GNRC.atr_pct", "inputs": [item["id"]]}
    if recompute:
        attribution["recompute"] = _recompute({item["id"]: settled}, value, shadow_value, **over)
    return {"inputs": [item], "attributions": [attribution]}


def test_an_input_inside_the_input_tolerance_cannot_attribute_without_a_recompute():
    grade = _grade(_gnrc(recompute=False), 1)
    assert grade.verdict == adj.INVALID and "agree" in grade.detail


def test_a_recompute_that_reproduces_the_shadow_attributes_a_within_tolerance_input():
    grade = _grade(_gnrc(), 1)
    assert grade.verdict == adj.CLEARED, grade.detail


def test_a_recompute_that_does_not_reproduce_is_a_breach():
    grade = _grade(_gnrc(value=0.0219), 1)
    assert grade.verdict == adj.BREACH and "does not reproduce" in grade.detail


def test_a_recompute_still_needs_an_input_that_moved_at_all():
    grade = _grade(_gnrc(v1=208.14, shadow=208.14), 1)
    assert grade.verdict == adj.INVALID and "identical" in grade.detail


def test_a_recompute_from_values_other_than_the_settled_ones_is_invalid():
    entry = _gnrc()
    entry["attributions"][0]["recompute"]["settled_inputs_sha256"] = adj.settled_inputs_digest(
        {"2026-09-25:GNRC:Close": 208.1401})
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "settled_inputs_sha256" in grade.detail


@pytest.mark.parametrize("formula", [None, {"ref": "features/technical.py:atr_pct"}, {"commit_sha": SHA},
                                     {"ref": "x", "commit_sha": "abc"}])
def test_a_recompute_must_state_its_formulas_provenance(formula):
    entry = _gnrc()
    entry["attributions"][0]["recompute"]["formula"] = formula
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "formula" in grade.detail


def test_a_recompute_without_the_shadow_value_it_must_reproduce_is_invalid():
    entry = _gnrc()
    del entry["attributions"][0]["recompute"]["shadow_value"]
    assert _grade(entry, 1).verdict == adj.INVALID


def test_a_recompute_over_an_unread_input_is_pending():
    entry = _gnrc()
    del entry["inputs"][0]["settled"]
    assert _grade(entry, 1).verdict == adj.PENDING


# --------------------------------------------------------------------------
# 5. input groups
# --------------------------------------------------------------------------

def _xs(*, members_settled=(175.21, 42.85, 99.0), recompute=True, value=1.25, **group_over):
    """A cross-sectional z-score over three closes: two moved, one (C) did not."""
    a = _bar("2026-09-28:A:Close", v1=175.19, shadow=175.21, settled=members_settled[0])
    b = _bar("2026-09-28:B:Close", v1=42.87, shadow=42.85, settled=members_settled[1])
    c = _bar("2026-09-28:C:Close", v1=99.0, shadow=99.0, settled=members_settled[2])
    inputs = [a, b, c]
    group = {"definition": "every universe close on 2026-09-28 (cross-sectional z-score)",
             "members": [i["id"] for i in inputs]}
    group.update(group_over)
    attribution = {"path": "A.close_z_xs", "input_groups": ["XS:2026-09-28:Close"]}
    if recompute:
        settled = {i["id"]: i["settled"]["value"] for i in inputs if i.get("settled")}
        attribution["recompute"] = _recompute(settled, value, 1.25)
    return {"inputs": inputs, "input_groups": {"XS:2026-09-28:Close": group}, "attributions": [attribution]}


def test_an_input_group_with_every_member_graded_and_a_recompute_clears():
    grade = _grade(_xs(), 1)
    assert grade.verdict == adj.CLEARED, grade.detail


def test_an_input_group_with_a_breaching_member_is_a_breach():
    grade = _grade(_xs(members_settled=(175.30, 42.85, 99.0)), 1)
    assert grade.verdict == adj.BREACH


def test_a_member_that_did_not_move_is_still_graded():
    # C agrees between v1 and the shadow, but the vendor says otherwise: red.
    grade = _grade(_xs(members_settled=(175.21, 42.85, 99.5)), 1)
    assert grade.verdict == adj.BREACH


def test_an_input_group_with_an_ungraded_member_is_pending():
    entry = _xs()
    del entry["inputs"][1]["settled"]
    grade = _grade(entry, 1)
    assert grade.verdict == adj.PENDING


def test_an_input_group_without_a_recompute_is_invalid():
    grade = _grade(_xs(recompute=False), 1)
    assert grade.verdict == adj.INVALID and "recompute" in grade.detail


def test_an_input_group_whose_recompute_does_not_reproduce_is_a_breach():
    assert _grade(_xs(value=1.31), 1).verdict == adj.BREACH


def test_every_group_member_must_be_a_declared_input():
    entry = _xs()
    entry["input_groups"]["XS:2026-09-28:Close"]["members"].append("2026-09-28:D:Close")
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "not declared" in grade.detail


@pytest.mark.parametrize("over", [{"members": []}, {"definition": " "}])
def test_a_group_must_be_defined_and_enumerated(over):
    assert _grade(_xs(**over), 1).verdict == adj.INVALID


def test_a_path_citing_an_undefined_group_is_invalid():
    entry = _xs()
    entry["attributions"][0]["input_groups"] = ["XS:other"]
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "undeclared input group" in grade.detail


def test_a_group_in_which_nothing_moved_explains_nothing():
    entry = _xs()
    for item in entry["inputs"]:
        item["shadow_value"] = item["v1_value"]
        item["settled"]["value"] = item["v1_value"]
    rc = entry["attributions"][0]["recompute"]
    rc["settled_inputs_sha256"] = adj.settled_inputs_digest({i["id"]: i["v1_value"] for i in entry["inputs"]})
    grade = _grade(entry, 1)
    assert grade.verdict == adj.INVALID and "differs" in grade.detail


def test_a_group_and_explicit_inputs_compose_on_one_path():
    entry = _xs()
    vol = _volume()
    entry["inputs"].append(vol)
    settled = {i["id"]: i["settled"]["value"] for i in entry["inputs"]}
    entry["attributions"][0]["inputs"] = [vol["id"]]
    entry["attributions"][0]["recompute"] = _recompute(settled, 1.25, 1.25)
    grade = _grade(entry, 1)
    assert grade.verdict == adj.CLEARED, grade.detail

    broken = copy.deepcopy(entry)
    broken["inputs"][-1]["regrade"]["value"] = 2194086
    assert _grade(broken, 1).verdict == adj.BREACH


def test_settled_inputs_digest_is_order_independent_and_value_exact():
    assert adj.settled_inputs_digest({"a": 1.5, "b": 2}) == adj.settled_inputs_digest({"b": 2, "a": 1.5})
    assert adj.settled_inputs_digest({"a": 1.5}) != adj.settled_inputs_digest({"a": 1.5000001})
