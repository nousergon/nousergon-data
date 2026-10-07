"""Every unit the EOD collection machine runs declares the time it actually fires.

`alpha-engine-config-I11189` first pinned these fourteen descriptors (D19-D32)
to `ne-postclose-trading-pipeline`'s measured 16:00 ET start, because
`data_gate/evidence.py::_cycle` selects only manifests whose ``started`` falls
at or after the DECLARED fire instant: a declaration LATER than the real fire
discards every healthy manifest and the unit reads `run_record` UNMET.

The decoupled data cutover (`alpha-engine-config-I11269`) then moved these units
out of the v1 postclose SF into the standalone `ne-data-collection-eod`
machine, started by Scheduler entry
`nousergon-data-collection/data-collection-eod` at 18:15 ET. The descriptors
kept naming the v1 machine at 16:00, and the 6 h completion grace absorbed the
135-minute gap, so nothing went red — the declaration was simply false.

Measured 2026-10-02:

* live ``aws scheduler get-schedule --group-name nousergon-data-collection
  --name data-collection-eod`` -> ``State: ENABLED``,
  ``cron(15 18 ? * MON-FRI *)``, ``America/New_York``, ``verify_units`` D03 and
  D19-D32 (matching `infrastructure/cloudformation/nousergon-data-collection.yaml::EodSchedule`);
* D20's run manifests since the cutover start after that fire, never before it::

    data_collection/runs/D20/2026-09-29/...  started 2026-09-29T22:23:25Z (18:23 ET)
    data_collection/runs/D20/2026-10-01/...  started 2026-10-01T22:23:24Z (18:23 ET)

  (the last v1-run manifest, 2026-09-28, was written at 20:10Z = 16:10 ET).

**What this test can and cannot do.** It pins the declared owner and fire time
against the measurement above and against the in-repo schedule definition, so
the declaration cannot silently drift again in an edit. Reconciling against the
LIVE trigger is `data.phase1.triggers_reconciled`'s job
(`data_gate/trigger_reconcile.py`, which keys these units on ``started_by``).
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from data_gate.descriptors import load_units

REPO = Path(__file__).resolve().parents[1]

#: The standalone machine and the Scheduler entry that starts it.
EOD_OWNER = "ne-data-collection-eod"
EOD_STARTED_BY = "eventbridge-scheduler:nousergon-data-collection/data-collection-eod"

#: Measured 2026-10-02 from the live Scheduler entry (see module docstring). A
#: constant, not a lookup: a value derived from the thing it checks would check
#: nothing.
MEASURED_SCHEDULE = "weekdays 18:15 America/New_York"

#: The units the EOD machine owns: D19-D32, plus D03 (prices), which is in its
#: verify_units and, since nousergon-data-PR2016 dropped it from the weekly
#: check, graded only there (alpha-engine-config-I11832), plus D51 (the daily
#: panel, alpha-engine-config-I10791), the machine's last workload. D51 is not in
#: the eod-spine freshness family: it is not a verify_unit, so its deadline is
#: not the family's derived bound.
EOD_UNITS = {f"D{i}" for i in range(19, 33)} | {"D03", "D51"}

#: The family whose `freshness.deadline` is the EOD fire plus its writers' caps.
#: D03 keeps its own `weekly` freshness family: that axis is the freshness SLO's
#: grouping, not `trigger.owner`, and is deliberately not changed with the owner.
EOD_SPINE_UNITS = {f"D{i}" for i in range(19, 33)}


def _eod_units():
    return [u for u in load_units() if (u.raw.get("trigger") or {}).get("owner") == EOD_OWNER]


def test_the_eod_units_are_owned_by_the_standalone_machine():
    """Non-vacuity guard and the owner pin in one: exactly D19-D32, D03 and D51."""
    assert {u.unit_id for u in _eod_units()} == EOD_UNITS


def test_no_unit_still_names_the_v1_postclose_pipeline_as_owner():
    stale = sorted(
        u.unit_id
        for u in load_units()
        if (u.raw.get("trigger") or {}).get("owner") == "ne-postclose-trading-pipeline"
    )
    assert not stale, (
        f"{stale} still name ne-postclose-trading-pipeline as trigger.owner; it has not run "
        "a data stage since the decoupled cutover (alpha-engine-config-I11269)."
    )


def test_every_eod_unit_declares_the_measured_fire_time_and_starter():
    wrong = {
        u.unit_id: (u.raw["trigger"].get("schedule"), u.raw["trigger"].get("started_by"))
        for u in _eod_units()
        if u.raw["trigger"].get("schedule") != MEASURED_SCHEDULE
        or u.raw["trigger"].get("started_by") != EOD_STARTED_BY
    }
    assert not wrong, (
        f"these units run in {EOD_OWNER}, whose Scheduler entry was MEASURED firing at "
        f"{MEASURED_SCHEDULE!r}, but declare something else: {wrong}. If the schedule "
        "genuinely moved, re-measure it and update MEASURED_SCHEDULE in the same commit."
    )


def test_measured_schedule_matches_the_in_repo_schedule_definition():
    """The constant and the CloudFormation EodSchedule must agree, so a schedule
    edit in the template cannot leave every EOD descriptor stale."""

    class _CfnLoader(yaml.SafeLoader):
        pass

    _CfnLoader.add_multi_constructor("!", lambda loader, suffix, node: None)
    tpl = yaml.load(
        (REPO / "infrastructure/cloudformation/nousergon-data-collection.yaml").read_text(),
        Loader=_CfnLoader,
    )
    props = tpl["Resources"]["EodSchedule"]["Properties"]
    assert props["Name"] == "data-collection-eod"
    assert props["ScheduleExpression"] == "cron(15 18 ? * MON-FRI *)"
    assert props["ScheduleExpressionTimezone"] == "America/New_York"


def test_the_eod_spine_deadline_is_the_derived_worst_case_write_bound():
    """`freshness.deadline` for the eod-spine family is the fire plus the declared
    runtime ceiling of the workloads that write its units — the same rule the
    weekly family's Sat 13:00 uses (05:00 + the machine timeout, plan §2 row 1).

    Derived from the template and the dispatcher's caps, never remembered: when
    the fire moved 16:45 -> 18:15 the old 18:15 deadline silently became equal to
    the fire time, a clause no run could ever meet. A cron or cap change now
    fails here until the deadline moves with it.
    """
    from infrastructure import data_collection_stack as stack

    eod = {s["name"]: s for s in stack.schedules(stack.load_template())}["data-collection-eod"]
    match = re.match(r"cron\((\d+) (\d+) ", eod["expression"])
    assert match, eod["expression"]
    fire = int(match.group(2)) * 60 + int(match.group(1))
    worst = stack.worst_case_through_units(eod, eod["input"]["verify_units"])
    bound = fire + -(-worst // 60)
    expected = f"{bound // 60:02d}:{bound % 60:02d} America/New_York"

    declared = {
        u.unit_id: (u.raw.get("freshness") or {}).get("deadline")
        for u in _eod_units()
        if (u.raw.get("freshness") or {}).get("family") == "eod-spine"
    }
    assert set(declared) == EOD_SPINE_UNITS
    wrong = {uid: d for uid, d in declared.items() if d != expected}
    assert not wrong, (
        f"eod-spine units must declare freshness.deadline {expected!r} (the EOD fire plus the "
        f"declared caps of its unit writers); these do not: {wrong}"
    )
