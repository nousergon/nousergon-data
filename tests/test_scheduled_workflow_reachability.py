"""A scheduled workflow must run every check it declares, and say so when it fails.

alpha-engine-config-I9121. Three of this repo's five scheduled workflows were
permanently red on 2026-08-28 and no surface anywhere reported it:

    pause-check-alert.yml       33 of 33 runs failed  (never observed working)
    fault-injection-weekly.yml   4 of 4 SCHEDULED runs failed since 2026-07-30
    sf-arn-drift-check.yml       red on main, last success 18 runs back

Two defects of this file's concern came out of that, and both are about a
CLASS rather than about those three:

1.  FAIL-FAST HID NINE CHECKS BEHIND ONE.  `sf-arn-drift-check.yml` runs
    eleven independent comparisons of a different codified source against a
    different live AWS surface, in one job, with the default fail-fast. On run
    33217483446 the FIRST of them reported five stale alarm classifications
    and steps 6 and 8-15 never executed — including the unscoped
    `trigger_surface_drift.py` sweep nousergon-data-PR1563 had wired in an
    hour earlier, which has four real `marker-missing` findings live and has
    never once been reported by this workflow. A red workflow that hides every
    finding behind the first one cannot tell its reader whether one check is
    failing or ten.

2.  NO SCHEDULED WORKFLOW HAD AN ALERT PATH.  A failing cron posts no PR
    check, and none of the five called the reusable notifier that 31 other
    workflows in this repo already use. `pause-check-alert.yml` — whose entire
    purpose is to page on pause drift — failed 33 consecutive times in silence.

Both guards below are written against the CLASS, so a workflow added later
inherits them. The gate guard in particular is the one that matters: a check
step added without an `id` would be un-gated, its failure would silently pass
the job, and that is a worse defect than the fail-fast this replaced.

Every parse here uses a DUPLICATE-KEY-REJECTING loader. `yaml.safe_load`
silently keeps the last of two identical keys, which is exactly how
alpha-engine-config-I8729's workflow failed 100 of 100 runs with ZERO jobs
(a duplicate `with:` key startup-fails at parse) while the checker written for
that class read it as valid.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
WORKFLOWS = REPO / ".github" / "workflows"
DRIFT_CHECK = WORKFLOWS / "sf-arn-drift-check.yml"

NOTIFIER = "nousergon/nousergon-lib/.github/workflows/notify-ci-failure.yml@"

#: Steps in sf-arn-drift-check.yml that are NOT graded checks and are therefore
#: legitimately outside the gate. Keyed by step name so adding a step cannot
#: land here by accident.
NON_CHECK_STEPS = {
    # Repair, deliberately tolerated: it runs BEFORE the verify and its failure
    # is not the verdict (alpha-engine-config-I7023, repair-then-verify).
    "Paused-component alarm actions reconciled with the manifest (live)",
    # A report, not an assertion. `if: always()` already makes it reachable.
    "Publish the arming record",
    # Setup, not a check: installs a dependency the next step needs. Its own
    # failure already fails the job outright (no continue-on-error) rather
    # than being read as a finding, which is correct — a broken pip install
    # is an infrastructure break, not a drift finding (alpha-engine-config-
    # I10164 part 2).
    "Install boto3 for the floor-calibration check",
    # Log hygiene, not a check: masks the account id before the credentials
    # step so this public repo's run log never prints it (alpha-engine-
    # config-I10973). Its own failure already fails the job outright (no
    # continue-on-error) rather than being read as a finding.
    "Mask the account id in this public log",
}


class _StrictLoader(yaml.SafeLoader):
    """`yaml.SafeLoader`, but a repeated mapping key is an error.

    GitHub Actions rejects a duplicate key at workflow-parse time with a
    startup failure and zero jobs; `safe_load` accepts it. A checker that
    cannot see what the runner rejects is not checking the runner's input.
    """


def _no_duplicate_keys(loader, node, deep=False):
    seen: set = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark
            )
        seen.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
)


def load_workflow(path: pathlib.Path) -> dict:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader)


def triggers(doc: dict) -> dict:
    """The `on:` block. YAML 1.1 resolves a bare `on` key to the boolean True."""
    on = doc.get("on", doc.get(True))
    return on if isinstance(on, dict) else {}


def scheduled_workflows() -> list[pathlib.Path]:
    return sorted(
        p for p in WORKFLOWS.glob("*.yml") if "schedule" in triggers(load_workflow(p))
    )


# ── the gate: every check runs, and every check is graded ───────────────────


def gate_findings(doc: dict) -> list[str]:
    """Every way ``sf-arn-drift-check.yml``'s gate can fail to be a gate.

    Pure over a parsed document so a seeded regression can be graded without
    writing a broken workflow to disk.
    """
    findings: list[str] = []
    steps = doc["jobs"]["drift-check"]["steps"]

    gate = [s for s in steps if str(s.get("name", "")).startswith("Drift-check gate")]
    if len(gate) != 1:
        return [f"expected exactly one 'Drift-check gate' step, found {len(gate)}"]
    gate_body = gate[0].get("run", "") + str(gate[0].get("env", ""))
    if gate[0].get("if") != "always()":
        findings.append("the gate step must carry `if: always()` or a failed check skips it")

    for step in steps:
        name = step.get("name")
        if name is None or name == gate[0]["name"] or name in NON_CHECK_STEPS:
            continue
        if "run" not in step:
            continue  # `uses:` setup steps (checkout, OIDC, setup-python)
        step_id = step.get("id")
        if step_id is None:
            findings.append(
                f"check step {name!r} has no `id`, so the gate cannot read its outcome — "
                "its failure would pass the job silently"
            )
            continue
        if step.get("continue-on-error") is not True:
            findings.append(
                f"check step {name!r} is not `continue-on-error: true`, so it still "
                "fail-fasts the job and skips every check after it"
            )
        if f"steps.{step_id}.outcome" not in gate_body:
            findings.append(
                f"check step {name!r} (id={step_id}) is not read by the gate"
            )
    return findings


@pytest.fixture(scope="module")
def drift_check() -> dict:
    return load_workflow(DRIFT_CHECK)


def test_every_drift_check_runs_and_is_graded(drift_check):
    assert gate_findings(drift_check) == []


def test_the_gate_reads_the_trigger_surface_sweep(drift_check):
    """The sweep nousergon-data-PR1563 wired in, named explicitly.

    It is the last step in the file, so it was the one the fail-fast hid on
    every single run. A regression that drops it would otherwise only show up
    as a workflow that is quietly green.
    """
    steps = drift_check["jobs"]["drift-check"]["steps"]
    sweep = [s for s in steps if "trigger_surface_drift.py" in str(s.get("run", ""))]
    assert len(sweep) == 1, "the unscoped trigger-surface sweep is gone"
    assert sweep[0]["id"] == "trigger_surface_drift"
    assert sweep[0]["continue-on-error"] is True


def test_a_check_step_added_without_an_id_is_rejected(drift_check):
    """The seeded regression — the guard must FAIL on the shape it exists for.

    A check never observed to fail is not a check.
    """
    import copy

    seeded = copy.deepcopy(drift_check)
    seeded["jobs"]["drift-check"]["steps"].append(
        {"name": "A new live check nobody gated", "run": "python3 infrastructure/whatever.py"}
    )
    findings = gate_findings(seeded)
    assert any("has no `id`" in f for f in findings), findings


def test_a_check_step_left_fail_fast_is_rejected(drift_check):
    import copy

    seeded = copy.deepcopy(drift_check)
    for step in seeded["jobs"]["drift-check"]["steps"]:
        if step.get("id") == "pause_check":
            del step["continue-on-error"]
    findings = gate_findings(seeded)
    assert any("fail-fasts the job" in f for f in findings), findings


def test_a_check_step_the_gate_does_not_read_is_rejected(drift_check):
    import copy

    seeded = copy.deepcopy(drift_check)
    seeded["jobs"]["drift-check"]["steps"].append(
        {
            "name": "A new live check with an id the gate never reads",
            "id": "ungraded_new_check",
            "continue-on-error": True,
            "run": "python3 infrastructure/whatever.py",
        }
    )
    findings = gate_findings(seeded)
    assert any("is not read by the gate" in f for f in findings), findings


# ── the class: a scheduled workflow that fails must say so ──────────────────


def test_every_scheduled_workflow_has_a_failure_alert_path():
    """A failing cron posts no PR check and dispatches no agent.

    Without this the only reader is someone opening the Actions tab, which is
    how `pause-check-alert.yml` failed 33 consecutive times unnoticed.
    """
    missing = []
    for path in scheduled_workflows():
        doc = load_workflow(path)
        notify = doc.get("jobs", {}).get("notify-main-failure")
        if notify is None or NOTIFIER not in str(notify.get("uses", "")):
            missing.append(path.name)
    assert missing == [], (
        "scheduled workflows with no failure-alert path: " + ", ".join(missing)
    )


def test_the_alert_job_waits_on_every_other_job():
    """`needs:` must name every real job, or a failure in an un-named job
    leaves the notifier skipped and the failure silent again."""
    problems = []
    for path in scheduled_workflows():
        jobs = load_workflow(path)["jobs"]
        needs = jobs["notify-main-failure"].get("needs") or []
        needs = [needs] if isinstance(needs, str) else list(needs)
        expected = [j for j in jobs if j != "notify-main-failure"]
        if sorted(needs) != sorted(expected):
            problems.append(f"{path.name}: needs={sorted(needs)} expected={sorted(expected)}")
    assert problems == [], problems


def test_the_alert_job_is_gated_to_the_default_branch():
    """It must not fire on a pull_request run — `fault-injection-weekly.yml`
    and `gitleaks.yml` both carry a PR trigger, and a per-PR page is the alert
    storm config#2855 already fixed once."""
    for path in scheduled_workflows():
        cond = load_workflow(path)["jobs"]["notify-main-failure"]["if"]
        assert "failure()" in cond, path.name
        assert "refs/heads/main" in cond, path.name


def test_every_workflow_parses_with_duplicate_keys_rejected():
    """The alpha-engine-config-I8729 class, over the whole directory.

    A duplicate key is a startup failure with zero jobs — a workflow that
    reports nothing forever while every `safe_load`-based checker reads it as
    healthy.
    """
    for path in sorted(WORKFLOWS.glob("*.yml")):
        try:
            load_workflow(path)
        except yaml.YAMLError as exc:  # pragma: no cover - the failure IS the message
            pytest.fail(f"{path.name}: {exc}")


def test_the_duplicate_key_loader_actually_rejects_duplicates():
    """Seeded regression for the loader itself. `yaml.safe_load` accepts this
    document; the runner does not, and neither may we."""
    doc = "jobs:\n  a:\n    with: 1\n    with: 2\n"
    assert yaml.safe_load(doc) == {"jobs": {"a": {"with": 2}}}
    with pytest.raises(yaml.YAMLError):
        yaml.load(doc, Loader=_StrictLoader)
