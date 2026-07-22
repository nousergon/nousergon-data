"""Contract test for the Overseer playbook registry (alpha-engine-config-I2823).

Pins:
  1. ``infrastructure/overseer/playbooks.yaml`` validates against its shipped
     JSON Schema (``playbooks.schema.json``) — the registry is a versioned
     contract, not a loose config file.
  2. Cross-reference integrity: every playbook's ``executor_lambda_dir``
     exists in ``infrastructure/lambdas/`` and its ``executor_function``
     matches the fleet naming convention derived from that dir.
  3. Benign-reason lockstep: every ``benign_reasons`` entry appears as a
     literal ``"reason": "<value>"`` in the executor's ``index.py`` — a
     registry reason the executor can never return is dead config; an executor
     decline the registry doesn't know stays escalating (correct default), but
     a TYPO'd benign reason would silently page on a by-design decline.
  4. Kill-switch lockstep: ``kill_switch_env`` appears in the executor source.

Pins 2 and 4 above only apply to Lambda-backed entries (``trigger_type``
absent, or explicitly ``lambda_dispatch``). Entries with
``trigger_type: github_actions_cron`` (alpha-engine-config-I2928 Phase 3) have
NO Lambda at all — they are pure GitHub Actions scheduled workflows, usually
in a different repo — so they carry ``workflow_file``/``repo`` instead, which
this test module checks separately below.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
OVERSEER_DIR = REPO_ROOT / "infrastructure" / "overseer"
LAMBDAS_DIR = REPO_ROOT / "infrastructure" / "lambdas"

REGISTRY = yaml.safe_load((OVERSEER_DIR / "playbooks.yaml").read_text())
SCHEMA = json.loads((OVERSEER_DIR / "playbooks.schema.json").read_text())

# alpha-engine-config-I2928 Phase 3: split the playbook set into lambda-backed
# entries (trigger_type absent, or explicitly lambda_dispatch — the implicit
# default, so pre-existing entries with no trigger_type at all land here) vs.
# github_actions_cron entries (no Lambda exists for these at all).
LAMBDA_BACKED_PLAYBOOKS = sorted(
    name
    for name, spec in REGISTRY["playbooks"].items()
    if spec.get("trigger_type", "lambda_dispatch") != "github_actions_cron"
)
GHA_CRON_PLAYBOOKS = sorted(
    name
    for name, spec in REGISTRY["playbooks"].items()
    if spec.get("trigger_type") == "github_actions_cron"
)


def test_registry_validates_against_schema():
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.validate(REGISTRY, SCHEMA)


@pytest.mark.parametrize("name", LAMBDA_BACKED_PLAYBOOKS)
def test_executor_lambda_dir_exists(name):
    spec = REGISTRY["playbooks"][name]
    lambda_dir = LAMBDAS_DIR / spec["executor_lambda_dir"]
    assert (lambda_dir / "index.py").is_file(), (
        f"playbook {name!r}: executor_lambda_dir {spec['executor_lambda_dir']!r} "
        f"has no index.py under infrastructure/lambdas/"
    )


@pytest.mark.parametrize("name", LAMBDA_BACKED_PLAYBOOKS)
def test_executor_function_matches_dir_convention(name):
    spec = REGISTRY["playbooks"][name]
    assert spec["executor_function"] == f"alpha-engine-{spec['executor_lambda_dir']}", (
        f"playbook {name!r}: executor_function {spec['executor_function']!r} does not "
        f"follow the alpha-engine-<lambda-dir> fleet naming convention"
    )


@pytest.mark.parametrize(
    "name",
    sorted(k for k, v in REGISTRY["playbooks"].items() if v.get("routed")),
)
def test_benign_reasons_exist_in_executor_source(name):
    spec = REGISTRY["playbooks"][name]
    src = (LAMBDAS_DIR / spec["executor_lambda_dir"] / "index.py").read_text()
    returnable = set(re.findall(r'"reason":\s*"([a-z_]+)"', src))
    missing = set(spec["benign_reasons"]) - returnable
    assert not missing, (
        f"playbook {name!r}: benign_reasons {sorted(missing)} never appear as "
        f'literal "reason" values in {spec["executor_lambda_dir"]}/index.py — '
        f"typo'd benign reasons silently page on by-design declines"
    )


@pytest.mark.parametrize("name", LAMBDA_BACKED_PLAYBOOKS)
def test_kill_switch_env_exists_in_executor_source(name):
    spec = REGISTRY["playbooks"][name]
    src = (LAMBDAS_DIR / spec["executor_lambda_dir"] / "index.py").read_text()
    assert spec["kill_switch_env"] in src, (
        f"playbook {name!r}: kill_switch_env {spec['kill_switch_env']!r} not found "
        f"in {spec['executor_lambda_dir']}/index.py"
    )


# ── GitHub Actions cron entries (alpha-engine-config-I2928 Phase 3) ──────────
# These have no Lambda at all, so they carry workflow_file/repo instead of the
# executor_*/kill_switch_env fields checked above. Cross-repo file-existence
# checks are impractical from this test (the workflow lives in
# alpha-engine-config, this test runs in nousergon-data), so we assert shape
# instead: a sane workflow_file path pattern + a repo field for cross-repo
# entries.

WORKFLOW_FILE_PATTERN = re.compile(r"^\.github/workflows/[a-z0-9_-]+\.ya?ml$")


@pytest.mark.parametrize("name", GHA_CRON_PLAYBOOKS)
def test_gha_cron_entry_has_sane_workflow_file(name):
    spec = REGISTRY["playbooks"][name]
    workflow_file = spec.get("workflow_file")
    assert workflow_file, (
        f"playbook {name!r}: trigger_type github_actions_cron requires a "
        f"workflow_file (no Lambda exists for this entry to fall back on)"
    )
    assert WORKFLOW_FILE_PATTERN.match(workflow_file), (
        f"playbook {name!r}: workflow_file {workflow_file!r} does not look like "
        f"a .github/workflows/*.yml path"
    )


@pytest.mark.parametrize("name", GHA_CRON_PLAYBOOKS)
def test_gha_cron_entry_has_repo(name):
    spec = REGISTRY["playbooks"][name]
    assert spec.get("repo"), (
        f"playbook {name!r}: trigger_type github_actions_cron requires a repo "
        f"field — this registry lives in nousergon-data but the workflow may "
        f"live elsewhere, and the field disambiguates that for readers"
    )


@pytest.mark.parametrize("name", GHA_CRON_PLAYBOOKS)
def test_gha_cron_entry_has_no_lambda_fields(name):
    """A github_actions_cron entry has no Lambda — Lambda-specific fields
    should not be present (schema already forbids requiring them, but this
    pins the intent: don't half-fill a Lambda reference for an entry with
    none)."""
    spec = REGISTRY["playbooks"][name]
    for field in ("executor_function", "executor_lambda_dir", "kill_switch_env"):
        assert field not in spec, (
            f"playbook {name!r}: trigger_type github_actions_cron entries should "
            f"not carry {field!r} — there is no Lambda backing this entry"
        )


def test_router_bundles_this_registry():
    """The router's deploy.sh must copy THIS registry file into its zip —
    pin the copy line so a rename breaks CI, not the deploy."""
    deploy = (LAMBDAS_DIR / "overseer-dispatcher" / "deploy.sh").read_text()
    assert "overseer/playbooks.yaml" in deploy


# ── Liveness block contract (alpha-engine-config-I2831) ──────────────────────
# The registry also drives the registry-driven overseer-liveness-probe: each
# playbook's optional `liveness.checks` + the top-level `watch_plane_liveness`.


def test_liveness_probe_bundles_this_registry():
    """The overseer-liveness-probe's deploy.sh must copy THIS registry into its
    zip (the probe's check table) — same bundling contract as the router."""
    deploy = (LAMBDAS_DIR / "overseer-liveness-probe" / "deploy.sh").read_text()
    assert "overseer/playbooks.yaml" in deploy


def _sibling_dispatcher_pipeline_names() -> set[str]:
    """SF names in saturday-sf-watch-dispatcher's PIPELINES dict — the SSoT the
    sf-watch liveness pipeline list must mirror. (Was pinned inside the sf-watch
    probe's own test before I2831 moved the list into this registry.)"""
    text = (LAMBDAS_DIR / "saturday-sf-watch-dispatcher" / "index.py").read_text()
    start = text.index("PIPELINES: dict")
    end = text.index("\n}\n", start)
    block = text[start:end]
    # Only keys at the dict's own 4-space indent are pipeline names.
    return set(re.findall(r'^ {4}"([\w.-]+)":\s*\{', block, re.M))


def test_sf_watch_liveness_pipelines_lockstep_with_dispatcher():
    """REGRESSION GUARD (moved from sf-watch-liveness-probe's own test): the
    sf-watch playbook's liveness pipeline list (the eventbridge_rule
    ``expect_state_machines`` + ``state_machines_exist`` list — ONE YAML anchor)
    must exactly match saturday-sf-watch-dispatcher's PIPELINES registry. Drift
    would silently check a stale pipeline set — the exact class the probe exists
    to catch."""
    checks = REGISTRY["playbooks"]["sf-watch"]["liveness"]["checks"]
    ebr = next(c for c in checks if c["type"] == "eventbridge_rule")
    sme = next(c for c in checks if c["type"] == "state_machines_exist")
    # The anchor: the two lists are literally the same object after YAML load.
    assert ebr["expect_state_machines"] == sme["state_machines"]
    registry_pipes = set(ebr["expect_state_machines"])
    sibling = _sibling_dispatcher_pipeline_names()
    assert registry_pipes == sibling, (
        "sf-watch liveness pipeline list drifted from saturday-sf-watch-dispatcher's "
        f"PIPELINES — only-registry: {sorted(registry_pipes - sibling)}, "
        f"only-dispatcher: {sorted(sibling - registry_pipes)}"
    )


# ── sf_watch_invocation_success lockstep (alpha-engine-config-I2901) ─────────


def _sibling_dispatcher_watch_prefixes() -> dict[str, str]:
    """{state_machine_name: watch_prefix} parsed from saturday-sf-watch-
    dispatcher's PIPELINES dict — the SSoT the sf_watch_invocation_success
    check's ``pipelines`` (state_machine + watch_prefix pairs) must mirror.
    Scopes the ``watch_prefix`` search to each pipeline's OWN segment (bounded
    by the next top-level key, or end-of-dict) so a nested sub-dict (e.g. the
    weekday pipeline's ``fast_path`` block) can never be mistaken for a
    sibling's fields."""
    text = (LAMBDAS_DIR / "saturday-sf-watch-dispatcher" / "index.py").read_text()
    start = text.index("PIPELINES: dict")
    end = text.index("\n}\n", start)
    block = text[start:end]
    matches = list(re.finditer(r'^ {4}"([\w.-]+)":\s*\{', block, re.M))
    prefixes: dict[str, str] = {}
    for i, m in enumerate(matches):
        seg_end = matches[i + 1].start() if i + 1 < len(matches) else len(block)
        segment = block[m.end():seg_end]
        wp = re.search(r'"watch_prefix":\s*"([^"]+)"', segment)
        if wp:
            prefixes[m.group(1)] = wp.group(1)
    return prefixes


def test_sf_watch_invocation_success_pipelines_lockstep_with_dispatcher():
    """REGRESSION GUARD (alpha-engine-config-I2901): the sf_watch_invocation_
    success check's per-pipeline ``watch_prefix`` must exactly match
    saturday-sf-watch-dispatcher's PIPELINES mapping — a drifted prefix would
    make the check read the WRONG S3 key and either false-alarm or (worse)
    silently never find the real watch-log, defeating the whole check."""
    checks = REGISTRY["playbooks"]["sf-watch"]["liveness"]["checks"]
    inv = next(c for c in checks if c["type"] == "sf_watch_invocation_success")
    registry_map = {p["state_machine"]: p["watch_prefix"] for p in inv["pipelines"]}
    sibling_map = _sibling_dispatcher_watch_prefixes()
    assert registry_map == sibling_map, (
        "sf_watch_invocation_success pipelines drifted from saturday-sf-watch-"
        f"dispatcher's PIPELINES watch_prefix mapping — registry: {registry_map}, "
        f"dispatcher: {sibling_map}"
    )
    # Must cover exactly the same pipeline set as the eventbridge_rule/
    # state_machines_exist anchor — no silent partial coverage of the 3 SFs.
    ebr = next(c for c in checks if c["type"] == "eventbridge_rule")
    assert set(registry_map) == set(ebr["expect_state_machines"])


# ── scheduler_schedule_exists coverage (alpha-engine-config-I2906) ───────────


def test_watch_plane_covers_all_four_router_scheduler_schedules():
    """REGRESSION GUARD (alpha-engine-config-I2906): the 4 router-targeting
    EventBridge Scheduler schedules (alert-drain x2, weekly canary-drill x2)
    must each have a scheduler_schedule_exists entry under watch_plane_
    liveness — a deleted/disabled schedule is otherwise invisible (a
    DIFFERENT AWS resource from the classic `events` rules the
    eventbridge_rule check type covers). Name+state only, deliberately no
    target-ARN assertion (a concurrent workstream, alpha-engine-config-I2832,
    re-points two of these schedules' targets)."""
    checks = REGISTRY["watch_plane_liveness"]["checks"]
    names = {c["schedule_name"] for c in checks if c["type"] == "scheduler_schedule_exists"}
    assert names == {
        "alpha-engine-alert-drain-1000utc",
        "alpha-engine-alert-drain-2200utc",
        "alpha-engine-ci-watch-canary-drill-weekly",
        "alpha-engine-sf-watch-canary-drill-weekly",
    }
