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
    """REGRESSION GUARD (moved from sf-watch-reclaim-sweep-handler's own test): the
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
    re-points two of these schedules' targets).

    alpha-engine-config-I4988 (2026-07-31): the four groom dispatch schedules
    joined the same set when the run_window groom check was retired — a rule
    that never fires produces no dispatch execution, so the reconciler's
    trigger-health leg (which screens fires) cannot see it; existence+state
    here is the only wiring signal for it."""
    checks = REGISTRY["watch_plane_liveness"]["checks"]
    names = {c["schedule_name"] for c in checks if c["type"] == "scheduler_schedule_exists"}
    assert names == {
        "alpha-engine-alert-drain-0400utc",
        "alpha-engine-alert-drain-1000utc",
        "alpha-engine-alert-drain-1600utc",
        "alpha-engine-alert-drain-2200utc",
        "alpha-engine-ci-watch-canary-drill-weekly",
        "alpha-engine-sf-watch-canary-drill-weekly",
        "alpha-engine-scheduled-groom-0400-daily",
        "alpha-engine-scheduled-groom-1200-daily",
        "alpha-engine-scheduled-groom-2000-daily",
        "alpha-engine-scheduled-groom-sun0900-weekly",
    }


# ── model/wake/cadence declaration (config-I3293) ────────────────────────────


def test_every_routed_playbook_declares_model_wake_cadence():
    """config-I3293: the registry is the ONE place that answers 'what model
    runs this agent, what wakes it, on what cadence'. Every routed playbook
    (its agent is launched THROUGH the router, so the router can inject the
    declared model) must carry all three fields; the schema alone keeps them
    optional (inventory-only entries may omit `model` — e.g. groom's
    complexity-tiered per-run choice is deliberately not router-injected)."""
    for name, spec in REGISTRY["playbooks"].items():
        if not spec.get("routed"):
            continue
        assert spec.get("model"), (
            f"routed playbook {name!r} missing a registry-declared model "
            f"(config-I3293) — the run script's inline default would silently "
            f"become undeclared live config"
        )
        # alpha-engine-config-I4478: was `.startswith("claude-")`. That, the
        # schema pattern, and each executor's `_MODEL_RE` all independently
        # hardcoded Anthropic — four layers making it structurally impossible
        # for the registry (the declared SSoT for "what model runs this agent")
        # to express the fleet's own 2026-07-24 zero-Anthropic-model ruling.
        assert not spec["model"].startswith(("claude-", "anthropic")), (
            f"routed playbook {name!r} declares Anthropic model "
            f"{spec['model']!r} — forbidden fleet-wide by the 2026-07-24 ruling "
            f"(nous-ergon-ops/policies/llm-provider-model-policy.md)"
        )
        assert spec.get("wake"), f"routed playbook {name!r} missing wake declaration"
        assert spec.get("cadence"), f"routed playbook {name!r} missing cadence declaration"


def test_drain_wake_names_all_four_schedules():
    """The alert-drain wake declaration and the watch_plane_liveness
    scheduler checks must agree on the 4x-daily schedule set — a schedule
    added to one surface but not the other is drift."""
    wake_text = " ".join(REGISTRY["playbooks"]["alert-drain"]["wake"])
    checks = REGISTRY["watch_plane_liveness"]["checks"]
    drain_scheds = {
        c["schedule_name"] for c in checks
        if c["type"] == "scheduler_schedule_exists" and "alert-drain" in c["schedule_name"]
    }
    assert len(drain_scheds) == 4
    for sched in drain_scheds:
        hour_token = sched.rsplit("-", 1)[-1]  # e.g. '0400utc'
        assert hour_token in wake_text, (
            f"drain schedule {sched} not reflected in the wake declaration"
        )


# ── cadence liveness coverage (config#4474) ───────────────────────────────────


def test_every_cadenced_playbook_has_run_window_or_invocation_success():
    """Every Lambda-backed playbook with a non-event-driven cadence string
    MUST declare either a ``run_window`` or ``sf_watch_invocation_success``
    liveness check — a mature trigger with no covering check is a silent
    outage. Entries with ``trigger_type: github_actions_cron`` have no
    Lambda/run-artifact surface. Entries whose cadence explicitly documents
    a sibling probe (``liveness tick``/``probe`` in the cadence string) are
    exempt — the sibling is their coverage. (config#4474, overseer
    conformance audit 2026-07-27.)"""
    for name, spec in REGISTRY["playbooks"].items():
        if spec.get("trigger_type") == "github_actions_cron":
            continue
        cadence = spec.get("cadence")
        if not cadence or "event-driven" in cadence:
            continue
        # Exempt: cadence documents a sibling liveness probe (not a
        # run_window but independently covered, e.g. canary-replay).
        if "liveness tick" in cadence or "probe" in cadence:
            continue
        checks = spec.get("liveness", {}).get("checks", [])
        check_types = [c["type"] for c in checks]
        assert "run_window" in check_types or "sf_watch_invocation_success" in check_types, (
            f"playbook {name!r} has a non-event-driven cadence "
            f"({cadence!r}) but no run_window or invocation_success "
            f"liveness check — add one or annotate the cadence with "
            f"the coverage mechanism"
        )


# ── alert-class registry (config-I3211) ──────────────────────────────────────


def test_alert_classes_present_and_unique():
    """The alert-class registry exists and class ids are unique — this is
    the fleet's declared answer to 'what notifications exist and what
    responds to each' (Brian directive 2026-07-22)."""
    rows = REGISTRY.get("alert_classes")
    assert rows, "alert_classes section missing from playbooks.yaml"
    ids = [r["class"] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate alert class ids"


def test_no_undeclared_drain_blind_class():
    """Every intake:none row must carry a migration_issue (tracked path onto
    the bus) or an operator_reason (explicit page-only declaration). A
    drain-blind class with neither is the exact I3211 defect."""
    for r in REGISTRY["alert_classes"]:
        if r["intake"] == "none":
            assert r.get("migration_issue") or r.get("operator_reason"), (
                f"alert class {r['class']!r} is drain-blind (intake: none) "
                f"with no migration_issue or operator_reason"
            )


def test_alert_class_playbook_refs_exist():
    """A response of playbook:<name> must reference a real, routed playbook."""
    for r in REGISTRY["alert_classes"]:
        resp = r["response"]
        if resp.startswith("playbook:"):
            name = resp.split(":", 1)[1]
            spec = REGISTRY["playbooks"].get(name)
            assert spec and spec.get("routed"), (
                f"alert class {r['class']!r} responds via playbook {name!r} "
                f"which is missing or not routed"
            )


def test_known_bus_sources_have_rows():
    """Spot-pin the highest-value bus sources from the 2026-07-22 fleet
    inventory — the classes most recently exercised live. (The full
    cross-repo static completeness chokepoint is tracked separately; this
    keeps the registry honest for the sources we KNOW page.)"""
    sources = " ".join(r["source"] for r in REGISTRY["alert_classes"])
    for known in ("freshness-monitor", "overseer-dispatcher",
                  "research:alerts_handler",
                  "alpha-engine-backtester/optimizer/live_key_reconciliation.py",
                  "cloudwatch-alarm:*"):
        assert known in sources, f"known emitter {known!r} has no alert-class row"


def test_registry_model_passes_its_executor_validator():
    """LOCKSTEP: every routed playbook's declared `model` must actually be
    accepted by the executor Lambda that receives it.

    alpha-engine-config-I4478/I4516: the registry declared `claude-*` models
    while the run scripts had migrated to DeepSeek, and each executor's
    `_MODEL_RE` accepted ONLY `claude-*`. Nothing tied the three together, so
    the drift was invisible until an agent died on a spot box at 04:00 UTC.
    This test makes that class of drift a CI failure instead.
    """
    checked = 0
    for name, spec in REGISTRY["playbooks"].items():
        if not spec.get("routed") or not spec.get("model"):
            continue
        exec_dir = spec.get("executor_lambda_dir")
        assert exec_dir, f"routed playbook {name!r} has no executor_lambda_dir"
        src = (LAMBDAS_DIR / exec_dir / "index.py").read_text(encoding="utf-8")
        m = re.search(r'_MODEL_RE\s*=\s*re\.compile\(r"([^"]+)"\)', src)
        assert m, f"{exec_dir}/index.py has no _MODEL_RE to validate against"
        assert re.compile(m.group(1)).match(spec["model"]), (
            f"registry declares model {spec['model']!r} for playbook {name!r}, "
            f"but {exec_dir}'s _MODEL_RE ({m.group(1)!r}) REJECTS it — the "
            f"router would inject a value the executor refuses, and the agent "
            f"never launches"
        )
        checked += 1
    assert checked >= 3, f"expected >=3 routed playbooks with models, checked {checked}"


# Fleet ROUTING-TIER suffixes. These are LiteLLM/registry group aliases
# (LLM_MODEL_REGISTRY ids like `deepseek-v4-pro-max`, whose own `model:` field
# resolves to the real provider name `deepseek-v4-pro`). They are NOT provider
# API model names.
_ROUTING_ALIAS_SUFFIXES = ("-max", "-low", "-mid", "-high")


@pytest.mark.parametrize(
    "name",
    sorted(k for k, v in REGISTRY["playbooks"].items() if v.get("routed") and v.get("model")),
)
def test_registry_model_is_a_provider_name_not_a_routing_alias(name):
    """A routed playbook's `model` is passed STRAIGHT to `claude -p --model` on
    the spot box, which talks directly to the provider through the local egress
    proxy — there is no LiteLLM hop to resolve a fleet routing alias.

    alpha-engine-config-I4471: alert-drain shipped `deepseek-v4-pro-max` (a
    registry TIER id, not an API model) and every run died on

        400 The supported API model names are deepseek-v4-pro or
        deepseek-v4-flash, but you passed deepseek-v4-pro-max.

    The alias resolves to `deepseek-v4-pro` in LLM_MODEL_REGISTRY, but nothing
    performed that resolution on this path.
    """
    model = REGISTRY["playbooks"][name]["model"]
    bad = [s for s in _ROUTING_ALIAS_SUFFIXES if model.endswith(s)]
    assert not bad, (
        f"routed playbook {name!r} declares model {model!r}, which ends in the "
        f"routing-tier suffix {bad[0]!r} — that is a fleet registry ALIAS, not a "
        f"provider API model name. The spot box passes this value directly to "
        f"the provider and the call will 400. Use the resolved provider model "
        f"(e.g. 'deepseek-v4-pro')."
    )


def test_every_registry_bundling_lambda_redeploys_on_registry_change():
    """LOCKSTEP: if a Lambda's deploy.sh BUNDLES playbooks.yaml into its zip,
    that Lambda's deploy workflow must be path-triggered on the registry too.

    Otherwise a registry-only edit deploys nothing and the live Lambda keeps
    running on a stale copy — silently. That is exactly what happened on
    2026-07-27: #1064 fixed alert-drain's model in the registry; the liveness
    probe redeployed (its filter listed the registry) but the ROUTER did not
    (its filter listed only its own lambda dir), so the router kept injecting
    the broken model and the drain stayed down.

    The two existing `*_bundles_this_registry` tests pin the copy step; this
    pins the trigger that makes the copy actually reach production.
    """
    workflows = REPO_ROOT / ".github" / "workflows"
    checked = 0
    for lambda_dir in sorted(LAMBDAS_DIR.iterdir()):
        deploy_sh = lambda_dir / "deploy.sh"
        if not deploy_sh.is_file():
            continue
        if "overseer/playbooks.yaml" not in deploy_sh.read_text(encoding="utf-8"):
            continue
        wf = workflows / f"deploy-{lambda_dir.name}.yml"
        assert wf.is_file(), (
            f"{lambda_dir.name}/deploy.sh bundles the registry but there is no "
            f"{wf.name} to trigger a redeploy"
        )
        # Parse the YAML and read the ACTUAL push-paths list. A raw substring
        # check would match an explanatory comment mentioning the path and pass
        # even with the trigger missing (verified while writing this test).
        wf_doc = yaml.safe_load(wf.read_text(encoding="utf-8"))
        # PyYAML parses the bare key `on:` as the boolean True (YAML 1.1).
        triggers = wf_doc.get("on", wf_doc.get(True)) or {}
        paths = ((triggers.get("push") or {}).get("paths")) or []
        assert "infrastructure/overseer/playbooks.yaml" in paths, (
            f"{wf.name} push paths {paths} do not include "
            f"'infrastructure/overseer/playbooks.yaml', but {lambda_dir.name}/deploy.sh "
            f"BUNDLES that file — a registry-only change would leave this Lambda "
            f"running a stale registry"
        )
        checked += 1
    assert checked >= 2, (
        f"expected >=2 registry-bundling lambdas (router + liveness probe), found {checked}"
    )
