"""Per-environment parameters of the nousergon-data-collection stack (S-18,
alpha-engine-config-I12506).

Holds four properties:

1. every template parameter carries a declared decision (a new parameter
   without one fails here, not on a deploy);
2. PROD resolved through the environment registry is byte-for-byte what
   deploy-data-collection-stack.sh deploys today, so wiring the deploy path to
   this module cannot change prod;
3. STAGING's values come from the registry, never literals, and never name a
   prod store, a prod topic or a schedule that is on;
4. staging rendering refuses while a runtime gap is open, and each gap's probe
   still holds (a closed gap is deleted in the PR that closes it).
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

env = pytest.importorskip("nousergon_lib.env")

REPO = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO / "infrastructure" / "data_collection_env_params.py"
STACK_HELPER = REPO / "infrastructure" / "data_collection_stack.py"
DEPLOY_SCRIPT = REPO / "infrastructure" / "deploy-data-collection-stack.sh"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def mod():
    return _load("data_collection_env_params", MODULE_PATH)


@pytest.fixture(scope="module")
def stack():
    return _load("data_collection_stack_for_env_params", STACK_HELPER)


@pytest.fixture(scope="module")
def prod():
    return env.resolve("prod")


@pytest.fixture(scope="module")
def staging():
    return env.resolve("staging")


# ── 1. every parameter is declared ───────────────────────────────────────────
def test_every_template_parameter_is_declared(mod):
    r = mod.resolve("prod")
    assert r["undeclared"] == [], f"declare a per-environment decision for {r['undeclared']}"
    assert r["stale"] == [], f"remove declarations for parameters the template dropped: {r['stale']}"


def test_declared_classes_are_known(mod):
    assert {cls for cls, _ in mod.DECLARED.values()} <= {"registry", "state", "deploy", "shared"}


def test_deploy_params_have_no_template_default(mod):
    tpl = mod.template_parameters()
    for name, (cls, _) in mod.DECLARED.items():
        if cls == "deploy":
            assert "Default" not in tpl[name], f"{name} is deploy-computed; a template default would hide a miss"


def test_state_params_are_the_stack_helpers_state_parameters(mod, stack):
    state = {n for n, (cls, _) in mod.DECLARED.items() if cls == "state"}
    shared_states = {"ShadowSamedayState", "ShadowMorningState"}
    assert state | shared_states == set(stack.STATE_PARAMETERS)


# ── 2. prod is unchanged ─────────────────────────────────────────────────────
def test_prod_values_are_the_template_defaults(mod):
    tpl = mod.template_parameters()
    r = mod.resolve("prod")
    expected = {name: spec["Default"] for name, spec in tpl.items() if "Default" in spec}
    assert r["params"] == expected
    assert r["deploy"] == ["DefinitionS3Key"]


def test_prod_matches_what_the_deploy_script_deploys_today(mod, stack):
    r = mod.resolve("prod")
    fields = stack.deploy_fields()
    assert r["target"]["stack"] == fields["stack-name"] == stack.STACK_NAME
    assert r["params"]["DefinitionS3Bucket"] == fields["definition-bucket"]
    for p in stack.STATE_PARAMETERS:
        assert r["params"][p] == fields[f"param-{p}"]


def test_prod_target_is_the_registry_prod_account_and_region(mod, prod):
    assert mod.resolve("prod")["target"] == {
        "account": prod.account,
        "region": prod.region,
        "stack": prod.stack("data_collection"),
    }


def test_prod_has_no_runtime_gaps(mod):
    assert mod.resolve("prod")["gaps"] == []


def test_prod_render_is_every_defaulted_parameter(mod, capsys):
    assert mod.main(["render", "--env", "prod"]) == 0
    rendered = json.loads(capsys.readouterr().out)
    tpl = mod.template_parameters()
    assert rendered == [
        {"ParameterKey": name, "ParameterValue": spec["Default"]} for name, spec in tpl.items() if "Default" in spec
    ]


# ── 3. staging comes from the registry ───────────────────────────────────────
def test_staging_target_is_the_staging_account(mod, staging, prod):
    t = mod.resolve("staging")["target"]
    assert t == {"account": staging.account, "region": staging.region, "stack": staging.stack("data_collection")}
    assert t["account"] != prod.account


def test_staging_stages_the_definition_into_its_own_bucket(mod, staging, prod):
    bucket = mod.resolve("staging")["params"]["DefinitionS3Bucket"]
    assert bucket == staging.bucket("crucible")
    assert bucket not in {prod.bucket(role) for role in prod.roles("buckets")}, "staging may write no prod bucket"


def test_staging_alerts_go_to_the_muted_topic(mod, staging, prod):
    topic = mod.resolve("staging")["params"]["AlertsTopicName"]
    assert topic == staging.topic("alerts") == staging.topic("pages_muted"), "staging pages nobody (S-20)"
    assert topic != prod.topic("alerts")


def test_staging_schedules_are_all_off(mod):
    r = mod.resolve("staging")
    tpl = mod.template_parameters()
    states = [n for n, spec in tpl.items() if spec.get("AllowedValues") == ["DISABLED", "ENABLED"]]
    assert states, "the template's schedule-state parameters moved; update this test"
    for name in states:
        assert r["params"][name] == "DISABLED", f"{name} must be born DISABLED in staging"


def test_every_environment_has_a_definition_bucket_role(mod):
    assert set(mod.DEFINITION_BUCKET_ROLE) == set(env.environment_names())


def test_module_carries_no_environment_literals(mod, prod, staging):
    src = MODULE_PATH.read_text(encoding="utf-8")
    assert not re.search(r"(?<!\d)\d{12}(?!\d)", src), "an account id literal; resolve it from the registry"
    for e in (prod, staging):
        for role in e.roles("buckets"):
            assert e.bucket(role) not in src, f"bucket literal {e.bucket(role)!r}; use the role"
        for role in e.roles("topics"):
            assert f'"{e.topic(role)}"' not in src, f"topic literal {e.topic(role)!r}; use the role"
    assert "us-east-1" not in src, "a region literal; resolve it from the registry"


def test_staging_has_no_orchestration_stack():
    """S-18 deploys only the data-collection half to staging (design comment, 2026-10-25)."""
    with pytest.raises(env.UnknownRoleError):
        env.resolve("staging").stack("orchestration")


# ── 4. gaps refuse, and each probe still holds ───────────────────────────────
def test_staging_render_refuses_while_gaps_are_open(mod, capsys):
    assert mod.resolve("staging")["gaps"], "no gaps left: drop this test and wire the staging deploy"
    assert mod.main(["render", "--env", "staging"]) == 3
    assert "refusing" in capsys.readouterr().err


def test_staging_render_allow_gaps_is_review_only(mod, capsys):
    assert mod.main(["render", "--env", "staging", "--allow-gaps"]) == 0
    captured = capsys.readouterr()
    keys = [row["ParameterKey"] for row in json.loads(captured.out)]
    assert "DefinitionS3Key" not in keys
    assert captured.err.count("GAP (review only)") == len(mod.RUNTIME_GAPS)


def _gap_rows():
    m = _load("data_collection_env_params_rows", MODULE_PATH)
    return [pytest.param(name, probe, id=name) for name, _why, probe in m.RUNTIME_GAPS if probe is not None]


@pytest.mark.parametrize(("name", "probe"), _gap_rows())
def test_runtime_gap_probe_still_holds(name, probe, prod):
    path, needle, present = probe
    text = (REPO / path).read_text(encoding="utf-8")
    needle = needle.format(research=prod.bucket("research"))
    if present:
        assert needle in text, f"gap {name!r} looks closed ({needle!r} gone from {path}); delete its RUNTIME_GAPS row"
    else:
        assert needle not in text, f"gap {name!r} looks closed ({needle!r} now in {path}); delete its RUNTIME_GAPS row"


def test_gap_names_are_unique(mod):
    names = [name for name, _why, _probe in mod.RUNTIME_GAPS]
    assert len(names) == len(set(names))


def test_cli_table_and_target(mod, capsys, staging):
    assert mod.main(["table", "--env", "staging"]) == 0
    out = capsys.readouterr().out
    assert f"account={staging.account}" in out
    assert out.count("\nGAP ") == len(mod.RUNTIME_GAPS)
    assert mod.main(["target", "--env", "staging"]) == 0
    assert json.loads(capsys.readouterr().out)["stack"] == staging.stack("data_collection")


def test_deploy_path_does_not_read_this_module_yet():
    """Code only (S-18 slice 1): merging this module changes no deploy. The slice that
    wires it in deletes the ``deploy-script-env`` gap and this test together."""
    assert "data_collection_env_params" not in DEPLOY_SCRIPT.read_text(encoding="utf-8")
