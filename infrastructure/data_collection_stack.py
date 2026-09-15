#!/usr/bin/env python3
"""data_collection_stack.py — lint, deploy arguments and live check for the
``nousergon-data-collection`` CloudFormation stack (alpha-engine-config-I10739).

Nous Ergon's data collection is its own component with its own schedule and
stack (alpha-engine-config architecture.d/146, Brian ruling 2026-09-14). This
module is the one implementation every caller uses — the PR-time lint, the
deploy script and the scheduled live check — so the three can never disagree
about what the template declares.

Subcommands::

    lint                 static checks, no credentials (exit 1 on any finding)
    get <field>          one deploy argument, printed bare, for the deploy script
    check-live           compare the live stack and schedules to this checkout
                         (boto3; exit 1 on drift, 2 when it cannot measure)

``check-live`` exists because "No changes to deploy" is indistinguishable from
success, and a merged-but-unapplied stack is a known fleet failure: the deploy
script calls it after every apply, and the workflow runs it weekly so a console
edit or a failed apply is red on a schedule rather than discovered.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infrastructure"
TEMPLATE = INFRA / "cloudformation" / "nousergon-data-collection.yaml"
DEFINITION = INFRA / "step-functions" / "data-collection.asl.json"
DISPATCHER = INFRA / "lambdas" / "data-spot-dispatcher" / "index.py"
PAUSE_MANIFEST = INFRA / "automation_pause.json"

STACK_NAME = "nousergon-data-collection"
DEFINITION_PREFIX = "infrastructure/nousergon-data-collection/"
STATE_PARAMETERS = ("CollectionState", "DailyHealState")
MARKET_TZ = "America/New_York"
INPUT_FIELDS = {"collection", "workloads", "require_trading_day", "verify_units"}
UNITS_DIR = REPO_ROOT / "registry.d" / "units"
COMPLETE = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
_ACCOUNT_LITERAL = re.compile(r"(?<!\d)\d{12}(?!\d)")
_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


# ── template loading ─────────────────────────────────────────────────────────
class _CfnLoader(yaml.SafeLoader):
    """SafeLoader that reads CloudFormation short-form tags as their long form
    and REJECTS aliases: CloudFormation refuses a template that uses them."""


def _construct_tag(loader: yaml.SafeLoader, suffix: str, node: yaml.Node):
    name = "Ref" if suffix == "Ref" else f"Fn::{suffix}"
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
        if suffix == "GetAtt" and isinstance(value, str):
            value = value.split(".", 1)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {name: value}


_CfnLoader.add_multi_constructor("!", _construct_tag)


def _refuse_alias(self, node):  # noqa: ANN001 — PyYAML composer hook
    raise yaml.composer.ComposerError(
        None, None, "YAML aliases are not accepted by CloudFormation", node.start_mark
    )


def load_template(path: Path = TEMPLATE) -> dict:
    text = path.read_text(encoding="utf-8")
    if re.search(r"^[^#\n]*:\s+[&*][A-Za-z]", text, re.MULTILINE):
        raise ValueError(f"{path}: YAML anchors/aliases are rejected by CloudFormation")
    return yaml.load(text, Loader=_CfnLoader)  # noqa: S506 — SafeLoader subclass


def parameter_defaults(tpl: dict) -> dict[str, str]:
    return {k: v.get("Default") for k, v in tpl.get("Parameters", {}).items()}


def resources_of_type(tpl: dict, rtype: str) -> dict[str, dict]:
    return {k: v for k, v in tpl["Resources"].items() if v["Type"] == rtype}


def schedules(tpl: dict) -> list[dict]:
    """Every schedule, flattened to what the checks and the live compare need."""
    groups = resources_of_type(tpl, "AWS::Scheduler::ScheduleGroup")
    defaults = parameter_defaults(tpl)
    out = []
    for logical, res in resources_of_type(tpl, "AWS::Scheduler::Schedule").items():
        p = res["Properties"]
        group_ref = p["GroupName"]
        group = (
            groups[group_ref["Ref"]]["Properties"]["Name"]
            if isinstance(group_ref, dict)
            else group_ref
        )
        state = p["State"]
        state_param = state["Ref"] if isinstance(state, dict) else None
        out.append(
            {
                "logical_id": logical,
                "name": p["Name"],
                "group": group,
                "qualified_name": f"{group}/{p['Name']}",
                "state_parameter": state_param,
                "declared_state": defaults.get(state_param) if state_param else state,
                "expression": p["ScheduleExpression"],
                "timezone": p.get("ScheduleExpressionTimezone"),
                "flexible_mode": p.get("FlexibleTimeWindow", {}).get("Mode"),
                "target_ref": (p["Target"].get("Arn") or {}).get("Ref"),
                "input": json.loads(p["Target"]["Input"]),
            }
        )
    return sorted(out, key=lambda s: s["name"])


# ── digests and deploy arguments ─────────────────────────────────────────────
def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def definition_key(path: Path = DEFINITION) -> str:
    return f"{DEFINITION_PREFIX}data-collection-{sha256_of(path)}.asl.json"


def deploy_fields() -> dict[str, str]:
    defaults = parameter_defaults(load_template())
    return {
        "stack-name": STACK_NAME,
        "definition-bucket": defaults["DefinitionS3Bucket"],
        "definition-key": definition_key(),
        "template-sha256": sha256_of(TEMPLATE),
        "definition-sha256": sha256_of(DEFINITION),
        **{f"param-{p}": defaults[p] for p in STATE_PARAMETERS},
    }


# ── static checks ────────────────────────────────────────────────────────────
def dispatcher_workloads(path: Path = DISPATCHER) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        target = getattr(node, "target", None) or (getattr(node, "targets", None) or [None])[0]
        if isinstance(target, ast.Name) and target.id == "_WORKLOADS":
            return {k.value for k in node.value.keys}  # type: ignore[union-attr]
    raise ValueError(f"{path}: no _WORKLOADS literal found")


def declared_units(directory: Path = UNITS_DIR) -> set[str]:
    """Every unit id with a descriptor, from the filenames alone.

    Filename-derived rather than YAML-parsed on purpose: `data_gate` already
    grades the filename against the in-file `unit_id`, so this lint needs no
    second opinion — it only needs to refuse a `verify_units` entry that names a
    unit nobody declared, which would grade nothing and read as a pass.
    """
    return {p.name.split("-", 1)[0] for p in directory.glob("*.yaml")}


def _state_maps(states: dict, where: str = "States"):
    yield where, states
    for name, st in states.items():
        for key in ("ItemProcessor", "Iterator"):
            if key in st:
                yield from _state_maps(st[key]["States"], f"{where}.{name}.{key}")
        for i, branch in enumerate(st.get("Branches", [])):
            yield from _state_maps(branch["States"], f"{where}.{name}.Branches[{i}]")


def asl_problems(asl: dict) -> list[str]:
    problems: list[str] = []
    starts = {"States": asl["StartAt"]}
    for name, st in asl["States"].items():
        for key in ("ItemProcessor", "Iterator"):
            if key in st:
                starts[f"States.{name}.{key}"] = st[key]["StartAt"]
    for where, states in _state_maps(asl["States"]):
        if where in starts and starts[where] not in states:
            problems.append(f"{where}: StartAt {starts[where]!r} is not a state")
        for name, st in states.items():
            targets = [st.get("Next"), st.get("Default")]
            targets += [c.get("Next") for c in st.get("Choices", [])]
            targets += [c.get("Next") for c in st.get("Catch", [])]
            for t in filter(None, targets):
                if t not in states:
                    problems.append(f"{where}.{name}: transition to unknown state {t!r}")
            if st["Type"] not in {"Choice", "Succeed", "Fail"} and not st.get("End") and not st.get("Next"):
                problems.append(f"{where}.{name}: non-terminal state with no Next")
    return problems


def pause_manifest_problems(sched: list[dict], manifest: dict) -> list[str]:
    """The declared state and automation_pause.json must agree, both directions.

    DISABLED needs a `pending` or `paused.scheduler_schedules` entry, or the
    4-hourly pause reconcile reports `undeclared-dark`; ENABLED needs a
    `not_paused` entry, or `automation_pause.py --enforce` would treat it as an
    undeclared trigger. Keeping them in one test makes the enable PR one change.
    """
    pending = {k for k in manifest.get("pending", {}) if not k.startswith("_")}
    paused = set(manifest.get("paused", {}).get("scheduler_schedules", {}))
    kept = {k for k in manifest.get("not_paused", {}) if not k.startswith("_")}
    problems = []
    for s in sched:
        q = s["qualified_name"]
        if s["declared_state"] == "DISABLED" and q not in pending | paused:
            problems.append(f"{q}: declared DISABLED but not in automation_pause.json pending/paused")
        if s["declared_state"] == "ENABLED" and q not in kept:
            problems.append(f"{q}: declared ENABLED but not in automation_pause.json not_paused")
        if s["declared_state"] == "ENABLED" and q in pending | paused:
            problems.append(f"{q}: declared ENABLED but still listed as pending/paused")
    return problems


def lint() -> list[str]:
    problems: list[str] = []
    try:
        tpl = load_template()
    except (ValueError, yaml.YAMLError) as exc:
        return [str(exc)]
    text = TEMPLATE.read_text(encoding="utf-8")
    asl_text = DEFINITION.read_text(encoding="utf-8")
    for label, body in (("template", text), ("definition", asl_text)):
        if _ACCOUNT_LITERAL.search(body):
            problems.append(f"{label}: contains a 12-digit account literal (public repo)")
    iam = [k for k, v in tpl["Resources"].items() if v["Type"].startswith("AWS::IAM::")]
    if iam:
        problems.append(f"template declares IAM resources {iam}; IAM lives in nous-ergon-ops")
    asl = json.loads(asl_text)
    problems += asl_problems(asl)
    placeholders = set(_PLACEHOLDER.findall(asl_text))
    machines = resources_of_type(tpl, "AWS::StepFunctions::StateMachine")
    for logical, res in machines.items():
        subs = set(res["Properties"].get("DefinitionSubstitutions", {}))
        if placeholders - subs:
            problems.append(f"{logical}: unsubstituted placeholders {sorted(placeholders - subs)}")
    workloads = dispatcher_workloads()
    units = declared_units()
    for s in schedules(tpl):
        where = s["qualified_name"]
        if s["state_parameter"] not in STATE_PARAMETERS:
            problems.append(f"{where}: State must Ref one of {STATE_PARAMETERS}")
        if s["timezone"] != MARKET_TZ:
            problems.append(f"{where}: ScheduleExpressionTimezone must be {MARKET_TZ}")
        if s["flexible_mode"] != "OFF":
            problems.append(f"{where}: FlexibleTimeWindow must be OFF")
        if s["target_ref"] not in machines:
            problems.append(f"{where}: target is not a state machine in this stack")
        if set(s["input"]) != INPUT_FIELDS:
            problems.append(f"{where}: Input fields {sorted(s['input'])} != {sorted(INPUT_FIELDS)}")
        unknown = [w for w in s["input"].get("workloads", []) if w not in workloads]
        if unknown or not s["input"].get("workloads"):
            problems.append(f"{where}: workloads {unknown or '[]'} not in the dispatcher's _WORKLOADS")
        verify_units = s["input"].get("verify_units", [])
        undeclared = [u for u in verify_units if u not in units]
        if undeclared:
            problems.append(
                f"{where}: verify_units {undeclared} have no descriptor under registry.d/units/; "
                "the completion check would grade nothing for them and read as a pass"
            )
        if not verify_units:
            problems.append(
                f"{where}: verify_units is empty. Every schedule's completion claim is its "
                "units' run manifests (alpha-engine-config-I10787); an empty list is a "
                "machine whose only completion claim is an SSM exit code"
            )
    problems += pause_manifest_problems(
        schedules(tpl), json.loads(PAUSE_MANIFEST.read_text(encoding="utf-8"))
    )
    return problems


# ── live check ───────────────────────────────────────────────────────────────
def live_findings(cfn, scheduler) -> list[str]:  # noqa: ANN001 — boto3 clients
    fields = deploy_fields()
    findings: list[str] = []
    stacks = cfn.describe_stacks(StackName=STACK_NAME)["Stacks"]
    stack = stacks[0]
    if stack["StackStatus"] not in COMPLETE:
        findings.append(f"stack {STACK_NAME} is {stack['StackStatus']}, not *_COMPLETE")
    tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
    for key in ("template-sha256", "definition-sha256"):
        if tags.get(key) != fields[key]:
            findings.append(
                f"stack tag {key}={tags.get(key)!r} but main carries {fields[key]!r}: "
                "the checked-in template/definition is not what is deployed"
            )
    for s in schedules(load_template()):
        live = scheduler.get_schedule(GroupName=s["group"], Name=s["name"])
        if live.get("State") != s["declared_state"]:
            findings.append(
                f"schedule {s['qualified_name']} is {live.get('State')} live but "
                f"{s['declared_state']} in the template"
            )
        if live.get("ScheduleExpression") != s["expression"]:
            findings.append(
                f"schedule {s['qualified_name']} expression {live.get('ScheduleExpression')!r} "
                f"!= {s['expression']!r}"
            )
    return findings


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    cmd = argv[0]
    if cmd == "lint":
        problems = lint()
        for p in problems:
            print(f"::error::{p}")
        print(f"data-collection lint: {len(problems)} finding(s)")
        return 1 if problems else 0
    if cmd == "get" and len(argv) == 2:
        fields = deploy_fields()
        if argv[1] not in fields:
            print(f"unknown field {argv[1]!r}; one of {sorted(fields)}", file=sys.stderr)
            return 2
        print(fields[argv[1]])
        return 0
    if cmd == "check-live":
        import boto3

        try:
            findings = live_findings(
                boto3.client("cloudformation"), boto3.client("scheduler")
            )
        except Exception as exc:  # noqa: BLE001 — reported as UNMEASURED, exit 2
            print(f"::error::check-live could not measure: {type(exc).__name__}: {exc}")
            return 2
        for f in findings:
            print(f"::error::{f}")
        print(f"data-collection check-live: {len(findings)} finding(s)")
        return 1 if findings else 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
