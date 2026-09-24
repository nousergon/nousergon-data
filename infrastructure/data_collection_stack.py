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
                         (boto3; exit 1 on drift, 2 when it cannot measure) and
                         publish the verdict to s3://alpha-engine-research/
                         data_collection/deploy/check-live/latest.json on EVERY
                         run, failures included (exit 3 if only the publish
                         fails). --no-publish for a local read.

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
STATE_PARAMETERS = (
    "CollectionState",
    "DailyHealState",
    "ShadowSamedayState",
    "ShadowMorningState",
)
MARKET_TZ = "America/New_York"
STATE_MACHINE_INPUT_FIELDS = {"collection", "workloads", "require_trading_day", "verify_units"}
# alpha-engine-config-I11233: ShadowSamedaySchedule targets the dispatcher
# Lambda directly rather than a state machine, so its Input is the
# dispatcher's own direct-invoke contract, not the collection/workloads/
# verify_units shape every state-machine-targeting schedule uses.
LAMBDA_INPUT_FIELDS = {"workload"}
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
        target_arn = p["Target"].get("Arn") or {}
        target_ref = target_arn.get("Ref") if isinstance(target_arn, dict) else None
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
                "target_ref": target_ref,
                # A schedule either Refs a state machine in this stack (Arn:
                # {Ref: <logical id>}) or Subs the dispatcher Lambda's ARN
                # directly (Arn: {Fn::Sub: "...function:${DispatcherFunctionName}"}).
                # target_ref is None for the latter, which is what distinguishes
                # them — alpha-engine-config-I11233 is the first of the second kind.
                "target_kind": "state_machine" if target_ref else "lambda",
                "target_arn_raw": target_arn,
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


def _env_int_default(node: ast.AST) -> int | None:
    """``int(os.environ.get("X", "7200"))`` -> 7200, anything else -> None."""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "int"
        and node.args
        and isinstance(node.args[0], ast.Call)
        and len(node.args[0].args) == 2
        and isinstance(node.args[0].args[1], ast.Constant)
    ):
        return int(node.args[0].args[1].value)
    return None


def dispatcher_runtime_caps(path: Path = DISPATCHER) -> tuple[int, dict[str, int], int]:
    """``(default cap, per-workload caps, SSM-online budget)``, all in seconds.

    Read from the dispatcher's SOURCE, never imported (its module imports its
    launch stack). These are the numbers every "worst case" ordering claim about
    the collection schedules has to be derived from (alpha-engine-config-I11363):
    a workload is hard-stopped at its cap, and each launch first spends up to the
    SSM-online budget before the workload clock starts.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    default = overrides = ssm = None
    for node in ast.walk(tree):
        target = getattr(node, "target", None) or (getattr(node, "targets", None) or [None])[0]
        if not isinstance(target, ast.Name) or getattr(node, "value", None) is None:
            continue
        if target.id == "MAX_RUNTIME_SECONDS":
            default = _env_int_default(node.value)
        elif target.id == "SSM_ONLINE_BUDGET_SEC":
            ssm = _env_int_default(node.value)
        elif target.id == "_WORKLOAD_MAX_RUNTIME_SECONDS":
            overrides = {k.value: int(v.value) for k, v in zip(node.value.keys, node.value.values)}  # type: ignore[union-attr]
    if default is None or overrides is None or ssm is None:
        raise ValueError(f"{path}: could not read the runtime caps")
    return default, overrides, ssm


def worst_case_seconds(workloads: list[str], *, through: str, path: Path = DISPATCHER) -> int:
    """Worst-case seconds from a schedule's fire until ``through`` is hard-stopped.

    The collection state machine runs its workloads strictly in order
    (``RunWorkloads``, MaxConcurrency 1), so the bound for any one workload is
    the sum over it and every workload before it of (SSM-online budget + cap).
    """
    default, overrides, ssm = dispatcher_runtime_caps(path)
    upto = workloads[: workloads.index(through) + 1]
    return sum(ssm + overrides.get(w, default) for w in upto)


#: Which workload of each state-machine schedule writes each unit a consumer
#: waits on (alpha-engine-config-I11264 / -I11363). Declared once, here: the v1
#: readiness waits size their budgets from it, and the sameday-shadow ordering
#: check below bounds the EOD run with it. Completeness against the v1 waits is
#: asserted by tests/test_v1_collection_readiness_wait.py.
UNIT_WRITERS: dict[str, dict[str, str]] = {
    "data-collection-eod": {
        **{u: "post-market-data" for u in (
            "D03", "D19", "D20", "D21", "D22", "D23", "D24", "D25", "D26", "D27",
            "D28", "D29", "D30", "D31",
        )},
        "D32": "post-market-arctic-append",
    },
    "data-collection-morning": {"D17": "morning-enrich", "D18": "morning-arctic-append"},
    "data-collection-weekly": {
        "D17": "morning-enrich",
        **{u: "weekly-phase-one" for u in (
            "D01", "D02", "D03", "D04", "D05", "D06", "D07", "D08", "D10", "D11",
            "D12", "D13", "D14",
        )},
        "D34": "chronic-gap-heal",
    },
}


def worst_case_through_units(schedule: dict, units: list[str], *, path: Path = DISPATCHER) -> int:
    """Worst-case seconds from ``schedule``'s fire until every workload that
    writes one of ``units`` is hard-stopped (see :func:`worst_case_seconds`)."""
    workloads = schedule["input"]["workloads"]
    writers = UNIT_WRITERS[schedule["name"]]
    last = max((writers[u] for u in units), key=workloads.index)
    return worst_case_seconds(workloads, through=last, path=path)


def _cron_minutes(expression: str) -> int | None:
    """``cron(M H ? * MON-FRI *)`` -> minutes after local midnight."""
    m = re.fullmatch(r"cron\((\d+) (\d+) \?.*\)", expression)
    return int(m.group(2)) * 60 + int(m.group(1)) if m else None


def sameday_ordering_problems(sched: list[dict], *, path: Path = DISPATCHER) -> list[str]:
    """alpha-engine-config-I11363: the sameday shadow may not run beside a live
    EOD collection that could still be writing what it compares.

    Refused when BOTH are ENABLED and the shadow's cron does not clear the EOD
    cron plus the declared caps of every workload up to the last one writing an
    EOD verify_unit. Both crons are same-day America/New_York weekday times
    (checked by lint), so the comparison is in local minutes.
    """
    by_name = {s["name"]: s for s in sched}
    eod = by_name.get("data-collection-eod")
    shadow = by_name.get("data-collection-shadow-sameday")
    if not eod or not shadow:
        return ["sameday ordering: data-collection-eod or data-collection-shadow-sameday is missing"]
    if eod["declared_state"] != "ENABLED" or shadow["declared_state"] != "ENABLED":
        return []
    eod_start, shadow_start = _cron_minutes(eod["expression"]), _cron_minutes(shadow["expression"])
    if eod_start is None or shadow_start is None:
        return ["sameday ordering: unparseable cron on data-collection-eod or -shadow-sameday"]
    bound = eod_start + -(-worst_case_through_units(eod, eod["input"]["verify_units"], path=path) // 60)
    if shadow_start < bound:
        return [
            f"sameday ordering: data-collection-shadow-sameday fires at minute {shadow_start} "
            f"while data-collection-eod (ENABLED) may still be writing its verify_units until "
            f"minute {bound} ET — move the shadow past it or keep it DISABLED "
            "(alpha-engine-config-I11363)"
        ]
    return []


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
        if s["target_kind"] == "lambda":
            # alpha-engine-config-I11233: invokes the dispatcher directly, so
            # there is no state machine to check membership against and no
            # collection/workloads/verify_units contract to hold it to — its
            # own {"workload": ...} contract is checked below instead.
            raw = s["target_arn_raw"]
            sub = raw.get("Fn::Sub") if isinstance(raw, dict) else None
            if not sub or "${DispatcherFunctionName}" not in sub:
                problems.append(
                    f"{where}: lambda-target schedule does not Sub ${{DispatcherFunctionName}}"
                )
            if set(s["input"]) != LAMBDA_INPUT_FIELDS:
                problems.append(
                    f"{where}: Input fields {sorted(s['input'])} != {sorted(LAMBDA_INPUT_FIELDS)}"
                )
            workload = s["input"].get("workload")
            if workload not in workloads:
                problems.append(f"{where}: workload {workload!r} not in the dispatcher's _WORKLOADS")
            continue
        if s["target_ref"] not in machines:
            problems.append(f"{where}: target is not a state machine in this stack")
        if set(s["input"]) != STATE_MACHINE_INPUT_FIELDS:
            problems.append(
                f"{where}: Input fields {sorted(s['input'])} != {sorted(STATE_MACHINE_INPUT_FIELDS)}"
            )
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
    problems += sameday_ordering_problems(schedules(tpl))
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


#: Where every check-live run publishes its verdict, for the data gate's
#: `data.cutover_ready.stack_check_live` clause (alpha-engine-config-I10870).
#: The gate reads it relative to its store root `data_collection/`
#: (`data_gate.evidence.STACK_CHECK_LIVE_KEY`); a test pins the two together.
#: The writer, github-actions-data-collection-stack-deploy, holds s3:PutObject
#: on exactly this key (nous-ergon-ops infrastructure/iam/).
CHECK_LIVE_BUCKET = "alpha-engine-research"
CHECK_LIVE_KEY = "data_collection/deploy/check-live/latest.json"
CHECK_LIVE_SCHEMA = "data_collection_check_live.v1"


def _code_sha() -> str:
    import os
    import subprocess

    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha
    try:
        return subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown (git rev-parse failed)"


def check_live_verdict(findings: list[str], *, error: str | None, now=None) -> dict:  # noqa: ANN001
    """The published verdict. A run that could not measure is ``measured: false``
    and ``in_sync: false`` — never silent, never in sync."""
    import datetime as dt

    moment = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    return {
        "schema_version": CHECK_LIVE_SCHEMA,
        "as_of": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "code_sha": _code_sha(),
        "stack": STACK_NAME,
        "measured": error is None,
        "in_sync": error is None and not findings,
        "drift": list(findings),
        "error": error,
    }


def publish_verdict(s3, verdict: dict) -> None:  # noqa: ANN001 — boto3 client
    s3.put_object(
        Bucket=CHECK_LIVE_BUCKET,
        Key=CHECK_LIVE_KEY,
        Body=json.dumps(verdict, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


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

        publish = "--no-publish" not in argv[1:]
        findings: list[str] = []
        error: str | None = None
        try:
            findings = live_findings(
                boto3.client("cloudformation"), boto3.client("scheduler")
            )
        except Exception as exc:  # noqa: BLE001 — published as measured:false, exit 2
            error = f"{type(exc).__name__}: {exc}"
            print(f"::error::check-live could not measure: {error}")
        for f in findings:
            print(f"::error::{f}")
        print(f"data-collection check-live: {len(findings)} finding(s)")
        rc = 2 if error else (1 if findings else 0)
        if not publish:
            return rc
        verdict = check_live_verdict(findings, error=error)
        try:
            publish_verdict(boto3.client("s3"), verdict)
        except Exception as exc:  # noqa: BLE001 — fail loud: an unpublished verdict is exit 3
            print(
                f"::error::check-live could not publish its verdict to "
                f"s3://{CHECK_LIVE_BUCKET}/{CHECK_LIVE_KEY}: {type(exc).__name__}: {exc}"
            )
            return rc or 3
        print(f"data-collection check-live: verdict published to s3://{CHECK_LIVE_BUCKET}/{CHECK_LIVE_KEY}")
        return rc
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
