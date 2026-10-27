#!/usr/bin/env python3
"""Per-environment parameters for the nousergon-data-collection stack (S-18,
alpha-engine-config-I12506; epic alpha-engine-config-I12488).

Staging deploys the SAME template as prod
(``infrastructure/cloudformation/nousergon-data-collection.yaml``). This module
is the parameter half of that deploy: for every template parameter it DECLARES
how each environment sets it, so a parameter added to the template without a
decision fails ``tests/test_data_collection_env_params.py``, not a deploy.

Code only. Nothing here calls AWS, and nothing reads it on a deploy path yet:
``deploy-data-collection-stack.sh`` still deploys prod from the template
defaults. That is deliberate (see RUNTIME_GAPS ``deploy-script-env``); the
tests prove that prod, resolved through this module, is byte-for-byte the
values that script deploys today, so wiring it in is a no-op for prod.

Every value that names an account, a region, a stack, a bucket or a topic is
resolved from the fleet environment registry through ``nousergon_lib.env``
(ENV-NO-LITERALS; source of truth nous-ergon-ops governance/environments.yaml).
This file carries ROLE names only.

Every parameter is one of:

- ``registry``  resolved from the registry for the environment (a role lookup);
- ``state``     a schedule state: prod keeps the template default, staging keeps
                every schedule DISABLED until its light run is enabled on purpose
                (the S-21 rule: a staging schedule is born off);
- ``deploy``    computed per deploy by the deploy script (no default);
- ``shared``    the template default holds in every environment, with the reason.

Staging boundary (plan section 3, ENV-PROD-OWNS-DATA): staging reads prod data
and writes only its own bucket; staging pages nobody (S-20). RUNTIME_GAPS lists
what no stack parameter can make true today. ``render --env staging`` refuses
(exit 3) while any is open, so the first staging deploy cannot happen on a
parameter set known to write a prod store.

Subcommands::

    table   [--env E]                 one line per parameter, then the target and gaps
    render  [--env E] [--allow-gaps]  CloudFormation ParameterKey/ParameterValue JSON
                                      (every parameter but the deploy-computed key)
    target  [--env E]                 {"account", "region", "stack"} JSON

``--env`` defaults to ``$NE_ENV``, else the registry default (prod).
``--registry PATH`` reads a local registry (e.g. nous-ergon-ops'
governance/environments.yaml) instead of the copy packaged with nousergon-lib.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INFRA = REPO_ROOT / "infrastructure"
TEMPLATE = INFRA / "cloudformation" / "nousergon-data-collection.yaml"

#: The registry stack role of this template.
STACK_ROLE = "data_collection"

#: The bucket ROLE the deploy stages the ASL definition into: the one bucket the
#: environment's deploy may write. Prod is the writer of record of research;
#: staging may write only its own crucible bucket (S-08 grants it research READ).
DEFINITION_BUCKET_ROLE = {"prod": "research", "staging": "crucible"}

#: The staging state of every schedule-state parameter.
STAGING_SCHEDULE_STATE = "DISABLED"

#: name -> (class, how). ``registry``: a key of ``_registry_values``; otherwise
#: the reason.
DECLARED: dict[str, tuple[str, str]] = {
    "CollectionState": (
        "state",
        "the eod, morning and weekly schedules; staging's light runs are enabled on purpose, never by default",
    ),
    "DailyHealState": ("state", "the daily-heal schedule; same rule as CollectionState"),
    "ShadowSamedayState": (
        "shared",
        "DISABLED in prod since the decoupled cutover (alpha-engine-config-I11269); staging has no v1 to compare",
    ),
    "ShadowMorningState": ("shared", "DISABLED in prod for the same reason as ShadowSamedayState"),
    "DefinitionS3Bucket": ("registry", "definition_bucket"),
    "DefinitionS3Key": (
        "deploy",
        "content-addressed key the deploy script computes (data_collection_stack.definition_key)",
    ),
    "DispatcherFunctionName": (
        "shared",
        "the same function name in each account; the staging SFN role (nous-ergon-ops#1839) may invoke only it",
    ),
    "AlertsTopicName": ("registry", "alerts_topic"),
    "StateMachineRoleName": (
        "shared",
        "the same role name in each account; staging's is in nous-ergon-ops infrastructure/tofu/staging (I12647)",
    ),
    "SchedulerRoleName": ("shared", "the same role name in each account; staging's as StateMachineRoleName"),
}

#: Staging behaviour no stack PARAMETER can change today. Each keeps
#: ``render --env staging`` refusing. ``probe`` is (path, needle, present): the
#: gap is still open while ``needle`` is (present=True) or is not (present=False)
#: in ``path``. ``{research}`` in a needle is prod's research bucket, resolved
#: from the registry. A probe that stops holding fails the tests, so a closed
#: gap is deleted here in the PR that closes it. ``None`` = declared, no probe.
RUNTIME_GAPS: tuple[tuple[str, str, tuple[str, str, bool] | None], ...] = (
    (
        "dispatcher-store",
        "the data-spot dispatcher's on-box commands pass --store s3://<prod research>/data_collection, so a "
        "staging run would write the prod store (denied by S-08; plan: staging outputs go to the staging bucket "
        "only). Needs the store derived from the registry and passed in the dispatcher's environment.",
        (
            "infrastructure/lambdas/data-spot-dispatcher/index.py",
            "--store s3://{research}/data_collection",
            True,
        ),
    ),
    (
        "dispatcher-run-log",
        "the dispatcher's run-log bucket defaults to prod research (DATA_COLLECTION_RUN_LOG_BUCKET).",
        (
            "infrastructure/lambdas/data-spot-dispatcher/index.py",
            '"DATA_COLLECTION_RUN_LOG_BUCKET", "{research}"',
            True,
        ),
    ),
    (
        "dispatcher-launch",
        "the dispatcher's spot launch defaults name prod-account resources (security group, key pair, box "
        "instance profile); staging needs its own, set through the dispatcher's environment.",
        (
            "infrastructure/lambdas/data-spot-dispatcher/index.py",
            'os.environ.get("DATA_SPOT_SECURITY_GROUP", "sg-',
            True,
        ),
    ),
    (
        "dispatcher-deploy",
        "deploy-data-spot-dispatcher.yml deploys the prod account only; no staging dispatcher exists for the "
        "staging state machines to invoke.",
        ("infrastructure/lambdas/data-spot-dispatcher/deploy.sh", "NE_ENV", False),
    ),
    (
        "check-live-publish",
        "data_collection_stack.py check-live publishes its verdict to prod research, which staging cannot "
        "write, and reads the stack by a fixed name.",
        ("infrastructure/data_collection_stack.py", 'CHECK_LIVE_BUCKET = "{research}"', True),
    ),
    (
        "deploy-script-env",
        "deploy-data-collection-stack.sh takes no environment: it passes only DefinitionS3Key and the four "
        "states and stages the definition into the template's default bucket. Needs it to read this module "
        "(prod unchanged, proven by the tests).",
        ("infrastructure/deploy-data-collection-stack.sh", "data_collection_env_params", False),
    ),
    (
        "replay-inputs",
        "S-18 acceptance is a green light run on REPLAYED vendor inputs; the collection workloads have no "
        "replay input mode and would call live vendors.",
        None,
    ),
    (
        "subset-universe",
        "staging declares universe scope=subset (registry `universe`); the collection workloads read the "
        "full roster.",
        None,
    ),
)


def template_parameters(template: Path = TEMPLATE) -> dict[str, dict]:
    """The template's Parameters block (CloudFormation tags tolerated)."""
    import yaml  # noqa: PLC0415

    class _Loader(yaml.SafeLoader):
        pass

    _Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    return yaml.load(template.read_text(encoding="utf-8"), Loader=_Loader)["Parameters"]


def _resolve(env_name: str | None, registry: Path | None):  # noqa: ANN202 — nousergon_lib.env.Environment
    from nousergon_lib import env  # noqa: PLC0415

    return env.resolve(env_name, registry=registry)


def _registry_values(e) -> dict[str, str]:  # noqa: ANN001 — nousergon_lib.env.Environment
    if e.name not in DEFINITION_BUCKET_ROLE:
        raise KeyError(f"no definition-bucket role declared for environment {e.name!r}")
    return {
        "definition_bucket": e.bucket(DEFINITION_BUCKET_ROLE[e.name]),
        "alerts_topic": e.topic("alerts"),
    }


def runtime_gaps(env_name: str) -> list[tuple[str, str]]:
    """The open runtime gaps for ``env_name`` (none for prod, which is what runs)."""
    if env_name == "prod":
        return []
    return [(name, why) for name, why, _probe in RUNTIME_GAPS]


def resolve(env_name: str | None = None, *, registry: Path | None = None, template: Path = TEMPLATE) -> dict:
    """Resolve every parameter for one environment.

    Returns ``{'env', 'target': {account, region, stack}, 'params': {name: value},
    'classes': {name: class}, 'deploy': [...], 'gaps': [(name, why)],
    'undeclared': [...], 'stale': [...]}``. ``params`` holds every parameter
    except the ``deploy`` ones.
    """
    e = _resolve(env_name, registry)
    tpl = template_parameters(template)
    values = _registry_values(e)
    out: dict = {
        "env": e.name,
        "target": {"account": e.account, "region": e.region, "stack": e.stack(STACK_ROLE)},
        "params": {},
        "classes": {},
        "deploy": [],
        "undeclared": sorted(set(tpl) - set(DECLARED)),
        "stale": sorted(set(DECLARED) - set(tpl)),
    }
    for name, spec in tpl.items():
        if name not in DECLARED:
            continue
        cls, how = DECLARED[name]
        out["classes"][name] = cls
        if cls == "registry":
            out["params"][name] = values[how]
        elif cls == "state":
            out["params"][name] = spec["Default"] if e.name == "prod" else STAGING_SCHEDULE_STATE
        elif cls == "shared":
            out["params"][name] = spec["Default"]
        else:
            out["deploy"].append(name)
    out["gaps"] = runtime_gaps(e.name)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="nousergon-data-collection per-environment parameters (code only).")
    ap.add_argument("cmd", choices=("table", "render", "target"))
    ap.add_argument("--env", default=None, help="environment (default: $NE_ENV, else the registry default)")
    ap.add_argument("--registry", type=Path, default=None, help="a local registry file instead of the packaged one")
    ap.add_argument("--template", type=Path, default=TEMPLATE)
    ap.add_argument("--allow-gaps", action="store_true", help="render anyway, for review only")
    args = ap.parse_args(argv)
    r = resolve(args.env, registry=args.registry, template=args.template)
    if r["undeclared"] or r["stale"]:
        print(f"undeclared parameters {r['undeclared']}, stale declarations {r['stale']}", file=sys.stderr)
        return 2
    if args.cmd == "target":
        print(json.dumps(r["target"], indent=2))
        return 0
    if args.cmd == "table":
        print(f"# env={r['env']} account={r['target']['account']} region={r['target']['region']} "
              f"stack={r['target']['stack']}")
        for name, value in r["params"].items():
            print(f"{name:24} {r['classes'][name]:9} {value}")
        for name in r["deploy"]:
            print(f"{name:24} deploy    {DECLARED[name][1]}")
        for name, why in r["gaps"]:
            print(f"GAP {name}: {why}")
        return 0
    if r["gaps"] and not args.allow_gaps:
        print(f"refusing: {len(r['gaps'])} {r['env']} gap(s) open; see `table --env {r['env']}`", file=sys.stderr)
        return 3
    for name, why in r["gaps"]:
        print(f"GAP (review only): {name}: {why}", file=sys.stderr)
    print(json.dumps([{"ParameterKey": k, "ParameterValue": v} for k, v in r["params"].items()], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
