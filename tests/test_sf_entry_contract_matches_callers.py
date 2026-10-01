"""``sf_entry_contract.json`` must match what the callers actually send.

The reachability analysis in ``infrastructure/sf_reachability.py`` treats the
entry contract as ground truth: a field listed there is assumed present on every
execution, so a wrong entry is not a stale comment — it silently certifies a
reference that a different trigger makes fatal, which is the whole class the
analysis exists to catch (alpha-engine-config#5950).

So the contract is pinned against every caller that lives in THIS repo: the
CloudFormation EventBridge target inputs, the Friday shell-run trigger Lambda,
the off-cycle operator script, and the EOD backstop Lambda. A caller in another
repo cannot be read from here and is declared under ``external_callers``; its
pin is a consumer-contract test in that repo (alpha-engine-config-I9096).

The contract is the INTERSECTION over callers, never the union: a field only one
trigger passes is not guaranteed, and listing it would certify exactly the
reference that the other trigger breaks.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"
_CONTRACT = json.loads((_INFRA / "sf_entry_contract.json").read_text())

#: Every in-repo file that starts one of these state machines, and the definition
#: it targets. A caller added without a row here is caught by
#: ``test_no_in_repo_caller_is_unpinned`` below.
_CALLERS = {
    "step_function.json": [
        _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml",
        _INFRA / "lambdas" / "eod-success-friday-shell-trigger" / "index.py",
        _INFRA / "run_weekly_offcycle.sh",
    ],
    "step_function_daily.json": [
        _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml",
    ],
    "step_function_eod.json": [
        _INFRA / "lambdas" / "eod-backstop" / "index.py",
    ],
    # alpha-engine-config-I11269 follow-up (2026-09-30): the collector-dependent
    # half of the post-close pipeline. Both its starters (the collection-terminal
    # event trigger and the scheduled reconcile backstop) live in this Lambda.
    "step_function_eod_reconcile.json": [
        _INFRA / "lambdas" / "eod-backstop" / "index.py",
    ],
    "step_function_groom.json": [
        _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml",
    ],
}

#: Files that mention a pipeline ARN next to a StartExecution but do not perform
#: one, each with the reason it is exempt. An exemption is a claim about the file
#: and is asserted below, not taken on trust.
_NOT_CALLERS = {
    "deploy_step_function.sh": "echoes a suggested manual command; never calls",
    "deploy_step_function_daily.sh": "echoes a suggested manual command; never calls",
    "lambdas/saturday-sf-watch-dispatcher/index.py": (
        "pass-through: replays describe_resp['input'] verbatim, so it cannot "
        "introduce a weaker input than the execution it is rerunning"
    ),
}


@pytest.mark.parametrize("definition", sorted(_CONTRACT))
def test_every_contract_field_appears_in_every_declared_caller(definition):
    """A field is only guaranteed if EVERY declared in-repo caller sends it."""
    if definition.startswith("_"):
        return
    entry = _CONTRACT[definition]
    for caller in _CALLERS[definition]:
        text = caller.read_text()
        for field in entry["fields"]:
            # The weekday target's Input is a NESTED JSON string inside the CFN
            # (a Scheduler target wrapping an SFN StartExecution body), so its
            # quotes arrive backslash-escaped. Matching only bare quotes reported
            # a live, correct caller as missing every field it sends.
            assert re.search(rf'\\?["\']{re.escape(field)}\\?["\']', text), (
                f"{definition} declares {field!r} as guaranteed at entry, but "
                f"{caller.name} — a declared caller — never sends it. Either the "
                f"caller regressed or the contract is claiming more than it can."
            )


@pytest.mark.parametrize("definition", sorted(_CONTRACT))
def test_every_contract_entry_names_its_callers(definition):
    if definition.startswith("_"):
        return
    entry = _CONTRACT[definition]
    assert entry.get("callers"), (
        f"{definition} declares an entry contract with no provenance. A contract "
        f"nobody can trace back to a caller is a hand-kept list, which is the "
        f"thing this file exists to avoid."
    )


def test_the_pass_through_rerunner_really_is_pass_through():
    """The exemption above is a claim about the code; assert it rather than trust it.

    If this dispatcher ever BUILDS an input instead of replaying one, it becomes a
    real caller whose fields must be pinned, and the exemption silently stops being
    true — which is exactly how an unchecked caller gets in.
    """
    source = (_INFRA / "lambdas" / "saturday-sf-watch-dispatcher" / "index.py").read_text()
    assert 'input=describe_resp["input"]' in source, (
        "saturday-sf-watch-dispatcher no longer replays the original execution "
        "input verbatim; it is now a real caller and must be added to _CALLERS "
        "with its fields pinned"
    )


def test_the_deploy_scripts_only_print_their_example():
    """Their StartExecution lines must stay inside an ``echo``."""
    for name in ("deploy_step_function.sh", "deploy_step_function_daily.sh"):
        for line in (_INFRA / name).read_text().splitlines():
            if "start-execution" in line:
                assert line.lstrip().startswith("echo"), (
                    f"{name} now performs a StartExecution rather than printing one; "
                    f"it must be declared in _CALLERS"
                )


def test_external_callers_are_declared_not_silently_omitted():
    """A cross-repo caller must be NAMED, so its absence from the pin is visible."""
    eod = _CONTRACT["step_function_eod.json"]
    assert any(
        "crucible-executor" in c for c in eod.get("external_callers", [])
    ), (
        "the EOD pipeline's primary trigger is the executor daemon in another "
        "repo; dropping it from external_callers would make this file look fully "
        "pinned when its main caller is unchecked"
    )


def test_no_in_repo_caller_is_unpinned():
    """Every in-repo StartExecution against these SFs is in ``_CALLERS``."""
    pinned = {p.resolve() for group in _CALLERS.values() for p in group}
    targets = (
        "ne-weekly-freshness-pipeline",
        "ne-preopen-trading-pipeline",
        "ne-postclose-trading-pipeline",
        "ne-postclose-reconcile-pipeline",
    )
    unpinned = []
    for path in _INFRA.rglob("*"):
        if not path.is_file() or path.suffix not in (".py", ".sh", ".yaml", ".yml"):
            continue
        relative = str(path.relative_to(_INFRA))
        if path.resolve() in pinned or "test" in path.name or relative in _NOT_CALLERS:
            continue
        text = path.read_text(errors="ignore")
        if not any(t in text for t in targets):
            continue
        if "start_execution" in text or "start-execution" in text or "StartExecution" in text:
            unpinned.append(relative)
    assert not unpinned, (
        "these files start one of the pipelines but are not declared callers in "
        "sf_entry_contract.json, so the entry contract is unverified against them: "
        + ", ".join(sorted(unpinned))
    )
