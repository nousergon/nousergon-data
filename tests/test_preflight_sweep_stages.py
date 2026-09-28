"""The sweep's DENOMINATOR is derived from the live SF definition, and it
cannot shrink quietly (alpha-engine-config-I7249).

Two obligations shape this file:

1. **A stage added to the pipeline must not reduce coverage.** Either it is
   picked up automatically, or it FAILS. Both halves are tested here against
   the real ``infrastructure/step_function.json``, not a fixture — a
   derivation test that only ever sees its own fixture proves the parser
   works and nothing about the pipeline it is supposed to be tracking.

2. **The classifier must be PROVEN able to fail.** This fleet has shipped
   detectors that could not. Every classification has a test that produces
   it, including the three ways a stage becomes ``unsweepable``.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from infrastructure.preflight_sweep_stages import (
    BOX_DIR_TO_REPO,
    NO_DRY_PATH,
    SWEEPABLE,
    UNSWEEPABLE,
    apply_map_bindings,
    derive_required_map_bindings,
    derive_shell_run_bindings,
    derive_stages,
    load_manifest,
    manifest_disagreement,
    map_binding_disagreement,
    upstream_dependencies,
    upstream_dependency_disagreement,
)

REPO = pathlib.Path(__file__).resolve().parent.parent
SF_PATH = REPO / "infrastructure" / "step_function.json"
MANIFEST_PATH = REPO / "infrastructure" / "preflight_sweep_manifest.json"
CONTEXT = {"Execution": {"Name": "preflight-sweep-test", "Id": "arn:test"}}


@pytest.fixture(scope="module")
def definition() -> dict:
    return json.loads(SF_PATH.read_text())


@pytest.fixture(scope="module")
def manifest() -> dict:
    return load_manifest(MANIFEST_PATH)


@pytest.fixture(scope="module")
def bindings(definition, manifest) -> dict:
    base = derive_shell_run_bindings(definition)
    merged = apply_map_bindings(base, manifest)
    merged.setdefault("run_date", "2026-08-12")
    return merged


@pytest.fixture(scope="module")
def stages(definition, bindings):
    # checkout_root deliberately absent: this asserts the DERIVATION, which
    # must work anywhere. Launcher-on-disk checks are exercised separately.
    return derive_stages(definition, bindings, CONTEXT, checkout_root="/nonexistent")


# ── Bindings come from the definition, not from this file ────────────────────


def test_shell_run_bindings_are_read_out_of_the_definition(definition):
    b = derive_shell_run_bindings(definition)
    assert b["preflight_args"] == " --preflight-only"
    # ApplyShellRunDefaults' overrides must win over InitializeInput's seeds,
    # which is the precedence States.JsonMerge($, shellDefaults, false) has
    # live. Getting this backwards is what made the 2026-05-22 shell-run
    # execute full-fat instead of dry.
    assert b["research_dry"] is True
    assert b["data_phase2_dry"] is True
    assert b["regime_action"] == "dry_run"


def test_a_definition_that_stops_declaring_the_dry_flag_fails_loud(definition):
    """The sweep's whole contract with 21 launchers is that one value. If the
    definition stops setting it, the sweep must refuse rather than assume."""
    broken = json.loads(json.dumps(definition))
    params = broken["States"]["ApplyShellRunDefaults"]["Parameters"]
    key = next(iter(params))
    params[key] = params[key].replace(" --preflight-only", " --some-other-flag")
    with pytest.raises(ValueError, match="preflight_args"):
        derive_shell_run_bindings(broken)


def test_a_missing_shell_run_state_fails_loud(definition):
    broken = json.loads(json.dumps(definition))
    del broken["States"]["ApplyShellRunDefaults"]
    with pytest.raises(KeyError):
        derive_shell_run_bindings(broken)


# ── The live pipeline's stage set ────────────────────────────────────────────


def test_the_derivation_finds_the_pipeline_s_send_command_stages(stages):
    names = {s.name for s in stages}
    # Spot-check the three nesting shapes the walker has to handle: a
    # top-level stage, a Parallel-branch stage, and a second Parallel.
    # (The top-level spot-check was DataPhase1 until the decoupled data
    # cutover, alpha-engine-config-I11269, removed it.)
    assert "Backtester" in names
    assert "ResearchPredictorParallel.PredictorTraining" in names
    assert "ParityParallel.ParityReplay" in names


def test_every_stage_lands_in_exactly_one_classification(stages):
    for stage in stages:
        assert stage.classification in {SWEEPABLE, UNSWEEPABLE, NO_DRY_PATH}


def test_the_preflight_capable_stages_carry_a_launcher_and_a_working_directory(
    definition, bindings
):
    stages = derive_stages(definition, bindings, CONTEXT, checkout_root="/nonexistent")
    capable = [
        s
        for s in stages
        # With a nonexistent checkout every capable stage is UNSWEEPABLE for
        # the on-disk reason; what is asserted here is that the derivation
        # still extracted both anchors from the definition.
        if s.classification is UNSWEEPABLE and "not present in the checkout" in (s.reason or "")
    ]
    assert capable, "expected the live definition to declare preflight-capable stages"
    for stage in capable:
        # Both LEGITIMATE entry-point forms, not just the shell one: a stage's
        # dry path is a property of its entry point and not of the language it
        # is written in (alpha-engine-config-I9329 — EvalJudgeProcess runs
        # `python -m evals.judge_spot_run`). What the two checks downstream of
        # this derivation need is a repo-relative FILE, which is what is
        # asserted; the suffix set is closed so a third form has to be added
        # here deliberately rather than arriving as a silently-None launcher.
        assert stage.launcher and stage.launcher.endswith((".sh", ".py")), stage.name
        assert not stage.launcher.startswith("/"), (
            f"{stage.name}: launcher must be repo-relative, got {stage.launcher}"
        )
        # Membership, not a name prefix. The dedicated eval-judge box clones
        # under the repo's real name rather than the legacy `alpha-engine-*`
        # scheme, and what the sweep actually requires of a box_dir is that it
        # RESOLVES to a repo — a prefix check would pass on a directory the
        # attribution map has never heard of.
        assert stage.box_dir in BOX_DIR_TO_REPO, (
            f"{stage.name}: box_dir {stage.box_dir!r} is not in BOX_DIR_TO_REPO"
        )


def test_rendered_commands_carry_the_dry_flag_and_no_unresolved_placeholder(stages):
    for stage in stages:
        if stage.classification is NO_DRY_PATH or not stage.commands:
            continue
        joined = "\n".join(stage.commands)
        assert "--preflight-only" in joined, stage.name
        # An unrendered placeholder would be shipped to the box verbatim.
        assert "{}" not in joined, stage.name
        assert "States.Format" not in joined, stage.name


# ── Coverage cannot shrink quietly ───────────────────────────────────────────


def test_the_manifest_and_the_definition_agree_today(stages, manifest):
    """THE regression guard. A stage added to the pipeline without a dry path
    lands in the derived no-dry-path set, disagrees with the manifest, and
    fails HERE — at merge time, not on the next nightly sweep."""
    assert manifest_disagreement(stages, manifest) == []


def test_map_bindings_and_the_definition_agree_today(definition, manifest):
    base = derive_shell_run_bindings(definition)
    # run_date is supplied by the runner (see sweep()), not by a Map iteration.
    base["run_date"] = "2026-08-12"
    required = derive_required_map_bindings(definition, base)
    assert map_binding_disagreement(required, manifest) == []


def test_an_unacknowledged_no_dry_path_stage_is_a_finding(stages, manifest):
    stripped = {"no_dry_path_stages": [], "map_bindings": manifest["map_bindings"]}
    findings = manifest_disagreement(stages, stripped)
    assert findings, "a stage with no dry path must not pass unacknowledged"
    assert any("denominator" in f for f in findings)


def test_a_stale_manifest_entry_is_also_a_finding(stages, manifest):
    inflated = {
        "no_dry_path_stages": manifest["no_dry_path_stages"]
        + [{"stage": "StageThatNoLongerExists", "reason": "x"}],
        "map_bindings": manifest["map_bindings"],
    }
    findings = manifest_disagreement(stages, inflated)
    assert any("StageThatNoLongerExists" in f for f in findings)


def test_an_undeclared_map_binding_is_a_finding(definition, manifest):
    base = derive_shell_run_bindings(definition)
    base["run_date"] = "2026-08-12"
    required = derive_required_map_bindings(definition, base)
    assert required, "expected the live definition to carry a Map-scoped binding"
    findings = map_binding_disagreement(required, {"map_bindings": {}})
    assert findings and all("map_bindings" in f for f in findings)


def test_a_new_stage_without_a_dry_path_fails_rather_than_being_dropped(
    definition, bindings, manifest
):
    """Simulates the actual regression: somebody adds a sendCommand stage and
    does not give it --preflight-only. It must not vanish from the count."""
    mutated = json.loads(json.dumps(definition))
    mutated["States"]["BrandNewStage"] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
        "Parameters": {
            "DocumentName": "AWS-RunShellScript",
            "InstanceIds.$": "$.ec2_instance_id",
            "Parameters": {
                "executionTimeout": ["600"],
                "commands.$": (
                    "States.Array('set -eo pipefail',"
                    "'cd /home/ec2-user/alpha-engine-data',"
                    "'bash infrastructure/spot_brand_new.sh')"
                ),
            },
        },
        "End": True,
    }
    stages = derive_stages(mutated, bindings, CONTEXT, checkout_root="/nonexistent")
    new = next(s for s in stages if s.name == "BrandNewStage")
    assert new.classification is NO_DRY_PATH
    assert any("BrandNewStage" in f for f in manifest_disagreement(stages, manifest))


def test_a_stage_threading_the_flag_at_a_missing_launcher_is_unsweepable(
    definition, bindings
):
    mutated = json.loads(json.dumps(definition))
    mutated["States"]["GhostStage"] = {
        "Type": "Task",
        "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
        "Parameters": {
            "DocumentName": "AWS-RunShellScript",
            "InstanceIds.$": "$.ec2_instance_id",
            "Parameters": {
                "executionTimeout": ["600"],
                "commands.$": (
                    "States.Array('set -eo pipefail',"
                    "'cd /home/ec2-user/alpha-engine-data',"
                    "States.Format('bash infrastructure/does_not_exist.sh{}',$.preflight_args))"
                ),
            },
        },
        "End": True,
    }
    stages = derive_stages(mutated, bindings, CONTEXT, checkout_root="/nonexistent")
    ghost = next(s for s in stages if s.name == "GhostStage")
    assert ghost.classification is UNSWEEPABLE
    assert "not present in the checkout" in ghost.reason


def test_a_launcher_without_the_flag_is_unsweepable_not_a_pass(
    definition, bindings, tmp_path
):
    """The quiet failure this guards: the stage passes --preflight-only, the
    launcher does not implement it, and the flag is ignored or rejected. Either
    way the sweep must not report the stage as checked."""
    box = tmp_path / "alpha-engine-data" / "infrastructure"
    box.mkdir(parents=True)
    (box / "no_flag.sh").write_text("#!/usr/bin/env bash\necho hello\n")
    mutated = json.loads(json.dumps(definition))
    mutated["States"] = {
        "FlaglessStage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
            "Parameters": {
                "DocumentName": "AWS-RunShellScript",
                "InstanceIds.$": "$.ec2_instance_id",
                "Parameters": {
                    "executionTimeout": ["600"],
                    "commands.$": (
                        "States.Array('cd /home/ec2-user/alpha-engine-data',"
                        "States.Format('bash infrastructure/no_flag.sh{}',$.preflight_args))"
                    ),
                },
            },
            "End": True,
        }
    }
    stages = derive_stages(mutated, bindings, CONTEXT, checkout_root=str(tmp_path))
    stage = stages[0]
    assert stage.classification is UNSWEEPABLE
    assert "does not implement --preflight-only" in stage.reason


def test_a_stage_whose_command_cannot_be_rendered_is_unsweepable(definition, bindings):
    """Definition drift — a JSONPath no binding covers. The sweep must refuse
    to claim the stage, never substitute a blank and run something else."""
    mutated = json.loads(json.dumps(definition))
    mutated["States"] = {
        "DriftedStage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
            "Parameters": {
                "DocumentName": "AWS-RunShellScript",
                "InstanceIds.$": "$.ec2_instance_id",
                "Parameters": {
                    "executionTimeout": ["600"],
                    "commands.$": (
                        "States.Array('cd /home/ec2-user/alpha-engine-data',"
                        "States.Format('bash infrastructure/spot_data_weekly.sh --x {}{}',"
                        "$.a_field_nobody_declared,$.preflight_args))"
                    ),
                },
            },
            "End": True,
        }
    }
    stages = derive_stages(mutated, bindings, CONTEXT, checkout_root="/nonexistent")
    assert stages[0].classification is UNSWEEPABLE
    assert "could not be rendered" in stages[0].reason


# ── Declared same-day upstream dependencies (alpha-engine-config#7323) ───────
# The classification of a stage whose preflight fails only because its upstream
# has not run today is DECLARED here and nowhere else. If it were inferred from
# the launcher's stderr, rewording an error message would silently turn a real
# failure into a "could not measure".


def test_the_declared_upstream_dependencies_and_the_definition_agree_today(
    stages, manifest
):
    assert upstream_dependency_disagreement(stages, manifest) == []


def test_the_backtest_chain_is_declared_with_its_prefix_and_its_producer(manifest):
    declared = upstream_dependencies(manifest)
    assert set(declared) == {"PredictorBacktest", "PortfolioOptimizerBacktest"}
    assert declared["PredictorBacktest"]["produced_by"] == "Backtester"
    assert declared["PortfolioOptimizerBacktest"]["produced_by"] == "PredictorBacktest"
    for entry in declared.values():
        assert entry["prefix"] == "backtest/{run_date}/"
        # The non-inferable gotcha, declared rather than buried in code: the
        # prefix EXISTS on a day nothing produced it, because the sweep's own
        # phase markers are written under it.
        assert ".phases/" in entry["ignore_subprefixes"]


def test_a_declaration_for_a_stage_the_definition_lost_is_a_finding(stages):
    stale = {"upstream_artifact_dependencies": [
        {"stage": "StageThatWasRenamed", "produced_by": "Backtester",
         "prefix": "backtest/{run_date}/", "reason": "r"}
    ]}
    findings = upstream_dependency_disagreement(stages, stale)
    assert findings and "does not contain" in findings[0]


def test_a_declaration_naming_a_producer_that_does_not_exist_is_a_finding(stages):
    bad = {"upstream_artifact_dependencies": [
        {"stage": "PredictorBacktest", "produced_by": "NoSuchStage",
         "prefix": "backtest/{run_date}/", "reason": "r",
         "date_normalization": "nyse_trading_day"}
    ]}
    findings = upstream_dependency_disagreement(stages, bad)
    assert findings and "not a stage in the definition" in findings[0]


def test_a_declaration_for_a_stage_with_no_dry_path_at_all_is_a_finding(stages):
    no_dry = next(s for s in stages if s.classification is NO_DRY_PATH)
    bad = {"upstream_artifact_dependencies": [
        {"stage": no_dry.name, "produced_by": "Backtester",
         "prefix": "backtest/{run_date}/", "reason": "r"}
    ]}
    findings = upstream_dependency_disagreement(stages, bad)
    assert findings and "can never apply" in findings[0]


def test_a_malformed_declaration_is_dropped_and_reported_never_half_applied(stages):
    """A declaration the sweep cannot act on must not arm a reclassification."""
    bad = {"upstream_artifact_dependencies": [
        {"stage": "PredictorBacktest", "produced_by": "Backtester", "reason": "r"}
    ]}
    assert upstream_dependencies(bad) == {}
    findings = upstream_dependency_disagreement(stages, bad)
    assert findings and "missing prefix" in findings[0]


# ── The dedicated-box acknowledgement is graded BOTH ways (I9329) ────────────


def test_the_dedicated_box_declaration_and_the_definition_agree_today(stages, manifest):
    """Same obligation as the no-dry-path set: the live definition and the
    manifest must agree, in both directions, at merge time."""
    assert manifest_disagreement(stages, manifest) == []
    declared = {e["stage"] for e in manifest["dedicated_box_stages"]}
    assert declared == {"ResearchPredictorParallel.EvalJudgeProcess"}
    derived_unsweepable = {s.name for s in stages if s.classification == UNSWEEPABLE}
    assert declared <= derived_unsweepable


def test_a_dedicated_box_entry_for_a_stage_that_became_sweepable_is_a_finding(
    definition, bindings, tmp_path
):
    """The stale direction. Give every launcher a real file on disk and the
    acknowledged stage is no longer UNSWEEPABLE — the entry now suppresses
    nothing and must be dropped, loudly."""
    reachable = derive_stages(definition, bindings, CONTEXT, checkout_root="/nonexistent")
    for stage in reachable:
        if not (stage.box_dir and stage.launcher):
            continue
        path = tmp_path / stage.box_dir / stage.launcher
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/bash\n# --preflight-only\n")
    sweepable_stages = derive_stages(definition, bindings, CONTEXT,
                                     checkout_root=str(tmp_path))
    ack = {"dedicated_box_stages": [
        {"stage": "ResearchPredictorParallel.EvalJudgeProcess",
         "acknowledged": "2026-09-06", "reason": "r"}
    ]}
    findings = [f for f in manifest_disagreement(sweepable_stages, ack)
                if "ResearchPredictorParallel.EvalJudgeProcess" in f]
    assert findings and "no longer agrees" in findings[0]
    assert "'sweepable'" in findings[0], (
        "the finding names what the definition derives now, so the operator "
        "does not have to re-derive it"
    )


def test_a_dedicated_box_entry_for_a_stage_the_definition_lost_is_a_finding(stages):
    ack = {"dedicated_box_stages": [
        {"stage": "StageThatWasRenamed", "acknowledged": "2026-09-06", "reason": "r"}
    ]}
    findings = [f for f in manifest_disagreement(stages, ack)
                if "StageThatWasRenamed" in f]
    assert findings and "no longer in the definition" in findings[0]


def test_an_unacknowledged_unsweepable_stage_raises_no_dedicated_box_finding(stages):
    """The declaration can never hide a defect: an UNSWEEPABLE stage nobody
    acknowledged keeps today's behaviour — no finding here, and the sweep's own
    coverage_defect count fails the run."""
    assert any(s.classification == UNSWEEPABLE for s in stages)
    findings = manifest_disagreement(stages, {"dedicated_box_stages": []})
    assert not [f for f in findings if "dedicated-box" in f]


def test_a_stage_acknowledged_in_both_blocks_is_a_contradiction(stages):
    no_dry = next(s for s in stages if s.classification is NO_DRY_PATH)
    both = {
        "no_dry_path_stages": [{"stage": no_dry.name, "reason": "r",
                                "acknowledged": "2026-09-06"}],
        "dedicated_box_stages": [{"stage": no_dry.name, "reason": "r",
                                  "acknowledged": "2026-09-06"}],
    }
    findings = manifest_disagreement(stages, both)
    assert any("contradictory claims" in f for f in findings)
