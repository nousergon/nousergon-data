"""Derive the nightly preflight sweep's stage list from the weekly Step
Function definition — never from a hand-maintained list.

WHY THIS EXISTS
---------------
Every real execution of ``ne-weekly-freshness-pipeline`` since 2026-08-10 has
failed, and each had a DIFFERENT root cause: a missing ``predictor.yaml``,
``backtest.py`` rc=1, an SSM ``Undeliverable`` from a spot reclaim, DataPhase1
rc=1, rc=137 OOM, rc=75, and ``No module named 'nousergon_lib'`` raised inside
the preflight Lambda itself. On 2026-08-10 alone the operator burned nine
reruns without converging; ``sf-pipeline-policy`` §2.5's target for operator
actions to recover is ONE.

Every one of those failures happened ON THE SPOT BOX, after boot, at first
use — a filesystem and runtime environment the ``weekly-preflight`` Lambda
(``sf_preflight.py``) structurally cannot observe. That Lambda asserts S3
freshness, ArcticDB connectivity, Polygon coverage, SF IAM reachability, tool
contracts, definition/JSONPath coherence and Lambda memory headroom — all
correct, none of it about the substrate that actually broke.

The instrument for the substrate already exists: every spot stage launcher
implements ``--preflight-only`` (boot + deps + smoke harness, exit 0, zero
spend), and the SF threads ``$.preflight_args`` into each of them so a
``shell_run=true`` execution runs the whole pipeline dry. But the SF is a
FAIL-FAST CHAIN: the first failing stage aborts the run, so a shell-run
teaches you exactly one root cause per execution — which is precisely the
observed pathology of sixteen consecutive runs with sixteen different causes.
Restructuring that topology into a non-short-circuiting fan-out is a
``complexity:ultra``, human-authored change (``sf-pipeline-policy`` §5).

So the sweep is a separate driver over the same instruments: it runs EVERY
stage's ``--preflight-only`` INDEPENDENTLY and CONTINUES PAST FAILURES,
producing the whole per-stage matrix in one pass.

HOW THE STAGE LIST IS DERIVED
-----------------------------
The denominator is ``infrastructure/step_function.json`` itself — the same
artifact the live pipeline is deployed from. For every ``ssm:sendCommand``
Task state, including those nested inside ``Parallel`` branches:

* the stage's target repo directory comes from its own ``cd``/``git -C`` line;
* the stage's launcher script comes from its own ``bash <path>.sh`` invocation;
* whether the stage HAS a dry path is decided by one signal and one only —
  does its command thread ``$.preflight_args``?

A stage that threads ``$.preflight_args`` is declaring itself preflight-capable
and MUST be sweepable. If its launcher is missing from the checkout, or the
launcher does not implement ``--preflight-only``, or its command cannot be
fully rendered (an unresolvable JSONPath — definition drift), the stage is
``UNSWEEPABLE`` and the sweep FAILS. Coverage never shrinks quietly.

A stage that does NOT thread ``$.preflight_args`` has no dry path at all.
Those are enumerated in ``infrastructure/preflight_sweep_manifest.json`` with
a written reason. The derived set and the manifest must agree EXACTLY — a new
stage without a dry path fails ``tests/test_preflight_sweep_stages.py`` at
merge time, and fails the sweep at run time. Adding a stage therefore cannot
silently reduce the denominator in either direction.

The stages' EXECUTION CONTEXT is derived the same way: the shell-run binding
map is read out of ``InitializeInput``'s seed blob merged under
``ApplyShellRunDefaults``' override blob, so a control variable added to the
definition is picked up without editing this file.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from infrastructure.sf_commands import UnresolvedReference, render_commands

__all__ = [
    "Stage",
    "SWEEPABLE",
    "UNSWEEPABLE",
    "NO_DRY_PATH",
    "derive_shell_run_bindings",
    "derive_stages",
    "load_manifest",
    "manifest_disagreement",
    "upstream_dependencies",
    "upstream_dependency_disagreement",
]

# ── Stage classification (closed vocabulary; add by PR) ──────────────────────
SWEEPABLE = "sweepable"
UNSWEEPABLE = "unsweepable"
NO_DRY_PATH = "no_dry_path"

# The one signal that decides whether a stage has a dry path. Not a name
# pattern, not a list — the definition's own contract with its launchers.
PREFLIGHT_ARGS_REF = "$.preflight_args"

# The execution-state path that names the LAUNCHER box — the one box the sweep
# runs on and can reach. A sendCommand stage whose ``InstanceIds`` is anything
# else runs on a box the sweep does not have (EvalJudgeProcess targets
# ``$.eval_judge_instance_id``, a dedicated spot box its own dispatcher
# bootstraps — alpha-engine-config-I9329). WHERE a stage runs is read from the
# definition, never inferred from what happens to be cloned on the sweep box:
# crucible-research was cloned onto the sweep box around 2026-09-26, the
# launcher file appeared in the checkout, and the on-disk probe below flipped
# the stage to SWEEPABLE — so the sweep ran it on a box with no eval-judge.env
# and graded the box's absence as the stage's failure (alpha-engine-config-I11779).
LAUNCHER_BOX_INSTANCE_REF = "$.ec2_instance_id"

SSM_SEND_COMMAND = "arn:aws:states:::aws-sdk:ssm:sendCommand"

_BOX_ROOT = "/home/ec2-user/"
_CD_RE = re.compile(r"^cd\s+" + re.escape(_BOX_ROOT) + r"([\w.-]+)\s*$")
_GIT_C_RE = re.compile(r"git\s+-C\s+" + re.escape(_BOX_ROOT) + r"([\w.-]+)\b")
_LAUNCHER_RE = re.compile(r"\bbash\s+((?:infrastructure|rag)/[\w./-]+\.sh)")
# The second legitimate entry-point form: `python -m dotted.module`. A stage's
# dry path is a property of its ENTRY POINT, not of the language it is written
# in — EvalJudgeProcess is the first stage to use it (alpha-engine-config-I9329)
# and AGENTS.md's "re-expressible as a Python CLI entry" makes it not the last.
_MODULE_LAUNCHER_RE = re.compile(r"\bpython[\w.]*\s+-m\s+([A-Za-z_][\w.]*)")
# Every stage wraps its real command in
# `python -m krepis.ssm_log_capture run ... -- <real command>`, so the wrapper
# itself matches _MODULE_LAUNCHER_RE. Its argv is stripped before the scan.
_LOG_CAPTURE_MARKER = "ssm_log_capture"
_ARGV_SEPARATOR = " -- "

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DEFINITION = REPO_ROOT / "infrastructure" / "step_function.json"
DEFAULT_MANIFEST = REPO_ROOT / "infrastructure" / "preflight_sweep_manifest.json"

# Maps the box's checkout directory name to the repo it is a clone of. Used
# only for reporting/attribution — the sweep runs against the directory the
# definition names, never against a repo name it guessed.
BOX_DIR_TO_REPO = {
    "alpha-engine-data": "nousergon/nousergon-data",
    "alpha-engine-backtester": "nousergon/crucible-backtester",
    "alpha-engine-predictor": "nousergon/crucible-predictor",
    "alpha-engine-research": "nousergon/crucible-research",
    # The DEDICATED eval-judge spot box clones under the repo's real name, not
    # the legacy `alpha-engine-*` directory scheme the shared launcher box uses
    # (alpha-engine-config-I9329). Two directory names, one repo: the mapping is
    # dir -> repo and was never required to be injective.
    "crucible-research": "nousergon/crucible-research",
    "alpha-engine-dashboard": "nousergon/crucible-dashboard",
    "alpha-engine-config": "nousergon/alpha-engine-config",
}


@dataclass
class Stage:
    """One weekly-SF stage as the sweep sees it."""

    name: str
    classification: str
    box_dir: str | None = None
    repo: str | None = None
    launcher: str | None = None
    reason: str | None = None
    commands: list[str] = field(default_factory=list)
    execution_timeout_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Definition walking ───────────────────────────────────────────────────────


def _iter_send_command_states(states: dict, prefix: str = ""):
    """Yield ``(qualified_name, state)`` for every ssm:sendCommand Task,
    descending into Parallel branches and Map item processors.

    Nesting is walked rather than flattened by name so a stage added inside a
    new Parallel branch is picked up with no change here.
    """
    for name, state in states.items():
        qualified = prefix + name
        if state.get("Resource") == SSM_SEND_COMMAND:
            yield qualified, state
        for branch in state.get("Branches", []) or []:
            yield from _iter_send_command_states(branch["States"], f"{qualified}.")
        processor = state.get("ItemProcessor") or state.get("Iterator")
        if processor:
            yield from _iter_send_command_states(processor["States"], f"{qualified}.")


def _raw_commands_expression(state: dict) -> str:
    params = state["Parameters"]["Parameters"]
    if "commands.$" in params:
        return params["commands.$"]
    return json.dumps(params.get("commands", []))


def _instance_target(state: dict) -> str | None:
    """The box a sendCommand stage targets: its ``InstanceIds.$`` path, or
    ``None`` when the definition names none (a literal list, or absent)."""
    ref = state.get("Parameters", {}).get("InstanceIds.$")
    return ref if isinstance(ref, str) and ref.strip() else None


def _box_dir(commands: list[str]) -> str | None:
    """The directory the stage runs in: its own ``cd``, else its ``git -C``."""
    for cmd in commands:
        m = _CD_RE.match(cmd.strip())
        if m:
            return m.group(1)
    for cmd in commands:
        m = _GIT_C_RE.search(cmd)
        if m:
            return m.group(1)
    return None


def _wrapped_payload(cmd: str) -> str:
    """``cmd`` with ``krepis.ssm_log_capture``'s OWN argv stripped.

    Load-bearing for the module form and only for it: the wrapper is itself a
    ``python -m`` invocation, so an unstripped scan would name
    ``krepis/ssm_log_capture.py`` as the launcher of every stage in the
    pipeline. The shell form never had this problem — the wrapper is not
    invoked with ``bash`` — which is why it is scanned against the raw command
    and keeps its existing derivation exactly.
    """
    idx = cmd.find(_LOG_CAPTURE_MARKER)
    if idx == -1:
        return cmd
    sep = cmd.find(_ARGV_SEPARATOR, idx)
    return cmd[sep + len(_ARGV_SEPARATOR) :] if sep != -1 else cmd


def _launcher(commands: list[str]) -> str | None:
    """The repo-relative entry point the stage runs, in either legitimate form.

    Both forms resolve to a repo-relative FILE, which is what lets the caller
    apply the same two checks to each — present in the checkout, and implements
    ``--preflight-only`` — with no branch on which form it was. The shell form
    is tried first so no existing stage's derivation can change.
    """
    for cmd in commands:
        m = _LAUNCHER_RE.search(cmd)
        if m:
            return m.group(1)
    for cmd in commands:
        m = _MODULE_LAUNCHER_RE.search(_wrapped_payload(cmd))
        if m:
            return m.group(1).replace(".", "/") + ".py"
    return None


def _execution_timeout(state: dict) -> int | None:
    raw = state["Parameters"]["Parameters"].get("executionTimeout")
    if isinstance(raw, list) and raw:
        try:
            return int(raw[0])
        except (TypeError, ValueError):
            return None
    return None


# ── Binding derivation ───────────────────────────────────────────────────────


def _iter_strings(node: Any):
    """Every string leaf of a parsed-JSON subtree, in document order.

    The intrinsics are scanned as the RAW strings they are in the definition.
    Re-serialising the subtree first would double-escape the quotes inside a
    ``States.StringToJson('{"a":"b"}')`` body and the blob would silently stop
    parsing — a failure that reads as "this state declares no bindings".
    """
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from _iter_strings(value)
    elif isinstance(node, list):
        for value in node:
            yield from _iter_strings(value)


def _embedded_json_blobs(expr: str) -> list[dict]:
    """Every ``States.StringToJson('{...}')`` object literal in an intrinsic.

    The definition seeds and overrides the execution input through these, so
    reading them IS reading the pipeline's declared control variables. Blobs
    containing a ``States.Format`` (the run_date builder) are skipped — those
    are computed at execution time and supplied by the caller instead.
    """
    blobs: list[dict] = []
    needle = "States.StringToJson('"
    idx = 0
    while True:
        start = expr.find(needle, idx)
        if start == -1:
            return blobs
        body_start = start + len(needle)
        end = expr.find("')", body_start)
        if end == -1:
            return blobs
        idx = end + 2
        body = expr[body_start:end]
        if "States.Format" in body:
            continue
        try:
            parsed = json.loads(body.replace("\\{", "{").replace("\\}", "}"))
        except json.JSONDecodeError:
            # Not a plain object literal (a computed blob). Skipping it is
            # safe and never silent: any binding it would have supplied
            # surfaces as an UnresolvedReference when a stage's command needs
            # it, which classifies that stage UNSWEEPABLE and fails the sweep.
            continue
        if isinstance(parsed, dict):
            blobs.append(parsed)


def derive_shell_run_bindings(definition: dict) -> dict[str, Any]:
    """The execution input a ``shell_run=true`` execution would carry.

    Read out of the definition — ``InitializeInput``'s seed blob first, then
    ``ApplyShellRunDefaults``' override blob on top (the override wins, which
    is the same precedence ``States.JsonMerge($, shellDefaults, false)`` has
    live). A control variable added to either state is picked up here with no
    edit, so the sweep cannot drift from the dry path the SF actually runs.
    """
    states = definition["States"]
    bindings: dict[str, Any] = {}
    for state_name in ("InitializeInput", "ApplyShellRunDefaults"):
        state = states.get(state_name)
        if state is None:
            raise KeyError(
                f"{state_name} is absent from the definition — the sweep derives its "
                "dry-run bindings from that state and cannot substitute a guess"
            )
        for expr in _iter_strings(state.get("Parameters", {})):
            for blob in _embedded_json_blobs(expr):
                bindings.update(blob)
    if bindings.get("preflight_args") != " --preflight-only":
        raise ValueError(
            "ApplyShellRunDefaults no longer sets preflight_args=' --preflight-only' "
            f"(got {bindings.get('preflight_args')!r}) — the sweep's dry contract with "
            "every launcher is derived from that value and must not be assumed"
        )
    return bindings


# ── Stage derivation ─────────────────────────────────────────────────────────


def derive_stages(
    definition: dict,
    bindings: dict[str, Any],
    context: dict[str, Any],
    checkout_root: str | os.PathLike[str] = _BOX_ROOT,
) -> list[Stage]:
    """Classify every ssm:sendCommand stage in the definition.

    ``checkout_root`` is where the box's repo clones live; each stage's
    launcher is verified to exist there and to implement ``--preflight-only``.
    """
    root = Path(checkout_root)
    stages: list[Stage] = []

    for name, state in _iter_send_command_states(definition["States"]):
        raw_expr = _raw_commands_expression(state)
        threads_preflight = PREFLIGHT_ARGS_REF in raw_expr

        try:
            commands = render_commands(state, bindings, context)
        except (UnresolvedReference, ValueError) as exc:
            # Definition drift: a JSONPath the sweep's derived bindings do not
            # cover, or a States.Format arity mismatch. Loud by construction —
            # a stage whose command cannot be reproduced is not a stage that
            # can be reported as passing.
            stages.append(
                Stage(
                    name=name,
                    classification=UNSWEEPABLE,
                    reason=f"command could not be rendered: {exc}",
                    execution_timeout_seconds=_execution_timeout(state),
                )
            )
            continue

        box_dir = _box_dir(commands)
        launcher = _launcher(commands)
        repo = BOX_DIR_TO_REPO.get(box_dir) if box_dir else None
        timeout = _execution_timeout(state)

        if not threads_preflight:
            stages.append(
                Stage(
                    name=name,
                    classification=NO_DRY_PATH,
                    box_dir=box_dir,
                    repo=repo,
                    launcher=launcher,
                    reason=(
                        f"stage command does not thread {PREFLIGHT_ARGS_REF} — it "
                        "declares no dry path"
                    ),
                    commands=commands,
                    execution_timeout_seconds=timeout,
                )
            )
            continue

        if launcher is None or box_dir is None:
            stages.append(
                Stage(
                    name=name,
                    classification=UNSWEEPABLE,
                    box_dir=box_dir,
                    repo=repo,
                    launcher=launcher,
                    reason=(
                        f"stage threads {PREFLIGHT_ARGS_REF} but no "
                        f"{'launcher script' if launcher is None else 'working directory'} "
                        "could be derived from its commands"
                    ),
                    commands=commands,
                    execution_timeout_seconds=timeout,
                )
            )
            continue

        # WHERE the stage runs is decided by the definition, BEFORE any look at
        # the sweep box's disk. The checkout probe answers "is the file here";
        # it cannot answer "does the stage run here", and the two came apart
        # the day a second repo was cloned onto the sweep box.
        target = _instance_target(state)
        if target != LAUNCHER_BOX_INSTANCE_REF:
            stages.append(
                Stage(
                    name=name,
                    classification=UNSWEEPABLE,
                    box_dir=box_dir,
                    repo=repo,
                    launcher=launcher,
                    reason=(
                        f"stage runs on {target or 'an undeclared target'}, not on the "
                        f"launcher box ({LAUNCHER_BOX_INSTANCE_REF}) the sweep runs on — "
                        "its environment (env files, venv) exists only on that other "
                        "box, so a checkout of its launcher here proves nothing"
                    ),
                    commands=commands,
                    execution_timeout_seconds=timeout,
                )
            )
            continue

        script_path = root / box_dir / launcher
        if not script_path.is_file():
            stages.append(
                Stage(
                    name=name,
                    classification=UNSWEEPABLE,
                    box_dir=box_dir,
                    repo=repo,
                    launcher=launcher,
                    reason=f"launcher not present in the checkout at {script_path}",
                    commands=commands,
                    execution_timeout_seconds=timeout,
                )
            )
            continue

        if "--preflight-only" not in script_path.read_text(errors="replace"):
            stages.append(
                Stage(
                    name=name,
                    classification=UNSWEEPABLE,
                    box_dir=box_dir,
                    repo=repo,
                    launcher=launcher,
                    reason=(
                        f"{launcher} does not implement --preflight-only, but the "
                        "stage threads it — the flag would be passed and ignored, or "
                        "rejected as an unknown argument"
                    ),
                    commands=commands,
                    execution_timeout_seconds=timeout,
                )
            )
            continue

        stages.append(
            Stage(
                name=name,
                classification=SWEEPABLE,
                box_dir=box_dir,
                repo=repo,
                launcher=launcher,
                commands=commands,
                execution_timeout_seconds=timeout,
            )
        )

    return stages


# ── Manifest agreement ───────────────────────────────────────────────────────


def load_manifest(path: str | os.PathLike[str] = DEFAULT_MANIFEST) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# Execution-input references only. The negative lookbehind is load-bearing:
# `$$.Execution.Name` contains the literal `$.Execution`, so without it every
# stage that stamps a correlation id would be reported as needing an
# undeclared `Execution` binding — a coverage finding manufactured by the
# scanner rather than by the definition.
_INPUT_REF_RE = re.compile(r"(?<!\$)\$\.([A-Za-z_][\w.]*)")


def derive_required_map_bindings(
    definition: dict, base_bindings: dict[str, Any]
) -> set[str]:
    """Execution-input references no derived binding can supply.

    These are the Map-scoped variables (``$.spec_id`` and friends) that exist
    only per iteration. Derived by scanning the sendCommand states' own command
    expressions, so a new one appears here the moment it is added to the
    definition — and, being absent from the manifest, fails the agreement check.
    """
    required: set[str] = set()
    for _name, state in _iter_send_command_states(definition["States"]):
        for ref in _INPUT_REF_RE.findall(_raw_commands_expression(state)):
            root = ref.split(".")[0]
            if root not in base_bindings:
                required.add(root)
    return required


def apply_map_bindings(bindings: dict[str, Any], manifest: dict) -> dict[str, Any]:
    """``bindings`` plus the manifest's declared Map-scoped values."""
    merged = dict(bindings)
    for key, entry in (manifest.get("map_bindings") or {}).items():
        merged[key] = entry["value"]
    return merged


def map_binding_disagreement(required: set[str], manifest: dict) -> list[str]:
    """Differences between the Map-scoped bindings the definition needs and the
    ones the manifest declares. Empty list means they agree."""
    declared = set((manifest.get("map_bindings") or {}).keys())
    findings: list[str] = []
    for key in sorted(required - declared):
        findings.append(
            f"stage command references $.{key}, which no derived binding supplies and "
            "the manifest does not declare — the stage cannot be rendered and the "
            "sweep would drop it; declare a map_bindings value with a written reason"
        )
    for key in sorted(declared - required):
        findings.append(
            f"manifest declares map_bindings.{key}, but no stage command references "
            "$.%s any more — drop the stale entry" % key
        )
    return findings


_REQUIRED_UPSTREAM_FIELDS = ("stage", "produced_by", "prefix", "reason")

#: How the probe date is resolved from the sweep's ``run_date`` binding. Closed
#: vocabulary, mirrored in preflight_sweep.py. Every dependency must DECLARE
#: one: a launcher that normalizes RUN_DATE (every crucible-backtester stage
#: does, via spot_common_normalize_run_date) reads a different prefix than the
#: sweep's raw calendar date names, and an undeclared default is exactly how a
#: real weekend failure gets downgraded to "awaiting upstream".
_DATE_NORMALIZATIONS = ("none", "nyse_trading_day")


def upstream_dependencies(manifest: dict) -> dict[str, dict]:
    """The DECLARED same-day upstream artifact dependency of each stage.

    Keyed by stage name. This is the ONLY place a stage's preflight failure may
    be reclassified from ``failed`` to ``unsweepable`` — the decision is never
    taken from the launcher's stderr text, so rewording an error message cannot
    turn a real failure into a "could not measure". Adding a dependency is a
    reviewed diff against this manifest.

    An entry missing a required field is dropped and reported by
    ``upstream_dependency_disagreement`` rather than half-applied: a declaration
    the sweep cannot act on must not silently arm a reclassification.
    """
    out: dict[str, dict] = {}
    for entry in manifest.get("upstream_artifact_dependencies", []) or []:
        if not isinstance(entry, dict):
            continue
        if any(not entry.get(f) for f in _REQUIRED_UPSTREAM_FIELDS):
            continue
        out[entry["stage"]] = entry
    return out


def upstream_dependency_disagreement(stages: list[Stage], manifest: dict) -> list[str]:
    """Differences between the declared upstream dependencies and the pipeline.

    Only the stale direction is derivable: nothing in ``step_function.json``
    says a stage reads a same-day upstream artifact — that lives in the
    launcher's own preflight — so the sweep cannot detect a MISSING declaration
    here. It is detected the other way instead, at run time: an undeclared
    upstream failure stays ``failed`` and pages, which is the safe direction.

    What IS checked, in both stale directions:

    * a declaration for a stage the definition no longer has, or that no longer
      threads ``$.preflight_args`` — the acknowledgement went stale and would
      arm a reclassification for nothing. Checked against the DEFINITION-level
      classification only: whether the launcher happens to be present in this
      particular checkout is a fact about the checkout, and treating that as a
      stale declaration would make the check fire everywhere the repo is not
      deployed;
    * a declaration naming a producing stage that is not in the definition —
      the reason line points at a stage that does not exist;
    * a malformed declaration (missing a required field), which
      ``upstream_dependencies`` drops.
    """
    by_name = {s.name: s for s in stages}
    has_dry_path = {s.name for s in stages if s.classification != NO_DRY_PATH}
    findings: list[str] = []
    for entry in manifest.get("upstream_artifact_dependencies", []) or []:
        if not isinstance(entry, dict):
            findings.append(
                "upstream_artifact_dependencies contains a non-object entry "
                f"({entry!r}) — it declares nothing and arms nothing"
            )
            continue
        name = entry.get("stage") or "<unnamed>"
        missing = [f for f in _REQUIRED_UPSTREAM_FIELDS if not entry.get(f)]
        if missing:
            findings.append(
                f"upstream_artifact_dependencies entry for {name!r} is missing "
                f"{', '.join(missing)} — it is DROPPED, so the stage would still be "
                "reported as a real failure when its upstream is simply absent"
            )
            continue
        if name not in by_name:
            findings.append(
                f"manifest declares an upstream dependency for stage {name!r}, which the "
                "definition does not contain (renamed or removed) — drop the stale entry"
            )
            continue
        if name not in has_dry_path:
            findings.append(
                f"manifest declares an upstream dependency for stage {name!r}, but that "
                f"stage is classified {NO_DRY_PATH!r} — it is never exercised at all, so "
                "the declaration can never apply; drop it or give the stage a dry path"
            )
        normalization = entry.get("date_normalization")
        if normalization is None:
            findings.append(
                f"upstream dependency for {name!r} declares no date_normalization — the "
                "probe would use the sweep's raw calendar run_date while the launcher "
                "normalizes RUN_DATE to the NYSE trading day, so on every non-trading day "
                "a REAL failure would be downgraded to upstream_pending; declare one of "
                f"{', '.join(_DATE_NORMALIZATIONS)}"
            )
        elif normalization not in _DATE_NORMALIZATIONS:
            findings.append(
                f"upstream dependency for {name!r} declares date_normalization="
                f"{normalization!r}, which is not one of {', '.join(_DATE_NORMALIZATIONS)} "
                "— the probe cannot be dated and the stage will report unmeasured"
            )
        producer = entry["produced_by"]
        if producer not in by_name:
            findings.append(
                f"upstream dependency for {name!r} names produced_by={producer!r}, which is "
                "not a stage in the definition — the reason an operator reads would point "
                "at a stage that does not exist"
            )
    return findings


def manifest_disagreement(stages: list[Stage], manifest: dict) -> list[str]:
    """Differences between the derived no-dry-path set and the manifest.

    Empty list means they agree. A non-empty list is a coverage finding in
    BOTH directions: a stage that quietly stopped having a dry path (the
    denominator shrank), or a manifest entry for a stage that gained one or no
    longer exists (the acknowledgement went stale). Callers fail on either.

    Two acknowledged sets are graded here, not one. ``no_dry_path_stages``
    acknowledges stages the definition gives no dry path at all;
    ``dedicated_box_stages`` acknowledges stages that ARE preflight-capable but
    run on a box the sweep does not have, so derivation classifies them
    ``UNSWEEPABLE``. Both are graded in the stale direction — an acknowledgement
    that outlives its cause is how a silent exclusion is born — and neither is
    graded in a way that lets a declaration hide a real defect: an UNSWEEPABLE
    stage that nobody acknowledged keeps today's behaviour (``coverage_defect``,
    and the run fails), which is why there is no `derived - acknowledged`
    finding for the dedicated-box set here.
    """
    derived = {s.name for s in stages if s.classification == NO_DRY_PATH}
    acknowledged = {entry["stage"] for entry in manifest.get("no_dry_path_stages", [])}
    findings: list[str] = []
    for name in sorted(derived - acknowledged):
        findings.append(
            f"stage {name!r} has no dry path and is not acknowledged in the manifest — "
            "the sweep's denominator would silently shrink; add it with a written "
            "reason or give the stage a --preflight-only path"
        )
    for name in sorted(acknowledged - derived):
        findings.append(
            f"manifest acknowledges {name!r} as having no dry path, but the definition "
            "no longer agrees (the stage gained one, was renamed, or was removed) — "
            "drop the stale entry"
        )

    # The dedicated-box set, graded the same way. A stage acknowledged as
    # running on a box the sweep does not have, but which no longer derives as
    # UNSWEEPABLE, has either become reachable (the acknowledgement is now
    # suppressing nothing and should be dropped so real coverage is claimed) or
    # changed shape underneath the entry (renamed, removed, or it lost its dry
    # path, in which case it belongs in no_dry_path_stages instead). Either way
    # the entry outlived its cause, which is exactly the silent exclusion a
    # first-class acknowledgement exists to prevent.
    derived_unsweepable = {s.name for s in stages if s.classification == UNSWEEPABLE}
    by_name = {s.name: s for s in stages}
    dedicated = {
        entry["stage"]
        for entry in (manifest.get("dedicated_box_stages") or [])
        if isinstance(entry, dict) and entry.get("stage")
    }
    for name in sorted(dedicated - derived_unsweepable):
        stage = by_name.get(name)
        if stage is None:
            became = "the stage is no longer in the definition (renamed or removed)"
        else:
            became = f"the definition now derives it {stage.classification!r}"
        findings.append(
            f"manifest acknowledges {name!r} as a dedicated-box stage the sweep cannot "
            f"reach, but the definition no longer agrees — {became}. Drop the stale "
            "entry: an acknowledgement that outlives its cause is a silent exclusion, "
            "and while it stands the sweep claims a gap it no longer has"
        )
    for name in sorted(dedicated & acknowledged):
        findings.append(
            f"stage {name!r} is acknowledged in BOTH no_dry_path_stages and "
            "dedicated_box_stages — the two make contradictory claims (no dry path at "
            "all versus preflight-capable on a box the sweep lacks); keep exactly one"
        )
    return findings
