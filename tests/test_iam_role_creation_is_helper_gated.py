"""Role creation must go through the shared, presence-gated helper.

WHY (alpha-engine-config-I9114, generalising alpha-engine-config-I9045)
-----------------------------------------------------------------------
`_shared/apply_iam_policy.sh` was fixed on 2026-08-28 so that a DENIED
`iam:GetRole` can never be read as "the role does not exist" — `AccessDenied`
and `NoSuchEntity` had been the same observation, which is how four workflows
came to call `iam:CreateRole` on every run for a week with a grant they do not
hold.

That fix protected exactly the call sites that used the helper.
`thinktank-spot-dispatcher/deploy.sh` had its own copy of the same three lines
and was untouched by it. THAT is the finding this file exists for: a class fix
at the correct layer is worth nothing to a call site that reimplemented the
layer, and the only way to know how many such call sites exist is to enumerate
them and refuse to let the number grow.

The sweep, measured 2026-08-28: 45 shell scripts in this repo still reach
`aws iam create-role` from their own code rather than through
`apply_iam_policy`. Every one of them guards it with the same
`if ! aws iam get-role ... >/dev/null 2>&1` probe, i.e. every one carries the
I9045 misclassification. They are DORMANT, not safe: each sits inside an
operator-only `--bootstrap` block that the CI identity never enters, which is a
property of today's flag layout and not a guarantee. Migrating 45 scripts would
collide with four in-flight PRs, so it is tracked separately; what lands here is
the detection, because detection blindness outranks the defects it hides.

`_REGISTER` is a debt register, not an allowlist. It may only shrink.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# The ONE sanctioned implementation. `apply_iam_policy` reaches create-role only
# through `probe_role_presence` (which distinguishes denied from absent) and only
# when its `may_create_role` argument is "true" — the operator path. The
# code-only auto-apply path passes "false" and cannot reach it at all.
_SANCTIONED = {
    "infrastructure/lambdas/_shared/apply_iam_policy.sh",
}

# Scripts that still hand-roll it. Tracked as alpha-engine-config-I9207.
# Delete an entry the moment its script migrates onto `apply_iam_policy` — the
# staleness assertion below fails until you do, because an exemption that
# outlives its debt starts hiding a regression on the same script.
_REGISTER = {
    "infrastructure/codebuild-runners/deploy_codebuild_runner.sh",
    "infrastructure/lambdas/_shared/ensure_overseer_scheduler_role.sh",
    "infrastructure/lambdas/alert-drain-dispatcher/deploy.sh",
    "infrastructure/lambdas/alert-drain-liveness-probe/deploy.sh",
    "infrastructure/lambdas/alpha-engine-alerts-forwarder/deploy.sh",
    "infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh",
    "infrastructure/lambdas/backstop-telegram-notifier/deploy.sh",
    "infrastructure/lambdas/canary-replay-dispatcher/deploy.sh",
    "infrastructure/lambdas/canary-replay-liveness-probe/deploy.sh",
    "infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh",
    "infrastructure/lambdas/changelog-incident-mirror/deploy.sh",
    "infrastructure/lambdas/ci-watch-dispatcher/deploy.sh",
    "infrastructure/lambdas/crypto-balances/deploy.sh",
    "infrastructure/lambdas/data-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/eod-backstop/deploy.sh",
    "infrastructure/lambdas/eod-precondition-probe/deploy.sh",
    "infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh",
    "infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh",
    "infrastructure/lambdas/expense-collector/deploy.sh",
    "infrastructure/lambdas/freshness-monitor/deploy.sh",
    "infrastructure/lambdas/friday-shell-run-report/deploy.sh",
    "infrastructure/lambdas/overseer-backstop-responder/deploy.sh",
    "infrastructure/lambdas/overseer-dispatcher/deploy.sh",
    "infrastructure/lambdas/overseer-liveness-probe/deploy.sh",
    "infrastructure/lambdas/pipeline-watchdog/deploy.sh",
    "infrastructure/lambdas/preflight-sweep-dispatcher/deploy.sh",
    "infrastructure/lambdas/preopen-deploy-readiness-probe/deploy.sh",
    "infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh",
    "infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh",
    "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh",
    "infrastructure/lambdas/sf-telegram-notifier/deploy.sh",
    "infrastructure/lambdas/sf-watch-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/spot-interruption-recorder/deploy.sh",
    "infrastructure/lambdas/spot-orphan-reaper/deploy.sh",
    "infrastructure/lambdas/ssm-liveness-poller/deploy.sh",
    "infrastructure/lambdas/ssm-reachability-probe/deploy.sh",
    "infrastructure/lambdas/substrate-health-gate/deploy.sh",
    "infrastructure/lambdas/sweep-artifact-monitor/deploy.sh",
    "infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/weekly-preflight/deploy.sh",
    "infrastructure/run_weekly_offcycle.sh",
    "infrastructure/setup_overseer_intake.sh",
}


def _code_only(body: str) -> str:
    """Shell source with FULL-LINE `#` comments removed.

    Load-bearing, not cosmetic. Several of these scripts explain the IAM
    boundary in prose containing the very command names scanned for, and this
    fleet has shipped the false positive three times in two days: a scan matched
    `ne-admin` inside a YAML comment, a test harvested a prose
    `systemctl enable --now` as an installer, and a drift guard matched
    `python3 -m pytest` inside the comment explaining the correct fix. Each time
    the pressure was to delete the rationale rather than fix the scan.

    Whole lines only. A `#` mid-line is left alone: stripping from the first `#`
    anywhere would eat a legitimate `"$URL#frag"` and is a false NEGATIVE risk
    this repo's sibling guard deliberately accepts, but here the assertion is
    about presence, so cutting less is the safer direction.
    """
    return "\n".join(
        line for line in body.splitlines() if not line.lstrip().startswith("#")
    )


def _shell_scripts() -> list[str]:
    out = subprocess.run(  # noqa: S603
        ["git", "ls-files", "*.sh"],  # noqa: S607
        cwd=_REPO_ROOT, capture_output=True, text=True, check=True,
    )
    paths = [line for line in out.stdout.split() if line]
    assert paths, "no shell scripts discovered — the glob or cwd is wrong"
    return paths


def _hand_rolled_creators() -> set[str]:
    """Every tracked shell script reaching `aws iam create-role` in CODE."""
    return {
        rel
        for rel in _shell_scripts()
        if "aws iam create-role" in _code_only((_REPO_ROOT / rel).read_text())
    } - _SANCTIONED


def test_no_new_script_hand_rolls_iam_role_creation() -> None:
    """The ratchet. A NEW script reaching `aws iam create-role` on its own is
    the defect this file exists to stop — it arrives carrying the I9045 probe
    because it is copied from a neighbour that has it."""
    new = sorted(_hand_rolled_creators() - _REGISTER)
    assert not new, (
        "these scripts reach `aws iam create-role` without going through "
        "_shared/apply_iam_policy.sh:\n  " + "\n  ".join(new)
        + "\n\nUse:\n"
        '  source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"\n'
        '  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" '
        '"${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"\n'
        "\nA hand-rolled `if ! aws iam get-role ... >/dev/null 2>&1` probe makes "
        "AccessDenied and NoSuchEntity the same observation, so a DENIED read is "
        "acted on as proof of absence. That is alpha-engine-config-I9045: four "
        "workflows red on every run for a week. `apply_iam_policy` reaches "
        "create-role only via probe_role_presence, which classifies a denial as "
        "`unknown` and creates nothing.\n"
        "Adding a name to _REGISTER instead is not the fix — the register may "
        "only shrink."
    )


def test_the_register_does_not_outlive_its_debt() -> None:
    """A register entry whose script has since migrated must be deleted. Left in
    place it is a standing exemption for a script that no longer needs one, and
    it would silently absorb a regression that re-introduced the hand-rolled
    probe there."""
    stale = sorted(_REGISTER - _hand_rolled_creators())
    assert not stale, (
        "these scripts no longer hand-roll `aws iam create-role`, so their "
        "entries in _REGISTER are stale. Delete each line "
        f"(tests/{Path(__file__).name}):\n  " + "\n  ".join(stale)
    )


def test_the_register_only_shrinks() -> None:
    """A hard ceiling, so growth cannot be laundered through an edit to the set
    literal in the same commit that adds an offender. Measured 2026-08-28: 45,
    down from 46 — thinktank-spot-dispatcher migrated in
    alpha-engine-config-I9114. Lower this number when you remove entries; never
    raise it."""
    assert len(_REGISTER) <= 45, (
        "the hand-rolled-IAM debt register grew. Migrate the script onto "
        "_shared/apply_iam_policy.sh instead (alpha-engine-config-I9207)."
    )


def test_the_shared_helper_is_the_only_sanctioned_creator() -> None:
    """The sanctioned set is not a second register. It names one file, and that
    file must actually still be the presence-gated implementation — otherwise
    this whole guard is asserting against a helper that has itself regressed."""
    assert _SANCTIONED == {"infrastructure/lambdas/_shared/apply_iam_policy.sh"}
    helper = _code_only(
        (_REPO_ROOT / "infrastructure/lambdas/_shared/apply_iam_policy.sh").read_text()
    )
    assert "probe_role_presence" in helper, (
        "apply_iam_policy.sh no longer defines the presence probe, so nothing "
        "distinguishes a denied read from an absent role (alpha-engine-config-I9045)."
    )
    assert "may_create_role" in helper, (
        "apply_iam_policy.sh no longer gates role creation, so the code-only "
        "auto-apply path can reach iam:CreateRole again."
    )


# ---------------------------------------------------------------------------
# THE WIDER CLASS: every IAM-MUTATING verb, not just create-role.
#
# The register above stops a new script hand-rolling `create-role`. It says
# nothing about `put-role-policy` — which is exactly how, after PR1569 landed,
# `deploy-overseer-dispatcher` got past CreateRole and then issued
# `aws iam put-role-policy` on the code-only path anyway, once per merge, from
# the identity the single-writer rule says must never hold that grant
# (run 33229043798, 2026-08-29).
#
# Naming only the verb that just bit is how a class survives its own fix. What
# follows asserts the property that actually matters: NO IAM-mutating verb is
# reachable on an ordinary merge-triggered deploy — i.e. outside an
# operator-gated `--bootstrap` / `--apply-iam` block — in any script a workflow
# executes.
# ---------------------------------------------------------------------------

_IAM_MUTATING_VERBS = (
    "create-role",
    "delete-role",
    "put-role-policy",
    "delete-role-policy",
    "attach-role-policy",
    "detach-role-policy",
    "update-assume-role-policy",
    "update-role",
    "tag-role",
    "untag-role",
    "put-user-policy",
    "attach-user-policy",
    "create-policy",
    "create-instance-profile",
    "add-role-to-instance-profile",
    "put-role-permissions-boundary",
)

_VERB_RE = re.compile(r"aws iam (" + "|".join(_IAM_MUTATING_VERBS) + r")\b")

# Scripts that carry an ungated IAM mutation and are NOT executed by any
# workflow. Each is an operator-run script or a function library whose callers
# are themselves gated; `test_no_ungated_iam_mutation_is_workflow_reachable`
# below is what keeps that true, so this set records WHY rather than granting an
# exemption. Migrating them onto apply_iam_policy is alpha-engine-config-I9207.
_OPERATOR_ONLY_WITH_UNGATED_IAM = {
    # A function library. Its two callers (sf-watch-spot-dispatcher,
    # ci-watch-dispatcher) invoke it only inside their --bootstrap blocks.
    "infrastructure/lambdas/_shared/ensure_overseer_scheduler_role.sh",
    # A function library. apply_iam_policy is reached only from --bootstrap and
    # --apply-iam; the code-only path calls check_iam_policy_on_deploy, which
    # mutates nothing (proved by executing it in
    # tests/test_iam_auto_apply_code_only_path.py).
    "infrastructure/lambdas/_shared/apply_iam_policy.sh",
    # One-shot operator provisioning scripts, run by hand.
    "infrastructure/attach_overseer_put_events_policy.sh",
    "infrastructure/codebuild-runners/deploy_codebuild_runner.sh",
    "infrastructure/setup_overseer_intake.sh",
    # Run on the EC2 box / by an operator. Named in two workflows' PATH FILTERS
    # only, never executed by one — which is why the assertion below tests for
    # execution rather than for mention.
    "infrastructure/run_weekly_offcycle.sh",
}


def _ungated_iam_mutations(rel: str) -> list[tuple[int, str]]:
    """IAM-mutating verbs in `rel` that sit OUTSIDE a `--bootstrap` /
    `--apply-iam` guarded block.

    Full-line comments are stripped first (see `_code_only`): several of these
    scripts explain the IAM boundary in prose naming the very verbs scanned
    for."""
    found: list[tuple[int, str]] = []
    depth = 0
    guards: list[tuple[int, str | None]] = []
    for lineno, raw in enumerate((_REPO_ROOT / rel).read_text().splitlines(), 1):
        line = raw.strip()
        if line.startswith("#"):
            continue
        if re.match(r"^(if|while|until)\b", line) or line.startswith("case "):
            depth += 1
            label = None
            if re.search(r"\$\{?BOOTSTRAP", line):
                label = "BOOTSTRAP"
            if re.search(r"\$\{?APPLY_IAM", line):
                label = "APPLY_IAM"
            guards.append((depth, label))
        if re.match(r"^(fi|done|esac)\b", line):
            if guards and guards[-1][0] == depth:
                guards.pop()
            depth = max(0, depth - 1)
        match = _VERB_RE.search(line)
        if match and not any(label for _, label in guards):
            found.append((lineno, match.group(1)))
    return found


def _workflow_executed_scripts() -> set[str]:
    """Tracked shell scripts a workflow actually RUNS.

    `bash <path>` / `./<path>` / `source <path>`, not a mere mention: two
    workflows name run_weekly_offcycle.sh in their `paths:` filters and neither
    executes it. A guard that could not tell those apart would either exempt a
    script that CI runs or file a finding against one it does not."""
    workflows = sorted((_REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    assert workflows, "no workflows discovered — the path is wrong"
    blob = "\n".join(w.read_text() for w in workflows)
    executed = set()
    for rel in _shell_scripts():
        name = re.escape(Path(rel).name)
        if re.search(rf"(?:bash|sh|source|\.)\s+\S*{name}\b", blob) or re.search(
            rf"\./\S*{name}\b", blob
        ):
            executed.add(rel)
    # Precondition, not decoration: if the match ever stops finding the deploy
    # scripts, every assertion built on this set passes vacuously. Measured
    # 2026-08-29: 48.
    assert len(executed) >= 20, (
        f"only {len(executed)} workflow-executed scripts discovered — the "
        "invocation match has broken and the guards below are now vacuous"
    )
    return executed


def test_no_ungated_iam_mutation_is_workflow_reachable() -> None:
    """THE PROPERTY. A merge-triggered deploy runs `deploy.sh` flagless; if any
    IAM-mutating verb is reachable from there, the deploy is asking for a grant
    the CI identity does not and must not hold (identity-access-policy.md §4 —
    the fix is never to widen the grant).

    Scoped to what CI executes on purpose: the ~45 hand-rolled `create-role`
    calls inside operator-only `--bootstrap` blocks are dormant by design and
    tracked as alpha-engine-config-I9207, not findings here."""
    offenders = {
        rel: _ungated_iam_mutations(rel)
        for rel in sorted(_workflow_executed_scripts())
        if _ungated_iam_mutations(rel)
    }
    offenders = {
        rel: hits
        for rel, hits in offenders.items()
        if rel not in _OPERATOR_ONLY_WITH_UNGATED_IAM
    }
    assert not offenders, (
        "these workflow-executed scripts mutate IAM outside an operator-gated "
        "--bootstrap / --apply-iam block:\n"
        + "\n".join(
            f"  {rel}: " + ", ".join(f"line {n} ({verb})" for n, verb in hits)
            for rel, hits in offenders.items()
        )
        + "\n\nA merge-triggered deploy is CODE ONLY. Move the mutation behind a "
        "flag, or use check_iam_policy_on_deploy, which reports drift and names "
        "the operator command without writing anything "
        "(alpha-engine-config-I9045)."
    )


def test_the_operator_only_exemptions_are_still_operator_only() -> None:
    """The exemption set records a measured fact — "no workflow runs this" —
    that a single new `run: bash ...` line silently invalidates. An exemption
    nothing re-checks is how a dormant defect wakes up unobserved."""
    executed = _workflow_executed_scripts()
    woken = sorted(
        rel
        for rel in _OPERATOR_ONLY_WITH_UNGATED_IAM
        if rel in executed and _ungated_iam_mutations(rel)
    )
    # The two _shared/ function libraries are sourced BY deploy.sh scripts, not
    # invoked by a workflow, so they must never appear here either.
    assert not woken, (
        "a workflow now executes a script that was exempted as operator-only "
        "and still mutates IAM outside an operator gate:\n  "
        + "\n  ".join(woken)
    )


def test_the_exemption_set_does_not_outlive_its_debt() -> None:
    """Same ratchet as `_REGISTER`: an entry whose script no longer carries an
    ungated IAM mutation must go, or it stands ready to absorb a regression."""
    stale = sorted(
        rel for rel in _OPERATOR_ONLY_WITH_UNGATED_IAM
        if not _ungated_iam_mutations(rel)
    )
    assert not stale, (
        "these scripts no longer mutate IAM outside an operator gate; delete "
        "their entries from _OPERATOR_ONLY_WITH_UNGATED_IAM:\n  "
        + "\n  ".join(stale)
    )
