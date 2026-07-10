"""Pin the SSH→SSM transport migration in infrastructure/spot_data_weekly.sh.

Origin: ROADMAP L342 PR 2 — the 2026-05-27 SSH/SCP→SSM migration moved
all dispatcher→spot communication to the lib chokepoint
``python -m nousergon_lib.ssm_dispatcher`` (lib v0.35.0+). Without
these chokepoint tests, a future refactor could silently re-introduce
SSH+SCP (the prior transport) and re-open the port-22 dependency the
migration was designed to retire.

The shape of each test mirrors PR #322's
``TestDeployScriptsHaveNoEventBridgeWrites`` — a regex-based
"forbidden phrase" assertion on the deploy script's source. The lib
chokepoint is the canonical path; any reintroduction of SSH/SCP at the
top-level dispatch surface fails loud at PR time.

Closes the (i) alive-SSH-path finding from the 2026-05-24 audit (PR 2
of the 5-PR ROADMAP L342 arc). PR 3 will follow this exact same pattern
for ``spot_backtest.sh``; PR 4 will retire predictor #168's inline
``run_ssm`` bash helper in favor of the lib CLI; PR 5 will revoke the
port-22 SG inbound rule once 1 clean Saturday SF runs on the new
transport across all three spots.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"


def _script_lines() -> list[tuple[int, str]]:
    """Return (line_no, line) tuples, comment lines stripped.

    Comment lines may legitimately reference SSH / SCP / port-22 in
    historical-context prose (e.g. "Replaces the pre-2026-05-27 SCP
    path"); only non-comment lines are subject to the forbidden-phrase
    chokepoint.
    """
    assert _SCRIPT.exists(), f"spot_data_weekly.sh missing at {_SCRIPT}"
    out: list[tuple[int, str]] = []
    for i, raw in enumerate(_SCRIPT.read_text().splitlines(), start=1):
        stripped = raw.strip()
        if stripped.startswith("#"):
            continue
        out.append((i, raw))
    return out


def test_spot_data_weekly_script_exists():
    """Guards against accidental script deletion. Without the script,
    the chokepoint assertions below silently no-op."""
    assert _SCRIPT.exists(), (
        f"infrastructure/spot_data_weekly.sh missing at {_SCRIPT}. "
        "This script drives the Saturday SF DataPhase1 / MorningEnrich / "
        "RAG / Phase 1 spots; CI cannot validate its SSM transport "
        "invariant without it."
    )


def test_no_top_level_ssh_invocation():
    """No ``ssh ...`` command at the top of any non-comment line.

    Replaces the pre-2026-05-27 SSH dispatch (``ssh -i $KEY_FILE
    ec2-user@$PUBLIC_IP "<cmd>"``) with ``python -m
    nousergon_lib.ssm_dispatcher`` (lib v0.35.0+). Any new ``ssh``
    invocation surfaces as an immediate red CI signal.

    Allow-list: none. Inside heredoc bodies (the spot-side shell
    scripts dispatched to the instance), an ``ssh`` token would
    legitimately invoke ssh ON THE SPOT — but the data path has zero
    use for that today, so the test treats any non-comment ``ssh``
    occurrence as a regression worth surfacing at PR time. If a
    legitimate future need lands an ssh inside a heredoc, scope this
    test to dispatcher-side lines only.
    """
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if re.search(r"\bssh\s+-\w+", line) or re.search(r"^\s*ssh\s+", line)
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``ssh`` invocations in "
        f"spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nThe 2026-05-27 SSH→SSM migration moved all dispatch to "
        "``python -m krepis.ssm_dispatcher``. Re-introducing "
        "ssh re-opens the port-22 dependency the migration retired. "
        "If the change is deliberate, update this test + ROADMAP L342 "
        "PR 5 (the planned port-22 SG revoke)."
    )


def test_no_top_level_scp_invocation():
    """No ``scp ...`` command at the top of any non-comment line.

    Replaces the pre-2026-05-27 SCP config upload (``scp -i $KEY_FILE
    <config> ec2-user@$PUBLIC_IP:<path>``) with the S3 staging pattern
    (dispatcher ``aws s3 cp`` to a temporary ``tmp/spot_data_weekly/``
    prefix, spot pulls via its existing ``alpha-engine-executor-profile``
    IAM role's ``s3:GetObject`` grant). Mirrors the
    alpha-engine-predictor #168 precedent.
    """
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if re.search(r"\bscp\s+-\w+", line) or re.search(r"^\s*scp\s+", line)
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``scp`` invocations in "
        f"spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nThe 2026-05-27 migration replaced SCP with S3 staging. "
        "Re-introducing scp re-opens the port-22 dependency."
    )


def test_no_ssh_keyscan_invocation():
    """No ``ssh-keyscan`` invocation — the pre-2026-05-27 bootstrap had
    ``ssh-keyscan github.com >> ~/.ssh/known_hosts`` to pre-seed the
    spot's known_hosts file for the git clone over HTTPS. Post-migration
    the spot clones via HTTPS (no host-key concern) and the dispatcher
    never SSHs in, so the keyscan step is dead code. Re-introducing it
    would silently re-introduce the SSH bootstrap dependency."""
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "ssh-keyscan" in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} ``ssh-keyscan`` invocations in "
        f"spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
    )


def test_uses_lib_ssm_dispatcher_chokepoint():
    """The migration's load-bearing surface: ``python -m
    krepis.ssm_dispatcher`` MUST appear in the script. Pinning
    this catches a regression where a future PR replaces the lib CLI
    with an inline ``aws ssm send-command`` bash helper (the
    alpha-engine-predictor #168 pre-lift pattern that L342 explicitly
    lifts to the lib chokepoint)."""
    body = _SCRIPT.read_text()
    assert "krepis.ssm_dispatcher" in body, (
        "spot_data_weekly.sh does not reference "
        "krepis.ssm_dispatcher. The 2026-05-27 migration uses "
        "the lib chokepoint as the SSM dispatch path; re-introducing a "
        "raw `aws ssm send-command` bash helper would undo the lift to "
        "``alpha-engine-lib`` v0.35.0."
    )
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "-m nousergon_lib." in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment 'python -m nousergon_lib.<mod>' "
        f"invocation(s) in spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nOn lib >=0.81.0 that path is a guard-less re-export shim: "
        "under `python -m` (runpy) it exits 0 silently WITHOUT executing "
        "the inner dispatch (config#1646 bug class). Invoke `-m "
        "krepis.ssm_dispatcher` / `-m krepis.ec2_spot` directly "
        "(config#1649)."
    )


def test_run_ssm_passes_diagnostics_flags():
    """L394 cascade — ``run_ssm`` MUST pass both ``--diagnostics-bucket``
    and ``--diagnostics-prefix`` so terminal non-Success in any spot
    SSM step writes a JSON failure record to
    ``s3://${S3_BUCKET}/_spot_diagnostics/ae-data/{date}.json`` per the
    lib v0.39.0 contract. Both flags must be present — lib's partial-
    config guard makes a missing flag a silent no-op (the worst
    failure mode: future grep for "did diagnostics fire?" returns
    empty even though the cron hit a real failure)."""
    body = _SCRIPT.read_text()
    assert "--diagnostics-bucket" in body, (
        "spot_data_weekly.sh does not pass --diagnostics-bucket to the "
        "lib CLI. L394 cascade requires both --diagnostics-bucket and "
        "--diagnostics-prefix together; without --diagnostics-bucket "
        "the lib's partial-config guard makes the diagnostics-write a "
        "silent no-op even on terminal non-Success."
    )
    assert "--diagnostics-prefix" in body, (
        "spot_data_weekly.sh does not pass --diagnostics-prefix to the "
        "lib CLI."
    )
    # Prefix MUST scope to ae-data so cascade B (ae-backtester) and
    # cascade C (ae-predictor) write to non-overlapping S3 namespaces —
    # multi-failure-per-day per-prefix would otherwise clobber.
    assert "_spot_diagnostics/ae-data" in body, (
        "spot_data_weekly.sh --diagnostics-prefix must scope to "
        "_spot_diagnostics/ae-data so ae-backtester / ae-predictor "
        "cascade siblings write to disjoint S3 namespaces. The current "
        "{date}.json key shape overwrites within a prefix; the per-repo "
        "subprefix is the multi-cascade discriminator."
    )


def test_no_inline_aws_ssm_send_command():
    """The script MUST NOT call ``aws ssm send-command`` directly — that
    bypasses the lib chokepoint and reverts to the pre-lift
    alpha-engine-predictor #168 pattern. The lib CLI wraps that exact
    call with the InvocationDoesNotExist registration grace, stdout
    streaming, and consistent S3 output-key layout; bypassing it loses
    those guarantees.

    Excludes comment lines (the prose may legitimately mention the
    underlying API name)."""
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "aws ssm send-command" in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``aws ssm send-command`` "
        f"invocations in spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nRoute through ``python -m krepis.ssm_dispatcher "
        "run`` instead — that's the chokepoint v0.35.0 lifted."
    )


def test_stages_config_via_s3():
    """The script MUST upload the private ``alpha-engine-config/data/
    config.yaml`` to a temporary S3 prefix before dispatching the
    bootstrap SSM call. Without S3 staging, the spot has no path to
    read the dispatcher's private config (no SCP, no shared filesystem)
    and the bootstrap step would fail at the
    ``aws s3 cp ... /home/ec2-user/alpha-engine-config/data/config.yaml``
    line. Pinning the dispatcher-side ``aws s3 cp ... config.yaml``
    catches a regression that drops the staging step but somehow
    keeps the bootstrap ``aws s3 cp`` (which would then return
    NoSuchKey)."""
    body = _SCRIPT.read_text()
    assert "aws s3 cp" in body and "/config.yaml" in body, (
        "spot_data_weekly.sh does not stage alpha-engine-config/data/"
        "config.yaml to S3. The migration replaced the SCP path with "
        "an S3 staging pattern; the dispatcher uploads the file to "
        "tmp/spot_data_weekly/<run_id>/config.yaml, and the spot pulls "
        "it via its existing alpha-engine-executor-profile IAM role."
    )


def test_no_residual_key_file_dispatch_use():
    """The pre-migration script referenced ``$KEY_FILE`` extensively for
    ssh + scp. Post-migration the SSH key file is no longer used for
    dispatch (the spot is launched WITH the key for break-glass operator
    SSH only). Any remaining ``$KEY_FILE`` or ``$SSH_OPTS`` reference in
    a NON-COMMENT line means the migration is incomplete.

    Allow-list: the ``KEY_NAME`` variable for the lib.ec2_spot
    ``--key-name`` launch flag stays — that's a different concern
    (instance attribute, not dispatch transport).
    """
    forbidden = ["$KEY_FILE", "${KEY_FILE}", "$SSH_OPTS", "${SSH_OPTS}"]
    offenders: list[tuple[int, str]] = []
    for n, line in _script_lines():
        if any(token in line for token in forbidden):
            offenders.append((n, line))
    assert not offenders, (
        f"Found {len(offenders)} residual KEY_FILE / SSH_OPTS uses in "
        f"non-comment lines of spot_data_weekly.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nThe migration retired the SSH key file as a dispatch "
        "credential. KEY_NAME stays as a launch attribute for "
        "nousergon_lib.ec2_spot's --key-name flag (break-glass "
        "operator SSH only); KEY_FILE / SSH_OPTS should not appear "
        "anywhere."
    )
