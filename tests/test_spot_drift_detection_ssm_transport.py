"""Pin the SSH→SSM transport migration in infrastructure/spot_drift_detection.sh.

Origin: config#893 — "Migrate spot_train.sh (+ siblings) off SSH/SCP onto
SSM + S3 staging". The drift sibling was the one spot launcher the
2026-05-27 migration arc (ROADMAP L342 PR 2/3/4) missed: it still launched
with a hard ``$KEY_FILE`` SSH-key dependency, polled for SSH readiness, and
dispatched every spot-side step via ``ssh -i $KEY_FILE ec2-user@$PUBLIC_IP``
+ ``ssh-keyscan``. This test pins the migration to the lib chokepoint
``python -m nousergon_lib.ssm_dispatcher`` (lib v0.35.0+), mirroring
tests/test_spot_data_weekly_ssm_transport.py 1:1.

The shape of each test mirrors that sibling — a regex-based "forbidden
phrase" assertion on the launcher source. The lib chokepoint is the
canonical path; any reintroduction of SSH/SCP at the top-level dispatch
surface fails loud at PR time and re-opens the port-22 dependency the
migration retired.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_drift_detection.sh"


def _script_lines() -> list[tuple[int, str]]:
    """Return (line_no, line) tuples, comment lines stripped.

    Comment lines may legitimately reference SSH / SCP / port-22 in
    historical-context prose (e.g. "the spot still launches with this key
    ... for break-glass operator SSH only"); only non-comment lines are
    subject to the forbidden-phrase chokepoint.
    """
    assert _SCRIPT.exists(), f"spot_drift_detection.sh missing at {_SCRIPT}"
    out: list[tuple[int, str]] = []
    for i, raw in enumerate(_SCRIPT.read_text().splitlines(), start=1):
        stripped = raw.strip()
        if stripped.startswith("#"):
            continue
        out.append((i, raw))
    return out


def test_spot_drift_detection_script_exists():
    """Guards against accidental script deletion. Without the script,
    the chokepoint assertions below silently no-op."""
    assert _SCRIPT.exists(), (
        f"infrastructure/spot_drift_detection.sh missing at {_SCRIPT}. "
        "This script drives the Saturday SF DriftDetection spot; CI cannot "
        "validate its SSM transport invariant without it."
    )


def test_no_top_level_ssh_invocation():
    """No ``ssh ...`` command on any non-comment line.

    Replaces the pre-migration SSH dispatch (``ssh -i $KEY_FILE
    ec2-user@$PUBLIC_IP "<cmd>"`` + the SSH-readiness poll) with
    ``python -m nousergon_lib.ssm_dispatcher``. Any new ``ssh``
    invocation surfaces as an immediate red CI signal and re-opens the
    port-22 dependency the migration retired.
    """
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if re.search(r"\bssh\s+-\w+", line) or re.search(r"^\s*ssh\s+", line)
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``ssh`` invocations in "
        f"spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nThe config#893 SSH→SSM migration moved all dispatch to "
        "``python -m nousergon_lib.ssm_dispatcher``. Re-introducing ssh "
        "re-opens the port-22 dependency the migration retired."
    )


def test_no_top_level_scp_invocation():
    """No ``scp ...`` command on any non-comment line.

    The drift launcher never SCP'd a config (the workload reads the
    alpha-engine-research bucket directly), but pin scp-free anyway so a
    future change can't introduce an scp config-push instead of the S3
    staging pattern the siblings use.
    """
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if re.search(r"\bscp\s+-\w+", line) or re.search(r"^\s*scp\s+", line)
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``scp`` invocations in "
        f"spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
    )


def test_no_ssh_keyscan_invocation():
    """No ``ssh-keyscan`` invocation — the pre-migration bootstrap had
    ``ssh-keyscan github.com >> ~/.ssh/known_hosts``. Post-migration the
    spot clones via HTTPS (no host-key concern) and the dispatcher never
    SSHs in, so the keyscan step is dead code; re-introducing it would
    silently re-introduce the SSH bootstrap dependency."""
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "ssh-keyscan" in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} ``ssh-keyscan`` invocations in "
        f"spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
    )


def test_uses_lib_ssm_dispatcher_chokepoint():
    """The migration's load-bearing surface: ``python -m
    krepis.ssm_dispatcher`` MUST appear in the script. Pinning
    this catches a regression where a future PR replaces the lib CLI
    with an inline ``aws ssm send-command`` bash helper (the predictor
    #168 pre-lift pattern that L342 explicitly lifts to the lib
    chokepoint)."""
    body = _SCRIPT.read_text()
    assert "krepis.ssm_dispatcher" in body, (
        "spot_drift_detection.sh does not reference "
        "krepis.ssm_dispatcher. The config#893 migration uses the "
        "lib chokepoint as the SSM dispatch path."
    )


def test_uses_lib_ec2_spot_launcher():
    """Launch goes through ``python -m krepis.ec2_spot`` (the same
    capacity-rotating launcher the data/backtest siblings use), not a raw
    ``aws ec2 run-instances`` with a hard ``--key-name`` SSH dependency."""
    body = _SCRIPT.read_text()
    assert "krepis.ec2_spot" in body, (
        "spot_drift_detection.sh does not launch via krepis.ec2_spot; "
        "the migration routes launch through the lib's capacity-rotating CLI."
    )
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "-m nousergon_lib." in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment 'python -m nousergon_lib.<mod>' "
        f"invocation(s) in spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nOn lib >=0.81.0 that path is a guard-less re-export shim: "
        "under `python -m` (runpy) it exits 0 silently WITHOUT executing "
        "the inner dispatch/launch (config#1646 bug class). Invoke `-m "
        "krepis.ssm_dispatcher` / `-m krepis.ec2_spot` directly "
        "(config#1649)."
    )


def test_run_ssm_passes_diagnostics_flags():
    """L394 cascade — ``run_ssm`` MUST pass both ``--diagnostics-bucket``
    and ``--diagnostics-prefix`` so terminal non-Success in any spot SSM
    step writes a JSON failure record per the lib v0.39.0 contract. Both
    flags must be present — the lib's partial-config guard makes a missing
    flag a silent no-op."""
    body = _SCRIPT.read_text()
    assert "--diagnostics-bucket" in body, (
        "spot_drift_detection.sh does not pass --diagnostics-bucket to the "
        "lib CLI. L394 cascade requires both --diagnostics-bucket and "
        "--diagnostics-prefix together."
    )
    assert "--diagnostics-prefix" in body, (
        "spot_drift_detection.sh does not pass --diagnostics-prefix."
    )
    # Prefix scopes to ae-data so cascade siblings write to disjoint S3
    # namespaces — the drift launcher lives in the data repo.
    assert "_spot_diagnostics/ae-data" in body, (
        "spot_drift_detection.sh --diagnostics-prefix must scope to "
        "_spot_diagnostics/ae-data (the data-repo cascade namespace)."
    )


def test_no_inline_aws_ssm_send_command():
    """The script MUST NOT call ``aws ssm send-command`` directly — that
    bypasses the lib chokepoint and reverts to the predictor #168 pre-lift
    pattern, losing the InvocationDoesNotExist registration grace, stdout
    streaming, and consistent S3 output-key layout the lib CLI provides.

    Excludes comment lines (prose may legitimately mention the API name)."""
    offenders = [
        (n, line)
        for n, line in _script_lines()
        if "aws ssm send-command" in line
    ]
    assert not offenders, (
        f"Found {len(offenders)} non-comment ``aws ssm send-command`` "
        f"invocations in spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nRoute through ``python -m krepis.ssm_dispatcher run`` "
        "instead — that's the chokepoint v0.35.0 lifted."
    )


def test_no_residual_key_file_dispatch_use():
    """Post-migration the SSH key file is no longer used for dispatch (the
    spot launches WITH the key for break-glass operator SSH only). Any
    remaining ``$KEY_FILE`` or ``$SSH_OPTS`` reference in a NON-COMMENT
    line means the migration is incomplete.

    Allow-list: the ``KEY_NAME`` variable for the lib.ec2_spot
    ``--key-name`` launch flag stays — that's an instance attribute, not
    dispatch transport.
    """
    forbidden = ["$KEY_FILE", "${KEY_FILE}", "$SSH_OPTS", "${SSH_OPTS}", "$PUBLIC_IP", "${PUBLIC_IP}"]
    offenders: list[tuple[int, str]] = []
    for n, line in _script_lines():
        if any(token in line for token in forbidden):
            offenders.append((n, line))
    assert not offenders, (
        f"Found {len(offenders)} residual KEY_FILE / SSH_OPTS / PUBLIC_IP "
        f"uses in non-comment lines of spot_drift_detection.sh:\n"
        + "\n".join(f"  line {n}: {line.strip()}" for n, line in offenders)
        + "\n\nThe migration retired the SSH key file + public-IP SSH "
        "target as dispatch credentials. KEY_NAME stays as a launch "
        "attribute for nousergon_lib.ec2_spot's --key-name flag."
    )


def test_waits_for_ssm_agent_not_ssh():
    """Readiness poll must be the SSM-agent PingStatus probe, not the old
    SSH-readiness loop."""
    body = _SCRIPT.read_text()
    assert "describe-instance-information" in body and "PingStatus" in body, (
        "spot_drift_detection.sh must poll SSM agent PingStatus for "
        "readiness (replacing the old SSH-readiness loop)."
    )
