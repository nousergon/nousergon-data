"""Single-writer shape-guard for the weekly SF definition deploy path
(alpha-engine-config#2273).

Root cause this pins: the ne-weekly-freshness-pipeline definition existed as
THREE copies — the repo file, the S3 object CFN's ``DefinitionS3Location``
references, and the live state machine — with TWO writers reading two
different sources. ``deploy_step_function.sh`` updated the live machine from
the LOCAL file without refreshing the S3 object (and without
``--logging-configuration``), so a stale S3 copy sat armed: any CFN
restamp/replacement would silently roll the live definition back to old
bytes, and a recreate would drop execution logging (the config#1464 class,
hit live by the ne-* rename config#1381).

The codified contract (this test fails loudly the moment any leg is removed):

  1. The repo file is the SOLE source of truth. ``deploy_step_function.sh``
     uploads the stamped repo bytes to EXACTLY the S3 bucket/key CFN's
     ``SaturdayPipeline.DefinitionS3Location`` declares — no path may exist
     where CFN and the script can write/read different bytes.
  2. The S3 upload happens BEFORE any update/create-state-machine call.
  3. The SAME stamped artifact feeds both the S3 upload and the
     ``--definition`` apply.
  4. Every update/create passes ``--logging-configuration`` EXPLICITLY,
     matching the CFN-declared shape (ERROR / includeExecutionData / the
     CFN-owned log group) — never relying on partial-update preservation.
  5. ``deploy-infrastructure.sh`` (the on-merge writer) passes explicit
     logging configs for the CFN-managed pair too, mirroring its EOD
     precedent (config#1416).

Raw-text parsing per the existing test_deploy_step_function_eventbridge_input
precedent — CFN's !Ref / !Sub / !GetAtt tags require a custom YAML loader.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEPLOY_SF = _INFRA / "deploy_step_function.sh"
_DEPLOY_INFRA = _INFRA / "deploy-infrastructure.sh"
_CFN = _INFRA / "cloudformation" / "alpha-engine-orchestration.yaml"

_TOP_LEVEL_KEY_RE = re.compile(r"^  ([A-Za-z0-9]+):\s*$", re.MULTILINE)


def _script_text() -> str:
    assert _DEPLOY_SF.is_file(), f"missing {_DEPLOY_SF}"
    return _DEPLOY_SF.read_text()


def _cfn_block(logical_id: str) -> str:
    """Slice one top-level CFN resource block out of the template text."""
    text = _CFN.read_text()
    matches = list(_TOP_LEVEL_KEY_RE.finditer(text))
    for i, m in enumerate(matches):
        if m.group(1) == logical_id:
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            return text[m.end():end]
    raise AssertionError(f"CFN resource {logical_id} not found in {_CFN}")


def _cfn_definition_s3_location() -> tuple[str, str]:
    block = _cfn_block("SaturdayPipeline")
    bucket = re.search(r"Bucket:\s*(\S+)", block)
    key = re.search(r"Key:\s*(\S+)", block)
    assert bucket and key, "SaturdayPipeline has no parseable DefinitionS3Location"
    return bucket.group(1), key.group(1)


def _shell_var(text: str, name: str) -> str:
    m = re.search(rf'^{name}="([^"]+)"', text, re.MULTILINE)
    assert m, f"deploy_step_function.sh must define {name}=\"...\""
    return m.group(1)


# ── 1. script's S3 target == CFN's DefinitionS3Location ─────────────────────


def test_script_uploads_to_exact_cfn_definition_s3_location() -> None:
    """The single-writer invariant: the script's upload target and CFN's
    read-source are the same object, pinned to the same literals."""
    text = _script_text()
    cfn_bucket, cfn_key = _cfn_definition_s3_location()
    assert _shell_var(text, "WEEKLY_SF_S3_BUCKET") == cfn_bucket, (
        "deploy_step_function.sh WEEKLY_SF_S3_BUCKET must equal the CFN "
        "SaturdayPipeline DefinitionS3Location Bucket (config#2273)."
    )
    assert _shell_var(text, "WEEKLY_SF_S3_KEY") == cfn_key, (
        "deploy_step_function.sh WEEKLY_SF_S3_KEY must equal the CFN "
        "SaturdayPipeline DefinitionS3Location Key (config#2273)."
    )
    assert re.search(
        r'aws s3 cp "\$\w+" "s3://\$\{WEEKLY_SF_S3_BUCKET\}/\$\{WEEKLY_SF_S3_KEY\}"',
        text,
    ), (
        "deploy_step_function.sh must upload the stamped definition to "
        "s3://${WEEKLY_SF_S3_BUCKET}/${WEEKLY_SF_S3_KEY} — removing the "
        "upload re-arms the stale-S3 CFN-rollback hazard (config#2273)."
    )


# ── 2. ordering: upload before any state-machine write ──────────────────────


def test_s3_upload_precedes_state_machine_write() -> None:
    text = _script_text()
    upload_at = text.find("aws s3 cp")
    write_m = re.search(r"aws stepfunctions (update|create)-state-machine", text)
    assert upload_at != -1, "expected an `aws s3 cp` upload of the definition"
    assert write_m, "expected an update/create-state-machine apply"
    assert upload_at < write_m.start(), (
        "the S3 upload must run BEFORE update/create-state-machine so the "
        "CFN read-source never lags the live machine (config#2273)."
    )


# ── 3. same stamped bytes to S3 and to --definition ─────────────────────────


def test_same_stamped_artifact_uploaded_and_applied() -> None:
    text = _script_text()
    upload_m = re.search(r'aws s3 cp "\$(\w+)" "s3://', text)
    assert upload_m, "expected `aws s3 cp \"$<var>\" \"s3://...\"`"
    stamped_var = upload_m.group(1)

    definition_args = re.findall(r'--definition "file://\$(\w+)"', text)
    assert definition_args, (
        "update/create-state-machine must pass --definition via file:// "
        "(inline $(cat ...) blows ARG_MAX on the ~130 KB weekly ASL — the "
        "2026-06-04 regression class)."
    )
    assert all(v == stamped_var for v in definition_args), (
        f"every --definition must apply the SAME stamped artifact uploaded "
        f"to S3 (${stamped_var}); got {definition_args} — diverging here "
        f"recreates the two-writers-two-sources bug (config#2273)."
    )


def test_no_inline_cat_definition_path_remains() -> None:
    text = _script_text()
    assert not re.search(r'DEFINITION=\$\(cat\b', text), (
        "the inline DEFINITION=$(cat ...) read path must not come back — it "
        "bypasses the stamped artifact and blows ARG_MAX (config#2273)."
    )


def test_definition_is_stamped_from_the_repo_file() -> None:
    text = _script_text()
    assert re.search(r'ASL_FILE="\$SCRIPT_DIR/step_function\.json"', text), (
        "the deploy must read the repo file infrastructure/step_function.json "
        "— it is the sole source of truth (config#2273)."
    )
    assert '"$ASL_FILE" "$STAMPED_ASL" "$GIT_SHA"' in text, (
        "the git-SHA stamp step (repo file -> stamped artifact) is missing — "
        "both deploy paths must emit byte-identical stamped artifacts for the "
        "same commit (mirrors deploy-infrastructure.sh)."
    )


# ── 4. explicit logging configuration on every write ────────────────────────


def _state_machine_write_calls(text: str) -> list[str]:
    """Each update/create-state-machine invocation's argument text (up to the
    terminating `--region` arg)."""
    calls = []
    for m in re.finditer(r"aws stepfunctions (?:update|create)-state-machine", text):
        tail = text[m.end():]
        region_at = tail.find("--region")
        calls.append(tail[: region_at if region_at != -1 else len(tail)])
    return calls


def test_every_state_machine_write_passes_explicit_logging_configuration() -> None:
    text = _script_text()
    calls = _state_machine_write_calls(text)
    assert calls, "expected update/create-state-machine calls"
    for call in calls:
        assert "--logging-configuration" in call, (
            "every update/create-state-machine call must pass "
            "--logging-configuration explicitly — partial-update preservation "
            "is exactly what a recreate silently drops (config#2273 "
            "deliverable 3; bug class config#1464)."
        )


def test_logging_config_matches_cfn_declared_shape() -> None:
    """The script reconstructs the CFN-declared LoggingConfiguration; pin the
    shape AND that the log group is the CFN-owned WeeklyFreshnessLogGroup."""
    text = _script_text()
    assert '"level":"ERROR"' in text
    assert '"includeExecutionData":true' in text

    # CFN-declared shape for the weekly SF.
    sat_block = _cfn_block("SaturdayPipeline")
    assert re.search(r"Level:\s*ERROR", sat_block)
    assert re.search(r"IncludeExecutionData:\s*true", sat_block)

    log_group_block = _cfn_block("WeeklyFreshnessLogGroup")
    cfn_log_group = re.search(r"LogGroupName:\s*(\S+)", log_group_block)
    assert cfn_log_group, "WeeklyFreshnessLogGroup has no LogGroupName"

    # The script builds the ARN as .../aws/stepfunctions/${STATE_MACHINE_NAME};
    # resolve the variable and compare against the CFN log group name.
    sm_name = _shell_var(text, "STATE_MACHINE_NAME")
    assert f"/aws/stepfunctions/{sm_name}" == cfn_log_group.group(1), (
        "the script's logging destination must be the CFN-owned "
        "WeeklyFreshnessLogGroup — a diverging log group silently breaks the "
        "L274 MutexConflict metric-filter chain (config#729)."
    )
    assert re.search(
        r"log-group:/aws/stepfunctions/\$\{STATE_MACHINE_NAME\}", text
    ), "LOG_GROUP_ARN must target /aws/stepfunctions/${STATE_MACHINE_NAME}"


# ── 5. the on-merge writer (deploy-infrastructure.sh) — CFN pair logging ────


def test_deploy_infrastructure_passes_logging_for_cfn_managed_pair() -> None:
    text = _DEPLOY_INFRA.read_text()
    assert re.search(
        r'update_or_defer_to_cfn "\$SAT_ARN"\s+"\$SAT_STAMPED"\s+"[^"]+"\s+"\$SAT_LOGGING_CONFIG"',
        text,
    ), (
        "deploy-infrastructure.sh must pass $SAT_LOGGING_CONFIG to the weekly "
        "SF update (config#2273 deliverable 3)."
    )
    assert re.search(
        r'update_or_defer_to_cfn "\$DAILY_ARN"\s+"\$DAILY_STAMPED"\s+"[^"]+"\s+"\$DAILY_LOGGING_CONFIG"',
        text,
    ), (
        "deploy-infrastructure.sh must pass $DAILY_LOGGING_CONFIG to the "
        "preopen SF update (config#2273 deliverable 3)."
    )
    # The helper must actually forward it.
    helper = text.split("update_or_defer_to_cfn() {", 1)[1].split("\n}", 1)[0]
    assert "--logging-configuration" in helper, (
        "update_or_defer_to_cfn must forward the logging arg via "
        "--logging-configuration."
    )


def test_deploy_infrastructure_logging_literals_target_cfn_owned_log_groups() -> None:
    text = _DEPLOY_INFRA.read_text()
    # Capture the full assignment line — the literal embeds '"$VAR"' shell
    # quote-breaks, so a naive '([^']+)' capture stops early.
    sat = re.search(r"^SAT_LOGGING_CONFIG='(.+)'$", text, re.MULTILINE)
    daily = re.search(r"^DAILY_LOGGING_CONFIG='(.+)'$", text, re.MULTILINE)
    assert sat and daily, "expected SAT_LOGGING_CONFIG / DAILY_LOGGING_CONFIG literals"
    assert "/aws/stepfunctions/ne-weekly-freshness-pipeline" in sat.group(1)
    assert "/aws/stepfunctions/ne-preopen-trading-pipeline" in daily.group(1)
    for blob in (sat.group(1), daily.group(1)):
        assert '"level":"ERROR"' in blob
        assert '"includeExecutionData":true' in blob
