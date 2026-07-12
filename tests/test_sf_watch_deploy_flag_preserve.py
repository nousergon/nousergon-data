"""config#1818 / config#2236 / config#2264 — dispatcher deploy scripts must
preserve operator-owned kill-switch flags across redeploys.

2026-07-05: the saturday dispatcher's update path hardcoded false; a routine
redeploy silently disarmed autonomous dispatch, and both 2026-07-06 preopen SF
failures went un-dispatched (dispatched=False) with the market open. The same
clobber class recurred in sf-watch-spot-dispatcher (config#2236, re-arm
direction) and ci-watch-dispatcher (config#2264, re-arm direction). 3rd
instance = consolidation: the preserve logic now lives in ONE shared sourced
helper, infrastructure/lambdas/_shared/preserve_env_flags.sh.
"""

from pathlib import Path

LAMBDAS_DIR = Path(__file__).parent.parent / "infrastructure/lambdas"
HELPER_REL = "_shared/preserve_env_flags.sh"
SOURCE_LINE = 'source "${SCRIPT_DIR}/../_shared/preserve_env_flags.sh"'

# dispatcher -> list of (flag_name, bootstrap_default, preserved_shell_var)
OPERATOR_FLAGS = {
    "saturday-sf-watch-dispatcher": [
        ("AGENT_DISPATCH_ENABLED", "false", "CURRENT_DISPATCH"),
        ("FAST_PATH_ENABLED", "false", "CURRENT_FAST_PATH"),
        # config#2003: post-escalation dispatch override — operator-owned
        ("EOD_SF_WATCH_DISPATCH_AFTER_ESCALATION", "false", "CURRENT_DISPATCH_AFTER_ESCALATION"),
    ],
    "sf-watch-spot-dispatcher": [
        ("SF_WATCH_DISPATCH_ENABLED", "true", "CURRENT_DISPATCH"),
    ],
    "ci-watch-dispatcher": [
        ("CI_WATCH_DISPATCH_ENABLED", "true", "CURRENT_DISPATCH"),
    ],
}


def test_shared_helper_reads_live_value():
    """The shared helper must query the LIVE function config and validate the
    value, falling back to the caller-supplied default only for non-boolean
    reads (missing var / fresh bootstrap)."""
    src = (LAMBDAS_DIR / HELPER_REL).read_text()
    assert "preserve_env_flag()" in src, "helper must define preserve_env_flag()"
    assert "aws lambda get-function-configuration" in src, \
        "helper must query the live function configuration"
    assert '--query "Environment.Variables.${var}"' in src, \
        "helper must read the flag's live value"
    assert 'case "${val}" in true|false) ;; *) val="${default}" ;; esac' in src, \
        "helper must validate true|false and fall back to the default"


def test_deploy_update_path_preserves_operator_dispatch_flag():
    """config#1818/#2236/#2264: operator-owned flags — every dispatcher deploy
    script's UPDATE path must read the live value (via the shared helper) and
    carry it, never reset to bootstrap defaults."""
    for dispatcher, flags in OPERATOR_FLAGS.items():
        src = (LAMBDAS_DIR / dispatcher / "deploy.sh").read_text()

        # Each deploy.sh sources the ONE shared helper — no hand-copied forks.
        assert SOURCE_LINE in src, \
            f"{dispatcher}: must source the shared {HELPER_REL} helper"
        assert "aws lambda get-function-configuration" not in src, \
            f"{dispatcher}: live read belongs in the shared helper, not inline"

        for flag_name, default_val, shell_var in flags:
            # The update path reads the current live value via the helper...
            call = (
                f'{shell_var}=$(preserve_env_flag "${{FUNCTION_NAME}}" '
                f'"${{REGION}}" {flag_name} {default_val})'
            )
            assert call in src, \
                f"{dispatcher}: update path must preserve {flag_name} via the helper"
            # ...and the env applied on update carries the preserved value —
            # either inline (`Variables={...,FLAG=${VAR},...}`) or through a
            # JSON env-builder taking the preserved var as its argument
            # (sf-watch-spot-dispatcher's lambda_env_json, config#2265).
            inline_carry = f"{flag_name}=${{{shell_var}}}" in src
            builder_carry = (
                f'lambda_env_json "${{{shell_var}}}"' in src
                and f'"{flag_name}":"%s"' in src
            )
            assert inline_carry or builder_carry, \
                f"{dispatcher}: update env must use the preserved {flag_name} value"

            # The hardcoded default may only exist in the bootstrap
            # (create-function) posture, never in the update path's env.
            assert "create-function" in src, f"{dispatcher}: bootstrap block missing"
            hardcoded = (
                f"{flag_name}={default_val}" if inline_carry
                else f"lambda_env_json {default_val}"
            )
            assert src.count(hardcoded) >= 1, \
                f"{dispatcher}: must keep one hardcoded {hardcoded} (bootstrap default)"
            update_pos = src.index("Updating Lambda environment")
            assert src.rindex(hardcoded) < update_pos, \
                f"{dispatcher}: hardcoded {hardcoded} may only exist pre-update (bootstrap)"
