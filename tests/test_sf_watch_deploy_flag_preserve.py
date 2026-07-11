"""config#1818 — the sf-watch dispatcher deploy script must preserve the
operator-owned AGENT_DISPATCH_ENABLED flag across redeploys.

2026-07-05: the update path hardcoded false; a routine redeploy silently
disarmed autonomous dispatch, and both 2026-07-06 preopen SF failures went
un-dispatched (dispatched=False) with the market open.
"""

def test_deploy_update_path_preserves_operator_dispatch_flag():
    """config#1818/config#2236: AGENT_DISPATCH_ENABLED and SF_WATCH_DISPATCH_ENABLED
    are operator-owned — deploy scripts' UPDATE paths must read the live value and
    carry it, never reset to bootstrap defaults. 2026-07-05 incident: the hardcoded
    false in the saturday dispatcher's update path silently disarmed autonomous
    dispatch during a routine redeploy; both 2026-07-06 preopen failures went
    un-dispatched with the market open. Same bug class in spot dispatcher (config#2236)."""
    from pathlib import Path

    for dispatcher in ["saturday-sf-watch-dispatcher", "sf-watch-spot-dispatcher"]:
        src = (
            Path(__file__).parent.parent
            / f"infrastructure/lambdas/{dispatcher}/deploy.sh"
        ).read_text()

        if dispatcher == "saturday-sf-watch-dispatcher":
            flag_name = "AGENT_DISPATCH_ENABLED"
        else:
            flag_name = "SF_WATCH_DISPATCH_ENABLED"

        # The update path reads the current live value...
        assert "CURRENT_DISPATCH=$(aws lambda get-function-configuration" in src, \
            f"{dispatcher}: update path must query live value"
        assert f"{flag_name}=${{CURRENT_DISPATCH}}" in src, \
            f"{dispatcher}: update path must use preserved value"

        # ...and exactly ONE hardcoded false/true in bootstrap (create-function).
        # Saturday dispatcher uses false default; spot uses true default.
        default_val = "false" if dispatcher == "saturday-sf-watch-dispatcher" else "true"
        hardcoded_count = src.count(f"{flag_name}={default_val}")
        assert hardcoded_count >= 1, \
            f"{dispatcher}: must have at least one hardcoded {flag_name}={default_val} (bootstrap)"

        # Verify the hardcoded value is in the bootstrap section, not update
        create_pos = src.index("create-function")
        default_pos = src.index(f"{flag_name}={default_val}")
        update_pos = src.index("Updating Lambda environment")
        assert default_pos < update_pos, \
            f"{dispatcher}: hardcoded {flag_name}={default_val} may only exist in bootstrap, not update path"
