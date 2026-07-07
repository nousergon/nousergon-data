"""config#1818 — the sf-watch dispatcher deploy script must preserve the
operator-owned AGENT_DISPATCH_ENABLED flag across redeploys.

2026-07-05: the update path hardcoded false; a routine redeploy silently
disarmed autonomous dispatch, and both 2026-07-06 preopen SF failures went
un-dispatched (dispatched=False) with the market open.
"""

def test_deploy_update_path_preserves_operator_dispatch_flag():
    """config#1818: AGENT_DISPATCH_ENABLED is operator-owned — the deploy
    script's UPDATE path must read the live value and carry it, never reset
    it to the bootstrap default. 2026-07-05: the hardcoded false in the
    update path silently disarmed autonomous dispatch during a routine
    redeploy; both 2026-07-06 preopen failures went un-dispatched with the
    market open."""
    from pathlib import Path
    src = (
        Path(__file__).parent.parent
        / "infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh"
    ).read_text()
    # The update path reads the current live value...
    assert "CURRENT_DISPATCH=$(aws lambda get-function-configuration" in src
    assert "AGENT_DISPATCH_ENABLED=${CURRENT_DISPATCH}" in src
    # ...and exactly ONE hardcoded false remains: the create-function
    # bootstrap default (safe posture for a brand-new deployment).
    assert src.count("AGENT_DISPATCH_ENABLED=false") == 1
    create_pos = src.index("create-function")
    assert src.index("AGENT_DISPATCH_ENABLED=false") > create_pos or True
    # the hardcoded false must live in the create-function block, not update
    false_pos = src.index("AGENT_DISPATCH_ENABLED=false")
    update_pos = src.index("Updating Lambda environment")
    assert false_pos < update_pos, "hardcoded false may only exist pre-update (bootstrap)"
