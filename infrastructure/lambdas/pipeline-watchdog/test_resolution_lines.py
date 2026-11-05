"""alpha-engine-config-I13257: watchdog alerts name the issue that resolves them.

flow-doctor >= 0.19.7 moves a sender's own ``Resolution:`` line into the
Telegram footer in place of "none tracked yet".
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from test_handler import index  # noqa: E402 — reuses that module's lib stubs


def test_known_alert_carries_its_resolution_line():
    out = index._with_resolution("EOD SF has not executed in the last 19h on a trading-day window.")
    assert out.splitlines()[-1].startswith("Resolution: aec#13255")


def test_unknown_alert_and_declared_resolution_are_left_alone():
    msg = "EOD reconcile SF has not executed in the last 19h on a trading-day window."
    assert index._with_resolution(msg) == msg
    own = "EOD SF has not executed\nResolution: aec#1"
    assert index._with_resolution(own) == own


def test_publish_sends_the_resolution_to_both_channels():
    index._publish_watchdog_alert(
        "Saturday SF has not executed in the last 168h", severity="error",
        dedup_key="k", context={},
    )
    assert "Resolution: aec#13255" in index.alerts.publish.call_args.kwargs["message"]
    assert "Resolution: aec#13255" in index.notify_via_flow_doctor.call_args.args[0]
