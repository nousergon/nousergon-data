"""alpha-engine-config-I10880: a shadow run reports flow-doctor diagnosis
under its own flow name, so it draws its own rate-limiter budget instead of
sharing production `data-collector`'s.

Measured 2026-09-15: a `shadow-weekday` run filed nousergon-data-I1743/I1752
under `flow=data-collector`, and its diagnosis attempts counted against the
production flow's `max_diagnosed_per_day: 3` — a real production failure
later the same day would have found the budget already spent by a shadow
run.

``weekly_collector.py`` computes ``_FLOW_NAME`` at import time from
``DATA_COLLECTION_SHADOW_TRADING_DAY`` (set by `python -m shadow run` on the
child process before this module is imported — `shadow.root.ENV_TRADING_DAY`)
and passes it to `krepis.logging.setup_logging(..., flow_name=...)`, which
flow-doctor's `RateLimiter` keys every daily budget on
(`flow_doctor/core/rate_limiter.py::RateLimiter.check` ->
`store.count_actions_today(action, self.flow_name)`) — so this override
alone is sufficient to give the shadow workload a separate budget.

Run in a subprocess rather than via a direct import: several other test
modules already import `weekly_collector` at collection time, and its
`_FLOW_NAME` is computed once, at that first import, from whatever the
environment happened to be then. Reusing the in-process module object here
would read a stale value; reloading it would re-run its (heavy) collectors
import for the rest of the session.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

_PROBE = "import weekly_collector; print(weekly_collector._FLOW_NAME)"


def _flow_name_for(extra_env: dict[str, str]) -> str:
    env = dict(os.environ)
    env["FLOW_DOCTOR_DISABLED"] = "1"
    env.pop("DATA_COLLECTION_SHADOW_TRADING_DAY", None)
    env.update(extra_env)
    result = subprocess.run(
        [sys.executable, "-c", _PROBE],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"weekly_collector import failed:\nstdout={result.stdout}\n"
        f"stderr={result.stderr}"
    )
    lines = [ln for ln in result.stdout.strip().splitlines() if ln]
    assert lines, f"no output; stderr={result.stderr}"
    return lines[-1]


def test_flow_name_is_production_without_shadow_env():
    assert _flow_name_for({}) == "data-collector"


def test_flow_name_is_shadow_when_shadow_trading_day_set():
    assert (
        _flow_name_for({"DATA_COLLECTION_SHADOW_TRADING_DAY": "2026-09-16"})
        == "data-collector-shadow"
    )


def test_flow_name_reverts_to_production_when_env_cleared():
    """Not a cache: a fresh process with the env var absent again reports
    the production flow — this is a per-process env read, not a sticky
    global toggled once and left on."""
    assert (
        _flow_name_for({"DATA_COLLECTION_SHADOW_TRADING_DAY": "2026-09-16"})
        == "data-collector-shadow"
    )
    assert _flow_name_for({}) == "data-collector"
