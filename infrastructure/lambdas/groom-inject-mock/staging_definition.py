"""Build the fault-injection staging state machine FROM the production
definition (alpha-engine-config-I5718).

The whole value of this harness rests on one property: **the topology under
test is the production topology.** A hand-written staging definition would
drift within a week and would then be testing a state machine that does not
exist, which is worse than testing nothing because it reports green.

So the staging machine is *derived*, not authored. Exactly three
transformations are applied to `infrastructure/step_function_groom.json`, and
each one is here because leaving it out would make the harness unusable or
dangerous:

1. **Lambda ARN -> the mock.** Otherwise an injection run launches real spot
   boxes against the real backlog.
2. **SNS topic -> a staging topic.** Otherwise every injection run pages Brian
   on the real alerts topic. This is the transformation most likely to be
   forgotten and the most obnoxious when it is.
3. **Timeouts scaled down.** The lane timeout is 21600s; a harness that takes
   six hours to observe one timeout path will never run. Scaling preserves the
   ORDERING of budgets (which is what the recovery logic branches on) while
   making the run take seconds.

Everything else — state names, Choice conditions, Catch blocks, Retry policy,
the relaunch budget, ResultPath wiring — is passed through untouched. If a
transformation beyond these three is ever needed, that is a signal the
production definition is not testable, and the fix belongs there.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

#: Applied to every TimeoutSeconds in the definition, including the top-level
#: one. 720 turns the 21600s lane budget into 30s and the 72000s execution
#: ceiling into 100s, so a full three-attempt relaunch exhaustion — the exact
#: 18h shape observed in production on 2026-07-29 — completes in ~90 seconds.
TIMEOUT_DIVISOR = 720

#: Never scale below this. A 360s state at /720 would be 0.5s and would start
#: failing on Lambda cold starts, turning the harness flaky — which is how an
#: exercise programme gets switched off.
MIN_TIMEOUT_SECONDS = 5

PROD_LAMBDA = "alpha-engine-scheduled-groom-dispatcher"
MOCK_LAMBDA = "alpha-engine-groom-inject-mock"
PROD_SNS_PATTERN = re.compile(r"arn:aws:sns:[^\"]*:alpha-engine-alerts")
STAGING_SNS_NAME = "alpha-engine-groom-inject-alerts"


def _scale_timeouts(node: Any) -> Any:
    """Recursively scale every TimeoutSeconds, preserving budget ORDER."""
    if isinstance(node, dict):
        out = {}
        for key, value in node.items():
            if key == "TimeoutSeconds" and isinstance(value, (int, float)):
                out[key] = max(MIN_TIMEOUT_SECONDS, int(value) // TIMEOUT_DIVISOR)
            else:
                out[key] = _scale_timeouts(value)
        return out
    if isinstance(node, list):
        return [_scale_timeouts(item) for item in node]
    return node


def build_staging_definition(prod_definition: dict, *, account_id: str,
                             region: str) -> dict:
    """Return the staging definition derived from ``prod_definition``.

    Pure: no AWS calls, no file IO. That is what lets the unit tests assert the
    transformation without credentials.
    """
    staging_topic = f"arn:aws:sns:{region}:{account_id}:{STAGING_SNS_NAME}"

    raw = json.dumps(prod_definition)
    if PROD_LAMBDA not in raw:
        raise ValueError(
            f"production definition names no {PROD_LAMBDA!r} — the Lambda was "
            "renamed and this builder would have produced a staging machine "
            "still pointing at production"
        )
    raw = raw.replace(PROD_LAMBDA, MOCK_LAMBDA)

    raw, sns_swaps = PROD_SNS_PATTERN.subn(staging_topic, raw)
    if sns_swaps == 0:
        raise ValueError(
            "production definition names no alpha-engine-alerts topic — either "
            "alerting moved, or this builder is about to ship a staging machine "
            "that publishes somewhere unaudited. Fix the pattern deliberately."
        )

    definition = _scale_timeouts(json.loads(raw))
    definition["Comment"] = (
        "FAULT-INJECTION STAGING — derived from step_function_groom.json by "
        "staging_definition.py (alpha-engine-config-I5718). DO NOT EDIT: "
        "regenerate instead. Lambda -> inject mock, SNS -> staging topic, "
        f"timeouts /{TIMEOUT_DIVISOR}. Original comment: "
        + str(prod_definition.get("Comment", ""))[:400]
    )
    return definition


def load_prod_definition(repo_root: Path | None = None) -> dict:
    root = repo_root or Path(__file__).resolve().parents[3]
    return json.loads(
        (root / "infrastructure" / "step_function_groom.json").read_text()
    )
