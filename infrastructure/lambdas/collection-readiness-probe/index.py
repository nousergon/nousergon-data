"""alpha-engine-collection-readiness-probe (alpha-engine-config-I11264).

The v1 consumer's question, asked of the standalone collector's run manifests:
"has ``ne-data-collection-<collection>`` published every unit I read, for THIS
cycle?". Invoked by the ``WaitForCollectionManifests`` state of each of the
three v1 state machines (``step_function{,_daily,_eod}.json``), which polls it
under a bounded budget before the first surviving data consumer.

It is the SAME predicate the producer's own ``VerifyRunManifests`` applies —
``data_gate/run_manifest_predicate.py::readiness_check`` over the shared
``_check_unit`` — packaged beside the descriptors it grades, not a second
implementation of it.

Why a separate function and not an action on the data-spot dispatcher, which
already runs that predicate for the producer: the dispatcher is the function
that LAUNCHES collector boxes. A v1 state machine allowed to invoke it is one
Payload edit away from a second writer of ``market_data/*`` and the ArcticDB
libraries, which is the path the decoupled cutover closes
(alpha-engine-config-I11266 deliverable 6: no state in the v1 EOD definition
invokes the dispatcher). This function holds no EC2, SSM or write grant at all
(``iam-policy.json``): it can read run manifests and nothing else.

Event (from the v1 ASL): ``{"collection", "units": [...], "not_before":
$$.Execution.StartTime, "lookback_seconds": int}``. Returns
``{"readiness": {"ready", "settled", "missing", "failed", "failure_mode",
"baseline", "summary"}}``. RAISES on anything it cannot measure; the ASL's
Catch counts that as one not-ready poll, so a persistent raise exhausts the
bounded budget and degrades loudly rather than proceeding on an unmeasured
claim.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

#: Keys this function refuses, so a mis-wired v1 state cannot read as a
#: readiness poll while meaning something else. `workload` is the dispatcher's
#: launch key; `action` is its routing key.
_REFUSED_KEYS = ("workload", "action")


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    event = event or {}
    refused = [k for k in _REFUSED_KEYS if k in event]
    if refused:
        raise ValueError(
            f"collection-readiness-probe received {refused}: it answers one question "
            "(readiness over run manifests) and launches nothing. A caller sending a "
            "dispatcher key is mis-wired."
        )
    from data_gate import run_manifest_predicate

    return run_manifest_predicate.readiness_check(event)
