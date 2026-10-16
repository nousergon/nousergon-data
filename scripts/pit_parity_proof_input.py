#!/usr/bin/env python3
"""Emit the StartExecution input for ONE pit_parity proof run (alpha-engine-config-I7309).

Print-only. This script calls no AWS API and starts nothing; it prints the
input and the exact ``start-execution`` command for the operator who has been
authorised to run it.

Why it exists
-------------
The 2026-08-26 ruling on I7309 requires one ad-hoc proof run that yields a
real verdict (PASS or FAIL) before ``skip_parity`` leaves the cadence trigger.
The fixes it has to prove (crucible-backtester PR729/PR732) have never
completed end-to-end at production size: nothing has landed under
``parity/`` since 2026-08-13.

The proof input written into the issue so far was a bare
``{"skip_parity": false}``. On this state machine that is not a parity proof.
It is a whole weekly run:

* it carries no ``pipeline_role``, so ``CheckWeeklyRunDayGate`` lets it through
  on any day;
* every other stage takes its gate's default and runs, including the
  side-effecting ones: PredictorTraining, Backtester, Evaluator (which promotes
  and writes config), ReportCard and Director. Its name does not start with
  ``rehearsal-``, so crucible-backtester's live-config write guard does not
  apply either.

Neither existing tool can build the input instead:

* ``weekly_sf_rerun.py`` only recovers a FAILED/TIMED_OUT/ABORTED execution,
  and it re-applies the cadence's declared ``skip_parity`` on purpose
  (alpha-engine-config-I8153);
* the rehearsal door (``nous-ergon-ops`` ``weekly_sf_rehearsal_guard.py``) pins
  ``skip_parity: true`` in its fixed input.

What the input does
-------------------
Every ``skip_*`` gate the definition reads is set, either to skip or, for the
parity family, to run, so no stage relies on a default. Two non-obvious flags:

* ``skip_backtester`` must stay FALSE. ``CheckSkipBacktester``'s skip route
  overshoots to ``MarkParityVerdictUnknownByCadence``, past the whole parity
  family, so ``skip_backtester: true`` would also skip parity. The Backtester
  stage itself is bypassed by ``skip_backtester_stage_only``, and the two model
  backtests by their own flags.
* ``skip_post_eval`` is TRUE, which bypasses the whole advisory tail
  (health checks, ReportCard, Director, ScannerLeaderboard).

``tests/test_pit_parity_proof_input.py`` checks this against the checked-in
definition using ``weekly_sf_rerun._simulate_reachable_works``: exactly
LibPinDriftCheck and the parity family run.

Why the run_date must be Monday to Wednesday
--------------------------------------------
``backtest/{run_date}/pit_parity.json`` is the contamination half of that
cycle's attestation. ``crucible-evaluator`` ``grading/attestation.py``
``_mark_scope_out_of_scope`` keeps a verdict it READ over the run-scope's "not
dispatched" (alpha-engine-config-I8811: a claim about dispatch never overwrites
evidence). So a proof dated on a weekly cycle's run_date becomes that cycle's
contamination verdict. A proof execution on a cycle's run_date also joins that
date's ``weekly_sf_rerun`` recovery chain, and its skips would read as
completed stages (alpha-engine-config-I8161).

A weekly cycle's run_date is the week's last NYSE session. That is a Friday,
or a Thursday in a holiday week, and never earlier because NYSE never closes
on both Thursday and Friday of the same week. A Monday-to-Wednesday run_date
therefore belongs to no cycle. Pass the session that has closed: features for
that date must already exist.

Usage
-----
    python3 scripts/pit_parity_proof_input.py --run-date 2026-10-20 [--suffix 1]
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import shlex
import sys
from datetime import date
from pathlib import Path

_HERE = Path(__file__).resolve().parent


def _load_rerun_module():
    """``weekly_sf_rerun`` loaded by path, the same way its tests load it, so
    the stage table is shared and is not copied here."""
    if "weekly_sf_rerun" in sys.modules:
        return sys.modules["weekly_sf_rerun"]
    spec = importlib.util.spec_from_file_location(
        "weekly_sf_rerun", _HERE / "weekly_sf_rerun.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["weekly_sf_rerun"] = mod
    spec.loader.exec_module(mod)
    return mod


_rerun = _load_rerun_module()

#: The stages a proof run exists to run. ``backtester`` is listed because its
#: flag must stay false for the parity family to be reachable at all (see the
#: module docstring); the Backtester work state is bypassed by
#: ``skip_backtester_stage_only`` below.
PROOF_STAGES = frozenset({
    "backtester",
    "parity",
    "pit_parity_lookahead",
    "pit_parity_walkforward",
    "parity_replay",
    "pit_parity_compare",
})

#: Gates the stage table does not emit, set explicitly.
EXTRA_SKIPS = {
    "skip_backtester_stage_only": True,
    "skip_post_eval": True,
}

#: Gates left at their defaults on purpose. ``LibPinDriftCheck`` is a read-only
#: check, and ``skip_weekly_run_day_gate`` is moot because ``watch-rerun`` is
#: not the ``weekly`` role that gate applies to.
LEFT_AT_DEFAULT = frozenset({"skip_lib_pin_drift_check", "skip_weekly_run_day_gate"})

#: Not ``weekly``, so neither the NYSE run-day gate nor the run-slot mutex
#: applies (the convention ``weekly_sf_rerun.py`` documents for operator runs).
PIPELINE_ROLE = _rerun.EMITTED_ROLE

_ACCOUNT_REGION = _rerun.DEFAULT_STATE_MACHINE_ARN.split(":")[3:5]
#: Alerts go to the muted topic; the operator watches this run directly.
MUTED_TOPIC_ARN = "arn:aws:sns:{}:{}:alpha-engine-alerts-muted".format(*_ACCOUNT_REGION)

NAME_PREFIX = "pit-parity-proof-"


class ProofInputError(ValueError):
    pass


def proof_skip_flags() -> dict:
    flags = {}
    for stage in _rerun.STAGES:
        if stage.name in PROOF_STAGES:
            flags[stage.flag] = False
        elif stage.emit_skip:
            flags[stage.flag] = True
    flags.update(EXTRA_SKIPS)
    return flags


def check_run_date(run_date: str) -> date:
    try:
        d = date.fromisoformat(run_date)
    except ValueError as exc:
        raise ProofInputError(f"run_date {run_date!r} is not YYYY-MM-DD") from exc
    if d.weekday() > 2:
        raise ProofInputError(
            f"run_date {run_date} is a {d:%A}. A proof run must be dated "
            "Monday to Wednesday: a Thursday or Friday can be a weekly cycle's "
            "run_date, and its pit_parity.json would become that cycle's "
            "contamination verdict (see this script's docstring)."
        )
    return d


def build_proof_input(run_date: str) -> dict:
    check_run_date(run_date)
    return {
        "sns_topic_arn": MUTED_TOPIC_ARN,
        "pipeline_role": PIPELINE_ROLE,
        "run_date": run_date,
        "calendar_date": run_date,
        **proof_skip_flags(),
    }


def execution_name(run_date: str, suffix: str) -> str:
    if not re.fullmatch(r"[a-z0-9]{1,12}", suffix or ""):
        raise ProofInputError(f"suffix {suffix!r} must be lowercase letters/digits")
    return f"{NAME_PREFIX}{run_date}-{suffix}"


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--run-date", required=True, help="closed NYSE session, Monday-Wednesday")
    ap.add_argument("--suffix", default="1")
    args = ap.parse_args(argv)
    try:
        payload = build_proof_input(args.run_date)
        name = execution_name(args.run_date, args.suffix)
    except ProofInputError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 1
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    print(json.dumps(payload, sort_keys=True, indent=2))
    print("\n# Operator only (AWS_PROFILE=ne-admin), on a weekday evening ET after")
    print("# the post-close reconcile has SUCCEEDED, with no weekly execution RUNNING:")
    print(
        "aws stepfunctions start-execution --region {} \\\n"
        "  --state-machine-arn {} \\\n"
        "  --name {} \\\n"
        "  --input {}".format(
            _ACCOUNT_REGION[0], _rerun.DEFAULT_STATE_MACHINE_ARN, name, shlex.quote(body)
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
