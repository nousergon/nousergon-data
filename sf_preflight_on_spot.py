"""
sf_preflight_on_spot.py — the weekly SF's SECOND preflight pass, on the box.

alpha-engine-config-I11312. ``WeeklyPreflight`` (the Lambda) runs
``sf_preflight`` under ``LAMBDA_CAPABILITIES`` = {CAP_AWS}, so every check
that needs ArcticDB, the repo's collector modules or a Polygon key has
ALWAYS skipped there (``required_skip_count: 8`` on rehearsal
``rehearsal-2026-09-23-2``). The weekly box already carries those
capabilities (``/home/ec2-user/alpha-engine-data/.venv`` is built from this
repo's own ``requirements.txt`` by the weekly-freshness-spot bootstrap,
alpha-engine-config-I7427), so this module runs exactly those checks THERE,
instead of rebuilding their environment inside a Lambda.

Where it runs: the state ``WeeklyPreflightOnSpot`` in
``infrastructure/step_function.json``, entered from
``CheckSubstrateHealthGate``'s HEALTHY edge and exiting to ``CheckShellRun``
— i.e. after the box is bootstrapped and proven SSM-responsive, and before
any stage (MorningEnrich is the first data-mutating one) is dispatched.

Which checks: the ones whose declared capabilities are NOT a subset of
``LAMBDA_CAPABILITIES`` (``_spot_checks()``). The AWS-only checks stay with
the Lambda: they read IAM/Lambda/Step Functions control-plane APIs under the
Lambda's own role, and the box's instance role (``alpha-engine-executor-role``)
holds none of ``iam:SimulatePrincipalPolicy`` / ``lambda:GetAlias`` — running
them here would report the BOX's IAM, as fails, not the pipeline's.

Capabilities are DETECTED per run, not assumed (``detect_capabilities``). A
box that has lost arcticdb, the repo modules or the Polygon key reports those
checks as REQUIRED skips — a named gap — never as a fail and never as silence.
``CAP_CHECKOUT`` is never claimed: the box does not carry every sibling
checkout, and that group (price_cards / recursion_budget / tool_contracts) is
alpha-engine-config-I11313's, owned at merge time. Its skips are EXPECTED and
are reported apart (``expected_skip_names``) so they cannot escalate.

OBSERVE MODE (sf-pipeline-policy.md §7a)
----------------------------------------
This pass makes seven checks reachable that could previously never fail, so it
is a change to an existing halting check's reachable inputs (§7a.4) and it
observes before it enforces:

* It never halts the run. Exit code ``OBSERVED_FAIL_EXIT_CODE`` on a fail
  verdict, ``0`` otherwise; the state machine records either and proceeds to
  ``CheckShellRun``. Any crash or timeout inside this module is caught and
  recorded as verdict ``ERROR`` (exit 0); a crash before this module can run
  (missing venv, import error) or an SSM/poll failure lands on
  ``WeeklyPreflightOnSpotUnobserved`` and proceeds too. It fails OPEN.
* It is not silent (§7a obligation 3). A fail verdict is logged at ERROR, is
  published by the state machine as its own notice
  (``PublishWeeklyPreflightOnSpotNotice``), and every verdict is written to
  ``s3://alpha-engine-research/health/weekly_preflight_on_spot/<run_date>/
  <execution_name>.json`` and emitted as the ``AlphaEngine/WeeklyPreflight``
  series with ``Profile=weekly-spot-full`` — the same metric names the Lambda
  emits under its own profile, so the two passes read side by side.

Observe window: TWO cycles — the Friday shell-run rehearsal
(``shell_run=true``) and the Saturday scheduled run that follows it. Both
reach this state (it sits before ``CheckShellRun``).

Promotion criterion (§7a obligation 2): promote ``fail`` to HALTING when two
consecutive observed cycles (a Friday rehearsal and the following Saturday)
each show, in the S3 record above:
  1. ``verdict`` != ``ERROR`` and the SF recorded ``observed: true``;
  2. ``group_required_skip_count == 0`` and ``blocked_count == 0`` — all
     seven ARCTIC/REPO_MODULES/POLYGON checks actually RAN (the issue's
     closes-when). A ``blocked`` check did not run: its upstream failed
     (alpha-engine-config-I11566);
  3. no ``fail`` that was not a true positive — every fail in the window is
     either absent or was confirmed against the system it describes (a fail
     that was the probe's own environment is a false positive and resets the
     count after its fix lands).
Promotion is a separate PR: route ``RecordWeeklyPreflightOnSpotFail`` to
``NormalizeFailureContext`` (via an Extract*Error Pass, the pre-spend-gate
convention) instead of the notice, and delete this paragraph's "observe"
language. Re-exam: 2026-10-05 — the Monday after the second Friday/Saturday
pair following merge, so one slipped cycle (e.g. an unobserved run, or the
companion nousergon-lib registry release landing late) still fits. The date
belongs on alpha-engine-config-I11312 as a `Re-exam:` line; a pass still in
observe mode after it is the §7a failure one direction over.

Usage (on the box, from the data checkout):
    .venv/bin/python sf_preflight_on_spot.py --run-date 2026-09-25 \\
        --execution-name <sf execution name>

stdout is ONE compact JSON line (the verdict summary the state machine
stores verbatim as a string — never parsed in ASL, so no stdout shape can
crash the run). Everything else goes to stderr.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

log = logging.getLogger("sf_preflight_on_spot")

MODE = "observe"
PROFILE = "weekly-spot-full"
DEFAULT_BUCKET = "alpha-engine-research"
# Explicit, never ambient (alpha-engine-config-I11567): the SSM shell this runs
# in exports no AWS_DEFAULT_REGION, so a region-less ``boto3.client
# ("cloudwatch")`` raised NoRegionError and the I11312 observe window recorded
# no metric at all (rehearsal-2026-09-24-1: ``metric_error: "NoRegionError"``).
# Same value as ``sf_preflight._REGION``, pinned equal by
# tests/test_sf_preflight_on_spot.py; restated rather than imported so an
# ERROR verdict whose cause is sf_preflight's own import still emits.
REGION = "us-east-1"
ARTIFACT_PREFIX = "health/weekly_preflight_on_spot"
METRIC_NAMESPACE = "AlphaEngine/WeeklyPreflight"

# The ONE non-zero exit this module chooses on purpose. The state machine's
# CheckWeeklyPreflightOnSpotStatus matches it by ResponseCode to tell an
# observed FAIL verdict apart from a crash — pinned against the definition by
# tests/test_sf_preflight_on_spot_wiring.py. Deliberately clear of python's 1
# (uncaught exception), argparse's 2 and the shell's 126/127.
OBSERVED_FAIL_EXIT_CODE = 10

# Internal wall-clock budget. Below the SSM executionTimeout (600s) in the
# state machine so a hung check becomes a recorded ERROR verdict with a
# written artifact instead of an SSM TimedOut with nothing on S3.
BUDGET_SECONDS = 420

# The capability group this pass exists to reach (alpha-engine-config-I11312).
IN_SCOPE_CAPABILITIES = frozenset({"arctic", "repo_modules", "polygon"})

# Modules the REPO_MODULES capability names (sf_preflight.CAP_REPO_MODULES).
_REPO_MODULES = ("collectors", "features", "builders", "polygon_client")


class _BudgetExceeded(BaseException):
    """BaseException, not Exception: run_preflight wraps each check in
    ``except Exception`` and would otherwise swallow the timeout into one
    check's fail and carry on past the budget."""


def _on_alarm(signum, frame):  # pragma: no cover - signal plumbing
    raise _BudgetExceeded(f"on-spot preflight exceeded its {BUDGET_SECONDS}s budget")


def detect_capabilities() -> frozenset:
    """What THIS host provides, measured — never CAP_CHECKOUT (see module
    docstring). Each probe is independently guarded: a probe that raises
    withholds its capability, which surfaces as a required skip."""
    import sf_preflight as sp

    caps = {sp.CAP_AWS}
    try:
        if importlib.util.find_spec("arcticdb") is not None:
            caps.add(sp.CAP_ARCTIC)
    except Exception as exc:  # noqa: BLE001 - a failed probe withholds the capability
        log.warning("arcticdb probe raised: %s", exc)
    try:
        if all(importlib.util.find_spec(m) is not None for m in _REPO_MODULES):
            caps.add(sp.CAP_REPO_MODULES)
    except Exception as exc:  # noqa: BLE001 - a failed probe withholds the capability
        log.warning("repo-module probe raised: %s", exc)
    try:
        from nousergon_lib.secrets import get_secret

        if get_secret("POLYGON_API_KEY", required=False):
            caps.add(sp.CAP_POLYGON)
    except Exception as exc:  # noqa: BLE001 - a failed probe withholds the capability
        log.warning("POLYGON_API_KEY probe raised: %s", exc)
    return frozenset(caps)


def _spot_checks() -> list:
    """The checks the Lambda profile cannot reach, in CHECKS order."""
    import sf_preflight as sp

    return [
        fn for fn in sp.CHECKS
        if not sp.CHECK_CAPABILITIES.get(fn.__name__, sp.FULL_CAPABILITIES)
        <= sp.LAMBDA_CAPABILITIES
    ]


def classify(results: list) -> dict:
    """Verdict + counts over one run's CheckResults.

    ``required_skip_count`` is ``sf_preflight.summarize_results``'s — the
    same number the Lambda reports, so the two are comparable.
    ``group_required_skip_count`` narrows it to the ARCTIC/REPO_MODULES/
    POLYGON group this pass exists for; it is the closes-when metric and the
    one the promotion criterion reads. A skip whose ONLY missing capability
    is CAP_CHECKOUT is expected here and is listed, not escalated.
    """
    import sf_preflight as sp

    summary = sp.summarize_results(results)
    group_skips, expected_skips = [], []
    for r in summary["skip_results"]:
        fn_name = f"check_{r['name']}"
        needs = sp.CHECK_CAPABILITIES.get(fn_name, sp.FULL_CAPABILITIES)
        missing = set((r.get("details") or {}).get("missing") or ())
        if needs & IN_SCOPE_CAPABILITIES:
            if sp.CHECK_REQUIRED.get(fn_name, True):
                group_skips.append(r["name"])
        elif missing and missing <= {sp.CAP_CHECKOUT}:
            expected_skips.append(r["name"])
    if summary["fail_count"]:
        verdict = "FAIL"
    elif group_skips or summary["blocked_count"]:
        # A blocked check did not run. With no fail beside it (its upstream
        # was never run at all) that is an unobserved check, never an OK.
        verdict = "BLIND_SPOT"
    else:
        verdict = "OK"
    return {
        "verdict": verdict,
        "ran_count": summary["ran_count"],
        "fail_count": summary["fail_count"],
        "warn_count": summary["warn_count"],
        "skip_count": summary["skip_count"],
        "required_skip_count": summary["required_skip_count"],
        "required_skip_names": summary["required_skip_names"],
        "group_required_skip_count": len(group_skips),
        "group_required_skip_names": group_skips,
        "expected_skip_names": expected_skips,
        "blocked_count": summary["blocked_count"],
        "blocked_names": summary["blocked_names"],
        "fail_names": [r["name"] for r in summary["fail_results"]],
        "warn_names": [r["name"] for r in summary["warn_results"]],
        "results": summary["result_dicts"],
    }


def observe(bucket: str, run_date: "str | None") -> dict:
    """Run the pass. Never raises: every failure becomes verdict ERROR."""
    started = time.time()
    record: dict = {"mode": MODE, "profile": PROFILE}
    previous = signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(BUDGET_SECONDS)
    try:
        import sf_preflight as sp

        caps = detect_capabilities()
        record["capabilities"] = sorted(caps)
        _, results = sp.run_preflight(
            bucket=bucket, capabilities=caps, run_date=run_date,
            checks=_spot_checks(),
        )
        record.update(classify(results))
    except BaseException as exc:  # noqa: BLE001 - observe mode fails OPEN, recorded
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        record.update({
            "verdict": "ERROR",
            "error": f"{type(exc).__name__}: {exc}",
            "timed_out": isinstance(exc, _BudgetExceeded),
        })
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    record["elapsed_seconds"] = round(time.time() - started, 2)
    return record


def _write_artifact(bucket: str, key: str, record: dict) -> "str | None":
    try:
        import boto3

        boto3.client("s3", region_name=REGION).put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(record, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        return None
    except Exception as exc:  # noqa: BLE001 - recorded on the verdict line
        log.error("on-spot preflight artifact write failed (s3://%s/%s): %s", bucket, key, exc)
        return f"{type(exc).__name__}: {exc}"


def _emit_metrics(record: dict) -> "str | None":
    """Same metric names as the Lambda's _emit_preflight_metrics, under this
    pass's own Profile dimension. An ERROR verdict emits ChecksRan=0, which is
    exactly what a reader should see for a pass that observed nothing."""
    try:
        import boto3

        dims = [{"Name": "Profile", "Value": PROFILE}]
        boto3.client("cloudwatch", region_name=REGION).put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {"MetricName": name, "Dimensions": dims, "Value": float(value), "Unit": "Count"}
                for name, value in (
                    ("ChecksRan", record.get("ran_count", 0)),
                    ("ChecksSkipped", record.get("skip_count", 0)),
                    ("ChecksWarned", record.get("warn_count", 0)),
                    ("ChecksFailed", record.get("fail_count", 0)),
                    ("RequiredChecksSkipped", record.get("required_skip_count", 0)),
                    ("GroupRequiredChecksSkipped", record.get("group_required_skip_count", 0)),
                )
            ],
        )
        return None
    except Exception as exc:  # noqa: BLE001 - recorded on the verdict line
        log.error("on-spot preflight metric emission failed: %s", exc)
        return f"{type(exc).__name__}: {exc}"


def verdict_line(record: dict, artifact_uri: str) -> str:
    """The compact stdout summary the state machine stores as a string.
    Bounded well under SSM's 24,000-character StandardOutputContent cap."""
    keys = (
        "mode", "verdict", "ran_count", "fail_count", "required_skip_count",
        "group_required_skip_count", "group_required_skip_names", "fail_names",
        "blocked_names",
        "expected_skip_names", "error", "timed_out", "artifact_error",
        "metric_error", "elapsed_seconds",
    )
    line = {k: record[k] for k in keys if k in record}
    line["artifact"] = artifact_uri
    return json.dumps(line, separators=(",", ":"), default=str)[:4000]


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(description="Weekly SF on-spot preflight pass (observe mode).")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--execution-name", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING, stream=sys.stderr,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    record = observe(args.bucket, args.run_date)
    record.update({
        "run_date": args.run_date,
        "execution_name": args.execution_name,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    partition = args.run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{ARTIFACT_PREFIX}/{partition}/{args.execution_name or 'adhoc'}.json"
    artifact_error = _write_artifact(args.bucket, key, record)
    if artifact_error:
        record["artifact_error"] = artifact_error
    metric_error = _emit_metrics(record)
    if metric_error:
        record["metric_error"] = metric_error

    if record["verdict"] == "FAIL":
        # sf-pipeline-policy §7a obligation 3: a real ERROR on a real surface,
        # distinguishable from an enforcing verdict only by the exit code's
        # consequence (the state machine records it and proceeds).
        log.error(
            "OBSERVE-MODE preflight FAIL (would halt once promoted): %s",
            ", ".join(record.get("fail_names", [])),
        )
    elif record["verdict"] in ("ERROR", "BLIND_SPOT"):
        log.error("on-spot preflight %s: %s", record["verdict"],
                  record.get("error") or record.get("group_required_skip_names")
                  or record.get("blocked_names"))

    print(verdict_line(record, f"s3://{args.bucket}/{key}"))
    return OBSERVED_FAIL_EXIT_CODE if record["verdict"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
