"""alpha-engine-data-spot-dispatcher — launch the data-heavy weekday/EOD enrich
workloads on a dedicated ephemeral EC2 spot box (config#1767, Phase 2).

WHY SPOT, NOT ON THE ALWAYS-ON TRADING BOX (config#1767): today the weekday
pre-open pipeline (`step_function_daily.json`: MorningEnrich + MorningArcticAppend)
and the EOD post-close pipeline (`step_function_eod.json`: PostMarketData +
PostMarketArcticAppend) SSM-invoke onto ae-trading (i-018eb3307a21329bf, t3.small
8GB). That ~30-50 min of daily_closes fetch + ArcticDB append fills /tmp, competes
with IB Gateway + the executor daemon, and on 2026-07-05 pushed /tmp to 100% and
failed a manual risk_model run. This Lambda moves the data phase onto a fresh spot
box with a large ephemeral disk; ae-trading stays data-free and reserved for IB
Gateway + the daemon.

Mechanism (mirrors the fleet gold-standard `scheduled-groom-dispatcher/index.py`,
which itself mirrors the Saturday `spot_data_weekly.sh` — SAME two fleet
chokepoints, no lib change):
  1. `nousergon_lib.ec2_spot.launch()` rotates instance_type x subnet on capacity
     error; on SpotCapacityExhausted across all pools we relaunch ON-DEMAND
     (spot=False) so a capacity dip never starves the pre-open enrich the
     predictor reads next.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` (AWS-RunShellScript) that clones
     alpha-engine-data, builds a venv, and runs the SAME `weekly_collector.py`
     entrypoint the on-trading states ran (e.g. `--morning-enrich`). The box
     self-terminates (InstanceInitiatedShutdownBehavior=terminate + a watchdog).
     The Lambda returns immediately with the command_id — the Step Function polls
     ssm:GetCommandInvocation to a terminal status, exactly like the groom SF.

The box does its Arctic write / S3 read+write via its OWN instance profile —
today still `alpha-engine-executor-profile` -> `alpha-engine-executor-role`,
the SAME profile `spot_data_weekly.sh` uses for the Saturday data spot, so the
ArcticDB/S3 credentials already exist on the box and this Lambda passes NONE
of them. `DATA_SPOT_IAM_PROFILE` is env-overridable to the narrower
`nousergon-data-collection-box-profile` (alpha-engine-config-I10756) once that
role is live — see the constant's definition below for the cutover sequencing.

FAILURE ISOLATION (config#1767 deliverable #4, LOAD-BEARING): this Lambda is only
the launcher. The fail-OPEN decision lives in the Step Function: a data-spot
launch/run failure must NOT block daemon start (weekday) or reconcile+instance-stop
(EOD). The SF routes a spot failure to the continue path, mirroring the Saturday
`ResearchPredictorParallel` branch-error pattern (record failure as data, do not
hard-fail the pipeline). This Lambda still RAISES on launch failure so the SF's
Catch can convert it to that fail-open branch (a raise is observable; a silent
launched:false is not — same posture as the groom dispatcher).

Managed OUTSIDE CloudFormation (same as scheduled-groom-dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect until
the new code + IAM are deployed AND the daily/EOD SFs are re-deployed with the
new states.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid

import boto3
from krepis import alerts
from krepis.spot_bootstrap import RunLog, SpotBootstrapSpec, render_bootstrap
from nousergon_lib import ec2_spot
from nousergon_lib.ec2_spot import SpotCapacityExhausted, SpotQuotaExceededError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: DATA_SPOT_DISPATCH_ENABLED=false disables the launch without
# deleting the SF states — the SF's CheckDataSpotLaunched -> *Skipped branch
# (same shape as the groom SF's CheckLaunched -> GroomSkipped) handles it as an
# intentional no-op, NOT a failure. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("DATA_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh) ──
# Widened 3 -> 9 types across 3 families (alpha-engine-config-I11340 item 5).
#
# The three originals (c5.large, c5a.large, m5.large) are a narrow capacity
# surface: `launch_with_fallback` rotates instance_type x subnet, so the
# number of distinct pools it can fall through is what decides whether a
# capacity dip escalates to on-demand. Measured against September CE data:
# `BoxUsage:c5.large` (on-demand) ran CONCURRENTLY with `SpotUsage:c5.large`
# for 946 combined hours in August (> the 744 hours in the month) — proof
# spot capacity for this one type was unavailable often enough that the
# dispatcher fell back to on-demand for a large share of the month.
#
# This mirrors the already-shipped, already-measured diversification pattern
# `crucible-v2.yaml`'s `InstanceTypeCandidates` parameter documents
# (alpha-engine-config-I10344/I10734) and `weekly-freshness-spot-dispatcher`'s
# own widening (alpha-engine-config-I7133, 4 -> 10 types): more (type, subnet)
# pools to rotate through before falling back to on-demand.
#
# Every addition is x86_64 (the AMI below is x86_64 AL2023), 2 vCPU, and the
# same 4 GiB class as c5.large, drawn from the set this repo's own
# `spot_data_weekly.sh`/`_spot_common.sh` ALLOWED_INSTANCE_TYPES already
# treats as offered in this account's subnets — no new, unevidenced type is
# introduced. Two silicon vendors (Intel/no-suffix, AMD `a`) x three
# generations (5, 6) across three families (c: compute, m: general, r: memory)
# so a capacity dip in any one pool leaves eight siblings to rotate through
# before on-demand.
#
# NEEDS AN IAM CHANGE: this role's `ec2:InstanceType` Condition previously
# enumerated exactly the 3 original types (config#11227), so it must be
# widened to the same 9 IN THE SAME CHANGE SET (see iam-policy.json) — an
# operator-gated `deploy.sh --apply-iam`, per this file's own deploy.sh
# comment ("widening the pool means editing BOTH the default and this
# policy, then --apply-iam").
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "DATA_SPOT_INSTANCE_TYPES",
        "c5.large,c5a.large,c6i.large,m5.large,m5a.large,m6i.large,"
        "r5.large,r5a.large,r6i.large",
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "DATA_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("DATA_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("DATA_SPOT_KEY_NAME", "alpha-engine-key")
# NO IB port exposure (config#1767 deliverable #3): reuse the standard fleet SG,
# which does not open the IB Gateway port. The data spot only needs egress + SSM.
SECURITY_GROUP = os.environ.get("DATA_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# The box's Arctic-write + S3 read/write come from this profile — historically
# the SAME one spot_data_weekly.sh grants the Saturday data spot (executor
# role, component 3/crucible-trading). alpha-engine-config-I10756
# (architecture.d/146, one workload identity per component) splits data
# collection (component 1) onto its own `nousergon-data-collection-box-role`
# / `nousergon-data-collection-box-profile` (nous-ergon-ops
# `infrastructure/iam/nousergon-data-collection-box-role/`) so a grant widened
# for collection can no longer widen the trader, and a CloudTrail S3 write is
# attributable to one component. This Lambda's own iam-policy.json already
# holds the PassRole grant for the new role (`PassDataCollectionBoxRoleToEc2`)
# so DATA_SPOT_IAM_PROFILE can be overridden to
# "nousergon-data-collection-box-profile" today (e.g. per-workload via the SF
# input, or a per-environment Lambda env var) — the DEFAULT stays the
# executor profile until nous-ergon-ops bootstraps the new role live
# (create-role/create-instance-profile are operator-gated,
# role-provisioning-notes.md). Flip the default in a follow-up once
# `iam-drift-check.yml` shows the new role off its `NEVER BOOTSTRAPPED` list;
# flipping it earlier fails every collection launch with an IAM error the
# launch path cannot recover from.
IAM_PROFILE = os.environ.get("DATA_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# Large ephemeral disk so daily_closes fetch + ArcticDB append never hit the
# /tmp-100% failure mode that motivated this move (config#1767 gotcha).
VOLUME_SIZE_GB = int(os.environ.get("DATA_SPOT_VOLUME_SIZE_GB", "60"))

# Cost-allocation tag (alpha-engine-config-I10788). Per the binding plan
# (`data_collection_plan_260914.md` §2 objective 8, `data_gate.clauses`'
# `data.cost.monthly` literal text) the key/value is `component=data-collection`.
# `alpha-engine-config-I10905` (gate:decision, OPEN/unruled as of 2026-09-21)
# asks whether `component` or `system` should be the fleet-wide key — this
# constant follows the plan's literal text as the stated ASSUMPTION per this
# issue's dispatch instructions, not a resolution of I10905. `component` was
# activated as a cost-allocation tag key in Billing 2026-09-21 (previously
# INACTIVE — `aws ce list-cost-allocation-tags` showed only `system` Active),
# so tagged spend under this key starts accruing in the cost-and-usage export
# from this date forward, never retroactively.
#
# Applied on EVERY launch (spot AND on-demand, no conditional), unconditionally
# merged into `extra_tags` in `_launch_instance` so it rides the SAME
# `RunInstances` `TagSpecifications` entry atomically — same reasoning
# `krepis.ec2_spot`'s own extra_tags docstring gives for per-run identity tags:
# a separate post-launch `create_tags` call leaves a window where the box is
# observably untagged.
#
# COVERS THE INSTANCE ONLY. `krepis.ec2_spot._build_launch_kwargs` puts
# `TagSpecifications` under `ResourceType: "instance"` only — the EBS volume
# created by the same `BlockDeviceMappings` entry carries no TagSpecifications
# at all, fleet-wide, for every caller of this shared launch helper. Fixing
# that is a `krepis` library change, out of this file's (and this repo's)
# ownership; filed as alpha-engine-config-I10788's follow-up (see PR body).
COST_TAG_KEY = "component"
COST_TAG_VALUE = "data-collection"

DATA_REPO = os.environ.get("DATA_SPOT_REPO", "nousergon/nousergon-data")
DATA_BRANCH = os.environ.get("DATA_SPOT_BRANCH", "main")
# Private config package weekly_collector.py resolves via resolve_experiment_config
# (experiments/reference/data/config.yaml). The spot box mirrors groom-dispatcher:
# read the fleet PAT from SSM (executor role grants alpha-engine/* GetParameter)
# and shallow-clone alpha-engine-config. spot_data_weekly.sh stages config via S3
# from ae-dashboard instead — no dispatcher host with a local clone exists here.
CONFIG_REPO = os.environ.get("DATA_SPOT_CONFIG_REPO", "nousergon/alpha-engine-config")
CONFIG_BRANCH = os.environ.get("DATA_SPOT_CONFIG_BRANCH", "main")
GH_PAT_SSM = os.environ.get(
    "DATA_SPOT_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)
# Hard ceiling for the on-box SSM command (matches the bootstrap watchdog). Sized
# above the observed ~50 min enrich + ~38 min append tail with headroom.
MAX_RUNTIME_SECONDS = int(os.environ.get("DATA_SPOT_MAX_RUNTIME_SECONDS", "7200"))

# Per-workload runtime caps that must exceed the shared default. shadow-weekday
# chains FOUR boundary invocations that each ran under their own 7200 s box
# before this workload existed (~50 min enrich + ~38 min append + EOD data +
# ~38 min EOD append), plus the one-time ArcticDB seed from live, which is
# bounded at ~30 min in-region with its own 45 min budget (shadow/root.py
# "Budget", alpha-engine-config-I10866), plus parity. 5 h covers that sum
# (~3.9 h) with headroom. It drives BOTH the SSM executionTimeout and the
# box's hard-stop timer, so neither of them can cut the chain off first.
# The two standalone comparators (alpha-engine-config-I10920) are deliberately
# ABSENT: both are read-and-compare passes over one trading day, and the 7200 s
# shared default already covers the parity leg measured inside the 2026-09-15
# shadow-weekday chain. MEASURED on the first dispatch of each, 2026-09-17,
# trading day 2026-09-14 (alpha-engine-config-I10920): `shadow-parity`
# published its 23-row report ~4.5 min after launch, `arctic-parity` ran
# 18:24:02Z -> 18:26:02Z (120 s of comparator), both including box boot and
# the venv build. Neither is within an order of magnitude of 7200 s.
# A cap is added here only against a MEASURED overrun, per
# that issue's deliverable 3 ("measure the first run rather than guessing"); an
# entry invented ahead of evidence is a number nothing checks.
#: `shadow-morning` gets 3600, not the 18000 its two siblings carry: it runs
#: TWO legs plus the comparator, and the pair took 6 minutes on 2026-09-21
#: before failing. A cap ten times the observed run and a fifth of the chained
#: workloads' — the point of a per-workload cap is that it is per workload
#: (alpha-engine-config-I11352).
_WORKLOAD_MAX_RUNTIME_SECONDS: dict[str, int] = {
    "shadow-weekday": 18000,
    "shadow-sameday": 18000,
    "shadow-morning": 3600,
}


# Parity time windows, per workload (alpha-engine-config-I10892 deliverable 3).
# DECLARED, and deliberately EMPTY for shadow-weekday: no dispatch is refused for
# overlapping v1's D+1 postclose (~20:00Z). The 09-15 run's 103 false mismatches
# came from grading day D's shadow against live keys v1 had already rewritten
# for D+1. That race is closed at the comparison, not the clock:
#   * `alpha-engine-research` is versioned with 30-day noncurrent retention
#     (nous-ergon-ops-PR1308), and every v1 run manifest now records each
#     output's etag + VersionId (nousergon-lib run_manifest.record_output);
#   * `shadow parity` grades each key against the object v1's manifest recorded
#     for the trading day — the current object when its ETag still matches, the
#     recorded VersionId when not, and the named verdict `live_superseded`
#     (never met, never mismatch) when neither can be read.
# A clock window would add a refusal that cannot improve a correct report and
# would still be wrong for a run finishing after 20:00Z by a minute; snapshotting
# VersionIds at dispatch would duplicate what the manifests already record, and
# be less exact (dispatch precedes the D-close run for an early dispatch). The
# remaining bound is retention: a parity run more than 30 days after its trading
# day reads `live_superseded`, by name. Adding a workload here means adding a
# refusal in `_resolve_workload` and its test; an empty entry is the ruling.
# The two standalone comparators (alpha-engine-config-I10920) inherit the same
# ruling for the same reason: each grades a key against the object v1's run
# manifest recorded for the trading day, so a clock window would refuse runs
# that are correct and admit runs that are not.
_WORKLOAD_PARITY_WINDOW: dict[str, "tuple[str, str] | None"] = {
    "shadow-weekday": None,
    "shadow-sameday": None,
    "shadow-morning": None,
    "shadow-parity": None,
    "arctic-parity": None,
}


def _max_runtime_seconds(workload: "str | None") -> int:
    """The declared cap for ``workload``, or the shared default.

    An entry OVERRIDES the default in both directions (alpha-engine-config-
    I11352). It used to be `max(default, declared)`, which silently ignored any
    declaration below 7200 — so `shadow-morning`'s deliberate 3600, chosen
    because it runs two legs that measured 6 minutes rather than the five-hour
    chain its siblings run, would have been read as 7200 and the per-workload
    map would have been a one-way ratchet nothing said it was. A cap is a
    statement about THIS workload; a floor that overrides it makes every
    tightening inert.
    """
    declared = _WORKLOAD_MAX_RUNTIME_SECONDS.get(workload or "")
    return MAX_RUNTIME_SECONDS if declared is None else declared


# gitleaks pin for the DLP boot gate. It moves in LOCKSTEP with
# infrastructure/_spot_common.sh::install_gitleaks_dlp (same version, same
# sha256, same release asset); test_handler.py asserts that equality.
# alpha-engine-config-I10370 / I10866: krepis.session_dlp shells out to this
# binary on every LLM call (flow-doctor diagnosis) and fails CLOSED without it.
# A wheel cannot carry it, so the venv install never provides it.
GITLEAKS_VERSION = "8.30.1"
GITLEAKS_SHA256 = "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("DATA_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("DATA_SPOT_CW_LOG_GROUP", "/alpha-engine/data-spot")

# The data-phase workloads this dispatcher can run. The five scheduled ones
# map to the EXACT weekly_collector.py invocation the on-trading SF states ran
# (unchanged args = unchanged M0 data contract: same paths/schemas); the sixth
# is the on-demand declared-benchmark-proxy load (alpha-engine-config-I10704);
# the last two build the EDGAR point-in-time fundamentals (alpha-engine-config-I10733).
# Any other value is rejected.
_WORKLOADS: dict[str, str] = {
    # weekday pre-open (was step_function_daily.json MorningEnrich)
    "morning-enrich": (
        "python weekly_collector.py --morning-enrich "
        "--skip-chronic-heal --skip-arctic-append"
    ),
    # weekday pre-open (was step_function_daily.json MorningArcticAppend)
    "morning-arctic-append": "python weekly_collector.py --morning-arctic-append",
    # EOD post-close (was step_function_eod.json PostMarketData)
    "post-market-data": (
        "python weekly_collector.py --daily --skip-arctic-append"
    ),
    # EOD post-close (was step_function_eod.json PostMarketArcticAppend)
    "post-market-arctic-append": (
        "python weekly_collector.py --daily-arctic-append"
    ),
    # alpha-engine-config-I2717: standalone daily data-heal, EventBridge-triggered
    # ~09:00 UTC weekdays (alpha-engine-daily-heal rule) — was inline in preopen's
    # MorningArcticAppend (universe-gap self-heal head) + the weekday SF's own
    # on-trading ChronicGapSelfHeal state, both REMOVED from
    # step_function_daily.json entirely. Runs off the preopen critical path with
    # a much bigger heal timeout budget (see weekly_collector._run_daily_heal).
    "daily-heal": "python weekly_collector.py --daily-heal",
    # alpha-engine-config-I11002: D34 (chronic-gap-heal) had no successor at
    # all — `morning-enrich` above passes `--skip-chronic-heal` on BOTH the
    # morning and the weekly schedule, and `daily-heal` maps to D33
    # (`run_units.py:197`), a different unit and a different mode. The
    # standalone entrypoint already existed unreachable
    # (`weekly_collector.py:1197`, `args.chronic_gap_heal` ->
    # `_run_whole_mode_unit("chronic_gap_heal", ...)`, manifest key extractor at
    # `weekly_collector.py:1042`) — this is the missing dispatcher key over it.
    # Wired into `WeeklySchedule` (Sat 05:00 America/New_York), matching both
    # D34's descriptor `trigger.schedule` and its v1 cadence
    # (`ne-data-collection-weekly`, the successor its `trigger.successor`
    # already names), not `daily-heal`'s weekday cadence — chronic-gap-heal was
    # never a daily operation and D33's own weekday heal is a different unit
    # with a different scope. No `trading_day` template: the heal always
    # operates on the CURRENT chronic-gap state, the same "today" semantics
    # `weekly-phase-one` and `rag-weekly-ingestion` already use.
    "chronic-gap-heal": "python weekly_collector.py --chronic-gap-heal",
    # alpha-engine-config-I10704: the one-off, idempotent in-region load of
    # every DECLARED benchmark proxy (features.compute.
    # UNIVERSE_BENCHMARK_PROXIES) into the ArcticDB `universe` library. Not
    # scheduled and not on any pipeline's critical path — this exists so the
    # load is an `aws lambda invoke` against an EXISTING runner rather than a
    # hand-typed `ssm send-command`, which is the difference between a step
    # that is code in the repo and a step that is text someone has to
    # remember (the alpha-engine-config-I1906 class). Re-running it is a
    # no-op: every write underneath is union/no-shrink.
    "benchmark-proxy-backfill": "python -m scripts.backfill_benchmark_proxies",
    # alpha-engine-config-I10739: the weekly phase-1 collection
    # (`market_data/valuation_medians`, constituents, macro, fundamentals, the
    # universe prune) as a dispatcher workload, so the standalone
    # `nousergon-data-collection` stack can run it without the weekly SF's
    # `DataPhase1` state (`infrastructure/spot_data_phase1.sh`), which Crucible
    # v2 phase 4 disables. SAME two commands in the SAME order that script's run
    # block executes (`weekly_collector.py --phase 1`, then
    # `builders.prune_delisted_tickers --apply`), so the data contract is
    # unchanged. The subshell makes the pair one pipeline element: the tail runs
    # `{cmd} 2>&1 | tee` and reads PIPESTATUS[0], which without the parentheses
    # would report the prune alone and let a failed phase 1 pass.
    "weekly-phase-one": (
        "( python weekly_collector.py --phase 1 "
        "&& python -m builders.prune_delisted_tickers --apply )"
    ),
    # alpha-engine-config-I10733: the filing-date-indexed EDGAR fundamentals
    # dataset (`collectors/edgar_pit_fundamentals.py`). In-region because it
    # reads ArcticDB `Close` for market cap. The backfill is on-demand and
    # write-if-absent (re-running it writes only missing sessions); the daily
    # workload materializes the trailing 15 sessions the same way. Neither is
    # scheduled by this change.
    "edgar-pit-fundamentals-backfill": (
        "python -m collectors.edgar_pit_fundamentals backfill --start 2022-01-03"
    ),
    "edgar-pit-fundamentals-daily": (
        "python -m collectors.edgar_pit_fundamentals incremental --lookback-sessions 15"
    ),
    # data-collector plan P-05 (alpha-engine-config-I10748): the in-region
    # ArcticDB probe (`collectors/arctic_probe.py`). Wired as the FINAL
    # workload of both the eod and morning schedule inputs
    # (infrastructure/cloudformation/nousergon-data-collection.yaml) so the
    # data_gate red board (nousergon-data architecture.d/146 §4.1) has fresh
    # ArcticDB evidence without ever opening ArcticDB itself, which is
    # unreachable from the laptop (alpha-engine-config-I9771).
    "arctic-probe": "python -m collectors.arctic_probe",
    # alpha-engine-config-I10753: the five weekly units with no standalone
    # successor (D15, D16, D40, D41, D46). D40/D41 (analyst snapshotter /
    # analyst_revisions) are RETIRED by Brian ruling 2026-09-14 R7 — no
    # workload; see their descriptors' `retirement:` block. D15, D16 and D46
    # get real successors here.
    #
    # SAME command the v1 SF's DataPhase2 state ran
    # (`infrastructure/spot_data_weekly.sh --phase2-only` ->
    # `weekly_collector.py --phase 2`, alpha-engine-config-I5759's move off
    # lambda:invoke): unchanged args = unchanged M0 data contract.
    "alternative-phase-two": "python weekly_collector.py --phase 2",
    # SAME script the v1 SF's RAGIngestion state ran
    # (`infrastructure/spot_rag_ingestion.sh` -> `bash
    # rag/pipelines/run_weekly_ingestion.sh`), now wrapped by
    # `rag/pipelines/run_weekly_ingestion_recorded.py`
    # (alpha-engine-config-I10862): a thin `run_units.recorded_entry("D16", ...)`
    # entrypoint that runs the SAME unchanged bash script as a subprocess and
    # writes `data_collection/runs/D16/{trading_day}/{run_id}.json` around it
    # — the ingestion steps themselves, their order, their env/venv resolution
    # are untouched. Covers D16 (its own writes) AND D46 (Form 4 insider
    # transactions, step 5 of the script) — D46 does NOT get its own
    # dispatcher key: it is a substep of this same script, not a standalone
    # entry point, so a second key would re-run the identical EDGAR fetch a
    # second time per week. D40/D41 (step 7, analyst pipeline) still execute as part of this
    # unchanged script (this dispatcher does not own
    # rag/pipelines/run_weekly_ingestion.sh — that is `nousergon-data`'s RAG
    # pipeline code, a sibling surface); their retirement is at the
    # descriptor/registry layer (not tracked, not asserted), matching the
    # standing "retiring a producer never deletes its archive" preference.
    #
    # The RAG-specific secrets (VOYAGE_API_KEY, FINNHUB_API_KEY,
    # EDGAR_IDENTITY, RAG_DATABASE_URL) are NOT part of this dispatcher's
    # generic bootstrap — the phase-1/phase-2 boxes never needed them
    # (I10753 gotcha). Fetched here with the EXACT SSM read-loop
    # `infrastructure/spot_rag_ingestion.sh` already uses, verbatim, so this
    # workload's on-box environment mirrors what run_weekly_ingestion.sh has
    # always run under rather than a parallel invention.
    "rag-weekly-ingestion": (
        "( for name in VOYAGE_API_KEY FINNHUB_API_KEY EDGAR_IDENTITY RAG_DATABASE_URL; do "
        "val=$(aws ssm get-parameter --name /alpha-engine/$name --with-decryption "
        "--query Parameter.Value --output text --region us-east-1 2>/dev/null || echo ''); "
        "if [ -z \"$val\" ]; then echo \"ERROR: could not fetch /alpha-engine/$name from SSM\" >&2; exit 1; fi; "
        "export $name=\"$val\"; unset val; done; "
        "python -m rag.pipelines.run_weekly_ingestion_recorded )"
    ),
    # alpha-engine-config-I10778, plan P-11: the pre-cutover shadow run. Chains
    # the FOUR weekday-boundary invocations that a live v1 producer still owns
    # today — morning-enrich (was step_function_daily.json MorningEnrich),
    # morning-arctic-append (MorningArcticAppend), post-market-data (was
    # step_function_eod.json PostMarketData, expressed here as `--daily
    # --skip-arctic-append`) and post-market-arctic-append (`--daily-arctic-
    # append`, PostMarketArcticAppend) — each run under `python -m shadow run`
    # (PR1720), which activates the shadow output root BEFORE weekly_collector
    # is imported so every S3 write lands under `staging/shadow/{trading_day}/`
    # and every ArcticDB library carries the `shadow_{YYYYMMDD}_` prefix
    # (`shadow/root.py::INVARIANT`) — then `python -m shadow parity` publishes
    # the diff against that same day's live v1 output to
    # `s3://alpha-engine-research/data_collection`. SAME weekly_collector.py
    # entrypoints and flags as the four scheduled workloads above (unchanged
    # args = unchanged M0 data contract); the only difference is `--date
    # {trading_day}` on every leg, pinning all four runs (and the parity
    # comparison) to the SAME historical trading day rather than "today", and
    # the shadow-run wrapper redirecting every write.
    #
    # The subshell + `&&` chain (same pipeline-element pattern as
    # weekly-phase-one above) makes the five legs ONE unit: PIPESTATUS[0] is
    # the whole chain's exit code, so a failure in any leg — including
    # `shadow parity` returning 1 for "ran but NOT MET" — fails the run rather
    # than silently continuing to the next leg or reporting success. Each
    # `shadow run` leg still runs weekly_collector.py's own manifest-writing
    # code path unchanged (data_run_manifest.v1 via run_units), so this
    # workload writes its own run manifests exactly the way every sibling
    # weekly_collector-backed workload does — the shadow interceptor redirects
    # WHERE they land, never WHETHER they are written.
    #
    # `{trading_day}` is a TEMPLATE placeholder, not embedded user input: it is
    # substituted by `_resolve_workload` below from `event["trading_day"]`
    # after validating it is a real ISO calendar date, the same
    # allowlist-before-substitution posture `_WORKLOAD_RE` already applies to
    # the workload key itself. On-demand only (data collector plan P-11 §6.2
    # step 4) — deliberately not wired into any EventBridge/Scheduler cadence
    # or CFN schedule input, so it needs no `governance/observability.d/` /
    # `authority.d/` row: that requirement (`nousergon-data` AGENTS.md
    # "Infrastructure is one workflow per deployed unit") binds a *scheduled*
    # workflow, and this is a manual validation command inside the ALREADY
    # registered data-spot-dispatcher Lambda, invoked at most a handful of
    # times during the pre-cutover validation window. It is also not a
    # `registry.d/units/` entry: those units feed the `verify_units` /
    # completion-check contract for LIVE producers, and every byte this
    # workload writes is, by construction, under the shadow prefix and never a
    # live key — there is no live completeness claim for this workload to make.
    # LEGS RUN INDEPENDENTLY, COMPARATOR ALWAYS (alpha-engine-config-I11200).
    #
    # This was `leg1 && leg2 && leg3 && leg4 && parity` until 2026-09-20.
    # `weekly_collector` exits 1 on anything less than fully `ok` -- its
    # declared fail-loud contract, correct for production where a degraded
    # feature store must halt the pipeline. Chained with `&&`, that same
    # contract meant `shadow parity` ran only if EVERY collector on EVERY leg
    # was perfectly ok, and it never once was:
    #
    #   09-14 dispatch  died in leg 2 (UniverseFreshnessViolation)   no report
    #   09-18 dispatch  died end of leg 3 (features=degraded)        no report
    #
    # Two unrelated causes, zero reports -- and the 09-18 run had already
    # written 1,977 objects the comparator could read. Dispatching
    # `shadow-parity` by hand over that prefix produced a complete 959-key
    # report in minutes: the proof that only the `&&` was in the way.
    #
    # A production halt-the-pipeline contract was being used as a sequencing
    # operator for a diagnostic tool. Each leg now records its exit code to a
    # legs file and the comparator ALWAYS runs, so a partial run NAMES its
    # gaps instead of looking complete.
    #
    # The legs file is `name<TAB>exit_code` per line, not JSON: this string is
    # `.format()`ed, and a literal `{` would have to be doubled through two
    # layers of quoting for no gain. `shadow parity --legs-file` reads both.
    #
    # `set +e` is scoped to this subshell; the tail still exits non-zero when a
    # leg failed, so the dispatcher's own success/failure reporting is
    # unchanged. What changes is that the comparator runs first.
    "shadow-weekday": (
        "( set +e; LEGS=/tmp/shadow_legs_{trading_day}.tsv; : > $LEGS; RC_ALL=0; "
        "python -m shadow run --trading-day {trading_day} --module weekly_collector -- "
        "--morning-enrich --skip-chronic-heal --skip-arctic-append --date {trading_day}; "
        "RC=$?; printf 'morning-enrich\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow run --trading-day {trading_day} --module weekly_collector -- "
        "--morning-arctic-append --date {trading_day}; "
        "RC=$?; printf 'morning-arctic-append\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow run --trading-day {trading_day} --module weekly_collector -- "
        "--daily --skip-arctic-append --date {trading_day}; "
        "RC=$?; printf 'post-market-data\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow run --trading-day {trading_day} --module weekly_collector -- "
        "--daily-arctic-append --date {trading_day}; "
        "RC=$?; printf 'post-market-arctic-append\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow parity --trading-day {trading_day} --legs-file $LEGS "
        "--store s3://alpha-engine-research/data_collection; PARITY_RC=$?; "
        "[ $RC_ALL -ne 0 ] && exit $RC_ALL; exit $PARITY_RC )"
    ),
    # SAME-DAY shadow run (Brian ruling 2026-09-21, alpha-engine-config-I11203).
    #
    # `shadow-weekday` above REPLAYS a completed day, and the 2026-09-18 report
    # measured what that costs: of 957 keys, 18 mismatched and every one of them
    # was the world moving between the day and the replay, not the collector
    # disagreeing with v1. A replay dispatched two days later re-fetched live
    # vendors and re-read inputs that had since been overwritten, so the
    # comparison conflated "the implementation differs" with "the world moved".
    # Comparing two implementations means holding the inputs fixed; running them
    # on the SAME day is how you do that when concurrency is available, and here
    # it is.
    #
    # THE TRADING DAY IS RESOLVED ON THE BOX, not templated in by the Lambda.
    # That is the whole difference from `shadow-weekday`, and it is why this key
    # is deliberately NOT in `_WORKLOADS_REQUIRING_TRADING_DAY`: an EventBridge
    # Scheduler rule carries a STATIC input and cannot compute today's date, and
    # `_resolve_workload` refuses — correctly — to default a replay's day to
    # "today". So the box asks `dates.default_run_date()`, the same function
    # every scheduled collector uses to key its own run.
    #
    # NOT wired into the data-collection state machine, and this is not an
    # oversight: that SF's contract is `verify_units` over LIVE run manifests
    # under `data_collection/runs/{unit}/{day}/`, and every byte a shadow run
    # writes is under `staging/shadow/{day}/` by construction. It has no live
    # completeness claim to make, which is the same reasoning `shadow-weekday`
    # records for staying off the schedule. A standalone Scheduler rule pointed
    # at this dispatcher is the honest shape.
    #
    # THE GUARD IS `default_run_date() == today in ET`, and it does two jobs.
    # `dates.default_run_date()` returns the last session whose 4:00 PM ET close
    # HAS OCCURRED, so comparing it to today's ET date refuses:
    #
    #   * a non-trading day — the rule fires Mon-Fri and NYSE holidays fall
    #     inside that. MEASURED: Saturday -> TD=Fri != Sat, exits 0;
    #     Thanksgiving -> TD=Wed != Thu, exits 0. Running anyway would replay
    #     the previous session under today's date and publish a parity report
    #     keyed to a day on which nothing was collected.
    #   * a PRE-CLOSE fire — 12:00 ET Monday gives TD=Friday != Monday, exits 0.
    #     That case was not designed for and is the more valuable half: a
    #     same-day run started before the close would grade a complete v2 run
    #     against a v1 day that has not finished, and every key v1 had yet to
    #     write would read `live_missing`.
    #
    # MEASURED 2026-09-21 against `dates.default_run_date` for all four cases;
    # `test_shadow_sameday_guard_refuses_non_sessions_and_pre_close` pins them.
    #
    # IT RUNS THE TWO POST-MARKET LEGS ONLY (alpha-engine-config-I11352). It
    # used to run all four, including v1's two MORNING legs — twelve hours
    # before v1 runs them, against a Polygon grouped-daily bar that is a D+1
    # fact. Both failed on the first scheduled run. The morning legs moved to
    # the `shadow-morning` key below, on v1's own cadence; this one's parity
    # call now declares `--legs-group sameday` so a report carrying only this
    # group renders the morning group as unknown rather than as silence.
    "shadow-sameday": (
        "( set -e; "
        "TD=$(python -c 'from dates import default_run_date; print(default_run_date())'); "
        "TODAY=$(TZ=America/New_York date +%F); "
        'if [ "$TD" != "$TODAY" ]; then '
        'echo "shadow-sameday: $TODAY is not an NYSE trading day (latest is $TD) — nothing to run"; '
        "exit 0; fi; "
        "set +e; LEGS=/tmp/shadow_legs_$TD.tsv; : > $LEGS; RC_ALL=0; "
        "python -m shadow run --trading-day $TD --module weekly_collector -- "
        "--daily --skip-arctic-append --date $TD; "
        "RC=$?; printf 'post-market-data\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow run --trading-day $TD --module weekly_collector -- "
        "--daily-arctic-append --date $TD; "
        "RC=$?; printf 'post-market-arctic-append\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow parity --trading-day $TD --legs-file $LEGS --legs-group sameday "
        "--store s3://alpha-engine-research/data_collection --dispatch-gate; PARITY_RC=$?; "
        "[ $RC_ALL -ne 0 ] && exit $RC_ALL; exit $PARITY_RC )"
    ),
    # D+1 MORNING shadow run (alpha-engine-config-I11352). The other half of
    # `shadow-sameday` above, split out because v1 runs these two legs TWELVE
    # HOURS LATER than the same-day dispatch was running them.
    #
    # WHAT THE SPLIT FIXES, measured on the first scheduled `shadow-sameday`
    # (2026-09-21, launched 22:30:34Z): `morning-enrich` exited 1 (D17,
    # `morning_daily_closes` status='error') and `morning-arctic-append` exited
    # 1 a twentieth of a second later (D18, `NoSuchKey` — it reads the
    # daily-closes parquet the failed leg never wrote). Neither is a collector
    # defect. `collectors/daily_closes.py::collect` in window mode is
    # TARGET-DRIVEN: the aggregate is `error` when `per_date[target_date]` is
    # not ok, and the phase manifest shows 2026-09-08…09-18 all ok at 928
    # tickers while the run failed on target 2026-09-21 itself. Polygon's
    # grouped-daily bar for session D is not final until the next morning —
    # which is exactly why v1 schedules these legs at `cron(30 7 ? * MON-FRI *)`
    # (`nousergon-data-collection.yaml` MorningSchedule) for the PREVIOUS
    # session, and why running them at 18:30 ET on D graded v2 against a bar
    # the vendor had not published.
    #
    # 07:45 ET, fifteen minutes after v1's own 07:30 fire, so v1's copy of the
    # keys exists to compare against rather than reading `live_missing` on
    # every one of them.
    #
    # THE GUARD IS TWO QUESTIONS, and conflating them is the bug this key would
    # otherwise ship with:
    #
    #   * IS TODAY A SESSION? A weekday NYSE holiday fires this Mon-Fri rule and
    #     v1's MorningSchedule (`require_trading_day: true`) does NOT run, so
    #     there would be no v1 copy to compare against. `default_run_date()`
    #     alone cannot answer this at 07:45 — on Thanksgiving it returns
    #     Wednesday, which is a perfectly valid previous session, so the
    #     `!=` comparison `shadow-sameday` uses passes and the run proceeds
    #     wrongly. Asked directly, of the NYSE calendar, for TODAY.
    #   * IS THIS ACTUALLY PRE-OPEN? `default_run_date() == today` means the
    #     16:00 ET close has already occurred, i.e. this fired late enough to be
    #     an evening run. The morning legs must target the PREVIOUS session; a
    #     late fire would target today and duplicate `shadow-sameday`.
    #
    # `nousergon_lib.trading_calendar` answers the first, NOT weekday
    # arithmetic — holidays have bitten this repo before (see
    # `_previous_business_days` in `collectors/daily_closes.py`). The TD itself
    # comes from `dates.default_run_date()`, the same chokepoint every
    # scheduled collector keys its run by, so the shadow and v1 agree on which
    # session this is by construction rather than by two calendars agreeing.
    #
    # `--skip-chronic-heal` stays on the enrich leg: a shadow must never heal
    # live data.
    #
    # The comparator runs LAST and rewrites `parity/{TD}.json` with
    # `--legs-group morning`, so the D report carries both groups' legs rather
    # than the morning group's keys being silently absent. Same
    # legs-independent shape as the two workloads above
    # (alpha-engine-config-I11200): `set +e`, one legs file, the comparator
    # ALWAYS runs.
    "shadow-morning": (
        "( set -e; "
        "TD=$(python -c 'from dates import default_run_date; print(default_run_date())'); "
        "TODAY=$(TZ=America/New_York date +%F); "
        "IS_SESSION=$(python -c \"import datetime, zoneinfo; "
        "from nousergon_lib.trading_calendar import is_trading_day; "
        "print(int(is_trading_day(datetime.datetime.now("
        "zoneinfo.ZoneInfo('America/New_York')).date())))\"); "
        'if [ "$IS_SESSION" != "1" ]; then '
        'echo "shadow-morning: $TODAY is not an NYSE session — v1\'s 07:30 morning run '
        'does not fire either, so there is nothing to compare against"; '
        "exit 0; fi; "
        'if [ "$TD" = "$TODAY" ]; then '
        'echo "shadow-morning: default_run_date() is $TD = today, so the 16:00 ET close '
        'has already occurred — this is not a pre-open fire; refusing"; '
        "exit 0; fi; "
        "set +e; LEGS=/tmp/shadow_morning_legs_$TD.tsv; : > $LEGS; RC_ALL=0; "
        "python -m shadow run --trading-day $TD --module weekly_collector -- "
        "--morning-enrich --skip-chronic-heal --skip-arctic-append --date $TD; "
        "RC=$?; printf 'morning-enrich\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow run --trading-day $TD --module weekly_collector -- "
        "--morning-arctic-append --date $TD; "
        "RC=$?; printf 'morning-arctic-append\\t%s\\n' $RC >> $LEGS; [ $RC -ne 0 ] && RC_ALL=$RC; "
        "python -m shadow parity --trading-day $TD --legs-file $LEGS --legs-group morning "
        "--store s3://alpha-engine-research/data_collection --dispatch-gate; PARITY_RC=$?; "
        "[ $RC_ALL -ne 0 ] && exit $RC_ALL; exit $PARITY_RC )"
    ),
    # alpha-engine-config-I10920: the two parity COMPARATORS, each launchable on
    # its own rather than only as the tail of the five-hour `shadow-weekday`
    # chain above.
    #
    # Why they are separate keys and not `&&`-chained into one: `shadow parity`
    # exits 1 for "the comparison ran and parity is NOT MET" and 2 for "the
    # comparison itself failed" (`shadow/__main__.py`), which is the same
    # two-code contract `data_gate.__main__` declares. A chained `&&` would make
    # the (expected, informative) NOT-MET exit skip the ArcticDB leg entirely,
    # so the report would keep its four `in_region_only` rows precisely on the
    # runs where the rest of it has something to say.
    #
    # `shadow-parity` re-publishes `data_collection/parity/{trading_day}.json`
    # from the shadow prefix an EARLIER `shadow-weekday` run already wrote. It
    # exists because the comparator's own fixes ship far more often than the
    # 5-hour producer chain can be re-run: I10890 (`out_of_run_scope`), I10892
    # (`live_superseded`) and I10894 (`provenance_diffs`) all landed after the
    # only published report was generated, and without this key the sole way to
    # read a post-fix verdict was to re-run four collector legs that write
    # nothing new. Reads and compares only; the single object it writes is the
    # report itself.
    #
    # `arctic-parity` then fills that report's four `in_region_only` rows
    # (`arcticdb/universe`, `macro`, `delisted_history`, `universe_schema_meta`)
    # in place, via `shadow/arctic_parity.py::rewrite_report`, which only ever
    # NARROWS the `in_region_only` set. It must run in-region and can run
    # nowhere else: it opens `store.arctic_store._open_library` directly, and
    # ArcticDB is unreachable from the laptop (alpha-engine-config-I9771). It
    # reads the report it is about to rewrite, so it is ordered AFTER
    # `shadow-parity`, never before — run alone against a stale report it would
    # write four fresh ArcticDB verdicts onto 464 stale S3 rows and publish the
    # mixture under one `generated_at`.
    #
    # Neither is scheduled, by the same reasoning `shadow-weekday` records: a
    # manual validation command inside the already-registered dispatcher Lambda
    # is not a scheduled workflow and needs no `governance/observability.d/`
    # row. Neither writes a live key — every byte is under
    # `data_collection/parity/` or `data_collection/runs/arctic-parity/`.
    # Coverage of these keys against `shadow/` is asserted by
    # `shadow/dispatch.py` plus its test, so the next comparator cannot land
    # unreachable the way `arctic_parity` did (alpha-engine-config-I10920).
    "shadow-parity": (
        "python -m shadow parity --trading-day {trading_day} "
        "--store s3://alpha-engine-research/data_collection"
    ),
    "arctic-parity": (
        "python -m shadow arctic-parity --trading-day {trading_day} "
        "--store s3://alpha-engine-research/data_collection"
    ),
}
# Defense-in-depth: the workload key is SF-config-controlled, not raw user input,
# but the value is embedded verbatim into the SSM shell command, so pin it to a
# strict allowlist regex too (rules out shell-metacharacter injection outright).
_WORKLOAD_RE = re.compile(r"^[a-z][a-z-]{0,63}$")

# Workloads whose command is a TEMPLATE requiring a "trading_day" substitution
# from the event rather than a fixed string. Never grows into a general
# templating mechanism: adding a workload here means adding its own validated
# placeholder(s) below, never accepting free-text into the rendered command.
_WORKLOADS_REQUIRING_TRADING_DAY: frozenset[str] = frozenset(
    {"shadow-weekday", "shadow-parity", "arctic-parity"}
)
_TRADING_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _resolve_workload(event: dict) -> tuple[str, str]:
    """Pull the workload key from the SF input; unknown/malformed RAISES (a
    mis-wired SF state must fail loud, not silently run the wrong collector).

    A workload in ``_WORKLOADS_REQUIRING_TRADING_DAY`` additionally requires
    ``event["trading_day"]`` to be a real ISO calendar date — validated the
    same way every other input this Lambda embeds into a shell command is
    validated (allowlist first, never free text), and RAISES rather than
    guessing or defaulting to "today" on anything malformed.
    """
    w = str(event.get("workload") or "").strip()
    if not _WORKLOAD_RE.match(w) or w not in _WORKLOADS:
        raise ValueError(
            f"unknown data-spot workload {w!r} — expected one of {sorted(_WORKLOADS)}"
        )
    template = _WORKLOADS[w]
    if w in _WORKLOADS_REQUIRING_TRADING_DAY:
        import datetime as _dt

        trading_day = str(event.get("trading_day") or "").strip()
        if not _TRADING_DAY_RE.match(trading_day):
            raise ValueError(
                f"workload {w!r} requires event['trading_day'] as an ISO date "
                f"(YYYY-MM-DD); got {trading_day!r}"
            )
        try:
            _dt.date.fromisoformat(trading_day)
        except ValueError as exc:
            raise ValueError(
                f"workload {w!r} trading_day={trading_day!r} is not a valid calendar date"
            ) from exc
        return w, template.format(trading_day=trading_day)
    return w, template


_MARKET_TZ = "America/New_York"


def _trading_day_check(now=None) -> dict:
    """Answer "is today an NYSE trading day?" for the data-collection SFs.

    alpha-engine-config-I10739. A standalone EventBridge Scheduler cron cannot
    express the NYSE calendar, and the v1 SFs got it from the predictor
    Lambda's MarketHoursGate, which phase 4 retires. This dispatcher already
    ships krepis, whose ``trading_calendar`` is stdlib-only, so the gate lives
    beside the workloads it gates instead of in a new function.

    The date is today in America/New_York, passed explicitly: ``is_trading_day()``
    with no argument uses the process's local date, which is UTC in Lambda and
    is already tomorrow for an evening ET run. Out-of-coverage dates RAISE
    (krepis' own contract) and the SF's Catch pages — never a guessed answer.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from krepis.trading_calendar import is_trading_day

    today = (now or datetime.now(ZoneInfo(_MARKET_TZ))).date()
    return {
        "trading_day": {
            "date": today.isoformat(),
            "is_trading_day": bool(is_trading_day(today)),
        }
    }


# ── completion check (alpha-engine-config-I10787, data collector plan P-20) ───
#
# WHY THIS LIVES HERE AND NOT IN THE ASL. The completion claim is now the run
# manifest (`data_run_manifest.v1`) of every unit the machine runs, graded on
# three properties: the manifest EXISTS for this execution, its `status` is
# `ok`, and every key the unit's descriptor says it publishes appears in
# `outputs[]` at or above its declared `rows_out` floor.
#
# Pure ASL could do the first two (`s3:listObjectsV2` + `s3:getObject` +
# `States.StringToJson` + a Choice). It cannot do the third: `outputs` is an
# ARRAY OF OBJECTS and ASL has no way to search an array by a field value, so
# per-key floor compliance is not expressible declaratively — the best a Map
# over `outputs` could assert is "some output exists", which is the claim we are
# replacing. A `{"action": "completion-check"}` action on THIS Lambda puts the
# logic where the descriptors it reads already live, mirrors the existing
# `trading-day-check` precedent (a pure computation that launches nothing), and
# is unit-testable. SOTA is the declarative integration; the delta is that the
# declarative form cannot express the per-key claim at all, so it would have had
# to be weakened to fit the mechanism.
#
# FAIL LOUD, NEVER FAIL OPEN. This function RAISES on anything it cannot
# measure (an undeclared unit, an unparseable manifest, a missing `rows_out`),
# and returns `ok: false` with a machine-readable finding list on anything it
# measured and found wanting. The ASL routes BOTH to a Fail state — a dispatcher
# error through the existing Catch, a finding through four distinct named Fail
# states — so there is no path from this state to CollectionSucceeded except an
# affirmative, measured pass.

#: The bucket every collector already writes its manifests to
#: (`run_units.MANIFEST_BUCKET`). Env-overridable for a rehearsal account.
MANIFEST_BUCKET = os.environ.get("DATA_COLLECTION_MANIFEST_BUCKET", "alpha-engine-research")

#: The floor a published key's ``rows_out`` must meet when its descriptor
#: declares none. ONE, not zero: a key published with zero rows is the
#: empty-but-fresh silent degradation this whole objective exists to end, and a
#: default of zero would make the floor mechanism vacuous everywhere it was not
#: hand-calibrated. A unit for which zero is legitimate declares that, with a
#: reason, under ``completeness.rows_out_floor_na_code``. Calibrated per-unit
#: cardinality floors are P-13 (alpha-engine-config-I5935); this ships the
#: mechanism and the loud default.
DEFAULT_ROWS_OUT_FLOOR = 1

#: Failure modes in PRECEDENCE order. The ASL switches on the first one present
#: across all units so the execution's named error is the most upstream cause —
#: a missing manifest explains a missing output, never the other way round.
COMPLETION_FAILURE_MODES = (
    "manifest_missing",
    "run_not_ok",
    "output_missing",
    "rows_below_floor",
)

#: How far back to list manifests. The prefix is partitioned by trading day, so
#: `StartAfter` at (execution start - this many days) bounds a listing that
#: would otherwise grow without limit, while staying partition-agnostic: the
#: check never has to GUESS which trading day the unit filed its run under.
MANIFEST_LOOKBACK_DAYS = int(os.environ.get("DATA_COLLECTION_MANIFEST_LOOKBACK_DAYS", "3"))

#: The guard whose `not_applicable` verdict is a unit DECLARING that it
#: published nothing this run (same-date auto-skip or a dry run —
#: `weekly_collector.py::_record_phase_lineage`). It exempts key coverage and
#: nothing else. Legitimate because the auto-skip predicate re-verifies the
#: artifact's presence on S3 before returning the cache hit, so the published
#: object IS there; the unit simply did not rewrite it. Without this exemption
#: every idempotent re-drive of the weekly phase 1 would be a failure.
EMPTY_FRESH_GUARD = "empty_fresh"

#: A ``writes:`` entry that is a prose declaration rather than an S3 key
#: template — `arcticdb/universe (library)`, `research.db::score_performance`,
#: `predictor/price_cache/<macro series>`. These are COUNTED and returned under
#: `unverifiable`, never silently dropped: the swallowed failure mode would be
#: "this unit's publish claim grades nothing", and the recording surface is the
#: `unverifiable` list on every completion-check response plus the WARNING log
#: line below. Typing `writes:` entries (`kind: s3-key|arctic-library|table`) is
#: the robust fix and is a tracked follow-up, not something to infer here.
_NON_S3_WRITE = re.compile(r"(::|\s|<|>|\(|\))")
_WRITE_TOKEN = re.compile(r"\{[a-z_]+\}|\*")
_MAX_SUMMARY_CHARS = 4000

_UNITS_CACHE: dict[str, dict] | None = None


def _descriptors():
    """The repo's ONE descriptor loader, imported lazily.

    Lazy so the module-import graph the hermetic test gate derives stays free of
    it, and because a launch invocation must not pay to parse 46 YAML files.
    `data_gate/descriptors.py` and `registry.d/units/` are packaged into this
    Lambda's zip at their repo-relative paths (see deploy.sh), so the loader's
    own ``REPO_ROOT``-relative ``UNITS_DIR`` resolves to ``/var/task`` — one
    implementation of "what a unit declares", not a second parser that drifts.
    """
    from data_gate import descriptors

    return descriptors


def _unit_descriptors() -> dict[str, dict]:
    global _UNITS_CACHE
    if _UNITS_CACHE is None:
        _UNITS_CACHE = {u.unit_id: u.raw for u in _descriptors().load_units()}
    return _UNITS_CACHE


def _parse_ts(value, *, where: str):
    """RFC3339 -> aware datetime, or RAISE naming where the bad value came from.

    Both timestamps this reads are contracts: `$$.Execution.StartTime` and the
    manifest's `finished`. A value that will not parse is a contract violation,
    and guessing one would silently move the freshness baseline.
    """
    from datetime import datetime, timezone

    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{where} is empty; the completion check has no freshness baseline")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{where}={value!r} is not an RFC3339 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _key_pattern(template: str, trading_day: str):
    """One ``writes:`` template as a regex over concrete manifest output keys.

    ``{date}``/``{trading_day}`` resolve to the day the MANIFEST ITSELF declares
    it ran for — never a day this function computes, which is how a check like
    this ends up disagreeing with the producer about what "today" was. Every
    other placeholder and every ``*`` is a fan-out (per ticker, per symbol, per
    currency) and matches one path segment. A trailing ``/`` is a declared
    prefix and matches anything under it. Returns None for a prose declaration.
    """
    if _NON_S3_WRITE.search(template) or template.startswith("arcticdb/"):
        return None
    parts, pos = [], 0
    for match in _WRITE_TOKEN.finditer(template):
        parts.append(re.escape(template[pos : match.start()]))
        token = match.group(0)
        parts.append(re.escape(trading_day) if token in ("{date}", "{trading_day}") else r"[^/]+")
        pos = match.end()
    parts.append(re.escape(template[pos:]))
    body = "".join(parts)
    if template.endswith("/"):
        body += r".+"
    return re.compile(rf"^{body}$")


def _rows_out_floor(unit_id: str, completeness: dict) -> tuple[int | None, str]:
    """The per-key ``rows_out`` floor for a unit, and how it was arrived at.

    ``completeness.floor`` is deliberately NOT used: it is a RATIO against a
    denominator (`metron/holdings_universe.json`, "constituents - delisted"),
    which this function cannot resolve and must not approximate. The absolute
    floor is its own declaration.
    """
    declared = completeness.get("rows_out_floor")
    if declared is not None:
        return int(declared), "declared"
    na_code = completeness.get("rows_out_floor_na_code")
    if na_code:
        if na_code not in _descriptors().NA_TAXONOMY:
            raise ValueError(
                f"{unit_id}: completeness.rows_out_floor_na_code={na_code!r} is not in "
                f"observability-policy §3.5's closed taxonomy "
                f"{sorted(_descriptors().NA_TAXONOMY)}"
            )
        return None, f"not_applicable ({na_code})"
    if completeness.get("status") == "not_applicable":
        return None, f"not_applicable ({completeness.get('na_code')})"
    return DEFAULT_ROWS_OUT_FLOOR, "default"


def _finding(mode: str, unit_id: str, key: str | None, detail: str) -> dict:
    if mode not in COMPLETION_FAILURE_MODES:
        raise ValueError(f"unknown completion failure mode {mode!r}")
    return {"mode": mode, "unit": unit_id, "key": key, "detail": detail}


def _newest_manifest(s3, prefix: str, started_at):
    """The unit's newest run manifest, if it finished at or after ``started_at``.

    ``run_id`` is a ULID and the day partition is an ISO date, so the prefix
    lists in execution order and ``max()`` IS the newest run — if THAT one
    predates this execution, every other one does too.
    """
    from datetime import timedelta

    prefix = f"{prefix.rstrip('/')}/"
    start_after = f"{prefix}{(started_at - timedelta(days=MANIFEST_LOOKBACK_DAYS)).date().isoformat()}"
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": MANIFEST_BUCKET, "Prefix": prefix, "StartAfter": start_after}
        if token:
            kwargs = {"Bucket": MANIFEST_BUCKET, "Prefix": prefix, "ContinuationToken": token}
        page = s3.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json"))
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    if not keys:
        return None, None
    key = max(keys)
    doc = json.loads(s3.get_object(Bucket=MANIFEST_BUCKET, Key=key)["Body"].read())
    if _parse_ts(doc.get("finished"), where=f"{key}:finished") < started_at:
        return key, None
    return key, doc


def _check_unit(s3, unit_id: str, raw: dict, started_at) -> tuple[list[dict], dict]:
    """Grade one unit's run against its descriptor. Returns (findings, row)."""
    prefix = str(raw["run_manifest_prefix"])
    row = {"unit": unit_id, "manifest": None, "status": None, "keys_checked": 0,
           "unverifiable": [], "auto_skipped": False, "floor": None}
    key, doc = _newest_manifest(s3, prefix, started_at)
    if doc is None:
        stale = f" (newest is {key}, which finished before it)" if key else ""
        return [
            _finding(
                "manifest_missing", unit_id, None,
                f"no data_run_manifest.v1 under s3://{MANIFEST_BUCKET}/{prefix}/ finished at "
                f"or after this execution started{stale}: the workloads exited 0 but the "
                f"unit left no run record for this run",
            )
        ], row

    row["manifest"] = key
    status = str(doc.get("status") or "")
    row["status"] = status
    if status != "ok":
        return [
            _finding(
                "run_not_ok", unit_id, None,
                f"manifest {key} reports status={status!r} reason={str(doc.get('reason') or '')[:400]!r}. "
                f"Naming a unit in verify_units IS the machine's declaration that this run must "
                f"publish it, so `not_applicable` is not a pass here — a unit that may "
                f"legitimately do nothing on this schedule is simply not named.",
            )
        ], row

    outputs = list(doc.get("outputs") or [])
    trading_day = str(doc.get("trading_day") or "")
    auto_skipped = any(
        g.get("guard") == EMPTY_FRESH_GUARD and g.get("verdict") == "not_applicable"
        for g in (doc.get("guards") or [])
    )
    row["auto_skipped"] = auto_skipped
    floor, floor_source = _rows_out_floor(unit_id, raw.get("completeness") or {})
    row["floor"] = floor if floor is not None else floor_source

    findings: list[dict] = []
    for template in raw.get("writes") or []:
        pattern = _key_pattern(str(template), trading_day)
        if pattern is None:
            row["unverifiable"].append(template)
            continue
        row["keys_checked"] += 1
        matched = [o for o in outputs if pattern.match(str(o.get("key") or ""))]
        if not matched:
            if auto_skipped:
                continue
            findings.append(
                _finding(
                    "output_missing", unit_id, str(template),
                    f"{unit_id} declares it publishes {template!r} but manifest {key} lists no "
                    f"matching key in outputs[] (it lists {[o.get('key') for o in outputs]}). "
                    f"A run that did not record the artifact did not publish it.",
                )
            )
            continue
        if floor is None:
            continue
        for out in matched:
            if "rows_out" not in out:
                raise ValueError(
                    f"{key}: outputs entry {out.get('key')!r} has no rows_out; "
                    "data_run_manifest.v1 requires it and 'we did not count' is not a value"
                )
            rows = int(out["rows_out"])
            if rows < floor:
                findings.append(
                    _finding(
                        "rows_below_floor", unit_id, str(out.get("key")),
                        f"{unit_id} published {out.get('key')!r} with rows_out={rows}, below its "
                        f"floor of {floor} ({floor_source}). Manifest {key}.",
                    )
                )
    if row["unverifiable"]:
        logger.warning(
            "completion-check: %s declares %d writes entries that are not S3 key templates "
            "and are therefore ungraded: %s",
            unit_id, len(row["unverifiable"]), row["unverifiable"],
        )
    return findings, row


def _completion_check(event: dict, s3_client=None) -> dict:
    """Grade every named unit's run manifest for this execution.

    Returns ``{"completion": {...}}`` — `ok`, a `failure_mode` the ASL switches
    on, the full machine-readable `findings` list, one `units` row per unit
    (including the ones that passed, so a unit emitting nothing is visible), and
    a `summary` the Fail state uses as its Cause.
    """
    units = [str(u).strip() for u in (event.get("units") or []) if str(u).strip()]
    if not units:
        raise ValueError(
            "completion-check was invoked with no units. The ASL only reaches this state "
            "when verify_units is non-empty, so an empty list here is a mis-wired input, "
            "not a machine with nothing to verify."
        )
    started_at = _parse_ts(event.get("started_at"), where="started_at")
    collection = str(event.get("collection") or "unknown")
    descriptors = _unit_descriptors()
    s3 = s3_client if s3_client is not None else boto3.client("s3", region_name=REGION)

    findings: list[dict] = []
    rows: list[dict] = []
    for unit_id in units:
        raw = descriptors.get(unit_id)
        if raw is None:
            raise ValueError(
                f"verify_units names {unit_id!r}, which has no descriptor under "
                f"registry.d/units/. The descriptors are the only source of units "
                f"(data_collection_plan §4.1); a machine verifying a unit nobody declared "
                f"would grade nothing and read as a pass."
            )
        unit_findings, row = _check_unit(s3, unit_id, raw, started_at)
        findings.extend(unit_findings)
        rows.append(row)

    mode = next(
        (m for m in COMPLETION_FAILURE_MODES if any(f["mode"] == m for f in findings)), ""
    )
    summary = (
        f"data collection {collection}: completion check PASSED over {len(units)} unit(s)"
        if not findings
        else f"data collection {collection}: {len(findings)} completion finding(s) over "
        f"{len(units)} unit(s); first mode {mode}. "
        + " | ".join(f"[{f['mode']}] {f['unit']} {f['key'] or ''}: {f['detail']}" for f in findings)
    )[:_MAX_SUMMARY_CHARS]
    logger.info("completion-check %s: ok=%s mode=%s", collection, not findings, mode or "-")
    return {
        "completion": {
            "ok": not findings,
            "failure_mode": mode,
            "findings": findings,
            "units": rows,
            "summary": summary,
        }
    }


def _bootstrap_spec(
    workload: "str | None" = None,
    *,
    run_log: "RunLog | None" = None,
) -> SpotBootstrapSpec:
    """The part of this box's provisioning krepis renders.

    ``nousergon-data`` is public and was already cloned from a plain URL, so
    it moves here unchanged. ``alpha-engine-config`` is private and stays in
    the tail, where the PAT is read on the BOX from SSM — the renderer bakes
    URLs in as launcher-side literals, so expressing that clone here would
    mean this Lambda reading the secret and embedding it in an SSM document.

    ``run_log`` (alpha-engine-config-I11353) turns on the renderer's own
    whole-script capture: ``exec > >(tee -a …)`` over every line this script
    and its children emit, a background shipper pushing to the SAME key every
    60 s, a flush-before-read on the final copy, and a ``TERM``/``INT`` trap.
    This dispatcher used to hand-roll a strictly weaker version of all four —
    the collector's output only, one copy at exit, no periodic push, and no
    signal trap — which is why the 2026-09-21 box lost 70 of its 73 minutes.
    The shared primitive is used rather than the local copy improved, per the
    fleet's mirror-don't-fork rule.
    """
    # The manifest writer's log_location, from the SAME literal the shipper
    # writes to. Absent (a spec rendered with no run log) the writer keeps
    # `local:`, which the daily report counts as a detection gap rather than
    # rendering as fine — never silently.
    run_log_exports = {RUN_LOG_ENV: run_log.s3_uri} if run_log is not None else {}
    return SpotBootstrapSpec(
        run_log=run_log,
        repo_url=f"https://github.com/{DATA_REPO}.git",
        checkout="/home/ec2-user/alpha-engine-data",
        branch=DATA_BRANCH,
        region=REGION,
        # Same value and meaning as the hand-written timer this replaces; the
        # renderer additionally ABORTS when it cannot be armed, where the old
        # copy ended in `|| true`. A dispatcher-side failure after
        # send-command can never leave this box orphaned.
        max_runtime_seconds=_max_runtime_seconds(workload),
        exports={
            "XDG_CACHE_HOME": "/home/ec2-user/.cache",
            "FLOW_DOCTOR_ENABLED": "1",
            "ALPHA_ENGINE_DEPLOYED": "1",
            "ALPHA_ENGINE_EXPERIMENT_ID": "reference",
            # Router addressing (alpha-engine-config-I7409). Without
            # KREPIS_EXEC_CONTEXT this box never declared where it runs, so
            # krepis.router silently defaulted flow-doctor's diagnosis calls to
            # 'laptop' and tried the loopback egress proxy that only exists on
            # the dashboard box — RouterUnresolvable on every attempt (measured
            # live 2026-08-31, SPY cross-source quarantine diagnosis).
            # KREPIS_LITELLM_PROXY_URL overrides krepis's loopback default
            # (127.0.0.1:8980) to the real routed edge — mirrors
            # eval-judge-spot-dispatcher / thinktank-spot-dispatcher in
            # crucible-research, which resolve through the SAME edge from the
            # SAME kind of ephemeral spot box.
            "KREPIS_EXEC_CONTEXT": os.environ.get("DATA_SPOT_EXEC_CONTEXT", "ec2"),
            "KREPIS_LITELLM_PROXY_URL": os.environ.get(
                "DATA_SPOT_ROUTER_URL", "https://router.nousergon.ai:8443"
            ),
            "KREPIS_ROUTER_CREDENTIAL_SECRET": os.environ.get(
                "DATA_SPOT_ROUTER_CREDENTIAL_SECRET", "ROUTER_CONSUMER_DATA"
            ),
            "KREPIS_APPCONFIG_APPLICATION": os.environ.get(
                "DATA_SPOT_APPCONFIG_APPLICATION", "alpha-engine"
            ),
            "KREPIS_APPCONFIG_CONFIG_PROFILE": os.environ.get(
                "DATA_SPOT_APPCONFIG_CONFIG_PROFILE", "llm-model-registry"
            ),
            "KREPIS_APPCONFIG_ENVIRONMENT": os.environ.get(
                "DATA_SPOT_APPCONFIG_ENVIRONMENT", "production"
            ),
            **run_log_exports,
        },
    )


#: Bucket + prefix the box's whole run log lands under
#: (alpha-engine-config-I11353). It is under ``data_collection/`` and NOT the
#: historical ``_ssm_logs/`` tree on purpose: the run manifests this log is the
#: evidence for live at ``data_collection/runs/…``, and the daily report's
#: log-resolution row (`data_gate.report`) lists one prefix, not two.
RUN_LOG_BUCKET = os.environ.get("DATA_COLLECTION_RUN_LOG_BUCKET", "alpha-engine-research")
RUN_LOG_PREFIX = os.environ.get("DATA_COLLECTION_RUN_LOG_PREFIX", "data_collection/logs")

#: The env var the box exports and ``run_units.resolve_log_location()`` reads,
#: so every manifest the run writes points at the object this same launch
#: named. ONE literal, computed launcher-side, used for the key AND for the
#: variable — the manifest cannot name a key the shipper did not write.
RUN_LOG_ENV = "ALPHA_ENGINE_RUN_LOG_S3"


def _run_log_trading_day(declared: "str | None" = None) -> str:
    """The trading day this run's log is filed under.

    ``declared`` is ``event["trading_day"]`` for the workloads that carry one.
    Otherwise this resolves the last CLOSED NYSE session through the same
    ``nousergon_lib.dates.now_dual()`` chokepoint ``dates.default_run_date()``
    uses on the box, so a 07:45 ET morning launch files under the previous
    session and an 18:30 ET same-day launch files under today — matching what
    the collector itself keys its manifests by.

    Falls back to the UTC calendar date rather than raising: a log that lands
    one partition off is recoverable, a launch refused over a log path is not.
    """
    if declared:
        return declared
    try:
        from nousergon_lib.dates import now_dual

        return str(now_dual().trading_day)
    except Exception as exc:  # noqa: BLE001 — a log path must never block a launch
        import datetime as _dt

        fallback = _dt.datetime.now(_dt.timezone.utc).date().isoformat()
        logger.warning(
            "run-log trading day: now_dual() unavailable (%s) — filing under the UTC date %s",
            exc,
            fallback,
        )
        return fallback


def _run_log_uri(workload: str, trading_day: str, instance_id: str) -> str:
    """``s3://…/data_collection/logs/{workload}/{trading_day}/{instance_id}.log``."""
    return (
        f"s3://{RUN_LOG_BUCKET}/{RUN_LOG_PREFIX}/{workload}/{trading_day}/{instance_id}.log"
    )


def _bootstrap_command(
    workload: str,
    collector_cmd: str,
    run_token: str,
    *,
    instance_id: str = "unknown-instance",
    trading_day: "str | None" = None,
) -> str:
    """The async SSM RunShellScript body: install runtime, clone alpha-engine-data
    + alpha-engine-config (private config.yaml), build the venv, run the collector,
    self-terminate.

    Runs as root on the box. Composed (alpha-engine-config-I7372) as a PRELUDE
    this Lambda owns (``fail()``, EXIT trap), then
    ``krepis.spot_bootstrap.render_bootstrap()`` (run-log capture + shipper,
    watchdog unit, hard-timeout timer, interpreter, public clone), then a TAIL
    (private config clone, venv, the collector run). It used to render all of
    it inline and carried its own copy of the silent interpreter fallback
    (``command -v python3.12 ... || PYTHON_BIN=python3``) plus a hand-written
    timer and no SSM-liveness watchdog — invisible to the fleet's Bash-only
    fork scanner because this is a ``.py`` file.

    The EXIT trap is load-bearing: the rendered block runs under ``set -e``, so
    an abort inside it never reaches a ``|| fail`` and would otherwise skip the
    log upload and the shutdown. The box self-terminates on completion
    (InstanceInitiatedShutdownBehavior=terminate).

    **CloudWatch is the live tail, not the record** (alpha-engine-config-I11353).
    ``CloudWatchOutputConfig`` on the SSM command caps the ``…/stdout`` stream
    at ~1 MB: the 2026-09-21 ``shadow-sameday`` box (i-09e64cb257b556b82) wrote
    1,025,054 bytes / 4,848 lines and the stream STOPPED at 22:37:07Z, three
    minutes into a 73-minute run, before every one of the three leg failures it
    was being read for. The authoritative record is the S3 object named by
    :func:`_run_log_uri`, shipped by ``krepis.spot_bootstrap``'s run-log block
    every 60 s and again from the EXIT / TERM / INT traps, and recorded as
    ``log_location`` in every run manifest the box writes.
    """
    log = f"/var/log/data-spot-{workload}.log"
    resolved_day = _run_log_trading_day(trading_day)
    s3_log = _run_log_uri(workload, resolved_day, instance_id)
    # `_ship_run_log` / `_stop_run_log_shipper` are defined by the renderer's
    # run-log block, which comes AFTER this prelude — so a failure in the
    # prelude itself (or in the watchdog block above the run-log block) reaches
    # a trap whose shipper does not exist yet. Probe rather than assume: a
    # `command not found` inside an EXIT trap would skip the shutdown.
    prelude = f"""set -uo pipefail
mkdir -p "$(dirname {log})"
_ship_if_available() {{ if declare -F _ship_run_log >/dev/null 2>&1; then _stop_run_log_shipper 2>/dev/null || true; _ship_run_log || true; fi; }}
fail() {{ trap - EXIT; echo "[data-spot-prelude] FATAL: $1"; _ship_if_available; shutdown -h now; exit 1; }}
trap 'rc=$?; _ship_if_available; [ "$rc" -eq 0 ] || fail "bootstrap aborted (rc=$rc)"' EXIT
"""
    tail = f"""set +e
set -uo pipefail
git config --global --add safe.directory '*' || true
# alpha-engine-config is PRIVATE. The PAT is read HERE, on the box, from SSM
# via the instance profile — never known to this Lambda, never in the SSM
# document.
PAT=$(aws ssm get-parameter --name {GH_PAT_SSM} --with-decryption \\
  --query Parameter.Value --output text --region {REGION}) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CONFIG_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{CONFIG_REPO}.git" \\
  /home/ec2-user/alpha-engine-config || fail "config clone failed"
cd /home/ec2-user/alpha-engine-data
# python3.12 literally, matching the renderer, which has already installed and
# ASSERTED it: requirements.txt is resolved against 3.12 and the AMI's python3
# resolves different wheels.
python3.12 -m venv .venv || fail "venv create failed"
source .venv/bin/activate
pip install --upgrade pip -q || fail "pip upgrade failed"
pip install -q -r requirements.txt || fail "deps install failed"
# numpy<2 pin to match other spot workloads (pyarrow compiled against 1.x).
pip install -q 'numpy<2' || fail "numpy pin failed"
# gitleaks + DLP preflight boot gate, for EVERY workload (alpha-engine-config-I10370,
# I10866). Same pin, same asset, same gate as _spot_common.sh::install_gitleaks_dlp.
# Without it, flow-doctor's diagnosis fails closed with "gitleaks binary not found
# on PATH", and the page goes out with no diagnosis. Runs as root, so no sudo.
if ! command -v gitleaks >/dev/null 2>&1; then
  curl -fsSL -o /tmp/gitleaks.tar.gz \\
    "https://github.com/gitleaks/gitleaks/releases/download/v{GITLEAKS_VERSION}/gitleaks_{GITLEAKS_VERSION}_linux_x64.tar.gz" \\
    || fail "gitleaks download failed"
  echo "{GITLEAKS_SHA256}  /tmp/gitleaks.tar.gz" | sha256sum -c - || fail "gitleaks sha256 mismatch"
  tar -xzf /tmp/gitleaks.tar.gz -C /usr/local/bin gitleaks || fail "gitleaks extract failed"
  chmod +x /usr/local/bin/gitleaks || fail "gitleaks chmod failed"
  rm -f /tmp/gitleaks.tar.gz
fi
command -v gitleaks >/dev/null 2>&1 || fail "gitleaks binary unavailable after install (fail-closed)"
python -m krepis.session_dlp preflight || fail "DLP preflight failed (gitleaks binary/config not ready)"
# Diagnosis-transport preflight, for EVERY workload (alpha-engine-config-I10880).
# flow-doctor's `diagnosis.provider: router` wire imports `openai`
# (flow_doctor/diagnosis/provider.py's RouterProvider) at CALL time, not
# import time, so a missing package was never caught until the first real
# diagnosis — measured on this exact box class 2026-09-15 (i-0521ac62ccfa5196a,
# shadow-weekday, SSM command 426ffbd1): "ModuleNotFoundError: No module
# named 'openai'", diagnosis filed WITHOUT a diagnosis. requirements.txt now
# declares it (krepis[openai] pin, installed by the `pip install -r
# requirements.txt` above); this gate catches future drift the same way the
# DLP gate above does: fail closed at boot, not at the first LLM call.
python -c "import openai" || fail "openai package (flow-doctor diagnosis router wire) not installed"
# No `| tee` here: the renderer's run-log block already `exec`'d this shell's
# stdout+stderr through tee, so every line of the collector is captured with
# the provisioning that preceded it. Piping again would double every line AND
# put tee's exit status in the way of the collector's
# (bugclass_a_pipe_into_tee_discards_the_exit_code_260920) — `set -o pipefail`
# is on, but the shorter path has no pipe to get wrong.
{collector_cmd}
rc=$?
[ "$rc" -eq 0 ] || fail "workload {workload} exited $rc"
echo "[data-spot] workload {workload} complete"
"""
    # gzip=True (alpha-engine-config-I11359): periodic pushes stay plain
    # (append semantics, readable mid-run), and the final EXIT/TERM-trap push
    # writes `<s3_uri>.gz`, deleting the plain object once it lands. A
    # 73-minute shadow run's log is ~1 MB+, five runs a weekday — the
    # compressed final object is what a post-mortem reads days later, so it
    # costs nothing that matters. `run_units.resolve_log_location()` and the
    # `-I11353` data-report detector both try `.log.gz` before `.log` to
    # account for the swap.
    spec = _bootstrap_spec(workload, run_log=RunLog(local_path=log, s3_uri=s3_log, gzip=True))
    return prelude + "\n" + render_bootstrap(spec) + "\n" + tail


def _launch_instance(force_on_demand: bool = False, extra_tags: dict | None = None) -> tuple[str, str]:
    """Launch the data spot box; spot first, on-demand fallback on capacity
    exhaustion OR account-wide spot quota exhaustion (config#2698 — e.g.
    MaxSpotInstanceCountExceeded; a 2026-07-15 incident hard-failed
    LaunchPostMarketDataSpot instead of falling back, since this launcher calls
    ec2_spot.launch directly rather than through nousergon_lib.spot_dispatch's
    launch_with_fallback chokepoint and had no quota-specific branch) — a
    pre-open enrich the predictor reads next must not be starved by either. Mirrors
    _launch_instance in scheduled-groom-dispatcher.

    force_on_demand=True skips the spot attempt entirely. Set by the EOD SF's
    bounded retry-on-relaunch (2026-07-14 incident: a data-spot box was
    reclaimed by AWS — Server.SpotInstanceTermination — ~22min into a
    post-market-data run, which the SF now retries once) and, identically
    (config#2542), by the weekday SF's morning-enrich/morning-arctic-append
    retry-on-relaunch. A workload that already lost one box to a spot
    interruption should not gamble on spot again for its one retry attempt —
    the cost delta is a few cents for a sub-hour c5.large-class box,
    negligible against the reconcile-reliability this buys.

    extra_tags (config#5504): per-run identity tags (execution_id, run_date,
    pipeline_role) ride the SAME RunInstances call atomically via krepis.ec2_spot's
    extra_tags kwarg — never a separate post-launch create_tags call subject to a
    race. When omitted, the box is launched with only the Name tag and the cost
    tag below.

    Cost-allocation tag (alpha-engine-config-I10788): every launch — spot or
    on-demand, every branch below — carries COST_TAG_KEY=COST_TAG_VALUE,
    merged into the SAME extra_tags dict so it rides the SAME atomic
    RunInstances call as the per-run identity tags. Unconditional: this is the
    resource class the issue's scope measurement found carrying no cost tag
    at all, and it is where nearly all of this component's spend is."""
    tags = dict(extra_tags or {})
    tags[COST_TAG_KEY] = COST_TAG_VALUE
    common = dict(
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        shutdown_behavior="terminate",
        tag_name="alpha-engine-data-spot",
        extra_tags=tags,
        region=REGION,
    )
    if force_on_demand:
        logger.info(
            "force_on_demand=True (spot-interruption retry) — launching ON-DEMAND directly, skipping spot"
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"
    try:
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=True, **common)
        return iid, "spot"
    except SpotCapacityExhausted:
        logger.warning(
            "spot capacity exhausted across all type x subnet pools — relaunching ON-DEMAND"
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"
    except SpotQuotaExceededError as exc:
        # Account-wide (config#2698) — distinct from ordinary capacity rotation
        # exhaustion, so this gets its own operator page: capacity exhaustion
        # self-heals as AWS capacity shifts, but a quota ceiling only clears via
        # a service-quota increase, which needs a human to notice and request.
        logger.warning("spot quota exceeded (%s) — relaunching ON-DEMAND", exc)
        alerts.publish(
            f"EC2 spot quota exceeded for 'alpha-engine-data-spot' in {REGION} — "
            f"falling back to on-demand: {exc}",
            severity="warning",
            source="data-spot-dispatcher._launch_instance",
            dedup_key=f"spot-quota-exceeded-{REGION}",
        )
        iid = ec2_spot.launch(INSTANCE_TYPES, SUBNETS, spot=False, **common)
        return iid, "on-demand"


def _wait_ssm_online(instance_id: str) -> None:
    """Block until the instance is running AND its SSM agent registers Online."""
    import time

    ec2 = boto3.client("ec2", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    ec2.get_waiter("instance_running").wait(
        InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60}
    )
    deadline = time.time() + SSM_ONLINE_BUDGET_SEC
    while time.time() < deadline:
        info = ssm.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        ).get("InstanceInformationList", [])
        if info and info[0].get("PingStatus") == "Online":
            logger.info("SSM agent Online for %s", instance_id)
            return
        time.sleep(5)
    raise RuntimeError(
        f"SSM agent not Online after {SSM_ONLINE_BUDGET_SEC}s for {instance_id}"
    )


def _send_bootstrap(
    instance_id: str,
    workload: str,
    collector_cmd: str,
    run_token: str,
    trading_day: "str | None" = None,
) -> str:
    """Fire the async, detached SSM command that runs the collector + self-terminates."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"data-spot {workload} — config#1767",
        Parameters={
            "commands": [
                _bootstrap_command(
                    workload,
                    collector_cmd,
                    run_token,
                    instance_id=instance_id,
                    trading_day=trading_day,
                )
            ],
            # Execution timeout (NOT the start timeout) — without this SSM kills
            # the command at the 3600s default, guillotining the append tail.
            "executionTimeout": [str(_max_runtime_seconds(workload))],
        },
        TimeoutSeconds=600,  # time to START delivering before giving up
        CloudWatchOutputConfig={
            "CloudWatchLogGroupName": CW_LOG_GROUP,
            "CloudWatchOutputEnabled": True,
        },
    )
    return resp["Command"]["CommandId"]


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps failed.
    Without this the box orphans: it received no bootstrap, so neither the in-script
    watchdog nor the EXIT trap is running to tear it down. Never masks the original
    error (logged, not raised)."""
    try:
        boto3.client("ec2", region_name=REGION).terminate_instances(InstanceIds=[instance_id])
        logger.warning("terminated data-spot box %s after post-launch failure (no orphan)", instance_id)
    except Exception as exc:  # noqa: BLE001 — cleanup; original error re-raises below
        logger.error(
            "FAILED to terminate %s after a post-launch error (%s) — MANUAL cleanup needed",
            instance_id, exc,
        )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function handler — launch the data spot box for one workload.

    Two pure-computation actions launch nothing and return before any of the
    below: ``{"action": "trading-day-check"}`` and
    ``{"action": "completion-check", "units": [...], "started_at": ...,
    "collection": ...}`` (alpha-engine-config-I10787).

    `event` carries {"workload": "morning-enrich" | "morning-arctic-append" |
    "post-market-data" | "post-market-arctic-append" | "daily-heal" | ... |
    "shadow-weekday", "force_on_demand": bool, "execution_id": str,
    "run_date": str, "pipeline_role": str}. The "shadow-weekday" workload
    additionally requires "trading_day" (ISO YYYY-MM-DD) — see
    `_WORKLOADS_REQUIRING_TRADING_DAY`. `force_on_demand` (default False) is set by the
    EOD SF's post-interruption retry (2026-07-14 incident) and the weekday
    SF's identical retry (config#2542) so the one retry attempt never gambles
    on spot a second time; the "daily-heal" workload (alpha-engine-config-
    I2717) is invoked directly by its own EventBridge rule (NOT from either
    SF) and omits `force_on_demand`, so it defaults to False (spot-first, no
    retry-budget coupling to either pipeline).

    execution_id / run_date / pipeline_role (config#5504): per-run identity
    fields threaded from the SF execution context ($$.Execution.Id,
    $$.Execution.StartTime, $.pipeline_role). They ride the RunInstances call
    atomically as EC2 tags so every launched instance is attributable to the
    SF execution that launched it — the prerequisite for per-run cost
    measurement. Any field omitted (e.g. daily-heal EventBridge invoke, which
    has no SF execution context) is simply not tagged.

    Returns, wrapped under a `data_spot` key (mirrors the groom dispatcher's
    `groom` wrap so the SF's JSONPath is $.<result>.Payload.data_spot.*):

      {"launched": true, "instance_id", "command_id", "workload", "run_token"}
      or {"launched": false, "reason": "disabled"} under the kill-switch.

    Fail-loud on launch: a launch/SSM error RAISES so the SF's Catch converts it
    to the fail-open continue branch (config#1767 deliverable #4). Any box brought
    up before the error is torn down first so nothing orphans.
    """
    event = event or {}
    if event.get("action") == "trading-day-check":
        return _trading_day_check()
    if event.get("action") == "completion-check":
        return _completion_check(event)
    workload, collector_cmd = _resolve_workload(event)
    force_on_demand = bool(event.get("force_on_demand", False))

    # Per-run identity tags (config#5504): attribute every launched instance
    # to the SF execution so per-run EC2 cost is measurable. Gracefully
    # absent for invocations without SF execution context (daily-heal
    # EventBridge, operator re-drives).
    extra_tags = {}
    for key, tag_name in (
        ("execution_id", "execution-id"),
        ("run_date", "run-date"),
        ("pipeline_role", "pipeline-role"),
    ):
        val = str(event.get(key, "")).strip()
        if val:
            extra_tags[tag_name] = val

    if not DISPATCH_ENABLED:
        logger.warning("DATA_SPOT_DISPATCH_ENABLED=false — data spot NOT launched")
        return {"data_spot": {"launched": False, "reason": "disabled", "workload": workload}}

    run_token = uuid.uuid4().hex
    # Resolved BEFORE the launch so the log URI this call returns is the one
    # the box was told to write — the SF and the run manifests then quote the
    # same string, and a reader never has to guess which partition a failed
    # run's log landed in (alpha-engine-config-I11353).
    run_log_day = _run_log_trading_day(str(event.get("trading_day") or "").strip() or None)
    instance_id, market = _launch_instance(force_on_demand=force_on_demand, extra_tags=extra_tags or None)
    logger.info("launched data-spot box %s (%s) for %s", instance_id, market, workload)
    # Once the box is up, ANY failure before the bootstrap command is delivered
    # would orphan it (no watchdog/trap yet). Terminate-on-error so a slow
    # SSM-online or an SSM SendCommand error tears the box down.
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(
            instance_id, workload, collector_cmd, run_token, trading_day=run_log_day
        )
    except Exception:
        _terminate_instance(instance_id)
        raise
    log_location = _run_log_uri(workload, run_log_day, instance_id)
    logger.info(
        "data-spot dispatched: instance=%s market=%s command=%s workload=%s run_token=%s log=%s",
        instance_id, market, command_id, workload, run_token, log_location,
    )
    return {
        "data_spot": {
            "launched": True,
            "instance_id": instance_id,
            "market": market,
            "command_id": command_id,
            "workload": workload,
            "run_token": run_token,
            # Returned so a failed execution's history NAMES the log, rather
            # than a reader having to reconstruct the key from the instance id.
            "log_location": log_location,
        }
    }
