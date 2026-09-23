"""alpha-engine-weekly-freshness-spot-dispatcher — launch the Saturday weekly
pipeline's LAUNCHER box on a fresh, ephemeral EC2 spot instead of the
always-on dashboard box (nousergon/alpha-engine-config#2248).

WHY THIS EXISTS (config#2248): all 14 `ne-weekly-freshness-pipeline`
sendCommand/Lambda states that touch an EC2 instance key off
`$.ec2_instance_id`, and that field was a HARDCODED literal
(i-09b539c844515d549 — the always-on dashboard box, which also runs 12 live
services) baked into the SaturdayTrigger EventBridge Input and the Friday
shell-run trigger Lambda. A full disk on that box killed the entire weekly
pipeline — it is a structural single point of failure for the whole SF, not
because it does the heavy lifting (MorningEnrich/DataPhase1/Backtester/etc.
each launch their OWN nested spot via `spot_data_weekly.sh`/`spot_backtest.sh`
and only use the dashboard box as a LAUNCHER), but because the SF has no
launcher of its own — it borrows a persistent, stateful, shared box for that
role every week.

THIS Lambda breaks that coupling: dispatched ONCE per SF execution (from a
new leading Choice/Task pair in step_function.json, after AcquireMutex and
before any of the 14 consumer states), it launches a FRESH ephemeral spot,
clones all four repos the consumer states' `git -C ... pull --ff-only`
commands expect at their dashboard-box paths
(`/home/ec2-user/alpha-engine-{data,config,backtester,dashboard}`), builds
`/home/ec2-user/alpha-engine-dashboard/.venv` (the interpreter
`MorningEnrich`/`DataPhase1`/etc. invoke via
`/home/ec2-user/alpha-engine-dashboard/.venv/bin/python -m
krepis.ssm_log_capture`), and returns the new instance id for the SF to
thread into `$.ec2_instance_id` — so all 14 downstream states work UNCHANGED
(same paths, same venv, same `InstanceIds.$: "$.ec2_instance_id"` reference).

Mechanism (mirrors the fleet `nousergon_lib.spot_dispatch` chokepoint —
`launch_with_fallback` + `wait_ssm_online` + `send_async_command`, same as
`alert-drain-dispatcher`/`ci-watch-dispatcher`/`scheduled-groom-dispatcher`):
  1. `spot_dispatch.launch_with_fallback()` rotates instance_type x subnet on
     capacity error; on SpotCapacityExhausted/SpotQuotaExceededError across
     all pools we relaunch ON-DEMAND — a capacity dip must never sink the
     whole weekly run.
  2. Wait for the instance to run + its SSM agent to come Online.
  3. Fire an ASYNC, detached `ssm send-command` that clones the 4 repos +
     builds the dashboard venv, THEN runs the two direct on-box workloads
     this box itself executes (SaturdayHealthCheck/WeeklySubstrateHealthCheck
     are the only 2 of the 14 consumers that run ON this box rather than on a
     nested spot — but those are separate SF states with their own commands;
     this Lambda's bootstrap is ONLY the clone+venv setup those and every
     other consumer state's `git pull` depends on already having succeeded).
     The Lambda returns immediately with instance_id + command_id — it does
     NOT block on the multi-minute clone+venv-build. The SF's own
     WaitForWeeklyFreshnessSpotBootstrap/Check.../Wait polling loop (mirrors
     the existing WaitForMorningEnrich-style idiom used by every other
     sendCommand state in this SF) polls `ssm:getCommandInvocation` to a
     terminal status BEFORE the SF proceeds into CheckShellRun /
     CheckSkipMorningEnrich — so no downstream state can race an incomplete
     bootstrap.

NOT SELF-TERMINATING AFTER ONE WORKLOAD (unlike data-spot-dispatcher's nested
spots): this box IS the launcher for the WHOLE weekly pipeline — it must stay
up for the SF's entire run (TimeoutSeconds 43200 = 12h at the top level). Its
bootstrap arms a `systemd-run --on-active=<seconds>` shutdown watchdog sized
to comfortably exceed that 12h ceiling (WATCHDOG_SECONDS default 46800 = 13h)
as an orphan-prevention backstop only — nothing on the happy path relies on
it firing. `InstanceInitiatedShutdownBehavior=terminate` (set by
spot_dispatch.launch_with_fallback's `shutdown_behavior="terminate"`) so the
watchdog's `shutdown -h now` actually TERMINATES the box, not just stops it.

That timer, the interpreter install and the four PUBLIC repo clones are all
rendered by `krepis.spot_bootstrap` since alpha-engine-config-I7372 — see
`_bootstrap_spec()` / `_bootstrap_command()`. The cutover also adds the
`ec2-spot-watchdog` unit this box never had: it answers "the SSM agent died
and nothing can ever reach this box again", which for a launcher driven
ENTIRELY over SSM is the failure mode that strands the whole weekly run.

IAM: reuses `alpha-engine-executor-profile` (-> `alpha-engine-executor-role`,
home repo `alpha-engine`) — the SAME profile `spot_data_weekly.sh` /
`spot_backtest.sh` grant their Saturday spots, and the SAME profile
`data-spot-dispatcher`'s launched box uses. That role already carries
`ec2:RunInstances`/`ec2:CreateTags`/`ec2:DescribeInstances` etc. (see
`scheduled-groom-dispatcher/index.py`'s header — "alpha-engine-executor-role,
which already has..."), which is exactly what THIS box needs to itself launch
the nested spots `spot_data_weekly.sh`/`spot_backtest.sh` create — no new
role, no IAM change outside this Lambda's own execution role
(iam-policy.json).

FAIL-LOUD (mirrors data-spot-dispatcher, NOT alert-drain-dispatcher's clean-
JSON contract): this Lambda is invoked by the Step Function via
`arn:aws:states:::lambda:invoke`, so a launch/SSM failure RAISES — the SF's
own Catch (-> ExtractWeeklyFreshnessSpotDispatchError -> NormalizeFailureContext
-> HandleFailure) converts it into the SAME loud SNS-paged failure path every
other Task state in this SF uses. There is no fail-open branch here (unlike
data-spot-dispatcher's weekday/EOD fail-open posture): the weekly pipeline
cannot run AT ALL without a launcher box, so a dispatch failure must halt the
run loudly, not silently skip 13 downstream states.

ESCAPE HATCH: the SF's new CheckSpotDispatchNeeded Choice (step_function.json,
inserted right after AcquireMutex) skips this Lambda entirely when
`$.ec2_instance_id` is ALREADY present/non-empty on the execution input — the
operator manual-override / partial-redrive-against-an-existing-box path.
`scripts/weekly_sf_rerun.py`'s `rerun_input()` passthrough (unchanged by this
PR) is exactly that path: a watch-rerun's emitted input carries the ORIGINAL
failed execution's `ec2_instance_id` (which THIS Lambda populated on the
original run) verbatim, so a recovery rerun reuses the same still-live spot
rather than paying for a second launch.

Managed OUTSIDE CloudFormation (same as every sibling dispatcher): operator-
deployed via `deploy.sh --bootstrap`. Merging the PR has ZERO live effect
until the Lambda + IAM are deployed AND the weekly SF is re-deployed with the
new states — see this dispatcher's README for the exact rollout order.
"""

from __future__ import annotations

import datetime
import logging
import os
import uuid

from krepis.spot_bootstrap import Clone, SpotBootstrapSpec, render_bootstrap
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch: disables the launch without deleting the SF states. There is
# deliberately NO fail-open/skip branch downstream of this flag on the SF
# side — flipping it off is an explicit "I will pass ec2_instance_id myself"
# operator action (the CheckSpotDispatchNeeded escape hatch), not a silent
# no-op. Default ON.
DISPATCH_ENABLED = (
    os.environ.get("WEEKLY_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config (env-overridable; defaults mirror spot_data_weekly.sh /
# spot_backtest.sh — same AMI/instance-type family/subnets the nested spots
# THIS box itself launches already use, so a c5.large-class launcher is
# consistent with the rest of the fleet's Saturday spend). ───────────────────
# Widened 4 -> 10 types across 6 families (alpha-engine-config-I7133), then
# ORDERED current-generation-first (alpha-engine-config-I11427).
#
# The original four were all 2-vCPU x86 compute/general types of adjacent
# generations, which is a NARROW capacity surface: `launch_with_fallback`
# rotates instance_type x subnet, so the number of distinct pools it can fall
# through is what decides whether a capacity dip is survivable. Measured
# 2026-08-12: 3 of 11 recent spot requests in this account died
# `instance-terminated-no-capacity`, one of them mid-DataPhase1 on the
# scheduled weekly run (config-I7119).
#
# I7119 makes a mid-run reclaim RECOVERABLE. This makes it RARER, which is the
# better half — a recovery still costs a relaunch, a re-bootstrap and the
# stage's runtime. Recovery is the floor, not the goal.
#
# Every addition is x86_64 (the AMI below is x86_64 AL2023 — an arm64 type
# would fail the architecture check at launch), 2 vCPU, and >= the 4 GiB of
# the c5.large that already runs this workload successfully; the m/r families
# are strictly more memory. All 10 verified offered in 5 of the 6 subnets'
# AZs on 2026-08-12.
#
# ORDER is a COST property, not a style question. `launch_with_fallback` hands
# this list to `krepis.ec2_spot.launch`, which walks instance_type x subnet IN
# ORDER, and the on-demand rung buys the FIRST entry. Measured August 2026:
# `BoxUsage:c5.large` $38.83 / 456.8 hrs ran CONCURRENTLY with
# `SpotUsage:c5.large` $15.70 / 489.0 hrs — 945.8 hours against 744 in the
# month, a ~48% escalation to on-demand, all of it billed at the head of this
# list. Leading with `c6i.large` costs nothing (same on-demand rate as
# `c5.large`) and puts the deepest current-generation us-east-1 c-family pool
# first on the spot rung too. The generation-7 c-family pair follows the
# gen-6 head, so the on-demand rung (the FIRST entry) is unchanged while the
# spot rung gains two more pools before it falls back to the m/r families and
# generation 5 (alpha-engine-config-I11433). Each rung alternates Intel/AMD
# so no rung is single-vendor (alpha-engine-config-I11427; same order
# property as the shell launchers, alpha-engine-config-I11412).
#
# This list IS governed by an allow-list, contrary to the older comment here:
# alpha-engine-config-I11227 replaced the unconditioned `Resource: "*"` grant
# with `RunInstancesInstanceScopedByTagAndType` in this directory's
# `iam-policy.json`, which enumerates `ec2:InstanceType`. A type named here
# but absent there raises `UnauthorizedOperation`, which
# `krepis.ec2_spot.launch` treats as a NON-capacity error and re-raises as
# `SpotLaunchError` WITHOUT rotating — i.e. it converts a survivable capacity
# dip into a hard weekly-SF failure. The two are held in lockstep by
# `tests/test_weekly_spot_pool_breadth.py::test_the_default_is_not_refused_by_the_roles_own_iam_allow_list`.
# Adding a type is therefore TWO edits plus `deploy.sh --apply-iam` (the CI
# auto-deploy path is code-only and will NOT apply the policy). The
# generation-7 rung (`c7i.large` / `c7a.large`, both 2 vCPU / 4 GiB / x86_64)
# landed that way under alpha-engine-config-I11433: the role's condition was
# widened live BEFORE this default named the types, because the reverse
# order is an outage that opens on merge.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "WEEKLY_SPOT_INSTANCE_TYPES",
        "c6i.large,c6a.large,c7i.large,c7a.large,m6i.large,m6a.large,"
        "r6i.large,c5.large,c5a.large,m5.large,m5a.large,r5.large",
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "WEEKLY_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("WEEKLY_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("WEEKLY_SPOT_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("WEEKLY_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# Same profile the nested spots THIS box launches already run under, and the
# same profile data-spot-dispatcher's box uses — grants ec2:RunInstances/
# CreateTags/DescribeInstances (this box launching its OWN nested spots),
# ssm:GetParameter on /alpha-engine/* (PAT + other secrets), and the Arctic/S3
# read-write the two on-box health checks need.
IAM_PROFILE = os.environ.get("WEEKLY_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# Modest disk: this box does not itself hold price data (its nested spots do
# their own large-disk launches) — it only holds 4 shallow repo clones + one
# venv. Headroom above the groom box's 40GB since the dashboard venv pulls in
# the full nousergon_lib/krepis/pandas/numpy/pyarrow stack.
VOLUME_SIZE_GB = int(os.environ.get("WEEKLY_SPOT_VOLUME_SIZE_GB", "40"))

DATA_REPO = os.environ.get("WEEKLY_SPOT_DATA_REPO", "nousergon/nousergon-data")
DATA_BRANCH = os.environ.get("WEEKLY_SPOT_DATA_BRANCH", "main")
CONFIG_REPO = os.environ.get("WEEKLY_SPOT_CONFIG_REPO", "nousergon/alpha-engine-config")
CONFIG_BRANCH = os.environ.get("WEEKLY_SPOT_CONFIG_BRANCH", "main")
BACKTESTER_REPO = os.environ.get("WEEKLY_SPOT_BACKTESTER_REPO", "nousergon/crucible-backtester")
BACKTESTER_BRANCH = os.environ.get("WEEKLY_SPOT_BACKTESTER_BRANCH", "main")
DASHBOARD_REPO = os.environ.get("WEEKLY_SPOT_DASHBOARD_REPO", "nousergon/crucible-dashboard")
DASHBOARD_BRANCH = os.environ.get("WEEKLY_SPOT_DASHBOARD_BRANCH", "main")
PREDICTOR_REPO = os.environ.get("WEEKLY_SPOT_PREDICTOR_REPO", "nousergon/crucible-predictor")
PREDICTOR_BRANCH = os.environ.get("WEEKLY_SPOT_PREDICTOR_BRANCH", "main")

# alpha-engine-config is private; the box reads the fleet PAT from SSM via its
# instance profile — same pattern data-spot-dispatcher/scheduled-groom-
# dispatcher/alert-drain-dispatcher all already use.
GH_PAT_SSM = os.environ.get(
    "WEEKLY_SPOT_GH_PAT_SSM", "/alpha-engine/saturday_sf_watch/github_pat"
)

# Bootstrap (clone x5 + TWO venv builds) execution timeout — the SSM command's
# own ceiling, independent of the SF's poll loop. Generous: 5 shallow clones +
# a full nousergon_lib/krepis/pandas/numpy/pyarrow venv build realistically
# takes low-single-digit minutes, but a cold pip index / dnf mirror can be
# slow; bounding at 30 min leaves large headroom without risking a false
# guillotine on a slow-but-healthy build.
#
# Raised 1200 -> 1800 with the alpha-engine-data venv (config-I7427): that
# build pulls arcticdb, a large native wheel, on top of the dashboard venv's
# closure. Two builds under a ceiling sized for one is how a correct bootstrap
# starts failing on a slow mirror day and reads as an infrastructure fault.
BOOTSTRAP_TIMEOUT_SECONDS = int(
    os.environ.get("WEEKLY_SPOT_BOOTSTRAP_TIMEOUT_SECONDS", "1800")
)
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("WEEKLY_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get("WEEKLY_SPOT_CW_LOG_GROUP", "/alpha-engine/weekly-freshness-spot")

# Orphan-prevention backstop ONLY — sized to comfortably exceed the weekly
# SF's own top-level TimeoutSeconds (43200s = 12h, step_function.json) so it
# never fires on a healthy run. 46800s = 13h: 1h of headroom past the SF's
# own hang-detection ceiling, so a genuinely hung SF still gets caught by
# TIMED_OUT (routing into sf-watch, per test_sf_global_timeout.py) before
# this watchdog would ever pull the box out from under it.
WATCHDOG_SECONDS = int(os.environ.get("WEEKLY_SPOT_WATCHDOG_SECONDS", "46800"))


def _bootstrap_spec() -> SpotBootstrapSpec:
    """Everything about this box's provisioning that krepis renders.

    Only ``alpha-engine-config`` is a private repo (measured 2026-08-14 with
    ``gh repo view --json visibility``: PUBLIC for nousergon-data,
    crucible-backtester, crucible-predictor and crucible-dashboard; PRIVATE
    for alpha-engine-config). The four public ones were being cloned through
    a PAT-bearing URL they never needed, which put the fleet token in the
    box's ``git clone`` argv and in ``.git/config`` for four checkouts. They
    move here as plain literals; the private one stays in the tail below,
    where the PAT is read on the BOX from SSM and never leaves it.

    The renderer bakes URLs and branches in as launcher-side literals, so a
    ``${PAT}`` clone cannot be expressed through it — and must not be, because
    expressing it would mean this Lambda reading the secret and embedding it
    in an SSM document. The credential's location is unchanged by this PR; the
    number of places carrying it drops from four to one.
    """
    return SpotBootstrapSpec(
        repo_url=f"https://github.com/{DATA_REPO}.git",
        checkout="/home/ec2-user/alpha-engine-data",
        branch=DATA_BRANCH,
        region=REGION,
        extra_clones=(
            Clone(
                repo_url=f"https://github.com/{BACKTESTER_REPO}.git",
                checkout="/home/ec2-user/alpha-engine-backtester",
                branch=BACKTESTER_BRANCH,
            ),
            Clone(
                repo_url=f"https://github.com/{PREDICTOR_REPO}.git",
                checkout="/home/ec2-user/alpha-engine-predictor",
                branch=PREDICTOR_BRANCH,
            ),
            Clone(
                repo_url=f"https://github.com/{DASHBOARD_REPO}.git",
                checkout="/home/ec2-user/alpha-engine-dashboard",
                branch=DASHBOARD_BRANCH,
            ),
        ),
        # Orphan-prevention backstop, unchanged in value and meaning — the
        # renderer emits the same transient `systemd-run --on-active` timer
        # this function used to write by hand. It now ABORTS the bootstrap if
        # the timer cannot be armed, where the hand-written copy ended in
        # `|| true`: "the cap could not be armed" is exactly the condition
        # under which a box that must not outlive the SF should not start.
        max_runtime_seconds=WATCHDOG_SECONDS,
        # The renderer's preamble sets XDG_CACHE_HOME=/tmp; exports are emitted
        # after it, so this preserves the cache path this box used before.
        exports={"XDG_CACHE_HOME": "/home/ec2-user/.cache"},
    )


def _bootstrap_command(run_token: str) -> str:
    """The async SSM RunShellScript body: install the runtime, clone the repos
    the 14 downstream SF states' `git -C ... pull --ff-only` commands expect at
    their dashboard-box paths, build the dashboard venv, arm the long-lived
    watchdog. Runs as root; the repos land under /home/ec2-user so the
    downstream states' `sudo -u ec2-user git -C ... pull` succeeds unchanged
    (they pull, not clone — this bootstrap does the initial clone).

    Deliberately does NOT run any workload itself (unlike data-spot-
    dispatcher's/scheduled-groom-dispatcher's bootstrap, which execs straight
    into the actual job) — this box's job IS the clone+venv setup; the 14
    consumer states drive the actual work via their own separate sendCommand
    calls once the SF's poll loop observes this command reach Success.

    ## Composition (alpha-engine-config-I7372)

    Three parts, in order:

    1. a PRELUDE this Lambda owns — the tee'd log, ``fail()``, an EXIT trap;
    2. ``krepis.spot_bootstrap.render_bootstrap()`` — the watchdog unit, the
       hard-timeout timer, the interpreter, the four public clones;
    3. a TAIL this Lambda owns — the private config clone, the ownership
       fixes and the dashboard venv, none of which the renderer can express.

    This function used to render all of it inline and carried its own copy of
    three things the fleet had already paid for elsewhere: the silent
    interpreter fallback (``command -v python3.12 ... || PYTHON_BIN=python3``,
    which resolves a different wheel set and says nothing), a hand-written
    timer, and no SSM-liveness watchdog at all — the unit that answers "the
    SSM agent died and nothing can ever reach this box again", which is the
    one failure mode a launcher box driven ENTIRELY over SSM cannot survive.
    The fleet's fork scanner is Bash-only, so none of it was visible to the
    sweep that found the shell copies.

    The EXIT trap is new and load-bearing: the rendered block runs under
    ``set -e`` (the renderer's contract), so a failure inside it aborts the
    script rather than reaching a ``|| fail``. Without the trap that abort
    would skip the log upload and leave the box idling until the 13h watchdog.
    The tail keeps ``set -uo pipefail`` + explicit ``|| fail``, its posture
    before this change.
    """
    log = f"/var/log/weekly-freshness-spot-bootstrap-{run_token}.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/weekly-freshness-spot/bootstrap/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{run_token}.log"
    )
    prelude = f"""set -uo pipefail
mkdir -p "$(dirname {log})"
exec > >(tee -a {log}) 2>&1
fail() {{ trap - EXIT; echo "[weekly-freshness-spot-bootstrap] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
trap 'rc=$?; [ "$rc" -eq 0 ] || fail "bootstrap aborted (rc=$rc)"' EXIT
"""
    tail = f"""set +e
set -uo pipefail
git config --global --add safe.directory '*' || true
# alpha-engine-config is the one PRIVATE repo of the five. The PAT is read
# HERE, on the box, from SSM via the instance profile — it is never known to
# this Lambda and never appears in the SSM document.
PAT=$(aws ssm get-parameter --name {GH_PAT_SSM} --with-decryption \\
  --query Parameter.Value --output text --region {REGION}) || fail "PAT read failed"
[ -n "$PAT" ] || fail "PAT empty"
echo "[weekly-freshness-spot-bootstrap] cloning alpha-engine-config..."
rm -rf /home/ec2-user/alpha-engine-config
git clone --depth 1 --branch {CONFIG_BRANCH} \\
  "https://x-access-token:${{PAT}}@github.com/{CONFIG_REPO}.git" \\
  /home/ec2-user/alpha-engine-config || fail "alpha-engine-config clone failed"
chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-data /home/ec2-user/alpha-engine-config \\
  /home/ec2-user/alpha-engine-backtester /home/ec2-user/alpha-engine-dashboard \\
  /home/ec2-user/alpha-engine-predictor || fail "chown failed"
# crucible-backtester's config.yaml is gitignored (the .example pattern), and
# spot_backtest.sh hard-exits without it: "ERROR: config.yaml not found".
# The canonical provisioning is a symlink into alpha-engine-config's TRACKED
# backtester/config.yaml -- the same shape the long-lived dashboard box has
# carried by hand since 2026-04-11, and the shape spot_backtest.sh's own
# pre-launch check expects (it warns when the target is NOT inside a git repo,
# because operator flags outside version control have no audit trail).
# Without this, every backtest-family stage dies on a freshly-built box.
echo "[weekly-freshness-spot-bootstrap] linking backtester config.yaml..."
ln -sfn /home/ec2-user/alpha-engine-config/backtester/config.yaml \\
  /home/ec2-user/alpha-engine-backtester/config.yaml || fail "config.yaml symlink failed"
chown -h ec2-user:ec2-user /home/ec2-user/alpha-engine-backtester/config.yaml || fail "config.yaml chown failed"
echo "[weekly-freshness-spot-bootstrap] building alpha-engine-dashboard venv..."
cd /home/ec2-user/alpha-engine-dashboard
# python3.12 literally, matching the renderer: it has already installed and
# ASSERTED the interpreter, so there is nothing left to select between.
python3.12 -m venv .venv || fail "venv create failed"
source .venv/bin/activate
pip install --upgrade pip -q || fail "pip upgrade failed"
if [ -f requirements.txt ]; then
  pip install -q -r requirements.txt || fail "dashboard requirements install failed"
fi
# numpy<2 pin to match every other spot workload (pyarrow compiled against 1.x).
pip install -q 'numpy<2' || fail "numpy pin failed"
chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-dashboard/.venv || fail "venv chown failed"
deactivate
# A SECOND venv, for alpha-engine-data's own code (alpha-engine-config-I7427).
#
# WeeklySubstrateHealthCheck runs three of its four checks out of
# /home/ec2-user/alpha-engine-data (validators.constituents_drift_check,
# validators.phase_marker_sweep, validators.stage_output_sweep) and, until
# this venv existed, ran them under the DASHBOARD interpreter above. The
# constituents drift check therefore never once reached its comparison:
#
#   WARNING [collectors.constituents] Constituents fetch failed
#     (`Import openpyxl` failed...); trying local cache...
#   ERROR   [__main__] Drift check failed at stage=arctic_list:
#     No module named 'arcticdb'
#
# (measured 2026-08-15, execution watch-rerun-2026-08-15-2).
#
# The two dependency sets CANNOT be merged into one venv: the dashboard venv
# is pinned `numpy<2` on the line above because every spot workload's pyarrow
# is compiled against 1.x, and alpha-engine-data declares `numpy>=2.4.6`.
# Installing data's requirements into the dashboard venv would silently break
# the pin that fourteen other stages depend on. Two venvs is the only shape
# that is correct for both.
echo "[weekly-freshness-spot-bootstrap] building alpha-engine-data venv..."
cd /home/ec2-user/alpha-engine-data
# python3.12 literally, for the same reason as the dashboard venv above: an
# interpreter-selection fallback resolves a different wheel set and says
# nothing when it does. The renderer has already installed 3.12 by this point,
# and test_handler.py asserts no such fallback survives in this rendered
# command at all.
python3.12 -m venv .venv || fail "data venv create failed"
.venv/bin/pip install --upgrade pip -q || fail "data pip upgrade failed"
[ -f requirements.txt ] || fail "alpha-engine-data requirements.txt missing"
.venv/bin/pip install -q -r requirements.txt || fail "data requirements install failed"
# Keep the dispatch box on the same released contract as its packaged Lambda.
# A floor here would let a later bootstrap resolve a pre-I8155 stage-coverage
# implementation even though the repository pin has already moved.
.venv/bin/pip install -q 'krepis==0.59.41' || fail "data krepis install failed"
chown -R ec2-user:ec2-user /home/ec2-user/alpha-engine-data/.venv || fail "data venv chown failed"
trap - EXIT
aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true
echo "[weekly-freshness-spot-bootstrap] complete — launcher box ready"
"""
    return prelude + "\n" + render_bootstrap(_bootstrap_spec()) + "\n" + tail


def _launch_instance(
    extra_tags: dict[str, str] | None = None,
    force_on_demand: bool = False,
) -> tuple[str, str]:
    """Launch the launcher box; spot first, on-demand fallback on capacity/
    quota exhaustion via the shared spot_dispatch chokepoint (same posture as
    every other fleet dispatcher — the weekly run must not be starved by a
    capacity dip).

    ``extra_tags`` (config#5695, config#5504): additional instance tags
    threaded through to the SAME RunInstances TagSpecifications as the Name
    tag (atomic with launch — no post-launch create_tags race). Used to
    stamp ``watchdog-deadline`` (the orphan reaper reads the box's own
    deadline rather than the fleet-wide cap) and the per-run identity tags
    (execution_id, run_date, pipeline_role) for EC2 cost attribution."""
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES, SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name="alpha-engine-weekly-freshness-spot",
        extra_tags=extra_tags,
        region=REGION,
        force_on_demand=force_on_demand,
    )


def _wait_ssm_online(instance_id: str) -> None:
    spot_dispatch.wait_ssm_online(
        instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
    )


def _send_bootstrap(instance_id: str, run_token: str) -> str:
    """Fire the async, detached SSM command that clones the 4 repos + builds
    the dashboard venv. Returns the command id for the SF's poll loop."""
    return spot_dispatch.send_async_command(
        instance_id,
        _bootstrap_command(run_token),
        comment=f"weekly-freshness-spot bootstrap ({run_token}) — config#2248",
        region=REGION,
        cw_log_group=CW_LOG_GROUP,
        execution_timeout_seconds=BOOTSTRAP_TIMEOUT_SECONDS,
    )


def _terminate_instance(instance_id: str) -> None:
    """Best-effort terminate of a just-launched box whose post-launch steps
    failed — without this the box orphans (no watchdog/trap armed yet, that
    only happens inside the bootstrap this box never received). Never masks
    the original error (logged, not raised)."""
    spot_dispatch.terminate_on_failure(instance_id, region=REGION, label="weekly-freshness")


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function handler — launch the weekly pipeline's launcher spot box.

    `event` carries `{"force_on_demand": bool}` (defaults False). BOTH SF
    callers set it true today, for two different reasons:
    `RelaunchWeeklyFreshnessSpot` (config-I7119) because spot-first would
    re-enter the pool that just reclaimed the box, and
    `DispatchWeeklyFreshnessSpot` (config-I7120) because this ONE box is the
    shared substrate all 13 stage-liveness gates address via
    `$.ec2_instance_id` and 5 of those sites — the ones inside
    `ResearchPredictorParallel` — have no recovery path at all. The False
    default is retained for operator off-cycle invocations. Returns:

      {"instance_id": "i-...", "command_id": "...", "market": "spot"|"on-demand",
       "run_token": "..."}

    Fail-loud: a launch/SSM error RAISES (no kill-switch skip, no fail-open
    branch) — the SF's own Catch converts it into the same loud
    HandleFailure/SNS path every other Task state in this pipeline uses. The
    weekly run cannot proceed at all without a launcher box, so degrading
    silently here would be strictly worse than halting loudly.
    """
    event = event or {}
    force_on_demand = bool(event.get("force_on_demand", False))
    # Captured at handler ENTRY: the stage-coverage window must predate any
    # write this invocation makes (alpha-engine-config-I7214).
    started = datetime.datetime.now(datetime.timezone.utc)

    # Per-run identity tags (config#5504): attribute the launcher box to the SF
    # execution so per-run EC2 cost is measurable. Gracefully absent for
    # operator off-cycle reruns that bypass the SF.
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
        # No fail-open skip on the SF side for this flag — flipping it off is
        # an explicit "I will pass ec2_instance_id myself" operator action.
        # Raising here (rather than data-spot-dispatcher's silent-skip
        # {"launched": false}) keeps that contract honest: a dispatch that
        # was supposed to happen and didn't must not be indistinguishable
        # from "the operator already supplied an instance id".
        raise RuntimeError(
            "WEEKLY_SPOT_DISPATCH_ENABLED=false but no $.ec2_instance_id was "
            "supplied on the execution input — either re-enable the "
            "dispatcher or pass ec2_instance_id explicitly (see "
            "scripts/weekly_sf_rerun.py / run_weekly_offcycle.sh for the "
            "manual-override shape)"
        )

    run_token = uuid.uuid4().hex
    # ── Watchdog-deadline tag (config#5695) ──────────────────────────────────
    # The orphan reaper uses this per-box deadline when present (instead of the
    # fleet-wide global cap), so the box is never reaped before its own watchdog
    # fires. Sized to WATCHDOG_SECONDS (13h by default, comfortably above the
    # SF's 12h top-level TimeoutSeconds) with GRACE_SECONDS added by the reaper.
    deadline = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=WATCHDOG_SECONDS)
    deadline_tag = {"watchdog-deadline": deadline.strftime("%Y-%m-%dT%H:%M:%S+00:00")}
    try:
        instance_id, market = _launch_instance(extra_tags={**deadline_tag, **(extra_tags or {})}, force_on_demand=force_on_demand)
    except SpotLaunchError:
        logger.error("weekly-freshness-spot launch failed (spot + on-demand exhausted)")
        raise
    logger.info("launched weekly-freshness-spot launcher box %s (%s)", instance_id, market)
    # Once the box is up, ANY failure before the bootstrap command is fired
    # would orphan it (no watchdog/trap yet — that's armed BY the bootstrap
    # this box hasn't received). Terminate-on-error so a slow SSM-online or
    # an SSM SendCommand error tears the box down instead of leaving it idle
    # for the rest of the week.
    try:
        _wait_ssm_online(instance_id)
        command_id = _send_bootstrap(instance_id, run_token)
    except Exception:
        _terminate_instance(instance_id)
        raise
    logger.info(
        "weekly-freshness-spot dispatched: instance=%s market=%s command=%s run_token=%s",
        instance_id, market, command_id, run_token,
    )
    return {
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_token": run_token,
        # The stage name is EXPLICIT, never derived (alpha-engine-config-
        # I10172): this one Lambda backs two SF states, DispatchWeeklyFresh-
        # nessSpot and RelaunchWeeklyFreshnessSpot, and both callers pass
        # `force_on_demand: true` (config-I7119 / config-I7120 — spot-first
        # would re-enter the pool that just reclaimed the box, and this ONE
        # box is the shared substrate every stage-liveness gate addresses).
        # With force_on_demand no longer distinguishing the two callers, the
        # OLD derivation `"RelaunchWeeklyFreshnessSpot" if force_on_demand
        # else "DispatchWeeklyFreshnessSpot"` always resolved to the first
        # branch — DispatchWeeklyFreshnessSpot's own invocations wrote their
        # verdict under RelaunchWeeklyFreshnessSpot's name, and
        # DispatchWeeklyFreshnessSpot had ZERO verdicts in any partition,
        # ever (measured 2026-09-08). Each SF Task now stamps its own
        # identity as a Payload literal (`sf_stage`); the force_on_demand
        # heuristic survives only as the fallback for an operator off-cycle
        # invocation that predates this field.
        "stage_coverage": _assert_stage_coverage(
            str(event.get("sf_stage", "")).strip()
            or ("RelaunchWeeklyFreshnessSpot" if force_on_demand else "DispatchWeeklyFreshnessSpot"),
            started,
            str(event.get("run_date", "")).strip() or None,
        ),
    }


def _assert_stage_coverage(stage: str, started: datetime.datetime, run_date: str | None) -> dict:
    """Record this stage's own output verdict (alpha-engine-config-I7214).

    Both stages this Lambda backs are INFRASTRUCTURE/GATE stages: they
    positively declare in `ARTIFACT_REGISTRY.yaml`'s `pipeline_stages:` that
    they write no durable artifact, so the verdict is `COVERED_NO_OUTPUT`.
    They assert nothing and still RECORD that they declared nothing —
    "declares nothing" and "was never considered" must not be the same
    absence.

    Never alters the handler's outcome. The ImportError branch is loud rather
    than silent because the nousergon-lib pin may predate the module, and an
    inert assertion must be distinguishable from a covered stage.

    ``run_date`` comes from the state's ``Payload.run_date.$: "$.run_date"``
    (alpha-engine-config-I8155 — DispatchWeeklyFreshnessSpot and
    RelaunchWeeklyFreshnessSpot previously passed a narrow Payload with no
    run_date at all, so this verdict has been writing under an empty
    run_date since I7214 shipped). Never fabricated: a missing/blank
    run_date is reported UNMEASURED rather than defaulting to any derived
    date — inventing one here is exactly the defect I8155 fixes.
    """
    if not run_date:
        logger.error("stage-coverage assertion has no run_date for %s — event carried none", stage)
        return {"stage": stage, "status": "UNMEASURED", "reason": "no run_date on state input"}
    try:
        from krepis.stage_coverage import assert_stage_coverage
    except ImportError as exc:
        logger.error("stage-coverage assertion unavailable for %s: %s", stage, exc)
        return {"stage": stage, "status": "UNMEASURED", "reason": str(exc)}
    return assert_stage_coverage(stage, window_start=started, run_date=run_date)
