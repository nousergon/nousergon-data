# sf-watch-market-hours-toggler

Structural (IAM-level) market-hours enforcement for
`alpha-engine-sf-watch-executor-role`'s trading-pipeline `StartExecution`
grant. Spec: [nousergon/alpha-engine-config#2932](https://github.com/nousergon/alpha-engine-config/issues/2932).

## What it does

Every 5 minutes (`rate(5 minutes)` EventBridge rule — polling live state
instead of a fixed UTC cron avoids DST drift), reads
`alpha-engine-sf-watch-executor-role`'s current `sf-watch-executor-least-priv`
inline policy and compares it to whichever of two codified variants
matches NYSE market hours right now (weekday, not a holiday, 9:30–16:00 ET):

- **Market open** → the restrictive variant
  (`sf-watch-executor-role-policy-market-hours.json`): `RerunFleetSFFromFailedStep`
  drops `ne-preopen-trading-pipeline` / `ne-postclose-trading-pipeline` —
  only `ne-weekly-freshness-pipeline` + the legacy `alpha-engine-eod-pipeline`
  alias remain rerunnable.
- **Market closed** → the permissive variant
  (`sf-watch-executor-role-policy.json`): all four pipelines rerunnable,
  matching the role's original grant.

If the live policy already matches the desired variant, it's a no-op (a
single `iam:GetRolePolicy` call, no write). This closes the gap
config#2903 found: the "sf-watch never touches the trading box during
market hours" charter rule (`.github/sf-watch-prompt.md`) was prompt-text
enforcement only, with no IAM boundary.

## Why a Lambda, not a literal scheduled `apply.sh` run (config#2932)

Brian's ruling (Option E, 2026-07-20) authorized closing this gap by
scheduling the SAME codified writer (`alpha-engine-config/infrastructure/
iam/apply.sh`) rather than adding an independent second writer — preserving
the fleet's one-writer-per-codified-role posture (the class of bug
`crucible-executor/infrastructure/iam/check-no-foreign-writers.py` exists
to catch: four prior incidents were two DIFFERENT pieces of logic racing
to decide a role's policy content). `apply.sh` is a bash script that shells
out to the AWS CLI; standard Lambda runtimes ship no AWS CLI binary, so it
cannot run inside a Lambda unmodified. This handler reimplements
`apply.sh`'s one operation (`put-role-policy` with a checked-in JSON
document) in boto3, but — critically — never decides policy CONTENT
itself: both variant documents are `alpha-engine-config`'s files, copied
verbatim by `deploy.sh` at packaging time (see below). The only decision
this code makes is WHICH already-codified variant should be live right
now, from a single, shared `is_market_hours()`-equivalent implementation
(the same 9:30–16:00 ET / weekday / NYSE-holiday logic as
`crucible-executor/executor/market_hours.py::is_market_hours()` — see
`index.py`'s module docstring for the duplication caveat, since no
cross-repo shared-lib home for this table exists yet).

## Deploy

```sh
# From a checkout of nousergon-data, with a sibling alpha-engine-config checkout:
bash infrastructure/lambdas/sf-watch-market-hours-toggler/deploy.sh \
    --iam-repo ../alpha-engine-config --bootstrap

# Confirm it behaves correctly against the LIVE role before trusting the
# unattended schedule:
bash infrastructure/lambdas/sf-watch-market-hours-toggler/deploy.sh \
    --iam-repo ../alpha-engine-config --smoke
```

`--iam-repo <path>` is required on every real deploy — it's where the two
policy JSON files are sourced from (`alpha-engine-config/infrastructure/iam/
sf-watch-executor-role-policy{,-market-hours}.json`). The copies committed
in this directory are fixtures for `test_handler.py` / CI only;
`deploy.sh` always overwrites them from `--iam-repo` before packaging, so a
deploy can never ship policy content that's silently out of sync with
`alpha-engine-config`'s source of truth.

**First deploy has zero live effect until `--bootstrap` is run** — see
`alpha-engine-config/infrastructure/iam/README.md`'s `sf-watch-executor-role`
section for the manual `apply.sh` command that applies the restrictive
variant directly, if you want the structural fix live before wiring the
schedule.

## Blast radius

The toggler's own execution role (`alpha-engine-sf-watch-market-hours-
toggler-role`, `iam-policy.json`) is scoped to exactly
`iam:GetRolePolicy` + `iam:PutRolePolicy` on
`alpha-engine-sf-watch-executor-role`'s ARN — nothing else, no other role,
no other IAM action. It cannot touch its own permissions, any other
codified role, or any AWS service outside CloudWatch Logs for its own
function.
