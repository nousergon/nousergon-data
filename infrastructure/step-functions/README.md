# SF LoggingConfiguration drift guard (alpha-engine-config#1464)

`check-drift.py` diffs each orchestrated Step Function's live
`loggingConfiguration` against what's codified. This is the CI backstop
for a specific, easy-to-miss failure mode: `LoggingConfiguration` survives
a plain `update-state-machine --definition ...` call (that's a partial
update), but is **dropped whenever the state machine is recreated** rather
than updated — e.g. a CloudFormation replacement triggered by a
`StateMachineName` change. The 2026-06-29 `ne-*` rename (config#1381) did
exactly that to the two CFN-managed SFs, silently breaking the L274
MutexConflict CloudWatch metric-filter chain (config#729) until a later PR
noticed and restored it.

## Scope + source of truth

Source of truth is split across two files (this script parses both, no
duplicated table to keep in sync):

| State machine | Source of truth | Expected |
|---|---|---|
| `ne-weekly-freshness-pipeline` | `infrastructure/cloudformation/alpha-engine-orchestration.yaml` (`SaturdayPipeline`) | `level=ERROR`, `includeExecutionData=true` |
| `ne-preopen-trading-pipeline` | same file (`WeekdayPipeline`) | `level=ERROR`, `includeExecutionData=true` |
| `ne-postclose-trading-pipeline` | `infrastructure/deploy-infrastructure.sh` (`EOD_LOGGING_CONFIG`) | `level=ERROR`, `includeExecutionData=true` |
| `alpha-engine-groom-dispatch` | same file (`update_or_create` call omits the logging arg, deliberately) | no logging (`level=OFF`) |

The CFN template isn't parsed with a real YAML library — `!Ref`/`!GetAtt`/
`!Sub` intrinsics aren't valid plain YAML, and this repo's existing test
suite already made the same call (see
`tests/test_deploy_step_function_eventbridge_input.py`: "CFN's intrinsic
tags require a custom loader" → slice the text instead).

## Usage

```bash
# Check every codified state machine
./infrastructure/step-functions/check-drift.py

# Check one
./infrastructure/step-functions/check-drift.py --name ne-weekly-freshness-pipeline
```

Requires AWS creds with `states:DescribeStateMachine` on the target state
machines.

## CI wiring

Wired via `.github/workflows/sf-arn-drift-check.yml` (PR-triggered on the
paths above, daily 09:30 UTC, `workflow_dispatch`), alongside the
EventBridge guard — see `infrastructure/eventbridge/README.md`'s "CI
wiring" note. Requires `states:DescribeStateMachine` /
`states:ListStateMachines` on the `github-actions-iam-drift-check` OIDC
role's policy (codified in `crucible-executor`) — until that policy is
applied live (operator-run `apply.sh`, not automated per that repo's IAM
doctrine), this workflow's steps will fail on AccessDenied rather than
report real drift.

# SF lambda:invoke Lambda-existence guard (alpha-engine-config#1464)

`check-lambda-existence.py` walks every codified SF definition for
`lambda:invoke` states and verifies each referenced `FunctionName` exists
live on AWS. This is the CI backstop for the 2026-07-08 EOD incident class:
config#1767 Phase 2 (nousergon-data#643) wired `step_function_eod.json` to
invoke `alpha-engine-data-spot-dispatcher` before that Lambda had ever been
deployed. The IAM grant reached live AWS late (config#1446); once that was
fixed, the SF step still 404'd (`ResourceNotFoundException`) because the
function itself didn't exist — fail-open at that step masked it until it
surfaced two hops downstream as a hard `EODReconcile` failure.

## Scope + source of truth

Same four codified definition files as `check-definition-drift.py`'s
`SF_DEFINITIONS` map — `step_function.json`, `step_function_daily.json`,
`step_function_eod.json`, `step_function_groom.json`. Every `Task` state
whose `Resource` targets `arn:aws:states:::lambda:invoke` (including
`.waitForTaskToken`/`.sync` variants), at any nesting depth (`Map`
`Iterator`/`ItemProcessor`, `Parallel` `Branches`), is discovered
automatically — no hand-maintained function registry to keep in sync.

## Usage

```bash
# Check every codified state machine
./infrastructure/step-functions/check-lambda-existence.py

# Check one
./infrastructure/step-functions/check-lambda-existence.py --name ne-postclose-trading-pipeline
```

Requires AWS creds with `lambda:GetFunction` on the referenced functions.

## CI wiring

Wired via `.github/workflows/sf-arn-drift-check.yml` alongside the other two
guards in this directory. Requires `lambda:GetFunction` on the
`github-actions-iam-drift-check` OIDC role's policy (codified in
`crucible-executor`) — until applied live, this step fails on AccessDenied
rather than reporting real drift.
