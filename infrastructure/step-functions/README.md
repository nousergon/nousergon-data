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
| `alpha-engine-groom-pipeline` | same file (`update_or_create` call omits the logging arg, deliberately) | no logging (`level=OFF`) |

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

Not yet wired into a workflow — see
`infrastructure/eventbridge/README.md`'s "CI wiring" note; the same
`sf-drift-check.yml` draft (in the PR description for config#1464) runs
both this and the EventBridge guard. Also needs
`states:DescribeStateMachine` added to the `github-actions-iam-drift-check`
OIDC role's policy (codified in `crucible-executor`).
