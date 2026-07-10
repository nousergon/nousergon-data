# ssm-liveness-poller

Liveness-aware poll iteration for `ne-preopen-trading-pipeline`'s SSM
command loops. One invocation = one poll: command status
(`ssm:GetCommandInvocation`) + independent SSM-agent liveness
(`ssm:DescribeInstanceInformation` `PingStatus`) + bounded attempt /
consecutive-ping-miss accounting, returning a single `verdict` the SF
Choice states branch on:

| verdict | meaning | SF routing |
|---|---|---|
| `SUCCESS` | command Status=Success | next pipeline state |
| `IN_PROGRESS` | still running, box responsive, budgets OK | Wait → re-poll |
| `COMMAND_FAILED` | terminal non-success (Failed/TimedOut/Cancelled) | HandleFailure (or fail-soft continue for chronic-gap) |
| `INSTANCE_UNRESPONSIVE` | ≥N consecutive polls with PingStatus≠Online while nominally running | stamp error → ForceStopUnresponsiveInstance → HandleFailure |
| `POLL_BUDGET_EXHAUSTED` | attempt cap hit without terminal status | stamp error → HandleFailure |

## Why (config#1811)

The SSM agent enforces `executionTimeout` from **inside** the box being
watched. 2026-07-06 (config#1807): the trading box wedged under memory
pressure mid-`MorningArcticAppend`; the agent went `ConnectionLost`, the
timeout could not fire, and the SF poll loop read a frozen `InProgress`
for 62 minutes (22 past the timeout) until the agent self-recovered.
This poller detects that shape in ~1 minute (3 misses × 20s), from
outside the box. Also consolidates the previously copy-pasted poll
blocks whose semantics had drifted (only MorningEnrich ever received
the #970 bounded-attempt cap).

Counters are carried through SF state (`$.<step>_poll.attempts` /
`.ping_misses` round-trip through the invoke Parameters) — the Lambda
is stateless.

Read-only by design: force-stop remediation belongs to the state
machine's role, not this function.

## Deploy

```bash
bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --bootstrap  # first time
bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh              # code update
bash infrastructure/lambdas/ssm-liveness-poller/deploy.sh --smoke      # read-only smoke
```

Caller grant: `lambda:InvokeFunction` for
`alpha-engine-step-functions-role` lives in the codified IAM
(`alpha-engine/infrastructure/iam/`), applied via `apply.sh` — not here.
