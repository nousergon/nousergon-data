# substrate-health-gate

Fast pre-dispatch substrate check for the weekly SF's `MorningEnrich` stage.
One invocation = the whole gate: it issues its own tiny SSM `df` probe
command against the dispatch box and polls it to a terminal status inside
this single Lambda invocation (bounded by `_POLL_BUDGET_SECONDS`, well under
the Lambda's own configured timeout), returning a single `verdict` the SF
Choice state branches on:

| verdict | reason | meaning | SF routing |
|---|---|---|---|
| `HEALTHY` | — | disk headroom OK, SSM agent responsive | continues into `MorningEnrich` |
| `SUBSTRATE_UNHEALTHY` | `disk_full` | df probe ran (agent alive) but used% ≥ `DISK_WARN_PERCENT` (90) | short-circuits to a named-failure notify, does NOT enter MorningEnrich's own retry ladder |
| `SUBSTRATE_UNHEALTHY` | `ssm_unresponsive` | probe registered but never reached Success within the poll budget | same |
| `SUBSTRATE_UNHEALTHY` | `ssm_command_never_registered` | SSM never confirms the invocation exists at all (agent unreachable) | same |

## Why (config#2249)

`MorningEnrich` currently discovers a dead dispatch box (disk 100% full, or
the SSM agent wedged/unresponsive so a command silently never registers)
only after burning its full "gold 4+2" retry ladder (config#2279) — up to
~15 minutes — before failing into the generic `PipelineFailure` path with no
signal distinguishing "the box is dead" from an ordinary transient
SendCommand hiccup.

This Lambda runs as a NEW Task state immediately BEFORE `MorningEnrich`,
failing loud and fast (<2 min) with a distinctly-named
`SubstrateUnhealthy: <reason>` verdict when the box can't take work — a
dedicated pre-check, not a replacement for `MorningEnrich`'s own resilience
against real transient issues once dispatch is confirmed healthy.

~10-20s of added latency on every Saturday run (the probe's own round-trip)
is accepted — the Saturday run has slack, unlike the tight weekday pre-open
buffer (this Lambda is NOT wired into the weekday SF).

Fail-loud by design: any unexpected AWS error (AccessDenied, a malformed
API response) raises rather than being folded into a verdict — only the
three named `SUBSTRATE_UNHEALTHY` reasons above are "this box is bad"
outcomes; everything else is an ordinary infra failure the SF's own Catch
already handles.

## Registry note (config#2480 invariant)

The new SF Task state this Lambda backs (`SubstrateHealthGate` in
`infrastructure/step_function.json`) needs a paired entry in
`nousergon_lib.pipeline_status.registry.STATE_TO_ARCHIVE_PAGE` per the
source-side drift check added for config#2480 — see this PR's description
for the companion nousergon-lib dependency.

## Deploy

```bash
bash infrastructure/lambdas/substrate-health-gate/deploy.sh --bootstrap  # first time
bash infrastructure/lambdas/substrate-health-gate/deploy.sh              # code update
bash infrastructure/lambdas/substrate-health-gate/deploy.sh --smoke      # LIVE smoke — issues a real df probe command
```

Caller grant: `lambda:InvokeFunction` for `alpha-engine-step-functions-role`
lives in the codified IAM (`infrastructure/iam/`), applied via `apply.sh` —
not here.
