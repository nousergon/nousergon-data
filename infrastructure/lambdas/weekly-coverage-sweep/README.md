# alpha-engine-weekly-coverage-sweep

The caller the stage-coverage sweep never had (`alpha-engine-config-I8214`,
carrying `I8154` deliverable 4 and `I8186`'s state-machine half).

## What it does

Runs `nousergon_lib.pipeline_status`'s stage-coverage sweep over one weekly
**cycle**, then:

1. writes the sweep artifact to `_stage_coverage/_sweep/<pipeline>/<run_date>.json`
   and publishes its metrics, plus one `AlphaEngine/StageCoverageSweepAlarmable`
   point (`Pipeline` dimension) = findings + absent (only when coverage is
   established) + 1 if deferred, zero included. It is the only series
   `alpha-engine-stage-coverage-findings` alarms on: the former `-absent` and
   `-deferred` alarms were folded into it on 2026-10-09
   (`alpha-engine-config-I11792`). A failed put is logged at ERROR and never
   changes the outcome,
2. **augments the SF completion marker** at
   `_sf_completion/<pipeline>/<run_date>.json` with the cycle's real shape —
   which executions contributed, what each entered, what the union adds up to,
3. pages (via `krepis.alerts`, deduped per pipeline + run date) when the sweep
   finds a gap.

## Why it exists

The reader shipped in `nousergon-lib` and nothing called it, so it detected
nothing. Meanwhile the completion marker's name was a claim the run could not
support: on 2026-08-22 the marker was written by `watch-rerun-2026-08-22-3`,
an execution that entered **1 of 16** declared spine stages. The SF now stamps
`claim: sf_execution_terminal` and `cycle_verdict: unknown` on the object —
what its own write actually asserts — and this handler is what upgrades that
to a cycle verdict.

## The three outcomes, and the one that matters

| `outcome` | Meaning | SF terminal |
|---|---|---|
| `clean` | the sweep ran, no gap | `WeeklyCoverageSweepClean` |
| `findings` | the sweep ran, found a gap, **already paged** | `WeeklyCoverageSweepFindings` |
| `unavailable` | **the sweep did not run**, or ran and could not publish | `WeeklyCoverageSweepUnavailable` → SNS → `WeeklyCoverageSweepUnobserved` |

`unavailable` is never collapsed into either of the others. "Found nothing"
and "did not run" are different facts, and only the second means the coverage
surface is unobserved (`principles.md` §2.7). The state machine pages for it
because a handler that never started cannot page for itself.

The handler **never raises**. It runs downstream of the pipeline's real success
terminal, and an observe-only tail that fails a completed run is a worse defect
than the one it was added to detect (`sf-pipeline-policy.md` §2.1).

## Why a Lambda and not an SSM command

This pipeline has no always-on box. `$.ec2_instance_id` is an ephemeral spot,
and since `alpha-engine-config-I8162` a recovery run whose every box stage is
skipped carries **no instance id at all** — which is precisely the run that
most needs a coverage verdict. A sweep dereferencing it would throw
`States.Runtime` one state after the pipeline's own success terminal. Mirrors
`weekly-run-scope`, the sibling Lambda reading the same execution history for
the same pipeline.

## Deploy

```
bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh --bootstrap  # first time
bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh              # code only
bash infrastructure/lambdas/weekly-coverage-sweep/deploy.sh --apply-iam  # IAM only
```

Managed outside CloudFormation, same rationale as `weekly-run-scope`.

## Rehearsal path

`infrastructure/lambdas/weekly-coverage-sweep/test_handler.py` — run pre-merge
by `.github/workflows/ci.yml`'s `infrastructure/lambdas/*/test_handler.py` glob
and pre-deploy by `_shared/run_handler_tests.sh`, so neither gate can drift from
the other.

Beyond the unit tests, the Friday-PM shell run is a live rehearsal: the SF
threads `dry_run.$ = $.research_dry`, so the whole read path — client
construction, every IAM grant the real run needs, the coverage derivation —
executes against real infrastructure and writes nothing.
