# alpha-engine-weekly-run-scope

Derives the weekly pipeline's **own scope** — which stages this run dispatched,
which an operator flag switched off — and writes it to
`s3://alpha-engine-research/backtest/{run_date}/run_scope.json`.

Tracked as `alpha-engine-config-I7620`.

## The problem it closes

Which stages actually ran has never been recorded anywhere a consumer could
read. The Director grades the week's numbers without knowing which producers
were disabled, so a stage that was deliberately switched off is
indistinguishable, on the rendered page, from a stage that ran and failed.

Measured, 2026-08-14: `skip_parity: true` had been set on the Saturday
EventBridge target since 2026-08-13 by a recorded ruling. The Director reported
the resulting absence as

> contamination attestation absent at `s3://alpha-engine-research/backtest/2026-08-14/pit_parity.json` — the producer never ran this cycle

and withheld `issue_filing` and `loop_verification` for the cycle. The producer
did not "never run"; it was turned off on purpose, and nothing on the surface
could say so.

## Why it derives instead of reading a registry

`alpha-engine-config/private-docs/` already carries thirteen registries
(`ARTIFACT_REGISTRY`, `PIPELINE_CONTRACT`, `OBSERVATION_REGISTRY`,
`KILL_SWITCH_REGISTRY`, …). Every one exists because its fact had no
machine-readable home.

This fact has two, and both are authoritative:

| Source | Answers |
|---|---|
| `states:DescribeStateMachine` | which stages exist, and which `skip_*` flag gates each |
| `states:GetExecutionHistory` | which branch every gate actually took |

A fourteenth YAML listing enabled stages would be a **copy** — and it would
drift the first time somebody adds a stage and forgets, which is the failure
mode the other thirteen were built to prevent. Deriving means one flag flip in
the CFN preset changes the pipeline, this artifact, and the Director's purview
together, because all three read the same two sources.

## The vocabulary

Four dispositions, closed. Three is the number an operator thinks in; the
fourth exists because a run that dies at stage 3 leaves stages 4..40 in a state
that is neither *disabled* nor *failed*.

| Disposition | Meaning | Graded? |
|---|---|---|
| `DISABLED` | Its gate was entered and took the skip branch, or a parent gate whose enabled branch is the only way in did (`source: parent_gate`). `disabled_by` names the flag. | No — a decision |
| `ENABLED_COMPLETED` | Dispatched, entered, exited cleanly | Yes |
| `ENABLED_FAILED` | Dispatched and entered, never exited cleanly — including a raise that a `Catch` routed on (`caught_error`) | **Yes, as a failure** |
| `NOT_REACHED` | The gate was never entered — the run ended upstream | No — an absence of evidence |

Gated stages that run after `RunScope` itself (`ReportCard`, `Director`,
`ScannerLeaderboard`, `AggregateCosts`) are not rows: the history cannot hold
them yet. They are listed under `after_scope` and named in the statement
(alpha-engine-config-I11502).

`ENABLED_FAILED` is why the whole module is written against **dispatch** rather
than **success**. If grading followed what succeeded, a stage could silently
disable itself by crashing.

`DISABLED` and `NOT_REACHED` are both excluded from grading, for opposite
reasons, and are never merged.

## What the derivation deliberately does not do

Three plausible approaches were tried against the live definition and the two
captured executions in `fixtures/`, and each produced a confident, wrong answer:

- **Sequence adjacency** — "the state entered after the Choice". Six gates live
  inside `ResearchPredictorParallel`, whose events interleave across concurrent
  branches, so adjacency read `CheckSkipScanner` as followed by a state in a
  different branch and degraded six stages to `NOT_REACHED`. Replaced by the
  history's own `previousEventId` chain.
- **Reachability** from a gate's enabled branch, to attribute nested stages. The
  machine has retry loops (`MorningEnrichReissue` → `MorningEnrich`, the poll
  waits), so "reachable from the evaluator branch" measured 132 states,
  including states that run *before* it.
- **Dominance**, to fix reachability. `RouteAfterBootstrapSuccess` is a shared
  spot-relaunch hub with an edge back into the middle of several stage branches,
  so almost nothing in this machine is strictly dominated by its own gate.

What survives is a **bounded local walk** (≤6 hops) from a gate to the work
state behind it, plus a nested-gate check — neither needs a global graph
property to be true. Cross-branch blame was dropped entirely: a `NOT_REACHED`
row reports the run's own input flag as an explanation, which is a fact rather
than an inference, because a wrong parent flag is worse than none — the flag it
names is not the flag to flip.

## Failure posture

Fail-open, and only here. A scope block that could not be built returns every
stage as `NOT_REACHED`, sets `degraded: true`, and states
`SCOPE UNAVAILABLE`. That is safe **because the degraded block grades nothing** —
the consumer's denominator collapses to zero and the card says so out loud. The
one thing this must never do is emit a scope that looks complete.

The SF state's `Catch` rejoins the tail: an advisory artifact must not kill a
run that produced real trading artifacts.

## Wiring

- SF state `RunScope`, immediately before `CheckSkipReportCard` — both routes
  into the post-eval tail pass through it. Pinned by
  `tests/test_run_scope_wiring.py`.
- Consumer reads the **S3 artifact**, not the SF payload, so the two sides are
  not coupled through payload shape.
- IAM: `states:DescribeStateMachine` + `states:GetExecutionHistory` scoped to
  this state machine, and `s3:PutObject` scoped to
  `backtest/*/run_scope.json`.

## Deploy

```
bash infrastructure/lambdas/weekly-run-scope/deploy.sh --bootstrap   # first time
bash infrastructure/lambdas/weekly-run-scope/deploy.sh               # code update
bash infrastructure/lambdas/weekly-run-scope/deploy.sh --apply-iam   # policy only
```

## Tests

`test_handler.py` — named for the only filename either gate looks for
(`ci.yml` globs `infrastructure/lambdas/*/test_handler.py`; the shared runner
returns 0 for a lambda without one). Runs against two verbatim captured
executions in `fixtures/`:

- `history_all_skip_shell.json` — `watch-rerun-2026-08-16-4`, the execution that
  terminated **SUCCEEDED** carrying 22 `skip_*` flags. Scope says 3 of 29.
- `history_real_run_failed.json` — `watch-rerun-2026-08-15-1`, the last run that
  did real work, with `skip_parity` set.
