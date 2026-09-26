# alpha-engine-collection-readiness-probe

The v1 consumers' readiness question over the standalone collector's run manifests
(**alpha-engine-config-I11264** deliverable 1, built inside the decoupled cutover,
**alpha-engine-config-I11269**).

## Why

Under the decoupled cutover (Brian's 2026-09-21 ruling (b)) the three v1 state
machines keep running with their inline data stages removed, and the standalone
`nousergon-data-collection` schedules produce the data. Every surviving v1 consumer
would otherwise run AHEAD of its producer (plan §6.2b). Each v1 definition now
reaches its first data consumer only through `WaitForCollectionManifests`, a bounded
poll of this function.

## What it does

`{"collection", "units": [...], "not_before": $$.Execution.StartTime, "lookback_seconds"}`
→ `{"readiness": {"ready", "settled", "missing", "failed", "failure_mode", "baseline", "summary"}}`.

It runs `data_gate/run_manifest_predicate.py::readiness_check` — the SAME `_check_unit`
the producer's `VerifyRunManifests` runs through the data-spot dispatcher's
`completion-check`. `ready` = no finding; `settled` = every unit has a manifest for
this cycle, so waiting longer cannot change the answer.

## Why a separate function

The data-spot dispatcher launches collector boxes. A v1 state machine allowed to
invoke it is one Payload edit away from a second writer of `market_data/*`
(**alpha-engine-config-I11266** deliverable 6). This function's role can list and
read `data_collection/runs/*` and nothing else, and `tests/test_v1_collection_readiness_wait.py`
asserts no v1 definition invokes the dispatcher.

## Deploy

```bash
# first time, operator with IAM rights, BEFORE the v1 definitions that call it deploy:
AWS_PROFILE=ne-admin bash infrastructure/lambdas/collection-readiness-probe/deploy.sh --bootstrap
# afterwards: code-only, automatic on merge (.github/workflows/deploy-collection-readiness-probe.yml)
```
