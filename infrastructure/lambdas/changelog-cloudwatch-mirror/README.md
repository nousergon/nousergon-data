# changelog-cloudwatch-mirror Lambda

Closes the **Lambda errors directly to S3** half of the system-wide event-mining coverage matrix (ROADMAP > Observability > line ~2121).

## What it does

CloudWatch Logs subscription filters on every alpha-engine Lambda match `ERROR / CRITICAL / "Task timed out"` patterns and deliver matched log events to this relay Lambda. The relay decodes the gzipped subscription-filter event payload, parses each `logEvent`, and writes one structured incident entry per event to:

```
s3://alpha-engine-research/changelog/entries/{YYYY-MM-DD}/{event_id}.json
```

Schema 1.0.0 per `alpha-engine-config/changelog/vocab.yaml`. Same shape as the SNS-mirror Lambda's entries; downstream aggregator + retro-candidate filter consume both transparently.

## Why a separate Lambda from the SNS-mirror

- SNS-mirror covers events that land on the `alpha-engine-alerts` topic (Step Function failures, EOD failures, CloudWatch alarm state-changes wired to SNS).
- This Lambda covers events that land **only in CloudWatch Logs** — Lambda crashes (cold-start failures, OOM, timeouts, Python exceptions) are the canonical example. These never reach SNS by default.
- Decoder shape differs (gzipped + base64 logs payload vs. plain SNS message), so split handlers keep both clean.

## Per-Lambda inferred subsystem

Log-group prefix → vocab.yaml subsystem (matching order; first-match wins):

| Log group prefix | subsystem |
|---|---|
| `/aws/lambda/alpha-engine-predictor*` | `predictor` |
| `/aws/lambda/alpha-engine-research*` | `research` |
| `/aws/lambda/alpha-engine-data*` | `data_pipeline` |
| `/aws/lambda/alpha-engine-replay*` | `eval` |
| (default) | `infrastructure` |

## Defaults applied to auto-emitted entries

| Field | Value |
|---|---|
| `severity` | `high` |
| `subsystem` | inferred (see above) |
| `root_cause_category` | `infrastructure_failure` |
| `auto_emitted` | `true` |
| `source` | `cloudwatch-mirror` |
| `actor` | source Lambda's function name |
| `machine` | `lambda:changelog-cloudwatch-mirror` |

Operator can refine via a follow-up `changelog-log --event-type investigation` entry whose `git_refs` reference the original event_id.

## Subscription targets (11 Lambdas)

The deploy script wires subscription filters to every alpha-engine Lambda **except** the two changelog-mirror Lambdas (recursion guard — if this Lambda errors, its log lines must not feed back into itself).

```
alpha-engine-data-collector
alpha-engine-ec2-lifecycle
alpha-engine-predictor-health-check
alpha-engine-predictor-inference
alpha-engine-replay-concordance
alpha-engine-replay-counterfactual
alpha-engine-research-alerts
alpha-engine-research-eval-judge
alpha-engine-research-eval-rolling-mean
alpha-engine-research-rationale-clustering
alpha-engine-research-runner
```

## Recursion guard

Two layers:
1. **Target list excludes self.** The deploy script's `TARGET_FUNCTIONS` array does not include `alpha-engine-changelog-cloudwatch-mirror` or `alpha-engine-changelog-incident-mirror`.
2. **Pattern is narrow.** Filter pattern `?ERROR ?CRITICAL ?"Task timed out"` does not match this Lambda's normal log lines (`Wrote structured=...`).

If this Lambda's own errors need to be captured, write a separate downstream surface — do not subscribe a Lambda to its own log group.

## Operator deploy steps

**First time (creates IAM role + Lambda + all 11 subscription filters):**
```
bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --bootstrap
bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --smoke
```

**Code update (after PR merge):**
```
bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh
```

**Re-apply subscription filters only (e.g., after adding a new target Lambda):**
```
bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --wire-subs
```

**Dry-run any action:**
```
bash infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh --dry-run --bootstrap
```

## Smoke test verification

After `--smoke` runs (synthetic ERROR delivered via direct Lambda invoke), check:

```
aws s3 ls s3://alpha-engine-research/changelog/entries/$(date -u +%Y-%m-%d)/ --recursive | grep cloudwatch-mirror
```

Expect one entry with `source: cloudwatch-mirror` and `actor: alpha-engine-predictor-inference` (the synthetic payload's source).

## "Done" signal

Every observable Lambda failure mode in the system surfaces as an entry in `s3://alpha-engine-research/changelog/entries/`, regardless of whether it originated as an SNS alert, flow-doctor diagnose call, or Lambda crash. Closes the original ROADMAP P1 line ~2099 entry end-to-end (Gap 1 closed by flow-doctor 0.4.0 S3Notifier 2026-05-01; Gap 2 closes here).

## Cost

11 subscription filters × paper-trading error frequency (~few/week) ≈ negligible (<$0.01/month). Each filter invocation transfers <1KB to the relay Lambda, which executes in <100ms.

## Tests

```
python3 infrastructure/lambdas/changelog-cloudwatch-mirror/test_handler.py
```

14 tests cover: subscription event decode, per-logEvent fan-out, structured payload shape, subsystem inference (predictor / research / data_pipeline / eval / default), event_id format, event_id distinctness for same-second errors, control-message no-op, empty-events no-op, empty-message skip, missing-timestamp fallback.
