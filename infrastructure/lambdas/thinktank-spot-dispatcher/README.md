# alpha-engine-thinktank-spot-dispatcher

Runs the daily Think Tank on a self-terminating EC2 spot box instead of a Lambda.

**Why:** alpha-engine-config-I5208 / nous-ergon-ops-I162 / ARCHITECTURE §47. The
daily run hit the 900s Lambda hard ceiling every day from 2026-07-17 and died
mid-loop before its terminal writes — `thinktank/ratings/`,
`thinktank/challenger_selection/`, `thinktank/events/` and the coverage shadow
view all froze for twelve days while the run looked busy in logs.

§47 has required since 2026-06-30 that a long-running agent/batch job runs on
owned compute behind a dispatcher. This is at least the fourth site of the
universe-growth timeout class (config-I3095 logged the evaluator as the third);
the earlier ones were closed with guards rather than the placement fix.

## Sizing is measured, not guessed

| Run | Work done | Wall clock |
|---|---|---|
| 2026-07-16 (pre-#464) | 8 theses + 70-name sweep | **443s** |
| 2026-07-29 (post-#464) | 5 theses, **sweep skipped** | 801s, truncated |

crucible-research#464's pillar/moat call roughly tripled per-thesis wall-clock
(~55s → ~160s). That, not the sweep, crossed the ceiling — the sweep chunks at
`sweep_chunk_size=25`, so 135 covered names is 6 LLM calls costing well under a
cent. Steady state is ~25 min, a shade over 2x the Lambda maximum.

Defaults: **budget 5400s (90 min)**, SSM timeout 7200s, watchdog 9000s.

## The coupling you must not break

```
RUN_BUDGET_SECONDS + 120s reserve  <  RUN_TIMEOUT_SECONDS  <  WATCHDOG_SECONDS
```

The box derives its deadline from the budget; SSM kills the command at the
timeout. If the budget ever meets or exceeds the timeout, SSM guillotines the
run mid-loop and every terminal write is lost again. `handler()` refuses to
launch in that state and `test_handler.py` asserts it.

**If runs start truncating, raise the timeout first, then the budget** —
never the budget alone. Re-derive from `thinktank/runs/{date}/manifest_*.json`
rather than guessing; `deadline_skipped_sweep` / `deadline_skipped_new` /
`deadline_skipped_refresh` tell you exactly what did not fit.

## Rollout order (staged — §47 sub-rule (b))

Merging this PR has **zero live effect**. The Think Tank keeps running on
Lambda until step 3.

1. **crucible-research-PR544 must merge first.** The SSM prelude execs
   `infrastructure/thinktank_spot_bootstrap.sh` from a shallow clone of
   `main` — the script has to exist there before any box can run.
2. `./deploy.sh --bootstrap` — creates the Lambda + IAM role. Still nothing
   scheduled against it.
3. `./deploy.sh --smoke` — **fires a REAL run on a REAL spot box.** This is the
   validation gate, not a formality: §47 sub-rule (b) exists because stock
   AMIs ship no git, SSM shells run as root with no `$HOME`, and a `$`-bearing
   string expands as positional params under `set -u`. Three rounds of exactly
   these were found on the 2026-06-30 groom cutover only by launching a box and
   reading its output. Confirm the run wrote `thinktank/challenger_selection/`,
   `thinktank/ratings/` **and** `thinktank/events/` for the trading day, and
   that the box terminated itself.
4. `./deploy.sh --cutover` — repoints `alpha-research-thinktank-daily` from
   `alpha-engine-research-thinktank:live` to this dispatcher. Deliberately a
   separate flag so no merge and no code deploy can repoint the live schedule.

Roll back by re-running `put-targets` against the Lambda alias; the Lambda
itself is left deployed and functional throughout.

## Alarm rotation (handled by `--cutover`)

`alpha-engine-thinktank-daily-run-failed` and its `-timeout` sibling watch the
**old Lambda's** metrics. The instant the rule stops targeting that function
they stop seeing invocations — and because both were created with
`--treat-missing-data notBreaching`, zero invocations evaluates to **OK**. They
would go green *because nothing ran*, which is precisely the silence class
config-I5208 is about.

So `--cutover` rotates them atomically with the repoint:

| Signal | Covers | Where |
|---|---|---|
| `alpha-engine-thinktank-spot-dispatch-failed` | **launch** — Errors >= 3/day means the invoke plus both async retries all raised, so no box exists | armed by `--cutover` |
| `thinktank_challenger_selection` | **end-to-end** — the artifact itself going stale, i.e. a box that booted and produced nothing | ARTIFACT_REGISTRY, already live |

Both are required. The dispatcher alarm cannot see a box that boots and then
fails its run; the freshness row cannot distinguish "never launched" from
"launched and failed". The two old alarms are deleted, not left green.
