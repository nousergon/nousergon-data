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

Defaults: **budget 5400s (90 min)**, job-unit timeout 7200s, watchdog 9000s.

## The coupling you must not break

```
RUN_BUDGET_SECONDS + 120s reserve  <  RUN_TIMEOUT_SECONDS  <  WATCHDOG_SECONDS
```

The box derives its deadline from the budget; systemd kills the job unit at the
timeout (`TimeoutStartSec`). If the budget ever meets or exceeds the timeout,
the run is killed mid-loop and every terminal write is lost again. `handler()` refuses to
launch in that state and `test_handler.py` asserts it.

**If runs start truncating, raise the timeout first, then the budget** —
never the budget alone. Re-derive from `thinktank/runs/{date}/manifest_*.json`
rather than guessing; `deadline_skipped_sweep` / `deadline_skipped_new` /
`deadline_skipped_refresh` tell you exactly what did not fit.

## The box carries its own job (alpha-engine-config-I11597)

On 2026-09-23 (alpha-engine-config-I11532) this dispatcher still launched a box,
waited for its SSM agent to come Online, then sent the job with `send-command`.
SSM registered slowly, Lambda killed the handler between the launch and the
send, and EventBridge's async retry found the box running and skipped it. The
box ran nothing and the day was lost. nousergon-data#1957 was the stopgap: a
longer timeout, dispatch-record tags and a rule for adopting a half-dispatched
box on retry.

The dispatch is now **one step**. `krepis.ec2_spot.launch_self_starting` makes
one `RunInstances` whose user-data
(`krepis.spot_bootstrap.render_self_starting_user_data`) installs `_job_script`
as the oneshot unit `alpha-engine-thinktank-run` and starts it at boot. The
unit's `TimeoutStartSec` is `RUN_TIMEOUT_SECONDS`, and its `ExecStopPost`
powers the box off. The Lambda returns in seconds. It has no SSM wait, no send,
no half-done state and no adoption logic.

- **Replay-safe.** Every `RunInstances` attempt carries a `ClientToken` derived
  from the invocation's request id. Before launching, krepis looks up every
  token that request id could have produced. EventBridge's async retries of one
  event reuse its request id, so a retry gets back the box its predecessor
  launched. It gets that box whether it sees it by Name tag (`already_running`),
  by client token (`replayed: true`), or only through `RunInstances`' own token
  replay. The run token is derived from the request id too, so the replay's tags
  and user-data match the original's byte for byte. EC2 only hands the instance
  back when they do.
- **No secrets in user-data.** `DescribeInstanceAttribute` exposes it. It carries
  the run token, SSM parameter names and public URLs. The box resolves every
  secret from SSM, as it did under the SSM command. `test_handler.py` asserts
  this, and that the user-data stays well under EC2's 16 KB limit.
- **Launch record (alpha-engine-config-I5752).** With no SSM command there is
  no `command_id` to reconcile against. So the box writes
  `s3://alpha-engine-research/thinktank/_control/launched/{trading_day}-{run_token}.json`
  at boot, before the job starts. The record (`krepis_self_start_launch.v1`)
  holds `run_token`, `trading_day`, `budget_seconds`, `timeout_seconds`,
  `completion_marker`, `instance_id`, `booted_at` and `deadline_at`. It sits
  beside the completion marker under the same key. A launch record with no
  marker after `deadline_at` means the run failed or wedged. The box writes the
  record because its role already writes this bucket, while this Lambda's role
  has no S3 grant. A box that never boots writes nothing. For that case the
  record is the Lambda's own return value (`launch_record`, `run_token`,
  `instance_id`) and the instance's tags.
- **Liveness backstop is unchanged.** It is still the success-path completion
  marker, spot-orphan-reaper's Think-Tank WatchKind, and the
  `thinktank_challenger_selection` freshness row.
- **Logs.** The job's output goes to the journal and the EC2 console
  (`aws ec2 get-console-output`). The bootstrap log ships to
  `s3://alpha-engine-research/_ssm_logs/thinktank-spot/<date>/` on exit. The
  `/alpha-engine/thinktank-spot` CloudWatch group was fed by SSM and receives
  nothing new.
- **Timeout.** `FN_TIMEOUT=900` in `deploy.sh` stays, converged on every deploy.
  It is now headroom for a worst-case capacity rotation, not a budget any step
  is sized against.
- **IAM.** No new grant is needed. `ec2:RunInstances` covers `UserData` and
  `ClientToken`, and `ec2:DescribeInstances` covers the client-token lookup. The
  role's SSM and `TerminateInstances` statements are now unused. They are left
  in place, and narrowing them is a separate change.

## Rollout order (staged — §47 sub-rule (b))

Merging this PR has **zero live effect**. The Think Tank keeps running on
Lambda until step 3.

1. **crucible-research-PR544 must merge first.** The job prelude execs
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
