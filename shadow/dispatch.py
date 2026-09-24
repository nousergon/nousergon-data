"""Which `shadow/` modules the data-spot dispatcher can launch — declared.

`alpha-engine-config-I10920`. `shadow/arctic_parity.py` landed on `main` in
`b8993d50` with no `_WORKLOADS` entry. The comparator was written, reviewed,
tested and merged — and could not be run at all: ArcticDB is unreachable from
the laptop (`alpha-engine-config-I9771`) and only the data-spot dispatcher puts
code on an in-region box. Nothing went red. There was no surface anywhere on
which "merged but unreachable" was a visible state, which is why it sat unrun
for two days while `data.cutover_ready.parity` stayed UNMET on a reading
predating every parity fix merged since.

This file is that surface, and it is the class fix rather than the instance
fix. Every module under `shadow/` appears in EXACTLY ONE of the two maps
below. `infrastructure/lambdas/data-spot-dispatcher/test_handler.py` fails if

* a module under `shadow/` appears in neither map (a new comparator cannot
  land without answering the question);
* a module appears in both;
* a declared workload key is absent from the dispatcher's `_WORKLOADS`;
* the workload's command does not actually invoke the declared
  `python -m shadow` subcommand;
* a dispatchable workload is absent from `_WORKLOADS_REQUIRING_TRADING_DAY`
  (every one of these comparators is meaningless without a trading day, and a
  missing day must be refused at dispatch rather than defaulted to "today").

Deliberately import-free. The Lambda's test process loads this file BY PATH,
outside the `shadow` package, so that asserting dispatcher coverage never
drags `boto3`, `arcticdb` or `data_gate` into it.
"""

from __future__ import annotations

#: module stem -> (data-spot dispatcher workload key, the `python -m shadow`
#: subcommand that workload's rendered command must contain).
#:
#: `root` is keyed to `shadow-weekday` because `shadow run` — the output-root
#: override `shadow/root.py` implements — is what those four collector legs are
#: wrapped in; it is reachable, through that workload, and the test proves it.
DISPATCHABLE_MODULES: dict[str, tuple[str, str]] = {
    "root": ("shadow-weekday", "shadow run"),
    "parity": ("shadow-parity", "shadow parity"),
    "arctic_parity": ("arctic-parity", "shadow arctic-parity"),
}

#: module stem -> why it has no workload. A reason, never a blank: "it is a
#: library" is a claim the next reader can check, and an empty string is how
#: this map would quietly become a skip-list.
NOT_DISPATCHABLE_MODULES: dict[str, str] = {
    "__init__": "package init — re-exports from shadow.root, no entrypoint of its own",
    "__main__": (
        "the CLI every dispatchable module above is reached THROUGH "
        "(`python -m shadow <subcommand>`); it is not a workload itself"
    ),
    "arctic_seed": (
        "a step inside `shadow run` (shadow/root.py 'Budget', "
        "alpha-engine-config-I10866) — seeding on its own would leave a shadow "
        "ArcticDB library that nothing then appends to, which arctic_parity "
        "grades `shadow_missing_day` by design"
    ),
    "interceptor": (
        "the boto3 hook `shadow.root.activate` installs into the running "
        "process; it has no entrypoint and running it alone redirects nothing"
    ),
    "run_state": (
        "a helper the collectors read under `shadow run` so a shadow run reads "
        "its own phase markers rather than v1's (alpha-engine-config-I10891)"
    ),
    "pinned_inputs": (
        "a resolver the collectors call under `shadow run` so a replay reads "
        "each mutable input at the VERSION the replayed day's run actually "
        "read (alpha-engine-config-I11216); it answers a question and launches "
        "nothing, and outside a replay it answers 'unpinned' immediately"
    ),
    "gate_dispatch": (
        "a step inside `shadow parity --dispatch-gate` that dispatches "
        "data-gate.yml after the report is published (alpha-engine-config-I11361); "
        "it reads nothing to compare, and run alone it would trigger a gate read "
        "for a report nobody just wrote"
    ),
    "dispatch": "this declaration itself",
    "retention": (
        "the date rule and guarded delete behind `python -m shadow prune` "
        "(alpha-engine-config-I11447); deleting libraries is destructive, so it "
        "runs by hand on the data-spot box, dry run by default, and no dispatcher "
        "workload may reach `--apply`"
    ),
}
