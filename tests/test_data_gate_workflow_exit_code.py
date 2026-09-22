"""alpha-engine-config-I10906: the scheduled data-gate reader must never fail the
job on a business verdict (gate UNMET) — only on a genuine reader failure. This
guards the WORKFLOW wiring, so the coupling data-gate.yml used to carry (and the
CLI default `data_gate/__main__.py::main` used to enforce) cannot come back
without a red test, from either side:

* the CLI defaulting to `--fail-on-unmet` behavior again (see
  test_data_gate.py::test_the_cli_exit_codes_separate_not_met_from_unmeasured),
  or
* this workflow hard-coding `--fail-on-unmet` onto the scheduled/push-triggered
  invocation.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "data-gate.yml"


def _load_workflow() -> dict:
    # PyYAML parses the bare `on:` mapping key as the boolean `True` under the
    # default 1.1 resolver — read it back out with `.get(True, ...)` as a
    # fallback the same way GitHub's own schema treats it, so this test does
    # not silently see an empty `on:` block on a PyYAML version bump.
    text = WORKFLOW_PATH.read_text(encoding="utf-8")
    doc = yaml.safe_load(text)
    return {"text": text, "doc": doc}


def _read_steps(doc: dict) -> list[dict]:
    return doc["jobs"]["read-gate"]["steps"]


def test_scheduled_and_push_triggers_never_hardcode_fail_on_unmet():
    """The literal flag must only ever be reachable through the workflow_dispatch
    input — never typed directly into a `run:` step, which would apply it on
    every trigger including the two crons and the push trigger."""
    loaded = _load_workflow()
    for step in _read_steps(loaded["doc"]):
        run = step.get("run", "")
        # The flag may appear via the $FAIL_ON_UNMET_FLAG env var reference; it
        # must never appear as a literal token in the run script itself.
        assert "--fail-on-unmet" not in run, (
            f"step {step.get('name')!r} hard-codes --fail-on-unmet; it must only "
            "be reachable through the workflow_dispatch input, gated to that "
            "event name, or a scheduled/push run will fail on every UNMET "
            "reading again (alpha-engine-config-I10906)."
        )


def test_the_fail_on_unmet_env_var_is_gated_to_workflow_dispatch():
    loaded = _load_workflow()
    job = loaded["doc"]["jobs"]["read-gate"]
    flag_expr = job["env"]["FAIL_ON_UNMET_FLAG"]
    assert "workflow_dispatch" in flag_expr
    assert "inputs.fail_on_unmet" in flag_expr


def test_the_workflow_dispatch_input_defaults_off():
    loaded = _load_workflow()
    on_block = loaded["doc"].get("on") or loaded["doc"].get(True)
    dispatch = on_block["workflow_dispatch"]
    assert dispatch["inputs"]["fail_on_unmet"]["default"] is False


def test_scheduled_triggers_are_unchanged_cron_and_push_only():
    """Sanity anchor: the cron cadence and push-path triggers this fix must not
    touch are still exactly what alpha-engine-config-I10906 measured."""
    loaded = _load_workflow()
    on_block = loaded["doc"].get("on") or loaded["doc"].get(True)
    crons = {entry["cron"] for entry in on_block["schedule"]}
    # The two 23:30/Saturday crons are the BACKSTOP and are unchanged; the two
    # added by alpha-engine-config-I11355 are the reads that follow the parity
    # publish (01:30 UTC Tue-Sat after the same-day report, 12:30 UTC Mon-Fri
    # after the shadow-morning rewrite). Pinned here as a set so a cadence
    # change is a deliberate edit to this line.
    assert crons == {"30 23 * * *", "0 18 * * 6", "30 1 * * 2-6", "30 12 * * 1-5"}
    assert on_block["push"]["branches"] == ["main"]


def test_no_step_re_derives_the_gate_verdict_into_the_job_result():
    """The old "Fail the job on a non-MET reading" step re-coupled the verdict
    to the job's own conclusion by hand; it must not come back under any name."""
    loaded = _load_workflow()
    for step in _read_steps(loaded["doc"]):
        run = step.get("run", "")
        assert "outcome" not in run and "exit_code" not in run, (
            f"step {step.get('name')!r} inspects a sibling step's outcome/exit_code "
            "to re-derive the job's own pass/fail from the gate verdict — that is "
            "exactly the coupling alpha-engine-config-I10906 removed."
        )
