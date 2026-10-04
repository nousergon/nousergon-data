"""A job killed at its `timeout-minutes` still reaches `notify-main-failure`.

alpha-engine-config C23 (nightly run 2026-10-02): `phase-exit-metrics.yml`'s
`executor_profile` job hit its timeout on four consecutive scheduled runs
(36282165477, 36508478334, 36653682089, 36799641194, then 36950614227) and
`notify-main-failure` was SKIPPED every time. A job GitHub kills at
`timeout-minutes` concludes `cancelled`, not `failure`, and the old
`if: failure() && github.ref == 'refs/heads/main'` is false for a `cancelled`
need, so a safeguard that had stopped producing its metric paged nobody.
`data-gate.yml` had already recorded the same blind spot in a comment.

**The semantics this file models were measured, not assumed.** Probe run
37099245752 on this repo (a throwaway workflow on the fix branch, deleted in
the same PR): a job with `timeout-minutes: 1` running `sleep 150`, and
dependants with `if: always()` reporting the status functions. Measured:
`needs.slow.result == 'cancelled'`, `cancelled() == false`,
`failure() == false`, the run's own conclusion `cancelled`, and a job gated on
the condition below RAN while one gated on `cancelled()` was skipped. So a
timeout cancels the JOB without cancelling the RUN, and `!cancelled()` is what
separates it from a concurrency supersede or a human pressing Cancel (both of
which cancel the run).

This evaluates every notify job's `if:` against the need results a timeout, a
failure, a green run, a superseded run and a pull-request run produce. It is a
small evaluator for the expression subset those conditions use, not a GitHub
Actions emulator: a token it does not know RAISES, so a condition rewritten
into a shape nobody modelled fails here rather than being read as whatever the
evaluator guessed. Same evaluator as `crucible/tests/test_workflow_notify_on_cancel.py`
(alpha-engine-config-I11448), so the fleet's two copies agree on semantics.
"""

from __future__ import annotations

import pathlib
import re
from typing import Any

import pytest
import yaml

WORKFLOW_DIR = pathlib.Path(__file__).resolve().parents[1] / ".github" / "workflows"
WORKFLOWS = sorted(p for p in WORKFLOW_DIR.glob("*.y*ml") if p.suffix in {".yml", ".yaml"})
NOTIFY_WORKFLOW = "nousergon/nousergon-lib/.github/workflows/notify-ci-failure.yml@"

MAIN = "refs/heads/main"
PR_REF = "refs/pull/1/merge"

_TOKEN = re.compile(
    r"""\s*(?:
        (?P<contains>contains\(\s*needs\.\*\.result\s*,\s*'(?P<cval>[^']*)'\s*\))
      | (?P<need>needs\.(?P<nname>[A-Za-z0-9_-]+)\.result)
      | (?P<fn>(?:always|failure|cancelled|success)\(\))
      | (?P<ctx>github\.(?:event_name|ref))
      | (?P<str>'[^']*')
      | (?P<op>&&|\|\||==|!=|!|\(|\))
    )""",
    re.VERBOSE,
)


def _to_python(condition: str) -> str:
    """Translate the expression subset into a Python expression over the
    evaluation namespace. Anything else raises."""
    expr = condition.strip()
    if expr.startswith("${{") and expr.endswith("}}"):
        expr = expr[3:-2]
    expr = expr.rstrip()
    out: list[str] = []
    pos = 0
    while pos < len(expr):
        match = _TOKEN.match(expr, pos)
        if match is None or match.end() == pos:
            raise ValueError(f"unmodelled expression at {expr[pos:]!r} in {condition!r}")
        pos = match.end()
        if match["contains"]:
            out.append(f"_contains_need({match['cval']!r})")
        elif match["need"]:
            out.append(f"_need({match['nname']!r})")
        elif match["fn"]:
            out.append(f"_fn({match['fn'][:-2]!r})")
        elif match["ctx"]:
            out.append(f"_ctx({match['ctx']!r})")
        elif match["str"]:
            out.append(repr(match["str"][1:-1]))
        else:
            out.append({"&&": " and ", "||": " or ", "!": " not "}.get(match["op"], match["op"]))
    return "".join(out)


def _evaluate(
    condition: str,
    *,
    needs: dict[str, str],
    run_cancelled: bool,
    event_name: str,
    ref: str = MAIN,
) -> bool:
    if not condition:
        # GitHub's implicit `success()`.
        return all(result == "success" for result in needs.values()) and not run_cancelled
    functions = {
        "always": True,
        "failure": any(result == "failure" for result in needs.values()),
        "cancelled": run_cancelled,
        "success": all(result == "success" for result in needs.values()) and not run_cancelled,
    }
    namespace: dict[str, Any] = {
        "_fn": functions.__getitem__,
        "_need": needs.__getitem__,
        "_contains_need": lambda value: value in needs.values(),
        "_ctx": {"github.event_name": event_name, "github.ref": ref}.__getitem__,
        "__builtins__": {},
    }
    return bool(eval(_to_python(condition), namespace))  # noqa: S307 - translated, closed namespace


def _notify_jobs() -> list[tuple[str, str, dict[str, Any], bool]]:
    """Every job calling the fleet notification workflow, with whether its
    workflow's concurrency cancels a superseded run."""
    found = []
    for path in WORKFLOWS:
        workflow = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        concurrency = workflow.get("concurrency") or {}
        supersedes = isinstance(concurrency, dict) and bool(concurrency.get("cancel-in-progress"))
        for name, job in (workflow.get("jobs") or {}).items():
            if str(job.get("uses", "")).startswith(NOTIFY_WORKFLOW):
                found.append((path.name, name, job, supersedes))
    return found


NOTIFY_JOBS = _notify_jobs()
SUPERSEDING = [entry for entry in NOTIFY_JOBS if entry[3]]


def _ids(entries: list[tuple[str, str, dict[str, Any], bool]]) -> list[str]:
    return [f"{workflow}:{job}" for workflow, job, _, _ in entries]


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def test_the_notify_jobs_are_found() -> None:
    """Guard the guard: an empty parametrisation passes vacuously."""
    names = {workflow for workflow, _, _, _ in NOTIFY_JOBS}
    assert {"phase-exit-metrics.yml", "data-gate.yml", "ci.yml"} <= names
    assert len(NOTIFY_JOBS) >= 40
    assert "cost-gate.yml" in {workflow for workflow, _, _, _ in SUPERSEDING}


@pytest.mark.parametrize(("workflow", "name", "job", "supersedes"), NOTIFY_JOBS, ids=_ids(NOTIFY_JOBS))
def test_a_timed_out_need_reaches_the_notification(
    workflow: str, name: str, job: dict[str, Any], supersedes: bool
) -> None:
    """The executor_profile case: each need in turn killed at its timeout,
    the jobs after it skipped or green, and the run itself NOT cancelled."""
    needs = _needs(job)
    assert needs, f"{workflow}:{name} needs nothing, so it can notify on nothing"
    for index, timed_out in enumerate(needs):
        for after in ("skipped", "success"):
            results = {
                need: ("success" if i < index else "cancelled" if i == index else after)
                for i, need in enumerate(needs)
            }
            for event in ("schedule", "push", "workflow_dispatch"):
                assert _evaluate(
                    job.get("if", ""), needs=results, run_cancelled=False, event_name=event
                ), (
                    f"{workflow}:{name} does not run on a {event} run when `{timed_out}` "
                    f"is killed at its timeout-minutes (needs {results}). A timeout "
                    "concludes `cancelled`, not `failure`."
                )


@pytest.mark.parametrize(("workflow", "name", "job", "supersedes"), NOTIFY_JOBS, ids=_ids(NOTIFY_JOBS))
def test_a_failed_need_still_reaches_the_notification(
    workflow: str, name: str, job: dict[str, Any], supersedes: bool
) -> None:
    needs = _needs(job)
    results = {need: ("failure" if i == 0 else "skipped") for i, need in enumerate(needs)}
    assert _evaluate(job.get("if", ""), needs=results, run_cancelled=False, event_name="push")


@pytest.mark.parametrize(("workflow", "name", "job", "supersedes"), NOTIFY_JOBS, ids=_ids(NOTIFY_JOBS))
def test_a_green_run_notifies_nobody(
    workflow: str, name: str, job: dict[str, Any], supersedes: bool
) -> None:
    results = dict.fromkeys(_needs(job), "success")
    assert not _evaluate(job.get("if", ""), needs=results, run_cancelled=False, event_name="push")


@pytest.mark.parametrize(("workflow", "name", "job", "supersedes"), NOTIFY_JOBS, ids=_ids(NOTIFY_JOBS))
def test_a_cancelled_run_does_not_page(
    workflow: str, name: str, job: dict[str, Any], supersedes: bool
) -> None:
    """A run cancelled as a whole -- a concurrency supersede or a human --
    is routine; a burst of merges would otherwise page once per merge
    (cost-gate.yml carries `cancel-in-progress: true`)."""
    results = dict.fromkeys(_needs(job), "cancelled")
    assert not _evaluate(job.get("if", ""), needs=results, run_cancelled=True, event_name="push")


@pytest.mark.parametrize(("workflow", "name", "job", "supersedes"), NOTIFY_JOBS, ids=_ids(NOTIFY_JOBS))
def test_a_pull_request_run_never_pages(
    workflow: str, name: str, job: dict[str, Any], supersedes: bool
) -> None:
    """`always()` must not widen the default-branch gate: a PR-branch red is
    the gate doing its job (config#2855's storm)."""
    for result in ("failure", "cancelled"):
        results = dict.fromkeys(_needs(job), result)
        assert not _evaluate(
            job.get("if", ""),
            needs=results,
            run_cancelled=False,
            event_name="pull_request",
            ref=PR_REF,
        )


def test_the_evaluator_refuses_an_unmodelled_token() -> None:
    with pytest.raises(ValueError, match="unmodelled"):
        _to_python("${{ startsWith(github.ref, 'refs/tags/') }}")


def test_the_pre_fix_condition_is_what_skipped_executor_profile() -> None:
    """The measured defect, reproduced through the same evaluator."""
    assert not _evaluate(
        "${{ failure() && github.ref == 'refs/heads/main' }}",
        needs={"executor_profile": "cancelled", "v1_data_stage": "success"},
        run_cancelled=False,
        event_name="schedule",
    )
