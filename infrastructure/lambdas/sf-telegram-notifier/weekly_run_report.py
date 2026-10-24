"""Weekly run report: every imperfect step, plus the Director's verdict.

Brian, 2026-10-24: "in addition to the weekly freshness SF succeeded Telegram,
I also need bullet points for every step of the process that is less than
functioning perfectly ... even if it's running successfully, there still may
be issues or bugs or things that are being passed on ... And I also need just a
high level of the director report as well. So is everything red? And what are
the key concerns of the director, say the top three concerns?"

The 2026-10-24 run is why. It ended SUCCEEDED with 38/38 stage coverage, and
the message said nothing else, while the run had skipped parity (so it was not
checked for look-ahead), skipped data phase 2 and RAG ingestion, left the model
zoo unservable, failed to rewrite ``config/executor_params.json`` for the
second week running, and the Director had graded the system RED.

Every bullet carries an ``Auto-fix:`` line (observability-policy §7.4b). The
line states only what existing wiring does. Nothing in this module fixes
anything, so a bullet with no specific handler says so and names the lane that
picks it up.

Every source is read-only and best-effort. A source that cannot be read
renders as its own bullet ("could not read ..."), never as a clean step: a
report that silently drops a source reads as "nothing wrong there".
"""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

S3_BUCKET = "alpha-engine-research"
WEEKLY_PIPELINE_NAME = "ne-weekly-freshness-pipeline"

# ── Auto-fix lines ──────────────────────────────────────────────────────────
# Each states what existing wiring does, and nothing more (§7.4b: "a claimed
# fix that does not exist is worse than none").

#: The report itself is sent at warning severity when it carries any bullet
#: (see index.handler), so it lands on the alert intake the nightly sweep reads.
AUTO_FIX_SWEEP = "none automatic; tonight's nightly sweep (7pm PT) triages it from the alert intake."
AUTO_FIX_DECLARED_SKIP = "none needed; skipped on purpose by the run input, runs again once the flag is cleared."
#: validators/stage_output_sweep.py publishes its verdict on the alert bus;
#: overseer/playbooks.yaml registers class data_stage_output_sweep with
#: response drain-queue.
AUTO_FIX_STAGE_OUTPUT = "the stage-output check already put this on the alert bus; the alert-drain lane picks it up."
AUTO_FIX_DIRECTOR = "none automatic; the Director's action plan names an owner and an ask for each concern."

#: Bullet cap. Telegram's message limit is 4096 characters and the digest
#: above the bullets already uses part of it. Elision is always announced.
MAX_ISSUES = 10
_PROBLEM_MAX_CHARS = 150
_RISK_MAX_CHARS = 190
#: Seconds this module may spend reading sources. The Lambda times out at 30s
#: and the execution-history fetch runs first.
READ_BUDGET_SEC = 8.0

#: Degraded-family booleans the weekly definition sets on its own state
#: (step_function.json, CheckGateDegradedNotify and NotifyCompleteMultipleDegraded).
_DEGRADED_FLAGS: Tuple[Tuple[str, str], ...] = (
    ("gate_degraded", "Pre-spend gates"),
    ("health_check_degraded", "Tail health checks"),
    ("report_card_degraded", "Report card"),
    ("parity_degraded", "Parity verdict"),
    ("research_predictor_degraded", "Research/predictor"),
    ("aggregate_costs_degraded", "Cost aggregation"),
)

#: Statuses of a per-stage coverage record that are not a problem
#: (krepis.stage_coverage STATUS_*; COVERED_NO_OUTPUT is a stage that declares
#: no artifact).
_COVERAGE_OK = frozenset({"COVERED", "COVERED_NO_OUTPUT", "NOT_APPLICABLE"})
#: walk_forward_stability statuses that are not a problem.
_WALK_FORWARD_OK = frozenset({"ok", "stable", "pass"})
_DIRECTOR_STATUS_RE = re.compile(r"\bOverall\s+(RED|AMBER|YELLOW|GREEN)\b", re.IGNORECASE)
_STATUS_DOT = {"RED": "🔴", "AMBER": "🟠", "YELLOW": "🟡", "GREEN": "🟢"}


@dataclass(frozen=True)
class Issue:
    step: str
    problem: str
    auto_fix: str

    @property
    def needs_triage(self) -> bool:
        """False only for a bullet whose auto-fix is "none needed"."""
        return not self.auto_fix.startswith("none needed")


@dataclass(frozen=True)
class DirectorSummary:
    status: Optional[str]  # RED / AMBER / YELLOW / GREEN, or None if unstated
    concerns: Tuple[str, ...]
    source_key: str


@dataclass
class WeeklyReport:
    issues: List[Issue]
    director: Optional[DirectorSummary]


def md_escape(text: str) -> str:
    """Escape Telegram legacy-Markdown entity characters in free text.

    Stage, flag and metric names are full of underscores
    (``exit_rules_contribution_lift``); unescaped, Telegram reads them as
    italics and can reject the whole message.
    """
    return re.sub(r"([_*`\[])", r"\\\1", text)


def _clip(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


class _Reader:
    """Best-effort S3 JSON reads with a shared time budget."""

    def __init__(self, s3_client: Any, *, budget_sec: float, clock: Callable[[], float]):
        self._s3 = s3_client
        self._clock = clock
        self._deadline = clock() + budget_sec
        self.unreadable: List[Tuple[str, str]] = []

    def out_of_time(self) -> bool:
        return self._clock() > self._deadline

    def get_json(self, key: str, *, optional: bool = False) -> Optional[Any]:
        if self.out_of_time():
            self.unreadable.append((key, "read budget spent"))
            return None
        try:
            body = self._s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            doc = json.loads(body)
            # Some writers double-encode (a JSON string holding JSON).
            if isinstance(doc, str):
                doc = json.loads(doc)
            return doc
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
            if optional and code in ("NoSuchKey", "404"):
                return None
            reason = code or type(exc).__name__
            logger.warning("weekly report: could not read s3://%s/%s: %s", S3_BUCKET, key, exc)
            self.unreadable.append((key, reason))
            return None

    def get_many(self, keys: Sequence[str]) -> List[Tuple[str, Optional[Any]]]:
        """``get_json`` over many keys concurrently (boto3 clients are
        thread-safe). A weekly run leaves ~40 per-stage coverage records."""
        keys = list(keys)
        if not keys:
            return []
        with ThreadPoolExecutor(max_workers=min(8, len(keys))) as pool:
            docs = list(pool.map(self.get_json, keys))
        return list(zip(keys, docs))

    def list_keys(self, prefix: str) -> List[Tuple[str, Optional[datetime]]]:
        if self.out_of_time():
            self.unreadable.append((prefix, "read budget spent"))
            return []
        try:
            keys: List[Tuple[str, Optional[datetime]]] = []
            token: Optional[str] = None
            for _ in range(5):
                kwargs: dict = {"Bucket": S3_BUCKET, "Prefix": prefix, "Delimiter": "/"}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self._s3.list_objects_v2(**kwargs)
                for obj in resp.get("Contents") or []:
                    keys.append((obj["Key"], obj.get("LastModified")))
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            return keys
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
            logger.warning("weekly report: could not list s3://%s/%s: %s", S3_BUCKET, prefix, exc)
            self.unreadable.append((prefix, code or type(exc).__name__))
            return []


def _parse_json(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        doc = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return doc if isinstance(doc, dict) else {}


def _aware(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


# ── Sources ─────────────────────────────────────────────────────────────────


def issues_from_execution(exec_input: Mapping[str, Any], exec_output: Mapping[str, Any]) -> List[Issue]:
    """Issues the execution's own input and final state record."""
    issues: List[Issue] = []

    # Declared skips. The final state carries the normalized flags; fall back
    # to the raw input when the output is absent.
    flags = exec_output if any(k.startswith("skip_") for k in exec_output) else exec_input
    skipped = sorted(k for k, v in flags.items() if k.startswith("skip_") and v is True)
    if skipped:
        shown = ", ".join(f"`{k}`" for k in skipped[:8])
        more = f" (+{len(skipped) - 8} more)" if len(skipped) > 8 else ""
        issues.append(Issue(
            step="Declared skips",
            problem=f"{len(skipped)} stage group(s) did not run this week: {shown}{more}",
            auto_fix=AUTO_FIX_DECLARED_SKIP,
        ))

    for flag, step in _DEGRADED_FLAGS:
        if exec_output.get(flag) is True:
            issues.append(Issue(step=step, problem=f"degraded on this run (`{flag}`)", auto_fix=AUTO_FIX_SWEEP))

    reasons = (exec_output.get("degraded_summary") or {}).get("reasons") if isinstance(
        exec_output.get("degraded_summary"), dict) else None
    if isinstance(reasons, dict):
        flagged = {step for flag, step in _DEGRADED_FLAGS if exec_output.get(flag) is True}
        for family, reason in sorted(reasons.items()):
            if family in flagged:
                continue
            issues.append(Issue(step=f"Degraded: {family}", problem=md_escape(str(reason)), auto_fix=AUTO_FIX_SWEEP))

    if exec_output.get("model_zoo_unservable") is True:
        issues.append(Issue(
            step="Model zoo",
            problem="model-zoo selection produced no servable model this week (`model_zoo_unservable`)",
            auto_fix=AUTO_FIX_SWEEP,
        ))
    if exec_output.get("director_retro_refused") is True:
        issues.append(Issue(step="Director", problem="retro was refused (`director_retro_refused`)", auto_fix=AUTO_FIX_SWEEP))
    if exec_output.get("cost_coverage_gap") is True:
        issues.append(Issue(step="Cost aggregation", problem="cost coverage gap on this run (`cost_coverage_gap`)", auto_fix=AUTO_FIX_SWEEP))
    blind = exec_output.get("weekly_preflight_blind_spot")
    if isinstance(blind, dict) and blind.get("present") is True:
        names = ", ".join(f"`{n}`" for n in (blind.get("required_skip_names") or [])[:6])
        issues.append(Issue(
            step="Preflight",
            problem=f"{blind.get('required_skip_count', '?')} required check(s) never ran anywhere: {names}",
            auto_fix=AUTO_FIX_SWEEP,
        ))
    relaunches = exec_output.get("substrate_relaunch_attempts")
    if isinstance(relaunches, int) and relaunches > 0:
        issues.append(Issue(
            step="Spot box",
            problem=f"the weekly spot box was relaunched {relaunches} time(s) mid-run",
            auto_fix="the pipeline's own relaunch already ran; nothing more is automatic. " + AUTO_FIX_SWEEP,
        ))
    if exec_output.get("notify_error"):
        issues.append(Issue(step="Completion email", problem="the SNS completion notice failed to publish", auto_fix=AUTO_FIX_SWEEP))
    return issues


def issues_from_stage_outputs(reader: _Reader, run_date: str, execution_arn: str) -> List[Issue]:
    """Artifacts a stage that ran should have written and did not (or wrote stale)."""
    doc = reader.get_json(f"_stage_outputs/{WEEKLY_PIPELINE_NAME}/{run_date}.json", optional=True)
    if not isinstance(doc, dict):
        return []
    if doc.get("execution_arn") and execution_arn and doc["execution_arn"] != execution_arn:
        return []
    issues: List[Issue] = []
    # Only `defects` and `unmeasured`. Its "skipped" verdicts are not used:
    # the sweep runs before the pipeline's last stages, so it reports them as
    # "did not enter" on every run (measured 2026-10-24: ReportCard and
    # Director, both of which ran).
    for row in list(doc.get("defects") or []) + list(doc.get("unmeasured") or []):
        if not isinstance(row, dict):
            continue
        what = row.get("artifact_id") or row.get("key") or "artifact"
        detail = row.get("detail") or row.get("verdict") or "defect"
        issues.append(Issue(
            step=str(row.get("stage") or "Stage output"),
            problem=md_escape(_clip(f"{what}: {detail}", _PROBLEM_MAX_CHARS)),
            auto_fix=AUTO_FIX_STAGE_OUTPUT,
        ))
    return issues


def issues_from_stage_coverage(reader: _Reader, run_date: str) -> List[Issue]:
    """Each stage's own coverage verdict, as the stage recorded it during the run."""
    issues: List[Issue] = []
    keys = [k for k, _ in reader.list_keys(f"_stage_coverage/{run_date}/") if k.endswith(".json")]
    for key, rec in reader.get_many(keys):
        if not isinstance(rec, dict):
            continue
        status = str(rec.get("status") or "").upper()
        if status in _COVERAGE_OK and not rec.get("is_finding"):
            continue
        parts = [status.lower() or "no status"]
        for field in ("missing", "stale", "unmeasured"):
            vals = rec.get(field) or []
            if vals:
                parts.append(f"{field}: {', '.join(map(str, vals[:4]))}")
        issues.append(Issue(
            step=str(rec.get("stage") or key.rsplit("/", 1)[-1][:-5]),
            problem=md_escape(_clip("; ".join(parts), _PROBLEM_MAX_CHARS)),
            auto_fix=AUTO_FIX_SWEEP,
        ))
    return issues


def issues_from_backtest(reader: _Reader, run_date: str) -> List[Issue]:
    issues: List[Issue] = []
    wf = reader.get_json(f"backtest/{run_date}/walk_forward_stability.json", optional=True)
    if isinstance(wf, dict) and str(wf.get("status") or "").lower() not in _WALK_FORWARD_OK:
        issues.append(Issue(
            step="Backtester",
            problem=md_escape(_clip(
                f"walk-forward stability {wf.get('status')}: {wf.get('reason') or ''}", _PROBLEM_MAX_CHARS)),
            auto_fix=AUTO_FIX_SWEEP,
        ))
    st = reader.get_json(f"backtest/{run_date}/self_test.json", optional=True)
    if isinstance(st, dict) and str(st.get("verdict") or "").upper() not in ("PASS", ""):
        issues.append(Issue(step="Backtester", problem=f"self-test verdict {md_escape(str(st.get('verdict')))}", auto_fix=AUTO_FIX_SWEEP))
    return issues


def issues_from_health(reader: _Reader, execution_start: datetime) -> List[Issue]:
    """Module health files this run rewrote that report anything but a clean pass."""
    issues: List[Issue] = []
    keys = []
    for key, modified in reader.list_keys("health/"):
        modified = _aware(modified)
        if key.endswith(".json") and modified is not None and modified >= execution_start:
            keys.append(key)
    for key, doc in reader.get_many(keys):
        if not isinstance(doc, dict) or "status" not in doc:
            continue
        problems: List[str] = []
        if str(doc.get("status")).lower() not in ("ok", "healthy", "pass"):
            problems.append(f"status {doc.get('status')}")
        if doc.get("error"):
            problems.append(f"error: {doc['error']}")
        for w in (doc.get("warnings") or [])[:2]:
            problems.append(str(w))
        for d in doc.get("deliverables") or []:
            if isinstance(d, dict) and d.get("required") and not d.get("produced"):
                problems.append(f"required deliverable {d.get('name')} not produced")
        if problems:
            issues.append(Issue(
                step=str(doc.get("module") or key.rsplit("/", 1)[-1][:-5]),
                problem=md_escape(_clip("; ".join(problems), _PROBLEM_MAX_CHARS)),
                auto_fix=AUTO_FIX_SWEEP,
            ))
    return issues


def director_from_plan(plan: Mapping[str, Any], key: str) -> Tuple[DirectorSummary, List[Issue]]:
    summary_text = str(plan.get("system_summary") or "")
    match = _DIRECTOR_STATUS_RE.search(summary_text)
    status = match.group(1).upper() if match else None
    risks = plan.get("top_risks") or []
    concerns = tuple(
        md_escape(_clip(str(r), _RISK_MAX_CHARS)) for r in risks[:3] if str(r).strip()
    )
    issues: List[Issue] = []
    att = plan.get("attestation") if isinstance(plan.get("attestation"), dict) else {}
    contamination = str(att.get("contamination_verdict") or "").upper()
    if att and contamination != "PASS":
        issues.append(Issue(
            step="Report card",
            problem=f"look-ahead contamination verdict {contamination or 'missing'}; this run is not proven look-ahead-free",
            auto_fix=AUTO_FIX_SWEEP,
        ))
    for half in ("arithmetic_verdict", "evaluator_verdict", "backtester_verdict", "evaluator_stage_verdict"):
        val = str(att.get(half) or "").upper()
        if att and val and val != "PASS":
            issues.append(Issue(step="Report card", problem=f"attestation `{half}` {val}", auto_fix=AUTO_FIX_SWEEP))
    if att.get("promotion_withheld") is True:
        issues.append(Issue(step="Report card", problem="promotion withheld by the attestation", auto_fix=AUTO_FIX_SWEEP))
    if plan.get("route_degraded") is True:
        issues.append(Issue(
            step="Director",
            problem=md_escape(_clip(f"plan ran on a degraded model route: {plan.get('route_degraded_reason') or ''}", _PROBLEM_MAX_CHARS)),
            auto_fix=AUTO_FIX_SWEEP,
        ))
    if plan.get("advisory_unverified") is True:
        issues.append(Issue(step="Director", problem="plan is advisory-unverified", auto_fix=AUTO_FIX_SWEEP))
    tel = plan.get("plan_call_telemetry") if isinstance(plan.get("plan_call_telemetry"), dict) else {}
    omitted = tel.get("DirectorPlanCarryoverOmitted")
    if isinstance(omitted, (int, float)) and omitted > 0:
        issues.append(Issue(
            step="Director",
            problem=f"{int(omitted)} of {int(tel.get('DirectorPlanCarryoverItems') or omitted)} carryover items were left out of the plan prompt",
            auto_fix=AUTO_FIX_SWEEP,
        ))
    if tel.get("DirectorPlanLatencyAmber") == 1:
        issues.append(Issue(
            step="Director",
            problem=f"plan call slow: {tel.get('DirectorPlanLatencySeconds')}s (amber at {tel.get('DirectorPlanAmberSeconds')}s)",
            auto_fix=AUTO_FIX_SWEEP,
        ))
    return DirectorSummary(status=status, concerns=concerns, source_key=key), issues


def issues_from_retro(retro: Mapping[str, Any]) -> List[Issue]:
    tel = retro.get("plan_call_telemetry") if isinstance(retro.get("plan_call_telemetry"), dict) else {}
    if tel.get("RetroJudgeLatencyAmber") == 1:
        return [Issue(
            step="Director retro",
            problem=(
                f"judge call slow: {tel.get('RetroJudgeLatencySeconds')}s against a "
                f"{tel.get('RetroJudgeCeilingSeconds')}s ceiling"
            ),
            auto_fix=AUTO_FIX_SWEEP,
        )]
    return []


# ── Assembly ────────────────────────────────────────────────────────────────


def build_weekly_report(
    *,
    s3_client: Any,
    run_date: Optional[str],
    execution_arn: str,
    execution_start: datetime,
    exec_input: Mapping[str, Any],
    exec_output: Mapping[str, Any],
    budget_sec: float = READ_BUDGET_SEC,
    clock: Callable[[], float] = time.monotonic,
) -> WeeklyReport:
    exec_issues = issues_from_execution(exec_input, exec_output)
    issues: List[Issue] = []
    director: Optional[DirectorSummary] = None
    if s3_client is None:
        return WeeklyReport(issues=exec_issues, director=None)
    if not run_date:
        exec_issues.append(Issue(
            step="Report",
            problem="run date unknown, so stage outputs and the Director report could not be read",
            auto_fix=AUTO_FIX_SWEEP,
        ))
        return WeeklyReport(issues=exec_issues, director=None)

    reader = _Reader(s3_client, budget_sec=budget_sec, clock=clock)
    plan_key = f"director/{run_date}/action_plan.json"
    plan = reader.get_json(plan_key, optional=True)
    if isinstance(plan, dict):
        director, plan_issues = director_from_plan(plan, plan_key)
        issues.extend(plan_issues)
    elif plan is None and not any(k == plan_key for k, _ in reader.unreadable):
        issues.append(Issue(step="Director", problem="no action plan was written for this run", auto_fix=AUTO_FIX_SWEEP))
    retro = reader.get_json(f"director/{run_date}/retro.json", optional=True)
    if isinstance(retro, dict):
        issues.extend(issues_from_retro(retro))
    issues.extend(issues_from_stage_outputs(reader, run_date, execution_arn))
    issues.extend(issues_from_backtest(reader, run_date))
    issues.extend(issues_from_stage_coverage(reader, run_date))
    issues.extend(issues_from_health(reader, execution_start))

    # Unreadable sources go right after the run's own issues, ahead of the
    # cap, so a missing source can never be elided out of sight.
    # Same-reason failures collapse into one bullet (a missing grant fails
    # every key under it the same way).
    by_reason: dict = {}
    for key, reason in reader.unreadable:
        by_reason.setdefault(reason, []).append(key)
    unreadable = []
    for reason, keys in by_reason.items():
        shown = ", ".join(keys[:3]) + (f" (+{len(keys) - 3} more)" if len(keys) > 3 else "")
        unreadable.append(Issue(
            step="Report",
            problem=md_escape(_clip(
                f"could not read {shown} ({reason}); missing from this list", _PROBLEM_MAX_CHARS + 60)),
            auto_fix=AUTO_FIX_SWEEP,
        ))
    return WeeklyReport(issues=exec_issues + unreadable + issues, director=director)


def format_report_lines(report: WeeklyReport) -> List[str]:
    lines: List[str] = []
    issues = report.issues
    if issues:
        lines.append(f"*Issues ({len(issues)}):*")
        for issue in issues[:MAX_ISSUES]:
            lines.append(f"• *{md_escape(issue.step)}:* {issue.problem}")
            lines.append(f"  Auto-fix: {issue.auto_fix}")
        if len(issues) > MAX_ISSUES:
            rest = len(issues) - MAX_ISSUES
            lines.append(f"_(+{rest} more issue{'s' if rest > 1 else ''} not shown, to fit Telegram's size limit)_")
    else:
        lines.append("*Issues:* none found in the run record, stage outputs or Director report.")
    d = report.director
    if d is not None:
        status = d.status or "not stated"
        dot = _STATUS_DOT.get(d.status or "", "⚪")
        lines.append(f"*Director:* {dot} overall {status}")
        for i, concern in enumerate(d.concerns, 1):
            lines.append(f"{i}. {concern}")
        if d.concerns:
            lines.append(f"  Auto-fix: {AUTO_FIX_DIRECTOR}")
    return lines


def report_needs_triage(report: WeeklyReport) -> bool:
    return any(i.needs_triage for i in report.issues) or (
        report.director is not None and report.director.status in ("RED", "AMBER", "YELLOW")
    )


def overall_auto_fix_line(report: WeeklyReport) -> str:
    if report_needs_triage(report):
        return f"Auto-fix: {AUTO_FIX_SWEEP}"
    return "Auto-fix: none needed (clean run)."
