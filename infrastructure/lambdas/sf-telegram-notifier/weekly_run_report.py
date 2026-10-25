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
import fnmatch
import os
from dataclasses import dataclass, field
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
AUTO_FIX_SWEEP = "none automatic; tonight's nightly sweep (7pm PT) triages it."
AUTO_FIX_DECLARED_SKIP = "none needed; skipped on purpose by the run input."
#: validators/stage_output_sweep.py publishes its verdict on the alert bus;
#: overseer/playbooks.yaml registers class data_stage_output_sweep with
#: response drain-queue.
AUTO_FIX_STAGE_OUTPUT = "already on the alert bus from the stage-output check; the alert-drain lane picks it up."
AUTO_FIX_DIRECTOR = "none automatic; the Director's action plan names an owner and an ask for each concern."

# ── Resolutions ─────────────────────────────────────────────────────────────
# Brian, 2026-10-24: "when we draft out the problems we also need to draft out
# the planned resolutions to the problems including the issue number that will
# resolve it ... Any problems that do not have planned resolutions should list
# out our alternatives for solving the issue and a recommended path forward".
#
# Each bullet carries stable keys; a registry maps key patterns (fnmatch) to the
# tracked item that resolves it. Two layers, first match wins:
#   1. s3://alpha-engine-research/ops/daily-summaries/weekly-issue-resolutions.json
#      — kept current by the nightly sweep, which files an issue (alternatives +
#      recommendation) for every bullet that arrived with no resolution;
#   2. weekly_issue_resolutions.json bundled with this Lambda — the seed.
# A bullet with no match says so; it never invents an issue number.

RESOLUTIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_issue_resolutions.json")
RESOLUTIONS_S3_KEY = "ops/daily-summaries/weekly-issue-resolutions.json"
DIRECTOR_LEDGER_KEY = "director/carryover_ledger.json"
NO_RESOLUTION = (
    "none tracked yet; tonight's nightly sweep files an issue with the "
    "alternatives and a recommended path."
)
_REPO_SHORT = {"alpha-engine-config": "aec", "nousergon-data": "data", "nous-ergon-ops": "ops"}
_REF_RE = re.compile(r"^([A-Za-z0-9_.-]+)#(\d+)$")
#: Director action items shown under the concerns, P0 first.
MAX_DIRECTOR_PLANNED = 3

#: Bullet cap. Telegram's message limit is 4096 characters and the digest
#: above the bullets already uses part of it. Elision is always announced.
MAX_ISSUES = 10
_PROBLEM_MAX_CHARS = 150
_PLAN_MAX_CHARS = 70
_RISK_MAX_CHARS = 160
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
    #: Stable fingerprints for the resolution registry (see RESOLUTIONS_*).
    #: One bullet can carry several (one per declared skip flag).
    keys: Tuple[str, ...] = ()

    @property
    def needs_triage(self) -> bool:
        """False only for a bullet whose auto-fix is "none needed"."""
        return not self.auto_fix.startswith("none needed")


@dataclass(frozen=True)
class Resolution:
    """The planned fix for a problem: the tracked item and what it does."""

    ref: str  # e.g. "alpha-engine-config#7309"
    plan: str


@dataclass(frozen=True)
class DirectorSummary:
    status: Optional[str]  # RED / AMBER / YELLOW / GREEN, or None if unstated
    concerns: Tuple[str, ...]
    source_key: str
    #: (action item id, title, tracked issue number or None), P0 first.
    planned: Tuple[Tuple[str, str, Optional[int]], ...] = ()


@dataclass
class WeeklyReport:
    issues: List[Issue]
    director: Optional[DirectorSummary]
    #: Registry key pattern -> Resolution (bundled file, overlaid from S3).
    resolutions: Mapping[str, Resolution] = field(default_factory=dict)

    def resolutions_for(self, issue: Issue) -> List[Resolution]:
        found: List[Resolution] = []
        for key in issue.keys:
            for pattern, res in self.resolutions.items():
                if fnmatch.fnmatchcase(key, pattern) and res not in found:
                    found.append(res)
                    break
        return found


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


def parse_resolutions(doc: Any) -> dict:
    """Registry document -> {pattern: Resolution}, in file order."""
    out: dict = {}
    entries = doc.get("entries") if isinstance(doc, dict) else None
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        pattern, ref, plan = entry.get("match"), entry.get("ref"), entry.get("plan")
        if isinstance(pattern, str) and isinstance(ref, str) and _REF_RE.match(ref):
            out.setdefault(pattern, Resolution(ref=ref, plan=str(plan or "")))
    return out


def load_bundled_resolutions(path: str = RESOLUTIONS_FILE) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            return parse_resolutions(json.load(fh))
    except (OSError, ValueError) as exc:
        logger.warning("weekly report: bundled resolutions unreadable (%s): %s", path, exc)
        return {}


def ref_link(ref: str) -> str:
    """``alpha-engine-config#7309`` -> a Telegram Markdown link ``[aec#7309](…)``."""
    m = _REF_RE.match(ref)
    if not m:
        return md_escape(ref)
    repo, num = m.group(1), m.group(2)
    label = f"{_REPO_SHORT.get(repo, repo)}#{num}"
    return f"[{md_escape(label)}](https://github.com/nousergon/{repo}/issues/{num})"


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
            keys=tuple(f"skip:{k}" for k in skipped),
        ))

    for flag, step in _DEGRADED_FLAGS:
        if exec_output.get(flag) is True:
            issues.append(Issue(step=step, problem=f"degraded on this run (`{flag}`)", auto_fix=AUTO_FIX_SWEEP,
                                keys=(f"degraded:{flag}",)))

    reasons = (exec_output.get("degraded_summary") or {}).get("reasons") if isinstance(
        exec_output.get("degraded_summary"), dict) else None
    if isinstance(reasons, dict):
        flagged = {step for flag, step in _DEGRADED_FLAGS if exec_output.get(flag) is True}
        for family, reason in sorted(reasons.items()):
            if family in flagged:
                continue
            issues.append(Issue(step=f"Degraded: {family}", problem=md_escape(str(reason)), auto_fix=AUTO_FIX_SWEEP,
                                keys=(f"degraded_reason:{family}",)))

    if exec_output.get("model_zoo_unservable") is True:
        issues.append(Issue(
            step="Model zoo",
            problem="model-zoo selection produced no servable model this week (`model_zoo_unservable`)",
            auto_fix=AUTO_FIX_SWEEP,
            keys=("model_zoo_unservable",),
        ))
    if exec_output.get("director_retro_refused") is True:
        issues.append(Issue(step="Director", problem="retro was refused (`director_retro_refused`)", auto_fix=AUTO_FIX_SWEEP,
                            keys=("director_retro_refused",)))
    if exec_output.get("cost_coverage_gap") is True:
        issues.append(Issue(step="Cost aggregation", problem="cost coverage gap on this run (`cost_coverage_gap`)", auto_fix=AUTO_FIX_SWEEP,
                            keys=("cost_coverage_gap",)))
    blind = exec_output.get("weekly_preflight_blind_spot")
    if isinstance(blind, dict) and blind.get("present") is True:
        names = ", ".join(f"`{n}`" for n in (blind.get("required_skip_names") or [])[:6])
        issues.append(Issue(
            step="Preflight",
            problem=f"{blind.get('required_skip_count', '?')} required check(s) never ran anywhere: {names}",
            auto_fix=AUTO_FIX_SWEEP,
            keys=("preflight_blind_spot",),
        ))
    relaunches = exec_output.get("substrate_relaunch_attempts")
    if isinstance(relaunches, int) and relaunches > 0:
        issues.append(Issue(
            step="Spot box",
            problem=f"the weekly spot box was relaunched {relaunches} time(s) mid-run",
            auto_fix="the pipeline's own relaunch already ran; nothing more is automatic. " + AUTO_FIX_SWEEP,
            keys=("spot_relaunch",),
        ))
    if exec_output.get("notify_error"):
        issues.append(Issue(step="Completion email", problem="the SNS completion notice failed to publish", auto_fix=AUTO_FIX_SWEEP,
                            keys=("completion_notify_failed",)))
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
            keys=(f"stage_output:{row.get('stage')}:{what}",),
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
            keys=(f"coverage:{rec.get('stage')}:{status}",),
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
            keys=(f"walk_forward:{wf.get('status')}",),
        ))
    st = reader.get_json(f"backtest/{run_date}/self_test.json", optional=True)
    if isinstance(st, dict) and str(st.get("verdict") or "").upper() not in ("PASS", ""):
        issues.append(Issue(step="Backtester", problem=f"self-test verdict {md_escape(str(st.get('verdict')))}", auto_fix=AUTO_FIX_SWEEP,
                            keys=("backtest_self_test",)))
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
                keys=(f"health:{doc.get('module') or key.rsplit('/', 1)[-1][:-5]}",),
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
            keys=(f"contamination:{contamination or 'MISSING'}",),
        ))
    for half in ("arithmetic_verdict", "evaluator_verdict", "backtester_verdict", "evaluator_stage_verdict"):
        val = str(att.get(half) or "").upper()
        if att and val and val != "PASS":
            issues.append(Issue(step="Report card", problem=f"attestation `{half}` {val}", auto_fix=AUTO_FIX_SWEEP,
                                keys=(f"attestation:{half}",)))
    if att.get("promotion_withheld") is True:
        issues.append(Issue(step="Report card", problem="promotion withheld by the attestation", auto_fix=AUTO_FIX_SWEEP,
                            keys=("promotion_withheld",)))
    if plan.get("route_degraded") is True:
        issues.append(Issue(
            step="Director",
            problem=md_escape(_clip(f"plan ran on a degraded model route: {plan.get('route_degraded_reason') or ''}", _PROBLEM_MAX_CHARS)),
            auto_fix=AUTO_FIX_SWEEP,
            keys=("director_route_degraded",),
        ))
    if plan.get("advisory_unverified") is True:
        issues.append(Issue(step="Director", problem="plan is advisory-unverified", auto_fix=AUTO_FIX_SWEEP,
                            keys=("director_advisory_unverified",)))
    tel = plan.get("plan_call_telemetry") if isinstance(plan.get("plan_call_telemetry"), dict) else {}
    omitted = tel.get("DirectorPlanCarryoverOmitted")
    if isinstance(omitted, (int, float)) and omitted > 0:
        issues.append(Issue(
            step="Director",
            problem=f"{int(omitted)} of {int(tel.get('DirectorPlanCarryoverItems') or omitted)} carryover items were left out of the plan prompt",
            auto_fix=AUTO_FIX_SWEEP,
            keys=("director_carryover_omitted",),
        ))
    if tel.get("DirectorPlanLatencyAmber") == 1:
        issues.append(Issue(
            step="Director",
            problem=f"plan call slow: {tel.get('DirectorPlanLatencySeconds')}s (amber at {tel.get('DirectorPlanAmberSeconds')}s)",
            auto_fix=AUTO_FIX_SWEEP,
            keys=("director_plan_latency",),
        ))
    planned = tuple(
        (str(a.get("id") or ""), str(a.get("title") or ""), None)
        for a in sorted(
            (a for a in (plan.get("action_items") or []) if isinstance(a, dict)),
            key=lambda a: str(a.get("priority") or "P9"),
        )[:MAX_DIRECTOR_PLANNED]
    )
    return DirectorSummary(status=status, concerns=concerns, source_key=key, planned=planned), issues


def _with_ledger_issues(d: DirectorSummary, ledger: Any) -> DirectorSummary:
    """Fill each planned item's issue number from the Director's own ledger."""
    numbers: dict = {}
    if isinstance(ledger, dict):
        for item in ledger.get("items") or []:
            if isinstance(item, dict) and isinstance(item.get("issue_number"), int):
                numbers[item.get("id")] = item["issue_number"]
    planned = tuple((i, t, numbers.get(i)) for i, t, _ in d.planned)
    return DirectorSummary(status=d.status, concerns=d.concerns, source_key=d.source_key, planned=planned)


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
            keys=("director_retro_latency",),
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
    bundled_resolutions: Optional[Mapping[str, Resolution]] = None,
) -> WeeklyReport:
    exec_issues = issues_from_execution(exec_input, exec_output)
    issues: List[Issue] = []
    director: Optional[DirectorSummary] = None
    bundled = dict(bundled_resolutions if bundled_resolutions is not None else load_bundled_resolutions())
    if s3_client is None:
        return WeeklyReport(issues=exec_issues, director=None, resolutions=bundled)
    if not run_date:
        exec_issues.append(Issue(
            step="Report",
            problem="run date unknown, so stage outputs and the Director report could not be read",
            auto_fix=AUTO_FIX_SWEEP,
            keys=("no_run_date",),
        ))
        return WeeklyReport(issues=exec_issues, director=None, resolutions=bundled)

    reader = _Reader(s3_client, budget_sec=budget_sec, clock=clock)
    resolutions = dict(parse_resolutions(reader.get_json(RESOLUTIONS_S3_KEY, optional=True)))
    for pattern, res in bundled.items():
        resolutions.setdefault(pattern, res)
    plan_key = f"director/{run_date}/action_plan.json"
    plan = reader.get_json(plan_key, optional=True)
    if isinstance(plan, dict):
        director, plan_issues = director_from_plan(plan, plan_key)
        issues.extend(plan_issues)
        if director.planned:
            director = _with_ledger_issues(director, reader.get_json(DIRECTOR_LEDGER_KEY, optional=True))
    elif plan is None and not any(k == plan_key for k, _ in reader.unreadable):
        issues.append(Issue(step="Director", problem="no action plan was written for this run", auto_fix=AUTO_FIX_SWEEP,
                            keys=("director_no_plan",)))
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
            keys=(f"unreadable:{reason}",),
        ))
    return WeeklyReport(issues=exec_issues + unreadable + issues, director=director, resolutions=resolutions)


#: Rendered-character budget for the report section. Telegram's 4096 limit is
#: counted after entity parsing (link URLs are free); the header and state
#: digest above this section take up to ~900.
REPORT_MAX_RENDERED_CHARS = 3100
_LINK_URL_RE = re.compile(r"\]\([^)]*\)")


def rendered_len(lines: Sequence[str]) -> int:
    """Approximate length after Telegram parses Markdown (URLs and escapes drop out)."""
    text = _LINK_URL_RE.sub("]", "\n".join(lines))
    return len(text.replace("\\", "").replace("*", "").replace("[", ""))


def format_report_lines(report: WeeklyReport, *, max_rendered: int = REPORT_MAX_RENDERED_CHARS) -> List[str]:
    """Shrinks the number of bullets shown until the section fits the budget;
    the elision line always says how many were cut."""
    shown = min(MAX_ISSUES, len(report.issues))
    while True:
        lines = _format_report_lines(report, shown)
        if shown <= 1 or rendered_len(lines) <= max_rendered:
            return lines
        shown -= 1


def _format_report_lines(report: WeeklyReport, shown: int) -> List[str]:
    lines: List[str] = []
    issues = report.issues
    if issues:
        lines.append(f"*Issues ({len(issues)}):*")
        for issue in issues[:shown]:
            lines.append(f"• *{md_escape(issue.step)}:* {issue.problem}")
            lines.append(f"  Auto-fix: {issue.auto_fix}")
            found = report.resolutions_for(issue)
            if found:
                for res in found:
                    plan = f" — {md_escape(_clip(res.plan, _PLAN_MAX_CHARS))}" if res.plan else ""
                    lines.append(f"  Resolution: {ref_link(res.ref)}{plan}")
            elif issue.needs_triage:
                lines.append(f"  Resolution: {NO_RESOLUTION}")
        if len(issues) > shown:
            rest = len(issues) - shown
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
        if d.planned:
            lines.append("  Planned fixes (Director action items):")
            for item_id, title, number in d.planned:
                ref = ref_link(f"alpha-engine-config#{number}") if number else "issue not in the ledger yet"
                lines.append(f"  – {md_escape(_clip(title or item_id, _PLAN_MAX_CHARS))}: {ref}")
    return lines


def report_needs_triage(report: WeeklyReport) -> bool:
    return any(i.needs_triage for i in report.issues) or (
        report.director is not None and report.director.status in ("RED", "AMBER", "YELLOW")
    )


def overall_auto_fix_line(report: WeeklyReport) -> str:
    if report_needs_triage(report):
        return f"Auto-fix: {AUTO_FIX_SWEEP}"
    return "Auto-fix: none needed (clean run)."
