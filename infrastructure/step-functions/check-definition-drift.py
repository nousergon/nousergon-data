#!/usr/bin/env python3
"""check-definition-drift.py — Diff the codified Step Function DEFINITIONS
(repo `infrastructure/step_function*.json`) against live AWS state AND the
S3 staged copies CloudFormation reads from.

**Background (alpha-engine-config#2273).** The weekly SF definition existed
as THREE copies with no reconciliation: the repo file (source of truth), the
S3 object CFN's ``DefinitionS3Location`` references (read at stack-create /
resource-replacement time), and the live state machine. Historically
``deploy_step_function.sh`` updated the live machine from the LOCAL file
without refreshing the S3 object — so a stale S3 copy sat armed, and any
future CFN restamp/replacement would silently ROLL BACK the live definition
to whatever the S3 object held. config#2273 codified the single-writer
contract (every deploy path uploads the stamped repo bytes to the CFN key
before ``update-state-machine`` from those same bytes); this script is the
drift BACKSTOP that pages when any of the three copies diverges anyway
(out-of-band console edit, aborted deploy, drive-by S3 write).

Sibling of `infrastructure/step-functions/check-drift.py` (the
LoggingConfiguration drift guard, config#1464) — same shape: standalone,
regex/JSON parsing of repo sources, live state via the AWS CLI, exit 0/1/2.

**Groom-dispatch coverage (alpha-engine-config#2391).** The backlog-groom SF
was silently orphaned from deploys for 11 days because
`deploy-infrastructure.sh` targeted `alpha-engine-groom-pipeline` — a name
this script's own `SF_DEFINITIONS` map ALSO carried, so drift there would
have gone unnoticed even if this script had been running. Both the deploy
script (config#2391-related PR#780) and this map now agree on the live
name, `alpha-engine-groom-dispatch`. The groom SF has no
`LoggingConfiguration` (see check-drift.py's groom entry) but that's
orthogonal to this script: this script only ever diffs the `definition`
JSON (repo vs. live vs. S3-staged), never `loggingConfiguration`, so the
groom SF's lack of logging needs no special-casing here.

**Normalization.** Deploys stamp the top-level ``Comment`` with a
``[git:<sha>] `` prefix (see deploy-infrastructure.sh); the repo file is
unstamped. The comparison strips that stamp from both sides and compares
canonical JSON (sorted keys) — so a stamp-only difference is NOT drift, but
any real Comment/state/field change is.

Drift cases (all exit non-zero):
  * Live definition differs from the repo file (normalized) — the live
    machine was written from something other than the repo HEAD bytes.
  * S3 staged copy differs from the repo file (normalized) — the CFN
    read-source is stale; a CFN restamp would deploy those stale bytes.
  * A codified state machine isn't found on AWS at all (missing-in-aws).
  * The S3 staged object is missing entirely.
  * A repo definition file is missing or malformed JSON (source-error).

On drift, this script also fires an `nousergon_lib.alerts.publish` (same
`alpha-engine-alerts` SNS topic every other drift/preflight alert in this
repo uses — see validators/constituents_drift_check.py) unless `--no-alert`
is passed. The import is lazy and best-effort: a missing/broken
`nousergon_lib` degrades to a logged warning, never to a false "clean"
exit — the non-zero exit code is always the authoritative signal.

**Paging discipline (alpha-engine-config-I9036).** ``REPO_ROOT`` is derived
from ``__file__``, so an out-of-band invocation inside a stale checkout
compares THAT checkout's bytes against live and pages production with
findings that are entirely local delta — measured 2026-08-29, six ERROR
findings from a worktree 49 commits behind ``origin/main``, zero real drift.
Two rules follow, and both are load-bearing:

  * Every page carries the provenance of the "repo" side — ``REPO_ROOT``,
    ``HEAD``, and the commits-behind count — and leads with the
    ``stale_checkout_note`` when there is one. A drift page whose repo side
    is unidentified is unactionable by construction.
  * ``main()`` REFUSES to page from a checkout whose definition files differ
    from the remote's DEFAULT branch. The findings still print and the exit
    code is still 1 — only the page is withheld. Both legitimate callers
    (deploy-infrastructure.yml, sf-arn-drift-check.yml) run on ``main``.

Usage:
  ./infrastructure/step-functions/check-definition-drift.py               # every codified SF, alerts on drift
  ./infrastructure/step-functions/check-definition-drift.py --name NAME   # one (by SF name)
  ./infrastructure/step-functions/check-definition-drift.py --no-alert    # diagnostic — no SNS/Telegram

Requires AWS creds with states:DescribeStateMachine on the target state
machines and s3:GetObject on s3://alpha-engine-research/infrastructure/*.
Wiring: runs as a step in `.github/workflows/deploy-infrastructure.yml`
(every push to main, right after the deploy applies) — reuses that job's
`github-actions-lambda-deploy` OIDC role, which already carries
states:DescribeStateMachine on `alpha-engine-groom-dispatch` plus
sns:Publish on `alpha-engine-alerts`. Still standalone-callable from
liveness sweeps / operator sessions too, same as its check-drift.py sibling.
Shape-guarded by tests/test_sf_definition_check_drift.py (mocked CLI — no
real AWS access in CI).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
INFRA_DIR = REPO_ROOT / "infrastructure"

# Fallback defaults used only to build the ARN this script queries AWS with
# (mirrors step-functions/check-drift.py's same constants).
DEFAULT_REGION = "us-east-1"
DEFAULT_ACCOUNT_ID = "711398986525"

# The S3 bucket/prefix every deploy path stages definitions to — MUST match
# deploy-infrastructure.sh ($BUCKET) and the CFN DefinitionS3Location keys.
S3_BUCKET = "alpha-engine-research"
S3_PREFIX = "infrastructure/"

# Last-resort ref for the staleness comparison when git cannot name the
# remote's default branch. Never the tracked branch: see default_upstream().
DEFAULT_UPSTREAM = "origin/main"

# repo definition file -> live state machine name. Mirrors the ARN mapping in
# deploy-infrastructure.sh step 3. A renamed/removed file or SF fails loud
# below (source-error / missing-in-aws) rather than silently dropping out.
SF_DEFINITIONS: tuple[dict, ...] = (
    {"sf_name": "ne-weekly-freshness-pipeline", "definition_file": "step_function.json"},
    {"sf_name": "ne-preopen-trading-pipeline", "definition_file": "step_function_daily.json"},
    {"sf_name": "ne-postclose-trading-pipeline", "definition_file": "step_function_eod.json"},
    {"sf_name": "ne-postclose-reconcile-pipeline", "definition_file": "step_function_eod_reconcile.json"},
    {"sf_name": "alpha-engine-groom-dispatch", "definition_file": "step_function_groom.json"},
    # alpha-engine-config-I2890 (2026-07-17): the I2544/I2545 advisory +
    # Sunday-modelzoo child SFs were RETIRED (splits reversed) — the weekly SF
    # carries the full inline pattern in step_function.json again.
)

_GIT_STAMP_RE = re.compile(r"^\[git:[0-9a-fA-F]{7,40}\]\s*")


def _normalized_dict(definition: dict) -> dict:
    """Deep copy with the git-stamp stripped from the top-level Comment —
    the ONLY tolerated difference between the repo file and deployed copies."""
    d = json.loads(json.dumps(definition))  # deep copy — never mutate input
    comment = d.get("Comment")
    if isinstance(comment, str):
        d["Comment"] = _GIT_STAMP_RE.sub("", comment)
    return d


def _normalize(definition: dict) -> str:
    """Canonical form for comparison: git-stamp stripped from the top-level
    Comment, keys sorted, whitespace-free dump."""
    return json.dumps(_normalized_dict(definition), sort_keys=True, separators=(",", ":"))


def _diff_summary(expected: dict, actual: dict) -> str:
    """Human-oriented pointer at WHERE two definitions diverge (top-level
    keys; differing state names when States is the divergent key). Callers
    pass stamp-stripped (_normalized_dict) copies so the git stamp never
    reads as a Comment divergence."""
    expected, actual = _normalized_dict(expected), _normalized_dict(actual)
    parts: list[str] = []
    keys = sorted(set(expected) | set(actual))
    for key in keys:
        if json.dumps(expected.get(key), sort_keys=True) == json.dumps(actual.get(key), sort_keys=True):
            continue
        if key == "States" and isinstance(expected.get(key), dict) and isinstance(actual.get(key), dict):
            exp_states, act_states = expected[key], actual[key]
            differing = sorted(
                name
                for name in set(exp_states) | set(act_states)
                if json.dumps(exp_states.get(name), sort_keys=True)
                != json.dumps(act_states.get(name), sort_keys=True)
            )
            shown = ", ".join(differing[:5]) + (" …" if len(differing) > 5 else "")
            parts.append(f"States ({len(differing)} differing: {shown})")
        else:
            parts.append(key)
    return "; ".join(parts) if parts else "<no top-level divergence found — nested/ordering?>"


def _aws_cli(*args: str, allow_missing_patterns: tuple[str, ...] = ()):
    """Run an AWS CLI command; return raw stdout, None when the error matches
    an allow_missing pattern, or hard-exit 2 on any other failure (a broken
    CLI/creds state must never read as 'no drift')."""
    result = subprocess.run(
        ["aws", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if any(pat in result.stderr for pat in allow_missing_patterns):
            return None
        sys.stderr.write(
            f"AWS CLI failed: aws {' '.join(args)}\n"
            f"stderr: {result.stderr}\n"
        )
        sys.exit(2)
    return result.stdout


def _fetch_live_definition(sf_name: str) -> dict | None:
    """Live definition dict, or None when the state machine doesn't exist."""
    arn = f"arn:aws:states:{DEFAULT_REGION}:{DEFAULT_ACCOUNT_ID}:stateMachine:{sf_name}"
    out = _aws_cli(
        "stepfunctions",
        "describe-state-machine",
        "--state-machine-arn",
        arn,
        "--output",
        "json",
        allow_missing_patterns=("StateMachineDoesNotExist", "ResourceNotFoundException"),
    )
    if out is None:
        return None
    desc = json.loads(out)
    return json.loads(desc["definition"])


def _fetch_s3_definition(key: str) -> dict | None:
    """S3 staged definition dict, or None when the object doesn't exist."""
    out = _aws_cli(
        "s3",
        "cp",
        f"s3://{S3_BUCKET}/{key}",
        "-",
        allow_missing_patterns=("Not Found", "NoSuchKey", "404", "does not exist"),
    )
    if out is None:
        return None
    return json.loads(out)


def _git(*args: str) -> str | None:
    """``git`` output, or ``None`` if git is unavailable / the command fails
    (not a git checkout, no remote-tracking ref, shallow clone). Never raises:
    the staleness note is an aid, and its absence must never break a real
    drift check."""
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True, text=True, check=False,
        )
    except (OSError, ValueError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def default_upstream() -> str:
    """The remote's DEFAULT branch (e.g. ``origin/main``), never the branch
    this checkout happens to track.

    ``stale_definition_files`` originally resolved ``@{upstream}``, which on a
    feature branch resolves to that feature branch's own remote ref — so a
    checkout tens of commits behind ``main`` compares clean against itself and
    the guard reports nothing. That is the exact configuration that produced a
    false production page on 2026-08-29 from a worktree 49 commits behind
    ``origin/main``. When this script is deciding whether it may PAGE, the only
    meaningful reference is the branch the deployment is built from.

    Falls back to ``DEFAULT_UPSTREAM`` when git cannot answer (a CI checkout
    does not always carry ``refs/remotes/origin/HEAD``)."""
    for args in (
        ("rev-parse", "--abbrev-ref", "origin/HEAD"),
        ("symbolic-ref", "--short", "refs/remotes/origin/HEAD"),
    ):
        ref = _git(*args)
        if ref and "/" in ref:
            return ref
    return DEFAULT_UPSTREAM


def repo_provenance(upstream: str | None = None) -> dict:
    """WHICH bytes this run called "the repo".

    A drift page whose repo side is unidentified is unactionable by
    construction: the reader cannot tell a live hand-patch from an operator
    running the checker inside a stale checkout. Every field is best-effort —
    git may be absent — but an unknown is rendered as the literal string
    ``unknown``, never omitted, so the page never reads as if the question was
    not asked."""
    upstream = upstream or default_upstream()
    head = _git("rev-parse", "HEAD") or "unknown"
    behind_raw = _git("rev-list", "--count", f"HEAD..{upstream}")
    try:
        behind = int(behind_raw) if behind_raw is not None else None
    except ValueError:
        behind = None
    return {
        "repo_root": str(REPO_ROOT.resolve()),
        "head": head,
        "upstream": upstream,
        "behind": behind,
    }


def repo_provenance_note(provenance: dict) -> str:
    """One line naming REPO_ROOT, HEAD and the behind-count. Pure / testable."""
    behind = provenance.get("behind")
    behind_txt = (
        f"{behind} commits behind {provenance['upstream']}"
        if behind is not None
        else f"behind-count vs {provenance['upstream']} unknown"
    )
    return (
        f"repo side: {provenance['repo_root']} @ HEAD {provenance['head']} "
        f"({behind_txt})"
    )


def stale_definition_files(entries, upstream: str | None = None) -> list[str]:
    """Definition files whose WORKING-TREE bytes differ from ``origin/HEAD``'s.

    This script's entire premise is "the repo file is the intended truth" — so
    when the working tree does not match the pushed branch, every finding it
    produces is suspect: the difference may be local staleness rather than live
    drift. On 2026-07-27 a checkout 9 commits behind origin produced four
    confident findings (an alleged 18-state divergence in the weekly trading
    pipeline) that were entirely the local delta; live, S3 and origin/main all
    agreed. A tool whose job is comparing repo bytes to live must not report
    drift from bytes it cannot vouch for.

    Deliberately does NOT fetch — a checker must not mutate git state, and a
    stale remote-tracking ref still catches the common case (a checkout that
    was never pulled). Deliberately does NOT fail: comparing an in-flight
    branch's definitions against live before merging is a legitimate use, and
    that case would trip this every time. It annotates instead.

    ``upstream`` names the ref to compare against. ``None`` resolves the
    tracked remote branch (the legacy annotate-only behaviour); ``main()``
    always passes :func:`default_upstream` instead, because on a feature
    branch the tracked ref IS the stale bytes and comparing against it can
    never detect staleness (alpha-engine-config-I9036, deliverable 4).

    Returns repo-relative paths, sorted. Empty when git is unavailable — an
    unknown answer is reported as "no note", never as a false reassurance,
    because ``main()`` prints the note only when this is non-empty."""
    if _git("rev-parse", "--is-inside-work-tree") != "true":
        return []
    if upstream is None:
        # Annotate-only callers keep the historical behaviour: prefer the
        # tracked remote branch. main() NEVER relies on this — it passes
        # default_upstream() explicitly, because a tracked feature branch is
        # exactly the ref that makes a stale checkout look current.
        upstream = (_git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
                    or DEFAULT_UPSTREAM)
    stale: list[str] = []
    for entry in SF_DEFINITIONS:
        rel = (INFRA_DIR / entry["definition_file"]).relative_to(REPO_ROOT)
        local = _git("hash-object", str(rel))
        remote = _git("rev-parse", f"{upstream}:{rel}")
        if local and remote and local != remote:
            stale.append(str(rel))
    return sorted(set(stale))


def stale_checkout_note(stale: list[str], upstream: str = "origin/main") -> str:
    """The operator-facing warning. Pure / testable."""
    files = ", ".join(stale)
    return (
        f"⚠️  LOCAL CHECKOUT MAY BE STALE — {len(stale)} definition file(s) "
        f"differ from {upstream}: {files}\n"
        f"    Findings below compare LOCAL bytes against live/S3, so a "
        f"'drift' here may be your checkout, not the deployment.\n"
        f"    Run `git pull` and re-check before acting on anything below."
    )


def _check_sf(entry: dict) -> list[str]:
    sf_name = entry["sf_name"]
    definition_path = INFRA_DIR / entry["definition_file"]
    source_rel = definition_path.relative_to(REPO_ROOT)

    if not definition_path.is_file():
        return [
            f"{sf_name}: codified definition {source_rel} not found — has the "
            f"file been renamed without updating SF_DEFINITIONS in this script?"
        ]
    try:
        repo_def = json.loads(definition_path.read_text())
    except json.JSONDecodeError as exc:
        return [f"{sf_name}: {source_rel} is not valid JSON ({exc})"]

    repo_norm = _normalize(repo_def)
    findings: list[str] = []

    # ── live vs repo ─────────────────────────────────────────────────────
    live_def = _fetch_live_definition(sf_name)
    if live_def is None:
        findings.append(
            f"{sf_name}: codified in {source_rel} but state machine not found "
            f"on AWS (renamed/recreated without updating the source, or vice "
            f"versa?)"
        )
    elif _normalize(live_def) != repo_norm:
        findings.append(
            f"{sf_name}: definition drift (LIVE vs {source_rel}) — the live "
            f"state machine was not written from the repo bytes. Diverges at: "
            f"{_diff_summary(repo_def, live_def)}"
        )

    # ── S3 staged copy vs repo ───────────────────────────────────────────
    s3_key = f"{S3_PREFIX}{entry['definition_file']}"
    s3_def = _fetch_s3_definition(s3_key)
    if s3_def is None:
        findings.append(
            f"{sf_name}: staged copy s3://{S3_BUCKET}/{s3_key} is missing — "
            f"a CFN stack-create/replacement would fail (or read nothing); "
            f"run a deploy to restore it."
        )
    else:
        try:
            s3_drifted = _normalize(s3_def) != repo_norm
        except (TypeError, ValueError) as exc:
            findings.append(f"{sf_name}: s3://{S3_BUCKET}/{s3_key} unparseable ({exc})")
            s3_drifted = False
        if s3_drifted:
            findings.append(
                f"{sf_name}: definition drift (S3 staged copy vs {source_rel}) "
                f"— s3://{S3_BUCKET}/{s3_key} is stale; a future CFN "
                f"restamp/replacement would silently roll the live definition "
                f"back to those bytes (config#2273). Diverges at: "
                f"{_diff_summary(repo_def, s3_def)}"
            )

    return findings


def _dedup_key(total_findings: list[str]) -> str:
    """Dedup key over the finding CONTENT, not its count.

    ``sf_definition_drift_{len(findings)}`` collapsed every 6-finding
    condition into one page: an unrelated drift arriving inside the dedup
    window while a first one was open would have been silently dropped. The
    count is a property of the alert, never an identity of the condition."""
    digest = hashlib.sha256(
        "\n".join(sorted(total_findings)).encode("utf-8")
    ).hexdigest()[:16]
    return f"sf_definition_drift_{digest}"


def _alert_on_drift(
    total_findings: list[str],
    *,
    severity: str = "error",
    provenance: dict | None = None,
    stale_note: str = "",
) -> None:
    """Best-effort SNS/Telegram page via nousergon_lib.alerts — same
    `alpha-engine-alerts` topic every other drift/preflight alert in this
    repo publishes to (see validators/constituents_drift_check.py). Import
    is lazy so this script still runs (and still exits non-zero) in any
    environment where nousergon_lib isn't installed; a broken/missing
    alerts path is logged, never swallowed into a false-clean result."""
    try:
        from nousergon_lib import alerts  # noqa: PLC0415
    except ImportError as exc:
        sys.stderr.write(
            f"WARNING: alerts publish skipped — nousergon_lib.alerts "
            f"unavailable: {exc}\n"
        )
        return

    # The page must carry WHICH repo bytes produced the findings, and must
    # lead with the stale-checkout warning when one exists — the guard being
    # visible on stdout while stripped from the page is what let a stale
    # worktree page production on 2026-08-29.
    provenance = provenance if provenance is not None else repo_provenance()
    header = "" if not stale_note else stale_note + "\n"
    message = (
        header
        + f"SF definition drift detected ({len(total_findings)} finding(s)) — "
        + repo_provenance_note(provenance)
        + ": "
        + "; ".join(total_findings)
    )
    try:
        result = alerts.publish(
            message,
            severity=severity,
            source="alpha-engine-data/infrastructure/step-functions/check-definition-drift.py",
            dedup_key=_dedup_key(total_findings),
            dedup_window_min=60,
        )
        sys.stderr.write(
            f"Drift alert publish: sns_ok={result.sns.ok} "
            f"telegram_ok={result.telegram.ok} any_ok={result.any_ok}\n"
        )
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"WARNING: drift alert publish failed: {exc}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--name", help="Check one state machine by name (default: every codified one)"
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="diagnostic mode — no SNS/Telegram alert on drift",
    )
    args = parser.parse_args()

    entries = list(SF_DEFINITIONS)

    if args.name:
        entries = [e for e in entries if e["sf_name"] == args.name]
        if not entries:
            sys.stderr.write(
                f"ERROR: no codified definition mapping for state machine "
                f"'{args.name}'\n"
            )
            return 2

    total_findings: list[str] = []
    for entry in entries:
        total_findings.extend(_check_sf(entry))

    if total_findings:
        # The paging decision compares against the remote's DEFAULT branch,
        # never the tracked one — see default_upstream().
        upstream = default_upstream()
        provenance = repo_provenance(upstream)
        # Printed BEFORE the findings, so it cannot be missed by someone who
        # reads the first line and starts acting (which is exactly what
        # happened on 2026-07-27).
        stale = stale_definition_files(entries, upstream=upstream)
        stale_note = stale_checkout_note(stale, upstream=upstream) if stale else ""
        if stale_note:
            print(stale_note)
        print(repo_provenance_note(provenance))
        print(f"SF definition drift detected ({len(total_findings)} finding(s)):")
        for f in total_findings:
            print(f"  - {f}")
        if args.no_alert:
            return 1
        if stale:
            # DELIBERATE NON-PAGE — a refusal, not a swallow. Nothing is
            # discarded: the findings are on stdout above, the stale note
            # leads them, and the exit code stays 1, so every caller's
            # failure signal is intact. What is withheld is the PAGE, because
            # findings derived from bytes that were never on the default
            # branch cannot distinguish deployment drift from local staleness
            # — §2.4's premise ("the repo is the only writer") does not hold
            # for a checkout that is not the repo's default branch. Both
            # legitimate callers run on main, so no true positive is lost.
            # Recording surface: stdout + stderr below, and exit code 1.
            sys.stderr.write(
                "REFUSING TO PAGE: findings were produced from a stale "
                f"checkout ({len(stale)} definition file(s) differ from "
                f"{upstream}). {repo_provenance_note(provenance)}. "
                "Exit code 1 stands; re-run from a checkout at "
                f"{upstream} to page.\n"
            )
            print(
                "REFUSING TO PAGE: stale checkout — see the warning above. "
                "Exit code 1 stands."
            )
            return 1
        _alert_on_drift(total_findings, provenance=provenance, stale_note=stale_note)
        return 1

    sf_names = ", ".join(e["sf_name"] for e in entries)
    print(f"OK: repo, live, and S3 staged definitions all match for {sf_names}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
