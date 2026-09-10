#!/usr/bin/env python3
"""PR-time guard: a PR that adds an UNREGISTERED alert source fails ITS OWN CI.

``alpha-engine-config-I7896`` deliverable 2. Extends the mechanism
``alpha-engine-config-I7860`` / ``-PR7871`` established for the fleet
observability registry ("a PR that adds an unregistered component fails its own
check") to the second registry with the same cross-repo shape: ``alert_classes``
in ``infrastructure/overseer/playbooks.yaml``.

THE DEFECT
----------
An alert emitter (``publish_observe_alert(source=...)`` and friends) lives in a
``crucible-*`` repo; the row that declares it lives here. Neither repo's own
merge-time CI can see the pair, so the producer PR is green, the registry PR is
green, and the drift only surfaces afterwards on a THIRD repo's ``main`` —
``alpha-engine-config``'s push-to-main sweep. A red ``main`` there silently
EJECTS every entry from that repo's merge queue while each queued PR keeps
reporting a healthy state, so a missing playbook row in one repo blocks merges
in another with no message anywhere naming the cause.

Measured 2026-08-20 — twice in one session, same shape, ~2h of fleet merge
throughput: ``research:cut_promotion`` (``crucible-research-PR673`` →
``alpha-engine-config-I7876``) and ``aggregate_costs_handler:corpus_stats``
(``alpha-engine-config-I7896``).

WHAT THIS DOES
--------------
Runs ``alert_class_registry_drift.py`` — the SAME scanner the fleet sweep runs,
moved here beside the registry rather than duplicated (``policy-shared-code``)
— against ONE repo's checkout at the PR's BASE and at its HEAD, and fails only
on the DELTA. Pre-existing drift never blocks an unrelated PR; that is the
distinction ``observability_registry_pr_guard.py`` draws in its own docstring,
and it is what keeps the guard from being routed around on day one.

Diffing the two SCAN RESULTS, rather than the changed files, is deliberate and
covers both directions of the defect:

  * HEAD adds an emitter with no row  → uncovered at HEAD, absent at BASE.
  * HEAD *removes a row* that a live emitter still needs (only reachable when
    the graded repo IS this one) → the base scan uses the BASE playbooks and
    the head scan uses the HEAD playbooks, so the emitter is covered at BASE
    and uncovered at HEAD. A file-path diff would see a one-line YAML edit and
    conclude nothing.

THE COMPANION-PR CYCLE, HANDLED FROM THE START
----------------------------------------------
The ``I7860`` guard produced a CIRCULAR red on its first live day:
``alpha-engine-config-PR7820`` was blocked on a row that lived in
``nous-ergon-ops``, and the ``nous-ergon-ops`` PR carrying that row was blocked
by a reconcile reading ``alpha-engine-config@main``, where the component did not
exist yet. Each PR was red because the other had not merged. Two things prevent
that here, and both are structural rather than hopeful:

1. **The dependency graph is a DAG, and this guard adds no reverse edge.**
   Emitter repos read this registry at PR time; this repo runs NO PR-time check
   that reads an emitter repo. (``alpha-engine-config``'s fleet sweep still
   reads every repo, but it is push-to-main only — a backstop, never a merge
   gate.) A row PR here therefore can always merge, whatever the emitter PR is
   doing, so even a hard red on the emitter side resolves in one direction.

2. **An OPEN companion PR in this repo already satisfies the guard.** The
   registry is read as the union of ``playbooks.yaml`` at the ref CI checked
   out AND ``playbooks.yaml`` at the head of every open PR in this repo that
   changes it. The emitter PR goes green the moment the row PR EXISTS —
   merge order is genuinely free, in either direction, with no second round of
   CI needed on either side.

   The tolerance the ``I7860`` mechanism carried (``pending_component_pr``)
   failed live because its resolver logged
   ``REST lookup ... failed (HTTPError); falling back to gh`` and then returned
   "unknown", which the caller read as "not pending". So here: the lookup tries
   stdlib ``urllib`` first (present on BOTH halves of the ``CI_RUNNER_MODE``
   toggle; ``gh`` is not on the CodeBuild AL2023 image) and ``gh`` second, and
   if BOTH fail it raises. **An unavailable tolerance fails the guard as
   ``UNMEASURED``; it never silently degrades into "no violation".** That is
   safe precisely because of (1): the fix is still one merge away in a
   direction nothing is blocking.

A DISCOVERER THAT CANNOT READ FAILS AS UNMEASURED, NEVER AS CLEAN
-----------------------------------------------------------------
Missing checkout, missing ``playbooks.yaml``, a git operation that fails, an
unreadable source file, an unavailable pending-PR lookup: every one of these
exits non-zero with ``UNMEASURED``. Zero-found and cannot-look never share an
exit code.

Exit 0: this PR introduced no new unregistered alert source (or every new one
        is already carried by an open companion PR here).
Exit 1: it did — or the guard could not measure.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, TypeVar

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alert_class_registry_drift as scan  # noqa: E402

_T = TypeVar("_T")

_GIT = shutil.which("git") or "/usr/bin/git"
_GH = shutil.which("gh") or "gh"

#: Path of the registry file, relative to this repo's root.
PLAYBOOKS_REL = Path("infrastructure/overseer/playbooks.yaml")

#: The repo that owns the registry. Also the repo this file lives in.
REGISTRY_REPO = "nousergon/nousergon-data"

#: Cap on how many open PRs the pending-companion lookup will examine. Beyond
#: this the lookup RAISES rather than truncating — a truncated scan that finds
#: nothing is indistinguishable from a complete one that finds nothing.
_MAX_OPEN_PRS = 100


class GuardError(RuntimeError):
    """The guard could not measure. Always fatal — never degraded into a pass."""


def _run_git(args: list[str], cwd: Path | None = None) -> str:
    p = subprocess.run(  # noqa: S603 — argv is ours, no shell
        [_GIT, *args], cwd=cwd, capture_output=True, text=True, check=False, timeout=120
    )
    if p.returncode != 0:
        raise GuardError(f"git {' '.join(args)} failed: {p.stderr.strip()[:400]}")
    return p.stdout


# ── registry loading ────────────────────────────────────────────────────────


def _patterns_from(playbooks: Path) -> list[tuple[str, str, bool]]:
    if not playbooks.is_file():
        raise GuardError(f"playbooks.yaml not readable at {playbooks}")
    try:
        classes = scan._load_alert_classes(playbooks)  # noqa: SLF001 — reused, not reimplemented
    except Exception as exc:  # noqa: BLE001 — re-raised as UNMEASURED, never swallowed
        raise GuardError(f"playbooks.yaml at {playbooks} failed to parse: {exc}") from exc
    if not classes:
        raise GuardError(
            f"playbooks.yaml at {playbooks} declares no alert_classes — an empty "
            "registry would mark every emitter in the fleet as drift"
        )
    return scan._build_registry_patterns(classes)  # noqa: SLF001


# ── scanning one repo at one ref ────────────────────────────────────────────


def scan_at_ref(repo_root: Path, ref: str, fn: Callable[[Path], _T]) -> _T:
    """Run ``fn`` against ``repo_root``'s tree as it stands at ``ref``.

    ``ref`` resolving to the checkout's own HEAD is read in place; any other
    ref is materialized as a detached ``git worktree`` inside ``repo_root``,
    read, and torn down — the same mechanism ``observability_registry_pr_guard``
    uses, and the reason the caller needs ``fetch-depth: 0``.

    Public and generic because ``alert_message_lint`` needs exactly this
    base-vs-head materialization and must not carry a second copy of it
    (``policy-shared-code``): a fork of the worktree teardown is a fork of the
    ``worktree remove --force`` that keeps a failed run from leaving the graded
    checkout with a stale registration.
    """
    head_sha = _run_git(["rev-parse", "HEAD"], cwd=repo_root).strip()
    resolved = _run_git(["rev-parse", ref], cwd=repo_root).strip()
    if resolved == head_sha:
        return fn(repo_root)

    with tempfile.TemporaryDirectory(prefix="alert-pr-guard-") as tmp:
        wt = Path(tmp) / "base"
        _run_git(["worktree", "add", "--detach", "--quiet", str(wt), resolved], cwd=repo_root)
        try:
            return fn(wt)
        finally:
            _run_git(["worktree", "remove", "--force", str(wt)], cwd=repo_root)


def _scan_at(repo_root: Path, ref: str, playbooks_override: Path | None) -> dict[str, set[str]]:
    """Uncovered source literals in ``repo_root`` at ``ref``.

    When the registry file itself lives INSIDE the graded repo (this repo
    grading its own PRs), the ref's OWN copy of ``playbooks.yaml`` is used.
    Otherwise ``playbooks_override`` — the registry as CI checked it out —
    applies at both refs, because the graded repo does not contain it.
    """
    def _do(root: Path) -> dict[str, set[str]]:
        pb = playbooks_override if playbooks_override is not None else root / PLAYBOOKS_REL
        return scan._scan_repo(root, _patterns_from(pb))  # noqa: SLF001

    return scan_at_ref(repo_root, ref, _do)


# ── the pending open companion PR tolerance ─────────────────────────────────


def _api_get(path: str) -> object:
    """GitHub REST GET, urllib first and ``gh`` second. Raises on both failing.

    urllib FIRST because ``gh`` is absent from the CodeBuild AL2023 runner
    image while urllib is stdlib on both halves of the ``CI_RUNNER_MODE``
    toggle — the exact asymmetry that made the ``I7860`` tolerance pass on a
    laptop and fail in CI. ``gh`` remains the fallback for an interactive run
    with no token exported.
    """
    # GH_TOKEN ONLY, deliberately -- not GITHUB_TOKEN. This repo pins
    # `GITHUB_TOKEN` in `tests/test_no_secret_environ_reads.py`'s
    # `_PINNED_SECRETS`: every SSM-backed secret must route through
    # `nousergon_lib.secrets.get_secret()`, never `os.environ`. The token this
    # function wants is the RUNNER's ephemeral per-job token, which does not
    # exist in SSM and which `get_secret()` cannot supply -- so rather than
    # widen that invariant with an allowlist entry, the workflow passes it
    # explicitly as GH_TOKEN. An ambient token is not silently picked up, which
    # also makes the credential this check needs visible in the workflow file.
    token = (os.environ.get("GH_TOKEN") or "").strip()
    rest_error = "no GH_TOKEN in the environment (the caller workflow must set it)"
    if token:
        import urllib.error  # noqa: PLC0415 — only needed on this path
        import urllib.request  # noqa: PLC0415

        req = urllib.request.Request(  # noqa: S310 — literal https host
            f"https://api.github.com{path}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "nousergon-data-alert-class-pr-guard",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
                return json.loads(resp.read().decode())
        except Exception as exc:  # noqa: BLE001 — fall through to gh, then RAISE
            rest_error = f"{type(exc).__name__}: {exc}"

    try:
        out = subprocess.run(  # noqa: S603 — argv is ours, no shell
            [_GH, "api", path], capture_output=True, text=True, check=False, timeout=60
        )
    except Exception as exc:  # noqa: BLE001
        raise GuardError(
            f"pending-companion-PR lookup of {path} failed: REST ({rest_error}) and "
            f"gh ({type(exc).__name__}: {exc})"
        ) from exc
    if out.returncode != 0:
        raise GuardError(
            f"pending-companion-PR lookup of {path} failed: REST ({rest_error}) and "
            f"gh (exit {out.returncode}: {out.stderr.strip()[:200]})"
        )
    return json.loads(out.stdout or "null")


def pending_registry_patterns(registry_repo: str) -> tuple[list[tuple[str, str, bool]], dict[str, int]]:
    """Registry rows carried by OPEN PRs in ``registry_repo`` but not yet merged.

    Returns ``(patterns, {source: pr_number})``. Raises ``GuardError`` when the
    lookup cannot be completed — an unknown tolerance is never a granted one.
    """
    prs = _api_get(f"/repos/{registry_repo}/pulls?state=open&per_page={_MAX_OPEN_PRS}")
    if not isinstance(prs, list):
        raise GuardError(f"unexpected shape from /repos/{registry_repo}/pulls: {type(prs).__name__}")
    if len(prs) >= _MAX_OPEN_PRS:
        raise GuardError(
            f"{registry_repo} has >= {_MAX_OPEN_PRS} open PRs — this lookup would be "
            "truncated, and a truncated scan that finds nothing reads exactly like a "
            "complete one that finds nothing"
        )

    patterns: list[tuple[str, str, bool]] = []
    origin: dict[str, int] = {}
    import base64  # noqa: PLC0415

    for pr in prs:
        number = pr.get("number")
        sha = (pr.get("head") or {}).get("sha")
        if not number or not sha:
            continue
        try:
            blob = _api_get(
                f"/repos/{registry_repo}/contents/{PLAYBOOKS_REL.as_posix()}?ref={sha}"
            )
        except GuardError:
            # The file may legitimately not exist on some head, but a lookup
            # that FAILED is not the same fact. Re-raise: this whole function
            # is all-or-nothing by design.
            raise
        if not isinstance(blob, dict) or "content" not in blob:
            continue
        try:
            text = base64.b64decode(blob["content"]).decode("utf-8", errors="replace")
            import yaml  # noqa: PLC0415

            classes = (yaml.safe_load(text) or {}).get("alert_classes") or []
        except Exception as exc:  # noqa: BLE001
            raise GuardError(
                f"{registry_repo}#{number}'s playbooks.yaml could not be parsed: {exc}"
            ) from exc
        for pat in scan._build_registry_patterns(classes):  # noqa: SLF001
            patterns.append(pat)
            origin.setdefault(pat[1], number)
    return patterns, origin


# ── the paste-ready row ─────────────────────────────────────────────────────


def class_name_for(source: str) -> str:
    """A ``^[a-z0-9_]{3,60}$`` class name derived from the source literal."""
    slug = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")
    slug = re.sub(r"_+", "_", slug) or "alert_class"
    if len(slug) < 3:
        slug = f"{slug}_alert"
    return slug[:60].strip("_")


#: Suggested tier for a newly-detected source, by the emitted severity. This
#: is a STARTING POINT the author must justify, not a verdict: the tier is a
#: statement about ACTIONABILITY (is something broken?), and severity is only
#: the best evidence available before a human has read the call site
#: (alpha-engine-config-I6751; observability-policy.md 7.1 — derived where
#: derivable, declared where not). `dynamic` is suggested where the emitter
#: picks its severity at runtime, which is exactly when it is not derivable
#: here.
_SUGGESTED_TIER: dict[str, str] = {
    "critical": "page",
    "alarm": "page",
    "error": "notify-silent",
    "warning": "tracked-only",
    "info": "tracked-only",
    "dynamic": "dynamic",
}


def row_yaml(source: str, severity: str) -> str:
    tier = _SUGGESTED_TIER.get(severity, "page")
    return (
        f"  - class: {class_name_for(source)}\n"
        f"    source: {source}\n"
        f"    severities: [{severity}]\n"
        f"    intake: bus\n"
        f"    response: drain-queue\n"
        f"    # tier suggested from severity={severity!r}; CONFIRM it against\n"
        f"    # actionability -- page only if something is BROKEN and cannot\n"
        f"    # wait for the next console look (alpha-engine-config-I6751).\n"
        f"    tier: {tier}\n"
    )


def _severities_for(repo_root: Path, rel_files: set[str]) -> dict[str, str]:
    """Best-effort severity per source, read from the emitting files themselves."""
    out: dict[str, str] = {}
    for rel in sorted(rel_files):
        f = repo_root / rel
        if not f.is_file():
            continue
        try:
            out.update(scan.find_publish_sites(f.read_text(encoding="utf-8", errors="replace")))
        except OSError as exc:
            raise GuardError(f"UNREADABLE: {f} ({exc})") from exc
    return out


# ── main ────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--repo", required=True, help="canonical name of the repo being graded")
    ap.add_argument("--repo-root", required=True, help="checkout of --repo, with full history")
    ap.add_argument("--base", required=True, help="git ref for the PR's base sha")
    ap.add_argument("--head", default="HEAD", help="git ref for the PR's head (default: HEAD)")
    ap.add_argument(
        "--playbooks", default=None,
        help="path to the registry. Default: this file's own repo copy. Omit when the "
             "graded repo IS the registry repo, so each ref uses its own copy.",
    )
    ap.add_argument("--registry-repo", default=REGISTRY_REPO)
    ap.add_argument(
        "--no-pending-tolerance", action="store_true",
        help="do not consult open PRs in the registry repo (tests, and a deliberate "
             "strict local run)",
    )
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    if not (repo_root / ".git").exists():
        print(
            f"UNMEASURED: {repo_root} is not a git checkout — this guard would otherwise "
            "report green having verified nothing",
            file=sys.stderr,
        )
        return 1

    graded_is_registry_repo = (repo_root / PLAYBOOKS_REL).is_file() and args.playbooks is None
    override = None if graded_is_registry_repo else Path(
        args.playbooks or (Path(__file__).resolve().parent.parent.parent / PLAYBOOKS_REL)
    ).resolve()

    print(f"grading: {args.repo} at {repo_root}")
    print(
        "registry: "
        + ("each ref's own infrastructure/overseer/playbooks.yaml (this IS the registry repo)"
           if graded_is_registry_repo else str(override))
    )
    print("scope: publish source literals only — alert_classes rows, nothing else.")

    try:
        head_bad = _scan_at(repo_root, args.head, override)
        base_bad = _scan_at(repo_root, args.base, override)
    except GuardError as exc:
        print(f"UNMEASURED: {exc}", file=sys.stderr)
        print("A guard that cannot read its substrate fails as unmeasured, never as clean.",
              file=sys.stderr)
        return 1

    newly = {src: files for src, files in head_bad.items() if src not in base_bad}
    carried = sorted(set(head_bad) & set(base_bad))
    if carried:
        print(
            f"pre-existing drift NOT gated by this PR ({len(carried)}): {', '.join(carried)}\n"
            "  (the alpha-engine-config fleet sweep owns those — this guard only fails on "
            "what THIS diff introduced)"
        )

    if not newly:
        print("no newly-unregistered alert source in this diff — nothing for this guard to fail.")
        return 0

    tolerated: dict[str, int] = {}
    if not args.no_pending_tolerance:
        try:
            pend_patterns, pend_origin = pending_registry_patterns(args.registry_repo)
        except GuardError as exc:
            print(f"UNMEASURED: {exc}", file=sys.stderr)
            print(
                "The rows below ARE missing from the registry as checked out; an open "
                "companion PR in "
                f"{args.registry_repo} would have cleared them, but that lookup could not "
                "run, so this guard will not claim it did. Land the row and re-run.",
                file=sys.stderr,
            )
            _report(newly, {}, repo_root, args, to_stderr=True)
            return 1
        del pend_patterns  # coverage is decided per-source below, via pend_origin
        for src in list(newly):
            if src == scan.MISSING_SOURCE_SENTINEL:
                continue
            matched_pr = next(
                (n for pat, n in sorted(pend_origin.items())
                 if scan._source_matches_registry(src, [("", pat, pat.endswith(":*"))])),  # noqa: SLF001
                None,
            )
            if matched_pr is not None:
                tolerated[src] = matched_pr
                newly.pop(src)

    for src, number in sorted(tolerated.items()):
        print(
            f"TOLERATED-PENDING: {src!r} has no row on the registry as checked out, but "
            f"{args.registry_repo}#{number} (OPEN) carries one. Merge it, or this drift "
            "reaches alpha-engine-config@main and reddens it."
        )

    if not newly:
        print("every newly-unregistered source is carried by an open companion PR — passing.")
        return 0

    _report(newly, tolerated, repo_root, args, to_stderr=True)
    return 1


def _report(newly: dict[str, set[str]], _tolerated: dict[str, int], repo_root: Path,
            args: argparse.Namespace, *, to_stderr: bool) -> None:
    out = sys.stderr if to_stderr else sys.stdout
    print(file=out)
    print(f"FAIL: {len(newly)} alert source(s) introduced by this PR have no "
          "alert_classes row.", file=out)

    for src, files in sorted(newly.items()):
        print(f"\n--- {src} ---", file=out)
        for f in sorted(files):
            print(f"  emitted by: {args.repo}/{f}", file=out)

        if src == scan.MISSING_SOURCE_SENTINEL:
            print(
                "\n  This is a `python -m krepis.alerts publish` invocation with NO "
                "--source.\n"
                "  No registry row can ever cover it — the drain ingests it as an "
                "unknown-source\n"
                "  finding. The fix is to ADD `--source <literal>` at the call site, then "
                "register it.",
                file=out,
            )
            continue

        try:
            severity = _severities_for(repo_root, files).get(src, "dynamic")
        except GuardError as exc:
            print(f"  (severity could not be read: {exc}; using `dynamic`)", file=out)
            severity = "dynamic"

        print(
            "\n  The row lives in a DIFFERENT repo. Add it there as a companion PR:\n"
            f"    file:  {args.registry_repo.split('/')[-1]}/{PLAYBOOKS_REL.as_posix()}\n"
            "    under: alert_classes:\n",
            file=out,
        )
        print(row_yaml(src, severity), file=out)
        print(
            "  severity was read from the call site and NORMALIZED to this file's "
            "vocabulary\n"
            "  (`WARN` at a call site is `warning` here; nothing matches `warn`). "
            "`dynamic` means\n"
            "  the call site's severity is not a literal. Change `intake`/`response` if "
            "this class\n"
            "  is drain-blind — an `intake: none` row MUST carry `migration_issue` or "
            "`operator_reason`.\n",
            file=out,
        )

    print(
        "MERGE ORDER: either PR first, in either direction.\n"
        f"  This check re-reads the registry on every run AND treats an OPEN {args.registry_repo}\n"
        "  PR carrying the row as satisfying it, so pushing the companion PR turns this "
        "check\n"
        "  green without waiting for it to merge. Nothing in the registry repo is blocked "
        "by this\n"
        "  PR, so there is no order in which the two can deadlock.\n",
        file=out,
    )


if __name__ == "__main__":
    raise SystemExit(main())
