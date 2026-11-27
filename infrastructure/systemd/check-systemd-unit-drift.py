#!/usr/bin/env python3
"""check-systemd-unit-drift.py — Diff installed systemd units against the
repo copies in `infrastructure/systemd/` (config#2352).

**Background.** `daily-news.{service,timer}` and `metron-intraday.{service,
timer}` are version-tracked but only ever reach `/etc/systemd/system/` via a
manual (or, after config#2352's deploy-on-merge workflows/boot-pull sync)
install-script run. Both delivery paths can still drift from the repo in
practice:

  * daily-news: the deploy-on-merge workflow (deploy-daily-news-units.yml)
    could fail silently, or someone could hand-edit the on-box unit file
    without touching the repo.
  * metron-intraday: relies on the trading box picking up the repo copy at
    its next boot-pull run (2026-07-13 operator ruling on config#2352,
    "queue on merge, apply on next boot") — a box that stays up unusually
    long, or a boot-pull systemd-sync regression, could leave it stale
    for longer than the "pages within a day" acceptance bar.

This script is a same-box, read-only comparison — it runs AS a systemd timer
ON the box that owns the units (no cross-box SSM reach, no new IAM grant:
see the config#2352 PR description for why the alternative, a GHA-side SSM
hash-pull, would have needed a new SendCommand grant on the trading-box
instance that github-actions-lambda-deploy does not currently have).
Reports divergence via flow-doctor, mirroring every other on-box self-report
in this repo (e.g. scripts/run_daily_news_standalone.sh's fail-loud git
sync). Sibling in shape (repo-vs-live diff, exit 0/1/2) to
../step-functions/check-drift.py and check-definition-drift.py, but LOCAL
file compare instead of an AWS API call — there is no "live AWS state" for
a systemd unit, only "what's on this box's disk".

Usage:
  ./infrastructure/systemd/check-systemd-unit-drift.py               # every unit this box installs
  ./infrastructure/systemd/check-systemd-unit-drift.py --unit NAME   # one unit (e.g. daily-news.timer)
  ./infrastructure/systemd/check-systemd-unit-drift.py --report      # on divergence, also flow-doctor report

Exit codes: 0 clean, 1 drift found, 2 config/source error.

**Scope correction (2026-08-08, alpha-engine-config-I6656).** The above
describes four units. The dashboard box has **60** locally-installed unit
files, and this script's closing line read:

    Systemd unit drift check PASSED (installed units match repo).

That is a claim about "installed units" made after comparing four of them —
and not even the six this repo codifies, since its own
`systemd-unit-drift-check.{service,timer}` were absent from `ALL_UNITS`.
From outside, a check with narrow scope and a check with full coverage are
the same green line. Measured while raising `TimeoutStartSec` on
`morning-signal.service`: applied to a live box with nothing anywhere that
would notice it being reverted, hours after this check reported PASSED.

So the scope is now **every regular `.service` / `.timer` file in
`/etc/systemd/system`**, each classified `clean`, `drift` or `uncodified`.
Symlinks are skipped — they are systemd's own aliases into `/usr/lib/systemd`
(`dbus.service` and friends), not units anybody here authored.

Three rules follow:

  * **PASSED is reserved for full coverage.** With 54 of 60 units uncodified,
    no wording of "passed" is true, so the run states the shortfall instead.
  * **The known-uncodified set does not fail the run.** There are 54 of them;
    failing on the backlog would page once per unit on the first run and
    teach exactly one lesson, which is to disable the check. They live in
    `UNCODIFIED_BASELINE_FILE` — a work queue that should only shrink — and
    a unit uncodified but NOT in it is named individually as
    `uncodified-NEW`, because new is the only kind a given run can act on.
    `--strict` fails on anything uncodified and is the end state once the
    baseline is empty.
  * **The counts are emitted** (`--metric`), including zeros: a number that
    only appears when something is wrong cannot distinguish healthy from
    dead (`principles.md` §2.7).
  * **A unit this check cannot READ is `unreadable` — a failing finding,
    never a crash.** 2026-08-09: `nousergon-console.service` was installed
    root-owned 0600 and the whole sweep died on the PermissionError, taking
    coverage of the other 59 units with it. Unit files on these boxes are
    world-readable 0644 (secrets belong in an EnvironmentFile or SSM, never
    inline), so unreadable means both a convention violation and a hole in
    the coverage claim — it exits 1 until the mode is fixed.

"Codified" means a file of the same name under a codified root. The roots do
NOT decide ownership — `infrastructure-ownership-policy` does; they verify
hashes wherever a unit is already codified, and the baseline remains the
register of units codified NOWHERE. The original 2026-08-08 baseline
mislabeled the 21 dashboard-owned units "codified in no known root" because
this script only read its own directory — a coverage gap in the checker, not
in the codification.

**Roots are DISCOVERED, not listed (2026-08-13, alpha-engine-config-I6960).**
They used to be a hand-written `--codified-root` list baked into
`systemd-unit-drift-check.service` — one unit file, installed on every box,
carrying only the DASHBOARD box's checkouts. On the trading box that list
named eleven directories, none of which was
`/home/ec2-user/alpha-engine/infrastructure/systemd`, where six of the seven
units the check reported as "installed but codified nowhere" were codified
all along:

    alpha-engine-daemon.service, alpha-engine-morning.service,
    ibgateway.service, upstream-gate-dryrun-validation.{service,timer},
    xvfb.service

So the check reported a CODIFICATION gap while looking at the wrong
directories — it could not measure, and said it had found a defect. That is
the failure shape the `unreadable` bucket exists to prevent, arriving through
the roots instead of through the units.

A hardcoded list cannot be right on two boxes at once, and a third box would
have inherited the same wrong list. Roots are therefore found by walking
`CODIFIED_SEARCH_BASES` for `<repo>/infrastructure/`, including any `systemd/`
directory nested beneath it —
so a checkout that lands on a box is covered the day it lands, with nothing
to remember to edit. `--codified-root` still works and is ADDITIVE, for a
tree outside the search bases.

**A drift/uncodified finding is annotated with `boot-pull.service` health
(2026-08-31, alpha-engine-config-I9444).** Measured live on the trading box
(ip-172-31-79-214): `daily-news.timer` and `systemd-unit-drift-check.timer`
both drifted, and the bare finding ("installed (c78dd9556b6a) != repo
(7cab59f44f9d)") gave no signal that the box's ONLY reconciliation path
outside a manual install — `boot-pull.service`, per the 2026-07-13 "queue on
merge, apply on next boot" ruling on config#2352 — had been failing at
EVERY boot since 2026-08-28 (crucible-executor's `boot-pull-launcher.sh` was
never copied to `/usr/local/sbin` on this instance, so systemd could not
exec it). A human reading that finding has to already know boot-pull exists
and go check it by hand; `_boot_pull_diagnosis()` does that check itself and,
when boot-pull.service is loaded and its last run did not succeed, appends
one diagnostic line naming the actual cause. Best-effort and silent
everywhere boot-pull.service does not apply — the dashboard box (push-
deployed on merge, no boot-pull), a laptop, or CI — so its own failure to
run `systemctl` can never affect the exit code or mask the underlying drift
finding it is only ever adding CONTEXT to, never replacing.
"""

from __future__ import annotations

import argparse
import hashlib
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent.parent
INSTALLED_DIR = Path("/etc/systemd/system")
UNCODIFIED_BASELINE_FILE = SCRIPT_DIR / "uncodified-units-baseline.txt"

UNIT_SUFFIXES = (".service", ".timer")

#: Where repo checkouts live on a fleet box. Both the dashboard box and the
#: trading box put every checkout directly under the login user's home, so one
#: base covers both; a box that ever differs adds its own rather than editing
#: a per-box list into a shared unit file (which is exactly what I6960 was).
CODIFIED_SEARCH_BASES: tuple[Path, ...] = (Path("/home/ec2-user"),)

#: How deep below a search base a codified root can sit. Four is measured, not
#: guessed: the deepest real one on either box is
#: `nous-ergon-ops/alpha-engine-dashboard/live/infrastructure/systemd`, which is
#: depth 4. Bounding the walk keeps a stray checkout of a monorepo from turning
#: a read-only check into a full-disk crawl on every run.
CODIFIED_SEARCH_MAX_DEPTH = 4

#: Directory names never worth descending into. `.git` alone holds thousands of
#: entries with no unit file among them, and a virtualenv's `site-packages`
#: ships vendored `.service` samples that are not this fleet's units.
_SEARCH_PRUNE = frozenset({
    ".git", "node_modules", "__pycache__", "site-packages", "lib", "lib64",
    "share", "data", ".venv", ".venv-intraday", ".mypy_cache", ".pytest_cache",
})

#: CloudWatch namespace for the coverage counts. Shares box-health's actual
#: namespace deliberately — coverage of the unit set is a property of the box,
#: and a separate namespace is one more place to remember to look. This was
#: born as "NousErgon/BoxHealth", which (a) is NOT what box_health.sh emits
#: to and (b) the box role's PutMetricData grant is namespace-conditioned to
#: `AlphaEngine`/`AlphaEngine/*` (alpha-engine-cloudwatch-metrics.json), so
#: every emit would have been denied — measured live 2026-08-09, before the
#: first `--metric` run ever fired.
METRIC_NAMESPACE = "AlphaEngine/Box"

# The units this repo codifies and installs. NO LONGER THE SCOPE OF THE
# CHECK — scope is now whatever is installed (see the docstring). Kept
# because install-daily-news.sh / install-metron-intraday.sh still describe
# which pair belongs on which box (dashboard: daily-news; trading:
# metron-intraday), and because a codified unit missing from a box is
# reported rather than silently dropped from the comparison.
ALL_UNITS: tuple[str, ...] = (
    "daily-news.service",
    "daily-news.timer",
    "metron-intraday.service",
    "metron-intraday.timer",
)


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except (FileNotFoundError, IsADirectoryError):
        return None


def installed_units(installed_dir: Path | None = None) -> list[str]:
    """Every locally-installed unit file, sorted.

    Regular files only. A symlink here is systemd's own alias mechanism
    pointing into `/usr/lib/systemd`; counting those would put `dbus.service`
    in our work queue.
    """
    d = installed_dir or INSTALLED_DIR
    if not d.is_dir():
        return []
    return sorted(
        p.name
        for p in d.iterdir()
        if p.name.endswith(UNIT_SUFFIXES) and p.is_file() and not p.is_symlink()
    )


def discover_codified_roots(
    bases: tuple[Path, ...] | list[Path] | None = None,
    max_depth: int = CODIFIED_SEARCH_MAX_DEPTH,
) -> list[Path]:
    """Every `infrastructure/` and nested `systemd/` dir under `bases`.

    Matching on directory names rather than on a list of repo names is what
    makes a new checkout covered on arrival instead of on the next time
    somebody remembers to edit a unit file.

    Read-only and best-effort per directory: a base that does not exist (this
    script also runs in CI and on a laptop, where `/home/ec2-user` does not) is
    simply not a source of roots, and one unreadable subtree does not cost the
    coverage of its siblings. A root that cannot be READ still surfaces — as
    `unreadable` on the unit, which is a finding.
    """
    found: list[Path] = []

    def nested_systemd_dirs(infrastructure: Path) -> list[Path]:
        """Find systemd roots below one infrastructure tree, best-effort."""
        nested: list[Path] = []
        stack = [infrastructure]
        while stack:
            current = stack.pop()
            try:
                entries = [p for p in current.iterdir() if p.is_dir() and not p.is_symlink()]
            except (PermissionError, OSError):
                print(
                    f"[check-systemd-unit-drift] could not list {current} while "
                    f"discovering codified roots — any unit codified only there "
                    f"will read as uncodified",
                    file=sys.stderr,
                )
                continue
            for entry in entries:
                if entry.name in _SEARCH_PRUNE or entry.name.startswith("."):
                    continue
                if entry.name == "systemd":
                    nested.append(entry)
                    continue
                stack.append(entry)
        return nested
    for base in (bases if bases is not None else CODIFIED_SEARCH_BASES):
        base = Path(base)
        if not base.is_dir():
            continue
        stack: list[tuple[Path, int]] = [(base, 0)]
        while stack:
            current, depth = stack.pop()
            if depth >= max_depth:
                continue
            try:
                entries = [p for p in current.iterdir() if p.is_dir() and not p.is_symlink()]
            except (PermissionError, OSError):
                # Recorded, not swallowed: a subtree we cannot list is a subtree
                # whose units we cannot claim to have compared.
                print(
                    f"[check-systemd-unit-drift] could not list {current} while "
                    f"discovering codified roots — any unit codified only there "
                    f"will read as uncodified",
                    file=sys.stderr,
                )
                continue
            for entry in entries:
                if entry.name in _SEARCH_PRUNE or entry.name.startswith("."):
                    continue
                if entry.name == "infrastructure":
                    found.append(entry)
                    found.extend(nested_systemd_dirs(entry))
                    continue
                stack.append((entry, depth + 1))
    return sorted(set(found))


def codified_units(roots: list[Path] | None = None) -> list[str]:
    """Every unit file present in the codified roots, sorted."""
    rs = roots or [SCRIPT_DIR]
    return sorted({
        p.name for r in rs if r.is_dir()
        for p in r.iterdir() if p.name.endswith(UNIT_SUFFIXES) and p.is_file()
    })


def load_baseline(path: Path | None = None) -> set[str]:
    """Unit names already known to be uncodified. Blank lines and `#`
    comments ignored; the comments carry which repo should own each one."""
    f = path or UNCODIFIED_BASELINE_FILE
    if not f.is_file():
        return set()
    out = set()
    for line in f.read_text().splitlines():
        name = line.split("#", 1)[0].strip()
        if name:
            out.add(name)
    return out


def codified_paths(name: str, roots: list[Path] | None = None) -> list[Path]:
    """Every codified copy of `name`, in root order."""
    return [root / name for root in (roots or [SCRIPT_DIR]) if (root / name).is_file()]


def codified_path(
    name: str,
    roots: list[Path] | None = None,
    installed_hash: str | None = None,
) -> Path | None:
    """The codified copy to compare against, or None.

    With one copy this is that copy. With SEVERAL — possible since roots became
    discovered rather than listed — first-root-wins would make the verdict
    depend on directory iteration order, so a copy whose hash MATCHES what is
    installed is preferred. That is not a way of hiding disagreement: the
    disagreement is reported separately by `check_unit`, and preferring the
    match only ensures a box running a correctly-codified unit is never called
    drifted because an unrelated checkout happened to sort first.
    """
    candidates = codified_paths(name, roots)
    if not candidates:
        return None
    if installed_hash is not None:
        for candidate in candidates:
            try:
                if _sha256(candidate) == installed_hash:
                    return candidate
            except PermissionError:
                continue
    return candidates[0]


def check_unit(
    name: str,
    roots: list[Path] | None = None,
    installed_dir: Path | None = None,
) -> tuple[str, str]:
    """Returns (status, detail).

    status in {"clean", "drift", "uncodified", "not-installed", "unreadable"}.

    `SCRIPT_DIR` / `INSTALLED_DIR` are read at CALL time when the arguments
    are omitted, so a caller (or a test) can repoint the module globals.

    Note the retired status: installed-with-no-repo-copy used to be
    `source-error`, i.e. a fault in this script's configuration. It is now
    `uncodified` — a gap in coverage, which is what it always was. 54 of the
    60 units on the dashboard box are in that state, and calling that a
    config error made the honest reading unavailable.

    `not-installed` means a codified unit this box does not host. A box
    legitimately runs a subset (dashboard: daily-news; trading:
    metron-intraday), so it is informational, never a finding.
    """
    d = installed_dir or INSTALLED_DIR
    try:
        installed_hash = _sha256(d / name)
    except PermissionError:
        return "unreadable", (
            f"{name}: installed here but not readable by this user — unit "
            f"files are world-readable 0644 on this box (secrets belong in an "
            f"EnvironmentFile or SSM, never inline); drift is unverifiable "
            f"until the mode is fixed"
        )
    repo_path = codified_path(name, roots, installed_hash)

    if installed_hash is None:
        if repo_path is None:
            return "not-installed", f"{name}: neither installed here nor codified"
        return "not-installed", f"{name}: codified, not present on this box ({d / name})"

    if repo_path is None:
        return "uncodified", f"{name}: installed here, codified in no known root"

    try:
        repo_hash = _sha256(repo_path)
    except PermissionError:
        return "unreadable", f"{name}: codified copy at {repo_path} is not readable — drift is unverifiable"
    if installed_hash != repo_hash:
        return "drift", f"{name}: installed ({installed_hash[:12]}) != repo ({repo_hash[:12]})"

    return "clean", f"{name}: OK"


def _boot_pull_diagnosis(
    run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> str | None:
    """If `boot-pull.service` exists on this box and its last run failed, say so.

    Boot-pull is the trading box's ONLY reconciliation path outside a manual
    `install-*.sh` run (2026-07-13 ruling on config#2352: "queue on merge,
    apply on next boot") — when it fails, every unit it manages goes stale
    silently until the next boot, and a bare "installed != repo" finding
    gives no signal that this is the actual cause rather than an on-box
    hand-edit (alpha-engine-config-I9444: `boot-pull-launcher.sh` missing
    from `/usr/local/sbin` left `boot-pull.service` failing at every boot
    from 2026-08-28 through 2026-08-31, and that is exactly what produced
    the `daily-news.timer` / `systemd-unit-drift-check.timer` drift this
    function now names on sight).

    `run` is injectable for tests; production always calls
    `subprocess.run`. Best-effort and silent on any box without
    `boot-pull.service` — the dashboard box updates via deploy-on-merge SSM
    push instead, and this script also runs on a laptop / in CI where
    `systemctl` may not exist at all — so this is a diagnostic ANNOTATION,
    never a second source of findings: its own failure must never raise,
    change the exit code, or mask the drift finding it only adds context to.
    """
    runner = run or subprocess.run
    try:
        proc = runner(
            [
                "systemctl", "show", "boot-pull.service",
                "--property=LoadState,ActiveState,Result",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    props = dict(
        line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line
    )
    if props.get("LoadState") != "loaded":
        return None  # boot-pull.service does not exist on this box
    result = props.get("Result", "")
    if props.get("ActiveState") == "failed" or result not in ("", "success"):
        return (
            f"boot-pull.service (Result={result or 'unknown'}) is in a FAILED "
            "state — this box's only reconciliation path outside a manual "
            "install did not complete its last run, which is the likely cause "
            "of the drift/uncodified findings above, not an on-box hand-edit; "
            "see `systemctl status boot-pull.service` and "
            "/var/log/boot-pull.log on the box (alpha-engine-config-I9444)"
        )
    return None


def _emit_metrics(counts: dict) -> None:
    """Put the coverage counts to CloudWatch. Best-effort, loudly.

    Every value goes on every run, zeros included: `principles.md` §2.7 — a
    metric that only appears when something is wrong makes "healthy" and "the
    emitter is dead" the same shape on the graph.
    """
    try:
        import boto3

        host = socket.gethostname()
        boto3.client("cloudwatch").put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": f"SystemdUnits{key}",
                    "Dimensions": [{"Name": "Host", "Value": host}],
                    "Value": float(value),
                    "Unit": "Count",
                }
                for key, value in counts.items()
            ],
        )
        print(f"[metric] emitted {len(counts)} coverage counts for {host}")
    except Exception as e:  # noqa: BLE001 - a metric failure must not mask the check
        # Recorded, never swallowed: this is the surface that says whether the
        # coverage number is real, so its absence has to be attributable.
        print(
            f"[check-systemd-unit-drift] METRIC EMIT FAILED — coverage is "
            f"UNOBSERVED: {e}",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit", help="check a single unit by filename (e.g. daily-news.timer)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="on a finding, also publish an alert (in addition to the exit code + stdout)",
    )
    parser.add_argument("--metric", action="store_true", help="emit the coverage counts to CloudWatch")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="treat ANY uncodified unit as a failure, not only ones outside the baseline",
    )
    parser.add_argument(
        "--codified-root",
        action="append",
        default=None,
        help="ADDITIONAL directory holding codified unit files (repeatable); "
             "added to the discovered roots, never replacing them",
    )
    parser.add_argument(
        "--no-discover-roots",
        action="store_true",
        help="do not walk the search bases for codified roots (compare only "
             "against --codified-root and this script's directory)",
    )
    args = parser.parse_args()

    # Discovered ∪ explicit ∪ this script's own directory. `--codified-root` is
    # ADDITIVE rather than a replacement: it exists for a tree outside the
    # search bases, and a flag that silently narrowed coverage back to a
    # hand-written list would reintroduce I6960 the first time somebody passed
    # one. `--no-discover-roots` is the explicit way to ask for the old
    # behaviour, and it says so in the output rather than looking identical.
    roots: list[Path] = [] if args.no_discover_roots else discover_codified_roots()
    roots += [Path(r).resolve() for r in (args.codified_root or [])]
    if SCRIPT_DIR not in roots:
        roots.append(SCRIPT_DIR)
    # Deduplicate, keeping order — a root named explicitly AND discovered would
    # otherwise make every unit in it look ambiguous with itself.
    roots = list(dict.fromkeys(roots))
    print(
        f"[roots] comparing against {len(roots)} codified root(s)"
        + (" (discovery DISABLED)" if args.no_discover_roots else "")
    )
    baseline = load_baseline()

    if args.unit:
        names = [args.unit]
    else:
        # The union: what is installed, plus anything codified that is not —
        # so a unit deleted from the box still appears rather than silently
        # dropping out of the comparison.
        names = sorted(set(installed_units()) | set(codified_units(roots)))

    buckets: dict[str, list[str]] = {
        "clean": [], "drift": [], "uncodified": [], "not-installed": [], "unreadable": [],
    }
    details: dict[str, str] = {}
    for name in names:
        status, detail = check_unit(name, roots)
        buckets[status].append(name)
        details[name] = detail
        label = "uncodified-NEW" if (status == "uncodified" and name not in baseline) else status
        print(f"[{label}] {detail}")

    # Widening the roots makes it possible for one unit name to be codified in
    # two checkouts that DISAGREE. `codified_path` then prefers the copy
    # matching what is installed, which is the right verdict for this box and
    # the wrong thing to leave unsaid — one of the two repos is stale, and
    # nothing else on the box is looking.
    #
    # Reported as a NOTICE, exit 0, deliberately: this bucket has never been
    # measured on either box, and shipping an unmeasured new failure condition
    # onto a check whose whole problem was false pages would repeat the defect
    # in the other direction. It becomes a finding once the count is known to
    # be zero — tracked on alpha-engine-config-I6960.
    ambiguous: list[str] = []
    for name in names:
        hashes = set()
        for path in codified_paths(name, roots):
            try:
                digest = _sha256(path)
            except PermissionError:
                continue
            if digest is not None:
                hashes.add(digest)
        if len(hashes) > 1:
            where = ", ".join(str(p) for p in codified_paths(name, roots))
            ambiguous.append(name)
            print(f"[notice] {name}: codified in {len(hashes)} disagreeing copies — {where}")

    uncodified = buckets["uncodified"]
    uncodified_new = [n for n in uncodified if n not in baseline]
    unreadable = buckets["unreadable"]
    installed_count = (
        len(buckets["clean"]) + len(buckets["drift"]) + len(uncodified) + len(unreadable)
    )

    print(
        "SUMMARY "
        f"installed={installed_count} "
        f"clean={len(buckets['clean'])} "
        f"drift={len(buckets['drift'])} "
        f"uncodified={len(uncodified)} "
        f"uncodified_new={len(uncodified_new)} "
        f"unreadable={len(unreadable)} "
        f"codified_not_installed={len(buckets['not-installed'])} "
        f"ambiguous={len(ambiguous)} "
        f"roots={len(roots)}"
    )

    if args.metric:
        _emit_metrics({
            "Installed": installed_count,
            "Clean": len(buckets["clean"]),
            "Drifted": len(buckets["drift"]),
            "Uncodified": len(uncodified),
            "UncodifiedNew": len(uncodified_new),
            "Unreadable": len(unreadable),
            "Ambiguous": len(ambiguous),
            "CodifiedRoots": len(roots),
        })

    exit_code = 0
    findings: list[str] = []
    if buckets["drift"]:
        findings.extend(details[n] for n in buckets["drift"])
        exit_code = max(exit_code, 1)
    if unreadable:
        findings.extend(details[n] for n in unreadable)
        exit_code = max(exit_code, 1)
    if uncodified_new:
        findings.append(
            f"{len(uncodified_new)} unit(s) installed but codified nowhere and absent from the "
            f"baseline: {', '.join(uncodified_new)}"
        )
        if args.strict:
            exit_code = max(exit_code, 1)
    if uncodified and args.strict:
        exit_code = max(exit_code, 1)

    # Diagnostic annotation, never a second source of findings: only runs
    # when there is already something to explain, and never changes
    # exit_code (computed above) — see _boot_pull_diagnosis's docstring.
    if buckets["drift"] or uncodified_new:
        diagnosis = _boot_pull_diagnosis()
        if diagnosis:
            print(f"[diagnosis] {diagnosis}")
            findings.append(diagnosis)

    # Console publish runs on EVERY invocation — clean or not, `--report` or
    # not — mirroring crucible-dashboard's box_health_hygiene precedent
    # (alpha-engine-config-I7857): a surface that only publishes when
    # something is wrong is indistinguishable from one that has died. This is
    # DISTINCT from `_report_drift` below, which is the channel alert and
    # stays gated on `--report` + non-empty findings.
    _publish_console(findings, installed_count=installed_count, drift_count=len(buckets["drift"]))

    if findings:
        print(f"FINDINGS: {len(findings)}", file=sys.stderr)
        for f in findings:
            print(f"  {f}", file=sys.stderr)
        if args.report:
            _report_drift(findings)

    # PASSED is reserved for a box where every installed unit is codified AND
    # matches. Anything else states the shortfall: there is no phrasing of
    # "passed" that is true while 54 units sit outside the comparison.
    if not installed_count:
        print("No unit files installed on this box — nothing to compare.")
    elif not buckets["drift"] and not uncodified and not unreadable:
        print("Systemd unit coverage PASSED (every installed unit is codified and matches).")
    else:
        print(
            f"Systemd unit coverage INCOMPLETE: {len(buckets['clean'])}/{installed_count} "
            f"installed units are codified and clean; {len(uncodified)} uncodified, "
            f"{len(buckets['drift'])} drifted, {len(unreadable)} unreadable. "
            f"This is not a pass."
        )

    return exit_code


# Timer cadence: systemd-unit-drift-check.timer fires OnCalendar=*-*-*
# 06:17:00 UTC once daily. Declared honestly per the fleet check-result
# contract — understating makes the console call this check stale early,
# overstating lets a dead emitter read healthy for longer than it should.
CONSOLE_CHECK_ID = "systemd_unit_drift"
CONSOLE_CADENCE_MINUTES = 1440


def _publish_console(findings: list[str], *, installed_count: int, drift_count: int) -> None:
    """Route the standing-drift finding set to the console (alpha-engine-config-I7857).

    Runs on EVERY invocation, findings or not — see the call site in `main()`.
    This is DISTINCT from `_report_drift`: that function is the Telegram/SNS
    channel alert (gated on `--report` and on findings being non-empty); this
    one is the console's standing-state surface, published unconditionally so
    the console can tell "clean" apart from "dead" via `ran_at` staleness
    rather than the absence of a row meaning either.

    Why this exists at all: `_report_drift` used to be the ONLY surface for
    this finding, at `severity="error"` with a 24h dedup window that matches
    the timer's own cadence exactly — so an UNCHANGED drift re-pages the
    channel every single day it stays true, forever, with no new information
    in the repeat. That is precisely the shape alpha-engine-config-I7857
    audited: `krepis.alerts`' severity tiering only controls the Telegram
    phone-push, never delivery, so there was never a tier that would have
    kept this quiet on its own. The channel alert still fires (drift is a
    real, actionable finding, not hygiene noise) but its repeat window is
    widened in the same change (see `_report_drift`) — the console is what
    makes that safe: the standing set is visible here continuously, with its
    own count, rather than being remembered only between channel repeats.

    Status is `attention`, never `error`, for a non-empty finding set: this
    check can tell you drift EXISTS but the console's status vocabulary
    reserves `error` for "the check itself is blind" (mirrors
    nousergon-lib's fleet_check_result contract and pause_reconcile.py's
    `publish_error` in this same repo) — a check that ran and found drift is
    working exactly as designed, not broken.
    """
    try:
        from nousergon_lib import fleet_check_result as fcr
    except Exception as e:  # noqa: BLE001 — telemetry must not break the check
        print(
            f"[check-systemd-unit-drift] console publish skipped — "
            f"nousergon_lib.fleet_check_result unavailable: {e}",
            file=sys.stderr,
        )
        return

    status = fcr.STATUS_OK if not findings else fcr.STATUS_ATTENTION
    if findings:
        summary = (
            f"{len(findings)} standing finding(s) ({drift_count} drift) on "
            f"{socket.gethostname()} out of {installed_count} installed unit(s)"
        )
    else:
        summary = f"{installed_count} installed unit(s), all codified and matching"

    try:
        fcr.emit_result(
            check_id=CONSOLE_CHECK_ID,
            label="Systemd unit drift (installed vs repo)",
            status=status,
            summary=summary,
            cadence_minutes=CONSOLE_CADENCE_MINUTES,
            findings=[{"id": f"finding-{i}", "detail": f} for i, f in enumerate(findings)],
            deep_link=(
                "https://github.com/nousergon/nousergon-data/blob/main/"
                "infrastructure/systemd/check-systemd-unit-drift.py"
            ),
        )
    except Exception as e:  # noqa: BLE001 — telemetry must not break the check
        print(
            f"[check-systemd-unit-drift] console publish failed (best-effort, "
            f"check result unaffected): {e}",
            file=sys.stderr,
        )


def _report_drift(findings: list[str]) -> None:
    """Alert on detected systemd unit drift.

    This used to construct flow-doctor by hand and had been BROKEN for an
    unknown length of time (alpha-engine-config-I4509). Two independent faults,
    either one fatal:

      1. `flow_doctor.init()` does not exist. flow-doctor 0.8.7 exports
         FlowDoctor / FlowDoctorBuilder and no `init`, so the call raised
         AttributeError every time.
      2. The env hydration was incomplete anyway -- flow-doctor.yaml references
         more ${VAR}s than were being set, so even with (1) fixed, construction
         raises ConfigError.

    Neither was noticed because this function only runs when drift is FOUND,
    and the daily check normally passes -- the same reason boot-pull's copy of
    this bug survived. A reporting path that is only exercised on failure needs
    a test that exercises failure; see the accompanying test module.

    `krepis.alerts` is the canonical alert CLI (config#1649). It resolves its
    own secrets, so there is no hydration list here to drift out of sync with
    flow-doctor.yaml.

    **Repeat window widened 1440 -> 43200 (alpha-engine-config-I7857).** The
    timer runs daily, and the old 1440min (24h) window matched that cadence
    exactly — so an UNCHANGED drift re-paged the channel every single day it
    stayed true, with no new information in the repeat. This is the same
    condition wrongly believed fixed by tuning severity/window before
    (`krepis.alerts` was never actually gating visibility by severity — see
    that issue). The dedup KEY still derives from the finding SET, so a
    drift appearing, clearing, or changing pages IMMEDIATELY regardless of
    the window; the window governs only how often an UNCHANGED set repeats.
    30 days mirrors the precedent set by crucible-dashboard's box_health
    `warning` tier (same audit). The standing set stays continuously visible
    on the console via `_publish_console` above, independent of this window.
    """
    message = (
        f"systemd unit drift on {socket.gethostname()}: "
        f"installed units no longer match the repo -- {'; '.join(findings)}"
    )
    try:
        from krepis.alerts import publish

        publish(
            message=message,
            severity="error",
            # alpha-engine-config-I7740 (operator ruling 2026-08-21): source
            # must equal the registry's declared string exactly — the
            # registry is the contract, emitters conform to it.
            source="alpha-engine-data/infrastructure/systemd/check-systemd-unit-drift.py",
            # Dedup on the FINDINGS, not the message: the same drift persisting
            # alerts once per window, while a new finding changes the key and
            # pages immediately regardless of window.
            dedup_key="unit-drift-" + hashlib.sha256(
                "|".join(sorted(findings)).encode()
            ).hexdigest()[:16],
            dedup_window_min=43200,
        )
    except Exception as e:
        # Fail LOUD. A silent failure here is the exact defect this rewrite
        # fixes: the drift was detected and the telling threw.
        print(
            f"[check-systemd-unit-drift] ALERT PUBLISH FAILED — drift is "
            f"UNREPORTED: {e}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    sys.exit(main())
