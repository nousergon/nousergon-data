"""A cycle-tracking stage's window opens at its FIRST ENTRY for the run date.

``alpha-engine-config-I10173`` / ``-I10194`` §3. ``DataPhase1`` auto-skips
phases whose output is already on S3 for the cycle, so a second attempt of
the SAME cycle writes nothing new for them. Its stage-coverage window must
therefore open where the cycle's first attempt entered the stage, or the
stage reads STALE on its own output.

THE MEASURED CASE (2026-09-25 cycle, read 2026-09-28 from
``s3://alpha-engine-research/_stage_coverage/2026-09-25/DataPhase1.json`` and
``_ssm_logs/data-weekly/``):

- ``rehearsal-2026-09-25-1``'s one DataPhase1 command entered 21:50:25Z.
  Attempt 1 wrote ``macro.json`` (21:54:54Z), the macro history/calendar
  parquets (21:54:57Z / 21:54:59Z), ``short_interest.json`` (22:03:34Z) and
  ``universe_classification/latest.json`` (22:12:13Z), then its spot was
  reclaimed. The relaunch recomputed the window as 22:19:59Z, auto-skipped
  those phases at 22:22Z, and recorded STALE with ``window_start
  2026-09-25T22:19:59+00:00``. (The same shape hit the 2026-09-11 cycle:
  attempt 1 at 09:39:50Z, window 09:57:33Z, four artifacts STALE.)
- ``nousergon-data#1972`` exported the window so an in-command relaunch keeps
  it. But the late window was already PERSISTED in the verdict, and the
  scheduled run the next morning reused it verbatim through the prior-verdict
  path: verdict version recorded 2026-09-26T10:24:06Z, same 22:19:59Z window,
  five artifacts STALE again.
- An SF reissue (``DataPhase1Reissue``) is a new SSM command, so no exported
  value reaches it, and a first command that died mid-workload left no
  verdict to reuse. It captured its own start after the first command's writes.

The fix is a write-once first-entry record (``record_stage_entry``), written
before the workload, which the resolver prefers to any verdict's window. These
tests drive the real ``_stage_window.sh`` functions against a directory-backed
stand-in for S3, one bash process per SSM command, as production runs them.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
INFRA = REPO / "infrastructure"
COMMON = INFRA / "_spot_common.sh"
WINDOW = INFRA / "_stage_window.sh"
BUCKET = "alpha-engine-research"
RUN_DATE = "2026-09-25"
ENTRY_KEY = f"_stage_coverage/_entry/{RUN_DATE}/DataPhase1.json"
VERDICT_KEY = f"_stage_coverage/{RUN_DATE}/DataPhase1.json"

#: LastModified of the five artifacts the 2026-09-25 verdicts called STALE.
MEASURED_WRITES = {
    "macro_snapshot": "2026-09-25T21:54:54Z",
    "macro_history": "2026-09-25T21:54:57Z",
    "macro_release_calendar": "2026-09-25T21:54:59Z",
    "short_interest_snapshot": "2026-09-25T22:03:34Z",
    "universe_classification_latest": "2026-09-25T22:12:13Z",
}

# A directory-backed `aws` covering exactly the three calls _stage_window.sh
# makes. Errors mimic the real CLI's stderr, which is what the functions grep.
_FAKE_AWS = r"""#!{python}
import os, pathlib, sys
root = pathlib.Path(os.environ["FAKE_S3"])
deny = os.environ.get("FAKE_S3_DENY", "")
args = sys.argv[1:]
with open(root / "calls.log", "a") as log:
    log.write(" ".join(args) + "\n")

def split(uri):
    bucket, _, key = uri[len("s3://"):].partition("/")
    return root / bucket / key, key

if args[:2] == ["s3api", "head-object"]:
    key = args[args.index("--key") + 1]
    path = root / args[args.index("--bucket") + 1] / key
    if deny and key.startswith(deny):
        sys.stderr.write("An error occurred (403) when calling the HeadObject operation: Forbidden\n")
        sys.exit(254)
    if path.exists():
        print("{{}}")
        sys.exit(0)
    sys.stderr.write("An error occurred (404) when calling the HeadObject operation: Not Found\n")
    sys.exit(254)

if args[:2] == ["s3", "cp"]:
    src, dst = args[2], args[3]
    if src == "-":
        path, key = split(dst)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(sys.stdin.read())
        sys.exit(0)
    path, key = split(src)
    if deny and key.startswith(deny):
        sys.stderr.write("download failed: An error occurred (AccessDenied) when calling the GetObject operation: Access Denied\n")
        sys.exit(1)
    if not path.exists():
        sys.stderr.write(f"fatal error: An error occurred (404) when calling the HeadObject operation: Key \"{{key}}\" does not exist\n")
        sys.exit(1)
    sys.stdout.write(path.read_text())
    sys.exit(0)

sys.stderr.write(f"fake aws: unexpected call {{args}}\n")
sys.exit(2)
"""


class _S3:
    def __init__(self, tmp_path: Path) -> None:
        self.root = tmp_path / "s3"
        self.root.mkdir()
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        aws = self.bin / "aws"
        aws.write_text(_FAKE_AWS.format(python=sys.executable))
        aws.chmod(0o755)

    def put(self, key: str, body: dict) -> None:
        path = self.root / BUCKET / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(body))

    def get(self, key: str) -> dict | None:
        path = self.root / BUCKET / key
        return json.loads(path.read_text()) if path.exists() else None

    def calls(self) -> list[str]:
        log = self.root / "calls.log"
        return log.read_text().splitlines() if log.exists() else []

    def command(
        self,
        started: str,
        body: str,
        *,
        declared: bool = True,
        deny: str = "",
    ) -> subprocess.CompletedProcess[str]:
        """One SSM command: a fresh bash that sources the real launcher lib."""
        script = f"""
set -euo pipefail
export PATH={shlex.quote(str(self.bin))}:$PATH
export FAKE_S3={shlex.quote(str(self.root))}
export FAKE_S3_DENY={shlex.quote(deny)}
export LIB_PYTHON={shlex.quote(sys.executable)}
export _STAGE_WINDOW_START={shlex.quote(started)}
_SPOT_NAME=x
_SSM_SLUG=x
_PROCESS_NAME=x
MAX_RUNTIME_SECONDS=1
source {shlex.quote(str(COMMON))}
_STAGE_WINDOW_TRACKS_CYCLE={"1" if declared else "0"}
{body}
"""
        proc = subprocess.run(
            ["bash", "-c", script], capture_output=True, text=True, check=False
        )
        assert proc.returncode == 0, proc.stderr
        return proc


def _enter(s3: _S3, started: str, **kw) -> subprocess.CompletedProcess[str]:
    return s3.command(started, f"record_stage_entry DataPhase1 {RUN_DATE}", **kw)


def _resolve(s3: _S3, started: str, run_date: str = RUN_DATE, **kw) -> str:
    return s3.command(
        started, f"resolve_stage_window_start DataPhase1 {run_date}", **kw
    ).stdout


def _at(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _stale(window: str) -> list[str]:
    return [name for name, at in MEASURED_WRITES.items() if _at(at) < _at(window)]


# ── The measured 2026-09-25 cycle, replayed ──────────────────────────────────


def test_an_sf_reissue_asserts_against_the_first_commands_entry(tmp_path: Path) -> None:
    """Command 1 enters at 21:50:25Z, writes, and dies before asserting — no
    verdict. The reissue is a NEW process, so nothing exported reaches it."""
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z")

    _enter(s3, "2026-09-25T22:19:59Z")  # the reissue's own entry
    window = _resolve(s3, "2026-09-25T22:19:59Z")

    assert window == "2026-09-25T21:50:25Z"
    assert _stale(window) == [], "the stage would read STALE on its own writes"
    assert s3.get(ENTRY_KEY)["window_start"] == "2026-09-25T21:50:25Z", (
        "the reissue moved the first entry"
    )


def test_a_persisted_late_window_does_not_poison_the_rest_of_the_cycle(
    tmp_path: Path,
) -> None:
    """The scheduled run on 2026-09-26 reused the rehearsal verdict's
    22:19:59Z window verbatim. With the first-entry record present, the
    earliest start this cycle can prove wins."""
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z")
    s3.put(
        VERDICT_KEY,
        {
            "stage": "DataPhase1",
            "status": "STALE",
            "window_start": "2026-09-25T22:19:59+00:00",
        },
    )
    assert set(_stale("2026-09-25T22:19:59Z")) == set(
        MEASURED_WRITES
    )  # the bug, as measured

    window = _resolve(s3, "2026-09-26T10:20:00Z")

    assert window == "2026-09-25T21:50:25Z"
    assert _stale(window) == []


def test_a_relaunch_inside_one_command_neither_rewrites_nor_moves_the_entry(
    tmp_path: Path,
) -> None:
    """The in-command relaunch inherits the exported start (#1972), finds the
    record, and leaves it alone — write-once."""
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z")
    puts_before = [c for c in s3.calls() if c.startswith("s3 cp - ")]

    proc = _enter(s3, "2026-09-25T21:50:25Z")

    assert [c for c in s3.calls() if c.startswith("s3 cp - ")] == puts_before
    assert "keeping the FIRST entry" in proc.stderr


def test_a_cycle_with_no_record_still_reuses_the_prior_verdict(tmp_path: Path) -> None:
    """Cycles whose first attempt ran before the record existed keep the
    I10194 §3 behaviour unchanged."""
    s3 = _S3(tmp_path)
    s3.put(VERDICT_KEY, {"stage": "DataPhase1", "window_start": "2026-09-25T21:50:25Z"})
    assert _resolve(s3, "2026-09-26T10:20:00Z") == "2026-09-25T21:50:25Z"


def test_the_cycles_true_first_attempt_keeps_its_own_window(tmp_path: Path) -> None:
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z")
    assert _resolve(s3, "2026-09-25T21:50:25Z") == "2026-09-25T21:50:25Z"


# ── A previous run's writes still do not count ───────────────────────────────


def test_a_previous_cycles_record_is_never_read(tmp_path: Path) -> None:
    """Keyed by run_date: next week's run cannot find this week's entry, so
    this week's writes are still leftovers to it."""
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z")
    assert (
        _resolve(s3, "2026-10-02T21:40:00Z", run_date="2026-10-02")
        == "2026-10-02T21:40:00Z"
    )


def test_a_window_earlier_than_its_own_run_date_is_rejected(tmp_path: Path) -> None:
    """No attempt for run_date X can start before X. A record or verdict that
    claims one would re-admit a previous cycle's output as this one's."""
    s3 = _S3(tmp_path)
    s3.put(ENTRY_KEY, {"stage": "DataPhase1", "window_start": "2026-09-18T21:50:25Z"})
    s3.put(VERDICT_KEY, {"stage": "DataPhase1", "window_start": "2026-09-19T09:40:00Z"})
    proc = s3.command(
        "2026-09-25T22:19:59Z", f"resolve_stage_window_start DataPhase1 {RUN_DATE}"
    )
    assert proc.stdout == "2026-09-25T22:19:59Z"
    assert "predates run_date" in proc.stderr


def test_an_undeclared_stage_records_nothing_and_keeps_its_own_window(
    tmp_path: Path,
) -> None:
    """For a stage with no auto-skip, 'predates this attempt' IS the finding."""
    s3 = _S3(tmp_path)
    _enter(s3, "2026-09-25T21:50:25Z", declared=False)
    assert s3.get(ENTRY_KEY) is None
    assert s3.calls() == []
    s3.put(ENTRY_KEY, {"window_start": "2026-09-25T21:50:25Z"})
    assert (
        _resolve(s3, "2026-09-25T22:19:59Z", declared=False) == "2026-09-25T22:19:59Z"
    )


def test_an_unreadable_record_is_never_overwritten(tmp_path: Path) -> None:
    """A rewrite after a failed read could move the first entry LATER."""
    s3 = _S3(tmp_path)
    proc = _enter(s3, "2026-09-25T22:19:59Z", deny="_stage_coverage/_entry/")
    assert not any(c.startswith("s3 cp - ") for c in s3.calls())
    assert "could not read" in proc.stderr and "Forbidden" in proc.stderr


def test_an_unreadable_record_degrades_to_the_alarming_side(tmp_path: Path) -> None:
    s3 = _S3(tmp_path)
    proc = s3.command(
        "2026-09-25T22:19:59Z",
        f"resolve_stage_window_start DataPhase1 {RUN_DATE}",
        deny="_stage_coverage/",
    )
    assert proc.stdout == "2026-09-25T22:19:59Z"
    assert "could not read" in proc.stderr and "AccessDenied" in proc.stderr


# ── Wiring: every cycle-tracking launcher records its entry, in the right place ─


def _launchers() -> dict[Path, str]:
    return {path: path.read_text() for path in sorted(INFRA.glob("spot_*.sh"))}


@pytest.mark.parametrize(
    "path",
    sorted(p for p, t in _launchers().items() if "resolve_stage_window_start " in t),
)
def test_every_resolving_launcher_records_its_first_entry(path: Path) -> None:
    """A stage that resolves against the record but never writes it would
    silently fall back to the late-window behaviour this file exists to end."""
    text = path.read_text()
    stages = re.findall(r"resolve_stage_window_start (\w+)", text)
    for stage in stages:
        assert f'record_stage_entry {stage} "${{EXECUTION_RUN_DATE:-}}"' in text, (
            f"{path.name} resolves {stage}'s window but never records its first entry"
        )


def test_data_phase1_records_its_entry_after_the_dry_paths_and_before_the_workload() -> (
    None
):
    text = (INFRA / "spot_data_phase1.sh").read_text()
    record = text.index('record_stage_entry DataPhase1 "${EXECUTION_RUN_DATE:-}"')
    assert text.index('if [ "$PREFLIGHT_ONLY" = "1" ]; then') < record, (
        "a --preflight-only run writes nothing and must not open the window"
    )
    assert text.index('if [ "$MODE" = "smoke-only" ]; then') < record
    assert record < text.index('run_ssm "phase1"'), (
        "recorded after the workload, a command that dies mid-workload leaves no entry"
    )


def test_data_phase2_records_its_entry_before_the_workload_and_never_on_preflight() -> (
    None
):
    text = (INFRA / "spot_data_weekly.sh").read_text()
    record = text.index('record_stage_entry DataPhase2 "${EXECUTION_RUN_DATE:-}"')
    assert record < text.index('run_ssm "phase2-only"')
    guard = text.rindex('if [ "$PREFLIGHT_ONLY" != "1" ]; then', 0, record)
    assert record - guard < 120, (
        "the DataPhase2 record is not guarded against --preflight-only"
    )


def test_the_record_lives_outside_every_partition_the_coverage_sweep_reads() -> None:
    """The sweep reads every ``.json`` directly under
    ``_stage_coverage/<date>/`` as a stage verdict."""
    assert '_STAGE_ENTRY_PREFIX="_stage_coverage/_entry"' in WINDOW.read_text()
    assert not re.match(r"_stage_coverage/\d{4}-\d{2}-\d{2}/", ENTRY_KEY)
