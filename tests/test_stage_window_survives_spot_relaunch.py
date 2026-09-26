"""A spot-interruption relaunch keeps the execution's stage-coverage window.

THE INCIDENT (rehearsal-2026-09-25-1, DataPhase1). Attempt 1 launched at
21:50Z, wrote ``constituents.json``, ``macro.json``, ``short_interest.json``,
``macro_history.parquet``, ``macro_release_calendar.parquet`` and
``universe_classification/latest.json`` (21:52-22:12Z), and was reclaimed.
``on_exit`` relaunched with ``exec bash "$0"``. ``_STAGE_WINDOW_START`` was not
exported, so the re-exec'd script recomputed it as "now" (22:19:59Z). Attempt 2
auto-skipped those six phases (``auto_skip_marker_ok``: correct idempotency)
and the stage-coverage verdict read STALE on six artifacts that the same
execution had written 30 minutes earlier. The ``_stage_window.sh`` cycle
resolver could not help: attempt 1 never reached its assertion, so there was
no prior verdict to reuse.

These tests drive the real relaunch path (``_spot_common.sh``'s ``on_exit``
and the ``spot_data_weekly.sh`` monolith's) with the stand-ins from
``test_spot_reclaim_demotion``, plus a ``date`` stand-in that returns a later
timestamp on every call, so a recomputed window is visible as a different
value.
"""

from __future__ import annotations

from pathlib import Path

from tests.test_spot_reclaim_demotion import _INFRA, _Rig, _why

# Returns 2026-09-25T21:50:0<n>Z for the n-th ISO-8601 call, so every
# recomputation of the window yields a new value; any other format goes to the
# real binary.
_FAKE_DATE = r"""#!/usr/bin/env bash
if [ "$*" = "-u +%Y-%m-%dT%H:%M:%SZ" ]; then
  n=$(( $(cat "$FAKE_DIR/date.n" 2>/dev/null || echo 0) + 1 ))
  echo "$n" > "$FAKE_DIR/date.n"
  printf '2026-09-25T21:50:%02dZ\n' "$n"
  exit 0
fi
exec /bin/date "$@"
"""

# The per-stage launcher shape from test_spot_reclaim_demotion, printing the
# window each attempt would assert against.
_STAGE = """#!/usr/bin/env bash
set -euo pipefail
source "{infra}/_spot_common.sh"
_SPOT_NAME="${{_SPOT_NAME:-data-phase1}}"
_SSM_SLUG="${{_SSM_SLUG:-spot-data-phase1}}"
_PROCESS_NAME="${{_PROCESS_NAME:-data-phase1}}"
MAX_RUNTIME_SECONDS="${{MAX_RUNTIME_SECONDS:-6600}}"
ORIG_ARGS=("$@")
spot_launch
echo "WINDOW attempt=$SPOT_ATTEMPT start=$_STAGE_WINDOW_START"
exit "${{FAKE_WORKLOAD_RC:-7}}"
"""


def _install_fake_date(rig: _Rig) -> None:
    fake = rig.bin / "date"
    fake.write_text(_FAKE_DATE)
    fake.chmod(0o755)


def _windows(stdout: str) -> dict[int, str]:
    out: dict[int, str] = {}
    for line in stdout.splitlines():
        if line.startswith("WINDOW "):
            fields = dict(f.split("=", 1) for f in line.split()[1:])
            out[int(fields["attempt"])] = fields["start"]
    return out


def test_relaunch_keeps_the_first_attempts_window(tmp_path: Path) -> None:
    rig = _Rig(tmp_path)
    _install_fake_date(rig)
    stage = tmp_path / "stage-window.sh"
    stage.write_text(_STAGE.format(infra=_INFRA))
    stage.chmod(0o755)

    proc = rig.run(stage, RUN_TOKEN="rehearsal-2026-09-25-1")

    assert len(rig.launches()) == 2, _why(proc)
    windows = _windows(proc.stdout)
    assert set(windows) == {1, 2}, _why(proc)
    assert windows[1].startswith("2026-09-25T21:50:"), _why(proc)
    assert windows[2] == windows[1], (
        "the relaunch recomputed the stage-coverage window instead of keeping "
        f"the execution's start: attempt 1 {windows[1]}, attempt 2 {windows[2]}"
        + _why(proc)
    )


def test_a_fresh_run_still_captures_its_own_window(tmp_path: Path) -> None:
    """An SF reissue is a NEW run of the script, not a relaunch: it must not
    inherit anything, so a genuinely stale leftover still reads STALE."""
    rig = _Rig(tmp_path)
    _install_fake_date(rig)
    stage = tmp_path / "stage-window.sh"
    stage.write_text(_STAGE.format(infra=_INFRA))
    stage.chmod(0o755)

    first = rig.run(stage, RUN_TOKEN="exec-a", FAKE_WORKLOAD_RC="0")
    second = rig.run(stage, RUN_TOKEN="exec-b", FAKE_WORKLOAD_RC="0")

    w1 = _windows(first.stdout)[1]
    w2 = _windows(second.stdout)[1]
    assert w1 != w2, _why(second)


def test_monolith_relaunch_inherits_the_window(tmp_path: Path) -> None:
    """``spot_data_weekly.sh`` carries its own on_exit/re-exec pair. Observed
    through the environment the launch CLI is handed: the relaunch must see
    the first attempt's window, not an absent or recomputed one."""
    rig = _Rig(tmp_path)
    _install_fake_date(rig)
    fake = rig.python.read_text().replace(
        'if [ "$1" = "-m" ] && [ "$2" = "krepis.ec2_spot" ] && [ "$3" = "launch" ]; then\n',
        'if [ "$1" = "-m" ] && [ "$2" = "krepis.ec2_spot" ] && [ "$3" = "launch" ]; then\n'
        '  printf \'%s\\n\' "${_STAGE_WINDOW_START:-<unset>}" >> "$FAKE_DIR/window.log"\n',
        1,
    )
    assert fake != rig.python.read_text()
    rig.python.write_text(fake)

    proc = rig.run(
        _INFRA / "spot_data_weekly.sh",
        "--phase2-only",
        RUN_TOKEN="exec-mono-window",
        FAKE_WAIT_RC="255",
    )

    assert len(rig.launches()) == 2, _why(proc)
    seen = (rig.dir / "window.log").read_text().split()
    assert len(seen) == 2, _why(proc)
    assert seen[0].startswith("2026-09-25T21:50:"), (seen, _why(proc))
    assert seen[1] == seen[0], (seen, _why(proc))
