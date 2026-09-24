"""A guard-skipped MorningEnrich is not graded STALE — alpha-engine-config-I11474.

rehearsal-2026-09-23-2: MorningEnrich's stale-overwrite guard correctly
skipped the enrich (its run manifest says ``not_applicable``), and the same
stage then logged ``stage-coverage MorningEnrich: STALE — WHOLLY STALE — 0 of
1 declared artifact(s) refreshed``. The launcher's coverage assertion had no
way to learn the guard fired. The hand-off is now:

1. ``weekly_collector.py --guard-skip-record PATH`` writes the guard's reason
   to PATH iff the mode reported ``status=skipped`` (and removes PATH
   otherwise, so a leftover can never excuse a run that executed).
2. The spot workload copies that record to the run's S3 staging prefix.
3. The launcher reads it back and passes the reason to
   ``krepis.stage_coverage assert --not-applicable-reason``, which records a
   non-finding ``COVERED_NO_OUTPUT`` carrying it instead of ``STALE``.
"""

from __future__ import annotations

import re
from pathlib import Path

import weekly_collector

LAUNCHER = Path(__file__).resolve().parent.parent / "infrastructure" / "spot_morning_enrich.sh"

_REASON = (
    "stale_overwrite (polygon target=2026-09-22, ArcticDB SPY last=2026-09-23) — polygon's "
    "T+1 settled day is older than the yfinance EOD row already in ArcticDB"
)


def test_a_guard_skip_writes_the_reason_verbatim(tmp_path):
    record = tmp_path / "skip.txt"
    weekly_collector._write_guard_skip_record(
        str(record), {"mode": "morning_enrich", "status": "skipped", "skip_reason": _REASON},
    )
    assert record.read_text().strip() == _REASON


def test_a_run_that_executed_leaves_no_record_and_clears_a_leftover(tmp_path):
    record = tmp_path / "skip.txt"
    record.write_text("left over from an earlier run\n")
    weekly_collector._write_guard_skip_record(
        str(record), {"mode": "morning_enrich", "status": "ok"},
    )
    assert not record.exists()


def test_a_skip_with_no_reason_is_not_a_declaration(tmp_path):
    record = tmp_path / "skip.txt"
    weekly_collector._write_guard_skip_record(str(record), {"status": "skipped"})
    assert not record.exists()


def test_the_flag_is_parsed(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["weekly_collector.py", "--morning-enrich", "--guard-skip-record", "/tmp/x.txt"],
    )
    args = weekly_collector._parse_args()
    assert args.guard_skip_record == "/tmp/x.txt"


def test_main_writes_the_record_from_the_run_result(monkeypatch, tmp_path):
    """End to end through main(): the guard's skipped result reaches the file."""
    record = tmp_path / "skip.txt"
    monkeypatch.setattr(
        "sys.argv",
        ["weekly_collector.py", "--morning-enrich", "--guard-skip-record", str(record)],
    )
    monkeypatch.setattr(weekly_collector, "load_config", lambda _p: {"bucket": "b"})

    class _Preflight:
        def __init__(self, *a, **k):
            pass

        def run(self):
            return None

    import preflight

    monkeypatch.setattr(preflight, "DataPreflight", _Preflight)
    monkeypatch.setattr(
        weekly_collector, "run_weekly",
        lambda config, args: {"mode": "morning_enrich", "status": "skipped", "skip_reason": _REASON},
    )
    monkeypatch.setattr(weekly_collector, "get_flow_doctor", lambda: None)
    weekly_collector.main()
    assert record.read_text().strip() == _REASON


def test_the_launcher_hands_the_record_to_the_coverage_assertion():
    body = LAUNCHER.read_text()
    # The workload asks for the record and stages it under this run's prefix.
    assert "--morning-enrich --guard-skip-record /tmp/morning_enrich_guard_skip.txt" in body
    assert '_GUARD_SKIP_KEY="${_S3_STAGING}/stage_guard/MorningEnrich.txt"' in body
    assert 'aws s3 cp /tmp/morning_enrich_guard_skip.txt "${_GUARD_SKIP_KEY}"' in body
    # The launcher reads it back and passes it on the ONE assertion line,
    # which stays observe-mode and loud-guarded.
    (line,) = [
        ln for ln in body.splitlines()
        if "krepis.stage_coverage assert --stage MorningEnrich" in ln
    ]
    assert '${_GUARD_SKIP_ARGS[@]+"${_GUARD_SKIP_ARGS[@]}"}' in line
    assert "--enforce" not in line
    assert "|| echo" in line and ">&2" in line
    assert "_GUARD_SKIP_ARGS=(--not-applicable-reason \"$_GUARD_SKIP_REASON\")" in body


def test_staging_the_record_can_never_fail_the_stage():
    """The observer must not kill the stage it observes: a failed copy is a
    loud WARNING, never an exit under the workload's `set -eo pipefail`."""
    body = LAUNCHER.read_text()
    (line,) = [ln for ln in body.splitlines() if "aws s3 cp /tmp/morning_enrich_guard_skip.txt" in ln]
    assert re.search(r"\|\| echo \"WARNING: .*\" >&2$", line.strip()), line
