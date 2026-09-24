"""alpha-engine-config-I11475 — launchers label themselves with the run's date.

The 2026-09-23 weekly rehearsal (run_date 2026-09-23) crossed 00:00 UTC, and
every launcher banner printed after that read 2026-09-24, because each one
printed ``$(date +%Y-%m-%d)`` — the spot box's UTC calendar day. The
DataPhase2 banner also claimed "Phase1 + RAG" for a ``--phase2-only`` run.
``stage_run_date`` in ``infrastructure/_stage_window.sh`` is now the one
definition: ``$EXECUTION_RUN_DATE`` (the SF's $.run_date), else the exchange's
calendar day, never the UTC day.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HELPER = REPO / "infrastructure" / "_stage_window.sh"
LAUNCHERS = (
    "spot_data_phase1.sh",
    "spot_data_weekly.sh",
    "spot_morning_enrich.sh",
    "spot_rag_ingestion.sh",
)


def _stage_run_date(env: dict[str, str], path_prefix: Path | None = None) -> str:
    run_env = {k: v for k, v in os.environ.items() if k not in ("EXECUTION_RUN_DATE", "TZ")}
    run_env.update(env)
    if path_prefix is not None:
        run_env["PATH"] = f"{path_prefix}{os.pathsep}{run_env.get('PATH', '')}"
    out = subprocess.run(
        ["bash", "-c", f'set -u; source "{HELPER}"; stage_run_date'],
        env=run_env, capture_output=True, text=True, check=True,
    )
    return out.stdout


def test_sf_run_date_wins():
    assert _stage_run_date({"EXECUTION_RUN_DATE": "2026-09-23"}) == "2026-09-23"


def test_fallback_reads_the_exchange_calendar_not_utc(tmp_path):
    """With no SF date, the day comes from ``date`` under America/New_York.

    A stub ``date`` reports which zone it was asked in, so the assertion does
    not depend on what time the test happens to run."""
    stub = tmp_path / "date"
    stub.write_text('#!/usr/bin/env bash\nprintf "day-in-%s" "${TZ:-UTC}"\n')
    stub.chmod(0o755)
    assert _stage_run_date({}, path_prefix=tmp_path) == "day-in-America/New_York"


def test_empty_sf_date_falls_back_rather_than_printing_nothing(tmp_path):
    stub = tmp_path / "date"
    stub.write_text('#!/usr/bin/env bash\nprintf "day-in-%s" "${TZ:-UTC}"\n')
    stub.chmod(0o755)
    assert (
        _stage_run_date({"EXECUTION_RUN_DATE": ""}, path_prefix=tmp_path)
        == "day-in-America/New_York"
    )


def test_no_launcher_banner_labels_itself_with_the_utc_day():
    utc_banner = re.compile(r'^echo ".*\$\(date \+%Y-%m-%d\)"\s*$', re.M)
    for name in LAUNCHERS:
        text = (REPO / "infrastructure" / name).read_text()
        assert not utc_banner.search(text), f"{name} banner still prints the box's UTC day"
        assert "$(stage_run_date)" in text, f"{name} banner must use stage_run_date"


def test_data_weekly_banner_names_its_actual_mode():
    text = (REPO / "infrastructure" / "spot_data_weekly.sh").read_text()
    assert "Weekly Data Spot Run (Phase1 + RAG)" not in text
    assert "Weekly Data Spot Run (mode: $RUN_MODE)" in text
