"""The RAG weekly ingestion keys its dated S3 objects by the SF cycle date.

alpha-engine-config-I11514: ``emit_manifest.py`` wrote
``rag/manifest/{date.today()}.json`` and ``run_weekly_ingestion.sh`` set
``RUN_DATE`` from ``date -u``, while the registry row ``rag_manifest_dated``
resolves ``{date}`` to the Step Function's cycle date. Any RAGIngestion run that
crossed midnight UTC (the 2026-09-23 rehearsal wrote ``2026-09-24.json`` at
00:55Z) left the cycle's dated key missing, so the stage-output sweep reported
it on every weekly run.

These tests pin every link of the date's path, from the SF to the key:

  step_function.json RAGIngestion  exports EXECUTION_RUN_DATE from $.run_date
  spot_rag_ingestion.sh            passes --run-date "$EXECUTION_RUN_DATE"
  run_weekly_ingestion.sh          parses --run-date, threads it to every dated writer
  emit_manifest / filing_change_detection  key the dated object by it
  run_weekly_ingestion_recorded    passes its manifest trading_day as --run-date
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
RUN_WEEKLY = REPO_ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh"
SPOT_RAG = REPO_ROOT / "infrastructure" / "spot_rag_ingestion.sh"
STEP_FUNCTION = REPO_ROOT / "infrastructure" / "step_function.json"

CYCLE_DATE = "2026-09-18"


# ─────────────────────────── resolve_run_date ────────────────────────────────


def test_resolve_run_date_passes_an_explicit_date_through():
    from rag.pipelines._run_date import resolve_run_date

    assert resolve_run_date(CYCLE_DATE, producer="t") == CYCLE_DATE


@pytest.mark.parametrize("bad", ["2026-9-18", "20260918", "2026-09-18T00:00:00Z", "2026-02-30", "latest"])
def test_resolve_run_date_rejects_a_value_that_is_not_a_real_iso_date(bad):
    from rag.pipelines._run_date import resolve_run_date

    with pytest.raises(ValueError):
        resolve_run_date(bad, producer="t")


def test_resolve_run_date_fallback_is_utc_and_loud(caplog):
    from rag.pipelines._run_date import resolve_run_date

    with caplog.at_level(logging.WARNING, logger="rag.pipelines._run_date"):
        got = resolve_run_date(None, producer="emit_manifest")

    assert got == datetime.now(timezone.utc).date().isoformat()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings and "no --run-date" in warnings[0].getMessage()
    assert "emit_manifest" in warnings[0].getMessage()


# ─────────────────────────────── emit_manifest ───────────────────────────────


class _CapturingS3:
    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


def _run_emit_manifest(argv: list[str]) -> list[dict]:
    from rag.pipelines import emit_manifest

    s3 = _CapturingS3()
    with patch.object(emit_manifest, "build_manifest", return_value={"totals": {"documents": 1}}):
        with patch("boto3.client", return_value=s3):
            with patch.object(sys, "argv", ["emit_manifest", *argv]):
                emit_manifest.main()
    return s3.puts


def test_emit_manifest_keys_the_dated_object_by_run_date():
    puts = _run_emit_manifest(["--output-s3", "--bucket", "b", "--run-date", CYCLE_DATE])

    assert sorted(p["Key"] for p in puts) == [
        f"rag/manifest/{CYCLE_DATE}.json",
        "rag/manifest/latest.json",
    ]


def test_emit_manifest_rejects_a_malformed_run_date_before_writing():
    with pytest.raises(ValueError):
        _run_emit_manifest(["--output-s3", "--run-date", "2026/09/18"])


def test_emit_manifest_without_run_date_falls_back_to_utc_today():
    puts = _run_emit_manifest(["--output-s3", "--bucket", "b"])

    today = datetime.now(timezone.utc).date().isoformat()
    assert f"rag/manifest/{today}.json" in {p["Key"] for p in puts}


# ─────────────────────────── filing_change_detection ─────────────────────────


def _run_filing_changes(argv: list[str]) -> list[dict]:
    from rag.pipelines import filing_change_detection as fcd

    s3 = _CapturingS3()
    with patch.object(fcd, "compute_filing_changes", return_value=[]):
        with patch("boto3.client", return_value=s3):
            with patch.object(sys, "argv", ["filing_change_detection", *argv]):
                fcd.main()
    return s3.puts


def test_filing_changes_keys_the_dated_object_and_body_by_run_date():
    puts = _run_filing_changes(["--output-s3", "--bucket", "b", "--run-date", CYCLE_DATE])

    by_key = {p["Key"]: json.loads(p["Body"]) for p in puts}
    assert set(by_key) == {f"rag/filing_changes/{CYCLE_DATE}.json", "rag/filing_changes/latest.json"}
    assert {body["date"] for body in by_key.values()} == {CYCLE_DATE}


def test_filing_changes_key_prefix_still_prefixes_the_run_dated_key():
    puts = _run_filing_changes(
        ["--output-s3", "--bucket", "b", "--run-date", CYCLE_DATE, "--key-prefix", "canary/r1/"]
    )

    assert [p["Key"] for p in puts] == [f"rag/filing_changes/canary/r1/{CYCLE_DATE}.json"]


# ─────────────────────────── run_weekly_ingestion.sh ─────────────────────────


@pytest.fixture
def stub_python(tmp_path):
    """A PYTHON_BIN that records its argv and succeeds, so the script's flag
    parsing and RUN_DATE resolution run for real with no pipeline work."""
    log = tmp_path / "calls.log"
    stub = tmp_path / "python-stub"
    stub.write_text(f'#!/usr/bin/env bash\necho "$*" >> "{log}"\nexit 0\n')
    stub.chmod(0o755)
    return stub, log


def _run_script(stub: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "PYTHON_BIN": str(stub)}
    # --preflight-only exits after Step 0 — no ingest, no AWS call.
    return subprocess.run(  # noqa: S603 -- fixed argv, no shell
        ["bash", str(RUN_WEEKLY), "--preflight-only", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _progress_run_dates(log: Path) -> list[str]:
    dates = []
    for line in log.read_text().splitlines():
        if "rag.pipelines.emit_progress" in line:
            parts = line.split()
            dates.append(parts[parts.index("--run-date") + 1])
    return dates


@pytest.mark.parametrize("flag", [["--run-date", CYCLE_DATE], [f"--run-date={CYCLE_DATE}"]])
def test_script_threads_the_passed_run_date_to_progress_rows(stub_python, flag):
    stub, log = stub_python
    proc = _run_script(stub, *flag)

    assert proc.returncode == 0, proc.stderr
    assert _progress_run_dates(log) == [CYCLE_DATE]
    assert "falling back" not in proc.stderr


def test_script_without_run_date_falls_back_loudly(stub_python):
    stub, log = stub_python
    proc = _run_script(stub)

    assert proc.returncode == 0, proc.stderr
    assert "WARNING: no --run-date given" in proc.stderr
    m = re.search(r"RUN_DATE=(\d{4}-\d{2}-\d{2})", proc.stderr)
    assert m, proc.stderr
    assert _progress_run_dates(log) == [m.group(1)]
    # The UTC day can roll over between the script and this assertion.
    today = datetime.now(timezone.utc).date()
    assert m.group(1) in {today.isoformat(), (today - timedelta(days=1)).isoformat()}


@pytest.mark.parametrize("bad", [["--run-date", "2026/09/18"], ["--run-date"]])
def test_script_refuses_a_malformed_run_date(stub_python, bad):
    stub, log = stub_python
    proc = _run_script(stub, *bad)

    assert proc.returncode == 2
    assert "--run-date" in proc.stderr
    assert not log.exists() or "rag.pipelines.emit_progress" not in log.read_text()


@pytest.mark.parametrize("module", ["rag.pipelines.emit_manifest", "rag.pipelines.filing_change_detection"])
def test_script_passes_run_date_to_every_dated_writer(module):
    """Steps 9/10 are skipped under --dry-run and the full run reaches AWS, so
    their argv is pinned statically."""
    text = RUN_WEEKLY.read_text()
    calls = [line.strip() for line in text.splitlines() if line.strip().startswith(f"$PYTHON_BIN -m {module}")]
    assert calls, f"no invocation of {module}"
    for call in calls:
        assert '--run-date "$RUN_DATE"' in call, call


# ─────────────────────────── the SF → launcher carrier ───────────────────────


def _rag_ingestion_state() -> dict:
    sf = json.loads(STEP_FUNCTION.read_text())

    def walk(states):
        for name, state in states.items():
            if name == "RAGIngestion":
                return state
            for branch in state.get("Branches", []):
                found = walk(branch["States"])
                if found:
                    return found
        return None

    state = walk(sf["States"])
    assert state is not None, "RAGIngestion state not found"
    return state


def test_sf_rag_ingestion_exports_the_cycle_date_to_the_launcher():
    commands = _rag_ingestion_state()["Parameters"]["Parameters"]["commands.$"]
    assert "export EXECUTION_RUN_DATE" in commands and "$.run_date" in commands
    assert commands.index("EXECUTION_RUN_DATE") < commands.index("spot_rag_ingestion.sh")


def test_launcher_passes_execution_run_date_to_the_ingestion_script():
    text = SPOT_RAG.read_text()
    assert '_RAG_RUN_DATE_ARGS="--run-date ${EXECUTION_RUN_DATE}"' in text
    run_lines = [
        line
        for line in text.splitlines()
        if "bash rag/pipelines/run_weekly_ingestion.sh" in line
        and "--dry-run" not in line
        and "--preflight-only" not in line
    ]
    assert run_lines, "full-run invocation not found"
    assert all("${_RAG_RUN_DATE_ARGS}" in line for line in run_lines), run_lines


# ─────────────────────────── the D16 recorded wrapper ────────────────────────


def test_recorded_wrapper_always_passes_run_date_to_the_script():
    from rag.pipelines import run_weekly_ingestion_recorded as module

    argv = module._ingestion_argv(False, CYCLE_DATE)
    assert argv[:2] == ["bash", str(RUN_WEEKLY)]
    assert argv[argv.index("--run-date") + 1] == CYCLE_DATE

    assert "--dry-run" in module._ingestion_argv(True, CYCLE_DATE)
