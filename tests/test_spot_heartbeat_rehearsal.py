"""The spot launchers' shared heartbeat stays silent under a rehearsal (alpha-engine-config-I12375).

``emit_heartbeat`` in ``infrastructure/_spot_common.sh`` puts CloudWatch
``AlphaEngine/Heartbeat Process=<stage>`` after the workload. It is called by
``spot_rag_ingestion.sh``, ``spot_data_phase1.sh`` and
``spot_morning_enrich.sh``. Until this test it ran unconditionally, so a
weekly-SF rehearsal's RAGIngestion made the live ``rag-ingestion`` freshness
signal look fresh when production had not run — the one writer #2191 left.

These tests EXECUTE the function with ``aws`` stubbed and every call recorded:

  * under ``RUN_TOKEN=rehearsal-*`` no ``aws`` call is made;
  * a production token, and no token, make exactly the call they made before,
    with the same arguments and the same output.
"""

from __future__ import annotations

import re
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SPOT_COMMON = ROOT / "infrastructure" / "_spot_common.sh"

PRODUCTION_CALL = [
    "cloudwatch", "put-metric-data",
    "--namespace", "AlphaEngine",
    "--metric-name", "Heartbeat",
    "--dimensions", "Process=rag-ingestion",
    "--value", "1", "--unit", "Count",
    "--region", "us-east-1",
]


def _emit_heartbeat_fn() -> str:
    text = SPOT_COMMON.read_text()
    m = re.search(r"^emit_heartbeat\(\) \{\n.*?^\}\n", text, re.S | re.M)
    assert m, "_spot_common.sh no longer defines emit_heartbeat() the way this test reads it"
    return m.group(0)


def _run(tmp_path: Path, run_token: str | None) -> tuple[subprocess.CompletedProcess, list[list[str]]]:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "calls.log"
    log.touch()
    aws = bindir / "aws"
    aws.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\0" "$@" >> "{log}"\n'
        f'printf "\\036" >> "{log}"\n'
        "exit 0\n"
    )
    aws.chmod(aws.stat().st_mode | stat.S_IEXEC)
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "HOME": str(tmp_path)}
    if run_token is not None:
        env["RUN_TOKEN"] = run_token
    script = "set -euo pipefail\n_PROCESS_NAME=rag-ingestion\n" + _emit_heartbeat_fn() + "emit_heartbeat\n"
    proc = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True)
    calls = [
        [a for a in rec.split("\0") if a]
        for rec in log.read_text().split("\036")
        if rec.strip("\0")
    ]
    return proc, calls


@pytest.mark.parametrize("token", ["rehearsal-2026-10-21-1", "rehearsal-2026-10-16-2"])
def test_rehearsal_emits_no_heartbeat(tmp_path, token):
    proc, calls = _run(tmp_path, token)
    assert proc.returncode == 0, proc.stderr
    assert calls == []
    assert "REHEARSAL: production heartbeat (Process=rag-ingestion) not emitted" in proc.stdout


@pytest.mark.parametrize("token", [None, "", "2026-10-24-weekly", "spot-data-weekly-20261024", "not-a-rehearsal-x"])
def test_production_heartbeat_is_unchanged(tmp_path, token):
    proc, calls = _run(tmp_path, token)
    assert proc.returncode == 0, proc.stderr
    assert calls == [PRODUCTION_CALL]
    assert proc.stdout == "Heartbeat emitted: rag-ingestion\n"


def test_every_stage_launcher_still_calls_the_shared_function():
    """The guard lives in the shared function, so it covers every stage that calls it."""
    for name in ("spot_rag_ingestion.sh", "spot_data_phase1.sh", "spot_morning_enrich.sh"):
        text = (ROOT / "infrastructure" / name).read_text()
        assert re.search(r"^emit_heartbeat$", text, re.M), f"{name} no longer calls emit_heartbeat"
        assert "Heartbeat" not in text, (
            f"{name} emits its own heartbeat outside the rehearsal-guarded shared function"
        )
