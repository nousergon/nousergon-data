"""A weekly-SF rehearsal's RAGIngestion writes nothing live (alpha-engine-config-I12375).

rehearsal-2026-10-16-1 ran ``run_weekly_ingestion.sh`` exactly as Saturday
does and stored 15 documents (103 chunks) in the production pgvector corpus:
CTAS and TRV 10-Qs, 11 8-Ks, and NOW/VLTO 13F summaries. Both 10-16 rehearsals
also overwrote ``data/insider_transactions/latest.json``, the analyst snapshot
series, ``rag/manifest/latest.json``, ``rag/filing_changes/latest.json``,
``rag/corpus_freshness/latest.json`` and the production heartbeat. Corpus dedup
is by (ticker, doc_type, filed_date, source), so every document a rehearsal
stores is one Saturday's run then finds "already held".

These tests EXECUTE the script with the interpreter and the AWS CLI stubbed,
recording every call, so what is pinned is what the script does, not what its
text says:

  * under ``RUN_TOKEN=rehearsal-*`` every ingest step gets ``--dry-run``, the
    freshness assertion gets ``--no-write``, the change-detection and manifest
    steps do not run, no heartbeat is emitted, and the only S3 writes land
    under ``rehearsal/<execution>/``;
  * a production token, and no token, run byte-for-byte as before;
  * the shell's prefix is ``emailer.REHEARSAL_PREFIX``.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

import emailer
from rag.pipelines import emit_progress as ep

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "rag" / "pipelines" / "run_weekly_ingestion.sh"

INGEST_MODULES = (
    "rag.pipelines.ingest_sec_filings",
    "rag.pipelines.ingest_8k_filings",
    "rag.pipelines.ingest_theses",
    "rag.pipelines.ingest_form4",
    "rag.pipelines.ingest_13f",
    "rag.pipelines.run_analyst_pipeline",
)
WRITE_ONLY_STEPS = (
    "rag.pipelines.filing_change_detection",
    "rag.pipelines.emit_manifest",
)


def _stub(path: Path, log: Path, name: str) -> None:
    path.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\0" "{name}" "$@" >> "{log}"\n'
        f'printf "\\036" >> "{log}"\n'
        'if [ "${1:-}" = "--version" ]; then echo "Python 3.12.0"; fi\n'
        "exit 0\n"
    )
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


def _run(tmp_path: Path, run_token: str | None) -> tuple[subprocess.CompletedProcess, list[list[str]]]:
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "calls.log"
    log.touch()
    _stub(bindir / "python-stub", log, "python")
    _stub(bindir / "aws", log, "aws")
    env = {
        "PATH": f"{bindir}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "TMPDIR": str(tmp_path),
        "PYTHON_BIN": str(bindir / "python-stub"),
        "RAG_SOURCE_YIELD_DIR": str(tmp_path / "yield"),
    }
    if run_token is not None:
        env["RUN_TOKEN"] = run_token
    proc = subprocess.run(
        ["bash", str(SCRIPT), "--run-date", "2026-10-15"],
        env=env, capture_output=True, text=True, timeout=120,
    )
    # One record per call (RS-terminated; the email step's `-c` body spans
    # lines), NUL-terminated argv so an empty argument survives.
    calls = [rec.split("\0")[:-1] for rec in log.read_text().split("\036") if rec]
    return proc, calls


def _module_calls(calls: list[list[str]], module: str) -> list[list[str]]:
    return [c for c in calls if c[0] == "python" and "-m" in c and module in c]


def _aws_calls(calls: list[list[str]]) -> list[list[str]]:
    return [c[1:] for c in calls if c[0] == "aws"]


REHEARSAL = "rehearsal-2026-10-16-2"


@pytest.fixture()
def rehearsal_run(tmp_path):
    proc, calls = _run(tmp_path, REHEARSAL)
    assert proc.returncode == 0, proc.stderr
    return proc, calls


@pytest.mark.parametrize("module", INGEST_MODULES)
def test_rehearsal_forces_dry_run_on_every_ingest_step(rehearsal_run, module):
    _, calls = rehearsal_run
    hits = _module_calls(calls, module)
    assert len(hits) == 1, f"{module} should run exactly once, got {hits}"
    assert "--dry-run" in hits[0], f"{module} ran WITHOUT --dry-run under a rehearsal"


def test_rehearsal_freshness_assertion_publishes_nothing(rehearsal_run):
    _, calls = rehearsal_run
    (hit,) = _module_calls(calls, "rag.pipelines.assert_corpus_freshness")
    assert "--no-write" in hit


@pytest.mark.parametrize("module", WRITE_ONLY_STEPS)
def test_rehearsal_skips_the_write_only_steps(rehearsal_run, module):
    _, calls = rehearsal_run
    assert _module_calls(calls, module) == []


def test_rehearsal_progress_ticks_are_namespaced(rehearsal_run):
    _, calls = rehearsal_run
    ticks = _module_calls(calls, "rag.pipelines.emit_progress")
    assert len(ticks) == 10, "steps 0-9 each tick once"
    for tick in ticks:
        i = tick.index("--key-prefix")
        assert tick[i + 1] == f"rehearsal/{REHEARSAL}/"


def test_rehearsal_emits_no_heartbeat_and_writes_s3_only_in_its_namespace(rehearsal_run):
    proc, calls = rehearsal_run
    aws = _aws_calls(calls)
    assert not any(c[:2] == ["cloudwatch", "put-metric-data"] for c in aws), aws
    s3_writes = [c for c in aws if c[:1] == ["s3"]]
    assert s3_writes, "the rehearsal's source-yield record should be uploaded"
    for c in s3_writes:
        dests = [a for a in c if a.startswith("s3://")]
        assert dests and all(
            d.startswith(f"s3://alpha-engine-research/rehearsal/{REHEARSAL}/") for d in dests
        ), c
    assert "forcing --dry-run" in proc.stdout


@pytest.mark.parametrize(
    "token",
    [
        None,
        "ff6ac9fe-90a8-471f-9905-d73f00012ff5",  # scheduled Saturday run
        "weekly-rerun-2026-10-10-d16-2",
        "my-rehearsal-2026-10-16-1",  # prefix, not substring
    ],
)
def test_production_runs_unchanged(tmp_path, token):
    proc, calls = _run(tmp_path, token)
    assert proc.returncode == 0, proc.stderr
    for module in INGEST_MODULES:
        (hit,) = _module_calls(calls, module)
        assert "--dry-run" not in hit, module
    (fresh,) = _module_calls(calls, "rag.pipelines.assert_corpus_freshness")
    assert "--no-write" not in fresh
    for module in WRITE_ONLY_STEPS:
        assert len(_module_calls(calls, module)) == 1, module
    ticks = _module_calls(calls, "rag.pipelines.emit_progress")
    assert len(ticks) == 10
    for tick in ticks:
        assert tick[tick.index("--key-prefix") + 1] == "", "production keys stay unprefixed"
    aws = _aws_calls(calls)
    assert any(c[:2] == ["cloudwatch", "put-metric-data"] for c in aws)
    assert not any(c[:1] == ["s3"] for c in aws), "production uploads no rehearsal record"
    assert "REHEARSAL" not in proc.stdout


def test_unsafe_rehearsal_token_is_refused(tmp_path):
    proc, calls = _run(tmp_path, "rehearsal-x/../../health")
    assert proc.returncode == 2
    assert not any(c[0] == "python" and "-m" in c for c in calls), "nothing ran"


def test_shell_prefix_is_the_emailer_prefix():
    assert emailer.REHEARSAL_PREFIX == "rehearsal-"
    assert f"    {emailer.REHEARSAL_PREFIX}*) REHEARSAL_EXECUTION=" in SCRIPT.read_text()


# ── emit_progress key ────────────────────────────────────────────────────────


def test_production_progress_key_is_unchanged():
    assert ep.progress_key("2026-10-16") == "health/rag_ingestion_progress/2026-10-16.json"


def test_rehearsal_progress_key_is_namespaced():
    assert ep.progress_key("2026-10-15", f"rehearsal/{REHEARSAL}/") == (
        f"rehearsal/{REHEARSAL}/health/rag_ingestion_progress/2026-10-15.json"
    )


@pytest.mark.parametrize("prefix", ["health/", "rehearsal/", "rehearsal/x/", "rehearsal/rehearsal-a/\n", "/"])
def test_unknown_prefix_writes_nothing(monkeypatch, prefix):
    class _Boto3:
        @staticmethod
        def client(*_a, **_k):
            raise AssertionError("no S3 client may be created for an unvouched key")

    monkeypatch.setitem(sys.modules, "boto3", _Boto3)
    assert ep.emit_progress(
        run_date="2026-10-15", step=1, of=9, label="sec_filings",
        started_at="2026-10-16T20:16:05Z", key_prefix=prefix,
    ) is False
