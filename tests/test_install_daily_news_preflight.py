"""install-daily-news.sh + run_daily_news_standalone.sh must provision a
python3.12 venv and prove it can import the run's real closure BEFORE enabling
the timer / invoking the collector (alpha-engine-config#6063).

WHY
---
The dashboard box's shared .venv was created with AL2023's default python3.9.
requirements-daily-news.txt pins boto3>=1.43.59 (Python 3.10+) and
numpy>=2.4.6 (Python 3.11+), so `pip install` failed on EVERY run and the venv
could never advance — the RAG-ingest deps config-I5702 added
(psycopg2-binary/pgvector via the lib's [rag] extra, voyageai for embeddings)
never landed, and every daily run silently skipped corpus warming with only a
WARN in the log. daily_news.py's RAG ingest is fail-soft by design, so the
missing-import failure mode was invisible for days.

The venv fix is two-layer: the RUNNER self-heals (rebuild-on-stale-interpreter)
on every run, and the INSTALLER provisions + preflights at install time.

Source-text assertions: both scripts run on an EC2 box (the runner as
ec2-user via systemd; the installer as root via SSM), so executing them in CI
is not meaningful. What these pin is that the rebuild-on-stale-interpreter
exists, the RAG secrets SSM fetch exists and fails loud, and the preflight
runs before the thing it guards.
"""

from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent

_RUNNER = (_REPO / "scripts" / "run_daily_news_standalone.sh").read_text()
_INSTALLER = (_REPO / "infrastructure" / "install-daily-news.sh").read_text()
_REQS = (_REPO / "requirements-daily-news.txt").read_text()


def test_requirements_carry_the_rag_ingest_deps():
    """The daily_news RAG ingest chain (config-I5702) imports nousergon_lib.rag
    (psycopg2-binary/pgvector via the lib's [rag] extra) and uses voyageai at
    embed time. Without them the lazy import inside daily_news.py's fail-soft
    try/except degrades silently — the exact #6063 failure."""
    assert "nousergon-lib[rag,flow_doctor]" in _REQS, (
        "the slim venv must ride the lib's [rag] extra so psycopg2-binary/"
        "pgvector stay in lockstep with the lib pin"
    )
    assert "voyageai>=0.5.0" in _REQS, (
        "voyageai has no lib extra; it mirrors requirements.txt's own pin"
    )


def test_runner_rebuilds_a_stale_interpreter_venv():
    """The box's venv is python3.9; the requirements need Python 3.10+/3.11+.
    'Provision if missing' alone would leave the stale 3.9 venv in place
    forever — the runner must detect the stale interpreter and rebuild on
    python3.12 (the interpreter install-metron-intraday.sh provisions on the
    same box)."""
    assert "python3.12 -m venv" in _RUNNER
    assert "version_info >= (3, 11)" in _RUNNER
    assert "rm -rf" in _RUNNER


def test_runner_fetches_rag_secrets_from_ssm_fail_loud():
    """RAG_DATABASE_URL + VOYAGE_API_KEY come from SSM Parameter Store (the
    SCP'd .env lacks them — spot_data_weekly.sh's Phase-2 SSM migration).
    A missing secret is an environment error that fails EVERY run; fail loud
    rather than re-entering the silent-degradation loop #6063 closed."""
    assert "aws ssm get-parameter" in _RUNNER
    assert "/alpha-engine/$name" in _RUNNER
    for name in ("RAG_DATABASE_URL", "VOYAGE_API_KEY"):
        assert name in _RUNNER


def test_runner_preflight_runs_before_the_collector():
    """The preflight proves the venv can import the run's real closure
    (including the RAG chain's lazy imports) BEFORE the collector runs —
    a check after the fact proves nothing."""
    assert "preflight import failed" in _RUNNER
    assert _RUNNER.index("preflight import failed") < _RUNNER.index(
        "--require-digest"
    )


def test_installer_preflight_runs_before_the_timer_is_enabled():
    """Order is the whole point: a passing check after `enable --now` proves
    nothing (metron-intraday 2026-07-29 incident class)."""
    assert "preflight import failed" in _INSTALLER
    assert _INSTALLER.index("preflight import failed") < _INSTALLER.index(
        "systemctl enable --now daily-news.timer"
    )
