"""Regression: no module in this repo reads a secret via ``os.environ.get``.

After the 2026-05-12 ``.env`` → SSM migration (PR 2 of the arc), every
secret-bearing call site routes through ``alpha_engine_lib.secrets.get_secret()``.
This test re-greps the codebase on every CI run so a future commit can't
silently re-introduce an ``os.environ.get("POLYGON_API_KEY")`` style read.

Non-secret env vars (``LANGCHAIN_PROJECT``, ``EMAIL_SENDER``, etc.) are
allowed for now — they migrate to alpha-engine-config YAML in PR 8 of the
arc. The pin here is only the secret-name set.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Secret names that must NEVER be read via os.environ.get / os.getenv.
# EMAIL_SENDER + EMAIL_RECIPIENTS aren't secrets per se but live in SSM
# under /alpha-engine/* and route through get_secret() — pinning them
# here prevents regressions to the bulk-load-into-os.environ shim
# pattern that PR 8 of the .env→SSM arc retired.
_PINNED_SECRETS = frozenset(
    [
        "ANTHROPIC_API_KEY",
        "LANGCHAIN_API_KEY",
        "VOYAGE_API_KEY",
        "POLYGON_API_KEY",
        "FMP_API_KEY",
        "FINNHUB_API_KEY",
        "FRED_API_KEY",
        "GMAIL_APP_PASSWORD",
        "GITHUB_TOKEN",
        "RAG_DATABASE_URL",
        "EDGAR_IDENTITY",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "EMAIL_SENDER",
        "EMAIL_RECIPIENTS",
    ]
)

# Files that are explicitly allowed to read secrets via os.environ — the
# legacy ssm_secrets.py bulk-load shim is the only one; it covers
# non-migrated reads in this repo and other repos pending PRs 3-7. The
# shim itself is removed in PR 9 of the arc.
_ALLOWED_FILES = frozenset(["ssm_secrets.py"])

_ENV_READ_RE = re.compile(
    r'os\.(?:environ\.get|getenv)\(\s*["\']([A-Z_][A-Z0-9_]*)["\']'
)


def _iter_python_files():
    for path in _REPO_ROOT.rglob("*.py"):
        # Skip venv / build / tests / dot-dirs.
        parts = set(path.parts)
        if parts & {".venv", "build", "tests", "node_modules"}:
            continue
        if path.name in _ALLOWED_FILES:
            continue
        yield path


def test_no_secret_environ_reads():
    """Grep for ``os.environ.get("SECRET")`` over the codebase."""
    violations: list[tuple[Path, int, str]] = []
    for path in _iter_python_files():
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), start=1):
            for match in _ENV_READ_RE.finditer(line):
                name = match.group(1)
                if name in _PINNED_SECRETS:
                    violations.append((path.relative_to(_REPO_ROOT), lineno, name))
    assert not violations, (
        "Found os.environ.get reads of pinned secrets — use "
        "`from alpha_engine_lib.secrets import get_secret` instead:\n"
        + "\n".join(f"  {p}:{ln}  {name}" for p, ln, name in violations)
    )
