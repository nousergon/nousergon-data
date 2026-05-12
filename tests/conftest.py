"""Test fixtures + sys.path setup.

Pins ``ALPHA_ENGINE_SECRETS_SOURCE=env`` for every test so
``alpha_engine_lib.secrets.get_secret()`` reads from monkeypatched
env vars only — never the real SSM Parameter Store. Without this,
tests that simulate "missing API key" via ``monkeypatch.delenv``
would be silently no-op'd by a live SSM read. See
``alpha-engine-docs/private/env-to-ssm-260512.md`` § Risks.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def _isolate_secrets_from_ssm(monkeypatch):
    """Force every test to read secrets from env only, not SSM.

    Also clears the per-process secret cache before each test so a prior
    test's reads don't leak into the next test's state.
    """
    monkeypatch.setenv("ALPHA_ENGINE_SECRETS_SOURCE", "env")
    try:
        from alpha_engine_lib.secrets import clear_cache
    except ImportError:
        # Lib not installed (rare — tests that don't import secrets path).
        yield
        return
    clear_cache()
    yield
    clear_cache()
