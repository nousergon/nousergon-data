"""Pin ``requirements.txt`` + ``Dockerfile`` to the same alpha-engine-lib version.

The Dockerfile strips alpha-engine-lib from ``requirements.txt`` before
``pip install`` (see the ``grep -vE ...alpha-engine-lib`` line in the
Dockerfile RUN block) and instead installs the lib via a hardcoded
``pip install "alpha-engine-lib@vX.Y.Z"`` line ABOVE that grep. So
bumping ``requirements.txt`` alone does NOT propagate to the Lambda
image — the Dockerfile's hardcoded pin wins.

This drift class has bitten production multiple times:

  - 2026-05-06 (research): requirements.txt bumped @v0.4.0 → @v0.5.1
    but Dockerfile kept v0.3.0; Research Lambda canary failed with
    ``ModuleNotFoundError: alpha_engine_lib.agent_schemas``.
  - 2026-05-12 (predictor): requirements.txt → v0.12.0 but
    requirements-lambda.txt stayed v0.9.1; predictor canary failed
    with ``ModuleNotFoundError: alpha_engine_lib.secrets``.
  - 2026-05-12 (data, this repo): requirements.txt → v0.12.0 in PR
    #221 but Dockerfile kept v0.3.0 (a 9-version-old pin); data
    Lambda canary failed at 17:22 UTC with the same
    ``alpha_engine_lib.secrets`` ModuleNotFoundError.

This test re-greps both files on every CI run so a future single-file
bump fails here, not in a canary.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

_REQUIREMENTS_PIN_RE = re.compile(
    r"alpha-engine-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)
_DOCKERFILE_PIN_RE = re.compile(
    r'"alpha-engine-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"'
)


def _read_pin(filename: str, regex: re.Pattern[str]) -> str:
    text = (_REPO_ROOT / filename).read_text()
    match = regex.search(text)
    assert match is not None, (
        f"could not find alpha-engine-lib pin in {filename}"
    )
    return match.group(1)


def test_requirements_and_dockerfile_pins_match():
    req_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    docker_pin = _read_pin("Dockerfile", _DOCKERFILE_PIN_RE)
    assert req_pin == docker_pin, (
        f"alpha-engine-lib pin drift: requirements.txt={req_pin!r} but "
        f"Dockerfile={docker_pin!r}. Both must move in lockstep — the "
        f"Dockerfile strips lib from requirements.txt before pip install, "
        f"so requirements-only bumps don't propagate to the Lambda image."
    )
