"""Pin every lib-install surface to the same nousergon-lib version.

(The dist was renamed ``alpha-engine-lib`` → ``nousergon-lib`` at v0.60.0;
the historical incidents below predate the rename and reference the old
``nousergon_lib`` import name accordingly — kept verbatim as the
drift-class record.)

The Dockerfile strips nousergon-lib from ``requirements.txt`` before
``pip install`` (see the ``grep -vE ...nousergon-lib`` line in the
Dockerfile RUN block) and instead installs the lib via a hardcoded
``pip install "nousergon-lib@vX.Y.Z"`` line ABOVE that grep. So
bumping ``requirements.txt`` alone does NOT propagate to the Lambda
image — the Dockerfile's hardcoded pin wins. The slim
``requirements-daily-news.txt`` (standalone daily-news collector on the
dashboard box) carries its own copy of the pin and its header demands
lockstep with ``requirements.txt`` — so it is guarded here too.

Some Lambdas have deliberate exemptions documented in their requirements.txt
comments. These must move in lockstep within their exemption group (e.g., all
spot-dispatch Lambdas stay together) and MUST NOT silently drift from their
documented version without a named contract reason.

This drift class has bitten production multiple times:

  - 2026-05-06 (research): requirements.txt bumped @v0.4.0 → @v0.5.1
    but Dockerfile kept v0.3.0; Research Lambda canary failed with
    ``ModuleNotFoundError: nousergon_lib.agent_schemas``.
  - 2026-05-12 (predictor): requirements.txt → v0.12.0 but
    requirements-lambda.txt stayed v0.9.1; predictor canary failed
    with ``ModuleNotFoundError: nousergon_lib.secrets``.
  - 2026-05-12 (data, this repo): requirements.txt → v0.12.0 in PR
    #221 but Dockerfile kept v0.3.0 (a 9-version-old pin); data
    Lambda canary failed at 17:22 UTC with the same
    ``nousergon_lib.secrets`` ModuleNotFoundError.

This test re-greps all three files on every CI run so a future single-file
bump fails here, not in a canary.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

_REQUIREMENTS_PIN_RE = re.compile(
    r"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)
_DOCKERFILE_PIN_RE = re.compile(
    r'"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"'
)
_LAMBDA_PIN_RE = re.compile(
    r"nousergon-lib(?:\[[^\]]*\])?\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)

# Lambda exemptions: deliberate pins outside the root lockstep guard,
# documented in each Lambda's requirements.txt header comment.
# Key: lambda directory name, Value: (pin version, contract reason)
_LAMBDA_PIN_EXEMPTIONS = {
    "canary-replay-dispatcher": (
        "v0.106.0",
        "nousergon_lib.spot_dispatch chokepoint (alpha-engine-config#2246: same SpotProbeError "
        "handling as ci-watch-dispatcher)",
    ),
    "ci-watch-dispatcher": (
        "v0.122.0",
        "nousergon_lib.spot_dispatch chokepoint (config#2267: SpotProbeError handling; "
        "bumped for extra_tags atomic-launch-tagging, config#2292)",
    ),
    "config-runner-dispatcher": (
        "v0.110.0",
        "nousergon_lib.spot_dispatch chokepoint (alpha-engine-config-I2572: same SpotProbeError "
        "handling as ci-watch-dispatcher, pinned to the latest tag at time of writing)",
    ),
    "metron-runner-dispatcher": (
        "v0.110.0",
        "nousergon_lib.spot_dispatch chokepoint — mirrors config-runner-dispatcher's pin exactly "
        "(same SpotProbeError handling requirement, same source code, 2026-07-17 metron/telos GHA "
        "quota migration)",
    ),
    "telos-runner-dispatcher": (
        "v0.110.0",
        "nousergon_lib.spot_dispatch chokepoint — mirrors config-runner-dispatcher's pin exactly "
        "(same SpotProbeError handling requirement, same source code, 2026-07-17 metron/telos GHA "
        "quota migration)",
    ),
    "vires-runner-dispatcher": (
        "v0.110.0",
        "nousergon_lib.spot_dispatch chokepoint — mirrors config-runner-dispatcher's pin exactly "
        "(same SpotProbeError handling requirement, same source code, 2026-07-17 vires GHA "
        "quota migration)",
    ),
    "data-spot-dispatcher": (
        "v0.83.0",
        "ec2_spot launch chokepoint (config#1767)",
    ),
    "eod-backstop": (
        "v0.83.0",
        "trading_calendar coherence with sibling Lambdas",
    ),
    "eod-success-friday-shell-trigger": (
        "v0.83.0",
        "date helpers coherence with sf-telegram-notifier",
    ),
    "freshness-monitor": (
        "v0.85.0",
        "flow-doctor event_driven + liveness_via (config#1747/1718/1726)",
    ),
    "friday-shell-run-report": (
        "v0.83.0",
        "trading_calendar coherence with eod-success-friday-shell-trigger",
    ),
    "groom-liveness-probe": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "pipeline-watchdog": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "saturday-integrity-sentinel": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "saturday-sf-watch-dispatcher": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "scheduled-groom-dispatcher": (
        "v0.124.0",
        "spot_dispatch + SlotDecision + label-exclude parity (config#2146/2106/2129); "
        "bumped for TIER_MODELS[\"high\"] Opus->Sonnet (config#2409); "
        "v0.124.0 for nousergon_lib.github_app — _github_token() mints the "
        "ne-groomer App installation token first, PAT fallback (config-I2785, "
        "nousergon-lib#220, incident config-I2784)",
    ),
    "sf-telegram-notifier": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "sf-watch-liveness-probe": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "sf-watch-spot-dispatcher": (
        "v0.122.0",
        "nousergon_lib.spot_dispatch chokepoint (config#2267: SpotProbeError handling; "
        "bumped for extra_tags atomic-launch-tagging, config#2292)",
    ),
    "spot-orphan-reaper": (
        "v0.97.0",
        "telegram alert shape for CI-watch (config#2106)",
    ),
}


def _read_pin(filename: str, regex: re.Pattern[str]) -> str:
    text = (_REPO_ROOT / filename).read_text()
    match = regex.search(text)
    assert match is not None, (
        f"could not find nousergon-lib pin in {filename}"
    )
    return match.group(1)


def test_requirements_and_dockerfile_pins_match():
    req_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    docker_pin = _read_pin("Dockerfile", _DOCKERFILE_PIN_RE)
    daily_news_pin = _read_pin("requirements-daily-news.txt", _REQUIREMENTS_PIN_RE)
    assert req_pin == docker_pin == daily_news_pin, (
        f"nousergon-lib pin drift: requirements.txt={req_pin!r}, "
        f"Dockerfile={docker_pin!r}, requirements-daily-news.txt={daily_news_pin!r}. "
        f"All three must move in lockstep — the Dockerfile strips lib from "
        f"requirements.txt before pip install, so requirements-only bumps "
        f"don't propagate to the Lambda image, and the slim daily-news file "
        f"carries an independent copy of the pin."
    )


def test_lambda_pins_match_or_are_explicitly_exempted():
    root_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    lambdas_dir = _REPO_ROOT / "infrastructure" / "lambdas"

    for req_file in sorted(lambdas_dir.glob("*/requirements.txt")):
        lambda_name = req_file.parent.name
        text = req_file.read_text()
        match = _LAMBDA_PIN_RE.search(text)

        if match is None:
            continue

        lambda_pin = match.group(1)

        if lambda_name in _LAMBDA_PIN_EXEMPTIONS:
            exempted_pin, reason = _LAMBDA_PIN_EXEMPTIONS[lambda_name]
            assert (
                lambda_pin == exempted_pin
            ), f"{lambda_name}: pin {lambda_pin!r} does not match exempted pin {exempted_pin!r} (reason: {reason})"
        else:
            assert (
                lambda_pin == root_pin
            ), f"{lambda_name}: pin {lambda_pin!r} must match root pin {root_pin!r}, or be added to _LAMBDA_PIN_EXEMPTIONS with a contract reason"
