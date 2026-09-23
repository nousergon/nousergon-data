"""Dispatch the gate read the moment a parity report is published.

`alpha-engine-config-I11361`. `data_gate.read.TRIGGERS` has declared
``parity-published`` since `-I11355`, but nothing emitted it: the box's only
GitHub credential was the fleet PAT at
``/alpha-engine/saturday_sf_watch/github_pat``, whose scope is undeclared and
must not be widened or probed. So the gate read followed the publish on two
crons (01:30Z / 12:30Z), up to ~1h47m after the report landed.

This module is the event-driven half. After ``shadow parity --dispatch-gate``
publishes ``parity/{D}.json`` it mints a short-lived installation token from
a GitHub App installed on ``nousergon-data`` alone (Brian's ruling (a) on the
issue), narrowed at mint time to ``actions: write`` on this one repository,
and ``workflow_dispatch``-es ``data-gate.yml`` with ``trading_day=D`` and
``trigger=parity-published``.

**Never fatal.** The parity exit code is the comparison's verdict, and a
dispatch failure says nothing about whether the keys matched. A failure is
printed to stderr and recorded in the published report as
``gate_dispatch: {ok: false, error: ...}``; the two post-publish crons stay
as the backstop, so a missed dispatch costs latency, never a reading.

The App credentials live at ``/alpha-engine/data-spot/github_app_id``,
``..._installation_id`` and ``..._private_key``, the same three-name shape
``nousergon_lib.github_app`` reads for the groomer under
``/alpha-engine/groom/``. The box role is granted exactly those three
parameters and nothing else under the prefix.
"""

from __future__ import annotations

import datetime as dt
import json
import urllib.request
from typing import Callable

GATE_REPO_OWNER = "nousergon"
GATE_REPO_NAME = "nousergon-data"
GATE_WORKFLOW = "data-gate.yml"
GATE_REF = "main"
GATE_TRIGGER = "parity-published"
APP_SSM_PREFIX = "/alpha-engine/data-spot/"
#: The only permission the minted token carries, whatever the installation
#: itself was granted. Narrowing at mint time is what keeps a mis-scoped
#: installation from becoming a mis-scoped token on the box.
TOKEN_PERMISSIONS = {"actions": "write"}
REGION = "us-east-1"
HTTP_TIMEOUT_SECONDS = 20
#: A recorded error is for a human reading the report, not a traceback dump.
MAX_ERROR_CHARS = 300


def dispatch_url() -> str:
    return (
        f"https://api.github.com/repos/{GATE_REPO_OWNER}/{GATE_REPO_NAME}"
        f"/actions/workflows/{GATE_WORKFLOW}/dispatches"
    )


def dispatch_payload(trading_day: dt.date) -> dict:
    """The body `data-gate.yml`'s `workflow_dispatch` inputs accept.

    Pinned against the workflow's declared inputs by
    `tests/test_parity_gate_dispatch.py`, so a rename on either side fails CI
    rather than a dispatch at 23:43Z.
    """
    return {
        "ref": GATE_REF,
        "inputs": {"trading_day": trading_day.isoformat(), "trigger": GATE_TRIGGER},
    }


def mint_token() -> str:
    from nousergon_lib.github_app import installation_token

    return installation_token(
        ssm_prefix=APP_SSM_PREFIX,
        region=REGION,
        permissions=TOKEN_PERMISSIONS,
        repositories=[GATE_REPO_NAME],
    )


def post_dispatch(url: str, token: str, payload: dict) -> int:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:  # noqa: S310 - fixed https URL
        return response.status


def dispatch_gate_read(
    trading_day: dt.date,
    *,
    mint: Callable[[], str] = mint_token,
    post: Callable[[str, str, dict], int] = post_dispatch,
) -> dict:
    """Dispatch the gate read for ``trading_day``; return what happened.

    The returned dict is written into the parity report verbatim as
    ``gate_dispatch``, so ``ok: false`` with a reason is a visible fact on
    the same object the gate grades.
    """
    payload = dispatch_payload(trading_day)
    record = {
        "ok": False,
        "error": None,
        "workflow": f"{GATE_REPO_OWNER}/{GATE_REPO_NAME}/{GATE_WORKFLOW}",
        "inputs": payload["inputs"],
        "dispatched_at": None,
    }
    try:
        token = mint()
        status = post(dispatch_url(), token, payload)
    except Exception as exc:  # noqa: BLE001 - recorded in the report and on stderr, never fatal
        # Swallowed deliberately: (a) any failure here (App not installed, SSM
        # AccessDenied on a box still running the executor profile, GitHub
        # down) leaves the comparison itself untouched; (b) the two
        # post-publish crons in data-gate.yml read the same report shortly
        # after; (c) it is recorded as `gate_dispatch.ok: false` with the
        # reason inside the published report, and printed by the caller.
        record["error"] = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_CHARS]
        return record
    if status != 204:
        record["error"] = f"unexpected HTTP {status} from the workflow_dispatch endpoint (expected 204)"
        return record
    record["ok"] = True
    record["dispatched_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    return record
