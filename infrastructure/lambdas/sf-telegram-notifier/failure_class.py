"""Classify a failed Step Functions run and say what happens next.

Brian, 2026-11-02, after the preopen refused to trade on a 10-day-old champion
cohort: "we don't currently have an automatic way to handle this?" The alert
carried the error but never said whether anything would retry it, so the
reader had to work out alone that a retry could not help.

Every FAILED / TIMED_OUT / DEGRADED terminal alert now ends with one
``Auto-fix:`` line that answers that question. The class is read from the
failure text alone (the detailed cause the digest already extracts), so it
costs no extra AWS call.

Measured 2026-11-02 over the last 22 preopen failures (2026-08-05 .. 11-02):
every one was a guard refusing, a deploy-drift block, a credential failure or
a collector error. None was a glitch a blind retry would have cleared, which
is why a guard refusal is never labelled retryable here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

GUARD = "guard"
CREDENTIAL = "credential"
TRANSIENT = "transient"
UNKNOWN = "unknown"


@dataclass(frozen=True)
class FailureClass:
    kind: str
    auto_fix_line: str


# Order matters: a guard refusal that mentions a timeout is still a guard.
_GUARD_RE = re.compile(
    r"Stale\w*Error|must not trade|MarketHours(Blocked|Unverified)"
    r"|DeployDrift|CorrectnessVerdict|verdict[\"': ]+(FAIL|STALE)"
    r"|CoverageGap|coverage gate|refuse[sd]? to trade",
    re.IGNORECASE,
)
_CREDENTIAL_RE = re.compile(
    r"\b403\b|AccessDenied|NoCredentials|not granted|Unauthorized|ExpiredToken"
    r"|InvalidClientTokenId|Bad credentials|authentication failed",
    re.IGNORECASE,
)
_TRANSIENT_RE = re.compile(
    r"Throttl|TooManyRequests|Rate exceeded|ServiceUnavailable|InternalServerError"
    r"|\b50[234]\b|Undeliverable|DeliveryTimedOut|ConnectionLost|ConnectionReset"
    r"|Connection reset|EndpointConnectionError|ReadTimeout|Unresponsive"
    r"|InvalidInstanceId|not in a valid state|PollTimeout|States\.Timeout",
    re.IGNORECASE,
)

_LINES = {
    GUARD: (
        "Auto-fix: none, by design. A safety guard refused, so a retry gives the "
        "same answer. It needs an upstream fix, then a rerun through the recovery "
        "dispatcher outside market hours."
    ),
    CREDENTIAL: (
        "Auto-fix: none. A credential or permission was refused, so a retry fails "
        "the same way until the grant or token is fixed."
    ),
    TRANSIENT: (
        "Auto-fix: looks like a glitch. A failed preopen that never reached the "
        "order step is retried once automatically before 09:15 ET (a separate "
        "'Preopen auto-retry' alert names the run); anything else is rerun by an "
        "agent through the recovery dispatcher outside market hours."
    ),
    UNKNOWN: (
        "Auto-fix: none. Unclassified failure; the nightly sweep triages it from "
        "the alert intake."
    ),
}


def classify_failure(cause: str | None) -> FailureClass:
    text = cause or ""
    for kind, pattern in (
        (GUARD, _GUARD_RE),
        (CREDENTIAL, _CREDENTIAL_RE),
        (TRANSIENT, _TRANSIENT_RE),
    ):
        if pattern.search(text):
            return FailureClass(kind, _LINES[kind])
    return FailureClass(UNKNOWN, _LINES[UNKNOWN])
