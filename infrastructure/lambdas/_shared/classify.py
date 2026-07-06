"""Deterministic (no-LLM) classifier for SNS-mirror changelog entries.

Maps an alert's subject + message to ``(event_type, severity, subsystem,
root_cause_category)`` drawn from the controlled vocab in ``vocab.py``. Pure
and rule-based — reused by both:

  - ``changelog-incident-mirror/index.py``  (go-forward, every SNS message)
  - ``reclassify_history.py``               (one-time backfill of the corpus)

so the two never drift apart.

**Why this exists.** The SNS topic ``alpha-engine-alerts`` carries a MIX:
real failures (``ALARM:`` / ``— FAILED`` / ``[ERROR]``), warnings
(``[WARN]``), recoveries (``OK:`` alarm-clears), and success notifications
(``— SUCCESS`` / ``PASSED`` / ``Skipped``). The original Lambda hard-coded
**every** message to ``incident`` / ``high`` / ``infrastructure_failure``, so
SUCCESS + OK entries flooded the incident corpus and made the retro-candidate
feed meaningless. This classifier sorts each message to its true type so the
console Retros page can mine real incidents deterministically — no LLM.

**Fail-loud default.** An unrecognized subject is classified as a real
``incident`` (``high``) so a new, unanticipated alert shape surfaces LOUDLY in
the incident corpus rather than being silently downgraded to noise. (Per the
"fail loud and fast" rule — a producer must not silently swallow.)
"""

from __future__ import annotations

# Real-failure markers (substring, case-insensitive). FAILED is checked before
# SUCCESS so a "PredictorTraining FAILED" never matches a success rule.
_FAIL_MARKERS = ("FAILED", "FAILURE", "CRASHED", "TIMED OUT", "TIMEOUT", "EXCEPTION")
# Success / benign-completion markers.
_SUCCESS_MARKERS = ("SUCCESS", "SUCCEEDED", "PASSED", "SKIPPED")

# Subsystem inference — first token match wins. Keys are uppercase substrings
# tested against the subject; values are vocab SUBSYSTEMS members.
_SUBSYSTEM_TOKENS = (
    ("BACKTESTER", "backtester"),
    ("PREDICTOR", "predictor"),
    ("RESEARCH", "research"),
    ("EXECUTOR", "executor"),
    ("DASHBOARD", "dashboard"),
    ("RAG", "retrieval"),
    ("RETRIEVAL", "retrieval"),
    ("PROMPT", "prompts"),
    ("EVAL", "eval"),
    ("DATAPHASE", "data_pipeline"),
    ("DATA STALENESS", "data_pipeline"),
    ("DATA-PIPELINE", "data_pipeline"),
)


def _scan_text(subject: str, message: str) -> str:
    """The text the rules inspect: the subject, or — when the subject is empty
    (some raw SNS publishes carry only a body) — the first line of the
    message. Mirrors the handler's own summary fallback."""
    text = (subject or "").strip()
    if not text and message:
        lines = message.strip().splitlines()
        text = lines[0] if lines else ""
    return text


def infer_subsystem(subject: str, message: str = "") -> str:
    """Best-effort subsystem from the subject/message; defaults to
    ``infrastructure``.

    Most SNS alerts are SF/Lambda/box-health failures (infrastructure); the
    token table promotes the ones that name a specific module so the retro
    page can group by subsystem.
    """
    su = _scan_text(subject, message).upper()
    for token, subsystem in _SUBSYSTEM_TOKENS:
        if token in su:
            return subsystem
    return "infrastructure"


def classify_sns(subject: str, message: str = "") -> tuple[str, str, str, str | None]:
    """Return ``(event_type, severity, subsystem, root_cause_category)``.

    Precedence (first match wins):
      1. CloudWatch alarm state transitions — subject prefix ``OK:`` /
         ``ALARM:`` / ``INSUFFICIENT_DATA``.
      2. Explicit ``nousergon_lib.alerts`` severity tags —
         ``[CRITICAL]`` / ``[ERROR]`` / ``[WARN]`` / ``[WARNING]`` / ``[INFO]``.
      3. Pipeline/job result suffixes — ``FAILED`` (real) vs ``SUCCESS`` /
         ``PASSED`` / ``SKIPPED`` (benign).
      4. Default — unrecognized alert → ``incident`` / ``high`` (fail loud).

    ``root_cause_category`` is ``infrastructure_failure`` for incidents (an
    auto-default the operator refines via ``changelog-log``) and ``None`` for
    non-incident (recovery / change) entries.
    """
    su = _scan_text(subject, message).upper()
    subsystem = infer_subsystem(subject, message)

    def incident(sev: str):
        return ("incident", sev, subsystem, "infrastructure_failure")

    def non_incident(event_type: str, sev: str):
        return (event_type, sev, subsystem, None)

    # 1. CloudWatch alarm state transitions (subject is canonical).
    if su.startswith("OK:"):
        return non_incident("recovery", "informational")
    if su.startswith("ALARM:"):
        return incident("high")
    if su.startswith("INSUFFICIENT_DATA"):
        return incident("low")

    # 2. Explicit severity tags from nousergon_lib.alerts.
    if "[CRITICAL]" in su:
        return incident("critical")
    if "[ERROR]" in su:
        return incident("high")
    if "[WARN]" in su or "[WARNING]" in su:
        return incident("medium")
    if "[INFO]" in su:
        return non_incident("change", "informational")

    # 3. Pipeline / job result notifications. FAILED before SUCCESS.
    if any(m in su for m in _FAIL_MARKERS):
        return incident("critical" if "CRITICAL" in su else "high")
    if any(m in su for m in _SUCCESS_MARKERS):
        return non_incident("change", "informational")

    # 4. Unrecognized → treat as a real incident (fail loud).
    return incident("high")


def derive_fields(subject: str, message: str = "") -> dict[str, str | None]:
    """Convenience wrapper returning the classified fields as a dict, for
    merging into a structured changelog entry."""
    event_type, severity, subsystem, rcc = classify_sns(subject, message)
    return {
        "event_type": event_type,
        "severity": severity,
        "subsystem": subsystem,
        "root_cause_category": rcc,
    }
