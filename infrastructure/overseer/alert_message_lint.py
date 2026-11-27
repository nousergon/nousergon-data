#!/usr/bin/env python3
"""PR-time lint: an alert MESSAGE that is defective in SHAPE fails its own CI.

``alpha-engine-config-I9460``. Companion to :mod:`alert_class_pr_guard`, which
asks *is this alert source registered*. This module asks the next question:
*can the person who receives this page act on it without asking anyone
anything, and will it stop firing when the condition stops standing?*

WHY THIS EXISTS
---------------
``alpha-engine-config-I9449`` swept the fleet for alert messages defective in
shape rather than in truth and fixed the loudest instances across five repos.
A sweep is a one-time cleanup: nothing stopped the class returning on the next
PR, and every instance read as *helpful* in review, which is why each was
written that way in the first place.

Measured scale, from the 633 krepis dedup markers in
``s3://alpha-engine-research/_alerts/_dedup/`` (``publish_count`` increments
only on a successful publish, so these are real deliveries): **134 pages over
11 days**, of which the sweep's fixes covered 104.

THE THREE PATTERNS
------------------
``ALERT001`` — **the unanswerable ask.** The message requests a human
decision, approval or acknowledgement for which there is no channel, no state
that changes when it is given, and no consequence of never giving it. It fires
on a schedule and asks the same question every session. Real instance, removed
in ``crucible-executor-PR518``::

    "... (run_date=...) Review and approve acceleration if the reallocation
     is intended."

``ALERT002`` — **go look it up.** The message instructs the reader to inspect
a log, artifact, dashboard or table the emitting code has already read or
could cheaply read. It converts a detector into a pager. Real instances, fixed
in ``crucible-executor-PR518`` and ``crucible-dashboard-PR810``::

    "... review the optimizer shadow logs for the driver."
    "memory budget: BREACH (detail in journal)"

Once the executor tripwire was rewritten to attribute its own driver, a replay
on 2026-08-31 produced ``driver=predictor_conviction_collapse ... This is an
UPSTREAM condition`` with no human involved at all.

``ALERT003`` — **run-keyed rather than episode-keyed dedup.** The ``dedup_key``
embeds a value that changes on every run, so one standing condition mints a
fresh key and re-pages every run. This is the highest-volume class in the
fleet by a wide margin: the router canary produced ~430 identical hourly pages
for a single 18-day condition, and ``freshness_digest_{date}_{fp}`` guaranteed
a fresh page every calendar day for a condition that had not changed.

ALERT003 IS GRADED IN TWO TIERS, AND THE REASON IS THE FALSE-POSITIVE RATE
--------------------------------------------------------------------------
Whether a run-keyed dedup is a defect turns on ONE question the lint cannot
answer from source alone: *can this condition stand across runs?* A once-per-
trading-day reconciliation failure keyed on ``{run_date}`` pages once per
occurrence and is correct. A standing staleness condition keyed on
``{run_date}`` pages every day forever and is the defect.

So the tell is split by what the interpolated value can POSSIBLY be:

* ``execution`` tier — sub-day clock readings and per-execution identities
  (``now``, ``timestamp``, ``execution_arn``, ``run_token``, ``instance_id``,
  ``elapsed``, ``uuid``). These can NEVER be episode identity, under any
  cadence, because a second run of the same standing condition always mints a
  different one. No judgement is required and the finding is unconditional.

* ``calendar`` tier — date-granular values (``run_date``, ``date_str``,
  ``trading_day``, ``today``). Correct exactly when the condition cannot
  outlive one day. That is a real judgement, so the finding asks for it: fix
  the key, or record the judgement in a waiver whose reason names why the
  condition cannot stand. Measured on the fleet as it stands today: 12
  ``execution``-tier sites and 45 ``calendar``-tier sites, ALL pre-existing
  and therefore invisible to this lint, which grades only the PR delta.

WHAT IS DELIBERATELY NOT FLAGGED
--------------------------------
* ``as_of`` in any spelling. ``crucible-research-PR780`` keys the fixed
  freshness grouping on ``(driver, upstream_prefix, as_of)``: the upstream
  artifact's own as-of stamp IS the episode identity there, and it stops
  moving exactly when the episode closes.
* Content hashes and fingerprints of the finding set — the correct shape.
  ``crucible-research/infrastructure/lambda_deploy_drift.py`` keys on
  ``head_sha[:12], severity``; ``check-definition-drift.py`` keys on a content
  hash of the findings, replacing a count-based defect;
  ``leaderboard_producers.py::vacuous_comparison`` keys on champion plus
  sorted challenger names. All three pass, and are pinned as positive controls
  in ``tests/test_alert_message_lint.py``.
* A legitimate **operator-gated security boundary** is NOT ``ALERT001``. That
  has a detector which stays red until the step runs, so never answering it
  has a consequence and the loop does close. The lint cannot see the detector,
  so this is the waiver's primary intended use and its reason must name it.

THE WAIVER, AND WHY IT MUST CARRY A REASON
-------------------------------------------
An inline comment on, or within three lines above, the flagged line::

    # alert-lint: allow ALERT003 -- receipt is an info-severity per-run
    #   acknowledgement; the SF execution IS the episode (see arming.py).

The rule id is required and a reason of at least
:data:`_MIN_WAIVER_REASON_CHARS` characters is required. **A waiver with no
reason is itself a finding** (``ALERT000``) — a bare suppression records that
someone silenced the rule and nothing about why, which is the same defect the
lint exists to prevent, one level up. ``--list-waivers`` enumerates every
waiver in a checkout, so the suppression set is a readable artifact rather
than something only grep knows about.

ONE SCANNER, NOT TWO
--------------------
File selection and publish-call extraction come from
:mod:`alert_class_registry_drift`; base-vs-head materialization comes from
:func:`alert_class_pr_guard.scan_at_ref`. Nothing about locating an alert call
site is reimplemented here (``policy-shared-code``) — this module contributes
only the message-shape rules.

EXIT CODES
----------
* ``0`` — no newly-introduced finding (or ``--warn-only``, which never fails).
* ``1`` — a finding this diff introduced, or UNMEASURED. A lint that cannot
  read its substrate fails as unmeasured, never as clean.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alert_class_pr_guard as guard  # noqa: E402
import alert_class_registry_drift as scan  # noqa: E402

GuardError = guard.GuardError


# ── rule definitions ────────────────────────────────────────────────────────

#: Minimum characters of prose in a waiver reason. Short enough that a real
#: one-line justification clears it; long enough that "n/a", "ok" and "see
#: above" do not.
_MIN_WAIVER_REASON_CHARS = 15

#: A candidate message string must look like prose. A bare identifier, a path
#: fragment or an S3 key is not a message and matching one is noise.
_MIN_MESSAGE_CHARS = 15

#: ALERT001 — the unanswerable ask.
#:
#: Every tell is an IMPERATIVE or a request. Past participles are excluded
#: deliberately: "mTLS confirmed", "promotion approved" and "receipt
#: acknowledged" are statements of fact, which is what an alert is supposed to
#: contain. `\b(?!ed\b)` is not expressible directly, so each verb lists the
#: inflections that are asks and omits the ones that are facts.
_ALERT001_TELLS: list[tuple[str, str]] = [
    (r"\bapprove\b", "asks for an approval"),
    (r"\bapprovals?\b", "asks for an approval"),
    (r"\backnowledge\b", "asks for an acknowledgement"),
    (r"\backnowledge?ments?\b", "asks for an acknowledgement"),
    (r"\bconfirm\b", "asks the reader to confirm"),
    (r"\bplease\s+(confirm|verify|review|check|advise|acknowledge)\b",
     "asks the reader to perform an action"),
    (r"\bif\s+(this\s+is\s+|the\s+\w+\s+is\s+|it\s+is\s+)?intended\b",
     "conditions on an intent only a human holds, with no channel to supply it"),
    (r"\bif\s+(this\s+is\s+)?expected\b",
     "conditions on an expectation only a human holds, with no channel to supply it"),
    (r"\bunless\s+(this\s+is\s+)?(expected|intended|deliberate)\b",
     "conditions on an expectation only a human holds, with no channel to supply it"),
    (r"\bsign\s*-?\s*off\b", "asks for a sign-off"),
    (r"\bdecide\s+whether\b", "hands a decision to the reader with no channel"),
]

#: ALERT002 — go look it up.
#:
#: Each tell pairs an INSPECTION VERB with a NAMED PLACE to inspect. The verb
#: alone is far too loose: "check" appears in half the fleet's prose. The
#: place alone is looser still. The conjunction is the signal, and it is what
#: distinguishes "review the optimizer shadow logs for the driver" from
#: "reviewed 14 units".
_INSPECTABLE = (
    r"(?:logs?\b|log\s+group|journal\b|journalctl|artifacts?\b|dashboards?\b|"
    r"tables?\b|consoles?\b|reports?\b|outputs?\b|cloudwatch\b|s3://|bucket\b|"
    r"manifest\b|traceback\b|stack\s*trace|transcript\b|payload\b)"
)
_ALERT002_TELLS: list[tuple[str, str]] = [
    (rf"\b(?:review|check|inspect|consult|examine|look\s+at|pull\s+up|"
     rf"go\s+(?:and\s+)?(?:read|check|look))\s+(?:the\s+|your\s+|its\s+|our\s+)?"
     # Up to three qualifier words between the verb and the thing to inspect.
     # "review the optimizer shadow logs for the driver" needs two; a
     # single-word window silently missed it, which is the whole reason the
     # pre-fix ref is replayed in the test suite rather than trusted.
     rf"(?:[\w\-./]+\s+){{0,3}}{_INSPECTABLE}",
     "tells the reader to inspect something the emitter could read itself"),
    (r"\b(?:see|refer\s+to|consult)\b[^.]{0,60}?\bfor\s+(?:details?|the\s+driver|"
     r"the\s+cause|the\s+reason|more|context|diagnosis)\b",
     "defers the diagnosis to another surface"),
    (r"\bdetails?\s+in\s+(?:the\s+)?" + _INSPECTABLE,
     "defers the diagnosis to another surface"),
    (r"\b(?:investigate|triage|diagnose)\b(?!\s+(?:d|s\b))",
     "asks the reader to perform the diagnosis the emitter is standing on the data for"),
    (r"\bssh\s+(?:in|to|into)\b", "asks the reader to go to the box"),
    (r"\brun\s+`?(?:journalctl|kubectl|aws\s+logs|tail\b)",
     "asks the reader to run the command the emitter could have run"),
]

#: ALERT003 tier 1 — values that can NEVER be episode identity.
_RUNKEY_EXECUTION = re.compile(
    r"(?:^|_|\.|\b)(?:now|utcnow|timestamp|timestamps|ts|epoch|monotonic|"
    r"execution_arn|executionarn|execution_id|run_token|run_id|runid|"
    r"invocation_id|request_id|instance_id|task_arn|pid|uuid|uuid4|token|"
    r"elapsed|elapsed_s|elapsed_sec|age_sec|age_seconds|duration_s|started_at|"
    r"finished_at|completed_at)(?:$|_|\b)",
    re.IGNORECASE,
)

#: ALERT003 tier 2 — date-granular values. Correct only when the condition
#: cannot outlive one day; that judgement is the author's to record.
_RUNKEY_CALENDAR = re.compile(
    r"(?:^|_|\.|\b)(?:run_date|rundate|date_str|datestr|trading_day|tradingday|"
    r"today|day|the_date|target_date|cycle_date|calendar_day|business_day|"
    r"session_date)(?:$|_|\b)",
    re.IGNORECASE,
)

#: Never a run key, whatever the tier regexes would otherwise say. ``as_of``
#: is the upstream artifact's own stamp — it stops moving when the episode
#: closes, which is precisely what episode identity means
#: (``crucible-research-PR780``).
_RUNKEY_EXEMPT = re.compile(r"as_?of", re.IGNORECASE)

#: A clock reading truncated to a calendar day: `now.date()`, `date.today()`,
#: `strftime("%Y-%m-%d")` with no time part.
_DATE_TRUNCATION = re.compile(r"\.date\s*\(\s*\)|\btoday\s*\(\s*\)")

#: Sub-day strftime directives inside a key literal: ``{now:%Y-%m-%dT%H:%M}``
#: mints a new key every minute however the variable is named.
_SUBDAY_STRFTIME = re.compile(r"%[HIMSfjs]")

#: A tell preceded by one of these is a STATEMENT, not an instruction. The
#: live false positive that put this here: ``crucible-executor``'s
#: ``"turnover tripwire: could not read shadow log for %s: %s"`` — a fact about
#: what the emitter itself failed to do, matched by the same "read ... log"
#: tell that catches "review the optimizer shadow logs for the driver". A lint
#: that flags an honest failure report teaches authors to stop writing them.
_NOT_AN_INSTRUCTION = re.compile(
    # The leading \b is load-bearing: without it `each\s+` matched inside
    # "BREACH (" and suppressed `(detail in journal)`, the single
    # highest-volume ALERT002 instance in the fleet. A cue must be a word.
    r"\b(?:could\s*n[o']t|cannot|can'?t|couldn'?t|failed\s+to|unable\s+to|"
    r"did\s*n[o']t|didn'?t|will\s+not|won'?t|no\s+one\s+to|nothing\s+to|"
    r"already|automatically|we\s+|it\s+|which\s+|that\s+|error\s+|"
    r"exception\s+|skipp?(?:ed|ing)\s+|while\s+|when\s+|after\s+|before\s+|"
    r"this\s+|these\s+|each\s+|every\s+|"
    r"instead\s+of\s+)"
    r"[^.;]{0,12}$"
)


#: A tell joined to its neighbours by an arrow or a slash is a token in a
#: SEQUENCE, not a verb aimed at anyone. Live false positive:
#: `saturday-sf-watch-dispatcher`'s receipt says the resilience agent runs
#: "(diagnose->fix->merge->rerun)" — a description of what the AUTOMATION does,
#: matched by the same `diagnose` tell that catches "investigate before the
#: next session".
_SEQUENCE_JOINER = re.compile(r"^\s*(?:\u2192|->|/|\u2794|\u27a1)")
_SEQUENCE_JOINER_BEFORE = re.compile(r"(?:\u2192|->|/|\u2794|\u27a1)\s*$")


def _is_sequence_token(text_lower: str, start: int, end: int) -> bool:
    return bool(
        _SEQUENCE_JOINER.match(text_lower[end:end + 3])
        or _SEQUENCE_JOINER_BEFORE.search(text_lower[max(0, start - 3):start])
    )


#: A message carrying the Decision Queue's own field shape IS the remedy this
#: lint recommends, not the defect it names. `engagement-protocol-policy.md`
#: 6.4b requires an escalation to carry `**Ask:**` / `**Recommendation:**` /
#: `**SOTA:**` / `**Delta:**`, and a queue item has exactly what ALERT001 says
#: is missing: a channel, state, and a consequence of never being answered.
#: Live false positive: `freshness-monitor/index.py` composes a queue item
#: reading "**Options:** ... B) Acknowledge as expected ...", flagged for
#: `acknowledge` — the one place in the fleet where asking for an
#: acknowledgement is correct.
_DECISION_QUEUE_SHAPE = re.compile(
    r"\*\*(?:ask|options?|recommendation|sota|delta)\s*:?\*\*", re.IGNORECASE
)


def _is_instruction(text_lower: str, start: int) -> bool:
    """Is the tell at ``start`` an imperative aimed at the reader?

    Judged from the ~40 characters preceding it, which is where the negation
    or the subject sits in every instance in the corpus. Deliberately cheap:
    a parser would be more precise and would also be a second grammar to
    maintain for a rule whose whole value is that authors trust its output.
    """
    return not _NOT_AN_INSTRUCTION.search(text_lower[max(0, start - 40):start])


_RULE_TITLES = {
    "ALERT000": "waiver with no reason",
    "ALERT001": "unanswerable ask",
    "ALERT002": "go look it up",
    "ALERT003": "run-keyed dedup",
}

_REMEDY = {
    "ALERT000": (
        "A suppression records that someone silenced this rule and nothing about why.\n"
        "  Write the reason inline:  # alert-lint: allow <RULE> -- <why this call site "
        "is correct>"
    ),
    "ALERT001": (
        "Delete the ask. If a human decision genuinely IS required, it belongs on the\n"
        "  Decision Queue with state, not in a message that re-fires tomorrow having\n"
        "  learned nothing from the last time it was read.\n"
        "  If this is a genuine operator-gated SECURITY BOUNDARY, it has a detector that\n"
        "  stays red until the step runs — waive it and name that detector in the reason."
    ),
    "ALERT002": (
        "Move the diagnosis into the detector. The test is whether the emitting code\n"
        "  has, or can cheaply obtain, the data the message tells the human to fetch.\n"
        "  Name the driver from a CLOSED set and keep an explicit `unattributed` branch —\n"
        "  a new failure mode must be reported as new, never rounded to the nearest known\n"
        "  one. Write the attribution on EVERY run, not only on the alerting path."
    ),
    "ALERT003": (
        "Key on the EPISODE, not the run. The key must be identical on every run for\n"
        "  which the condition is the same standing condition, and different the moment a\n"
        "  genuinely new one opens. A fingerprint over the finding set is the usual shape\n"
        "  (see check-definition-drift.py); an upstream artifact's own as-of stamp is\n"
        "  another (crucible-research-PR780).\n"
        "  If this condition genuinely cannot stand across runs, waive it and say why."
    ),
}


# ── findings ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Finding:
    rule: str
    relpath: str
    line: int
    snippet: str
    why: str

    @property
    def fingerprint(self) -> str:
        """Identity used for the base-vs-head delta.

        Deliberately EXCLUDES the line number. A finding is the same finding
        after an unrelated edit shifts it down the file; keying on the line
        would report every pre-existing instance in a touched file as newly
        introduced, and a lint that cries about code the PR did not write is
        the one that gets switched off.
        """
        digest = hashlib.sha256(
            f"{self.rule}\0{self.relpath}\0{_normalize(self.snippet)}".encode()
        ).hexdigest()
        return digest[:16]


@dataclass(frozen=True)
class Waiver:
    relpath: str
    line: int
    rule: str
    reason: str


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


# ── waiver parsing ──────────────────────────────────────────────────────────

#: `# alert-lint: allow ALERT003 -- reason` / `-- ` / `— ` / `: ` all accepted;
#: the separator is not the point, the reason is.
_WAIVER_RE = re.compile(
    r"alert-lint\s*:\s*allow\s+(?P<rule>ALERT\d{3}|\*)\s*(?:[-–—:]+\s*(?P<reason>.*))?$",
    re.IGNORECASE,
)

#: How many lines ABOVE a finding a waiver may sit. Three, because a long
#: message literal is routinely wrapped across two or three source lines and
#: the natural place for the comment is above the whole construct.
_WAIVER_LOOKBACK = 3


def parse_waivers(text: str, relpath: str) -> tuple[list[Waiver], list[Finding]]:
    """Every waiver in one file, plus an ``ALERT000`` for each with no reason."""
    waivers: list[Waiver] = []
    bare: list[Finding] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        match = _WAIVER_RE.search(raw)
        if not match:
            continue
        reason = (match.group("reason") or "").strip().strip("\"'`")
        rule = match.group("rule").upper()
        if len(reason) < _MIN_WAIVER_REASON_CHARS:
            bare.append(
                Finding(
                    rule="ALERT000",
                    relpath=relpath,
                    line=lineno,
                    snippet=raw.strip(),
                    why=(
                        f"waiver for {rule} carries "
                        + (f"a {len(reason)}-character reason" if reason else "no reason")
                        + f"; at least {_MIN_WAIVER_REASON_CHARS} characters are required"
                    ),
                )
            )
            continue
        waivers.append(Waiver(relpath=relpath, line=lineno, rule=rule, reason=reason))
    return waivers, bare


def _waived(finding: Finding, waivers: list[Waiver]) -> bool:
    return any(
        w.relpath == finding.relpath
        and w.rule in (finding.rule, "*")
        and 0 <= finding.line - w.line <= _WAIVER_LOOKBACK
        for w in waivers
    )


# ── message-string extraction ───────────────────────────────────────────────


def _python_message_strings(text: str) -> list[tuple[int, str]]:
    """``(line, value)`` for every runtime string constant in a Python file.

    Docstrings are excluded — a module that documents the anti-patterns (this
    one, and the scanner it imports) would otherwise flag itself, which is
    exactly the trap ``SELF_EXCLUDED_RELPATHS`` was added to the sibling
    scanner for. f-strings are walked so their literal fragments are read and
    their interpolations are not.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        # Not measurable by AST. Fall back to a line scan of quoted prose so a
        # file the parser cannot read is never silently treated as clean.
        return _quoted_prose_fallback(text)

    docstrings: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None) or []
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))

    # An f-string built from adjacent literals is ONE message and must be read
    # as one. `crucible-executor`'s pre-fix tripwire wrote:
    #
    #     f"abnormally even though each day is under the cap; review "
    #     f"the optimizer shadow logs for the driver."
    #
    # Two `ast.Constant` values inside a single `ast.JoinedStr`, with the tell
    # straddling the boundary. Reading the parts separately made the single
    # highest-value ALERT002 instance in the corpus invisible — caught here by
    # replaying the pre-fix ref, which is the only reason it is not still
    # invisible.
    joined_parts: set[int] = set()
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        pieces: list[str] = []
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                joined_parts.add(id(part))
                pieces.append(part.value)
            else:
                # An interpolation is a value, not prose. A placeholder keeps
                # the surrounding words from fusing into a phrase nobody wrote.
                pieces.append(" {} ")
        out.append((getattr(node, "lineno", 0), "".join(pieces)))

    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docstrings or id(node) in joined_parts:
                continue
            out.append((getattr(node, "lineno", 0), node.value))
    return out


_QUOTED = re.compile(r"""(?P<q>['"])(?P<v>(?:\\.|(?!(?P=q)).){4,})(?P=q)""")


def _quoted_prose_fallback(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.lstrip()
        if stripped.startswith("#"):
            continue
        for m in _QUOTED.finditer(raw):
            out.append((lineno, m.group("v")))
    return out


def _shell_message_strings(text: str) -> list[tuple[int, str]]:
    """``(line, value)`` for quoted strings on non-comment shell lines.

    ``crucible-dashboard``'s ``box_health.sh`` composed
    ``"memory budget: BREACH (detail in journal)"`` in a shell function, never
    inside a publish call, so a lint scoped to publish-call bodies would have
    missed the single highest-volume ``ALERT002`` instance in the fleet
    (32 pages in 11 days).
    """
    out: list[tuple[int, str]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        code = raw.split("#", 1)[0] if raw.lstrip().startswith("#") else raw
        if not code.strip():
            continue
        for m in _QUOTED.finditer(code):
            out.append((lineno, m.group("v")))
    return out


# ── dedup-key extraction ────────────────────────────────────────────────────


def _dedup_key_expressions(text: str) -> list[tuple[int, str]]:
    """``(line, source)`` for every ``dedup_key`` binding in a Python file.

    Covers both shapes the fleet uses: the keyword argument at the publish
    call, and the local assigned a line or two above it. Both were live in the
    corpus — ``freshness_monitor`` assigned
    ``dedup_key = _digest_dedup_key(decisions, now, unproduced)`` and passed
    the local, so a rule that read only keyword arguments would have missed
    the instance ``nousergon-data-PR1603`` was opened to fix.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return _dedup_key_expressions_textual(text)

    out: list[tuple[int, str]] = []

    def _record(value: ast.AST, lineno: int) -> None:
        try:
            src = ast.unparse(value)
        except Exception:  # noqa: BLE001 — unparse is best-effort; the segment below is the fallback
            src = ""
        out.append((lineno, src))

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "dedup_key":
                    _record(kw.value, getattr(kw.value, "lineno", node.lineno))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                name = _target_name(target)
                if name and name.endswith("dedup_key"):
                    _record(node.value, node.value.lineno)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            name = _target_name(node.target)
            if name and name.endswith("dedup_key"):
                _record(node.value, node.value.lineno)
    return out


def _target_name(target: ast.AST) -> str | None:
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


_DEDUP_TEXTUAL = re.compile(r"""dedup[_-]key\s*[=:]\s*(?P<expr>[^\n]{0,300})""")


def _dedup_key_expressions_textual(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        for m in _DEDUP_TEXTUAL.finditer(raw):
            out.append((lineno, m.group("expr")))
    return out


_SHELL_DEDUP = re.compile(r"--dedup-key[\s'\",=]+(?P<expr>[^\s'\"]{1,200})")


def _shell_dedup_key_expressions(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        if raw.lstrip().startswith("#"):
            continue
        for m in _SHELL_DEDUP.finditer(raw):
            out.append((lineno, m.group("expr")))
    return out


def classify_dedup_expression(expr: str) -> tuple[str, str] | None:
    """``(tier, matched)`` when ``expr`` is run-keyed, else ``None``."""
    scrubbed = _RUNKEY_EXEMPT.sub("", expr)
    if _SUBDAY_STRFTIME.search(scrubbed):
        return ("execution", _SUBDAY_STRFTIME.search(scrubbed).group(0))
    hit = _RUNKEY_EXECUTION.search(scrubbed)
    if hit:
        # `now.date().isoformat()` reads a clock and then TRUNCATES it to a
        # day, so it re-pages daily, not per-run. Still run-keyed, but the tier
        # drives the remediation priority in the register, and calling a
        # date-granular key "per-execution" overstates it.
        tier = "calendar" if _DATE_TRUNCATION.search(scrubbed) else "execution"
        return (tier, hit.group(0).strip("_."))
    hit = _RUNKEY_CALENDAR.search(scrubbed)
    if hit:
        return ("calendar", hit.group(0).strip("_."))
    return None


# ── the file-level lint ─────────────────────────────────────────────────────


def lint_text(text: str, relpath: str) -> tuple[list[Finding], list[Waiver]]:
    """Every finding and waiver in one file's text. Waivers are NOT applied here."""
    is_python = relpath.endswith(".py")
    waivers, findings = parse_waivers(text, relpath)

    strings = _python_message_strings(text) if is_python else _shell_message_strings(text)
    for lineno, value in strings:
        if len(value) < _MIN_MESSAGE_CHARS or " " not in value:
            continue
        if _DECISION_QUEUE_SHAPE.search(value):
            continue
        low = value.lower()
        for pattern, why in _ALERT001_TELLS:
            m = next(
                (c for c in re.finditer(pattern, low)
                 if _is_instruction(low, c.start())
                 and not _is_sequence_token(low, c.start(), c.end())),
                None,
            )
            if m:
                findings.append(Finding("ALERT001", relpath, lineno, value.strip()[:300],
                                        f"{why} (matched {m.group(0)!r})"))
                break
        for pattern, why in _ALERT002_TELLS:
            m = next(
                (c for c in re.finditer(pattern, low)
                 if _is_instruction(low, c.start())
                 and not _is_sequence_token(low, c.start(), c.end())),
                None,
            )
            if m:
                findings.append(Finding("ALERT002", relpath, lineno, value.strip()[:300],
                                        f"{why} (matched {m.group(0)!r})"))
                break

    exprs = (_dedup_key_expressions(text) if is_python else []) + \
        _shell_dedup_key_expressions(text)
    for lineno, expr in exprs:
        verdict = classify_dedup_expression(expr)
        if verdict is None:
            continue
        tier, matched = verdict
        why = (
            f"dedup_key interpolates {matched!r}, a per-execution value that can never "
            "be episode identity — one standing condition mints a new key every run"
            if tier == "execution"
            else
            f"dedup_key interpolates {matched!r}, so a condition that STANDS for more "
            "than a day re-pages every day. Correct only if this condition cannot "
            "outlive one day — say so in a waiver if it cannot"
        )
        findings.append(Finding("ALERT003", relpath, lineno, expr.strip()[:300], why))

    return findings, waivers


# ── repo scan ───────────────────────────────────────────────────────────────

#: Files graded even though they carry no publish call of their own. A message
#: is frequently composed in one module and published by another; without
#: these the ``ALERT002`` instance ``crucible-dashboard-PR810`` fixed would be
#: out of scope. The filter stays NAME-based and narrow rather than "every
#: file in the repo": the whole-repo variant flags library prose about alerts.
_ALERTISH_NAME = re.compile(
    r"(alert|page[rs]?|notif|tripwire|watchdog|canary|health|digest|monitor|"
    r"freshness|drift|escalat)", re.IGNORECASE,
)


#: Path segments whose files render to a SCREEN a person chose to open, not to
#: a page that arrives unasked. "check here during triage" is correct guidance
#: on a dashboard view and is the anti-pattern in an alert; the difference is
#: entirely the delivery surface, so it is drawn on the path.
_UI_PATH_SEGMENTS: frozenset[str] = frozenset(
    {"views", "pages", "templates", "docs", "notebooks", "examples", "static"}
)


def _in_scope(relpath: str, text: str) -> bool:
    """A file is graded when it publishes an alert, or is named like an emitter.

    Both halves are needed and neither is sufficient. Publish-call presence
    alone misses the composer module; the name heuristic alone would pull in
    unrelated ``*_monitor.py`` helpers that never reach an operator.
    """
    if relpath in scan.SELF_EXCLUDED_RELPATHS:
        return False
    if relpath == "infrastructure/overseer/alert_message_lint.py":
        return False
    parts = Path(relpath).parts[:-1]
    if any(seg in _UI_PATH_SEGMENTS for seg in parts):
        return False
    # The shared scanner includes operator-reaching ``notify_via_*`` wrappers;
    # keep this scope check on that single publish-shape definition.
    publishes = bool(
        scan._PUBLISH_CALL_PATTERN.search(text)
        or scan._CLI_CALL_PATTERN.search(text)
    )
    if publishes:
        return True
    # A SHELL script that publishes nothing is talking to the terminal of the
    # person who ran it, and that person can answer, or Ctrl-C. Live false
    # positive: `pipeline-watchdog/deploy.sh` echoes "Confirm before proceeding
    # or omit --smoke" -- an interactive prompt, admitted only because
    # `watchdog` appears in its PATH. The name heuristic exists for the Python
    # module that COMPOSES a message another module publishes; a shell script
    # that composes without publishing is an operator script, not an emitter.
    # Measured: of the six shell files this lint flags, that one is the only
    # one with zero publish invocations.
    if relpath.endswith(".sh"):
        return False
    # Matched against the whole relative path, not just the basename. Every
    # Lambda in `nousergon-data` is `index.py`; the emitter identity lives in
    # its DIRECTORY (`lambdas/freshness-monitor/`, `lambdas/pipeline-watchdog/`).
    # A basename-only test put `freshness-monitor/index.py` out of scope, and
    # with it the run-keyed dedup that `nousergon-data-PR1603` exists to fix.
    return bool(_ALERTISH_NAME.search(relpath))


def scan_repo(repo_root: Path) -> tuple[list[Finding], list[Waiver]]:
    findings: list[Finding] = []
    waivers: list[Waiver] = []
    for path in sorted(repo_root.rglob("*.py")) + sorted(repo_root.rglob("*.sh")):
        rel = path.relative_to(repo_root)
        if any(p in scan.EXCLUDED_DIR_NAMES or p == ".claude" for p in rel.parts):
            continue
        if path.name.endswith("_pb2.py") or scan._is_test_file(path, repo_root):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            raise GuardError(f"UNREADABLE: {path} ({exc})") from exc
        relpath = rel.as_posix()
        if not _in_scope(relpath, text):
            continue
        f, w = lint_text(text, relpath)
        findings.extend(f)
        waivers.extend(w)
    return findings, waivers


def scan_repo_applied(repo_root: Path) -> dict[str, Finding]:
    """``{fingerprint: Finding}`` after waivers are applied."""
    findings, waivers = scan_repo(repo_root)
    return {f.fingerprint: f for f in findings if not _waived(f, waivers)}


# ── reporting ───────────────────────────────────────────────────────────────


def _gha_annotation(finding: Finding, *, warn_only: bool) -> str:
    level = "warning" if warn_only else "error"
    msg = f"[{finding.rule} {_RULE_TITLES[finding.rule]}] {finding.why}"
    return f"::{level} file={finding.relpath},line={finding.line},title=alert-message-lint::{msg}"


def report(findings: list[Finding], *, warn_only: bool, out: TextIO) -> None:
    by_rule: dict[str, list[Finding]] = {}
    for f in findings:
        by_rule.setdefault(f.rule, []).append(f)

    verb = "WARN" if warn_only else "FAIL"
    print(f"\n{verb}: this diff introduced {len(findings)} alert-message finding(s).", file=out)
    for rule in sorted(by_rule):
        print(f"\n=== {rule} — {_RULE_TITLES[rule]} "
              f"({len(by_rule[rule])}) ===", file=out)
        for f in sorted(by_rule[rule], key=lambda x: (x.relpath, x.line)):
            print(f"\n  {f.relpath}:{f.line}", file=out)
            print(f"    {f.snippet}", file=out)
            print(f"    WHY: {f.why}", file=out)
        print(f"\n  WHAT TO DO INSTEAD:\n  {_REMEDY[rule]}", file=out)
    print(
        "\n  If this call site is a genuine exception, waive it INLINE with a reason:\n"
        "    # alert-lint: allow <RULE> -- <why this one is correct>\n"
        "  A waiver with no reason is itself a finding. `--list-waivers` enumerates every\n"
        "  waiver in the repo, so the suppression set stays readable.\n",
        file=out,
    )
    if warn_only:
        print(
            "  WARN-ONLY: this step does not fail the build. It was landed warn-first so a\n"
            "  change to the shared lint could never redden eleven repos' CI at once "
            "(alpha-engine-config-I9471 carries the enforcement flip and its predicate).\n",
            file=out,
        )


# ── main ────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--repo", required=True, help="canonical name of the repo being graded")
    ap.add_argument("--repo-root", required=True, help="checkout of --repo, with full history")
    ap.add_argument("--base", default=None,
                    help="git ref for the PR's base sha. Omitted: grade the whole checkout.")
    ap.add_argument("--head", default="HEAD", help="git ref for the PR's head (default: HEAD)")
    ap.add_argument("--warn-only", action="store_true",
                    help="report findings as GitHub warning annotations and exit 0")
    ap.add_argument("--list-waivers", action="store_true",
                    help="enumerate every alert-lint waiver in the checkout and exit")
    ap.add_argument("--no-annotations", action="store_true",
                    help="suppress ::warning/::error workflow commands (local runs)")
    args = ap.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()

    if args.list_waivers:
        try:
            _, waivers = scan_repo(repo_root)
        except GuardError as exc:
            print(f"UNMEASURED: {exc}", file=sys.stderr)
            return 1
        print(f"{len(waivers)} alert-lint waiver(s) in {args.repo}:")
        for w in sorted(waivers, key=lambda x: (x.relpath, x.line)):
            print(f"  {w.relpath}:{w.line}  {w.rule}  — {w.reason}")
        return 0

    if not (repo_root / ".git").exists():
        print(
            f"UNMEASURED: {repo_root} is not a git checkout — this lint would otherwise "
            "report green having verified nothing",
            file=sys.stderr,
        )
        return 1

    print(f"linting: {args.repo} at {repo_root}")
    print("rules: ALERT001 unanswerable ask · ALERT002 go look it up · "
          "ALERT003 run-keyed dedup")

    try:
        head = guard.scan_at_ref(repo_root, args.head, scan_repo_applied)
        base = (
            guard.scan_at_ref(repo_root, args.base, scan_repo_applied)
            if args.base else {}
        )
    except GuardError as exc:
        print(f"UNMEASURED: {exc}", file=sys.stderr)
        print("A lint that cannot read its substrate fails as unmeasured, never as clean.",
              file=sys.stderr)
        return 1

    carried = [f for fp, f in head.items() if fp in base]
    newly = [f for fp, f in head.items() if fp not in base]

    if carried:
        print(
            f"\npre-existing findings NOT gated by this PR ({len(carried)}): "
            + ", ".join(sorted({f"{f.rule}@{f.relpath}" for f in carried}))
            + "\n  (alpha-engine-config-I9491's register owns those — this lint only "
              "reports what THIS diff introduced)"
        )

    if not newly:
        print("\nno newly-introduced alert-message finding in this diff.")
        return 0

    if not args.no_annotations:
        for f in sorted(newly, key=lambda x: (x.relpath, x.line)):
            print(_gha_annotation(f, warn_only=args.warn_only))

    report(newly, warn_only=args.warn_only, out=sys.stderr if not args.warn_only else sys.stdout)
    return 0 if args.warn_only else 1


if __name__ == "__main__":
    raise SystemExit(main())
