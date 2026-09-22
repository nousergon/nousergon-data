"""`python -m data_gate report --store <uri>` — the data collector's daily update.

The data collector's accountability surface. `data-gate.yml` READS the ladder
and publishes it (`gates/ladder.json`, `gates/board/latest.json`, the dated
`gates/<gate>/{date}/gate.json` history); this module reads what that reader
wrote and delivers it to Brian's phone, once a day, with a durable copy filed
under `runs/report.daily/` and a full-detail comment on the rolling tracker
issue. It is `alpha-engine-config-I10952`.

**This module never renders the ladder itself.** Every figure below is quoted
from an artifact the gate reader published. A reporting surface that
re-evaluated the board under its own environment would give the fleet two
disagreeing answers to "what does the board say" — the exact defect
`data_gate.read`'s own docstring records the console having caused.

**Why this is a second message and not two sections inside crucible's
`report.morning`.** Two systems need two ABSENCE detectors. One combined
message that still arrives while its data-collection half renders empty is the
unobserved-but-green state principle 7 forbids. Its own job, its own manifest
prefix, its own observability row.

## Principle 7 is the whole job

*No data is never rendered as green*, and the three ways that rule gets broken
are each closed by name here:

* An empty clause list renders ``no clauses``, never ``0/0`` — see
  :func:`_fraction`. ``0/0`` reads as "nothing outstanding".
* A rung whose dated history does not exist yet renders **ABSENT** with the
  reason, never as zero movement and never as green. `gates/data-phase1/` was
  measured EMPTY on 2026-09-17 while `current_phase` already read
  `data-phase1`, so this is the live case, not a defensive branch.
* **Absent** and **denied** are distinct third facts and get distinct
  sentences. S3 returns 403 for a missing key when the caller also lacks
  `s3:ListBucket` on the prefix, so a denied read rendered as "not there yet"
  hides an IAM gap behind a normal-looking report.

A stale ladder puts a bolded staleness line FIRST in BOTH documents. A reader
who has to scroll to learn that every number above is a day old has already
acted on it.

## Everything mechanical lives in the library

`nousergon_lib.gates.report` and `.tracker` (alpha-engine-config-I10951) own
the read/stale/moved/budget/deliver/tracker primitives, shared with crucible's
`report.morning`. This module is the data collector's ADAPTER over them: which
keys to read, what the message says, where the manifest goes. Nothing in here
is a second implementation of something in there — when a helper below looks
like it wants to be general, it belongs in the library instead.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from nousergon_lib.gates import LADDER_KEY, gate_prefix
from nousergon_lib.gates.report import (
    MessageTooLongError,
    MovedResult,
    Read,
    UndeliveredError,
    assert_within_budget,
    deliver as _lib_deliver,
    escape,
    history_row,
    moved_since,
    read_optional,
    read_required,
    render_history_index,
    resolve_trigger,
    staleness,
    wire_budget,
)
from nousergon_lib.gates.tracker import Tracker, TrackerConfig

from data_gate.read import BOARD_KEY

__all__ = [
    "BOARD_CONSOLE_PATH",
    "DELIVERY_CRON_UTC",
    "DELIVERY_TZ",
    "DataReportInputs",
    "MessageTooLongError",
    "REPORT_JOB",
    "ROLLING_ISSUE_TITLE",
    "STALE_AFTER",
    "UndeliveredError",
    "UnresolvedLog",
    "deliver",
    "manifest_key",
    "previous_trading_day",
    "read_inputs",
    "read_unresolved_logs",
    "render_full_update",
    "render_history_body",
    "render_message",
    "run_report",
]

# ── the job, and where it files ──────────────────────────────────────────────

#: The manifest prefix. Its OWN prefix, not a sub-key of the gate's: the
#: observability row that pages on this report's silence
#: (`nous-ergon-ops/governance/observability.d/nousergon-data-data-report.yaml`)
#: watches this and nothing else, so a gate reading that keeps landing while
#: the report stops cannot hold the absence detector green.
REPORT_JOB = "report.daily"
RUNS_PREFIX = f"runs/{REPORT_JOB}"

#: `krepis.alerts.publish`'s severity and source. `info`, because the report
#: arriving is the normal case; the ABSENCE of it is what pages, and that page
#: is the observability row's, not this process's.
DELIVERY_SEVERITY = "info"
DELIVERY_SOURCE = f"data-collection/{REPORT_JOB}"

#: The rolling issue every full update is a comment on, on the FLEET tracker —
#: `nousergon-data` is public and this update quotes the collector's internal
#: state. Minted through the same `ne-groomer` GitHub App crucible's
#: `report.morning` uses; an Actions token cannot reach a different private
#: repository at all.
ROLLING_ISSUE_TITLE = "[data collection] daily update"
TRACKER_REPO = "nousergon/alpha-engine-config"
TRACKER_TOKEN_VAR = "DATA_TRACKER_TOKEN"
TRACKER_APP_SSM_PREFIX_VAR = "DATA_TRACKER_APP_SSM_PREFIX"

#: The console's Decision list filtered to this board — a STABLE address, so
#: none of crucible's presigned-URL expiry caveat machinery is needed here.
BOARD_CONSOLE_PATH = "/decision?pipeline=data-collection-board"

# ── cadence ──────────────────────────────────────────────────────────────────

#: The DECLARED cron, which is an INPUT to delivery time and not a statement of
#: it. Every scheduled workflow on this account fires 3-5h after its cron,
#: MEASURED and chronic (`alpha-engine-config-I9966`), which is why crucible's
#: `report.morning` moved `0 13` -> `0 10`.
#:
#: THE ARITHMETIC. Intended delivery 06:30 PT = 13:30 UTC during PDT. Subtract
#: the 3h LOWER bound of the measured lag — the lower bound, so the delivery
#: window opens at the intended time and closes 2h later, rather than opening
#: 2h early — and the declared instant is 10:30 UTC.
#:
#: WHY 30 MINUTES AFTER crucible's `0 10`, and not the same instant: two
#: accountability messages arriving as one burst are read as one message, and a
#: missing data-collection message then hides inside the crucible one having
#: arrived. Staggering them makes this report's absence visible to the reader,
#: not only to the registry.
#:
#: The gate publishes at 23:30 UTC, ~11h before this, so any `0 10`-class cron
#: reads a reading that completed the previous evening — never a half-written
#: one and never one from two days back.
#:
#: `tests/test_data_report.py` parses `.github/workflows/data-report.yml` and
#: asserts its only cron equals this constant, so the literal and the code
#: cannot silently disagree.
DELIVERY_CRON_UTC = "30 10 * * *"

#: GitHub Actions crons are UTC and have no timezone, so the local instant
#: DRIFTS at the US daylight-time boundary. Declared rather than discovered:
#: `30 10 * * *` is 03:30 PDT today and becomes 02:30 PST on 2026-11-01, the
#: message prints the local time it was actually delivered at, and
#: `tests/test_data_report.py` asserts both resolutions.
DELIVERY_TZ = ZoneInfo("America/Los_Angeles")

#: How old a ladder may be before the staleness headline fires. 26h, which is
#: NOT a number invented here: it is the ceiling `data.ladder_fresh` already
#: pages on, so the report and the freshness detector cannot disagree about
#: whether the same ladder is stale. The gate's daily cadence is 24h, so a
#: single missed reading is not yet stale and two are.
STALE_AFTER = dt.timedelta(hours=26)

# ── the wire ─────────────────────────────────────────────────────────────────

#: The Telegram headline budget, before krepis's own `[INFO] <source>: `
#: prefix. :func:`wire_budget` subtracts that prefix from the SAME formatter
#: krepis prepends, so this is a budget for the body alone.
MESSAGE_MAX_CHARS = 1200

#: Rendered instead of a fraction when the denominator is zero. `0/0` reads as
#: "nothing outstanding" — it is the shape of a green number — when what it
#: actually means is that nothing was graded at all.
NO_CLAUSES = "no clauses"


def _fraction(met: Any, total: Any) -> str:
    """``met/total``, or :data:`NO_CLAUSES` when nothing was graded.

    Principle 7's smallest instance. A rung that grades zero clauses is
    UNMEASURED, not complete, and `0/0` is indistinguishable on a phone from a
    rung with nothing left to do.
    """
    try:
        total_n = int(total)
        met_n = int(met)
    except (TypeError, ValueError):
        return "unreadable"
    if total_n <= 0:
        return NO_CLAUSES
    return f"{met_n}/{total_n}"


# ── the trading-day axis ─────────────────────────────────────────────────────


def previous_trading_day(calendar_date: dt.date) -> dt.date:
    """The last weekday STRICTLY BEFORE ``calendar_date``.

    Saturday, Sunday and Monday all resolve to the preceding Friday — three
    calendar days collapsing onto one trading day. That collapse is exactly why
    :func:`manifest_key` carries the calendar date as well: without it, three
    genuinely different deliveries would overwrite each other at one key and
    the weekend's reports would vanish leaving the Monday one looking complete.
    """
    day = calendar_date - dt.timedelta(days=1)
    while day.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        day -= dt.timedelta(days=1)
    return day


def manifest_key(basename: str, trading_day: str, calendar_date: str) -> str:
    """``runs/report.daily/{trading_day}/{calendar_date}/{basename}``."""
    if not trading_day or not calendar_date:
        raise ValueError(
            "trading_day and calendar_date must both be non-empty — an empty segment "
            "collapses every delivery onto one key, which is the collision this two-level "
            "layout exists to prevent."
        )
    return f"{RUNS_PREFIX}/{trading_day}/{calendar_date}/{basename}"


# ── reading ──────────────────────────────────────────────────────────────────


class _JsonReads:
    """Adapts `data_gate`'s byte store to the ``.get_json(key)`` surface
    `nousergon_lib.gates.report.read_optional` reads through.

    One translation, and it is load-bearing. `data_gate.store.S3Store`
    normalizes S3's `NoSuchKey`/`404`/`NotFound` to `FileNotFoundError` before
    the caller sees it, because its own clause engine wants absence typed as an
    answer. The library's reader classifies absence from the botocore code, so
    this reconstructs the `ClientError` S3 actually returned. Without it an
    ABSENT key would reach the library as an unclassified exception and come
    back as a DENIED read — turning "no reading has been taken yet" into "we
    are not permitted to look", which is the conflation principle 7 forbids and
    the reason `denied` and `absent` are separate properties at all.

    Nothing else is translated: a genuine `AccessDenied` keeps its own
    `ClientError` and reaches the library as the denial it is.
    """

    def __init__(self, store: Any) -> None:
        self._store = store

    @property
    def uri(self) -> str:
        return str(getattr(self._store, "uri", "") or "")

    def get_json(self, key: str) -> Any:
        from botocore.exceptions import ClientError  # noqa: PLC0415 - lazy, like data_gate.store

        try:
            raw = self._store.get_bytes(key)
        except (FileNotFoundError, KeyError) as exc:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": f"no object at {key}"}},
                "GetObject",
            ) from exc
        return json.loads(raw)


def _current_gate(ladder: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """The gate named by the ladder's ``current_phase``, and that phase's row.

    Read from the ladder's own rows rather than reconstructed from this
    process's checkout of `phases.yaml`: the report must quote the phase
    topology the PUBLISHER saw, or a report run from a branch would print that
    branch's ladder as though it were `main`'s reading.
    """
    current = ladder.get("current_phase")
    for row in ladder.get("phases") or []:
        if isinstance(row, dict) and row.get("phase") == current:
            return (row.get("gate") or None), row
    return None, None


def _normalize_gate_rows(document: dict[str, Any] | None) -> dict[str, Any] | None:
    """A dated gate reading as ``{"rows": [{"id", "state"}]}``.

    `moved_since` compares two documents of rows carrying an id and a state.
    A dated `gate.json` carries `clauses`, each with `met` and `unmeasurable`
    rather than one state field, so the three-valued state is RECONSTRUCTED
    here — and `unmeasurable` is checked FIRST, because a clause that is both
    unmeasurable and not met is UNMEASURABLE, never UNMET: "we could not read
    this" must not render as "we read it and it said no".
    """
    if document is None:
        return None
    rows = []
    for clause in document.get("clauses") or []:
        if not isinstance(clause, dict):
            continue
        if clause.get("unmeasurable"):
            state = "UNMEASURABLE"
        elif clause.get("met"):
            state = "MET"
        else:
            state = "UNMET"
        rows.append({"id": str(clause.get("name", "?")), "state": state})
    return {"rows": rows}


@dataclass(frozen=True)
class DataReportInputs:
    """Everything both documents are rendered from. Read once, in one place.

    Every field that can be absent carries its own reason string beside it,
    because "absent", "unreadable" and "denied" want three different sentences
    and are indistinguishable from a bare `None`.
    """

    ladder: dict[str, Any]
    board: dict[str, Any]
    store_uri: str
    trading_day: dt.date
    calendar_date: dt.date
    #: The gate the ladder's `current_phase` names, or `None` when the ladder
    #: names a phase with no gate at all (which is itself reportable).
    current_gate: str | None
    current_phase_row: dict[str, Any] | None
    #: The two most recent dated readings of :attr:`current_gate`, newest
    #: first, as `(key, Read)`. EMPTY when that rung has no dated history —
    #: the live case for `data-phase1` on 2026-09-17.
    readings: tuple[tuple[str, Read], ...]
    #: `None` when movement could not be computed at all; :attr:`moved_absent`
    #: then carries the reason and the documents say ABSENT rather than
    #: "nothing moved".
    moved: MovedResult | None
    moved_absent: str | None
    #: Per-rung dated-history availability, `{gate: reason-or-None}`. A rung
    #: with no history is ABSENT, and that is a fact about the LADDER, not
    #: about this report.
    history_absent: dict[str, str]
    staleness_line: str | None
    console_url: str | None
    #: Non-ok run manifests of :attr:`trading_day` whose `log_location` does
    #: not resolve to a readable object (alpha-engine-config-I11353). EMPTY is
    #: a real, reportable answer — rendered as a stated "none", never omitted.
    unresolved_logs: tuple[UnresolvedLog, ...] = ()
    #: Why the check above could not be completed, per prefix. A blind check
    #: renders as BLIND, never as a clean row.
    unresolved_log_problems: tuple[str, ...] = ()


def read_inputs(
    store: Any,
    *,
    trading_day: dt.date,
    calendar_date: dt.date,
    now: dt.datetime | None = None,
    console_url: str | None = None,
) -> DataReportInputs:
    """Read every artifact the report quotes. Reads; never runs.

    The ladder and the board are REQUIRED: a report that could not read them
    has nothing to say, and rendering a message about an absent board would be
    a surface reporting its own outage as the system's state. They raise, this
    job's manifest reads `failed`, and the observability row pages on the
    silence that follows.

    Everything else is optional and three-valued.
    """
    moment = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    reads = _JsonReads(store)

    # Deliberately NOT wrapped in a try/except that would let the whole check
    # vanish: `read_unresolved_logs` already returns its own failures as named
    # problems, and an exception escaping it is a defect this report should
    # fail on rather than deliver around.
    unresolved_logs, unresolved_log_problems = read_unresolved_logs(
        store, trading_day=trading_day
    )

    ladder = read_required(reads, LADDER_KEY)
    board = read_required(reads, BOARD_KEY)

    gate, phase_row = _current_gate(ladder)

    history_absent: dict[str, str] = {}
    for row in ladder.get("phases") or []:
        if not isinstance(row, dict):
            continue
        row_gate = row.get("gate")
        if not row_gate:
            continue
        keys = _dated_keys(store, str(row_gate))
        if not keys:
            history_absent[str(row_gate)] = (
                f"no dated reading has ever been written under {gate_prefix(str(row_gate))}"
            )

    readings: list[tuple[str, Read]] = []
    if gate:
        for key in _dated_keys(store, gate)[:2]:
            readings.append((key, read_optional(reads, key)))

    moved: MovedResult | None = None
    moved_absent: str | None = None
    if not gate:
        moved_absent = (
            f"the ladder's current phase ({ladder.get('current_phase')!r}) names no gate, "
            "so there is no dated history to compare"
        )
    elif not readings:
        moved_absent = (
            f"ABSENT: {gate_prefix(gate)} holds no dated reading at all, so there is no "
            "'before' and no 'after' — the rung the ladder says we are on has never been "
            "filed. This is not zero movement."
        )
    else:
        current_key, current_read = readings[0]
        if current_read.document is None:
            moved_absent = (
                f"the newest dated reading ({current_key}) could not be read: "
                f"{_read_reason(current_read)}"
            )
        else:
            previous_key, previous_read = (
                readings[1] if len(readings) > 1 else (None, None)
            )
            previous_document = None if previous_read is None else previous_read.document
            if previous_read is None:
                previous_reason = (
                    f"{gate_prefix(gate)} holds exactly one dated reading ({current_key}), "
                    "so there is nothing to compare it against yet"
                )
            else:
                previous_reason = (
                    None if previous_document is not None else _read_reason(previous_read)
                )
            moved = moved_since(
                previous=_normalize_gate_rows(previous_document),
                current=_normalize_gate_rows(current_read.document) or {"rows": []},
                previous_reason=previous_reason,
            )

    return DataReportInputs(
        ladder=ladder,
        board=board,
        store_uri=reads.uri or str(getattr(store, "uri", "")),
        trading_day=trading_day,
        calendar_date=calendar_date,
        current_gate=gate,
        current_phase_row=phase_row,
        readings=tuple(readings),
        moved=moved,
        moved_absent=moved_absent,
        history_absent=history_absent,
        staleness_line=staleness(
            generated_at=ladder.get("generated_utc"),
            now=moment,
            stale_after=STALE_AFTER,
            label="gates/ladder.json",
        ),
        console_url=console_url,
        unresolved_logs=tuple(unresolved_logs),
        unresolved_log_problems=tuple(unresolved_log_problems),
    )


# ── detection gap: a failed run whose log cannot be read ────────────────────
#
# `alpha-engine-config-I11353`. On 2026-09-21 three shadow legs failed and
# every manifest they wrote recorded `log_location:
# "local:ip-172-31-33-124.ec2.internal:<pid>"` — a host terminated minutes
# later. Nothing anywhere said so. A failed run whose log cannot be read is not
# an absence, it is a hole in the recording surface, and principle 7 forbids
# rendering it as nothing: it gets a NAMED row on the daily report, every day,
# until it resolves.
#
# The check is deliberately over NON-OK manifests only. A successful run whose
# log went nowhere is a smaller defect and would drown this row in volume on
# the day the fix ships but the boxes have not yet been redeployed.

#: What the log_location of a shipped run looks like, relative to the store.
LOG_PREFIX = "logs/"


@dataclass(frozen=True)
class UnresolvedLog:
    """One non-ok run manifest whose ``log_location`` does not resolve."""

    unit_id: str
    manifest_key: str
    log_location: str
    reason: str


def _resolve_log(store: Any, log_location: str, store_uri: str) -> str | None:
    """``None`` when the log resolves; otherwise WHY it does not.

    Three distinct answers, because they have three different owners: a
    `local:`/`cloudwatch:` location is a box running code that predates the
    shipper (a deploy gap), a `s3://` location outside this store is a
    misconfiguration, and a key inside the store that is not there is a
    shipper failure.
    """
    if not log_location:
        return "the manifest declares no log_location at all"
    if not log_location.startswith("s3://"):
        scheme = log_location.split(":", 1)[0]
        return (
            f"`{log_location}` is a {scheme}: location, which dies with the box "
            "(or is capped) — the box has not been redeployed with the S3 run-log "
            "shipper (alpha-engine-config-I11353)"
        )
    base = (store_uri or "").rstrip("/") + "/"
    if not log_location.startswith(base):
        return (
            f"`{log_location}` is outside this store ({base}) — this report can "
            "neither confirm nor deny it, which is not the same as it being there"
        )
    key = log_location[len(base) :]
    try:
        store.get_bytes(key)
    except (FileNotFoundError, KeyError):
        return f"`{log_location}` does not exist — the run log was never shipped"
    except Exception as exc:  # noqa: BLE001 — a denial is a finding, not an absence
        return f"`{log_location}` could not be read: {type(exc).__name__}: {exc}"
    return None


def read_unresolved_logs(
    store: Any, *, trading_day: dt.date, units: Any = None
) -> tuple[list[UnresolvedLog], list[str]]:
    """Every non-ok run manifest of ``trading_day`` whose log does not resolve.

    Returns ``(findings, problems)``. ``problems`` carries listing/parse
    failures by name: this check going blind must not read as it having found
    nothing, which is the same conflation it exists to end one level up.

    Lists per unit prefix, the way `data_gate.evidence.manifests_since` does —
    one bounded listing per unit per day rather than a walk of the whole
    `runs/` tree, whose size grows without bound.
    """
    from data_gate.descriptors import load_units  # noqa: PLC0415 — lazy, like the rest of this module
    from data_gate.evidence import _store_relative  # noqa: PLC0415

    findings: list[UnresolvedLog] = []
    problems: list[str] = []
    store_uri = str(getattr(store, "uri", "") or "")
    day = trading_day.isoformat()
    for unit in units if units is not None else load_units():
        base = f"{_store_relative(unit.run_manifest_prefix)}/{day}/"
        try:
            keys = [k for k in store.list_keys(base) if k.endswith(".json")]
        except Exception as exc:  # noqa: BLE001 — named, never swallowed
            problems.append(f"{base}: listing failed ({type(exc).__name__}: {exc})")
            continue
        for key in sorted(keys):
            try:
                doc = json.loads(store.get_bytes(key))
            except Exception as exc:  # noqa: BLE001 — named, never swallowed
                problems.append(f"{key}: unreadable ({type(exc).__name__}: {exc})")
                continue
            if not isinstance(doc, dict) or doc.get("status") == "ok":
                continue
            why = _resolve_log(store, str(doc.get("log_location") or ""), store_uri)
            if why is not None:
                findings.append(
                    UnresolvedLog(
                        unit_id=str(doc.get("unit_id") or unit.unit_id),
                        manifest_key=key,
                        log_location=str(doc.get("log_location") or ""),
                        reason=why,
                    )
                )
    return findings, problems


def _read_reason(read: Read) -> str:
    """One sentence naming which of the three outcomes happened.

    DENIED is never folded into absent. The denial keeps its service code,
    because `AccessDenied` on a gate prefix is an IAM finding with an owner and
    a fix, and "not there yet" is neither.
    """
    if read.denied:
        return f"DENIED ({read.denied_code}) — this is an access gap, not an absence"
    return read.reason or "absent"


def _dated_keys(store: Any, gate: str) -> list[str]:
    """Every dated reading of ``gate``, newest first.

    A LISTING, deliberately. S3 returns 403 for a missing key when the caller
    also lacks `s3:ListBucket` on the prefix, which is how an absence silently
    becomes a denial; the reporting role is granted `ListBucket` on exactly
    these two prefixes so this call can tell the two apart.
    """
    prefix = gate_prefix(gate)
    keys = [k for k in store.list_keys(prefix) if k.endswith("/gate.json")]
    return sorted(keys, reverse=True)


# ── rendering ────────────────────────────────────────────────────────────────


def _board_line(board: dict[str, Any]) -> str:
    return (
        f"board: {_fraction(board.get('clauses_met'), board.get('clauses_total'))} met, "
        f"{board.get('clauses_unmet', '?')} unmet, "
        f"{board.get('transparency_gap', '?')} UNMEASURABLE "
        f"({board.get('clauses_retired', '?')} retired, "
        f"{board.get('clauses_unconnected', '?')} unconnected, "
        f"{board.get('clauses_standing', 0)} standing, graded by no gate)"
    )


def _phases_met(ladder: dict[str, Any]) -> str:
    rows = [r for r in (ladder.get("phases") or []) if isinstance(r, dict)]
    met = sum(1 for r in rows if r.get("state") == "MET")
    return _fraction(met, len(rows))


def _md_cell(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def render_full_update(inputs: DataReportInputs, *, now: dt.datetime) -> str:
    """The Markdown comment posted to the rolling tracker issue.

    The full detail lives HERE and the headline points at it, rather than the
    headline carrying a truncated version of it — a truncated report is a
    report whose missing part is invisible.
    """
    lines: list[str] = []
    # First, before anything it would qualify. A staleness notice below the
    # numbers is read after the numbers have already been believed.
    if inputs.staleness_line:
        lines += [f"**{inputs.staleness_line}**", ""]

    lines += [
        f"## data collection — {inputs.trading_day.isoformat()} "
        f"(delivered {now.astimezone(DELIVERY_TZ):%Y-%m-%d %H:%M %Z})",
        "",
        f"- store: `{inputs.store_uri}`",
        f"- ladder generated: `{inputs.ladder.get('generated_utc', 'unknown')}`",
        f"- current phase: `{inputs.ladder.get('current_phase', 'unknown')}` "
        f"({_phases_met(inputs.ladder)} phases MET)",
        f"- {_board_line(inputs.board)}",
        "",
        "### ladder",
        "",
        "| phase | state | clauses | dated history |",
        "| --- | --- | --- | --- |",
    ]

    rows = [r for r in (inputs.ladder.get("phases") or []) if isinstance(r, dict)]
    if not rows:
        lines.append(
            "| _no phase row on the ladder_ | | | "
            "_the ladder is not being rendered — a defect in the gate reader_ |"
        )
    for row in rows:
        gate = row.get("gate")
        absent = inputs.history_absent.get(str(gate)) if gate else None
        history = f"ABSENT — {absent}" if absent else "present"
        if not gate:
            history = "n/a — this rung declares no gate"
        lines.append(
            "| {} | {} | {} | {} |".format(
                _md_cell(row.get("phase", "?")),
                _md_cell(row.get("state", "?")),
                _md_cell(_fraction(row.get("clauses_met"), row.get("clauses_total"))),
                _md_cell(history),
            )
        )

    # `alpha-engine-config-I10793`/`-I10788` (Brian's 2026-09-21 ruling): a
    # standing clause is read and rendered every day but gates no phase, so it
    # never appears in the ladder table above (which is built from each
    # phase's OWN gated clause list). Surfaced here, explicitly, so the daily
    # report never hides a real reading behind the exclusion that keeps it
    # from blocking a later phase.
    standing_rows = [r for r in (inputs.board.get("rows") or []) if r.get("standing")]
    if standing_rows:
        lines += ["", "### standing (measured daily, gates no phase)", ""]
        for row in standing_rows:
            lines.append(f"- **{row.get('state', '?')}** `{row.get('clause', '?')}`: {row.get('detail', '')}")

    # `alpha-engine-config-I11353`. A failed run whose log cannot be read is a
    # hole in the recording surface, and it is rendered EVERY day — including
    # the day it is clean, which is what makes the row's own absence visible.
    lines += ["", "### failed runs whose log does not resolve", ""]
    if inputs.unresolved_log_problems:
        lines.append(
            "- **BLIND: this check could not be completed — "
            + "; ".join(inputs.unresolved_log_problems[:4])
            + "**"
        )
    if inputs.unresolved_logs:
        lines += ["| unit | manifest | log_location | why |", "| --- | --- | --- | --- |"]
        for row in inputs.unresolved_logs:
            lines.append(
                "| {} | `{}` | `{}` | {} |".format(
                    _md_cell(row.unit_id),
                    _md_cell(row.manifest_key),
                    _md_cell(row.log_location or "(none)"),
                    _md_cell(row.reason),
                )
            )
    elif not inputs.unresolved_log_problems:
        lines.append(
            f"- none — every non-ok run manifest under `runs/*/{inputs.trading_day.isoformat()}/` "
            "names a log this job could read"
        )

    lines += ["", "### what moved since the previous dated reading", ""]
    if inputs.moved_absent:
        lines.append(f"- **{inputs.moved_absent}**")
    if inputs.moved is not None:
        if inputs.moved.cannot_say:
            lines.append(f"- **cannot say: {inputs.moved.cannot_say}**")
        elif not inputs.moved.lines:
            lines.append("- no clause changed state between the two readings")
        else:
            lines += [f"- {line.lstrip('- ')}" for line in inputs.moved.lines]
    elif not inputs.moved_absent:
        lines.append("- **cannot say: movement was not computed**")

    lines += [
        "",
        "### where to look",
        "",
        f"- board: {_console_link(inputs) or '_no console URL configured for this job_'}",
        f"- readings compared: "
        + (
            ", ".join(f"`{key}`" for key, _ in inputs.readings)
            if inputs.readings
            else "_none — this rung has no dated history_"
        ),
        "",
        "_Rendered by `data_gate report`. Every figure above is quoted from an artifact "
        "`data-gate.yml` published; this job evaluates no clause of its own._",
    ]
    return "\n".join(lines)


def _console_link(inputs: DataReportInputs) -> str | None:
    if not inputs.console_url:
        return None
    return inputs.console_url.rstrip("/") + BOARD_CONSOLE_PATH


def render_message(inputs: DataReportInputs, *, now: dt.datetime, update_url: str) -> str:
    """The Telegram headline. A POINTER at ``update_url``, never a summary of it.

    ``update_url`` is the indispensable content, which is why the tracker
    comment is posted BEFORE this is rendered: a headline with no link to the
    detail is a notification, not a report.
    """
    lines: list[str] = []
    if inputs.staleness_line:
        lines.append(f"<b>{escape(inputs.staleness_line)}</b>")

    lines += [
        f"<b>data collection — {escape(inputs.trading_day.isoformat())}</b>",
        escape(
            f"phase {inputs.ladder.get('current_phase', 'unknown')} "
            f"· {_phases_met(inputs.ladder)} phases MET"
        ),
        escape(_board_line(inputs.board)),
    ]

    if inputs.moved_absent:
        lines.append(escape(inputs.moved_absent))
    elif inputs.moved is not None and inputs.moved.cannot_say:
        lines.append(escape(f"cannot say what moved: {inputs.moved.cannot_say}"))
    elif inputs.moved is not None:
        count = inputs.moved.count
        lines.append(
            escape(
                "no clause changed state since the previous reading"
                if count == 0
                else f"{count} clause(s) changed state since the previous reading"
            )
        )

    for gate, reason in sorted(inputs.history_absent.items()):
        lines.append(escape(f"ABSENT: {gate} — {reason}"))

    # `alpha-engine-config-I11353`. A COUNT in the headline, the rows in the
    # full update — but never silence: a failed run nobody can post-mortem is
    # exactly the thing a reader must learn without opening anything.
    if inputs.unresolved_log_problems:
        lines.append(escape("BLIND: the failed-run log check could not be completed"))
    elif inputs.unresolved_logs:
        lines.append(
            escape(
                f"{len(inputs.unresolved_logs)} failed run(s) whose log does not resolve "
                "— not post-mortemable"
            )
        )

    lines.append(f'<a href="{escape(update_url)}">full update</a>')
    console = _console_link(inputs)
    if console:
        lines.append(f'<a href="{escape(console)}">board</a>')
    lines.append(escape(f"delivered {now.astimezone(DELIVERY_TZ):%H:%M %Z}"))

    body = "\n".join(lines)
    assert_within_budget(
        body,
        budget=wire_budget(
            severity=DELIVERY_SEVERITY,
            source=DELIVERY_SOURCE,
            max_chars=MESSAGE_MAX_CHARS,
        ),
    )
    return body


def render_history_body(rows, *, console_url: str | None = None) -> str:
    """The rolling issue's BODY: an index of every daily update filed.

    The body is REGENERATED after every comment rather than appended to, so the
    index can never disagree with the comments it indexes.
    """
    header = [
        f"# {ROLLING_ISSUE_TITLE}",
        "",
        "One comment per delivered daily update; this body is the index, regenerated "
        "after every comment by `data_gate report` (`alpha-engine-config-I10952`).",
    ]
    if console_url:
        header.append("")
        header.append(f"Board: {console_url.rstrip('/') + BOARD_CONSOLE_PATH}")
    header.append("")
    index = render_history_index(
        rows=rows,
        column_titles=["phase", "phases MET", "board met", "unmeasurable", "moved"],
    )
    return "\n".join(header) + index


def _history_columns(inputs: DataReportInputs) -> dict[str, str]:
    if inputs.moved_absent:
        moved = "ABSENT"
    elif inputs.moved is not None and inputs.moved.cannot_say:
        moved = "cannot say"
    elif inputs.moved is not None and inputs.moved.count is not None:
        moved = str(inputs.moved.count)
    else:
        moved = "cannot say"
    return {
        "phase": str(inputs.ladder.get("current_phase", "unknown")),
        "phases MET": _phases_met(inputs.ladder),
        "board met": _fraction(
            inputs.board.get("clauses_met"), inputs.board.get("clauses_total")
        ),
        "unmeasurable": str(inputs.board.get("transparency_gap", "?")),
        "moved": moved,
    }


# ── delivery ─────────────────────────────────────────────────────────────────


def deliver(message: str, *, console_artifact: str) -> None:
    """Telegram, to the OPERATOR chat, notifying, undeduplicated.

    Three choices that are each a correction of a measured failure, not a
    preference:

    * ``destination`` is passed EXPLICITLY. `krepis.alerts.resolve_destination`
      routes non-`error` severities to the LOG chat whenever
      `TELEGRAM_LOG_CHAT_ID` is configured, so this report reaching Brian would
      otherwise be a property of a fleet-wide setting nobody here controls.
    * ``silent=False`` (the library's fixed default). Crucible's first delivery
      went out silent, its manifest read `ok`, and Brian never saw it — Brian's
      2026-09-03 ruling, `alpha-engine-config-I9916`.
    * no ``dedup_key``. A report that stops arriving when nothing changed is
      indistinguishable from one that stopped arriving.
    """
    _lib_deliver(
        message,
        severity=DELIVERY_SEVERITY,
        source=DELIVERY_SOURCE,
        console_artifact=console_artifact,
        parse_mode="HTML",
    )


def _tracker() -> Tracker:
    return Tracker(
        TrackerConfig(
            repo=TRACKER_REPO,
            token_var=TRACKER_TOKEN_VAR,
            app_ssm_prefix_var=TRACKER_APP_SSM_PREFIX_VAR,
        )
    )


def _put(store: Any, key: str, payload: str) -> None:
    store.put_bytes(key, payload.encode("utf-8"))


def _history_rows(store: Any) -> list[tuple[str, dict | None, str | None]]:
    """Every filed `history_row.json`, oldest first, with its read problem.

    An unreadable row is carried as a row WITH a problem rather than dropped:
    a history index that silently omits the days it could not parse renders a
    gap as continuity.
    """
    rows: list[tuple[str, dict | None, str | None]] = []
    for key in sorted(store.list_keys(f"{RUNS_PREFIX}/")):
        if not key.endswith("/history_row.json"):
            continue
        try:
            rows.append((key, json.loads(store.get_bytes(key)), None))
        except Exception as exc:  # noqa: BLE001 - recorded as the row's own problem
            rows.append((key, None, f"{type(exc).__name__}: {exc}"))
    return rows


def run_report(
    store: Any,
    *,
    trading_day: dt.date,
    calendar_date: dt.date,
    now: dt.datetime | None = None,
    console_url: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Read, file, post, deliver, index. Returns the run manifest.

    ORDER IS THE CONTRACT. The tracker comment is posted BEFORE the headline is
    rendered, because the headline's indispensable content is that comment's
    permalink. Both tracker calls raise, so a failed post leaves this job's
    manifest `failed` and NO Telegram message is sent — a headline pointing at
    a comment that does not exist is worse than no headline.

    The manifest is written on BOTH paths. A job that dies without filing one
    is indistinguishable from a job that never ran, and the absence detector
    would then page for the wrong reason.
    """
    moment = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    trigger = resolve_trigger(override_var="DATA_REPORT_TRIGGER")
    day = trading_day.isoformat()
    cal = calendar_date.isoformat()
    manifest: dict[str, Any] = {
        "schema_version": "run_manifest.v2",
        "job": REPORT_JOB,
        "trading_day": day,
        "calendar_date": cal,
        "trigger": trigger,
        "started_utc": moment.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store": str(getattr(store, "uri", "")),
        "status": "failed",
        "reason": "the run did not reach its own completion",
    }

    def _file_manifest() -> None:
        if dry_run:
            return
        manifest["finished_utc"] = dt.datetime.now(dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        _put(store, manifest_key("run.json", day, cal), json.dumps(manifest, indent=2, sort_keys=True))
        # The trigger is a KEY, not only a field: the closes-when predicate for
        # this deliverable asks whether `trigger.schedule` exists, and a field
        # inside a document cannot be asserted by a key listing.
        _put(store, manifest_key(f"trigger.{trigger}", day, cal), trigger)

    try:
        inputs = read_inputs(
            store,
            trading_day=trading_day,
            calendar_date=calendar_date,
            now=moment,
            console_url=console_url,
        )
        update = render_full_update(inputs, now=moment)

        tracker = _tracker()
        issue = tracker.find_or_create_issue(
            title=ROLLING_ISSUE_TITLE,
            body=render_history_body([], console_url=console_url),
        )
        manifest["tracker_issue"] = issue
        update_url = tracker.post_comment(issue, update)
        manifest["update_url"] = update_url

        message = render_message(inputs, now=moment, update_url=update_url)

        row = history_row(
            trading_day=day,
            delivered_local=moment.astimezone(DELIVERY_TZ).strftime("%Y-%m-%d %H:%M %Z"),
            columns=_history_columns(inputs),
            update_url=update_url,
        )
        if not dry_run:
            _put(store, manifest_key("update.md", day, cal), update)
            _put(store, manifest_key("message.txt", day, cal), message)
            _put(
                store,
                manifest_key("history_row.json", day, cal),
                json.dumps(row, indent=2, sort_keys=True),
            )

        deliver(message, console_artifact=manifest_key("update.md", day, cal))
        manifest["delivered"] = True

        if not dry_run:
            tracker.update_issue_body(
                issue, render_history_body(_history_rows(store), console_url=console_url)
            )

        manifest["status"] = "ok"
        manifest["reason"] = ""
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["reason"] = f"{type(exc).__name__}: {exc}"
        _file_manifest()
        raise
    _file_manifest()
    return manifest
