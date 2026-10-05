"""Append-only adjudication of a FROZEN parity report's exceptions (`alpha-engine-config-I12023`).

**Why this exists.** After the decoupled data cutover the parity clause is frozen at
the last pre-cutover report (`evidence.read_parity`, `alpha-engine-config-I11269`).
That report — `parity/2026-09-28.json` — reads UNMET on CONTENT, and no later
collector run can change it: v1 no longer writes the compared keys, so a new
report would compare the collector with itself. Waiting a week cannot clear it
either. The only honest way forward is to adjudicate each frozen exception from
INDEPENDENT evidence and to record that adjudication beside the report — never
over it. The failed reading stays the reading of record; this module decides
whether a separate, citing record proves every exception away.

**What a record is.** One JSON document under
``parity_adjudication/{report_trading_day}/{NNNN}.json`` in the data_collection
store (`ADJUDICATION_KEY_PREFIX`). Records are append-only: a newer record names
the one it supersedes, and the reader grades only the newest, after checking the
chain is unbroken. A record pins the report it adjudicates by store key AND by
the SHA-256 of the report's bytes, so it cannot be re-pointed at a different
report, and a rewritten report orphans it (red, never silently re-applied).

**The clearing rule — per PATH, never per key** (the Crucible v2 review's
condition (a) on `alpha-engine-config-I12023`). A frozen exception clears only
when ALL of these hold:

* every breaching path is listed (``attributions`` + ``unattributed`` covers
  exactly that many distinct paths) — and the count is the MEASUREMENT's, never
  the record's own claim: a mismatch row's ``values.breaches`` or a prior-day
  pair's ``breaches`` from the report, and for an ``in_region_only`` row the
  ``breach_paths`` of an in-region comparator artifact the record pins by key
  and SHA-256 (the listed paths must be exactly those). A record that lists
  fewer paths than were measured proves nothing about the rest;
* ``unattributed`` is empty — one unattributed path leaves the whole key red;
* every attribution names its SETTLING INPUT(S) (the bar / release that moved
  between v1's read and the shadow's), each declared once per exception with
  the v1 source key AND version id it was read from, and each one itself
  DIFFERING between v1 and the shadow — a derived cell is explained only by an
  input that moved, so "a derived breach with no differing input" is invalid;
* the input really was unsettled at v1's read, by the named lag
  (`settling_lag_rule`) — never by a threshold chosen here;
* the settled value comes from an INDEPENDENT source: v1's own later object
  version written before the cutover (`SETTLED_SOURCE_V1_LATER`, which must
  also declare ``same_definition: true`` and a ``definition_note`` — same field definition and precision
  as the input, so v1's hundreds-rounded volume never serves), or a
  third-party vendor read (`SETTLED_SOURCE_VENDOR`). The collector itself is
  never a source — that would be the self-comparison the freeze exists to
  refuse;
* the input's settled value matches the SHADOW's value OF THAT INPUT within
  the strict parity tolerance — the re-check is made on the input, never on
  the derived cell. A settling cell that still differs after settlement is a BREACH
  (condition (b)) — settling is a reason to wait for the re-check, never a
  reason to drop the cell.

An attribution whose settled value has not been read yet is ``pending``: red,
named, and counted — it is the state exceptions 1, 2 and 6 of the 09-28
report are in until the in-region FRED ALFRED / Polygon reads land.

Anything else — a missing field, an unknown source kind, a vendor read with no
retrieval time — is ``invalid`` and red. There is no waiver verdict: this
module has no vocabulary for "accepted without evidence".
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

__all__ = [
    "ADJUDICATION_KEY_PREFIX",
    "ADJUDICATION_SCHEMA_VERSION",
    "Adjudication",
    "ExceptionGrade",
    "INPUT_KIND_BAR",
    "INPUT_KIND_RELEASE",
    "PRECISION_LIMITED",
    "SETTLED_SOURCE_V1_LATER",
    "SETTLED_SOURCE_VENDOR",
    "adjudication_record_keys",
    "grade_record",
    "report_breach_counts",
    "report_exceptions",
    "settling_lag_rule",
]

#: Relative to the data_collection store, beside — never under — ``parity/``:
#: `evidence._parity_report_day` lists ``parity/`` and must never see a record.
ADJUDICATION_KEY_PREFIX = "parity_adjudication/"

ADJUDICATION_SCHEMA_VERSION = "data_parity_adjudication.v1"

_RECORD_KEY_RE = re.compile(r"^parity_adjudication/(\d{4}-\d{2}-\d{2})/(\d{4})\.json$")

#: The settling input is a vendor daily BAR (OHLCV). Its lag is
#: `dates.bar_settlement`: settled from `dates.SETTLED_AFTER_ET` ET on the
#: bar's own day — the vendor-settlement contract `alpha-engine-config-I11354`
#: declared, not a number tuned to any report.
INPUT_KIND_BAR = "bar"
#: The settling input is a vendor RELEASE of an observation (FRED). It was
#: unsettled at v1's read exactly when v1 read before the observation's first
#: release, which only the vendor's own vintage record can say — so a release
#: attribution must carry ``released_at_utc`` from the settled (vendor) read.
INPUT_KIND_RELEASE = "release"
INPUT_KINDS = frozenset({INPUT_KIND_BAR, INPUT_KIND_RELEASE})

#: v1's own later object version, written BEFORE the cutover (so v1, not the
#: collector, wrote it).
SETTLED_SOURCE_V1_LATER = "v1_later_version"
#: A third-party vendor read (Polygon grouped daily, FRED ALFRED vintage, ...).
SETTLED_SOURCE_VENDOR = "vendor"
SETTLED_SOURCES = frozenset({SETTLED_SOURCE_V1_LATER, SETTLED_SOURCE_VENDOR})

#: Exception kinds — the three ways a frozen report fails on content.
KIND_MISMATCH = "mismatch"
KIND_IN_REGION_ONLY = "in_region_only"
KIND_PRIOR_DAY_UNSETTLED = "prior_day_unsettled"

#: Per-exception verdicts. Only ``cleared`` is passing.
CLEARED = "cleared"
PENDING = "pending"
BREACH = "breach"
UNATTRIBUTED = "unattributed"
#: The settled reference is coarser than the shadow: rounding the shadow
#: half-up to the reference's DECLARED quantum gives the reference, but the
#: unrounded values differ. Neither a breach nor verified-equal — a
#: full-precision vendor read decides it (Crucible v2 ruling on
#: `alpha-engine-config-I12023`, the eight half-cent closes of 2026-09-25).
PRECISION_LIMITED = "precision_limited"
INVALID = "invalid"
MISSING = "missing"


def report_exceptions(report: Mapping[str, Any]) -> dict[str, str]:
    """Every exception a parity report carries, ``{key: kind}``.

    The report's non-passing rows, plus its ``prior_day_settled`` unsettled
    D-1 pairs (they live outside ``summary``; `evidence.read_parity` reads
    them UNMET the same way). `v1_cause` rows are not exceptions — the report
    already explains them.
    """
    passing = {"match", "v1_cause", "not_applicable", "live_superseded"}
    out: dict[str, str] = {}
    for row in report.get("keys") or []:
        verdict = str(row.get("verdict") or "")
        if verdict and verdict not in passing:
            out[str(row.get("key"))] = KIND_IN_REGION_ONLY if verdict == "in_region_only" else KIND_MISMATCH
    prior = report.get("prior_day_settled") or {}
    for example in prior.get("unsettled_examples") or []:
        out[str(example.get("key"))] = KIND_PRIOR_DAY_UNSETTLED
    return out


def settling_lag_rule(
    kind: str, row_date: dt.date, v1_read_utc: dt.datetime, released_at_utc: dt.datetime | None
) -> bool | None:
    """Was the input still settling when v1 read it? ``None`` = cannot say yet.

    The lag is the vendor contract's, by input kind (module docstring):
    `dates.bar_settlement` for a bar; the vendor's own first-release time for a
    release. With no settled read yet there is no release time, which is
    ``pending`` work, never a guess; a settled vendor read that omits
    ``released_at_utc`` is INVALID before this is ever asked (`_grade_input`).
    """
    if kind == INPUT_KIND_BAR:
        from dates import BAR_PROVISIONAL, bar_settlement

        return bar_settlement(v1_read_utc, row_date) == BAR_PROVISIONAL
    if kind == INPUT_KIND_RELEASE:
        if released_at_utc is None:
            return None
        return v1_read_utc < released_at_utc
    raise ValueError(f"unknown settling input kind {kind!r}")


def adjudication_record_keys(keys: Iterable[str], report_day: dt.date) -> list[str]:
    """The adjudication record keys for ``report_day``, oldest first."""
    want = report_day.isoformat()
    found = []
    for key in keys:
        match = _RECORD_KEY_RE.match(key)
        if match and match.group(1) == want:
            found.append((int(match.group(2)), key))
    return [key for _, key in sorted(found)]


@dataclass
class ExceptionGrade:
    key: str
    kind: str
    verdict: str
    detail: str
    paths: int = 0
    attributed: int = 0
    pending: int = 0


@dataclass
class Adjudication:
    """The graded newest record for one report."""

    record_key: str
    cleared: bool
    grades: list[ExceptionGrade] = field(default_factory=list)
    problem: str = ""

    def summary(self) -> str:
        if self.problem:
            return f"adjudication {self.record_key} INVALID: {self.problem}"
        counts: dict[str, int] = {}
        for grade in self.grades:
            counts[grade.verdict] = counts.get(grade.verdict, 0) + 1
        tally = ", ".join(f"{name}={counts[name]}" for name in sorted(counts))
        head = "ADJUDICATED — every frozen exception cleared on independent evidence" if self.cleared else "adjudication open"
        open_items = "; ".join(
            f"{g.key}: {g.verdict} ({g.detail})" for g in self.grades if g.verdict != CLEARED
        )
        return f"{head} ({self.record_key}: {tally})" + (f" — {open_items}" if open_items else "")


def _parse_utc(value: Any, field_name: str) -> dt.datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} is required (ISO-8601 UTC)")
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must carry a UTC offset, got {value!r}")
    return parsed.astimezone(dt.timezone.utc)


def _within(a: Any, b: Any, rel: float, absolute: float) -> bool:
    if isinstance(a, bool) or isinstance(b, bool) or not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return a == b
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return abs(a - b) <= max(absolute, rel * max(abs(a), abs(b)))


def _rounds_to(value: Any, quantum: Any, reference: Any) -> bool:
    """Does ``value`` round half-up to ``reference`` at the DECLARED ``quantum``?

    ``quantum`` must be declared by the settled read (the reference's own
    published precision, e.g. ``0.01`` for a cents-rounded close); without one
    there is no precision claim to test and the answer is False.
    """
    from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

    if quantum in (None, "", 0) or isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        q = Decimal(str(quantum))
        if q <= 0:
            return False
        rounded = (Decimal(str(value)) / q).quantize(Decimal(1), rounding=ROUND_HALF_UP) * q
        return rounded == Decimal(str(reference)).quantize(q, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return False


def _grade_input(item: Mapping[str, Any], cutover_utc: dt.datetime, rel: float, absolute: float) -> tuple[str, str]:
    """``(verdict, why)`` for one settling INPUT. Verdict is CLEARED / PENDING / BREACH / INVALID.

    The settled re-check is made on the input, never on the derived cell: a
    feature computed from a bar is explained only by that bar having moved, and
    is cleared only when that bar's settled value equals the shadow's.
    """
    ident = item.get("id")
    if not isinstance(ident, str) or not ident:
        return INVALID, "a settling input without an id"
    kind = item.get("kind")
    if kind not in INPUT_KINDS:
        return INVALID, f"{ident}: kind {kind!r} is not one of {sorted(INPUT_KINDS)}"
    if not item.get("source_key") or not item.get("source_version_id"):
        return INVALID, f"{ident}: must name the v1 source_key AND source_version_id it was read from"
    try:
        v1_read = _parse_utc(item.get("v1_read_utc"), "v1_read_utc")
        row_date = dt.date.fromisoformat(str(item.get("row_date")))
    except ValueError as exc:
        return INVALID, f"{ident}: {exc}"
    if v1_read >= cutover_utc:
        return INVALID, f"{ident}: v1_read_utc {v1_read.isoformat()} is not before the cutover"
    if "v1_value" not in item or "shadow_value" not in item:
        return INVALID, f"{ident}: needs both v1_value and shadow_value"
    if _within(item["v1_value"], item["shadow_value"], rel, absolute):
        return INVALID, (
            f"{ident}: v1 and the shadow agree on this input, so it cannot explain a breach — "
            "a path is attributed only to an input that itself differs"
        )

    settled = item.get("settled")
    released_at = None
    if settled:
        source_kind = settled.get("source_kind")
        if source_kind not in SETTLED_SOURCES:
            return INVALID, f"{ident}: settled.source_kind {source_kind!r} is not independent ({sorted(SETTLED_SOURCES)})"
        if source_kind == SETTLED_SOURCE_V1_LATER:
            if not settled.get("source_key") or not settled.get("source_version_id"):
                return INVALID, f"{ident}: a v1 later version needs source_key AND source_version_id"
            if settled.get("same_definition") is not True or not str(settled.get("definition_note") or "").strip():
                return INVALID, (
                    f"{ident}: a v1 later version is a reference only when it declares same_definition: true "
                    "and a definition_note (same field definition and precision as the input) — a hundreds-rounded volume is not one"
                )
            try:
                written = _parse_utc(settled.get("source_last_modified_utc"), "settled.source_last_modified_utc")
            except ValueError as exc:
                return INVALID, f"{ident}: {exc}"
            if written >= cutover_utc:
                return INVALID, (
                    f"{ident}: settled version written {written.isoformat()}, after the cutover — "
                    "that is the collector, not v1"
                )
            if written <= v1_read:
                return INVALID, f"{ident}: settled version is not LATER than v1's read"
        elif not settled.get("vendor") or not settled.get("retrieved_at_utc"):
            return INVALID, f"{ident}: a vendor read needs vendor AND retrieved_at_utc"
        if "value" not in settled:
            return INVALID, f"{ident}: settled read carries no value"
        if kind == INPUT_KIND_RELEASE:
            try:
                released_at = _parse_utc(settled.get("released_at_utc"), "settled.released_at_utc")
            except ValueError as exc:
                return INVALID, f"{ident}: {exc}"

    settling = settling_lag_rule(kind, row_date, v1_read, released_at)
    if settling is False:
        return BREACH, f"{ident}: the input was already settled at v1's read, so the difference is not settling"
    if not settled or settling is None:
        return PENDING, f"{ident}: awaiting the post-settlement re-check"
    if not _within(settled["value"], item["shadow_value"], rel, absolute):
        if _rounds_to(item["shadow_value"], settled.get("quantum"), settled["value"]):
            return PRECISION_LIMITED, (
                f"{ident}: the shadow's {item['shadow_value']!r} rounds half-up to the reference's "
                f"declared quantum {settled.get('quantum')!r} as {settled['value']!r}; a full-precision "
                "read decides it"
            )
        return BREACH, (
            f"{ident}: settled {settled['value']!r} still differs from the shadow's "
            f"{item['shadow_value']!r} after settlement"
        )
    return CLEARED, f"{ident}: settled value matches the shadow"


#: Worst first: a path takes the worst verdict among its inputs.
_SEVERITY = (INVALID, BREACH, PRECISION_LIMITED, PENDING, CLEARED)


def report_breach_counts(report: Mapping[str, Any]) -> dict[str, int | None]:
    """The breach count the REPORT measured per exception — never the record's own claim.

    A mismatch row's ``values.breaches`` and a prior-day pair's ``breaches``.
    An ``in_region_only`` row measured nothing (``None``): its count comes from
    the pinned in-region comparator artifact instead (`_grade_exception`).
    """
    out: dict[str, int | None] = {}
    for row in report.get("keys") or []:
        key = str(row.get("key"))
        verdict = str(row.get("verdict") or "")
        if verdict == "in_region_only":
            out[key] = None
        else:
            count = (row.get("values") or {}).get("breaches")
            out[key] = int(count) if isinstance(count, int) and not isinstance(count, bool) else None
    for example in (report.get("prior_day_settled") or {}).get("unsettled_examples") or []:
        count = example.get("breaches")
        out[str(example.get("key"))] = int(count) if isinstance(count, int) and not isinstance(count, bool) else None
    return out


def _comparator_paths(measured: Mapping[str, Any], fetch_bytes: Callable[[str], bytes] | None) -> tuple[list[str] | None, str]:
    """The breaching paths of a pinned in-region comparator artifact, or ``(None, why)``."""
    artifact = measured.get("artifact") or {}
    key, digest = artifact.get("key"), artifact.get("sha256")
    if not key or not digest:
        return None, "an in_region_only row needs measured.artifact {key, sha256}: the in-region comparison it was measured by"
    if fetch_bytes is None:
        return None, f"cannot read the comparator artifact {key}"
    try:
        raw = fetch_bytes(key)
    except Exception as exc:  # noqa: BLE001 - graded INVALID, which is red and named
        return None, f"cannot read the comparator artifact {key}: {type(exc).__name__}: {exc}"
    actual = hashlib.sha256(raw).hexdigest()
    if actual != digest:
        return None, f"comparator artifact {key} hashes to {actual}, not the pinned {digest}"
    try:
        paths = json.loads(raw.decode("utf-8")).get("breach_paths")
    except (ValueError, AttributeError) as exc:
        return None, f"comparator artifact {key} is not a JSON object with breach_paths: {exc}"
    if not isinstance(paths, list) or not all(isinstance(p, str) and p for p in paths):
        return None, f"comparator artifact {key} carries no breach_paths list"
    return paths, ""


def _grade_exception(
    entry: Mapping[str, Any],
    kind: str,
    report_breaches: int | None,
    cutover_utc: dt.datetime,
    rel: float,
    absolute: float,
    fetch_bytes: Callable[[str], bytes] | None,
) -> ExceptionGrade:
    key = str(entry.get("key"))
    measured = entry.get("measured") or {}
    expected_paths: set[str] | None = None
    if kind == KIND_IN_REGION_ONLY:
        paths_from_artifact, why = _comparator_paths(measured, fetch_bytes)
        if paths_from_artifact is None:
            return ExceptionGrade(key, kind, INVALID, why)
        expected_paths = set(paths_from_artifact)
        breaches = len(expected_paths)
    else:
        if report_breaches is None:
            return ExceptionGrade(
                key, kind, INVALID,
                "the report row carries no breach count, so there is nothing to pin the path list to",
            )
        breaches = report_breaches
    if measured.get("breaches") is not None and measured.get("breaches") != breaches:
        return ExceptionGrade(
            key, kind, INVALID,
            f"record claims {measured.get('breaches')} breach(es); the measurement it must match has {breaches}",
            paths=breaches,
        )
    if breaches <= 0:
        return ExceptionGrade(
            key, kind, INVALID,
            "no value breach was measured, so this exception is not a settling difference",
        )

    inputs = {}
    for item in entry.get("inputs") or []:
        ident = item.get("id")
        if ident in inputs:
            return ExceptionGrade(key, kind, INVALID, f"input {ident!r} is declared twice", paths=breaches)
        inputs[ident] = item
    input_verdicts = {ident: _grade_input(item, cutover_utc, rel, absolute) for ident, item in inputs.items()}

    attributions = list(entry.get("attributions") or [])
    unattributed = list(entry.get("unattributed") or [])
    paths = [a.get("path") for a in attributions] + unattributed
    if any(not isinstance(p, str) or not p for p in paths):
        return ExceptionGrade(key, kind, INVALID, "every path must be a non-empty string", paths=breaches)
    if len(set(paths)) != len(paths):
        return ExceptionGrade(key, kind, INVALID, "a path is listed twice", paths=breaches)
    if len(paths) != breaches:
        return ExceptionGrade(
            key, kind, INVALID,
            f"lists {len(paths)} path(s) for {breaches} measured breach(es) — every breaching path must be accounted for",
            paths=breaches,
        )
    if expected_paths is not None and set(paths) != expected_paths:
        return ExceptionGrade(
            key, kind, INVALID, "the listed paths are not the comparator artifact's breach_paths", paths=breaches,
        )

    path_verdicts: list[tuple[str, str]] = []
    for attribution in attributions:
        refs = attribution.get("inputs") or []
        if not refs:
            path_verdicts.append((INVALID, f"{attribution['path']}: names no settling input"))
            continue
        unknown = [r for r in refs if r not in inputs]
        if unknown:
            path_verdicts.append((INVALID, f"{attribution['path']}: names undeclared input(s) {unknown}"))
            continue
        worst = min((input_verdicts[r] for r in refs), key=lambda v: _SEVERITY.index(v[0]))
        path_verdicts.append((worst[0], f"{attribution['path']} <- {worst[1]}"))

    pending = sum(1 for v, _ in path_verdicts if v == PENDING)
    grade = ExceptionGrade(key, kind, CLEARED, "", paths=breaches, attributed=len(attributions), pending=pending)
    for verdict_name in (INVALID, BREACH):
        hits = [why for v, why in path_verdicts if v == verdict_name]
        if hits:
            grade.verdict, grade.detail = verdict_name, f"{len(hits)} path(s); first: {hits[0]}"
            return grade
    limited = [why for v, why in path_verdicts if v == PRECISION_LIMITED]
    if limited:
        grade.verdict, grade.detail = PRECISION_LIMITED, f"{len(limited)} path(s); first: {limited[0]}"
        return grade
    if unattributed:
        grade.verdict, grade.detail = UNATTRIBUTED, f"{len(unattributed)} of {breaches} path(s) unattributed; first: {unattributed[0]}"
        return grade
    if pending:
        grade.verdict, grade.detail = PENDING, f"{pending} of {breaches} path(s) await the settled re-check"
        return grade
    grade.detail = f"{breaches}/{breaches} path(s) attributed to inputs that settled to the shadow's value"
    return grade


def grade_record(
    record: Mapping[str, Any],
    *,
    record_key: str,
    report_key: str,
    report: Mapping[str, Any],
    report_bytes: bytes,
    previous_record_key: str | None,
    cutover_utc: str,
    rel: float | None = None,
    absolute: float | None = None,
    fetch_bytes: Callable[[str], bytes] | None = None,
) -> Adjudication:
    """Grade the newest adjudication record against the report it must cite."""
    if rel is None or absolute is None:
        from shadow.parity import DEFAULT_ABSOLUTE_TOLERANCE, DEFAULT_RELATIVE_TOLERANCE

        rel = DEFAULT_RELATIVE_TOLERANCE if rel is None else rel
        absolute = DEFAULT_ABSOLUTE_TOLERANCE if absolute is None else absolute

    def invalid(problem: str) -> Adjudication:
        return Adjudication(record_key=record_key, cleared=False, problem=problem)

    if record.get("schema_version") != ADJUDICATION_SCHEMA_VERSION:
        return invalid(f"schema_version {record.get('schema_version')!r} is not {ADJUDICATION_SCHEMA_VERSION!r}")
    if record.get("supersedes") != previous_record_key:
        return invalid(
            f"supersedes {record.get('supersedes')!r} but the previous record is {previous_record_key!r} — "
            "the append-only chain is broken"
        )
    cited = record.get("report") or {}
    if cited.get("key") != report_key:
        return invalid(f"cites report {cited.get('key')!r}, not {report_key!r}")
    digest = hashlib.sha256(report_bytes).hexdigest()
    if cited.get("sha256") != digest:
        return invalid(
            f"cites report sha256 {cited.get('sha256')!r} but {report_key} hashes to {digest} — "
            "the report changed after it was adjudicated"
        )

    expected = report_exceptions(report)
    entries = {str(e.get("key")): e for e in record.get("exceptions") or []}
    unknown = sorted(set(entries) - set(expected))
    if unknown:
        return invalid(f"adjudicates key(s) the report does not carry as exceptions: {unknown}")
    cut = _parse_utc(cutover_utc, "cutover_utc")
    counts = report_breach_counts(report)
    grades = []
    for key, kind in sorted(expected.items()):
        entry = entries.get(key)
        if entry is None:
            grades.append(ExceptionGrade(key, kind, MISSING, "not adjudicated"))
            continue
        if entry.get("kind") != kind:
            grades.append(ExceptionGrade(key, kind, INVALID, f"record says kind {entry.get('kind')!r}, report says {kind!r}"))
            continue
        grades.append(_grade_exception(entry, kind, counts.get(key), cut, rel, absolute, fetch_bytes))
    return Adjudication(
        record_key=record_key,
        cleared=bool(grades) and all(g.verdict == CLEARED for g in grades),
        grades=grades,
    )
