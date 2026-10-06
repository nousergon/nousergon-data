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

Four further rules (Crucible v2 rulings on `alpha-engine-config-I12023`,
issuecomment-6023457908 and -6024224623). Each is a proof shape, never a tolerance:

* **Regraded settling value** (an input's ``regrade`` block). The value the
  collector KEEPS for a day-T bar is the one its scheduled regrade writes after
  settlement; the same-evening write is provisional by contract. So when an
  input carries a regrade, the post-settlement re-check compares the REGRADE
  with the vendor's settled value — exactly, never within a band — and the
  frozen ``shadow_value`` stays recorded beside it. A regrade counts only when
  a ``scheduled`` run wrote it and `dates.bar_settlement` (computed HERE from
  its write time) reads it ``settled``; anything else is invalid.
* **Declared cast quantum** (``regrade.cast``). A regrade that differs from a
  fractional vendor value is ``quantized_equal`` iff it equals ``trunc(vendor)``
  and the record cites the writer's cast by file:line at the commit that wrote
  the regrade. Any other difference is a breach; a missing citation is invalid.
* **Release interval** (``settled.released_within``). A FRED release clears on
  ``[lower, upper]`` — lower: v1's own archived object showing the observation
  ABSENT; upper: the shadow's read — with ALFRED's ``realtime_start`` date
  inside it and ALFRED's value equal to the shadow's. No instant is inferred.
* **Recompute proof** (an attribution's ``recompute`` block). The derived value
  recomputed from the vendor-settled inputs must reproduce the shadow's derived
  value within the strict tolerance. With it, a path may cite an input that
  moved only inside the input tolerance; it is required for any path that cites
  a named ``input_groups`` entry, whose every member is graded (a pending
  member makes the path pending, a breaching one a breach).
* **Shared input** (``kind: shared``, issuecomment-6025196251 ruling 2). An
  input BOTH sides read at the same S3 key and VersionId contributes no
  difference, so it cannot be a cause and needs no vendor read — the yfinance
  IV snapshot behind ``iv_vs_rv``, which no vendor can re-serve. It is cleared
  only on READ EVIDENCE: each side's VersionId with the run log, manifest or
  object-version listing it came from; a side not evidenced is ``pending``, and
  equal values never stand in for it. A shared input explains nothing by
  itself: it may be cited only beside a recompute and beside at least one
  non-shared input, which must clear on its own. It does not establish that the
  snapshot was correct — that is vendor vetting, not parity.

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
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal, InvalidOperation
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

__all__ = [
    "ADJUDICATION_KEY_PREFIX",
    "ADJUDICATION_SCHEMA_VERSION",
    "Adjudication",
    "ExceptionGrade",
    "INPUT_KIND_BAR",
    "INPUT_KIND_RELEASE",
    "INPUT_KIND_SHARED",
    "PRECISION_LIMITED",
    "QUANTIZED_EQUAL",
    "REGRADE_TRIGGER_SCHEDULED",
    "CAST_TRUNC",
    "SETTLED_SOURCE_V1_LATER",
    "SETTLED_SOURCE_VENDOR",
    "SHARED_EVIDENCE_KINDS",
    "adjudication_record_keys",
    "grade_record",
    "report_breach_counts",
    "report_exceptions",
    "settled_inputs_digest",
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
#: An input both sides read at the SAME S3 key and VersionId (module docstring,
#: "Shared input"): not a settling input, so it has no lag and no settled read.
INPUT_KIND_SHARED = "shared"
INPUT_KINDS = frozenset({INPUT_KIND_BAR, INPUT_KIND_RELEASE, INPUT_KIND_SHARED})

#: Where a side's read VersionId may be evidenced from — what the side itself
#: recorded at read time, or the object's version history bounding what any
#: read in that window could have returned. Never the two values being equal.
SHARED_EVIDENCE_RUN_LOG = "run_log"
SHARED_EVIDENCE_RUN_MANIFEST = "run_manifest"
SHARED_EVIDENCE_OBJECT_VERSIONS = "object_versions"
SHARED_EVIDENCE_KINDS = frozenset({
    SHARED_EVIDENCE_RUN_LOG, SHARED_EVIDENCE_RUN_MANIFEST, SHARED_EVIDENCE_OBJECT_VERSIONS,
})

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
#: A regraded value equals the vendor's settled value under the WRITER's
#: declared cast (``trunc`` for an integer ``Volume`` column), cited by
#: file:line at the commit that wrote the regrade. A clearing verdict: the
#: subject's own storage type cannot represent more (Crucible v2 ruling 1 in
#: issuecomment-6024224623) — unlike ``precision_limited``, where the REFERENCE
#: is the coarse side and a full-precision read still has to decide.
QUANTIZED_EQUAL = "quantized_equal"
INVALID = "invalid"
MISSING = "missing"

#: The only run trigger whose regrade counts: the collector's own scheduled
#: regrade path. A manual or backfill rewrite is not the value the contract
#: keeps (issuecomment-6023457908, condition 2).
REGRADE_TRIGGER_SCHEDULED = "scheduled"

#: The writer's declared casts a regrade may be quantized by. ``trunc`` is
#: Python's ``int()`` on a non-integer: toward zero.
CAST_TRUNC = "trunc"
CASTS = frozenset({CAST_TRUNC})

#: The calendars a date-only vendor release stamp (ALFRED ``realtime_start``)
#: is tested in. The vendor does not publish an instant, so the date must sit
#: inside the release interval in EVERY plausible publisher calendar — the
#: strictest reading, never the most convenient one.
_RELEASE_DATE_ZONES = ("UTC", "America/New_York", "America/Chicago")

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

#: How an input is graded for one path (`_grade_input`):
#: ``strict`` — the input must differ between v1 and the shadow beyond the
#: strict tolerance; ``recompute`` — a recompute proof backs the path, so an
#: input that moved only inside the input tolerance may be cited (it must
#: still differ AT ALL); ``member`` — a member of a named input group: it is
#: graded against its settled read whether or not it moved.
_MODE_STRICT = "strict"
_MODE_RECOMPUTE = "recompute"
_MODE_MEMBER = "member"


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
    kind: str,
    row_date: dt.date,
    v1_read_utc: dt.datetime,
    released_at_utc: dt.datetime | None,
    *,
    absent_at_utc: dt.datetime | None = None,
) -> bool | None:
    """Was the input still settling when v1 read it? ``None`` = cannot say yet.

    The lag is the vendor contract's, by input kind (module docstring):
    `dates.bar_settlement` for a bar; the vendor's own first-release time for a
    release. With no settled read yet there is no release time, which is
    ``pending`` work, never a guess; a settled vendor read that omits both
    ``released_at_utc`` and ``released_within`` is INVALID before this is ever
    asked (`_grade_input`).

    ``absent_at_utc`` is a release interval's lower bound: v1's own archived
    object, written then, shows the observation ABSENT, so the release came
    after it — and v1 read no later than that, settling by proof.
    """
    if kind == INPUT_KIND_BAR:
        from dates import BAR_PROVISIONAL, bar_settlement

        return bar_settlement(v1_read_utc, row_date) == BAR_PROVISIONAL
    if kind == INPUT_KIND_RELEASE:
        if absent_at_utc is not None:
            return v1_read_utc <= absent_at_utc
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
    quantized: int = 0


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


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _exactly_equal(a: Any, b: Any) -> bool:
    """Equal with NO tolerance: ``1 == 1.0``, ``2194085 != 2194085.662907``."""
    if _is_number(a) and _is_number(b):
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return Decimal(str(a)) == Decimal(str(b))
    return a == b


def _trunc(value: Any) -> int:
    """The writer's ``int()`` cast: truncation toward zero, computed in decimal."""
    return int(Decimal(str(value)).to_integral_value(rounding=ROUND_DOWN))


def settled_inputs_digest(settled_values: Mapping[str, Any]) -> str:
    """SHA-256 of the vendor-settled input values a recompute proof was computed from.

    ``{input id: settled.value}`` (a shared input contributes its own value, read
    identically by both sides) → canonical JSON (sorted ``[id, value]`` pairs,
    compact separators, no NaN). A record builder computes it with this same
    function; `_grade_recompute` recomputes it from the record's own inputs, so a
    recompute cannot silently have used values other than the settled ones.
    """
    pairs = sorted([str(k), v] for k, v in settled_values.items())
    return hashlib.sha256(json.dumps(pairs, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _validate_regrade(item: Mapping[str, Any], ident: str, kind: str, row_date: dt.date) -> str:
    """``""`` when the input's ``regrade`` block is admissible, else why it is INVALID.

    Ruling issuecomment-6023457908: the regrade must come from the collector's
    own SCHEDULED regrade path, land after `dates.bar_settlement` (computed
    here from the write time, never taken from the record), and name the store
    version it wrote. Ruling 1 of issuecomment-6024224623 for ``cast``.
    """
    regrade = item.get("regrade")
    if not isinstance(regrade, Mapping):
        return f"{ident}: regrade must be an object"
    if kind != INPUT_KIND_BAR:
        return f"{ident}: only a bar is regraded after settlement; a {kind!r} input cannot carry a regrade"
    if not _is_number(regrade.get("value")):
        return f"{ident}: regrade.value must be the regraded number"
    if not regrade.get("key") or not regrade.get("version_id"):
        return f"{ident}: regrade must name the store key AND version_id it wrote"
    try:
        written = _parse_utc(regrade.get("written_utc"), "regrade.written_utc")
    except ValueError as exc:
        return f"{ident}: {exc}"
    run = regrade.get("run")
    if not isinstance(run, Mapping) or not run.get("run_id"):
        return f"{ident}: regrade.run must name the run_id that wrote it"
    if run.get("trigger") != REGRADE_TRIGGER_SCHEDULED:
        return (
            f"{ident}: regrade written by a {run.get('trigger')!r} run — only the collector's scheduled "
            "regrade path counts; a manual or backfill rewrite is not the value the contract keeps"
        )
    from dates import BAR_SETTLED, bar_settlement

    if bar_settlement(written, row_date) != BAR_SETTLED:
        return (
            f"{ident}: regrade written {written.isoformat()}, before {row_date.isoformat()}'s bar settled "
            "(dates.bar_settlement) — it is another provisional value, not the settled one"
        )
    if "cast" in regrade:
        cast = regrade.get("cast")
        if not isinstance(cast, Mapping):
            return f"{ident}: regrade.cast must be an object"
        if cast.get("rule") not in CASTS:
            return f"{ident}: regrade.cast.rule {cast.get('rule')!r} is not a declared cast ({sorted(CASTS)})"
        code_sha = run.get("code_sha")
        if not isinstance(code_sha, str) or not _SHA_RE.match(code_sha):
            return f"{ident}: a declared cast needs regrade.run.code_sha (the full commit that wrote the regrade)"
        citations = cast.get("citations")
        if not isinstance(citations, list) or not citations:
            return f"{ident}: a declared cast needs its citations (file:line at the commit that wrote the regrade)"
        for cite in citations:
            if not isinstance(cite, Mapping) or not cite.get("file") or not isinstance(cite.get("line"), int) \
                    or isinstance(cite.get("line"), bool) or cite["line"] <= 0:
                return f"{ident}: every cast citation needs file and a positive line"
            if cite.get("commit_sha") != code_sha:
                return (
                    f"{ident}: cast cited at {cite.get('commit_sha')!r}, not the regrade's commit {code_sha} — "
                    "a cast is the writer's only at the commit that wrote the value"
                )
        if not isinstance(regrade.get("value"), int) or isinstance(regrade.get("value"), bool):
            return f"{ident}: a trunc cast writes an integer; regrade.value {regrade.get('value')!r} is not one"
    return ""


def _grade_regraded(item: Mapping[str, Any], ident: str, settled: Mapping[str, Any]) -> tuple[str, str]:
    """The post-settlement re-check on the REGRADE, never on the frozen shadow value."""
    regrade = item["regrade"]
    regraded, vendor = regrade["value"], settled["value"]
    if _exactly_equal(regraded, vendor):
        return CLEARED, f"{ident}: regrade {regraded!r} equals the vendor's settled {vendor!r}"
    cast = regrade.get("cast")
    if cast is None:
        return BREACH, (
            f"{ident}: regraded {regraded!r} still differs from the vendor's settled {vendor!r} after "
            f"settlement (frozen shadow value {item['shadow_value']!r}); no cast is declared"
        )
    if not _is_number(vendor) or math.isnan(vendor) or math.isinf(vendor):
        return INVALID, f"{ident}: the vendor value {vendor!r} cannot be cast"
    if "vendor_value" not in cast or not _exactly_equal(cast.get("vendor_value"), vendor):
        return INVALID, f"{ident}: regrade.cast.vendor_value {cast.get('vendor_value')!r} is not settled.value {vendor!r}"
    truncated = _trunc(vendor)
    if "vendor_trunc" not in cast or not _exactly_equal(cast.get("vendor_trunc"), truncated):
        return INVALID, f"{ident}: regrade.cast.vendor_trunc {cast.get('vendor_trunc')!r} is not trunc({vendor!r}) = {truncated}"
    if regraded == truncated:
        cites = ", ".join(f"{c['file']}:{c['line']}" for c in cast["citations"])
        return QUANTIZED_EQUAL, (
            f"{ident}: regrade {regraded!r} == trunc({vendor!r}) under the writer's declared cast ({cites})"
        )
    return BREACH, (
        f"{ident}: regraded {regraded!r} is neither the vendor's settled {vendor!r} nor its trunc {truncated} "
        "under the declared cast"
    )


def _release_interval(
    settled: Mapping[str, Any], ident: str, v1_read: dt.datetime, cutover_utc: dt.datetime
) -> tuple[dt.datetime, dt.datetime, dt.date]:
    """``(lower, upper, realtime_start)`` of a ``released_within`` block; raises ValueError naming why not.

    Ruling 3 of issuecomment-6024224623: lower is v1's OWN archived object
    showing the observation absent (independent of the collector, so written
    before the cutover), upper is the shadow's read. Both carry key + version id.
    """
    within = settled.get("released_within")
    if not isinstance(within, Mapping):
        raise ValueError("released_within must be an object {lower, upper}")
    lower, upper = within.get("lower"), within.get("upper")
    if not isinstance(lower, Mapping) or not lower.get("key") or not lower.get("version_id"):
        raise ValueError(
            "released_within.lower needs v1's archived object (key AND version_id) showing the observation absent"
        )
    if lower.get("observation_absent") is not True:
        raise ValueError("released_within.lower must show the observation ABSENT (observation_absent: true)")
    lower_at = _parse_utc(lower.get("written_utc"), "released_within.lower.written_utc")
    if lower_at >= cutover_utc:
        raise ValueError(
            f"released_within.lower written {lower_at.isoformat()}, after the cutover — that is the collector, not v1"
        )
    if lower_at < v1_read:
        raise ValueError(
            "released_within.lower predates v1's read, so it cannot show the observation was unreleased when v1 read"
        )
    if not isinstance(upper, Mapping) or not upper.get("key") or not upper.get("version_id"):
        raise ValueError("released_within.upper needs the shadow's read (key AND version_id)")
    upper_at = _parse_utc(upper.get("read_utc"), "released_within.upper.read_utc")
    if upper_at <= lower_at:
        raise ValueError("released_within.upper is not after its lower bound")
    raw = settled.get("realtime_start")
    if not isinstance(raw, str) or not raw:
        raise ValueError("released_within needs the vendor's realtime_start date (ALFRED)")
    return lower_at, upper_at, dt.date.fromisoformat(raw)


def _date_within(day: dt.date, lower: dt.datetime, upper: dt.datetime) -> bool:
    """Is a date-only release stamp inside ``[lower, upper]`` in every candidate calendar?"""
    from zoneinfo import ZoneInfo

    for zone in _RELEASE_DATE_ZONES:
        tz = ZoneInfo(zone)
        if not lower.astimezone(tz).date() <= day <= upper.astimezone(tz).date():
            return False
    return True


def _grade_input(
    item: Mapping[str, Any], cutover_utc: dt.datetime, rel: float, absolute: float, mode: str = _MODE_STRICT
) -> tuple[str, str]:
    """``(verdict, why)`` for one settling INPUT. Verdict is CLEARED / QUANTIZED_EQUAL / PENDING /
    PRECISION_LIMITED / BREACH / INVALID.

    The settled re-check is made on the input, never on the derived cell: a
    feature computed from a bar is explained only by that bar having moved, and
    is cleared only when that bar's settled value equals the shadow's — or,
    when the input carries an admissible ``regrade``, when the REGRADE equals
    the vendor (exactly, or under the writer's declared cast).
    """
    ident = item.get("id")
    if not isinstance(ident, str) or not ident:
        return INVALID, "a settling input without an id"
    kind = item.get("kind")
    if kind not in INPUT_KINDS:
        return INVALID, f"{ident}: kind {kind!r} is not one of {sorted(INPUT_KINDS)}"
    if kind == INPUT_KIND_SHARED:
        return _grade_shared(item, ident, mode)
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
    moved = not _within(item["v1_value"], item["shadow_value"], rel, absolute)
    if mode == _MODE_STRICT and not moved:
        return INVALID, (
            f"{ident}: v1 and the shadow agree on this input, so it cannot explain a breach — "
            "a path is attributed only to an input that itself differs"
        )
    if mode == _MODE_RECOMPUTE and _exactly_equal(item["v1_value"], item["shadow_value"]):
        return INVALID, (
            f"{ident}: v1 and the shadow hold the identical value, so even a recompute proof cannot "
            "attribute a breach to it"
        )
    if "regrade" in item:
        problem = _validate_regrade(item, ident, kind, row_date)
        if problem:
            return INVALID, problem

    settled = item.get("settled")
    released_at = None
    absent_at = None
    if settled:
        source_kind = settled.get("source_kind")
        if source_kind not in SETTLED_SOURCES:
            return INVALID, f"{ident}: settled.source_kind {source_kind!r} is not independent ({sorted(SETTLED_SOURCES)})"
        if source_kind == SETTLED_SOURCE_V1_LATER:
            if "regrade" in item:
                return INVALID, f"{ident}: a regrade is graded against a third-party vendor read, not a v1 version"
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
            has_instant = settled.get("released_at_utc") is not None
            has_interval = settled.get("released_within") is not None
            if has_instant and has_interval:
                return INVALID, f"{ident}: give settled.released_at_utc OR settled.released_within, not both"
            if has_interval:
                try:
                    absent_at, upper_at, realtime_start = _release_interval(settled, ident, v1_read, cutover_utc)
                except ValueError as exc:
                    return INVALID, f"{ident}: {exc}"
                if not _date_within(realtime_start, absent_at, upper_at):
                    return BREACH, (
                        f"{ident}: the vendor's realtime_start {realtime_start.isoformat()} is outside the release "
                        f"interval [{absent_at.isoformat()}, {upper_at.isoformat()}]"
                    )
            else:
                try:
                    released_at = _parse_utc(settled.get("released_at_utc"), "settled.released_at_utc")
                except ValueError as exc:
                    return INVALID, (
                        f"{ident}: {exc} — or settled.released_within [lower, upper]; an instant is never inferred"
                    )

    if mode == _MODE_MEMBER and not moved:
        # A group member that did not move explains nothing by itself; it is
        # graded only on whether its settled value is the shadow's.
        settling: bool | None = True
    else:
        settling = settling_lag_rule(kind, row_date, v1_read, released_at, absent_at_utc=absent_at)
    if settling is False:
        return BREACH, f"{ident}: the input was already settled at v1's read, so the difference is not settling"
    if not settled or settling is None:
        return PENDING, f"{ident}: awaiting the post-settlement re-check"
    if "regrade" in item:
        return _grade_regraded(item, ident, settled)
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


def _grade_shared(item: Mapping[str, Any], ident: str, mode: str) -> tuple[str, str]:
    """A shared input: both sides read the same S3 key at the same VersionId, on read evidence.

    CLEARED only when each side's VersionId is evidenced and they are equal;
    PENDING while either side is not evidenced (never inferred from equal
    values); BREACH when the sides read different versions — then the input is
    not shared and must be attributed as a settling input instead.
    """
    if mode == _MODE_STRICT:
        return INVALID, (
            f"{ident}: a shared input contributes no difference, so it may be cited only beside a recompute proof"
        )
    if any(field_name in item for field_name in ("settled", "regrade", "source_version_id")):
        return INVALID, (
            f"{ident}: a shared input has no settling read, regrade or single source_version_id — "
            "each side's read is evidenced under shared.v1_read / shared.shadow_read"
        )
    if not item.get("source_key"):
        return INVALID, f"{ident}: a shared input must name the S3 source_key both sides read"
    if "v1_value" not in item or "shadow_value" not in item:
        return INVALID, f"{ident}: needs both v1_value and shadow_value"
    shared = item.get("shared")
    if not isinstance(shared, Mapping):
        return INVALID, f"{ident}: a shared input needs shared.v1_read and shared.shadow_read"
    versions: dict[str, str] = {}
    unevidenced: list[str] = []
    for side in ("v1_read", "shadow_read"):
        read = shared.get(side)
        if read is None:
            unevidenced.append(side)
            continue
        if not isinstance(read, Mapping):
            return INVALID, f"{ident}: shared.{side} must be an object {{version_id, evidence}}"
        evidence = read.get("evidence")
        if evidence is not None and (
            not isinstance(evidence, Mapping) or evidence.get("kind") not in SHARED_EVIDENCE_KINDS
        ):
            return INVALID, (
                f"{ident}: shared.{side}.evidence.kind must be one of {sorted(SHARED_EVIDENCE_KINDS)}"
            )
        if not read.get("version_id") or evidence is None or not str(evidence.get("ref") or "").strip():
            unevidenced.append(side)
            continue
        versions[side] = str(read["version_id"])
    if unevidenced:
        return PENDING, (
            f"{ident}: the VersionId {' and '.join(unevidenced)} read is not evidenced yet — "
            "equal values never stand in for it"
        )
    if versions["v1_read"] != versions["shadow_read"]:
        return BREACH, (
            f"{ident}: v1 read VersionId {versions['v1_read']} and the shadow {versions['shadow_read']} — "
            "different versions, so this input is not shared"
        )
    if not _exactly_equal(item["v1_value"], item["shadow_value"]):
        return INVALID, (
            f"{ident}: both sides read VersionId {versions['v1_read']} of {item['source_key']} yet record "
            "different values — the extraction differs, not the input"
        )
    return CLEARED, f"{ident}: both sides read VersionId {versions['v1_read']} of {item['source_key']}"


def _reference_value(item: Mapping[str, Any]) -> tuple[bool, Any]:
    """``(known, value)`` an input contributes to a recompute: its settled read, or a shared input's own value."""
    if item.get("kind") == INPUT_KIND_SHARED:
        return ("shadow_value" in item), item.get("shadow_value")
    settled = item.get("settled")
    if not settled or "value" not in settled:
        return False, None
    return True, settled["value"]


def _grade_recompute(
    path: str,
    recompute: Any,
    input_ids: list[str],
    inputs: Mapping[str, Mapping[str, Any]],
    rel: float,
    absolute: float,
    digests: dict[tuple[str, ...], str],
) -> tuple[str, str]:
    """A path's recompute proof (ruling 4 of issuecomment-6024224623).

    The module never computes a feature: it receives the value the formula
    gives on the vendor-settled inputs as DATA, with the formula's provenance,
    and checks (i) the recompute was made from exactly those settled values
    (``settled_inputs_sha256``, `settled_inputs_digest`) and (ii) it reproduces
    the shadow's derived value at this path within the strict tolerance.
    """
    if not isinstance(recompute, Mapping):
        return INVALID, f"{path}: recompute must be an object"
    formula = recompute.get("formula")
    if not isinstance(formula, Mapping) or not formula.get("ref") or not isinstance(formula.get("commit_sha"), str) \
            or not _SHA_RE.match(formula["commit_sha"]):
        return INVALID, f"{path}: recompute.formula needs ref AND the full commit_sha it was taken from"
    if not _is_number(recompute.get("value")):
        return INVALID, f"{path}: recompute.value must be the recomputed number"
    if "shadow_value" not in recompute:
        return INVALID, f"{path}: recompute.shadow_value (the shadow's derived value it must reproduce) is required"
    unsettled = [i for i in input_ids if not _reference_value(inputs[i])[0]]
    if unsettled:
        return PENDING, f"{path}: recompute awaits the settled read of {len(unsettled)} input(s); first: {unsettled[0]}"
    key = tuple(input_ids)
    if key not in digests:
        try:
            digests[key] = settled_inputs_digest({i: _reference_value(inputs[i])[1] for i in input_ids})
        except (TypeError, ValueError) as exc:
            return INVALID, f"{path}: the settled inputs cannot be digested: {exc}"
    if recompute.get("settled_inputs_sha256") != digests[key]:
        return INVALID, (
            f"{path}: recompute.settled_inputs_sha256 is not the digest of its inputs' settled values — "
            "it was not computed from the vendor-settled inputs this record grades"
        )
    if not _within(recompute["value"], recompute["shadow_value"], rel, absolute):
        return BREACH, (
            f"{path}: recomputed {recompute['value']!r} from the settled inputs ({formula['ref']}) does not "
            f"reproduce the shadow's {recompute['shadow_value']!r}"
        )
    return CLEARED, f"{path}: recompute from the settled inputs reproduces the shadow's value ({formula['ref']})"


#: Worst first: a path takes the worst verdict among its inputs.
_SEVERITY = (INVALID, BREACH, PRECISION_LIMITED, PENDING, QUANTIZED_EQUAL, CLEARED)


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

    groups, why = _input_groups(entry, inputs)
    if groups is None:
        return ExceptionGrade(key, kind, INVALID, why, paths=breaches)

    graded: dict[tuple[str, str], tuple[str, str]] = {}

    def grade_one(ident: str, mode: str) -> tuple[str, str]:
        if (ident, mode) not in graded:
            graded[(ident, mode)] = _grade_input(inputs[ident], cutover_utc, rel, absolute, mode)
        return graded[(ident, mode)]

    def worst(verdicts: Iterable[tuple[str, str]]) -> tuple[str, str]:
        return min(verdicts, key=lambda v: _SEVERITY.index(v[0]))

    group_worst: dict[str, tuple[str, str]] = {}
    group_moved: dict[str, bool] = {}
    for name, members in groups.items():
        group_worst[name] = worst(grade_one(m, _MODE_MEMBER) for m in members)
        group_moved[name] = any(not _exactly_equal(inputs[m].get("v1_value"), inputs[m].get("shadow_value")) for m in members)
    digests: dict[tuple[str, ...], str] = {}

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
        path = attribution["path"]
        refs = attribution.get("inputs") or []
        group_refs = attribution.get("input_groups") or []
        recompute = attribution.get("recompute")
        if not isinstance(refs, list) or not isinstance(group_refs, list):
            path_verdicts.append((INVALID, f"{path}: inputs and input_groups must be lists"))
            continue
        if not refs and not group_refs:
            path_verdicts.append((INVALID, f"{path}: names no settling input"))
            continue
        unknown = [r for r in refs if r not in inputs]
        if unknown:
            path_verdicts.append((INVALID, f"{path}: names undeclared input(s) {unknown}"))
            continue
        unknown_groups = [g for g in group_refs if g not in groups]
        if unknown_groups:
            path_verdicts.append((INVALID, f"{path}: names undeclared input group(s) {unknown_groups}"))
            continue
        if group_refs and recompute is None:
            path_verdicts.append((
                INVALID,
                f"{path}: cites input group(s) {group_refs} without a recompute proof — a group attributes a path "
                "only when a recompute from its settled members reproduces it",
            ))
            continue
        if refs and not group_refs and all(inputs[r].get("kind") == INPUT_KIND_SHARED for r in refs):
            path_verdicts.append((
                INVALID,
                f"{path}: every input it cites is shared, so nothing differs that could explain the breach",
            ))
            continue
        if group_refs and not refs and not any(group_moved[g] for g in group_refs):
            path_verdicts.append((INVALID, f"{path}: no member of {group_refs} differs between v1 and the shadow"))
            continue
        mode = _MODE_STRICT if recompute is None else _MODE_RECOMPUTE
        verdicts = [grade_one(r, mode) for r in refs] + [group_worst[g] for g in group_refs]
        if recompute is not None:
            all_ids = sorted(set(refs).union(*(groups[g] for g in group_refs)))
            verdicts.append(_grade_recompute(path, recompute, all_ids, inputs, rel, absolute, digests))
        verdict, why = worst(verdicts)
        path_verdicts.append((verdict, f"{path} <- {why}"))

    pending = sum(1 for v, _ in path_verdicts if v == PENDING)
    quantized = sum(1 for v, _ in path_verdicts if v == QUANTIZED_EQUAL)
    grade = ExceptionGrade(
        key, kind, CLEARED, "", paths=breaches, attributed=len(attributions), pending=pending, quantized=quantized,
    )
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
        first_pending = next(why for v, why in path_verdicts if v == PENDING)
        grade.verdict, grade.detail = PENDING, (
            f"{pending} of {breaches} path(s) await the settled re-check; first: {first_pending}"
        )
        return grade
    grade.detail = f"{breaches}/{breaches} path(s) attributed to inputs that settled to the shadow's value"
    if quantized:
        grade.detail += f" ({quantized} quantized_equal under the writer's declared cast)"
    return grade


def _input_groups(
    entry: Mapping[str, Any], inputs: Mapping[str, Any]
) -> tuple[dict[str, list[str]] | None, str]:
    """The exception's named input groups ``{name: [member ids]}``, or ``(None, why)``.

    A group is defined ONCE per exception and lists every member by id; each
    member must be a declared input (so it is graded). An attribution may cite
    a group in place of listing its members (the "Also confirmed" ruling in
    issuecomment-6024224623) — never a member set the record does not spell out.
    """
    raw = entry.get("input_groups")
    if raw is None:
        return {}, ""
    if not isinstance(raw, Mapping):
        return None, "input_groups must be an object {name: {definition, members}}"
    out: dict[str, list[str]] = {}
    for name, group in raw.items():
        if not isinstance(group, Mapping) or not str(group.get("definition") or "").strip():
            return None, f"input group {name!r} needs a definition (what the group is)"
        members = group.get("members")
        if not isinstance(members, list) or not members:
            return None, f"input group {name!r} must enumerate its members"
        if len(set(members)) != len(members):
            return None, f"input group {name!r} lists a member twice"
        undeclared = [m for m in members if m not in inputs]
        if undeclared:
            return None, (
                f"input group {name!r} names {len(undeclared)} member(s) that are not declared inputs, so they "
                f"are not graded; first: {undeclared[0]!r}"
            )
        out[str(name)] = list(members)
    return out, ""


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
