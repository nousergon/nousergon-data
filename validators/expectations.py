"""Write-time expectations — the guards every published key passes.

`data_collection_plan_260914.md` §2 row 6 and §4.5; `alpha-engine-config-I10785`
(plan item P-18).

**The gap this closes.** Before this module, exactly three of forty-six units
guarded against publishing an empty-but-fresh artifact: D19 sets
``verify_artifact_exists=True``, D03 refuses a short fetch, D37 refuses an empty
intraday slice. Every other unit could report ``ok``, advance its freshness
sentinel and publish nothing at all, and every detector downstream would read
green — because they read the key's timestamp, not its contents.

**What this checks.** One guard, `empty_fresh`, at the shared publish
chokepoint:

* the unit's contracted key EXISTS on S3 — an absent key under an ``ok`` status
  is a success claim with no artifact behind it;
* it is not zero bytes;
* the collector's own reported row count is above its declared floor.

**What it does NOT do yet, and why that is stated rather than hidden.** The
guard runs at ``weekly_collector._phase_collect`` — after the collector's own
PUT, not before it. `_phase_collect` is the one place every scheduled collector
passes through, which is what makes a *common* guard possible at all; moving the
predicate strictly ahead of the write means every collector routing its own PUT
through this module, which is the phase-2 change this observe period is the
evidence for. Post-PUT is not a weaker check of the same property — it catches
the same empty artifact, one moment later and before any consumer has been told
the artifact is ready, because nothing downstream reads the key until the phase
marker lands.

**Observe first** (`sf-pipeline-policy` §7a). This guard is NEW on a scheduled
pipeline path and its verdict would halt a stage, so it ships in OBSERVE mode:
the predicate runs, the verdict is logged at ERROR, a MetricRecord rides on the
run manifest and renders on the data board — and the exit code does not move.
The promotion criterion is in this module, below, because a guard parked in
observe mode forever is the same defect one direction over.

**A second guard, `cardinality`** (`data_collection_plan_260914.md` §2 row 2 and
§4.5; `alpha-engine-config-I10780`, plan item P-13; extends the gap named by
`alpha-engine-config-I5935`). ``empty_fresh`` catches a unit that published
nothing; ``cardinality`` catches a unit that published SOMETHING but not
everything it was supposed to. The EOD spine (D20,
``collectors/metron_market_data.py``) is the canonical instance: it published
80 closes for an 88-ticker universe with a green SF, because nothing checked
the published count against the declared universe (audit §4.2).

The check is ``covered / (denominator - declared_exclusions) >= floor``:

* **denominator** — the unit's declared universe (D20:
  ``metron/holdings_universe.json``'s ``tickers`` list).
* **declared exclusions** — symbols the denominator can never price, one row
  each in ``contracts/exclusions/unpriced_symbols.yaml`` with a class, reason,
  owner and re-exam date. An UNDECLARED missing symbol is never silently
  dropped from the denominator — it counts against the floor and is named in
  the verdict detail, per the plan's rule: a missing symbol is a failure or a
  declared exclusion, never a silent absence.
* **suffix normalization** — applied to the denominator BEFORE the coverage
  count, from ``contracts/exclusions/suffix_normalization.yaml`` (declared,
  not inferred): a foreign listing's bare denominator symbol (``D05``) is
  mapped to the suffixed form it is actually published under (``D05.SI``)
  before checking whether it was covered.

Same OBSERVE staging as ``empty_fresh`` — logged loud, never raises until
promoted — plus a standalone ``MetricRecord`` published at
``data_collection/metrics/eod_completeness/{trading_day}.json`` (in addition to
riding the run manifest's ``guards[]``/``metrics[]``), which is what
`data_gate/clauses.py`'s ``data.<unit>.completeness`` clause reads.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import yaml
from botocore.exceptions import ClientError
from krepis.metrics import MetricRecord
from nousergon_lib.guard_mode import GuardMode, GuardStaging

logger = logging.getLogger(__name__)

__all__ = [
    "CARDINALITY_GUARD",
    "EMPTY_FRESH_GUARD",
    "CardinalityReading",
    "GuardReading",
    "cardinality_metric",
    "check_cardinality",
    "SPINE_WINDOW_GUARD",
    "check_empty_fresh",
    "check_spine_window",
    "classify_by_rule",
    "default_exclusions_path",
    "default_suffix_map_path",
    "load_class_rules",
    "load_exclusions",
    "load_suffix_map",
    "publish_completeness_metric",
    "read_spine_window",
    "verdict_metric",
]

#: `sf-pipeline-policy` §7a: the staging, its promotion criterion and its
#: tracker, declared in the guard's OWN module.
#:
#: **Promotion criterion: the codified per-guard, risk-based criterion** in
#: `data_gate/guard_promotion.py` (entry ``data_empty_fresh``, HIGH risk — the
#: raise below halts the shared `_phase_collect` chokepoint), graded on the
#: board as ``data.guard_promotion.data_empty_fresh``: every unit that records
#: this guard shows a clean verdict on the latest due cycle of its schedule
#: (`unmeasurable` is NOT clean; it means a unit still reports no row count, and
#: promoting over it would enforce a predicate on units it cannot read), plus an
#: induced-fault commissioning record per unit. Brian's 2026-10-04 option (c)
#: (alpha-engine-config-I11973) replaced the adopted ten-clean-cycle count.
#: Promotion is a deliberate PR flipping `mode` to `GuardMode.ENFORCE` and the
#: entry's `mode` with it, citing that row in its body.
#:
#: `Re-exam:` is tracked on alpha-engine-config-I10785.
EMPTY_FRESH_GUARD = GuardStaging(
    name="data_empty_fresh",
    mode=GuardMode.OBSERVE,
    promotion_criterion=(
        "enforce once the codified per-guard risk-based criterion holds "
        "(data_gate/guard_promotion.py, data_empty_fresh: high risk) — every unit "
        "recording this guard clean on the latest due scheduled cycle, `unmeasurable` "
        "not counting as clean, and commissioned by an induced fault; Brian's "
        "2026-10-04 option (c) on alpha-engine-config-I11973 replaced the ten-cycle "
        "count (data_collection_plan_260914.md §4.5); Re-exam tracked on "
        "alpha-engine-config-I10785"
    ),
    tracked_issue="alpha-engine-config-I10785",
)

#: The guard's closed verdict vocabulary, matching `data_run_manifest.v1`'s
#: `GuardVerdict.verdict` enum. `unmeasurable` is red and counted — it means the
#: guard could not look, which is never a pass (`observability-policy` §8.3).
VERDICTS = ("ok", "empty_fresh", "below_floor", "unmeasurable", "not_applicable")


@dataclass(frozen=True)
class GuardReading:
    """One guard verdict, with what it measured and what it measured against."""

    verdict: str
    detail: str
    key: str | None = None
    value: float | None = None
    baseline: float | None = None

    @property
    def clean(self) -> bool:
        """`ok` and `not_applicable` are clean; everything else is not.

        `unmeasurable` is deliberately NOT clean: a cycle in which the guard
        could not look is not a cycle in which the guard passed, and counting it
        as one is how a promotion criterion gets met by a guard that never ran.
        """
        return self.verdict in ("ok", "not_applicable")


def _head(s3_client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """HEAD the key. ``None`` means a real 404; anything else RAISES.

    Fail-loud per the repo's producer rule: a check that silently reads
    "couldn't look" as "doesn't exist" produces nondeterministic failures, and
    reading it as "exists" defeats the guard entirely. The raise is caught one
    level up and recorded as `unmeasurable`, which is red — never as a pass.
    """
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in ("404", "NoSuchKey"):
            return None
        raise


def check_empty_fresh(
    *,
    unit_id: str,
    artifact_key: str | None,
    bucket: str | None,
    s3_client: Any,
    rows_out: int | None,
    floor: int | None = None,
    rows_key: str | None = None,
    staging: GuardStaging = EMPTY_FRESH_GUARD,
) -> GuardReading:
    """Grade one unit's published key: present, non-empty, above its floor.

    Args:
        unit_id: The audit unit being graded, for the log line and the metric.
        artifact_key: The unit's contracted output key, or ``None`` when the
            unit publishes no single stable key (a per-symbol writer, an
            ArcticDB append). That is `not_applicable`, not a pass — the unit's
            descriptor is what says so, and its cardinality guard is the one
            that covers it (phase 2).
        bucket: The bucket ``artifact_key`` lives in.
        rows_out: The collector's own reported row count, or ``None`` when the
            collector reports none. ``None`` is `unmeasurable`, NEVER 0: reading
            an unreported count as zero would mark every such unit an
            empty-but-fresh write, and reading it as fine would mark none of
            them.
        floor: Minimum acceptable ``rows_out``, from the unit's descriptor.
            ``None`` means the unit declares no floor and only the non-empty
            half of the guard applies.
        rows_key: The result-dict field ``rows_out`` was read from (e.g.
            ``run_units.PhaseUnit.rows_key``), named in the verdict for exactly
            one reason: a floor check that silently trusts a mis-counted
            ``rows_out`` fires false positives, and the fastest way to catch
            that is a verdict that says which number it read, not just what
            the number was. ``None`` when the caller has no such field to name
            (a literal count was passed directly).

    Returns a :class:`GuardReading`. **It never raises on a verdict** — the
    consequence, if any, is the caller's, which is what keeps observe mode a
    one-line difference from enforcing mode.
    """
    source = f"result[{rows_key!r}]" if rows_key else "the caller's own count"
    if not artifact_key or not bucket:
        return GuardReading(
            "not_applicable",
            f"{unit_id} declares no single stable published key at this call site; "
            "its cardinality guard covers the per-symbol/ArcticDB writes (phase 2)",
        )

    try:
        head = _head(s3_client, bucket, artifact_key)
    except Exception as exc:  # noqa: BLE001 -- recorded as UNMEASURABLE, which is red
        return GuardReading(
            "unmeasurable",
            f"could not HEAD s3://{bucket}/{artifact_key}: {type(exc).__name__}: {exc}",
            key=artifact_key,
        )

    if head is None:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} reported success but s3://{bucket}/{artifact_key} does not exist — "
            "a success claim with no artifact behind it",
            key=artifact_key,
            value=0.0,
        )

    size = int(head.get("ContentLength") or 0)
    if size == 0:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} published a ZERO-BYTE object at s3://{bucket}/{artifact_key} — "
            "fresh by timestamp, empty by content, which is the exact write every "
            "freshness detector reads as green",
            key=artifact_key,
            value=0.0,
        )

    if rows_out is None:
        return GuardReading(
            "unmeasurable",
            f"{unit_id} publishes s3://{bucket}/{artifact_key} ({size} bytes) but reports "
            f"no row count (read from {source}), so the empty-and-floor half of this guard "
            "cannot be evaluated. UNMEASURABLE, not a pass: the fix is the collector "
            "reporting its count (run_units.PHASE_UNITS rows_key)",
            key=artifact_key,
            baseline=None if floor is None else float(floor),
        )

    if rows_out == 0:
        return GuardReading(
            "empty_fresh",
            f"{unit_id} published s3://{bucket}/{artifact_key} with 0 rows (read from {source})",
            key=artifact_key,
            value=0.0,
            baseline=None if floor is None else float(floor),
        )

    if floor is not None and rows_out < floor:
        return GuardReading(
            "below_floor",
            f"{unit_id} published {rows_out} rows (read from {source}) to "
            f"s3://{bucket}/{artifact_key}, below its declared floor of {floor}",
            key=artifact_key,
            value=float(rows_out),
            baseline=float(floor),
        )

    return GuardReading(
        "ok",
        f"{unit_id} published {rows_out} rows (read from {source}, {size} bytes) to {artifact_key}",
        key=artifact_key,
        value=float(rows_out),
        baseline=None if floor is None else float(floor),
    )


def verdict_metric(unit_id: str, reading: GuardReading, *, source_path: str) -> MetricRecord:
    """The board row for one verdict — emitted for PASSES as well as failures.

    A guard that records only when it fires is indistinguishable from a guard
    that stopped running (`principles.md` §2.7), so every verdict becomes a
    MetricRecord and rides on the run manifest.
    """
    status = {
        "ok": "GREEN",
        "empty_fresh": "RED",
        "below_floor": "RED",
        "unmeasurable": "N/A-MISSING-INPUT",
        "not_applicable": "N/A-NOT-IMPL",
    }[reading.verdict]
    return MetricRecord(
        name=f"data.{unit_id}.guard.empty_fresh",
        module="nousergon-data",
        metric_type="count",
        # `rows` is the unit of both `value` and `target`: the guard's whole
        # question is how many records landed, against the declared floor.
        unit="rows",
        value=reading.value,
        n_floor=0,
        target=reading.baseline,
        status=status,
        status_reason=reading.detail[:500],
        source_path=source_path,
        last_updated_utc=_now_z(),
    )


# ---------------------------------------------------------------------------
# Cardinality guard — `data_collection_plan_260914.md` §2 row 2 and §4.5
# (plan item P-13); `alpha-engine-config-I10780`, extending `alpha-engine-config-I5935`.
# ---------------------------------------------------------------------------

#: `sf-pipeline-policy` §7a staging for the cardinality guard, declared
#: separately from `EMPTY_FRESH_GUARD` because the two classes promote on
#: independent evidence (a guard parked in observe mode forever is the same
#: defect one direction over, so each needs its own criterion and tracker).
#:
#: **Promotion criterion: the codified per-guard, risk-based criterion** in
#: `data_gate/guard_promotion.py` (entry ``data_cardinality``, MODERATE risk —
#: one unit's own output, and no ENFORCE raise site exists yet), graded on the
#: board as ``data.guard_promotion.data_cardinality``: the EOD spine's
#: ``cardinality`` verdict ``ok`` (coverage >= floor with zero undeclared
#: misses; `unmeasurable` does not count, matching `EMPTY_FRESH_GUARD`'s rule)
#: on the latest due scheduled cycle, plus an induced-fault commissioning
#: record. Brian's 2026-10-04 option (c) (alpha-engine-config-I11973) replaced
#: the adopted ten-clean-trading-day count.
CARDINALITY_GUARD = GuardStaging(
    name="data_cardinality",
    mode=GuardMode.OBSERVE,
    promotion_criterion=(
        "enforce once the codified per-guard risk-based criterion holds "
        "(data_gate/guard_promotion.py, data_cardinality: moderate risk) — the EOD spine's "
        "cardinality verdict `ok` (coverage >= floor, zero undeclared misses) on the latest "
        "due scheduled cycle and commissioned by an induced fault; Brian's 2026-10-04 option "
        "(c) on alpha-engine-config-I11973 replaced the ten-day count "
        "(data_collection_plan_260914.md §4.5); Re-exam tracked on alpha-engine-config-I10780"
    ),
    tracked_issue="alpha-engine-config-I10780",
)

#: Same closed vocabulary shape as `VERDICTS` above, restricted to the
#: verdicts `check_cardinality` can actually return. `empty_fresh`/`below_floor`
#: overlap in spelling with the other guard's vocabulary by design — the board
#: renders them per-guard (`data.<unit>.guard.cardinality` vs
#: `data.<unit>.guard.empty_fresh`), so the shared spelling costs nothing and
#: keeps one status→color mapping for every guard on this board.
CARDINALITY_VERDICTS = ("ok", "below_floor", "unmeasurable", "not_applicable")

#: `CardinalityReading` is spelled distinctly in `__all__` for readers of this
#: module, but it is the exact same shape as `GuardReading` — one verdict
#: vocabulary, one `.clean` rule (`ok`/`not_applicable` clean, everything else
#: not) is enough for both guards on this board.
CardinalityReading = GuardReading

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DEFAULT_EXCLUSIONS_PATH = _REPO_ROOT / "contracts" / "exclusions" / "unpriced_symbols.yaml"
_DEFAULT_SUFFIX_MAP_PATH = _REPO_ROOT / "contracts" / "exclusions" / "suffix_normalization.yaml"

#: The exclusion classes `contracts/exclusions/unpriced_symbols.yaml` may
#: declare. Closed, same rationale as `NA_TAXONOMY`
#: (`data_gate/descriptors.py`): a class nobody defined a rendering for is a
#: new engineering state, not a valid exclusion.
EXCLUSION_CLASSES = frozenset({"fixed_income_cusip", "unsupported_exchange", "contingent_value_right"})


def _cusip_check_digit_ok(symbol: str) -> bool:
    """True when ``symbol`` is a 9-character CUSIP whose check digit verifies
    (the standard modulus-10 "double-add-double" over the first eight)."""
    s = symbol.strip().upper()
    if len(s) != 9 or not s[8].isdigit():
        return False
    total = 0
    for i, ch in enumerate(s[:8]):
        if ch.isdigit():
            v = int(ch)
        elif "A" <= ch <= "Z":
            v = ord(ch) - ord("A") + 10
        elif ch in "*@#":
            v = 36 + "*@#".index(ch)
        else:
            return False
        if i % 2:
            v *= 2
        total += v // 10 + v % 10
    return (10 - total % 10) % 10 == int(s[8])


#: Structural checks a class rule may require on top of its pattern. Closed:
#: a rule naming a validator nobody implemented fails the load.
CLASS_RULE_VALIDATORS = {"cusip_check_digit": _cusip_check_digit_ok}


def default_exclusions_path() -> pathlib.Path:
    return _DEFAULT_EXCLUSIONS_PATH


def default_suffix_map_path() -> pathlib.Path:
    return _DEFAULT_SUFFIX_MAP_PATH


def load_exclusions(path: pathlib.Path | None = None) -> dict[str, dict[str, str]]:
    """Parse ``contracts/exclusions/unpriced_symbols.yaml`` into ``{symbol: entry}``.

    Fails loud on a malformed row (missing field, unknown class, duplicate
    symbol) — a declared exclusion the loader silently drops is the exact
    silent-exclusion failure mode this file exists to prevent, one level up.
    """
    p = path or _DEFAULT_EXCLUSIONS_PATH
    document = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    rows = document.get("exclusions") or []
    out: dict[str, dict[str, str]] = {}
    required = ("symbol", "class", "reason", "owner", "re_exam")
    for row in rows:
        missing = [f for f in required if not str(row.get(f) or "").strip()]
        if missing:
            raise ValueError(f"{p}: exclusion entry {row!r} is missing required field(s) {missing}")
        symbol = str(row["symbol"]).strip()
        cls = str(row["class"]).strip()
        if cls not in EXCLUSION_CLASSES:
            raise ValueError(
                f"{p}: {symbol!r} declares class {cls!r}, not one of {sorted(EXCLUSION_CLASSES)}"
            )
        if symbol in out:
            raise ValueError(f"{p}: duplicate exclusion for symbol {symbol!r}")
        out[symbol] = {
            "class": cls,
            "reason": str(row["reason"]).strip(),
            "owner": str(row["owner"]).strip(),
            "re_exam": str(row["re_exam"]).strip(),
        }
    return out


def load_class_rules(path: pathlib.Path | None = None) -> list[dict[str, Any]]:
    """Parse the ``class_rules`` block of ``unpriced_symbols.yaml``.

    A class rule declares an instrument CLASS the vendor chain can never price
    (a contingent value right, a CUSIP-keyed bond), so a new instance of that
    class is a declared exclusion the day it enters the universe instead of an
    undeclared miss waiting for a hand-added row (2026-09-28: ATAI.CVR, a CVR
    from the ATAI/Beckley merger, was an undeclared miss on every trading day
    from 09-18). The rule is still a committed declaration, with a reason,
    owner and re-exam date, and it only ever explains a MISS: a symbol it
    matches that was priced counts as covered.

    Fails loud on a malformed rule: unanchored or uncompilable pattern,
    unknown class or validator, missing field.
    """
    p = path or _DEFAULT_EXCLUSIONS_PATH
    document = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    out: list[dict[str, Any]] = []
    required = ("class", "pattern", "reason", "owner", "re_exam")
    for row in document.get("class_rules") or []:
        missing = [f for f in required if not str(row.get(f) or "").strip()]
        if missing:
            raise ValueError(f"{p}: class rule {row!r} is missing required field(s) {missing}")
        cls = str(row["class"]).strip()
        if cls not in EXCLUSION_CLASSES:
            raise ValueError(f"{p}: class rule declares class {cls!r}, not one of {sorted(EXCLUSION_CLASSES)}")
        pattern = str(row["pattern"]).strip()
        if not (pattern.startswith("^") and pattern.endswith("$")):
            raise ValueError(f"{p}: class rule pattern {pattern!r} must be anchored with ^...$")
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"{p}: class rule pattern {pattern!r} does not compile: {exc}") from exc
        validator = str(row.get("validator") or "").strip() or None
        if validator is not None and validator not in CLASS_RULE_VALIDATORS:
            raise ValueError(
                f"{p}: class rule validator {validator!r} is not one of {sorted(CLASS_RULE_VALIDATORS)}"
            )
        out.append({
            "class": cls,
            "pattern": compiled,
            "validator": validator,
            "reason": str(row["reason"]).strip(),
            "owner": str(row["owner"]).strip(),
            "re_exam": str(row["re_exam"]).strip(),
        })
    return out


def classify_by_rule(symbol: str, class_rules: Sequence[Mapping[str, Any]]) -> str | None:
    """The class of the first rule ``symbol`` satisfies, else None."""
    for rule in class_rules:
        if not rule["pattern"].match(symbol):
            continue
        validator = rule.get("validator")
        if validator and not CLASS_RULE_VALIDATORS[validator](symbol):
            continue
        return rule["class"]
    return None


def load_suffix_map(path: pathlib.Path | None = None) -> dict[str, str]:
    """Parse ``contracts/exclusions/suffix_normalization.yaml`` into
    ``{denominator_symbol: priced_symbol}``.

    Declared, not inferred: a symbol with no row here that fails a direct
    match is a real miss (declared exclusion or an undeclared gap), never
    silently resolved by a guessed suffix pattern.
    """
    p = path or _DEFAULT_SUFFIX_MAP_PATH
    document = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    rows = document.get("normalize") or []
    out: dict[str, str] = {}
    for row in rows:
        denom = str(row.get("denominator_symbol") or "").strip()
        priced = str(row.get("priced_symbol") or "").strip()
        if not denom or not priced:
            raise ValueError(f"{p}: normalize entry {row!r} needs denominator_symbol and priced_symbol")
        if denom in out:
            raise ValueError(f"{p}: duplicate normalization row for {denom!r}")
        out[denom] = priced
    return out


def check_cardinality(
    *,
    unit_id: str,
    denominator_symbols: Any,
    covered_symbols: Any,
    exclusions: Mapping[str, Mapping[str, str]] | None = None,
    suffix_map: Mapping[str, str] | None = None,
    class_rules: Sequence[Mapping[str, Any]] | None = None,
    floor: float = 1.0,
    staging: GuardStaging = CARDINALITY_GUARD,
) -> CardinalityReading:
    """Grade one unit's published coverage against its declared universe.

    ``covered / (denominator - declared_exclusions) >= floor``, denominator
    symbols normalized through ``suffix_map`` before the lookup. Args:

        unit_id: The audit unit being graded (e.g. ``D20``).
        denominator_symbols: The unit's declared universe (e.g. the `tickers`
            list from ``metron/holdings_universe.json``).
        covered_symbols: The symbols the unit actually published this run
            (e.g. the keys of the published `closes` map).
        exclusions: ``{symbol: entry}`` from ``load_exclusions()`` — declared
            symbols the denominator can never price. ``None`` loads the
            committed contract.
        suffix_map: ``{denominator_symbol: priced_symbol}`` from
            ``load_suffix_map()``, applied to a denominator symbol before
            checking whether it was covered. ``None`` loads the committed
            contract.
        class_rules: from ``load_class_rules()`` — declared instrument classes
            that are never priced. Applied only to a symbol that is missing
            after normalization and the per-symbol exclusions; each one it
            explains is named with its class in the detail. ``None`` loads
            the committed contract.
        floor: Minimum acceptable coverage ratio (D20's declared floor is 1.0).

    An empty denominator is `unmeasurable`, never a vacuous pass — a unit that
    cannot even measure its own universe has nothing for this guard to grade,
    and a silent 1.0 there would look identical to a genuinely complete unit.
    A symbol from the denominator missing after both normalization AND
    exclusion is named explicitly in the detail — never absorbed into a bare
    coverage number — because that is exactly the undeclared-miss failure
    this guard exists to catch.
    """
    if exclusions is None:
        exclusions = load_exclusions()
    if suffix_map is None:
        suffix_map = load_suffix_map()
    if class_rules is None:
        class_rules = load_class_rules()

    denominator = {str(s).strip() for s in denominator_symbols if str(s).strip()}
    covered = {str(s).strip() for s in covered_symbols if str(s).strip()}

    if not denominator:
        return CardinalityReading(
            "unmeasurable",
            f"{unit_id}: denominator is empty — nothing to grade cardinality against",
            baseline=float(floor),
        )

    declared_excluded = sorted(s for s in denominator if s in exclusions)
    effective_denominator = denominator - set(declared_excluded)

    matched: list[str] = []
    undeclared_missing: list[str] = []
    rule_excluded: list[tuple[str, str]] = []
    for symbol in sorted(effective_denominator):
        lookup = suffix_map.get(symbol, symbol)
        if lookup in covered or symbol in covered:
            matched.append(symbol)
            continue
        cls = classify_by_rule(symbol, class_rules)
        if cls is not None:
            rule_excluded.append((symbol, cls))
        else:
            undeclared_missing.append(symbol)
    effective_denominator -= {s for s, _ in rule_excluded}

    n_effective = len(effective_denominator)
    n_matched = len(matched)
    coverage = (n_matched / n_effective) if n_effective else 1.0

    detail = (
        f"{unit_id}: {n_matched}/{n_effective} covered "
        f"({len(declared_excluded)} declared exclusion(s): "
        f"{', '.join(declared_excluded) if declared_excluded else 'none'}) "
        f"against a {len(denominator)}-symbol denominator, floor {floor}"
    )
    if rule_excluded:
        detail += (
            f"; {len(rule_excluded)} class-rule exclusion(s): "
            + ", ".join(f"{s} ({c})" for s, c in rule_excluded)
        )
    if undeclared_missing:
        detail += (
            f" — {len(undeclared_missing)} UNDECLARED miss(es): "
            f"{', '.join(undeclared_missing)}. An undeclared missing symbol is a failure, "
            "never a silent exclusion (plan §2 row 2): add a row (or, for a whole "
            "never-priced instrument class, a class rule) to "
            "contracts/exclusions/unpriced_symbols.yaml, or fix the fetch."
        )
    else:
        detail += " — zero undeclared misses"

    if coverage < floor:
        return CardinalityReading("below_floor", detail, value=coverage, baseline=float(floor))
    return CardinalityReading("ok", detail, value=coverage, baseline=float(floor))


def cardinality_metric(unit_id: str, reading: CardinalityReading, *, source_path: str) -> MetricRecord:
    """The board row for one cardinality verdict — emitted for passes too
    (`principles.md` §2.7 — a guard that records only when it fires is
    indistinguishable from a guard that stopped running).

    Distinct metric name/unit from `verdict_metric` (``ratio`` of covered
    symbols, not a row count) so the two guards never collide on the board.
    """
    status = {
        "ok": "GREEN",
        "below_floor": "RED",
        "unmeasurable": "N/A-MISSING-INPUT",
        "not_applicable": "N/A-NOT-IMPL",
    }[reading.verdict]
    return MetricRecord(
        name=f"data.{unit_id}.completeness",
        module="nousergon-data",
        metric_type="ratio",
        unit="ratio",
        value=reading.value,
        n_floor=0,
        target=reading.baseline,
        status=status,
        status_reason=reading.detail[:500],
        source_path=source_path,
        last_updated_utc=_now_z(),
    )


def publish_completeness_metric(
    s3_client: Any,
    bucket: str,
    trading_day: str,
    metric: MetricRecord,
    *,
    prefix: str = "data_collection/metrics/eod_completeness",
) -> str:
    """PUT one unit's completeness `MetricRecord` at ``{prefix}/{trading_day}.json``.

    The dedicated key `data_gate/clauses.py`'s ``data.<unit>.completeness``
    clause reads — separate from the run manifest's ``metrics[]`` (which also
    carries this record) because the gate ladder reads a fixed address per
    trading day, not a run id it would have to discover first.
    """
    import json

    key = f"{prefix.rstrip('/')}/{trading_day}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(metric.model_dump(mode="json", exclude_none=True), sort_keys=True, indent=2).encode(
            "utf-8"
        ),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# Spine window — the cardinality guard's SESSION axis (`alpha-engine-config-I10780`)
# ---------------------------------------------------------------------------

#: `sf-pipeline-policy` §7a staging for the spine-window reading. Separate from
#: `CARDINALITY_GUARD` because it answers a question that guard cannot:
#: ``check_cardinality`` grades ONE session's symbol set as the collector holds
#: it, so (a) a session the EOD run never published leaves no reading at all on
#: the producer side, and (b) a symbol published with a bar from an EARLIER
#: session counts as covered, because the guard sees the key, not the bar date.
#: Measured 2026-10-05 over 2026-08-03..2026-10-02 (44 NYSE sessions): two
#: sessions absent (08-05, 08-06), and nine symbol-days carried a bar two
#: sessions old (e.g. FNILX/FTIHX/FZILX on 09-23, ATAI on 09-15) while the
#: per-day reading counted them covered.
#:
#: **Promotion criterion: 10 consecutive clean trading days**, the same count
#: and the same `unmeasurable`-is-not-clean rule as `CARDINALITY_GUARD`, on the
#: same tracker.
SPINE_WINDOW_GUARD = GuardStaging(
    name="data_spine_window",
    mode=GuardMode.OBSERVE,
    promotion_criterion=(
        "enforce once the codified per-guard risk-based criterion holds "
        "(data_gate/guard_promotion.py, data_spine_window: moderate risk) — the EOD spine-window "
        "verdict `ok` (every session in the window published, non-empty and dated to its "
        "session; every undeclared symbol carrying a bar within the declared lag) on the latest "
        "due scheduled cycle, `unmeasurable` not counting as clean; Brian's 2026-10-04 option "
        "(c) on alpha-engine-config-I11973 replaced the ten-day count "
        "(data_collection_plan_260914.md §4.5); Re-exam tracked on alpha-engine-config-I10780"
    ),
    tracked_issue="alpha-engine-config-I10780",
)

#: The verdicts `check_spine_window` can return. `empty_fresh` is reserved for
#: the session THIS run published: an empty earlier session is graded
#: `below_floor` and named, because `data_gate/evidence.py::empty_fresh_runs`
#: attributes any `empty_fresh` verdict on a manifest to the run that carries it.
SPINE_WINDOW_VERDICTS = ("ok", "empty_fresh", "below_floor", "unmeasurable")


def read_spine_window(
    s3_client: Any,
    bucket: str,
    sessions: Sequence[str],
    *,
    prefix: str = "market_data/eod_closes/",
) -> dict[str, Mapping[str, Any] | None]:
    """GET each session's published spine document → ``{session: doc | None}``.

    ``None`` is a real 404. A zero-byte object reads as ``{}`` (empty-but-fresh,
    graded by ``check_spine_window``). Any other read error, or a body that is
    not JSON, RAISES — "could not look" is never read as "absent" or as
    "present"; the caller grades it ``unmeasurable``.
    """
    import json

    out: dict[str, Mapping[str, Any] | None] = {}
    for day in sessions:
        key = f"{prefix}{day}.json"
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code") or "")
            if code in ("NoSuchKey", "404", "NotFound"):
                out[day] = None
                continue
            raise
        raw = obj["Body"].read()
        out[day] = json.loads(raw) if raw else {}
    return out


def check_spine_window(
    *,
    unit_id: str,
    session: str,
    documents: Mapping[str, Mapping[str, Any] | None],
    denominator_symbols: Any,
    max_bar_lag_sessions: int,
    bar_lag: Any,
    exclusions: Mapping[str, Mapping[str, str]] | None = None,
    suffix_map: Mapping[str, str] | None = None,
    class_rules: Sequence[Mapping[str, Any]] | None = None,
    floor: float = 1.0,
) -> GuardReading:
    """Grade the published EOD spine across a window of sessions.

    Two axes, one reading:

    * **sessions** — every session in ``documents`` must have a published
      document (``None`` = missing), with a non-empty ``closes`` map (else
      empty-but-fresh), whose ``as_of`` is that session (else misdated: a copy
      of another day's spine under this day's key).
    * **symbols, on ``session``** — ``check_cardinality`` over the symbols
      whose ``bar_date`` is within ``max_bar_lag_sessions`` trading sessions of
      ``session`` (``bar_lag(bar_date, session) -> int``). A symbol published
      with an older bar is NOT covered for this session and is named with its
      lag; exclusions, suffix map and class rules apply exactly as in
      ``check_cardinality``.

    Earlier sessions are graded on the session axis only: the universe is read
    once, today, and grading last week's spine against today's holdings would
    name every newly bought symbol as a miss.

    ``documents`` must contain ``session``. Verdict: ``empty_fresh`` when
    ``session`` itself published an empty spine; ``below_floor`` when any
    session is missing, empty or misdated, or the symbol axis is below
    ``floor``; ``unmeasurable`` when the symbol axis could not be graded (empty
    denominator); else ``ok``.
    """
    if session not in documents:
        raise ValueError(f"{unit_id}: documents must include the graded session {session!r}")

    missing: list[str] = []
    empty: list[str] = []
    misdated: list[str] = []
    for day in sorted(documents):
        doc = documents[day]
        if doc is None:
            missing.append(day)
            continue
        closes = doc.get("closes")
        if not isinstance(closes, Mapping) or not closes:
            empty.append(day)
            continue
        if str(doc.get("as_of") or "") != day:
            misdated.append(f"{day} (as_of {doc.get('as_of')!r})")

    current = documents[session]
    current_closes = (current or {}).get("closes")
    fresh: list[str] = []
    stale: list[str] = []
    if isinstance(current_closes, Mapping):
        for symbol, point in sorted(current_closes.items()):
            bar = point.get("bar_date") if isinstance(point, Mapping) else None
            try:
                lag = int(bar_lag(str(bar), session)) if bar else None
            except (TypeError, ValueError):
                lag = None
            if lag is not None and lag <= max_bar_lag_sessions:
                fresh.append(str(symbol))
            else:
                stale.append(f"{symbol}@{bar} (lag {lag if lag is not None else 'unreadable'})")

    symbols = check_cardinality(
        unit_id=unit_id,
        denominator_symbols=denominator_symbols,
        covered_symbols=fresh,
        exclusions=exclusions,
        suffix_map=suffix_map,
        class_rules=class_rules,
        floor=floor,
    )

    n_sessions = len(documents)
    n_good = n_sessions - len(missing) - len(empty) - len(misdated)
    days = sorted(documents)
    detail = (
        f"{unit_id} spine {days[0]}..{days[-1]}: {n_good}/{n_sessions} session(s) published, "
        "non-empty and dated to their session"
    )
    if missing:
        detail += f"; MISSING session(s): {', '.join(missing)}"
    if empty:
        detail += f"; EMPTY-but-fresh session(s): {', '.join(empty)}"
    if misdated:
        detail += f"; MISDATED session(s): {', '.join(misdated)}"
    if stale:
        detail += (
            f"; {len(stale)} symbol(s) on {session} with a bar older than "
            f"{max_bar_lag_sessions} session(s), not counted covered: {', '.join(stale)}"
        )
    detail += f". Symbol axis on {session}: {symbols.detail}"

    if session in empty:
        verdict = "empty_fresh"
    elif missing or empty or misdated or symbols.verdict == "below_floor":
        verdict = "below_floor"
    elif symbols.verdict == "unmeasurable":
        verdict = "unmeasurable"
    else:
        verdict = "ok"
    return GuardReading(verdict, detail, value=symbols.value, baseline=float(floor))


def report(
    reading: GuardReading,
    *,
    unit_id: str,
    staging: GuardStaging = EMPTY_FRESH_GUARD,
    log: logging.Logger | None = None,
) -> None:
    """Log the verdict. LOUD while observing (`sf-pipeline-policy` §7a rule 3).

    An observe-mode verdict nobody reads is a suppression, not an observation.
    So a non-clean verdict is a real ERROR on the real log surface in both
    modes, and the only thing that differs at promotion is whether the caller
    then raises.
    """
    logger_ = log or logger
    if reading.clean:
        logger_.info("guard=%s unit=%s verdict=%s %s", staging.name, unit_id, reading.verdict, reading.detail)
        return
    logger_.error(
        "guard=%s unit=%s verdict=%s mode=%s %s [%s]",
        staging.name,
        unit_id,
        reading.verdict,
        staging.mode.value,
        reading.detail,
        staging.describe(),
    )


def _now_z() -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
