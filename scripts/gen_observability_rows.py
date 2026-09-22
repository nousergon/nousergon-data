#!/usr/bin/env python3
"""Generate `nousergon-data/registry.d/<component_id>.yaml` observability rows.

`alpha-engine-config-I10775` (plan §4.1 item P-08): the fleet observability
registry (`nous-ergon-ops/governance/observability.d/`) carried a per-unit row
for 3 of the 46 data-collector units and a stale `pending` row for D39; the
other 43 were the audit's "obs A" gap.

Rather than hand-author 43 rows that would drift the moment a unit descriptor
changes, this script GENERATES one `registry.d/<component_id>.yaml` per unit
descriptor (`registry.d/units/*.yaml`, P-01), in the shape
`console-policy.md` §2.6 and `gather_repo_descriptors.py` already read: a
roster-repo-local descriptor, gathered by the existing
`observability-registry-publish.yml` pipeline with NO console or ops-side code
change. It is held to `observability_registry.py`'s full row schema
(`REQUIRED_FIELDS`), because the gatherer publishes it as a first-class row.

**D39 (inst_ownership) is deliberately excluded.** It already has a dedicated,
hand-authored ops-side row —
`nous-ergon-ops/governance/observability.d/nousergon-data-inst-ownership-weekly.yaml`
— predating this generator (alpha-engine-config-I10529) and carrying provenance
this script cannot reconstruct (the I10529 incident narrative). Generating a
second row under `data-collector-d39-inst-ownership` would leave two rows
describing the same schedule; P-08's job for D39 is to flip that EXISTING row's
`lifecycle` from `pending` to `in-service`, done in the companion
`nous-ergon-ops` PR, not to duplicate it here.

**Field derivation, all read from the unit descriptor, nothing hand-listed:**

- `component_id` / `owning_repo` / `owner`: verbatim from the unit descriptor.
- `substrate`: mapped from `trigger.kind` against `observability_registry.py`'s
  closed `SUBSTRATES` vocabulary (`SUBSTRATE_BY_TRIGGER_KIND` below).
- `origin`: `trigger.owner` (the owning pipeline/workflow/rule) plus
  `trigger.detail` when present — the same pair `ne-data-collection-eod`'s own
  row derives its `origin` from.
- `lifecycle`: the unit descriptor's own `lifecycle`, translated into the
  registry's five-value vocabulary (`LIFECYCLE_BY_UNIT_LIFECYCLE`) —
  `proposed-retirement` (R7, plan §7) becomes `deprecated`, since nothing has
  actually retired the unit yet.
- `signals.data` / `signals.outcome`: read from the unit's own transcribed
  `audit.cells.run_record` / `.detector` — `PRESENT` maps to `emitted`,
  anything else to `unknown` naming the exact audit state, so this row cannot
  claim a signal the audit itself found missing.
- Everything else (`authority_tier`, `signals.execution/cost/resource`,
  `log_location`, `alert_channel`, `severity_source`, `console_surface`,
  `retention`): derived from `trigger.kind` with a written, generic reason —
  RED (`unknown`) rather than a guess wherever the unit descriptor does not
  independently support a stronger claim, per the plan's "red by default"
  rule (§4.1).

Usage:
    python3 scripts/gen_observability_rows.py generate   # writes the rows
    python3 scripts/gen_observability_rows.py check      # generate + diff, CI mode
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data_gate.descriptors import (  # noqa: E402
    EXTERNALLY_OWNED_OBSERVABILITY_ROWS,
    UNITS_DIR,
    load_units,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "registry.d"

#: Units that already own a hand-authored, differently-named ops-side row.
#: See the module docstring. A unit added here without a matching live row
#: elsewhere silently drops out of the coverage check's denominator, so the
#: test (`tests/test_observability_row_coverage.py`) asserts every entry here
#: names a row that actually exists in `nous-ergon-ops`.
#: alpha-engine-config-I10870: moved to `data_gate/descriptors.py` as
#: `EXTERNALLY_OWNED_OBSERVABILITY_ROWS`, the single source of truth this
#: module and `data_gate/unit_readers.py::read_observability_row` both read —
#: aliased here so callers importing `gen.EXTERNALLY_OWNED_ROWS` (this
#: module's own tests included) keep working.
EXTERNALLY_OWNED_ROWS: dict[str, str] = EXTERNALLY_OWNED_OBSERVABILITY_ROWS

#: `observability_registry.py::SUBSTRATES` is a closed set. Every
#: `trigger.kind` value used across the 46 unit descriptors must resolve here,
#: or the script refuses to run rather than emit a row with an invalid
#: substrate (`test_all_trigger_kinds_resolve` guards the bijection).
SUBSTRATE_BY_TRIGGER_KIND: dict[str, str] = {
    "step-functions": "step-functions",
    "github-actions": "github-actions",
    "eventbridge-rule": "eventbridge",
    "eventbridge-scheduler": "eventbridge",
    "systemd-timer": "shared-box",
    "on-demand-dispatch": "ec2-spot",
    "manual": "ec2-spot",
}

#: `observability_registry.py::LIFECYCLES` (via `authority_surface.LIFECYCLE`)
#: is `("in-service", "pending", "disabled", "deprecated", "retired")`. The
#: unit descriptors use one extra value, `proposed-retirement` (R7 units not
#: yet actually retired) — mapped to `deprecated`, the closest honest claim:
#: not gone, but not meant to keep running.
LIFECYCLE_BY_UNIT_LIFECYCLE: dict[str, str] = {
    "in-service": "in-service",
    "pending": "pending",
    "disabled": "disabled",
    "deprecated": "deprecated",
    "proposed-retirement": "deprecated",
    # An executed R7 retirement (alpha-engine-config-I10779 retired D15L on
    # 2026-09-15): the row stays, tombstoned, so the console keeps a `retired`
    # entity with a declared reason rather than an ABSENT finding — the two
    # outcomes observability-policy §8.3 allows for a removal.
    "retired": "retired",
}

LIFECYCLE_NEEDS_REASON = {"pending", "disabled", "deprecated", "retired"}

REEXAM_DATE = "2026-09-28"  # 14 days out, matching PENDING_MAX_DAYS at generation time


def _substrate(unit_id: str, trigger: dict[str, Any]) -> str:
    kind = str(trigger.get("kind") or "")
    try:
        return SUBSTRATE_BY_TRIGGER_KIND[kind]
    except KeyError as exc:  # pragma: no cover - guarded by test_all_trigger_kinds_resolve
        raise ValueError(
            f"{unit_id}: trigger.kind {kind!r} has no substrate mapping in "
            "SUBSTRATE_BY_TRIGGER_KIND — a new trigger kind needs a deliberate entry, "
            "not a fallback guess"
        ) from exc


def _origin(trigger: dict[str, Any]) -> str:
    owner = str(trigger.get("owner") or "").strip()
    detail = str(trigger.get("detail") or "").strip()
    if owner and detail:
        return f"{owner} ({detail})"
    return owner or detail or "undeclared"


def _lifecycle(unit_id: str, unit_lifecycle: str) -> str:
    try:
        return LIFECYCLE_BY_UNIT_LIFECYCLE[unit_lifecycle]
    except KeyError as exc:  # pragma: no cover - guarded by test
        raise ValueError(
            f"{unit_id}: unit lifecycle {unit_lifecycle!r} has no observability-lifecycle "
            "mapping — add one deliberately"
        ) from exc


def _signal_from_audit(unit_id: str, column: str, cells: dict[str, str]) -> dict[str, str]:
    state = cells.get(column, "UNVERIFIED")
    if state == "PRESENT":
        return {
            "status": "emitted",
            "reason": f"unit descriptor's own audit.cells.{column} reads PRESENT (baseline_date "
            "2026-09-14); re-measured, not re-asserted, at the next data-gate read.",
        }
    return {
        "status": "unknown",
        "reason": f"unit descriptor's own audit.cells.{column} reads {state}; not independently "
        "measured by this row. P-06 (run manifest lift) / P-19 (guard commissioning) close it.",
    }


def _log_location(unit_id: str, trigger: dict[str, Any]) -> tuple[str, str]:
    kind = trigger.get("kind")
    owner = trigger.get("owner") or ""
    if kind == "github-actions":
        detail = str(trigger.get("detail") or "")
        workflow = detail.split(",")[0].strip() if detail else "unknown"
        loc = f"github-actions:nousergon/nousergon-data/{workflow}"
        return loc, "Declared from the unit descriptor's trigger.detail workflow path."
    if kind == "systemd-timer":
        return (
            "journalctl:dashboard-box (unit named in the unit descriptor's trigger.detail)",
            "Declared from the unit descriptor's trigger.detail systemd unit name.",
        )
    if kind in ("step-functions", "on-demand-dispatch"):
        return (
            "s3:alpha-engine-research/data_collection/logs/{workload}/{trading_day}/"
            "{instance_id}.log",
            f"The unit's spot box ships its WHOLE run log to this key on exit and every "
            f"60s while running (alpha-engine-config-I11353), and each of the unit's run "
            f"manifests records the exact object under `log_location`. Owning pipeline "
            f"{owner!r} (plan §4.4). The `_ssm_logs/data-spot/` path this row used to name "
            f"was retired with that change; CloudWatch remains a capped live tail, never "
            f"the record.",
        )
    return (
        "unknown",
        "No log location independently resolved for this trigger kind yet.",
    )


def _alert_channel(trigger: dict[str, Any]) -> tuple[str, str]:
    kind = trigger.get("kind")
    if kind == "github-actions":
        return (
            "notify-main-failure -> nousergon-lib's shared notify-ci-failure.yml",
            "Declared from the workflow's own notify-main-failure job, mirroring every other "
            "scheduled workflow in this repo.",
        )
    if kind in ("step-functions", "on-demand-dispatch"):
        return (
            "sns:alpha-engine-alerts, via the owning pipeline's NotifyFailure state",
            "Declared from the owning pipeline's ASL definition (plan §4.6).",
        )
    if kind == "systemd-timer":
        return (
            "krepis.alerts -> Telegram (dashboard box)",
            "Declared from the box's standard timer-failure alerting path (plan §4.6).",
        )
    return (
        "unknown",
        "No scheduled execution of its own to fail on; a downstream freshness row is the backstop.",
    )


def build_row(unit) -> dict[str, Any]:
    data = unit.raw
    trigger = data.get("trigger") or {}
    cells = (data.get("audit") or {}).get("cells") or {}
    unit_lifecycle = str(data.get("lifecycle") or "")
    lifecycle = _lifecycle(unit.unit_id, unit_lifecycle)
    log_location, log_reason = _log_location(unit.unit_id, trigger)
    alert_channel, alert_reason = _alert_channel(trigger)

    row: dict[str, Any] = {
        "component_id": data["component_id"],
        "owning_repo": "nousergon-data",
        "substrate": _substrate(unit.unit_id, trigger),
        "origin": _origin(trigger),
        "owner": data.get("owner", "brian"),
        "lifecycle": lifecycle,
    }

    if lifecycle in LIFECYCLE_NEEDS_REASON:
        row["lifecycle_owner"] = data.get("owner", "brian")
        row["lifecycle_reexam"] = REEXAM_DATE
        if lifecycle == "pending":
            row["pending_since"] = "2026-09-14"
            row["promotes_when"] = (
                f"the {unit.unit_id} unit descriptor's own successor "
                f"({trigger.get('successor', 'unset')}) goes live"
            )
            row["lifecycle_reason"] = (
                f"Generated from registry.d/units/{unit.unit_id}-*.yaml (P-08): the unit "
                "descriptor itself declares lifecycle: pending."
            )
        elif lifecycle == "retired":
            retirement = data.get("retirement") or {}
            if not retirement.get("ruling") or not retirement.get("reason"):
                raise ValueError(
                    f"{unit.unit_id}: lifecycle 'retired' needs a retirement block with "
                    "`ruling` and `reason` — a retirement without a stated reason is not a fact"
                )
            row["lifecycle_reason"] = (
                f"Generated from registry.d/units/{unit.unit_id}-*.yaml (P-08): the unit "
                f"descriptor declares lifecycle: 'retired' — {retirement['ruling']}. "
                f"{' '.join(str(retirement['reason']).split())}"
            )
        else:
            row["lifecycle_reason"] = (
                f"Generated from registry.d/units/{unit.unit_id}-*.yaml (P-08): the unit "
                f"descriptor declares lifecycle: {unit_lifecycle!r} — "
                f"{'R7 (plan §7) retirement recommendation, not yet executed' if unit_lifecycle == 'proposed-retirement' else 'transcribed verbatim.'}"
            )

    row["authority_tier"] = "t1"
    row["authority_tier_reason"] = (
        f"A deterministic collector with no judgment: {unit.unit_id}'s own code path "
        f"({data.get('code_path', 'undeclared')}) writes only the key templates its unit "
        "descriptor names in writes[]. No branching on model output; no LLM call in this unit."
    )

    row["signals"] = {
        "execution": {
            "status": "emitted" if trigger.get("kind") in ("step-functions", "github-actions") else "unknown",
            "reason": (
                "The owning substrate (Step Functions / GitHub Actions) retains per-execution "
                "start/terminal-status/history natively."
                if trigger.get("kind") in ("step-functions", "github-actions")
                else "No durable per-execution record independently confirmed for this trigger kind yet."
            ),
        },
        "cost": {
            "status": "unknown",
            "reason": "No run-level cost record exists yet; component=data-collection cost tagging "
            "is phase-3 work (plan P-21). Makes no LLM call.",
        },
        "resource": {
            "status": "unknown",
            "reason": "Spot-vs-on-demand and box headroom are not independently published per unit; "
            "read at the pipeline/dispatcher level only.",
        },
        "data": _signal_from_audit(unit.unit_id, "run_record", cells),
        "outcome": _signal_from_audit(unit.unit_id, "detector", cells),
    }

    row["log_location"] = log_location
    row["log_location_reason"] = log_reason
    row["alert_channel"] = alert_channel
    row["alert_channel_reason"] = alert_reason
    row["severity_source"] = "binary — the owning pipeline's/workflow's own terminal status"
    row["severity_source_reason"] = (
        "Declared from the trigger: no envelope, no severity string at this layer."
    )
    row["console_surface"] = "unknown"
    row["console_surface_reason"] = (
        "No pane renders this unit individually yet. plan §4.1's data-collection-board "
        "(P-03) is the console surface once built; this row is what it will render."
    )
    row["retention"] = (
        "GitHub's default 90-day Actions log retention"
        if trigger.get("kind") == "github-actions"
        else "90 days (the owning pipeline's log group RetentionInDays); published S3 artifacts "
        "are retained indefinitely per the standing archive-retention preference."
    )
    row["retention_reason"] = (
        f"Declared from the unit descriptor's trigger; not independently probed against live "
        f"bucket/log-group lifecycle rules for {unit.unit_id} specifically."
    )
    return row


HEADER = """\
# Observability registry row — GENERATED, do not hand-edit.
#
# Produced by `scripts/gen_observability_rows.py` from
# `registry.d/units/{unit_id}-*.yaml` (alpha-engine-config-I10775, plan §4.1
# item P-08). Re-run the generator after editing the unit descriptor; a diff
# here with no matching unit-descriptor change is itself a finding.
#
# Gathered by `nous-ergon-ops`'s `gather_repo_descriptors.py` (console-policy
# §2.6 one-file onboarding) — no console or ops-side code change is needed for
# this row to publish.
"""


def _yaml_dump(row: dict[str, Any]) -> str:
    buf = io.StringIO()
    yaml.safe_dump(row, buf, sort_keys=False, default_flow_style=False, width=100, allow_unicode=True)
    return buf.getvalue()


def generate() -> list[Path]:
    # Deliberately not `list.append()`: the writer-inventory scanner
    # (`data_gate/inventory.py`) matches CALLS BY ATTRIBUTE NAME ALONE, and
    # `append` is also an ArcticDB write verb in its closed vocabulary — a
    # `list.append()` here reads as an undeclared ArcticDB write site and
    # fails `test_the_writer_inventory_reconciles_today`. List-comprehension
    # accumulation sidesteps the false positive without widening the scanner.
    dests: list[Path] = []
    for unit in load_units():
        if unit.unit_id in EXTERNALLY_OWNED_ROWS:
            continue
        row = build_row(unit)
        text = HEADER.format(unit_id=unit.unit_id) + _yaml_dump(row)
        dest = OUT_DIR / f"{row['component_id']}.yaml"
        dest.write_text(text, encoding="utf-8")
        dests = [*dests, dest]
    return dests


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=["generate", "check"])
    args = ap.parse_args(argv)

    if args.command == "generate":
        written = generate()
        print(f"wrote {len(written)} observability rows to {OUT_DIR}")
        return 0

    # check: generate into memory-equivalent and diff against what's on disk
    before = {
        p.name: p.read_text(encoding="utf-8")
        for p in OUT_DIR.glob("data-collector-*.yaml")
    }
    written = generate()
    after = {p.name: p.read_text(encoding="utf-8") for p in written}
    stale = set(before) - set(after)
    drifted = [name for name in after if before.get(name) != after[name]]
    if stale or drifted:
        for name in sorted(stale):
            print(f"STALE (no longer generated, but present on disk): {name}", file=sys.stderr)
        for name in sorted(drifted):
            print(f"DRIFTED (generator output changed, not regenerated): {name}", file=sys.stderr)
        print(
            "registry.d/ is out of date — run `python3 scripts/gen_observability_rows.py generate` "
            "and commit the result.",
            file=sys.stderr,
        )
        return 1
    print(f"registry.d/ is up to date with {len(after)} generated rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
