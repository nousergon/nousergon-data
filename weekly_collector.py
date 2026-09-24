"""
weekly_collector.py — Centralized weekly data collection for Alpha Engine.

Phase 1 (before research): constituents, prices, macro, universe returns.
Phase 2 (after research): alternative data for promoted tickers.

Phase 1 runs on EC2 via SSM RunCommand (price refresh takes 15-25 min).
Phase 2 runs as Lambda (concurrent ThreadPoolExecutor, ~3-5 min for
900+ tickers; timeout=900s).

Usage:
    python weekly_collector.py --phase 1              # Phase 1 only
    python weekly_collector.py --phase 2              # Phase 2 only
    python weekly_collector.py                        # Phase 1 (default)
    python weekly_collector.py --phase 1 --dry-run    # validate Phase 1
    python weekly_collector.py --phase 1 --only prices # single collector
    python weekly_collector.py --phase 2 --only alternative  # explicit
    python weekly_collector.py --daily                # weekday EOD pass (yfinance OHLCV, no VWAP)
    python weekly_collector.py --daily --dry-run      # validate daily
    python weekly_collector.py --morning-enrich       # morning polygon overwrite (prior trading day)
    python weekly_collector.py --morning-enrich --date 2026-04-23  # backfill specific date
    python weekly_collector.py --daily-heal           # standalone daily data heal (I2717), off the preopen critical path
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ClientError

def _load_dotenv() -> None:
    """Load .env file into os.environ (lightweight, no dependency).

    Defined at module-top so it can run before setup_logging() — local-dev
    workflows put FLOW_DOCTOR_ENABLED + FLOW_DOCTOR_GITHUB_TOKEN in .env,
    and the flow-doctor handler attach reads those at import time.
    Production (Lambda/EC2) gets env from SSM/systemd before Python starts;
    .env is the local-dev fallback.
    """
    env_path = Path(".env")
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            if key and val and key not in os.environ:
                os.environ[key] = val


_load_dotenv()

# Structured logging + flow-doctor singleton via alpha-engine-lib (shared
# pattern across all 5 entrypoints; see executor/main.py for reference).
# Module-top so import-time errors in the collectors block below are also
# captured by flow-doctor's ERROR handler.
from nousergon_lib.logging import setup_logging, guard_entrypoint, get_flow_doctor
from nousergon_lib.phase_registry import PhaseRegistry
from shadow.root import active_root as _active_shadow_root  # I10891: run state under a shadow root
from shadow.run_state import RunStatePhaseRegistry
# Canonical experiment-package config resolver (alpha-engine-config#1157): the
# lift of the five inline _find_config / load_config / config_loader copies into
# the shared-lib chokepoint. load_config below delegates to it.
from nousergon_lib.config import resolve_experiment_config
from nousergon_lib.yfinance_quiet import quiet_yfinance
_FLOW_DOCTOR_EXCLUDE_PATTERNS: list[str] = []
_FLOW_DOCTOR_YAML = str(Path(__file__).parent / "flow-doctor.yaml")
# alpha-engine-config-I10880: a shadow run (`python -m shadow run` sets
# DATA_COLLECTION_SHADOW_TRADING_DAY on the child process before this module
# is imported — shadow/root.py's ENV_TRADING_DAY) reports under a distinct
# flow name so it draws its own rate-limiter budget instead of the
# production "data-collector" flow's. flow_doctor's RateLimiter keys
# max_diagnosed_per_day / max_issues_per_day / max_alerts_per_day on
# flow_name (flow_doctor/core/rate_limiter.py::RateLimiter.check ->
# store.count_actions_today(action, self.flow_name)), so this override alone
# is sufficient to give the shadow workload a separate budget — no change to
# flow-doctor.yaml's rate_limits block, and no import of the shadow module
# (a bare env-var read keeps this file off the excluded shadow/* surface).
# Measured 2026-09-15: a shadow-weekday run filed nousergon-data-I1743/I1752
# under flow=data-collector and burned the production 3/day diagnosis
# budget hours before a live collection run needed it.
_FLOW_NAME = (
    "data-collector-shadow"
    if os.environ.get("DATA_COLLECTION_SHADOW_TRADING_DAY")
    else "data-collector"
)
setup_logging(
    "data-collector",
    flow_doctor_yaml=_FLOW_DOCTOR_YAML,
    exclude_patterns=_FLOW_DOCTOR_EXCLUDE_PATTERNS,
    flow_name=_FLOW_NAME,
)

from collectors import constituents, historical_constituents, prices, macro, universe_returns, signal_returns, alternative, daily_closes, fundamentals, short_interest, metron_market_data, universe_classification, fred_history, technical_rating_ledger
from collectors import CaretTickerError  # I10904 write-site guard
from builders._price_cache_writeboth import (
    assert_valid_price_cache_ticker as _assert_valid_price_cache_ticker,
    price_cache_read_prefixes as _price_cache_read_prefixes,
    price_cache_write_prefixes as _price_cache_write_prefixes,
    write_price_cache_freshness_sentinel as _write_price_cache_freshness_sentinel,
)
from dates import (  # config#1014 trading-day axis; I10893 write guard
    FutureBarError as _FutureBarError,
    assert_no_bar_after as _assert_no_bar_after,
    default_run_date,
)
# alpha-engine-config-I10773 (P-06) / I10785 (P-18): one run manifest per unit
# execution, and the common empty-but-fresh guard before every publish claim.
import run_units
from nousergon_lib import run_manifest
from validators import expectations

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    """Load config.yaml, experiment-package first (config#1042).

    Search order mirrors features/feature_engineer.py::_load_feature_cfg_overrides:
    experiments/$ALPHA_ENGINE_EXPERIMENT_ID/data/config.yaml (default experiment
    ``reference``) first, then the legacy top-level alpha-engine-config/data/config.yaml,
    then the repo-local fallback (``path``). The experiment-package layer was already
    live in feature_engineer; this closes the gap that file's docstring references.

    Delegates to the canonical nousergon-lib resolver (resolve_experiment_config,
    alpha-engine-config#1157). The data ``Path(path)`` tail is preserved verbatim
    via repo_local_fallback (``path`` is CWD-relative, not subdir-anchored).
    """
    resolved = resolve_experiment_config(
        "data",
        "config.yaml",
        repo_root=Path(__file__).parent,
        repo_local_fallback=Path(path),
        resolve=True,
    )
    with open(resolved) as f:
        return yaml.safe_load(f)


def _load_chronic_polygon_gaps(config: dict) -> list[str]:
    """Return the sorted list of chronic-polygon-gap tickers from config.

    Empty list when the config section is missing or malformed: the
    chronic-gap self-heal step then becomes a no-op, preserving the
    pre-PR strict ``polygon_only`` behavior. Adding/removing a ticker
    requires a deliberate edit to ``data/config.yaml`` in the
    alpha-engine-config repo (private), surfaced by drift detection if
    polygon coverage recovers for an entry.
    """
    section = config.get("chronic_polygon_gaps") or {}
    tickers = section.get("tickers") or {}
    if not isinstance(tickers, dict):
        return []
    return sorted(tickers.keys())


# A recurring `status=degraded` collector must page with the SAME actionable
# detail every day: which sub-defect, the issue tracking it, and the
# condition that clears it — without these a human cannot tell today's page
# from yesterday's (alpha-engine-config-I10359). Keyed by collector name;
# extend when a new producer starts reporting `degraded`. Do NOT downgrade
# severity or de-dupe this away while an entry's issue stays open — I7572's
# own non-inferable gotcha forbids weakening the zero-variance guard's
# visibility, and a recurring page for a genuinely still-broken producer
# defect is the correct behavior until the tracked issue actually closes.
_DEGRADED_DEFECT_REGISTRY: dict[str, dict[str, str]] = {
    "features": {
        "tracked_issue": "alpha-engine-config-I7572",
        "clears_when": (
            "every named column shows non-zero cross-sectional std on a live "
            "s3://alpha-engine-research/features/<date>/*.parquet snapshot, "
            "or is removed from features/registry.py::CATALOG + SCHEMA.md §3"
        ),
    },
}


def _write_guard_skip_record(path: str, results: dict) -> None:
    """Record, for the stage launcher, that this run's own guard skipped it.

    alpha-engine-config-I11474: MorningEnrich's stale-overwrite guard returns
    ``status="skipped"`` with a ``skip_reason`` (the run manifest already
    records that as ``not_applicable``), but the launcher's stage-coverage
    assertion could not see it and graded the deliberate skip ``WHOLLY
    STALE``. This file is the hand-off: present, non-empty, and holding the
    guard's reason verbatim iff the guard fired; ABSENT on every other
    outcome — including a leftover from an earlier run, which is removed
    first so a stale record can never excuse a run that did execute.
    """
    target = Path(path)
    target.unlink(missing_ok=True)
    if (results or {}).get("status") != "skipped":
        return
    reason = " ".join(str(results.get("skip_reason") or "").split())
    if not reason:
        # A skip with no stated reason is not a declaration anything may act
        # on: leave the record absent, so coverage grades the run as it would
        # any other, and say so.
        logger.warning(
            "mode reported status=skipped with no skip_reason — no guard-skip record "
            "written; stage coverage will grade this run as a normal one"
        )
        return
    target.write_text(reason + "\n")
    logger.info("guard-skip record written to %s: %s", target, reason)


def _describe_degraded_defects(results: dict) -> str:
    """Build the `Defect detail: ...` clause of the DEGRADED alert.

    Pulled out of ``main()`` so it can be unit-tested without driving the
    whole daily-collect entrypoint (alpha-engine-config-I10359): a recurring
    ``degraded`` page must name, per degraded collector, the offending
    columns, the tracked issue, and the condition that clears it — a human
    (or the flow-doctor notifier that turns this exact string into a GitHub
    issue body) must not have to re-derive that from source on the day it
    pages. A collector missing from ``_DEGRADED_DEFECT_REGISTRY`` reads as
    explicitly ``UNTRACKED``, never silently blended into the tracked shape.
    """
    degraded_names = results.get("degraded_collectors", [])
    lines = []
    for name in degraded_names:
        info = results.get("collectors", {}).get(name, {}) or {}
        cols = sorted({
            *(info.get("zero_variance_columns") or {}).keys(),
            *(info.get("all_null_columns") or []),
        })
        reg = _DEGRADED_DEFECT_REGISTRY.get(name)
        if reg:
            lines.append(
                f"{name}: columns={cols or '?'} "
                f"tracked={reg['tracked_issue']} "
                f"clears_when={reg['clears_when']}"
            )
        else:
            lines.append(
                f"{name}: columns={cols or '?'} tracked=UNTRACKED — file an "
                "alpha-engine-config issue and add a "
                "_DEGRADED_DEFECT_REGISTRY entry for this collector"
            )
    return " | ".join(lines) or "no detail available"


class _CollectorError(RuntimeError):
    """Raised inside a phase block when a collector returns ``status=error`` so the
    phase writes an ``error`` marker (→ a recovery RE-RUNS it) instead of a lying
    ``ok`` marker. Caught at the call site to preserve best-effort-continue."""

    def __init__(self, name: str, detail, result: dict | None = None) -> None:
        super().__init__(f"{name}: {detail}")
        self.detail = detail
        #: The collector's own result dict, when it produced one before failing.
        #: Carried so the FAILURE manifest can still record the guard readings
        #: and metrics the collector graded itself on — `observability-policy`
        #: §3.1: the failure path writes the same telemetry as the success path,
        #: except the completion claim (`alpha-engine-config-I10827`).
        self.result = result or {}


class _DegradedRun(RuntimeError):
    """A phase that produced its artifact with a KNOWN defect in it, OR
    published only PART of what it was asked to.

    `alpha-engine-config-I10784` (P-17). ``degraded`` is a real third outcome
    for the PROCESS — the EOD Step Function reads this process's exit code to
    decide whether to run the ArcticDB append, and a features-only column
    defect must not withhold the day's SPY close (2026-08-17). It is NOT a
    third outcome for the RUN MANIFEST: `data_run_manifest.v1`'s status is
    ``ok`` | ``failed`` | ``not_applicable`` and nothing else, and its own
    contract says a run that produced a partial or defective artifact is
    ``failed``.

    `alpha-engine-config-I11230` reuses this same class for a collector's own
    ``status="partial"`` (e.g. `collectors/prices.py`: N of ~900 tickers
    failed a refresh, the rest wrote fine) — the manifest-vs-process split is
    identical, and the fix is "do not invent a fourth state" (I11230's own
    recommendation).

    So the two claims are separated here rather than conflated. This exception
    is raised from inside the manifest wrapper AFTER the phase's lineage is
    recorded, which makes the manifest say ``failed`` and carry the defect as
    its reason; :func:`_phase_collect` catches it outside the wrapper and
    returns the collector's ORIGINAL ``degraded``/``partial`` result, so the
    aggregator, the alert text and the exit code are byte-for-byte what they
    were.
    """

    def __init__(self, name: str, result: dict) -> None:
        detail = result.get("error") or result.get("detail") or result.get("reason") or "no detail reported"
        kind = "DEGRADED" if result.get("status") == "degraded" else "PARTIAL"
        super().__init__(f"{name} produced a {kind} artifact: {detail}")
        self.name = name
        self.result = result


class _PhaseNotApplicable(run_manifest.NotApplicable):
    """A phase that RAN and correctly published nothing new this cycle.

    `alpha-engine-config-I11011`. A same-date auto-skip used to return
    ``{"status": "ok", "auto_skipped": True}`` and record a manifest reading
    ``status: ok`` with ``rows_out: 0`` and ``outputs: []`` — a run that
    published nothing, indistinguishable on every downstream surface from one
    that published its deliverable. It is a ``not_applicable`` with the lib's
    own closed-list reason for "a target date already published", and it is
    COUNTED.

    Carries the collector's result dict so :func:`_phase_collect` returns
    exactly what it returned before the record existed — the manifest becomes
    honest, the exit code does not move.
    """

    def __init__(self, name: str, result: dict, reason: str, detail: str) -> None:
        super().__init__(reason, detail)
        self.name = name
        self.result = result


class _PhaseNotRun(RuntimeError):
    """A phase that did not run at all, for a reason that is NOT a failure.

    `alpha-engine-config-I10784`. Raised from inside the manifest wrapper by
    :func:`_phase_not_run` so the non-run leaves a record; see that function
    for why the status split is what it is.
    """

    def __init__(self, name: str, reason: str) -> None:
        super().__init__(f"{name} did not run: {reason}")
        self.name = name
        self.reason = reason


#: The closed vocabulary of `status` values a collector dispatched through
#: `_phase_collect` may return from `run_fn()` itself (never the synthetic
#: `{"status": "ok", "auto_skipped": True}` `_phase_body` manufactures for its
#: own registry-level cache-hit — that path never reaches this check).
#:
#: `alpha-engine-config-I11230` deliverable 4. Before this existed,
#: `collectors/prices.py::collect` (and, swept the same day, `alternative.py`,
#: `signal_returns.py`, `fred_history.py::backfill_to_s3`) could each return
#: `status="partial"` and `_phase_body`/`_record_phase_lineage` would branch on
#: neither `"error"` nor `"degraded"` for it — the manifest wrapper completed
#: normally and recorded `status: ok`, exactly as if every ticker/step had
#: succeeded. Measured 2026-09-21: the 2026-09-18 shadow replay's D03 manifest
#: read `ok` with 4 of 930 price_cache parquets silently absent
#: (`alpha-engine-config-I11203`).
#:
#: Kept here rather than per-collector so a NEW status value introduced by any
#: collector fails loud at this one choke point (`_CollectorError`, an
#: "error" marker) instead of silently completing as `ok` the way `"partial"`
#: did. Grows only by adding a member here AND deciding, in the same PR, what
#: `_phase_body`/`_record_phase_lineage` do with it -- never by defaulting an
#: unrecognized value to a completion claim.
_KNOWN_COLLECTOR_STATUSES = frozenset({
    "ok",          # clean: published (or nothing needed publishing)
    "ok_dry_run",  # dry-run: identified work, wrote nothing
    "error",       # producer-detected failure -> _CollectorError, below
    "degraded",    # artifact published, a KNOWN defect is in it (I7572)
    "partial",     # SOME of the unit's work failed; the rest published --
                   # given the SAME manifest treatment as "degraded" (I11230):
                   # raised as _DegradedRun in _record_phase_lineage so the
                   # per-unit manifest reads "failed" naming the loss, while
                   # the PROCESS posture (the collector's own returned dict,
                   # the aggregate status, the exit code) is unchanged.
    "skipped",     # the collector itself declined to run this cycle (e.g. an
                   # empty universe) -- falls through to record_empty_production
                   # below when nothing was published, exactly like any other
                   # non-run that recorded no output.
})


#: The manifest context of the whole-mode unit currently executing, if any.
#:
#: `alpha-engine-config-I10784`: the two heal modes (D33/D34) contain DECLARED
#: best-effort steps whose failure is deliberately non-fatal — a hung heal must
#: not fail the pipeline, and the loud gate is Saturday's postflight. Those
#: swallows stay (each carries its own inline rationale at its site), but a
#: swallow that leaves no trace is indistinguishable from a step that stopped
#: running. This lets :func:`_record_swallowed_step` fold each one onto the
#: mode's own manifest as a ``failed`` guard reading, so the exit code does not
#: move and the step is COUNTED. A ContextVar rather than a parameter because
#: the swallow sites are ~8 frames below the wrapper and threading a context
#: through them is how the next one added gets forgotten.
_CURRENT_RUN_CTX: ContextVar = ContextVar("_CURRENT_RUN_CTX", default=None)


def _record_swallowed_step(step: str, detail: str, *, unit_id: str | None = None) -> None:
    """Record a DECLARED best-effort step's failure on the current run manifest.

    No-op outside a whole-mode unit (a dry run, or a direct call in a test):
    there is no manifest to fold onto, and the caller's own log line is the
    record. Never raises — this is the transparency surface for a swallow, and
    it must not itself become a new way for the swallow to become fatal.
    """
    ctx = _CURRENT_RUN_CTX.get()
    if ctx is None:
        return
    # `unmeasurable` rather than a new verdict: `data_run_manifest.v1`'s
    # GuardVerdict enum is closed to (ok, empty_fresh, below_floor,
    # unmeasurable, not_applicable), and a step that aborted contributed a
    # quantity nobody measured. Red and counted, which is the property that
    # matters — never a pass.
    ctx.record_guard(
        "best_effort_step",
        mode="observe",
        verdict="unmeasurable",
        detail=(
            f"{step} failed and was swallowed by a DECLARED best-effort posture "
            f"(the loud gate for this class is the postflight, not this step): {detail}"
        )[:2000],
        key=unit_id,
    )


def _build_registry(config: dict, args: argparse.Namespace, date: str) -> "PhaseRegistry | None":
    """Construct a per-date :class:`PhaseRegistry` for marker-based skip/resume +
    watchdog (L4528 — data is the 2nd consumer of the lib phase framework, after
    the backtester), or ``None`` in dry-run.

    Dry-run returns None so a validation pass never writes markers — a dry-run
    marker would claim ``ok`` while writing no artifact, poisoning a later real
    run's auto-skip decision. ``--only <collector>`` and ``--force`` force every
    phase to RUN (the operator explicitly asked for that work) while still writing
    markers. Per-phase hard caps come from the optional ``full_run_hard_caps_seconds``
    config block (absent → watchdog off, no behavior change).
    """
    if args.dry_run:
        return None
    _csv = lambda s: [p.strip() for p in (s or "").split(",") if p.strip()]
    reg = RunStatePhaseRegistry(
        date=date,
        bucket=config["bucket"],
        marker_prefix="data",
        skip_phases=_csv(getattr(args, "skip_phases", "")),
        force=bool(getattr(args, "force", False)) or (getattr(args, "only", None) is not None),
        force_phases=_csv(getattr(args, "force_phases", "")),
        hard_caps=config.get("full_run_hard_caps_seconds") or {},
    )
    # alpha-engine-config-I10773: which run mode this is, resolved from the SAME
    # args `run_weekly` dispatches on, and carried on the registry every phase
    # already receives. Phase names are not unique across modes (`prices` and
    # `features` each appear in two), so the mode is half the key that resolves
    # a phase to its audit unit — see `run_units.PHASE_UNITS`. Derived once
    # here rather than threaded through 29 call sites, and derived from the
    # dispatch inputs rather than declared a second time, so it cannot disagree
    # with the mode that actually ran.
    reg.data_mode = _resolve_run_mode(args)
    return reg


def _resolve_run_mode(args: argparse.Namespace) -> str:
    """The run mode, by the same ladder ``run_weekly`` dispatches on."""
    for flag in (
        "morning_enrich",
        "morning_arctic_append",
        "daily_arctic_append",
        "chronic_gap_heal",
        "daily_heal",
    ):
        if getattr(args, flag, False):
            return flag
    if getattr(args, "daily", False):
        return "daily"
    return f"phase{getattr(args, 'phase', None) or 1}"


def _s3_object_exists(bucket: str, key: str) -> bool:
    """HEAD-check an S3 object. Any non-404/NoSuchKey ClientError (IAM drift,
    throttling, etc.) RAISES — fail-loud per feedback_no_silent_fails; a
    verification check that silently treats "couldn't check" as "doesn't
    exist" would produce nondeterministic spurious failures, and treating it
    as "exists" would defeat the whole point of verify-by-artifact."""
    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            return False
        raise


def _rows_out(result: dict, rows_key: str | None) -> int | None:
    """The row count this collector reported, or ``None`` when it reports none.

    ``None`` is never coerced to 0: "the collector did not count" and "the
    collector counted zero" are different facts with opposite consequences, and
    the empty-but-fresh objective (plan §2 row 6) is computed from this number.
    A missing key read as zero would mark every such unit an empty write; read
    as fine, it would mark none of them. The guard records UNMEASURABLE instead.
    """
    if not rows_key:
        return None
    value = result.get(rows_key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


#: Severity a `GuardReading.verdict` carries onto the board's single per-run
#: metric (`_worst_reading`, below). Matches `expectations.verdict_metric`'s
#: status mapping: RED outranks the missing-input state, which outranks clean.
_EMPTY_FRESH_SEVERITY: dict[str, int] = {
    "ok": 0,
    "not_applicable": 0,
    "unmeasurable": 1,
    "below_floor": 2,
    "empty_fresh": 2,
}


def _worst_reading(readings: list["expectations.GuardReading"]) -> "expectations.GuardReading":
    """The one verdict a unit's single board metric renders, from every key
    graded this run (`alpha-engine-config-I10785`).

    Never the first key checked, and never an average — the worst, so a
    broken second or third published key cannot hide behind a clean first
    one. Within the clean tier, `ok` is preferred over `not_applicable`: a
    unit that actually measured something reports what it measured rather
    than a shrug from an earlier, less-informative key.
    """
    def _rank(reading: "expectations.GuardReading") -> tuple[int, int]:
        return (
            _EMPTY_FRESH_SEVERITY.get(reading.verdict, 2),
            1 if reading.verdict == "ok" else 0,
        )

    return max(readings, key=_rank)


#: The real feature-store groups `features/writer.py::write_feature_snapshot` can
#: write (`features.registry.GROUPS` — "factor_loading" is NOT one: it is an
#: ArcticDB-materialized column set (features/cross_sectional.py), never a
#: standalone `features/{date}/factor_loading.parquet` key; D12's descriptor
#: entry naming it was corrected alongside this list, alpha-engine-config-I10855).
_FEATURE_GROUPS: tuple[str, ...] = ("technical", "fundamental", "interaction", "macro", "alternative")


def _feature_group_extra_outputs(run_date: str) -> tuple[tuple[str, object, object], ...]:
    """One ``(key, present_fn, rows_fn)`` per feature-store group, read from
    ``compute_and_write``'s own ``groups_written`` (threaded straight from
    ``write_feature_snapshot``'s return — features/writer.py). A group absent
    from ``groups_written`` had no columns available this run (writer.py:60) and
    is correctly NOT recorded — never a copy of the descriptor's 5-entry list
    regardless of what actually landed on S3 (alpha-engine-config-I10855)."""
    def _present(group: str):
        return lambda r: group in (r.get("groups_written") or {})

    def _rows(group: str):
        return lambda r: (r.get("groups_written") or {}).get(group) or 0

    return tuple(
        (f"features/{run_date}/{group}.parquet", _present(group), _rows(group))
        for group in _FEATURE_GROUPS
    )


def _prices_extra_outputs(s3_prefix: str) -> tuple[tuple[object, object, object], ...]:
    """The per-ticker price-cache parquet keys ``collectors/prices.py::collect``
    actually uploaded THIS run, each with its own row count
    (alpha-engine-config-I11026 — D03's manifest recorded ``rows_out: 0``,
    ``outputs: []`` on every run, including a measured 3m38s run that wrote
    real data, because the phase declares no single stable ``artifact_key``
    — per-symbol writes have no fixed key a descriptor could name ahead of
    the run). Read from ``result["written"]`` via
    :func:`collectors.prices.written_keys` — what the run actually published
    — never from the requested ticker population or the descriptor's
    (nonexistent) declared list: a ticker that was never stale, or that
    failed the refresh, never appears. ``extra_key`` is the callable form
    because the key SET is per-run; ``rows_fn`` returns the
    ``{key: rows}`` mapping so each key is graded on ITS OWN count, not the
    batch's aggregate ``refreshed``."""
    def _keys(r: dict) -> list[str]:
        return list(prices.written_keys(r, s3_prefix))

    def _rows(r: dict) -> dict[str, int]:
        return prices.written_keys(r, s3_prefix)

    return (
        (_keys, lambda r: bool(r.get("written")), _rows),
    )


def _run_manifest_context(reg: "PhaseRegistry", unit: run_units.PhaseUnit) -> dict:
    """The per-run manifest arguments shared by every wrapped call site here."""
    return {
        "sink": run_units.manifest_sink(reg.bucket, reg.s3_client),
        "trigger": run_units.resolve_trigger("scheduled"),
        "trading_day": reg.date,
        "log_location": run_units.resolve_log_location(),
    }


def _phase_collect(
    reg: "PhaseRegistry | None",
    name: str,
    run_fn,
    *,
    artifact_key: str | None = None,
    extra_outputs: tuple[tuple[str, object, object], ...] = (),
    supports_auto_skip: bool = True,
    verify_artifact_exists: bool = False,
    bucket: str | None = None,
) -> dict:
    """Run a collector under the phase registry (markers + L4524 artifact-validated
    auto-skip + watchdog), preserving the module's best-effort-continue posture.

    - ``reg is None`` (dry-run): run ``run_fn`` directly, no markers.
    - auto-skip (prior ``ok`` marker AND its recorded artifact still on S3): return an
      ``ok`` cache-hit dict WITHOUT recomputing. Recorded as ``ok`` — not ``skipped`` —
      because the module's status aggregator fails the run on any non-``ok`` collector,
      and a resumed phase is a success, not a failure.
    - success: ``record_artifact(artifact_key)`` so the next run's L4524 checkpoint can
      verify the output still exists (a marker whose artifact vanished re-runs).
    - collector error / raise: write an ``error`` marker (via ``_CollectorError``) and
      return an error dict so the loop continues AND main()'s aggregation still exits 1.

    ``supports_auto_skip=False`` (multi-file / shared-DB / ArcticDB producers with no
    single stable S3 key) → markers + watchdog only, the phase always runs.

    ``extra_outputs`` (alpha-engine-config-I10855): a unit whose descriptor declares
    MORE than one published key records every one of them, never just ``artifact_key``.
    Each entry is ``(key, present_fn, rows_fn)`` — both callables take the collector's
    own ``result`` dict and are evaluated at record time (never before: the write may be
    conditional). ``present_fn(result)`` is truthy only when THIS run actually wrote
    ``key`` — recording a key the collector did not write would be the same defect this
    deliverable exists to close, just moved one level up. ``rows_fn(result)`` returns the
    ``rows_out`` this run measured for that key; never a placeholder 0 standing in for an
    uncounted write. Recorded under the SAME gating as ``artifact_key`` (skipped on
    auto-skip / dry-run) so a cache-hit or dry run never claims a fresh write.

    ``verify_artifact_exists=True`` (config-I2702 deliverable #2, "verify-by-artifact
    workloads"): after a collector reports ``status="ok"``, HEAD-check that
    ``artifact_key`` actually exists on S3 BEFORE recording success — a collector's own
    internal ``status="ok"`` reflects "the fetch/write call didn't raise", not "the
    contracted output is queryable". A missing artifact downgrades the phase to
    ``error``, which _CollectorError already routes to main()'s existing
    ``results["status"] not in ("ok", "skipped") -> SystemExit(1)`` contract — no new
    exit-code plumbing needed, just a stricter success predicate. Skipped for
    ``status="ok_dry_run"`` (dry-run collectors write nothing real to verify) and for
    auto-skip cache hits (the L4524 auto-skip check already re-verified the artifact's
    presence via its own S3 probe before returning the cache hit).
    """
    if reg is None:
        try:
            return run_fn()
        except Exception as e:  # best-effort: record + continue (mirrors prior try/except)
            logger.error("%s failed: %s", name, e)
            return {"status": "error", "error": str(e)}

    # alpha-engine-config-I10773 (P-06) + I10785 (P-18): every execution of
    # every unit writes one `data_run_manifest.v1` record, and every published
    # key passes the empty-but-fresh guard. Both hang off THIS function because
    # it is the one path every scheduled collector already passes through —
    # wiring them per call site is how a unit ends up unobserved on the one day
    # someone adds a collector in a hurry.
    unit = run_units.unit_for(reg.data_mode, name)
    captured: dict = {}

    def _body(run_ctx) -> dict:
        try:
            result = _phase_body(
                reg,
                name,
                run_fn,
                artifact_key=artifact_key,
                supports_auto_skip=supports_auto_skip,
                verify_artifact_exists=verify_artifact_exists,
                bucket=bucket,
            )
        except _CollectorError as ce:
            # The manifest is about to be written with `status: failed`. Fold on
            # whatever the collector graded itself before it failed, so the
            # failure record carries its verdicts rather than only its cause
            # (`alpha-engine-config-I10827`).
            _record_collector_guards(run_ctx, ce.result)
            raise
        captured["result"] = result
        _record_phase_lineage(
            run_ctx, unit, name, result, artifact_key, bucket or reg.bucket, reg,
            extra_outputs=extra_outputs,
        )
        return result

    try:
        run_result = run_manifest.run_unit(
            unit.unit_id, _body, **_run_manifest_context(reg, unit)
        )
        # `run_unit` does not re-raise `NotApplicable` — it records the declared
        # non-production and returns `value=None`. The caller's contract is the
        # collector's own result dict, so the captured one is returned rather
        # than the wrapper's None (the same restoration `_run_whole_mode_unit`
        # makes, for the same reason).
        if run_result.status == "not_applicable":
            return captured["result"]
        return run_result.value
    except run_units.EmptyProduction:
        # `alpha-engine-config-I11011`. The manifest is already durable with
        # `status: failed` and the empty production as its reason. The PROCESS
        # posture is unchanged: the collector's own result dict is returned, so
        # a cache-hit or a genuinely empty phase moves no exit code — only the
        # record it leaves behind.
        return captured["result"]
    except _PhaseNotApplicable:
        # Unreachable in practice: `run_unit` catches `NotApplicable` itself.
        # Kept so a future change to that contract cannot turn a declared
        # non-production into an unhandled traceback.
        return captured["result"]
    except _DegradedRun as dr:
        # The manifest is already durable with `status: failed` and the defect
        # as its reason. The PROCESS posture is unchanged: the collector's own
        # `degraded` result is returned, the aggregator still renders the run
        # `degraded`, and main() still exits 0 so the EOD SF runs the ArcticDB
        # append. See `_DegradedRun` for why the two claims are separated.
        return dr.result
    except _CollectorError as ce:
        return {"status": "error", "error": ce.detail}
    except Exception as e:
        logger.error("%s phase failed: %s", name, e)
        return {"status": "error", "error": str(e)}


def _phase_not_run(
    reg: "PhaseRegistry | None",
    name: str,
    *,
    reason: str,
    applicable: bool,
) -> dict:
    """Record a phase that did NOT run, and return the caller's status dict.

    `alpha-engine-config-I10784` (P-17). Before this existed, every one of these
    call sites assigned a status dict straight into ``results["collectors"]``,
    which meant the unit's non-run was invisible: no manifest, nothing under
    ``data_collection/runs/<unit>/<day>/``, and therefore indistinguishable on
    every downstream surface from a unit that has silently stopped being
    scheduled at all. A non-run that leaves no record is the exact defect the
    run-record objective exists to end.

    The two cases are genuinely different and are recorded differently:

    * ``applicable=False`` — the unit was correctly asked to do nothing this
      cycle (a collector switched off in ``config.yaml``). Manifest status
      ``not_applicable``, with the closed-list reason
      :data:`run_units.NOT_RUN_DISABLED_BY_DECLARATION`. The returned dict keeps
      the ``{"status": "ok", "skipped": ...}`` shape the aggregator already
      treats as a clean run, so the exit code does not move.
    * ``applicable=True`` — the unit SHOULD have run and could not, because an
      upstream input it needs is missing (no tickers, no research.db).
      Manifest status ``failed``, naming the missing input. The returned dict
      keeps ``{"status": "skipped", ...}``, which the aggregator already routes
      to ``results["status"] = "failed"`` and main() to ``SystemExit(1)`` — so
      this is a new RECORD of an existing non-zero exit, not a new exit path.

    ``reg is None`` (dry run) writes nothing, exactly as ``_phase_collect``
    does: a dry run must not leave a manifest claiming a unit ran.
    """
    if not applicable:
        payload = {"status": "ok", "skipped": reason}
    else:
        payload = {"status": "skipped", "reason": reason}
    if reg is None:
        return payload

    unit = run_units.unit_for(reg.data_mode, name)

    def _body(run_ctx) -> dict:
        run_ctx.record_guard(
            expectations.EMPTY_FRESH_GUARD.name,
            mode=expectations.EMPTY_FRESH_GUARD.mode.value,
            verdict="not_applicable",
            detail=(
                f"{unit.unit_id} published nothing on this run ({reason}); there is no new "
                "write for the guard to grade"
            ),
        )
        if applicable:
            raise _PhaseNotRun(name, reason)
        raise run_manifest.NotApplicable(run_units.NOT_RUN_DISABLED_BY_DECLARATION, reason)

    try:
        run_manifest.run_unit(unit.unit_id, _body, **_run_manifest_context(reg, unit))
    except _PhaseNotRun:
        # The manifest is durable with `status: failed` naming the missing
        # input. The caller's dict is returned unchanged so the aggregator and
        # the exit code behave exactly as they did before the record existed.
        pass
    return payload


def _record_phase_lineage(
    run_ctx,
    unit: run_units.PhaseUnit,
    name: str,
    result: dict,
    artifact_key: str | None,
    bucket: str | None,
    reg: "PhaseRegistry",
    *,
    extra_outputs: tuple[tuple[str, object, object], ...] = (),
) -> None:
    """Fold one phase's outcome onto its run manifest, and grade its publish.

    Called on the SUCCESS path only: a phase that raised never reaches here, and
    its manifest is written by the wrapper with `status: failed` and no output
    — which is the completion claim the failure path must not make
    (`observability-policy` §3.1).
    """
    rows = _rows_out(result, unit.rows_key)
    auto_skipped = bool(result.get("auto_skipped"))
    dry = result.get("status") == "ok_dry_run"

    # `extra_written` collects every extra key this run actually wrote, paired
    # with the row count just recorded for it — the exact number the
    # empty-but-fresh guard below grades each one against
    # (`alpha-engine-config-I10785`: "every published PUT", not only the
    # primary `artifact_key`).
    extra_written: list[tuple[str, int]] = []
    if artifact_key and not auto_skipped and not dry:
        run_ctx.record_output(artifact_key, rows_out=rows if rows is not None else 0)
    # alpha-engine-config-I10855: a unit that publishes MORE than one declared key
    # records every one it actually wrote THIS run — never copied from the
    # descriptor (`present_fn`), and never a 0 standing in for an uncounted write
    # (`rows_fn`). Same auto-skip/dry-run gating as the primary artifact_key: a
    # cache-hit or dry run wrote nothing new, so it claims nothing new.
    if not auto_skipped and not dry:
        for extra_key, present_fn, rows_fn in extra_outputs:
            if present_fn(result):
                # `extra_key` is either a literal key, or a ``result -> list[str]``
                # callable for a unit whose per-run key set is only known from
                # what it actually wrote (e.g. one key per currency/symbol) —
                # never a fixed count guessed ahead of the run.
                keys = extra_key(result) if callable(extra_key) else (extra_key,)
                # `rows_fn(result)` is either a single count (applied to every
                # key this call site declares — the I10855 shape, unchanged)
                # or a ``{key: rows}`` mapping for a unit whose keys carry
                # DIFFERENT counts each (e.g. one price-cache parquet per
                # ticker) — alpha-engine-config-I11026. A key absent from the
                # mapping records 0 rather than raising: `extra_key` is the
                # single source of what was published, so a mapping that
                # under-reports a key `extra_key` names is a producer bug to
                # surface via the empty-fresh guard below, not to hide here.
                rows_val = rows_fn(result)
                per_key_rows = rows_val if isinstance(rows_val, dict) else None
                default_rows = 0 if per_key_rows is not None else int(rows_val or 0)
                for k in keys:
                    rows_out = int(per_key_rows.get(k, 0) or 0) if per_key_rows is not None else default_rows
                    run_ctx.record_output(k, rows_out=rows_out)
                    extra_written.append((k, rows_out))
    if not auto_skipped and not dry:
        _record_rejections(run_ctx, result, unit.rejected_keys)
    _record_collector_guards(run_ctx, result)

    if auto_skipped or dry:
        readings = [
            expectations.GuardReading(
                "not_applicable",
                f"{unit.unit_id} published nothing on this run "
                f"({'same-date auto-skip' if auto_skipped else 'dry run'}); there is no new "
                "write for the guard to grade",
                key=artifact_key,
            )
        ]
    else:
        readings = [
            expectations.check_empty_fresh(
                unit_id=unit.unit_id,
                artifact_key=artifact_key,
                bucket=bucket,
                s3_client=reg.s3_client,
                rows_out=rows,
                rows_key=unit.rows_key,
            )
        ]
        # `alpha-engine-config-I10785` (P-18): "every published key passes a
        # non-empty + floor check before the PUT" — a unit recording extra
        # outputs (`I10855`) publishes real keys this guard would otherwise
        # never look at. Each is graded on its OWN row count (the same number
        # `run_ctx.record_output` above was just given), never the primary
        # key's count standing in for a key it did not measure.
        for extra_key_name, extra_rows in extra_written:
            readings.append(
                expectations.check_empty_fresh(
                    unit_id=unit.unit_id,
                    artifact_key=extra_key_name,
                    bucket=bucket,
                    s3_client=reg.s3_client,
                    rows_out=extra_rows,
                )
            )

    for reading in readings:
        expectations.report(reading, unit_id=unit.unit_id)
        run_ctx.record_guard(
            expectations.EMPTY_FRESH_GUARD.name,
            mode=expectations.EMPTY_FRESH_GUARD.mode.value,
            verdict=reading.verdict,
            detail=reading.detail,
            key=reading.key,
            value=reading.value,
            baseline=reading.baseline,
        )

    # The board's `data.<unit>.guard.empty_fresh` clause reads ONE MetricRecord
    # per run (`data_gate/clauses.py`, sibling-owned) — every graded key rides
    # its own `guards[]` entry above for diagnosis, but the single board row is
    # the WORST of them, never just the first, so a unit with N published keys
    # cannot read clean on the strength of only its first key being checked.
    worst = _worst_reading(readings)
    run_ctx.record_metric(
        expectations.verdict_metric(
            unit.unit_id, worst, source_path=f"weekly_collector.py::_phase_collect[{name}]"
        )
    )
    # OBSERVE mode (`sf-pipeline-policy` §7a): the verdict is logged at ERROR and
    # rides on the manifest and the board, and the exit code does not move. The
    # promotion criterion lives in `validators/expectations.py`.
    if expectations.EMPTY_FRESH_GUARD.enforcing and not worst.clean:
        raise _CollectorError(name, f"empty_fresh guard: {worst.detail}")

    # `alpha-engine-config-I10784` (P-17): the manifest has no third
    # ok-but-degraded state. Raised LAST, after every fact above is on the
    # record, so the failure manifest carries the output, the guard reading and
    # the metric that explain it — `observability-policy` §3.1: the failure path
    # writes the same telemetry as the success path, except the completion
    # claim.
    #
    # `alpha-engine-config-I11230`: "partial" (SOME of the unit's work failed,
    # the rest published) gets the SAME treatment as "degraded" here, for the
    # same reason `_DegradedRun`'s own docstring gives — the manifest's
    # contract has no room for a fourth status, and a run that lost part of
    # its output is exactly what "produced its artifact with a KNOWN defect in
    # it" means. `_phase_collect` catches `_DegradedRun` and returns the
    # collector's ORIGINAL "partial" result unchanged, so the aggregate status
    # and exit code this run already produces are untouched — only the
    # PER-UNIT manifest moves, from a false `ok` to `failed` naming the loss.
    if result.get("status") in ("degraded", "partial"):
        raise _DegradedRun(name, result)

    # `alpha-engine-config-I11011`: a phase that completed having published
    # NOTHING does not record `ok`. Two shapes reach here, and both are raised
    # from inside the manifest wrapper and caught outside it, so the record
    # moves and the exit code does not:
    #
    # * a same-date auto-skip, whose output was already published earlier today
    #   — `not_applicable` with the closed-list reason the lib defines as "a
    #   target date already published", never `ok`. It is COUNTED, which is the
    #   property that matters: a unit answering not-applicable every cycle is a
    #   unit that has stopped working, and the board can see that only because
    #   the non-run left a record that says what it was.
    # * a run that was not skipped and still recorded no output at all — the
    #   ten units of the 2026-09-15 shadow run. `failed` unless the descriptor
    #   declares empty-is-valid.
    #
    # A DRY run is exempt: it wrote nothing because it was asked to write
    # nothing, and `reg is None` already keeps a dry run from writing a manifest
    # at all on the paths that have one.
    if dry:
        return
    if auto_skipped:
        raise _PhaseNotApplicable(
            name,
            result,
            run_units.NOT_RUN_NO_NEW_DATA_DECLARED,
            f"{unit.unit_id} auto-skipped: its output for this date was already published "
            f"({result.get('skip_reason')})",
        )
    if not run_ctx.outputs and not int(run_ctx.rows_out or 0):
        run_units.record_empty_production(
            run_ctx,
            unit.unit_id,
            detail=(
                f"phase {name!r} reported status={result.get('status')!r} and recorded no "
                f"published output"
            ),
        )


def _record_collector_guards(run_ctx, result: dict) -> None:
    """Fold guard readings a COLLECTOR graded itself onto its run manifest.

    `alpha-engine-config-I10827`. Most guards on this board are evaluated here,
    from outside the collector, because most of them only need the published
    key and a row count. The cardinality guard is different: grading
    ``covered / (denominator - declared exclusions)`` needs the SYMBOLS, and by
    the time a result dict reaches this function the symbols are gone — only
    their count survived. So the collector grades itself and returns the
    reading, and this folds it on.

    Generic on purpose rather than special-cased to D20: a collector that
    returns ``guards``/``metrics`` gets them recorded, which is what makes the
    next such guard a change in ONE collector rather than a change here too.
    A malformed entry RAISES through ``record_guard``'s own validation — the
    manifest's guard vocabulary is closed, and a reading nobody can render is a
    finding, not something to drop on the floor.
    """
    for entry in result.get("guards") or ():
        run_ctx.record_guard(
            entry["guard"],
            mode=entry["mode"],
            verdict=entry["verdict"],
            detail=entry["detail"],
            key=entry.get("key"),
            value=entry.get("value"),
            baseline=entry.get("baseline"),
        )
    for metric in result.get("metrics") or ():
        run_ctx.record_metric(metric)


def _record_rejections(run_ctx, result: dict, pairs: tuple[tuple[str, str], ...]) -> None:
    """Fold a collector's own not-published counts onto its manifest, by reason.

    `alpha-engine-config-I10810` deliverable 2. A count that is absent, is not a
    number, or is zero contributes nothing — ``UnitRun.reject`` refuses a
    non-positive count on purpose, and a rejection class with no members is not
    a rejection.

    A key the collector RENAMES would drop out silently here — the known
    weakness of every declared-key table, and the reason the alternative
    (searching the result for something that looks like a count) is worse: it
    invents numbers. The backstop is
    ``tests/test_whole_mode_row_counts.py::test_every_declared_row_and_rejection
    _key_exists_in_its_writer``, which pins each declared key against the source
    of the writer that reports it.
    """
    for key, reason in pairs:
        value = result.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if int(value) > 0:
            run_ctx.reject(reason, int(value))


def _phase_body(
    reg: "PhaseRegistry",
    name: str,
    run_fn,
    *,
    artifact_key: str | None,
    supports_auto_skip: bool,
    verify_artifact_exists: bool,
    bucket: str | None,
) -> dict:
    """The phase-registry half of :func:`_phase_collect`.

    Split out so it RAISES rather than returning an error dict: the run-manifest
    wrapper needs the exception to write `status: failed`, and
    :func:`_phase_collect` restores the module's best-effort-continue posture
    outside it. Returning an error dict from inside the wrapper would file every
    collector failure as a successful run.
    """
    with reg.phase(name, supports_auto_skip=supports_auto_skip) as ctx:
        if ctx.skipped:
            logger.info(
                "%s: auto-skip (%s) — output already on S3 this date", name, ctx.skip_reason
            )
            if _active_shadow_root() is not None:  # I10891: a shadow skip is never `ok`
                raise _CollectorError(name, f"auto-skipped under shadow ({ctx.skip_reason}); "
                                      "published nothing on this run — clear the shadow prefix "
                                      "or --force-phases, a skipped shadow unit is not evidence")
            return {"status": "ok", "auto_skipped": True, "skip_reason": ctx.skip_reason}
        result = run_fn() or {}
        # alpha-engine-config-I11230 deliverable 4: the status vocabulary this
        # function branches on below is closed. A collector returning anything
        # outside it (a typo, a new status nobody wired here yet) must fail
        # loud rather than fall through every branch below and reach the
        # manifest wrapper's success path uncontested — which is exactly how
        # "partial" reached an `ok` D03 manifest before this existed.
        _status = result.get("status")
        if _status not in _KNOWN_COLLECTOR_STATUSES:
            raise _CollectorError(
                name,
                f"{name} returned status={_status!r}, outside the closed vocabulary "
                f"_phase_collect branches on ({sorted(_KNOWN_COLLECTOR_STATUSES)}). "
                "Classify it explicitly at _KNOWN_COLLECTOR_STATUSES (and decide what "
                "_phase_body/_record_phase_lineage do with it) rather than letting an "
                "unrecognized status fall through to a completion claim "
                "(alpha-engine-config-I11230).",
                result,
            )
        if _status == "error":
            raise _CollectorError(name, result.get("error"), result)
        # `degraded` is verified exactly like `ok` (alpha-engine-config-I7572):
        # its whole meaning is "the artifact WAS produced, and something in it
        # is known-defective". Verifying only `ok` would let the one status
        # that continues the pipeline skip the verify-by-artifact contract —
        # i.e. the new non-fatal path would be the only unverified one.
        if verify_artifact_exists and artifact_key and result.get("status") in ("ok", "degraded"):
            if bucket is None:
                raise _CollectorError(
                    name, "verify_artifact_exists=True but bucket was not supplied to _phase_collect"
                )
            if not _s3_object_exists(bucket, artifact_key):
                raise _CollectorError(
                    name,
                    f"collector reported status={result.get('status')} but its contracted artifact "
                    f"s3://{bucket}/{artifact_key} does not exist (verify-by-artifact, "
                    f"config-I2702 deliverable #2 — rc=0 must mean the artifact exists, "
                    f"never just 'the process did not crash')",
                )
        # `degraded` is deliberately NOT recorded as a completed artifact.
        # record_artifact is what arms same-date auto-skip, and a rerun that
        # skips a degraded phase returns `{"status": "ok", "auto_skipped":
        # True}` — the degradation would vanish on the second run, which is a
        # false green on exactly the day someone is rerunning to look at it.
        # The cost is that a same-date rerun recomputes the snapshot; that is
        # the right trade against losing the verdict.
        if artifact_key and result.get("status") in ("ok", "ok_dry_run"):
            ctx.record_artifact(artifact_key)
        return result


def _maybe_phase(reg: "PhaseRegistry | None", name: str, **log_ctx):
    """Marker-only phase wrapper (watchdog + START/END marker, no auto-skip) for
    steps whose body manages its own hard-fail/best-effort posture inline —
    MorningEnrich's preflight/append/self-heal. Returns the registry's phase
    context manager, or :func:`contextlib.nullcontext` in dry-run (reg is None).
    The yielded value is unused by these call sites (no ``ctx.skipped`` / no
    ``record_artifact``)."""
    if reg is None:
        return nullcontext()
    return reg.phase(name, supports_auto_skip=False, **log_ctx)


def _run_whole_mode_unit(mode: str, fn, config: dict, args: argparse.Namespace) -> dict:
    """Run a whole-mode unit under the run-manifest wrapper.

    Five of this module's run modes ARE one audit unit end to end — morning
    enrich (D17), morning and post-market ArcticDB append (D18/D32), daily heal
    (D33), chronic-gap heal (D34). They do not go through ``_phase_collect``,
    so they are wrapped here, at the single dispatch, rather than inside each
    body: one place, and a mode added without a manifest fails loudly on the
    ``MODE_UNITS`` lookup instead of running unobserved.

    ``rows_out`` comes from :data:`run_units.MODE_ROWS`, which names the step
    inside each mode's own result that published and the key it reports its
    count under (`alpha-engine-config-I10810` deliverable 2). A mode with no
    entry there, or whose named step did not report a number, records an
    ``unmeasurable`` guard verdict rather than 0 — red, counted, and with an
    address (plan §4.5).

    A mode returning ``status="skipped"`` is a NON-RUN, not a clean run: the
    manifest says ``not_applicable`` with the closed-list reason, never ``ok``
    (`alpha-engine-config-I10784`).

    ``skip_reason`` is matched explicitly against a known producer, never
    guess-bucketed (`alpha-engine-config-I10831` deliverable 1, corrected
    2026-09-15). Today the ONLY producer among the five wrapped modes is
    :func:`_should_skip_morning_enrich`'s ``stale_overwrite`` reason —
    MorningEnrich's own freshness guard, skipping because its target date is
    already appended to ArcticDB. That is
    :data:`run_units.NOT_RUN_NO_NEW_DATA_DECLARED`: `nousergon_lib.run_manifest`
    defines the member as "an upstream explicitly declared there is nothing
    new for THIS run to collect ... a target date already published" — the
    lib's own example, verbatim. It is neither an operator/config decision
    (``disabled_by_declaration``) nor a clock fact (``outside_session_window``
    — no schedule/window was involved, just a freshness comparison against
    what is already in ArcticDB). A ``skip_reason`` this function does not
    recognize FAILS LOUD (``_CollectorError``) rather than defaulting to any
    member — a new skip producer earns its own classification against the
    lib's definitions, not a guess.
    """
    unit_id = run_units.MODE_UNITS[mode]
    run_date = getattr(args, "date", None) or default_run_date()
    if getattr(args, "dry_run", False):
        return fn(config, args)

    captured: dict = {}

    def _body(run_ctx) -> dict:
        token = _CURRENT_RUN_CTX.set(run_ctx)
        try:
            result = fn(config, args)
        finally:
            _CURRENT_RUN_CTX.reset(token)
        captured["result"] = result
        # A mode function returning a non-ok status has FAILED — the manifest
        # says so rather than recording a successful run whose body reported a
        # failure it swallowed (`observability-policy` §3.1). The raise is
        # caught below and the caller's own return value is unchanged: the
        # manifest's honesty must not become a new exit-code path on a
        # scheduled pipeline.
        status = (result or {}).get("status")
        if status not in (None, "ok", "skipped", "ok_dry_run"):
            # alpha-engine-config-I10941: bounded + elided, never a raw
            # f-string of `result` — that inlined `constituents_preflight`'s
            # 903-ticker array and truncated the actual cause out of the
            # 2026-09-16 shadow run's manifest. `result` is also passed
            # through so the failure manifest still folds on whatever guards
            # the mode graded itself (`_record_collector_guards` below).
            raise _CollectorError(mode, run_units.describe_mode_failure(mode, result), result)
        if status == "skipped":
            detail = str((result or {}).get("skip_reason") or f"{mode} reported status=skipped")
            # Match against the one enumerated producer BEFORE recording the
            # guard verdict — an unrecognized reason is not a not_applicable
            # candidate at all (see this function's docstring;
            # alpha-engine-config-I10831, corrected 2026-09-15).
            if not detail.startswith("stale_overwrite"):
                raise _CollectorError(
                    mode,
                    f"{mode} returned status=skipped with an unclassified skip_reason "
                    f"{detail!r} — no nousergon_lib.run_manifest.NOT_APPLICABLE_REASONS "
                    "member has been matched to this cause. Classify it explicitly "
                    "against the lib's own per-member definitions rather than "
                    "defaulting (alpha-engine-config-I10831).",
                )
            run_ctx.record_guard(
                expectations.EMPTY_FRESH_GUARD.name,
                mode=expectations.EMPTY_FRESH_GUARD.mode.value,
                verdict="not_applicable",
                detail=f"{unit_id} published nothing on this run ({detail})",
            )
            raise run_manifest.NotApplicable(run_units.NOT_RUN_NO_NEW_DATA_DECLARED, detail)
        _record_mode_lineage(run_ctx, mode, unit_id, result or {})
        # `alpha-engine-config-I11011`: same rule as `_phase_collect`, at the
        # other dispatch. A whole-mode unit that finished having recorded no
        # output at all — including one whose declared row source reported no
        # number, so nothing could be recorded — does not file `ok`. Raised
        # inside the wrapper, caught below, exit code unchanged.
        if not run_ctx.outputs and not int(run_ctx.rows_out or 0):
            run_units.record_empty_production(
                run_ctx,
                unit_id,
                detail=f"mode {mode!r} completed and recorded no published output",
            )
        return result

    try:
        run_result = run_manifest.run_unit(
            unit_id,
            _body,
            sink=run_units.manifest_sink(config["bucket"]),
            trigger=run_units.resolve_trigger("scheduled"),
            trading_day=run_date,
            log_location=run_units.resolve_log_location(),
        )
        # `run_unit` does not re-raise NotApplicable — it records the non-run
        # and returns with `value=None`. The caller's contract is the mode's own
        # result dict, so the captured one is returned rather than the wrapper's
        # None, which would turn an honest non-run into an AttributeError.
        if run_result.status == "not_applicable":
            return captured["result"]
        return run_result.value
    except run_units.EmptyProduction:
        # `alpha-engine-config-I11011`. The manifest is durable with
        # `status: failed` and the empty production as its reason; the caller
        # keeps the result dict it would have received before this existed.
        return captured["result"]
    except _CollectorError:
        # The manifest is already written with `status: failed`; the caller
        # keeps the result dict it would have received before this wrapper
        # existed, and main()'s exit-code contract is untouched.
        return captured["result"]


def _daily_heal_daily_closes_keys(result: dict) -> list[str]:
    """The ``staging/daily_closes/{day}.parquet`` keys D33's universe-gap heal
    actually staged this run — one per entry in
    ``collectors.universe_gap_heal.healed_days``, never a copy of the
    descriptor's ``staging/daily_closes/*`` wildcard (alpha-engine-config-I10861).
    Each entry is normally ``{"date": ..., "kind": ..., ...}``
    (``_self_heal_missing_universe_days``); a bare date string is tolerated too
    so a caller passing the plain list this function's own row count is
    measured from does not need to fabricate the dict shape."""
    healed = (
        (result.get("collectors") or {}).get("universe_gap_heal") or {}
    ).get("healed_days") or []
    dates = [h.get("date") if isinstance(h, dict) else h for h in healed]
    return [f"staging/daily_closes/{d}.parquet" for d in dates if d]


def _chronic_gap_heal_price_cache_keys(result: dict) -> list[str]:
    """The ``reference/price_cache/{ticker}.parquet`` keys D34's chronic-gap
    self-heal actually wrote this run (``_self_heal_chronic_polygon_gaps`` via
    ``_price_cache_write_prefixes`` — never ``staging/daily_closes``, which
    this mode never touches; alpha-engine-config-I10861)."""
    healed = (
        (result.get("collectors") or {}).get("chronic_gap_self_heal") or {}
    ).get("healed") or []
    return [f"reference/price_cache/{h['ticker']}.parquet" for h in healed if h.get("ticker")]


#: mode -> extra ``(key_or_keys_fn, present_fn, rows_fn)`` entries recording
#: every S3 key that mode's own result shows it ACTUALLY wrote this run — the
#: whole-mode twin of ``_phase_collect``'s ``extra_outputs``
#: (alpha-engine-config-I10861). Each callable is evaluated against the
#: mode's own ``result`` dict at record time, never against the descriptor:
#: recording a key the mode did not write this run would be the same defect
#: this deliverable exists to close, just moved one level up. Deliberately
#: keyed independent of the ArcticDB row measurement below — D17's weekday
#: ``--skip-arctic-append`` run publishes ``staging/daily_closes`` with NO
#: arctic write in the same run, and gating this recording behind the arctic
#: count (as the prior single-key implementation did) left it permanently
#: unrecorded on every weekday run.
_MODE_EXTRA_OUTPUTS: dict[str, tuple[tuple[object, object, object], ...]] = {
    "morning_enrich": (
        (
            lambda r: [f"staging/daily_closes/{r.get('date')}.parquet"],
            lambda r: ((r.get("collectors") or {}).get("daily_closes") or {}).get("status")
            == "ok",
            lambda r: ((r.get("collectors") or {}).get("daily_closes") or {}).get(
                "tickers_captured"
            )
            or 0,
        ),
    ),
    "daily_heal": (
        (
            lambda r: [f"data/heal/daily/{r.get('date')}.json"],
            lambda r: bool(r.get("date")),
            lambda r: r.get("days_healed") or 0,
        ),
        (
            _daily_heal_daily_closes_keys,
            lambda r: bool(_daily_heal_daily_closes_keys(r)),
            lambda r: len(
                ((r.get("collectors") or {}).get("universe_gap_heal") or {}).get(
                    "healed_days"
                )
                or []
            ),
        ),
    ),
    "chronic_gap_heal": (
        (
            _chronic_gap_heal_price_cache_keys,
            lambda r: bool(_chronic_gap_heal_price_cache_keys(r)),
            lambda r: sum(
                int(h.get("rows_added") or 0)
                for h in (
                    (r.get("collectors") or {}).get("chronic_gap_self_heal") or {}
                ).get("healed")
                or []
            ),
        ),
    ),
}


def _record_mode_lineage(run_ctx, mode: str, unit_id: str, result: dict) -> None:
    """Fold a whole-mode unit's published outputs onto its run manifest.

    ``alpha-engine-config-I10861``: every S3 key the mode's own result shows
    it actually wrote this run is recorded under THAT key (``_MODE_EXTRA_
    OUTPUTS`` — never copied from the descriptor). The ArcticDB library write,
    when this run made one, is recorded under the library's own reference
    spelling (``arcticdb/universe``, matching every ``MODE_UNITS`` descriptor's
    ``writes:`` entry) instead of a synthesized ``arcticdb://{unit_id}`` that
    no S3 key template a descriptor could declare would ever match — the
    literal-key defect that left D33/D34's completion check red by
    construction on every run. PR1739 fixed the same class one call site over,
    for ``_record_phase_lineage``.
    """
    for key_or_keys, present_fn, rows_fn in _MODE_EXTRA_OUTPUTS.get(mode, ()):
        if present_fn(result):
            keys = key_or_keys(result) if callable(key_or_keys) else (key_or_keys,)
            rows_out = int(rows_fn(result) or 0)
            for k in keys:
                run_ctx.record_output(k, rows_out=rows_out)

    spec = run_units.MODE_ROWS.get(mode)
    published = (result.get("collectors") or {}).get(spec.collector, {}) if spec else {}
    rows: int | None = None
    if spec:
        raw = published.get(spec.rows_key)
        if spec.counts_list and isinstance(raw, list):
            rows = len(raw)
        elif not spec.counts_list and not isinstance(raw, bool) and isinstance(raw, (int, float)):
            rows = int(raw)

    if rows is None:
        where = (
            f"{spec.collector}.{spec.rows_key}" if spec else "no declared row source"
        )
        run_ctx.record_guard(
            expectations.EMPTY_FRESH_GUARD.name,
            mode=expectations.EMPTY_FRESH_GUARD.mode.value,
            verdict="unmeasurable",
            detail=(
                f"{unit_id} reported no row count this run ({where}), so the empty-but-fresh "
                "guard cannot read it from this call site. UNMEASURABLE, not a pass — the "
                "work item is the step recording its own outputs "
                "(data_collection_plan_260914.md §4.5)."
            ),
        )
        return

    # These units publish through ArcticDB libraries and per-symbol keys rather
    # than one S3 object, so the manifest's output is addressed by the unit's
    # declared prefix. The count is REAL and measured, which is what the
    # empty-but-fresh objective needs; the ArcticDB probe
    # (data_collection/probes/arctic/{trading_day}.json) is the independent
    # read-back of the same write.
    run_ctx.record_output("arcticdb/universe", rows_out=rows)
    _record_rejections(run_ctx, published, spec.rejected_keys)
    run_ctx.record_guard(
        expectations.EMPTY_FRESH_GUARD.name,
        mode=expectations.EMPTY_FRESH_GUARD.mode.value,
        verdict="ok" if rows > 0 else "empty_fresh",
        detail=(
            f"{unit_id} published {rows} row(s) via {spec.collector}.{spec.rows_key}"
            if rows > 0
            else (
                f"{unit_id} completed and published ZERO rows via "
                f"{spec.collector}.{spec.rows_key} — a fresh, empty write"
            )
        ),
        value=float(rows),
    )


def run_weekly(config: dict, args: argparse.Namespace) -> dict:
    """Run collectors based on mode selection."""
    if getattr(args, "morning_enrich", False):
        return _run_whole_mode_unit("morning_enrich", _run_morning_enrich, config, args)

    if getattr(args, "morning_arctic_append", False):
        return _run_whole_mode_unit(
            "morning_arctic_append", _run_morning_arctic_append, config, args
        )

    if getattr(args, "daily_arctic_append", False):
        return _run_whole_mode_unit(
            "daily_arctic_append", _run_daily_arctic_append, config, args
        )

    if getattr(args, "chronic_gap_heal", False):
        return _run_whole_mode_unit("chronic_gap_heal", _run_chronic_gap_heal, config, args)

    if getattr(args, "daily_heal", False):
        return _run_whole_mode_unit("daily_heal", _run_daily_heal, config, args)

    if args.daily:
        return _run_daily(config, args)

    phase = args.phase
    if phase is None:
        phase = 1

    if phase == 1:
        return _run_phase1(config, args)
    elif phase == 2:
        return _run_phase2(config, args)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def _run_phase1(config: dict, args: argparse.Namespace) -> dict:
    """Phase 1: constituents, historical (PIT) constituents, prices, macro, universe returns."""
    bucket = config["bucket"]
    price_cfg = config.get("price_cache", {})
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    ur_cfg = config.get("universe_returns", {})
    run_date = args.date or default_run_date()
    dry_run = args.dry_run
    only = args.only
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "phase": 1,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    # ── Preflight ────────────────────────────────────────────────────────────
    # Preflight runs once at the entrypoint via ``main()`` (see preflight.py).
    # The previous _run_phase1-local invocation against ``validators/preflight.py``
    # was retired 2026-04-30 alongside the lib consolidation — both files were
    # running back-to-back with overlapping scope. Single source of truth now.

    # ── 1. Constituents ──────────────────────────────────────────────────────
    tickers: list[str] = []
    if only in (None, "constituents"):
        logger.info("=" * 60)
        logger.info("COLLECTING: constituents")
        logger.info("=" * 60)
        const_result = _phase_collect(
            reg, "constituents",
            lambda: constituents.collect(
                bucket=bucket, s3_prefix=market_prefix, run_date=run_date, dry_run=dry_run,
            ),
            artifact_key=f"{market_prefix}weekly/{run_date}/constituents.json",
            # alpha-engine-config-I10855: `latest_weekly.json` is D01's second
            # declared key, but it is written once, by `_finalize`/`_write_manifest`
            # at the very end of the whole weekly run — not by `constituents.collect`
            # itself. `_finalize` only writes it when `only is None` (a full run) and
            # not dry_run; that write is then deterministic once THIS phase completes
            # successfully. There is no later hook back into D01's manifest to record
            # it after the fact, so it is declared here — present only under the same
            # `only is None` condition `_finalize` itself gates on.
            extra_outputs=(
                (f"{market_prefix}latest_weekly.json", lambda r: True, lambda r: r.get("count") or 0),
                # alpha-engine-config-I10898: the three dual-path maps
                # `constituents.collect` writes. Declared in D01's descriptor as
                # published keys, so the manifest has to record them or the
                # dispatcher's completion check fails D01 on a key it declares
                # and never reports. Written unconditionally by a real run, so
                # the same `present_fn` as latest_weekly.json; rows come from
                # the counts collect() now returns, never a bare 0.
                ("data/sector_map.json", lambda r: True, lambda r: r.get("sector_map_count") or 0),
                ("reference/price_cache/sector_map.json", lambda r: True, lambda r: r.get("sector_map_count") or 0),
                ("data/sub_industry_map.json", lambda r: True, lambda r: r.get("sub_industry_map_count") or 0),
                ("reference/price_cache/sub_industry_map.json", lambda r: True, lambda r: r.get("sub_industry_map_count") or 0),
                ("data/sub_sector_etf_map.json", lambda r: True, lambda r: r.get("sub_sector_etf_map_count") or 0),
                ("reference/price_cache/sub_sector_etf_map.json", lambda r: True, lambda r: r.get("sub_sector_etf_map_count") or 0),
            ) if only is None else (),
        )
        results["collectors"]["constituents"] = const_result
        # Use the tickers returned by collect() directly (empty on auto-skip →
        # the load_from_s3 fallback below repopulates from the cached artifact).
        tickers = const_result.get("tickers", [])

    # If we didn't collect constituents, load from S3
    if not tickers and only not in ("constituents",):
        try:
            existing = constituents.load_from_s3(bucket, market_prefix)
            if existing:
                tickers = existing.get("tickers", [])
                logger.info("Loaded %d tickers from existing constituents.json", len(tickers))
        except Exception as exc:
            logger.warning("S3 constituents load failed — will fall back to Wikipedia: %s", exc)

    # ── 1b. Historical (point-in-time) constituents ──────────────────────────
    # Replays the S&P 500 "Selected changes" table backward from today's roster
    # to a {date: [tickers]} PIT membership map at
    # market_data/historical_constituents.json — the survivorship-free universe
    # substrate the backtester consumes (config#657, G12). Reuses `tickers` (the
    # roster already collected/loaded above) so the two collectors' rosters stay
    # consistent and it avoids a second live fetch. Runs in the default Phase-1
    # sweep; without this wiring the collector was dead code and the S3 key was
    # never written.
    if only in (None, "historical_constituents"):
        logger.info("=" * 60)
        logger.info("COLLECTING: historical constituents (point-in-time membership)")
        logger.info("=" * 60)
        if not tickers:
            logger.warning(
                "No tickers available — skipping historical constituents (PIT map "
                "needs today's roster to replay changes from)"
            )
            results["collectors"]["historical_constituents"] = _phase_not_run(
                reg, "historical_constituents", reason="no tickers", applicable=True
            )
        else:
            results["collectors"]["historical_constituents"] = _phase_collect(
                reg, "historical_constituents",
                lambda: historical_constituents.collect(
                    bucket=bucket, current_tickers=tickers,
                    s3_prefix=market_prefix, dry_run=dry_run,
                ),
                artifact_key=f"{market_prefix}historical_constituents.json",
            )

    # ── 2. Price cache refresh ───────────────────────────────────────────────
    if only in (None, "prices"):
        logger.info("=" * 60)
        logger.info("COLLECTING: price cache")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping price cache refresh")
            results["collectors"]["prices"] = _phase_not_run(
                reg, "prices", reason="no tickers", applicable=True
            )
        else:
            # supports_auto_skip=False: prices writes per-ticker parquet (no single
            # stable S3 key to validate), so markers + watchdog only — the phase
            # always runs.
            results["collectors"]["prices"] = _phase_collect(
                reg, "prices",
                lambda: prices.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                    fetch_period=price_cfg.get("fetch_period", "10y"),
                    staleness_threshold_days=price_cfg.get("staleness_threshold_days", 3),
                    batch_size=price_cfg.get("refresh_batch_size", 50),
                    dry_run=dry_run,
                    reference_date=run_date,
                ),
                supports_auto_skip=False,
                # alpha-engine-config-I11026: every per-ticker parquet this run
                # actually wrote, with its own row count.
                extra_outputs=_prices_extra_outputs(
                    price_cfg.get("s3_prefix", "predictor/price_cache/")
                ),
            )
            # config#2350 — reference/price_cache/ is variable cardinality
            # (grandfathered in ARTIFACT_REGISTRY.yaml) so this unconditional
            # sentinel is the ordinary-S3-ArtifactSpec proxy the freshness
            # monitor actually probes (price_cache_freshness_sentinel row),
            # mirroring the config#1787 feature-store sentinel. Written on
            # every successful (non-dry-run) weekly refresh, independent of
            # how many tickers were actually stale.
            if not dry_run and results["collectors"]["prices"].get("status") == "ok":
                _write_price_cache_freshness_sentinel(
                    boto3.client("s3"), bucket,
                    writer="nousergon-data:weekly_collector.py",
                )

    # ── 2b. FRED-only macro history refresh (alpha-engine-config-I9287) ──────
    # ``collectors/fred_history.py::backfill_to_s3`` was a one-shot operator
    # step ("Run after Stage 2.5 ships") never wired to any schedule.
    # Measured 2026-08-29: ``reference/price_cache/HYOAS.parquet`` last
    # modified 2026-05-19 and never touched since — the weekly ``macro``
    # rebuild (``builders/backfill.py``) faithfully rewrites
    # ``macro/HYOAS`` from that frozen parquet every Saturday, so the ArcticDB
    # symbol never advances even though the writer never stops. Explicit
    # ``tickers=`` (not ``FRED_HISTORY_MAP`` default) so this call never grows
    # to cover the caret index tickers (VIX/VIX3M/TNX/IRX) — those are
    # ``collectors/prices.py``'s longest-of-yfinance-and-FRED job
    # (alpha-engine-config-I9286); re-deriving them here from FRED alone
    # would silently discard that fallback logic.
    if only in (None, "prices", "fred_macro_history"):
        results["collectors"]["fred_macro_history"] = _phase_collect(
            reg, "fred_macro_history",
            lambda: fred_history.backfill_to_s3(
                bucket=bucket,
                s3_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                tickers=["TWO", "HYOAS", "BAA10Y"], trading_day=run_date,
                dry_run=dry_run,
            ),
            supports_auto_skip=False,
        )

    # ── 3. Slim cache — REMOVED (Wave-4) ─────────────────────────────────────
    # predictor/price_cache_slim/ deleted: every consumer (data macro-breadth
    # + feature compute, backtester exit_timing) reads the ArcticDB universe/
    # macro libs directly. No slim writer; the prefix is gone.

    # ── 4. Macro data ────────────────────────────────────────────────────────
    if only in (None, "macro"):
        logger.info("=" * 60)
        logger.info("COLLECTING: macro data")
        logger.info("=" * 60)
        results["collectors"]["macro"] = _phase_collect(
            reg, "macro",
            lambda: macro.collect(
                bucket=bucket, s3_prefix=market_prefix, run_date=run_date, dry_run=dry_run,
            ),
            artifact_key=f"{market_prefix}weekly/{run_date}/macro.json",
            # alpha-engine-config-I10855: `macro.collect` writes two SECONDARY,
            # fail-soft artifacts alongside macro.json (`write_macro_history` /
            # `write_release_calendar` — collectors/macro.py), each guarded so a
            # secondary failure never masks the primary write. Their own status
            # dicts (`macro_history`/`release_calendar` in the result) are the
            # only truthful source of "did this run actually write it" — a
            # `skipped_empty`/`error` status means no PUT happened this run.
            extra_outputs=(
                (
                    f"{market_prefix}macro_history.parquet",
                    lambda r: (r.get("macro_history") or {}).get("status") == "ok",
                    lambda r: (r.get("macro_history") or {}).get("rows") or 0,
                ),
                (
                    f"{market_prefix}macro_release_calendar.parquet",
                    lambda r: (r.get("release_calendar") or {}).get("status") == "ok",
                    lambda r: (r.get("release_calendar") or {}).get("rows") or 0,
                ),
            ),
        )

    # ── 4b. Short interest ───────────────────────────────────────────────────
    # Per-ticker yfinance Ticker.info scrape for the full S&P 500+400 universe.
    # FINRA data is bi-monthly (15th + EoM) so weekly Saturday cadence captures
    # every refresh with a buffer. ~10 min on the spot; the constituents list
    # was already fetched earlier in this phase.
    #
    # Gated by config["short_interest"]["enabled"] (default True). Disabling
    # lets the operator soft-launch a new collector without blocking the
    # whole pipeline if yfinance has trouble on the first Saturday — set
    # enabled=false in config, run once manually with --only short_interest,
    # then flip back to true once stable.
    si_cfg = config.get("short_interest", {})
    si_enabled = si_cfg.get("enabled", True)
    if only in (None, "short_interest") and si_enabled:
        logger.info("=" * 60)
        logger.info("COLLECTING: short interest")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping short interest")
            results["collectors"]["short_interest"] = _phase_not_run(
                reg, "short_interest", reason="no tickers", applicable=True
            )
        else:
            results["collectors"]["short_interest"] = _phase_collect(
                reg, "short_interest",
                lambda: short_interest.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=market_prefix,
                    run_date=run_date,
                    inter_request_delay=si_cfg.get("inter_request_delay", 0.4),
                    dry_run=dry_run,
                ),
                artifact_key=f"{market_prefix}weekly/{run_date}/short_interest.json",
            )
    elif only in (None, "short_interest") and not si_enabled:
        logger.info("short_interest collector disabled via config — skipping")
        results["collectors"]["short_interest"] = _phase_not_run(
            reg, "short_interest", reason="disabled_in_config", applicable=False
        )

    # ── 4c. Universe classification ──────────────────────────────────────────
    # Per-ticker yfinance Ticker.info scrape for sector/country-of-domicile/
    # industry over the full S&P 500+400 universe — the country dimension the
    # ~900-stock universe scoreboard (crucible-research scoring/universe_board.py)
    # filters on alongside sector and the factor/valuation metrics. Domicile is
    # near-static, so the weekly Saturday cadence is ample; the artifact is a
    # single latest.json (+ dated copy). ~10 min on the spot off the constituents
    # list already fetched earlier in this phase.
    #
    # Gated by config["universe_classification"]["enabled"] (default True), same
    # soft-launch pattern as short_interest: flip enabled=false to run once
    # manually with --only universe_classification, then back to true once stable.
    uc_cfg = config.get("universe_classification", {})
    uc_enabled = uc_cfg.get("enabled", True)
    if only in (None, "universe_classification") and uc_enabled:
        logger.info("=" * 60)
        logger.info("COLLECTING: universe classification")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping universe classification")
            results["collectors"]["universe_classification"] = _phase_not_run(
                reg, "universe_classification", reason="no tickers", applicable=True
            )
        else:
            results["collectors"]["universe_classification"] = _phase_collect(
                reg, "universe_classification",
                lambda: universe_classification.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=market_prefix,
                    run_date=run_date,
                    inter_request_delay=uc_cfg.get("inter_request_delay", 0.4),
                    dry_run=dry_run,
                ),
                artifact_key=f"{market_prefix}universe_classification/{run_date}.json",
                # alpha-engine-config-I10855: `latest.json` is co-written with the
                # dated key in the SAME `s3.put_object` pair, unconditionally on
                # `status == "ok"` (collectors/universe_classification.py) — same
                # row count as the dated artifact.
                extra_outputs=(
                    (
                        f"{market_prefix}universe_classification/latest.json",
                        lambda r: r.get("status") == "ok",
                        lambda r: r.get("ok_count") or 0,
                    ),
                ),
            )
    elif only in (None, "universe_classification") and not uc_enabled:
        logger.info("universe_classification collector disabled via config — skipping")
        results["collectors"]["universe_classification"] = _phase_not_run(
            reg, "universe_classification", reason="disabled_in_config", applicable=False
        )

    # ── 5. Universe returns ──────────────────────────────────────────────────
    if only in (None, "universe_returns"):
        logger.info("=" * 60)
        logger.info("COLLECTING: universe returns")
        logger.info("=" * 60)
        db_path = ur_cfg.get("db_path")
        if not db_path:
            # Download research.db from S3 to temp dir
            import tempfile
            tmp_dir = tempfile.mkdtemp(prefix="ae-data-")
            db_path = os.path.join(tmp_dir, "research.db")
            try:
                s3 = boto3.client("s3")
                s3.download_file(bucket, "research.db", db_path)
                logger.info("Downloaded research.db to %s", db_path)
            except Exception as e:
                logger.warning("Could not download research.db: %s", e)
                results["collectors"]["universe_returns"] = {"status": "error", "error": str(e)}
                db_path = None

        if db_path:
            # supports_auto_skip=False: writes the shared mutable research.db.
            # It ALSO writes backups/research_{run_date}.db since
            # alpha-engine-config-I10202, but that dated object is a backup of
            # the shared database rather than this phase's own product, so the
            # auto-skip validator still has nothing phase-specific to check.
            results["collectors"]["universe_returns"] = _phase_collect(
                reg, "universe_returns",
                lambda: universe_returns.collect(
                    bucket=bucket,
                    db_path=db_path,
                    signals_prefix=ur_cfg.get("signals_prefix", "signals"),
                    sector_map_key=ur_cfg.get(
                        "sector_map_key", "reference/price_cache/sector_map.json"
                    ),
                    dry_run=dry_run,
                    run_date=run_date,
                ),
                supports_auto_skip=False,
                # alpha-engine-config-I10855: D08 had NO recording at all (no
                # `artifact_key`) — `research.db` (the live pointer) and
                # `backups/research_{date}.db` are both written, ONLY when the run
                # actually inserted rows (`db_upload` names the two keys the
                # producer itself just uploaded; see collectors/universe_returns.py).
                extra_outputs=(
                    (
                        "research.db",
                        lambda r: bool((r.get("db_upload") or {}).get("pointer_key")),
                        lambda r: r.get("rows_inserted") or 0,
                    ),
                    (
                        f"backups/research_{run_date}.db",
                        lambda r: bool((r.get("db_upload") or {}).get("backup_key")),
                        lambda r: r.get("rows_inserted") or 0,
                    ),
                ),
            )

    # ── 5b. Signal returns (score_performance + predictor_outcomes) ────────────
    if only in (None, "signal_returns"):
        logger.info("=" * 60)
        logger.info("COLLECTING: signal returns (score_performance + predictor_outcomes)")
        logger.info("=" * 60)
        # Reuse the same db_path from universe_returns (already pulled from S3)
        sr_db_path = db_path
        if sr_db_path:
            sr_cfg = config.get("signal_returns") or {}
            # supports_auto_skip=False: also writes the shared research.db.
            results["collectors"]["signal_returns"] = _phase_collect(
                reg, "signal_returns",
                lambda: signal_returns.collect(
                    bucket=bucket,
                    db_path=sr_db_path,
                    signals_prefix=ur_cfg.get("signals_prefix", "signals"),
                    dry_run=dry_run,
                    forward_days=int(sr_cfg.get("forward_days", 21)),
                    run_date=run_date,
                ),
                supports_auto_skip=False,
            )
        else:
            results["collectors"]["signal_returns"] = _phase_not_run(
                reg, "signal_returns", reason="no research.db", applicable=True
            )

    # ── 6. Fundamentals ───────────────────────────────────────────────────────
    if only in (None, "fundamentals"):
        logger.info("=" * 60)
        logger.info("COLLECTING: fundamentals (FMP)")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping fundamentals")
            results["collectors"]["fundamentals"] = _phase_not_run(
                reg, "fundamentals", reason="no tickers", applicable=True
            )
        else:
            results["collectors"]["fundamentals"] = _phase_collect(
                reg, "fundamentals",
                lambda: fundamentals.collect(
                    bucket=bucket, tickers=tickers, run_date=run_date, dry_run=dry_run,
                ),
                artifact_key=f"archive/fundamentals/{run_date}.json",
            )

    # ── 6b. Metron valuation medians (SP1500-broad sector & country benchmark) ──
    # Powers Metron's Holdings "by sector → country" median bands. Weekly cadence —
    # the median of ~900 names' multiples is stable week to week. Builds its own
    # (SP1500 ∪ held) universe, so it runs independent of `tickers`.
    if only in (None, "metron_valuation_medians"):
        logger.info("=" * 60)
        logger.info("COLLECTING: metron valuation medians (sector & country)")
        logger.info("=" * 60)
        results["collectors"]["metron_valuation_medians"] = _phase_collect(
            reg, "metron_valuation_medians",
            lambda: metron_market_data.collect_valuation_medians(
                bucket=bucket, run_date=run_date, dry_run=dry_run,
            ),
            artifact_key=f"{metron_market_data.VALUATION_MEDIANS_PREFIX}latest.json",
        )

    # ── 7. Feature store compute ───────────────────────────────────────────
    if only in (None, "features"):
        logger.info("=" * 60)
        logger.info("COMPUTING: feature store snapshot")
        logger.info("=" * 60)
        from features.compute import compute_and_write
        results["collectors"]["features"] = _phase_collect(
            reg, "features",
            lambda: compute_and_write(date_str=run_date, bucket=bucket, dry_run=dry_run),
            artifact_key=f"features/{run_date}/schema_version.json",
            # alpha-engine-config-I10855: D12 declares 5 real parquet keys under
            # `features/{date}/` (D12 does not declare the metron_supplemental
            # prefix — that is D31's daily call below).
            extra_outputs=_feature_group_extra_outputs(run_date),
        )

    # ── 8. ArcticDB universe rebuild ─────────────────────────────────────────
    if only in (None, "arcticdb"):
        logger.info("=" * 60)
        logger.info("REBUILDING: ArcticDB universe (full backfill)")
        logger.info("=" * 60)
        from builders.backfill import backfill
        # supports_auto_skip=False: writes ArcticDB (no S3 key); backfill is
        # idempotent and cheap to repeat → markers + watchdog only.
        results["collectors"]["arcticdb"] = _phase_collect(
            reg, "arcticdb",
            lambda: backfill(bucket=bucket, dry_run=dry_run, run_date=run_date),
            supports_auto_skip=False,
        )

    # ── Finalize ─────────────────────────────────────────────────────────────
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, only)
    return results


def _run_phase2(config: dict, args: argparse.Namespace) -> dict:
    """Phase 2: alternative data for the constituent universe (after research)."""
    bucket = config["bucket"]
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    run_date = args.date or default_run_date()
    dry_run = args.dry_run
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "phase": 2,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    # Scope resolution (alpha-engine-config-I5814, Brian ruling 2026-07-31).
    #
    # Phase 2 resolves its ticker list from constituents.json — the SAME source
    # phase 1 passes to fundamentals.collect — rather than from
    # signals/{date}/signals.json's `universe` array.
    #
    # Why: the seven `alternative`-family columns in features/registry.py are
    # written for every ticker in the ArcticDB universe, so this collector's
    # scope IS the constituent universe. Resolving it from signals.json made
    # that scope a function of whichever producer happens to be champion:
    # measured 2026-07-21, the retirement of the multi-agent Research stage
    # (config-I2515 / config#1580) swapped a 27-name promoted list for a
    # 902-row board, so this collector's input grew 33x overnight with no
    # change to its own code. Its Lambda duration went 126-215s -> ~2000s and
    # it broke through the 600s ceiling on the next weekly run.
    #
    # signals.json's `universe` is a SIZING ENVELOPE for the executor, not a
    # scope (alpha-engine-config-I5809). constituents.json is the universe
    # definition, and it is what every other universe-wide collector reads.
    universe_tickers: list[str] = []
    existing = constituents.load_from_s3(bucket, market_prefix)
    if existing:
        universe_tickers = list(existing.get("tickers") or [])
    if not universe_tickers:
        # No silent narrowing. A missing constituents artifact used to fall
        # through to signals.json, which is exactly how the scope moved
        # without anyone deciding it.
        raise _CollectorError(
            "alternative",
            "constituents.json unreadable or empty at "
            f"{market_prefix}weekly/<latest>/constituents.json — refusing to "
            "collect alternative data against an implicit ticker list. Fix the "
            "constituents artifact rather than falling back to "
            "signals.json::universe (alpha-engine-config-I5814).",
        )

    logger.info("=" * 60)
    logger.info("COLLECTING: alternative data (Phase 2)")
    logger.info("=" * 60)

    # ── Record resolved scope BEFORE collection ───────────────────────────
    # The scope guard (_assert_scope_stable) needs a truthful baseline for the
    # NEXT run.  If this run fails mid-collection (spot termination, provider
    # outage), the manifest is never written and the prior-run lookup deadlocks:
    # the only run that can advance the baseline is a run the guard permits,
    # and the guard permits none (the prior manifest is pre-change).  Writing
    # scope.json at resolution time — before the first API call — means even a
    # partial run leaves a truthful baseline.  Read preference is scope.json
    # first, manifest second (see collectors/alternative.py).
    # The write is NOT swallowed (alpha-engine-config-I10784, P-17; was a bare
    # `except Exception: logger.warning(... "non-fatal")` here). The paragraph
    # above is the reason it cannot be non-fatal: the whole point of writing
    # scope.json at resolution time is that a partial run still leaves a
    # truthful baseline for the guard. A run that fails to write it and then
    # collects anyway produces exactly the state that paragraph exists to
    # prevent — the guard falls back to a PRE-CHANGE prior manifest, and the
    # only run that can advance the baseline is one the guard permits, which is
    # none. The failure now surfaces as a `_CollectorError`, which the phase
    # registry records as an `error` marker (so a recovery re-runs it) and
    # which reaches the run manifest as `status: failed`.
    scope_key = f"{market_prefix}weekly/{run_date}/alternative/scope.json"

    def _collect_alternative() -> dict:
        # Inside the phase body, so the failure below lands on D15's own run
        # manifest as `status: failed` rather than crashing outside every
        # record this unit writes. Still strictly before the first vendor call,
        # which is the property the paragraph above depends on.
        _write_alternative_scope_baseline(bucket, scope_key, run_date, universe_tickers)
        return alternative.collect(
            bucket=bucket, s3_prefix=market_prefix, run_date=run_date,
            tickers=universe_tickers, dry_run=dry_run,
        )

    results["collectors"]["alternative"] = _phase_collect(
        reg, "alternative", _collect_alternative,
        artifact_key=f"{market_prefix}weekly/{run_date}/alternative/manifest.json",
        # alpha-engine-config-I10855: `scope.json` is written unconditionally,
        # inside THIS phase body, strictly before the first vendor call (see the
        # comment block above) — a write failure there raises and never reaches
        # this point, so by the time _record_phase_lineage runs it always
        # exists. `{ticker}.json` (D15's third declared, wildcard, key) needs no
        # separate entry: both this key and manifest.json are single path
        # segments under .../alternative/, so either one already satisfies that
        # template's regex.
        extra_outputs=(
            (scope_key, lambda r: True, lambda r: len(universe_tickers)),
        ),
    )

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, None)
    return results


def _write_alternative_scope_baseline(
    bucket: str, scope_key: str, run_date: str, universe_tickers: list[str]
) -> None:
    """Publish D15's resolved scope, or RAISE.

    `alpha-engine-config-I10784` (P-17) — this write used to be wrapped in a
    bare ``except Exception: logger.warning(..., "non-fatal; scope guard will
    fall back to prior manifest")``, the swallow named in the plan's §1 layer
    verdict. It is not non-fatal. The caller's own comment says why: the guard
    (`collectors/alternative.py::_assert_scope_stable`) needs a truthful
    baseline for the NEXT run, and writing it at resolution time is what makes
    a partial run still leave one. A run that fails this write and collects
    anyway leaves the guard reading a PRE-CHANGE prior manifest — after which
    the only run that can advance the baseline is one the guard permits, and
    the guard permits none. The "fallback" the warning promised is the
    deadlock, not a recovery from it.

    The fleet default is RAISE (`AGENTS.md`, fail loud and fast); the deviation
    this swallow represented is closed here rather than re-justified.
    """
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=scope_key,
        Body=json.dumps({
            "tickers_requested": len(universe_tickers),
            "resolved_from": "constituents.json",
            "run_date": run_date,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }),
        ContentType="application/json",
    )
    logger.info(
        "Wrote scope baseline: s3://%s/%s (%d tickers)", bucket, scope_key, len(universe_tickers)
    )


def _previous_trading_day(reference: datetime | None = None) -> str:
    """Find the most recent trading day strictly before ``reference`` (UTC).

    Used by --morning-enrich to determine which date polygon's grouped-daily
    should be fetched for. Free tier won't serve today's data, so we always
    enrich the prior session. Walks back at most 10 calendar days as a
    runaway guard against a broken trading-calendar implementation.
    """
    from nousergon_lib.trading_calendar import is_trading_day
    from datetime import timedelta

    ref = reference or datetime.now(timezone.utc)
    d = ref.date() - timedelta(days=1)
    for _ in range(10):
        if is_trading_day(d):
            return d.strftime("%Y-%m-%d")
        d -= timedelta(days=1)
    raise RuntimeError(
        f"Could not find a trading day in the 10 calendar days before {ref.date()} — "
        f"trading_calendar.is_trading_day appears broken or NYSE has been closed for >1 week."
    )


def _arctic_spy_last_date(bucket: str) -> "date | None":
    """Return SPY's last-indexed date in ArcticDB macro lib, or None on read failure.

    Best-effort: any exception (lib unavailable, empty symbol, transient S3) is
    logged and resolved as None so the skip decision falls through to "run
    polygon". Polygon will surface its own failures loudly downstream.
    """
    try:
        from store.arctic_store import get_macro_lib
        import pandas as pd
        macro_lib = get_macro_lib(bucket)
        df = macro_lib.tail("SPY", n=1).data
        if df.empty:
            return None
        return pd.Timestamp(df.index[-1]).date()
    except Exception as exc:
        logger.warning(
            "ArcticDB SPY last_date read failed (%s) — skip-guard will proceed without staleness check",
            exc,
        )
        return None


def _should_skip_morning_enrich(
    target_date: str,
    arctic_last_date: "date | None",
) -> tuple[bool, str | None]:
    """Decide whether to skip MorningEnrich based on data staleness.

    Returns ``(skip, reason)``. ``skip=True`` means the caller should bail
    out before invoking polygon. Rationale:

    Polygon's free tier 403's same-day grouped-daily, and the next session's
    T+1 settlement isn't visible until the following morning (the Saturday SF
    cron — 09:00 UTC = 02:00 AM PT — was chosen on this basis). On a manual
    midweek "Saturday SF" rerun after the EOD post-market pass has already
    landed today's yfinance row, polygon's prior-trading-day overwrite would
    write *older* data over *newer* data already in ArcticDB.

    The check: if polygon's ``target_date`` is strictly before what's already
    in ArcticDB, skip. The yfinance row written by this afternoon's EOD
    PostMarketData stays authoritative for this run; the next regular
    Saturday SF re-fetches the affected sessions from polygon T+1, restoring
    authoritative VWAP/OHLCV.

    The scheduled Saturday 02:00 PT cron and the weekday-SF 06:15 PT
    MorningEnrich Lambda both target the prior trading day, which equals
    ArcticDB's last_date at that hour, so the check returns ``skip=False``
    and polygon runs as normal.

    Explicit ``--date`` is handled by the caller and bypasses the check.
    """
    if arctic_last_date is None:
        return False, None
    if target_date < arctic_last_date.isoformat():
        return True, (
            f"stale_overwrite (polygon target={target_date}, "
            f"ArcticDB SPY last={arctic_last_date.isoformat()}) — polygon's "
            f"T+1 settled day is older than the yfinance EOD row already in "
            f"ArcticDB; skipping to avoid overwriting newer with older. "
            f"Next Saturday SF re-fetches via polygon T+1."
        )
    return False, None


def _detect_chronic_gap_polygon_recovery(
    bucket: str,
    target_date: str,
    chronic_tickers: list[str],
    daily_closes_prefix: str = "staging/daily_closes/",
) -> dict:
    """Drift alarm: detect when polygon STARTS covering a chronic-gap ticker.

    Pairs with ``_self_heal_chronic_polygon_gaps``. The chronic_polygon_gaps
    allowlist (alpha-engine-config #88) was added because polygon doesn't
    reliably serve these tickers (BF-B/BRK-B/MOG-A/PSTG today). If polygon
    coverage recovers — e.g. polygon adds a Berkshire B share class CIK
    or fixes a flaky data feed — the allowlist entry is no longer needed
    and should be pruned. Without active drift detection the entry would
    persist indefinitely as a silent piece of operational debt.

    Reads ``staging/daily_closes/{target_date}.parquet`` written by
    ``daily_closes.collect(source="polygon_only")`` and counts how many
    chronic_polygon_gaps tickers polygon DID cover today. Emits a
    CloudWatch gauge ``AlphaEngine/Data/chronic_gap_polygon_recovery_count``
    so an alarm can fire if the count > 0 for N consecutive Saturdays
    (operator action: prune the allowlist entry).

    Best-effort: read errors / metric-emit errors log a warning but never
    raise — this is observability, not a load-bearing path.

    Returns a summary dict; caller logs at INFO + persists in collector
    results so the manifest carries a record.
    """
    summary: dict = {
        "status": "ok",
        "chronic_tickers_checked": len(chronic_tickers),
        "polygon_recovered": [],
        "absent_as_expected": [],
        "errors": [],
    }
    if not chronic_tickers:
        return summary

    import io as _io

    try:
        s3 = boto3.client("s3")
        key = f"{daily_closes_prefix.rstrip('/')}/{target_date}.parquet"
        obj = s3.get_object(Bucket=bucket, Key=key)
        import pandas as _pd
        df = _pd.read_parquet(_io.BytesIO(obj["Body"].read()))
    except Exception as exc:
        logger.warning(
            "chronic-gap drift check: could not read staging/daily_closes "
            "parquet for %s — drift alarm skipped this cycle. %s",
            target_date, exc,
        )
        summary["status"] = "skipped"
        summary["errors"].append({"reason": str(exc)})
        return summary

    # Index is ticker (collectors/daily_closes.py:251 sets index=ticker).
    daily_closes_tickers = (
        set(df.index.astype(str)) if df.index.size else set()
    )

    for ticker in chronic_tickers:
        if ticker in daily_closes_tickers:
            summary["polygon_recovered"].append(ticker)
        else:
            summary["absent_as_expected"].append(ticker)

    n_recovered = len(summary["polygon_recovered"])
    if n_recovered > 0:
        logger.warning(
            "chronic-gap drift detected: polygon now covers %d chronic_polygon_gaps "
            "ticker(s) it did not previously serve: %s. Consider pruning these from "
            "alpha-engine-config/predictor.yaml chronic_polygon_gaps.tickers if the "
            "coverage persists across multiple cycles.",
            n_recovered, summary["polygon_recovered"],
        )
    else:
        logger.info(
            "chronic-gap drift: 0 of %d chronic tickers showed up in today's "
            "polygon_only daily_closes — allowlist still load-bearing.",
            len(chronic_tickers),
        )

    # Emit CloudWatch metric — best-effort. Always emits (including 0) so
    # alarm baselines are continuous; CloudWatch missing-data is harder to
    # alarm against than a steady 0 stream.
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "chronic_gap_polygon_recovery_count",
                "Value": float(n_recovered),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning(
            "chronic_gap_polygon_recovery_count metric emit failed: %s — "
            "drift alarm cadence may degrade until next cycle.", exc,
        )

    return summary


def _detect_chronic_gap_constituents_drift(
    bucket: str,
    chronic_tickers: list[str],
) -> dict:
    """Drift alarm: detect when a chronic_polygon_gaps allowlist ticker has
    dropped out of the current S&P 500/400 constituents set.

    Pairs with ``_self_heal_chronic_polygon_gaps`` and serves as the GATE
    on its inputs — non-constituent tickers are filtered out before any
    yfinance fetch or ``backfill(ticker_filter=...)`` call so the heal
    path never hands a non-constituent ticker to the constituents-filtered
    backfill writer (which hard-errs per ``builders/backfill.py``).

    Mirrors :func:`_detect_chronic_gap_polygon_recovery` on the inverse
    axis. The polygon-recovery detector catches the "polygon now serves
    this — remove from allowlist" direction; this detector catches the
    "ticker no longer a constituent — remove from allowlist" direction.

    Origin: 2026-05-27 flow-doctor ERROR "Ticker PSTG not found in
    universe" — PSTG dropped from S&P 500/400 constituents between the
    5/16 and 5/23 weekly partitions (REMOVED cohort = {BK, FLO, PSTG};
    see config private-docs/ROADMAP.md L1772) but stayed in the
    chronic_polygon_gaps allowlist. MorningEnrich yfinance-backfilled
    PSTG.parquet then called ``backfill(ticker_filter='PSTG')``, which
    hard-erred against the constituents filter. The polygon-recovery
    drift detector was the existing axis; this is the missing inverse
    axis.

    Reads the current constituents via the shared chokepoint
    :func:`builders._constituents_loader.load_constituents_for_run_date`
    (no ``run_date`` argument → pointer-following ad-hoc read, which is
    the correct read for a MorningEnrich-time check between Saturday SFs).

    Emits a CloudWatch gauge
    ``AlphaEngine/Data/chronic_gap_non_constituent_count`` for alarming.

    Best-effort: a constituents read failure logs a WARN and returns
    ``status='skipped'`` with the full chronic list as ``still_constituents``
    so the caller falls through to the existing behavior (the original
    hard-err at backfill is then the load-bearing surface). Never raises.

    Returns
    -------
    dict
        ``{"status": "ok"|"skipped", "chronic_tickers_checked": int,
           "still_constituents": list[str], "dropped_non_constituent": list[str],
           "weekly_date": str|None, "errors": list[dict]}``
    """
    summary: dict = {
        "status": "ok",
        "chronic_tickers_checked": len(chronic_tickers),
        "still_constituents": list(chronic_tickers),
        "dropped_non_constituent": [],
        "weekly_date": None,
        "errors": [],
    }
    if not chronic_tickers:
        return summary

    try:
        from builders._constituents_loader import load_constituents_for_run_date
        s3 = boto3.client("s3")
        constituents_set, weekly_date = load_constituents_for_run_date(s3, bucket)
        summary["weekly_date"] = weekly_date
    except Exception as exc:
        logger.warning(
            "chronic-gap constituents-drift check: could not load current "
            "constituents — drift gate skipped this cycle, all %d chronic "
            "ticker(s) will proceed to self-heal. %s",
            len(chronic_tickers), exc,
        )
        summary["status"] = "skipped"
        summary["errors"].append({"reason": str(exc)})
        return summary

    still: list[str] = []
    dropped: list[str] = []
    for ticker in chronic_tickers:
        if ticker in constituents_set:
            still.append(ticker)
        else:
            dropped.append(ticker)

    summary["still_constituents"] = still
    summary["dropped_non_constituent"] = dropped

    if dropped:
        logger.warning(
            "chronic-gap constituents drift detected: %d chronic_polygon_gaps "
            "ticker(s) no longer in current constituents (%s, weekly=%s): %s. "
            "These will be SKIPPED by self-heal — prune from "
            "alpha-engine-config/data/config.yaml chronic_polygon_gaps.tickers "
            "to silence this WARN.",
            len(dropped), bucket, weekly_date, dropped,
        )
    else:
        logger.info(
            "chronic-gap constituents drift: %d of %d chronic tickers still "
            "in current constituents (weekly=%s) — allowlist coherent.",
            len(still), len(chronic_tickers), weekly_date,
        )

    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "chronic_gap_non_constituent_count",
                "Value": float(len(dropped)),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning(
            "chronic_gap_non_constituent_count metric emit failed: %s — "
            "drift alarm cadence may degrade until next cycle.", exc,
        )

    return summary


# Hard wall-clock bound for the chronic-gap self-heal (L4605). Generous enough
# never to false-positive a legitimately-slow all-4-stale heal (~4 tickers ×
# (yf.download ≤30s + backfill) ≈ 6 min worst case), but finite so an INFINITE
# network hang in yf.download / backfill is converted to a bounded best-effort
# skip rather than running forever. On the WEEKDAY pipeline the heal is its own
# fail-soft SF state with a 300s SSM timeout (which fires first); on the SATURDAY
# pipeline the heal runs INLINE inside MorningEnrich (5400s SSM budget), so this
# in-process bound is the one that actually prevents an infinite heal hang from
# SIGKILLing the load-bearing Saturday MorningEnrich. Chosen over a separate
# Saturday SF state because the Saturday SF launches a fresh spot instance per
# state — a spot-per-4-ticker-heal would be wasteful (2026-06-11 decision).
_CHRONIC_HEAL_HARD_TIMEOUT_S = 600

# Hard bound for the prior-universe-gap self-heal. Historically ran at the
# head of the MorningArcticAppend state (40-min SSM budget) — REMOVED from that
# state (alpha-engine-config-I2717, 2026-07-16; the standalone --daily-heal
# entrypoint below is the sole remaining caller). Retained UNCHANGED at 1500s
# per the I2717 build instruction in case a future caller ever needs the
# tighter (critical-path-safe) bound again; the standalone daily heal instead
# uses the much larger _UNIVERSE_GAP_HEAL_STANDALONE_HARD_TIMEOUT_S below,
# since it runs off the critical path with no daemon-start deadline pressure.
_UNIVERSE_GAP_HEAL_HARD_TIMEOUT_S = 1500
# Hard bound for the universe-gap self-heal when run from the standalone
# `--daily-heal` EventBridge-triggered job (alpha-engine-config-I2717). Off the
# preopen critical path entirely (fires ~09:00 UTC, hours before the 12:45 UTC
# preopen), so this affords a much bigger budget than the 1500s
# _UNIVERSE_GAP_HEAL_HARD_TIMEOUT_S above ever could — a multi-day backfill or
# a slow ArcticDB rewrite no longer risks delaying daemon start. Config-
# overridable via config["universe_gap_heal"]["standalone_hard_timeout_s"].
_UNIVERSE_GAP_HEAL_STANDALONE_HARD_TIMEOUT_S = 3600
# How many trading days back to scan for a missing universe append, and how
# many to heal per run. Default heals only the single most-recent missing day
# so one run stays comfortably inside the 40-min append budget; a multi-day
# outage chips away one day per subsequent weekday run (and the executor's
# gap-aware reconcile + freshness monitor cover the interim). Both overridable
# via config["universe_gap_heal"].
_UNIVERSE_GAP_HEAL_LOOKBACK_TD = 5
_UNIVERSE_GAP_HEAL_MAX_PER_RUN = 1

# config#2672 (Brian-ratified binding design, 2026-07-15): a durable
# desired-state ledger so a fallback-quality (yfinance-basis) trading day can
# NEVER age out unhealed — the bug this issue exists to fix. The sliding
# -window detectors above (``_detect_missing_universe_days``,
# ``_detect_fallback_quality_universe_days``) only look back
# ``lookback_trading_days`` sessions; a day that misses every heal attempt
# within that window (e.g. repeated Polygon rate-limiting) silently ages out
# and is never revisited again. This ledger removes that ceiling structurally
# rather than widening the window (which would just move the ceiling, not
# remove it): every successful yfinance-basis (fallback-quality) EOD write
# marks its date here; every successful polygon-corrected write (morning
# MorningArcticAppend, and the gap-heal path itself) clears it. The reader
# (``_self_heal_missing_universe_days``) UNIONS this ledger with both
# existing sliding-window detectors — belt-and-braces: a ledger read/write
# failure degrades to today's in-window-only behavior, never below it.
#
# Single small JSON object keyed by trading_day, touched at most twice/day —
# plain S3 read-modify-write is adequate (mirrors ARTIFACT_REGISTRY
# conventions); no DynamoDB needed. Best-effort fail-soft on the trading
# path: a ledger write failure must NEVER block or crash the daemon-path
# append (wrapped in try/except, log-and-continue at every call site) — but
# its absence is self-evident, since the sliding-window detectors above still
# independently catch in-window days regardless of ledger health.
_PENDING_UPGRADES_LEDGER_BUCKET = "alpha-engine-research"
_PENDING_UPGRADES_LEDGER_KEY = "_data_quality/pending_upgrades.json"


def _load_pending_upgrades_ledger(
    bucket: str = _PENDING_UPGRADES_LEDGER_BUCKET,
    key: str = _PENDING_UPGRADES_LEDGER_KEY,
) -> dict:
    """Read the pending-upgrades ledger — ``{trading_day: {reason, detected_at}}``.

    Best-effort: a missing object (first-ever write) or any read/parse
    failure returns ``{}`` so the reader degrades to sliding-window-only
    detection rather than raising — this ledger is a belt-and-braces
    ADDITION to the existing detectors, never a replacement dependency.
    """
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        doc = json.loads(obj["Body"].read())
        return doc if isinstance(doc, dict) else {}
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("NoSuchKey", "404"):
            logger.warning("pending-upgrades ledger read failed (%s) — "
                           "degrading to sliding-window-only detection.", exc)
        return {}
    except Exception as exc:  # noqa: BLE001 — best-effort read, never blocks the reader
        logger.warning("pending-upgrades ledger read failed (%s) — "
                       "degrading to sliding-window-only detection.", exc)
        return {}


def _write_pending_upgrades_ledger(
    doc: dict,
    bucket: str = _PENDING_UPGRADES_LEDGER_BUCKET,
    key: str = _PENDING_UPGRADES_LEDGER_KEY,
) -> None:
    """Best-effort PUT of the full ledger document. Never raises — a write
    failure is logged and swallowed (fail-soft on the trading path)."""
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(doc, sort_keys=True, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001 — fail-soft: never blocks the trading-path append
        logger.warning("pending-upgrades ledger write failed (%s) — "
                       "in-window sliding-window detection remains the backstop.", exc)


def _mark_pending_upgrade(
    trading_day: str,
    reason: str = "fallback_quality",
    bucket: str = _PENDING_UPGRADES_LEDGER_BUCKET,
    key: str = _PENDING_UPGRADES_LEDGER_KEY,
) -> None:
    """Read-modify-write: mark ``trading_day`` as needing the Polygon-corrected
    upgrade. Called on every successful yfinance-basis (fallback-quality) EOD
    write (:func:`_run_daily_arctic_append`). Idempotent (overwrites any
    existing entry for the same day) and fail-soft — never raises."""
    try:
        doc = _load_pending_upgrades_ledger(bucket, key)
        doc[trading_day] = {
            "reason": reason,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_pending_upgrades_ledger(doc, bucket, key)
        logger.info("pending-upgrades ledger: marked %s (%s).", trading_day, reason)
    except Exception as exc:  # noqa: BLE001 — fail-soft: never blocks the daemon-path append
        logger.warning("pending-upgrades ledger: failed to mark %s (%s) — non-blocking.",
                       trading_day, exc)


def _clear_pending_upgrade(
    trading_day: str,
    bucket: str = _PENDING_UPGRADES_LEDGER_BUCKET,
    key: str = _PENDING_UPGRADES_LEDGER_KEY,
) -> None:
    """Read-modify-write: clear ``trading_day`` from the ledger on a
    successful Polygon-corrected write (the morning load-bearing append, or
    the gap-heal path itself). A day absent from the ledger is a no-op
    (nothing to clear). Fail-soft — never raises."""
    try:
        doc = _load_pending_upgrades_ledger(bucket, key)
        if trading_day in doc:
            del doc[trading_day]
            _write_pending_upgrades_ledger(doc, bucket, key)
            logger.info("pending-upgrades ledger: cleared %s.", trading_day)
    except Exception as exc:  # noqa: BLE001 — fail-soft: never blocks the daemon-path append
        logger.warning("pending-upgrades ledger: failed to clear %s (%s) — non-blocking.",
                       trading_day, exc)


class _HardTimeout(BaseException):
    """Raised by :func:`_hard_timeout` on SIGALRM expiry. Subclasses
    BaseException (not Exception) so the per-ticker ``except Exception`` inside
    the self-heal loop does NOT swallow it — the alarm aborts the whole heal
    immediately and propagates to the caller, which logs + continues
    (best-effort). Mirrors how KeyboardInterrupt escapes broad excepts."""


@contextmanager
def _hard_timeout(seconds: int, label: str):
    """SIGALRM-based hard wall-clock bound for a best-effort block.

    Main-thread only (weekly_collector runs as the main thread under both
    ``--morning-enrich`` and ``--chronic-gap-heal``). Raises :class:`_HardTimeout`
    on expiry. No-op (yields without arming) if SIGALRM is unavailable — not the
    main thread, or a non-POSIX platform — so callers behave identically minus
    the bound. SIGALRM interrupts blocking syscalls (socket recv), so it bounds
    a hung network fetch, not just CPU-bound loops.
    """
    def _handler(signum, frame):
        raise _HardTimeout(f"{label} exceeded {seconds}s hard timeout")

    try:
        previous = signal.signal(signal.SIGALRM, _handler)
    except (ValueError, AttributeError):
        # Not in the main thread, or SIGALRM unavailable — run unbounded.
        yield
        return
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _self_heal_chronic_polygon_gaps(
    bucket: str,
    target_date: str,
    chronic_tickers: list[str],
    dry_run: bool = False,
) -> dict:
    """Yfinance-backfill any ArcticDB row gap for chronic-polygon-gap tickers.

    For each ticker in ``chronic_tickers``:
      1. Read ArcticDB universe ``last_date``.
      2. If ``last_date >= target_date``, skip (already fresh — common case
         after the first heal lands).
      3. Else yfinance-fetch ``[last_date+1, target_date]`` OHLCV.
      4. Append the new rows to ``predictor/price_cache/{ticker}.parquet``
         (dedupe by date keep="last" so repeated heals are idempotent). Wave 3
         PR1 mirrors the put to ``reference/price_cache/{ticker}.parquet`` via
         the ``_price_cache_write_prefixes`` helper.
      5. Invoke ``builders.backfill(ticker_filter=ticker)`` — reuses the
         per-ticker compute_features + ArcticDB write path so the new
         rows get the same feature schema as every other ticker.

    Closes the multi-day rot caused by polygon never serving the chronic
    gaps + the EOD yfinance pass occasionally dropping a day. Origin:
    2026-05-09 weekly SF DataPhase1 postflight failure — PSTG ended at
    5/5 (ArcticDB) vs SPY at 5/8 (3d stale, > 2d threshold), every other
    chronic ticker at 5/6 (2d, just under threshold). Without this step
    the only recovery was hand-running a yfinance backfill script.

    Idempotent: tickers already at target_date are skipped, so re-running
    after a partial completion costs only the freshness reads. Best-effort
    per-ticker — one ticker's ordinary yfinance/network failure does not
    block the others. NOT best-effort for a run-level contract violation:
    a caret-prefixed ticker reaching the write-time guard
    (``CaretTickerError``) or a post-close/future bar
    (``dates.FutureBarError``) propagates out of this loop rather than
    being folded into ``summary["errors"]`` — both are population/clock
    bugs upstream of any one ticker's fetch, and the caller
    (``_run_chronic_gap_self_heal_step`` / weekly_collector's
    `chronic_gap_self_heal` state) already has a broad handler that
    records the step as ``status: error`` without failing the pipeline
    (alpha-engine-config-I10904).

    Returns a summary dict with per-ticker outcomes; the caller should
    log it (not raise) so a yfinance hiccup on a chronic gap doesn't
    halt the whole pipeline. Postflight will catch any remaining staleness
    via its uniform check.
    """
    import io as _io

    import yfinance as _yf

    from store.arctic_store import get_universe_lib

    s3 = boto3.client("s3")
    universe_lib = get_universe_lib(bucket)
    target_ts = __import__("pandas").Timestamp(target_date).normalize()

    summary: dict = {
        "checked": len(chronic_tickers),
        "healed": [],
        "skipped_already_fresh": [],
        "errors": [],
    }

    if not chronic_tickers:
        return summary

    import pandas as _pd

    for ticker in chronic_tickers:
        try:
            try:
                df_tail = universe_lib.tail(ticker, n=1).data
                existing_last = (
                    _pd.Timestamp(df_tail.index[-1]).normalize()
                    if df_tail is not None and not df_tail.empty
                    else None
                )
            except Exception:
                existing_last = None

            if existing_last is not None and existing_last >= target_ts:
                summary["skipped_already_fresh"].append(
                    {"ticker": ticker, "last_date": str(existing_last.date())}
                )
                continue

            start_ts = (
                (existing_last + _pd.Timedelta(days=1))
                if existing_last is not None
                else (target_ts - _pd.Timedelta(days=30))
            )
            end_excl = target_ts + _pd.Timedelta(days=1)

            with quiet_yfinance():
                yf_df = _yf.download(
                    ticker,
                    start=start_ts.strftime("%Y-%m-%d"),
                    end=end_excl.strftime("%Y-%m-%d"),
                    progress=False,
                    auto_adjust=True,
                    # Bound the network call so a hung yfinance fetch can't stall
                    # the heal indefinitely. The 2026-06-11 incident was an
                    # unbounded yf.download here; the SF state isolation is the
                    # primary fix, this is defence-in-depth so a single ticker's
                    # stall is capped rather than eating the whole state timeout.
                    timeout=30,
                )
            if isinstance(yf_df.columns, _pd.MultiIndex):
                yf_df.columns = yf_df.columns.get_level_values(0)

            yf_df = yf_df[(yf_df.index >= start_ts) & (yf_df.index <= target_ts)]
            if yf_df.empty:
                summary["errors"].append(
                    {"ticker": ticker, "reason": "yfinance returned no rows in target range"}
                )
                continue

            ohlcv_cols = ["Open", "High", "Low", "Close", "Volume"]
            new_rows = yf_df[[c for c in ohlcv_cols if c in yf_df.columns]].copy()

            # Wave 3 PR4 (cutover): read via the read-prefix chain (now
            # ``reference/price_cache/`` only) for the existing-rows union,
            # then write back to the write-prefix chain (also ``reference/``
            # only). Post-cutover both chains resolve to the reference home;
            # the legacy ``predictor/price_cache/`` tree is removed live via
            # ``aws s3 rm`` (see builders/_price_cache_writeboth.py).
            existing_pcache = _pd.DataFrame(columns=ohlcv_cols)
            for _read_prefix in _price_cache_read_prefixes():
                try:
                    obj = s3.get_object(
                        Bucket=bucket, Key=f"{_read_prefix}{ticker}.parquet"
                    )
                    existing_pcache = _pd.read_parquet(
                        _io.BytesIO(obj["Body"].read())
                    )
                    break
                except s3.exceptions.NoSuchKey:
                    continue

            combined_pcache = _pd.concat([existing_pcache, new_rows])
            combined_pcache = combined_pcache[
                ~combined_pcache.index.duplicated(keep="last")
            ].sort_index()

            if not dry_run:
                try:
                    _assert_valid_price_cache_ticker(ticker)
                except ValueError as _caret_exc:
                    # I10904: this site's only handler (`except Exception as
                    # exc` below) was folding a caret-ticker contract
                    # violation into an ordinary per-ticker self-heal
                    # failure. Re-raise as the dedicated type so the
                    # `except CaretTickerError: raise` clause below catches
                    # only this failure, not an unrelated ValueError.
                    raise CaretTickerError(str(_caret_exc)) from _caret_exc
                _assert_no_bar_after(combined_pcache.index, target_date, artifact=f"price_cache/{ticker}.parquet")  # I10893
                buf = _io.BytesIO()
                combined_pcache.to_parquet(buf, engine="pyarrow", compression="snappy")
                body = buf.getvalue()
                for _prefix in _price_cache_write_prefixes():
                    s3.put_object(
                        Bucket=bucket, Key=f"{_prefix}{ticker}.parquet", Body=body
                    )

                from builders.backfill import backfill as _backfill
                _backfill(bucket=bucket, ticker_filter=ticker, dry_run=False)

            summary["healed"].append(
                {
                    "ticker": ticker,
                    "previous_last_date": (
                        str(existing_last.date()) if existing_last is not None else None
                    ),
                    "rows_added": int(len(new_rows)),
                    "new_last_date": str(new_rows.index[-1].date()),
                }
            )
            logger.info(
                "chronic-gap self-heal: %s healed (prev=%s → new=%s, +%d rows)",
                ticker,
                existing_last.date() if existing_last is not None else "none",
                new_rows.index[-1].date(),
                len(new_rows),
            )
        except _FutureBarError:
            raise  # run-level contract violation, never a per-ticker miss (I10893)
        except CaretTickerError:
            raise  # run-level contract violation, never a per-ticker miss (I10904)
        except Exception as exc:
            logger.exception("chronic-gap self-heal failed for %s", ticker)
            summary["errors"].append({"ticker": ticker, "reason": str(exc)})

    return summary


def _daily_closes_written_keys(
    dc_result: dict, s3_prefix: str, target_date: str
) -> list[str]:
    """The ``staging/daily_closes/{date}.parquet`` key(s) a
    ``daily_closes.collect`` call actually wrote.

    `alpha-engine-config-I10942` deliverable 3. ``morning_daily_closes``'s
    phase marker declared ``artifact_keys: []`` on the 2026-09-16 shadow run
    while the underlying window-mode call had just written nine parquet
    files — ``_maybe_phase`` is marker-only (see its docstring) and this
    call site never told it what got written. Window mode
    (``window_days > 1``) reports each date it touched under
    ``dc_result["per_date"]``; single-date mode's ``dc_result`` IS that one
    date's own result, keyed on ``target_date``. Only dates whose own
    per-date status is ``ok``/``ok_dry_run`` are counted — a best-effort
    backfill miss (see ``_collect_window``'s target-driven fatality) did not
    write a parquet and must not be declared as if it had.
    """
    prefix = s3_prefix.rstrip("/")
    per_date = (dc_result or {}).get("per_date")
    if isinstance(per_date, dict):
        return sorted(
            f"{prefix}/{d}.parquet"
            for d, r in per_date.items()
            if isinstance(r, dict) and r.get("status") in ("ok", "ok_dry_run")
        )
    if (dc_result or {}).get("status") in ("ok", "ok_dry_run"):
        return [f"{prefix}/{target_date}.parquet"]
    return []


def _run_morning_enrich(config: dict, args: argparse.Namespace) -> dict:
    """Morning polygon enrichment: overwrite the prior trading day's parquet
    + ArcticDB row with polygon's authoritative OHLCV+VWAP.

    Called by the new MorningEnrich Lambda step in the weekday SF (and
    available via --morning-enrich for backfills). Hard-fails on any polygon
    failure — predictor inference reads ArcticDB right after this runs and
    must see polygon-corrected data, not silently-stale yfinance values.

    Skips the feature_store snapshot step (that already ran with yfinance EOD;
    re-running it is expensive and the polygon delta on OHLCV is typically <1%).
    daily_append's per-ticker compute_features call recomputes per-ticker
    features inside ArcticDB based on the polygon-overwritten row, which is
    what downstream consumers actually read.
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Compute target_date PT-aware so a Wed-evening manual rerun (UTC rolled
    # past midnight) doesn't resolve "previous trading day" to today PT — that
    # was the original 403 trap the wall-clock guard worked around.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    # Skip guard: data-staleness check. If polygon's target_date is older than
    # what's already in ArcticDB (yfinance EOD already landed today's row),
    # skip so we don't overwrite newer data with older. See
    # _should_skip_morning_enrich() for full rationale. Explicit --date
    # bypasses the guard so operator-driven backfills still work.
    if args.date is None:
        arctic_last_date = _arctic_spy_last_date(bucket)
        skip, reason = _should_skip_morning_enrich(target_date, arctic_last_date)
        if skip:
            logger.info(
                "Skipping MorningEnrich: %s. Would have targeted %s.",
                reason, target_date,
            )
            return {
                "mode": "morning_enrich",
                "status": "skipped",
                "skip_reason": reason,
                "would_have_targeted": target_date,
                "started_at": started_at,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "collectors": {},
            }
    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    # Registry date = target_date (the prior trading day MorningEnrich enriches),
    # so markers land under data/{target_date}/.phases/. None in dry-run.
    reg = _build_registry(config, args, target_date)

    results: dict = {
        "mode": "morning_enrich",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    # ── Pre-flight: refresh constituents + prune ArcticDB stragglers ─────────
    # Order matters. Without these, MorningEnrich loads last week's
    # constituents.json (Phase 1's writer hasn't run yet this Saturday), so
    # any S&P churn-out from the past week is invisible — the ticker stays
    # in the request list, polygon doesn't have it (now-delisted), and the
    # downstream missing-from-closes + freshness checks have to defend
    # against the drift via ``expected_tickers`` scoping (PR #132 + PR #133
    # are exactly that defense). Refreshing constituents + pruning here
    # makes the universe coherent BEFORE any check fires, so the bandages
    # become a quiet no-op rather than the load-bearing path.
    #
    # 2026-05-02 redrive #4 (after PR #132/#133 shipped) is the validation
    # window: with the reorder, prune drops the 8 churn-outs (ASGN, GTM,
    # HOLX, KMPR, LW, MOH, MTCH, PAYC) before MorningEnrich's writes;
    # downstream checks see a coherent universe without needing the
    # ``expected_tickers`` scoping at all.
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    if not dry_run:
        logger.info("=" * 60)
        logger.info("REFRESHING: constituents (pre-MorningEnrich)")
        logger.info("=" * 60)
        try:
            with _maybe_phase(reg, "morning_constituents"):
                cons_result = constituents.collect(
                    bucket=bucket,
                    s3_prefix=market_prefix,
                    run_date=default_run_date(),  # config#1014: trading-day axis
                    dry_run=False,
                )
            results["collectors"]["constituents_preflight"] = cons_result
            tickers = cons_result.get("tickers", [])
            logger.info(
                "Pre-MorningEnrich constituents refresh: %d tickers", len(tickers),
            )
        except Exception as exc:
            logger.exception("Pre-MorningEnrich constituents refresh failed")
            results["collectors"]["constituents_preflight"] = {
                "status": "error", "error": str(exc),
            }
            results["status"] = "failed"
            results["completed_at"] = datetime.now(timezone.utc).isoformat()
            return results

        logger.info("=" * 60)
        logger.info("PRUNING: ArcticDB universe stragglers (pre-MorningEnrich)")
        logger.info("=" * 60)
        try:
            from builders.prune_delisted_tickers import prune_delisted_tickers
            # Use absent_days=5 to match the post-write freshness scan
            # threshold — consistent with what daily_append considers
            # "stale enough to drop". The post-Phase-1 prune still runs
            # later with the conservative 14d default for any newcomers
            # the SF picked up between MorningEnrich and Phase 1.
            with _maybe_phase(reg, "morning_prune"):
                prune_result = prune_delisted_tickers(
                    bucket=bucket,
                    absent_days=5,
                    apply=True,
                    constituents_override=set(tickers),
                )
            results["collectors"]["prune_preflight"] = prune_result
            logger.info(
                "Pre-MorningEnrich prune: pruned %d stragglers (skipped_recent=%d)",
                prune_result.get("pruned_count", 0),
                prune_result.get("skipped_recent_count", 0),
            )
        except Exception as exc:
            # Prune failure is non-fatal here — the bandage scoping in
            # daily_append (PR #132/#133) still tolerates stragglers, and
            # the post-Phase-1 prune gets another shot. Surface it
            # loudly per feedback_no_silent_fails — operator should
            # investigate, but the SF can complete tonight on the
            # bandages alone.
            #
            # Recorded under top-level ``prune_preflight_warning`` rather
            # than ``results["collectors"]`` because the per-collector
            # status aggregator treats any ``error`` entry there as a
            # whole-pipeline failure. The prune side-effect is best-effort
            # observability, not a blocking step.
            logger.error(
                "Pre-MorningEnrich prune failed: %s. Falling through to "
                "MorningEnrich; daily_append's expected_tickers scoping "
                "will still tolerate stragglers, but please investigate "
                "the prune failure.", exc,
            )
            results["prune_preflight_warning"] = {
                "status": "error", "error": str(exc),
            }
    else:
        # Dry-run: skip side effects but still load tickers for the
        # downstream daily_closes call.
        try:
            existing = constituents.load_from_s3(bucket, market_prefix)
            tickers = existing.get("tickers", []) if existing else []
        except Exception:
            tickers = []
        if not tickers:
            try:
                tickers, _, _, _, _, _, _ = constituents._fetch_constituents()
            except Exception as exc:
                logger.error("Wikipedia constituents fallback failed: %s", exc)

    if not tickers:
        logger.error("No tickers available for morning enrichment")
        results["status"] = "failed"
        return results

    # Shared with _load_daily_universe_tickers / _run_daily_arctic_append
    # (the EOD PostMarketArcticAppend twin of this weekday MorningEnrich
    # path) — was a SEPARATE inline copy of the same 17-symbol list until
    # config-I2703 (2026-07-15): two independently-editable literals of
    # "the macro/benchmark daily tickers" is exactly the duplicate-pin
    # drift class (one copy gets updated, the other silently doesn't).
    # Both call sites now derive from the single module-level
    # _MACRO_DAILY_TICKERS constant so SPY (and any future addition) can't
    # drift out of one path while staying in the other. NOTE: kept as the
    # direct constant reference (not routed through the newer
    # _augment_with_macro_daily_tickers helper added for config#2898) — this
    # exact literal is pinned by
    # test_morning_enrich_and_daily_arctic_append_share_one_macro_ticker_list's
    # `"_MACRO_DAILY_TICKERS" in inspect.getsource(...)` source-text guard.
    tickers = list(dict.fromkeys(tickers + _MACRO_DAILY_TICKERS))

    logger.info("=" * 60)
    logger.info("MORNING ENRICH: polygon overwrite for %s (prior trading day)", target_date)
    logger.info("=" * 60)
    # Windowed-reconciliation knobs from config — default window_days=1
    # preserves legacy single-date overwrite. When set to N > 1, polygon
    # makes one grouped-daily call per BDay in the window (N total —
    # bounded by free-tier rate limit). Polygon ignores skip_if_canonical
    # per option (a): re-overwrites within the window to absorb
    # corporate-action backfills.
    window_days = int(daily_cfg.get("window_days", 1))
    skip_if_canonical = bool(daily_cfg.get("skip_if_canonical", False))
    daily_closes_s3_prefix = daily_cfg.get("s3_prefix", "staging/daily_closes/")
    try:
        with _maybe_phase(reg, "morning_daily_closes") as dc_ctx:
            dc_result = daily_closes.collect(
                bucket=bucket,
                tickers=tickers,
                run_date=target_date,
                s3_prefix=daily_closes_s3_prefix,
                dry_run=dry_run,
                source="polygon_only",
                window_days=window_days,
                skip_if_canonical=skip_if_canonical,
            )
            # `alpha-engine-config-I10942`. `daily_closes.collect` in window
            # mode does NOT raise on a target-date failure — `_collect_window`
            # returns `status="error"` as a VALUE so a best-effort backfill
            # miss on an older date never aborts the run ("Fatality is
            # TARGET-driven" in that function). `_maybe_phase` is marker-only
            # (see its docstring) and records `ok` unless this block raises,
            # so a returned failure has to be re-raised HERE or the phase
            # marker says `ok` on a run whose own trading day never got
            # written — exactly the 2026-09-16 shadow shape. Mirrors
            # `_phase_collect`'s own `result.get("status") == "error"` check.
            dc_status = (dc_result or {}).get("status")
            if dc_status not in ("ok", "ok_dry_run", "skipped"):
                raise _CollectorError(
                    "morning_daily_closes",
                    run_units.describe_mode_failure("morning_daily_closes", dc_result),
                    dc_result,
                )
            if dc_ctx is not None and not dry_run:
                # Declare what this phase actually wrote — an empty
                # `artifact_keys` on a phase that wrote nine parquet files is
                # a false declaration (I10942 deliverable 3).
                for written_key in _daily_closes_written_keys(
                    dc_result, daily_closes_s3_prefix, target_date
                ):
                    dc_ctx.record_artifact(written_key)
        results["collectors"]["daily_closes"] = dc_result
        # ── Vendor cross-check: no silent non-measurement ────────────────────
        # alpha-engine-config-I10783 / nousergon-data-PR1712. The divergence
        # MetricRecord is written INSIDE `daily_closes.collect` on the
        # `polygon_only` path — the one moment this run's fresh polygon closes
        # and the prior parquet's yfinance closes for the SAME settled date are
        # both in hand. That call is gated on a non-empty prior-rows set, and a
        # morning with no comparison set would otherwise write nothing at all,
        # which makes "we did not measure" and "we measured and it was fine"
        # the same silence (`champion-challenger-policy` §7.2: measurement is
        # unconditional). So when the collector wrote no record, D17 writes the
        # UNMEASURABLE one — `compute_vendor_divergence` reports exactly that
        # for an empty comparison set, and never raises.
        if not dry_run and not (dc_result or {}).get("vendor_divergence"):
            try:
                from collectors.cross_source_observer import write_vendor_divergence_metric

                write_vendor_divergence_metric(bucket, {}, {}, target_date)
            except Exception as exc:  # noqa: BLE001
                # Swallow rationale (repo fail-loud rule): (a) the failure mode
                # swallowed is a failed PUT of the divergence METRIC, never a
                # data write; (b) the primary deliverable — polygon's
                # authoritative OHLCV+VWAP overwriting the yfinance row the
                # predictor reads minutes later — is already complete and
                # unaffected; (c) the recording surface is this ERROR line, which
                # reaches flow-doctor and the morning run's log capture, and the
                # metric's own key stays absent, which its freshness clause reads
                # as a miss. Raising here would trade a missing metric for a
                # failed morning enrich.
                logger.error(
                    "vendor_divergence UNMEASURABLE record failed to write for %s: %s — "
                    "divergence is unrecorded for this run",
                    target_date, exc,
                )
    except Exception as e:
        logger.exception("Morning polygon enrichment failed for %s", target_date)
        results["collectors"]["daily_closes"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    # ── Re-append to ArcticDB so the polygon-overwritten row replaces the
    # yfinance row written at EOD. universe_lib.update() is idempotent for
    # same-date overwrites (see daily_append.py:232-242 for the design intent).
    #
    # daily_append is the SLOW part of MorningEnrich (~20-38 min on the
    # t3.small). 2026-06-11: a same-morning rerun's append exceeded the 1800s
    # SSM executionTimeout and SIGKILLed MorningEnrich (L4608). ``--skip-arctic-append``
    # decouples it on the WEEKDAY pipeline, where it runs as its OWN skip-gated,
    # load-bearing SF state (``MorningArcticAppend``, via ``_run_morning_arctic_append``)
    # with a longer timeout AFTER the fast fetch — so (a) the append's duration
    # can no longer time out the fetch, and (b) a recovery rerun can skip the
    # completed fetch and re-run only the append (or vice-versa). The SATURDAY
    # pipeline still runs it INLINE here (no skip flag): the append must precede
    # DataPhase1's postflight, same rationale as the inline chronic-gap heal.
    if not getattr(args, "skip_arctic_append", False):
        logger.info("=" * 60)
        logger.info("APPENDING: ArcticDB universe (morning enrich, %s)", target_date)
        logger.info("=" * 60)
        try:
            from builders.daily_append import daily_append
            with _maybe_phase(reg, "morning_arcticdb"):
                arctic_result = daily_append(
                    date_str=target_date,
                    bucket=bucket,
                    dry_run=dry_run,
                    expected_tickers=tickers,
                )
            results["collectors"]["arcticdb"] = arctic_result
        except Exception as e:
            logger.exception("ArcticDB daily_append (morning enrich) failed for %s", target_date)
            results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}

    # ── Chronic-polygon-gap self-heal ────────────────────────────────────────
    # The chronic-gap drift-detection + yfinance self-heal logic lives in
    # ``_run_chronic_gap_heal`` (shared). It heals BF-B/BRK-B/MOG-A/PSTG row
    # gaps (polygon doesn't reliably serve them) before downstream consumers
    # read ArcticDB.
    #
    # ``--skip-chronic-heal`` decouples it from MorningEnrich on the WEEKDAY
    # pipeline, where it runs as its own fail-soft SF state (``ChronicGapSelfHeal``)
    # AFTER MorningEnrich. Origin: 2026-06-11 — an unbounded ``yf.download`` hang
    # in the inline heal ran out MorningEnrich's SSM ``executionTimeout`` and
    # SIGKILLed the whole command, discarding ~20 min of completed daily_append
    # and failing the weekday pipeline. Splitting it (per the standing rule: a
    # best-effort downstream step must never force re-running a completed
    # upstream task) means a heal hang can no longer touch the load-bearing
    # data write.
    #
    # The SATURDAY pipeline still runs the heal INLINE here (no skip flag): it
    # must precede DataPhase1's postflight, whose freshness gate is the heal's
    # origin (2026-05-09 stale-PSTG failure). The ``yf.download(timeout=30)``
    # bound now caps the hang that the inline path can't otherwise isolate;
    # splitting Saturday's heal into its own state too is a tracked follow-up.
    if not getattr(args, "skip_chronic_heal", False):
        heal = _run_chronic_gap_heal(config, args)
        results["collectors"].update(heal.get("collectors", {}))

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    results["status"] = "ok" if all(s in ("ok", "ok_dry_run") for s in statuses) else "failed"

    # Refresh the `daily_data` health stamp on daily_closes success.
    #
    # Health gate is decoupled from arcticdb daily_append status — the field
    # is named `daily_data` and represents the canonical OHLCV write state,
    # which is what daily_closes produces. ArcticDB append is a downstream
    # consumer of the same data; its failure mode (slow lib.write rewrites,
    # universe drift) is separate and surfaces via the arcticdb collector's
    # own status entry in `results["collectors"]["arcticdb"]`.
    #
    # Pre-2026-05-05 the gate required all collectors ok. Symptom: morning
    # polygon overwrite landed (parquet timestamp 6:21 AM PT, VWAP populated)
    # but health stayed stale on yesterday's EOD yfinance write because
    # arcticdb append failed silently in this Lambda invocation. The
    # `health/daily_data.json` consumer (executor staleness gate, dashboard
    # ingestion-attribution panel) needs the polygon row counts to reflect
    # ingestion truth — the arcticdb failure isn't theirs to gate on.
    #
    # Without this the executor's 26h staleness gate trips on Monday mornings
    # (post-close stamp is from Friday ~13h before market close → ~65h on
    # Monday open). Only write on daily_closes success — a failed
    # daily_closes must leave the prior stamp in place so the gate fires
    # correctly. Post-close DailyData run still writes the canonical stamp;
    # this is a refresh after the polygon overwrite.
    if not dry_run:
        _dc = results["collectors"].get("daily_closes", {})
        _dc_status = _dc.get("status", "unknown")
        if _dc_status in ("ok", "ok_dry_run"):
            try:
                _morning_duration = (
                    datetime.fromisoformat(results["completed_at"])
                    - datetime.fromisoformat(results["started_at"])
                ).total_seconds()
            except Exception:
                _morning_duration = 0.0
            _write_module_health(
                bucket,
                module_name="daily_data",
                run_date=target_date,
                status="ok",
                summary={
                    "tickers_captured": _dc.get("tickers_captured", 0),
                    "polygon": _dc.get("polygon", 0),
                    "fred": _dc.get("fred", 0),
                    "yfinance": _dc.get("yfinance", 0),
                    "morning_enrich": True,
                },
                duration_seconds=_morning_duration,
            )

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        # CARVE-OUT (alpha-engine-config-I10226): (a) failure mode
        # swallowed — `results["started_at"]`/`["completed_at"]` missing or
        # unparseable. (c) no recording surface — this only formats a
        # cosmetic duration suffix for the `logger.info` summary line right
        # below; it writes no data and feeds no downstream consumer, so a
        # blank `duration` is the correct degraded rendering, not a
        # corruption. See `.debug-swallow-allowlist.yaml`.
        pass
    logger.info(
        "Morning enrichment %s for %s: %s%s",
        results["status"].upper(), target_date,
        ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
        duration,
    )
    return results


def _detect_missing_universe_days(
    bucket: str,
    target_date: str,
    lookback_trading_days: int = _UNIVERSE_GAP_HEAL_LOOKBACK_TD,
) -> list[str]:
    """Trading days strictly before ``target_date`` absent from ArcticDB.

    A skipped weekday/EOD Step Function leaves a hole in the universe: no
    daily_append ran for that session, so neither the universe tickers nor
    the macro keys have a row for it. The next day's EOD reconcile then reads
    a *non-adjacent* prior close and mislabels a multi-session move as one day
    (the 2026-06-24 halt → RGEN +14.92% on 06-25; config#1228).

    Reference for "did the append run for day D": the macro/SPY index. The
    macro keys are a fixed list written on *every* ``daily_append``, so a day
    present for SPY is a day the universe append ran — and an interior hole in
    SPY's index is exactly an interior universe gap. (A per-ticker constituent
    would give false gaps on reconstitution churn; the fixed macro keys do
    not.) ``target_date`` itself is owned by the same run's load-bearing
    append, so it is excluded here.

    Returns the missing days as ``YYYY-MM-DD`` strings, NEWEST first (the most
    recent gap is the one closest to poisoning the next reconcile). Best-effort:
    a reference-read failure returns ``[]`` so the heal is a no-op and the
    load-bearing append proceeds unguarded (freshness monitor remains the
    backstop).
    """
    from datetime import date as _date

    import pandas as pd

    from nousergon_lib.trading_calendar import previous_trading_day

    target = _date.fromisoformat(target_date)
    # Expected: the N trading sessions strictly before target_date.
    expected: list[_date] = []
    d = previous_trading_day(target)
    for _ in range(lookback_trading_days):
        expected.append(d)
        d = previous_trading_day(d)

    try:
        from store.arctic_store import get_macro_lib

        macro_lib = get_macro_lib(bucket)
        # Pull a little beyond the window so an end-of-series gap (several
        # recent days all missing) still has present anchors to compare to.
        df = macro_lib.tail("SPY", n=lookback_trading_days + 6).data
        present = {pd.Timestamp(ix).date() for ix in df.index}
    except Exception as exc:  # best-effort reference read
        logger.warning(
            "universe-gap detect: macro/SPY index read failed (%s) — "
            "skipping prior-gap heal; freshness monitor remains the backstop.",
            exc,
        )
        return []

    missing = [d for d in expected if d not in present]
    missing.sort(reverse=True)  # newest first
    return [d.strftime("%Y-%m-%d") for d in missing]


def _detect_fallback_quality_universe_days(
    bucket: str,
    target_date: str,
    lookback_trading_days: int = _UNIVERSE_GAP_HEAL_LOOKBACK_TD,
) -> list[str]:
    """Trading days strictly before ``target_date`` PRESENT in ArcticDB but
    still on EOD's yfinance-fallback quality — never received the next
    morning's Polygon VWAP-corrected overwrite (alpha-engine-config#2664).

    Every trading day is written twice: EOD same-evening writes a
    yfinance-sourced row (``source="yfinance"``, ``VWAP=NaN``); the next
    morning's MorningArcticAppend is supposed to overwrite it with a
    Polygon-sourced row (``source="polygon"``). ``_detect_missing_universe_days``
    only catches a day with NO row at all — if the Polygon overwrite fails
    (e.g. a rate limit; 2026-07-15 incident) the day is PRESENT, so that
    detector never flags it, and nothing else in the pipeline ever revisits
    a day that already has a row. This closes that hole using the same
    fixed-key SPY proxy as the missing-day detector (avoids
    reconstitution-churn false positives) — but reads it from the
    **universe** library, not macro. SPY exists in both: ``macro_lib`` holds
    only a bare ``Close`` reference copy (no ``source``/OHLCV — that's all
    ``_detect_missing_universe_days`` needs, since it only checks index
    presence); the full ``OHLCV_COLS + [PROVENANCE_COL] + FEATURES`` schema
    with ``source`` lives in ``universe_lib`` (verified live 2026-07-15:
    ``macro_lib.tail("SPY").data`` columns == ``['Close']`` only — reading
    ``source`` off it would silently always return ``[]``).

    Returns the affected days as ``YYYY-MM-DD`` strings, newest first.
    Best-effort: any read failure, or a schema without a ``source`` column
    (pre-provenance-tagging history), returns ``[]`` — degrades to
    missing-day-only healing rather than mis-flagging.
    """
    from datetime import date as _date

    import pandas as pd

    from nousergon_lib.trading_calendar import previous_trading_day

    target = _date.fromisoformat(target_date)
    expected: list[_date] = []
    d = previous_trading_day(target)
    for _ in range(lookback_trading_days):
        expected.append(d)
        d = previous_trading_day(d)

    try:
        from store.arctic_store import get_universe_lib

        universe_lib = get_universe_lib(bucket)
        df = universe_lib.tail("SPY", n=lookback_trading_days + 6).data
    except Exception as exc:  # best-effort reference read
        logger.warning(
            "universe-gap detect: universe/SPY source read failed (%s) — "
            "skipping fallback-quality heal; freshness monitor remains the backstop.",
            exc,
        )
        return []

    if "source" not in df.columns:
        return []

    by_date = {pd.Timestamp(ix).date(): src for ix, src in df["source"].items()}

    fallback_days = [
        d for d in expected
        if d in by_date and pd.notna(by_date[d]) and str(by_date[d]).lower() != "polygon"
    ]
    fallback_days.sort(reverse=True)  # newest first
    return [d.strftime("%Y-%m-%d") for d in fallback_days]


def _self_heal_missing_universe_days(
    bucket: str,
    target_date: str,
    config: dict,
    dry_run: bool = False,
    lookback_trading_days: int = _UNIVERSE_GAP_HEAL_LOOKBACK_TD,
    max_heal_days: int = _UNIVERSE_GAP_HEAL_MAX_PER_RUN,
) -> dict:
    """Backfill trading days missing OR fallback-quality in the ArcticDB
    universe (config#1228; fallback-quality healing added config#2664).

    Two distinct conditions are healed through the same path, most-severe
    first (missing days, then present-but-fallback-quality days), each
    newest-first within its own group, capped at ``max_heal_days`` combined
    per run:
      - **Missing**: no row at all for that day (a skipped weekday/EOD SF).
      - **Fallback-quality**: EOD wrote a yfinance-sourced row, but the next
        morning's Polygon VWAP-correction pass never landed (e.g. a rate
        limit) — the day is present, so nothing else ever revisits it
        (see :func:`_detect_fallback_quality_universe_days`).

    config#2672: the two detectors above only scan the last
    ``lookback_trading_days`` sessions — a day that misses healing on every
    attempt within that sliding window silently ages out and is never
    revisited again (the bug this issue exists to fix). The durable
    ``_data_quality/pending_upgrades.json`` ledger (best-effort marked by
    every yfinance-basis EOD write, cleared by every polygon-corrected write)
    is UNIONED in here as a third source — belt-and-braces: a ledger read
    failure degrades to the two window detectors' existing (in-window)
    coverage, never below it. A ledger day already covered by the window scan
    is de-duplicated; a ledger day OUTSIDE the window (the actual case this
    ledger exists for) is still healed.

    For each day to heal:
      1. Resolve that day's constituents via ``load_constituents_for_run_date``
         (run_date-direct, #468-correct — never the latest_weekly pointer),
         plus the fixed macro keys.
      2. Stage OHLCV from Polygon's immutable T+1 grouped-daily for that date
         (``daily_closes.collect``; ``auto`` source so FRED/yfinance backfill
         any Polygon gaps — completeness matters more than VWAP purity here).
      3. Splice the day into ArcticDB via ``daily_append(date_str=day)`` — an
         idempotent mid-series insert. ``skip_if_exists`` defaults to False
         (not passed here), so a fallback-quality day's existing row is
         overwritten exactly like MorningEnrich's own polygon-over-yfinance
         overwrite — no separate write path needed for the two conditions.

    Best-effort and per-day isolated: one day's failure is recorded and the
    loop continues. NEVER raises — returns a summary the caller logs. The
    same-day load-bearing append is the caller's responsibility and must not
    be blocked by a heal failure here.
    """
    summary: dict = {
        "scan_window_td": lookback_trading_days,
        "max_per_run": max_heal_days,
        "missing_days": [],
        "fallback_quality_days": [],
        "ledger_days": [],
        "healed_days": [],
        "deferred_days": [],
        "errors": [],
    }

    missing = _detect_missing_universe_days(bucket, target_date, lookback_trading_days)
    fallback_quality = _detect_fallback_quality_universe_days(
        bucket, target_date, lookback_trading_days
    )
    summary["missing_days"] = missing
    summary["fallback_quality_days"] = fallback_quality

    # config#2672: the durable ledger, newest-first — the third (unbounded
    # -horizon) source. Best-effort read: a ledger failure returns {} and this
    # degrades to the two window detectors above, never below their coverage.
    ledger = _load_pending_upgrades_ledger(bucket)
    ledger_days = sorted(ledger.keys(), reverse=True)
    summary["ledger_days"] = ledger_days

    # Mutually exclusive by construction between missing/fallback_quality
    # (fallback-quality requires an existing row; missing requires none) — de
    # -dup there is defensive only. The ledger is NOT mutually exclusive with
    # either (a day already caught by the window scan is also very likely
    # ledger-marked) — de-dup against both. Missing days are the more severe
    # gap, so they get healed first; ledger-only days (already outside the
    # window) come last since the window-scan days are the fresher signal.
    combined = (
        missing
        + [d for d in fallback_quality if d not in missing]
        + [d for d in ledger_days if d not in missing and d not in fallback_quality]
    )
    if not combined:
        logger.info(
            "universe-gap heal: no prior trading-day gaps, fallback-quality "
            "rows, or ledger-pending upgrades before %s.",
            target_date,
        )
        return summary

    to_heal = combined[:max_heal_days]
    summary["deferred_days"] = combined[max_heal_days:]
    logger.warning(
        "universe-gap heal: %d day(s) need healing before %s — %d missing (%s), "
        "%d fallback-quality (%s), %d ledger-pending (%s); healing %s this run%s.",
        len(combined), target_date, len(missing), missing,
        len(fallback_quality), fallback_quality, len(ledger_days), ledger_days, to_heal,
        f", deferring {summary['deferred_days']} to subsequent runs" if summary["deferred_days"] else "",
    )

    from builders._constituents_loader import load_constituents_for_run_date
    from builders.daily_append import UniverseFreshnessViolation, daily_append
    from collectors import daily_closes

    daily_cfg = config.get("daily_closes", {})
    s3_prefix = daily_cfg.get("s3_prefix", "staging/daily_closes/")
    s3 = boto3.client("s3")

    for day in to_heal:
        if day in missing:
            kind = "missing"
        elif day in fallback_quality:
            kind = "fallback_quality"
        else:
            kind = "ledger"  # config#2672: aged out of both sliding windows, ledger-only
        try:
            try:
                tickers_set, weekly_date = load_constituents_for_run_date(
                    s3, bucket, run_date=day
                )
            except Exception as direct_exc:
                logger.warning(
                    "universe-gap heal: direct constituents read for %s failed (%s) — "
                    "falling back to latest_weekly pointer.",
                    day, direct_exc,
                )
                tickers_set, weekly_date = load_constituents_for_run_date(
                    s3, bucket, run_date=None
                )
            tickers = sorted(tickers_set)
            # Mirror MorningEnrich's universe scope: constituents + macro keys.
            tickers = list(dict.fromkeys(tickers + _MACRO_DAILY_TICKERS))
            if not tickers:
                summary["errors"].append(
                    {"date": day, "kind": kind, "reason": "no constituents resolved"}
                )
                continue

            # 1+2: stage the day's OHLCV (Polygon T+1, auto-chain fallback).
            daily_closes.collect(
                bucket=bucket,
                tickers=tickers,
                run_date=day,
                s3_prefix=s3_prefix,
                dry_run=dry_run,
                source="auto",
            )
            # 3: idempotent mid-series splice into ArcticDB.
            append_result = daily_append(
                date_str=day,
                bucket=bucket,
                dry_run=dry_run,
                expected_tickers=tickers,
            )
        except UniverseFreshnessViolation as exc:
            # config#2685: this is only ever raised by the post-write
            # whole-universe freshness scan, which runs after the
            # per-ticker append loop + error-rate gate have both already
            # succeeded — so `day`'s own write landed. An unrelated stale
            # ticker elsewhere in the universe must not misreport this
            # day's heal as failed; it's a monitored signal, not a heal
            # error (config-I2684 — the 2026-07-15 backfill's actually-
            # successful writes were unreadable as `errors` in this exact
            # summary because of this conflation).
            summary["healed_days"].append(
                {
                    "date": day,
                    "kind": kind,
                    "tickers": len(tickers),
                    "weekly_date": str(weekly_date),
                }
            )
            logger.warning(
                "universe-gap heal: backfilled %s (%s, %d tickers) — target-date "
                "write ok, but %d unrelated universe symbol(s) elsewhere are "
                "stale (monitored separately, not a heal failure): %s",
                day, kind, len(tickers), len(exc.stale_symbols),
                [s["symbol"] for s in exc.stale_symbols][:10],
            )
        except Exception as exc:
            logger.warning("universe-gap heal: failed to backfill %s (%s, %s).", day, kind, exc)
            summary["errors"].append({"date": day, "kind": kind, "reason": str(exc)})
        else:
            status = append_result.get("status", "unknown")
            if status in ("ok", "ok_dry_run"):
                summary["healed_days"].append(
                    {
                        "date": day,
                        "kind": kind,
                        "tickers": len(tickers),
                        "weekly_date": str(weekly_date),
                    }
                )
                logger.info(
                    "universe-gap heal: backfilled %s (%s, %d tickers, status=%s).",
                    day, kind, len(tickers), status,
                )
                # config#2672: this path always writes the polygon-corrected
                # (auto-source) row (see daily_closes.collect(source="auto")
                # above) — clear the ledger entry, if any, on every successful
                # heal regardless of `kind` (a day can be ledger-marked AND
                # independently caught by the window scan). Fail-soft, never
                # blocks this already-successful heal.
                if not dry_run:
                    _clear_pending_upgrade(day, bucket)
            else:
                summary["errors"].append(
                    {"date": day, "kind": kind, "reason": f"daily_append status={status}"}
                )
                logger.warning(
                    "universe-gap heal: daily_append for %s (%s) returned status=%s.",
                    day, kind, status,
                )

    return summary


def _run_daily_heal(config: dict, args: argparse.Namespace) -> dict:
    """Standalone daily data-heal workload (alpha-engine-config-I2717).

    Brian ruling 2026-07-16 (I2717 + the shared I2722 scope note): the
    universe-gap self-heal (missing/fallback-quality days, config#1228 +
    config#2664) and the chronic-polygon-gap heal (config#1811's
    ``ChronicGapSelfHeal``, folded in here per the ruling — "the new daily
    heal run bundles the ChronicGapSelfHeal logic too") both move OFF the
    ``ne-preopen-trading-pipeline`` critical path entirely into this single
    EventBridge-triggered daily spot job (~09:00 UTC, hours before the
    12:45 UTC preopen so heals still land same-day ahead of inference).
    Freed from the preopen's tight poll budget, this affords each heal a much
    larger timeout than either could safely spend inline (1500s for the
    universe-gap heal at the head of MorningArcticAppend; 300s SSM budget for
    ChronicGapSelfHeal on the trading box).

    Dispatched via the data-spot-dispatcher Lambda's ``daily-heal`` workload
    on its own ephemeral spot box — see
    ``infrastructure/lambdas/data-spot-dispatcher/index.py``.

    Both heals stay fail-soft / best-effort INTERNALLY exactly as before (a
    single stale ticker or a single bad day must never abort the whole run) —
    see :func:`_self_heal_missing_universe_days` and
    :func:`_run_chronic_gap_heal`, both of which are documented to NEVER
    raise. This function does not additionally swallow their output; if
    either ever starts raising unexpectedly (a genuine regression, not a
    per-item failure) that propagates out of this function, out of
    ``run_weekly()``, and crashes ``main()`` with a nonzero exit — the
    MODE-level fail-loud posture the build spec requires ("exits nonzero if
    the heal infrastructure itself blows up"). DataPreflight (wired in
    ``main()`` for this mode, same surface as ``--daily``) is the first line
    of defense: a genuinely unreachable S3/ArcticDB fails loud there, before
    any heal work starts.

    Emits the ``AlphaEngine/Data daily_heal_days_healed`` CloudWatch metric
    EVERY run (continuous baseline, including 0 — matches the existing
    chronic-gap drift-alarm convention in this file) and writes a heal-summary
    artifact to ``data/heal/daily/{run_date}.json`` — the artifact the
    freshness-monitor plane watches (per the I2722 health-plane ruling: no new
    bundled health SF, extend the existing Lambda watch plane instead).
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Same PT-aware target-date resolution as MorningEnrich/MorningArcticAppend
    # (the day being healed is the prior trading day relative to this run).
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    dry_run = args.dry_run
    results: dict = {
        "mode": "daily_heal",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    # ── Universe-gap heal (config#1228 + config#2664; formerly the head of
    # MorningArcticAppend) — bigger, config-overridable hard-timeout budget
    # now that this runs off the critical path.
    ugh_cfg = config.get("universe_gap_heal", {})
    standalone_timeout_s = int(
        ugh_cfg.get(
            "standalone_hard_timeout_s", _UNIVERSE_GAP_HEAL_STANDALONE_HARD_TIMEOUT_S
        )
    )
    try:
        with _hard_timeout(standalone_timeout_s, "standalone universe-gap self-heal"):
            heal_summary = _self_heal_missing_universe_days(
                bucket=bucket,
                target_date=target_date,
                config=config,
                dry_run=dry_run,
                lookback_trading_days=int(
                    ugh_cfg.get("lookback_trading_days", _UNIVERSE_GAP_HEAL_LOOKBACK_TD)
                ),
                max_heal_days=int(
                    ugh_cfg.get("max_heal_days_per_run", _UNIVERSE_GAP_HEAL_MAX_PER_RUN)
                ),
            )
        results["collectors"]["universe_gap_heal"] = {"status": "ok", **heal_summary}
    except _HardTimeout as e:
        logger.warning(
            "standalone universe-gap self-heal hit the %ds hard timeout for "
            "%s — skipping (best-effort). %s",
            standalone_timeout_s, target_date, e,
        )
        _record_swallowed_step("universe_gap_heal", f"hard timeout after {standalone_timeout_s}s: {e}")
        results["collectors"]["universe_gap_heal"] = {"status": "skipped", "error": str(e)}
    except Exception as e:
        logger.exception("standalone universe-gap self-heal failed for %s (non-blocking)", target_date)
        results["collectors"]["universe_gap_heal"] = {"status": "error", "error": str(e)}

    # ── Chronic-polygon-gap heal (config#1811's ChronicGapSelfHeal logic,
    # folded in here per the I2717 ruling) — reuses _run_chronic_gap_heal
    # verbatim (same yfinance-backfill + polygon-recovery/constituents-drift
    # alarms), just invoked from this standalone entrypoint instead of the
    # weekday SF's on-trading-box state. Not wrapped in an extra try/except:
    # _run_chronic_gap_heal's own docstring commits to NEVER raising (its
    # entire body is a defense-in-depth wrapper already); an unjustified
    # extra swallow here would only hide a genuine regression from the
    # MODE-level fail-loud posture this function documents above.
    chronic_result = _run_chronic_gap_heal(config, args)
    results["collectors"]["chronic_gap_heal"] = chronic_result

    # ── Metric: continuous baseline of actual heal work performed (Brian's
    # "loud alert whenever the healer actually does heal-work" ruling). Counts
    # BOTH the universe-gap days healed and the chronic-gap tickers healed —
    # either kind firing is a real data-quality event worth seeing. Always
    # emits (including 0) — best-effort, mirrors the existing chronic-gap
    # drift-alarm metrics in this file (_detect_chronic_gap_polygon_recovery /
    # _detect_chronic_gap_constituents_drift).
    universe_days_healed = len(
        results["collectors"]["universe_gap_heal"].get("healed_days", [])
    )
    chronic_tickers_healed = len(
        chronic_result.get("collectors", {})
        .get("chronic_gap_self_heal", {})
        .get("healed", [])
    )
    days_healed = universe_days_healed + chronic_tickers_healed
    try:
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="AlphaEngine/Data",
            MetricData=[{
                "MetricName": "daily_heal_days_healed",
                "Value": float(days_healed),
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.warning(
            "daily_heal_days_healed metric emit failed: %s — the "
            "days-healed alarm cadence may degrade until next cycle.", exc,
        )

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    results["days_healed"] = days_healed
    # Best-effort mode overall: neither sub-heal raises (see above), so this
    # always reports "ok" — matching _run_chronic_gap_heal's own "always ok"
    # semantics. A genuine infrastructure blowup escapes as an uncaught
    # exception instead (see the fail-loud note in the docstring above), never
    # as a status="failed" return.
    results["status"] = "ok"

    # Heal-summary artifact — the freshness-monitor plane's watch target
    # (I2722 health-plane ruling). Deliberately NOT wrapped in a try/except:
    # an inability to write this IS the "cannot reach S3 at all" case the
    # MODE-level fail-loud posture must catch, so a write failure propagates
    # and exits nonzero rather than silently leaving the freshness monitor
    # blind to this run ever having happened.
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=f"data/heal/daily/{target_date}.json",
        Body=json.dumps(results, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Daily heal complete for %s: universe_days_healed=%d "
        "chronic_tickers_healed=%d (metric daily_heal_days_healed=%d) → "
        "s3://%s/data/heal/daily/%s.json",
        target_date, universe_days_healed, chronic_tickers_healed, days_healed,
        bucket, target_date,
    )
    return results


def _run_morning_arctic_append(config: dict, args: argparse.Namespace) -> dict:
    """Standalone ArcticDB universe append for the prior trading day (L4608).

    Split out of :func:`_run_morning_enrich` (2026-06-11) into its own weekday-SF
    state (``MorningArcticAppend``). MorningEnrich now does only the fast fetch
    (constituents refresh + prune + polygon daily_closes overwrite, via
    ``--skip-arctic-append``); this runs the SLOW ``daily_append`` that writes
    the polygon-corrected row + recomputed features into the ArcticDB universe
    library.

    Why split: ``daily_append`` ran ~20-38 min on the t3.small and on 2026-06-11
    a same-morning rerun exceeded MorningEnrich's 1800s SSM ``executionTimeout``
    and SIGKILLed it. As its own state the append gets a longer timeout decoupled
    from the fetch, and a recovery rerun can skip whichever half already
    completed (``skip_morning_enrich`` / ``skip_morning_arctic_append``) instead
    of re-paying both.

    LOAD-BEARING: predictor inference reads the ArcticDB universe right after
    this, so an append failure returns ``status="failed"`` → ``main()`` exits 1
    → the SF's ``CheckMorningArcticAppendStatus`` routes to HandleFailure. Reads
    the constituents MorningEnrich just refreshed to S3 (so the expected-ticker
    scope matches the post-prune universe).
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Same PT-aware target-date resolution as MorningEnrich, so the append
    # targets the prior trading day MorningEnrich just fetched.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    dry_run = args.dry_run
    results: dict = {
        "mode": "morning_arctic_append",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    # Prior-gap self-heal (config#1228) REMOVED from this state (alpha-engine-
    # config-I2717, 2026-07-16): it moved off the preopen critical path entirely
    # into the standalone `--daily-heal` EventBridge-triggered run (see
    # `_run_daily_heal` below), which has a much larger (3600s default, config-
    # overridable) hard-timeout budget than the 1500s this state could spare
    # without risking today's load-bearing append. MorningArcticAppend is now a
    # pure append — no heal work runs inline here.

    # Load the constituents MorningEnrich just refreshed to S3 (post-prune
    # universe scope for daily_append's expected-ticker check). S3 → Wikipedia
    # fallback, mirroring _run_daily.
    #
    # MUST read THIS run's dated constituents directly, NOT the
    # ``latest_weekly.json`` pointer. The pointer only advances on the weekly
    # (Saturday) ``_write_manifest``; daily MorningEnrich writes the dated
    # ``weekly/{run_date}/constituents.json`` but leaves the pointer alone. So
    # a pointer-following read (``constituents.load_from_s3``) returns the
    # PRIOR weekly universe — and on an S&P-reconstitution week that universe
    # still lists the churn-out tickers. They are absent from today's
    # daily_closes (collected against the fresh universe), so daily_append's
    # missing-from-closes guard counts them as a data gap and halts the SF.
    # (2026-06-25: pointer stuck at 2026-06-19; BLKB/BRBR/CNXC/COTY/CPB/POOL/
    # SATS dropped in the 06-22 reconstitution → 7 > threshold 5 → halt.)
    # The straggler-exclusion in daily_append only works when expected_tickers
    # is the FRESH universe; reading by run_date restores that invariant.
    # Same pointer-vs-direct-read TOCTOU defect class closed for backfill/prune
    # via ``builders._constituents_loader``; this is the third in-repo reader.
    tickers: list[str] = []
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    # Run date == the date MorningEnrich wrote constituents under
    # (``run_date = args.date or default_run_date()`` in _run_morning_enrich,
    # config#1014 trading-day axis). Mirror that expression exactly so we read
    # the file this run produced — NOT target_date, which is the prior trading
    # day the append ROW is keyed on.
    run_date = args.date or default_run_date()
    try:
        from builders._constituents_loader import load_constituents_for_run_date
        s3 = boto3.client("s3")
        try:
            tickers_set, weekly_date = load_constituents_for_run_date(
                s3, bucket, run_date=run_date
            )
            tickers = sorted(tickers_set)
            logger.info(
                "Loaded %d tickers from S3 constituents (run_date=%s direct)",
                len(tickers), weekly_date,
            )
        except Exception as direct_exc:
            # Standalone append rerun on a day MorningEnrich didn't run (no
            # dated file). Fall back to the pointer — stale-but-present beats
            # empty; the straggler-exclusion degrades gracefully to the prior
            # weekly universe, same as before this fix.
            logger.warning(
                "Direct constituents read for run_date=%s failed (%s) — "
                "falling back to latest_weekly.json pointer",
                run_date, direct_exc,
            )
            tickers_set, weekly_date = load_constituents_for_run_date(
                s3, bucket, run_date=None
            )
            tickers = sorted(tickers_set)
            logger.info(
                "Loaded %d tickers from S3 constituents (pointer→%s fallback)",
                len(tickers), weekly_date,
            )
    except Exception as exc:
        logger.warning("S3 constituents load failed — will try Wikipedia fallback: %s", exc)
    if not tickers:
        try:
            tickers, _, _, _, _, _, _ = constituents._fetch_constituents()
            logger.info("Loaded %d tickers from Wikipedia (S3 fallback)", len(tickers))
        except Exception as exc:
            logger.error("Wikipedia constituents fallback failed: %s", exc)

    if not tickers:
        logger.error("No tickers available for ArcticDB append")
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    # config#2898: union with _MACRO_DAILY_TICKERS here — the evening twin
    # (_run_daily_arctic_append, via _load_daily_universe_tickers) already
    # did this, but this morning path never did, so SPY (and the other macro
    # tickers) were silently absent from expected_tickers on every morning
    # run regardless of which of the three branches above populated
    # `tickers`. Applied post-branch so it covers the direct-read,
    # pointer-fallback, AND Wikipedia-fallback paths uniformly.
    tickers = _augment_with_macro_daily_tickers(tickers)

    logger.info("=" * 60)
    logger.info("APPENDING: ArcticDB universe (arctic-append state, %s)", target_date)
    logger.info("=" * 60)
    reg = _build_registry(config, args, target_date)
    try:
        from builders.daily_append import daily_append
        with _maybe_phase(reg, "morning_arcticdb"):
            arctic_result = daily_append(
                date_str=target_date,
                bucket=bucket,
                dry_run=dry_run,
                expected_tickers=tickers,
            )
        results["collectors"]["arcticdb"] = arctic_result
        # daily_append returns its own status; surface it as the load-bearing
        # verdict so a write failure halts the pipeline (predictor reads next).
        _status = arctic_result.get("status", "unknown")
        results["status"] = "ok" if _status in ("ok", "ok_dry_run") else "failed"
        if results["status"] == "ok" and not dry_run:
            # config#2672: this IS the polygon-corrected write — clear
            # target_date from the pending-upgrades ledger on success (a
            # no-op if it was never marked). Fail-soft, never blocks the
            # already-successful load-bearing append.
            _clear_pending_upgrade(target_date, bucket)
    except Exception as e:
        logger.exception("ArcticDB daily_append (arctic-append state) failed for %s", target_date)
        results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("ArcticDB append %s for %s", results["status"].upper(), target_date)
    return results


def _run_chronic_gap_heal(config: dict, args: argparse.Namespace) -> dict:
    """Best-effort chronic-polygon-gap drift detection + yfinance self-heal.

    Split out of :func:`_run_morning_enrich` (2026-06-11) into its own
    weekday-SF state (``ChronicGapSelfHeal``) — that dedicated SF state was
    REMOVED entirely (alpha-engine-config-I2717, 2026-07-16); this function
    itself is unchanged and is now called from two places: inline from
    :func:`_run_morning_enrich` (the Saturday path, unaffected by I2717) and
    from the new standalone :func:`_run_daily_heal` (the weekday path's
    replacement for the old ChronicGapSelfHeal state). Yfinance-backfills any
    ArcticDB universe row gap for the chronic-gap tickers (BF-B/BRK-B/MOG-A/
    PSTG by default — see config; polygon does not reliably serve them) and
    emits the polygon-recovery + constituents-drift alarms.

    Runs AFTER MorningEnrich's load-bearing daily_append, as a fail-soft SF
    state, so an unbounded ``yf.download`` hang here (the 2026-06-11 SIGKILL
    incident) can never run out MorningEnrich's SSM ``executionTimeout`` and
    throw away completed daily_append work. The standing rule: a best-effort
    downstream step must never force re-running a completed upstream task.

    NEVER raises — the whole body is wrapped so that any unexpected failure
    returns ``status="error"`` rather than propagating a non-zero exit that
    the SF would (correctly, but pointlessly here) treat as a state failure.
    The SF Catch makes a failed state non-fatal regardless; this is
    defence-in-depth so the SSM command itself exits 0. Postflight remains the
    load-bearing freshness gate — a still-stale chronic ticker surfaces there.
    """
    bucket = config["bucket"]
    started_at = datetime.now(timezone.utc).isoformat()

    # Mirror _run_morning_enrich's PT-aware target-date resolution so the heal
    # targets the same trading day the enrich just wrote.
    if args.date is None:
        from zoneinfo import ZoneInfo
        target_date = _previous_trading_day(
            reference=datetime.now(ZoneInfo("America/Los_Angeles"))
        )
    else:
        target_date = args.date

    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    results: dict = {
        "mode": "chronic_gap_heal",
        "date": target_date,
        "started_at": started_at,
        "collectors": {},
    }

    try:
        chronic_tickers = _load_chronic_polygon_gaps(config)
        if not chronic_tickers:
            logger.info("chronic-gap heal: no chronic_polygon_gaps configured — nothing to do.")
            results["status"] = "ok"
            results["completed_at"] = datetime.now(timezone.utc).isoformat()
            return results

        # Drift alarm: detect polygon recovery for chronic tickers (BEFORE
        # self-heal so the signal is a clean read of what polygon shipped
        # today, not contaminated by our yfinance backfill). Best-effort,
        # observability only — never raises.
        try:
            drift_result = _detect_chronic_gap_polygon_recovery(
                bucket=bucket,
                target_date=target_date,
                chronic_tickers=chronic_tickers,
                daily_closes_prefix=daily_cfg.get("s3_prefix", "staging/daily_closes/"),
            )
            results["collectors"]["chronic_gap_drift_detection"] = drift_result
        except Exception as e:
            logger.warning("Chronic-gap drift detection failed (non-blocking): %s", e)
            _record_swallowed_step("chronic_gap_drift_detection", str(e))
            results["collectors"]["chronic_gap_drift_detection"] = {
                "status": "skipped",
                "error": str(e),
            }

        # Drift GATE: filter out chronic tickers that have dropped out of
        # the current constituents set. The heal path ends in
        # ``backfill(ticker_filter=...)``, which hard-errs against the
        # constituents filter for non-constituents (2026-05-27 PSTG
        # flow-doctor alert origin). Filtering here closes the loop so a
        # config that lags a constituents change becomes a WARN + skip
        # instead of a hard ERROR. Best-effort — a read failure falls
        # through with the original list and the existing backfill-side
        # error remains the load-bearing surface.
        try:
            cdrift_result = _detect_chronic_gap_constituents_drift(
                bucket=bucket,
                chronic_tickers=chronic_tickers,
            )
            results["collectors"]["chronic_gap_constituents_drift"] = cdrift_result
            chronic_tickers = cdrift_result["still_constituents"]
        except Exception as e:
            logger.warning("Chronic-gap constituents drift check failed (non-blocking): %s", e)
            _record_swallowed_step("chronic_gap_constituents_drift", str(e))
            results["collectors"]["chronic_gap_constituents_drift"] = {
                "status": "skipped",
                "error": str(e),
            }

        logger.info("=" * 60)
        logger.info(
            "SELF-HEAL: chronic polygon coverage gaps (%d ticker(s): %s)",
            len(chronic_tickers), ", ".join(chronic_tickers),
        )
        logger.info("=" * 60)
        try:
            # Hard wall-clock bound (L4605): yf.download carries timeout=30 but
            # the heal's builders.backfill() call is an unbounded second network
            # path. This watchdog caps the WHOLE per-ticker heal loop so an
            # infinite hang becomes a bounded best-effort skip — the
            # load-bearing surface stays MorningEnrich (weekday: a separate
            # fail-soft SF state; Saturday: DataPhase1's postflight), never a
            # SIGKILL of the inline Saturday MorningEnrich.
            with _hard_timeout(_CHRONIC_HEAL_HARD_TIMEOUT_S, "chronic-gap self-heal"):
                heal_result = _self_heal_chronic_polygon_gaps(
                    bucket=bucket,
                    target_date=target_date,
                    chronic_tickers=chronic_tickers,
                    dry_run=dry_run,
                )
            # Always "ok" by design — chronic-gap self-heal is best-effort.
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "ok",
                **heal_result,
            }
            logger.info(
                "chronic-gap self-heal: %d healed, %d already-fresh, %d errors",
                len(heal_result["healed"]),
                len(heal_result["skipped_already_fresh"]),
                len(heal_result["errors"]),
            )
        except _HardTimeout as e:
            # Best-effort: a hung heal must not fail the pipeline. Postflight
            # (Saturday) catches any still-stale chronic ticker as the loud gate.
            logger.warning(
                "Chronic-gap self-heal hit the %ds hard timeout for %s — "
                "skipping (best-effort); postflight remains the freshness gate. %s",
                _CHRONIC_HEAL_HARD_TIMEOUT_S, target_date, e,
            )
            _record_swallowed_step(
                "chronic_gap_self_heal",
                f"hard timeout after {_CHRONIC_HEAL_HARD_TIMEOUT_S}s: {e}",
            )
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "skipped",
                "error": str(e),
            }
        except Exception as e:
            logger.exception("Chronic-gap self-heal step failed for %s", target_date)
            results["collectors"]["chronic_gap_self_heal"] = {
                "status": "error",
                "error": str(e),
            }
    except Exception as e:  # defence-in-depth — this state must exit 0
        logger.exception("Chronic-gap heal wrapper failed for %s", target_date)
        results["collectors"]["chronic_gap_heal_wrapper"] = {
            "status": "error",
            "error": str(e),
        }

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    # Best-effort step: report ok unless the whole thing fell over. Per-ticker /
    # per-substep failures are recorded in collectors but do not flip the state
    # to failed (the SF Catch makes it non-fatal either way).
    results["status"] = "ok"
    statuses = {
        k: v.get("status", "?") for k, v in results["collectors"].items()
    }
    logger.info("Chronic-gap heal complete for %s: %s", target_date, statuses)
    return results


# Macro symbols are not S&P constituents but are core daily predictor inputs
# (vix_level, vix_term_slope, yield_10y, yield_curve_slope, sector-relative
# features). Appending them lets builders/daily_append.py update the ArcticDB
# macro library every weekday — pre-ArcticDB, the predictor Lambda fetched these
# from yfinance on each run; post-migration, the write path moved here. ETFs
# come from polygon; indices (^-prefix) fall through to FRED then yfinance in
# daily_closes.collect. Shared by the EOD --daily collector and the split-out
# --daily-arctic-append state so both pass daily_append the SAME expected_tickers.
_MACRO_DAILY_TICKERS = [
    # alpha-engine-config-I10704 follow-up: every member of
    # ``features.compute.UNIVERSE_BENCHMARK_PROXIES`` must be requested here,
    # or the day's close never reaches staging/daily_closes and daily_append
    # has no bar to write. IWM was declared (nousergon-data-PR1694) and loaded
    # once in-region, but was absent from this list, so 2026-09-14's
    # staging/daily_closes held no IWM row and crucible `data.daily` refused
    # the session. Enforced by
    # tests/test_benchmark_proxies_i10704.py::test_declared_proxies_are_requested_daily.
    "SPY", "IWM", "GLD", "USO",
    "XLB", "XLC", "XLE", "XLF", "XLI", "XLK",
    "XLP", "XLRE", "XLU", "XLV", "XLY",
    # config#934 — sub-sector benchmark ETFs (SMH/IGV/XBI/PPH/XOP/KRE/ITA/GDX),
    # the distinct non-XL* symbols in constituents.GICS_SUBINDUSTRY_TO_ETF.
    # Their daily closes must land in staging/daily_closes so daily_append can
    # write their bars to ArcticDB macro for the sub_sector_vs_benchmark_*
    # features. Additive: unlike the XL* set, a missing bar here is non-fatal
    # in daily_append (the feature neutral-defaults) — see daily_append.
    "SMH", "IGV", "XBI", "PPH", "XOP", "KRE", "ITA", "GDX",
    "^VIX", "^VIX3M", "^TNX", "^IRX",
]


def _augment_with_macro_daily_tickers(tickers: list[str]) -> list[str]:
    """Union ``tickers`` with :data:`_MACRO_DAILY_TICKERS`, de-duplicated.

    config#2898: every constructor of a ``daily_append``/``expected_tickers``
    input must route through this ONE function, not re-inline the union —
    two independent inline copies is exactly how the morning-arctic-append
    caller silently diverged from the evening one and dropped SPY out of its
    expected-ticker scope while the evening path kept it. Routing all callers
    through here guarantees the macro tickers (SPY included) reach
    ``expected_tickers`` regardless of which entrypoint constructed the list."""
    return list(dict.fromkeys(tickers + _MACRO_DAILY_TICKERS))


def _load_daily_universe_tickers(config: dict) -> list[str]:
    """Load the daily universe (S3 constituents → Wikipedia fallback) plus the
    macro daily tickers. Shared by :func:`_run_daily` and
    :func:`_run_daily_arctic_append` so a split EOD run (PostMarketData computes,
    PostMarketArcticAppend appends) feeds daily_append the identical
    expected-ticker scope. Returns ``[]`` when no constituents are resolvable —
    callers treat that as a hard failure."""
    tickers: list[str] = []
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    try:
        existing = constituents.load_from_s3(config["bucket"], market_prefix)
        if existing:
            tickers = existing.get("tickers", [])
            logger.info("Loaded %d tickers from S3 constituents", len(tickers))
    except Exception as exc:
        logger.warning("S3 constituents load failed — will try Wikipedia fallback: %s", exc)
    if not tickers:
        try:
            tickers, _, _, _, _, _, _ = constituents._fetch_constituents()
            logger.info("Loaded %d tickers from Wikipedia (S3 fallback)", len(tickers))
        except Exception as exc:
            logger.error("Wikipedia constituents fallback failed: %s", exc)
    if not tickers:
        return []
    return _augment_with_macro_daily_tickers(tickers)


def _run_daily(config: dict, args: argparse.Namespace) -> dict:
    """Daily mode: capture today's OHLCV closes for all tracked tickers, plus
    (config#2756) refresh reference/price_cache/*.parquet for any ticker that
    missed a trading session — keeps the cache daily-fresh instead of the
    prior Saturday-only cadence."""
    bucket = config["bucket"]
    run_date = args.date or default_run_date()
    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})
    price_cfg = config.get("price_cache", {})
    reg = _build_registry(config, args, run_date)

    results: dict = {
        "mode": "daily",
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    tickers = _load_daily_universe_tickers(config)
    if not tickers:
        logger.error("No tickers available for daily closes")
        results["status"] = "failed"
        return results

    logger.info("=" * 60)
    logger.info("COLLECTING: daily closes")
    logger.info("=" * 60)
    dc_started_at = datetime.now(timezone.utc)
    # Windowed-reconciliation knobs from config — default window_days=1
    # preserves single-date legacy behavior. When N > 1, the EOD yfinance
    # pass scans the last N BDays, filling NaN cells per the source-
    # precedence ladder. With skip_if_canonical=true, tickers that
    # already have an authoritative source skip the yfinance fetch
    # entirely so the batch cost stays near zero in steady state.
    window_days = int(daily_cfg.get("window_days", 1))
    skip_if_canonical = bool(daily_cfg.get("skip_if_canonical", False))
    _dc_prefix = daily_cfg.get("s3_prefix", "staging/daily_closes/")
    # EOD pass uses yfinance_only — polygon free-tier 403's same-day, and
    # silently substituting yfinance was the 2026-04-17 → 2026-04-23 VWAP
    # outage. Morning polygon_only enrichment (separate SF step) fills VWAP
    # the next morning by overwriting these rows with polygon's authoritative
    # OHLCV+VWAP. See module docstring on collectors/daily_closes.py.
    results["collectors"]["daily_closes"] = _phase_collect(
        reg, "daily_closes",
        lambda: daily_closes.collect(
            bucket=bucket,
            tickers=tickers,
            run_date=run_date,
            s3_prefix=_dc_prefix,
            dry_run=dry_run,
            source="yfinance_only",
            window_days=window_days,
            skip_if_canonical=skip_if_canonical,
        ),
        artifact_key=f"{_dc_prefix}{run_date}.parquet",
        # config-I2702 deliverable #2: this collector's artifact is the EOD
        # post-market-data data-spot workload's whole contracted output — the
        # PostMarketArcticAppend state (and the reconcile-only replay's
        # skip_post_market_data=false path) read this exact parquet. A
        # collector that reports status=ok without the file actually landing
        # on S3 must not be treated as success.
        verify_artifact_exists=True,
        bucket=bucket,
    )

    # ── Price cache refresh (config#2756 — daily cadence) ───────────────────
    # reference/price_cache/*.parquet previously only refreshed on the
    # Saturday DataPhase1 run, so metron_market_data.collect_history's
    # 1-trading-day price_cache staleness gate (data#693) only cleared on
    # Monday; Tue-Fri the whole universe fell back to an independent
    # yfinance fetch. Running the same collector here (Mon-Fri EOD) keeps
    # the cache within `daily_staleness_threshold_days` trading sessions on
    # every weekday. _find_stale_fast is trading-day-exact (config#2756), so
    # in steady state only tickers that actually missed a session (new
    # listings, corporate actions, a prior-day miss) pay the full 10y
    # yfinance rewrite — most weekdays this list is empty.
    if bool(price_cfg.get("daily_enabled", True)):
        logger.info("=" * 60)
        logger.info("COLLECTING: price cache (daily)")
        logger.info("=" * 60)
        results["collectors"]["prices"] = _phase_collect(
            reg, "prices",
            lambda: prices.collect(
                bucket=bucket,
                tickers=tickers,
                s3_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                fetch_period=price_cfg.get("fetch_period", "10y"),
                staleness_threshold_days=price_cfg.get("daily_staleness_threshold_days", 1),
                batch_size=price_cfg.get("refresh_batch_size", 50),
                dry_run=dry_run,
                reference_date=run_date,
            ),
            supports_auto_skip=False,
            # alpha-engine-config-I11026: every per-ticker parquet this run
            # actually wrote, with its own row count.
            extra_outputs=_prices_extra_outputs(
                price_cfg.get("s3_prefix", "predictor/price_cache/")
            ),
        )
        # Same unconditional sentinel the Saturday run writes (config#2350) —
        # now also written on a successful weekday refresh so the freshness
        # monitor sees the true (now-daily) cadence.
        if not dry_run and results["collectors"]["prices"].get("status") == "ok":
            _write_price_cache_freshness_sentinel(
                boto3.client("s3"), bucket,
                writer="nousergon-data:weekly_collector.py:daily",
            )

    # Metron market-data producer — EOD closes + FX for Metron's held-ticker universe.
    # `alpha-engine-data` is the single market-data ground truth for the NE system;
    # Metron reads these artifacts (it makes no direct market-data API calls). Reads its
    # own universe from s3://<bucket>/metron/holdings_universe.json (fail-soft → skipped
    # when absent), independent of the constituent `tickers` above.
    results["collectors"]["metron_market_data"] = _phase_collect(
        reg, "metron_market_data",
        lambda: metron_market_data.collect(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.CLOSES_PREFIX}{run_date}.json",
        # alpha-engine-config-I10855: D20 writes 4 keys unconditionally on
        # status="ok" — dated + latest closes, dated + latest fx
        # (collectors/metron_market_data.py::collect).
        extra_outputs=(
            (
                f"{metron_market_data.CLOSES_PREFIX}latest.json",
                lambda r: r.get("status") == "ok",
                lambda r: r.get("closes") or 0,
            ),
            (
                f"{metron_market_data.FX_PREFIX}{run_date}.json",
                lambda r: r.get("status") == "ok",
                lambda r: r.get("fx") or 0,
            ),
            (
                f"{metron_market_data.FX_PREFIX}latest.json",
                lambda r: r.get("status") == "ok",
                lambda r: r.get("fx") or 0,
            ),
        ),
    )
    # Per-symbol close-history + per-currency FX-history for Metron's NAV reconstruction +
    # as-of-date realized/dividend FX. Per-symbol keys (no single stable artifact) →
    # markers + watchdog only, no auto-skip.
    results["collectors"]["metron_market_data_history"] = _phase_collect(
        reg, "metron_market_data_history",
        lambda: metron_market_data.collect_history(bucket=bucket, run_date=run_date, dry_run=dry_run),
        supports_auto_skip=False,
        # alpha-engine-config-I10855: D21 had NO recording at all. The
        # consolidated artifact satisfies BOTH `close_history/{sym}.json`
        # (wildcard) and its own literal `close_history/consolidated.json`
        # entry — a single-path-segment file under close_history/ matches the
        # `{sym}.json` pattern too. `fx_history/{ccy}.json` has no consolidated
        # companion, so every currency actually written is recorded by name
        # (`fx_currencies` — collectors/metron_market_data.py::collect_history).
        extra_outputs=(
            (
                metron_market_data.CONSOLIDATED_CLOSE_HISTORY_KEY,
                lambda r: r.get("status") == "ok",
                lambda r: r.get("close_series") or 0,
            ),
            (
                lambda r: [
                    f"{metron_market_data.FX_HISTORY_PREFIX}{ccy}.json"
                    for ccy in (r.get("fx_currencies") or [])
                ],
                lambda r: r.get("status") == "ok" and bool(r.get("fx_currencies")),
                lambda r: 1,
            ),
        ),
    )
    # GICS sectors + SPY weights + earnings dates — Metron's last external fetches, now
    # on the spine so Metron reads ALL market/reference data from `data`.
    results["collectors"]["metron_reference_data"] = _phase_collect(
        reg, "metron_reference_data",
        lambda: metron_market_data.collect_reference(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.SECTORS_PREFIX}latest.json",
        # alpha-engine-config-I10855: D22's second key (earnings/latest.json) is
        # co-written unconditionally with sectors/latest.json on status="ok"
        # (collectors/metron_market_data.py::collect_reference).
        extra_outputs=(
            (
                f"{metron_market_data.EARNINGS_PREFIX}latest.json",
                lambda r: r.get("status") == "ok",
                lambda r: r.get("earnings") or 0,
            ),
        ),
    )
    # Macro indicators (FRED observation series) for Metron's Macro page — Metron's last
    # direct external fetch, now on the spine.
    results["collectors"]["metron_macro_data"] = _phase_collect(
        reg, "metron_macro_data",
        lambda: metron_market_data.collect_macro(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.MACRO_PREFIX}latest.json",
    )
    # Tearsheet fundamentals (multiples + balance-sheet ratios) for Metron's held
    # universe — config#1022. Daily cadence; yfinance pass-through values; the
    # 15-min intraday family (config#1023) runs OUTSIDE this pipeline via a systemd
    # timer on the trading box (infrastructure/systemd/metron-intraday.timer).
    results["collectors"]["metron_fundamentals_data"] = _phase_collect(
        reg, "metron_fundamentals_data",
        lambda: metron_market_data.collect_fundamentals(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.FUNDAMENTALS_PREFIX}latest.json",
    )
    # Technical indicators (RSI / MACD / MA / 52w range / momentum) for Metron's Holdings
    # table, computed from the close_history written by metron_market_data_history above —
    # no new fetch. Runs after history so it reads the freshly-written close series.
    results["collectors"]["metron_technicals_data"] = _phase_collect(
        reg, "metron_technicals_data",
        lambda: metron_market_data.collect_technicals(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.TECHNICALS_PREFIX}latest.json",
    )
    # Immutable daily technical-rating ledger (metron-ops#297 part 2) — self-seeding
    # backfill to 252 trading days, then one immutable date per EOD run. Derived from
    # the SAME consolidated close_history metron_market_data_history just published (no
    # new fetch); per-symbol keys (one file per date) → markers + watchdog only.
    # Writes stay under market_data/technicals/ — the SAME writer identity as
    # collect_technicals above, no new IAM grant.
    results["collectors"]["metron_rating_ledger"] = _phase_collect(
        reg, "metron_rating_ledger",
        lambda: technical_rating_ledger.collect_rating_ledger(bucket=bucket, run_date=run_date, dry_run=dry_run),
        supports_auto_skip=False,
        # alpha-engine-config-I10855: D26 had NO recording at all. The manifest
        # key (a single path segment under rating_history/) matches BOTH its
        # own literal declared entry AND the `rating_history/*` wildcard entry
        # — recorded only when the collector actually rewrote it this run
        # (`backfill_written or live_written`; a rerun with nothing new to
        # backfill/live-write, same day, legitimately does not rewrite it).
        extra_outputs=(
            (
                technical_rating_ledger.RATING_LEDGER_MANIFEST_KEY,
                lambda r: bool(r.get("backfill_written") or r.get("live_written")),
                lambda r: r.get("total_dates") or 0,
            ),
        ),
    )
    # Realized near-term performance of the rating ledger (metron-ops#297 part 2) —
    # recomputed every EOD run from the ledger + close_history. Runs after the ledger
    # phase so today's date is already written.
    results["collectors"]["metron_rating_performance"] = _phase_collect(
        reg, "metron_rating_performance",
        lambda: technical_rating_ledger.collect_rating_performance(bucket=bucket, dry_run=dry_run),
        artifact_key=technical_rating_ledger.RATING_PERFORMANCE_KEY,
    )
    # Period returns + risk stats for Metron tearsheet / Holdings LTM — derived from
    # close_history (no new fetch). Runs after history + technicals.
    results["collectors"]["metron_security_performance_data"] = _phase_collect(
        reg, "metron_security_performance_data",
        lambda: metron_market_data.collect_security_performance(
            bucket=bucket, run_date=run_date, dry_run=dry_run,
        ),
        artifact_key=f"{metron_market_data.SECURITY_PERFORMANCE_PREFIX}latest.json",
    )
    # Consensus research (rating + price targets + #analysts) for Metron's Holdings
    # Sentiment/Consensus band + per-holding attractiveness score (metron-ops#105).
    # FREE sources only (yfinance + optional Finnhub rating buckets); forward consensus
    # ESTIMATES are a paid feed scaffolded N/A in the consumer (metron-ops#107).
    results["collectors"]["metron_analyst_data"] = _phase_collect(
        reg, "metron_analyst_data",
        lambda: metron_market_data.collect_analyst(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.ANALYST_PREFIX}latest.json",
    )
    # News sentiment (held-universe latest slice of the news_aggregates_daily parquet,
    # projected to JSON) for the Holdings Sentiment/Consensus band + attractiveness
    # score (metron-ops#105). Runs after RunDailyNews has written the parquet.
    results["collectors"]["metron_sentiment_data"] = _phase_collect(
        reg, "metron_sentiment_data",
        lambda: metron_market_data.collect_sentiment(bucket=bucket, run_date=run_date, dry_run=dry_run),
        artifact_key=f"{metron_market_data.SENTIMENT_PREFIX}latest.json",
    )

    # Module health stamp for daily_data — scoped to daily_closes only. The
    # executor gate at alpha-engine/executor/main.py reads this key to decide
    # whether upstream data is fresh. Emitted on both ok and failure paths
    # so downstream can distinguish "ran and failed" from "hasn't run".
    if not dry_run:
        _dc = results["collectors"]["daily_closes"]
        _dc_status = _dc.get("status", "unknown")
        _dc_ok = _dc_status in ("ok", "ok_dry_run")
        _dc_duration = (datetime.now(timezone.utc) - dc_started_at).total_seconds()
        _write_module_health(
            bucket,
            module_name="daily_data",
            run_date=run_date,
            status="ok" if _dc_ok else "failed",
            summary={
                "tickers_captured": _dc.get("tickers_captured", 0),
                "polygon": _dc.get("polygon", 0),
                "fred": _dc.get("fred", 0),
                "yfinance": _dc.get("yfinance", 0),
            },
            error=None if _dc_ok else _dc.get("error", f"daily_closes status={_dc_status}"),
            duration_seconds=_dc_duration,
        )

    # ── Feature store compute ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("COMPUTING: feature store snapshot")
    logger.info("=" * 60)
    from features.compute import compute_and_write
    # zero_variance_fatal=False is EOD-path-only (alpha-engine-config-I7572).
    # Every other caller of compute_and_write keeps the default True, so a
    # backfill or weekly recompute still refuses to write a snapshot carrying a
    # dead column — nothing downstream is waiting on those, so refusing is the
    # right answer there. Here the snapshot IS written and the run is reported
    # `degraded`; see the status-aggregation note below for why.
    results["collectors"]["features"] = _phase_collect(
        reg, "features",
        lambda: compute_and_write(
            date_str=run_date, bucket=bucket, dry_run=dry_run,
            zero_variance_fatal=False,
        ),
        artifact_key=f"features/{run_date}/schema_version.json",
        # alpha-engine-config-I10855: D31 declares the same 5 parquet keys as
        # D12 PLUS `features/metron_supplemental/` (a prefix — any key under it
        # satisfies the pattern). The supplemental write is its own best-effort
        # step inside compute_and_write (swallowed on failure); recorded via its
        # `sectors.json` sidecar, the one key `write_metron_supplemental_snapshot`
        # writes unconditionally on every non-empty write
        # (features/metron_supplemental.py) — present only when that function
        # got PAST its early "nothing to write" return, i.e. it wrote at least
        # one group parquet or a non-empty sectors map (`{"sectors": 0}` alone
        # is that early-return sentinel, never a real write).
        extra_outputs=_feature_group_extra_outputs(run_date) + (
            (
                f"features/metron_supplemental/{run_date}/sectors.json",
                lambda r: bool(
                    {k: v for k, v in (r.get("metron_supplemental_written") or {}).items() if k != "sectors"}
                ) or ((r.get("metron_supplemental_written") or {}).get("sectors") or 0) > 0,
                lambda r: (r.get("metron_supplemental_written") or {}).get("sectors") or 0,
            ),
        ),
    )

    # ── ArcticDB daily append ────────────────────────────────────────────────
    # EOD post-market path: yfinance closes are immutable once written, so
    # re-runs short-circuit on tickers whose target-date row already lives
    # in ArcticDB. skip_if_exists=True keeps re-runs cheap (microsecond
    # in-memory check vs. 904 × ~1.5s slow lib.write rewrites — the path
    # that timed out the 2026-05-01 EOD SF rerun at the SSM 1200s ceiling).
    # MorningEnrich runs (_run_morning_enrich, polygon source) leave the
    # default False so polygon's true VWAP overwrites yfinance's NaN.
    #
    # --skip-arctic-append: the EOD SF runs the slow daily_append as its own
    # load-bearing PostMarketArcticAppend state (longer timeout decoupled from
    # the feature compute), exactly mirroring the weekday MorningEnrich +
    # MorningArcticAppend split (L4608). Without the flag (the Saturday DataPhase
    # path) the append still runs inline here. 2026-06-16: the monolithic
    # --daily run exceeded PostMarketData's 1200s SSM ceiling mid-append → SIGKILL.
    if getattr(args, "skip_arctic_append", False):
        logger.info("Skipping inline ArcticDB append (--skip-arctic-append; "
                    "runs as the EOD SF PostMarketArcticAppend state)")
    else:
        logger.info("=" * 60)
        logger.info("APPENDING: ArcticDB universe (daily)")
        logger.info("=" * 60)
        from builders.daily_append import daily_append
        # supports_auto_skip=False: ArcticDB write (no S3 key) + already cheap on
        # re-run via skip_if_exists=True → markers + watchdog only.
        results["collectors"]["arcticdb"] = _phase_collect(
            reg, "arcticdb",
            lambda: daily_append(
                date_str=run_date,
                bucket=bucket,
                dry_run=dry_run,
                skip_if_exists=True,
                expected_tickers=tickers,
            ),
            supports_auto_skip=False,
        )

    results["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Status. ``degraded`` (alpha-engine-config-I7572) is a THIRD outcome, not a
    # softer failure: the phase produced its artifact and every consumer of that
    # artifact can proceed, but something in it is known-defective. The only
    # producer of it today is the feature store's zero-variance postflight.
    #
    # It is non-fatal HERE, on the EOD daily path, for one reason: this process's
    # exit code is what the EOD Step Function reads to decide whether to run
    # LaunchPostMarketArcticAppendSpot. Exiting 1 on a features-only defect
    # therefore withholds the day's SPY close from ArcticDB, which strands the
    # freshness sentinel on the prior trading day, skips EODReconcile, and sends
    # the self-heal loop round twice on a deterministic failure before it pages.
    # That is what happened on 2026-08-17. An analytics column defect must not be
    # able to take the price-append and reconcile path down with it.
    #
    # It is NOT silent: the offending columns are logged at ERROR by
    # compute_and_write, the phase status is `degraded` in the per-collector map
    # printed below, the run's own status is `degraded`, and the health marker
    # records it. `ok` is still reserved for a genuinely clean run.
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    elif all(s in ("ok", "ok_dry_run", "degraded") for s in statuses):
        results["status"] = "degraded"
        results["degraded_collectors"] = sorted(
            k for k, v in results["collectors"].items()
            if v.get("status") == "degraded"
        )
    else:
        results["status"] = "failed"

    # Health marker. Written on degraded too, carrying the degraded status —
    # previously only an `ok` run wrote one, so a degraded day left the marker
    # absent, which reads on every freshness surface as "this never ran"
    # (principles.md §2.7: no data is never rendered as green, and it must not
    # be rendered as a different failure either).
    if not dry_run and results["status"] in ("ok", "degraded"):
        _write_health_marker(bucket, 0, run_date, results["status"])

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        # CARVE-OUT (alpha-engine-config-I10226): (a) failure mode
        # swallowed — `results["started_at"]`/`["completed_at"]` missing or
        # unparseable. (c) no recording surface — this only formats a
        # cosmetic duration suffix for the `logger.info` summary line right
        # below; it writes no data and feeds no downstream consumer, so a
        # blank `duration` is the correct degraded rendering, not a
        # corruption. See `.debug-swallow-allowlist.yaml`.
        pass
    logger.info("Daily collection %s: %s%s", results["status"].upper(),
                ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
                duration)

    return results


#: More declared members absent from the ArcticDB universe than this is not
#: an index rebalance (an S&P quarterly reconstitution moves a handful of
#: names; the 2026-09-21 change moved three), it is an outage or a wrong
#: library, and seeding hundreds of symbols one backfill at a time inside the
#: EOD append is the wrong response to either. Refused loudly instead.
_ENTRANT_SEED_MAX = 25
#: Wall-clock bound for the whole seed loop. A per-ticker backfill loads only
#: the ticker's parquet plus its macro/ETF context (`builders.backfill.
#: _PER_TICKER_CACHE_CONTEXT`), so one seed is seconds, not the ~20 minutes a
#: full-cache load costs; this caps the pathological case so it can never eat
#: the load-bearing append's budget.
_ENTRANT_SEED_HARD_TIMEOUT_S = 600


def _seed_index_entrants(
    bucket: str,
    expected_tickers: list[str],
    dry_run: bool = False,
) -> dict:
    """Write a new index member's full history into the ArcticDB universe the
    day it joins the declared membership (alpha-engine-config-I11444).

    The class: a ticker added to the S&P 500/400 appears in the constituents
    document the first morning it is a member, and `_run_daily` refreshes its
    10y price-cache parquet that same evening — but nothing on a weekday wrote
    its ArcticDB symbol. Only the Saturday DataPhase1 backfill did, and
    `daily_append` appends only to symbols that already exist. So every
    consumer that reads the SAME membership document (`latest_weekly.json`)
    against the library saw a member with no symbol for up to a week. Measured
    2026-09-21..23: BE joined the S&P 500 effective 2026-09-21; its parquet
    landed 2026-09-21 20:06Z; its ArcticDB symbol's first version is
    2026-09-23 01:17Z (the weekly rehearsal's backfill); Crucible v2's
    `data.daily` failed both windows in between with `MissingSourceError`
    naming exactly `['BE']`.

    Entrants are ``admits_universe_write`` members of ``expected_tickers`` with
    no universe symbol, excluding :data:`_MACRO_DAILY_TICKERS`: those ride
    along in every append's expected scope but are not index members, and one
    of them (XLRE) passes ``admits_universe_write`` yet is deliberately never a
    universe symbol — measured 2026-09-23, it was the ONLY name the unfiltered
    set produced, so without this every EOD run would try, and fail, to seed
    it. Each is seeded with ``backfill(ticker_filter=...)`` —
    the same per-ticker write path the chronic-gap heal and every manual seed
    (I8094) already use, so the symbol lands with the identical schema.
    Best-effort per ticker: one ticker's failure (``ticker_no_data`` when its
    parquet refresh failed, say) is recorded and the rest proceed. Returns
    ``status`` ``ok`` / ``ok_dry_run`` / ``error``; the caller logs and never
    lets it fail the append.
    """
    from features.compute import admits_universe_write
    from store.arctic_store import get_universe_lib

    not_members = {t.lstrip("^") for t in _MACRO_DAILY_TICKERS}
    wanted = {
        t.lstrip("^") for t in expected_tickers
        if admits_universe_write(t) and t.lstrip("^") not in not_members
    }
    present = set(get_universe_lib(bucket).list_symbols())
    entrants = sorted(wanted - present)
    summary: dict = {"entrants": entrants, "seeded": [], "errors": []}
    if not entrants:
        return {"status": "ok", **summary}
    if len(entrants) > _ENTRANT_SEED_MAX:
        return {
            "status": "error",
            "error": (
                f"{len(entrants)} declared members are absent from the ArcticDB "
                f"universe (cap {_ENTRANT_SEED_MAX}) — that is an outage or a wrong "
                f"library, not an index rebalance; refusing to seed. First 20: "
                f"{entrants[:20]}"
            ),
            **summary,
        }
    logger.info(
        "Seeding %d index entrant(s) absent from the ArcticDB universe: %s",
        len(entrants), entrants,
    )
    if dry_run:
        return {"status": "ok_dry_run", **summary}

    from builders.backfill import backfill as _backfill

    for ticker in entrants:
        try:
            outcome = _backfill(bucket=bucket, ticker_filter=ticker, dry_run=False)
        except Exception as exc:
            logger.exception("Index-entrant seed failed for %s", ticker)
            summary["errors"].append({"ticker": ticker, "reason": str(exc)})
            continue
        if outcome.get("status") == "ok":
            summary["seeded"].append(ticker)
        else:
            summary["errors"].append(
                {"ticker": ticker, "reason": outcome.get("error") or outcome.get("status")}
            )
    status = "ok" if not summary["errors"] else "error"
    return {"status": status, **summary}


def _run_entrant_seed_step(bucket: str, expected_tickers: list[str], dry_run: bool) -> dict:
    """`_seed_index_entrants` under a hard wall-clock bound, never raising.

    A seed failure is logged at ERROR (so it alerts) and recorded as a
    swallowed best-effort step, but it never fails the append: the append is
    load-bearing for reconcile and inference, and an unseeded entrant only
    leaves the library where it was before this step existed.
    """
    try:
        with _hard_timeout(_ENTRANT_SEED_HARD_TIMEOUT_S, "index-entrant seed"):
            result = _seed_index_entrants(bucket, expected_tickers, dry_run=dry_run)
    except _HardTimeout as exc:
        result = {"status": "error", "error": f"hard timeout: {exc}"}
    except Exception as exc:
        result = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    if result.get("status") not in ("ok", "ok_dry_run"):
        logger.error(
            "Index-entrant seed did not complete — new members stay absent from the "
            "ArcticDB universe until the next seed or Saturday backfill: %s",
            result.get("error") or result.get("errors"),
        )
        _record_swallowed_step(
            "index_entrant_seed", str(result.get("error") or result.get("errors")),
        )
    return result


def _run_daily_arctic_append(config: dict, args: argparse.Namespace) -> dict:
    """Standalone ArcticDB universe append for the EOD post-market path.

    The daily/EOD twin of :func:`_run_morning_arctic_append`. Split out of
    :func:`_run_daily` (2026-06-16) into its own EOD-SF state
    (``PostMarketArcticAppend``): the monolithic ``--daily`` run does
    daily_closes + metron collectors + feature-store compute + the SLOW
    ``daily_append`` in one shot, and on 2026-06-16 it exceeded PostMarketData's
    1200s SSM ``executionTimeout`` mid-append → SIGKILL → the whole EOD pipeline
    failed (no reconcile, no EOD email). As its own state the append gets a
    longer timeout decoupled from the feature compute, exactly mirroring the
    weekday MorningEnrich + MorningArcticAppend split (L4608).

    LOAD-BEARING: EOD reconcile + predictor inference read the ArcticDB universe
    after this, so an append failure returns ``status="failed"`` → ``main()``
    exits 1 → the SF's ``CheckPostMarketArcticAppendStatus`` routes to
    HandleFailure.

    Targets the same date as :func:`_run_daily` (today's UTC date, or ``--date``)
    and passes ``skip_if_exists=True`` so an operator rerun short-circuits
    tickers whose row already landed — identical semantics to the inline block
    it replaces (the Saturday DataPhase path still appends inline via
    ``--daily`` without ``--skip-arctic-append``). PostMarketData writes today's
    daily_closes parquet that this append reads, so this state runs after it.
    """
    bucket = config["bucket"]
    run_date = args.date or default_run_date()
    dry_run = args.dry_run
    results: dict = {
        "mode": "daily_arctic_append",
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    tickers = _load_daily_universe_tickers(config)
    if not tickers:
        logger.error("No tickers available for ArcticDB append")
        results["status"] = "failed"
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        return results

    # Seed any declared member the library has no symbol for BEFORE the append,
    # so the append then writes today's row onto it like every other member
    # (alpha-engine-config-I11444). `tickers` is the same `latest_weekly.json`
    # membership Crucible v2's `data.daily` grades against, so the two cannot
    # disagree about who is a member.
    entrant_seed = _run_entrant_seed_step(bucket, tickers, dry_run)

    logger.info("=" * 60)
    logger.info("APPENDING: ArcticDB universe (daily-arctic-append state, %s)", run_date)
    logger.info("=" * 60)
    reg = _build_registry(config, args, run_date)
    try:
        from builders.daily_append import daily_append
        with _maybe_phase(reg, "arcticdb"):
            arctic_result = daily_append(
                date_str=run_date,
                bucket=bucket,
                dry_run=dry_run,
                skip_if_exists=True,
                expected_tickers=tickers,
            )
        results["collectors"]["arcticdb"] = arctic_result
        # daily_append returns its own status; surface it as the load-bearing
        # verdict so a write failure halts the pipeline (reconcile reads next).
        _status = arctic_result.get("status", "unknown")
        results["status"] = "ok" if _status in ("ok", "ok_dry_run") else "failed"
        if results["status"] == "ok" and not dry_run:
            # config#2672: EOD is ALWAYS the yfinance-basis (fallback-quality)
            # write — mark run_date pending the next morning's polygon
            # -corrected overwrite. Idempotent (re-marking is harmless) and
            # fail-soft; never blocks this already-successful append.
            _mark_pending_upgrade(run_date, "fallback_quality", bucket)
    except Exception as e:
        logger.exception("ArcticDB daily_append (daily-arctic-append state) failed for %s", run_date)
        results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}
        results["status"] = "failed"

    # Recorded AFTER `arcticdb` so a failed append's reason names the append,
    # not a best-effort step that never decides this mode's status.
    results["collectors"]["entrant_seed"] = entrant_seed
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    logger.info("ArcticDB append %s for %s", results["status"].upper(), run_date)
    return results


def _finalize(
    results: dict,
    bucket: str,
    market_prefix: str,
    run_date: str,
    dry_run: bool,
    only: str | None,
) -> None:
    """Compute status, write manifest, log summary."""
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    elif any(s == "error" for s in statuses):
        results["status"] = "partial" if any(s == "ok" for s in statuses) else "failed"
    else:
        results["status"] = "partial"

    # Surface per-collector errors to Flow Doctor's ERROR-level handler.
    # Without this, the only logger.error() that fires on a partial run is
    # main()'s generic "non-ok status" summary line — single dedup signature
    # across every partial run, no diagnose-context error text. The helper
    # emits one logger.error per error-status entry with the collector name
    # + original message, restoring per-failure alert granularity.
    from nousergon_lib.collector_results import report_collector_errors
    report_collector_errors(results["collectors"])

    if not dry_run and only is None:
        _write_manifest(bucket, market_prefix, run_date, results)
        _write_validation_json(bucket, market_prefix, run_date, results)

    # Postflight: producer-side hard-fail if the outputs we just wrote
    # don't satisfy the consumer contracts downstream modules will enforce
    # at their own preflight. Fails before any downstream Lambda cold-start
    # or spot-EC2 bootstrap. See validators/postflight.py for the full
    # contract spec and the ROADMAP item that motivates it.
    phase = results.get("phase")
    if (
        not dry_run
        and phase == 1  # Only DataPhase1 is gated today; Phase 2 gets its own postflight.
        and only is None
        and results["status"] == "ok"
    ):
        from validators.postflight import DataPostflight, PostflightError
        try:
            DataPostflight(
                bucket=bucket,
                run_date=run_date,
                market_prefix=market_prefix,
                phase=phase,
            ).run()
        except PostflightError as exc:
            logger.error(
                "DataPhase%d POSTFLIGHT FAILED: %s — consumer contracts not met. "
                "Refusing to signal Step Function success.",
                phase, exc,
            )
            results["status"] = "postflight_failed"
            results["postflight_error"] = str(exc)

    # Write health marker for Step Functions
    if not dry_run and phase and only is None:
        _write_health_marker(bucket, phase, run_date, results["status"])

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        # CARVE-OUT (alpha-engine-config-I10226): (a) failure mode
        # swallowed — `results["started_at"]`/`["completed_at"]` missing or
        # unparseable. (c) no recording surface — this only formats a
        # cosmetic duration suffix for the `logger.info` summary line right
        # below; it writes no data and feeds no downstream consumer, so a
        # blank `duration` is the correct degraded rendering, not a
        # corruption. See `.debug-swallow-allowlist.yaml`.
        pass

    phase_label = f"Phase {phase} " if phase else ""
    logger.info(
        "%scollection %s: %s%s",
        phase_label,
        results["status"].upper(),
        ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
        duration,
    )

    # Send completion email.
    # send_step_email never raises (see emailer.py docstring) — it returns
    # True/False. The old try/except was dead code, AND the False return
    # was being silently dropped. If Gmail SMTP AND SES both fail, the
    # caller needs to know so monitoring isn't blind to a successful run
    # that silently had no notification.
    if not dry_run and only is None:
        from emailer import send_step_email
        step_name = f"Data Phase {phase}" if phase else "Data Collection"
        sent = send_step_email(step_name, results, run_date)
        if not sent:
            # Log at ERROR so CloudWatch alarms (if wired to ERROR-level)
            # surface the missed email. Not raising because the data
            # collection itself succeeded — only monitoring is affected.
            # Downstream Step Function steps can still consume the S3 output.
            logger.error(
                "Step email '%s' failed to send — both Gmail SMTP and SES "
                "fallback returned failure. Monitoring will be blind to "
                "this run's result summary. Check EMAIL_SENDER, "
                "EMAIL_RECIPIENTS, GMAIL_APP_PASSWORD env vars and SES "
                "identity verification.",
                step_name,
            )


def _write_manifest(bucket: str, s3_prefix: str, run_date: str, results: dict) -> None:
    """Write manifest.json and update latest_weekly.json pointer."""
    s3 = boto3.client("s3")

    # Manifest
    manifest_key = f"{s3_prefix}weekly/{run_date}/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(results, indent=2, default=str),
        ContentType="application/json",
    )

    # Latest pointer
    pointer = {"date": run_date, "s3_prefix": f"{s3_prefix}weekly/{run_date}/"}
    s3.put_object(
        Bucket=bucket,
        Key=f"{s3_prefix}latest_weekly.json",
        Body=json.dumps(pointer, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote manifest + latest pointer for %s", run_date)


def _write_validation_json(
    bucket: str, s3_prefix: str, run_date: str, results: dict,
) -> None:
    """Aggregate validation results from all collectors and write to S3."""
    collectors = results.get("collectors", {})
    validations: dict[str, dict] = {}

    for name, info in collectors.items():
        val = info.get("validation")
        if val:
            validations[name] = val

    if not validations:
        return

    total_validated = sum(v.get("total_validated", 0) for v in validations.values())
    total_anomalies = sum(v.get("anomalies", 0) for v in validations.values())
    total_clean = sum(v.get("clean", 0) for v in validations.values())

    payload = {
        "date": run_date,
        "total_validated": total_validated,
        "total_clean": total_clean,
        "total_anomalies": total_anomalies,
        "collectors": validations,
    }

    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/validation.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Wrote validation.json: %d validated, %d anomalies → s3://%s/%s",
        total_validated, total_anomalies, bucket, key,
    )


def _write_health_marker(bucket: str, phase: int, run_date: str, status: str) -> None:
    """Write phase-based health marker (legacy) for Step Functions dependency checking."""
    s3 = boto3.client("s3")
    key = f"health/data_phase{phase}.json"
    marker = {
        "phase": phase,
        "date": run_date,
        "status": status,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(marker, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote health marker: s3://%s/%s", bucket, key)


def _write_module_health(
    bucket: str,
    module_name: str,
    run_date: str,
    status: str,
    *,
    summary: dict | None = None,
    warnings: list | None = None,
    error: str | None = None,
    duration_seconds: float = 0.0,
) -> None:
    """Write module-scoped health stamp consumed by the executor's
    check_upstream_health() (alpha-engine/executor/health_status.py:91).

    Delegates to ``nousergon_lib.health.write_health`` (config#1727 Phase C).
    The legacy ``status`` string is mapped to :class:`Deliverable` objects;
    status is derived by the lib (required-missing / error / warnings) so a
    caller cannot stamp ``"ok"`` over a failed run. Key pattern remains
    ``health/{module_name}.json`` with ``last_success`` nulled on failure.
    """
    from nousergon_lib.health import Deliverable, write_health

    warnings = warnings or []
    if error:
        deliverables = [
            Deliverable(
                name=module_name,
                required=True,
                produced=False,
                detail=error,
            ),
        ]
    elif status == "failed":
        deliverables = [
            Deliverable(name=module_name, required=True, produced=False),
        ]
    elif status == "degraded":
        deliverables = [
            Deliverable(name=module_name, required=True, produced=True),
        ]
        if not warnings:
            deliverables.append(
                Deliverable(
                    name=f"{module_name}_optional",
                    required=False,
                    produced=False,
                )
            )
    else:
        deliverables = [
            Deliverable(name=module_name, required=True, produced=True),
        ]

    s3 = boto3.client("s3")
    write_health(
        module_name=module_name,
        deliverables=deliverables,
        run_date=run_date,
        duration_seconds=duration_seconds,
        summary=summary,
        warnings=warnings or None,
        error=error,
        bucket=bucket,
        s3_client=s3,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alpha Engine Weekly Data Collector")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing to S3")
    parser.add_argument(
        "--preflight-only", dest="preflight_only", action="store_true",
        help="Run ONLY the entry preflight (DataPreflight: env/secret resolution, "
             "S3 HEAD, polygon/FRED auth-reachability probes, ArcticDB connect + "
             "libraries-present read) then exit 0 BEFORE run_weekly(). No collector "
             "fetch, no S3/ArcticDB/parquet/config write. Friday shell-run dry path "
             "(ROADMAP 'Friday shell-run — per-module dry-path activation' #1) — "
             "catches bootstrap-class breakage ~12h before the real Saturday run.",
    )
    parser.add_argument("--date", default=None, help="Override run date (YYYY-MM-DD)")
    parser.add_argument(
        "--daily", action="store_true",
        help="Daily mode: capture today's OHLCV closes for all tickers (yfinance-only EOD pass).",
    )
    parser.add_argument(
        "--morning-enrich", dest="morning_enrich", action="store_true",
        help="Morning polygon enrichment: overwrite the prior trading day's parquet + ArcticDB row "
             "with polygon's authoritative OHLCV+VWAP. Hard-fails on polygon failure (no yfinance "
             "fallback). --date overrides which trading day to enrich (default: previous trading day).",
    )
    parser.add_argument(
        "--guard-skip-record", dest="guard_skip_record", default=None, metavar="PATH",
        help="Write the mode's own guard skip_reason to PATH when its guard deliberately "
             "skipped the run (status=skipped), and remove PATH otherwise. The stage "
             "launcher hands it to the stage-coverage assertion as --not-applicable-reason "
             "so a declared skip is not graded STALE (alpha-engine-config-I11474).",
    )
    parser.add_argument(
        "--chronic-gap-heal", dest="chronic_gap_heal", action="store_true",
        help="Best-effort: yfinance-backfill ArcticDB row gaps for the chronic-polygon-gap "
             "tickers (polygon doesn't reliably serve them) + emit the polygon-recovery / "
             "constituents-drift alarms. Split out of --morning-enrich (2026-06-11) so a "
             "yfinance hang in this best-effort step can never SIGKILL the load-bearing "
             "MorningEnrich. Never raises — returns a status dict. --date overrides target day.",
    )
    parser.add_argument(
        "--daily-heal", dest="daily_heal", action="store_true",
        help="Standalone daily data-heal mode (alpha-engine-config-I2717): bundles "
             "the universe-gap self-heal (config#1228 + config#2664 fallback-quality "
             "detection, formerly the head of --morning-arctic-append) and the "
             "chronic-polygon-gap heal (config#1811's ChronicGapSelfHeal logic) into "
             "one EventBridge-triggered run, OFF the ne-preopen-trading-pipeline "
             "critical path entirely (~09:00 UTC, hours before the 12:45 UTC "
             "preopen). Both heals stay fail-soft internally; emits the "
             "AlphaEngine/Data daily_heal_days_healed CloudWatch metric every run "
             "and writes data/heal/daily/{run_date}.json for the freshness-monitor "
             "plane. --date overrides target day.",
    )
    parser.add_argument(
        "--skip-chronic-heal", dest="skip_chronic_heal", action="store_true",
        help="With --morning-enrich: skip the inline chronic-gap self-heal. "
             "The weekday morning-enrich data-spot workload passes this flag "
             "because the heal now runs entirely separately, standalone, via "
             "--daily-heal (alpha-engine-config-I2717, 2026-07-16 — formerly "
             "a separate fail-soft ChronicGapSelfHeal SF state, now removed). "
             "The Saturday SF omits this flag so the heal still runs inline "
             "before DataPhase1's postflight.",
    )
    parser.add_argument(
        "--skip-arctic-append", dest="skip_arctic_append", action="store_true",
        help="With --morning-enrich: skip the inline ArcticDB daily_append "
             "(the weekday SF runs it as a separate load-bearing MorningArcticAppend "
             "state with a longer timeout). The Saturday SF omits this flag so the "
             "append still runs inline before DataPhase1's postflight.",
    )
    parser.add_argument(
        "--morning-arctic-append", dest="morning_arctic_append", action="store_true",
        help="Standalone ArcticDB universe append for the prior trading day "
             "(the slow daily_append split out of --morning-enrich, L4608). "
             "Load-bearing: exits 1 on append failure. --date overrides target day.",
    )
    parser.add_argument(
        "--daily-arctic-append", dest="daily_arctic_append", action="store_true",
        help="Standalone ArcticDB universe append for the EOD post-market path "
             "(the slow daily_append split out of --daily, 2026-06-16). The EOD SF "
             "runs --daily --skip-arctic-append (compute) then this state with a "
             "longer timeout. Load-bearing: exits 1 on append failure. Targets "
             "today's UTC date (or --date); skip_if_exists short-circuits reruns.",
    )
    parser.add_argument(
        "--phase", type=int, choices=[1, 2], default=None,
        help="Phase 1: pre-research data. Phase 2: post-research alternative data.",
    )
    parser.add_argument(
        "--only",
        choices=["constituents", "historical_constituents", "prices", "macro", "short_interest", "universe_classification", "universe_returns", "alternative", "daily_closes", "features", "arcticdb"],
        help="Run a single collector instead of all",
    )
    # Phase-registry recovery controls (L4528 — markers under data/{date}/.phases/).
    # A recovery re-run of the same date auto-skips collectors whose marker is ok
    # AND whose declared S3 artifact still exists (L4524). These flags override that:
    parser.add_argument(
        "--skip-phases", dest="skip_phases", default="",
        help="CSV of phase names to force-SKIP this run (e.g. 'prices,features').",
    )
    parser.add_argument(
        "--force-phases", dest="force_phases", default="",
        help="CSV of phase names to force-RERUN even if a valid marker exists.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force-rerun ALL phases (ignore every completion marker).",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    # _load_dotenv() + setup_logging() already ran at module-top so import-time
    # errors in the collectors block are captured. Apply user-requested level.
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    config = load_config(args.config)

    # Pre-flight: fail fast on env / connectivity drift before starting
    # the real collection work. See alpha-engine-lib/README.md.
    from preflight import DataPreflight
    if getattr(args, "morning_enrich", False):
        # Dedicated morning_enrich mode (preflight-task-split 2026-05-16):
        # morning-enrich is its own Saturday SF task and needs a proper
        # UNION entry preflight (polygon + FRED secrets + reachability +
        # S3 writeable + ArcticDB libraries present). The previous
        # "daily" mapping only probed ArcticDB freshness and did NOT
        # validate polygon/FRED reachability, even though
        # _run_morning_enrich hits polygon — so a drifted key failed
        # 28min into the spot run instead of in <1s at the entry.
        mode = "morning_enrich"
    elif args.daily or getattr(args, "daily_arctic_append", False) or getattr(args, "daily_heal", False):
        # --daily-arctic-append reads the daily_closes PostMarketData wrote +
        # the ArcticDB universe libraries — same preflight surface as --daily.
        # --daily-heal (alpha-engine-config-I2717) reads the same two surfaces
        # (staging/daily_closes parquet for the chronic-gap drift check +
        # detect_*_universe_days' ArcticDB reads, and writes both via
        # daily_append) — no dedicated preflight mode needed, "daily" already
        # covers S3 + ArcticDB reachability, which is what this mode's
        # fail-loud posture depends on catching before any heal work starts.
        mode = "daily"
    else:
        mode = f"phase{args.phase or 1}"
    DataPreflight(config["bucket"], mode).run()

    # Friday shell-run dry path (ROADMAP "Friday shell-run — per-module
    # dry-path activation" owed-item #1). --preflight-only exits HERE,
    # immediately after the existing DataPreflight has passed and strictly
    # BEFORE run_weekly(). run_weekly() is the sole function in this module
    # that performs ANY collector fetch (polygon/FMP/FRED/yfinance) or ANY
    # S3 / ArcticDB / parquet / config / module-health write — gating in
    # front of it makes every fetch/write code path statically unreachable
    # under this flag. The preflight itself only does read-only / auth
    # probes (S3 HEAD, polygon/FRED reference-data auth calls that fetch no
    # collector data, ArcticDB list_libraries) plus an S3 PUT+DELETE
    # sentinel under preflight/ — that sentinel is the preflight's own
    # liveness probe, not a data write, and it self-cleans. No external
    # API data is fetched and no production artifact is mutated.
    if getattr(args, "preflight_only", False):
        logger.info(
            "Pre-flight passed; --preflight-only set — exiting 0 before "
            "run_weekly() (NO collector fetch, NO S3/ArcticDB/config write). "
            "Friday shell-run dry path: bootstrap-class breakage would have "
            "surfaced above."
        )
        raise SystemExit(0)

    results = run_weekly(config, args)

    if getattr(args, "guard_skip_record", None):
        _write_guard_skip_record(args.guard_skip_record, results)

    # config#646 (Option A): write the flow's end-of-run status() snapshot to
    # s3://alpha-engine-research/_flow_doctor/heartbeat/data-collector/{date}.json
    # so the dashboard System Health consumer can read it. emit_heartbeat soft-
    # fails (returns None, never raises); fires here — after run_weekly() and
    # before the exit-code decision — so the heartbeat lands on every completed
    # run (ok, partial, or failing). config["bucket"] is the research bucket
    # (RESEARCH_BUCKET default "alpha-engine-research"; see preflight.py:197),
    # which is where the dashboard reads heartbeats from.
    # hasattr guard: emit_heartbeat only exists in flow-doctor >=0.6.2. The 5
    # producing repos deploy independently, so a version-skewed box could hold
    # an older lib (the #646 arc historically pinned 0.6.0rc3, which lacks it)
    # — guarding on the method's presence keeps this forward/backward-compatible
    # during the phased rollout so it can never AttributeError a production run.
    fd = get_flow_doctor()
    if fd and hasattr(fd, "emit_heartbeat"):
        fd.emit_heartbeat(bucket=config["bucket"])

    # Hard-fail on any non-ok status — strict form of the no-silent-fails
    # rule applied while the system is unstable. `partial` previously exited
    # 0 which let SSM report Success and the Step Function march forward on
    # missing/corrupt data. See feedback_hard_fail_until_stable memory for
    # rationale. Lift this back to == "failed" only after the system is
    # demonstrably stable (multiple clean Saturday runs in a row).
    #
    # ``skipped`` is the deliberate-no-op status emitted by _run_morning_enrich
    # when invoked after 1:30pm PT on a trading day (polygon free-tier 403's
    # today's grouped-daily). Treated as success so spot_data_weekly.sh's
    # ``if ! ... exit 1`` check does not trip.
    # `degraded` joins ok/skipped as a non-halting outcome (alpha-engine-config-
    # I7572). It is emitted ONLY by the EOD daily path, ONLY for the feature
    # store's zero-variance postflight, and it is loud at every layer: ERROR log
    # naming the offending columns, `degraded` in the per-collector status map,
    # `degraded` on the run, and a health marker recording it. The hard-fail
    # posture for every genuine data failure is unchanged — see the comment
    # above, which still governs `failed`.
    if results["status"] == "degraded":
        logger.error(
            "Collection finished DEGRADED — the artifact was produced and the "
            "pipeline continues, but %s reported a known defect. Exiting 0 so "
            "the EOD SF still runs the ArcticDB append and EODReconcile. "
            "Defect detail: %s. Per-collector statuses: %s",
            ", ".join(results.get("degraded_collectors", [])) or "a collector",
            _describe_degraded_defects(results),
            {k: v.get("status", "?") for k, v in results.get("collectors", {}).items()},
        )
    if results["status"] not in ("ok", "skipped", "degraded"):
        logger.error(
            "Weekly collection finished with non-ok status=%s — exiting 1 "
            "to halt the pipeline. Per-collector statuses: %s",
            results["status"],
            {k: v.get("status", "?") for k, v in results.get("collectors", {}).items()},
        )
        raise SystemExit(1)


if __name__ == "__main__":
    # Capture an uncaught crash via flow-doctor before re-raising
    # (no-ops when flow-doctor is inactive).
    with guard_entrypoint():
        main()
