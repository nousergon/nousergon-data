"""Guard commissioning, class ``empty_fresh``: one induced-fault record.

`alpha-engine-config-I10786` (plan item P-19, `data_collection_plan_260914.md`
§4.5 and the §6 phase-2 exit gate: "Guard commissioning: one induced fault per
guard class (withheld vendor, truncated universe, unit-scaled column, empty
frame, stale ``as_of``) -> ``failed``, one page, stand-down"). A guard that has
never fired is not in service (`observability-policy` §9.1); this file is the
first fault record, for the class the plan pairs with an EMPTY FRAME.

**The fault.** D19 (post-market data, ``weekly_collector.py`` daily mode,
phase ``daily_closes``) PUTs a real zero-row parquet frame to its contracted
key and reports ``status: ok`` with a row count of 0, which is the
empty-but-fresh write the guard exists to catch: fresh by timestamp, a
non-zero-byte object, and nothing in it.

**What runs for real:** ``weekly_collector._phase_collect`` (and through it
the `nousergon_lib` run-manifest wrapper and the guard in
``validators/expectations.py``), the data repo's own
``RunStatePhaseRegistry`` writing real phase markers, the completion check the
dispatcher Lambda runs (``data_gate/run_manifest_predicate.py``), and the
deployed ``infrastructure/step-functions/data-collection.asl.json`` walked
state by state. S3 is an in-memory fake; nothing here reaches AWS.

**What is modelled, and stated:** a workload's process exit code. It is the
daily aggregator's own rule (every collector ``ok``/``ok_dry_run``/``degraded``
exits 0, anything else exits 1 via ``main()``'s ``SystemExit(1)``), applied to
the one collector this record faults.

**The three properties, per the issue's closes-when:**

1. a ``failed`` manifest — under the ENFORCING guard. The guard ships in
   OBSERVE (`sf-pipeline-policy` §7a); the record is taken against the
   promotion itself (the one-line ``mode`` flip), because the phase-2 gate
   reads "every applicable guard clause **enforcing** and **commissioned**",
   and an observe-mode verdict has no consequence to commission. What the
   shipped OBSERVE mode does with the same fault is recorded alongside.
2. exactly one page — counted as ``sns:publish`` states the execution
   actually visits on the deployed definition, retry included.
3. a clean stand-down — the execution reaches a terminal ``Fail`` with no
   state left armed: the phase marker is NOT ``ok``, so the retry and every
   same-date rerun recompute rather than auto-skip the refused artifact; and
   once the fault is gone the next run files ``ok``, passes the completion
   check and pages nobody.

**What the first run of this record found** (fixed in the same change): the
enforce branch raised AFTER the phase marker had been written ``ok``, so the
Step Function's on-demand retry auto-skipped the very artifact the guard had
just refused, exited 0, and the only thing still between that and a green run
was the completion check's same-date carry-forward. That is the
`alpha-engine-config-I11812` defect reached through the guard instead of
through ``degraded``; ``weekly_collector._GuardRejectedPublish`` closes it.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
import pathlib
import re
import types
from dataclasses import dataclass

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from nousergon_lib import run_manifest
from nousergon_lib.guard_mode import GuardMode, GuardStaging

import run_units
import weekly_collector
from collectors import metron_market_data, technical_rating_ledger
from data_gate import descriptors, evidence
from data_gate import run_manifest_predicate as completion
from data_gate.guard_commissioning import record_document, record_key
from data_gate.store import LocalStore
from shadow.run_state import RunStatePhaseRegistry
from validators import expectations

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ASL_PATH = REPO_ROOT / "infrastructure" / "step-functions" / "data-collection.asl.json"
STACK_PATH = REPO_ROOT / "infrastructure" / "cloudformation" / "nousergon-data-collection.yaml"

GUARD_CLASS = "empty_fresh"
WORKLOAD = "post-market-data"
BUCKET = "alpha-engine-research"

#: When the harness's first execution starts: the EOD schedule's 18:15 ET
#: trigger on a trading day. Every stamp the record is graded on comes from
#: :class:`HarnessClock`, never the wall clock, so the record reads the same on
#: any day and at any hour it is taken.
EOD_START = dt.datetime(2026, 10, 15, 22, 15, tzinfo=dt.timezone.utc)
TRADING_DAY = EOD_START.date().isoformat()


class HarnessClock:
    """The one clock every run manifest and execution start in this file reads.

    The completion check takes the lexically greatest manifest key as a unit's
    newest run, and that key is a ULID: a millisecond timestamp, then 80 random
    bits. Read off the wall clock, the faulted retry and the recovery run started
    1-2 ms apart on a CI-class box, and on a faster one in the SAME millisecond,
    where the random bits decided which was "newest". When they picked the
    retry, the recovery graded the refused run (`run_not_ok`) and failed — a
    random subset of units on each run (alpha-engine-config-I10786, 2026-10-15).
    In production the two are separate box launches minutes apart.

    Each read advances one second, so every run starts strictly after the last
    one finished, and the manifest's whole-second ``finished`` stamp never
    reads earlier than the execution start it is graded against.
    """

    STEP = dt.timedelta(seconds=1)

    def __init__(self, start: dt.datetime) -> None:
        self._now = start

    def now(self) -> dt.datetime:
        self._now += self.STEP
        return self._now


@dataclass(frozen=True)
class Case:
    """One unit's induced fault: the phase it runs as and the key it publishes.

    ``key`` is the unit's PRIMARY ``artifact_key`` exactly as the daily mode's
    ``_phase_collect`` call site builds it in ``weekly_collector.py`` (from the
    same collector-module constants), so the guard grades the key production
    publishes. ``fault`` is the induced-fault sentence the record carries.
    """

    unit: str
    phase: str
    key: str
    fault: str
    #: D19's call site alone passes ``verify_artifact_exists=True, bucket=...``
    #: (config-I2702); the metron call sites pass neither. Mirrored, not tidied.
    verify_artifact_exists: bool = False
    #: ``(key, result count key)`` for every ``extra_outputs`` entry the call
    #: site declares (alpha-engine-config-I10855) — D20 and D22 publish more
    #: than one key, and the completion check reads ``output_missing`` on a
    #: recovery run that published only the primary one.
    extra: tuple[tuple[str, str], ...] = ()

    @property
    def keys(self) -> tuple[str, ...]:
        return (self.key, *(k for k, _ in self.extra))

    @property
    def rows_key(self) -> str:
        return run_units.PHASE_UNITS[("daily", self.phase)].rows_key


def _json_case(unit: str, phase: str, key: str, extra: tuple[tuple[str, str], ...] = ()) -> Case:
    return Case(
        unit, phase, key,
        f"empty payload: a zero-record JSON document PUT to {key} under status ok, "
        "reporting a row count of 0" + (" (and every co-published key empty too)" if extra else ""),
        extra=extra,
    )


_M = metron_market_data
#: Every daily-mode ``_phase_collect`` unit on the EOD machine's
#: ``post-market-data`` workload that publishes through a primary
#: ``artifact_key``. D19 is the first record (nousergon-data#2069); the metron
#: units ride the identical call path (`_phase_collect` ->
#: `_record_phase_lineage` -> `check_empty_fresh`) on the same workload and the
#: same deployed ASL, and each gets its own record because the gate grades
#: commissioning per unit (`faults/<unit>/<guard>/latest.json`).
#:
#: Not here, and why: D03/D31/D32 run on the same machine and are left to a
#: later slice (this one adds the metron family only); D21 and D26 publish only
#: per-symbol / ledger keys through ``extra_outputs`` with no primary
#: ``artifact_key``, so their fault needs the extra-output shape, not this one.
CASES: tuple[Case, ...] = (
    Case(
        "D19", "daily_closes", f"staging/daily_closes/{TRADING_DAY}.parquet",
        "empty frame: a zero-row parquet PUT to the contracted key under status ok",
        verify_artifact_exists=True,
    ),
    _json_case(
        "D20", "metron_market_data", f"{_M.CLOSES_PREFIX}{TRADING_DAY}.json",
        extra=(
            (f"{_M.CLOSES_PREFIX}latest.json", "closes"),
            (f"{_M.FX_PREFIX}{TRADING_DAY}.json", "fx"),
            (f"{_M.FX_PREFIX}latest.json", "fx"),
        ),
    ),
    _json_case(
        "D22", "metron_reference_data", f"{_M.SECTORS_PREFIX}latest.json",
        extra=((f"{_M.EARNINGS_PREFIX}latest.json", "earnings"),),
    ),
    _json_case("D23", "metron_macro_data", f"{_M.MACRO_PREFIX}latest.json"),
    _json_case("D24", "metron_fundamentals_data", f"{_M.FUNDAMENTALS_PREFIX}latest.json"),
    _json_case("D25", "metron_technicals_data", f"{_M.TECHNICALS_PREFIX}latest.json"),
    _json_case("D27", "metron_rating_performance", technical_rating_ledger.RATING_PERFORMANCE_KEY),
    _json_case("D28", "metron_security_performance_data", f"{_M.SECURITY_PERFORMANCE_PREFIX}latest.json"),
    _json_case("D29", "metron_analyst_data", f"{_M.ANALYST_PREFIX}latest.json"),
    _json_case("D30", "metron_sentiment_data", f"{_M.SENTIMENT_PREFIX}latest.json"),
)
CASE_IDS = [c.unit for c in CASES]
D19 = CASES[0]


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class InMemoryS3:
    """The four S3 calls the run path makes, over a dict of bytes."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **_):  # noqa: N803 -- boto3 kwarg names
        self.objects[Key] = Body if isinstance(Body, bytes) else str(Body).encode("utf-8")
        return {"ETag": '"fake"'}

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix="", StartAfter="", **_):  # noqa: N803
        keys = sorted(k for k in self.objects if k.startswith(Prefix) and k > StartAfter)
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def manifests(self, case: Case) -> list[dict]:
        prefix = f"data_collection/runs/{case.unit}/{TRADING_DAY}/"
        return [json.loads(self.objects[k]) for k in sorted(self.objects) if k.startswith(prefix)]

    def marker(self, case: Case) -> dict:
        return json.loads(self.objects[f"data/{TRADING_DAY}/.phases/{case.phase}.json"])


def _parquet(rows: int) -> bytes:
    frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(rows)], "close": [1.0] * rows})
    buf = io.BytesIO()
    frame.to_parquet(buf)
    return buf.getvalue()


def _body(case: Case, rows: int) -> bytes:
    if case.key.endswith(".parquet"):
        return _parquet(rows)
    return json.dumps({"records": [{"symbol": f"T{i}"} for i in range(rows)]}).encode("utf-8")


def _collector(s3: InMemoryS3, case: Case, rows: int):
    """The unit's collector body, reduced to its publish: PUT the key, report its count."""

    def run() -> dict:
        result = {"status": "ok", case.rows_key: rows}
        for key in case.keys:
            s3.put_object(Bucket=BUCKET, Key=key, Body=_body(case, rows))
        for _, count_key in case.extra:
            result[count_key] = rows
        return result

    return run


def _extra_outputs(case: Case) -> tuple:
    """The call site's ``extra_outputs``, in its own shape: written on ``ok``, counted by key."""
    return tuple(
        (key, lambda r: r.get("status") == "ok", lambda r, c=count_key: r.get(c) or 0)
        for key, count_key in case.extra
    )


def _run_phase(s3: InMemoryS3, case: Case, rows: int) -> dict:
    """One process attempt of the unit's phase, exactly as the daily mode calls it."""
    reg = RunStatePhaseRegistry(date=TRADING_DAY, bucket=BUCKET, marker_prefix="data", s3_client=s3)
    reg.data_mode = "daily"
    if case.verify_artifact_exists:
        return weekly_collector._phase_collect(
            reg, case.phase, _collector(s3, case, rows), artifact_key=case.key,
            verify_artifact_exists=True, bucket=BUCKET,
        )
    return weekly_collector._phase_collect(
        reg, case.phase, _collector(s3, case, rows), artifact_key=case.key,
        extra_outputs=_extra_outputs(case),
    )


def _process_status(result: dict) -> str:
    """The daily aggregator's rule for ONE collector -> the SSM command status."""
    return "Success" if result.get("status") in ("ok", "ok_dry_run", "degraded") else "Failed"


@pytest.fixture(autouse=True)
def _environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:commissioning")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")
    monkeypatch.delenv("RUN_TOKEN", raising=False)
    completion.reset_unit_cache()


@pytest.fixture(autouse=True)
def clock(monkeypatch) -> HarnessClock:
    """Stamp every run manifest from :class:`HarnessClock` (the wrapper's only clock reads)."""
    harness_clock = HarnessClock(EOD_START)

    class _HarnessDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return harness_clock.now().astimezone(tz) if tz else harness_clock.now()

    monkeypatch.setattr(
        run_manifest, "dt", types.SimpleNamespace(datetime=_HarnessDatetime, timezone=dt.timezone)
    )
    return harness_clock


@pytest.fixture
def s3(monkeypatch) -> InMemoryS3:
    store = InMemoryS3()
    # verify-by-artifact (config-I2702) HEADs through its own boto3 client.
    monkeypatch.setattr(weekly_collector, "_s3_object_exists", lambda _b, key: key in store.objects)
    return store


@pytest.fixture
def enforcing(monkeypatch) -> GuardStaging:
    """The promotion: the one-line ``mode`` flip `EMPTY_FRESH_GUARD` declares."""
    promoted = GuardStaging(
        name=expectations.EMPTY_FRESH_GUARD.name,
        mode=GuardMode.ENFORCE,
        promotion_criterion=expectations.EMPTY_FRESH_GUARD.promotion_criterion,
        tracked_issue=expectations.EMPTY_FRESH_GUARD.tracked_issue,
    )
    monkeypatch.setattr(expectations, "EMPTY_FRESH_GUARD", promoted)
    # `report()` binds its default staging at import, so after an in-test flip
    # its log line would still say `mode=observe`; a source-level promotion
    # rebinds it. Pin the default so the record's log line says what ran.
    report = expectations.report
    monkeypatch.setattr(
        expectations, "report", lambda reading, *, unit_id, staging=promoted, log=None: report(
            reading, unit_id=unit_id, staging=staging, log=log
        )
    )
    return promoted


# ---------------------------------------------------------------------------
# The deployed state machine, walked
# ---------------------------------------------------------------------------


def _deployed_eod_input() -> dict:
    """The EOD schedule's execution input, read from the deployed template."""
    match = re.search(r"Input: '(\{\"collection\": \"eod\".*?\})'", STACK_PATH.read_text())
    assert match, "the EOD schedule's Input could not be found in the collection stack template"
    return json.loads(match.group(1))


def _get(data: dict, path: str):
    node = data
    for part in path.removeprefix("$.").split("."):
        m = re.fullmatch(r"(\w+)\[(\d+)\]", part)
        name, index = (m.group(1), int(m.group(2))) if m else (part, None)
        if not isinstance(node, dict) or name not in node:
            raise KeyError(path)
        node = node[name]
        if index is not None:
            if not isinstance(node, list) or index >= len(node):
                raise KeyError(path)
            node = node[index]
    return node


def _rule(rule: dict, data: dict) -> bool:
    if "Or" in rule:
        return any(_rule(r, data) for r in rule["Or"])
    if "And" in rule:
        return all(_rule(r, data) for r in rule["And"])
    if "Not" in rule:
        return not _rule(rule["Not"], data)
    if "IsPresent" in rule:
        try:
            _get(data, rule["Variable"])
            return rule["IsPresent"]
        except KeyError:
            return not rule["IsPresent"]
    value = _get(data, rule["Variable"])
    for op, test in (
        ("BooleanEquals", lambda v, x: v is x),
        ("StringEquals", lambda v, x: v == x),
        ("NumericLessThan", lambda v, x: v < x),
    ):
        if op in rule:
            return test(value, rule[op])
    raise AssertionError(f"Choice operator not modelled by this walker: {rule}")  # fail loud


def _resolve(params: dict, data: dict, context: dict) -> dict:
    out = {}
    for k, v in params.items():
        if not k.endswith(".$"):
            out[k] = v
            continue
        if v.startswith("$$."):
            out[k[:-2]] = context[v]
        elif v.startswith("States.MathAdd("):
            path, inc = re.fullmatch(r"States\.MathAdd\((\$\.\w+), (\d+)\)", v).groups()
            out[k[:-2]] = _get(data, path) + int(inc)
        else:
            out[k[:-2]] = _get(data, v)
    return out


def _walk(states: dict, start: str, data: dict, tasks, path: list[str], context: dict) -> str:
    """Run one (sub-)machine to a terminal state. Returns the terminal's type."""
    name = start
    for _ in range(500):
        state = states[name]
        path.append(name)
        kind = state["Type"]
        if kind in ("Fail", "Succeed"):
            return kind
        if kind == "Choice":
            name = next((c["Next"] for c in state["Choices"] if _rule(c, data)), state.get("Default"))
            assert name, f"Choice {path[-1]} matched nothing and has no Default"
            continue
        if kind == "Pass":
            if "Parameters" in state:
                data = _resolve(state["Parameters"], data, context)
            name = state["Next"]
            continue
        if kind == "Wait":
            name = state["Next"]
            continue
        if kind == "Map":
            failed = False
            for item in _get(data, state["ItemsPath"]):
                item_input = _resolve(state["ItemSelector"], data, {**context, "$$.Map.Item.Value": item})
                proc = state["ItemProcessor"]
                if _walk(proc["States"], proc["StartAt"], item_input, tasks, path, context) == "Fail":
                    failed = True
                    break
            if failed:
                catch = state["Catch"][0]
                data = {**data, "error": {"Error": "States.TaskFailed"}}
                name = catch["Next"]
                continue
            name = state["Next"]
            continue
        if kind == "Task":
            result = tasks(name, state, data)
            if state.get("ResultPath", "$") is not None:
                data = {**data, state["ResultPath"].removeprefix("$."): result}
            name = state["Next"]
            continue
        raise AssertionError(f"state type {kind} not modelled by this walker")
    raise AssertionError("the execution did not terminate within 500 transitions — a stuck state")


class Execution:
    """One execution of the deployed EOD machine, with the case's phase run for real."""

    def __init__(self, s3: InMemoryS3, case: Case, rows_per_attempt):
        self.s3 = s3
        self.case = case
        self.rows_per_attempt = rows_per_attempt
        self.attempts: list[dict] = []
        self.pages: list[str] = []
        self.path: list[str] = []
        # The same clock the manifests are stamped by (the `clock` fixture).
        self.started_at = run_manifest.dt.datetime.now(dt.timezone.utc)

    def tasks(self, name: str, state: dict, data: dict):
        resource = state["Resource"]
        if resource.endswith(":sns:publish"):
            self.pages.append(name)
            return None
        if name == "TradingDayCheck":
            return {"is_trading_day": True}
        if name == "LaunchWorkload":
            return {"data_spot": {"launched": True, "command_id": "cmd", "instance_id": "i-0"}}
        if name == "PollWorkload":
            if data["workload"] != WORKLOAD:
                return {"Status": "Success", "StatusDetails": "Success"}
            rows = self.rows_per_attempt[len(self.attempts)]
            result = _run_phase(self.s3, self.case, rows)
            self.attempts.append(result)
            status = _process_status(result)
            return {"Status": status, "StatusDetails": status}
        if name == "VerifyRunManifests":
            # The dispatcher Lambda's own action, graded over the faulted unit.
            out = completion._completion_check(
                {
                    "collection": data["collection"],
                    "units": [self.case.unit],
                    "started_at": data["started_at"],
                },
                s3_client=self.s3,
            )["completion"]
            self.completion = out
            return out
        raise AssertionError(f"task {name} not modelled by this walker")

    def run(self) -> str:
        asl = json.loads(ASL_PATH.read_text())
        data = _deployed_eod_input()
        context = {
            "$$.Execution.Id": "arn:aws:states:::execution:commissioning",
            "$$.Execution.StartTime": self.started_at.isoformat().replace("+00:00", "Z"),
        }
        return _walk(asl["States"], asl["StartAt"], data, self.tasks, self.path, context)


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_fault_record_empty_frame_fails_pages_once_and_stands_down(case, s3, enforcing, caplog):
    """The commissioning record for ``empty_fresh``, under the promoted guard, per unit."""
    caplog.set_level(logging.ERROR, logger=weekly_collector.logger.name)

    # Induce: the empty publish, on the attempt AND on the on-demand retry
    # (the fault is in the data, so the retry meets it again).
    faulted = Execution(s3, case, rows_per_attempt=[0, 0])
    terminal = faulted.run()

    # 1. A `failed` manifest, carrying the guard's verdict under `enforce`.
    manifests = s3.manifests(case)
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert "empty_fresh guard" in m["reason"]
        guard = next(g for g in m["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
        assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "enforce")
        assert sorted((o["key"], o["rows_out"]) for o in m["outputs"]) == sorted((k, 0) for k in case.keys)

    # 2. Exactly one page — not zero, not one per attempt.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    guard_errors = [r for r in caplog.records if "guard=data_empty_fresh" in r.getMessage()]
    # One ERROR verdict line per graded key per attempt (every published PUT is graded).
    assert len(faulted.attempts) == 2
    assert len(guard_errors) == len(faulted.attempts) * len(case.keys)

    # 3. Stand-down: terminal Fail, the retry RECOMPUTED (never auto-skipped
    # the refused artifact), and the marker is not left `ok`.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert faulted.path.count("RetryOnDemand") == 1
    assert not any(a.get("auto_skipped") for a in faulted.attempts)
    marker_after_fault = s3.marker(case)
    assert marker_after_fault["status"] == "error"
    assert "enforcing empty_fresh guard rejected this publish" in marker_after_fault["error"]

    # ...and once the fault is gone, the next run is clean end to end.
    recovered = Execution(s3, case, rows_per_attempt=[896])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True
    # Keys list in run order (HarnessClock), so the completion check's
    # "newest" is the recovery run, not whichever ULID drew higher random bits.
    assert [m["status"] for m in s3.manifests(case)] == ["failed", "failed", "ok"]
    assert s3.marker(case)["status"] == "ok"

    _print_record(case, faulted, recovered, s3, marker_after_fault)


def test_the_same_fault_under_the_shipped_observe_mode_pages_once_through_the_completion_check(s3):
    """What OBSERVE does with the same fault — recorded, not commissioned.

    The manifest stays ``ok`` (observe moves no consequence) with the RED
    verdict on it, the process exits 0, and the page comes from the
    completion check's per-key floor (``rows_below_floor``), not from the
    guard. So the empty frame is caught today, once — by the completion
    check. That is why this is not the commissioning record: the guard's own
    consequence is only reachable once it enforces.
    """
    observed = Execution(s3, D19, rows_per_attempt=[0])
    assert observed.run() == "Fail"
    m = s3.manifests(D19)[-1]
    assert m["status"] == "ok"
    guard = next(g for g in m["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
    assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "observe")
    assert observed.pages == ["NotifyCompletionFindings"]
    assert observed.completion["failure_mode"] == "rows_below_floor"
    assert observed.path[-1] == "RowsBelowFloor"


def test_observe_mode_never_withholds_the_marker(s3):
    """The marker fix is gated on ENFORCE: in observe, behaviour is unchanged."""
    _run_phase(s3, D19, 0)
    assert s3.marker(D19)["status"] == "ok"


def test_every_case_is_a_daily_phase_unit_on_the_post_market_data_workload():
    """A case is commissioned only on the machine and workload it actually runs on.

    The record's page and stand-down are walked on the deployed EOD machine,
    whose ``verify_units`` must name the unit (else the completion check grades
    nothing for it) and whose ``post-market-data`` workload is the daily mode
    that runs these phases.
    """
    eod = _deployed_eod_input()
    assert WORKLOAD in eod["workloads"]
    for case in CASES:
        assert run_units.PHASE_UNITS[("daily", case.phase)].unit_id == case.unit
        assert case.unit in eod["verify_units"], case.unit
        unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
        assert unit.guards[GUARD_CLASS]["state"] == "present", case.unit


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def test_the_record_shape_is_what_the_phase_two_gate_reads(case, tmp_path):
    """``faults/<unit>/<guard>/latest.json`` with ``outcome: induced`` reads MET.

    The gate reader is `data_gate.evidence.read_guard_commissioning`. The
    document is built by the same function the publisher
    (`scripts/publish_guard_commissioning.py`) writes, so what this asserts is
    what lands in the store.
    """
    unit = next(u for u in descriptors.load_units() if u.unit_id == case.unit)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(TRADING_DAY)
    before = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    document = record_document(case.unit, GUARD_CLASS, fault=case.fault, test=nodeid(case), code_sha="c" * 40)
    store.put_bytes(record_key(case.unit, GUARD_CLASS), json.dumps(document).encode("utf-8"))
    after = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def nodeid(case: Case) -> str:
    return (
        "tests/test_guard_commissioning_empty_fresh.py::"
        f"test_the_fault_record_empty_frame_fails_pages_once_and_stands_down[{case.unit}]"
    )


def _print_record(case: Case, faulted: Execution, recovered: Execution, s3: InMemoryS3, marker: dict) -> None:
    """The fault record, printed for the PR body (`pytest -s`)."""
    first = s3.manifests(case)[0]
    guard = next(g for g in first["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
    lines = [
        f"FAULT RECORD guard={GUARD_CLASS} unit={case.unit} phase={case.phase} workload={WORKLOAD}",
        f"  fault:      {case.fault}",
        "  attempts:   " + ", ".join(f"{a['status']}" for a in faulted.attempts),
        "  manifests:  " + ", ".join(m["status"] for m in s3.manifests(case)[: len(faulted.attempts)]),
        f"  verdict:    {guard['verdict']} mode={guard['mode']}: {guard['detail']}",
        f"  pages:      {len(faulted.pages)} {faulted.pages}",
        "  path:       " + " > ".join(faulted.path),
        f"  marker:     {marker['status']} ({marker['error'][:90]}...)",
        "  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})",
    ]
    print("\n" + "\n".join(lines))
