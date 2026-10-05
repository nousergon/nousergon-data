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

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from nousergon_lib.guard_mode import GuardMode, GuardStaging

import weekly_collector
from data_gate import descriptors, evidence
from data_gate import run_manifest_predicate as completion
from data_gate.store import LocalStore
from shadow.run_state import RunStatePhaseRegistry
from validators import expectations

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ASL_PATH = REPO_ROOT / "infrastructure" / "step-functions" / "data-collection.asl.json"
STACK_PATH = REPO_ROOT / "infrastructure" / "cloudformation" / "nousergon-data-collection.yaml"

UNIT = "D19"
GUARD_CLASS = "empty_fresh"
PHASE = "daily_closes"
WORKLOAD = "post-market-data"
BUCKET = "alpha-engine-research"

#: The trading day is TODAY (UTC) rather than a fixed date: the completion
#: check lists manifests from ``started_at - MANIFEST_LOOKBACK_DAYS`` onward and
#: requires ``finished >= started_at``, both against wall-clock stamps the
#: wrapper writes. A fixed past date would grade an empty window.
TRADING_DAY = dt.datetime.now(dt.timezone.utc).date().isoformat()
KEY = f"staging/daily_closes/{TRADING_DAY}.parquet"


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

    def manifests(self) -> list[dict]:
        prefix = f"data_collection/runs/{UNIT}/{TRADING_DAY}/"
        return [json.loads(self.objects[k]) for k in sorted(self.objects) if k.startswith(prefix)]

    def marker(self) -> dict:
        return json.loads(self.objects[f"data/{TRADING_DAY}/.phases/{PHASE}.json"])


def _parquet(rows: int) -> bytes:
    frame = pd.DataFrame({"ticker": [f"T{i}" for i in range(rows)], "close": [1.0] * rows})
    buf = io.BytesIO()
    frame.to_parquet(buf)
    return buf.getvalue()


def _collector(s3: InMemoryS3, rows: int):
    """D19's collector body, reduced to its publish: PUT a frame, report it."""

    def run() -> dict:
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=_parquet(rows))
        return {"status": "ok", "tickers_captured": rows}

    return run


def _run_phase(s3: InMemoryS3, rows: int) -> dict:
    """One process attempt of the D19 phase, exactly as the daily mode calls it."""
    reg = RunStatePhaseRegistry(date=TRADING_DAY, bucket=BUCKET, marker_prefix="data", s3_client=s3)
    reg.data_mode = "daily"
    return weekly_collector._phase_collect(
        reg, PHASE, _collector(s3, rows), artifact_key=KEY, verify_artifact_exists=True, bucket=BUCKET,
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
    """One execution of the deployed EOD machine, with D19's phase run for real."""

    def __init__(self, s3: InMemoryS3, rows_per_attempt):
        self.s3 = s3
        self.rows_per_attempt = rows_per_attempt
        self.attempts: list[dict] = []
        self.pages: list[str] = []
        self.path: list[str] = []
        self.started_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)

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
            result = _run_phase(self.s3, rows)
            self.attempts.append(result)
            status = _process_status(result)
            return {"Status": status, "StatusDetails": status}
        if name == "VerifyRunManifests":
            # The dispatcher Lambda's own action, graded over the faulted unit.
            out = completion._completion_check(
                {
                    "collection": data["collection"],
                    "units": [UNIT],
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


def test_the_fault_record_empty_frame_fails_pages_once_and_stands_down(s3, enforcing, caplog):
    """The commissioning record for ``empty_fresh``, under the promoted guard."""
    caplog.set_level(logging.ERROR, logger=weekly_collector.logger.name)

    # Induce: the empty frame, on the attempt AND on the on-demand retry (the
    # fault is in the data, so the retry meets it again).
    faulted = Execution(s3, rows_per_attempt=[0, 0])
    terminal = faulted.run()

    # 1. A `failed` manifest, carrying the guard's verdict under `enforce`.
    manifests = s3.manifests()
    assert [m["status"] for m in manifests] == ["failed", "failed"], manifests
    for m in manifests:
        assert "empty_fresh guard" in m["reason"]
        guard = next(g for g in m["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
        assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "enforce")
        assert [(o["key"], o["rows_out"]) for o in m["outputs"]] == [(KEY, 0)]

    # 2. Exactly one page — not zero, not one per attempt.
    assert faulted.pages == ["NotifyFailure"], faulted.path
    guard_errors = [r for r in caplog.records if "guard=data_empty_fresh" in r.getMessage()]
    assert len(guard_errors) == len(faulted.attempts) == 2  # one ERROR verdict line per attempt

    # 3. Stand-down: terminal Fail, the retry RECOMPUTED (never auto-skipped
    # the refused artifact), and the marker is not left `ok`.
    assert terminal == "Fail" and faulted.path[-1] == "CollectionFailed"
    assert faulted.path.count("RetryOnDemand") == 1
    assert not any(a.get("auto_skipped") for a in faulted.attempts)
    marker_after_fault = s3.marker()
    assert marker_after_fault["status"] == "error"
    assert "enforcing empty_fresh guard rejected this publish" in marker_after_fault["error"]

    # ...and once the fault is gone, the next run is clean end to end.
    recovered = Execution(s3, rows_per_attempt=[896])
    assert recovered.run() == "Succeed", recovered.path
    assert recovered.path[-1] == "CollectionSucceeded"
    assert recovered.pages == []
    assert recovered.completion["ok"] is True
    assert s3.manifests()[-1]["status"] == "ok"
    assert s3.marker()["status"] == "ok"

    _print_record(faulted, recovered, s3, marker_after_fault)


def test_the_same_fault_under_the_shipped_observe_mode_pages_once_through_the_completion_check(s3):
    """What OBSERVE does with the same fault — recorded, not commissioned.

    The manifest stays ``ok`` (observe moves no consequence) with the RED
    verdict on it, the process exits 0, and the page comes from the
    completion check's per-key floor (``rows_below_floor``), not from the
    guard. So the empty frame is caught today, once — by the completion
    check. That is why this is not the commissioning record: the guard's own
    consequence is only reachable once it enforces.
    """
    observed = Execution(s3, rows_per_attempt=[0])
    assert observed.run() == "Fail"
    m = s3.manifests()[-1]
    assert m["status"] == "ok"
    guard = next(g for g in m["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
    assert (guard["verdict"], guard["mode"]) == ("empty_fresh", "observe")
    assert observed.pages == ["NotifyCompletionFindings"]
    assert observed.completion["failure_mode"] == "rows_below_floor"
    assert observed.path[-1] == "RowsBelowFloor"


def test_observe_mode_never_withholds_the_marker(s3):
    """The marker fix is gated on ENFORCE: in observe, behaviour is unchanged."""
    _run_phase(s3, 0)
    assert s3.marker()["status"] == "ok"


def test_the_record_shape_is_what_the_phase_two_gate_reads(tmp_path):
    """``faults/<unit>/<guard>/latest.json`` with ``outcome: induced`` reads MET.

    The gate reader exists (`data_gate.evidence.read_guard_commissioning`);
    what does not exist yet is anything that writes the record to the data
    collection store — that is a production write and is parked on I10786.
    """
    unit = next(u for u in descriptors.load_units() if u.unit_id == UNIT)
    store = LocalStore(tmp_path)
    day = dt.date.fromisoformat(TRADING_DAY)
    before = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert before.met is False and "no induced-fault record" in before.detail

    key = tmp_path / "faults" / UNIT / GUARD_CLASS / "latest.json"
    key.parent.mkdir(parents=True)
    key.write_text(json.dumps(_record_document()))
    after = evidence.read_guard_commissioning(store, unit, GUARD_CLASS, trading_day=day)
    assert after.met is True, after.detail


def _record_document() -> dict:
    return {
        "unit": UNIT,
        "guard": GUARD_CLASS,
        "outcome": "induced",
        "fault": "empty frame: a zero-row parquet PUT to the contracted key under status ok",
        "test": "tests/test_guard_commissioning_empty_fresh.py::"
        "test_the_fault_record_empty_frame_fails_pages_once_and_stands_down",
        "as_of": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _print_record(faulted: Execution, recovered: Execution, s3: InMemoryS3, marker: dict) -> None:
    """The fault record, printed for the PR body (`pytest -s`)."""
    first = s3.manifests()[0]
    guard = next(g for g in first["guards"] if g["guard"] == expectations.EMPTY_FRESH_GUARD.name)
    lines = [
        f"FAULT RECORD guard={GUARD_CLASS} unit={UNIT} phase={PHASE} workload={WORKLOAD}",
        "  fault:      zero-row parquet frame PUT to " + KEY + " under status=ok",
        "  attempts:   " + ", ".join(f"{a['status']}" for a in faulted.attempts),
        "  manifests:  " + ", ".join(m["status"] for m in s3.manifests()[: len(faulted.attempts)]),
        f"  verdict:    {guard['verdict']} mode={guard['mode']}: {guard['detail']}",
        f"  pages:      {len(faulted.pages)} {faulted.pages}",
        "  path:       " + " > ".join(faulted.path),
        f"  marker:     {marker['status']} ({marker['error'][:90]}...)",
        "  recovery:   " + " > ".join(recovered.path) + f" (pages={len(recovered.pages)})",
        "  record:     " + json.dumps({k: v for k, v in _record_document().items() if k != "as_of"}),
    ]
    print("\n" + "\n".join(lines))
