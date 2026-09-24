"""`shadow-morning` grades the ArcticDB row before the gate reads the report.

`alpha-engine-config-I11546`. The same-day parity reports for 2026-09-21,
09-22 and 09-23 each carried one `in_region_only` row, `arcticdb/universe`,
and `data_gate.evidence.read_parity` counts that verdict as an exception, so
`data.cutover_ready.parity` could not read MET while it stood. The comparator
that fills it (`python -m shadow arctic-parity`) existed, but no scheduled
workload ran it.

Pinned here:

1. `shadow-morning` runs `arctic-parity` for the box-resolved trading day AFTER
   `parity`, and `--dispatch-gate` sits on `arctic-parity`, not on `parity`;
2. the shell's ordering and exit-code rules, by EXECUTING the workload's own
   command against a stub `python`: arctic runs on parity's MET and NOT MET,
   never on parity's 2, and the workload's exit reads the whole report;
3. `arctic_parity.await_live_units` — the wait for v1's D18 manifest, since v1's
   `morning-arctic-append` measured 12:22-12:55Z, after the ~12:30Z publish;
4. `python -m shadow arctic-parity` dispatches the gate on success, on a
   refused wait and on a failed comparison, whenever a report exists;
5. `rewrite_report` keeps the summary counts the published report declared.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess

import jsonschema
import pytest

from shadow import __main__ as shadow_main
from shadow import arctic_parity, gate_dispatch
from shadow.parity import parity_key
from tests.test_shadow_morning_split_i11352 import workloads  # noqa: F401 - pytest fixture

REPO = pathlib.Path(__file__).resolve().parents[1]
SCHEMA = REPO / "contracts" / "data_parity_report.schema.json"
DAY = dt.date(2026, 9, 23)
STORE_URI = "s3://alpha-engine-research/data_collection"


def _segment(cmd: str, marker: str) -> str:
    """The shell statement starting at ``marker``, up to the next ``;``."""
    return cmd.split(marker, 1)[1].split(";", 1)[0]


# ---------------------------------------------------------------------------
# 1 — the command's shape
# ---------------------------------------------------------------------------


def test_shadow_morning_runs_arctic_parity_after_parity(workloads):  # noqa: F811
    cmd = workloads["shadow-morning"]
    assert cmd.index("python -m shadow parity ") < cmd.index("python -m shadow arctic-parity ")
    arctic = _segment(cmd, "python -m shadow arctic-parity ")
    assert "--trading-day $TD" in arctic
    assert f"--store {STORE_URI}" in arctic
    assert "--await-live-unit D18" in arctic
    assert "--await-timeout-seconds " in arctic


def test_the_wait_and_the_runtime_cap_agree():
    """The wait is a literal in the command (`_WORKLOADS` is AST-parsed as
    plain strings) and the cap is sized to include it: legs plus `parity`
    measured 42-45 min on 2026-09-22..24, the ArcticDB comparison ~2 min."""
    import ast

    index = REPO / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py"
    tree = ast.parse(index.read_text(encoding="utf-8"))
    values: dict = {}
    for node in ast.walk(tree):
        target = getattr(node, "target", None) or (getattr(node, "targets", None) or [None])[0]
        if isinstance(target, ast.Name) and target.id in {
            "_SHADOW_MORNING_AWAIT_V1_SECONDS",
            "_WORKLOAD_MAX_RUNTIME_SECONDS",
            "_WORKLOADS",
        }:
            values[target.id] = ast.literal_eval(node.value)
    wait = values["_SHADOW_MORNING_AWAIT_V1_SECONDS"]
    cmd = values["_WORKLOADS"]["shadow-morning"]
    assert f"--await-timeout-seconds {wait} " in cmd
    measured_legs_and_parity = 45 * 60
    arctic_compare_with_margin = 10 * 60
    assert measured_legs_and_parity + wait + arctic_compare_with_margin < (
        values["_WORKLOAD_MAX_RUNTIME_SECONDS"]["shadow-morning"]
    )


def test_the_gate_dispatch_moved_onto_arctic_parity(workloads):  # noqa: F811
    cmd = workloads["shadow-morning"]
    assert "--dispatch-gate" not in _segment(cmd, "python -m shadow parity ")
    assert "--dispatch-gate" in _segment(cmd, "python -m shadow arctic-parity ")
    assert cmd.count("--dispatch-gate") == 1


def test_shadow_sameday_is_unchanged(workloads):  # noqa: F811
    """The morning run rewrites the whole comparison for the day, so an
    ArcticDB verdict written the evening before would be replaced by
    `in_region_only` again; the morning is where it has to run."""
    cmd = workloads["shadow-sameday"]
    assert "arctic-parity" not in cmd
    assert "--dispatch-gate" in _segment(cmd, "python -m shadow parity ")


# ---------------------------------------------------------------------------
# 2 — the shell, executed
# ---------------------------------------------------------------------------


def _run_morning(cmd: str, tmp_path: pathlib.Path, *, legs: int, parity: int, arctic: int):
    """Run the workload's own command with `python` replaced by a stub.

    The stub answers the two guard probes so the run proceeds (a trading day
    that is a session and is not today), returns ``legs`` for each
    `shadow run`, and ``parity`` / ``arctic`` for the two comparators. Every
    `-m` invocation is logged in order.
    """
    calls = tmp_path / "calls.log"
    stub = f"""
python() {{
  if [ "$1" = "-c" ]; then
    case "$2" in
      *default_run_date*) echo 2000-01-03 ;;
      *is_trading_day*) echo 1 ;;
    esac
    return 0
  fi
  echo "$*" >> {calls}
  case "$3" in
    run) return {legs} ;;
    parity) return {parity} ;;
    arctic-parity) return {arctic} ;;
  esac
  return 99
}}
"""
    proc = subprocess.run(
        ["bash", "-c", stub + cmd],
        capture_output=True,
        text=True,
        timeout=60,
    )
    lines = calls.read_text().splitlines() if calls.exists() else []
    return proc.returncode, [line.split()[2] for line in lines]


@pytest.mark.parametrize(
    "legs,parity,arctic,expected_rc,arctic_runs",
    [
        (0, 1, 0, 0, True),  # parity NOT MET only on the in_region_only row; arctic clears it
        (0, 1, 1, 1, True),  # still NOT MET after the ArcticDB row is graded
        (0, 0, 0, 0, True),
        (0, 0, 2, 2, True),  # the ArcticDB comparison failed or was refused
        (0, 2, 0, 2, False),  # nothing published: never rewrite the previous report
        (1, 1, 0, 1, True),  # a failed leg is the exit, and the comparators still ran
    ],
)
def test_the_shell_orders_and_grades_the_two_comparators(
    workloads, tmp_path, legs, parity, arctic, expected_rc, arctic_runs  # noqa: F811
):
    rc, calls = _run_morning(
        workloads["shadow-morning"], tmp_path, legs=legs, parity=parity, arctic=arctic
    )
    expected_calls = ["run", "run", "parity"] + (["arctic-parity"] if arctic_runs else [])
    assert calls == expected_calls
    assert rc == expected_rc


# ---------------------------------------------------------------------------
# 3 — waiting for v1's own append
# ---------------------------------------------------------------------------


class _Store:
    """`data_gate.store` shape: list_keys / get_bytes / put_bytes."""

    def __init__(self, objects: "dict[str, bytes] | None" = None):
        self.objects = dict(objects or {})

    def list_keys(self, prefix: str = ""):
        return iter(sorted(k for k in self.objects if k.startswith(prefix)))

    def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise FileNotFoundError(key)
        return self.objects[key]

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.objects[key] = payload


def _manifest(unit: str, run_id: str, status: str, finished: str, day: dt.date = DAY) -> tuple[str, bytes]:
    body = {
        "schema_version": "data_run_manifest.v1",
        "unit_id": unit,
        "run_id": run_id,
        "status": status,
        "trading_day": day.isoformat(),
        "finished": finished,
    }
    return f"runs/{unit}/{day.isoformat()}/{run_id}.json", json.dumps(body).encode("utf-8")


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_the_wait_returns_at_once_when_v1_already_landed():
    store = _Store(dict([_manifest("D18", "01A", "ok", "2026-09-24T12:52:28Z")]))
    clock = _Clock()
    found = arctic_parity.await_live_units(
        store, DAY, ["D18"], timeout_seconds=2700, sleep=clock.sleep, clock=clock
    )
    assert found["D18"]["run_id"] == "01A"
    assert clock.sleeps == []


def test_the_wait_polls_until_v1_lands():
    store = _Store()
    clock = _Clock()

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        if len(clock.sleeps) == 3:
            key, body = _manifest("D18", "01B", "ok", "2026-09-24T12:55:00Z")
            store.objects[key] = body

    found = arctic_parity.await_live_units(
        store, DAY, ["D18"], timeout_seconds=2700, poll_seconds=60, sleep=sleep, clock=clock
    )
    assert found["D18"]["run_id"] == "01B"
    assert clock.sleeps == [60, 60, 60]


def test_a_failed_v1_run_keeps_the_wait_going_until_its_rerun_lands():
    """v1's recovery path re-runs a failed append; the re-run is what the live
    library ends up holding, so a failure is not an answer yet."""
    store = _Store(dict([_manifest("D18", "01A", "failed", "2026-09-24T12:40:00Z")]))
    clock = _Clock()

    def sleep(seconds: float) -> None:
        clock.sleep(seconds)
        key, body = _manifest("D18", "01B", "ok", "2026-09-24T13:10:00Z")
        store.objects[key] = body

    found = arctic_parity.await_live_units(
        store, DAY, ["D18"], timeout_seconds=2700, sleep=sleep, clock=clock
    )
    assert found["D18"]["run_id"] == "01B"


def test_the_wait_refuses_on_timeout_naming_what_it_last_saw():
    store = _Store(dict([_manifest("D18", "01A", "failed", "2026-09-24T12:40:00Z")]))
    clock = _Clock()
    with pytest.raises(arctic_parity.LiveUnitNotReady) as info:
        arctic_parity.await_live_units(
            store, DAY, ["D18"], timeout_seconds=150, poll_seconds=60, sleep=clock.sleep, clock=clock
        )
    assert "D18" in str(info.value)
    assert "'failed'" in str(info.value)
    # Never sleeps past the deadline.
    assert clock.sleeps == [60, 60, 30]


def test_the_wait_reads_only_the_trading_days_live_manifests():
    """Another day's ok, or another unit's, is not v1 having written THIS day."""
    store = _Store(
        dict(
            [
                _manifest("D18", "01A", "ok", "2026-09-23T12:52:00Z", day=dt.date(2026, 9, 22)),
                _manifest("D17", "01C", "ok", "2026-09-24T12:21:37Z"),
            ]
        )
    )
    clock = _Clock()
    with pytest.raises(arctic_parity.LiveUnitNotReady, match="no manifest"):
        arctic_parity.await_live_units(
            store, DAY, ["D18"], timeout_seconds=0, sleep=clock.sleep, clock=clock
        )


def test_the_latest_attempt_wins():
    store = _Store(
        dict(
            [
                _manifest("D18", "01A", "ok", "2026-09-24T12:30:00Z"),
                _manifest("D18", "01B", "failed", "2026-09-24T12:50:00Z"),
            ]
        )
    )
    assert arctic_parity.latest_live_manifest(store, "D18", DAY)["run_id"] == "01B"


# ---------------------------------------------------------------------------
# 4 — the CLI: compare, then dispatch the gate
# ---------------------------------------------------------------------------


def _published(report: dict) -> _Store:
    return _Store({parity_key(DAY): json.dumps(report).encode("utf-8")})


def _report() -> dict:
    return {
        "schema_version": "data_parity_report.v1",
        "trading_day": DAY.isoformat(),
        "generated_at": "2026-09-24T12:29:39Z",
        "bucket": "alpha-engine-research",
        "shadow_prefix": "staging/shadow/2026-09-23/",
        "code_sha": "abc123",
        "tolerance": {"relative": 1e-6, "absolute": 1e-9},
        "met": False,
        "summary": {
            "total": 2,
            "match": 1,
            "mismatch": 0,
            "live_missing": 0,
            "shadow_missing": 0,
            "both_missing": 0,
            "unmeasurable": 0,
            "in_region_only": 1,
            "live_superseded": 0,
            "not_applicable": 0,
            "settling_bar_keys": 1,
        },
        "excluded_units": [],
        "keys": [
            {
                "key": "staging/daily_closes/2026-09-23.parquet",
                "unit_ids": ["D17"],
                "verdict": "match",
                "comparator": "parquet",
            },
            {
                "key": "arcticdb/universe",
                "unit_ids": ["D18"],
                "verdict": "in_region_only",
                "comparator": "arcticdb",
                "unmeasurable_reason": "...",
            },
        ],
    }


_OUTCOME = {
    "ok": True,
    "error": None,
    "workflow": "nousergon/nousergon-data/data-gate.yml",
    "inputs": {"trading_day": DAY.isoformat(), "trigger": "parity-published"},
    "dispatched_at": "2026-09-24T12:56:00+00:00",
}


@pytest.fixture
def cli(monkeypatch):
    """Run `shadow arctic-parity` against an in-memory store."""
    state: dict = {"dispatched": [], "compared": 0}

    def fake_dispatch(day):
        state["dispatched"].append(day)
        return dict(_OUTCOME)

    monkeypatch.setattr(gate_dispatch, "dispatch_gate_read", fake_dispatch)

    def run(store: _Store, argv: list[str], *, compare=None, await_error=None) -> int:
        monkeypatch.setattr(shadow_main, "open_store", lambda uri, dry_run=False: store)

        def fake_run_arctic_parity(*, trading_day, store, **_kw):
            state["compared"] += 1
            if compare is None:
                raise RuntimeError("ArcticDB read failed")
            report = json.loads(store.get_bytes(parity_key(trading_day)))
            updated = arctic_parity.rewrite_report(report, compare)
            store.put_bytes(parity_key(trading_day), json.dumps(updated).encode("utf-8"))
            return updated

        monkeypatch.setattr(arctic_parity, "run_arctic_parity", fake_run_arctic_parity)

        def fake_await(*_a, **_kw):
            if await_error is not None:
                raise await_error
            return {}

        monkeypatch.setattr(arctic_parity, "await_live_units", fake_await)
        return shadow_main.main(
            ["arctic-parity", "--trading-day", DAY.isoformat(), "--store", "/unused", *argv]
        )

    state["run"] = run
    return state


def _graded(verdict: str) -> dict:
    return {"arcticdb/universe": {"verdict": verdict, "comparator": "arcticdb"}}


def test_a_graded_row_is_published_and_the_gate_reads_it(cli):
    store = _published(_report())
    rc = cli["run"](store, ["--await-live-unit", "D18", "--dispatch-gate"], compare=_graded("match"))
    assert rc == shadow_main.EXIT_MET
    published = json.loads(store.objects[parity_key(DAY)])
    by_key = {row["key"]: row for row in published["keys"]}
    assert by_key["arcticdb/universe"]["verdict"] == "match"
    assert published["summary"]["in_region_only"] == 0
    assert published["gate_dispatch"] == _OUTCOME
    assert cli["dispatched"] == [DAY]
    jsonschema.validate(published, json.loads(SCHEMA.read_text(encoding="utf-8")))


def test_a_refused_wait_leaves_the_row_and_still_dispatches_the_gate(cli):
    """v1 not finished: the ArcticDB row stays `in_region_only` — never a
    grade against a half-written library — and the gate still reads the report
    `parity` just published, exactly as it did before this change."""
    store = _published(_report())
    rc = cli["run"](
        store,
        ["--await-live-unit", "D18", "--dispatch-gate"],
        compare=_graded("match"),
        await_error=arctic_parity.LiveUnitNotReady("D18: no manifest"),
    )
    assert rc == shadow_main.EXIT_UNMEASURED
    assert cli["compared"] == 0
    published = json.loads(store.objects[parity_key(DAY)])
    assert {row["key"]: row["verdict"] for row in published["keys"]}["arcticdb/universe"] == "in_region_only"
    assert published["gate_dispatch"] == _OUTCOME


def test_a_failed_comparison_still_dispatches_the_gate(cli):
    store = _published(_report())
    rc = cli["run"](store, ["--dispatch-gate"], compare=None)
    assert rc == shadow_main.EXIT_UNMEASURED
    assert cli["dispatched"] == [DAY]


def test_no_report_means_no_gate_dispatch(cli):
    rc = cli["run"](_Store(), ["--dispatch-gate"], compare=None)
    assert rc == shadow_main.EXIT_UNMEASURED
    assert cli["dispatched"] == []


def test_the_manual_arctic_parity_workload_never_dispatches(cli):
    """The standalone `arctic-parity` workload replays a past day; without the
    flag it must not move the board."""
    store = _published(_report())
    rc = cli["run"](store, [], compare=_graded("mismatch"))
    assert rc == shadow_main.EXIT_NOT_MET
    assert cli["dispatched"] == []
    assert "gate_dispatch" not in json.loads(store.objects[parity_key(DAY)])


# ---------------------------------------------------------------------------
# 5 — the rewrite keeps what the report declared
# ---------------------------------------------------------------------------


def test_rewrite_report_keeps_declared_counts_and_the_settling_breakdown():
    """Measured on the 2026-09-23 report: `settling_bar_keys: 6`,
    `not_applicable: 1`, `live_superseded: 0`. A rewrite rebuilt from a fixed
    verdict list dropped all three."""
    report = _report()
    report["summary"]["not_applicable"] = 1
    report["summary"]["settling_bar_keys"] = 6
    updated = arctic_parity.rewrite_report(report, _graded("match"))
    summary = updated["summary"]
    assert summary["settling_bar_keys"] == 6
    assert summary["live_superseded"] == 0
    assert summary["not_applicable"] == 0  # re-derived from the rows, which carry none
    assert summary["in_region_only"] == 0
    assert summary["match"] == 2
    assert summary["total"] == 2
    jsonschema.validate(updated, json.loads(SCHEMA.read_text(encoding="utf-8")))
