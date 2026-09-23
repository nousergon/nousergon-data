"""The parity publish dispatches the gate read, and a failed dispatch is never fatal.

`alpha-engine-config-I11361`. `data_gate.read.TRIGGERS` declared
`parity-published` from `-I11355` on, but nothing emitted it, so the gate read
followed the same-day publish (~23:43Z) on a 01:30Z cron. Brian ruled option
(a): a GitHub App installed on this repository only, its credentials in SSM
under `/alpha-engine/data-spot/`, minted per run on the data-spot box.

Pinned here:

1. both ends of the dispatch agree: the payload `shadow.gate_dispatch` sends is
   exactly the input set `data-gate.yml`'s `workflow_dispatch` declares, and
   the workflow turns it into `--trigger parity-published` for that day;
2. the minted token is narrowed to `actions: write` on this one repository,
   from the data-spot App's SSM prefix and not the groomer's;
3. any failure (mint, HTTP, non-204) is recorded, never raised;
4. the outcome lands in the published report as `gate_dispatch`, and the
   report still validates against its contract;
5. only the two SCHEDULED parity commands dispatch, never a replay.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
import types

import jsonschema
import pytest
import yaml

from data_gate import read as read_module
from shadow import __main__ as shadow_main
from shadow import gate_dispatch
from tests.test_shadow_morning_split_i11352 import workloads  # noqa: F401 - pytest fixture

REPO = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "data-gate.yml"
SCHEMA = REPO / "contracts" / "data_parity_report.schema.json"
DAY = dt.date(2026, 9, 22)


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _dispatch_inputs() -> dict:
    on_block = _workflow().get("on") or _workflow().get(True)
    return on_block["workflow_dispatch"]["inputs"]


# ---------------------------------------------------------------------------
# 1 — the two ends of the dispatch agree
# ---------------------------------------------------------------------------


def test_the_payload_names_only_inputs_the_workflow_declares():
    payload = gate_dispatch.dispatch_payload(DAY)
    declared = _dispatch_inputs()
    assert set(payload["inputs"]) <= set(declared)
    assert payload["inputs"] == {"trading_day": "2026-09-22", "trigger": "parity-published"}
    assert payload["ref"] == "main"


def test_the_trigger_sent_is_a_declared_choice_and_a_declared_trigger():
    trigger = gate_dispatch.dispatch_payload(DAY)["inputs"]["trigger"]
    assert trigger in _dispatch_inputs()["trigger"]["options"]
    assert trigger in read_module.TRIGGERS


def test_the_workflow_records_the_dispatched_trigger_and_day():
    env = _workflow()["jobs"]["read-gate"]["env"]
    assert "inputs.trigger == 'parity-published'" in env["GATE_TRIGGER"]
    assert "'parity-published'" in env["GATE_TRIGGER"]
    assert "inputs.trading_day" in env["GATE_TRADING_DAY"]
    # An empty day (a human dispatch that left it blank) keeps today's reading.
    assert "inputs.trading_day != ''" in env["GATE_TRADING_DAY"]


def test_the_dispatch_targets_this_repositorys_gate_workflow():
    assert gate_dispatch.dispatch_url() == (
        "https://api.github.com/repos/nousergon/nousergon-data/actions/workflows/data-gate.yml/dispatches"
    )
    assert WORKFLOW.name == gate_dispatch.GATE_WORKFLOW


# ---------------------------------------------------------------------------
# 2 — the token is narrow, and it is the data-spot App's
# ---------------------------------------------------------------------------


def test_the_minted_token_is_narrowed_to_actions_write_on_this_repo(monkeypatch):
    seen = {}

    def fake_installation_token(**kwargs):
        seen.update(kwargs)
        return "ghs_test"

    module = types.ModuleType("nousergon_lib.github_app")
    module.installation_token = fake_installation_token  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nousergon_lib.github_app", module)

    assert gate_dispatch.mint_token() == "ghs_test"
    assert seen["ssm_prefix"] == "/alpha-engine/data-spot/"
    assert seen["permissions"] == {"actions": "write"}
    assert seen["repositories"] == ["nousergon-data"]


def test_the_ssm_prefix_is_not_the_groomers():
    """The whole point of ruling (a) over reusing an existing identity: this
    box's key can do one thing on one repository."""
    assert gate_dispatch.APP_SSM_PREFIX != "/alpha-engine/groom/"
    assert gate_dispatch.APP_SSM_PREFIX.startswith("/alpha-engine/")


# ---------------------------------------------------------------------------
# 3 — never fatal
# ---------------------------------------------------------------------------


def test_a_204_is_recorded_as_ok():
    calls = []
    outcome = gate_dispatch.dispatch_gate_read(
        DAY, mint=lambda: "tok", post=lambda url, token, payload: calls.append((url, token, payload)) or 204
    )
    assert outcome["ok"] is True
    assert outcome["error"] is None
    assert outcome["dispatched_at"]
    assert calls == [(gate_dispatch.dispatch_url(), "tok", gate_dispatch.dispatch_payload(DAY))]


def test_a_mint_failure_is_recorded_not_raised():
    def broken_mint():
        raise RuntimeError("App credential unreadable at SSM /alpha-engine/data-spot/github_app_id")

    outcome = gate_dispatch.dispatch_gate_read(DAY, mint=broken_mint, post=lambda *a: 204)
    assert outcome["ok"] is False
    assert "RuntimeError" in outcome["error"]
    assert "github_app_id" in outcome["error"]
    assert outcome["dispatched_at"] is None


def test_an_http_failure_is_recorded_not_raised():
    def broken_post(url, token, payload):
        raise OSError("connection reset")

    outcome = gate_dispatch.dispatch_gate_read(DAY, mint=lambda: "tok", post=broken_post)
    assert outcome["ok"] is False
    assert "connection reset" in outcome["error"]


def test_a_non_204_is_not_ok():
    outcome = gate_dispatch.dispatch_gate_read(DAY, mint=lambda: "tok", post=lambda *a: 200)
    assert outcome["ok"] is False
    assert "HTTP 200" in outcome["error"]


def test_a_long_error_is_bounded():
    def noisy_mint():
        raise RuntimeError("x" * 5000)

    outcome = gate_dispatch.dispatch_gate_read(DAY, mint=noisy_mint)
    assert len(outcome["error"]) <= gate_dispatch.MAX_ERROR_CHARS


# ---------------------------------------------------------------------------
# 4 — the outcome is published in the report, which stays valid
# ---------------------------------------------------------------------------


class _Store:
    def __init__(self):
        self.puts: list[tuple[str, bytes]] = []

    def put_bytes(self, key: str, payload: bytes) -> None:
        self.puts.append((key, payload))


def _minimal_report() -> dict:
    return {
        "schema_version": 2,
        "trading_day": "2026-09-22",
        "generated_at": "2026-09-22T23:43:04Z",
        "bucket": "alpha-engine-research",
        "shadow_prefix": "staging/shadow/2026-09-22/",
        "code_sha": "abc123",
        "tolerance": {"relative": 1e-6, "absolute": 1e-9},
        "met": False,
        "summary": {},
        "excluded_units": [],
        "keys": [],
    }


@pytest.mark.parametrize("ok", [True, False])
def test_the_outcome_is_republished_as_gate_dispatch_and_validates(monkeypatch, ok):
    outcome = {
        "ok": ok,
        "error": None if ok else "RuntimeError: no App installed",
        "workflow": "nousergon/nousergon-data/data-gate.yml",
        "inputs": {"trading_day": "2026-09-22", "trigger": "parity-published"},
        "dispatched_at": "2026-09-22T23:43:05+00:00" if ok else None,
    }
    monkeypatch.setattr(gate_dispatch, "dispatch_gate_read", lambda day: outcome)
    store = _Store()
    document = _minimal_report()

    shadow_main._dispatch_gate(store, "parity/2026-09-22.json", document, DAY)

    assert len(store.puts) == 1
    key, payload = store.puts[0]
    assert key == "parity/2026-09-22.json"
    published = json.loads(payload)
    assert published["gate_dispatch"] == outcome

    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    gate_dispatch_schema = schema["properties"]["gate_dispatch"]
    jsonschema.validate(published["gate_dispatch"], gate_dispatch_schema)


def test_gate_dispatch_is_optional_in_the_contract():
    """Reports written before this change, and every manual or replay run,
    carry no `gate_dispatch`."""
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert "gate_dispatch" in schema["properties"]
    assert "gate_dispatch" not in schema["required"]


def test_the_flag_is_off_unless_asked_for():
    args = shadow_main._parser().parse_args(
        ["parity", "--trading-day", "2026-09-22", "--store", "/tmp/x"]
    )
    assert args.dispatch_gate is False
    args = shadow_main._parser().parse_args(
        ["parity", "--trading-day", "2026-09-22", "--store", "/tmp/x", "--dispatch-gate"]
    )
    assert args.dispatch_gate is True


# ---------------------------------------------------------------------------
# 5 — only the scheduled parity commands dispatch
# ---------------------------------------------------------------------------


def test_only_the_scheduled_sameday_and_morning_parity_commands_dispatch(workloads):  # noqa: F811
    """A replay (`shadow-parity`, `shadow-weekday`) of a past day must never
    move the board: the gate read writes `board/latest.json`, and a dispatch
    for an old trading day would overwrite today's reading with it."""
    dispatching = {name for name, command in workloads.items() if command and "--dispatch-gate" in command}
    assert dispatching == {"shadow-sameday", "shadow-morning"}
    for name in dispatching:
        parity_segment = workloads[name].split("python -m shadow parity", 1)[1]
        assert "--dispatch-gate" in parity_segment.split(";", 1)[0], name
