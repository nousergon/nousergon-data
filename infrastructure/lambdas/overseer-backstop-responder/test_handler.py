"""Tests for the overseer-backstop-responder (alpha-engine-config-I4480).

The properties that matter are behavioural, not cosmetic:

  1. an alarm NOT in the reviewed allowlist never triggers an action;
  2. a second firing inside the cooldown window escalates instead of retrying;
  3. the page still goes out when evidence-gathering partially fails — a
     backstop that crashes while reporting an outage is worse than one that
     reports "could not read X";
  4. the responder never raises, whatever AWS does to it.

Every boto3 client is faked. Nothing here touches AWS, SNS, or Telegram.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import index  # noqa: E402


class NoSuchKey(Exception):
    """Named to match botocore's real exception — _is_missing_key matches on
    the class NAME, so a differently-named fake would not exercise the path."""


class FakeS3:
    def __init__(self, existing: dict | None = None, raise_on_get: bool = False):
        self.objects = existing or {}
        self.puts: list[str] = []
        self.raise_on_get = raise_on_get
        self.exceptions = type("E", (), {"NoSuchKey": NoSuchKey})

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg casing
        if self.raise_on_get:
            raise RuntimeError("s3 down")
        if Key not in self.objects:
            raise NoSuchKey()
        body = json.dumps(self.objects[Key]).encode()
        return {"Body": type("B", (), {"read": lambda self, b=body: b})()}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.puts.append(Key)
        self.objects[Key] = json.loads(Body)

    def list_objects_v2(self, Bucket, Prefix):  # noqa: N803
        return {"Contents": []}


class FakeLambda:
    def __init__(self):
        self.invocations: list[dict] = []

    def get_function_configuration(self, FunctionName):  # noqa: N803
        return {"Environment": {"Variables": {}}}

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803
        self.invocations.append(
            {"function": FunctionName, "payload": json.loads(Payload or b"{}")}
        )
        body = json.dumps({"routed": True, "verdict": {"launched": True}}).encode()
        return {"Payload": type("P", (), {"read": lambda self, b=body: b})()}


class FakeSqs:
    """Validates attribute names the way SQS actually does.

    The first version of this fake returned whatever it was asked for,
    including `ApproximateAgeOfOldestMessage` — which is an AWS/SQS *metric*,
    not a queue attribute. Real SQS answers `InvalidAttributeName` and fails
    the WHOLE call, taking the depth reading with it. The permissive fake
    turned an absent guarantee into a believed one, which is the exact class
    overseer-policy §4 inv. 13 exists to prevent; the defect surfaced only on
    a live invoke of the deployed function.
    """

    VALID = {
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
        "All",
    }

    def get_queue_url(self, QueueName):  # noqa: N803
        return {"QueueUrl": f"https://sqs/{QueueName}"}

    def get_queue_attributes(self, QueueUrl, AttributeNames):  # noqa: N803
        bad = set(AttributeNames) - self.VALID
        if bad:
            raise type("InvalidAttributeName", (Exception,), {})(
                f"Unknown Attribute {sorted(bad)[0]}."
            )
        return {"Attributes": {
            "ApproximateNumberOfMessages": "99",
            "ApproximateNumberOfMessagesNotVisible": "0",
        }}


class FakeCw:
    def get_metric_statistics(self, **kw):
        from datetime import datetime as _dt
        return {"Datapoints": [
            {"Sum": 6.0, "Maximum": 7200.0,
             "Timestamp": _dt(2026, 7, 28, 16, 0, tzinfo=timezone.utc)}
        ]}


@pytest.fixture
def wired(monkeypatch):
    """Wire fakes in and capture the page instead of sending it."""
    state = {
        "s3": FakeS3(), "lambda": FakeLambda(), "sqs": FakeSqs(),
        "cloudwatch": FakeCw(), "pages": [],
    }
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: state[name])
    }))
    monkeypatch.setattr(index, "_telegram",
                        lambda text: (state["pages"].append(text), True)[1])
    monkeypatch.setattr(index, "RECOVERY_ENABLED", True)
    return state


def _sns(alarm: str, reason: str = "Threshold crossed") -> dict:
    return {"Records": [{"Sns": {"Message": json.dumps(
        {"AlarmName": alarm, "NewStateReason": reason})}}]}


INTAKE_AGE = "alpha-engine-watch-plane-overseer-intake-age"
PROBE_ERRORS = "alpha-engine-watch-plane-overseer-liveness-probe-errors"


# ── Property 1: the allowlist is the entire authority ───────────────────────


def test_unmapped_alarm_takes_no_action(wired):
    out = index.handler(_sns("some-unrelated-billing-alarm"), None)
    assert wired["lambda"].invocations == [], "an unmapped alarm must never act"
    assert "allowlist" in out["outcome"]["skipped"]
    assert wired["pages"], "it must still page — reporting is unconditional"


def test_intake_age_alarm_redispatches_the_drain_once(wired):
    index.handler(_sns(INTAKE_AGE), None)
    invs = wired["lambda"].invocations
    assert len(invs) == 1
    assert invs[0]["function"] == index.ROUTER_FUNCTION
    assert invs[0]["payload"]["playbook"] == "alert-drain"
    assert invs[0]["payload"]["payload"]["is_drill"] == "false", (
        "a drill would prove the pipe works while leaving the backlog untouched"
    )


def test_probe_errors_alarm_reinvokes_the_probe(wired):
    index.handler(_sns(PROBE_ERRORS), None)
    invs = wired["lambda"].invocations
    assert len(invs) == 1
    assert invs[0]["function"] == index.PROBE_FUNCTION


def test_kill_switch_disables_action_but_not_the_page(wired, monkeypatch):
    monkeypatch.setattr(index, "RECOVERY_ENABLED", False)
    out = index.handler(_sns(INTAKE_AGE), None)
    assert wired["lambda"].invocations == []
    assert "kill switch" in out["outcome"]["skipped"]
    assert wired["pages"]


def test_every_allowlist_entry_is_a_known_action():
    """Guards against a typo'd action name silently becoming a no-op."""
    for alarm, spec in index.ALARM_ACTIONS.items():
        assert spec["action"] in ("redispatch", "invoke_probe"), alarm
        assert spec.get("rationale"), f"{alarm} must carry a rationale"
        if spec["action"] == "redispatch":
            assert spec.get("playbook") and isinstance(spec.get("payload"), dict)


# ── Property 2: one attempt per window, then escalate ───────────────────────


def test_second_firing_in_window_escalates_instead_of_retrying(wired):
    index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1
    out = index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1, "must NOT retry in-window"
    assert out["outcome"]["escalated"] is True
    assert "SECOND firing" in wired["pages"][-1]


def test_a_different_alarm_is_not_blocked_by_anothers_cooldown(wired):
    index.handler(_sns(INTAKE_AGE), None)
    index.handler(_sns(PROBE_ERRORS), None)
    assert len(wired["lambda"].invocations) == 2


def test_window_key_is_stable_within_and_changes_across_windows(monkeypatch):
    monkeypatch.setattr(index, "COOLDOWN_HOURS", 6)
    at = lambda h: datetime(2026, 7, 28, h, 30, tzinfo=timezone.utc)  # noqa: E731
    assert index._window_start(at(1)) == index._window_start(at(5))
    assert index._window_start(at(5)) != index._window_start(at(7))


def test_cooldown_state_unreadable_fails_open(wired, monkeypatch):
    """If S3 cannot be read we make ONE extra bounded attempt rather than none —
    and the alternative (fail closed) would silently disable recovery exactly
    when the plane is least healthy."""
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            FakeS3(raise_on_get=True) if name == "s3" else wired[name]
        ))
    }))
    index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1


# ── Property 3+4: the page survives partial blindness; nothing raises ───────


def test_page_still_sent_when_every_evidence_call_fails(monkeypatch):
    pages: list[str] = []

    class Dead:
        def __getattr__(self, _name):
            def _boom(*a, **kw):
                raise RuntimeError("aws down")
            return _boom

    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: Dead())
    }))
    monkeypatch.setattr(index, "_telegram",
                        lambda text: (pages.append(text), True)[1])
    out = index.handler(_sns(INTAKE_AGE), None)
    assert pages, "the page is the primary deliverable and must survive"
    assert "UNREADABLE" in pages[0], "blindness must be named, not hidden"
    assert out["alarm"] == INTAKE_AGE


def test_malformed_sns_event_still_pages(wired):
    out = index.handler({"Records": [{"Sns": {"Message": "not json"}}]}, None)
    assert wired["pages"]
    assert out["alarm"] == "(unknown)"


def test_empty_event_does_not_raise(wired):
    assert index.handler({}, None)["alarm"] == "(unknown)"


def test_router_returning_a_decline_is_reported_as_failure(wired, monkeypatch):
    class Declining(FakeLambda):
        def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803
            self.invocations.append({"function": FunctionName, "payload": {}})
            body = json.dumps({"routed": False, "reason": "playbook_disabled"}).encode()
            return {"Payload": type("P", (), {"read": lambda self, b=body: b})()}

    declining = Declining()
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            declining if name == "lambda" else wired[name]
        ))
    }))
    out = index.handler(_sns(INTAKE_AGE), None)
    assert out["outcome"]["result"]["ok"] is False
    assert "FAILED" in wired["pages"][-1]


# ── The dumbness invariant (§4 inv. 3) ──────────────────────────────────────


def test_no_agent_bus_or_queue_dependency_in_the_source():
    """§4 inv. 3: the backstop stays dumb forever.

    Asserted over the parsed AST, not the raw text — the module docstring
    NAMES the forbidden dependencies in order to explain why they are absent,
    so a substring scan would flag its own rationale. Imports and attribute
    calls are the things that can actually erode the invariant.
    """
    import ast

    tree = ast.parse(Path(index.__file__).read_text())

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    forbidden_imports = {"krepis", "flow_doctor", "nousergon_lib", "anthropic",
                         "openai", "requests"}
    assert not (imported & forbidden_imports), (
        f"{sorted(imported & forbidden_imports)} imported by the backstop "
        f"responder — it must have no agent or bus dependency "
        f"(overseer-policy §4 inv. 3)"
    )
    assert imported <= {"__future__", "json", "os", "urllib", "datetime", "boto3"}, (
        f"unexpected import(s) {sorted(imported - {'__future__', 'json', 'os', 'urllib', 'datetime', 'boto3'})} "
        f"— the backstop's dependency set is boto3 + stdlib, by design"
    )

    called = {n.func.attr for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    forbidden_calls = {"receive_message", "delete_message", "publish"}
    assert not (called & forbidden_calls), (
        f"{sorted(called & forbidden_calls)} called — the backstop must never "
        f"consume the intake queue or re-publish onto the bus it may be rescuing"
    )


def test_unreadable_state_still_claims_so_the_second_firing_escalates(wired, monkeypatch):
    """Regression anchor for the unbounded-retry loop.

    The first version returned early when the state read failed, so the claim
    was never written — every subsequent firing also read-failed and also
    acted. Unbounded retries in the one component whose whole contract is
    'one bounded attempt'.
    """
    broken = FakeS3(raise_on_get=True)
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            broken if name == "s3" else wired[name]
        ))
    }))
    index.handler(_sns(INTAKE_AGE), None)
    assert broken.puts, "the claim must be written even when the read failed"

    # Second firing: the claim is now present, so the read succeeds and escalates.
    broken.raise_on_get = False
    out = index.handler(_sns(INTAKE_AGE), None)
    assert out["outcome"].get("escalated") is True
    assert len(wired["lambda"].invocations) == 1, "must not act twice in-window"


def test_queue_state_reports_depth_and_age_without_an_invalid_attribute(wired):
    """Regression anchor: age comes from CloudWatch, depth from SQS. Asking SQS
    for the age attribute fails the whole call and loses the depth too."""
    out = index.handler(_sns("unmapped-for-this-test"), None)
    intake = out["evidence"]["queues"]["intake"]
    assert "UNREADABLE" not in intake, intake
    assert "99 visible" in intake
    assert "oldest 2h00m" in intake


OWN_ALARM = "alpha-engine-watch-plane-backstop-responder-errors"


def test_the_responders_own_alarm_is_never_actionable(wired):
    """The responder's own error alarm publishes to the topic it subscribes to,
    so it invokes itself. That is safe ONLY because its own alarm has no
    allowlist entry — it reports and stops. If someone ever adds one, this
    fails. (The human leg of that topic is an email subscription sharing no
    component with the responder, which is what satisfies inv. 1; the
    self-invoke is a harmless second reader, not the terminating path.)"""
    assert OWN_ALARM not in index.ALARM_ACTIONS
    out = index.handler(_sns(OWN_ALARM), None)
    assert wired["lambda"].invocations == []
    assert "allowlist" in out["outcome"]["skipped"]
    assert wired["pages"], "it must still page with full plane state"
