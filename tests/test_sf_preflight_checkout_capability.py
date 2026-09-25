"""CAP_CHECKOUT on the weekly box (alpha-engine-config-I11568).

``tool_contracts`` is REQUIRED and needs CAP_CHECKOUT, which neither the
WeeklyPreflight Lambda nor the on-spot pass claimed, so a required check ran
nowhere. Granting the capability is only honest if the checks can actually run
on the box, so this file pins the three things that make that true:

1. ``CHECKOUT_SIBLINGS`` names every sibling the CAP_CHECKOUT checks read —
   derived from the committed definition's ``commands.$`` for tool_contracts,
   and from the checks' own ``_sibling_repo(...)`` literals for the other two.
2. The weekly box's bootstrap clones every one of them.
3. tool_contracts reads real requirement lines (extras included) and builds
   its Step Functions client with an explicit region, so it does not fail on
   the box's environment rather than on the system it probes.
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

import sf_preflight as sp

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFINITION = _REPO_ROOT / "infrastructure" / "step_function.json"
_DISPATCHER = (
    _REPO_ROOT / "infrastructure" / "lambdas"
    / "weekly-freshness-spot-dispatcher" / "index.py"
)


def _aliases(name: str) -> set:
    return set(sp._SIBLING_REPO_ALIASES.get(name, (name,)))


def _covered(name: str) -> bool:
    return any(_aliases(name) & _aliases(s) for s in sp.CHECKOUT_SIBLINGS)


def _definition_checkouts() -> set:
    found: set = set()

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "commands.$" and isinstance(v, str):
                    found.update(sp.scan_command_checkouts(v)[:1])
                else:
                    walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(json.loads(_DEFINITION.read_text(encoding="utf-8")))
    return found


# ── 1. what CAP_CHECKOUT means ───────────────────────────────────────────────


def test_every_checkout_the_definition_shells_out_to_is_a_declared_sibling():
    checkouts = _definition_checkouts()
    assert checkouts, "the committed definition names no checkout venv at all"
    missing = sorted(c for c in checkouts if not _covered(c))
    assert not missing, (
        f"tool_contracts reads these checkouts' requirements but CAP_CHECKOUT "
        f"does not require them: {missing} — add to sf_preflight.CHECKOUT_SIBLINGS "
        f"and clone them on the weekly box"
    )


@pytest.mark.parametrize("check", [
    sp.check_price_cards_cover_all_models,
    sp.check_recursion_budget_for_response_format,
])
def test_every_sibling_a_checkout_check_reads_is_declared(check):
    names = re.findall(r'_sibling_repo\("([^"]+)"\)', inspect.getsource(check))
    assert names, f"{check.__name__} reads no sibling — re-examine its capability"
    for name in names:
        assert _covered(name), f"{check.__name__} reads {name}, not in CHECKOUT_SIBLINGS"


def test_every_declared_sibling_has_an_alias_entry():
    for name in sp.CHECKOUT_SIBLINGS:
        assert name in sp._SIBLING_REPO_ALIASES, name


# ── 2. the weekly box actually carries them ──────────────────────────────────


def _weekly_box_checkouts() -> set:
    src = _DISPATCHER.read_text(encoding="utf-8")
    public = set(re.findall(r'checkout="/home/ec2-user/([A-Za-z0-9_.-]+)"', src))
    # The private config repo is cloned in the handler's own tail.
    private = set(re.findall(
        r"^\s*/home/ec2-user/([A-Za-z0-9_.-]+) \|\| fail \"[^\"]* clone failed\"",
        src, re.M,
    ))
    return public | private


@pytest.mark.parametrize("sibling", sp.CHECKOUT_SIBLINGS)
def test_the_weekly_box_bootstrap_clones_every_declared_sibling(sibling):
    cloned = _weekly_box_checkouts()
    assert _aliases(sibling) & cloned, (
        f"{sibling}: the weekly box clones none of {sorted(_aliases(sibling))} "
        f"(it clones {sorted(cloned)}), so sf_preflight_on_spot would withhold "
        f"CAP_CHECKOUT and required tool_contracts would never run"
    )


def test_every_box_checkout_is_chowned_to_ec2_user():
    """The on-spot pass runs as ec2-user; a root-owned clone is readable today
    but is the one checkout a later stage's `git pull` would trip on."""
    src = _DISPATCHER.read_text(encoding="utf-8")
    chown = re.search(r"chown -R ec2-user:ec2-user (.*?)\|\| fail \"chown failed\"", src, re.S)
    assert chown
    for checkout in _weekly_box_checkouts():
        assert f"/home/ec2-user/{checkout}" in chown.group(1), checkout


# ── 3. tool_contracts reads real content, in the box's environment ───────────


@pytest.mark.parametrize("line,expected", [
    # crucible-dashboard/requirements.txt, measured 2026-09-25.
    ("krepis[flow-doctor, openai]==0.59.70\n", "0.59.70"),
    ("krepis[openai]==0.59.70  # pinned per §139\n", "0.59.70"),
    ("krepis==0.59.53  # pinned per §139 — first-party\n", "0.59.53"),
    ("krepis>=0.18.8\n", "0.18.8"),
    ("krepis[flow-doctor] >= 0.20.0\n", "0.20.0"),
])
def test_read_pinned_version_handles_extras(tmp_path, line, expected):
    req = tmp_path / "requirements.txt"
    req.write_text("boto3>=1.34\n    #   krepis\n" + line)
    assert sp._read_pinned_version(req) == expected


class _Sfn:
    def __init__(self, definition):
        self._definition = definition

    def describe_state_machine(self, **_kw):
        return {"definition": self._definition}


def _patch_boto3(monkeypatch, definition, calls):
    import boto3

    def _client(service, **kwargs):
        calls.append((service, kwargs))
        return _Sfn(definition)

    monkeypatch.setattr(boto3, "client", _client)


def test_tool_contracts_names_its_region(monkeypatch):
    """The box's SSM shell exports no AWS_DEFAULT_REGION (I11567)."""
    calls: list = []
    _patch_boto3(monkeypatch, _DEFINITION.read_text(encoding="utf-8"), calls)
    monkeypatch.setattr(sp, "_sibling_repo", lambda name: None)
    sp.check_tool_contracts(None)
    assert calls == [("stepfunctions", {"region_name": sp._REGION})]


def _layout(tmp_path, pins: dict) -> "callable":
    """A box-shaped sibling layout: each checkout carries a requirements.txt."""
    roots = {}
    for name, line in pins.items():
        root = tmp_path / name
        root.mkdir()
        (root / "requirements.txt").write_text(line)
        roots[name] = root

    def _resolve(name):
        for alias in sp._SIBLING_REPO_ALIASES.get(name, (name,)):
            if alias in roots:
                return roots[alias]
        return None

    return _resolve


def test_tool_contracts_passes_the_committed_definition_on_real_pin_shapes(
    monkeypatch, tmp_path,
):
    """rehearsal-2026-09-25 prediction: run against the real crucible-dashboard
    pin line, tool_contracts reported 18 violations "pin not found" — every
    one the parser missing ``krepis[...]==``, none a real contract break."""
    _patch_boto3(monkeypatch, _DEFINITION.read_text(encoding="utf-8"), [])
    monkeypatch.setattr(sp, "_sibling_repo", _layout(tmp_path, {
        "alpha-engine-dashboard": "krepis[flow-doctor, openai]==0.59.70\n",
        "crucible-research": "krepis==0.59.53  # pinned per §139\n",
        "alpha-engine-data": "krepis[openai]==0.59.70  # pinned\n",
    }))
    result = sp.check_tool_contracts(None)
    assert result.status == "ok", result.details
    assert result.details["checked"] >= 20


def test_tool_contracts_still_fails_a_pin_that_is_too_old(monkeypatch, tmp_path):
    """The parser fix must not have made the check unable to fail."""
    _patch_boto3(monkeypatch, _DEFINITION.read_text(encoding="utf-8"), [])
    monkeypatch.setattr(sp, "_sibling_repo", _layout(tmp_path, {
        "alpha-engine-dashboard": "krepis[flow-doctor, openai]==0.16.2\n",
        "crucible-research": "krepis==0.59.53\n",
        "alpha-engine-data": "krepis==0.59.70\n",
    }))
    result = sp.check_tool_contracts(None)
    assert result.status == "fail"
    assert any("too old" in f for f in result.details["failures"])


def test_tool_contracts_fails_loud_when_a_governing_checkout_is_absent(
    monkeypatch, tmp_path,
):
    """A box missing a checkout the definition shells out to is a named
    failure from the check itself — never a pass over the repos it did see."""
    _patch_boto3(monkeypatch, _DEFINITION.read_text(encoding="utf-8"), [])
    monkeypatch.setattr(sp, "_sibling_repo", _layout(tmp_path, {
        "alpha-engine-dashboard": "krepis[flow-doctor, openai]==0.59.70\n",
        "alpha-engine-data": "krepis==0.59.70\n",
    }))
    result = sp.check_tool_contracts(None)
    assert result.status == "fail"
    assert any("not checked out as sibling" in f for f in result.details["failures"])
