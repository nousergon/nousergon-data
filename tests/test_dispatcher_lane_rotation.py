"""Test per-lane anti-affinity rotation in the scheduled-groom-dispatcher
(config#4989).

Tests that ``_lane_rotation_offset`` produces distinct starting points for
co-launched lanes, and that applying the offset to the instance type and
subnet lists puts each lane's first attempt in a different pool.
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DISPATCHER_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "scheduled-groom-dispatcher"


def _load_dispatcher_module():
    """Load the dispatcher's index.py as an importable module, stubbing out
    external dependencies unavailable in this test environment.
    krepis IS available (installed), so no stub needed for the krepis chain.
    Stub only flow_doctor_telegram (not installed here) and the stubs from
    test_groom_cycle_notifications.py that this mirrors."""
    # Stub flow_doctor_telegram (not available in this test environment)
    _stub_module("flow_doctor_telegram",
                 notify_via_flow_doctor=lambda *a, **k: True)

    # Stub nousergon_lib submodules that the dispatcher module-level code
    # touches at import time (the rest are lazily accessed inside functions).
    # groom_eligibility: needs VALID_ISSUE_FILTERS, TIERS, etc.
    _stub_module("nousergon_lib.groom_eligibility",
                 VALID_ISSUE_FILTERS={"mid-only", "low", "high",
                                      "gated-reverify"},
                 TIERS={"low", "mid", "high"},
                 is_actionable=lambda labels: "mid",
                 RULING_PENDING_LABEL="ruling:pending-exec",
                 decide_slot=lambda *a, **k: None,
                 decide_trigger=lambda *a, **k: [],
                 filter_tiers=lambda f: ["mid"],
                 FALLBACK_TIER_MODELS={})
    # spot_dispatch: stubbed to avoid real AWS calls
    _stub_module("nousergon_lib.spot_dispatch",
                 launch_with_fallback=lambda *a, **k: ("i-fake", "spot"),
                 wait_ssm_online=lambda *a, **k: None,
                 send_async_command=lambda *a, **k: "cmd-fake",
                 running_instance_ids=lambda *a, **k: [],
                 terminate_on_failure=lambda *a, **k: None,
                 SpotProbeError=type("SpotProbeError", (Exception,), {}))
    # flow_doctor_fleet: FleetTelegramTopic enum
    ft_mod = types.ModuleType("nousergon_lib.flow_doctor_fleet")
    ft_mod.FleetTelegramTopic = type("FleetTelegramTopic", (), {
        "GROOM": "groom",
        "GROOM_CYCLE": "groom-cycle",
    })
    sys.modules["nousergon_lib.flow_doctor_fleet"] = ft_mod

    # ec2_spot shim (re-exports krepis.ec2_spot — already importable)
    _stub_module("nousergon_lib.ec2_spot")
    # nousergon_lib (package) must exist after its submodules
    if "nousergon_lib" not in sys.modules:
        sys.modules["nousergon_lib"] = types.ModuleType("nousergon_lib")

    spec = importlib.util.spec_from_file_location(
        "groom_dispatcher_under_test", DISPATCHER_DIR / "index.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stub_module(name: str, **attrs):
    """Ensure a stub exists in sys.modules under ``name``, with optional
    ``attrs``. Does NOT check if the module is already importable — this
    function is deliberately unconditional so a test can force its stubs
    into sys.modules before importlib resolves the real module."""
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod


@pytest.fixture(scope="module")
def dispatcher():
    mod = _load_dispatcher_module()
    return mod


# ── Tests ────────────────────────────────────────────────────────────────────


class TestLaneRotationIndex:
    def test_all_concurrent_lanes_have_entries(self, dispatcher):
        for lane in ("mid-only", "low", "high", "sweep"):
            assert lane in dispatcher._LANE_ROTATION_INDEX, (
                f"missing lane: {lane}")

    def test_co_launched_offsets_are_distinct(self, dispatcher):
        offsets = {lane: dispatcher._lane_rotation_offset(lane)
                   for lane in ("mid-only", "low", "high")}
        assert len(set(offsets.values())) == len(offsets), (
            f"co-launched lanes share offset: {offsets}")

    def test_unknown_lane_defaults_to_zero(self, dispatcher):
        assert dispatcher._lane_rotation_offset("unknown") == 0
        assert dispatcher._lane_rotation_offset("") == 0
        assert dispatcher._lane_rotation_offset("gated-reverify") == 0

    def test_offsets_within_bounds(self, dispatcher):
        n_types = len(dispatcher.INSTANCE_TYPES)
        n_subnets = len(dispatcher.SUBNETS)
        for lane, offset in dispatcher._LANE_ROTATION_INDEX.items():
            assert 0 <= offset < max(n_types, n_subnets), (
                f"{lane}: offset {offset} out of range "
                f"(types={n_types}, subnets={n_subnets})")


class TestRotationProducesDistinctPools:

    @staticmethod
    def _first_pool(mod, offset: int):
        """(instance_type, subnet) pair a lane would attempt first."""
        types = list(mod.INSTANCE_TYPES)
        subnets = list(mod.SUBNETS)
        if offset:
            o = offset % len(types) if types else 0
            if o:
                types = types[o:] + types[:o]
            o = offset % len(subnets) if subnets else 0
            if o:
                subnets = subnets[o:] + subnets[:o]
        return (types[0], subnets[0]) if types and subnets else None

    def test_concurrent_lanes_diverge(self, dispatcher):
        pools = {
            lane: self._first_pool(dispatcher,
                                    dispatcher._lane_rotation_offset(lane))
            for lane in ("mid-only", "low", "high")
        }
        pool_set = {tuple(p) for p in pools.values()}
        assert len(pool_set) == len(pools), (
            f"Concurrent lanes converged: {pools}")

    def test_sweep_diverges_from_groom_lanes(self, dispatcher):
        sweep_first = self._first_pool(
            dispatcher, dispatcher._lane_rotation_offset("sweep"))
        for lane in ("mid-only", "low", "high"):
            lane_first = self._first_pool(
                dispatcher, dispatcher._lane_rotation_offset(lane))
            assert sweep_first != lane_first, (
                f"Sweep and {lane} share first pool {sweep_first}")

    def test_zero_and_one_offsets_differ(self, dispatcher):
        pool_0 = self._first_pool(dispatcher, 0)
        pool_1 = self._first_pool(dispatcher, 1)
        assert pool_0 != pool_1, (
            f"Offset 0 and 1 produce same first pool {pool_0}")

    def test_all_default_types_are_arm64(self, dispatcher):
        """All default instance types are arm64/Graviton — they must match the
        AL2023 arm64 AMI."""
        arm64_families = {"t4g", "c7g", "c6g", "m7g", "m6g", "a1", "a2"}
        for t in dispatcher.INSTANCE_TYPES:
            family = t.split(".")[0]
            assert family in arm64_families, (
                f"{t} is not a recognised arm64 family — "
                f"will not boot with AMI {dispatcher.AMI_ID}")
