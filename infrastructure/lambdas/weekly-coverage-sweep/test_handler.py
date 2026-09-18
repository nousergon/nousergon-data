"""The coverage-sweep handler's OUTCOME ROUTING, against stubbed lib calls.

Named ``test_handler.py`` because that is the only filename either gate looks
for: ``.github/workflows/ci.yml`` globs ``infrastructure/lambdas/*/test_handler.py``
pre-merge, and ``_shared/run_handler_tests.sh`` returns 0 for a lambda that has
none.

**What is under test, and what deliberately is not.** The sweep itself —
coverage derivation, the cycle union, the marker merge — is
``nousergon_lib.pipeline_status``'s, tested there against captured live
executions. What lives HERE and nowhere else is the mapping from what the
sweep did to the three outcomes the state machine routes on, and that mapping
has exactly one property worth pinning: **``unavailable`` is never collapsed
into either of the others.** A sweep that could not run, and a sweep that ran
and found nothing, are different facts; only the second means the coverage
surface is observed. Every test below is that property from a different angle.

The lib is stubbed via ``sys.modules`` rather than installed: the tests must
run on a bare deploy runner that has not pulled the git-only dependency, and
stubs take precedence over anything installed (``run_handler_tests.sh``).
"""
from __future__ import annotations

import sys
import types

import pytest


def _install_stubs(
    *,
    sweep=None,
    read_raises: Exception | None = None,
    publish_raises: Exception | None = None,
    augment_raises: Exception | None = None,
    alerts_raises: Exception | None = None,
    calls: dict | None = None,
):
    """Stub boto3, krepis and nousergon_lib in sys.modules; return the module."""
    calls = calls if calls is not None else {}

    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *a, **k: object()
    sys.modules["boto3"] = boto3

    krepis = types.ModuleType("krepis")
    region_mod = types.ModuleType("krepis.aws_region")
    region_mod.resolve_region = lambda: "us-east-1"
    alerts_mod = types.ModuleType("krepis.alerts")

    def _publish(*a, **k):
        calls["alerted"] = calls.get("alerted", 0) + 1
        if alerts_raises:
            raise alerts_raises

    alerts_mod.publish = _publish
    krepis.alerts = alerts_mod
    krepis.aws_region = region_mod
    sys.modules["krepis"] = krepis
    sys.modules["krepis.alerts"] = alerts_mod
    sys.modules["krepis.aws_region"] = region_mod

    nl = types.ModuleType("nousergon_lib")
    ps = types.ModuleType("nousergon_lib.pipeline_status")
    cov = types.ModuleType("nousergon_lib.pipeline_status.coverage")
    cm = types.ModuleType("nousergon_lib.pipeline_status.completion_marker")

    def _read(**kwargs):
        # alpha-engine-config-I8809: recorded so a test can assert BOTH date
        # families reach the reader — the whole point of the migration window.
        calls["read_kwargs"] = kwargs
        if read_raises:
            raise read_raises
        return sweep

    def _publish_sweep(*a, **k):
        calls["published"] = calls.get("published", 0) + 1
        if publish_raises:
            raise publish_raises

    def _augment(*a, **k):
        calls["augmented"] = calls.get("augmented", 0) + 1
        calls["augment_kwargs"] = k
        if augment_raises:
            raise augment_raises

    cov.read_coverage_sweep = _read
    cov.publish_sweep = _publish_sweep
    cm.augment_marker = _augment
    sys.modules["nousergon_lib"] = nl
    sys.modules["nousergon_lib.pipeline_status"] = ps
    sys.modules["nousergon_lib.pipeline_status.coverage"] = cov
    sys.modules["nousergon_lib.pipeline_status.completion_marker"] = cm

    sys.modules.pop("index", None)
    import index

    return index, calls


class _Sweep:
    def __init__(
        self,
        *,
        should_alert: bool,
        cycle=object(),
        partitions_read=("2026-08-21", "2026-08-22"),
        legacy_partition_rows=0,
        coverage_established=True,
        deferred=False,
        deferral_reason=None,
    ):
        self.should_alert = should_alert
        self.cycle = cycle
        # alpha-engine-config-I8809: the sweep now reports which date
        # partitions it unioned. The handler threads both onto its result and
        # into augment_marker, so a stub without them makes every outcome
        # `unavailable` — which is exactly what the real handler does with a
        # nousergon-lib pin predating the field, and why the pin floor is
        # asserted in tests/test_weekly_partition_family_contract.py.
        self.partitions_read = partitions_read
        self.legacy_partition_rows = legacy_partition_rows
        # alpha-engine-config-I10170. The real CoverageSweep exposes these as
        # PROPERTIES, so a stub that omits them raises AttributeError rather
        # than reading as a benign default — which is what happened: every
        # test in this file failed on `'_Sweep' object has no attribute
        # 'deferred'` once `_outcome_for` started reading it.
        #
        # Defaulting them True/False/None here is deliberate and is the
        # HEALTHY case: coverage established, nothing deferred. A test that
        # wants the deferred path passes deferred=True with a reason, so the
        # two paths stay distinguishable — a stub hardcoding `deferred=False`
        # would make `_outcome_for`'s deferred branch unreachable and untested
        # while turning the suite green, which is the shape of defect this
        # whole issue is about.
        self.coverage_established = coverage_established
        self.deferred = deferred
        self.deferral_reason = deferral_reason

    def explain(self):
        return "sweep says so"


@pytest.fixture(autouse=True)
def _clean():
    yield
    for name in list(sys.modules):
        if name.startswith(("nousergon_lib", "krepis")) or name in ("boto3", "index"):
            sys.modules.pop(name, None)


def test_a_clean_sweep_is_clean_and_augments_the_marker():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert out["marker_augmented"] is True
    assert calls.get("alerted") is None, "a clean sweep must not page"


def test_a_finding_is_findings_and_pages_once():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=True))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS
    assert calls["alerted"] == 1


def test_a_sweep_that_cannot_run_is_unavailable_never_clean():
    """The whole point. A crash reading the cycle means the coverage surface is
    UNOBSERVED for this run — rendering that as clean is principles.md §2.7's
    'no data rendered green'."""
    index, _ = _install_stubs(read_raises=RuntimeError("AccessDenied"))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE
    assert "AccessDenied" in out["reason"]


def test_a_sweep_that_ran_but_could_not_publish_is_unavailable():
    """It ran, but nothing downstream can read what it found — including the
    marker, which keeps its bare envelope claim. Unobserved, not clean."""
    index, _ = _install_stubs(
        sweep=_Sweep(should_alert=False), publish_raises=OSError("no such bucket")
    )
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE
    assert "could not publish" in out["reason"]


def test_a_missing_run_date_is_unavailable_not_clean():
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({}, None)
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE


def test_an_unreadable_cycle_leaves_the_marker_alone_and_still_reports():
    """`--augment-marker` with no cycle: the marker keeps cycle_verdict unknown,
    which resolves to UNKNOWN downstream. The sweep itself still ran, so the
    outcome is its own finding — not unavailable."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False, cycle=None))
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert out["marker_augmented"] is False
    assert calls.get("augmented") is None


def test_a_failed_page_does_not_turn_a_finding_into_a_clean_result():
    index, _ = _install_stubs(
        sweep=_Sweep(should_alert=True), alerts_raises=RuntimeError("SNS down")
    )
    out = index.handler({"run_date": "2026-08-22"}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS


def test_dry_run_reports_the_real_outcome_not_a_hardcoded_clean():
    """A rehearsal that reports green whatever it saw certifies nothing.

    Measured 2026-08-22 on the FIRST live dry invocation: the sweep found 28
    absent verdicts and 1 finding, and this branch returned `outcome: clean`
    anyway. That is the same "no data rendered as healthy" defect
    (principles.md 2.7) the whole sweep exists to detect, shipped inside the
    detector. What dry_run withholds is the WRITES and the page — never the
    verdict.
    """
    index, calls = _install_stubs(sweep=_Sweep(should_alert=True))
    out = index.handler({"run_date": "2026-08-22", "dry_run": True}, None)
    assert out["outcome"] == index.OUTCOME_FINDINGS
    assert out["dry_run"] is True
    assert calls.get("published") is None, "a dry run must not write"
    assert calls.get("augmented") is None, "a dry run must not touch the marker"
    assert calls.get("alerted") is None, "a dry run must not page"


def test_dry_run_writes_nothing():
    """The Friday-PM preflight exercises the read path and every IAM grant it
    needs, and must not touch the marker or the artifact."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-22", "dry_run": True}, None)
    assert out["dry_run"] is True
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert calls.get("published") is None
    assert calls.get("augmented") is None


def test_the_handler_never_raises_on_any_stub_failure():
    """An observe-only tail downstream of the success terminal must not be able
    to fail a completed weekly run (sf-pipeline-policy §2.1)."""
    for kwargs in (
        {"read_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=False), "publish_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=False), "augment_raises": RuntimeError("boom")},
        {"sweep": _Sweep(should_alert=True), "alerts_raises": RuntimeError("boom")},
    ):
        index, _ = _install_stubs(**kwargs)
        out = index.handler({"run_date": "2026-08-22"}, None)
        assert out["outcome"] in {
            index.OUTCOME_CLEAN,
            index.OUTCOME_FINDINGS,
            index.OUTCOME_UNAVAILABLE,
        }


# ── alpha-engine-config-I8809 ────────────────────────────────────────────────


def test_the_legacy_partition_is_threaded_into_the_reader():
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    index.handler(
        {"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None
    )
    assert calls["read_kwargs"]["run_date"] == "2026-08-28"
    assert calls["read_kwargs"]["calendar_date"] == "2026-08-29"


def test_no_calendar_date_is_a_single_partition_sweep_not_an_error():
    """The post-cutover shape, and any caller that predates the field."""
    index, calls = _install_stubs(sweep=_Sweep(should_alert=False))
    out = index.handler({"run_date": "2026-08-28"}, None)
    assert out["outcome"] == index.OUTCOME_CLEAN
    assert calls["read_kwargs"]["calendar_date"] is None


def test_the_result_says_which_partitions_it_unioned():
    index, _ = _install_stubs(
        sweep=_Sweep(
            should_alert=False,
            partitions_read=("2026-08-28", "2026-08-29"),
            legacy_partition_rows=28,
        )
    )
    out = index.handler(
        {"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None
    )
    assert out["partitions_read"] == ["2026-08-28", "2026-08-29"]
    assert out["legacy_partition_rows"] == 28


def test_the_marker_is_augmented_in_every_partition_that_was_read():
    index, calls = _install_stubs(
        sweep=_Sweep(should_alert=False, partitions_read=("2026-08-28", "2026-08-29"))
    )
    index.handler({"run_date": "2026-08-28", "calendar_date": "2026-08-29"}, None)
    assert calls["augment_kwargs"]["also_dates"] == ("2026-08-28", "2026-08-29")


def test_deferred_outranks_findings():
    """`_outcome_for`'s deferred branch, which had no test at all.

    `alpha-engine-config-I10170`: when the cycle cannot support an absence
    claim, the honest headline is that coverage is not established — not that
    N stages are absent, which is the assertion the sweep just DECLINED to
    make. Reporting `findings` there would publish a number the sweep does not
    stand behind.

    This existed only as a docstring until 2026-09-08. Every test in this file
    had been failing on `'_Sweep' object has no attribute 'deferred'` because
    the stub predated the attribute, and the repair that added it could have
    hardcoded `deferred=False` — turning the suite green while leaving this
    branch unreachable. That is the same "green over an unexercised path"
    shape the sweep itself exists to catch, so the branch is asserted here.
    """
    index, _ = _install_stubs(
        sweep=_Sweep(
            should_alert=True,
            deferred=True,
            deferral_reason="cycle in flight — absence not established",
        )
    )
    out = index.handler({"run_date": "2026-08-22"}, None)

    assert out["outcome"] == index.OUTCOME_DEFERRED, (
        "a deferred sweep reported its findings count as the headline — the "
        "sweep declined to make that absence claim"
    )
    assert out["outcome"] != index.OUTCOME_FINDINGS


def test_a_clean_sweep_that_is_not_deferred_is_still_clean():
    """The other side of the same branch: `deferred` must not swallow a pass.

    Guards against a fix that makes the assertion above true by reporting
    `deferred` unconditionally.
    """
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False))
    assert index.handler({"run_date": "2026-08-22"}, None)["outcome"] == index.OUTCOME_CLEAN

    index2, _ = _install_stubs(sweep=_Sweep(should_alert=True))
    assert index2.handler({"run_date": "2026-08-22"}, None)["outcome"] == index2.OUTCOME_FINDINGS


# ---------------------------------------------------------------------------
# observer_did_work — the vacuous-run discriminator (alpha-engine-config-I9693)
# ---------------------------------------------------------------------------
#
# The cycle verdict answers "did the WEEK's work happen". It cannot answer "did
# THIS execution do any of it", and the two diverge on exactly the run that
# matters. Measured live 2026-09-18: `watch-rerun-2026-09-11-1` entered ZERO of
# the 16 declared spine stages, reported SUCCEEDED, and sat on a cycle whose
# earlier (FAILED) execution had entered 15 of them. Five consecutive Saturdays
# were "recovered" that way.
#
# The handler SURFACES a fact the sweep already established; these tests pin
# that it never invents one.


class _Exec:
    def __init__(self, arn, stages_entered=(), is_observer=False, role=None):
        self.execution_arn = arn
        self.stages_entered = tuple(stages_entered)
        self.is_observer = is_observer
        self.pipeline_role = role


class _Cycle:
    def __init__(self, executions=(), spine=("A", "B", "C")):
        self.executions = tuple(executions)
        self.stage_spine = tuple(spine)


def test_an_execution_that_entered_no_spine_stage_reports_did_work_false():
    cycle = _Cycle(
        executions=[
            _Exec("arn:real", stages_entered=("A", "B"), role="weekly"),
            _Exec("arn:me", stages_entered=(), is_observer=True, role="watch-rerun"),
        ]
    )
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=cycle))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["observer_did_work"] is False
    assert out["observer"]["stages_entered_count"] == 0
    assert "entered NONE" in out["observer"]["reason"]
    assert out["outcome"] == index.OUTCOME_CLEAN, (
        "the vacuity verdict is a SEPARATE axis from the sweep's own outcome — "
        "conflating them would make a clean sweep of a vacuous run unreadable"
    )


def test_an_execution_that_entered_spine_stages_reports_did_work_true():
    cycle = _Cycle(
        executions=[_Exec("arn:me", stages_entered=("A", "B", "C"), is_observer=True)]
    )
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=cycle))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["observer_did_work"] is True
    assert out["observer"]["stages_entered_count"] == 3


def test_an_unreadable_cycle_reports_unknown_never_false():
    """``None``, not ``False``. The SF Choice fires only on an explicit boolean
    false, so an unknown must not be expressible as one — a verdict
    manufactured from a failed read is the defect this issue is about, wearing
    the opposite sign."""
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=None))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["observer_did_work"] is None
    assert "unestablished" in out["observer"]["reason"]


def test_an_observer_missing_from_the_cycle_reports_unknown_never_false():
    cycle = _Cycle(executions=[_Exec("arn:someone-else", stages_entered=("A",))])
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=cycle))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["observer_did_work"] is None


def test_the_observer_is_found_by_arn_when_the_flag_was_not_set():
    """``is_observer`` is set by the lib when the observer was NAMED. A pin
    predating that, or a call made without it, must still resolve the row
    rather than silently degrade to unknown."""
    cycle = _Cycle(executions=[_Exec("arn:me", stages_entered=())])
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=cycle))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["observer_did_work"] is False


def test_the_dry_run_reports_the_real_verdict_not_a_hardcoded_one():
    """Same contract the outcome already honours: what ``dry_run`` withholds is
    the WRITES and the page, never the verdict."""
    cycle = _Cycle(executions=[_Exec("arn:me", stages_entered=(), is_observer=True)])
    index, _ = _install_stubs(sweep=_Sweep(should_alert=False, cycle=cycle))
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me", "dry_run": True},
        None,
    )
    assert out["dry_run"] is True
    assert out["observer_did_work"] is False


def test_a_sweep_that_could_not_publish_still_reports_the_observer_verdict():
    """The write failed; the derivation did not. Withholding the verdict here
    would lose the one fact the SF terminal depends on."""
    cycle = _Cycle(executions=[_Exec("arn:me", stages_entered=(), is_observer=True)])
    index, _ = _install_stubs(
        sweep=_Sweep(should_alert=False, cycle=cycle),
        publish_raises=RuntimeError("AccessDenied"),
    )
    out = index.handler(
        {"run_date": "2026-09-11", "observer_execution_arn": "arn:me"}, None
    )
    assert out["outcome"] == index.OUTCOME_UNAVAILABLE
    assert out["observer_did_work"] is False
