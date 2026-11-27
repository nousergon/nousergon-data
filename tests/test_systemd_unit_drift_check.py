"""Tests for infrastructure/systemd/check-systemd-unit-drift.py (config#2352).

Covers the installed-vs-repo systemd unit drift probe: clean match,
divergence detection, not-installed (box hosts neither pair) as non-error,
and missing repo source as a config error. No real AWS/systemd access —
purely local file comparison, so this is a plain tmp-dir fixture test
(mirrors the module-load pattern used by test_sf_definition_check_drift.py).
The one exception is `_boot_pull_diagnosis` (alpha-engine-config-I9444),
which does shell out to `systemctl` — those tests inject a fake `run`
callable rather than touching a real subprocess.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT_PATH = _REPO_ROOT / "infrastructure" / "systemd" / "check-systemd-unit-drift.py"


class _FakeFcr:
    """Stand-in for ``nousergon_lib.fleet_check_result`` — no real S3 access.

    Mirrors the pattern in ``tests/test_pause_reconcile.py``. Every module test
    that reaches ``main()`` now also reaches ``_publish_console`` (alpha-engine-
    config-I7857), and without this the un-mocked ``fcr.emit`` would construct a
    real boto3 S3 client and attempt a live PutObject whenever local AWS creds
    happen to be present — exactly the "tests leaked real fan-out" class this
    repo's conftest.py already guards against for flow-doctor/SSM.
    """

    STATUS_OK = "ok"
    STATUS_ATTENTION = "attention"
    STATUS_ERROR = "error"
    calls: list[dict] = []

    @classmethod
    def build(cls, **kw):
        return kw

    @classmethod
    def emit(cls, env, dry_run=False):
        cls.calls.append(env)
        return "s3://fake/latest.json"

    @classmethod
    def emit_result(cls, **kw):
        env = cls.build(**kw)
        return cls.emit(env)


@pytest.fixture()
def cd(tmp_path, monkeypatch):
    """Load the module fresh per-test, pointed at an isolated repo+installed dir pair."""
    spec = importlib.util.spec_from_file_location("check_systemd_unit_drift", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    script_dir = tmp_path / "infrastructure" / "systemd"
    script_dir.mkdir(parents=True)
    installed_dir = tmp_path / "etc-systemd-system"
    installed_dir.mkdir()

    monkeypatch.setattr(module, "SCRIPT_DIR", script_dir)
    monkeypatch.setattr(module, "INSTALLED_DIR", installed_dir)

    _FakeFcr.calls = []
    monkeypatch.setitem(sys.modules, "nousergon_lib", type(sys)("nousergon_lib"))
    monkeypatch.setattr(sys.modules["nousergon_lib"], "fleet_check_result", _FakeFcr, raising=False)

    return module, script_dir, installed_dir


def _write(path: Path, content: str) -> None:
    path.write_text(content)


def test_clean_when_installed_matches_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.timer", "UNIT A\n")
    _write(installed_dir / "daily-news.timer", "UNIT A\n")

    status, detail = module.check_unit("daily-news.timer")

    assert status == "clean"
    assert "OK" in detail


def test_drift_when_installed_diverges_from_repo(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.service", "UNIT NEW\n")
    _write(installed_dir / "metron-intraday.service", "UNIT OLD (stale)\n")

    status, detail = module.check_unit("metron-intraday.service")

    assert status == "drift"
    assert "metron-intraday.service" in detail


def test_not_installed_when_box_never_had_the_unit(cd):
    module, script_dir, installed_dir = cd
    _write(script_dir / "metron-intraday.timer", "UNIT A\n")
    # No file under installed_dir — this box never installed it (e.g. the
    # dashboard box probing for metron-intraday, which only the trading box
    # hosts).

    status, detail = module.check_unit("metron-intraday.timer")

    assert status == "not-installed"


def test_installed_with_no_repo_copy_is_uncodified_not_a_source_error(cd):
    """Reclassified 2026-08-08 (alpha-engine-config-I6656).

    This used to assert `source-error` — i.e. that a unit installed with no
    repo copy meant THIS SCRIPT was misconfigured. On the dashboard box 54 of
    60 units are in that state; they are not 54 configuration errors, they are
    the coverage gap, and naming them an error made the honest reading
    unavailable.
    """
    module, script_dir, installed_dir = cd
    _write(installed_dir / "ghost.service", "UNIT GHOST\n")

    status, detail = module.check_unit("ghost.service")

    assert status == "uncodified"
    assert "codified in no known root" in detail


def test_all_not_installed_when_box_hosts_neither_pair(cd):
    module, script_dir, installed_dir = cd
    for name in module.ALL_UNITS:
        _write(script_dir / name, f"UNIT {name}\n")
    # installed_dir stays empty — this box hosts none of the tracked units.

    statuses = [module.check_unit(name)[0] for name in module.ALL_UNITS]

    assert all(s == "not-installed" for s in statuses)


def test_main_reports_drift_exit_code_via_cli(cd, monkeypatch, capsys):
    module, script_dir, installed_dir = cd
    _write(script_dir / "daily-news.service", "UNIT NEW\n")
    _write(installed_dir / "daily-news.service", "UNIT OLD\n")
    for name in module.ALL_UNITS:
        if name != "daily-news.service":
            _write(script_dir / name, f"UNIT {name}\n")

    monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
    exit_code = module.main()

    out = capsys.readouterr().out
    assert exit_code == 1
    assert "drift" in out.lower()


class TestDriftReportingPath:
    """The FAILURE path must actually report (alpha-engine-config-I4509).

    These are the tests that were missing. `_report_drift` (formerly
    `_flow_doctor_report`) was broken for an unknown length of time and nobody
    noticed, because it only runs when drift is FOUND and the daily check
    normally passes. Two independent faults were live simultaneously:
    `flow_doctor.init()` does not exist in flow-doctor 0.8.7, and the env
    hydration for flow-doctor.yaml was incomplete regardless.

    A reporting path exercised only on failure needs a test that exercises
    failure. That is the whole lesson.
    """

    def test_report_publishes_via_krepis(self, cd, monkeypatch):
        module, _, _ = cd
        captured = {}

        def fake_publish(**kwargs):
            captured.update(kwargs)

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(fake_publish)})
        )
        module._report_drift(["daily-news.service: DIVERGED"])

        assert captured, "drift must publish an alert, not just print"
        assert captured["severity"] == "error"
        assert captured["source"] == (
            "alpha-engine-data/infrastructure/systemd/check-systemd-unit-drift.py"
        )
        assert "daily-news.service" in captured["message"], (
            "the alert must name the drifting unit — an alert that says "
            "'drift detected' sends you to the box to find out what"
        )

    def test_report_dedup_window_widened_past_the_timer_cadence(self, cd, monkeypatch):
        """alpha-engine-config-I7857: the old 1440min (24h) window matched the
        daily timer exactly, so an UNCHANGED drift re-paged the channel every
        single day with no new information. Must now exceed 1440 by a wide
        margin so the console (not a daily repeat) carries the standing state."""
        module, _, _ = cd
        captured = {}

        def fake_publish(**kwargs):
            captured.update(kwargs)

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(fake_publish)})
        )
        module._report_drift(["a.service: DIVERGED"])

        assert captured["dedup_window_min"] > 1440

    def test_report_dedups_on_findings_not_message(self, cd, monkeypatch):
        # Same drift persisting should alert once a day; DIFFERENT drift must
        # produce a different key so it pages immediately.
        module, _, _ = cd
        keys = []

        def fake_publish(**kwargs):
            keys.append(kwargs["dedup_key"])

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(fake_publish)})
        )
        module._report_drift(["a.service: DIVERGED"])
        module._report_drift(["a.service: DIVERGED"])
        module._report_drift(["b.service: DIVERGED"])

        assert keys[0] == keys[1], "identical findings must share a dedup key"
        assert keys[2] != keys[0], "different findings must not be deduped together"

    def test_publish_failure_is_loud_not_swallowed(self, cd, monkeypatch, capsys):
        # The exact defect this rewrite fixes: drift was detected and the
        # telling threw, silently.
        module, _, _ = cd

        def boom(**kwargs):
            raise RuntimeError("simulated transport failure")

        monkeypatch.setitem(
            __import__("sys").modules, "krepis.alerts",
            type("M", (), {"publish": staticmethod(boom)})
        )
        module._report_drift(["a.service: DIVERGED"])

        err = capsys.readouterr().err
        assert "UNREPORTED" in err, (
            "a failure to publish must be loud — silent failure here is "
            "indistinguishable from no drift"
        )

    def test_flow_doctor_init_is_not_resurrected(self):
        """`flow_doctor.init` has not existed for some time -- guard the CALL.

        Parsed with `ast` rather than grepped, deliberately. A source-text
        guard trips on the module's own docstring explaining why this API is
        gone, which pushes the next person to delete the explanation rather
        than keep the guard. Matching a real Call node means prose is free to
        name the dead API as often as it is useful to.
        """
        import ast

        tree = ast.parse(_SCRIPT_PATH.read_text())
        bad = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "init"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "flow_doctor"
        ]
        assert not bad, (
            "flow_doctor.init() does not exist in flow-doctor 0.8.7 -- it "
            "exports FlowDoctor/FlowDoctorBuilder. Use krepis.alerts (the "
            "canonical CLI, config#1649)."
        )


class TestConsolePublish:
    """`_publish_console` (alpha-engine-config-I7857) — the standing-drift
    finding set routed to the console fleet-check surface, published on every
    run regardless of findings, distinct from the channel alert."""

    def test_publishes_on_clean_run_too(self, cd):
        """A surface that only publishes when something is wrong is
        indistinguishable from one that has died — must publish on clean runs."""
        module, _, _ = cd
        module._publish_console([], installed_count=4, drift_count=0)

        assert len(_FakeFcr.calls) == 1
        assert _FakeFcr.calls[0]["status"] == "ok"

    def test_findings_render_as_attention_not_error(self, cd):
        """A check that ran and found drift is working as designed — `error`
        is reserved for a check that could not run at all (mirrors
        pause_reconcile.py's publish_error contract in this same repo)."""
        module, _, _ = cd
        module._publish_console(["a.service: DIVERGED"], installed_count=4, drift_count=1)

        assert _FakeFcr.calls[0]["status"] == "attention"
        assert _FakeFcr.calls[0]["status"] != "error"

    def test_findings_are_carried_in_the_envelope(self, cd):
        module, _, _ = cd
        module._publish_console(
            ["a.service: DIVERGED", "b.timer: DIVERGED"], installed_count=4, drift_count=2,
        )

        findings = _FakeFcr.calls[0]["findings"]
        assert len(findings) == 2
        assert any("a.service" in f["detail"] for f in findings)
        assert any("b.timer" in f["detail"] for f in findings)

    def test_main_publishes_console_on_every_run_including_clean(self, cd, monkeypatch):
        """The end-to-end wiring: `main()` must reach `_publish_console`
        whether or not `--report`/drift is present, mirroring box_health's
        'publish on every run including clean ones' property."""
        module, script_dir, installed_dir = cd
        for name in module.ALL_UNITS:
            content = f"UNIT {name}\n"
            _write(script_dir / name, content)
            _write(installed_dir / name, content)

        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        exit_code = module.main()

        assert exit_code == 0
        assert len(_FakeFcr.calls) == 1
        assert _FakeFcr.calls[0]["status"] == "ok"

    def test_console_publish_failure_does_not_break_the_check(self, cd, capsys):
        """Telemetry must never fail the check itself — best-effort, logged."""
        module, _, _ = cd

        class _Boom:
            STATUS_OK = "ok"
            STATUS_ATTENTION = "attention"

            @staticmethod
            def emit_result(**kw):
                raise RuntimeError("simulated S3 failure")

        import sys as _sys
        _sys.modules["nousergon_lib"].fleet_check_result = _Boom

        module._publish_console(["a.service: DIVERGED"], installed_count=4, drift_count=1)

        err = capsys.readouterr().err
        assert "console publish failed" in err


class TestCoverageAccounting:
    """alpha-engine-config-I6656 — the defect was not a wrong diff. The diff
    was right. It was that four units were compared and the closing line said
    "installed units match repo", on a box with 60 installed unit files. So
    these assert what the script SAYS as much as what it compares.
    """

    def test_an_uncodified_unit_is_never_reported_as_a_pass(self, cd, capsys, monkeypatch):
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "X\n")
        _write(installed_dir / "hand-installed.timer", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        module.main()
        out = capsys.readouterr().out

        assert "PASSED" not in out
        assert "INCOMPLETE" in out
        assert "1/2 installed units are codified and clean" in out

    def test_full_coverage_is_the_only_thing_called_a_pass(self, cd, capsys, monkeypatch):
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "X\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "coverage PASSED" in out
        assert "uncodified=0" in out

    def test_a_baselined_uncodified_unit_does_not_fail_the_run(self, cd, capsys, monkeypatch):
        """54 of them exist. Failing on the backlog pages once per unit on the
        first run and teaches the reader to disable the check."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "uncodified_new=0" in out
        assert "uncodified-NEW" not in out

    def test_an_unbaselined_uncodified_unit_is_named_individually(self, cd, capsys, monkeypatch):
        """New is the only kind a given run can act on, so it must not blend
        into the count."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        _write(installed_dir / "surprise.timer", "Z\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        module.main()
        cap = capsys.readouterr()

        assert "[uncodified-NEW] surprise.timer" in cap.out
        assert "[uncodified] known.service" in cap.out
        assert "uncodified=2 uncodified_new=1" in cap.out
        assert "absent from the baseline" in cap.err

    def test_strict_fails_on_any_uncodified_unit(self, cd, capsys, monkeypatch):
        """The end state, once the baseline is empty."""
        module, script_dir, installed_dir = cd
        _write(installed_dir / "known.service", "Y\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: {"known.service"})
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py", "--strict"])

        assert module.main() == 1

    def test_symlinked_units_are_not_counted(self, cd, capsys, monkeypatch):
        """`/etc/systemd/system` carries systemd's own aliases into
        /usr/lib/systemd as symlinks. Counting those puts `dbus.service` in
        the work queue."""
        module, script_dir, installed_dir = cd
        _write(script_dir / "real.service", "X\n")
        _write(installed_dir / "real.service", "X\n")
        (installed_dir / "alias.service").symlink_to(installed_dir / "real.service")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py"])

        code = module.main()
        out = capsys.readouterr().out

        assert code == 0
        assert "installed=1" in out
        assert "alias.service" not in out

    def test_metric_failure_is_loud_and_does_not_mask_the_check(self, cd, capsys, monkeypatch):
        """The counts are the surface that says whether coverage is real, so
        their absence has to be attributable rather than silent — and a broken
        metric must not take the drift check itself down with it.

        The failure is injected at the real boundary (the boto3 call), not by
        replacing `_emit_metrics`: stubbing the function under test would
        assert the behaviour of the stub.
        """
        module, script_dir, installed_dir = cd
        _write(script_dir / "covered.service", "X\n")
        _write(installed_dir / "covered.service", "EDITED\n")
        monkeypatch.setattr(module, "load_baseline", lambda *a, **k: set())
        monkeypatch.setattr(sys, "argv", ["check-systemd-unit-drift.py", "--metric"])

        import boto3

        monkeypatch.setattr(
            boto3, "client",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no credentials")),
        )

        code = module.main()
        cap = capsys.readouterr()

        # The drift is still detected and still reported.
        assert code == 1
        assert "drift=1" in cap.out
        # And the metric failure is named, not swallowed.
        assert "METRIC EMIT FAILED" in cap.err
        assert "UNOBSERVED" in cap.err

    def test_the_shipped_baseline_parses_and_holds_only_unit_names(self, cd):
        """The baseline is data the check depends on; a typo in it silently
        un-covers a unit."""
        module, _, _ = cd
        baseline = module.load_baseline(_REPO_ROOT / "infrastructure" / "systemd" / "uncodified-units-baseline.txt")

        assert baseline, "the shipped baseline should not be empty — 22 units are uncodified"
        for name in baseline:
            assert name.endswith((".service", ".timer")), name
            assert " " not in name, name

    def test_the_shipped_baseline_covers_the_measured_dashboard_box(self, cd):
        """Re-derived 2026-08-09 from i-09b539c844515d549: every installed unit
        was hash-compared against every repo checkout on the box; 32 matched a
        root byte-for-byte and left the baseline, 22 matched nothing. The three
        mnemon units moved to the covered list 2026-08-09: token relocated to an
        EnvironmentFile, units codified in nous-ergon-ops (ops-PR541). Spot-checks
        both directions, so an edit that drops a genuinely-uncodified unit — or
        re-adds a root-covered one — is caught."""
        module, _, _ = cd
        baseline = module.load_baseline(_REPO_ROOT / "infrastructure" / "systemd" / "uncodified-units-baseline.txt")

        for name in (
            "amazon-cloudwatch-agent.service",
        ):
            assert name in baseline, f"{name} missing from the uncodified baseline"

        for name in (
            "morning-signal.service",      # alpha-engine-dashboard root
            "box-health.timer",            # alpha-engine-dashboard root
            "metron-refresh.service",      # metron-ops root
            "telos-web.service",           # telos-ops root
            "dashboard.service",           # alpha-engine-dashboard top-level root
            "nous-ergon-live.service",     # alpha-engine-dashboard live/ root
            "nousergon-console.service",   # nousergon-console root
            "signal.service",              # the-cyphering-ops root
            "vires.service",               # vires root
            "morning-signal-pull.service", # codified by crucible-dashboard-PR635
            "litellm-proxy.service",       # nous-ergon-ops live-infrastructure root
            "llm-egress-proxy.service",    # nous-ergon-ops (ops-PR533)
            "ibgateway.service",           # nous-ergon-ops (ops-PR534)
            "certbot-renew.timer",         # nous-ergon-ops
            "ops-config-drift.service",    # nous-ergon-ops (ops-PR535) — never baselined
            "mnemon.service",              # nous-ergon-ops (ops-PR541) — token relocated to /etc/mnemon/env first (config-I6712)
            "mnemon-sync.service",         # nous-ergon-ops (ops-PR541)
            "mnemon-sync.timer",           # nous-ergon-ops (ops-PR541)
        ):
            assert name not in baseline, (
                f"{name} is covered by a --codified-root and must not be baselined "
                f"— a baselined unit is exempt from drift verification"
            )

    def test_the_unit_carries_no_hardcoded_root_list(self):
        """The root list is NOT in the unit any more (alpha-engine-config-I6960).

        It used to be, and this file used to assert its eleven entries were
        present — which is why the defect survived: the list was the dashboard
        box's checkouts, the SAME unit file installs on the trading box, and a
        test pinning the list could only ever confirm the wrong list was
        faithfully deployed to both. A per-box fact cannot live in a file that
        is byte-identical on every box; roots are discovered now.
        """
        unit = (_REPO_ROOT / "infrastructure" / "systemd" / "systemd-unit-drift-check.service").read_text()
        exec_lines = [l for l in unit.splitlines() if l.startswith("ExecStart=")]
        assert len(exec_lines) == 1
        line = exec_lines[0]
        assert "--metric" in line, "coverage counts must be emitted every run"
        assert "--report" in line, "a finding must reach an alert, not only the journal"
        assert "--codified-root" not in line, (
            "a hardcoded root list in a unit file shared by every box is I6960; "
            "roots are discovered per-box"
        )
        assert "--no-discover-roots" not in line, (
            "disabling discovery in the shipped unit restores the defect"
        )
        # boto3 does not infer region from IMDS: without these, --metric dies
        # with "You must specify a region" (seen live 2026-08-09).
        assert "Environment=AWS_REGION=us-east-1" in unit
        assert "Environment=AWS_DEFAULT_REGION=us-east-1" in unit

    def test_metric_namespace_is_one_the_box_role_may_put_to(self, cd):
        """The box role's PutMetricData grant is namespace-conditioned to
        AlphaEngine/AlphaEngine/* (alpha-engine-cloudwatch-metrics.json in
        nous-ergon-ops). The original "NousErgon/BoxHealth" value would have
        been denied on every emit — and it also wasn't box_health.sh's actual
        namespace, despite the comment saying it shared it."""
        module, _, _ = cd
        assert module.METRIC_NAMESPACE == "AlphaEngine/Box"


class TestUnreadableUnit:
    """A unit file the check cannot read (2026-08-09: nousergon-console.service
    was installed root-owned 0600) must be a classified, failing finding — the
    PermissionError previously escaped `_sha256`'s except clause and killed the
    whole sweep, taking coverage of the other 59 units with it."""

    @pytest.fixture(autouse=True)
    def _not_root(self):
        import os
        if os.geteuid() == 0:
            pytest.skip("file modes cannot deny root; unreadable is untestable as uid 0")

    def test_unreadable_installed_unit_is_classified_not_a_crash(self, cd):
        module, script_dir, installed_dir = cd
        path = installed_dir / "nousergon-console.service"
        path.write_text("UNIT SECRETIVE\n")
        path.chmod(0o000)

        status, detail = module.check_unit("nousergon-console.service")

        assert status == "unreadable"
        assert "0644" in detail

    def test_main_survives_the_unreadable_unit_and_fails_loud(self, cd, monkeypatch, capsys):
        """The other units must still be swept — the crash mode reported on
        NONE of them."""
        module, script_dir, installed_dir = cd
        blocked = installed_dir / "nousergon-console.service"
        blocked.write_text("UNIT SECRETIVE\n")
        blocked.chmod(0o000)
        (script_dir / "daily-news.timer").write_text("UNIT A\n")
        (installed_dir / "daily-news.timer").write_text("UNIT A\n")

        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        code = module.main()
        cap = capsys.readouterr()

        assert code == 1
        assert "unreadable=1" in cap.out
        assert "[clean] daily-news.timer" in cap.out
        assert "nousergon-console.service" in cap.err


class TestCodifiedRootDiscovery:
    """Roots are found by walking the box, not read from a list
    (alpha-engine-config-I6960).

    The regression this closes: on the trading box, six units WERE codified in
    `/home/ec2-user/alpha-engine/infrastructure/systemd` and the check reported
    them as "installed but codified nowhere" every day, because that directory
    was not among the eleven the shared unit file named. A check that cannot
    see where a unit is codified reports a codification gap — the harness fault
    arriving dressed as the finding.
    """

    def _box(self, tmp_path):
        """A miniature of the real layout: repos under a home, units in both
        `infrastructure/systemd/` and bare `infrastructure/`."""
        home = tmp_path / "home" / "ec2-user"
        (home / "alpha-engine" / "infrastructure" / "systemd").mkdir(parents=True)
        (home / "alpha-engine-dashboard" / "infrastructure").mkdir(parents=True)
        (home / "nous-ergon-ops" / "alpha-engine-dashboard" / "live" / "infrastructure" / "systemd").mkdir(parents=True)
        return home

    def test_discovers_both_layouts_at_every_real_depth(self, cd, tmp_path):
        module, _, _ = cd
        home = self._box(tmp_path)

        roots = module.discover_codified_roots(bases=[home])

        assert home / "alpha-engine" / "infrastructure" / "systemd" in roots, (
            "the trading box's codified root — the one I6960 was missing"
        )
        assert home / "alpha-engine" / "infrastructure" in roots
        assert home / "alpha-engine-dashboard" / "infrastructure" in roots
        assert home / "nous-ergon-ops" / "alpha-engine-dashboard" / "live" / "infrastructure" / "systemd" in roots, (
            "depth-4 root: the deepest real one on either box"
        )

    def test_a_new_checkout_is_covered_with_no_edit_anywhere(self, cd, tmp_path):
        """The whole point. A repo that lands on a box is compared the same day,
        rather than on the next time somebody remembers to extend a list."""
        module, _, _ = cd
        home = self._box(tmp_path)
        newcomer = home / "some-new-repo" / "infrastructure" / "systemd"
        newcomer.mkdir(parents=True)

        assert newcomer in module.discover_codified_roots(bases=[home])

    def test_a_unit_codified_in_a_discovered_root_reads_clean_not_uncodified(
        self, cd, tmp_path, monkeypatch, capsys
    ):
        """End to end, in the exact shape of the live finding."""
        module, script_dir, installed_dir = cd
        home = self._box(tmp_path)
        codified = home / "alpha-engine" / "infrastructure" / "systemd" / "ibgateway.service"
        codified.write_text("UNIT IBGW\n")
        (installed_dir / "ibgateway.service").write_text("UNIT IBGW\n")

        monkeypatch.setattr(module, "CODIFIED_SEARCH_BASES", (home,))
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        code = module.main()
        cap = capsys.readouterr()

        assert code == 0
        assert "[clean] ibgateway.service" in cap.out
        assert "uncodified=0" in cap.out

    def test_prune_keeps_the_walk_off_git_and_venvs(self, cd, tmp_path):
        module, _, _ = cd
        home = tmp_path / "home"
        buried = home / "repo" / ".git" / "modules" / "infrastructure"
        buried.mkdir(parents=True)
        venv = home / "repo" / ".venv" / "infrastructure"
        venv.mkdir(parents=True)

        roots = module.discover_codified_roots(bases=[home])

        assert buried not in roots
        assert venv not in roots

    def test_demo_units_in_nested_systemd_root_are_clean(self, cd, tmp_path, monkeypatch, capsys):
        """The metron demo layout is codified despite its infrastructure nesting."""
        module, _, installed_dir = cd
        home = tmp_path / "home"
        root = home / "metron-ops" / "infrastructure" / "demo" / "systemd"
        root.mkdir(parents=True)
        for name in ("metron-demo-api.service", "metron-demo-web.service"):
            (root / name).write_text(f"UNIT {name}\n")
            (installed_dir / name).write_text(f"UNIT {name}\n")

        monkeypatch.setattr(module, "CODIFIED_SEARCH_BASES", (home,))
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        code = module.main()
        cap = capsys.readouterr()

        assert code == 0
        assert "[clean] metron-demo-api.service" in cap.out
        assert "[clean] metron-demo-web.service" in cap.out
        assert "uncodified=0" in cap.out

    def test_nested_walk_prunes_and_survives_unreadable_directories(
        self, cd, tmp_path, monkeypatch, capsys
    ):
        module, _, _ = cd
        infrastructure = tmp_path / "repo" / "infrastructure"
        readable = infrastructure / "demo" / "systemd"
        unreadable = infrastructure / "blocked"
        pruned = infrastructure / ".venv" / "systemd"
        readable.mkdir(parents=True)
        unreadable.mkdir()
        pruned.mkdir(parents=True)
        original_iterdir = Path.iterdir

        def fake_iterdir(path):
            if path == unreadable:
                raise PermissionError("blocked for test")
            return original_iterdir(path)

        monkeypatch.setattr(Path, "iterdir", fake_iterdir)
        roots = module.discover_codified_roots(bases=[tmp_path])

        assert infrastructure in roots
        assert readable in roots
        assert pruned not in roots
        assert f"could not list {unreadable}" in capsys.readouterr().err

    def test_explicit_roots_are_additive_never_a_replacement(self, cd, tmp_path, monkeypatch, capsys):
        """`--codified-root` narrowing coverage back to a hand-written list is
        how I6960 would return through the flag instead of the unit file."""
        module, script_dir, installed_dir = cd
        home = self._box(tmp_path)
        (home / "alpha-engine" / "infrastructure" / "systemd" / "xvfb.service").write_text("X\n")
        (installed_dir / "xvfb.service").write_text("X\n")
        extra = tmp_path / "outside-the-bases"
        extra.mkdir()
        (extra / "daily-news.timer").write_text("D\n")
        (installed_dir / "daily-news.timer").write_text("D\n")

        monkeypatch.setattr(module, "CODIFIED_SEARCH_BASES", (home,))
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py", "--codified-root", str(extra)])
        code = module.main()
        cap = capsys.readouterr()

        assert code == 0
        assert "[clean] xvfb.service" in cap.out, "discovered root dropped when a flag was passed"
        assert "[clean] daily-news.timer" in cap.out, "explicit root ignored"

    def test_disagreeing_copies_are_reported_and_the_installed_one_wins(
        self, cd, tmp_path, monkeypatch, capsys
    ):
        """Two checkouts can codify the same unit name differently once roots
        are wide. Preferring the copy that matches the box keeps the verdict
        from depending on iteration order; the disagreement is still said out
        loud, because one of the two repos is stale."""
        module, script_dir, installed_dir = cd
        home = tmp_path / "home"
        a = home / "repo-a" / "infrastructure" / "systemd"
        b = home / "repo-b" / "infrastructure" / "systemd"
        a.mkdir(parents=True)
        b.mkdir(parents=True)
        (a / "ibgateway.service").write_text("STALE\n")
        (b / "ibgateway.service").write_text("LIVE\n")
        (installed_dir / "ibgateway.service").write_text("LIVE\n")

        monkeypatch.setattr(module, "CODIFIED_SEARCH_BASES", (home,))
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        code = module.main()
        cap = capsys.readouterr()

        assert code == 0, "a box running the correctly-codified unit is not drifted"
        assert "[clean] ibgateway.service" in cap.out
        assert "disagreeing copies" in cap.out
        assert "ambiguous=1" in cap.out


class _FakeCompleted:
    def __init__(self, returncode: int, stdout: str):
        self.returncode = returncode
        self.stdout = stdout


class TestBootPullDiagnosis:
    """`_boot_pull_diagnosis` (alpha-engine-config-I9444): a drift/uncodified
    finding is annotated with boot-pull.service health, because a bare
    hash-mismatch line gives no signal that the box's ONLY reconciliation
    path outside a manual install failed rather than someone hand-editing a
    unit file. Diagnostic only — must never raise, never change exit_code,
    and stay silent everywhere boot-pull.service does not apply."""

    def test_failed_boot_pull_is_named(self, cd):
        module, _, _ = cd
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return _FakeCompleted(0, "LoadState=loaded\nActiveState=failed\nResult=exit-code\n")

        diagnosis = module._boot_pull_diagnosis(run=fake_run)
        assert diagnosis is not None
        assert "boot-pull.service" in diagnosis
        assert "FAILED" in diagnosis
        assert calls and calls[0][:2] == ["systemctl", "show"]

    def test_healthy_boot_pull_is_silent(self, cd):
        module, _, _ = cd

        def fake_run(cmd, **kw):
            return _FakeCompleted(0, "LoadState=loaded\nActiveState=inactive\nResult=success\n")

        assert module._boot_pull_diagnosis(run=fake_run) is None

    def test_box_without_boot_pull_service_is_silent(self, cd):
        """The dashboard box (push-deployed on merge) has no boot-pull.service
        at all — `systemctl show` on a unit that was never loaded returns
        LoadState=not-found, and that must never be reported as a failure."""
        module, _, _ = cd

        def fake_run(cmd, **kw):
            return _FakeCompleted(0, "LoadState=not-found\nActiveState=inactive\nResult=success\n")

        assert module._boot_pull_diagnosis(run=fake_run) is None

    def test_missing_systemctl_is_silent_not_raised(self, cd):
        """A laptop or CI runner has no systemctl at all — this is a
        diagnostic annotation, not a hard dependency of the check."""
        module, _, _ = cd

        def fake_run(cmd, **kw):
            raise FileNotFoundError("systemctl not found")

        assert module._boot_pull_diagnosis(run=fake_run) is None

    def test_nonzero_returncode_is_silent(self, cd):
        module, _, _ = cd

        def fake_run(cmd, **kw):
            return _FakeCompleted(1, "")

        assert module._boot_pull_diagnosis(run=fake_run) is None

    def test_main_appends_diagnosis_to_findings_when_boot_pull_failed(self, cd, monkeypatch, capsys):
        """End-to-end: a drifted unit plus a failed boot-pull.service produces
        a console/alert finding set that names BOTH — the drift and its
        likely cause — not the drift alone."""
        module, script_dir, installed_dir = cd
        _write(script_dir / "daily-news.service", "UNIT NEW\n")
        _write(installed_dir / "daily-news.service", "UNIT OLD\n")
        for name in module.ALL_UNITS:
            if name != "daily-news.service":
                _write(script_dir / name, f"UNIT {name}\n")

        monkeypatch.setattr(
            module,
            "_boot_pull_diagnosis",
            lambda: "boot-pull.service (Result=exit-code) is in a FAILED state",
        )
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        exit_code = module.main()
        out = capsys.readouterr().out

        assert exit_code == 1, "the diagnosis must never change the drift exit code"
        assert "[diagnosis] boot-pull.service" in out

    def test_main_does_not_call_diagnosis_when_clean(self, cd, monkeypatch, capsys):
        """No need to shell out to systemctl on every clean run — the
        annotation only has something to explain once there is a finding."""
        module, script_dir, installed_dir = cd
        for name in module.ALL_UNITS:
            _write(script_dir / name, f"UNIT {name}\n")
            _write(installed_dir / name, f"UNIT {name}\n")

        called = []
        monkeypatch.setattr(module, "_boot_pull_diagnosis", lambda: called.append(1))
        monkeypatch.setattr("sys.argv", ["check-systemd-unit-drift.py"])
        code = module.main()

        assert code == 0
        assert not called, "_boot_pull_diagnosis must not run on a clean box"
