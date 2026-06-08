"""Verify flow-doctor wiring in data-module entrypoints.

Asserts that the canonical alpha-engine-lib pattern (setup_logging at
module-top, exclude_patterns plumbed, yaml resolvable from the entrypoint
location) is in place for the three data entrypoints:

- ``lambda/handler.py``     — Phase 2 alternative-data Lambda
- ``weekly_collector.py``   — Phase 1 / MorningEnrich / daily on EC2
- ``rag/preflight.py``      — RAG ingestion preflight CLI

Runs without firing any LLM diagnosis: ``setup_logging`` is exercised with
FLOW_DOCTOR_ENABLED=1 + stub env vars + a real yaml, but no ERROR records
are emitted (so flow-doctor's report() / diagnose() pipeline is never
triggered — no Anthropic calls, no email, no GitHub issue).
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def stub_flow_doctor_env(monkeypatch):
    """Populate the env vars that flow-doctor.yaml's ${VAR} refs resolve.

    flow_doctor.init() substitutes these at load time. Stubs are non-empty
    strings; nothing actually contacts SMTP/GitHub since no report() fires.

    FLOW_DOCTOR_SKIP_PREFLIGHT=1 is required because flow-doctor 0.4.0
    added strict token preflight on GitHubNotifier.validate() (calls
    api.github.com /user with the configured token at init time). With a
    stub token like "stub-token" the call returns 401 and raises — which
    is correct behavior in production (catches revoked PATs at startup)
    but breaks tests that don't intend to fire the network call. Same
    knob applies to S3Notifier's bucket head-check in 0.4.0+.
    """
    monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "1")
    monkeypatch.setenv("FLOW_DOCTOR_SKIP_PREFLIGHT", "1")
    monkeypatch.setenv("EMAIL_SENDER", "test@example.com")
    monkeypatch.setenv("EMAIL_RECIPIENTS", "test@example.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "stub-password")
    monkeypatch.setenv("FLOW_DOCTOR_GITHUB_TOKEN", "stub-token")
    # 0.6.0rc2 soak: flow-doctor.yaml now enables Haiku diagnosis with
    # api_key: ${ANTHROPIC_API_KEY}. flow-doctor fails loud on an unresolved
    # ${VAR}, so the wiring tests must seed it (mirrors the runtime, where the
    # box resolves it from SSM /alpha-engine/ANTHROPIC_API_KEY).
    monkeypatch.setenv("ANTHROPIC_API_KEY", "stub-anthropic-key")


@pytest.fixture
def temp_flow_doctor_yaml(tmp_path):
    """Write a copy of the production flow-doctor.yaml with store.path
    redirected into the test's tmp_path. Returns the temp yaml path.

    The production yaml hardcodes /tmp/flow_doctor.db (Lambda ephemeral
    convention) which isn't writable in every CI/sandbox env. Tests that
    actually invoke flow_doctor.init() need a redirectable path.
    """
    import yaml as yamllib
    with open(REPO_ROOT / "flow-doctor.yaml") as f:
        cfg = yamllib.safe_load(f)
    cfg["store"]["path"] = str(tmp_path / "flow_doctor_test.db")
    yaml_path = tmp_path / "flow-doctor.yaml"
    with open(yaml_path, "w") as f:
        yamllib.safe_dump(cfg, f)
    return str(yaml_path)


@pytest.fixture
def reset_root_logger():
    """Snapshot + restore root logger handlers around each test."""
    root = logging.getLogger()
    saved = list(root.handlers)
    yield
    root.handlers = saved


def _flow_doctor_available() -> bool:
    try:
        import flow_doctor  # noqa: F401
        return True
    except ImportError:
        return False


flow_doctor_required = pytest.mark.skipif(
    not _flow_doctor_available(),
    reason="flow-doctor not installed (pip install alpha-engine-lib[flow_doctor])",
)


class TestFlowDoctorYamlPresence:
    """The yaml file each entrypoint resolves must exist at that path.

    Catches the specific 2026-05-01 bug: flow-doctor.yaml was gitignored,
    only flow-doctor.yaml.example existed, and weekly_collector.py/main()
    pointed at the missing path — silent flow-doctor disable for months.
    """

    def test_yaml_at_repo_root_exists(self):
        assert (REPO_ROOT / "flow-doctor.yaml").is_file()

    def test_yaml_path_resolved_by_lambda_handler_exists(self):
        # Mirrors lambda/handler.py's path computation in the local-dev
        # branch (LAMBDA_TASK_ROOT unset):
        #   os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        handler_path = REPO_ROOT / "lambda" / "handler.py"
        resolved = Path(os.path.dirname(os.path.dirname(os.path.abspath(handler_path)))) / "flow-doctor.yaml"
        assert resolved.is_file(), f"Lambda handler resolves to {resolved}"

    def test_yaml_path_resolved_by_lambda_handler_under_lambda_runtime(self, tmp_path, monkeypatch):
        # Lambda flattens lambda/handler.py to /var/task/handler.py, so
        # the original two-dirs-up resolution would land at /var/ — wrong.
        # The handler must honor LAMBDA_TASK_ROOT first. Simulate by
        # placing flow-doctor.yaml under a stand-in task root and
        # asserting the same os.environ.get(...) pattern resolves to it.
        fake_task_root = tmp_path / "fake_lambda_task_root"
        fake_task_root.mkdir()
        (fake_task_root / "flow-doctor.yaml").write_text("flow_name: test\n")
        monkeypatch.setenv("LAMBDA_TASK_ROOT", str(fake_task_root))
        # Use the literal expression from lambda/handler.py so any drift
        # in the source breaks this test.
        resolved = os.path.join(
            os.environ.get(
                "LAMBDA_TASK_ROOT",
                "/should-not-fall-back",
            ),
            "flow-doctor.yaml",
        )
        assert os.path.isfile(resolved), (
            "Lambda handler must honor LAMBDA_TASK_ROOT — flattened image "
            "layout means dirname(dirname(__file__)) lands at /var, not /var/task"
        )

    def test_yaml_path_resolved_by_weekly_collector_exists(self):
        # Mirrors weekly_collector.py's path computation:
        #   Path(__file__).parent / "flow-doctor.yaml"
        wc_path = REPO_ROOT / "weekly_collector.py"
        resolved = wc_path.parent / "flow-doctor.yaml"
        assert resolved.is_file(), f"weekly_collector resolves to {resolved}"

    def test_yaml_path_resolved_by_rag_preflight_exists(self):
        # Mirrors rag/preflight.py's path computation:
        #   os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        rp_path = REPO_ROOT / "rag" / "preflight.py"
        resolved = Path(os.path.dirname(os.path.dirname(os.path.abspath(rp_path)))) / "flow-doctor.yaml"
        assert resolved.is_file(), f"rag/preflight resolves to {resolved}"


class TestFlowDoctorYamlSchema:
    """flow-doctor.yaml must declare the required keys for the Saturday SF.

    Drift between repos has surfaced in past PRs (research missing github
    notify, predictor missing rate-limit cap fields). Lock the data-module
    yaml to the executor-canonical shape.
    """

    def test_yaml_has_required_top_level_keys(self):
        import yaml
        with open(REPO_ROOT / "flow-doctor.yaml") as f:
            cfg = yaml.safe_load(f)
        for key in ("flow_name", "repo", "notify", "store", "rate_limits"):
            assert key in cfg, f"missing top-level key: {key}"
        assert cfg["flow_name"] == "data-collector"
        assert cfg["repo"] == "cipher813/alpha-engine-data"

    def test_yaml_has_email_and_github_notify_channels(self):
        import yaml
        with open(REPO_ROOT / "flow-doctor.yaml") as f:
            cfg = yaml.safe_load(f)
        types = {n.get("type") for n in cfg.get("notify", [])}
        assert "email" in types, "email channel required for ops alerts"
        assert "github" in types, "github issue channel required for diagnosis"

    def test_yaml_has_per_day_caps(self):
        import yaml
        with open(REPO_ROOT / "flow-doctor.yaml") as f:
            cfg = yaml.safe_load(f)
        rl = cfg.get("rate_limits", {})
        for key in ("max_alerts_per_day", "max_issues_per_day", "max_diagnosed_per_day"):
            assert key in rl, f"rate_limits.{key} required (Anthropic-cost cap)"


@flow_doctor_required
class TestSetupLoggingAttach:
    """setup_logging() should attach FlowDoctorHandler when ENABLED=1.

    Does NOT fire any ERROR records, so flow-doctor's diagnose() / Anthropic
    calls are never invoked. Verifies wiring shape only.
    """

    def test_disabled_attaches_no_flow_doctor_handler(self, monkeypatch, reset_root_logger):
        monkeypatch.setenv("FLOW_DOCTOR_ENABLED", "0")
        from alpha_engine_lib.logging import setup_logging
        setup_logging(
            "data-collector-test-disabled",
            flow_doctor_yaml=str(REPO_ROOT / "flow-doctor.yaml"),
            exclude_patterns=[],
        )
        import flow_doctor
        attached = [h for h in logging.getLogger().handlers
                    if isinstance(h, flow_doctor.FlowDoctorHandler)]
        assert attached == [], "FlowDoctorHandler should NOT attach when DISABLED"

    def test_enabled_attaches_flow_doctor_handler(
        self, stub_flow_doctor_env, reset_root_logger, temp_flow_doctor_yaml
    ):
        from alpha_engine_lib.logging import setup_logging, get_flow_doctor
        setup_logging(
            "data-collector-test-enabled",
            flow_doctor_yaml=temp_flow_doctor_yaml,
            exclude_patterns=[],
        )
        import flow_doctor
        attached = [h for h in logging.getLogger().handlers
                    if isinstance(h, flow_doctor.FlowDoctorHandler)]
        assert len(attached) == 1, (
            f"exactly one FlowDoctorHandler expected, got {len(attached)}"
        )
        assert get_flow_doctor() is not None, "shared singleton not populated"

    def test_exclude_patterns_plumbed_to_handler(
        self, stub_flow_doctor_env, reset_root_logger, temp_flow_doctor_yaml
    ):
        from alpha_engine_lib.logging import setup_logging
        patterns = [r"polygon transient 5\d\d", r"yfinance possibly delisted"]
        setup_logging(
            "data-collector-test-patterns",
            flow_doctor_yaml=temp_flow_doctor_yaml,
            exclude_patterns=patterns,
        )
        import flow_doctor
        attached = [h for h in logging.getLogger().handlers
                    if isinstance(h, flow_doctor.FlowDoctorHandler)]
        assert len(attached) == 1
        # FlowDoctorHandler compiles exclude_patterns into _exclude_re
        # (re.Pattern objects). Verify the regexes round-trip.
        compiled = attached[0]._exclude_re
        assert [p.pattern for p in compiled] == patterns


class TestEntrypointModuleTopWiring:
    """Each entrypoint must call setup_logging at MODULE-TOP, not inside a
    function. Module-top is the canonical alpha-engine-lib pattern (mirrors
    executor/main.py:67) so cold-start / import-time errors are captured.

    These are source-text checks: they verify the structural property
    without exercising flow_doctor.init() (which writes to /tmp and isn't
    portable across CI sandboxes — runtime behavior is covered by
    TestSetupLoggingAttach above using a redirectable yaml).
    """

    @staticmethod
    def _index_of(needle: str, text: str) -> int:
        idx = text.find(needle)
        assert idx != -1, f"missing required text: {needle!r}"
        return idx

    def test_lambda_handler_calls_setup_logging_at_module_top(self):
        text = (REPO_ROOT / "lambda" / "handler.py").read_text()
        # setup_logging call appears before the def of handler()
        setup_idx = self._index_of("setup_logging(", text)
        handler_def_idx = self._index_of("def handler(", text)
        assert setup_idx < handler_def_idx, (
            "setup_logging must be called at module-top, before def handler()"
        )
        # exclude_patterns is plumbed (even if empty list)
        assert "exclude_patterns=" in text[setup_idx:handler_def_idx]

    def test_weekly_collector_calls_setup_logging_at_module_top(self):
        text = (REPO_ROOT / "weekly_collector.py").read_text()
        setup_idx = self._index_of("setup_logging(", text)
        main_def_idx = self._index_of("def main()", text)
        assert setup_idx < main_def_idx, (
            "setup_logging must be called at module-top, before def main()"
        )
        # No leftover setup_logging call inside main()
        # Strip comments / docstrings so the check ignores informational
        # references to "setup_logging() already ran" left in main()'s body.
        body = "\n".join(
            line for line in text[main_def_idx:].splitlines()
            if not line.lstrip().startswith("#")
        )
        assert "setup_logging(" not in body, (
            "duplicate setup_logging call inside main() — should only run once"
        )

    def test_rag_preflight_calls_setup_logging_at_module_top(self):
        text = (REPO_ROOT / "rag" / "preflight.py").read_text()
        setup_idx = self._index_of("setup_logging(", text)
        main_def_idx = self._index_of("def main()", text)
        assert setup_idx < main_def_idx, (
            "setup_logging must be called at module-top, before def main()"
        )
        # Strip comments / docstrings so the check ignores informational
        # references to "setup_logging() already ran" left in main()'s body.
        body = "\n".join(
            line for line in text[main_def_idx:].splitlines()
            if not line.lstrip().startswith("#")
        )
        assert "setup_logging(" not in body, (
            "duplicate setup_logging call inside main() — should only run once"
        )
