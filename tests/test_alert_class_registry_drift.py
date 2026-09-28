"""
Unit tests for ``infrastructure/overseer/alert_class_registry_drift.py`` — the
cross-repo alert-class registry chokepoint (alpha-engine-config-I3211 final
leg, #3305), moved into this repo beside the registry it grades under
``alpha-engine-config-I7896`` so the PUBLIC emitter repos can run it at their
own merge time (``alert_class_pr_guard.py``) without a private checkout.

Uses synthetic playbooks.yaml trees and synthetic repo source files so the
test is self-contained and does not depend on network access or live checkouts.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"))
import alert_class_registry_drift as d  # noqa: E402

# ── helpers ──


def _make_playbooks(tmp_path: Path, alert_classes: list[dict]) -> Path:
    """Write a synthetic playbooks.yaml and return its path."""
    p = tmp_path / "infrastructure" / "overseer"
    p.mkdir(parents=True)
    data = {"schema_version": 1, "playbooks": {}, "alert_classes": alert_classes}
    (p / "playbooks.yaml").write_text(yaml.dump(data))
    return p / "playbooks.yaml"


def _make_repo(tmp_path: Path, name: str, files: dict[str, str]) -> Path:
    """Create a synthetic repo under tmp_path with the given files."""
    root = tmp_path / name
    for rel, content in files.items():
        f = root / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(content)
    return root


# ── fixtures ──

_BASIC_CLASSES = [
    {"class": "executor_turnover_tripwire", "source": "alpha-engine/executor/turnover_tripwire.py",
     "severities": ["dynamic"], "intake": "bus", "response": "drain-queue"},
    {"class": "freshness_monitor_staleness", "source": "freshness-monitor",
     "severities": ["dynamic"], "intake": "bus", "response": "playbook:alert-drain"},
    {"class": "groom_lifecycle_bus_events", "source": "groom:*",
     "severities": ["error", "warning"], "intake": "bus", "response": "drain-queue"},
    {"class": "cw_alarms_all", "source": "cloudwatch-alarm:*",
     "severities": ["alarm"], "intake": "cw-alarm", "response": "drain-queue"},
    {"class": "research_score_aggregator_gap", "source": "research:score_aggregator",
     "severities": ["warning"], "intake": "bus", "response": "drain-queue"},
]


# ── tests for _find_publish_source_literals ──


class TestFindPublishSourceLiterals:
    def test_single_line_publish_with_source(self):
        text = '''from nousergon_lib import alerts
alerts.publish(source="my-source", severity="warning")'''
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"my-source"}

    def test_multi_line_publish(self):
        text = """alerts.publish(
    severity="warning",
    source="multi-line-source",
    message="test",
)"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"multi-line-source"}

    def test_publish_ops_alert(self):
        text = """publish_ops_alert("test alert", severity="error", source="my-ops-alert")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"my-ops-alert"}

    def test_publish_observe_alert(self):
        text = """publish_observe_alert("observe alert", source="observe-source")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"observe-source"}

    def test_krepis_alerts_publish(self):
        text = """krepis.alerts.publish(severity="critical", source="krepis-source")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"krepis-source"}

    def test_multiple_publish_calls(self):
        text = """alerts.publish(source="first")
alerts.publish(source="second")
publish_ops_alert("test", source="third")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"first", "second", "third"}

    def test_nested_parentheses(self):
        text = """alerts.publish(
    source="nested-source",
    message=format_message(get_data("test")),
)"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"nested-source"}

    def test_no_source_arg(self):
        text = '''alerts.publish(severity="warning", message="no source here")'''
        assert d._find_publish_source_literals(text, Path("dummy.py")) == set()

    def test_source_with_single_quotes(self):
        text = """alerts.publish(source='single-quoted-source')"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"single-quoted-source"}

    def test_non_publish_source_ignored(self):
        """source= in non-publish contexts should not be extracted."""
        text = """other_func(source="not-a-publish")
alerts.publish(source="real-source")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"real-source"}

    def test_source_with_dynamic_value(self):
        """Dynamic source= values (variables, not literals) are not matched."""
        text = '''alerts.publish(source=source_var, severity="warning")'''
        sources = d._find_publish_source_literals(text, Path("dummy.py"))
        assert sources == set()

    def test_publish_ops_digest(self):
        text = """publish_ops_digest("digest", source="digest-source")"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"digest-source"}

    def test_very_nested_call(self):
        text = """alerts.publish(
    severity="error",
    source="deeply-nested",
    extra=some_function(
        arg1="hello",
        arg2=other_call(nested=True),
    ),
)"""
        assert d._find_publish_source_literals(text, Path("dummy.py")) == {"deeply-nested"}


# ── tests for _source_matches_registry ──


class TestSourceMatchesRegistry:
    def test_exact_match(self):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)
        assert d._source_matches_registry("freshness-monitor", patterns)

    def test_wildcard_prefix_match(self):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)
        assert d._source_matches_registry("groom:nousergon/alpha-engine-config@main", patterns)
        assert d._source_matches_registry("cloudwatch-alarm:CPUUtilization", patterns)

    def test_no_match(self):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)
        assert not d._source_matches_registry("unknown-source", patterns)

    def test_wildcard_no_match(self):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)
        assert not d._source_matches_registry("other-prefix:test", patterns)

    def test_empty_patterns(self):
        assert not d._source_matches_registry("anything", [])

    def test_slash_wildcard_is_a_prefix_claim_that_keeps_its_slash(self):
        """``foo/*`` claims ``foo/<anything>`` but never ``foobar`` — the same
        semantics krepis.alert_tiers._match applies at emit time
        (alpha-engine-config-I9409)."""
        patterns = d._build_registry_patterns([
            {"class": "alert_transport_liveness_probe",
             "source": "alert-transport-liveness/*"},
        ])
        assert patterns[0][2] is True
        assert d._source_matches_registry("alert-transport-liveness/ne-admin", patterns)
        assert d._source_matches_registry(
            "alert-transport-liveness/github-actions-alert-transport-liveness", patterns)
        assert not d._source_matches_registry("alert-transport-livenessX", patterns)
        assert not d._source_matches_registry("alert-transport-liveness", patterns)

    def test_research_prefix(self):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)
        assert d._source_matches_registry("research:score_aggregator", patterns)
        assert not d._source_matches_registry("research:unknown", patterns)


# ── integration tests with synthetic repos ──


class TestScanRepo:
    def test_all_sources_registered(self, tmp_path):
        """Repo with only registered sources should pass cleanly."""
        patterns = d._build_registry_patterns(_BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "src/module.py": """alerts.publish(source="freshness-monitor", severity="warning")""",
            "src/other.py": """alerts.publish(source="alpha-engine/executor/turnover_tripwire.py")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert uncovered == {}

    def test_uncovered_source(self, tmp_path):
        """Repo with an unregistered source should report it."""
        patterns = d._build_registry_patterns(_BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "src/module.py": """alerts.publish(source="unregistered-source", severity="warning")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert "unregistered-source" in uncovered
        assert "src/module.py" in uncovered["unregistered-source"]

    def test_skips_test_files(self, tmp_path):
        """Test files should be excluded from scanning."""
        patterns = d._build_registry_patterns(_BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "tests/test_module.py": """alerts.publish(source="test-only-source")""",
            "src/test_foo.py": """alerts.publish(source="another-test-source")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert uncovered == {}

    def test_wildcard_coverage(self, tmp_path):
        """Sources matching wildcard patterns should not be flagged."""
        classes = _BASIC_CLASSES + [
            {"class": "research_experiment_record_alerts", "source": "research:experiment_record",
             "severities": ["warning"], "intake": "bus", "response": "drain-queue"},
        ]
        patterns = d._build_registry_patterns(classes)

        repo = _make_repo(tmp_path, "research", {
            "producers/runner.py": """publish_observe_alert("recording", source="research:experiment_record")""",
            "lambda/handler.py": """publish_observe_alert("handling", source="research:thinktank_daily")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert "research:experiment_record" not in uncovered
        # research:thinktank_daily is NOT registered, so should be flagged
        assert "research:thinktank_daily" in uncovered

    def test_skips_virtual_envs(self, tmp_path):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            ".venv/lib/site-packages/package.py": """alerts.publish(source="venv-source")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert uncovered == {}

    def test_mixed_registered_and_unregistered(self, tmp_path):
        patterns = d._build_registry_patterns(_BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "src/registered.py": """alerts.publish(source="freshness-monitor")""",
            "src/unregistered.py": """alerts.publish(source="brand-new-source", severity="error")""",
        })

        uncovered = d._scan_repo(repo, patterns)
        assert "freshness-monitor" not in uncovered
        assert "brand-new-source" in uncovered


# ── end-to-end check() tests ──


class TestCheck:
    def test_success_no_drift(self, tmp_path):
        playbooks = _make_playbooks(tmp_path, _BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "module.py": """alerts.publish(source="freshness-monitor")""",
        })

        assert d.check(playbooks, [repo]) == 0

    def test_drift_found(self, tmp_path):
        playbooks = _make_playbooks(tmp_path, _BASIC_CLASSES)

        repo = _make_repo(tmp_path, "test-repo", {
            "module.py": """alerts.publish(source="unknown-source")""",
        })

        assert d.check(playbooks, [repo]) == 1

    def test_missing_playbooks(self, tmp_path):
        fake_path = tmp_path / "nonexistent.yaml"
        repo = _make_repo(tmp_path, "test-repo", {"m.py": "pass"})
        assert d.check(fake_path, [repo]) == 1

    def test_no_valid_repos(self, tmp_path):
        playbooks = _make_playbooks(tmp_path, _BASIC_CLASSES)
        assert d.check(playbooks, [tmp_path / "nonexistent"]) == 1

    def test_multiple_repos_one_drift(self, tmp_path):
        playbooks = _make_playbooks(tmp_path, _BASIC_CLASSES)

        repo1 = _make_repo(tmp_path, "repo-ok", {
            "m.py": """alerts.publish(source="freshness-monitor")""",
        })
        repo2 = _make_repo(tmp_path, "repo-drift", {
            "m.py": """alerts.publish(source="leaked-source")""",
        })

        assert d.check(playbooks, [repo1, repo2]) == 1


# ── CLI-shaped emission (I6753) ──

_CLI_CLASSES = [
    {"class": "scan_unlisted_state", "source": "scan-unlisted-state",
     "severities": ["warning"], "intake": "bus", "response": "drain-queue"},
    {"class": "deploy_notification_research",
     "source": "alpha-engine-research/infrastructure/deploy.sh",
     "severities": ["error"], "intake": "bus", "response": "drain-queue"},
]


class TestCliShapedEmission:
    def test_sh_registered_source_passes(self, tmp_path):
        """Shell invocation with a registered literal --source: clean."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infra/check.sh": (
                '"$VENV_PY" -m krepis.alerts publish \\\n'
                '    --message "$msg" \\\n'
                "    --severity warning \\\n"
                "    --source scan-unlisted-state \\\n"
                '    --dedup-key "$dkey"\n'
            ),
        })
        assert d._scan_repo(repo, patterns) == {}

    def test_py_fstring_unregistered_source_flagged(self, tmp_path):
        """CLI invocation built in a Python f-string: unregistered source drifts."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infra/scanner.py": (
                "cmd = (\n"
                "    f'exec \"{VENV_PY}\" -m krepis.alerts publish '\n"
                "    f\"--message {msg} --severity critical \"\n"
                "    f\"--source rogue-new-emitter --dedup-key {key}\"\n"
                ")\n"
            ),
        })
        uncovered = d._scan_repo(repo, patterns)
        assert "rogue-new-emitter" in uncovered

    def test_path_shaped_source_not_truncated(self, tmp_path):
        """A path-shaped --source matches its row in full — never cut at `/`.

        First live run truncated `alpha-engine-research/infrastructure/
        deploy.sh` to `alpha-engine-research`, mis-reporting a covered
        emitter as uncovered drift.
        """
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infrastructure/deploy.sh": (
                "python3 -m krepis.alerts publish \\\n"
                "  --severity error \\\n"
                '  --source "alpha-engine-research/infrastructure/deploy.sh" \\\n'
                '  --message "deploy failed"\n'
            ),
        })
        assert d._scan_repo(repo, patterns) == {}

    def test_sourceless_cli_invocation_fails(self, tmp_path):
        """No --source at all: unattributable, reported under the sentinel."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "watch/canary.py": (
                "subprocess.run(\n"
                '    ["python3", "-m", "krepis.alerts", "--message", message,\n'
                '     "--severity", "error"],\n'
                "    capture_output=True, timeout=20, check=False,\n"
                ")\n"
            ),
        })
        uncovered = d._scan_repo(repo, patterns)
        assert d.MISSING_SOURCE_SENTINEL in uncovered
        assert "watch/canary.py" in uncovered[d.MISSING_SOURCE_SENTINEL]

    def test_python_arglist_source_extracted(self, tmp_path):
        """subprocess arg-list form: `"--source", "name"` is a literal source."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "watch/canary.py": (
                "subprocess.run(\n"
                '    ["python3", "-m", "krepis.alerts", "publish",\n'
                '     "--message", message, "--severity", "error",\n'
                '     "--source", "router-canary"],\n'
                "    check=False,\n"
                ")\n"
            ),
        })
        uncovered = d._scan_repo(repo, patterns)
        assert "router-canary" in uncovered  # not registered in _CLI_CLASSES
        assert d.MISSING_SOURCE_SENTINEL not in uncovered

    def test_dynamic_source_skipped(self, tmp_path):
        """`--source "$SRC"`: statically unverifiable, skipped — not sourceless."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infra/generic.sh": (
                'python3 -m krepis.alerts publish --message "$msg" '
                '--severity "$sev" --source "$SRC"\n'
            ),
        })
        assert d._scan_repo(repo, patterns) == {}

    def test_prose_mention_not_an_invocation(self, tmp_path):
        """A comment naming the channel is not an invocation (box_health.sh
        false-positived on `krepis.alerts publish` prose in the first run —
        only the `-m krepis.alerts` module shape counts)."""
        patterns = d._build_registry_patterns(_CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infra/notes.sh": (
                "# the verdict travelled ONLY via `krepis.alerts publish` "
                "(SNS + Telegram).\n"
            ),
        })
        assert d._scan_repo(repo, patterns) == {}

    def test_check_end_to_end_cli_drift(self, tmp_path):
        playbooks = _make_playbooks(tmp_path, _CLI_CLASSES)
        repo = _make_repo(tmp_path, "test-repo", {
            "infra/new.sh": (
                'python3 -m krepis.alerts publish --message "x" '
                "--severity error --source undeclared-cli-emitter\n"
            ),
        })
        assert d.check(playbooks, [repo]) == 1
