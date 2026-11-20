"""Unit tests for ``infrastructure/overseer/alert_message_lint.py``.

``alpha-engine-config-I9460``. Overseer invariant 13 — a guard is not a guard
until it has been observed failing — so every rule is asserted on a real
instance from the corpus in BOTH directions: the pre-fix text must flag, and
the text the sweep replaced it with must not.

The literals below are quoted from the fleet as it stood, not invented:

* ``crucible-executor`` at ``16574bd`` (the base of ``PR518``) for ALERT001
  and ALERT002, and at ``4296f76`` for their fixed forms.
* ``crucible-dashboard`` at ``25bc37ec`` (the base of ``PR810``) for the
  shell-composed ``(detail in journal)``.
* ``nousergon-data`` at ``9018a8e5`` (the base of ``PR1603``) for the
  run-keyed ``_digest_dedup_key(decisions, now, unproduced)``.

The three positive controls named in the issue are pinned too, because the
expensive failure mode for a lint is not missing a defect — it is flagging a
correct call site, which gets the whole rule switched off within a week.
"""
# ruff: noqa: ANN001, ANN002, ANN003, ANN201, ANN202, ANN204, ANN205, ANN206

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_OVERSEER = Path(__file__).resolve().parent.parent / "infrastructure" / "overseer"
sys.path.insert(0, str(_OVERSEER))
import alert_message_lint as lint  # noqa: E402


# ── helpers ─────────────────────────────────────────────────────────────────


def _rules(text: str, relpath: str = "emitter_alert.py") -> list[str]:
    findings, waivers = lint.lint_text(text, relpath)
    return sorted(f.rule for f in findings if not lint._waived(f, waivers))


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True, timeout=60
    ).stdout


def _init_repo(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "t@t")
    _git(root, "config", "user.name", "t")


def _commit(root: Path, message: str) -> str:
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "--allow-empty", "-m", message)
    return _git(root, "rev-parse", "HEAD").strip()


# ── ALERT001: the unanswerable ask ──────────────────────────────────────────


def test_alert001_flags_the_executor_ask_verbatim():
    """The instance Brian quoted, at the commit that introduced it."""
    src = '''
def emit(run_date, detail):
    publish_ops_alert(
        f"[executor] Optimizer large rebalance: {detail} (run_date={run_date}) "
        f"Review and approve acceleration if the reallocation is intended.",
        severity="WARN", source="executor:optimizer", dedup_key="x",
    )
'''
    assert "ALERT001" in _rules(src)


def test_alert001_does_not_flag_the_fixed_form():
    """``crucible-executor-PR518`` deleted the ask and kept the facts."""
    src = '''
def emit(run_date, detail):
    publish_ops_alert(
        f"[executor] Optimizer large rebalance: {detail} (run_date={run_date})",
        severity="WARN", source="executor:optimizer", dedup_key="x",
    )
'''
    assert "ALERT001" not in _rules(src)


@pytest.mark.parametrize(
    "message",
    [
        "Please confirm the reallocation before the next session opens.",
        "Acknowledge this page so the watchdog stops re-arming the gate.",
        "Turnover accelerated past the band; approve if intended.",
        "Position drift exceeded the cap unless this is expected by the desk.",
        "The rebalance needs sign-off before Monday's open.",
    ],
)
def test_alert001_tells(message: str):
    assert "ALERT001" in _rules(f'publish_ops_alert("{message}", source="s")')


@pytest.mark.parametrize(
    "message",
    [
        # Past participles are statements of fact, which is what an alert is for.
        "mTLS to the vires origin confirmed byte-exact on both sides.",
        "Champion promotion approved by the weekly gate; version_id recorded.",
        # An inability, not a request.
        "cycle indeterminate: monitor probe failed — cannot confirm cycle",
        "a stage list this card cannot confirm was dispatched",
    ],
)
def test_alert001_does_not_flag_statements_of_fact(message: str):
    assert "ALERT001" not in _rules(f'publish_ops_alert("{message}", source="s")')


# ── ALERT002: go look it up ─────────────────────────────────────────────────


def test_alert002_flags_the_executor_shadow_logs_ask_across_fstring_parts():
    """The tell straddles two adjacent f-string literals, exactly as written.

    Reading the parts separately made this instance invisible — the single
    highest-value ALERT002 in the corpus — which is why the f-string parts are
    rejoined before matching.
    """
    src = '''
def emit(rolling_sum, window, run_date):
    publish_ops_alert(
        message=(
            f"[executor] TURNOVER TRIPWIRE (rolling): summed {rolling_sum:.1%} over "
            f"{len(window)} session(s) (run_date={run_date}). The book is churning "
            f"abnormally even though each day is under the cap; review "
            f"the optimizer shadow logs for the driver."
        ),
        source="executor:turnover_tripwire",
    )
'''
    assert "ALERT002" in _rules(src)


def test_alert002_does_not_flag_the_fixed_attributing_form():
    """``PR518`` replaced the ask with the driver the emitter had all along."""
    src = '''
def emit(attribution):
    publish_ops_alert(
        message=(
            f"[executor] TURNOVER TRIPWIRE (rolling): "
            f"DRIVER ({attribution['driver']}): {attribution['detail']}"
        ),
        source="executor:turnover_tripwire",
    )
'''
    assert "ALERT002" not in _rules(src)


def test_alert002_flags_detail_in_journal_composed_in_shell():
    """``crucible-dashboard``'s ``box_health.sh`` never touched a publish call.

    The message was composed in a shell function and published elsewhere, so a
    lint scoped to publish-call bodies would have missed 32 of the 134 pages
    measured over the window.
    """
    src = 'emit_line() {\n    echo "memory budget: BREACH (detail in journal)"\n}\n'
    assert "ALERT002" in _rules(src, "infrastructure/box_health.sh")


def test_alert002_does_not_flag_the_fixed_driver_vocabulary():
    src = 'emit_line() {\n    echo "memory budget: BREACH driver=cgroup-oom-kill unit=$u"\n}\n'
    assert "ALERT002" not in _rules(src, "infrastructure/box_health.sh")


@pytest.mark.parametrize(
    "message",
    [
        "Deploy failed at $SHA — auto-reverted. Investigate before re-merging.",
        "Check CloudWatch logs for details.",
        "Predictions not written; see the manifest for the cause.",
        "Promotion refused — inspect s3://alpha-engine-research/weights/meta/.",
        "The gate breached; check the PHASE_END logs for the first error.",
    ],
)
def test_alert002_tells(message: str):
    assert "ALERT002" in _rules(f'publish_ops_alert("{message}", source="s")')


@pytest.mark.parametrize(
    "message",
    [
        # A fact about what the emitter itself could not do. Flagging this
        # teaches authors to stop writing honest failure reports.
        "turnover tripwire: could not read shadow log for %s: %s",
        "champion_monitor: could not resolve the serving champion version_id",
        # A statement about this component's own return value.
        "the weekly-SF silence deadman owns the page; this check reports ONLY_GATE_SKIPS.",
        # A past-tense report of a completed check.
        "FATAL: pip check reported non-allowlisted dependency conflicts:",
    ],
)
def test_alert002_does_not_flag_statements(message: str):
    assert "ALERT002" not in _rules(f'publish_ops_alert("{message}", source="s")')


def test_ui_surfaces_are_out_of_scope():
    """"Check here during triage" is correct guidance on a page someone opened.

    The anti-pattern is defined by the delivery surface: an alert arrives
    unasked, a dashboard view does not. The line is drawn on the path.
    """
    src = 'st.markdown("Warning rows appear below but do not route — check the logs during triage.")'
    assert lint._in_scope("views/26_Artifact_Freshness.py", src) is False
    assert lint._in_scope("monitoring/freshness_alert.py", src) is True
    assert _rules(src, "monitoring/freshness_alert.py") == ["ALERT002"]


# ── ALERT003: run-keyed dedup ───────────────────────────────────────────────


def test_alert003_flags_the_freshness_digest_run_key():
    """``nousergon-data-PR1603``'s pre-fix key, assigned to a local.

    The publish call passed the LOCAL, so a rule reading only keyword
    arguments would have missed the instance the PR exists to fix.
    """
    src = '''
def emit(decisions, now, unproduced):
    dedup_key = _digest_dedup_key(decisions, now, unproduced)
    publish_ops_alert("freshness digest", source="freshness-monitor", dedup_key=dedup_key)
'''
    findings, _ = lint.lint_text(src, "infrastructure/lambdas/freshness-monitor/index.py")
    a3 = [f for f in findings if f.rule == "ALERT003"]
    assert a3 and "per-execution" in a3[0].why


def test_alert003_does_not_flag_the_fixed_episode_key():
    src = '''
def emit(decisions, unproduced):
    dedup_key = _digest_dedup_key(decisions, unproduced)
    publish_ops_alert("freshness digest", source="freshness-monitor", dedup_key=dedup_key)
'''
    assert "ALERT003" not in _rules(src, "infrastructure/lambdas/freshness-monitor/index.py")


@pytest.mark.parametrize(
    "expr,tier",
    [
        ("f'freshness_digest_{today}_{fp}'", "calendar"),
        ("f'watchdog:{pipeline}:{record[\"execution_arn\"]}'", "execution"),
        ("f'lane_never_started:{now:%Y-%m-%dT%H:%M}'", "execution"),
        ("f'canary-replay:{run_token}'", "execution"),
        ("f'turnover_tripwire_daily_{run_date}'", "calendar"),
        ("f'model_zoo_promote_{date_str}'", "calendar"),
        ("f'predictor_shadow_unmeasurable_{trading_day}'", "calendar"),
    ],
)
def test_alert003_tiers(expr: str, tier: str):
    assert lint.classify_dedup_expression(expr)[0] == tier


@pytest.mark.parametrize(
    "expr",
    [
        # The three positive examples the issue names, verbatim in shape.
        "f'lambda-deploy-drift-{report[\"head_sha\"][:12]}-{severity}'",
        "'definition-drift-' + hashlib.sha256(payload).hexdigest()[:12]",
        "f'{leaderboard_id}_leaderboard_vacuous:{champion_name}:{sorted_challengers}'",
        # PR780's fixed grouping. `as_of` is the UPSTREAM artifact's own stamp:
        # it stops moving exactly when the episode closes, which is what
        # episode identity means.
        "f'stale-upstream:{driver}:{upstream_prefix}:{as_of}'",
        "group.episode_key()",
        "f'freshness_digest_{fingerprint}'",
    ],
)
def test_alert003_passes_the_positive_controls(expr: str):
    assert lint.classify_dedup_expression(expr) is None


def test_alert003_reads_shell_dedup_keys():
    src = 'python3 -m krepis.alerts publish --source x --dedup-key "drift-$RUN_DATE"\n'
    assert "ALERT003" in _rules(src, "infrastructure/alert_on_failure.sh")


# ── the waiver ──────────────────────────────────────────────────────────────


def test_a_waiver_with_a_reason_suppresses():
    src = '''
def emit(execution_arn, pipeline):
    # alert-lint: allow ALERT003 -- info-severity per-run receipt; the SF
    #   execution IS the episode, per saturday-sf-watch-dispatcher's design.
    publish_ops_alert("receipt", source="sf-watch", dedup_key=f"watch:{execution_arn}")
'''
    assert _rules(src) == []


def test_a_waiver_with_no_reason_is_itself_a_finding():
    src = '''
def emit(execution_arn):
    # alert-lint: allow ALERT003
    publish_ops_alert("receipt", source="sf-watch", dedup_key=f"watch:{execution_arn}")
'''
    rules = _rules(src)
    assert "ALERT000" in rules
    assert "ALERT003" in rules, "a bare suppression must not suppress"


def test_a_waiver_with_a_token_reason_is_a_finding():
    src = '''
def emit(execution_arn):
    # alert-lint: allow ALERT003 -- ok
    publish_ops_alert("receipt", source="sf-watch", dedup_key=f"watch:{execution_arn}")
'''
    assert "ALERT000" in _rules(src)


def test_a_waiver_does_not_reach_beyond_its_lookback():
    src = (
        "# alert-lint: allow ALERT003 -- a reason long enough to be a real one\n"
        + "x = 1\n" * 10
        + 'publish_ops_alert("m", source="s", dedup_key=f"k:{execution_arn}")\n'
    )
    assert "ALERT003" in _rules(src)


def test_a_waiver_is_rule_specific():
    src = '''
def emit(execution_arn):
    # alert-lint: allow ALERT001 -- this one is about the ask, not the key
    publish_ops_alert("receipt", source="s", dedup_key=f"watch:{execution_arn}")
'''
    assert "ALERT003" in _rules(src)


def test_waivers_are_enumerable(tmp_path: Path, capsys):
    root = tmp_path / "repo"
    (root / "infrastructure").mkdir(parents=True)
    (root / "infrastructure" / "alerting.py").write_text(
        "# alert-lint: allow ALERT003 -- the SF execution IS the episode here\n"
        'publish_ops_alert("m", source="s", dedup_key=f"k:{execution_arn}")\n'
    )
    rc = lint.main(["--repo", "r", "--repo-root", str(root), "--list-waivers"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "1 alert-lint waiver(s)" in out
    assert "the SF execution IS the episode here" in out


# ── delta behaviour and exit codes ──────────────────────────────────────────


_BAD = (
    'def emit(run_date):\n'
    '    publish_ops_alert("Turnover accelerated. Review and approve if intended.",\n'
    '                      source="executor:optimizer", dedup_key="k")\n'
)
_CLEAN = 'def emit():\n    publish_ops_alert("Turnover 12.4% over the 9% band.", source="s")\n'


def test_a_new_instance_fails_its_own_pr(tmp_path: Path, capsys):
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "alerting.py").write_text(_CLEAN)
    base = _commit(root, "clean")
    (root / "alerting.py").write_text(_CLEAN + _BAD)
    head = _commit(root, "adds the ask")

    rc = lint.main(["--repo", "r", "--repo-root", str(root), "--base", base, "--head", head])
    captured = capsys.readouterr()
    assert rc == 1
    assert "ALERT001" in captured.err + captured.out
    assert "Decision Queue" in captured.err, "the failure must say what to do instead"


def test_pre_existing_instances_never_fail_an_unrelated_pr(tmp_path: Path, capsys):
    """The property that keeps a chokepoint from being routed around on day one."""
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "alerting.py").write_text(_BAD)
    base = _commit(root, "pre-existing")
    (root / "unrelated.py").write_text("VALUE = 1\n")
    head = _commit(root, "unrelated change")

    rc = lint.main(["--repo", "r", "--repo-root", str(root), "--base", base, "--head", head])
    assert rc == 0
    assert "pre-existing findings NOT gated by this PR (1)" in capsys.readouterr().out


def test_an_unrelated_edit_that_shifts_lines_is_not_a_new_finding(tmp_path: Path):
    """Fingerprints exclude the line number, deliberately.

    Keying on the line would report every pre-existing instance in a touched
    file as newly introduced, and a lint that cries about code the PR did not
    write is the one that gets switched off.
    """
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "alerting.py").write_text(_BAD)
    base = _commit(root, "pre-existing")
    (root / "alerting.py").write_text("# a new leading comment\n" * 20 + _BAD)
    head = _commit(root, "shifts the lines")

    assert lint.main(["--repo", "r", "--repo-root", str(root),
                      "--base", base, "--head", head]) == 0


def test_warn_only_annotates_and_exits_zero(tmp_path: Path, capsys):
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "alerting.py").write_text(_CLEAN)
    base = _commit(root, "clean")
    (root / "alerting.py").write_text(_CLEAN + _BAD)
    head = _commit(root, "adds the ask")

    rc = lint.main(["--repo", "r", "--repo-root", str(root),
                    "--base", base, "--head", head, "--warn-only"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "::warning file=alerting.py,line=" in out
    assert "::error" not in out


def test_a_non_git_root_is_unmeasured_never_clean(tmp_path: Path, capsys):
    rc = lint.main(["--repo", "r", "--repo-root", str(tmp_path), "--base", "x"])
    assert rc == 1
    assert "UNMEASURED" in capsys.readouterr().err


def test_a_bad_ref_is_unmeasured_never_clean(tmp_path: Path, capsys):
    root = tmp_path / "repo"
    _init_repo(root)
    (root / "alerting.py").write_text(_CLEAN)
    _commit(root, "clean")
    rc = lint.main(["--repo", "r", "--repo-root", str(root), "--base", "deadbeef"])
    assert rc == 1
    assert "UNMEASURED" in capsys.readouterr().err


# ── self-exclusion ──────────────────────────────────────────────────────────


def test_the_lint_and_the_scanner_do_not_grade_themselves():
    """Their docstrings spell out every anti-pattern by design.

    The sibling scanner shipped without this and reported three of its own
    examples as live emitters on its first CI run (``nousergon-data-PR1479``).
    """
    findings, _ = lint.scan_repo(Path(__file__).resolve().parent.parent)
    graded = {f.relpath for f in findings}
    assert "infrastructure/overseer/alert_message_lint.py" not in graded
    assert "infrastructure/overseer/alert_class_registry_drift.py" not in graded
    assert "infrastructure/overseer/alert_class_pr_guard.py" not in graded


# ── false positives found by calibration, pinned so they stay fixed ─────────
#
# Every case below flagged on a real fleet call site and was WRONG. A lint's
# expensive failure is not the defect it misses — it is the correct call site
# it flags, because that is what gets the whole rule switched off.


def test_a_decision_queue_item_is_the_remedy_not_the_defect():
    """`**Options:** ... B) Acknowledge as expected ...` — from freshness-monitor.

    ALERT001's own remedy text says a genuine human decision belongs on the
    Decision Queue with state. A queue item therefore has exactly what the rule
    says is missing: a channel, state, and a consequence of never answering.
    """
    src = (
        'publish_ops_alert(\n'
        '    "**Ask:** Investigate why `x` has stopped updating, and either fix the '
        'producer or acknowledge the staleness is expected right now.",\n'
        '    source="freshness-monitor")\n'
    )
    assert _rules(src) == []


def test_an_arrow_joined_vocabulary_is_not_an_instruction():
    """saturday-sf-watch-dispatcher describes the AGENT's own workflow."""
    src = (
        'publish_ops_alert("_autonomous fix LAUNCHING — resilience agent dispatched; '
        'outcome reported on box completion (diagnose->fix->merge->rerun)_", source="s")'
    )
    assert "ALERT002" not in _rules(src)


def test_a_shell_script_that_publishes_nothing_is_an_operator_script():
    """`pipeline-watchdog/deploy.sh` echoes "Confirm before proceeding".

    That is an interactive prompt to the person who ran the script, who can
    answer it or Ctrl-C — a channel with state. It was in scope only because
    `watchdog` appears in its PATH.
    """
    src = 'echo "WARNING: --smoke will publish a real alert."\necho "Confirm before proceeding or omit --smoke."\n'
    assert lint._in_scope("infrastructure/lambdas/pipeline-watchdog/deploy.sh", src) is False
    publishing = src + 'python3 -m krepis.alerts publish --source x --message y\n'
    assert lint._in_scope("infrastructure/lambdas/pipeline-watchdog/deploy.sh", publishing) is True


def test_a_flow_doctor_emitter_is_in_scope():
    """The shared class scanner keeps flow-doctor emitters in lint scope.

    `saturday-sf-watch-dispatcher/index.py` and `scheduled-groom-dispatcher/index.py`
    were out of scope entirely before I9490 — 4 dedup keys and 6
    unactionable messages that no guard had ever looked at
    (``alpha-engine-config-I9490``).
    """
    src = 'notify_via_flow_doctor(text, dedup_key=f"w:{execution_arn}", flow_name="x")'
    assert lint._in_scope("infrastructure/lambdas/saturday-sf-watch-dispatcher/index.py", src)
    assert "ALERT003" in _rules(src, "infrastructure/lambdas/saturday-sf-watch-dispatcher/index.py")


def test_a_clock_truncated_to_a_day_is_graded_calendar_not_execution():
    """`pipeline-watchdog` keys on `datetime.now(timezone.utc).date().isoformat()`.

    Still run-keyed, but it re-pages daily rather than per-run, and the tier is
    what sets the remediation priority in the register.
    """
    assert lint.classify_dedup_expression(
        "f'pipeline-watchdog-{sf_label}-{datetime.now(timezone.utc).date().isoformat()}'"
    ) == ("calendar", "now")
    assert lint.classify_dedup_expression("f'canary-replay:{run_token}'")[0] == "execution"
