"""Unit tests for infrastructure/iam/check-drift.py (config#2340 surface 3).

Pins the lambda-role drift-check coverage: the discovery that maps every
tracked `lambdas/<name>/iam-policy.json` to its primary role via the
authoritative `ROLE_NAME=`/`POLICY_NAME=` in deploy.sh, the coverage-gap guard
(a tracked policy file with no derivable role fails the sweep), and the drift
comparison logic (AWS calls monkeypatched — no live IAM in CI).

Includes a live-tree smoke: the real infrastructure/lambdas must all be
discoverable with zero gaps, so a future lambda that ships an iam-policy.json
without ROLE_NAME/POLICY_NAME fails this test before it can silently escape the
drift sweep.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
_CHECK_DRIFT = REPO_ROOT / "infrastructure" / "iam" / "check-drift.py"


def _load():
    spec = importlib.util.spec_from_file_location("check_drift", _CHECK_DRIFT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cd = _load()


# ── _parse_shell_assignment ──────────────────────────────────────────────────


def test_parse_top_level_assignment():
    text = 'ROLE_NAME="alpha-engine-foo-role"\nPOLICY_NAME="alpha-engine-foo-policy"\n'
    assert cd._parse_shell_assignment(text, "ROLE_NAME") == "alpha-engine-foo-role"
    assert cd._parse_shell_assignment(text, "POLICY_NAME") == "alpha-engine-foo-policy"


def test_parse_ignores_references_and_indented():
    """A `--role-name "${ROLE_NAME}"` reference must NOT be mistaken for a def."""
    text = '  run aws iam put-role-policy --role-name "${ROLE_NAME}"\n'
    assert cd._parse_shell_assignment(text, "ROLE_NAME") is None


def test_parse_takes_first_top_level():
    text = 'ROLE_NAME="primary-role"\nSCHED_ROLE_NAME="sched-role"\n'
    assert cd._parse_shell_assignment(text, "ROLE_NAME") == "primary-role"
    assert cd._parse_shell_assignment(text, "SCHED_ROLE_NAME") == "sched-role"


# ── discover_lambda_role_policies ────────────────────────────────────────────


def _mk_lambda(root: Path, name: str, *, policy=True, role="R", pol="P"):
    d = root / name
    d.mkdir(parents=True)
    if policy:
        (d / "iam-policy.json").write_text('{"Version":"2012-10-17"}')
    lines = []
    if role is not None:
        lines.append(f'ROLE_NAME="{role}"')
    if pol is not None:
        lines.append(f'POLICY_NAME="{pol}"')
    (d / "deploy.sh").write_text("\n".join(lines) + "\n")


def test_discovers_uniform_lambda(tmp_path):
    _mk_lambda(tmp_path, "freshness-monitor",
               role="alpha-engine-freshness-monitor-role",
               pol="alpha-engine-freshness-monitor-policy")
    rps, gaps = cd.discover_lambda_role_policies(tmp_path)
    assert gaps == []
    assert len(rps) == 1
    assert rps[0].role_name == "alpha-engine-freshness-monitor-role"
    assert rps[0].policy_name == "alpha-engine-freshness-monitor-policy"
    assert rps[0].origin == "lambdas/freshness-monitor"


def test_discovers_irregular_names(tmp_path):
    """changelog-incident-mirror: role without -role suffix, policy `-s3`."""
    _mk_lambda(tmp_path, "changelog-incident-mirror",
               role="alpha-engine-changelog-incident-mirror",
               pol="changelog-incident-mirror-s3")
    rps, gaps = cd.discover_lambda_role_policies(tmp_path)
    assert gaps == []
    assert rps[0].role_name == "alpha-engine-changelog-incident-mirror"
    assert rps[0].policy_name == "changelog-incident-mirror-s3"


def test_coverage_gap_when_no_role_name(tmp_path):
    _mk_lambda(tmp_path, "broken", role=None, pol="P")
    rps, gaps = cd.discover_lambda_role_policies(tmp_path)
    assert rps == []
    assert len(gaps) == 1 and "ROLE_NAME" in gaps[0]


def test_coverage_gap_when_no_deploy_sh(tmp_path):
    d = tmp_path / "orphan"
    d.mkdir()
    (d / "iam-policy.json").write_text("{}")
    rps, gaps = cd.discover_lambda_role_policies(tmp_path)
    assert rps == []
    assert len(gaps) == 1 and "no" in gaps[0] and "deploy.sh" in gaps[0]


def test_lambda_without_policy_file_is_not_a_role(tmp_path):
    """A lambda that ships no iam-policy.json defines no file-backed role."""
    _mk_lambda(tmp_path, "code-only", policy=False)
    rps, gaps = cd.discover_lambda_role_policies(tmp_path)
    assert rps == [] and gaps == []


# ── drift comparison (_check_policy) ─────────────────────────────────────────


def test_check_policy_clean(tmp_path, monkeypatch):
    f = tmp_path / "p.json"
    f.write_text('{"Version":"2012-10-17","Statement":[]}')
    monkeypatch.setattr(cd, "_aws_iam",
                        lambda *a: {"PolicyDocument": {"Statement": [], "Version": "2012-10-17"}})
    rp = cd.RolePolicy("r", "p", f, "lambdas/x")
    assert cd._check_policy(rp) == []


def test_check_policy_content_drift(tmp_path, monkeypatch):
    f = tmp_path / "p.json"
    f.write_text('{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}')
    monkeypatch.setattr(cd, "_aws_iam", lambda *a: {"PolicyDocument": {"Version": "2012-10-17", "Statement": []}})
    rp = cd.RolePolicy("r", "p", f, "lambdas/x")
    out = cd._check_policy(rp)
    assert len(out) == 1 and "content drift" in out[0]


def test_check_policy_missing_in_aws(tmp_path, monkeypatch):
    f = tmp_path / "p.json"
    f.write_text('{"Version":"2012-10-17"}')
    monkeypatch.setattr(cd, "_aws_iam", lambda *a: {})
    rp = cd.RolePolicy("r", "p", f, "lambdas/x")
    out = cd._check_policy(rp)
    assert len(out) == 1 and "not found on AWS role" in out[0]


# ── live-tree smoke ──────────────────────────────────────────────────────────


def test_live_lambdas_all_discoverable_no_gaps():
    rps, gaps = cd.discover_lambda_role_policies()
    assert gaps == [], f"undiscoverable tracked lambda policies: {gaps}"
    assert len(rps) >= 20  # 21 lambda roles as of config#2340 surface 3
    # every discovered role maps to a real, readable iam-policy.json
    for rp in rps:
        assert rp.source_file.name == "iam-policy.json"
        assert rp.source_file.is_file()
