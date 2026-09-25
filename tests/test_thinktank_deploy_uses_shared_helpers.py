"""thinktank-spot-dispatcher/deploy.sh, EXECUTED — not read.

WHY THIS FILE EXISTS (alpha-engine-config-I9114)
------------------------------------------------
Four workflows in this repo failed 100% of their runs on `main` for a week
because `_shared/apply_iam_policy.sh` probed a role with

    aws iam get-role ... >/dev/null 2>&1

which makes AccessDenied and NoSuchEntity the SAME observation: a denied read
was read as "the role is absent", so the script called `iam:CreateRole`, a grant
the CI identity does not and must not hold. nousergon-data-PR1569 fixed that in
the shared helper.

It did not fix `thinktank-spot-dispatcher/deploy.sh`, because that script did
not use the shared helper. It carried its own copy of the same three lines. That
is the whole cost of a private dialect: a class fix at the correct layer sails
straight past the one call site that reimplemented the layer. It had not gone
red only because its IAM lived behind `--apply-iam` / `--bootstrap`, which the
CI path does not pass — a property of the current flag layout, not a guarantee.

WHY THESE TESTS RUN THE SHELL
-----------------------------
"A loop proven only by reading it is a loop nobody has run." Asserting on the
file's TEXT would pass against the broken version too: the broken version also
contained the strings `create-role` and `get-role`. Every case below executes
the REAL deploy.sh against a fake `aws`/`python3`/`docker`/`zip` on PATH and
asserts on what was actually invoked.

The cases are deliberately a matched set. Proving role creation is unreachable
is only half an answer — a script that had simply deleted its IAM handling would
pass that alone. `test_bootstrap_still_creates_a_genuinely_absent_role` is the
other half: on an explicit NoSuchEntity the operator path must still create it.
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEPLOY = (
    _REPO_ROOT / "infrastructure" / "lambdas" / "thinktank-spot-dispatcher" / "deploy.sh"
)

ROLE = "alpha-engine-thinktank-spot-dispatcher-role"

# The literal stderr `aws iam` emits for each condition, reproduced verbatim so
# a change in how probe_role_presence greps is caught here and not in CI.
_DENIED = (
    "An error occurred (AccessDenied) when calling the GetRole operation: "
    "User: arn:aws:sts::711398986525:assumed-role/github-actions-lambda-deploy/"
    "GitHubActions is not authorized to perform: iam:GetRole on resource: "
    f"arn:aws:iam::711398986525:role/{ROLE}"
)
_NO_SUCH_ENTITY = (
    "An error occurred (NoSuchEntity) when calling the GetRole operation: "
    f"The role with name {ROLE} cannot be found."
)

# Whatever the fake python3 prints as the artifact digest, the fake
# `aws lambda get-function` must echo back, or verify_code_deployed correctly
# refuses.
_SHA = "FAKECODESHA256="


def _fake_bin(tmp_path: Path, *, get_role: tuple[int, str]) -> Path:
    """A PATH shim standing in for every external binary deploy.sh reaches.

    `get_role` is (exit_code, stderr) for `aws iam get-role` — the one call
    whose classification this whole file is about.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    aws_log = tmp_path / "aws-calls.log"
    py_log = tmp_path / "python3-calls.log"

    rc, err = get_role
    (bindir / "aws").write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            echo "$*" >> {aws_log}
            case "$1 $2" in
              "sts get-caller-identity") echo 711398986525 ;;
              "iam get-role") echo {err!r} >&2; exit {rc} ;;
              "iam get-role-policy") exit 1 ;;
              "lambda get-function") echo "{_SHA}" ;;
            esac
            exit 0
            """
        )
    )
    # `-c` is verify_code_deployed computing the artifact digest; `-m pytest` is
    # the shared handler-test gate; `-m pip` is that gate provisioning its deps.
    (bindir / "python3").write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            echo "$*" >> {py_log}
            for a in "$@"; do
              if [ "$a" = "-c" ]; then echo "{_SHA}"; exit 0; fi
            done
            exit 0
            """
        )
    )
    (bindir / "docker").write_text(
        f'#!/usr/bin/env bash\necho "docker $*" >> {aws_log}\nexit 0\n'
    )
    # zip's target is the first argument ending in .zip; create it so
    # verify_code_deployed finds a readable artifact.
    (bindir / "zip").write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            for a in "$@"; do
              case "$a" in *.zip) : > "$a"; exit 0 ;; esac
            done
            exit 0
            """
        )
    )
    for name in ("aws", "python3", "docker", "zip"):
        (bindir / name).chmod(0o755)
    return bindir


def _deploy(tmp_path: Path, *flags: str, get_role: tuple[int, str]):
    bindir = _fake_bin(tmp_path, get_role=get_role)
    # A minimal, explicit environment: the fakes must win over any real binary,
    # and nothing from the developer's shell may leak into the run.
    shell_env = {
        "PATH": f"{bindir}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "AWS_REGION": "us-east-1",
    }
    return subprocess.run(  # noqa: S603
        ["/bin/bash", str(_DEPLOY), *flags],
        capture_output=True,
        text=True,
        env=shell_env,
        timeout=300,
        check=False,
    )


def _aws_calls(tmp_path: Path) -> str:
    log = tmp_path / "aws-calls.log"
    return log.read_text() if log.exists() else ""


def _python_calls(tmp_path: Path) -> str:
    log = tmp_path / "python3-calls.log"
    return log.read_text() if log.exists() else ""


def test_deploy_script_exists_and_is_valid_bash() -> None:
    assert _DEPLOY.exists(), f"thinktank deploy.sh not found at {_DEPLOY}"
    syntax = subprocess.run(  # noqa: S603
        ["/bin/bash", "-n", str(_DEPLOY)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr


def test_code_only_path_makes_no_iam_call_at_all(tmp_path: Path) -> None:
    """THE load-bearing case. The flagless run is what
    `.github/workflows/deploy-thinktank-spot-dispatcher.yml` executes as
    `github-actions-lambda-deploy`, an identity holding neither `iam:GetRole`
    nor `iam:CreateRole` on any lambda execution role. It must not merely avoid
    CREATING a role — it must not touch IAM at all, so no probe of any kind can
    be misclassified into an attempt.
    """
    proc = _deploy(tmp_path, get_role=(255, _DENIED))
    calls = _aws_calls(tmp_path)
    assert proc.returncode == 0, (
        f"code-only deploy failed:\n{proc.stdout}\n{proc.stderr}"
    )
    iam_calls = [line for line in calls.splitlines() if line.startswith("iam ")]
    assert not iam_calls, (
        "the code-only deploy path reached IAM. It must not: the CI identity "
        "holds no iam:GetRole/iam:CreateRole/iam:PutRolePolicy on a lambda "
        "execution role by design (single-writer rule), and a DENIED probe is "
        "what got misread as 'role absent' in alpha-engine-config-I9045.\n"
        f"calls: {iam_calls}"
    )
    assert "lambda update-function-code" in calls, (
        f"the code-only path did not ship any code:\n{calls}"
    )
    assert "lambda get-function" in calls, (
        "verify_code_deployed did not read back the live CodeSha256 — the "
        "upload's own exit code is not trusted (alpha-engine-config-I8033)."
    )


def test_code_only_path_converges_the_timeout_and_nothing_else(tmp_path: Path) -> None:
    """alpha-engine-config-I11532. The timeout used to be set ONCE, by
    --bootstrap's create-function, so FN_TIMEOUT could be edited in deploy.sh
    and never reach the live function — the flagless path is the only one CI
    runs on merge. It must converge the timeout, and ONLY the timeout: the live
    function carries no environment, and an `--environment` here would silently
    wipe any override an operator set."""
    proc = _deploy(tmp_path, get_role=(255, _DENIED))
    assert proc.returncode == 0, f"{proc.stdout}\n{proc.stderr}"
    config_calls = [
        line
        for line in _aws_calls(tmp_path).splitlines()
        if line.startswith("lambda update-function-configuration")
    ]
    assert len(config_calls) == 1, config_calls
    call = config_calls[0]
    assert "--timeout 900" in call, call
    for forbidden in ("--environment", "--role", "--handler", "--runtime"):
        assert forbidden not in call, f"{forbidden} rode the timeout convergence: {call}"


def test_code_only_path_runs_the_shared_handler_test_gate(tmp_path: Path) -> None:
    """25 handler tests sat beside index.py that this deploy.sh never ran. The
    gate must be the SHARED one, so it cannot re-drift into the no-install form
    that made saturday-sf-watch-dispatcher's deploy red (config#2295)."""
    _deploy(tmp_path, get_role=(255, _DENIED))
    py = _python_calls(tmp_path)
    assert "-m pytest" in py, f"no pytest invocation reached:\n{py}"
    assert "thinktank-spot-dispatcher/test_handler.py" in py, (
        f"the lambda's own test_handler.py was never run:\n{py}"
    )
    assert "-m pip install" in py, (
        "the gate ran pytest without provisioning it — the exact config#2295 "
        f"no-install shape:\n{py}"
    )


def test_a_denied_probe_is_never_read_as_an_absent_role(tmp_path: Path) -> None:
    """The operator `--bootstrap` path under a DENIED `iam:GetRole`. This is the
    literal alpha-engine-config-I9045 condition. `probe_role_presence` must
    classify it `unknown` — not `absent` — so nothing is created, and the
    `put-role-policy` attempt (whose own success or AccessDenied is the real
    answer) is what speaks."""
    proc = _deploy(tmp_path, "--bootstrap", get_role=(255, _DENIED))
    calls = _aws_calls(tmp_path)
    assert "iam get-role " in calls, f"the role was never probed at all:\n{calls}"
    assert "iam create-role" not in calls, (
        "a DENIED iam:GetRole was treated as evidence the role does not exist, "
        "and role creation was attempted. AccessDenied is not absence — that "
        "conflation is alpha-engine-config-I9045, which made four workflows red "
        f"for a week.\ncalls:\n{calls}\nstderr:\n{proc.stderr}"
    )
    assert "UNKNOWN" in proc.stderr, (
        "the unknown-presence verdict was not reported. A probe that cannot "
        "answer must SAY it cannot answer; that message is the whole remedy "
        f"for the silent misclassification.\nstderr:\n{proc.stderr}"
    )


def test_bootstrap_still_creates_a_genuinely_absent_role(tmp_path: Path) -> None:
    """The other half. On an explicit NoSuchEntity — the only observation that
    actually proves absence — the operator bootstrap path must still create the
    role. Without this case, deleting the IAM handling outright would pass every
    other test in this file."""
    proc = _deploy(tmp_path, "--bootstrap", get_role=(255, _NO_SUCH_ENTITY))
    calls = _aws_calls(tmp_path)
    assert "iam create-role" in calls, (
        "an explicit NoSuchEntity did not lead to role creation on the operator "
        f"bootstrap path.\ncalls:\n{calls}\nstdout:\n{proc.stdout}\n{proc.stderr}"
    )
    assert "iam put-role-policy" in calls, (
        f"the inline policy was never applied after creating the role:\n{calls}"
    )


@pytest.mark.parametrize(
    "helper",
    ["deploy_run.sh", "run_handler_tests.sh", "apply_iam_policy.sh"],
)
def test_no_private_dialect_remains(helper: str) -> None:
    """A cheap companion to the executed cases above: the script must SOURCE
    each shared helper rather than reimplement it. Text-level on purpose — this
    one asserts the absence of a second implementation, which no execution can
    observe. Comments are stripped first: a scan that reads a rationale comment
    as code is a shape this fleet has already shipped twice."""
    body = "\n".join(
        line.split("#", 1)[0] for line in _DEPLOY.read_text().splitlines()
    )
    assert f"_shared/{helper}" in body, (
        f"thinktank-spot-dispatcher/deploy.sh no longer sources _shared/{helper}. "
        "Every mechanism here is the shared one so that the next fix to it lands "
        "here too — which is exactly what did NOT happen for the IAM probe "
        "(alpha-engine-config-I9114)."
    )
