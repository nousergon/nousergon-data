"""`alpha-engine-config-I10973` — no AWS account id or S3 bucket literal in any
workflow's EXECUTABLE lines.

This repository is public and its run logs are public. `data-gate.yml` used to
hardcode `arn:aws:iam::<account>:role/...` and the S3 bucket name; the fix
adopted crucible's shape (`alpha-engine-config-I10156`): repository
VARIABLES (`vars.AWS_ACCOUNT_ID`, `vars.DATA_STORE_URI`) plus an
`::add-mask::` step ahead of the credentials step, because GitHub masks
SECRETS and does NOT mask repository variables.

`tests/test_data_report.py::test_the_workflow_carries_no_account_id_or_bucket_literal`
covered one file. This is the class-level guard the I10973 sweep commissioned:
every workflow in this directory, with an explicit, rationale-carrying
allowlist for any hit deliberately left in place (comment lines are already
exempt — a comment is never executed, so it cannot leak into a run log; this
guard checks CODE lines only).
"""

from __future__ import annotations

import pathlib

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = REPO / ".github" / "workflows"

LITERALS = ("711398986525", "alpha-engine-research")

# path -> {literal: rationale}. A hit not covered here (and not on a comment
# line) fails the test. Keep this small and specific — a blanket allowlist
# defeats the guard it was written for.
ALLOWLIST: dict[str, dict[str, str]] = {
    "deploy-eval-judge-spot-dispatcher.yml": {
        "alpha-engine-research": (
            "the Lambda FUNCTION NAME "
            "'alpha-engine-research-eval-judge-spot-dispatcher' shares the "
            "bucket's name as a naming-convention coincidence, not an S3 "
            "bucket reference — it names no infrastructure location and is "
            "not the class this guard defends against."
        ),
    },
}


def _code_lines(path: pathlib.Path) -> list[str]:
    """Non-comment lines only. A comment is never executed, so it cannot leak
    into a public run log — the leak vector this guard defends against."""
    return [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    ]


def _workflow_files() -> list[pathlib.Path]:
    return sorted(WORKFLOWS_DIR.glob("*.yml"))


@pytest.mark.parametrize("path", _workflow_files(), ids=lambda p: p.name)
def test_no_account_id_or_bucket_literal_on_a_code_line(path: pathlib.Path):
    allowed = ALLOWLIST.get(path.name, {})
    body = "\n".join(_code_lines(path))
    for literal in LITERALS:
        if literal in body and literal not in allowed:
            pytest.fail(
                f"{path.name} carries the literal {literal!r} on an executable "
                "line. This repo is public and its run logs are public — use "
                "a repository variable (vars.AWS_ACCOUNT_ID / "
                "vars.DATA_STORE_URI) instead, or add an explicit, "
                "rationale-carrying ALLOWLIST entry in this test if the hit "
                "is not actually infrastructure-identifying "
                "(alpha-engine-config-I10973)."
            )


def test_the_allowlist_carries_no_stale_entries():
    """An allowlist entry for a literal the file no longer contains is dead
    weight that would silently stop covering the next real hit in that slot."""
    for filename, literals in ALLOWLIST.items():
        path = WORKFLOWS_DIR / filename
        assert path.exists(), f"allowlisted file {filename!r} no longer exists"
        body = "\n".join(_code_lines(path))
        for literal in literals:
            assert literal in body, (
                f"{filename}: allowlisted literal {literal!r} is no longer "
                "present on a code line — remove the stale allowlist entry"
            )
