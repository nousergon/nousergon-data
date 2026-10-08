"""No code or grant in this repository reaches AWS Cost Explorer.

alpha-engine-config-I12168 (Brian, 2026-10-08): the fleet makes ZERO Cost
Explorer calls, permanently. Every request bills $0.01; an unbounded caller
burned $441.67 in four days (I10389), and AWS Support processes that credit
only once the account's Cost Explorer calls have stopped. AWS spend is read
from the CUR 2.0 billing export instead (the expense collector, and
`data_gate/producers/cost_monthly.py`).

This scans the whole tree, so a NEW caller anywhere fails here, not only a
regression in the collector:

  * a boto3 Cost Explorer client, in any spelling of the call;
  * an `aws ce ...` CLI invocation in a script or workflow;
  * an IAM Allow of a `ce:` action in any JSON/YAML policy document.

Prose that NAMES Cost Explorer is fine and common (it is why the export is
used); only constructions and grants are matched.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "__pycache__", ".pytest_cache"}
THIS = Path(__file__).resolve()

_CLIENT = re.compile(r"""(?:client|resource)\s*\(\s*(?:service_name\s*=\s*)?["']ce["']""")
_CLI = re.compile(r"(?:^|[\s;|&(`$])aws\s+ce\s+[a-z]")
_GRANT = re.compile(r"""["']ce:[A-Za-z*?]""")


def _files(*suffixes: str):
    for path in ROOT.rglob("*"):
        if path.suffix not in suffixes or not path.is_file():
            continue
        if SKIP_DIRS & set(path.relative_to(ROOT).parts) or path.resolve() == THIS:
            continue
        yield path


def _code_lines(path: Path):
    for n, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue  # a comment describing the old call is not a call
        yield n, line


def test_no_cost_explorer_client_is_constructed():
    offenders = [f"{p.relative_to(ROOT)}:{n}"
                 for p in _files(".py") for n, line in _code_lines(p)
                 if _CLIENT.search(line)]
    assert offenders == [], offenders


def test_no_script_or_workflow_calls_the_cost_explorer_cli():
    offenders = [f"{p.relative_to(ROOT)}:{n}"
                 for p in _files(".sh", ".yml", ".yaml", ".py") for n, line in _code_lines(p)
                 if _CLI.search(line)]
    assert offenders == [], offenders


def _statements(doc):
    if isinstance(doc, dict):
        if "Effect" in doc and ("Action" in doc or "NotAction" in doc):
            yield doc
        for v in doc.values():
            yield from _statements(v)
    elif isinstance(doc, list):
        for v in doc:
            yield from _statements(v)


def test_no_policy_document_allows_a_cost_explorer_action():
    offenders = []
    for path in _files(".json"):
        try:
            doc = json.loads(path.read_text())
        except (ValueError, UnicodeDecodeError):
            continue
        for st in _statements(doc):
            if st.get("Effect") != "Allow":
                continue
            actions = st.get("Action") or []
            actions = [actions] if isinstance(actions, str) else actions
            offenders += [f"{path.relative_to(ROOT)}: {a}" for a in actions
                          if a.lower().startswith("ce:")]
    # CloudFormation/SAM templates: any `ce:` action string inside a policy.
    for path in _files(".yml", ".yaml"):
        for n, line in _code_lines(path):
            if _GRANT.search(line) or re.search(r"^\s*-\s*ce:[A-Za-z*]", line):
                offenders.append(f"{path.relative_to(ROOT)}:{n}")
    assert offenders == [], offenders


def test_the_guard_matches_what_it_claims_to():
    """A guard never shown to fire is a comment. Each pattern on a positive."""
    assert _CLIENT.search('ce = boto3.client("ce", region_name="us-east-1")')
    assert _CLIENT.search("boto3.client(service_name='ce')")
    assert not _CLIENT.search('boto3.client("ecs")')
    assert _CLI.search("aws ce get-cost-and-usage --time-period Start=x")
    assert not _CLI.search("aws cloudwatch get-metric-data")
    assert _GRANT.search('Action: "ce:GetCostAndUsage"')
