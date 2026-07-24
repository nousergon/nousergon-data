"""Pins the IAM drift checker's unordered-set normalization (2026-07-22).

AWS GetRole returned a trust document's Principal.Service as
["scheduler.amazonaws.com","events.amazonaws.com"] — the reverse of the
codified snapshot's order. Same set, but _canonical_json compared arrays
order-sensitively, so the drift check went red and blocked every PR in the
repo. IAM policy-document string arrays (Action / Resource /
Principal.Service / ...) are UNORDERED sets per the policy grammar; the
comparator must treat them as such. Statement arrays (lists of objects)
deliberately KEEP their order — the codified file controls reader-facing
ordering even though IAM ORs statements.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "check_drift",
    Path(__file__).resolve().parent.parent / "infrastructure" / "iam" / "check-drift.py",
)
check_drift = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(check_drift)


def test_service_principal_order_is_insignificant():
    """The exact 2026-07-22 live finding: reversed Principal.Service order
    must compare equal."""
    codified = {"Statement": [{"Action": "sts:AssumeRole", "Effect": "Allow",
                               "Principal": {"Service": ["events.amazonaws.com",
                                                         "scheduler.amazonaws.com"]}}],
                "Version": "2012-10-17"}
    live = {"Statement": [{"Action": "sts:AssumeRole", "Effect": "Allow",
                           "Principal": {"Service": ["scheduler.amazonaws.com",
                                                     "events.amazonaws.com"]}}],
            "Version": "2012-10-17"}
    assert check_drift._canonical_json(codified) == check_drift._canonical_json(live)


def test_action_list_order_is_insignificant():
    a = {"Statement": [{"Action": ["s3:GetObject", "s3:PutObject"]}]}
    b = {"Statement": [{"Action": ["s3:PutObject", "s3:GetObject"]}]}
    assert check_drift._canonical_json(a) == check_drift._canonical_json(b)


def test_set_membership_differences_still_detected():
    """Sorting must not mask REAL drift — a different set is still drift."""
    a = {"Statement": [{"Action": ["s3:GetObject"]}]}
    b = {"Statement": [{"Action": ["s3:GetObject", "s3:PutObject"]}]}
    assert check_drift._canonical_json(a) != check_drift._canonical_json(b)


def test_statement_object_order_still_significant():
    a = {"Statement": [{"Sid": "A"}, {"Sid": "B"}]}
    b = {"Statement": [{"Sid": "B"}, {"Sid": "A"}]}
    assert check_drift._canonical_json(a) != check_drift._canonical_json(b)
