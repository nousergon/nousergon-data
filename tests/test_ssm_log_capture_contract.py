"""Tests for the SF-definition ssm_log_capture contract lint
(``validators/ssm_log_capture_contract.py``, config#4223).

The lint pins the invocation shape the krepis CLI ACTUALLY enforces
(argparse-verified 2026-07-31), so a malformed ``ssm_log_capture``
invocation in a Step Function definition is caught at definition-edit
time instead of killing the pipeline mid-execution (the 2026-07-25
weekly-SF incident: 4 re-run attempts to restore the flag shape).

Canonical (conformant)::

    ssm_log_capture run --correlation-id <value> --slug S --log L -- <inner...>

``--correlation-id`` is a VALUE-TAKING option that must follow the ``run``
subcommand; its absence is NOT a violation (the module auto-generates an id
with a WARNING since config#4223).
"""

from __future__ import annotations

import json

from validators import ssm_log_capture_contract as lint


def _definition(commands_string: str) -> dict:
    """A minimal SF definition whose one Task state carries ``commands.$``."""
    return {
        "States": {
            "SpotWork": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:ssm:sendCommand",
                "Parameters": {
                    "commands.$": commands_string,
                },
            }
        }
    }


def _array(*elements: str) -> str:
    quoted = ", ".join(repr(e) for e in elements)
    return f"States.Array({quoted})"


_CONFORMANT = (
    "python3 -m krepis.ssm_log_capture run --correlation-id {} --slug "
    "morning-enrich --log /var/log/morning-enrich.log -- bash "
    "infrastructure/spot_data_weekly.sh --morning-enrich-only{}"
)


def _violations(commands_string: str) -> list[str]:
    return lint.validate_definition(_definition(commands_string))


class TestConformantShapes:
    def test_canonical_invocation_clean(self):
        assert _violations(_array(_CONFORMANT)) == []

    def test_correlation_id_absence_is_not_a_violation(self):
        # config#4223: the module auto-generates a correlation id with a
        # WARNING — absence is the module's own degraded posture, not a
        # definition defect.
        no_cid = (
            "python3 -m krepis.ssm_log_capture run --slug morning-enrich "
            "--log /var/log/morning-enrich.log -- true"
        )
        assert _violations(_array(no_cid)) == []

    def test_equals_form_accepted(self):
        eq = (
            "krepis.ssm_log_capture run --correlation-id=sf-123 --slug x "
            "--log /var/log/x.log -- true"
        )
        assert _violations(_array(eq)) == []

    def test_step_name_and_bucket_allowed(self):
        full = (
            "krepis.ssm_log_capture run --correlation-id cid --slug x "
            "--log /var/log/x.log --step-name my-step --bucket other-bucket "
            "-- true"
        )
        assert _violations(_array(full)) == []

    def test_multiple_invocations_all_checked(self):
        cmd = _array(_CONFORMANT, "krepis.ssm_log_capture run --slug y --log /var/log/y.log -- true")
        assert _violations(cmd) == []


class TestFailureShapes:
    """Each shape below is a hard argparse error on the live krepis CLI —
    pinned by running ``python -m krepis.ssm_log_capture`` (config#4223)."""

    def test_flag_before_run_subcommand(self):
        # `--correlation-id <v> run ...` → `invalid choice: '<v>' (choose from run)`
        bad = (
            "krepis.ssm_log_capture --correlation-id sf-1 run --slug x "
            "--log /var/log/x.log -- true"
        )
        vs = _violations(_array(bad))
        assert any("BEFORE the `run` subcommand" in v for v in vs)

    def test_valueless_flag(self):
        # `run --correlation-id --slug ...` → `expected one argument`
        bad = (
            "krepis.ssm_log_capture run --correlation-id --slug x "
            "--log /var/log/x.log -- true"
        )
        vs = _violations(_array(bad))
        assert any("has no value" in v for v in vs)

    def test_missing_slug(self):
        bad = (
            "krepis.ssm_log_capture run --correlation-id cid "
            "--log /var/log/x.log -- true"
        )
        vs = _violations(_array(bad))
        assert any("--slug" in v for v in vs)

    def test_missing_log(self):
        bad = (
            "krepis.ssm_log_capture run --correlation-id cid --slug x -- true"
        )
        vs = _violations(_array(bad))
        assert any("--log" in v for v in vs)

    def test_missing_inner_command_separator(self):
        bad = "krepis.ssm_log_capture run --correlation-id cid --slug x --log /var/log/x.log"
        vs = _violations(_array(bad))
        assert any("`--`" in v for v in vs)

    def test_empty_inner_command(self):
        bad = (
            "krepis.ssm_log_capture run --correlation-id cid --slug x "
            "--log /var/log/x.log --"
        )
        vs = _violations(_array(bad))
        assert any("empty inner command" in v for v in vs)

    def test_missing_run_subcommand(self):
        bad = "krepis.ssm_log_capture --slug x --log /var/log/x.log -- true"
        vs = _violations(_array(bad))
        assert any("`run` subcommand" in v for v in vs)

    def test_valueless_slug(self):
        bad = (
            "krepis.ssm_log_capture run --correlation-id cid --slug "
            "--log /var/log/x.log -- true"
        )
        vs = _violations(_array(bad))
        assert any("`--slug` has no value" in v for v in vs)


class TestExtraction:
    def test_double_quoted_elements(self):
        cmd = (
            'States.Array("set -o pipefail", "krepis.ssm_log_capture run '
            '--correlation-id cid --slug x --log /var/log/x.log -- true")'
        )
        assert _violations(cmd) == []

    def test_state_path_named_in_violations(self):
        vs = _violations(_array(
            "krepis.ssm_log_capture --correlation-id cid run --slug x "
            "--log /var/log/x.log -- true"
        ))
        assert any("States.SpotWork" in v for v in vs)

    def test_live_weekly_definition_passes(self):
        # The real weekly SF definition (2026-07-31) must stay clean — this
        # is the regression guard for the actual pipeline.
        live = json.loads(
            (
                __import__("pathlib").Path(__file__).resolve().parents[1]
                / "infrastructure"
                / "step_function.json"
            ).read_text(encoding="utf-8")
        )
        assert lint.validate_definition(live, source="step_function.json") == []
