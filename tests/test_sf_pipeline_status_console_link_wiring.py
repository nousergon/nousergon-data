"""Pins the console deep-link in the terminal (fail/complete) SNS notifications
of the three producer Step Functions (config#856).

Pipeline reporting revamp shape: *pull-for-state console page + push-on-
transition emails*. The push-on-transition notifications (DAG complete / DAG
fail) must deep-link to the pull-for-state console page
(``https://console.nousergon.ai/pipeline-status`` — the ``pipeline-status``
slug pinned on the dashboard's ``25_Pipeline_Status.py``) so the operator can
jump from the terminal email to the full per-state render, instead of the old
generic "Check dashboard" / bare ``JsonToString($.error)`` text.

This is a wiring pin, not a soak-gated content-email change: these terminal
notifications are the emails the revamp KEEPS (it only drops the per-step
*content* emails), so pinning their console link is safe to land ahead of the
content-email holdback soak.

Guards, per SF:
- every terminal SNS ``publish`` state (``End: true`` success notifies +
  ``HandleFailure``) carries the console URL in its Message;
- the URL host + slug are exactly the canonical console page
  (a slug drift on either side silently breaks the deep-link).
"""

import json
import pathlib
import unittest

_INFRA = pathlib.Path(__file__).resolve().parent.parent / "infrastructure"

# Must match krepis.console (host) + the dashboard 25_Pipeline_Status.py url_path
# (slug). If either moves, this pin and the dashboard slug-drift test both fail.
CONSOLE_PIPELINE_URL = "https://console.nousergon.ai/pipeline-status"

# The three producer pipelines the console page renders (weekly / weekday / EOD).
_TEMPLATES = [
    "step_function.json",
    "step_function_daily.json",
    "step_function_eod.json",
]


def _message_of(state: dict) -> str:
    p = state.get("Parameters", {})
    return p.get("Message.$") or p.get("Message") or ""


def _terminal_notify_states(sf: dict) -> dict[str, dict]:
    """SNS-publish states that are a genuine DAG fail/complete transition — the
    two the revamp keeps and deep-links.

    Includes ``HandleFailure`` and any success-complete notify (Subject carries
    ``SUCCESS``). Deliberately EXCLUDES informational mid-DAG transitions that
    have nothing to render on the console — a market-holiday skip ("nothing
    ran") or a gate-failed-but-proceeding warning.
    """
    out = {}
    for name, st in sf["States"].items():
        if st.get("Resource") != "arn:aws:states:::sns:publish":
            continue
        subject = st.get("Parameters", {}).get("Subject", "") or st.get(
            "Parameters", {}
        ).get("Subject.$", "")
        if name == "HandleFailure" or "SUCCESS" in subject:
            out[name] = st
    return out


class PipelineStatusConsoleLinkWiringTest(unittest.TestCase):
    def _load(self, fname: str) -> dict:
        return json.loads((_INFRA / fname).read_text())

    def test_each_template_has_terminal_notify_states(self):
        for fname in _TEMPLATES:
            sf = self._load(fname)
            self.assertTrue(
                _terminal_notify_states(sf),
                f"{fname}: expected at least one terminal SNS-publish state",
            )

    def test_terminal_notifications_deeplink_to_console(self):
        for fname in _TEMPLATES:
            sf = self._load(fname)
            for name, st in _terminal_notify_states(sf).items():
                msg = _message_of(st)
                self.assertIn(
                    CONSOLE_PIPELINE_URL,
                    msg,
                    f"{fname}:{name} terminal notification must deep-link to "
                    f"the console pipeline-status page (config#856). Got: {msg!r}",
                )

    def test_no_terminal_notify_uses_bare_check_dashboard(self):
        # The old generic "Check dashboard" text is what the revamp replaces —
        # a regression to it means the console deep-link was dropped.
        for fname in _TEMPLATES:
            sf = self._load(fname)
            for name, st in _terminal_notify_states(sf).items():
                msg = _message_of(st)
                self.assertNotIn(
                    "Check dashboard for results",
                    msg,
                    f"{fname}:{name} reverted to the pre-revamp generic text",
                )

    def test_templates_are_valid_json(self):
        # The URL was spliced into raw template strings — guard that every
        # template still parses (a stray quote would break the deploy).
        for fname in _TEMPLATES:
            self.assertIsInstance(self._load(fname)["States"], dict)


if __name__ == "__main__":
    unittest.main(verbosity=2)
