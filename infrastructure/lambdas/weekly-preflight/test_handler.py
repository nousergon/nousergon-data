"""Handler tests for the WeeklyPreflight pre-spend gate.

Why this file exists (2026-08-10): it did not, and `_shared/run_handler_tests.sh`
returns 0 when a lambda has no `test_handler.py`, so BOTH pre-merge gates —
ci.yml's glob step and deploy.sh's pytest gate — reported green on a Lambda
that could not execute a single line of its own gate logic. The first real
Saturday invocation returned
``ModuleNotFoundError: No module named 'nousergon_lib'`` and halted
ne-weekly-freshness-pipeline.

These tests stub ``sf_preflight`` in ``sys.modules``, so they pin the
handler's CONTRACT (which capability profile it asks for, how it classifies
skips, that an all-skipped run is not a pass). They deliberately cannot catch
a packaging gap — that is the job of
``tests/test_sf_preflight.py::test_lambda_profile_imports_are_packaged`` and of
deploy.sh's post-deploy smoke invoke against the real runtime.
"""

import os
import sys
import types
import unittest
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@dataclass
class _Result:
    name: str
    status: str
    message: str = ""
    details: dict = field(default_factory=dict)
    elapsed_seconds: float = 0.0


def _install_stub(results, n_fail=None, raises=None, required=None):
    """Install a stub sf_preflight module and return the recorded call kwargs.

    ``required`` is the set of bare check names (e.g. "arctic_connectivity")
    this stub's ``summarize_results`` treats as REQUIRED — i.e. a skip of one
    of them is a reportable gap (I11112's CHECK_REQUIRED). Defaults to "every
    name in results", the same safe default sf_preflight.CHECK_REQUIRED uses
    for an undeclared check, so a test that doesn't care about the
    required/optional distinction gets the conservative behaviour.
    """
    recorded = {}
    stub = types.ModuleType("sf_preflight")
    stub.LAMBDA_CAPABILITIES = frozenset({"aws"})
    stub.FULL_CAPABILITIES = frozenset({"aws", "arctic", "repo_modules", "checkout", "polygon"})
    all_names = {r.name for r in results}
    required_names = required if required is not None else all_names
    # Every result name is declared explicitly (True or False) — passing
    # required=set() means "everything here is declared OPTIONAL", not
    # "nothing is declared" (which would fall through to the safe
    # undeclared-defaults-to-True direction and defeat the test).
    stub.CHECK_REQUIRED = {f"check_{n}": (n in required_names) for n in all_names}

    def run_preflight(bucket=None, capabilities=None, run_date=None, skip_flags=None):
        recorded["bucket"] = bucket
        recorded["capabilities"] = capabilities
        # alpha-engine-config-I7443: the handler forwards the SF execution
        # input so check_skip_flag_artifact_coherence can verify each skip
        # claim before spot spend. Recorded so the forwarding is asserted,
        # not assumed.
        recorded["run_date"] = run_date
        recorded["skip_flags"] = skip_flags
        if raises is not None:
            raise raises
        fails = n_fail if n_fail is not None else sum(1 for r in results if r.status == "fail")
        return fails, results

    def summarize_results(rs):
        # Pins the CONTRACT summarize_results must satisfy, not sf_preflight's
        # own implementation — this is a stub the handler is tested against.
        dicts = [
            {"name": r.name, "status": r.status, "message": r.message,
             "details": r.details, "elapsed_seconds": r.elapsed_seconds}
            for r in rs
        ]
        fail_r = [d for d in dicts if d["status"] == "fail"]
        warn_r = [d for d in dicts if d["status"] == "warn"]
        skip_r = [d for d in dicts if d["status"] == "skip"]
        required_skip = [
            d for d in skip_r if stub.CHECK_REQUIRED.get(f"check_{d['name']}", True)
        ]
        return {
            "result_dicts": dicts,
            "fail_results": fail_r,
            "warn_results": warn_r,
            "skip_results": skip_r,
            "ran_count": len(dicts) - len(skip_r),
            "fail_count": len(fail_r),
            "warn_count": len(warn_r),
            "skip_count": len(skip_r),
            "required_skip_count": len(required_skip),
            "required_skip_names": [d["name"] for d in required_skip],
        }

    stub.run_preflight = run_preflight
    stub.summarize_results = summarize_results
    sys.modules["sf_preflight"] = stub
    return recorded


class WeeklyPreflightHandlerTests(unittest.TestCase):
    def tearDown(self):
        sys.modules.pop("sf_preflight", None)
        sys.modules.pop("index", None)

    def _handler(self):
        sys.modules.pop("index", None)
        import index
        return index.handler

    def test_requests_the_lambda_capability_profile(self):
        """The gate must NOT run the full laptop/spot profile.

        Running it is not a degraded gate: check_arctic_connectivity and
        check_tool_contracts return status="fail" in a Lambda by
        construction, so the pipeline halts however healthy the system is.
        """
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        out = self._handler()({}, None)
        self.assertEqual(recorded["capabilities"], frozenset({"aws"}))
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["has_violation"])

    def test_skips_are_not_violations(self):
        """A skip never HALTS the run — has_violation stays False whether the
        skipped check is required (I11112: it becomes DEGRADED) or optional
        (it stays OK). Neither case is a confirmed violation."""
        recorded = _install_stub([
            _Result("sf_iam_reachability", "ok"),
            _Result("arctic_connectivity", "skip", "Not run: ... arctic"),
            _Result("tool_contracts", "skip", "Not run: ... checkout"),
        ])
        out = self._handler()({}, None)
        self.assertIn(out["status"], ("OK", "DEGRADED"))
        self.assertFalse(out["has_violation"])
        self.assertEqual(out["skip_count"], 2)
        self.assertEqual(out["ran_count"], 1)
        self.assertEqual(recorded["capabilities"], frozenset({"aws"}))

    def test_required_skip_degrades_status_without_halting(self):
        """alpha-engine-config-I11112: the defect this fix closes. A run of
        5/15 checks (10 REQUIRED skips) must not report status="OK" — but
        must also not HALT, per sf-pipeline-policy.md §5's pre-spend-gate-
        probe carve-out (the probe's own coverage gap fails open, visibly)."""
        _install_stub(
            [
                _Result("sf_iam_reachability", "ok"),
                _Result("arctic_connectivity", "skip", "Not run: ... arctic"),
                _Result("constituents_fetch", "skip", "Not run: ... repo_modules"),
            ],
            required={"sf_iam_reachability", "arctic_connectivity", "constituents_fetch"},
        )
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "DEGRADED")
        self.assertFalse(out["has_violation"], "a coverage gap fails OPEN, never halts")
        self.assertTrue(out["degraded"])
        self.assertEqual(out["required_skip_count"], 2)
        self.assertEqual(
            sorted(out["required_skip_names"]),
            ["arctic_connectivity", "constituents_fetch"],
        )
        self.assertIn("stage_coverage", out, "DEGRADED still records stage coverage, like OK")

    def test_optional_skip_stays_ok(self):
        """A skip declared optional (CHECK_REQUIRED[...] = False) never
        escalates the aggregate status — only a REQUIRED skip does."""
        _install_stub(
            [
                _Result("sf_iam_reachability", "ok"),
                _Result("some_advisory_check", "skip", "Not run: ... arctic"),
            ],
            required=set(),
        )
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["degraded"])
        self.assertEqual(out["required_skip_count"], 0)

    def test_real_failure_outranks_required_skip(self):
        """A confirmed violation still hard-fails even alongside required
        skips — FAIL is strictly worse than DEGRADED, never demoted to it."""
        _install_stub(
            [
                _Result("sf_iam_reachability", "fail", "role cannot invoke"),
                _Result("arctic_connectivity", "skip", "Not run: ... arctic"),
            ],
            required={"sf_iam_reachability", "arctic_connectivity"},
        )
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "FAIL")
        self.assertTrue(out["has_violation"])
        self.assertEqual(out["required_skip_count"], 1)

    def test_all_skipped_is_an_error_not_a_pass(self):
        """Zero checks run is an unobserved gate, never a green one."""
        _install_stub([
            _Result("arctic_connectivity", "skip"),
            _Result("tool_contracts", "skip"),
        ])
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "ERROR")
        self.assertTrue(out["has_violation"])
        self.assertIn("0 checks", out["error"])

    def test_real_failure_still_halts(self):
        _install_stub([
            _Result("sf_iam_reachability", "fail", "role cannot invoke"),
            _Result("arctic_connectivity", "skip"),
        ])
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "FAIL")
        self.assertTrue(out["has_violation"])
        self.assertEqual(out["failures"], ["sf_iam_reachability"])

    def test_run_preflight_raising_is_reported_as_error(self):
        """The 2026-08-10 shape: a missing dependency in the PROLOGUE."""
        _install_stub([], raises=ModuleNotFoundError("No module named 'nousergon_lib'"))
        out = self._handler()({}, None)
        self.assertEqual(out["status"], "ERROR")
        self.assertTrue(out["has_violation"])
        self.assertIn("nousergon_lib", out["error"])

    def test_bucket_override_from_event(self):
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        self._handler()({"bucket": "some-other-bucket"}, None)
        self.assertEqual(recorded["bucket"], "some-other-bucket")


class WeeklyPreflightExecutionInputForwardingTests(unittest.TestCase):
    """alpha-engine-config-I7443.

    The SF Task invokes this Lambda with no explicit Payload, so the whole
    state input arrives as ``event`` — run_date and every skip_* flag are
    already present and were simply discarded. Forwarding them is what lets
    the pre-spend gate reject an incoherent recovery input (a skip claim
    with no artifact for that run_date) in seconds, instead of the in-SF
    guard catching it after a spot dispatch and ~18 minutes.
    """

    def tearDown(self):
        sys.modules.pop("sf_preflight", None)
        sys.modules.pop("index", None)

    def _handler(self):
        sys.modules.pop("index", None)
        import index
        return index.handler

    def test_run_date_and_skip_flags_reach_run_preflight(self):
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        event = {
            "run_date": "2026-08-16",
            "skip_predictor_training": True,
            "skip_scanner": True,
            "skip_aggregate_costs": False,
            "pipeline_role": "watch-rerun",
            "sns_topic_arn": "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
        }
        self._handler()(event, None)
        self.assertEqual(recorded["run_date"], "2026-08-16")
        self.assertEqual(
            recorded["skip_flags"],
            {
                "skip_predictor_training": True,
                "skip_scanner": True,
                "skip_aggregate_costs": False,
            },
        )

    def test_non_skip_keys_are_not_forwarded_as_skip_flags(self):
        """Only skip_* keys — pipeline_role and sns_topic_arn are not claims."""
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        self._handler()(
            {"run_date": "2026-08-16", "pipeline_role": "watch-rerun"}, None
        )
        self.assertEqual(recorded["skip_flags"], {})

    def test_bare_event_forwards_nothing_and_still_passes(self):
        """A bare {} test invoke must keep working. An absent payload is
        'nothing claimed', never a violation — this gate must not begin
        halting the pipeline over a shape it previously ignored."""
        recorded = _install_stub([_Result("sf_iam_reachability", "ok")])
        out = self._handler()({}, None)
        self.assertIsNone(recorded["run_date"])
        self.assertEqual(recorded["skip_flags"], {})
        self.assertEqual(out["status"], "OK")
        self.assertFalse(out["has_violation"])


if __name__ == "__main__":
    unittest.main()
