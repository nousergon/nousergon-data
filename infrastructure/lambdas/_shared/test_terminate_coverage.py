"""Tests for the source-derived box-launcher termination guard
(alpha-engine-config#3189).

Runs on bare python (unittest, no pytest/boto3 needed) so it can join the
_shared suite the CI gate executes as ``python3 <path>``.
"""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _shared.terminate_coverage import (  # noqa: E402
    assert_every_launcher_terminates_on_failure,
    find_box_launchers,
)

# The dispatchers verified (2026-07-23, alpha-engine-config#3189) to launch EC2
# boxes today. A launcher added or removed from the real lambdas/ tree without
# a matching edit here forces a deliberate, reviewed change to this test file
# rather than a silent pass — the "operator can't sleepwalk past it" pin.
_EXPECTED_LAUNCHERS = frozenset(
    {
        "data-spot-dispatcher",
        "scheduled-groom-dispatcher",
        "ci-watch-dispatcher",
        "sf-watch-spot-dispatcher",
        "canary-replay-dispatcher",
        "alert-drain-dispatcher",
        "arctic-migration-dispatcher",
    }
)

_LAMBDAS_ROOT = Path(__file__).resolve().parent.parent


class TerminateCoverageUnitTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def _write(self, dispatcher: str, body: str) -> None:
        d = self.root / dispatcher
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.py").write_text(body)

    def test_launcher_with_shared_terminate_helper_passes(self):
        self._write(
            "good-dispatcher",
            "from nousergon_lib import spot_dispatch\n"
            "def handler(e, c):\n"
            "    iid = spot_dispatch.launch_with_fallback(x, y)\n"
            "    try:\n"
            "        do_thing(iid)\n"
            "    except Exception:\n"
            "        spot_dispatch.terminate_on_failure(iid, region=R, label='x')\n",
        )
        assert_every_launcher_terminates_on_failure(self.root)  # no raise

    def test_launcher_with_local_terminate_instances_wrapper_passes(self):
        self._write(
            "good-dispatcher-2",
            "from nousergon_lib import ec2_spot\n"
            "def handler(e, c):\n"
            "    iid = ec2_spot.launch(x, y)\n"
            "def _terminate_instance(iid):\n"
            "    boto3.client('ec2').terminate_instances(InstanceIds=[iid])\n",
        )
        assert_every_launcher_terminates_on_failure(self.root)  # no raise

    def test_launcher_with_no_terminate_call_fails(self):
        self._write(
            "bad-dispatcher",
            "from nousergon_lib import spot_dispatch\n"
            "def handler(e, c):\n"
            "    return spot_dispatch.launch_with_fallback(x, y)\n",
        )
        with self.assertRaises(AssertionError) as ctx:
            assert_every_launcher_terminates_on_failure(self.root)
        self.assertIn("bad-dispatcher", str(ctx.exception))
        self.assertIn("alpha-engine-config#3189", str(ctx.exception))

    def test_non_launcher_is_ignored(self):
        self._write(
            "routing-only-dispatcher",
            "def handler(e, c):\n"
            "    boto3.client('lambda').invoke(FunctionName='x')\n",
        )
        launchers = find_box_launchers(self.root)
        self.assertNotIn("routing-only-dispatcher", launchers)
        assert_every_launcher_terminates_on_failure(self.root)  # no raise

    def test_nested_local_terminate_call_is_found(self):
        # The terminate call site may live inside a nested except/finally
        # block, not module scope — must still be detected (ast.walk, not
        # module.body iteration).
        self._write(
            "nested-dispatcher",
            "from nousergon_lib import ec2_spot\n"
            "def handler(e, c):\n"
            "    iid = ec2_spot.launch(x, y)\n"
            "    try:\n"
            "        do_thing(iid)\n"
            "    except Exception:\n"
            "        try:\n"
            "            _terminate_instance(iid)\n"
            "        finally:\n"
            "            pass\n"
            "def _terminate_instance(iid):\n"
            "    spot_dispatch.terminate_on_failure(iid, region=R, label='x')\n",
        )
        assert_every_launcher_terminates_on_failure(self.root)  # no raise


class TerminateCoverageLiveTest(unittest.TestCase):
    """Runs against the real infrastructure/lambdas/ tree — the actual
    chokepoint. A NEW box-launching dispatcher added without a matching entry
    here (and without terminate-on-failure coverage) fails this suite."""

    def test_known_launcher_set_is_pinned(self):
        discovered = set(find_box_launchers(_LAMBDAS_ROOT))
        self.assertEqual(
            discovered,
            set(_EXPECTED_LAUNCHERS),
            "Box-launching dispatcher set changed. If you ADDED a new "
            "launcher, give it terminate-on-failure coverage (see "
            "ci-watch-dispatcher's `_terminate_instance` pattern) and add it "
            "to _EXPECTED_LAUNCHERS above. If you REMOVED one, drop it from "
            "_EXPECTED_LAUNCHERS.",
        )

    def test_every_real_launcher_terminates_on_failure(self):
        assert_every_launcher_terminates_on_failure(_LAMBDAS_ROOT)  # no raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
