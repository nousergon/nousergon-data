"""Tests for the source-derived hermetic import guard (config#1746).

Runs on bare python (unittest, no pytest/boto3 needed) so it can join the
_shared suite the CI/deploy gates execute as ``python3 <path>``.
"""

import sys
import tempfile
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _shared.hermetic_import_guard import (  # noqa: E402
    assert_hermetic_imports_satisfied,
)


class HermeticImportGuardTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.pkg = Path(self._tmp.name)
        # Mirror the hermetic tests' sys.path (lambda dir + lambdas/ root).
        sys.path.insert(0, str(self.pkg))
        self._stubbed: list[str] = []

    def tearDown(self):
        for name in self._stubbed:
            sys.modules.pop(name, None)
        if str(self.pkg) in sys.path:
            sys.path.remove(str(self.pkg))
        self._tmp.cleanup()

    def _stub(self, name: str) -> None:
        sys.modules[name] = types.ModuleType(name)
        self._stubbed.append(name)

    def _write(self, name: str, body: str) -> None:
        (self.pkg / name).write_text(body)

    # A test file the guard resolves index.py relative to. Its own imports are
    # irrelevant — the guard walks the *handler*, not the caller.
    def _test_file(self) -> str:
        f = self.pkg / "test_handler.py"
        f.write_text("# hermetic test stub\n")
        return str(f)

    def test_passes_when_all_imports_stdlib_or_stubbed(self):
        self._write(
            "index.py",
            "import json\nimport os\nfrom acme_lib.thing import Widget\n",
        )
        self._stub("acme_lib")
        self._stub("acme_lib.thing")
        # Does not raise.
        assert_hermetic_imports_satisfied(self._test_file())

    def test_raises_on_unstubbed_git_only_import(self):
        self._write("index.py", "import json\nfrom acme_lib.thing import Widget\n")
        # acme_lib deliberately NOT stubbed — this is the drift.
        with self.assertRaises(AssertionError) as ctx:
            assert_hermetic_imports_satisfied(self._test_file())
        self.assertIn("acme_lib.thing", str(ctx.exception))
        self.assertIn("config#1746", str(ctx.exception))

    def test_walks_transitive_sibling_imports(self):
        # index imports a LOCAL sibling that itself pulls an unstubbed dep — the
        # 2026-07-04 class (source moved onto nousergon_lib via a sibling).
        self._write("index.py", "from sibling import thing\n")
        self._write("sibling.py", "from deep_lib.core import q\n")
        with self.assertRaises(AssertionError) as ctx:
            assert_hermetic_imports_satisfied(self._test_file())
        self.assertIn("deep_lib.core", str(ctx.exception))

    def test_transitive_sibling_import_satisfied_when_stubbed(self):
        self._write("index.py", "from sibling import thing\n")
        self._write("sibling.py", "from deep_lib.core import q\n")
        self._stub("deep_lib")
        self._stub("deep_lib.core")
        assert_hermetic_imports_satisfied(self._test_file())  # no raise

    def test_stubbed_sibling_is_not_walked(self):
        # When the test stubs a whole sibling module wholesale, the real file's
        # own imports never execute — the guard must NOT flag them (the
        # scheduled-groom-dispatcher flow_doctor_telegram pattern).
        self._write("index.py", "from sibling import thing\n")
        self._write("sibling.py", "from unstubbed_dep.x import y\n")
        self._stub("sibling")  # sibling stubbed → its imports irrelevant
        assert_hermetic_imports_satisfied(self._test_file())  # no raise

    def test_function_scope_imports_are_ignored(self):
        # Deferred (in-function) imports do not run at `import index` time.
        self._write(
            "index.py",
            "import json\n\n\ndef handler(e, c):\n    from lazy_lib import z\n    return z\n",
        )
        assert_hermetic_imports_satisfied(self._test_file())  # no raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
