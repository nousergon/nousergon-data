"""The data_gate runtime a Lambda zip carries is enough to run the predicate.

alpha-engine-config-I11269, measured 2026-09-29: the data-spot dispatcher and
the collection-readiness probe zips carried data_gate's three .py files and
registry.d/units/, but not data_gate/config/consumer_repos.yaml, which
descriptors.py reads while validating each unit. Every completion check raised
FileNotFoundError; the morning collection failed at VerifyRunManifests and the
first post-cutover preopen ended DegradedRun. The handler tests run against the
repo tree, where the file exists, so nothing caught it.

This test builds a package with the SAME helper both deploy.sh scripts source,
then loads the units and the predicate's descriptor cache from that package in a
fresh interpreter whose only import root is the package, the way /var/task is.
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
HELPER = REPO_ROOT / "infrastructure" / "lambdas" / "_shared" / "package_data_gate.sh"
PACKAGING_DEPLOYS = (
    REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "deploy.sh",
    REPO_ROOT / "infrastructure" / "lambdas" / "collection-readiness-probe" / "deploy.sh",
)


def _build(pkg: pathlib.Path) -> None:
    subprocess.run(
        ["bash", "-c", 'source "$1" && package_data_gate "$2" "$3"', "_",
         str(HELPER), str(pkg), str(REPO_ROOT)],
        check=True, capture_output=True, text=True,
    )


def test_units_load_from_a_package_built_by_the_shared_helper(tmp_path):
    pytest.importorskip("yaml")
    _build(tmp_path)
    probe = (
        "import data_gate.descriptors as d, data_gate.run_manifest_predicate as p\n"
        "units = d.load_units()\n"
        "assert units, 'no units loaded'\n"
        "assert pathlib.Path(d.__file__).resolve().is_relative_to(pkg), d.__file__\n"
        "assert p._unit_descriptors()\n"
        "print(len(units))\n"
    )
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    env["PYTHONPATH"] = str(tmp_path)
    result = subprocess.run(
        [sys.executable, "-I", "-c",
         f"import sys, pathlib; sys.path.insert(0, {str(tmp_path)!r}); "
         f"pkg = pathlib.Path({str(tmp_path)!r}).resolve()\n" + probe],
        cwd=tmp_path, env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    assert int(result.stdout.strip()) > 0


def test_every_packaging_deploy_uses_the_shared_helper():
    for deploy in PACKAGING_DEPLOYS:
        text = deploy.read_text(encoding="utf-8")
        assert 'source "${SCRIPT_DIR}/../_shared/package_data_gate.sh"' in text, deploy
        assert 'package_data_gate "${PKG}" "${REPO_ROOT_DIR}"' in text, deploy
        # No second, hand-kept copy list beside the helper.
        assert "data_gate/descriptors.py" not in text.split("package_data_gate.sh")[-1], deploy
