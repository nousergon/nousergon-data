"""constraints/pyarrow-numpy.toml is the one declaration of the pyarrow/numpy pair
(alpha-engine-config-I13002; mechanism record nous-ergon-ops#2041).

pyarrow >= 26 refuses to import under NumPy 1.x. In October the pair broke the
fleet three times, each time through a surface the previous fix had not
touched (data requirements.txt 10-09, a Dependabot bump of it 10-16,
crucible-dashboard's lock under the weekly-freshness box's ``numpy<2`` 10-21).

What these tests hold, on every PR to this repo:

- every surface in this repo agrees with the declaration (so a Dependabot
  pyarrow bump of requirements.txt is red here, not on a box);
- every file in this repo that declares or pip-installs pyarrow/numpy is
  registered, so a new surface cannot appear beside the declaration;
- the declaration itself never pairs a ``numpy<2`` venv with a pyarrow that
  admits >= 26;
- the checker actually fails on each shape that broke the fleet.

The other repos' registered surfaces are checked by
``python3 scripts/check_pyarrow_numpy_constraint.py --remote``; that needs the
network, so it is not part of this gate.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import check_pyarrow_numpy_constraint as c  # noqa: E402
from packaging.specifiers import SpecifierSet  # noqa: E402


def _venv(pyarrow: str, below_2: bool = False, name: str = "v") -> c.Venv:
    return c.Venv(name, SpecifierSet(pyarrow), below_2)


def _surface(kind: str, path: str = "requirements.txt", **kw) -> c.Surface:
    return c.Surface(venv="v", repo=c.THIS_REPO, path=path, kind=kind, **kw)


# --- this repo, as it stands -------------------------------------------------


def test_every_local_surface_agrees_with_the_declaration():
    findings = [f for f in c.run(remote=False)]
    assert findings == [], "\n".join(findings)


def test_every_local_pyarrow_numpy_surface_is_registered():
    missing = c.unregistered_local_surfaces()
    assert missing == [], (
        "these files declare or pip-install pyarrow/numpy but are not registered in "
        f"constraints/pyarrow-numpy.toml: {missing}"
    )


def test_declaration_never_pairs_numpy_below_2_with_pyarrow_26():
    threshold, venvs, _ = c.load()
    assert threshold == "26"
    for venv in venvs.values():
        assert c.check_venv_rule(venv, threshold) == []


def test_every_registered_venv_has_a_requirements_surface_and_known_kinds():
    _, venvs, surfaces = c.load()
    for name in venvs:
        kinds = {s.kind for s in surfaces if s.venv == name}
        assert "requirements" in kinds, f"venv {name} has no requirements surface"
    assert {s.kind for s in surfaces} <= set(c.KINDS)
    assert {s.venv for s in surfaces} <= set(venvs)


# --- the checker fails on each shape that broke the fleet --------------------


def test_dependabot_pyarrow_bump_of_requirements_is_refused():
    # 10-16 (data#2165): the surface moved, the declaration did not.
    text = "pandas>=2.0\npyarrow>=27\nnumpy>=2.4.6\n"
    out = c.check_surface(_surface("requirements"), text, _venv(">=26"), "26")
    assert any("disagrees with the declared" in f for f in out), out


def test_numpy_below_2_override_under_pyarrow_26_is_refused():
    # 10-09: requirements resolved pyarrow 26, then the box forced numpy<2.
    text = "pip install -q -r requirements.txt\npip install -q 'numpy<2' || fail x\n"
    out = c.check_surface(_surface("bootstrap", "x.sh"), text, _venv(">=26"), "26")
    assert any("pip-installs numpy<2" in f for f in out), out
    shell_var = '\\$PIP install -q "numpy<2"\n'
    assert c.forces_numpy_below_2(shell_var)


def test_lock_relocked_past_the_cap_is_refused():
    # 10-21 (crucible-dashboard#952): the lock moved to 26 under a numpy<2 venv.
    text = "numpy==2.5.4\npyarrow==26.0.0\n    # via pandas\n"
    out = c.check_surface(_surface("lock"), text, _venv(">=25.0.1,<26", True), "26")
    assert any("outside the declared" in f for f in out), out


def test_numpy_below_2_venv_without_a_pyarrow_cap_is_refused():
    assert c.check_venv_rule(_venv(">=25.0.1", True), "26")
    assert not c.check_venv_rule(_venv(">=25.0.1,<26", True), "26")


def test_requirements_numpy_1_beside_pyarrow_26_is_refused():
    text = "pyarrow>=26\nnumpy<2\n"
    out = c.check_surface(_surface("requirements"), text, _venv(">=26"), "26")
    assert any("admits NumPy 1.x" in f for f in out), out


def test_declared_override_that_disappeared_is_reported():
    out = c.check_surface(_surface("bootstrap", "x.sh"), "pip install -r r.txt\n", _venv("<26", True), "26")
    assert any("no longer forces it" in f for f in out), out


def test_comments_and_inline_comments_are_not_surfaces():
    text = "# pip install 'numpy<2' (history)\npyarrow>=25.0.1  # floor note\n"
    assert not c.forces_numpy_below_2(text)
    assert c.requirement_specs("requirements.txt", text)["pyarrow"] == [">=25.0.1"]


def test_pyproject_specs_are_read():
    text = 'dependencies = [\n    "numpy>=2.5.4",\n    "pyarrow>=25.0.1",\n]\n'
    specs = c.requirement_specs("pyproject.toml", text)
    assert specs == {"pyarrow": [">=25.0.1"], "numpy": [">=2.5.4"]}


@pytest.mark.parametrize(
    ("ignore_yaml", "declared", "ok"),
    [
        ("", (), True),
        ('    ignore:\n      - dependency-name: pyarrow\n        versions: [">=26"]\n', (), False),
        ('    ignore:\n      - dependency-name: pyarrow\n        versions: [">=26"]\n', (">=26",), True),
    ],
)
def test_dependabot_ignore_must_match_the_declaration(ignore_yaml, declared, ok):
    text = "version: 2\nupdates:\n  - package-ecosystem: pip\n    directory: /\n" + ignore_yaml
    out = c.check_surface(
        _surface("dependabot", ".github/dependabot.yml", pyarrow_ignore=declared), text, _venv(">=26"), "26"
    )
    assert (out == []) is ok, out


def test_dependabot_pair_group_must_come_first():
    text = (
        "version: 2\nupdates:\n  - package-ecosystem: pip\n    directory: /\n    groups:\n"
        "      minor-and-patch:\n        update-types: [minor, patch]\n"
        "      pyarrow-numpy:\n        patterns: [pyarrow, numpy]\n"
    )
    out = c.check_surface(
        _surface("dependabot", ".github/dependabot.yml", pyarrow_ignore=(), group="pyarrow-numpy"),
        text,
        _venv(">=26"),
        "26",
    )
    assert any("FIRST group" in f for f in out), out
