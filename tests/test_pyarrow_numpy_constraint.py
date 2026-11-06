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
``python3 scripts/check_pyarrow_numpy_constraint.py --remote`` (daily, from
alpha-engine-config) and, on each of those repos' own pull requests, by
``--repo <repo> --root <checkout>``. Both read other repos, so neither runs
here; the ``--repo`` mode itself is tested below against a temporary checkout.
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


def test_every_registered_venv_has_a_spec_carrying_surface_and_known_kinds():
    # A venv's pyarrow spec is carried by a requirements file, or (for a runner
    # job with no requirements file) inline by the bootstrap that installs it,
    # which the checker now compares to the declaration too.
    _, venvs, surfaces = c.load()
    for name in venvs:
        kinds = {s.kind for s in surfaces if s.venv == name}
        assert kinds & {"requirements", "bootstrap"}, f"venv {name} has no spec-carrying surface"
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


# --- uv.lock, inline bootstrap specs, and the --repo mode (other repos' PRs) --

_UV_LOCK = (
    'version = 1\n\n[[package]]\nname = "numpy"\nversion = "2.5.4"\n\n'
    '[[package]]\nname = "pyarrow"\nversion = "{v}"\nsource = {{ registry = "https://pypi.org/simple" }}\n'
)


def test_uv_lock_pin_is_read_and_a_bump_past_the_cap_is_refused():
    assert c.lock_pins("uv.lock", _UV_LOCK.format(v="25.0.1")) == ["25.0.1"]
    ok = c.check_surface(_surface("lock", "uv.lock"), _UV_LOCK.format(v="25.0.1"), _venv("~=25.0"), "26")
    assert ok == [], ok
    out = c.check_surface(_surface("lock", "uv.lock"), _UV_LOCK.format(v="26.0.0"), _venv("~=25.0"), "26")
    assert any("outside the declared" in f for f in out), out


def test_inline_pyarrow_spec_in_a_bootstrap_must_match():
    line = "          python3 -m pip install -q 'boto3>=1.39' pyyaml 'pyarrow>=17'\n"
    assert c.check_surface(_surface("bootstrap", "w.yml"), line, _venv(">=17"), "26") == []
    out = c.check_surface(_surface("bootstrap", "w.yml"), line.replace(">=17", ">=26"), _venv(">=17"), "26")
    assert any("inline" in f and "disagrees" in f for f in out), out
    assert c.inline_pyarrow_specs("pip install -r requirements.txt pyarrow-stubs\n") == []


def _other_repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return tmp_path


def _toml(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    t = tmp_path / "decl.toml"
    t.write_text(
        'schema = 1\n[rule]\npyarrow_needs_numpy2_from = "26"\n'
        '[venvs.x]\npyarrow = "~=25.0"\nnumpy_below_2 = false\n'
        '[[venvs.x.surfaces]]\nrepo = "other"\npath = "requirements.txt"\nkind = "requirements"\n'
    )
    return t


def test_repo_mode_refuses_a_dependabot_bump_on_the_other_repos_pr(tmp_path):
    decl = _toml(tmp_path / "d")
    root = _other_repo(tmp_path / "r", {"requirements.txt": "pyarrow~=25.0\nnumpy>=2\n"})
    assert c.run(remote=False, constraint_file=decl, only_repo="other", root=root) == []
    (root / "requirements.txt").write_text("pyarrow~=26.0\nnumpy>=2\n")  # the bump
    out = c.run(remote=False, constraint_file=decl, only_repo="other", root=root)
    assert any("disagrees with the declared" in f for f in out), out


def test_repo_mode_reports_unregistered_surfaces_in_the_other_repo(tmp_path):
    decl = _toml(tmp_path / "d")
    root = _other_repo(
        tmp_path / "r",
        {
            "requirements.txt": "pyarrow~=25.0\n",
            "lambda/requirements-x.txt": "pyarrow>=14\n",
            "uv.lock": _UV_LOCK.format(v="25.0.1"),
            ".github/dependabot.yml": "version: 2\nupdates:\n  - package-ecosystem: pip\n    directory: /\n",
            "requirements-numpy-only.txt": "numpy>=2.5\n",
            "requirements-numpy-1.txt": "numpy<2\n",
        },
    )
    got = set(c.unregistered_local_surfaces(decl, repo="other", root=root))
    assert got == {
        "lambda/requirements-x.txt",
        "uv.lock",
        ".github/dependabot.yml",
        "requirements-numpy-1.txt",
    }, got



@pytest.mark.parametrize("spec", ["==26.0.2", ">=28,<29", "~=27.1", ">26.5,<26.6"])
def test_a_narrow_spec_admitting_26_or_later_is_seen(spec):
    # codex c66464c8ae44aa09: none of the fixed probes sat inside these.
    assert c.admits_from(SpecifierSet(spec), "26")


def test_a_narrow_spec_below_the_major_still_does_not_admit():
    assert not c.admits_from(SpecifierSet("==25.0.2"), "26")
    assert not c.admits_from(SpecifierSet(">=17,<26"), "26")


@pytest.mark.parametrize("line", ["pip install 'numpy==1.26.4'", "pip install numpy~=1.26.0",
                                  "python -m pip install 'numpy>=1.24,<1.27'"])
def test_an_exact_numpy_1_pin_forces_numpy_below_2(line):
    # codex 190311c09401d4d6: only the literal 'numpy<2' was recognised.
    assert c.forces_numpy_below_2(line)


def test_a_numpy_spec_admitting_2_does_not_force_below_2():
    assert not c.forces_numpy_below_2("pip install 'numpy>=1.24'")


# --- libraries: compatible with every consuming venv, not equal to one ------


def _lib(pyarrow: str, numpy: str = ">=1.24", consumers=("v",)) -> c.Library:
    return c.Library("lib", "other", SpecifierSet(pyarrow), SpecifierSet(numpy), tuple(consumers))


def test_declared_libraries_are_compatible_with_every_consumer():
    _, venvs, _ = c.load()
    libs, surfaces = c.load_libraries()
    assert "nousergon-lib" in libs
    for lib in libs.values():
        assert c.check_library_rule(lib, venvs) == []
    assert {s.kind for s in surfaces} <= set(c.LIBRARY_KINDS)


@pytest.mark.parametrize(
    ("lib_spec", "venv_spec", "want"),
    [
        (">=14.0", ">=26", "contains"),
        (">=14.0", ">=25.0.1,<26", "contains"),
        (">=14.0", "~=25.0", "contains"),
        (">=25.0.1", ">=25.0.1", "contains"),
        (">=26", ">=25.0.1", "narrow"),  # too narrow: excludes 25.x the venv admits
        ("<26", ">=25.0.1", "narrow"),
        ("~=25.0", ">=17", "narrow"),
        (">=26", ">=25.0.1,<26", "disjoint"),  # disjoint: the venv cannot install it
        ("<20", "~=25.0", "disjoint"),
    ],
)
def test_library_range_relation(lib_spec, venv_spec, want):
    assert c.relation(SpecifierSet(lib_spec), SpecifierSet(venv_spec)) == want


def test_compatible_library_spans_numpy_1_and_2_consumers():
    venvs = {"data": _venv(">=26", name="data"), "dash": _venv(">=25.0.1,<26", True, "dash")}
    assert c.check_library_rule(_lib(">=14.0", consumers=("data", "dash")), venvs) == []


def test_too_narrow_library_is_refused():
    venvs = {"v": _venv(">=25.0.1")}
    out = c.check_library_rule(_lib(">=26"), venvs)
    assert len(out) == 1 and "narrower than consumer venv v" in out[0], out


def test_disjoint_library_is_refused():
    venvs = {"dash": _venv(">=25.0.1,<26", True, "dash")}
    out = c.check_library_rule(_lib(">=26", consumers=("dash",)), venvs)
    assert any("DISJOINT" in f for f in out), out


def test_library_numpy_2_floor_breaks_a_numpy_below_2_consumer():
    # R's case: pinning the library to the data venv's numpy>=2 strands dashboard.
    venvs = {"dash": _venv(">=25.0.1,<26", True, "dash"), "data": _venv(">=26", name="data")}
    out = c.check_library_rule(_lib(">=14.0", ">=2", ("dash", "data")), venvs)
    assert len(out) == 1 and "excludes every NumPy 1.x" in out[0] and "dash" in out[0], out
    out = c.check_library_rule(_lib(">=14.0", "<2", ("dash", "data")), venvs)
    assert len(out) == 1 and "excludes NumPy 2" in out[0] and "data" in out[0], out


def test_library_with_unknown_consumer_is_refused():
    assert any("not a declared venv" in f for f in c.check_library_rule(_lib(">=14"), {}))


def _lib_toml(tmp_path: Path, lib_pyarrow: str = ">=14.0") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    t = tmp_path / "decl.toml"
    t.write_text(
        'schema = 1\n[rule]\npyarrow_needs_numpy2_from = "26"\n'
        '[venvs.x]\npyarrow = "~=25.0"\nnumpy_below_2 = true\n'
        f'[libraries.lib]\nrepo = "other"\npyarrow = "{lib_pyarrow}"\nnumpy = ">=1.24"\nconsumers = ["x"]\n'
        '[[libraries.lib.surfaces]]\npath = "pyproject.toml"\nkind = "library"\n'
        '[[libraries.lib.surfaces]]\npath = "uv.lock"\nkind = "lock"\n'
    )
    return t


def test_repo_mode_checks_a_library_checkout(tmp_path):
    decl = _lib_toml(tmp_path / "d")
    pyproject = '[project.optional-dependencies]\nq = ["numpy>=1.24"]\nr = ["pyarrow>=14.0", "numpy>=1.24"]\n'
    root = _other_repo(tmp_path / "r", {"pyproject.toml": pyproject, "uv.lock": _UV_LOCK.format(v="25.0.1")})
    assert c.run(remote=False, constraint_file=decl, only_repo="other", root=root) == []
    assert c.unregistered_local_surfaces(decl, repo="other", root=root) == []
    # A tightening to one venv's numpy>=2 on the library's own PR is refused there.
    (root / "pyproject.toml").write_text(pyproject.replace("numpy>=1.24", "numpy>=2"))
    out = c.run(remote=False, constraint_file=decl, only_repo="other", root=root)
    assert any("disagrees with the declared library numpy" in f for f in out), out
    # A declaration narrowed past a consumer fails without reading any repo.
    out = c.run(remote=False, constraint_file=_lib_toml(tmp_path / "d2", ">=26"))
    assert any("DISJOINT" in f for f in out), out


# --- codex 2026-11-05/06 findings ------------------------------------------


def test_a_bare_inline_pyarrow_install_is_validated_as_unbounded():
    # codex d53b77479380316f: dropping the operator took the line out of validation.
    line = "          python3 -m pip install -q 'boto3>=1.39' pyarrow\n"
    assert c.inline_pyarrow_specs(line) == [""]
    out = c.check_surface(_surface("bootstrap", "w.yml"), line, _venv(">=17"), "26")
    assert any("(unbounded) inline" in f for f in out), out
    # an import in a later command, a stub package or an uninstall is not an install of pyarrow
    assert c.inline_pyarrow_specs("pip install 'pyarrow>=17' && python -c 'import pyarrow'\n") == [">=17"]
    assert c.inline_pyarrow_specs("pip install pyarrow-stubs\n") == []
    assert c.inline_pyarrow_specs("pip uninstall -y pyarrow\n") == []


@pytest.mark.parametrize("line", ["pip install 'numpy>=2.2,<2.3'", "pip install numpy==2.2.1",
                                  "pip install 'numpy==2.2.*'"])
def test_a_narrow_numpy_2_spec_does_not_force_below_2(line):
    # codex 3ea669b090402cfb: a 2.2.x-only spec matched no fixed probe.
    assert not c.forces_numpy_below_2(line)


def test_library_numpy_excluding_one_2x_release_is_too_narrow():
    # codex 6106b2fe5c959582: '!=2.1.0' passed every fixed NumPy 2 probe.
    venvs = {"data": _venv(">=26", name="data")}
    out = c.check_library_rule(_lib(">=14.0", ">=1.24,!=2.1.0", ("data",)), venvs)
    assert len(out) == 1 and "is too narrow" in out[0] and "2.1.0" in out[0], out
