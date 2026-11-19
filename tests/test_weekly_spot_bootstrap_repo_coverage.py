"""The weekly spot bootstrap must clone every repo the SF actually uses.

Bug class this guards (found by the 2026-07-27 post-#975 rehearsal): the
per-execution launcher box's bootstrap cloned four repos —
``alpha-engine-{data,config,backtester,dashboard}`` — while the weekly SF's
command strings ``cd`` into **five**. ``alpha-engine-predictor`` was never
cloned, so `PredictorTraining` died at launch:

    fatal: cannot change to '/home/ec2-user/alpha-engine-predictor':
    No such file or directory
    failed to run commands: exit status 128

That took out the whole predictor arm — `PredictorTraining`, `ResolveZooSpecs`,
`TrainSpecDispatch`, `ModelZooSelect`, and therefore the live-champion
promotion.

It was invisible before #975 because the long-lived dashboard box had all five
repos provisioned by hand years of runs ago. Only a box built from scratch
exposes the gap — which is exactly why the rehearsal exists, and why pinning
the invariant in CI matters more than fixing the one missing clone.

The assertion is derived from BOTH sides — the SF definition's own command
strings and the dispatcher's bootstrap script — so adding a stage that uses a
sixth repo fails here rather than at 02:00 on a Saturday.
"""

from __future__ import annotations

import json
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SF_JSON = _REPO_ROOT / "infrastructure" / "step_function.json"
_DISPATCHER = (
    _REPO_ROOT
    / "infrastructure"
    / "lambdas"
    / "weekly-freshness-spot-dispatcher"
    / "index.py"
)

# Checkout paths under /home/ec2-user that the bootstrap creates.
_CHECKOUT_RE = re.compile(r"/home/ec2-user/(alpha-engine-[a-z-]+)")


def _repos_the_sf_uses() -> set[str]:
    """Every alpha-engine-* checkout path referenced by an SF command string."""
    doc = json.loads(_SF_JSON.read_text(encoding="utf-8"))
    found: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for key, val in node.items():
                if key == "commands.$" and isinstance(val, str):
                    found.update(_CHECKOUT_RE.findall(val))
                else:
                    walk(val)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(doc)
    return found


def _bootstrap_script() -> str:
    """The command this dispatcher actually sends.

    Since alpha-engine-config-I7372 most of it is rendered by
    ``krepis.spot_bootstrap`` rather than written in this file, so reading the
    SOURCE for clone lines finds only the tail's private-repo clone — which is
    how this test read three of the five repos as missing on the cutover PR.
    Import the handler and render, exactly as the Lambda does: the renderer is
    a pure function of its spec, so this needs no AWS and no stubbing.
    """
    spec = importlib.util.spec_from_file_location(
        "_weekly_freshness_index_for_test", _DISPATCHER
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._bootstrap_command("test-run-token")


def _repos_the_bootstrap_clones() -> set[str]:
    """Every checkout path the bootstrap git-clones, rendered parts included."""
    # Line continuations first: a clone whose destination sits on its own
    # continuation line (the private-config one does) is invisible to a
    # line-anchored match.
    script = _bootstrap_script().replace("\\\n", " ")
    return {
        m.group(1)
        for m in re.finditer(
            r"git clone[^\n]*?/home/ec2-user/(alpha-engine-[a-z-]+)(?:\s|$)", script
        )
    }


def _krepis_floor_assertion() -> str:
    """Extract the self-contained Python assertion from the rendered shell."""
    match = re.search(
        r"\.venv/bin/python - requirements\.txt <<'PY'.*?\n(.*?)\nPY\n",
        _bootstrap_script(),
        re.S,
    )
    assert match, "bootstrap has no dashboard krepis floor assertion"
    return match.group(1)


def _run_krepis_floor_assertion(tmp_path: Path, installed: str | None) -> subprocess.CompletedProcess[str]:
    """Run the assertion with isolated package metadata and no network."""
    packages = tmp_path / "packages"
    packages.mkdir()
    if installed is not None:
        dist_info = packages / f"krepis-{installed}.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: krepis\nVersion: {installed}\n",
            encoding="utf-8",
        )
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("krepis==0.19.0\n", encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-S", "-", str(requirements)],
        input=_krepis_floor_assertion(),
        text=True,
        capture_output=True,
        env={"PYTHONPATH": str(packages)},
        check=False,
    )


def test_krepis_floor_assertion_accepts_a_satisfying_install(tmp_path: Path) -> None:
    result = _run_krepis_floor_assertion(tmp_path, "0.18.8")
    assert result.returncode == 0, result.stderr


def test_krepis_floor_assertion_fails_loudly_for_old_or_missing_install(
    tmp_path: Path,
) -> None:
    for installed, reported in (("0.18.7", "0.18.7"), (None, "<absent>")):
        case_path = tmp_path / (installed or "missing")
        case_path.mkdir()
        result = _run_krepis_floor_assertion(case_path, installed)
        assert result.returncode != 0
        assert (
            f"installed krepis={reported}; required floor=0.18.8; "
            "alpha-engine-dashboard/requirements.txt pin=0.19.0; "
            "the dashboard venv is built per-boot from that file"
        ) in result.stderr


def test_sf_commands_do_not_install_packages_at_runtime() -> None:
    """The floor check must not revive per-state runtime pip installs."""
    for name in ("step_function.json", "step_function_eod.json"):
        definition = json.loads((_REPO_ROOT / "infrastructure" / name).read_text())
        commands: list[str] = []

        def walk(node: object) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in {"commands", "commands.$"}:
                        if isinstance(value, str):
                            commands.append(value)
                        elif isinstance(value, list):
                            commands.extend(str(command) for command in value)
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(definition)
        rendered = "\n".join(commands)
        assert not re.search(r"(?:^|\s)(?:python\s+-m\s+)?pip\s+install(?:\s|$)", rendered), name


def test_sf_references_at_least_one_repo() -> None:
    """Guard the guard — a broken walker must not silently pass."""
    assert _repos_the_sf_uses(), "no alpha-engine-* checkout paths found in the SF"


def test_bootstrap_clones_every_repo_the_sf_uses() -> None:
    used = _repos_the_sf_uses()
    cloned = _repos_the_bootstrap_clones()
    missing = used - cloned
    assert not missing, (
        f"the weekly SF cds into {sorted(missing)} but the launcher-box bootstrap "
        f"never clones {'it' if len(missing) == 1 else 'them'}. Every stage using "
        f"{'that repo' if len(missing) == 1 else 'those repos'} will die with "
        f"\"cannot change to ...: No such file or directory\" (exit 128) on a "
        f"freshly-launched box. Add the clone (and the chown) in "
        f"infrastructure/lambdas/weekly-freshness-spot-dispatcher/index.py. "
        f"SF uses {sorted(used)}; bootstrap clones {sorted(cloned)}."
    )


def test_every_cloned_repo_is_chowned_to_ec2_user() -> None:
    """A clone the box cannot write to is as broken as a missing one.

    The bootstrap runs as root; every stage runs as ec2-user and does a
    ``git pull``, which needs write access to the checkout.
    """
    chown_block = re.search(
        r"chown -R ec2-user:ec2-user(.*?)\|\| fail", _bootstrap_script(), re.S
    )
    assert chown_block, "bootstrap has no chown block"
    chowned = set(_CHECKOUT_RE.findall(chown_block.group(1)))
    missing = _repos_the_bootstrap_clones() - chowned
    assert not missing, (
        f"{sorted(missing)} cloned but never chowned to ec2-user — stages run as "
        f"ec2-user and `git pull` needs write access"
    )
