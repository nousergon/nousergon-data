"""
features/private_pack.py — Private feature-pack loading mechanism.

alpha-engine-config#1032 (sub-task of #1031, the private-edge divergence
policy): NEW alpha-bearing feature columns land through a PRIVATE pack
rather than this public repo, while ``features/feature_engineer.py`` keeps
only baseline/plumbing features. This module is the loading seam — the
mechanism, not any alpha-bearing compute (there is none here or anywhere
in this public repo).

Design (per the #1032 comment thread + this PR):

  1. **Discovery.** A private pack is a Python module discovered by
     filesystem path via the ``NOUSERGON_PRIVATE_FEATURE_PACK`` env var
     (e.g. ``/home/user/private-feature-pack/pack.py`` or a package
     ``__init__.py``). No env var set => no pack => public/baseline-only
     run. This is the SAME degrade-gracefully-when-absent convention
     ``features/feature_engineer.py::_load_feature_cfg_overrides`` already
     uses for the experiment-config resolver — absent config/pack is a
     normal, fully-supported state, never an error.

  2. **Contract.** The module at that path must expose a callable
     ``add_private_features(df: pd.DataFrame) -> pd.DataFrame`` — same
     shape contract as ``features.feature_engineer.compute_features``
     (or ``features.cross_sectional.apply_factor_zscores`` for
     cross-sectional/second-pass columns): takes the already-featured
     frame, returns it with additional columns appended. It must also
     expose ``PRIVATE_FEATURE_NAMES: list[str]`` naming exactly the
     columns it adds — this is what lets the schema-contract CI assert
     "the private pack's declared columns are registered" without ever
     importing or introspecting the pack's compute body.

  3. **Loading seam choice (needs-decision resolved by this PR, see the
     #1032 comment for the alternative considered).** A gitignored
     directory loaded by path was chosen over a separate private repo
     consumed as a pip/entry-point dependency:
       - Lighter to wire (no private PyPI index / git-dependency auth
         needed in CI or on the trading box).
         - The private compute never enters this repo's git history —
         ``importlib`` loads it from a path OUTSIDE the repo tree at
         runtime; nothing under ``private_features/`` is committed here
         (see ``.gitignore``). The directory exists in the checked-out
         tree only as a local convention/landing-spot, not as a
         guarantee of confinement — ``NOUSERGON_PRIVATE_FEATURE_PACK``
         may point anywhere on disk.
       - If a harder public/private seam is later wanted (e.g. the pack
         grows large enough to want its own versioning/release cycle),
         swapping the loader body for an ``importlib.metadata`` entry-point
         lookup is a contained change — callers of ``load_private_pack()``
         do not need to change.

  4. **Public CI never needs the pack.** ``tests/test_schema_contract.py``
     and every other public test runs with the env var unset, exercising
     the "no pack" path. The mechanism is proven end-to-end in
     ``tests/test_private_feature_pack.py`` via a trivial, obviously-fake
     fixture pack (``tests/fixtures/dummy_private_pack.py`` —
     ``test_private_dummy_feature_raw``, a one-line arithmetic transform,
     nothing resembling real trading logic).

See ``features/SCHEMA.md`` §3b for how a private-pack column is documented
for consumers without disclosing compute.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd

log = logging.getLogger(__name__)

ENV_VAR = "NOUSERGON_PRIVATE_FEATURE_PACK"

# The two attributes a conforming private-pack module MUST expose.
_REQUIRED_ATTRS = ("add_private_features", "PRIVATE_FEATURE_NAMES")


class PrivateFeaturePackError(RuntimeError):
    """Raised when NOUSERGON_PRIVATE_FEATURE_PACK is set but the target
    module fails to load or doesn't conform to the contract.

    Deliberately fail LOUD (not degrade-and-skip) once the env var is set:
    an operator who pointed at a pack expects it to run. Silent skips would
    let a broken private pack quietly regress to baseline-only features
    without anyone noticing — the opposite failure mode of the "absent env
    var" case, which is intentionally silent (see module docstring).
    """


def _load_module_from_path(path: Path) -> ModuleType:
    module_name = "_nousergon_private_feature_pack"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PrivateFeaturePackError(
            f"{ENV_VAR}={path} could not be turned into an import spec "
            "(not a valid .py file or package __init__.py?)"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - re-raise as our loud contract error
        raise PrivateFeaturePackError(
            f"{ENV_VAR}={path} raised on import: {exc}"
        ) from exc
    return module


def _validate_contract(module: ModuleType, path: Path) -> None:
    missing = [a for a in _REQUIRED_ATTRS if not hasattr(module, a)]
    if missing:
        raise PrivateFeaturePackError(
            f"Private feature pack at {path} is missing required "
            f"attribute(s) {missing}. A conforming pack module exposes "
            "add_private_features(df) -> df AND PRIVATE_FEATURE_NAMES: "
            "list[str]. See features/private_pack.py module docstring."
        )
    if not callable(module.add_private_features):
        raise PrivateFeaturePackError(
            f"Private feature pack at {path}: add_private_features is not "
            "callable."
        )
    names = module.PRIVATE_FEATURE_NAMES
    if not isinstance(names, (list, tuple)) or not all(
        isinstance(n, str) for n in names
    ):
        raise PrivateFeaturePackError(
            f"Private feature pack at {path}: PRIVATE_FEATURE_NAMES must "
            f"be a list[str], got {type(names)!r}."
        )


def load_private_pack(env_value: str | None = None) -> ModuleType | None:
    """Resolve + load the private feature pack, or return None if absent.

    ``env_value`` is injectable for tests; production callers omit it and
    the function reads ``os.environ[ENV_VAR]``.

    Returns None (no pack configured) when the env var is unset or blank
    — this is the default, fully-supported, public-CI path. Raises
    ``PrivateFeaturePackError`` if the env var IS set but the target
    fails to load or conform (loud-on-configured, per the class docstring).
    """
    raw = env_value if env_value is not None else os.environ.get(ENV_VAR, "")
    raw = raw.strip()
    if not raw:
        return None

    path = Path(raw).expanduser()
    if not path.exists():
        raise PrivateFeaturePackError(
            f"{ENV_VAR}={raw!r} does not exist on disk."
        )

    module = _load_module_from_path(path)
    _validate_contract(module, path)
    log.info(
        "Loaded private feature pack from %s (%d column(s): %s)",
        path, len(module.PRIVATE_FEATURE_NAMES), module.PRIVATE_FEATURE_NAMES,
    )
    return module


def apply_private_features(
    df: pd.DataFrame, *, env_value: str | None = None,
) -> pd.DataFrame:
    """Append private-pack columns to ``df`` if a pack is configured.

    No-op (returns ``df`` unchanged) when no pack is configured — the
    public/default behaviour. This is the call site
    ``features/compute.py`` wires in immediately after
    ``apply_factor_zscores`` (the existing post-per-ticker-compute,
    pre-write extension point for second-pass/cross-sectional columns).
    """
    module = load_private_pack(env_value)
    if module is None:
        return df

    out = module.add_private_features(df)
    declared = set(module.PRIVATE_FEATURE_NAMES)
    produced = set(out.columns) - set(df.columns)
    missing = declared - produced
    if missing:
        raise PrivateFeaturePackError(
            f"Private pack declared PRIVATE_FEATURE_NAMES={sorted(declared)} "
            f"but add_private_features did not add column(s) {sorted(missing)}. "
            "Declared names and actually-emitted columns must match exactly."
        )
    return out
