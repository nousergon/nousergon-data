"""Chokepoint: infrastructure ``*.sh`` alert invocations must use ``krepis.alerts``.

The alerts CLI relocated from ``nousergon_lib.alerts`` (pre-rename) →
``nousergon_lib.alerts`` (v0.60.0 rename) → ``krepis.alerts`` (v0.66.0 MIT
rebase). Both older names are now shims, and invoking a shim under runpy
(``python -m <shim>.alerts``) is broken in two distinct ways:

  * ``python -m nousergon_lib.alerts`` — the alias shim's ``_AliasLoader``
    has no ``get_code``, so runpy raises ``AttributeError`` and the process
    dies (crashes, or is swallowed by a ``|| echo`` degrade → no alert).
  * ``python -m nousergon_lib.alerts`` — a re-export shim; on any nousergon-lib
    pin < v0.81.1 it fell off its end with exit 0 before the target's
    ``__main__`` guard fired, i.e. a SILENT no-op (the config#1646 incident: a
    weekly Step Function reported SUCCESS while running zero workloads). This
    repo pins nousergon-lib v0.83.0, so that path no-ops here today.

The robust, pin-independent entrypoint is ``python -m krepis.alerts`` — ``krepis``
is a hard transitive dep (``requirements.txt`` floors ``krepis>=0.6.0``), so the
real CLI runs under runpy regardless of the nousergon-lib pin. This guard fails
loud at PR time if any box-executed ``infrastructure/`` script re-introduces a
shim-name ``-m ...alerts`` runpy invocation. Tracks config#1339.

Shape mirrors the repo's forbidden-phrase chokepoint tests (e.g.
``test_spot_data_weekly_ssm_transport.py``).
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_INFRA = _REPO_ROOT / "infrastructure"

# A runpy invocation of the alerts CLI under a SHIM name (``python``/``python3``/
# ``$VAR`` before ``-m`` tolerated). Bare imports and prose mentions are fine —
# only the ``-m`` runpy entrypoint is broken for the shim names.
_SHIM_ALERTS_RE = re.compile(r"-m\s+(?:nousergon_lib|nousergon_lib)\.alerts\b")


def _iter_infra_scripts():
    """Shell scripts under infrastructure/ — the scripts SSM/systemd/deploy
    flows execute on the box, the only place a runpy shim invocation does harm."""
    if not _INFRA.is_dir():
        return
    yield from _INFRA.rglob("*.sh")


def test_infra_alert_invocations_use_krepis():
    violations: list[tuple[str, int, str]] = []
    for path in _iter_infra_scripts():
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if line.lstrip().startswith("#"):  # historical-context prose is fine
                continue
            if _SHIM_ALERTS_RE.search(line):
                violations.append(
                    (str(path.relative_to(_REPO_ROOT)), lineno, line.strip())
                )
    assert not violations, (
        "Found `python -m {nousergon_lib,nousergon_lib}.alerts` runpy "
        "invocation(s) in infrastructure scripts. The alias shim crashes under "
        "runpy and the re-export shim silently no-ops on nousergon-lib < "
        "v0.81.1. Use the pin-independent `-m krepis.alerts` (config#1339):\n"
        + "\n".join(f"  {p}:{ln}  {src}" for p, ln, src in violations)
    )
