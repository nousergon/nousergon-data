"""A `pending` row's re-exam must sit within the ops validator's ceiling.

Every `registry.d/*.yaml` row here is gathered by nous-ergon-ops's
`observability-registry-publish.yml` and validated there by
`authority_surface.check_pending_row`, which fails the WHOLE publish (every
repo's rows, not just this one) when `lifecycle_reexam` is more than
`PENDING_MAX_DAYS` after `pending_since`. Nothing in this repo checked that,
so nousergon-data#2126 merged green with D51 at 17 days and turned
nous-ergon-ops main red on its next scheduled run (2026-10-12).

The constant mirrors `nous-ergon-ops/scripts/authority_surface.py ::
PENDING_MAX_DAYS`, which is normative; that repo is private and this CI cannot
import it. If the two ever disagree, the ops value wins and this one is wrong.

Only the ceiling and the ordering are checked here. The "re-exam has passed"
failure depends on today's date, and a test that goes red on a calendar date
with no commit would make every unrelated PR in this repo red.
"""

from __future__ import annotations

import datetime as dt
import pathlib

import pytest
import yaml

PENDING_MAX_DAYS = 14  # mirrors nous-ergon-ops authority_surface.PENDING_MAX_DAYS

ROOT = pathlib.Path(__file__).resolve().parents[1] / "registry.d"
ROWS = sorted(p for p in ROOT.rglob("*.yaml"))


def _pending(path: pathlib.Path) -> dict | None:
    data = yaml.safe_load(path.read_text())
    if isinstance(data, dict) and data.get("lifecycle") == "pending":
        return data
    return None


PENDING = [(p, d) for p in ROWS if (d := _pending(p)) is not None]


def test_registry_has_rows():
    assert ROWS, f"no registry rows found under {ROOT}"


@pytest.mark.parametrize(
    "path,data", PENDING, ids=[str(p.relative_to(ROOT)) for p, _ in PENDING]
)
def test_pending_reexam_within_ceiling(path, data):
    since = dt.date.fromisoformat(str(data["pending_since"]))
    reexam = dt.date.fromisoformat(str(data["lifecycle_reexam"]))
    assert reexam >= since, f"{path.name}: lifecycle_reexam {reexam} is before pending_since {since}"
    assert (reexam - since).days <= PENDING_MAX_DAYS, (
        f"{path.name}: lifecycle_reexam {reexam} is {(reexam - since).days} days after "
        f"pending_since {since}, over the {PENDING_MAX_DAYS}-day ceiling the ops "
        "publish validator enforces. Either the component is live (flip to in-service) "
        "or the row records a new decision; moving the date past the ceiling fails "
        "nous-ergon-ops observability-registry-publish for the whole fleet."
    )
