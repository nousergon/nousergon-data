"""Only Phase 1 advances ``market_data/latest_weekly.json``.

The pointer names the ``weekly/{date}/`` prefix whose ``constituents.json``
its readers resolve, and only Phase 1 writes that file (D01,
alpha-engine-config-I10855). On 2026-10-16 two weekday rehearsals' DataPhase2
advanced the pointer to ``2026-10-16``, a prefix with no ``constituents.json``,
and D48 failed that night on SPX ``weight_method='unavailable'``.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import weekly_collector as wc


def _keys_written(phase: int) -> list[str]:
    s3 = MagicMock()
    with patch.object(wc.boto3, "client", return_value=s3):
        wc._write_manifest("bkt", "market_data/", "2026-10-16", {"phase": phase})
    return [c.kwargs["Key"] for c in s3.put_object.call_args_list]


def test_phase1_writes_manifest_and_advances_pointer():
    assert _keys_written(1) == [
        "market_data/weekly/2026-10-16/manifest.json",
        "market_data/latest_weekly.json",
    ]


@pytest.mark.parametrize("phase", [2, None])
def test_other_phases_write_manifest_but_never_the_pointer(phase):
    assert _keys_written(phase) == ["market_data/weekly/2026-10-16/manifest.json"]
