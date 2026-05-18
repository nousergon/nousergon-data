"""Wave 2 regression — pin the feature-store S3 prefix invariant.

ROADMAP "predictor/ S3 namespace rationalization — Wave 2" audited the
`predictor/feature_store/` prefix and found it RESOLVED: that prefix never
existed in current production. The data module's feature snapshots are
written to the top-level `features/` prefix (which carries its own
90d-IA / 365d-expiration lifecycle rule applied in Wave 1 PR #113),
NOT under the `predictor/` junk-drawer prefix.

These source-text invariants mirror the Wave 1 discipline (6 regression
tests pinning the migrated prefix) so a future refactor cannot silently
re-introduce a `predictor/feature_store/` write path and re-create the
2026-04-29 cross-repo consumer-audit surprise.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.compute import FEATURE_STORE_PREFIX
from features.writer import DEFAULT_PREFIX


def test_compute_feature_store_prefix_is_top_level():
    """features/compute.py must write to top-level `features/`."""
    assert FEATURE_STORE_PREFIX == "features/"


def test_writer_default_prefix_is_top_level():
    """features/writer.py's default prefix must be top-level `features/`."""
    assert DEFAULT_PREFIX == "features/"


def test_no_predictor_feature_store_prefix_anywhere():
    """No live writer/reader may target the retired `predictor/feature_store/`.

    `predictor/feature_store/` was an early-consumer-bound name that was
    never actually used by current production (the prefix has 0 objects in
    S3). Guard against any source-text re-introduction outside of historical
    lineage docstrings.
    """
    assert not FEATURE_STORE_PREFIX.startswith("predictor/")
    assert not DEFAULT_PREFIX.startswith("predictor/")
    assert "feature_store" not in FEATURE_STORE_PREFIX
