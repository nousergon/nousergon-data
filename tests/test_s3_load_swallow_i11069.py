"""Regression tests for fail-loud S3 loaders (alpha-engine-config-I11069)."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

from collectors import alternative, crypto_balances, metron_market_data, signal_returns
from features import compute, reader


@pytest.mark.parametrize(
    ("loader", "args"),
    [
        (reader.read_feature_snapshot, ("2026-11-18", "technical", "bucket", reader.DEFAULT_PREFIX, MagicMock())),
        (reader.read_registry, ("bucket", reader.DEFAULT_PREFIX, MagicMock())),
        (compute._expected_alternative_count, (MagicMock(), "bucket", "weekly/2026-11-18/alternative/")),
        (alternative._load_promoted_tickers, (MagicMock(), "bucket", "signals.json", "2026-11-18")),
        (alternative._get_latest_date, (MagicMock(), "bucket", "weekly/")),
    ],
)
def test_load_failures_raise(
    loader: Callable[..., object], args: tuple[object, ...]
) -> None:
    """An unexpected object-load failure keeps its cause instead of becoming a default."""
    s3 = next((arg for arg in args if isinstance(arg, MagicMock)), None)
    assert s3 is not None
    s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    s3.get_object.side_effect = PermissionError("denied")
    with pytest.raises(Exception) as raised:
        loader(*args)
    assert raised.value.__cause__ is not None


def test_feature_snapshot_still_returns_none_for_genuine_absence() -> None:
    """The reader retains its explicit NoSuchKey absence contract."""
    s3 = MagicMock()
    s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
    s3.get_object.side_effect = s3.exceptions.NoSuchKey("missing")
    assert reader.read_feature_snapshot("2026-11-18", "technical", "bucket", s3_client=s3) is None
