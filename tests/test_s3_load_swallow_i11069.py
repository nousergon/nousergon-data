"""Regression tests for fail-loud S3 loaders (alpha-engine-config-I11069)."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

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


def _not_found() -> ClientError:
    return ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def _denied() -> ClientError:
    return ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")


def _signals_body(*tickers: str) -> dict[str, object]:
    payload = {"buy_candidates": [{"ticker": t} for t in tickers]}
    return {"Body": io.BytesIO(json.dumps(payload).encode())}


def test_expected_alternative_count_returns_none_for_genuine_absence() -> None:
    """A missing manifest stays None; the caller reports the undetectable under-read."""
    s3 = MagicMock()
    s3.get_object.side_effect = _not_found()
    assert compute._expected_alternative_count(s3, "bucket", "weekly/2026-11-18/alternative/") is None


def test_expected_alternative_count_raises_on_other_client_errors() -> None:
    s3 = MagicMock()
    s3.get_object.side_effect = _denied()
    with pytest.raises(compute.AlternativeCoverageError) as raised:
        compute._expected_alternative_count(s3, "bucket", "weekly/2026-11-18/alternative/")
    assert raised.value.__cause__ is not None


def test_promoted_tickers_fall_back_to_a_previous_day_on_genuine_absence() -> None:
    """The dated signals.json is absent: the previous-days fallback still applies."""
    s3 = MagicMock()

    def _get(Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803 - boto3 kwarg casing
        if Key == "signals/2026-11-16/signals.json":
            return _signals_body("AAA")
        raise _not_found()

    s3.get_object.side_effect = _get
    tickers = alternative._load_promoted_tickers(s3, "bucket", None, "2026-11-18")
    assert "AAA" in tickers
    keys = [c.kwargs["Key"] for c in s3.get_object.call_args_list]
    assert keys == [
        "signals/2026-11-18/signals.json",
        "signals/2026-11-17/signals.json",
        "signals/2026-11-16/signals.json",
    ]


def test_promoted_tickers_return_empty_only_when_every_day_is_absent() -> None:
    s3 = MagicMock()
    s3.get_object.side_effect = _not_found()
    assert alternative._load_promoted_tickers(s3, "bucket", None, "2026-11-18") == []
    assert s3.get_object.call_count == 8


def test_promoted_tickers_raise_on_a_non_absence_error_for_the_dated_file() -> None:
    s3 = MagicMock()
    s3.get_object.side_effect = _denied()
    with pytest.raises(RuntimeError) as raised:
        alternative._load_promoted_tickers(s3, "bucket", None, "2026-11-18")
    assert raised.value.__cause__ is not None
    assert s3.get_object.call_count == 1


def test_promoted_tickers_raise_on_a_non_absence_error_during_fallback() -> None:
    """A fallback day that fails for a reason other than absence is not skipped."""
    s3 = MagicMock()
    s3.get_object.side_effect = [_not_found(), _denied()]
    with pytest.raises(RuntimeError) as raised:
        alternative._load_promoted_tickers(s3, "bucket", None, "2026-11-18")
    assert "signals/2026-11-17/signals.json" in str(raised.value)
    assert raised.value.__cause__ is not None
