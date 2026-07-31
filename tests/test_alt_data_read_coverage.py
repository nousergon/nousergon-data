"""``_load_cached_alternative`` must read the WHOLE partition and say so.

alpha-engine-config-I5811. The reader used to call ``list_objects_v2`` once
with ``MaxKeys=200`` and no pagination, so a 903-ticker partition resolved to
the alphabetically-first 200 and every ticker past the cut got 0.0 for all
seven registered ``alternative``-family features — indistinguishable from a
genuine zero, in every one of the five feature-store writers.

The regression test that matters is the first one: it fails against the old
implementation for exactly the production reason (partition > 200).
"""

from __future__ import annotations

import json

import pytest

from features.compute import (
    AlternativeCoverageError,
    _load_cached_alternative,
)


PREFIX = "market_data/weekly/2026-07-30/alternative/"


def _payload(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "eps_revision": {
            "surprise_pct": 1.5,
            "days_since_earnings": 0.25,
            "revision_4w": 0.02,
            "streak": 3,
        },
        "analyst_consensus": {"surprise_pct": 9.9},
        "options_flow": {
            "put_call_ratio": 0.8,
            "iv_rank": 0.4,
            "expected_move_pct": 0.05,
        },
    }


class _Body:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def read(self) -> bytes:
        return self._raw


class _Paginator:
    """Pages at 1000 keys, matching the real list_objects_v2 page size."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def paginate(self, **_kwargs):
        for start in range(0, len(self._keys), 1000):
            yield {"Contents": [{"Key": k} for k in self._keys[start:start + 1000]]}


class FakeS3:
    """Minimal S3 double: latest_weekly pointer, per-ticker objects, manifest."""

    def __init__(
        self,
        tickers: list[str],
        *,
        manifest_count: int | None = None,
        unreadable: set[str] | None = None,
        omit_manifest: bool = False,
    ) -> None:
        self.tickers = tickers
        self.manifest_count = len(tickers) if manifest_count is None else manifest_count
        self.unreadable = unreadable or set()
        self.omit_manifest = omit_manifest
        self.list_calls = 0

    def _keys(self) -> list[str]:
        # Sorted, like S3 returns them — the old cap took the first 200 of these.
        return sorted(f"{PREFIX}{t}.json" for t in self.tickers) + [f"{PREFIX}manifest.json"]

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        self.list_calls += 1
        return _Paginator(self._keys())

    def list_objects_v2(self, **kwargs):  # pragma: no cover - old code path only
        raise AssertionError(
            "unpaginated list_objects_v2 is the defect this module tests for"
        )

    def get_object(self, Bucket: str, Key: str):  # noqa: N803 - boto3 kwarg casing
        if Key == "market_data/latest_weekly.json":
            return {"Body": _Body(json.dumps({"date": "2026-07-30"}).encode())}
        if Key == f"{PREFIX}manifest.json":
            if self.omit_manifest:
                raise KeyError(Key)
            return {
                "Body": _Body(
                    json.dumps({
                        "run_date": "2026-07-30",
                        "tickers_requested": self.manifest_count,
                        "tickers_succeeded": self.manifest_count,
                    }).encode()
                )
            }
        ticker = Key.split("/")[-1].replace(".json", "")
        if ticker in self.unreadable:
            raise RuntimeError("simulated S3 read failure")
        if ticker not in self.tickers:
            raise KeyError(Key)
        return {"Body": _Body(json.dumps(_payload(ticker)).encode())}


def _universe(n: int) -> list[str]:
    """N distinct tickers whose sort order spans the whole alphabet."""
    return [f"{chr(65 + i // 26)}{chr(65 + i % 26)}{i:04d}" for i in range(n)]


def test_reads_every_ticker_past_the_old_200_cap():
    """The regression. 903 objects in, 903 tickers out — not 200."""
    tickers = _universe(903)
    s3 = FakeS3(tickers)

    alt = _load_cached_alternative(s3, "alpha-engine-research")

    assert len(alt) == 903
    assert set(alt) == set(tickers)
    # The tail of the alphabet is what the cap silently dropped.
    assert alt[sorted(tickers)[-1]]["earnings"]["surprise_pct"] == 1.5


def test_paginates_beyond_one_list_page():
    """More than 1000 keys must still resolve fully."""
    tickers = _universe(1500)
    alt = _load_cached_alternative(FakeS3(tickers), "alpha-engine-research")
    assert len(alt) == 1500


def test_zero_loaded_against_a_nonempty_manifest_raises():
    """Provably-published data that reads as nothing is a red run, not 0.0s."""
    tickers = _universe(10)
    s3 = FakeS3(tickers, unreadable=set(tickers))

    with pytest.raises(AlternativeCoverageError) as exc:
        _load_cached_alternative(s3, "alpha-engine-research")

    assert "loaded ZERO" in str(exc.value)


def test_partial_shortfall_is_reported_at_error_but_does_not_raise(caplog):
    """A provider hiccup degrades loudly; it does not fail the daily append."""
    tickers = _universe(100)
    s3 = FakeS3(tickers, unreadable=set(sorted(tickers)[:5]))

    with caplog.at_level("ERROR"):
        alt = _load_cached_alternative(s3, "alpha-engine-research")

    assert len(alt) == 95
    text = caplog.text
    assert "coverage SHORTFALL" in text
    assert "95 of 100" in text
    # The consequence is named, not just the count.
    assert "0.0" in text


def test_unreadable_objects_are_named_not_swallowed(caplog):
    tickers = _universe(20)
    s3 = FakeS3(tickers, unreadable={sorted(tickers)[0]})

    with caplog.at_level("ERROR"):
        _load_cached_alternative(s3, "alpha-engine-research")

    assert "objects unreadable" in caplog.text
    assert sorted(tickers)[0] in caplog.text


def test_missing_manifest_warns_that_under_read_is_undetectable(caplog):
    tickers = _universe(30)
    s3 = FakeS3(tickers, omit_manifest=True)

    with caplog.at_level("WARNING"):
        alt = _load_cached_alternative(s3, "alpha-engine-research")

    assert len(alt) == 30
    assert "no producer count to reconcile against" in caplog.text


def test_null_subsection_does_not_drop_the_ticker():
    """A present-but-null section used to raise and be swallowed per-ticker."""
    tickers = ["AAA0000"]
    s3 = FakeS3(tickers)
    original = s3.get_object

    def _with_null_sections(Bucket: str, Key: str):  # noqa: N803
        if Key.endswith("AAA0000.json"):
            return {
                "Body": _Body(
                    json.dumps({
                        "ticker": "AAA0000",
                        "eps_revision": None,
                        "analyst_consensus": None,
                        "options_flow": None,
                    }).encode()
                )
            }
        return original(Bucket=Bucket, Key=Key)

    s3.get_object = _with_null_sections

    alt = _load_cached_alternative(s3, "alpha-engine-research")

    assert "AAA0000" in alt
    assert alt["AAA0000"]["earnings"]["surprise_pct"] == 0.0
    assert alt["AAA0000"]["options"]["iv_rank"] is None
