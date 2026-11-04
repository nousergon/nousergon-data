"""alpha-engine-config-I13143: the dead-column postflight's unpublished-field grace.

nousergon-data#2408 made the weekly DataPhase2 write
``eps_revision.days_since_earnings``. Every daily D31/D50 until the next
Saturday reads a partition written BEFORE that, whose payloads carry no such
key, so the column is the 90-day default for every ticker and the postflight
fails a column no daily input can fill. The grace skips grading exactly that
column, only while the key is absent from EVERY payload, and only through
``UNPUBLISHED_FIELD_GRACE_LAST_DAY``. These tests pin all three bounds, and
that every other dead column still fails.
"""

from __future__ import annotations

import pandas as pd
import pytest

from features import compute
from tests.test_alt_data_partial_load_i13103 import TICKERS, _END, _entry, _stock
from tests.test_alt_data_read_coverage import FakeS3, _universe

DAY = _END.date().isoformat()  # 2026-09-24, inside the grace


class _NoKeyS3(FakeS3):
    """A pre-#2408 partition: ``eps_revision`` has no ``days_since_earnings``."""

    def __init__(self, tickers, *, keyed: set[str] = frozenset()) -> None:
        super().__init__(tickers)
        self.keyed = keyed

    def get_object(self, Bucket: str, Key: str):  # noqa: N803
        out = super().get_object(Bucket=Bucket, Key=Key)
        ticker = Key.split("/")[-1].replace(".json", "")
        if ticker not in self.tickers:
            return out
        import json

        payload = json.loads(out["Body"].read())
        if ticker not in self.keyed:
            payload["eps_revision"].pop("days_since_earnings")
        return {"Body": type(out["Body"])(json.dumps(payload).encode())}


def test_loader_flags_a_key_no_payload_carries():
    alt = compute._load_cached_alternative(_NoKeyS3(_universe(5)), "b")
    assert alt.unpublished == frozenset({"days_since_earnings"})


def test_loader_flags_nothing_once_one_payload_carries_the_key():
    tickers = _universe(5)
    alt = compute._load_cached_alternative(_NoKeyS3(tickers, keyed={tickers[0]}), "b")
    assert alt.unpublished == frozenset()


def test_loader_flags_nothing_when_the_key_is_present_but_none():
    """Post-#2408 shape with yfinance down everywhere: key present, value None.
    That is a producer that RAN and wrote nothing, so it must stay graded."""
    tickers = _universe(5)

    class _NoneS3(FakeS3):
        def get_object(self, Bucket, Key):  # noqa: N803
            out = super().get_object(Bucket=Bucket, Key=Key)
            ticker = Key.split("/")[-1].replace(".json", "")
            if ticker not in self.tickers:
                return out
            import json

            payload = json.loads(out["Body"].read())
            payload["eps_revision"]["days_since_earnings"] = None
            return {"Body": type(out["Body"])(json.dumps(payload).encode())}

    assert compute._load_cached_alternative(_NoneS3(tickers), "b").unpublished == frozenset()


def _build(unpublished=()):
    return compute.FeatureBuild(None, {}, alt_unpublished=tuple(unpublished))


@pytest.mark.parametrize(
    "unpublished, day, expected",
    [
        (("days_since_earnings",), "2026-11-04", {"days_since_earnings"}),
        (("days_since_earnings",), compute.UNPUBLISHED_FIELD_GRACE_LAST_DAY, {"days_since_earnings"}),
        (("days_since_earnings",), "2026-11-10", set()),   # backstop expired
        ((), "2026-11-04", set()),                          # key published
        (("iv_rank",), "2026-11-04", set()),                # never in the grace
    ],
)
def test_ungraded_set_is_bounded(unpublished, day, expected):
    assert compute.unpublished_ungraded(_build(unpublished), day) == frozenset(expected)


def test_grace_covers_one_column_only():
    assert set(compute.UNPUBLISHED_FIELD_GRACE) == {"days_since_earnings"}


def _frame(dead: str | None) -> pd.DataFrame:
    """Every graded column varies except ``days_since_earnings`` (constant 1.0)
    and, optionally, one more ``dead`` column."""
    cols, _ = compute.dead_column_scope()
    df = pd.DataFrame({c: [0.1 * i for i in range(30)] for c in cols})
    df["days_since_earnings"] = 1.0
    if dead:
        df[dead] = 0.0
    return df


def test_fatal_postflight_skips_only_the_ungraded_column():
    with pytest.raises(RuntimeError, match="days_since_earnings"):
        compute.assert_snapshot_columns_live(_frame(None))
    compute.assert_snapshot_columns_live(_frame(None), ungraded=frozenset({"days_since_earnings"}))
    with pytest.raises(RuntimeError, match="iv_rank"):
        compute.assert_snapshot_columns_live(
            _frame("iv_rank"), ungraded=frozenset({"days_since_earnings"})
        )


def _patch(monkeypatch, alt):
    spy = _stock(300, seed=999)
    price_data = {t: _stock(300, seed=i) for i, t in enumerate(TICKERS)}
    price_data["SPY"] = spy
    monkeypatch.setattr(
        compute, "_load_prices_and_macro",
        lambda s3, bucket, date_str, **_kw: (dict(price_data), {"SPY": spy["Close"]}),
    )
    monkeypatch.setattr(compute, "_load_sector_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_sub_sector_etf_map", lambda s3, bucket: {})
    monkeypatch.setattr(compute, "_load_cached_fundamentals", lambda s3, bucket, date_str: {})
    monkeypatch.setattr(compute, "_load_cached_alternative", lambda s3, bucket: alt)
    graded: list[list[str]] = []
    monkeypatch.setattr(compute, "find_all_null_columns", lambda df, cols: [])
    monkeypatch.setattr(
        compute, "find_zero_variance_columns",
        lambda df, cols: graded.append(list(cols)) or {},
    )
    return graded


def _alt(unpublished: frozenset) -> compute.AlternativeLoad:
    alt = compute.AlternativeLoad({t: _entry(i) for i, t in enumerate(TICKERS)})
    alt.unpublished = unpublished
    return alt


@pytest.mark.parametrize("unpublished", [frozenset({"days_since_earnings"}), frozenset()])
def test_d31_grades_the_column_unless_unpublished_and_says_so(monkeypatch, unpublished):
    graded = _patch(monkeypatch, _alt(unpublished))
    result = compute.compute_and_write(
        date_str=DAY, bucket="test-bucket", dry_run=True, zero_variance_fatal=False,
    )
    assert result["dead_column_ungraded"] == sorted(unpublished)
    assert ("days_since_earnings" in graded[0]) is (not unpublished)
    # Every other non-macro column is still graded.
    assert len(graded[0]) == len(compute.dead_column_scope()[0]) - len(unpublished)
