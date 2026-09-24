"""D21 carries interior bars forward from its previous publish (alpha-engine-config-I11556).

``collect_history`` rebuilt every close series from its sources on each run. On
2026-09-23 the yfinance gap-fill answered ``[..., 09-21, 09-23]`` for VOO, ASML,
MELI and 21 other symbols that reach ``market_data/close_history/`` only through
that gap-fill, so the publish dropped the 2026-09-22 bar the 09-22 run had
written. Replaying the real 09-22 and 09-23 versions of ``consolidated.json``
through :func:`carry_forward_interior_bars` carries that session back for 854
symbols with no refusal.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
from unittest.mock import MagicMock

import boto3
import botocore.client
import pytest
from botocore.exceptions import ClientError

from collectors import metron_market_data as mmd
from shadow import interceptor
from shadow.root import ShadowRoot, activate, deactivate

RUN_DATE = "2026-09-23"
KEY = mmd.CONSOLIDATED_CLOSE_HISTORY_KEY

#: What the 2026-09-22 D21 run published for VOO (real values).
PUBLISHED_0922 = [["2026-09-18", 710.0], ["2026-09-21", 709.5], ["2026-09-22", 712.780029]]
#: What the 2026-09-23 yfinance gap-fill answered: the 09-22 session is gone.
REFETCH_0923 = [("2026-09-18", 710.0), ("2026-09-21", 709.5), ("2026-09-23", 705.0)]


# ── the pure rule ───────────────────────────────────────────────────────────


def test_a_refetch_that_omits_an_interior_session_keeps_the_published_bar():
    series, carried, refused = mmd.carry_forward_interior_bars(
        {"VOO": REFETCH_0923}, {"VOO": PUBLISHED_0922},
    )
    assert series["VOO"] == [
        ("2026-09-18", 710.0), ("2026-09-21", 709.5), ("2026-09-22", 712.780029), ("2026-09-23", 705.0),
    ]
    assert [(c["symbol"], c["date"], c["close"]) for c in carried] == [("VOO", "2026-09-22", 712.780029)]
    assert refused == []


def test_a_refetched_bar_that_disagrees_still_wins():
    fresh = [("2026-09-21", 709.5), ("2026-09-22", 700.0), ("2026-09-23", 705.0)]
    series, carried, _ = mmd.carry_forward_interior_bars({"VOO": fresh}, {"VOO": PUBLISHED_0922})
    assert series["VOO"] is fresh
    assert carried == []


def test_only_the_interior_is_carried_never_the_ends():
    """The window rolls forward and the newest session may be unprinted; neither is a loss."""
    previous = [["2026-09-15", 1.0], ["2026-09-16", 2.0], ["2026-09-21", 3.0], ["2026-09-22", 4.0],
                ["2026-09-23", 5.0], ["2026-09-24", 6.0]]
    fresh = [("2026-09-16", 2.0), ("2026-09-21", 3.0), ("2026-09-23", 5.0)]
    series, carried, _ = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert [d for d, _c in series["X"]] == ["2026-09-16", "2026-09-21", "2026-09-22", "2026-09-23"]
    assert [c["date"] for c in carried] == ["2026-09-22"]


def test_a_dropped_non_session_is_not_carried_and_is_recorded():
    previous = [["2026-09-18", 1.0], ["2026-09-19", 1.5], ["2026-09-21", 2.0]]  # 09-19 is a Saturday
    fresh = [("2026-09-18", 1.0), ("2026-09-21", 2.0)]
    series, carried, refused = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert series["X"] is fresh and carried == []
    assert refused == [{"symbol": "X", "date": "2026-09-19", "reason": "not_a_session"}]


def test_a_carried_bar_is_rescaled_onto_the_fresh_dividend_adjusted_basis():
    """A dividend that went ex after the previous publish rescaled every earlier bar."""
    factor = 0.99
    previous = [["2026-09-21", 100.0], ["2026-09-22", 102.0], ["2026-09-23", 101.0]]
    fresh = [("2026-09-21", 100.0 * factor), ("2026-09-23", 101.0 * factor), ("2026-09-24", 98.0)]
    series, carried, refused = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert dict(series["X"])["2026-09-22"] == pytest.approx(102.0 * factor)
    assert carried[0]["factor"] == pytest.approx(factor) and refused == []


def test_the_carry_is_refused_when_a_corporate_action_sits_between_the_anchors():
    previous = [["2026-09-21", 100.0], ["2026-09-22", 102.0], ["2026-09-23", 101.0]]
    fresh = [("2026-09-21", 50.0), ("2026-09-23", 101.0)]  # a 2:1 split applied on one side only
    series, carried, refused = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert series["X"] is fresh and carried == []
    assert [r["reason"] for r in refused] == ["basis_factor_disagreement"]


def test_the_upper_anchor_is_optional_because_the_previous_publish_ends_on_the_lost_session():
    """The 2026-09-23 run's case: the 09-22 publish's LAST bar was the one then dropped."""
    previous = [["2026-09-21", 100.0], ["2026-09-22", 102.0]]
    fresh = [("2026-09-21", 100.0), ("2026-09-23", 99.0)]
    _, carried, refused = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert [c["date"] for c in carried] == ["2026-09-22"] and refused == []


def test_no_lower_anchor_refuses():
    previous = [["2026-09-18", 100.0], ["2026-09-22", 102.0], ["2026-09-23", 101.0]]
    fresh = [("2026-09-21", 100.0), ("2026-09-23", 101.0)]
    series, carried, refused = mmd.carry_forward_interior_bars({"X": fresh}, {"X": previous})
    assert series["X"] is fresh and carried == []
    assert [r["reason"] for r in refused] == ["no_lower_anchor"]


def test_a_symbol_absent_from_the_previous_publish_or_malformed_is_left_alone():
    fresh = {"NEW": REFETCH_0923, "BAD": REFETCH_0923}
    series, carried, refused = mmd.carry_forward_interior_bars(fresh, {"BAD": [["2026-09-22"]]})
    assert series == fresh and carried == []
    assert refused == [{"symbol": "BAD", "date": None, "reason": "previous_series_malformed"}]


# ── wired into collect_history ──────────────────────────────────────────────


def _s3_serving(previous_doc):
    s3 = MagicMock()

    def _get(Bucket, Key, **_kw):
        if Key == KEY and previous_doc is not None:
            if isinstance(previous_doc, Exception):
                raise previous_doc
            return {"Body": io.BytesIO(json.dumps(previous_doc).encode())}
        raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")

    s3.get_object.side_effect = _get
    return s3


def _puts(s3) -> dict[str, dict]:
    return {c.kwargs["Key"]: json.loads(c.kwargs["Body"]) for c in s3.put_object.call_args_list}


@pytest.fixture
def one_symbol_universe(monkeypatch):
    monkeypatch.setattr(
        mmd, "load_price_derived_universe", lambda bucket, s3: ([{"yf_symbol": "VOO", "currency": "USD"}], []),
    )


def _run(s3):
    return mmd.collect_history(
        bucket="b", run_date=RUN_DATE, s3_client=s3,
        close_history_source=lambda syms: {"VOO": list(REFETCH_0923)},
        fx_history_source=lambda ccys: {},
    )


def test_collect_history_publishes_the_carried_bar_in_both_artifacts(one_symbol_universe, caplog):
    s3 = _s3_serving({"series": {"VOO": PUBLISHED_0922}, "currency": {"VOO": "USD"}})
    with caplog.at_level(logging.WARNING, logger=mmd.logger.name):
        result = _run(s3)
    assert result["status"] == "ok"
    assert (result["carry_base"], result["carried_bars"], result["carry_refused"]) == ("read", 1, 0)
    puts = _puts(s3)
    want = ["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]
    assert [d for d, _c in puts[KEY]["series"]["VOO"]] == want
    assert [d for d, _c in puts[f"{mmd.CLOSE_HISTORY_PREFIX}VOO.json"]["closes"]] == want
    carry_lines = [r.getMessage() for r in caplog.records if "carried" in r.getMessage()]
    assert len(carry_lines) == 1 and "VOO@2026-09-22=712.78" in carry_lines[0]


def test_with_no_previous_publish_the_fresh_series_is_published_as_is(one_symbol_universe):
    s3 = _s3_serving(None)
    result = _run(s3)
    assert (result["carry_base"], result["carried_bars"]) == ("absent", 0)
    assert [d for d, _c in _puts(s3)[KEY]["series"]["VOO"]] == [d for d, _c in REFETCH_0923]


def test_an_unreadable_previous_publish_is_an_error_line_not_a_failed_run(one_symbol_universe, caplog):
    s3 = _s3_serving(ClientError({"Error": {"Code": "SlowDown", "Message": "x"}}, "GetObject"))
    with caplog.at_level(logging.ERROR, logger=mmd.logger.name):
        result = _run(s3)
    assert result["status"] == "ok" and result["carry_base"] == "unreadable"
    assert any("unreadable" in r.getMessage() and r.levelno == logging.ERROR for r in caplog.records)


# ── the previous-publish read is run state under a shadow root ─────────────


class _MemoryS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.reads: list[str] = []

    def __call__(self, client, operation, params):
        name = params.get("Key")
        if operation == "PutObject":
            self.objects[name] = params["Body"]
            return {"ETag": "e"}
        if operation == "GetObject":
            self.reads.append(name)
            if name not in self.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, operation)
            return {"Body": io.BytesIO(self.objects[name])}
        raise AssertionError(f"not modelled: {operation}")


@pytest.fixture
def shadow_bucket(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    root = ShadowRoot(dt.date.fromisoformat(RUN_DATE))
    real = botocore.client.BaseClient._make_api_call
    v1 = json.dumps({"series": {"VOO": PUBLISHED_0922}}).encode()
    s3 = _MemoryS3({KEY: v1})  # v1's LIVE publish, carrying 09-22
    activate(root)
    interceptor._ORIGINAL = s3
    try:
        yield root, s3
    finally:
        interceptor._ORIGINAL = real
        deactivate()


def test_under_a_shadow_root_the_previous_publish_is_read_from_the_shadow(one_symbol_universe, shadow_bucket):
    """Read as a live input, the publish of the same key raises ShadowGuardViolation."""
    root, s3 = shadow_bucket
    result = _run(boto3.client("s3"))
    assert result["status"] == "ok" and result["carry_base"] == "absent"
    assert s3.reads == [root.key(KEY)], "never v1's live copy"
    published = json.loads(s3.objects[root.key(KEY)])
    assert "2026-09-22" not in [d for d, _c in published["series"]["VOO"]], "v1's bar is not the shadow's"


def test_under_a_shadow_root_the_shadows_own_previous_publish_is_carried(one_symbol_universe, shadow_bucket):
    root, s3 = shadow_bucket
    s3.objects[root.key(KEY)] = json.dumps({"series": {"VOO": PUBLISHED_0922}}).encode()
    result = _run(boto3.client("s3"))
    assert (result["carry_base"], result["carried_bars"]) == ("read", 1)


# ── the one-off restore, and what the next D21 run does with it ─────────────


def test_restore_plan_puts_back_the_bar_and_the_next_d21_run_keeps_it(one_symbol_universe):
    from scripts import restore_close_history_bars as restore

    live = {"series": {"VOO": [list(p) for p in REFETCH_0923], "FNILX": [["2026-09-21", 27.85], ["2026-09-23", 27.64]]},
            "currency": {"VOO": "USD", "FNILX": "USD"}}
    source = {"series": {"VOO": PUBLISHED_0922, "FNILX": [["2026-09-18", 27.43], ["2026-09-21", 27.85]]}}

    plan = restore.plan_restore(live, source, ["VOO"], ["2026-09-22"])
    assert plan["ok"] and plan["changed_symbols"] == ["VOO"]

    # A symbol the source version never carried on that session cannot be restored from it.
    assert not restore.plan_restore(live, source, ["FNILX", "VOO"], ["2026-09-22"])["ok"]

    s3 = MagicMock()
    s3.get_object.return_value = {"Body": io.BytesIO(json.dumps({"yf_symbol": "VOO", "closes": []}).encode())}
    restore.apply_restore(s3, "b", live, '"etag-1"', plan)
    calls = {c.kwargs["Key"]: c.kwargs for c in s3.put_object.call_args_list}
    assert calls[KEY]["IfMatch"] == '"etag-1"', "never overwrites a D21 publish that landed after the read"
    restored = json.loads(calls[KEY]["Body"])
    assert "2026-09-22" in [d for d, _c in restored["series"]["VOO"]]
    assert restored["series"]["FNILX"] == live["series"]["FNILX"], "unnamed symbols untouched"
    assert "2026-09-22" in [d for d, _c in json.loads(calls[f"{mmd.CLOSE_HISTORY_PREFIX}VOO.json"]["Body"])["closes"]]

    # The next scheduled D21 run: the source still omits 09-22, and the bar survives.
    nxt = _s3_serving(restored)
    result = _run(nxt)
    assert result["carried_bars"] == 1
    assert "2026-09-22" in [d for d, _c in _puts(nxt)[KEY]["series"]["VOO"]]
