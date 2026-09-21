"""Every standalone unit entry point writes exactly one manifest per execution.

`data_collection_plan_260914.md` §2 row 7 / §4.4; `alpha-engine-config-I10810`
(deliverable 1), the standalone half of `-I10773` (P-06).

``tests/test_run_units.py`` grades the phase table inside ``weekly_collector``.
These five units have no phase: each is its OWN process — a systemd timer (D36,
D37), a Lambda handler (D38), a GitHub Actions job (D39, D42) — so what has to
be graded is the property the plan actually asks for, at the call site:

  * the manifest is written on the OK path, with the unit's real row counts;
  * it is written on the FAILURE path too, saying ``failed``, and the entry
    point's own return value / exit code is UNCHANGED;
  * an exception from the body still PROPAGATES, after the record is durable.

The third is the one a naive wrapper breaks: swallowing the exception to
guarantee the record is exactly how a dying producer starts reporting success.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import run_units

REPO_ROOT = Path(__file__).resolve().parents[1]

#: A real-shaped sha so `resolve_code_sha` never shells out to git in a test.
FAKE_SHA = "a" * 40


class FakeSink:
    """Captures what a real ``S3ManifestSink`` would have PUT."""

    bucket = "test-bucket"

    def __init__(self) -> None:
        self.writes: list[tuple[str, dict]] = []

    def write(self, key: str, payload: bytes):
        self.writes.append((key, json.loads(payload.decode("utf-8"))))
        return None

    @property
    def only(self) -> dict:
        assert len(self.writes) == 1, (
            f"expected exactly ONE manifest per execution, got {len(self.writes)}: "
            f"{[k for k, _ in self.writes]}"
        )
        return self.writes[0][1]

    @property
    def only_key(self) -> str:
        assert len(self.writes) == 1
        return self.writes[0][0]


@pytest.fixture
def sink(monkeypatch) -> FakeSink:
    """Swap the SINK, not the wrapper — the tests stay on the real code path."""
    fake = FakeSink()
    monkeypatch.setenv("NE_DATA_CODE_SHA", FAKE_SHA)
    monkeypatch.delenv(run_units.TRIGGER_ENV, raising=False)
    monkeypatch.delenv(run_units.LOG_LOCATION_ENV, raising=False)
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: fake)
    return fake


def _load_script(path: Path, name: str):
    """Import a by-path script (``scripts/``, a Lambda handler) as a module."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ───────────────────────────── D36 — daily-news ─────────────────────────────


def _daily_news_result(status: str = "ok") -> dict:
    return {
        "status": status,
        "tickers": 61,
        "articles": 412,
        "rows": 61,
        "key": "data/news_aggregates_daily/2026-09-14/aggregates.parquet",
        "articles_status": "ok",
        "articles_key": "data/news_articles_daily/2026-09-14/articles.parquet",
        "articles_rows": 389,
        "digest_status": "ok",
        "digest_key": "data/news_digest_daily/latest.json",
        "digest_total": 24,
        "topic_status": "ok",
        "rag_status": "ok",
        "rag_documents_ingested": 389,
        "rag_documents_skipped_exists": 0,
    }


@pytest.fixture
def daily_news(monkeypatch):
    from collectors import daily_news as module

    monkeypatch.setattr(sys, "argv", ["daily_news", "--date", "2026-09-14"])
    return module


def test_d36_writes_one_manifest_with_every_published_key(sink, daily_news, monkeypatch):
    monkeypatch.setattr(daily_news, "collect", lambda *a, **kw: _daily_news_result())  # noqa: ARG005

    assert daily_news.main() == 0

    manifest = sink.only
    assert sink.only_key.startswith("data_collection/runs/D36/2026-09-14/")
    assert manifest["unit_id"] == "D36"
    assert manifest["status"] == "ok"
    assert manifest["trigger"] == "scheduled"
    assert manifest["schema_version"] == "data_run_manifest.v1"
    # All three published keys, each with the count the collector measured —
    # never a 0 standing in for an unreported one.
    assert {(o["key"], o["rows_out"]) for o in manifest["outputs"]} == {
        ("data/news_aggregates_daily/2026-09-14/aggregates.parquet", 61),
        ("data/news_articles_daily/2026-09-14/articles.parquet", 389),
        ("data/news_digest_daily/latest.json", 24),
    }
    assert manifest["rows_out"] == 474
    assert manifest["rows_in"] == 412
    assert any("holdings_universe.json" in i["key"] for i in manifest["inputs"])


def test_d36_failure_writes_a_failed_manifest_and_keeps_the_exit_code(sink, daily_news, monkeypatch):
    monkeypatch.setattr(daily_news, "collect", lambda *a, **kw: _daily_news_result("error"))  # noqa: ARG005

    # The exit-code contract is exactly what it was before the manifest existed.
    assert daily_news.main() == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "status='error'" in manifest["reason"] or "'error'" in manifest["reason"]


def test_d36_empty_run_is_graded_not_silently_passed(sink, daily_news, monkeypatch):
    monkeypatch.setattr(daily_news, "collect", lambda *a, **kw: {  # noqa: ARG005
        "status": "skipped", "reason": "empty_universe", "tickers": 0,
    })

    assert daily_news.main() == 0

    manifest = sink.only
    assert manifest["outputs"] == []
    verdicts = {g["verdict"] for g in manifest["guards"]}
    assert "empty_fresh" in verdicts, (
        "a run that published NO key must be graded, not recorded as a clean ok"
    )


def test_d36_exception_propagates_after_the_record_is_durable(sink, daily_news, monkeypatch):
    def _boom(*a, **kw):  # noqa: ARG001
        raise RuntimeError("polygon exploded")

    monkeypatch.setattr(daily_news, "collect", _boom)

    with pytest.raises(RuntimeError, match="polygon exploded"):
        daily_news.main()

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "polygon exploded" in manifest["reason"]


def test_d36_default_trading_day_uses_session_date_not_last_closed(sink, monkeypatch):
    """No ``--date`` (the real systemd-timer invocation, 04:00 America/Los_Angeles,
    hours before that day's own NYSE close): the manifest must key on the session
    ``now`` falls WITHIN, never the last one that has fully closed —
    `alpha-engine-config-I10810`, same defect class as D37 (`nousergon-data-PR1844`).
    Measured live: every D36 object written 2026-09-18 through 2026-09-21 sat under
    ``runs/D36/2026-09-18/``; ``runs/D36/2026-09-21/`` (Monday, a trading day)
    stayed empty."""
    from collectors import daily_news as module
    import nousergon_lib.dates as lib_dates

    monkeypatch.setattr(sys, "argv", ["daily_news"])
    monkeypatch.setattr(module, "collect", lambda *a, **kw: _daily_news_result())  # noqa: ARG005
    # The as-of axis (last CLOSED session) is stuck on Friday; the event-time
    # axis (the session `now` falls within) is Monday itself.
    monkeypatch.setattr(lib_dates, "last_closed_trading_day", lambda *_a, **_kw: date(2026, 9, 18))
    monkeypatch.setattr(lib_dates, "session_date", lambda *_a, **_kw: date(2026, 9, 21))

    assert module.main() == 0

    manifest = sink.only
    assert sink.only_key.startswith("data_collection/runs/D36/2026-09-21/")
    assert manifest["trading_day"] == "2026-09-21"


def test_d36_default_trading_day_does_not_move_any_published_artifact_key(sink, monkeypatch):
    """Switching the run-manifest's `trading_day` to `default_session_date()`
    must NOT move `collect()`'s own `aggregate_date`/`filed_date`/`digest_date`
    keys — those come from `run_date=args.date` (``None`` on the scheduled
    path, falling back to the literal UTC calendar date inside `collect()`),
    never from `default_run_date()`/`default_session_date()`. This asserts the
    published keys `collect()` reports are exactly what a mock returns,
    independent of the manifest's own `trading_day` field."""
    from collectors import daily_news as module

    monkeypatch.setattr(sys, "argv", ["daily_news", "--date", "2026-09-14"])
    captured_run_date: dict = {}

    def _collect(*_a, run_date=None, **_kw):
        captured_run_date["run_date"] = run_date
        return _daily_news_result()

    monkeypatch.setattr(module, "collect", _collect)

    assert module.main() == 0

    manifest = sink.only
    # The manifest's own trading_day (from --date, unaffected by this change).
    assert manifest["trading_day"] == "2026-09-14"
    # collect() received the SAME --date value, not something derived from
    # default_session_date() — the published-artifact date axis is untouched.
    assert captured_run_date["run_date"] == "2026-09-14"
    assert {o["key"] for o in manifest["outputs"]} == {
        "data/news_aggregates_daily/2026-09-14/aggregates.parquet",
        "data/news_articles_daily/2026-09-14/articles.parquet",
        "data/news_digest_daily/latest.json",
    }


# D36's cadence is `continuous` (`data_gate/cadence.py`'s third shape: runs at
# least daily, so the gate's own trading day is the right day) — the ONLY
# other unit sharing that cadence is D37, which cannot fall on this test's
# open finding because it is gated to `in_us_market_window` and never runs on
# a weekend or holiday. D36 has no such gate: it runs every calendar day.
#
# `data_gate/cadence.py::latest_trading_day_on_or_before` (owned by the
# `data_gate` track, NOT edited here) computes the folder the gate expects a
# continuous-cadence unit's manifest under, for a given calendar date, as
# ``calendar_date if is_trading_day(calendar_date) else previous_trading_day``
# — i.e. on a non-trading day it looks BACKWARD to the last session.
# `nousergon_lib.dates.session_date()` (the event-time axis this PR now uses
# for D36's `trading_day`) resolves a non-trading day FORWARD, to the next
# session ("Saturday -> Mon: the upcoming session", per its own docstring).
# The two axes agree on every TRADING day (both mean "today") and disagree on
# every NON-trading day (backward vs forward) — measured below over a real
# Thu-to-Tue week spanning a weekend, plus one NYSE holiday.
_D36_WEEK_SEQUENCE = [
    # (calendar_date, is a trading day, expect producer/reader agreement)
    (date(2026, 9, 17), True, True),   # Thu
    (date(2026, 9, 18), True, True),   # Fri
    (date(2026, 9, 19), False, False),  # Sat — KNOWN GAP, see below
    (date(2026, 9, 20), False, False),  # Sun — KNOWN GAP, see below
    (date(2026, 9, 21), True, True),   # Mon
    (date(2026, 9, 22), True, True),   # Tue
    (date(2026, 1, 1), False, False),  # New Year's Day (NYSE holiday) — KNOWN GAP
]


@pytest.mark.parametrize("calendar_date,is_trading,agrees", _D36_WEEK_SEQUENCE)
def test_d36_producer_and_reader_trading_day_axes_over_a_real_week(calendar_date, is_trading, agrees):
    """Documents, rather than patches, the open finding above: on every TRADING
    day the axes agree (this PR's fix makes D36's weekday manifests visible to
    the gate); on every NON-trading day (weekend or holiday) they now disagree,
    which is a REGRESSION relative to `default_run_date()`'s old behavior on
    those two days specifically (it coincided with the reader's backward-looking
    axis by chance). Net effect measured over a full week: 5/7 days newly
    correct (every weekday, previously 0/5 correct) vs 2/7 days newly
    incorrect (Sat/Sun, previously 2/2 correct) — a clear net improvement, not
    a complete fix.

    This is a `data_gate` reader question, not a `nousergon-data` one: fixing
    it means `latest_trading_day_on_or_before` mirroring `session_date()`'s
    OWN forward-looking non-trading-day semantics for `continuous`-cadence
    units, rather than deriving one independently via `is_trading_day`
    branching. Filed as an open finding on `alpha-engine-config-I10810` for
    the `data_gate` owner — NOT patched here."""
    from zoneinfo import ZoneInfo

    from data_gate.cadence import latest_trading_day_on_or_before
    from nousergon_lib.dates import session_date
    from nousergon_lib.trading_calendar import is_trading_day

    assert is_trading_day(calendar_date) is is_trading

    # The producer's own reference moment: 04:00 America/Los_Angeles on the
    # calendar day the systemd timer fires.
    local_run_time = datetime(
        calendar_date.year, calendar_date.month, calendar_date.day, 4, 0,
        tzinfo=ZoneInfo("America/Los_Angeles"),
    )
    producer_folder = session_date(local_run_time)
    reader_expected_folder = latest_trading_day_on_or_before(calendar_date)

    assert (producer_folder == reader_expected_folder) is agrees, (
        f"{calendar_date} ({'trading day' if is_trading else 'non-trading day'}): "
        f"producer folder (session_date) = {producer_folder}, "
        f"reader-expected folder (latest_trading_day_on_or_before) = "
        f"{reader_expected_folder} — expected agreement={agrees}"
    )


# ──────────────────────────── D37 — metron-intraday ─────────────────────────


@pytest.fixture
def metron(monkeypatch):
    from collectors import metron_market_data as module

    monkeypatch.setattr(module, "collect", lambda **kw: {"status": "ok"})  # noqa: ARG005
    return module


def test_d37_writes_one_manifest_per_tick_with_both_declared_keys(sink, metron, monkeypatch):
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "ok", "universe": 7, "quotes": 7,
        "indices": 4, "fund_proxies": 2, "ratings": 6,
    })

    assert metron.main(["--only-intraday", "--date", "2026-09-14"]) == 0

    manifest = sink.only
    assert manifest["unit_id"] == "D37"
    assert manifest["status"] == "ok"
    assert {(o["key"], o["rows_out"]) for o in manifest["outputs"]} == {
        ("market_data/intraday/latest.json", 13),
        ("market_data/intraday/technical_ratings.json", 6),
    }


def test_d37_default_trading_day_uses_session_date_not_last_closed(sink, metron, monkeypatch):
    """No ``--date`` (the real systemd-timer invocation): the manifest must key
    on the session `now` falls WITHIN, never the last one that has fully
    closed — `alpha-engine-config-I10810`, measured live: D37's manifests
    written all day Monday 2026-09-21 carried ``trading_day: "2026-09-18"``
    (three sessions stale) because ``default_run_date()`` resolves the
    as-of/knowledge axis (``last_closed_trading_day``), which doesn't advance
    to Monday until Monday's own 16:00 ET close. ``default_session_date()``
    (``nousergon_lib.dates.session_date()``) resolves the event-time axis
    instead, so a mid-session Monday tick keys under Monday."""
    import nousergon_lib.dates as lib_dates

    monkeypatch.setattr(sys, "argv", ["metron_market_data"])
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "ok", "universe": 3, "quotes": 3, "indices": 1, "fund_proxies": 0, "ratings": 3,
    })
    # The as-of axis (last CLOSED session) is stuck on Friday; the event-time
    # axis (the session `now` falls within) is Monday itself.
    monkeypatch.setattr(lib_dates, "last_closed_trading_day", lambda *_a, **_kw: date(2026, 9, 18))
    monkeypatch.setattr(lib_dates, "session_date", lambda *_a, **_kw: date(2026, 9, 21))

    assert metron.main(["--only-intraday"]) == 0

    manifest = sink.only
    assert sink.only_key.startswith("data_collection/runs/D37/2026-09-21/")
    assert manifest["trading_day"] == "2026-09-21"


def test_d37_off_session_tick_is_recorded_as_not_applicable(sink, metron, monkeypatch):
    """The timer fires ~288x/day; the non-runs leave a record, not silence.

    `alpha-engine-config-I10831` deliverable 1: this site now tags the
    precise `outside_session_window` member rather than the generic
    `no_new_data_declared` — the withholding shape below fails against the
    old tag."""
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "skipped", "reason": "outside US market window",
    })

    assert metron.main(["--only-intraday", "--date", "2026-09-14"]) == 0

    manifest = sink.only
    assert manifest["status"] == "not_applicable"
    assert manifest["reason"] == "outside_session_window"
    assert manifest["reason"] in run_units.run_manifest.NOT_APPLICABLE_REASONS
    assert manifest["outputs"] == []


def test_d37_an_unclassified_skip_reason_fails_loud(sink, metron, monkeypatch):
    """No default bucket: `collect_intraday`'s only two source-level skip
    reasons are "outside US market window" and "metron app inactive (no
    fresh UI heartbeat)" — the latter unreachable in production because
    metron-intraday.service's ExecStart never passes `--require-heartbeat`.
    Anything else is unclassified and fails loud rather than being tagged
    `outside_session_window` by default (`alpha-engine-config-I10831`,
    corrected 2026-09-15)."""
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "skipped", "reason": "metron app inactive (no fresh UI heartbeat)",
    })

    with pytest.raises(RuntimeError, match="unclassified reason"):
        metron.main(["--only-intraday", "--date", "2026-09-14"])

    manifest = sink.only
    assert manifest["status"] == "failed"


def test_d37_empty_fetch_refusal_is_a_failed_manifest(sink, metron, monkeypatch):
    """The 2026-07-29 class: a pandas-less venv wrote an empty artifact as ok."""
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "error", "error": "empty intraday fetch for indices,fund_proxies",
        "quotes": 0, "indices": 0, "fund_proxies": 0,
    })

    assert metron.main(["--only-intraday", "--date", "2026-09-14"]) == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "empty intraday fetch" in manifest["reason"]


def test_d37_exception_propagates_after_the_record_is_durable(sink, metron, monkeypatch):
    def _boom(**kw):  # noqa: ARG001
        raise RuntimeError("yfinance exploded")

    monkeypatch.setattr(metron, "collect_intraday", _boom)

    with pytest.raises(RuntimeError, match="yfinance exploded"):
        metron.main(["--only-intraday", "--date", "2026-09-14"])

    assert sink.only["status"] == "failed"


# ─────────────────────────── D39 — inst_ownership ───────────────────────────


class _Row:
    quarter = "2026Q1"
    ticker = "AAPL"
    n_funds_holding = 812
    total_shares_held = 1_234_567.0


@pytest.fixture
def inst_ownership(monkeypatch):
    from data.derived import inst_ownership as module

    monkeypatch.setattr(sys, "argv", ["inst_ownership", "--from-membership"])
    monkeypatch.setattr(module, "load_universe_from_membership", lambda **kw: ["AAPL", "MSFT"])  # noqa: ARG005
    fake_boto3 = type("_B", (), {"client": staticmethod(lambda *a, **kw: object())})
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    return module


def test_d39_writes_one_manifest_with_the_rows_that_landed(sink, inst_ownership, monkeypatch):
    rows = [_Row() for _ in range(137)]
    monkeypatch.setattr(inst_ownership, "compute_and_write_inst_ownership", lambda *a, **kw: rows)  # noqa: ARG005

    inst_ownership.main()

    manifest = sink.only
    assert manifest["unit_id"] == "D39"
    assert manifest["status"] == "ok"
    assert manifest["trigger"] == "gha"
    assert {(o["key"], o["rows_out"]) for o in manifest["outputs"]} == {
        ("data/inst_ownership/2026Q1/latest.parquet", 137),
        ("data/inst_ownership/latest.json", 137),
    }
    assert manifest["denominator"]["count"] == 2
    assert any("universe_membership" in i["key"] for i in manifest["inputs"])


def test_d39_empty_result_writes_a_failed_manifest_and_still_exits_1(sink, inst_ownership, monkeypatch):
    """`sys.exit(1)` is the producer's fail-loud contract; the record survives it."""
    monkeypatch.setattr(inst_ownership, "compute_and_write_inst_ownership", lambda *a, **kw: None)  # noqa: ARG005

    with pytest.raises(SystemExit) as exit_info:
        inst_ownership.main()
    assert exit_info.value.code == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert manifest["outputs"] == []


def test_d39_exception_propagates_after_the_record_is_durable(sink, inst_ownership, monkeypatch):
    def _boom(*a, **kw):  # noqa: ARG001
        raise RuntimeError("SEC download exploded")

    monkeypatch.setattr(inst_ownership, "compute_and_write_inst_ownership", _boom)

    with pytest.raises(RuntimeError, match="SEC download exploded"):
        inst_ownership.main()

    assert sink.only["status"] == "failed"


# ────────────────────────── D42 — ArcticDB migrations ───────────────────────


@pytest.fixture
def arctic_migrations():
    return _load_script(
        REPO_ROOT / "scripts" / "run_arctic_migrations.py", "_test_run_arctic_migrations"
    )


D42_ARGV = ["--merged-sha", "b" * 40, "--head-migration-number", "7"]


def test_d42_success_records_the_applied_chain_and_the_unmeasurable_rows(
    sink, arctic_migrations, monkeypatch,
):
    monkeypatch.setattr(arctic_migrations, "run", lambda args, run_ctx=None: _d42_success(run_ctx))

    assert arctic_migrations.main(D42_ARGV) == 0

    manifest = sink.only
    assert manifest["unit_id"] == "D42"
    assert manifest["status"] == "ok"
    assert manifest["trigger"] == "gha"
    # The merge sha this box was cloned at, not a `git rev-parse` of the laptop.
    assert manifest["code_sha"] == "b" * 40
    assert [(o["key"], o["rows_out"]) for o in manifest["outputs"]] == [
        ("overseer/_control/completed/arctic-migration-0007.json", 2)
    ]
    assert "unmeasurable" in {g["verdict"] for g in manifest["guards"]}


def _d42_success(run_ctx) -> int:
    run_ctx.record_output(
        "overseer/_control/completed/arctic-migration-0007.json", rows_out=2
    )
    run_ctx.record_guard(
        "data_empty_fresh", mode="observe", verdict="unmeasurable",
        detail="ArcticDB libraries report no row count",
    )
    return 0


def test_d42_nonzero_exit_writes_a_failed_manifest_and_keeps_the_code(
    sink, arctic_migrations, monkeypatch,
):
    monkeypatch.setattr(arctic_migrations, "run", lambda args, run_ctx=None: 1)

    assert arctic_migrations.main(D42_ARGV) == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "exited 1" in manifest["reason"]


def test_d42_exception_propagates_after_the_record_is_durable(
    sink, arctic_migrations, monkeypatch,
):
    def _boom(args, run_ctx=None):  # noqa: ARG001
        raise RuntimeError("arcticdb exploded")

    monkeypatch.setattr(arctic_migrations, "run", _boom)

    with pytest.raises(RuntimeError, match="arcticdb exploded"):
        arctic_migrations.main(D42_ARGV)

    assert sink.only["status"] == "failed"


def test_d42_completion_marker_is_only_recorded_when_its_put_landed(arctic_migrations):
    """A manifest listing an object the run failed to write is worse than none."""

    class _Boom:
        def put_object(self, **kw):  # noqa: ARG002
            raise RuntimeError("s3 down")

    import types

    fake_boto3 = types.SimpleNamespace(client=lambda *a, **kw: _Boom())
    original = sys.modules.get("boto3")
    sys.modules["boto3"] = fake_boto3
    try:
        landed = arctic_migrations.write_completion_marker(
            bucket="b", region="us-east-1", head_migration_number=7, payload={},
        )
    finally:
        if original is None:
            del sys.modules["boto3"]
        else:
            sys.modules["boto3"] = original
    assert landed is False


# ──────────────────────────── D38 — crypto-balances ─────────────────────────


@pytest.fixture
def crypto_handler(monkeypatch):
    """The Lambda handler, imported the way its deployed package lays out."""
    lambda_dir = REPO_ROOT / "infrastructure" / "lambdas" / "crypto-balances"
    monkeypatch.syspath_prepend(str(REPO_ROOT / "collectors"))
    monkeypatch.setenv("CRYPTO_BALANCES_ENABLED", "true")
    monkeypatch.setenv("MARKET_DATA_BUCKET", "alpha-engine-research")
    return _load_script(lambda_dir / "index.py", "_test_crypto_balances_index")


def test_d38_writes_one_manifest_with_the_balances_that_landed(sink, crypto_handler, monkeypatch):
    monkeypatch.setattr(crypto_handler.crypto_balances, "collect", lambda **kw: {  # noqa: ARG005
        "status": "ok", "n_balances": 4, "n_failed": 1,
    })

    out = crypto_handler.handler({}, None)
    assert out["statusCode"] == 200

    manifest = sink.only
    assert manifest["unit_id"] == "D38"
    assert manifest["status"] == "ok"
    assert [(o["key"], o["rows_out"]) for o in manifest["outputs"]] == [("crypto/holdings.json", 4)]
    # The soft per-address failures become a number somebody can trend.
    assert manifest["rows_rejected"] == [{"reason": "address_fetch_failed", "count": 1}]


def test_d38_systemic_failure_records_then_reraises(sink, crypto_handler, monkeypatch):
    monkeypatch.setattr(crypto_handler.crypto_balances, "collect", lambda **kw: {  # noqa: ARG005
        "status": "error", "n_failed": 3,
    })

    with pytest.raises(RuntimeError, match="crypto-balances run failed"):
        crypto_handler.handler({}, None)

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "crypto-balances run failed" in manifest["reason"]


def test_d38_kill_switch_writes_no_manifest(sink, crypto_handler, monkeypatch):
    """A disabled producer must not leave a record claiming it executed."""
    monkeypatch.setattr(crypto_handler, "ENABLED", False)

    out = crypto_handler.handler({}, None)

    assert out["body"]["status"] == "disabled"
    assert sink.writes == []


def test_d38_unmeasurable_code_sha_runs_the_collector_unrecorded(
    crypto_handler, monkeypatch, caplog,
):
    """The record layer never decides whether a producer produces."""
    monkeypatch.delenv("NE_DATA_CODE_SHA", raising=False)
    monkeypatch.setattr(
        run_units.run_manifest,
        "resolve_code_sha",
        lambda *a, **kw: (_ for _ in ()).throw(
            run_units.run_manifest.CodeShaError("no git here")
        ),
    )
    ran: list[bool] = []

    def _collect(**kw):  # noqa: ARG001
        ran.append(True)
        return {"status": "ok", "n_balances": 2, "n_failed": 0}

    monkeypatch.setattr(crypto_handler.crypto_balances, "collect", _collect)

    out = crypto_handler.handler({}, None)

    assert ran == [True], "the collector must still run when no manifest can be written"
    assert out["statusCode"] == 200


# ───────────────────────── D16 — rag-weekly-ingestion ────────────────────────
#
# `alpha-engine-config-I10862`: D16's pipeline is a bash script
# (`run_weekly_ingestion.sh`) run as a subprocess by
# `rag.pipelines.run_weekly_ingestion_recorded`, which never reimplements it
# and never parses its logs — it reads back the real S3 objects the script's
# own steps wrote. These tests fake both boundaries: the subprocess exit code
# and the S3 objects a real run would have left behind.


class _FakeS3:
    """A minimal ``list_objects_v2``/``get_object`` double keyed by object.

    ``objects`` maps a full S3 key to ``(body_bytes, last_modified)``. No
    pagination — every fixture here is well under 1000 keys.
    """

    def __init__(self, objects: dict[str, tuple[bytes, datetime]]) -> None:
        self.objects = objects

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        return {
            "Contents": [
                {"Key": key, "LastModified": last_modified}
                for key, (_, last_modified) in self.objects.items()
                if key.startswith(Prefix)
            ],
            "IsTruncated": False,
        }

    def get_object(self, Bucket, Key):  # noqa: N803
        body, _ = self.objects[Key]
        return {"Body": io.BytesIO(body)}


@pytest.fixture
def d16(monkeypatch):
    from rag.pipelines import run_weekly_ingestion_recorded as module

    since = datetime(2026, 9, 19, 6, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(module, "_utcnow", lambda: since)
    monkeypatch.setattr(sys, "argv", ["run_weekly_ingestion_recorded", "--date", "2026-09-19"])
    return module, since


def _d16_fixture_objects(after):
    """Every declared D16 output, timestamped just after ``after`` (the run's
    own "since" boundary) so `_iter_new_keys` picks all of them up. Also
    includes D46's declared write key (alpha-engine-config-I10753: D46 has no
    dispatcher entry point of its own and is graded against this same
    manifest — see D46's `run_manifest_prefix` and this module's
    `OUTPUT_PREFIXES`)."""
    ts = after + timedelta(seconds=5)
    manifest_body = json.dumps({"totals": {"documents": 1234, "chunks": 9000, "tickers": 640}}).encode()
    filing_body = json.dumps({"n_analyzed": 42, "n_lazy": 3}).encode()
    return {
        "rag/manifest/2026-09-19.json": (manifest_body, ts),
        "rag/manifest/latest.json": (manifest_body, ts),
        "rag/watermarks/v1/sec_edgar.json": (
            json.dumps({"AAPL::10-K": "2026-09-19T00:00:00Z", "MSFT::10-K": "2026-09-19T00:00:00Z"}).encode(),
            ts,
        ),
        "rag/filing_changes/2026-09-19.json": (filing_body, ts),
        "rag/filing_changes/latest.json": (filing_body, ts),
        "rag/corpus_freshness/latest.json": (json.dumps({"status": "fresh"}).encode(), ts),
        "health/rag_ingestion_progress/2026-09-19.json": (json.dumps({"step": 10, "of": 10}).encode(), ts),
        "data/insider_transactions/2609190500_result.parquet": (b"PAR1-fake-parquet-bytes", ts),
        "data/insider_transactions/latest.json": (json.dumps({"rows": 17}).encode(), ts),
    }


def test_d16_writes_one_manifest_with_measured_outputs(sink, d16, monkeypatch):
    module, since = d16
    fake_s3 = _FakeS3(_d16_fixture_objects(since))
    monkeypatch.setattr(module, "_run_ingestion_script", lambda dry_run: 0)  # noqa: ARG005
    monkeypatch.setattr(module, "_s3_client", lambda: fake_s3)

    assert module.main() == 0

    manifest = sink.only
    assert sink.only_key.startswith("data_collection/runs/D16/2026-09-19/")
    assert manifest["unit_id"] == "D16"
    assert manifest["status"] == "ok"
    assert manifest["trigger"] == "scheduled"
    assert manifest["trading_day"] == "2026-09-19"
    keyed = {o["key"]: o["rows_out"] for o in manifest["outputs"]}
    assert keyed == {
        "rag/manifest/2026-09-19.json": 1234,
        "rag/manifest/latest.json": 1234,
        "rag/watermarks/v1/sec_edgar.json": 2,
        "rag/filing_changes/2026-09-19.json": 42,
        "rag/filing_changes/latest.json": 42,
        "rag/corpus_freshness/latest.json": 1,
        "health/rag_ingestion_progress/2026-09-19.json": 1,
        "data/insider_transactions/2609190500_result.parquet": 1,
        "data/insider_transactions/latest.json": 1,
    }
    verdicts = {g["verdict"] for g in manifest["guards"]}
    assert "ok" in verdicts


def test_d16_script_failure_writes_a_failed_manifest_and_keeps_the_exit_code(sink, d16, monkeypatch):
    module, since = d16
    fake_s3 = _FakeS3({})  # nothing published — the script died before step 10
    monkeypatch.setattr(module, "_run_ingestion_script", lambda dry_run: 1)  # noqa: ARG005
    monkeypatch.setattr(module, "_s3_client", lambda: fake_s3)

    assert module.main() == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "exited 1" in manifest["reason"]
    assert manifest["outputs"] == []
    verdicts = {g["verdict"] for g in manifest["guards"]}
    assert "empty_fresh" in verdicts


def test_d16_exit_zero_with_no_published_output_is_a_failed_manifest(sink, d16, monkeypatch):
    """The withholding shape for a script that exits 0 but silently wrote
    nothing — indistinguishable from a real failure to every downstream
    reader unless it is graded the same way."""
    module, since = d16
    fake_s3 = _FakeS3({})
    monkeypatch.setattr(module, "_run_ingestion_script", lambda dry_run: 0)  # noqa: ARG005
    monkeypatch.setattr(module, "_s3_client", lambda: fake_s3)

    assert module.main() == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "published no output" in manifest["reason"]
    assert manifest["outputs"] == []


def test_d16_dry_run_writes_no_manifest(d16, monkeypatch):
    module, since = d16
    monkeypatch.setattr(module, "_run_ingestion_script", lambda dry_run: 0)  # noqa: ARG005
    monkeypatch.setattr(sys, "argv", ["run_weekly_ingestion_recorded", "--date", "2026-09-19", "--dry-run"])
    monkeypatch.setenv("NE_DATA_CODE_SHA", FAKE_SHA)

    # sink=None on a dry run (write=False) — run_manifest.run_unit logs one
    # line in its place and calls no sink at all, so no S3 client is needed.
    assert module.main() == 0


def test_d16_dispatcher_workload_calls_the_recorded_entrypoint():
    """The dispatcher no longer runs the bare bash script directly."""
    dispatcher = _load_script(
        REPO_ROOT / "infrastructure" / "lambdas" / "data-spot-dispatcher" / "index.py",
        "_test_data_spot_dispatcher_index",
    )

    command = dispatcher._WORKLOADS["rag-weekly-ingestion"]
    assert "rag.pipelines.run_weekly_ingestion_recorded" in command
    assert "bash rag/pipelines/run_weekly_ingestion.sh" not in command


# ─────────────────── D14 — prune_delisted_tickers (I11001) ───────────────────
#
# Unlike D36/D37/D38/D39/D42 this unit is not its own process: it is the second
# half of the `weekly-phase-one` dispatcher workload
# (`( python weekly_collector.py --phase 1 && python -m
# builders.prune_delisted_tickers --apply )`). It still has to satisfy the same
# property — its descriptor declares `run_manifest_prefix:
# data_collection/runs/D14`, and until alpha-engine-config-I11001 nothing wrote
# it, so the weekly schedule could not name D14 in `verify_units` without
# failing every run on a manifest that never existed.


def _prune_summary(**over) -> dict:
    summary = {
        "applied": True,
        "trading_day": "2026-09-18",
        "pruned_count": 3,
        "retained_count": 2,
        "retained": ["AAA", "BBB"],
        "audit_key": "builders/prune_audit/2026-09-18-2026-09-19T010203Z-apply.json",
    }
    summary.update(over)
    return summary


@pytest.fixture
def prune(monkeypatch):
    from builders import prune_delisted_tickers as module

    monkeypatch.setattr(sys, "argv", ["prune_delisted_tickers", "--apply"])
    monkeypatch.setattr(module, "default_run_date", lambda: "2026-09-18")
    return module


def test_d14_writes_one_manifest_keyed_on_the_day_the_audit_key_embeds(sink, prune, monkeypatch):
    seen: dict = {}

    def _fake(**kwargs):
        seen.update(kwargs)
        return _prune_summary()

    monkeypatch.setattr(prune, "prune_delisted_tickers", _fake)

    assert prune.main() == 0

    # The prune and the manifest must agree about the day, or the completion
    # check renders `builders/prune_audit/{trading_day}-*.json` against a date
    # the key it is looking for does not carry.
    assert seen["trading_day"] == "2026-09-18"
    manifest = sink.only
    assert sink.only_key.startswith("data_collection/runs/D14/2026-09-18/")
    assert manifest["unit_id"] == "D14"
    assert manifest["status"] == "ok"
    assert manifest["trading_day"] == "2026-09-18"
    keys = {o["key"]: o["rows_out"] for o in manifest["outputs"]}
    assert keys["builders/prune_audit/2026-09-18-2026-09-19T010203Z-apply.json"] == 3
    assert keys["delisted_history::AAA"] == 1
    assert keys["delisted_history::BBB"] == 1


def test_d14_zero_prune_week_is_still_a_clean_ok(sink, prune, monkeypatch):
    """A week with nothing to prune is the NORMAL case, not an empty run: the
    audit object is written unconditionally and is the key being graded."""
    monkeypatch.setattr(
        prune, "prune_delisted_tickers",
        lambda **kw: _prune_summary(pruned_count=0, retained_count=0, retained=[]),  # noqa: ARG005
    )

    assert prune.main() == 0

    manifest = sink.only
    assert manifest["status"] == "ok"
    assert [o["key"] for o in manifest["outputs"]] == [
        "builders/prune_audit/2026-09-18-2026-09-19T010203Z-apply.json"
    ]
    assert {g["verdict"] for g in manifest["guards"]} == {"ok"}


def test_d14_a_failed_audit_put_is_a_failed_manifest_not_a_silent_ok(sink, prune, monkeypatch):
    """The audit object is the only S3 key this unit publishes. Losing it means
    the run produced nothing gradable, which must not record as `ok`."""
    monkeypatch.setattr(
        prune, "prune_delisted_tickers", lambda **kw: _prune_summary(audit_key=None),  # noqa: ARG005
    )

    assert prune.main() == 1

    manifest = sink.only
    assert manifest["status"] == "failed"
    assert "empty_fresh" in {g["verdict"] for g in manifest["guards"]}
    assert [o["key"] for o in manifest["outputs"]] == ["delisted_history::AAA", "delisted_history::BBB"]


def test_d14_exception_propagates_after_the_record_is_durable(sink, prune, monkeypatch):
    def _boom(**kw):  # noqa: ARG001
        raise RuntimeError("arctic exploded")

    monkeypatch.setattr(prune, "prune_delisted_tickers", _boom)

    with pytest.raises(RuntimeError, match="arctic exploded"):
        prune.main()

    assert sink.only["status"] == "failed"
