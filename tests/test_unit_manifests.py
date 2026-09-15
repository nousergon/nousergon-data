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
import json
import sys
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


def test_d37_off_session_tick_is_recorded_as_not_applicable(sink, metron, monkeypatch):
    """The timer fires ~288x/day; the non-runs leave a record, not silence."""
    monkeypatch.setattr(metron, "collect_intraday", lambda **kw: {  # noqa: ARG005
        "status": "skipped", "reason": "outside US market window",
    })

    assert metron.main(["--only-intraday", "--date", "2026-09-14"]) == 0

    manifest = sink.only
    assert manifest["status"] == "not_applicable"
    assert manifest["reason"] in run_units.run_manifest.NOT_APPLICABLE_REASONS
    assert manifest["outputs"] == []


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
