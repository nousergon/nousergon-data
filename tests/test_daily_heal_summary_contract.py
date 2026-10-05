"""Producer contract test for daily_heal_summary.schema.json (D33 daily-heal,
alpha-engine-config-I10933).

Same pattern as the other P-07 contract tests: the record the REAL producer
(``weekly_collector._run_daily_heal``) writes to ``data/heal/daily/{date}.json``
validates against its own versioned schema, checked at PR time. It also checks
three real records copied from ``s3://alpha-engine-research/data/heal/daily/``
(the 2026-07-16 to 2026-08-04 runs, the last time the unit ran): a no-op, a
healed day and a refused heal. A producer bug that matched its own wrong output
could not also pass those.

D33's second published key, ``staging/daily_closes/{day}.parquet``, is written
through the same ``collectors.daily_closes`` path D17 uses, so it is governed by
``staging_daily_closes.schema.json`` and ``tests/test_staging_daily_closes_contract.py``.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import weekly_collector
from contracts import validate_daily_heal_summary

pytest.importorskip("jsonschema")

_FIXTURES = Path(__file__).parent / "fixtures" / "daily_heal"


def _run(universe_heal):
    """Drive the real producer with both sub-heals and AWS stubbed; return the
    exact body it PUT to data/heal/daily/{date}.json."""
    puts = []

    class _S3:
        def put_object(self, **kwargs):
            puts.append(kwargs)

    class _CW:
        def put_metric_data(self, **kwargs):
            pass

    def _client(name):
        return {"s3": _S3(), "cloudwatch": _CW()}[name]

    side = {"side_effect": universe_heal} if isinstance(universe_heal, Exception) else {"return_value": universe_heal}
    with patch("weekly_collector.boto3.client", side_effect=_client), patch(
        "weekly_collector._self_heal_missing_universe_days", **side
    ):
        weekly_collector._run_daily_heal({"bucket": "b"}, SimpleNamespace(date="2026-10-02", dry_run=True))
    [put] = puts
    assert put["Key"] == "data/heal/daily/2026-10-02.json"
    return json.loads(put["Body"])


def _summary(**overrides):
    base = {
        "scan_window_td": 5,
        "max_per_run": 1,
        "missing_days": [],
        "fallback_quality_days": [],
        "ledger_days": [],
        "healed_days": [],
        "deferred_days": [],
        "errors": [],
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize("path", sorted(_FIXTURES.glob("*.json")), ids=lambda p: p.stem)
def test_every_real_heal_record_validates(path):
    assert validate_daily_heal_summary(json.loads(path.read_text())) == []


def test_the_real_fixtures_cover_noop_healed_and_refused():
    records = [json.loads(p.read_text()) for p in _FIXTURES.glob("*.json")]
    ugh = [r["collectors"]["universe_gap_heal"] for r in records]
    assert any(not u["healed_days"] and not u["errors"] for u in ugh), "no no-op fixture"
    assert any(u["healed_days"] for u in ugh), "no healed fixture"
    assert any(u["errors"] for u in ugh), "no refused fixture"


def test_a_noop_run_validates():
    body = _run(_summary())
    assert body["days_healed"] == 0
    assert validate_daily_heal_summary(body) == []


def test_a_healing_run_validates():
    body = _run(
        _summary(
            ledger_days=["2026-10-01"],
            healed_days=[{"date": "2026-10-01", "kind": "ledger", "tickers": 929, "weekly_date": "2026-09-26"}],
        )
    )
    assert body["days_healed"] == 1
    assert validate_daily_heal_summary(body) == []


def test_a_refused_heal_validates():
    body = _run(_summary(missing_days=["2026-10-01"], errors=[{"date": "2026-10-01", "kind": "missing", "reason": "x"}]))
    assert validate_daily_heal_summary(body) == []


def test_a_crashed_universe_heal_validates():
    """The producer records an unexpected heal exception as status=error; the
    summary is still written and still conforms."""
    body = _run(RuntimeError("boom"))
    assert body["collectors"]["universe_gap_heal"] == {"status": "error", "error": "boom"}
    assert validate_daily_heal_summary(body) == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r.pop("days_healed"),
        lambda r: r.update(days_healed=-1),
        lambda r: r.update(mode="daily"),
        lambda r: r.update(date="10/02/2026"),
        lambda r: r["collectors"].pop("universe_gap_heal"),
        lambda r: r["collectors"]["universe_gap_heal"]["healed_days"].append({"date": "2026-10-01", "kind": "guess"}),
        lambda r: r["collectors"]["universe_gap_heal"].pop("errors"),
    ],
    ids=["no-days_healed", "negative", "wrong-mode", "bad-date", "no-universe-heal", "bad-healed-entry", "ok-without-errors"],
)
def test_the_schema_refuses_a_drifted_record(mutate):
    record = json.loads((_FIXTURES / "2026-08-04.json").read_text())
    mutate(record)
    assert validate_daily_heal_summary(record), "schema accepted a drifted record"
