"""Unit tests for data_gate/producers/pages_monthly.py (alpha-engine-config-I10788
A9, plan P-22): the ``data.pages.monthly`` producer.

Covers: which mirror entries count as this component's page (each of the plan
§2 row 11 conditions, and the near-misses that must not count), that the
matchers are DERIVED from the repository (the collection ASL, the unit
descriptors, the pre-open definition) rather than hand-listed, that a day with
no ledger entry is unobserved rather than a quiet day, that a read failure
raises instead of producing a verdict, and that the document reads through
the real clause.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import pytest

from data_gate import clauses as clause_module
from data_gate.descriptors import load_units
from data_gate.producers import pages_monthly as m
from data_gate.store import LocalStore

UTC = dt.timezone.utc
REPO = pathlib.Path(__file__).resolve().parents[1]
TOPIC_ARN = "arn:aws:sns:us-east-1:111122223333:alpha-engine-alerts"
NOTIFY = frozenset({"Nous Ergon data collection FAILED", "Nous Ergon data collection COMPLETION CHECK FAILED"})
PATTERNS = ("data_collection/*", "market_data/technicals/latest.json", "staging/daily_closes/*.parquet")


def _entry(subject: str, description: str = "", *, ts: str = "2026-10-01T12:00:00Z", topic: str = TOPIC_ARN) -> dict:
    return {
        "event_id": f"{ts}_{subject[:8]}",
        "ts_utc": ts,
        "source": "sns-mirror",
        "description": description,
        "sns": {"subject": subject, "topic_arn": topic, "message_id": "x"},
    }


def _alarm(name: str, state: str = "ALARM") -> dict:
    return _entry(
        f'{"ALARM" if state == "ALARM" else "OK"}: "{name}" in US East (N. Virginia)',
        json.dumps({"AlarmName": name, "NewStateValue": state, "OldStateValue": "OK"}),
    )


def _freshness(*artifacts: tuple[str, str]) -> dict:
    lines = ["[CRITICAL] freshness-monitor: n artifact(s) past SLA", "", "[owner_repo:x] 1 artifact(s)"]
    lines += [f"  - artifact_id={a} owner_repo=x state=stale key={k} sla_violated_by_minutes=5" for a, k in artifacts]
    return _entry("Alpha Engine alert [CRITICAL] — freshness-monitor", "\n".join(lines))


def _classify(entry: dict):
    return m.classify_entry(entry, notify_subjects=NOTIFY, key_patterns=PATTERNS)


# ---------------------------------------------------------------------------
# The matchers come from the repository.
# ---------------------------------------------------------------------------


def test_notify_subjects_are_read_from_the_collection_definition():
    subjects = m.collection_notify_subjects()
    assert "Nous Ergon data collection FAILED" in subjects
    assert "Nous Ergon data collection COMPLETION CHECK FAILED" in subjects


def test_a_definition_that_pages_on_nothing_raises(tmp_path):
    path = tmp_path / "asl.json"
    path.write_text(json.dumps({"States": {"Done": {"Type": "Succeed"}}}))
    with pytest.raises(ValueError, match="no sns:publish Subject"):
        m.collection_notify_subjects(path)


def test_preopen_subject_is_the_one_the_preopen_pipeline_publishes():
    """Condition 4 is matched on a literal; renaming it in the pre-open
    definition must fail here rather than silently drop the condition."""
    definition = json.loads((REPO / "infrastructure" / "step_function_daily.json").read_text())
    subjects = []

    def walk(states):
        for state in states.values():
            params = state.get("Parameters") or {}
            if isinstance(params, dict) and isinstance(params.get("Subject"), str):
                subjects.append(params["Subject"])
            for branch in state.get("Branches") or []:
                walk(branch["States"])

    walk(definition["States"])
    assert m.PREOPEN_NOT_READY_SUBJECT in subjects


def test_component_keys_come_from_the_descriptors():
    units = load_units()
    patterns = m.component_key_patterns(units)
    assert "data_collection/*" in patterns
    assert "market_data/technicals/latest.json" in patterns  # D25, a Metron-read key
    assert "staging/daily_closes/*.parquet" in patterns  # D17/D19, {date} templated
    assert not any(p.startswith("arcticdb/") or " " in p or "::" in p for p in patterns)
    retired_only = {
        str(w)
        for u in units
        if u.retired
        for w in u.raw.get("writes") or []
        if not any(str(w) in (x.raw.get("writes") or []) for x in units if not x.retired)
    }
    assert not retired_only & set(patterns)


# ---------------------------------------------------------------------------
# What counts as a page.
# ---------------------------------------------------------------------------


def test_condition_1_alarm_counts_only_on_entering_alarm():
    assert _classify(_alarm("ne-data-collection-eod-failed")).page_class == "execution_failed_alarm"
    assert _classify(_alarm("ne-data-collection-eod-failed", state="OK")) is None
    assert _classify(_alarm("alpha-engine-weekday-sf-failed")) is None


def test_condition_1_state_machine_notify_counts():
    event = _classify(_entry("Nous Ergon data collection FAILED", "Data collection morning failed."))
    assert event.page_class == "execution_failed_notify"


def test_condition_4_preopen_not_ready_counts():
    assert _classify(_entry(m.PREOPEN_NOT_READY_SUBJECT)).page_class == "preopen_not_ready"


def test_freshness_page_counts_only_when_it_names_a_component_artifact():
    ours = _classify(
        _freshness(
            ("zero_job_run_health", "ops/zero_job_run_health/2026-10-05.json"),
            ("data_collection_gate_ladder", "data_collection/gates/data-phase1/2026-10-05/gate.json"),
            ("technicals", "market_data/technicals/latest.json"),
        )
    )
    assert ours.page_class == "freshness_deadline_missed"
    assert ours.artifacts == ("data_collection_gate_ladder", "technicals")
    assert _classify(_freshness(("pr_resting_state_trend", "ops/pr_resting_state_trend/latest.json"))) is None


def test_other_components_messages_do_not_count():
    assert _classify(_entry("Alpha Engine Weekday Pipeline — FAILED")) is None
    assert _classify(_entry("Alpha Engine alert [INFO] — research:thinktank_daily")) is None


# ---------------------------------------------------------------------------
# Reading the ledger.
# ---------------------------------------------------------------------------


class _FakeReader:
    def __init__(self, objects: dict[str, dict | bytes]):
        self.objects = objects
        self.gets: list[str] = []

    def list_objects(self, bucket, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def get_object(self, bucket, key):
        self.gets.append(key)
        value = self.objects[key]
        return value if isinstance(value, bytes) else json.dumps(value).encode()


def _key(day: str, name: str, prefix: str = "changelog/entries/") -> str:
    return f"{prefix}{day}/{day}T12-00-00_{name}.json"


def _read(reader, *, through="2026-10-03"):
    return m.read_month(
        reader,
        bucket="b",
        month_start=dt.date(2026, 10, 1),
        through=dt.date.fromisoformat(through),
        notify_subjects=NOTIFY,
        key_patterns=PATTERNS,
    )


def test_read_month_counts_pages_and_names_unobserved_days():
    reader = _FakeReader(
        {
            _key("2026-10-01", "alpha-engine-alerts_a1"): _alarm("ne-data-collection-morning-failed"),
            _key("2026-10-01", "alpha-engine-alerts_a2"): _entry("Alpha Engine alert [INFO] — x"),
            # deploy changelog entries share the folder; never read
            _key("2026-10-01", "nousergon-data_d1"): b"not json",
            # a quarantined page is still a delivered page
            _key("2026-10-03", "alpha-engine-alerts_q1", "changelog/quarantine/"): _entry(
                "Nous Ergon data collection FAILED", ts="2026-10-03T12:00:00Z"
            ),
        }
    )
    reading = _read(reader)
    assert [p.page_class for p in reading.pages] == ["execution_failed_alarm", "execution_failed_notify"]
    assert reading.observed_days == ["2026-10-01", "2026-10-03"]
    assert reading.unobserved_days == ["2026-10-02"]
    assert reading.days_in_month == 31 and reading.days_read == 3 and not reading.complete
    assert not any("nousergon-data" in k for k in reader.gets)


def test_an_entry_from_another_topic_is_not_counted():
    reader = _FakeReader(
        {_key("2026-10-01", "alpha-engine-alerts_a1"): _entry("Nous Ergon data collection FAILED", topic="arn:aws:sns:us-east-1:1:other")}
    )
    assert _read(reader, through="2026-10-01").pages == []


def test_an_unreadable_entry_raises_rather_than_counting_zero():
    reader = _FakeReader({_key("2026-10-01", "alpha-engine-alerts_a1"): b"{truncated"})
    with pytest.raises(ValueError):
        _read(reader, through="2026-10-01")


def test_a_closed_month_reads_through_its_last_day():
    reader = _FakeReader({})
    reading = m.read_month(
        reader,
        bucket="b",
        month_start=dt.date(2026, 9, 1),
        through=dt.date(2026, 10, 2),
        notify_subjects=NOTIFY,
        key_patterns=PATTERNS,
    )
    assert reading.complete and reading.days_read == 30 and len(reading.unobserved_days) == 30


# ---------------------------------------------------------------------------
# The document and the clause that reads it.
# ---------------------------------------------------------------------------


def _reading_with(n_pages: int) -> m.MonthReading:
    return m.MonthReading(
        month="2026-10",
        days_in_month=31,
        days_read=5,
        observed_days=[f"2026-10-0{d}" for d in range(1, 6)],
        pages=[m.PageEvent(f"2026-10-01T0{i}:00:00Z", "execution_failed_alarm", "s", f"e{i}") for i in range(n_pages)],
    )


NOW = dt.datetime(2026, 10, 5, 22, 0, tzinfo=UTC)


@pytest.mark.parametrize(("pages", "status"), [(0, "ok"), (2, "ok"), (3, "breach")])
def test_budget_is_the_plans_two_pages(pages, status):
    doc = m.build_document(_reading_with(pages), now=NOW)
    assert doc["status"] == status and doc["value"] == pages
    assert doc["target"]["max_pages_per_month"] == 2
    assert doc["days_observed"] == 5 and doc["days_in_month"] == 31
    assert doc["by_class"]["execution_failed_alarm"] == pages
    assert doc["stale_after_utc"] == "2026-10-08T00:00:00Z"
    assert doc["vendor_outage_exclusions"]["excluded_pages"] == 0


def _clause_over(tmp_path, document):
    target = tmp_path / "metrics" / "pages" / "monthly" / "latest.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(document))
    return clause_module._clause_pages_monthly(LocalStore(tmp_path))


def test_the_clause_reads_an_ok_month_as_a_partial_window(tmp_path):
    clause = _clause_over(tmp_path, m.build_document(_reading_with(1), now=NOW))
    assert clause.met
    assert clause.window_observed == 5 and clause.window_required == 31 and not clause.window_complete


def test_the_clause_reads_an_over_budget_month_as_reopened(tmp_path):
    clause = _clause_over(tmp_path, m.build_document(_reading_with(3), now=NOW))
    assert not clause.met and "REOPENED" in clause.detail


def test_closed_previous_month_is_rewritten_early_in_the_month():
    reader = _FakeReader({})
    early = m.compute_documents(reader, bucket="b", key=m.DEFAULT_KEY, now=dt.datetime(2026, 11, 2, 22, tzinfo=UTC))
    assert set(early) == {m.DEFAULT_KEY, "data_collection/metrics/pages/monthly/2026-10.json"}
    assert early["data_collection/metrics/pages/monthly/2026-10.json"]["month_complete"] is True
    later = m.compute_documents(reader, bucket="b", key=m.DEFAULT_KEY, now=dt.datetime(2026, 11, 4, 22, tzinfo=UTC))
    assert set(later) == {m.DEFAULT_KEY}


def test_main_writes_documents_and_run_record_and_logs_counts_only(monkeypatch, capsys):
    puts: dict[str, dict] = {}

    class _Client:
        def get_paginator(self, _name):
            class _P:
                def paginate(self, Bucket, Prefix):
                    if Prefix == "changelog/entries/2026-10-01/":
                        return [{"Contents": [{"Key": _key("2026-10-01", "alpha-engine-alerts_a1")}]}]
                    return [{}]

            return _P()

        def get_object(self, Bucket, Key):
            class _Body:
                def read(self):
                    return json.dumps(_entry("Nous Ergon data collection FAILED", "secret detail")).encode()

            return {"Body": _Body()}

        def put_object(self, Bucket, Key, Body, ContentType):
            puts[Key] = json.loads(Body)

    import boto3

    monkeypatch.setattr(boto3, "client", lambda *_a, **_k: _Client())

    class _Clock(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2026, 10, 7, 22, 0, tzinfo=UTC)

    monkeypatch.setattr(m.dt, "datetime", _Clock)
    assert m.main([]) == 0
    doc = puts[m.DEFAULT_KEY]
    assert doc["status"] == "ok" and doc["value"] == 1 and doc["days_observed"] == 1
    assert puts["data_collection/runs/pages_monthly/2026-10-07.json"]["status"] == "ok"
    out = capsys.readouterr().out
    assert "secret detail" not in out and "pages=1" in out
