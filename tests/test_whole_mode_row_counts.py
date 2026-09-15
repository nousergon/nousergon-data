"""The whole-mode units report a REAL row count, or an honest `unmeasurable`.

`alpha-engine-config-I10810` deliverable 2. Five run modes ARE one audit unit end
to end (D17 morning enrich, D18/D32 the ArcticDB appends, D33 daily heal, D34
chronic-gap heal). Until now each recorded a blanket `unmeasurable` guard verdict
with `rows_out: 0`, on the stated ground that they "publish through ArcticDB and
per-symbol keys and report no row count".

They do report one. `builders.daily_append.daily_append` returns
`tickers_appended` alongside a named count for every class of ticker it did NOT
append, and both heal paths return the list of what they healed. What was missing
was a DECLARED place to read it from — which is what `run_units.MODE_ROWS` is,
for the same reason `PhaseUnit.rows_key` is one level down: the writers do not
agree on a key, and guessing one either invents counts or hides them.

The property these tests defend is the asymmetry: a real count is recorded as a
real count, and a MISSING one is recorded as `unmeasurable` — never as 0. A
missing key read as zero would report every one of these units as an
empty-but-fresh write; read as fine, it would report none of them.
"""

from __future__ import annotations

import json

import pytest

import run_units
import weekly_collector


class FakeS3:
    def __init__(self) -> None:
        self.puts: list[tuple[str, dict]] = []

    def put_object(self, Bucket, Key, Body, ContentType=None, **kw):  # noqa: N803
        raw = Body.decode("utf-8") if isinstance(Body, (bytes, bytearray)) else Body
        self.puts.append((Key, json.loads(raw)))
        return {"ETag": '"abc"'}


@pytest.fixture(autouse=True)
def _measured_environment(monkeypatch):
    monkeypatch.setenv("NE_DATA_CODE_SHA", "c" * 40)
    monkeypatch.setenv("NE_DATA_LOG_LOCATION", "cloudwatch:/alpha-engine/data-spot:s-1")
    monkeypatch.setenv("NE_DATA_TRIGGER", "scheduled")


class _Args:
    def __init__(self, **kw):
        self.date = "2026-09-14"
        self.dry_run = False
        for k, v in kw.items():
            setattr(self, k, v)


def _run(monkeypatch, mode: str, result: dict):
    s3 = FakeS3()
    monkeypatch.setattr(run_units, "manifest_sink", lambda bucket, s3_client=None: _Sink(s3))
    out = weekly_collector._run_whole_mode_unit(
        mode, lambda config, args: result, {"bucket": "alpha-engine-research"}, _Args()
    )
    manifests = [body for key, body in s3.puts if key.startswith("data_collection/runs/")]
    assert len(manifests) == 1, manifests
    return out, manifests[0]


class _Sink:
    def __init__(self, s3: FakeS3) -> None:
        self.s3 = s3

    def write(self, key: str, payload: bytes) -> str | None:
        self.s3.put_object(Bucket="b", Key=key, Body=payload)
        return None


# ── A real count is recorded as a real count ─────────────────────────────────


def test_an_arctic_append_records_its_measured_ticker_count(monkeypatch):
    out, m = _run(
        monkeypatch,
        "daily_arctic_append",
        {
            "status": "ok",
            "collectors": {
                "arcticdb": {
                    "status": "ok",
                    "tickers_appended": 891,
                    "tickers_errored": 2,
                    "tickers_quality_blocked": 5,
                }
            },
        },
    )
    assert out["status"] == "ok"
    assert m["unit_id"] == "D32"
    assert m["status"] == "ok"
    assert m["rows_out"] == 891
    assert [o["rows_out"] for o in m["outputs"]] == [891]
    # The counts of what was NOT published ride alongside, by reason. A manifest
    # carrying rows_out and silent about the rejected rows is the same shape of
    # blindness as rows_out: 0 with no guard verdict.
    assert {r["reason"]: r["count"] for r in m["rows_rejected"]} == {
        "append_error": 2,
        "quality_gate_blocked": 5,
    }
    assert [g["verdict"] for g in m["guards"]] == ["ok"]


def test_a_heal_unit_counts_the_length_of_what_it_healed(monkeypatch):
    out, m = _run(
        monkeypatch,
        "daily_heal",
        {
            "status": "ok",
            "collectors": {
                "universe_gap_heal": {
                    "status": "ok",
                    "healed_days": ["2026-09-11", "2026-09-12"],
                }
            },
        },
    )
    assert m["unit_id"] == "D33"
    assert m["rows_out"] == 2
    assert [g["verdict"] for g in m["guards"]] == ["ok"]


def test_a_real_zero_is_empty_fresh_not_a_pass(monkeypatch):
    """A run that completed and published nothing is the exact write every
    freshness detector reads as green. It is graded, not excused."""
    _, m = _run(
        monkeypatch,
        "morning_arctic_append",
        {"status": "ok", "collectors": {"arcticdb": {"status": "ok", "tickers_appended": 0}}},
    )
    assert m["rows_out"] == 0
    assert [g["verdict"] for g in m["guards"]] == ["empty_fresh"]


# ── A missing count is `unmeasurable`, never 0 ───────────────────────────────


def test_a_mode_whose_declared_step_reported_no_count_is_unmeasurable(monkeypatch):
    _, m = _run(
        monkeypatch,
        "daily_arctic_append",
        {"status": "ok", "collectors": {"arcticdb": {"status": "ok"}}},
    )
    assert m["outputs"] == []
    assert [g["verdict"] for g in m["guards"]] == ["unmeasurable"]
    assert "arcticdb.tickers_appended" in m["guards"][0]["detail"]


def test_every_declared_row_and_rejection_key_exists_in_its_writer():
    """The declared keys are pinned against the writers that report them.

    A declared-key table's known weakness is a RENAME: the reader falls back to
    `unmeasurable` (loud) for a missing row count, but a renamed *rejection* key
    would simply stop being counted, silently. This is the backstop — the key
    strings are asserted to appear in the source of the function that produces
    them, so a rename breaks here rather than quietly emptying `rows_rejected`.
    """
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    sources = {
        "daily_append": (root / "builders" / "daily_append.py").read_text(),
        "backfill": (root / "builders" / "backfill.py").read_text(),
        "weekly_collector": (root / "weekly_collector.py").read_text(),
    }
    haystack = "\n".join(sources.values())

    declared: set[str] = set()
    for unit in run_units.PHASE_UNITS.values():
        if unit.rows_key:
            declared.add(unit.rows_key)
        declared.update(k for k, _reason in unit.rejected_keys)
    for spec in run_units.MODE_ROWS.values():
        declared.add(spec.rows_key)
        declared.update(k for k, _reason in spec.rejected_keys)

    # Only the keys produced by the writers read here — the per-collector keys
    # (`tickers_captured`, `sectors`, …) live in `collectors/` and are covered by
    # their own phases' guard verdicts.
    checked = {
        "tickers_written", "tickers_appended", "tickers_errored", "tickers_skipped",
        "tickers_missing_from_closes", "tickers_quality_blocked", "tickers_l2_quarantined",
        "healed_days", "healed",
    }
    missing = sorted(k for k in declared & checked if f'"{k}"' not in haystack)
    assert not missing, (
        f"run_units declares row/rejection key(s) {missing} that no writer in "
        "builders/daily_append.py, builders/backfill.py or weekly_collector.py reports. "
        "A renamed key stops being counted silently — this is the backstop."
    )


def test_every_whole_mode_unit_has_a_declared_row_source():
    """`MODE_ROWS` is graded against `MODE_UNITS` here rather than left to
    drift: a mode added without a row source records `unmeasurable` forever and
    nothing says so."""
    assert set(run_units.MODE_ROWS) == set(run_units.MODE_UNITS), (
        "every mode in MODE_UNITS needs a MODE_ROWS entry naming where its published "
        "row count lives, or it is permanently unmeasurable with no address"
    )


# ── A non-run is `not_applicable`, never `ok` ────────────────────────────────


def test_a_skipped_mode_writes_not_applicable_and_returns_its_own_result(monkeypatch):
    """`alpha-engine-config-I10784`: MorningEnrich after 1:30pm PT returns
    `status="skipped"` and used to record a manifest saying `ok` — a non-run
    filed as a successful run."""
    out, m = _run(
        monkeypatch,
        "morning_enrich",
        {"status": "skipped", "skip_reason": "polygon free tier will not serve today", "collectors": {}},
    )
    assert m["status"] == "not_applicable"
    assert m["reason"] == run_units.NOT_RUN_NOT_APPLICABLE
    assert [g["verdict"] for g in m["guards"]] == ["not_applicable"]
    # The caller's contract is the mode's own dict — `run_unit` returns
    # `value=None` on the not-applicable path, and returning that would turn an
    # honest non-run into an AttributeError upstream.
    assert out["status"] == "skipped"
    assert out["skip_reason"] == "polygon free tier will not serve today"


def test_a_failing_mode_still_writes_failed_and_returns_its_result(monkeypatch):
    out, m = _run(
        monkeypatch,
        "daily_heal",
        {"status": "failed", "collectors": {}},
    )
    assert m["status"] == "failed"
    assert out["status"] == "failed"
