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
    """`tickers_published` (n_ok + n_partial) is the row count, not
    `tickers_appended` (n_ok alone) — a row with >=1 NaN feature is still a
    real ArcticDB write. Fixture shape mirrors the 2026-09-16 D32 live
    incident: almost every row landed in `tickers_partial`, not
    `tickers_appended` (alpha-engine-config-I10810)."""
    out, m = _run(
        monkeypatch,
        "daily_arctic_append",
        {
            "status": "ok",
            "collectors": {
                "arcticdb": {
                    "status": "ok",
                    "tickers_appended": 2,
                    "tickers_partial": 889,
                    "tickers_published": 891,
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
            "date": "2026-09-13",
            "days_healed": 2,
            "collectors": {
                "universe_gap_heal": {
                    "status": "ok",
                    "healed_days": [
                        {"date": "2026-09-11", "kind": "missing", "tickers": 903},
                        {"date": "2026-09-12", "kind": "fallback_quality", "tickers": 903},
                    ],
                }
            },
        },
    )
    assert m["unit_id"] == "D33"
    # alpha-engine-config-I10861: the manifest's aggregate rows_out is now the
    # SUM over every real key this run wrote (never the single synthesized
    # arcticdb://{unit_id} key alone) — the heal-summary artifact, one
    # staging/daily_closes/{day}.parquet per healed day, and the ArcticDB
    # library write.
    keys = {o["key"]: o["rows_out"] for o in m["outputs"]}
    assert keys == {
        "data/heal/daily/2026-09-13.json": 2,
        "staging/daily_closes/2026-09-11.parquet": 2,
        "staging/daily_closes/2026-09-12.parquet": 2,
        "arcticdb/universe": 2,
    }
    assert [g["verdict"] for g in m["guards"]] == ["ok"]


def test_morning_enrich_records_its_daily_closes_write_even_without_an_arctic_append(
    monkeypatch,
):
    """`alpha-engine-config-I10861`: D17's weekday run passes
    ``--skip-arctic-append`` — the arctic append happens in D18's own SF
    state, not inline here. Gating the S3 output recording behind the
    (absent) arctic row measurement, as the pre-fix single-key
    ``_record_mode_lineage`` did, left D17's weekday runs with NO recorded
    output at all."""
    out, m = _run(
        monkeypatch,
        "morning_enrich",
        {
            "status": "ok",
            "date": "2026-09-14",
            "collectors": {
                "daily_closes": {"status": "ok", "tickers_captured": 903},
            },
        },
    )
    assert m["unit_id"] == "D17"
    keys = {o["key"]: o["rows_out"] for o in m["outputs"]}
    assert keys == {"staging/daily_closes/2026-09-14.parquet": 903}
    # The arctic row measurement is genuinely absent this run — unmeasurable,
    # never silently 0 — and the S3 write above is recorded regardless.
    assert [g["verdict"] for g in m["guards"]] == ["unmeasurable"]


def test_chronic_gap_heal_records_one_price_cache_key_per_healed_ticker(monkeypatch):
    """`alpha-engine-config-I10861`: D34 writes
    ``reference/price_cache/{ticker}.parquet`` per healed ticker — never the
    descriptor's stale ``staging/daily_closes/*`` claim, which this mode does
    not touch (see registry.d/units/D34-chronic-gap-heal.yaml)."""
    out, m = _run(
        monkeypatch,
        "chronic_gap_heal",
        {
            "status": "ok",
            "collectors": {
                "chronic_gap_self_heal": {
                    "status": "ok",
                    "healed": [
                        {"ticker": "PSTG", "rows_added": 5},
                        {"ticker": "BF-B", "rows_added": 3},
                    ],
                    "skipped_already_fresh": [],
                    "errors": [],
                },
            },
        },
    )
    assert m["unit_id"] == "D34"
    keys = {o["key"]: o["rows_out"] for o in m["outputs"]}
    assert keys == {
        "reference/price_cache/PSTG.parquet": 8,
        "reference/price_cache/BF-B.parquet": 8,
        "arcticdb/universe": 2,
    }
    assert [g["verdict"] for g in m["guards"]] == ["ok"]


def test_a_real_zero_is_empty_fresh_not_a_pass(monkeypatch):
    """A run that completed and published nothing is the exact write every
    freshness detector reads as green. It is graded, not excused."""
    _, m = _run(
        monkeypatch,
        "morning_arctic_append",
        {"status": "ok", "collectors": {"arcticdb": {"status": "ok", "tickers_published": 0}}},
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
    assert "arcticdb.tickers_published" in m["guards"][0]["detail"]


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
        "tickers_written", "tickers_appended", "tickers_published", "tickers_errored", "tickers_skipped",
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


def test_a_stale_overwrite_skip_writes_no_new_data_declared(monkeypatch):
    """`alpha-engine-config-I10784`: MorningEnrich returning `status="skipped"`
    used to record a manifest saying `ok` — a non-run filed as a successful
    run.

    `alpha-engine-config-I10831` deliverable 1, corrected 2026-09-15:
    `_should_skip_morning_enrich`'s `stale_overwrite` reason — the target
    date is already appended to ArcticDB — matches
    `nousergon_lib.run_manifest`'s own `no_new_data_declared` definition
    verbatim ("an upstream explicitly declared there is nothing new for THIS
    run to collect ... a target date already published"), not
    `disabled_by_declaration` (no operator/config switch was involved) and
    not `outside_session_window` (this is a data fact, not a clock fact)."""
    out, m = _run(
        monkeypatch,
        "morning_enrich",
        {
            "status": "skipped",
            "skip_reason": (
                "stale_overwrite (polygon target=2026-09-14, ArcticDB SPY last=2026-09-15) — "
                "polygon's T+1 settled day is older than the yfinance EOD row already in "
                "ArcticDB"
            ),
            "collectors": {},
        },
    )
    assert m["status"] == "not_applicable"
    assert m["reason"] == run_units.NOT_RUN_NO_NEW_DATA_DECLARED
    assert [g["verdict"] for g in m["guards"]] == ["not_applicable"]
    # The caller's contract is the mode's own dict — `run_unit` returns
    # `value=None` on the not-applicable path, and returning that would turn an
    # honest non-run into an AttributeError upstream.
    assert out["status"] == "skipped"


def test_an_unclassified_skip_reason_fails_loud(monkeypatch):
    """No default bucket: a skip_reason this dispatch has not explicitly
    matched to a `NOT_APPLICABLE_REASONS` member is a FAILED manifest, not a
    guess (`alpha-engine-config-I10831`, corrected 2026-09-15 — the prior
    revision defaulted every non-`stale_overwrite` skip to
    `outside_session_window`, which was a bucket, not a match)."""
    out, m = _run(
        monkeypatch,
        "morning_enrich",
        {"status": "skipped", "skip_reason": "polygon free tier will not serve today", "collectors": {}},
    )
    assert m["status"] == "failed"
    assert "polygon free tier will not serve today" in m["reason"]
    assert out["status"] == "skipped"


def test_a_failing_mode_still_writes_failed_and_returns_its_result(monkeypatch):
    out, m = _run(
        monkeypatch,
        "daily_heal",
        {"status": "failed", "collectors": {}},
    )
    assert m["status"] == "failed"
    assert out["status"] == "failed"


def test_arctic_append_units_read_tickers_published_not_tickers_appended():
    """Pin against regressing alpha-engine-config-I10810 (measured 2026-09-16):
    `tickers_appended` alone (n_ok, "fully-featured" rows) undercounted real
    ArcticDB writes on a live D18/D32 run where 909 of 910 published rows
    carried >=1 NaN feature (`tickers_partial`) and so were invisible to both
    `rows_out` and `rows_rejected`. Every arctic-append rows_key MUST be
    `tickers_published` (n_ok + n_partial), which `builders.daily_append`
    reports as the total actually written this run."""
    assert run_units.PHASE_UNITS[("daily", "arcticdb")].rows_key == "tickers_published"
    for mode in ("morning_enrich", "morning_arctic_append", "daily_arctic_append"):
        assert run_units.MODE_ROWS[mode].rows_key == "tickers_published", mode
