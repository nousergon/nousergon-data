"""The data gate's own invariants — including the phase-0 commissioning test.

Plan §6 phase-0 exit **(e)**, verbatim: *"commissioning: in a test store,
withholding the ladder renders the ladder rows UNREPORTED and stale, withholding
one manifest renders that clause UNMEASURABLE, and neither renders green. The
phase-0 gate measures that the board is HONEST, not that it is green."*

Everything here runs against an in-memory or tmp-dir store. No AWS.
"""

from __future__ import annotations

import ast
import datetime as dt
import json
import textwrap

import pytest
import yaml

from data_gate import clauses as clause_module
from data_gate import descriptors, evidence
from data_gate import exit_criteria as xc
from data_gate.descriptors import REPO_ROOT, DescriptorError, load_units
from data_gate.inventory import load_inventory_scope, scan
from data_gate.read import GATES, BOARD_KEY, _board_document, evaluate, load_phases, run
from data_gate.store import DryRunWriteRefusedError, LocalStore, open_store, parse_store_uri

from tests.data_gate_support import TRADING_DAY, DeniedStore, EmptyStore


@pytest.fixture(scope="module")
def units():
    return load_units()


@pytest.fixture(scope="module")
def phases():
    return load_phases()


@pytest.fixture(scope="module")
def board(units, phases):
    return clause_module.generate(EmptyStore(), units, phases, trading_day=TRADING_DAY)


# ---------------------------------------------------------------------------
# Phase-0 exit (e) — the commissioning test.
# ---------------------------------------------------------------------------


def test_withholding_the_ladder_renders_unreported_never_green(tmp_path):
    """No ladder in the store: the freshness clause fails and nothing is green."""
    store = LocalStore(tmp_path)
    result, ladder, board_doc = run(store, gate="data-phase0", trading_day=TRADING_DAY)

    fresh = next(c for c in result.clauses if c.name == "data.gate.ladder_fresh")
    assert fresh.met is False
    assert "never been published" in fresh.detail

    assert result.met is False
    # Every rung that has never been read renders red on the console — never
    # HEALTHY, and never a number that reads as a partial pass.
    for row in ladder["phases"]:
        assert row["console_state"] in {"DEGRADED", "FAILED", "UNREPORTED"}
        assert row["state"] != "MET"


def test_a_stale_ladder_pages_rather_than_rendering_its_last_state(tmp_path):
    """Page condition 3: the observer observed.

    Without this the whole board freezes in its last state and keeps rendering
    it, indefinitely, with nothing saying the gate stopped running.
    """
    store = LocalStore(tmp_path)
    run(store, gate="data-phase0", trading_day=TRADING_DAY)
    document = json.loads((tmp_path / "gates" / "ladder.json").read_text())
    document["generated_utc"] = "2026-09-01T00:00:00Z"
    (tmp_path / "gates" / "ladder.json").write_text(json.dumps(document))

    result, _ladder, _board = run(store, gate="data-phase0", trading_day=TRADING_DAY)
    fresh = next(c for c in result.clauses if c.name == "data.gate.ladder_fresh")
    assert fresh.met is False
    assert "page condition 3" in fresh.detail


def test_withholding_a_manifest_renders_red_never_green(board):
    """A unit with no run manifest for the day is UNMET, with its key named.

    UNMET rather than UNMEASURABLE since `alpha-engine-config-I10810` built the
    reader: the store ANSWERED and the prefix was empty, which is a finding about
    the producer, not about our read. The two are kept apart on purpose — "we
    could not look" and "we looked and there was nothing" have opposite owners.
    Either way it is red, which is the property this test defends.
    """
    run_record = next(c for c in board if c.name == "data.D01.run_record")
    assert run_record.met is False
    assert run_record.unmeasurable is False
    # Store-relative, like every other reader here: the store is opened at
    # `s3://alpha-engine-research/data_collection`, so the key it looked at is
    # `runs/<unit>/<day>/*.json` under that.
    assert "runs/D01" in " ".join(run_record.evidence)
    assert "no run manifest" in run_record.detail


def test_a_denied_store_never_produces_a_met_gate(units, phases):
    """Withholding the whole store: red, and red for OUR reason, not theirs."""
    clauses = clause_module.generate(DeniedStore(), units, phases, trading_day=TRADING_DAY)
    assert any(c.unmeasurable for c in clauses)
    for gate in GATES:
        result = evaluate(DeniedStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=clauses)
        assert result.met is False
        # One unmeasurable clause makes the whole ratio None: a number computed
        # over a partial read is an overclaim.
        assert result.met_ratio is None


def test_no_clause_is_met_without_a_read(board):
    """`never MET without a read` — the rule evidence.py exists to enforce."""
    for clause in board:
        if not clause.met:
            continue
        assert clause.evidence, f"{clause.name} reads MET citing no evidence"
        assert "no reader is built" not in clause.detail, (
            f"{clause.name} reads MET off a phase-0 stub — the one thing a stub may never do"
        )


def test_the_board_is_red_at_birth(board):
    """214 cells were scored PRESENT; none of them is green on day one."""
    base = [
        c
        for c in board
        if c.name.startswith("data.D") and ".guard." not in c.name and not c.name.endswith(".completeness")
    ]
    assert len(base) == 414
    # A declared not-applicable (an on-demand unit with no invocation to record,
    # or no scheduled trigger for phase 4 to remove — alpha-engine-config-I10870/
    # I10871) is MET off the descriptor plus a real listing, not off a reader
    # grading evidence; it is held to the `not applicable:` prefix instead.
    declared_na = [c for c in base if c.met and c.detail.startswith("not applicable")]
    assert all(c.name.endswith((".run_record", ".survives_phase4")) for c in declared_na), declared_na
    met = [c for c in base if c.met and c not in declared_na]
    # Only `schema_contract` has a real reader in phase 0, and it reads MET only
    # where a schema, a producer test AND a consumer pin all exist.
    # I10823 added readers whose evidence is in THIS tree even against an empty
    # store: generated observability rows, and this repository's own consumer
    # paths. Every other column needs a store or an external source to read MET.
    tree_readers = (".schema_contract", ".observability_row", ".consumers")
    assert all(c.name.endswith(tree_readers) for c in met), sorted(c.name for c in met)
    # The ceiling is DERIVED, not hand-tuned: every base clause whose column is
    # one of the tree readers above, excluding RETIRED rows, is the whole
    # universe a real (non-store) reader could possibly mark MET. A hand-picked
    # number breaks on every legitimate schema/registry-row/consumer-pin landing
    # (I10774 raised it 20->21, I10775/I10823 raised it again) — this bound
    # tracks the reader registry itself, so it only breaks when a clause OUTSIDE
    # that registry reads MET, which `assert all(...)` above already catches
    # structurally, or when RETIRED bookkeeping is wrong.
    eligible = [c for c in base if c.name.endswith(tree_readers) and not clause_module.is_retired(c)]
    assert len(met) <= len(eligible), "more clauses read MET than have a real reader backing them"


# ---------------------------------------------------------------------------
# Standing clauses (alpha-engine-config-I10793 / -I10788, Brian's 2026-09-21
# ruling): "i'm not clear why we need to time gate anything, why not just
# collect the data when it is ready but not block subsequent issues" — a
# clause measured and reported every day, blocking no phase, ever.
# ---------------------------------------------------------------------------


def test_the_cost_baseline_clause_is_standing_and_carries_its_real_state(board):
    """Against an empty store (no cost document at all), the clause reads its
    REAL state — UNMET, "no cost document" — never forced, never hidden."""
    clause = next(c for c in board if c.name == "data.phase1.cost_baseline_measured")
    assert clause_module.is_standing(clause)
    assert clause.met is False
    assert clause.unmeasurable is False
    assert "no cost document" in clause.detail
    assert clause.ruling.startswith("Brian, 2026-09-21")


def _write_cost_document(tmp_path, *, days_covered: int) -> LocalStore:
    store = LocalStore(tmp_path)
    store.put_bytes(
        "metrics/cost/monthly/latest.json",
        json.dumps({"baseline": 42.0, "days_covered": days_covered, "as_of": "2026-10-20T00:00:00Z"}).encode(),
    )
    return store


def test_a_standing_clause_reads_met_once_its_evidence_exists(tmp_path, units, phases):
    """Unlike RetiredClause/UnconnectedClause/DisabledTriggerClause — always
    `met=False` because the STATE is the fact — a StandingClause carries
    whatever its reading says. Once 28 days of cost data exist, it reads
    MET, honestly, same as an ordinary clause would."""
    store = _write_cost_document(tmp_path, days_covered=28)
    generated = clause_module.generate(store, units, phases, trading_day=TRADING_DAY)
    clause = next(c for c in generated if c.name == "data.phase1.cost_baseline_measured")
    assert clause_module.is_standing(clause)
    assert clause.met is True
    assert "baseline=42.0" in clause.detail


def test_a_standing_clause_never_changes_any_gates_met(board):
    """The whole point of the ruling: this clause's state — MET or UNMET —
    must never be able to hold a phase shut. Proven by construction, not by
    the current data: even MET, it is excluded from `evaluate`'s selection,
    and (today) UNMET, it does not appear in `data-phase1`'s own clause list
    at all — a phase-1 read that were somehow blind to this exclusion would
    show it as UNMET among `result.clauses` below, which it does not."""
    result = evaluate(EmptyStore(), gate="data-phase1", trading_day=TRADING_DAY, all_clauses=board)
    assert "data.phase1.cost_baseline_measured" not in {c.name for c in result.clauses}
    # And the reverse: still on the full board, for every gate to see if it wanted to.
    assert any(c.name == "data.phase1.cost_baseline_measured" for c in board)


def test_is_ungraded_excludes_standing_from_every_gate(board):
    for gate in GATES:
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        assert "data.phase1.cost_baseline_measured" not in {c.name for c in result.clauses}, gate


def test_the_board_document_counts_standing_separately_and_shows_real_state(board):
    document = _board_document(board, trading_day=TRADING_DAY, generated_utc="2026-09-21T00:00:00Z", store_uri=None)
    row = next(r for r in document["rows"] if r["clause"] == "data.phase1.cost_baseline_measured")
    assert row["standing"] is True
    assert row["state"] == "UNMET"  # the REAL state — never a euphemism, never hidden
    assert "standing_ruling" in row and row["standing_ruling"].startswith("Brian, 2026-09-21")
    assert document["clauses_standing"] >= 1
    # Excluded from the graded denominator, same as RETIRED/UNCONNECTED.
    assert row["clause"] not in {
        c.name
        for c in board
        if c.met and not clause_module.is_ungraded(c) and c.name != "data.phase1.cost_baseline_measured"
    }


def test_render_lists_the_standing_clause_even_though_no_gate_grades_it(board):
    """`data_gate read --gate data-phase1`'s printed output must never hide a
    real reading behind the exclusion that keeps it from blocking a phase —
    `result.render()` (the gate's OWN clause list) cannot show it, so `render`
    prints it separately."""
    from data_gate.read import render

    result = evaluate(EmptyStore(), gate="data-phase1", trading_day=TRADING_DAY, all_clauses=board)
    document = _board_document(board, trading_day=TRADING_DAY, generated_utc="2026-09-21T00:00:00Z", store_uri=None)
    text = render(result, document, dry_run=True)
    assert "data.phase1.cost_baseline_measured" not in result.render()
    assert "data.phase1.cost_baseline_measured" in text
    assert "standing (measured daily, gates no phase)" in text


# ---------------------------------------------------------------------------
# Reliability streaks (alpha-engine-config-I10793/-I10788, Brian's second
# 2026-09-21 ruling): "it sounds like the only time gate we should have here
# is for v2 phase 4 deleting the v1 pipelines ... we should be able to work
# up to this point without time gates." Phase 1's own exit narrows from a
# streak to ONE complete cycle; the ratified streak (5/5/2) becomes a
# standing row published for Crucible v2 phase 4 to gate its irreversible
# v1-pipeline deletion on.
# ---------------------------------------------------------------------------


def _cycles(schedule: str, *, complete: int, total: int) -> xc.CycleSet:
    """A `CycleSet` with `complete` OK cycles, most recent first, followed by
    an incomplete one — enough to prove a threshold without touching a store."""
    import types

    units = [types.SimpleNamespace(unit_id="D19")]
    cycles = []
    now = dt.datetime(2026, 9, 21, 20, 45, tzinfo=dt.timezone.utc)
    for i in range(total):
        fire = now - dt.timedelta(days=i)
        if i < complete:
            manifests = {"D19": [(f"k{i}", {"trigger": "scheduled", "status": "ok"})]}
        else:
            manifests = {"D19": []}
        cycles.append(xc.Cycle(fire=fire, manifests=manifests))
    return xc.CycleSet(schedule=schedule, units=units, cycles=cycles)


def test_the_exit_clause_needs_exactly_one_complete_cycle():
    """Brian's ruling narrows the phase-1 exit from a streak to ONE cycle —
    proven against the reader directly, not inferred from the constant."""
    assert xc.PHASE1_EXIT_CONSECUTIVE[xc.SCHEDULE_EOD] == 1
    one_complete = _cycles(xc.SCHEDULE_EOD, complete=1, total=3)
    clause = clause_module._clause_phase1_consecutive_eod_cycles(one_complete)
    assert clause.met is True
    assert clause.phase == "data-phase1"
    assert not clause_module.is_standing(clause)

    zero_complete = _cycles(xc.SCHEDULE_EOD, complete=0, total=3)
    clause = clause_module._clause_phase1_consecutive_eod_cycles(zero_complete)
    assert clause.met is False


def test_the_standing_row_renders_the_true_streak_against_the_ratified_target():
    """The plan's ORIGINAL target (5/5/2) is preserved on the standing row —
    a 3-cycle streak reads UNMET against 5, honestly, never inflated to MET
    just because phase 1's own exit only needed one."""
    assert xc.RELIABILITY_STREAK_TARGET[xc.SCHEDULE_EOD] == 5
    three_complete = _cycles(xc.SCHEDULE_EOD, complete=3, total=6)
    clause = clause_module._clause_reliability_eod_streak(three_complete)
    assert clause_module.is_standing(clause)
    assert clause.met is False
    assert "3 consecutive complete cycle(s)" in clause.detail
    assert "against the 5 the plan's exit names" in clause.detail
    assert clause.phase == clause_module.RELIABILITY_GATE
    assert clause.ruling.startswith("Brian, 2026-09-21")

    five_complete = _cycles(xc.SCHEDULE_EOD, complete=5, total=6)
    clause = clause_module._clause_reliability_eod_streak(five_complete)
    assert clause.met is True


def test_an_absent_reading_renders_absent_never_green_for_the_reliability_row():
    """An unmeasurable cycle window (denied/failed listing) is UNMEASURABLE on
    the standing row too — never MET, never silently dropped."""
    unreadable = xc.CycleSet(schedule=xc.SCHEDULE_EOD, units=[], unreadable="AccessDenied")
    clause = clause_module._clause_reliability_eod_streak(unreadable)
    assert clause_module.is_standing(clause)
    assert clause.met is False
    assert clause.unmeasurable is True


def test_a_reliability_streak_never_moves_any_data_phase_gates_met(board):
    """However the streak reads — MET or UNMET — it is excluded from
    data-phase1/2/3's own clause list, structurally, so it can never hold or
    pass a numbered phase on its own account."""
    names = {"data.standing.eod_reliability_streak", "data.standing.morning_reliability_streak",
             "data.standing.weekly_reliability_streak"}
    for gate in ("data-phase1", "data-phase2", "data-phase3"):
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        assert not (names & {c.name for c in result.clauses}), gate


def test_the_reliability_gate_reads_the_three_streaks_and_only_them(board):
    result = evaluate(EmptyStore(), gate=clause_module.RELIABILITY_GATE, trading_day=TRADING_DAY, all_clauses=board)
    assert {c.name for c in result.clauses} == {
        "data.standing.eod_reliability_streak",
        "data.standing.morning_reliability_streak",
        "data.standing.weekly_reliability_streak",
    }


def test_the_reliability_gate_is_published_alongside_the_ladder(tmp_path):
    """The same `run()` pipeline that publishes `data-cutover-ready` publishes
    `data-collection-reliability` — the artifact Crucible v2 phase 4 reads."""
    from data_gate.store import LocalStore

    store = LocalStore(tmp_path)
    result, ladder, board_doc = run(store, gate="data-collection-reliability", trading_day=TRADING_DAY)
    assert result.gate == "data-collection-reliability"
    assert {c.name for c in result.clauses} == {
        "data.standing.eod_reliability_streak",
        "data.standing.morning_reliability_streak",
        "data.standing.weekly_reliability_streak",
    }
    dated = json.loads(store.get_bytes(f"gates/data-collection-reliability/{TRADING_DAY.isoformat()}/gate.json"))
    assert dated["gate"] == "data-collection-reliability"
    assert dated["schema_version"] == "gate.v1"
    assert {c["name"] for c in dated["clauses"]} == {
        "data.standing.eod_reliability_streak",
        "data.standing.morning_reliability_streak",
        "data.standing.weekly_reliability_streak",
    }
    # The whole-board `latest.json` (current state, rewritten every read) also
    # carries these three rows, marked `standing` — the single board every
    # gate's read shares (`run()`'s own "BOTH READS, ONE BOARD" contract).
    board_latest = json.loads(store.get_bytes("gates/board/latest.json"))
    standing_rows = [r for r in board_latest["rows"] if r.get("standing")]
    assert {r["clause"] for r in standing_rows} >= {
        "data.standing.eod_reliability_streak",
        "data.standing.morning_reliability_streak",
        "data.standing.weekly_reliability_streak",
    }


# ---------------------------------------------------------------------------
# The gate contract
# ---------------------------------------------------------------------------


def test_an_unregistered_gate_raises_rather_than_grading_nothing():
    with pytest.raises(KeyError, match="unknown gate"):
        evaluate(EmptyStore(), gate="data-phase9", trading_day=TRADING_DAY)


def test_each_gate_grades_only_the_clauses_tagged_at_or_below_its_phase(board):
    ceilings = {}
    for gate, ceiling in GATES.items():
        result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=board)
        ceilings[gate] = len(result.clauses)
        if ceiling is None:
            # A sub-gate (`data-cutover-ready`): selects its own exactly-
            # tagged clauses, never a numbered ceiling.
            for clause in result.clauses:
                assert clause.phase == gate
            continue
        for clause in result.clauses:
            assert int(clause.phase.removeprefix("data-phase")) <= ceiling
    assert ceilings["data-phase0"] < ceilings["data-phase1"] < ceilings["data-phase3"]
    # Phase 3's exit is "all clauses MET" over the numbered ladder — except
    # the `data-cutover-ready` sub-gate's own four clauses, which are
    # DELIBERATELY not a rung (`registry.d/phases.yaml`) and are graded only
    # by their own gate.
    # RETIRED clauses (I10823 deliverable 6) are rendered but graded by no gate.
    retired = sum(1 for c in board if clause_module.is_ungraded(c))  # RETIRED + UNCONNECTED (I10873)
    assert ceilings["data-phase3"] == len(board) - len(clause_module.CUTOVER_READY_CLAUSES) - retired


def test_every_reading_names_its_store_and_its_commit(board):
    """Plan §6: one number per phase, quoted WITH its store and commit."""
    result = evaluate(EmptyStore(), gate="data-phase0", trading_day=TRADING_DAY, all_clauses=board)
    assert result.store == "memory://empty"
    assert result.code_sha
    assert result.coverage and "out of" in result.coverage


def test_a_dry_run_writes_nothing(tmp_path):
    store = LocalStore(tmp_path, dry_run=True)
    run(store, gate="data-phase0", trading_day=TRADING_DAY, dry_run=True)
    assert not list(tmp_path.rglob("*")), "a dry run left artifacts behind"


def test_a_dry_run_store_refuses_a_write_outright(tmp_path):
    with pytest.raises(DryRunWriteRefusedError):
        LocalStore(tmp_path, dry_run=True).put_bytes("gates/ladder.json", b"{}")


def test_a_real_run_publishes_the_reading_the_ladder_and_the_board(tmp_path):
    store = LocalStore(tmp_path)
    run(store, gate="data-phase0", trading_day=TRADING_DAY)
    assert (tmp_path / "gates" / "ladder.json").is_file()
    assert (tmp_path / BOARD_KEY).is_file()
    assert (tmp_path / "gates" / "data-phase0" / "2026-09-14" / "gate.json").is_file()


def test_the_board_document_publishes_the_transparency_gap(tmp_path):
    """observability-policy §8.4: published even at zero, or absence reads as health."""
    _result, _ladder, board_doc = run(LocalStore(tmp_path), gate="data-phase0", trading_day=TRADING_DAY)
    assert "transparency_gap" in board_doc
    assert board_doc["transparency_gap"] > 0, "on day one the gap is most of the board"
    assert board_doc["clauses_total"] == board_doc["clauses_met"] + board_doc["clauses_unmet"] + board_doc["transparency_gap"]
    for row in board_doc["rows"]:
        assert set(row) >= {"clause", "state", "console_state", "source", "as_of", "evidence"}


def test_the_ladder_and_the_gate_reading_cannot_disagree(tmp_path):
    """One evaluation, several projections — the crucible I10575 defect, refused."""
    store = LocalStore(tmp_path)
    result, ladder, _board = run(store, gate="data-phase0", trading_day=TRADING_DAY)
    row = next(r for r in ladder["phases"] if r["gate"] == "data-phase0")
    assert row["clauses_total"] == len(result.clauses)
    assert row["clauses_met"] == sum(1 for c in result.clauses if c.met)


def test_the_cli_exit_codes_separate_not_met_from_unmeasured(tmp_path, monkeypatch):
    """alpha-engine-config-I10906: default exit is 0 on any successful measurement —
    the verdict is not a process failure, it already has a durable surface (ladder/
    board/history). Only `--fail-on-unmet` recovers the old "1 means UNMET" code,
    for a human `workflow_dispatch`/PR-time invocation that wants it."""
    from data_gate.__main__ import EXIT_MET, EXIT_NOT_MET, EXIT_UNMEASURED, main

    # Two DISTINCT fresh stores: reading a gate publishes gates/ladder.json, and
    # `data.gate.ladder_fresh` grades a store that already has one — reusing the
    # same store between these two calls would flip the second reading's verdict
    # from UNMET to MET on that clause alone, independent of --fail-on-unmet.
    store_a = tmp_path / "a"
    store_b = tmp_path / "b"

    code = main(["read", "--gate", "data-phase0", "--store", str(store_a), "--trading-day", "2026-09-14"])
    assert code == EXIT_MET

    code = main(
        [
            "read",
            "--gate",
            "data-phase0",
            "--store",
            str(store_b),
            "--trading-day",
            "2026-09-14",
            "--fail-on-unmet",
        ]
    )
    assert code == EXIT_NOT_MET

    def _explode(*_args, **_kwargs):
        raise RuntimeError("the descriptors would not load")

    monkeypatch.setattr("data_gate.read.run", _explode)
    assert (
        main(["read", "--gate", "data-phase0", "--store", str(tmp_path), "--dry-run"])
        == EXIT_UNMEASURED
    )
    # A measurement failure exits 2 regardless of --fail-on-unmet — that flag only
    # governs the UNMET code path, never the "we could not ask" code path.
    assert (
        main(
            ["read", "--gate", "data-phase0", "--store", str(tmp_path), "--dry-run", "--fail-on-unmet"]
        )
        == EXIT_UNMEASURED
    )


def test_store_uris_resolve(tmp_path):
    assert parse_store_uri("s3://bucket/prefix") == ("s3", "bucket/prefix")
    store = open_store("s3://alpha-engine-research/data_collection")
    assert store.uri == "s3://alpha-engine-research/data_collection"
    assert isinstance(open_store(str(tmp_path)), LocalStore)


# ---------------------------------------------------------------------------
# Descriptor discipline — the refusals
# ---------------------------------------------------------------------------


def _write_descriptor(tmp_path, overrides: dict) -> None:
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base.update(overrides)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))


def test_an_empty_units_directory_is_refused(tmp_path):
    with pytest.raises(DescriptorError, match="no unit descriptors"):
        descriptors.load_units(tmp_path)


def test_a_missing_audit_column_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["audit"]["cells"].pop("detector")
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="scores exactly"):
        descriptors.load_units(tmp_path)


def test_an_unknown_cell_state_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["audit"]["cells"]["detector"] = "PROBABLY FINE"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="four cell states"):
        descriptors.load_units(tmp_path)


def test_a_not_applicable_guard_without_a_taxonomy_code_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"]["cardinality"] = {"state": "not_applicable", "note": "does not apply"}
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="no `na_code`"):
        descriptors.load_units(tmp_path)


def test_an_na_code_outside_the_closed_taxonomy_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"]["cardinality"] = {
        "state": "not_applicable",
        "na_code": "N/A-WHATEVER",
        "note": "x",
    }
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="closed taxonomy"):
        descriptors.load_units(tmp_path)


def test_a_missing_guard_class_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["guards"].pop("pit")
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="missing"):
        descriptors.load_units(tmp_path)


def test_empty_consumers_without_a_reason_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["consumers"] = []
    base.pop("consumers_reason", None)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="no consumers and gives no reason"):
        descriptors.load_units(tmp_path)


# ---------------------------------------------------------------------------
# Connected / unconnected (alpha-engine-config-I10873, Brian ruling 2026-09-15).
# ---------------------------------------------------------------------------

_DECISION = {
    "decision": "kept-unconnected",
    "ruled_by": "Brian",
    "ruled_on": "2026-09-15",
    "ruling": "Brian 2026-09-15 (alpha-engine-config-I10873)",
    "reason": "kept, reincorporation possible",
    "reexam": "a v2 component proposes reading this key",
}


def _d01(tmp_path, **overrides):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base.update(overrides)
    for key, value in list(base.items()):
        if value is _DROP:
            base.pop(key)
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    return tmp_path


_DROP = object()


def _gated(clause, gate="data-phase1"):
    result = evaluate(EmptyStore(), gate=gate, trading_day=TRADING_DAY, all_clauses=[clause])
    return {c.name for c in result.clauses}


def test_the_retired_consumers_ruling_block_is_refused(tmp_path):
    """The I10870 block rendered a kept, unread unit MET. A stale copy must fail
    loud, never silently keep meaning green."""
    _d01(tmp_path, consumers=[], consumers_reason="none", consumers_ruling={"ruling": "x", "reason": "y"})
    with pytest.raises(DescriptorError, match="replaced by `consumers_decision`"):
        descriptors.load_units(tmp_path)


@pytest.mark.parametrize("field", ["decision", "ruled_by", "ruled_on", "ruling", "reason", "reexam"])
def test_a_consumers_decision_missing_any_field_is_refused(tmp_path, field):
    decision = {k: v for k, v in _DECISION.items() if k != field}
    _d01(tmp_path, consumers=[], consumers_reason="none", consumers_decision=decision)
    with pytest.raises(DescriptorError, match="consumers_decision is missing"):
        descriptors.load_units(tmp_path)


def test_a_consumers_decision_on_a_connected_unit_is_refused(tmp_path):
    """Reincorporation flips the unit to connected; the decision block must go
    in the same change, or the descriptor says two contradictory things."""
    _d01(tmp_path, consumers=["metron:api/services/prices.py"], consumers_decision=dict(_DECISION))
    with pytest.raises(DescriptorError, match="surviving consumer"):
        descriptors.load_units(tmp_path)


def test_an_unclassified_consumer_repo_is_refused(tmp_path):
    _d01(tmp_path, consumers=["some-new-repo:reader.py"])
    with pytest.raises(DescriptorError, match="in neither list"):
        descriptors.load_units(tmp_path)


def test_an_unconnected_kept_unit_is_neither_met_nor_red(tmp_path):
    """The ruling: exists, no consumer — never MET-green, never a phase-1 failure."""
    _d01(tmp_path, consumers=[], consumers_reason="no reader", consumers_decision=dict(_DECISION))
    [unit] = descriptors.load_units(tmp_path)
    assert unit.connection == "unconnected"

    clause = clause_module._clause_base(EmptyStore(), unit, "consumers", trading_day=TRADING_DAY)
    assert clause_module.is_unconnected(clause)
    assert clause.met is False and clause.unmeasurable is False
    assert clause.detail.startswith("UNCONNECTED:")
    assert "kept-unconnected by Brian on 2026-09-15" in clause.detail
    for gate in GATES:
        assert clause.name not in _gated(clause, gate), gate

    document = _board_document([clause], trading_day=TRADING_DAY, generated_utc="t", store_uri=None, units=[unit])
    [row] = document["rows"]
    assert row["state"] == "UNCONNECTED"
    assert row["console_state"] == "DISABLED"
    assert row["console_state"] not in {"HEALTHY", "DEGRADED", "FAILED"}
    assert row["connection"] == "unconnected" and row["consumers"] == []
    assert row["connection_decision"] == "kept-unconnected by Brian 2026-09-15"
    assert document["clauses_total"] == 0 and document["clauses_unconnected"] == 1


def test_a_unit_listing_only_v1_consumers_reads_unconnected(tmp_path):
    _d01(
        tmp_path,
        consumers=["crucible-research:agents/macro_agent.py", "crucible-predictor:x.py"],
        consumers_decision=dict(_DECISION),
    )
    [unit] = descriptors.load_units(tmp_path)
    assert unit.connection == "unconnected"
    assert unit.surviving_consumers == []
    assert unit.retiring_consumers == ["crucible-research:agents/macro_agent.py", "crucible-predictor:x.py"]
    clause = clause_module._clause_base(EmptyStore(), unit, "consumers", trading_day=TRADING_DAY)
    assert clause_module.is_unconnected(clause)
    assert "retiring in crucible v2 phase 4" in clause.detail


def test_an_unconnected_unit_without_a_decision_stays_a_graded_finding(tmp_path):
    _d01(tmp_path, consumers=["crucible-research:agents/macro_agent.py"])
    [unit] = descriptors.load_units(tmp_path)
    clause = clause_module._clause_base(EmptyStore(), unit, "consumers", trading_day=TRADING_DAY)
    assert not clause_module.is_unconnected(clause)
    assert clause.met is False and not clause.unmeasurable
    assert "NO recorded keep/retire decision" in clause.detail
    assert clause.name in _gated(clause, "data-phase1")


def test_a_connected_unit_is_unaffected(tmp_path):
    """A surviving consumer keeps the real reader path: the clause is graded,
    never UNCONNECTED, and the row names the consumer."""
    _d01(tmp_path, consumers=["nousergon-data:data_gate/read.py", "crucible-research:x.py"])
    [unit] = descriptors.load_units(tmp_path)
    assert unit.connection == "connected"
    assert unit.surviving_consumers == ["nousergon-data:data_gate/read.py"]
    clause = clause_module._clause_base(EmptyStore(), unit, "consumers", trading_day=TRADING_DAY)
    assert not clause_module.is_unconnected(clause)
    assert clause.name in _gated(clause, "data-phase1")
    document = _board_document([clause], trading_day=TRADING_DAY, generated_utc="t", store_uri=None, units=[unit])
    [row] = document["rows"]
    assert row["connection"] == "connected"
    assert row["consumers"] == ["nousergon-data:data_gate/read.py"]
    assert row["state"] in {"MET", "UNMET", "UNMEASURABLE"}


def test_the_ruled_units_are_unconnected_on_the_real_board(units, board):
    ruled = {"D02", "D03", "D04", "D05", "D06", "D07", "D08", "D14", "D33", "D46"}
    by_id = {u.unit_id: u for u in units}
    for unit_id in ruled:
        assert by_id[unit_id].connection == "unconnected", unit_id
        [clause] = [c for c in board if c.name == f"data.{unit_id}.consumers"]
        assert clause_module.is_unconnected(clause), unit_id
    assert not any(c.met for c in board if clause_module.is_unconnected(c))


def test_connection_counts_are_correct_and_cover_every_unit(units, board):
    document = _board_document(board, trading_day=TRADING_DAY, generated_utc="t", store_uri=None, units=units)
    counts = document["connection_counts"]
    expected = {state: sum(1 for u in units if u.connection == state) for state in descriptors.CONNECTION_STATES}
    for state, n in expected.items():
        assert counts[state] == n, state
    assert counts["units_total"] == len(units) == sum(expected.values())
    assert counts["unconnected_decided"] + counts["unconnected_undecided"] == counts["unconnected"]
    assert counts["unconnected_decided"] == sum(
        1 for u in units if u.connection == "unconnected" and u.consumers_decision
    )
    unit_rows = [r for r in document["rows"] if r["unit_id"] in {u.unit_id for u in units}]
    assert unit_rows and all(r["connection"] in descriptors.CONNECTION_STATES for r in unit_rows)
    assert all("connection" not in r for r in document["rows"] if r["unit_id"] not in {u.unit_id for u in units})
    assert document["clauses_unconnected"] == sum(1 for c in board if clause_module.is_unconnected(c))


def test_a_filename_that_disagrees_with_the_unit_id_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    (tmp_path / "D99-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="filename"):
        descriptors.load_units(tmp_path)


# ---------------------------------------------------------------------------
# Retention validation — alpha-engine-config-I10831 deliverable 3
# ---------------------------------------------------------------------------


def test_a_sub_hourly_unit_without_retention_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = 5
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="must declare `run_manifest_retention`"):
        descriptors.load_units(tmp_path)


def test_a_sub_hourly_unit_with_retention_loads(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = 5
    base["run_manifest_retention"] = "400 days"
    base["run_manifest_retention_reason"] = "a full seasonal cycle of session slots"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    units = descriptors.load_units(tmp_path)
    assert units[0].cadence_minutes == 5
    assert units[0].run_manifest_retention_days == 400


def test_an_hourly_or_slower_unit_needs_no_retention(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = 60
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    units = descriptors.load_units(tmp_path)
    assert units[0].run_manifest_retention_days is None


def test_a_non_integer_cadence_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = "five"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="must be an integer"):
        descriptors.load_units(tmp_path)


def test_a_malformed_retention_shape_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = 5
    base["run_manifest_retention"] = "a long time"
    base["run_manifest_retention_reason"] = "because"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="is not `<positive integer> days`"):
        descriptors.load_units(tmp_path)


def test_a_retention_with_no_reason_is_refused(tmp_path):
    base = yaml.safe_load((descriptors.UNITS_DIR / "D01-constituents.yaml").read_text())
    base["trigger"]["cadence_minutes"] = 5
    base["run_manifest_retention"] = "400 days"
    (tmp_path / "D01-constituents.yaml").write_text(yaml.safe_dump(base))
    with pytest.raises(DescriptorError, match="no run_manifest_retention_reason"):
        descriptors.load_units(tmp_path)


def test_d37_declares_a_validated_retention(units):
    d37 = next(u for u in units if u.unit_id == "D37")
    assert d37.cadence_minutes == 5
    assert d37.run_manifest_retention_days == 400


def test_every_declared_na_uses_the_closed_taxonomy(units):
    for unit in units:
        for name, block in unit.guards.items():
            if block.get("state") == "not_applicable":
                assert block["na_code"] in descriptors.NA_TAXONOMY, f"{unit.unit_id}/{name}"


# ---------------------------------------------------------------------------
# The writer inventory — the one clause that notices a unit nobody registered
# ---------------------------------------------------------------------------


def test_the_writer_inventory_reconciles_today(units):
    reading = scan(units)
    assert reading.undeclared == [], (
        "write-site file(s) with no descriptor and no declared parent: "
        f"{reading.undeclared}. Add the unit descriptor, or attribute the module to its "
        "parent in data_gate/config/writer_inventory.yaml with a reason."
    )
    assert reading.units_without_write_site == []
    assert not reading.parse_failures


def test_a_new_writer_with_no_descriptor_turns_the_board_red(units, tmp_path, monkeypatch):
    """The mechanism, exercised: an unclaimed write site fails the clause."""
    fake_root = tmp_path / "repo"
    (fake_root / "collectors").mkdir(parents=True)
    (fake_root / "collectors" / "brand_new_collector.py").write_text(
        textwrap.dedent(
            """
            def collect(client):
                client.put_object(Bucket="b", Key="k", Body=b"{}")
            """
        )
    )
    monkeypatch.setattr("data_gate.inventory.REPO_ROOT", fake_root)
    reading = scan(units)
    assert "collectors/brand_new_collector.py" in reading.undeclared


def test_a_file_that_will_not_parse_is_not_no_write_sites(units, tmp_path, monkeypatch):
    fake_root = tmp_path / "repo"
    (fake_root / "collectors").mkdir(parents=True)
    (fake_root / "collectors" / "broken.py").write_text("def f(:\n")
    monkeypatch.setattr("data_gate.inventory.REPO_ROOT", fake_root)
    reading = scan(units)
    assert "collectors/broken.py" in reading.parse_failures
    assert reading.ok is False


# ---------------------------------------------------------------------------
# The scan POPULATION itself — I10830: `roots` is a hand-maintained list, and
# a producer dropped outside every root is invisible to the clause above
# rather than failing it (weekly_collector.py sat at the repo's top level,
# one directory above every walked root, while D17/D19/D33/D34's REVERSE
# check stayed green via the collectors/daily_closes.py shared-writer entry —
# so its own ~30 write sites were never scanned and `writers_declared` read
# MET over an incomplete population for months). This test re-derives the
# producer population from source on every run, independent of
# writer_inventory.yaml's declared `roots`, so a NEW producer placed outside
# both `roots` and `extra_paths` turns this test red instead of staying
# silently unscanned.
# ---------------------------------------------------------------------------

# Verb subset deliberately narrower than writer_inventory.yaml's `write_calls`:
# `put_object`/`upload_file`/`upload_fileobj`/`copy_object`/`write_batch`/
# `write_metadata`/`stage`/`to_parquet` are essentially unambiguous S3/ArcticDB/
# parquet writes wherever they appear. `write`/`append`/`update` are NOT — they
# collide with `list.append`/`dict.update`/plain file `.write()` constantly
# outside the already-curated roots, and applying them repo-wide would drown
# a real finding in false positives from files that have never been a producer
# (emailer.py, sf_preflight.py, the data_gate tool itself, …). Narrowing here
# trades false negatives on ambiguous verbs for a signal an autonomous sweep
# can act on; the four narrow verbs are exactly the ones that found
# weekly_collector.py in the first place (I10830 root-cause investigation).
_UNAMBIGUOUS_WRITE_VERBS = frozenset(
    {
        "put_object",
        "upload_file",
        "upload_fileobj",
        "copy_object",
        "write_batch",
        "write_metadata",
        "stage",
        "to_parquet",
    }
)

# Directories that are never a producer's home. `infrastructure/` is the
# watch/alert/dispatch/ops plane (writer_inventory.yaml boundary 1, verbatim:
# "almost entirely the watch/alert/dispatch plane — it writes ops artifacts,
# not published data keys, and it is a different component with its own
# registry rows"); its two genuine data producers are named individually in
# `extra_paths` and stay covered because that membership is checked
# separately below, independent of this prefix skip.
_NON_PRODUCER_DIR_PREFIXES = (
    "tests/",
    "migrations/",
    "data_gate/",  # the gate tool itself, not a thing it grades
    "infrastructure/",  # ops plane; its two producers are named in extra_paths
    ".venv/",
    ".git/",
    ".worktrees/",
    "__pycache__/",
)

# Individual files, anywhere in the tree, whose only write call sites publish
# an OPS artifact (a permission sentinel, a sweep's own verdict) rather than a
# data key a unit descriptor would claim — verified by reading each one
# (I10830). A third entry here needs the same read before being added.
_KNOWN_OPS_PLANE_WRITE_SITES = {
    "preflight.py": (
        "_check_s3_writeable_sentinel() PUTs a throwaway "
        "preflight/sentinel-<uuid>.txt to prove the IAM grant, then relies on "
        "the DELETE to clean it up — not a published data key."
    ),
    "shadow/arctic_seed.py": (
        "ensure_seeded() write_batch/write calls land ONLY in "
        "shadow_{YYYYMMDD}_* ArcticDB libraries (every name is re-checked "
        "against the shadow prefix and LIVE_ARCTIC_LIBRARIES; live handles are "
        "read-only wrappers) — the pre-cutover shadow run's private copy of "
        "live state (alpha-engine-config-I10866), consumed by no pipeline and "
        "claimed by no unit descriptor, not a published data key."
    ),
    "validators/stage_output_sweep.py": (
        "_publish_verdict() writes the sweep's OWN verdict document "
        "(alpha-engine-config-I7167) — an ops artifact about other stages' "
        "output, not a data key any unit descriptor would claim."
    ),
    "validators/expectations.py": (
        "publish_completeness_metric() PUTs a unit's completeness MetricRecord "
        "at data_collection/metrics/eod_completeness/{trading_day}.json "
        "(alpha-engine-config-I10780) — gate evidence ABOUT a unit's output, "
        "read by the data.<unit>.completeness clause, not a published data key."
    ),
}


def _source_derived_write_sites(repo_root):
    """AST-scan the WHOLE repo tree for unambiguous S3/ArcticDB/parquet write
    call sites — never reading writer_inventory.yaml's `roots`, so this cannot
    agree with the scope by construction."""
    found: dict[str, list[str]] = {}
    for path in sorted(repo_root.rglob("*.py")):
        rel = path.relative_to(repo_root).as_posix()
        if rel.startswith(_NON_PRODUCER_DIR_PREFIXES):
            continue
        name = path.name
        if name == "conftest.py" or name.startswith("test_") or name.endswith("_test.py"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            # A file that won't parse is covered by the inventory scan's own
            # parse_failures clause (test_a_file_that_will_not_parse_is_not_no_write_sites)
            # once it is inside the scan population; not this test's concern.
            continue
        sites = [
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _UNAMBIGUOUS_WRITE_VERBS
        ]
        if sites:
            found[rel] = sites
    return found


def test_every_genuine_write_site_sits_inside_the_scanned_population():
    scope = load_inventory_scope()
    outside = {}
    for rel, verbs in _source_derived_write_sites(REPO_ROOT).items():
        top_level_dir = rel.split("/", 1)[0]
        if top_level_dir in scope.roots or rel in scope.extra_paths:
            continue
        if rel in _KNOWN_OPS_PLANE_WRITE_SITES:
            continue
        outside[rel] = verbs
    assert not outside, (
        "producer module(s) with unambiguous S3/ArcticDB/parquet write call "
        f"sites sit outside writer_inventory.yaml's scan population: {outside}. "
        "Add each to `roots` (a whole producer directory) or `extra_paths` (a "
        "single file) in data_gate/config/writer_inventory.yaml with a reason, "
        "or — only if it genuinely writes an ops artifact rather than a "
        "published data key — add it to _KNOWN_OPS_PLANE_WRITE_SITES above "
        "with the reason, after reading the call site."
    )


# ---------------------------------------------------------------------------
# Evidence readers
# ---------------------------------------------------------------------------


def test_a_guard_is_not_commissioned_by_an_absorbed_fault_record(units):
    """Only an `induced` record is evidence the guard actually fired."""
    unit = next(u for u in units if u.unit_id == "D20")
    store = EmptyStore(
        {
            "faults/D20/cardinality/latest.json": json.dumps(
                {"outcome": "absorbed", "as_of": "2026-09-14"}
            ).encode()
        }
    )
    reading = evidence.read_guard_commissioning(store, unit, "cardinality", trading_day=TRADING_DAY)
    assert reading.met is False
    assert "absorbed" in reading.detail


def test_a_guard_commissioned_by_an_induced_fault_reads_met(units):
    unit = next(u for u in units if u.unit_id == "D20")
    store = EmptyStore(
        {
            "faults/D20/cardinality/latest.json": json.dumps(
                {"outcome": "induced", "as_of": "2026-09-14"}
            ).encode()
        }
    )
    reading = evidence.read_guard_commissioning(store, unit, "cardinality", trading_day=TRADING_DAY)
    assert reading.met is True


def test_an_objective_status_outside_the_closed_set_is_a_finding():
    store = EmptyStore(
        {"metrics/cost/monthly/latest.json": json.dumps({"status": "probably ok"}).encode()}
    )
    reading = evidence.read_objective(store, "metrics/cost/monthly/latest.json")
    assert reading.met is False
    assert "closed set" in reading.detail


def test_an_objective_with_no_emitter_is_unobserved_not_met():
    reading = evidence.read_objective(EmptyStore(), "metrics/cost/monthly/latest.json")
    assert reading.met is False
    assert reading.unmeasurable is False, "absence is an ANSWER; only a failed read is unmeasurable"


def test_a_denied_objective_read_is_unmeasurable_not_a_finding():
    reading = evidence.read_objective(DeniedStore(), "metrics/cost/monthly/latest.json")
    assert reading.unmeasurable is True


# ---------------------------------------------------------------------------
# The `data-cutover-ready` sub-gate (`alpha-engine-config-I10777`, plan §6.2)
# ---------------------------------------------------------------------------


def test_cutover_ready_is_registered_but_carries_no_numbered_ceiling():
    assert "data-cutover-ready" in GATES
    assert GATES["data-cutover-ready"] is None


def test_cutover_ready_has_exactly_its_four_clauses(board):
    names = {c.name for c in board if c.phase == "data-cutover-ready"}
    assert names == set(clause_module.CUTOVER_READY_CLAUSES)


def test_cutover_ready_is_unmet_with_nothing_published(board):
    """Every clause it reads is a phase-1 evidence gap on an empty store, so
    the sub-gate itself reads UNMET/UNMEASURABLE — never MET on day one."""
    result = evaluate(EmptyStore(), gate="data-cutover-ready", trading_day=TRADING_DAY, all_clauses=board)
    assert result.met is False
    assert len(result.clauses) == 4


def test_stack_check_live_absent_is_unmet_not_unmeasurable():
    """No emitter yet is a finding about the producer side (`read_objective`'s
    rule), not about our own read."""
    reading = evidence.read_stack_check_live(EmptyStore())
    assert reading.met is False
    assert reading.unmeasurable is False
    assert evidence.STACK_CHECK_LIVE_KEY in reading.evidence


_CHECK_LIVE_NOW = dt.datetime(2026, 9, 15, 12, 0, tzinfo=dt.timezone.utc)


def _verdict_store(**over):
    doc = {
        "schema_version": evidence.STACK_CHECK_LIVE_SCHEMA,
        "as_of": "2026-09-14T22:31:00Z",
        "code_sha": "a" * 40,
        "stack": "nousergon-data-collection",
        "measured": True,
        "in_sync": True,
        "drift": [],
        "error": None,
        **over,
    }
    return EmptyStore({evidence.STACK_CHECK_LIVE_KEY: json.dumps(doc).encode()})


def test_stack_check_live_reads_the_published_verdict():
    assert evidence.read_stack_check_live(_verdict_store(), now=_CHECK_LIVE_NOW).met is True
    drift = evidence.read_stack_check_live(_verdict_store(in_sync=False, drift=["x"]), now=_CHECK_LIVE_NOW)
    assert drift.met is False and drift.unmeasurable is False and "x" in drift.detail


def test_stack_check_live_that_could_not_measure_is_unmeasurable():
    reading = evidence.read_stack_check_live(
        _verdict_store(measured=False, in_sync=False, error="AccessDenied"), now=_CHECK_LIVE_NOW
    )
    assert reading.unmeasurable is True and "AccessDenied" in reading.detail


def test_a_stale_in_sync_verdict_is_unmet():
    reading = evidence.read_stack_check_live(_verdict_store(as_of="2026-09-01T00:00:00Z"), now=_CHECK_LIVE_NOW)
    assert reading.met is False and reading.unmeasurable is False


def test_the_pre_i10870_status_shape_is_a_finding_not_a_pass():
    store = EmptyStore({evidence.STACK_CHECK_LIVE_KEY: json.dumps({"status": "clean"}).encode()})
    assert evidence.read_stack_check_live(store, now=_CHECK_LIVE_NOW).met is False


def test_stack_check_live_denied_is_unmeasurable():
    reading = evidence.read_stack_check_live(DeniedStore())
    assert reading.unmeasurable is True


def test_parity_absent_is_unmet_naming_i10778():
    reading = evidence.read_parity(EmptyStore(), trading_day=TRADING_DAY)
    assert reading.unmeasurable is False
    assert reading.met is False
    assert "I10778" in reading.detail
    assert reading.evidence == (f"{evidence.PARITY_KEY_PREFIX}*.json",)


def _parity_report(trading_day, *, met=True, matched=3, total=3, exceptions=None):
    summary = {"total": total, "match": matched, **(exceptions or {})}
    return json.dumps(
        {
            "schema_version": "data_parity_report.v1",
            "trading_day": trading_day.isoformat(),
            "generated_at": f"{trading_day.isoformat()}T21:00:00Z",
            "met": met,
            "summary": summary,
        }
    ).encode()


def test_parity_fresh_prior_day_report_is_graded_not_absent():
    """The bug this issue fixes: a shadow run for the day BEFORE the gate's own
    trading day must be found and graded, never treated as absent just because
    it is not filed under the gate's own trading_day key."""
    from nousergon_lib.trading_calendar import previous_trading_day

    report_day = previous_trading_day(TRADING_DAY)
    key = evidence.parity_store_key(report_day)
    store = EmptyStore({key: _parity_report(report_day)})
    reading = evidence.read_parity(store, trading_day=TRADING_DAY)
    assert reading.unmeasurable is False
    assert reading.met is True
    assert key in reading.evidence
    assert report_day.isoformat() in reading.detail


def test_parity_stale_report_reads_unmet_naming_age():
    from nousergon_lib.trading_calendar import subtract_trading_days

    stale_day = subtract_trading_days(TRADING_DAY, evidence.PARITY_FRESHNESS_TRADING_DAYS + 1)
    key = evidence.parity_store_key(stale_day)
    store = EmptyStore({key: _parity_report(stale_day)})
    reading = evidence.read_parity(store, trading_day=TRADING_DAY)
    assert reading.unmeasurable is False
    assert reading.met is False
    assert "Stale" in reading.detail
    assert str(evidence.PARITY_FRESHNESS_TRADING_DAYS + 1) in reading.detail
    assert key in reading.evidence


def test_parity_never_selects_a_report_dated_after_the_gate_day():
    """A report filed for a day after the gate's own trading day is never
    picked, even when it is the only report in the store."""
    from nousergon_lib.trading_calendar import previous_trading_day

    future_day = TRADING_DAY + dt.timedelta(days=1)
    past_day = previous_trading_day(TRADING_DAY)
    store = EmptyStore(
        {
            evidence.parity_store_key(future_day): _parity_report(future_day),
            evidence.parity_store_key(past_day): _parity_report(past_day),
        }
    )
    reading = evidence.read_parity(store, trading_day=TRADING_DAY)
    assert reading.met is True
    assert evidence.parity_store_key(past_day) in reading.evidence
    assert evidence.parity_store_key(future_day) not in reading.evidence


def test_parity_denied_listing_is_unmeasurable():
    reading = evidence.read_parity(DeniedStore(), trading_day=TRADING_DAY)
    assert reading.unmeasurable is True


class _FakeIamClient:
    """A `list_role_policies` double — `NoSuchEntity` for a declared-missing
    role, success otherwise, and nothing else implemented (a call this
    module should never make would raise `AttributeError`, loud)."""

    def __init__(self, missing: set[str] = frozenset()) -> None:
        self.missing = missing
        self.calls: list[str] = []

    def list_role_policies(self, *, RoleName: str) -> dict:
        self.calls.append(RoleName)
        if RoleName in self.missing:
            from botocore.exceptions import ClientError

            raise ClientError(
                {"Error": {"Code": "NoSuchEntity", "Message": "no such role"}},
                "ListRolePolicies",
            )
        return {"PolicyNames": []}


def test_roles_bootstrapped_is_unmeasurable_with_no_iam_client():
    """`LocalStore`/`EmptyStore` carry no `iam_client` attribute at all — this
    reads UNMEASURABLE without ever reaching for `boto3`."""
    reading = evidence.read_roles_bootstrapped(EmptyStore(), clause_module.CUTOVER_READY_ROLES)
    assert reading.unmeasurable is True


def test_roles_bootstrapped_met_when_every_role_exists():
    store = EmptyStore()
    store.iam_client = _FakeIamClient()
    reading = evidence.read_roles_bootstrapped(store, clause_module.CUTOVER_READY_ROLES)
    assert reading.met is True
    assert sorted(store.iam_client.calls) == sorted(clause_module.CUTOVER_READY_ROLES)


def test_roles_bootstrapped_unmet_via_list_role_policies_nosuchentity():
    """The grant gap this issue names: no `iam:GetRole`, so existence is read
    through `ListRolePolicies`'s `NoSuchEntity` instead."""
    store = EmptyStore()
    store.iam_client = _FakeIamClient(missing={clause_module.CUTOVER_READY_ROLES[0]})
    reading = evidence.read_roles_bootstrapped(store, clause_module.CUTOVER_READY_ROLES)
    assert reading.met is False
    assert reading.unmeasurable is False
    assert clause_module.CUTOVER_READY_ROLES[0] in reading.detail


def test_units_covered_reconciles_against_the_37_named_in_the_plan(units):
    """The reconciliation is arithmetic and is ALWAYS printed: the plan's
    figure, the declared population, the recorded retirements by name, and
    what is left to grade. The old version of this test only asserted
    anything when the two numbers disagreed, so it passed vacuously the
    moment they agreed (alpha-engine-config-I10908)."""
    declared = clause_module._sf_only_units(units)
    retired = sorted(u.unit_id for u in declared if u.retired)
    graded = [u for u in declared if not u.retired]
    clause = clause_module._clause_cutover_ready_units_covered(EmptyStore(), units, trading_day=TRADING_DAY)

    assert str(clause_module.PLAN_SF_ONLY_UNITS) in clause.detail
    assert f"descriptors declare {len(declared)}" in clause.detail
    assert f"leaving {len(graded)} graded here" in clause.detail
    for unit_id in retired:
        assert unit_id in clause.detail


def test_units_covered_membership_keys_on_the_successor_not_the_trigger_kind(units):
    """`alpha-engine-config-I10908`. D33's trigger kind is `eventbridge-rule`
    and its successor is `ne-data-collection-daily-heal (nousergon-data-PR1701,
    DISABLED)` — the standalone stack replaces it exactly like the
    step-functions units. Keying membership on `kind == "step-functions"`
    dropped it silently, so `units_covered` could read a clean MET with D33's
    `survives_phase4` never graded at all. A gate that reads MET over an
    ungraded member is worse than one that reads UNMET."""
    members = {u.unit_id for u in clause_module._sf_only_units(units)}
    kinds = {
        u.unit_id: str((u.raw.get("trigger") or {}).get("kind") or "")
        for u in units
    }

    declares_successor = {
        u.unit_id
        for u in units
        if "nousergon-data-PR1701" in str((u.raw.get("trigger") or {}).get("successor") or "")
        or "alpha-engine-config-I10753" in str((u.raw.get("trigger") or {}).get("successor") or "")
    }
    assert declares_successor <= members, (
        "every unit declaring a PR1701/I10753 successor must be a member — the successor "
        "leg is what carries D33, whose kind is not step-functions"
    )

    # NOT an equality any more (alpha-engine-config-I11014). Membership is the
    # UNION of the successor leg and `kind == "step-functions"`, because
    # recording a retirement REWRITES the successor: D09, D40 and D41 are
    # step-functions units whose retirement is recorded, and under an equality
    # they fell out of the population before the retirement count ran. That
    # printed "0 carry a recorded retirement" as a structural constant and
    # invented a residual disagreement with plan §6.2's 37.
    #
    # Measured 2026-09-17: successor-token 34, step-functions 36, union 37 —
    # the plan's figure exactly — of which 3 retired leaves the 34 graded.
    # The assertion above still forbids the original defect (dropping D33 by
    # keying on kind alone); it just no longer forbids the union.
    extra = members - declares_successor
    assert all(
        str((u.raw.get("trigger") or {}).get("kind") or "") == "step-functions"
        for u in units
        if u.unit_id in extra
    ), "the only members beyond the successor leg may be step-functions units"

    dropped_by_kind = {u for u in declares_successor if kinds.get(u) != "step-functions"}
    assert dropped_by_kind, (
        "this regression test is only meaningful while at least one replaced unit "
        "has a non-step-functions trigger; if that is no longer true, keep the "
        "assertion above and delete this one deliberately"
    )
    assert dropped_by_kind <= members

    clause = clause_module._clause_cutover_ready_units_covered(EmptyStore(), units, trading_day=TRADING_DAY)
    graded = {u.unit_id for u in clause_module._sf_only_units(units) if not u.retired}
    for unit_id in sorted(dropped_by_kind & graded):
        assert any(unit_id in name for name in clause.evidence), (
            f"{unit_id} is replaced by the standalone stack but its descriptor is not in "
            "the rollup's evidence"
        )


def test_phase1_units_produced_is_unmeasurable_when_no_survives_phase4_evidence_exists(units):
    """Every `survives_phase4` base clause is a phase-0 stub on an empty
    store, so the PHASE-1 rollup over them is UNMEASURABLE — not a false UNMET.

    It moved off `data.cutover_ready.units_covered` with the live reading
    itself (`alpha-engine-config-I10989`): that sub-gate is read before the
    cutover and may not depend on production evidence that only the cutover can
    produce.
    """
    clause = clause_module._clause_phase1_units_produced(EmptyStore(), units, trading_day=TRADING_DAY)
    assert clause.unmeasurable is True
    assert clause.met is False


def test_units_covered_answers_statically_with_no_store_reads(units):
    """`alpha-engine-config-I10989`: the sub-gate's leg is answered from the
    committed stack definition and the committed descriptors, so an empty store
    and a DENIED store give the same verdict — it reads neither."""
    empty = clause_module._clause_cutover_ready_units_covered(EmptyStore(), units, trading_day=TRADING_DAY)
    denied = clause_module._clause_cutover_ready_units_covered(DeniedStore(), units, trading_day=TRADING_DAY)
    assert empty.unmeasurable is False
    assert (empty.met, empty.detail, empty.evidence) == (denied.met, denied.detail, denied.evidence)
    assert "verify_units" in empty.requirement


def test_the_board_document_publishes_unit_id_per_row(tmp_path):
    """`alpha-engine-config-I10802`: every board row carries `unit_id`, a
    `D`-number for a base/guard clause or a category label otherwise."""
    _result, _ladder, board_doc = run(LocalStore(tmp_path), gate="data-phase0", trading_day=TRADING_DAY)
    by_clause = {row["clause"]: row["unit_id"] for row in board_doc["rows"]}
    assert by_clause["data.D01.schema_contract"] == "D01"
    assert by_clause["data.D20.guard.cardinality"] == "D20"
    assert by_clause["data.board.population_complete"] == "board"
    assert by_clause["data.gate.ladder_fresh"] == "gate"
    assert all(row["unit_id"] for row in board_doc["rows"]), "every row names a unit_id"


# ---------------------------------------------------------------------------
# The phases declaration
# ---------------------------------------------------------------------------


def test_every_declared_phase_has_a_registered_gate(phases):
    for phase in phases:
        assert phase.gate in GATES, f"{phase.id} names an unregistered gate {phase.gate!r}"


def test_every_rung_names_its_own_tracker_now_that_p26_is_filed(board, phases):
    """P-26 filed 2026-09-14 as alpha-engine-config-I10792..I10795, one per phase."""
    clause = next(c for c in board if c.name == "data.board.phase_trackers_declared")
    assert clause.met is True, clause.detail
    assert {p.tracker_issue for p in phases} == {10792, 10793, 10794, 10795}
    assert not any(p.tracker_is_placeholder for p in phases)


def test_a_rung_pointing_at_the_parent_issue_holds_the_board_clause(phases):
    """A rung that falls back to the parent KEY issue must keep the clause UNMET —
    a ladder pointed at one issue looks tracked and is not."""
    import dataclasses

    degraded = [dataclasses.replace(phases[1], tracker_issue=10748, tracker_is_placeholder=True)] + [
        p for p in phases if p.id != phases[1].id
    ]
    clause = clause_module._clause_board_phase_trackers_declared(EmptyStore(), degraded)
    assert clause.met is False
    assert "P-26" in clause.detail and phases[1].id in clause.detail


# ---------------------------------------------------------------------------
# Completeness clause — `alpha-engine-config-I10780` (plan item P-13).
# ---------------------------------------------------------------------------


def test_only_d20_gets_a_completeness_clause(board):
    """`data_collection/metrics/eod_completeness/{trading_day}.json` is a single
    non-unit-scoped key naming the EOD spine specifically — the clause is
    generated for D20 only, not for every unit that merely declares a
    `completeness` block (most are `status: proposed`, no reader behind them)."""
    names = [c.name for c in board if c.name.endswith(".completeness") and c.name.startswith("data.D")]
    assert names == ["data.D20.completeness"]


def test_the_completeness_clause_is_tagged_phase_1_observe(board):
    clause = next(c for c in board if c.name == "data.D20.completeness")
    assert clause.phase == "data-phase1"


def test_no_completeness_metric_published_is_unmet_not_a_vacuous_pass():
    """Absent means we looked and there is nothing there — UNMET (we looked
    successfully), not UNMEASURABLE (we could not look at all)."""
    unit = next(u for u in load_units() if u.unit_id == "D20")
    reading = evidence.read_completeness_metric(EmptyStore(), unit, trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is False
    assert "eod_completeness" in reading.detail


def test_a_green_completeness_metric_reads_met():
    unit = next(u for u in load_units() if u.unit_id == "D20")
    store = EmptyStore(
        {
            "metrics/eod_completeness/2026-09-14.json": json.dumps(
                {
                    "status": "GREEN",
                    "value": 1.0,
                    "target": 1.0,
                    "status_reason": "D20: 80/80 covered — zero undeclared misses",
                    "last_updated_utc": "2026-09-14T21:15:00Z",
                }
            ).encode()
        }
    )
    reading = evidence.read_completeness_metric(store, unit, trading_day=TRADING_DAY)
    assert reading.met is True
    assert reading.unmeasurable is False
    assert reading.as_of == "2026-09-14T21:15:00Z"


def test_a_red_completeness_metric_reads_unmet_but_measured():
    """RED is a real reading — the guard looked and found a gap — never
    UNMEASURABLE, which would let a genuine miss hide behind 'we didn't look'."""
    unit = next(u for u in load_units() if u.unit_id == "D20")
    store = EmptyStore(
        {
            "metrics/eod_completeness/2026-09-14.json": json.dumps(
                {
                    "status": "RED",
                    "value": 0.9,
                    "target": 1.0,
                    "status_reason": "D20: undeclared miss",
                    "last_updated_utc": "2026-09-14T21:15:00Z",
                }
            ).encode()
        }
    )
    reading = evidence.read_completeness_metric(store, unit, trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is False


def test_an_na_status_completeness_metric_is_unmeasurable():
    unit = next(u for u in load_units() if u.unit_id == "D20")
    store = EmptyStore(
        {
            "metrics/eod_completeness/2026-09-14.json": json.dumps(
                {
                    "status": "N/A-MISSING-INPUT",
                    "status_reason": "denominator artifact unreadable",
                    "last_updated_utc": "2026-09-14T21:15:00Z",
                }
            ).encode()
        }
    )
    reading = evidence.read_completeness_metric(store, unit, trading_day=TRADING_DAY)
    assert reading.met is False
    assert reading.unmeasurable is True


def test_a_denied_completeness_read_is_unmeasurable():
    unit = next(u for u in load_units() if u.unit_id == "D20")
    reading = evidence.read_completeness_metric(DeniedStore(), unit, trading_day=TRADING_DAY)
    assert reading.unmeasurable is True
