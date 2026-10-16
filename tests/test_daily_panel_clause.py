"""`data.phase3.daily_panel_adopted` — the daily panel acceptance clause (audit gap A10).

`alpha-engine-config-I10791` / `-I10795`. Graded the way the rest of the board
is: red by default over an empty store, MET only when every leg is proven on
evidence, and each leg able to turn the clause red on its own. No AWS and no
GitHub: the store is in-memory and the consumer repository is a dict.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from contracts import daily_panel as dp
from data_gate import panel
from data_gate.clauses import EXIT_CRITERION_CLAUSES, generate
from data_gate.descriptors import load_units
from data_gate.read import evaluate, load_phases
from data_gate.sources import SourceUnavailable

from tests.data_gate_support import DeniedStore, EmptyStore

DAY = dt.date(2026, 10, 2)  # a Friday session
SHA = "c" * 64

TRACK_A_BEFORE = '''
def _source(args, config):
    return ArcticPriceSource(config.arctic_bucket)

def handle_data_daily(args):
    source = _source(args, config)
    return run_daily(source=source)
'''

TRACK_A_AFTER = '''
from crucible.data.sources import PublishedPanelSource

def handle_data_daily(args):
    source = PublishedPanelSource(config.arctic_bucket, trading_day=args.trading_day)
    return run_daily(source=source)
'''

# crucible main after crucible#398 (alpha-engine-config-I12020): the compile
# moved out of the entrypoint into a helper it calls, where it still runs. The
# module NAMES the read site, so a read of the entrypoint's own body alone
# graded this MET (alpha-engine-config-I10791).
TRACK_A_COMPILE_IN_HELPER = '''
from crucible.data.sources import PublishedPanelSource

def _source(args, config):
    return ArcticPriceSource(config.arctic_bucket)

def _panel(args):
    return PublishedPanelSource(config.arctic_bucket, trading_day=args.trading_day)

def handle_data_daily(args):
    with _daily_chain_lease(args):
        code = _data_daily_body(args)
    return code

def _daily_chain_lease(args):
    return lock(args)

def _data_daily_body(args):
    source = _source(args, config)
    return run_daily(source=source)
'''

SOURCES = "class PublishedPanelSource(PriceSource):\n    name = 'published-panel'\n"


class FakeGitHub:
    """`data_gate.sources.GitHubContents.read_file`'s contract over a dict."""

    def __init__(self, files: dict[str, bytes | None], *, unavailable: bool = False) -> None:
        self.files = files
        self.unavailable = unavailable

    def read_file(self, repo: str, path: str):
        if self.unavailable:
            raise SourceUnavailable(f"GitHub answered HTTP 503 for nousergon/{repo}:{path}")
        if repo != dp.CONSUMER.repo or path not in self.files:
            return None, None
        return "file", self.files[path]


def _pin() -> bytes:
    return json.dumps(dp.load_schema("row")).encode()


def _consumer_files(**overrides) -> dict[str, bytes]:
    files = {
        dp.CONSUMER.pin_path: _pin(),
        "crucible/data/sources.py": SOURCES.encode(),
        dp.CONSUMER.entrypoint_path: TRACK_A_AFTER.encode(),
    }
    files.update(overrides)
    return {k: v for k, v in files.items() if v is not None}


def _manifest(day: dt.date = DAY, **overrides) -> dict:
    manifest = {
        "schema_version": dp.MANIFEST_SCHEMA_VERSION,
        "panel_schema_version": dp.PANEL_SCHEMA_VERSION,
        "trading_day": day.isoformat(),
        "panel_key": dp.panel_key(day),
        "panel_sha256": SHA,
        "panel_bytes": 1234,
        "columns": list(dp.PANEL_COLUMNS),
        "row_count": 280000,
        "symbol_count": 905,
        "symbols_on_trading_day": 903,
        "session_count": 410,
        "first_session": "2025-05-20",
        "last_session": day.isoformat(),
        "lookback_calendar_days": 600,
        "source": {"store": "arcticdb", "library": "universe"},
        "generated_at": f"{day.isoformat()}T23:05:00Z",
        "producer": {"module": "builders.daily_panel", "code_sha": "0123abc"},
    }
    manifest.update(overrides)
    return manifest


def _receipt(day: dt.date = DAY, **overrides) -> dict:
    receipt = {
        "schema_version": dp.PARITY_SCHEMA_VERSION,
        "trading_day": day.isoformat(),
        "verdict": "equivalent",
        "tolerance": dict(dp.PARITY_TOLERANCE),
        "producer": {"key": dp.panel_key(day), "sha256": SHA, "trading_day": day.isoformat(), "rows": 280000},
        "consumer": {"key": "crucible-v2: data/panel.parquet", "sha256": "d" * 64, "trading_day": day.isoformat(),
                     "rows": 279000},
        "rows_compared": 279000,
        "tickers_compared": 903,
        "missing_in_producer": 0,
        "missing_in_consumer": 0,
        "value_mismatches": 0,
        "max_abs_diff": {},
        "max_rel_diff": {},
        "examples": [],
        "generated_at": f"{day.isoformat()}T23:20:00Z",
    }
    receipt.update(overrides)
    return receipt


def _rel(key: str) -> str:
    return dp.store_relative(key)


def _store(*, manifest=None, parquet=True, receipt=None, github=None) -> EmptyStore:
    objects: dict[str, bytes] = {}
    manifest = _manifest() if manifest is None else manifest
    if manifest is not False:
        day = manifest["trading_day"]
        objects[_rel(dp.manifest_key(day))] = json.dumps(manifest).encode()
        if parquet:
            objects[_rel(dp.panel_key(day))] = b"PAR1"
    receipt = _receipt() if receipt is None else receipt
    if receipt is not False:
        objects[_rel(dp.parity_key(receipt["trading_day"]))] = json.dumps(receipt).encode()
    store = EmptyStore(objects)
    store.github_contents = FakeGitHub(_consumer_files()) if github is None else github
    return store


def _read(store, trading_day: dt.date = DAY):
    return panel.read_daily_panel_adopted(store, trading_day=trading_day)


def _leg(reading, name: str) -> str:
    """The rendered state of one leg, read back out of the detail."""
    marker = f"[{name} "
    start = reading.detail.index(marker) + len(marker)
    return reading.detail[start : reading.detail.index("]", start)]


# -- the board --------------------------------------------------------------


@pytest.fixture(scope="module")
def board():
    return generate(EmptyStore(), load_units(), load_phases(), trading_day=DAY)


def test_the_clause_is_on_the_board_and_graded_by_phase_3_only(board):
    clause = next(c for c in board if c.name == panel.PANEL_CLAUSE)
    assert clause.phase == "data-phase3"
    assert panel.PANEL_CLAUSE in EXIT_CRITERION_CLAUSES
    graded = {c.name for c in evaluate(EmptyStore(), gate="data-phase3", trading_day=DAY, all_clauses=board).clauses}
    assert panel.PANEL_CLAUSE in graded
    for gate in ("data-phase0", "data-phase1", "data-phase2"):
        lower = evaluate(EmptyStore(), gate=gate, trading_day=DAY, all_clauses=board).clauses
        assert panel.PANEL_CLAUSE not in {c.name for c in lower}


def test_the_clause_is_red_over_an_empty_store_and_names_every_leg(board):
    clause = next(c for c in board if c.name == panel.PANEL_CLAUSE)
    assert not clause.met
    for leg in ("contract", "published", "consumer_pin", "parity", "direct_compile_removed"):
        assert f"[{leg} " in clause.detail
    assert clause.requirement and clause.evidence and clause.source


def test_the_contract_leg_holds_in_this_tree():
    assert _leg(_read(EmptyStore()), "contract") == "MET"


# -- MET, and each leg's way back to red ------------------------------------


def test_every_leg_proven_is_met():
    reading = _read(_store())
    assert reading.met, reading.detail
    assert not reading.unmeasurable
    assert reading.as_of == "2026-10-02T23:05:00Z"


def test_a_weekend_read_grades_the_friday_panel():
    assert _read(_store(), trading_day=dt.date(2026, 10, 4)).met


def test_the_previous_session_panel_still_counts_before_today_publishes():
    reading = _read(_store(), trading_day=dt.date(2026, 10, 5))
    assert _leg(reading, "published") == "MET", reading.detail


def test_a_panel_two_sessions_old_is_a_stopped_publisher():
    reading = _read(_store(), trading_day=dt.date(2026, 10, 6))
    assert _leg(reading, "published") == "UNMET"
    assert not reading.met


@pytest.mark.parametrize(
    "store, leg, phrase",
    [
        (lambda: _store(manifest=False), "published", "no panel published"),
        (lambda: _store(parquet=False), "published", "incomplete publish"),
        (lambda: _store(manifest=_manifest(columns=list(reversed(dp.PANEL_COLUMNS)))), "published", "columns"),
        (lambda: _store(manifest=_manifest(panel_schema_version="panel.v1")), "published", "breaks"),
        (lambda: _store(receipt=False), "parity", "no parity receipt"),
        (lambda: _store(receipt=_receipt(verdict="divergent", value_mismatches=3)), "parity", "verdict divergent"),
        (lambda: _store(receipt=_receipt(tolerance={"price_rel": 0.01, "volume_abs": 0.0})), "parity",
         "not the declared"),
        (lambda: _store(receipt=_receipt(producer={"key": "k", "sha256": "e" * 64, "trading_day": "2026-10-02",
                                                    "rows": 1})), "parity", "is not the panel published"),
        (lambda: _store(receipt=_receipt(consumer={"key": "k", "sha256": "e" * 64, "trading_day": "2026-10-01",
                                                    "rows": 1})), "parity", "not the receipt's"),
        (lambda: _store(github=FakeGitHub(_consumer_files(**{dp.CONSUMER.pin_path: None}))), "consumer_pin",
         "ABSENT"),
        (lambda: _store(github=FakeGitHub(_consumer_files(**{dp.CONSUMER.pin_path: b'{"type": "object"}'}))),
         "consumer_pin", "STALE"),
        (lambda: _store(github=FakeGitHub(_consumer_files(**{"crucible/data/sources.py": b"class Other: pass"}))),
         "consumer_pin", "defines no class"),
        (lambda: _store(github=FakeGitHub(_consumer_files(**{dp.CONSUMER.entrypoint_path: TRACK_A_BEFORE.encode()}))),
         "direct_compile_removed", "still reaches"),
        (lambda: _store(github=FakeGitHub(_consumer_files(
            **{dp.CONSUMER.entrypoint_path: b"def handle_data_daily(args):\n    return run_daily(read())\n"}))),
         "direct_compile_removed", "undeclared"),
        (lambda: _store(github=FakeGitHub(_consumer_files(**{dp.CONSUMER.entrypoint_path: b"def other(): pass\n"}))),
         "direct_compile_removed", "defines no handle_data_daily"),
    ],
    ids=["no-manifest", "no-parquet", "wrong-columns", "wrong-version", "no-receipt", "divergent",
         "loosened-tolerance", "parity-on-another-panel", "parity-across-days", "pin-absent", "pin-stale",
         "no-read-site", "direct-compile-kept", "reads-undeclared", "no-entrypoint"],
)
def test_one_broken_leg_turns_the_clause_red(store, leg, phrase):
    reading = _read(store())
    assert not reading.met
    assert not reading.unmeasurable, "a definite finding is UNMET, not UNMEASURABLE"
    assert _leg(reading, leg) == "UNMET", reading.detail
    assert phrase in reading.detail


def _direct_compile_reading(track_a: str):
    return _read(_store(github=FakeGitHub(_consumer_files(**{dp.CONSUMER.entrypoint_path: track_a.encode()}))))


def test_a_compile_moved_into_a_helper_the_entrypoint_calls_is_not_removed():
    """Regression for alpha-engine-config-I10791: crucible#398's shape read MET."""
    reading = _direct_compile_reading(TRACK_A_COMPILE_IN_HELPER)
    assert not reading.met
    assert _leg(reading, "direct_compile_removed") == "UNMET", reading.detail
    assert "handle_data_daily -> _data_daily_body" in reading.detail


def test_the_compile_is_found_however_deep_and_however_it_is_reached():
    deeper = TRACK_A_COMPILE_IN_HELPER.replace(
        "def _data_daily_body(args):\n    source = _source(args, config)\n    return run_daily(source=source)",
        "def _data_daily_body(args):\n    return run_job(_compile, args)\n\n"
        "def _compile(args):\n    return run_daily(source=_build(args))\n\n"
        "def _build(args):\n    return ArcticPriceSource(config.arctic_bucket)",
    )
    assert deeper != TRACK_A_COMPILE_IN_HELPER
    reading = _direct_compile_reading(deeper)
    assert _leg(reading, "direct_compile_removed") == "UNMET", reading.detail
    assert "_data_daily_body -> _compile -> _build" in reading.detail


def test_a_compile_only_other_entrypoints_reach_does_not_block():
    """``data.weekly``/``data.heal`` keeping the direct compile is not ``data.daily`` keeping it."""
    track_a = TRACK_A_AFTER + (
        "\ndef _source(args, config):\n    return ArcticPriceSource(config.arctic_bucket)\n"
        "\ndef handle_data_weekly(args):\n    return run_weekly(source=_source(args, config))\n"
    )
    reading = _direct_compile_reading(track_a)
    assert _leg(reading, "direct_compile_removed") == "MET", reading.detail


def test_a_recursive_helper_terminates():
    track_a = TRACK_A_AFTER + "\ndef _again(args):\n    return _again(args)\n"
    track_a = track_a.replace("return run_daily(source=source)", "return _again(run_daily(source=source))")
    reading = _direct_compile_reading(track_a)
    assert _leg(reading, "direct_compile_removed") == "MET", reading.detail


def test_no_github_reader_is_unmeasurable_never_met():
    store = _store()
    store.github_contents = None
    reading = _read(store)
    assert not reading.met and reading.unmeasurable
    assert _leg(reading, "consumer_pin") == "UNMEASURABLE"
    assert _leg(reading, "direct_compile_removed") == "UNMEASURABLE"


def test_an_unreachable_consumer_repo_is_unmeasurable():
    reading = _read(_store(github=FakeGitHub({}, unavailable=True)))
    assert not reading.met and reading.unmeasurable


def test_a_denied_store_is_unmeasurable_not_absent():
    store = DeniedStore()
    store.github_contents = FakeGitHub(_consumer_files())
    reading = _read(store)
    assert not reading.met
    assert _leg(reading, "published") == "UNMEASURABLE"
    assert _leg(reading, "parity") == "UNMEASURABLE"


def test_the_latest_receipt_is_the_one_graded():
    """A later divergent receipt is a regression, and an older equivalent one does not hide it."""
    store = _store()
    later = dt.date(2026, 10, 5)
    store.objects[_rel(dp.manifest_key(later))] = json.dumps(_manifest(later)).encode()
    store.objects[_rel(dp.panel_key(later))] = b"PAR1"
    store.objects[_rel(dp.parity_key(later))] = json.dumps(_receipt(later, verdict="divergent")).encode()
    reading = _read(store, trading_day=later)
    assert _leg(reading, "parity") == "UNMET"
    assert "2026-10-05" in reading.detail
