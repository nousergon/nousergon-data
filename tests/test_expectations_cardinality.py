"""The cardinality guard (`alpha-engine-config-I10780`, plan item P-13,
extending `alpha-engine-config-I5935`).

``empty_fresh`` catches a unit that published nothing; ``cardinality`` catches
a unit that published something but not everything it was supposed to. These
tests are about the three properties that make the guard worth having: it
counts a real gap, it never silently resolves a suffix mismatch or a declared
exclusion into "not a miss", and an UNDECLARED miss is always named rather
than absorbed into a passing ratio.
"""

from __future__ import annotations

import pathlib

import pytest
from nousergon_lib.guard_mode import GuardMode

from validators import expectations


def _write_yaml(tmp_path: pathlib.Path, name: str, text: str) -> pathlib.Path:
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


@pytest.fixture
def exclusions_path(tmp_path):
    return _write_yaml(
        tmp_path,
        "unpriced_symbols.yaml",
        """
schema_version: 1
exclusions:
  - symbol: "912810UJ5"
    class: fixed_income_cusip
    reason: "US Treasury CUSIP; no equity vendor prices it."
    owner: brian
    re_exam: "2026-12-14"
  - symbol: "1299"
    class: unsupported_exchange
    reason: "HKEX listing; no known .HK vendor path."
    owner: brian
    re_exam: "2026-12-14"
""",
    )


@pytest.fixture
def suffix_map_path(tmp_path):
    return _write_yaml(
        tmp_path,
        "suffix_normalization.yaml",
        """
schema_version: 1
normalize:
  - denominator_symbol: "D05"
    priced_symbol: "D05.SI"
    exchange: "Singapore Exchange (SGX)"
""",
    )


def _check(*, exclusions_path, suffix_map_path, **kw):
    base = dict(
        unit_id="D20",
        denominator_symbols=["AAPL", "D05", "912810UJ5", "1299"],
        covered_symbols=["AAPL", "D05.SI"],
        exclusions=expectations.load_exclusions(exclusions_path),
        suffix_map=expectations.load_suffix_map(suffix_map_path),
    )
    base.update(kw)
    return expectations.check_cardinality(**base)


def test_full_coverage_after_normalization_and_declared_exclusions_is_ok(exclusions_path, suffix_map_path):
    reading = _check(exclusions_path=exclusions_path, suffix_map_path=suffix_map_path)
    assert reading.verdict == "ok"
    assert reading.clean
    assert reading.value == 1.0
    assert "zero undeclared misses" in reading.detail


def test_suffix_normalization_matches_a_bare_symbol_to_its_suffixed_publish(exclusions_path, suffix_map_path):
    """Without the map, D05 (denominator) would never match D05.SI (covered)."""
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=["D05"],
        covered_symbols=["D05.SI"],
    )
    assert reading.verdict == "ok"
    assert reading.value == 1.0


def test_an_undeclared_missing_symbol_is_named_and_fails_the_floor(exclusions_path, suffix_map_path):
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=["AAPL", "MSFT"],
        covered_symbols=["AAPL"],
    )
    assert reading.verdict == "below_floor"
    assert not reading.clean
    assert "UNDECLARED miss" in reading.detail
    assert "MSFT" in reading.detail


def test_a_declared_exclusion_is_never_counted_as_an_undeclared_miss(exclusions_path, suffix_map_path):
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=["AAPL", "912810UJ5"],
        covered_symbols=["AAPL"],
    )
    assert reading.verdict == "ok"
    assert "UNDECLARED" not in reading.detail
    assert "912810UJ5" in reading.detail  # named as a declared exclusion, not a miss


def test_below_floor_when_coverage_is_partial(exclusions_path, suffix_map_path):
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=["AAPL", "MSFT", "GOOGL", "AMZN"],
        covered_symbols=["AAPL", "MSFT", "GOOGL"],
        floor=1.0,
    )
    assert reading.verdict == "below_floor"
    assert reading.value == pytest.approx(0.75)
    assert "AMZN" in reading.detail


def test_a_floor_below_one_can_still_pass_with_a_real_gap(exclusions_path, suffix_map_path):
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=["AAPL", "MSFT", "GOOGL", "AMZN"],
        covered_symbols=["AAPL", "MSFT", "GOOGL"],
        floor=0.75,
    )
    assert reading.verdict == "ok"


def test_an_empty_denominator_is_unmeasurable_never_a_vacuous_pass(exclusions_path, suffix_map_path):
    reading = _check(
        exclusions_path=exclusions_path,
        suffix_map_path=suffix_map_path,
        denominator_symbols=[],
        covered_symbols=[],
    )
    assert reading.verdict == "unmeasurable"
    assert not reading.clean


# ── declared-contract loaders ─────────────────────────────────────────────


def test_load_exclusions_rejects_a_row_with_an_unknown_class(tmp_path):
    p = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
schema_version: 1
exclusions:
  - symbol: "X"
    class: not_a_real_class
    reason: "r"
    owner: brian
    re_exam: "2026-12-14"
""",
    )
    with pytest.raises(ValueError, match="not_a_real_class"):
        expectations.load_exclusions(p)


def test_load_exclusions_rejects_a_missing_field(tmp_path):
    p = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
schema_version: 1
exclusions:
  - symbol: "X"
    class: fixed_income_cusip
    owner: brian
    re_exam: "2026-12-14"
""",
    )
    with pytest.raises(ValueError, match="missing required field"):
        expectations.load_exclusions(p)


def test_load_exclusions_rejects_a_duplicate_symbol(tmp_path):
    p = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
schema_version: 1
exclusions:
  - symbol: "X"
    class: fixed_income_cusip
    reason: "r"
    owner: brian
    re_exam: "2026-12-14"
  - symbol: "X"
    class: unsupported_exchange
    reason: "r2"
    owner: brian
    re_exam: "2026-12-14"
""",
    )
    with pytest.raises(ValueError, match="duplicate exclusion"):
        expectations.load_exclusions(p)


def test_load_suffix_map_rejects_a_duplicate_denominator_symbol(tmp_path):
    p = _write_yaml(
        tmp_path,
        "bad.yaml",
        """
schema_version: 1
normalize:
  - denominator_symbol: "D05"
    priced_symbol: "D05.SI"
    exchange: "x"
  - denominator_symbol: "D05"
    priced_symbol: "D05.SG"
    exchange: "y"
""",
    )
    with pytest.raises(ValueError, match="duplicate normalization"):
        expectations.load_suffix_map(p)


# ── the committed contract, against the measured 2026-09-14 gap ───────────


def test_committed_exclusions_and_suffix_map_explain_the_measured_d20_gap():
    """Reproduces the 2026-09-14 live measurement (alpha-engine-config-I10780):
    88-ticker `metron/holdings_universe.json` universe, 80 published closes,
    zero undeclared misses once the committed contract is applied."""
    denominator = [
        "1299", "149159VZ5", "40219MBJ2", "46659C4L1", "63983RFZ7", "912810UJ5",
        "912828YK0", "912834PB8", "AAPL", "AEHR", "AMD", "AMZN", "ANET", "ANF",
        "AON", "APH", "ASML", "ATAI", "AVAV", "AVGO", "AXON", "BE", "BN", "BX",
        "CCJ", "CEG", "CME", "COST", "CRSP", "CRUS", "CRWD", "D05", "DECK",
        "DOCS", "DUOL", "ENVX", "EQIX", "FDRXX", "FNILX", "FTIHX", "FZILX",
        "GEV", "GOOGL", "GTBIF", "HELP", "HL", "HOOD", "IBKR", "IONQ", "ISRG",
        "JOBY", "JPM", "LLY", "LUNR", "MARUY", "MELI", "META", "MP", "MRNA",
        "MSFT", "MU", "NBIS", "NBIX", "NOVN", "NVDA", "PBF", "PLTR", "QLYS",
        "RACE", "RGEN", "RIO", "RKLB", "RMS", "SGOV", "SKHY", "SNDK", "SPCX",
        "SU", "TOELY", "TSLA", "TSM", "UAL", "VICR", "VMFXX", "VOO", "VST",
        "WM", "XPEV",
    ]
    assert len(denominator) == 88
    covered = set(denominator) - {
        "1299", "149159VZ5", "40219MBJ2", "46659C4L1", "63983RFZ7", "912810UJ5",
        "912828YK0", "912834PB8", "D05", "NOVN", "RMS", "SU",
    } | {"D05.SI", "NOVN.SW", "RMS.PA", "SU.PA"}
    assert len(covered) == 80

    reading = expectations.check_cardinality(
        unit_id="D20",
        denominator_symbols=denominator,
        covered_symbols=covered,
        floor=1.0,
    )
    assert reading.verdict == "ok"
    assert reading.value == 1.0
    assert "zero undeclared misses" in reading.detail


# ── observe-mode staging (`sf-pipeline-policy` §7a) ───────────────────────


def test_the_guard_ships_observing_with_its_own_promotion_criterion_and_tracker():
    guard = expectations.CARDINALITY_GUARD
    assert guard.mode is GuardMode.OBSERVE
    assert not guard.enforcing
    assert "10 consecutive clean" in guard.promotion_criterion
    assert guard.tracked_issue == "alpha-engine-config-I10780"


def test_it_is_a_distinct_staging_from_empty_fresh():
    assert expectations.CARDINALITY_GUARD.name != expectations.EMPTY_FRESH_GUARD.name


def test_observe_mode_is_loud_for_a_below_floor_verdict(caplog):
    reading = expectations.GuardReading("below_floor", "x", value=0.5, baseline=1.0)
    with caplog.at_level("ERROR"):
        expectations.report(reading, unit_id="D20", staging=expectations.CARDINALITY_GUARD)
    assert "data_cardinality" in caplog.text
    assert "mode=observe" in caplog.text


# ── the board row ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "verdict, status",
    [
        ("ok", "GREEN"),
        ("below_floor", "RED"),
        ("unmeasurable", "N/A-MISSING-INPUT"),
        ("not_applicable", "N/A-NOT-IMPL"),
    ],
)
def test_every_verdict_renders_a_completeness_metric_including_the_passes(verdict, status):
    reading = expectations.CardinalityReading(verdict, "detail", value=1.0, baseline=1.0)
    record = expectations.cardinality_metric("D20", reading, source_path="tests")
    assert record.name == "data.D20.completeness"
    assert record.status == status
    assert record.metric_type == "ratio"
    assert record.unit == "ratio"


def test_the_cardinality_verdict_vocabulary_is_a_subset_of_the_manifest_contract():
    from nousergon_lib import contracts

    schema = contracts.load_schema("data_run_manifest")
    enum = set(schema["$defs"]["GuardVerdict"]["properties"]["verdict"]["enum"])
    assert set(expectations.CARDINALITY_VERDICTS).issubset(enum)


class _FakeS3:
    def __init__(self):
        self.put_calls: list[dict] = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {"ETag": '"abc123"'}


def test_publish_completeness_metric_writes_the_dated_key():
    s3 = _FakeS3()
    reading = expectations.CardinalityReading("ok", "detail", value=1.0, baseline=1.0)
    metric = expectations.cardinality_metric("D20", reading, source_path="tests")
    key = expectations.publish_completeness_metric(s3, "alpha-engine-research", "2026-09-14", metric)
    assert key == "data_collection/metrics/eod_completeness/2026-09-14.json"
    assert len(s3.put_calls) == 1
    assert s3.put_calls[0]["Key"] == key
    assert s3.put_calls[0]["Bucket"] == "alpha-engine-research"


# ── default contract paths resolve to the committed files ────────────────


def test_default_paths_resolve_to_the_committed_contract_files():
    assert expectations.default_exclusions_path().name == "unpriced_symbols.yaml"
    assert expectations.default_suffix_map_path().name == "suffix_normalization.yaml"
    assert expectations.default_exclusions_path().exists()
    assert expectations.default_suffix_map_path().exists()


def test_the_committed_exclusions_file_loads_and_has_eight_entries():
    exclusions = expectations.load_exclusions()
    assert len(exclusions) == 8
    assert exclusions["1299"]["class"] == "unsupported_exchange"
    assert exclusions["912810UJ5"]["class"] == "fixed_income_cusip"


def test_the_committed_suffix_map_loads_and_has_four_entries():
    suffix_map = expectations.load_suffix_map()
    assert suffix_map == {
        "D05": "D05.SI",
        "NOVN": "NOVN.SW",
        "RMS": "RMS.PA",
        "SU": "SU.PA",
    }
