"""A `vendor_live` key is graded on shape and bounded agreement, not bytes.

Brian ruling 2026-09-20, `alpha-engine-config-I11203`, option (a).

**The problem it answers.** The 2026-09-18 parity report compared 959 keys:
918 matched, 34 mismatched. Every one of the 34 was re-fetch drift, not a
producer defect -- `ANET.close 199.43 live vs 199.39 shadow` (yfinance revising
a settled close), `EUR 1.149161 vs 1.149029`, `AAPL.mean_target 327.8374 vs
328.2221`, `$.analyst.ANF: only in live` (delisted between the two fetches).
The declared tolerance is `relative: 1e-6`; those deltas are three orders of
magnitude outside it, and correctly so -- the numbers genuinely changed.

So `data.cutover_ready.parity`, which requires every key to match, could never
reach 4/4: no amount of work on the collector makes a Sunday re-fetch equal a
Friday fetch.

**What the class does and does not forgive.** Values inside the declared band
are drift. Everything else is still a breach:

* SHAPE is graded exactly -- a cardinality diff is never forgiven;
* KEY-SET COVERAGE is graded against a declared floor, so a delisted ticker
  passes and a producer dropping half the universe does not;
* a value diff OUTSIDE the band is still a breach;
* `deterministic` keys are completely untouched -- 918 of them already pass
  byte-equality and must keep doing so.

**`metron_sentiment` is deliberately NOT vendor_live.** Its worst observed
delta is 25.0 relative -- `$.sentiment.AAPL.event_count: 12 live vs 4 shadow`,
a count over an OBSERVATION WINDOW rather than a moving value. A band wide
enough to absorb it would pass anything. It stays deterministic and keeps
failing until the ruling's option (b), replay from captured vendor payloads.
"""

from __future__ import annotations

import json

import pytest

from shadow.parity import (
    ContractSchema,
    _CONTRACTS_DIR,
    classify_diff,
    compare_bytes,
    resolve_contract,
)

REL = 1e-6
ABS = 1e-9


def _contract(rel: float, floor: float) -> ContractSchema:
    import re

    return ContractSchema(
        _CONTRACTS_DIR / "synthetic.schema.json",
        frozenset(),
        re.compile("^synthetic$"),
        comparison_class="vendor_live",
        value_band_relative=rel,
        value_band_absolute=0.0,
        coverage_floor=floor,
    )


def _body(live: dict, shadow: dict, contract=None) -> dict:
    return compare_bytes(
        "k.json",
        json.dumps(live).encode(),
        json.dumps(shadow).encode(),
        rel=REL,
        absolute=ABS,
        contract=contract,
    )


# ── the diff classifier ────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "diff,expected",
    [
        ("$.analyst.ANF: only in live", "membership"),
        ("$.currency.TELWY: only in shadow", "membership"),
        ("$.series.DGS10: length 498 live vs 497 shadow", "cardinality"),
        ("$.closes.ANET.close: 199.43 live vs 199.39 shadow", "value"),
        # A string VALUE that merely CONTAINS the membership wording must not
        # be classified as membership -- the suffix is anchored for this reason.
        ("$.note: 'only in live' live vs 'other' shadow", "value"),
    ],
)
def test_classify_diff(diff, expected):
    assert classify_diff(diff) == expected


# ── the class itself ───────────────────────────────────────────────────────

def test_a_value_inside_the_band_is_drift_not_a_breach():
    body = _body({"closes": {"ANET": 199.43}}, {"closes": {"ANET": 199.39}}, _contract(0.005, 0.99))
    assert body["verdict"] == "match"
    assert body["values"]["breaches"] == 0
    assert body["vendor_drift"]["class"] == "vendor_live"


def test_a_value_outside_the_band_is_still_a_breach():
    body = _body({"closes": {"ANET": 199.43}}, {"closes": {"ANET": 150.00}}, _contract(0.005, 0.99))
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1


def test_the_same_value_diff_is_a_breach_for_a_deterministic_key():
    """The band must not leak into keys that did not declare the class."""
    body = _body({"closes": {"ANET": 199.43}}, {"closes": {"ANET": 199.39}}, contract=None)
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] == 1


def test_a_cardinality_diff_is_never_forgiven():
    """Shape is graded exactly, however wide the band."""
    body = _body({"series": [1, 2, 3]}, {"series": [1, 2]}, _contract(0.99, 0.0))
    assert body["verdict"] == "mismatch"
    assert body["values"]["breaches"] >= 1


def test_one_delisted_ticker_passes_the_coverage_floor():
    live = {f"T{i}": 1.0 for i in range(100)}
    shadow = {k: v for k, v in live.items() if k != "T0"}
    body = _body(live, shadow, _contract(0.005, 0.98))
    assert body["coverage"]["missing"] == 1
    assert body["coverage"]["met"] is True
    assert body["verdict"] == "match"


def test_a_producer_dropping_the_universe_fails_the_coverage_floor():
    live = {f"T{i}": 1.0 for i in range(100)}
    shadow = {f"T{i}": 1.0 for i in range(50)}
    body = _body(live, shadow, _contract(0.005, 0.98))
    assert body["coverage"]["missing"] == 50
    assert body["coverage"]["met"] is False
    assert body["verdict"] == "mismatch"


# ── the declarations ───────────────────────────────────────────────────────

def test_a_vendor_live_contract_must_declare_its_bounds():
    """A class with no declared band or floor would pass everything."""
    import re

    from shadow.parity import _load_contract_schemas

    _load_contract_schemas.cache_clear()
    for contract in _load_contract_schemas():
        if contract.is_vendor_live:
            assert contract.coverage_floor > 0.0, f"{contract.path.name}: coverage_floor is 0"
            assert contract.coverage_floor <= 1.0, f"{contract.path.name}: coverage_floor > 1"


def test_sentiment_is_deliberately_not_vendor_live():
    """Its worst observed delta is 25.0 relative on a window-dependent count.

    Pinned as a TEST rather than a comment because the obvious next move --
    'sentiment still mismatches, give it a band too' -- is the one that would
    hollow out the whole class.
    """
    contract = resolve_contract("market_data/sentiment/latest.json")
    assert contract is not None
    assert contract.comparison_class == "deterministic", (
        "metron_sentiment must stay deterministic: `event_count` is a count over an "
        "observation window, and a band wide enough to absorb 12-vs-4 passes anything. "
        "The fix is captured-payload replay (I11203 option b), not a wider band."
    )


def test_deterministic_is_the_default_for_every_unclassified_contract():
    from shadow.parity import _load_contract_schemas

    _load_contract_schemas.cache_clear()
    declared = 0
    for contract in _load_contract_schemas():
        doc = json.loads(contract.path.read_text())
        if not doc.get("x-comparison-class"):
            assert contract.comparison_class == "deterministic"
        else:
            declared += 1
    assert declared, "no contract declares a comparison class; this suite would be vacuous"
